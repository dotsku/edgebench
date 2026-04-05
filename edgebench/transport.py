"""
edgebench.transport
~~~~~~~~~~~~~~~~~~~
Transport backends behind a common interface.
Add new backends (aiohttp, raw sockets) without touching the runner.
"""
from __future__ import annotations

import time
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, Tuple

from .models import (
    ConnectionMode,
    FailureKind,
    RequestResult,
    RequestSpec,
    TimingBreakdown,
    TransportBackend,
)


# ---------------------------------------------------------------------------
# Result builder helpers
# ---------------------------------------------------------------------------

def _fail(spec: RequestSpec, kind: FailureKind, detail: str, timing: TimingBreakdown) -> RequestResult:
    return RequestResult(
        request_index=spec.request_index,
        group_key=spec.group_key,
        is_warmup=spec.is_warmup,
        success=False,
        failure_kind=kind,
        failure_detail=detail,
        timing=timing,
        identity_label=spec.identity.label,
        proxy_id=spec.proxy.id if spec.proxy else None,
        target_id=spec.target.id,
    )


def _bust_url(url: str, idx: int) -> str:
    from urllib.parse import urlparse, urlencode, urlunparse, parse_qs
    parsed = urlparse(url)
    qs = f"_eb={idx}"
    new_query = f"{parsed.query}&{qs}" if parsed.query else qs
    return urlunparse(parsed._replace(query=new_query))


def _resolve_body(spec: RequestSpec, idx: int):
    if spec.target.body_generator:
        import importlib
        parts = spec.target.body_generator.rsplit(".", 1)
        mod = importlib.import_module(parts[0])
        fn = getattr(mod, parts[1])
        return fn(spec, idx)
    return spec.target.body or {}


def _build_headers(spec: RequestSpec) -> dict:
    from .models import AuthMode
    h = dict(spec.target.headers)
    h["User-Agent"] = spec.identity.user_agent
    h.setdefault("Accept", "application/json")
    if spec.target.auth_mode == AuthMode.BEARER and spec.target.auth_value:
        h["Authorization"] = f"Bearer {spec.target.auth_value}"
    elif spec.target.auth_mode == AuthMode.BASIC and spec.target.auth_value:
        import base64
        h["Authorization"] = "Basic " + base64.b64encode(spec.target.auth_value.encode()).decode()
    elif spec.target.auth_mode == AuthMode.HEADER and spec.target.auth_value:
        h["X-Auth-Token"] = spec.target.auth_value
    return h


# ---------------------------------------------------------------------------
# Abstract backend
# ---------------------------------------------------------------------------

class TransportBackendBase(ABC):
    def __init__(self, connection_mode: ConnectionMode) -> None:
        self.connection_mode = connection_mode

    @abstractmethod
    async def execute(self, spec: RequestSpec, cache_bust: bool = False) -> RequestResult:
        ...

    async def close(self) -> None:
        pass


# ---------------------------------------------------------------------------
# curl_cffi backend
# ---------------------------------------------------------------------------

class CurlCffiBackend(TransportBackendBase):
    """
    Uses curl_cffi for TLS fingerprint impersonation.
    Supports all ConnectionMode values.
    
    Maintains separate warm sessions per (profile, proxy) tuple so reuse
    does not bleed across incompatible identities.
    """

    def __init__(self, connection_mode: ConnectionMode) -> None:
        super().__init__(connection_mode)
        # Key: (profile, proxy) tuple -> session
        self._sessions: Dict[Tuple[str, Optional[str]], Any] = {}

    async def _get_session(self, profile: str, proxy: Optional[str]):
        from curl_cffi.requests import AsyncSession
        if self.connection_mode in (
            ConnectionMode.WARM_REUSE,
            ConnectionMode.WARM_TLS,
            ConnectionMode.POOLED,
        ):
            key = (profile, proxy)
            if key not in self._sessions:
                self._sessions[key] = AsyncSession(impersonate=profile, proxy=proxy)
            return self._sessions[key], False  # don't close after use
        # Cold modes: new session every time
        return AsyncSession(impersonate=profile, proxy=proxy), True

    async def execute(self, spec: RequestSpec, cache_bust: bool = False) -> RequestResult:
        from curl_cffi.requests import AsyncSession

        timing = TimingBreakdown(scheduled_ns=spec.scheduled_ns)
        url = _bust_url(spec.target.url, spec.request_index) if cache_bust else spec.target.url
        headers = _build_headers(spec)
        body = _resolve_body(spec, spec.request_index)
        proxy_url = spec.proxy.url if spec.proxy else None

        t_client_start = time.perf_counter()
        session, should_close = await self._get_session(spec.identity.profile, proxy_url)
        timing.client_creation_ms = (time.perf_counter() - t_client_start) * 1000.0

        t_wall = time.perf_counter()
        try:
            response = await session.request(
                spec.target.method.value,
                url,
                headers=headers,
                json=body if body else None,
                timeout=10.0,
            )
            timing.total_wall_ms = (time.perf_counter() - t_wall) * 1000.0

            if should_close:
                await session.close()

            result = RequestResult(
                request_index=spec.request_index,
                group_key=spec.group_key,
                is_warmup=spec.is_warmup,
                success=(response.status_code == spec.target.expected_status),
                status_code=response.status_code,
                failure_kind=None if response.status_code == spec.target.expected_status
                             else FailureKind.HTTP_ERROR,
                timing=timing,
                identity_label=spec.identity.label,
                proxy_id=spec.proxy.id if spec.proxy else None,
                target_id=spec.target.id,
                backend=TransportBackend.CURL_CFFI.value,
                connection_mode=self.connection_mode.value,
            )
            return result

        except Exception as exc:
            timing.total_wall_ms = (time.perf_counter() - t_wall) * 1000.0
            if should_close:
                try:
                    await session.close()
                except Exception:
                    pass
            kind = _classify_curl_error(exc)
            return _fail(spec, kind, str(exc), timing)

    async def close(self) -> None:
        for session in self._sessions.values():
            try:
                await session.close()
            except Exception:
                pass
        self._sessions.clear()


# ---------------------------------------------------------------------------
# httpx backend
# ---------------------------------------------------------------------------

class HttpxBackend(TransportBackendBase):
    """httpx async backend with connection reuse support.
    
    Maintains separate warm clients per proxy URL so reuse does not bleed
    across proxy pools.
    """

    def __init__(self, connection_mode: ConnectionMode) -> None:
        super().__init__(connection_mode)
        # Key: proxy_url -> client
        self._clients: Dict[Optional[str], Any] = {}

    async def _get_client(self, proxy_url: Optional[str]):
        import httpx
        if self.connection_mode in (
            ConnectionMode.WARM_REUSE,
            ConnectionMode.WARM_TLS,
            ConnectionMode.POOLED,
        ):
            if proxy_url not in self._clients:
                proxies = {"http://": proxy_url, "https://": proxy_url} if proxy_url else None
                self._clients[proxy_url] = httpx.AsyncClient(proxies=proxies, timeout=10.0, follow_redirects=True)
            return self._clients[proxy_url], False
        proxies = {"http://": proxy_url, "https://": proxy_url} if proxy_url else None
        return httpx.AsyncClient(proxies=proxies, timeout=10.0, follow_redirects=True), True

    async def execute(self, spec: RequestSpec, cache_bust: bool = False) -> RequestResult:
        import httpx

        timing = TimingBreakdown(scheduled_ns=spec.scheduled_ns)
        url = _bust_url(spec.target.url, spec.request_index) if cache_bust else spec.target.url
        headers = _build_headers(spec)
        body = _resolve_body(spec, spec.request_index)
        proxy_url = spec.proxy.url if spec.proxy else None

        t_client_start = time.perf_counter()
        client, should_close = await self._get_client(proxy_url)
        timing.client_creation_ms = (time.perf_counter() - t_client_start) * 1000.0

        t_wall = time.perf_counter()
        try:
            response = await client.request(
                spec.target.method.value,
                url,
                headers=headers,
                json=body if body else None,
            )
            timing.total_wall_ms = (time.perf_counter() - t_wall) * 1000.0

            if should_close:
                await client.aclose()

            return RequestResult(
                request_index=spec.request_index,
                group_key=spec.group_key,
                is_warmup=spec.is_warmup,
                success=(response.status_code == spec.target.expected_status),
                status_code=response.status_code,
                failure_kind=None if response.status_code == spec.target.expected_status
                             else FailureKind.HTTP_ERROR,
                timing=timing,
                identity_label=spec.identity.label,
                proxy_id=spec.proxy.id if spec.proxy else None,
                target_id=spec.target.id,
                backend=TransportBackend.HTTPX.value,
                connection_mode=self.connection_mode.value,
            )

        except httpx.ProxyError as exc:
            timing.total_wall_ms = (time.perf_counter() - t_wall) * 1000.0
            return _fail(spec, FailureKind.PROXY_CONNECT_FAILURE, str(exc), timing)
        except httpx.ConnectTimeout as exc:
            timing.total_wall_ms = (time.perf_counter() - t_wall) * 1000.0
            return _fail(spec, FailureKind.TCP_CONNECT_TIMEOUT, str(exc), timing)
        except httpx.ReadTimeout as exc:
            timing.total_wall_ms = (time.perf_counter() - t_wall) * 1000.0
            return _fail(spec, FailureKind.READ_TIMEOUT, str(exc), timing)
        except httpx.WriteTimeout as exc:
            timing.total_wall_ms = (time.perf_counter() - t_wall) * 1000.0
            return _fail(spec, FailureKind.WRITE_TIMEOUT, str(exc), timing)
        except httpx.RemoteProtocolError as exc:
            timing.total_wall_ms = (time.perf_counter() - t_wall) * 1000.0
            return _fail(spec, FailureKind.REMOTE_RESET, str(exc), timing)
        except Exception as exc:
            timing.total_wall_ms = (time.perf_counter() - t_wall) * 1000.0
            if should_close:
                try:
                    await client.aclose()
                except Exception:
                    pass
            return _fail(spec, FailureKind.UNKNOWN, str(exc), timing)

    async def close(self) -> None:
        for client in self._clients.values():
            try:
                await client.aclose()
            except Exception:
                pass
        self._clients.clear()


# ---------------------------------------------------------------------------
# Error classification
# ---------------------------------------------------------------------------

def _classify_curl_error(exc: Exception) -> FailureKind:
    msg = str(exc).lower()
    t = type(exc).__name__.lower()
    if "proxy" in msg or "proxy" in t:
        return FailureKind.PROXY_CONNECT_FAILURE
    if "tls" in msg or "ssl" in msg or "certificate" in msg:
        return FailureKind.TLS_FAILURE
    if "timeout" in msg or "timed out" in msg:
        return FailureKind.READ_TIMEOUT
    if "dns" in msg or "resolve" in msg or "name" in msg:
        return FailureKind.DNS_FAILURE
    if "reset" in msg or "connection" in msg:
        return FailureKind.REMOTE_RESET
    return FailureKind.UNKNOWN


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------

def make_backend(backend: TransportBackend, connection_mode: ConnectionMode) -> TransportBackendBase:
    if backend == TransportBackend.CURL_CFFI:
        return CurlCffiBackend(connection_mode)
    if backend == TransportBackend.HTTPX:
        return HttpxBackend(connection_mode)
    raise ValueError(f"Unknown backend: {backend}")
