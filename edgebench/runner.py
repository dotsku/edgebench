"""
edgebench.runner
~~~~~~~~~~~~~~~~
Executes a pre-built plan.  Handles:
  - per-host / per-proxy / global concurrency semaphores
  - SIGINT + partial batch failure draining
  - result buffer flushing
  - client cleanup on shutdown
  - run status marking (aborted vs completed)
"""
from __future__ import annotations

import asyncio
import signal
import time
from collections import defaultdict
from typing import Dict, List, Optional

from .clock import Clock, get_clock
from .events import (
    emit_request_failed,
    emit_request_fired,
    emit_response_received,
    emit_request_scheduled,
    emit_run_aborted,
    emit_run_completed,
    emit_run_started,
)
from .metrics import aggregate
from .models import (
    BatchResult,
    FailureKind,
    RequestResult,
    RequestSpec,
    RunConfig,
    TimingBreakdown,
)
from .scheduler import schedule_fire
from .transport import TransportBackendBase, make_backend
from .writer import ResultWriter


class Runner:
    """
    Executes a plan (list of RequestSpec) against the configured backend.

    Concurrency is controlled at three levels:
      - global_semaphore: total in-flight across all groups
      - host_semaphores: per-hostname
      - proxy_semaphores: per-proxy id
    """

    def __init__(self, cfg: RunConfig, clock: Optional[Clock] = None) -> None:
        self.cfg = cfg
        self._clock = clock or get_clock()
        self._backend: Optional[TransportBackendBase] = None
        self._results: List[RequestResult] = []
        self._writer: Optional[ResultWriter] = None
        self._aborted = False
        self._abort_reason: Optional[str] = None

        # Semaphores (created in run() inside the event loop)
        self._global_sem: Optional[asyncio.Semaphore] = None
        self._host_sems: Dict[str, asyncio.Semaphore] = {}
        self._proxy_sems: Dict[str, asyncio.Semaphore] = {}

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    async def run(self, plan: List[RequestSpec]) -> BatchResult:
        cfg = self.cfg
        self._global_sem = asyncio.Semaphore(cfg.global_concurrency)
        self._backend = make_backend(cfg.backend, cfg.connection_mode)

        # JSONL writer
        if cfg.raw_jsonl:
            import os, uuid
            jsonl_path = os.path.join(cfg.output_dir, f"{cfg.run_id}.jsonl")
            self._writer = ResultWriter(jsonl_path)

        # SIGINT handler
        loop = asyncio.get_running_loop()
        loop.add_signal_handler(signal.SIGINT, self._handle_sigint)

        emit_run_started(cfg.run_id, {
            "mode": cfg.mode.value,
            "total_requests": len(plan),
            "backend": cfg.backend.value,
            "connection_mode": cfg.connection_mode.value,
        })

        try:
            tasks = [asyncio.create_task(self._execute_one(spec)) for spec in plan]
            await asyncio.gather(*tasks, return_exceptions=True)
        except asyncio.CancelledError:
            self._aborted = True
            self._abort_reason = "CancelledError"
        finally:
            loop.remove_signal_handler(signal.SIGINT)
            if self._writer:
                self._writer.close()
            if self._backend:
                await self._backend.close()

        if self._aborted:
            emit_run_aborted(cfg.run_id, self._abort_reason or "unknown")
        else:
            success = sum(1 for r in self._results if r.success)
            emit_run_completed(cfg.run_id, len(self._results), success)

        group_stats = aggregate(self._results)
        return BatchResult(
            run_id=cfg.run_id,
            results=self._results,
            group_stats=group_stats,
            aborted=self._aborted,
            abort_reason=self._abort_reason,
        )

    # ------------------------------------------------------------------
    # Per-request execution
    # ------------------------------------------------------------------

    async def _execute_one(self, spec: RequestSpec) -> None:
        if self._aborted:
            result = self._make_cancelled(spec)
            self._record(result)
            return

        emit_request_scheduled(
            self.cfg.run_id, spec.request_index, spec.scheduled_ns, spec.group_key
        )

        # Two-phase async scheduler
        timing = await schedule_fire(spec.scheduled_ns, self._clock)

        emit_request_fired(
            self.cfg.run_id, spec.request_index, timing.fired_ns, timing.drift_ns
        )

        # Check for severe scheduler miss
        miss_threshold_ns = 50_000_000  # 50 ms
        if timing.drift_ns > miss_threshold_ns:
            result = RequestResult(
                request_index=spec.request_index,
                group_key=spec.group_key,
                is_warmup=spec.is_warmup,
                success=False,
                failure_kind=FailureKind.LOCAL_SCHEDULER_MISS,
                failure_detail=f"drift={timing.drift_ns}ns > {miss_threshold_ns}ns threshold",
                timing=timing,
                identity_label=spec.identity.label,
                proxy_id=spec.proxy.id if spec.proxy else None,
                target_id=spec.target.id,
                backend=self.cfg.backend.value,
                connection_mode=self.cfg.connection_mode.value,
            )
            emit_request_failed(
                self.cfg.run_id,
                spec.request_index,
                FailureKind.LOCAL_SCHEDULER_MISS.value,
                result.failure_detail or "",
            )
            self._record(result)
            return

        host = self._host_from_spec(spec)
        proxy_id = spec.proxy.id if spec.proxy else "__direct__"

        host_sem = self._get_host_sem(host)
        proxy_sem = self._get_proxy_sem(proxy_id)

        async with self._global_sem:
            async with host_sem:
                async with proxy_sem:
                    cache_bust = self.cfg.cache_bust_policy.value != "none"
                    result = await self._backend.execute(spec, cache_bust=cache_bust)
                    # Merge scheduler timing into transport result
                    result.timing.scheduled_ns = timing.scheduled_ns
                    result.timing.awakened_ns = timing.awakened_ns
                    result.timing.fired_ns = timing.fired_ns
                    result.timing.drift_ns = timing.drift_ns
                    result.timing.scheduler_drift_ms = timing.scheduler_drift_ms

        if result.success:
            emit_response_received(
                self.cfg.run_id,
                spec.request_index,
                result.status_code or 0,
                result.timing.total_wall_ms or 0.0,
            )
        else:
            emit_request_failed(
                self.cfg.run_id,
                spec.request_index,
                result.failure_kind.value if result.failure_kind else "unknown",
                result.failure_detail or "",
            )

        self._record(result)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _record(self, result: RequestResult) -> None:
        self._results.append(result)
        if self._writer:
            self._writer.write(result)

    def _make_cancelled(self, spec: RequestSpec) -> RequestResult:
        return RequestResult(
            request_index=spec.request_index,
            group_key=spec.group_key,
            is_warmup=spec.is_warmup,
            success=False,
            failure_kind=FailureKind.CANCELLED,
            failure_detail="run aborted before execution",
            timing=TimingBreakdown(scheduled_ns=spec.scheduled_ns),
            identity_label=spec.identity.label,
            proxy_id=spec.proxy.id if spec.proxy else None,
            target_id=spec.target.id,
            backend=self.cfg.backend.value,
            connection_mode=self.cfg.connection_mode.value,
        )

    def _host_from_spec(self, spec: RequestSpec) -> str:
        from urllib.parse import urlparse
        return urlparse(spec.target.url).netloc

    def _get_host_sem(self, host: str) -> asyncio.Semaphore:
        if host not in self._host_sems:
            self._host_sems[host] = asyncio.Semaphore(self.cfg.per_host_concurrency)
        return self._host_sems[host]

    def _get_proxy_sem(self, proxy_id: str) -> asyncio.Semaphore:
        if proxy_id not in self._proxy_sems:
            self._proxy_sems[proxy_id] = asyncio.Semaphore(self.cfg.per_proxy_concurrency)
        return self._proxy_sems[proxy_id]

    def _handle_sigint(self) -> None:
        self._aborted = True
        self._abort_reason = "SIGINT"
        if self._writer:
            self._writer.flush()
