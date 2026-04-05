"""
edgebench.models
~~~~~~~~~~~~~~~~
All typed data models. No untyped dicts anywhere in the execution path.
Schema versions are embedded in every top-level model.
"""
from __future__ import annotations

import platform
import sys
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional


# ---------------------------------------------------------------------------
# Schema versions
# ---------------------------------------------------------------------------

CONFIG_SCHEMA_VERSION = "1.0"
RESULT_SCHEMA_VERSION = "1.0"
REPORT_SCHEMA_VERSION = "1.0"
EVENT_SCHEMA_VERSION = "1.0"


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class HttpMethod(str, Enum):
    GET = "GET"
    PUT = "PUT"
    POST = "POST"
    PATCH = "PATCH"
    DELETE = "DELETE"
    HEAD = "HEAD"


class TransportBackend(str, Enum):
    HTTPX = "httpx"
    CURL_CFFI = "curl_cffi"


class ConnectionMode(str, Enum):
    """Controls client/socket/TLS reuse between requests."""
    COLD_SOCKET = "cold_socket"       # new TCP + TLS + client each time
    COLD_TLS = "cold_tls"             # new TLS but reuse TCP if possible
    COLD_CLIENT = "cold_client"       # new client, pooled underneath
    WARM_REUSE = "warm_reuse"         # reuse connection pool
    WARM_TLS = "warm_tls"             # reuse TLS session
    POOLED = "pooled"                 # persistent pooled clients


class RunMode(str, Enum):
    BENCHMARK = "benchmark"   # no retries, fixed identities, stats-heavy
    PROBE = "probe"           # faster, tolerant, ops-focused


class FailureKind(str, Enum):
    DNS_FAILURE = "dns_failure"
    PROXY_CONNECT_FAILURE = "proxy_connect_failure"
    TCP_CONNECT_TIMEOUT = "tcp_connect_timeout"
    TLS_FAILURE = "tls_failure"
    WRITE_TIMEOUT = "write_timeout"
    READ_TIMEOUT = "read_timeout"
    REMOTE_RESET = "remote_reset"
    HTTP_ERROR = "http_error"
    LOCAL_SCHEDULER_MISS = "local_scheduler_miss"
    CANCELLED = "cancelled"
    UNKNOWN = "unknown"


class AuthMode(str, Enum):
    NONE = "none"
    BEARER = "bearer"
    BASIC = "basic"
    HEADER = "header"


class CacheBustPolicy(str, Enum):
    NONE = "none"
    QUERY_PARAM = "query_param"
    HEADER = "header"


class RetryPolicy(str, Enum):
    DISABLED = "disabled"
    ON_NETWORK_ERROR = "on_network_error"
    ON_5XX = "on_5xx"


class EventKind(str, Enum):
    RUN_STARTED = "run_started"
    REQUEST_SCHEDULED = "request_scheduled"
    REQUEST_FIRED = "request_fired"
    RESPONSE_RECEIVED = "response_received"
    REQUEST_FAILED = "request_failed"
    RUN_COMPLETED = "run_completed"
    RUN_ABORTED = "run_aborted"


# ---------------------------------------------------------------------------
# Specs (immutable inputs)
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class IdentitySpec:
    """Paired TLS profile + User-Agent. Never mix them independently."""
    profile: str          # curl_cffi impersonate profile name
    user_agent: str
    label: str            # human label, e.g. "chrome124-win"


@dataclass(frozen=True)
class ProxySpec:
    id: str
    url: str              # e.g. "http://host:port"
    region: Optional[str] = None
    provider: Optional[str] = None


@dataclass(frozen=True)
class TargetSpec:
    id: str
    url: str
    method: HttpMethod = HttpMethod.PUT
    headers: Dict[str, str] = field(default_factory=dict)
    body: Optional[Any] = None
    body_generator: Optional[str] = None   # dotted import path to a callable
    auth_mode: AuthMode = AuthMode.NONE
    auth_value: Optional[str] = None
    expected_status: int = 200
    idempotent: bool = True


@dataclass(frozen=True)
class RequestSpec:
    """One fully-resolved request in the execution plan. Immutable."""
    request_index: int
    target: TargetSpec
    identity: IdentitySpec
    proxy: Optional[ProxySpec]
    scheduled_ns: int          # monotonic ns
    group_key: str             # for aggregation
    is_warmup: bool = False


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

@dataclass
class RunConfig:
    schema_version: str = CONFIG_SCHEMA_VERSION
    run_id: str = ""
    mode: RunMode = RunMode.BENCHMARK
    targets: List[TargetSpec] = field(default_factory=list)
    identities: List[IdentitySpec] = field(default_factory=list)
    proxies: List[ProxySpec] = field(default_factory=list)
    backend: TransportBackend = TransportBackend.CURL_CFFI
    connection_mode: ConnectionMode = ConnectionMode.COLD_SOCKET

    # schedule
    warmup_runs: int = 3
    measured_runs: int = 10
    inter_request_gap_ms: float = 100.0
    per_group_cooldown_ms: float = 500.0
    randomize_order: bool = False
    seed: Optional[int] = 42
    schedule_start_ns: Optional[int] = None

    # concurrency
    global_concurrency: int = 10
    per_host_concurrency: int = 5
    per_proxy_concurrency: int = 3

    # policies
    retry_policy: RetryPolicy = RetryPolicy.DISABLED
    cache_bust_policy: CacheBustPolicy = CacheBustPolicy.QUERY_PARAM
    connection_reuse: Optional[ConnectionMode] = None

    # output
    output_dir: str = "./results"
    raw_jsonl: bool = True


# ---------------------------------------------------------------------------
# Timing
# ---------------------------------------------------------------------------

@dataclass
class TimingBreakdown:
    """All phase timings in milliseconds. None = not captured."""
    scheduled_ns: int = 0
    awakened_ns: int = 0
    fired_ns: int = 0
    drift_ns: int = 0

    queue_wait_ms: Optional[float] = None
    scheduler_drift_ms: Optional[float] = None
    client_creation_ms: Optional[float] = None
    connect_ms: Optional[float] = None
    tls_ms: Optional[float] = None
    first_byte_ms: Optional[float] = None
    body_read_ms: Optional[float] = None
    total_wall_ms: Optional[float] = None


# ---------------------------------------------------------------------------
# Results
# ---------------------------------------------------------------------------

@dataclass
class RequestResult:
    schema_version: str = RESULT_SCHEMA_VERSION
    request_index: int = 0
    group_key: str = ""
    is_warmup: bool = False

    success: bool = False
    status_code: Optional[int] = None
    failure_kind: Optional[FailureKind] = None
    failure_detail: Optional[str] = None

    timing: TimingBreakdown = field(default_factory=TimingBreakdown)

    identity_label: str = ""
    proxy_id: Optional[str] = None
    target_id: str = ""
    backend: str = ""
    connection_mode: str = ""


@dataclass
class GroupStats:
    group_key: str = ""
    count: int = 0
    success_count: int = 0
    success_rate: float = 0.0

    min_ms: Optional[float] = None
    max_ms: Optional[float] = None
    mean_ms: Optional[float] = None
    median_ms: Optional[float] = None
    p90_ms: Optional[float] = None
    p95_ms: Optional[float] = None
    p99_ms: Optional[float] = None
    stddev_ms: Optional[float] = None
    variance_ms: Optional[float] = None
    mad_ms: Optional[float] = None
    iqr_ms: Optional[float] = None
    outlier_count: int = 0

    drift_mean_ns: Optional[float] = None
    drift_p99_ns: Optional[float] = None

    error_distribution: Dict[str, int] = field(default_factory=dict)


@dataclass
class BatchResult:
    schema_version: str = RESULT_SCHEMA_VERSION
    run_id: str = ""
    results: List[RequestResult] = field(default_factory=list)
    group_stats: Dict[str, GroupStats] = field(default_factory=dict)
    aborted: bool = False
    abort_reason: Optional[str] = None


@dataclass
class EnvironmentCapture:
    os: str = ""
    kernel: str = ""
    python_version: str = ""
    cpu_model: str = ""
    cpu_governor: Optional[str] = None
    load_average: Optional[str] = None
    clock_source: Optional[str] = None
    library_versions: Dict[str, str] = field(default_factory=dict)
    network_interfaces: List[str] = field(default_factory=list)
    public_egress_ip: Optional[str] = None

    @classmethod
    def capture(cls) -> "EnvironmentCapture":
        import importlib.metadata
        libs = {}
        for lib in ("httpx", "curl_cffi", "anyio"):
            try:
                libs[lib] = importlib.metadata.version(lib)
            except Exception:
                libs[lib] = "unknown"

        gov = None
        try:
            with open("/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor") as f:
                gov = f.read().strip()
        except Exception:
            pass

        clock_src = None
        try:
            with open("/sys/devices/system/clocksource/clocksource0/current_clocksource") as f:
                clock_src = f.read().strip()
        except Exception:
            pass

        load_avg = None
        try:
            import os
            la = os.getloadavg()
            load_avg = f"{la[0]:.2f} {la[1]:.2f} {la[2]:.2f}"
        except Exception:
            pass

        return cls(
            os=platform.system(),
            kernel=platform.release(),
            python_version=sys.version,
            cpu_model=platform.processor() or platform.machine(),
            cpu_governor=gov,
            load_average=load_avg,
            clock_source=clock_src,
            library_versions=libs,
        )


@dataclass
class ExperimentReport:
    schema_version: str = REPORT_SCHEMA_VERSION
    run_id: str = ""
    config: Optional[RunConfig] = None
    environment: Optional[EnvironmentCapture] = None
    batch: Optional[BatchResult] = None
    raw_jsonl_path: Optional[str] = None
    summary_json_path: Optional[str] = None
    csv_path: Optional[str] = None
    html_report_path: Optional[str] = None


# ---------------------------------------------------------------------------
# Structured event log
# ---------------------------------------------------------------------------

@dataclass
class Event:
    schema_version: str = EVENT_SCHEMA_VERSION
    kind: EventKind = EventKind.RUN_STARTED
    run_id: str = ""
    request_index: Optional[int] = None
    payload: Dict[str, Any] = field(default_factory=dict)
    ts_ns: int = 0
