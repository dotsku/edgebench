"""
edgebench — Reproducible HTTP edge benchmarking.

Claim: reproducible HTTP edge benchmarking with TLS fingerprint control.
Everything else is in service of that.
"""
from .models import (
    RunConfig, RunMode, TransportBackend, ConnectionMode,
    IdentitySpec, ProxySpec, TargetSpec, RequestSpec,
    RequestResult, BatchResult, ExperimentReport,
    TimingBreakdown, GroupStats, FailureKind,
)
from .clock import MonotonicClock, SimulatedClock, get_clock, set_clock
from .rng import seed as seed_rng
from .planner import build_plan
from .runner import Runner
from .metrics import aggregate
from .validation import validate, ConfigError

__all__ = [
    "RunConfig", "RunMode", "TransportBackend", "ConnectionMode",
    "IdentitySpec", "ProxySpec", "TargetSpec", "RequestSpec",
    "RequestResult", "BatchResult", "ExperimentReport",
    "TimingBreakdown", "GroupStats", "FailureKind",
    "MonotonicClock", "SimulatedClock", "get_clock", "set_clock",
    "seed_rng", "build_plan", "Runner", "aggregate",
    "validate", "ConfigError",
]
