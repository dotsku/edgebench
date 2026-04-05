"""
edgebench.validation
~~~~~~~~~~~~~~~~~~~~
Hard-fail config validation.  Bad configs never reach the runner.
"""
from __future__ import annotations

from typing import List

from .models import (
    ConnectionMode,
    RetryPolicy,
    RunConfig,
    RunMode,
)


class ConfigError(ValueError):
    pass


def validate(cfg: RunConfig) -> None:
    """Raise ConfigError on any invalid config. Never returns partially."""
    errors: List[str] = []

    if not cfg.targets:
        errors.append("empty target set")

    if not cfg.identities:
        errors.append("no identities defined")

    # Duplicate proxy IDs
    proxy_ids = [p.id for p in cfg.proxies]
    if len(proxy_ids) != len(set(proxy_ids)):
        errors.append(f"duplicate proxy IDs: {proxy_ids}")

    # Duplicate target IDs
    target_ids = [t.id for t in cfg.targets]
    if len(target_ids) != len(set(target_ids)):
        errors.append(f"duplicate target IDs: {target_ids}")

    # Benchmark mode must not have retries enabled
    if cfg.mode == RunMode.BENCHMARK and cfg.retry_policy != RetryPolicy.DISABLED:
        errors.append("benchmark mode requires retry_policy=DISABLED")

    # Warm reuse + cold flags conflict
    if cfg.connection_reuse is not None and cfg.connection_mode in (
        ConnectionMode.WARM_REUSE,
        ConnectionMode.WARM_TLS,
        ConnectionMode.POOLED,
    ):
        if cfg.connection_reuse == ConnectionMode.COLD_SOCKET:
            errors.append(
                f"connection_mode={cfg.connection_mode.value} conflicts with "
                f"connection_reuse={cfg.connection_reuse.value}"
            )

    if cfg.measured_runs < 1:
        errors.append(f"measured_runs must be >= 1, got {cfg.measured_runs}")

    if cfg.global_concurrency < 1:
        errors.append(f"global_concurrency must be >= 1, got {cfg.global_concurrency}")

    if cfg.per_host_concurrency < 1:
        errors.append(f"per_host_concurrency must be >= 1, got {cfg.per_host_concurrency}")

    if cfg.inter_request_gap_ms < 0:
        errors.append(f"inter_request_gap_ms must be >= 0, got {cfg.inter_request_gap_ms}")

    # Rate safety: refuse obviously unsafe rates
    estimated_rps = 1000.0 / max(cfg.inter_request_gap_ms, 1.0) * cfg.global_concurrency
    if estimated_rps > 500 and cfg.mode == RunMode.BENCHMARK:
        errors.append(
            f"unsafe estimated rate ~{estimated_rps:.0f} rps in benchmark mode — "
            "lower global_concurrency or increase inter_request_gap_ms"
        )

    if errors:
        raise ConfigError("Invalid RunConfig:\n" + "\n".join(f"  - {e}" for e in errors))
