"""
edgebench.compare
~~~~~~~~~~~~~~~~~
First-class comparison between two BatchResult / ExperimentReport objects.
Surfaces: latency deltas, percentile regressions, error-rate regressions,
drift changes, environment differences.
"""
from __future__ import annotations

import dataclasses
import json
from dataclasses import dataclass
from typing import Dict, List, Optional

from .models import BatchResult, EnvironmentCapture, ExperimentReport, GroupStats


@dataclass
class GroupDiff:
    group_key: str
    baseline_count: int = 0
    candidate_count: int = 0

    mean_delta_ms: Optional[float] = None
    median_delta_ms: Optional[float] = None
    p90_delta_ms: Optional[float] = None
    p95_delta_ms: Optional[float] = None
    p99_delta_ms: Optional[float] = None

    success_rate_delta: Optional[float] = None
    error_rate_delta: Optional[float] = None
    drift_mean_delta_ns: Optional[float] = None

    regression: bool = False
    regression_reasons: List[str] = dataclasses.field(default_factory=list)


@dataclass
class CompareResult:
    baseline_run_id: str = ""
    candidate_run_id: str = ""
    group_diffs: Dict[str, GroupDiff] = dataclasses.field(default_factory=dict)
    env_diffs: Dict[str, str] = dataclasses.field(default_factory=dict)
    summary: str = ""


def compare(baseline: ExperimentReport, candidate: ExperimentReport) -> CompareResult:
    result = CompareResult(
        baseline_run_id=baseline.run_id,
        candidate_run_id=candidate.run_id,
    )

    b_groups = baseline.batch.group_stats if baseline.batch else {}
    c_groups = candidate.batch.group_stats if candidate.batch else {}

    all_keys = set(b_groups) | set(c_groups)
    for gk in all_keys:
        b = b_groups.get(gk)
        c = c_groups.get(gk)
        diff = GroupDiff(
            group_key=gk,
            baseline_count=b.count if b else 0,
            candidate_count=c.count if c else 0,
        )

        if b and c:
            diff.mean_delta_ms = _delta(c.mean_ms, b.mean_ms)
            diff.median_delta_ms = _delta(c.median_ms, b.median_ms)
            diff.p90_delta_ms = _delta(c.p90_ms, b.p90_ms)
            diff.p95_delta_ms = _delta(c.p95_ms, b.p95_ms)
            diff.p99_delta_ms = _delta(c.p99_ms, b.p99_ms)
            diff.success_rate_delta = (c.success_rate or 0) - (b.success_rate or 0)
            diff.error_rate_delta = -diff.success_rate_delta
            diff.drift_mean_delta_ns = _delta(c.drift_mean_ns, b.drift_mean_ns)

            # Regression detection
            reasons = []
            LATENCY_REGRESSION_PCT = 10.0
            if diff.p99_delta_ms and b.p99_ms and diff.p99_delta_ms > b.p99_ms * LATENCY_REGRESSION_PCT / 100:
                reasons.append(f"p99 up {diff.p99_delta_ms:+.2f}ms")
            if diff.p95_delta_ms and b.p95_ms and diff.p95_delta_ms > b.p95_ms * LATENCY_REGRESSION_PCT / 100:
                reasons.append(f"p95 up {diff.p95_delta_ms:+.2f}ms")
            if diff.error_rate_delta and diff.error_rate_delta > 0.01:
                reasons.append(f"error rate up {diff.error_rate_delta*100:+.1f}%")
            diff.regression = bool(reasons)
            diff.regression_reasons = reasons

        result.group_diffs[gk] = diff

    # Environment diff
    if baseline.environment and candidate.environment:
        b_env = dataclasses.asdict(baseline.environment)
        c_env = dataclasses.asdict(candidate.environment)
        for k in set(b_env) | set(c_env):
            bv = str(b_env.get(k, ""))
            cv = str(c_env.get(k, ""))
            if bv != cv:
                result.env_diffs[k] = f"{bv!r} → {cv!r}"

    regressions = [d for d in result.group_diffs.values() if d.regression]
    result.summary = (
        f"{len(regressions)} regressions detected across {len(all_keys)} groups."
        if regressions
        else f"No regressions detected across {len(all_keys)} groups."
    )

    return result


def _delta(new: Optional[float], old: Optional[float]) -> Optional[float]:
    if new is None or old is None:
        return None
    return new - old
