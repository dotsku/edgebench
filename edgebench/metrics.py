"""
edgebench.metrics
~~~~~~~~~~~~~~~~~
Aggregates RequestResult objects into GroupStats.
Calculates: count, success_rate, min/max, mean, median, p90/p95/p99,
stddev, variance, MAD, IQR, outlier count, drift distribution,
error distribution.
"""
from __future__ import annotations

import math
import statistics
from collections import defaultdict
from typing import Dict, List, Optional

from .models import BatchResult, FailureKind, GroupStats, RequestResult


def aggregate(results: List[RequestResult]) -> Dict[str, GroupStats]:
    """
    Group *results* by group_key and compute full statistics.
    Warmup results are excluded from stats.
    """
    groups: Dict[str, List[RequestResult]] = defaultdict(list)
    for r in results:
        if not r.is_warmup:
            groups[r.group_key].append(r)

    return {gk: _compute_stats(gk, items) for gk, items in groups.items()}


def _compute_stats(group_key: str, results: List[RequestResult]) -> GroupStats:
    stats = GroupStats(group_key=group_key)
    stats.count = len(results)
    if stats.count == 0:
        return stats

    success = [r for r in results if r.success]
    stats.success_count = len(success)
    stats.success_rate = stats.success_count / stats.count

    latencies = [
        r.timing.total_wall_ms
        for r in success
        if r.timing.total_wall_ms is not None
    ]

    if latencies:
        latencies_sorted = sorted(latencies)
        n = len(latencies_sorted)

        stats.min_ms = latencies_sorted[0]
        stats.max_ms = latencies_sorted[-1]
        stats.mean_ms = statistics.mean(latencies_sorted)
        stats.median_ms = statistics.median(latencies_sorted)
        stats.p90_ms = _percentile(latencies_sorted, 90)
        stats.p95_ms = _percentile(latencies_sorted, 95)
        stats.p99_ms = _percentile(latencies_sorted, 99)

        if n > 1:
            stats.variance_ms = statistics.variance(latencies_sorted)
            stats.stddev_ms = statistics.stdev(latencies_sorted)
        else:
            stats.variance_ms = 0.0
            stats.stddev_ms = 0.0

        stats.mad_ms = _mad(latencies_sorted)
        stats.iqr_ms = _iqr(latencies_sorted)
        stats.outlier_count = _count_outliers(latencies_sorted, stats.iqr_ms)

    # Drift distribution (all results, including failures)
    drifts = [r.timing.drift_ns for r in results if r.timing.drift_ns != 0]
    if drifts:
        stats.drift_mean_ns = statistics.mean(drifts)
        stats.drift_p99_ns = _percentile(sorted(drifts), 99)

    # Error distribution
    err_dist: Dict[str, int] = defaultdict(int)
    for r in results:
        if not r.success:
            kind = r.failure_kind.value if r.failure_kind else FailureKind.UNKNOWN.value
            err_dist[kind] += 1
    stats.error_distribution = dict(err_dist)

    return stats


def _percentile(sorted_data: List[float], p: float) -> float:
    if not sorted_data:
        return 0.0
    n = len(sorted_data)
    idx = (p / 100.0) * (n - 1)
    lo = int(idx)
    hi = min(lo + 1, n - 1)
    frac = idx - lo
    return sorted_data[lo] * (1 - frac) + sorted_data[hi] * frac


def _mad(sorted_data: List[float]) -> float:
    """Median Absolute Deviation."""
    if not sorted_data:
        return 0.0
    med = statistics.median(sorted_data)
    return statistics.median([abs(x - med) for x in sorted_data])


def _iqr(sorted_data: List[float]) -> float:
    """Interquartile range."""
    if len(sorted_data) < 4:
        return 0.0
    q1 = _percentile(sorted_data, 25)
    q3 = _percentile(sorted_data, 75)
    return q3 - q1


def _count_outliers(sorted_data: List[float], iqr: float) -> int:
    """Count values beyond 1.5 * IQR from Q1/Q3."""
    if not sorted_data or iqr == 0:
        return 0
    q1 = _percentile(sorted_data, 25)
    q3 = _percentile(sorted_data, 75)
    fence_lo = q1 - 1.5 * iqr
    fence_hi = q3 + 1.5 * iqr
    return sum(1 for x in sorted_data if x < fence_lo or x > fence_hi)
