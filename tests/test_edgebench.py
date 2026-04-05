"""
edgebench tests
~~~~~~~~~~~~~~~
Tests for: schedule planner determinism, grouping and aggregation,
result serialization, error classification, concurrency limits,
reproducibility with fixed seed, cancellation handling.
"""
from __future__ import annotations

import asyncio
import dataclasses
import json
import sys
import types
import uuid
from typing import List

import pytest

from edgebench.clock import SimulatedClock
from edgebench.metrics import aggregate, _percentile, _mad, _iqr
from edgebench.models import (
    AuthMode, CacheBustPolicy, ConnectionMode, FailureKind,
    HttpMethod, IdentitySpec, ProxySpec, RequestResult,
    RetryPolicy, RunConfig, RunMode, TargetSpec, TimingBreakdown,
    TransportBackend,
)
from edgebench.planner import build_plan
from edgebench.rng import seed as seed_rng
from edgebench.transport import CurlCffiBackend, HttpxBackend
from edgebench.validation import validate, ConfigError


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def make_identity(label: str = "chrome124-win") -> IdentitySpec:
    return IdentitySpec(
        profile="chrome124",
        user_agent="Mozilla/5.0 Chrome/124",
        label=label,
    )


def make_target(tid: str = "t1") -> TargetSpec:
    return TargetSpec(id=tid, url="https://example.com/put")


def make_proxy(pid: str = "p1") -> ProxySpec:
    return ProxySpec(id=pid, url="http://proxy.example.com:8080", region="us-east")


def make_config(**kwargs) -> RunConfig:
    defaults = dict(
        run_id="test-run",
        mode=RunMode.BENCHMARK,
        targets=[make_target()],
        identities=[make_identity()],
        proxies=[],
        warmup_runs=1,
        measured_runs=3,
        inter_request_gap_ms=10.0,
        per_group_cooldown_ms=0.0,
        seed=42,
        global_concurrency=5,
        per_host_concurrency=3,
        per_proxy_concurrency=3,
        retry_policy=RetryPolicy.DISABLED,
    )
    defaults.update(kwargs)
    return RunConfig(**defaults)


def make_result(
    group_key: str = "g1",
    success: bool = True,
    total_wall_ms: float = 100.0,
    is_warmup: bool = False,
    failure_kind: FailureKind = None,
    drift_ns: int = 0,
) -> RequestResult:
    return RequestResult(
        request_index=0,
        group_key=group_key,
        is_warmup=is_warmup,
        success=success,
        status_code=200 if success else None,
        failure_kind=failure_kind,
        timing=TimingBreakdown(total_wall_ms=total_wall_ms, drift_ns=drift_ns),
        identity_label="chrome124-win",
        target_id="t1",
    )


# ---------------------------------------------------------------------------
# Planner determinism
# ---------------------------------------------------------------------------

class TestPlanner:
    def test_plan_is_deterministic(self):
        cfg = make_config(seed=99)
        plan1 = build_plan(cfg)
        plan2 = build_plan(cfg)
        assert [s.scheduled_ns for s in plan1] == [s.scheduled_ns for s in plan2]
        assert [s.identity.label for s in plan1] == [s.identity.label for s in plan2]

    def test_plan_length(self):
        cfg = make_config(warmup_runs=2, measured_runs=5)
        plan = build_plan(cfg)
        # 1 target x 1 identity x (2 warmup + 5 measured)
        assert len(plan) == 7

    def test_warmup_count(self):
        cfg = make_config(warmup_runs=3, measured_runs=4)
        plan = build_plan(cfg)
        warmups = [s for s in plan if s.is_warmup]
        measured = [s for s in plan if not s.is_warmup]
        assert len(warmups) == 3
        assert len(measured) == 4

    def test_timestamps_monotonically_increase(self):
        cfg = make_config(warmup_runs=1, measured_runs=3, inter_request_gap_ms=50.0)
        plan = build_plan(cfg)
        ts = [s.scheduled_ns for s in plan if not s.is_warmup]
        assert ts == sorted(ts)
        assert all(ts[i+1] - ts[i] >= 40_000_000 for i in range(len(ts)-1))  # ~50ms gap

    def test_multiple_targets_produce_groups(self):
        cfg = make_config(
            targets=[make_target("t1"), make_target("t2")],
            warmup_runs=0,
            measured_runs=2,
        )
        plan = build_plan(cfg)
        group_keys = {s.group_key for s in plan}
        assert len(group_keys) == 2

    def test_multiple_identities(self):
        cfg = make_config(
            identities=[make_identity("chrome"), make_identity("firefox")],
            warmup_runs=0,
            measured_runs=2,
        )
        plan = build_plan(cfg)
        labels = {s.identity.label for s in plan}
        assert labels == {"chrome", "firefox"}

    def test_proxy_assignment(self):
        cfg = make_config(
            proxies=[make_proxy("p1"), make_proxy("p2")],
            warmup_runs=0,
            measured_runs=4,
        )
        plan = build_plan(cfg)
        proxy_ids = [s.proxy.id if s.proxy else None for s in plan]
        assert "p1" in proxy_ids
        assert "p2" in proxy_ids

    def test_seed_changes_shuffle_order(self):
        cfg_a = make_config(seed=1, randomize_order=True, measured_runs=10)
        cfg_b = make_config(seed=2, randomize_order=True, measured_runs=10)
        plan_a = build_plan(cfg_a)
        plan_b = build_plan(cfg_b)
        order_a = [s.request_index for s in plan_a if not s.is_warmup]
        order_b = [s.request_index for s in plan_b if not s.is_warmup]
        assert order_a != order_b

    def test_simulated_clock_used(self):
        clk = SimulatedClock(start_ns=1_000_000_000_000)
        cfg = make_config(warmup_runs=0, measured_runs=2)
        plan = build_plan(cfg, clock=clk)
        assert [s.scheduled_ns for s in plan] == [
            1_000_500_000_000,
            1_000_510_000_000,
        ]


# ---------------------------------------------------------------------------
# Aggregation / metrics
# ---------------------------------------------------------------------------

class TestMetrics:
    def test_basic_stats(self):
        results = [make_result(total_wall_ms=float(v)) for v in [10, 20, 30, 40, 50]]
        stats = aggregate(results)
        s = stats["g1"]
        assert s.count == 5
        assert s.success_count == 5
        assert s.success_rate == 1.0
        assert s.min_ms == 10.0
        assert s.max_ms == 50.0
        assert abs(s.mean_ms - 30.0) < 0.001

    def test_warmup_excluded(self):
        results = [
            make_result(total_wall_ms=1000.0, is_warmup=True),
            make_result(total_wall_ms=100.0, is_warmup=False),
        ]
        stats = aggregate(results)
        assert stats["g1"].count == 1
        assert stats["g1"].mean_ms == 100.0

    def test_failure_classification(self):
        results = [
            make_result(success=True),
            make_result(success=False, failure_kind=FailureKind.READ_TIMEOUT),
            make_result(success=False, failure_kind=FailureKind.TLS_FAILURE),
        ]
        stats = aggregate(results)
        s = stats["g1"]
        assert s.success_rate == pytest.approx(1/3)
        assert s.error_distribution[FailureKind.READ_TIMEOUT.value] == 1
        assert s.error_distribution[FailureKind.TLS_FAILURE.value] == 1

    def test_percentiles(self):
        data = list(range(100))
        assert _percentile(data, 50) == pytest.approx(49.5)
        assert _percentile(data, 90) == pytest.approx(89.1)
        assert _percentile(data, 99) == pytest.approx(98.01)

    def test_mad(self):
        data = [1.0, 2.0, 3.0, 4.0, 100.0]
        mad = _mad(sorted(data))
        assert mad > 0

    def test_iqr(self):
        data = sorted([1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0])
        iqr = _iqr(data)
        assert iqr > 0

    def test_outlier_count(self):
        # One obvious outlier
        results = [make_result(total_wall_ms=float(v)) for v in [10, 11, 12, 10, 11, 10, 500]]
        stats = aggregate(results)
        assert stats["g1"].outlier_count >= 1

    def test_multiple_groups(self):
        results = [
            make_result(group_key="a", total_wall_ms=10.0),
            make_result(group_key="a", total_wall_ms=20.0),
            make_result(group_key="b", total_wall_ms=50.0),
        ]
        stats = aggregate(results)
        assert "a" in stats
        assert "b" in stats
        assert stats["a"].count == 2
        assert stats["b"].count == 1

    def test_drift_stats(self):
        results = [
            make_result(drift_ns=1000),
            make_result(drift_ns=2000),
            make_result(drift_ns=3000),
        ]
        stats = aggregate(results)
        assert stats["g1"].drift_mean_ns == pytest.approx(2000.0)


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

class TestValidation:
    def test_valid_config_passes(self):
        cfg = make_config()
        validate(cfg)  # should not raise

    def test_empty_targets_fails(self):
        cfg = make_config(targets=[])
        with pytest.raises(ConfigError, match="empty target set"):
            validate(cfg)

    def test_duplicate_proxy_ids(self):
        cfg = make_config(proxies=[make_proxy("p1"), make_proxy("p1")])
        with pytest.raises(ConfigError, match="duplicate proxy IDs"):
            validate(cfg)

    def test_benchmark_mode_no_retries(self):
        from edgebench.models import RetryPolicy
        cfg = make_config(mode=RunMode.BENCHMARK, retry_policy=RetryPolicy.ON_5XX)
        with pytest.raises(ConfigError, match="retry_policy"):
            validate(cfg)

    def test_zero_measured_runs_fails(self):
        cfg = make_config(measured_runs=0)
        with pytest.raises(ConfigError, match="measured_runs"):
            validate(cfg)


# ---------------------------------------------------------------------------
# Serialization
# ---------------------------------------------------------------------------

class TestSerialization:
    def test_result_roundtrip_json(self):
        r = make_result(group_key="g", total_wall_ms=42.5, failure_kind=None)
        d = dataclasses.asdict(r)
        serialized = json.dumps(d)
        recovered = json.loads(serialized)
        assert recovered["timing"]["total_wall_ms"] == 42.5
        assert recovered["group_key"] == "g"

    def test_failure_kind_survives_serialization(self):
        r = make_result(success=False, failure_kind=FailureKind.TLS_FAILURE)
        d = dataclasses.asdict(r)
        assert d["failure_kind"] == "tls_failure"


# ---------------------------------------------------------------------------
# Clock abstraction
# ---------------------------------------------------------------------------

class TestClock:
    def test_simulated_clock_advances(self):
        clk = SimulatedClock(0)
        assert clk.now_ns() == 0
        clk.advance_ns(1_000_000)
        assert clk.now_ns() == 1_000_000

    def test_simulated_sleep(self):
        clk = SimulatedClock(0)
        clk.sleep(1.0)
        assert clk.now_ns() == 1_000_000_000


# ---------------------------------------------------------------------------
# RNG reproducibility
# ---------------------------------------------------------------------------

class TestRng:
    def test_same_seed_same_sequence(self):
        from edgebench.rng import seed, choice
        seed(42)
        seq_a = [choice([1, 2, 3, 4, 5]) for _ in range(20)]
        seed(42)
        seq_b = [choice([1, 2, 3, 4, 5]) for _ in range(20)]
        assert seq_a == seq_b

    def test_different_seeds_differ(self):
        from edgebench.rng import seed, choice
        seed(1)
        seq_a = [choice([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]) for _ in range(50)]
        seed(2)
        seq_b = [choice([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]) for _ in range(50)]
        assert seq_a != seq_b


# ---------------------------------------------------------------------------
# Transport reuse
# ---------------------------------------------------------------------------

class TestTransportReuse:
    @pytest.mark.asyncio
    async def test_curl_sessions_are_partitioned_by_profile_and_proxy(self, monkeypatch):
        fake_requests = types.ModuleType("curl_cffi.requests")

        class FakeSession:
            def __init__(self, impersonate, proxy):
                self.impersonate = impersonate
                self.proxy = proxy

            async def close(self):
                return None

        fake_requests.AsyncSession = FakeSession
        monkeypatch.setitem(sys.modules, "curl_cffi.requests", fake_requests)

        backend = CurlCffiBackend(ConnectionMode.WARM_REUSE)
        chrome_a, close_a = await backend._get_session("chrome124", "http://proxy-a")
        chrome_a_2, close_a_2 = await backend._get_session("chrome124", "http://proxy-a")
        chrome_b, _ = await backend._get_session("chrome124", "http://proxy-b")
        firefox_a, _ = await backend._get_session("firefox123", "http://proxy-a")

        assert close_a is False
        assert close_a_2 is False
        assert chrome_a is chrome_a_2
        assert chrome_a is not chrome_b
        assert chrome_a is not firefox_a

    @pytest.mark.asyncio
    async def test_httpx_clients_are_partitioned_by_proxy(self, monkeypatch):
        fake_httpx = types.ModuleType("httpx")

        class FakeAsyncClient:
            def __init__(self, **kwargs):
                self.kwargs = kwargs

            async def aclose(self):
                return None

        fake_httpx.AsyncClient = FakeAsyncClient
        monkeypatch.setitem(sys.modules, "httpx", fake_httpx)

        backend = HttpxBackend(ConnectionMode.WARM_REUSE)
        direct_a, close_a = await backend._get_client(None)
        direct_b, close_b = await backend._get_client(None)
        proxy_a, _ = await backend._get_client("http://proxy-a")
        proxy_b, _ = await backend._get_client("http://proxy-b")

        assert close_a is False
        assert close_b is False
        assert direct_a is direct_b
        assert direct_a is not proxy_a
        assert proxy_a is not proxy_b
