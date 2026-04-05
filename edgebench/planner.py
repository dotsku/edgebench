"""
edgebench.planner
~~~~~~~~~~~~~~~~~
Generates an immutable list of RequestSpec objects before any execution.
The runner executes a plan; it does not invent behaviour mid-flight.
All randomness is applied here using the seeded RNG, not inline.
"""
from __future__ import annotations

from typing import List, Optional

from .clock import Clock, get_clock
from .models import (
    IdentitySpec,
    ProxySpec,
    RequestSpec,
    RunConfig,
    TargetSpec,
)
from .rng import choice, shuffle, seed as seed_rng


def build_plan(cfg: RunConfig, clock: Optional[Clock] = None) -> List[RequestSpec]:
    """
    Generate a fully-resolved, ordered list of RequestSpec for *cfg*.

    Identity and proxy assignments are computed here using the seeded RNG so
    the plan is reproducible from config alone.  The runner never calls
    random.choice().

    Schedule
    --------
    For each (target x identity x proxy) group:
      - warmup_runs warmup requests
      - measured_runs measured requests
    Spaced by inter_request_gap_ms.
    If randomize_order=True the measured (non-warmup) requests are shuffled,
    preserving warmup ordering.
    """
    seed_rng(cfg.seed)
    clk = clock or get_clock()

    proxies: List[Optional[ProxySpec]] = list(cfg.proxies) if cfg.proxies else [None]
    idx = 0
    gap_ns = int(cfg.inter_request_gap_ms * 1_000_000)
    cooldown_ns = int(cfg.per_group_cooldown_ms * 1_000_000)
    if cfg.schedule_start_ns is None:
        cfg.schedule_start_ns = clk.now_ns() + 500_000_000
    cursor_ns = cfg.schedule_start_ns

    # Build warmup + measured per group
    warmup_specs: List[RequestSpec] = []
    measured_specs: List[RequestSpec] = []

    for target in cfg.targets:
        for identity in cfg.identities:
            proxy_cycle_idx = 0
            for run_n in range(cfg.warmup_runs + cfg.measured_runs):
                is_warmup = run_n < cfg.warmup_runs
                proxy = proxies[proxy_cycle_idx % len(proxies)]
                proxy_cycle_idx += 1

                group_key = _group_key(target, identity, proxy)
                spec = RequestSpec(
                    request_index=idx,
                    target=target,
                    identity=identity,
                    proxy=proxy,
                    scheduled_ns=cursor_ns,
                    group_key=group_key,
                    is_warmup=is_warmup,
                )
                if is_warmup:
                    warmup_specs.append(spec)
                else:
                    measured_specs.append(spec)

                cursor_ns += gap_ns
                idx += 1

            cursor_ns += cooldown_ns  # cooldown between groups

    if cfg.randomize_order:
        measured_specs = shuffle(measured_specs)
        # Re-assign timestamps in shuffled order
        measured_specs = _reassign_timestamps(measured_specs, cursor_ns, gap_ns)

    return warmup_specs + measured_specs


def _group_key(
    target: TargetSpec,
    identity: IdentitySpec,
    proxy: Optional[ProxySpec],
) -> str:
    proxy_part = proxy.id if proxy else "direct"
    return f"{target.id}::{identity.label}::{proxy_part}"


def _reassign_timestamps(
    specs: List[RequestSpec],
    start_ns: int,
    gap_ns: int,
) -> List[RequestSpec]:
    """Return new RequestSpec list with monotonically increasing timestamps."""
    result = []
    cursor = start_ns
    for spec in specs:
        result.append(RequestSpec(
            request_index=spec.request_index,
            target=spec.target,
            identity=spec.identity,
            proxy=spec.proxy,
            scheduled_ns=cursor,
            group_key=spec.group_key,
            is_warmup=spec.is_warmup,
        ))
        cursor += gap_ns
    return result
