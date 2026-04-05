"""
edgebench.scheduler
~~~~~~~~~~~~~~~~~~~
Two-phase async scheduler.  Never blocks the event loop.

Phase 1 (coarse): await asyncio.sleep() — hands control back to the loop.
Phase 2 (fine):   dedicated thread runs precision_sleep() so the event loop
                  is not blocked during the spin window.
"""
from __future__ import annotations

import asyncio
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

from .clock import Clock, get_clock, precision_sleep
from .models import TimingBreakdown


# One shared executor for fine-sleep threads.
# Workers spin for at most 2 ms so thread count stays small.
_fine_executor = ThreadPoolExecutor(max_workers=32, thread_name_prefix="eb-fine-sleep")

_COARSE_HANDOFF_NS = 2_000_000  # hand off to thread 2 ms before target


async def schedule_fire(
    target_ns: int,
    clock: Optional[Clock] = None,
) -> TimingBreakdown:
    """
    Async-friendly two-phase wait until *target_ns*.

    Returns a TimingBreakdown with scheduled_ns, awakened_ns, fired_ns,
    drift_ns and scheduler_drift_ms filled in.
    The caller should copy remaining fields (connect, tls, etc.) after the
    actual HTTP call.
    """
    clk = clock or get_clock()
    timing = TimingBreakdown(scheduled_ns=target_ns)

    # ── Phase 1: coarse async sleep ──────────────────────────────────────
    now = clk.now_ns()
    coarse_target = target_ns - _COARSE_HANDOFF_NS
    coarse_wait_s = (coarse_target - now) / 1e9
    if coarse_wait_s > 0:
        await asyncio.sleep(coarse_wait_s)

    # ── Phase 2: precision sleep in a thread (non-blocking to event loop) ─
    loop = asyncio.get_running_loop()
    awakened_ns, fired_ns = await loop.run_in_executor(
        _fine_executor,
        _thread_precision_sleep,
        target_ns,
        clk,
    )

    timing.awakened_ns = awakened_ns
    timing.fired_ns = fired_ns
    timing.drift_ns = fired_ns - target_ns
    timing.scheduler_drift_ms = timing.drift_ns / 1e6

    return timing


def _thread_precision_sleep(target_ns: int, clock: Clock) -> tuple[int, int]:
    """Runs in a thread pool. Returns (awakened_ns, fired_ns)."""
    return precision_sleep(target_ns, clock)
