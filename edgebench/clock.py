"""
edgebench.clock
~~~~~~~~~~~~~~~
Monotonic time abstraction.  All clock access in the execution path goes
through this interface so scheduling can be unit-tested, drift simulated,
and runs replayed without touching the OS clock.
"""
from __future__ import annotations

import time
from abc import ABC, abstractmethod
from typing import Optional


class Clock(ABC):
    @abstractmethod
    def now_ns(self) -> int:
        """Return current time as monotonic nanoseconds."""

    @abstractmethod
    def sleep(self, seconds: float) -> None:
        """Blocking sleep for *seconds*."""

    def sleep_ns(self, nanoseconds: int) -> None:
        self.sleep(nanoseconds / 1e9)


class MonotonicClock(Clock):
    """Real OS monotonic clock."""

    def now_ns(self) -> int:
        return time.monotonic_ns()

    def sleep(self, seconds: float) -> None:
        time.sleep(seconds)


class SimulatedClock(Clock):
    """
    Simulated clock for testing.  Time advances only when explicitly told to.
    """

    def __init__(self, start_ns: int = 0) -> None:
        self._now_ns = start_ns

    def now_ns(self) -> int:
        return self._now_ns

    def sleep(self, seconds: float) -> None:
        self._now_ns += int(seconds * 1e9)

    def advance_ns(self, delta_ns: int) -> None:
        self._now_ns += delta_ns

    def set_ns(self, ns: int) -> None:
        self._now_ns = ns


# Default singleton used throughout the library unless overridden.
_default_clock: Clock = MonotonicClock()


def get_clock() -> Clock:
    return _default_clock


def set_clock(clock: Clock) -> None:
    global _default_clock
    _default_clock = clock


# ---------------------------------------------------------------------------
# Precision sleep — yield-spin, not busy-spin
# ---------------------------------------------------------------------------

_SPIN_THRESHOLD_NS = 2_000_000  # 2 ms coarse handoff window


def precision_sleep(target_ns: int, clock: Optional[Clock] = None) -> tuple[int, int]:
    """
    Block until *target_ns* on the given clock.

    Two-phase:
    1. Coarse — ``clock.sleep()`` for the bulk of the wait (GIL-friendly).
    2. Fine   — yield-spin with ``time.sleep(0)`` to cap CPU burn while
                still hitting sub-millisecond accuracy.

    Returns:
        (awakened_ns, fired_ns) — both sampled from the clock after waking.
    """
    clk = clock or get_clock()
    remaining_ns = target_ns - clk.now_ns()
    if remaining_ns > _SPIN_THRESHOLD_NS:
        clk.sleep((remaining_ns - _SPIN_THRESHOLD_NS) / 1e9)

    awakened_ns = clk.now_ns()

    # yield-spin: releases GIL, keeps latency tight
    while clk.now_ns() < target_ns:
        time.sleep(0)

    fired_ns = clk.now_ns()
    return awakened_ns, fired_ns
