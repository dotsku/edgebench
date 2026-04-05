"""
edgebench.rng
~~~~~~~~~~~~~
All randomness in edgebench flows through this module.
Callers never call random.choice() directly.
"""
from __future__ import annotations

import random
from typing import Optional, Sequence, TypeVar

T = TypeVar("T")

_rng: random.Random = random.Random(42)


def seed(s: Optional[int]) -> None:
    """Re-seed the global RNG.  Call once at startup from RunConfig.seed."""
    _rng.seed(s)


def choice(seq: Sequence[T]) -> T:
    return _rng.choice(seq)  # type: ignore[arg-type]


def shuffle(seq: list) -> list:
    copy = list(seq)
    _rng.shuffle(copy)
    return copy


def get_rng() -> random.Random:
    return _rng
