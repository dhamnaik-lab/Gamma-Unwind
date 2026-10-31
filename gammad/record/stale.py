"""One feed-packet staleness rule shared by the D274 recorder and T-368 grader."""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime

from gammad.config import IST

__all__ = ["arrival_ms", "is_stale", "is_stale_tick", "ist_date"]


def ist_date(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000, IST).date().isoformat()


def is_stale(ts_ms: int, arrival_date: str) -> bool:
    """D274: the tick's exchange last-trade DATE (IST) is earlier than the date it arrived on. ts_ms <= 0 = no
    last-trade time at all (every feed substitutes its receive time when a packet has none): not a stale quote."""
    return ts_ms > 0 and ist_date(ts_ms) < arrival_date


def arrival_ms(recv_ms: int | None, wall_ms: Callable[[], int]) -> int:
    """Use a positive receive time, calling the wall clock only as a fallback."""
    return recv_ms if recv_ms is not None and recv_ms > 0 else wall_ms()


def is_stale_tick(ts_ms: int, recv_ms: int | None, wall_ms: Callable[[], int]) -> bool:
    """Compare the trade's IST date with its arrival's IST date."""
    return is_stale(ts_ms, ist_date(arrival_ms(recv_ms, wall_ms)))
