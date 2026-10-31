"""Pure missed-minute repair. Returned bars are tagged; callers decide how to apply them."""

from collections.abc import Callable, Iterable, Sequence
from datetime import date, datetime

from gammad.broker.base import Candle
from gammad.config import IST
from gammad.core.session_clock import fo_market_hours as _market_hours
from gammad.watch.bars import MINUTE_MS, Bar


def missing_minutes(
    have: Iterable[int], from_ms: int, to_ms: int, market_hours: Callable[[int], bool]
) -> list[int]:
    """Minute opens in the half-open range (including a forming minute)."""
    present = set(have)
    first = -(-from_ms // MINUTE_MS) * MINUTE_MS
    return [t for t in range(first, to_ms, MINUTE_MS) if t not in present and market_hours(t)]


def bars_from_candles(
    candles: Sequence[Candle], want: Sequence[int], *, interval_min: int, to_ms: int
) -> list[tuple[Bar, str]]:
    """Convert closed history only, carrying OI chronologically as OiCarry does.

    Leading candles without OI are skipped. Closed non-wanted candles may seed OI,
    but future candles cannot. Duplicate candle opens use the last supplied row.
    Larger candles are retrospective flat minute repairs, not intraminute observations.
    """
    if isinstance(interval_min, bool) or not isinstance(interval_min, int) or interval_min < 1:
        raise ValueError("interval_min must be a positive integer")
    span = interval_min * MINUTE_MS
    wanted = set(want)
    by_time = {c.ts_ms: c for c in candles if c.ts_ms + span <= to_ms}
    last_oi = None
    result = {}
    for start, c in sorted(by_time.items()):
        if c.oi is not None:
            last_oi = int(c.oi)
        if last_oi is None:
            continue
        for minute in range(start, start + span, MINUTE_MS):
            if minute not in wanted:
                continue
            o, h, lo = (c.open, c.high, c.low) if interval_min == 1 else (c.close,) * 3
            result[minute] = (Bar(
                minute, 1, float(o), float(h), float(lo), float(c.close),
                int(c.volume or 0) if minute + MINUTE_MS == start + span else 0,
                last_oi, ticks=0,
            ), "backfill")
    return [result[t] for t in sorted(result)]


def plan_backfill(
    gap_from_ms: int, gap_to_ms: int, *, max_minutes: int = 120
) -> tuple[date, date] | None:
    """Inclusive intraday request dates for short gaps overlapping weekday NSE hours.

    Uses the existing feed's weekday/session rule (no holiday calendar I/O).
    The limit measures elapsed time, including any out-of-session part of a gap.
    """
    if gap_to_ms <= gap_from_ms or gap_to_ms - gap_from_ms > max_minutes * MINUTE_MS:
        return None
    first = gap_from_ms // MINUTE_MS * MINUTE_MS
    inside = [t for t in range(first, gap_to_ms, MINUTE_MS) if _market_hours(t)]
    if not inside:
        return None
    return tuple(datetime.fromtimestamp(t / 1000, IST).date() for t in (inside[0], inside[-1]))
