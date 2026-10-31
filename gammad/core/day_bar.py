"""T-221 (D195(c)+(d)): a trade date's DAILY bar built from that day's intraday candles. Broker-neutral.

Why: Dhan publishes the day's daily candle late (missing at 16:00 and at 22:01 for all 210 stocks on 28 Sep), so a
ref="close" mark_levels run saw bars through D-1 only: ref_price = the PREVIOUS session's close and the zones missed
the day's bar. The same day's 5-min bars ARE served after hours (live log 30 Sep 21:45: "50/50 underlyings have
today's 5-min bars"), so the missing bar is aggregated from them.

Conventions (checked 2026-10-03, not assumed):
  * a DAILY candle is stamped at IST MIDNIGHT of its date (Dhan: the 28 Sep cassette's GODREJCP bars read
    2026-09-25T00:00:00+05:30; Angel / Nuvama wire docs say the same), so the synthetic bar carries
    `day_ts_ms(day)` and `gammad.broker.dhan.candle_date(bar) == day`, sorting after D-1's bar;
  * an INTRADAY candle is stamped at its bar OPEN, true epoch (live Dhan 5-min bars of 25 Sep span 09:15..15:25 IST;
    app.py's underlying seed reads them the same way). Only bars opening inside the era's F&O session of `day`
    are aggregated, so a pre-open (09:00-09:08) or post-close row a broker may return never moves the bar.

T-217 (first-login preview) reuses `with_day_bar`.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from datetime import date, datetime, time
from typing import Literal

from gammad.broker.base import BrokerAdapter, Candle
from gammad.config import IST
from gammad.core import session_clock

SESSION_OPEN = time(9, 15)  # IST, inclusive (bar OPEN time)
DAY_BAR_INTERVAL_MIN = 5

# "daily" = the broker's daily series already holds `day`; "intraday" = the bar was aggregated from intraday candles;
# "missing" = the intraday reply held no in-session candle of `day` (a holiday, or the broker has nothing yet)
DayBarHow = Literal["daily", "intraday", "missing"]


def ist_date(ts_ms: int) -> date:
    return datetime.fromtimestamp(ts_ms / 1000, IST).date()


def day_ts_ms(day: date) -> int:
    """IST midnight of `day`, epoch ms: the stamp a broker's DAILY candle carries (module doc)."""
    return int(datetime.combine(day, time(0, 0), tzinfo=IST).timestamp() * 1000)


def _in_session(ts_ms: int, day: date) -> bool:
    dt = datetime.fromtimestamp(ts_ms / 1000, IST)
    return dt.date() == day and SESSION_OPEN <= dt.time() < session_clock.fo_window(day)[1]


def aggregate_day_bar(candles: Iterable[Candle], day: date) -> Candle | None:
    """`day`'s daily bar from intraday candles: open = the first in-session candle's open, high = max high, low = min
    low, close = the last candle's close, volume = sum; stamped `day_ts_ms(day)`. Candles of another IST date or
    outside the era's F&O session are ignored; None when none is left. Order of the input does not matter."""
    keep = sorted((c for c in candles if _in_session(c.ts_ms, day)), key=lambda c: c.ts_ms)
    if not keep:
        return None
    return Candle(
        ts_ms=day_ts_ms(day),
        open=keep[0].open,
        high=max(c.high for c in keep),
        low=min(c.low for c in keep),
        close=keep[-1].close,
        volume=sum(c.volume for c in keep),
    )


def with_day_bar(
    adapter: BrokerAdapter,
    security_id: int,
    bars: Sequence[Candle],
    day: date,
    exchange_segment: str = "NSE_EQ",
    instrument: str = "EQUITY",
) -> tuple[list[Candle], DayBarHow]:
    """`bars` (daily, oldest first, none after `day`) completed with `day`'s bar when the newest one is older.

    One `adapter.intraday(..., DAY_BAR_INTERVAL_MIN, day, day, oi=False)` call, made ONLY when the bar is missing.
    Broker errors propagate (the caller decides: mark_levels keeps the daily bars on a non-auth error)."""
    out = list(bars)
    if out and ist_date(out[-1].ts_ms) >= day:
        return out, "daily"
    candles = adapter.intraday(
        security_id, exchange_segment, DAY_BAR_INTERVAL_MIN, day, day, oi=False, instrument=instrument
    )
    bar = aggregate_day_bar(candles, day)
    if bar is None:
        return out, "missing"
    return [*out, bar], "intraday"
