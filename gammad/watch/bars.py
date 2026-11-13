"""Tick -> 1m / 5m / 15m OHLCV bars for one contract, plus the OI peak and indicator helpers.

Pure and synchronous: no asyncio, no clock reads. The caller feeds ticks (`on_tick`) and wall-clock
instants (`on_timer`); both return the bars they closed. Strategy spec Step 3; D6 (5m signal, 15m
display); D7 (OI baseline = TODAY's intraday peak, never the previous day's OI).

Rules, each pinned by tests/test_bars.py:
- Buckets align to the session open (09:15 IST): the first 5m bar is 09:15:00-09:19:59.999.
- Bar volume is the delta of the cumulative day volume. The FIRST tick of the day only seeds the
  baseline (it contributes 0): after a mid-day restart the cumulative figure is the whole morning,
  and dumping it into one bar would fake a volume build. A cumulative figure that goes DOWN re-seeds
  the baseline (contributes 0) rather than counting a spike.
- A bar closes on the first tick at/after its end, or when `on_timer(now)` passes end + grace, so a
  contract that stops trading still closes its bar. Empty intervals produce no bar (Dhan intraday
  candles do the same, so a replay of recorded data and of Dhan history line up).
- A tick stamped before the forming bar (a stale exchange LTT, or a tick that lands after the timer
  closed its bar) is folded into the current / next bar; a closed bar is never mutated.
- `oi_peak_today` = max OI over ticks stamped >= 09:15. The first OI seen in-session sets it;
  `prev_oi` and pre-open OI are never the peak (D7).
"""

from __future__ import annotations

import math
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, replace
from datetime import date, datetime, time
from typing import TYPE_CHECKING

import numpy as np

from gammad.config import IST
from gammad.core import session_clock
from gammad.core.zones import atr as _pine_atr
from gammad.log import get_logger

if TYPE_CHECKING:
    from gammad.watch.feed import Tick

log = get_logger(__name__)

MINUTE_MS = 60_000
DEFAULT_TFS: tuple[int, ...] = (1, 5, 15)
SESSION_OPEN = time(9, 15)
SESSION_CLOSE = time(15, 30)
# pre-CAS F&O close; backtest/data_sources/dhan_history.py:74 imports it (pinned D343); no default reads it
# 0.3 s (was 1.5 s; D85 "an implementation change, replay-tested"): a bar that no tick closes is closed by the
# timer this long after its end. A tick-closed bar never depends on it (tests/test_s130_grace.py replays that).
DEFAULT_TIMER_GRACE_MS = 300


def next_close_deadline(
    now_ms: int,
    trade_date: date,
    tf_min: int = 5,
    grace_ms: int = DEFAULT_TIMER_GRACE_MS,
    *,
    session_open: time = SESSION_OPEN,
    session_close: time | None = None,
) -> int | None:
    """The first instant >= now_ms at which on_timer() would close a `tf_min` bar: the next 09:15-aligned bar
    END (up to and including the session close) + grace_ms. For the watcher's timer (PLAN-W12 §2 item 5): sleep
    until this instead of polling every TIMER_EVERY_S. Before the open -> the first bar's end + grace; after the
    session close + grace -> None (nothing left to close today). Pure."""
    if tf_min < 1:
        raise ValueError(f"tf_min must be a positive number of minutes, got {tf_min}")
    open_ms, close_ms = session_bounds_ms(trade_date, session_open, session_close)
    span = tf_min * MINUTE_MS
    if now_ms > close_ms + grace_ms:
        return None
    if now_ms < open_ms + span + grace_ms:
        return open_ms + span + grace_ms
    k = (now_ms - grace_ms - open_ms) // span  # bar ends passed so far (end k*span has deadline <= now)
    end = open_ms + k * span
    deadline = end + grace_ms
    if deadline < now_ms:
        end += span
        deadline = end + grace_ms
    return min(deadline, close_ms + grace_ms)


@dataclass(slots=True)
class Bar:
    start_ms: int
    tf_min: int
    open: float
    high: float
    low: float
    close: float
    volume: int
    oi: int | None
    ticks: int = 1

    @property
    def end_ms(self) -> int:
        return self.start_ms + self.tf_min * MINUTE_MS


@dataclass(frozen=True, slots=True)
class BarClose:
    security_id: int
    tf_min: int
    bar: Bar
    reason: str  # "tick" | "timer" | "session_end"


def session_bounds_ms(
    trade_date: date, open_t: time = SESSION_OPEN, close_t: time | None = None
) -> tuple[int, int]:
    if close_t is None:
        close_t = session_clock.fo_window(trade_date)[1]
    o = datetime.combine(trade_date, open_t, tzinfo=IST)
    c = datetime.combine(trade_date, close_t, tzinfo=IST)
    return int(o.timestamp() * 1000), int(c.timestamp() * 1000)


class BarBuilder:
    """Bars for ONE security_id on ONE trade date."""

    def __init__(
        self,
        security_id: int,
        trade_date: date,
        tfs: Sequence[int] = DEFAULT_TFS,
        *,
        session_open: time = SESSION_OPEN,
        session_close: time | None = None,
        timer_grace_ms: int = DEFAULT_TIMER_GRACE_MS,
        quiet: bool = False,
    ) -> None:
        if not tfs or any(tf < 1 for tf in tfs):
            raise ValueError(f"timeframes must be positive minutes, got {tfs!r}")
        self.security_id = security_id
        self.trade_date = trade_date
        self.tfs: tuple[int, ...] = tuple(sorted(set(tfs)))
        self.open_ms, self.close_ms = session_bounds_ms(trade_date, session_open, session_close)
        self.timer_grace_ms = timer_grace_ms
        self._closed: dict[int, list[Bar]] = {tf: [] for tf in self.tfs}
        self._forming: dict[int, Bar | None] = dict.fromkeys(self.tfs)
        # Earliest start a new bar may take (end of the last closed bar) -- no bar is ever reopened.
        self._floor: dict[int, int] = dict.fromkeys(self.tfs, self.open_ms)
        self._pending_vol: dict[int, int] = dict.fromkeys(self.tfs, 0)
        self._cum_volume: int | None = None
        self.oi_peak_today: int | None = None
        self.oi_now: int | None = None
        self.prev_oi: int | None = None
        self.prev_close: float | None = None
        self.last_ltp: float | None = None
        self.exchange_oi_day_high: int | None = None  # pass-through only; NOT used for the peak (see report)
        self.volume_reseeds = 0
        self.quiet = quiet  # a REPLAY of recorded ticks (api/recorded_bars.py) counts re-seeds, it does not log them

    # ------------------------------------------------------------------ ingest
    def on_tick(self, tick: Tick) -> list[BarClose]:
        if tick.security_id != self.security_id:
            raise ValueError(f"tick for {tick.security_id} fed to builder for {self.security_id}")
        if tick.prev_oi is not None:
            self.prev_oi = tick.prev_oi
        if tick.prev_close is not None:
            self.prev_close = tick.prev_close
        if tick.oi_day_high is not None:
            self.exchange_oi_day_high = tick.oi_day_high
        if tick.oi is not None:
            self.oi_now = tick.oi
        if tick.ltp is not None and tick.ltp > 0:
            self.last_ltp = tick.ltp

        delta = self._volume_delta(tick.volume_day)
        ts = tick.ts_ms
        if ts < self.open_ms:
            return []  # pre-open: seeds the volume baseline and oi_now, never a bar or the peak
        if ts >= self.close_ms:
            return self._close_all("session_end")

        if (
            tick.oi is not None
            and tick.oi > 0
            and (self.oi_peak_today is None or tick.oi > self.oi_peak_today)
        ):
            self.oi_peak_today = tick.oi

        closed: list[BarClose] = []
        for tf in self.tfs:
            ev = self._apply(tf, ts, tick.ltp, delta, tick.oi)
            if ev is not None:
                closed.append(ev)
        return closed

    def on_timer(self, now_ms: int) -> list[BarClose]:
        """Close every forming bar whose end + grace has passed. Call it every second or so."""
        if now_ms >= self.close_ms + self.timer_grace_ms:
            return self._close_all("session_end")
        out: list[BarClose] = []
        for tf in self.tfs:
            bar = self._forming[tf]
            if bar is not None and now_ms >= bar.end_ms + self.timer_grace_ms:
                out.append(self._close(tf, "timer"))
        return out

    # ------------------------------------------------------------------ read
    def series(self, tf: int) -> list[Bar]:
        """Closed bars for a timeframe, oldest first (a copy of the list; do not mutate the bars)."""
        return list(self._closed[tf])

    def forming_bar(self, tf: int) -> Bar | None:
        bar = self._forming[tf]
        return None if bar is None else replace(bar)

    # ------------------------------------------------------------------ internals
    def _volume_delta(self, cum: int | None) -> int:
        if cum is None:
            return 0
        if self._cum_volume is None:
            self._cum_volume = cum  # first tick of the day seeds
            return 0
        if cum < self._cum_volume:
            self.volume_reseeds += 1
            if not self.quiet:
                log.warning(
                    "sec %s cumulative volume went down %s -> %s; re-seeding",
                    self.security_id,
                    self._cum_volume,
                    cum,
                )
            self._cum_volume = cum
            return 0
        d = cum - self._cum_volume
        self._cum_volume = cum
        return d

    def _bucket(self, tf: int, ts: int) -> int:
        span = tf * MINUTE_MS
        return self.open_ms + ((ts - self.open_ms) // span) * span

    def _apply(self, tf: int, ts: int, ltp: float | None, delta: int, oi: int | None) -> BarClose | None:
        start = self._bucket(tf, ts)
        closed: BarClose | None = None
        bar = self._forming[tf]
        if bar is not None and start >= bar.end_ms:
            closed = self._close(tf, "tick")
            bar = None
        floor = bar.start_ms if bar is not None else self._floor[tf]
        start = max(start, floor)  # late / stale-stamped tick: fold forward, never reopen a closed bar
        if start >= self.close_ms:
            return closed  # folding forward would open a bar past the session end: drop it for this tf
        has_price = ltp is not None and ltp > 0
        if bar is None:
            if not has_price:
                if delta > 0:
                    self._pending_vol[tf] += delta
                return closed  # an OI-only update with no price cannot open a bar; preserve delta for next priced tick
            assert ltp is not None
            prev = self._closed[tf][-1].oi if self._closed[tf] else None
            total_vol = delta + self._pending_vol.pop(tf, 0)
            self._pending_vol[tf] = 0
            self._forming[tf] = Bar(start, tf, ltp, ltp, ltp, ltp, total_vol, oi if oi is not None else prev)
            return closed
        if has_price:
            assert ltp is not None
            bar.high = max(bar.high, ltp)
            bar.low = min(bar.low, ltp)
            bar.close = ltp
        bar.volume += delta
        if oi is not None:
            bar.oi = oi
        bar.ticks += 1
        return closed

    def _close(self, tf: int, reason: str) -> BarClose:
        bar = self._forming[tf]
        assert bar is not None
        self._closed[tf].append(bar)
        self._forming[tf] = None
        self._floor[tf] = bar.end_ms
        return BarClose(self.security_id, tf, bar, reason)

    def _close_all(self, reason: str) -> list[BarClose]:
        return [self._close(tf, reason) for tf in self.tfs if self._forming[tf] is not None]


# ---------------------------------------------------------------------- indicators
def sma(values: Sequence[float], n: int) -> float | None:
    """Mean of the last n values; None until n values exist."""
    if n < 1 or len(values) < n:
        return None
    return math.fsum(values[-n:]) / n


def atr(bars: Iterable[Bar], n: int) -> float | None:
    """Pine ta.atr(n) (RMA of true range, SMA-seeded) at the LAST bar; None until n bars exist.

    Delegates to gammad.core.zones.atr so the daily zones and the intraday watcher share one ATR.
    """
    bl = list(bars)
    if n < 1 or len(bl) < n:
        return None
    h = np.array([b.high for b in bl], dtype=float)
    lo = np.array([b.low for b in bl], dtype=float)
    c = np.array([b.close for b in bl], dtype=float)
    v = float(_pine_atr(h, lo, c, n)[-1])
    return None if math.isnan(v) else v
