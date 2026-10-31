"""Watcher-side shapes -> engine shapes (`gammad.watch.types`).

`bars.Bar` (start_ms, int volume, oi possibly None) and broker `Candle`s (warm-up history) both become
`types.Bar` (ts_ms = bar OPEN time, float volume, float oi). The engine never sees a bar without OI:

- oi None -> carry the last OI known for that security_id (a live bar, a candle, or `prime()`).
- oi None and nothing known yet -> the bar is SKIPPED for the engine (returns None) and that is
  logged ONCE per security_id. Showing a made-up OI (0) on the dashboard would be worse than a gap.

`ContractBars` is the per-contract closed-5m series the engine is called with: the warm-up seed from
the previous session (D24) followed by today's closed bars. `ContractEngine` keeps no bars of its own
(it is handed the series on every call), so "pre-loading the engine" means seeding this series.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from typing import TYPE_CHECKING

from gammad.log import get_logger
from gammad.watch import bars as wbars
from gammad.watch.types import Bar, TickView

if TYPE_CHECKING:
    from gammad.broker.base import Candle
    from gammad.watch.feed import Tick

log = get_logger(__name__)


class OiCarry:
    """Last known OI per security_id, and the conversions that use it."""

    def __init__(self) -> None:
        self._last: dict[int, float] = {}
        self._warned: set[int] = set()
        self.skipped: dict[int, int] = {}

    def last(self, security_id: int) -> float | None:
        return self._last.get(security_id)

    def prime(self, security_id: int, oi: float | None) -> None:
        if oi is not None:
            self._last[security_id] = float(oi)

    def forget(self) -> None:
        """New trade date: drop everything (the next day's warm-up primes again)."""
        self._last.clear()
        self._warned.clear()
        self.skipped.clear()

    def _resolve(self, security_id: int, oi: float | None, ts_ms: int, what: str) -> float | None:
        if oi is not None:
            self._last[security_id] = float(oi)
            return float(oi)
        carried = self._last.get(security_id)
        if carried is not None:
            return carried
        self.skipped[security_id] = self.skipped.get(security_id, 0) + 1
        if security_id not in self._warned:
            self._warned.add(security_id)
            log.warning(
                "adapter: sec %s %s at %d has no OI and none is known yet; skipped for the engine "
                "(logged once per contract)",
                security_id,
                what,
                ts_ms,
            )
        return None

    def bar(self, security_id: int, bar: wbars.Bar) -> Bar | None:
        oi = self._resolve(security_id, None if bar.oi is None else float(bar.oi), bar.start_ms, "bar")
        if oi is None:
            return None
        return Bar(
            ts_ms=int(bar.start_ms),
            open=float(bar.open),
            high=float(bar.high),
            low=float(bar.low),
            close=float(bar.close),
            volume=float(bar.volume),
            oi=oi,
        )

    def candle(self, security_id: int, candle: Candle) -> Bar | None:
        oi = self._resolve(security_id, candle.oi, candle.ts_ms, "candle")
        if oi is None:
            return None
        return Bar(
            ts_ms=int(candle.ts_ms),
            open=float(candle.open),
            high=float(candle.high),
            low=float(candle.low),
            close=float(candle.close),
            volume=float(candle.volume),
            oi=oi,
        )

    def candles(self, security_id: int, candles: Iterable[Candle]) -> list[Bar]:
        out: list[Bar] = []
        for c in candles:
            b = self.candle(security_id, c)
            if b is not None:
                out.append(b)
        return out

    def tick(self, tick: Tick) -> TickView | None:
        """Feed tick -> engine tick. None when the tick has no price or no OI can be resolved."""
        if tick.ltp is None or tick.ltp <= 0:
            return None
        oi = self._resolve(tick.security_id, None if tick.oi is None else float(tick.oi), tick.ts_ms, "tick")
        if oi is None:
            return None
        return TickView(
            ts_ms=int(tick.ts_ms),
            recv_ms=int(tick.recv_ms),
            ltp=float(tick.ltp),
            volume_day=float(tick.volume_day or 0),
            oi=oi,
        )


class ContractBars:
    """Closed 5m engine bars for ONE contract: previous-session seed + today's closed bars."""

    def __init__(self, security_id: int) -> None:
        self.security_id = security_id
        self._seed: list[Bar] = []
        self._today: list[Bar] = []

    def seed(self, bars: Sequence[Bar]) -> None:
        """Replace the warm-up seed. Seed bars must be older than any bar of today."""
        first_today = self._today[0].ts_ms if self._today else None
        kept = [b for b in bars if first_today is None or b.ts_ms < first_today]
        self._seed = sorted(kept, key=lambda b: b.ts_ms)

    def append(self, bar: Bar) -> None:
        last = self._today[-1].ts_ms if self._today else (self._seed[-1].ts_ms if self._seed else None)
        if last is not None and bar.ts_ms <= last:
            log.warning("adapter: sec %s bar %d not after %d; ignored", self.security_id, bar.ts_ms, last)
            return
        self._today.append(bar)

    @property
    def n_seed(self) -> int:
        return len(self._seed)

    @property
    def n_today(self) -> int:
        return len(self._today)

    def closed(self) -> list[Bar]:
        return [*self._seed, *self._today]

    def today(self) -> list[Bar]:
        return list(self._today)


def to_engine_bar(bar: wbars.Bar, last_oi: float | None) -> Bar | None:
    """Stateless form of OiCarry.bar for one-off conversions (backtest, tests)."""
    oi = float(bar.oi) if bar.oi is not None else last_oi
    if oi is None:
        return None
    return Bar(int(bar.start_ms), float(bar.open), float(bar.high), float(bar.low), float(bar.close),
               float(bar.volume), oi)
