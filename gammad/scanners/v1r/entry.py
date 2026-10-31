"""Prefix-only entry rules, independently expressed from the exact specification."""

from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import UTC, datetime, time
from math import isfinite


@dataclass(frozen=True)
class OptBar:
    start_ms: int
    open: float
    high: float
    low: float
    close: float
    volume: int | None
    oi: int | None


@dataclass(frozen=True)
class StockBar:
    start_ms: int
    high: float
    low: float
    close: float
    volume: int | None


@dataclass(frozen=True)
class V1rParams:
    oi_drop_alert: float = 0.08
    oi_drop_tolerance: float = 0.01
    carry_oi_drop: float = 0.05
    carry_unwind_frac: float = 0.25
    premium_jump: float = 0.04
    sma_len: int = 20
    atr_len: int = 14
    c2_max: int = 12
    c2_min: int = 4
    nr_lookback: int = 6
    c3_window: int = 3
    stock_back_bars: int = 3
    max_entries_per_stock: int = 2
    pct_stop: float = 0.40
    carry_cap: int = 120
    expiry_dte_min: int = 1
    expiry_dte_max: int = 4
    eps: float = 1e-9


def ist_stamp(ms):
    return datetime.fromtimestamp((ms + 19_800_000) / 1000, UTC)


def vol_sma(bars, i, p):
    if bars[i].volume is None or bars[i].volume <= 0:
        return None
    values = [b.volume for b in bars[: i + 1] if b.volume is not None and b.volume > 0][-p.sma_len :]
    return sum(values) / p.sma_len if len(values) == p.sma_len else None


def cond_c(bars, i, p):
    indices = range(max(0, i - 2), i + 1)
    return c_from([bars[j].volume for j in indices], [vol_sma(bars, j, p) for j in indices], p)


def c_from(vols, smas, p):
    return vols[-1] is not None and vols[-1] > 0 and any(
        avg is not None and v is not None and v > avg + p.eps for v, avg in zip(vols, smas)
    )


def atr_series(bars, p):
    out = [None] * len(bars)
    ranges = []
    for i, b in enumerate(bars):
        ranges.append(
            max(b.high - b.low, abs(b.high - bars[i - 1].close), abs(b.low - bars[i - 1].close))
            if i
            else b.high - b.low
        )
        if i == p.atr_len - 1:
            out[i] = sum(ranges) / p.atr_len
        elif i >= p.atr_len:
            out[i] = ranges[i] / p.atr_len + (1 - 1 / p.atr_len) * out[i - 1]
    return out


def cond_c2(bars, i, p, atr):
    if i < p.atr_len or atr[i - 1] is None or not isfinite(atr[i - 1]) or atr[i - 1] <= 0:
        return False
    for n in range(min(p.c2_max, i), p.c2_min - 1, -1):
        window = bars[i - n : i]
        hi, lo = max(b.high for b in window), min(b.low for b in window)
        if hi - lo <= atr[i - 1] + p.eps:
            return bars[i].close > hi + p.eps
    return False


def cond_c3(bars, i, p):
    for back in range(1, p.c3_window + 1):
        j = i - back
        if j >= p.nr_lookback:
            b = bars[j]
            if (
                all(b.high - b.low < x.high - x.low for x in bars[j - p.nr_lookback : j])
                and bars[i].close > b.high + p.eps
            ):
                return True
    return False


def cond_d(c2, c3):
    return c2 or c3


def day_oi_peak(bars, i):
    day = ist_stamp(bars[i].start_ms).date()
    return max(
        (b.oi for b in bars[: i + 1] if b.oi is not None and ist_stamp(b.start_ms).date() == day),
        default=None,
    )


def cond_a_fresh(bars, i, p):
    return a_fresh_from(day_oi_peak(bars, i), bars[i].oi, p)


def a_fresh_from(peak, oi, p):
    return (
        oi is not None
        and peak is not None
        and peak > 0
        and ((peak - oi) / peak >= p.oi_drop_alert - p.oi_drop_tolerance - p.eps)
    )


def cond_a_carry(bars, i, p, build_low, peak_prior):
    return a_carry_from(day_oi_peak(bars, i), bars[i].oi, p, build_low, peak_prior)


def a_carry_from(peak, oi, p, build_low, peak_prior):
    if oi is None or peak is None or peak <= 0 or build_low is None:
        return False
    build_peak = max(peak_prior or 0, peak)
    added = build_peak - build_low
    return (
        (peak - oi) / peak >= p.carry_oi_drop - p.eps
        and added > 0
        and build_peak - oi >= p.carry_unwind_frac * added - p.eps
    )


def cond_b(bars, i, p):
    return i >= 1 and bars[i].close > bars[i - 1].close * (1 + p.premium_jump) + p.eps


def session_vwap(sbars, j, open_at: time):
    day = ist_stamp(sbars[j].start_ms).date()
    selected = [
        b
        for b in sbars[: j + 1]
        if ist_stamp(b.start_ms).date() == day and ist_stamp(b.start_ms).time() >= open_at
        and b.volume is not None and b.volume > 0
    ]
    weight = sum(b.volume for b in selected)
    return sum((b.high + b.low + b.close) / 3 * b.volume for b in selected) / weight if weight else None


def stock_confirms(sbars, j, side, p, open_at: time):
    if j < p.stock_back_bars:
        return False
    vwap = session_vwap(sbars, j, open_at)
    return confirms_from(sbars[j].close, sbars[j - p.stock_back_bars].close, vwap, side, p)


def confirms_from(close, prior, vwap, side, p):
    if vwap is None:
        return False
    if side == "CE":
        return close > vwap + p.eps and close > prior + p.eps
    return close < vwap - p.eps and close < prior - p.eps


class OptSeries:
    """Append-only prefix values; work per push is independent of history length."""

    def __init__(self, p: V1rParams):
        self.p = p
        self.bars, self.atr, self.sma, self.day_peak = [], [], [], []
        self._volumes = deque(maxlen=p.sma_len)
        self._first_trs = []

    def push(self, bar) -> bool:
        if self.bars and bar.start_ms <= self.bars[-1].start_ms:
            return False
        p, i = self.p, len(self.bars)
        previous = self.bars[-1] if i else None
        tr = (max(bar.high - bar.low, abs(bar.high - previous.close), abs(bar.low - previous.close))
              if previous is not None else bar.high - bar.low)
        atr = None
        if i < p.atr_len:
            self._first_trs.append(tr)
            if i == p.atr_len - 1:
                atr = sum(self._first_trs) / p.atr_len
        else:
            atr = tr / p.atr_len + (1 - 1 / p.atr_len) * self.atr[-1]
        sma = None
        if bar.volume is not None and bar.volume > 0:
            self._volumes.append(bar.volume)
            if len(self._volumes) == p.sma_len:
                # Python's compensated sum must see the same ordered window as vol_sma.
                sma = sum(self._volumes) / p.sma_len
        peak = (self.day_peak[-1] if previous is not None
                and ist_stamp(previous.start_ms).date() == ist_stamp(bar.start_ms).date() else None)
        if bar.oi is not None and (peak is None or bar.oi > peak):
            peak = bar.oi
        # T-671: `bars` is published LAST. Readers on other threads (live_snapshot) find an index
        # through `bars`, so every index it shows must already exist in atr / sma / day_peak.
        self.atr.append(atr)
        self.sma.append(sma)
        self.day_peak.append(peak)
        self.bars.append(bar)
        return True


class StockSeries:
    """Ordered VWAP terms indexed by session; no cross-session rescans."""

    def __init__(self):
        self.bars = []
        self._days = defaultdict(list)

    def push(self, bar) -> bool:
        if self.bars and bar.start_ms <= self.bars[-1].start_ms:
            return False
        i = len(self.bars)
        if bar.volume is not None and bar.volume > 0:
            stamp = ist_stamp(bar.start_ms)
            self._days[stamp.date()].append(
                (i, stamp.time(), (bar.high + bar.low + bar.close) / 3 * bar.volume, bar.volume))
        # T-671: `bars` LAST, so vwap(j) for any j a reader finds in `bars` already holds term j.
        self.bars.append(bar)
        return True

    def vwap(self, j: int, open_at: time) -> float | None:
        day = ist_stamp(self.bars[j].start_ms).date()
        selected = [term for term in self._days.get(day, ()) if term[0] <= j and term[1] >= open_at]
        weight = sum(term[3] for term in selected)
        return sum(term[2] for term in selected) / weight if weight else None
