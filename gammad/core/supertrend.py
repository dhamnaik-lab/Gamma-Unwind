"""In-house DAILY Supertrend, an exact port of the owner's TradingView CLASSIC v4 script (T-267, D246-D248).

Reference (never shipped): docs/research/2026-10-04-t263/reference/supertrend-classic-v4.pine. Per bar:

  src  = hl2 = (high + low) / 2
  atr  = Pine v4 atr(period) = rma(tr(true), period)   (changeATR = true, D247)
         tr(true): high - low on bar 0 (close[1] is na), else max(h - l, |h - c[1]|, |l - c[1]|)
         rma: na until `period` values exist, seeded with their SMA, then alpha * x + (1 - alpha) * prev,
         alpha = 1 / period (the form Pine's own rma reference uses; same as gammad/core/zones.py rma)
  up   = src - mult * atr;  up1 = nz(up[1], up);  up := close[1] > up1 ? max(up, up1) : up
  dn   = src + mult * atr;  dn1 = nz(dn[1], dn);  dn := close[1] < dn1 ? min(dn, dn1) : dn
  trend starts at 1 (green), then
  trend := trend == -1 and close > dn1 ? 1 : trend == 1 and close < up1 ? -1 : trend

THE FLIP compares close with the PREVIOUS bar's FINAL band (up1 / dn1), not this bar's band -- unlike
ta.supertrend and unlike docs/research/2026-10-01-backtest/scripts/t248_data.py (which ports ta.supertrend).

`na` semantics reproduced: before the ATR seed bar, up / dn are na (None here) and every comparison with
na is false, so the trend holds at its initial 1. On the SEED bar up[1] is na, so up1 = this bar's own raw
band (nz(up[1], up)) and the flip test compares close with it -- the seed bar CAN flip to -1.

Pure and stdlib only (no numpy, no I/O). The ATR is computed locally rather than imported from zones.py:
zones.py's helpers are numpy functions and importing zones pulls numpy + gammad.broker / gammad.config;
the arithmetic is the same formula and tests/test_supertrend.py cross-checks it against zones.atr.
"""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from itertools import pairwise
from typing import Any, Literal

IST = timezone(timedelta(hours=5, minutes=30))

DEFAULT_PERIOD = 10
DEFAULT_MULT = 2.0
SrcKind = Literal["hl2", "hlc3", "close"]
SRC_KINDS: tuple[str, ...] = ("hl2", "hlc3", "close")
DEFAULT_SRC: SrcKind = "hl2"  # FROZEN rule (D247); hlc3 / close = descriptive replay side table only


@dataclass(frozen=True)
class StPoint:
    """One bar of Supertrend. value / up / dn are None while the ATR is na (warm-up)."""

    ts_ms: int
    value: float | None  # up when trend == 1 else dn
    trend: int  # +1 green (bullish), -1 red (bearish)
    up: float | None
    dn: float | None


def _ohlc(bar: Any) -> tuple[int, float, float, float]:
    """(ts_ms, high, low, close) from a Candle-like object, a mapping, or a (ts_ms, o, h, l, c, ...) row."""
    if isinstance(bar, Mapping):
        h = bar["h"] if "h" in bar else bar["high"]
        lo = bar["l"] if "l" in bar else bar["low"]
        c = bar["c"] if "c" in bar else bar["close"]
        return int(bar["ts_ms"]), float(h), float(lo), float(c)
    if hasattr(bar, "ts_ms"):
        return int(bar.ts_ms), float(bar.high), float(bar.low), float(bar.close)
    if isinstance(bar, Sequence) and not isinstance(bar, (str, bytes)) and len(bar) >= 5:
        return int(bar[0]), float(bar[2]), float(bar[3]), float(bar[4])
    raise TypeError(f"unsupported bar shape: {type(bar).__name__}")


def ist_date(ts_ms: int) -> date:
    """IST calendar date of an epoch-ms stamp (a daily candle's ts_ms = IST midnight of its session)."""
    return datetime.fromtimestamp(ts_ms / 1000, IST).date()


def atr_rma(
    highs: Sequence[float], lows: Sequence[float], closes: Sequence[float], period: int
) -> list[float | None]:
    """Pine v4 atr(period) = rma(tr(true), period); None (na) for the first period - 1 bars."""
    if period < 1:
        raise ValueError(f"period must be >= 1, got {period}")
    alpha = 1.0 / period
    out: list[float | None] = []
    trs: list[float] = []
    atr: float | None = None
    for i, (h, lo) in enumerate(zip(highs, lows, strict=True)):
        # tr(true): close[1] na on bar 0 -> high - low
        if i == 0:
            tr = h - lo
        else:
            pc = closes[i - 1]
            tr = max(h - lo, abs(h - pc), abs(lo - pc))
        # rma: na until `period` values, SMA seed, then alpha recursion
        if atr is None:
            trs.append(tr)
            if len(trs) == period:
                atr = sum(trs) / period
        else:
            atr = alpha * tr + (1.0 - alpha) * atr
        out.append(atr)
    return out


def _src(kind: SrcKind, h: float, lo: float, c: float) -> float:
    if kind == "hl2":
        return (h + lo) / 2.0
    if kind == "hlc3":
        return (h + lo + c) / 3.0
    return c


def supertrend(
    bars: Sequence[Any],
    period: int = DEFAULT_PERIOD,
    mult: float = DEFAULT_MULT,
    src: SrcKind = DEFAULT_SRC,
) -> list[StPoint]:
    """Classic v4 Supertrend over daily bars, oldest first; one StPoint per bar. `src` other than "hl2" is for
    the research replay's DESCRIPTIVE side table only (D247: the rule is frozen at hl2)."""
    if src not in SRC_KINDS:
        raise ValueError(f"src must be one of {SRC_KINDS}, got {src!r}")
    rows = [_ohlc(b) for b in bars]
    for prev, cur in pairwise(rows):
        if cur[0] <= prev[0]:
            raise ValueError(f"bars must be strictly ascending by ts_ms ({cur[0]} after {prev[0]})")
    atrs = atr_rma([r[1] for r in rows], [r[2] for r in rows], [r[3] for r in rows], period)
    out: list[StPoint] = []
    prev_close: float | None = None
    prev_up: float | None = None  # up[1] (the previous bar's FINAL up), None = na
    prev_dn: float | None = None
    trend = 1
    for (ts, h, lo, c), atr in zip(rows, atrs, strict=True):
        s = _src(src, h, lo, c)
        up_raw = None if atr is None else s - mult * atr
        dn_raw = None if atr is None else s + mult * atr
        up1 = prev_up if prev_up is not None else up_raw  # nz(up[1], up)
        dn1 = prev_dn if prev_dn is not None else dn_raw  # nz(dn[1], dn)

        # up := close[1] > up1 ? max(up, up1) : up   (any na operand -> condition false)
        up = up_raw
        if prev_close is not None and up1 is not None and up_raw is not None and prev_close > up1:
            up = max(up_raw, up1)
        dn = dn_raw
        if prev_close is not None and dn1 is not None and dn_raw is not None and prev_close < dn1:
            dn = min(dn_raw, dn1)

        # trend := nz(trend[1], trend), then the flip against the PREVIOUS final band (dn1 / up1)
        if trend == -1 and dn1 is not None and c > dn1:
            trend = 1
        elif trend == 1 and up1 is not None and c < up1:
            trend = -1

        value = up if trend == 1 else dn
        out.append(StPoint(ts_ms=ts, value=value, trend=trend, up=up, dn=dn))
        prev_close, prev_up, prev_dn = c, up, dn
    return out


def regime_as_of(
    bars: Sequence[Any],
    day: date,
    *,
    period: int = DEFAULT_PERIOD,
    mult: float = DEFAULT_MULT,
    src: SrcKind = DEFAULT_SRC,
) -> int | None:
    """Trend (+1 / -1) of the LAST bar dated strictly before `day` (IST date of ts_ms); no look-ahead: bars
    dated `day` or later are dropped BEFORE computing. None when no such bar exists or its ATR is still na."""
    prior = [b for b in bars if ist_date(_ohlc(b)[0]) < day]
    if not prior:
        return None
    last = supertrend(prior, period=period, mult=mult, src=src)[-1]
    if last.value is None or not math.isfinite(last.value):
        return None
    return last.trend
