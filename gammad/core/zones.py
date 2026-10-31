"""EXACT port of the zone half of Pine v6 `Volatility Supply and Demand Zones` (structure half unused, D3).

Pine semantics reproduced, bar by bar:
  atrRef  = ta.atr(50)  = ta.rma(trueRange, 50); trueRange = high - low on bar 0, else
            max(high - low, |high - close[1]|, |low - close[1]|); rma seeds with ta.sma(src, 50)
            (na for the first 49 bars) then alpha = 1/50 recursion.
  buffer  = atrRef * zoneDepth / 10          band = atrRef * guardMult   (THIS bar's ATR)
  pivHigh = ta.pivothigh(high, 10, 10), pivLow = ta.pivotlow(low, 10, 10): value of bar i-10,
            reported on bar i (confirmation). Tie rule: see PIVOT TIES below.
  per bar: f_retire(sell) -> f_retire(buy) -> open sell (if pivHigh) -> open buy (if pivLow).
  retire:  sell broken if probe > top, buy broken if probe < bottom; probe = close (wick: high/low).
  open:    sell top = pivot, bottom = top - buffer; buy bottom = pivot, top = bottom + buffer;
           mid = (top + bottom) / 2; refused if |mid - live_mid| <= band for ANY live same-side
           zone; newest unshifted to the FRONT, list trimmed from the BACK to zoneMemory.

Before ATR exists (the first atr_len - 1 bars) there is no buffer, so a pivot confirmed then
is DROPPED (D18; Pine keeps a one-edged zone, this port does not; T-490).

PIVOT TIES: `left_strict` is the CODE default kept for research reproducibility (earlier equal bar).
`right_strict` matches TradingView (later equal bar; tests/fixtures/zones_parity/, D506 (2)).
The product reads `zones.tie_rule` via from_config (D514), for both daily and hourly books.
`strict` rejects any tie. Lows are the mirror image.
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Literal, get_args

import numpy as np
from numpy.lib.stride_tricks import sliding_window_view

from gammad.broker.base import Candle
from gammad.config import ZonesCfg

TieRule = Literal["left_strict", "right_strict", "strict"]
TIE_RULES: tuple[str, ...] = get_args(TieRule)


@dataclass(frozen=True)
class ZoneParams:
    pivot_len: int = 10
    atr_len: int = 50
    zone_depth: float = 2.5
    guard_mult: float = 2.0
    zone_memory: int = 20
    break_on_wick: bool = False
    use_guard: bool = True
    tie_rule: TieRule = "left_strict"

    @classmethod
    def from_config(cls, cfg: ZonesCfg) -> ZoneParams:
        if cfg.tie_rule not in TIE_RULES:
            raise ValueError(f"zones.tie_rule {cfg.tie_rule!r} is not one of {TIE_RULES}")
        return cls(
            pivot_len=int(cfg.pivot_len),
            atr_len=int(cfg.atr_len),
            zone_depth=float(cfg.zone_depth),
            guard_mult=float(cfg.guard_mult),
            zone_memory=int(cfg.zone_memory),
            break_on_wick=bool(cfg.break_on_wick),
            tie_rule=cfg.tie_rule,
        )


@dataclass(frozen=True)
class Zone:
    top: float
    bottom: float
    mid: float
    formed_bar_index: int
    confirmed_bar_index: int
    side: Literal["sell", "buy"]


@dataclass
class ZoneState:
    live_sell: list[Zone] = field(default_factory=list)  # newest first (Pine unshift order)
    live_buy: list[Zone] = field(default_factory=list)
    n_bars: int = 0
    atr_last: float = math.nan
    retired_sell: int = 0
    retired_buy: int = 0
    rejected_sell: int = 0
    rejected_buy: int = 0
    dropped_fifo: int = 0


def true_range(high: np.ndarray, low: np.ndarray, close: np.ndarray) -> np.ndarray:
    tr = high - low
    if len(tr) > 1:
        pc = close[:-1]
        tr[1:] = np.maximum(np.maximum(high[1:] - low[1:], np.abs(high[1:] - pc)), np.abs(low[1:] - pc))
    return tr


def rma(src: np.ndarray, length: int) -> np.ndarray:
    """Pine ta.rma: na until `length` values exist, seeded by their SMA, then alpha = 1/length."""
    out = np.full(len(src), np.nan)
    if length < 1 or len(src) < length:
        return out
    alpha = 1.0 / length
    prev = float(np.mean(src[:length]))
    out[length - 1] = prev
    for i in range(length, len(src)):
        prev = alpha * float(src[i]) + (1.0 - alpha) * prev
        out[i] = prev
    return out


def atr(high: np.ndarray, low: np.ndarray, close: np.ndarray, length: int) -> np.ndarray:
    return rma(true_range(high, low, close), length)


def _pivots(src: np.ndarray, length: int, high: bool, tie_rule: TieRule) -> np.ndarray:
    """Pivot value reported on its CONFIRMATION bar (center + length); NaN elsewhere."""
    n = len(src)
    out = np.full(n, np.nan)
    width = 2 * length + 1
    if n < width:
        return out
    win = sliding_window_view(src, width)
    center = win[:, length]
    left = win[:, :length]
    right = win[:, length + 1 :]
    if high:
        lmax, rmax = left.max(axis=1), right.max(axis=1)
        left_ok = center > lmax if tie_rule in ("left_strict", "strict") else center >= lmax
        right_ok = center > rmax if tie_rule in ("right_strict", "strict") else center >= rmax
    else:
        lmin, rmin = left.min(axis=1), right.min(axis=1)
        left_ok = center < lmin if tie_rule in ("left_strict", "strict") else center <= lmin
        right_ok = center < rmin if tie_rule in ("right_strict", "strict") else center <= rmin
    ok = left_ok & right_ok & ~np.isnan(center)
    out[width - 1 :] = np.where(ok, center, np.nan)
    return out


def pivot_high(high: np.ndarray, length: int, tie_rule: TieRule = "left_strict") -> np.ndarray:
    return _pivots(high, length, True, tie_rule)


def pivot_low(low: np.ndarray, length: int, tie_rule: TieRule = "left_strict") -> np.ndarray:
    return _pivots(low, length, False, tie_rule)


def _is_clear(zones: list[Zone], mid: float, band: float) -> bool:
    for z in zones:
        if abs(mid - (z.top + z.bottom) / 2) <= band:  # NaN -> False, as Pine's na
            return False
    return True


def compute_zones_arrays(
    high: np.ndarray, low: np.ndarray, close: np.ndarray, params: ZoneParams | None = None
) -> ZoneState:
    params = params or ZoneParams()
    high = np.asarray(high, dtype=float)
    low = np.asarray(low, dtype=float)
    close = np.asarray(close, dtype=float)
    n = len(close)
    state = ZoneState(n_bars=n)
    if n == 0:
        return state
    atr_v = atr(high, low, close, params.atr_len)
    ph = pivot_high(high, params.pivot_len, params.tie_rule)
    pl = pivot_low(low, params.pivot_len, params.tie_rule)
    sell_probe = high if params.break_on_wick else close
    buy_probe = low if params.break_on_wick else close
    depth = params.zone_depth / 10.0
    cap = params.zone_memory
    sells: list[Zone] = []
    buys: list[Zone] = []

    has_ph = ~np.isnan(ph)
    has_pl = ~np.isnan(pl)
    for i in range(n):
        # 1. retire (sell first, then buy -- independent lists, order kept for survivors)
        if sells:
            sp = sell_probe[i]
            keep = [z for z in sells if not (sp > z.top)]
            state.retired_sell += len(sells) - len(keep)
            sells = keep
        if buys:
            bp = buy_probe[i]
            keep = [z for z in buys if not (bp < z.bottom)]
            state.retired_buy += len(buys) - len(keep)
            buys = keep
        if not (has_ph[i] or has_pl[i]):
            continue
        a = float(atr_v[i])
        if math.isnan(a):  # D18 / T-490: consume the pivot without opening a warm-up zone.
            continue
        buffer = a * depth
        band = a * params.guard_mult
        formed = i - params.pivot_len
        # 2. open sell zone from a confirmed pivot high
        if has_ph[i]:
            top = float(ph[i])
            bot = top - buffer
            mid = (top + bot) / 2
            if not params.use_guard or _is_clear(sells, mid, band):
                sells.insert(0, Zone(top, bot, mid, formed, i, "sell"))
                if len(sells) > cap:
                    state.dropped_fifo += len(sells) - cap
                    del sells[cap:]
            else:
                state.rejected_sell += 1
        # 3. open buy zone from a confirmed pivot low
        if has_pl[i]:
            bot = float(pl[i])
            top = bot + buffer
            mid = (top + bot) / 2
            if not params.use_guard or _is_clear(buys, mid, band):
                buys.insert(0, Zone(top, bot, mid, formed, i, "buy"))
                if len(buys) > cap:
                    state.dropped_fifo += len(buys) - cap
                    del buys[cap:]
            else:
                state.rejected_buy += 1

    state.live_sell = sells
    state.live_buy = buys
    state.atr_last = float(atr_v[-1])
    return state


def compute_zones(candles: Sequence[Candle], params: ZoneParams | None = None) -> ZoneState:
    """Candles oldest first. Bar index 0 = candles[0], as on a Pine chart whose history starts there."""
    if not candles:
        return ZoneState()
    high = np.fromiter((c.high for c in candles), dtype=float, count=len(candles))
    low = np.fromiter((c.low for c in candles), dtype=float, count=len(candles))
    close = np.fromiter((c.close for c in candles), dtype=float, count=len(candles))
    return compute_zones_arrays(high, low, close, params)
