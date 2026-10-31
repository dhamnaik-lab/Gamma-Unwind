"""T-285 (D268(5)): the split guard on the daily bars the zone engine reads (gammad/jobs/mark_levels.py).

An unadjusted split / bonus / demerger in the broker's daily history would draw zones at the pre-action price for
the whole 500-bar window. Its signature is an OVERNIGHT jump: the ex-date opens at the new level. A genuine crash
looks different: it opens within the exchange's opening band and trades down through the day (MEASURED on the held
bars, newest 500 per stock, 210 stocks: POLICYBZR 24 Sep 2026, IEX 24 Jul 2025 and INDUSINDBK 11 Mar 2025 closed
-36 / -30 / -27 % but each OPENED -10.0 %; no overnight gap above 25 % anywhere, one above 20 %). So the test is the
open against the previous close, not the regime's close-to-close test (gammad/v2scan/regime.py, frozen, which would
refuse those three genuine falls for up to two years).

Two checks, either one refuses:
  1. overnight: |b.open / a.close - 1| > gap_max for consecutive bars a, b anywhere in the series;
  2. the reference price: |ref_price / bars[-1].close - 1| > gap_max (the ex-date morning, when the history is not
     adjusted yet and the live price is already at the new level). For a ref="close" run the ref IS the last close,
     so check 1 over the completed day bar carries it.
A bar whose price is non-positive / non-finite is skipped (no claim either way: this guard never divides by it).
Pure: no I/O, no logging."""

from __future__ import annotations

import math
from collections.abc import Sequence
from datetime import date, datetime
from itertools import pairwise

from gammad.broker.base import Candle
from gammad.config import IST

GAP_MAX = 0.25  # the regime's M14 threshold, applied to the overnight jump
SPLIT_GAP = "split_gap"


def _ok(x: float | None) -> bool:
    return x is not None and math.isfinite(x) and x > 0


def _day(c: Candle) -> date:
    return datetime.fromtimestamp(c.ts_ms / 1000, IST).date()


def first_open_gap(bars: Sequence[Candle], gap_max: float = GAP_MAX) -> tuple[Candle, Candle, float] | None:
    """The FIRST consecutive pair (a, b) with |b.open / a.close - 1| > gap_max (strict), and that move; else None."""
    for a, b in pairwise(bars):
        if not (_ok(a.close) and _ok(b.open)):
            continue
        move = b.open / a.close - 1.0
        if abs(move) > gap_max:
            return a, b, move
    return None


def split_gap(bars: Sequence[Candle], ref_price: float | None = None, gap_max: float = GAP_MAX) -> str | None:
    """Why `bars` (+ the run's reference price) must not feed the zone engine, or None when they may.
    The reason reads 'split_gap: close A -> open B (+x.x %) on D' or 'split_gap: close A on D -> ref B (+x.x %)'."""
    hit = first_open_gap(bars, gap_max)
    if hit is not None:
        a, b, move = hit
        return f"{SPLIT_GAP}: close {a.close:g} -> open {b.open:g} ({move * 100:+.1f} %) on {_day(b).isoformat()}"
    if bars and _ok(ref_price) and _ok(bars[-1].close):
        last = bars[-1]
        move = float(ref_price) / last.close - 1.0  # type: ignore[arg-type]
        if abs(move) > gap_max:
            return f"{SPLIT_GAP}: close {last.close:g} on {_day(last).isoformat()} -> ref {ref_price:g} ({move * 100:+.1f} %)"
    return None
