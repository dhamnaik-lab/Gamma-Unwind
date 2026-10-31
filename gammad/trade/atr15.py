"""Atr15: the Still Watching trail's ATR (0.8.9, T-419, D471 Q6 / D472 X9A). PURE, no I/O.

ATR(atr_len) -- Wilder / RMA -- of the COMPLETED 09:15-anchored `tf_min`-minute STOCK bars built from 5-min stock bars.
PORT (never imported at runtime) of docs/research/2026-10-14-zone-exits/next/phase1/p1_exits.py `agg15` /
`wilder_atr` / `Atr15.atr_at`, the X3 / X9A ATR the T-417 Phase 2 replay graded (p2_exits X9A: "ATR14 of COMPLETED
09:15-anchored 15-min stock bars (p1_exits.Atr15 at each bar's OPEN)"):

  agg      a bucket of IST day d starts at 09:15 + tf_min x k and CLOSES at its start + tf_min, whatever 5-min bars it
           holds (open = its first bar's open, high = max, low = min, close = its last bar's close);
  wilder   the first value = the mean of the first atr_len true ranges (the first bar's TR = high - low), then
           (a x (atr_len - 1) + tr) / atr_len; None before atr_len bars;
  at open  value_at_open(ts) = the ATR of the last bucket whose nominal CLOSE is <= ts (bisect_right on close_ms) --
           a bucket still forming at ts never counts (its close is after ts), so a 5-min bar opening at 09:25 sees no
           09:15 bucket. None before atr_len completed buckets.

Live use: feed every 5-min stock bar (the previous session's for the warm-up, then each completed bar) through add();
a re-sent bar with the same ts replaces the earlier copy. Bars are [ts_ms, open, high, low, close, ...] or objects
with ts_ms / open / high / low / close.
"""

from __future__ import annotations

import bisect
from collections.abc import Iterable
from typing import Any

IST_OFFSET_MS = 19_800_000  # +05:30
DAY_MS = 86_400_000
ANCHOR_MS = (9 * 60 + 15) * 60_000  # 09:15 IST after the IST midnight
ATR_LEN = 14  # p1_exits.ATR_N (config still_watching.trail.atr_len)
TF_MIN = 15  # p1_exits.BAR15_MS / 60_000 (config still_watching.trail.tf_min)


def _fields(b: Any) -> tuple[int, float, float, float, float]:
    if hasattr(b, "ts_ms"):
        return int(b.ts_ms), float(b.open), float(b.high), float(b.low), float(b.close)
    return int(b[0]), float(b[1]), float(b[2]), float(b[3]), float(b[4])


def bucket_start(ts_ms: int, tf_ms: int) -> int:
    """The 09:15-anchored bucket start of a bar opening at ts_ms (p1_exits.agg15: base + ((t - base) // tf) x tf)."""
    t = int(ts_ms)
    base = ((t + IST_OFFSET_MS) // DAY_MS) * DAY_MS - IST_OFFSET_MS + ANCHOR_MS
    return base + ((t - base) // tf_ms) * tf_ms


def wilder_atr(rows: Iterable[tuple[float, float, float]], n: int = ATR_LEN) -> list[float | None]:
    """ATR(n), Wilder, over rows (high, low, close) oldest first (p1_exits.wilder_atr)."""
    out: list[float | None] = []
    trs: list[float] = []
    prev_c: float | None = None
    a: float | None = None
    for h, lo, c in rows:
        tr = h - lo if prev_c is None else max(h - lo, abs(h - prev_c), abs(lo - prev_c))
        prev_c = c
        trs.append(tr)
        if len(trs) == n:
            a = sum(trs) / n
        elif len(trs) > n:
            a = (a * (n - 1) + tr) / n  # type: ignore[operator]
        out.append(a)
    return out


class Atr15:
    """ATR(atr_len) of the COMPLETED tf_min-minute (default 15) stock bars, 09:15-anchored, from 5-min bars."""

    def __init__(self, bars5: Iterable[Any] = (), *, atr_len: int = ATR_LEN, tf_min: int = TF_MIN) -> None:
        if not isinstance(atr_len, int) or isinstance(atr_len, bool) or atr_len < 1:
            raise ValueError(f"Atr15: atr_len must be a positive int, got {atr_len!r}")
        if not isinstance(tf_min, int) or isinstance(tf_min, bool) or tf_min < 5 or tf_min % 5:
            raise ValueError(f"Atr15: tf_min must be a positive multiple of 5, got {tf_min!r}")
        self.atr_len = atr_len
        self.tf_ms = tf_min * 60_000
        self._bars: dict[int, tuple[int, float, float, float, float]] = {}
        self._dirty = True
        self._close_ms: list[int] = []
        self._atr: list[float | None] = []
        for b in bars5:
            self.add(b)

    def add(self, bar5: Any) -> None:
        """One 5-min stock bar (a re-sent ts replaces the earlier copy)."""
        f = _fields(bar5)
        self._bars[f[0]] = f
        self._dirty = True

    def rows(self) -> list[list[float]]:
        """The aggregated buckets [start, o, h, l, c, close_ms], oldest first (p1_exits.agg15's rows)."""
        out: dict[int, list[float]] = {}
        for t in sorted(self._bars):
            _, o, h, lo, c = self._bars[t]
            k = bucket_start(t, self.tf_ms)
            r = out.get(k)
            if r is None:
                out[k] = [k, o, h, lo, c, k + self.tf_ms]
            else:
                r[2], r[3], r[4] = max(r[2], h), min(r[3], lo), c
        return [out[k] for k in sorted(out)]

    def _build(self) -> None:
        if not self._dirty:
            return
        rows = self.rows()
        self._close_ms = [int(r[5]) for r in rows]
        self._atr = wilder_atr(((r[2], r[3], r[4]) for r in rows), self.atr_len)
        self._dirty = False

    def value_at_open(self, ts_ms: int) -> float | None:
        """The ATR of the buckets COMPLETED at ts_ms (nominal close <= ts_ms), or None (p1_exits.Atr15.atr_at)."""
        self._build()
        i = bisect.bisect_right(self._close_ms, int(ts_ms)) - 1
        return None if i < 0 else self._atr[i]
