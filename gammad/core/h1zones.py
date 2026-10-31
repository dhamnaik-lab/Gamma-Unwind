"""1-hour zones (T-267, STRATEGY-SPEC 4.7, PLAN-T267 §1 + M1 / M10 / M12 / M13, D249(2)).

0.8.9 (T-419, D480 (3)): MOVED here from gammad/v2scan/h1zones.py (content unchanged; that module is now a shim that
IS this module) so the client edition's Still Watching list can read its 1h stop zone (the client package drops
gammad/v2scan/). New here: `near_far` (the stop zone's NEAR / FAR edge per option side).

`H1ZoneBook` is the zone engine of `gammad/core/zones.py` (the exact Pine port, IMPORTED, never edited) run ONE bar at
a time over the stock's 60-min rows `[ts_open, o, h, l, c, close_ms, tag]` (09:15-anchored; the 15:15 slot closes at
15:30). It is a port of the research books the T-263 backtest used (t234_levels.ZoneBook / one_pass_snapshots and
t263_study.H1Zones / stop_zone / breached / trail_ok, docs/ never imported from here), and it holds the FULL engine
state so it can be snapshotted and restored (M1, the D249(2) ZONE SEED):

  ATR(50)   ta.rma of the true range: the first atr_len TRs (warm-up, seeded by their np.mean like zones.rma) or the
            running rma + the last close (the next bar's TR needs close[1]);
  pivots    the last 2 x pivot_len highs / lows (ta.pivothigh / ta.pivotlow(10, 10) report bar i - 10 on bar i);
  zones     the live sell / buy lists in Pine order (newest first), each zone with its confirmed_ms (M10: a zone
            restored from a seed resolves its confirmation time from the seed, never from pre-seed close_ms);
            a pivot confirmed before ATR exists is dropped (D18), as in zones.py;
  counters  every counter ZoneState keeps (n_bars, atr_last, retired / rejected per side, dropped_fifo).

restore(snapshot after bar D) + append(the bars after D) == one pass over the whole series, zone for zone and counter
for counter (tests/test_h1zones.py, tests/test_zone_seed.py). Bar indices are absolute from the history start; a
restored book continues from the snapshot's `n` (the bar-index offset).

Time rules (as researched): state_at(t) = the zones on the 1h bars CLOSED at or before t; new_zones(side, t0, t1) =
trade-side zones confirmed on a 1h bar closing in (t0, t1]; a step confirmed at T applies from the first 5-min stock
close AFTER T (the tracker's rule). Sides: CE trades use BUY (support) zones, PE trades use SELL (resistance) zones.

M12: a missing book is a value, never an exception that blocks an entry -- `stop_zone_for(None, ...)` is None =
NO_ZONE (M7: no stop; DAY5 / EXPIRY only), logged for the owner by the caller.
"""

from __future__ import annotations

import bisect
import hashlib
import json
import math
import time
from collections import deque
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Literal

import numpy as np

from gammad.core.levels import pick_levels
from gammad.core.zones import Zone, ZoneParams, ZoneState, _is_clear
from gammad.log import get_logger

log = get_logger(__name__)

IST = timezone(timedelta(hours=5, minutes=30))
H_MS = 3_600_000
M5_MS = 300_000
DAY_MS = 86_400_000
_ANCHOR_UTC_MS = 13_500_000  # 09:15 IST = 03:45 UTC
SESSION_MIN = 375  # 09:15 -> 15:30
H1_HISTORY_START = "2017-04-03"  # the first H60 bar Dhan serves (t234_common.H1_HISTORY_START)
# 0.8.9 (T-419, D480 (3)): the module moved to gammad/core/; the seed file did NOT move (the packager and
# tools/build_zone_seed.py own its location), so the path stays pinned to gammad/v2scan/seed/ and the schema id keeps
# its v2scan name (a seed written before the move must still load)
SEED_PATH = Path(__file__).resolve().parents[1] / "v2scan" / "seed" / "zone-seed.json"
SEED_SCHEMA = "gammad.v2scan.zone-seed/1"
SNAPSHOT_VERSION = 1

Side = Literal["CE", "PE"]


# ---------------------------------------------------------------------------------------------- small helpers
def ist_ms(d: date | str, hhmm: str) -> int:
    x = d if isinstance(d, date) else date.fromisoformat(str(d)[:10])
    h, m = (int(v) for v in hhmm.split(":"))
    return int(datetime(x.year, x.month, x.day, h, m, tzinfo=IST).timestamp() * 1000)


def ist_day(ms: int) -> date:
    return datetime.fromtimestamp(int(ms) / 1000, IST).date()


def day_anchor(ts: int) -> int:
    """09:15 IST of the IST day holding ts (ts >= 03:45 UTC of that day)."""
    return ts - ((ts - _ANCHOR_UTC_MS) % DAY_MS)


def default_params() -> ZoneParams:
    """The zone params of a 1h book built WITHOUT explicit params (tools, tests, any caller that passes none):
    config/strategy.yaml `zones:` via gammad.config.settings() -> ZoneParams.from_config. The running app never
    relies on it: every Runtime site (the V2T books, the daily job, reconcile_all, the Still Watching books and the
    zone top-up) passes Runtime._zone_params() = ZoneParams.from_config(runtime.cfg.zones) (T-422, CX-58 / CX-81),
    so a `zones:` edit takes effect after a restart, for every book together."""
    # lazy: read at call time (a test may swap settings())
    from gammad.config import settings

    return ZoneParams.from_config(settings().zones)


def params_hash(params: ZoneParams) -> str:
    raw = json.dumps(asdict(params), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def _f(x: float) -> float | None:
    """JSON-safe float: NaN -> None (Pine's na)."""
    return None if x is None or (isinstance(x, float) and math.isnan(x)) else float(x)


def _u(x: Any) -> float:
    return math.nan if x is None else float(x)


def _isnan(x: float) -> bool:
    return math.isnan(x)


def _nmax(a: float, b: float) -> float:
    """np.maximum on two scalars (NaN propagates)."""
    if _isnan(a) or _isnan(b):
        return math.nan
    return max(a, b)


def _seq_max(xs: Iterable[float]) -> float:
    vals = list(xs)
    return math.nan if any(_isnan(v) for v in vals) else max(vals)


def _seq_min(xs: Iterable[float]) -> float:
    vals = list(xs)
    return math.nan if any(_isnan(v) for v in vals) else min(vals)


def _fin(top: float, bottom: float) -> bool:
    return math.isfinite(top) and math.isfinite(bottom)


# ---------------------------------------------------------------------------------------------- contract types
@dataclass(frozen=True)
class StopZone:
    top: float
    bottom: float
    confirmed_ms: int


@dataclass(frozen=True)
class ZoneEvent:
    """A newly confirmed 1h zone. zone_side ("buy" = CE support, "sell" = PE resistance) is ADDITIVE to the pinned
    contract (defaulted, last): append() returns both sides, new_zones() filters by trade side."""

    confirmed_ms: int
    top: float
    bottom: float
    h1_close: float
    zone_side: str = ""


def _zone_side(side: str) -> str:
    if side == "CE":
        return "buy"
    if side == "PE":
        return "sell"
    raise ValueError(f"side must be CE | PE, got {side!r}")


def breached(side: str, close: float, zone: StopZone) -> bool:
    """A 5-min stock CLOSE beyond the FAR edge: CE close < bottom; PE close > top (t263_study.breached)."""
    return bool(close < zone.bottom) if side == "CE" else bool(close > zone.top)


def near_far(option_type: str, zone: Any) -> tuple[float, float]:
    """(near, far) edge of a stop zone (anything with .top / .bottom) for an option side (T-419). A CE's stop zone is
    the BUY zone BELOW price: near = top, far = bottom; a PE's is the SELL zone ABOVE price: near = bottom, far = top.
    = zx_levels.py:330 `s1, s2 = (bot, top) if side == "CE" else (top, bot)` (s1 = FAR, s2 = NEAR, zx_common.Levels),
    the edges p2_exits' S2_CLOSE (CE close <= near, PE close >= near) and S1_CLOSE / `breached` (FAR) test."""
    top, bottom = float(zone.top), float(zone.bottom)
    if option_type == "CE":
        return top, bottom
    if option_type == "PE":
        return bottom, top
    raise ValueError(f"option_type must be CE | PE, got {option_type!r}")


def trail_ok(side: str, stop: StopZone, ev: ZoneEvent) -> bool:
    """t263_study.trail_ok: a new zone between the current stop and the stock price. CE = bottom above the stop's
    bottom (the exit level only moves up) and top below the 1h close; PE = top below the stop's top and bottom above
    the 1h close. A zone with a NaN edge never qualifies, so the stop never moves back."""
    if not _fin(ev.top, ev.bottom):
        return False
    if side == "CE":
        return ev.bottom > stop.bottom and ev.top < ev.h1_close
    return ev.top < stop.top and ev.bottom > ev.h1_close


# ---------------------------------------------------------------------------------------------- hourly bars from 5-min
def _bar_fields(b: Any) -> tuple[int, float, float, float, float]:
    if hasattr(b, "ts_ms"):
        return int(b.ts_ms), float(b.open), float(b.high), float(b.low), float(b.close)
    return int(b[0]), float(b[1]), float(b[2]), float(b[3]), float(b[4])


def hour_bars_from_5m(bars5: Iterable[Any], day: date | str, *, now_ms: int | None = None,
                      include_stub: bool = False) -> list[list]:
    """ONE day's 5-min stock bars (Bar objects or rows [ts_open, o, h, l, c, ...]) -> hourly rows
    [ts_open, o, h, l, c, close_ms, tag]: the 6 09:15-anchored FULL hours over the slots 09:15 .. 15:10 (tag "h";
    09:15, 10:15, ..., 14:15, each closing 1 h later). D253 (as backtested, D248(5)): NO 15:15-15:30 stub by default
    -- Dhan's 60-min history has served only these 6 hours since the 2026-08-12 chunk and the research zones used
    them; the 16:05 reconcile (broker bars replace today's hours, M13) stays the authority. include_stub=True (opt-in
    only) adds the stub from the 15:15 / 15:20 / 15:25 slots (tag "real", closes 15:30). An hour with no bar is
    omitted. now_ms: only rows whose close_ms <= now_ms (an hour still forming is never returned)."""
    t0 = ist_ms(day, "09:15")
    s0, s1 = ist_ms(day, "15:15"), ist_ms(day, "15:30")
    buckets: dict[int, list[tuple[int, float, float, float, float]]] = {}
    stub: list[tuple[int, float, float, float, float]] = []
    seen: dict[int, tuple[int, float, float, float, float]] = {}
    for b in bars5:
        f = _bar_fields(b)
        seen[f[0]] = f  # one bar per slot, the last wins
    for ts in sorted(seen):
        f = seen[ts]
        if t0 <= ts < s0:
            buckets.setdefault((ts - t0) // H_MS, []).append(f)
        elif s0 <= ts < s1:
            stub.append(f)

    def agg(seg: list[tuple[int, float, float, float, float]]) -> list[float]:
        return [seg[0][1], max(x[2] for x in seg), min(x[3] for x in seg), seg[-1][4]]

    rows: list[list] = [[t0 + j * H_MS, *agg(seg), t0 + (j + 1) * H_MS, "h"] for j, seg in sorted(buckets.items())]
    if include_stub and stub:
        rows.append([s0, *agg(stub), s1, "real"])
    if now_ms is not None:
        rows = [r for r in rows if int(r[5]) <= int(now_ms)]
    return rows


# ---------------------------------------------------------------------------------------------- the book
@dataclass
class _Snap:
    close_ms: int
    n: int
    sells: tuple[Zone, ...]
    buys: tuple[Zone, ...]


class H1ZoneBook:
    """The stock's 1h zone book (module doc). `H1ZoneBook(rows, params)` = one pass from the first row;
    `H1ZoneBook.restore(snapshot, params)` = the state a snapshot holds, ready for append().

    history_from_ms: state_at / new_zones answer for times >= this (None = every appended bar). A restored book
    answers from the snapshot's last close on (earlier times return None: unknown, never guessed)."""

    def __init__(self, rows: Iterable[Sequence[Any]] = (), params: ZoneParams | None = None, *,
                 history_from_ms: int | None = None) -> None:
        self.params = params or default_params()
        p = self.params
        self.n = 0
        self.last_close: float | None = None
        self.last_close_ms: int | None = None
        self.atr_rma: float | None = None  # None until atr_len TRs exist
        self.atr_warm: list[float] = []
        self.atr_last = math.nan
        self.win_high: deque[float] = deque(maxlen=max(0, 2 * p.pivot_len))
        self.win_low: deque[float] = deque(maxlen=max(0, 2 * p.pivot_len))
        self.sells: list[Zone] = []
        self.buys: list[Zone] = []
        self.retired_sell = self.retired_buy = 0
        self.rejected_sell = self.rejected_buy = 0
        self.dropped_fifo = 0
        self.skipped = 0  # rows refused because they do not close after the last appended bar
        self._cms: dict[tuple[str, int], int] = {}  # (side, confirmed_bar_index) -> confirmed_ms
        self._history_from_ms = history_from_ms
        self._recording = False
        self._floor_ms: int | None = None  # earliest answerable time; None = from the beginning
        self._snaps: list[_Snap] = []
        self._snap_ms: list[int] = []
        self._events: list[ZoneEvent] = []
        self._event_ms: list[int] = []
        for r in rows:
            self.append(r)

    # -- engine --------------------------------------------------------------------------------------------------
    def append(self, row: Sequence[Any]) -> list[ZoneEvent]:
        """Feed ONE closed 1h row [ts_open, o, h, l, c, close_ms, (tag)]. Returns the zones it confirmed (both sides,
        finite edges only). A row that does not close after the last appended bar is skipped (returns [])."""
        close_ms = int(row[5])
        if self.last_close_ms is not None and close_ms <= self.last_close_ms:
            self.skipped += 1
            return []
        h, lo, c = float(row[2]), float(row[3]), float(row[4])
        p = self.params
        recording = self._history_from_ms is None or close_ms >= self._history_from_ms
        if recording and not self._recording:
            self._recording = True
            if self.last_close_ms is not None:  # bars before history_from_ms: answer from the last of them on
                self._floor_ms = self.last_close_ms
                self._push_snap(self.last_close_ms)
        i = self.n
        # 1. ATR(atr_len) = ta.rma(trueRange): zones.true_range + zones.rma, one value at a time
        if self.last_close is None:
            tr = h - lo
        else:
            pc = self.last_close
            tr = _nmax(_nmax(h - lo, abs(h - pc)), abs(lo - pc))
        if p.atr_len < 1:
            a = math.nan
        elif self.atr_rma is None:
            self.atr_warm.append(tr)
            if len(self.atr_warm) == p.atr_len:
                self.atr_rma = float(np.mean(np.asarray(self.atr_warm, dtype=float)))
                self.atr_warm = []
                a = self.atr_rma
            else:
                a = math.nan
        else:
            alpha = 1.0 / p.atr_len
            self.atr_rma = alpha * float(tr) + (1.0 - alpha) * self.atr_rma
            a = self.atr_rma
        # 2. pivots (zones._pivots on the window ending at this bar)
        ph = pl = math.nan
        L = p.pivot_len
        if len(self.win_high) == 2 * L:
            wh = [*self.win_high, h]
            wl = [*self.win_low, lo]
            ch, cl = wh[L], wl[L]
            lmax, rmax = _seq_max(wh[:L]), _seq_max(wh[L + 1:])
            lmin, rmin = _seq_min(wl[:L]), _seq_min(wl[L + 1:])
            strict_l = p.tie_rule in ("left_strict", "strict")
            strict_r = p.tie_rule in ("right_strict", "strict")
            ok_h = (ch > lmax if strict_l else ch >= lmax) and (ch > rmax if strict_r else ch >= rmax)
            ok_l = (cl < lmin if strict_l else cl <= lmin) and (cl < rmin if strict_r else cl <= rmin)
            if ok_h and not _isnan(ch):
                ph = ch
            if ok_l and not _isnan(cl):
                pl = cl
        if L > 0:
            self.win_high.append(h)
            self.win_low.append(lo)
        # 3. retire (sell first, then buy), then open sell, then open buy -- zones.compute_zones_arrays step i
        changed = False
        sell_probe = h if p.break_on_wick else c
        buy_probe = lo if p.break_on_wick else c
        if self.sells:
            keep = [z for z in self.sells if not (sell_probe > z.top)]
            if len(keep) != len(self.sells):
                self.retired_sell += len(self.sells) - len(keep)
                self.sells = keep
                changed = True
        if self.buys:
            keep = [z for z in self.buys if not (buy_probe < z.bottom)]
            if len(keep) != len(self.buys):
                self.retired_buy += len(self.buys) - len(keep)
                self.buys = keep
                changed = True
        events: list[ZoneEvent] = []
        # D18 / T-490: consume pivots with no confirmation-bar ATR without opening a zone.
        if not _isnan(a) and not (_isnan(ph) and _isnan(pl)):
            buffer = a * (p.zone_depth / 10.0)
            band = a * p.guard_mult
            formed = i - L
            cap = p.zone_memory
            if not _isnan(ph):
                top = float(ph)
                bot = top - buffer
                mid = (top + bot) / 2
                if not p.use_guard or _is_clear(self.sells, mid, band):
                    z = Zone(top, bot, mid, formed, i, "sell")
                    self.sells.insert(0, z)
                    self._cms[("sell", i)] = close_ms
                    if len(self.sells) > cap:
                        self.dropped_fifo += len(self.sells) - cap
                        del self.sells[cap:]
                    changed = True
                    if self.sells and self.sells[0] is z and _fin(top, bot):
                        events.append(ZoneEvent(close_ms, top, bot, c, "sell"))
                else:
                    self.rejected_sell += 1
            if not _isnan(pl):
                bot = float(pl)
                top = bot + buffer
                mid = (top + bot) / 2
                if not p.use_guard or _is_clear(self.buys, mid, band):
                    z = Zone(top, bot, mid, formed, i, "buy")
                    self.buys.insert(0, z)
                    self._cms[("buy", i)] = close_ms
                    if len(self.buys) > cap:
                        self.dropped_fifo += len(self.buys) - cap
                        del self.buys[cap:]
                    changed = True
                    if self.buys and self.buys[0] is z and _fin(top, bot):
                        events.append(ZoneEvent(close_ms, top, bot, c, "buy"))
                else:
                    self.rejected_buy += 1
        self.n = i + 1
        self.atr_last = a
        self.last_close = c
        self.last_close_ms = close_ms
        if changed:
            self._prune_cms()
        if recording:
            if changed:
                self._push_snap(close_ms)
            for ev in events:
                self._events.append(ev)
                self._event_ms.append(close_ms)
        return events

    def extend(self, rows: Iterable[Sequence[Any]]) -> list[ZoneEvent]:
        out: list[ZoneEvent] = []
        for r in rows:
            out += self.append(r)
        return out

    def _prune_cms(self) -> None:
        """Drop the confirmation times of dead zones -- only while no history is kept (a retained snapshot may
        still hold a dead zone that state_at / stop_zone must resolve)."""
        if not self._recording and len(self._cms) > 4 * max(1, self.params.zone_memory) + 8:
            live = {("sell", z.confirmed_bar_index) for z in self.sells} | {
                ("buy", z.confirmed_bar_index) for z in self.buys}
            self._cms = {k: v for k, v in self._cms.items() if k in live}

    def _push_snap(self, close_ms: int) -> None:
        if self._snaps and self._snaps[-1].close_ms == close_ms:
            self._snaps[-1] = _Snap(close_ms, self.n, tuple(self.sells), tuple(self.buys))
            return
        self._snaps.append(_Snap(close_ms, self.n, tuple(self.sells), tuple(self.buys)))
        self._snap_ms.append(close_ms)

    # -- reads ---------------------------------------------------------------------------------------------------
    def zone_state(self) -> ZoneState:
        """The current state as zones.ZoneState (lists newest first, every counter)."""
        return ZoneState(live_sell=list(self.sells), live_buy=list(self.buys), n_bars=self.n, atr_last=self.atr_last,
                         retired_sell=self.retired_sell, retired_buy=self.retired_buy,
                         rejected_sell=self.rejected_sell, rejected_buy=self.rejected_buy,
                         dropped_fifo=self.dropped_fifo)

    def confirmed_ms(self, z: Zone) -> int | None:
        return self._cms.get((z.side, z.confirmed_bar_index))

    def answers_from(self) -> int | None:
        """The earliest t state_at answers for (None = any t)."""
        return self._floor_ms

    def state_at(self, t_ms: int) -> ZoneState | None:
        """The zones on the 1h bars CLOSED at or before t_ms; None when t_ms precedes the book's retained history."""
        t = int(t_ms)
        if self.last_close_ms is not None and t >= self.last_close_ms:
            return ZoneState(live_sell=list(self.sells), live_buy=list(self.buys), n_bars=self.n)
        if self._floor_ms is not None and t < self._floor_ms:
            return None
        if self._history_from_ms is not None and not self._recording:
            return None
        k = bisect.bisect_right(self._snap_ms, t) - 1
        if k < 0:
            if self._floor_ms is None:
                return ZoneState()
            return None
        s = self._snaps[k]
        return ZoneState(live_sell=list(s.sells), live_buy=list(s.buys), n_bars=s.n)

    def stop_zone(self, side: str, px: float, t_ms: int) -> StopZone | None:
        """CE: the nearest live BUY zone with top < px; PE: the nearest live SELL zone with bottom > px, on the state
        at t_ms (t263_study.stop_zone + pick_levels). None = NO_ZONE."""
        zs = _zone_side(side)
        if not (px and math.isfinite(px) and px > 0):
            return None
        st = self.state_at(t_ms)
        if st is None:
            log.warning("h1zones: state at %s precedes the book's history (from %s): NO_ZONE", t_ms, self._floor_ms)
            return None
        lv = pick_levels(st, float(px))
        z = lv.support if zs == "buy" else lv.resistance
        if z is None:
            return None
        cms = self.confirmed_ms(z)
        if cms is None:  # cannot happen for a live zone (kept on open / restore); never invent a time
            log.error("h1zones: zone %s has no confirmed_ms: NO_ZONE", z)
            return None
        return StopZone(float(z.top), float(z.bottom), int(cms))

    def new_zones(self, side: str, t0: int, t1: int) -> list[ZoneEvent]:
        """Trade-side zones confirmed on a 1h bar closing in (t0, t1], oldest first (t263_study.H1Zones.new_zones)."""
        zs = _zone_side(side)
        a = bisect.bisect_right(self._event_ms, int(t0))
        b = bisect.bisect_right(self._event_ms, int(t1))
        return [e for e in self._events[a:b] if e.zone_side == zs]

    # -- snapshot / restore (M1, M10) ----------------------------------------------------------------------------
    def snapshot(self) -> dict[str, Any]:
        """The FULL engine state as a JSON-safe dict (NaN -> None)."""
        def zl(zs: list[Zone], side: str) -> list[list]:
            out = []
            for z in zs:
                cms = self._cms.get((side, z.confirmed_bar_index))
                out.append([_f(z.top), _f(z.bottom), int(z.formed_bar_index), int(z.confirmed_bar_index),
                            None if cms is None else int(cms)])
            return out

        return {
            "v": SNAPSHOT_VERSION,
            "params_hash": params_hash(self.params),
            "n": self.n,
            "last_close": _f(self.last_close) if self.last_close is not None else None,
            "last_close_ms": self.last_close_ms,
            "atr_rma": _f(self.atr_rma) if self.atr_rma is not None else None,
            "atr_seeded": self.atr_rma is not None,
            "atr_warm": [_f(x) for x in self.atr_warm],
            "atr_last": _f(self.atr_last),
            "win_high": [_f(x) for x in self.win_high],
            "win_low": [_f(x) for x in self.win_low],
            "sell": zl(self.sells, "sell"),
            "buy": zl(self.buys, "buy"),
            "counters": {"retired_sell": self.retired_sell, "retired_buy": self.retired_buy,
                         "rejected_sell": self.rejected_sell, "rejected_buy": self.rejected_buy,
                         "dropped_fifo": self.dropped_fifo},
        }

    @classmethod
    def restore(cls, state: Mapping[str, Any], params: ZoneParams | None = None, *,
                history_from_ms: int | None = None) -> H1ZoneBook:
        """A book holding exactly `state` (a snapshot()); raises ValueError on a version / params mismatch."""
        params = params or default_params()
        if int(state.get("v", -1)) != SNAPSHOT_VERSION:
            raise ValueError(f"zone snapshot version {state.get('v')!r} != {SNAPSHOT_VERSION}")
        if state.get("params_hash") != params_hash(params):
            raise ValueError(f"zone snapshot params {state.get('params_hash')!r} != {params_hash(params)!r}")
        b = cls((), params, history_from_ms=history_from_ms)
        b.n = int(state["n"])
        b.last_close = None if state.get("last_close") is None else float(state["last_close"])
        b.last_close_ms = None if state.get("last_close_ms") is None else int(state["last_close_ms"])
        seeded = bool(state.get("atr_seeded"))
        b.atr_rma = (_u(state.get("atr_rma"))) if seeded else None
        b.atr_warm = [_u(x) for x in state.get("atr_warm") or []]
        b.atr_last = _u(state.get("atr_last"))
        b.win_high.extend(_u(x) for x in state.get("win_high") or [])
        b.win_low.extend(_u(x) for x in state.get("win_low") or [])
        for side, dest in (("sell", b.sells), ("buy", b.buys)):
            for top, bot, formed, conf, cms in state.get(side) or []:
                t, bt = _u(top), _u(bot)
                dest.append(Zone(t, bt, (t + bt) / 2, int(formed), int(conf), side))
                if cms is not None:
                    b._cms[(side, int(conf))] = int(cms)
        c = state.get("counters") or {}
        b.retired_sell = int(c.get("retired_sell", 0))
        b.retired_buy = int(c.get("retired_buy", 0))
        b.rejected_sell = int(c.get("rejected_sell", 0))
        b.rejected_buy = int(c.get("rejected_buy", 0))
        b.dropped_fifo = int(c.get("dropped_fifo", 0))
        if b.n:
            b._floor_ms = b.last_close_ms
            if history_from_ms is None or (b.last_close_ms is not None and b.last_close_ms >= history_from_ms):
                b._recording = True
                b._push_snap(b.last_close_ms if b.last_close_ms is not None else -1)
        return b


# ---------------------------------------------------------------------------------------------- M12: a missing book
def stop_zone_for(book: H1ZoneBook | None, side: str, px: float, t_ms: int) -> StopZone | None:
    """M12: no book = None = NO_ZONE (never an exception that blocks an entry)."""
    if book is None:
        return None
    try:
        return book.stop_zone(side, px, t_ms)
    except Exception:  # an engine fault must never block an entry (M12); logged for the owner
        log.exception("h1zones: stop_zone failed: NO_ZONE")
        return None


# ---------------------------------------------------------------------------------------------- the seed (D249(2))
@dataclass
class Seed:
    as_of: date
    params_hash: str
    source: str
    books: dict[int, dict[str, Any]]
    meta: dict[str, Any] = field(default_factory=dict)


def seed_doc(as_of: date, params: ZoneParams, source: str, books: Mapping[int, Mapping[str, Any]],
             **meta: Any) -> dict[str, Any]:
    return {"schema": SEED_SCHEMA, "as_of": as_of.isoformat(), "h1_history_start": H1_HISTORY_START,
            "params": asdict(params), "params_hash": params_hash(params), "source": source,
            "readme": "V2 Scanner 1h zone seed (D249(2)): per underlying security id, the H1ZoneBook.snapshot() after "
                      "the last 60-min bar of as_of. Built by tools/build_zone_seed.py; restore + append bars after "
                      "as_of == one pass from h1_history_start.",
            **meta, "books": {str(int(k)): dict(v) for k, v in sorted(books.items())}}


def load_seed(path: Path | str | None = None, params: ZoneParams | None = None) -> Seed | None:
    """The packaged seed, or None (missing / unreadable / built with other params) -- logged, never raised (M2)."""
    p = Path(path) if path is not None else SEED_PATH
    params = params or default_params()
    try:
        doc = json.loads(p.read_text(encoding="utf-8"))
    except FileNotFoundError:
        log.warning("h1zones: no zone seed at %s: books build from the H60 store alone", p)
        return None
    except (OSError, ValueError) as exc:
        log.error("h1zones: zone seed %s unreadable (%s): books build from the H60 store alone", p, exc)
        return None
    if doc.get("schema") != SEED_SCHEMA or doc.get("params_hash") != params_hash(params):
        log.error("h1zones: zone seed %s schema %r / params %r do not match %r: ignored", p, doc.get("schema"),
                  doc.get("params_hash"), params_hash(params))
        return None
    meta = {k: v for k, v in doc.items() if k not in ("books", "as_of", "params_hash", "source")}
    return Seed(date.fromisoformat(doc["as_of"]), doc["params_hash"], str(doc.get("source", "")),
                {int(k): v for k, v in (doc.get("books") or {}).items()}, meta)


@dataclass
class OpenReport:
    seeded: list[int] = field(default_factory=list)
    full_pass: list[int] = field(default_factory=list)
    missing: list[int] = field(default_factory=list)
    appended_bars: int = 0
    ms: dict[int, float] = field(default_factory=dict)


def open_books(usids: Iterable[int], rows_after: Callable[[int, date | None], list[list]],
               seed: Seed | None, params: ZoneParams | None = None, *,
               history_from_ms: int | None = None) -> tuple[dict[int, H1ZoneBook], OpenReport]:
    """Startup (hunk A): per underlying, restore its seed book and append rows_after(usid, seed.as_of) (the H60
    store's rows of the days AFTER as_of), else a full pass over rows_after(usid, None) (short-history fallback, M2).
    No rows and no seed = no book (M12: NO_ZONE). history_from_ms = how far back state_at must answer."""
    params = params or default_params()
    books: dict[int, H1ZoneBook] = {}
    rep = OpenReport()
    for usid in usids:
        t0 = time.perf_counter()
        state = seed.books.get(int(usid)) if seed else None
        book: H1ZoneBook | None = None
        if state is not None:
            try:
                book = H1ZoneBook.restore(state, params)
            except (ValueError, KeyError, TypeError) as exc:
                log.error("h1zones: seed book %s unusable (%s): full pass", usid, exc)
                book = None
        if book is not None and seed is not None:
            rows = rows_after(int(usid), seed.as_of)
            before = book.n
            book.extend(rows)
            rep.appended_bars += book.n - before
            rep.seeded.append(int(usid))
        else:
            rows = rows_after(int(usid), None)
            if rows:
                book = H1ZoneBook(rows, params, history_from_ms=history_from_ms)
                rep.full_pass.append(int(usid))
                log.info("h1zones: %s built from %d stored 60-min bars (no seed book)", usid, len(rows))
            else:
                rep.missing.append(int(usid))
                log.info("h1zones: %s has no seed book and no 60-min bars: NO_ZONE until bars arrive", usid)
        if book is not None:
            books[int(usid)] = book
        rep.ms[int(usid)] = (time.perf_counter() - t0) * 1000.0
    return books, rep


# ---------------------------------------------------------------------------------------------- M13: 16:05 reconcile
def zone_keys(book: H1ZoneBook) -> set[tuple[str, float | None, float | None, int | None]]:
    out = set()
    for z in (*book.sells, *book.buys):
        out.add((z.side, _f(z.top), _f(z.bottom), book.confirmed_ms(z)))
    return out


@dataclass
class Reconcile:
    book: H1ZoneBook
    added: list[tuple] = field(default_factory=list)  # live in the broker-built book, not in the tick-built one
    removed: list[tuple] = field(default_factory=list)  # live in the tick-built book, not in the broker-built one

    @property
    def differs(self) -> bool:
        return bool(self.added or self.removed)


def reconcile(open_state: Mapping[str, Any], broker_rows: Iterable[Sequence[Any]], live: H1ZoneBook | None,
              params: ZoneParams | None = None) -> Reconcile:
    """M13: restore the session-open snapshot, append the broker's 60-min bars of today, and diff the live zones
    against the tick-built book (the measure of tick-built drift). The caller saves `book` and keeps applied steps."""
    book = H1ZoneBook.restore(open_state, params)
    book.extend(broker_rows)
    new = zone_keys(book)
    old = zone_keys(live) if live is not None else set()
    return Reconcile(book, sorted(new - old, key=_zkey_order), sorted(old - new, key=_zkey_order))


def _zkey_order(k: tuple) -> tuple:
    return (k[0], k[3] or 0, k[1] or 0.0)


def reconcile_all(open_states: Mapping[int, Mapping[str, Any]], broker_rows: Mapping[int, Sequence[Sequence[Any]]],
                  live: Mapping[int, H1ZoneBook], params: ZoneParams | None = None) -> dict[int, Reconcile]:
    out: dict[int, Reconcile] = {}
    for usid, st in open_states.items():
        try:
            out[int(usid)] = reconcile(st, broker_rows.get(usid) or [], live.get(usid), params)
        except (ValueError, KeyError, TypeError) as exc:
            log.error("h1zones: reconcile %s failed (%s): the tick-built book stands", usid, exc)
    return out
