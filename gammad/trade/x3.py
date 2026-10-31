"""X3: the whole-lot V1 exit with an arm, an optional fixed T2, a trail off the running high and dated backstops.

PLAN-T336 §1 B1. Owner rulings: D362 R2-01 (+25 % arms: stop -> entry, then a trail off the running high, floor = entry,
no fixed T2), R2-02 (trail widths 20 % and 30 %), R2-03 (a pre-arm -40 % premium cap for SWING + EXPIRY-WEEK on top of
today's structure, the D68 stock stop kept), R2-04 (whole lot, one tracked exit), R2-05 + D361 Q17 (SAME-DAY stop =
the nearer (higher) of the base low and entry x 0.60, floored 1 tick below entry; 15:20 close), R2-07 + Q22
(EXPIRY-WEEK: the trail + a close at the 15:15 bar of the day before expiry); D365 (3): EXPIRY-WEEK keeps day 5 beside
the expiry-1 cap (whichever first); 'base_low' = today's rule.

PURE: stdlib only, no I/O, no clock. The caller (the Lab preset backtest/lab/exits.py X3, the owner shadow, a later
release) supplies the bars of each hold day, the sessions, the DTE and the underlying's daily closes. The ONE
implementation every caller uses.

    walk(bars, entry, entry_ms, tick, plan, ctx) -> X3Result

Rules (bar mode; the fill rules of T-231 exits.py, kept so that the reduction below is exact):
  * entry = the fire bar's close at entry_ms; levels are tested from the NEXT bar (bars OPENING at/after entry_ms);
  * a bar opening through a stop fills at its OPEN, else at the stop; a bar opening above a target fills at its open,
    else at the target; bar fills are stamped at the bar's CLOSE time;
  * SAME-BAR CONVENTION = STOP FIRST: a bar touching the stop and a target exits at the stop (flag same_bar_sl when the
    stop is the initial one); a stop change caused inside a bar applies from the NEXT bar;
  * book by DTE at entry: DTE <= 1 -> SD (same day: the SAME-DAY stop, flat at the close of the 15:15 bar = 15:20);
    DTE >= 2 -> SW (no premium stop before the arm unless pre_stop_pct; D68 stock stop on the daily close until the
    arm; flat at 15:20 of trading day 5 or of the expiry day, whichever first; cal_cap "expiry_1_1515" adds the close
    at the 15:15 bar of the session before expiry, whichever first);
  * the arm (+arm_pct, rounded UP to the tick): without a trail the stop goes TO the entry (non-strict, even when the
    initial SD stop was at / above the entry: the product's rules.on_tick, PLAN-X2P J2); with a trail the stop goes to
    the trail level, then ratchets up with the running high; the trail level is never below the entry;
  * a fixed T2 (t2_pct) closes the whole lot; t2_pct None = no target (the trail is the target, R2-01);
  * a hold day without bars ends the trade OPEN at the last price: HOLE when that day is <= ctx.data_end, else
    OPEN_DATA_END; sessions that stop before the hold ends (a data cliff) -> OPEN_DATA_END.

THE REDUCTION (the golden, lab-specs/golden-x3-reduce.yaml): X3Plan() -- arm 20, t2 40, trail None, pre_stop None,
d68 True, same_day_stop "base_low", floor_ticks 0, cal_cap "day5" -- IS the product exit X2-LIVE as T-231 exits.py
grades it under PLAN-X2P J2 (x2p_adapter.grade_x2): same legs, same exit bar, same price, on every row. 'base_low' is
today's rule: the base low when the caller has one (an E2 row's sl_struct), else the 50 % stand-in (exits.py:197);
'nearer' uses the base low the caller passes (a proxy row: xexp_common.base_low, supplied by the Lab).
"""
from __future__ import annotations

import math
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import ROUND_CEILING, Decimal
from itertools import pairwise
from typing import Any

IST = timezone(timedelta(hours=5, minutes=30))
TF_MS = 300_000                 # 5-min bars
MIN_TICK = 0.01                 # = rules.MIN_TICK: the grid when a contract's tick is unknown
TICK_EPS = 1e-6                 # = rules.TICK_EPS
HOLD_DAYS = 5                   # = rules.HOLD_DAYS: a swing exits on trading day 5 after the entry day
INTRADAY_MAX_DTE = 1            # = rules.INTRADAY_MAX_DTE: DTE <= 1 at entry is held the same day
SL50_PCT = 50                   # the stand-in stop when no base low is known (T-231 exits.py SL_PCT)
NEARER_PCT = 40                 # R2-05: the 'nearer' SAME-DAY stop's premium leg = entry x 0.60
SESSION_OPEN, SESSION_END = "09:15", "15:30"
CUT = "15:20"                   # the close of the 15:15 bar: same-day, day-5, expiry and expiry-1 exits
# The caller supplies the dated session close through X3Ctx; SESSION_END is the legacy default.

SAME_DAY_STOPS = ("base_low", "nearer")
CAL_CAPS = ("day5", "expiry_1_1515")
KIND_OF = {"T2": "T2", "BE": "TRAIL", "TRAIL": "TRAIL", "SL": "SL", "SAME_BAR_SL": "SL", "PRE_STOP": "PRE_STOP",
           "SWING_STOP": "SWING_STOP", "DAY5": "DAY5", "EXPIRY": "EXPIRY", "EXPIRY_1": "EXPIRY_1", "EOD": "EOD"}
KINDS = ("T2", "TRAIL", "SL", "PRE_STOP", "SWING_STOP", "DAY5", "EXPIRY", "EXPIRY_1", "EOD", "HOLE", "OPEN_DATA_END")


# ---------------------------------------------------------------------------------------------- tick arithmetic
# The product's own comparisons (gammad/trade/rules.py px_ge / px_le / px_lt / target_price / d68_step) restated in
# stdlib so this module imports nothing; tests/test_trade_x3.py asserts they agree with rules.py on a grid.
def px_ge(a: float, b: float, tick: float) -> bool:
    return (a - b) / tick > -TICK_EPS


def px_le(a: float, b: float, tick: float) -> bool:
    return (a - b) / tick < TICK_EPS


def px_lt(a: float, b: float, tick: float) -> bool:
    return not px_ge(a, b, tick)


def target_price(entry: float, pct: float, tick: float | None) -> float:
    """entry x (1 + pct) rounded UP to the tick (MIN_TICK when unknown), exact decimal arithmetic."""
    t = Decimal(repr(float(tick or MIN_TICK)))
    if not t > 0:
        raise ValueError(f"tick must be > 0, got {tick!r}")
    raw = Decimal(repr(float(entry))) * (1 + Decimal(repr(float(pct))))
    return float((raw / t).to_integral_value(rounding=ROUND_CEILING) * t)


def floor_tick(x: float, t: float) -> float:
    """x rounded DOWN to the tick grid (T-231 exits.floor_tick)."""
    return math.floor(round(x / t, 6)) * t


def ticks_below(px: float, n: int, t: float) -> float:
    """px - n ticks, exact decimal arithmetic (99.95, never 99.94999...)."""
    return float(Decimal(repr(float(px))) - int(n) * Decimal(repr(float(t))))


def d68_step(side: str, edge: float, close: float, armed: bool) -> str | None:
    """D68 on one DAILY close of the underlying: PE arms on close < edge and stops (once armed) on close > edge; CE
    mirrors; a close ON the edge does nothing."""
    if side not in ("PE", "CE"):
        raise ValueError(f"side must be 'PE' or 'CE', got {side!r}")
    pe = side == "PE"
    if not armed:
        return "arm" if ((close < edge) if pe else (close > edge)) else None
    return "stop" if ((close > edge) if pe else (close < edge)) else None


def ist_ms(d: date, hhmm: str) -> int:
    h, m = (int(v) for v in hhmm.split(":"))
    return int(datetime(d.year, d.month, d.day, h, m, tzinfo=IST).timestamp() * 1000)


def day_of(ms: int) -> date:
    return datetime.fromtimestamp(int(ms) / 1000, IST).date()


# ---------------------------------------------------------------------------------------------- inputs / result
@dataclass(frozen=True)
class X3Plan:
    """The exit's parameters (the Lab preset X3's params; defaults = the X2-LIVE reduction)."""

    arm_pct: float = 20             # the arm (+%): R2-01 says 25; 20 = today's T1
    t2_pct: float | None = 40       # a fixed whole-lot target (+%); None = the trail is the target (R2-01)
    trail_pct: float | None = None  # after the arm: stop = running high x (1 - trail/100), floored at the entry (R2-02)
    pre_stop_pct: float | None = None  # SW only, before the arm: stop = entry x (1 - p/100) (R2-03)
    d68: bool = True                # SW only, before the arm: the D68 stock stop on the daily close
    same_day_stop: str = "base_low"  # SD: "base_low" (today's rule) | "nearer" (max(base low, entry x 0.60), R2-05)
    floor_ticks: int = 0            # SD: the stop is at most entry - n ticks (X11; R2-05 says 1)
    cal_cap: str = "day5"           # SW: "day5" (today) | "expiry_1_1515" (+ the 15:15 bar of the session before expiry)

    def __post_init__(self) -> None:
        def num(v: Any) -> bool:
            return isinstance(v, (int, float)) and not isinstance(v, bool) and math.isfinite(float(v))
        if not num(self.arm_pct) or not self.arm_pct > 0:
            raise ValueError(f"arm_pct must be > 0, got {self.arm_pct!r}")
        if self.t2_pct is not None and (not num(self.t2_pct) or not self.t2_pct > self.arm_pct):
            raise ValueError(f"t2_pct must be None or > arm_pct {self.arm_pct}, got {self.t2_pct!r}")
        if self.trail_pct is not None and (not num(self.trail_pct) or not 0 < self.trail_pct < 100):
            raise ValueError(f"trail_pct must be None or in (0, 100), got {self.trail_pct!r}")
        if self.pre_stop_pct is not None and (not num(self.pre_stop_pct) or not 0 < self.pre_stop_pct < 100):
            raise ValueError(f"pre_stop_pct must be None or in (0, 100), got {self.pre_stop_pct!r}")
        if not isinstance(self.d68, bool):  # ValueError like every other param (one error type for the plan)
            raise ValueError(f"d68 must be a bool, got {self.d68!r}")  # noqa: TRY004
        if self.same_day_stop not in SAME_DAY_STOPS:
            raise ValueError(f"same_day_stop must be one of {SAME_DAY_STOPS}, got {self.same_day_stop!r}")
        if isinstance(self.floor_ticks, bool) or not isinstance(self.floor_ticks, int) or self.floor_ticks < 0:
            raise ValueError(f"floor_ticks must be an int >= 0, got {self.floor_ticks!r}")
        if self.cal_cap not in CAL_CAPS:
            raise ValueError(f"cal_cap must be one of {CAL_CAPS}, got {self.cal_cap!r}")


@dataclass(frozen=True)
class X3Ctx:
    """What the walk needs beyond the bars. sessions = the trading days from `day` (inclusive) in order, as far as the
    caller may read (stop them at a data cliff); dte = rules.trading_dte(cal, day, expiry). base_low: the SAME-DAY
    base low (an E2 row's sl_struct, or the proxy base the caller computed); None = unknown. und_close(day) -> the
    underlying's DAILY close or None (no D68 that day); a Mapping {date: close} is accepted too. data_end: a missing day
    after it is OPEN_DATA_END, on / before it a HOLE."""
    day: date
    expiry: date
    dte: int
    side: str
    sessions: Sequence[date]
    base_low: float | None = None
    zone_edge: float | None = None
    und_close: Callable[[date], float | None] | Mapping[date, float | None] | None = None
    data_end: date | None = None
    tf_ms: int = TF_MS
    session_end: Callable[[date], int] | Mapping[date, int] | None = None  # epoch ms; None = pre-CAS


@dataclass(frozen=True)
class X3Result:
    """reason = the exit kind (KINDS); legs = ((1.0, px),) (whole lot); exit_day = the IST date of exit_ms;
    touched_arm = the arm level printed on a bar that did not stop the trade first; mfe / mae = (high / low over the
    walked bars) / entry - 1, in %. Beside: status CLOSED | HOLE | OPEN_DATA_END, raw = the walk's leg reason
    (BE / TRAIL / SL / SAME_BAR_SL / PRE_STOP / T2 / SWING_STOP / DAY5 / EXPIRY / EXPIRY_1 / EOD / OPEN), hold_eff
    SD | SW, hold_days = trading days walked (entry day = 1), stop0 = the initial stop (None = none), flags."""

    reason: str
    legs: tuple[tuple[float, float], ...]
    exit_ms: int
    exit_day: date
    touched_arm: bool
    mfe: float
    mae: float
    status: str = "CLOSED"
    raw: str = ""
    exit_px: float = 0.0
    hold_eff: str = "SW"
    dte: int = 0
    hold_days: int = 1
    stop0: float | None = None
    missing: str | None = None
    flags: tuple[str, ...] = ()


# ---------------------------------------------------------------------------------------------- the hold calendar
def hold_plan(day: date, expiry: date, dte: int, sessions: Sequence[date], cal_cap: str) -> tuple[list[date], bool, str]:
    """(hold days, complete, the final day's reason). SD: [day], EOD. SW: from `day` through the first session that is
    the expiry day (EXPIRY), the session before expiry under cal_cap expiry_1_1515 (EXPIRY_1) or trading day 5 (DAY5),
    in that precedence; complete False = the sessions ran out first (the caller's data cliff)."""
    if dte <= INTRADAY_MAX_DTE:
        return [day], True, "EOD"
    s = list(sessions)
    if not s or s[0] != day or any(b <= a for a, b in pairwise(s)):
        raise ValueError(f"sessions must start at {day} and ascend, got {s[:3]}...")
    out = [day]
    i = 0
    while True:
        d = out[-1]
        if d >= expiry:
            return out, True, "EXPIRY"
        if cal_cap == "expiry_1_1515" and i + 1 < len(s) and s[i + 1] >= expiry:
            return out, True, "EXPIRY_1"
        if i >= HOLD_DAYS:
            return out, True, "DAY5"
        if i + 1 >= len(s):
            return out, False, ""
        i += 1
        out.append(s[i])


# ---------------------------------------------------------------------------------------------- the walk
class _Walk:
    """One position, bar by bar (T-231 exits._Walk, bar mode, with the arm / trail / pre-stop of X3)."""

    def __init__(self, plan: X3Plan, entry: float, tick: float | None, stop0: float | None, kind0: str) -> None:
        self.plan, self.entry, self.tick = plan, entry, tick
        self.t = tick if tick and tick > 0 else MIN_TICK
        self.stop = stop0
        self.stop_kind = kind0
        self.strict = stop0 is not None and px_ge(stop0, entry, self.t)
        self.pending: tuple[float, str] | None = None
        self.levels: list[tuple[float, str]] = [(target_price(entry, plan.arm_pct / 100, tick), "ARM")]
        if plan.t2_pct is not None:
            self.levels.append((target_price(entry, plan.t2_pct / 100, tick), "T2"))
        self.nt = 0
        self.armed = False
        self.legs: list[dict[str, Any]] = []
        self.closed = False
        self.hi = self.lo = entry
        self.last_px: float | None = None
        self.last_ms: int | None = None
        self.day_k = 0
        self.same_bar = False

    def leg(self, px: float, ms: int, reason: str) -> None:
        self.legs.append({"px": float(px), "ms": int(ms), "reason": reason, "day_k": self.day_k})
        self.closed = True

    def want_stop(self, level: float, kind: str) -> None:
        """Raise-only: a request below the current stop / pending stop is ignored."""
        cur = max(x for x in (self.stop, self.pending[0] if self.pending else None, -math.inf) if x is not None)
        if level > cur:
            self.pending = (level, kind)

    def trail_level(self) -> float:
        level = floor_tick(self.hi * (1 - float(self.plan.trail_pct) / 100), self.t)
        level = max(level, self.entry)  # R2-01: the trail never sits below the entry
        return level

    def step(self, ts_end: int, o: float, h: float, lo: float, c: float) -> None:
        if self.pending is not None:  # a change caused by the previous bar applies from this one
            self.stop, self.stop_kind = self.pending
            self.strict, self.pending = False, None
        self.hi, self.lo = max(self.hi, h), min(self.lo, lo)
        self.last_px, self.last_ms = c, ts_end
        t = self.t
        if self.stop is not None:
            hit = px_lt(lo, self.stop, t) if self.strict else px_le(lo, self.stop, t)
            if hit:  # STOP FIRST: checked before any target of the same bar
                through = px_lt(o, self.stop, t) if self.strict else px_le(o, self.stop, t)
                px = o if through else self.stop
                both = self.nt < len(self.levels) and px_ge(h, self.levels[self.nt][0], t)
                if self.stop_kind == "SL":
                    reason = "SAME_BAR_SL" if both else "SL"
                else:
                    reason = self.stop_kind
                if both and self.stop_kind in ("SL", "PRE_STOP"):
                    self.same_bar = True
                self.leg(px, ts_end, reason)
                return
        while self.nt < len(self.levels) and px_ge(h, self.levels[self.nt][0], t):
            level, name = self.levels[self.nt]
            self.nt += 1
            if name == "T2":
                self.leg(max(level, o), ts_end, "T2")
                return
            self.armed = True  # the arm: the stop is SET (not raised) from the next bar (J2)
            self.pending = (self.trail_level(), "TRAIL") if self.plan.trail_pct is not None else (self.entry, "BE")
        if self.armed and self.plan.trail_pct is not None:
            self.want_stop(self.trail_level(), "TRAIL")

    def close_all(self, px: float, ms: int, reason: str) -> None:
        self.leg(px, ms, reason)


def _bars_of(bars: Callable[[date], Any] | Mapping[date, Any], d: date) -> Sequence[Sequence[float]] | None:
    return bars(d) if callable(bars) else bars.get(d)


def _und_of(und: Callable[[date], Any] | Mapping[date, Any] | None, d: date) -> float | None:
    if und is None:
        return None
    v = und(d) if callable(und) else und.get(d)
    return None if v is None else float(v)


def _steps(rows: Sequence[Sequence[float]], d: date, *, hi_ms: int, start_ms: int | None, cut_ms: int | None,
           tf_ms: int) -> list[tuple[int, float, float, float, float]]:
    """(ts_close, o, h, l, c) of day d: bars opening from SESSION_OPEN to hi_ms, at/after start_ms (entry day), closing
    at/before cut_ms (the final day); in time order."""
    lo_ms = ist_ms(d, SESSION_OPEN)
    out = []
    for b in rows:
        ts = int(b[0])
        if ts < lo_ms or ts >= hi_ms or (start_ms is not None and ts < start_ms):
            continue
        if cut_ms is not None and ts + tf_ms > cut_ms:
            continue
        out.append((ts, float(b[1]), float(b[2]), float(b[3]), float(b[4])))
    out.sort(key=lambda x: x[0])
    return [(ts + tf_ms, o, h, lo, c) for ts, o, h, lo, c in out]


def initial_stop(plan: X3Plan, entry: float, tick: float | None, dte: int,
                 base_low: float | None) -> tuple[float | None, str]:
    """(stop, kind) at entry. SD: base_low = the base low, else the 50 % stand-in; nearer = max(base low,
    entry x 0.60); then at most entry - floor_ticks ticks. SW: entry x (1 - pre_stop_pct) or no stop."""
    t = tick if tick and tick > 0 else MIN_TICK
    if dte <= INTRADAY_MAX_DTE:
        if plan.same_day_stop == "base_low":
            sl = float(base_low) if base_low is not None else floor_tick(entry * (1 - SL50_PCT / 100), t)
        else:
            sl = floor_tick(entry * (1 - NEARER_PCT / 100), t)
            if base_low is not None:
                sl = max(sl, float(base_low))
        if plan.floor_ticks:
            cap = ticks_below(entry, plan.floor_ticks, t)
            if px_ge(sl, cap, t):
                sl = cap
        return sl, "SL"
    if plan.pre_stop_pct is not None:
        return floor_tick(entry * (1 - float(plan.pre_stop_pct) / 100), t), "PRE_STOP"
    return None, "SL"


def walk(bars: Callable[[date], Any] | Mapping[date, Any], entry: float, entry_ms: int, tick: float | None,
         plan: X3Plan, ctx: X3Ctx) -> X3Result:
    """Grade ONE whole-lot position (module doc). bars(day) -> that day's 5-min rows [ts_open_ms, o, h, l, c, ...] or
    None / [] (no data), or a Mapping {date: rows}. Every hold day up to the first one without data is asked for, in
    hold order, before the walk (as T-231 exits.grade loads them); a day after ctx.data_end is never asked for."""
    entry = float(entry)
    if not entry > 0:
        raise ValueError(f"entry must be > 0, got {entry!r}")
    if ctx.side not in ("PE", "CE"):
        raise ValueError(f"side must be 'PE' or 'CE', got {ctx.side!r}")
    sd = ctx.dte <= INTRADAY_MAX_DTE
    hold_eff = "SD" if sd else "SW"
    days, complete, final_reason = hold_plan(ctx.day, ctx.expiry, ctx.dte, ctx.sessions, plan.cal_cap)
    stop0, kind0 = initial_stop(plan, entry, tick, ctx.dte, ctx.base_low)

    loaded: list[tuple[date, list]] = []  # T-231 exits.grade loads every hold day up to the first one without data
    missing: tuple[date, str] | None = None
    for k, d in enumerate(days):
        rows = None if (ctx.data_end is not None and d > ctx.data_end) else _bars_of(bars, d)
        rows = [b for b in (rows or []) if day_of(int(b[0])) == d]
        if not rows:
            complete = False
            missing = (d, "OPEN_DATA_END" if ctx.data_end is not None and d > ctx.data_end else "HOLE")
            break
        final = complete and k == len(days) - 1
        loaded.append((d, _steps(rows, d, hi_ms=session_end_ms(d, ctx), start_ms=int(entry_ms) if k == 0 else None,
                                 cut_ms=ist_ms(d, CUT) if final else None, tf_ms=ctx.tf_ms)))

    w = _Walk(plan, entry, tick, stop0, kind0)
    d68_on = plan.d68 and not sd and ctx.zone_edge is not None
    armed68 = False
    for k, (d, st) in enumerate(loaded):
        w.day_k = k
        final = complete and k == len(days) - 1
        for s in st:
            w.step(*s)
            if w.closed:
                break
        if w.closed:
            break
        if final:
            w.close_all(w.last_px if w.last_px is not None else entry, ist_ms(d, CUT), final_reason)
            break
        if d68_on and not w.armed:  # the stock stop ends at the arm (D86; exits.py:442 `not target_seen`)
            und = _und_of(ctx.und_close, d)
            if und is not None:
                act = d68_step(ctx.side, float(ctx.zone_edge), und, armed68)
                if act == "arm":
                    armed68 = True
                elif act == "stop":
                    w.close_all(w.last_px if w.last_px is not None else entry, session_end_ms(d, ctx), "SWING_STOP")
                    break
    flags: list[str] = []
    status = "CLOSED"
    if not w.closed:  # the data ended before the hold did
        status = missing[1] if missing is not None else "OPEN_DATA_END"
        flags.append("open_at_data_end")
        w.day_k = max(len(loaded) - 1, 0)
        w.close_all(w.last_px if w.last_px is not None else entry, w.last_ms or int(entry_ms), "OPEN")
    if w.same_bar:
        flags.append("same_bar_sl")
    lg = w.legs[-1]
    kind = status if status != "CLOSED" else KIND_OF[lg["reason"]]
    return X3Result(kind, ((1.0, lg["px"]),), lg["ms"], day_of(lg["ms"]), w.nt > 0,
                    (w.hi / entry - 1) * 100, (w.lo / entry - 1) * 100, status, lg["reason"], lg["px"], hold_eff,
                    int(ctx.dte), int(lg["day_k"]) + 1, stop0, None if missing is None else missing[0].isoformat(),
                    tuple(sorted(flags)))


def session_end_ms(d: date, ctx: X3Ctx | None = None) -> int:
    """The caller's dated close, or the named pre-CAS default for legacy contexts."""
    end = getattr(ctx, "session_end", None)
    if end is None:
        return ist_ms(d, SESSION_END)
    return int(end(d) if callable(end) else end[d])


__all__ = ["CAL_CAPS", "CUT", "HOLD_DAYS", "IST", "KINDS", "KIND_OF", "MIN_TICK", "NEARER_PCT", "SAME_DAY_STOPS",
           "TF_MS", "X3Ctx", "X3Plan", "X3Result", "d68_step", "day_of", "floor_tick", "hold_plan", "initial_stop",
           "ist_ms", "px_ge", "px_le", "px_lt", "session_end_ms", "target_price", "ticks_below", "walk"]
