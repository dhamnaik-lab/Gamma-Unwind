"""Trade follow-up rules: the PURE state machine of docs/TRADE-TRACKER-SPEC.md §2-3 (S1.3.0 (ruled, being built); D68, D80, D86).

No I/O, no DB, no Telegram, no clock: every input is an argument, every output a value. The tracker
(gammad/trade/tracker.py) stores a `Position` as its `trades` row and each `Event` as a `trade_events` row;
field names follow those columns (TRACKER §5).

Levels (§2), fixed at entry by `open_position`:
  entry = signal.ltp; T1 = entry x 1.20 and T2 = entry x 1.40, each rounded UP to the contract tick (never below
  +20 % / +40 %; exact decimal arithmetic). INTRADAY SL = signal.sl (the base low). SWING has NO premium SL
  until T1: the D68 stock stop on the underlying's daily close guards it. An initial SL >= entry is tested
  STRICTLY (ltp < sl) so the entry tick cannot stop it; otherwise ltp <= sl.

States x inputs (§3; D86: "D68 stock stop until T1, then SL trails to the entry premium, T2 closes, else day 5"):
  state     input                                   -> new state            event(s)
  OPEN      tick <= SL (intraday only)              -> CLOSED_SL            SL
  OPEN      tick >= T1                              -> T1_TRAIL, sl=entry   T1   (+ T2 -> CLOSED_T2 on the same tick)
  OPEN      daily close THROUGH the edge (swing)    -> OPEN, armed          SWING_ARMED   (no exit that day)
  OPEN      armed, daily close back OVER the edge   -> CLOSED_STOCK_STOP    SWING_STOP    (exit = that day's option close)
  OPEN|T1   15:20, intraday                         -> CLOSED_EOD           EOD           (exit = last LTP)
  OPEN|T1   15:20 on trading day >= 5, swing        -> CLOSED_DAY5          DAY5          (exit = last LTP)
  T1_TRAIL  tick >= T2                              -> CLOSED_T2            T2
  T1_TRAIL  tick <= entry                           -> CLOSED_TRAIL         TRAIL
  T1_TRAIL  daily close (swing)                     -> T1_TRAIL             none: the stock stop ends at T1 (D86)
  OPEN|T1   start-up finds an earlier-day intraday  -> CLOSED_EOD_MISSED    EOD_MISSED
  OPEN|T1   swing at/over expiry before day 5       -> CLOSED_EXPIRY        EXPIRY        (exit = last LTP; W15, D114:
            (the tracker's 15:20 cap)                                                     every list is swing, so an
                                                                                          EXPIRY-list trade meets it
                                                                                          routinely; was: no event)
  CLOSED_*  anything                                -> unchanged            none
Fills: a TARGET (T1 / T2) fills at the triggering tick's LTP -- a gap through a level fills at the tick, as the
backtest fills a gapped bar at its OPEN (`backtest/runner.py` intraday_trade). A gap through T1 and T2 fires T1 then
T2 at the same LTP, never skipping T1; a restart resumes from the persisted state, so a stored T1_TRAIL never re-fires
T1. A STOP (SL while OPEN, TRAIL after T1) exits AT THE STOP LEVEL (D125 F6, T-161: exit_px = the SL / the entry;
grading and P&L read it) and the event keeps the crossing tick as `fill_px` (`slippage_pct` = (fill - stop) / stop);
the forensic trade 3 (entered at 2.10 = its SL, T1 2.55, trail crossed at 1.60) exits 2.10, fill 1.60, slippage -23.8 %.
Hold at entry (D125 F7, a TRADE-TRACKER-SPEC rule, not a STRATEGY-SPEC one): `hold_at_entry` makes a DTE <= 1 entry
INTRADAY (SL = the base low, 15:20 CLOSED_EOD); swing only for DTE >= 2 (`trading_dte`, the cards' DTE rule).
The backtest's TIME (12-bar) and SAME_BAR_SL exits are bar-grading conventions with no live counterpart: live
ticks are ordered, and the 15:20 exits differ from the backtest's grading at the close (§3 note, D75).

Premium comparisons are tick-aware: two prices within 1e-6 of a tick are equal, so float noise can neither miss
a level printed exactly on the tick nor fire one a tick early. The D68 stock test compares the underlying's
close with the zone edge exactly, as `backtest.runner.swing_outcome` does (the parity test pins the two).

V2 Scanner zone family (T-267, S1.7.0, TRADE-TRACKER-SPEC §12, PLAN-T267 §1 "Tracker" + M7 / M11 / M15; the research
exit walk docs/research/2026-10-04-t263/scripts/t263_study.py :269-400 + PLAN-T263 Appendix A5). A V2T trade is a
`ZonePosition` (stored hold `swing`, t1 = t2 = 0.0, live state Z_OPEN): it never meets `on_tick` (no T1 / T2 / trail).
  state    input                                                    -> new state       event
  Z_OPEN   stock 5-min CLOSE beyond the FAR edge of the stop in      -> CLOSED_ZSL       ZSL    (exit = the option LTP
           force (CE close < bottom, PE close > top); only bars                                   at that close)
           whose slot OPENS <= 15:10 (A5) and close after the entry  -> CLOSED_ZTRAIL    ZTRAIL (the stop was a trail step)
  Z_OPEN   a NEW trade-side 1h zone confirmed at T between the       -> Z_OPEN, stop     (a ZSTEP: trade_zone_steps, never
           stop and the 1h close (h1zones.trail_ok)                     moved             a trade_events row)
  Z_OPEN   15:20 on the expiry day                                   -> CLOSED_EXPIRY    EXPIRY (wins over DAY5)
  Z_OPEN   15:20 on zone day >= 5 (the ENTRY day is day 1)           -> CLOSED_DAY5      DAY5   (exit = the last LTP)
A step confirmed at T is the stop only for stock closes STRICTLY after T (t263 walk: `ev[k][0] < tc`); the stop never
moves back (trail_ok: CE bottom only rises, PE top only falls). NO_ZONE (no 1h stop zone at entry, M7 / D251): no stop
EVER, a later zone is never adopted; exits only DAY5 / EXPIRY. The 15:20 DAY5 / EXPIRY exit is the recorded deviation
D251 / M15 (the research closed at 15:30). V1's `hold_day` counts the entry day as 0: the zone family uses
`zone_hold_day` (entry day = 1) and never the V1 helper.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, replace
from datetime import date, datetime, time
from decimal import ROUND_CEILING, Decimal
from typing import TYPE_CHECKING, Any, Final, Literal, get_args

from gammad.config import IST
from gammad.watch.types import Hold

# 0.8.5 (D287(6), PLAN-D287 I7): the zone family's names are type-only here and imported lazily where they run (`_h1`):
# the client package ships no gammad/v2scan, and V1 never needs it
if TYPE_CHECKING:
    from gammad.core.calendar import TradingCalendar
    from gammad.v2scan.h1zones import ZoneEvent

T1_PCT: Final = 0.20  # D80
T2_PCT: Final = 0.40  # D80
HOLD_DAYS: Final = 5  # D58/D68: a swing exits on trading day 5 after the entry day
EOD_REASON: Final = "15:20 intraday close"  # the trades_eod job's EOD event (on_session_end's default reason)
EXPIRY_REASON: Final = "expiry day: closed at 15:20 before day 5"  # on_expiry's EXPIRY event (W15, D114)
MIN_TICK: Final = 0.01  # the finest NSE F&O tick: the rounding grid when a contract's tick is unknown
TICK_EPS: Final = 1e-6  # float-noise tolerance, in ticks

TradeState = Literal[
    "OPEN", "T1_TRAIL", "CLOSED_T2", "CLOSED_TRAIL", "CLOSED_SL", "CLOSED_EOD", "CLOSED_EOD_MISSED",
    "CLOSED_STOCK_STOP", "CLOSED_DAY5", "CLOSED_EXPIRY",
]
EventKind = Literal["T1", "T2", "TRAIL", "SL", "EOD", "EOD_MISSED", "SWING_ARMED", "SWING_STOP", "DAY5", "EXPIRY"]
D68Action = Literal["arm", "stop"]

STATES: Final[tuple[str, ...]] = get_args(TradeState)
EVENT_KINDS: Final[tuple[str, ...]] = get_args(EventKind)
LIVE_STATES: Final = frozenset({"OPEN", "T1_TRAIL"})  # V1 only: every V1 reader (on_tick, carry, retire) keeps it
EXIT_KINDS: Final = frozenset({"T2", "TRAIL", "SL", "EOD", "EOD_MISSED", "SWING_STOP", "DAY5", "EXPIRY"})
_HOLDS: Final = frozenset(get_args(Hold))

# V2 Scanner zone family (T-267, module docstring). Kept OUT of TradeState / EventKind / LIVE_STATES / EXIT_KINDS, so
# V1's readers (and 0.8.3's tracker, which loads only OPEN / T1_TRAIL) never see a V2T trade.
ZoneState = Literal["Z_OPEN", "CLOSED_ZSL", "CLOSED_ZTRAIL", "CLOSED_DAY5", "CLOSED_EXPIRY"]
ZoneEventKind = Literal["ZSL", "ZTRAIL", "DAY5", "EXPIRY"]
ZONE_STATE_ALL: Final[tuple[str, ...]] = get_args(ZoneState)
ZONE_EVENT_KINDS: Final[tuple[str, ...]] = get_args(ZoneEventKind)
ZONE_STATES: Final = frozenset({"Z_OPEN"})  # M11: the live zone state
ZONE_EXIT_KINDS: Final = frozenset(ZONE_EVENT_KINDS)
ZSTEP: Final = "ZSTEP"  # a 1h trail step (trade_zone_steps; M4: owner PERSONAL + dashboard only, never the group)
V2T: Final = "V2T"  # trades.scanner of a zone-family trade
ZONE_HOLD_DAYS: Final = 5  # v2_scanner.hold_days: the ENTRY day is day 1
STOCK_LAST_SLOT: Final = time(15, 10)  # A5: stock 5-min slots opening at or before 15:10 count
STOCK_TF_MS: Final = 300_000
ZSL_REASON: Final = "stock closed beyond the 1h stop zone"
ZTRAIL_REASON: Final = "stock closed beyond the trailed 1h stop zone"
ZDAY5_REASON: Final = f"day-{ZONE_HOLD_DAYS} exit at 15:20"
ZEXPIRY_REASON: Final = "expiry day: closed at 15:20"

# 0.8.9 T-419 "Still Watching" (STRATEGY-SPEC 4.8; T419-CONTRACT names; D471 Q5 / Q6, D472 X9A, D480). The exit walk is
# gammad/trade/zone_exit.py (PURE; SwPosition / on_bar / on_clock); these are its trades / trade_events vocabulary. Like
# the zone family it stays OUT of TradeState / EventKind / LIVE_STATES / EXIT_KINDS, so V1's readers (on_tick, run_daily,
# snapshot_closes, 0.8.8's tracker) never see a Still Watching trade. The kind names EQUAL zone_exit.SW_* (zone_exit
# imports this module, so they are restated here, never imported; tests/test_rules_zone.py pins the equality).
SCANNER_SW: Final = "SW"  # trades.scanner of a Still Watching trade (never shown to a client)
SW_HOLD: Final = "carry"  # trades.hold of a Still Watching trade (schema v11 widened the CHECK)
SwEventKind = Literal["SW_STOP", "SW_TARGET", "SW_TRAIL", "SW_EVE"]
SW_EVENT_KINDS: Final[tuple[str, ...]] = get_args(SwEventKind)
SW_EXIT_KINDS: Final = frozenset({"SW_STOP", "SW_TRAIL", "SW_EVE"})  # SW_TARGET is NOT an exit (the trail starts)
SwTradeState = Literal["SW_OPEN", "SW_TRAILING", "CLOSED_SW_STOP", "CLOSED_SW_TRAIL", "CLOSED_SW_EVE"]
SW_STATE_ALL: Final[tuple[str, ...]] = get_args(SwTradeState)
SW_STATES: Final = frozenset({"SW_OPEN", "SW_TRAILING"})  # the live Still Watching states
# zone_exit.SwPosition.state <-> trades.state (a CLOSED position's stored state is its exit kind's)
SW_STATE_OF: Final[dict[str, str]] = {"OPEN": "SW_OPEN", "TRAILING": "SW_TRAILING"}
SW_CLOSED_STATE: Final[dict[str, str]] = {"SW_STOP": "CLOSED_SW_STOP", "SW_TRAIL": "CLOSED_SW_TRAIL",
                                          "SW_EVE": "CLOSED_SW_EVE"}  # fmt: skip
SW_POS_STATE_OF: Final[dict[str, str]] = {"SW_OPEN": "OPEN", "SW_TRAILING": "TRAILING"}
# every live trade of any family (the tracker's load / open view; the API's "open" filter): V1 OPEN / T1_TRAIL, the V2
# Scanner's Z_OPEN and (0.8.9 T-419) Still Watching's SW_OPEN / SW_TRAILING
OPEN_STATES: Final = LIVE_STATES | ZONE_STATES | SW_STATES


@dataclass(frozen=True, slots=True)
class Event:
    """One follow-up (a `trade_events` row): what happened, at what premium, when."""

    kind: EventKind | ZoneEventKind | SwEventKind
    # the triggering LTP; a STOP (SL / TRAIL): the stop level = the exit (D125 F6); a swing stock event: that day's
    # option close; None = not known
    price: float | None
    ts_ms: int
    reason: str
    level: float | None = None  # the level crossed: T1 / T2 / SL / entry (TRAIL) / zone edge (SWING_*)
    und_close: float | None = None  # SWING_ARMED / SWING_STOP: the underlying's daily close
    fill_px: float | None = None  # SL / TRAIL (D125 F6): the tick that crossed the stop; None = every other kind

    @property
    def is_exit(self) -> bool:
        return self.kind in EXIT_KINDS or self.kind in ZONE_EXIT_KINDS or self.kind in SW_EXIT_KINDS

    @property
    def slippage_pct(self) -> float | None:
        """(fill - stop) / stop in percent for a stop exit (trade 3: (1.60 - 2.10) / 2.10 = -23.8 %); else None."""
        return slippage_pct(self.fill_px, self.price)


def slippage_pct(fill_px: float | None, stop: float | None) -> float | None:
    """(fill - stop) / stop x 100; None when either is unknown or the stop is not > 0 (a trade_events row's fill_px
    and price read through this too)."""
    if fill_px is None or stop is None or not stop > 0:
        return None
    return (float(fill_px) - float(stop)) / float(stop) * 100.0


@dataclass(frozen=True, slots=True)
class Position:
    """A tracked entry's rule state (the rule columns of a `trades` row)."""

    hold: Hold
    option_type: str  # "PE" | "CE"
    entry: float
    t1: float
    t2: float
    sl: float | None  # premium SL: intraday = the base low, then entry after T1; swing = None until T1
    tick_size: float | None  # rupees; None = unknown (levels were rounded to MIN_TICK)
    zone_edge: float | None = None  # swing: the D68 edge (support top for PE, resistance bottom for CE)
    armed: bool = False  # swing: a daily close went through the edge (D68)
    state: TradeState = "OPEN"
    exit_px: float | None = None
    exit_ts_ms: int | None = None

    def __post_init__(self) -> None:
        if self.hold not in _HOLDS:
            raise ValueError(f"hold must be one of {sorted(_HOLDS)}, got {self.hold!r}")
        _side(self.option_type)
        if self.state not in STATES:
            raise ValueError(f"unknown trade state {self.state!r}")
        if not self.entry > 0:
            raise ValueError(f"entry must be > 0, got {self.entry!r}")
        if self.tick_size is not None and not self.tick_size > 0:
            raise ValueError(f"tick_size must be > 0 or None, got {self.tick_size!r}")
        if not self.entry < self.t1 < self.t2:
            raise ValueError(f"levels must satisfy entry < T1 < T2, got {self.entry} / {self.t1} / {self.t2}")
        if self.hold == "intraday" and self.state == "OPEN" and self.sl is None:
            raise ValueError("an OPEN intraday trade needs its premium SL (the base low)")

    @property
    def is_open(self) -> bool:
        return self.state in LIVE_STATES

    @property
    def tick(self) -> float:
        """The comparison / rounding grid: the contract's tick, else MIN_TICK."""
        return self.tick_size or MIN_TICK


@dataclass(frozen=True, slots=True)
class Step:
    """The result of one input: the new position and the events it fired, in order."""

    position: Position | ZonePosition
    events: tuple[Event, ...] = ()


# ---------------------------------------------------------------- ticks and levels


def px_ge(a: float, b: float, tick: float) -> bool:
    """a >= b on a tick grid (a within TICK_EPS of a tick below b counts as equal)."""
    return (a - b) / tick > -TICK_EPS


def px_le(a: float, b: float, tick: float) -> bool:
    """a <= b on a tick grid."""
    return (a - b) / tick < TICK_EPS


def px_lt(a: float, b: float, tick: float) -> bool:
    """a < b by at least TICK_EPS of a tick (the complement of px_ge)."""
    return not px_ge(a, b, tick)


def target_price(entry: float, pct: float, tick: float | None) -> float:
    """entry x (1 + pct) rounded UP to the tick (MIN_TICK when unknown), in exact decimal arithmetic: never below
    the percentage, and the smallest tick that is not."""
    t = Decimal(repr(float(tick or MIN_TICK)))
    if not t > 0:
        raise ValueError(f"tick must be > 0, got {tick!r}")
    raw = Decimal(repr(float(entry))) * (1 + Decimal(repr(float(pct))))
    return float((raw / t).to_integral_value(rounding=ROUND_CEILING) * t)


def open_position(
    *,
    hold: Hold,
    option_type: str,
    entry: float,
    tick_size: float | None,
    sl: float | None = None,
    zone_edge: float | None = None,
    t1_pct: float = T1_PCT,
    t2_pct: float = T2_PCT,
) -> Position:
    """The levels of §2, fixed at entry. intraday: sl = signal.sl (required). swing: sl must be None (no premium SL
    until T1); zone_edge = watchlist.zone_edge (None = no stock stop can apply; the trade runs to T1/T2/day 5)."""
    if hold == "intraday" and sl is None:
        raise ValueError("an intraday trade needs sl = signal.sl (the base low)")
    if hold == "swing" and sl is not None:
        raise ValueError("a swing trade has no premium SL until T1 (TRACKER §2); pass sl=None")
    if not 0 < t1_pct < t2_pct:
        raise ValueError(f"targets must satisfy 0 < t1_pct < t2_pct, got {t1_pct} / {t2_pct}")
    if tick_size is not None and not tick_size > 0:
        raise ValueError(f"tick_size must be > 0 or None, got {tick_size!r}")
    return Position(
        hold=hold,
        option_type=option_type,
        entry=float(entry),
        t1=target_price(entry, t1_pct, tick_size),
        t2=target_price(entry, t2_pct, tick_size),
        sl=None if sl is None else float(sl),
        tick_size=tick_size,
        zone_edge=zone_edge,
    )


# ---------------------------------------------------------------- transitions


def _side(option_type: str) -> str:
    if option_type not in ("PE", "CE"):
        raise ValueError(f"option_type must be 'PE' or 'CE', got {option_type!r}")
    return option_type


def _close(pos: Position, state: TradeState, px: float | None, ts_ms: int) -> Position:
    return replace(pos, state=state, exit_px=px, exit_ts_ms=ts_ms)


def on_tick(pos: Position, ltp: float, ts_ms: int) -> Step:
    """A premium tick: SL (intraday, OPEN), T1 (-> trail to entry), T2, trail stop. Closed -> unchanged."""
    if not pos.is_open:
        return Step(pos)
    tick = pos.tick
    events: list[Event] = []
    if pos.state == "OPEN":
        if pos.hold == "intraday" and pos.sl is not None:
            strict = px_ge(pos.sl, pos.entry, tick)  # SL >= entry: the entry tick itself must not stop it
            if px_lt(ltp, pos.sl, tick) if strict else px_le(ltp, pos.sl, tick):
                # D125 F6: exit AT the SL; the crossing tick is the fill
                ev = Event("SL", pos.sl, ts_ms, "SL hit (the base low)", level=pos.sl, fill_px=ltp)
                return Step(_close(pos, "CLOSED_SL", pos.sl, ts_ms), (ev,))
        if not px_ge(ltp, pos.t1, tick):
            return Step(pos)
        # D380 (2) X03: nothing is booked at T1 -- the whole lot stays open, its SL moved to the entry
        events.append(Event("T1", ltp, ts_ms, "T1 hit: SL moved to entry on the whole lot", level=pos.t1))
        pos = replace(pos, state="T1_TRAIL", sl=pos.entry)
    if px_ge(ltp, pos.t2, tick):
        events.append(Event("T2", ltp, ts_ms, "T2 hit: final target", level=pos.t2))
        return Step(_close(pos, "CLOSED_T2", ltp, ts_ms), tuple(events))
    if px_le(ltp, pos.entry, tick):
        # D125 F6: exit AT the entry (the trailed stop); the crossing tick is the fill
        events.append(Event("TRAIL", pos.entry, ts_ms, "stopped at entry after T1", level=pos.entry, fill_px=ltp))
        return Step(_close(pos, "CLOSED_TRAIL", pos.entry, ts_ms), tuple(events))
    return Step(pos, tuple(events))


def d68_step(option_type: str, edge: float, close: float, armed: bool) -> D68Action | None:
    """D68 on one DAILY close of the underlying. PE: arms on close < edge, stops (once armed) on close > edge;
    CE mirrors. A close ON the edge neither arms nor stops; an armed close through the edge again is nothing."""
    pe = _side(option_type) == "PE"
    if not armed:
        return "arm" if ((close < edge) if pe else (close > edge)) else None
    return "stop" if ((close > edge) if pe else (close < edge)) else None


def on_daily_close(pos: Position, *, und_close: float | None, option_close: float | None, ts_ms: int) -> Step:
    """SWING only: the underlying's daily close through D68. Applies while OPEN (the stock stop ends at T1, D86);
    no edge or no close -> unchanged. A stop exits at option_close (that day's option close, the 15:35 snapshot)."""
    if pos.hold != "swing":
        raise ValueError("on_daily_close is the SWING stock stop; an intraday trade has none")
    if pos.state != "OPEN" or pos.zone_edge is None or und_close is None:
        return Step(pos)
    edge = pos.zone_edge
    act = d68_step(pos.option_type, edge, und_close, pos.armed)
    if act == "arm":
        ev = Event("SWING_ARMED", option_close, ts_ms, "stock closed through the zone edge: stop armed",
                   level=edge, und_close=und_close)
        return Step(replace(pos, armed=True), (ev,))
    if act == "stop":
        ev = Event("SWING_STOP", option_close, ts_ms, "stock closed back over the zone edge: stock stop",
                   level=edge, und_close=und_close)
        return Step(_close(pos, "CLOSED_STOCK_STOP", option_close, ts_ms), (ev,))
    return Step(pos)


def on_session_end(
    pos: Position, *, day_k: int, ltp: float | None, ts_ms: int, hold_days: int = HOLD_DAYS,
    reason: str = EOD_REASON,
) -> Step:  # fmt: skip
    """The 15:20 IST exit (§3, job trades_eod): intraday -> CLOSED_EOD; swing on trading day >= hold_days after
    the entry day (day_k = hold_day(...)) -> CLOSED_DAY5; otherwise unchanged. Exit = ltp (the last LTP).
    `reason` = the intraday EOD event's text: the 15:20 job's by default; close_day (15:35, D98(1)) passes its
    own (FIX2 F8)."""
    if day_k < 0:
        raise ValueError(f"day_k must be >= 0, got {day_k}")
    if not pos.is_open:
        return Step(pos)
    if pos.hold == "intraday":
        ev = Event("EOD", ltp, ts_ms, reason)
        return Step(_close(pos, "CLOSED_EOD", ltp, ts_ms), (ev,))
    if day_k >= hold_days:
        ev = Event("DAY5", ltp, ts_ms, f"swing day-{hold_days} exit")
        return Step(_close(pos, "CLOSED_DAY5", ltp, ts_ms), (ev,))
    return Step(pos)


def on_eod_missed(pos: Position, *, ts_ms: int, price: float | None = None) -> Step:
    """Start-up found an INTRADAY trade from an earlier date still open (§4): CLOSED_EOD_MISSED."""
    if pos.hold != "intraday":
        raise ValueError("EOD_MISSED is for intraday trades; a swing carries over days")
    if not pos.is_open:
        return Step(pos)
    ev = Event("EOD_MISSED", price, ts_ms, "intraday trade still open from an earlier session")
    return Step(_close(pos, "CLOSED_EOD_MISSED", price, ts_ms), (ev,))


def on_expiry(pos: Position, *, ts_ms: int, price: float | None = None) -> Step:
    """The tracker's expiry cap (a swing contract at/over its expiry before day 5, §3): CLOSED_EXPIRY + the EXPIRY
    exit event at `price` (the last LTP; None = not known). W15 (D114, PLAN §4): every list is swing, so an
    EXPIRY-list trade reaches its expiry before day 5 routinely and the client gets a follow-up (was: no event)."""
    if not pos.is_open:
        return Step(pos)
    ev = Event("EXPIRY", price, ts_ms, EXPIRY_REASON)
    return Step(_close(pos, "CLOSED_EXPIRY", price, ts_ms), (ev,))


# ---------------------------------------------------------------- trading days


def hold_day(cal: TradingCalendar, entry_date: date, day: date) -> int:
    """Trading day k of a swing: 0 on the entry day, k = trading days d with entry_date < d <= day (holidays
    from config/nse_holidays.yaml through the calendar)."""
    return cal.trading_days_between(entry_date, day)


def trading_dte(cal: TradingCalendar, day: date, expiry_date: date) -> int:
    """Days to expiry as the cards count them (templates.warnings, STRATEGY-SPEC 2.1): trading days d with
    day < d <= expiry (0 on the expiry day; a past expiry reads 0)."""
    return cal.trading_days_between(day, expiry_date) if expiry_date >= day else 0


INTRADAY_MAX_DTE: Final = 1  # D125 F7: an entry with DTE <= 1 is held intraday; swing only for DTE >= 2

# Independent machine: old tick / daily / session functions never receive these states.
from gammad.v1.states import V1R_EVENT_KINDS, V1R_EXIT_KINDS  # noqa: F401

V1R_STATES: Final = frozenset({"V1R_OPEN", "V1R_TRAILING", "V1R_HALF"})
OPEN_STATES = OPEN_STATES | V1R_STATES


def hold_at_entry(configured: Hold, dte: int) -> Hold:
    """D125 F7 (T-161, a TRADE-TRACKER-SPEC rule): DTE <= INTRADAY_MAX_DTE -> "intraday" (SL = the base low, 15:20
    CLOSED_EOD); otherwise the configured hold (every V1 list is swing, S1.4.0; a same-day config stays same-day)."""
    return "intraday" if dte <= INTRADAY_MAX_DTE else configured


def day5_precedes_expiry(cal: TradingCalendar, entry_date: date, expiry_date: date,
                         hold_days: int = HOLD_DAYS) -> bool:
    """True when trading day hold_days falls strictly before the expiry day (§3: D59 puts swings >= 8 days out)."""
    return expiry_date > entry_date and cal.trading_days_between(entry_date, expiry_date) > hold_days


# ---------------------------------------------------------------- V2 Scanner zone family (T-267, docstring table)


@dataclass(frozen=True, eq=False)
class StopZone:
    """A 1h stop zone (underlying rupees) confirmed at confirmed_ms: the same three fields as
    gammad.v2scan.h1zones.StopZone, defined HERE so a stored stop (`stop_zone_of`, `steps_of`) is read without importing
    gammad.v2scan (0.8.5 D297: the client package no longer ships it, and a 0.8.4 client store still holds V2 trades with a
    stop). Equal to (and hashed as) any stop zone with the same top / bottom / confirmed_ms, h1zones' own included, so
    the owner's zone family compares and dedupes exactly as before; h1zones reads only the three attributes."""

    top: float
    bottom: float
    confirmed_ms: int

    def _key(self) -> tuple[Any, Any, Any]:
        return (self.top, self.bottom, self.confirmed_ms)

    def __eq__(self, other: object) -> bool:
        try:
            theirs = (other.top, other.bottom, other.confirmed_ms)  # type: ignore[attr-defined]
        except AttributeError:
            return NotImplemented
        return self._key() == theirs

    def __hash__(self) -> int:
        return hash(self._key())  # = the hash of h1zones.StopZone (a frozen dataclass hashes its field tuple)


@dataclass(frozen=True, slots=True)
class ZonePosition:
    """A V2T trade's rule state. `initial` = the 1h stop zone at entry (None = NO_ZONE, or not known after a restart
    when `no_zone` is False); `steps` = the trail steps in confirmation order (each a StopZone whose confirmed_ms is
    its 1h close T). The stop in force for a stock close at tc is the newest of them confirmed strictly before tc."""

    option_type: str  # "PE" | "CE"
    entry: float
    entry_ts_ms: int  # the trigger candle's CLOSE (Signal.ts_ms; t263 t_e)
    initial: StopZone | None
    steps: tuple[StopZone, ...] = ()
    no_zone: bool = False  # M7: no stop at entry -> no stop ever
    state: ZoneState = "Z_OPEN"
    exit_px: float | None = None
    exit_ts_ms: int | None = None

    def __post_init__(self) -> None:
        _side(self.option_type)
        if self.state not in ZONE_STATE_ALL:
            raise ValueError(f"unknown zone trade state {self.state!r}")
        if not self.entry > 0:
            raise ValueError(f"entry must be > 0, got {self.entry!r}")
        if self.no_zone and (self.initial is not None or self.steps):
            raise ValueError("a NO_ZONE trade holds no stop zone and never adopts one (M7)")

    @property
    def is_open(self) -> bool:
        return self.state in ZONE_STATES

    @property
    def sl(self) -> None:  # the tracker's shared write-ahead reads these two V1 columns
        return None

    @property
    def armed(self) -> bool:
        return False

    @property
    def stop(self) -> StopZone | None:
        """The newest stop zone (the trades.stop_* columns): the last step, else the initial zone."""
        return self.steps[-1] if self.steps else self.initial


def open_zone_position(*, option_type: str, entry: float, entry_ts_ms: int, stop: StopZone | None) -> ZonePosition:
    """A V2T trade at entry: stop = the 1h stop zone (h1zones.stop_zone_for), None = NO_ZONE (M7)."""
    return ZonePosition(option_type=option_type, entry=float(entry), entry_ts_ms=int(entry_ts_ms), initial=stop,
                        no_zone=stop is None)  # fmt: skip


def zone_hold_day(cal: TradingCalendar, entry_date: date, day: date) -> int:
    """Zone day k: 1 on the ENTRY day (t263 hold_days: the first n trading days from the entry day), k = 1 + trading
    days d with entry_date < d <= day. NOT `hold_day` (V1: entry day = 0), M15."""
    return cal.trading_days_between(entry_date, day) + 1


def stock_slot_counts(bar_open_ms: int) -> bool:
    """A5: a stock 5-min bar counts for a breach only when its slot OPENS at or before 15:10 IST."""
    return datetime.fromtimestamp(int(bar_open_ms) / 1000, IST).time() <= STOCK_LAST_SLOT


def zone_stop_at(pos: ZonePosition, close_ms: int) -> tuple[StopZone | None, bool]:
    """(the stop in force for a stock close at close_ms, is it a trail step). A step confirmed at T applies only to
    closes STRICTLY after T; before every step, the initial zone. (None, False) = no stop (NO_ZONE, or the initial
    zone is not known after a restart and no step applies yet)."""
    for s in reversed(pos.steps):
        if int(s.confirmed_ms) < int(close_ms):
            return s, True
    return pos.initial, False


def _far_edge(option_type: str, z: StopZone) -> float:
    return float(z.bottom) if option_type == "CE" else float(z.top)


def _h1() -> Any:
    """gammad.v2scan.h1zones, imported when the zone family runs (owner only, 0.8.5 PLAN-D287 I7: never at import)."""
    from gammad.v2scan import h1zones

    return h1zones


def on_stock_close(pos: ZonePosition, *, bar_open_ms: int, close: float, opt_ltp: float | None) -> Step:
    """One stock 5-min bar close (t263 walk): a CLOSE beyond the far edge of the stop in force exits at the option
    LTP of that close (ZTRAIL when that stop is a trail step, else ZSL). Ignored: a closed trade, a bar that does not
    close after the entry, a slot opening after 15:10 (A5), no stop in force."""
    if not pos.is_open:
        return Step(pos)
    close_ms = int(bar_open_ms) + STOCK_TF_MS
    if close_ms <= pos.entry_ts_ms or not stock_slot_counts(bar_open_ms):
        return Step(pos)
    stop, is_step = zone_stop_at(pos, close_ms)
    if stop is None or not _h1().breached(pos.option_type, float(close), stop):
        return Step(pos)
    kind: ZoneEventKind = "ZTRAIL" if is_step else "ZSL"
    px = None if opt_ltp is None else float(opt_ltp)
    ev = Event(kind, px, close_ms, ZTRAIL_REASON if is_step else ZSL_REASON, level=_far_edge(pos.option_type, stop),
               und_close=float(close))  # fmt: skip
    state: ZoneState = "CLOSED_ZTRAIL" if is_step else "CLOSED_ZSL"
    return Step(replace(pos, state=state, exit_px=px, exit_ts_ms=close_ms), (ev,))


def _zone_side(option_type: str) -> str:
    return "buy" if _side(option_type) == "CE" else "sell"


def on_zone_event(pos: ZonePosition, ev: ZoneEvent) -> tuple[ZonePosition, StopZone | None]:
    """A 1h zone confirmed at ev.confirmed_ms (H1ZoneBook.append / new_zones). It becomes the newest stop when it is
    trade-side, confirmed after the entry and after the newest step (events apply in time order, so a re-fed hour is
    a no-op), and h1zones.trail_ok(side, newest stop, ev). Returns (position, the new stop | None). NO_ZONE / closed /
    no known stop -> never a step (M7)."""
    if not pos.is_open or pos.no_zone:
        return pos, None
    side_tag = getattr(ev, "zone_side", "") or ""
    if side_tag and side_tag != _zone_side(pos.option_type):
        return pos, None
    t = int(ev.confirmed_ms)
    if t <= pos.entry_ts_ms or (pos.steps and t <= int(pos.steps[-1].confirmed_ms)):
        return pos, None
    cur = pos.stop
    if cur is None or not _h1().trail_ok(pos.option_type, cur, ev):
        return pos, None
    new = _h1().StopZone(float(ev.top), float(ev.bottom), t)
    return replace(pos, steps=(*pos.steps, new)), new


def on_zone_session_end(pos: ZonePosition, *, day_k: int, expiry_day: bool, ltp: float | None, ts_ms: int,
                        hold_days: int = ZONE_HOLD_DAYS) -> Step:  # fmt: skip
    """The 15:20 exit of a V2T trade (D251 / M15): on (or past) the expiry day -> CLOSED_EXPIRY (t263 end_reason: the
    expiry wins when it is the hold's last day); else on zone day >= hold_days (entry day = 1) -> CLOSED_DAY5; exit =
    ltp (the last LTP). Otherwise unchanged."""
    if day_k < 1:
        raise ValueError(f"zone day_k must be >= 1 (the entry day is day 1), got {day_k}")
    if not pos.is_open:
        return Step(pos)
    px = None if ltp is None else float(ltp)
    if expiry_day:
        return Step(replace(pos, state="CLOSED_EXPIRY", exit_px=px, exit_ts_ms=ts_ms),
                    (Event("EXPIRY", px, ts_ms, ZEXPIRY_REASON),))  # fmt: skip
    if day_k >= hold_days:
        return Step(replace(pos, state="CLOSED_DAY5", exit_px=px, exit_ts_ms=ts_ms),
                    (Event("DAY5", px, ts_ms, ZDAY5_REASON),))  # fmt: skip
    return Step(pos)


def on_zone_expiry(pos: ZonePosition, *, ts_ms: int, price: float | None = None) -> Step:
    """Start-up found a V2T trade past its expiry still open: CLOSED_EXPIRY at `price` (None = not known)."""
    if not pos.is_open:
        return Step(pos)
    return Step(replace(pos, state="CLOSED_EXPIRY", exit_px=price, exit_ts_ms=ts_ms),
                (Event("EXPIRY", price, ts_ms, ZEXPIRY_REASON),))  # fmt: skip


def stop_zone_of(top: float | None, bottom: float | None, confirmed_ms: int | None) -> StopZone | None:
    """A stored stop (trades.stop_* / a snapshot's v2t.stop) as a StopZone; None when any part is missing. Never
    imports gammad.v2scan (D297: the client reads a 0.8.4 store's V2 trades without it)."""
    if top is None or bottom is None or confirmed_ms is None:
        return None
    return StopZone(float(top), float(bottom), int(confirmed_ms))


def steps_of(rows: Sequence[object]) -> tuple[StopZone, ...]:
    """trade_zone_steps rows (seq order, dicts) as StopZones (never imports gammad.v2scan, D297)."""
    out: list[StopZone] = []
    for r in rows:
        d = r if isinstance(r, dict) else dict(r)  # type: ignore[call-overload]
        out.append(StopZone(float(d["top"]), float(d["bottom"]), int(d["confirmed_ms"])))
    return tuple(out)
