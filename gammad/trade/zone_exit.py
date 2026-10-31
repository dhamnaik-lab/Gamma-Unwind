"""Still Watching exits (0.8.9, T-419; D471 Q5 / Q6, D472 X9A, D480, D555). No wall-clock reads.

ONE live position of the Still Watching list (trades.scanner SW, hold carry), walked one COMPLETED 5-min stock bar at a
time. PORT (never imported at runtime) of the T-417 Phase 2 replay that picked the rule:
docs/research/2026-10-14-zone-exits/next/phase2/p2_exits.py, stop S2_CLOSE x exit X9A (`_own_walk`, module docstring
"STOPS" / "X9A TGT->CHAND") and phase1/p1_exits.py `Atr15` (ported as gammad/trade/atr15.py):

  SW_STOP    (S2_CLOSE, p2_exits.zone_hit) a 5-min stock CLOSE at / beyond the 1h stop zone's NEAR edge: CE close <=
             near, PE close >= near (near = gammad.core.h1zones.near_far). An EXIT. Live before AND after the touch.
  SW_TARGET  the daily target FIRST touched by a 5-min stock WICK (zx_exits._target_touched: CE high >= target, PE low
             <= target). NOT an exit: nothing is booked, the position turns TRAILING (p2_exits X9A "the touch bar books
             nothing"). Never fires again. No target -> never trails (X9A = X0 without a target).
  SW_TRAIL   (X9A chandelier) from the touch bar's CLOSE: extreme = CE the highest HIGH / PE the lowest LOW of the
             completed 5-min stock bars since the touch (the touch bar included); at each later bar's OPEN the level =
             extreme -/+ K x ATR (Atr15.value_at_open of that bar: COMPLETED 09:15-anchored 15-min bars only),
             tightening only (CE only up, PE only down); exit on a 5-min stock CLOSE at / beyond it (CE close <=
             level, PE close >= level). An EXIT.
  SW_EVE     Era day-end on expiry eve (15:25 pre-CAS / 15:35 CAS, IST; on_clock). An EXIT.
             Under CAS no stock bar exists after 15:15: until 15:35 only the clock can close the trade;
             the stock-close stop cannot fire on option-only slots (D553).

Order inside one bar (p2_exits._own_walk): (1) at the OPEN the chandelier updates from the extreme of the bars BEFORE
this one (a level set at a close applies from the NEXT bar); (2) the zone stop and the chandelier test this bar's
close -- both stock-triggered fills price at the same option close, so a tie resolves to the first of zone stop,
chandelier (p2_exits: equal ideals -> the first) = SW_STOP; (3) a stop and the target in one bar -> the stop
(BOTH_IN_BAR: the target is not reported); (4) else a target touch -> SW_TARGET; (5) at the CLOSE, once touched, the
extreme takes this bar's high (CE) / low (PE).
Prices: an event's price = the option's 5-min close of that bar (p2_exits' ideal fill), None when the caller has no
option bar; level = the level crossed (near edge / target / chandelier); und_close = the stock 5-min close.
Bars are fed in time order, each once, only bars opening at / after the entry (the caller's job, as zx_exits._prepare's
slots). A bar closing AFTER the eve deadline is never evaluated for a stop: it closes the position SW_EVE (the walk's
deadline; normally on_clock fires first, at the era day-end, 15:25 pre-CAS / 15:35 CAS).
V1 (BREAKDOWN-PE / BREAKOUT-CE) exits never use this module (lane
log A4: BLOCKED); the rule is config-driven (TrailRule.from_cfg) so a later ruling could adopt it.
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, time
from typing import Any, Final, Literal

from gammad.config import IST
from gammad.core.sessions import Era, SessionsError
from gammad.core.v1_hours import last_bar_ms
from gammad.trade.atr15 import Atr15
from gammad.trade.rules import Event, Step

SW_STOP: Final = "SW_STOP"
SW_TARGET: Final = "SW_TARGET"
SW_TRAIL: Final = "SW_TRAIL"
SW_EVE: Final = "SW_EVE"
SW_EVENT_KINDS: Final[tuple[str, ...]] = (SW_STOP, SW_TARGET, SW_TRAIL, SW_EVE)
SW_EXIT_KINDS: Final = frozenset({SW_STOP, SW_TRAIL, SW_EVE})
SwState = Literal["OPEN", "TRAILING", "CLOSED"]
SW_STATES: Final[tuple[str, ...]] = ("OPEN", "TRAILING", "CLOSED")
EPS: Final = 1e-9  # = zx_exits.EPS (the research comparisons' float tolerance)
TF_MS: Final = 300_000  # a 5-min bar closes at ts_ms + TF_MS
TRAIL_K: Final = 3.0  # p2_exits.X9_CHAND_K (config still_watching.trail.k)
ERA_DAY_END: Final = "era_day_end"
EVE_EXIT_HM: Final = ERA_DAY_END  # D555: start of the era's last 5-min bar

SW_STOP_REASON: Final = "stock closed beyond the 1h stop zone"
SW_TARGET_REASON: Final = "daily target reached: the stop now trails"
SW_TRAIL_REASON: Final = "stock closed beyond the trailing stop"


class EveClockError(ValueError):
    """The expiry-eve session clock could not be resolved; callers must refuse the trade."""


def eve_reason(hm: str | None = None) -> str:
    if hm is None or hm == ERA_DAY_END:
        return "expiry eve: closed at the day-end clock"
    return f"expiry eve: closed at {hm}"


@dataclass(frozen=True, slots=True)
class Bar5:
    """One COMPLETED 5-min stock bar; ts_ms = its OPEN (it closes at ts_ms + 5 min)."""

    ts_ms: int
    open: float
    high: float
    low: float
    close: float

    @property
    def close_ms(self) -> int:
        return int(self.ts_ms) + TF_MS


@dataclass(frozen=True)
class TrailRule:
    """The configurable part of the rule (config/strategy.yaml `still_watching:` -> Settings.still_watching)."""

    k: float = TRAIL_K
    atr_len: int = 14
    tf_min: int = 15
    eve_exit_hm: str = EVE_EXIT_HM

    def __post_init__(self) -> None:
        if not (isinstance(self.k, (int, float)) and not isinstance(self.k, bool) and math.isfinite(self.k)
                and self.k > 0):
            raise ValueError(f"TrailRule: k must be > 0, got {self.k!r}")

    @classmethod
    def from_cfg(cls, cfg: Any) -> TrailRule:
        """cfg = Settings, Settings.still_watching (StillWatchingCfg) or None (the defaults)."""
        sw = getattr(cfg, "still_watching", cfg)
        if sw is None:
            return cls()
        tr = sw.trail
        return cls(k=float(tr.k), atr_len=int(tr.atr_len), tf_min=int(tr.tf_min), eve_exit_hm=str(sw.eve_exit_hm))

    def atr(self, bars5: Any = ()) -> Atr15:
        return Atr15(bars5, atr_len=self.atr_len, tf_min=self.tf_min)

    def eve_ms(self, eve_day: date, eras: Sequence[Era] | None = None) -> int:
        """Era day-end (or explicit HH:MM IST) on the expiry eve, epoch ms."""
        if self.eve_exit_hm == ERA_DAY_END:
            try:
                return last_bar_ms(eve_day, eras)
            except (SessionsError, OSError) as exc:
                raise EveClockError(
                    f"Cannot resolve expiry-eve clock for {eve_day} from config/market_sessions.yaml: {exc}"
                ) from exc
        hh, mm = (int(x) for x in self.eve_exit_hm.split(":"))
        dt = datetime.combine(eve_day, time(hh, mm), tzinfo=IST)
        return int(dt.astimezone(UTC).timestamp() * 1000)


DEFAULT_RULE: Final = TrailRule()


@dataclass(frozen=True)
class SwPosition:
    """A Still Watching trade's exit state (trades: stop_top / stop_bottom carry the zone, target_level, target_ms =
    touched_ms, trail_level). trail_level = the chandelier in force during the LAST evaluated bar (set at that bar's
    open); extreme = the stock extreme since the touch (through the last evaluated bar)."""

    option_type: str
    entry: float
    near_edge: float
    far_edge: float
    target: float | None
    expiry_eve_ms: int
    state: SwState = "OPEN"
    touched_ms: int | None = None
    extreme: float | None = None
    trail_level: float | None = None
    exit_px: float | None = None
    exit_ts_ms: int | None = None

    def __post_init__(self) -> None:
        if self.option_type not in ("CE", "PE"):
            raise ValueError(f"SwPosition: option_type must be CE | PE, got {self.option_type!r}")
        if self.state not in SW_STATES:
            raise ValueError(f"SwPosition: unknown state {self.state!r}")
        if not self.entry > 0:
            raise ValueError(f"SwPosition: entry must be > 0, got {self.entry!r}")
        for name in ("near_edge", "far_edge"):
            v = getattr(self, name)
            if v is None or not math.isfinite(float(v)):
                raise ValueError(f"SwPosition: {name} must be a finite price (no zone = no Still Watching entry)")
        # the stop zone sits BELOW price for a CE (near = top >= far = bottom), ABOVE it for a PE (near = bottom)
        if (self.near_edge < self.far_edge - EPS) if self.option_type == "CE" else (self.near_edge > self.far_edge + EPS):
            raise ValueError(f"SwPosition: {self.option_type} near {self.near_edge} / far {self.far_edge} are swapped "
                             "(h1zones.near_far)")

    @property
    def is_open(self) -> bool:
        return self.state != "CLOSED"


def _close(pos: SwPosition, kind: str, reason: str, price: float | None, ts_ms: int, level: float | None,
           und_close: float | None) -> Step:
    ev = Event(kind=kind, price=price, ts_ms=int(ts_ms), reason=reason, level=level,  # type: ignore[arg-type]
               und_close=und_close)
    return Step(replace(pos, state="CLOSED", exit_px=price, exit_ts_ms=int(ts_ms)), (ev,))  # type: ignore[arg-type]


def on_clock(pos: SwPosition, now_ms: int, *, option_ltp: float | None, rule: TrailRule = DEFAULT_RULE) -> Step:
    """At / after expiry_eve_ms (eve_exit_hm on the expiry eve) an open position exits SW_EVE at option_ltp."""
    if pos.state == "CLOSED" or int(now_ms) < int(pos.expiry_eve_ms):
        return Step(pos)  # type: ignore[arg-type]
    hm = datetime.fromtimestamp(pos.expiry_eve_ms / 1000, IST).strftime("%H:%M")
    return _close(pos, SW_EVE, eve_reason(hm), option_ltp, int(now_ms), None, None)


def on_bar(pos: SwPosition, bar5: Bar5, *, option_close: float | None, atr15: float | None,
           rule: TrailRule = DEFAULT_RULE) -> Step:
    """One COMPLETED 5-min stock bar. atr15 = Atr15.value_at_open(bar5.ts_ms) (completed 15-min bars only)."""
    if pos.state == "CLOSED":
        return Step(pos)  # type: ignore[arg-type]
    close_ms = int(bar5.ts_ms) + TF_MS
    if close_ms > int(pos.expiry_eve_ms):  # the walk ends at the eve deadline: never a stop test after it
        return on_clock(pos, close_ms, option_ltp=option_close, rule=rule)
    ce = pos.option_type == "CE"
    hi, lo, c = float(bar5.high), float(bar5.low), float(bar5.close)
    trail = pos.trail_level
    # (1) at this bar's OPEN: the chandelier from the extreme of the bars BEFORE it, tightening only
    if pos.state == "TRAILING" and pos.extreme is not None and atr15 is not None:
        cand = pos.extreme - rule.k * float(atr15) if ce else pos.extreme + rule.k * float(atr15)
        if trail is None or (cand > trail + EPS if ce else cand < trail - EPS):
            trail = cand
    pos = replace(pos, trail_level=trail)
    # (2) the stops at this bar's CLOSE: zone first (a tie resolves to the zone stop)
    near = float(pos.near_edge)
    if (c <= near + EPS) if ce else (c >= near - EPS):
        return _close(pos, SW_STOP, SW_STOP_REASON, option_close, close_ms, near, c)
    if trail is not None and ((c <= trail + EPS) if ce else (c >= trail - EPS)):
        return _close(pos, SW_TRAIL, SW_TRAIL_REASON, option_close, close_ms, trail, c)
    events: tuple[Event, ...] = ()
    # (4) the first target touch (a wick) starts the trail; nothing is booked
    if pos.state == "OPEN" and pos.target is not None:
        tgt = float(pos.target)
        if (hi >= tgt - EPS) if ce else (lo <= tgt + EPS):
            pos = replace(pos, state="TRAILING", touched_ms=close_ms)
            events = (Event(kind=SW_TARGET, price=option_close, ts_ms=close_ms,  # type: ignore[arg-type]
                            reason=SW_TARGET_REASON, level=tgt, und_close=c),)
    # (5) at the CLOSE, once touched: the extreme takes this bar (the touch bar included)
    if pos.state == "TRAILING":
        hx = hi if ce else lo
        ext = hx if pos.extreme is None else (max(pos.extreme, hx) if ce else min(pos.extreme, hx))
        pos = replace(pos, extreme=ext)
    return Step(pos, events)  # type: ignore[arg-type]
