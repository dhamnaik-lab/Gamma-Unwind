"""Trade tracker: persists the rules.py state machine as `trades` / `trade_events` rows and routes follow-ups
(docs/TRADE-TRACKER-SPEC.md §1-8; S1.3.0; D68, D80, D86, D88).

The rules live in `gammad/trade/rules.py` (pure, T0); this module only feeds them and stores the result.

Life of a trade
  open      `trade_hook(signal_id, signal, meta, ctx, message_id)` -- awaited by AlertRouter.on_signal (B1) after
            the GROUP decision; message_id = the entry's GROUP message (None: gated / muted / paused / failed).
            Edition filter inside (§1): owner tracks both signal kinds (separate trades); client tracks E1 only.
            W15 (D114, PLAN-W15 §2 "Trade opening"): in BOTH editions an E1 opens a trade only when
            `gates.tier(ctx, gates_cfg) == "ENTRY"` (4 of 4 gates; a missing input or a disabled gate is not a pass)
            and the router did not demote it (`dup_stock` in ctx.gate_reasons, R4); the owner's arming-signal trades are unchanged.
            W16 (T-161, D125 F5-F7): every owner arming signal opens a trade, gated included, with trades.gate_status = the
            verdict; hold = intraday when DTE <= 1 at entry (rules.hold_at_entry), else ctx.hold; a stop exit
            records exit_px = the stop level and the crossing tick as trade_events.fill_px (schema v8).
            S1.9.0 (D380 (6), owner edition only): a CARRIED contract's signal (either kind) opens a trade only as an
            ENTRY R4 did not demote (gates.tier, uncapped for the owner); the client edition still refuses every one.
  ticks     `on_tick(sid, ltp, ts_ms)` for sid in `open_ids` (T1 / T2 / trail / same-day SL).
  15:20     `run_eod(now)`         same-day hold -> CLOSED_EOD; swing on trading day >= 5 -> CLOSED_DAY5; swing at/over
                                   expiry -> CLOSED_EXPIRY + the EXPIRY follow-up (W15: routine, INFO; every
                                   list is swing, so an EXPIRY-list trade meets its expiry before day 5).
  15:35     `snapshot_closes(now)` last LTP per open swing contract = that day's option close (persisted); then
            `close_intraday(now)` closes any intraday trade still open (a restart between 15:20 and 15:30, D98(1)).
            No intraday trade is opened for an entry at/after 15:20 (D98(1)); the alert itself is unchanged.
  16:05     `run_daily(adapter, now)` the underlying's daily close through D68 (swing, OPEN only); a candle not
                                   dated `day` defers that trade to `run_daily_retry` at 09:21 next trading day.
  start     `load(now)` re-reads every open trade (the DB is the truth); an intraday trade still open from an
            earlier session -> CLOSED_EOD_MISSED; then `await resume(now_ms)` sends what load() wrote and
            re-sends, once, every `pending` event older than 60 s left by the previous process.

V2 Scanner trades (T-267, TRADE-TRACKER-SPEC §12, rules.py "zone family"; PLAN-T267 §1 "Tracker" + M4 / M7 / M11 / M15)
  open      the same trade_hook: a signal whose snapshot holds a `v2t` block is a V2T trade (scanner 'V2T', hold swing,
            t1 = t2 = 0.0, state Z_OPEN, stop_* = snapshot.v2t.stop {top, bottom, confirmed_ms} | None = NO_ZONE,
            und_entry = snapshot.v2t.und_px). V1's gates / DTE hold rule / off_client refusal do not apply (D248(7):
            owner off-client entries are tracked "as backtested"); gates.v2t_tier(snapshot), when it exists, must say
            ENTRY. A carried contract's V2T signal never opens a trade, in either edition (D157(2); D380 (6) is V1 only).
  ticks     never `on_tick` (open_ids holds V1 contracts only, M11); `on_zone_tick(sid, ltp, ts_ms)` for sid in
            `zone_ids` only records the option's last LTP (the DAY5 / EXPIRY exit price and on_stock_bar's default).
  5-min     `on_stock_bar(und_sid, bar, opt_ltp=None)` per underlying 5-min close: ZSL / ZTRAIL (rules.on_stock_close).
  1h        `on_hour_close(und_sid, events)` with H1ZoneBook.append()'s events: a trail step is written to
            trade_zone_steps ('pending') in the SAME transaction as trades.stop_*, then offered to
            `router.send_zone_step(trade, step)` (M4: owner PERSONAL + dashboard, never the group; a router without
            it records the step 'suppressed', nothing is sent).
  15:20     run_eod: EXPIRY on the expiry day, else DAY5 on zone day 5 (entry day = 1), at the last LTP (D251 / M15).
  start     load() restores Z_OPEN rows with their stop, steps and the entry's initial zone (signals.payload_json).

Still Watching trades (0.8.9 T-419, STRATEGY-SPEC 4.8; D471 Q5 / Q6, D472 X9A, D480; both editions while
still_watching.enabled -- the tracker's `still_watching` argument; None = off = the pre-0.8.9 carried rules)
  open      the same trade_hook: a CARRIED entry of the group's entry kind at tier ENTRY (gates.tier still_watching=True)
            not demoted by R4 opens scanner 'SW', hold 'carry', state SW_OPEN, t1 = t2 = 0.0, from the signal's
            snapshot["sw"] levels (sw_entry_levels, picked once by the app; the entry card prints the same block): the 1h
            stop zone in stop_* (fixed), target_level, und_entry. No zone = no trade (logged).
  5-min     `on_sw_stock_bar(und_sid, bar)` per COMPLETED stock bar: Atr15 + zone_exit.on_bar -> SW_STOP / SW_TARGET (not
            an exit: SW_TRAILING, target_ms) / SW_TRAIL; trail_level persisted whenever it moves.
  15:15     `sw_clock(now_ms)` (the app's bar timer): the expiry-eve exit SW_EVE.
  start     load() restores SW_OPEN / SW_TRAILING rows (zone, target, touch, trail); `sw_seed_bars` rebuilds the
            extreme since the touch and warms the ATR from the app's 5-min bars. run_eod / run_daily / on_tick never
            touch them (hold 'carry', SW_* states).

Write-ahead (§6): a transition and its trade_events rows (status 'pending') commit in ONE transaction BEFORE
any send (`_write_ahead`); the send result then moves each row to the router's status (+ tg_message_id).
UNIQUE (trade_id, kind) refuses a second event of a kind, so a crash can duplicate one message, never lose one.
One trade's events are delivered sequentially (a per-trade lock, rows in id order).

Pinned interfaces (W12 wave 1):
  router side (B1): `await router.send_followup(trade: Mapping, event: Mapping)` ->
      (status, message_id | None, error | None), status in FOLLOWUP_STATUSES. EOD_MISSED is PERSONAL/dashboard only.
  dashboard: `push(dict)` with {"type": "trade", **trade row, "symbol", "strike", "events": [event rows]} on every
      transition and every delivery-status change (§8).
"""

from __future__ import annotations

import asyncio
import json
import math
import sqlite3
from collections.abc import Awaitable, Callable, Iterable, Mapping
from dataclasses import dataclass, replace
from datetime import date, datetime, time
from typing import TYPE_CHECKING, Any, Final, Protocol

from gammad import edition
from gammad.alerts import gates as gates_mod
from gammad.alerts.templates import _is_day_end
from gammad.config import IST, GatesCfg, now_ist
from gammad.core import session_clock, v1_hours, v1_params
from gammad.log import get_logger
from gammad.store.db import insert_zone_step, list_zone_steps, update_zone_step
from gammad.trade import rules, zone_exit
from gammad.v1 import budget as v1_budget
from gammad.v1 import exits as v1_exits
from gammad.v1 import position as v1_position
from gammad.v1 import stops as v1_stops
from gammad.watch.feed import MODE_FULL, MODE_QUOTE, Instrument
from gammad.watch.types import ContractMeta, Signal, SignalContext

if TYPE_CHECKING:
    from gammad.core.calendar import TradingCalendar

log = get_logger(__name__)

OPTION_SEGMENT: Final = "NSE_FNO"
UNDERLYING_SEGMENT: Final = "NSE_EQ"

FOLLOWUP_STATUSES: Final = frozenset({"sent", "failed", "muted", "paused", "no_entry_msg", "suppressed"})
# 0.8.2 T-214 F3 (D186(2)): the follow-up outcomes recorded without a log line; `failed` logs an ERROR, any other status
# (`no_entry_msg`: legacy, the router no longer produces it) a WARNING -- a follow-up is never dropped silently
QUIET_STATUSES: Final = frozenset({"sent", "muted", "paused", "suppressed"})
ST_PENDING: Final = "pending"
RESEND_MIN_AGE_MS: Final = 60_000  # §6: pending rows older than this at start are re-sent once
# 0.8.5 D297: a CLIENT store's open V2T trade (0.8.4 ran the V2 Scanner for clients) is retired at load: a terminal
# state no rule produces, one 'suppressed' event carrying the reason (no V2 word in the state: a client surface may list it)
ZONE_RETIRED_STATE: Final = "CLOSED_RETIRED"
ZONE_RETIRED_EVENT: Final = "RETIRED"
ZONE_RETIRED_REASON: Final = "v2_removed"

EOD_AT: Final = time(15, 20)  # job trades_eod
DAILY_AT: Final = time(16, 5)  # job trades_daily
DAILY_RETRY_AT: Final = time(9, 21)  # job trades_daily_retry (a deferred daily close, next trading day)
CANDLE_NOT_PUBLISHED: Final = "DH-907"  # T-180: Dhan's answer at 16:05 before the day's daily candle exists
NO_DAILY_FROM: Final = time(7, 0)  # D72: Dhan daily history is unreliable 07:00-08:00 IST; never fetch then
NO_DAILY_UNTIL: Final = time(8, 0)
# FIX2 F8: the EOD event close_day writes (D98(1)) is not the 15:20 job's
CLOSE_DAY_REASON: Final = "close_day 15:35: intraday closed at the last price"

TIER_ENTRY: Final = "ENTRY"  # PLAN-W15 §2: the only E1 tier that opens a trade (D114)
DUP_STOCK: Final = "dup_stock"  # PLAN-W15 §2: the router's R4 demotion token in ctx.gate_reasons

JOB_EOD: Final = "trades_eod"
JOB_DAILY: Final = "trades_daily"
JOB_DAILY_RETRY: Final = "trades_daily_retry"

TRADE_COLUMNS: Final[tuple[str, ...]] = (
    "entry_signal_id", "kind", "hold", "entry_date", "entry_ts_ms", "security_id", "underlying_security_id",
    "list_name", "expiry_date", "option_type", "lot_size", "tick_size", "entry", "sl", "t1", "t2", "zone_edge",
    "armed", "armed_date", "last_close_date", "last_close_px", "state", "exit_px", "exit_ts_ms", "entry_msg_id",
    "spec_version", "updated_ms", "gate_status",
)
# v10 (T-267): a V2T trade also writes these; a V1 INSERT names TRADE_COLUMNS only (its row reads scanner 'V1', NULLs)
ZONE_TRADE_COLUMNS: Final[tuple[str, ...]] = (
    *TRADE_COLUMNS, "scanner", "stop_top", "stop_bottom", "stop_confirmed_ms", "und_entry",
)
ZSTEP_REASON: Final = "1h stop zone trailed"
# v11 (T-419): a Still Watching trade also writes its target / touch / trail (stop_* = the 1h zone fixed at entry)
SW_TRADE_COLUMNS: Final[tuple[str, ...]] = (*ZONE_TRADE_COLUMNS, "target_level", "target_ms", "trail_level", "sw_bar_ms")
# an event found by a restart's re-walk (its stock bar closed while the process was down): fired late, once
SW_LATE_NOTE: Final = "late: the stock bar closed while the app was down"
_SW_REASONS: Final[dict[str, str]] = {
    "SW_STOP": zone_exit.SW_STOP_REASON, "SW_TARGET": zone_exit.SW_TARGET_REASON,
    "SW_TRAIL": zone_exit.SW_TRAIL_REASON, "SW_EVE": zone_exit.eve_reason(),
}  # fmt: skip

_REASONS: Final[dict[str, str]] = {
    "T1": "T1 hit: SL moved to entry on the whole lot",  # D380 (2) X03: nothing is booked (rules.on_tick)
    "T2": "T2 hit: final target",
    "TRAIL": "stopped at entry after T1",
    "SL": "SL hit (the base low)",
    "EOD": "15:20 intraday close",
    "EOD_MISSED": "intraday trade still open from an earlier session",
    "SWING_ARMED": "stock closed through the zone edge: stop armed",
    "SWING_STOP": "stock closed back over the zone edge: stock stop",
    "DAY5": f"swing day-{rules.HOLD_DAYS} exit",
    "EXPIRY": rules.EXPIRY_REASON,
    "ZSL": rules.ZSL_REASON,
    "ZTRAIL": rules.ZTRAIL_REASON,
}
# a V2T trade's DAY5 / EXPIRY read the zone family's wording (no "before day 5": its expiry exit is routine)
_ZONE_REASONS: Final[dict[str, str]] = {
    "ZSL": rules.ZSL_REASON, "ZTRAIL": rules.ZTRAIL_REASON, "DAY5": rules.ZDAY5_REASON, "EXPIRY": rules.ZEXPIRY_REASON,
}
# T-098: close_day's EOD (15:35, D98(1)) is not the 15:20 job's; the card's own clock rule decides (templates.
# _is_day_end: the EVENT's IST time at/after EOD_DAY_END_FROM 15:30), so a re-send from the DB reads the same.
EOD_DAY_END_REASON: Final = "day-end close at the last price seen"


def event_reason(kind: str, ts_ms: Any, scanner: str | None = None) -> str:
    """The follow-up wording of one trade event (handed to the router as event["reason"])."""
    if scanner == rules.V2T and kind in _ZONE_REASONS:
        return _ZONE_REASONS[kind]
    if kind in _SW_REASONS:  # T-419: Still Watching kinds exist only on scanner SW trades
        return _SW_REASONS[kind]
    if kind == "EOD" and _is_day_end(ts_ms):
        return EOD_DAY_END_REASON
    return _REASONS.get(kind, kind)


class FollowupRouter(Protocol):
    """The router side of a follow-up (B1): GROUP reply to trade['entry_msg_id'] (EOD_MISSED -> PERSONAL; 0.8.2 T-214:
    no entry message -> a standalone message to the chat the entry went to, never dropped)."""

    async def send_followup(
        self, trade: Mapping[str, Any], event: Mapping[str, Any]
    ) -> tuple[str, int | None, str | None]: ...


class DailyCandles(Protocol):
    """The one adapter call the tracker makes (`BrokerAdapter.daily_candles`)."""

    def daily_candles(
        self, security_id: int, exchange_segment: str, from_date: date, to_date: date, instrument: str = ...
    ) -> list[Any]: ...


def ist_date(ts_ms: int) -> date:
    return datetime.fromtimestamp(ts_ms / 1000, IST).date()


def _ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def event_level(kind: str, trade: Mapping[str, Any]) -> float | None:
    """The level an event crossed, derived from the trade row (so a re-sent row after a restart carries it too)."""
    if kind == "T1":
        return trade.get("t1")
    if kind == "T2":
        return trade.get("t2")
    if kind == "TRAIL":
        return trade.get("entry")
    if kind == "SL":
        return trade.get("sl")
    if kind in ("SWING_ARMED", "SWING_STOP"):
        return trade.get("zone_edge")
    if kind in ("ZSL", "ZTRAIL", rules.ZSTEP):  # the far edge of the stored (newest) stop zone
        return trade.get("stop_bottom") if trade.get("option_type") == "CE" else trade.get("stop_top")
    if kind == "SW_STOP":  # T-419: the NEAR edge of the 1h stop zone (h1zones.near_far: CE top, PE bottom)
        return trade.get("stop_top") if trade.get("option_type") == "CE" else trade.get("stop_bottom")
    if kind == "SW_TARGET":
        return trade.get("target_level")
    if kind == "SW_TRAIL":  # the chandelier in force at the exit bar (persisted with the exit, one transaction)
        return trade.get("trail_level")
    return None


def v2t_block(signal: Signal) -> dict[str, Any] | None:
    """The V2 Scanner block of a signal's snapshot (gammad.v2scan.trigger: snapshot["v2t"]); None = a V1 signal."""
    snap = signal.snapshot if isinstance(signal.snapshot, dict) else None
    block = snap.get("v2t") if snap is not None else None
    return block if isinstance(block, dict) else None


def _stop_from(obj: Any) -> rules.StopZone | None:
    """A snapshot's v2t.stop ({top, bottom, confirmed_ms} or a StopZone) as a StopZone; None when absent / partial."""
    if obj is None:
        return None
    if isinstance(obj, Mapping):
        top, bottom, cms = obj.get("top"), obj.get("bottom"), obj.get("confirmed_ms")
    else:
        top, bottom, cms = getattr(obj, "top", None), getattr(obj, "bottom", None), getattr(obj, "confirmed_ms", None)
    try:
        return rules.stop_zone_of(top, bottom, cms)
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------- 0.8.9 T-419 Still Watching: the entry levels
SW_SNAPSHOT_KEY: Final = "sw"  # = templates.SW_SNAPSHOT_KEY: the entry card reads the SAME block the trade opens from
SW_NO_ZONE: Final = "no_zone"  # app._sw_gate's block reason when the 1h stop zone is missing (no zone = no entry)


@dataclass(frozen=True)
class SwLevels:
    """A Still Watching entry's stock levels, picked ONCE at the entry (sw_entry_levels) and carried on the signal's
    snapshot["sw"] (to_snapshot): the entry card prints them and open_trade opens the trade from them, so the two never
    disagree. near / far = h1zones.near_far of the 1h stop zone; target = the daily zone target (None = NO_TARGET: the
    trade never trails, X9A = X0); und_px = the stock reference price both were picked at."""

    near_edge: float
    far_edge: float
    target: float | None
    zone_top: float
    zone_bottom: float
    zone_confirmed_ms: int
    und_px: float

    def to_snapshot(self) -> dict[str, Any]:
        return {"near_edge": self.near_edge, "far_edge": self.far_edge, "target": self.target,
                "zone_top": self.zone_top, "zone_bottom": self.zone_bottom,
                "zone_confirmed_ms": self.zone_confirmed_ms, "und_px": self.und_px}  # fmt: skip


def sw_ref_px(bars5: Iterable[Any], signal_bar_open_ms: int, spot: float | None = None) -> float | None:
    """The stock reference price of a Still Watching entry = zx_levels.levels_detail (:282-284): the close of the LAST
    5-min stock bar of the entry day opening at / before the signal bar's open (forming_candle: the signal bar itself
    when present, else the last bar before it), else `spot` (trade['spot']); None when neither is a positive price."""
    best: tuple[int, float] | None = None
    for b in bars5 or ():
        got = _bar_open_close(b)
        if got is None or got[0] > int(signal_bar_open_ms):
            continue
        if best is None or got[0] >= best[0]:
            best = got
    if best is not None and best[1] > 0:
        return float(best[1])
    return float(spot) if spot is not None and float(spot) > 0 else None


def sw_entry_levels(option_type: str, und_px: float | None, entry_ms: int, book: Any, daily_zones: Any) -> SwLevels | None:
    """THE Still Watching level pick (the replayed truth docs/research/2026-10-14-zone-exits/scripts/zx_levels.py
    levels_detail :304-330; the T-417 Phase 2 replay used it unchanged): the 1h STOP zone = H1ZoneBook.stop_zone(side, px,
    entry_ms) on the 60-min bars CLOSED at / before the entry (signal close) -- CE: the nearest live BUY zone wholly below
    px, PE: the nearest live SELL zone wholly above (h1zones.stop_zone_for; no book / no zone = None = NO ENTRY, never a
    guess) -- and the daily TARGET = pick_levels (levels.py, NOT pick_levels_daily: zx_levels.pick_target) on the daily
    zones of the 500 bars before the trade date: CE = the nearest SELL zone's BOTTOM above px, PE = the nearest BUY zone's
    TOP below px (None = NO_TARGET). Live prices are as-traded (the replay's adjusted level x F(D), F(D) = 1 on D)."""
    if und_px is None or not (float(und_px) > 0) or option_type not in ("CE", "PE"):
        return None
    from gammad.core import h1zones  # lazy: numpy-backed, only when a Still Watching entry is priced

    stop = h1zones.stop_zone_for(book, option_type, float(und_px), int(entry_ms))
    if stop is None:
        return None
    near, far = h1zones.near_far(option_type, stop)
    target: float | None = None
    if daily_zones is not None:
        from gammad.core.levels import pick_levels

        try:
            lv = pick_levels(daily_zones, float(und_px))
        except ValueError:
            lv = None
        z = None if lv is None else (lv.resistance if option_type == "CE" else lv.support)
        if z is not None:
            target = float(z.bottom) if option_type == "CE" else float(z.top)
    return SwLevels(near_edge=float(near), far_edge=float(far), target=target, zone_top=float(stop.top),
                    zone_bottom=float(stop.bottom), zone_confirmed_ms=int(stop.confirmed_ms), und_px=float(und_px))


def sw_levels_of(snapshot: Any) -> SwLevels | None:
    """The SwLevels a signal's snapshot["sw"] carries (to_snapshot), or None when absent / partial / not finite."""
    snap = snapshot if isinstance(snapshot, Mapping) else None
    block = snap.get(SW_SNAPSHOT_KEY) if snap is not None else None
    if not isinstance(block, Mapping):
        return None
    try:
        vals = {k: float(block[k]) for k in ("near_edge", "far_edge", "zone_top", "zone_bottom", "und_px")}
        cms = int(block["zone_confirmed_ms"])
        tgt = block.get("target")
        target = None if tgt is None else float(tgt)
    except (KeyError, TypeError, ValueError):
        return None
    if not all(math.isfinite(v) for v in vals.values()) or (target is not None and not math.isfinite(target)):
        return None
    return SwLevels(target=target, zone_confirmed_ms=cms, **vals)


def sw_daily_zones(conn: sqlite3.Connection, trade_date: str, symbol: str | None) -> Any:
    """The daily ZoneState of `symbol` for trade_date: the levels row of the latest run of that date (mark_levels: the
    zones over the daily bars BEFORE the trade date, zones_json), or None when there is none / it is unreadable."""
    if not symbol:
        return None
    try:
        got = conn.execute(
            "SELECT l.zones_json FROM levels l LEFT JOIN runs r ON r.run_id = l.run_id WHERE l.trade_date = ?"
            " AND l.symbol = ? AND l.zones_json IS NOT NULL ORDER BY COALESCE(r.started_ist, '') DESC, l.run_id DESC"
            " LIMIT 1", (trade_date, str(symbol)),
        ).fetchone()  # fmt: skip
    except sqlite3.Error:
        log.exception("Still Watching: levels of %s on %s unreadable", symbol, trade_date)
        return None
    if got is None or not got[0]:
        return None
    from gammad.jobs.mark_levels import zone_state_of

    try:
        return zone_state_of(json.loads(got[0]))
    except (TypeError, ValueError, KeyError):
        log.error("Still Watching: zones_json of %s on %s unreadable; no daily target", symbol, trade_date)
        return None


def zone_exit_fields(bar: Any) -> tuple[int, float, float, float, float]:
    """(open ts, open, high, low, close) of a 5-min stock bar: an object with ts_ms / open / high / low / close (a Candle,
    a Bar5) or a row [ts, o, h, l, c, ...] (= atr15._fields)."""
    if hasattr(bar, "ts_ms"):
        return int(bar.ts_ms), float(bar.open), float(bar.high), float(bar.low), float(bar.close)
    return int(bar[0]), float(bar[1]), float(bar[2]), float(bar[3]), float(bar[4])


def _bar_open_close(bar: Any) -> tuple[int, float] | None:
    """(slot open ms, close) of a stock 5-min bar: a Bar (ts_ms = the OPEN, D14) or a row [ts, o, h, l, c, ...]."""
    try:
        if hasattr(bar, "ts_ms"):
            return int(bar.ts_ms), float(bar.close)
        return int(bar[0]), float(bar[4])
    except (TypeError, ValueError, IndexError):
        return None


class TradeTracker:
    """See the module docstring. One instance per Runtime, sharing the Runtime's sqlite connection."""

    def __init__(
        self,
        conn: sqlite3.Connection,
        router: FollowupRouter | None,
        *,
        spec_version: str,
        push: Callable[[dict[str, Any]], Any] | None = None,
        calendar: TradingCalendar | None = None,
        now: Callable[[], datetime] = now_ist,
        wall_ms: Callable[[], int] | None = None,
        gates_cfg: GatesCfg | None = None,
        still_watching: Any = None,
    ) -> None:
        """`gates_cfg` = Settings.gates, the config the router tiers with (W15: an E1 trades only at tier ENTRY);
        None = GatesCfg() defaults (every gate enabled). `still_watching` (0.8.9 T-419) = Settings.still_watching (or
        Settings / a mapping): its `enabled` switches the Still Watching trades on (edition.still_watching_on) and its
        trail / eve_exit_hm build the exit rule (zone_exit.TrailRule.from_cfg). None = OFF: a tracker the app did not
        configure keeps the pre-0.8.9 carried rules (the client refuses a carried entry, D157 (2); the owner tracks his
        carried ENTRY as a V1 trade, D380 (6)) -- a Still Watching trade needs the app's zone inputs to open at all."""
        self.conn = conn
        self.sw_on = still_watching is not None and edition.still_watching_on(still_watching)
        try:
            self.sw_rule = zone_exit.TrailRule.from_cfg(still_watching) if still_watching is not None else \
                zone_exit.DEFAULT_RULE
        except (AttributeError, TypeError, ValueError):  # a partial config (a mapping / test double): the defaults
            self.sw_rule = zone_exit.DEFAULT_RULE
        # T-419: trade id -> its SwPosition; option / underlying -> open SW trade ids; underlying -> its Atr15 (every
        # 5-min stock bar fed, the previous session's for the warm-up); trade id -> the last bar open it was walked on
        self._sw_pos: dict[int, zone_exit.SwPosition] = {}
        self._sw_by_sid: dict[int, list[int]] = {}
        self._sw_by_und: dict[int, list[int]] = {}
        self._sw_atr: dict[int, Any] = {}
        self._sw_last_bar: dict[int, int] = {}
        # underlying -> live stock bars held until its restart re-walk ran (sw_seed_bars / sw_release); absent = live
        self._sw_hold: dict[int, list[tuple[int, float, float, float, float]]] = {}
        # called with the underlying id when a Still Watching trade opens (the app seeds that stock's 5-min bars)
        self.on_sw_open: Callable[[int], Any] | None = None
        self.gates_cfg = gates_cfg if gates_cfg is not None else GatesCfg()
        self.router = router
        self.spec_version = spec_version
        self.push = push
        self._cal = calendar
        self._now = now
        self._wall_ms = wall_ms or (lambda: _ms(self._now()))
        self._open: dict[int, dict[str, Any]] = {}  # trade id -> trades row (a cache: the DB is the truth)
        self._by_sid: dict[int, list[int]] = {}  # contract security_id -> open trade ids
        # sid -> (last LTP, the IST date of its tick). A price belongs to ONE trading day: read only via _ltp_on(sid,
        # today), so a contract that does not tick on an exit day never exits at yesterday's price (overnight service).
        self._last_ltp: dict[int, tuple[float, date]] = {}
        self._contract: dict[int, tuple[str | None, float | None]] = {}  # trade id -> (symbol, strike)
        self._own: set[int] = set()  # event ids written by THIS process and not yet delivered
        self._locks: dict[int, asyncio.Lock] = {}
        self._tasks: set[asyncio.Task[Any]] = set()
        self._daily_done: date | None = None  # the last daily-close day fully processed by this process
        # V2T (T-267): trade id -> its rule state (the steps live in trade_zone_steps); option / underlying -> trade ids;
        # trade id -> [(seq, confirmed_ms)] of steps not yet applied to a stock close (trade_zone_steps.applied_ms NULL)
        self._zone_pos: dict[int, rules.ZonePosition] = {}
        self._zone_by_sid: dict[int, list[int]] = {}
        self._zone_by_und: dict[int, list[int]] = {}
        self._unapplied: dict[int, list[tuple[int, int]]] = {}
        self._own_steps: set[tuple[int, int]] = set()  # (trade id, seq) written by THIS process, not yet delivered
        self.loaded = False
        self._v1r_pos: dict[int, v1_position.V1Position] = {}
        self._v1r_wait: set[int] = set()
        self._v1r_limits: dict[tuple[int, int], tuple[float, int]] = {}
        self._v1r_replay_cursor: dict[int, int] = {}

    # ------------------------------------------------------------------ accessors

    @property
    def calendar(self) -> TradingCalendar:
        if self._cal is None:
            from gammad.core.calendar import default_calendar

            self._cal = default_calendar()
        return self._cal

    @property
    def open_ids(self) -> Mapping[int, list[int]]:
        """Contract security_id -> open V1 trade ids. `sid in tracker.open_ids` is the handle_tick filter (§4) of the
        on_tick path; a V2T (Z_OPEN) contract is NEVER in it (M11: T1 / T2 / trail must never reach a V2T trade)."""
        return self._by_sid

    @property
    def zone_ids(self) -> Mapping[int, list[int]]:
        """Contract security_id -> open V2T (Z_OPEN) trade ids: the on_zone_tick filter (M11)."""
        return self._zone_by_sid

    def zone_unds(self) -> set[int]:
        """The underlyings of open V2T trades: their 5-min bars (on_stock_bar) and 1h closes (on_hour_close) must keep
        coming across carry days, listed today or not."""
        return set(self._zone_by_und)

    @property
    def sw_ids(self) -> Mapping[int, list[int]]:
        """T-419: contract security_id -> open Still Watching trade ids (the option LTP filter: on_zone_tick)."""
        return self._sw_by_sid

    def sw_unds(self) -> set[int]:
        """T-419: the underlyings of open Still Watching trades: their 5-min bars (on_sw_stock_bar) must keep coming
        across carry days, listed today or not."""
        return set(self._sw_by_und)

    def sw_positions(self) -> dict[int, zone_exit.SwPosition]:
        return dict(self._sw_pos)

    def sw_touched_ms(self, und_sid: int) -> list[int]:
        """The target-touch times and last walked bars of the open Still Watching trades on an underlying (the app's
        re-hydration / re-walk window starts at the earliest of them)."""
        out = []
        for tid in self._sw_by_und.get(int(und_sid), ()):
            pos = self._sw_pos.get(tid)
            if pos is not None and pos.touched_ms is not None:
                out.append(int(pos.touched_ms))
            if tid in self._sw_last_bar:
                out.append(int(self._sw_last_bar[tid]))
        return out

    def open_trades(self) -> list[dict[str, Any]]:
        return [dict(r) for r in sorted(self._open.values(), key=lambda r: int(r["id"]))]

    def open_instruments(self) -> list[Instrument]:
        """Every open trade's contract (FULL, NSE_FNO) and underlying (QUOTE, NSE_EQ), for the subscription union
        at load_day / _subscribe (§4): a swing contract no longer on today's list keeps its ticks. M11: a V2T (Z_OPEN)
        trade's option AND underlying are included (the exit reads the option LTP at a stock close)."""
        contracts = sorted({int(r["security_id"]) for r in self._open.values()})
        unds = sorted({int(r["underlying_security_id"]) for r in self._open.values()
                       if r.get("underlying_security_id") is not None})  # fmt: skip
        return [(OPTION_SEGMENT, s, MODE_FULL) for s in contracts] + [(UNDERLYING_SEGMENT, s, MODE_QUOTE) for s in unds]

    def state_counts(self, trade_date: str | None = None) -> dict[str, int]:
        """Trades per state (operator summary, §7); trade_date = entry_date filter, None = every trade."""
        sql = "SELECT state, COUNT(*) FROM trades" + (" WHERE entry_date = ?" if trade_date else "") + " GROUP BY state"
        return {str(r[0]): int(r[1]) for r in self.conn.execute(sql, (trade_date,) if trade_date else ())}

    def trade_view(self, trade_id: int) -> dict[str, Any] | None:
        row = self.conn.execute("SELECT * FROM trades WHERE id = ?", (trade_id,)).fetchone()
        if row is None:
            return None
        return self._view(dict(row))

    def list_trades(self, *, trade_date: str | None = None, state: str = "open") -> list[dict[str, Any]]:
        """GET /api/trades?date=&state=open|all (§8): trade rows + symbol/strike + their events. The client
        edition never holds an arming-signal trade (trade_hook refuses it); the filter below is belt and braces."""
        where, args = [], []
        if trade_date:
            where.append("entry_date = ?")
            args.append(trade_date)
        if state == "open":  # T-267: both families' live states (V1 OPEN / T1_TRAIL + V2T Z_OPEN)
            where.append(f"state IN ({', '.join('?' for _ in rules.OPEN_STATES)})")
            args.extend(sorted(rules.OPEN_STATES))
        kinds = edition.visible_kinds()
        where.append(f"kind IN ({', '.join('?' for _ in kinds)})")
        args.extend(kinds)
        rows = self.conn.execute(
            "SELECT * FROM trades WHERE " + " AND ".join(where) + " ORDER BY entry_ts_ms, id", args
        ).fetchall()
        return [self._view(dict(r)) for r in rows]

    # ------------------------------------------------------------------ open (router hook)

    async def trade_hook(
        self, signal_id: int, signal: Signal, meta: ContractMeta, ctx: SignalContext, message_id: int | None
    ) -> None:
        """AlertRouter.on_signal's hook (pinned): open a trade for a tracked, RECORDED entry. Never raises."""
        try:
            self.open_trade(signal_id, signal, meta, ctx, message_id)
        except Exception:  # a tracker bug must never break signal routing
            log.exception("trade tracker: opening a trade for signal %s failed", signal_id)

    def open_trade(
        self, signal_id: int, signal: Signal, meta: ContractMeta, ctx: SignalContext | None, message_id: int | None
    ) -> int | None:
        """The trades row for one entry (levels fixed now, §2). Returns the trade id, or None when the edition
        does not track this signal, the levels cannot be built, or signal_id already has a trade."""
        if signal.kind not in edition.visible_kinds():
            return None  # client: never the arming signal (D81/D86)
        if isinstance(signal.snapshot, dict) and "v1r" in signal.snapshot:
            return self._open_v1r_trade(signal_id, signal, meta, ctx, message_id)
        carried = (ctx is not None and getattr(ctx, "carried", False)) or getattr(meta, "carried", False)
        block = v2t_block(signal)
        if carried and block is None and self.sw_on:
            # 0.8.9 T-419 (D471 Q10 "switch on for clients", D480): with still_watching.enabled a carried entry is a
            # STILL WATCHING trade in BOTH editions (scanner SW, hold carry, the zone exits of zone_exit.py); the
            # refusal below holds only while the switch is off
            return self._open_sw_trade(signal_id, signal, meta, ctx, message_id)
        if carried and (edition.is_client() or block is not None):
            # 0.8.1 (D157(2), D161(2)): carry is WATCH-ONLY -- a carried entry signal of EITHER kind never opens a trade,
            # whatever tier a caller computed (this tracker re-tiers entries itself, and the owner's arming signal
            # opens trades); tools/check_client_package.py L11 scans this file's comments for the kind names.
            # S1.9.0 (D380 (6) "Lift the cap now (owner edition)"): this refusal holds in the CLIENT edition (and for a
            # V2 Scanner row, which D380 (6) does not touch); the OWNER's carried ENTRY is tracked below
            log.info("trade tracker: %s signal %s is on a carried contract; watch only, not tracked", signal.kind,
                     signal_id)  # fmt: skip
            return None
        if block is not None:  # T-267: the V2 Scanner's zone family (never V1's gates, levels or hold rule)
            return self._open_zone_trade(signal_id, signal, meta, ctx, message_id, block)
        if signal.kind == "E1" and ctx is not None and getattr(ctx, "off_client", False):
            # 0.8.2 T-202 (D176(1)): an entry on a contract not on the client's lists is a PERSONAL card only, never a
            # tracked trade; the router runs no hook for it, and this refuses it whatever a caller computed (belt and
            # braces, the carried precedent above). The owner's arming-signal trades are unchanged.
            log.info("trade tracker: E1 signal %s is not on the client's lists (personal only); not tracked", signal_id)
            return None
        if signal.kind == "E1" or carried:  # W15 (D114): both editions, ENTRY only; the owner's arming-signal trades are
            # unchanged. S1.9.0 (D380 (6)): an OWNER carried signal of either kind opens a trade only as an ENTRY that
            # R4 did not demote (the router hooks only those; this refuses any other whatever a caller computed)
            tier = gates_mod.tier(ctx, self.gates_cfg)
            demoted = ctx is not None and DUP_STOCK in ctx.gate_reasons
            if tier != TIER_ENTRY or demoted:
                log.info("trade tracker: %s signal %s is %s%s%s; only an ENTRY opens a trade", signal.kind, signal_id,
                         tier, " (dup_stock)" if demoted else "", " (carried)" if carried else "")  # fmt: skip
                return None
        hold = ctx.hold if ctx is not None else None
        if hold not in ("intraday", "swing"):
            log.error("trade tracker: signal %s has no hold style (ctx=%r); not tracked", signal_id, ctx)
            return None
        entry_date = ist_date(signal.ts_ms)
        try:
            expiry = date.fromisoformat(str(meta.expiry_date))
        except ValueError:
            log.error("trade tracker: signal %s has no readable expiry %r; not tracked", signal_id, meta.expiry_date)
            return None
        # D125 F7 (T-161, TRADE-TRACKER-SPEC §2): DTE <= 1 at entry -> intraday (SL = the base low, 15:20 exit)
        dte = rules.trading_dte(self.calendar, entry_date, expiry)
        at_entry = rules.hold_at_entry(hold, dte)
        if at_entry != hold:
            log.info("trade tracker: signal %s is DTE %d at entry; held %s, not %s (D125 F7)", signal_id, dte,
                     at_entry, hold)  # fmt: skip
        hold = at_entry
        entry_at = datetime.fromtimestamp(signal.ts_ms / 1000, IST).time()
        if hold == "intraday" and entry_at >= EOD_AT:
            # D98(1): the 15:20 intraday exit has already passed (D77: a 15:25-bar signal fires 15:30, untradeable);
            # the ALERT is unchanged, no trade is opened (no follow-ups, no open row carried into 15:35).
            log.info("trade tracker: intraday signal %s at %s is at/after the %s exit; not tracked", signal_id,
                     entry_at.isoformat("seconds"), EOD_AT.isoformat("minutes"))  # fmt: skip
            return None
        tick = getattr(meta, "tick_size", None)  # S1.3.0 ContractMeta.tick_size (S13); None = unknown
        tick = float(tick) if tick else None
        edge = meta.zone_edge if meta.zone_edge is not None else (ctx.zone_edge if ctx is not None else None)
        try:
            pos = rules.open_position(
                hold=hold,
                option_type=meta.option_type,
                entry=signal.ltp,
                tick_size=tick,
                sl=signal.sl if hold == "intraday" else None,
                zone_edge=edge if hold == "swing" else None,
            )
        except ValueError as exc:
            log.error("trade tracker: signal %s levels refused (%s); not tracked", signal_id, exc)
            return None
        if hold == "swing" and not rules.day5_precedes_expiry(self.calendar, entry_date, expiry):
            # W15 (D114, PLAN §4): routine now that every list is swing (EXPIRY lists expire within the week)
            log.info("trade tracker: swing signal %s expires %s before trading day %d after %s (D59 cap);"
                     " the 15:20 expiry cap will close it CLOSED_EXPIRY", signal_id, meta.expiry_date, rules.HOLD_DAYS,
                     entry_date)  # fmt: skip
        lot = meta.lot_size if meta.lot_size is not None else (ctx.lot_size if ctx is not None else None)
        row: dict[str, Any] = {
            "entry_signal_id": int(signal_id),
            "kind": signal.kind,
            "hold": hold,
            "entry_date": entry_date.isoformat(),
            "entry_ts_ms": int(signal.ts_ms),
            "security_id": int(signal.security_id),
            "underlying_security_id": meta.underlying_security_id,
            "list_name": meta.list_name,
            "expiry_date": meta.expiry_date,
            "option_type": meta.option_type,
            "lot_size": lot,
            "tick_size": tick,
            "entry": pos.entry,
            "sl": pos.sl,
            "t1": pos.t1,
            "t2": pos.t2,
            "zone_edge": pos.zone_edge,
            "armed": 0,
            "armed_date": None,
            "last_close_date": None,
            "last_close_px": None,
            "state": pos.state,
            "exit_px": None,
            "exit_ts_ms": None,
            "entry_msg_id": message_id,
            "spec_version": self.spec_version,
            "updated_ms": self._wall_ms(),
            # D125 F5: every owner arming signal is tracked, gated included; the verdict rides on the row (surfaces mark it)
            "gate_status": ctx.gate_status if ctx is not None else None,
        }
        cur = self.conn.execute(
            f"INSERT OR IGNORE INTO trades ({', '.join(TRADE_COLUMNS)}) VALUES ({', '.join('?' for _ in TRADE_COLUMNS)})",
            tuple(row[c] for c in TRADE_COLUMNS),
        )
        self.conn.commit()
        if not cur.rowcount:
            log.warning("trade tracker: signal %s already has a trade; ignored", signal_id)
            return None
        row["id"] = int(cur.lastrowid or 0)
        self._contract[row["id"]] = (meta.symbol, meta.strike)  # the entry's own contract (T-170: keyed per trade)
        self._cache(row)
        self._push(row)
        log.info("trade %s opened: %s %s %s entry %.2f SL %s T1 %.2f T2 %.2f (signal %s, msg %s)", row["id"],
                 signal.kind, hold, meta.symbol, pos.entry, pos.sl, pos.t1, pos.t2, signal_id, message_id)  # fmt: skip
        return int(row["id"])

    @property
    def v1r_ids(self) -> set[int]:
        return {int(r["security_id"]) for r in self._open.values() if r["state"] in rules.V1R_STATES}

    def v1r_unds(self) -> set[int]:
        return {int(r["underlying_security_id"]) for r in self._open.values()
                if r["state"] in rules.V1R_STATES and r.get("underlying_security_id") is not None}

    def _v1r_initial(self, row: Mapping[str, Any], block: Mapping[str, Any]) -> v1_position.V1Position:
        resolved = v1_stops.Resolved(**block["stop"])
        pick = v1_params.parse_exit("exit", block["exit"])
        hist = block.get("history", {})
        return v1_position.V1Position.open(
            float(row["entry"]), int(block["break_ts"]), str(row["option_type"]), resolved, pick,
            date.fromisoformat(str(row["expiry_date"])),
            v1_exits.History(*(tuple(tuple(b) for b in hist.get(k, ())) for k in ("option", "stock", "h1"))),
        )

    def _open_v1r_trade(self, signal_id, signal, meta, ctx, message_id):
        if signal.kind != "E1" or gates_mod.v1r_tier(signal.snapshot) != TIER_ENTRY:
            return None
        if ctx is not None and ctx.off_client:
            return None
        block = signal.snapshot["v1r"]
        row = dict.fromkeys(TRADE_COLUMNS)
        row.update(entry_signal_id=signal_id, kind="E1", scanner="V1", entry_date=ist_date(signal.ts_ms).isoformat(),
                   entry_ts_ms=signal.ts_ms, security_id=signal.security_id,
                   underlying_security_id=meta.underlying_security_id, list_name=meta.list_name,
                   expiry_date=meta.expiry_date, option_type=meta.option_type, lot_size=meta.lot_size,
                   tick_size=meta.tick_size, entry=float(signal.ltp), t2=0., zone_edge=block["level"],
                   armed=0, state="V1R_OPEN", entry_msg_id=message_id, spec_version=self.spec_version,
                   updated_ms=self._wall_ms(), gate_status="pass", und_entry=block["stock_close"],
                   sw_bar_ms=None)
        try:
            pos = self._v1r_initial(row, block)
        except (KeyError, TypeError, ValueError):
            log.exception("trade tracker: invalid rebuild entry snapshot")
            return None
        row.update(hold=pos.exit_pick.form, sl=None if pos.resolved.cap is None else
                   pos.entry_px * (1 - pos.resolved.cap),
                   t1=self._v1r_limit(pos.exit_pick, pos.entry_px),
                   stop_bottom=pos.resolved.level if pos.side == "PE" else None,
                   stop_top=pos.resolved.level if pos.side == "CE" else None,
                   stop_confirmed_ms=pos.resolved.confirmed_ms)
        columns = (*ZONE_TRADE_COLUMNS, "sw_bar_ms")
        cur = self.conn.execute(
            f"INSERT OR IGNORE INTO trades ({', '.join(columns)}) VALUES ({', '.join('?' for _ in columns)})",
            tuple(row[c] for c in columns))
        self.conn.commit()
        if not cur.rowcount:
            return None
        tid = row["id"] = int(cur.lastrowid)
        self._v1r_pos[tid] = pos
        self._contract[tid] = (meta.symbol, meta.strike)
        self._cache(row)
        self._push(row)
        return tid

    @staticmethod
    def _v1r_limit(pick, entry):
        if pick.family == "HALF":
            return entry * 1.2
        if pick.family == "TARGET":
            return entry * (1 + pick.param)
        limits = [TradeTracker._v1r_limit(p, entry) for p in pick.members]
        return min((p for p in limits if p > 0), default=0.)

    def v1r_budget(self, day: date) -> list[v1_budget.BudgetTrade]:
        rows = self.conn.execute(
            "SELECT t.*, s.payload_json FROM trades t JOIN signals s ON s.id=t.entry_signal_id "
            "WHERE t.state LIKE '%V1R_%' AND (t.entry_date=? OR t.state IN ('V1R_OPEN','V1R_HALF',"
            "'V1R_TRAILING') OR t.exit_ts_ms>=?) ORDER BY t.id",
            (day.isoformat(), _ms(datetime.combine(day, time(), IST))))
        out = []
        for r in rows:
            kind = r["state"].removeprefix("CLOSED_") if r["state"].startswith("CLOSED_") else None
            out.append(v1_budget.BudgetTrade(str(r["security_id"]), json.loads(r["payload_json"])["symbol"], r["option_type"],
                       r["state"] in rules.V1R_STATES, kind,
                       int(r["sw_bar_ms"]) + 300_000 if r["sw_bar_ms"] is not None else None))
        return out

    def _v1r_apply(self, tid, pos, events, *, cursor=None, late=False):
        row = self._open[tid]
        terminal = next((e for e in reversed(events) if e.kind != "V1R_HALF"), None)
        ids = []
        try:
            self.conn.execute("UPDATE trades SET state=?, exit_px=?, exit_ts_ms=?, "
                              "sw_bar_ms=COALESCE(?,sw_bar_ms), updated_ms=? WHERE id=?",
                              (pos.state, terminal.price if terminal else None,
                               terminal.ts_ms if terminal else None, cursor, self._wall_ms(), tid))
            for ev in events:
                cur = self.conn.execute(
                    "INSERT OR IGNORE INTO trade_events (trade_id,kind,ts_ms,price,fill_px,status,reply_to,error) "
                    "VALUES (?,?,?,?,?,?,?,?)", (tid, ev.kind, ev.ts_ms, ev.price, ev.price, ST_PENDING,
                                                row.get("entry_msg_id"), SW_LATE_NOTE if late else None))
                if cur.rowcount:
                    ids.append(int(cur.lastrowid))
            self.conn.commit()
        except BaseException:
            self.conn.rollback()
            raise
        row.update(state=pos.state, exit_px=terminal.price if terminal else None,
                   exit_ts_ms=terminal.ts_ms if terminal else None)
        if cursor is not None:
            row["sw_bar_ms"] = cursor
        self._v1r_pos[tid] = pos
        self._own.update(ids)
        if not pos.remaining:
            self._uncache(tid)
        self._schedule(tid, ids)
        self._push(row)

    def on_v1r_slot(self, und, stock_bar, option_bar, *, sid=None, h1_closes=None, replay=False):
        """Walk aligned CLOSED arrays; live pending closes fill only on a subsequent tick."""
        for tid, old in list(self._v1r_pos.items()):
            row = self._open.get(tid)
            if row is None or row.get("underlying_security_id") != und or (sid is not None and
                    row["security_id"] != sid) or (tid in self._v1r_wait and not replay):
                continue
            ts = int(option_bar[0])
            if ts <= old.entry_ts or (old.last_ts is not None and ts <= old.last_ts):
                continue
            day = ist_date(ts)
            last = ts == v1_hours.last_bar_ms(day)
            pos, events = v1_position.step(old, v1_position.Slot(
                                          None if stock_bar is None else tuple(stock_bar), tuple(option_bar), h1_closes,
                                          last, day >= old.expiry, None))
            fill = self._v1r_limits.pop((tid, ts), None)
            if fill is not None and not replay:
                events = [replace(e, price=fill[0], ts_ms=fill[1]) if e.kind in ("V1R_HALF", "V1R_TARGET")
                          else e for e in events]
            cursor = (events[-1].decided_close_ms - 300_000 if events and
                      events[-1].decided_close_ms is not None else ts)
            self._v1r_apply(tid, pos, events, cursor=cursor,
                            late=replay and ts > self._v1r_replay_cursor.get(tid, -1))

    def on_v1r_tick(self, sid, px, ts):
        if px is None or not math.isfinite(px) or px <= 0:
            return
        self._last_ltp[sid] = (px, ist_date(ts))
        for tid, pos in list(self._v1r_pos.items()):
            row = self._open.get(tid)
            if row is None or row["security_id"] != sid or tid in self._v1r_wait:
                continue
            if pos.pending is not None and ts >= pos.pending.decided_close_ms:
                # close_now gives an already decided stop/trail priority; it does not rename it.
                new, events = v1_position.close_now(pos, px, ts, "V1R_EOD")
                self._v1r_apply(tid, new, events, cursor=pos.pending.decided_close_ms - 300_000)
            elif row["t1"] and px >= row["t1"] and pos.remaining == 1:
                # Retain the first executable limit quote; the closed-slot core still arbitrates stop-first.
                start = ts - ts % 300_000
                self._v1r_limits.setdefault((tid, start), (px, ts))

    def v1r_clock(self, ts):
        at = datetime.fromtimestamp(ts / 1000, IST)
        if ts < v1_hours.last_bar_ms(at.date()):
            return 0
        n = 0
        for tid, pos in list(self._v1r_pos.items()):
            row = self._open.get(tid)
            if row is None or tid in self._v1r_wait:
                continue
            kind = "V1R_EOD" if pos.exit_pick.form == "intraday" else "V1R_SETTLED"
            if kind == "V1R_SETTLED" and at.date() < pos.expiry:
                continue
            px = self._ltp_on(row["security_id"], at.date())
            if px is None:
                continue
            new, events = v1_position.close_now(pos, px, ts, kind)
            self._v1r_apply(tid, new, events)
            n += 1
        return n

    def v1r_rewalk(self, tid, option, stock, h1=(), daily=()):
        """Restore from the entry snapshot, never from the persisted trail cursor."""
        row = self._open[tid]
        rec = self.conn.execute("SELECT payload_json FROM signals WHERE id=?", (row["entry_signal_id"],)).fetchone()
        block = json.loads(rec[0])["snapshot"]["v1r"]
        stock_marks = {b[0] for b in stock}
        option_marks = {b[0] for b in option}
        if block["break_ts"] not in stock_marks & option_marks or any(
                t not in stock_marks for t in option_marks
                if block["break_ts"] <= t < v1_hours.cash_close_ms(ist_date(t))):
            raise ValueError("restart history does not cover the entry and every option slot")
        self._v1r_pos[tid] = self._v1r_initial(row, block)
        self._v1r_replay_cursor[tid] = row.get("sw_bar_ms") or -1
        stocks = {b[0]: b for b in stock}
        daily = sorted(daily)
        for b in sorted(option):
            if tid not in self._open:
                break
            if b[0] <= block["break_ts"]:
                continue
            stock = stocks.get(b[0]) if b[0] < v1_hours.cash_close_ms(ist_date(b[0])) else None
            pos = self._v1r_pos[tid]
            for stamp, close in daily:
                if (pos.last_ts or pos.entry_ts) < stamp <= b[0]:
                    pos, _ = v1_position.on_daily_close(pos, close, stamp)
            self._v1r_pos[tid] = pos
            at = datetime.fromtimestamp(b[0] / 1000, IST)
            if b[0] >= v1_hours.last_bar_ms(at.date()) and (pos.exit_pick.form == "intraday" or at.date() >= pos.expiry):
                kind = "V1R_EOD" if pos.exit_pick.form == "intraday" else "V1R_SETTLED"
                pos, events = v1_position.close_now(pos, b[1], b[0], kind)
                self._v1r_apply(tid, pos, events, late=b[0] > self._v1r_replay_cursor[tid])
                break
            hours = tuple(h for h in h1 if b[0] < h[5] <= b[0] + 300_000) if stock is not None else ()
            self.on_v1r_slot(row["underlying_security_id"], stock, b, sid=row["security_id"],
                             h1_closes=hours, replay=True)
        if tid in self._v1r_pos:
            pos = self._v1r_pos[tid]
            for stamp, close in daily:
                if stamp > (pos.last_ts or pos.entry_ts):
                    pos, _ = v1_position.on_daily_close(pos, close, stamp)
            self._v1r_pos[tid] = pos
        self._v1r_wait.discard(tid)
        self._v1r_replay_cursor.pop(tid, None)

    def _open_sw_trade(
        self, signal_id: int, signal: Signal, meta: ContractMeta, ctx: SignalContext | None, message_id: int | None,
    ) -> int | None:  # fmt: skip
        """A Still Watching trade (T-419, STRATEGY-SPEC 4.8): scanner SW, hold carry, t1 = t2 = 0.0, state SW_OPEN. The
        entry is the group's entry kind (edition.client_entry_kind: the bar-close entry, as each edition stores it) at
        tier ENTRY (gates.tier with still_watching=True: no carry cap) that R4 did not demote. Levels = the signal's
        snapshot["sw"] (sw_entry_levels, picked once by the app at the entry; the card prints the same block): the 1h
        stop zone (stop_top / stop_bottom / stop_confirmed_ms, fixed for the trade's life), target_level, und_entry. No
        zone -> not tracked (logged; never a guess). The exit walk is zone_exit.on_bar / on_clock (on_sw_stock_bar,
        sw_clock)."""
        if signal.kind != edition.client_entry_kind():
            log.info("trade tracker: carried %s signal %s is not the Still Watching entry kind; not tracked",
                     signal.kind, signal_id)  # fmt: skip
            return None
        from gammad.v1.carry_break import break_entered

        tier = TIER_ENTRY if break_entered(signal.snapshot) else gates_mod.tier(ctx, self.gates_cfg, still_watching=True)
        demoted = ctx is not None and DUP_STOCK in ctx.gate_reasons
        if tier != TIER_ENTRY or demoted:
            log.info("trade tracker: Still Watching signal %s is %s%s; only an ENTRY opens a trade", signal_id, tier,
                     " (dup_stock)" if demoted else "")  # fmt: skip
            return None
        lv = sw_levels_of(signal.snapshot)
        if lv is None:
            log.warning("trade tracker: Still Watching signal %s has no 1h stop zone at entry; not tracked (no zone ="
                        " no entry)", signal_id)  # fmt: skip
            return None
        try:
            expiry = date.fromisoformat(str(meta.expiry_date))
        except ValueError:
            log.error("trade tracker: Still Watching signal %s has no readable expiry %r; not tracked", signal_id,
                      meta.expiry_date)  # fmt: skip
            return None
        try:
            eve_ms = self.sw_rule.eve_ms(self.calendar.prev_trading_day(expiry))
            pos = zone_exit.SwPosition(option_type=str(meta.option_type), entry=float(signal.ltp),
                                       near_edge=lv.near_edge, far_edge=lv.far_edge, target=lv.target,
                                       expiry_eve_ms=eve_ms)  # fmt: skip
        except (LookupError, ValueError, TypeError) as exc:
            log.error("trade tracker: Still Watching signal %s refused (%s); not tracked", signal_id, exc)
            return None
        entry_date = ist_date(signal.ts_ms)
        tick = getattr(meta, "tick_size", None)
        tick = float(tick) if tick else None
        lot = meta.lot_size if meta.lot_size is not None else (ctx.lot_size if ctx is not None else None)
        row: dict[str, Any] = {
            "entry_signal_id": int(signal_id), "kind": signal.kind, "hold": rules.SW_HOLD,
            "entry_date": entry_date.isoformat(), "entry_ts_ms": int(signal.ts_ms), "security_id": int(signal.security_id),
            "underlying_security_id": meta.underlying_security_id, "list_name": meta.list_name,
            "expiry_date": meta.expiry_date, "option_type": meta.option_type, "lot_size": lot, "tick_size": tick,
            "entry": pos.entry, "sl": None, "t1": 0.0, "t2": 0.0, "zone_edge": None, "armed": 0, "armed_date": None,
            "last_close_date": None, "last_close_px": None, "state": rules.SW_STATE_OF[pos.state], "exit_px": None,
            "exit_ts_ms": None, "entry_msg_id": message_id, "spec_version": self.spec_version,
            "updated_ms": self._wall_ms(), "gate_status": ctx.gate_status if ctx is not None else None,
            "scanner": rules.SCANNER_SW, "stop_top": lv.zone_top, "stop_bottom": lv.zone_bottom,
            "stop_confirmed_ms": lv.zone_confirmed_ms, "und_entry": lv.und_px, "target_level": lv.target,
            "target_ms": None, "trail_level": None, "sw_bar_ms": None,
        }  # fmt: skip
        cur = self.conn.execute(
            f"INSERT OR IGNORE INTO trades ({', '.join(SW_TRADE_COLUMNS)}) VALUES ({', '.join('?' for _ in SW_TRADE_COLUMNS)})",
            tuple(row[c] for c in SW_TRADE_COLUMNS),
        )
        self.conn.commit()
        if not cur.rowcount:
            log.warning("trade tracker: Still Watching signal %s already has a trade; ignored", signal_id)
            return None
        tid = int(cur.lastrowid or 0)
        row["id"] = tid
        self._contract[tid] = (meta.symbol, meta.strike)
        self._sw_pos[tid] = pos
        self._cache(row)
        self._push(row)
        log.info("trade %s opened: Still Watching %s %s entry %.2f stop zone %g-%g (near %g) target %s (signal %s,"
                 " msg %s)", tid, meta.symbol, meta.option_type, pos.entry, lv.zone_bottom, lv.zone_top, lv.near_edge,
                 "none" if lv.target is None else f"{lv.target:g}", signal_id, message_id)  # fmt: skip
        if self.on_sw_open is not None and meta.underlying_security_id is not None:
            try:
                self.on_sw_open(int(meta.underlying_security_id))
            except Exception:  # the app's bar seeding must never cost the trade
                log.exception("trade tracker: on_sw_open failed for trade %s", tid)
        return tid

    def _open_zone_trade(
        self, signal_id: int, signal: Signal, meta: ContractMeta, ctx: SignalContext | None, message_id: int | None,
        block: Mapping[str, Any],
    ) -> int | None:  # fmt: skip
        """A V2T trade (rules.py zone family): hold swing, t1 = t2 = 0.0, state Z_OPEN, the 1h stop zone from
        snapshot.v2t.stop (None = NO_ZONE, M7), und_entry = snapshot.v2t.und_px (else ctx.spot)."""
        if signal.kind != "E1":
            log.error("trade tracker: V2T signal %s is %s, not E1; not tracked", signal_id, signal.kind)
            return None
        tier_fn = getattr(gates_mod, "v2t_tier", None)  # PLAN-T267 §1 "Tier rule" (B4a); the router decides without it
        if callable(tier_fn):
            try:  # the side from the contract itself, never from whatever the router wrote into the snapshot
                tier = tier_fn(signal.snapshot, meta.option_type)
            except Exception:  # a tier bug refuses the trade, never breaks routing
                log.exception("trade tracker: gates.v2t_tier failed for V2T signal %s; not tracked", signal_id)
                return None
            if tier != TIER_ENTRY:
                log.info("trade tracker: V2T signal %s is %s; only an ENTRY opens a trade", signal_id, tier)
                return None
        entry_date = ist_date(signal.ts_ms)
        try:
            expiry = date.fromisoformat(str(meta.expiry_date))
        except ValueError:
            log.error("trade tracker: V2T signal %s has no readable expiry %r; not tracked", signal_id, meta.expiry_date)
            return None
        if expiry < entry_date:
            log.error("trade tracker: V2T signal %s is on a contract that expired %s; not tracked", signal_id, expiry)
            return None
        if "stop" not in block:
            log.error("trade tracker: V2T signal %s carries no snapshot.v2t.stop; tracked as NO_ZONE (no stop)", signal_id)
        stop = _stop_from(block.get("stop"))
        try:
            pos = rules.open_zone_position(option_type=meta.option_type, entry=signal.ltp, entry_ts_ms=signal.ts_ms,
                                           stop=stop)  # fmt: skip
        except ValueError as exc:
            log.error("trade tracker: V2T signal %s refused (%s); not tracked", signal_id, exc)
            return None
        tick = getattr(meta, "tick_size", None)
        tick = float(tick) if tick else None
        lot = meta.lot_size if meta.lot_size is not None else (ctx.lot_size if ctx is not None else None)
        und_px = block.get("und_px")
        if und_px is None and ctx is not None:
            und_px = ctx.spot
        row: dict[str, Any] = {
            "entry_signal_id": int(signal_id), "kind": "E1", "hold": "swing", "entry_date": entry_date.isoformat(),
            "entry_ts_ms": int(signal.ts_ms), "security_id": int(signal.security_id),
            "underlying_security_id": meta.underlying_security_id, "list_name": meta.list_name,
            "expiry_date": meta.expiry_date, "option_type": meta.option_type, "lot_size": lot, "tick_size": tick,
            "entry": pos.entry, "sl": None, "t1": 0.0, "t2": 0.0, "zone_edge": None, "armed": 0, "armed_date": None,
            "last_close_date": None, "last_close_px": None, "state": pos.state, "exit_px": None, "exit_ts_ms": None,
            "entry_msg_id": message_id, "spec_version": self.spec_version, "updated_ms": self._wall_ms(),
            "gate_status": ctx.gate_status if ctx is not None else None, "scanner": rules.V2T,
            "stop_top": None if stop is None else stop.top, "stop_bottom": None if stop is None else stop.bottom,
            "stop_confirmed_ms": None if stop is None else stop.confirmed_ms,
            "und_entry": None if und_px is None else float(und_px),
        }  # fmt: skip
        cur = self.conn.execute(
            f"INSERT OR IGNORE INTO trades ({', '.join(ZONE_TRADE_COLUMNS)})"
            f" VALUES ({', '.join('?' for _ in ZONE_TRADE_COLUMNS)})",
            tuple(row[c] for c in ZONE_TRADE_COLUMNS),
        )
        self.conn.commit()
        if not cur.rowcount:
            log.warning("trade tracker: V2T signal %s already has a trade; ignored", signal_id)
            return None
        tid = int(cur.lastrowid or 0)
        row["id"] = tid
        self._contract[tid] = (meta.symbol, meta.strike)
        self._zone_pos[tid] = pos
        self._cache(row)
        self._push(row)
        log.info("trade %s opened: V2T %s %s entry %.2f stop %s (signal %s, msg %s)", tid, meta.symbol,
                 meta.option_type, pos.entry, "NO_ZONE" if stop is None else f"{stop.bottom:g}-{stop.top:g}",
                 signal_id, message_id)  # fmt: skip
        return tid

    # ------------------------------------------------------------------ start-up

    def load(self, now: datetime | None = None) -> dict[str, int]:
        """Re-read every open trade from the DB (§6). An INTRADAY trade from an earlier session still open ->
        CLOSED_EOD_MISSED (PERSONAL + dashboard note; delivered by resume()). An intraday trade from TODAY found
        after the 15:30 close is the same miss. A swing past its expiry -> CLOSED_EXPIRY, logged at INFO (T-128 (1))."""
        now = now or self._now()
        today = now.date()
        self._open.clear()
        self._by_sid.clear()
        self._zone_pos.clear()
        self._zone_by_sid.clear()
        self._zone_by_und.clear()
        self._unapplied.clear()
        self._sw_pos.clear()
        self._sw_by_sid.clear()
        self._sw_by_und.clear()
        self._v1r_pos.clear()
        self._v1r_wait.clear()
        marks = ", ".join("?" for _ in rules.OPEN_STATES)
        rows = self.conn.execute(
            f"SELECT * FROM trades WHERE state IN ({marks}) ORDER BY id", sorted(rules.OPEN_STATES)
        ).fetchall()
        missed = expired = 0
        ts = _ms(now)
        retired = 0
        for r in rows:
            row = dict(r)
            if row["state"] in rules.V1R_STATES:
                self._cache(row)
                self._v1r_wait.add(int(row["id"]))
                continue  # app fetches entry..now history before allowing a live walk
            if row["state"] in rules.SW_STATES:  # T-419: re-hydrate (state, touch, trail; the extreme: sw_seed_bars)
                pos = self._load_sw_position(row)
                if pos is None:
                    continue
                self._sw_pos[int(row["id"])] = pos
                self._cache(row)
                if ts >= pos.expiry_eve_ms:  # the eve exit passed while the process was down: SW_EVE, price unknown
                    log.info("trade %s: Still Watching trade still open past its expiry-eve exit; SW_EVE", row["id"])
                    self._transition(row, zone_exit.on_clock(pos, ts, option_ltp=None, rule=self.sw_rule),
                                     schedule=False)  # fmt: skip
                    expired += 1
                    continue
                if row.get("sw_bar_ms") is not None:  # the last stock bar it walked: the re-walk resumes after it
                    self._sw_last_bar[int(row["id"])] = int(row["sw_bar_ms"])
                if row.get("underlying_security_id") is not None:  # live bars wait for the restart re-walk
                    self.sw_hold_live_bars(int(row["underlying_security_id"]))
                continue
            if row["state"] in rules.ZONE_STATES and edition.is_client():
                # D297: the client edition no longer runs (or ships) the V2 Scanner; a 0.8.4 store's open V2 trade is
                # retired, never restored (restoring it would need gammad.v2scan): no Telegram, no push
                self._retire_zone_trade(row, ts)
                retired += 1
                continue
            if row["state"] in rules.ZONE_STATES:  # T-267: restore the stop, the steps and the entry's initial zone
                self._zone_pos[int(row["id"])] = self._load_zone_position(row)
                self._cache(row)
                if today > date.fromisoformat(str(row["expiry_date"])):
                    log.info("trade %s: V2T contract expired %s while still open; CLOSED_EXPIRY", row["id"],
                              row["expiry_date"])  # fmt: skip
                    self._transition(row, rules.on_zone_expiry(self._zone_pos[int(row["id"])], ts_ms=ts),
                                     schedule=False)  # fmt: skip
                    expired += 1
                continue
            self._cache(row)
            entry_d = date.fromisoformat(str(row["entry_date"]))
            if row["hold"] == "intraday" and (
                entry_d < today or (entry_d == today and now.time() >= session_clock.fo_window(today)[1])
            ):
                self._transition(row, rules.on_eod_missed(self._position(row), ts_ms=ts), schedule=False)
                missed += 1
            elif row["hold"] == "swing" and today > date.fromisoformat(str(row["expiry_date"])):
                log.info("trade %s: swing contract expired %s while still open; CLOSED_EXPIRY", row["id"],
                          row["expiry_date"])  # fmt: skip
                self._transition(row, rules.on_expiry(self._position(row), ts_ms=ts), schedule=False)
                expired += 1
        self.loaded = True
        n_open = len(self._open)
        if retired:
            log.warning("trade tracker: %d open V2 Scanner trade(s) retired (%s): the client edition no longer runs it",
                        retired, ZONE_RETIRED_REASON)  # fmt: skip
        log.info("trade tracker loaded: %d open, %d EOD_MISSED, %d expired", n_open, missed, expired)
        return {"open": n_open, "eod_missed": missed, "expired": expired}

    def _load_sw_position(self, row: Mapping[str, Any]) -> zone_exit.SwPosition | None:
        """An open SW row as its SwPosition: the stop zone (stop_top / stop_bottom -> near / far), target_level, state
        (SW_OPEN / SW_TRAILING), touched_ms = target_ms, trail_level (the chandelier in force; tightening only, so it is
        kept as stored), the eve deadline from expiry_date. extreme = None until sw_seed_bars recomputes it from the
        stock bars since the touch (a None extreme takes the next bar's, and the stored trail never loosens)."""
        tid = int(row["id"])
        try:
            ot = str(row["option_type"])
            top, bottom = float(row["stop_top"]), float(row["stop_bottom"])
            near, far = (top, bottom) if ot == "CE" else (bottom, top)
            eve_ms = self.sw_rule.eve_ms(self.calendar.prev_trading_day(date.fromisoformat(str(row["expiry_date"]))))
            return zone_exit.SwPosition(
                option_type=ot, entry=float(row["entry"]), near_edge=near, far_edge=far,
                target=None if row.get("target_level") is None else float(row["target_level"]), expiry_eve_ms=eve_ms,
                state=rules.SW_POS_STATE_OF[str(row["state"])],  # type: ignore[arg-type]
                touched_ms=None if row.get("target_ms") is None else int(row["target_ms"]),
                trail_level=None if row.get("trail_level") is None else float(row["trail_level"]),
            )  # fmt: skip
        except (KeyError, LookupError, TypeError, ValueError) as exc:
            log.error("trade %s: Still Watching row unreadable (%s); left as stored, not followed", tid, exc)
            return None

    def _retire_zone_trade(self, row: dict[str, Any], ts: int) -> None:
        """D297 (client): close an open V2T trade as ZONE_RETIRED_STATE, exit price unknown, with ONE trade_events row
        (kind ZONE_RETIRED_EVENT, status 'suppressed', error = the reason): recorded, never delivered (only 'pending'
        rows are sent or re-sent). Touches no rules / gammad.v2scan code."""
        tid = int(row["id"])
        try:
            self.conn.execute("UPDATE trades SET state = ?, exit_px = NULL, exit_ts_ms = ?, updated_ms = ? WHERE id = ?",
                              (ZONE_RETIRED_STATE, ts, self._wall_ms(), tid))  # fmt: skip
            self.conn.execute(
                "INSERT OR IGNORE INTO trade_events (trade_id, kind, ts_ms, price, status, error)"
                " VALUES (?, ?, ?, NULL, 'suppressed', ?)", (tid, ZONE_RETIRED_EVENT, ts, ZONE_RETIRED_REASON),
            )  # fmt: skip
            self.conn.commit()
        except BaseException:
            self.conn.rollback()
            raise

    def _load_zone_position(self, row: Mapping[str, Any]) -> rules.ZonePosition:
        """A Z_OPEN row as a ZonePosition: the steps from trade_zone_steps, the initial zone = the stored stop when
        there is no step, else the entry signal's snapshot.v2t.stop (signals.payload_json); NO_ZONE = no stop at all."""
        tid = int(row["id"])
        step_rows = list_zone_steps(self.conn, tid)
        steps = rules.steps_of(step_rows)
        stored = rules.stop_zone_of(row.get("stop_top"), row.get("stop_bottom"), row.get("stop_confirmed_ms"))
        initial = stored if not steps else self._initial_stop_from_signal(row)
        if steps and initial is None:
            log.warning("trade %s: the entry's 1h stop zone is not in its signal payload; a stock close at or before"
                        " %s reads no stop (restart window)", tid, steps[0].confirmed_ms)  # fmt: skip
        self._unapplied[tid] = [(int(s["seq"]), int(s["confirmed_ms"])) for s in step_rows if s.get("applied_ms") is None]
        return rules.ZonePosition(
            option_type=str(row["option_type"]), entry=float(row["entry"]), entry_ts_ms=int(row["entry_ts_ms"]),
            initial=initial, steps=steps, no_zone=stored is None and not steps, state=row["state"],
        )  # fmt: skip

    def _initial_stop_from_signal(self, row: Mapping[str, Any]) -> rules.StopZone | None:
        got = self.conn.execute("SELECT payload_json FROM signals WHERE id = ?", (row.get("entry_signal_id"),)).fetchone()
        if got is None or not got[0]:
            return None
        try:
            payload = json.loads(got[0])
        except (TypeError, ValueError):
            return None
        snap = payload.get("snapshot") if isinstance(payload, dict) else None
        block = snap.get("v2t") if isinstance(snap, dict) else None
        return _stop_from(block.get("stop")) if isinstance(block, dict) else None

    async def resume(self, now_ms: int | None = None) -> dict[str, int]:
        """After load(): deliver what load() wrote, then re-send ONCE every pending row older than 60 s left by a
        previous process (§6, at-least-once; the row leaves 'pending' after the attempt). A V2T trail step left
        'pending' is offered again the same way (counted in `resent`)."""
        now_ms = self._wall_ms() if now_ms is None else now_ms
        own = await self.drain()
        resent = await self.resend_pending(now_ms)
        resent += await self._resend_pending_steps()
        return {"delivered": own, "resent": resent}

    async def resend_pending(self, now_ms: int | None = None, min_age_ms: int = RESEND_MIN_AGE_MS) -> int:
        now_ms = self._wall_ms() if now_ms is None else now_ms
        rows = self.conn.execute(
            "SELECT id, trade_id FROM trade_events WHERE status = ? AND ts_ms <= ? ORDER BY id",
            (ST_PENDING, now_ms - min_age_ms),
        ).fetchall()
        stale = [(int(r[0]), int(r[1])) for r in rows if int(r[0]) not in self._own]
        by_trade: dict[int, list[int]] = {}
        for eid, tid in stale:
            by_trade.setdefault(tid, []).append(eid)
        for tid, ids in by_trade.items():
            log.warning("trade %s: re-sending %d pending follow-up(s) from before the restart: %s", tid, len(ids), ids)
            await self._deliver(tid, ids)
        return len(stale)

    # ------------------------------------------------------------------ inputs

    def on_tick(self, sid: int, ltp: float | None, ts_ms: int) -> int:
        """A contract tick (Runtime.handle_tick, when sid in open_ids). Returns the events fired."""
        if ltp is None or not ltp > 0:
            return 0
        self._last_ltp[sid] = (float(ltp), ist_date(ts_ms))
        fired = 0
        for tid in list(self._by_sid.get(sid, ())):
            row = self._open.get(tid)
            if row is None or ts_ms < int(row["entry_ts_ms"]):
                continue
            pos = self._position(row)
            step = rules.on_tick(pos, float(ltp), ts_ms)
            if step.position != pos or step.events:
                fired += len(self._transition(row, step))
        return fired

    # ------------------------------------------------------------------ inputs: V2T zone family (T-267)

    def on_zone_tick(self, sid: int, ltp: float | None, ts_ms: int) -> None:
        """An option tick of an open V2T contract (Runtime.handle_tick, when sid in zone_ids): records its last LTP
        only -- the DAY5 / EXPIRY exit price and on_stock_bar's default. No rule runs on a premium tick (M11)."""
        if ltp is None or not ltp > 0:
            return
        self._last_ltp[sid] = (float(ltp), ist_date(ts_ms))

    def _opt_price(self, sid: int, day: date, opt_ltp: Any) -> float | None:
        if opt_ltp is None:
            return self._ltp_on(sid, day)
        if isinstance(opt_ltp, Mapping):
            got = opt_ltp.get(sid)
        elif callable(opt_ltp):
            got = opt_ltp(sid)
        else:
            got = opt_ltp
        if got is None or not float(got) > 0:
            return self._ltp_on(sid, day)
        return float(got)

    def on_stock_bar(self, und_sid: int, bar: Any, opt_ltp: Any = None) -> list[dict[str, Any]]:
        """One CLOSED 5-min bar of an underlying (a Bar, ts_ms = its OPEN; or [ts, o, h, l, c, ...]): every open V2T
        trade on it meets rules.on_stock_close -- a close beyond the far edge of the stop in force exits ZSL / ZTRAIL
        at the option LTP of that close. `opt_ltp` = {option sid: LTP}, a callable sid -> LTP, one number (every trade
        on this underlying), or None = the last LTP on_zone_tick recorded today. Returns one dict per exit."""
        got = _bar_open_close(bar)
        tids = sorted(self._zone_by_und.get(int(und_sid), ()))
        if got is None or not tids:
            return []
        open_ms, close = got
        close_ms = open_ms + rules.STOCK_TF_MS
        day = ist_date(open_ms)
        out: list[dict[str, Any]] = []
        for tid in tids:
            row, pos = self._open.get(tid), self._zone_pos.get(tid)
            if row is None or pos is None or not pos.is_open:
                continue
            counts = close_ms > pos.entry_ts_ms and rules.stock_slot_counts(open_ms)
            if counts:
                self._mark_applied(tid, close_ms)
            sid = int(row["security_id"])
            step = rules.on_stock_close(pos, bar_open_ms=open_ms, close=close, opt_ltp=self._opt_price(sid, day, opt_ltp))
            if not step.events:
                continue
            assert isinstance(step.position, rules.ZonePosition)
            self._zone_pos[tid] = step.position
            ids = self._transition(row, step)
            for ev in step.events:
                out.append({"trade_id": tid, "kind": ev.kind, "price": ev.price, "und_close": ev.und_close,
                            "level": ev.level, "ts_ms": ev.ts_ms, "event_ids": ids})  # fmt: skip
        return out

    def _mark_applied(self, tid: int, close_ms: int) -> None:
        """trade_zone_steps.applied_ms = the first counted stock close after a step's T (bookkeeping only)."""
        pending = self._unapplied.get(tid)
        if not pending:
            return
        due = [(seq, t) for seq, t in pending if t < close_ms]
        if not due:
            return
        for seq, _t in due:
            update_zone_step(self.conn, tid, seq, applied_ms=close_ms, commit=False)
        self.conn.commit()
        self._unapplied[tid] = [p for p in pending if p not in due]

    def on_hour_close(self, und_sid: int, events: Iterable[Any], now_ms: int | None = None) -> list[dict[str, Any]]:
        """The 1h close of an underlying: `events` = H1ZoneBook.append(row)'s ZoneEvents (both sides; zone_side picks
        the trade side). A trail step (rules.on_zone_event) is written to trade_zone_steps ('pending') in ONE
        transaction with trades.stop_*, takes effect from the next stock close after its T, and is offered to the
        router's send_zone_step (owner PERSONAL + dashboard, M4). Returns one dict per step (kind ZSTEP)."""
        tids = sorted(self._zone_by_und.get(int(und_sid), ()))
        evs = sorted(events or (), key=lambda e: int(e.confirmed_ms))
        if not tids or not evs:
            return []
        out: list[dict[str, Any]] = []
        for tid in tids:
            for ev in evs:
                row, pos = self._open.get(tid), self._zone_pos.get(tid)
                if row is None or pos is None:
                    break
                new_pos, new = rules.on_zone_event(pos, ev)
                if new is None:
                    continue
                seq = self._write_step(row, new, float(ev.h1_close) if ev.h1_close is not None else None)
                self._zone_pos[tid] = new_pos
                self._unapplied.setdefault(tid, []).append((seq, int(new.confirmed_ms)))
                self._own_steps.add((tid, seq))
                step = {"kind": rules.ZSTEP, "trade_id": tid, "seq": seq, "confirmed_ms": new.confirmed_ms,
                        "top": new.top, "bottom": new.bottom, "h1_close": ev.h1_close}  # fmt: skip
                out.append(step)
                log.info("trade %s: 1h stop zone trailed to %g-%g (step %d, confirmed %s)", tid, new.bottom, new.top,
                         seq, new.confirmed_ms)  # fmt: skip
                self._push(row)
                self._schedule_step(tid, seq)
        return out

    def _write_step(self, row: dict[str, Any], new: rules.StopZone, h1_close: float | None) -> int:
        """§6 for a trail step: the trade_zone_steps row ('pending') and trades.stop_* commit in ONE transaction.
        reply_to stays NULL here (nothing replied to yet; T-275(2)): _deliver_step writes the id the router actually
        replied to, never the trade's GROUP entry_msg_id."""
        tid = int(row["id"])
        upd = self._wall_ms()
        try:
            seq = insert_zone_step(self.conn, tid, confirmed_ms=int(new.confirmed_ms), top=float(new.top),
                                   bottom=float(new.bottom), h1_close=h1_close, status=ST_PENDING,
                                   reply_to=None, commit=False)  # fmt: skip
            self.conn.execute(
                "UPDATE trades SET stop_top = ?, stop_bottom = ?, stop_confirmed_ms = ?, updated_ms = ? WHERE id = ?",
                (float(new.top), float(new.bottom), int(new.confirmed_ms), upd, tid),
            )
            self.conn.commit()
        except BaseException:
            self.conn.rollback()
            raise
        row.update(stop_top=float(new.top), stop_bottom=float(new.bottom), stop_confirmed_ms=int(new.confirmed_ms),
                   updated_ms=upd)  # fmt: skip
        return seq

    def _schedule_step(self, trade_id: int, seq: int) -> None:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        task = loop.create_task(self._deliver_step(trade_id, seq), name=f"trade-{trade_id}-zstep-{seq}")
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def _deliver_step(self, trade_id: int, seq: int) -> None:
        """Offer one pending trail step to router.send_zone_step(trade, step) (M4: owner PERSONAL + dashboard only;
        the router decides). A router without that method: 'suppressed', nothing sent (never a GROUP follow-up)."""
        lock = self._locks.setdefault(trade_id, asyncio.Lock())
        async with lock:
            steps = {int(s["seq"]): s for s in list_zone_steps(self.conn, trade_id)}
            st = steps.get(int(seq))
            trow = self.conn.execute("SELECT * FROM trades WHERE id = ?", (trade_id,)).fetchone()
            if st is None or st["status"] != ST_PENDING or trow is None:
                self._own_steps.discard((trade_id, seq))
                return
            trade = self._trade_payload(dict(trow))
            far = st["bottom"] if trade.get("option_type") == "CE" else st["top"]
            step = {**st, "kind": rules.ZSTEP, "ts_ms": st["confirmed_ms"], "level": far, "reason": ZSTEP_REASON}
            send = getattr(self.router, "send_zone_step", None) if self.router is not None else None
            replied: int | None = None  # T-275(2): the id the router replied to (its 4th value; a 3-tuple -> None)
            if send is None:
                status, msg_id, error = "suppressed", None, "router has no send_zone_step (ZSTEP: owner PERSONAL only)"
            else:
                try:
                    res = tuple(await send(trade, step))
                    status, msg_id, error = res[:3]
                    replied = int(res[3]) if len(res) > 3 and res[3] is not None else None
                except Exception as exc:  # noqa: BLE001 -- a router bug is a 'failed' row, never a stuck 'pending' one
                    status, msg_id, error = "failed", None, f"{type(exc).__name__}: {exc}"
                if status not in FOLLOWUP_STATUSES:
                    status, msg_id, error = "failed", None, f"router returned unknown status {status!r}"
            sent = status == "sent"
            update_zone_step(self.conn, trade_id, seq, status=status,
                             tg_message_id=int(msg_id) if msg_id is not None and sent else None, error=error,
                             reply_to=replied if sent else None)  # fmt: skip
            self._own_steps.discard((trade_id, seq))
            if status == "failed":
                log.error("trade %s: ZSTEP %d follow-up failed: %s", trade_id, seq, error)
            self._push(dict(trow))

    async def _resend_pending_steps(self) -> int:
        rows = self.conn.execute(
            "SELECT trade_id, seq FROM trade_zone_steps WHERE status = ? ORDER BY trade_id, seq", (ST_PENDING,)
        ).fetchall()
        stale = [(int(r[0]), int(r[1])) for r in rows if (int(r[0]), int(r[1])) not in self._own_steps]
        for tid, seq in stale:
            await self._deliver_step(tid, seq)
        return len(stale)

    # ------------------------------------------------------------------ inputs: Still Watching (T-419)

    def _sw_atr_of(self, und: int) -> Any:
        atr = self._sw_atr.get(int(und))
        if atr is None:
            atr = self._sw_atr[int(und)] = self.sw_rule.atr()
        return atr

    def on_sw_stock_bar(self, und_sid: int, bar: Any, opt_ltp: Any = None) -> list[dict[str, Any]]:
        """One COMPLETED 5-min stock bar of an underlying (a Candle / Bar with ts_ms = its OPEN, or [ts, o, h, l, c, ..]).
        The bar first joins that stock's Atr15 (completed 15-min buckets only count, value_at_open), then every open
        Still Watching trade on it whose entry closed at / before the bar's open walks it ONCE (zone_exit.on_bar: SW_STOP
        on a close at / beyond the 1h zone's NEAR edge, SW_TARGET on the first target touch -- not an exit --, SW_TRAIL on
        a close at / beyond the chandelier, SW_EVE past the eve deadline). option close = `opt_ltp` ({sid: px}, a callable,
        one number) or the last LTP recorded today. Events and every state / touch / trail change go through _transition
        (write-ahead). Returns one dict per event."""
        try:
            f = zone_exit_fields(bar)
        except (TypeError, ValueError, IndexError, AttributeError):
            return []
        und = int(und_sid)
        self._sw_atr_of(und).add(f)
        held = self._sw_hold.get(und)
        if held is not None:  # a restart's re-walk of this stock is pending: the live bar waits for it (in order)
            held.append(f)
            return []
        return self._sw_walk(und, f, opt_ltp, late=False)

    def _sw_walk(self, und: int, f: tuple[int, float, float, float, float], opt_ltp: Any, *,
                 late: bool) -> list[dict[str, Any]]:  # fmt: skip
        """Walk ONE completed stock bar (already in the stock's Atr15) through every open SW trade on `und` that has not
        walked it (bars opening at / after the entry, each once, in time order: trades.sw_bar_ms = the last one).
        `late` = a bar that closed while the process was down (a restart's re-walk): its events are marked late."""
        ts, o, h, lo, c = f
        tids = sorted(self._sw_by_und.get(und, ()))
        if not tids:
            return []
        b5 = zone_exit.Bar5(ts, o, h, lo, c)
        atr = self._sw_atr_of(und).value_at_open(ts)
        day = ist_date(ts)
        out: list[dict[str, Any]] = []
        for tid in tids:
            row, pos = self._open.get(tid), self._sw_pos.get(tid)
            if row is None or pos is None or not pos.is_open:
                continue
            if ts < int(row["entry_ts_ms"]) or ts <= self._sw_last_bar.get(tid, -1):
                continue  # bars opening at / after the entry, each once (a re-sent bar is never walked twice)
            self._sw_last_bar[tid] = ts
            row["sw_bar_ms"] = ts
            # a late bar's exit price: the option's last LTP TODAY (the exit can only be taken now), never a guess
            opx = self._ltp_on(int(row["security_id"]), ist_date(self._wall_ms())) if late else \
                self._opt_price(int(row["security_id"]), day, opt_ltp)
            step = zone_exit.on_bar(pos, b5, option_close=opx, atr15=atr, rule=self.sw_rule)
            new = step.position
            assert isinstance(new, zone_exit.SwPosition)
            self._sw_pos[tid] = new
            stored = (pos.state, pos.touched_ms, pos.trail_level)
            if not step.events and (new.state, new.touched_ms, new.trail_level) == stored:
                self._sw_mark_bar(tid, ts)  # only the walk mark moves (the extreme lives in memory)
                continue
            ids = self._transition(row, step, late=late)
            for ev in step.events:
                out.append({"trade_id": tid, "kind": ev.kind, "price": ev.price, "und_close": ev.und_close,
                            "level": ev.level, "ts_ms": ev.ts_ms, "event_ids": ids, "late": late})  # fmt: skip
        return out

    def _sw_mark_bar(self, tid: int, ts: int) -> None:
        """trades.sw_bar_ms = the open of the last stock bar this trade walked (a restart re-walks every bar after it)."""
        try:
            self.conn.execute("UPDATE trades SET sw_bar_ms = ? WHERE id = ?", (int(ts), int(tid)))
            self.conn.commit()
        except sqlite3.Error:
            log.exception("trade %s: walk mark not stored; a restart re-walks from the previous one", tid)

    def sw_hold_live_bars(self, und_sid: int) -> None:
        """Hold the live stock bars of `und` until its re-walk (sw_seed_bars / sw_release) has run, so the bars that
        closed while the process was down are walked FIRST, in time order (load() holds every open SW trade's stock)."""
        self._sw_hold.setdefault(int(und_sid), [])

    def sw_release(self, und_sid: int) -> list[dict[str, Any]]:
        """No re-walk bars for `und` (no broker / the fetch failed): walk the held live bars in order and stop
        holding. Logged: the bars the process missed are then NOT re-walked."""
        held = self._sw_hold.pop(int(und_sid), None)
        if held is None:
            return []
        if self._sw_by_und.get(int(und_sid)):
            log.warning("Still Watching: stock %s has no bars for the restart re-walk; the bars closed while the process"
                        " was down are not walked", und_sid)  # fmt: skip
        out: list[dict[str, Any]] = []
        for f in sorted({x[0]: x for x in held}.values()):
            out += self._sw_walk(int(und_sid), f, None, late=False)
        return out

    def sw_seed_bars(self, und_sid: int, bars: Iterable[Any]) -> int:
        """Re-hydration / warm-up / RE-WALK (the app's COMPLETED 5-min stock bars from the previous session -- or the
        trade's touch / last walked bar, when earlier -- through now, at start and when a trade opens on a new stock):
        every bar joins the stock's Atr15; a TRAILING trade on it gets its extreme back = CE the highest HIGH / PE the
        lowest LOW of the bars closing at / after its touch (the touch bar included, zone_exit step 5) through the last
        bar it walked (trades.sw_bar_ms); then every bar AFTER that one -- the bars that closed while the process was
        down, then the live bars held meanwhile -- is walked through zone_exit.on_bar in time order, so a stop / target
        / trail of the downtime fires once now (its event marked late). Returns the bars read."""
        und = int(und_sid)
        atr = self._sw_atr_of(und)
        rows: list[tuple[int, float, float, float, float]] = []
        for b in bars or ():
            try:
                rows.append(zone_exit_fields(b))
            except (TypeError, ValueError, IndexError, AttributeError):
                continue
        for r in rows:
            atr.add(r)
        for tid in sorted(self._sw_by_und.get(und, ())):
            pos = self._sw_pos.get(tid)
            if pos is None or pos.state != "TRAILING" or pos.touched_ms is None:
                continue
            last = self._sw_last_bar.get(tid)
            ce = pos.option_type == "CE"
            ext = pos.extreme
            for ts, _o, h, lo, _c in rows:
                if ts + zone_exit.TF_MS < int(pos.touched_ms) or (last is not None and ts > last):
                    continue
                x = h if ce else lo
                ext = x if ext is None else (max(ext, x) if ce else min(ext, x))
            if ext != pos.extreme:
                self._sw_pos[tid] = replace(pos, extreme=ext)
        # the re-walk: fetched bars (late) then the live bars held meanwhile; a held live bar wins a shared slot
        held = self._sw_hold.pop(und, None) or []
        merged: dict[int, tuple[tuple[int, float, float, float, float], bool]] = {r[0]: (r, True) for r in rows}
        merged.update({f[0]: (f, False) for f in held})
        for ts in sorted(merged):
            f, late = merged[ts]
            self._sw_walk(und, f, None, late=late)
        return len(rows)

    def sw_clock(self, now_ms: int) -> int:
        """The expiry-eve exit (zone_exit.on_clock): at / after eve_exit_hm (15:15 IST) on the session before expiry an
        open Still Watching trade exits SW_EVE at the option's last LTP today. Cheap enough for the app's bar timer.
        Returns the trades closed."""
        n = 0
        day = ist_date(int(now_ms))
        for tid in sorted(self._sw_pos):
            row, pos = self._open.get(tid), self._sw_pos.get(tid)
            if row is None or pos is None or not pos.is_open or int(now_ms) < pos.expiry_eve_ms:
                continue
            if int(row.get("underlying_security_id") or -1) in self._sw_hold:
                continue  # its restart re-walk has not run: a downtime stop before the eve must win
            step = zone_exit.on_clock(pos, int(now_ms), option_ltp=self._ltp_on(int(row["security_id"]), day),
                                      rule=self.sw_rule)  # fmt: skip
            if step.events:
                self._transition(row, step)
                n += 1
        return n

    def _ltp_on(self, sid: int, day: date) -> float | None:
        """The last LTP this process saw for `sid` on `day` (IST); None when it did not tick that day."""
        got = self._last_ltp.get(sid)
        return got[0] if got is not None and got[1] == day else None

    def snapshot_closes(self, now: datetime | None = None) -> int:
        """15:35 (close_day, before the builders are cleared): the last LTP of each open SWING contract is that
        day's option close -> trades.last_close_px / last_close_date (the SWING_STOP exit price, §4). A contract
        that did not tick today gets no snapshot (run_daily then finds none for the day: a stock stop, no price)."""
        now = now or self._now()
        day = now.date().isoformat()
        n = 0
        for row in list(self._open.values()):
            ltp = self._ltp_on(int(row["security_id"]), now.date())
            if row["hold"] != "swing" or ltp is None or row["state"] in rules.ZONE_STATES:  # V1's SWING_STOP price
                continue
            row.update(last_close_px=ltp, last_close_date=day, updated_ms=self._wall_ms())
            self.conn.execute(
                "UPDATE trades SET last_close_px = ?, last_close_date = ?, updated_ms = ? WHERE id = ?",
                (ltp, day, row["updated_ms"], row["id"]),
            )
            n += 1
        self.conn.commit()
        if n:
            log.info("trade tracker: option close snapshot for %d swing trade(s) on %s", n, day)
        return n

    def close_intraday(self, now: datetime | None = None) -> int:
        """close_day (15:35, D98(1)): every INTRADAY trade still open -> CLOSED_EOD at the last LTP this process saw
        (None if none). The 15:20 job normally closed them; one survives when the process (re)started between 15:20
        and 15:30 (the cron never fires a past time and load() only calls it a miss from 15:30), and must not be
        carried into the next morning. An intraday trade from an EARLIER day (load() normally closed it) is the
        EOD_MISSED transition. Follow-ups are scheduled, not awaited (close_day must not wait on Telegram)."""
        now = now or self._now()
        today = now.date()
        ts = _ms(now)
        n = 0
        for row in [self._open[t] for t in sorted(self._open)]:
            if row["hold"] != "intraday" or row["state"] in rules.V1R_STATES:
                continue
            pos = self._position(row)
            ltp = self._ltp_on(int(row["security_id"]), today)
            if date.fromisoformat(str(row["entry_date"])) < today:
                step = rules.on_eod_missed(pos, ts_ms=ts, price=ltp)
            else:
                step = rules.on_session_end(pos, day_k=0, ltp=ltp, ts_ms=ts, reason=CLOSE_DAY_REASON)
            if step.events:
                self._transition(row, step)
                n += 1
        if n:
            log.warning("trade tracker: %d intraday trade(s) still open at close_day %s; closed", n, today)
        return n

    async def run_eod(self, now: datetime | None = None) -> dict[str, int]:
        """Job trades_eod (15:20 IST): same-day hold -> CLOSED_EOD, swing on trading day >= 5 -> CLOSED_DAY5, swing
        at or past expiry -> CLOSED_EXPIRY + its EXPIRY follow-up (§3; W15: routine, logged at INFO). Exit = the
        last LTP seen by this process TODAY (None if none)."""
        now = now or self._now()
        today = now.date()
        ts = _ms(now)
        out = {"eod": 0, "day5": 0, "expired": 0}
        for row in [self._open[t] for t in sorted(self._open)]:
            if row["state"] in rules.V1R_STATES:
                continue
            if row["state"] in rules.ZONE_STATES:  # T-267: EXPIRY on the expiry day, else DAY5 on zone day 5 (M15)
                zstep = self._zone_session_end(row, today, ts)
                if zstep is not None and zstep.events:
                    self._transition(row, zstep, schedule=False)
                    out["expired" if zstep.events[0].kind == "EXPIRY" else "day5"] += 1
                continue
            if row["state"] in rules.SW_STATES:  # T-419: no 15:20 / day-5 exit; the eve exit is sw_clock's (15:15)
                continue
            pos = self._position(row)
            ltp = self._ltp_on(int(row["security_id"]), today)
            if row["hold"] == "swing" and today >= date.fromisoformat(str(row["expiry_date"])):
                log.info("trade %s: swing still open on its expiry %s (D59 cap); CLOSED_EXPIRY", row["id"],
                         row["expiry_date"])  # fmt: skip
                self._transition(row, rules.on_expiry(pos, ts_ms=ts, price=ltp), schedule=False)
                out["expired"] += 1
                continue
            day_k = 0
            if row["hold"] == "swing":
                day_k = rules.hold_day(self.calendar, date.fromisoformat(str(row["entry_date"])), today)
            step = rules.on_session_end(pos, day_k=day_k, ltp=ltp, ts_ms=ts)
            if step.events:
                self._transition(row, step, schedule=False)
                out["eod" if row["hold"] == "intraday" else "day5"] += 1
        await self.drain()
        await self.resend_pending(ts)  # a young-at-start pending row is not stranded
        log.info("trades_eod %s: %s", today, out)
        return out

    def _zone_session_end(self, row: Mapping[str, Any], today: date, ts: int) -> rules.Step | None:
        tid = int(row["id"])
        pos = self._zone_pos.get(tid)
        if pos is None:
            return None
        day_k = rules.zone_hold_day(self.calendar, date.fromisoformat(str(row["entry_date"])), today)
        step = rules.on_zone_session_end(
            pos, day_k=day_k, expiry_day=today >= date.fromisoformat(str(row["expiry_date"])),
            ltp=self._ltp_on(int(row["security_id"]), today), ts_ms=ts,
        )  # fmt: skip
        if step.events:
            assert isinstance(step.position, rules.ZonePosition)
            self._zone_pos[tid] = step.position
        return step

    def daily_day(self, now: datetime) -> date:
        """The daily close run_daily() processes: today once the session has closed, else the previous trading day."""
        today = now.date()
        if now.time() >= session_clock.fo_window(today)[1] and self.calendar.is_trading_day(today):
            return today
        return self.calendar.prev_trading_day(today)

    async def run_daily(self, adapter: DailyCandles | None, now: datetime | None = None,
                        *, day: date | None = None) -> dict[str, Any]:  # fmt: skip
        """Job trades_daily (16:05 IST): D68 on each open SWING trade's underlying daily close (§3-4).

        `adapter.daily_candles(und, "NSE_EQ", day, day)` once per underlying (<= ~20 calls). The candle's IST date
        must equal `day`; otherwise that underlying's trades are DEFERRED to run_daily_retry (09:21 next trading
        day; Dhan publishes daily bars late, D77). Never runs 07:00-08:00 IST (D72). Re-processing a day's close
        is a no-op (D68 cannot arm twice or stop on the arming close), so a retry after a restart is safe.

        Returns (and logs at INFO) {day, processed, stops_set, stopped, deferred, skipped}; `stops_set` counts the
        SWING_ARMED events. Never an "armed" key: the client log filter hides any line holding that word (D98(6)),
        and this result line is a legitimate client log line (FIX2 F4)."""
        now = now or self._now()
        if NO_DAILY_FROM <= now.time() < NO_DAILY_UNTIL:
            log.warning("trades_daily: refused at %s (07:00-08:00 IST, D72)", now.time().isoformat("minutes"))
            return {"day": None, "processed": 0, "stops_set": 0, "stopped": 0, "deferred": [],
                    "skipped": "D72 window"}  # fmt: skip
        day = day or self.daily_day(now)
        out: dict[str, Any] = {"day": day.isoformat(), "processed": 0, "stops_set": 0, "stopped": 0, "deferred": [],
                               "skipped": None}  # fmt: skip
        by_und: dict[int, list[dict[str, Any]]] = {}
        for row in [self._open[t] for t in sorted(self._open)]:
            if row["state"] in rules.V1R_STATES:
                pos = self._v1r_pos.get(int(row["id"]))
                if pos is None or (pos.resolved.candle == "daily" and adapter is None):
                    out["deferred"].append(int(row["id"]))
                if pos is not None and pos.resolved.candle == "daily" and adapter is not None:
                    close = await self._daily_close(adapter, int(row["underlying_security_id"]), day,
                                                    same_day=day == now.date())
                    if close is None:
                        out["deferred"].append(int(row["id"]))
                    else:
                        pos, _ = v1_position.on_daily_close(pos, close, _ms(now))
                        self._v1r_pos[int(row["id"])] = pos
                        out["processed"] += 1
                continue
            if (row["hold"] != "swing" or row["state"] != "OPEN" or row.get("zone_edge") is None
                    or row.get("underlying_security_id") is None
                    or date.fromisoformat(str(row["entry_date"])) > day):  # fmt: skip
                continue
            by_und.setdefault(int(row["underlying_security_id"]), []).append(row)
        if not by_und:
            if not out["deferred"]:
                self._daily_done = day
            return out
        if adapter is None:
            out["deferred"] = sorted(int(r["id"]) for rows in by_und.values() for r in rows)
            out["skipped"] = "no broker adapter"
            log.error("trades_daily %s: no broker adapter; %d trade(s) deferred", day, len(out["deferred"]))
            return out
        ts = _ms(now)
        for und, rows in sorted(by_und.items()):
            close = await self._daily_close(adapter, und, day, same_day=day == now.date())
            if close is None:
                out["deferred"].extend(int(r["id"]) for r in rows)
                continue
            for row in rows:
                if row["id"] not in self._open:
                    continue
                opt_close = row.get("last_close_px") if row.get("last_close_date") == day.isoformat() else None
                if opt_close is None:
                    log.warning("trade %s: no option close snapshot for %s; a stock stop exits with no price",
                                row["id"], day)  # fmt: skip
                step = rules.on_daily_close(self._position(row), und_close=close, option_close=opt_close, ts_ms=ts)
                out["processed"] += 1
                if step.events:
                    kinds = [e.kind for e in step.events]
                    out["stops_set"] += kinds.count("SWING_ARMED")
                    out["stopped"] += kinds.count("SWING_STOP")
                    self._transition(row, step, schedule=False, armed_date=day.isoformat())
        await self.drain()
        if out["deferred"]:
            log.warning("trades_daily %s: candle not published for %d trade(s); retry at %s next trading day",
                        day, len(out["deferred"]), DAILY_RETRY_AT.isoformat("minutes"))  # fmt: skip
        else:
            self._daily_done = day
        log.info("trades_daily %s: %s", day, out)
        return out

    async def run_daily_retry(self, adapter: DailyCandles | None, now: datetime | None = None) -> dict[str, Any]:
        """Job trades_daily_retry (09:21 IST): the previous trading day's close when this process has not fully
        processed it (deferred at 16:05, or the process restarted since)."""
        now = now or self._now()
        day = self.calendar.prev_trading_day(now.date())
        if self._daily_done is not None and self._daily_done >= day:
            return {"day": day.isoformat(), "processed": 0, "stops_set": 0, "stopped": 0, "deferred": [],
                    "skipped": "already processed"}  # fmt: skip
        return await self.run_daily(adapter, now, day=day)

    async def _daily_close(self, adapter: DailyCandles, und: int, day: date, *, same_day: bool = False) -> float | None:
        """T-180: on the SAME day (the 16:05 run) Dhan's DH-907 means the candle is not published yet (D77; the 09:21
        retry gets it): INFO, the run's one WARNING summary covers it. A DH-907 on the retry, or any other failure,
        stays ERROR."""
        try:
            candles = await asyncio.to_thread(adapter.daily_candles, und, UNDERLYING_SEGMENT, day, day)
        except Exception as exc:  # noqa: BLE001 -- an auth / network error defers, never crashes the job
            if same_day and getattr(exc, "code", None) == CANDLE_NOT_PUBLISHED:
                log.info("trades_daily: daily candle for underlying %s on %s not published yet (%s); deferred",
                         und, day, CANDLE_NOT_PUBLISHED)  # fmt: skip
                return None
            log.error("trades_daily: daily candle for underlying %s on %s failed (%s: %s); deferred", und, day,
                      type(exc).__name__, exc)  # fmt: skip
            return None
        for c in candles or ():
            if ist_date(int(c.ts_ms)) == day:
                return float(c.close)
        got = sorted({ist_date(int(c.ts_ms)).isoformat() for c in candles or ()})
        log.warning("trades_daily: underlying %s has no %s candle yet (got %s); deferred", und, day, got or "none")
        return None

    # ------------------------------------------------------------------ transitions + write-ahead

    def _transition(
        self, row: dict[str, Any], step: rules.Step, *, schedule: bool = True, armed_date: str | None = None,
        late: bool = False,
    ) -> list[int]:
        """Write-ahead the step (state + pending events, ONE transaction), update the cache, push, and (schedule)
        start the delivery. Returns the new event ids. `late` (T-419, a Still Watching re-walk only): the events
        happened while the process was down."""
        if late and isinstance(step.position, zone_exit.SwPosition):
            ids = self._sw_write_ahead(row, step, late=True)
        else:
            ids = self._write_ahead(row, step, armed_date=armed_date)
        self._push(row)
        if ids and schedule:
            self._schedule(int(row["id"]), ids)
        return ids

    def _write_ahead(self, row: dict[str, Any], step: rules.Step, *, armed_date: str | None = None) -> list[int]:
        """§6: the transition and its trade_events rows (status 'pending') commit in ONE transaction, BEFORE any
        send. A second event of a kind is refused by UNIQUE (trade_id, kind): logged, never sent."""
        pos = step.position
        if isinstance(pos, zone_exit.SwPosition):  # T-419: the Still Watching columns (V1 / V2T below, unchanged)
            return self._sw_write_ahead(row, step)
        new_armed_date = row.get("armed_date")
        if pos.armed and not row["armed"]:  # SWING_ARMED: the daily close's date (else the event's IST date)
            new_armed_date = armed_date or (ist_date(step.events[0].ts_ms).isoformat() if step.events else None)
        row.update(
            state=pos.state,
            sl=pos.sl,
            armed=int(pos.armed),
            armed_date=new_armed_date,
            exit_px=pos.exit_px,
            exit_ts_ms=pos.exit_ts_ms,
            updated_ms=self._wall_ms(),
        )
        tid = int(row["id"])
        ids: list[int] = []
        try:
            self.conn.execute(
                "UPDATE trades SET state = ?, sl = ?, armed = ?, armed_date = ?, exit_px = ?, exit_ts_ms = ?,"
                " updated_ms = ? WHERE id = ?",
                (row["state"], row["sl"], row["armed"], row["armed_date"], row["exit_px"], row["exit_ts_ms"],
                 row["updated_ms"], tid),
            )  # fmt: skip
            for ev in step.events:
                try:
                    cur = self.conn.execute(
                        "INSERT INTO trade_events (trade_id, kind, ts_ms, price, und_close, status, reply_to, fill_px)"
                        " VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                        (tid, ev.kind, ev.ts_ms, ev.price, ev.und_close, ST_PENDING, row.get("entry_msg_id"), ev.fill_px),
                    )
                except sqlite3.IntegrityError as exc:
                    log.error("trade %s: a second %s event refused by UNIQUE (trade_id, kind): %s", tid, ev.kind, exc)
                    continue
                ids.append(int(cur.lastrowid or 0))
            self.conn.commit()
        except BaseException:
            self.conn.rollback()
            raise
        self._own.update(ids)
        if not pos.is_open:
            self._uncache(tid)
            log.info("trade %s %s at %s (%s)", tid, pos.state, pos.exit_px, ",".join(e.kind for e in step.events))
        elif step.events:
            log.info("trade %s -> %s (%s)", tid, pos.state, ",".join(e.kind for e in step.events))
        return ids

    def _sw_write_ahead(self, row: dict[str, Any], step: rules.Step, *, late: bool = False) -> list[int]:
        """§6 for a Still Watching step: trades.state (SW_OPEN / SW_TRAILING, or CLOSED_SW_<exit kind>), exit_px /
        exit_ts_ms, target_ms (= the touch), trail_level (the chandelier in force), sw_bar_ms (the walk mark) and the
        step's trade_events rows ('pending') commit in ONE transaction BEFORE any send; UNIQUE (trade_id, kind) as for
        every family. `late`: each event row carries SW_LATE_NOTE (trade_events.error until delivery keeps it; the
        delivered reason says it), the bar closed while the process was down."""
        pos = step.position
        assert isinstance(pos, zone_exit.SwPosition)
        if pos.is_open:
            state = rules.SW_STATE_OF[pos.state]
        else:
            exits = [e.kind for e in step.events if e.kind in rules.SW_EXIT_KINDS]
            state = rules.SW_CLOSED_STATE[exits[-1] if exits else zone_exit.SW_EVE]
        row.update(state=state, exit_px=pos.exit_px, exit_ts_ms=pos.exit_ts_ms, target_ms=pos.touched_ms,
                   trail_level=pos.trail_level, updated_ms=self._wall_ms())  # fmt: skip
        tid = int(row["id"])
        ids: list[int] = []
        try:
            self.conn.execute(
                "UPDATE trades SET state = ?, exit_px = ?, exit_ts_ms = ?, target_ms = ?, trail_level = ?, updated_ms = ?,"
                " sw_bar_ms = COALESCE(?, sw_bar_ms) WHERE id = ?",
                (row["state"], row["exit_px"], row["exit_ts_ms"], row["target_ms"], row["trail_level"],
                 row["updated_ms"], row.get("sw_bar_ms"), tid),
            )  # fmt: skip
            for ev in step.events:
                try:
                    cur = self.conn.execute(
                        "INSERT INTO trade_events (trade_id, kind, ts_ms, price, und_close, status, reply_to, fill_px,"
                        " error) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (tid, ev.kind, ev.ts_ms, ev.price, ev.und_close, ST_PENDING, row.get("entry_msg_id"), ev.fill_px,
                         SW_LATE_NOTE if late else None),
                    )  # fmt: skip
                except sqlite3.IntegrityError as exc:
                    log.error("trade %s: a second %s event refused by UNIQUE (trade_id, kind): %s", tid, ev.kind, exc)
                    continue
                ids.append(int(cur.lastrowid or 0))
            self.conn.commit()
        except BaseException:
            self.conn.rollback()
            raise
        self._own.update(ids)
        if not pos.is_open:
            self._uncache(tid)
            log.info("trade %s %s at %s (%s)", tid, state, pos.exit_px, ",".join(e.kind for e in step.events))
        else:
            self._sw_pos[tid] = pos
            if step.events:
                log.info("trade %s -> %s (%s)", tid, state, ",".join(e.kind for e in step.events))
        return ids

    # ------------------------------------------------------------------ delivery

    def _schedule(self, trade_id: int, ids: list[int]) -> None:
        """Start delivering from a sync caller (on_tick). With no running loop the rows stay pending until drain()."""
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        task = loop.create_task(self._deliver(trade_id, ids), name=f"trade-{trade_id}-followup")
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def drain(self) -> int:
        """Await in-flight deliveries, then deliver every event this process wrote that is still pending."""
        if self._tasks:
            await asyncio.gather(*list(self._tasks), return_exceptions=True)
        for tid, seq in sorted(self._own_steps):  # V2T trail steps written with no running loop (on_hour_close)
            await self._deliver_step(tid, seq)
        if not self._own:
            return 0
        marks = ", ".join("?" for _ in self._own)
        rows = self.conn.execute(
            f"SELECT id, trade_id FROM trade_events WHERE id IN ({marks}) AND status = ? ORDER BY id",
            (*sorted(self._own), ST_PENDING),
        ).fetchall()
        by_trade: dict[int, list[int]] = {}
        for r in rows:
            by_trade.setdefault(int(r[1]), []).append(int(r[0]))
        for tid, ids in by_trade.items():
            await self._deliver(tid, ids)
        return len(rows)

    async def _deliver(self, trade_id: int, ids: Iterable[int]) -> None:
        """Send one trade's pending events, in id order, one at a time (§7). Each row leaves 'pending' exactly
        once: the router's status (+ message id), or 'failed' when the router raised or is missing."""
        lock = self._locks.setdefault(trade_id, asyncio.Lock())
        async with lock:
            want = sorted(set(ids))
            if not want:
                return
            trow = self.conn.execute("SELECT * FROM trades WHERE id = ?", (trade_id,)).fetchone()
            if trow is None:
                return
            trade = self._trade_payload(dict(trow))
            marks = ", ".join("?" for _ in want)
            events = self.conn.execute(
                f"SELECT * FROM trade_events WHERE id IN ({marks}) AND status = ? ORDER BY id", (*want, ST_PENDING)
            ).fetchall()
            for er in events:
                ev = dict(er)
                ev["level"] = event_level(str(ev["kind"]), trade)
                ev["reason"] = event_reason(str(ev["kind"]), ev.get("ts_ms"), trade.get("scanner"))
                late = ev.get("error") == SW_LATE_NOTE  # T-419: a Still Watching re-walk event (only ever SW rows)
                if late:
                    ev["reason"] = f"{ev['reason']} ({SW_LATE_NOTE})"
                    ev["late"] = True
                status, msg_id, error = await self._send(trade, ev)
                if late and error is None:
                    error = SW_LATE_NOTE  # the row keeps the mark after delivery
                self.conn.execute(
                    "UPDATE trade_events SET status = ?, tg_message_id = ?, error = ? WHERE id = ? AND status = ?",
                    (status, msg_id, error, ev["id"], ST_PENDING),
                )
                self.conn.commit()
                self._own.discard(int(ev["id"]))
                if status == "failed":
                    log.error("trade %s: %s follow-up failed: %s", trade_id, ev["kind"], error)
                elif status not in QUIET_STATUSES:  # 0.8.2 T-214 F3: e.g. a legacy no_entry_msg is never silent again
                    log.warning("trade %s: %s follow-up not delivered (%s)%s", trade_id, ev["kind"], status,
                                f": {error}" if error else "")  # fmt: skip
            if events:
                self._push(dict(trow))

    async def _send(self, trade: Mapping[str, Any], ev: Mapping[str, Any]) -> tuple[str, int | None, str | None]:
        if self.router is None:
            return "failed", None, "no router"
        try:
            status, msg_id, error = await self.router.send_followup(trade, ev)
        except Exception as exc:  # noqa: BLE001 -- a router bug is a 'failed' row, never a stuck 'pending' one
            return "failed", None, f"{type(exc).__name__}: {exc}"
        if status not in FOLLOWUP_STATUSES:
            return "failed", None, f"router returned unknown status {status!r}"
        return status, (int(msg_id) if msg_id is not None and status == "sent" else None), error

    # ------------------------------------------------------------------ helpers

    def _cache(self, row: dict[str, Any]) -> None:
        tid, sid = int(row["id"]), int(row["security_id"])
        if row["state"] in rules.V1R_STATES:
            self._open[tid] = row
            return
        if row["state"] in rules.SW_STATES:  # T-419: a Still Watching contract is never in _by_sid (no on_tick rule)
            self._open[tid] = row
            for index, key in ((self._sw_by_sid, sid), (self._sw_by_und, row.get("underlying_security_id"))):
                if key is None:
                    continue
                ids = index.setdefault(int(key), [])
                if tid not in ids:
                    ids.append(tid)
            return
        if row["state"] in rules.ZONE_STATES:  # M11: a V2T contract is never in _by_sid (the on_tick path)
            self._open[tid] = row
            for index, key in ((self._zone_by_sid, sid), (self._zone_by_und, row.get("underlying_security_id"))):
                if key is None:
                    continue
                ids = index.setdefault(int(key), [])
                if tid not in ids:
                    ids.append(tid)
            return
        if row["state"] not in rules.LIVE_STATES:
            return
        self._open[tid] = row
        ids = self._by_sid.setdefault(sid, [])
        if tid not in ids:
            ids.append(tid)

    @staticmethod
    def _drop(index: dict[int, list[int]], key: Any, tid: int) -> None:
        if key is None:
            return
        ids = index.get(int(key), [])
        if tid in ids:
            ids.remove(tid)
        if not ids:
            index.pop(int(key), None)

    def _uncache(self, tid: int) -> None:
        self._v1r_pos.pop(tid, None)
        self._v1r_wait.discard(tid)
        row = self._open.pop(tid, None)
        if row is None:
            return
        self._drop(self._by_sid, row["security_id"], tid)
        self._drop(self._zone_by_sid, row["security_id"], tid)
        self._drop(self._zone_by_und, row.get("underlying_security_id"), tid)
        self._zone_pos.pop(tid, None)
        self._unapplied.pop(tid, None)
        self._drop(self._sw_by_sid, row["security_id"], tid)
        self._drop(self._sw_by_und, row.get("underlying_security_id"), tid)
        self._sw_pos.pop(tid, None)
        self._sw_last_bar.pop(tid, None)

    @staticmethod
    def _position(row: Mapping[str, Any]) -> rules.Position:
        return rules.Position(
            hold=row["hold"],
            option_type=str(row["option_type"]),
            entry=float(row["entry"]),
            t1=float(row["t1"]),
            t2=float(row["t2"]),
            sl=None if row["sl"] is None else float(row["sl"]),
            tick_size=None if row["tick_size"] is None else float(row["tick_size"]),
            zone_edge=None if row["zone_edge"] is None else float(row["zone_edge"]),
            armed=bool(row["armed"]),
            state=row["state"],
            exit_px=None if row["exit_px"] is None else float(row["exit_px"]),
            exit_ts_ms=None if row["exit_ts_ms"] is None else int(row["exit_ts_ms"]),
        )

    def _contract_info(self, row: Mapping[str, Any]) -> tuple[str | None, float | None]:
        """T-170: the ONE resolver (gammad.trade.identity), cached per TRADE id (was per security id, newest watchlist
        row, no expiry / option-type guard: a restart pushed an old trade under a re-used id's newer contract)."""
        tid = int(row["id"])
        if tid not in self._contract:
            from gammad.trade.identity import resolve_contract

            sig = self.conn.execute("SELECT payload_json FROM signals WHERE id = ?", (row.get("entry_signal_id"),))
            payload = sig.fetchone()
            self._contract[tid] = resolve_contract(
                self.conn, int(row["security_id"]), row.get("entry_date"), row.get("expiry_date"),
                row.get("option_type"), payload[0] if payload is not None else None,
            )  # fmt: skip
        return self._contract[tid]

    def _trade_payload(self, row: Mapping[str, Any]) -> dict[str, Any]:
        symbol, strike = self._contract_info(row)
        return {**row, "symbol": symbol, "strike": strike}

    def _view(self, row: Mapping[str, Any]) -> dict[str, Any]:
        events = [dict(r) for r in self.conn.execute(
            "SELECT * FROM trade_events WHERE trade_id = ? ORDER BY id", (row["id"],)).fetchall()]  # fmt: skip
        for ev in events:  # D125 F6: a stop exit's slippage, (fill - stop) / stop %; None for every other event
            ev["slippage_pct"] = rules.slippage_pct(ev.get("fill_px"), ev.get("price"))
        # T-267 trade view (PLAN-T267 §1 "Trade view"): {scanner, stop: {top, bottom, confirmed_ms} | None, steps, und_entry}
        scanner = row.get("scanner") or "V1"
        stop = rules.stop_zone_of(row.get("stop_top"), row.get("stop_bottom"), row.get("stop_confirmed_ms"))
        steps = list_zone_steps(self.conn, int(row["id"])) if scanner == rules.V2T else []
        return {
            **self._trade_payload(row), "events": events, "scanner": scanner,
            "stop": None if stop is None else {"top": stop.top, "bottom": stop.bottom, "confirmed_ms": stop.confirmed_ms},
            "steps": steps, "und_entry": row.get("und_entry"), "tier": self._entry_tier(row),
        }  # fmt: skip

    def _entry_tier(self, row: Mapping[str, Any]) -> str | None:
        """D296 M1: the entry signal's stored tier (signals.tier, the /api/trades `tier` column), so a trade pushed over
        WS draws its real / paper chip without a reload. None = untiered or no signals row."""
        got = self.conn.execute("SELECT tier FROM signals WHERE id = ?", (row.get("entry_signal_id"),)).fetchone()
        return None if got is None or got[0] is None else str(got[0])

    def _push(self, row: Mapping[str, Any]) -> None:
        if self.push is None:
            return
        try:
            self.push({"type": "trade", **self._view(row)})
        except Exception:  # the dashboard must never break the tracker
            log.exception("trade tracker: dashboard push failed for trade %s", row.get("id"))


# ---------------------------------------------------------------------- scheduler (the orchestrator calls this)


def schedule_trade_jobs(
    scheduler: Any,
    tracker: TradeTracker,
    adapter: Callable[[], DailyCandles | None],
    *,
    is_trading_day: Callable[[date], bool] | None = None,
    now: Callable[[], datetime] = now_ist,
) -> list[str]:
    """Register trades_eod 15:20, trades_daily 16:05 and trades_daily_retry 09:21 IST (§4) on an APScheduler
    scheduler; each skips a non-trading day. `adapter` is called at run time (Runtime.adapter). Returns the ids."""
    from apscheduler.triggers.cron import CronTrigger

    trading = is_trading_day or tracker.calendar.is_trading_day

    def _guarded(fn: Callable[[datetime], Awaitable[Any]]) -> Callable[[], Awaitable[Any]]:
        async def job() -> Any:
            t = now()
            if not trading(t.date()):
                return None
            return await fn(t)

        return job

    jobs: list[tuple[str, time, Callable[[datetime], Awaitable[Any]], int]] = [
        (JOB_EOD, EOD_AT, lambda t: tracker.run_eod(t), 300),
        (JOB_DAILY, DAILY_AT, lambda t: tracker.run_daily(adapter(), t), 3600),
        (JOB_DAILY_RETRY, DAILY_RETRY_AT, lambda t: tracker.run_daily_retry(adapter(), t), 3600),
    ]
    for job_id, at, fn, grace in jobs:
        scheduler.add_job(
            _guarded(fn),
            CronTrigger(hour=at.hour, minute=at.minute, timezone=IST),
            id=job_id,
            name=f"{at:%H:%M} {job_id}",
            replace_existing=True,
            coalesce=True,
            max_instances=1,
            misfire_grace_time=grace,
        )
    return [j[0] for j in jobs]


__all__ = [
    "CLOSE_DAY_REASON", "DAILY_AT", "DAILY_RETRY_AT", "EOD_AT", "FOLLOWUP_STATUSES", "JOB_DAILY", "JOB_DAILY_RETRY", "JOB_EOD",
    "FollowupRouter", "TradeTracker", "event_level", "schedule_trade_jobs",
]
