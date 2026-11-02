"""The ONE supervisor: scheduler + live feed + bars + recorder + engines + alert router + API, one asyncio loop.

Wiring (strategy Steps 1-4; D9, D10, D11, D24):
  scheduler (APScheduler AsyncIOScheduler, Asia/Kolkata), slots derived from config.schedule:
    a time in BOTH mark_levels and build_watchlists (09:20) -> mark_levels(ref=ltp), then
      build_watchlists(kind=<HHMM>) as soon as it finishes (skipped if mark_levels failed outright);
    a time only in build_watchlists (13:15)                -> build_watchlists(kind=<HHMM>);
    a time only in mark_levels (16:00)                     -> mark_levels(ref=close if >= 15:30 else ltp);
    T-277 (S1.8.0): 09:10 (schedule.preopen_build) -> mark_levels(prevclose) + build_watchlists(0910), no post;
      the 09:20 mark is a price-only re-mark on that run (base="auto"; none -> today's full run); ONE `refresh` job
      (schedule.refresh, 09:30..14:45 every 15 min) -> re-mark + build_watchlists(refresh) + load_day, and at the
      schedule.group_additions marks the router's post_pending_additions; a build time on a refresh mark (13:15) is
      run by the refresh (no job of its own);
    15:35 close_day -> stop the recorder (it flushes + closes the day) and reset every engine, then the
      daily_summary card to the GROUP (D46; not sent while alerts are paused).
  After every scheduled build (and a manual one run with post=True): the rows it ADDED, one watchlist_post
    per (list_name, expiry_date) to the GROUP (D45; a list with no new row posts nothing; paused = no post).
    T-181 F2: a manual 09:20-kind run (tray "Run 09:20 now") posts every list when no 09:20 post was sent today.
  Every scheduled run first checks core.calendar.is_trading_day(today); a holiday / weekend is SKIPPED.
  After every watchlist build: subscriptions.build_subscriptions -> feed.add_instruments (new only)
    -> warm-up (D24) for the NEW contracts, in a worker thread.
  feed task: Tick -> recorder.submit -> BarBuilder(security_id) -> 5m closes -> ContractEngine.on_bar_close;
    every tick of an ARMED contract -> ContractEngine.on_tick (E1 breakout). Signals -> AlertRouter with the
    ContractMeta of the contract's watchlist row -> Telegram + the WS hub (router's dashboard push).
  market-hours guard (checked every 2 s): the feed runs only on a trading day 09:15 <= t < 15:30 IST.
  stop: `stop_event` (tray Quit, Ctrl-C, or the data/STOP file written by scripts/stop.cmd, polled every 2 s;
    the day pass -- warm-up included -- runs as a task off that poll, T-060).

W5 plug points (duck-typed, nothing imported from gammad/auth):
  runtime.register_token_watcher(obj): obj.on_feed_auth_error(code, msg) is called on feed 806-809 /
    handshake 401/403; obj.token_status() (if present) becomes `token_health_hook`; obj.update_token
    (if present) becomes `token_updater` unless one is already set.
  runtime.token_health_hook: Callable[[], Mapping] -> {valid, validity_ist, ...} for the status token block.
  runtime.token_updater: Callable[[str], Any | Awaitable] -> POST /api/token delegates to it (501 when None).
  runtime.on_auth_error(code, msg): the callback handed to the feed.

W12 seams (PLAN-W12 §2, seam ledger):
  broker   `registry.resolve(BROKER, edition)` -> `broker_entry` (owner + BROKER absent -> dhan; client + absent ->
           none, the scanner stays idle). Adapter = entry.make_adapter(creds, data_dir); feed =
           entry.make_feed(creds, queue, on_auth_error, adapter=runtime.adapter()) for EVERY broker (R1/R10);
           `capabilities.assess` (runtime.scanner): BLOCKED -> no engines; DEGRADED -> signal snapshot
           feed_poll_s; candle_oi False -> warm-up OI primed from one quotes() snapshot.
  tracker  TradeTracker: router.trade_hook, on_tick for open trades, the subscription union, snapshot_closes
           first in close_day, trades_eod / trades_daily / trades_daily_retry jobs, resume() at run start;
           its pushes go out as WS "trade" (`_push_trade`).
  S1.3.0   the contract's tick_size to the engine; the underlying's closed 5-min bars today (a BarBuilder per
           underlying fed by its QUOTE ticks, seeded from adapter.intraday on a mid-day start) + the signal
           snapshot to build_context; the bar timer wakes at the next bar end + grace (bars.next_close_deadline).
  T-086    a restart after 09:15 re-seeds today's recorded bars + OI peak and replays them SILENTLY through the
           engines (warmup.load_today), so an ARMED contract is ARMED again and nothing is re-sent.
"""

from __future__ import annotations

import asyncio
import contextlib
import inspect
import json
import math
import sqlite3
import threading
import time
from collections import deque
from collections.abc import Awaitable, Callable, Coroutine, Iterable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import date, datetime, timedelta
from datetime import time as dtime
from pathlib import Path
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Protocol

from gammad import __version__
from gammad import edition as _edition
from gammad.alerts.gates import build_context
from gammad.api.server import (
    EventHub,
    entered_e1_ts,
    project_state,
    s089_manual_card,
    s089_send_manual,
    s089_signal_key,
)
from gammad.broker.base import BrokerAdapter, BrokerAuthError, BrokerEntitlementError, Candle, TokenHealth
from gammad.broker.capabilities import BrokerEntry, ScannerMode, assess
from gammad.broker.hints import EVENT_DATA_REFUSED, token_line
from gammad.config import IST, ScheduleCfg, Settings, StillWatchingCfg, now_ist
from gammad.core import calendar as cal
from gammad.core import lookahead as _lookahead
from gammad.core import session_clock, v1_hours
from gammad.core.sessions import era_for
from gammad.jobs.announce import announce_start
from gammad.jobs.carry import CARRY_TAG, SwCfg, entered_options, still_watching_cfg
from gammad.jobs.readiness import ReadinessJob
from gammad.log import get_logger
from gammad.record.stale import is_stale_tick
from gammad.store import db
from gammad.store import lifecycle as store_lifecycle
from gammad.trade.rules import V1R_STATES, trading_dte
from gammad.trade.tracker import (
    SW_NO_ZONE,
    TradeTracker,
    schedule_trade_jobs,
    sw_daily_zones,
    sw_entry_levels,
    sw_ref_px,
)
from gammad.watch import subscriptions as subs
from gammad.watch.adapter import ContractBars, OiCarry
from gammad.watch.bars import (
    DEFAULT_TIMER_GRACE_MS,
    MINUTE_MS,
    BarBuilder,
    BarClose,
    next_close_deadline,
    session_bounds_ms,
)
from gammad.watch.engine import (
    SW_ANY3_SCANNER,
    SW_BAN_OK,
    SW_BAN_UNKNOWN,
    ContractEngine,
    EngineParams,
    State,
    ist_date,
    sw_entry_block,
    sw_regime_prev,
)
from gammad.watch.engine_pool import (
    V2T,
    EnginePool,
    HoldParams,
    ScannerSpec,
    arm_context,
    build_scanners,
    make_any3_pool,
    make_pool,
)
from gammad.watch.feed import Instrument, Tick
from gammad.watch.feed_alarm import FEED_SILENT_S, FeedAlarm
from gammad.watch.types import ContractMeta, Signal, SignalContext
from gammad.watch.warmup import (
    TodayResume,
    apply_opening,
    backfill_today,
    bar_close_peak,
    fetch_opening_bars,
    fetch_warmup,
    load_today,
    missed_breakout,
    opening_cutoff_ms,
    preload,
    slot_candles,
)

if TYPE_CHECKING:  # I7 / AM7 (D288): gammad/v2scan is owner-only and leaves the client package; imported lazily below
    from gammad.v2scan import h1zones
    from gammad.v2scan.h60_store import H60Store
    from gammad.v2scan.regime import Regime

log = get_logger(__name__)

SESSION_OPEN_HM = "09:15"
SESSION_CLOSE_HM = "15:30"
CLOSE_DAY_AT = "15:35"
CLOSE_DAY_JOB = "close_day"
SCANNER_DAY_END_JOB = "scanner_day_end"  # W15 §6: 15:35 owner PERSONAL card
SCANNER_WEEKLY_JOB = "scanner_weekly"  # W15 §6: Friday 16:05 owner PERSONAL review
READINESS_0900_JOB = "readiness_0900"  # W16 T-132 (B4 / S2): ONE READY / NOT READY card per trading day
READINESS_0910_JOB = "readiness_0910"  # W16 T-132: the all-clear, only after a NOT READY at 09:00
SUPERVISE_EVERY_S = 2.0
TIMER_EVERY_S = 1.0  # the bar timer's idle poll (no trade date / after the session)
TIMER_MIN_S = 0.01
TIMER_MAX_S = 60.0  # never trust one long sleep across a clock jump / resume from hibernation


def _expiry_trading_days(trade_date: Any, expiry_date: Any) -> int | None:
    """T-556 (D542 (3)): trading days to expiry, the REST trigger's rule (review.trigger_fields)."""
    try:
        return trading_dte(
            cal.default_calendar(), date.fromisoformat(str(trade_date)), date.fromisoformat(str(expiry_date))
        )
    except (TypeError, ValueError, LookupError, OSError):
        return None


def startup_line(version: str, spec_version: str, *, client: bool) -> str:
    """The start log line; the client's never names the strategy spec (D241(2): the Logs page shows gammad.log)."""
    return f"gammad {version} starting" if client else f"gammad {version} (spec {spec_version}) starting"
UNDERLYING_SEGMENT = "NSE_EQ"
UNDERLYING_INSTRUMENT = "EQUITY"  # Dhan's instrument for NSE_EQ history (DhanAdapter.daily_candles default)
TICK_PUSH_MIN_MS = 250  # <= 4 tick pushes per second per contract
LATENCY_WINDOW = 200
RUNS_TODAY_TTL_S = 10  # perf B3: status()'s runs_today re-read at most this often with no job_state move
BUILD_KINDS = ("0910", "0920", "refresh", "1315", "manual")  # = build_watchlists.RUN_KINDS (T-277)
CATCHUP_UNTIL_HM = "15:20"  # W14 H5: a missed 09:20 build is caught up only before this (IST)
RETIRE_BUILD_KIND = "0920"  # = build_watchlists.RETIRE_KIND (T-063): the build kind whose retire runs as its own step
# T-277 (S1.8.0, design C3): the 09:10 pre-open build on the previous close and the ONE 15-minute refresh job
PREOPEN_JOB = "0910"  # job id = job_state key = the build kind (no clash with readiness_0910)
PREOPEN_KIND = "0910"  # = build_watchlists.PREOPEN_KIND
REFRESH_JOB = "refresh"  # ONE job_state row / ONE scheduler job for every refresh mark
REFRESH_KIND = "refresh"  # = build_watchlists.REFRESH_KIND
# mark_levels(ltp) ahead of one of these builds = mark_levels.run(base="auto"): a price-only re-mark on the day's newest
# full run (09:10 prevclose / an earlier full ltp run), else today's full ltp run (which then also runs the regime step)
REMARK_BUILD_KINDS = ("0920", "refresh", "1315")
REMARK_BASE = "auto"  # = mark_levels.AUTO_BASE
REFRESH_MISFIRE_S = 300  # a refresh more than 5 min after its mark is pointless: skipped (the next mark comes soon)
WAIT_0920_S = 1500.0  # the 09:20 slot waits this long (its misfire grace) for a running 09:10 chain, never skips it
RETRY_0920_AFTER_S = 600.0  # T-213 (D189(5)): a FAILED 09:20-kind run is retried ONCE per day, this long after it
LATE_POST_AFTER_MIN = 10  # T-215 (F11, D189(6)): a 09:20 post this long after the slot says `posted HH:MM`
# T-213 (audit S5): a spawned task whose name starts with one of these sends ONE personal SYSTEM line when it raises
REPORTED_TASKS = ("watchlist-post", "carry-line", "day-end", "s089-delivery")  # audit P2B-APP-06: + 0.8.9 cards
PREPARE_089_RETRIES = 3  # audit P2B-APP-03 (review F1): a failed first s089 prepare of the day is redone fresh <= this
DELIVERY_CHECK_KIND = "delivery_check"  # = router KIND_DELIVERY_CHECK (FIX-WAVE C2): the verdict's manual_sends kind
STOP_FILE_NAME = "STOP"
# S1.7.0 (T-267 hunk A): the V2 Scanner's live plumbing
V2T_OPENING_DELAY_MS = 5_000  # M8: the opening fetch runs this long after the subscription slot's close
H60_SYNC_JOB = "h60_sync_{hm}"  # schedule.h60_sync: 08:30 top-up (through yesterday) + 16:05 sync + reconcile (M13)
H1_HOUR_MS = 3_600_000
_SEED_UNSET: Any = object()  # Runtime._h1_seed before the packaged zone seed is read
# 0.8.9 T-419 (Still Watching, spec 4.8; D480 (2)): the daily NSE F&O ban-list fetch (both editions, still_watching
# .ban_list.fetch_hm, 08:45 IST)
SW_BAN_JOB = "sw_ban_list"
SW_DAILY_BARS = 500  # = v2scan.regime DAILY_BARS_DEFAULT (the ST-PREV daily fetch); NOT ZonesCfg.daily_bars since D506 (2,000): Supertrend converges at 500 (D250)


# ------------------------------------------------------------------------------------------ helpers
def _hm(s: str) -> tuple[int, int]:
    hh, mm = s.strip().split(":")
    h, m = int(hh), int(mm)
    if not (0 <= h < 24 and 0 <= m < 60):
        raise ValueError(f"bad HH:MM {s!r}")
    return h, m


def _runs_v2t(rt: Any) -> bool:
    """AM6 (PLAN-D287-085): the V2 Scanner's own jobs (h60_sync) exist only when the V2T pool does (owner, D287 (6) /
    I7). A stand-in without `shadow_pools` (tools/edition_probe's namespace) follows the edition, as the scanner
    registry does: the client never runs V2."""
    pools = getattr(rt, "shadow_pools", None)
    if pools is None:
        return not _edition.is_client()
    return V2T in pools


# T-091: a resume must never stop the day on a damaged record (a huge integer, deep nesting, garbage text)
_EVIDENCE_ERRORS = (TypeError, ValueError, AttributeError, OverflowError, RecursionError)


class _Unreadable(Exception):
    """A recorded value that is present but cannot be read (counted, then treated as no evidence)."""


def _json_dict(raw: Any) -> dict[str, Any]:
    """{} for an absent document; the object for a JSON object; _Unreadable for anything else."""
    if raw is None or raw == "":
        return {}
    try:
        v = json.loads(raw)
    except _EVIDENCE_ERRORS as exc:
        raise _Unreadable(type(exc).__name__) from None
    if not isinstance(v, dict):
        raise _Unreadable("not an object")
    return v


def _finite(v: Any) -> float | None:
    """None for an absent value; the float for a finite number; _Unreadable for anything else."""
    if v is None:
        return None
    try:
        if isinstance(v, bool):
            raise TypeError("bool")
        f = float(v)
    except _EVIDENCE_ERRORS as exc:
        raise _Unreadable(type(exc).__name__) from None
    if not math.isfinite(f):
        raise _Unreadable("not finite")
    return f


def _pick(docs: Iterable[tuple[Mapping[str, Any], tuple[str, ...]]], bad: list[int]) -> float | None:
    """The first readable number along each (document, key path); every unreadable one found adds 1 to bad[0]."""
    for doc, path in docs:
        v: Any = doc
        for k in path:
            v = v.get(k) if isinstance(v, Mapping) else None
        try:
            f = _finite(v)
        except _Unreadable:
            bad[0] += 1
            continue
        if f is not None:
            return f
    return None


def recorded_oi_now(payload_json: str | None, ctx_json: str | None = None, key: str = "oi_now") -> float | None:
    """What the LIVE engine was handed at a recorded signal (`key` oi_now or oi_peak): payload_json snapshot[key]
    (S1.3.0+), else ctx_json[key] (SignalContext: "oi = exactly what the engine was handed"; rows recorded before the
    snapshot existed carry only this). None when neither is readable. Never raises."""
    bad = [0]
    docs = []
    for raw in (payload_json, ctx_json):
        try:
            docs.append(_json_dict(raw))
        except _Unreadable:
            docs.append({})
    return _pick(((docs[0], ("snapshot", key)), (docs[1], (key,))), bad)


@dataclass(frozen=True, slots=True)
class RecordedEntry:
    """One of today's recorded E1s of a contract, as the restart replay applies it (T-091).
    hidden          a client's hidden gated E1 (a closed_cycles row, D98(4)): no signals row, nothing to reveal.
    oi_now          the OI the live engine was handed (its oi_at_alert after the E1); None when not recorded.
    consol          the cycle's (consol_lo, consol_hi, consol_mid) from the signal payload, when present.
    cycle_e2_ts_ms  the cycle's arming bar (owner snapshot cycle_e2_ts_ms), when present.
    bar_ts_ms       the bar the E1 fired in (snapshot bar_ts_ms), when present (T-104a: see order_key)."""

    ts_ms: int
    oi_now: float | None
    hidden: bool = False
    consol: tuple[float, float, float] | None = None
    cycle_e2_ts_ms: int | None = None
    bar_ts_ms: int | None = None

    def order_key(self, span_ms: int) -> tuple[int, int, int]:
        """T-104(a): where the replay applies this E1 -- before the first bar close C with key[0] < C. A breakout
        (bar_ts_ms recorded, not its bar's close) fired in the bar STARTING at bar_ts_ms, so after that bar's opening
        close: a late tick (exchange time < C, received after the timer closed the bar ending at C) was folded into
        the bar starting at C, and its ts_ms < C would wrongly put it before close C. A shakeout (ts = its bar's close)
        and a row without bar_ts_ms (a pre-v6 closed_cycles row; pre-snapshot rows) go by ts_ms. At the same
        ms a shakeout (decided AT that close) comes before a breakout (a tick in the bar after it)."""
        if self.bar_ts_ms is not None and self.bar_ts_ms + span_ms != self.ts_ms:
            return (self.bar_ts_ms, 1, self.ts_ms)
        return (self.ts_ms, 0, self.ts_ms)


@dataclass(frozen=True, slots=True)
class RecordedArm:
    """I8 (PLAN-D287-085): one of today's recorded V1 E2s of a contract, the restart replay's ARM evidence.
    ts_ms      the E2's time = its arming bar's close.
    consol     (consol_lo, consol_hi, consol_mid) from the signal payload; None when unreadable.
    bar_ts_ms  the arming bar's open (snapshot bar_ts_ms), when recorded.
    oi_now     the OI the live engine was handed at that close (its oi_at_alert after the E2), when recorded.
    cycle      the cycle's unwind inputs from the snapshot (cycle_e2_* / oi_peak_bar_*, D94(1)): the E1 inherits them."""

    ts_ms: int
    consol: tuple[float, float, float] | None
    bar_ts_ms: int | None = None
    oi_now: float | None = None
    cycle: Mapping[str, Any] | None = None


# the engine's cycle inputs a recorded E2 snapshot carries (engine.ContractEngine._cycle_inputs; T-160 adds arm_*)
_CYCLE_KEYS = (
    "cycle_e2_ts_ms", "cycle_e2_close", "oi_peak_bar_ts_ms", "oi_peak_bar_close", "oi_peak_bar_oi", "oi_peak_bar_exact",
)  # fmt: skip


@dataclass
class RecordedDay:
    """T-091: today's recorded evidence for the restart replay, read once (few rows a day: signals, not ticks).
    bar_inputs  (security_id, bar close ms) -> the (oi_now, oi_peak) live handed the engine at that close: every E2
                (ts = its arming bar's close) and a shakeout E1 (snapshot bar_ts_ms + tf == ts). A breakout E1 is
                stamped with its tick's time, not a bar close.
    arms        I8 (D287): security_id -> today's recorded V1 E2s (RecordedArm), oldest first: a replay that does not
                reproduce one at its bar is forced ARMED on it. (The 0.8.4 e2_oi re-arm reference is gone: D287 (1)
                removed the re-arm it fed.)
    entries     security_id -> today's E1s (visible signals rows + hidden closed_cycles rows), in replay order
                (RecordedEntry.order_key, T-104a). In the CLIENT this includes its entry rows (the E2 bar close,
                stored as kind E1, D288).
    unreadable  recorded values (or rows) present but unreadable: treated as no evidence, counted."""

    bar_inputs: dict[tuple[int, int], tuple[float, float]] = field(default_factory=dict)
    arms: dict[int, list[RecordedArm]] = field(default_factory=dict)
    entries: dict[int, list[RecordedEntry]] = field(default_factory=dict)
    unreadable: int = 0


def read_recorded_day(conn: sqlite3.Connection, d: date, tf_min: int = 5) -> RecordedDay:
    """Today's recorded signals + closed_cycles as RecordedDay. Never raises: a store error or a damaged row is
    counted in `unreadable` and read as no evidence (a resume bug never stops the day)."""
    span = tf_min * MINUTE_MS
    out = RecordedDay()
    bad = [0]
    try:
        rows = conn.execute(
            "SELECT security_id, ts_ms, kind, payload_json, ctx_json FROM signals WHERE trade_date = ?"
            " AND scanner = 'V1' ORDER BY id",  # W15: a V2 shadow row never closes a live cycle
            (d.isoformat(),),
        ).fetchall()
    except sqlite3.Error:
        log.exception("resume: today's recorded signals could not be read; bars replay with their own OI")
        rows = []
        bad[0] += 1
    for sid_raw, ts_raw, kind, payload_raw, ctx_raw in rows:
        try:
            sid, ts = int(sid_raw), int(ts_raw)
        except _EVIDENCE_ERRORS:
            bad[0] += 1
            continue
        docs: list[dict[str, Any]] = []
        for raw in (payload_raw, ctx_raw):
            try:
                docs.append(_json_dict(raw))
            except _Unreadable:
                bad[0] += 1
                docs.append({})
        payload, ctx = docs
        oi_now = _pick(((payload, ("snapshot", "oi_now")), (ctx, ("oi_now",))), bad)
        peak = _pick(((payload, ("snapshot", "oi_peak")), (ctx, ("oi_peak",))), bad)
        bar_ts = _pick(((payload, ("snapshot", "bar_ts_ms")),), bad)
        if kind not in ("E1", "E2"):
            continue
        lo, hi, mid = (_pick(((payload, (k,)),), bad) for k in ("consol_lo", "consol_hi", "consol_mid"))
        consol = (lo, hi, mid) if lo is not None and hi is not None and mid is not None else None
        if kind == "E2":  # I8: arm evidence (the replay forces ARMED when it does not reproduce it)
            snap = payload.get("snapshot")
            cycle = {k: v for k, v in snap.items() if k in _CYCLE_KEYS or k.startswith("arm_")} if isinstance(
                snap, Mapping) else {}  # fmt: skip
            out.arms.setdefault(sid, []).append(
                RecordedArm(ts, consol, None if bar_ts is None else int(bar_ts), oi_now, cycle)
            )
        else:  # E1
            cyc = _pick(((payload, ("snapshot", "cycle_e2_ts_ms")),), bad)
            out.entries.setdefault(sid, []).append(
                RecordedEntry(ts, oi_now, False, consol, None if cyc is None else int(cyc),
                              None if bar_ts is None else int(bar_ts))
            )  # fmt: skip
            if bar_ts is None or int(bar_ts) + span != ts:
                continue  # a breakout: not decided at a bar close
        if oi_now is not None and peak is not None:
            out.bar_inputs[sid, ts] = (oi_now, peak)
    hidden: list[Any] = []
    for sql in (  # T-107: v6 records the hidden entry's bar; a pre-v6 store (no column) is read by ts alone
        "SELECT security_id, ts_ms, oi_now, bar_ts_ms FROM closed_cycles WHERE trade_date = ? ORDER BY id",
        "SELECT security_id, ts_ms, oi_now, NULL FROM closed_cycles WHERE trade_date = ? ORDER BY id",
    ):
        try:
            hidden = conn.execute(sql, (d.isoformat(),)).fetchall()
            break
        except sqlite3.Error:  # a store without the table (pre-D98 v4): nothing was hidden in it
            continue
    for sid_raw, ts_raw, oi_raw, bar_raw in hidden:
        try:
            sid, ts = int(sid_raw), int(ts_raw)
            oi = _finite(oi_raw)
            bar_ts = None if bar_raw is None else int(bar_raw)
        except (_Unreadable, *_EVIDENCE_ERRORS):
            bad[0] += 1
            continue
        out.entries.setdefault(sid, []).append(RecordedEntry(ts, oi, True, bar_ts_ms=bar_ts))
    for lst in out.entries.values():
        lst.sort(key=lambda e: e.order_key(span))  # T-104(a): by the bar each E1 fired in, then its time
    for arms in out.arms.values():
        arms.sort(key=lambda a: a.ts_ms)
    out.unreadable = bad[0]
    return out


def percentile(values: Iterable[float], p: float) -> float | None:
    """Nearest-rank percentile; None for an empty input."""
    xs = sorted(values)
    if not xs:
        return None
    k = max(1, math.ceil(p / 100.0 * len(xs)))
    return float(xs[k - 1])


def token_level(valid: bool | None, hours_left: float | None, warn_hours: Iterable[int]) -> str:
    """ok | warn24 | warn2 | expired | unknown. Thresholds come from alerts.token_warn_hours."""
    if valid is None and hours_left is None:
        return "unknown"
    if valid is False or (hours_left is not None and hours_left <= 0):
        return "expired"
    if hours_left is None:
        return "ok" if valid else "unknown"
    for h in sorted(warn_hours):
        if hours_left <= h:
            return f"warn{h}"
    return "ok"


def zone_dist_pct(row: Mapping[str, Any]) -> float | None:
    """Stock distance to the zone edge in percent units (D20), from the watchlist row."""
    ref, edge = row.get("ref_price"), row.get("zone_edge")
    if ref is None or edge is None or float(ref) <= 0:
        return None
    ref_f, edge_f = float(ref), float(edge)
    d = (ref_f - edge_f) if str(row.get("option_type")).upper() == "PE" else (edge_f - ref_f)
    return round(d / ref_f * 100.0, 4)


def meta_from_row(row: Mapping[str, Any]) -> ContractMeta:
    """Watchlist row -> ContractMeta. S1.3.0: `tick_size` (watchlist.tick_size, RUPEES) feeds test H and the
    unwind gate; a row without one (pre-v4 lists) -> None (the engine falls back to its own default)."""
    edge, lot, und = row.get("zone_edge"), row.get("lot_size"), row.get("underlying_security_id")
    tick = row.get("tick_size")
    # S1.5.0 (D157(2), D161): a CARRY row's `oi` is the previous close; its build-up base is the FIRST listing's OI
    carried = row.get("tag") == "CARRY"
    oi = row.get("oi_at_listing") if carried else row.get("oi")
    atr, k = row.get("atr_pct"), row.get("carry_k")
    return ContractMeta(
        symbol=str(row["symbol"]),
        strike=float(row["strike"]),
        option_type=str(row["option_type"]),
        list_name=str(row["list_name"]),
        expiry_date=str(row["expiry_date"]),
        tag=row.get("tag"),
        zone_dist_pct=zone_dist_pct(row),
        zone_edge=float(edge) if edge is not None else None,
        lot_size=int(lot) if lot is not None else None,
        underlying_security_id=int(und) if und is not None else None,
        tick_size=float(tick) if tick is not None and float(tick) > 0 else None,
        build_oi=float(oi) if oi is not None else None,  # T-160 (D125 F9): the 09:20 build OI
        atr_pct=float(atr) if atr is not None else None,  # S1.5.0: the V1 dynamic zone gate (D150(2))
        carried=carried,
        carry_state=row.get("carry_state") if carried else None,
        carry_k=int(k) if carried and k is not None else None,
    )


@dataclass(frozen=True)
class Slot:
    """One scheduled time and what runs at it."""

    job_id: str  # "0920"
    at: str  # "09:20"
    mark_ref: str | None  # "ltp" | "close" | "prevclose" | None
    build_kind: str | None  # "0910" | "0920" | "1315" | "manual" | None
    # T-277: False = a build time that is also a refresh mark (the old 13:15): the refresh job runs it, so the
    # scheduler registers no job and status.jobs shows no row for it; run_slot still runs it by hand (replay_day)
    scheduled: bool = True

    @property
    def name(self) -> str:
        parts = []
        if self.mark_ref:
            parts.append(f"mark_levels({self.mark_ref})")
        if self.build_kind:
            parts.append(f"build_watchlists({self.build_kind})")
        return f"{self.at} " + " + ".join(parts)

    def steps(self) -> list[tuple[str, str]]:
        out: list[tuple[str, str]] = []
        if self.mark_ref:
            out.append(("mark_levels", self.mark_ref))
        if self.build_kind:
            out.append(("build_watchlists", self.build_kind))
        return out


def plan_slots(schedule: ScheduleCfg, v1r: Any = None, day: date | None = None) -> list[Slot]:
    """The fixed-time slots: the T-277 pre-open slot (`preopen_build`, "" = none) + one per mark_levels /
    build_watchlists time. A build time other than 09:20 that sits on a refresh mark is `scheduled=False` (the refresh
    job runs it: never a second build beside the refresh). The refresh marks themselves are not slots (REFRESH_JOB)."""
    marks = {f"{h:02d}:{m:02d}" for h, m in map(_hm, schedule.mark_levels)}
    builds = {f"{h:02d}:{m:02d}" for h, m in map(_hm, schedule.build_watchlists)}
    pre = getattr(schedule, "preopen_build", "")
    refresh = set(schedule.refresh.marks()) if getattr(schedule, "refresh", None) is not None else set()
    if v1r is not None:
        refresh = set(v1r_refresh_marks(v1r, tuple(refresh)))
    out: list[Slot] = [Slot(PREOPEN_JOB, pre, "prevclose", PREOPEN_KIND)] if pre else []
    close_hm = SESSION_CLOSE_HM if day is None else session_clock.fo_close_hm(day)
    for t in sorted(marks | builds):
        if t == pre:  # the pre-open slot owns its time
            continue
        job_id = t.replace(":", "")
        ref = ("close" if t >= close_hm else "ltp") if t in marks else None
        kind = (job_id if job_id in BUILD_KINDS else "manual") if t in builds else None
        absorbed = kind is not None and kind != RETIRE_BUILD_KIND and t in refresh
        out.append(Slot(job_id, t, ref, kind, scheduled=not absorbed))
    return sorted(out, key=lambda s: s.at)


def refresh_trigger(marks: Sequence[str]) -> Any:
    """T-277: ONE APScheduler trigger that fires at every refresh mark "HH:MM" (an OrTrigger of one CronTrigger per
    distinct minute set: 09:30,45 / 10-13 every quarter / 14:00..14:45). None when there is no mark."""
    if not marks:
        return None
    from apscheduler.triggers.combining import OrTrigger
    from apscheduler.triggers.cron import CronTrigger

    by_hour: dict[int, list[int]] = {}
    for hm in marks:
        h, m = _hm(hm)
        by_hour.setdefault(h, []).append(m)
    groups: dict[tuple[int, ...], list[int]] = {}
    for h, mins in sorted(by_hour.items()):
        groups.setdefault(tuple(sorted(mins)), []).append(h)
    triggers = [
        CronTrigger(hour=",".join(map(str, hours)), minute=",".join(map(str, mins)), timezone=IST)
        for mins, hours in groups.items()
    ]
    return triggers[0] if len(triggers) == 1 else OrTrigger(triggers)


# ------------------------------------------------------------------------------------------ seams
class FeedLike(Protocol):
    async def start(self, instruments: Iterable[Instrument] = ()) -> None: ...
    async def add_instruments(self, instruments: Iterable[Instrument]) -> int: ...
    async def stop(self) -> None: ...
    # registry feeds take the creds MAPPING (feed_protocol.Feed); the legacy Dhan feed also (client_id, token)
    def reload_credentials(self, *args: Any, **kwargs: Any) -> None: ...


FeedFactory = Callable[[asyncio.Queue[Tick], Callable[[int, str], None], Settings], FeedLike]


class RecorderLike(Protocol):
    def submit(self, tick: Tick) -> bool: ...
    def submit_bar(self, security_id: int, bar: Any) -> None: ...
    async def run(self, stop: asyncio.Event) -> None: ...


RecorderFactory = Callable[[Settings], RecorderLike]


def _readiness_lookahead() -> Callable[[date], Any] | None:
    return None if _edition.is_client() else _lookahead.from_files


def _default_recorder_factory(cfg: Settings) -> RecorderLike:
    from gammad.record.recorder import Recorder

    return Recorder(cfg.paths.db_path, cfg.paths.data_dir / "parquet")


@dataclass
class Jobs:
    """The Step 1/2 jobs, injectable for tests. All are blocking; the runtime calls them in a thread.

    `retire(trade_date, run_id)` = the S1.3.1 retire of a 09:20-kind build as its OWN step (T-063 M1): _execute runs
    it, then load_day (which drops the retired contracts), THEN build_watchlists(d, kind, run_id=, retire=False), so
    a retired contract routes nothing during the build's chain loop. None (a test's two-job fake) = the build retires
    inside run() as the CLI does."""

    mark_levels: Callable[..., dict[str, Any]]
    build_watchlists: Callable[..., dict[str, Any]]
    retire: Callable[..., dict[str, Any]] | None = None


def _default_jobs(runtime: Runtime) -> Jobs:
    from gammad.jobs import build_watchlists, mark_levels

    def _mark(trade_date: date, ref: str, *, bars_sink: dict[int, list[Candle]] | None = None,
              base: str | None = None, refresh_hm: str | None = None,
              prev_trading_day: date | None = None) -> dict[str, Any]:  # fmt: skip
        # T-267 hunk A: bars_sink receives the daily candles the run fetched (the V2 regime step reads them)
        # T-277: base ("auto" = a price-only re-mark when the day has a full run), refresh_hm ("HHMM" mark),
        # prev_trading_day (ref prevclose: the late_bar check)
        return mark_levels.run(trade_date, ref=ref, adapter=runtime.require_adapter(), cfg=runtime.cfg,  # type: ignore[arg-type]
                               bars_sink=bars_sink, base=base, refresh_hm=refresh_hm,
                               prev_trading_day=prev_trading_day)  # fmt: skip

    def _build(trade_date: date, kind: str, *, run_id: str | None = None, retire: bool = True,
               mark_hm: str | None = None) -> dict[str, Any]:  # fmt: skip
        return build_watchlists.run(
            trade_date, kind, ref="ltp", adapter=runtime.require_adapter(), cfg=runtime.cfg,  # type: ignore[arg-type]
            run_id=run_id, retire=retire, mark_hm=mark_hm, v1r=runtime.v1r,
        )  # fmt: skip

    def _retire(trade_date: date, run_id: str) -> dict[str, Any]:
        return build_watchlists.run_retire(trade_date, run_id, ref="ltp", cfg=runtime.cfg)

    return Jobs(_mark, _build, _retire)


@dataclass
class JobState:
    name: str
    last_run_ist: str | None = None
    last_status: str | None = None
    last_run_id: str | None = None
    detail: str | None = None


@dataclass
class Stats:
    ticks: int = 0
    engine_tick_calls: int = 0
    bar_closes_5m: int = 0
    signals: int = 0
    tick_errors: int = 0
    extra: dict[str, int] = field(default_factory=dict)


# ------------------------------------------------------------------------------------------ runtime
def load_v1r():
    from gammad.core import v1_params

    try:
        return v1_params.load()
    except (OSError, ValueError):
        log.exception("rebuild picks unavailable; all picks pending")
        return v1_params.all_pending()


def v1r_refresh_marks(params, legacy):
    from gammad.v1 import listing

    return tuple(m for m in listing.refresh_marks(params, legacy) if m not in ("09:10", "09:20"))


def v1r_listed_ms(conn, row):
    """The first listing's scheduled mark; start-time seconds never move eligibility."""
    import re

    run = conn.execute("SELECT note FROM runs WHERE run_id=?", (row.get("run_id"),)).fetchone()
    note = str(run[0] or "") if run else ""
    kind = re.search(r"kind=(\w+)", note)
    kind = kind[1] if kind else "0920"
    mark = re.search(r"mark=(\d{2}:?\d{2})", note)
    hm = mark[1].replace(":", "") if mark else kind
    if kind == "0910":
        hm = "0915"
    if not re.fullmatch(r"\d{4}", hm):
        raise ValueError("listing has no scheduled mark")
    return int(datetime.combine(date.fromisoformat(row["trade_date"]),
                                datetime.strptime(hm, "%H%M").replace(tzinfo=IST).time(), IST).timestamp() * 1000)


def v1r_sync_hours(desk, rt, day, und) -> int:
    """Append completed stock hours to the entry book and its immutable snapshot history."""
    from gammad.core.h1zones import hour_bars_from_5m
    from gammad.v1.carry_break import bar_array

    bars = tuple(map(bar_array, rt.und_bars.get(und, ())))
    if not bars:
        return 0
    hours = hour_bars_from_5m(bars, day, now_ms=max(b[0] for b in bars) + 300_000)
    book = rt.v1r_books.get(und)
    history = {h[0]: h for h in desk.h1.get((day, und), ())}
    appended = 0
    for h in hours:
        if book is not None and (book.last_close_ms is None or h[5] > book.last_close_ms):
            book.append(h)
            appended += 1
        history.setdefault(h[0], tuple(h))
    desk.h1[day, und] = tuple(history[t] for t in sorted(history))
    return appended


class V1Desk:
    """Live candidate holder; all entry and budget decisions come from the pure package."""

    def __init__(self, runtime, params):
        from collections import Counter

        self.rt, self.params = runtime, params
        self.stock_seed = {}
        self.h1 = {}
        self.nifty = {}
        self.ready = set()
        self.seen = {}
        self.pending = set()
        self.counts = Counter()
        self.reserved = set()

    def note_pending(self, side, day):
        key = (side, day)
        if key not in self.pending:
            self.pending.add(key)
            self.counts["pending_pick"] += 1
            log.info("rebuild %s %s: pending_pick", day, side)

    def decide(self, sid, closed, *, catch_up=False):
        from dataclasses import asdict

        from gammad.core.zones import ZoneState
        from gammad.v1 import budget, entry, stops
        from gammad.v1.carry_break import bar_array

        rt, p = self.rt, self.params
        row = rt.rows.get(sid, {})
        if row.get("tag") == "CARRY" or row.get("list_name") not in ("BREAKDOWN-PE", "BREAKOUT-CE"):
            return
        side, day = row["option_type"], rt.trade_date
        if not p.side_ready(side):
            self.note_pending(side, day)
            return
        if (day, sid) not in self.ready or sid in self.reserved:
            return
        now = rt._wall_ms()
        options = tuple(b for b in map(bar_array, closed) if b[0] + 300_000 <= now)
        options_today = tuple(b for b in options if datetime.fromtimestamp(b[0]/1000, IST).date() == day)
        und = int(row["underlying_security_id"])
        stocks = tuple(b for b in map(bar_array, rt.und_bars.get(und, ())) if b[0] + 300_000 <= now)
        if not stocks or not options_today or stocks[-1][0] != options_today[-1][0]:
            return
        ts, end = stocks[-1][0], stocks[-1][0] + 300_000
        if end != now - now % 300_000:
            self.counts["stale_slot"] += 1
            return
        if self.seen.get((day, sid), -1) >= ts:
            return
        self.seen[day, sid] = ts
        listed = v1r_listed_ms(rt.conn, row)
        cand = entry.Candidate(row["symbol"], side, float(row["zone_edge"]), listed, "list", str(sid), None)
        inputs = dict(stock_seed=self.stock_seed.get((day, und), ()),  # noqa: C408
                      option_seed=tuple(b for b in options if datetime.fromtimestamp(b[0]/1000, IST).date() < day),
                      rule=entry.rule_for(p, side, kind="list"), base=entry.base_for(p.base(side)),
                      filters=entry.filters_for(p.filters(side)), nifty=self.nifty.get(day, ()), day=day)
        if catch_up:
            for i, b in enumerate(stocks[:-1]):
                if b[0] + 300_000 >= listed:
                    old = entry.decide(cand, stock_today=stocks[:i+1],
                                       option_today=tuple(o for o in options_today if o[0] <= b[0]), **inputs)
                    self.counts["stale_break"] += int(old.entered)
        if stocks[-1][0] > v1_hours.last_entry_signal_ms(day):
            self.counts["entry_cut"] += 1
            return
        trades = rt.tracker.v1r_budget(day)
        allowed, why = budget.may_enter(trades, str(sid), symbol=cand.symbol, side=side, slot_close_ms=end)
        if not allowed:
            self.counts[why] += 1
            return
        if why == "reentry":
            previous = [t for t in trades if t.item == str(sid)][-1]
            cand = replace(cand, listed_ms=budget.reentry_listed_ms(previous))
        dec = entry.decide(cand, stock_today=stocks, option_today=options_today, **inputs)
        if not dec.entered:
            self.counts[dec.reason] += 1
            return
        v1r_sync_hours(self, rt, day, und)
        stop = stops.resolve(side, p.stop(side), entry_close=dec.stock_close,
                             daily_zones=sw_daily_zones(rt.conn, day.isoformat(), cand.symbol) or ZoneState(),
                             h1_book=rt.v1r_books.get(und) or rt.sw_books.get(und) or rt.h1_books.get(und),
                             t_ms=end)
        if stop.level is None and stop.cap is None:
            self.counts["no_stop_level"] += 1
            return
        px = rt.last_ltp.get(sid)
        if px is None or px <= 0:
            self.seen.pop((day, sid), None)
            return
        block = dict(side=side, level=cand.level, listed_ms=cand.listed_ms, break_ts=dec.break_ts,  # noqa: C408
                     stock_close=dec.stock_close, oi_drop=dec.oi_drop, oi=p.oi_unwind(side).name,
                     base=p.base(side), filters=list(p.filters(side)), stop=asdict(stop), exit=p.exit(side).name,
                     reentry=why == "reentry", entered=True,
                     history={"option": [b for b in options if b[0] <= ts],
                              "stock": [*inputs["stock_seed"], *stocks],
                              "h1": [h for h in self.h1.get((day, und), ()) if h[5] <= end]})
        signal = Signal("E1", False, now, sid, float(px), float(dec.oi_drop or 0)*100,
                        0., 0., 0., 0., float(px)*(1-stop.cap) if stop.cap is not None else 0.,
                        "stock close through", 0, {"bar_ts_ms": ts, "v1r": block})
        self.reserved.add(sid)  # the asynchronous route cannot admit this item twice
        rt._dispatch(signal, oi_now=options_today[-1][6], oi_peak=entry.bar_max_oi(options_today, day))


class S089Sender:
    """Share the router's pause gate and its safe send path with existing cards.

    Audit JOBS_ALERTS-02 (D713 (5)): router._deliver_full gives a 0.8.9 card the T-062 guard, the rate windows, the
    group -> supergroup migration follow and the failure streak (ERROR line, /api/status last_error); a delivery that
    is not sent raises, so s089_dispatch records it FAILED with the router's error text."""

    def __init__(self, router):
        self.router = router

    def is_muted(self, security_id):
        """Audit P2B-APP-02 (D42): the router's per-contract mute (mutes table); s089_dispatch records MUTED."""
        return self.router.is_muted(security_id)

    async def send(self, chat_id, text):
        from gammad.alerts.router import CH_GROUP, CH_PERSONAL, ST_SENT

        if self.router.paused:
            raise RuntimeError("paused")
        chat = str(chat_id)
        channel = CH_GROUP if chat == str(self.router.group_chat_id) else CH_PERSONAL
        delivery = await self.router._deliver_full(channel, chat, text)
        if delivery.status != ST_SENT:
            raise S089SendFailed(delivery.error or delivery.status)
        return delivery


class S089SendFailed(RuntimeError):
    """A 0.8.9 card the router did not deliver (its error text is the message)."""


class S089Chats(Mapping):
    """The router's CURRENT chat ids: a group -> supergroup migration re-points the group mid-process. A personal id
    equal to the group's is no personal chat (T-062: the owner copy never reaches the group; the post is NO_CHAT)."""

    def __init__(self, router):
        self.router = router

    def __getitem__(self, key):
        if key == "personal":
            return "" if self.router._personal_is_group() else self.router.personal_chat_id
        if key == "group":
            return self.router.group_chat_id
        raise KeyError(key)

    def __iter__(self):
        return iter(("personal", "group"))

    def __len__(self):
        return 2


class Runtime:
    def __init__(
        self,
        cfg: Settings,
        *,
        adapter: BrokerAdapter | None = None,
        adapter_factory: Callable[[], BrokerAdapter] | None = None,
        feed_factory: FeedFactory | None = None,
        recorder_factory: RecorderFactory | None = None,
        telegram: Any = None,
        jobs: Jobs | None = None,
        is_trading_day: Callable[[date], bool] = cal.is_trading_day,
        now: Callable[[], datetime] = now_ist,
        wall_ms: Callable[[], int] = lambda: time.time_ns() // 1_000_000,
        scheduler: Any = None,
        router_kwargs: Mapping[str, Any] | None = None,
        warmup_kwargs: Mapping[str, Any] | None = None,
        reload_settings: Callable[[], Settings] | None = None,
        v1r: Any = None,
        scanners_089: bool = False,
        session_manager: Any = None,
        watermark_ledger_path: Path | None = None,
        history_config: Any = None,
    ) -> None:
        from gammad.alerts.router import AlertRouter

        self.cfg = cfg
        self.scanners_089 = scanners_089
        self.s089 = None
        self.v1r = v1r
        self.v1desk = V1Desk(self, v1r) if v1r is not None and not scanners_089 else None
        self.session_manager = session_manager
        self.watermark_ledger_path = watermark_ledger_path or (
            cfg.paths.data_dir / "watermark.db" if session_manager is not None else None
        )
        self._history_config = history_config
        self.version = __version__
        self.spec_version = cfg.spec_version
        self.db_path: Path = cfg.paths.db_path
        self.logs_dir: Path = cfg.paths.logs_dir
        self.data_dir: Path = cfg.paths.data_dir  # X1: the tray supervisor's control files (/api/service/stop)
        self.stop_file: Path = cfg.paths.data_dir / STOP_FILE_NAME
        # X1/R1: the registry-resolved broker (None = client without BROKER, or a broken entry: scanner idle)
        self.broker_problem: str | None = None
        self._broker_entry: BrokerEntry | None = self._resolve_broker(cfg)
        self._adapter = adapter
        self._adapter_factory = adapter_factory
        self._feed_factory = feed_factory or self._registry_feed
        self._recorder_factory = recorder_factory or _default_recorder_factory
        self._is_trading_day = is_trading_day
        self._now = now
        self._wall_ms = wall_ms
        self._reload_settings = reload_settings
        self._warmup_kwargs = dict(warmup_kwargs or {})
        self.jobs = jobs or _default_jobs(self)
        self.scheduler = scheduler
        self.slots = plan_slots(cfg.schedule, v1r, self._now().date())
        # T-277 (design C3 / C4): the refresh marks ("HH:MM"), the marks followed by a group additions post, and how long
        # the 09:20 slot waits for a running job (the 09:10 chain on a slow broker) before it gives up to the retry
        self.refresh_marks: tuple[str, ...] = cfg.schedule.refresh.marks()
        if v1r is not None:
            self.refresh_marks = v1r_refresh_marks(v1r, self.refresh_marks)
        self.group_additions: frozenset[str] = frozenset(cfg.schedule.group_additions)
        if v1r is not None:
            self.group_additions = frozenset(next((m for m in self.refresh_marks if m >= t), t)
                                            for t in cfg.schedule.group_additions)
        self.wait_0920_s = WAIT_0920_S
        for s in self.slots:
            if not s.scheduled:
                log.info("schedule: build_watchlists %s is a refresh mark: the refresh runs it (no separate %s job)",
                         s.at, s.job_id)  # fmt: skip

        self.stop_event = asyncio.Event()
        self.hub = EventHub()
        self.conn = db.connect(self.db_path)
        db.migrate(self.conn)
        if scanners_089:
            from dataclasses import asdict

            from gammad.store import sqlite_caps

            log.info("sqlite caps %s", asdict(sqlite_caps.probe()))
        self.telegram = telegram
        rkw: dict[str, Any] = {
            "group_chat_id": cfg.env.telegram_group_id,
            "personal_chat_id": cfg.env.telegram_personal_chat_id,
            "spec_version": cfg.spec_version,
            "group_rate_per_min": cfg.alerts.group_rate_per_min,
            "per_chat_rate_per_sec": cfg.alerts.per_chat_rate_per_sec,
            "hold": cfg.hold,
            "v1b": cfg.v1b,  # S1.5.0: V1 B's tier and the V1 PLUS component read the live V1BCfg (D161(4))
            # W1: the T-062 guard is enforced only when passed; a group migration is persisted to this .env
            "telegram_guard": cfg.telegram,
            "env_path": cfg.paths.root / ".env",
            # 0.8.9 T-419: the Still Watching switch + card config (calls_tag / eve_exit_hm), read by the router
            "still_watching": getattr(cfg, "still_watching", None),
        }
        rkw.update(router_kwargs or {})
        self.router = AlertRouter(self.conn, telegram, dashboard_push=self._push_from_router, **rkw)
        # Z1(1): the trade tracker shares the connection; the router awaits its hook after every entry
        self.tracker = TradeTracker(
            self.conn, self.router, spec_version=cfg.spec_version, push=self._push_trade, now=now, wall_ms=wall_ms,
            gates_cfg=cfg.gates,  # W15: the tracker tiers an E1 with the same GatesCfg as the router
            # 0.8.9 T-419: Still Watching trades (switch + X9A trail + eve exit); a Settings without the block -> on
            still_watching=getattr(cfg, "still_watching", None) or StillWatchingCfg(),
        )
        self.tracker.on_sw_open = self._sw_on_trade_open
        self.router.trade_hook = self.tracker.trade_hook
        self.tracker.load(now())
        # W16 S2 (T-132, B4): the 09:00 readiness check reads the CURRENT settings through zero-argument callables
        self.readiness = ReadinessJob(
            token_status=self.token_status, reload_env=self._reload_env, telegram=lambda: self.telegram,
            group_id=lambda: self.cfg.env.telegram_group_id, topic_id=lambda: self.cfg.env.telegram_topic_id,
            adapter_source=self.adapter, data_dir=self.data_dir, router=self.router, clock=self._now,
            lookahead=_readiness_lookahead(),
        )  # fmt: skip
        self.params = EngineParams.from_config(cfg.watcher)
        # S1.2.0 (T-061/MARUTI D73, ruled D74): each contract's engine gets the params of its hold style (list_name -> gates.hold_for),
        # cfg.hold_overrides applied over `watcher`; an unknown list -> the base params + a warning.
        self.hold_params = HoldParams(self.params, cfg.hold, cfg.hold_overrides)
        self.signal_tf = cfg.watcher.signal_tf_min
        self.tfs = tuple(sorted({cfg.watcher.base_tf_min, cfg.watcher.signal_tf_min, cfg.watcher.display_tf_min}))

        # live day state (one trade date at a time)
        self.trade_date: date | None = None
        self.closed_date: date | None = None
        # W15 (PLAN-W15 §5): one EnginePool per scanner. V1 = self.pool, client rules in BOTH editions (§1(8));
        # the owner's V2 shadow pool gets the same bars, OI and ticks and reaches the router as `scanner=`.
        self.scanners: tuple[ScannerSpec, ...] = build_scanners(cfg, owner=_edition.is_owner())
        self.pool = self._make_pool(self.scanners[0])
        self.shadow_pools: dict[str, EnginePool] = {s.name: self._make_pool(s) for s in self.scanners[1:]}
        # 0.8.9 T-419 "Still Watching" (spec 4.8; D471, D476 (1), D480), BOTH editions: a CARRY row's V1 engine is a
        # CarryEngine (load_day primes it with watchlist.oi_peak_prior); its E2 is an entry only through _sw_gate (the
        # F&O ban list of the day, ST-PREV, 15:20, one entry per option). sw_ban = today's ban list (None = UNKNOWN);
        # sw_regimes = ST-PREV per underlying for sw_regime_day; sw_any3_pool = the OWNER's CARRY-ANY3 live shadow.
        self.sw: SwCfg = still_watching_cfg(cfg)
        self.sw_ban: Any = None
        self.sw_regimes: dict[int, int] = {}
        self.sw_regime_day: date | None = None
        self._sw_regime_pending: set[int] = set()
        self._sw_entered: set[tuple[int, str]] = set()
        self._sw_h60_done: tuple[date | None, set[int]] = (None, set())
        self._sw_ban_tried: date | None = None  # the day of the one start-time ban fetch (_sw_day_inputs)
        # T-419 (D480 (3)): the 1-hour zone books of the carried / Still Watching underlyings, BOTH editions (gammad.core:
        # the client package ships no gammad/v2scan): the shipped seed + the topped-up 60-min store + today's closed hours
        # (_sw_h1_feed). The stop zone of a Still Watching entry is picked on them (_sw_levels). _sw_bars_seeded = the
        # underlyings whose 5-min bars (previous session -> now) the tracker got for its ATR / extreme (sw_seed_bars).
        self.sw_books: dict[int, Any] = {}
        # CX-122 review finding 1: the V1 rebuild's own 1-hour books (raw broker 60-min, the hours the desk stores in
        # the entry snapshot and the restart re-walk replays); never written into sw_books (D480 (3) seed + store)
        self.v1r_books: dict[int, Any] = {}
        self._sw_books_pending: set[int] = set()
        self._sw_bars_seeded: tuple[date | None, set[int]] = (None, set())
        self.sw_any3_pool: EnginePool | None = self._make_any3_pool()
        self.grader = self._make_grader(cfg)  # owner only; None when gammad/scan is not shipped (client ZIP)
        # 0.8.2 (C.6 / C.7): the owner extension; None in the client edition, without gammad/scan, or disabled
        self.owner_ext: Any = self._make_owner_ext(cfg)
        self.carry = OiCarry()
        self.builders: dict[int, BarBuilder] = {}
        self.series: dict[int, ContractBars] = {}
        self._peak_memo: dict[int, tuple[tuple[Any, ...], float | None]] = {}  # T-585: _bar_close_peak
        self.meta: dict[int, ContractMeta] = {}
        self.rows: dict[int, dict[str, Any]] = {}
        self.underlyings: set[int] = set()
        # S1.3.0 direction gate input: per underlying, a 5-min BarBuilder fed by its QUOTE ticks and today's
        # closed bars (Candle: ts_ms = bar open), seeded from adapter.intraday on a mid-day start
        self.und_builders: dict[int, BarBuilder] = {}
        self.und_bars: dict[int, list[Candle]] = {}
        # T-086: handed to the recorder when it starts; T-102: with the engine inputs the file had recorded for it
        self._resumed_bars: list[tuple[int, Any, tuple[float, float] | None]] = []
        self.subscribed: dict[tuple[str, int], str] = {}
        # 0.8.2 (B4): every security id V1 subscribed today (lists + open trades); an owner-claimed tick of one of
        # them still takes the whole V1 path, exactly as in 0.8.1
        self._v1_sids: set[int] = set()
        self.last_ltp: dict[int, float] = {}
        self.last_tick_ms: int | None = None
        self._tick_pushed: dict[int, int] = {}
        # D98(5): security_id -> ts_ms of its LATEST VISIBLE E1 today (router pushes + load_day's DB read after a
        # restart); the client's DONE -> ENTERED evidence in live_row, which also needs that E1 to belong to the
        # engine's CURRENT cycle (FIX2 F2; no DB read there: it runs on the API's worker thread)
        self._entered_today: dict[int, int] = {}
        # S1.7.0 (T-267 hunk A, PLAN-T267 + M1 / M2 / M8 / M12 / M13 / M14): the V2 Scanner's state.
        # h1_books: underlying -> its 1h zone book (seed restored + the H60 store after the seed's as_of + live hours),
        # kept across days (the 16:05 reconcile replaces it); h1_open_states: underlying -> the book's snapshot at the
        # session open (the 16:05 reconcile restores it); v2_regimes: today's daily Supertrend per underlying (M14).
        self.h1_books: dict[int, h1zones.H1ZoneBook] = {}
        self.h1_open_states: dict[int, dict[str, Any]] = {}
        self._h1_seed: Any = _SEED_UNSET
        self._h1_pending: set[int] = set()  # underlyings whose book is being opened (worker thread)
        self._h1_wait: set[int] = set()  # underlyings whose mid-day 5-min seed is in flight: no hour is built yet
        self.v2_regimes: dict[int, Regime] = {}
        self.v2_regime_day: date | None = None
        self._run_started: dict[str, int | None] = {}  # runs.run_id -> started_ist ms (A1 listing time)
        self._v2t_sleep: Callable[[float], Awaitable[Any]] = asyncio.sleep  # M8 delay; tests replace it

        # feed / recorder
        self.feed: FeedLike | None = None
        self.tick_queue: asyncio.Queue[Tick] | None = None
        self._feed_task: asyncio.Task[None] | None = None
        self._feed_disabled: str | None = None
        # 0.8.5 A1 (AUDIT-BROKERS-085 D1): instruments the live feed refused today, (segment, security_id) ->
        # {reason, since_ms, name}; retried at the next load_day (a list refresh), never in a loop. The rows stay
        # listed (/api/watchlists no_feed). A failed start leaves self.feed None, its reason in status, and is retried
        # by the 2 s supervisor pass no sooner than FEED_START_RETRY_S later.
        self.feed_refused: dict[tuple[str, int], dict[str, Any]] = {}
        self._feed_refused_day: date | None = None
        self._feed_refused_told: date | None = None
        self._feed_start_error: str | None = None
        self._feed_start_failed_ms: int | None = None
        self._feed_start_told: date | None = None
        self.feed_start_retry_s = 60.0
        self.recorder: RecorderLike | None = None
        self._recorder_stop: asyncio.Event | None = None
        self._recorder_task: asyncio.Task[None] | None = None
        # T-284 (1): ONE personal line when live prices go silent in market hours, ONE when they return (in memory:
        # a restart re-arms; the service's own start line covers a restart)
        self.feed_alarm = FeedAlarm(FEED_SILENT_S)
        # T-284 (3): failed parquet files / sqlite batches of recorders already stopped today (_stop_recorder folds a
        # recorder in before dropping it), so the 15:35 close does not zero the count before the next day
        self.recorder_errors_today: dict[str, Any] = {
            "day": None, "parquet_failed": 0, "db_failed": 0, "last_failed_ms": None, "stale_skipped": 0,
        }  # fmt: skip
        self._stale_guarded_day: str | None = None
        self._stale_guarded = 0
        # perf B3: runs_today() cache = (job_state fingerprint, wall ms read, today, rows); see runs_today
        self._runs_cache: tuple[Any, int, str, list[dict[str, Any]]] | None = None

        # jobs / token / metrics
        self._job_lock = asyncio.Lock()
        # T-107: ensure_day / load_day / close_day (subscribe + warm-up + resume, or the day's reset) never interleave
        # between the day pass and a job; only that sequence is held -- never a job's build, never the feed start
        self._day_lock = asyncio.Lock()
        self.job_state: dict[str, JobState] = {s.job_id: JobState(s.name) for s in self.slots if s.scheduled}
        if self.refresh_marks:  # T-277: ONE row for every mark (last mark, its status, next mark), not one per mark
            self.job_state[REFRESH_JOB] = JobState(self._refresh_name())
        self._close_day_at = v1_hours.day_mark_at(self._now().date())
        self.job_state[CLOSE_DAY_JOB] = JobState(f"{self._close_day_at:%H:%M} close_day")
        # T-275 (1): keyed by the scheduler job id, so jobs_view finds next_run; AM6: only with the V2T pool (owner)
        for at in cfg.schedule.h60_sync if _runs_v2t(self) else ():
            hh, mm = _hm(at)
            self.job_state[H60_SYNC_JOB.format(hm=f"{hh:02d}{mm:02d}")] = JobState(f"{hh:02d}:{mm:02d} h60_sync")
        self.latencies: deque[int] = deque(maxlen=LATENCY_WINDOW)
        self.last_latency_ms: int | None = None
        self.token_health_hook: Callable[[], Mapping[str, Any]] | None = None
        self.token_updater: Callable[[str], Any] | None = None
        self.token_login: Callable[[Mapping[str, str]], Any] | None = None  # X1: TokenWatcher.attach sets both
        self.token_view: Callable[[], Mapping[str, Any]] | None = None
        self._token_watchers: list[Any] = []
        self._token: TokenHealth | None = None
        # 0.8.3 T-229 (2), D230: the broker hint of the last job that hit a BrokerEntitlementError (/api/status
        # token.problem); cleared by the next ok / partial levels run and by apply_credentials (a new login)
        self.data_problem: str | None = None
        self._refusals_sent: set[tuple[date, str, str]] = set()  # (day, broker, native): one TOKEN line each (L4)
        self.stats = Stats()
        self._tasks: set[asyncio.Task[Any]] = set()
        self._daemons: set[asyncio.Task[Any]] = set()
        self._day_task: asyncio.Task[Any] | None = None  # T-060: the supervise loop's day pass (warm-up included)
        self._warned: set[str] = set()
        # W14 H5: per day, who holds the 09:20 intent: "sched" | "catchup" (queued or running) | "done" (succeeded)
        self._claims_0920: dict[date, str] = {}
        self._retry_0920_days: set[date] = set()  # T-213: the days a failed 09:20 already armed its one retry
        self.retry_0920_after_s = RETRY_0920_AFTER_S
        # T-310 (owner ruling D278(5)): V1 BEFORE THE 09:20 LIST POST = HOLD (v1_hold). The days whose hold is released,
        # the days whose 09:20 post + release task is in flight, the days the restart pass tracked, and per held
        # signal (security_id -> {signal ts_ms: lowest LTP since}) the price its release checks against the stop
        self._hold_released: set[date] = set()
        self._hold_posting: set[date] = set()
        self._hold_seen: set[date] = set()
        self._held_lows: dict[int, dict[int, float]] = {}
        # T-310 review (B7): per day, the release tasks spawned / running (M2: from the spawn to the last row's stamp a
        # new V1 signal is HELD and released in time order behind the earlier ones), and the days whose earlier days'
        # still-pending rows were stamped expired (L6)
        self._hold_releasing: dict[date, int] = {}
        self._hold_expired: set[date] = set()
        self.router.v1_hold = self.v1_hold
        self.router.on_released = self._grade_released  # M1: a held row is graded at its release, with its tier
        # T-211 (D189(2)): the newest delivery-check verdict (owner; /api/status.delivery), read once here and replaced
        # by every run: status() is polled on a worker thread, so it never reads the store for it
        self.delivery_last: dict[str, Any] | None = self._load_delivery_last()
        if scanners_089:
            self._init_089()

    # ---------------------------------------------------------------- small accessors
    @property
    def alerts_paused(self) -> bool:
        return bool(self.router.paused)

    def today(self) -> date:
        return self._now().date()

    # ---------------------------------------------------------------- broker (registry, W12)
    def _resolve_broker(self, cfg: Settings) -> BrokerEntry | None:
        """`.env` BROKER -> registry entry (owner + absent -> dhan). None + ONE error line when the registry
        refuses (client without BROKER, unknown / broken broker): the API still comes up to say so."""
        from gammad.broker import registry

        broker_name = cfg.env.broker or None
        if not broker_name and getattr(self, "session_manager", None) is not None:
            active = self.session_manager.active_account()
            if active is not None:
                broker_name = active.identity.broker
        try:
            return registry.resolve(broker_name)
        except LookupError as exc:  # RegistryError
            self.broker_problem = str(exc)
            log.error("broker: %s -- no feed and no scanner until it is fixed", exc)
            return None

    @property
    def broker_entry(self) -> BrokerEntry | None:
        if getattr(self, "session_manager", None) is not None:
            active = self.session_manager.active_account()
            if active is not None:
                from gammad.broker import registry

                try:
                    return registry.get(active.identity.broker)
                except Exception:
                    pass
        return self._broker_entry

    @broker_entry.setter
    def broker_entry(self, entry: BrokerEntry | None) -> None:
        self._broker_entry = entry

    def broker_creds(self) -> dict[str, str]:
        """{env key: value} of the broker entry's keys (D54-resolved settings). For Dhan the EnvCfg fields
        win: they are what apply_credentials / the legacy token path update. Values are secrets."""
        entry = self.broker_entry
        if getattr(self, "session_manager", None) is not None:
            active = self.session_manager.active_account()
            if active is not None:
                from gammad.broker import registry

                creds = dict(active.credentials or {})
                if entry is not None:
                    merged = {**dict(self.cfg.env.values), **creds}
                    return registry.creds_from_env(entry, merged)
                return creds
        if entry is None:
            return {}
        from gammad.broker import registry

        vals = dict(self.cfg.env.values)
        if entry.name == "dhan":
            if self.cfg.env.dhan_client_id:
                vals["DHAN_CLIENT_ID"] = self.cfg.env.dhan_client_id
            if self.cfg.env.dhan_access_token:
                vals["DHAN_ACCESS_TOKEN"] = self.cfg.env.dhan_access_token
        return registry.creds_from_env(entry, vals)

    @property
    def scanner(self) -> ScannerMode | None:
        """capabilities.assess of the broker's declaration (pure; None without a broker)."""
        return assess(self.broker_entry.caps) if self.broker_entry is not None else None

    @property
    def engines_allowed(self) -> bool:
        """False only for a BLOCKED broker: no engines, no signals; levels, lists and ticks still run."""
        sc = self.scanner
        return sc is None or sc.engines_allowed

    def _scanner_flag(self, name: str) -> Any:
        sc = self.scanner
        return None if sc is None else sc.flags.get(name)

    def _note_scanner(self) -> None:
        """load_day / start_feed: say ONCE when the broker runs the scanner BLOCKED or DEGRADED."""
        sc = self.scanner
        if sc is not None and sc.mode != "FULL":
            self._warn_once(f"scanner-{sc.mode}", "scanner %s (%s): %s", sc.mode,
                            self.broker_entry.name if self.broker_entry else "?", " | ".join(sc.reasons))

    def _feed_problem(self) -> str | None:
        entry = self.broker_entry
        if entry is None:
            return self.broker_problem or "no broker configured (BROKER)"
        from gammad.broker import registry

        missing = registry.missing_credentials(entry, self.broker_creds())
        return f"{' / '.join(missing)} not set" if missing else None

    def _registry_feed(
        self, queue: asyncio.Queue[Tick], on_auth_error: Callable[[int, str], None], cfg: Settings
    ) -> FeedLike:
        """R1/R10: every broker's feed gets the runtime's adapter (Upstox / Kite index + equity routing, the
        identity guard at subscribe, and a poll feed's shared rate buckets need it; groww / nuvama raise
        without it)."""
        del cfg  # the creds come from broker_creds() (the settings the runtime holds now)
        entry = self.broker_entry
        creds = self.broker_creds()
        active = None
        if getattr(self, "session_manager", None) is not None:
            active = self.session_manager.active_account()

        if entry is None:
            raise BrokerAuthError(self.broker_problem or "no broker configured (BROKER)")

        # R1.9: Attach durable watermark queue gate if session_manager & ledger configured
        feed_queue = queue
        if active is not None and getattr(self, "watermark_ledger_path", None) is not None:
            from gammad.broker.durable_watermark import WatermarkStore, wrap_feed_queue

            try:
                store = WatermarkStore(
                    self.watermark_ledger_path,
                    broker=active.identity.broker,
                    account_scope=active.identity.account_id,
                )
                feed_queue = wrap_feed_queue(queue, store)
            except Exception as exc:  # noqa: BLE001
                log.warning("durable watermark unavailable: %s; falling back to direct queue", exc)

        feed: FeedLike = entry.make_feed(creds, feed_queue, on_auth_error, adapter=self.adapter())
        return feed

    def adapter(self) -> BrokerAdapter | None:
        if getattr(self, "session_manager", None) is not None:
            active = self.session_manager.active_account()
            if active is not None and active.adapter is not None:
                return active.adapter
        if self._adapter is None:
            factory = self._adapter_factory
            if factory is None:
                entry = self.broker_entry
                if entry is None:
                    self._warn_once("adapter", "broker adapter unavailable: %s", self.broker_problem)
                    return None

                def factory() -> BrokerAdapter:
                    adapter: BrokerAdapter = entry.make_adapter(self.broker_creds(), self.data_dir)
                    return adapter

            try:
                self._adapter = factory()
            except BrokerAuthError as exc:
                self._warn_once("adapter", "broker adapter unavailable: %s", exc)
                return None
            except Exception as exc:  # noqa: BLE001 -- a broken BETA adapter must not take the supervisor down
                self._warn_once("adapter", "broker adapter unavailable: %s: %s", type(exc).__name__, exc)
                return None
        return self._adapter

    def history_adapter(self, provider_name: str | None = None) -> BrokerAdapter | None:
        """R1.9: Historical data adapter, decoupled from active execution broker."""
        if getattr(self, "session_manager", None) is not None:
            try:
                return self.session_manager.history_adapter(provider_name)
            except Exception:  # noqa: BLE001
                pass
        return self.adapter()

    def history_config(self, provider_name: str | None = None) -> Any:
        """R1.9: Historical data configuration, decoupled from active execution broker."""
        if getattr(self, "session_manager", None) is not None:
            try:
                return self.session_manager.history_config(provider_name)
            except Exception:  # noqa: BLE001
                pass
        return getattr(self, "_history_config", None)

    def require_adapter(self) -> BrokerAdapter:
        a = self.adapter()
        if a is None:
            name = self.broker_entry.name if self.broker_entry is not None else "no broker"
            raise BrokerAuthError(f"broker adapter unavailable ({name}: {self._feed_problem() or 'credentials'})")
        return a

    def _warn_once(self, key: str, msg: str, *args: object) -> None:
        if key not in self._warned:
            self._warned.add(key)
            log.warning(msg, *args)

    def spawn(
        self, coro: Coroutine[Any, Any, Any], name: str | None = None, *, daemon: bool = False
    ) -> asyncio.Task[Any]:
        """Keep a reference to a background task. `daemon` = runs for the service's life (loops, consumer,
        recorder); the rest (signal sends, jobs, alerts) are what `flush()` waits for."""
        task = asyncio.get_running_loop().create_task(coro, name=name)
        (self._daemons if daemon else self._tasks).add(task)
        task.add_done_callback(self._task_done)
        return task

    def _task_done(self, task: asyncio.Task[Any]) -> None:
        self._tasks.discard(task)
        self._daemons.discard(task)
        if not task.cancelled() and task.exception() is not None:
            exc = task.exception()
            log.error("task %s failed", task.get_name(), exc_info=exc)
            name = task.get_name() or ""
            if name.startswith(REPORTED_TASKS):  # T-213 (audit S5): a crashed post / summary is never silent
                self._report_failure(f"task {name} crashed: {type(exc).__name__}: {exc}")

    def _report_failure(self, text: str) -> None:
        """T-213 (D186(1), audit F1 / S5): ONE personal SYSTEM line for an automatic failure. send_system never goes to
        the group (a client with no personal chat: dashboard only); the task is not in REPORTED_TASKS (no loop)."""
        try:
            self.spawn(self.router.send_system(text[:300], "SYSTEM"), name="failure-line")
        except RuntimeError:  # no running loop: the ERROR log line stands alone
            log.warning("failure line not sent (no event loop): %s", text[:300])

    async def flush(self) -> None:
        """Wait for every transient background task spawned so far (router sends, jobs, alerts).
        A FINISHED task is dropped here, not left to its queued done-callback: on Python 3.12+ `gather` over tasks
        that are all finished completes without yielding, so the callback never ran and this loop spun forever
        (0.8.1, the 5 h gate hang; tests/test_app_wiring.py). `_task_done` still runs later and still logs."""
        while self._tasks:
            for t in [t for t in self._tasks if t.done()]:
                self._tasks.discard(t)
            if self._tasks:
                await asyncio.gather(*list(self._tasks), return_exceptions=True)

    # ---------------------------------------------------------------- market hours
    def is_trading_day(self, d: date) -> bool:
        try:
            return self._is_trading_day(d)
        except LookupError as exc:  # CalendarYearMissing: loud, never a guess
            self._warn_once(f"cal{d.year}", "calendar: %s -- treating %s as NOT a trading day", exc, d)
            return False

    def market_open(self, now: datetime | None = None) -> bool:
        now = now or self._now()
        if not self.is_trading_day(now.date()):
            return False
        hm = f"{now.hour:02d}:{now.minute:02d}"
        return SESSION_OPEN_HM <= hm < era_for(now.date()).fo_close.strftime("%H:%M")

    # ---------------------------------------------------------------- day state
    async def ensure_day(self, d: date) -> bool:
        """Make `d` the live trade date (loading its watchlist). False when `d` was already closed."""
        if self.trade_date == d:
            return True
        if self.closed_date == d:
            return False
        if self.trade_date is not None:
            await self.close_day()
        # T-058 follow-up: a NEW trade date starts with no LTP at all. The map only holds ticks of an earlier session
        # here (handle_tick is its only writer, the feed runs 09:15-15:30 and starts after this pass), and every
        # reader must see "no price yet" rather than yesterday's: the D63 spot (signal_context), the list post's
        # premium (_new_rows_by_list: a live ltp outranks the build's stored chain_ltp), manual sends, live_row.
        self.last_ltp.clear()
        self.trade_date = d
        self.closed_date = None
        if self._close_day_at != v1_hours.day_mark_at(d):
            self._schedule_close_day(d)
        self.slots = plan_slots(self.cfg.schedule, self.v1r, d)
        self._schedule_day_end(d)
        self._v2_open_day(d)
        self._v2_regime_at_start(d)
        if self.owner_ext is not None:  # 0.8.2: a fresh owner sink; its claims come in the background (never V1's wait)
            try:
                self.owner_ext.open_day(d)
                self.spawn(self.owner_ext.on_day(d), name="owner-day")
            except Exception:
                log.exception("owner extension day %s not opened", d)
        await self.load_day(d)
        return True

    async def load_day(self, d: date) -> list[int]:
        """Register today's watchlist contracts not yet known: builders, engines, meta; subscribe; warm up."""
        if self.trade_date != d:
            return []
        rows = [
            dict(r)
            for r in self.conn.execute(
                # T-063/T-107: a row the 09:20 build retired (retired_run_id set) is not watched. D429: a CLIENT never
                # watches an EXPIRY row (a store built before 0.8.8): no engine, so no new entry from it; an OPEN trade
                # on one keeps its ticks through tracker.open_instruments() and runs to its exit
                "SELECT * FROM watchlist WHERE trade_date = ? AND retired_run_id IS NULL" + _edition.v1_lists_sql()
                + " ORDER BY list_name, expiry_date, security_id",
                (d.isoformat(),),
            ).fetchall()
        ]
        self._note_scanner()
        self._drop_retired(d, rows)
        for sid_e1, ts_e1 in self._visible_e1_ts(d).items():  # D98(5): a restart keeps today's ENTERED evidence
            self._note_visible_e1(sid_e1, ts_e1)
        new: list[int] = []
        new_unds: list[int] = []
        v2t_new: list[int] = []  # T-267: contracts whose V2T engine was created now (they wait for the M8 bars)
        for r in rows:
            sid = int(r["security_id"])
            und = int(r["underlying_security_id"])
            self.underlyings.add(und)
            if und not in self.und_builders:
                self.und_builders[und] = BarBuilder(und, d, (self.signal_tf,), session_close=era_for(d).fo_close)
                self.und_bars[und] = []
                new_unds.append(und)
            if sid in self.meta:
                if self.rows[sid].get("tag") == "CARRY" and r.get("tag") != "CARRY":
                    # S1.5.0 (D161(6)): a carried contract RE-LISTED by a later build (13:15) is a listed contract
                    # again: refresh meta so its signals are no longer capped as carried. The engine keeps the
                    # params it was created with (hold style by the first list); the list change is logged.
                    if self.rows[sid]["list_name"] != r["list_name"]:
                        log.info("sec %s re-listed %s -> %s (was CARRY)", sid, self.rows[sid]["list_name"],
                                 r["list_name"])
                    self.meta[sid] = meta_from_row(r)
                    self.rows[sid] = r
                    self.pool.clear_carry(sid)  # T-419: a re-listed contract runs the listed V1 rule from now on
                    if self.v1r is not None:
                        self.pool.discard(sid)
                    if self.sw_any3_pool is not None:
                        self.sw_any3_pool.discard(sid)
                    self._v2t_register(sid, r, d, v2t_new)
                    continue
                if self.rows[sid]["list_name"] != r["list_name"]:
                    self._warn_once(f"dup{sid}", "sec %s is in %s and %s; keeping the first", sid,
                                    self.rows[sid]["list_name"], r["list_name"])
                continue
            self.meta[sid] = meta_from_row(r)
            self.rows[sid] = r
            self.series[sid] = ContractBars(sid)
            self.builders[sid] = BarBuilder(sid, d, self.tfs, session_close=era_for(d).fo_close)
            if self.engines_allowed and (self.v1r is None or r.get("tag") == "CARRY"):
                self._sw_prime(sid, r)  # T-419: BEFORE the engine exists, so a CARRY row gets its CarryEngine
                self.pool.get_or_create(sid, str(r["list_name"]))  # S1.2.0: params by the list's hold style
            self._v2t_register(sid, r, d, v2t_new)
            new.append(sid)
        # T-267 (M11): an open V2T trade's underlying keeps its 5-min bars on a carry day, listed today or not (its
        # option + underlying stay subscribed through tracker.open_instruments())
        # T-419: so does an open Still Watching trade's (its exits walk the stock's 5-min closes)
        for und in sorted(self.tracker.zone_unds() | self.tracker.sw_unds() | self.tracker.v1r_unds()):
            if und not in self.und_builders:
                self.und_builders[und] = BarBuilder(und, d, (self.signal_tf,), session_close=era_for(d).fo_close)
                self.und_bars[und] = []
                new_unds.append(und)
        if self.s089 is None:
            await self._subscribe(d)
        if v2t_new:  # M8: subscribed now -> the slots before the subscription slot's end come from the broker
            cutoff = opening_cutoff_ms(self._wall_ms(), d)
            self.spawn(self._v2t_opening(d, v2t_new, cutoff), name="v2t-opening", daemon=True)
        if new:
            await self._warmup(d, new)
        if new_unds:
            self._h1_wait.update(new_unds)  # no hour is built from a half-seeded day
            self.spawn(self._seed_underlyings(d, new_unds), name="underlying-seed")
        self._h1_ensure(d, {int(r["underlying_security_id"]) for r in rows} | self.tracker.zone_unds())
        self._sw_day_inputs(d)
        sw_unds = self.tracker.sw_unds()  # T-419: re-hydrate the open Still Watching trades (ATR + extreme), once a day
        if sw_unds:
            self.spawn(self._sw_seed_bars(d, sorted(sw_unds)), name="sw-seed-bars")
        if self.v1desk is not None:
            await self._v1r_prepare(d)
        if self.s089 is not None:
            await self._prepare_089(d, rows)
        return new

    async def _v1r_prepare(self, d):
        """Broker history is fetched off-loop, then only closed bars enter the desk/re-walk."""
        from gammad.core.h1zones import H1ZoneBook
        from gammad.v1.carry_break import bar_array
        from gammad.watch.types import Bar

        desk = self.v1desk
        adapter = self.adapter()
        if adapter is None:
            return
        now = self._wall_ms()
        prev = self._prev_trading_day(d) or d
        if d not in desk.nifty:
            try:
                raw = await asyncio.to_thread(adapter.daily_candles, 13, "IDX_I", d-timedelta(days=100),
                                               d-timedelta(days=1), instrument="INDEX")
                desk.nifty[d] = tuple((c.ts_ms, c.close) for c in raw
                                      if datetime.fromtimestamp(c.ts_ms/1000, IST).date() < d)
            except Exception:  # noqa: BLE001 -- no index history fails the selected filter closed
                desk.nifty[d] = ()
                desk.counts["nifty_unknown"] += 1
                log.warning("rebuild %s: nifty_unknown", d)
        waiting = [dict(r) for r in self.tracker.open_trades() if r["state"] in V1R_STATES]
        for trade in waiting:
            sid = int(trade["security_id"])
            if sid not in self.rows:
                rec = self.conn.execute("SELECT payload_json FROM signals WHERE id=?", (trade["entry_signal_id"],)).fetchone()
                payload = json.loads(rec[0])
                self.rows[sid] = {**trade, "symbol": payload["symbol"], "strike": payload["strike"],
                                  "trade_date": d.isoformat(), "tag": "CARRY"}
                self.meta[sid] = meta_from_row(self.rows[sid])
                self.series[sid] = ContractBars(sid)
                self.builders[sid] = BarBuilder(sid, d, self.tfs, session_close=era_for(d).fo_close)
        for sid, row in list(self.rows.items()):
            rewalk = [r for r in waiting if r["security_id"] == sid and r["id"] in self.tracker._v1r_wait]
            und = int(row["underlying_security_id"])
            v1r_sync_hours(desk, self, d, und)
            if (d, sid) in desk.ready and not rewalk:
                continue
            start = min([prev, *(date.fromisoformat(r["entry_date"]) for r in rewalk)])
            try:
                stock = await asyncio.to_thread(adapter.intraday, und, "NSE_EQ", 5, start, d,
                                                 oi=False, instrument="EQUITY")
                option = await asyncio.to_thread(adapter.intraday, sid, "NSE_FNO", 5, start, d, oi=True)
                hours = await asyncio.to_thread(adapter.intraday, und, "NSE_EQ", 60,
                                                 start-timedelta(days=180), d, oi=False, instrument="EQUITY")
                if self.trade_date != d:
                    return
                if sid not in self.last_ltp and hasattr(adapter, "ltp"):
                    prices = await asyncio.to_thread(adapter.ltp, [sid], "NSE_FNO")
                    if prices.get(sid) is not None:
                        self.last_ltp[sid] = prices[sid]
                stocks = tuple(bar_array(b) for b in stock if b.ts_ms+300_000 <= now)
                opts = tuple(bar_array(b) for b in option if b.ts_ms+300_000 <= now)
                if any(b[6] is None for b in opts):
                    raise ValueError("option OI missing")
                h1 = []
                for h in hours:
                    cash_close = v1_hours.cash_close_ms(datetime.fromtimestamp(h.ts_ms/1000, IST).date())
                    end = min(h.ts_ms + 3_600_000, cash_close)
                    if h.ts_ms < cash_close and end <= now:
                        h1.append((h.ts_ms, h.open, h.high, h.low, h.close, end, "broker"))
                h1 = tuple(h1)
                self.v1r_books[und] = H1ZoneBook(h1, self._zone_params())
                desk.h1[d, und] = h1
                desk.stock_seed[d, und] = tuple(b for b in stocks if datetime.fromtimestamp(b[0]/1000, IST).date() == prev)
                today = [b for b in stocks if datetime.fromtimestamp(b[0]/1000, IST).date() == d]
                merged = {b.ts_ms: b for b in self.und_bars.get(und, ())}
                merged.update({b[0]: Candle(*b) for b in today})
                self.und_bars[und] = [merged[k] for k in sorted(merged)]
                if und not in self.und_builders:
                    self.und_builders[und] = BarBuilder(und, d, (self.signal_tf,), session_close=era_for(d).fo_close)
                merged_options = {b.ts_ms: b for b in self.series[sid].closed()}
                merged_options.update({b[0]: Bar(*b) for b in opts})
                rebuilt = ContractBars(sid)
                rebuilt.seed([merged_options[t] for t in sorted(merged_options)
                              if datetime.fromtimestamp(t/1000, IST).date() < d])
                for t in sorted(merged_options):
                    if datetime.fromtimestamp(t/1000, IST).date() == d:
                        rebuilt.append(merged_options[t])
                self.series[sid] = rebuilt
                if opts:
                    self.carry.prime(sid, opts[-1][6])
                for trade in rewalk:
                    daily_raw = await asyncio.to_thread(adapter.daily_candles, und, "NSE_EQ", start, d,
                                                         instrument="EQUITY")
                    daily = [(int(datetime.combine(datetime.fromtimestamp(c.ts_ms/1000, IST).date(),
                               datetime.strptime("16:05", "%H:%M").replace(tzinfo=IST).time(), IST).timestamp()*1000), c.close)
                             for c in daily_raw]
                    self.tracker.v1r_rewalk(trade["id"], opts, stocks, h1, [(t, c) for t, c in daily if t <= now])
                desk.ready.add((d, sid))
                desk.decide(sid, self.series[sid].closed(), catch_up=True)
            except Exception:
                log.exception("rebuild history unavailable for %s; entry/restart waits", sid)
        await self._subscribe(d)

    def _drop_retired(self, d: date, live_rows: list[dict[str, Any]]) -> list[int]:
        """T-063/T-107: load_day only ADDS, so a pre-market row the 09:20 build retired (retired_run_id set) would keep
        its builder, engine and meta from the day roll. Drop every loaded contract with no LIVE row today (a contract
        live in another list stays), and an underlying no live row needs. Subscriptions: no feed client can
        unsubscribe (FeedLike: start / add_instruments / stop), so with the feed up the instrument keeps streaming but
        closes no bar and fires nothing (no builder, no engine, no meta); before the feed starts it is dropped from
        the set start_feed subscribes. An open trade's contract / underlying keeps its subscription (Z1(5))."""
        # CX-122 review finding 2: an open V1R trade's contract keeps its builder / bars (a swing trade is unlisted on
        # later days) and its underlying keeps its 5-min bars, as T-267 / T-419 do for V2T / Still Watching
        # Audit P2B-APP-01 (D689): so does every contract the 0.8.9 hub still tracks (an open OWN / GROUP ladder carried
        # from an earlier day, a pending fill, today's s089 rows incl. retired CARRY rows and the owner's V2SQ rows):
        # dropped, _prepare_089 re-created its builder EMPTY (a mid-bar open, no closed bars, atr14 None)
        s089_ids, s089_unds = self._089_tracked(d)
        live = {int(r["security_id"]) for r in live_rows} | self.tracker.v1r_ids | s089_ids
        live_unds = {int(r["underlying_security_id"]) for r in live_rows} | s089_unds
        gone = sorted(sid for sid in self.meta if sid not in live)
        if not gone:
            return []
        for sid in gone:
            self.meta.pop(sid, None)
            self.rows.pop(sid, None)
            self.series.pop(sid, None)
            self.builders.pop(sid, None)
            self._entered_today.pop(sid, None)
            self.pool.discard(sid)
            if self.sw_any3_pool is not None:  # T-419
                self.sw_any3_pool.discard(sid)
        # T-267 (M11): an open V2T trade's underlying keeps its bars; T-419: an open Still Watching trade's too
        zone_unds = self.tracker.zone_unds() | self.tracker.sw_unds() | self.tracker.v1r_unds()
        stale_unds = sorted(u for u in self.underlyings if u not in live_unds and u not in zone_unds)
        for und in stale_unds:
            self.underlyings.discard(und)
            self.und_builders.pop(und, None)
            self.und_bars.pop(und, None)
        if self.feed is None:
            keep = {(seg, int(sid)) for seg, sid, _m in self.tracker.open_instruments()}
            for key in [(subs.OPTION_SEGMENT, s) for s in gone] + [(subs.UNDERLYING_SEGMENT, u) for u in stale_unds]:
                if key not in keep:
                    self.subscribed.pop(key, None)
        log.info("watchlist: %d retired contract(s) no longer watched%s", len(gone),
                 "" if self.feed is None else " (still streamed: the feed cannot unsubscribe)")  # fmt: skip
        return gone

    def _089_tracked(self, d: date) -> tuple[set[int], set[int]]:
        """Audit P2B-APP-01: (contracts, underlyings) the 0.8.9 hub still needs bars for = what _prepare_089 claims:
        today's list rows (V1N + V2SQ), every open ladder (V2SQ ladders sit in hub.states too) and every pending fill."""
        hub = self.s089
        if hub is None:
            return set(), set()
        tracked = [*hub.list_rows(d), *(p for p in hub.states.values() if not p.closed), *hub.pending.values()]
        return {int(r.security_id) for r in tracked}, {int(r.underlying_security_id) for r in tracked}

    def _visible_e1_ts(self, d: date) -> dict[int, int]:
        """security_id -> ts_ms of its latest visible E1 on `d`, only where that E1 is the contract's latest entry
        (server.entered_e1_ts, on the loop's own connection). A contract whose later setup ended in a hidden gated
        E1 is not seeded: the restart's silent replay need not rebuild that setup's arming bar (a client records no
        E2, so a bar replayed at its own OI may not re-arm where live's closing tick did, T-091), so the arming-bar
        check alone could not tell (FIX2 F2)."""
        try:
            return entered_e1_ts(self.conn, d)
        except sqlite3.Error:
            log.exception("today's recorded entries could not be read")
            return {}

    def _note_visible_e1(self, sid: int, ts_ms: int) -> None:
        prev = self._entered_today.get(sid)
        self._entered_today[sid] = ts_ms if prev is None else max(prev, ts_ms)

    def _entered_this_cycle(self, sid: int, eng: ContractEngine) -> bool:
        """D98(5) + FIX2 F2: the client's ARMED / DONE -> ENTERED evidence. The contract's latest visible entry today
        must belong to the engine's CURRENT arming cycle: at/after the arming bar's close (the bound
        _close_cycle_already_entered uses). Since D287 (1) there is one cycle a day (no re-arm), and the client entry
        (the E2 bar close, stored as kind E1, D288) is stamped exactly at that close; the bound still guards a research
        engine that re-arms (oi_rearm_extra set), where a visible entry of setup 1 is no evidence for setup 2."""
        ts = self._entered_today.get(sid)
        if ts is None:
            return False
        if eng.armed_bar_ts is None:  # no cycle on record (DONE always keeps its arming bar): the E1 is the evidence
            return True
        return ts >= eng.armed_bar_ts + self.signal_tf * MINUTE_MS

    async def _seed_underlyings(self, d: date, unds: list[int]) -> int:
        """Mid-day start (PLAN-W12 §2 item 4): today's closed 5-min bars of each NEW underlying from
        adapter.intraday, merged under the live bars its BarBuilder closes meanwhile. Before the first bar
        end there is nothing to seed. Returns how many underlyings got bars.
        T-267: the underlying's 1h book builds no hour of today until this seed is done (success or not); then the
        hours already closed are appended at once (_h1_feed)."""
        try:
            return await self._seed_und_bars(d, unds)
        finally:
            self._h1_wait.difference_update(unds)
            if self.trade_date == d:
                for und in unds:
                    self._h1_feed(und)

    async def _seed_und_bars(self, d: date, unds: list[int]) -> int:
        span = self.signal_tf * MINUTE_MS
        open_ms, _ = session_bounds_ms(d)
        now_ms = self._wall_ms()
        if now_ms < open_ms + span or self._scanner_flag("warmup") is False:
            return 0
        adapter = self.adapter()
        if adapter is None:
            return 0
        seeded = 0
        for und in unds:
            try:
                candles = await asyncio.to_thread(
                    adapter.intraday, und, UNDERLYING_SEGMENT, self.signal_tf, d, d,
                    oi=False, instrument=UNDERLYING_INSTRUMENT,
                )  # fmt: skip
            except BrokerAuthError as exc:
                log.error("underlying seed: auth rejected (%s); the rest build live", exc)
                return seeded
            except Exception as exc:  # noqa: BLE001 -- a missing history only leaves the direction gate unknown
                log.warning("underlying seed: sec %s failed (%s: %s); its bars build live", und,
                            type(exc).__name__, exc)
                continue
            closed = {c.ts_ms: c for c in candles if open_ms <= c.ts_ms and c.ts_ms + span <= now_ms}
            if und not in self.und_bars:  # the day closed meanwhile
                return seeded
            closed.update({b.ts_ms: b for b in self.und_bars[und]})  # a bar the live feed closed wins
            self.und_bars[und] = [closed[k] for k in sorted(closed)]
            seeded += bool(closed)
        log.info("underlying seed: %d/%d underlyings have today's 5-min bars", seeded, len(unds))
        return seeded

    async def _subscribe(self, d: date) -> list[Instrument]:
        # Z1(5): today's lists + every OPEN trade's contract and underlying (a swing contract off today's list
        # keeps its ticks); first occurrence of a (segment, security_id) wins
        wanted = subs.union(subs.build_subscriptions(self.conn, d.isoformat()), self.tracker.open_instruments())
        self._v1_sids.update(int(sid) for _seg, sid, _mode in wanted)
        if self.owner_ext is not None:  # 0.8.2 (C.7(2)): the owner claims AFTER the V1 sets, so V1's mode wins
            wanted = subs.union(wanted, self.owner_ext.instruments(d))
        current = [(s, i, m) for (s, i), m in self.subscribed.items()]
        added = subs.additions(current, wanted)
        refused: dict[tuple[str, int], str] = {}
        if added and self.feed is not None:
            # 0.8.5 A1 (D1): an instrument is subscribed only AFTER the feed took it; a refusal never raises out of
            # here (load_day's warm-up, the V2 opening fetch and the build's post go on). A refused one is not in
            # self.subscribed, so the next load_day (a list refresh) offers it again.
            added, refused = await self._feed_take(added)
        for seg, sid, mode in added:
            self.subscribed[seg, sid] = mode
        if self.feed is not None:  # 0.8.5 seam: an id the app holds but the feed itself refused (start, unmapped)
            own = self._feed_own_rejected(self.subscribed)
            for key in own:
                self.subscribed.pop(key, None)
            added = [x for x in added if (x[0], int(x[1])) not in own]
            refused = {**own, **refused}
        self._note_feed_refused(d, added, refused)
        if added:
            log.info("subscriptions: +%d (total %d)", len(added), len(self.subscribed))
        return added

    async def _feed_take(self, items: list[Instrument]) -> tuple[list[Instrument], dict[tuple[str, int], str]]:
        """0.8.5 A1: add NEW `items` to the running feed. Returns (accepted, refused key -> reason); never raises
        (cancellation aside). Everything in one call when it fits the cap (or no cap is known); when the cap is short,
        the units _feed_units orders, as many as fit; a capacity refusal anyway (the feed counts differently) is
        retried ONCE with the units that fit the error's `.free` (feed_protocol D9), or, from a feed whose error
        carries no `.free`, one unit per call. Any other feed error refuses the units not yet taken."""
        from gammad.watch.feed import FeedCapacityError

        feed = self.feed
        assert feed is not None
        cap = self._feed_cap()
        on = self._feed_count()
        free = None if cap is None else cap - (on if on is not None else len(self.subscribed))
        first = items if free is None or len(items) <= free else self._feed_fit(items, max(free, 0))[0]
        accepted: list[Instrument] = []
        reason = "capacity"
        try:
            if first:
                await feed.add_instruments(first)
            accepted = list(first)
        except FeedCapacityError as cap_exc:
            free_now = getattr(cap_exc, "free", None)  # 0.8.5 seam (feed_protocol D9): the feed says what fits
            if isinstance(free_now, int) and not isinstance(free_now, bool):
                fit = self._feed_fit(items, max(free_now, 0))[0]  # the _feed_units priority order, retried ONCE
                try:
                    if fit:
                        await feed.add_instruments(fit)
                    accepted = list(fit)
                except FeedCapacityError:
                    pass  # the count moved again: the next load_day offers them
                except Exception as exc2:  # noqa: BLE001 -- a broken feed refuses the rest; never out of load_day
                    reason = f"feed error: {type(exc2).__name__}"
                items_left: list[list[Instrument]] = []
            else:  # a feed without .free: one unit per call
                items_left = self._feed_units(items)
            for unit in items_left:
                try:
                    await feed.add_instruments(unit)
                except FeedCapacityError:
                    continue  # a smaller unit further down may still fit
                except Exception as exc:  # noqa: BLE001 -- a broken feed refuses the rest; never out of load_day
                    reason = f"feed error: {type(exc).__name__}"
                    break
                accepted.extend(unit)
        except Exception as exc:  # noqa: BLE001 -- same: the feed's error is reported, the day goes on
            reason = f"feed error: {type(exc).__name__}"
        took = {(s, int(i)) for s, i, _m in accepted}
        refused = {(s, int(i)): reason for s, i, _m in items if (s, int(i)) not in took}
        if refused:
            log.warning("live feed: %d of %d new instrument(s) could not be added (%s%s); they stay listed and are "
                        "offered again at the next list load", len(refused), len(items), reason,
                        f", cap {cap}" if cap is not None and reason == "capacity" else "")  # fmt: skip
        return accepted, refused

    def _feed_units(self, items: list[Instrument]) -> list[list[Instrument]]:
        """0.8.5 A1: `items` grouped and ordered for a short cap. A unit = a contract preceded by its underlying when
        that underlying is new too (a contract without its underlying's ticks has no direction gate). Order: open
        trades' instruments first (their exits read these prices), then the other contracts in `items` order, then
        whatever is left (an underlying no new contract needs, an owner extra) in `items` order. `items` order is
        _subscribe's `wanted`: subs.build_subscriptions (contracts by security_id ascending -- the store keeps no
        build order), then the open trades, then the owner's extras."""
        by_key = {(s, int(i)): (s, int(i), m) for s, i, m in items}
        und_of = {sid: int(r["underlying_security_id"]) for sid, r in self.rows.items()}
        for t in self.tracker.open_trades():
            if t.get("underlying_security_id") is not None:
                und_of.setdefault(int(t["security_id"]), int(t["underlying_security_id"]))
        open_keys = {(s, int(i)) for s, i, _m in self.tracker.open_instruments()}
        placed: set[tuple[str, int]] = set()
        units: list[list[Instrument]] = []

        def unit_of(key: tuple[str, int]) -> None:
            if key in placed:
                return
            unit: list[Instrument] = []
            if key[0] == subs.OPTION_SEGMENT and key[1] in und_of:
                ukey = (subs.UNDERLYING_SEGMENT, und_of[key[1]])
                if ukey in by_key and ukey not in placed:
                    unit.append(by_key[ukey])
                    placed.add(ukey)
            unit.append(by_key[key])
            placed.add(key)
            units.append(unit)

        keys = list(by_key)
        for key in [k for k in keys if k in open_keys and k[0] == subs.OPTION_SEGMENT]:
            unit_of(key)
        for key in [k for k in keys if k in open_keys]:
            unit_of(key)
        for key in [k for k in keys if k[0] == subs.OPTION_SEGMENT]:
            unit_of(key)
        for key in keys:
            unit_of(key)
        return units

    def _feed_fit(self, items: list[Instrument], room: int) -> tuple[list[Instrument], list[Instrument]]:
        """(taken, left) = the _feed_units that fit `room`, in their order (a unit too big is skipped, not split)."""
        taken: list[Instrument] = []
        left: list[Instrument] = []
        for unit in self._feed_units(items):
            if len(unit) <= room - len(taken):
                taken.extend(unit)
            else:
                left.extend(unit)
        return taken, left

    def _feed_cap(self, feed: Any = None) -> int | None:
        """The live feed's instrument cap: its FeedCaps.max_instruments, Dhan's connections x per-connection, else the
        broker's declared max_feed_instruments; None when nothing declares one."""
        f = self.feed if feed is None else feed

        def _int(v: Any) -> int | None:
            return v if isinstance(v, int) and not isinstance(v, bool) and v > 0 else None

        m = _int(getattr(getattr(f, "caps", None), "max_instruments", None))
        if m is not None:
            return m
        per, conns = _int(getattr(f, "max_per_conn", None)), _int(getattr(f, "max_conns", None))
        if per is not None and conns is not None:
            return per * conns
        entry = self.broker_entry
        return _int(getattr(getattr(entry, "caps", None), "max_feed_instruments", None)) if entry is not None else None

    def _feed_count(self) -> int | None:
        """The feed's OWN count of instruments on the wire (the uniform `subscribed_ids()`, or `n_live`), None when the
        feed offers neither (status then has n_subscribed only, the app's count)."""
        f = self.feed
        if f is None:
            return None
        ids = getattr(f, "subscribed_ids", None)
        if callable(ids):
            try:
                return len(list(ids()))
            except Exception:  # a status read never fails on a feed's helper
                log.debug("feed subscribed_ids() failed", exc_info=True)
        n = getattr(f, "n_live", None)
        return n if isinstance(n, int) and not isinstance(n, bool) else None

    def _feed_name(self, key: tuple[str, int]) -> str:
        seg, sid = key
        if seg == subs.OPTION_SEGMENT and sid in self.rows:
            r = self.rows[sid]
            return f"{r.get('symbol')} {float(r.get('strike') or 0):g} {r.get('option_type')}"
        if seg == subs.UNDERLYING_SEGMENT:
            sym = next((r.get("symbol") for r in self.rows.values() if int(r["underlying_security_id"]) == sid), None)
            if sym:
                return str(sym)
        return f"{seg}:{sid}"

    def _note_feed_refused(self, d: date, accepted: list[Instrument], refused: Mapping[tuple[str, int], str]) -> None:
        """Keep today's refused map (first seen kept; an accepted retry leaves it) and send ONE personal line the
        first time in a day that contracts are refused (send_system: the owner's / client's own chat, never the
        group)."""
        if self._feed_refused_day != d:
            self.feed_refused.clear()
            self._feed_refused_day = d
        for s, i, _m in accepted:
            self.feed_refused.pop((s, int(i)), None)
        now_ms = self._wall_ms()
        for key, why in refused.items():
            prev = self.feed_refused.get(key)
            self.feed_refused[key] = {"reason": why, "since_ms": prev["since_ms"] if prev else now_ms,
                                      "name": self._feed_name(key)}  # fmt: skip
        if not refused or self._feed_refused_told == d:
            return
        self._feed_refused_told = d
        n_opt = sum(1 for seg, _sid in refused if seg == subs.OPTION_SEGMENT)
        n, what = (n_opt, "contract") if n_opt else (len(refused), "instrument")
        cap = self._feed_cap()
        why = (f"the broker's limit of {cap} instruments is reached" if cap is not None
               and all(r == "capacity" for r in refused.values()) else "the broker's live feed refused them")  # fmt: skip
        self._report_failure(f"live prices: {n} {what}{'' if n == 1 else 's'} could not be added ({why}); "
                             "they are listed but cannot alert today")  # fmt: skip

    def _feed_own_rejected(self, keys: Iterable[tuple[str, int]]) -> dict[tuple[str, int], str]:
        """0.8.5 seam (feed_protocol D2): of `keys`, the ones the feed's OWN rejected() names and its subscribed_ids()
        does not carry -> reason (REJECT_UNMAPPED / REJECT_ERROR as the feed says; its REJECT_CAP reads "capacity",
        the app's word for the same refusal). {} for a feed without the interface; a status read never fails."""
        f = self.feed
        rej, ids = getattr(f, "rejected", None), getattr(f, "subscribed_ids", None)
        if f is None or not callable(rej):
            return {}
        try:
            own = {(str(s), int(i)): str(why) for (s, i), why in dict(rej()).items()}
            carried = {(str(s), int(i)) for s, i in ids()} if callable(ids) else set()
        except Exception:  # a feed helper failing never fails a load or a status read
            log.debug("feed rejected() failed", exc_info=True)
            return {}
        return {k: ("capacity" if own[k] == "cap" else own[k]) for k in keys if k in own and k not in carried}

    def _feed_status(self) -> dict[str, Any]:
        """0.8.5 A1 (D2): the extra /api/status.feed keys -- refused {n, by_reason, names (first 20, contracts
        first)}, cap, n_on_feed (the feed's own count; None before the feed interface lands), start_error. Seam
        pass: `refused` also names what the app holds but the feed itself refused (its rejected(), with reasons)."""
        today = self._feed_refused_day is not None and self._feed_refused_day == self.trade_date
        items = list(self.feed_refused.items()) if today else []
        held = [k for k in self.subscribed if k not in self.feed_refused]
        items += [(k, {"reason": why, "name": self._feed_name(k)}) for k, why in self._feed_own_rejected(held).items()]
        by_reason: dict[str, int] = {}
        for _key, v in items:
            by_reason[v["reason"]] = by_reason.get(v["reason"], 0) + 1
        names = [v["name"] for k, v in items if k[0] == subs.OPTION_SEGMENT]
        names += [v["name"] for k, v in items if k[0] != subs.OPTION_SEGMENT]
        return {
            "n_on_feed": self._feed_count(),
            "cap": self._feed_cap(),
            "refused": {"n": len(items), "by_reason": by_reason, "names": names[:20]},
            "start_error": self._feed_start_error,
        }

    async def _warmup(self, d: date, sids: list[int]) -> None:
        """D24 previous-session seed, then (T-086) today's recorded bars replayed silently."""
        await self._warmup_previous(d, sids)
        await self._resume_today(d, sids)

    async def _warmup_previous(self, d: date, sids: list[int]) -> None:
        if self._scanner_flag("warmup") is False:
            log.info("warmup: %s has no usable intraday history; %d contracts warm up live",
                     self.broker_entry.name if self.broker_entry else "broker", len(sids))
            return
        adapter = self.adapter()
        if adapter is None:
            log.warning("warmup: no broker adapter; %d contracts warm up live", len(sids))
            return
        carry = OiCarry()
        if self._scanner_flag("warmup_oi") == "quote":  # candles carry no OI: prime it from one quote snapshot
            await asyncio.to_thread(self._prime_from_quotes, adapter, sids, carry)
        # T-107: cancelling this task (STOP -> shutdown cancels the day pass) does not stop the worker thread; the
        # event does -- fetch_warmup checks it before every contract, so no broker call follows the cancel
        stop = threading.Event()
        try:
            res = await asyncio.to_thread(
                fetch_warmup, adapter, sids, d, carry, cancelled=stop.is_set, **self._warmup_kwargs
            )
        except asyncio.CancelledError:
            stop.set()
            raise
        except BrokerAuthError as exc:
            log.error("warmup: auth rejected (%s); contracts warm up live", exc)
            return
        except LookupError as exc:  # calendar year missing for prev_trading_day
            log.error("warmup: %s; contracts warm up live", exc)
            return
        n = preload(self.series, res)
        for sid, bars in res.bars.items():
            if bars and self.carry.last(sid) is None:
                self.carry.prime(sid, bars[-1].oi)
        log.info("warmup: seeded %d/%d contracts", n, len(sids))

    @staticmethod
    def _prime_from_quotes(adapter: Any, sids: list[int], carry: OiCarry) -> int:
        """capabilities flag warmup_oi="quote": one quotes() batch primes the warm-up OiCarry (bar OI is
        display-only; condition A uses the in-session peak, D7). Blocking: runs in a worker thread."""
        try:
            quotes = adapter.quotes(sids, subs.OPTION_SEGMENT)
        except Exception as exc:  # noqa: BLE001 -- without it the candles are skipped, never invented
            log.warning("warmup: OI quote snapshot failed (%s: %s); OI-less candles are skipped",
                        type(exc).__name__, exc)
            return 0
        n = 0
        for sid, q in quotes.items():
            oi = getattr(q, "oi", None)
            if oi is not None:
                carry.prime(int(sid), oi)
                n += 1
        log.info("warmup: OI primed from quotes for %d/%d contracts", n, len(sids))
        return n

    def _read_today(
        self, d: date, sids: list[int], now_ms: int, adapter: Any = None, cancelled: Callable[[], bool] = lambda: False
    ) -> tuple[TodayResume, dict[int, frozenset[int]]]:
        """D98(7): the resume read in a WORKER thread (a parquet read + an oi_series scan + the T-585 broker
        backfill must not stall the tick loop), on its own read connection: sqlite3 objects are bound to the thread
        that made them. T-585 (CX-158 §1 C): warmup.read_today's two steps (load_today, then ALWAYS backfill_today:
        missing closed slots from the broker when `adapter` is given, every peak re-derived as the bar-close peak).
        Returns the resume and, per contract, the RECORDED slots (bars-5m.parquet), so the replay can tell a
        broker-filled bar (live never saw it) from a recorded one."""
        conn = db.connect(self.db_path)
        try:
            res = load_today(self.cfg.paths.data_dir / "parquet", conn, sids, d, now_ms)
        finally:
            conn.close()
        recorded = {sid: frozenset(b.start_ms for b in bars) for sid, bars in res.bars.items()}
        kw = {k: v for k, v in self._warmup_kwargs.items() if k in ("min_interval_s", "sleep", "monotonic")}
        return backfill_today(res, adapter, sids, d, now_ms, cancelled=cancelled, **kw), recorded

    async def _resume_today(self, d: date, sids: list[int]) -> dict[str, int]:
        """T-086 (D93): a (re)start at/after 09:15 re-seeds TODAY's recorded closed 5m bars and OI peak, then
        replays each bar SILENTLY through the engine (oi_now = the bar's OI, peak = the in-session peak at
        that bar's end), so an ARMED contract is ARMED again. Nothing is dispatched, sent, recorded or pushed:
        the signals of the replayed bars are already in the DB.

        T-091 (measured on 2026-09-25): the bar's OI IS what live handed the engine at a TIMER close (the timer
        closes a bar whose last tick set both bar.oi and builder.oi_now); only a TICK close hands it the closing
        tick's OI, and nothing recorded tells the two apart (oi_series / ticks parquet carry the exchange LTT, not
        the receive time the 300 ms timer races). 12/12 live bar-close OIs that day equal the bar's OI; the first
        tick at/after the end matched 4/12. So the replay keeps the bar's OI, and RECORDED EVIDENCE WINS
        (read_recorded_day):
          - a bar whose close decided a recorded signal is replayed with that signal's oi_now / oi_peak;
          - I8 (D287): a recorded V1 E2 the replay does NOT reproduce at its bar (a bar missing from the file, other
            inputs than live's) forces the engine ARMED on the recorded base (Engine.force_armed; counted as
            `armed_from_record` and diverged). The 0.8.4 re-arm reference correction is gone with the re-arm;
          - every recorded E1 of the day (a breakout is tick-driven: no bar replay can see it; a client's hidden one
            is a closed_cycles row) is applied IN TIME ORDER, before the first bar close after it, exactly as live's
            E1 changed the engine (_close_cycle_already_entered), so the replay never re-arms a cycle live closed.
        Damaged evidence is read as none and counted; a replay that had diverged from live is counted.

        T-102 (0.5.1): _on_bar_close records the (oi_now, oi_peak) it hands the engine WITH every bar
        (bars-5m.parquet eng_oi_now / eng_oi_peak, both editions), so a bar carrying them is replayed with exactly
        live's inputs -- a client, which records no E2, included. Precedence per bar: those recorded inputs > a
        recorded signal's evidence > the bar's own OI (a file written before 0.5.1 has no inputs).

        I8 invariant: after the replay a contract with ANY recorded V1 row today is never IDLE -- also when today's bars
        cannot be read (the evidence is applied without them) and for an engine re-created mid-day (retired, then
        re-listed: AM8; a recorded entry sets DONE, an E2 alone ARMED)."""
        now_ms = self._wall_ms()
        open_ms, _ = session_bounds_ms(d)
        if now_ms < open_ms:
            return {}
        # T-585 (CX-158 §1 C): the broker backfills today's missing closed slots unless it has no usable intraday
        # history (flag warmup False: no call, the recorded bars still get bar-close peaks). T-107: cancelling this
        # task does not stop the worker thread; the event does (checked before every broker call).
        adapter = None if self._scanner_flag("warmup") is False else self.adapter()
        stop = threading.Event()
        recorded: dict[int, frozenset[int]] = {}
        try:
            res, recorded = await asyncio.to_thread(self._read_today, d, sids, now_ms, adapter, stop.is_set)
        except asyncio.CancelledError:
            stop.set()
            raise
        except Exception:  # a resume bug must never stop the day: it warms up live, on the recorded evidence (I8)
            log.exception("resume: today's bars could not be loaded; contracts warm up live")
            res = TodayResume(trade_date=d)
        rec = read_recorded_day(self.conn, d, self.signal_tf)
        span = self.signal_tf * MINUTE_MS
        n = dict.fromkeys(
            ("contracts", "bars", "inputs", "from_record", "armed_from_record", "closed", "diverged", "missed_e1"), 0
        )
        bad_arms = [0]
        quiet = _edition.is_client()  # a client's hidden entries are never counted in an INFO line (FIX2 F1)
        for sid in sids:
            builder, series = self.builders.get(sid), self.series.get(sid)
            if builder is None or series is None:
                continue
            peak = res.oi_peak.get(sid)
            if peak is not None:  # T-585: the bar-close peak, never max-ed with a tick peak (display only now)
                builder.oi_peak_today = int(peak)
            bars, peaks = res.bars.get(sid, []), res.peaks.get(sid, [])
            recorded_in = res.inputs.get(sid) or [None] * len(bars)
            evidence = sid in rec.entries or sid in rec.arms  # I8: recorded rows alone re-seed the engine
            eng = (
                self.pool.get_or_create(sid, self._list_of(sid)) if (bars or evidence) and self.engines_allowed
                and (self.v1r is None or self.rows.get(sid, {}).get("tag") == "CARRY")
                else None
            )  # fmt: skip
            meta = self.meta.get(sid)
            tick_size = meta.tick_size if meta is not None else None
            pending = deque(rec.entries.get(sid, ()) if eng is not None else ())
            arms = deque(rec.arms.get(sid, ()) if eng is not None else ())

            def apply_until(limit: int, eng: ContractEngine | None = eng, pending: deque[RecordedEntry] = pending,
                            arms: deque[RecordedArm] = arms, sid: int = sid) -> None:  # fmt: skip
                """Every recorded E2 (whose bar the replay never reached) and E1 before `limit`, in time order."""
                while eng is not None:
                    a_key = arms[0].ts_ms if arms else None
                    e_key = pending[0].order_key(span)[0] if pending else None
                    if a_key is not None and a_key < limit and (e_key is None or a_key <= e_key):
                        self._force_recorded_arm(sid, eng, arms.popleft(), n, bad_arms)
                    elif e_key is not None and e_key < limit:
                        e = pending.popleft()
                        diverged = self._close_cycle_already_entered(sid, eng, e)
                        if not (quiet and e.hidden):
                            n["closed"] += 1
                            n["diverged"] += diverged
                    else:
                        return

            for b, pk, row_in in zip(bars, peaks, recorded_in, strict=True):
                close = b.start_ms + span
                apply_until(close)  # live's E1s inside this bar happened before its close
                eb = self.carry.bar(sid, b)
                if eb is None:
                    continue
                series.append(eb)
                self._resumed_bars.append((sid, b, row_in))
                n["bars"] += 1
                if eng is None:
                    continue
                # T-102 precedence: the engine inputs recorded WITH the bar > a recorded signal's evidence > bar OI
                oi_now, oi_pk = float(b.oi) if b.oi is not None else eb.oi, float(pk)
                inputs = rec.bar_inputs.get((sid, close))
                if row_in is not None:  # exactly what live handed the engine at this close
                    (oi_now, oi_pk), n["inputs"] = row_in, n["inputs"] + 1
                elif inputs is not None:  # live's own inputs at this close (a tick close's closing-tick OI included)
                    (oi_now, oi_pk), n["from_record"] = inputs, n["from_record"] + 1
                # SILENT: the returned Signal (already recorded before the restart) is never dispatched
                sig = eng.on_bar_close(series.closed(), oi_now, oi_pk, recv_ms=None, tick_size=tick_size)
                if arms and arms[0].ts_ms == close:  # I8: live armed at this close
                    arm = arms.popleft()
                    if sig is None or sig.kind != "E2":  # ... and the replay did not: put live's E2 back
                        self._force_recorded_arm(sid, eng, arm, n, bad_arms)
                if sig is None:
                    continue
                if sig.kind == "E1" and pending and pending[0].order_key(span)[:2] == (sig.ts_ms, 0):
                    pending.popleft()  # the replay reproduced this recorded shakeout E1: applied once, not twice
            apply_until(now_ms + 1)  # the E2s / E1s after the last closed bar (the bar forming at the restart)
            n["missed_e1"] += self._hold_missed_e1(sid, eng, series, recorded.get(sid, frozenset()), bars, span)
            last = res.last_oi.get(sid)
            if last is not None:
                builder.oi_now = int(last)
                self.carry.prime(sid, last)
            n["contracts"] += bool(bars) or (eng is not None and evidence)
        unreadable = rec.unreadable + bad_arms[0]
        if n["contracts"]:
            parts = [f"{n['inputs']} bars with recorded engine inputs"] if n["inputs"] else []
            parts += [f"{n['from_record']} bar closes"] if n["from_record"] else []
            # "arms", never "armed": server.CLIENT_LOG_HIDE drops a line matching \bARMED\b (owner-only count anyway)
            parts += [f"{n['armed_from_record']} arms from the record"] if n["armed_from_record"] else []
            parts += [f"{n['closed']} recorded cycle closes"] if n["closed"] else []
            parts += [f"{n['diverged']} diverged from the replay"] if n["diverged"] else []
            parts += [f"{n['missed_e1']} missed E1 held done"] if n["missed_e1"] else []  # T-585 (CX-158 §1 C)
            parts += [f"{unreadable} unreadable"] if unreadable else []
            note = f"; recorded evidence: {', '.join(parts)}" if parts else ""
            log.info("resume %s: %d bars of today replayed silently into %d contracts%s", d, n["bars"],
                     n["contracts"], note)  # fmt: skip
        if self.recorder is not None and self._resumed_bars:
            self._hand_resumed_bars_to_recorder()
        return {**n, "unreadable": unreadable}

    @staticmethod
    def _hold_missed_e1(
        sid: int, eng: ContractEngine | None, series: ContractBars, recorded: frozenset[int], bars: list[Any], span: int
    ) -> int:
        """T-585 (CX-158 §1 C / §1 B4): an engine the replay left ARMED on a broker-FILLED slot (a bar live never saw,
        so no E1 after it was ever recorded) whose closed bars already show the breakout at bar resolution
        (warmup.missed_breakout) is held DONE: that E1 was missed, and firing it late would be a stale entry. Returns 1
        when it held the engine, else 0."""
        if eng is None or eng.state is not State.ARMED or eng.armed_bar_ts is None or eng.consol_hi is None:
            return 0
        filled = {b.start_ms for b in bars} - recorded
        if eng.armed_bar_ts not in filled:
            return 0
        hit = missed_breakout(series.closed(), eng.armed_bar_ts + span, eng.consol_hi, eng.params.vol_sma_len)
        if hit is None:
            return 0
        eng.state = State.DONE
        log.debug("resume: sec %s armed on a filled bar at %s, breakout at %s already passed; held DONE",
                  sid, eng.armed_bar_ts, hit)  # fmt: skip
        return 1

    def _force_recorded_arm(
        self, sid: int, eng: ContractEngine, arm: RecordedArm, n: dict[str, int], bad: list[int]
    ) -> None:
        """I8: a recorded V1 E2 the replay did not reproduce at its bar -> the engine ARMED on the recorded base
        (Engine.force_armed), counted in n armed_from_record + diverged. A row with no readable base cannot be armed
        on (no consol_hi for the breakout, no consol_lo for the shakeout): DONE instead, counted unreadable -- the
        contract stays out of IDLE (an E2 is once per contract per day, D287 (1)). Owner-only evidence (a client
        records its entry as kind E1, D288), logged at INFO in the owner edition only."""
        span = self.signal_tf * MINUTE_MS
        if arm.consol is None:
            eng.state, bad[0] = State.DONE, bad[0] + 1
            log.debug("resume: sec %s recorded E2 at %s has no readable base; held DONE", sid, arm.ts_ms)
            return
        bar_ts = arm.bar_ts_ms if arm.bar_ts_ms is not None else arm.ts_ms - span
        eng.force_armed(arm.consol, bar_ts, cycle=dict(arm.cycle or {}), oi_at_alert=arm.oi_now)
        n["armed_from_record"] += 1
        n["diverged"] += 1
        (log.debug if _edition.is_client() else log.info)(
            "resume: sec %s armed from the recorded E2 at %s (the replay did not reproduce it)", sid, arm.ts_ms
        )

    def _close_cycle_already_entered(self, sid: int, eng: ContractEngine, e: RecordedEntry) -> bool:
        """T-091: apply ONE recorded E1 (visible signals row, a client entry row -- the E2 bar close stored as kind E1,
        D288 -- or a client's hidden closed_cycles row, D98(4)) to the replayed engine exactly as live's E1 changed it
        (engine.on_tick / _shakeout: state DONE, oi_at_alert = the E1's oi_now, signals_today + 1; the cycle's consol
        and arming bar stay). Returns True when the replay had DIVERGED from live (the engine was not ARMED, or ARMED on
        another arming bar than the E1's cycle): it is still moved to live's post-E1 state, taking the cycle's consol /
        arming bar from the record when recorded (else the latest arming bar the E1 allows, so an older setup's visible
        E1 is not read as this cycle's entry).

        I8 (D287): an E1 recorded WITHOUT a readable oi_now is applied too (the T-104(b) skip is gone): with no re-arm,
        DONE without a reference is exactly the day's rule, and leaving the engine IDLE would let it enter twice."""
        span = self.signal_tf * MINUTE_MS
        diverged = eng.state is not State.ARMED or (
            e.cycle_e2_ts_ms is not None and e.cycle_e2_ts_ms != eng.armed_bar_ts
        )
        if diverged:
            if e.consol is not None:
                eng.consol_lo, eng.consol_hi, eng.consol_mid = e.consol
            if e.cycle_e2_ts_ms is not None:
                eng.armed_bar_ts = e.cycle_e2_ts_ms
            else:
                open_ms = session_bounds_ms(ist_date(e.ts_ms))[0]
                eng.armed_bar_ts = open_ms + ((e.ts_ms - open_ms) // span) * span - span
        eng.state = State.DONE
        eng.signals_today += 1
        if e.oi_now is not None:
            eng.oi_at_alert = e.oi_now
        note = "; the replay had diverged" if diverged else ""
        if e.hidden and _edition.is_client():
            # FIX2 F1: the client never saw this entry; an INFO line would reveal it on /api/logs, the WS log feed
            # and gammad.log (the client log filter does not match it). DEBUG, as the router's own hidden-entry line.
            log.debug("resume: sec %s cycle closed by a hidden entry at %s%s", sid, e.ts_ms, note)
        else:
            log.info("resume: sec %s entry already recorded at %s; its cycle stays closed%s", sid, e.ts_ms, note)
        return diverged

    def _hand_resumed_bars_to_recorder(self) -> None:
        """The recorder rewrites bars-5m.parquet from ITS OWN in-memory bars: hand it today's recovered bars
        first, so a later flush (and a second restart) keeps the morning."""
        assert self.recorder is not None
        bars, self._resumed_bars = self._resumed_bars, []
        for sid, b, inputs in bars:  # T-102: only RECORDED inputs go back (a replay's own guess is never written)
            self._record_bar(sid, b, inputs)

    async def close_day(self) -> None:
        """15:35: stop the feed if still up, let the recorder flush + close the day, reset every engine."""
        d = self.trade_date
        self.tracker.snapshot_closes(self._now())  # Z1(6): FIRST, while the last LTPs are still known
        self.tracker.close_intraday(self._now())  # D98(1): an intraday trade never carries into the next morning
        await self.stop_feed()
        if self.owner_ext is not None:  # 0.8.2: the owner sink's last bars, after stop_feed drained the queue
            try:
                await self.owner_ext.close_day(d)
            except Exception:
                log.exception("owner extension close_day %s failed", d)
        await self._stop_recorder()
        self.pool.reset_all_day()
        self.pool = self._make_pool(self.scanners[0])
        for pool in self.shadow_pools.values():
            pool.reset_all_day()
        self.shadow_pools = {s.name: self._make_pool(s) for s in self.scanners[1:]}
        self.sw_any3_pool = self._make_any3_pool()  # T-419: re-primed by the next load_day
        self.carry.forget()
        self.builders.clear()
        self.series.clear()
        self.meta.clear()
        self.rows.clear()
        # T-058: an underlying's last LTP is the spot of the D63 zone gate (signal_context); an overnight service must
        # not gate the next session's first signals on this session's close before the underlying's first tick. Only
        # the underlyings' entries (today's lists + the open trades' NSE_EQ subscriptions) -- the contracts' LTPs
        # stay for the evening's dashboard and manual sends. The feed writes last_ltp only 09:15-15:30 and close_day
        # runs before the next day's load_day (ensure_day), so every entry dropped here is this session's.
        spot = {sid for seg, sid in self.subscribed if seg == subs.UNDERLYING_SEGMENT}
        for und in self.underlyings | spot:
            self.last_ltp.pop(und, None)
        self.underlyings.clear()
        self.und_builders.clear()
        self.und_bars.clear()
        self._resumed_bars.clear()
        self._h1_wait.clear()  # T-267: the books stay (kept across days); only today's seed waits end
        self.subscribed.clear()
        # T-277 rider (0.8.5 A1): the day's feed refusals end with the day (the next load_day subscribes afresh)
        self.feed_refused.clear()
        self._feed_refused_day = None
        self._v1_sids.clear()
        self._tick_pushed.clear()
        self._entered_today.clear()
        self.trade_date = None
        self.closed_date = d
        log.info("close_day %s: recorder flushed, engines reset", d)

    # ---------------------------------------------------------------- feed + recorder lifecycle
    async def start_feed(self) -> bool:
        if self.feed is not None:
            return True
        self._note_scanner()
        problem = self._feed_problem()  # the broker's REQUIRED keys (registry), not Dhan's by name
        if problem is not None:
            if self._feed_disabled is None:
                self._feed_disabled = problem
                log.error("feed not started: %s", self._feed_disabled)
            return False
        self._feed_disabled = None
        now_ms = self._wall_ms()
        last_fail = self._feed_start_failed_ms
        if last_fail is not None and now_ms - last_fail < self.feed_start_retry_s * 1000:
            return False  # 0.8.5 A1: a failed start is retried by this 2 s pass, at most once a minute
        self._start_recorder()
        queue: asyncio.Queue[Tick] = asyncio.Queue(maxsize=100_000)
        items = [(s, i, m) for (s, i), m in self.subscribed.items()]
        refused: dict[tuple[str, int], str] = {}
        feed: Any = None
        try:
            # 0.8.5 A1 (D1): self.feed is set only once start() returned; over the cap, start what fits (the
            # _feed_units order) and refuse the rest (offered again at the next load_day, by add_instruments)
            feed = self._feed_factory(queue, self.on_auth_error, self.cfg)
            cap = self._feed_cap(feed)
            if cap is not None and len(items) > cap:
                items, left = self._feed_fit(items, cap)
                refused = {(s, int(i)): "capacity" for s, i, _m in left}
                log.warning("live feed: %d of %d instrument(s) over the broker's limit of %d; starting with %d, the "
                            "rest stay listed and are offered again at the next list load", len(left),
                            len(items) + len(left), cap, len(items))  # fmt: skip
            await feed.start(items)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 -- no half-alive feed: nothing is kept, the reason goes to status
            self._feed_start_error = f"{type(exc).__name__}: {exc}"[:200]
            self._feed_start_failed_ms = now_ms
            log.error("feed start failed (%s); retried in %.0f s", self._feed_start_error, self.feed_start_retry_s)
            if feed is not None:
                with contextlib.suppress(Exception):
                    await feed.stop()
            d = self.trade_date
            if d is not None and self._feed_start_told != d:  # ONE personal line a day (the alarm needs a feed)
                self._feed_start_told = d
                self._report_failure(f"live prices could not start ({type(exc).__name__}); retrying every minute")
            return False
        self._feed_start_error = None
        self._feed_start_failed_ms = None
        self.tick_queue = queue
        self.feed = feed
        self.feed_alarm.set_feed_gap(getattr(feed, "max_tick_gap_s", None))  # T-308 (4): a slow poll feed's gap
        self._feed_task = self.spawn(self._consume(self.tick_queue), name="tick-consumer", daemon=True)
        for key in refused:
            self.subscribed.pop(key, None)
        accepted = list(items)
        # T-308 (1): a load_day that ran while start() awaited (a mid-session restart: _supervise_day calls start_feed
        # outside the day lock) wrote its new instruments straight into self.subscribed; the feed started with the
        # snapshot, and subs.additions never offers a held key again -- so add them now, through the normal add path
        offered = {(s, int(i)) for s, i, _m in items} | set(refused)
        late = [(s, i, m) for (s, i), m in self.subscribed.items() if (s, int(i)) not in offered]
        if late:
            took, late_refused = await self._feed_take(late)
            for key in late_refused:
                self.subscribed.pop(key, None)
            accepted += took
            refused.update(late_refused)
            log.info("feed start: +%d instrument(s) listed while it started", len(took))
        # T-308 (2): ids the feed itself refused at start (unmapped / lookup raised) -> refused now, as _subscribe does
        own = self._feed_own_rejected(self.subscribed)
        for key in own:
            self.subscribed.pop(key, None)
        accepted = [x for x in accepted if (x[0], int(x[1])) not in own]
        refused = {**own, **refused}
        if self.trade_date is not None:
            self._note_feed_refused(self.trade_date, accepted, refused)
        log.info("feed started with %d instruments", len(accepted))
        return True

    async def stop_feed(self) -> None:
        feed, self.feed = self.feed, None
        if feed is not None:
            await feed.stop()
            log.info("feed stopped")
        task, self._feed_task = self._feed_task, None
        if task is not None:
            q = self.tick_queue
            while q is not None and not q.empty():  # drain what already arrived
                self._safe_tick(q.get_nowait())
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        self.tick_queue = None

    def _start_recorder(self) -> None:
        if self.recorder is not None:
            return
        self.recorder = self._recorder_factory(self.cfg)
        if self._resumed_bars:
            self._hand_resumed_bars_to_recorder()
        self._recorder_stop = asyncio.Event()
        self._recorder_task = self.spawn(self.recorder.run(self._recorder_stop), name="recorder", daemon=True)

    async def _stop_recorder(self) -> None:
        if self.recorder is None:
            return
        assert self._recorder_stop is not None
        self._recorder_stop.set()
        if self._recorder_task is not None:
            # T-060 follow-up: shielded -- a cancelled CALLER (the day pass running a missed close_day when STOP
            # arrives) must not cancel the recorder's drain + close_day flush. The CancelledError still reaches the
            # caller, leaving self.recorder set, so shutdown's own _stop_recorder then awaits the same task to its end.
            task = self._recorder_task
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                if not task.cancelled():  # the caller was cancelled, not the recorder: propagate
                    raise
            except Exception:  # noqa: BLE001, S110 -- a failed recorder is logged by spawn's done-callback
                pass
        # T-284 (3): after the drain + close_day flush, so its failures count too; NOT in a finally (a cancelled caller
        # leaves self.recorder set and shutdown's own _stop_recorder folds it once)
        self._fold_recorder_errors()
        self.recorder = None
        self._recorder_stop = None
        self._recorder_task = None

    def _fold_recorder_errors(self) -> None:
        """T-284 (3): keep a stopped recorder's failure counts for the rest of the day (status()["recorder"])."""
        st = getattr(self.recorder, "stats", None)
        if st is None:
            return
        self._recorder_health()  # rolls the accumulator to today first
        acc = self.recorder_errors_today
        acc["parquet_failed"] += int(getattr(st, "parquet_failed", 0) or 0)
        acc["db_failed"] += int(getattr(st, "db_failed", 0) or 0)
        acc["stale_skipped"] += int(getattr(st, "stale_skipped", 0) or 0)
        last = getattr(st, "last_failed_ms", None)
        if last is not None and (acc["last_failed_ms"] is None or last > acc["last_failed_ms"]):
            acc["last_failed_ms"] = last

    async def _consume(self, q: asyncio.Queue[Tick]) -> None:
        while True:
            tick = await q.get()
            self._safe_tick(tick)

    def _safe_tick(self, tick: Tick) -> None:
        try:
            self.handle_tick(tick)
        except Exception:  # one bad tick must never kill the feed consumer
            self.stats.tick_errors += 1
            log.exception("tick handling failed for sec %s", tick.security_id)

    # ---------------------------------------------------------------- tick path
    def handle_tick(self, tick: Tick) -> None:
        sid = tick.security_id
        self.last_tick_ms = tick.recv_ms
        ext = self.owner_ext
        if ext is not None and sid in ext.claimed:  # 0.8.2 (C.7(3), B4): an owner-claimed sid feeds the owner sink
            try:
                ext.on_tick(tick)
            except Exception:  # the owner sink must never cost a V1 tick
                if "owner_tick" not in self._warned:
                    self._warned.add("owner_tick")
                    log.exception("owner sink on_tick failed for sec %s (logged once)", sid)
            if not self._v1_instrument(sid):  # and nothing else here: no recorder, last_ltp, push, bar or stats
                return
        self.stats.ticks += 1
        stale = is_stale_tick(tick.ts_ms, tick.recv_ms, self._wall_ms)
        if stale:
            self._count_stale_guarded()
        # Keep the three call-site guard blocks explicit (T-412 / T-414, D493 (1)).
        if not stale:  # noqa: SIM102 -- T-412 / T-414 (a+b), D493 (1)
            if tick.ltp is not None and tick.ltp > 0:
                self.last_ltp[sid] = tick.ltp
                if self.s089 is not None:
                    observations = self._089_prices.setdefault(sid, [])
                    observations.append((tick.ts_ms, tick.ltp))
                    # IV needs the last 15 minutes, plus boundary observations.
                    if len(observations) > 4096:
                        del observations[:-2048]
                held = self._held_lows.get(sid)
                if held:  # T-310: a held V1 signal's lowest price since it fired (its release checks the stop)
                    for ts, low in held.items():
                        if tick.ts_ms >= ts and tick.ltp < low:
                            held[ts] = tick.ltp
        if not stale:  # T-412 / T-414 (tracker), D493 (1)
            if sid in self.tracker.v1r_ids:
                self.tracker.on_v1r_tick(sid, tick.ltp, tick.ts_ms)
            if sid in self.tracker.open_ids:  # Z1(4): T1 / T2 / trail / same-day SL of an open trade
                try:
                    self.tracker.on_tick(sid, tick.ltp, tick.ts_ms)
                except Exception:  # a tracker bug must never cost the bars / engine of this tick
                    log.exception("trade tracker on_tick failed for sec %s", sid)
            # T-267 (M11): an open V2T option's LTP (its DAY5 / EXPIRY / ZSL exit price); T-419: a Still Watching option's
            # (its SW_* exit price at a stock close / the 15:15 eve exit)
            if sid in self.tracker.zone_ids or sid in getattr(self.tracker, "sw_ids", ()):
                try:
                    self.tracker.on_zone_tick(sid, tick.ltp, tick.ts_ms)
                except Exception:
                    log.exception("trade tracker on_zone_tick failed for sec %s", sid)
        if self.grader is not None and sid in self.grader.open_ids:  # W15 §6: MFE / MAE / T1 / SL of a graded E1
            try:
                self.grader.on_tick(sid, tick.ltp, tick.ts_ms, recv_ms=tick.recv_ms)
            except Exception:
                log.exception("scanner grader on_tick failed for sec %s", sid)
        if self.recorder is not None:
            self.recorder.submit(tick)
        if not stale:  # T-412 / T-414 (d), D493 (1)
            ub = self.und_builders.get(sid)
            if ub is not None:  # an underlying's QUOTE tick: its 5-min bars feed the direction gate
                for bc in ub.on_tick(tick):
                    self._on_und_close(bc)
            builder = self.builders.get(sid)
            if builder is not None:
                for bc in builder.on_tick(tick):
                    if bc.tf_min == self.signal_tf:  # D295: the underlying's bar of this slot first
                        self._close_und_due(sid, bc.bar.end_ms)
                    self._on_bar_close(bc, recv_ms=tick.recv_ms)
                if self.s089 is not None and self.engines_allowed and sid not in self._089_wait:
                    forming = builder.forming_bar(self.signal_tf)
                    if forming is not None:
                        self.s089.fill(sid, forming.start_ms, forming.open)
                        if self.s089.v2 is not None and tick.ltp is not None:
                            self.s089.v2.on_tick(sid, tick.ltp, tick.ts_ms, forming.open)
                if sid in self.tracker.v1r_ids:
                    self.tracker.on_v1r_tick(sid, tick.ltp, tick.ts_ms)
                if self.tracker.v1r_ids and tick.ts_ms >= v1_hours.last_bar_ms(self.trade_date):
                    self.on_timer(tick.ts_ms)  # all option slots walk before the common clock closes any trade
                eng = self.pool.get(sid)
                if eng is not None and eng.state is State.ARMED:  # E1 breakout is tick-driven, ARMED only
                    self._route_tick(sid, tick, builder)
                for name, pool in self.shadow_pools.items():  # W15 §5: the shadow scanners' ARMED engines too
                    seng = pool.get(sid)
                    if seng is not None and seng.state is State.ARMED:
                        self._route_tick(sid, tick, builder, pool=pool, scanner=name)
            self._push_tick(tick)

    def _close_und_due(self, sid: int, close_ms: int) -> None:
        """D295 (0.8.5 audit A L1): an option tick closing its signal-tf bar at `close_ms` first closes its
        underlying's forming bar when that bar ends at/before close_ms (the option's tick proves the slot is over),
        so the E2's context (gates.direction: drift + VWAP) sees the underlying's bar ending at the E2 close even
        when the underlying's own next tick arrives later. The same BarBuilder.on_timer close the timer path uses."""
        meta = self.meta.get(sid)
        und = meta.underlying_security_id if meta is not None else None
        ub = self.und_builders.get(und) if und is not None else None
        if ub is None:
            return
        forming = ub.forming_bar(self.signal_tf)
        if forming is None or forming.end_ms > close_ms:
            return
        for ubc in ub.on_timer(forming.end_ms + ub.timer_grace_ms):
            self._on_und_close(ubc)

    def _v1_instrument(self, sid: int) -> bool:
        """0.8.2 (B4): a security id the V1 path must see: subscribed by V1 today (lists, open trades), a list
        contract or underlying, an open trade's (a carried V2T Z_OPEN option included, T-267 M11), or a graded E1's."""
        return (
            sid in self._v1_sids or sid in self.builders or sid in self.und_builders or sid in self.tracker.open_ids
            or sid in self.tracker.zone_ids or (self.grader is not None and sid in self.grader.open_ids)
        )  # fmt: skip

    def _make_pool(self, spec: ScannerSpec) -> EnginePool:
        """The EnginePool of one scanner. V1 keeps the runtime's own params / HoldParams objects (tests and the
        settings reload own them); every other scanner is built from its registry spec by engine_pool.make_pool
        (S1.7.0: V2T's engine_factory = V2TEngine and its `lists`, which a plain EnginePool would drop)."""
        if getattr(self, "scanners_089", False):
            return EnginePool(self.params, by_list=self.hold_params, client_rules=spec.client_rules, lists=())
        if spec is self.scanners[0]:
            pool = EnginePool(self.params, by_list=self.hold_params, client_rules=spec.client_rules)
            pool.base_atr_max = still_watching_cfg(self.cfg).base_atr_max  # T-419: a CARRY row's relax-C (owner Q3)
            if self.v1r is not None and not self.v1r.is_pending("carry.entry_mode") and self.v1r.carry_mode() == "v1_break":
                from gammad.v1.carry_break import V1BreakCarryEngine

                pool.carry_cls = V1BreakCarryEngine
            return pool
        return make_pool(spec)

    def _make_any3_pool(self) -> EnginePool | None:
        """T-419 (D474 / D476 (1)): the OWNER's CARRY-ANY3 live shadow pool (carried contracts only); None in the client
        edition, with Still Watching off, or with still_watching.owner_any3_shadow false."""
        if getattr(self, "scanners_089", False):
            return None
        sw = still_watching_cfg(self.cfg)
        if not (_edition.is_owner() and sw.enabled and sw.owner_any3_shadow):
            return None
        return make_any3_pool(self.pool, base_atr_max=sw.base_atr_max)

    def _init_089(self):
        from gammad.core.expiry import dte
        from gammad.scanners.live.hub import ScannerHub
        from gammad.scanners.live.views import LiveBanView, LiveIvView, LiveZoneView
        from gammad.scanners.shared.gates import E1Cut, SharedGates
        from gammad.scanners.shared.results_cal import ResultsCalendar
        from gammad.scanners.shared.risk import RiskPerLot
        from gammad.scanners.v1r.engine import V1rEngine

        self._089_wait = {}
        self._089_und_wait = {}
        # T-677: set while no prepare runs; a backfill waits on it so it never seeds before the prepare's own seed
        self._089_seeded = asyncio.Event()
        self._089_seeded.set()
        self._089_preparing = 0
        self._089_retry = (None, 0)  # audit P2B-APP-03 (review F1): (day, fresh first-of-day retries spent)
        self._089_failed_day = None  # ... and the day its ONE SYSTEM line went out
        self._089_engine_day = None  # delta G1: the day hub.prepare last returned for
        self._089_prices = {}
        self._089_results = ResultsCalendar(self.today(), self.today(), stale=True)
        self._089_results_day = None
        self._089_delivery = None
        iv = LiveIvView(self._089_price_at, self._089_price_at, self._089_meta)
        engine = V1rEngine(
            edition="owner" if _edition.is_owner() else "client",
            gates=SharedGates(LiveBanView(lambda day: self.sw_ban)), results=self._089_results,
            zones=LiveZoneView(lambda symbol: next((self.sw_books.get(r["underlying_security_id"])
                                                   for r in self.rows.values() if r["symbol"] == symbol), None)),
            dte_fn=dte, risk_fn=RiskPerLot(iv))
        self.s089 = ScannerHub(
            self.conn, edition=engine.edition, engine=engine, e1_cut=E1Cut(cal.prev_trading_day),
            sender=S089Sender(self.router), chat_ids=S089Chats(self.router),
            on_failure=self._089_failed, on_no_group_chat=self._089_no_group_chat,
            clock_ms=self._wall_ms, publish=self.hub.publish, spawn=self._089_send,
            spec_version=self.spec_version, ltp=self.last_ltp.get,
            config={"band_pct": 2.5, "refresh": {"first": self.cfg.schedule.refresh.first,
                                               "every_min": self.cfg.schedule.refresh.every_min,
                                               "last": self.cfg.schedule.refresh.last},
                    "group_additions": sorted(self.group_additions)})
        if _edition.is_owner():
            from gammad.scanners.v2sq.live import LiveSqueeze

            self.s089.v2 = LiveSqueeze(self.s089, root=self.data_dir / "v2sq", owner=self.owner_ext, iv=iv)

    async def _089_failed(self, post, delivery):
        """JOBS_ALERTS-02: a FAILED GROUP card -> the router's one ERROR line + one personal GROUP SEND FAILED line
        (latched per day and card kind; the failed send itself already counted in the router's streak). A card held by
        the PAUSE is not a failure (the legacy router logs it paused and never escalates)."""
        from gammad.alerts import s089_dispatch as dispatch

        if post.chat == dispatch.GROUP and delivery.error != "RuntimeError: paused":
            what = "exit card" if post.key.count("|") >= 4 else "entry card"  # s089_dispatch.exit_key: 5 parts
            await self.router._auto_post_failed(what, delivery.error)

    def _089_no_group_chat(self):
        """JOBS_ALERTS-02: a GROUP card with no group chat counts in the router's failure streak, as its own sends do."""
        from gammad.alerts.router import CH_GROUP

        self.router._telegram_failed(CH_GROUP, "telegram not configured")

    def _089_meta(self, sid):
        return next((r for r in self.s089.list_rows(self.s089.day) if r.security_id == sid), None)

    def _089_price_at(self, sid, ts_ms):
        observations = self._089_prices.get(sid, ())
        return next((px for stamp, px in reversed(observations) if stamp <= ts_ms), None)

    def _089_send(self, coroutine):
        previous = self._089_delivery

        async def ordered():
            try:
                if previous is not None:
                    try:
                        await asyncio.shield(previous)
                    except Exception:
                        log.warning("s089 delivery: the previous delivery failed; sending the next",
                                    exc_info=True)
                return await coroutine
            finally:
                coroutine.close()

        self._089_delivery = self.spawn(ordered(), name="s089-delivery")

    async def _prepare_089(self, day, rows):
        """T-677: a backfill spawned while a prepare runs (a slot end crossed mid-prepare) waits until the prepare has
        seeded (or returned early, or failed): the engine's series drop any bar older than their last, so a backfill
        seeding first would drop every previous-day bar the prepare sends after it."""
        self._089_preparing += 1
        self._089_seeded.clear()
        hub = self.s089
        retry_day, spent = self._089_retry
        spent = spent if retry_day == day else 0
        if retry_day == day and hub.day != day and hub.pending and self._089_engine_day == day:
            # review F1: a fresh retry runs hub.prepare's first-of-day path, which clears hub.pending: a fill accepted
            # since the failed prepare is never wiped by it; the day goes on along the refresh path instead. Delta G1:
            # only once hub.prepare has run today (else the engine is on an earlier day: every refresh would raise)
            log.warning("s089 prepare %s: a fill is pending; no fresh retry", day)
            hub.day, spent = day, PREPARE_089_RETRIES
            self._089_retry = (day, spent)
        first = hub.day != day
        try:
            await self._prepare_089_body(day, rows)
        except Exception as exc:
            # Audit P2B-APP-03: hub.prepare set hub.day before the raise, so every later prepare took the refresh path
            # (no re-seed / restore / V2SQ morning) and nothing was said (a refresh job is quiet, T-277). A failed FIRST
            # prepare of the day is undone (the next mark retries it whole, at most PREPARE_089_RETRIES times, never
            # over a pending fill) and the day's FIRST failure says so in ONE SYSTEM line; the raise still reaches
            # load_day's caller as before (T-677 (b) pins it).
            if first and spent < PREPARE_089_RETRIES and not hub.pending:
                hub.day = None
                self._089_retry = (day, spent + 1)
            elif first:
                log.warning("s089 prepare %s: no further fresh retry (%d spent)", day, spent)
            if self._089_failed_day != day:
                self._089_failed_day = day
                self._report_failure(f"s089 prepare {day} failed: {type(exc).__name__}: {exc}")
            raise
        finally:
            self._089_preparing -= 1
            if not self._089_preparing:
                self._089_seeded.set()

    async def _prepare_089_body(self, day, rows):
        from gammad.scanners import late_start
        from gammad.scanners.v1r.entry import StockBar

        hub = self.s089
        fresh_day = hub.day != day
        if fresh_day:
            self._089_wait.clear()
            self._089_und_wait.clear()
            self._089_prices.clear()
        # Include read-side Carry rows retired only by the old ENTERED rule.
        sources = [dict(r) for r in self.conn.execute("SELECT * FROM watchlist WHERE trade_date=?",
                                                     (day.isoformat(),))]
        if sources and self._sw_ban_tried != day:
            self._sw_ban_tried = day
            await self.run_sw_ban_job(day)
        added = hub.prepare(day, sources, at_ms=self._wall_ms())
        self._089_engine_day = day  # P2B-APP-03 delta G1: hub.prepare returned, so its engine runs `day`
        if fresh_day and hub.v2 is not None:
            if self.owner_ext is not None:
                master = await asyncio.to_thread(self.owner_ext._master_for, day, self.adapter())
            else:
                master_fn = getattr(self.adapter(), "scrip_master", None)
                master = await asyncio.to_thread(master_fn) if callable(master_fn) else None
            added += list(hub.v2.morning(day, master, {}))
            if added and self._sw_ban_tried != day:
                self._sw_ban_tried = day
                await self.run_sw_ban_job(day)
        # Open ladders retain subscriptions even when no current list contains their contract.
        active = [p for p in hub.states.values() if not p.closed]
        claims, squeeze_claims = [], []
        for row in sorted([*hub.list_rows(day), *active], key=lambda r: getattr(r, "rank", 0)):
            sid, und = row.security_id, row.underlying_security_id
            data = row.to_dict()
            self.rows.setdefault(sid, data)
            self.meta.setdefault(sid, meta_from_row(data))
            self.series.setdefault(sid, ContractBars(sid))
            self.builders.setdefault(sid, BarBuilder(sid, day, self.tfs, session_close=era_for(day).fo_close))
            self.und_builders.setdefault(und, BarBuilder(und, day, (self.signal_tf,),
                                                        session_close=era_for(day).fo_close))
            target = squeeze_claims if row.scanner == "V2SQ" and hasattr(row, "rank") else claims
            target.extend(((subs.UNDERLYING_SEGMENT, und, "QUOTE"), (subs.OPTION_SEGMENT, sid, "FULL")))
        claims = subs.union(self.tracker.open_instruments(), claims)
        current = [(seg, sid, mode) for (seg, sid), mode in self.subscribed.items()]
        owner_claims = self.owner_ext.instruments(day) if self.owner_ext is not None else []
        if hub.v2 is not None:
            hub.v2.capacity(subs.union(current, claims), squeeze_claims, owner_claims, self._feed_cap())
        claims = subs.union(claims, owner_claims, squeeze_claims)
        missing = [x for x in claims if x[:2] not in self.subscribed]
        cap = self._feed_cap()
        fit, skipped = self._feed_fit(missing, max(0, cap - len(current))) if cap is not None else (missing, [])
        skipped_keys = set()
        for _, sid, _ in skipped:
            for row in hub.list_rows(day):
                if row.security_id == sid or row.underlying_security_id == sid:
                    skipped_keys.add(f"{row.list_name}|{row.security_id}")
        for key in sorted(skipped_keys):
            log.warning("s089 feed-cap skip %s", key)
        listed = hub.list_rows(day)

        def waits_for(items):
            return ({sid for seg, sid, _ in items if seg == subs.OPTION_SEGMENT} & {r.security_id for r in listed},
                    {sid for seg, sid, _ in items if seg == subs.UNDERLYING_SEGMENT}
                    & {r.underlying_security_id for r in listed})

        # T-646 (rule 4) / T-677 (finding 2): the waits are set from the clock BEFORE the feed take, so no bar closing
        # during add_instruments or any later await meets no wait. Ticks start only once the take has begun, so the
        # partial slot ends at or after this cutoff: the first bar closing at / after it is held and starts the
        # backfill, which seeds through its close (a later post-take cutoff would hold nothing more).
        pre_opts, pre_unds = waits_for(fit)
        self._089_set_waits(day, pre_opts, pre_unds)
        if fit and self.feed is not None:
            fit, _ = await self._feed_take(fit)
        for seg, sid, mode in fit:
            self.subscribed[seg, sid] = mode
            self._v1_sids.add(sid)
        new_opts, new_unds = waits_for(fit)
        for sid in pre_opts - new_opts:  # refused by the feed: never subscribed, no wait
            self._089_wait.pop(sid, None)
        for und in pre_unds - new_unds:
            self._089_und_wait.pop(und, None)
        unds = {r.underlying_security_id for r in added} | {p.underlying_security_id for p in active}
        await self._sw_h60_top_up(day, unds)
        self._sw_books_ensure(day, unds)
        if not added and not fresh_day:
            return
        now = self._wall_ms()
        sids = sorted({r.security_id for r in added} | {p.security_id for p in active})
        unseeded = [sid for sid in sids if not self.series[sid].closed()]
        if unseeded:
            await self._warmup_previous(day, unseeded)
        prior = {sid: [self._089_option(b) for b in self.series[sid].closed()
                       if b.ts_ms < session_bounds_ms(day)[0]] for sid in sids}
        adapter = self.adapter() if self._scanner_flag("warmup") is not False else None
        today, _ = await asyncio.to_thread(self._read_today, day, sids, now, adapter)
        stocks, previous_stocks = {}, {}
        for und in sorted(unds):
            candles = []
            if adapter is not None:
                try:
                    candles = await asyncio.to_thread(adapter.intraday, und, subs.UNDERLYING_SEGMENT,
                                                     5, cal.prev_trading_day(day), day, False, "EQUITY")
                except Exception:
                    log.exception("s089 stock seed unavailable %s", und)
            # D711 (SCAN-01 review F1): key each candle by its day's slot (a 09:16 first candle is the 09:15 bar),
            # then keep the slots closed by now.
            day_open = session_bounds_ms(day)[0]
            prev_open = session_bounds_ms(cal.prev_trading_day(day))[0]
            slotted = {**slot_candles([b for b in candles if b.ts_ms < day_open], prev_open, 300_000)[0],
                       **slot_candles([b for b in candles if b.ts_ms >= day_open], day_open, 300_000)[0]}
            closed = [(slot, b) for slot, b in sorted(slotted.items()) if slot + 300_000 <= now]
            previous_stocks[und] = [StockBar(slot, b.high, b.low, b.close, b.volume)
                                   for slot, b in closed if slot < day_open]
            stocks[und] = [StockBar(slot, b.high, b.low, b.close, b.volume)
                           for slot, b in closed if slot >= day_open]
            if hub.v2 is not None and previous_stocks[und]:
                hub.v2.engine.prev_day_hl[und] = (max(b.high for b in previous_stocks[und]),
                                                 min(b.low for b in previous_stocks[und]))
        options = {sid: [self._089_option(b) for b in bars] for sid, bars in today.bars.items()}
        for sid, bars in today.bars.items():
            for bar in bars:
                closed = self.series[sid].closed()
                if not closed or bar.start_ms > closed[-1].ts_ms:
                    engine_bar = self.carry.bar(sid, bar)
                    if engine_bar is not None:
                        self.series[sid].append(engine_bar)
        v1_added = [r for r in added if r.scanner == "V1N"]
        hub.seed(v1_added, prior, previous_stocks, options, stocks, now)
        if hub.v2 is not None:
            hub.v2.seed(late_start.build_seeds(options, sids, day, now), stocks)

    @staticmethod
    def _089_option(bar):
        from gammad.scanners.v1r.entry import OptBar

        return OptBar(getattr(bar, "start_ms", getattr(bar, "ts_ms", None)), bar.open, bar.high,
                      bar.low, bar.close, bar.volume, bar.oi)

    async def _089_backfill(self, sid, close_ms):
        from gammad.scanners import late_start

        try:
            await self._089_seeded.wait()  # T-677: never before the running prepare's own seed
            close_ms = self._089_closed_to(close_ms)
            adapter = self.adapter() if self._scanner_flag("warmup") is not False else None
            res, _ = await asyncio.to_thread(self._read_today, self.trade_date, [sid], close_ms, adapter)
            options = {sid: [self._089_option(b) for b in res.bars.get(sid, ())]}
            seeds = late_start.build_seeds(options, [sid], self.trade_date, close_ms)
            late_start.seed_v1n(self.s089.engine, seeds, lambda b: b)
            if self.s089.v2 is not None:
                self.s089.v2.seed(seeds, {})
        except Exception:
            log.exception("s089 backfill %s failed; entry runs on live bars", sid)
        finally:
            self._089_wait.pop(sid, None)

    def _089_closed_to(self, close_ms):
        """T-677 fix round 1 (F1): a bar that closed while the backfill waited was held (entry=False, never pushed to
        the engine), so the backfill reads to the end of the last 5-min slot CLOSED by now (capped at the session
        close), never less than its trigger's close_ms. Closed slots only: never the forming bar."""
        open_ms, end_ms = session_bounds_ms(self.trade_date)
        now = min(self._wall_ms(), end_ms)
        return max(close_ms, open_ms + max(0, now - open_ms) // 300_000 * 300_000)

    def _089_set_waits(self, day, sids, unds):
        cutoff = opening_cutoff_ms(self._wall_ms(), day)
        for sid in sids:
            self._089_wait[sid] = cutoff
        for und in unds:
            self._089_und_wait[und] = cutoff

    async def _089_und_backfill(self, und, cutoff, close_ms):
        from gammad.scanners.v1r.entry import StockBar

        try:
            await self._089_seeded.wait()  # T-677: never before the running prepare's own seed
            close_ms = self._089_closed_to(close_ms)
            adapter = self.adapter() if self._scanner_flag("warmup") is not False else None
            if adapter is not None:
                candles = await asyncio.to_thread(adapter.intraday, und, subs.UNDERLYING_SEGMENT, 5,
                                                 self.trade_date, self.trade_date, False, "EQUITY")
                for b in sorted(candles, key=lambda b: b.ts_ms):
                    if cutoff - 300_000 <= b.ts_ms and b.ts_ms + 300_000 <= close_ms:
                        self.s089.stock_bar(und, StockBar(b.ts_ms, b.high, b.low, b.close, b.volume))
        except Exception:
            log.exception("s089 stock backfill %s failed; entry runs on live bars", und)
        finally:
            self._089_und_wait.pop(und, None)

    async def run_s089_evening(self):
        return await self.s089.v2.evening(self._now())

    def _make_grader(self, cfg: Settings) -> Any:
        """W15 §6: the owner's scanner grader (gammad/scan). The client package ships no gammad/scan, so the import
        is guarded; a client Runtime never grades."""
        if not _edition.is_owner():
            return None
        try:
            from gammad.scan.grader import Grader
        except ImportError:  # CLIENT_EXCLUDE: the module is not in the package
            return None
        try:
            grader = Grader(self.conn, spec_version=cfg.spec_version)
            grader.load(self.conn)
        except Exception:  # a grader fault must never stop the supervisor
            log.exception("scanner grader unavailable; shadow signals are recorded but not graded")
            return None
        return grader

    def _make_owner_ext(self, cfg: Any) -> Any:
        """0.8.2 (C.6): the owner extension (gammad/scan/owner_hooks.py). The client package ships no gammad/scan, so
        the import is guarded like the grader's; any fault leaves the service as 0.8.1 had it (None)."""
        if not _edition.is_owner():
            return None
        try:
            from gammad.scan import owner_hooks
        except ImportError:  # CLIENT_EXCLUDE: the module is not in the package
            return None
        try:
            return owner_hooks.make(self, cfg)
        except Exception:  # an owner fault must never stop the supervisor
            log.exception("owner extension unavailable; the service runs without it")
            return None

    def _shadow_bar_close(
        self, sid: int, closed: Sequence[Any], oi_now: float, peak: float, *, recv_ms: int | None,
        tick_size: float | None,
    ) -> None:
        """Feed one closed signal-tf bar to every shadow scanner's engine; their signals carry `scanner=`."""
        for name, pool in self.shadow_pools.items():
            try:
                sig = pool.get_or_create(sid, self._list_of(sid)).on_bar_close(
                    closed, oi_now, peak, recv_ms=recv_ms, tick_size=tick_size,
                    arm_ctx=arm_context(self.rows.get(sid), closed[-1].ts_ms, self.last_ltp),  # T-160: V4 only
                )
            except Exception:  # a shadow scanner bug must never cost the live scanner's bar
                log.exception("shadow scanner %s bar close failed for sec %s", name, sid)
                continue
            if sig is not None:
                if name == V2T:  # T-267: the regime + the 1h stop zone + the stock price go in BEFORE the router
                    sig = self._v2t_enrich(sig)
                self._dispatch(sig, oi_now=oi_now, oi_peak=peak, scanner=name)

    def _list_of(self, sid: int) -> str | None:
        """The contract's watchlist list_name. S1.2.0: an engine created late (e.g. after the close_day pool reset)
        must still get its hold style's params, never silently the base ones."""
        row = self.rows.get(sid)
        return None if row is None else str(row["list_name"])

    # ---------------------------------------------------------------- T-419 "Still Watching" (spec 4.8, both editions)
    # D471 (1) / D476 (1) / D480. The CARRY rows of the 09:20 carry (gammad/jobs/carry.py) run a CarryEngine (engine.py:
    # the multi-day OI peak, relax-C 2.0 x ATR on the 8 % unwind bar, U-TODAY). Its E2 reaches the router only through
    # _sw_gate (engine.sw_entry_block: the day's F&O ban list, fail closed; ST-PREV; the 15:20 cut; one entry per option);
    # the router's tier, R4 (one entry per stock-day across listed + carried) and the tracker's trade follow. Every input
    # is fetched in the background and FAILS CLOSED (no ban list / no regime = no entry). The OWNER also runs the
    # CARRY-ANY3 shadow (PE rows only: D474 "calls must never get a window rule"), recorded, never sent.

    def _sw_prime(self, sid: int, row: Mapping[str, Any]) -> None:
        """A CARRY row -> its pool engine is a CarryEngine with watchlist.oi_peak_prior (schema 11; absent / NULL ->
        today's peak only). Called before the engine is created (load_day)."""
        if not self.sw.enabled or str(row.get("tag") or "") != CARRY_TAG:
            return
        raw = row.get("oi_peak_prior")
        try:
            prior = float(raw) if raw is not None else None
        except (TypeError, ValueError):
            prior = None
        prior = prior if prior is not None and math.isfinite(prior) and prior > 0 else None
        self.pool.set_carry(sid, prior)
        if self.sw_any3_pool is not None and str(row.get("option_type") or "").upper() == "PE":
            self.sw_any3_pool.set_carry(sid, prior)

    def _sw_carried_unds(self) -> set[int]:
        out: set[int] = set()
        for sid in self.pool.carry_ids():
            und = (self.rows.get(sid) or {}).get("underlying_security_id")
            if und is not None:
                out.add(int(und))
        return out

    def _sw_ban_ok(self, d: date) -> bool:
        ban = self.sw_ban
        return ban is not None and ban.date == d and ban.ok

    def _sw_day_inputs(self, d: date) -> None:
        """After load_day: today's Still Watching inputs, each in the background, only when a CARRY row is watched: the
        options already entered (trades), the ban list (when today's is not read yet: a start after 08:45; the 08:45 job
        is the daily fetch), ST-PREV per carried underlying, and the 1-hour zone top-up of the carried underlyings."""
        if not self.sw.enabled or self.trade_date != d:
            return
        unds = self._sw_carried_unds()
        if not unds:
            return
        try:
            self._sw_entered = entered_options(self.conn)
        except sqlite3.Error:
            log.exception("Still Watching: entered options unreadable; the previous set stays")
        if self.sw.ban_enabled and not self._sw_ban_ok(d) and self._sw_ban_tried != d:
            self._sw_ban_tried = d  # one start-time fetch a day; the scheduled job is the other
            self.spawn(self.run_sw_ban_job(d), name="sw-ban-list")
        self.spawn(self._sw_regime_step(d, unds), name="sw-regime")
        self.spawn(self._sw_zones_for_day(d, unds), name="sw-h60-top-up")

    async def _sw_zones_for_day(self, d: date, unds: set[int]) -> None:
        """D480 (3): top the 60-min store up for the carried underlyings, THEN turn it into their 1h zone books (seed +
        the store's rows after its as_of + today's closed hours). The books open even when the top-up failed / was
        skipped (whatever the store holds; a book that misses hours picks from older zones, logged by the top-up)."""
        try:
            await self._sw_h60_top_up(d, unds)
        finally:
            if self.trade_date == d:
                self._sw_books_ensure(d, unds)

    def _sw_ban_fetch(self, d: date) -> Any:
        """The blocking fetch (a worker thread); tests replace it."""
        from gammad.jobs.ban_list import fetch_ban_list

        return fetch_ban_list(d)

    async def run_sw_ban_job(self, d: date | None = None) -> dict[str, Any] | None:
        """still_watching.ban_list.fetch_hm (08:45 IST) on a trading day, both editions: NSE fo_secban_<D> for trade
        date D (gammad/jobs/ban_list.py; one request + one retry; failure = UNKNOWN = no Still Watching entry today).
        A day that already holds an OK list is not fetched again."""
        if not (getattr(self, "scanners_089", False) or (self.sw.enabled and self.sw.ban_enabled)):
            return None
        d = d or self.today()
        if not self.is_trading_day(d):
            return None
        if self._sw_ban_ok(d):
            await self._089_fetch_results(d)
            return dict(self.sw_ban.to_dict())
        try:
            res = await asyncio.to_thread(self._sw_ban_fetch, d)
        except Exception:  # a fetch fault is an UNKNOWN list (fail closed), never the job
            log.exception("Still Watching: ban list %s not read", d)
            await self._089_fetch_results(d)
            return None
        if not self._sw_ban_ok(d):
            self.sw_ban = res
        await self._089_fetch_results(d)
        return dict(res.to_dict())

    async def _089_fetch_results(self, day):
        if self.s089 is None or self._089_results_day == day:
            return
        from gammad.jobs.results_calendar import fetch_results_calendar

        self._089_results = await asyncio.to_thread(
            fetch_results_calendar, day, self.data_dir / "results_calendar.json")
        self._089_results_day = day
        self.s089.engine.results = self._089_results
        if self.s089.v2 is not None and self.s089.v2.engine is not None:
            self.s089.v2.engine.results = self._089_results

    async def _sw_regime_step(self, d: date, unds: set[int]) -> None:
        """ST-PREV per carried underlying for trade date d (engine.sw_regime_prev on the broker's daily bars before d).
        OWNER: today's V2 regime is the same rule on the same bars (v2scan/regime.py) and is reused. A failure leaves
        the underlying unknown (no entry) and is retried at the next load_day."""
        if self.sw_regime_day != d:
            self.sw_regimes, self.sw_regime_day = {}, d
        todo = sorted(u for u in unds if u not in self.sw_regimes and u not in self._sw_regime_pending)
        if todo and self.v2_regime_day == d:
            for u in list(todo):
                reg = self.v2_regimes.get(u)
                if reg is not None and reg.st_dir is not None:
                    self.sw_regimes[u] = int(reg.st_dir)
                    todo.remove(u)
        if not todo:
            return
        adapter = self.adapter()
        if adapter is None:
            log.warning("Still Watching: no broker adapter; %d stock(s) have no Supertrend side today", len(todo))
            return
        self._sw_regime_pending.update(todo)
        try:
            got = await asyncio.to_thread(self._sw_regime_fetch, adapter, todo, d)
        except Exception:  # fail closed: no regime, no entry
            log.exception("Still Watching: Supertrend side not computed for %d stock(s)", len(todo))
            got = {}
        finally:
            self._sw_regime_pending.difference_update(todo)
        if self.sw_regime_day == d:
            self.sw_regimes.update(got)

    def _sw_regime_fetch(self, adapter: BrokerAdapter, usids: Sequence[int], d: date) -> dict[int, int]:
        """Blocking (worker thread): {usid: +1 / -1} for the underlyings whose ST-PREV is known; the rest are absent."""
        from gammad.jobs.mark_levels import fetch_bars

        prev = self._prev_trading_day(d)
        out: dict[int, int] = {}
        for u in usids:
            try:
                bars = fetch_bars(adapter, int(u), d, "ltp", SW_DAILY_BARS)
            except BrokerAuthError as exc:
                log.warning("Still Watching: daily bars refused by the broker (%s); no Supertrend side", exc)
                break
            except Exception as exc:  # noqa: BLE001 -- one stock's fault is that stock's unknown side
                log.warning("Still Watching: daily bars of %s not read (%s)", u, type(exc).__name__)
                continue
            st, why = sw_regime_prev(bars, d, prev)
            if st is None:
                log.warning("Still Watching: %s has no Supertrend side today (%s)", u, why)
                continue
            out[int(u)] = st
        return out

    async def _sw_h60_top_up(self, d: date, unds: set[int]) -> None:
        """D480 (3): the 1-hour zone top-up (gammad.jobs.h60_sync.top_up, builder B1) for the carried underlyings not
        yet topped up today. Skipped (logged) when the module or its top_up is absent from this build."""
        day, done = self._sw_h60_done
        if day != d:
            done = set()
            self._sw_h60_done = (d, done)
        todo = sorted(set(unds) - done)
        if not todo:
            return
        try:
            from gammad.jobs import h60_sync
        except ImportError:
            log.info("Still Watching: no 1-hour zone sync in this build; zones are not topped up")
            return
        top_up = getattr(h60_sync, "top_up", None)
        if top_up is None:
            log.info("Still Watching: h60_sync.top_up is not in this build yet; zones are not topped up")
            return
        adapter = self.adapter()
        through = self._prev_trading_day(d)
        if adapter is None or through is None:
            return
        done.update(todo)
        try:
            rep = await asyncio.to_thread(top_up, todo, adapter, through=through, settings=self.cfg,
                                          params=self._zone_params())
        except Exception:  # the zones stay as they are; retried at the next load_day
            log.exception("Still Watching: 1-hour zone top-up failed for %d stock(s)", len(todo))
            done.difference_update(todo)
            return
        line = rep.line() if hasattr(rep, "line") else str(rep)
        log.info("Still Watching: 1-hour zones topped up through %s for %d stock(s): %s", through, len(todo), line)

    def _sw_gate(self, sid: int, sig: Signal, *, shadow: bool = False) -> Signal | None:
        """A carried row's E2 -> the signal with snapshot.sw.gate (the inputs and the verdict), or None when
        engine.sw_entry_block refuses it (never dispatched: no row, no card, no R4 slot). `shadow` = annotate only."""
        row = self.rows.get(sid) or {}
        d = self.trade_date
        if self.v1r is not None:
            side = str(row.get("option_type") or "")
            if side not in ("PE", "CE") or not self.v1r.carry_ready(side):
                self.v1desk.note_pending(side, d)
                return None
        und = row.get("underlying_security_id")
        regime = self.sw_regimes.get(int(und)) if und is not None and self.sw_regime_day == d else None
        if self.sw.ban_enabled:
            ban = self.sw_ban if (self.sw_ban is not None and self.sw_ban.date == d) else None
            status = str(ban.status) if ban is not None else SW_BAN_UNKNOWN
            banned = bool(ban is not None and ban.ok and ban.banned(str(row.get("symbol") or "")))
        else:
            status, banned = SW_BAN_OK, False
        entered = (sid, str(row.get("expiry_date") or "")[:10]) in self._sw_entered
        why = sw_entry_block(str(row.get("option_type") or ""), int(sig.ts_ms), regime_prev=regime, ban_status=status,
                             banned=banned, entered=entered, cutoff_hm=self.sw.entry_cutoff_hm)  # fmt: skip
        snap = dict(sig.snapshot or {})
        sw = dict(snap.get("sw") or {})
        # T-419: the entry's stock levels, picked ONCE here (tracker.sw_entry_levels): the card prints this block and the
        # tracker opens the trade from it. No 1h stop zone = no entry (STRATEGY-SPEC 4.8, "no zone = no entry")
        lv = self._sw_levels(sid, sig) if why is None or shadow else None
        if lv is not None:
            sw.update(lv.to_snapshot())
        elif why is None:
            why = SW_NO_ZONE
        sw["gate"] = {"block": why, "regime_prev": regime, "ban": status, "banned": banned}
        snap["sw"] = sw
        sig = replace(sig, snapshot=snap)
        if why is None or shadow:
            return sig
        (log.debug if _edition.is_client() else log.info)(
            "Still Watching: sec %s signal at %s is not an entry (%s)", sid,
            datetime.fromtimestamp(sig.ts_ms / 1000, IST).strftime("%H:%M"), why)  # fmt: skip
        return None

    def _sw_levels(self, sid: int, sig: Signal) -> Any:
        """tracker.sw_entry_levels for a carried row's entry signal (zx_levels.levels_detail): reference price = the
        stock's 5-min bar at / before the signal bar (sw_ref_px; else ctx-free fallback: the underlying's last LTP), the
        1h book (sw_books, fed through the hour this signal closes), the daily zones of today's levels row. None = no
        zone (or no price) = no entry."""
        row = self.rows.get(sid) or {}
        und = row.get("underlying_security_id")
        ot = str(row.get("option_type") or "").upper()
        if und is None or ot not in ("CE", "PE") or self.trade_date is None:
            return None
        und = int(und)
        snap = sig.snapshot if isinstance(sig.snapshot, Mapping) else {}
        bar_open = snap.get("bar_ts_ms")
        bar_open = int(bar_open) if isinstance(bar_open, (int, float)) else int(sig.ts_ms) - self.signal_tf * MINUTE_MS
        # ZONE ORDER (zx_levels.py:36 "Entry time for the 1h book = trade['signal_ms'] (= bar_ts + 5 min, the signal bar's
        # close)" + :18-19 "stop zone = H1ZoneBook.stop_zone ... on the adjusted 60-min bars CLOSED at / before the entry
        # (signal close)"; h1_book :233-236 appends every h60 row with close_ms <= t_entry): the hour CLOSING AT the
        # signal close counts. Its last 5-min stock bar ends exactly then; D295 (_close_und_due on a tick-closed option bar,
        # on_timer's underlyings-first order on a timer-closed one) has already closed every stock bar whose slot ENDED
        # by the signal close before this runs (never one still open: no look-ahead), so the reference price and that
        # hour are in at the pick (tests/test_tracker_still_watching.py::test_app_pulls_the_stock_bar_..., P10).
        px = sw_ref_px(self.und_bars.get(und) or (), bar_open, self.last_ltp.get(und))
        self._sw_h1_feed(und)  # the hour closing at this signal's close counts (its stock bar is in now)
        book = self.sw_books.get(und) or self.h1_books.get(und)
        try:
            daily = sw_daily_zones(self.conn, self.trade_date.isoformat(), str(row.get("symbol") or ""))
            lv = sw_entry_levels(ot, px, int(sig.ts_ms), book, daily)
        except Exception:  # a level bug is a refused entry, never a lost bar
            log.exception("Still Watching: levels of sec %s not picked; no entry", sid)
            return None
        if lv is None:
            log.info("Still Watching: sec %s has no 1-hour stop zone at %s (book %s, price %s); no entry", sid,
                     datetime.fromtimestamp(sig.ts_ms / 1000, IST).strftime("%H:%M"),
                     "none" if book is None else "ok", px)  # fmt: skip
        return lv

    # -- T-419: the Still Watching 1h books (both editions) + the tracker's stock bars
    def _sw_h1_feed(self, und: int) -> int:
        """Append to sw_books[und] every hour today's closed 5-min stock bars complete that it does not hold yet (the
        _h1_feed rule: hour_bars_from_5m, an hour counts once a bar ending at / after its close has closed). No trail
        steps (a Still Watching stop is fixed at entry). Returns hours added."""
        book = self.sw_books.get(und)
        d = self.trade_date
        bars = self.und_bars.get(und)
        if book is None or d is None or not bars or und in self._h1_wait:
            return 0
        from gammad.core import h1zones as core_h1

        last_end = int(bars[-1].ts_ms) + self.signal_tf * MINUTE_MS
        n = 0
        for r in core_h1.hour_bars_from_5m(bars, d, now_ms=last_end):
            if book.last_close_ms is not None and int(r[5]) <= book.last_close_ms:
                continue
            try:
                book.append(r)
            except Exception:
                log.exception("Still Watching: 1-hour book of %s: hour %s not appended", und, r[0])
                return n
            n += 1
        return n

    def _sw_books_ensure(self, d: date, unds: Iterable[int]) -> None:
        """Open (in the background) the 1h book of every underlying in `unds` without one: the shipped zone seed + the
        60-min store's rows after its as_of (gammad.core.h1zones.open_books; the store is topped up by _sw_h60_top_up)."""
        if not self.sw.enabled:
            return
        from gammad.core import h1zones as core_h1

        prev = self._prev_trading_day(d)
        floor = core_h1.ist_ms(prev, "15:00") if prev is not None else None

        def stale(u: int) -> bool:  # a book that does not hold the previous session (its stock was not watched then)
            b = self.sw_books.get(u)
            return b is None or (floor is not None and (b.last_close_ms is None or b.last_close_ms < floor))

        want = sorted(int(u) for u in unds if stale(int(u)) and int(u) not in self._sw_books_pending)
        if not want:
            return
        self._sw_books_pending.update(want)
        self.spawn(self._sw_books_open(d, want), name="sw-h1-books")

    def _sw_books_open_blocking(self, d: date, usids: list[int]) -> tuple[dict[int, Any], Any]:
        from gammad.core import h1zones as core_h1
        from gammad.core.h60_store import H60Store as CoreH60Store
        from gammad.jobs.h60_sync import history_root

        store = CoreH60Store(history_root(self.cfg))
        through = d - timedelta(days=1)
        params = self._zone_params()  # T-422, CX-81
        seed = core_h1.load_seed(params=params)
        return core_h1.open_books(usids, lambda u, after: store.rows_after(u, after, through), seed, params=params)

    async def _sw_books_open(self, d: date, usids: list[int]) -> int:
        try:
            books, rep = await asyncio.to_thread(self._sw_books_open_blocking, d, usids)
        except asyncio.CancelledError:
            self._sw_books_pending.difference_update(usids)
            raise
        except Exception:
            log.exception("Still Watching: %d 1-hour books not opened; their entries have no zone", len(usids))
            self._sw_books_pending.difference_update(usids)
            return 0
        self._sw_books_pending.difference_update(usids)
        for u, b in books.items():
            self.sw_books[int(u)] = b
            if self.trade_date == d:
                self._sw_h1_feed(int(u))
        log.info("Still Watching: 1-hour books %s: %d opened (seed %d, full pass %d, none %d)", d, len(books),
                 len(rep.seeded), len(rep.full_pass), len(rep.missing))  # fmt: skip
        return len(books)

    def _sw_on_trade_open(self, und: int) -> None:
        """tracker.on_sw_open: a Still Watching trade opened on `und` -> its 5-min bars (previous session -> now) feed
        the tracker's ATR (the X9A trail's ATR14 of completed 15-min bars needs ~3.5 h of history)."""
        d = self.trade_date
        if d is not None:
            self.spawn(self._sw_seed_bars(d, [int(und)]), name="sw-seed-bars")

    async def _sw_seed_bars(self, d: date, unds: Sequence[int], *, force: bool = False) -> int:
        """Re-hydration / ATR warm-up / RESTART RE-WALK of the open Still Watching trades: per underlying, the broker's
        5-min stock bars from the previous session (or the earliest open trade's target touch / last walked bar,
        trades.sw_bar_ms, when earlier) through now, COMPLETED bars only, handed to tracker.sw_seed_bars (ATR, the extreme
        since the touch, then every bar after the last walked one through zone_exit.on_bar in order: a downtime stop /
        target / trail fires once, marked late; the live bars held meanwhile follow). No broker / a failed fetch:
        tracker.sw_release (the held live bars walk on; the downtime bars are logged as not walked). Once per underlying
        per day unless `force`. The recorder keeps no STOCK 5-min bars (bars-5m.parquet holds the options' only), so the
        broker's intraday history is the bar source."""
        day, done = self._sw_bars_seeded
        if day != d:
            done = set()
            self._sw_bars_seeded = (d, done)
        todo = sorted(int(u) for u in unds if force or int(u) not in done)
        if not todo:
            return 0
        adapter = self.adapter()
        if adapter is None:
            for und in todo:
                self.tracker.sw_release(und)
            return 0
        done.update(todo)
        prev = self._prev_trading_day(d) or d
        now_ms = self._wall_ms()
        span = self.signal_tf * MINUTE_MS
        n = 0
        for i, und in enumerate(todo):
            marks = self.tracker.sw_touched_ms(und)
            start = min([prev, *(datetime.fromtimestamp(ms / 1000, IST).date() for ms in marks)])
            try:
                candles = await asyncio.to_thread(adapter.intraday, und, UNDERLYING_SEGMENT, self.signal_tf, start, d,
                                                  oi=False, instrument=UNDERLYING_INSTRUMENT)  # fmt: skip
            except BrokerAuthError as exc:
                log.error("Still Watching: 5-min bars refused by the broker (%s); the trail's ATR builds live", exc)
                done.difference_update(todo)
                for u in todo[i:]:
                    self.tracker.sw_release(u)
                return n
            except Exception as exc:  # noqa: BLE001 -- that stock's ATR / extreme build from the live bars only
                log.warning("Still Watching: 5-min bars of %s not read (%s); its ATR builds live", und,
                            type(exc).__name__)  # fmt: skip
                done.discard(und)
                self.tracker.sw_release(und)
                continue
            closed = [c for c in candles if int(c.ts_ms) + span <= now_ms]
            n += self.tracker.sw_seed_bars(und, closed)
        return n

    def _sw_any3_bar_close(self, sid: int, closed: Sequence[Any], oi_now: float, peak: float, *,
                           recv_ms: int | None, tick_size: float | None) -> None:  # fmt: skip
        """The OWNER's CARRY-ANY3 shadow on one closed bar: an E2 is recorded under its own scanner name (router
        _record_shadow: no card, no trade, no R4), with the Still Watching gate facts annotated, never applied."""
        pool = self.sw_any3_pool
        if pool is None:
            return
        try:
            sig = pool.get_or_create(sid, self._list_of(sid)).on_bar_close(
                closed, oi_now, peak, recv_ms=recv_ms, tick_size=tick_size)  # fmt: skip
        except Exception:  # a shadow bug must never cost the live scanner's bar
            log.exception("shadow scanner %s bar close failed for sec %s", SW_ANY3_SCANNER, sid)
            return
        if sig is None or str(sig.kind) != "E2":
            return
        gated = self._sw_gate(sid, sig, shadow=True)
        if gated is not None:
            self._dispatch(gated, oi_now=oi_now, oi_peak=peak, scanner=SW_ANY3_SCANNER)

    def _route_tick(
        self, sid: int, tick: Tick, builder: BarBuilder, *, pool: EnginePool | None = None, scanner: str = "V1"
    ) -> None:
        self.stats.engine_tick_calls += 1
        eng = (pool or self.pool).get_or_create(sid, self._list_of(sid))
        tv = self.carry.tick(tick)
        forming = builder.forming_bar(self.signal_tf)
        if tv is None or forming is None:
            return
        fb = self.carry.bar(sid, forming)
        if fb is None:
            return
        oi_now, peak = self._oi(sid, builder, tv.oi)
        sig = eng.on_tick(tv, fb, self.series[sid].closed(), oi_now, peak)
        if sig is not None:
            self._dispatch(sig, oi_now=oi_now, oi_peak=peak, scanner=scanner)

    def _oi(self, sid: int, builder: BarBuilder, fallback: float) -> tuple[float, float]:
        """(oi_now, peak) for an engine call. T-585 (CX-158 §1 C): the peak is today's BAR-CLOSE peak
        (warmup.bar_close_peak over the closed bars, 0.0 before the first), never the tick max BarBuilder keeps
        (oi_peak_today stays in the pinned bars.py, D343, read by no decision)."""
        oi_now = float(builder.oi_now) if builder.oi_now is not None else fallback
        return oi_now, self._bar_close_peak(sid) or 0.0

    def _bar_close_peak(self, sid: int) -> float | None:
        """warmup.bar_close_peak(self.series[sid].closed(), self.trade_date), memoised per closed-bar count: the tick
        path asks once per tick and the answer only changes when a bar closes (the seed never counts)."""
        series, d = self.series.get(sid), getattr(self, "trade_date", None)
        if series is None or d is None:
            return None
        closed = series.closed()
        key = (d, id(series), series.n_today, closed[-1].ts_ms if closed else None)
        hit = self._peak_memo.get(sid)
        if hit is not None and hit[0] == key:
            return hit[1]
        peak = bar_close_peak(closed, d)
        self._peak_memo[sid] = (key, peak)
        return peak

    def on_timer(self, now_ms: int | None = None) -> int:
        """Close bars whose end + grace has passed (a contract that stopped trading). Returns closes."""
        now_ms = self._wall_ms() if now_ms is None else now_ms
        n = 0
        # T-267: the underlyings first, so a timer-closed option bar's V2T entry reads its stock bar of the same slot
        day = getattr(self, "trade_date", None) or datetime.fromtimestamp(now_ms / 1000, IST).date()
        clock_cut = now_ms // 1000 == v1_hours.last_bar_ms(day) // 1000
        for ub in list(self.und_builders.values()):
            for bc in ub.on_timer(now_ms + (ub.timer_grace_ms if clock_cut else 0)):
                self._on_und_close(bc)
        for builder in list(self.builders.values()):
            for bc in builder.on_timer(now_ms + (builder.timer_grace_ms if clock_cut else 0)):
                n += 1
                self._on_bar_close(bc, recv_ms=None)
        # T-419: the Still Watching expiry-eve exit (still_watching.eve_exit_hm, 15:15 IST on the session before expiry)
        # rides the bar timer: no extra job id, and a late wake still exits (on_clock fires at / after the deadline)
        try:
            self.tracker.sw_clock(now_ms)
        except Exception:
            log.exception("trade tracker sw_clock failed")
        self.tracker.v1r_clock(now_ms)
        return n

    def timer_delay_s(self, now_ms: int | None = None) -> float:
        """Seconds until the bar timer must run again (PLAN-W12 §2 item 5): the next 09:15-aligned end of the
        shortest timeframe + the builders' grace (bars.next_close_deadline), so a timer-closed 5-min bar closes
        <= grace after its end. No live day / after the session: the idle poll."""
        now_ms = self._wall_ms() if now_ms is None else now_ms
        d = self.trade_date
        if d is None or not (self.builders or self.und_builders):
            return TIMER_EVERY_S
        deadline = next_close_deadline(now_ms, d, min(self.tfs), DEFAULT_TIMER_GRACE_MS,
                                       session_close=era_for(d).fo_close)
        if self.tracker.v1r_ids:
            close_at = v1_hours.last_bar_ms(d)
            if now_ms < close_at:
                deadline = min(deadline or close_at, close_at)
        if deadline is None:
            return TIMER_EVERY_S
        return min(TIMER_MAX_S, max(TIMER_MIN_S, (deadline - now_ms) / 1000.0))

    def _on_und_close(self, bc: BarClose) -> None:
        """An underlying's closed 5-min bar -> today's und_bars (Candle, ts_ms = bar open); never an engine.
        T-267: then the open V2T trades on it meet the bar (tracker.on_stock_bar: ZSL / ZTRAIL), then its 1h book
        takes every hour this close completed (_h1_feed -> tracker.on_hour_close: trail steps)."""
        if bc.tf_min != self.signal_tf:
            return
        b = bc.bar
        bars = self.und_bars.setdefault(bc.security_id, [])
        if bars and bars[-1].ts_ms >= b.start_ms:
            return
        candle = Candle(int(b.start_ms), float(b.open), float(b.high), float(b.low), float(b.close),
                        float(b.volume), None)
        bars.append(candle)
        if self.s089 is not None:
            from gammad.scanners.v1r.entry import StockBar

            wait = self._089_und_wait.get(bc.security_id)
            if wait is None:
                self.s089.stock_bar(bc.security_id, StockBar(b.start_ms, b.high, b.low, b.close, b.volume))
            elif wait != -1 and b.end_ms >= wait:
                self._089_und_wait[bc.security_id] = -1
                self.spawn(self._089_und_backfill(bc.security_id, wait, b.end_ms),
                           name="s089-stock-backfill")
        try:
            self.tracker.on_stock_bar(bc.security_id, candle, None)
        except Exception:  # a tracker bug must never cost the bars
            log.exception("trade tracker on_stock_bar failed for underlying %s", bc.security_id)
        # T-419: the open Still Watching trades on it walk this COMPLETED bar (SW_STOP / SW_TARGET / SW_TRAIL; the
        # option close = its last LTP), then the stock's 1h book takes any hour this close completed
        try:
            self.tracker.on_sw_stock_bar(bc.security_id, candle, None)
        except Exception:  # a tracker bug must never cost the bars
            log.exception("trade tracker on_sw_stock_bar failed for underlying %s", bc.security_id)
        self._sw_h1_feed(bc.security_id)
        self._h1_feed(bc.security_id)

    def _on_bar_close(self, bc: BarClose, *, recv_ms: int | None) -> None:
        b = bc.bar
        sid = bc.security_id
        self.hub.publish(
            "bar",
            {
                "security_id": sid,
                "tf": bc.tf_min,
                "ts_ms": b.start_ms,
                "open": b.open,
                "high": b.high,
                "low": b.low,
                "close": b.close,
                "volume": b.volume,
                "oi": b.oi,
                "reason": bc.reason,
            },
        )
        if bc.tf_min != self.signal_tf:
            return
        self.stats.bar_closes_5m += 1
        eb = self.carry.bar(sid, b)
        series = self.series.get(sid)
        if eb is None or series is None:
            self._record_bar(sid, b, None)
            return
        series.append(eb)
        if self.s089 is not None and self.engines_allowed:
            from gammad.scanners.exits import LadderBar
            from gammad.scanners.v1r.entry import atr_series

            wait = self._089_wait.get(sid)
            if wait is not None and wait != -1 and b.end_ms >= wait:
                self._089_wait[sid] = -1
                self.spawn(self._089_backfill(sid, b.end_ms), name="s089-backfill")
            und = self.meta[sid].underlying_security_id
            stocks = tuple(c.close for c in self.und_bars.get(und, ()) if c.ts_ms == b.start_ms)
            atr = atr_series([self._089_option(x) for x in series.closed()], self.s089.engine.p)
            self.s089.option_bar(sid, self._089_option(b), LadderBar(
                b.start_ms, b.open, b.high, b.low, b.close, b.oi,
                atr14=atr[-1] if atr else None, stock_closes=stocks), entry=wait is None)
        if not self.engines_allowed:  # BLOCKED broker: bars for the dashboard, no engine, no signal
            self._record_bar(sid, b, None)
            return
        builder = self.builders[sid]
        # T-585 (CX-158 §1 C): the bar just appended IS in closed(), so the peak includes this close. The carry
        # contracts' former bar_max_oi line is this same rule (bar_close_peak wraps it), now for every contract.
        oi_now, peak = self._oi(sid, builder, eb.oi)
        from gammad.v1.carry_break import V1BreakCarryEngine, bar_array

        closed = series.closed()
        self._record_bar(sid, b, (oi_now, peak))  # T-102: exactly what the engine is handed below
        meta = self.meta.get(sid)
        tick_size = meta.tick_size if meta is not None else None
        und = meta.underlying_security_id if meta else None
        stock = next((x for x in reversed(self.und_bars.get(und, ())) if x.ts_ms == eb.ts_ms), None)
        if eb.ts_ms >= v1_hours.cash_close_ms(self.trade_date):
            self.tracker.on_v1r_slot(und, None, bar_array(eb), sid=sid, h1_closes=())
        elif stock is not None:
            from gammad.core.h1zones import hour_bars_from_5m

            hours = tuple(h for h in hour_bars_from_5m(self.und_bars[und], self.trade_date, now_ms=b.end_ms)
                          if b.start_ms < h[5] <= b.end_ms)
            self.tracker.on_v1r_slot(und, bar_array(stock), bar_array(eb), sid=sid, h1_closes=hours)
        if self.v1desk is not None and not self.pool.is_carry(sid):
            self.v1desk.decide(sid, closed)
            sig = None
        else:
            eng = self.pool.get_or_create(sid, self._list_of(sid))
            if isinstance(eng, V1BreakCarryEngine):
                eng.configure(self.v1r, self.rows.get(sid, {}),
                              stock_seed=self.v1desk.stock_seed.get((self.trade_date, und), ()),
                              nifty=self.v1desk.nifty.get(self.trade_date, ()))
            extra = {"und_bars": self.und_bars.get(und, ())} if isinstance(eng, V1BreakCarryEngine) else {}
            sig = eng.on_bar_close(
                closed, oi_now, peak, recv_ms=recv_ms, tick_size=tick_size,
                arm_ctx=arm_context(self.rows.get(sid), closed[-1].ts_ms, self.last_ltp), **extra,
            )
        if sig is not None and self.pool.is_carry(sid):  # T-419: a carried row's E2 is an entry only through the gate
            sig = self._sw_gate(sid, sig)
        if sig is not None:
            self._dispatch(sig, oi_now=oi_now, oi_peak=peak)
        if self.sw_any3_pool is not None and self.sw_any3_pool.is_carry(sid):
            self._sw_any3_bar_close(sid, closed, oi_now, peak, recv_ms=recv_ms, tick_size=tick_size)
        if self.shadow_pools:
            self._shadow_bar_close(sid, closed, oi_now, peak, recv_ms=recv_ms, tick_size=tick_size)

    def _record_bar(self, sid: int, bar: Any, inputs: tuple[float, float] | None) -> None:
        """A closed signal-tf bar to the recorder. T-102: with the (oi_now, oi_peak) the engine was handed at this
        close, so a restart replays every bar with live's own inputs; only a recorder that declares
        `records_engine_inputs` gets them (a test / tool fake keeps the two-argument call)."""
        rec = self.recorder
        if rec is None:
            return
        if inputs is not None and getattr(rec, "records_engine_inputs", False):
            rec.submit_bar(sid, bar, eng_oi_now=inputs[0], eng_oi_peak=inputs[1])  # type: ignore[call-arg]
        else:
            rec.submit_bar(sid, bar)

    def signal_context(
        self, sig: Signal, meta: ContractMeta, *, oi_now: float | None, oi_peak: float | None, rules: str = "V1"
    ) -> SignalContext:
        """S1.1.0 context at signal time (D58/D60/D63). spot = the underlying's last live tick (QUOTE, D28);
        volumes = the contract's closed 5-min series, D24 seed included (ends at the signal bar for E2, at
        the last closed bar for the tick-driven E1); oi = exactly what the engine was handed. Gates and hold
        are read from the CURRENT settings object, never cached at start-up."""
        und = meta.underlying_security_id
        spot = self.last_ltp.get(und) if und is not None else None
        series = self.series.get(sig.security_id)
        volumes = [b.volume for b in series.closed()] if series is not None else []
        und_bars = list(self.und_bars.get(und, ())) if und is not None else []
        cfg = self.cfg
        return build_context(
            meta, spot=spot, volumes=volumes, oi_peak=oi_peak, oi_now=oi_now, gates=cfg.gates, hold=cfg.hold,
            und_bars=und_bars, snapshot=sig.snapshot, signal_ts_ms=sig.ts_ms, rules=rules,
        )  # fmt: skip

    def _dispatch(
        self, sig: Signal, *, oi_now: float | None = None, oi_peak: float | None = None, scanner: str = "V1",
        catch_up: bool = False,
    ) -> None:
        self.stats.signals += 1
        if not catch_up:  # perf B1: a restart catch-up's skipped bar, evaluated late, is not a live-path latency
            self.latencies.append(int(sig.latency_ms))
            self.last_latency_ms = int(sig.latency_ms)
        meta = self.meta.get(sig.security_id)
        client = _edition.is_client()
        if meta is None:
            if not client:
                log.error("signal for sec %s with no watchlist meta; dropped: %s", sig.security_id, sig)
            elif sig.kind in _edition.visible_kinds():  # CLIENT §2: an internal arm is never logged
                # T-107 (D86/D99 class): this entry was never sent, so /api/logs must not name it (no sec, no kind)
                log.warning("signal with no watchlist meta; dropped")
            return
        poll_s = self._scanner_flag("feed_poll_s")
        if poll_s is not None:  # DEGRADED: OI/volume polled -- recorded with the signal (payload_json.snapshot)
            sig = replace(sig, snapshot={**(sig.snapshot or {}), "feed_poll_s": poll_s})
        ctx: SignalContext | None
        try:
            # S1.5.0 (D161(3)): each scanner's own gate rules (V1: D1 + dynamic zone; V2: S1.4.0; V1 B: options-only)
            rules = next((s.rules for s in self.scanners if s.name == scanner), "V1")
            ctx = self.signal_context(sig, meta, oi_now=oi_now, oi_peak=oi_peak, rules=rules)
            if isinstance(sig.snapshot, dict) and "v1r" in sig.snapshot:
                from gammad.core.v1_params import parse_exit

                ctx = replace(ctx, hold=parse_exit("exit", sig.snapshot["v1r"]["exit"]).form)
        except Exception:  # a context bug must never lose a signal: route it ungated (fail-open, D63)
            log.exception("signal context failed for sec %s; routing without ctx", sig.security_id)
            ctx = None
        # the task name reaches the log on failure: no kind in the client edition (no E2 detail in a log line)
        name = f"signal-{sig.security_id}" if client else f"signal-{sig.security_id}-{sig.kind}-{scanner}"
        self.spawn(self._route_and_grade(sig, meta, ctx, scanner), name=name)

    async def _route_and_grade(
        self, sig: Signal, meta: ContractMeta, ctx: SignalContext | None, scanner: str
    ) -> int:
        """W15: the router records + routes the signal (scanner-aware); then every recorded E1 of every scanner
        starts a grade (§6). The grader reads the stored row, so it grades exactly what the router recorded."""
        if scanner == "V1":  # the live scanner uses the router's original entry (tools / tests with a 3-arg router)
            signal_id = await self.router.on_signal(sig, meta, ctx)
        else:
            signal_id = await self.router.on_signal(sig, meta, ctx, scanner=scanner)
        if getattr(self, "v1desk", None) is not None:
            self.v1desk.reserved.discard(sig.security_id)
        # T-310 review M1: a V1 row HELD for the 09:20 list post is graded at its release, with its released tier
        # (router.on_released -> _grade_released); its V1 PLUS (if any) is derived only then
        held = getattr(self.router, "held_pending", None)
        if held is not None and signal_id > 0 and held(signal_id) is True:
            return signal_id
        # S1.5.0 (D159, D160(1)): the V1 PLUS rows the router derived from this signal; always drained
        pop = getattr(self.router, "pop_derived", None)  # tools / tests with a minimal router have none
        derived = pop(signal_id) if pop is not None and signal_id > 0 else []
        # called on the class: tests run this method on a stand-in `self` holding router / grader / conn only
        Runtime._grade_released(self, signal_id, sig.kind, meta, derived)  # type: ignore[arg-type]
        return signal_id

    def _grade_released(self, signal_id: int, kind: str, meta: ContractMeta, derived: Sequence[int]) -> None:
        """W15 §6: grade a recorded entry and the V1 PLUS rows it derived from the STORED rows (on_signal, and T-310
        review M1 the router's `on_released` for a held row after its release). Never raises.
        AM2 (PLAN-D287-085, D288 Q6): the STORED row's kind decides, never the engine signal's `kind` (kept for the
        callers' signature; the client stores its E2 as kind E1): an E1 of any scanner, and a TIERED V1 E2 (the D287
        group / client entry) are graded; an untiered (legacy) E2 and a V2T E2 are not. The grader starts on E1-kind
        rows only (gammad/scan/grader.py), so a V1 E2 entry is handed to it with kind "E1" (its grade row stores no
        kind; review counts it as the V1 entry, Q6)."""
        del kind  # AM2: the stored row decides
        if self.grader is None or signal_id <= 0:
            return
        for sid_ in (signal_id, *derived):
            try:
                row = self.conn.execute("SELECT * FROM signals WHERE id = ?", (sid_,)).fetchone()
                if row is None:
                    continue
                rec = dict(row)
                stored = rec.get("kind")
                if stored == "E2" and str(rec.get("scanner") or "V1") == "V1" and rec.get("tier"):
                    rec["kind"] = "E1"  # a V1 E2 entry is the V1 entry (D287 (1)); the grader takes E1-kind rows only
                elif stored != "E1":
                    continue
                self.grader.on_signal({**rec, "symbol": meta.symbol, "tick_size": meta.tick_size})
            except Exception:
                log.exception("scanner grader on_signal failed for signal %s", sid_)

    def _push_tick(self, tick: Tick) -> None:
        now = self._wall_ms()
        last = self._tick_pushed.get(tick.security_id)
        if last is not None and now - last < TICK_PUSH_MIN_MS:
            return
        self._tick_pushed[tick.security_id] = now
        self.hub.publish(
            "tick",
            {
                "security_id": tick.security_id,
                "ts_ms": tick.ts_ms,
                "ltp": tick.ltp,
                "volume_day": tick.volume_day,
                "oi": tick.oi,
            },
        )

    def _push_from_router(self, message: dict[str, Any]) -> None:
        kind = str(message.get("type", "log"))
        data = {k: v for k, v in message.items() if k != "type"}
        if kind in ("signal", "scanner"):
            data["expiry_days"] = _expiry_trading_days(data.get("trade_date"), data.get("expiry_date"))
        if kind == "signal" and data.get("kind") == "E1" and data.get("tier") == "ENTRY":
            # D98(5) + W15: an ENTRY-tier E1 was recorded (the router pushes after its INSERT) -> DONE may say ENTERED
            with contextlib.suppress(TypeError, ValueError):
                self._note_visible_e1(int(data.get("security_id")), int(data.get("ts_ms")))  # type: ignore[arg-type]
        self.hub.publish(kind if kind in ("signal", "token", "scanner") else "log", data)

    def _push_trade(self, message: dict[str, Any]) -> None:
        """Z1(2)/X3: the tracker's push -> WS "trade" ({type:"trade", **row, symbol, strike, events}); never
        through _push_from_router, which maps an unknown type to "log"."""
        self.hub.publish("trade", {k: v for k, v in message.items() if k != "type"})

    # ---------------------------------------------------------------- V2 Scanner (S1.7.0, T-267 hunk A)
    # PLAN-T267 §1 + M1 / M2 / M8 / M10-M14, D249, D251, D253. Every step below fails OPEN for an entry: a missing
    # regime = st_dir None (the tier decides, gates.v2t_tier), a missing book = NO_ZONE (M7 / M12); every fault is
    # logged for the OWNER only (a client logs it at DEBUG: no shallow-history or zone fact on a client surface,
    # D249(1)).
    @staticmethod
    def _v2_log(msg: str, *args: object, warn: bool = False) -> None:
        if _edition.is_client():
            log.debug(msg, *args)
        else:
            (log.warning if warn else log.info)(msg, *args)

    def _v2t_pool(self) -> EnginePool | None:
        """The V2 Scanner's EnginePool (both editions), None when this Runtime runs without it (a test harness that
        keeps the live V1 scanner only): then no V2 plumbing runs at all."""
        return self.shadow_pools.get(V2T)

    def _row_added_ms(self, row: Mapping[str, Any]) -> int | None:
        """A1 (PLAN-T263, second_touch.run_ms): when the row's build started (runs.started_ist of its run_id); None
        when unknown (listed_ms_for then reads the 09:20 build)."""
        run_id = row.get("run_id")
        if not run_id:
            return None
        key = str(run_id)
        if key not in self._run_started:
            got: int | None = None
            try:
                hit = self.conn.execute("SELECT started_ist FROM runs WHERE run_id = ?", (key,)).fetchone()
                if hit is not None and hit[0]:
                    dt = datetime.fromisoformat(str(hit[0]))
                    got = int((dt if dt.tzinfo is not None else dt.replace(tzinfo=IST)).timestamp() * 1000)
            except (sqlite3.Error, ValueError):
                got = None
            self._run_started[key] = got
        return self._run_started[key]

    def _v2t_register(self, sid: int, row: Mapping[str, Any], d: date, created: list[int]) -> None:
        """A watchlist row's V2T engine (the pool refuses a list it does not run: the client's V2T never sees an
        EXPIRY list, D248(7); HOI-1 only through the client's own top-N). listed_ms = A1's listing bar."""
        pool = self._v2t_pool()
        if pool is None or not self.engines_allowed:
            return
        ln = str(row["list_name"])
        if not pool.accepts(ln):
            return
        from gammad.v2scan.trigger import listed_ms_for  # lazy: owner-only package (AM7)

        existed = sid in pool
        pool.get_or_create(sid, ln, listed_ms=listed_ms_for(self._row_added_ms(row), d))
        if not existed:
            created.append(sid)

    def _v2t_fail_opening(self, sids: Iterable[int], reason: str) -> int:
        """M8: every engine of `sids` still waiting for its opening bars evaluates on its live bars from now on."""
        pool = self._v2t_pool()
        n = 0
        for sid in sids:
            eng = pool.get(sid) if pool is not None else None
            if eng is not None and getattr(eng, "opening_state", None) == "pending":
                eng.opening_failed(reason)
                n += 1
        return n

    async def _v2t_opening(self, d: date, sids: list[int], cutoff_ms: int) -> int:
        """M8 (PLAN-T267): ~5 s after the subscription slot closes, today's broker 5-min bars with OI (a worker
        thread) REPLACE the partial / missing opening slots in each V2T engine, on the loop. A contract left unfetched
        (cancelled, an auth error, no adapter) is told opening_failed: it never evaluates while pending. Returns how
        many engines took broker bars."""
        try:
            delay = (cutoff_ms + V2T_OPENING_DELAY_MS - self._wall_ms()) / 1000.0
            if delay > 0:
                await self._v2t_sleep(delay)
            adapter = self.adapter()
            if adapter is None or self.trade_date != d:
                self._v2t_fail_opening(sids, "no broker adapter" if adapter is None else "day closed")
                self._v2t_catch_up(d, sids)
                return 0
            kw = {k: v for k, v in self._warmup_kwargs.items() if k in ("min_interval_s", "sleep", "monotonic")}
            stop = threading.Event()
            try:
                res = await asyncio.to_thread(fetch_opening_bars, adapter, sids, d, cutoff_ms, carry=self.carry,
                                              cancelled=stop.is_set, **kw)  # fmt: skip
            except asyncio.CancelledError:
                stop.set()
                raise
            except Exception as exc:  # noqa: BLE001 -- an auth error / anything: the live bars stand
                self._v2_log("v2t opening bars %s: %s: %s; %d contracts evaluate their live bars", d,
                             type(exc).__name__, exc, len(sids), warn=True)  # fmt: skip
                self._v2t_fail_opening(sids, type(exc).__name__)
                self._v2t_catch_up(d, sids)
                return 0
            if self.trade_date != d:
                return 0
            pool = self._v2t_pool()
            n = apply_opening(pool, res) if pool is not None else 0
            done = {*res.bars, *res.missing, *res.errors}
            self._v2t_fail_opening([s for s in sids if s not in done], "cancelled")
            self._v2_log("v2t opening bars %s: %s", d, res.summary())
            self._v2t_catch_up(d, sids)
            return n
        except asyncio.CancelledError:
            self._v2t_fail_opening(sids, "cancelled")
            raise

    def _v2t_catch_up(self, d: date, sids: Iterable[int]) -> int:
        """M8 catch-up (D254 / D258): once the opening bars landed (or failed), each engine of `sids` evaluates the
        bar closes it skipped while pending, in time order, on the contract's current series (V2TEngine.catch_up); a
        trigger found goes the normal way (_v2t_enrich at that bar's close, then _dispatch with the inputs that close
        was handed). On the loop, synchronously (no live bar close interleaves). Returns the signals dispatched."""
        pool = self._v2t_pool()
        if pool is None or not self.engines_allowed or self.trade_date != d:
            return 0
        n = 0
        for sid in sids:
            eng = pool.get(sid)
            if eng is None or not hasattr(eng, "catch_up"):
                continue
            series = self.series.get(sid)
            try:
                hits = eng.catch_up(series.closed() if series is not None else None)
            except Exception:  # a catch-up bug must never cost the other contracts' catch-up
                log.exception("v2t catch-up failed for sec %s", sid)
                continue
            for sig, oi_now, oi_peak in hits:
                self._dispatch(self._v2t_enrich(sig), oi_now=oi_now, oi_peak=oi_peak, scanner=V2T, catch_up=True)
                n += 1
        return n

    def _v2t_stock_px(self, und: int, bar_ts_ms: int) -> float | None:
        """The stock price at a V2T trigger (t263_study.simulate :586): the close of the stock's 5-min bar of the
        trigger slot; that bar not closed yet -> the stock's last tick (that slot's close in the making); else its
        latest earlier close."""
        bars = self.und_bars.get(und) or []
        earlier: float | None = None
        for b in reversed(bars):
            if int(b.ts_ms) == int(bar_ts_ms):
                return float(b.close)
            if int(b.ts_ms) < int(bar_ts_ms):
                earlier = float(b.close)
                break
        ltp = self.last_ltp.get(und)
        if ltp is not None and ltp > 0:
            return float(ltp)
        return earlier

    def _regime_view(self, und: int | None) -> dict[str, Any]:
        """snapshot.v2t.regime = {st_dir, st_value, asof, reason}; an underlying with no regime today = st_dir None
        (unknown: gates.v2t_tier decides, never a defaulted +1)."""
        reg = self.v2_regimes.get(und) if und is not None and self.v2_regime_day == self.trade_date else None
        if reg is None:
            return {"st_dir": None, "st_value": None, "asof": None,
                    "reason": "missing: no regime computed for this underlying today"}  # fmt: skip
        return {"st_dir": reg.st_dir, "st_value": reg.st_value, "asof": reg.asof.isoformat() if reg.asof else None,
                "reason": reg.reason}  # fmt: skip

    def _v2t_enrich(self, sig: Signal) -> Signal:
        """A V2T E1 before _dispatch (so before the router stores payload_json): snapshot.v2t gains `regime`
        (the underlying's daily Supertrend, M14), `stop` (the 1h stop zone at the trigger close: {top, bottom,
        confirmed_ms}, or None = NO_ZONE, M7) and `und_px` (the stock price it was picked at). Never raises: a fault
        = regime unknown / NO_ZONE (M12)."""
        snap = dict(sig.snapshot or {})
        block = dict(snap.get("v2t") or {})
        meta = self.meta.get(sig.security_id)
        und = meta.underlying_security_id if meta is not None else None
        block["regime"] = self._regime_view(und)
        stop: dict[str, Any] | None = None
        px: float | None = None
        try:
            if meta is not None and und is not None:
                bar_ts = int(block.get("bar_ts_ms") or (sig.ts_ms - self.signal_tf * MINUTE_MS))
                px = self._v2t_stock_px(int(und), bar_ts)
                book = self.h1_books.get(int(und))
                from gammad.v2scan import h1zones  # lazy: owner-only package (AM7)

                zone = h1zones.stop_zone_for(book, str(meta.option_type), px, int(sig.ts_ms)) if px else None
                if zone is not None:
                    stop = {"top": zone.top, "bottom": zone.bottom, "confirmed_ms": zone.confirmed_ms}
                else:
                    self._v2_log("v2t sec %s: NO_ZONE (%s)", sig.security_id,
                                 "no 1h book" if book is None else "no stock price" if not px else "no zone")
        except Exception:  # M12: never blocks the entry
            log.exception("v2t sec %s: stop zone failed; NO_ZONE", sig.security_id)
            stop = None
        block["stop"] = stop
        block["und_px"] = px
        snap["v2t"] = block
        return replace(sig, snapshot=snap)

    def _wants_regime(self, ref: str) -> bool:
        # T-277: the 09:10 prevclose run fetches the same bars (< today) as the 09:20 full ltp run
        return ref in ("ltp", "prevclose") and self._v2t_pool() is not None

    @staticmethod
    def _call_job(fn: Callable[..., dict[str, Any]], *args: Any, **kw: Any) -> dict[str, Any]:
        """fn(*args, **kw) with only the keywords `fn` takes (the default jobs take them all; a test's two-argument
        fake gets the plain call). Blocking: runs in a worker thread."""
        if kw:
            try:
                params = inspect.signature(fn).parameters
            except (TypeError, ValueError):
                params = MappingProxyType({})
            if not any(p.kind is inspect.Parameter.VAR_KEYWORD for p in params.values()):
                kw = {k: v for k, v in kw.items() if k in params}
        return fn(*args, **kw)

    def _call_mark_levels(self, d: date, ref: str, sink: dict[int, list[Candle]] | None,
                          **kw: Any) -> dict[str, Any]:  # fmt: skip
        """jobs.mark_levels(d, ref), handing it `sink` (and T-277's base / refresh_hm / prev_trading_day) when it
        takes them. Blocking: runs in a worker thread."""
        if sink is not None:
            kw["bars_sink"] = sink
        return self._call_job(self.jobs.mark_levels, d, ref, **kw)

    def _mark_kwargs(
        self, d: date, ref: str, steps: Sequence[tuple[str, str]], extra: Mapping[str, Any] | None
    ) -> dict[str, Any]:
        """T-277: a prevclose run checks each stock's last bar against the previous trading day; an ltp mark ahead of a
        0920 / refresh / 1315 build is a price-only re-mark on the day's base when there is one (`base="auto"`)."""
        kw = dict(extra or {})
        if ref == "prevclose":
            kw.setdefault("prev_trading_day", self._prev_trading_day(d))
        elif ref == "ltp" and any(s == "build_watchlists" and k in REMARK_BUILD_KINDS for s, k in steps):
            kw.setdefault("base", REMARK_BASE)
        return kw

    async def _v2_regime_step(self, d: date, summary: Mapping[str, Any], sink: Mapping[int, list[Candle]],
                              merge: bool = False) -> None:  # fmt: skip
        """M14: today's daily Supertrend per underlying from the candles the ltp levels run just fetched (`sink`): NO
        second fetch (compute_regimes gets no adapter and only the underlyings it has bars for). Stored in v2_regime
        (a restart reads it back, _v2_open_day). A failure leaves every regime unknown (no V2 ENTRY), never the job.
        `merge` (D278(1)): the 09:20 re-mark's full marks ADD to today's stored regimes, never replace them."""
        try:
            from gammad.v2scan.regime import compute_regimes, load_regimes, upsert_regimes  # lazy (AM7)

            names: dict[int, str] = {}
            with contextlib.suppress(sqlite3.Error):
                for usid, sym in self.conn.execute(
                    "SELECT security_id, symbol FROM levels WHERE run_id = ?", (summary.get("run_id"),)
                ):
                    names[int(usid)] = str(sym)
            unds = {int(u): names.get(int(u), str(u)) for u in sink}
            regs = await asyncio.to_thread(compute_regimes, None, unds, d, self.cfg, bars=sink)  # type: ignore[arg-type]
            upsert_regimes(self.conn, d, regs, unds, computed_ms=self._wall_ms())
            if merge:
                regs = load_regimes(self.conn, d)
            self.v2_regimes, self.v2_regime_day = regs, d
        except Exception:
            log.exception("v2 regime %s: not computed; no underlying has a regime today", d)

    def _v2_regime_at_start(self, d: date) -> None:
        """0.8.5 seam (measured 5 Oct: a 10:25 start after the old version's 09:20 run left v2_regime empty, every V2
        signal capped `supertrend_unknown`): the regime step runs only inside a levels run that fetched bars, so a
        start AFTER today's levels run exists computes it ONCE, in the background, off the event loop, from daily bars
        before today (compute_regimes' own fetch; no levels run written). Only when the V2 pool exists, today has an
        ok / partial levels run and v2_regime holds no row for today (idempotent); never when no levels run exists
        yet (the scheduled runs own that). A broker / auth failure logs once and stores nothing (V2 fails closed;
        the next start tries again); nothing raises into the start path."""
        if self.v2_regimes or not self._wants_regime("ltp"):
            return
        try:
            from gammad.jobs.build_watchlists import latest_levels_run

            run_id = latest_levels_run(self.conn, d, "ltp") or latest_levels_run(self.conn, d, "prevclose")
            unds = {} if run_id is None else {int(u): str(s) for u, s in self.conn.execute(
                "SELECT security_id, symbol FROM levels WHERE run_id = ?", (run_id,))}  # fmt: skip
        except Exception:  # a store read failing never fails the start
            log.exception("v2 regime %s: levels unreadable; not computed at start", d)
            return
        if not unds:
            return

        async def compute() -> None:
            from gammad.v2scan.regime import compute_regimes, upsert_regimes  # lazy: owner-only package (AM7)

            try:
                adapter = self.require_adapter()
                regs = await asyncio.to_thread(compute_regimes, adapter, unds, d, self.cfg)
            except Exception as exc:  # noqa: BLE001 -- fail closed: no regime, no V2 ENTRY, never the start
                log.warning("v2 regime %s: not computed at start (%s); V2 stays capped", d, type(exc).__name__)
                return
            if not any(r.st_dir is not None for r in regs.values()):
                log.warning("v2 regime %s: not computed at start (no underlying got bars); V2 stays capped", d)
                return
            if self.trade_date != d or self.v2_regimes:  # the day closed, or a levels run computed it meanwhile
                return
            upsert_regimes(self.conn, d, regs, unds, computed_ms=self._wall_ms())
            self.v2_regimes, self.v2_regime_day = regs, d
            log.info("v2 regime: computed at start for %s: %d underlyings (reason: no rows for an existing levels run)",
                     d.isoformat(), len(regs))  # fmt: skip

        self.spawn(compute(), name="v2-regime-at-start")

    def _v2_open_day(self, d: date) -> None:
        """A new trade date: today's stored regimes (a restart after 09:20 reads the 09:20 result back) and each
        book's session-open snapshot (M13)."""
        if self._v2t_pool() is None:
            return
        from gammad.v2scan.regime import load_regimes  # lazy: owner-only package (AM7)

        try:
            self.v2_regimes = load_regimes(self.conn, d)
        except sqlite3.Error:
            log.exception("v2 regime %s: unreadable; recomputed at the next ltp levels run", d)
            self.v2_regimes = {}
        self.v2_regime_day = d
        for u, b in self.h1_books.items():
            self._h1_mark_open(u, b, d)

    # -- the 1h zone books (B2: h1zones / h60_store / h60_sync)
    def _h1_store(self) -> H60Store:
        from gammad.jobs.h60_sync import history_root
        from gammad.v2scan.h60_store import H60Store  # lazy: owner-only package (AM7)

        return H60Store(history_root(self.cfg))

    def _zone_params(self):
        from gammad.core.zones import ZoneParams

        return ZoneParams.from_config(self.cfg.zones)

    def _h1_seed_get(self) -> h1zones.Seed | None:
        """The packaged zone seed (D249(2), M10), read once; None when missing / unusable (M2: books then build from
        the H60 store alone)."""
        if self._h1_seed is _SEED_UNSET:
            from gammad.v2scan import h1zones  # lazy: owner-only package (AM7)

            self._h1_seed = h1zones.load_seed(params=self._zone_params())
        seed: h1zones.Seed | None = self._h1_seed
        return seed

    def _h1_mark_open(self, und: int, book: h1zones.H1ZoneBook, d: date) -> None:
        """M13: the book's session-open snapshot, taken while it holds no hour of `d`."""
        open_ms, _ = session_bounds_ms(d)
        if book.last_close_ms is None or book.last_close_ms <= open_ms:
            self.h1_open_states[und] = book.snapshot()

    def _h1_stale(self, book: h1zones.H1ZoneBook, d: date) -> bool:
        """A book that does not hold the previous session (its underlying was not watched then): re-opened."""
        try:
            prev = cal.prev_trading_day(d)
        except LookupError:
            return False
        from gammad.v2scan import h1zones  # lazy: owner-only package (AM7)

        return book.last_close_ms is None or book.last_close_ms < h1zones.ist_ms(prev, "15:00")

    def _h1_usids(self) -> set[int]:
        if self._v2t_pool() is None:
            return set(self.h1_books) | self.tracker.zone_unds()
        return set(self.h1_books) | self.underlyings | self.tracker.zone_unds()

    def _h1_ensure(self, d: date, usids: Iterable[int]) -> None:
        """Open the 1h book of every underlying in `usids` that has none (or a stale one) in a background task."""
        if self._v2t_pool() is None:
            return
        want = sorted(
            int(u) for u in usids
            if (int(u) not in self.h1_books or self._h1_stale(self.h1_books[int(u)], d)) and int(u) not in self._h1_pending
        )  # fmt: skip
        if not want:
            return
        self._h1_pending.update(want)
        self.spawn(self._h1_open(d, want), name="h1-books")

    def _h1_open_blocking(
        self, d: date, usids: list[int], adapter: BrokerAdapter | None
    ) -> tuple[dict[int, h1zones.H1ZoneBook], h1zones.OpenReport, Any]:
        """Worker thread: top up the H60 store from the day after the seed's as_of through yesterday (the client never
        syncs from 2017, M1), then restore each seed book and append the store's rows after as_of (open_books)."""
        from gammad.jobs.h60_sync import run_h60_sync
        from gammad.v2scan import h1zones  # lazy: owner-only package (AM7)

        seed = self._h1_seed_get()
        through = d - timedelta(days=1)
        store = self._h1_store()
        srep = None
        if adapter is not None and seed is not None:
            srep = run_h60_sync(self.cfg, adapter, usids, through, start=seed.as_of + timedelta(days=1), store=store)
        elif seed is None:
            self._v2_log("h1 zones: no zone seed; %d books build from the stored 60-min bars only", len(usids),
                         warn=True)  # fmt: skip
        books, orep = h1zones.open_books(
            usids, lambda u, after: store.rows_after(u, after, through), seed, params=self._zone_params(),
        )
        return books, orep, srep

    async def _h1_open(self, d: date, usids: list[int]) -> int:
        """Open the books of `usids` (_h1_open_blocking in a worker thread), then install them on the loop with their
        session-open snapshot and today's closed hours. A failure leaves them without a book (NO_ZONE, M12)."""
        adapter = self.adapter()
        try:
            books, orep, srep = await asyncio.to_thread(self._h1_open_blocking, d, usids, adapter)
        except asyncio.CancelledError:
            self._h1_pending.difference_update(usids)
            raise
        except Exception:
            log.exception("h1 zones: %d books not opened; their V2 entries are NO_ZONE", len(usids))
            self._h1_pending.difference_update(usids)
            return 0
        self._h1_pending.difference_update(usids)
        n = 0
        for u, b in books.items():
            old = self.h1_books.get(u)
            if old is not None and not self._h1_stale(old, d):
                continue
            self.h1_books[u] = b
            self._h1_mark_open(u, b, d)
            if self.trade_date == d:
                self._h1_feed(u)
            n += 1
        ms = sorted(orep.ms.values())
        self._v2_log(
            "h1 zones %s: %d books (seed %d, full pass %d, none %d), %d bars appended, median %.2f ms%s", d, n,
            len(orep.seeded), len(orep.full_pass), len(orep.missing), orep.appended_bars,
            ms[len(ms) // 2] if ms else 0.0, f"; {srep.line()}" if srep is not None else "",
        )  # fmt: skip
        return n

    def _h1_feed(self, und: int) -> int:
        """Append every hour today's closed 5-min stock bars complete (hour_bars_from_5m: the 6 full hours, D253)
        that the book does not hold yet, and hand each append's ZoneEvents to tracker.on_hour_close (trail steps).
        An hour counts as complete once a stock bar ending at or after its close has closed. Returns hours added."""
        book = self.h1_books.get(und)
        d = self.trade_date
        bars = self.und_bars.get(und)
        if book is None or d is None or not bars or und in self._h1_wait:
            return 0
        last_end = int(bars[-1].ts_ms) + self.signal_tf * MINUTE_MS
        from gammad.v2scan import h1zones  # lazy (AM7); a client holds no book: returned above

        n = 0
        for row in h1zones.hour_bars_from_5m(bars, d, now_ms=last_end):
            if book.last_close_ms is not None and int(row[5]) <= book.last_close_ms:
                continue
            try:
                evs = book.append(row)
            except Exception:
                log.exception("h1 zones: underlying %s hour %s not appended", und, row[0])
                return n
            n += 1
            try:
                self.tracker.on_hour_close(und, evs)
            except Exception:
                log.exception("trade tracker on_hour_close failed for underlying %s", und)
        return n

    def _h60_job_blocking(
        self, d: date, usids: list[int], through: date, adapter: BrokerAdapter, reconcile_for: list[int]
    ) -> tuple[Any, dict[int, list[list]]]:
        from gammad.jobs.h60_sync import default_start, run_h60_sync

        seed = self._h1_seed_get()
        start = seed.as_of + timedelta(days=1) if seed is not None else default_start(self.cfg)
        store = self._h1_store()
        rep = run_h60_sync(self.cfg, adapter, usids, through, start=start, store=store)
        rows = {u: store.rows(u, d, d) for u in reconcile_for if u not in rep.failed}
        return rep, rows

    async def run_h60_sync_job(self, hm: str) -> dict[str, Any]:
        """schedule.h60_sync: before the close (08:30) the H60 store is topped up through yesterday; after it (16:05)
        through today, then M13: each book fed live today is rebuilt from its session-open snapshot + the broker's
        60-min bars of today (D253: those stay authoritative) and the zone differences are logged for the owner.
        Trail steps already applied stand. A failed / short broker answer keeps the tick-built book.
        T-275 (1): every run sets its job_state slot (/api/status.jobs) and logs ONE line, idle included."""
        job = self.job_state.setdefault(H60_SYNC_JOB.format(hm=hm.replace(":", "")), JobState(f"{hm} h60_sync"))
        job.last_run_ist, job.last_status, job.detail = self._now().isoformat(), "running", None
        d = self.today()
        if not self.is_trading_day(d):
            job.last_status, job.detail = "skipped", "not a trading day"
            self._v2_log("h60 sync %s: skipped (%s)", hm, job.detail)
            return {"status": "skipped"}
        usids = sorted(self._h1_usids())
        adapter = self.adapter()
        if not usids or adapter is None:
            job.last_status, job.detail = "idle", ("no 1h book" if not usids else "no broker adapter")
            self._v2_log("h60 sync %s: idle (%s)", hm, job.detail)
            return {"status": "idle"}
        after_close = hm >= session_clock.fo_close_hm(d)
        through = d if after_close else d - timedelta(days=1)
        fed: list[int] = []
        if after_close:
            for u, st in self.h1_open_states.items():
                b = self.h1_books.get(u)
                if b is not None and b.last_close_ms is not None and b.last_close_ms > int(st.get("last_close_ms") or -1):
                    fed.append(u)
        try:
            rep, broker = await asyncio.to_thread(self._h60_job_blocking, d, usids, through, adapter, sorted(fed))
        except Exception:
            log.exception("h60 sync %s: failed; the books stand", hm)
            job.last_status, job.detail = "failed", "the books stand"
            return {"status": "failed"}
        out: dict[str, Any] = {"status": "ok", "sync": rep.line(), "reconciled": 0, "differs": 0, "kept": 0}
        if not after_close:
            self._v2_log("h60 sync %s: %s", hm, rep.line())
            job.last_status, job.detail = "ok", rep.line()
            return out
        todo: dict[int, list[list]] = {}
        for u in fed:
            rows, b, st = broker.get(u) or [], self.h1_books.get(u), self.h1_open_states.get(u)
            live_n = (b.n - int(st.get("n") or 0)) if (b is not None and st is not None) else 0
            if not rows or len(rows) < live_n:  # nothing served / fewer hours than were built live: keep the live book
                out["kept"] += 1
                self._v2_log("h1 reconcile %s: underlying %s kept its tick-built hours (broker %d, live %d)", d, u,
                             len(rows), live_n)  # fmt: skip
                continue
            todo[u] = rows
        from gammad.v2scan import h1zones  # lazy: owner-only package (AM7)

        try:
            recs = h1zones.reconcile_all(
                {u: self.h1_open_states[u] for u in todo}, todo, self.h1_books,
                params=self._zone_params(),
            )
            for u, rec in recs.items():
                self.h1_books[u] = rec.book
                out["reconciled"] += 1
                if rec.differs:
                    out["differs"] += 1
                    self._v2_log("h1 reconcile %s: underlying %s zones +%d / -%d vs the tick-built book: added %s removed %s",
                                 d, u, len(rec.added), len(rec.removed), rec.added, rec.removed)  # fmt: skip
        except Exception:
            log.exception("h60 sync %s: reconcile failed; the books stand", hm)
            job.last_status, job.detail = "failed", "reconcile failed; the books stand"
            return {"status": "failed"}
        self._v2_log("h60 sync %s: %s; reconciled %d (%d differ, %d kept live)", hm, rep.line(), out["reconciled"],
                     out["differs"], out["kept"])  # fmt: skip
        job.last_status, job.detail = "ok", rep.line()
        return out

    # ---------------------------------------------------------------- jobs
    def _steps_for(self, name: str, kind: str | None) -> list[tuple[str, str]]:
        if name == "mark_levels":
            if kind == "0920":
                return [("mark_levels", "ltp"), ("build_watchlists", "0920")]
            if kind == PREOPEN_KIND:  # T-277: the 09:10 chain by hand
                return [("mark_levels", "prevclose"), ("build_watchlists", PREOPEN_KIND)]
            if kind in (None, ""):
                now = self._now()
                return [("mark_levels", "close" if f"{now.hour:02d}:{now.minute:02d}" >=
                         session_clock.fo_close_hm(now.date()) else "ltp")]
            if kind in ("ltp", "close", "prevclose"):
                return [("mark_levels", kind)]
            if kind == "1600":
                return [("mark_levels", "close")]
            raise ValueError(f"mark_levels kind must be one of 0910, 0920, 1600, ltp, close, prevclose (got {kind!r})")
        if name == "build_watchlists":
            k = kind or "manual"
            if k not in BUILD_KINDS:
                raise ValueError(f"build_watchlists kind must be one of {BUILD_KINDS} (got {kind!r})")
            if k in (REFRESH_KIND, "1315"):  # T-277: "Run 13:15 now" = a refresh: re-mark the prices, then build
                return [("mark_levels", "ltp"), ("build_watchlists", k)]
            return [("build_watchlists", k)]
        raise ValueError(f"unknown job {name!r}")

    async def run_job(self, name: str, kind: str | None = None, *, post: bool = False) -> str:
        """Manual run (API / tray). Returns a job-run id at once; the job runs in the background.
        Manual runs do NOT check the trading calendar: the operator asked for it. `post` = also post the
        new watchlist rows to the GROUP (D45), as the scheduled 09:20 build does.
        T-181 F2 (R2): a run whose steps include the 09:20-kind build (tray "Run 09:20 now", an API / CLI run of the
        0920 kind) posts EVERY list of the day when no 09:20 list post was sent today (on a trading day): the
        README's late-login advice. A `manual` build keeps post=False unless asked."""
        steps = self._steps_for(name, kind)
        run_id = f"manual-{name}-{kind or 'default'}-{self._now():%Y%m%dT%H%M%S}"
        key = f"manual:{name}"
        self.job_state.setdefault(key, JobState(f"manual {name}"))
        unposted = not post and ("build_watchlists", RETIRE_BUILD_KIND) in steps
        self.spawn(self._execute(key, steps, run_id, post=post, post_if_unposted=unposted), name=run_id)
        return run_id

    def _posted_0920(self, d: date) -> bool:
        """T-181 F2: a 09:20 list post of `d` reached the group (a manual_sends watchlist_post row, status sent, whose
        ref names the day and the "09:20 build" label). Unreadable = True: never risk a second post."""
        from gammad.alerts.router import KIND_WATCHLIST_POST, ST_SENT

        try:
            refs = self.conn.execute(
                "SELECT ref FROM manual_sends WHERE kind = ? AND status = ?", (KIND_WATCHLIST_POST, ST_SENT)
            ).fetchall()
        except Exception:
            log.exception("manual_sends read failed; the 09:20 lists are not re-posted")
            return True
        label = self._post_label(RETIRE_BUILD_KIND)
        for (raw,) in refs:
            try:
                ref = json.loads(raw or "{}")
            except ValueError:
                continue
            if isinstance(ref, dict) and ref.get("date") == d.isoformat() and ref.get("label") == label:
                return True
        return False

    def spawn_job(self, name: str, kind: str | None = None) -> None:
        """Sync entry for loop.call_soon_threadsafe (tray thread)."""
        self.spawn(self.run_job(name, kind), name=f"spawn-{name}")

    async def run_slot(self, job_id: str) -> str:
        """Scheduled entry: skip non-trading days, then run the slot's steps. Returns the final status.
        T-277: a slot the refresh absorbed (`scheduled=False`, the old 13:15) still runs here by hand (replay_day); its
        job_state row appears only once it ran."""
        today = self.today()
        if job_id not in self.job_state:
            slot0 = next((s for s in self.slots if s.job_id == job_id), None)
            self.job_state[job_id] = JobState(slot0.name if slot0 is not None else job_id)
        st = self.job_state[job_id]
        if not self.is_trading_day(today):
            st.last_run_ist, st.last_status = self._now().isoformat(), "skipped"
            st.detail = f"{today} is not an NSE trading day"
            log.info("job %s skipped: %s", job_id, st.detail)
            return "skipped"
        if job_id == CLOSE_DAY_JOB:
            st.last_run_ist = self._now().isoformat()
            day = self.trade_date or today
            status = "ok"
            try:
                async with self._day_lock:  # T-107: never mid-way through a job's / the day pass's load_day
                    await self.close_day()
            except Exception as exc:  # T-213 (audit S5 / G6): the summary and the check still run; the crash is told
                status, st.detail = "failed", f"{type(exc).__name__}: {exc}"
                log.exception("job %s failed", job_id)
                self._report_failure(f"job {job_id} FAILED: {st.detail}")
            self.spawn(self._day_end_telegram(day.isoformat()), name="day-end-telegram")
            st.last_status = status
            return status
        slot = next(s for s in self.slots if s.job_id == job_id)
        if slot.build_kind == PREOPEN_KIND:  # T-277: the 09:10 chain; a failure here leaves the day as before 0.8.5
            why = self._preopen_block(today)
            if why is not None:
                st.last_run_ist, st.last_status, st.detail = self._now().isoformat(), "skipped", why
                log.info("job %s skipped: %s", job_id, why)
                return "skipped"
            return await self._execute(job_id, slot.steps(), f"sched-{job_id}-{today:%Y%m%d}", quiet=True)
        # D148 (owner 2026-09-29): only the 09:20 build posts its lists; the 13:15 scan still runs, posts nothing
        post = slot.build_kind == RETIRE_BUILD_KIND
        # 0.8.3 T-229 (4)(b): a build-only slot (13:15) on a day whose 09:20 levels never landed (an auth error, no
        # login) marks them first -- decided INSIDE _job_lock (skeptic L3: a catch-up / retry holding the lock may mark
        # them meanwhile); build_watchlists' levels_refusal still guards a mostly-failed run
        ensure = bool(slot.build_kind and not slot.mark_ref)
        # T-277: the 09:20 slot is the build the day keys on: behind a running job (the 09:10 chain on a slow broker,
        # AUDIT-BROKERS-085 defect 12) it WAITS, up to wait_0920_s, and is never skipped
        wait = self.wait_0920_s if slot.build_kind == RETIRE_BUILD_KIND else None
        return await self._execute(job_id, slot.steps(), f"sched-{job_id}-{today:%Y%m%d}", post=post,
                                   ensure_ltp_levels=ensure, lock_wait_s=wait)  # fmt: skip

    def _preopen_block(self, d: date) -> str | None:
        """T-277: why the 09:10 slot must not run now (None = run it). Late (the 09:20 slot is due: it does the full
        job), the day's 09:20 build claimed / built, or a job holds _job_lock (queued behind it, 09:10 would delay
        09:20)."""
        now = self._now()
        hm = f"{now.hour:02d}:{now.minute:02d}"
        slot = next((s for s in self.slots if s.build_kind == RETIRE_BUILD_KIND), None)
        if slot is not None and hm >= slot.at:
            return f"too late at {hm}: the {slot.at} build does the full job"
        if d in self._claims_0920 or self._has_0920_build(d):
            return "today's 09:20 build is running or built"
        if self._job_lock.locked():
            return "another job is running; the 09:20 build does the full job"
        return None

    # ---------------------------------------------------------------- T-277: the 15-minute refresh
    def _refresh_name(self) -> str:
        r = self.cfg.schedule.refresh
        return f"{r.first}-{r.last} every {r.every_min} min mark_levels(ltp) + build_watchlists({REFRESH_KIND})"

    def _due_refresh_mark(self, now: datetime | None = None) -> str:
        """The newest refresh mark at or before now ("HH:MM"); now's own HH:MM before the first mark (not a mark:
        run_refresh skips it). The ONE refresh job reads its mark here (APScheduler hands the job no fire time)."""
        now = now or self._now()
        hm = f"{now.hour:02d}:{now.minute:02d}"
        due = [m for m in self.refresh_marks if m <= hm]
        return due[-1] if due else hm

    def _next_refresh(self, now: datetime) -> datetime | None:
        """The next refresh mark at or after now on a trading day (the refresh row's next_run_ist)."""
        for ahead in range(15):
            day = now.date() + timedelta(days=ahead)
            if not self.is_trading_day(day):
                continue
            for hm in self.refresh_marks:
                h, m = _hm(hm)
                t = datetime(day.year, day.month, day.day, h, m, tzinfo=IST)
                if t >= now:
                    return t
        return None

    def _refresh_block(self, d: date, now: datetime, hm: str) -> str | None:
        """Why the refresh mark `hm` must not run now (None = run it). Design C3: never two builds at once, never
        before the day's 09:20 build exists (the catch-up owns that), never off the marks / after `last`, never more
        than REFRESH_MISFIRE_S late."""
        r = self.cfg.schedule.refresh
        if hm not in self.refresh_marks:
            return f"{hm} is not a refresh mark ({r.first}..{r.last} every {r.every_min} min)"
        h, m = _hm(hm)
        late_s = (now - now.replace(hour=h, minute=m, second=0, microsecond=0)).total_seconds()
        if late_s > REFRESH_MISFIRE_S:
            return f"started {now:%H:%M:%S}, more than {REFRESH_MISFIRE_S // 60} min after the mark"
        if self._job_lock.locked():
            return "a list build or levels run is still running; the next mark catches up"
        claim = self._claims_0920.get(d)
        if claim in ("sched", "catchup"):
            return f"the 09:20 build is running ({claim})"
        if not self._has_0920_build(d):
            return "no 09:20 build today yet (the 09:20 catch-up owns that)"
        return None

    async def run_refresh(self, hm: str) -> str:
        """T-277 (design C3 / C4): ONE refresh mark "HH:MM" = a price-only re-mark (mark_levels ltp, base auto,
        refresh=<HHMM>) + build_watchlists(refresh, mark_hm=<HHMM>) + load_day + the WS watchlist event; no regime, no
        carry, no retire, no FAR after_build, no list post. A refused / failed mark logs once, sets the row, builds
        nothing and is not retried before the next mark. At a `group_additions` mark the rows added since the last list
        post go to the group AFTER it (also when it failed or was skipped). Trading days only; never raises."""
        st = self.job_state.setdefault(REFRESH_JOB, JobState(self._refresh_name()))
        now = self._now()
        d = now.date()
        if not self.is_trading_day(d):
            st.last_run_ist, st.last_status, st.detail = now.isoformat(), "skipped", f"{d} is not an NSE trading day"
            log.info("refresh %s skipped: %s", hm, st.detail)
            return "skipped"
        hhmm = hm.replace(":", "")
        try:
            why = self._refresh_block(d, now, hm)
            if why is not None:
                st.last_run_ist, st.last_status, st.detail = now.isoformat(), "skipped", why
                log.info("refresh %s skipped: %s", hm, why)
                self.hub.publish("status", self.status())
                status = "skipped"
            else:
                status = await self._execute(
                    REFRESH_JOB, [("mark_levels", "ltp"), ("build_watchlists", REFRESH_KIND)],
                    f"sched-refresh-{hhmm}-{d:%Y%m%d}", quiet=True, mark_kw={"refresh_hm": hhmm},
                    build_kw={"mark_hm": hhmm},
                )  # fmt: skip
        except Exception as exc:  # never into the scheduler
            status, st.last_status, st.detail = "failed", "failed", f"{type(exc).__name__}: {exc}"
            log.exception("refresh %s failed", hm)
        if hm in self.group_additions:
            await self._post_additions(d, hm)
        return status

    async def _post_additions(self, d: date, hm: str) -> None:
        """Design C4: the router posts the live non-CARRY rows added since the day's last list post (it derives that
        from the store), called as post_pending_additions("<YYYY-MM-DD>", hm="<HHMM>"). Never raises."""
        post = getattr(self.router, "post_pending_additions", None)
        if post is None:
            log.warning("refresh %s: the router has no post_pending_additions; no group additions post", hm)
            return
        try:
            res = await post(d.isoformat(), hm=hm.replace(":", ""))
            log.info("refresh %s: group additions: %s", hm, res)
        except Exception:
            log.exception("refresh %s: the group additions post failed", hm)

    def _has_ltp_levels(self, d: date) -> bool:
        """An ok / partial mark_levels(ltp) run that wrote levels for `d` exists (build_watchlists'
        latest_levels_run). Unreadable = True: never add a broker pass on a guess."""
        try:
            from gammad.jobs.build_watchlists import latest_levels_run

            return latest_levels_run(self.conn, d, "ltp") is not None
        except Exception:
            log.exception("levels runs read failed; the slot runs as scheduled")
            return True

    async def run_scheduled_slot(self, job_id: str) -> str:
        """The SCHEDULER's entry (schedule_jobs). W14 H5: for the 09:20-kind slot the day's intent is claimed BEFORE
        queueing on _job_lock, and a login catch-up that claimed it first (running, or done) makes this fire a skip.
        Only the scheduled fire and the catch-up share the claim: run_slot itself (tools/replay_day.py) and
        run_job (tray "Run 09:20 now", API, CLI) never consult it -- a second 0920-kind run a day is legitimate
        (D106(5) / T-108)."""
        slot = next((s for s in self.slots if s.job_id == job_id), None)
        if slot is None or slot.build_kind != RETIRE_BUILD_KIND:
            return await self.run_slot(job_id)
        today = self.today()
        claim = self._claims_0920.get(today)
        if claim is not None:
            st = self.job_state[job_id]
            st.last_run_ist, st.last_status = self._now().isoformat(), "skipped"
            st.detail = f"{today}: the 09:20 build is {'already done' if claim == 'done' else 'running as ' + claim}"
            log.info("job %s skipped: %s", job_id, st.detail)
            return "skipped"
        self._claims_0920[today] = "sched"  # claimed NOW, before run_slot waits for _job_lock
        status = "failed"
        try:
            status = await self.run_slot(job_id)
        finally:
            self._settle_0920(today, status)
        return status

    def _settle_0920(self, d: date, status: str) -> None:
        """A 09:20 claim's outcome: ok / partial = the day is done; anything else (failed, auth_error, skipped as a
        holiday, cancelled) releases the claim so a later login may catch up."""
        if status in ("ok", "partial"):
            self._claims_0920[d] = "done"  # T-310: its list post task releases the held V1 signals (_post_then_release)
        else:
            self._claims_0920.pop(d, None)
            if status == "failed":  # T-213 (D186(1)): a crash while the token stays valid is retried ONCE a day
                first = d not in self._retry_0920_days
                self._arm_0920_retry(d)
                if first and d in self._retry_0920_days:
                    return  # T-310: the held V1 signals wait for the retry's list post
            # T-310 (D278(5)): no 09:20 list post follows this run (its last retry failed, or auth / skip): release now
            self._spawn_hold_release(d, f"the 09:20 run ended {status}")

    def _arm_0920_retry(self, d: date) -> None:
        """T-213 (D189(5), audit F1): on_token_valid fires only on a token TRANSITION, so a 09:20 build that failed on a
        long-running service with a valid token was never re-run (30 Sep: until the owner restarted at 09:53). ONE
        retry per day, `retry_0920_after_s` later, through catch_up_0920 (its claim, window, trading-day and
        no-09:20-build rules). A daemon task: flush() never waits out the delay."""
        if d in self._retry_0920_days:
            return
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return
        self._retry_0920_days.add(d)
        log.warning("the 09:20 build of %s failed: one retry in %.0f s", d, self.retry_0920_after_s)
        self.spawn(self._retry_0920_later(d), name=f"retry-0920-{d.isoformat()}", daemon=True)

    async def _retry_0920_later(self, d: date) -> str | None:
        await asyncio.sleep(self.retry_0920_after_s)
        if self._now().date() != d:
            return None
        run_id = self.catch_up_0920()
        if run_id is None:
            log.info("09:20 retry of %s: nothing to do (built meanwhile, out of the window, or claimed)", d)
            if self._claims_0920.get(d) not in ("sched", "catchup"):  # T-310: the last retry point: never held all day
                await self._release_v1_hold(d, "the 09:20 retry had nothing to run")
            return None
        task = next((t for t in self._tasks if t.get_name() == run_id), None)
        if task is not None:  # the retry task ends with the run it started (tests await it; the scheduler never does)
            await asyncio.wait({task})
        return run_id

    # ---------------------------------------------------------------- T-310: V1 before the 09:20 list post = HOLD
    # Owner ruling D278(5): V1 tracks OI and bars from 09:15 (engine state unchanged), but no V1 card or trade goes out
    # before the day's first 09:20-kind list post; a signal before then is sent right after the post if still valid.
    # The router records a held row at signal time (ctx_json held = pending: resume / R4 / restart read the store) and
    # routes nothing; `_release_v1_hold` hands the day to router.release_held. Release points: right after the first
    # 09:20-kind build's list post is attempted (sent or failed; no rows to post = right after the build), else the 09:20
    # run's last retry (_settle_0920 / _retry_0920_later), else the supervisor's pass once no 09:20 run is pending.
    def _hold_slot_ms(self, d: date) -> int | None:
        """The 09:20 slot's time on `d` (epoch ms); None = no 09:20-kind slot configured (nothing is ever held)."""
        slot = next((s for s in self.slots if s.build_kind == RETIRE_BUILD_KIND), None)
        if slot is None:
            return None
        h, m = _hm(slot.at)
        return int(datetime.combine(d, dtime(h, m), tzinfo=IST).timestamp() * 1000)

    def _hold_waiting(self, d: date) -> bool:
        """Is the day's 09:20-kind list post still to come? Its post + release task is in flight, a scheduled / catch-up
        09:20 run holds the claim, its one retry is armed, or (a scheduling runtime) no 09:20 build exists yet and the
        last retry's time (slot + wait_0920_s + retry_0920_after_s) has not passed."""
        if d in self._hold_posting or self._claims_0920.get(d) in ("sched", "catchup") or d in self._retry_0920_days:
            return True
        slot_ms = self._hold_slot_ms(d)
        if self.scheduler is None or slot_ms is None:
            return False
        deadline = slot_ms + int((self.wait_0920_s + self.retry_0920_after_s) * 1000)
        now = self._now()
        if now.date() != d or now.timestamp() * 1000 >= deadline:
            return False
        return not self._has_0920_build(d)

    def v1_hold(self, sig: Signal) -> bool:
        """The router's hold rule for a V1 signal (router.v1_hold): True = record it held. Held = the day's hold is
        not released AND (the signal is at / before the 09:20 slot, or the day's 09:20 list post is still to come).
        A held signal's lowest price from here on is tracked tick by tick (handle_tick) for its release check.
        T-310 review M2: from a release's spawn until its last held row is stamped, a new V1 signal is HELD too: the
        release re-reads the store and sends it behind the earlier held ones, in signal-time order (R4 never lets a later
        live signal take the ENTRY of an earlier held one)."""
        d = ist_date(sig.ts_ms)
        if not self._hold_releasing.get(d):
            if d in self._hold_released:
                return False
            slot_ms = self._hold_slot_ms(d)
            if slot_ms is None or (sig.ts_ms > slot_ms and not self._hold_waiting(d)):
                return False
        self._held_lows.setdefault(sig.security_id, {})[int(sig.ts_ms)] = float(sig.ltp)
        return True

    def _held_low(self, sid: int, ts_ms: int) -> float | None:
        """The lowest price of `sid` since its held signal at `ts_ms`: the ticks tracked since the hold (or since a
        restart), the signal-tf bars that started at / after it (closed today + the forming one) and (T-310 review L5)
        the recorder's oi_series rows at / after it -- a restart between the signal and the release keeps that evidence
        (oi_series holds one row per second: a print inside a second can be missed; the signal bar's own low is never
        used, it includes the prices before the signal). None = unknown."""
        lows: list[float] = []
        tracked = self._held_lows.get(sid, {}).get(int(ts_ms))
        if tracked is not None and tracked != float("inf"):
            lows.append(float(tracked))
        series = self.series.get(sid)
        if series is not None:
            lows += [float(b.low) for b in series.today() if b.ts_ms >= ts_ms]
        builder = self.builders.get(sid)
        if builder is not None:
            fb = builder.forming_bar(self.signal_tf)
            if fb is not None and fb.start_ms >= ts_ms:
                lows.append(float(fb.low))
        try:
            row = self.conn.execute(
                "SELECT MIN(ltp) FROM oi_series WHERE ts_ms >= ? AND ts_ms < ? AND security_id = ? AND ltp > 0",
                (int(ts_ms), int(ts_ms) + 86_400_000, int(sid)),
            ).fetchone()
            if row is not None and row[0] is not None:
                lows.append(float(row[0]))
        except sqlite3.Error:
            log.warning("T-310: the recorded ticks of %s could not be read for its held signal", sid)
        return min(lows) if lows else None

    def _has_held(self, d: date) -> bool:
        has_held = getattr(self.router, "has_held", None)  # tools / tests with a minimal router have none
        try:
            return has_held is not None and has_held(d.isoformat()) is True
        except Exception:
            log.exception("T-310: the held V1 signals of %s could not be read", d)
            return False

    def _0920_post_state(self, d: date) -> str:
        """T-310 review L9: what became of the day's 09:20-kind GROUP list post (manual_sends watchlist_post rows whose
        ref names the day and the 09:20 label, as _posted_0920): router.HELD_POST_SENT (one sent; also when unreadable,
        as _posted_0920), HELD_POST_FAILED (rows, none sent: failed / paused), else HELD_POST_NONE."""
        from gammad.alerts.router import (
            HELD_POST_FAILED,
            HELD_POST_NONE,
            HELD_POST_SENT,
            KIND_WATCHLIST_POST,
            ST_SENT,
        )

        try:
            rows = self.conn.execute("SELECT status, ref FROM manual_sends WHERE kind = ?",
                                     (KIND_WATCHLIST_POST,)).fetchall()  # fmt: skip
        except Exception:
            log.exception("manual_sends read failed; the held V1 cards say the 09:20 list was posted")
            return HELD_POST_SENT
        label = self._post_label(RETIRE_BUILD_KIND)
        seen: list[str] = []
        for status, raw in rows:
            try:
                ref = json.loads(raw or "{}")
            except ValueError:
                continue
            if isinstance(ref, dict) and ref.get("date") == d.isoformat() and ref.get("label") == label:
                seen.append(str(status))
        if ST_SENT in seen:
            return HELD_POST_SENT
        return HELD_POST_FAILED if seen else HELD_POST_NONE

    def _spawn_hold_release(self, d: date, why: str) -> None:
        """Spawn a release unless one is in flight (it re-reads the store to its end) or nothing is left to release.
        From here on the day counts as releasing (v1_hold holds a new V1 signal: review M2)."""
        if self._hold_releasing.get(d):
            return
        if d in self._hold_released and not self._has_held(d):
            return
        self._hold_releasing[d] = 1
        coro = self._release_v1_hold(d, why, spawned=True)
        try:
            self.spawn(coro, name=f"v1-hold-release-{d.isoformat()}")
        except Exception:  # never leave the day "releasing" (every V1 signal would stay held) without a task
            coro.close()
            self._hold_releasing.pop(d, None)
            log.exception("T-310: the V1 hold release of %s could not be started", d)

    async def _release_v1_hold(self, d: date, why: str, *, spawned: bool = False) -> None:
        """Release the day's held V1 signals (router.release_held reads them from the store, so a second process
        releases what the first held, and a second call only what is still held). Never raises. T-310 review: the day
        counts as releasing until the router stamped its last held row (M2, `_hold_releasing`); the cards say what
        became of the 09:20 list post (L9, `_0920_post_state`)."""
        if not spawned:
            self._hold_releasing[d] = self._hold_releasing.get(d, 0) + 1
        try:
            if d in self._hold_released and not self._has_held(d):
                return
            self._hold_released.add(d)
            release = getattr(self.router, "release_held", None)  # tools / tests with a minimal router have none
            if release is not None:
                res = await release(d.isoformat(), ltp_of=self.last_ltp.get, low_since=self._held_low,
                                    now_ms=self._wall_ms(), list_post=self._0920_post_state(d))  # fmt: skip
                if any(res.values()):
                    (log.debug if _edition.is_client() else log.info)("V1 hold released (%s): %s", why, res)
        except Exception:
            log.exception("releasing the held V1 signals of %s failed", d)
        finally:
            left = self._hold_releasing.get(d, 1) - 1
            if left > 0:
                self._hold_releasing[d] = left
            else:
                self._hold_releasing.pop(d, None)
                self._held_lows.clear()
            self._hold_posting.discard(d)

    async def _post_then_release(self, d: date, post: Awaitable[Any]) -> Any:
        """The 09:20-kind list post, then (sent, failed or raised) the hold's release: the held cards go right after it.
        Spawned under the post's own task name, so a raising post is still reported as that task's crash."""
        try:
            return await post
        finally:
            await self._release_v1_hold(d, "after the 09:20 list post")

    def _hold_pass(self, now: datetime) -> None:
        """The supervisor's T-310 pass (each day pass): a restarted process tracks the store's held rows from its first
        tick; past the 09:20 slot with no 09:20 list post still to come, whatever is held is released (a restart after
        the post, a day whose 09:20 build had nothing to post, the last retry's time on a scheduling runtime).
        Review fixes: (L6) a day's first pass stamps every EARLIER day's still-held row invalid (expired), and a row still
        held after the day's release is released again; (L9) past the slot the hold is released only once a 09:20-kind
        list post row exists for the day (sent or failed) or the last retry's time (slot + wait_0920_s +
        retry_0920_after_s) has passed: a restart between the 09:20 build and its post never sends "after the 09:20
        list" for a list that was never posted."""
        from gammad.alerts.router import HELD_POST_NONE

        d = now.date()
        if d not in self._hold_expired:
            self._hold_expired.add(d)
            expire = getattr(self.router, "expire_held", None)
            if expire is not None:
                expire(d.isoformat())
        if self.trade_date != d:
            return
        if d in self._hold_released:
            if not self._hold_releasing.get(d) and self._has_held(d):
                self._spawn_hold_release(d, "a row still held after the day's release")
            return
        slot_ms = self._hold_slot_ms(d)
        if slot_ms is None:
            return
        keys = getattr(self.router, "held_keys", None)
        if d not in self._hold_seen and keys is not None:
            self._hold_seen.add(d)
            for sid, ts in keys(d.isoformat()):
                self._held_lows.setdefault(sid, {}).setdefault(ts, float("inf"))
        now_ms = now.timestamp() * 1000
        if now_ms <= slot_ms or self._hold_waiting(d):
            return
        if not self._has_held(d):
            self._hold_released.add(d)  # nothing held: nothing to send, no task
            return
        deadline = slot_ms + int((self.wait_0920_s + self.retry_0920_after_s) * 1000)
        if now_ms < deadline and self._0920_post_state(d) == HELD_POST_NONE:
            return  # L9: the 09:20 build may have run but its list post has not (a restart in between): wait for it
        self._spawn_hold_release(d, "no 09:20 list post pending")

    # ---------------------------------------------------------------- D45 watchlist posts
    def _watchlist_keys(self, d: date) -> set[tuple[str, str, int]]:
        rows = self.conn.execute(
            "SELECT list_name, expiry_date, security_id FROM watchlist WHERE trade_date = ? AND retired_run_id IS NULL"
            + _edition.v1_lists_sql(),  # D429: a client's counts / additions never hold an EXPIRY row
            (d.isoformat(),),
        ).fetchall()
        return {(str(a), str(b), int(c)) for a, b, c in rows}

    def _new_rows_by_list(
        self, d: date, before: set[tuple[str, str, int]]
    ) -> dict[tuple[str, str], list[dict[str, Any]]]:
        """Rows the build just added (not in `before`), grouped per (list_name, expiry_date)."""
        from gammad.alerts.router import fetch_watchlist_rows

        out: dict[tuple[str, str], list[dict[str, Any]]] = {}
        for r in fetch_watchlist_rows(self.conn, d.isoformat()):
            if (r["list_name"], r["expiry_date"], int(r["security_id"])) in before:
                continue
            ltp = self.last_ltp.get(int(r["security_id"]))
            if ltp is not None:
                r["ltp"] = ltp
            out.setdefault((str(r["list_name"]), str(r["expiry_date"])), []).append(r)
        return out

    def _late_hm(self, kind: str) -> str | None:
        """T-215 (F11, D189(6)): the post time "HH:MM" when a 09:20-kind post goes LATE_POST_AFTER_MIN or more after
        the slot (a login catch-up, a retry, "Run 09:20 now", a woken PC), else None. The label stays `09:20 build`."""
        slot = next((s for s in self.slots if s.build_kind == RETIRE_BUILD_KIND), None)
        if kind != RETIRE_BUILD_KIND or slot is None:
            return None
        now = self._now()
        h, m = _hm(slot.at)
        if now < now.replace(hour=h, minute=m, second=0, microsecond=0) + timedelta(minutes=LATE_POST_AFTER_MIN):
            return None
        return f"{now:%H:%M}"

    def _post_label(self, kind: str) -> str:
        if kind == "0920":
            return "09:20 build"
        if len(kind) == 4 and kind.isdigit():
            return f"{kind[:2]}:{kind[2:]} additions"
        return f"manual build {self._now():%H:%M}"

    async def _execute(
        self, key: str, steps: list[tuple[str, str]], run_id: str, *, post: bool = False, post_if_unposted: bool = False,
        ensure_ltp_levels: bool = False, quiet: bool = False, lock_wait_s: float | None = None,
        mark_kw: Mapping[str, Any] | None = None, build_kw: Mapping[str, Any] | None = None,
    ) -> str:
        """`post` = post the rows this build ADDED (D45). `post_if_unposted` (T-181 F2, run_job): after a 09:20-kind
        build on a trading day with no 09:20 list post sent today, post EVERY live list row of the day (the rows may
        not be new: a rebuild after a failed post re-finds the same contracts).
        T-277: `quiet` (the 09:10 slot, a refresh mark) = a failure / auth rejection logs and sets the status but sends
        no SYSTEM / TOKEN line (the next run tries again; the deduped price-refusal line still goes out).
        `lock_wait_s` = wait at most this long for _job_lock (None = as long as it takes), then fail.
        `mark_kw` / `build_kw` = extra keywords for every mark_levels / build_watchlists step (a refresh's mark)."""
        st = self.job_state.setdefault(key, JobState(key))
        if lock_wait_s is not None and self._job_lock.locked():
            log.info("job %s: waiting up to %.0f s for the running job to finish", key, lock_wait_s)
        try:
            await asyncio.wait_for(self._job_lock.acquire(), lock_wait_s)
        except TimeoutError:
            st.last_run_ist, st.last_status, st.last_run_id = self._now().isoformat(), "failed", run_id
            st.detail = f"waited {lock_wait_s:.0f} s for the running job; not run"
            log.error("job %s: %s", key, st.detail)
            if not quiet:
                self._report_failure(f"job {key} FAILED: {st.detail}")
            self.hub.publish("status", self.status())
            return "failed"
        try:  # = `async with self._job_lock` (acquired above, bounded); released in the finally
            d = self.today()
            st.last_run_ist, st.last_status, st.last_run_id, st.detail = self._now().isoformat(), "running", run_id, None
            if ensure_ltp_levels and not self._has_ltp_levels(d):  # 0.8.3 T-229 (4)(b), under the lock (L3)
                log.warning("job %s: no ok/partial ltp levels run today: mark_levels(ltp) runs first", key)
                steps = [("mark_levels", "ltp"), *steps]
            status = "ok"
            raised = False
            try:
                for step, arg in steps:
                    if step == "mark_levels":
                        # T-267 (M14): a full run (09:10 prevclose / 09:20 with no base) hands its daily candles to the
                        # V2 regime step; T-277: a price-only re-mark fetches none, so the regime stays as computed
                        sink: dict[int, list[Candle]] | None = {} if self._wants_regime(arg) else None
                        kw = self._mark_kwargs(d, arg, steps, mark_kw)
                        summary = await asyncio.to_thread(self._call_mark_levels, d, arg, sink, **kw)
                        self._publish_levels_event(d, arg, summary)
                        if str(summary.get("status", "ok")) in ("ok", "partial"):
                            self._clear_data_problem()  # 0.8.3 T-229 (2): prices flow again
                        if sink and summary.get("bars_fetched", True):
                            await self._v2_regime_step(d, summary, sink)
                        elif sink and summary.get("regime_bars"):  # D278(1): the 09:20 re-mark's full marks
                            await self._v2_regime_step(d, summary, sink, merge=True)
                    else:
                        before = self._watchlist_keys(d)
                        summary = {"status": "failed"}  # D127: the event goes out even when the build raises
                        try:
                            if arg == RETIRE_BUILD_KIND and self.jobs.retire is not None:
                                # T-063 M1: retire FIRST and drop the retired contracts, THEN the (minute-long)
                                # chain loop; a retired contract must route no E1 while the build runs
                                from gammad.jobs.build_watchlists import new_run_id

                                build_run = new_run_id(d, arg, self._now())
                                await asyncio.to_thread(self.jobs.retire, d, build_run)
                                async with self._day_lock:
                                    if await self.ensure_day(d):
                                        await self.load_day(d)
                                summary = await asyncio.to_thread(
                                    self.jobs.build_watchlists, d, arg, run_id=build_run, retire=False
                                )
                            else:  # T-277: a refresh's build gets its mark (`mark_hm`) through build_kw
                                summary = await asyncio.to_thread(self._call_job, self.jobs.build_watchlists, d, arg,
                                                                  **dict(build_kw or {}))  # fmt: skip
                            async with self._day_lock:  # T-107: not while a day pass loads (double subscribe/warm-up)
                                if await self.ensure_day(d):
                                    await self.load_day(d)
                            if (arg == RETIRE_BUILD_KIND and self.owner_ext is not None
                                    and str(summary.get("status", "ok")) != "failed"):  # fmt: skip
                                # 0.8.2 (C.7(5), B3, seam S7): the owner's FAR inputs, only after load_day's warm-up
                                self.spawn(self.owner_ext.after_build(d, arg), name="owner-after-build")
                        finally:
                            # a retire-only step or a raised build still changed (or failed to change) the rows:
                            # the dashboard refetches on the event and shows the failed status (skeptic 2a)
                            self._publish_watchlist_event(d, arg, summary)
                        catch_up_post = (
                            not post and post_if_unposted and arg == RETIRE_BUILD_KIND and self.is_trading_day(d)
                            and not self._posted_0920(d)
                        )  # fmt: skip
                        if (post or catch_up_post) and str(summary.get("status", "ok")) != "failed":
                            if catch_up_post:
                                log.info("job %s: no 09:20 list post sent today: posting every list", key)
                            # T-215 (F4, D45's intent): a 09:20-kind post carries EVERY live row of the day (a
                            # pre-market manual build made the 25 Sep post a 32-of-149 fragment); "the rows this build
                            # added" stays for a manual 1315-kind post only (D148(3): the schedule posts no 13:15)
                            added_only = post and arg != RETIRE_BUILD_KIND
                            new = self._new_rows_by_list(d, before if added_only else set())
                            if new and arg == RETIRE_BUILD_KIND and self._posted_0920(d):
                                # the whole-list post makes a second 09:20-kind run (retry, catch-up after a
                                # manual post) a DUPLICATE post: never a second 09:20 post a day
                                log.info("job %s: the 09:20 lists were posted today already; not posted again", key)
                                new = {}
                            if new:
                                post_coro = self.router.post_watchlists(d.isoformat(), new, label=self._post_label(arg),
                                                                        late_hm=self._late_hm(arg))  # fmt: skip
                                if arg == RETIRE_BUILD_KIND:  # T-310: the held V1 signals go right after this post
                                    self._hold_posting.add(d)
                                    post_coro = self._post_then_release(d, post_coro)
                                self.spawn(post_coro, name=f"watchlist-post-{arg}")
                            if arg == RETIRE_BUILD_KIND and not _edition.is_client():
                                # S1.5.0 (D160(4)): `+N carried (P primed)` to the PERSONAL chat, never the group
                                self.spawn(self.router.send_carry_line(d.isoformat()), name="carry-line")
                        if (arg == RETIRE_BUILD_KIND and str(summary.get("status", "ok")) != "failed"
                                and d not in self._hold_posting):  # fmt: skip
                            # T-310: a 09:20-kind build with no list post to make (nothing new, posted already, not
                            # asked) still ends the hold: the day's lists are built
                            self._spawn_hold_release(d, "after the 09:20 build (no list post)")
                    s = str(summary.get("status", "ok"))
                    log.info("job %s step %s(%s): %s", key, step, arg, s)
                    if s == "failed":
                        status = "failed"
                        why = summary.get("error") or summary.get("note")
                        st.detail = f"step {step}({arg}) failed" + (f": {why}" if why else "")
                        break
                    if s == "partial":
                        status = "partial"
            except BrokerEntitlementError as exc:  # 0.8.3 T-229 (2), D230: login fine, prices refused
                status, st.detail = "auth_error", str(exc)
                log.error("job %s: broker refused price data (%s %s): %s", key, exc.broker, exc.native, exc)
                seen = (d, exc.broker, str(exc.native))
                if seen in self._refusals_sent:  # L4: one TOKEN line (and row) per broker per day per refusal
                    self.data_problem = exc.hint
                    log.info("job %s: the same price refusal was told today; no second TOKEN line", key)
                else:
                    self._refusals_sent.add(seen)
                    self._record_data_refused(exc)
                    # the "not built yet" sentence only on a LIST job (a build step: 09:20 / 13:15, not the 16:00
                    # close mark, L4) while today's 09:20 lists are not built (an ok / partial 0920-kind build or its
                    # live rows; unreadable counts as built: never claim a false "not built")
                    list_job = any(s == "build_watchlists" for s, _ in steps)
                    built = (not list_job) or self._has_0920_build(d)
                    line = token_line(exc.broker, lists_built=built) or f"job {key} stopped: broker auth rejected"
                    self.spawn(self.router.send_system(line, "TOKEN"))
            except BrokerAuthError as exc:
                status, st.detail = "auth_error", str(exc)
                log.error("job %s: broker auth rejected: %s", key, exc)
                if not quiet:  # T-277: a 09:10 / refresh rejection is not told (the token watcher / 09:20 tell it)
                    self.spawn(self.router.send_system(f"job {key} stopped: broker auth rejected", "TOKEN"))
            except Exception as exc:  # a job failure is reported, never allowed to kill the supervisor
                status, st.detail = "failed", f"{type(exc).__name__}: {exc}"
                log.exception("job %s failed", key)
                raised = True
            if status == "failed" and not quiet:  # T-213 (audit F1: 30 Sep 09:21:50 died with an ERROR line only)
                self._report_failure(f"job {key} FAILED: {st.detail or 'no detail'}")
            elif status == "failed" and not raised:  # T-277: ONE line for a refused / failed 09:10 or refresh step
                log.warning("job %s failed, nothing sent; the next run tries again: %s", key, st.detail or "no detail")
            st.last_status = status
            self.hub.publish("status", self.status())
            if status in ("ok", "partial") and any(s == "build_watchlists" for s, _ in steps):
                self.refresh_readiness("list build")  # T-346 (D386 orchestrator fix): lists built = a fixed problem clears itself
            return status
        finally:
            self._job_lock.release()

    # ---------------------------------------------------------------- scheduler
    def make_scheduler(self) -> Any:
        from apscheduler.schedulers.asyncio import AsyncIOScheduler

        return AsyncIOScheduler(timezone=IST)

    def _schedule_close_day(self, day) -> None:
        from apscheduler.triggers.cron import CronTrigger

        at = v1_hours.day_mark_at(day)
        self._close_day_at = at
        label = f"{at:%H:%M} close_day"
        if CLOSE_DAY_JOB in getattr(self, "job_state", {}):
            self.job_state[CLOSE_DAY_JOB].name = label
        if self.scheduler is not None:
            self.scheduler.add_job(
                self.run_slot, CronTrigger(hour=at.hour, minute=at.minute, timezone=IST),
                args=[CLOSE_DAY_JOB], id=CLOSE_DAY_JOB, name=label, replace_existing=True,
                coalesce=True, max_instances=1, misfire_grace_time=3600,
            )

    def _schedule_day_end(self, day: date) -> None:
        """Replace day-end triggers whose era clock changed at the start of a session."""
        if self.scheduler is None:
            return
        from apscheduler.triggers.cron import CronTrigger

        if self.grader is not None:
            from gammad.scan import review as scan_review

            async def scanner_day_end() -> Any:
                return await scan_review.run_day_end(self.conn, self.grader, self.router, self._now())

            hh, mm = _hm(session_clock.day_mark_hm(day))
            trig = CronTrigger(hour=hh, minute=mm, timezone=IST)
            job = self.scheduler.get_job(SCANNER_DAY_END_JOB)
            if job is None or str(job.trigger) != str(trig):
                self.scheduler.add_job(
                    scanner_day_end, trig, id=SCANNER_DAY_END_JOB, name=SCANNER_DAY_END_JOB,
                    replace_existing=True, coalesce=True, max_instances=1, misfire_grace_time=600,
                )
        if self.owner_ext is not None:
            for job_id, fn, trig in self.owner_ext.jobs(day):
                job = self.scheduler.get_job(job_id)
                if job is None or str(job.trigger) != str(trig):
                    self.scheduler.add_job(
                        fn, trig, id=job_id, name=job_id, replace_existing=True, coalesce=True, max_instances=1,
                        misfire_grace_time=self.owner_ext.misfire_s.get(job_id, 300),
                    )

    def schedule_jobs(self) -> Any:
        from apscheduler.triggers.cron import CronTrigger

        if self.scheduler is None:
            self.scheduler = self.make_scheduler()
        for slot in self.slots:
            if not slot.scheduled:  # T-277: the refresh runs this time (the old 13:15): no second job beside it
                continue
            h, m = _hm(slot.at)
            self.scheduler.add_job(
                self.run_scheduled_slot,  # W14 H5: the 09:20 claim shared with the login catch-up
                CronTrigger(hour=h, minute=m, timezone=IST),
                args=[slot.job_id],
                id=slot.job_id,
                name=slot.name,
                replace_existing=True,
                coalesce=True,
                max_instances=1,
                # T-133 / PLAN-W16 section 13 S0(3): the 09:20 slot tolerates a 25-min late fire (a PC woken at
                # 08:55 that is up by 09:4x still builds); the H5 claim keeps it to one build a day. Others 5 min.
                misfire_grace_time=1500 if slot.build_kind == RETIRE_BUILD_KIND else 300,
            )
        refresh = refresh_trigger(self.refresh_marks)
        if refresh is not None:  # T-277: ONE job fires at every refresh mark; the handler reads the mark off the clock

            async def refresh_job() -> Any:
                return await self.run_refresh(self._due_refresh_mark())

            self.scheduler.add_job(
                refresh_job, refresh, id=REFRESH_JOB, name=REFRESH_JOB, replace_existing=True, coalesce=True,
                # 2: a mark that fires while the previous one still runs reaches run_refresh, which logs its skip
                max_instances=2, misfire_grace_time=REFRESH_MISFIRE_S,
            )  # fmt: skip
        if self.grader is not None:  # W15 §6: the owner's scanner review cards (PERSONAL chat)
            from gammad.scan import review as scan_review

            async def scanner_day_end() -> Any:
                return await scan_review.run_day_end(self.conn, self.grader, self.router, self._now())

            async def scanner_weekly() -> Any:
                t = self._now()
                if not cal.default_calendar().is_trading_day(t.date()):
                    return None
                return await scan_review.run_weekly(self.conn, self.router, t)

            hh, mm = _hm(session_clock.day_mark_hm(self._now().date()))
            for job_id, fn, trig in (
                (SCANNER_DAY_END_JOB, scanner_day_end, CronTrigger(hour=hh, minute=mm, timezone=IST)),
                (SCANNER_WEEKLY_JOB, scanner_weekly, CronTrigger(day_of_week="fri", hour=16, minute=5, timezone=IST)),
            ):
                self.scheduler.add_job(
                    fn, trig, id=job_id, name=job_id, replace_existing=True, coalesce=True, max_instances=1,
                    misfire_grace_time=600,
                )  # fmt: skip
        # W16 S2 (T-132, B4): 09:00 readiness card, 09:10 all-clear; both editions; trading days only; the 09:20
        # grace (1500 s) applies so a PC woken at 08:55 that is up by 09:2x still gets its card
        for job_id, fn, hh, mm in ((READINESS_0900_JOB, self.readiness_0900, 9, 0), (READINESS_0910_JOB, self.readiness_0910, 9, 10)):
            self.scheduler.add_job(
                fn, CronTrigger(hour=hh, minute=mm, timezone=IST), id=job_id, name=job_id, replace_existing=True,
                coalesce=True, max_instances=1, misfire_grace_time=1500,
            )  # fmt: skip
        # 0.8.9 T-419 (D480 (2)): the NSE F&O ban list for the trade date, both editions, while Still Watching is on
        # read defensively: tools/edition_probe.py schedules on a test double that has cfg but no `sw`
        sw = getattr(self, "sw", None) or still_watching_cfg(getattr(self, "cfg", None))
        if getattr(self, "scanners_089", False) or (sw.enabled and sw.ban_enabled):
            hh, mm = _hm(sw.ban_fetch_hm)
            self.scheduler.add_job(
                getattr(self, "run_sw_ban_job", None) or (lambda: Runtime.run_sw_ban_job(self)),  # type: ignore[arg-type]
                CronTrigger(hour=hh, minute=mm, timezone=IST), id=SW_BAN_JOB, name=SW_BAN_JOB,
                replace_existing=True, coalesce=True, max_instances=1, misfire_grace_time=1800,
            )  # fmt: skip
        if getattr(self, "scanners_089", False) and _edition.is_owner():
            from gammad.jobs.v2sq_eod import READY_AT

            self.scheduler.add_job(self.run_s089_evening,
                                   CronTrigger(hour=READY_AT.hour, minute=READY_AT.minute, timezone=IST),
                                   id="s089-v2sq-evening", replace_existing=True, coalesce=True,
                                   max_instances=1, misfire_grace_time=1800)
        if self.owner_ext is not None:  # 0.8.2 (C.7(4)): the owner shadow jobs; never Telegram, never a trade
            for job_id, fn, trig in self.owner_ext.jobs(self._now().date()):
                self.scheduler.add_job(
                    fn, trig, id=job_id, name=job_id, replace_existing=True, coalesce=True, max_instances=1,
                    misfire_grace_time=self.owner_ext.misfire_s.get(job_id, 300),
                )  # fmt: skip
        Runtime._schedule_close_day(self, self._now().date())
        # Z1(7): trades_eod 15:20, trades_daily 16:05, trades_daily_retry 09:21 (each skips a non-trading day)
        schedule_trade_jobs(self.scheduler, self.tracker, self.adapter, is_trading_day=self.is_trading_day,
                            now=self._now)  # fmt: skip
        # T-267: schedule.h60_sync (08:30 top-up through yesterday; 16:05 sync through today + the M13 reconcile).
        # AM6 (D288): scheduled ONLY when the V2T pool exists (owner); the client has no V2 at all (D287 (6), I7)
        for at in self.cfg.schedule.h60_sync if _runs_v2t(self) else ():
            hh, mm = _hm(at)
            hm = f"{hh:02d}:{mm:02d}"

            async def h60_job(hm: str = hm) -> Any:
                return await self.run_h60_sync_job(hm)

            job_id = H60_SYNC_JOB.format(hm=f"{hh:02d}{mm:02d}")
            self.scheduler.add_job(
                h60_job, CronTrigger(hour=hh, minute=mm, timezone=IST), id=job_id, name=job_id,
                replace_existing=True, coalesce=True, max_instances=1, misfire_grace_time=1800,
            )  # fmt: skip
        return self.scheduler

    async def readiness_0900(self) -> Any:
        """The 09:00 readiness job body (S2): skips a non-trading day; publishes the status block afterwards."""
        now = self._now()
        if not self.is_trading_day(now.date()):
            return None
        r = await self.readiness.run(now)
        self.hub.publish("status", self.status())
        if _edition.is_owner():  # T-211 (D189(8)): a silent evening (service down at 15:35) is caught the next morning
            self.spawn(self._delivery_catch_up(now.date()), name="day-end-delivery-catchup")
        return r

    # ---------------------------------------------------------------- T-211: the 15:35 day end + delivery check
    async def _day_end_telegram(self, day: str) -> None:
        """15:35: the GROUP summary (+ the PERSONAL operator counters), THEN, owner edition only, the daily
        group-delivery check (T-211, D189(2)), which reads the rows the summary just wrote (audit section 5: AWAITED,
        never spawned beside it). A summary that raises is reported and the check still runs (it then flags it)."""
        try:
            if getattr(self, "scanners_089", False):
                # Audit JOBS_ALERTS-03 (owner D714): the 0.8.9 group summary from the V1N book (client lists only).
                from gammad.alerts import s089_summary

                text = s089_summary.day_summary(day, self.s089.group_day_entries(date.fromisoformat(day)))
                await self.router.send_daily_summary(day, group_text=text)
            else:
                await self.router.send_daily_summary(day)
        except Exception as exc:
            log.exception("daily summary failed")
            self._report_failure(f"day summary FAILED: {type(exc).__name__}: {exc}")
        await self.run_delivery_check(day)

    async def run_delivery_check(self, day: str) -> Any:
        """The owner's daily group-delivery check (gammad/scan/delivery_check.py; the client package ships no
        gammad/scan, so the import is guarded like the grader's). Returns its Verdict, or None when not run."""
        if not _edition.is_owner():
            return None
        try:
            from gammad.scan import delivery_check
        except ImportError:  # CLIENT_EXCLUDE: the module is not in the package
            return None
        verdict = await delivery_check.run(self.conn, self.router, day)  # never raises (FIX-WAVE C4)
        self.delivery_last = self._load_delivery_last()
        self.hub.publish("status", self.status())
        return verdict

    def _load_delivery_last(self) -> dict[str, Any] | None:
        if not _edition.is_owner():
            return None
        try:
            from gammad.scan import delivery_check
        except ImportError:
            return None
        try:
            return delivery_check.last_verdict(self.conn)
        except Exception:  # a broken verdict row must never stop the supervisor
            log.exception("delivery check verdict unreadable")
            return None

    def _prev_trading_day(self, d: date) -> date | None:
        for back in range(1, 11):
            p = d - timedelta(days=back)
            if self.is_trading_day(p):
                return p
        return None

    def _has_delivery_verdict(self, day: str) -> bool:
        """A delivery_check manual_sends row names `day` (any status). Unreadable = True: never a second check."""
        try:
            refs = self.conn.execute("SELECT ref FROM manual_sends WHERE kind = ?", (DELIVERY_CHECK_KIND,)).fetchall()
        except Exception:
            log.exception("manual_sends read failed; no delivery-check catch-up")
            return True
        for (raw,) in refs:
            try:
                ref = json.loads(raw or "{}")
            except ValueError:
                continue
            if isinstance(ref, dict) and ref.get("date") == day:
                return True
        return False

    async def _delivery_catch_up(self, today: date) -> Any:
        prev = self._prev_trading_day(today)
        if prev is None or self._has_delivery_verdict(prev.isoformat()):
            return None
        log.warning("no delivery check recorded for %s: running it now", prev)
        return await self.run_delivery_check(prev.isoformat())

    async def readiness_0910(self) -> Any:
        """The 09:10 re-check (S2): only sends the all-clear after a NOT READY at 09:00."""
        now = self._now()
        if not self.is_trading_day(now.date()):
            return None
        r = await self.readiness.recheck(now)
        self.hub.publish("status", self.status())
        return r

    def refresh_readiness(self, why: str) -> asyncio.Task[Any] | None:
        """T-346 (D386 orchestrator fix, both editions): today's readiness is NOT READY -> re-run it now (ReadinessJob.refresh), so a
        login after the 09:10 re-check, or a list build that just worked, clears the 09:00 NOT READY instead of leaving
        it on the dashboard all day. Called after every accepted login / token change (apply_credentials, on_token_valid)
        and every successful list build (_execute). Nothing is spawned when no re-run is due or no loop runs; the refresh
        never sends a second NOT READY card (only the all-clear, once)."""
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return None
        if not self.readiness.stale(self._now()):
            return None
        return self.spawn(self._refresh_readiness(why), name=f"readiness-refresh ({why})")

    async def _refresh_readiness(self, why: str) -> Any:
        now = self._now()
        if not self.is_trading_day(now.date()):
            return None
        r = await self.readiness.refresh(now, why)
        if r is not None:
            self.hub.publish("status", self.status())
        return r

    def _next_run(self, job: Any, now: datetime) -> datetime | None:
        """Next fire time that lands on a trading day (the job skips the others)."""
        t: datetime | None = now
        prev: datetime | None = None
        for _ in range(15):
            t = job.trigger.get_next_fire_time(prev, t)
            if t is None:
                return None
            if self.is_trading_day(t.date()):
                return t
            prev = t
            t = t + timedelta(seconds=1)
        return None

    # ---------------------------------------------------------------- token
    def register_token_watcher(self, obj: Any) -> None:
        """W5 plug point. `snapshot()` (or `token_status()`) feeds the status token block; `update_token`
        becomes the token updater unless one is set (TokenWatcher.attach sets it itself)."""
        self._token_watchers.append(obj)
        for attr in ("snapshot", "token_status"):
            hook = getattr(obj, attr, None)
            if callable(hook):
                self.token_health_hook = hook
                break
        upd = getattr(obj, "update_token", None)
        if callable(upd) and self.token_updater is None:
            self.token_updater = upd
        log.info("token watcher registered: %s", type(obj).__name__)

    def apply_credentials(self, creds: Mapping[str, str] | str, access_token: str | None = None) -> None:
        """on_credentials_changed target for the token watcher: new credentials -> settings, adapter, live feed.

        X2: the broker-aware watcher hands ONE creds mapping {env key: value} (every key a login returned, R6);
        it is merged into the settings' env values, handed WHOLE to `adapter.set_credentials(mapping)` and
        `feed.reload_credentials(mapping)`. The legacy Dhan call (client_id, access_token) keeps working."""
        if isinstance(creds, Mapping):
            self._apply_creds_mapping(creds)
        else:
            client_id, token = creds, access_token or ""
            self.cfg = replace(self.cfg, env=replace(self.cfg.env, dhan_client_id=client_id,
                                                     dhan_access_token=token))  # fmt: skip
            adapter = self.adapter()
            if adapter is not None and hasattr(adapter, "set_token"):
                adapter.set_token(token)
            if self.feed is not None:
                self.feed.reload_credentials(client_id, token)
        self._feed_disabled = None
        self._clear_data_problem()  # 0.8.3 T-229 (4)(a): a new login clears the refusal; the watcher re-judges it
        self.hub.publish("token", {"event": "credentials_changed", **self.token_status()})
        self.catch_up_0920()
        # T-346 (D386 orchestrator fix): an accepted login re-runs a NOT READY readiness. The spawned check runs after the watcher's
        # _apply_locked has set its new level (no await between this call and its _transition), so it reads the new token
        self.refresh_readiness("login")

    # ---------------------------------------------------------------- T-151 / D127: build events for the dashboard
    def _next_change_hm(self) -> str | None:
        """The next build slot after now (HH:MM IST), or None when no later build is scheduled today (a holiday
        or weekend has none: the scheduler skips every slot, `run_scheduled_slot`)."""
        now = self._now()
        if not self.is_trading_day(now.date()):
            return None
        hm = f"{now.hour:02d}:{now.minute:02d}"
        later = {s.at for s in self.slots if s.build_kind and s.scheduled and s.at > hm}
        later |= {m for m in self.refresh_marks if m > hm}  # T-277: every refresh mark is a list change
        return min(later) if later else None

    def _publish_levels_event(self, d: date, ref: str, summary: dict[str, Any]) -> None:
        """WS `levels` after every mark_levels step (D127 contract): the Levels page refetches on it."""
        self.hub.publish(
            "levels",
            {
                "run_id": summary.get("run_id"),
                "ref": ref,
                "trade_date": d.isoformat(),
                "n_symbols": summary.get("n_symbols"),
                "n_failed": summary.get("n_failed"),
                "status": str(summary.get("status", "ok")),
                "built_at": self._now().isoformat(),
            },
        )

    def _publish_watchlist_event(self, d: date, kind: str, summary: dict[str, Any]) -> None:
        """WS `watchlist` after every build_watchlists step (D127 contract): the desk store refetches on it and
        shows "built HH:MM:SS · N contracts · next change HH:MM". Counts are the LIVE rows now in the DB (retired
        rows excluded), never the summary's own count, so the event and /api/watchlists agree by construction."""
        per_list: dict[str, int] = {}
        for list_name, expiry, _sid in self._watchlist_keys(d):
            key = f"{list_name}@{expiry}"
            per_list[key] = per_list.get(key, 0) + 1
        self.hub.publish(
            "watchlist",
            {
                "run_id": summary.get("run_id"),
                "kind": kind,
                "trade_date": d.isoformat(),
                "n_rows": sum(per_list.values()),
                "per_list": per_list,
                "status": str(summary.get("status", "ok")),
                "built_at": self._now().isoformat(),
                "next_change_hm": self._next_change_hm(),
            },
        )

    # ---------------------------------------------------------------- W14 H5: the missed 09:20 build
    def _has_0920_build(self, d: date) -> bool:
        """A 09:20-kind build_watchlists run of `d` is recorded (runs; any status but failed), OR live watchlist
        rows for `d` already exist (T-150(b), D125 L2: a restart after a good build must never rebuild, even when
        the runs row is missing or was recorded failed by a partial levels run)."""
        try:
            row = self.conn.execute(
                "SELECT 1 FROM runs WHERE kind = 'build_watchlists' AND run_id LIKE ? AND status != 'failed' LIMIT 1",
                (f"watchlists-{d.isoformat()}-{RETIRE_BUILD_KIND}-%",),
            ).fetchone()
            if row is None:  # skeptic 2d: only rows a 09:20-KIND build wrote count; a manual pre-market build
                row = self.conn.execute(  # must not block the catch-up (its rows carry a manual run id)
                    "SELECT 1 FROM watchlist WHERE trade_date = ? AND retired_run_id IS NULL AND run_id LIKE ? LIMIT 1",
                    (d.isoformat(), f"watchlists-{d.isoformat()}-{RETIRE_BUILD_KIND}-%"),
                ).fetchone()
        except Exception:  # unreadable = assume it ran: never risk a second 09:20 build
            log.exception("runs read failed; no 09:20 catch-up")
            return True
        return row is not None

    def catch_up_0920(self) -> str | None:
        """Credentials just became valid: on a trading day between the 09:20 slot and 15:20 IST, with NO 09:20-kind
        build recorded today, run that slot (mark_levels + build 0920, retire step included) ONCE through _execute.
        Returns the run id, or None when nothing was due. The day's 09:20 intent is claimed synchronously (on the
        loop) before the job queues: a scheduled 09:20 that claimed it first (queued or running) is left alone, and
        the scheduled fire skips a day this catch-up claimed. A FAILED run releases the claim (the next login may
        try again, one attempt per login); only a successful one marks the day done."""
        slot = next((s for s in self.slots if s.build_kind == RETIRE_BUILD_KIND), None)
        now = self._now()
        d, hm = now.date(), f"{now.hour:02d}:{now.minute:02d}"
        if slot is None or d in self._claims_0920 or not (slot.at <= hm < CATCHUP_UNTIL_HM):
            return None
        if not self.is_trading_day(d):
            return None
        st = self.job_state.get(slot.job_id)
        if st is not None and st.last_status == "running":  # the scheduled 09:20 is on it right now
            return None
        if self._has_0920_build(d):
            return None
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return None
        self._claims_0920[d] = "catchup"  # claimed NOW, before the job waits for _job_lock
        run_id = f"catchup-{slot.job_id}-{now:%Y%m%dT%H%M%S}"
        log.warning("token valid at %s with no 09:20 build today: running the 09:20 slot now (%s)", hm, run_id)
        self.spawn(self._run_catch_up(d, slot, run_id), name=run_id)
        return run_id

    def on_token_valid(self) -> str | None:
        """T-181 F1 (R1): the token is known valid NOW -- the token watcher's first valid poll (attach() wires it), or
        the start's own token check after scheduler.start() (no watcher). A service started after 09:20 with a valid
        token already in .env never saw a login (apply_credentials), and APScheduler sets a late-started cron job's
        first run to TOMORROW (misfire_grace_time only helps a loop already running): no lists, no cards all day.
        This hands over to catch_up_0920() with every W14 H5 rule kept (the claim, 09:20 <= now < 15:20, a trading
        day, no 09:20-kind build recorded today: never a second 09:20 build a day). Returns its run id or None."""
        try:
            valid = self.token_status().get("valid") is True
        except Exception:  # a broken token block must never take the service down
            log.exception("token status failed; no 09:20 catch-up")
            return None
        if not valid:
            return None
        self.refresh_readiness("token valid")  # T-346 (D386 orchestrator fix): a token valid again clears a stale NOT READY
        return self.catch_up_0920()

    async def _start_token_check(self) -> None:
        """No token watcher: probe the token once at start (refresh_token_health), then F1's catch-up check."""
        await self.refresh_token_health()
        self.on_token_valid()

    async def _run_catch_up(self, d: date, slot: Slot, run_id: str) -> str:
        """The catch-up's 09:20 slot under its claim (catch_up_0920 took it); _settle_0920 decides done / released."""
        status = "failed"
        try:
            status = await self._execute(slot.job_id, slot.steps(), run_id, post=True)
        finally:
            self._settle_0920(d, status)
        return status

    def _apply_creds_mapping(self, creds: Mapping[str, str]) -> None:
        new ={str(k): str(v) for k, v in creds.items() if v is not None}
        env = self.cfg.env
        values = {**dict(env.values), **new}
        self.cfg = replace(
            self.cfg,
            env=replace(
                env,
                values=MappingProxyType(values),
                dhan_client_id=new.get("DHAN_CLIENT_ID") or env.dhan_client_id,
                dhan_access_token=new.get("DHAN_ACCESS_TOKEN") or env.dhan_access_token,
            ),
        )
        full = self.broker_creds()
        if not full:
            full = new
        else:
            full = {**full, **new}
        if getattr(self, "session_manager", None) is not None:
            active = self.session_manager.active_account()
            if active is not None:
                active.credentials = dict(full)
        adapter = self.adapter()
        if adapter is not None:
            if hasattr(adapter, "set_credentials"):
                adapter.set_credentials(full)
            elif hasattr(adapter, "set_token"):
                entry = self.broker_entry
                if getattr(self, "session_manager", None) is not None:
                    active = self.session_manager.active_account()
                    if active is not None:
                        from gammad.broker import registry

                        try:
                            entry = registry.get(active.identity.broker)
                        except Exception:
                            pass
                tok_key = entry.token_key if entry is not None else "TOKEN"
                adapter.set_token(full.get(tok_key, ""))
        if self.feed is not None:
            self.feed.reload_credentials(full)

    def on_auth_error(self, code: int, msg: str) -> None:
        """Feed callback (806-809, handshake 401/403). A registered watcher owns token_events + alerts;
        without one the runtime records the event and sends the TOKEN alert itself."""
        log.error("feed auth error %s: %s", code, msg)
        handled = False
        for w in self._token_watchers:
            cb = getattr(w, "on_feed_auth_error", None)
            if callable(cb):
                handled = True
                try:
                    res = cb(code, msg)
                    if inspect.isawaitable(res):
                        self.spawn(_await(res), name="watcher-auth-error")
                except Exception:  # a broken watcher must not break the feed callback
                    log.exception("token watcher on_feed_auth_error raised")
        if not handled:
            with contextlib.suppress(Exception):
                self.conn.execute(
                    "INSERT INTO token_events (ts_ist, kind, detail) VALUES (?, ?, ?)",
                    (self._now().isoformat(), "feed_auth_error", f"{code} {msg}"),
                )
                self.conn.commit()
            with contextlib.suppress(RuntimeError):  # no running loop (sync test call)
                self.spawn(self.router.send_system(f"live feed stopped: {code} {msg}", "TOKEN"), name="auth-alert")
        self.hub.publish("token", {"event": "feed_auth_error", "code": code, "message": msg, **self.token_status()})

    async def refresh_token_health(self) -> TokenHealth | None:
        adapter = self.adapter()
        if adapter is None:
            return None
        try:
            self._token = await asyncio.to_thread(adapter.token_health)
        except Exception as exc:  # noqa: BLE001 -- token_health must never take the service down
            log.warning("token health check failed: %s", exc)
            return None
        if getattr(self._token, "prices", None) is True:  # 0.8.3 L1 (no watcher): a good price check clears it
            self.on_prices_ok()
        return self._token

    def token_status(self) -> dict[str, Any]:
        valid: bool | None = None
        validity: str | None = None
        if self.token_health_hook is not None:
            try:
                snap = dict(self.token_health_hook())
            except Exception:  # fall back to "unknown" rather than break /api/status
                log.exception("token_health_hook raised")
                snap = {}
            level = snap.get("level")
            # T-346 (D390 (C)): a CLIENT watcher's last-hour level warn1 (token_watch.classify_client) passes through as
            # it is; it used to fall to token_level below and come back re-rated on the owner ladder as warn2. The owner
            # edition is unchanged (its watcher never reports warn1; one that did would still be re-rated).
            if level in ("ok", "warn24", "warn2", "expired", "unknown") or (level == "warn1" and _edition.is_client()):
                return self._with_problem({
                    "valid": snap.get("valid"),
                    "validity_ist": snap.get("validity_ist"),
                    "hours_left": snap.get("hours_left"),
                    "level": level,
                }, snap.get("problem"))  # fmt: skip
            valid, validity = snap.get("valid"), snap.get("validity_ist")
            probed = snap.get("problem")
        elif self._token is not None:
            valid, validity = self._token.valid, self._token.validity_ist
            probed = getattr(self._token, "problem", None)
        else:
            probed = None
        hours_left: float | None = None
        if validity:
            try:
                hours_left = round((datetime.fromisoformat(validity) - self._now()).total_seconds() / 3600.0, 2)
            except (TypeError, ValueError):
                hours_left = None
        return self._with_problem({
            "valid": valid,
            "validity_ist": validity,
            "hours_left": hours_left,
            "level": token_level(valid, hours_left, self.cfg.alerts.token_warn_hours),
        }, probed)  # fmt: skip

    def _with_problem(self, block: dict[str, Any], probed: Any) -> dict[str, Any]:
        """0.8.3 T-229 (2)/(3), D230: `problem` = the last job's entitlement hint, else the token probe's; always
        present, None when clear."""
        block["problem"] = self.data_problem or (str(probed) if probed else None)
        return block

    def _clear_data_problem(self) -> None:
        """Prices flow (an ok / partial levels run, a good price check) or a new login: no standing refusal, and the
        next refusal is told again (L4's per-day dedupe restarts)."""
        self.data_problem = None
        self._refusals_sent.clear()

    def on_prices_ok(self) -> None:
        """0.8.3 skeptic L1: the token watcher's probe saw a price come back (TokenHealth.prices True)."""
        if self.data_problem is not None:
            log.info("the token check got a price: the standing price refusal is cleared")
        self._clear_data_problem()

    def _record_data_refused(self, exc: BrokerEntitlementError) -> None:
        """0.8.3 T-229 (2), D230: set `data_problem` to the broker's hint and write ONE token_events row (kind
        data_refused, detail = the broker's native code; the dashboard says "prices refused")."""
        self.data_problem = exc.hint
        try:
            self.conn.execute(
                "INSERT INTO token_events (ts_ist, kind, detail) VALUES (?, ?, ?)",
                (self._now().isoformat(timespec="seconds"), EVENT_DATA_REFUSED, str(exc.native)),
            )
            self.conn.commit()
        except Exception:  # an unwritable history must not hide the TOKEN line
            log.exception("token_events data_refused row not written")

    def _reload_env(self) -> Any:
        """Re-read .env through the injected or the real config loader (raises when .env does not load): the
        readiness `env` check (S2) and reload_credentials share it."""
        if self._reload_settings is None:
            from gammad.config import reload_settings

            self._reload_settings = reload_settings
        return self._reload_settings()

    def broker_name(self) -> str:
        """The broker this install runs (the registry entry's key, else the .env value, else Dhan: the owner's default)."""
        entry = self.broker_entry
        for attr in ("key", "name", "id"):
            v = getattr(entry, attr, None)
            if isinstance(v, str) and v:
                return v
        return self.cfg.env.broker or "dhan"

    def reload_credentials(self) -> None:
        """Re-read settings (.env) and hand the new token to the adapter and the live feed."""
        fresh = self._reload_env()
        self.cfg = replace(self.cfg, env=fresh.env)
        adapter = self.adapter()
        entry = self.broker_entry
        if entry is not None:  # the registry path: the whole creds mapping (feed_protocol.Feed)
            creds = self.broker_creds()
            if getattr(self, "session_manager", None) is not None:
                active = self.session_manager.active_account()
                if active is not None:
                    active.credentials = dict(creds)
            if adapter is not None and hasattr(adapter, "set_credentials"):
                adapter.set_credentials(creds)
            elif adapter is not None and hasattr(adapter, "set_token"):
                tok_key = entry.token_key if entry is not None else "TOKEN"
                adapter.set_token(creds.get(tok_key, ""))
            if self.feed is not None:
                self.feed.reload_credentials(creds)
        else:
            if adapter is not None and hasattr(adapter, "set_token"):
                adapter.set_token(fresh.env.dhan_access_token)
            if self.feed is not None:
                self.feed.reload_credentials(fresh.env.dhan_client_id, fresh.env.dhan_access_token)
        self._feed_disabled = None
        log.info("credentials reloaded")
        with contextlib.suppress(RuntimeError):
            self.spawn(self.refresh_token_health(), name="token-refresh")

    # ---------------------------------------------------------------- controls
    def set_paused(self, paused: bool) -> None:
        self.router.set_paused(bool(paused))
        self.hub.publish("status", self.status())

    def _live(self, security_id: int) -> dict[str, Any]:
        return {"ltp": self.last_ltp.get(security_id)}

    def preview(self, kind: str, ref: Mapping[str, Any], dest: str = "group") -> list[str]:
        """The exact Telegram message(s) a manual send of (kind, ref) to `dest` would post (0.8.3 C.5: the owner's
        personal text of an E2 renders; a group preview of a refused row raises the send path's own ValueError).
        T-637: an s089 signal ref (its served key) previews the 0.8.9 card its send would post."""
        key = s089_signal_key(kind, ref)
        if key is not None:
            return [s089_manual_card(self.s089, key, dest)]
        return self.router.render(kind, ref, live=self._live, today=self.today().isoformat(), dest=dest)

    async def send_manual(
        self, kind: str, ref: Mapping[str, Any], dest: str = "group", *, who: str = "local"
    ) -> dict[str, Any]:
        """D42/D43 manual send; WS event {type: manual_send}. T-637: an s089 signal ref sends by its served key."""
        if s089_signal_key(kind, ref) is not None:
            result = await s089_send_manual(self.router, self.s089, kind, ref, dest, who=who)
        else:
            result = await self.router.send_manual(kind, ref, dest, who=who, live=self._live,
                                                 today=self.today().isoformat())
        self.hub.publish(
            "manual_send",
            {"kind": kind, "ref": dict(ref), "dest": dest, "ok": result["ok"], "parts": result["parts"],
             "error": result["error"]},
        )  # fmt: skip
        return result

    def mute(self, security_id: int, muted: bool, reason: str | None = None) -> list[int]:
        """D42 per-contract mute; WS event {type: mute, data: {security_id, muted}}."""
        ids = self.router.mute(security_id, muted, reason)
        self.hub.publish("mute", {"security_id": int(security_id), "muted": bool(muted)})
        return ids

    def request_stop(self) -> None:
        self.stop_event.set()

    # ---------------------------------------------------------------- views
    def feed_connected(self) -> bool:
        """True while the broker feed's socket (or poll loop) is up and not parked on an auth error.

        T-222(b) / ALERTS.md D4: this read only a bool `connected` or Dhan's `conns[].ws`, so every
        non-Dhan feed reported False all session (the Zerodha client's PRICES STOPPED while ticks flowed).
        Order: an explicit bool `connected` wins; Dhan's `conns` keep their 0.8.2 answer unchanged; then
        the FeedBase feeds (gammad/watch/feed_protocol.py): parked (`auth_blocked`) = False; a WebSocket
        feed (Kite / Upstox / Angel / Kotak / Fyers: `_ws`, set by `_run` only while a socket is open) =
        socket open, and Kotak also needs its login frame acknowledged (`_authed`); a poll feed (Groww /
        Nuvama PollFeed: `caps.kind` "poll") = its poll task is running. The dashboard's staleness check
        on `last_tick_ms` stays the backstop for an open socket that delivers nothing.
        """
        feed = self.feed
        if feed is None:
            return False
        flag = getattr(feed, "connected", None)
        if isinstance(flag, bool):
            return flag
        conns = getattr(feed, "conns", None)
        if conns is not None:  # Dhan (gammad/watch/feed.py DhanFeed): unchanged
            return any(getattr(c, "ws", None) is not None for c in conns)
        if getattr(feed, "auth_blocked", False) is True:
            return False
        if hasattr(feed, "_ws"):
            return getattr(feed, "_ws", None) is not None and getattr(feed, "_authed", True) is not False
        if "poll" in str(getattr(getattr(feed, "caps", None), "kind", "")):
            task = getattr(feed, "_task", None)
            return task is not None and not task.done()
        return False

    def jobs_view(self) -> list[dict[str, Any]]:
        now = self._now()
        jobs = {j.id: j for j in (self.scheduler.get_jobs() if self.scheduler is not None else [])}
        out: list[dict[str, Any]] = []
        for key, st in self.job_state.items():
            job = jobs.get(key)
            nxt = None
            if job is not None:  # T-277: the refresh row's next run = the next mark (22 fires a day outrun _next_run)
                nxt = self._next_refresh(now) if key == REFRESH_JOB else self._next_run(job, now)
            out.append(
                {
                    "name": st.name,
                    "next_run_ist": nxt.isoformat() if nxt else None,
                    "last_run_ist": st.last_run_ist,
                    "last_status": st.last_status,
                }
            )
        return out

    def runs_today(self) -> list[dict[str, Any]]:
        """D127: today's runs rows for the dashboard's "built HH:MM:SS · N contracts · next change" line and the
        stale badge (T-151); the same rows the WS `levels` / `watchlist` events announce. Read-only, never raises.
        perf B3: status() is pushed every 2 s per WS client, so the rows are cached and re-read only when a job starts or
        finishes (its job_state slot moves), the day changes, or RUNS_TODAY_TTL_S passed (a run another process wrote)."""
        today = self.today().isoformat()
        mark = tuple((k, s.last_run_ist, s.last_status, s.last_run_id) for k, s in self.job_state.items())
        now_ms = self._wall_ms()
        hit = self._runs_cache
        if hit is not None and hit[0] == mark and hit[2] == today and 0 <= now_ms - hit[1] < RUNS_TODAY_TTL_S * 1000:
            return [dict(r) for r in hit[3]]  # copies: a caller may edit its body, never the cache
        rows = self._read_runs_today(today)
        self._runs_cache = (mark, now_ms, today, rows)
        return [dict(r) for r in rows]

    def _read_runs_today(self, today: str) -> list[dict[str, Any]]:
        cols = ("run_id", "kind", "status", "started_ist", "finished_ist", "note")
        # status() is polled every 2 s from the API worker thread and self.conn belongs to the Runtime thread:
        # sqlite3 refuses cross-thread use, so this read opens its own short-lived connection (the pattern of
        # server.py _read). 0.6.0-0.7.0 logged one ERROR per poll and always answered [] (LESSONS 2026-09-29).
        try:
            conn = db.connect(self.db_path)
            try:
                rows = conn.execute(
                    f"SELECT {', '.join(cols)} FROM runs WHERE started_ist LIKE ? ORDER BY started_ist",
                    (f"{today}%",),
                ).fetchall()
            finally:
                conn.close()
        except Exception:  # a missing table on an old store is a status, not a crash
            log.exception("runs read failed for /api/status.runs_today")
            return []
        return [dict(zip(cols, tuple(r), strict=True)) for r in rows]

    def _roll_stale_guarded(self) -> None:
        today = self._now().date().isoformat()
        if self._stale_guarded_day != today:
            self._stale_guarded_day, self._stale_guarded = today, 0

    def _count_stale_guarded(self) -> None:
        self._roll_stale_guarded()  # F58-1: roll BEFORE counting the first pre-open packet
        self._stale_guarded += 1

    def _stale_health(self) -> dict[str, int]:
        self._recorder_health()  # roll stopped-recorder totals first
        self._roll_stale_guarded()
        st = getattr(self.recorder, "stats", None)
        return {
            "recorder_skipped": self.recorder_errors_today["stale_skipped"]
            + int(getattr(st, "stale_skipped", 0) or 0),
            "guarded": self._stale_guarded,
        }

    def _recorder_health(self) -> dict[str, Any]:
        """T-284 (3): today's failed parquet FILES and sqlite BATCHES (the live recorder + any recorder stopped
        today). Zeros with no recorder or a recorder without stats (test fakes)."""
        today = self._now().date().isoformat()
        acc = self.recorder_errors_today
        if acc["day"] != today:  # a new day starts the count afresh
            acc.update(day=today, parquet_failed=0, db_failed=0, last_failed_ms=None, stale_skipped=0)
        st = getattr(self.recorder, "stats", None)
        stamps = [v for v in (acc["last_failed_ms"], getattr(st, "last_failed_ms", None)) if v is not None]
        last = max(stamps) if stamps else None
        return {
            "parquet_failed": acc["parquet_failed"] + int(getattr(st, "parquet_failed", 0) or 0),
            "db_failed": acc["db_failed"] + int(getattr(st, "db_failed", 0) or 0),
            "last_failed_ist": datetime.fromtimestamp(last / 1000, IST).isoformat() if last is not None else None,
        }

    def status(self) -> dict[str, Any]:
        lat = list(self.latencies)
        out = {
            "version": self.version,
            "spec_version": self.spec_version,
            "now_ist": self._now().isoformat(),
            "market_open": self.market_open(),
            "feed": {
                "connected": self.feed_connected(),
                "stale": self._stale_health(),
                "n_subscribed": len(self.subscribed),
                "last_tick_ms": self.last_tick_ms,
                # 0.8.3: True = no feed runs (never started, or start_feed refused it: _feed_disabled)
                "disabled": self.feed is None or self._feed_disabled is not None,
                # T-284 (1): start of the silence a "live prices stopped" line went out for; None = no open episode
                "silent_since_ms": self.feed_alarm.silent_since_ms,
                # 0.8.5 A1 (D1 / D2): n_on_feed (the feed's own count), cap, refused {n, by_reason, names},
                # start_error (the last failed start's reason; None once a start succeeds). n_subscribed stays the app's
                **self._feed_status(),
            },
            "token": self.token_status(),
            "alerts_paused": self.alerts_paused,
            "jobs": self.jobs_view(),
            "runs_today": self.runs_today(),
            "readiness": self.readiness.status_block(),  # W16 S2: None before the first 09:00 check
            "recorder": self._recorder_health(),  # T-284 (3): data health, both editions (not in CLIENT_STATUS_DROP)
            # T-211 (D189(2)): the last delivery-check verdict, owner only (CLIENT_STATUS_DROP drops it too)
            **({"delivery": self.delivery_last} if _edition.is_owner() else {}),

            "latency": {
                "p50_ms": percentile(lat, 50),
                "p95_ms": percentile(lat, 95),
                "last_ms": self.last_latency_ms,
                "n": len(lat),  # perf B1: below ~20 samples the nearest-rank p95 is simply the slowest one
            },
        }
        if self.owner_ext is not None:  # 0.8.2 (C.7(6)): owner edition only (a client Runtime never has one)
            try:
                out["owner"] = self.owner_ext.status()
            except Exception:
                log.exception("owner status failed")
                out["owner"] = None
        return out

    def live_row(self, security_id: int) -> dict[str, Any]:
        b = self.builders.get(security_id)
        eng = self.pool.get(security_id)
        oi_now = b.oi_now if b is not None else None
        peak = self._bar_close_peak(security_id) if b is not None else None  # T-585: the engine's own peak
        drop = round((peak - oi_now) / peak * 100.0, 3) if oi_now is not None and peak else None
        # edition projection at the source (idempotent; owner unchanged): client IDLE/ARMED -> WATCHING,
        # ARMED or DONE -> ENTERED only with a visible entry of the CURRENT cycle (D98(5), FIX2 F2; I8: the client entry
        # is the E2 bar close, so its engine is still ARMED when the entry shows), so no consumer of live_row
        # (watchlists, trades) can leak an ARMED state or a hidden gated entry
        state = None
        if eng is not None:
            entered = self._entered_this_cycle(security_id, eng)
            raw = str(State.DONE) if entered and eng.state is State.ARMED and _edition.is_client() else str(eng.state)
            state = project_state(raw, entered)
        return {
            "ltp": self.last_ltp.get(security_id),
            "oi_now": oi_now,
            "oi_drop_pct": drop,
            "engine_state": state,
        }

    def broker_budget_status(self) -> str | None:
        """D98(8): /api/status.broker.budget_status -- the Nuvama adapter's daily API budget line (None = OK). Reads
        an adapter already built; never builds one (the status poll runs every 2 s, on a worker thread)."""
        value = getattr(self._adapter, "budget_status", None) if self._adapter is not None else None
        return value if isinstance(value, str) and value else None

    def contract_view(self, security_id: int, tf: int) -> dict[str, Any] | None:
        """Live bars (closed + forming) and the engine's last A/B/C. None when the contract is not live."""
        b = self.builders.get(security_id)
        eng = self.pool.get(security_id)
        if b is None and eng is None:
            return None
        bars: list[dict[str, Any]] = []
        if b is not None and tf in b.tfs:
            forming = b.forming_bar(tf)
            for x in [*b.series(tf), *([forming] if forming is not None else [])]:
                bars.append(
                    {"ts_ms": x.start_ms, "open": x.open, "high": x.high, "low": x.low, "close": x.close,
                     "volume": x.volume, "oi": x.oi}
                )
        engine: dict[str, Any] = dict.fromkeys(
            ("state", "a", "b", "c", "oi_drop_pct", "vol_ratio", "consol_lo", "consol_hi", "consol_mid")
        )
        if eng is not None:
            ev = eng.last_eval
            engine.update(
                state=str(eng.state),
                a=ev.a if ev else None,
                b=ev.b if ev else None,
                c=ev.c if ev else None,
                oi_drop_pct=round(ev.oi_drop * 100.0, 3) if ev and ev.oi_drop is not None else None,
                vol_ratio=ev.vol_ratio if ev else None,
                consol_lo=eng.consol_lo if eng.consol_lo is not None else (ev.consol_lo if ev else None),
                consol_hi=eng.consol_hi if eng.consol_hi is not None else (ev.consol_hi if ev else None),
                consol_mid=eng.consol_mid,
            )
        return {"bars": bars, "engine": engine}

    # ---------------------------------------------------------------- supervision
    async def supervise_once(self) -> None:
        """One pass of the 2 s loop: STOP file, day roll-over, market-hours guard for the feed."""
        if self._poll_stop_file():
            return
        await self._supervise_day()

    def _poll_stop_file(self) -> bool:
        """The data/STOP file (scripts/stop.cmd, the tray): True = found, the stop event is set."""
        if not self.stop_file.exists():
            return False
        with contextlib.suppress(OSError):
            self.stop_file.unlink()
        log.info("stop file %s found; stopping", self.stop_file)
        self.stop_event.set()
        return True

    async def _supervise_day(self) -> None:
        """Day roll-over (ensure_day: load_day, subscribe, warm-up + today's resume) and the market-hours feed guard,
        in that order: the feed starts only after the day's warm-up, so no live bar is appended before a resumed one."""
        now = self._now()
        d = now.date()
        if self.is_trading_day(d):
            async with self._day_lock:  # T-107: a job's load in progress finishes first (the feed starts after it)
                await self.ensure_day(d)
            self._hold_pass(now)  # T-310: track / release the held V1 signals (a restart, the last retry's time)
        if self.market_open(now):
            if self.feed is None and self.trade_date == d:
                await self.start_feed()
        elif self.feed is not None:
            await self.stop_feed()

    async def _day_pass(self) -> None:
        try:
            await self._supervise_day()
        except Exception:  # the supervisor must survive any single failure
            log.exception("supervise pass failed")

    def _kick_day_pass(self) -> None:
        """T-060 (D70b): the day pass runs as ONE tracked task off the 2 s loop, so a warm-up (116 intraday calls,
        ~70 s) never holds the STOP poll; a pass still running is not started again. shutdown() cancels it."""
        task = self._day_task
        if task is not None and not task.done():
            return
        self._day_task = self.spawn(self._day_pass(), name="supervise-day", daemon=True)

    def _feed_alarm_pass(self) -> None:
        """T-284 (1): ONE personal SYSTEM line when live prices are silent for more than FEED_SILENT_S in market hours,
        ONE when they return. A parked (auth-blocked) feed is the TOKEN line's job; a disabled / absent feed and a
        closed market disarm silently. send_system = the personal chat (a client's own chat), never the group."""
        now = self._now()
        line = self.feed_alarm.observe(
            int(now.timestamp() * 1000),
            self.market_open(now),
            self.feed is not None and self._feed_disabled is None,
            getattr(self.feed, "auth_blocked", False) is True,
            self.last_tick_ms,
        )
        if line is None:
            return
        log.warning("feed alarm: %s", line)
        self.spawn(self.router.send_system(line, "SYSTEM"), name="feed-alarm")

    async def _supervise_loop(self) -> None:
        while not self.stop_event.is_set():
            try:
                if not self._poll_stop_file():
                    self._kick_day_pass()
                    self._feed_alarm_pass()  # T-284 (1)
            except Exception:  # the supervisor loop must survive any single failure
                log.exception("supervise pass failed")
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self.stop_event.wait(), timeout=SUPERVISE_EVERY_S)

    async def _timer_loop(self) -> None:
        while not self.stop_event.is_set():
            if self.builders or self.und_builders:
                try:
                    self.on_timer()
                except Exception:  # keep closing bars even if one close raised
                    log.exception("bar timer failed")
            try:
                delay = self.timer_delay_s()
            except Exception:  # never let the schedule computation stop the timer
                log.exception("bar timer delay failed")
                delay = TIMER_EVERY_S
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self.stop_event.wait(), timeout=delay)

    async def run(self, extra: Iterable[Awaitable[Any]] = ()) -> None:
        """Start everything, wait for the stop event, shut down gracefully."""
        log.info("%s", startup_line(self.version, self.spec_version, client=_edition.is_client()))
        try:  # Z1(3): deliver what load() wrote + re-send, once, the previous process's stale pending events
            await self.tracker.resume()
        except Exception:  # the tracker must never stop the service from starting
            log.exception("trade tracker resume failed")
        self.schedule_jobs()
        self.scheduler.start()
        # T-181 F1 (R1): a start after 09:20 with a token already known valid catches the 09:20 build up now (the
        # watcher's first valid poll and the no-watcher token check below reach the same on_token_valid)
        self.on_token_valid()
        # W16 S2 (T-134(2), B4): first start ever = one group card; later starts = one personal line (throttled);
        # never blocks the start, never raises (announce_start swallows)
        self.spawn(
            announce_start(self.router, self.data_dir, version=self.version, edition=_edition.edition(),
                           broker=self.broker_name(), now=self._now()),
            name="start-card", daemon=True,
        )  # fmt: skip
        if self.token_health_hook is None:  # a registered watcher polls on its own (then on_token_valid, T-181 F1)
            self.spawn(self._start_token_check(), name="token-check")
        loops = [
            self.spawn(self._supervise_loop(), name="supervise", daemon=True),
            self.spawn(self._timer_loop(), name="bar-timer", daemon=True),
        ]
        loops += [self.spawn(_await(x), name="extra", daemon=True) for x in extra]
        try:
            await self.stop_event.wait()
        finally:
            await self.shutdown(loops)

    async def shutdown(self, loops: Iterable[asyncio.Task[Any]] = ()) -> None:
        log.info("shutting down")
        self.stop_event.set()
        if self.scheduler is not None:
            with contextlib.suppress(Exception):
                self.scheduler.shutdown(wait=False)
        day, self._day_task = self._day_task, None
        if day is not None and not day.done():  # T-060: a warm-up still running is cancelled, never awaited
            day.cancel()
            await asyncio.gather(day, return_exceptions=True)
        await self.stop_feed()
        if self.owner_ext is not None:  # 0.8.2: the owner sink's CLOSED bars (a forming bar is lost, as V1's)
            try:
                await self.owner_ext.shutdown()
            except Exception:
                log.exception("owner extension shutdown failed")
        await self._stop_recorder()
        # T-397: the writers are stopped: fold the WAL now, so a stop the tray cuts short (10 s, then terminate)
        # leaves at most what the last tasks below write
        store_lifecycle.checkpoint(self.conn, label="main store (writers stopped)")
        for t in loops:
            if not t.done():
                t.cancel()
        await asyncio.gather(*loops, return_exceptions=True)
        pending = [t for t in self._tasks if not t.done()]
        if pending:
            _done, still = await asyncio.wait(pending, timeout=5.0)
            for t in still:
                t.cancel()
        aclose = getattr(self.telegram, "aclose", None)
        if callable(aclose):
            with contextlib.suppress(Exception):
                await aclose()
        self._close_store()
        log.info("stopped")

    def _close_store(self) -> None:
        """T-397 (D443 (4)): the main connection (router, tracker, grader, token watcher share it; the recorder closed
        its own in _stop_recorder) checkpointed with TRUNCATE and closed, so a clean quit leaves no -wal even when a
        job thread still holds the store (0.8.7 left 6,258,312 B after tray -> Quit and install_live switch refused).
        Any tracked connection still open is named in the log. Idempotent; never raises."""
        try:
            store_lifecycle.report_open(self.db_path, label="service")
            store_lifecycle.close_store(self.conn, label="main store")
            left = store_lifecycle.wal_bytes(self.db_path)
            if left:
                log.warning("store: %s-wal still %d B after the stop (a reader on another thread holds it); "
                            "tools/install_live.py checkpoint folds it", self.db_path, left)  # fmt: skip
        except Exception:  # the stop must finish whatever the store does
            log.exception("store close at shutdown failed")


async def _await(x: Awaitable[Any]) -> Any:
    return await x


# ------------------------------------------------------------------------------------------ entry point
def _make_telegram(cfg: Settings) -> Any:
    if not cfg.env.telegram_bot_token:
        log.warning("TELEGRAM_BOT_TOKEN not set: alerts go to the dashboard only")
        return None
    from gammad.alerts.telegram import TelegramClient

    return TelegramClient(
        cfg.env.telegram_bot_token, group_id=cfg.env.telegram_group_id, topic_id=cfg.env.telegram_topic_id
    )


def _make_token_watcher(runtime: Runtime, cfg: Settings) -> Any:
    """W5 TokenWatcher, attached to the runtime (sets token_updater / token_login / token_view, registers for
    feed auth errors). X2: broker-aware -- `entry` = the runtime's registry entry, `creds` = its keys from the
    D54-resolved settings; the watcher then hands ONE creds mapping to runtime.apply_credentials."""
    try:
        from gammad.auth.token_watch import TokenWatcher
    except ImportError:
        log.warning("gammad.auth.token_watch unavailable: token watch off, runtime checks the token once")
        return None
    entry = runtime.broker_entry
    creds = runtime.broker_creds()
    if entry is None:  # client without BROKER / a broken entry: nothing to probe; POST /api/token answers 501
        log.warning("token watch off: %s", runtime.broker_problem or "no broker configured")
        return None
    # W14 H1 (T-113): attached whenever the ENTRY exists. With no token yet there is no adapter: the probe loop
    # idles ("no <TOKEN_KEY> configured") and the Token page's login / paste still work (they need entry + creds);
    # apply_credentials then builds the runtime's adapter, which the watcher re-reads through adapter_source.
    adapter = runtime.adapter()
    if adapter is None:
        log.warning("token watch: no %s adapter yet (%s); log in on the Token page", entry.label,
                    runtime._feed_problem() or "credentials")  # fmt: skip
    watcher = TokenWatcher(
        adapter, runtime.conn, runtime.router, cfg, on_credentials_changed=runtime.apply_credentials,
        entry=entry, creds=creds, adapter_source=runtime.adapter,
    )  # fmt: skip
    watcher.attach(runtime)
    return watcher


async def serve_async(*, tray: bool = True, cfg: Settings | None = None) -> int:
    from gammad.api import lan
    from gammad.api import server as api_server
    from gammad.broker.config_loader import load_account_session_manager, load_history_provider_config
    from gammad.config import settings

    cfg = cfg or settings()
    session_manager = load_account_session_manager(cfg)
    history_cfg = load_history_provider_config(cfg)
    watermark_ledger_path = cfg.paths.data_dir / "watermark.db"
    runtime = Runtime(
        cfg,
        telegram=_make_telegram(cfg),
        v1r=load_v1r(),
        scanners_089=True,
        session_manager=session_manager,
        watermark_ledger_path=watermark_ledger_path,
        history_config=history_cfg,
    )
    app = api_server.create_app(runtime)
    # W16 S2 (T-129, B1): loopback, plus this PC's RFC1918 address ONLY when GAMMA_LAN is on AND a password is set
    # (bind_hosts logs the single ERROR for GAMMA_LAN without a password; it never returns 0.0.0.0)
    hosts = lan.bind_hosts(cfg.env)
    urls = ", ".join(f"http://{h}:{cfg.env.port}" for h in hosts)
    server = api_server.make_server(app, cfg.env.port, hosts=hosts)
    exit_code = 0

    async def _api() -> None:
        nonlocal exit_code
        try:
            await server.serve()
        except SystemExit:  # uvicorn exits when the port is taken: a second gammad is the usual cause
            exit_code = 4
            log.exception("API could not bind %s (another gammad running?); stopping", urls)
        finally:
            if not runtime.stop_event.is_set():
                runtime.request_stop()

    async def _stop_api() -> None:
        await runtime.stop_event.wait()
        server.should_exit = True

    extra: list[Awaitable[Any]] = [_api(), _stop_api()]
    watcher = _make_token_watcher(runtime, cfg)
    if watcher is not None:
        extra.append(watcher.run())

    tray_handle = None
    if tray:
        try:
            from gammad.tray.tray import start_tray

            tray_handle = start_tray(runtime, asyncio.get_running_loop(), cfg)
        except Exception:  # no tray is not fatal: the dashboard still works
            log.exception("tray failed to start; continuing without it")
    log.info("dashboard on %s", urls)
    try:
        await runtime.run(extra=extra)
    finally:
        if watcher is not None:
            with contextlib.suppress(Exception):
                watcher.stop()
        if tray_handle is not None:
            with contextlib.suppress(Exception):
                tray_handle.stop()
    return exit_code


def serve(*, tray: bool = True) -> int:
    try:
        return asyncio.run(serve_async(tray=tray))
    except KeyboardInterrupt:
        return 0
