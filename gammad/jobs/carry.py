"""S1.5.0 carry-forward (D157(2), D159(4), D161(6); docs/research/2026-09-29-carry-forward/DESIGN.md sections 1-4 as
corrected by D161(6); docs/research/2026-09-30-workflow-0.8.1/CONTRACTS.md section 6). Runs in the OWNER edition on
carry_forward.enabled, and in BOTH editions while still_watching.enabled (0.8.9 T-419, D480; build_watchlists).

A contract ENTERS the book only through a list build; carry decides whether it STAYS. At a 09:20-kind build (after
the S1.3.1 retire, before the build's dedupe keys) every contract of the SOURCE day -- the latest trade_date before
today holding watchlist rows -- that is LIVE there, or was dropped there by the cap (`capacity`, re-eligible the next
day), is re-created under today's trade_date as a `tag = 'CARRY'` row, keeping its list_name / expiry_date / zone
edge (the LAST listing's), unless a build already listed it today (any non-CARRY row of that security id today).

Inputs, read at 09:20 of day d: the PREVIOUS session's last closed 5-min bar (oi_prev, prem_prev) and the OI peak since
the first listing (`PrevSession`, from data/parquet/<day>/bars-5m.parquet by default), today's levels row (spot =
ref_price, atr_pct = daily ATR %), and the row's listing base (oi_at_listing, prem_at_listing; a pre-v9 row: its oi /
chain_ltp). dist_atr = the strike's OTM distance in percent of spot / atr_pct (ITM negative).

  retire DEAD      prem_prev < alive_min_ticks x tick           (tick: the row's, else the NSE default for the spot)
  retire SPENT     oi_prev < spent_ratio x peak
  retire AWAY      dist_atr > away_atr
  retire DEEP_ITM  dist_atr < -deep_itm_atr
  retire EXPIRY    expiry_date < d
  retire FLOOR     oi_prev under the LISTING floor (T-282 (d2), S1.8.0): OI >= proximity.min_oi_lots x the row's
                   lot_size, the list build's own test (chain_select.passes_floor, OI part only: carry reads no traded
                   value); an unknown lot fails open and min_oi_lots 0 is off, exactly as at the build
  BUILD-UP        oi_prev >= build_ratio x oi_at_listing ("strong" at strong_ratio, reported in the why text)
  PRIMED           BUILD-UP and prem_prev <= prem_at_listing and |dist_atr| <= near_atr
  WATCH            otherwise; also a contract with NO previous-session bar (unknown is not gone)
  BROKEN (flag)    spot through the last listing's zone edge (PE spot < edge; CE spot > edge)

A threshold met exactly keeps the contract (a 1e-9 float slack). The retire tests run in the order above (the first
that holds names the reason); a contract with no previous-session bar is never retired by SPENT / DEAD / FLOOR (unknown
is not gone). No max_sessions (D159(4)). The cap `max_contracts` (120 since S1.8.0, T-282 (a); was 60) ranks the alive: PRIMED > BUILD-UP > WATCH; inside a state BROKEN first, then oi_prev / oi_at_listing descending,
then |dist_atr| ascending (unknowns last), then security id. The rest retire as `capacity`. A contract with an OPEN
trade (trades.state in CARRY_HOLD_STATES: V1, Still Watching and V1R live states; T-419 / CX-121 R-121h)
is never retired for any reason but EXPIRY and takes a cap slot FIRST (D157(2):
"cap 60 carried a day, never an open trade"), so the day carries at most max_contracts unless more trades than that
are open. The 13:15 drop order of the design is NOT built (D161(6)). T-267 M11: a contract held ONLY by an open V2
Scanner trade (trades.state in ZONE_STATES, Z_OPEN) is never carried and never written as a carry row at all (the
tracker's open_instruments keeps its option and underlying subscribed); a V1 open trade on it keeps the V1 rule.

Retired carry rows are written (retired_run_id = the run, carry_reason = the code) and never deleted; only
`capacity` rows are read back. A re-run of the same day's 09:20 recomputes the same rows (an upsert over CARRY rows
only; a listed row is never touched). `carry_k` = sessions since the last listing (the source row's + 1).

0.8.9 T-419 "Still Watching" (D471 Q2 / Q10, D476 (1), D480; spec 4.8; `still_watching:` -> SwCfg below): carry runs
in BOTH editions while still_watching.enabled (build_watchlists gates the call). With `sw` given, carry_forward adds:
  retire EXPIRY_WEEK  DTE <= sw.dte_max (4; D557 (3), was 7) trading days (calendar.trading_days_between(d, expiry),
                      the engine_pool / card DTE): carried until expiry week; next-month
                      contracts arrive as new listings, never a swap. Applies with an open trade too (the trade runs to
                      its own exit; only the row goes).
  entered options     stay carried (D617 (1), D633 (4)): the ENTERED retire is dropped.
  open V1N positions  keep their CARRY rows through DEAD / SPENT / AWAY / DEEP_ITM / FLOOR / CAPACITY
                      (D657 (2), D658 (1)); both OWN and GROUP books protect the row, outside the cap.
                      EXPIRY / EXPIRY_WEEK still apply; the exit ladder owns the trade itself.
  oi_peak_prior       the OI peak of the sessions BEFORE today (PrevSession.oi_peak: the max 5-min bar OI from the first
                      listing through the previous session = cp_engine.py:11-13's PeakBook prior), written when the
                      watchlist table has the column (schema 11); the engine's condition A reads max(it, today's peak).
  client source rows  only the client's own lists (edition.v1_lists_sql, D429).
The cap (max_contracts, 120) excludes open V1N positions; `sw` None skips the expiry-week rule and prior-peak write."""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import date, datetime
from pathlib import Path
from typing import Any, Final

import pyarrow as pa
import pyarrow.parquet as pq

from gammad.broker.base import ChainRow
from gammad.config import IST, CarryCfg, ProximityCfg, now_ist
from gammad.core.calendar import default_calendar
from gammad.core.chain_select import default_tick, passes_floor
from gammad.log import get_logger
from gammad.trade.rules import LIVE_STATES, SW_STATES, ZONE_STATES

log = get_logger(__name__)

# = gammad.trade.rules.V1R_STATES (CX-120 W2; names frozen there; Claude asserts equality at merge)
V1R_LIVE_STATES: Final = frozenset({"V1R_OPEN", "V1R_TRAILING", "V1R_HALF"})
CARRY_HOLD_STATES: Final = LIVE_STATES | SW_STATES | V1R_LIVE_STATES

CARRY_TAG = "CARRY"
PRIMED, BUILD_UP, WATCH = "PRIMED", "BUILD-UP", "WATCH"
STATE_RANK: dict[str, int] = {PRIMED: 0, BUILD_UP: 1, WATCH: 2}
BROKEN = "broken"
DEAD, SPENT, AWAY, DEEP_ITM, EXPIRY, CAPACITY = "dead", "spent", "away", "deep_itm", "expiry", "capacity"
FLOOR = "floor"  # T-282 (d2): under the listing OI floor at the 09:20 carry
EXPIRY_WEEK = "expiry_week"  # T-419 (D471 Q2): DTE <= still_watching.dte_max
ENTERED = "entered"  # T-419: the option already entered once (one entry per option)


@dataclass(frozen=True)
class SwCfg:
    """T-419 `still_watching:` (spec 4.8), read DEFENSIVELY by `still_watching_cfg` (Settings.still_watching is parsed by
    gammad/config.py, another builder's file; absent -> these defaults, D471 Q10 "switch on in 0.8.9")."""

    enabled: bool = True
    dte_max: int = 4  # D557 (3): the D554 roll (was 7)
    base_atr_max: float = 2.0
    entry_cutoff_hm: str = "15:20"
    ban_enabled: bool = True
    ban_fetch_hm: str = "08:45"
    owner_any3_shadow: bool = True


def _get(obj: Any, key: str, default: Any) -> Any:
    if obj is None:
        return default
    v = obj.get(key, default) if isinstance(obj, Mapping) else getattr(obj, key, default)
    return default if v is None else v


def still_watching_cfg(settings: Any) -> SwCfg:
    """SwCfg from Settings (or a mapping holding `still_watching`; None -> defaults); every missing key = its default."""
    if isinstance(settings, SwCfg):
        return settings
    block = _get(settings, "still_watching", None)
    ban = _get(block, "ban_list", None)
    d = SwCfg()
    return SwCfg(
        enabled=bool(_get(block, "enabled", d.enabled)),
        dte_max=int(_get(block, "dte_max", d.dte_max)),
        base_atr_max=float(_get(block, "base_atr_max", d.base_atr_max)),
        entry_cutoff_hm=str(_get(block, "entry_cutoff_hm", d.entry_cutoff_hm)),
        ban_enabled=bool(_get(ban, "enabled", d.ban_enabled)),
        ban_fetch_hm=str(_get(ban, "fetch_hm", d.ban_fetch_hm)),
        owner_any3_shadow=bool(_get(block, "owner_any3_shadow", d.owner_any3_shadow)),
    )
RELISTED = "relisted"  # not a carry retirement: build_watchlists.insert_watchlist_rows re-listed the contract
BARS_FILE = "bars-5m.parquet"
_EPS = 1e-9

# every column a carry row writes (the watchlist PK + the build's columns + the v9 carry columns + the retire mark)
CARRY_COLUMNS: tuple[str, ...] = (
    "trade_date", "list_name", "expiry_date", "security_id", "symbol", "underlying_security_id", "strike",
    "option_type", "tag", "oi", "candidates_n", "ref_price", "zone_edge", "run_id", "spec_version", "lot_size",
    "tick_size", "chain_ltp", "chain_ltp_at", "atr_pct", "origin_run_id", "first_listed", "last_listed", "carry_k",
    "carry_state", "carry_flags", "oi_at_listing", "prem_at_listing", "carry_reason", "retired_run_id",
    "oi_peak_prior",  # T-419 schema 11: written only when the table has it (_upsert reads PRAGMA table_info)
)  # fmt: skip
_KEY: tuple[str, ...] = ("trade_date", "list_name", "expiry_date", "security_id")


@dataclass(frozen=True)
class PrevSession:
    """A contract's previous-session view: the last closed bar's OI and close (premium, rupees), the max bar OI over
    the sessions read (the first listing through the previous session), the day of that last bar and its close time."""

    oi: float | None
    close: float | None
    oi_peak: float | None = None
    day: date | None = None
    close_at: str | None = None  # ISO IST of the last bar's END (the carried row's chain_ltp_at)


@dataclass
class CarrySummary:
    carried: int = 0  # live CARRY rows written
    primed: int = 0
    build_up: int = 0
    watch: int = 0
    retired: dict[str, int] = field(default_factory=dict)  # reason -> n (only reasons that happened)
    broken: int = 0  # carried rows flagged BROKEN
    no_data: int = 0  # carried rows with no previous-session bar (WATCH)
    source_date: str | None = None


PrevSource = Mapping[int, PrevSession] | Callable[[Mapping[int, date]], Mapping[int, PrevSession]]


# -- the previous session from the recorder's closed 5-min bars ---------------------------------------------------


def _day_dirs(root: Path, since: date, before: date) -> list[date]:
    """The recorded day folders `since <= day < before`, never TODAY's or later (D142: the live recorder rewrites
    today's files; a reader holding one fails its os.replace on Windows)."""
    if not root.is_dir():
        return []
    today = now_ist().date()
    out: list[date] = []
    for p in root.iterdir():
        try:
            d = date.fromisoformat(p.name)
        except ValueError:
            continue
        if since <= d < before and d < today and (p / BARS_FILE).is_file():
            out.append(d)
    return sorted(out)


def _read_bars(path: Path, sids: set[int], tf_min: int) -> dict[int, dict[int, tuple[float | None, float | None]]]:
    """sid -> {bar start_ms: (close, oi)} of `path` (a re-written bar: the last row wins, as warmup reads it)."""
    try:
        # read_bytes: the handle is held for one read, never across the parse (gammad/api/recorded_bars.py)
        table = pq.read_table(pa.BufferReader(path.read_bytes()), columns=["security_id", "start_ms", "tf_min",
                                                                          "close", "oi"])  # fmt: skip
    except (OSError, ValueError, pa.ArrowException) as exc:
        log.warning("carry: %s unreadable (%s); that session is skipped", path, type(exc).__name__)
        return {}
    out: dict[int, dict[int, tuple[float | None, float | None]]] = {}
    cols = [table.column(c).to_pylist() for c in ("security_id", "start_ms", "tf_min", "close", "oi")]
    for sid, start, tf, close, oi in zip(*cols, strict=True):
        if sid is None or start is None or int(tf or 0) != tf_min or int(sid) not in sids:
            continue
        out.setdefault(int(sid), {})[int(start)] = (
            None if close is None else float(close),
            None if oi is None else float(oi),
        )
    return out


def parquet_prev_session(root: Path, *, before: date, tf_min: int = 5) -> Callable[[Mapping[int, date]], dict[int, PrevSession]]:
    """A `prev_session` source over the recorder's `<root>/<day>/bars-5m.parquet`: for each asked security id (->
    its first listing date) the last closed bar of the LATEST recorded day before `before`, and the max bar OI over
    the days from its first listing on. A contract with no bar in those days is absent (carry reads it as unknown)."""

    def load(wanted: Mapping[int, date]) -> dict[int, PrevSession]:
        if not wanted:
            return {}
        days = _day_dirs(root, min(wanted.values()), before)
        sids = {int(s) for s in wanted}
        last: dict[int, tuple[date, int, float | None, float | None]] = {}
        peak: dict[int, float] = {}
        for d in days:
            for sid, bars in _read_bars(root / d.isoformat() / BARS_FILE, sids, tf_min).items():
                if d < wanted[sid]:
                    continue  # before this contract's first listing: never its peak
                for close, oi in bars.values():
                    if oi is not None and oi > 0:
                        peak[sid] = max(peak.get(sid, 0.0), oi)
                start = max(bars)
                close, oi = bars[start]
                last[sid] = (d, start, close, oi)  # days ascend: the latest day wins
        out: dict[int, PrevSession] = {}
        for sid, (d, start, close, oi) in last.items():
            end = datetime.fromtimestamp((start + tf_min * 60_000) / 1000, IST).isoformat()
            out[sid] = PrevSession(oi=oi, close=close, oi_peak=peak.get(sid), day=d, close_at=end)
        return out

    return load


# -- one contract's evaluation (pure) ------------------------------------------------------------------------------


@dataclass(frozen=True)
class _Eval:
    row: dict[str, Any]
    state: str | None  # None = retired
    retired: str | None
    broken: bool
    ratio: float | None
    dist_atr: float | None
    open_trade: bool
    no_data: bool


def _num(v: Any) -> float | None:
    return None if v is None else float(v)


def listing_floor(floor: ProximityCfg) -> ProximityCfg:
    """The list build's floor config reduced to its OI-lots test (T-282 (d2)): same min_oi_lots, the previous-session
    traded value and the premium tests off (carry reads neither)."""
    return replace(floor, min_prev_turnover_rs=0.0, min_premium_ticks=0.0)


def under_listing_floor(src: Mapping[str, Any], oi: float, floor: ProximityCfg) -> bool:
    """True when `oi` fails the list build's OI floor for this row (chain_select.passes_floor with the row's lot_size;
    `floor` already reduced by listing_floor). An unknown lot fails open, as at the build. min_oi_lots <= 0 is OFF
    (spec §2.3): never under the floor, even at OI 0 (passes_floor's own OI > 0 test is not the lots floor; T-309)."""
    if float(floor.min_oi_lots) <= 0:
        return False
    row = ChainRow(strike=float(src["strike"]), option_type=str(src["option_type"]).upper(),
                   security_id=int(src["security_id"]), ltp=0.0, oi=round(oi), volume=0, iv=None,
                   previous_oi=None)  # fmt: skip
    return not passes_floor(row, lot_size=_num(src.get("lot_size")), tick_size=None, cfg=floor)


def trading_dte(trade_date: date, expiry: date) -> int | None:
    """T-419: trading days d with trade_date < d <= expiry (= engine_pool._dte_cached); 0 on / after expiry; None when
    the calendar cannot say (a year missing from the holiday file)."""
    if expiry <= trade_date:
        return 0
    try:
        return default_calendar().trading_days_between(trade_date, expiry)
    except (ValueError, LookupError, OSError):
        return None


def _evaluate(src: Mapping[str, Any], prev: PrevSession | None, level: Mapping[str, Any] | None, cfg: CarryCfg,
              trade_date: date, open_trade: bool, floor: ProximityCfg | None = None, *, sw: SwCfg | None = None,
              entered: bool = False, in_trade: bool = False) -> _Eval:  # fmt: skip
    strike, opt = float(src["strike"]), str(src["option_type"]).upper()
    spot = _num(level.get("ref_price")) if level else None
    spot = spot if spot and spot > 0 else None
    atr = _num(level.get("atr_pct")) if level else None
    atr = atr if atr and atr > 0 else None
    edge = _num(src.get("zone_edge"))
    oi0 = _num(src.get("oi_at_listing")) if src.get("oi_at_listing") is not None else _num(src.get("oi"))
    prem0 = _num(src.get("prem_at_listing")) if src.get("prem_at_listing") is not None else _num(src.get("chain_ltp"))
    oi_prev = _num(prev.oi) if prev else None
    prem_prev = _num(prev.close) if prev else None
    peak = max(x for x in (oi0 or 0.0, _num(prev.oi_peak) if prev else None, oi_prev) if x is not None)

    dist_atr = None
    if spot is not None and atr is not None:
        dist = (spot - strike) / spot * 100.0 if opt == "PE" else (strike - spot) / spot * 100.0
        dist_atr = dist / atr
    broken = bool(spot is not None and edge is not None and ((spot < edge) if opt == "PE" else (spot > edge)))
    tick = _num(src.get("tick_size")) or default_tick(spot or _num(src.get("ref_price")) or strike)

    retired: str | None = None
    expiry = date.fromisoformat(str(src["expiry_date"]))
    dte = trading_dte(trade_date, expiry) if sw is not None else None
    if expiry < trade_date:
        retired = EXPIRY
    elif sw is not None and dte is not None and dte <= int(sw.dte_max):  # T-419 (D471 Q2): expiry week
        retired = EXPIRY_WEEK
    elif not open_trade and not in_trade:
        if prem_prev is not None and prem_prev < cfg.alive_min_ticks * tick - _EPS:
            retired = DEAD
        elif oi_prev is not None and peak > 0 and oi_prev < cfg.spent_ratio * peak - _EPS:
            retired = SPENT
        elif dist_atr is not None and dist_atr > cfg.away_atr + _EPS:
            retired = AWAY
        elif dist_atr is not None and dist_atr < -cfg.deep_itm_atr - _EPS:
            retired = DEEP_ITM
        elif floor is not None and oi_prev is not None and under_listing_floor(src, oi_prev, floor):
            retired = FLOOR

    ratio = oi_prev / oi0 if (oi_prev is not None and oi0) else None
    state: str | None = None
    why = ""
    if retired is None:
        build = ratio is not None and oi_prev >= cfg.build_ratio * oi0 - _EPS  # type: ignore[operator]
        compressed = prem_prev is not None and prem0 is not None and prem_prev <= prem0 + _EPS
        near = dist_atr is not None and abs(dist_atr) <= cfg.near_atr + _EPS
        if prev is None or oi_prev is None:
            state, why = WATCH, "no previous-session bar"
        elif build and compressed and near:
            state = PRIMED
            why = f"OI {ratio:.2f} x listing, premium {prem_prev:g} <= {prem0:g}, {abs(dist_atr):.2f} ATR"  # type: ignore[arg-type]
        elif build:
            state = BUILD_UP
            why = f"OI {ratio:.2f} x listing" + (" (strong)" if ratio >= cfg.strong_ratio - _EPS else "")  # type: ignore[operator]
        else:
            state = WATCH
            why = "OI below listing" if ratio is not None else "no OI at listing"
        if open_trade:
            why += "; open trade"

    k = int(src.get("carry_k") or 0) + 1 if str(src.get("tag") or "") == CARRY_TAG else 1
    row: dict[str, Any] = {
        "trade_date": trade_date.isoformat(),
        "list_name": src["list_name"],
        "expiry_date": src["expiry_date"],
        "security_id": int(src["security_id"]),
        "symbol": src["symbol"],
        "underlying_security_id": src["underlying_security_id"],
        "strike": strike,
        "option_type": src["option_type"],
        "tag": CARRY_TAG,
        "oi": int(oi_prev) if oi_prev is not None else src.get("oi"),
        "candidates_n": src.get("candidates_n"),
        "ref_price": spot if spot is not None else src.get("ref_price"),
        "zone_edge": edge,
        "spec_version": src.get("spec_version"),
        "lot_size": src.get("lot_size"),
        "tick_size": src.get("tick_size"),
        "chain_ltp": prem_prev if prem_prev is not None else src.get("chain_ltp"),
        "chain_ltp_at": prev.close_at if (prev is not None and prem_prev is not None) else src.get("chain_ltp_at"),
        "atr_pct": atr if atr is not None else src.get("atr_pct"),
        "origin_run_id": src.get("origin_run_id") or src.get("run_id"),
        "first_listed": src.get("first_listed") or src["trade_date"],
        "last_listed": src.get("last_listed") or src["trade_date"],
        "carry_k": k,
        "carry_state": state,
        "carry_flags": BROKEN if (broken and retired is None) else None,
        "oi_at_listing": oi0,
        "prem_at_listing": prem0,
        "carry_reason": retired if retired is not None else f"{state}: {why}",
    }
    if sw is not None:  # T-419: the multi-day OI peak before today (bar OI only; None = no recorded bar since listing)
        pk = _num(prev.oi_peak) if prev is not None else None
        row["oi_peak_prior"] = pk if pk is not None and pk > 0 else None
    return _Eval(row, state, retired, broken, ratio, dist_atr, open_trade, prev is None or oi_prev is None)


def _rank(e: _Eval) -> tuple[Any, ...]:
    return (
        STATE_RANK[e.state or WATCH],
        0 if e.broken else 1,
        -e.ratio if e.ratio is not None else float("inf"),
        abs(e.dist_atr) if e.dist_atr is not None else float("inf"),
        int(e.row["security_id"]),
    )


# -- the store -----------------------------------------------------------------------------------------------------


def _dicts(cur: sqlite3.Cursor) -> list[dict[str, Any]]:
    names = [c[0] for c in cur.description]
    return [dict(zip(names, r, strict=True)) for r in cur.fetchall()]


def source_rows(conn: sqlite3.Connection, trade_date: date, lists_sql: str = "") -> tuple[str | None, list[dict[str, Any]]]:
    """(source date, one row per contract): the latest trade_date before `trade_date` with watchlist rows; its LIVE
    rows plus its `capacity`-retired CARRY rows (a live row wins over a capacity row of the same contract).
    `lists_sql` = edition.v1_lists_sql() (T-419: a client carries only its own lists; "" = every list)."""
    row = conn.execute("SELECT MAX(trade_date) FROM watchlist WHERE trade_date < ?", (trade_date.isoformat(),)).fetchone()
    src_date = None if row is None or row[0] is None else str(row[0])
    if src_date is None:
        return None, []
    rows = _dicts(conn.execute(
        "SELECT * FROM watchlist WHERE trade_date = ? AND (retired_run_id IS NULL OR (tag = ? AND carry_reason = ?))"
        + lists_sql + " ORDER BY (retired_run_id IS NULL) DESC, list_name, security_id",
        (src_date, CARRY_TAG, CAPACITY),
    ))  # fmt: skip
    seen: set[int] = set()
    out: list[dict[str, Any]] = []
    for r in rows:
        sid = int(r["security_id"])
        if sid not in seen:
            seen.add(sid)
            out.append(r)
    return src_date, out


def listed_today(conn: sqlite3.Connection, trade_date: date) -> set[int]:
    """Contracts with a non-CARRY row today (live or retired): the build owns them; carry never re-creates them."""
    cur = conn.execute(
        "SELECT DISTINCT security_id FROM watchlist WHERE trade_date = ? AND COALESCE(tag, '') <> ?",
        (trade_date.isoformat(), CARRY_TAG),
    )
    return {int(r[0]) for r in cur.fetchall()}


def open_trade_ids(conn: sqlite3.Connection, states: frozenset[str] = CARRY_HOLD_STATES) -> set[int]:
    """The live states of every family that HOLDS a carried row (V1 OPEN / T1_TRAIL, Still Watching SW_OPEN /
    SW_TRAILING, V1R V1R_OPEN / V1R_TRAILING / V1R_HALF); ZONE_STATES = the V2T contracts (M11)."""
    marks = ", ".join("?" for _ in states)
    cur = conn.execute(f"SELECT DISTINCT security_id FROM trades WHERE state IN ({marks})", tuple(sorted(states)))
    return {int(r[0]) for r in cur.fetchall()}


ZONE_SCANNER = "V2T"  # trades.scanner of a V2 Scanner trade (its own family; never a V1 / SW entry)


def ladder_open_ids(conn: sqlite3.Connection) -> set[int]:
    """Open V1N contracts in either book keep their carry rows (D657 (2), D658 (1))."""
    if not conn.execute("PRAGMA table_info(ladder_state)").fetchall():
        return set()
    return {int(json.loads(r[0])["security_id"]) for r in conn.execute(
        "SELECT state_json FROM ladder_state WHERE closed = 0 AND scanner = 'V1N'"
    )}


def entered_options(conn: sqlite3.Connection) -> set[tuple[int, str]]:
    """T-430 (1)(d), CX-121 R-121c: every option (security id + expiry) with ANY non-V2T trade in ANY state
    (V1 listed / owner-carried, SW; E1 or E2). A listed V1 trade stands in for the replayed cell's listing-day entry
    (cp_select.py:7-9: its universe starts at first listing). SW-only counting was rejected: an option V1 traded would
    re-enter as Still Watching, which the cell never did. Kind cannot filter (the client stores its entry as E1).
    V2T is its own family (M11). Key by expiry too: an NSE token is re-used for a later contract. A pre-v10 store
    (no trades.scanner) counts every trade."""
    cols = {str(r[1]) for r in conn.execute("PRAGMA table_info(trades)").fetchall()}
    if not cols:
        return set()
    where = " WHERE COALESCE(scanner, 'V1') <> ?" if "scanner" in cols else ""
    cur = conn.execute("SELECT DISTINCT security_id, expiry_date FROM trades" + where,
                       (ZONE_SCANNER,) if where else ())  # fmt: skip
    return {(int(s), str(e)[:10]) for s, e in cur.fetchall() if s is not None and e is not None}


def _upsert(conn: sqlite3.Connection, rows: Sequence[Mapping[str, Any]]) -> None:
    """Insert carry rows; a key already holding a CARRY row is overwritten (a re-run), a listed row never is."""
    have = {str(r[1]) for r in conn.execute("PRAGMA table_info(watchlist)").fetchall()}
    use = [c for c in CARRY_COLUMNS if c in have]
    refresh = ", ".join(f"{c} = excluded.{c}" for c in use if c not in _KEY)
    sql = (
        f"INSERT INTO watchlist ({', '.join(use)}) VALUES ({', '.join('?' for _ in use)}) "
        f"ON CONFLICT ({', '.join(_KEY)}) DO UPDATE SET {refresh} WHERE watchlist.tag = '{CARRY_TAG}'"
    )
    conn.executemany(sql, [tuple(r.get(c) for c in use) for r in rows])
    conn.commit()


def carry_forward(
    conn: sqlite3.Connection,
    trade_date: date,
    run_id: str,
    cfg: CarryCfg,
    *,
    levels: Sequence[Mapping[str, Any]],
    prev_session: PrevSource,
    spec_version: str | None = None,
    floor: ProximityCfg | None = None,
    sw: SwCfg | None = None,
    lists_sql: str = "",
) -> CarrySummary:
    """Carry the source day's contracts into `trade_date` (module docstring). `levels` = today's levels rows (spot,
    atr_pct by symbol); `prev_session` = {security_id: PrevSession} or a callable given {security_id: first listing
    date}; `spec_version` = the spec the rows are written under (None -> the source row's); `floor` = the list build's
    floor config (Settings.proximity; None -> ProximityCfg(), whose min_oi_lots is the yaml's 50) -- its min_oi_lots
    is the FLOOR re-check. The caller gates the edition and `cfg.enabled`; this function carries whenever called.
    T-419: `sw` = the Still Watching config (EXPIRY_WEEK / oi_peak_prior; None = pre-0.8.9 exactly);
    `lists_sql` = the edition's list clause for the source rows (edition.v1_lists_sql())."""
    oi_floor = listing_floor(floor if floor is not None else ProximityCfg())
    summary = CarrySummary()
    src_date, sources = source_rows(conn, trade_date, lists_sql)
    summary.source_date = src_date
    if not sources:
        return summary
    opened = open_trade_ids(conn)
    ladder_ids = ladder_open_ids(conn)
    # M11 (T-267): a contract held only by an open V2T trade is never carried (no carry row, carried or retired)
    skip = listed_today(conn, trade_date) | (open_trade_ids(conn, ZONE_STATES) - opened)
    sources = [r for r in sources if int(r["security_id"]) not in skip]
    if not sources:
        return summary
    wanted = {int(r["security_id"]): date.fromisoformat(str(r.get("first_listed") or r["trade_date"]))
              for r in sources}  # fmt: skip
    prev_map = prev_session(wanted) if callable(prev_session) else prev_session
    by_symbol = {str(r["symbol"]): r for r in levels}
    entered = entered_options(conn) if sw is not None else set()
    evals = [
        _evaluate(r, prev_map.get(int(r["security_id"])), by_symbol.get(str(r["symbol"])), cfg, trade_date,
                  int(r["security_id"]) in opened, oi_floor, sw=sw,
                  entered=(int(r["security_id"]), str(r["expiry_date"])[:10]) in entered,
                  in_trade=int(r["security_id"]) in ladder_ids)
        for r in sources
    ]  # fmt: skip
    alive = sorted((e for e in evals if e.retired is None), key=_rank)
    held = {int(e.row["security_id"]) for e in alive if e.open_trade}  # first in, never dropped
    in_trade_rows = {int(e.row["security_id"]) for e in alive if int(e.row["security_id"]) in ladder_ids}
    rest = [int(e.row["security_id"]) for e in alive
            if not e.open_trade and int(e.row["security_id"]) not in in_trade_rows]
    keep = held | in_trade_rows | set(rest[: max(0, int(cfg.max_contracts) - len(held))])

    out: list[dict[str, Any]] = []
    for e in evals:
        row = dict(e.row, run_id=run_id, spec_version=spec_version or e.row["spec_version"])
        reason = e.retired or (None if int(row["security_id"]) in keep else CAPACITY)
        if reason is None:
            row["retired_run_id"] = None
            summary.carried += 1
            summary.primed += e.state == PRIMED
            summary.build_up += e.state == BUILD_UP
            summary.watch += e.state == WATCH
            summary.broken += e.broken
            summary.no_data += e.no_data
        else:  # a retired row is not carried: no state, no flag; the reason is the code
            row.update(retired_run_id=run_id, carry_reason=reason, carry_state=None, carry_flags=None)
            summary.retired[reason] = summary.retired.get(reason, 0) + 1
        out.append(row)
    _upsert(conn, out)
    log.info("carry %s: %d carried (primed %d, build-up %d, watch %d, broken %d, no bar %d) from %s; retired %s",
             trade_date, summary.carried, summary.primed, summary.build_up, summary.watch, summary.broken,
             summary.no_data, src_date, summary.retired or "none")  # fmt: skip
    return summary
