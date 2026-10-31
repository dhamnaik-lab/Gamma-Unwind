"""Step 2 job: build the OTM watchlists from today's levels and write `runs` + `watchlist`.

Flow for one run (09:10 / 09:20 / refresh / 13:15 / manual; T-277, S1.8.0: `0910` = the pre-open build on the
previous close, `refresh` = the 15-minute price refresh, `1315` = the old name of the 13:15 refresh, same behaviour):
  1. levels = rows of the most recent successful mark_levels run for trade_date with the same ref
     (runs.note starts "ref=<ref>"; a 0910 build reads ref=prevclose, a refresh the newest ltp re-mark); none ->
     NoLevels (loud). T-150(a): a levels run that failed for more than
     MAX_LEVELS_FAILED_PCT of its universe -> the build is recorded failed and stops here (levels_refusal).
  2. approaching support    : 0 <= dist_support_pct    <= proximity.stock_band_pct  (percent units, D20)
     approaching resistance : 0 <= dist_resistance_pct <= proximity.stock_band_pct
  3. per approaching stock: expiries() once per underlying per day (cached in data/expiries-<date>.json),
     then lists_for() (D5, gammad/core/expiry.py) filtered to the stock's side(s): PE lists for
     support, CE lists for resistance. A (symbol, list, expiry) that already has rows today is NOT
     re-fetched: D9 says existing rows are never re-ranked, so a re-scan only adds new stocks/lists.
  4. option_chain() ONCE per (underlying, expiry) still needed; the adapter's 1-per-3-s limiter
     paces it. Expected minutes (= calls * 3 / 60) are logged before the first call.
  5. select_strikes() per ListSpec; INSERT keyed (trade_date, list_name, expiry_date, security_id). A live
     row is never updated and no row is ever deleted (D9). Each row keeps the chain LTP its strike was picked
     with (watchlist.chain_ltp, schema v5, T-053) and when that chain was fetched (chain_ltp_at, v6, T-107):
     the list post's premium and its "(HH:MM)" label before the contract's first tick.
  1b. S1.3.1 (T-063), between steps 1 and 2: a 09:20-KIND run (scheduled, tray, CLI or API) retires
     (watchlist.retired_run_id, never deleted) every PRE-MARKET row (its run started before 09:15 IST on trade_date)
     whose zone is no longer LIVE in its levels (retire_stale); a row of a contract that already ENTERED today (an
     E1 signals row today, or a live trade) is never retired. A retired row is not subscribed, posted or deduped
     against; a later run that picks the same key again un-retires and refreshes it. The runtime runs the retire as
     its own step first (retire_for_run, then load_day drops the contracts, then run(run_id=, retire=False)) so a
     retired contract routes nothing during the chain loop; the CLI keeps the one-call run().
  1c. S1.5.0 (D157(2), D161(6)), after 1b and before the dedupe keys: a 09:20-kind run in the OWNER edition carries
     the previous session's contracts forward as tag = 'CARRY' rows (gammad/jobs/carry.py). A CARRY row is not an
     "existing" list (step 3 re-fetches the stock), the S1.3.1 retire never touches it, and a pick of the same
     contract RE-LISTS it (insert_watchlist_rows): one live row per contract, across list names.
  5b. S1.5.0 (D157(1)): select_strikes applies the LIQUIDITY floor (cfg=proximity: OI >= 50 lots of the list's lot
     from the scrip master, previous-session traded value >= Rs 1 lakh, no premium minimum); the 13:15 re-scan may also
     admit on today's traded value (floor_rows). Unknown lot / previous value = fail-open, counted (floor_stats).

UnderlyingScrip / UnderlyingSeg for a stock chain = the NSE_EQ security_id from the levels row and
"NSE_EQ" (the adapter passes both through verbatim, dhan.py::option_chain). CONFIRMED live
2026-09-25 (D52): 32 expirylist + 64 optionchain calls, 0 errors.
"""

from __future__ import annotations

import json
import re
import sqlite3
import time
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, replace
from datetime import date, datetime
from datetime import time as dtime
from pathlib import Path
from typing import Any, Literal

from gammad import edition
from gammad.broker.base import BrokerAdapter, BrokerAuthError, BrokerError, ChainRow
from gammad.config import IST, CarryCfg, Settings, now_ist, settings
from gammad.core.chain_select import prev_turnover_rs, select_strikes
from gammad.core.expiry import ExpiryMissing, ListSpec, lists_for, validate_rows
from gammad.jobs import carry
from gammad.log import get_logger
from gammad.store import db
from gammad.trade.rules import OPEN_STATES

log = get_logger(__name__)

RunKind = Literal["0910", "0920", "refresh", "1315", "manual"]
RefKind = Literal["ltp", "close", "prevclose"]
# T-277 (S1.8.0): 0910 = the pre-open build on the previous close; refresh = the 15-minute price refresh (1315 is the
# old name of the 13:15 refresh: still accepted, and it behaves exactly like `refresh`)
RUN_KINDS: tuple[str, ...] = ("0910", "0920", "refresh", "1315", "manual")
PREOPEN_KIND = "0910"  # reads the newest `prevclose` levels run, whatever ref the caller passes
REFRESH_KIND = "refresh"
UNDERLYING_SEG = "NSE_EQ"
CHAIN_SECONDS_PER_CALL = 3.0

WATCHLIST_COLUMNS: tuple[str, ...] = (
    "trade_date",
    "list_name",
    "expiry_date",
    "security_id",
    "symbol",
    "underlying_security_id",
    "strike",
    "option_type",
    "tag",
    "oi",
    "candidates_n",
    "ref_price",
    "zone_edge",
    "run_id",
    "spec_version",
    "lot_size",
    "tick_size",  # S1.3.0 (schema v4): rupees from the scrip master; written only when the column exists
    "chain_ltp",  # schema v5 (T-053): the chain premium the strike was picked with; the list post's price pre-tick
    "chain_ltp_at",  # schema v6 (T-107): ISO IST when that chain was fetched = the list post's "(HH:MM)" label
    # schema v9 (S1.5.0): the levels row's atr_pct, the listing (first / last listed, the first listing's run, OI and
    # premium at the FIRST listing) and the carry columns, NULL on a listed row (a re-list resets a carried one)
    "atr_pct",
    "origin_run_id",
    "first_listed",
    "last_listed",
    "carry_k",
    "carry_state",
    "carry_flags",
    "oi_at_listing",
    "prem_at_listing",
    "carry_reason",
)
WATCHLIST_KEY: tuple[str, ...] = ("trade_date", "list_name", "expiry_date", "security_id")
RETIRE_KIND = "0920"  # S1.3.1 (T-063): only a 09:20-kind build retires pre-market rows whose zone died
RESCAN_KIND = "1315"  # S1.5.0: the floor may also read TODAY's traded value (floor_rows)
# T-277: the kinds that read today's traded value (floor_rows) AND add per (stock, side) only (a side with any live
# non-CARRY row today fetches no chain): the 15-minute refresh and its old 13:15 name
RESCAN_KINDS: frozenset[str] = frozenset({REFRESH_KIND, RESCAN_KIND})
PREMARKET_BEFORE = dtime(9, 15)  # S1.3.1: a row is retire-eligible only if its run started before this IST time
PIVOT_TOL = 1e-3  # rupees: zones_json rounds edges to 4 dp; a pivot is a bar's high/low, identical across runs
FLOOR_STATS: tuple[str, ...] = ("lot_unknown", "prev_unknown")  # chain_select.passes_floor counters (D161(5))
# T-150(a): a build whose levels run failed for MORE than this percent of the universe it walked (failed / (levels
# rows + failed)) is recorded failed and runs no chain loop; the rows already in the table stay exactly as they are.
# A module constant, not a yaml key: config.py rejects unknown keys and the yaml has no owner this wave (PLAN-W16 §12).
MAX_LEVELS_FAILED_PCT = 20
_FAILED_COUNT = re.compile(r"(?:^|\s)failed=(\d+):")  # mark_levels >= 0.7.0: "ref=<r> failed=<N>: A,B,...,+k"
_FAILED_NAMES = re.compile(r"(?:^|\s)failed=(\S+)")  # before 0.7.0: "ref=<r> failed=A,B,C" (every name)


class NoLevels(LookupError):
    """No successful mark_levels run wrote levels for this trade_date and ref."""


@dataclass(frozen=True)
class Approach:
    row: Mapping[str, Any]
    support: bool
    resistance: bool

    @property
    def symbol(self) -> str:
        return str(self.row["symbol"])

    @property
    def underlying_id(self) -> int:
        return int(self.row["security_id"])


# -- levels ----------------------------------------------------------------------------------


def latest_levels_run(conn: sqlite3.Connection, trade_date: date, ref: RefKind) -> str | None:
    row = conn.execute(
        "SELECT l.run_id FROM levels l JOIN runs r ON r.run_id = l.run_id "
        "WHERE l.trade_date = ? AND r.kind = 'mark_levels' AND r.status IN ('ok', 'partial') "
        "AND (r.note = ? OR r.note LIKE ?) "
        "ORDER BY r.started_ist DESC, l.run_id DESC LIMIT 1",
        (trade_date.isoformat(), f"ref={ref}", f"ref={ref} %"),
    ).fetchone()
    return None if row is None else str(row[0])


def levels_failed_count(note: str | None) -> int:
    """Per-symbol candle failures a mark_levels runs.note records: 'failed=<N>: ...' (the count; >= 0.7.0) or the
    pre-0.7.0 'failed=A,B,C' (every name listed). No 'failed=' -> 0."""
    text = str(note or "")
    m = _FAILED_COUNT.search(text)
    if m:
        return int(m.group(1))
    m = _FAILED_NAMES.search(text)
    return len([s for s in m.group(1).split(",") if s]) if m else 0


def levels_refusal(conn: sqlite3.Connection, levels_run: str) -> str | None:
    """Why a build must NOT run on `levels_run` (T-150(a), empty-result-two-causes), or None when it may. The run is
    refused when its status is 'failed' or its failures exceed MAX_LEVELS_FAILED_PCT of the symbols it walked
    (levels rows + failures): a list built on 60 % of the universe would look like a quiet morning, not a broken one.

    latest_levels_run() only returns ok / partial runs that wrote levels, and the runtime never builds after a
    mark_levels step that raised (app.py _execute breaks on it), so in practice this is the partial-run test; the
    status check keeps the guard true for any caller that hands a run id in directly."""
    row = conn.execute("SELECT status, note FROM runs WHERE run_id = ?", (levels_run,)).fetchone()
    status, note = (str(row[0] or ""), row[1]) if row else ("", None)
    if status == "failed":
        return f"levels run {levels_run} failed"
    n_failed = levels_failed_count(note)
    if not n_failed:
        return None
    n_rows = int(conn.execute("SELECT COUNT(*) FROM levels WHERE run_id = ?", (levels_run,)).fetchone()[0])
    walked = n_rows + n_failed
    pct = 100.0 * n_failed / walked
    if pct > MAX_LEVELS_FAILED_PCT:
        return f"levels failed={n_failed}/{walked} ({pct:.0f}%) > {MAX_LEVELS_FAILED_PCT}%"
    return None


def _within(pct: Any, band_pct: float) -> bool:
    return pct is not None and 0.0 <= float(pct) <= band_pct + 1e-9  # float slack on the stored percent


def approaching(levels_rows: Sequence[Mapping[str, Any]], band_pct: float,
                side_bands: Mapping[str, float] | None = None) -> list[Approach]:
    """Stocks within band_pct (percent units) of their support top / resistance bottom."""
    out: list[Approach] = []
    for r in levels_rows:
        if not r.get("ref_price"):
            continue
        sup = r.get("support_hi") is not None and _within(r.get("dist_support_pct"),
                                                         (side_bands or {}).get("PE", band_pct))
        res = r.get("resistance_lo") is not None and _within(r.get("dist_resistance_pct"),
                                                            (side_bands or {}).get("CE", band_pct))
        if sup or res:
            out.append(Approach(r, sup, res))
    return out


# -- expiry cache ----------------------------------------------------------------------------


def expiry_cache_path(data_dir: Path, trade_date: date) -> Path:
    return data_dir / f"expiries-{trade_date.isoformat()}.json"


def load_expiry_cache(path: Path) -> dict[str, tuple[int, list[date]]]:
    """{symbol: (underlying_security_id, [expiry, ...])} written by earlier runs of the same trade_date."""
    if not path.exists():
        return {}
    raw = json.loads(path.read_text(encoding="utf-8"))
    return {
        str(sym): (int(v["security_id"]), sorted(date.fromisoformat(d) for d in v["expiries"]))
        for sym, v in raw.items()
    }


def _save_expiry_cache(path: Path, entries: Mapping[str, tuple[int, list[date]]]) -> None:
    doc = {
        sym: {"security_id": sid, "expiries": [d.isoformat() for d in exps]}
        for sym, (sid, exps) in entries.items()
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".json.part")
    tmp.write_text(json.dumps(doc, indent=1, sort_keys=True), encoding="utf-8", newline="\n")
    tmp.replace(path)


# -- store -----------------------------------------------------------------------------------


def existing_keys(conn: sqlite3.Connection, trade_date: date) -> set[tuple[str, str, str]]:
    """(symbol, list_name, expiry_date) that already hold LIVE LISTED rows today; a retired row (S1.3.1) does not
    count, so a later run may add that list again, and neither does a CARRY row (S1.5.0, D157(2)): a stock holding
    only carried rows gets its chain re-ranked (the 29 Sep KFINTECH skip)."""
    cur = conn.execute(
        "SELECT DISTINCT symbol, list_name, expiry_date FROM watchlist WHERE trade_date = ? AND retired_run_id IS NULL "
        "AND COALESCE(tag, '') <> ?",
        (trade_date.isoformat(), carry.CARRY_TAG),
    )
    return {(str(a), str(b), str(c)) for a, b, c in cur.fetchall()}


def existing_sides(conn: sqlite3.Connection, trade_date: date) -> set[tuple[str, str]]:
    """T-277: (symbol, option_type) holding at least one LIVE LISTED row today (retired / CARRY rows do not count).
    A refresh-kind build (RESCAN_KINDS) fetches no chain for such a side: it already holds a list today, and a list
    whose strike pick came back empty is not re-fetched every 15 minutes. D429: in the CLIENT edition an EXPIRY row (a
    store built before 0.8.8) does not count either: the client never lists one, so it must not block that side."""
    cur = conn.execute(
        "SELECT DISTINCT symbol, option_type FROM watchlist WHERE trade_date = ? AND retired_run_id IS NULL "
        "AND COALESCE(tag, '') <> ?" + edition.v1_lists_sql(),
        (trade_date.isoformat(), carry.CARRY_TAG),
    )
    return {(str(a), str(b).upper()) for a, b in cur.fetchall()}


def _refresh_mark_of(conn: sqlite3.Connection, levels_run: str) -> str | None:
    """The `refresh=<HHMM>` mark a mark_levels re-mark wrote into its runs.note (T-277), or None."""
    row = conn.execute("SELECT note FROM runs WHERE run_id = ?", (levels_run,)).fetchone()
    for part in str(row[0] if row and row[0] else "").split():
        if part.startswith("refresh="):
            return part.split("=", 1)[1]
    return None


def insert_watchlist_rows(conn: sqlite3.Connection, rows: Sequence[Mapping[str, Any]]) -> tuple[int, int]:
    """Insert each row. Returns (n_new, n_already_present). A LIVE listed row is never updated (D9); a RETIRED row
    with the same key (S1.3.1: a later run picked it again) is un-retired and refreshed from the new row, counted as
    new. When the run re-picking it is the SAME run that retired it (a 09:20 build whose retire step dropped the row
    and whose chain loop picked the same key on a new zone, T-108), the row keeps its ORIGINAL run_id: its history is
    its pre-market run's, and a replay holds rows by run_id. Every other non-key column is still refreshed. Only
    the WATCHLIST_COLUMNS the table has are written (tick_size v4, chain_ltp v5, chain_ltp_at v6, the v9 columns); a
    pre-v6 table (no retired_run_id) keeps plain INSERT OR IGNORE.

    S1.5.0 RE-LIST (D161(6)): a LIVE CARRY row with the same key is overwritten like a retired one (tag, carry columns
    and all: the contract is listed again), and a CARRY row of the same contract under ANOTHER list name (a carry
    reaching DTE <= 7 re-picked under EXPIRY-*) is retired by the re-picking run with carry_reason 'relisted', so a
    contract has one live row a day."""
    have = {str(r[1]) for r in conn.execute("PRAGMA table_info(watchlist)").fetchall()}
    use = [c for c in WATCHLIST_COLUMNS if c in have]
    cols = ", ".join(use)
    marks = ", ".join("?" for _ in use)
    relist = {"tag", "retired_run_id", "carry_reason"} <= have
    if "retired_run_id" in have:
        refresh = ", ".join(
            "run_id = CASE WHEN watchlist.retired_run_id = excluded.run_id THEN watchlist.run_id "
            "ELSE excluded.run_id END"
            if c == "run_id"
            else f"{c} = excluded.{c}"
            for c in use
            if c not in WATCHLIST_KEY
        )
        sql = (
            f"INSERT INTO watchlist ({cols}) VALUES ({marks}) ON CONFLICT ({', '.join(WATCHLIST_KEY)}) "
            f"DO UPDATE SET {refresh}, retired_run_id = NULL "
            f"WHERE watchlist.retired_run_id IS NOT NULL OR watchlist.tag = '{carry.CARRY_TAG}'"
        )
    else:
        sql = f"INSERT OR IGNORE INTO watchlist ({cols}) VALUES ({marks})"
    new = 0
    for r in rows:
        if relist and r.get("tag") != carry.CARRY_TAG:
            conn.execute(
                "UPDATE watchlist SET retired_run_id = COALESCE(retired_run_id, ?), carry_reason = ? "
                "WHERE trade_date = ? AND security_id = ? AND tag = ? AND list_name <> ?",
                (r.get("run_id"), carry.RELISTED, r.get("trade_date"), r.get("security_id"), carry.CARRY_TAG,
                 r.get("list_name")),
            )  # fmt: skip
        cur = conn.execute(sql, tuple(r.get(c) for c in use))
        new += cur.rowcount == 1
    conn.commit()
    return new, len(rows) - new


@dataclass(frozen=True)
class Listing:
    """A contract's FIRST listing (schema v9): kept by every later re-list, carry's build-up base included."""

    first_listed: str
    origin_run_id: str | None
    oi_at_listing: float | None
    prem_at_listing: float | None


def listing_history(conn: sqlite3.Connection, security_ids: Sequence[int]) -> dict[int, Listing]:
    """security_id -> its FIRST listing, read from its EARLIEST watchlist row (any date, any tag; a pre-v9 row reads
    its trade_date / run_id / oi / chain_ltp). A contract listed again keeps its first listing's date, run and
    build-up base (oi_at_listing / prem_at_listing: orchestrator ruling 2026-09-30, the research carry.py 207/223
    measured the FIRST listing's OI and premium, carry DESIGN section 2)."""
    out: dict[int, Listing] = {}
    ids = sorted({int(s) for s in security_ids})
    for i in range(0, len(ids), 500):
        chunk = ids[i : i + 500]
        cur = conn.execute(
            "SELECT security_id, COALESCE(first_listed, trade_date) AS fl, COALESCE(origin_run_id, run_id), "
            "COALESCE(oi_at_listing, oi), COALESCE(prem_at_listing, chain_ltp) "
            f"FROM watchlist WHERE security_id IN ({', '.join('?' for _ in chunk)}) "
            "ORDER BY fl DESC, trade_date DESC, rowid DESC",
            chunk,
        )
        for sid, first, origin, oi0, prem0 in cur.fetchall():  # descending: the last row read is the earliest
            out[int(sid)] = Listing(str(first), None if origin is None else str(origin),
                                    None if oi0 is None else float(oi0), None if prem0 is None else float(prem0))  # fmt: skip
    return out


def floor_rows(chain_rows: Sequence[ChainRow], run_kind: str) -> Sequence[ChainRow]:
    """The rows the S1.5.0 floor reads. At the 13:15 re-scan (far-OTM DESIGN 5.1: 'the 13:15 re-scan may use
    today's') a strike whose traded value TODAY so far (the chain's volume, the day's cumulative QUANTITY, x its LTP)
    beats the previous session's is floored on today's: the previous-session fields of the row handed to
    select_strikes carry today's volume and LTP. Today's value can only ADMIT: an unknown previous session stays
    unknown (fail-open, D161(5)) and a 09:10 / 09:20 / manual build reads the previous session alone. T-277: the
    15-minute `refresh` reads it like 13:15 (RESCAN_KINDS)."""
    if run_kind not in RESCAN_KINDS:
        return chain_rows
    out: list[ChainRow] = []
    for r in chain_rows:
        prev = prev_turnover_rs(r)
        today = float(r.volume) * float(r.ltp) if (r.volume and r.ltp) else 0.0
        out.append(replace(r, previous_volume=r.volume, previous_close=r.ltp) if prev is not None and today > prev
                   else r)  # fmt: skip
    return out


def _master_rows(adapter: BrokerAdapter, symbol: str, expiry: date) -> list[Any]:
    """The scrip master's option Instruments for (symbol, expiry); [] when the adapter has no master."""
    master_fn = getattr(adapter, "scrip_master", None)
    if master_fn is None:
        return []
    try:
        return list(master_fn().option_rows(symbol, expiry))
    except (BrokerError, OSError, ValueError, KeyError) as exc:
        log.warning("build_watchlists: scrip master rows for %s %s unavailable: %s", symbol, expiry, exc)
        return []


def tick_map(instruments: Sequence[Any]) -> dict[int, float | None]:
    """security_id -> tick in rupees (Instrument.tick_size; None when the master row has none)."""
    return {int(i.security_id): getattr(i, "tick_size", None) for i in instruments}


def chain_ltp_map(chain_rows: Sequence[ChainRow]) -> dict[int, float | None]:
    """security_id -> the chain's LTP in rupees (watchlist.chain_ltp, T-053); a 0 / missing LTP (no trade yet) -> None."""
    out: dict[int, float | None] = {}
    for r in chain_rows:
        if r.security_id is None:
            continue
        ltp = r.ltp
        out[int(r.security_id)] = float(ltp) if ltp is not None and float(ltp) > 0 else None
    return out


def chain_iv_map(chain_rows: Sequence[ChainRow]) -> dict[int, float | None]:
    """0.8.8 (D380 (3)): security_id -> the chain's IV exactly as the broker sent it (ChainRow.iv; Dhan: percent);
    None = not sent. Read only by the owner edition's listing capture (_capture_listing); no rule reads it."""
    return {int(r.security_id): r.iv for r in chain_rows if r.security_id is not None}


def _capture_listing(conn: sqlite3.Connection, rows: Sequence[Mapping[str, Any]],
                     chain_iv: Mapping[int, float | None]) -> None:  # fmt: skip
    """0.8.8 (D380 (3), D382), OWNER edition only: the listed contracts' chain IV handed to the owner extension's store
    (gammad/scan/owner_hooks.py capture_listing). The client package ships no gammad/scan, so the import is guarded
    like app.py's; any fault is logged and never costs the build (its rows are already committed)."""
    try:
        from gammad.scan import owner_hooks
    except ImportError:  # CLIENT_EXCLUDE: the module is not in the package
        return
    try:
        owner_hooks.capture_listing(conn, rows, chain_iv)
    except Exception:  # descriptive data only: the lists outrank it
        conn.rollback()
        log.exception("build_watchlists: listing capture failed; the lists are unaffected")


# -- S1.3.1 retire (T-063) ---------------------------------------------------------------------


def _zones(levels_row: Mapping[str, Any] | None) -> dict[str, Any] | None:
    """The row's zones_json, or None when it is missing / unreadable / built from no bars (unknown, not 'gone')."""
    raw = None if levels_row is None else levels_row.get("zones_json")
    if not raw:
        return None
    try:
        doc = json.loads(raw)
    except (TypeError, ValueError):
        return None
    if not isinstance(doc, dict) or not doc.get("n_bars"):
        return None
    return doc


def _side(option_type: str) -> tuple[str, int, int]:
    """(zones_json side, near-edge index, pivot index). A PE row's zone_edge = support_hi = the buy zone's TOP (its
    pivot is the bottom, the pivot low); a CE row's = resistance_lo = the sell zone's BOTTOM (pivot = top). zones_json
    entries are [top, bottom, formed, confirmed] (mark_levels._zone_list)."""
    return ("buy", 0, 1) if option_type == "PE" else ("sell", 1, 0)


def _near(a: Any, b: float) -> bool:
    return a is not None and abs(float(a) - b) <= PIVOT_TOL


def _levels_run_of(conn: sqlite3.Connection, build_run_id: str) -> str | None:
    """The levels run a build read (its runs.note 'kind=<k> levels_run=<id> ...')."""
    row = conn.execute("SELECT note FROM runs WHERE run_id = ?", (build_run_id,)).fetchone()
    for part in str(row[0] if row and row[0] else "").split():
        if part.startswith("levels_run="):
            return part.split("=", 1)[1]
    return None


def _started_premarket(started_ist: Any, trade_date: date) -> bool | None:
    """True when a run's runs.started_ist is before 09:15 IST on trade_date; None when missing / unreadable."""
    if not started_ist:
        return None
    try:
        at = datetime.fromisoformat(str(started_ist))
    except ValueError:
        return None
    at = at.replace(tzinfo=IST) if at.tzinfo is None else at.astimezone(IST)
    return at < datetime.combine(trade_date, PREMARKET_BEFORE, tzinfo=IST)


def entered_today(conn: sqlite3.Connection, trade_date: date) -> set[int]:
    """Contracts that already ENTERED: an E1 signals row on trade_date (gated or not), or a live trade (any entry
    date: a swing trade holds its contract). Their rows are never retired: an E1 sent at 09:16 and retired at 09:21
    would leave a sent alert with no list row. T-267 M11: a V2 Scanner trade (Z_OPEN) holds its contract the same way
    (rules.OPEN_STATES = V1's live states + ZONE_STATES). D278(5): an E1 still HELD for the 09:20 list post has not
    entered (nothing was sent), so the 09:20 retire may retire its contract and the release then refuses it.
    0.8.5 (D288 Q6, PLAN-D287 I3): a TIERED V1 row of `edition.client_entry_kind()` (any tier, as an E1 gated or not)
    has entered too -- the owner store's group entry is the V1 E2 at its bar close (the client store records it as E1,
    so nothing changes there); an untiered E2 is an arm, never an entry."""
    marks = ", ".join("?" for _ in OPEN_STATES)
    cur = conn.execute(
        "SELECT security_id FROM signals WHERE trade_date = ?"
        " AND (kind = 'E1' OR (kind = ? AND scanner = 'V1' AND tier IS NOT NULL))"
        " AND (CASE WHEN json_valid(ctx_json) THEN json_extract(ctx_json, '$.held') END) IS NOT 'pending' "
        f"UNION SELECT security_id FROM trades WHERE state IN ({marks})",
        (trade_date.isoformat(), edition.client_entry_kind(), *sorted(OPEN_STATES)),
    )
    return {int(r[0]) for r in cur.fetchall()}


def retire_stale(
    conn: sqlite3.Connection,
    trade_date: date,
    run_id: str,
    levels_rows: Sequence[Mapping[str, Any]],
) -> tuple[int, int]:
    """S1.3.1: mark every live PRE-MARKET row of trade_date (its build run started before 09:15 IST on trade_date)
    whose zone is no longer LIVE in `levels_rows` (this run's levels) as retired by `run_id`; rows are never deleted.
    Never a row of a later run (a 14:00 0920-kind run leaves the 13:15 rows alone) nor of a contract that already
    entered today (entered_today). Returns (n_retired, n_unresolved); a row whose run has no readable runs row is
    kept and counted unresolved.

    The zone is identified by its PIVOT edge (buy bottom / sell top: a bar's low / high, the same number in every
    run). The near edge (the row's zone_edge) is pivot +/- ATR*depth, and the ATR moves when the 500-bar window
    moves: 2026-09-25 live store, 00:46 vs 09:20 levels, 53 of 1033 LIVE zones' near edges moved > Rs 0.05 (max
    Rs 2.20), so a near-edge match would retire live rows. The pivot is read from the levels run the row's build used
    (its zone whose near edge equals zone_edge). Anything unresolvable (no build note, no such zone, the symbol absent
    from this run's levels, a levels row with no bars, a TRUNCATED window: this run's levels row built from fewer bars
    than the build's, T-108, so a zone may merely fall outside it) is KEPT and counted: unknown is not gone."""
    now_levels = {str(r["symbol"]): r for r in levels_rows}
    rows = conn.execute(  # S1.5.0: a CARRY row is carry's to retire; a dead zone is its BROKEN flag, not this retire
        "SELECT w.rowid, w.symbol, w.option_type, w.zone_edge, w.run_id, w.security_id, r.started_ist "
        "FROM watchlist w LEFT JOIN runs r ON r.run_id = w.run_id "
        "WHERE w.trade_date = ? AND w.retired_run_id IS NULL AND COALESCE(w.run_id, '') <> ? "
        "AND COALESCE(w.tag, '') <> ?",
        (trade_date.isoformat(), run_id, carry.CARRY_TAG),
    ).fetchall()
    entered = entered_today(conn, trade_date)
    then_cache: dict[tuple[str, str], dict[str, Any] | None] = {}
    retire: list[int] = []
    unresolved = n_later = n_entered = 0
    truncated: set[str] = set()
    for rowid, symbol, option_type, zone_edge, build_run, security_id, started_ist in rows:
        symbol, build_run = str(symbol), str(build_run or "")
        pre = _started_premarket(started_ist, trade_date)
        if pre is None:
            unresolved += 1  # no runs row / no start time: cannot tell pre-market from a later run
            continue
        if not pre:
            n_later += 1  # built at/after 09:15: that run already read today's levels; never retired
            continue
        if int(security_id) in entered:
            n_entered += 1
            continue
        side, near_i, pivot_i = _side(str(option_type).upper())
        if symbol not in now_levels:
            unresolved += 1  # not in this run's levels (symbols filter / failed fetch): no evidence either way
            continue
        now_doc = _zones(now_levels[symbol])
        key = (build_run, symbol)
        if key not in then_cache:
            lv_run = _levels_run_of(conn, build_run)
            then_rows = [] if lv_run is None else db.fetch_levels(conn, trade_date.isoformat(), lv_run)
            then_cache[key] = _zones(next((r for r in then_rows if r["symbol"] == symbol), None))
        then_doc = then_cache[key]
        if now_doc is None or then_doc is None or zone_edge is None:
            unresolved += 1
            continue
        if int(now_doc.get("n_bars") or 0) < int(then_doc.get("n_bars") or 0):
            unresolved += 1  # T-108: a shorter window than the build's; a missing zone may be outside it, not dead
            truncated.add(symbol)
            continue
        pivot = next((z[pivot_i] for z in then_doc.get(side, []) if _near(z[near_i], float(zone_edge))), None)
        if pivot is None:
            unresolved += 1
            continue
        if not any(_near(z[pivot_i], float(pivot)) for z in now_doc.get(side, [])):
            retire.append(int(rowid))
    if retire:
        conn.executemany(
            "UPDATE watchlist SET retired_run_id = ? WHERE rowid = ? AND retired_run_id IS NULL",
            [(run_id, r) for r in retire],
        )
        conn.commit()
    if n_later or n_entered:
        log.info("retire %s: kept %d row(s) of a run started at/after 09:15, %d of an entered contract",
                 run_id, n_later, n_entered)  # fmt: skip
    if truncated:
        log.info("retire %s: kept the rows of %d symbol(s) whose levels window has fewer bars than their build's",
                 run_id, len(truncated))  # fmt: skip
    return len(retire), unresolved


def new_run_id(trade_date: date, run_kind: str, started: datetime | None = None) -> str:
    """The build's run_id; minted before the build when the runtime retires first (T-063 M1)."""
    at = started or now_ist()
    return f"watchlists-{trade_date.isoformat()}-{run_kind}-{at:%Y%m%dT%H%M%S%f}"


def retire_for_run(
    conn: sqlite3.Connection,
    trade_date: date,
    run_id: str,
    levels_run: str,
    symbols: Sequence[str] | None = None,
) -> tuple[int, int]:
    """S1.3.1 as its own step: retire_stale against the levels of `levels_run` (optionally only `symbols`)."""
    rows = db.fetch_levels(conn, trade_date.isoformat(), levels_run)
    if symbols:
        wanted = {s.strip().upper() for s in symbols if s.strip()}
        rows = [r for r in rows if r["symbol"] in wanted]
    return retire_stale(conn, trade_date, run_id, rows)


def run_retire(
    trade_date: date,
    run_id: str,
    ref: RefKind = "ltp",
    *,
    conn: sqlite3.Connection | None = None,
    cfg: Settings | None = None,
) -> dict[str, Any]:
    """The runtime's retire step (T-063 M1): retire against the levels the 09:20-kind build `run_id` will read. No
    levels -> nothing retired (the build that follows reports NoLevels in its own runs row)."""
    cfg = cfg or settings()
    own_conn = conn is None
    conn = conn or db.connect(cfg.paths.db_path)
    try:
        db.migrate(conn)
        levels_run = latest_levels_run(conn, trade_date, ref)
        if levels_run is None:
            return {"status": "ok", "run_id": run_id, "levels_run_id": None, "n_retired": 0, "n_retire_unresolved": 0}
        refused = levels_refusal(conn, levels_run)
        if refused:  # T-150(a): the build that follows refuses the same run; nothing is retired on it either
            log.warning("retire %s: skipped, %s (levels %s)", run_id, refused, levels_run)
            return {"status": "ok", "run_id": run_id, "levels_run_id": levels_run, "n_retired": 0,
                    "n_retire_unresolved": 0, "refused": refused}  # fmt: skip
        n_retired, n_unresolved = retire_for_run(conn, trade_date, run_id, levels_run)
        log.info("retire %s: %d retired, %d unresolved (levels %s)", run_id, n_retired, n_unresolved, levels_run)
        return {"status": "ok", "run_id": run_id, "levels_run_id": levels_run,
                "n_retired": n_retired, "n_retire_unresolved": n_unresolved}  # fmt: skip
    finally:
        if own_conn:
            conn.close()


# -- job -------------------------------------------------------------------------------------


def run(
    trade_date: date,
    run_kind: RunKind = "manual",
    ref: RefKind = "ltp",
    symbols: Sequence[str] | None = None,
    *,
    adapter: BrokerAdapter | None = None,
    conn: sqlite3.Connection | None = None,
    cfg: Settings | None = None,
    cache_dir: Path | None = None,
    run_id: str | None = None,
    retire: bool = True,
    parquet_root: Path | None = None,
    mark_hm: str | None = None,
    v1r: Any = None,
) -> dict[str, Any]:
    """One list build. `run_id` = a pre-minted id (new_run_id; the runtime's retire step already used it);
    `retire=False` = a 09:20-kind build whose retire already ran as its own step (retire_for_run). `parquet_root` =
    the recorder's folder carry reads the previous session's bars from (default <data_dir>/parquet).

    T-277 (S1.8.0) kinds: `0910` reads the newest `prevclose` levels run (whatever `ref` says); `refresh` / `1315`
    read the newest run of `ref` (ltp: the newest re-mark), add per (stock, side) only (existing_sides) and floor on
    today's traded value too. Only `0920` retires and carries. `mark_hm` ("HHMM") = a refresh's mark for the note
    (` mark=<HHMM>`); None -> the levels run's `refresh=` mark, else this build's start time.

    S1.5.0: the strikes pass the LIQUIDITY floor (chain_select.passes_floor with cfg.proximity and the list's lot;
    its fail-open counters land in summary['floor_stats']); a 09:20-kind run in the OWNER edition first carries the
    previous session's contracts forward (gammad/jobs/carry.py, summary['carry']; None = carry did not run)."""
    if run_kind not in RUN_KINDS:
        raise ValueError(f"run_kind must be one of {RUN_KINDS}")
    if ref not in ("ltp", "close", "prevclose"):
        raise ValueError("ref must be 'ltp', 'close' or 'prevclose'")
    levels_ref: RefKind = "prevclose" if run_kind == PREOPEN_KIND else ref
    t0 = time.perf_counter()
    cfg = cfg or settings()
    band = float(cfg.proximity.stock_band_pct)
    strike_band = float(cfg.proximity.strike_band_pct)
    top_n = edition.top_n_oi(cfg)  # T-153: 1 for the client edition (HOI-1), the yaml value for the owner
    threshold = int(cfg.expiry.expiry_dte_threshold)
    carry_cfg: CarryCfg = getattr(cfg, "carry_fwd", None) or CarryCfg()
    if adapter is None:
        from gammad.broker.dhan import DhanAdapter

        adapter = DhanAdapter.from_settings()
    own_conn = conn is None
    conn = conn or db.connect(cfg.paths.db_path)
    db.migrate(conn)
    cache_path = expiry_cache_path(cache_dir or cfg.paths.data_dir, trade_date)

    started = now_ist()
    run_id = run_id or new_run_id(trade_date, run_kind, started)
    db.insert_run(
        conn, run_id, "build_watchlists", started.isoformat(), cfg.spec_version, note=f"kind={run_kind}"
    )
    status, note = "failed", f"kind={run_kind}"
    summary: dict[str, Any] = {"run_id": run_id, "trade_date": trade_date.isoformat(), "kind": run_kind}
    try:
        levels_run = latest_levels_run(conn, trade_date, levels_ref)
        if levels_run is None:
            raise NoLevels(f"no successful mark_levels ref={levels_ref} run wrote levels for {trade_date}")
        refused = levels_refusal(conn, levels_run)
        if refused:  # T-150(a): recorded failed, no retire, no chain loop, the table is not touched
            note = f"kind={run_kind} levels_run={levels_run} refused: {refused}"
            log.error("build_watchlists %s: refused, %s; the rows already built today stay", run_id, refused)
            summary.update(levels_run_id=levels_run, refused=refused, n_rows_new=0, n_failed=0)
            return summary  # status stays "failed" (the finally records it)
        all_levels = db.fetch_levels(conn, trade_date.isoformat(), levels_run)
        rows = all_levels
        if symbols:
            wanted = {s.strip().upper() for s in symbols if s.strip()}
            rows = [r for r in rows if r["symbol"] in wanted]
        n_retired = n_unresolved = 0
        if run_kind == RETIRE_KIND and retire:  # S1.3.1: BEFORE the dedupe keys, so this run may re-add a retired list
            n_retired, n_unresolved = retire_stale(conn, trade_date, run_id, rows)
        carried: carry.CarrySummary | None = None
        # 0.8.9 T-419 (D471 Q10, D480): carry runs in BOTH editions while still_watching.enabled (the client's "Still
        # Watching" list); the owner also keeps carrying on carry_forward.enabled alone (pre-0.8.9). 09:20 only.
        sw_cfg = carry.still_watching_cfg(cfg)
        sw_on = bool(sw_cfg.enabled)
        if run_kind == RETIRE_KIND and ((edition.is_owner() and carry_cfg.enabled) or sw_on):
            root = parquet_root or cfg.paths.data_dir / "parquet"
            try:  # a carry failure must not cost the day's lists: logged loudly, recorded in the note
                carried = carry.carry_forward(conn, trade_date, run_id, carry_cfg, levels=all_levels,
                                              prev_session=carry.parquet_prev_session(root, before=trade_date),
                                              spec_version=cfg.spec_version,
                                              floor=cfg.proximity,  # T-282 (d2): the build's own floor config
                                              sw=sw_cfg if sw_on else None,
                                              lists_sql=edition.v1_lists_sql())  # fmt: skip
            except Exception as exc:  # the owner's lists outrank the watch-only carry
                conn.rollback()
                log.exception("build_watchlists %s: carry-forward failed; lists are built without it", run_id)
                summary["carry_error"] = f"{type(exc).__name__}: {exc}"
        if v1r is None:
            appr = approaching(rows, band)
        else:
            from gammad.v1 import listing

            mark = mark_hm or (run_kind if run_kind.isdigit() else _refresh_mark_of(conn, levels_run))
            mark = mark or started.strftime("%H%M")
            mark = mark.replace(":", "")
            hm = f"{mark[:2]}:{mark[2:]}"
            legacy = (*cfg.schedule.refresh.marks(), *cfg.schedule.build_watchlists, "09:10")
            bands = {side: listing.band_pct(v1r, side, band) for side in ("PE", "CE")}
            appr = [Approach(a.row, a.support and (run_kind == RETIRE_KIND or
                        listing.lists_side(v1r, "PE", hm, legacy)),
                        a.resistance and (run_kind == RETIRE_KIND or
                        listing.lists_side(v1r, "CE", hm, legacy))) for a in approaching(rows, band, bands)]
            appr = [a for a in appr if a.support or a.resistance]
        n_sup = sum(a.support for a in appr)
        n_res = sum(a.resistance for a in appr)

        before = int(  # LIVE LISTED rows only, as per_list_total counts them (retired / carried rows are not "existing")
            conn.execute(
                "SELECT COUNT(*) FROM watchlist WHERE trade_date = ? AND retired_run_id IS NULL AND COALESCE(tag, '') <> ?"
                + edition.v1_lists_sql(),  # D429: a client counts only its own lists ("" for the owner)
                (trade_date.isoformat(), carry.CARRY_TAG),
            ).fetchone()[0]
        )
        have = existing_keys(conn, trade_date)
        cache_entries = load_expiry_cache(cache_path)
        n_expiry_calls = 0
        failed: list[str] = []
        # plan: symbol -> (approach, expiries, [specs still to fill])
        plan: list[tuple[Approach, list[date], list[ListSpec]]] = []
        n_skipped = 0
        n_sides_skipped = 0
        if run_kind in RESCAN_KINDS:  # T-277: a side that already holds a live list today fetches nothing
            sides_have = existing_sides(conn, trade_date)
            kept: list[Approach] = []
            for a in appr:
                sup = a.support and (a.symbol, "PE") not in sides_have
                res = a.resistance and (a.symbol, "CE") not in sides_have
                n_sides_skipped += (a.support and not sup) + (a.resistance and not res)
                if sup or res:
                    kept.append(Approach(a.row, sup, res))
            appr_todo = kept
        else:
            appr_todo = appr
        for a in appr_todo:
            cached = cache_entries.get(a.symbol)
            exps = cached[1] if cached and cached[0] == a.underlying_id else []
            try:
                if not exps:
                    exps = list(adapter.expiries(a.underlying_id, UNDERLYING_SEG))
                    n_expiry_calls += 1
                    cache_entries[a.symbol] = (a.underlying_id, exps)
                specs = lists_for(trade_date, exps, threshold=threshold)
            except BrokerAuthError:
                raise
            except (BrokerError, ExpiryMissing) as exc:
                log.warning("build_watchlists: %s expiries unusable: %s", a.symbol, exc)
                failed.append(a.symbol)
                continue
            sides = {"PE"} if a.support else set()
            if a.resistance:
                sides.add("CE")
            todo = []
            for s in specs:
                # D429: the CLIENT edition builds only BREAKDOWN-PE / BREAKOUT-CE (expiry week = the next-month rows);
                # its EXPIRY specs fetch no chain and write no row. The owner builds every list (v1_list_ok is True).
                if s.side not in sides or not edition.v1_list_ok(s.list_name):
                    continue
                if (a.symbol, s.list_name, s.expiry_date.isoformat()) in have:
                    n_skipped += 1
                    continue
                todo.append(s)
            if todo:
                plan.append((a, list(exps), todo))
        if n_expiry_calls:
            _save_expiry_cache(cache_path, cache_entries)

        calls = sorted({(a.underlying_id, s.expiry_date) for a, _, specs in plan for s in specs})
        log.info(
            "build_watchlists %s: %d approaching (support %d, resistance %d), %d chain calls "
            "~%.1f min at 1 per %.0f s, %d list(s) already filled today",
            run_kind,
            len(appr),
            n_sup,
            n_res,
            len(calls),
            len(calls) * CHAIN_SECONDS_PER_CALL / 60.0,
            CHAIN_SECONDS_PER_CALL,
            n_skipped,
        )

        chains: dict[tuple[int, date], list[ChainRow]] = {}
        chain_at: dict[tuple[int, date], str] = {}  # T-107: when each chain was fetched (the premium's time)
        capture = edition.is_owner()  # 0.8.8 (D380 (3)): the owner edition records the listed contracts' chain IV
        chain_iv: dict[int, float | None] = {}
        new_rows: list[dict[str, Any]] = []
        floor_stats: dict[str, int] = dict.fromkeys(FLOOR_STATS, 0)
        empty_chain: set[tuple[str, str]] = set()  # R2: (symbol, side) whose 0910 chain had no strike with OI > 0
        for a, exps, specs in plan:
            ref_price = float(a.row["ref_price"])
            atr = a.row.get("atr_pct")  # schema v9: copied onto every row (the dynamic zone gate reads it there)
            sym_rows: list[dict[str, Any]] = []
            try:
                for s in specs:
                    ck = (a.underlying_id, s.expiry_date)
                    if ck not in chains:
                        chains[ck] = adapter.option_chain(a.underlying_id, UNDERLYING_SEG, s.expiry_date)
                        chain_at[ck] = now_ist().isoformat()
                    if run_kind == PREOPEN_KIND and not any(
                        r.oi and r.oi > 0 for r in chains[ck] if r.option_type == s.side
                    ):  # R2: pre-open junk (empty / every strike at OI 0) lists nothing; 09:20 fetches it again
                        empty_chain.add((a.symbol, s.side))
                        continue
                    edge = float(a.row["support_hi"] if s.side == "PE" else a.row["resistance_lo"])
                    master = _master_rows(adapter, a.symbol, s.expiry_date)
                    lot = master[0].lot_size if master else None  # BEFORE the pick: the floor's OI >= 50 lots
                    picks = select_strikes(
                        floor_rows(chains[ck], run_kind),
                        s.side,
                        ref_price,
                        edge,
                        strike_band,
                        top_n,
                        cfg=cfg.proximity,
                        lot_size=lot,
                        stats=floor_stats,
                        ticks=tick_map(master),
                    )
                    premiums = chain_ltp_map(chains[ck]) if picks else {}
                    if capture and picks:
                        chain_iv.update(chain_iv_map(chains[ck]))
                    for p in picks:
                        sym_rows.append(
                            {
                                "trade_date": trade_date.isoformat(),
                                "list_name": s.list_name,
                                "expiry_date": s.expiry_date.isoformat(),
                                "security_id": p.security_id,
                                "symbol": a.symbol,
                                "underlying_security_id": a.underlying_id,
                                "strike": p.strike,
                                "option_type": s.side,
                                "tag": p.tag,
                                "oi": p.oi,
                                "candidates_n": p.candidates_n,
                                "ref_price": ref_price,
                                "zone_edge": edge,
                                "run_id": run_id,
                                "spec_version": cfg.spec_version,
                                "lot_size": lot,
                                "tick_size": p.tick_size,
                                "chain_ltp": premiums.get(p.security_id),
                                "chain_ltp_at": chain_at[ck],
                                "atr_pct": atr,
                                "origin_run_id": run_id,  # a contract with history keeps its first (below)
                                "first_listed": trade_date.isoformat(),
                                "last_listed": trade_date.isoformat(),
                                "oi_at_listing": float(p.oi),  # a re-list keeps the FIRST listing's (below)
                                "prem_at_listing": premiums.get(p.security_id),
                            }
                        )
                    if not picks:
                        log.info(
                            "build_watchlists: %s %s@%s has no candidate strike",
                            a.symbol,
                            s.list_name,
                            s.expiry_date,
                        )
            except BrokerAuthError:
                raise
            except BrokerError as exc:
                log.warning("build_watchlists: %s chain failed: %s", a.symbol, exc)
                failed.append(a.symbol)
                continue
            validate_rows(sym_rows, trade_date, exps, threshold=threshold)
            new_rows.extend(sym_rows)

        retired_keys = {
            tuple(r)
            for r in conn.execute(
                f"SELECT {', '.join(WATCHLIST_KEY)} FROM watchlist WHERE trade_date = ? AND retired_run_id IS NOT NULL",
                (trade_date.isoformat(),),
            ).fetchall()
        }
        history = listing_history(conn, [int(r["security_id"]) for r in new_rows])
        for r in new_rows:  # schema v9: a contract listed before keeps its FIRST listing (date, run, build-up base)
            first = history.get(int(r["security_id"]))
            if first is None:
                continue  # a first listing: this pick is the base
            r["first_listed"] = min(first.first_listed, r["first_listed"])
            r["origin_run_id"] = first.origin_run_id or r["origin_run_id"]
            if first.oi_at_listing is not None:
                r["oi_at_listing"] = first.oi_at_listing
            if first.prem_at_listing is not None:
                r["prem_at_listing"] = first.prem_at_listing
        n_new, n_ignored = insert_watchlist_rows(conn, new_rows)
        if capture and new_rows:
            _capture_listing(conn, new_rows, chain_iv)
        n_unretired = sum(tuple(r[c] for c in WATCHLIST_KEY) in retired_keys for r in new_rows)
        if run_kind == RETIRE_KIND:  # net, also when the retire ran as its own step: a re-picked row is live
            n_retired = int(  # the S1.3.1 zone retire's rows; carry's own retirements are in summary['carry']
                conn.execute(
                    "SELECT COUNT(*) FROM watchlist WHERE retired_run_id = ? AND COALESCE(tag, '') <> ?",
                    (run_id, carry.CARRY_TAG),
                ).fetchone()[0]
            )
        per_list_new = dict(Counter(r["list_name"] for r in new_rows))
        per_list_total = {
            str(name): int(n)
            for name, n in conn.execute(
                "SELECT list_name, COUNT(*) FROM watchlist WHERE trade_date = ? AND retired_run_id IS NULL "
                "AND COALESCE(tag, '') <> ?" + edition.v1_lists_sql() + " GROUP BY list_name ORDER BY list_name",
                (trade_date.isoformat(), carry.CARRY_TAG),
            ).fetchall()
        }
        status = "ok" if not failed else "partial"
        note = f"kind={run_kind} levels_run={levels_run}"
        if run_kind == REFRESH_KIND:  # T-277: which 15-minute mark this refresh belongs to
            note += f" mark={mark_hm or _refresh_mark_of(conn, levels_run) or f'{started:%H%M}'}"
        elif v1r is not None and run_kind not in ("0910", "0920", "1315"):
            note += f" mark={mark_hm or f'{started:%H%M}'}"
        note += f" failed={','.join(failed)}" if failed else ""
        note += f" empty_chain={len(empty_chain)}" if empty_chain else ""  # R2: 0910 only
        if run_kind == RETIRE_KIND:
            note += f" retired={n_retired}" + (f" retire_unresolved={n_unresolved}" if n_unresolved else "")
        if carried is not None:
            note += f" carried={carried.carried}"
        if "carry_error" in summary:
            note += " carry=failed"
        if any(floor_stats.values()):
            note += " floor_unknown=" + ",".join(f"{k}:{v}" for k, v in floor_stats.items())
        summary.update(
            levels_run_id=levels_run,
            n_approaching_support=n_sup,
            n_approaching_resistance=n_res,
            n_expiry_calls=n_expiry_calls,
            n_chain_calls=len(chains),
            n_rows_new=n_new,
            n_rows_existing=before,
            n_rows_ignored=n_ignored,
            n_rows_unretired=n_unretired,
            n_retired=n_retired,
            n_retire_unresolved=n_unresolved,
            n_lists_skipped_existing=n_skipped,
            n_sides_skipped_existing=n_sides_skipped,  # T-277: refresh kinds only
            n_empty_chain=len(empty_chain),  # R2: 0910 stock-sides whose chain had no strike with OI > 0
            per_list_new=per_list_new,
            per_list_total=per_list_total,
            n_failed=len(failed),
            floor_stats=floor_stats,  # D161(5): strikes the floor passed on an unknown lot / previous-session value
            carry=None if carried is None else asdict(carried),
        )
        return summary
    except Exception as exc:
        note = f"kind={run_kind} error={type(exc).__name__}: {exc}"
        raise
    finally:
        summary["elapsed"] = round(time.perf_counter() - t0, 3)
        summary["status"] = status
        db.finish_run(conn, run_id, now_ist().isoformat(), status, note)
        log.info("build_watchlists %s: %s", run_id, summary)
        if own_conn:
            conn.close()
