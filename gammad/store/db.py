"""SQLite store (WAL). `connect()` opens data/gamma.db; `migrate()` applies schema.sql idempotently."""

from __future__ import annotations

import re
import sqlite3
from collections.abc import Iterable, Mapping
from datetime import date
from pathlib import Path
from typing import Any

from gammad.log import get_logger

log = get_logger(__name__)

SCHEMA_PATH = Path(__file__).with_name("schema.sql")
# v2 adds mutes + manual_sends; v3 adds the S1.1.0 signal context columns + alerts.tg_message_id (D58/D63/D65);
# v4 adds trades + trade_events + watchlist.tick_size (S1.3.0, D80; docs/TRADE-TRACKER-SPEC.md §5), and
# closed_cycles (D98(4); added before v4 shipped: migrate() runs schema.sql on every start, whose CREATE ... IF NOT
# EXISTS gives a store already stamped 4 the new table without a version bump).
# v5 adds watchlist.chain_ltp (0.5.1, T-053): the option-chain LTP the list build picked the strike with, so a list
# post shows a premium before the contract's first live tick.
# v6 adds watchlist.chain_ltp_at + watchlist.retired_run_id (T-107 label time, T-063 / S1.3.1 pre-market retire)
# and closed_cycles.bar_ts_ms (T-107: a hidden entry is ordered by its bar). Rollback to 0.5.1: PRAGMA user_version=5;
# rolling back to 0.5.1 un-retires every retired row (0.5.1 reads no filter): it watches, posts and dedupes against
# every row a 09:20-kind build retired.
# ROLLBACK after a newer build migrated the store: the older build refuses a newer user_version (migrate()
# below) -> set `PRAGMA user_version=<old>`; the extra tables / columns are harmless (D69; TRACKER §5).
# 0.5.0 on a v5 store: `PRAGMA user_version=4`; its list build names its INSERT columns, so chain_ltp reads NULL.
# v7 (0.6.0, W15 / D114, PLAN-W15 §2) adds signals.scanner (NOT NULL DEFAULT 'V1') + signals.tier and the tables
# scanner_grades / scanner_daily (empty in a client store: harmless). Every row recorded before v7 reads scanner 'V1'
# although S1.2.0 / S1.3.1 rules produced it (its spec_version column says which) and tier NULL.
# Rollback to 0.5.2: `PRAGMA user_version=6`; 0.5.2 names its INSERT columns, so a new row takes the 'V1' default,
# and it reads every row whatever its scanner (the owner's shadow rows then show as ordinary signals).
# v8 (0.7.0, T-161 / D125 F5-F6, PLAN-W16 section 12 H6) adds trade_events.fill_px (the tick that crossed a stop; the
# event price and trades.exit_px are now the stop level) and trades.gate_status (pass | gated, the entry's gate
# verdict). Pre-v8 rows read NULL in both; pre-v8 exits stay as recorded (at the crossing tick), never rewritten.
# Rollback to 0.6.x: `PRAGMA user_version=7`; 0.6.x names its INSERT columns, so its rows read NULL there, and it
# records a stop exit at the crossing tick again.
# v9 (0.8.1, S1.5.0; docs/research/2026-09-30-workflow-0.8.1/CONTRACTS.md section 2) adds levels.atr_pct (daily ATR14
# as PERCENT of the last close, mark_levels.atr_pct) and the watchlist carry-forward columns (atr_pct, origin_run_id,
# first_listed, last_listed, carry_k, carry_state, carry_flags, oi_at_listing, prem_at_listing, carry_reason). Pre-v9
# rows read NULL in all of them; PK unchanged. Rollback to 0.8.0: `PRAGMA user_version=8`; 0.8.0 names its INSERT
# columns (upsert_levels / insert_watchlist_rows), so its rows read NULL there, and a CARRY row it finds is an ordinary
# live row to it.
# v10 (0.8.4, T-267 "V2 Scanner", docs/PLAN-T267.md section 1 "Schema v10", D249) is ADDITIVE: trades gains scanner
# (NOT NULL DEFAULT 'V1'), stop_top, stop_bottom, stop_confirmed_ms, und_entry (ALTER appends them; trades is never
# rebuilt, no PK changes), plus the tables trade_zone_steps (1h trail steps, UNIQUE (trade_id, seq)) and v2_regime
# (daily Supertrend regime, PK (trade_date, underlying_security_id)) and app_settings (D255: per-install switches,
# key 'v2_enabled' = the V2 Scanner ON / OFF, get_v2_enabled / set_v2_enabled). Pre-v10 trades read scanner 'V1'
# and NULL in the rest. A V2T trade lives in state Z_OPEN. Rollback to 0.8.3: `PRAGMA user_version=9`; 0.8.3 names its INSERT columns
# (tracker.TRADE_COLUMNS), so its rows read 'V1' / NULL there, and its tracker loads only state IN ('OPEN', 'T1_TRAIL'),
# so an open V2T (Z_OPEN) trade is invisible to it: no T1 at t1 = 0.0, no follow-up; the extra tables are harmless.
# v11 (0.8.9, T-419 "Still Watching", D471 / D472 / D480; T419-CONTRACT "Schema 10 -> 11"): watchlist gains
# oi_peak_prior (the OI peak of the sessions BEFORE the row's trade date; gammad/jobs/carry.py writes it); trades gains
# target_level / target_ms / trail_level / sw_bar_ms (a Still Watching trade's daily target, its first touch, the
# chandelier in force, the last 5-min stock bar it walked -- a restart re-walks every completed bar after it;
# the 1h stop zone reuses stop_top / stop_bottom / stop_confirmed_ms). trades.hold's CHECK widens to admit 'carry'
# (trades.hold of a Still Watching trade, scanner 'SW'): SQLite cannot ALTER a CHECK, so `_widen_trades_hold` rewrites
# the stored CREATE text in place (SQLite's documented writable_schema procedure for loosening a constraint; no row,
# page or id moves: trades is still never rebuilt).
# A SQLite build that refuses writable_schema (SQLITE_DBCONFIG_DEFENSIVE) gets the documented 12-step rebuild instead
# (_rebuild_trades: rows, ids and sqlite_sequence kept; T-430 (1)(c), CX-121).
# A Still Watching trade lives in states SW_OPEN / SW_TRAILING,
# which 0.8.8's tracker (OPEN / T1_TRAIL / Z_OPEN) never loads. Rollback to 0.8.8: `PRAGMA user_version=10`; 0.8.8
# names its INSERT columns, so its rows read NULL in the new ones, and the wider CHECK only admits more.
# v12 (0.8.9, D651 / A17) adds one table, ladder_state, for the OWN / GROUP exit ladders.
# Rollback: `PRAGMA user_version=11`; the extra table is harmless to the older build.
SCHEMA_VERSION = 12
# v11: the trades.hold CHECK before / after the widening (the exact text of schema.sql v4..v10 / v11)
TRADES_HOLD_CHECK_V10 = "CHECK (hold IN ('intraday', 'swing'))"
TRADES_HOLD_CHECK_V11 = "CHECK (hold IN ('intraday', 'swing', 'carry'))"
TRADES_REBUILD_TMP = "trades__v11"
BACKUP_DIR_NAME = "backups"  # <db dir>/backups/gamma-v<old>-pre-<app version>-<YYYYMMDD>.db (D69 precedent)

# Columns added to a table AFTER it was first created (schema.sql lists them too, for a fresh store).
# (table, column, declaration) -- ALTER TABLE ADD COLUMN cannot add NOT NULL without a default, so a column is
# either nullable (every row that predates it reads NULL, "recorded before v3") or NOT NULL WITH a constant
# default that every older row then reads (v7 signals.scanner = 'V1').
ADDED_COLUMNS: tuple[tuple[str, str, str], ...] = (
    ("signals", "hold", "TEXT"),
    ("signals", "gate_status", "TEXT"),
    ("signals", "gate_reasons", "TEXT"),
    ("signals", "ctx_json", "TEXT"),
    ("alerts", "tg_message_id", "INTEGER"),
    ("watchlist", "tick_size", "REAL"),  # v4 (S1.3.0): rupees; NULL = unknown (the tracker rounds on 0.01)
    ("watchlist", "chain_ltp", "REAL"),  # v5 (T-053): rupees at build; NULL = no chain price / pre-v5 row
    ("watchlist", "chain_ltp_at", "TEXT"),  # v6 (T-107): ISO IST of the chain fetch; NULL = label from run start
    ("watchlist", "retired_run_id", "TEXT"),  # v6 (T-063): retiring 09:20 run id; NULL = live row
    ("closed_cycles", "bar_ts_ms", "INTEGER"),  # v6 (T-107): hidden entry's bar open; NULL = pre-v6 (by ts_ms)
    ("signals", "scanner", "TEXT NOT NULL DEFAULT 'V1'"),  # v7 (W15): the producing scanner; pre-v7 rows = 'V1'
    ("signals", "tier", "TEXT"),  # v7 (W15): tier of an E1, and of a V1 E2 since 0.8.5 (D287 (2)); NULL = untiered (older row / shadow E2)
    ("trade_events", "fill_px", "REAL"),  # v8 (D125 F6): SL / TRAIL crossing tick; NULL = other kinds / pre-v8
    ("trades", "gate_status", "TEXT"),  # v8 (D125 F5): pass | gated; NULL = pre-v8 row
    ("levels", "atr_pct", "REAL"),  # v9 (S1.5.0): daily ATR14 / last close x 100, PERCENT; NULL = < 15 bars / pre-v9
    # v9 watchlist, in schema.sql order (ALTER appends, so a migrated store matches a fresh one column for column)
    ("watchlist", "atr_pct", "REAL"),  # the levels row's atr_pct at build, PERCENT
    ("watchlist", "origin_run_id", "TEXT"),  # run id of the FIRST listing
    ("watchlist", "first_listed", "TEXT"),  # YYYY-MM-DD
    ("watchlist", "last_listed", "TEXT"),  # YYYY-MM-DD; a re-list resets it
    ("watchlist", "carry_k", "INTEGER"),  # sessions carried; NULL = not carried
    ("watchlist", "carry_state", "TEXT"),  # WATCH | BUILD-UP | PRIMED
    ("watchlist", "carry_flags", "TEXT"),  # 'broken' or NULL
    ("watchlist", "oi_at_listing", "REAL"),  # OI (contracts) at the first listing
    ("watchlist", "prem_at_listing", "REAL"),  # premium (rupees) at the first listing
    ("watchlist", "carry_reason", "TEXT"),  # why; retired dead | spent | away | deep_itm | expiry | capacity | floor
    # v10 trades (T-267), in schema.sql order (ALTER appends, so a migrated store matches a fresh one)
    ("trades", "scanner", "TEXT NOT NULL DEFAULT 'V1'"),  # V1 | V2T; pre-v10 rows = 'V1'
    ("trades", "stop_top", "REAL"),  # V2T 1h stop zone top, underlying rupees; NULL = V1 / NO_ZONE
    ("trades", "stop_bottom", "REAL"),  # V2T 1h stop zone bottom; NULL = V1 / NO_ZONE
    ("trades", "stop_confirmed_ms", "INTEGER"),  # when the stop zone in force was confirmed; NULL = V1 / NO_ZONE
    ("trades", "und_entry", "REAL"),  # underlying price at entry; NULL = V1 / pre-v10
    # v11 (T-419 Still Watching), in schema.sql order
    ("watchlist", "oi_peak_prior", "REAL"),  # OI peak of the sessions before trade_date; NULL = unknown / pre-v11
    ("trades", "target_level", "REAL"),  # SW daily target (underlying rupees); NULL = none / not SW
    ("trades", "target_ms", "INTEGER"),  # SW: the close of the bar that first touched the target; NULL = not yet
    ("trades", "trail_level", "REAL"),  # SW: the chandelier in force (underlying rupees); NULL = not trailing
    ("trades", "sw_bar_ms", "INTEGER"),  # SW: open of the last 5-min stock bar walked (restart re-walk); NULL = none
)

# v10 (T-267): the column lists the zone-step / regime helpers below read and write, in schema.sql order.
ZONE_STEP_COLUMNS: tuple[str, ...] = (
    "trade_id",
    "seq",
    "confirmed_ms",
    "applied_ms",
    "top",
    "bottom",
    "h1_close",
    "status",
    "tg_message_id",
    "reply_to",
    "error",
)
V2_REGIME_COLUMNS: tuple[str, ...] = (
    "trade_date",
    "underlying_security_id",
    "symbol",
    "st_dir",
    "st_value",
    "asof_date",
    "bars_n",
    "reason",
    "computed_ms",
)

LEVEL_COLUMNS: tuple[str, ...] = (
    "trade_date",
    "run_id",
    "symbol",
    "security_id",
    "support_lo",
    "support_hi",
    "resistance_lo",
    "resistance_hi",
    "ref_price",
    "dist_support_pct",
    "dist_resistance_pct",
    "zones_json",
    "spec_version",
    "atr_pct",  # v9
)


def connect(path: Path | str | None = None) -> sqlite3.Connection:
    if path is None:
        from gammad.config import settings

        path = settings().paths.db_path
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, timeout=30.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def user_version(conn: sqlite3.Connection) -> int:
    return int(conn.execute("PRAGMA user_version").fetchone()[0])


def table_columns(conn: sqlite3.Connection, table: str) -> list[str]:
    return [str(r[1]) for r in conn.execute(f"PRAGMA table_info({table})").fetchall()]


def _add_missing_columns(conn: sqlite3.Connection) -> list[str]:
    """ALTER an existing older table to gain the ADDED_COLUMNS it lacks. Tables that do not exist yet are
    skipped (schema.sql creates them whole). Idempotent; existing rows keep every value and read NULL in
    the new columns."""
    added: list[str] = []
    for table, column, decl in ADDED_COLUMNS:
        cols = table_columns(conn, table)
        if cols and column not in cols:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {decl}")
            added.append(f"{table}.{column}")
    return added


def _table_sql(conn: sqlite3.Connection, table: str) -> str | None:
    got = conn.execute("SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?", (table,)).fetchone()
    return None if got is None else str(got[0])


def _rewrite_trades_sql_in_place(conn: sqlite3.Connection, sql: str, new_sql: str) -> None:
    try:
        conn.execute("BEGIN IMMEDIATE")
        version = int(conn.execute("PRAGMA schema_version").fetchone()[0])
        conn.execute("PRAGMA writable_schema = ON")
        cur = conn.execute("UPDATE sqlite_master SET sql = ? WHERE type = 'table' AND name = 'trades' AND sql = ?",
                           (new_sql, sql))  # fmt: skip
        if cur.rowcount != 1:
            raise RuntimeError("store: trades schema row changed under the v11 widening")
        conn.execute(f"PRAGMA schema_version = {version + 1}")
        conn.execute("PRAGMA writable_schema = OFF")
        conn.commit()
    except BaseException:
        conn.rollback()
        conn.execute("PRAGMA writable_schema = OFF")
        raise


def _schema_write_refused(exc: sqlite3.OperationalError) -> bool:
    """SQLITE_DBCONFIG_DEFENSIVE's measured refusal: 'table sqlite_master may not be modified'."""
    return "may not be modified" in str(exc)


def _copy_trades_sequence(conn: sqlite3.Connection, seq: int | None) -> None:
    if seq is None:
        return
    cur = conn.execute("UPDATE sqlite_sequence SET seq = ? WHERE name = 'trades'", (seq,))
    if cur.rowcount == 0:
        conn.execute("INSERT INTO sqlite_sequence (name, seq) VALUES ('trades', ?)", (seq,))


def _rebuild_trades(conn: sqlite3.Connection, new_sql: str) -> None:
    """SQLite's 12-step rebuild (lang_altertable.html section 7), preserving rows, extras and spent ids."""
    if _table_sql(conn, TRADES_REBUILD_TMP) is not None:
        raise RuntimeError("store: trades__v11 already exists; refusing the trades rebuild")
    tmp_sql, replaced = re.subn(r'^CREATE TABLE\s+"?trades"?\s*\(',
                               f"CREATE TABLE {TRADES_REBUILD_TMP} (", new_sql, count=1)
    if replaced != 1:
        raise RuntimeError("store: unexpected trades CREATE text; refusing the rebuild")
    cols = table_columns(conn, "trades")
    row = conn.execute("SELECT seq FROM sqlite_sequence WHERE name = 'trades'").fetchone()
    seq = None if row is None else int(row[0])
    extras = conn.execute("SELECT sql FROM sqlite_master WHERE tbl_name = 'trades'"
                          " AND type IN ('index', 'trigger') AND sql IS NOT NULL ORDER BY name").fetchall()
    n = conn.execute("SELECT COUNT(*) FROM trades").fetchone()[0]
    fk = int(conn.execute("PRAGMA foreign_keys").fetchone()[0])
    conn.commit()
    conn.execute("PRAGMA foreign_keys=OFF")
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(tmp_sql)
        names = ", ".join('"' + col.replace('"', '""') + '"' for col in cols)
        conn.execute(f"INSERT INTO {TRADES_REBUILD_TMP} ({names}) SELECT {names} FROM trades")
        conn.execute("DROP TABLE trades")
        conn.execute(f"ALTER TABLE {TRADES_REBUILD_TMP} RENAME TO trades")
        for extra in extras:
            conn.execute(extra[0])
        _copy_trades_sequence(conn, seq)
        if conn.execute("SELECT COUNT(*) FROM trades").fetchone()[0] != n:
            raise RuntimeError("store: trades row count changed during rebuild")
        if conn.execute("PRAGMA foreign_key_check").fetchall():
            raise RuntimeError("store: foreign_key_check failed during trades rebuild")
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    finally:
        conn.execute(f"PRAGMA foreign_keys={'ON' if fk else 'OFF'}")


def _widen_trades_hold(conn: sqlite3.Connection) -> bool:
    """v11: widen an existing trades table's hold CHECK (intraday / swing) to admit 'carry' too, IN PLACE: SQLite's
    documented procedure for loosening a constraint (https://sqlite.org/lang_altertable.html section 7, the "simpler
    procedure ... for removing CHECK ... constraints"): in ONE transaction, read schema_version, writable_schema ON,
    UPDATE the stored CREATE text with only the CHECK replaced, schema_version + 1, writable_schema OFF; then a
    quick_check. Every existing row satisfies the wider CHECK (it only admits more), so no data page, root page, id or
    sqlite_sequence moves -- trades is never rebuilt (the v10 invariant holds). Idempotent: a table already widened (or
    absent: schema.sql creates it whole) is left alone; an unexpected CHECK text is refused, never guessed. Returns True
    when it widened.

    The measured SQLITE_DBCONFIG_DEFENSIVE refusal ('may not be modified') falls back to the documented 12-step
    rebuild: no row, id or sequence value moves; the root page does. A failed rebuild rolls back the trades table.
    """
    sql = _table_sql(conn, "trades")
    if sql is None or TRADES_HOLD_CHECK_V11 in sql:
        return False
    if sql.count(TRADES_HOLD_CHECK_V10) != 1:
        raise RuntimeError("store: trades.hold CHECK is not the v4..v10 text; refusing to widen it")
    new_sql = sql.replace(TRADES_HOLD_CHECK_V10, TRADES_HOLD_CHECK_V11, 1)
    conn.commit()
    path = "in place"
    try:
        _rewrite_trades_sql_in_place(conn, sql, new_sql)
    except sqlite3.OperationalError as exc:
        if not _schema_write_refused(exc):
            raise
        log.error("store: this SQLite build (%s) refuses the in-place trades.hold widening "
                  "(%s: a DEFENSIVE build); rebuilding the trades table instead", sqlite3.sqlite_version, exc)
        try:
            _rebuild_trades(conn, new_sql)
        except Exception as exc2:
            db_file = _db_file(conn)
            backups = db_file.parent / BACKUP_DIR_NAME if db_file else "<db dir>/backups/"
            raise RuntimeError(
                f"store: cannot widen trades.hold for schema 11 on SQLite {sqlite3.sqlite_version}; "
                f"the in-place rewrite was refused ({exc}); the trades rebuild failed ({exc2}); "
                f"the trades table is unchanged (user_version {user_version(conn)}; its v11 columns are already added); "
                f"restore from {backups} or run the previous version"
            ) from exc2
        path = "by rebuild"
    check = [str(r[0]) for r in conn.execute("PRAGMA quick_check").fetchall()]
    if check != ["ok"]:
        raise RuntimeError(f"store: quick_check after the v11 trades.hold widening: {check[:5]}")
    log.info("store: trades.hold CHECK widened %s to %s (v11)", path, TRADES_HOLD_CHECK_V11)
    return True


def _db_file(conn: sqlite3.Connection) -> Path | None:
    """The file behind the connection's main database; None for an in-memory / temporary store."""
    for row in conn.execute("PRAGMA database_list").fetchall():
        if str(row[1]) == "main":
            return Path(str(row[2])) if row[2] else None
    return None


def backup_path(db_file: Path, old_version: int, app_version: str, day: date) -> Path:
    """`<db dir>/backups/gamma-v<old>-pre-<app>-<YYYYMMDD>.db` -- the name the v2 -> v3 backup used on
    2026-09-25 (`data/backups/gamma-v2-pre-0.3.0-20260925.db`, D69)."""
    return db_file.parent / BACKUP_DIR_NAME / f"gamma-v{old_version}-pre-{app_version}-{day:%Y%m%d}.db"


def backup_before_migrate(
    conn: sqlite3.Connection, *, app_version: str | None = None, day: date | None = None
) -> Path | None:
    """Copy an OLDER, file-backed, non-empty store to backup_path() before migrate() touches it (D69; TRACKER §5).

    Uses the SQLite online-backup API (consistent with WAL; the live store is 30+ MB, seconds at most). Never
    overwrites: a second attempt the same day (e.g. after a crash half-way through the migration) keeps the
    FIRST copy, the true pre-migration state. Returns the backup path, or None when there is nothing to back
    up (in-memory store, a brand-new empty file, or already at SCHEMA_VERSION)."""
    before = user_version(conn)
    src = _db_file(conn)
    if src is None or before >= SCHEMA_VERSION:
        return None
    has_tables = conn.execute("SELECT COUNT(*) FROM sqlite_master WHERE type = 'table'").fetchone()[0]
    if not has_tables:
        return None
    if app_version is None:
        from gammad import __version__ as app_version
    if day is None:
        from gammad.config import now_ist

        day = now_ist().date()
    dest = backup_path(src, before, app_version, day)
    if dest.exists():
        log.info("store: pre-migration backup %s already exists; kept", dest)
        return dest
    dest.parent.mkdir(parents=True, exist_ok=True)
    out = sqlite3.connect(dest)
    try:
        conn.backup(out)
    finally:
        out.close()
    log.info("store: pre-migration backup of user_version %d -> %s", before, dest)
    return dest


def migrate(conn: sqlite3.Connection, *, backup: bool = True) -> int:
    """Bring the store to SCHEMA_VERSION in place. Idempotent: re-running is a no-op.

    An OLDER file-backed store is first copied to `<db dir>/backups/` (backup_before_migrate; a failed copy
    is logged at ERROR and the migration goes ahead: it is additive, and the rollback is `PRAGMA
    user_version=<old>`, which needs no copy). Order then matters: first ALTER the tables an older store
    already has (ADDED_COLUMNS), THEN run schema.sql, whose statements are all IF NOT EXISTS (new tables and
    indexes; an older store gains mutes/manual_sends (v2) and trades/trade_events (v4)) and whose LAST
    statement stamps `PRAGMA user_version`. A crash part-way leaves the old version number, so the next start
    simply finishes the job."""
    before = user_version(conn)
    if before > SCHEMA_VERSION:
        raise RuntimeError(f"database user_version {before} is newer than this code ({SCHEMA_VERSION})")
    if backup and before < SCHEMA_VERSION:
        try:
            backup_before_migrate(conn)
        except (OSError, sqlite3.Error) as exc:
            log.error("store: pre-migration backup FAILED (%s: %s); migrating without a copy", type(exc).__name__, exc)
    added = _add_missing_columns(conn)
    conn.commit()
    if added:
        log.info("store: added columns %s", ", ".join(added))
    _widen_trades_hold(conn)  # v11: after the ALTERs, before schema.sql (in place; a DEFENSIVE build: _rebuild_trades)
    conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
    conn.commit()
    after = user_version(conn)
    if after != before:
        log.info("store migrated user_version %d -> %d", before, after)
    return after


def insert_run(
    conn: sqlite3.Connection,
    run_id: str,
    kind: str,
    started_ist: str,
    spec_version: str,
    status: str = "running",
    note: str | None = None,
) -> None:
    conn.execute(
        "INSERT INTO runs (run_id, kind, started_ist, finished_ist, spec_version, status, note) "
        "VALUES (?, ?, ?, NULL, ?, ?, ?)",
        (run_id, kind, started_ist, spec_version, status, note),
    )
    conn.commit()


def finish_run(
    conn: sqlite3.Connection, run_id: str, finished_ist: str, status: str, note: str | None = None
) -> None:
    conn.execute(
        "UPDATE runs SET finished_ist = ?, status = ?, note = COALESCE(?, note) WHERE run_id = ?",
        (finished_ist, status, note, run_id),
    )
    conn.commit()


def upsert_levels(conn: sqlite3.Connection, rows: Iterable[Mapping[str, Any]]) -> int:
    cols = ", ".join(LEVEL_COLUMNS)
    marks = ", ".join("?" for _ in LEVEL_COLUMNS)
    updates = ", ".join(f"{c} = excluded.{c}" for c in LEVEL_COLUMNS[3:])
    sql = (
        f"INSERT INTO levels ({cols}) VALUES ({marks}) "
        f"ON CONFLICT (trade_date, run_id, symbol) DO UPDATE SET {updates}"
    )
    data = [tuple(r.get(c) for c in LEVEL_COLUMNS) for r in rows]
    conn.executemany(sql, data)
    conn.commit()
    return len(data)


def fetch_levels(conn: sqlite3.Connection, trade_date: str, run_id: str | None = None) -> list[dict[str, Any]]:
    """Levels for a date. With no run_id, the rows of the LATEST run that wrote that date."""
    if run_id is None:
        row = conn.execute(
            "SELECT l.run_id FROM levels l LEFT JOIN runs r ON r.run_id = l.run_id "
            "WHERE l.trade_date = ? ORDER BY COALESCE(r.started_ist, '') DESC, l.run_id DESC LIMIT 1",
            (trade_date,),
        ).fetchone()
        if row is None:
            return []
        run_id = str(row[0])
    cur = conn.execute(
        "SELECT * FROM levels WHERE trade_date = ? AND run_id = ? ORDER BY symbol", (trade_date, run_id)
    )
    return [dict(r) for r in cur.fetchall()]


# ------------------------------------------------------------------ v10 (T-267): V2T trail steps + daily regime
_UNSET: Any = object()
_IN_CHUNK = 500  # ids per `IN (...)` (well under SQLite's bound-variable limit)


def _row_dict(row: sqlite3.Row | tuple[Any, ...], cols: tuple[str, ...]) -> dict[str, Any]:
    return dict(zip(cols, tuple(row), strict=True))


def insert_zone_step(
    conn: sqlite3.Connection,
    trade_id: int,
    *,
    confirmed_ms: int,
    top: float,
    bottom: float,
    h1_close: float | None = None,
    applied_ms: int | None = None,
    status: str = "pending",
    reply_to: int | None = None,
    error: str | None = None,
    seq: int | None = None,
    commit: bool = True,
) -> int:
    """Append one 1h trail step of a V2T trade to trade_zone_steps; returns its seq.

    seq=None takes the trade's next seq (1 for its first step), computed inside the INSERT. An explicit seq that
    already exists for the trade raises sqlite3.IntegrityError (UNIQUE (trade_id, seq)): a replay after a crash cannot
    write the same step twice. commit=False leaves the row in the caller's open transaction, so the tracker can write
    the step and move trades.stop_* atomically (TRACKER section 6: recorded 'pending' BEFORE any send)."""
    cur = conn.execute(
        "INSERT INTO trade_zone_steps (trade_id, seq, confirmed_ms, applied_ms, top, bottom, h1_close, status,"
        " tg_message_id, reply_to, error) VALUES (?, COALESCE(?, (SELECT COALESCE(MAX(seq), 0) + 1"
        " FROM trade_zone_steps WHERE trade_id = ?)), ?, ?, ?, ?, ?, ?, NULL, ?, ?)",
        (
            int(trade_id), seq, int(trade_id), int(confirmed_ms), applied_ms, float(top), float(bottom), h1_close,
            status, reply_to, error,
        ),
    )
    got = conn.execute("SELECT seq FROM trade_zone_steps WHERE rowid = ?", (cur.lastrowid,)).fetchone()
    if commit:
        conn.commit()
    return int(got[0])


def update_zone_step(
    conn: sqlite3.Connection,
    trade_id: int,
    seq: int,
    *,
    status: str = _UNSET,
    applied_ms: int | None = _UNSET,
    tg_message_id: int | None = _UNSET,
    error: str | None = _UNSET,
    reply_to: int | None = _UNSET,
    commit: bool = True,
) -> bool:
    """Set the given fields of one step (only those passed; None is written as NULL). True when the row exists.
    reply_to (T-275(2)) = the message id the step was actually sent as a reply to (router.send_zone_step's 4th value)."""
    sets = {k: v for k, v in (("status", status), ("applied_ms", applied_ms), ("tg_message_id", tg_message_id),
                              ("error", error), ("reply_to", reply_to)) if v is not _UNSET}  # fmt: skip
    if not sets:
        raise ValueError("update_zone_step: no field to set")
    cur = conn.execute(
        f"UPDATE trade_zone_steps SET {', '.join(f'{k} = ?' for k in sets)} WHERE trade_id = ? AND seq = ?",
        (*sets.values(), int(trade_id), int(seq)),
    )
    if commit:
        conn.commit()
    return cur.rowcount > 0


def list_zone_steps(conn: sqlite3.Connection, trade_id: int) -> list[dict[str, Any]]:
    """Every trail step of one trade, oldest first (seq order); [] for a trade without steps."""
    cur = conn.execute(
        f"SELECT {', '.join(ZONE_STEP_COLUMNS)} FROM trade_zone_steps WHERE trade_id = ? ORDER BY seq",
        (int(trade_id),),
    )
    return [_row_dict(r, ZONE_STEP_COLUMNS) for r in cur.fetchall()]


def zone_steps_by_trade(conn: sqlite3.Connection, trade_ids: Iterable[int]) -> dict[int, list[dict[str, Any]]]:
    """{trade_id: steps in seq order} for many trades at once (the trade view); every asked id is a key, [] if none."""
    ids = sorted({int(t) for t in trade_ids})
    out: dict[int, list[dict[str, Any]]] = {t: [] for t in ids}
    for i in range(0, len(ids), _IN_CHUNK):
        chunk = ids[i : i + _IN_CHUNK]
        cur = conn.execute(
            f"SELECT {', '.join(ZONE_STEP_COLUMNS)} FROM trade_zone_steps"
            f" WHERE trade_id IN ({', '.join('?' for _ in chunk)}) ORDER BY trade_id, seq",
            chunk,
        )
        for r in cur.fetchall():
            d = _row_dict(r, ZONE_STEP_COLUMNS)
            out[int(d["trade_id"])].append(d)
    return out


def upsert_v2_regimes(conn: sqlite3.Connection, rows: Iterable[Mapping[str, Any]], *, commit: bool = True) -> int:
    """Insert or replace v2_regime rows keyed (trade_date, underlying_security_id); a missing key reads NULL.
    st_dir must be +1, -1 or None (None = unknown = no V2T entry, fails closed); anything else raises ValueError
    before any row is written. computed_ms is NOT NULL (sqlite3.IntegrityError without it). Returns rows written."""
    data: list[tuple[Any, ...]] = []
    for r in rows:
        st = r.get("st_dir")
        if st is not None and (isinstance(st, bool) or st not in (1, -1)):
            raise ValueError(f"v2_regime st_dir must be +1, -1 or None, got {st!r} ({r.get('symbol')})")
        data.append(tuple(int(st) if c == "st_dir" and st is not None else r.get(c) for c in V2_REGIME_COLUMNS))
    cols = ", ".join(V2_REGIME_COLUMNS)
    updates = ", ".join(f"{c} = excluded.{c}" for c in V2_REGIME_COLUMNS[2:])
    conn.executemany(
        f"INSERT INTO v2_regime ({cols}) VALUES ({', '.join('?' for _ in V2_REGIME_COLUMNS)}) "
        f"ON CONFLICT (trade_date, underlying_security_id) DO UPDATE SET {updates}",
        data,
    )
    if commit:
        conn.commit()
    return len(data)


def load_v2_regimes(conn: sqlite3.Connection, trade_date: str) -> dict[int, dict[str, Any]]:
    """{underlying_security_id: row} for one trade_date; {} when none were computed (= every V2T entry fails closed)."""
    cur = conn.execute(
        f"SELECT {', '.join(V2_REGIME_COLUMNS)} FROM v2_regime WHERE trade_date = ? ORDER BY underlying_security_id",
        (trade_date,),
    )
    return {int(d["underlying_security_id"]): d for d in (_row_dict(r, V2_REGIME_COLUMNS) for r in cur.fetchall())}


# ---------------------------------------------------------------- D255: the per-install V2 Scanner ON / OFF switch
V2_ENABLED_KEY = "v2_enabled"  # app_settings.key; value '1' = ON, '0' = OFF; no row = v2_enabled_default(owner)


def v1_shadowed_e1_sql(entry_kind: str, col: str = "signal_id") -> tuple[str, tuple[str, str]]:
    """0.8.5 (D288 Q6, PLAN-D287 I3): ONE V1 entry per contract-day in the scanner grades. A SQL predicate (+ its two
    parameters) that is TRUE when `col` (a signals.id, e.g. scanner_grades.signal_id) is a V1 E1 whose contract-day
    also holds a GRADED V1 row of `entry_kind` (= `edition.client_entry_kind()`): in the OWNER store that E1 is the
    owner's personal entry shadowed by the group entry (the tiered V1 E2, graded too by app._grade_released, AM2), so a
    V1 total reads `AND NOT <predicate>`. With entry_kind "E1" (the client store) it is never true. A pre-0.8.5 day (no
    graded V1 E2) keeps its E1."""
    sql = (
        f"({col} IN (SELECT e.id FROM signals e JOIN signals d ON d.trade_date = e.trade_date"
        " AND d.security_id = e.security_id AND d.scanner = 'V1' AND d.kind = ?"
        " JOIN scanner_grades gd ON gd.signal_id = d.id"
        " WHERE e.scanner = 'V1' AND e.kind = 'E1' AND ? <> 'E1'))"
    )
    return sql, (entry_kind, entry_kind)


def v2_enabled_default(*, owner: bool) -> bool:
    """D255(1): CLIENT default OFF (0.8.5, D287(6): and never ON, `get_v2_enabled`); OWNER default ON (the D246 live
    owner shadow from the first trading day)."""
    return bool(owner)


def get_v2_enabled(conn: sqlite3.Connection, *, owner: bool) -> bool:
    """Is the V2 Scanner ON for this install? The stored app_settings value when set ('1' / '0'), else the edition's
    default (v2_enabled_default). A store without the table (never migrated to v10) or an unreadable value reads the
    default too. The router gates NEW V2T entries on this (D255(2)); open V2T trades never read it.
    0.8.5 (D286(4), D287(6), PLAN-D287 I7): the CLIENT edition has no V2 Scanner at all: `owner=False` is always False,
    whatever a 0.8.4 client store holds (the store is never read)."""
    if not owner:
        return False
    try:
        row = conn.execute("SELECT value FROM app_settings WHERE key = ?", (V2_ENABLED_KEY,)).fetchone()
    except sqlite3.OperationalError as exc:
        if "no such table" not in str(exc):
            raise
        row = None
    value = None if row is None else str(row[0])
    if value in ("1", "0"):
        return value == "1"
    return v2_enabled_default(owner=owner)


def set_v2_enabled(conn: sqlite3.Connection, enabled: bool, *, source: str, now_ms: int | None = None) -> bool:
    """Store the V2 Scanner switch (D255(3): persisted, and every change logged with its source) and commit. Returns
    the stored value. Raises sqlite3.OperationalError on a store without app_settings (migrate() creates it)."""
    import time

    on = bool(enabled)
    ms = int(time.time() * 1000) if now_ms is None else int(now_ms)
    conn.execute(
        "INSERT INTO app_settings (key, value, updated_ms, source) VALUES (?, ?, ?, ?) ON CONFLICT (key) DO UPDATE SET"
        " value = excluded.value, updated_ms = excluded.updated_ms, source = excluded.source",
        (V2_ENABLED_KEY, "1" if on else "0", ms, str(source)),
    )
    conn.commit()
    log.info("V2 Scanner switch set %s (source %s)", "ON" if on else "OFF", source)
    return on
