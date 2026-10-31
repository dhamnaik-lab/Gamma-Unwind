"""CX-209 / T-430 (c) / D634 (A): probe the client interpreter's SQLite.

From a client bundle's app folder run its interpreter with
``-m gammad.store.sqlite_caps --drill`` (Mac: python/bin/python3.13).
Only disposable stores are opened; startup wiring belongs to CX-206.
"""

import json
import platform
import re
import sqlite3
import sys
import tempfile
from dataclasses import asdict, dataclass
from pathlib import Path

from gammad.store import db

V10 = 10  # FROZEN: schema carried by a 0.8.8 client.


@dataclass(frozen=True)
class SqliteCaps:
    sqlite_version: str
    python_version: str
    setconfig: bool
    defensive_default: bool | None
    writable_schema_ok: bool
    widen_path: str


def probe(connect=sqlite3.connect) -> SqliteCaps:
    conn = connect(":memory:")
    try:
        defensive = None
        if hasattr(sqlite3, "SQLITE_DBCONFIG_DEFENSIVE") and hasattr(conn, "getconfig"):
            defensive = conn.getconfig(sqlite3.SQLITE_DBCONFIG_DEFENSIVE)
        sql = ("CREATE TABLE trades (id INTEGER PRIMARY KEY AUTOINCREMENT, "
               f"hold TEXT NOT NULL {db.TRADES_HOLD_CHECK_V10})")
        conn.execute(sql)
        conn.commit()
        ok = True
        try:
            db._rewrite_trades_sql_in_place(
                conn, sql, sql.replace(db.TRADES_HOLD_CHECK_V10, db.TRADES_HOLD_CHECK_V11)
            )
        except sqlite3.OperationalError as exc:
            if not db._schema_write_refused(exc):
                raise
            ok = False
        return SqliteCaps(sqlite3.sqlite_version, platform.python_version(), hasattr(conn, "setconfig"),
                          defensive, ok, "in place" if ok else "rebuild")
    finally:
        conn.close()


def v10_sql(schema_sql: str) -> str:
    sql = "\n".join(line for line in schema_sql.split("\n") if "-- v11" not in line and "-- v12" not in line)
    sql, comma_count = re.subn(r"(und_entry\s+REAL),(\s+-- v10)", r"\1\2", sql)
    check_count = sql.count(db.TRADES_HOLD_CHECK_V11)
    sql = sql.replace(db.TRADES_HOLD_CHECK_V11, db.TRADES_HOLD_CHECK_V10)
    sql, version_count = re.subn(r"PRAGMA user_version = \d+;", f"PRAGMA user_version = {V10};", sql)
    if (comma_count, check_count, version_count) != (1, 1, 1):
        raise ValueError("expected exactly one und_entry marker, hold CHECK and user_version")
    return sql


def _drill(folder: Path, defensive: bool) -> dict:
    mode = "defensive" if defensive else "default"
    conn = None
    try:
        conn = db.connect(folder / f"{mode}.db")
        conn.executescript(v10_sql(db.SCHEMA_PATH.read_text(encoding="utf-8")))
        conn.execute(
            "INSERT INTO trades (entry_signal_id, kind, hold, entry_date, entry_ts_ms, security_id, "
            "list_name, expiry_date, option_type, entry, t1, t2, state, spec_version, updated_ms) "
            "VALUES (1, 'E1', 'swing', '2026-10-01', 1, 70001, 'BREAKOUT-CE', '2026-10-28', "
            "'CE', 10, 12, 14, 'OPEN', 'S1.8.0', 1)"
        )
        conn.commit()
        root_sql = "SELECT rootpage FROM sqlite_master WHERE type = 'table' AND name = 'trades'"
        before = conn.execute(root_sql).fetchone()[0]
        if defensive:
            if not hasattr(conn, "setconfig") or not hasattr(sqlite3, "SQLITE_DBCONFIG_DEFENSIVE"):
                raise RuntimeError("DEFENSIVE configuration unavailable on this interpreter")
            conn.setconfig(sqlite3.SQLITE_DBCONFIG_DEFENSIVE, True)
        db.migrate(conn, backup=False)
        after = db.user_version(conn)
        rows = conn.execute("SELECT COUNT(*) FROM trades").fetchone()[0]
        check = conn.execute("PRAGMA quick_check").fetchone()[0]
        return {"mode": mode, "from": V10, "to": after,
                "path": "rebuild" if conn.execute(root_sql).fetchone()[0] != before else "in place",
                "rows": rows, "quick_check": check,
                "ok": after == db.SCHEMA_VERSION and rows == 1 and check == "ok"}
    except (RuntimeError, sqlite3.Error, ValueError, OSError) as exc:
        return {"mode": mode, "from": V10, "ok": False, "error": f"{type(exc).__name__}: {exc}"}
    finally:
        if conn is not None:
            conn.close()


def drill_all() -> dict:
    with tempfile.TemporaryDirectory() as folder:
        caps = asdict(probe())
        drills = [_drill(Path(folder), defensive) for defensive in (False, True)]
        return {"caps": caps, "drills": drills, "ok": all(drill["ok"] for drill in drills)}


def main(argv=None) -> int:
    args = sys.argv[1:] if argv is None else argv
    if args == []:
        result = asdict(probe())
        rc = 0
    elif args == ["--drill"]:
        result = drill_all()
        rc = 0 if result["ok"] else 1
    else:
        sys.stderr.write("usage: python -m gammad.store.sqlite_caps [--drill]\n")
        return 2
    sys.stdout.write(json.dumps(result, sort_keys=True) + "\n")
    return rc


if __name__ == "__main__":
    sys.exit(main())
