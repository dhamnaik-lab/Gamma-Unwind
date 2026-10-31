"""Read-only trade results for the 15:35 GROUP day summary (T-178, D148; TRADE-TRACKER-SPEC §5 tables).

`trades_by_signal(conn, trade_date, kinds=...)` -> {entry signal id: the trades row + its `events` (trade_events rows,
id order)} for the trades OPENED on `trade_date`, the same rows GET /api/trades?date=&state=all answers
(TradeTracker.list_trades) -- the tracker is the only writer; this module only SELECTs. `kinds` = the edition's
visible signal kinds (the client tracker holds E1 trades only, D81/D86; passing ("E1",) is belt and braces).
A store without the tables (pre-v4) or any read error -> {} (logged): the summary then says "result not known yet"
for each entry (D144(5)), it never invents one.

T-267: V1-ONLY by default (`scanners=("V1",)` over trades.scanner, schema v10): a V2 Scanner (V2T) trade never counts in
V1's results / summary lines; its own section asks for scanners=("V2T",). A pre-v10 store has no scanner column: every
row there is a V1 trade.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Sequence
from typing import Any

from gammad.log import get_logger

log = get_logger(__name__)


def _dicts(cur: sqlite3.Cursor) -> list[dict[str, Any]]:
    """Rows as dicts whatever the connection's row_factory."""
    names = [d[0] for d in cur.description or ()]
    return [dict(zip(names, tuple(row), strict=True)) for row in cur.fetchall()]


def trades_by_signal(conn: sqlite3.Connection, trade_date: str, *, kinds: Sequence[str],
                     scanners: Sequence[str] = ("V1",)) -> dict[int, dict[str, Any]]:  # fmt: skip
    """The trades opened on `trade_date` whose kind is in `kinds` and scanner in `scanners` (V1 only by default),
    keyed by trades.entry_signal_id, each with its `events`. Read-only; {} on a missing table or a read error."""
    kinds = tuple(str(k) for k in kinds)
    scanners = tuple(str(s) for s in scanners)
    if not kinds or not scanners:
        return {}
    marks = ", ".join("?" for _ in kinds)
    try:
        has_scanner = any(str(r[1]) == "scanner" for r in conn.execute("PRAGMA table_info(trades)").fetchall())
        if has_scanner:
            where, args = f" AND scanner IN ({', '.join('?' for _ in scanners)})", scanners
        elif "V1" in scanners:  # pre-v10: every trade is a V1 trade
            where, args = "", ()
        else:
            return {}
        trades = _dicts(conn.execute(
            f"SELECT * FROM trades WHERE entry_date = ? AND kind IN ({marks}){where} ORDER BY entry_ts_ms, id",
            (trade_date, *kinds, *args)))  # fmt: skip
        out: dict[int, dict[str, Any]] = {}
        for t in trades:
            if t.get("entry_signal_id") is None:
                continue
            events = _dicts(conn.execute("SELECT * FROM trade_events WHERE trade_id = ? ORDER BY id", (t["id"],)))
            out[int(t["entry_signal_id"])] = {**t, "events": events}
        return out
    except sqlite3.Error as exc:
        log.warning("trade results for %s not read (%s: %s); the summary shows no result", trade_date,
                    type(exc).__name__, exc)  # fmt: skip
        return {}
