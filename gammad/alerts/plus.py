"""V1 PLUS, derived from the store (0.8.1, spec S1.5.0; D159(2), D160(1), D161(1)(4); CONTRACTS.md section 5).

V1 PLUS has no engine (engine_pool.DERIVED). Its book is exactly what the journal measured
(docs/research/2026-09-29-journal/journal.py:98-146):

    first_per_stock(V1 ENTRY + zone-confirmed V1 B at bar turnover >= Rs 25 lakh)

i.e. ONE V1 PLUS row per (trade_date, underlying symbol): the earliest origin by signal time, V1 before V1 B on a time
tie (a stable sort of `v1 + v1b`, journal.py:135), and nothing whose IST time is at or after 15:20 (journal.py:139).

`derive_plus(conn, trade_date, symbol, sig_row)` is called by the router right after it INSERTs an origin signal row,
with NO await between this check and its INSERT (two routing tasks for one stock cannot both derive). It is
store-derived, so it is restart-safe like router.has_entry_today: a PLUS row already in the store for the stock-day
decides, whatever this process remembers.

Origins (`sig_row["id"]` = the origin's signals.id; everything else is read back from the store):
  V1    an E1 whose stored tier is ENTRY (an R4-demoted row is WATCHLIST + dup_stock: never), not carried.
  V1 B  an E1 the caller found to be a PLUS component (`sig_row["plus"] is True`, gates.plus_candidate: it needs the
        V1BCfg / GatesCfg this module does not hold), not carried.
Carried = ctx_json.carried, payload carried, or payload tag 'CARRY' (D157(2): carry never enters a book).

The PLUS row is a copy of its origin row (contract, price, stop, time, context) with scanner 'V1 PLUS', tier ENTRY and
payload keys plus_origin ('V1' | 'V1 B') + plus_origin_id. If an EARLIER-ranked origin arrives after the PLUS row was
written (only on a time tie routed V1 B first, or out-of-order routing), the PLUS row is re-pointed to it in place (the
same id: "one row per stock-day" holds) and its id is returned again.

Owner edition only: the router never calls this in the client edition (clients get V1 only in 0.8.1, D159(1)).
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Mapping
from datetime import datetime
from typing import Any

from gammad.config import IST
from gammad.log import get_logger

log = get_logger(__name__)

SCANNER_V1 = "V1"
SCANNER_V1B = "V1 B"
SCANNER_PLUS = "V1 PLUS"
ORIGIN_RANK: dict[str, int] = {SCANNER_V1: 0, SCANNER_V1B: 1}  # V1 first on a time tie (journal.py:135)
TIER_ENTRY = "ENTRY"
DUP_STOCK = "dup_stock"
TAG_CARRY = "CARRY"
LAST_BAR_CLOSE_DEFAULT = "15:20"  # V1BCfg.last_bar_close

# the signals columns a PLUS row copies from its origin (id, scanner, tier and payload_json are set here)
_COPY_COLS: tuple[str, ...] = (
    "trade_date", "ts_ms", "security_id", "list_name", "expiry_date", "kind", "strong", "ltp", "oi_drop_pct",
    "vol_ratio", "zone_dist_pct", "sl", "latency_ms", "spec_version", "hold", "gate_status", "gate_reasons",
    "ctx_json",
)  # fmt: skip


def _hhmm(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000, IST).strftime("%H:%M")


def _json(raw: Any) -> dict[str, Any]:
    if not raw:
        return {}
    try:
        d = json.loads(str(raw))
    except ValueError:
        return {}
    return d if isinstance(d, dict) else {}


def _carried(payload: Mapping[str, Any], ctx: Mapping[str, Any]) -> bool:
    return bool(ctx.get("carried")) or bool(payload.get("carried")) or payload.get("tag") == TAG_CARRY


def _eligible(row: Mapping[str, Any], sig_row: Mapping[str, Any], last_bar_close: str) -> bool:
    """Is the stored origin row a V1 PLUS source (module docstring)?"""
    scanner = str(row["scanner"] or "")
    if scanner not in ORIGIN_RANK or str(row["kind"]) != "E1":
        return False
    if _hhmm(int(row["ts_ms"])) >= last_bar_close:
        return False
    payload, ctx = _json(row["payload_json"]), _json(row["ctx_json"])
    if _carried(payload, ctx):
        return False
    if DUP_STOCK in str(row["gate_reasons"] or "").split(","):
        return False
    if scanner == SCANNER_V1:
        return str(row["tier"] or "") == TIER_ENTRY and sig_row.get("plus", True) is not False
    return sig_row.get("plus") is True


def _key(row: Mapping[str, Any], origin: str) -> tuple[int, int]:
    return int(row["ts_ms"]), ORIGIN_RANK[origin]


def derive_plus(
    conn: sqlite3.Connection,
    trade_date: str,
    symbol: str,
    sig_row: Mapping[str, Any],
    *,
    last_bar_close: str = LAST_BAR_CLOSE_DEFAULT,
    repoint_sent: bool = True,
) -> int | None:
    """The V1 PLUS row id this origin signal makes (new, or re-pointed to it), or None (not a source, or the stock-day
    already has an earlier-ranked PLUS). Synchronous: no await anywhere, so the check and the INSERT are atomic for
    the asyncio router. Commits. `repoint_sent=False` (T-310 review L8: a V1 signal held for the 09:20 list post and
    released after a V1 B PLUS-only card went out) = a PLUS row with any `alerts` row (its card was routed) is never
    re-pointed: the store keeps the trade the owner was sent."""
    origin_id = sig_row.get("id")
    if origin_id is None:
        return None
    cur = conn.cursor()
    cur.row_factory = sqlite3.Row  # this cursor only: the caller's connection is left as it is
    found = cur.execute("SELECT * FROM signals WHERE id = ?", (int(origin_id),)).fetchone()
    if found is None:
        return None
    row = dict(found)
    if str(row["trade_date"]) != trade_date:
        return None
    if not _eligible(row, sig_row, last_bar_close):
        return None
    origin = str(row["scanner"])
    payload = _json(row["payload_json"])
    if str(payload.get("symbol") or "") != symbol:
        log.warning("V1 PLUS: signal %s is %r, not %r; not derived", origin_id, payload.get("symbol"), symbol)
        return None
    new_payload = {**payload, "plus_origin": origin, "plus_origin_id": int(origin_id)}
    values = [row[c] for c in _COPY_COLS]
    existing = conn.execute(
        "SELECT id, ts_ms, payload_json FROM signals WHERE trade_date = ? AND scanner = ?"
        " AND json_extract(payload_json, '$.symbol') = ? ORDER BY id LIMIT 1",
        (trade_date, SCANNER_PLUS, symbol),
    ).fetchone()
    if existing is not None:
        ex_origin = str(_json(existing[2]).get("plus_origin") or SCANNER_V1)
        ex_key = (int(existing[1]), ORIGIN_RANK.get(ex_origin, 0))
        if _key(row, origin) >= ex_key:
            return None  # first wins (by time, V1 first on a tie)
        if not repoint_sent and conn.execute("SELECT 1 FROM alerts WHERE signal_id = ? LIMIT 1",
                                             (int(existing[0]),)).fetchone() is not None:  # fmt: skip
            log.info("V1 PLUS %s on %s: its card went out; not re-pointed to the released signal %s", existing[0],
                     symbol, origin_id)  # fmt: skip
            return None
        conn.execute(
            f"UPDATE signals SET {', '.join(f'{c} = ?' for c in _COPY_COLS)}, payload_json = ?, tier = ?"
            " WHERE id = ?",
            (*values, json.dumps(new_payload, sort_keys=True), TIER_ENTRY, int(existing[0])),
        )
        conn.commit()
        log.info("V1 PLUS %s on %s re-pointed to the earlier %s signal %s", existing[0], symbol, origin, origin_id)
        return int(existing[0])
    cols = (*_COPY_COLS, "payload_json", "scanner", "tier")
    cur = conn.execute(
        f"INSERT INTO signals ({', '.join(cols)}) VALUES ({', '.join('?' for _ in cols)})",
        (*values, json.dumps(new_payload, sort_keys=True), SCANNER_PLUS, TIER_ENTRY),
    )
    conn.commit()
    log.info("V1 PLUS on %s from %s signal %s", symbol, origin, origin_id)
    return int(cur.lastrowid or 0) or None
