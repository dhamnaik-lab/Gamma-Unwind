"""T-170 (PLAN-W17 §2.1, review R3): the ONE resolver that names a trade's contract.

`query_trades` (GET /api/trades) and the tracker's WS trade push (`TradeTracker._trade_payload`) both call
`resolve_contract`, so the REST row and the push the dashboard merges over it (stores.ts) can never disagree.

Sources, in order; `symbol` and `strike` always come from the SAME source row:
  1. the watchlist row of the security id on the trade's ENTRY date;
  2. else the newest watchlist row of the security id whose expiry_date AND option_type equal the trade's;
  3. else the entry signal's payload_json `symbol` / `strike`;
  4. else (None, None).
Every source must agree with the trade's own `expiry_date` and `option_type` (a watchlist row is filtered on both; a
payload that carries either and differs is refused): a security id NSE re-used for another contract must print null,
never a wrong name (instrument identity). `retired_run_id` is NOT filtered: a trade can come from a row the 09:20 build
retired. Never reads TODAY's lists as such: the trade's own dates decide.
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Mapping
from typing import Any

_ROW_SQL = (
    "SELECT symbol, strike FROM watchlist WHERE security_id = ? AND expiry_date = ? AND option_type = ?{extra}"
    " ORDER BY trade_date DESC, list_name LIMIT 1"
)
_ON_ENTRY_DATE = _ROW_SQL.format(extra=" AND trade_date = ?")
_NEWEST = _ROW_SQL.format(extra="")


def _strike(value: Any) -> float | None:
    if isinstance(value, bool) or not isinstance(value, int | float | str):
        return None
    try:
        return float(value)
    except ValueError:
        return None


def _from_payload(payload_json: Any, expiry_date: Any, option_type: Any) -> tuple[str | None, float | None]:
    payload = payload_json
    if isinstance(payload_json, str | bytes):
        try:
            payload = json.loads(payload_json)
        except ValueError:
            return None, None
    if not isinstance(payload, Mapping):
        return None, None
    symbol = payload.get("symbol")
    if not isinstance(symbol, str) or not symbol.strip():
        return None, None
    for key, want in (("expiry_date", expiry_date), ("option_type", option_type)):
        got = payload.get(key)
        if got is not None and str(got) != str(want):
            return None, None  # a payload naming another contract is refused like a row
    return symbol, _strike(payload.get("strike"))


def resolve_contract(
    conn: sqlite3.Connection,
    security_id: int,
    entry_date: str | None,
    expiry_date: str | None,
    option_type: str | None,
    payload_json: Any,
) -> tuple[str | None, float | None]:
    """(symbol, strike) of a trade's contract, or (None, None); see the module docstring. `payload_json` is the entry
    signal's stored payload (a JSON string, an already-parsed mapping, or None)."""
    sid = int(security_id)
    if expiry_date is not None and option_type is not None:
        r = None
        if entry_date:
            r = conn.execute(_ON_ENTRY_DATE, (sid, expiry_date, option_type, entry_date)).fetchone()
        if r is None:
            r = conn.execute(_NEWEST, (sid, expiry_date, option_type)).fetchone()
        if r is not None:
            return str(r[0]), _strike(r[1])
    return _from_payload(payload_json, expiry_date, option_type)
