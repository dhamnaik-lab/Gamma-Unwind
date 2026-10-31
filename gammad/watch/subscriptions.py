"""Feed subscription set for a trade date, from the watchlist table (strategy Step 3, D11).

Every distinct watchlist contract is subscribed in FULL mode on NSE_FNO (OI arrives in-packet), and
every distinct underlying in QUOTE mode on NSE_EQ. Nothing else: never the whole chain (D11).
Used at start-up and after the 13:15 re-scan (`additions` gives only what is new).
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterable
from pathlib import Path

from gammad import edition
from gammad.store import db
from gammad.watch.feed import MODE_FULL, MODE_QUOTE, Instrument

OPTION_SEGMENT = "NSE_FNO"
UNDERLYING_SEGMENT = "NSE_EQ"


def build_subscriptions(conn: sqlite3.Connection, trade_date: str) -> list[Instrument]:
    """[(segment, security_id, mode)] for `trade_date` (YYYY-MM-DD): contracts first, then underlyings,
    each sorted by security_id so the result is deterministic."""
    rows = conn.execute(
        # S1.3.1 (T-063): a row the 09:20 build retired (its zone died) is not watched. D429: nor, in the CLIENT edition,
        # an EXPIRY row (an open trade on one keeps its ticks: the app unions tracker.open_instruments())
        "SELECT DISTINCT security_id, underlying_security_id FROM watchlist WHERE trade_date = ? "
        "AND retired_run_id IS NULL" + edition.v1_lists_sql(),
        (trade_date,),
    ).fetchall()
    contracts = sorted({int(r[0]) for r in rows})
    underlyings = sorted({int(r[1]) for r in rows})
    return [(OPTION_SEGMENT, sid, MODE_FULL) for sid in contracts] + [
        (UNDERLYING_SEGMENT, sid, MODE_QUOTE) for sid in underlyings
    ]


def load_subscriptions(trade_date: str, db_path: Path | str | None = None) -> list[Instrument]:
    conn = db.connect(db_path)
    try:
        return build_subscriptions(conn, trade_date)
    finally:
        conn.close()


def union(*sets: Iterable[Instrument]) -> list[Instrument]:
    """W12 (TRADE-TRACKER-SPEC §4): today's list subscriptions + every open trade's contract / underlying, in
    order; the FIRST occurrence of a (segment, security_id) wins (its mode is kept)."""
    seen: set[tuple[str, int]] = set()
    out: list[Instrument] = []
    for items in sets:
        for x in items:
            key = (x[0], int(x[1]))
            if key not in seen:
                seen.add(key)
                out.append(x)
    return out


def additions(current: Iterable[Instrument], wanted: Iterable[Instrument]) -> list[Instrument]:
    """Instruments in `wanted` whose (segment, security_id) is not already in `current` (13:15 re-scan).
    Watchlist rows are never removed during the day, so there is no removal counterpart."""
    have = {(s, i) for s, i, _ in current}
    return [x for x in wanted if (x[0], x[1]) not in have]
