"""GAMMA R10-B: local user-selected underlyings, not generated option contracts.

Storage: one JSON document under a namespaced key in existing app_settings.
No schema change, default selections empty, no scanner or live market-data effects.
"""
from __future__ import annotations

import json
import re
import sqlite3
import time
from typing import Any

KEY = 'r10_user_symbol_selections_v1'
SCHEMA = 1
MAX_STOCKS = 75
INDEX_SYMBOLS = frozenset(('NIFTY', 'BANKNIFTY'))
STOCK_RE = re.compile(r'^[A-Z][A-Z0-9&._-]{0,24}$', re.ASCII)


class SelectionError(ValueError):
    """Invalid user selection. No untrusted data in error strings."""


class SelectionConflict(RuntimeError):
    """Optimistic concurrency conflict; must refresh before saving again."""


def _default() -> dict[str, Any]:
    return {'schema': SCHEMA, 'revision': 0, 'stocks': [], 'indices': []}


def validate(stocks: Any, indices: Any) -> tuple[list[str], list[str]]:
    """Strict fixed-field API; do not auto-correct typos or permit arbitrary tickers."""
    if not isinstance(stocks, list) or len(stocks) > MAX_STOCKS:
        raise SelectionError('invalid_stocks_list')
    if not isinstance(indices, list) or len(indices) > len(INDEX_SYMBOLS):
        raise SelectionError('invalid_indices_list')
    good_stocks: list[str] = []
    good_indices: list[str] = []
    for token in stocks:
        if (not isinstance(token, str) or not STOCK_RE.fullmatch(token)
                or token in INDEX_SYMBOLS or token != token.strip()):
            raise SelectionError('invalid_stock_symbol')
        good_stocks.append(token)
    for token in indices:
        if not isinstance(token, str) or token not in INDEX_SYMBOLS:
            raise SelectionError('invalid_index_symbol')
        good_indices.append(token)
    if len(set(good_stocks)) != len(good_stocks):
        raise SelectionError('duplicate_stock_symbol')
    if len(set(good_indices)) != len(good_indices):
        raise SelectionError('duplicate_index_symbol')
    return sorted(good_stocks), sorted(good_indices)


def _from_row(row: Any) -> dict[str, Any]:
    if row is None:
        return _default()
    try:
        raw = row[0]
        data = json.loads(raw)
        if not isinstance(data, dict) or set(data) != {'schema', 'revision', 'stocks', 'indices'}:
            raise ValueError()
        if type(data['schema']) is not int or data['schema'] != SCHEMA:
            raise ValueError()
        if type(data['revision']) is not int or data['revision'] < 1:
            raise ValueError()
        stocks, indices = validate(data['stocks'], data['indices'])
        if stocks != data['stocks'] or indices != data['indices']:
            raise ValueError()
        return {'schema': SCHEMA, 'revision': data['revision'], 'stocks': stocks, 'indices': indices}
    except (ValueError, TypeError, KeyError, SelectionError) as exc:
        raise SelectionError('stored_selection_invalid') from exc


def load(conn: sqlite3.Connection) -> dict[str, Any]:
    row = conn.execute('SELECT value FROM app_settings WHERE key = ?', (KEY,)).fetchone()
    return _from_row(row)


def save(conn: sqlite3.Connection, *, expected_revision: int, stocks: Any,
         indices: Any, now_ms: int | None = None) -> dict[str, Any]:
    """Atomic compare-and-set. Caller uses a fresh SQLite connection; no global lock.

    Existing settings and generated watchlist/trades are never deleted or modified.
    """
    if type(expected_revision) is not int or expected_revision < 0:
        raise SelectionError('invalid_revision')
    normalized_stocks, normalized_indices = validate(stocks, indices)
    conn.execute('BEGIN IMMEDIATE')
    try:
        old = load(conn)
        if old['revision'] != expected_revision:
            raise SelectionConflict('stale_revision')
        if old['stocks'] == normalized_stocks and old['indices'] == normalized_indices:
            conn.commit()
            return old
        result = {'schema': SCHEMA, 'revision': expected_revision + 1,
                  'stocks': normalized_stocks, 'indices': normalized_indices}
        now = int(time.time() * 1000) if now_ms is None else int(now_ms)
        conn.execute(
            'INSERT INTO app_settings (key, value, updated_ms, source) VALUES (?, ?, ?, ?) '
            'ON CONFLICT(key) DO UPDATE SET value=excluded.value, '
            'updated_ms=excluded.updated_ms, source=excluded.source',
            (KEY, json.dumps(result, separators=(',', ':'), ensure_ascii=True), now, 'local_dashboard'),
        )
        conn.commit()
        return result
    except BaseException:
        conn.rollback()
        raise
