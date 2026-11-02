"""R1.9 Broker-Neutral Durable Feed Watermark Engine.

Provides durable SQLite high-water mark protection segregated by
(broker, account_scope, exchange_segment, security_id).

Architectural rules (ADR-004, ADR-010):
- Prevents stale tick regression across feed reconnects and process restarts.
- Synchronous SQLite WAL with synchronous=FULL commits before ticks reach the consumer.
- Bound to trusted AccountIdentity (never accepts tokens, keys or secrets).
- Works with any registered or synthetic broker adapter.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import sqlite3
from contextlib import closing
from pathlib import Path
from typing import Any

from gammad.watch.feed_protocol import Tick


class WatermarkHold(RuntimeError):
    """Raised when a tick must not reach the strategy / signal queue."""


class WatermarkStore:
    """SQLite durable high-water mark store segregated by broker and account scope.

    Every accepted watermark commits durably before the tick reaches the signal engine.
    Serializes concurrent writes and isolates namespaces.
    """

    def __init__(self, path: Path, *, broker: str, account_scope: str) -> None:
        self.path = Path(path).resolve()
        b_clean = broker.strip().lower() if isinstance(broker, str) else ""
        if not b_clean or len(b_clean) > 48:
            raise WatermarkHold("missing_or_invalid_broker_scope")

        acc_clean = account_scope.strip() if isinstance(account_scope, str) else ""
        if not acc_clean or len(acc_clean) > 160 or len(acc_clean) != len(account_scope):
            raise WatermarkHold("missing_or_invalid_account_scope")

        self.broker = b_clean
        self.account_scope = acc_clean
        # Store only a keyed namespace digest, never raw credentials or secrets
        self.namespace = hashlib.sha256(
            json.dumps([self.broker, self.account_scope], separators=(",", ":")).encode("utf-8")
        ).hexdigest()

        # Path security checks
        parent = self.path.parent
        if not parent.is_dir() or parent.is_symlink() or (hasattr(parent, "is_junction") and parent.is_junction()):
            raise WatermarkHold("untrusted_ledger_directory")
        if self.path.exists() and (
            not self.path.is_file() or self.path.is_symlink() or (hasattr(self.path, "is_junction") and self.path.is_junction())
        ):
            raise WatermarkHold("untrusted_ledger_file")

        # Initialize schema
        try:
            with closing(self._connect()) as con:
                con.execute("PRAGMA journal_mode=WAL")
                con.execute("""
                    CREATE TABLE IF NOT EXISTS feed_watermarks (
                        namespace TEXT NOT NULL,
                        segment TEXT NOT NULL,
                        security_id INTEGER NOT NULL,
                        ltt_ms INTEGER NOT NULL,
                        payload_digest TEXT NOT NULL,
                        PRIMARY KEY(namespace, segment, security_id)
                    ) WITHOUT ROWID
                """)
        except sqlite3.Error as exc:
            raise WatermarkHold("ledger_unavailable") from exc

    def _connect(self) -> sqlite3.Connection:
        con = sqlite3.connect(str(self.path), timeout=3.0, isolation_level=None)
        con.execute("PRAGMA busy_timeout=3000")
        con.execute("PRAGMA synchronous=FULL")
        return con

    def claim(self, t: Tick) -> bool:
        """Durably reserve strictly increasing LTT or a changed payload at the same LTT.

        Rejects absent/unverifiable LTT for NSE FNO instruments.
        Returns True if accepted and committed, False if duplicate or regressed.
        """
        if t.segment != "NSE_FNO":
            # Non-FNO instruments are not gated by the FNO LTT watermark in this stage
            return True

        ts = int(t.ts_ms)
        recv = int(t.recv_ms)
        if ts <= 0 or ts == recv or ts % 1000 != 0:
            raise WatermarkHold("exchange_ltt_unverifiable")
        if recv - ts > 300_000 or ts - recv > 5_000:
            raise WatermarkHold("exchange_ltt_stale_or_future")

        if self.path.is_symlink() or (hasattr(self.path, "is_junction") and self.path.is_junction()) or not self.path.is_file():
            raise WatermarkHold("ledger_file_changed")

        value = [t.ltp, t.volume_day, t.oi, t.prev_close]
        digest = hashlib.sha256(json.dumps(value, separators=(",", ":"), allow_nan=False).encode("utf-8")).hexdigest()

        try:
            with closing(self._connect()) as con:
                try:
                    con.execute("BEGIN IMMEDIATE")
                    previous = con.execute(
                        """
                        SELECT ltt_ms, payload_digest FROM feed_watermarks
                        WHERE namespace=? AND segment=? AND security_id=?
                        """,
                        (self.namespace, t.segment, int(t.security_id)),
                    ).fetchone()

                    if previous is not None:
                        prev_ltt, prev_digest = previous
                        if ts < prev_ltt:
                            con.rollback()
                            return False  # Regression rejected
                        if ts == prev_ltt and digest == prev_digest:
                            con.rollback()
                            return False  # Duplicate tick rejected

                    con.execute(
                        """
                        INSERT INTO feed_watermarks(namespace, segment, security_id, ltt_ms, payload_digest)
                        VALUES(?, ?, ?, ?, ?)
                        ON CONFLICT(namespace, segment, security_id) DO UPDATE SET
                            ltt_ms=excluded.ltt_ms,
                            payload_digest=excluded.payload_digest
                        """,
                        (self.namespace, t.segment, int(t.security_id), ts, digest),
                    )
                    con.commit()
                except BaseException:
                    con.rollback()
                    raise
        except sqlite3.Error as exc:
            raise WatermarkHold("ledger_write_refused") from exc

        return True


class DurableTickQueue:
    """Pre-consumer gate ensuring durable reservation occurs before queue insertion."""

    def __init__(self, target: asyncio.Queue[Tick], store: WatermarkStore) -> None:
        self.target = target
        self.store = store
        self.refused = 0
        self.accepted = 0

    def put_nowait(self, t: Tick) -> None:
        try:
            ok = self.store.claim(t)
        except WatermarkHold:
            self.refused += 1
            raise

        if not ok:
            self.refused += 1
            raise WatermarkHold("duplicate_or_regression")

        self.accepted += 1
        self.target.put_nowait(t)


def wrap_feed_queue(queue: asyncio.Queue[Tick], store: WatermarkStore) -> DurableTickQueue:
    """Factory helper to wrap a standard tick queue with durable watermark gating."""
    return DurableTickQueue(queue, store)
