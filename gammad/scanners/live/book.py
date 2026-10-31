"""Durable ladder snapshots and booked-only risk over the injected store."""

import json
import math
import sqlite3
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import yaml

from gammad import REPO_ROOT
from gammad.scanners import contract as sc

IST = timezone(timedelta(hours=5, minutes=30))


def load_capital(path: Path = REPO_ROOT / "config" / "owner_risk.yaml") -> float:
    path = Path(path)
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
        value = raw["capital_rs"]
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise TypeError("not a number")
        capital = float(value)
        if not math.isfinite(capital) or capital <= 0:
            raise ValueError("not positive and finite")
        return capital
    except (OSError, ValueError, TypeError, KeyError, yaml.YAMLError) as exc:
        raise ValueError(f"Invalid capital_rs in {path}") from exc


class LadderBook:
    """Save each ladder step, with its newly emitted events, before the next step.

    A step supplies a cumulative net realized value, not per-leg net amounts.
    Its delta is attached once to the first event; other events carry zero
    (D682 (2)). Each saved event is stamped with the step's FILL day, the IST
    date of state.updated_ms (the processed bar's start, where the legs
    actually closed), never the deciding bar's date: a stop decided on D's
    last bar that fills at D+1's open is D+1's loss (D732 (1), audit
    P2-LIVE-BOOK-01). Thus daily risk uses the ladder's exact net delta
    without recomputing charges or MTM.
    Client callers pass edition="client" and capital_rs=None.
    """

    def __init__(self, conn: sqlite3.Connection, *, capital_rs: float | None, edition: str = "owner"):
        if edition not in ("owner", "client"):
            raise ValueError("unknown edition")
        if edition == "owner" and capital_rs is None:
            raise ValueError("owner capital_rs is required")
        self.conn = conn
        self._capital = capital_rs

    def capital_rs(self) -> float:
        if self._capital is None:
            raise ValueError("capital_rs is unavailable")
        return self._capital

    def save(self, state: sc.PositionState, book: str, events=()) -> None:
        if book not in ("OWN", "GROUP"):
            raise ValueError("book must be OWN or GROUP")
        events = tuple(events)
        if any(event.position_key != state.position_key for event in events):
            raise ValueError("event position_key differs from state")
        with self.conn:
            row = self.conn.execute(
                "SELECT state_json, events_json FROM ladder_state WHERE position_key=? AND book=?",
                (state.position_key, book),
            ).fetchone()
            previous = sc.PositionState.from_dict(json.loads(row[0])) if row else None
            history = json.loads(row[1]) if row else []
            delta = state.realized_rs - (previous.realized_rs if previous else 0.0)
            fresh = [event for event in events if not any(item["event"] == event.to_dict() for item in history)]
            bookings = [event for event in fresh if event.lots > 0]
            if delta and not bookings:
                raise ValueError("save needs booking events for a realized delta")
            # One fill day per step, so a step whose events carry two dates (a V2SQ CARRY resolved on the
            # next morning's first bar) books on the bar it ran on instead of raising (P2-LIVE-BOOK-03).
            day = datetime.fromtimestamp(state.updated_ms / 1000, IST).date().isoformat()
            anchor = fresh[0] if fresh else None
            history.extend({"event": event.to_dict(), "realized_rs": delta if event is anchor else 0.0, "day": day}
                           for event in fresh)
            self.conn.execute(
                "INSERT INTO ladder_state "
                "(position_key, book, scanner, entry_date, closed, state_json, events_json, updated_ms) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(position_key, book) DO UPDATE SET "
                "scanner=excluded.scanner, entry_date=excluded.entry_date, closed=excluded.closed, "
                "state_json=excluded.state_json, events_json=excluded.events_json, updated_ms=excluded.updated_ms",
                (state.position_key, book, state.scanner, state.entry_date.isoformat(), int(state.closed),
                 json.dumps(state.to_dict()), json.dumps(history), state.updated_ms),
            )

    def load_open(self) -> dict:
        rows = self.conn.execute(
            "SELECT position_key, book, state_json, events_json FROM ladder_state WHERE closed=0")
        return {(key, book): (sc.PositionState.from_dict(json.loads(state)),
                             tuple(sc.ExitEvent.from_dict(item["event"]) for item in json.loads(events)))
                for key, book, state, events in rows}

    def insert_signal(self, signal: sc.EntrySignal, *, spec_version: str) -> int:
        with self.conn:
            cursor = self.conn.execute(
                "INSERT INTO signals (scanner, kind, trade_date, ts_ms, security_id, list_name, expiry_date, "
                "ltp, sl, latency_ms, payload_json, spec_version) VALUES (?, 'E1', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (signal.scanner, signal.trade_date.isoformat(), signal.signal_ms, signal.security_id,
                 signal.list_name, signal.expiry_date.isoformat(), signal.signal_px, signal.stop_level,
                 signal.alert_latency_ms, json.dumps(signal.to_dict()), spec_version),
            )
            return cursor.lastrowid

    def signal_of(self, key: str) -> sc.EntrySignal | None:
        # Narrow by the identity fields before decoding the strict payload.
        parts = key.split("|")
        if len(parts) != 3:
            return None
        rows = self.conn.execute(
            "SELECT payload_json FROM signals WHERE scanner=? AND security_id=? ORDER BY id DESC",
            (parts[0], parts[1]),
        )
        for (payload,) in rows:
            try:
                data = json.loads(payload)
                if not isinstance(data, dict):
                    continue
                signal = sc.EntrySignal.from_dict(data)
            except (ValueError, TypeError, KeyError):
                continue
            if signal.key == key:
                return signal
        return None

    def record_delivery(self, signal_id: int, delivery) -> None:
        with self.conn:
            self.conn.execute(
                "INSERT INTO alerts (signal_id, channel, sent_ts_ms, status, error) VALUES (?, ?, ?, ?, ?)",
                (signal_id, delivery.chat, delivery.sent_ms, delivery.status, delivery.error),
            )

    def open_count(self) -> int:
        return self.conn.execute(
            "SELECT COUNT(*) FROM ladder_state WHERE book='OWN' AND closed=0 AND scanner IN (?, ?)",
            sc.SCANNER_IDS,
        ).fetchone()[0]

    def day_pnl_rs(self, day: date) -> float:
        total = 0.0
        rows = self.conn.execute(
            "SELECT events_json FROM ladder_state WHERE book='OWN' AND scanner IN (?, ?)", sc.SCANNER_IDS)
        for (payload,) in rows:
            for item in json.loads(payload):
                if booked_day(item) == day:
                    total += item["realized_rs"]
        return total


def booked_day(item: dict) -> date:
    """The IST day a stored events_json item books on: its step's FILL day (D732 (1), superseding D682 (2)).

    A row saved before "day" was stored falls back to its event date. LiveLadderBook keys its risk snapshot
    by this same function, so the sizing / 3 % day-cap reads and the stored bookings agree.
    """
    if "day" in item:
        return date.fromisoformat(item["day"])
    return datetime.fromtimestamp(sc.ExitEvent.from_dict(item["event"]).ts_ms / 1000, IST).date()
