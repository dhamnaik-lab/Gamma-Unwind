"""FYERS Standard DATA API governor (local conservative limits; no broker requests made here).

SQLite reservations are committed before each Data REST attempt, including transport failures/retries.
Conservative 4/s, 40/rolling 60s, 4000/IST day; shared across process restarts.
All reservations consume budget: safe over-counting preferred to under-counting.
"""
from __future__ import annotations
import sqlite3
import time
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

IST = ZoneInfo("Asia/Kolkata")

class DailyBudgetExhausted(RuntimeError):
    """Stop remaining data calls until the next IST day; do not auto-retry."""

class FyersDataBudget:
    def __init__(self, path: Path, *, per_second: int = 4, per_minute: int = 40,
                 per_day: int = 4000, wall_clock=None, sleep=None):
        self.path = Path(path)
        self.per_second = per_second
        self.per_minute = per_minute
        self.per_day = per_day
        self.clock = wall_clock or time.time
        self.sleep = sleep or time.sleep
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._db() as db:
            db.execute("CREATE TABLE IF NOT EXISTS fyers_data_calls (ts REAL NOT NULL, ist_day TEXT NOT NULL)")
            db.execute("CREATE INDEX IF NOT EXISTS fyers_data_calls_day ON fyers_data_calls(ist_day)")

    @contextmanager
    def _db(self):
        conn = sqlite3.connect(str(self.path), timeout=20, isolation_level=None)
        try:
            yield conn
        finally:
            conn.close()

    def reserve(self) -> None:
        """Atomically reserve one data call; wait *outside* transaction if rolling cap reached."""
        while True:
            now = float(self.clock())
            day = datetime.fromtimestamp(now, IST).date().isoformat()
            with self._db() as db:
                db.execute("BEGIN IMMEDIATE")
                db.execute("DELETE FROM fyers_data_calls WHERE ts < ?", (now - 172800,))
                nday = db.execute("SELECT COUNT(*) FROM fyers_data_calls WHERE ist_day=?", (day,)).fetchone()[0]
                if nday >= self.per_day:
                    db.execute("COMMIT")
                    raise DailyBudgetExhausted("FYERS local Data API day budget exhausted; no request sent")
                second = db.execute("SELECT COUNT(*), MIN(ts) FROM fyers_data_calls WHERE ts > ?", (now - 1,)).fetchone()
                minute = db.execute("SELECT COUNT(*), MIN(ts) FROM fyers_data_calls WHERE ts > ?", (now - 60,)).fetchone()
                if second[0] < self.per_second and minute[0] < self.per_minute:
                    db.execute("INSERT INTO fyers_data_calls(ts,ist_day) VALUES(?,?)", (now, day))
                    db.execute("COMMIT")
                    return
                delay = max((float(second[1]) + 1 - now) if second[0] >= self.per_second else 0,
                            (float(minute[1]) + 60 - now) if minute[0] >= self.per_minute else 0, 0.05)
                db.execute("COMMIT")
            self.sleep(min(delay + 0.02, 60.1))
