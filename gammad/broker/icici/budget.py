"""Staged daily budget, modelled on nuvama/budget.py; SQLite commits BEFORE every HTTP send.

Local policy: ok <3500, lean <4000, slow <4500, shed <5000, stop >=5000.
Lean admits two chain requests/15 minutes, slow quotes once/id/60s, shed refuses
options/history/chains. Health remains available until stop. Counters are shared
across adapters and processes using one data_dir; external applications remain unseen.
Unlike the Nuvama batched flush, a crash cannot forget already admitted requests.
"""

from __future__ import annotations

import sqlite3
import threading
import time
from pathlib import Path

from gammad.broker.base import BrokerError
from gammad.broker.dhan import TokenBucket
from gammad.config import now_ist


class DailyBudget:
    def __init__(self, data_dir: Path | None, *, today=None, clock=time.monotonic, sleep=time.sleep):
        self._today = today or (lambda: now_ist().date())
        self._lock = threading.RLock()
        self._clock = clock
        self._bucket = TokenBucket(1, 1, clock, sleep)  # <=61/min, conservative against 100/min.
        if data_dir is not None:
            Path(data_dir).mkdir(parents=True, exist_ok=True)
        path = ":memory:" if data_dir is None else str(Path(data_dir) / "icici-budget.sqlite3")
        try:
            self._db = sqlite3.connect(path, timeout=5, check_same_thread=False, isolation_level=None)
            self._db.execute(
                "CREATE TABLE IF NOT EXISTS budget (day TEXT PRIMARY KEY, count INTEGER NOT NULL)"
            )
            self._db.execute(
                "CREATE TABLE IF NOT EXISTS slots (day TEXT, kind TEXT, stamp REAL, PRIMARY KEY(day,kind))"
            )
        except sqlite3.Error as exc:
            raise BrokerError("cannot open Breeze request budget", code="daily_quota") from exc

    @property
    def count(self) -> int:
        with self._lock:
            row = self._db.execute(
                "SELECT count FROM budget WHERE day=?", (self._today().isoformat(),)
            ).fetchone()
            return int(row[0]) if row else 0

    def stage(self) -> str:
        return self.stage_of(self.count)

    @staticmethod
    def stage_of(count: int) -> str:
        return (
            "stop"
            if count >= 5000
            else "shed"
            if count >= 4500
            else "slow"
            if count >= 4000
            else "lean"
            if count >= 3500
            else "ok"
        )

    def charge(self, kind="health", key="", *, now: float | None = None) -> None:
        # Stop is checked before any pacing: quota exhaustion never sleeps.
        with self._lock:
            if self.count >= 5000:
                raise BrokerError("daily quota reached", code="daily_quota")
            self._bucket.acquire()
            day = self._today().isoformat()
            stamp = time.time() if now is None else now
            try:
                self._db.execute("BEGIN IMMEDIATE")
                self._db.execute("INSERT OR IGNORE INTO budget VALUES (?,0)", (day,))
                count = self._db.execute("SELECT count FROM budget WHERE day=?", (day,)).fetchone()[0]
                stage = self.stage_of(count)
                if stage == "stop":
                    raise BrokerError("daily quota reached", code="daily_quota")
                if stage == "shed" and kind in ("option", "history", "chain"):
                    raise BrokerError("Breeze shed stage refuses optional data", code="budget_shed")
                slot = None
                interval = 0
                if stage != "ok" and kind == "chain":
                    # A native chain needs BOTH rights immediately: admit a pair, not half a chain.
                    recent = dict(
                        self._db.execute(
                            "SELECT kind,stamp FROM slots WHERE day=? AND kind IN ('chain:0','chain:1')",
                            (day,),
                        )
                    )
                    slot = min(("chain:0", "chain:1"), key=lambda k: recent.get(k, float("-inf")))
                    interval = 900
                elif stage in ("slow", "shed") and kind in ("quote", "option"):
                    slot, interval = "quote:" + key, 60
                if slot:
                    previous = self._db.execute(
                        "SELECT stamp FROM slots WHERE day=? AND kind=?", (day, slot)
                    ).fetchone()
                    if previous and stamp - previous[0] < interval:
                        raise BrokerError("Breeze budget cadence refusal", code="budget_cadence")
                    self._db.execute("INSERT OR REPLACE INTO slots VALUES (?,?,?)", (day, slot, stamp))
                self._db.execute("UPDATE budget SET count=count+1 WHERE day=?", (day,))
                self._db.execute("COMMIT")
            except (sqlite3.Error, BrokerError) as exc:
                if self._db.in_transaction:
                    self._db.execute("ROLLBACK")
                if isinstance(exc, BrokerError):
                    raise
                raise BrokerError("cannot persist Breeze request budget", code="daily_quota") from exc


_SHARED: dict[Path, DailyBudget] = {}
_LOCK = threading.Lock()


def shared_budget(data_dir: Path, *, clock=time.monotonic, sleep=time.sleep) -> DailyBudget:
    path = Path(data_dir).resolve()
    with _LOCK:
        if path not in _SHARED:
            _SHARED[path] = DailyBudget(path, clock=clock, sleep=sleep)
        return _SHARED[path]
