"""Nuvama request budget: the DAILY cap (86,400/day) and the 5-minute window (3,000 / 5 min / IP), wire.py P10.

Why a guard at all: SPEC "API Usage Threshold" (saved page line 466, the table's rowspan=4 "If breached?" cell
covers the 86,400 row): a breach blocks the APP "15 mins for the first breach ... full day for 5th breach", and
"repetetive breaches may lead to permanent blocking". A scanner silenced for a whole day is worse than one that
polls slower for its last hour, so the adapter degrades BEFORE the cap (stages below, sized in wire.py P10).

One DailyBudget per data_dir per process (`shared_budget`): every NuvamaAdapter built on the same data_dir (the
app's adapter, the PollFeed that reuses it, a job's adapter) spends from ONE counter. The count is persisted to
`<data_dir>/nuvama-budget.json` every FLUSH_EVERY calls and on every stage change, so a restart mid-day resumes
it instead of starting at 0. Calls this process cannot see (the two login calls, another process on the same API
key, up to FLUSH_EVERY counts lost to a hard kill) are what wire.BUDGET_RESERVE is for.

Stages (count = calls charged today, IST calendar day; sized for the 0.8.5 day in wire.py P10):
  ok    count <  LEAN_AT : everything allowed.
  lean  LEAN_AT <= count : 0.8.5 D4, the FIRST degradation: option chains admitted at most wire.LEAN_CHAINS_PER_MARK
                           per wire.LEAN_MARK_S process-wide (`chain_slot`); a refused chain's new listing waits
                           for the next mark. Quotes (the poll of every listed contract, ltp) are untouched.
  slow  SLOW_AT <= count : each id is quoted at most every wire.SLOW_POLL_S (the feed's other cycles see it
                           ABSENT -- no tick, nothing invented); candles/chains still allowed.
  shed  SHED_AT <= count : option (NSE_FNO) quotes, candles and chains REFUSED; underlying quotes (slow cadence),
                           the health probe and the instrument master still allowed.
  stop  STOP_AT <= count : every call refused until 00:00 IST.
"""

from __future__ import annotations

import json
import threading
from collections import deque
from collections.abc import Callable
from datetime import date
from pathlib import Path

from gammad.broker.base import BrokerError
from gammad.broker.nuvama import wire
from gammad.config import now_ist
from gammad.log import get_logger

log = get_logger(__name__)

OK = "ok"
LEAN = "lean"
SLOW = "slow"
SHED = "shed"
STOP = "stop"
STAGES = (OK, LEAN, SLOW, SHED, STOP)

FILE_NAME = "nuvama-budget.json"
FLUSH_EVERY = 100


class NuvamaBudgetError(BrokerError):
    """The adapter refused a call to stay under Nuvama's documented request limits (not an auth problem)."""


def _today_ist() -> date:
    return now_ist().date()


class DailyBudget:
    def __init__(
        self,
        data_dir: Path | None,
        *,
        today: Callable[[], date] = _today_ist,
        slow_at: int = wire.BUDGET_SLOW_AT,
        shed_at: int = wire.BUDGET_SHED_AT,
        stop_at: int = wire.BUDGET_STOP_AT,
        window_guard: int = wire.WINDOW_GUARD,
        window_s: float = wire.WINDOW_S,
        lean_at: int | None = None,
    ) -> None:
        # lean_at None = wire.BUDGET_LEAN_AT, never above slow_at (a test's tiny slow_at keeps its old stages)
        lean_at = min(wire.BUDGET_LEAN_AT, slow_at) if lean_at is None else lean_at
        if not (0 < lean_at <= slow_at <= shed_at <= stop_at <= wire.DAILY_REQUEST_CAP):
            raise ValueError("budget stages must satisfy 0 < lean <= slow <= shed <= stop <= the daily cap")
        self.path = None if data_dir is None else Path(data_dir) / FILE_NAME
        self._today = today
        self.lean_at, self.slow_at, self.shed_at, self.stop_at = lean_at, slow_at, shed_at, stop_at
        self._chains: deque[float] = deque()  # LEAN: clock() of each chain admitted in the last LEAN_MARK_S
        self.window_guard, self.window_s = window_guard, window_s
        self._lock = threading.Lock()
        self._day = today()
        self._count = 0
        self._unflushed = 0
        self._stage_logged = OK
        self._window: deque[float] = deque()
        self._write_failed = False
        self._load()

    # ---------------------------------------------------------------- persistence
    def _load(self) -> None:
        if self.path is None or not self.path.exists():
            return
        try:
            raw = json.loads(self.path.read_text(encoding="utf-8"))
            if raw.get("day") == self._day.isoformat():
                self._count = max(0, int(raw.get("count") or 0))
        except (OSError, ValueError, TypeError, AttributeError) as exc:
            log.warning(
                "nuvama budget: %s unreadable (%s); counting from 0 today", self.path, type(exc).__name__
            )

    def _flush(self) -> None:
        if self.path is None:
            return
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self.path.with_suffix(".json.part")
            tmp.write_text(json.dumps({"day": self._day.isoformat(), "count": self._count}), encoding="utf-8")
            tmp.replace(self.path)
            self._unflushed = 0
        except OSError as exc:
            if not self._write_failed:
                log.warning("nuvama budget: cannot persist %s (%s); counting in memory", self.path, exc)
            self._write_failed = True

    # ---------------------------------------------------------------- day + stage
    def _roll(self) -> None:
        """Caller holds the lock. A new IST day starts the count at 0."""
        d = self._today()
        if d != self._day:
            self._day, self._count, self._stage_logged = d, 0, OK
            self._flush()

    def _stage_of(self, n: int) -> str:
        if n >= self.stop_at:
            return STOP
        if n >= self.shed_at:
            return SHED
        if n >= self.slow_at:
            return SLOW
        if n >= self.lean_at:
            return LEAN
        return OK

    @property
    def count(self) -> int:
        with self._lock:
            self._roll()
            return self._count

    def stage(self) -> str:
        with self._lock:
            self._roll()
            return self._stage_of(self._count)

    def status(self) -> str | None:
        """None while OK; else one line for the dashboard / feed DEGRADED reason (numbers only, no secrets)."""
        with self._lock:
            self._roll()
            n, st = self._count, self._stage_of(self._count)
        if st == OK:
            return None
        what = {
            LEAN: (
                f"new option chains slowed to {wire.LEAN_CHAINS_PER_MARK} per {wire.LEAN_MARK_S / 60:.0f} min "
                f"(new listings wait a mark; listed contracts still polled every {wire.POLL_S:.0f} s)"
            ),
            SLOW: f"quotes slowed to every {wire.SLOW_POLL_S:.0f} s",
            SHED: "option quotes, candles and chains paused; underlyings polled slowly",
            STOP: "all Nuvama calls paused",
        }[st]
        return (
            f"Nuvama daily API budget: {n:,} of {wire.DAILY_REQUEST_CAP:,} calls used today; {what} "
            "until 00:00 IST (a breach blocks the app, up to a full day)"
        )

    def chain_slot(self, now: float) -> bool:
        """True = an option chain may be fetched now. Always True below LEAN; in LEAN at most
        wire.LEAN_CHAINS_PER_MARK per wire.LEAN_MARK_S process-wide (`now` = the caller's monotonic clock).
        The stages above LEAN refuse chains through the adapter's kind check, not here."""
        with self._lock:
            self._roll()
            if self._stage_of(self._count) != LEAN:
                return True
            while self._chains and now - self._chains[0] >= wire.LEAN_MARK_S:
                self._chains.popleft()
            if len(self._chains) >= wire.LEAN_CHAINS_PER_MARK:
                return False
            self._chains.append(now)
            return True

    def refuse(self, what: str) -> NuvamaBudgetError:
        return NuvamaBudgetError(f"{self.status() or 'Nuvama daily API budget'} -- refused: {what}", "budget")

    # ---------------------------------------------------------------- spending
    def charge(self, clock: Callable[[], float], sleep: Callable[[float], None]) -> None:
        """Record ONE HTTP call about to be sent. Waits (never refuses) when the process-wide 5-minute window is at
        WINDOW_GUARD; stage refusals are the CALLER's job (it knows what kind of call it is)."""
        with self._lock:
            self._roll()
            now = clock()
            while self._window and now - self._window[0] >= self.window_s:
                self._window.popleft()
            if len(self._window) >= self.window_guard:
                wait = self.window_s - (now - self._window[0])
                log.warning(
                    "nuvama budget: %d calls in %.0f s; waiting %.1f s",
                    len(self._window),
                    self.window_s,
                    wait,
                )
                sleep(max(wait, 1e-3))
                now = clock()
                while self._window and now - self._window[0] >= self.window_s:
                    self._window.popleft()
            self._window.append(now)
            before = self._stage_of(self._count)
            self._count += 1
            self._unflushed += 1
            after = self._stage_of(self._count)
            if after != before or self._unflushed >= FLUSH_EVERY:
                self._flush()
            log_stage = after != before and after != self._stage_logged
            if log_stage:
                self._stage_logged = after
        if log_stage:
            log.warning("%s", self.status())


_SHARED: dict[Path, DailyBudget] = {}
_SHARED_LOCK = threading.Lock()


def shared_budget(data_dir: Path) -> DailyBudget:
    """The ONE budget of this process for `data_dir` (every adapter on the same data_dir spends from it)."""
    key = Path(data_dir).resolve()
    with _SHARED_LOCK:
        b = _SHARED.get(key)
        if b is None:
            b = _SHARED[key] = DailyBudget(key)
        return b
