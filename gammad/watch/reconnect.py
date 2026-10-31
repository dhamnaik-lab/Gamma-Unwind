"""Shared feed timing policy. Callers own sockets, subscriptions and data parsing."""

from __future__ import annotations

import math
import random
import time
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone

IST = timezone(timedelta(hours=5, minutes=30))


def in_market_hours(now_ms: int, *, open_hm: tuple[int, int] = (9, 15),
                    close_hm: tuple[int, int] = (15, 30)) -> bool:
    """Weekday IST window, close minute excluded; not an exchange holiday calendar."""
    now = datetime.fromtimestamp(now_ms / 1000, IST)
    return now.weekday() < 5 and open_hm <= (now.hour, now.minute) < close_hm


@dataclass
class ReconnectPolicy:
    base_s: float = 1.0
    cap_s: float = 30.0
    max_attempts: int | None = None
    reset_after_data_s: float = 5.0
    _attempts: int = field(default=0, init=False, repr=False)
    _prev: float = field(init=False, repr=False)
    _data_since: float | None = field(default=None, init=False, repr=False)
    _connected: bool = field(default=False, init=False, repr=False)

    def __post_init__(self) -> None:
        if any(not math.isfinite(n) for n in (self.base_s, self.cap_s, self.reset_after_data_s)):
            raise ValueError("timings must be finite")
        if self.base_s <= 0 or self.cap_s < self.base_s or self.reset_after_data_s < 0:
            raise ValueError("require 0 < base <= cap and a nonnegative data window")
        if self.max_attempts is not None and (type(self.max_attempts) is not int or self.max_attempts < 0):
            raise ValueError("max_attempts must be None or a nonnegative integer")
        self._prev = self.base_s

    def next_delay(self, rng: random.Random) -> float:
        if self.exhausted:
            raise RuntimeError("reconnect attempts exhausted")
        self._connected = False
        self._data_since = None
        self._prev = min(self.cap_s, rng.uniform(self.base_s, self._prev * 3))
        self._attempts += 1
        return self._prev

    def on_connected(self) -> None:
        self._connected = True
        self._data_since = None

    def on_data(self, now_s: float) -> None:
        """First DATA starts the stability window; connect/ping alone never resets it."""
        if not self._connected:
            return
        if self._data_since is None:
            self._data_since = now_s
        if now_s - self._data_since >= self.reset_after_data_s:
            self._attempts = 0
            self._prev = self.base_s

    @property
    def attempts(self) -> int:
        return self._attempts

    @property
    def exhausted(self) -> bool:
        return self.max_attempts is not None and self._attempts >= self.max_attempts


class IdleWatchdog:
    """Create per connection. Only DATA calls feed; protocol pings do not."""

    def __init__(self, idle_s: float = 60.0, clock: Callable[[], float] = time.monotonic) -> None:
        if not math.isfinite(idle_s) or idle_s <= 0:
            raise ValueError("idle_s must be finite and positive")
        self.idle_s = idle_s
        self._last_data = clock()

    def feed(self, now_s: float) -> None:
        self._last_data = now_s

    def expired(self, now_s: float, market_open: bool) -> bool:
        return market_open and now_s - self._last_data >= self.idle_s


class Heartbeat:
    """Traffic-independent ping timer; sent is called only after sending a ping."""

    def __init__(self, every_s: float, clock: Callable[[], float] = time.monotonic) -> None:
        if not math.isfinite(every_s) or every_s <= 0:
            raise ValueError("every_s must be finite and positive")
        self.every_s = every_s
        self._last_sent = clock()

    def due(self, now_s: float) -> bool:
        return now_s - self._last_sent >= self.every_s

    def sent(self, now_s: float) -> None:
        self._last_sent = now_s


@dataclass
class FeedHealth:
    connects: int = 0
    reconnects: int = 0
    idle_reconnects: int = 0
    last_data_ms: int | None = None
    last_error: str | None = None
    breaker_state: str = "closed"

    def as_status(self) -> dict[str, int | str | None]:
        return asdict(self)
