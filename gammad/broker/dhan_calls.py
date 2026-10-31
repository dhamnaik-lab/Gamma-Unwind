"""T-350: process-local Dhan REST request counts, including retries, by IST day.

The registry caller installs the hook; the pinned adapter remains unchanged.
The response hook counts rate-limit rejections (HTTP 429 or RATE_CODES) and keeps
the request total at the first one, including other threads' in-flight requests
(approximate by up to the thread count). The request hook keeps the peak distinct
option-chain underlyings in any 8 s window (D397, counting only).
"""

import json
from collections import deque
from collections.abc import Callable
from datetime import date, datetime
from threading import Lock

import httpx

from gammad.broker.dhan import (
    EP_EXPIRY_LIST,
    EP_HISTORICAL,
    EP_INTRADAY,
    EP_OPTION_CHAIN,
    RATE_CODES,
    error_code,
)
from gammad.config import now_ist

BUCKETS = ("data", "quote", "option_chain", "other")
OC_WINDOW_S = 8.0
ERROR_BODY_MAX = 4096


def bucket_of(path: str) -> str:
    path = path.rstrip("/")
    if path.endswith((EP_EXPIRY_LIST, EP_HISTORICAL, EP_INTRADAY)):
        return "data"
    if path.endswith(EP_OPTION_CHAIN):
        return "option_chain"
    if "/marketfeed/" in path:
        return "quote"
    return "other"


class DailyCounter:
    def __init__(self, clock: Callable[[], datetime] = now_ist):
        self._clock = clock
        self._lock = Lock()
        self._started = clock()
        self._reset(self._started.date())

    def _reset(self, day: date) -> None:
        self._day = day
        self._counts = dict.fromkeys(BUCKETS, 0)
        self._limited = 0
        self._first_limited: tuple[int, str] | None = None
        self._oc: deque[tuple[datetime, str | None]] = deque()
        self._oc_peak = 0

    def count(self, path: str, scrip: str | None = None) -> None:
        with self._lock:
            now = self._clock()
            day = now.date()
            if day != self._day:
                self._reset(day)
            bucket = bucket_of(path)
            self._counts[bucket] += 1
            if bucket == "option_chain":
                self._oc.append((now, scrip))
                while self._oc and (now - self._oc[0][0]).total_seconds() >= OC_WINDOW_S:
                    self._oc.popleft()
                self._oc_peak = max(self._oc_peak, len({s for _, s in self._oc}))

    def limited(self) -> None:
        with self._lock:
            now = self._clock()
            if now.date() != self._day:
                self._reset(now.date())
            self._limited += 1
            if self._first_limited is None:
                self._first_limited = (sum(self._counts.values()), now.isoformat(timespec="seconds"))

    def snapshot(self) -> dict:
        with self._lock:
            now = self._clock()
            day = now.date()
            counts = dict(self._counts) if day == self._day else dict.fromkeys(BUCKETS, 0)
            first = self._first_limited if day == self._day else None
            since = self._started if self._started.date() == day else now.replace(
                hour=0, minute=0, second=0, microsecond=0,
            )
            return {"day": day.isoformat(), **counts, "total": sum(counts.values()),
                    "since_ist": since.isoformat(timespec="seconds"),
                    "rate_limited": self._limited if day == self._day else 0,
                    "first_rate_limited_total": first[0] if first else None,
                    "first_rate_limited_ist": first[1] if first else None,
                    "option_chain_peak_8s": self._oc_peak if day == self._day else 0}


COUNTER = DailyCounter()


def _scrip(request: httpx.Request) -> str | None:
    try:
        body = json.loads(request.content or b"null")
    except ValueError:
        return None
    scrip = body.get("UnderlyingScrip") if isinstance(body, dict) else None
    return str(scrip) if scrip is not None else None


def request_hook(request: httpx.Request) -> None:
    path = request.url.path
    COUNTER.count(path, _scrip(request) if bucket_of(path) == "option_chain" else None)


def response_hook(response: httpx.Response) -> None:
    """A counting failure never fails the Dhan call; a body-read error propagates
    unchanged, as without the hook (the adapter retries it).
    """
    if response.status_code == 429:
        COUNTER.limited()
        return
    response.read()
    try:
        if 200 <= response.status_code < 300 and len(response.content) > ERROR_BODY_MAX:
            return
        if error_code(response.json()) in RATE_CODES:
            COUNTER.limited()
    except Exception:  # noqa: BLE001 -- counting is best effort
        return
