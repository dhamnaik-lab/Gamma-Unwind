"""Shared REST policy, independent of broker wire formats.

Adapters normalize documented vendor rate codes to BrokerError(code="429") and
body-level auth codes to BrokerAuthError / BrokerEntitlementError before returning
from send. Raw HTTP 403 means entitlement here; vendor exceptions to that rule
must likewise be normalized by the adapter. Responses are buffered (not streams).
Retry hook attempts are one-based, per outcome; jitter exponents start at zero.
"""

from __future__ import annotations

import asyncio
import math
import random
import threading
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC
from email.utils import parsedate_to_datetime
from enum import StrEnum
from typing import Protocol

import httpx

from gammad.broker.base import BrokerAuthError, BrokerEntitlementError, BrokerError


class Outcome(StrEnum):
    OK = "ok"
    RATE = "rate"
    TRANSIENT = "transient"
    AUTH = "auth"
    ENTITLEMENT = "entitlement"
    PERMANENT = "permanent"


def classify(status: int | None, exc: BaseException | None) -> Outcome:
    """Classify HTTP status or a normalized adapter exception; never inspect bodies."""
    if isinstance(exc, BrokerEntitlementError) or getattr(exc, "code", None) == "806":
        return Outcome.ENTITLEMENT
    if isinstance(exc, BrokerAuthError):
        return Outcome.AUTH
    if isinstance(exc, (httpx.TransportError, TimeoutError, ConnectionError)):
        return Outcome.TRANSIENT
    if isinstance(exc, BrokerError):
        code = str(exc.code)
        if code in ("807", "808", "809"):
            return Outcome.AUTH
        if code == "429":
            return Outcome.RATE
        status = exc.status if exc.status is not None else status
    elif exc is not None:
        return Outcome.PERMANENT
    if status in (401, 807, 808, 809):
        return Outcome.AUTH
    if status in (403, 806):
        return Outcome.ENTITLEMENT
    if status == 429:
        return Outcome.RATE
    if status in (500, 502, 503, 504):
        return Outcome.TRANSIENT
    if status is not None and 200 <= status < 300:
        return Outcome.OK
    return Outcome.PERMANENT


def parse_retry_after(value: str | None, now_s: float) -> float | None:
    """Parse delay-seconds or HTTP-date against a WALL clock, capped at 60 seconds."""
    if value is None:
        return None
    value = value.strip()
    if value.isascii() and value.isdigit():
        # Avoid converting an arbitrarily large header to an integer.
        seconds = float(value)
    else:
        try:
            date = parsedate_to_datetime(value)
            if date.tzinfo is None:
                date = date.replace(tzinfo=UTC)
            seconds = date.timestamp() - now_s
        except (ValueError, TypeError, OverflowError):
            return None
    if math.isnan(seconds) or seconds < 0:
        return None
    return min(seconds, 60.0)


@dataclass(frozen=True)
class RetryPolicy:
    transient_max: int = 3
    rate_max: int = 6
    base_s: float = 0.5
    cap_s: float = 8.0
    transient_cap_s: float = 10.0

    def __post_init__(self) -> None:
        if any(type(n) is not int or n < 0 for n in (self.transient_max, self.rate_max)):
            raise ValueError("retry budgets must be nonnegative integers")
        if any(not math.isfinite(n) or n < 0 for n in (self.base_s, self.cap_s, self.transient_cap_s)):
            raise ValueError("delays must be finite and nonnegative")

    def delay(self, outcome: Outcome, attempt: int, rng: random.Random) -> float:
        """Full jitter, with a zero-based retry exponent and outcome-specific cap."""
        if attempt < 0 or outcome not in (Outcome.RATE, Outcome.TRANSIENT):
            raise ValueError("delay needs a retryable outcome and nonnegative attempt")
        cap = self.cap_s if outcome == Outcome.RATE else self.transient_cap_s
        # Saturate before exponentiation to support arbitrarily large budgets.
        ceiling = self.base_s
        for _ in range(attempt):
            if ceiling >= cap or ceiling == 0:
                break
            ceiling *= 2
        return rng.uniform(0, min(cap, ceiling))


class CircuitBreaker:
    """Thread-safe admission; only transient failures accumulate while closed.

    Only OK resets the transient streak. In half-open, TRANSIENT reopens,
    OK/RATE close, and other outcomes release the probe without changing state.
    Call record exactly once per admission.
    """

    def __init__(self, fail_threshold: int = 5, open_s: float = 30.0,
                 clock: Callable[[], float] = time.monotonic) -> None:
        if type(fail_threshold) is not int or fail_threshold < 1:
            raise ValueError("fail_threshold must be a positive integer")
        if not math.isfinite(open_s) or open_s < 0:
            raise ValueError("open_s must be finite and nonnegative")
        self.fail_threshold = fail_threshold
        self.open_s = open_s
        self._clock = clock
        self._lock = threading.Lock()
        self._state = "closed"
        self._failures = 0
        self._until = 0.0
        self._probe = False

    def _refresh(self) -> None:
        if self._state == "open" and self._clock() >= self._until:
            self._state = "half-open"
            self._probe = False

    @property
    def state(self) -> str:
        with self._lock:
            self._refresh()
            return self._state

    def allow(self) -> bool:
        with self._lock:
            self._refresh()
            if self._state == "open" or (self._state == "half-open" and self._probe):
                return False
            if self._state == "half-open":
                self._probe = True
            return True

    def record(self, outcome: Outcome) -> None:
        with self._lock:
            if self._state == "open":
                return  # Late results from already admitted calls cannot close an open circuit.
            if self._state == "half-open":
                self._probe = False
                if outcome in (Outcome.OK, Outcome.RATE):
                    self._state = "closed"
                    if outcome == Outcome.OK:
                        self._failures = 0
                elif outcome == Outcome.TRANSIENT:
                    self._state, self._until = "open", self._clock() + self.open_s
                return
            if outcome == Outcome.OK:
                self._failures = 0
            elif outcome == Outcome.TRANSIENT:
                self._failures += 1
            if outcome == Outcome.TRANSIENT and self._failures >= self.fail_threshold:
                self._state, self._until = "open", self._clock() + self.open_s

    def _abandon(self) -> None:
        """Cancellation or bucket failure must not leave a half-open probe stuck."""
        with self._lock:
            if self._state == "half-open":
                self._probe = False


class _Bucket(Protocol):
    def acquire(self) -> float: ...


def _decide(response: httpx.Response | None, exc: Exception | None, *,
            policy: RetryPolicy, breaker: CircuitBreaker, counts: dict[Outcome, int],
            rng: random.Random, clock: Callable[[], float]) -> tuple[Outcome, int, float] | None:
    """The only retry decision path used by both send loops."""
    status = response.status_code if response is not None else None
    outcome = classify(status, exc)
    breaker.record(outcome)
    if outcome == Outcome.ENTITLEMENT:
        if isinstance(exc, BrokerEntitlementError):
            raise exc
        raise BrokerEntitlementError("data entitlement rejected", broker="broker",
                                     native=status or getattr(exc, "code", "806"),
                                     hint="Check the broker's market data entitlement.", status=status) from exc
    if outcome == Outcome.AUTH:
        if isinstance(exc, BrokerAuthError):
            raise exc
        raise BrokerAuthError("authentication rejected", str(getattr(exc, "code", None) or status),
                              status) from exc
    if outcome not in (Outcome.RATE, Outcome.TRANSIENT):
        return None
    used = counts.get(outcome, 0)
    limit = policy.rate_max if outcome == Outcome.RATE else policy.transient_max
    if used >= limit:
        return None
    counts[outcome] = used + 1
    delay = policy.delay(outcome, used, rng)
    if response is not None:
        after = parse_retry_after(response.headers.get("Retry-After"), clock())
        if after is not None:
            delay = max(after, delay)
    return outcome, used + 1, delay


def send_with_policy(send: Callable[[], httpx.Response], *, policy: RetryPolicy,
                     breaker: CircuitBreaker, bucket: _Bucket | None = None,
                     sleep: Callable[[float], None], rng: random.Random,
                     on_retry: Callable[[Outcome, int, float], None] | None = None,
                     clock: Callable[[], float] = time.time) -> httpx.Response:
    """Send buffered requests; return the final response or propagate the final exception."""
    counts: dict[Outcome, int] = {}
    while True:
        if not breaker.allow():
            raise BrokerError("circuit open", code="circuit_open")
        response, error = None, None
        try:
            if bucket is not None:
                bucket.acquire()
            try:
                response = send()
            except Exception as exc:  # noqa: BLE001 -- decision re-raises every non-retryable exception
                error = exc
        except BaseException:
            breaker._abandon()
            raise
        retry = _decide(response, error, policy=policy, breaker=breaker, counts=counts, rng=rng, clock=clock)
        if retry is None:
            if error is not None:
                raise error
            assert response is not None
            return response
        if response is not None:
            response.close()
        if on_retry is not None:
            on_retry(*retry)
        sleep(retry[2])


async def asend_with_policy(send: Callable[[], Awaitable[httpx.Response]], *, policy: RetryPolicy,
                           breaker: CircuitBreaker, bucket: _Bucket | None = None,
                           sleep: Callable[[float], Awaitable[None]], rng: random.Random,
                           on_retry: Callable[[Outcome, int, float], None] | None = None,
                           clock: Callable[[], float] = time.time) -> httpx.Response:
    """Async twin; the existing synchronous TokenBucket acquires off the event loop."""
    counts: dict[Outcome, int] = {}
    while True:
        if not breaker.allow():
            raise BrokerError("circuit open", code="circuit_open")
        response, error = None, None
        try:
            if bucket is not None:
                await asyncio.to_thread(bucket.acquire)
            try:
                response = await send()
            except Exception as exc:  # noqa: BLE001 -- same exception policy as the synchronous sender
                error = exc
        except BaseException:
            breaker._abandon()
            raise
        retry = _decide(response, error, policy=policy, breaker=breaker, counts=counts, rng=rng, clock=clock)
        if retry is None:
            if error is not None:
                raise error
            assert response is not None
            return response
        if response is not None:
            await response.aclose()
        if on_retry is not None:
            on_retry(*retry)
        await sleep(retry[2])
