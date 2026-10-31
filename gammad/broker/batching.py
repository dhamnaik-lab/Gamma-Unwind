"""Id batching with a halving fallback (T-159; D125 K3 as narrowed by D129).

The GET-URL batchers whose per-request id limit is undocumented or unverified (Kotak path segment, Zerodha `i=`
query, FYERS `symbols=`) send their first try at the measured / documented size; when the broker answers HTTP 400
to a batch of more than one id, that slice is halved and retried, and the smaller size is kept for the rest of the
call. The adapter keeps it for the session through `on_shrink` (or the returned size). One WARNING per shrink.

Never retried: an auth error (BrokerAuthError and its broker subclasses -- the adapters raise those BEFORE the
status is looked at, so a 400 that means "bad token" is never halved), any non-400 error (5xx: only the
adapter's own transport retries, plus partial mode's one re-send below), a rate limit (status 429 or code "429",
`is_rate_limit`: never halved, never re-sent, audit BROKER-01) and a 400 on a single id (nothing left to halve: the
caller sees the broker's answer).

0.8.5 (adapter audit D5) `partial=True` -- THE shared multi-batch `ltp()` contract, used by every adapter whose
ltp() can take more than one request (Angel, FYERS, Groww, Kotak, Nuvama, Upstox, Zerodha):
  - a batch that fails for a NON-auth reason (after the adapter's own transport retries) is sent ONCE more; the
    retry goes through `call` again, so it waits on the adapter's own rate bucket (its pacing, nothing extra);
  - EXCEPT a rate limit (audit BROKER-01, every broker): the refused batch is never re-sent and NO further batch is
    sent in that call; the ids read so far are returned, the rest are absent (or, when nothing was read, the 429 is
    raised as below) -- one breach per call, not one per batch;
  - a batch that fails twice is GIVEN UP: its ids are simply absent from the result (one WARNING), the batches
    already read and the ones after it are kept. The caller (mark_levels) counts an absent id as stale and refuses
    the run only above its MAX_STALE_PCT;
  - an auth error (BrokerAuthError, incl. entitlement 806) still raises at once: it is not a partial failure;
  - when NOTHING was read (every batch given up) the last error is raised (`raise_if_none`, default True), so a
    total outage reads as a failed call exactly as a one-request broker's (Dhan) does, never as an empty dict.
`halve=False` keeps a 400 from halving (the batchers whose size is documented: Angel, Groww, Upstox, Nuvama).
"""

from __future__ import annotations

from collections.abc import Callable, Sequence

from gammad.broker.base import BrokerAuthError, BrokerError
from gammad.log import get_logger

log = get_logger(__name__)


HALVE_ON_STATUS = 400
RATE_LIMIT_STATUS = 429  # audit BROKER-01: never re-tried (FYERS F7: > 3 breaches a minute blocks the day)


def is_rate_limit(exc: BrokerError) -> bool:
    """A rate-limit refusal in either shape an adapter raises it: HTTP status 429 or broker code "429"."""
    return exc.status == RATE_LIMIT_STATUS or str(exc.code) == str(RATE_LIMIT_STATUS)


def batched[T, R](
    ids: Sequence[T],
    size: int,
    call: Callable[[list[T]], R],
    *,
    on_shrink: Callable[[int, int], None] | None = None,
    broker: str = "broker",
    partial: bool = False,
    halve: bool = True,
    raise_if_none: bool = True,
) -> tuple[list[R], int]:
    """Run `call(batch)` over consecutive slices of `ids` of at most `size`; -> (results in slice order, the
    working size). A slice refused with HTTP 400 is halved (never below 1, `halve`) and retried from the same
    position. `partial` = the multi-batch ltp contract (module docstring): retry a failed slice once, then skip it."""
    items = list(ids)
    size = max(1, int(size))
    results: list[R] = []
    pos = 0
    retried = False  # the slice at `pos` has had its one partial-mode retry
    given_up: list[T] = []
    last: BrokerError | None = None
    while pos < len(items):
        batch = items[pos : pos + size]
        try:
            out = call(batch)
        except BrokerAuthError:
            raise
        except BrokerError as exc:
            limited = is_rate_limit(exc)
            if halve and not limited and exc.status == HALVE_ON_STATUS and len(batch) > 1:
                new = max(1, len(batch) // 2)
                log.warning("%s: quote batch of %d ids refused (HTTP 400); batch size %d -> %d for this session",
                            broker, len(batch), size, new)  # fmt: skip
                if on_shrink is not None:
                    on_shrink(size, new)
                size = new
                continue
            if not partial:
                raise
            last = exc
            if limited:  # audit BROKER-01: no re-send AND no further batch in this call; the rest are left out
                log.warning("%s: rate limited (429) at id %d of %d; no further batch sent in this call", broker,
                            pos + 1, len(items))  # fmt: skip
                given_up.extend(items[pos:])
                break
            if not retried:
                retried = True
                continue  # ONE more try; `call` waits on the adapter's own bucket again
            given_up.extend(batch)
            retried = False
            pos += len(batch)
            continue
        results.append(out)
        retried = False
        pos += len(batch)
    if given_up and last is not None:
        if not results and raise_if_none:
            raise last
        log.warning("%s: %d of %d ids left out after a failed batch (tried twice): %s", broker, len(given_up),
                    len(items), last)  # fmt: skip
    return results, size
