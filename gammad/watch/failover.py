"""WebSocket silence fallback; poll emissions cannot supersede newer forwarded data.

Build the primary on a private queue, and subscribe through this wrapper so modes
are known. PollFeed receives a queue facade over the caller's output queue: its
put_nowait runs the same dedupe guard as primary ticks, without a second buffer.
"""

import asyncio
from collections.abc import Callable, Iterable, Mapping
from dataclasses import astuple

from gammad.log import get_logger
from gammad.watch.feed import safe_put_tick
from gammad.watch.feed_protocol import Feed, Instrument, QuoteSource, Tick, normalize_instruments
from gammad.watch.pollfeed import PollFeed

log = get_logger(__name__)
_RANK = {"TICKER": 0, "QUOTE": 1, "FULL": 2}


class _PollOutput(asyncio.Queue):
    """PollFeed only uses put_nowait; forward synchronously to the shared sink."""

    def __init__(self, owner):
        super().__init__()
        self.owner = owner

    def put_nowait(self, tick):
        self.owner._forward(tick, from_poll=True)


class FailoverFeed:
    def __init__(
        self, primary: Feed, source: QuoteSource, out: asyncio.Queue[Tick], *,
        silent_s=45.0, healthy_s=30.0, poll_s=5.0, batch_size: int,
        clock_ms: Callable[[], int], market_hours: Callable[[int], bool], make_poll=None,
    ):
        if min(silent_s, healthy_s, poll_s, batch_size) <= 0:
            raise ValueError("durations and batch_size must be positive")
        if primary.out is out:
            raise ValueError("primary must have a private queue")
        self.primary, self.source, self.out = primary, source, out
        self.caps = primary.caps
        self.on_auth_error = primary.on_auth_error
        self._clock_ms, self._market_hours = clock_ms, market_hours
        self.silent_ms, self.healthy_ms = silent_s * 1000, healthy_s * 1000
        self._make_poll = make_poll or PollFeed
        self._poll_kwargs = {
            "poll_s": poll_s, "batch_size": batch_size, "clock_ms": clock_ms,
            "market_hours": market_hours, "on_auth_error": self.on_auth_error,
            "max_instruments": self.caps.max_instruments,
        }
        self._poll_out = _PollOutput(self)
        self._poll = None
        self._cycles = 0
        self._modes = {}
        self._latest = {}
        self._primary_data = {}
        self._last_fresh_ms = None
        self._tasks = []
        self._lock = asyncio.Lock()
        self._running = False
        self._stopped = False
        self._armed_ms = None
        self._healthy_from = None
        self.last_primary_ms = None
        self.mode = "ws"
        self.failovers = self.dupes_dropped = self.dropped = 0
        self.supervisor_errors = self.bad_items = 0

    @property
    def auth_blocked(self):
        return self.primary.auth_blocked or bool(self._poll and self._poll.auth_blocked)

    def subscribed_ids(self):
        return self.primary.subscribed_ids()

    def pending_ids(self):
        return self.primary.pending_ids()

    def rejected(self):
        return self.primary.rejected()

    def reload_credentials(self, creds: Mapping[str, str]):
        self.primary.reload_credentials(creds)
        if self._poll is not None:
            self._poll.reload_credentials(creds)

    def _remember(self, instruments):
        accepted = self.subscribed_ids() | self.pending_ids()
        for seg, sid, mode in instruments:
            key = seg, sid
            if key in accepted and _RANK[mode] >= _RANK.get(self._modes.get(key), -1):
                self._modes[key] = mode

    def _instruments(self):
        ids = self.subscribed_ids() | self.pending_ids()
        if ids - self._modes.keys():
            raise ValueError("supply existing primary subscriptions to start() to establish their modes")
        return [(seg, sid, self._modes[(seg, sid)]) for seg, sid in sorted(ids)]

    async def start(self, instruments: Iterable[Instrument] = ()):
        async with self._lock:
            items = normalize_instruments(instruments)
            await self.primary.start(items)
            self._remember(items)
            # Existing subscriptions must be supplied again to establish their modes.
            self._instruments()
            if self._running:
                if self._poll is not None:
                    await self._poll.add_instruments(self._instruments())
                return
            self._running = True
            self._stopped = False
            now = self._clock_ms()
            self._armed_ms = now if self._market_hours(now) else None
            self._tasks = [asyncio.create_task(self._read_primary()), asyncio.create_task(self._supervise())]

    async def add_instruments(self, instruments: Iterable[Instrument]):
        async with self._lock:
            items = normalize_instruments(instruments)
            count = await self.primary.add_instruments(items)
            self._remember(items)
            if self._poll is not None:
                await self._poll.add_instruments(self._instruments())
            return count

    def _forward(self, tick, *, from_poll=False):
        key = tick.segment, tick.security_id
        # Receive time is not market data; repeated quotes may have a new receive clock.
        snapshot = astuple(tick)
        snapshot = snapshot[:1] + snapshot[2:]
        previous = self._latest.get(key)
        older_poll = from_poll and self.mode == "poll" and previous is not None and tick.ts_ms < previous[0]
        if previous is not None and (older_poll or snapshot == previous[1]):
            self.dupes_dropped += 1
            return
        ok, drop_reason = safe_put_tick(self.out, tick)
        if not ok:
            self.dropped += 1
            if drop_reason and drop_reason != "queue_full" and (self.dropped == 1 or self.dropped % 100 == 0):
                log.warning("failover: tick held (%s) on %s %d (total dropped=%d)", drop_reason, tick.segment,
                            tick.security_id, self.dropped)
            return
        # Primary receive-stamped OI and exchange-stamped quotes can arrive out of
        # timestamp order. Forward them, but never lower the poll watermark.
        self._latest[key] = max(tick.ts_ms, previous[0]) if previous else tick.ts_ms, snapshot

    def _primary_tick(self, tick):
        if not isinstance(tick, Tick):
            raise TypeError("primary queue item must be a Tick")
        now = self._clock_ms()
        key = tick.segment, tick.security_id
        fields = tick.ltp, tick.volume_day, tick.oi
        prior = self._primary_data.get(key)
        fresh = prior is None or fields != prior[1] or tick.ts_ms > prior[0]
        self._forward(tick)
        self._primary_data[key] = max(tick.ts_ms, prior[0]) if prior else tick.ts_ms, fields
        self.last_primary_ms = now
        if fresh:
            if (self._last_fresh_ms is None or now - self._last_fresh_ms >= self.silent_ms
                    or self._healthy_from is None):
                self._healthy_from = now
            self._last_fresh_ms = now

    async def _read_primary(self):
        while True:
            tick = await self.primary.out.get()
            try:
                self._primary_tick(tick)
            except Exception:
                self.bad_items += 1
                log.exception("failover: bad primary item; continuing reader")
            finally:
                self.primary.out.task_done()

    async def _stop_poll(self):
        if self._poll is not None:
            await self._poll.stop()
            self._cycles += self._poll.cycles
            self._poll = None
        if self.mode == "poll":
            self.mode = "ws"
            log.info("failover: returned to primary feed")
        self._healthy_from = None

    async def _check_once(self):
        async with self._lock:
            if not self._running:
                return
            now = self._clock_ms()
            if not self._market_hours(now) or self.primary.auth_blocked:
                self._armed_ms = None
                await self._stop_poll()
                return
            if self._armed_ms is None:
                self._armed_ms = now
            last = self.last_primary_ms if self.last_primary_ms is not None else self._armed_ms
            baseline = max(self._armed_ms, last)
            if self._poll is None and now - baseline >= self.silent_ms:
                poll = self._make_poll(self.source, self._poll_out, **self._poll_kwargs)
                try:
                    await poll.start(self._instruments())
                except BaseException:
                    await poll.stop()
                    raise
                self._poll = poll
                self.mode = "poll"
                self.failovers += 1
                self._healthy_from = None
                log.info("failover: primary silent; polling quotes")
            elif self._poll is not None:
                if self._last_fresh_ms is None or now - self._last_fresh_ms >= self.silent_ms:
                    self._healthy_from = None
                # Recovery must be evidenced by ticks spanning healthy_s, not a timer
                # running after a single packet. Gaps >= silent_s break continuity.
                if (self._healthy_from is not None and self._last_fresh_ms is not None
                        and self._last_fresh_ms - self._healthy_from >= self.healthy_ms):
                    await self._stop_poll()

    async def _supervise(self):
        while True:
            await asyncio.sleep(1)
            try:
                await self._check_once()
            except Exception:
                self.supervisor_errors += 1
                log.exception("failover: supervisor pass failed; retrying next pass")

    def status(self) -> dict:
        return {
            "mode": self.mode, "failovers": self.failovers, "last_primary_ms": self.last_primary_ms,
            "poll_cycles": self._cycles + (self._poll.cycles if self._poll else 0),
            "dupes_dropped": self.dupes_dropped,
            "supervisor_errors": self.supervisor_errors, "bad_items": self.bad_items,
        }

    async def stop(self):
        async with self._lock:
            if self._stopped:
                return
            self._running = False
            for task in self._tasks:
                task.cancel()
            await asyncio.gather(*self._tasks, return_exceptions=True)
            self._tasks = []
            try:
                await self.primary.stop()
            finally:
                await self._stop_poll()
            self._stopped = True
