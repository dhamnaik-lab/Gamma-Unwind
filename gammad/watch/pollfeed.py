"""PollFeed: a broker-neutral REST-polling `Feed` (BROKER-ADAPTER-SPEC §3 "PollFeed").

For adapters implementing `quotes(ids, segment) -> {id: Quote}` (feed_protocol.QuoteSource). Used for Groww (its
stream carries LTP only, G1), Nuvama (stream fields unverified, NV1), and as the fallback for any BETA broker whose
WebSocket decoder is not fixture-green by the freeze. The scanner then runs DEGRADED (capabilities.assess).

Each cycle (every `poll_s`, market hours only, first cycle immediately):
  * FULL and QUOTE ids -> `source.quotes(batch, segment)` in chunks of `batch_size` (the broker's documented
    batch). QUOTE ids are the underlyings: they go through quotes() too, NOT ltp(), so the underlying carries
    cumulative volume for the S1.3.0 direction gate's session VWAP (T-083, D89(4), D90(2), BROKER §3);
  * TICKER ids -> `source.ltp(batch, segment)` in chunks of `ltp_batch_size` (LTP only; volume stays None).
  The adapter's own TokenBuckets are the rate budget; a cycle that overruns `poll_s` is counted and logged once.
Per instrument a Tick is emitted only when ltp, volume_day, oi or the trade time changed. `ts_ms` = the quote's
trade time when present, else the receive time. A field the quote lacks repeats the last value seen; nothing is
ever invented (no OI for a broker that sends none). A quote whose cumulative volume or trade time went BACKWARDS
is a stale replica: dropped and counted (`stale`), never emitted.
New ids are mapped first (T-308, feed_protocol D2/D9): through the source's `resolve(segment, sid)` when it has
one, else its own `_native` (Nuvama) / `_row` (Groww) -- an id that maps to None (REJECT_UNMAPPED) or whose lookup
raised (REJECT_ERROR) is in rejected(), never in subscribed_ids(), and never counts for the cap.
Auth: a BrokerAuthError from the source parks the feed with on_auth_error(auth_code_for(exc), msg) -- 809 unless
the adapter set exc.code to "806".."809" -- until reload_credentials(creds), which also hands the creds to the
source's set_credentials when it has one. Any other error counts in `errors` and the cycle moves on.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass

from gammad.broker.base import BrokerAuthError
from gammad.core.session_clock import fo_market_hours as _market_hours
from gammad.log import get_logger
from gammad.watch.feed import _wall_ms
from gammad.watch.feed_protocol import (
    MODE_FULL,
    MODE_QUOTE,
    AuthErrorCallback,
    FeedBase,
    FeedCaps,
    Instrument,
    Quote,
    QuoteSource,
    Tick,
    auth_code_for,
    normalize_instruments,
)

log = get_logger(__name__)

_RANK = {"TICKER": 0, "QUOTE": 1, "FULL": 2}
_VIA_QUOTES = frozenset({MODE_FULL, MODE_QUOTE})  # T-083: underlyings (QUOTE) need volume -> quotes(), not ltp()


@dataclass(slots=True)
class _State:
    ltp: float | None = None
    volume: int | None = None
    oi: int | None = None
    ltt_ms: int | None = None
    emitted: tuple[float | None, int | None, int | None, int | None] | None = None


class PollFeed(FeedBase):
    def __init__(
        self,
        source: QuoteSource,
        out: asyncio.Queue[Tick],
        *,
        poll_s: float,
        batch_size: int,
        on_auth_error: AuthErrorCallback | None = None,
        max_instruments: int = 5000,
        ltp_batch_size: int | None = None,
        clock_ms: Callable[[], int] = _wall_ms,
        market_hours: Callable[[int], bool] = _market_hours,
    ) -> None:
        super().__init__(out, on_auth_error=on_auth_error)
        if poll_s <= 0 or batch_size <= 0:
            raise ValueError("poll_s and batch_size must be > 0")
        self.source = source
        self.caps = FeedCaps("poll", float(poll_s), int(max_instruments))
        self.poll_s = float(poll_s)
        self.batch_size = int(batch_size)
        self.ltp_batch_size = int(ltp_batch_size or batch_size)
        self._clock_ms = clock_ms
        self._market_hours = market_hours
        self.instruments: dict[tuple[str, int], str] = {}
        self._state: dict[tuple[str, int], _State] = {}
        self._task: asyncio.Task[None] | None = None
        self.cycles = 0
        self.overruns = 0
        self.errors = 0
        self.missing = 0
        self.stale = 0

    # ---------------------------------------------------------------- Feed protocol
    async def start(self, instruments: Iterable[Instrument] = ()) -> None:
        await self.add_instruments(instruments)
        if self._task is None:
            self._task = asyncio.create_task(self._run(), name="poll-feed")

    async def add_instruments(self, instruments: Iterable[Instrument]) -> int:
        upgraded: dict[tuple[str, int], str] = {}  # QUOTE -> FULL etc., applied only once the call fits (D9)
        new: dict[tuple[str, int], str] = {}
        for seg, sid, mode in normalize_instruments(instruments):
            key = (seg, sid)
            have = self.instruments.get(key)
            if have is not None:
                if _RANK[mode] > _RANK[upgraded.get(key, have)]:
                    upgraded[key] = mode
                continue
            if key not in new or _RANK[mode] > _RANK[new[key]]:
                new[key] = mode
        resolve = self._resolver()
        if new and resolve is not None:
            # T-308 (3), feed_protocol D2/D9: every NEW id is mapped first (off the loop, D8); an id the source cannot
            # map is rejected (REJECT_UNMAPPED / REJECT_ERROR), never carried, and does not count for the cap
            mapped = await self._resolve_off_loop(list(new), resolve, "poll feed")
            new = {key: mode for key, mode in new.items() if key in mapped}
        if len(self.instruments) + len(new) > self.caps.max_instruments:
            # 0.8.5 D9 (feed_protocol): refused BEFORE any change -- nothing added, no upgrade applied; the refused
            # ids are recorded as REJECT_CAP and the error carries .refused (call order) and .free
            refused = [(seg, sid, mode) for (seg, sid), mode in new.items()]
            raise self._capacity_error(len(self.instruments), refused, "poll feed")
        self.instruments.update(upgraded)
        self.instruments.update(new)
        self._accept(new)  # an id a later call accepts leaves rejected()
        return len(upgraded) + len(new)

    def _resolver(self) -> Callable[[str, int], object] | None:
        """The source's identity mapper (segment, security_id) -> native id or None: the public `resolve` hook when
        the source has one, else the adapter's own mapper its quotes() already uses (Nuvama `_native`, Groww
        `_row`). None = the source maps nothing up front (every id accepted, as before)."""
        for name in ("resolve", "_native", "_row"):
            fn = getattr(self.source, name, None)
            if callable(fn):
                return fn  # type: ignore[no-any-return]
        return None

    @property
    def max_tick_gap_s(self) -> float:
        """T-308 (4): the longest silence this feed shows in normal operation: two poll periods (a QuoteSource may
        leave an id absent on alternate cycles -- Nuvama's SLOW / SHED quotes each id every ~150 s at POLL_S 75 s).
        FeedAlarm alarms only past 1.5x this (never below its own 120 s)."""
        return 2 * self.poll_s

    async def stop(self) -> None:
        self._stop.set()
        self._auth_ok.set()  # wake a parked loop so it can exit
        if self._task is not None:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
            self._task = None

    def reload_credentials(self, creds: Mapping[str, str] | None = None) -> None:
        setter = getattr(self.source, "set_credentials", None)
        if creds and callable(setter):
            setter(dict(creds))
        self._unblock()

    # ---------------------------------------------------------------- polling
    def _groups(self, full: bool) -> dict[str, list[int]]:
        """`full=True`: the ids polled via quotes() (FULL + QUOTE); `full=False`: TICKER ids via ltp()."""
        out: dict[str, list[int]] = {}
        for (seg, sid), mode in self.instruments.items():
            if (mode in _VIA_QUOTES) == full:
                out.setdefault(seg, []).append(sid)
        return out

    async def poll_once(self) -> int:
        """One cycle over every subscribed instrument. Returns the number of Ticks emitted."""
        if self.auth_blocked:
            return 0
        self.cycles += 1
        emitted = 0
        for full, size in ((True, self.batch_size), (False, self.ltp_batch_size)):
            for seg, ids in self._groups(full).items():
                for i in range(0, len(ids), size):
                    batch = ids[i : i + size]
                    try:
                        if full:
                            got: Mapping[int, Quote] = await asyncio.to_thread(self.source.quotes, batch, seg)
                        else:
                            prices = await asyncio.to_thread(self.source.ltp, batch, seg)
                            got = {int(k): Quote(float(v), None, None, None) for k, v in prices.items()}
                    except BrokerAuthError as exc:
                        self._block(auth_code_for(exc), str(exc))
                        return emitted
                    except Exception as exc:  # noqa: BLE001 -- one bad batch must not kill the feed
                        self.errors += 1
                        log.warning(
                            "poll feed: %s batch of %d failed (%s: %s)", seg, len(batch), type(exc).__name__, exc
                        )
                        continue
                    recv = self._clock_ms()
                    for sid in batch:
                        q = got.get(sid)
                        if q is None:
                            self.missing += 1
                            continue
                        if self._on_quote(seg, sid, q, recv):
                            emitted += 1
        return emitted

    def _on_quote(self, seg: str, sid: int, q: Quote, recv_ms: int) -> bool:
        st = self._state.setdefault((seg, sid), _State())
        if q.volume_day is not None and st.volume is not None and int(q.volume_day) < st.volume:
            self.stale += 1
            return False
        if q.ltt_ms is not None and st.ltt_ms is not None and int(q.ltt_ms) < st.ltt_ms:
            self.stale += 1
            return False
        if q.ltp is not None:
            st.ltp = round(float(q.ltp), 2)
        if q.volume_day is not None:
            st.volume = int(q.volume_day)
        if q.oi is not None:
            st.oi = int(q.oi)
        if q.ltt_ms is not None:
            st.ltt_ms = int(q.ltt_ms)
        snap = (st.ltp, st.volume, st.oi, st.ltt_ms)
        if snap == st.emitted:
            return False
        st.emitted = snap
        tick = Tick(
            ts_ms=int(q.ltt_ms) if q.ltt_ms is not None else recv_ms,
            recv_ms=recv_ms,
            security_id=sid,
            segment=seg,
            ltp=st.ltp,
            volume_day=st.volume,
            oi=st.oi,
            prev_oi=None,
            prev_close=None,
            oi_day_high=None,
            oi_day_low=None,
        )
        return self._emit(tick)

    async def _run(self) -> None:
        while not self._stop.is_set():
            await self._auth_ok.wait()
            if self._stop.is_set():
                return
            started = time.monotonic()
            if self._market_hours(self._clock_ms()):
                await self.poll_once()
            elapsed = time.monotonic() - started
            if elapsed > self.poll_s:
                self.overruns += 1
                if self.overruns == 1:
                    log.warning(
                        "poll feed: one cycle took %.1f s > poll_s %.1f s (%d instruments); ticks arrive late",
                        elapsed,
                        self.poll_s,
                        len(self.instruments),
                    )
            await self._sleep(self.poll_s - elapsed)
