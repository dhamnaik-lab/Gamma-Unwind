"""Broker-neutral live-feed contract (BROKER-ADAPTER-SPEC §3). Every broker feed implements `Feed`.

The shape is today's `FeedLike` seam (`FeedLike` in `gammad/app.py`) = `DhanFeed`'s public API
(`gammad/watch/feed.py`), except `reload_credentials` takes the broker's credential MAPPING
({env key: value}); Dhan's wrapper (`gammad/broker/dhan_entry.py`) maps it to (client_id, token).

Semantics every feed keeps (contract-tested by tests/broker_harness.py):
  * `Instrument = (segment, security_id, mode)`; segment + security_id are CANONICAL (NSE exchange token,
    `NSE_FNO` / `NSE_EQ` / `IDX_I`, gammad/broker/canonical.py). `mode` stays canonical: FULL = LTP +
    cumulative volume + OI + trade time (options), QUOTE = LTP (+volume) (underlyings); each feed maps to its
    own modes.
  * `add_instruments` adds (never removes) and returns how many pairs were new or upgraded (QUOTE -> FULL).
  * Output: normalized `Tick` (`Tick`, gammad/watch/feed.py) put on the queue with put_nowait;
    a full queue drops and counts.
  * `Tick.ts_ms` = exchange last-trade time as TRUE epoch ms when the packet carries one, else receive time
    (a 09:15:00 IST trade == 03:45:00 UTC). `recv_ms` = local clock. `volume_day` = CUMULATIVE day volume in
    QUANTITY (shares), never lots. `oi` = open interest, None when the broker never sends it (never
    fabricated). Fields a packet did not carry repeat the last value seen.
  * Auth: `on_auth_error(code, msg)` with Dhan's canonical codes -- 806 entitlement (log + park, no alert,
    D33), 807 expired, 808 invalid client, 809 auth failed (TOKEN alert + park until reload_credentials, D28).
    One callback per blocked episode; `auth_blocked` is True while parked. Each feed documents its
    native -> canonical code map in its module docstring.

  * OI 0 (0.8.5 D10): a tick whose `oi` is 0 carries NO OI update -- `_emit` sends it with oi=None, so the
    last known OI stands (OiCarry, BarBuilder); counted in `oi_zero_ignored`. A 0 must never read as an unwind.

What a feed REALLY carries (0.8.5 D2) -- read-only, cheap, safe to call from the event loop at any time:
  * `subscribed_ids()` -> frozenset[(segment, security_id)]: every instrument the feed accepted AND mapped to a
    native id; exactly the set every (re)connect subscribes, intraday additions included.
  * `pending_ids()`    -> frozenset[(segment, security_id)]: the subset not on a subscribed live socket yet (no
    socket / not yet subscribed / a live add whose send failed). The next (re)connect sends them. A poll feed
    has none.
  * `rejected()`       -> dict[(segment, security_id), reason]: ids the feed refused; reason REJECT_UNMAPPED
    (no identity-checked native id), REJECT_ERROR (the identity lookup raised) or REJECT_CAP (did not fit).
    An id accepted by a later call leaves the map. A refused id is ALSO logged, but the log is not the record.

Capacity (0.8.5 D9, uniform, the Dhan reference rule): `add_instruments` first maps every NEW id (unmappable ones
are rejected and do not count), then, if the mapped new ids exceed the free slots, raises `FeedCapacityError`
BEFORE changing anything -- nothing added, no QUOTE -> FULL upgrade applied, no message sent. The error carries
`.refused` (the call's new instruments, call order) and `.free` (slots left); the refused ids are in
`rejected()` as REJECT_CAP. The caller may retry with at most `.free` new ids. PollFeed follows the same rule
(`_capacity_error`: `.refused` + `.free`), and maps every new id through its source's mapper first (T-308: the
public `resolve` hook, else the adapter's own `_native` / `_row`): an unmappable id is rejected, never carried.

Identity lookups (0.8.5 D8) never run on the event loop: `FeedBase._resolve_off_loop` runs a whole call's
lookups in ONE worker thread (master files / REST checks block).

`FeedBase` carries the shared plumbing (queue + drop counter, the one-shot "LTT > 1 h off the receive clock"
WARNING that DhanFeed._emit (gammad/watch/feed.py) does for Dhan, auth park/unpark, the OI-0 guard, the D2 record).
`DhanFeed` keeps its own copy until the seam moves it; new feeds (PollFeed and the BETA brokers) inherit this one.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable

from gammad.log import get_logger
from gammad.watch.feed import (
    MODE_FULL,
    MODE_QUOTE,
    MODE_REQUEST,
    MODE_TICKER,
    AuthErrorCallback,
    Connector,
    FeedCapacityError,
    Instrument,
    Tick,
    WSLike,
    zero_oi_as_absent,
)

log = get_logger(__name__)

Key = tuple[str, int]  # (segment, security_id), canonical
REJECT_UNMAPPED = "unmapped"
REJECT_ERROR = "resolve_error"
REJECT_CAP = "cap"

__all__ = [
    "AUTH_ENTITLEMENT",
    "AUTH_EXPIRED",
    "AUTH_FAILED",
    "AUTH_INVALID_CLIENT",
    "BLOCKING_CODES",
    "MODE_FULL",
    "MODE_QUOTE",
    "MODE_TICKER",
    "REJECT_CAP",
    "REJECT_ERROR",
    "REJECT_UNMAPPED",
    "TOKEN_CODES",
    "AuthErrorCallback",
    "Connector",
    "Feed",
    "FeedBase",
    "FeedCapacityError",
    "FeedCaps",
    "Instrument",
    "Quote",
    "QuoteSource",
    "Tick",
    "WSLike",
    "auth_code_for",
    "ltt_offset_warning",
    "normalize_instruments",
]

FEED_KINDS = ("ws", "poll", "ws+poll")

AUTH_ENTITLEMENT = 806
AUTH_EXPIRED = 807
AUTH_INVALID_CLIENT = 808
AUTH_FAILED = 809
AUTH_TEXT: dict[int, str] = {
    AUTH_ENTITLEMENT: "Data API subscription required",
    AUTH_EXPIRED: "access token expired",
    AUTH_INVALID_CLIENT: "invalid client id",
    AUTH_FAILED: "authentication failed",
}
TOKEN_CODES = frozenset({AUTH_EXPIRED, AUTH_INVALID_CLIENT, AUTH_FAILED})  # TOKEN alert + park (D28)
BLOCKING_CODES = frozenset(AUTH_TEXT)  # park until reload_credentials()

LTT_SANITY_MS = 3_600_000  # one WARNING when a fresh trade's ts is > 1 h off the receive clock


@dataclass(frozen=True)
class FeedCaps:
    kind: str  # FEED_KINDS
    poll_s: float | None  # the poll period when kind has "poll", else None
    max_instruments: int

    def __post_init__(self) -> None:
        if self.kind not in FEED_KINDS:
            raise ValueError(f"FeedCaps.kind {self.kind!r} not in {FEED_KINDS}")
        if "poll" in self.kind and not (self.poll_s and self.poll_s > 0):
            raise ValueError("a polling feed needs poll_s > 0")
        if self.max_instruments <= 0:
            raise ValueError("max_instruments must be > 0")


@runtime_checkable
class Feed(Protocol):
    caps: FeedCaps

    @property
    def auth_blocked(self) -> bool: ...

    async def start(self, instruments: Iterable[Instrument] = ()) -> None: ...

    async def add_instruments(self, instruments: Iterable[Instrument]) -> int: ...

    async def stop(self) -> None: ...

    def reload_credentials(self, creds: Mapping[str, str]) -> None: ...

    def subscribed_ids(self) -> frozenset[tuple[str, int]]: ...

    def pending_ids(self) -> frozenset[tuple[str, int]]: ...

    def rejected(self) -> dict[tuple[str, int], str]: ...


@dataclass(frozen=True, slots=True)
class Quote:
    """One REST quote (PollFeed input, candle-less OI priming, master+quotes chains).
    ltt_ms = exchange last-trade time as TRUE epoch ms, None when the broker's quote carries none."""

    ltp: float | None
    volume_day: int | None
    oi: int | None
    ltt_ms: int | None


@runtime_checkable
class QuoteSource(Protocol):
    """An adapter that PollFeed can drive: canonical ids in, canonical ids out, missing ids absent."""

    def quotes(self, security_ids: Sequence[int], exchange_segment: str) -> dict[int, Quote]: ...

    def ltp(self, security_ids: Sequence[int], exchange_segment: str = "NSE_EQ") -> dict[int, float]: ...


def normalize_instruments(instruments: Iterable[Instrument]) -> list[Instrument]:
    """Validate modes and coerce ids to int; raises ValueError on an unknown mode
    (as DhanFeed.add_instruments does)."""
    out: list[Instrument] = []
    for seg, sid, mode in instruments:
        if mode not in MODE_REQUEST:
            raise ValueError(f"unknown feed mode {mode!r}")
        out.append((str(seg), int(sid), mode))
    return out


def auth_code_for(exc: BaseException) -> int:
    """A BrokerAuthError (REST) -> the canonical feed code. Adapters may set `exc.code` to "806".."809";
    anything else is 809 (authentication failed), which alerts and parks like 807."""
    code = getattr(exc, "code", None)
    try:
        n = int(str(code))
    except (TypeError, ValueError):
        return AUTH_FAILED
    return n if n in BLOCKING_CODES else AUTH_FAILED


def ltt_offset_warning(t: Tick) -> str | None:
    """The WARNING text when a traded tick's ts is > 1 h from its receive clock (an epoch/IST mix-up), else None."""
    if t.ts_ms == t.recv_ms or not t.volume_day:
        return None
    if abs(t.ts_ms - t.recv_ms) <= LTT_SANITY_MS:
        return None
    return (
        f"feed: trade-time ts {t.ts_ms} is {(t.ts_ms - t.recv_ms) / 3.6e6:.1f} h off receive clock "
        f"{t.recv_ms} -- check the broker's epoch/IST rule"
    )


class FeedBase:
    """Shared feed plumbing. Subclasses set `caps` and implement start/add_instruments/stop."""

    caps: FeedCaps

    def __init__(self, out: asyncio.Queue[Tick], *, on_auth_error: AuthErrorCallback | None = None) -> None:
        self.out = out
        self.on_auth_error = on_auth_error
        self._auth_ok = asyncio.Event()
        self._auth_ok.set()
        self._stop = asyncio.Event()
        self.last_block: tuple[int, str] | None = None
        self.dropped = 0
        self.emitted = 0
        self.oi_zero_ignored = 0
        self.idle_reconnects = 0
        self._ltt_checked = False
        self._rejected: dict[Key, str] = {}

    @property
    def auth_blocked(self) -> bool:
        return not self._auth_ok.is_set()

    # ---------------------------------------------------------------- what the feed carries (D2)
    def _carried(self) -> Iterable[Key]:
        """The accepted, mapped keys. Default: the `instruments` map PollFeed keeps; WS feeds override."""
        instruments = getattr(self, "instruments", None)
        return instruments.keys() if isinstance(instruments, Mapping) else ()

    def subscribed_ids(self) -> frozenset[Key]:
        return frozenset(self._carried())

    def pending_ids(self) -> frozenset[Key]:
        return frozenset()

    def rejected(self) -> dict[Key, str]:
        return dict(self._rejected)

    def _reject(self, key: Key, reason: str) -> None:
        self._rejected[key] = reason

    def _accept(self, keys: Iterable[Key]) -> None:
        for key in keys:
            self._rejected.pop(key, None)

    def _capacity_error(self, have: int, new: Sequence[Instrument], who: str) -> FeedCapacityError:
        """The uniform D9 refusal (module doc): record every new id as REJECT_CAP; the caller raises BEFORE any
        change."""
        free = max(0, self.caps.max_instruments - have)
        for seg, sid, _mode in new:
            self._rejected[(seg, sid)] = REJECT_CAP
        return FeedCapacityError(
            f"{who}: {len(new)} new instruments, capacity left {free}", refused=new, free=free
        )

    async def _resolve_off_loop(
        self, keys: Sequence[Key], resolve: Callable[[str, int], Any], who: str
    ) -> dict[Key, Any]:
        """D8: every key's identity lookup in ONE worker thread, never on the event loop. Returns {key: native};
        a key whose lookup returned None (REJECT_UNMAPPED) or raised (REJECT_ERROR) is recorded in rejected(),
        logged at ERROR, and left out."""
        if not keys:
            return {}

        def run() -> tuple[dict[Key, Any], dict[Key, str]]:
            ok: dict[Key, Any] = {}
            bad: dict[Key, str] = {}
            for seg, sid in keys:
                try:
                    native = resolve(seg, sid)
                except Exception as e:  # noqa: BLE001 -- one failed lookup must not fail the whole add
                    log.warning("%s: identity lookup for %s %d raised %s", who, seg, sid, type(e).__name__)
                    bad[(seg, sid)] = REJECT_ERROR
                    continue
                if native is None:
                    bad[(seg, sid)] = REJECT_UNMAPPED
                else:
                    ok[(seg, sid)] = native
            return ok, bad

        ok, bad = await asyncio.to_thread(run)
        for (seg, sid), why in bad.items():
            self._reject((seg, sid), why)
            log.error("%s: %s %d has no identity-checked native id (%s); not subscribed (never substituted)",
                      who, seg, sid, why)
        return ok

    def _emit(self, t: Tick) -> bool:
        absent = zero_oi_as_absent(t)
        if absent is not None:
            self.oi_zero_ignored += 1
            if self.oi_zero_ignored == 1:
                log.warning(
                    "feed: OI 0 on %s %d treated as no OI update (the last known OI stands); "
                    "every one is counted in oi_zero_ignored",
                    t.segment,
                    t.security_id,
                )
            t = absent
        if not self._ltt_checked and t.ts_ms != t.recv_ms and t.volume_day:
            self._ltt_checked = True
            warning = ltt_offset_warning(t)
            if warning:
                log.warning("%s", warning)
        try:
            self.out.put_nowait(t)
        except asyncio.QueueFull:
            self.dropped += 1
            return False
        self.emitted += 1
        return True

    def _block(self, code: int, reason: str) -> None:
        if not self._auth_ok.is_set():
            return  # already parked; one callback per episode
        self._auth_ok.clear()
        self.last_block = (code, reason)
        log.error("feed: auth/entitlement failure %d (%s); parked until reload_credentials()", code, reason)
        if self.on_auth_error is not None:
            try:
                self.on_auth_error(code, reason)
            except Exception:
                log.exception("feed: on_auth_error callback raised")

    def _unblock(self) -> None:
        if not self._auth_ok.is_set():
            log.info("feed: credentials reloaded, resuming")
        self.last_block = None
        self._auth_ok.set()

    async def _sleep(self, seconds: float) -> None:
        """Sleep that returns early on stop()."""
        try:
            await asyncio.wait_for(self._stop.wait(), timeout=max(0.0, seconds))
        except TimeoutError:
            pass
