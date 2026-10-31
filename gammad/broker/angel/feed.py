"""Angel One SmartStream v2 live feed (BROKER-ADAPTER-SPEC §3). Wire layout + evidence: `wire.py`.

One WebSocket (<= 1000 tokens, each mode counted separately, A3). Canonical FULL -> SNAP_QUOTE (LTP + day
volume + OI + exchange time), QUOTE -> QUOTE (LTP + day volume), TICKER -> LTP. A QUOTE -> FULL upgrade
unsubscribes the QUOTE mode first, so a token never occupies two of the 1000 slots.

Tick: ts_ms = `exchange_timestamp` (epoch ms, A2) when > 0, else the receive clock; ltp = paise / 100;
volume_day = `volume_trade_for_the_day` (QUANTITY; UNCONFIRMED live, wire.py); oi = `open_interest` (SNAP_QUOTE
only; a QUOTE-mode instrument never gets OI); fields a packet did not carry repeat the last value seen.
prev_oi / prev_close / oi_day_high / oi_day_low are None (the stream carries no previous-day OI).

Identity: canonical (segment, token) -> (exchangeType, Angel token) through the adapter's identity-checked
`native()` map when `make_feed` gets the adapter (the app passes it); without one, NSE_FNO / NSE_EQ tokens are used
as-is (Angel token == NSE exchange token, spec §1 measured) and IDX_I is refused (index tokens differ). A contract
with no native row is dropped with an ERROR, never substituted. Frames for tokens never subscribed are ignored.

Native -> canonical auth code map (FeedBase: 806 entitlement, 807 expired, 808 invalid client, 809 failed):
    handshake HTTP 401 / 403            -> 809 (header auth refused: Authorization / x-api-key / x-client-code /
                                            x-feed-token, smartWebSocketV2.py L289-294)
    JSON text frame errorCode AG8002 / AB1010 (token / session expired)            -> 807
    JSON text frame errorCode AG8001 / AG8003 / AB8050 / AB8051 (invalid / missing) -> 809
    ANGEL_FEED_TOKEN or ANGEL_ACCESS_TOKEN empty                                    -> 809 without connecting
Any other text frame ("pong", subscription errors) is logged, never a block.

Heartbeat (0.8.3): a timer task sends the text "ping" every `heartbeat_s` (W.HEARTBEAT_S = 10 s) REGARDLESS of
incoming traffic, as the official SDK does (BROKER-API-RESEARCH.md row A2, smartWebSocketV2.py L17-18). Before
0.8.3 the ping went out only after 10 s with no data, i.e. never on a busy market stream. The timer is per socket:
cancelled before the socket closes, restarted on reconnect. A drop logs the close code / reason / closing side and
a per-session reconnect count (`reconnects`).
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import time
from collections.abc import Awaitable, Callable, Iterable, Mapping
from typing import Any

from gammad.broker.angel import wire as W
from gammad.broker.angel.adapter import KEY_API, KEY_CLIENT, KEY_FEED, KEY_TOKEN, strip_bearer
from gammad.broker.canonical import SEG_EQ, SEG_OPT
from gammad.core.session_clock import fo_market_hours as _market_hours
from gammad.log import get_logger
from gammad.watch.feed_protocol import (
    AUTH_EXPIRED,
    AUTH_FAILED,
    AuthErrorCallback,
    Connector,
    FeedBase,
    FeedCaps,
    Instrument,
    Tick,
    WSLike,
    normalize_instruments,
)

log = get_logger(__name__)

_MODE_RANK = {"TICKER": 0, "QUOTE": 1, "FULL": 2}
NativeKey = tuple[int, str]  # (exchangeType, token)
Resolver = Callable[[str, int], NativeKey | None]


def _wall_ms() -> int:
    return int(time.time() * 1000)


def _http_status(exc: BaseException) -> int | None:
    resp = getattr(exc, "response", None)
    code = getattr(resp, "status_code", None) or getattr(exc, "status_code", None)
    return int(code) if isinstance(code, int) else None


def _drop_detail(exc: BaseException) -> str:
    """The drop reason for the log: a websockets ConnectionClosed gives the close frame -- `.rcvd` is the server's,
    `.sent` ours (e.g. the library's keepalive timeout); `.code` / `.reason` are deprecated in websockets 13+."""
    name = type(exc).__name__
    if not hasattr(exc, "rcvd"):
        text = str(exc)[:160]
        return f"{name}: {text}" if text else name
    rcvd, sent = getattr(exc, "rcvd", None), getattr(exc, "sent", None)
    if rcvd is None and sent is None:
        return f"{name} no close frame (abnormal 1006)"
    frame, side = (rcvd, "server") if rcvd is not None else (sent, "client")
    return f"{name} code={getattr(frame, 'code', None)} reason={getattr(frame, 'reason', '')!r} from={side}"


def default_resolver(segment: str, security_id: int) -> NativeKey | None:
    """No adapter: NSE/NFO tokens are the canonical tokens (spec §1); indices need Angel's master."""
    if segment == SEG_OPT:
        return (W.EXCHANGE_TYPE["NFO"], str(int(security_id)))
    if segment == SEG_EQ:
        return (W.EXCHANGE_TYPE["NSE"], str(int(security_id)))
    return None


def adapter_resolver(adapter: Any) -> Resolver:
    def resolve(segment: str, security_id: int) -> NativeKey | None:
        try:
            exch, token = adapter.native(segment, int(security_id))
        except KeyError:
            return None
        return (W.EXCHANGE_TYPE[exch], str(token))

    return resolve


class AngelFeed(FeedBase):
    caps = FeedCaps("ws", None, W.MAX_FEED_TOKENS)

    def __init__(
        self,
        creds: Mapping[str, str],
        out: asyncio.Queue[Tick],
        *,
        on_auth_error: AuthErrorCallback | None = None,
        connect: Connector | None = None,
        clock_ms: Callable[[], int] = _wall_ms,
        resolve: Resolver = default_resolver,
        heartbeat_s: float = W.HEARTBEAT_S,
        backoff_min_s: float = 1.0,
        backoff_max_s: float = 30.0,
        market_hours: Callable[[int], bool] = _market_hours,
        idle_timeout_s: float = 60.0,
    ) -> None:
        super().__init__(out, on_auth_error=on_auth_error)
        self._creds = {k: (creds.get(k) or "").strip() for k in (KEY_API, KEY_CLIENT, KEY_TOKEN, KEY_FEED)}
        self._connect_fn = connect
        self._clock_ms = clock_ms
        self._resolve = resolve
        self._market_hours = market_hours
        self.idle_timeout_s = idle_timeout_s  # D13: no DATA frame this long in market hours -> reconnect
        self._synced = False  # the current socket got the full subscribe
        self._unsent: set[tuple[str, int]] = set()  # live adds whose send failed
        self.heartbeat_s = heartbeat_s
        self.backoff_min_s = backoff_min_s
        self.backoff_max_s = backoff_max_s
        self._modes: dict[tuple[str, int], str] = {}
        self._native: dict[tuple[str, int], NativeKey] = {}
        self._canon: dict[NativeKey, tuple[str, int]] = {}
        self._state: dict[tuple[str, int], list[Any]] = {}  # [ltp, volume, oi]
        self._ws: WSLike | None = None
        self._task: asyncio.Task[None] | None = None
        self._started = False
        self._corr = 0
        self.reconnects = 0  # socket drops this session (each one is followed by a reconnect)
        self._ping_sleep: Callable[[float], Awaitable[Any]] = asyncio.sleep  # the heartbeat timer; tests swap it

    # ---------------------------------------------------------------- public
    async def start(self, instruments: Iterable[Instrument] = ()) -> None:
        self._started = True
        await self.add_instruments(instruments)
        self._spawn()

    def subscribed_ids(self) -> frozenset[tuple[str, int]]:
        return frozenset(self._modes)

    def pending_ids(self) -> frozenset[tuple[str, int]]:
        return frozenset(self._modes) if not self._synced else frozenset(self._unsent & self._modes.keys())

    async def add_instruments(self, instruments: Iterable[Instrument]) -> int:
        """Map every new id first in a worker thread (D8: `adapter.native()` may download / parse the master),
        refuse atomically across the cap (feed_protocol D9), then commit and send only the changes."""
        ups: dict[tuple[str, int], str] = {}
        fresh: dict[tuple[str, int], str] = {}
        for seg, sid, mode in normalize_instruments(instruments):
            key = (seg, sid)
            have = self._modes.get(key)
            if have is not None:
                if _MODE_RANK[mode] > _MODE_RANK[ups.get(key, have)]:
                    ups[key] = mode
                continue
            if key not in fresh or _MODE_RANK[mode] > _MODE_RANK[fresh[key]]:
                fresh[key] = mode
        natives = await self._resolve_off_loop(list(fresh), self._resolve, "angel feed")
        new = [(k[0], k[1], m) for k, m in fresh.items() if k in natives]
        if len(self._modes) + len(new) > self.caps.max_instruments:
            raise self._capacity_error(len(self._modes), new, "angel feed")
        upgraded = [(k[0], k[1], self._modes[k], m) for k, m in ups.items()]
        for seg, sid, _old, mode in upgraded:
            self._modes[(seg, sid)] = mode
        for seg, sid, mode in new:
            self._native[(seg, sid)] = natives[(seg, sid)]
            self._modes[(seg, sid)] = mode
            self._canon[self._native[(seg, sid)]] = (seg, sid)
        self._accept((s, i) for s, i, _m in new)
        ws = self._ws
        if ws is not None:
            try:
                for seg, sid, old, _ in upgraded:
                    await ws.send(self._message(0, old, [(seg, sid)]))
                for msg in self._subscribe_all([(s, i, m) for s, i, m in new] + [(s, i, m) for s, i, _, m in upgraded]):
                    await ws.send(msg)
            except Exception as e:  # noqa: BLE001 -- the reconnect path resubscribes the full set
                self._unsent.update((s, i) for s, i, _m in new)
                self._unsent.update((s, i) for s, i, _o, _m in upgraded)
                log.warning("angel feed: live add failed (%s); will resubscribe on reconnect", type(e).__name__)
        elif self._started and new:
            self._spawn()
        return len(new) + len(upgraded)

    def reload_credentials(self, creds: Mapping[str, str] | None = None) -> None:
        for k in (KEY_API, KEY_CLIENT, KEY_TOKEN, KEY_FEED):
            v = ((creds or {}).get(k) or "").strip()
            if v:
                self._creds[k] = v
        self._unblock()

    async def stop(self) -> None:
        self._stop.set()
        self._auth_ok.set()
        ws = self._ws
        if ws is not None:
            with contextlib.suppress(Exception):
                await ws.close()
        if self._task is not None:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)

    # ---------------------------------------------------------------- internals
    def _spawn(self) -> None:
        if self._task is None and self._modes:
            self._task = asyncio.create_task(self._run(), name="angel-feed")

    def _message(self, action: int, mode: str, items: Iterable[tuple[str, int]]) -> str:
        by_exch: dict[int, list[str]] = {}
        for seg, sid in items:
            ex, tok = self._native[(seg, sid)]
            by_exch.setdefault(ex, []).append(tok)
        self._corr += 1
        return W.subscribe_message(action, W.STREAM_MODE[mode], by_exch, f"gamma{self._corr:05d}")

    def _subscribe_all(self, items: Iterable[tuple[str, int, str]]) -> list[str]:
        by_mode: dict[str, list[tuple[str, int]]] = {}
        for seg, sid, mode in items:
            by_mode.setdefault(mode, []).append((seg, sid))
        return [self._message(1, mode, keys) for mode, keys in sorted(by_mode.items())]

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {strip_bearer(self._creds[KEY_TOKEN])}",
            "x-api-key": self._creds[KEY_API],
            "x-client-code": self._creds[KEY_CLIENT],
            "x-feed-token": self._creds[KEY_FEED],
        }

    async def _open(self) -> WSLike:
        if self._connect_fn is not None:
            return await self._connect_fn(W.FEED_URL)
        import websockets

        return await websockets.connect(  # type: ignore[return-value]
            W.FEED_URL, additional_headers=self._headers(), max_size=2**20
        )

    async def _run(self) -> None:
        backoff = self.backoff_min_s
        while not self._stop.is_set():
            await self._auth_ok.wait()
            if self._stop.is_set():
                return
            if not self._creds[KEY_TOKEN] or not self._creds[KEY_FEED]:
                self._block(AUTH_FAILED, f"{KEY_TOKEN} / {KEY_FEED} not set (log in on the Broker login page)")
                continue
            try:
                ws = await self._open()
            except Exception as e:  # noqa: BLE001 -- any connect failure backs off; the loop must not die
                status = _http_status(e)
                if status in (401, 403):
                    self._block(AUTH_FAILED, f"SmartStream handshake rejected with HTTP {status}")
                    continue
                log.warning("angel feed: connect failed (%s); retry in %.1fs", type(e).__name__, backoff)
                await self._sleep(backoff)
                backoff = min(backoff * 2, self.backoff_max_s)
                continue
            self._ws = ws
            got_data = False
            dropped: BaseException | None = None
            pinger: asyncio.Task[None] | None = None
            try:
                keys = list(self._modes)
                for msg in self._subscribe_all([(s, i, self._modes[(s, i)]) for s, i in keys]):
                    await ws.send(msg)
                self._unsent.difference_update(keys)
                self._synced = True
                log.info("angel feed: connected, subscribed %d instruments", len(keys))
                pinger = asyncio.create_task(self._heartbeat(ws), name="angel-ping")
                got_data = await self._pump(ws)
            except Exception as e:  # noqa: BLE001 -- a dropped socket of any kind means reconnect
                dropped = e
            finally:
                if pinger is not None:  # stop the timer BEFORE the close: no ping on a closed socket
                    pinger.cancel()
                    await asyncio.gather(pinger, return_exceptions=True)
                self._synced = False
                self._ws = None
                with contextlib.suppress(Exception):
                    await ws.close()
            if self._stop.is_set():
                return
            if got_data or self.auth_blocked:
                backoff = self.backoff_min_s
            if dropped is not None:
                self.reconnects += 1
                log.warning("angel feed: dropped (%s), reconnect #%d in %.1fs",
                            _drop_detail(dropped), self.reconnects, backoff)
            if self.auth_blocked:
                continue
            await self._sleep(backoff)
            backoff = min(backoff * 2, self.backoff_max_s)

    async def _heartbeat(self, ws: WSLike) -> None:
        """Send "ping" every heartbeat_s whatever the traffic (A2). A failed send ends the timer: the reader sees
        the same broken socket and owns the reconnect."""
        while not self._stop.is_set():
            await self._ping_sleep(self.heartbeat_s)
            if self._stop.is_set():
                return
            try:
                await ws.send(W.HEARTBEAT_TEXT)
            except Exception as e:  # noqa: BLE001 -- the reader's recv raises the real close
                log.info("angel feed: heartbeat send failed (%s)", type(e).__name__)
                return

    async def _pump(self, ws: WSLike) -> bool:
        """Read until an auth frame, stop, a socket error (propagated) or -- D13, the Dhan reference rule -- no
        DATA frame for idle_timeout_s in market hours (returns; `_run` reconnects and re-subscribes everything).
        Text frames ("pong" answers our own pings) do not count as data. The heartbeat is `_heartbeat`'s job."""
        got_data = False
        loop = asyncio.get_running_loop()
        last = loop.time()
        while not self._stop.is_set():
            try:
                msg: str | bytes | None = await asyncio.wait_for(ws.recv(), timeout=self.idle_timeout_s)
            except TimeoutError:
                msg = None
            if isinstance(msg, str):
                if self._text_frame(msg):
                    return got_data
            elif msg is not None:
                got_data = True
                last = loop.time()
                t = self._decode(msg, self._clock_ms())
                if t is not None:
                    self._emit(t)
            if not self._market_hours(self._clock_ms()):
                last = loop.time()  # the timer counts market time only
            elif loop.time() - last >= self.idle_timeout_s:
                self.idle_reconnects += 1
                log.warning("angel feed: no data frame for %.0fs in market hours; reconnecting", self.idle_timeout_s)
                return got_data
        return got_data

    def _text_frame(self, msg: str) -> bool:
        """True when the frame blocked the feed (auth)."""
        if msg.strip().lower() == "pong":
            return False
        try:
            body = json.loads(msg)
        except ValueError:
            log.info("angel feed: text frame: %.200s", msg)
            return False
        code = W.error_code(body) or ""
        text = W.error_message(body, msg)
        if code in W.AUTH_EXPIRED_CODES:
            self._block(AUTH_EXPIRED, f"{code}: {text}")
            return True
        if code in W.AUTH_CODES:
            self._block(AUTH_FAILED, f"{code}: {text}")
            return True
        log.warning("angel feed: server message %s: %s", code or "-", text)
        return False

    def _decode(self, buf: bytes, recv_ms: int) -> Tick | None:
        p = W.decode_packet(bytes(buf))
        if p is None:
            return None
        key = self._canon.get((p.exchange_type, p.token))
        if key is None:
            return None
        st = self._state.setdefault(key, [None, None, None])
        st[0] = p.ltp
        if p.volume_day is not None:
            st[1] = int(p.volume_day)
        if p.oi is not None and self._modes.get(key) == "FULL":
            st[2] = int(p.oi)
        ts = p.exchange_ts_ms if p.exchange_ts_ms > 0 else recv_ms
        return Tick(
            ts_ms=int(ts),
            recv_ms=recv_ms,
            security_id=key[1],
            segment=key[0],
            ltp=st[0],
            volume_day=st[1],
            oi=st[2],
            prev_oi=None,
            prev_close=None,
            oi_day_high=None,
            oi_day_low=None,
        )
