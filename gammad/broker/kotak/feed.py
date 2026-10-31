"""Kotak Neo market-data WebSocket, `native_batch` protocol (BROKER-ADAPTER-SPEC §3). Wire facts: `wire.py`.

Lifecycle (websocket.md §2-§8): connect to KOTAK_FEED_URL (written by the login: config service by dataCenter, else
data.feedUrl, else the SDK fallback; when it is empty, wire.FEED_URL_FALLBACK, as kotakneoapi 3.0.7 does; https -> wss;
no handshake headers) -> send the auth
JSON {user: KOTAK_UCC, auth: KOTAK_ACCESS_TOKEN (the session sid)} -> on 1117 (auth ok) store the per-exchange price
dividers and send the subscriptions; 1119 also carries dividers; 1120 = auth failed. The server keeps no
subscriptions across connections, so every reconnect re-authenticates and re-subscribes everything.

Canonical mode -> Kotak event: FULL and QUOTE -> `subscribeScrips` (touch line = the full market picture: LTP, day
volume, OI, trade time -- QUOTE needs the volume for the direction gate's VWAP); TICKER -> `subscribeScripsLite`;
IDX_I -> `subscribeIndices` with the documented index name. A QUOTE -> FULL upgrade therefore needs no new message
(counted, not re-sent). Budget: 3000 tokens (KN1).

Tick: ts_ms = last_trade_time (unix seconds, or ms above 1e11; wire.unix_to_ms) when > 0, else the receive clock;
ltp = raw / the exchange divider (default 100); volume_day = volume_traded_today (QUANTITY; UNCONFIRMED live);
oi = open_interest, kept only for FULL-subscribed contracts (a QUOTE underlying never gets OI); fields a packet did
not carry repeat the last value seen. prev_oi / prev_close / oi_day_high / oi_day_low are None.

Identity: canonical (segment, token) -> Kotak inputtoken through the adapter's identity-checked `native()` when
`make_feed` gets the adapter (the app passes it); without one, NSE_FNO / NSE_EQ tokens are used as-is (pSymbol ==
NSE exchange token, instruments.md example) and IDX_I is refused. Frames for tokens never subscribed are ignored.

Native -> canonical auth code map (FeedBase: 806 entitlement, 807 expired, 808 invalid client, 809 failed):
    text frame message_code 1120 ("Auth failed", websocket.md §3)       -> 809
    handshake HTTP 401 / 403                                             -> 809
    KOTAK_ACCESS_TOKEN / KOTAK_UCC empty                                 -> 809 without connecting
Kotak documents no separate "expired" code, so an expired session also reports 809 (TOKEN alert + park, D28).

`probe_session(creds)` (T-229 (5)) is a one-shot check on its own socket (auth frame -> 1117 / 1120 -> close); it never
touches a running KotakFeed.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import time
from collections.abc import Callable, Iterable, Mapping
from typing import Any

from gammad.broker.canonical import SEG_EQ, SEG_IDX, SEG_OPT
from gammad.broker.kotak import wire as W
from gammad.broker.kotak.adapter import KEY_FEED_URL, KEY_TOKEN, KEY_UCC
from gammad.core.session_clock import fo_market_hours as _market_hours
from gammad.log import get_logger
from gammad.watch.feed_protocol import (
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
Native = tuple[str, str]  # (Kotak segment, token or index name)
Resolver = Callable[[str, int], Native | None]


def _wall_ms() -> int:
    return int(time.time() * 1000)


def _http_status(exc: BaseException) -> int | None:
    resp = getattr(exc, "response", None)
    code = getattr(resp, "status_code", None) or getattr(exc, "status_code", None)
    return int(code) if isinstance(code, int) else None


def _drop_detail(exc: BaseException) -> str:
    """The drop reason for the log: a websockets ConnectionClosed gives the close frame -- `.rcvd` is the server's,
    `.sent` ours (e.g. the library's keepalive timeout); `.code` / `.reason` are deprecated in websockets 13+.
    Same rule as angel/feed.py `_drop_detail` (kept per feed: the broker packages do not import each other)."""
    name = type(exc).__name__
    if not hasattr(exc, "rcvd"):
        text = str(exc)[:160]
        return f"{name}: {text}" if text else name
    rcvd, sent = getattr(exc, "rcvd", None), getattr(exc, "sent", None)
    if rcvd is None and sent is None:
        return f"{name} no close frame (abnormal 1006)"
    frame, side = (rcvd, "server") if rcvd is not None else (sent, "client")
    return f"{name} code={getattr(frame, 'code', None)} reason={getattr(frame, 'reason', '')!r} from={side}"


def default_resolver(segment: str, security_id: int) -> Native | None:
    if segment == SEG_OPT:
        return ("nse_fo", str(int(security_id)))
    if segment == SEG_EQ:
        return ("nse_cm", str(int(security_id)))
    return None


def adapter_resolver(adapter: Any) -> Resolver:
    def resolve(segment: str, security_id: int) -> Native | None:
        try:
            return adapter.native(segment, int(security_id))
        except KeyError:
            return None

    return resolve


def _event(segment: str, mode: str) -> str:
    if segment == SEG_IDX:
        return W.EVENT_INDEX
    return W.EVENT_LITE if mode == "TICKER" else W.EVENT_SCRIPS


async def probe_session(
    creds: Mapping[str, str], timeout: float = 8.0, *, connect: Connector | None = None
) -> bool | None:
    """T-229 (5): does the feed accept today's session? One throwaway socket, separate from any running feed: connect
    (KOTAK_FEED_URL, else the SDK fallback, as `_run`), send the auth frame, wait for 1117 (True) or 1120 (False),
    close. A handshake HTTP 401 / 403 is False too. None = could not test (no UCC / sid, connect failure, the socket
    closed or `timeout` seconds passed without an answer). Never subscribes; never logs the UCC or the sid."""
    ucc = (creds.get(KEY_UCC) or "").strip()
    sid = (creds.get(KEY_TOKEN) or "").strip()
    if not ucc or not sid:
        return None
    url = W.feed_ws_url((creds.get(KEY_FEED_URL) or "").strip() or W.FEED_URL_FALLBACK)

    async def run() -> bool | None:
        try:
            if connect is not None:
                ws: WSLike = await connect(url)
            else:
                import websockets

                ws = await websockets.connect(url, max_size=2**20)  # type: ignore[assignment]
        except Exception as e:  # noqa: BLE001 -- any connect failure is "could not test" unless the server refused
            if _http_status(e) in (401, 403):
                return False
            log.info("kotak probe: connect failed (%s)", type(e).__name__)
            return None
        try:
            await ws.send(W.auth_message(ucc, sid))
            while True:
                msg = await ws.recv()
                if not isinstance(msg, str):
                    continue
                try:
                    body = json.loads(msg)
                except ValueError:
                    continue
                code = body.get("message_code") if isinstance(body, dict) else None
                if code == W.MSG_AUTH_OK:
                    return True
                if code == W.MSG_AUTH_FAILED:
                    return False
        except Exception as e:  # noqa: BLE001 -- a socket that closes before answering is "could not test"
            log.info("kotak probe: no answer (%s)", _drop_detail(e))
            return None
        finally:
            with contextlib.suppress(Exception):
                await ws.close()

    try:
        return await asyncio.wait_for(run(), timeout=timeout)
    except TimeoutError:
        log.info("kotak probe: no 1117 / 1120 within %.1f s", timeout)
        return None


class KotakFeed(FeedBase):
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
        idle_timeout_s: float = 60.0,
        backoff_min_s: float = 1.0,
        backoff_max_s: float = 30.0,
        market_hours: Callable[[int], bool] = _market_hours,
    ) -> None:
        super().__init__(out, on_auth_error=on_auth_error)
        self._creds = {k: (creds.get(k) or "").strip() for k in (KEY_UCC, KEY_TOKEN, KEY_FEED_URL)}
        self._connect_fn = connect
        self._clock_ms = clock_ms
        self._resolve = resolve
        self._market_hours = market_hours
        self.idle_timeout_s = idle_timeout_s  # D13: no DATA frame this long in market hours -> reconnect
        self._synced = False  # authenticated AND the full subscribe went out on the current socket
        self._unsent: set[tuple[str, int]] = set()  # live adds whose send failed
        self.oi_absent = 0  # picture packets whose OI field decoded as 0 (treated as absent, D10)
        self.backoff_min_s = backoff_min_s
        self.backoff_max_s = backoff_max_s
        self._modes: dict[tuple[str, int], str] = {}
        self._native: dict[tuple[str, int], Native] = {}
        self._canon: dict[tuple[int, str], tuple[str, int]] = {}  # (exchange_id, token | "name:<index>")
        self._state: dict[tuple[str, int], list[Any]] = {}  # [ltp, volume, oi]
        self._dividers: dict[int, int] = {}
        self._ws: WSLike | None = None
        self._authed = False
        self._task: asyncio.Task[None] | None = None
        self._started = False
        self.reconnects = 0  # socket drops this session (each one is followed by a reconnect)

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
        """Map every new id first in a worker thread (D8: `adapter.native()` may make a REST chain call and
        sleep on its rate bucket), refuse atomically across the cap (feed_protocol D9), then commit and send."""
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
        natives = await self._resolve_off_loop(list(fresh), self._resolve, "kotak feed")
        new = [(k[0], k[1], m) for k, m in fresh.items() if k in natives]
        if len(self._modes) + len(new) > self.caps.max_instruments:
            raise self._capacity_error(len(self._modes), new, "kotak feed")
        for key, mode in ups.items():
            have = self._modes[key]
            self._modes[key] = mode
            if have == "TICKER" and self._ws is not None and self._authed:
                with contextlib.suppress(Exception):
                    await self._ws.send(W.subscribe_message(W.EVENT_SCRIPS, [self._input(key)]))
        for seg, sid, mode in new:
            self._native[(seg, sid)] = natives[(seg, sid)]
            self._modes[(seg, sid)] = mode
            ex, tok = self._native[(seg, sid)]
            eid = W.EXCHANGE_ID.get(ex, 0)
            self._canon[(eid, f"name:{tok}" if seg == SEG_IDX else tok)] = (seg, sid)
        self._accept((s, i) for s, i, _m in new)
        ws = self._ws
        if ws is not None and self._authed and new:
            try:
                for msg in self._subscribe_all(new):
                    await ws.send(msg)
            except Exception as e:  # noqa: BLE001 -- the reconnect path resubscribes the full set
                self._unsent.update((s, i) for s, i, _m in new)
                log.warning("kotak feed: live add failed (%s); will resubscribe on reconnect", type(e).__name__)
        elif self._started and new:
            self._spawn()
        return len(new) + len(ups)

    def reload_credentials(self, creds: Mapping[str, str] | None = None) -> None:
        for k in (KEY_UCC, KEY_TOKEN, KEY_FEED_URL):
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
            self._task = asyncio.create_task(self._run(), name="kotak-feed")

    def _input(self, key: tuple[str, int]) -> str:
        ex, tok = self._native[key]
        return f"{ex}|{tok}"

    def _subscribe_all(self, items: Iterable[tuple[str, int, str]]) -> list[str]:
        by_event: dict[str, list[str]] = {}
        for seg, sid, mode in items:
            by_event.setdefault(_event(seg, mode), []).append(self._input((seg, sid)))
        return [W.subscribe_message(ev, toks) for ev, toks in sorted(by_event.items())]

    async def _open(self, url: str) -> WSLike:
        if self._connect_fn is not None:
            return await self._connect_fn(url)
        import websockets

        return await websockets.connect(url, max_size=2**20)  # type: ignore[return-value]

    async def _run(self) -> None:
        backoff = self.backoff_min_s
        while not self._stop.is_set():
            await self._auth_ok.wait()
            if self._stop.is_set():
                return
            missing = [k for k in (KEY_UCC, KEY_TOKEN) if not self._creds[k]]
            if missing:
                self._block(AUTH_FAILED, f"{', '.join(missing)} not set (log in on the Broker login page)")
                continue
            feed_url = self._creds[KEY_FEED_URL]
            if not feed_url:  # a login whose validate sent no feedUrl (pre-fix): the SDK's own last resort
                feed_url = W.FEED_URL_FALLBACK
                log.info("kotak feed: %s not set; using the SDK fallback feed URL", KEY_FEED_URL)
            try:
                ws = await self._open(W.feed_ws_url(feed_url))
            except Exception as e:  # noqa: BLE001 -- any connect failure backs off; the loop must not die
                status = _http_status(e)
                if status in (401, 403):
                    self._block(AUTH_FAILED, f"Kotak feed handshake rejected with HTTP {status}")
                    continue
                log.warning("kotak feed: connect failed (%s); retry in %.1fs", type(e).__name__, backoff)
                await self._sleep(backoff)
                backoff = min(backoff * 2, self.backoff_max_s)
                continue
            self._ws = ws
            self._authed = False
            got_data = False
            dropped: BaseException | None = None
            try:
                await ws.send(W.auth_message(self._creds[KEY_UCC], self._creds[KEY_TOKEN]))
                got_data = await self._pump(ws)
            except Exception as e:  # noqa: BLE001 -- a dropped socket of any kind means reconnect
                dropped = e
            finally:
                self._ws = None
                self._authed = False
                self._synced = False
                with contextlib.suppress(Exception):
                    await ws.close()
            if self._stop.is_set():
                return
            if got_data or self.auth_blocked:
                backoff = self.backoff_min_s
            if dropped is not None:
                self.reconnects += 1
                log.warning("kotak feed: dropped (%s), reconnect #%d in %.1fs",
                            _drop_detail(dropped), self.reconnects, backoff)
            if self.auth_blocked:
                continue
            await self._sleep(backoff)
            backoff = min(backoff * 2, self.backoff_max_s)

    async def _pump(self, ws: WSLike) -> bool:
        """Read until an auth failure, stop, a socket error (propagated) or -- D13, the Dhan reference rule -- no
        DATA frame for idle_timeout_s in market hours (returns; `_run` reconnects, re-authenticates and
        re-subscribes everything). There is no application heartbeat in this protocol (websocket.md §8), so a
        live market stream that goes silent is a dead subscription."""
        got_data = False
        loop = asyncio.get_running_loop()
        last = loop.time()
        while not self._stop.is_set():
            try:
                msg: str | bytes | None = await asyncio.wait_for(ws.recv(), timeout=self.idle_timeout_s)
            except TimeoutError:
                msg = None
            if isinstance(msg, str):
                if await self._control(ws, msg):
                    return got_data
            elif msg is not None:
                got_data = True
                last = loop.time()
                recv_ms = self._clock_ms()
                for p in W.split_batch(bytes(msg)):
                    t = self._decode(p, recv_ms)
                    if t is not None:
                        self._emit(t)
            if not self._market_hours(self._clock_ms()):
                last = loop.time()  # the timer counts market time only
            elif loop.time() - last >= self.idle_timeout_s:
                self.idle_reconnects += 1
                log.warning("kotak feed: no data frame for %.0fs in market hours; reconnecting", self.idle_timeout_s)
                return got_data
        return got_data

    async def _control(self, ws: WSLike, text: str) -> bool:
        """A JSON control frame. True when it blocked the feed."""
        try:
            body = json.loads(text)
        except ValueError:
            log.info("kotak feed: text frame: %.200s", text)
            return False
        if not isinstance(body, dict):
            return False
        code = body.get("message_code")
        if code in (W.MSG_AUTH_OK, W.MSG_INITIAL):
            self._dividers.update(W.dividers(body))
            if str(body.get("format") or "native_batch") != "native_batch":
                log.error("kotak feed: server downgraded the format to %r (unsupported)", body.get("format"))
            if code == W.MSG_AUTH_OK and not self._authed:
                self._authed = True
                keys = list(self._modes)
                for msg in self._subscribe_all([(s, i, self._modes[(s, i)]) for s, i in keys]):
                    await ws.send(msg)
                self._unsent.difference_update(keys)
                self._synced = True
                log.info("kotak feed: authenticated, subscribed %d instruments", len(keys))
            return False
        if code == W.MSG_AUTH_FAILED:
            self._block(AUTH_FAILED, f"1120: {str(body.get('message') or 'Auth failed')[:160]}")
            return True
        if body.get("error_code") not in (None, 0, "0"):
            log.warning("kotak feed: server message %s: %.200s", code, text)
        return False

    def _decode(self, raw: bytes, recv_ms: int) -> Tick | None:
        p = W.decode_packet(raw)
        if p is None:
            return None
        if p.kind == "index":
            key = self._canon.get((p.exchange_id, f"name:{p.name}"))
        else:
            key = self._canon.get((p.exchange_id, str(p.token)))
        if key is None:
            return None
        div = self._dividers.get(p.exchange_id) or W.DEFAULT_DIVIDER
        st = self._state.setdefault(key, [None, None, None])
        st[0] = p.ltp_raw / div
        if p.volume is not None:
            st[1] = int(p.volume)
        if p.oi is not None and self._modes.get(key) == "FULL" and key[0] == SEG_OPT:
            if p.oi > 0:
                st[2] = int(p.oi)
            else:
                # D10: an OI field that decodes as 0 is ABSENT, never "OI fell to zero". The header bitmask
                # (wire._HDR, last byte) is undocumented, so it is logged once here as evidence for the client's
                # live log, not interpreted. The tick repeats the last OI seen (protocol: absent field repeats).
                self.oi_absent += 1
                if self.oi_absent == 1:
                    log.warning("kotak feed: picture packet for %s %d carries OI 0 (header bitmask 0x%02x); "
                                "treated as absent, counted in oi_absent", key[0], key[1], raw[8])
        ts = W.unix_to_ms(p.ltt_raw) or recv_ms
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
