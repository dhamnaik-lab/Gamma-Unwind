"""Upstox Market Data Feed v3 (WebSocket, protobuf) -> canonical Ticks (BROKER-ADAPTER-SPEC §3; U1, U2, U12).

Wire (https://upstox.com/developer/api-documentation/v3/get-market-data-feed/): GET
/v3/feed/market-data-feed/authorize with the Bearer token -> a one-time `authorizedRedirectUri` (wss) -> connect;
subscribe with a BINARY frame of JSON {"guid", "method": "sub", "data": {"mode", "instrumentKeys"}}; every data
frame is a protobuf FeedResponse decoded by `wire.decode_feed_response` (in-house, no protobuf dependency).
The first frame is `market_info` (type 2) and carries no ticks.

Canonical mapping:
  mode   FULL and QUOTE -> "full" (MarketFullFeed: LTP + `vtt` day volume + `oi` + `ltt`; underlyings need the
         volume for the S1.3.0 VWAP, D89(4)); TICKER -> "ltpc". Indices in "full" send IndexFullFeed (no volume).
  ids    `NSE_FO|<token>` / ISIN-based `NSE_EQ|INE...` from the adapter's identity-guarded map when an adapter is
         given; without one only NSE_FNO resolves (`NSE_FO|<token>`, spec §1) and equities/indices are dropped
         with an ERROR (never guessed).
  ts_ms  LTPC.ltt = TRUE epoch ms (no shift); absent/0 -> receive clock.
  volume `vtt` = cumulative day QUANTITY. oi = MarketFullFeed.oi (double -> int); equities/indices: None.
         prev_close = LTPC.cp. prev_oi: not in the proto (U2) -> None.
  auth   native -> canonical: authorize / handshake HTTP 401 or an error body with UDAPI100050 ("Invalid token
         used to access API") -> 809 (Upstox has no separate "expired" code). Park until reload_credentials().
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import time
import uuid
from collections.abc import Awaitable, Callable, Iterable, Mapping
from dataclasses import dataclass
from typing import Any

import httpx

from gammad.broker.upstox import wire
from gammad.core import session_clock
from gammad.log import get_logger
from gammad.watch.feed_protocol import (
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

KEY_TOKEN = "UPSTOX_ACCESS_TOKEN"
_RANK = {"TICKER": 0, "QUOTE": 1, "FULL": 2}
SUB_CHUNK = 500

Resolver = Callable[[str, int], str | None]


def _wall_ms() -> int:
    return int(time.time() * 1000)


def _market_hours(now_ms: int) -> bool:
    return session_clock.fo_market_hours(now_ms)


class HandshakeRejected(Exception):
    """The authorize call or the WS handshake refused the token (carries the HTTP status and body text)."""

    def __init__(self, status: int, body: str) -> None:
        super().__init__(f"HTTP {status}")
        self.status_code = status
        self.body = body


def handshake_rejection(exc: BaseException) -> tuple[int, str] | None:
    status = getattr(exc, "status_code", None)
    resp = getattr(exc, "response", None)
    if status is None:
        status = getattr(resp, "status_code", None)
    if not isinstance(status, int) or status not in (401, 403):
        return None
    body = getattr(exc, "body", None) or getattr(resp, "body", b"") or b""
    text = body.decode("utf-8", "replace") if isinstance(body, bytes | bytearray) else str(body)
    return (wire.text_auth_code(text) if text else None) or (
        int(wire.AUTH_FAILED),
        f"rejected with HTTP {status}",
    )


def authorizing_connector(
    token: Callable[[], str], *, transport: httpx.BaseTransport | None = None
) -> Callable[[str], Awaitable[WSLike]]:
    """The production connector: authorize over REST (Bearer), then open the one-time wss URL it returns."""

    async def connect(url: str) -> WSLike:
        del url
        async with httpx.AsyncClient(base_url=wire.API_BASE_URL, timeout=30.0, transport=transport) as http:  # type: ignore[arg-type]
            resp = await http.get(
                wire.EP_FEED_AUTHORIZE,
                headers={"Authorization": f"Bearer {token()}", "Accept": "application/json"},
            )
        if resp.status_code in (401, 403):
            raise HandshakeRejected(resp.status_code, resp.text[:500])
        resp.raise_for_status()
        data = resp.json().get("data") or {}
        uri = data.get("authorizedRedirectUri") or data.get("authorized_redirect_uri")
        if not uri:
            raise ConnectionError("upstox authorize returned no redirect URI")
        import websockets

        return await websockets.connect(uri, max_size=2**22)  # type: ignore[return-value]

    return connect


@dataclass(slots=True)
class _State:
    ltp: float | None = None
    volume: int | None = None
    oi: int | None = None
    prev_close: float | None = None


class UpstoxFeed(FeedBase):
    caps = FeedCaps("ws", None, wire.WS_MAX_INSTRUMENTS)

    def __init__(
        self,
        access_token: str,
        out: asyncio.Queue[Tick],
        *,
        on_auth_error: AuthErrorCallback | None = None,
        connect: Connector | None = None,
        clock_ms: Callable[[], int] | None = None,
        resolve: Resolver | None = None,
        market_hours: Callable[[int], bool] = _market_hours,
        idle_timeout_s: float = 60.0,
        backoff_min_s: float = 1.0,
        backoff_max_s: float = 30.0,
    ) -> None:
        super().__init__(out, on_auth_error=on_auth_error)
        self._token = access_token
        self._connect: Callable[[str], Awaitable[WSLike]] = connect or authorizing_connector(
            lambda: self._token
        )
        self._clock_ms = clock_ms or _wall_ms
        self._resolve = resolve
        self._market_hours = market_hours
        self.idle_timeout_s = idle_timeout_s
        self.backoff_min_s = backoff_min_s
        self.backoff_max_s = backoff_max_s
        self._subs: dict[tuple[str, int], str] = {}
        self._native: dict[tuple[str, int], str] = {}
        self._canon: dict[str, tuple[str, int]] = {}
        self._state: dict[tuple[str, int], _State] = {}
        self._ws: WSLike | None = None
        self._task: asyncio.Task[None] | None = None
        self._started = False
        self.connects = 0
        self.bad_frames = 0
        self.unresolved: list[tuple[str, int]] = []
        self._synced = False  # the current socket got the full subscribe
        self._unsent: set[tuple[str, int]] = set()  # live adds whose send failed

    # ---------------------------------------------------------------- public
    async def start(self, instruments: Iterable[Instrument] = ()) -> None:
        self._started = True
        await self.add_instruments(instruments)
        self._spawn()

    def subscribed_ids(self) -> frozenset[tuple[str, int]]:
        return frozenset(self._subs)

    def pending_ids(self) -> frozenset[tuple[str, int]]:
        return frozenset(self._subs) if not self._synced else frozenset(self._unsent & self._subs.keys())

    async def add_instruments(self, instruments: Iterable[Instrument]) -> int:
        """Map every new id first (off the loop), refuse atomically across the cap (feed_protocol D9), then
        commit and send only the changes on a live socket."""
        upgrades: dict[tuple[str, int], str] = {}
        fresh: dict[tuple[str, int], str] = {}
        for seg, sid, mode in normalize_instruments(instruments):
            key = (seg, sid)
            have = self._subs.get(key)
            if have is not None:
                if _RANK[mode] > _RANK[upgrades.get(key, have)]:
                    upgrades[key] = mode
                continue
            if key not in fresh or _RANK[mode] > _RANK[fresh[key]]:
                fresh[key] = mode
        natives = await self._resolve_off_loop(list(fresh), self._native_key, "upstox feed")
        self.unresolved.extend(k for k in fresh if k not in natives)
        new = [(k, m) for k, m in fresh.items() if k in natives]
        if len(self._subs) + len(new) > self.caps.max_instruments:
            raise self._capacity_error(len(self._subs), [(k[0], k[1], m) for k, m in new], "upstox feed")
        changed: list[tuple[str, str]] = []
        for key, mode in upgrades.items():
            have = self._subs[key]
            self._subs[key] = mode
            if wire.UPSTOX_MODE[mode] != wire.UPSTOX_MODE[have]:
                changed.append((self._native[key], wire.UPSTOX_MODE[mode]))
        for key, mode in new:
            native = natives[key]
            self._subs[key] = mode
            self._native[key] = native
            self._canon[native] = key
            changed.append((native, wire.UPSTOX_MODE[mode]))
        self._accept(k for k, _m in new)
        ws = self._ws
        if changed and ws is not None:
            try:
                for msg in subscribe_messages(changed):
                    await ws.send(msg)
            except Exception as e:  # noqa: BLE001 -- the reconnect path resubscribes the full set
                self._unsent.update(upgrades)
                self._unsent.update(k for k, _m in new)
                log.warning("upstox feed: live subscribe failed (%s); resubscribing on reconnect", e)
        if self._started:
            self._spawn()
        return len(upgrades) + len(new)

    def reload_credentials(self, creds: Mapping[str, str]) -> None:
        self._token = (creds.get(KEY_TOKEN) or "").strip() or self._token
        self._unblock()

    async def stop(self) -> None:
        self._stop.set()
        self._auth_ok.set()
        ws = self._ws
        if ws is not None:
            with contextlib.suppress(Exception):
                await ws.close()
        task = self._task
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    # ---------------------------------------------------------------- internals
    def _native_key(self, seg: str, sid: int) -> str | None:
        """Runs in the worker thread of `_resolve_off_loop` (D8): the instrument file may download."""
        if self._resolve is not None:
            try:
                return self._resolve(seg, sid)
            except Exception as e:  # noqa: BLE001 -- the file may be unreachable; options still resolve
                log.warning("upstox feed: instrument map unavailable (%s)", type(e).__name__)
        return f"NSE_FO|{int(sid)}" if seg == "NSE_FNO" else None

    def _spawn(self) -> None:
        if self._task is None and self._subs and not self._stop.is_set():
            self._task = asyncio.create_task(self._run(), name="upstox-feed")

    async def _run(self) -> None:
        backoff = self.backoff_min_s
        while not self._stop.is_set():
            await self._auth_ok.wait()
            if self._stop.is_set():
                return
            try:
                ws = await self._connect(wire.WS_URL)
            except Exception as e:  # noqa: BLE001 -- any connect failure backs off; the loop must not die
                rejected = handshake_rejection(e)
                if rejected is not None:
                    self._block(*rejected)
                    continue
                log.warning("upstox feed: connect failed (%s); retry in %.1fs", type(e).__name__, backoff)
                await self._sleep(backoff)
                backoff = min(backoff * 2, self.backoff_max_s)
                continue
            self._ws = ws
            self.connects += 1
            got_data = False
            try:
                keys = list(self._subs)
                items = [(self._native[k], wire.UPSTOX_MODE[self._subs[k]]) for k in keys]
                for msg in subscribe_messages(items):
                    await ws.send(msg)
                self._unsent.difference_update(keys)
                self._synced = True
                log.info("upstox feed: connected, subscribed %d instruments", len(items))
                got_data = await self._pump(ws)
            except Exception as e:  # noqa: BLE001 -- a dropped socket of any kind means reconnect
                if not self._stop.is_set():
                    log.warning("upstox feed: dropped (%s: %s)", type(e).__name__, e)
            finally:
                self._synced = False
                self._ws = None
                with contextlib.suppress(Exception):
                    await ws.close()
            if self._stop.is_set():
                return
            if self.auth_blocked:
                backoff = self.backoff_min_s
                continue
            if got_data:
                backoff = self.backoff_min_s
            await self._sleep(backoff)
            backoff = min(backoff * 2, self.backoff_max_s)

    async def _pump(self, ws: WSLike) -> bool:
        got_data = False
        while not self._stop.is_set():
            try:
                msg = await asyncio.wait_for(ws.recv(), timeout=self.idle_timeout_s)
            except TimeoutError:
                if self._market_hours(self._clock_ms()):
                    self.idle_reconnects += 1
                    log.warning(
                        "upstox feed: no frame for %.0fs in market hours; reconnecting", self.idle_timeout_s
                    )
                    return got_data
                continue
            recv_ms = self._clock_ms()
            if isinstance(msg, str):
                auth = wire.text_auth_code(msg)
                if auth is not None:
                    self._block(*auth)
                    return got_data
                log.info("upstox feed: text frame: %.200s", msg)
                continue
            got_data = True
            for t in self.decode(bytes(msg), recv_ms):
                self._emit(t)
        return got_data

    def decode(self, data: bytes, recv_ms: int) -> list[Tick]:
        """One protobuf FeedResponse -> canonical Ticks (unsubscribed keys and market_info are ignored)."""
        try:
            resp = wire.decode_feed_response(data)
        except wire.ProtoError as e:
            self.bad_frames += 1
            log.warning("upstox feed: undecodable frame (%s)", e)
            return []
        out: list[Tick] = []
        for native, v in resp.feeds.items():
            key = self._canon.get(native)
            if key is None:
                continue
            st = self._state.setdefault(key, _State())
            if v.ltp is not None:
                st.ltp = round(v.ltp, 2)
            if v.cp is not None:
                st.prev_close = round(v.cp, 2)
            if v.volume is not None and key[0] != "IDX_I":
                st.volume = int(v.volume)
            if v.oi is not None and key[0] == "NSE_FNO":
                st.oi = round(v.oi)
            ts = int(v.ltt_ms) if v.ltt_ms else recv_ms  # true epoch ms on the wire (module doc)
            out.append(
                Tick(ts, recv_ms, key[1], key[0], st.ltp, st.volume, st.oi, None, st.prev_close, None, None)
            )
        return out


def subscribe_messages(items: Iterable[tuple[str, str]]) -> list[bytes]:
    """[(instrument key, upstox mode)] -> binary JSON `sub` messages, <= SUB_CHUNK keys each."""
    by_mode: dict[str, list[str]] = {}
    for key, mode in items:
        by_mode.setdefault(mode, []).append(key)
    out: list[bytes] = []
    for mode, keys in by_mode.items():
        for i in range(0, len(keys), SUB_CHUNK):
            msg = {
                "guid": uuid.uuid4().hex,
                "method": "sub",
                "data": {"mode": mode, "instrumentKeys": keys[i : i + SUB_CHUNK]},
            }
            out.append(json.dumps(msg).encode())
    return out


def make_resolver(adapter: Any) -> Resolver | None:
    fn = getattr(adapter, "native_id", None)
    return fn if callable(fn) else None
