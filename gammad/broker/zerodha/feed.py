"""Zerodha Kite ticker (WebSocket, binary) -> canonical Ticks (BROKER-ADAPTER-SPEC §3; K1, K2).

Wire (https://kite.trade/docs/connect/v3/websocket/): connect `wss://ws.kite.trade?api_key=..&access_token=..`;
text `{"a":"subscribe","v":[tokens]}` then `{"a":"mode","v":[mode,[tokens]]}`; binary frames = int16 packet count
+ (int16 length, packet)*; a 1-byte frame is the heartbeat. Packet layouts: `wire.parse_packet`.

Canonical mapping:
  mode   FULL -> "full" (LTP + volume + OI + last trade time, K2); QUOTE -> "quote" (LTP + volume, no time);
         TICKER -> "ltp".
  ids    instrument_token from the adapter's identity-guarded map (`KiteAdapter.native_id`) when an adapter is
         given; without one, (exchange token << 8) | segment code for NSE_EQ / NSE_FNO (indices need the map).
  ts_ms  full packet: last_trade_time (TRUE epoch seconds) * 1000; index full: exchange_timestamp; quote / ltp
         packets carry no time -> receive clock.
  volume `volume_traded` = cumulative day QUANTITY (never lots). prev_close = the packet's `close` (previous
         session close). prev_oi: never sent (None).
  auth   native -> canonical code: WS handshake HTTP 403 (bad/expired api_key or access_token; Kite answers
         with a TokenException) -> 807 expired; a text `{"type":"error"}` naming the token -> 807; a
         PermissionException (no data subscription, K12) -> 806. Park until reload_credentials().
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import re
import time
from collections.abc import Awaitable, Callable, Iterable, Mapping
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlencode

from gammad.broker.zerodha import wire
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

KEY_API_KEY = "KITE_API_KEY"
KEY_TOKEN = "KITE_ACCESS_TOKEN"
_RANK = {"TICKER": 0, "QUOTE": 1, "FULL": 2}
SUB_CHUNK = 500

Resolver = Callable[[str, int], int | None]


def _wall_ms() -> int:
    return int(time.time() * 1000)


def _market_hours(now_ms: int) -> bool:
    return session_clock.fo_market_hours(now_ms)


_WS_SECRET_RE = re.compile(r"([?&](?:api_key|access_token)=)[^&\s'\"]+", re.IGNORECASE)


def _scrub_ws_err(exc: BaseException) -> str:
    return _WS_SECRET_RE.sub(r"\1<redacted>", str(exc))


async def _websockets_connect(url: str) -> WSLike:
    import websockets

    return await websockets.connect(url, max_size=2**22)  # type: ignore[return-value]


def handshake_rejection(exc: BaseException) -> tuple[int, str] | None:
    """A failed connect -> (canonical code, reason) when the server refused the credentials, else None."""
    resp = getattr(exc, "response", None)
    status = getattr(resp, "status_code", None) or getattr(exc, "status_code", None)
    if not isinstance(status, int) or status not in (401, 403):
        return None
    body = getattr(resp, "body", b"") or b""
    text = body.decode("utf-8", "replace") if isinstance(body, bytes | bytearray) else str(body)
    got = wire.text_auth_code(text) if text else None
    return got or (int(wire.AUTH_EXPIRED), f"handshake rejected with HTTP {status}")


@dataclass(slots=True)
class _State:
    ltp: float | None = None
    volume: int | None = None
    oi: int | None = None
    prev_close: float | None = None
    oi_day_high: int | None = None
    oi_day_low: int | None = None


class KiteFeed(FeedBase):
    caps = FeedCaps("ws", None, wire.WS_MAX_PER_CONNECTION)

    def __init__(
        self,
        api_key: str,
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
        self._api_key = api_key
        self._token = access_token
        self._connect: Callable[[str], Awaitable[WSLike]] = connect or _websockets_connect
        self._clock_ms = clock_ms or _wall_ms
        self._resolve = resolve
        self._market_hours = market_hours
        self.idle_timeout_s = idle_timeout_s
        self.backoff_min_s = backoff_min_s
        self.backoff_max_s = backoff_max_s
        self._subs: dict[tuple[str, int], str] = {}
        self._native: dict[tuple[str, int], int] = {}
        self._canon: dict[int, tuple[str, int]] = {}
        self._state: dict[tuple[str, int], _State] = {}
        self._ws: WSLike | None = None
        self._task: asyncio.Task[None] | None = None
        self._started = False
        self.connects = 0
        self.unknown_packets = 0
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
        toks = await self._resolve_off_loop(list(fresh), self._native_token, "zerodha feed")
        self.unresolved.extend(k for k in fresh if k not in toks)
        new = [(k, m) for k, m in fresh.items() if k in toks]
        if len(self._subs) + len(new) > self.caps.max_instruments:
            raise self._capacity_error(len(self._subs), [(k[0], k[1], m) for k, m in new], "zerodha feed")
        changed: list[tuple[int, str]] = []
        for key, mode in upgrades.items():
            self._subs[key] = mode
            changed.append((self._native[key], wire.KITE_MODE[mode]))
        for key, mode in new:
            tok = toks[key]
            self._subs[key] = mode
            self._native[key] = tok
            self._canon[tok] = key
            changed.append((tok, wire.KITE_MODE[mode]))
        self._accept(k for k, _m in new)
        ws = self._ws
        if changed and ws is not None:
            try:
                for msg in subscribe_messages(changed):
                    await ws.send(msg)
            except Exception as e:  # noqa: BLE001 -- the reconnect path resubscribes the full set
                self._unsent.update(upgrades)
                self._unsent.update(k for k, _m in new)
                log.warning("zerodha feed: live subscribe failed (%s); resubscribing on reconnect", e)
        if self._started:
            self._spawn()
        return len(changed)

    def reload_credentials(self, creds: Mapping[str, str]) -> None:
        self._api_key = (creds.get(KEY_API_KEY) or "").strip() or self._api_key
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
    def _native_token(self, seg: str, sid: int) -> int | None:
        """Runs in the worker thread of `_resolve_off_loop` (D8): the identity map may download the dump."""
        if self._resolve is not None:
            try:
                return self._resolve(seg, sid)
            except Exception as e:  # noqa: BLE001 -- the dump may be unreachable; fall back to the bit layout
                log.warning(
                    "zerodha feed: instrument map unavailable (%s); computing tokens", type(e).__name__
                )
        return wire.computed_token(seg, sid)

    def _url(self) -> str:
        return f"{wire.WS_URL}?{urlencode({'api_key': self._api_key, 'access_token': self._token})}"

    def _spawn(self) -> None:
        if self._task is None and self._subs and not self._stop.is_set():
            self._task = asyncio.create_task(self._run(), name="zerodha-feed")

    async def _run(self) -> None:
        backoff = self.backoff_min_s
        while not self._stop.is_set():
            await self._auth_ok.wait()
            if self._stop.is_set():
                return
            try:
                ws = await self._connect(self._url())
            except Exception as e:  # noqa: BLE001 -- any connect failure backs off; the loop must not die
                rejected = handshake_rejection(e)
                if rejected is not None:
                    self._block(*rejected)
                    continue
                log.warning("zerodha feed: connect failed (%s); retry in %.1fs", type(e).__name__, backoff)
                await self._sleep(backoff)
                backoff = min(backoff * 2, self.backoff_max_s)
                continue
            self._ws = ws
            self.connects += 1
            got_data = False
            try:
                keys = list(self._subs)
                items = [(self._native[k], wire.KITE_MODE[self._subs[k]]) for k in keys]
                for msg in subscribe_messages(items):
                    await ws.send(msg)
                self._unsent.difference_update(keys)
                self._synced = True
                log.info("zerodha feed: connected, subscribed %d instruments", len(items))
                got_data = await self._pump(ws)
            except Exception as e:  # noqa: BLE001 -- a dropped socket of any kind means reconnect
                if not self._stop.is_set():
                    log.warning("zerodha feed: dropped (%s: %s)", type(e).__name__, _scrub_ws_err(e))
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
                        "zerodha feed: no frame for %.0fs in market hours; reconnecting", self.idle_timeout_s
                    )
                    return got_data
                continue
            recv_ms = self._clock_ms()
            if isinstance(msg, str):
                auth = wire.text_auth_code(msg)
                if auth is not None:
                    self._block(*auth)
                    return got_data
                log.info("zerodha feed: text frame: %.200s", msg)
                continue
            got_data = True
            for t in self.decode(bytes(msg), recv_ms):
                self._emit(t)
        return got_data

    def decode(self, data: bytes, recv_ms: int) -> list[Tick]:
        """One binary frame -> canonical Ticks (unsubscribed tokens are ignored)."""
        packets, bad = wire.split_frame(data)
        self.unknown_packets += bad
        out: list[Tick] = []
        for raw in packets:
            p = wire.parse_packet(raw)
            if p is None:
                self.unknown_packets += 1
                continue
            key = self._canon.get(p.instrument_token)
            if key is None:
                continue
            st = self._state.setdefault(key, _State())
            st.ltp = round(p.ltp, 2)
            if p.volume is not None:
                st.volume = int(p.volume)
            if p.close is not None:
                st.prev_close = round(p.close, 2)
            if p.oi is not None:
                st.oi, st.oi_day_high, st.oi_day_low = int(p.oi), p.oi_day_high, p.oi_day_low
            if p.ltt_s:
                ts = int(p.ltt_s) * 1000  # true epoch seconds on the wire (module doc)
            elif p.exchange_ts_s:
                ts = int(p.exchange_ts_s) * 1000
            else:
                ts = recv_ms
            out.append(
                Tick(
                    ts,
                    recv_ms,
                    key[1],
                    key[0],
                    st.ltp,
                    st.volume,
                    st.oi,
                    None,
                    st.prev_close,
                    st.oi_day_high,
                    st.oi_day_low,
                )
            )
        return out


def subscribe_messages(items: Iterable[tuple[int, str]]) -> list[str]:
    """[(instrument_token, kite mode)] -> subscribe + mode messages, <= SUB_CHUNK tokens each."""
    by_mode: dict[str, list[int]] = {}
    for tok, mode in items:
        by_mode.setdefault(mode, []).append(int(tok))
    out: list[str] = []
    for mode, toks in by_mode.items():
        for i in range(0, len(toks), SUB_CHUNK):
            chunk = toks[i : i + SUB_CHUNK]
            out.append(json.dumps({"a": "subscribe", "v": chunk}))
            out.append(json.dumps({"a": "mode", "v": [mode, chunk]}))
    return out


def make_resolver(adapter: Any) -> Resolver | None:
    fn = getattr(adapter, "native_id", None)
    return fn if callable(fn) else None
