"""FyersFeed: the Fyers v3 data socket ("HSM", binary) as a `Feed` (BROKER-ADAPTER-SPEC §3). BETA: not verified live.

Wire + evidence: gammad/broker/fyers/wire.py (SDK data_ws.py line cites). Per connection:
    connect(wss://socket.fyers.in/hsm/v1-5/prod) -> send auth(hsm_key from the access token's JWT) + full mode +
    subscribe("sf|nse_fo|<token>", "sf|nse_cm|<token>") -> read frames; ping every 10 s; ack every `ack_count`
    data messages (the server's flow control).
Topics are built from the canonical token (Fyers' scrip code IS the NSE exchange token, spec §1). When the entry
hands in the adapter, every topic first passes the adapter's runtime identity guard (a mismatch is dropped, never
substituted); without one the topic is built directly and a WARNING says the guard did not run.

Tick semantics (feed_protocol): ts_ms = last_traded_time (TRUE epoch seconds on the wire, x1000), else receive
time; volume_day = vol_traded_today (quantity); oi = the wire's OI field (map.json data_val[12], D-6) for NSE_FNO
only -- an equity never carries OI; ltp = raw / (10**precision * multiplier). A tick is emitted only when ltp,
volume, OI or the trade time changed (bid/ask-only updates are not ticks).
Auth -> canonical codes: a token that is not a Fyers JWT (no hsm_key) -> 809 before connecting; a JWT whose `exp`
has passed -> 807 before connecting; the server's auth response != "K" -> 809. Each parks the feed (one callback
per episode) until reload_credentials(creds). Any other socket error reconnects with back-off.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from typing import Any

from gammad.broker.fyers import wire
from gammad.core.session_clock import fo_market_hours as _market_hours
from gammad.log import get_logger
from gammad.watch.feed import _wall_ms, _websockets_connect
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

_RANK = {"TICKER": 0, "QUOTE": 1, "FULL": 2}
PING_S = 10.0
RECONNECT_S = (1.0, 2.0, 5.0, 10.0, 30.0)


@dataclass(slots=True)
class _Last:
    emitted: tuple[float | None, int | None, int | None, int | None] | None = None


class FyersFeed(FeedBase):
    caps = FeedCaps("ws", None, wire.MAX_FEED_INSTRUMENTS)

    def __init__(
        self,
        app_id: str,
        access_token: str,
        out: asyncio.Queue[Tick],
        *,
        on_auth_error: AuthErrorCallback | None = None,
        connect: Connector | None = None,
        clock_ms: Callable[[], int] = _wall_ms,
        guard: Callable[[str, int], str | None] | None = None,
        now_s: Callable[[], float] = time.time,
        market_hours: Callable[[int], bool] = _market_hours,
        idle_timeout_s: float = 60.0,
    ) -> None:
        super().__init__(out, on_auth_error=on_auth_error)
        self.app_id = app_id
        self._token = access_token
        self._connect = connect or _websockets_connect
        self._clock_ms = clock_ms
        self._guard = guard
        self._now_s = now_s
        self._market_hours = market_hours
        self.idle_timeout_s = idle_timeout_s  # D13: no DATA frame this long in market hours -> reconnect
        self.instruments: dict[tuple[str, int], str] = {}
        self._topics: dict[str, tuple[str, int]] = {}  # topic -> canonical (segment, token)
        self._sent: set[str] = set()
        self._state: dict[int, wire.TopicState] = {}
        self._last: dict[tuple[str, int], _Last] = {}
        self._ws: WSLike | None = None
        self._task: asyncio.Task[None] | None = None
        self._ack_every = 0
        self._since_ack = 0
        self.connects = 0
        self.bad_frames = 0
        self.unguarded_warned = False

    # ---------------------------------------------------------------- Feed protocol
    async def start(self, instruments: Iterable[Instrument] = ()) -> None:
        await self.add_instruments(instruments)
        if self._task is None:
            self._task = asyncio.create_task(self._run(), name="fyers-feed")

    def subscribed_ids(self) -> frozenset[tuple[str, int]]:
        return frozenset(self.instruments)

    def pending_ids(self) -> frozenset[tuple[str, int]]:
        """Mapped topics not sent on the current socket (all of them while no socket is up)."""
        if self._ws is None:
            return frozenset(self.instruments)
        return frozenset(k for t, k in self._topics.items() if t not in self._sent)

    async def add_instruments(self, instruments: Iterable[Instrument]) -> int:
        """Map every new id to its topic first (the identity guard runs off the loop), refuse atomically across
        the cap (feed_protocol D9), then commit and send. A send that fails never raises (D7): the topics stay
        pending and the next connect sends every topic."""
        ups: dict[tuple[str, int], str] = {}
        fresh: dict[tuple[str, int], str] = {}
        for seg, sid, mode in normalize_instruments(instruments):
            key = (seg, sid)
            have = self.instruments.get(key)
            if have is not None:
                if _RANK[mode] > _RANK[ups.get(key, have)]:
                    ups[key] = mode
                continue
            if key not in fresh or _RANK[mode] > _RANK[fresh[key]]:
                fresh[key] = mode
        topics = await self._map_topics(list(fresh))
        new = [(k[0], k[1], m) for k, m in fresh.items() if k in topics]
        if len(self.instruments) + len(new) > self.caps.max_instruments:
            raise self._capacity_error(len(self.instruments), new, "fyers feed")
        self.instruments.update(ups)
        for seg, sid, mode in new:
            self.instruments[(seg, sid)] = mode
            self._topics[topics[(seg, sid)]] = (seg, sid)
        self._accept((s, i) for s, i, _m in new)
        if new:
            await self._send_pending()
        return len(ups) + len(new)

    async def stop(self) -> None:
        self._stop.set()
        self._auth_ok.set()
        await self._close()
        if self._task is not None:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
            self._task = None

    def reload_credentials(self, creds: Mapping[str, str] | None = None) -> None:
        if creds:
            self.app_id = (creds.get("FYERS_APP_ID") or "").strip() or self.app_id
            self._token = (creds.get("FYERS_ACCESS_TOKEN") or "").strip() or self._token
        self._unblock()

    # ---------------------------------------------------------------- topics
    async def _map_topics(self, keys: list[tuple[str, int]]) -> dict[tuple[str, int], str]:
        """{key: topic} for the keys the identity guard passes; the rest are in rejected() (unsupported segment
        or identity guard: REJECT_UNMAPPED; the guard raised: REJECT_ERROR). One worker thread per call."""
        if self._guard is None and keys and not self.unguarded_warned:
            self.unguarded_warned = True
            log.warning("fyers feed: no adapter given -- topics built without the identity guard")
        return await self._resolve_off_loop(keys, self._guard or wire.topic_for, "fyers feed")

    async def _send_pending(self) -> None:
        ws = self._ws
        pending = [t for t in self._topics if t not in self._sent]
        if ws is None or not pending:
            return
        try:
            await ws.send(wire.subscribe_frame(pending))
        except Exception as e:  # noqa: BLE001 -- D7: a dead socket must not raise into add_instruments
            log.warning("fyers feed: subscribe of %d topics failed (%s); sent on the reconnect",
                        len(pending), type(e).__name__)
            return
        self._sent.update(pending)

    # ---------------------------------------------------------------- connection loop
    def _hsm_key(self) -> str | None:
        claims = wire.jwt_claims(self._token)
        if not claims or not claims.get("hsm_key"):
            self._block(AUTH_FAILED, "Fyers access token is not a valid Fyers JWT (no hsm_key)")
            return None
        exp = claims.get("exp")
        if isinstance(exp, int | float) and exp < self._now_s():
            self._block(AUTH_EXPIRED, "Fyers access token expired")
            return None
        return str(claims["hsm_key"])

    async def _close(self) -> None:
        ws, self._ws = self._ws, None
        if ws is not None:
            try:
                await ws.close()
            except Exception as exc:  # noqa: BLE001 -- closing a dead socket
                log.debug("fyers feed: close failed (%s)", type(exc).__name__)

    async def _run(self) -> None:
        failures = 0
        while not self._stop.is_set():
            await self._auth_ok.wait()
            if self._stop.is_set():
                return
            key = self._hsm_key()
            if key is None:
                continue
            try:
                ws = await self._connect(wire.WS_URL)
            except Exception as exc:  # noqa: BLE001 -- network: back off and retry
                log.warning("fyers feed: connect failed (%s: %s)", type(exc).__name__, exc)
                await self._sleep(RECONNECT_S[min(failures, len(RECONNECT_S) - 1)])
                failures += 1
                continue
            self.connects += 1
            self._ws = ws
            self._sent.clear()
            self._state.clear()
            self._ack_every = self._since_ack = 0
            pinger = asyncio.create_task(self._ping(ws), name="fyers-ping")
            try:
                await ws.send(wire.auth_frame(key))
                await ws.send(wire.full_mode_frame())
                await self._send_pending()
                failures = 0
                loop = asyncio.get_running_loop()
                last = loop.time()
                while not self._stop.is_set() and not self.auth_blocked:
                    try:
                        msg = await asyncio.wait_for(ws.recv(), timeout=self.idle_timeout_s)
                    except TimeoutError:
                        msg = None
                    if isinstance(msg, bytes | bytearray):
                        data = bytes(msg)
                        if len(data) > 2 and data[2] == wire.RESP_DATA:
                            last = loop.time()
                        await self._on_frame(data, ws)
                    # D13, the Dhan reference rule: no DATA frame for idle_timeout_s in market hours -> reconnect
                    # (the finally closes; the next connect re-sends every topic, intraday additions included)
                    if not self._market_hours(self._clock_ms()):
                        last = loop.time()
                    elif loop.time() - last >= self.idle_timeout_s:
                        self.idle_reconnects += 1
                        log.warning("fyers feed: no data frame for %.0fs in market hours; reconnecting",
                                    self.idle_timeout_s)
                        break
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 -- socket dropped: reconnect
                if not self._stop.is_set():
                    log.warning("fyers feed: socket error (%s: %s); reconnecting", type(exc).__name__, exc)
            finally:
                pinger.cancel()
                await asyncio.gather(pinger, return_exceptions=True)
                await self._close()
            if self._stop.is_set():
                return
            if not self.auth_blocked:
                await self._sleep(RECONNECT_S[min(failures, len(RECONNECT_S) - 1)])
                failures += 1

    async def _ping(self, ws: WSLike) -> None:
        while True:
            await asyncio.sleep(PING_S)
            await ws.send(wire.PING)

    # ---------------------------------------------------------------- frames
    async def _on_frame(self, data: bytes, ws: WSLike) -> None:
        if len(data) < 3:
            return
        kind = data[2]
        try:
            if kind == wire.RESP_AUTH:
                res = wire.parse_auth(data)
                if not res.ok:
                    self._block(AUTH_FAILED, "Fyers data socket rejected the access token")
                    return
                self._ack_every = res.ack_count
            elif kind == wire.RESP_DATA:
                frame = wire.parse_data(data, self._state)
                if self._ack_every > 0:
                    self._since_ack += 1
                    if self._since_ack >= self._ack_every:
                        self._since_ack = 0
                        await ws.send(wire.ack_frame(frame.message_number))
                recv = self._clock_ms()
                for tid in frame.updated:
                    self._emit_topic(self._state[tid], recv)
        except Exception as exc:  # noqa: BLE001 -- struct.error / IndexError: one bad frame must not kill the feed
            self.bad_frames += 1
            log.warning("fyers feed: undecodable frame type %d (%s: %s)", kind, type(exc).__name__, exc)

    def _emit_topic(self, st: wire.TopicState, recv_ms: int) -> None:
        canon = self._topics.get(st.topic)
        if canon is None:
            return
        seg, sid = canon
        ltp = wire.scaled(st, "ltp")
        ltp = None if ltp is None else round(ltp, 2)
        vol = st.values.get("vol_traded_today")
        oi = st.values.get("OI") if seg == "NSE_FNO" else None
        ltt = st.values.get("last_traded_time")
        last = self._last.setdefault(canon, _Last())
        snap = (ltp, vol, oi, ltt)
        if snap == last.emitted:
            return
        last.emitted = snap
        prev_close = wire.scaled(st, "prev_close_price")
        self._emit(
            Tick(
                ts_ms=int(ltt) * 1000 if ltt else recv_ms,
                recv_ms=recv_ms,
                security_id=sid,
                segment=seg,
                ltp=ltp,
                volume_day=None if vol is None else int(vol),
                oi=None if oi is None else int(oi),
                prev_oi=None,
                prev_close=None if prev_close is None else round(prev_close, 2),
                oi_day_high=None,
                oi_day_low=None,
            )
        )


def describe_state(feed: FyersFeed) -> dict[str, Any]:
    """Counters for /api/status (no secrets)."""
    return {"connects": feed.connects, "bad_frames": feed.bad_frames, "topics": len(feed._topics)}
