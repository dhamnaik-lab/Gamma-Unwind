"""Dhan Live Market Feed v2 client (asyncio + `websockets`) -> normalized `Tick`s on an asyncio.Queue.

Every wire fact below is copied from the installed SDK, dhanhq==2.2.0,
`.venv/Lib/site-packages/dhanhq/marketfeed.py` (cited as `mf:<line>`). The SDK is the reference,
not a dependency: it prints, owns its own event loop and never reconnects on auth failure sanely.

Facts the SDK does NOT pin (UNCONFIRMED -- the first live run must check each):
- LTT is decoded as IST wall-clock seconds (epoch shifted +05:30). Evidence: the SDK renders it with
  `datetime.utcfromtimestamp(ltt)` (mf:523) and that shows IST clock time. `ltt_to_epoch_ms` undoes the
  shift; `DhanFeed` logs one WARNING if a fresh trade's converted LTT is > 1 h from the receive clock.
- One WebSocket message is assumed to hold one OR MORE packets back to back; the SDK decodes only the
  first (mf:327). We walk the buffer by the fixed per-code sizes and ignore the header length field,
  whose exact meaning (header included or not) the SDK never uses.
- How a bad token is rejected at the handshake (HTTP 401/403 vs. an accepted socket then packet 50).
  Both paths block reconnects here.
- Whether `oi_day_high` in the Full packet is seeded from the previous day's OI. It is passed through on
  the Tick, but the OI peak (D7) is computed by bars.BarBuilder from in-session ticks only.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import struct
import time
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass, replace
from typing import Any, Protocol

from gammad.core import session_clock
from gammad.log import get_logger

log = get_logger(__name__)

# ----------------------------------------------------------------------------- wire constants
FEED_URL = "wss://api-feed.dhan.co"  # mf:20; v2 query string mf:155

# Subscription request codes (mf:33-36). Unsubscribe = request code + 1 (mf:610). Disconnect = 12 (mf:187).
REQ_TICKER = 15
REQ_QUOTE = 17
REQ_FULL = 21
REQ_DISCONNECT = 12
MAX_PER_MESSAGE = 100  # mf:237 batch_size=100, mf:297-298
MAX_PER_CONNECTION = 5000  # docs/BROKERS.md (Dhan docs); not in the SDK
MAX_CONNECTIONS = 5  # docs/BROKERS.md (Dhan docs); not in the SDK

MODE_TICKER = "TICKER"
MODE_QUOTE = "QUOTE"
MODE_FULL = "FULL"
MODE_REQUEST: dict[str, int] = {MODE_TICKER: REQ_TICKER, MODE_QUOTE: REQ_QUOTE, MODE_FULL: REQ_FULL}
_MODE_RANK: dict[str, int] = {MODE_TICKER: 0, MODE_QUOTE: 1, MODE_FULL: 2}

# Numeric exchange segment in the binary header -> request-side name (mf:23-30, mf:313-322).
SEGMENT_NAMES: dict[int, str] = {
    0: "IDX_I",
    1: "NSE_EQ",
    2: "NSE_FNO",
    3: "NSE_CURRENCY",
    4: "BSE_EQ",
    5: "MCX_COMM",
    7: "BSE_CURRENCY",
    8: "BSE_FNO",
}

# Response codes = first byte of every packet (dispatch at mf:327-343).
RESP_TICKER = 2  # mf:328
RESP_DEPTH = 3  # mf:330
RESP_QUOTE = 4  # mf:332
RESP_OI = 5  # mf:334
RESP_PREV_CLOSE = 6  # mf:336
RESP_STATUS = 7  # mf:338
RESP_FULL = 8  # mf:340
RESP_DISCONNECT = 50  # mf:342

# All little-endian, no padding. Common 8-byte header = code u8, msg_len u16, segment u8, security_id u32
# (the `<BHBI` prefix shared by every unpack below).
HEADER = struct.Struct("<BHBI")
# mf:347 ticker: +8 ltp f32, +12 ltt u32
TICKER = struct.Struct("<BHBIfI")
# mf:373 depth (unused): +8 ltp f32, +12 5x20-byte depth
DEPTH = struct.Struct("<BHBIf100s")
# mf:410 quote: +8 ltp f32, +12 ltq u16, +14 ltt u32, +18 atp f32, +22 volume u32, +26 total_sell_qty u32,
#               +30 total_buy_qty u32, +34 open f32, +38 close f32, +42 high f32, +46 low f32
QUOTE = struct.Struct("<BHBIfHIfIIIffff")
# mf:431 OI: +8 oi u32
OI = struct.Struct("<BHBII")
# mf:360 prev close: +8 prev_close f32, +12 prev_oi u32
PREV_CLOSE = struct.Struct("<BHBIfI")
# mf:442 market status: header only
STATUS = struct.Struct("<BHBI")
# mf:448 full: +8 ltp f32, +12 ltq u16, +14 ltt u32, +18 atp f32, +22 volume u32, +26 total_sell_qty u32,
#              +30 total_buy_qty u32, +34 oi u32, +38 oi_day_high u32, +42 oi_day_low u32, +46 open f32,
#              +50 close f32, +54 high f32, +58 low f32, +62 depth 100 B (mf:452-468, `<IIHHff` x5)
FULL = struct.Struct("<BHBIfHIfIIIIIIffff100s")
# mf:452-468 one depth level (5 back to back from FULL +62; level 0 = the BEST): +0 bid_qty u32, +4 ask_qty u32,
#              +8 bid_orders u16, +10 ask_orders u16, +12 bid_price f32, +16 ask_price f32 (D380 (3): owner capture)
DEPTH_LEVEL = struct.Struct("<IIHHff")
FULL_DEPTH_FIELD = 18  # FULL.unpack index of the 100-byte depth block
# mf:495 disconnect: +8 reason u16
DISCONNECT = struct.Struct("<BHBIH")

PACKET_SIZE: dict[int, int] = {
    RESP_TICKER: TICKER.size,  # 16
    RESP_DEPTH: DEPTH.size,  # 112
    RESP_QUOTE: QUOTE.size,  # 50
    RESP_OI: OI.size,  # 12
    RESP_PREV_CLOSE: PREV_CLOSE.size,  # 16
    RESP_STATUS: STATUS.size,  # 8
    RESP_FULL: FULL.size,  # 162
    RESP_DISCONNECT: DISCONNECT.size,  # 10
}

# Disconnect reason codes (mf:496-505).
DISCONNECT_REASONS: dict[int, str] = {
    805: "too many active websocket connections",
    806: "Data API subscription required",
    807: "access token expired",
    808: "invalid client id",
    809: "authentication failed",
}
# Retrying cannot fix these without new credentials / a subscription: block until reload_credentials().
BLOCKING_REASONS: frozenset[int] = frozenset({806, 807, 808, 809})

IST_OFFSET_S = 19_800  # +05:30


def ltt_to_epoch_ms(ltt: int) -> int:
    """Dhan LTT (IST wall-clock seconds, UNCONFIRMED -- see module doc) -> true epoch milliseconds."""
    return (ltt - IST_OFFSET_S) * 1000


# ----------------------------------------------------------------------------- normalized tick
@dataclass(frozen=True, slots=True)
class Tick:
    """One normalized feed event. `ts_ms` = exchange time (LTT) when the packet carries one, else the
    receive time; `recv_ms` = local receive clock. Fields a packet did not carry hold the last value
    seen for that instrument (None if never seen).

    0.8.8 (D380 (3), owner capture; no rule reads them): `bid` / `bid_qty` / `ask` / `ask_qty` = the BEST level of
    the packet's own book (Dhan FULL depth level 0; a 0 price = that side empty = None). Unlike the fields above they
    are NEVER carried forward: a packet without a book (ticker, quote, OI, every other broker's feed) leaves them None,
    because a stale quote is worse than none. Defaults keep every existing constructor unchanged."""

    ts_ms: int
    recv_ms: int
    security_id: int
    segment: str
    ltp: float | None
    volume_day: int | None
    oi: int | None
    prev_oi: int | None
    prev_close: float | None
    oi_day_high: int | None
    oi_day_low: int | None
    bid: float | None = None
    bid_qty: int | None = None
    ask: float | None = None
    ask_qty: int | None = None


@dataclass(slots=True)
class _InstrState:
    ltp: float | None = None
    volume: int | None = None
    oi: int | None = None
    prev_oi: int | None = None
    prev_close: float | None = None
    oi_day_high: int | None = None
    oi_day_low: int | None = None


def _px(v: float) -> float:
    return round(float(v), 2)  # f32 on the wire; the SDK formats to 2 dp (mf:415)


Quote = tuple[float | None, int | None, float | None, int | None]  # (bid, bid_qty, ask, ask_qty)
NO_QUOTE: Quote = (None, None, None, None)


def best_quote(depth: bytes) -> Quote:
    """D380 (3): the FULL packet's depth block -> its BEST level (level 0, mf:456-468) as (bid, bid_qty, ask,
    ask_qty). A side whose price is <= 0 (an empty side of the book) is (None, None)."""
    bid_qty, ask_qty, _bid_orders, _ask_orders, bid, ask = DEPTH_LEVEL.unpack_from(depth, 0)
    b = (_px(bid), int(bid_qty)) if bid > 0 else (None, None)
    a = (_px(ask), int(ask_qty)) if ask > 0 else (None, None)
    return (b[0], b[1], a[0], a[1])


class Decoder:
    """Binary packets -> Ticks. Keeps last-known fields per instrument so every Tick is a full snapshot.

    Emits a Tick for ticker (2), quote (4), OI (5) and full (8) packets. Prev-close (6) only updates state
    (it is not a market event); status (7) and depth (3) are ignored. Disconnect (50) yields its reason.
    A full (8) Tick also carries its book's best bid / ask (best_quote; D380 (3)); no other Tick does.
    """

    def __init__(self) -> None:
        self._state: dict[tuple[str, int], _InstrState] = {}
        self.unknown_codes = 0
        self.truncated = 0

    def state(self, segment: str, security_id: int) -> _InstrState:
        return self._state.setdefault((segment, security_id), _InstrState())

    def feed(self, data: bytes, recv_ms: int) -> tuple[list[Tick], list[int]]:
        ticks: list[Tick] = []
        reasons: list[int] = []
        off = 0
        n = len(data)
        while off < n:
            code = data[off]
            size = PACKET_SIZE.get(code)
            if size is None:
                self.unknown_codes += 1
                log.warning(
                    "feed: unknown response code %d at offset %d (len %d); dropping rest", code, off, n
                )
                break
            if off + size > n:
                self.truncated += 1
                log.warning("feed: truncated packet code %d: need %d bytes, have %d", code, size, n - off)
                break
            pkt = data[off : off + size]
            off += size
            if code == RESP_DISCONNECT:
                reasons.append(DISCONNECT.unpack(pkt)[4])
                continue
            t = self._decode(code, pkt, recv_ms)
            if t is not None:
                ticks.append(t)
        return ticks, reasons

    def _decode(self, code: int, pkt: bytes, recv_ms: int) -> Tick | None:
        _, _, seg_code, sid = HEADER.unpack_from(pkt)
        segment = SEGMENT_NAMES.get(seg_code, str(seg_code))
        st = self.state(segment, sid)
        ts = recv_ms
        quote = NO_QUOTE  # D380 (3): only a FULL packet carries a book; never carried forward (Tick doc)
        if code == RESP_QUOTE:
            f = QUOTE.unpack(pkt)
            st.ltp, st.volume = _px(f[4]), int(f[8])
            ts = ltt_to_epoch_ms(f[6]) if f[6] else recv_ms
        elif code == RESP_FULL:
            f = FULL.unpack(pkt)
            st.ltp, st.volume = _px(f[4]), int(f[8])
            st.oi, st.oi_day_high, st.oi_day_low = int(f[11]), int(f[12]), int(f[13])
            ts = ltt_to_epoch_ms(f[6]) if f[6] else recv_ms
            quote = best_quote(f[FULL_DEPTH_FIELD])
        elif code == RESP_OI:
            st.oi = int(OI.unpack(pkt)[4])
        elif code == RESP_TICKER:
            f = TICKER.unpack(pkt)
            st.ltp = _px(f[4])
            ts = ltt_to_epoch_ms(f[5]) if f[5] else recv_ms
        elif code == RESP_PREV_CLOSE:
            f = PREV_CLOSE.unpack(pkt)
            st.prev_close, st.prev_oi = _px(f[4]), int(f[5])
            return None
        else:  # status, depth
            return None
        return Tick(
            ts_ms=ts,
            recv_ms=recv_ms,
            security_id=sid,
            segment=segment,
            ltp=st.ltp,
            volume_day=st.volume,
            oi=st.oi,
            prev_oi=st.prev_oi,
            prev_close=st.prev_close,
            oi_day_high=st.oi_day_high,
            oi_day_low=st.oi_day_low,
            bid=quote[0],
            bid_qty=quote[1],
            ask=quote[2],
            ask_qty=quote[3],
        )


# ----------------------------------------------------------------------------- subscription messages
Instrument = tuple[str, int, str]  # (exchange segment name, security_id, mode)


def build_subscribe_messages(instruments: Iterable[Instrument], *, unsubscribe: bool = False) -> list[str]:
    """JSON subscribe messages, one per <= 100 instruments of the same mode (mf:293-309)."""
    by_mode: dict[str, list[tuple[str, int]]] = {}
    for seg, sid, mode in instruments:
        if mode not in MODE_REQUEST:
            raise ValueError(f"unknown feed mode {mode!r}")
        by_mode.setdefault(mode, []).append((seg, sid))
    out: list[str] = []
    for mode in (MODE_TICKER, MODE_QUOTE, MODE_FULL):
        items = by_mode.get(mode, [])
        code = MODE_REQUEST[mode] + (1 if unsubscribe else 0)
        for i in range(0, len(items), MAX_PER_MESSAGE):
            batch = items[i : i + MAX_PER_MESSAGE]
            out.append(
                json.dumps(
                    {
                        "RequestCode": code,
                        "InstrumentCount": len(batch),
                        "InstrumentList": [{"ExchangeSegment": s, "SecurityId": str(x)} for s, x in batch],
                    }
                )
            )
    return out


def feed_url(client_id: str, access_token: str) -> str:
    return f"{FEED_URL}?version=2&token={access_token}&clientId={client_id}&authType=2"  # mf:155


# ----------------------------------------------------------------------------- client
class WSLike(Protocol):
    async def send(self, message: str | bytes) -> None: ...
    async def recv(self) -> str | bytes: ...
    async def close(self) -> None: ...


Connector = Callable[[str], Awaitable[WSLike]]
AuthErrorCallback = Callable[[int, str], None]


class FeedCapacityError(RuntimeError):
    """An `add_instruments` call whose NEW instruments do not fit the feed (Dhan: MAX_CONNECTIONS x
    MAX_PER_CONNECTION). Uniform across every feed (feed_protocol module doc, 0.8.5 D9): raised BEFORE the feed
    changes anything -- no instrument added, no upgrade applied, no message sent. `refused` = the call's new
    instruments in call order; `free` = the slots left (None when the raising feed does not say)."""

    def __init__(self, message: str = "", *, refused: Iterable[Instrument] = (), free: int | None = None) -> None:
        super().__init__(message)
        self.refused: tuple[Instrument, ...] = tuple(refused)
        self.free = free


def zero_oi_as_absent(t: Tick) -> Tick | None:
    """0.8.5 D10: a tick whose OI is 0 carries NO OI update -> the same tick with oi=None (OiCarry and BarBuilder
    keep the last known OI), else None (emit `t` unchanged). Downstream a 0 is a real value -- condition A would
    read it as a 100 % unwind -- and no option on a live watchlist (chain_select drops OI <= 0 legs) has a true
    OI of 0. Feeds count each one (`oi_zero_ignored`) and log the first."""
    if t.oi is not None and t.oi <= 0:
        return replace(t, oi=None)
    return None


def safe_put_tick(out: Any, tick: Tick) -> tuple[bool, str | None]:
    """Push `tick` to `out` without tearing down feed loops on QueueFull or WatermarkHold."""
    try:
        out.put_nowait(tick)
        return True, None
    except asyncio.QueueFull:
        return False, "queue_full"
    except Exception as exc:
        from gammad.broker.durable_watermark import WatermarkHold

        if isinstance(exc, WatermarkHold):
            return False, str(exc) or "watermark_hold"
        raise


async def _websockets_connect(url: str) -> WSLike:
    import websockets

    return await websockets.connect(url, max_size=2**20)  # type: ignore[return-value]


def _market_hours(now_ms: int) -> bool:
    return session_clock.fo_market_hours(now_ms)


def _wall_ms() -> int:
    return int(time.time() * 1000)


def _http_status(exc: BaseException) -> int | None:
    resp = getattr(exc, "response", None)
    code = getattr(resp, "status_code", None) or getattr(exc, "status_code", None)
    return int(code) if isinstance(code, int) else None


class _Conn:
    def __init__(self, idx: int) -> None:
        self.idx = idx
        self.instruments: dict[tuple[str, int], str] = {}
        self.ws: WSLike | None = None
        self.task: asyncio.Task[None] | None = None
        self.connects = 0
        self.synced = False  # True once this socket got the full subscribe; False again when it drops
        self.unsent: set[tuple[str, int]] = set()  # live adds whose send failed: they go out on the reconnect


class DhanFeed:
    """Sharded (<= 5000 per connection) auto-reconnecting feed. Ticks go to `out` via put_nowait."""

    def __init__(
        self,
        client_id: str,
        access_token: str,
        out: asyncio.Queue[Tick],
        *,
        on_auth_error: AuthErrorCallback | None = None,
        connect: Connector | None = None,
        clock_ms: Callable[[], int] = _wall_ms,
        market_hours: Callable[[int], bool] = _market_hours,
        idle_timeout_s: float = 60.0,
        backoff_min_s: float = 1.0,
        backoff_max_s: float = 30.0,
        max_per_conn: int = MAX_PER_CONNECTION,
        max_conns: int = MAX_CONNECTIONS,
    ) -> None:
        self._client_id = client_id
        self._token = access_token
        self.out = out
        self.on_auth_error = on_auth_error
        self._connect = connect or _websockets_connect
        self._clock_ms = clock_ms
        self._market_hours = market_hours
        self.idle_timeout_s = idle_timeout_s
        self.backoff_min_s = backoff_min_s
        self.backoff_max_s = backoff_max_s
        self.max_per_conn = max_per_conn
        self.max_conns = max_conns
        self.decoder = Decoder()
        self.conns: list[_Conn] = []
        self._keys: dict[tuple[str, int], _Conn] = {}
        self._auth_ok = asyncio.Event()
        self._auth_ok.set()
        self._stop = asyncio.Event()
        self._started = False
        self.last_block: tuple[int, str] | None = None
        self.dropped = 0
        self.idle_reconnects = 0
        self.oi_zero_ignored = 0
        self._ltt_checked = False
        self._rejected: dict[tuple[str, int], str] = {}

    # ---------------------------------------------------------------- public
    @property
    def auth_blocked(self) -> bool:
        return not self._auth_ok.is_set()

    def subscribed_ids(self) -> frozenset[tuple[str, int]]:
        """Feed protocol (0.8.5 D2): every (segment, security_id) this feed carries (Dhan maps nothing)."""
        return frozenset(self._keys)

    def pending_ids(self) -> frozenset[tuple[str, int]]:
        """Carried ids not on a subscribed live socket yet: their connection is down / not yet subscribed, or a
        live add's send failed. The next (re)connect subscribes them."""
        out: set[tuple[str, int]] = set()
        for c in self.conns:
            out.update(c.instruments if not c.synced else c.unsent)
        return frozenset(out)

    def rejected(self) -> dict[tuple[str, int], str]:
        """Ids refused, with the reason ("cap"); an id later accepted leaves the map."""
        return dict(self._rejected)

    async def start(self, instruments: Iterable[Instrument] = ()) -> None:
        self._started = True
        await self.add_instruments(instruments)
        for c in self.conns:
            self._spawn(c)

    async def add_instruments(self, instruments: Iterable[Instrument]) -> int:
        """Add instruments without reconnecting: live sockets get subscribe messages for the new ones only.
        Returns how many (segment, security_id) pairs were new or upgraded (QUOTE -> FULL)."""
        pending: dict[int, list[Instrument]] = {}
        new_keys: list[tuple[tuple[str, int], str]] = []
        upgrades: dict[tuple[str, int], str] = {}
        seen: set[tuple[str, int]] = set()
        for seg, sid, mode in instruments:
            if mode not in MODE_REQUEST:
                raise ValueError(f"unknown feed mode {mode!r}")
            key = (seg, int(sid))
            conn = self._keys.get(key)
            if conn is not None:
                if _MODE_RANK[mode] > _MODE_RANK[upgrades.get(key, conn.instruments[key])]:
                    upgrades[key] = mode
                continue
            if key in seen:
                continue
            seen.add(key)
            new_keys.append((key, mode))
        free = sum(self.max_per_conn - len(c.instruments) for c in self.conns)
        free += (self.max_conns - len(self.conns)) * self.max_per_conn
        if len(new_keys) > free:  # before ANY change, upgrades included (feed_protocol: uniform cap semantics)
            for key, _mode in new_keys:
                self._rejected[key] = "cap"
            raise FeedCapacityError(
                f"{len(new_keys)} new instruments, capacity left {free}",
                refused=[(k[0], k[1], m) for k, m in new_keys],
                free=free,
            )
        for key, mode in upgrades.items():
            conn = self._keys[key]
            conn.instruments[key] = mode
            pending.setdefault(conn.idx, []).append((key[0], key[1], mode))
        for key, mode in new_keys:
            self._rejected.pop(key, None)
            conn = next((c for c in self.conns if len(c.instruments) < self.max_per_conn), None)
            if conn is None:
                conn = _Conn(len(self.conns))
                self.conns.append(conn)
            conn.instruments[key] = mode
            self._keys[key] = conn
            pending.setdefault(conn.idx, []).append((key[0], key[1], mode))
        for idx, items in pending.items():
            conn = self.conns[idx]
            if conn.task is None:
                if self._started:
                    self._spawn(conn)  # subscribes its whole set once connected
                continue
            ws = conn.ws
            if ws is not None:
                try:
                    for msg in build_subscribe_messages(items):
                        await ws.send(msg)
                except Exception as e:  # noqa: BLE001 -- the reconnect path resubscribes the full set anyway
                    conn.unsent.update((s, i) for s, i, _m in items)
                    log.warning("feed conn %d: live add failed (%s); will resubscribe on reconnect", idx, e)
        return sum(len(v) for v in pending.values())

    def reload_credentials(self, client_id: str | None = None, access_token: str | None = None) -> None:
        """New token / client id. Unblocks connections parked by an auth failure."""
        if client_id:
            self._client_id = client_id
        if access_token:
            self._token = access_token
        if not self._auth_ok.is_set():
            log.info("feed: credentials reloaded, reconnecting")
        self.last_block = None
        self._auth_ok.set()

    async def stop(self) -> None:
        self._stop.set()
        self._auth_ok.set()  # wake parked loops so they can exit
        for c in self.conns:
            if c.ws is not None:
                with contextlib.suppress(Exception):
                    await c.ws.close()
        tasks = [c.task for c in self.conns if c.task is not None]
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    # ---------------------------------------------------------------- internals
    def _spawn(self, conn: _Conn) -> None:
        if conn.task is None and conn.instruments:
            conn.task = asyncio.create_task(self._run(conn), name=f"dhan-feed-{conn.idx}")

    def _block(self, code: int, reason: str) -> None:
        if not self._auth_ok.is_set():
            return  # already parked; one callback per episode
        self._auth_ok.clear()
        self.last_block = (code, reason)
        log.error(
            "feed: auth/entitlement failure %d (%s); not retrying until reload_credentials()", code, reason
        )
        if self.on_auth_error is not None:
            try:
                self.on_auth_error(code, reason)
            except Exception:
                log.exception("feed: on_auth_error callback raised")

    async def _sleep(self, seconds: float) -> None:
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(self._stop.wait(), timeout=seconds)

    async def _run(self, conn: _Conn) -> None:
        backoff = self.backoff_min_s
        while not self._stop.is_set():
            await self._auth_ok.wait()
            if self._stop.is_set():
                return
            try:
                ws = await self._connect(feed_url(self._client_id, self._token))
            except Exception as e:  # noqa: BLE001 -- any connect failure backs off; the loop must not die
                status = _http_status(e)
                if status in (401, 403):
                    self._block(status, f"handshake rejected with HTTP {status}")
                    continue
                log.warning(
                    "feed conn %d: connect failed (%s); retry in %.1fs", conn.idx, type(e).__name__, backoff
                )
                await self._sleep(backoff)
                backoff = min(backoff * 2, self.backoff_max_s)
                continue
            conn.ws = ws
            conn.connects += 1
            reason: int | None = None
            got_data = False
            try:
                snapshot = [(s, i, m) for (s, i), m in conn.instruments.items()]
                for msg in build_subscribe_messages(snapshot):
                    await ws.send(msg)
                conn.unsent.difference_update((s, i) for s, i, _m in snapshot)
                conn.synced = True
                log.info("feed conn %d: connected, subscribed %d instruments", conn.idx, len(snapshot))
                reason, got_data = await self._pump(conn, ws)
            except Exception as e:  # noqa: BLE001 -- a dropped socket of any kind means reconnect
                log.warning("feed conn %d: dropped (%s: %s)", conn.idx, type(e).__name__, e)
            finally:
                conn.synced = False
                conn.ws = None
                with contextlib.suppress(Exception):
                    await ws.close()
            if reason is not None:
                text = DISCONNECT_REASONS.get(reason, "unknown reason")
                if reason in BLOCKING_REASONS:
                    self._block(reason, text)
                    backoff = self.backoff_min_s
                    continue
                log.warning("feed conn %d: server disconnect %d (%s)", conn.idx, reason, text)
            if self._stop.is_set():
                return
            if got_data:
                backoff = self.backoff_min_s
            await self._sleep(backoff)
            backoff = min(backoff * 2, self.backoff_max_s)

    async def _pump(self, conn: _Conn, ws: WSLike) -> tuple[int | None, bool]:
        """Read until disconnect packet, idle timeout in market hours, or an exception (propagated)."""
        got_data = False
        while not self._stop.is_set():
            try:
                msg = await asyncio.wait_for(ws.recv(), timeout=self.idle_timeout_s)
            except TimeoutError:
                if self._market_hours(self._clock_ms()):
                    self.idle_reconnects += 1
                    log.warning(
                        "feed conn %d: no packet for %.0fs in market hours; reconnecting",
                        conn.idx,
                        self.idle_timeout_s,
                    )
                    return None, got_data
                continue
            recv_ms = self._clock_ms()
            if isinstance(msg, str):
                log.info("feed conn %d: text frame: %.200s", conn.idx, msg)
                continue
            got_data = True
            ticks, reasons = self.decoder.feed(msg, recv_ms)
            for t in ticks:
                self._emit(t)
            if reasons:
                return reasons[0], got_data
        return None, got_data

    def _emit(self, t: Tick) -> None:
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
            if abs(t.ts_ms - t.recv_ms) > 3_600_000:
                log.warning(
                    "feed: LTT-derived ts %d is %.1f h off receive clock %d -- check the IST-shift rule",
                    t.ts_ms,
                    (t.ts_ms - t.recv_ms) / 3.6e6,
                    t.recv_ms,
                )
        ok, drop_reason = safe_put_tick(self.out, t)
        if not ok:
            self.dropped += 1
            if drop_reason and drop_reason != "queue_full" and (self.dropped == 1 or self.dropped % 100 == 0):
                log.warning("feed: tick held (%s) on %s %d (total dropped=%d)", drop_reason, t.segment, t.security_id,
                            self.dropped)
