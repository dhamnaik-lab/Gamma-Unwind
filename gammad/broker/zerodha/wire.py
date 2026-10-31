"""Zerodha Kite Connect v3 -- wire facts, limits and PURE parsers (BROKER-ADAPTER-SPEC §8; research rows K1-K12 in
docs/research/BROKER-API-RESEARCH.md §2B). Nothing here does I/O.

Sources (read 2026-09-25, cited per constant below):
  REST + auth     https://kite.trade/docs/connect/v3/  (user/ login + session, exceptions/ K6, market-quotes/ K5,
                  historical/ K3 K4 K9)
  WebSocket       https://kite.trade/docs/connect/v3/websocket/ (K1, K2) -- binary layout below; segment code =
                  instrument_token & 0xff and the price divisor per segment follow the SDK's ticker
                  (kiteconnect 5.2.2 `kiteconnect/ticker.py` EXCHANGE_MAP / _parse_binary), read, never installed.
  Instrument dump https://api.kite.trade/instruments/{NSE,NFO} (public CSV, K5m)

TIME RULES (the IST/UTC handling the contract tests pin):
  * historical candles carry an explicit offset ("2026-09-24T09:15:00+0530") -> honoured, never dropped;
  * REST quote `last_trade_time` is IST wall clock WITHOUT an offset ("2026-09-24 09:20:01") -> read as IST;
  * the WebSocket `last_trade_time` / `exchange_timestamp` are true unix epoch SECONDS (no shift).
"""

from __future__ import annotations

import csv
import io
import struct
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

from gammad.broker.base import BrokerError, Candle
from gammad.config import IST
from gammad.watch.feed_protocol import Quote

API_BASE_URL = "https://api.kite.trade"
WS_URL = "wss://ws.kite.trade"
LOGIN_URL = "https://kite.zerodha.com/connect/login"  # user/ docs: ?v=3&api_key=...[&redirect_params=...]
DOC_URL = "https://kite.trade/docs/connect/v3/"
KITE_VERSION = "3"

EP_HISTORICAL = "/instruments/historical/{token}/{interval}"
EP_QUOTE = "/quote"
EP_LTP = "/quote/ltp"
EP_PROFILE = "/user/profile"
EP_SESSION = "/session/token"
EP_INSTRUMENTS = "/instruments/{exchange}"

QUOTE_MAX_IDS = 500  # K5: /quote takes up to 500 instruments
LTP_MAX_IDS = 1000  # K5: /ltp takes up to 1000
RATE_QUOTE_PER_S = 1.0  # K6: quote 1 req/s
RATE_HISTORY_PER_S = 3.0  # K6: historical 3 req/s
RATE_OTHER_PER_S = 10.0  # K6: everything else 10 req/s
WS_MAX_PER_CONNECTION = 3000  # K1 (3 connections per key; this feed uses one)

# K3 interval names; K9 per-request day spans (staff 2018, conservative).
INTERVALS: dict[int, str] = {
    1: "minute",
    3: "3minute",
    5: "5minute",
    10: "10minute",
    15: "15minute",
    30: "30minute",
    60: "60minute",
}
MAX_DAYS: dict[str, int] = {
    "minute": 30,
    "3minute": 90,
    "5minute": 90,
    "10minute": 90,
    "15minute": 180,
    "30minute": 180,
    "60minute": 365,
    "day": 2000,
}

# exceptions/ docs: TokenException = expired or invalidated session (403); PermissionException = the app/plan is
# not entitled to the call (no Connect data subscription, K12) -> canonical feed/REST codes (feed_protocol).
AUTH_EXPIRED = "807"
AUTH_ENTITLEMENT = "806"
ERROR_TYPE_CODE: dict[str, str] = {"TokenException": AUTH_EXPIRED, "PermissionException": AUTH_ENTITLEMENT}

# ticker.py EXCHANGE_MAP: the low byte of instrument_token is the segment.
SEG_CODE_NSE = 1
SEG_CODE_NFO = 2
SEG_CODE_CDS = 3
SEG_CODE_BCD = 6
SEG_CODE_INDICES = 9
CANONICAL_SEG_CODE: dict[str, int] = {"NSE_EQ": SEG_CODE_NSE, "NSE_FNO": SEG_CODE_NFO}

# Canonical index symbol (Dhan master UNDERLYING_SYMBOL) -> Kite NSE INDICES tradingsymbol.
INDEX_SYMBOLS: dict[str, str] = {
    "NIFTY": "NIFTY 50",
    "BANKNIFTY": "NIFTY BANK",
    "FINNIFTY": "NIFTY FIN SERVICE",
    "MIDCPNIFTY": "NIFTY MID SELECT",
}

# Canonical feed mode -> Kite mode (K2: only `full` carries OI + trade time; `quote` carries LTP + volume).
KITE_MODE: dict[str, str] = {"FULL": "full", "QUOTE": "quote", "TICKER": "ltp"}


# ------------------------------------------------------------------------------------------ time
def ist_naive_to_ms(text: str | None) -> int | None:
    """A Kite REST wall-clock string ("YYYY-MM-DD HH:MM:SS", IST, no offset) -> true epoch ms. None/blank -> None."""
    if not text:
        return None
    dt = datetime.fromisoformat(str(text).strip().replace("T", " "))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=IST)
    ms = round(dt.timestamp() * 1000)
    return ms if ms > 0 else None


def candle_ts_ms(text: str) -> int:
    """A historical candle timestamp ("...T09:15:00+0530") -> true epoch ms of the bar OPEN. A missing offset is
    read as IST (the exchange's clock), never as UTC."""
    dt = datetime.fromisoformat(str(text))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=IST)
    return round(dt.timestamp() * 1000)


def ist_date(ms: int) -> date:
    return datetime.fromtimestamp(ms / 1000, IST).date()


# ------------------------------------------------------------------------------------------ REST envelopes
def error_info(body: Any) -> tuple[str | None, str]:
    """-> (error_type, message) of a Kite error envelope {"status":"error","error_type":..,"message":..}."""
    if isinstance(body, Mapping) and body.get("status") == "error":
        return (str(body.get("error_type") or "") or None), str(body.get("message") or "")[:200]
    return None, ""


def auth_code_for_error(status: int, error_type: str | None) -> str | None:
    """Canonical auth code ("806"/"807") for a REST/handshake rejection, else None."""
    if error_type in ERROR_TYPE_CODE:
        return ERROR_TYPE_CODE[error_type]
    if status in (401, 403):
        return AUTH_EXPIRED
    return None


def parse_candles(body: Mapping[str, Any], *, with_oi: bool) -> list[Candle]:
    """{"data":{"candles":[[ts, o, h, l, c, v(, oi)], ...]}} -> Candles, in payload order."""
    data = body.get("data")
    rows = data.get("candles") if isinstance(data, Mapping) else None
    if not isinstance(rows, list):
        raise BrokerError("kite historical: payload has no data.candles list")
    out: list[Candle] = []
    for r in rows:
        if not isinstance(r, list) or len(r) < 6:
            raise BrokerError(f"kite historical: malformed candle {r!r}")
        oi = float(r[6]) if with_oi and len(r) > 6 and r[6] is not None else None
        out.append(
            Candle(candle_ts_ms(r[0]), float(r[1]), float(r[2]), float(r[3]), float(r[4]), float(r[5]), oi)
        )
    return out


def parse_quotes(body: Mapping[str, Any]) -> dict[int, tuple[Quote, bool]]:
    """/quote -> {instrument_token: (Quote, has_oi)}; `last_trade_time` is IST wall clock (module doc)."""
    data = body.get("data")
    out: dict[int, tuple[Quote, bool]] = {}
    if not isinstance(data, Mapping):
        return out
    for v in data.values():
        if not isinstance(v, Mapping) or v.get("instrument_token") is None:
            continue
        ltp = v.get("last_price")
        vol = v.get("volume")
        oi = v.get("oi")
        q = Quote(
            ltp=None if ltp is None else float(ltp),
            volume_day=None if vol is None else int(vol),
            oi=None if oi is None else int(oi),
            ltt_ms=ist_naive_to_ms(v.get("last_trade_time")),
        )
        out[int(v["instrument_token"])] = (q, oi is not None)
    return out


def parse_ltp(body: Mapping[str, Any]) -> dict[int, float]:
    """/quote/ltp -> {instrument_token: last_price}."""
    data = body.get("data")
    out: dict[int, float] = {}
    if isinstance(data, Mapping):
        for v in data.values():
            if (
                isinstance(v, Mapping)
                and v.get("instrument_token") is not None
                and v.get("last_price") is not None
            ):
                out[int(v["instrument_token"])] = float(v["last_price"])
    return out


# ------------------------------------------------------------------------------------------ instrument dump
@dataclass(frozen=True, slots=True)
class KiteRow:
    instrument_token: int
    exchange_token: int
    tradingsymbol: str
    name: str
    expiry: date | None
    strike: float | None
    lot_size: int
    instrument_type: str
    segment: str
    exchange: str

    @property
    def quote_key(self) -> str:
        return f"{self.exchange}:{self.tradingsymbol}"


def parse_instruments_csv(text: str) -> Iterator[KiteRow]:
    """The public instrument dump (K5m columns: instrument_token, exchange_token, tradingsymbol, name, last_price,
    expiry, strike, tick_size, lot_size, instrument_type, segment, exchange)."""
    for r in csv.DictReader(io.StringIO(text)):
        try:
            exp = (r.get("expiry") or "").strip()
            strike = float(r.get("strike") or 0) if (r.get("instrument_type") or "") in ("CE", "PE") else None
            yield KiteRow(
                instrument_token=int(r["instrument_token"]),
                exchange_token=int(r["exchange_token"]),
                tradingsymbol=(r.get("tradingsymbol") or "").strip(),
                name=(r.get("name") or "").strip().strip('"'),
                expiry=date.fromisoformat(exp[:10]) if exp else None,
                strike=strike,
                lot_size=int(float(r.get("lot_size") or 0)),
                instrument_type=(r.get("instrument_type") or "").strip(),
                segment=(r.get("segment") or "").strip(),
                exchange=(r.get("exchange") or "").strip(),
            )
        except (KeyError, ValueError):
            continue


def computed_token(segment: str, security_id: int) -> int | None:
    """instrument_token from the canonical exchange token: (token << 8) | segment code (ticker.py EXCHANGE_MAP).
    Used by the feed only when no adapter (no instrument dump) is available; indices need the dump."""
    code = CANONICAL_SEG_CODE.get(segment)
    return None if code is None else (int(security_id) << 8) | code


# ------------------------------------------------------------------------------------------ WebSocket binary
@dataclass(frozen=True, slots=True)
class KitePacket:
    instrument_token: int
    ltp: float
    volume: int | None = None
    close: float | None = None
    oi: int | None = None
    oi_day_high: int | None = None
    oi_day_low: int | None = None
    ltt_s: int | None = None  # epoch seconds of the last trade (full mode)
    exchange_ts_s: int | None = None


_I = struct.Struct(">i")


def _divisor(token: int) -> float:
    seg = token & 0xFF
    if seg == SEG_CODE_CDS:
        return 10_000_000.0
    if seg == SEG_CODE_BCD:
        return 10_000.0
    return 100.0


def _ints(pkt: bytes, n: int) -> list[int]:
    return [_I.unpack_from(pkt, 4 * i)[0] for i in range(n)]


def parse_packet(pkt: bytes) -> KitePacket | None:
    """One packet by its length (websocket/ docs): 8 = ltp; 28/32 = index quote/full; 44 = quote; 184 = full.
    Unknown lengths -> None (counted by the caller)."""
    n = len(pkt)
    if n < 8:
        return None
    token = _I.unpack_from(pkt, 0)[0]
    div = _divisor(token)
    if n == 8:
        return KitePacket(token, _I.unpack_from(pkt, 4)[0] / div)
    if n in (28, 32):  # token, ltp, high, low, open, close, change(, exchange_timestamp)
        v = _ints(pkt, n // 4)
        return KitePacket(token, v[1] / div, close=v[5] / div, exchange_ts_s=v[7] if n == 32 else None)
    if n in (44, 184):
        # token, ltp, last_qty, avg_price, volume, buy_qty, sell_qty, open, high, low, close
        v = _ints(pkt, 11)
        if n == 44:
            return KitePacket(token, v[1] / div, volume=v[4], close=v[10] / div)
        # full: + last_trade_time, oi, oi_day_high, oi_day_low, exchange_timestamp (then 120 bytes of depth)
        f = [_I.unpack_from(pkt, off)[0] for off in (44, 48, 52, 56, 60)]
        return KitePacket(
            token,
            v[1] / div,
            volume=v[4],
            close=v[10] / div,
            oi=f[1],
            oi_day_high=f[2],
            oi_day_low=f[3],
            ltt_s=f[0],
            exchange_ts_s=f[4],
        )
    return None


def split_frame(data: bytes) -> tuple[list[bytes], int]:
    """A binary frame -> (packets, malformed count). A 1-byte frame is the heartbeat (no packets).
    Layout: int16 packet count, then per packet int16 length + bytes (big-endian)."""
    if len(data) < 2:
        return [], 0
    count = struct.unpack_from(">H", data, 0)[0]
    off, out, bad = 2, [], 0
    for _ in range(count):
        if off + 2 > len(data):
            bad += 1
            break
        ln = struct.unpack_from(">H", data, off)[0]
        off += 2
        if off + ln > len(data):
            bad += 1
            break
        out.append(bytes(data[off : off + ln]))
        off += ln
    return out, bad


def text_auth_code(text: str) -> tuple[int, str] | None:
    """A text frame (or a rejected handshake's body) -> (canonical code, reason) when it is an auth failure.
    Kite text messages are {"type": "error"|"message"|"order", "data": ...}; a REST-style error envelope
    {"status":"error","error_type":"TokenException"} is accepted too (the handshake 403 body)."""
    import json

    try:
        body = json.loads(text)
    except ValueError:
        return None
    if not isinstance(body, Mapping):
        return None
    etype, msg = error_info(body)
    if etype in ERROR_TYPE_CODE:
        return int(ERROR_TYPE_CODE[etype]), msg or etype
    if body.get("type") == "error":
        data = str(body.get("data") or "")
        low = data.lower()
        if "tokenexception" in low or "access_token" in low or "api_key" in low or "token" in low:
            return int(AUTH_EXPIRED), data[:200]
    return None
