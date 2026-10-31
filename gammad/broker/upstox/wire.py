"""Upstox API v2/v3 -- wire facts, limits, PURE parsers and a minimal protobuf decoder (BROKER-ADAPTER-SPEC §7-§8;
research rows U1-U12 in docs/research/BROKER-API-RESEARCH.md §2B). Nothing here does I/O. No protobuf dependency.

Sources (read 2026-09-25):
  candles v3      https://upstox.com/developer/api-documentation/v3/get-historical-candle-data/ (U3): rows
                  [ts "+05:30", o, h, l, c, volume, oi], NEWEST FIRST; intraday (today) at .../intraday/...
  option chain    https://upstox.com/developer/api-documentation/get-pc-option-chain/ (U5)
  contracts       https://upstox.com/developer/api-documentation/get-option-contracts/ (U6)
  quotes          https://upstox.com/developer/api-documentation/ltp-v3 + /get-full-market-quote/ (data keyed
                  "NSE_EQ:RELIANCE"; each value carries `instrument_token` = the instrument key)
  limits          https://upstox.com/developer/api-documentation/rate-limiting/ (U7): 50/s, 500/min, 2000/30 min
  token           https://upstox.com/developer/api-documentation/get-token/ (U8: until 03:30 next day),
                  /analytics-token/ (U9: 1 year, read-only GET, covers WebSocket, history, chain)
  instruments     https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz (U4, U4m); option
                  `expiry` is epoch ms (23:59:59 IST of the expiry day)
  feed v3         https://upstox.com/developer/api-documentation/v3/get-market-data-feed/ (U1) and
                  MarketDataFeed.proto v3 (U2): FeedResponse{type=1, feeds=2 map<string,Feed>, currentTs=3};
                  Feed{ltpc=1 | fullFeed=2 | firstLevelWithGreeks=3}; LTPC{ltp=1 double, ltt=2 int64 epoch ms,
                  ltq=3, cp=4 double}; FullFeed{marketFF=1 | indexFF=2}; MarketFullFeed{ltpc=1, atp=5,
                  vtt=6 int64 (volume traded today), oi=7 double}; IndexFullFeed{ltpc=1};
                  FirstLevelWithGreeks{ltpc=1, vtt=4, oi=5}. Type: initial_feed 0, live_feed 1, market_info 2.

TIME RULES: candle timestamps carry "+05:30" (honoured; a missing offset is read as IST, never UTC); option
expiry epoch ms -> IST date; feed `ltt` is TRUE epoch ms (no shift).
"""

from __future__ import annotations

import json
import struct
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

from gammad.broker.base import BrokerError, Candle
from gammad.config import IST

API_BASE_URL = "https://api.upstox.com"
DOC_URL = "https://upstox.com/developer/api-documentation/"
INSTRUMENTS_URL = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz"
WS_URL = "wss://api.upstox.com/v3/feed/market-data-feed"  # the default connector authorizes, then follows
LOGIN_DIALOG_URL = "https://api.upstox.com/v2/login/authorization/dialog"

EP_HISTORICAL = "/v3/historical-candle/{key}/{unit}/{interval}/{to}/{frm}"
EP_INTRADAY = "/v3/historical-candle/intraday/{key}/{unit}/{interval}"
EP_CHAIN = "/v2/option/chain"
EP_CONTRACTS = "/v2/option/contract"
# LTP v3 (https://upstox.com/developer/api-documentation/ltp-v3, read 2026-09-29; the v2 path is marked Deprecated):
# the same keyed body (last_price, instrument_token) plus ltq / volume / cp -- the parser is unchanged.
EP_LTP = "/v3/market-quote/ltp"
EP_QUOTES = "/v2/market-quote/quotes"
EP_PROFILE = "/v2/user/profile"
EP_FEED_AUTHORIZE = "/v3/feed/market-data-feed/authorize"
EP_TOKEN = "/v2/login/authorization/token"

QUOTE_MAX_KEYS = 500  # market-quote: up to 500 instrument keys per request
RATE_PER_S = 50.0  # U7
RATE_BURST = 10.0
RATE_LONG_PER_S = 0.9  # U7 2000 per 30 min: capacity 300 + 0.9/s * 1800 s = 1920 <= 2000
RATE_LONG_BURST = 300.0
WS_MAX_INSTRUMENTS = 2000  # U1: full mode 2000 per connection (one connection)

# Canonical interval -> (unit, interval, days per request) (U3: 1-15 min = 1 month; above = 1 quarter; days = decade)
MINUTE_SPAN_DAYS = 28
LONG_MINUTE_SPAN_DAYS = 88
DAY_SPAN_DAYS = 3650

# 401 = invalid/expired token (UDAPI100050 "Invalid token used to access API"); no separate expiry code exists.
AUTH_FAILED = "809"
AUTH_TOKEN_CODES = frozenset({"UDAPI100050", "UDAPI100016", "UDAPI100036"})

# Canonical index symbol (Dhan master) -> Upstox NSE_INDEX key suffix.
INDEX_NAMES: dict[str, str] = {
    "NIFTY": "Nifty 50",
    "BANKNIFTY": "Nifty Bank",
    "FINNIFTY": "Nifty Fin Service",
    "MIDCPNIFTY": "NIFTY MID SELECT",
}
UPSTOX_MODE: dict[str, str] = {
    "FULL": "full",
    "QUOTE": "full",
    "TICKER": "ltpc",
}  # QUOTE needs volume (D89(4))


# ------------------------------------------------------------------------------------------ time
def candle_ts_ms(text: str) -> int:
    dt = datetime.fromisoformat(str(text))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=IST)
    return round(dt.timestamp() * 1000)


def ist_date(ms: int) -> date:
    return datetime.fromtimestamp(ms / 1000, IST).date()


def expiry_date(value: Any) -> date | None:
    """Instrument-file expiry (epoch ms, or an ISO date string) -> the IST calendar date."""
    if value in (None, "", 0):
        return None
    if isinstance(value, int | float) or (isinstance(value, str) and value.isdigit()):
        return ist_date(int(value))
    return date.fromisoformat(str(value)[:10])


# ------------------------------------------------------------------------------------------ REST envelopes
def error_info(body: Any) -> tuple[str | None, str]:
    """-> (first errorCode, message) of {"status":"error","errors":[{"errorCode":..,"message":..}]}."""
    if isinstance(body, Mapping) and body.get("status") == "error":
        errs = body.get("errors")
        first = errs[0] if isinstance(errs, list) and errs and isinstance(errs[0], Mapping) else {}
        code = first.get("errorCode") or first.get("error_code")
        return (str(code) if code else "error"), str(first.get("message") or "")[:200]
    return None, ""


def auth_code_for_error(status: int, code: str | None) -> str | None:
    if code in AUTH_TOKEN_CODES or status == 401:
        return AUTH_FAILED
    return None


def parse_candles(body: Mapping[str, Any], *, with_oi: bool) -> list[Candle]:
    data = body.get("data")
    rows = data.get("candles") if isinstance(data, Mapping) else None
    if not isinstance(rows, list):
        raise BrokerError("upstox historical: payload has no data.candles list")
    out: list[Candle] = []
    for r in rows:
        if not isinstance(r, list) or len(r) < 6:
            raise BrokerError(f"upstox historical: malformed candle {r!r}")
        oi = float(r[6]) if with_oi and len(r) > 6 and r[6] is not None else None
        out.append(
            Candle(candle_ts_ms(r[0]), float(r[1]), float(r[2]), float(r[3]), float(r[4]), float(r[5]), oi)
        )
    return out


def quote_values(body: Mapping[str, Any]) -> Iterator[Mapping[str, Any]]:
    """market-quote payloads are keyed "NSE_EQ:RELIANCE"; each value names its key in `instrument_token`."""
    data = body.get("data")
    if isinstance(data, Mapping):
        for v in data.values():
            if isinstance(v, Mapping) and v.get("instrument_token"):
                yield v


def parse_ltt_ms(value: Any) -> int | None:
    """Full-quote `last_trade_time` = epoch ms (string or int). 0/blank -> None."""
    if value in (None, "", 0, "0"):
        return None
    try:
        ms = int(value)
    except (TypeError, ValueError):
        return None
    return ms if ms > 0 else None


# ------------------------------------------------------------------------------------------ protobuf (no dependency)
class ProtoError(ValueError):
    """Truncated or malformed protobuf bytes."""


def _varint(buf: bytes, i: int) -> tuple[int, int]:
    shift = 0
    out = 0
    while True:
        if i >= len(buf):
            raise ProtoError("truncated varint")
        b = buf[i]
        i += 1
        out |= (b & 0x7F) << shift
        if not b & 0x80:
            return out, i
        shift += 7
        if shift > 63:
            raise ProtoError("varint too long")


def fields(buf: bytes) -> Iterator[tuple[int, int, Any]]:
    """(field number, wire type, value): varint -> int, 64-bit -> 8 bytes, length-delimited -> bytes,
    32-bit -> 4 bytes. Groups (3/4) are refused."""
    i = 0
    n = len(buf)
    while i < n:
        key, i = _varint(buf, i)
        num, wt = key >> 3, key & 7
        if wt == 0:
            val, i = _varint(buf, i)
        elif wt == 1:
            if i + 8 > n:
                raise ProtoError("truncated fixed64")
            val, i = buf[i : i + 8], i + 8
        elif wt == 2:
            ln, i = _varint(buf, i)
            if i + ln > n:
                raise ProtoError("truncated length-delimited field")
            val, i = buf[i : i + ln], i + ln
        elif wt == 5:
            if i + 4 > n:
                raise ProtoError("truncated fixed32")
            val, i = buf[i : i + 4], i + 4
        else:
            raise ProtoError(f"unsupported wire type {wt}")
        yield num, wt, val


def _double(v: Any) -> float:
    return struct.unpack("<d", v)[0] if isinstance(v, bytes | bytearray) and len(v) == 8 else 0.0


def _int64(v: Any) -> int:
    return v - (1 << 64) if isinstance(v, int) and v >= 1 << 63 else int(v)


@dataclass(frozen=True, slots=True)
class FeedValue:
    """One instrument's update. None = the message did not carry the field (the decoder repeats the last value)."""

    ltp: float | None
    ltt_ms: int | None
    volume: int | None
    oi: float | None
    cp: float | None


def _ltpc(buf: bytes) -> tuple[float | None, int | None, float | None]:
    ltp = ltt = cp = None
    for num, _, v in fields(buf):
        if num == 1:
            ltp = _double(v)
        elif num == 2:
            ltt = _int64(v)
        elif num == 4:
            cp = _double(v)
    return ltp, ltt, cp


def _with_volume(buf: bytes, vtt_field: int, oi_field: int) -> FeedValue:
    ltp = ltt = cp = None
    vol = 0
    oi = 0.0
    for num, _, v in fields(buf):
        if num == 1:
            ltp, ltt, cp = _ltpc(v)
        elif num == vtt_field:
            vol = _int64(v)
        elif num == oi_field:
            oi = _double(v)
    return FeedValue(ltp, ltt or None, vol, oi, cp)


def decode_feed(buf: bytes) -> FeedValue | None:
    """Feed message -> FeedValue (proto3: an absent scalar inside a present message is 0)."""
    for num, _, v in fields(buf):
        if num == 1:  # ltpc
            ltp, ltt, cp = _ltpc(v)
            return FeedValue(ltp, ltt or None, None, None, cp)
        if num == 2:  # fullFeed
            for fnum, _, fv in fields(v):
                if fnum == 1:  # marketFF
                    return _with_volume(fv, 6, 7)
                if fnum == 2:  # indexFF
                    for inum, _, iv in fields(fv):
                        if inum == 1:
                            ltp, ltt, cp = _ltpc(iv)
                            return FeedValue(ltp, ltt or None, None, None, cp)
            return None
        if num == 3:  # firstLevelWithGreeks
            return _with_volume(v, 4, 5)
    return None


@dataclass(frozen=True, slots=True)
class FeedResponse:
    type: int
    feeds: dict[str, FeedValue]
    current_ts: int | None


def decode_feed_response(buf: bytes) -> FeedResponse:
    kind = 0
    current: int | None = None
    feeds: dict[str, FeedValue] = {}
    for num, _, v in fields(bytes(buf)):
        if num == 1:
            kind = int(v)
        elif num == 3:
            current = _int64(v)
        elif num == 2:  # map entry {1: key, 2: Feed}
            key = ""
            value: FeedValue | None = None
            for enum_, _, ev in fields(v):
                if enum_ == 1:
                    key = bytes(ev).decode("utf-8", "replace")
                elif enum_ == 2:
                    value = decode_feed(ev)
            if key and value is not None:
                feeds[key] = value
    return FeedResponse(kind, feeds, current)


def text_auth_code(text: str) -> tuple[int, str] | None:
    """A text frame / rejected-authorize body -> (canonical code, reason) when it is an auth failure."""
    try:
        body = json.loads(text)
    except ValueError:
        return None
    code, msg = error_info(body)
    if code is not None and (code in AUTH_TOKEN_CODES or "token" in msg.lower()):
        return int(AUTH_FAILED), f"{code}: {msg}"
    return None
