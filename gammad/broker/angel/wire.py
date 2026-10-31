"""Angel One SmartAPI wire facts + pure parsers (BROKER-ADAPTER-SPEC §8). No I/O here.

Evidence (docs/research/BROKER-API-RESEARCH.md rows A1-A11, D-3) and the wire reference read for this module:
  * REST routes, headers, login body: github.com/angel-one/smartapi-python `SmartApi/smartConnect.py`
    (read 2026-09-25): root `https://apiconnect.angelone.in` L20; routes L25-68 -- loginByPassword L26,
    getProfile L30, getCandleData L51, market quote L53; headers `requestHeaders()` L146-156 (X-PrivateKey = API
    key, X-UserType USER, X-SourceID WEB, X-ClientLocalIP / X-ClientPublicIP / X-MACAddress); login body
    {clientcode, password, totp} and response data {jwtToken, refreshToken, feedToken} `generateSession` L276-298.
  * SmartStream v2: same repo `SmartApi/smartWebSocketV2.py` -- ROOT_URI `wss://smartapisocket.angelone.in/smart-stream`
    L16, heartbeat text "ping" every 10 s L17-18 (A2), auth HEADERS Authorization / x-api-key / x-client-code /
    x-feed-token L289-294, subscribe JSON {correlationID, action 1, params {mode, tokenList}} L176-183, modes
    1 LTP / 2 QUOTE / 3 SNAP_QUOTE L28-31, exchangeType 1 nse_cm / 2 nse_fo L34-35, LITTLE-endian packet layout
    `_parse_binary_data` L347-380 (A1, A2): token 2:27 NUL-terminated, exchange_timestamp int64 35:43 (epoch ms),
    LTP int64 43:51 (paise), volume_trade_for_the_day int64 67:75, open_interest int64 131:139 (SNAP_QUOTE only).
    The SDK SWAPS best-5 buy/sell (A11, L379-380); this decoder reads no depth at all, so the bug cannot reach us.
  * Limits: 1000 tokens per session, each mode counted separately, 3 connections (A3); getCandleData 3/s, 180/min
    (A9); 5-min history 100 days / request, 1-min 30 days (A8). Candles carry NO OI (A4) -> caps.candle_oi False;
    getOIData has no documented parameters (A5) and is not used. No option-chain / expiry route (A6): the chain is
    canonical master + one quote batch, expiries come from the canonical master.
  * Scrip master `https://margincalculator.angelone.in/OpenAPI_File/files/OpenAPIScripMaster.json`: GET only (HEAD
    404), strike and lotsize are STRINGS (A7); option strikes are read as PAISE (x100, like tick_size) --
    UNCONFIRMED here: if wrong, the identity guard drops every option with an ERROR, it never mis-maps one.
    Identity (spec §1, measured): Angel `token` == the NSE exchange token for NSE/NFO rows (0 mismatches over
    30,716 OPTSTK and 2,689 EQ rows). Indices use Angel's own tokens (e.g. 99926000), mapped by name.

TIME RULES (the IST/UTC handling the contract harness pins):
  * getCandleData timestamps are ISO-8601 WITH the +05:30 offset ("2026-09-24T09:15:00+05:30") -> parsed as an
    aware datetime -> true epoch ms of the bar OPEN (09:15 IST == 03:45 UTC). Daily bars carry IST midnight.
  * quote `exchTradeTime` / `exchFeedTime` are IST WALL-CLOCK text ("24-Sep-2026 09:44:59") -> localised to IST.
  * SmartStream `exchange_timestamp` is epoch MILLISECONDS (true epoch per the SDK field doc; UNCONFIRMED live --
    the FeedBase one-shot "> 1 h off the receive clock" WARNING will say so if it is IST wall clock like Dhan's).

UNCONFIRMED until the first live smoke (scripts/smoke_broker.py): SNAP_QUOTE volume is in QUANTITY (not lots);
quote rate limit (1 req/s used, below any published figure); the auth error bodies (AG8001 invalid token,
AG8002 expired, AG8003 missing) and whether SmartStream reports a bad token as an HTTP 401 handshake refusal
(handled) or as a JSON text frame (also handled, fixture hand-built).
"""

from __future__ import annotations

import json
import struct
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

from gammad.broker.base import Candle
from gammad.config import IST
from gammad.core import session_clock
from gammad.watch.feed_protocol import Quote

API_BASE_URL = "https://apiconnect.angelone.in"  # smartConnect.py L20
EP_LOGIN = "/rest/auth/angelbroking/user/v1/loginByPassword"  # L26
EP_PROFILE = "/rest/secure/angelbroking/user/v1/getProfile"  # L30
EP_CANDLES = "/rest/secure/angelbroking/historical/v1/getCandleData"  # L51
EP_QUOTE = "/rest/secure/angelbroking/market/v1/quote"  # L53
MASTER_URL = "https://margincalculator.angelone.in/OpenAPI_File/files/OpenAPIScripMaster.json"  # A7
FEED_URL = "wss://smartapisocket.angelone.in/smart-stream"  # smartWebSocketV2.py L16
DOC_URL = "https://smartapi.angelone.in/docs"
LOGIN_PAGE_URL = "https://smartapi.angelone.in/publisher-login"  # smartConnect.py L22

# Canonical segment -> Angel exchange (REST) and SmartStream exchangeType.
EXCHANGE = {"NSE_EQ": "NSE", "NSE_FNO": "NFO", "IDX_I": "NSE"}
EXCHANGE_TYPE = {"NSE": 1, "NFO": 2}  # smartWebSocketV2.py L34-35

# Canonical feed mode -> SmartStream mode (spec §3: FULL = SNAP_QUOTE, QUOTE = QUOTE).
STREAM_LTP, STREAM_QUOTE, STREAM_SNAP = 1, 2, 3
STREAM_MODE = {"FULL": STREAM_SNAP, "QUOTE": STREAM_QUOTE, "TICKER": STREAM_LTP}

MAX_FEED_TOKENS = 1000  # A3: per session (one connection used)
HEARTBEAT_S = 10.0  # A2
HEARTBEAT_TEXT = "ping"  # smartWebSocketV2.py L17; the server answers "pong"
QUOTE_MAX_TOKENS = 50  # SmartAPI market-data quote: 50 tokens per request
CANDLE_RATE_PER_S = 3.0  # A9
CANDLE_RATE_PER_MIN = 180.0  # A9
# T-150(c): the pace the adapter actually uses for getCandleData. A bucket of burst 3 at 3/s let 6 requests into one
# second (3 banked + 3 refilled) and sat exactly ON the 180/min line, and the client's 10:13 mark_levels (210
# underlyings x 3 daily requests = 630 calls) drew "HTTP 403 Access denied because of exceeding access rate". Burst 1
# at 90 % of the tighter of the two documented limits (2.7/s = 162/min) leaves room for clock jitter.
# 0.8.3: 2.7/s still drew 82 x 403 over 721 responses in the owner's 2 Oct 09:20 recording (r2: 639 candle requests,
# 72 retried, one 403-403-403-200). The bucket paces request STARTS; 4 requests land in one server second once network
# jitter compresses 3 gaps by > 3/2.7 - 1 = 0.11 s at 2.7/s, but > 0.25 s at 2.4/s (and 0.5 s at 2.0/s). 2.4/s =
# 80 % of the tighter limit (144/min): 639 requests take 266 s of pacing vs 237 s at 2.7/s and 320 s at 2.0/s.
CANDLE_PACE_MARGIN = 0.8
CANDLE_PACE_PER_S = min(CANDLE_RATE_PER_S, CANDLE_RATE_PER_MIN / 60.0) * CANDLE_PACE_MARGIN
# A rate-limit reply (403 "exceeding access rate" / 429) retries up to RATE_MAX_RETRIES times (0.8.3: 6, was 3) with
# exponential backoff + FULL jitter (uniform(0, min(cap, 2^attempt))), or the server's Retry-After when it sends one.
RATE_MAX_RETRIES = 6
RATE_BACKOFF_MAX_S = 8.0  # a Retry-After (or our own backoff ceiling) never waits longer than this (0.8.3: 8 s, was 10)
# The public scrip master download: up to MASTER_ATTEMPTS tries on a transport error / 5xx, waiting 2 s, 4 s between.
MASTER_ATTEMPTS = 3
QUOTE_RATE_PER_S = 1.0  # conservative; see module doc
LOGIN_RATE_PER_S = 1.0  # A9

INTERVALS = {
    1: "ONE_MINUTE",
    3: "THREE_MINUTE",
    5: "FIVE_MINUTE",
    10: "TEN_MINUTE",
    15: "FIFTEEN_MINUTE",
    30: "THIRTY_MINUTE",
    60: "ONE_HOUR",
}
DAILY_INTERVAL = "ONE_DAY"
# Days per getCandleData request (A8 for 1 and 5 min; the rest kept at or under the same ratio).
MAX_DAYS = {1: 30, 3: 30, 5: 100, 10: 100, 15: 100, 30: 100, 60: 100}
DAILY_MAX_DAYS = 365  # D-11: a 2000-day ONE_DAY claim is unconfirmed; 365 is safe

# Credential-class error codes (SmartAPI error table): invalid / expired / missing token, session expired.
AUTH_EXPIRED_CODES = frozenset({"AG8002", "AB1010"})
AUTH_CODES = frozenset({"AG8001", "AG8002", "AG8003", "AB1010", "AB8050", "AB8051"})
# 0.8.3 T-229 (2), D230: the login was accepted but the API key is refused for prices (AG8004 "Invalid API Key",
# seen as HTTP 200 status:false on a client's quote call 1 Oct) -> BrokerEntitlementError, never retried.
ENTITLEMENT_CODES = frozenset({"AG8004"})
RATE_WORDS = ("exceeding access rate", "rate limit", "too many")

# SmartStream packet layout (little-endian), smartWebSocketV2.py L347-373.
_HEAD = struct.Struct("<BB25sqqq")  # mode, exchange_type, token, sequence, exchange_timestamp, ltp -> 51 bytes
_VOLUME = struct.Struct("<q")  # volume_trade_for_the_day @67
_OI = struct.Struct("<q")  # open_interest @131
LTP_LEN = 51
QUOTE_LEN = 123
SNAP_LEN = 379
VOLUME_AT = 67
OI_AT = 131


def epoch_ms(dt: datetime) -> int:
    return round(dt.timestamp() * 1000)


def parse_candle_time(text: str) -> int:
    """'2026-09-24T09:15:00+05:30' -> true epoch ms (the offset is honoured; a naive stamp is IST wall clock)."""
    dt = datetime.fromisoformat(str(text).strip())
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=IST)
    return epoch_ms(dt)


def parse_ist_text(text: str | None) -> int | None:
    """Quote time '24-Sep-2026 09:44:59' (IST wall clock) -> true epoch ms; None when absent / unparseable."""
    s = (text or "").strip()
    if not s:
        return None
    for fmt in ("%d-%b-%Y %H:%M:%S", "%d-%m-%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            return epoch_ms(datetime.strptime(s, fmt).replace(tzinfo=IST))
        except ValueError:
            continue
    return None


def candle_body(exchange: str, token: str, interval: str, d0: date, d1: date) -> dict[str, str]:
    return {
        "exchange": exchange,
        "symboltoken": str(token),
        "interval": interval,
        "fromdate": f"{d0.isoformat()} 09:15",
        "todate": f"{d1.isoformat()} {session_clock.fo_close_hm(d1)}",
    }


def parse_candles(payload: Any) -> list[Candle]:
    """getCandleData `data` = [[time, open, high, low, close, volume], ...] -> Candles (no OI, A4), oldest first."""
    rows = payload.get("data") if isinstance(payload, Mapping) else None
    out: dict[int, Candle] = {}
    for r in rows or []:
        if not isinstance(r, list | tuple) or len(r) < 6:
            continue
        ts = parse_candle_time(r[0])
        out[ts] = Candle(ts, float(r[1]), float(r[2]), float(r[3]), float(r[4]), float(r[5]), None)
    return [out[k] for k in sorted(out)]


def _int_or_none(v: Any) -> int | None:
    if v in (None, ""):
        return None
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return None


def _float_or_none(v: Any) -> float | None:
    if v in (None, ""):
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def fetched_rows(payload: Any) -> list[Mapping[str, Any]]:
    data = payload.get("data") if isinstance(payload, Mapping) else None
    rows = data.get("fetched") if isinstance(data, Mapping) else None
    return [r for r in rows or [] if isinstance(r, Mapping)]


def parse_quote_row(r: Mapping[str, Any]) -> Quote:
    """One `fetched` row of mode FULL -> Quote (volume = tradeVolume, oi = opnInterest, ltt = exchTradeTime)."""
    return Quote(
        ltp=_float_or_none(r.get("ltp")),
        volume_day=_int_or_none(r.get("tradeVolume")),
        oi=_int_or_none(r.get("opnInterest")),
        ltt_ms=parse_ist_text(r.get("exchTradeTime")),
    )


def retry_after_s(value: str | None) -> float | None:
    """A Retry-After header in SECONDS ('2', '2.5') -> float; None when absent / an HTTP date / unparseable (the
    caller then uses its own exponential schedule)."""
    try:
        s = float(str(value).strip()) if value not in (None, "") else None
    except ValueError:
        return None
    return s if s is not None and s >= 0 else None


def error_code(body: Any) -> str | None:
    if not isinstance(body, Mapping):
        return None
    for k in ("errorcode", "errorCode", "error_code"):
        v = body.get(k)
        if v not in (None, ""):
            return str(v)
    return None


def error_message(body: Any, fallback: str = "") -> str:
    if isinstance(body, Mapping):
        for k in ("message", "errorMessage", "error"):
            v = body.get(k)
            if v:
                return str(v)[:200]
    return fallback[:200]


def is_failure(body: Any) -> bool:
    """SmartAPI signals failure with status/success False (HTTP may still be 200)."""
    if not isinstance(body, Mapping):
        return False
    for k in ("status", "success"):
        if k in body and body[k] is False:
            return True
    return False


# ------------------------------------------------------------------------------------------ scrip master
@dataclass(frozen=True)
class MasterRow:
    token: str
    name: str
    exch_seg: str
    instrumenttype: str
    expiry: date | None
    strike: float | None  # rupees (the file carries paise)
    option_type: str | None
    symbol: str


def parse_master_expiry(text: str) -> date | None:
    s = (text or "").strip()
    if not s:
        return None
    try:
        return datetime.strptime(s, "%d%b%Y").replace(tzinfo=IST).date()
    except ValueError:
        return None


def parse_master_row(r: Mapping[str, Any]) -> MasterRow | None:
    token = str(r.get("token") or "").strip()
    seg = str(r.get("exch_seg") or "").strip().upper()
    if not token or seg not in ("NSE", "NFO"):
        return None
    itype = str(r.get("instrumenttype") or "").strip().upper()
    symbol = str(r.get("symbol") or "").strip().upper()
    opt = None
    strike = None
    if itype in ("OPTSTK", "OPTIDX"):
        opt = symbol[-2:] if symbol[-2:] in ("CE", "PE") else None
        paise = _float_or_none(r.get("strike"))
        strike = None if paise is None else paise / 100.0
    return MasterRow(
        token=token,
        name=str(r.get("name") or "").strip().upper(),
        exch_seg=seg,
        instrumenttype=itype,
        expiry=parse_master_expiry(str(r.get("expiry") or "")),
        strike=strike,
        option_type=opt,
        symbol=symbol,
    )


# ------------------------------------------------------------------------------------------ SmartStream
@dataclass(frozen=True)
class StreamPacket:
    mode: int
    exchange_type: int
    token: str
    exchange_ts_ms: int
    ltp: float
    volume_day: int | None
    oi: int | None


def decode_packet(buf: bytes) -> StreamPacket | None:
    """One SmartStream binary frame -> StreamPacket; None for a short / unknown frame."""
    if len(buf) < LTP_LEN:
        return None
    mode, exch, raw_token, _seq, ex_ts, ltp_paise = _HEAD.unpack_from(buf, 0)
    token = raw_token.split(b"\x00", 1)[0].decode("ascii", "replace")
    volume = oi = None
    if mode in (STREAM_QUOTE, STREAM_SNAP):
        if len(buf) < QUOTE_LEN:
            return None
        volume = _VOLUME.unpack_from(buf, VOLUME_AT)[0]
    if mode == STREAM_SNAP:
        if len(buf) < OI_AT + 8:
            return None
        oi = _OI.unpack_from(buf, OI_AT)[0]
    if mode not in (STREAM_LTP, STREAM_QUOTE, STREAM_SNAP):
        return None
    return StreamPacket(mode, exch, token, int(ex_ts), ltp_paise / 100.0, volume, oi)


def subscribe_message(action: int, mode: int, by_exchange: Mapping[int, list[str]], correlation: str) -> str:
    return json.dumps(
        {
            "correlationID": correlation[:10],
            "action": action,
            "params": {
                "mode": mode,
                "tokenList": [{"exchangeType": ex, "tokens": list(toks)} for ex, toks in sorted(by_exchange.items())],
            },
        }
    )
