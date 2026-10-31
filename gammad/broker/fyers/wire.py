"""Fyers API v3 wire facts + pure parsers / HSM socket codec (BROKER-ADAPTER-SPEC §8; research F1-F9). BETA.

Sources (read 2026-09-25; the SDK is READ for wire facts, never installed -- spec §7):
  [SDK]  fyers-apiv3 3.1.18 wheel (PyPI 2026-09-17): fyers_apiv3/fyersModel.py, FyersWebsocket/data_ws.py,
         FyersWebsocket/map.json, FyersWebsocket/defines.py
  [SKILL] github.com/FyersDev/fyers-skills @ 4122644 (2026-07-11): skills/fyers-trading/references/{auth,
         market-data,websocket}.md
  [M]    public.fyers.in/sym_details/NSE_FO.csv + NSE_CM.csv downloaded 2026-09-25 (no header row). Columns used
         (0-based, spec §1 "ticker (col 9) by scrip code (col 12)"): 0 fyToken, 3 lot, 8 expiry epoch (15:30 IST
         of the expiry day), 9 ticker "NSE:RELIANCE26SEP1400CE", 10 exchange (10 = NSE), 11 segment (10 CM,
         11 FO), 12 scrip code == the canonical NSE token (spec §1: 68,374 = 68,374), 13 underlying symbol,
         15 strike, 16 CE | PE | XX. The NIFTY index row is "NSE:NIFTY50-INDEX" with underlying "NIFTY".

REST [SDK fyersModel.py:17-70, 139-195; SKILL auth.md, market-data.md]
  * API https://api-t1.fyers.in/api/v3 (profile, auth), DATA https://api-t1.fyers.in/data (history, quotes,
    options-chain-v3). Header `Authorization: <app_id>:<access_token>` + `version: 3`.
  * Body {"s": "ok"|"error", "code", "message", ...}. Auth failures: HTTP 401 or code -8 / -15 / -16 / -17
    [SKILL auth.md "Errors -8, -15, -16, -17 (or HTTP 401) mean the token is invalid/expired"]; HTTP 403 = the
    app lacks the Market Data permission template [SKILL auth.md "Permission templates"].
  * History: GET /history?symbol&resolution&date_format=1&range_from&range_to&cont_flag=1&oi_flag=1 ->
    {"s":"ok","candles":[[epoch_s, o, h, l, c, v(, oi)]]}; epoch = TRUE epoch seconds of the bar open (a daily bar
    is stamped 00:00 IST = 18:30Z the day before, so its date MUST be read in IST); minute resolutions 100 days
    per request, daily 366 (F4).
  * Option chain: GET /options-chain-v3?symbol=NSE:<U>-EQ&strikecount<=50&timestamp=<expiry epoch> ->
    data.expiryData[{date "DD-MM-YYYY", expiry epoch}], data.optionsChain[{symbol, fyToken, option_type,
    strike_price, ltp, oi, prev_oi, volume}] + one spot row (option_type "", strike -1) (F5).
  * Quotes: GET /quotes?symbols=<comma list, <= 50> -> d[{n, s, v{lp, volume, tt, ...}}] (no OI).
  * Limits 10/s, 200/min, 100k/day; > 3 breaches in a minute blocks the day (F7) -> one 3/s bucket (180/min).
Data socket [SDK data_ws.py] -- binary "HSM" protocol, big-endian unless noted:
  * URL wss://socket.fyers.in/hsm/v1-5/prod (:200), no headers. The access token is a JWT whose payload carries
    `hsm_key` and `exp` (:277-321); the socket authenticates with hsm_key.
  * Client frames: auth (type 1, :325-378), full mode (type 12, value 70, channel 11, :421-460), subscribe
    (type 4, topics "sf|nse_fo|<scrip code>" / "sf|nse_cm|<scrip code>" built from fyToken[10:] and
    map.json exch_seg_dict "1010" nse_cm / "1011" nse_fo, :64-145, :462-507), ack (type 3, :630-660) after every
    `ack_count` data messages, ping bytes 00 01 0B every 10 s (:1673-1680).
  * Server frames: [u16 len][u8 type]...; type 1 auth response: field 1 = "K" when accepted, field 2 = u32
    ack_count (:662-706); type 6 data feed: u32 message number at [3:7], u16 scrip count at [7:9], then entries
    (:1018-1287): 83 = snapshot (u8 83, u16 topic id NATIVE byte order (read with "H", i.e. little-endian on the
    x86 hosts we ship), u8 name len, name, u8 field count, i32 x count, 2 skipped bytes, u16 multiplier, u8
    precision, 3 x (u8 len + str): exchange, exchange token, symbol); 85 = update (u8 85, u16 topic id, u8 count,
    i32 x count: field i = map.json data_val[i]); 76 = lite (ltp only). i32 -2147483648 = "no value".
  * map.json data_val: 0 ltp, 1 vol_traded_today, 2 last_traded_time, 3 exch_feed_time, ... 12 OI, ...
    20 prev_close_price. Prices are divided by 10**precision * multiplier (:943-988); volume, OI and the two
    times are raw integers. Times are TRUE epoch seconds (no IST shift).
  * D-6 settled from the wire: the SDK DECODES field 12 "OI" into its state and then deletes it from the dict it
    hands to users (`response.pop("OI")`, :995-996) -- which is why the vendor skill's SymbolUpdate list (F3)
    omits it. OI is therefore on the wire; this decoder keeps it (caps.feed_oi "native", BETA until the live smoke).
"""

from __future__ import annotations

import base64
import json
import math
import struct
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

from gammad.broker.base import Candle
from gammad.config import IST

API_BASE_URL = "https://api-t1.fyers.in/api/v3"
DATA_BASE_URL = "https://api-t1.fyers.in/data"
MASTER_URLS = {
    "NSE_FO": "https://public.fyers.in/sym_details/NSE_FO.csv",
    "NSE_CM": "https://public.fyers.in/sym_details/NSE_CM.csv",
}
WS_URL = "wss://socket.fyers.in/hsm/v1-5/prod"
DOC_URL = "https://myapi.fyers.in/docsv3"
AUTHCODE_URL = f"{API_BASE_URL}/generate-authcode"
VALIDATE_AUTHCODE_URL = f"{API_BASE_URL}/validate-authcode"
VALIDATE_REFRESH_URL = f"{API_BASE_URL}/validate-refresh-token"

EP_HISTORY = "/history"
EP_QUOTES = "/quotes"
EP_CHAIN = "/options-chain-v3"
EP_PROFILE = "/profile"

RATE_PER_S = 3.0  # secondary short-burst guard; durable governor in limits.py enforces 4/s, 40/min, 4000/day
QUOTES_MAX = 50
CHAIN_STRIKECOUNT = 50  # hard cap (F5)
MINUTE_SPAN_DAYS = 100
DAY_SPAN_DAYS = 366
RESOLUTIONS = {1: "1", 2: "2", 3: "3", 5: "5", 10: "10", 15: "15", 20: "20", 30: "30", 60: "60"}
AUTH_CODES_EXPIRED = frozenset({"-8"})
AUTH_CODES_INVALID = frozenset({"-15", "-16", "-17"})

MAX_FEED_INSTRUMENTS = 100  # FYERS Standard, per user across sockets (effective 2026-10-16)
WS_CHANNEL = 11  # the SDK's default channel (data_ws.py:207)
WS_SOURCE = "PythonSDK-3.0.9"  # data_ws.py:206 -- the source string the server already sees from the SDK
PING = bytes([0, 1, 11])
NO_VALUE = -2147483648
DATA_VAL = (
    "ltp", "vol_traded_today", "last_traded_time", "exch_feed_time", "bid_size", "ask_size", "bid_price",
    "ask_price", "last_traded_qty", "tot_buy_qty", "tot_sell_qty", "avg_trade_price", "OI", "low_price",
    "high_price", "Yhigh", "Ylow", "lower_ckt", "upper_ckt", "open_price", "prev_close_price", "type", "symbol",
)  # map.json data_val, in wire order
SCALED = frozenset({"ltp", "bid_price", "ask_price", "avg_trade_price", "low_price", "high_price", "open_price",
                    "prev_close_price"})
SEG_TOPIC = {"NSE_FNO": "nse_fo", "NSE_EQ": "nse_cm"}
TOPIC_SEG = {v: k for k, v in SEG_TOPIC.items()}

# server response types (data_ws.py:1290-1317)
RESP_AUTH = 1
RESP_SUBSCRIBE = 4
RESP_UNSUBSCRIBE = 5
RESP_DATA = 6
RESP_MODE = 12
ENTRY_SNAPSHOT = 83
ENTRY_UPDATE = 85
ENTRY_LITE = 76


# ------------------------------------------------------------------------------------------ REST parsers
def epoch_ist_date(epoch_s: float) -> date:
    """The IST calendar date of a true-epoch instant (a daily bar at 18:30Z belongs to the NEXT day in IST)."""
    return datetime.fromtimestamp(float(epoch_s), IST).date()


def _num(v: Any) -> float | None:
    if v is None or v == "" or isinstance(v, bool):
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return None if math.isnan(f) else f


def parse_candles(body: Any, *, with_oi: bool) -> list[Candle]:
    """{"candles": [[epoch_s, o, h, l, c, v(, oi)]]} -> Candles oldest first (duplicate stamps collapse)."""
    rows = body.get("candles") if isinstance(body, Mapping) else None
    out: dict[int, Candle] = {}
    for r in rows or []:
        if not isinstance(r, list | tuple) or len(r) < 6:
            continue
        ts, o, h, lo, c, v = (_num(x) for x in r[:6])
        if None in (ts, o, h, lo, c):
            continue
        oi = _num(r[6]) if with_oi and len(r) > 6 else None
        ms = round(float(ts) * 1000)  # type: ignore[arg-type]
        out[ms] = Candle(ms, float(o), float(h), float(lo), float(c), float(v or 0.0), oi)  # type: ignore[arg-type]
    return [out[k] for k in sorted(out)]


def parse_expiry_data(body: Any) -> list[tuple[date, str]]:
    """data.expiryData[{date "DD-MM-YYYY", expiry "<epoch s>"}] -> [(date, epoch string)] ascending, unique."""
    data = body.get("data") if isinstance(body, Mapping) else None
    out: dict[date, str] = {}
    for e in (data or {}).get("expiryData") or []:
        try:
            dd, mm, yyyy = str(e.get("date")).split("-")
            d = date(int(yyyy), int(mm), int(dd))  # "DD-MM-YYYY", an IST calendar date
        except (AttributeError, TypeError, ValueError):
            continue
        out.setdefault(d, str(e.get("expiry") or ""))
    return sorted(out.items())


@dataclass(frozen=True)
class ChainLeg:
    symbol: str
    option_type: str
    strike: float
    ltp: float | None
    oi: int | None
    volume: int | None
    prev_oi: int | None


def parse_chain(body: Any) -> list[ChainLeg]:
    """data.optionsChain[] -> option legs only (the spot row has option_type "" and strike -1)."""
    data = body.get("data") if isinstance(body, Mapping) else None
    out: list[ChainLeg] = []
    for r in (data or {}).get("optionsChain") or []:
        side = str(r.get("option_type") or "")
        strike = _num(r.get("strike_price"))
        if side not in ("CE", "PE") or strike is None or strike < 0 or not r.get("symbol"):
            continue
        oi, vol, poi = _num(r.get("oi")), _num(r.get("volume")), _num(r.get("prev_oi"))
        out.append(
            ChainLeg(
                str(r["symbol"]),
                side,
                strike,
                _num(r.get("ltp")),
                None if oi is None else int(oi),
                None if vol is None else int(vol),
                None if poi is None else int(poi),
            )
        )
    return out


def parse_quotes_ltp(body: Any) -> dict[str, float]:
    """d[{n, s, v{lp}}] -> {ticker: lp}; rows with s != "ok" or no lp are absent (never zero-filled)."""
    rows = body.get("d") if isinstance(body, Mapping) else None
    out: dict[str, float] = {}
    for r in rows or []:
        if not isinstance(r, Mapping) or str(r.get("s", "ok")) != "ok":
            continue
        v = r.get("v") if isinstance(r.get("v"), Mapping) else {}
        lp = _num(v.get("lp"))
        name = r.get("n") or v.get("symbol")
        if name and lp is not None:
            out[str(name)] = lp
    return out


def error_code(body: Any) -> str | None:
    """The Fyers error code of an {"s": "error"} body, else None."""
    if isinstance(body, Mapping) and str(body.get("s", "")).lower() == "error":
        return str(body.get("code", ""))
    return None


# ------------------------------------------------------------------------------------------ JWT (no verify)
def jwt_claims(token: str) -> dict[str, Any] | None:
    """The unverified JWT payload of a Fyers access token (data_ws.py:289-310), or None if it is not a JWT."""
    parts = (token or "").split(":")[-1].split(".")
    if len(parts) != 3:
        return None
    try:
        claims = json.loads(base64.urlsafe_b64decode(parts[1] + "==="))
    except (ValueError, TypeError):
        return None
    return claims if isinstance(claims, dict) else None


# ------------------------------------------------------------------------------------------ HSM client frames
def _field(fid: int, value: bytes) -> bytes:
    return struct.pack("!BH", fid, len(value)) + value


def auth_frame(hsm_key: str, source: str = WS_SOURCE) -> bytes:
    """Type 1, 4 fields: hsm key, mode "P", 1, source (data_ws.py:325-378)."""
    body = bytes([1, 4]) + _field(1, hsm_key.encode()) + _field(2, b"P") + _field(3, bytes([1])) + _field(
        4, source.encode()
    )
    return struct.pack("!H", len(body)) + body


def full_mode_frame(channel: int = WS_CHANNEL) -> bytes:
    """Type 12, channel bitmask + mode 70 = full (data_ws.py:421-460; the length word is sent as 0)."""
    return struct.pack(">HBB", 0, 12, 2) + struct.pack(">BHQ", 1, 8, 1 << channel) + struct.pack(">BHB", 2, 1, 70)


def subscribe_frame(topics: Iterable[str], channel: int = WS_CHANNEL) -> bytes:
    """Type 4: u16 count + (u8 len + ascii topic)* and the channel (data_ws.py:462-507)."""
    ts = [t.encode("ascii") for t in topics]
    scrips = struct.pack(">H", len(ts)) + b"".join(bytes([len(t)]) + t for t in ts)
    body = bytes([4, 2, 1]) + struct.pack(">H", len(scrips)) + scrips + bytes([2]) + struct.pack(">H", 1) + bytes(
        [channel]
    )
    return struct.pack(">H", len(body)) + body


def ack_frame(message_number: int) -> bytes:
    """Type 3, one u32 field (data_ws.py:630-660)."""
    return struct.pack(">HBBBHI", 9, 3, 1, 1, 4, message_number)


def topic_for(segment: str, security_id: int) -> str | None:
    seg = SEG_TOPIC.get(segment)
    return None if seg is None else f"sf|{seg}|{int(security_id)}"


# ------------------------------------------------------------------------------------------ HSM server frames
@dataclass(frozen=True)
class AuthResult:
    ok: bool
    ack_count: int


def parse_auth(data: bytes) -> AuthResult:
    """Type 1 response: field 1 "K" = accepted; then u32 ack_count (data_ws.py:662-706)."""
    off = 5
    n = struct.unpack("!H", data[off : off + 2])[0]
    off += 2
    ok = data[off : off + n].decode("utf-8", "replace") == "K"
    off += n + 1
    ack = 0
    if len(data) >= off + 6:
        off += 2
        ack = struct.unpack(">I", data[off : off + 4])[0]
    return AuthResult(ok, ack)


@dataclass
class TopicState:
    topic: str
    values: dict[str, int]
    multiplier: int = 1
    precision: int = 0


@dataclass(frozen=True)
class DataFrame:
    message_number: int
    updated: tuple[int, ...]  # topic ids whose state changed, in wire order


def parse_data(data: bytes, topics: dict[int, TopicState]) -> DataFrame:
    """Apply one type-6 frame to `topics` (topic id -> state). Unknown/lite entries for unknown ids are skipped;
    a truncated frame raises struct.error / IndexError for the caller to count."""
    msg_no = struct.unpack(">I", data[3:7])[0]
    count = struct.unpack("!H", data[7:9])[0]
    off = 9
    updated: list[int] = []
    for _ in range(count):
        kind = data[off]
        off += 1
        if kind == ENTRY_SNAPSHOT:
            tid = struct.unpack("<H", data[off : off + 2])[0]
            off += 2
            nlen = data[off]
            off += 1
            name = data[off : off + nlen].decode("utf-8", "replace")
            off += nlen
            nfields = data[off]
            off += 1
            vals = struct.unpack(f">{nfields}i", data[off : off + 4 * nfields])
            off += 4 * nfields + 2
            mult = struct.unpack(">H", data[off : off + 2])[0]
            off += 2
            prec = data[off]
            off += 1
            for _s in range(3):  # exchange, exchange token, symbol
                off += 1 + data[off]
            st = TopicState(name, {}, mult or 1, prec)
            for i, v in enumerate(vals):
                if v != NO_VALUE and i < len(DATA_VAL):
                    st.values[DATA_VAL[i]] = v
            topics[tid] = st
            updated.append(tid)
        elif kind == ENTRY_UPDATE:
            tid = struct.unpack("<H", data[off : off + 2])[0]
            off += 2
            nfields = data[off]
            off += 1
            vals = struct.unpack(f">{nfields}i", data[off : off + 4 * nfields])
            off += 4 * nfields
            st = topics.get(tid)
            if st is None:
                continue
            changed = False
            for i, v in enumerate(vals):
                if v != NO_VALUE and i < len(DATA_VAL) and st.values.get(DATA_VAL[i]) != v:
                    st.values[DATA_VAL[i]] = v
                    changed = True
            if changed:
                updated.append(tid)
        elif kind == ENTRY_LITE:
            tid = struct.unpack("<H", data[off : off + 2])[0]
            off += 2
            v = struct.unpack(">i", data[off : off + 4])[0]
            off += 4
            st = topics.get(tid)
            if st is not None and v != NO_VALUE and st.values.get("ltp") != v:
                st.values["ltp"] = v
                updated.append(tid)
        else:
            break  # unknown entry kind: the rest of the frame cannot be framed
    return DataFrame(msg_no, tuple(updated))


def scaled(st: TopicState, name: str) -> float | None:
    v = st.values.get(name)
    if v is None:
        return None
    return v / ((10**st.precision) * st.multiplier)
