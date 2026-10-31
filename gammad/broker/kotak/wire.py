"""Kotak Neo Trade API wire facts + pure parsers (BROKER-ADAPTER-SPEC §8). No I/O here.

Source: github.com/Kotak-Neo/Kotak-Neo `docs/` (read 2026-09-25; research rows KN1-KN8):
  * authentication.md -- login is TWO fixed calls on https://mis.kotaksecurities.com with headers
    `Authorization: <access token from the NEO API dashboard>` (plain, no Bearer) + `neo-fin-key: neotradeapi`:
      POST /login/1.0/tradeApiLogin     {mobileNumber (+91...), ucc, totp}  -> data {token (view), sid (view)}
      POST /login/1.0/tradeApiValidate  {mpin}  + headers sid / Auth = the view pair
                                        -> data {token (trade), sid (session), baseUrl, feedUrl (websocket.md §2)}
    Error body {"status": "error", "message": ..., "errorCode": "401"}.
  * troubleshooting.md Q3, Q8 -- Quotes / Scripmaster (and the market-data endpoints below, "the Authorization
    header alone is sufficient ... a completed 2FA session is not required", expiries.md §6, option-chain.md §6)
    take ONLY `Authorization: <access token>`; baseUrl is per account, "stable for the day" (Q5). The trade token
    (which can place orders) is never needed for data and is never stored by this adapter.
  * historical-data.md -- GET {baseUrl}/market-data/1.0/historical/details?neosymbol=<seg>|<token>&fromdate&todate
    &interval (1min 3min 5min 10min 15min 30min 60min D W); rows [ts "...T09:15:00+0530", o, h, l, c, v, oi] with
    oi "Phase 1: not yet populated" (KN2) -> caps.candle_oi False; max range 1/3/5min 30 d, 10/15min 60 d,
    30/60min 90 d, D 180 d (KN3). Errors {"status": "ERROR", "fault": {"code", "message"}}.
  * expiries.md -- GET {baseUrl}/market-data/1.0/watchlist/expiries?underlying=<pSymbolName>&exchange=nse_fo
    -> {"expiries": [ISO dates, ascending]} (KN4).
  * option-chain.md -- GET {baseUrl}/market-data/1.0/watchlist/option-chain?exchange=nse_fo&underlying&expiry
    [&count, multiple of 10, default 40 strikes] -> data {common_data {unlSymbol, expiryDt}, call[] / put[] of
    {instrument {neoSymbol "nse_fo|<token>", optionType, strikePrice}, quote {ltp (string), volume},
    openInterest {current, previous}}} (KN4).
  * quotes.md -- GET {baseUrl}/script-details/1.0/quotes/neosymbol/<seg>|<pSymbol>[,...][/<filter>] -> a JSON LIST of
    {exchange_token, exchange, ltp (string), lstup_time (unix seconds), ...}; indices by exact name ("Nifty 50").
    The quote documents NO day-volume field (only `last_volume` = most recent trade) and no OI field name, so
    `quotes()` takes F&O LTP / volume / OI from the documented option chain, and cash LTP from here (volume None).
    Errors {"stat": "Not_Ok", "emsg", "stCode"}.
  * instruments.md -- pSymbol is the token in quotes / websocket (`nse_cm|11536` = TCS, `nse_cm|1594` = INFY: the NSE
    exchange tokens). Kotak's master was NOT measured in spec §1 (it needs a login); the adapter therefore checks
    identity at run time against every option-chain row it reads (neoSymbol token vs the canonical master's
    underlying / expiry / strike / CE-PE) and drops a mismatch with an ERROR, never substituting.
  * websocket.md -- the feed URL (config service by dataCenter, else data.feedUrl, else the SDK's sfeed URL: see
    FEED_URL_FALLBACK below; https -> wss), auth = first JSON frame {user: UCC, auth: session sid, format
    "native_batch", source "NEOTRADEAPI", ...}; 1117 auth ok (+ dividers), 1119 initial, 1120 auth failed (the server
    then breaks the socket); subscribe {"event": "subscribeScrips" | "subscribeScripsLite" | "subscribeIndices",
    "inputtoken": "nse_fo|1,nse_fo|2"}; 3000 tokens (KN1); binary frames are BATCHES of packets, each starting with
    its own uint16 LE length; market picture (message 7208, level 2/4/8): token u32 @9, volume_traded_today i64 @29,
    last_trade_time i64 @37, last_traded_price u32 @69 / divider, open_interest u32 @103 (KN1); mini touch line
    (level 1): last_trade_time i64 @13, LTP u32 @21; index (7207): index_value i32 @29, last_trade_time u64 @33,
    name char[21] @66. No application heartbeat (the websockets library's protocol pings keep it alive).
  * Rate: 10 req/s with 429 back-off (KN5, KN6: historical 429s for hours were reported -- the adapter backs off and
    then fails loudly rather than retrying forever).

TIME RULES: candle stamps carry "+0530" -> honoured (09:15 IST == 03:45 UTC). `lstup_time` and the stream's
last_trade_time are unix epoch SECONDS (quotes.md sample 1757915078 = 11:14 IST, a market-hours instant, so true
epoch, not IST wall clock); a value above 1e11 is read as milliseconds. UNCONFIRMED live: the stream timestamp unit,
volume in quantity (not lots), and the auth-failure frame (documented 1120, fixture hand-built from websocket.md §3).
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

LOGIN_BASE_URL = "https://mis.kotaksecurities.com"
EP_LOGIN = "/login/1.0/tradeApiLogin"
EP_VALIDATE = "/login/1.0/tradeApiValidate"
EP_FILE_PATHS = "/script-details/1.0/masterscrip/file-paths"
EP_QUOTES = "/script-details/1.0/quotes/neosymbol/"
EP_HISTORICAL = "/market-data/1.0/historical/details"
EP_EXPIRIES = "/market-data/1.0/watchlist/expiries"
EP_CHAIN = "/market-data/1.0/watchlist/option-chain"
NEO_FIN_KEY = "neotradeapi"
DOC_URL = "https://github.com/Kotak-Neo/Kotak-Neo/tree/main/docs"

# Live-feed URL, in kotakneoapi 3.0.7's order (neo_api.py create_websocket; utils/neo_utility.py
# resolve_dynamic_urls; utils/urls.py; read 2026-09-29, docs/research/2026-09-29-broker-audit/AUDIT.md change 1):
# (a) the config service, keyed by tradeApiValidate `dataCenter` -> (b) `data.feedUrl` -> (c) FEED_URL_FALLBACK.
# The SDK's validate "Real API Response" sample (docs/functions/authentication/totp_validate.md) has NO feedUrl.
CONFIG_SERVICE_URL = "https://lapi.kotaksecurities.com/5config/config"  # SDK CONFIG_SERVICE_URL_PROD
CONFIG_SERVICE_PARAMS = {"appVersion": "3.0.7", "platform": "api", "environment": "prod"}  # appVersion unvalidated
CONFIG_SERVICE_TIMEOUT_S = 5.0  # the SDK's timeout for this call
FEED_URL_FALLBACK = "wss://sfeed.kotaksecurities.com/apifeed"  # SDK SFEED_WEBSOCKET_URL

# Canonical segment -> Kotak exchange segment string; stream exchange_id enum (websocket.md §9).
EXCHANGE = {"NSE_EQ": "nse_cm", "NSE_FNO": "nse_fo", "IDX_I": "nse_cm"}
EXCHANGE_ID = {"nse_cm": 1, "nse_fo": 2}
# Index names as the quote / stream APIs spell them (quotes.md §7 glossary), keyed by canonical underlying.
INDEX_NAMES = {
    "NIFTY": "Nifty 50",
    "BANKNIFTY": "Nifty Bank",
    "FINNIFTY": "Nifty Fin Service",
    "MIDCPNIFTY": "NIFTY MID SELECT",
    "NIFTYNXT50": "Nifty Next 50",
    "INDIAVIX": "INDIA VIX",
}

MAX_FEED_TOKENS = 3000  # websocket.md §4, KN1
RATE_PER_S = 10.0  # KN5
QUOTE_MAX_IDS = 20  # the client's measured limit 2026-09-28 (D125 K1: 50 failed, 20 worked). The SDK docs
# (quotes.md, read 2026-09-29) now say 50 instruments per call; we keep the measured 20. A 400 halves it for the
# session (gammad/broker/batching.py, T-159)
INTERVALS = {1: "1min", 3: "3min", 5: "5min", 10: "10min", 15: "15min", 30: "30min", 60: "60min"}
DAILY_INTERVAL = "D"
MAX_DAYS = {1: 30, 3: 30, 5: 30, 10: 60, 15: 60, 30: 90, 60: 90}  # KN3
DAILY_MAX_DAYS = 180  # KN3

# Stream
MSG_INDEX = 7207
MSG_PICTURE = 7208
MSG_AUTH_OK = 1117
MSG_INITIAL = 1119
MSG_AUTH_FAILED = 1120
LEVEL_MINI = 1
LEVELS_FULL = (2, 4, 8)
DEFAULT_DIVIDER = 100
EVENT_SCRIPS = "subscribeScrips"  # touch line: full market picture (volume, OI, trade time)
EVENT_LITE = "subscribeScripsLite"  # mini touch line: LTP + trade time
EVENT_INDEX = "subscribeIndices"

_HDR = struct.Struct("<HHbBBBB")  # length, message_code, exchange_id, level, reserved, seq, bitmask
_U32 = struct.Struct("<I")
_I32 = struct.Struct("<i")
_I64 = struct.Struct("<q")


def epoch_ms(dt: datetime) -> int:
    return round(dt.timestamp() * 1000)


def parse_candle_time(text: str) -> int:
    """'2026-09-24T09:15:00+0530' -> true epoch ms (offset honoured; a naive stamp is IST wall clock)."""
    dt = datetime.fromisoformat(str(text).strip())
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=IST)
    return epoch_ms(dt)


def unix_to_ms(value: Any) -> int | None:
    """Unix seconds (or milliseconds above 1e11) -> epoch ms; None for empty / zero / junk."""
    try:
        v = int(float(value))
    except (TypeError, ValueError):
        return None
    if v <= 0:
        return None
    return v if v > 100_000_000_000 else v * 1000


def _f(v: Any) -> float | None:
    if v in (None, ""):
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _i(v: Any) -> int | None:
    f = _f(v)
    return None if f is None else int(f)


def parse_candles(payload: Any) -> list[Candle]:
    """data.candles [[ts, o, h, l, c, v, oi]] -> Candles, oldest first. OI is dropped: "not yet populated" (KN2)."""
    data = payload.get("data") if isinstance(payload, Mapping) else None
    rows = data.get("candles") if isinstance(data, Mapping) else None
    out: dict[int, Candle] = {}
    for r in rows or []:
        if not isinstance(r, list | tuple) or len(r) < 6:
            continue
        ts = parse_candle_time(r[0])
        out[ts] = Candle(ts, float(r[1]), float(r[2]), float(r[3]), float(r[4]), float(r[5]), None)
    return [out[k] for k in sorted(out)]


# T-229 (5): non-ISO spellings a live account may send (the docs show ISO only; UNCONFIRMED live, D229).
_DATE_FORMATS = ("%d-%b-%Y", "%d%b%Y", "%d %b %Y", "%d-%B-%Y")


def parse_date(value: Any) -> date | None:
    """'2026-10-27' (or an ISO stamp), '27-Oct-2026', '27Oct2026', '27 Oct 2026' -> date; None for junk / empty."""
    s = str(value if value is not None else "").strip()
    if not s:
        return None
    try:
        return date.fromisoformat(s[:10])
    except ValueError:
        pass
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=IST).date()  # a calendar date, IST by definition
        except ValueError:
            continue
    return None


def expiry_values(payload: Any) -> list[Any] | None:
    """The raw expiry list: documented top-level `expiries` (KN4), else a `data.expiries` wrapper, else a `data` list
    (ASSUMED: the wrapper shapes are tolerated, not documented). None when no list is found."""
    if isinstance(payload, Mapping):
        rows = payload.get("expiries")
        if isinstance(rows, list):
            return rows
        data = payload.get("data")
        if isinstance(data, Mapping) and isinstance(data.get("expiries"), list):
            return data["expiries"]
        if isinstance(data, list):
            return data
    return None


def parse_expiries(payload: Any) -> list[date]:
    out = {d for d in (parse_date(s) for s in expiry_values(payload) or []) if d is not None}
    return sorted(out)


@dataclass(frozen=True)
class ChainEntry:
    token: int
    underlying: str
    expiry: date | None
    strike: float
    option_type: str
    ltp: float | None
    volume: int | None
    oi: int | None
    previous_oi: int | None
    previous_close: float | None = None  # quote.prevClose (option-chain.md); no previous-session volume is documented


def split_neo_symbol(text: Any) -> tuple[str, str] | None:
    seg, sep, tok = str(text or "").partition("|")
    return (seg.strip().lower(), tok.strip()) if sep else None


def _chain_rows(payload: Any) -> list[Any]:
    data = payload.get("data") if isinstance(payload, Mapping) else None
    if not isinstance(data, Mapping):
        return []
    out: list[Any] = []
    for side in ("call", "put"):
        rows = data.get(side)
        if isinstance(rows, list):
            out.extend(rows)
    return out


def chain_raw_rows(payload: Any) -> int:
    """Rows the chain body carried (call[] + put[]), parseable or not (the diag counters)."""
    return len(_chain_rows(payload))


def parse_chain(payload: Any, underlying: str = "", expiry: date | None = None) -> list[ChainEntry]:
    """option-chain data.call[] + data.put[] -> ChainEntry per row (identity fields kept for the guard).

    T-229 (5): when common_data's unlSymbol / expiryDt is absent or unparseable, the REQUESTED underlying / expiry is
    used. Safe: the identity guard still compares each row's token against the canonical master's own row (underlying,
    expiry, strike, CE/PE), so a token that is not the requested contract still drops; a parsed value always wins."""
    data = payload.get("data") if isinstance(payload, Mapping) else None
    if not isinstance(data, Mapping):
        return []
    common = data.get("common_data") if isinstance(data.get("common_data"), Mapping) else {}
    und = str(common.get("unlSymbol") or "").strip().upper() or str(underlying or "").strip().upper()
    exp = parse_date(common.get("expiryDt")) or expiry
    out: list[ChainEntry] = []
    for side in ("call", "put"):
        rows = data.get(side)
        for row in rows if isinstance(rows, list) else []:
            if not isinstance(row, Mapping):
                continue
            inst = row.get("instrument") if isinstance(row.get("instrument"), Mapping) else {}
            quote = row.get("quote") if isinstance(row.get("quote"), Mapping) else {}
            raw_oi = row.get("openInterest")
            # documented: {current, previous} (KN4). ASSUMED (T-229 (5), unconfirmed live): a flat number = current OI.
            oi = raw_oi if isinstance(raw_oi, Mapping) else ({"current": raw_oi} if _f(raw_oi) is not None else {})
            ns = split_neo_symbol(inst.get("neoSymbol"))
            strike = _f(inst.get("strikePrice"))
            opt = str(inst.get("optionType") or "").strip().upper()
            if ns is None or strike is None or opt not in ("CE", "PE"):
                continue
            try:
                token = int(ns[1])
            except ValueError:
                continue
            out.append(
                ChainEntry(
                    token=token,
                    underlying=und,
                    expiry=exp,
                    strike=strike,
                    option_type=opt,
                    ltp=_f(quote.get("ltp")),
                    volume=_i(quote.get("volume")),
                    oi=_i(oi.get("current")),
                    previous_oi=_i(oi.get("previous")),
                    previous_close=_f(quote.get("prevClose")),
                )
            )
    return out


# ------------------------------------------------------------------------------------------ diag shapes (T-229 (5))
# KEY NAMES and a few named market-data VALUES only (dates, symbols, strikes, the neoSymbol segment). Nothing here ever
# reads a credential: these bodies are option-chain / expiry responses, and every value is cut to 40 characters.
def _keys(v: Any) -> list[str] | str:
    return sorted(str(k) for k in v) if isinstance(v, Mapping) else type(v).__name__


def _val(v: Any) -> str:
    return repr(v)[:40]


def expiries_shape(payload: Any) -> dict[str, Any]:
    data = payload.get("data") if isinstance(payload, Mapping) else None
    rows = expiry_values(payload)
    return {
        "top_keys": _keys(payload),
        "data_keys": _keys(data) if data is not None else None,
        "first": [_val(v) for v in (rows or [])[:3]],
    }


def chain_shape(payload: Any) -> dict[str, Any]:
    data = payload.get("data") if isinstance(payload, Mapping) else None
    common = data.get("common_data") if isinstance(data, Mapping) else None
    rows = _chain_rows(payload)
    row = rows[0] if rows else None
    inst = row.get("instrument") if isinstance(row, Mapping) else None
    ns = split_neo_symbol(inst.get("neoSymbol")) if isinstance(inst, Mapping) else None
    return {
        "top_keys": _keys(payload),
        "data_keys": _keys(data) if data is not None else None,
        "common_data_keys": _keys(common) if common is not None else None,
        "row_keys": _keys(row) if row is not None else None,
        "instrument_keys": _keys(inst) if inst is not None else None,
        "quote_keys": _keys(row.get("quote")) if isinstance(row, Mapping) else None,
        "openInterest": _keys(row.get("openInterest")) if isinstance(row, Mapping) else None,
        "expiryDt": _val(common.get("expiryDt")) if isinstance(common, Mapping) else None,
        "unlSymbol": _val(common.get("unlSymbol")) if isinstance(common, Mapping) else None,
        "strikePrice": _val(inst.get("strikePrice")) if isinstance(inst, Mapping) else None,
        "neoSymbol_prefix": ns[0] if ns else None,
    }


def quote_rows(payload: Any) -> list[Mapping[str, Any]]:
    if isinstance(payload, list):
        return [r for r in payload if isinstance(r, Mapping)]
    if isinstance(payload, Mapping) and isinstance(payload.get("data"), list):
        return [r for r in payload["data"] if isinstance(r, Mapping)]
    return []


def error_text(body: Any, fallback: str = "") -> tuple[str | None, str]:
    """(code, message) from any of the three documented error shapes."""
    if isinstance(body, Mapping):
        fault = body.get("fault")
        if isinstance(fault, Mapping):
            return (str(fault.get("code")) if fault.get("code") is not None else None,
                    str(fault.get("message") or "")[:200])
        if str(body.get("stat") or "").lower() == "not_ok":
            return (str(body.get("stCode")) if body.get("stCode") is not None else None,
                    str(body.get("emsg") or "")[:200])
        if str(body.get("status") or "").lower() in ("error", "failed", "failure"):
            return (str(body.get("errorCode") or body.get("code") or "") or None,
                    str(body.get("message") or "")[:200])
    return None, fallback[:200]


def is_error(body: Any) -> bool:
    if isinstance(body, Mapping):
        if isinstance(body.get("fault"), Mapping):
            return True
        if str(body.get("stat") or "").lower() == "not_ok":
            return True
        if str(body.get("status") or "").lower() in ("error", "failed", "failure"):
            return True
    return False


# ------------------------------------------------------------------------------------------ stream
def feed_url_from_config(payload: Any, data_center: str) -> str | None:
    """Config-service body -> this data center's live-feed endpoint, or None (SDK resolve_dynamic_urls: two steps,
    data.configs["<dc>_broadcast_source"] = src, then data.configs["<dc>_<src>_broadcast_endpoint"])."""
    dc = (data_center or "").strip()
    data = payload.get("data") if isinstance(payload, Mapping) else None
    configs = data.get("configs") if isinstance(data, Mapping) else None
    if not dc or not isinstance(configs, Mapping):
        return None
    source = str(configs.get(f"{dc}_broadcast_source") or "").strip()
    if not source:
        return None
    url = str(configs.get(f"{dc}_{source}_broadcast_endpoint") or "").strip()
    return url or None


def feed_ws_url(feed_url: str) -> str:
    u = (feed_url or "").strip()
    if u.startswith("https://"):
        return "wss://" + u[len("https://") :]
    if u.startswith("http://"):
        return "ws://" + u[len("http://") :]
    return u


def auth_message(ucc: str, sid: str) -> str:
    return json.dumps(
        {
            "user": ucc,
            "auth": sid,
            "format": "native_batch",
            "source": "NEOTRADEAPI",
            "sdk_version": 1.1,
            "sdk_date": "2026-09-25",
        }
    )


def subscribe_message(event: str, tokens: list[str]) -> str:
    return json.dumps({"event": event, "inputtoken": ",".join(tokens)})


@dataclass(frozen=True)
class StreamPacket:
    kind: str  # "picture" | "mini" | "index"
    exchange_id: int
    token: int
    name: str | None
    ltt_raw: int
    ltp_raw: int
    volume: int | None
    oi: int | None


def split_batch(frame: bytes) -> list[bytes]:
    """websocket.md §6.1: each packet starts with its own uint16 LE total length; a truncated tail is skipped."""
    out: list[bytes] = []
    off = 0
    while off + 2 <= len(frame):
        size = int.from_bytes(frame[off : off + 2], "little")
        if size < _HDR.size or off + size > len(frame):
            break
        out.append(frame[off : off + size])
        off += size
    return out


def decode_packet(p: bytes) -> StreamPacket | None:
    """websocket.md §6.2 routing: 7207 index, then level 1 mini touch line, then level 2/4/8 market picture."""
    if len(p) < _HDR.size:
        return None
    _length, code, exch, level, _res, _seq, _bm = _HDR.unpack_from(p, 0)
    if code == MSG_INDEX:
        if len(p) < 87:
            return None
        name = p[66:87].split(b"\x00", 1)[0].decode("utf-8", "replace").strip()
        return StreamPacket("index", exch, _U32.unpack_from(p, 9)[0], name, _I64.unpack_from(p, 33)[0],
                            _I32.unpack_from(p, 29)[0], None, None)
    if code != MSG_PICTURE:
        return None
    if level == LEVEL_MINI:
        if len(p) < 25:
            return None
        return StreamPacket("mini", exch, _U32.unpack_from(p, 9)[0], None, _I64.unpack_from(p, 13)[0],
                            _U32.unpack_from(p, 21)[0], None, None)
    if level in LEVELS_FULL:
        if len(p) < 107:
            return None
        return StreamPacket("picture", exch, _U32.unpack_from(p, 9)[0], None, _I64.unpack_from(p, 37)[0],
                            _U32.unpack_from(p, 69)[0], _I64.unpack_from(p, 29)[0], _U32.unpack_from(p, 103)[0])
    return None


def dividers(msg: Mapping[str, Any]) -> dict[int, int]:
    """1117 / 1119 `exchanges` -> {exchange_id: divider} (missing -> 100 at lookup)."""
    out: dict[int, int] = {}
    ex = msg.get("exchanges")
    if isinstance(ex, Mapping):
        for name, v in ex.items():
            eid = EXCHANGE_ID.get(str(name).lower())
            d = _i(v.get("divider")) if isinstance(v, Mapping) else None
            if eid is not None and d:
                out[eid] = d
    return out
