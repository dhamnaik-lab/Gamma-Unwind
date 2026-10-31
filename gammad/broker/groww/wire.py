"""Groww Trade API wire facts + pure parsers (BROKER-ADAPTER-SPEC §8; research rows G1-G9). BETA: not verified live.

Sources (read 2026-09-25; the SDK is READ for wire facts, never installed -- spec §7):
  [SDK]   growwapi 1.5.0 wheel, growwapi/groww/client.py (PyPI, 2025-12-06)
  [LIVE]  https://groww.in/trade-api/docs/python-sdk/live-data        (get_quote / get_ltp / get_option_chain)
  [BT]    https://groww.in/trade-api/docs/python-sdk/backtesting      (get_expiries / get_historical_candles)
  [INTRO] https://groww.in/trade-api/docs/python-sdk                  (auth flows + rate-limit table)
  [INST]  https://groww.in/trade-api/docs/python-sdk/instruments      (instrument.csv columns)
  [M]     instrument.csv downloaded 2026-09-25 (137,223 lines): NSE CASH/FNO `exchange_token` == the canonical NSE
          token (spec §1: 68,374 = 68,374 OPTSTK, 2,663 = 2,663 EQ); NSE index rows carry the SYMBOL as their
          exchange_token (`NSE,NIFTY,NIFTY,NSE-NIFTY,NIFTY 50,IDX,CASH,...`).

Wire facts:
  * REST base https://api.groww.in/v1 [SDK client.py:127]; headers `Authorization: Bearer <access token>` +
    `x-api-version: 1.0` [SDK client.py:1363-1378]. Body envelope {"status": "SUCCESS"|"FAILURE", "payload": {...},
    "error": {"code", "message"}} [SDK client.py:1485-1511]. HTTP 401 = authentication, 403 = authorisation,
    429 = rate limit [SDK client.py:32-39, exceptions.py].
  * Quote: GET /live-data/quote?exchange=NSE&segment=CASH|FNO&trading_symbol=... ONE instrument per call
    [SDK client.py:366-400]; payload last_price, volume (int), open_interest, last_trade_time ("epoch
    milliseconds" -- TRUE epoch, no IST shift) [LIVE "Get Quote"] (G2).
  * LTP: GET /live-data/ltp?segment=...&exchange_symbols=NSE_<trading_symbol> (repeated), <= 50 per call; payload
    {"NSE_RELIANCE": 2500.5} [SDK client.py:402-431, LIVE "Get LTP"] (G2).
  * Option chain: GET /option-chain/exchange/NSE/underlying/<SYMBOL>?expiry_date=YYYY-MM-DD; payload
    {"underlying_ltp", "strikes": {"<strike>": {"CE"|"PE": {trading_symbol, ltp, open_interest, volume}}}}
    [SDK client.py:490-520, LIVE "Get Option Chain"] (G3). Rows carry trading_symbol only -> native-id map.
  * Expiries: GET /historical/expiries?exchange=NSE&underlying_symbol=X[&year=Y]; no year = the current year only
    [SDK client.py:920-960, BT "Get Expiries"] (G3).
  * Candles: GET /historical/candles?exchange&segment&groww_symbol&start_time&end_time&candle_interval
    [SDK client.py:875-918]; rows [ts, open, high, low, close, volume, oi]; `ts` is "yyyy-MM-ddTHH:mm:ss" with NO
    zone = IST WALL CLOCK of the bar OPEN (sample: start 10:56 -> first bar "2025-09-24T10:30:00" for 30-min bars)
    [BT "Get Historical Candles"]; OI "only for FNO instruments, null for others" (G4). Max span per request:
    1-5 min 30 days, 10-30 min 90 days, 1 h / 1 day 180 days [BT "Backtesting Data Limits"].
  * Rate limits by TYPE [INTRO "Rate Limits"]: Live Data (quote, LTP, OHLC) 10/s + 300/min; token endpoint 5/s,
    30/min and 150 per 24 h (G6). Historical/option-chain are not in the table: a conservative 4/s is used.
  * Auth [SDK client.py:1514-1580, INTRO]: POST https://api.groww.in/v1/token/api/access with
    `Authorization: Bearer <API key>` and JSON {"key_type": "totp", "totp": "<6 digits>"} (TOTP flow: the TOTP
    token "No Expiry") or {"key_type": "approval", "checksum": sha256(secret + ts), "timestamp": ts} (key+secret
    flow, needs daily approval on the Groww Cloud page); the response's "token" is the access token. The docs state
    no access-token expiry (research §3).
  * The live feed carries LTP + tsInMillis only (G1) -> caps.feed "poll": quotes every 60 s = DEGRADED (spec §4).
Units: `volume` is taken as QUANTITY (shares) -- the docs say "Volume of trades" with no unit; the live smoke run
(spec §10) must confirm it (a volume in lots fails the harness's lot-multiple rule on real data).
"""

from __future__ import annotations

import math
from collections.abc import Mapping
from datetime import date, datetime
from typing import Any

from gammad.broker.base import Candle
from gammad.config import IST
from gammad.watch.feed_protocol import Quote

API_BASE_URL = "https://api.groww.in/v1"
INSTRUMENT_CSV_URL = "https://growwapi-assets.groww.in/instruments/instrument.csv"
TOKEN_URL = "https://api.groww.in/v1/token/api/access"
DOC_URL = "https://groww.in/trade-api/docs/python-sdk"

EP_QUOTE = "/live-data/quote"
EP_LTP = "/live-data/ltp"
EP_CHAIN = "/option-chain/exchange/NSE/underlying/{symbol}"
EP_EXPIRIES = "/historical/expiries"
EP_CANDLES = "/historical/candles"
EP_PROFILE = "/user/detail"

EXCHANGE = "NSE"
SEG_CASH = "CASH"
SEG_FNO = "FNO"
# canonical segment -> Groww segment (gammad/broker/canonical.py CANONICAL_SEGMENTS)
SEGMENT = {"NSE_EQ": SEG_CASH, "NSE_FNO": SEG_FNO, "IDX_I": SEG_CASH}

LTP_MAX_IDS = 50  # [LIVE "Get LTP"]: up to 50 instruments per call
LIVE_RATE_PER_S = 4.5  # 270/min under the 300/min Live Data cap (and far under 10/s)
LIVE_BURST = 5.0
HIST_RATE_PER_S = 4.0  # not documented: conservative
POLL_S = 60.0  # spec §4 table
# One quote call per instrument per poll: at 4.5/s a 60 s cycle fits 270 instruments.
MAX_FEED_INSTRUMENTS = int(LIVE_RATE_PER_S * POLL_S)

# interval minutes -> (candle_interval, max days per request) [BT]
INTERVALS: dict[int, tuple[str, int]] = {
    1: ("1minute", 30),
    2: ("2minute", 30),
    3: ("3minute", 30),
    5: ("5minute", 30),
    10: ("10minute", 90),
    15: ("15minute", 90),
    30: ("30minute", 90),
    60: ("1hour", 180),
}
DAILY_INTERVAL = ("1day", 180)


def unwrap(body: Any) -> Any:
    """The SUCCESS payload (or the body itself when it carries no envelope) [SDK client.py:1509-1511]."""
    if isinstance(body, dict) and "payload" in body:
        return body["payload"]
    return body


def failure(body: Any) -> tuple[str, str] | None:
    """(code, message) of a FAILURE envelope, else None."""
    if isinstance(body, dict) and str(body.get("status", "")).upper() == "FAILURE":
        err = body.get("error") if isinstance(body.get("error"), dict) else {}
        return str(err.get("code") or ""), str(err.get("message") or err.get("displayMessage") or "")[:200]
    return None


def ist_wall_to_epoch_ms(text: str) -> int:
    """A zone-less Groww candle timestamp ("2026-09-24T09:15:00" or "2026-09-24 09:15:00") read as IST wall clock
    -> true epoch ms (09:15 IST == 03:45 UTC). A value that carries its own zone keeps it."""
    dt = datetime.fromisoformat(str(text).strip().replace(" ", "T"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=IST)
    return round(dt.timestamp() * 1000)


def _ts_ms(value: Any) -> int:
    if isinstance(value, int | float) and not isinstance(value, bool):
        v = float(value)
        return round(v if v > 1e12 else v * 1000)  # epoch seconds (deprecated endpoint) or ms
    return ist_wall_to_epoch_ms(str(value))


def _num(v: Any) -> float | None:
    if v is None or v == "":
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return None if math.isnan(f) else f


def parse_candles(payload: Any, *, with_oi: bool) -> list[Candle]:
    """{"candles": [[ts, o, h, l, c, v, oi], ...]} -> Candles oldest first (duplicates by ts collapse)."""
    rows = (payload or {}).get("candles") if isinstance(payload, dict) else None
    out: dict[int, Candle] = {}
    for r in rows or []:
        if not isinstance(r, list | tuple) or len(r) < 6:
            continue
        o, h, lo, c, v = (_num(x) for x in r[1:6])
        if None in (o, h, lo, c):
            continue
        oi = _num(r[6]) if with_oi and len(r) > 6 else None
        ts = _ts_ms(r[0])
        out[ts] = Candle(ts, float(o), float(h), float(lo), float(c), float(v or 0.0), oi)  # type: ignore[arg-type]
    return [out[k] for k in sorted(out)]


def parse_expiries(payload: Any) -> list[date]:
    items = (payload or {}).get("expiries") if isinstance(payload, dict) else None
    return sorted({date.fromisoformat(str(x)[:10]) for x in items or []})


def parse_chain(payload: Any) -> list[tuple[float, str, str, float | None, int | None, int | None]]:
    """-> [(strike, "CE"|"PE", trading_symbol, ltp, oi, volume)] (native; the adapter maps symbols to tokens)."""
    strikes = (payload or {}).get("strikes") if isinstance(payload, dict) else None
    out: list[tuple[float, str, str, float | None, int | None, int | None]] = []
    for k, sides in (strikes or {}).items():
        strike = _num(k)
        if strike is None or not isinstance(sides, dict):
            continue
        for side in ("CE", "PE"):
            leg = sides.get(side)
            if not isinstance(leg, dict) or not leg.get("trading_symbol"):
                continue
            oi = _num(leg.get("open_interest"))
            vol = _num(leg.get("volume"))
            out.append(
                (
                    strike,
                    side,
                    str(leg["trading_symbol"]),
                    _num(leg.get("ltp")),
                    None if oi is None else int(oi),
                    None if vol is None else int(vol),
                )
            )
    return out


def parse_ltp(payload: Any) -> dict[str, float]:
    """{"NSE_RELIANCE": 2500.5} -> {"NSE_RELIANCE": 2500.5}; non-numeric values dropped (never zero-filled)."""
    out: dict[str, float] = {}
    if isinstance(payload, Mapping):
        for k, v in payload.items():
            f = _num(v.get("ltp") if isinstance(v, Mapping) else v)
            if f is not None:
                out[str(k)] = f
    return out


def parse_quote(payload: Any, *, has_oi: bool) -> Quote | None:
    """One get_quote payload -> Quote. `has_oi` False (equities/indices) never reports OI."""
    if not isinstance(payload, Mapping):
        return None
    ltp = _num(payload.get("last_price"))
    vol = _num(payload.get("volume"))
    oi = _num(payload.get("open_interest")) if has_oi else None
    ltt = _num(payload.get("last_trade_time"))
    if ltp is None and vol is None and oi is None:
        return None
    return Quote(
        ltp,
        None if vol is None else int(vol),
        None if oi is None else int(oi),
        None if not ltt or ltt <= 0 else int(ltt),
    )


def exchange_symbol(trading_symbol: str) -> str:
    """get_ltp's key form: "NSE_" + trading symbol [INST "Using CSV Data"]."""
    return f"{EXCHANGE}_{trading_symbol}"
