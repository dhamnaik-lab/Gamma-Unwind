"""5paisa wire facts; evidence IDs and primary URLs: tests/fixtures/fivepaisa/README.md.

Independent implementation, no vendor SDK code incorporated. FP54-W documents
price and OI as separate messages; Time has no documented units, so it is NOT LTT.
"""

from __future__ import annotations

import base64
import csv
import io
import json
import math
import re
from datetime import date, datetime
from urllib.parse import quote

from gammad.broker.base import Candle
from gammad.broker.canonical import ContractKey
from gammad.config import IST

DOC_URL = "https://xstream.5paisa.com/dev-docs"
API_BASE = "https://openapi.5paisa.com"
SERVICE = "/VendorsAPI/Service1.svc/"
MASTER = SERVICE + "ScripMaster/segment/All"
SNAPSHOT = SERVICE + "MarketSnapshot"
EXPIRIES = SERVICE + "V2/GetExpiryForSymbolOptions"
CHAIN = SERVICE + "GetOptionsForSymbol"
# FP54-U: conservative local limits, NOT advertised broker limits.
RATE_PER_S = 2.0
QUOTE_BATCH = 50
MAX_FEED = 100
INTERVALS = (1, 5, 10, 15, 30, 60)
SEGMENTS = {"NSE_EQ": "C", "NSE_FNO": "D", "IDX_I": "C"}
_DATE = re.compile(r"/Date\((-?\d+)(?:[+-]\d{4})?\)/")


def timestamp_ms(value: object) -> int | None:
    """FP54-H/E: .NET epoch milliseconds; ISO wall dates interpreted in IST.

    A .NET +0530 suffix is display metadata, not an additional epoch shift.
    Numeric Time (e.g. 35950) is deliberately unsupported.
    """
    if not isinstance(value, str):
        return None
    match = _DATE.fullmatch(value)
    if match:
        return int(match[1])
    try:
        dt = datetime.fromisoformat(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=IST)
        return int(dt.timestamp() * 1000)
    except (ValueError, OverflowError):
        return None


def ist_date(value: object) -> date | None:
    ms = timestamp_ms(value)
    try:
        return datetime.fromtimestamp(ms / 1000, IST).date() if ms is not None else None
    except (ValueError, OverflowError, OSError):
        return None


def number(value: object) -> float | None:
    try:
        n = float(value) if value is not None and not isinstance(value, bool) else float("nan")
        return n if math.isfinite(n) and n >= 0 else None
    except (ValueError, TypeError):
        return None


def auth_code(body: object, status: int = 200) -> int | None:
    """FP54-A: Invalid Session/9 -> 809; HTTP 401/403 -> 809/806.

    No separate documented expired-token code; never infer expiry from status 2.
    """
    if status == 401:
        return 809
    if status == 403:
        return 806
    if isinstance(body, dict):
        b = body.get("body", body)
        h = body.get("head", {})
        if isinstance(b, dict) and (
            str(b.get("Status")) == "9" or str(b.get("Message", "")).lower() == "invalid session"
        ):
            return 809
        if isinstance(h, dict) and str(h.get("statusDescription", "")).lower() == "invalid session":
            return 809
    return None


def master_rows(text: str) -> list[tuple[tuple[str, str, int], str, ContractKey]]:
    """FP54-M: native triplet plus full contract identity; never assume token equality."""
    out = []
    for r in csv.DictReader(io.StringIO(text.lstrip("\ufeff"))):
        try:
            if r["Exch"] != "N" or r["ExchType"] not in ("C", "D"):
                continue
            side = r["ScripType"]
            symbol = (r.get("SymbolRoot") or r["Name"]).strip().upper()
            if not symbol:
                continue
            seg = "NSE_FNO" if r["ExchType"] == "D" else "IDX_I" if side == "EQ" else "NSE_EQ"
            expiry = ist_date(r["Expiry"]) if seg == "NSE_FNO" else None
            if seg == "NSE_FNO" and expiry is None:
                continue
            # Canonical futures have neither strike nor option side (vendor uses XX).
            option = side if seg == "NSE_FNO" and side in ("CE", "PE") else None
            strike = float(r["StrikeRate"]) if option else None
            key = ContractKey(symbol, expiry, strike, option)
            out.append((("N", r["ExchType"], int(r["ScripCode"])), seg, key))
        except (KeyError, TypeError, ValueError):
            continue
    return out


def candles(body: dict) -> list[Candle]:
    """FP54-H: six columns only; OI is absent, never zero-filled."""
    out = {}
    data = body.get("data", {})
    for row in data.get("candles", []) if isinstance(data, dict) else []:
        if not isinstance(row, list) or len(row) != 6:
            continue
        ts = timestamp_ms(row[0])
        values = [number(v) for v in row[1:]]
        if ts is not None and all(v is not None for v in values):
            out[ts] = Candle(ts, *values)
    return [out[t] for t in sorted(out)]


def subscribe_messages(client: str, natives: list[tuple[str, str, int]]) -> list[str]:
    """FP54-W: both channels subscribed; depth is NOT the OI channel."""
    messages = []
    for method, rows in (
        ("MarketFeedV3", natives),
        ("GetScripInfoForFuture", [n for n in natives if n[1] == "D"]),
    ):
        if rows:
            messages.append(
                json.dumps(
                    {
                        "Method": method,
                        "Operation": "Subscribe",
                        "ClientCode": client,
                        "MarketFeedData": [{"Exch": e, "ExchType": s, "ScripCode": i} for e, s, i in rows],
                    }
                )
            )
    return messages


def ws_url(token: str, client: str) -> str:
    """FP54-R: only fixed SDK-documented shards; JWT payload is a routing hint, not authentication."""
    shard = None
    try:
        payload = token.split(".")[1]
        shard = json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4))).get(
            "RedirectServer"
        )
    except (ValueError, IndexError, AttributeError, UnicodeError):
        pass
    host = {"A": "aopenfeed", "B": "bopenfeed"}.get(shard, "openfeed")
    return f"wss://{host}.5paisa.com/feeds/api/chat?Value1={quote(token, safe='')}|{quote(client, safe='')}"
