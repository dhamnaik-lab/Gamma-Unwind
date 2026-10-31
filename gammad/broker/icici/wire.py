"""Breeze wire facts: tests/fixtures/icici/README.md A1-A3, F1-F4, H1, M1, C1.

Vendor SDK is a wire reference only; no vendor runtime dependency.
"""

from __future__ import annotations

import base64
import csv
import hashlib
import io
import json
import math
from datetime import date, datetime
from zipfile import ZipFile

from gammad.broker.base import BrokerAuthError, BrokerError, Candle
from gammad.config import IST
from gammad.watch.feed_protocol import Quote

BASE = "https://api.icicidirect.com/breezeapi/api/v1/"
HISTORY = "https://breezeapi.icicidirect.com/api/v2/historicalcharts"
MASTER = "https://directlink.icicidirect.com/MotherAppMaster/SecurityMaster.zip"
WS = "wss://livestream.icicidirect.com/socket.io/?EIO=4&transport=websocket"
MAX_FEED = 100  # Local conservative guard; vendor maximum UNSOURCED.
DAILY_QUOTA = 5000


def session_parts(token: str) -> tuple[str, str]:
    try:
        user, key = base64.b64decode(token, validate=True).decode("ascii").split(":", 1)
        if not user or not key:
            raise ValueError
        return user, key
    except (ValueError, UnicodeError) as exc:
        raise BrokerAuthError("invalid pasted Breeze session token", code="809") from exc


def headers(app: str, secret: str, token: str, body: str, timestamp: str) -> dict[str, str]:
    digest = hashlib.sha256((timestamp + body + secret).encode()).hexdigest()
    return {
        "Content-Type": "application/json",
        "X-AppKey": app,
        "X-SessionToken": token,
        "X-Timestamp": timestamp,
        "X-Checksum": "token " + digest,
    }


def auth_code(status: int, body: object) -> str | None:
    error = str(body.get("Error") or "").lower() if isinstance(body, dict) else ""
    if "session" in error and ("expired" in error or "invalid" in error):
        return "807" if "expired" in error else "809"
    if status in (401, 403):
        return "809"
    return None


def number(value: object) -> float | None:
    if value is None or value == "":
        return None
    n = float(value)
    if not math.isfinite(n):
        raise ValueError("non-finite market value")
    return n


def epoch(value: object) -> int:
    """Raw numeric LTT is Unix seconds. Vendor text examples are IST wall time (F2/H1)."""
    if isinstance(value, int | float):
        return int(value * 1000)
    text = str(value).strip()
    for fmt in ("%a %b %d %H:%M:%S %Y", "%Y-%m-%d %H:%M:%S", "%d-%b-%Y %H:%M:%S"):
        try:
            return int(datetime.strptime(text, fmt).replace(tzinfo=IST).timestamp() * 1000)
        except ValueError:
            pass
    dt = datetime.fromisoformat(text)
    return int((dt if dt.tzinfo else dt.replace(tzinfo=IST)).timestamp() * 1000)


def expiry(value: str) -> date:
    for fmt in ("%d-%b-%Y", "%d-%b-%y", "%Y-%m-%d"):
        try:
            return datetime.strptime(value, fmt).replace(tzinfo=IST).date()
        except ValueError:
            pass
    return datetime.fromisoformat(value).date()


def candle(row: dict, oi: bool) -> Candle:
    return Candle(
        epoch(row["datetime"]),
        *(float(row[k]) for k in ("open", "high", "low", "close", "volume")),
        number(row.get("open_interest")) if oi else None,
    )


def quote(row: dict, options: bool) -> Quote:
    vol = number(row.get("total_quantity_traded"))
    oi = number(row.get("open_interest")) if options else None
    return Quote(
        number(row.get("ltp")),
        None if vol is None else int(vol),
        None if oi is None else int(oi),
        epoch(row["ltt"]) if row.get("ltt") else None,
    )


def event(name: str, data: object) -> str:
    return "42" + json.dumps([name, data], separators=(",", ":"))


def packet(raw: str | bytes) -> tuple[str, object]:
    text = raw.decode() if isinstance(raw, bytes) else raw
    if text.startswith("42"):
        return "event", json.loads(text[2:])
    if text.startswith("44"):
        return "error", json.loads(text[2:])
    if text.startswith("40"):
        return "connected", json.loads(text[2:] or "{}")
    if text.startswith("0"):
        return "open", json.loads(text[1:])
    if text.startswith("2"):
        return "ping", text[1:]
    if text in ("1", "41"):
        return "close", None
    return "ignore", None


def tick_fields(data: object) -> dict:
    """Raw stock event arrays (F2), plus SDK dictionary shape for offline text-time proof."""
    if isinstance(data, dict):
        return data
    if not isinstance(data, list) or len(data) not in (21, 23):
        raise ValueError("not a quote packet")
    fno = len(data) == 23
    out = {
        "symbol": data[0],
        "last": data[2],
        "ttq": data[14 if fno else 12],
        "ltt": data[21 if fno else 19],
        "close": data[22 if fno else 20],
    }
    if fno:
        out["OI"] = data[12]
    return out


def master_rows(raw: bytes) -> list[tuple[str, list[str]]]:
    """Read only NSE/FONSE text members in memory; never extract arbitrary archive paths (M1)."""
    out = []
    with ZipFile(io.BytesIO(raw)) as archive:
        if sum(i.file_size for i in archive.infolist()) > 80_000_000:
            raise BrokerError("Breeze master expands beyond safety limit")
        for name in archive.namelist():
            up = name.upper()
            if not up.endswith(".TXT"):
                continue
            seg = "NSE_FNO" if "FONSE" in up else "NSE_EQ" if "NSE" in up else None
            if seg:
                rows = csv.reader(io.StringIO(archive.read(name).decode("utf-8-sig")))
                out.extend((seg, [v.strip() for v in row]) for row in rows if row and row[0].isdigit())
    return out
