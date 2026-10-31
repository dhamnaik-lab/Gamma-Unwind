"""Paytm public wire facts; evidence PM55-* in tests/fixtures/paytm/README.md.

Independent decoder of the vendor's documented little-endian layouts. Offsets
include the packet-type byte. Times count seconds from 1980-01-01 UTC.
"""
from __future__ import annotations

import json
import math
import struct

API_BASE = "https://developer.paytmmoney.com"
DOC_URL = API_BASE + "/docs/api/live-market-data-webSocket-streaming/"
WS_URL = "wss://developer-ws.paytmmoney.com/broadcast/user/v1/data"
LIVE = "/data/v1/price/live"
PROFILE = "/accounts/v1/user/details"
CHAIN = "/fno/v1/option-chain"
EXPIRIES = "/fno/v1/option-chain/config"
MAX_FEED = 500
ENTITLEMENT_HINT = "Check your market data access with Paytm Money support."
# Local conservative policy, NOT a sourced vendor limit (PM55-U3).
RATE = 0.5
BATCH = 20
SIZES = {61: 23, 62: 67, 63: 175, 64: 23, 65: 43, 66: 39}
SCRIP = {"NSE_EQ": "EQUITY", "NSE_FNO": "OPTION", "IDX_I": "INDEX"}


def epoch_ms(seconds: int) -> int | None:
    return (int(seconds) + 315532800) * 1000 if seconds > 0 else None


def auth_code(status: int, body: dict) -> int | None:
    if status == 401 or body.get("error_code") == "PM_OPEN_API_0001":
        return 809
    if status == 403:
        return 806
    return None


def text_auth(text: str) -> int | None:
    try:
        body = json.loads(text)
    except ValueError:
        return None
    return auth_code(body.get("status", 0), body) if isinstance(body, dict) else None


def subscribe(items: list[tuple[str, int, str]]) -> str:
    return json.dumps([
        {"actionType": "ADD", "modeType": "LTP" if mode == "TICKER" else mode,
         "scripType": SCRIP[seg], "exchangeType": "NSE", "scripId": str(sid)}
        for seg, sid, mode in items
    ])


def decode(data: bytes) -> list[dict]:
    """Decode a complete concatenated frame; refuse unknown/truncated frames atomically.

    Depth and price fields are retained for independent offset tests. CAS is
    deliberately unsupported: it is not a traded-price update.
    """
    out = []
    pos = 0
    while pos < len(data):
        kind = data[pos]
        size = SIZES.get(kind)
        if size is None or pos + size > len(data):
            return []
        packet = data[pos:pos + size]
        pos += size

        def read(offset, fmt="I", packet=packet):
            return struct.unpack_from("<" + fmt, packet, offset)[0]

        offset = 101 if kind == 63 else 1
        index = kind in (64, 65, 66)
        fields = {"kind": kind, "index": index, "last_price": round(read(offset, "f"), 2)}
        if not math.isfinite(fields["last_price"]) or fields["last_price"] < 0:
            return []
        if kind in (65, 66):
            fields.update(security_id=read(5), tradable=packet[9], mode=packet[10])
            for name, at in (("open", 11), ("close", 15), ("high", 19), ("low", 23),
                             ("change_percent", 27), ("change_absolute", 31)):
                fields[name] = round(read(at, "f"), 2)
            if kind == 66:
                fields["ts_ms"] = epoch_ms(read(35))
            else:
                fields.update(fifty_two_week_high=round(read(35, "f"), 2),
                              fifty_two_week_low=round(read(39, "f"), 2))
        else:
            fields.update(ts_ms=epoch_ms(read(offset + 4)), security_id=read(offset + 8),
                          tradable=packet[offset + 12], mode=packet[offset + 13])
            if kind in (61, 64):
                fields.update(change_absolute=round(read(15, "f"), 2),
                              change_percent=round(read(19, "f"), 2))
            else:
                for name, at, fmt in (
                    ("last_traded_quantity", 14, "I"), ("average_traded_price", 18, "f"),
                    ("volume_traded", 22, "I"), ("total_buy_quantity", 26, "I"),
                    ("total_sell_quantity", 30, "I"), ("open", 34, "f"), ("close", 38, "f"),
                    ("high", 42, "f"), ("low", 46, "f"), ("change_percent", 50, "f"),
                    ("change_absolute", 54, "f"), ("fifty_two_week_high", 58, "f"),
                    ("fifty_two_week_low", 62, "f"),
                ):
                    value = read(offset + at, fmt)
                    fields[name] = round(value, 2) if fmt == "f" else value
                if kind == 63:
                    fields["oi"] = read(167)
                    fields["change_oi"] = read(171, "i")
                    fields["depth"] = [struct.unpack_from("<IIhhff", packet, 1 + i * 20)
                                       for i in range(5)]
        out.append(fields)
    return out
