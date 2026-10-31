"""INDstocks read-only wire contract. Evidence IN2-IN8 in fixtures/indmoney/README.md."""

import csv
import io
from datetime import date, datetime, time

from gammad.broker.base import Candle
from gammad.broker.canonical import ContractKey
from gammad.config import IST

DOC_URL = "https://api-docs.indstocks.com/"
API_URL = "https://api.indstocks.com"
WS_URL = "wss://ws-prices.indstocks.com/api/v1/ws/prices"
MAX_FEED = 3000
QUOTE_BATCH = 1000
DAILY_QUOTA = 100_000
ENTITLEMENT_HINT = "Contact INDmoney support to enable market data access for the requested segment."
PREFIX = {"NSE_EQ": "NSE", "NSE_FNO": "NFO"}
MINUTES = (1, 2, 3, 4, 5, 10, 15, 30, 60, 120, 180, 240)


def midnight_ms(day: date) -> int:
    """Date bounds are IST; epoch values themselves never receive a timezone shift (IN4)."""
    return int(datetime.combine(day, time(), IST).timestamp() * 1000)


def candles(payload: dict, native: str, start: int, end: int) -> list[Candle]:
    """IN4: object candles, epoch seconds, no OI; sort/deduplicate and clip half-open bounds."""
    result = {}
    for row in (payload.get("data", {}).get(native, {}).get("candles") or []):
        stamp = int(row["ts"]) * 1000
        if start <= stamp < end:
            result[stamp] = Candle(stamp, *(float(row[k]) for k in ("o", "h", "l", "c", "v")), None)
    return [result[k] for k in sorted(result)]


def master_rows(text: str):
    """IN2: match contract identity, never assume vendor SECURITY_ID is the NSE token.

    Index CSV and index WS use different token examples without a mapping: withheld (U2).
    """
    for row in csv.DictReader(io.StringIO(text.lstrip("\ufeff"))):
        if row["EXCH"] != "NSE":
            continue
        kind = row.get("INSTRUMENT_NAME")
        if kind == "EQUITY" and row.get("SERIES") == "EQ":
            yield "NSE_EQ", row["SECURITY_ID"], ContractKey(row["TRADING_SYMBOL"])
        elif kind in ("OPTSTK", "OPTIDX") and row.get("OPTION_TYPE") in ("CE", "PE"):
            yield "NSE_FNO", row["SECURITY_ID"], ContractKey(
                row["SYMBOL_NAME"], date.fromisoformat(row["EXPIRY_DATE"][:10]),
                float(row["STRIKE_PRICE"]), row["OPTION_TYPE"],
            )


def auth_code(payload: dict, status: int = 200) -> int | None:
    """IN8: TokenException / HTTP 401 => 809; UserException / HTTP 403 => 806."""
    if status == 401 or payload.get("error_type") == "TokenException":
        return 809
    if status == 403 or payload.get("error_type") == "UserException":
        return 806
    return None


def subscribe_message(natives: list[str], action: str = "subscribe") -> dict:
    """IN5: only LTP response fields are documented, even for canonical FULL/QUOTE requests."""
    return {"action": action, "mode": "ltp", "instruments": natives}
