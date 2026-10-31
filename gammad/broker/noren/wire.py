"""Noren data-only wire primitives. Evidence: each fixtures/<broker>/README.md, N1-N12.

No vendor SDK code is copied. Unknown time/volume units are deliberately not promoted
to scanner inputs. A master must have named columns; positional guesses are refused.
"""

from __future__ import annotations

import csv
import io
import json
import math
import zipfile
from dataclasses import dataclass
from datetime import date, datetime

from gammad.broker.base import BrokerAuthError, BrokerError, Candle
from gammad.broker.canonical import ContractKey
from gammad.config import IST
from gammad.core import session_clock

INTERVALS = (1, 3, 5, 10, 15, 30, 60, 120, 240)
QUOTE_MAX_IDS = 1  # N6: GetQuotes takes ONE exch/token (Check Setup batch check, scripts/smoke_broker.py)
MAX_MASTER_BYTES = 32 * 1024 * 1024


@dataclass(frozen=True)
class NorenHost:
    broker: str
    rest_base: str
    ws_url: str | None
    master_urls: tuple[str, ...]
    login_style: str
    env_prefix: str
    rate_per_s: float = 0.5  # local conservative policy, NOT a vendor limit
    requests_per_minute: int | None = None  # only when documented by this host
    max_feed_instruments: int = 50  # local safety cap, NOT a documented entitlement
    heartbeat_s: float = 3.0
    connect_type: str = "c"
    token_field: str = "susertoken"
    # U4 / D726: True admits the documented `v` (cumulative day volume) ONLY through quantity_volume() below --
    # the unit is proved per value against the canonical lot, never assumed. False = no volume at all.
    volume_verified: bool = False
    feed_oi: bool = True


def quantity_volume(value: object, lot: int | None) -> int | None:
    """U4 guard (audit P2 brokers-beta, D726): a Noren `v` is admitted as QUANTITY only when it is a whole multiple
    of the contract's lot (every F&O trade is whole lots, so day quantity always is; a count in lots almost never
    is). lot None / <= 0 (unknown contract) -> None. Equity lot 1 always passes (lots == shares there)."""
    n = integer(value)
    if n is None or not isinstance(lot, int) or isinstance(lot, bool) or lot <= 0:
        return None
    return n if n % lot == 0 else None


def form(uid: str, token: str, values: dict) -> dict[str, str]:
    """N2: JSON in jData plus the pasted session token in jKey."""
    return {"jData": json.dumps({"uid": uid, **values}, separators=(",", ":")), "jKey": token}


def auth_code(body: object) -> str | None:
    if not isinstance(body, dict):
        return None
    message = str(body.get("emsg", "")).lower()
    if "session expired" in message:
        return "807"
    if "invalid session" in message or "invalid token" in message:
        return "809"
    if body.get("t") in ("ck", "ak") and str(body.get("s", "")).casefold() == "not_ok":
        return "809"
    return None


def check_response(body: object, status: int = 200) -> None:
    code = auth_code(body)
    if code is not None or status in (401, 403):
        # Never echo response bodies: an upstream error may repeat a credential.
        raise BrokerAuthError("Noren session rejected", code or "809", status)
    if status >= 400 or (isinstance(body, dict) and body.get("stat") == "Not_Ok"):
        raise BrokerError("Noren data request failed", status=status)


def number(value: object) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        n = float(value)
        return n if math.isfinite(n) and n >= 0 else None
    except (ValueError, TypeError):
        return None


def integer(value: object) -> int | None:
    n = number(value)
    return int(n) if n is not None and n.is_integer() else None


def ist_ms(text: str) -> int:
    """N7: exchange wall-clock candle time, interpreted explicitly in IST."""
    for fmt in ("%d-%m-%Y %H:%M:%S", "%d/%m/%Y %H:%M:%S"):
        try:
            return round(datetime.strptime(text, fmt).replace(tzinfo=IST).timestamp() * 1000)
        except ValueError:
            continue
    raise BrokerError("Noren candle has an invalid wall-clock time")


def candles(body: object, *, daily: bool, oi: bool, start: date, end: date) -> list[Candle]:
    """N7/N8: intv is interval volume; oi is level, intoi is CHANGE (never level).

    EOD returns an array of JSON strings in the documented example. Accept objects
    too. Sort and deduplicate timestamps; refuse malformed rows instead of zeros.
    """
    if not isinstance(body, list):
        raise BrokerError("Noren candle response must be a list")
    parsed: dict[int, Candle] = {}
    for raw in body:
        try:
            row = json.loads(raw) if isinstance(raw, str) else raw
            check_response(row)
            if not isinstance(row, dict):
                raise TypeError("row")
            ts = int(row["ssboe"]) * 1000 if daily else ist_ms(row["time"])
            dt = datetime.fromtimestamp(ts / 1000, IST)
            if not start <= dt.date() <= end:
                continue
            if not daily and not session_clock.in_fo_session(ts):
                continue
            vals = [number(row.get(k)) for k in ("into", "inth", "intl", "intc", "intv")]
            if any(v is None for v in vals):
                raise ValueError("OHLCV")
            level = number(row.get("oi")) if oi and not daily else None
            parsed[ts] = Candle(ts, *vals, level)
        except (TypeError, ValueError, KeyError) as exc:
            raise BrokerError("Malformed Noren candle") from exc
    return [parsed[ts] for ts in sorted(parsed)]


BASE_COLUMNS = frozenset({"Exchange", "Token", "Symbol", "TradingSymbol", "Instrument"})
OPTION_COLUMNS = frozenset({"Expiry", "OptionType", "StrikePrice"})
# casefolded header -> canonical name; only spellings seen in a host's published master (audit P2, D726).
HEADER_ALIASES = {
    "exchange": "Exchange", "token": "Token", "symbol": "Symbol", "tradingsymbol": "TradingSymbol",
    "instrument": "Instrument", "expiry": "Expiry", "optiontype": "OptionType", "strikeprice": "StrikePrice",
    "strike": "StrikePrice", "lotsize": "LotSize", "ticksize": "TickSize",
}


@dataclass(frozen=True)
class MasterRow:
    exchange: str
    token: str
    trading_symbol: str
    segment: str
    key: ContractKey

    @property
    def native(self) -> str:
        return f"{self.exchange}|{self.token}"


def master_rows(content: bytes) -> list[MasterRow]:
    """N4: ZIP bytes stay in memory. Explicit CSV headers only (schema gap U2).

    Neither tokens nor row order imply canonical identity. Consumers join ContractKey.
    Unknown archive layouts, duplicate ambiguous identities, and positional files fail closed.
    """
    try:
        if content.startswith(b"PK"):
            with zipfile.ZipFile(io.BytesIO(content)) as archive:
                files = [i for i in archive.infolist() if not i.is_dir()]
                if len(files) != 1 or files[0].file_size > MAX_MASTER_BYTES:
                    raise BrokerError("Unsupported Noren master archive")
                content = archive.read(files[0])
        if len(content) > MAX_MASTER_BYTES:
            raise BrokerError("Noren master exceeds size limit")
        reader = csv.DictReader(io.StringIO(content.decode("utf-8-sig")))
        # Audit P2 brokers-beta (D726), measured 2026-10-25 on the live public masters (header bytes only):
        # Shoonya NSE_symbols = Exchange,Token,LotSize,Symbol,TradingSymbol,Instrument,TickSize (no option
        # columns); Flattrade = Exchange,Token,Lotsize,Symbol,Tradingsymbol,Instrument,Expiry,Strike,Optiontype.
        # Names stay REQUIRED (no positional guess); spelling is matched through HEADER_ALIASES, and the option
        # columns are required only by an option row.
        names = {HEADER_ALIASES.get(n.strip().casefold(), n.strip()): n for n in (reader.fieldnames or []) if n}
        if not BASE_COLUMNS.issubset(names):
            raise BrokerError("Unverified Noren master layout: named CSV headers required")
        has_options = OPTION_COLUMNS.issubset(names)
        result = []
        for raw in reader:
            row = {canon: (raw.get(src) or "").strip() for canon, src in names.items()}
            exch = row["Exchange"]
            kind = row["Instrument"]
            if exch == "NSE" and row["TradingSymbol"].endswith("-EQ"):
                seg, key = "NSE_EQ", ContractKey(row["Symbol"].upper())
            elif exch == "NFO" and kind in ("OPTSTK", "OPTIDX"):
                if not has_options:
                    raise BrokerError("Unverified Noren master layout: option rows without option columns")
                if row["OptionType"] not in ("CE", "PE"):
                    continue
                expiry = datetime.strptime(row["Expiry"], "%d-%b-%Y").replace(tzinfo=IST).date()
                strike = number(row["StrikePrice"])
                if strike is None:
                    raise ValueError("strike")
                seg = "NSE_FNO"
                key = ContractKey(row["Symbol"].upper(), expiry, strike, row["OptionType"])
            else:
                continue
            if not row["Token"].isdigit() or not row["TradingSymbol"]:
                raise ValueError("token/symbol")
            result.append(MasterRow(exch, row["Token"], row["TradingSymbol"], seg, key))
        return result
    except (ValueError, KeyError, UnicodeError, zipfile.BadZipFile, csv.Error) as exc:
        raise BrokerError("Malformed Noren instrument master") from exc
