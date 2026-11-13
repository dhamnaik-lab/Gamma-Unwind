"""Definedge Securities pure protocol constants, URL builders, and CSV/JSON parsers.

This module intentionally performs NO network or SDK imports.
"""

from __future__ import annotations

import csv
import io
import json
import zipfile
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Sequence

from gammad.broker.base import BrokerAuthError, BrokerError, Candle, Instrument
from gammad.broker.canonical import ContractKey, SEG_EQ, SEG_IDX, SEG_OPT, norm_underlying
from gammad.core import session_clock

IST = timezone(timedelta(hours=5, minutes=30))

DOC_URL = "https://www.definedgesecurities.com/products/integrate/"
SDS_BASE_URL = "https://data.definedgesecurities.com"
DART_BASE_URL = "https://integrate.definedgesecurities.com"
ALLMASTER_URL = "https://app.definedgesecurities.com/public/allmaster.zip"

EP_HISTORY = "/sds/history/{segment}/{token}/{timeframe}/{from_dt}/{to_dt}"
EP_LIMITS = "/dart/v1/limits"
EP_QUOTES = "/dart/v1/quotes/{exch}/{token}"

MASTER_REFRESH_HM = (7, 30)
MASTER_KEEP = 4  # Current day + 3 prior cached CSV files

RATE_PER_S = 2.0
RATE_BURST = 3
COOLDOWN_S = 1.0

QUOTE_MAX_IDS = 1
LTP_MAX_IDS = 1

DAY_SPAN_DAYS = 365
MINUTE_SPAN_DAYS = 5
DAY_RETENTION_YEARS = 20
MINUTE_RETENTION_DAYS = 180
SUPPORTED_INTRADAY_MINS = (1, 5, 15, 60)

# Canonical underlying symbol -> (Definedge segment, Definedge token, recognized display/symbol aliases)
INDEX_MAP: dict[str, tuple[str, int, tuple[str, ...]]] = {
    "NIFTY": ("NSE", 26000, ("NIFTY", "NIFTY 50", "NIFTY50")),
    "BANKNIFTY": ("NSE", 26009, ("BANKNIFTY", "NIFTY BANK", "NIFTYBANK")),
    "FINNIFTY": ("NSE", 26037, ("FINNIFTY", "NIFTY FIN SERVICE", "NIFTY FINANCIAL SERVICES")),
    "MIDCPNIFTY": ("NSE", 26074, ("MIDCPNIFTY", "NIFTY MID SELECT", "NIFTY MIDCAP SELECT")),
    "INDIAVIX": ("NSE", 26017, ("INDIAVIX", "INDIA VIX")),
}

_INDEX_ALIAS_TO_CANON: dict[str, str] = {}
for _canon_sym, (_seg, _tok, _aliases) in INDEX_MAP.items():
    for _alias in _aliases:
        _INDEX_ALIAS_TO_CANON[" ".join(_alias.upper().split())] = _canon_sym

_SEGMENT_TO_CANON: dict[str, str] = {
    "NSE": SEG_EQ,
    "NFO": SEG_OPT,
}


class DefinedgeAuthError(BrokerAuthError):
    """Definedge rejected or expired the API session key (normalized 807/809)."""


@dataclass(frozen=True)
class DefinedgeRow:
    segment: str
    token: int
    symbol: str
    trading_symbol: str
    instrument_type: str
    underlying: str
    expiry_date: date | None
    strike: float | None
    option_type: str | None
    lot_size: int
    tick_size: float | None

    @property
    def canonical_segment(self) -> str:
        if self.segment == "NSE" and self.instrument_type in ("IDX", "INDEX"):
            return SEG_IDX
        return _SEGMENT_TO_CANON.get(self.segment, self.segment)

    def key(self) -> ContractKey:
        return ContractKey(
            norm_underlying(self.underlying or self.symbol),
            self.expiry_date,
            self.strike,
            self.option_type,
        )

    def to_instrument(self) -> Instrument:
        inst_type = "EQUITY" if (self.segment == "NSE" and self.instrument_type == "EQ") else self.instrument_type
        return Instrument(
            security_id=self.token,
            symbol=self.trading_symbol or self.symbol,
            exchange_segment=self.canonical_segment,
            instrument_type=inst_type,
            underlying_symbol=self.underlying or None,
            expiry_date=self.expiry_date,
            strike=self.strike,
            option_type=self.option_type,
            lot_size=self.lot_size,
            tick_size=self.tick_size,
        )


def _normalize_label(s: str) -> str:
    return " ".join(s.strip().upper().split())


def _parse_master_date(raw: str) -> date | None:
    s = raw.strip()
    if not s or s in ("0", "00000000", "01011980", "1980-01-01"):
        return None
    for fmt in ("%d%m%Y", "%Y-%m-%d", "%d-%m-%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def _parse_int_or(raw: str, default: int) -> int:
    s = raw.strip()
    if not s:
        return default
    try:
        return int(float(s))
    except ValueError:
        return default


def parse_allmaster_zip(zip_bytes: bytes) -> str:
    """Extract allmaster.csv text from Definedge's allmaster.zip archive."""
    if not zip_bytes:
        raise BrokerError("definedge: empty allmaster.zip response")
    try:
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            names = zf.namelist()
            target = next((n for n in names if n.lower().endswith("allmaster.csv")), None)
            if target is None:
                target = next((n for n in names if n.lower().endswith(".csv")), None)
            if target is None:
                raise BrokerError("definedge: no CSV entry found inside allmaster.zip")
            raw = zf.read(target)
    except zipfile.BadZipFile as exc:
        raise BrokerError(f"definedge: invalid allmaster.zip archive ({exc})") from exc
    if not raw or not raw.strip():
        raise BrokerError("definedge: empty allmaster.csv inside allmaster.zip")
    return raw.decode("utf-8", errors="replace")


def parse_allmaster_csv(text: str) -> list[DefinedgeRow]:
    """Parse Definedge's headerless 15-column allmaster.csv into DefinedgeRow items."""
    rows: list[DefinedgeRow] = []
    reader = csv.reader(io.StringIO(text))
    for col in reader:
        if not col or all(not c.strip() for c in col):
            continue
        if len(col) < 12:
            continue
        seg = col[0].strip().upper()
        if seg not in ("NSE", "NFO"):
            continue
        try:
            token = int(col[1].strip())
        except ValueError:
            continue
        sym = col[2].strip()
        tsym = col[3].strip()
        itype = col[4].strip().upper()
        if seg == "NSE" and itype not in ("EQ", "IDX", "INDEX"):
            continue
        if seg == "NFO" and itype not in ("FUTSTK", "FUTIDX", "OPTSTK", "OPTIDX"):
            continue
        if seg == "NSE" and itype == "EQ" and "-" in tsym and (sym and tsym.upper() != sym.upper()) and not tsym.upper().endswith("-EQ"):
            # Skip non-EQ series (e.g., BE/BL/SM) when trading symbol explicitly carries another series suffix
            continue

        expiry = _parse_master_date(col[5]) if seg == "NFO" else None
        opt_raw = col[8].strip().upper()
        opt = opt_raw if (seg == "NFO" and opt_raw in ("CE", "PE")) else None

        prec = _parse_int_or(col[10], 2)
        mult = _parse_int_or(col[11], 1)
        if mult <= 0:
            mult = 1
        scale = mult * (10 ** max(prec, 0))

        strike: float | None = None
        if opt is not None:
            strike_s = col[9].strip()
            if strike_s and scale > 0:
                try:
                    raw_strike = float(strike_s)
                except ValueError:
                    raw_strike = 0.0
                if raw_strike > 0:
                    strike = raw_strike / scale

        lot_size = max(1, _parse_int_or(col[7], 1))

        tick_raw = col[6].strip()
        tick_size: float | None = None
        if tick_raw:
            try:
                if "." in tick_raw:
                    tval = float(tick_raw)
                else:
                    tval = int(tick_raw) / scale
                if tval > 0:
                    tick_size = tval
            except ValueError:
                tick_size = None

        if seg == "NSE" and itype in ("IDX", "INDEX"):
            canon_idx = _INDEX_ALIAS_TO_CANON.get(_normalize_label(sym)) or _INDEX_ALIAS_TO_CANON.get(
                _normalize_label(tsym)
            )
            underlying = canon_idx if canon_idx is not None else norm_underlying(sym or tsym)
        else:
            underlying = norm_underlying(sym or tsym)

        rows.append(
            DefinedgeRow(
                segment=seg,
                token=token,
                symbol=sym,
                trading_symbol=tsym,
                instrument_type=itype,
                underlying=underlying,
                expiry_date=expiry,
                strike=strike,
                option_type=opt,
                lot_size=lot_size,
                tick_size=tick_size,
            )
        )
    return rows


class DefinedgeMaster:
    """Indexed view over Definedge allmaster.csv rows (NSE equities, NSE indices, NFO contracts)."""

    def __init__(self, rows: Sequence[DefinedgeRow]) -> None:
        self.rows = list(rows)
        self.by_code: dict[tuple[str, int], DefinedgeRow] = {}
        self.by_key: dict[ContractKey, DefinedgeRow] = {}
        self.index_by_name: dict[str, DefinedgeRow] = {}
        self._index_tokens_present: set[int] = set()

        for row in self.rows:
            if row.canonical_segment == SEG_IDX:
                self._index_tokens_present.add(row.token)
                self.index_by_name.setdefault(norm_underlying(row.underlying), row)
                self.by_code.setdefault((SEG_IDX, row.token), row)
            else:
                # Prefer -EQ series if two NSE EQ rows ever collide on token/key
                cseg = row.canonical_segment
                prev = self.by_code.get((cseg, row.token))
                if prev is None or (row.trading_symbol.upper().endswith("-EQ") and not prev.trading_symbol.upper().endswith("-EQ")):
                    self.by_code[(cseg, row.token)] = row
                ck = row.key()
                prev_k = self.by_key.get(ck)
                if prev_k is None or (row.trading_symbol.upper().endswith("-EQ") and not prev_k.trading_symbol.upper().endswith("-EQ")):
                    self.by_key[ck] = row

    def has_index_token(self, token: int) -> bool:
        return int(token) in self._index_tokens_present

    def instruments(self) -> list[Instrument]:
        return [r.to_instrument() for r in self.rows if r.canonical_segment in (SEG_EQ, SEG_OPT)]


def fallback_index_row(canon_symbol: str) -> DefinedgeRow | None:
    """Build a DefinedgeRow from INDEX_MAP when a minimal CSV omits index rows."""
    spec = INDEX_MAP.get(norm_underlying(canon_symbol))
    if spec is None:
        return None
    seg, token, aliases = spec
    display = aliases[1] if len(aliases) > 1 else aliases[0]
    return DefinedgeRow(
        segment=seg,
        token=token,
        symbol=display,
        trading_symbol=display,
        instrument_type="IDX",
        underlying=norm_underlying(canon_symbol),
        expiry_date=None,
        strike=None,
        option_type=None,
        lot_size=1,
        tick_size=0.05,
    )


def format_sds_dt(d: date, hm: str = "0000") -> str:
    return f"{d.strftime('%d%m%Y')}{hm}"


def history_url(segment: str, token: int, timeframe: str, from_date: date, to_date: date) -> str:
    path = EP_HISTORY.format(
        segment=segment,
        token=int(token),
        timeframe=timeframe,
        from_dt=format_sds_dt(from_date, "0000"),
        to_dt=format_sds_dt(to_date, "2359"),
    )
    return SDS_BASE_URL + path


def quotes_url(segment: str, token: int) -> str:
    return DART_BASE_URL + EP_QUOTES.format(exch=segment, token=int(token))


def limits_url() -> str:
    return DART_BASE_URL + EP_LIMITS


def parse_sds_timestamp(raw: str) -> datetime:
    """Parse Definedge SDS naive IST timestamp (ddMMyyyyHHmm; zero-pad to 12 chars)."""
    s = raw.strip()
    if not s.isdigit() or len(s) > 12:
        raise BrokerError(f"definedge: invalid SDS timestamp {raw!r}")
    padded = s.zfill(12)
    try:
        return datetime.strptime(padded, "%d%m%Y%H%M").replace(tzinfo=IST)
    except ValueError as exc:
        raise BrokerError(f"definedge: invalid SDS timestamp {raw!r}") from exc


def parse_sds_csv(
    text: str,
    *,
    timeframe: str,
    has_oi: bool,
    from_date: date,
    to_date: date,
) -> list[Candle]:
    """Parse headerless SDS CSV (6-col EQ/IDX or 7-col NFO) and clip to [from_date, to_date] IST."""
    stripped = text.strip()
    if not stripped:
        return []
    if stripped.startswith("{") or stripped.startswith("["):
        try:
            payload = json.loads(stripped)
        except ValueError as exc:
            raise BrokerError(f"definedge: unexpected non-CSV history response ({exc})") from exc
        raise_for_http_or_payload(200, stripped, payload=payload)
        return []

    by_ts: dict[int, Candle] = {}
    reader = csv.reader(io.StringIO(stripped))
    for col in reader:
        if not col or all(not c.strip() for c in col):
            continue
        if len(col) < 6:
            raise BrokerError(f"definedge: malformed SDS CSV row (expected >=6 cols, got {len(col)})")
        dt = parse_sds_timestamp(col[0])
        bar_date = dt.date()
        if bar_date < from_date or bar_date > to_date:
            continue
        if timeframe == "day":
            dt = dt.replace(hour=0, minute=0, second=0, microsecond=0)
        ts_ms = int(dt.timestamp() * 1000)
        try:
            o = float(col[1])
            h = float(col[2])
            low = float(col[3])
            c = float(col[4])
            vol = float(col[5])
            oi_val = float(col[6]) if (has_oi and len(col) >= 7 and col[6].strip() != "") else None
        except ValueError as exc:
            raise BrokerError(f"definedge: invalid numeric value in SDS row ({exc})") from exc
        by_ts[ts_ms] = Candle(ts_ms=ts_ms, open=o, high=h, low=low, close=c, volume=vol, oi=oi_val)
    return [by_ts[k] for k in sorted(by_ts)]


def resample_minute_candles(candles: Sequence[Candle], interval_min: int) -> list[Candle]:
    """Resample sorted 1m bars on IST session buckets (09:15-anchored via session_clock.fo_window)."""
    if interval_min not in SUPPORTED_INTRADAY_MINS:
        raise BrokerError(
            f"definedge: unsupported intraday interval {interval_min}m; expected {SUPPORTED_INTRADAY_MINS}"
        )
    if not candles:
        return []
    ordered = sorted({c.ts_ms: c for c in candles}.values(), key=lambda c: c.ts_ms)
    if interval_min == 1:
        return ordered

    open_cache: dict[date, int] = {}
    buckets: dict[int, list[Candle]] = {}
    for c in ordered:
        dt = datetime.fromtimestamp(c.ts_ms / 1000.0, IST)
        d = dt.date()
        anchor_min = open_cache.get(d)
        if anchor_min is None:
            fo_open, _ = session_clock.fo_window(d)
            anchor_min = fo_open.hour * 60 + fo_open.minute
            open_cache[d] = anchor_min
        mins = dt.hour * 60 + dt.minute
        if mins >= anchor_min:
            bucket_min = anchor_min + ((mins - anchor_min) // interval_min) * interval_min
        else:
            bucket_min = (mins // interval_min) * interval_min
        bdt = datetime(d.year, d.month, d.day, bucket_min // 60, bucket_min % 60, tzinfo=IST)
        bts = int(bdt.timestamp() * 1000)
        buckets.setdefault(bts, []).append(c)

    out: list[Candle] = []
    for bts in sorted(buckets):
        grp = buckets[bts]
        oi_val: float | None = None
        for bar in reversed(grp):
            if bar.oi is not None:
                oi_val = bar.oi
                break
        out.append(
            Candle(
                ts_ms=bts,
                open=grp[0].open,
                high=max(b.high for b in grp),
                low=min(b.low for b in grp),
                close=grp[-1].close,
                volume=sum(b.volume for b in grp),
                oi=oi_val,
            )
        )
    return out


def _extract_error_message(payload: object, body_text: str) -> str:
    if isinstance(payload, dict):
        for k in ("message", "error", "remarks", "emsg", "detail"):
            v = payload.get(k)
            if isinstance(v, str) and v.strip():
                return v.strip()
    return body_text.strip()[:300] or "unknown error"


def _is_auth_failure(status_code: int, message: str) -> str | None:
    low = message.lower()
    if "session expired" in low or "token expired" in low or "expired" in low and "session" in low:
        return "807"
    if status_code == 401 or "invalid session" in low or "invalid api" in low or "unauthorized" in low:
        return "809"
    return None


def raise_for_http_or_payload(status_code: int, body_text: str, *, payload: object | None = None) -> None:
    """Normalize HTTP statuses and Definedge JSON business errors into BrokerAuthError / BrokerError."""
    if payload is None and body_text.lstrip().startswith(("{", "[")):
        try:
            payload = json.loads(body_text)
        except ValueError:
            payload = None

    status_flag = str(payload.get("status") or payload.get("stat") or "").strip().upper() if isinstance(payload, dict) else ""
    has_json_error = status_flag in (
        "ERROR",
        "FAILED",
        "FAILURE",
        "NOT_OK",
        "NOT OK",
    )
    if status_code == 429:
        msg = _extract_error_message(payload, body_text)
        raise BrokerError(f"definedge rate limit (429): {msg}", code="429", status=429)

    if status_code >= 400 or has_json_error:
        msg = _extract_error_message(payload, body_text)
        auth_code = _is_auth_failure(status_code, msg)
        if auth_code is not None:
            raise DefinedgeAuthError(f"definedge auth error: {msg}", code=auth_code, status=status_code or 401)
        raise BrokerError(f"definedge error ({status_code}): {msg}", status=status_code)


def parse_limits_ok(body: object) -> dict[str, Any]:
    if not isinstance(body, dict):
        raise BrokerError("definedge: expected JSON object from /dart/v1/limits")
    raise_for_http_or_payload(200, "", payload=body)
    return body


def parse_quote_ltp(body: object) -> float:
    if not isinstance(body, dict):
        raise BrokerError("definedge: expected JSON object from /dart/v1/quotes")
    raise_for_http_or_payload(200, "", payload=body)
    for key in ("ltp", "lp", "last_price", "LTP"):
        val = body.get(key)
        if val is not None and str(val).strip() != "":
            try:
                return float(val)
            except (TypeError, ValueError) as exc:
                raise BrokerError(f"definedge: invalid quote ltp {val!r}") from exc
    raise BrokerError("definedge: quote response missing 'ltp'")
