"""Dhan v2 adapter over httpx.

Where each wire detail comes from (installed SDK dhanhq 2.2.0, .venv/Lib/site-packages/dhanhq/):
  * base URL `https://api.dhan.co/v2`                     -- dhan_http.py:34
  * headers `access-token`, `client-id`, JSON content     -- dhan_http.py:41-46
  * `dhanClientId` injected into every POST body          -- dhan_http.py:55-56
  * error body keys errorCode / errorType / errorMessage  -- dhan_http.py:91-95
  * POST /charts/historical body (securityId, exchangeSegment, instrument, expiryCode, oi,
    fromDate, toDate; expiryCode default 0)               -- _historical_data.py:41-75
  * POST /charts/intraday body (+ interval 1/5/15/25/60)  -- _historical_data.py:12-39
  * POST /optionchain {UnderlyingScrip, UnderlyingSeg, Expiry}       -- _option_chain.py:7-26
  * POST /optionchain/expirylist {UnderlyingScrip, UnderlyingSeg}    -- _option_chain.py:28-45
  * POST /marketfeed/ltp body {segment: [security_id, ...]}          -- _market_feed.py:7-23
  * GET /profile, headers access-token + dhanClientId     -- auth.py:151-168
  * scrip master URL (detailed)                           -- _security.py:17
  * segment names NSE_EQ / NSE_FNO / IDX_I ...            -- dhanhq.py:23-30
  * epoch-seconds timestamps read in IST, date-only at IST midnight -- dhanhq.py:61-76
  * 807 = access token expired (feed disconnect code)     -- fulldepth.py:368-370
NOT in the SDK (it returns raw JSON): the RESPONSE shapes below follow Dhan's public v2 API docs
and are pinned by tests/fixtures/dhan/*.json. The first live smoke run must confirm them.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx
import pandas as pd

from gammad.broker.base import (
    BrokerAdapter,
    BrokerAuthError,
    BrokerError,
    Candle,
    ChainRow,
    Instrument,
    TokenHealth,
    health_from_error,
)
from gammad.config import IST, now_ist
from gammad.log import get_logger

log = get_logger(__name__)

API_BASE_URL = "https://api.dhan.co/v2"
SCRIP_MASTER_URL = "https://images.dhan.co/api-data/api-scrip-master-detailed.csv"

EP_HISTORICAL = "/charts/historical"
EP_INTRADAY = "/charts/intraday"
EP_OPTION_CHAIN = "/optionchain"
EP_EXPIRY_LIST = "/optionchain/expirylist"
EP_LTP = "/marketfeed/ltp"
EP_PROFILE = "/profile"

INTRADAY_INTERVALS = (1, 5, 15, 25, 60)
INTRADAY_MAX_DAYS = 90  # Dhan docs: intraday history is served in <= 90-day windows
LTP_MAX_IDS = 1000  # Dhan docs: up to 1000 instruments per marketfeed request

# Credential-class failures. DH-901 = invalid/expired token (REST, Dhan docs); 807-810 are the
# token-expired / auth-failed / invalid-client family (807 also in SDK fulldepth.py:368-376).
AUTH_CODES = frozenset({"DH-901", "807", "808", "809", "810"})
RATE_CODES = frozenset({"DH-904", "805"})

# (EXCH_ID, SEGMENT) in the scrip master -> API exchange segment (names from dhanhq.py:23-30)
SEGMENT_MAP: dict[tuple[str, str], str] = {
    ("NSE", "E"): "NSE_EQ",
    ("BSE", "E"): "BSE_EQ",
    ("NSE", "D"): "NSE_FNO",
    ("BSE", "D"): "BSE_FNO",
    ("NSE", "C"): "NSE_CURRENCY",
    ("BSE", "C"): "BSE_CURRENCY",
    ("MCX", "M"): "MCX_COMM",
    ("NSE", "I"): "IDX_I",
    ("BSE", "I"): "IDX_I",
}

MASTER_COLUMNS = (
    "EXCH_ID",
    "SEGMENT",
    "SECURITY_ID",
    "INSTRUMENT",
    "UNDERLYING_SECURITY_ID",
    "UNDERLYING_SYMBOL",
    "SYMBOL_NAME",
    "DISPLAY_NAME",
    "SERIES",
    "LOT_SIZE",
    "SM_EXPIRY_DATE",
    "STRIKE_PRICE",
    "OPTION_TYPE",
    "TICK_SIZE",  # PAISE (measured 2026-09-25: 1.0000 = Rs 0.01, 5.0000 = Rs 0.05; 100% agreement with Kite)
)


class DhanAuthError(BrokerAuthError):
    """Dhan rejected the token (DH-901 / 807-class)."""


# --------------------------------------------------------------------------------------------
# Rate limiting
# --------------------------------------------------------------------------------------------


class TokenBucket:
    """Classic token bucket. `rate` tokens per second, at most `capacity` banked.

    Float guard (F3, W12 seam ledger): at a large clock base the refill `(now - last) * rate` can leave
    `1 - tokens` ~ 1e-16; the wait computed from it is below the clock's resolution, so the clock never moves
    and the loop spun (forever on a fake clock, up to one tick of busy-wait on a real one). Tokens within
    TOKEN_EPS of one count as one, and every wait is at least MIN_WAIT_S. Every adapter reusing this class
    gets the guard."""

    TOKEN_EPS = 1e-9
    MIN_WAIT_S = 1e-3

    def __init__(
        self,
        rate: float,
        capacity: float,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if rate <= 0 or capacity < 1:
            raise ValueError("rate must be > 0 and capacity >= 1")
        self.rate = rate
        self.capacity = capacity
        self._clock = clock
        self._sleep = sleep
        self._tokens = capacity
        self._last = clock()
        self._lock = threading.Lock()

    def acquire(self) -> float:
        """Take one token, sleeping as needed. Returns seconds slept."""
        slept = 0.0
        with self._lock:
            while True:
                now = self._clock()
                self._tokens = min(self.capacity, self._tokens + (now - self._last) * self.rate)
                self._last = now
                if self._tokens >= 1.0 - self.TOKEN_EPS:
                    self._tokens = max(0.0, self._tokens - 1.0)
                    return slept
                wait = max((1.0 - self._tokens) / self.rate, self.MIN_WAIT_S)
                self._sleep(wait)
                slept += wait


# --------------------------------------------------------------------------------------------
# Instrument master
# --------------------------------------------------------------------------------------------


def _read_master_csv(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, usecols=list(MASTER_COLUMNS), dtype=str, keep_default_na=False, low_memory=False)
    for col in ("EXCH_ID", "SEGMENT", "INSTRUMENT", "UNDERLYING_SYMBOL", "SERIES", "OPTION_TYPE"):
        df[col] = df[col].str.strip()
    return df


def load_scrip_master(
    data_dir: Path,
    as_of: date | None = None,
    fetch: Callable[[Path], None] | None = None,
    keep: int = 3,
) -> pd.DataFrame:
    """Return the detailed scrip master, downloading it at most once per IST day.

    Cached as data/scrip-master-YYYY-MM-DD.csv (the DOWNLOAD date). Only this module's own cache
    files are pruned, newest `keep` retained.

    0.8.3: when today's download fails (after _download_master's retries), the newest cached copy
    from an earlier day, at most MASTER_FALLBACK_MAX_AGE_DAYS old, is returned with one WARNING. It
    is NOT written under today's name, so the next call (today or tomorrow) retries the download.
    A contract listed today but absent from the older file has no identity in it: the identity
    guard (canonical.guard_contracts -> "not in the canonical master") drops it; it is never
    mis-mapped onto another contract.
    """
    as_of = as_of or now_ist().date()
    data_dir.mkdir(parents=True, exist_ok=True)
    path = data_dir / f"scrip-master-{as_of.isoformat()}.csv"
    if not path.exists():
        try:
            (fetch or _download_master)(path)
        except (httpx.HTTPError, OSError) as exc:
            stale = _newest_cached_master(data_dir, as_of)
            if stale is None:
                raise
            log.warning(
                "scrip master download failed (%s); using the cached copy from %s",
                type(exc).__name__,
                stale[0].isoformat(),
            )
            return _read_master_csv(stale[1])
        log.info("scrip master downloaded -> %s (%d bytes)", path.name, path.stat().st_size)
        cached = sorted(data_dir.glob("scrip-master-????-??-??.csv"))
        for old in cached[:-keep]:
            old.unlink(missing_ok=True)
    return _read_master_csv(path)


MASTER_FALLBACK_MAX_AGE_DAYS = 3
MASTER_DOWNLOAD_ATTEMPTS = 3
MASTER_RETRY_BACKOFF_S = (2.0, 4.0)  # sleep before attempt 2, attempt 3
_sleep: Callable[[float], None] = time.sleep  # patchable in tests


def _newest_cached_master(data_dir: Path, as_of: date) -> tuple[date, Path] | None:
    """Newest scrip-master-YYYY-MM-DD.csv dated before `as_of` and at most MASTER_FALLBACK_MAX_AGE_DAYS old."""
    best: tuple[date, Path] | None = None
    for p in data_dir.glob("scrip-master-????-??-??.csv"):
        try:
            d = date.fromisoformat(p.stem.removeprefix("scrip-master-"))
        except ValueError:
            continue
        if d < as_of and (as_of - d).days <= MASTER_FALLBACK_MAX_AGE_DAYS and (best is None or d > best[0]):
            best = (d, p)
    return best


def _retryable(exc: Exception) -> bool:
    """Transport errors (incl. timeouts) and HTTP 5xx are retried; a 4xx is final."""
    if isinstance(exc, httpx.TransportError):
        return True
    return isinstance(exc, httpx.HTTPStatusError) and exc.response.status_code >= 500


def _download_master(path: Path) -> None:
    part = path.with_suffix(".csv.part")
    for attempt in range(1, MASTER_DOWNLOAD_ATTEMPTS + 1):
        try:
            with httpx.stream("GET", SCRIP_MASTER_URL, timeout=120.0, follow_redirects=True) as resp:
                resp.raise_for_status()
                with part.open("wb") as fh:
                    for chunk in resp.iter_bytes():
                        fh.write(chunk)
            part.replace(path)
            return
        except httpx.HTTPError as exc:
            part.unlink(missing_ok=True)
            if attempt >= MASTER_DOWNLOAD_ATTEMPTS or not _retryable(exc):
                raise
            wait = MASTER_RETRY_BACKOFF_S[attempt - 1]
            log.info(  # INFO: the one WARNING belongs to the cache fallback in load_scrip_master
                "scrip master download attempt %d/%d failed (%s); retrying in %.0f s",
                attempt,
                MASTER_DOWNLOAD_ATTEMPTS,
                type(exc).__name__,
                wait,
            )
            _sleep(wait)


def _to_int(value: str) -> int:
    return int(float(value)) if value not in ("", "NA") else 0


def _to_float_or_none(value: str) -> float | None:
    if value in ("", "NA"):
        return None
    f = float(value)
    return None if f < 0 else f  # master uses -0.01 as "no strike"


def tick_rupees(value: str) -> float | None:
    """Dhan TICK_SIZE (paise) -> rupees; "", "NA", non-numeric or <= 0 -> None."""
    if value in ("", "NA"):
        return None
    try:
        paise = float(value)
    except ValueError:
        return None
    return round(paise / 100.0, 6) if paise > 0 else None


def _to_date_or_none(value: str) -> date | None:
    if value in ("", "NA") or value.startswith("0001-"):
        return None
    return date.fromisoformat(value[:10])


@dataclass
class ScripMaster:
    df: pd.DataFrame

    def _nse_eq(self) -> pd.DataFrame:
        d = self.df
        return d[(d.EXCH_ID == "NSE") & (d.SEGMENT == "E") & (d.INSTRUMENT == "EQUITY") & (d.SERIES == "EQ")]

    def eq_security_id(self, symbol: str) -> int | None:
        rows = self._nse_eq()
        rows = rows[rows.UNDERLYING_SYMBOL == symbol]
        if rows.empty:
            return None
        if len(rows) > 1:
            log.warning("scrip master: %d NSE_EQ rows for %s, taking the first", len(rows), symbol)
        return int(rows.iloc[0].SECURITY_ID)

    def index_security_id(self, symbol: str) -> int | None:
        """IDX_I security id of an index: rows SEGMENT "I" + INSTRUMENT "INDEX" keyed by UNDERLYING_SYMBOL,
        NSE before BSE (2026-09-25 master: NIFTY -> 13, BANKNIFTY -> 25; no name is in both exchanges)."""
        d = self.df
        rows = d[(d.SEGMENT == "I") & (d.INSTRUMENT == "INDEX") & (d.UNDERLYING_SYMBOL == symbol)]
        for exch in ("NSE", "BSE"):
            hit = rows[rows.EXCH_ID == exch]
            if hit.empty:
                continue
            if len(hit) > 1:
                log.warning("scrip master: %d %s index rows for %s, taking the first", len(hit), exch, symbol)
            return int(hit.iloc[0].SECURITY_ID)
        return None

    def option_rows(self, underlying_symbol: str, expiry: date) -> list[Instrument]:
        d = self.df
        rows = d[
            (d.EXCH_ID == "NSE")
            & (d.SEGMENT == "D")
            & (d.INSTRUMENT == "OPTSTK")
            & (d.UNDERLYING_SYMBOL == underlying_symbol)
            & (d.SM_EXPIRY_DATE.str[:10] == expiry.isoformat())
        ]
        out = [self._row_to_instrument(r) for r in rows.itertuples(index=False)]
        return sorted(out, key=lambda i: (i.strike or 0.0, i.option_type or ""))

    def instruments(self) -> list[Instrument]:
        d = self.df
        keep = (d.EXCH_ID == "NSE") & (
            ((d.SEGMENT == "E") & (d.INSTRUMENT == "EQUITY") & (d.SERIES == "EQ"))
            | ((d.SEGMENT == "D") & d.INSTRUMENT.isin(["FUTSTK", "OPTSTK", "FUTIDX", "OPTIDX"]))
        )
        return [self._row_to_instrument(r) for r in d[keep].itertuples(index=False)]

    @staticmethod
    def _row_to_instrument(r: Any) -> Instrument:
        seg = SEGMENT_MAP.get((r.EXCH_ID, r.SEGMENT), f"{r.EXCH_ID}_{r.SEGMENT}")
        is_eq = r.SEGMENT == "E"
        opt = r.OPTION_TYPE if r.OPTION_TYPE in ("CE", "PE") else None
        return Instrument(
            security_id=int(r.SECURITY_ID),
            symbol=r.UNDERLYING_SYMBOL if is_eq else r.SYMBOL_NAME,
            exchange_segment=seg,
            instrument_type=r.INSTRUMENT,
            underlying_symbol=r.UNDERLYING_SYMBOL or None,
            expiry_date=None if is_eq else _to_date_or_none(r.SM_EXPIRY_DATE),
            strike=_to_float_or_none(r.STRIKE_PRICE) if opt else None,
            option_type=opt,
            lot_size=_to_int(r.LOT_SIZE) or 1,
            tick_size=tick_rupees(getattr(r, "TICK_SIZE", "")),
        )


# --------------------------------------------------------------------------------------------
# Response parsing (pure; unit-tested against tests/fixtures/dhan/*.json)
# --------------------------------------------------------------------------------------------


def epoch_to_ist(ts: float) -> datetime:
    return datetime.fromtimestamp(float(ts), IST)


def parse_candles(payload: dict[str, Any]) -> list[Candle]:
    """Column arrays {open, high, low, close, volume, timestamp[, open_interest]} -> candles."""
    body = payload.get("data", payload) if isinstance(payload.get("data"), dict) else payload
    keys = ("open", "high", "low", "close", "volume", "timestamp")
    missing = [k for k in keys if k not in body]
    if missing:
        raise BrokerError(f"candle payload missing {missing}")
    n = len(body["timestamp"])
    if any(len(body[k]) != n for k in keys):
        raise BrokerError("candle payload arrays have unequal lengths")
    oi = body.get("open_interest")
    if oi is not None and len(oi) != n:
        oi = None
    out = [
        Candle(
            ts_ms=round(float(body["timestamp"][i]) * 1000),
            open=float(body["open"][i]),
            high=float(body["high"][i]),
            low=float(body["low"][i]),
            close=float(body["close"][i]),
            volume=float(body["volume"][i]),
            oi=None if oi is None else float(oi[i]),
        )
        for i in range(n)
    ]
    out.sort(key=lambda c: c.ts_ms)
    return out


def candle_date(c: Candle) -> date:
    return epoch_to_ist(c.ts_ms / 1000).date()


def parse_ltp(payload: dict[str, Any], exchange_segment: str) -> dict[int, float]:
    seg = (payload.get("data") or {}).get(exchange_segment) or {}
    out: dict[int, float] = {}
    for sid, row in seg.items():
        lp = row.get("last_price") if isinstance(row, dict) else None
        if lp is not None:
            out[int(sid)] = float(lp)
    return out


def parse_expiries(payload: dict[str, Any]) -> list[date]:
    return sorted(date.fromisoformat(str(s)[:10]) for s in payload.get("data") or [])


def parse_option_chain(payload: dict[str, Any]) -> list[ChainRow]:
    oc = ((payload.get("data") or {}).get("oc")) or {}
    rows: list[ChainRow] = []
    for strike_key, legs in oc.items():
        strike = float(strike_key)
        for leg_key, opt in (("ce", "CE"), ("pe", "PE")):
            leg = (legs or {}).get(leg_key)
            if not leg:
                continue
            sid = leg.get("security_id")
            iv = leg.get("implied_volatility")
            prev_oi = leg.get("previous_oi")
            # previous_volume is QUANTITY (shares): on the recorded 28 Sep day every non-zero value is a whole
            # multiple of the lot size (tests/test_dhan_parsing.py), so volume x close is rupees. Absent -> None.
            prev_vol = leg.get("previous_volume")
            prev_close = leg.get("previous_close_price")
            rows.append(
                ChainRow(
                    strike=strike,
                    option_type=opt,
                    security_id=int(sid) if sid not in (None, "") else None,
                    ltp=float(leg.get("last_price") or 0.0),
                    oi=int(leg.get("oi") or 0),
                    volume=int(leg.get("volume") or 0),
                    iv=float(iv) if iv is not None else None,
                    previous_oi=int(prev_oi) if prev_oi is not None else None,
                    previous_volume=int(prev_vol) if prev_vol is not None else None,
                    previous_close=float(prev_close) if prev_close is not None else None,
                )
            )
    rows.sort(key=lambda r: (r.strike, r.option_type))
    return rows


def parse_token_validity(value: str | None) -> str | None:
    """Dhan profile `tokenValidity` is 'DD/MM/YYYY HH:MM' in IST -> ISO-8601 with offset."""
    if not value:
        return None
    for fmt in ("%d/%m/%Y %H:%M", "%d/%m/%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(value.strip(), fmt).replace(tzinfo=IST).isoformat()
        except ValueError:
            continue
    return None


def error_code(body: Any) -> str | None:
    if not isinstance(body, dict):
        return None
    for key in ("errorCode", "internalErrorCode", "error_code"):
        v = body.get(key)
        if v not in (None, ""):
            return str(v)
    return None


# --------------------------------------------------------------------------------------------
# Adapter
# --------------------------------------------------------------------------------------------


class DhanAdapter(BrokerAdapter):
    def __init__(
        self,
        client_id: str,
        access_token: str,
        data_dir: Path,
        http: httpx.Client | None = None,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
        max_retries: int = 3,
    ) -> None:
        self.client_id = client_id
        self._token = access_token
        self.data_dir = data_dir
        self._sleep = sleep
        self.max_retries = max_retries
        self._http = http or httpx.Client(base_url=API_BASE_URL, timeout=30.0)
        self._data_bucket = TokenBucket(5.0, 5.0, clock, sleep)  # data APIs: 5 req/s
        self._chain_bucket = TokenBucket(1.0 / 3.0, 1.0, clock, sleep)  # option chain: 1 per 3 s
        self._quote_bucket = TokenBucket(1.0, 1.0, clock, sleep)  # marketfeed: 1 req/s (Dhan docs)
        self._master: ScripMaster | None = None

    @classmethod
    def from_settings(cls) -> DhanAdapter:
        from gammad.config import settings

        s = settings()
        if not s.env.dhan_client_id or not s.env.dhan_access_token:
            raise DhanAuthError("DHAN_CLIENT_ID / DHAN_ACCESS_TOKEN not set (.env)")
        return cls(s.env.dhan_client_id, s.env.dhan_access_token, s.paths.data_dir)

    def set_token(self, access_token: str) -> None:
        self._token = access_token

    def set_credentials(self, client_id: str, access_token: str) -> None:
        """Hot-swap credentials on this live adapter (same httpx client, buckets and master cache).

        The httpx client carries no auth headers of its own: `_headers()` builds them per request
        from these two fields, so the next request (and the `dhanClientId` body field) uses them.
        """
        self.client_id = client_id
        self._token = access_token

    # -- transport ----------------------------------------------------------------------------

    def _headers(self) -> dict[str, str]:
        return {
            "access-token": self._token,
            "client-id": self.client_id,
            "dhanClientId": self.client_id,  # /profile reads this name (auth.py:162-165)
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def _request(self, method: str, endpoint: str, bucket: TokenBucket, body: dict[str, Any] | None) -> Any:
        if body is not None:
            body = {**body, "dhanClientId": self.client_id}
        attempt = 0
        while True:
            bucket.acquire()
            try:
                resp = self._http.request(method, endpoint, json=body, headers=self._headers())
            except httpx.TransportError as exc:
                if attempt < self.max_retries:
                    attempt += 1
                    self._sleep(min(2.0**attempt, 10.0))
                    continue
                raise BrokerError(f"{endpoint}: transport error {exc}") from exc
            try:
                parsed: Any = resp.json()
            except ValueError:
                parsed = None
            code = error_code(parsed)
            if code in AUTH_CODES or resp.status_code == 401:
                msg = parsed.get("errorMessage") if isinstance(parsed, dict) else resp.text[:200]
                raise DhanAuthError(f"{endpoint}: auth rejected ({code}): {msg}", code, resp.status_code)
            if 200 <= resp.status_code < 300 and code is None:
                if parsed is None:
                    raise BrokerError(f"{endpoint}: non-JSON body", None, resp.status_code)
                return parsed
            retryable = resp.status_code == 429 or resp.status_code >= 500 or code in RATE_CODES
            if retryable and attempt < self.max_retries:
                attempt += 1
                self._sleep(min(2.0**attempt, 10.0))
                continue
            msg = parsed.get("errorMessage") if isinstance(parsed, dict) else resp.text[:200]
            raise BrokerError(f"{endpoint}: HTTP {resp.status_code} {code}: {msg}", code, resp.status_code)

    # -- instrument master --------------------------------------------------------------------

    def scrip_master(self, as_of: date | None = None) -> ScripMaster:
        if self._master is None:
            self._master = ScripMaster(load_scrip_master(self.data_dir, as_of))
        return self._master

    def instruments(self) -> list[Instrument]:
        return self.scrip_master().instruments()

    # -- data ---------------------------------------------------------------------------------

    def daily_candles(
        self,
        security_id: int,
        exchange_segment: str,
        from_date: date,
        to_date: date,
        instrument: str = "EQUITY",
    ) -> list[Candle]:
        body = {
            "securityId": str(security_id),
            "exchangeSegment": exchange_segment,
            "instrument": instrument,
            "expiryCode": 0,
            "oi": False,
            "fromDate": from_date.isoformat(),
            # Dhan documents toDate as exclusive; ask one day past and filter inclusively below.
            "toDate": (to_date + timedelta(days=1)).isoformat(),
        }
        candles = parse_candles(self._request("POST", EP_HISTORICAL, self._data_bucket, body))
        return [c for c in candles if from_date <= candle_date(c) <= to_date]

    def intraday(
        self,
        security_id: int,
        exchange_segment: str,
        interval_min: int,
        from_date: date,
        to_date: date,
        oi: bool = True,
        instrument: str = "OPTSTK",
    ) -> list[Candle]:
        if interval_min not in INTRADAY_INTERVALS:
            raise ValueError(f"interval_min must be one of {INTRADAY_INTERVALS}")
        out: dict[int, Candle] = {}
        start = from_date
        while start <= to_date:
            end = min(to_date, start + timedelta(days=INTRADAY_MAX_DAYS - 1))
            body = {
                "securityId": str(security_id),
                "exchangeSegment": exchange_segment,
                "instrument": instrument,
                "interval": interval_min,
                "oi": oi,
                "fromDate": start.isoformat(),
                "toDate": (end + timedelta(days=1)).isoformat(),
            }
            for c in parse_candles(self._request("POST", EP_INTRADAY, self._data_bucket, body)):
                if from_date <= candle_date(c) <= to_date:
                    out[c.ts_ms] = c
            start = end + timedelta(days=1)
        return [out[k] for k in sorted(out)]

    def expiries(self, underlying_security_id: int, underlying_seg: str) -> list[date]:
        body = {"UnderlyingScrip": int(underlying_security_id), "UnderlyingSeg": underlying_seg}
        return parse_expiries(self._request("POST", EP_EXPIRY_LIST, self._data_bucket, body))

    def option_chain(self, underlying_security_id: int, underlying_seg: str, expiry: date) -> list[ChainRow]:
        body = {
            "UnderlyingScrip": int(underlying_security_id),
            "UnderlyingSeg": underlying_seg,
            "Expiry": expiry.isoformat(),
        }
        return parse_option_chain(self._request("POST", EP_OPTION_CHAIN, self._chain_bucket, body))

    def ltp(self, security_ids: Sequence[int], exchange_segment: str = "NSE_EQ") -> dict[int, float]:
        ids = [int(s) for s in security_ids]
        out: dict[int, float] = {}
        for i in range(0, len(ids), LTP_MAX_IDS):
            body = {exchange_segment: ids[i : i + LTP_MAX_IDS]}
            out.update(parse_ltp(self._request("POST", EP_LTP, self._quote_bucket, body), exchange_segment))
        return out

    def token_health(self) -> TokenHealth:
        try:
            body = self._request("GET", EP_PROFILE, self._data_bucket, None)
        except BrokerError as exc:  # a DhanAuthError -> auth_failed (W14 H4: by TYPE, not by its text)
            return health_from_error(exc)
        validity = parse_token_validity(body.get("tokenValidity") if isinstance(body, dict) else None)
        if validity is None:
            return TokenHealth(True, None, "profile returned no parseable tokenValidity")
        valid = datetime.fromisoformat(validity) > now_ist()
        return TokenHealth(valid, validity, None if valid else "token validity is in the past")
