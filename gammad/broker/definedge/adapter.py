"""Definedge Securities adapter (Stage 1: historical data + DART limits/quotes + allmaster).

Endpoints:
- SDS history (6-col EQ/IDX, 7-col NFO CSV): https://data.definedgesecurities.com/sds/history/...
- DART REST (limits, quotes): https://integrate.definedgesecurities.com/dart/v1/...
- Public master (no auth header): https://app.definedgesecurities.com/public/allmaster.zip
"""

from __future__ import annotations

import logging
import threading
import time
import zipfile
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Callable, Mapping, Sequence

import httpx

from gammad.broker.base import (
    BrokerAdapter,
    BrokerError,
    Candle,
    ChainRow,
    Instrument,
    TokenHealth,
    health_from_error,
)
from gammad.broker.canonical import CanonicalMaster, SEG_IDX, check_identity
from gammad.broker.definedge import wire
from gammad.broker.definedge.wire import DefinedgeAuthError, DefinedgeMaster, DefinedgeRow
from gammad.broker.dhan import MASTER_FALLBACK_MAX_AGE_DAYS, ScripMaster
from gammad.config import now_ist

log = logging.getLogger(__name__)

_HTTP_TIMEOUT_S = 20.0
_MASTER_TIMEOUT_S = 30.0
_MAX_RETRY_AFTER_S = 5.0


class _TokenBucket:
    """Thread-safe conservative token bucket for Definedge API rate protection."""

    def __init__(
        self,
        rate: float,
        capacity: int,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._rate = float(rate)
        self._cap = float(capacity)
        self._tokens = float(capacity)
        self._last = clock()
        self._clock = clock
        self._sleep = sleep
        self._lock = threading.Lock()

    def acquire(self) -> None:
        while True:
            with self._lock:
                now = self._clock()
                elapsed = max(0.0, now - self._last)
                self._tokens = min(self._cap, self._tokens + elapsed * self._rate)
                self._last = now
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return
                wait = (1.0 - self._tokens) / self._rate if self._rate > 0 else 0.5
            if wait > 1e-9:
                self._sleep(wait)


def _newest_cached(data_dir: Path, as_of: date) -> tuple[date, Path] | None:
    best: tuple[date, Path] | None = None
    for path in data_dir.glob("definedge-allmaster-????-??-??.csv"):
        try:
            d = date.fromisoformat(path.name.removeprefix("definedge-allmaster-").removesuffix(".csv"))
        except ValueError:
            continue
        if d <= as_of and (as_of - d).days <= MASTER_FALLBACK_MAX_AGE_DAYS and (best is None or d > best[0]):
            best = (d, path)
    return best


class DefinedgeAdapter(BrokerAdapter):
    """Definedge Securities REST + SDS history adapter."""

    ltp_batch_size: int = wire.LTP_MAX_IDS

    def __init__(
        self,
        api_session_key: str,
        data_dir: Path,
        *,
        api_token: str = "",
        api_secret: str = "",
        http: httpx.Client | None = None,
        transport: httpx.BaseTransport | None = None,
        clock: Callable[[], float] | None = None,
        wall_clock: Callable[[], datetime] | None = None,
        sleep: Callable[[float], None] | None = None,
    ) -> None:
        self._api_session_key = (api_session_key or "").strip()
        self._api_token = (api_token or "").strip()
        self._api_secret = (api_secret or "").strip()
        self.data_dir = Path(data_dir)
        self._clock: Callable[[], float] = clock or time.monotonic
        self._wall_clock: Callable[[], datetime] = wall_clock or now_ist
        self._sleep: Callable[[float], None] = sleep or time.sleep
        self._owns_http = http is None
        self._http = http or httpx.Client(
            timeout=_HTTP_TIMEOUT_S,
            follow_redirects=False,
            trust_env=False,
            transport=transport,
        )
        self._bucket = _TokenBucket(wire.RATE_PER_S, wire.RATE_BURST, clock=self._clock, sleep=self._sleep)
        self._cooldown_until: float = 0.0
        self._cooldown_lock = threading.Lock()
        self._master_lock = threading.RLock()
        self._canon: CanonicalMaster | None = None
        self._native: DefinedgeMaster | None = None
        self._native_date: date | None = None
        self._resolved: dict[tuple[str, int], DefinedgeRow | None] = {}
        self.identity_dropped: list[tuple[str, int, str]] = []

    def set_credentials(self, creds: Mapping[str, str]) -> None:
        """Refresh credentials on live adapter without rebuilding caches."""
        if "DEFINEDGE_API_SESSION_KEY" in creds:
            self._api_session_key = str(creds.get("DEFINEDGE_API_SESSION_KEY") or "").strip()
        if "DEFINEDGE_API_TOKEN" in creds:
            self._api_token = str(creds.get("DEFINEDGE_API_TOKEN") or "").strip()
        if "DEFINEDGE_API_SECRET" in creds:
            self._api_secret = str(creds.get("DEFINEDGE_API_SECRET") or "").strip()

    def canonical(self) -> CanonicalMaster:
        with self._master_lock:
            if self._canon is None:
                self._canon = CanonicalMaster.load(self.data_dir)
            return self._canon

    def scrip_master(self, as_of: date | None = None) -> ScripMaster:
        """Return shared canonical Dhan scrip master for job duck-typing."""
        del as_of
        return self.canonical().sm  # type: ignore[return-value]

    def _effective_master_date(self, as_of: date | None = None) -> date:
        if as_of is not None:
            return as_of
        now = self._wall_clock().astimezone(wire.IST)
        if (now.hour, now.minute) >= wire.MASTER_REFRESH_HM:
            return now.date()
        return now.date() - timedelta(days=1)

    def native_master(self, as_of: date | None = None) -> DefinedgeMaster:
        target_date = self._effective_master_date(as_of)
        with self._master_lock:
            if self._native is not None and self._native_date == target_date:
                return self._native

            self.data_dir.mkdir(parents=True, exist_ok=True)
            path = self.data_dir / f"definedge-allmaster-{target_date.isoformat()}.csv"
            if not path.exists():
                try:
                    resp = self._http.get(wire.ALLMASTER_URL, timeout=_MASTER_TIMEOUT_S)
                    if resp.status_code >= 400:
                        raise BrokerError(
                            f"definedge allmaster.zip HTTP {resp.status_code}",
                            status=resp.status_code,
                        )
                    csv_text = wire.parse_allmaster_zip(resp.content)
                except (BrokerError, httpx.HTTPError, zipfile.BadZipFile) as exc:
                    stale = _newest_cached(self.data_dir, target_date)
                    if stale is None:
                        raise BrokerError(f"definedge allmaster download failed ({exc})") from exc
                    log.warning(
                        "definedge allmaster.zip download failed (%s); using cached %s",
                        exc,
                        stale[0].isoformat(),
                    )
                    path = stale[1]
                else:
                    part = path.with_suffix(".csv.part")
                    part.write_text(csv_text, encoding="utf-8")
                    part.replace(path)
                    for old in sorted(self.data_dir.glob("definedge-allmaster-????-??-??.csv"))[: -wire.MASTER_KEEP]:
                        old.unlink(missing_ok=True)

            rows = wire.parse_allmaster_csv(path.read_text(encoding="utf-8"))
            if self._native_date is not None and self._native_date != target_date:
                self._canon = None
            self._resolved.clear()
            self._native = DefinedgeMaster(rows)
            self._native_date = target_date
            return self._native

    def _auth_headers(self) -> dict[str, str]:
        if not self._api_session_key:
            raise DefinedgeAuthError("definedge: missing DEFINEDGE_API_SESSION_KEY", code="809")
        return {"Authorization": self._api_session_key}

    def _wait_rate_and_cooldown(self) -> None:
        with self._cooldown_lock:
            remaining = self._cooldown_until - self._clock()
        if remaining > 0:
            self._sleep(remaining)
        self._bucket.acquire()

    def _request(self, url: str) -> httpx.Response:
        headers = self._auth_headers()
        self._wait_rate_and_cooldown()
        try:
            resp = self._http.get(url, headers=headers)
        except httpx.HTTPError as exc:
            raise BrokerError(f"definedge request failed ({exc})") from exc
        if resp.status_code == 429:
            retry_after = wire.COOLDOWN_S
            ra_hdr = resp.headers.get("Retry-After", "").strip()
            if ra_hdr:
                try:
                    retry_after = min(_MAX_RETRY_AFTER_S, max(0.1, float(ra_hdr)))
                except ValueError:
                    retry_after = wire.COOLDOWN_S
            with self._cooldown_lock:
                self._cooldown_until = max(self._cooldown_until, self._clock() + retry_after)
        wire.raise_for_http_or_payload(resp.status_code, resp.text)
        return resp

    def _get_json(self, url: str) -> object:
        resp = self._request(url)
        try:
            return resp.json()
        except ValueError as exc:
            raise BrokerError(f"definedge: invalid JSON response ({exc})") from exc

    def _row(self, segment: str, security_id: int) -> DefinedgeRow | None:
        key = (segment, int(security_id))
        with self._master_lock:
            if key in self._resolved:
                return self._resolved[key]
            nm = self.native_master()
            canon = self.canonical()
            row: DefinedgeRow | None
            if segment == SEG_IDX:
                inst = canon.instrument(segment, security_id)
                raw_name = (inst.underlying_symbol or inst.symbol) if inst else ""
                uname = (
                    wire._INDEX_ALIAS_TO_CANON.get(wire._normalize_label(raw_name))
                    or wire.norm_underlying(raw_name)
                ) if raw_name else ""
                row = nm.index_by_name.get(uname)
                if row is None and uname in wire.INDEX_MAP:
                    _, idx_token, _ = wire.INDEX_MAP[uname]
                    if not nm.has_index_token(idx_token):
                        row = wire.fallback_index_row(uname)
            else:
                row = nm.by_code.get((segment, int(security_id)))
                if row is None:
                    ck = canon.key(segment, security_id)
                    if ck is not None:
                        row = nm.by_key.get(ck)
            bad = check_identity(canon, segment, security_id, None if row is None else row.key())
            if bad is not None:
                self.identity_dropped.append((segment, int(security_id), bad.reason))
                log.error("identity guard (definedge): %s %d dropped: %s", segment, int(security_id), bad.reason)
                row = None
            self._resolved[key] = row
            return row

    def _need_row(self, segment: str, security_id: int) -> DefinedgeRow:
        row = self._row(segment, security_id)
        if row is None:
            raise BrokerError(f"definedge: no identity-checked row for {segment} {security_id}")
        return row

    def instruments(self) -> list[Instrument]:
        return self.native_master().instruments()

    def _fetch_sds_history(
        self,
        row: DefinedgeRow,
        timeframe: str,
        span_days: int,
        from_date: date,
        to_date: date,
        *,
        has_oi: bool,
    ) -> list[Candle]:
        if from_date > to_date:
            return []
        out: dict[int, Candle] = {}
        start = from_date
        while start <= to_date:
            end = min(to_date, start + timedelta(days=span_days - 1))
            url = wire.history_url(row.segment, row.token, timeframe, start, end)
            resp = self._request(url)
            bars = wire.parse_sds_csv(
                resp.text,
                timeframe=timeframe,
                has_oi=has_oi,
                from_date=from_date,
                to_date=to_date,
            )
            for bar in bars:
                out[bar.ts_ms] = bar
            start = end + timedelta(days=1)
        return [out[k] for k in sorted(out)]

    def daily_candles(
        self,
        security_id: int,
        exchange_segment: str,
        from_date: date,
        to_date: date,
        instrument: str = "EQUITY",
    ) -> list[Candle]:
        del instrument
        if from_date > to_date:
            return []
        row = self._need_row(exchange_segment, security_id)
        return self._fetch_sds_history(
            row,
            "day",
            wire.DAY_SPAN_DAYS,
            from_date,
            to_date,
            has_oi=(row.segment == "NFO"),
        )

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
        del instrument
        if interval_min not in wire.SUPPORTED_INTRADAY_MINS:
            raise BrokerError(
                f"definedge: unsupported intraday interval {interval_min}m; expected {wire.SUPPORTED_INTRADAY_MINS}"
            )
        if from_date > to_date:
            return []
        row = self._need_row(exchange_segment, security_id)
        bars_1m = self._fetch_sds_history(
            row,
            "minute",
            wire.MINUTE_SPAN_DAYS,
            from_date,
            to_date,
            has_oi=bool(oi and row.segment == "NFO"),
        )
        return wire.resample_minute_candles(bars_1m, interval_min)

    def ltp(self, security_ids: Sequence[int], exchange_segment: str = "NSE_EQ") -> dict[int, float]:
        out: dict[int, float] = {}
        seen: set[int] = set()
        for raw_sid in security_ids:
            sid = int(raw_sid)
            if sid in seen:
                continue
            seen.add(sid)
            row = self._row(exchange_segment, sid)
            if row is None:
                continue
            body = self._get_json(wire.quotes_url(row.segment, row.token))
            out[sid] = wire.parse_quote_ltp(body)
        return out

    def expiries(self, underlying_security_id: int, underlying_seg: str) -> list[date]:
        del underlying_security_id, underlying_seg
        raise BrokerError("definedge: expiries endpoint is not supported in Stage 1")

    def option_chain(self, underlying_security_id: int, underlying_seg: str, expiry: date) -> list[ChainRow]:
        del underlying_security_id, underlying_seg, expiry
        raise BrokerError("definedge: option_chain endpoint is not supported in Stage 1")

    def token_health(self) -> TokenHealth:
        try:
            body = self._get_json(wire.limits_url())
            wire.parse_limits_ok(body)
        except BrokerError as exc:
            return health_from_error(exc)
        return TokenHealth(True, None, None)

    def close(self) -> None:
        if self._owns_http:
            self._http.close()
