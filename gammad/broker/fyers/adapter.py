"""FyersAdapter: BrokerAdapter over raw httpx (BROKER-ADAPTER-SPEC §2, §8). BETA: not verified live.

Identity (spec §1): canonical `security_id` = the NSE exchange token = Fyers' scrip code (master col 12). Fyers'
own public masters (NSE_FO.csv + NSE_CM.csv, downloaded once per IST day into `data/fyers-<NSE_FO|NSE_CM>-DATE.csv`,
3 kept) map it to the Fyers TICKER ("NSE:RELIANCE26SEP1400CE", col 9). IDX_I maps by the index's underlying
name (NSE:NIFTY50-INDEX has underlying "NIFTY"). Every native row passes the RUNTIME IDENTITY GUARD
(canonical.check_identity: underlying, expiry, strike, CE/PE) before its first use; a mismatch is dropped with one
ERROR and recorded in `identity_dropped`, never substituted.

Errors: HTTP 401 / code -8 -> FyersAuthError "807" (expired); -15/-16/-17 -> "809"; HTTP 403 (the app lacks the
Market Data permission) -> BrokerEntitlementError "806" with the D230 hint (0.8.3). Never retried. 5xx / transport
errors retry with back-off, then BrokerError. A rate limit (HTTP 429, or body code -429 / 429 at any status) raises
BrokerError(code "429", status 429) at once and opens a RATE_LIMIT_COOLDOWN_S cool-down in which no request is sent
(audit BROKER-01: every retry or send would count toward the F7 day-block).
One 3/s TokenBucket covers every data call (F7: breaching 10/s or 200/min more than 3 times a minute blocks the day).
No order method exists here.
"""

from __future__ import annotations

import csv
import io
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx

from gammad.broker import hints
from gammad.broker.base import (
    BrokerAdapter,
    BrokerAuthError,
    BrokerEntitlementError,
    BrokerError,
    Candle,
    ChainRow,
    Instrument,
    TokenHealth,
    health_from_error,
)
from gammad.broker.batching import batched
from gammad.broker.canonical import CanonicalMaster, ContractKey, check_identity
from gammad.broker.dhan import MASTER_FALLBACK_MAX_AGE_DAYS, ScripMaster, TokenBucket
from gammad.broker.fyers import wire
from gammad.broker.fyers.limits import DailyBudgetExhausted, FyersDataBudget
from gammad.config import IST, now_ist
from gammad.log import get_logger

log = get_logger(__name__)

KEY_APP_ID = "FYERS_APP_ID"
KEY_TOKEN = "FYERS_ACCESS_TOKEN"
MASTER_KEEP = 3
MASTER_ATTEMPTS = 3  # 0.8.3: tries per public master CSV on a transport error / 5xx (waits 2 s, 4 s)
QUOTE_CHECK_SYMBOL = "NSE:RELIANCE-EQ"  # 0.8.3 T-229 (3): token_health's one price check (no master needed)
# audit BROKER-01: after a rate-limit refusal (HTTP 429, or a body code -429 / 429 at any status) no request is sent
# for this long: every _get inside it raises BrokerError status 429 unsent (F7: > 3 breaches a minute = day-block)
RATE_LIMIT_COOLDOWN_S = 180.0
RATE_LIMIT_CODES = frozenset({"-429", "429"})


def _newest_cached(data_dir: Path, prefix: str, suffix: str, as_of: date) -> tuple[date, Path] | None:
    """Newest prior-day master within the shared Dhan fallback age limit."""
    best: tuple[date, Path] | None = None
    for path in data_dir.glob(f"{prefix}????-??-??{suffix}"):
        try:
            d = date.fromisoformat(path.name.removeprefix(prefix).removesuffix(suffix))
        except ValueError:
            continue
        if d < as_of and (as_of - d).days <= MASTER_FALLBACK_MAX_AGE_DAYS and (best is None or d > best[0]):
            best = (d, path)
    return best


class FyersAuthError(BrokerAuthError):
    """Fyers rejected the access token."""


class _Bucket(TokenBucket):
    """dhan.TokenBucket with a float guard: when `1 - tokens` rounds to ~1e-17, the base acquire() sleeps a wait
    that no longer moves a fake (or coarse) clock and spins forever -- the offline harness hit it (MemoryError in
    FakeClock.sleeps). Tokens within 1e-9 of one count as one, and every wait is at least 1 ms."""

    def acquire(self) -> float:
        slept = 0.0
        with self._lock:
            while True:
                now = self._clock()
                self._tokens = min(self.capacity, self._tokens + (now - self._last) * self.rate)
                self._last = now
                if self._tokens >= 1.0 - 1e-9:
                    self._tokens = max(0.0, self._tokens - 1.0)
                    return slept
                wait = max((1.0 - self._tokens) / self.rate, 1e-3)
                self._sleep(wait)
                slept += wait


@dataclass(frozen=True)
class FyersRow:
    ticker: str
    scrip_code: int
    segment: str  # "NSE_FNO" | "NSE_EQ" | "IDX_I"
    underlying: str
    expiry: date | None
    strike: float | None
    option_type: str | None
    expiry_epoch: int | None

    def key(self) -> ContractKey:
        if self.option_type in ("CE", "PE"):
            return ContractKey(self.underlying, self.expiry, self.strike, self.option_type)
        return ContractKey(self.underlying)


class FyersMaster:
    """Rows of NSE_FO.csv + NSE_CM.csv (headerless) indexed by (segment, scrip code) and by ticker."""

    def __init__(self, rows: Sequence[FyersRow]) -> None:
        self.by_code: dict[tuple[str, int], FyersRow] = {}
        self.by_ticker: dict[str, FyersRow] = {}
        self.index_by_name: dict[str, FyersRow] = {}
        for r in rows:
            self.by_ticker.setdefault(r.ticker, r)
            if r.segment == "IDX_I":
                self.index_by_name.setdefault(r.underlying, r)
            else:
                self.by_code.setdefault((r.segment, r.scrip_code), r)

    @staticmethod
    def parse_rows(text: str) -> list[FyersRow]:
        out: list[FyersRow] = []
        for c in csv.reader(io.StringIO(text)):
            if len(c) < 17 or c[10].strip() != "10":
                continue
            ticker = c[9].strip()
            seg_code = c[11].strip()
            opt = c[16].strip()
            try:
                code = int(c[12])
            except ValueError:
                continue
            if seg_code == "11":
                if opt not in ("CE", "PE", "XX"):
                    continue
                segment = "NSE_FNO"
            elif seg_code == "10":
                if ticker.endswith("-INDEX"):
                    segment = "IDX_I"
                elif ticker.endswith("-EQ"):
                    segment = "NSE_EQ"
                else:
                    continue
            else:
                continue
            exp_epoch = int(float(c[8])) if c[8].strip() else None
            is_opt = segment == "NSE_FNO" and opt in ("CE", "PE")
            out.append(
                FyersRow(
                    ticker=ticker,
                    scrip_code=code,
                    segment=segment,
                    underlying=c[13].strip(),
                    expiry=wire.epoch_ist_date(exp_epoch) if exp_epoch and segment == "NSE_FNO" else None,
                    strike=float(c[15]) if is_opt else None,
                    option_type=opt if is_opt else None,
                    expiry_epoch=exp_epoch,
                )
            )
        return out


class FyersAdapter(BrokerAdapter):
    def __init__(
        self,
        app_id: str,
        access_token: str,
        data_dir: Path,
        *,
        http: httpx.Client | None = None,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
        max_retries: int = 3,
    ) -> None:
        self.app_id = app_id
        self._token = access_token
        self.data_dir = Path(data_dir)
        self._sleep = sleep
        self._clock = clock
        self._cooldown_until: float | None = None  # audit BROKER-01: no request before this clock() value
        self.max_retries = max_retries
        self._http = http or httpx.Client(timeout=30.0)
        self._bucket = _Bucket(wire.RATE_PER_S, wire.RATE_PER_S, clock, sleep)
        self._data_budget = FyersDataBudget(self.data_dir / "fyers_api_budget.sqlite")
        self._canon: CanonicalMaster | None = None
        self._native: FyersMaster | None = None
        self._resolved: dict[tuple[str, int], FyersRow | None] = {}
        self.identity_dropped: list[tuple[str, int, str]] = []
        self.ltp_batch_size = wire.QUOTES_MAX  # T-159: first try; an HTTP 400 halves it for the session

    # -- credentials ---------------------------------------------------------------------------------------------
    def set_credentials(self, creds: Mapping[str, str]) -> None:
        """{FYERS_APP_ID, FYERS_ACCESS_TOKEN} (spec §5); an empty value keeps the current one."""
        self.app_id = (creds.get(KEY_APP_ID) or "").strip() or self.app_id
        self._token = (creds.get(KEY_TOKEN) or "").strip() or self._token

    # -- transport -----------------------------------------------------------------------------------------------
    def _headers(self) -> dict[str, str]:
        return {"Authorization": f"{self.app_id}:{self._token}", "version": "3", "Accept": "application/json"}

    def _get(self, url: str, params: Any, *, retries: int | None = None) -> Any:
        """`retries` = this call's retry budget (None = max_retries; 0 = ONE attempt: token_health's price check)."""
        limit = self.max_retries if retries is None else retries
        attempt = 0
        while True:
            if self._cooldown_until is not None and self._clock() < self._cooldown_until:
                left = self._cooldown_until - self._clock()
                raise BrokerError(f"fyers {url}: not sent, rate-limit cool-down after an HTTP 429 ({left:.0f} s left)",
                                  "429", 429)  # fmt: skip
            self._bucket.acquire()
            if url.startswith(wire.DATA_BASE_URL):
                try:
                    self._data_budget.reserve()
                except DailyBudgetExhausted as exc:
                    raise BrokerError(str(exc), "FYERS_LOCAL_DAY_BUDGET", 429) from exc
            try:
                resp = self._http.get(url, params=params, headers=self._headers())
            except httpx.TransportError as exc:
                if attempt < limit:
                    attempt += 1
                    self._sleep(min(2.0**attempt, 10.0))
                    continue
                raise BrokerError(f"fyers {url}: transport error {type(exc).__name__}") from exc
            try:
                body: Any = resp.json()
            except ValueError:
                body = None
            code = wire.error_code(body)
            msg = str(body.get("message") or "")[:200] if isinstance(body, dict) else resp.text[:200]
            if resp.status_code == 401 or code in wire.AUTH_CODES_EXPIRED:
                raise FyersAuthError(f"fyers: access token expired or rejected ({code or 401}) {msg}", "807",
                                     resp.status_code)
            if code in wire.AUTH_CODES_INVALID:
                raise FyersAuthError(f"fyers: authentication failed ({code}) {msg}", "809", resp.status_code)
            if resp.status_code == 429 or code in RATE_LIMIT_CODES:
                # audit BROKER-01: a rate-limit refusal is never retried -- each retry is one more breach toward the
                # F7 day-block (wire.py: > 3 breaches in a minute blocks the app for the day) -- and opens the
                # cool-down; one shape for every caller (code "429", status 429), whatever HTTP status carried it.
                self._cooldown_until = self._clock() + RATE_LIMIT_COOLDOWN_S
                raise BrokerError(f"fyers {url}: rate limited (HTTP {resp.status_code} {code or ''}) {msg}".strip(),
                                  "429", 429)  # fmt: skip
            if resp.status_code == 403:  # 0.8.3 T-229 (2), D230: an entitlement refusal, never retried
                raise BrokerEntitlementError(f"fyers: not permitted (HTTP 403; Market Data permission?) {msg}",
                                             broker="fyers", native=403, hint=hints.FYERS_HINT, status=403)
            if 200 <= resp.status_code < 300 and code is None:
                if body is None:
                    raise BrokerError(f"fyers {url}: non-JSON body", None, resp.status_code)
                return body
            retryable = resp.status_code >= 500  # 5xx keeps its back-off; a 429 never reaches here (above)
            if retryable and attempt < limit:
                attempt += 1
                self._sleep(min(2.0**attempt, 10.0))
                continue
            raise BrokerError(f"fyers {url}: HTTP {resp.status_code} {code or ''} {msg}".strip(), code,
                              resp.status_code)

    # -- masters -------------------------------------------------------------------------------------------------
    def canonical(self) -> CanonicalMaster:
        if self._canon is None:
            self._canon = CanonicalMaster.load(self.data_dir)
        return self._canon

    def scrip_master(self, as_of: date | None = None) -> ScripMaster:
        """Dhan's PUBLIC master = the canonical universe for every broker (spec §1); jobs duck-type this."""
        del as_of
        return self.canonical().sm  # type: ignore[return-value]

    def native_master(self) -> FyersMaster:
        if self._native is None:
            self.data_dir.mkdir(parents=True, exist_ok=True)
            as_of = now_ist().date()
            rows: list[FyersRow] = []
            for name, url in wire.MASTER_URLS.items():
                path = self.data_dir / f"fyers-{name}-{as_of.isoformat()}.csv"
                if not path.exists():
                    try:
                        resp = self._download_master(name, url)
                    except (BrokerError, httpx.HTTPError) as exc:
                        stale = _newest_cached(self.data_dir, f"fyers-{name}-", ".csv", as_of)
                        if stale is None:
                            raise
                        log.warning("fyers %s.csv download failed (%s); using the cached copy from %s",
                                    name, exc, stale[0].isoformat())
                        path = stale[1]
                    else:
                        part = path.with_suffix(".csv.part")
                        part.write_bytes(resp.content)
                        part.replace(path)
                        for old in sorted(self.data_dir.glob(f"fyers-{name}-????-??-??.csv"))[:-MASTER_KEEP]:
                            old.unlink(missing_ok=True)
                rows.extend(FyersMaster.parse_rows(path.read_text(encoding="utf-8")))
            self._native = FyersMaster(rows)
        return self._native

    def _download_master(self, name: str, url: str) -> httpx.Response:
        """GET one public master CSV: up to MASTER_ATTEMPTS tries on a transport error (dropped connection, connect
        failure, timeout) or HTTP 5xx, waiting 2 s, 4 s between (0.8.3). The last failure surfaces EXACTLY as before
        the retry existed -- the raw httpx.TransportError, or BrokerError on a non-200 -- so callers are unchanged.
        A 4xx is not retried."""
        for attempt in range(1, MASTER_ATTEMPTS + 1):
            last = attempt == MASTER_ATTEMPTS
            try:
                resp = self._http.get(url)
            except httpx.TransportError as exc:
                if last:
                    raise
                why = type(exc).__name__
            else:
                if resp.status_code == 200:
                    return resp
                if resp.status_code < 500 or last:
                    raise BrokerError(f"fyers {name}.csv: HTTP {resp.status_code}", None, resp.status_code)
                why = f"HTTP {resp.status_code}"
            wait = 2.0**attempt
            log.warning("fyers %s.csv: %s, try %d/%d, retrying in %.0f s", name, why, attempt, MASTER_ATTEMPTS, wait)
            self._sleep(wait)
        raise AssertionError("unreachable")  # pragma: no cover

    def _row(self, segment: str, security_id: int) -> FyersRow | None:
        key = (segment, int(security_id))
        if key in self._resolved:
            return self._resolved[key]
        nm = self.native_master()
        canon = self.canonical()
        row: FyersRow | None
        if segment == "IDX_I":
            inst = canon.instrument(segment, security_id)
            row = nm.index_by_name.get((inst.underlying_symbol or inst.symbol) if inst else "")
        else:
            row = nm.by_code.get((segment, int(security_id)))
        bad = check_identity(canon, segment, security_id, None if row is None else row.key())
        if bad is not None:
            self.identity_dropped.append((segment, int(security_id), bad.reason))
            log.error("identity guard (fyers): %s %d dropped: %s", segment, int(security_id), bad.reason)
            row = None
        self._resolved[key] = row
        return row

    def _need_row(self, segment: str, security_id: int) -> FyersRow:
        row = self._row(segment, security_id)
        if row is None:
            raise BrokerError(f"fyers: no identity-checked row for {segment} {security_id}")
        return row

    def canonical_for_ticker(self, ticker: str) -> tuple[str, int] | None:
        """Fyers ticker -> identity-checked canonical (segment, token), else None."""
        row = self.native_master().by_ticker.get(ticker)
        if row is None or row.segment == "IDX_I":
            return None
        return (row.segment, row.scrip_code) if self._row(row.segment, row.scrip_code) is row else None

    def topic(self, segment: str, security_id: int) -> str | None:
        """The socket topic for a canonical id after the identity guard (None = dropped / unsupported)."""
        if segment not in wire.SEG_TOPIC or self._row(segment, security_id) is None:
            return None
        return wire.topic_for(segment, security_id)

    # -- BrokerAdapter -------------------------------------------------------------------------------------------
    def instruments(self) -> list[Instrument]:
        return self.canonical().sm.instruments()  # type: ignore[attr-defined]

    def _history(self, row: FyersRow, resolution: str, span: int, from_date: date, to_date: date, *,
                 with_oi: bool) -> list[Candle]:
        out: dict[int, Candle] = {}
        start = from_date
        while start <= to_date:
            end = min(to_date, start + timedelta(days=span - 1))
            params = {
                "symbol": row.ticker,
                "resolution": resolution,
                "date_format": "1",
                "range_from": start.isoformat(),
                "range_to": end.isoformat(),
                "cont_flag": "1",
            }
            if with_oi:
                params["oi_flag"] = "1"
            for c in wire.parse_candles(self._get(wire.DATA_BASE_URL + wire.EP_HISTORY, params), with_oi=with_oi):
                if from_date <= wire.epoch_ist_date(c.ts_ms / 1000) <= to_date:  # IST date, never UTC
                    out[c.ts_ms] = c
            start = end + timedelta(days=1)
        return [out[k] for k in sorted(out)]

    def daily_candles(
        self, security_id: int, exchange_segment: str, from_date: date, to_date: date, instrument: str = "EQUITY"
    ) -> list[Candle]:
        del instrument
        row = self._need_row(exchange_segment, security_id)
        return self._history(row, "D", wire.DAY_SPAN_DAYS, from_date, to_date, with_oi=False)

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
        if interval_min not in wire.RESOLUTIONS:
            raise ValueError(f"interval_min must be one of {tuple(wire.RESOLUTIONS)}")
        row = self._need_row(exchange_segment, security_id)
        return self._history(row, wire.RESOLUTIONS[interval_min], wire.MINUTE_SPAN_DAYS, from_date, to_date,
                             with_oi=oi and row.segment == "NSE_FNO")

    def _chain_body(self, row: FyersRow, expiry_epoch: str | None, strikecount: int) -> Any:
        params: dict[str, str] = {"symbol": row.ticker, "strikecount": str(strikecount)}
        if expiry_epoch:
            params["timestamp"] = expiry_epoch
        return self._get(wire.DATA_BASE_URL + wire.EP_CHAIN, params)

    def expiries(self, underlying_security_id: int, underlying_seg: str) -> list[date]:
        row = self._need_row(underlying_seg, underlying_security_id)
        return [d for d, _ in wire.parse_expiry_data(self._chain_body(row, None, 1))]

    def _expiry_epoch(self, underlying: FyersRow, expiry: date) -> str | None:
        for r in self.native_master().by_code.values():
            if r.segment == "NSE_FNO" and r.option_type and r.underlying == underlying.underlying and r.expiry == expiry:
                return str(r.expiry_epoch) if r.expiry_epoch else None
        return None

    def option_chain(self, underlying_security_id: int, underlying_seg: str, expiry: date) -> list[ChainRow]:
        row = self._need_row(underlying_seg, underlying_security_id)
        epoch = self._expiry_epoch(row, expiry)
        if epoch is None:
            raise BrokerError(f"fyers: no {row.underlying} contract expiring {expiry} in the Fyers master")
        rows: list[ChainRow] = []
        skipped = 0
        for leg in wire.parse_chain(self._chain_body(row, epoch, wire.CHAIN_STRIKECOUNT)):
            canon = self.canonical_for_ticker(leg.symbol)
            inst = None if canon is None else self.canonical().instrument(*canon)
            if canon is None or inst is None or inst.expiry_date != expiry or leg.ltp is None or leg.oi is None:
                skipped += 1
                continue
            rows.append(ChainRow(leg.strike, leg.option_type, canon[1], float(leg.ltp), int(leg.oi),
                                 int(leg.volume or 0), None, leg.prev_oi))
        if skipped:
            log.info("fyers chain %s %s: %d legs skipped (no identity-checked token / LTP / OI)", row.ticker,
                     expiry, skipped)
        rows.sort(key=lambda r: (r.strike, r.option_type))
        return rows

    def ltp(self, security_ids: Sequence[int], exchange_segment: str = "NSE_EQ") -> dict[int, float]:
        wanted: dict[str, int] = {}
        for sid in security_ids:
            row = self._row(exchange_segment, int(sid))
            if row is not None:
                wanted[row.ticker] = int(sid)
        tickers = list(wanted)
        out: dict[int, float] = {}

        def fetch(chunk: list[str]) -> None:
            body = self._get(wire.DATA_BASE_URL + wire.EP_QUOTES, {"symbols": ",".join(chunk)})
            for t, p in wire.parse_quotes_ltp(body).items():
                if t in wanted:
                    out[wanted[t]] = p

        _, self.ltp_batch_size = batched(tickers, self.ltp_batch_size, fetch, broker="fyers", partial=True,
                                         on_shrink=lambda _o, n: setattr(self, "ltp_batch_size", n))
        return out

    def token_health(self) -> TokenHealth:
        claims = wire.jwt_claims(self._token) or {}
        exp = claims.get("exp")
        validity = None
        if isinstance(exp, int | float) and not isinstance(exp, bool):
            validity = datetime.fromtimestamp(float(exp), IST).isoformat(timespec="seconds")
        try:
            self._get(wire.API_BASE_URL + wire.EP_PROFILE, None)
        except BrokerError as exc:  # FyersAuthError 807/809 -> auth_failed; 806 (permission) is not (W14 H4)
            return health_from_error(exc, validity_ist=validity)
        prices, problem = self._quote_check()
        return TokenHealth(True, validity, None, problem=problem, prices=prices)

    def _quote_check(self) -> tuple[bool | None, str | None]:
        """0.8.3 T-229 (3): ONE direct quote, ONE attempt (no retry: a 429 must never count toward the F7 day-block),
        needing no master download (the ticker is literal). (True, None) = prices flowed; (False, hint) = refused for
        entitlement; (None, None) = not judged -- any other failure never makes the login invalid (WARNING)."""
        try:
            self._get(wire.DATA_BASE_URL + wire.EP_QUOTES, {"symbols": QUOTE_CHECK_SYMBOL}, retries=0)
        except BrokerEntitlementError as exc:
            return False, exc.hint
        except Exception as exc:  # noqa: BLE001 -- the profile passed; a failed price check is not a bad login
            log.warning("fyers token check: the price check could not run (%s)", type(exc).__name__)
            return None, None
        return True, None
