"""GrowwAdapter: BrokerAdapter + QuoteSource over raw httpx (BROKER-ADAPTER-SPEC §2, §3, §8). BETA: not verified live.

Identity (spec §1): canonical `security_id` = the NSE exchange token. Groww's own `instrument.csv` (downloaded once
per IST day, `data/groww-instrument-YYYY-MM-DD.csv`, 3 kept) maps it to Groww's `trading_symbol` / `groww_symbol`:
NSE_EQ and NSE_FNO by `exchange_token` (== the canonical token, measured), IDX_I by the index SYMBOL (Groww keys
its index rows by name, wire.py [M]). Every native row passes the RUNTIME IDENTITY GUARD
(`gammad.broker.canonical.check_identity`: underlying, expiry, strike, CE/PE vs Dhan's public master) before its
first use; a mismatch is dropped with one ERROR and recorded in `identity_dropped`, never substituted.

Wire + evidence: gammad/broker/groww/wire.py. Errors: HTTP 401 -> GrowwAuthError code "809" (authentication failed:
the docs give no finer code), HTTP 403 -> code "806" (authorisation = the Trade API subscription, G9); both never
retried. 429 / 5xx / transport errors retry with back-off, then BrokerError.
No order method exists here (spec: an adapter never places orders).
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
from gammad.broker.batching import batched
from gammad.broker.canonical import CanonicalMaster, ContractKey, check_identity
from gammad.broker.dhan import ScripMaster, TokenBucket
from gammad.broker.groww import wire
from gammad.config import IST, now_ist
from gammad.core import session_clock
from gammad.log import get_logger
from gammad.watch.feed_protocol import Quote

log = get_logger(__name__)

KEY_TOKEN = "GROWW_ACCESS_TOKEN"
MASTER_KEEP = 3


class GrowwAuthError(BrokerAuthError):
    """Groww rejected the access token (HTTP 401 / 403)."""


class _Bucket(TokenBucket):
    """dhan.TokenBucket with a float guard: when `1 - tokens` rounds to ~1e-17, the base acquire() sleeps a wait
    that no longer moves a fake (or coarse) clock and spins forever (hit by the Fyers harness run). Tokens within
    1e-9 of one count as one, and every wait is at least 1 ms."""

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
class GrowwRow:
    """One NSE row of Groww's instrument.csv (the columns the adapter reads)."""

    exchange_token: str
    trading_symbol: str
    groww_symbol: str
    instrument_type: str  # EQ | IDX | CE | PE | FUT
    segment: str  # CASH | FNO
    underlying_symbol: str
    expiry: date | None
    strike: float | None

    def key(self) -> ContractKey:
        if self.instrument_type in ("CE", "PE"):
            return ContractKey(self.underlying_symbol, self.expiry, self.strike, self.instrument_type)
        return ContractKey(self.trading_symbol)


class GrowwMaster:
    """NSE rows of instrument.csv indexed by (segment, exchange_token) and FNO trading_symbol."""

    def __init__(self, rows: Sequence[GrowwRow]) -> None:
        self.by_token: dict[tuple[str, str], GrowwRow] = {}
        self.by_symbol: dict[str, GrowwRow] = {}
        for r in rows:
            self.by_token.setdefault((r.segment, r.exchange_token), r)
            if r.segment == wire.SEG_FNO:
                self.by_symbol.setdefault(r.trading_symbol, r)

    @classmethod
    def parse(cls, text: str) -> GrowwMaster:
        rows: list[GrowwRow] = []
        for d in csv.DictReader(io.StringIO(text)):
            if (d.get("exchange") or "").strip() != wire.EXCHANGE:
                continue
            seg = (d.get("segment") or "").strip()
            if seg not in (wire.SEG_CASH, wire.SEG_FNO):
                continue
            itype = (d.get("instrument_type") or "").strip()
            exp_s = (d.get("expiry_date") or "").strip()
            strike_s = (d.get("strike_price") or "").strip()
            try:
                expiry = date.fromisoformat(exp_s[:10]) if exp_s and exp_s.lower() != "nan" else None
                strike = float(strike_s) if itype in ("CE", "PE") and strike_s and strike_s.lower() != "nan" else None
            except ValueError:
                continue
            rows.append(
                GrowwRow(
                    exchange_token=(d.get("exchange_token") or "").strip(),
                    trading_symbol=(d.get("trading_symbol") or "").strip(),
                    groww_symbol=(d.get("groww_symbol") or "").strip(),
                    instrument_type=itype,
                    segment=seg,
                    underlying_symbol=(d.get("underlying_symbol") or "").strip(),
                    expiry=expiry,
                    strike=strike,
                )
            )
        return cls(rows)


class GrowwAdapter(BrokerAdapter):
    def __init__(
        self,
        access_token: str,
        data_dir: Path,
        *,
        http: httpx.Client | None = None,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
        max_retries: int = 3,
    ) -> None:
        self._token = access_token
        self.data_dir = Path(data_dir)
        self._sleep = sleep
        self.max_retries = max_retries
        self._http = http or httpx.Client(base_url=wire.API_BASE_URL, timeout=30.0)
        self._live_bucket = _Bucket(wire.LIVE_RATE_PER_S, wire.LIVE_BURST, clock, sleep)
        self._hist_bucket = _Bucket(wire.HIST_RATE_PER_S, wire.HIST_RATE_PER_S, clock, sleep)
        self._canon: CanonicalMaster | None = None
        self._native: GrowwMaster | None = None
        self._resolved: dict[tuple[str, int], GrowwRow | None] = {}
        self.identity_dropped: list[tuple[str, int, str]] = []

    # -- credentials ---------------------------------------------------------------------------------------------
    def set_credentials(self, creds: Mapping[str, str] | str) -> None:
        """{GROWW_ACCESS_TOKEN: ...} (spec §5) or a bare token; an empty value keeps the current token."""
        token = (creds.get(KEY_TOKEN) or "") if isinstance(creds, Mapping) else (creds or "")
        if token.strip():
            self._token = token.strip()

    def set_token(self, access_token: str) -> None:
        self.set_credentials(access_token)

    # -- transport -----------------------------------------------------------------------------------------------
    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self._token}",
            "Accept": "application/json",
            "x-api-version": "1.0",
        }

    def _get(self, endpoint: str, params: Any, bucket: TokenBucket) -> Any:
        attempt = 0
        while True:
            bucket.acquire()
            try:
                resp = self._http.get(endpoint, params=params, headers=self._headers())
            except httpx.TransportError as exc:
                if attempt < self.max_retries:
                    attempt += 1
                    self._sleep(min(2.0**attempt, 10.0))
                    continue
                raise BrokerError(f"groww {endpoint}: transport error {type(exc).__name__}") from exc
            try:
                body: Any = resp.json()
            except ValueError:
                body = None
            fail = wire.failure(body)
            msg = fail[1] if fail else (resp.text[:200] if body is None else "")
            if resp.status_code == 401:
                raise GrowwAuthError(f"groww {endpoint}: authentication failed (HTTP 401) {msg}", "809", 401)
            if resp.status_code == 403:
                raise GrowwAuthError(f"groww {endpoint}: not authorised (HTTP 403) {msg}", "806", 403)
            if 200 <= resp.status_code < 300 and fail is None:
                if body is None:
                    raise BrokerError(f"groww {endpoint}: non-JSON body", None, resp.status_code)
                return wire.unwrap(body)
            retryable = resp.status_code == 429 or resp.status_code >= 500
            if retryable and attempt < self.max_retries:
                attempt += 1
                self._sleep(min(2.0**attempt, 10.0))
                continue
            code = fail[0] if fail else None
            raise BrokerError(f"groww {endpoint}: HTTP {resp.status_code} {code or ''} {msg}".strip(), code,
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

    def native_master(self) -> GrowwMaster:
        if self._native is None:
            self.data_dir.mkdir(parents=True, exist_ok=True)
            path = self.data_dir / f"groww-instrument-{now_ist().date().isoformat()}.csv"
            if not path.exists():
                resp = self._http.get(wire.INSTRUMENT_CSV_URL)
                if resp.status_code != 200:
                    raise BrokerError(f"groww instrument.csv: HTTP {resp.status_code}", None, resp.status_code)
                part = path.with_suffix(".csv.part")
                part.write_bytes(resp.content)
                part.replace(path)
                for old in sorted(self.data_dir.glob("groww-instrument-????-??-??.csv"))[:-MASTER_KEEP]:
                    old.unlink(missing_ok=True)
            self._native = GrowwMaster.parse(path.read_text(encoding="utf-8"))
        return self._native

    def _row(self, segment: str, security_id: int) -> GrowwRow | None:
        """The identity-checked Groww row for a canonical id, or None (dropped; logged once)."""
        key = (segment, int(security_id))
        if key in self._resolved:
            return self._resolved[key]
        nm = self.native_master()
        canon = self.canonical()
        row: GrowwRow | None
        if segment == "IDX_I":
            inst = canon.instrument(segment, security_id)
            sym = (inst.underlying_symbol or inst.symbol) if inst else ""
            row = nm.by_token.get((wire.SEG_CASH, sym)) if sym else None
        else:
            row = nm.by_token.get((wire.SEGMENT.get(segment, ""), str(int(security_id))))
        bad = check_identity(canon, segment, security_id, None if row is None else row.key())
        if bad is not None:
            self.identity_dropped.append((segment, int(security_id), bad.reason))
            log.error("identity guard (groww): %s %d dropped: %s", segment, int(security_id), bad.reason)
            row = None
        self._resolved[key] = row
        return row

    def _canon_for_fno_symbol(self, trading_symbol: str) -> int | None:
        row = self.native_master().by_symbol.get(trading_symbol)
        if row is None or not row.exchange_token.isdigit():
            return None
        sid = int(row.exchange_token)
        return sid if self._row("NSE_FNO", sid) is row else None

    # -- BrokerAdapter -------------------------------------------------------------------------------------------
    def instruments(self) -> list[Instrument]:
        return self.canonical().sm.instruments()  # type: ignore[attr-defined]

    def _candles(
        self, row: GrowwRow, interval: tuple[str, int], from_date: date, to_date: date, *, with_oi: bool,
        day_start: str, day_end: str | None,
    ) -> list[Candle]:
        name, span = interval
        out: dict[int, Candle] = {}
        start = from_date
        while start <= to_date:
            end = min(to_date, start + timedelta(days=span - 1))
            params = {
                "exchange": wire.EXCHANGE,
                "segment": row.segment,
                "groww_symbol": row.groww_symbol,
                "start_time": f"{start.isoformat()} {day_start}",
                "end_time": f"{end.isoformat()} {day_end or session_clock.fo_close_hm(end) + ':00'}",
                "candle_interval": name,
            }
            for c in wire.parse_candles(self._get(wire.EP_CANDLES, params, self._hist_bucket), with_oi=with_oi):
                if from_date <= datetime.fromtimestamp(c.ts_ms / 1000, IST).date() <= to_date:
                    out[c.ts_ms] = c
            start = end + timedelta(days=1)
        return [out[k] for k in sorted(out)]

    def _need_row(self, segment: str, security_id: int) -> GrowwRow:
        row = self._row(segment, security_id)
        if row is None:
            raise BrokerError(f"groww: no identity-checked row for {segment} {security_id}")
        return row

    def daily_candles(
        self, security_id: int, exchange_segment: str, from_date: date, to_date: date, instrument: str = "EQUITY"
    ) -> list[Candle]:
        del instrument
        row = self._need_row(exchange_segment, security_id)
        return self._candles(row, wire.DAILY_INTERVAL, from_date, to_date, with_oi=False,
                             day_start="00:00:00", day_end="23:59:59")

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
        if interval_min not in wire.INTERVALS:
            raise ValueError(f"interval_min must be one of {tuple(wire.INTERVALS)}")
        row = self._need_row(exchange_segment, security_id)
        return self._candles(row, wire.INTERVALS[interval_min], from_date, to_date,
                             with_oi=oi and row.segment == wire.SEG_FNO, day_start="09:15:00", day_end=None)

    def _underlying_symbol(self, security_id: int, segment: str) -> str:
        row = self._need_row(segment, security_id)
        return row.trading_symbol

    def expiries(self, underlying_security_id: int, underlying_seg: str) -> list[date]:
        sym = self._underlying_symbol(underlying_security_id, underlying_seg)
        params: dict[str, str] = {"exchange": wire.EXCHANGE, "underlying_symbol": sym}
        out = set(wire.parse_expiries(self._get(wire.EP_EXPIRIES, params, self._hist_bucket)))
        today = now_ist().date()
        if today.month >= 10:  # "no year = the current year only": monthlies run 3 months out
            nxt = {**params, "year": str(today.year + 1)}
            out |= set(wire.parse_expiries(self._get(wire.EP_EXPIRIES, nxt, self._hist_bucket)))
        return sorted(out)

    def option_chain(self, underlying_security_id: int, underlying_seg: str, expiry: date) -> list[ChainRow]:
        sym = self._underlying_symbol(underlying_security_id, underlying_seg)
        body = self._get(wire.EP_CHAIN.format(symbol=sym), {"expiry_date": expiry.isoformat()}, self._hist_bucket)
        rows: list[ChainRow] = []
        skipped = 0
        for strike, side, tsym, ltp, oi, vol in wire.parse_chain(body):
            sid = self._canon_for_fno_symbol(tsym)
            inst = None if sid is None else self.canonical().instrument("NSE_FNO", sid)
            if sid is None or ltp is None or oi is None or inst is None or inst.expiry_date != expiry:
                skipped += 1
                continue
            rows.append(ChainRow(float(strike), side, sid, float(ltp), int(oi), int(vol or 0), None, None))
        if skipped:
            log.info("groww chain %s %s: %d rows skipped (no identity-checked token / LTP / OI)", sym, expiry, skipped)
        rows.sort(key=lambda r: (r.strike, r.option_type))
        return rows

    def ltp(self, security_ids: Sequence[int], exchange_segment: str = "NSE_EQ") -> dict[int, float]:
        wanted: dict[str, int] = {}
        for sid in security_ids:
            row = self._row(exchange_segment, int(sid))
            if row is not None:
                wanted[wire.exchange_symbol(row.trading_symbol)] = int(sid)
        keys = list(wanted)
        out: dict[int, float] = {}
        seg = wire.SEGMENT.get(exchange_segment, wire.SEG_CASH)

        def fetch(chunk: list[str]) -> None:
            prices = wire.parse_ltp(self._get(wire.EP_LTP, [("segment", seg)] + [("exchange_symbols", k) for k in chunk],
                                              self._live_bucket))
            for k, v in prices.items():
                if k in wanted:
                    out[wanted[k]] = v

        # 0.8.5 D5: the shared multi-batch ltp contract (batching.py): a failed batch is retried once, then left out
        batched(keys, wire.LTP_MAX_IDS, fetch, broker="groww ltp", partial=True, halve=False)
        return out

    def quotes(self, security_ids: Sequence[int], exchange_segment: str) -> dict[int, Quote]:
        """One get_quote call per id (Groww has no batch quote). An id whose call fails for a non-auth reason is
        ABSENT (PollFeed counts it missing); an auth failure raises."""
        out: dict[int, Quote] = {}
        seg = wire.SEGMENT.get(exchange_segment, wire.SEG_CASH)
        for sid in security_ids:
            row = self._row(exchange_segment, int(sid))
            if row is None:
                continue
            params = {"exchange": wire.EXCHANGE, "segment": seg, "trading_symbol": row.trading_symbol}
            try:
                payload = self._get(wire.EP_QUOTE, params, self._live_bucket)
            except BrokerAuthError:
                raise
            except BrokerError as exc:
                log.warning("groww quote %s %s failed: %s", exchange_segment, sid, exc)
                continue
            q = wire.parse_quote(payload, has_oi=exchange_segment == "NSE_FNO")
            if q is not None:
                out[int(sid)] = q
        return out

    def token_health(self) -> TokenHealth:
        try:
            self._get(wire.EP_PROFILE, None, self._hist_bucket)
        except BrokerError as exc:  # GrowwAuthError 401 -> auth_failed; 403 (806, entitlement) is not (W14 H4)
            return health_from_error(exc)
        return TokenHealth(True, None, None)  # the docs state no expiry (research §3)
