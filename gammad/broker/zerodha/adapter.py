"""Zerodha Kite Connect REST adapter over raw httpx (BROKER-ADAPTER-SPEC §7-§8). Read-only: no order method.

Identity (spec §1): canonical (segment, NSE exchange token) <-> Kite `instrument_token` comes from Kite's OWN
public instrument dump (`/instruments/NSE`, `/instruments/NFO`, cached per IST day in data_dir). Building the map
IS the runtime identity guard: a Kite row sitting at a canonical token must be the same (underlying, expiry,
strike, CE/PE) as the canonical row, else it is dropped with an ERROR (`identity_dropped`) and never substituted.
Chain = "master+quotes": canonical master option rows + one /quote batch (K5, K6: no chain endpoint).
Expiries = canonical master (no expiry endpoint).
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable, Mapping, Sequence
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
from gammad.broker.canonical import (
    SEG_EQ,
    SEG_IDX,
    SEG_OPT,
    CanonicalMaster,
    ContractKey,
    IdentityMismatch,
    NativeIdMap,
    chain_from_master_quotes,
    check_identity,
)
from gammad.broker.dhan import TokenBucket
from gammad.broker.zerodha import wire
from gammad.config import IST, now_ist
from gammad.log import get_logger
from gammad.watch.feed_protocol import Quote

log = get_logger(__name__)

KEY_API_KEY = "KITE_API_KEY"
KEY_API_SECRET = "KITE_API_SECRET"
KEY_TOKEN = "KITE_ACCESS_TOKEN"
DUMP_EXCHANGES = ("NSE", "NFO")
DUMP_KEEP = 3
SESSION_TTL_S = 300.0


class KiteAuthError(BrokerAuthError):
    """Kite rejected the session (TokenException -> code "807") or the plan (PermissionException -> "806")."""


class KiteAdapter(BrokerAdapter):
    def __init__(
        self,
        api_key: str,
        access_token: str,
        data_dir: Path,
        *,
        http: httpx.Client | None = None,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
        max_retries: int = 3,
    ) -> None:
        self.api_key = api_key
        self._token = access_token
        self.data_dir = Path(data_dir)
        self._sleep = sleep
        self._clock = clock
        self._session_ok_until = float("-inf")
        self.max_retries = max_retries
        self._http = http or httpx.Client(base_url=wire.API_BASE_URL, timeout=30.0)
        self._quote_bucket = TokenBucket(wire.RATE_QUOTE_PER_S, 1.0, clock, sleep)
        self._hist_bucket = TokenBucket(wire.RATE_HISTORY_PER_S, wire.RATE_HISTORY_PER_S, clock, sleep)
        self._other_bucket = TokenBucket(wire.RATE_OTHER_PER_S, wire.RATE_OTHER_PER_S, clock, sleep)
        self._canon: CanonicalMaster | None = None
        self._map: NativeIdMap | None = None
        self._rows: dict[int, wire.KiteRow] = {}
        self.identity_dropped: list[IdentityMismatch] = []
        self._lock = threading.Lock()
        # T-159: the documented sizes (K5) are the first try; an HTTP 400 halves them for the session
        self.quote_batch_size = wire.QUOTE_MAX_IDS
        self.ltp_batch_size = wire.LTP_MAX_IDS

    # ------------------------------------------------------------------ credentials
    def set_credentials(self, creds: Mapping[str, str] | str, access_token: str | None = None) -> None:
        """Mapping {KITE_API_KEY, KITE_ACCESS_TOKEN} (spec §5) or legacy (api_key, access_token); an absent or
        empty value keeps the current one."""
        if isinstance(creds, Mapping):
            self.api_key = (creds.get(KEY_API_KEY) or "").strip() or self.api_key
            self._token = (creds.get(KEY_TOKEN) or "").strip() or self._token
        else:
            self.api_key = creds or self.api_key
            self._token = access_token if access_token is not None else self._token

    def set_token(self, access_token: str) -> None:
        self._token = access_token

    # ------------------------------------------------------------------ transport
    def _headers(self) -> dict[str, str]:
        return {"X-Kite-Version": wire.KITE_VERSION, "Authorization": f"token {self.api_key}:{self._token}"}

    def _send(self, path: str, bucket: TokenBucket, params: Any = None) -> httpx.Response:
        attempt = 0
        while True:
            bucket.acquire()
            try:
                resp = self._http.get(path, params=params, headers=self._headers())
            except httpx.TransportError as exc:
                if attempt < self.max_retries:
                    attempt += 1
                    self._sleep(min(2.0**attempt, 10.0))
                    continue
                raise BrokerError(f"kite {path}: transport error {type(exc).__name__}") from exc
            body: Any = None
            if "json" in resp.headers.get("content-type", ""):
                try:
                    body = resp.json()
                except ValueError:
                    body = None
            etype, msg = wire.error_info(body)
            code = wire.auth_code_for_error(resp.status_code, etype)
            if code is not None:
                raise KiteAuthError(
                    f"kite {path}: rejected ({etype or resp.status_code}): {msg}", code, resp.status_code
                )
            if 200 <= resp.status_code < 300 and etype is None:
                return resp
            if (resp.status_code == 429 or resp.status_code >= 500) and attempt < self.max_retries:
                attempt += 1
                self._sleep(min(2.0**attempt, 10.0))
                continue
            raise BrokerError(
                f"kite {path}: HTTP {resp.status_code} {etype or ''} {msg}".strip(), etype, resp.status_code
            )

    def _json(self, path: str, bucket: TokenBucket, params: Any = None) -> dict[str, Any]:
        resp = self._send(path, bucket, params)
        try:
            body = resp.json()
        except ValueError as exc:
            raise BrokerError(f"kite {path}: non-JSON body", None, resp.status_code) from exc
        if not isinstance(body, dict):
            raise BrokerError(f"kite {path}: unexpected body", None, resp.status_code)
        return body

    # ------------------------------------------------------------------ masters + identity
    def canonical(self) -> CanonicalMaster:
        if self._canon is None:
            self._canon = CanonicalMaster.load(self.data_dir)
        return self._canon

    def scrip_master(self, as_of: date | None = None) -> Any:
        """The CANONICAL (Dhan public) master: universe, lot sizes (mark_levels / build_watchlists duck-type)."""
        del as_of
        return self.canonical().sm

    def _dump_text(self, exchange: str) -> str:
        today = now_ist().date().isoformat()
        path = self.data_dir / f"kite-instruments-{exchange}-{today}.csv"
        if path.exists():
            return path.read_text(encoding="utf-8")
        text = self._send(wire.EP_INSTRUMENTS.format(exchange=exchange), self._other_bucket).text
        self.data_dir.mkdir(parents=True, exist_ok=True)
        part = path.with_suffix(".part")
        part.write_text(text, encoding="utf-8")
        part.replace(path)
        for old in sorted(self.data_dir.glob(f"kite-instruments-{exchange}-????-??-??.csv"))[:-DUMP_KEEP]:
            old.unlink(missing_ok=True)
        return text

    def native_map(self) -> NativeIdMap:
        """Canonical <-> instrument_token, built once per adapter from Kite's dump with the identity guard."""
        with self._lock:
            if self._map is not None:
                return self._map
            canon = self.canonical()
            m = NativeIdMap()
            rows: dict[int, wire.KiteRow] = {}
            dropped: list[IdentityMismatch] = []
            index_ids = {
                kite_name: canon.sm.index_security_id(sym) for sym, kite_name in wire.INDEX_SYMBOLS.items()
            }
            for exchange in DUMP_EXCHANGES:
                for r in wire.parse_instruments_csv(self._dump_text(exchange)):
                    placed = self._place(canon, r, index_ids)
                    if placed is None:
                        continue
                    seg, sid, native_key = placed
                    if canon.instrument(seg, sid) is None:
                        continue  # not a canonical contract (other series, far strikes): simply unmapped
                    bad = check_identity(canon, seg, sid, native_key)
                    if bad is not None:
                        dropped.append(bad)
                        log.error("identity guard (zerodha): %s %d dropped: %s", seg, sid, bad.reason)
                        continue
                    try:
                        m.add(seg, sid, r.instrument_token)
                    except ValueError as exc:
                        log.error("identity guard (zerodha): %s", exc)
                        continue
                    rows[r.instrument_token] = r
            self._map, self._rows, self.identity_dropped = m, rows, dropped
            log.info(
                "zerodha: native-id map %d contracts, %d dropped by the identity guard", len(m), len(dropped)
            )
            return m

    @staticmethod
    def _place(
        canon: CanonicalMaster, r: wire.KiteRow, index_ids: Mapping[str, int | None]
    ) -> tuple[str, int, ContractKey] | None:
        if r.exchange == "NSE" and r.segment == "NSE" and r.instrument_type == "EQ":
            return SEG_EQ, r.exchange_token, ContractKey(r.tradingsymbol)
        if r.exchange == "NSE" and r.segment == "INDICES":
            sid = index_ids.get(r.tradingsymbol)
            if sid is None:
                return None
            return SEG_IDX, sid, ContractKey(canon.underlying_symbol(SEG_IDX, sid) or r.tradingsymbol)
        if r.exchange == "NFO" and r.instrument_type in ("CE", "PE", "FUT"):
            opt = r.instrument_type if r.instrument_type in ("CE", "PE") else None
            return SEG_OPT, r.exchange_token, ContractKey(r.name, r.expiry, r.strike if opt else None, opt)
        return None

    def native_id(self, segment: str, security_id: int) -> int | None:
        """instrument_token for a canonical contract, None when Kite has no (identity-matching) row."""
        m = self.native_map()
        try:
            return int(m.native(segment, security_id))  # type: ignore[arg-type]
        except KeyError:
            return None

    def _row(self, segment: str, security_id: int) -> wire.KiteRow:
        tok = self.native_id(segment, security_id)
        if tok is None:
            raise BrokerError(f"kite: no identity-matching instrument for {segment} {security_id}")
        return self._rows[tok]

    # ------------------------------------------------------------------ BrokerAdapter
    def instruments(self) -> list[Instrument]:
        return self.canonical().sm.instruments()

    def _candles(
        self, segment: str, security_id: int, interval: str, from_date: date, to_date: date, with_oi: bool
    ) -> list[Candle]:
        if from_date > to_date:
            return []
        tok = self._row(segment, security_id).instrument_token
        path = wire.EP_HISTORICAL.format(token=tok, interval=interval)
        span = wire.MAX_DAYS[interval]
        oi = with_oi and segment == SEG_OPT
        bars: dict[int, Candle] = {}
        d0 = from_date
        while d0 <= to_date:
            d1 = min(to_date, d0 + timedelta(days=span - 1))
            params = {
                "from": f"{d0.isoformat()} 00:00:00",
                "to": f"{d1.isoformat()} 23:59:59",
                "oi": 1 if oi else 0,
            }
            body = self._json(path, self._hist_bucket, params)
            for c in wire.parse_candles(body, with_oi=oi):
                if from_date <= wire.ist_date(c.ts_ms) <= to_date:
                    bars[c.ts_ms] = c
            d0 = d1 + timedelta(days=1)
        return [bars[k] for k in sorted(bars)]

    def daily_candles(
        self,
        security_id: int,
        exchange_segment: str,
        from_date: date,
        to_date: date,
        instrument: str = "EQUITY",
    ) -> list[Candle]:
        del instrument
        return self._candles(exchange_segment, security_id, "day", from_date, to_date, with_oi=False)

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
        interval = wire.INTERVALS.get(int(interval_min))
        if interval is None:
            raise BrokerError(
                f"kite: unsupported interval {interval_min} min (one of {sorted(wire.INTERVALS)})"
            )
        return self._candles(exchange_segment, security_id, interval, from_date, to_date, with_oi=oi)

    def _check_session(self) -> None:
        """Calls answered from the public masters still prove the session first (spec 9.5: a dead token must
        raise BrokerAuthError, never return data); a success is trusted for SESSION_TTL_S."""
        now = self._clock()
        if now < self._session_ok_until:
            return
        self._json(wire.EP_PROFILE, self._other_bucket)
        self._session_ok_until = now + SESSION_TTL_S

    def expiries(self, underlying_security_id: int, underlying_seg: str) -> list[date]:
        self._check_session()
        sym = self.canonical().underlying_symbol(underlying_seg, underlying_security_id)
        if sym is None:
            raise BrokerError(f"kite: unknown underlying {underlying_seg} {underlying_security_id}")
        return self.canonical().expiries(sym)

    def option_chain(self, underlying_security_id: int, underlying_seg: str, expiry: date) -> list[ChainRow]:
        sym = self.canonical().underlying_symbol(underlying_seg, underlying_security_id)
        if sym is None:
            raise BrokerError(f"kite: unknown underlying {underlying_seg} {underlying_security_id}")
        rows = self.canonical().option_rows(sym, expiry)
        quotes = self.quotes([r.security_id for r in rows], SEG_OPT)
        return chain_from_master_quotes(rows, quotes)

    def quotes(self, security_ids: Sequence[int], exchange_segment: str) -> dict[int, Quote]:
        """/quote in batches of 500 at 1 req/s (K5, K6). Canonical ids in and out; ids Kite lacks are absent."""
        m = self.native_map()
        pairs = [(int(s), self.native_id(exchange_segment, s)) for s in security_ids]
        wanted = [(s, t) for s, t in pairs if t is not None]
        out: dict[int, Quote] = {}

        def fetch(chunk: list[tuple[int, int]]) -> None:
            params = [("i", self._rows[t].quote_key) for _, t in chunk]
            body = self._json(wire.EP_QUOTE, self._quote_bucket, params)
            for tok, (q, _) in wire.parse_quotes(body).items():
                try:
                    seg, sid = m.canonical(tok)
                except KeyError:
                    continue
                if seg != exchange_segment:
                    continue
                out[sid] = q if seg == SEG_OPT else Quote(q.ltp, q.volume_day, None, q.ltt_ms)

        _, self.quote_batch_size = batched(wanted, self.quote_batch_size, fetch, broker="zerodha /quote",
                                           on_shrink=lambda _o, n: setattr(self, "quote_batch_size", n))
        return out

    def ltp(self, security_ids: Sequence[int], exchange_segment: str = "NSE_EQ") -> dict[int, float]:
        m = self.native_map()
        wanted = [t for t in (self.native_id(exchange_segment, s) for s in security_ids) if t is not None]
        out: dict[int, float] = {}

        def fetch(chunk: list[int]) -> None:
            params = [("i", self._rows[t].quote_key) for t in chunk]
            body = self._json(wire.EP_LTP, self._quote_bucket, params)
            for tok, px in wire.parse_ltp(body).items():
                try:
                    seg, sid = m.canonical(tok)
                except KeyError:
                    continue
                if seg == exchange_segment:
                    out[sid] = px

        _, self.ltp_batch_size = batched(wanted, self.ltp_batch_size, fetch, broker="zerodha /quote/ltp", partial=True,
                                         on_shrink=lambda _o, n: setattr(self, "ltp_batch_size", n))
        return out

    def token_health(self) -> TokenHealth:
        """GET /user/profile. Kite states no expiry per token: the rule is 06:00 IST the next day (K7)."""
        try:
            self._json(wire.EP_PROFILE, self._other_bucket)
        except BrokerError as exc:  # KiteAuthError (TokenException / 401 / 403) -> auth_failed (W14 H4)
            return health_from_error(exc)
        return TokenHealth(True, next_expiry_ist(now_ist()).isoformat(), None)


def next_expiry_ist(now: datetime) -> datetime:
    """06:00 IST after `now` (K7: "expire at 6 AM on the next day")."""
    t = now.astimezone(IST)
    six = t.replace(hour=6, minute=0, second=0, microsecond=0)
    return six if t < six else six + timedelta(days=1)
