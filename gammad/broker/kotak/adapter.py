"""Kotak Neo REST adapter (BROKER-ADAPTER-SPEC §4, §8). Read-only: there is no order method at all.

Credentials: market data needs only `Authorization: <KOTAK_CONSUMER_KEY>` (the NEO API-dashboard "access token")
on the per-account KOTAK_BASE_URL (wire.py). The daily session sid (KOTAK_ACCESS_TOKEN) is what the live feed
authenticates with; the trade token that could place orders is never stored.

Identity (spec §1; Kotak NOT measured): public methods speak CANONICAL (segment, security_id). NSE_EQ tokens are the
NSE exchange tokens (instruments.md: nse_cm|11536 = TCS, nse_cm|1594 = INFY); IDX_I goes by the documented index
name (quotes.md glossary). NSE_FNO tokens are accepted only once an option-chain row read from Kotak at that token
carries the canonical (underlying, expiry, strike, CE/PE) -- `_guard` is the runtime identity guard; a mismatch is
dropped with an ERROR and remembered, never substituted. `native_ids()` holds every verified pair.

Capability gaps: candles carry no OI (KN2, "not yet populated") -> the app primes warm-up OI from `quotes()`,
which for F&O reads the documented option chain (LTP, volume, OI; no trade time) and for cash the quote API (LTP,
lstup_time; no day volume is documented there, so volume stays None).

T-229 (5) "0 contracts" diagnostics (D229; the live shape is unconfirmed, no account here): once per build day (IST)
one INFO `kotak chain diag: expiries ...` and one `kotak chain diag: first chain ...` line give the response SHAPES
(key names, a few named values: never the consumer key, UCC, sid or any token); `chain_counters` keeps the day's
counts (chain calls, raw rows, parsed, identity-dropped by field, skipped for a missing ltp / OI, kept) and a chain
that keeps 0 rows logs a WARNING with them. An empty expiry answer falls back to the canonical master's expiries
(as Angel does, angel/adapter.py A6), with an INFO line.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Iterable, Mapping, Sequence
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
    check_identity,
    norm_underlying,
)
from gammad.broker.dhan import TokenBucket
from gammad.broker.kotak import wire as W
from gammad.config import IST
from gammad.log import get_logger
from gammad.watch.feed_protocol import Quote

log = get_logger(__name__)

KEY_CONSUMER = "KOTAK_CONSUMER_KEY"
KEY_UCC = "KOTAK_UCC"
KEY_TOKEN = "KOTAK_ACCESS_TOKEN"  # the day's session sid (tradeApiValidate data.sid)
KEY_BASE = "KOTAK_BASE_URL"
KEY_FEED_URL = "KOTAK_FEED_URL"
KEY_MOBILE = "KOTAK_MOBILE"

Native = tuple[str, str]  # (Kotak exchange segment "nse_fo" | "nse_cm", pSymbol token or index name)


class KotakAuthError(BrokerAuthError):
    """Kotak rejected the consumer key / session (HTTP 401 / 403, errorCode 401)."""


class KotakAdapter(BrokerAdapter):
    def __init__(
        self,
        consumer_key: str,
        ucc: str,
        session_sid: str,
        base_url: str,
        data_dir: Path,
        *,
        feed_url: str = "",
        http: httpx.Client | None = None,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
        max_retries: int = 3,
    ) -> None:
        self.consumer_key = consumer_key
        self.ucc = ucc
        self._sid = session_sid
        self.base_url = base_url.rstrip("/")
        self.feed_url = feed_url
        self.data_dir = Path(data_dir)
        self._sleep = sleep
        self.max_retries = max_retries
        self._http = http or httpx.Client(base_url=self.base_url, timeout=30.0)
        self._bucket = TokenBucket(W.RATE_PER_S, W.RATE_PER_S, clock, sleep)
        self._canonical: CanonicalMaster | None = None
        self._native = NativeIdMap()
        self._rejected: dict[tuple[str, int], IdentityMismatch] = {}
        self._verified_chains: set[tuple[str, date]] = set()
        self.ltp_batch_size = W.QUOTE_MAX_IDS  # T-159: halved on an HTTP 400, kept for the session
        # T-229 (5) diagnostics (module doc): reset when the IST day changes
        self._today: Callable[[], date] = lambda: datetime.now(IST).date()
        self._diag_day: date | None = None
        self._diag_done: set[str] = set()
        self._counts: dict[str, Any] = {}
        self._last_chain: dict[str, Any] = {}

    # -- credentials ----------------------------------------------------------------------------
    def set_credentials(self, creds: Mapping[str, str] | str, access_token: str | None = None) -> None:
        """Mapping {env key: value} (spec §5; an absent / empty key keeps the current value), or the legacy
        positional (ucc, session sid). A new base URL rebuilds the HTTP client (same buckets and caches)."""
        if isinstance(creds, Mapping):
            self.consumer_key = (creds.get(KEY_CONSUMER) or "").strip() or self.consumer_key
            self.ucc = (creds.get(KEY_UCC) or "").strip() or self.ucc
            self._sid = (creds.get(KEY_TOKEN) or "").strip() or self._sid
            self.feed_url = (creds.get(KEY_FEED_URL) or "").strip() or self.feed_url
            base = (creds.get(KEY_BASE) or "").strip().rstrip("/")
            if base and base != self.base_url:
                transport = getattr(self._http, "_transport", None)
                self.base_url = base
                self._http = httpx.Client(base_url=base, timeout=30.0, transport=transport)
        else:
            self.ucc = creds or self.ucc
            if access_token:
                self._sid = access_token

    # -- transport ------------------------------------------------------------------------------
    def _headers(self) -> dict[str, str]:
        return {"Authorization": self.consumer_key, "Content-Type": "application/json", "Accept": "application/json"}

    def _get(self, path: str, params: Mapping[str, str] | None = None) -> Any:
        attempt = 0
        while True:
            self._bucket.acquire()
            try:
                resp = self._http.get(path, params=params, headers=self._headers())
            except httpx.TransportError as exc:
                if attempt < self.max_retries:
                    attempt += 1
                    self._sleep(min(2.0**attempt, 10.0))
                    continue
                raise BrokerError(f"{path}: transport error {type(exc).__name__}") from exc
            try:
                parsed: Any = resp.json()
            except ValueError:
                parsed = None
            code, msg = W.error_text(parsed, resp.text)
            low = msg.lower()
            if resp.status_code in (401, 403) or code in ("401", "403") or "unauthori" in low or "invalid token" in low:
                raise KotakAuthError(f"{path}: auth rejected ({code or resp.status_code}): {msg}", code,
                                     resp.status_code)
            if 200 <= resp.status_code < 300 and parsed is not None and not W.is_error(parsed):
                return parsed
            if (resp.status_code == 429 or resp.status_code >= 500 or code == "429") and attempt < self.max_retries:
                attempt += 1
                self._sleep(min(2.0**attempt, 10.0))
                continue
            raise BrokerError(f"{path}: HTTP {resp.status_code} {code}: {msg}", code, resp.status_code)

    # -- masters / identity ---------------------------------------------------------------------
    def canonical(self) -> CanonicalMaster:
        if self._canonical is None:
            self._canonical = CanonicalMaster.load(self.data_dir)
        return self._canonical

    def scrip_master(self, as_of: date | None = None) -> Any:
        """The canonical (Dhan public) ScripMaster -- the jobs duck-type this (spec §2)."""
        if as_of is not None and self._canonical is None:
            self._canonical = CanonicalMaster.load(self.data_dir, as_of)
        return self.canonical().sm

    def instruments(self) -> list[Instrument]:
        return self.scrip_master().instruments()

    def native_ids(self) -> NativeIdMap:
        return self._native

    @property
    def identity_dropped(self) -> list[IdentityMismatch]:
        return list(self._rejected.values())

    # -- T-229 (5) diagnostics ------------------------------------------------------------------
    def _diag_roll(self) -> None:
        """A new IST day = a new build day: the shapes are logged again and the counters restart."""
        today = self._today()
        if today != self._diag_day:
            self._diag_day = today
            self._diag_done = set()
            self._counts = {"chain_calls": 0, "raw_rows": 0, "parsed": 0, "identity_dropped": {},
                            "skipped_no_ltp_oi": 0, "kept": 0}

    @property
    def chain_counters(self) -> dict[str, Any]:
        """Today's chain counters (module doc); a copy."""
        self._diag_roll()
        return {k: dict(v) if isinstance(v, dict) else v for k, v in self._counts.items()}

    @staticmethod
    def _drop_field(reason: str) -> str:
        for prefix, field in (("underlying", "underlying"), ("expiry", "expiry"), ("strike", "strike"),
                              ("option type", "option_type"), ("not in the canonical master", "not_in_master"),
                              ("no native row", "no_native_row")):
            if reason.startswith(prefix):
                return field
        return "other"

    def _guard(self, entries: Iterable[W.ChainEntry]) -> list[W.ChainEntry]:
        """The runtime identity guard over option-chain rows (module doc)."""
        master = self.canonical()
        kept: list[W.ChainEntry] = []
        self._diag_roll()
        drops: dict[str, int] = self._counts["identity_dropped"]
        last_drops: dict[str, int] = self._last_chain.setdefault("identity_dropped", {})
        for e in entries:
            key = (SEG_OPT, e.token)
            bad = check_identity(master, SEG_OPT, e.token, ContractKey(e.underlying, e.expiry, e.strike, e.option_type))
            if bad is not None:
                if key not in self._rejected:
                    log.error("identity guard (kotak): %s %d dropped: %s", SEG_OPT, e.token, bad.reason)
                self._rejected[key] = bad
                field = self._drop_field(bad.reason)
                drops[field] = drops.get(field, 0) + 1
                last_drops[field] = last_drops.get(field, 0) + 1
                continue
            if key not in self._native:
                try:
                    self._native.add(SEG_OPT, e.token, ("nse_fo", str(e.token)))
                except ValueError as exc:
                    log.error("identity guard (kotak): %s", exc)
                    drops["native_map"] = drops.get("native_map", 0) + 1
                    continue
            kept.append(e)
        return kept

    def _chain_entries(self, underlying: str, expiry: date) -> list[W.ChainEntry]:
        params = {"exchange": "nse_fo", "underlying": underlying, "expiry": expiry.isoformat()}
        payload = self._get(W.EP_CHAIN, params)
        self._diag_roll()
        parsed = W.parse_chain(payload, underlying, expiry)
        raw = W.chain_raw_rows(payload)
        self._last_chain = {"raw_rows": raw, "parsed": len(parsed), "identity_dropped": {}}
        self._counts["chain_calls"] += 1
        self._counts["raw_rows"] += raw
        self._counts["parsed"] += len(parsed)
        if "chain" not in self._diag_done:
            self._diag_done.add("chain")
            log.info("kotak chain diag: first chain today (%s %s): shape %s; raw rows %d, parsed %d",
                     underlying, expiry.isoformat(), W.chain_shape(payload), raw, len(parsed))
        entries = self._guard(parsed)
        self._verified_chains.add((norm_underlying(underlying), expiry))
        return entries

    def native(self, segment: str, security_id: int) -> Native:
        """Canonical -> (Kotak segment, token or index name). KeyError when no identity-checked row exists.
        May call the option chain once per (underlying, expiry) to verify an F&O token."""
        sid = int(security_id)
        master = self.canonical()
        inst = master.instrument(segment, sid)
        if inst is None:
            raise KeyError((segment, sid))
        if segment == SEG_EQ:
            return ("nse_cm", str(sid))
        if segment == SEG_IDX:
            name = W.INDEX_NAMES.get(norm_underlying(inst.underlying_symbol or inst.symbol))
            if name is None:
                raise KeyError((segment, sid))
            return ("nse_cm", name)
        if segment != SEG_OPT:
            raise KeyError((segment, sid))
        if (segment, sid) not in self._native and (segment, sid) not in self._rejected:
            und = norm_underlying(inst.underlying_symbol)
            if inst.expiry_date is not None and (und, inst.expiry_date) not in self._verified_chains:
                self._chain_entries(und, inst.expiry_date)
        return self._native.native(segment, sid)  # type: ignore[return-value]

    def _underlying(self, security_id: int, segment: str) -> str:
        sym = self.canonical().underlying_symbol(segment, int(security_id))
        if not sym:
            raise BrokerError(f"kotak: {segment} {security_id} is not in the canonical master")
        return norm_underlying(sym)

    # -- candles --------------------------------------------------------------------------------
    def _candles(self, security_id: int, segment: str, interval: str, max_days: int, d0: date, d1: date) -> list[
        Candle
    ]:
        try:
            exch, token = self.native(segment, security_id)
        except KeyError:
            raise BrokerError(f"kotak: no identity-checked native row for {segment} {security_id}") from None
        out: dict[int, Candle] = {}
        start = d0
        while start <= d1:
            end = min(d1, start + timedelta(days=max_days - 2))  # todate asked one day past (inclusivity unstated)
            params = {
                "neosymbol": f"{exch}|{token}",
                "fromdate": start.isoformat(),
                "todate": (end + timedelta(days=1)).isoformat(),
                "interval": interval,
            }
            for c in W.parse_candles(self._get(W.EP_HISTORICAL, params)):
                if d0 <= datetime.fromtimestamp(c.ts_ms / 1000, IST).date() <= d1:
                    out[c.ts_ms] = c
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
        return self._candles(security_id, exchange_segment, W.DAILY_INTERVAL, W.DAILY_MAX_DAYS, from_date, to_date)

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
        del oi, instrument  # candle OI is not populated (KN2); never fabricated
        if interval_min not in W.INTERVALS:
            raise ValueError(f"interval_min must be one of {sorted(W.INTERVALS)}")
        return self._candles(
            security_id, exchange_segment, W.INTERVALS[interval_min], W.MAX_DAYS[interval_min], from_date, to_date
        )

    # -- chain ----------------------------------------------------------------------------------
    def expiries(self, underlying_security_id: int, underlying_seg: str) -> list[date]:
        und = self._underlying(underlying_security_id, underlying_seg)
        payload = self._get(W.EP_EXPIRIES, {"underlying": und, "exchange": "nse_fo"})
        out = W.parse_expiries(payload)
        self._diag_roll()
        if "expiries" not in self._diag_done:
            self._diag_done.add("expiries")
            log.info("kotak chain diag: expiries (%s): shape %s; parsed %d", und, W.expiries_shape(payload), len(out))
        if not out:  # T-229 (5): as Angel does (A6) -- the canonical master knows the listed expiries
            out = self.canonical().expiries(und)
            log.info("kotak: no expiry parsed from Kotak's answer for %s; using the canonical master's %d expiries",
                     und, len(out))
        return out

    def option_chain(self, underlying_security_id: int, underlying_seg: str, expiry: date) -> list[ChainRow]:
        und = self._underlying(underlying_security_id, underlying_seg)
        rows: list[ChainRow] = []
        skipped = 0
        for e in self._chain_entries(und, expiry):
            if e.ltp is None or e.oi is None:
                skipped += 1
                continue  # never a fabricated zero
            rows.append(ChainRow(e.strike, e.option_type, e.token, e.ltp, e.oi, e.volume or 0, None, e.previous_oi,
                                 previous_close=e.previous_close))  # no previous volume: floor value unknown
        rows.sort(key=lambda r: (r.strike, r.option_type))
        self._counts["skipped_no_ltp_oi"] += skipped
        self._counts["kept"] += len(rows)
        if not rows:
            last = self._last_chain
            log.warning(
                "kotak chain diag: %s %s kept 0 contracts (this chain: raw rows %d, parsed %d, identity-dropped %s, "
                "no ltp/OI %d; today: %s)",
                und, expiry.isoformat(), last.get("raw_rows", 0), last.get("parsed", 0),
                last.get("identity_dropped", {}), skipped, self.chain_counters,
            )
        return rows

    # -- quotes ---------------------------------------------------------------------------------
    def _quote_api(self, security_ids: Sequence[int], segment: str, flt: str, *,
                   partial: bool = False) -> dict[int, Mapping[str, Any]]:  # fmt: skip
        """quotes.md rows keyed by CANONICAL id; ids without a native row are absent. `partial` = the shared
        multi-batch ltp contract (batching.py): a failed batch is retried once, then its ids are absent."""
        back: dict[Native, int] = {}
        for sid in dict.fromkeys(int(s) for s in security_ids):
            try:
                back[self.native(segment, sid)] = sid
            except KeyError:
                log.warning("kotak: %s %d has no identity-checked native row; left out", segment, sid)
        out: dict[int, Mapping[str, Any]] = {}

        def fetch(batch: list[Native]) -> None:
            path = W.EP_QUOTES + ",".join(f"{ex}|{tok}" for ex, tok in batch) + f"/{flt}"
            for r in W.quote_rows(self._get(path)):
                key = (str(r.get("exchange") or "").lower(), str(r.get("exchange_token") or "").strip())
                sid = back.get(key)
                if sid is not None:
                    out[sid] = r

        batched(list(back), self.ltp_batch_size, fetch, on_shrink=self._shrink, broker="kotak", partial=partial)
        return out

    def _shrink(self, _old: int, new: int) -> None:
        self.ltp_batch_size = new

    def quotes(self, security_ids: Sequence[int], exchange_segment: str) -> dict[int, Quote]:
        ids = list(dict.fromkeys(int(s) for s in security_ids))
        if exchange_segment != SEG_OPT:
            return {
                sid: Quote(W._f(r.get("ltp")), None, None, W.unix_to_ms(r.get("lstup_time")))
                for sid, r in self._quote_api(ids, exchange_segment, "all").items()
            }
        master = self.canonical()
        groups: dict[tuple[str, date], set[int]] = {}
        for sid in ids:
            inst = master.instrument(SEG_OPT, sid)
            if inst is None or inst.expiry_date is None or inst.option_type not in ("CE", "PE"):
                continue
            groups.setdefault((norm_underlying(inst.underlying_symbol), inst.expiry_date), set()).add(sid)
        out: dict[int, Quote] = {}
        for (und, expiry), want in groups.items():
            for e in self._chain_entries(und, expiry):
                if e.token in want:
                    out[e.token] = Quote(e.ltp, e.volume, e.oi, None)
        return out

    def ltp(self, security_ids: Sequence[int], exchange_segment: str = "NSE_EQ") -> dict[int, float]:
        out: dict[int, float] = {}
        for sid, r in self._quote_api(security_ids, exchange_segment, "ltp", partial=True).items():
            v = W._f(r.get("ltp"))
            if v is not None:
                out[sid] = v
        return out

    # -- health ---------------------------------------------------------------------------------
    def token_health(self) -> TokenHealth:
        if not self._sid:
            return TokenHealth(False, None, f"{KEY_TOKEN} is empty: log in on the Broker login page")
        try:
            self._get(W.EP_FILE_PATHS)
        except BrokerAuthError as exc:
            return health_from_error(exc, str(exc)[:200])
        except BrokerError as exc:
            return health_from_error(exc, f"check failed: {str(exc)[:180]}")
        return TokenHealth(True, None, None)
