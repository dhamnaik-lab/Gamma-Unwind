"""Upstox REST adapter over raw httpx (BROKER-ADAPTER-SPEC §7-§8). Read-only: no order method.

Identity (spec §1): canonical (segment, NSE exchange token) <-> Upstox `instrument_key` from Upstox's OWN public
instrument file (NSE.json.gz, cached per IST day in data_dir). Options/futures are `NSE_FO|<token>`; equities
are ISIN-based (`NSE_EQ|INE...`) so they can only come from the file. Building the map IS the runtime identity
guard: an Upstox row at a canonical token must be the same (underlying, expiry, strike, CE/PE) or it is dropped
with an ERROR (`identity_dropped`), never substituted. Chain rows from /option/chain are mapped back through the
same map; a row the map does not know is skipped.
Token: the daily access token OR the 1-year Analytics token (D86, U9) -- both are Bearer tokens on the same GETs.
"""

from __future__ import annotations

import gzip
import json
import threading
import time
from collections.abc import Callable, Mapping, Sequence
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import quote

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
)
from gammad.broker.dhan import TokenBucket
from gammad.broker.upstox import wire
from gammad.config import IST, now_ist
from gammad.core.day_bar import SESSION_OPEN  # 09:15 IST
from gammad.log import get_logger
from gammad.watch.feed_protocol import Quote

log = get_logger(__name__)

KEY_API_KEY = "UPSTOX_API_KEY"
KEY_API_SECRET = "UPSTOX_API_SECRET"
KEY_REDIRECT_URI = "UPSTOX_REDIRECT_URI"
KEY_TOKEN = "UPSTOX_ACCESS_TOKEN"
DUMP_KEEP = 3
HEALTH_KEY = "NSE_INDEX|Nifty 50"


class UpstoxAuthError(BrokerAuthError):
    """Upstox rejected the token (HTTP 401 / UDAPI100050 -> code "809")."""


class UpstoxAdapter(BrokerAdapter):
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
        self._bucket = TokenBucket(wire.RATE_PER_S, wire.RATE_BURST, clock, sleep)
        self._long_bucket = TokenBucket(wire.RATE_LONG_PER_S, wire.RATE_LONG_BURST, clock, sleep)
        self._canon: CanonicalMaster | None = None
        self._map: NativeIdMap | None = None
        self.identity_dropped: list[IdentityMismatch] = []
        self._lock = threading.Lock()

    # ------------------------------------------------------------------ credentials
    def set_credentials(self, creds: Mapping[str, str] | str, access_token: str | None = None) -> None:
        """Mapping {UPSTOX_ACCESS_TOKEN} (spec §5) or a bare token; an empty value keeps the current one."""
        if isinstance(creds, Mapping):
            self._token = (creds.get(KEY_TOKEN) or "").strip() or self._token
        else:
            self._token = (access_token or creds or "").strip() or self._token

    def set_token(self, access_token: str) -> None:
        self._token = access_token

    @property
    def access_token(self) -> str:
        return self._token

    # ------------------------------------------------------------------ transport
    def _headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self._token}", "Accept": "application/json"}

    def _send(self, url: str, params: Any = None, *, auth: bool = True) -> httpx.Response:
        attempt = 0
        while True:
            self._bucket.acquire()
            self._long_bucket.acquire()
            try:
                resp = self._http.get(url, params=params, headers=self._headers() if auth else None)
            except httpx.TransportError as exc:
                if attempt < self.max_retries:
                    attempt += 1
                    self._sleep(min(2.0**attempt, 10.0))
                    continue
                raise BrokerError(f"upstox {url}: transport error {type(exc).__name__}") from exc
            body: Any = None
            if "json" in resp.headers.get("content-type", ""):
                try:
                    body = resp.json()
                except ValueError:
                    body = None
            code, msg = wire.error_info(body)
            auth_code = wire.auth_code_for_error(resp.status_code, code)
            if auth_code is not None:
                raise UpstoxAuthError(
                    f"upstox {url}: token rejected ({code or resp.status_code}): {msg}",
                    auth_code,
                    resp.status_code,
                )
            if 200 <= resp.status_code < 300 and code is None:
                return resp
            if (resp.status_code == 429 or resp.status_code >= 500) and attempt < self.max_retries:
                attempt += 1
                self._sleep(min(2.0**attempt, 10.0))
                continue
            raise BrokerError(
                f"upstox {url}: HTTP {resp.status_code} {code or ''} {msg}".strip(), code, resp.status_code
            )

    def _json(self, url: str, params: Any = None) -> dict[str, Any]:
        resp = self._send(url, params)
        try:
            body = resp.json()
        except ValueError as exc:
            raise BrokerError(f"upstox {url}: non-JSON body", None, resp.status_code) from exc
        if not isinstance(body, dict):
            raise BrokerError(f"upstox {url}: unexpected body", None, resp.status_code)
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

    def _dump_rows(self) -> list[dict[str, Any]]:
        today = now_ist().date().isoformat()
        path = self.data_dir / f"upstox-instruments-NSE-{today}.json"
        if not path.exists():
            raw = self._send(wire.INSTRUMENTS_URL, auth=False).content
            if raw[:2] == b"\x1f\x8b":
                raw = gzip.decompress(raw)
            self.data_dir.mkdir(parents=True, exist_ok=True)
            part = path.with_suffix(".part")
            part.write_bytes(raw)
            part.replace(path)
            for old in sorted(self.data_dir.glob("upstox-instruments-NSE-????-??-??.json"))[:-DUMP_KEEP]:
                old.unlink(missing_ok=True)
        rows = json.loads(path.read_bytes())
        return [r for r in rows if isinstance(r, dict)] if isinstance(rows, list) else []

    def native_map(self) -> NativeIdMap:
        """Canonical <-> instrument_key, built once per adapter from Upstox's file with the identity guard."""
        with self._lock:
            if self._map is not None:
                return self._map
            canon = self.canonical()
            index_ids = {name: canon.sm.index_security_id(sym) for sym, name in wire.INDEX_NAMES.items()}
            m = NativeIdMap()
            dropped: list[IdentityMismatch] = []
            for r in self._dump_rows():
                placed = _place(canon, r, index_ids)
                if placed is None:
                    continue
                seg, sid, native_key = placed
                if canon.instrument(seg, sid) is None:
                    continue
                bad = check_identity(canon, seg, sid, native_key)
                if bad is not None:
                    dropped.append(bad)
                    log.error("identity guard (upstox): %s %d dropped: %s", seg, sid, bad.reason)
                    continue
                try:
                    m.add(seg, sid, str(r["instrument_key"]))
                except ValueError as exc:
                    log.error("identity guard (upstox): %s", exc)
            self._map, self.identity_dropped = m, dropped
            log.info(
                "upstox: native-id map %d contracts, %d dropped by the identity guard", len(m), len(dropped)
            )
            return m

    def native_id(self, segment: str, security_id: int) -> str | None:
        try:
            return str(self.native_map().native(segment, security_id))
        except KeyError:
            return None

    def _key(self, segment: str, security_id: int) -> str:
        key = self.native_id(segment, security_id)
        if key is None:
            raise BrokerError(f"upstox: no identity-matching instrument for {segment} {security_id}")
        return key

    def _canonical_of(self, key: str) -> tuple[str, int] | None:
        try:
            return self.native_map().canonical(key)
        except KeyError:
            return None

    # ------------------------------------------------------------------ BrokerAdapter
    def instruments(self) -> list[Instrument]:
        return self.canonical().sm.instruments()

    def _candles(
        self,
        segment: str,
        security_id: int,
        unit: str,
        interval: int,
        from_date: date,
        to_date: date,
        with_oi: bool,
    ) -> list[Candle]:
        if from_date > to_date:
            return []
        key = quote(self._key(segment, security_id), safe="")
        oi = with_oi and segment == SEG_OPT
        span = (
            wire.DAY_SPAN_DAYS
            if unit == "days"
            else wire.MINUTE_SPAN_DAYS
            if interval <= 15
            else wire.LONG_MINUTE_SPAN_DAYS
        )
        now = now_ist()
        today = now.date()
        hist_to = min(to_date, today - timedelta(days=1))
        bars: dict[int, Candle] = {}
        d0 = from_date
        while d0 <= hist_to:
            d1 = min(hist_to, d0 + timedelta(days=span - 1))
            path = wire.EP_HISTORICAL.format(
                key=key, unit=unit, interval=interval, to=d1.isoformat(), frm=d0.isoformat()
            )
            for c in wire.parse_candles(self._json(path), with_oi=oi):
                bars[c.ts_ms] = c
            d0 = d1 + timedelta(days=1)
        # today's bars live on the intraday endpoint (U3). 0.8.5 D6: a DAILY range through today asked before the
        # 09:15 open (the 09:10 prevclose run: mark_levels asks through trade_date and keeps bars BEFORE it) has no
        # session bar to read, so the intraday call is skipped: 213 requests at 09:10, not 426, and no dependence
        # on what that endpoint answers pre-open (UNKNOWN). From 09:15 on (09:20 ltp run, 16:00 close) unchanged.
        preopen_day = unit == "days" and now.time() < SESSION_OPEN
        if to_date >= today >= from_date and not preopen_day:
            path = wire.EP_INTRADAY.format(key=key, unit=unit, interval=interval)
            for c in wire.parse_candles(self._json(path), with_oi=oi):
                bars[c.ts_ms] = c
        keep = [
            bars[k] for k in sorted(bars) if from_date <= wire.ist_date(k) <= to_date
        ]  # payload is newest-first
        return keep

    def daily_candles(
        self,
        security_id: int,
        exchange_segment: str,
        from_date: date,
        to_date: date,
        instrument: str = "EQUITY",
    ) -> list[Candle]:
        del instrument
        return self._candles(exchange_segment, security_id, "days", 1, from_date, to_date, with_oi=False)

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
        if not 1 <= int(interval_min) <= 300:
            raise BrokerError(f"upstox: unsupported interval {interval_min} min (1..300)")
        return self._candles(
            exchange_segment, security_id, "minutes", int(interval_min), from_date, to_date, oi
        )

    def expiries(self, underlying_security_id: int, underlying_seg: str) -> list[date]:
        body = self._json(
            wire.EP_CONTRACTS, {"instrument_key": self._key(underlying_seg, underlying_security_id)}
        )
        data = body.get("data")
        out = {wire.expiry_date(r.get("expiry")) for r in data or [] if isinstance(r, Mapping)}
        return sorted(d for d in out if d is not None)

    def option_chain(self, underlying_security_id: int, underlying_seg: str, expiry: date) -> list[ChainRow]:
        params = {
            "instrument_key": self._key(underlying_seg, underlying_security_id),
            "expiry_date": expiry.isoformat(),
        }
        body = self._json(wire.EP_CHAIN, params)
        canon = self.canonical()
        out: list[ChainRow] = []
        skipped = 0
        for row in body.get("data") or []:
            if not isinstance(row, Mapping):
                continue
            for side, opt in (("call_options", "CE"), ("put_options", "PE")):
                leg = row.get(side)
                if not isinstance(leg, Mapping):
                    continue
                place = self._canonical_of(str(leg.get("instrument_key") or ""))
                md = leg.get("market_data") if isinstance(leg.get("market_data"), Mapping) else {}
                inst = canon.instrument(*place) if place else None
                if (
                    place is None
                    or inst is None
                    or inst.option_type != opt
                    or inst.expiry_date != expiry
                    or md.get("oi") is None
                    or md.get("ltp") is None
                ):
                    skipped += 1
                    continue
                greeks = leg.get("option_greeks") if isinstance(leg.get("option_greeks"), Mapping) else {}
                prev = md.get("prev_oi")
                out.append(
                    ChainRow(
                        strike=float(inst.strike or row.get("strike_price") or 0.0),
                        option_type=opt,
                        security_id=place[1],
                        ltp=float(md["ltp"]),
                        oi=round(float(md["oi"])),
                        volume=int(md.get("volume") or 0),
                        iv=None if greeks.get("iv") is None else float(greeks["iv"]),
                        previous_oi=None if prev is None else round(float(prev)),
                    )
                )
        if skipped:
            log.info("upstox chain %s: %d legs skipped (unmapped / identity / no LTP or OI)", expiry, skipped)
        out.sort(key=lambda r: (r.strike, r.option_type))
        return out

    def _market_quote(
        self, endpoint: str, security_ids: Sequence[int], segment: str, *, partial: bool = False
    ) -> dict[int, Mapping[str, Any]]:
        """`partial` = the shared multi-batch ltp contract (batching.py): a failed batch is retried once, then its
        ids are absent."""
        keys = [k for k in (self.native_id(segment, s) for s in security_ids) if k is not None]
        out: dict[int, Mapping[str, Any]] = {}

        def fetch(batch: list[str]) -> None:
            body = self._json(endpoint, {"instrument_key": ",".join(batch)})
            for v in wire.quote_values(body):
                place = self._canonical_of(str(v["instrument_token"]))
                if place is not None and place[0] == segment:
                    out[place[1]] = v

        batched(keys, wire.QUOTE_MAX_KEYS, fetch, broker="upstox market quote", partial=partial, halve=False)
        return out

    def ltp(self, security_ids: Sequence[int], exchange_segment: str = "NSE_EQ") -> dict[int, float]:
        got = self._market_quote(wire.EP_LTP, security_ids, exchange_segment, partial=True)
        return {sid: float(v["last_price"]) for sid, v in got.items() if v.get("last_price") is not None}

    def quotes(self, security_ids: Sequence[int], exchange_segment: str) -> dict[int, Quote]:
        """Full market quote (PollFeed fallback / OI priming). Canonical ids in and out; missing ids absent."""
        got = self._market_quote(wire.EP_QUOTES, security_ids, exchange_segment)
        out: dict[int, Quote] = {}
        for sid, v in got.items():
            oi = v.get("oi") if exchange_segment == SEG_OPT else None
            vol = v.get("volume")
            out[sid] = Quote(
                ltp=None if v.get("last_price") is None else float(v["last_price"]),
                volume_day=None if vol is None else int(vol),
                oi=None if oi is None else round(float(oi)),
                ltt_ms=wire.parse_ltt_ms(v.get("last_trade_time")),
            )
        return out

    def token_health(self) -> TokenHealth:
        """One market-data GET (works for the daily AND the Analytics token, U9). The expiry is a rule: 03:30 IST
        next day for a daily token (U8); an Analytics token lasts a year (not distinguishable here)."""
        try:
            self._json(wire.EP_LTP, {"instrument_key": HEALTH_KEY})
        except BrokerError as exc:  # UpstoxAuthError (401 / UDAPI100050 ...) -> auth_failed (W14 H4)
            return health_from_error(exc)
        return TokenHealth(True, next_expiry_ist(now_ist()).isoformat(), None)


def next_expiry_ist(now: datetime) -> datetime:
    """03:30 IST after `now` (U8: a daily token "lasts until 3:30 AM the following day")."""
    t = now.astimezone(IST)
    cut = t.replace(hour=3, minute=30, second=0, microsecond=0)
    return cut if t < cut else cut + timedelta(days=1)


def _place(
    canon: CanonicalMaster, r: Mapping[str, Any], index_ids: Mapping[str, int | None]
) -> tuple[str, int, ContractKey] | None:
    seg = r.get("segment")
    key = str(r.get("instrument_key") or "")
    try:
        if seg == "NSE_EQ" and r.get("instrument_type") == "EQ":
            return SEG_EQ, int(r["exchange_token"]), ContractKey(str(r.get("trading_symbol") or ""))
        if seg == "NSE_INDEX":
            sid = index_ids.get(key.split("|", 1)[-1])
            if sid is None:
                return None
            return SEG_IDX, sid, ContractKey(canon.underlying_symbol(SEG_IDX, sid) or "")
        if seg == "NSE_FO" and r.get("instrument_type") in ("CE", "PE", "FUT"):
            opt = r["instrument_type"] if r["instrument_type"] in ("CE", "PE") else None
            strike = float(r["strike_price"]) if opt and r.get("strike_price") is not None else None
            under = str(r.get("underlying_symbol") or r.get("asset_symbol") or r.get("name") or "")
            return (
                SEG_OPT,
                int(r["exchange_token"]),
                ContractKey(under, wire.expiry_date(r.get("expiry")), strike, opt),
            )
    except (KeyError, TypeError, ValueError):
        return None
    return None
