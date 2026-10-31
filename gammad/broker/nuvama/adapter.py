"""Nuvama BrokerAdapter over raw httpx (BROKER-ADAPTER-SPEC §8; no SDK, §7). Status BETA-UNVERIFIED (D-12).

Identity: canonical (segment, NSE token) -> Nuvama streaming symbol by `wire.native_symbol` ("<token>_NFO",
"<token>_NSE", indices -29/-21), MEASURED 0 mismatches over every NSE option (wire.py docstring), then the RUNTIME
IDENTITY GUARD (spec §1): the first use of each contract per day checks Nuvama's own public instruments.zip row
against the canonical (underlying, expiry, strike, CE/PE); a mismatch drops the contract with one ERROR
(`identity_dropped`), never a substitute. If the master cannot be fetched that day: one WARNING, measured mapping.
Results are keyed back by the CANONICAL id the caller asked for; an id Nuvama has no symbol for, that the guard
dropped, or that returns no quote, is ABSENT (never zero-filled).
Chain: "master+quotes" (Nuvama has no chain call, NV1): canonical master option rows for (underlying, expiry) + one
quote per strike (`canonical.chain_from_master_quotes`). Expiries: the canonical master (no expiry call either).
Candles: charts/v2 (wire P4-P7); no OI in the chart -> Candle.oi None (candle_oi False, warm-up OI from a quote).
Intraday pages backwards on the SPEC's `nextTillDate` until the window start is covered (wire P6, <= 5 pages).
Auth: 401 -> NuvamaAuthError code "807" (DOC "Session Expired"), 403 -> "809". Once Nuvama has rejected the
session the adapter refuses every call (including the master-only expiries()) with NuvamaAuthError until
set_credentials() hands it new credentials -- a data call never answers [] / {} for an expired session.
Budget (wire P10, budget.py): every HTTP call is charged to the data_dir's shared DailyBudget. SLOW: each id is
quoted at most every wire.SLOW_POLL_S (the other cycles see it absent). SHED: option quotes are absent, candles and
chains raise NuvamaBudgetError; underlying quotes + the health probe continue. STOP: every call refused; the health
probe answers from the last observation without a call. `budget_status` is the one-line reason for the dashboard.
"""

from __future__ import annotations

import threading
import time
import zipfile
from collections.abc import Callable, Mapping, Sequence
from datetime import date
from pathlib import Path
from typing import Any, ClassVar

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
from gammad.broker.batching import batched
from gammad.broker.canonical import (
    SEG_EQ,
    SEG_IDX,
    SEG_OPT,
    CanonicalMaster,
    ContractKey,
    IdentityMismatch,
    chain_from_master_quotes,
    check_identity,
)
from gammad.broker.dhan import ScripMaster, TokenBucket, load_scrip_master
from gammad.broker.nuvama import budget as nb
from gammad.broker.nuvama import wire
from gammad.broker.nuvama.auth import (
    KEY_API_KEY,
    KEY_STATIC_IP,
    KEY_TOKEN,
    NuvamaAuthError,
    Session,
    unpack,
)
from gammad.config import now_ist
from gammad.log import get_logger
from gammad.watch.feed_protocol import Quote

log = get_logger(__name__)

HEALTH_SYMBOL = (
    SEG_EQ,
    2885,
)  # RELIANCE: a cheap authenticated quote proves the session (no profile call exists)


class NuvamaAdapter(BrokerAdapter):
    def __init__(
        self,
        api_key: str,
        session: Session,
        data_dir: Path,
        *,
        static_ip: str = "",
        http: httpx.Client | None = None,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
        budget: nb.DailyBudget | None = None,
    ) -> None:
        self.api_key = api_key
        self._session = session
        self._app_id_key = session.app_id_key
        self.static_ip = static_ip
        self.data_dir = Path(data_dir)
        self._http = http or httpx.Client(base_url=wire.HOST, timeout=30.0)
        self._clock = clock
        self._sleep = sleep
        self._bucket = TokenBucket(wire.RATE_PER_S, wire.RATE_PER_S, clock, sleep)
        self._budget = budget if budget is not None else nb.shared_budget(self.data_dir)
        self._last_quote: dict[tuple[str, int], float] = {}  # (segment, id) -> clock() of its last quote call
        self._last_price: dict[tuple[str, int], tuple[float, float]] = {}  # (segment, id) -> (clock(), ltp), D3
        self._last_full: dict[tuple[str, int], tuple[float, Quote]] = {}  # (segment, id) -> (clock(), quote): chains
        self._last_health: TokenHealth | None = None
        self._lock = threading.Lock()
        self._rejected: NuvamaAuthError | None = None
        self._master: tuple[date, CanonicalMaster] | None = None
        self._nv_master: tuple[date, dict[str, ContractKey] | None] | None = None
        self._verified: set[tuple[str, int]] = set()
        self._dropped: dict[tuple[str, int], IdentityMismatch] = {}

    # ---------------------------------------------------------------- credentials
    def set_credentials(self, creds: Mapping[str, str]) -> None:
        """{NUVAMA_API_KEY, NUVAMA_ACCESS_TOKEN, NUVAMA_STATIC_IP}; an absent/empty key keeps the current value.
        Raises NuvamaAuthError for a malformed session (the value is never echoed)."""
        key = (creds.get(KEY_API_KEY) or "").strip() or self.api_key
        token = (creds.get(KEY_TOKEN) or "").strip()
        session = unpack(token) if token else self._session
        with self._lock:
            self.api_key = key
            self._session = session
            self._app_id_key = session.app_id_key or self._app_id_key
            self.static_ip = (creds.get(KEY_STATIC_IP) or "").strip() or self.static_ip
            self._rejected = None

    def _headers(self) -> dict[str, str]:
        h = {
            "Accept": "application/json",
            "Authorization": self._session.auth,
            "Source": self.api_key,
            "SourceToken": self._session.source_token,
        }
        if self._app_id_key:
            h["AppIdKey"] = self._app_id_key
        if self.static_ip:
            h["X-Forwarded-For"] = self.static_ip
        return h

    # ---------------------------------------------------------------- budget (wire P10, budget.py)
    _ALLOWED: ClassVar[dict[str, frozenset[str]]] = {
        nb.OK: frozenset({"option", "underlying", "chart", "health", "master"}),
        nb.LEAN: frozenset({"option", "underlying", "chart", "health", "master"}),  # chains paced: option_chain
        nb.SLOW: frozenset({"option", "underlying", "chart", "health", "master"}),
        nb.SHED: frozenset({"underlying", "health", "master"}),
        nb.STOP: frozenset(),
    }

    @property
    def budget_status(self) -> str | None:
        """None while the daily budget is OK; else the one-line reason (for the dashboard / DEGRADED text)."""
        return self._budget.status()

    def _admit(self, kind: str) -> None:
        """Raise NuvamaBudgetError when today's budget stage does not allow a call of `kind`."""
        if kind not in self._ALLOWED[self._budget.stage()]:
            raise self._budget.refuse(kind)

    def _quote_kind(self, segment: str) -> str:
        return "option" if segment == SEG_OPT else "underlying"

    def _due(self, key: tuple[str, int]) -> bool:
        """SLOW/SHED cadence: an id whose last quote call is younger than SLOW_POLL_S - POLL_S/2 is skipped (so a
        75 s feed quotes it every 2nd cycle, 150 s; the half-cycle slack absorbs cycle jitter)."""
        if self._budget.stage() in (nb.OK, nb.LEAN):  # LEAN slows chains only: the poll keeps its cadence (D4)
            return True
        last = self._last_quote.get(key)
        return last is None or self._clock() - last >= wire.QUOTE_FRESH_S

    # ---------------------------------------------------------------- transport
    def _request(
        self, method: str, path: str, body: dict[str, Any] | None = None, *, kind: str = "chart"
    ) -> Any | None:
        """One call. -> parsed JSON, or None for Nuvama's HTTP 222 "empty / symbol error" (no data).
        Raises NuvamaAuthError (401 -> 807, 403 -> 809; sticky until set_credentials), NuvamaBudgetError (today's
        budget stage refuses `kind`) or BrokerError."""
        if self._rejected is not None:
            raise NuvamaAuthError(str(self._rejected), self._rejected.code, self._rejected.status)
        self._admit(kind)
        for attempt in (1, 2):  # 0.8.5 seam: ONE retry on a connection / timeout error, paced by the same bucket
            self._bucket.acquire()
            self._budget.charge(self._clock, self._sleep)
            try:
                resp = self._http.request(method, path, json=body, headers=self._headers())
                break
            except httpx.TransportError as exc:
                if attempt == 2:
                    raise BrokerError(f"nuvama {method} {path}: transport error {type(exc).__name__}") from exc
                log.info("nuvama %s %s: transport error %s; retrying once", method, path, type(exc).__name__)
        fresh = (resp.headers.get("AppIdKey") or "").strip()
        if fresh:
            self._app_id_key = fresh  # SDK http.py:67-68: the server rotates it on every response
        try:
            payload: Any = resp.json()
        except ValueError:
            payload = None
        status = resp.status_code
        if status in (401, 403):
            code = "807" if status == 401 else "809"
            err = NuvamaAuthError(
                f"nuvama rejected the session (HTTP {status}): {wire.error_text(payload) or 'Session Expired'}",
                code,
                status,
            )
            self._rejected = err
            raise err
        if status == wire.HTTP_EMPTY:
            return None
        if status >= 400:
            raise BrokerError(
                f"nuvama {method} {path}: HTTP {status} {wire.error_text(payload)}".strip(),
                str(status),
                status,
            )
        if payload is None:
            raise BrokerError(f"nuvama {method} {path}: non-JSON body (HTTP {status})", None, status)
        return payload

    # ---------------------------------------------------------------- canonical master
    def scrip_master(self, as_of: date | None = None) -> ScripMaster:
        """The canonical (Dhan public, no auth) master -- the jobs duck-type this (spec §2)."""
        return ScripMaster(load_scrip_master(self.data_dir, as_of))

    def _canonical(self) -> CanonicalMaster:
        today = now_ist().date()
        if self._master is None or self._master[0] != today:
            self._master = (today, CanonicalMaster.load(self.data_dir, today))
        return self._master[1]

    def instruments(self) -> list[Instrument]:
        return self.scrip_master().instruments()

    # ---------------------------------------------------------------- runtime identity guard (spec §1)
    def _nuvama_master(self) -> dict[str, ContractKey] | None:
        """Today's Nuvama instruments.zip (public, no auth headers), parsed; cached per IST day in data_dir.
        None when it cannot be fetched/read today (one WARNING; the measured pure mapping is used unguarded)."""
        today = now_ist().date()
        if self._nv_master is not None and self._nv_master[0] == today:
            return self._nv_master[1]
        path = self.data_dir / f"nuvama-instruments-{today.isoformat()}.zip"
        rows: dict[str, ContractKey] | None = None
        try:
            if not path.exists():
                self._admit("master")  # same host: counted against the daily budget (conservative)
                self._budget.charge(self._clock, self._sleep)
                resp = self._http.get(wire.MASTER_URL, headers={"Accept": "application/zip"})
                if resp.status_code != 200:
                    raise BrokerError(f"HTTP {resp.status_code}")
                self.data_dir.mkdir(parents=True, exist_ok=True)
                tmp = path.with_suffix(".zip.part")
                tmp.write_bytes(resp.content)
                tmp.replace(path)
                for old in sorted(self.data_dir.glob("nuvama-instruments-????-??-??.zip"))[:-2]:
                    old.unlink(missing_ok=True)
            with zipfile.ZipFile(path) as zf:
                rows = wire.parse_master(zf.read(wire.MASTER_MEMBER).decode("utf-8", errors="replace"))
        except (httpx.TransportError, BrokerError, OSError, KeyError, zipfile.BadZipFile) as exc:
            path.unlink(missing_ok=True)
            log.warning(
                "nuvama identity guard unavailable today (instrument master: %s: %s); using the measured "
                "<token>_NFO/_NSE mapping unguarded",
                type(exc).__name__,
                exc,
            )
            rows = None
        self._nv_master = (today, rows)
        return rows

    def _native(self, segment: str, security_id: int) -> str | None:
        """Canonical id -> Nuvama symbol, identity-checked against Nuvama's own master once per contract per day.
        A contract whose Nuvama row is missing or differs from the canonical (underlying, expiry, strike, CE/PE)
        is DROPPED (None) with one ERROR -- never a silent substitute (spec §1)."""
        sid = int(security_id)
        sym = wire.native_symbol(segment, sid)
        if sym is None or segment == SEG_IDX:
            return sym  # indices: exact display-name matches only (wire.INDEX_NATIVE)
        key = (segment, sid)
        if key in self._verified:
            return sym
        if key in self._dropped:
            return None
        rows = self._nuvama_master()
        if rows is None:
            return sym
        bad = check_identity(self._canonical(), segment, sid, rows.get(sym))
        if bad is not None:
            self._dropped[key] = bad
            log.error("identity guard (nuvama): %s %d (%s) dropped: %s", segment, sid, sym, bad.reason)
            return None
        self._verified.add(key)
        return sym

    @property
    def identity_dropped(self) -> list[IdentityMismatch]:
        """Contracts the runtime identity guard dropped today (for the dashboard flag)."""
        return list(self._dropped.values())

    # ---------------------------------------------------------------- quotes / ltp
    def quotes(self, security_ids: Sequence[int], exchange_segment: str) -> dict[int, Quote]:
        """One GET quote/scrip/{symbol}/ per id (no batch endpoint). Missing / unknown / dropped ids are absent;
        so are ids the daily budget skips (SLOW cadence) or refuses (SHED options, STOP) -- never raised here, so
        the poll feed does not log a warning per batch per cycle (budget.py logs each stage change once)."""
        out: dict[int, Quote] = {}
        kind = self._quote_kind(exchange_segment)
        for sid in dict.fromkeys(int(x) for x in security_ids):
            if self._rejected is not None:
                raise NuvamaAuthError(str(self._rejected), self._rejected.code, self._rejected.status)
            if kind not in self._ALLOWED[self._budget.stage()]:
                continue
            key = (exchange_segment, sid)
            if not self._due(key):
                continue
            sym = self._native(exchange_segment, sid)
            if sym is None:
                continue
            q = self._quote_one(key, sym, kind)
            if q is not None:
                out[sid] = q
        return out

    def _quote_one(self, key: tuple[str, int], sym: str, kind: str) -> Quote | None:
        """ONE quote call for `key`; remembers when (the SLOW cadence) and the price it got (ltp's cache, D3)."""
        self._last_quote[key] = self._clock()
        payload = self._request("GET", wire.PATH_QUOTE.format(symbol=sym), kind=kind)
        q = wire.parse_quote(payload, key[0]) if payload is not None else None
        if q is not None and q.ltp is not None:
            self._last_price[key] = (self._clock(), q.ltp)
            self._last_full[key] = (self._clock(), q)  # the chain's recent-quote cache (0.8.5 seam)
        return q

    def ltp(self, security_ids: Sequence[int], exchange_segment: str = "NSE_EQ") -> dict[int, float]:
        """0.8.5 D3 + D5. An id quoted (by the poll feed or an earlier ltp) less than wire.QUOTE_FRESH_S ago is
        answered from that quote with NO call -- the feed's SLOW cadence (`_due`) never makes it missing. Every
        other id gets one quote call through the shared multi-batch contract (batching.py, partial): a failed call
        is retried once, then the id is absent; an auth error raises. A kind the budget stage refuses (SHED options,
        STOP) is absent, never raised. Nothing read at all (every call failed, nothing cached) raises."""
        out: dict[int, float] = {}
        todo: list[tuple[tuple[str, int], str]] = []
        kind = self._quote_kind(exchange_segment)
        now = self._clock()
        for sid in dict.fromkeys(int(x) for x in security_ids):
            if self._rejected is not None:
                raise NuvamaAuthError(str(self._rejected), self._rejected.code, self._rejected.status)
            key = (exchange_segment, sid)
            hit = self._last_price.get(key)
            if hit is not None and now - hit[0] < wire.QUOTE_FRESH_S:
                out[sid] = hit[1]
                continue
            if kind not in self._ALLOWED[self._budget.stage()]:
                continue
            sym = self._native(exchange_segment, sid)
            if sym is not None:
                todo.append((key, sym))

        def fetch(batch: list[tuple[tuple[str, int], str]]) -> None:
            for key, sym in batch:
                try:
                    q = self._quote_one(key, sym, kind)
                except nb.NuvamaBudgetError:
                    return  # the stage changed mid-read: absent, like quotes()
                if q is not None and q.ltp is not None:
                    out[key[1]] = q.ltp

        batched(todo, wire.LTP_BATCH, fetch, broker="nuvama ltp", partial=True, halve=False, raise_if_none=not out)
        return out

    # ---------------------------------------------------------------- candles
    def _chart(
        self, interval: str, segment: str, security_id: int, instrument: str, body: dict[str, Any]
    ) -> Any:
        if self._rejected is not None:
            raise NuvamaAuthError(str(self._rejected), self._rejected.code, self._rejected.status)
        sym = self._native(segment, security_id)
        exc = wire.CHART_EXCHANGE.get(segment)
        if sym is None or exc is None:
            raise BrokerError(f"nuvama: no identity-checked native id for {segment} {security_id}")
        path = wire.PATH_CHART.format(
            interval=interval, exc=exc, atyp=wire.chart_asset_type(segment, instrument), symbol=sym
        )
        return self._request("POST", path, body)

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
        """Pages BACKWARDS on the SPEC's nextTillDate (wire P6): page 1 ends at to_date (Till_Date to_date + 1),
        each next page sends the previous page's nextTillDate verbatim, until a page's oldest row is at or before
        from_date 09:15 IST, the token is absent / not older, or MAX_CHART_PAGES (one WARNING). Bars keyed by ts:
        an overlap between pages keeps the NEWER page's bar."""
        del oi  # Nuvama's chart carries no OI (wire P5): bars always have oi=None
        interval = wire.INTRADAY_INTERVAL.get(int(interval_min))
        if interval is None:
            raise BrokerError(
                f"nuvama: intraday interval {interval_min} min not offered ({sorted(wire.INTRADAY_INTERVAL)})"
            )
        start = wire.window_start(from_date)
        bars: dict[int, Candle] = {}
        till: str | None = None
        till_dt = None
        for _page in range(wire.MAX_CHART_PAGES):
            payload = self._chart(
                interval, exchange_segment, security_id, instrument, wire.intraday_body(to_date, till)
            )
            if payload is None:
                break
            for c in wire.parse_intraday(payload, from_date, to_date):
                bars.setdefault(c.ts_ms, c)
            oldest = wire.oldest_chart_time(payload)
            nxt = wire.next_till_date(payload)
            nxt_dt = wire.parse_ist_text(nxt) if nxt else None
            if oldest is None or oldest <= start or nxt_dt is None:
                break
            if till_dt is not None and nxt_dt >= till_dt:
                break  # the token did not move backwards: no progress possible
            till, till_dt = nxt, nxt_dt
        else:
            log.warning(
                "nuvama intraday %s %s: %d pages did not reach %s; returning the %d bars that arrived",
                exchange_segment,
                security_id,
                wire.MAX_CHART_PAGES,
                start.isoformat(),
                len(bars),
            )
        return [bars[k] for k in sorted(bars)]

    def daily_candles(
        self,
        security_id: int,
        exchange_segment: str,
        from_date: date,
        to_date: date,
        instrument: str = "EQUITY",
    ) -> list[Candle]:
        payload = self._chart(
            wire.DAILY_INTERVAL,
            exchange_segment,
            security_id,
            instrument,
            wire.daily_body(from_date, to_date),
        )
        return [] if payload is None else wire.parse_daily(payload, from_date, to_date)

    # ---------------------------------------------------------------- expiries / chain (master+quotes)
    def _underlying(self, security_id: int, segment: str) -> str:
        sym = self._canonical().underlying_symbol(segment, security_id)
        if not sym:
            raise BrokerError(f"nuvama: {segment} {security_id} is not in the canonical master")
        return sym

    def expiries(self, underlying_security_id: int, underlying_seg: str) -> list[date]:
        if self._rejected is not None:
            raise NuvamaAuthError(str(self._rejected), self._rejected.code, self._rejected.status)
        return self._canonical().expiries(self._underlying(underlying_security_id, underlying_seg))

    def option_chain(self, underlying_security_id: int, underlying_seg: str, expiry: date) -> list[ChainRow]:
        if "option" not in self._ALLOWED[self._budget.stage()]:
            raise self._budget.refuse(f"option chain {underlying_seg} {underlying_security_id} {expiry}")
        if not self._budget.chain_slot(self._clock()):  # 0.8.5 D4 LEAN: the first thing the budget slows
            raise self._budget.refuse(
                f"option chain {underlying_seg} {underlying_security_id} {expiry} (slowed: waits for the next mark)"
            )
        rows = self._canonical().option_rows(self._underlying(underlying_security_id, underlying_seg), expiry)
        out = chain_from_master_quotes(rows, self._chain_quotes([r.security_id for r in rows]))
        if rows and 2 * len(out) < len(rows):  # 0.8.5 seam: a junk chain never lists
            raise BrokerError(f"nuvama option chain {underlying_security_id} {expiry}: only {len(out)} of "
                              f"{len(rows)} legs priced")  # fmt: skip
        return out

    def _chain_quotes(self, security_ids: Sequence[int]) -> dict[int, Quote]:
        """0.8.5 seam: a chain's legs. A leg quoted (by the poll feed or an earlier chain) less than
        wire.QUOTE_FRESH_S ago is answered from that quote with NO call -- the SLOW cadence (`_due`) never makes it
        missing (before: the chain came back short in SLOW). Every other leg gets one quote call through the shared
        multi-batch contract (batching.py, partial): a failed call is retried once, then the leg is absent (its
        strike is missing from the chain; one WARNING for the call). An auth error raises; a budget stage change
        mid-read leaves the rest absent."""
        out: dict[int, Quote] = {}
        todo: list[tuple[tuple[str, int], str]] = []
        now = self._clock()
        for sid in dict.fromkeys(int(x) for x in security_ids):
            if self._rejected is not None:
                raise NuvamaAuthError(str(self._rejected), self._rejected.code, self._rejected.status)
            key = (SEG_OPT, sid)
            hit = self._last_full.get(key)
            if hit is not None and now - hit[0] < wire.QUOTE_FRESH_S:
                out[sid] = hit[1]
                continue
            sym = self._native(SEG_OPT, sid)
            if sym is not None:
                todo.append((key, sym))

        def fetch(batch: list[tuple[tuple[str, int], str]]) -> None:
            for key, sym in batch:
                if key[1] in out:
                    continue  # a retried batch never re-quotes a leg it already priced
                try:
                    q = self._quote_one(key, sym, "option")
                except nb.NuvamaBudgetError:
                    return
                if q is not None:
                    out[key[1]] = q

        batched(todo, wire.LTP_BATCH, fetch, broker="nuvama chain", partial=True, halve=False, raise_if_none=False)
        return out

    # ---------------------------------------------------------------- health
    def token_health(self) -> TokenHealth:
        """Valid iff an authenticated quote answers. Nuvama exposes no expiry time (~12 h, disputed D-12).
        Budget STOP: no call; the answer is the last probe's (or "not rejected today" when none ran) -- a spent
        budget is not an expired session, so it must not raise a TOKEN alert."""
        seg, sid = HEALTH_SYMBOL
        try:
            self._request("GET", wire.PATH_QUOTE.format(symbol=wire.native_symbol(seg, sid)), kind="health")
        except nb.NuvamaBudgetError:
            if self._rejected is not None:
                return health_from_error(self._rejected, str(self._rejected)[:200])
            return self._last_health or TokenHealth(True, None, None)
        except NuvamaAuthError as exc:  # 401 / 403 -> auth_failed (W14 H4: by TYPE, not by its text)
            self._last_health = health_from_error(exc, str(exc)[:200])
            return self._last_health
        except BrokerError as exc:
            self._last_health = health_from_error(exc, f"health probe failed: {exc}"[:200])
            return self._last_health
        self._last_health = TokenHealth(True, None, None)
        return self._last_health
