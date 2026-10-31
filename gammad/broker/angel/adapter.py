"""Angel One SmartAPI REST adapter (BROKER-ADAPTER-SPEC §4, §8). Read-only: there is no order method at all.

Identity (spec §1): every public method speaks CANONICAL (segment, security_id) = NSE exchange token. The
native-id map (`native_ids()`) is built from Angel's own public scrip master: NSE/NFO `token` rows whose
(name, expiry, strike, CE/PE) match the canonical master row at the same token (the runtime identity guard --
a mismatch is dropped with an ERROR, never substituted); indices (IDX_I) map by name to Angel's index tokens.
A canonical id with no native row is ABSENT from quotes/ltp and raises BrokerError for candles.

Capability gaps (caps in `__init__.py`): candles carry no OI (A4) -> warm-up OI is primed from `quotes()`;
no chain / expiry route (A6) -> `option_chain` = canonical master rows + one quote batch
(`canonical.chain_from_master_quotes`), `expiries` = canonical master. Wire facts + time rules: `wire.py`.
"""

from __future__ import annotations

import base64
import json
import random
import socket
import time
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from functools import cache
from pathlib import Path
from typing import Any

import httpx

from gammad.broker import hints
from gammad.broker.angel import wire as W
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
    norm_underlying,
)
from gammad.broker.dhan import MASTER_FALLBACK_MAX_AGE_DAYS, TokenBucket
from gammad.config import IST, now_ist
from gammad.log import get_logger
from gammad.watch.feed_protocol import Quote

log = get_logger(__name__)

KEY_API = "ANGEL_API_KEY"
KEY_CLIENT = "ANGEL_CLIENT_CODE"
KEY_TOKEN = "ANGEL_ACCESS_TOKEN"
KEY_FEED = "ANGEL_FEED_TOKEN"

Native = tuple[str, str]  # (Angel exchange "NSE" | "NFO", Angel token)
SESSION_FRESH_S = 120.0
# 0.8.3 T-229 (3): token_health's one price check (RELIANCE = NSE token 2885, the same in Angel's master)
QUOTE_CHECK_BODY: dict[str, Any] = {"mode": "LTP", "exchangeTokens": {"NSE": ["2885"]}}
FALLBACK_IP = "127.0.0.1"
ZERO_MAC = "00:00:00:00:00:00"
PROBE_ADDR = ("192.0.2.1", 9)  # TEST-NET-1: UDP connect selects a route without sending a packet.


@dataclass(frozen=True)
class MachineIds:
    local_ip: str
    mac: str


def detect_local_ip(sock_factory: Callable[..., Any] = socket.socket) -> str:
    try:
        with sock_factory(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.connect(PROBE_ADDR)
            ip = str(s.getsockname()[0])
    except OSError:
        return FALLBACK_IP
    parts = ip.split(".")
    if len(parts) != 4 or parts[0] == "0" or any(
        not p.isdecimal() or int(p) > 255 for p in parts
    ):
        return FALLBACK_IP
    return ip


def detect_mac(getnode: Callable[[], int] = uuid.getnode) -> str:
    try:
        node = int(getnode()) & 0xFFFF_FFFF_FFFF
    except (OSError, ValueError, TypeError):
        return ZERO_MAC
    return ":".join(f"{(node >> shift) & 0xff:02x}" for shift in range(40, -1, -8))


@cache
def machine_ids() -> MachineIds:
    return MachineIds(detect_local_ip(), detect_mac())


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


class AngelAuthError(BrokerAuthError):
    """Angel rejected the JWT / API key (AG8001-AG8003, HTTP 401/403)."""


def strip_bearer(token: str) -> str:
    t = (token or "").strip()
    return t[7:].strip() if t.lower().startswith("bearer ") else t


def base_headers(api_key: str, *, ids: MachineIds | None = None) -> dict[str, str]:
    """SDK headers: local IP from a no-packet UDP route probe, MAC from uuid.getnode() like the SDK.

    Public IP stays a placeholder because the app never calls a lookup service.
    """
    m = ids if ids is not None else machine_ids()
    return {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "X-UserType": "USER",
        "X-SourceID": "WEB",
        "X-ClientLocalIP": m.local_ip,
        "X-ClientPublicIP": FALLBACK_IP,
        "X-MACAddress": m.mac,
        "X-PrivateKey": api_key,
    }


def jwt_expiry_ist(token: str) -> str | None:
    """The JWT `exp` claim as an IST ISO string, or None (the token is opaque to us otherwise)."""
    parts = strip_bearer(token).split(".")
    if len(parts) != 3:
        return None
    try:
        pad = parts[1] + "=" * (-len(parts[1]) % 4)
        claims = json.loads(base64.urlsafe_b64decode(pad.encode()))
        exp = int(claims["exp"])
    except (ValueError, KeyError, TypeError):
        return None
    return datetime.fromtimestamp(exp, UTC).astimezone(IST).isoformat(timespec="seconds")


class AngelAdapter(BrokerAdapter):
    def __init__(
        self,
        api_key: str,
        client_code: str,
        access_token: str,
        data_dir: Path,
        *,
        feed_token: str = "",
        http: httpx.Client | None = None,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
        max_retries: int = 3,
        rate_retries: int = W.RATE_MAX_RETRIES,
        rng: random.Random | None = None,
    ) -> None:
        self.api_key = api_key
        self.client_code = client_code
        self._token = strip_bearer(access_token)
        self.feed_token = feed_token
        self.data_dir = Path(data_dir)
        self._sleep = sleep
        self._clock = clock
        self._last_ok: float | None = None
        self.max_retries = max_retries  # transport errors + HTTP 5xx
        self.rate_retries = rate_retries  # rate-limit replies (403 "exceeding access rate" / 429), 0.8.3
        self._rng = rng or random.Random()
        self._http = http or httpx.Client(base_url=W.API_BASE_URL, timeout=30.0)
        self._candle_bucket = TokenBucket(W.CANDLE_PACE_PER_S, 1.0, clock, sleep)  # burst 1 (T-150(c), wire.py)
        self._quote_bucket = TokenBucket(W.QUOTE_RATE_PER_S, 1.0, clock, sleep)
        self._misc_bucket = TokenBucket(1.0, 1.0, clock, sleep)
        self._canonical: CanonicalMaster | None = None
        self._native: NativeIdMap | None = None
        self.identity_dropped: list[IdentityMismatch] = []

    # -- credentials ----------------------------------------------------------------------------
    def set_credentials(self, creds: Mapping[str, str] | str, access_token: str | None = None) -> None:
        """Mapping {env key: value} (spec §5; an absent / empty key keeps the current value), or the legacy
        positional (client_code, access_token)."""
        if isinstance(creds, Mapping):
            self.api_key = (creds.get(KEY_API) or "").strip() or self.api_key
            self.client_code = (creds.get(KEY_CLIENT) or "").strip() or self.client_code
            self._token = strip_bearer(creds.get(KEY_TOKEN) or "") or self._token
            self.feed_token = (creds.get(KEY_FEED) or "").strip() or self.feed_token
        else:
            self.client_code = creds or self.client_code
            if access_token:
                self._token = strip_bearer(access_token)

    def set_token(self, access_token: str) -> None:
        self._token = strip_bearer(access_token)

    # -- transport ------------------------------------------------------------------------------
    def _headers(self) -> dict[str, str]:
        return {**base_headers(self.api_key), "Authorization": f"Bearer {self._token}"}

    def _request(self, method: str, path: str, bucket: TokenBucket, body: dict[str, Any] | None, *,
                 once: bool = False) -> Any:  # fmt: skip
        # 0.8.3 (skeptic M1): two independent budgets -- rate-limit replies count against rate_retries, transport errors
        # and 5xx against max_retries -- each backing off from ITS OWN counter. `once` = ONE attempt, both budgets 0
        # (token_health's price check: no 6 x 8 s rate stall, no transport retries)
        max_retries, rate_retries = (0, 0) if once else (self.max_retries, self.rate_retries)
        rate_attempt = err_attempt = 0
        while True:
            bucket.acquire()
            try:
                resp = self._http.request(method, path, json=body, headers=self._headers())
            except httpx.TransportError as exc:
                if err_attempt < max_retries:
                    err_attempt += 1
                    self._sleep(min(2.0**err_attempt, 10.0))
                    continue
                raise BrokerError(f"{path}: transport error {type(exc).__name__}") from exc
            try:
                parsed: Any = resp.json()
            except ValueError:
                parsed = None
            code = W.error_code(parsed)
            msg = W.error_message(parsed, resp.text)
            rate = resp.status_code == 429 or any(w in msg.lower() for w in W.RATE_WORDS)
            if not rate and code in W.ENTITLEMENT_CODES:  # 0.8.3 T-229 (2), D230: ONE request, never retried
                raise BrokerEntitlementError(f"{path}: refused for prices ({code}): {msg}", broker="angel",
                                             native=str(code), hint=hints.ANGEL_HINT, status=resp.status_code)
            if not rate and (
                code in W.AUTH_CODES
                or resp.status_code == 401
                or (resp.status_code == 403 and "token" in msg.lower())
                or "invalid token" in msg.lower()
            ):
                raise AngelAuthError(f"{path}: auth rejected ({code or resp.status_code}): {msg}", code,
                                     resp.status_code)
            if 200 <= resp.status_code < 300 and isinstance(parsed, dict) and not W.is_failure(parsed):
                self._last_ok = self._clock()
                return parsed
            if rate and rate_attempt < rate_retries:
                rate_attempt += 1
                # 0.8.3: the server's Retry-After when it sends one, else FULL jitter under the exponential ceiling
                # (2, 4, 8, 8, ... s) so retries do not re-synchronise; never over RATE_BACKOFF_MAX_S
                after = W.retry_after_s(resp.headers.get("Retry-After"))
                ceiling = min(2.0**rate_attempt, W.RATE_BACKOFF_MAX_S)
                wait = min(after, W.RATE_BACKOFF_MAX_S) if after is not None else self._rng.uniform(0.0, ceiling)
                log.warning("angel %s: rate limited, retry %d/%d in %.1f s", path, rate_attempt, rate_retries,
                            wait)  # fmt: skip
                self._sleep(wait)
                continue
            if not rate and resp.status_code >= 500 and err_attempt < max_retries:
                err_attempt += 1
                wait = min(2.0**err_attempt, W.RATE_BACKOFF_MAX_S)  # 5xx: 2, 4, 8 s (unchanged)
                log.warning("angel %s: HTTP %d, retry %d/%d in %.1f s", path, resp.status_code, err_attempt,
                            max_retries, wait)  # fmt: skip
                self._sleep(wait)
                continue
            raise BrokerError(f"{path}: HTTP {resp.status_code} {code}: {msg}", code, resp.status_code)

    # -- masters --------------------------------------------------------------------------------
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

    def _angel_master_rows(self) -> list[W.MasterRow]:
        """Angel's public master, downloaded at most once per IST day (no auth headers; GET only, A7)."""
        as_of = now_ist().date()
        self.data_dir.mkdir(parents=True, exist_ok=True)
        path = self.data_dir / f"angel-master-{as_of.isoformat()}.json"
        if not path.exists():
            try:
                resp = self._download_master()
            except BrokerError as exc:
                stale = _newest_cached(self.data_dir, "angel-master-", ".json", as_of)
                if stale is None:
                    raise
                log.warning("angel scrip master download failed (%s); using the cached copy from %s",
                            exc, stale[0].isoformat())
                path = stale[1]
            else:
                part = path.with_suffix(".json.part")
                part.write_bytes(resp.content)
                part.replace(path)
                for old in sorted(self.data_dir.glob("angel-master-????-??-??.json"))[:-3]:
                    old.unlink(missing_ok=True)
        try:
            raw = json.loads(path.read_bytes())
        except ValueError as exc:
            path.unlink(missing_ok=True)
            raise BrokerError("angel scrip master: not JSON") from exc
        rows = [W.parse_master_row(r) for r in raw if isinstance(r, dict)] if isinstance(raw, list) else []
        return [r for r in rows if r is not None]

    def _download_master(self) -> httpx.Response:
        """GET the public master: up to W.MASTER_ATTEMPTS tries on a transport error (dropped connection, connect
        failure, timeout) or HTTP 5xx, waiting 2 s, 4 s between (0.8.3); then the same BrokerError as before. A 4xx is
        not retried."""
        for attempt in range(1, W.MASTER_ATTEMPTS + 1):
            last = attempt == W.MASTER_ATTEMPTS
            try:
                resp = self._http.get(W.MASTER_URL, headers={"Accept": "application/json"}, timeout=120.0)
            except httpx.TransportError as exc:
                if last:
                    raise BrokerError(f"angel scrip master: transport error {type(exc).__name__}") from exc
                why = type(exc).__name__
            else:
                if resp.status_code == 200:
                    return resp
                if resp.status_code < 500 or last:
                    raise BrokerError(f"angel scrip master: HTTP {resp.status_code}", None, resp.status_code)
                why = f"HTTP {resp.status_code}"
            wait = 2.0**attempt
            log.warning("angel scrip master: %s, try %d/%d, retrying in %.0f s", why, attempt, W.MASTER_ATTEMPTS, wait)
            self._sleep(wait)
        raise AssertionError("unreachable")  # pragma: no cover

    def native_ids(self) -> NativeIdMap:
        if self._native is None:
            self._native, self.identity_dropped = build_native_map(self.canonical(), self._angel_master_rows())
        return self._native

    def native(self, segment: str, security_id: int) -> Native:
        """Canonical -> (Angel exchange, token). KeyError when Angel has no identity-checked row for it."""
        return self.native_ids().native(segment, int(security_id))  # type: ignore[return-value]

    # -- candles --------------------------------------------------------------------------------
    def _candles(self, security_id: int, segment: str, interval: str, max_days: int, d0: date, d1: date) -> list[
        Candle
    ]:
        try:
            exch, token = self.native(segment, security_id)
        except KeyError:
            raise BrokerError(f"angel: no identity-checked native row for {segment} {security_id}") from None
        out: dict[int, Candle] = {}
        start = d0
        while start <= d1:
            end = min(d1, start + timedelta(days=max_days - 1))
            body = W.candle_body(exch, token, interval, start, end)
            for c in W.parse_candles(self._request("POST", W.EP_CANDLES, self._candle_bucket, body)):
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
        del instrument  # Angel keys candles by exchange + token only
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
        del oi, instrument  # candles carry no OI (A4); never fabricated
        if interval_min not in W.INTERVALS:
            raise ValueError(f"interval_min must be one of {sorted(W.INTERVALS)}")
        return self._candles(
            security_id, exchange_segment, W.INTERVALS[interval_min], W.MAX_DAYS[interval_min], from_date, to_date
        )

    # -- chain ----------------------------------------------------------------------------------
    def _underlying(self, security_id: int, segment: str) -> str:
        sym = self.canonical().underlying_symbol(segment, int(security_id))
        if not sym:
            raise BrokerError(f"angel: {segment} {security_id} is not in the canonical master")
        return sym

    def expiries(self, underlying_security_id: int, underlying_seg: str) -> list[date]:
        """From the canonical master (no expiry route, A6). A session check first (skipped when an authenticated
        call succeeded in the last SESSION_FRESH_S), so an expired token fails here exactly like on a broker with
        an expiry endpoint, without one profile call per underlying during build_watchlists."""
        if self._last_ok is None or self._clock() - self._last_ok > SESSION_FRESH_S:
            self._require_session()
        return self.canonical().expiries(self._underlying(underlying_security_id, underlying_seg))

    def option_chain(self, underlying_security_id: int, underlying_seg: str, expiry: date) -> list[ChainRow]:
        rows = self.canonical().option_rows(self._underlying(underlying_security_id, underlying_seg), expiry)
        quotes = self.quotes([r.security_id for r in rows], SEG_OPT)
        return chain_from_master_quotes(rows, quotes)

    # -- quotes ---------------------------------------------------------------------------------
    def _market(self, mode: str, security_ids: Sequence[int], segment: str, *,
                partial: bool = False) -> dict[int, dict[str, Any]]:  # fmt: skip
        """mode FULL | LTP quote rows keyed by CANONICAL id; ids without a native row are absent. `partial` = the
        shared multi-batch ltp contract (batching.py): a failed batch is retried once, then its ids are absent."""
        back: dict[Native, int] = {}
        for sid in dict.fromkeys(int(s) for s in security_ids):
            try:
                back[self.native(segment, sid)] = sid
            except KeyError:
                log.warning("angel: %s %d has no identity-checked native row; left out", segment, sid)
        out: dict[int, dict[str, Any]] = {}

        def fetch(batch: list[Native]) -> None:
            by_exch: dict[str, list[str]] = {}
            for exch, tok in batch:
                by_exch.setdefault(exch, []).append(tok)
            body = {"mode": mode, "exchangeTokens": by_exch}
            for r in W.fetched_rows(self._request("POST", W.EP_QUOTE, self._quote_bucket, body)):
                key = (str(r.get("exchange") or "").upper(), str(r.get("symbolToken") or "").strip())
                sid = back.get(key)
                if sid is not None:
                    out[sid] = dict(r)

        batched(list(back), W.QUOTE_MAX_TOKENS, fetch, broker=f"angel {mode} quote", partial=partial, halve=False)
        return out

    def quotes(self, security_ids: Sequence[int], exchange_segment: str) -> dict[int, Quote]:
        return {sid: W.parse_quote_row(r) for sid, r in self._market("FULL", security_ids, exchange_segment).items()}

    def ltp(self, security_ids: Sequence[int], exchange_segment: str = "NSE_EQ") -> dict[int, float]:
        out: dict[int, float] = {}
        for sid, r in self._market("LTP", security_ids, exchange_segment, partial=True).items():
            v = r.get("ltp")
            if v not in (None, ""):
                out[sid] = float(v)
        return out

    # -- health ---------------------------------------------------------------------------------
    def _require_session(self) -> Any:
        return self._request("GET", W.EP_PROFILE, self._misc_bucket, None)

    def token_health(self) -> TokenHealth:
        try:
            self._require_session()
        except BrokerAuthError as exc:
            return health_from_error(exc, str(exc)[:200])
        except BrokerError as exc:
            return health_from_error(exc, f"profile check failed: {str(exc)[:180]}")
        prices, problem = self._quote_check()
        return TokenHealth(True, jwt_expiry_ist(self._token), None, problem=problem, prices=prices)

    def _quote_check(self) -> tuple[bool | None, str | None]:
        """0.8.3 T-229 (3): ONE direct LTP quote for RELIANCE by Angel's own NSE token (no master download), ONE
        attempt (`once`). (True, None) = prices flowed; (False, hint) = AG8004; (None, None) = not judged -- any other
        failure never makes the login invalid (WARNING)."""
        try:
            self._request("POST", W.EP_QUOTE, self._quote_bucket, QUOTE_CHECK_BODY, once=True)
        except BrokerEntitlementError as exc:
            return False, exc.hint
        except Exception as exc:  # noqa: BLE001 -- the profile passed; a failed price check is not a bad login
            log.warning("angel token check: the price check could not run (%s)", type(exc).__name__)
            return None, None
        return True, None


def build_native_map(
    master: CanonicalMaster, rows: Sequence[W.MasterRow]
) -> tuple[NativeIdMap, list[IdentityMismatch]]:
    """Canonical (segment, token) -> (Angel exchange, Angel token), identity-checked row by row (spec §1)."""
    m = NativeIdMap()
    dropped: list[IdentityMismatch] = []
    for r in rows:
        if r.exch_seg == "NFO" and r.option_type in ("CE", "PE"):
            seg, sid = SEG_OPT, _int(r.token)
            native_key: ContractKey | None = ContractKey(r.name, r.expiry, r.strike, r.option_type)
        elif r.exch_seg == "NSE" and r.symbol.endswith("-EQ"):
            seg, sid = SEG_EQ, _int(r.token)
            native_key = ContractKey(r.name)
        elif r.exch_seg == "NSE" and r.instrumenttype == "AMXIDX":
            sid = master.sm.index_security_id(r.name) if r.name else None
            if sid is None or master.instrument(SEG_IDX, sid) is None:
                continue
            canon = master.key(SEG_IDX, sid)
            if canon is None or norm_underlying(canon.underlying) != norm_underlying(r.name):
                continue
            _add(m, SEG_IDX, sid, (r.exch_seg, r.token))
            continue
        else:
            continue
        if sid is None or master.instrument(seg, sid) is None:
            continue  # not a contract the app can ever ask for
        bad = check_identity(master, seg, sid, native_key)
        if bad is not None:
            dropped.append(bad)
            log.error("identity guard (angel): %s %d dropped: %s", seg, sid, bad.reason)
            continue
        _add(m, seg, sid, (r.exch_seg, r.token))
    return m, dropped


def _add(m: NativeIdMap, seg: str, sid: int, native: Native) -> None:
    try:
        m.add(seg, sid, native)
    except ValueError as exc:
        log.error("identity guard (angel): %s", exc)


def _int(text: str) -> int | None:
    try:
        return int(text)
    except (TypeError, ValueError):
        return None
