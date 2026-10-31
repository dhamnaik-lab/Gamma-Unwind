"""Read-only Noren REST adapter. All sends, including public masters, enter _send.

Identity is a contract join, NOT token equality (U1). Only daily in-memory master
caches are kept. No account store, SDK dependency, or order API is used.
"""

from __future__ import annotations

import threading
import time
from collections import Counter, deque
from datetime import datetime, timedelta
from datetime import time as day_time
from pathlib import Path

import httpx

from gammad.broker.base import BrokerAdapter, BrokerAuthError, BrokerError, TokenHealth, health_from_error
from gammad.broker.batching import batched
from gammad.broker.canonical import CanonicalMaster, ContractKey, NativeIdMap, chain_from_master_quotes
from gammad.broker.dhan import TokenBucket
from gammad.broker.noren import auth, units, wire
from gammad.config import IST, now_ist
from gammad.log import get_logger
from gammad.watch.feed_protocol import Quote

log = get_logger(__name__)


class NorenAdapter(BrokerAdapter):
    def __init__(self, host, creds, data_dir, *, transport=None, clock=None, sleep=None):
        self.host = host
        self._uid, self._token = auth.credentials(host, creds)
        self.data_dir = Path(data_dir)
        self._http = httpx.Client(transport=transport, timeout=30, follow_redirects=False)
        self._bucket = TokenBucket(host.rate_per_s, 1, clock or time.monotonic, sleep or time.sleep)
        self._clock, self._sleep = clock or time.monotonic, sleep or time.sleep
        self._rate_lock = threading.Lock()
        self._request_times = deque()
        self._master_lock = threading.RLock()
        self._canon = None
        self._rows = {}
        self._ids = NativeIdMap()
        self._day = None
        self._rejected_auth = None
        # U4 (D726 fix round F2): ONE switch-off per adapter for the IST day; the feed borrows it (units.py)
        self.volume_guard = units.VolumeUnitGuard(host.broker, self.data_dir)

    def set_credentials(self, creds):
        self._uid, self._token = auth.credentials(self.host, creds)
        self._rejected_auth = None

    def _send(self, endpoint, values=None, *, public=False):
        if not public and self._rejected_auth is not None:
            raise self._rejected_auth
        with self._rate_lock:
            # Flattrade N11: the 40/s limit also has a 200/min ceiling.
            # Reserve each request in a rolling minute, including public GETs.
            if self.host.requests_per_minute is not None:
                while True:
                    now = self._clock()
                    while self._request_times and self._request_times[0] <= now - 60:
                        self._request_times.popleft()
                    if len(self._request_times) < self.host.requests_per_minute:
                        break
                    self._sleep(max(self._request_times[0] + 60 - now, 0.001))
            self._bucket.acquire()
            if self.host.requests_per_minute is not None:
                self._request_times.append(self._clock())
        try:
            if public:
                response = self._http.get(endpoint)
            else:
                response = self._http.post(
                    self.host.rest_base.rstrip("/") + "/" + endpoint,
                    data=wire.form(self._uid, self._token, values or {}),
                )
        except httpx.TransportError as exc:
            raise BrokerError("Noren transport failed") from exc
        try:
            body = response.json()
        except ValueError:
            body = None
        try:
            wire.check_response(body, response.status_code)
        except BrokerAuthError as exc:
            if not public:
                self._rejected_auth = exc
            raise
        if 300 <= response.status_code < 400:
            raise BrokerError("Noren redirect refused", status=response.status_code)
        if public:
            return response.content
        if body is None:
            raise BrokerError("Noren returned non-JSON data")
        return body

    def canonical(self):
        if self._canon is None:
            self._canon = CanonicalMaster.load(self.data_dir)
        return self._canon

    def scrip_master(self):
        return self.canonical().sm

    def _masters(self):
        with self._master_lock:
            today = now_ist().date()
            if self._day == today:
                return
            if self._day is not None:
                self._canon = None
            native_rows = []
            for url in self.host.master_urls:
                native_rows.extend(wire.master_rows(self._send(url, public=True)))
            # Multiple vendor rows for one identity or token are ambiguous; drop all.
            keys = Counter((r.segment, r.key) for r in native_rows)
            tokens = Counter(r.native for r in native_rows)
            lookup = {(r.segment, r.key): r for r in native_rows
                      if keys[(r.segment, r.key)] == 1 and tokens[r.native] == 1}
            ids, rows = NativeIdMap(), {}
            for inst in self.canonical().sm.instruments():
                key = (inst.exchange_segment, ContractKey.from_instrument(inst))
                row = lookup.get(key)
                if row is not None:
                    ids.add(inst.exchange_segment, inst.security_id, row.native)
                    rows[(inst.exchange_segment, inst.security_id)] = row
            self._ids, self._rows, self._day = ids, rows, today

    def resolve(self, segment, security_id):
        self._masters()
        return self._rows.get((segment, int(security_id)))

    def native_id(self, segment, security_id):
        row = self.resolve(segment, security_id)
        return row.native if row is not None else None

    def instruments(self):
        self._masters()
        return [self.canonical().instrument(seg, sid) for seg, sid in self._rows]

    def _require(self, segment, security_id):
        if self._rejected_auth is not None:
            raise self._rejected_auth
        row = self.resolve(segment, security_id)
        if row is None:
            raise BrokerError("Noren contract not in identity-checked master")
        return row

    @staticmethod
    def _bounds(start, end):
        if end < start:
            raise ValueError("end precedes start")
        lo = int(datetime.combine(start, day_time(), IST).timestamp())
        hi = int(datetime.combine(end + timedelta(days=1), day_time(), IST).timestamp()) - 1
        return str(lo), str(hi)

    def daily_candles(self, security_id, exchange_segment, from_date, to_date, instrument="EQUITY"):
        row = self._require(exchange_segment, security_id)
        lo, hi = self._bounds(from_date, to_date)
        body = self._send("EODChartData", {"sym": f"{row.exchange}:{row.trading_symbol}", "from": lo, "to": hi})
        return wire.candles(body, daily=True, oi=False, start=from_date, end=to_date)

    def intraday(self, security_id, exchange_segment, interval_min, from_date, to_date, oi=True,
                 instrument="OPTSTK"):
        if interval_min not in wire.INTERVALS:
            raise ValueError("Unsupported Noren interval")
        row = self._require(exchange_segment, security_id)
        lo, hi = self._bounds(from_date, to_date)
        body = self._send("TPSeries", {"exch": row.exchange, "token": row.token,
                                     "st": lo, "et": hi, "intrv": str(interval_min)})
        return wire.candles(body, daily=False, oi=oi, start=from_date, end=to_date)

    def expiries(self, underlying_security_id, underlying_seg):
        # Public master alone cannot establish session validity (harness auth_rest).
        self._probe()
        self._masters()
        symbol = self.canonical().underlying_symbol(underlying_seg, underlying_security_id)
        return sorted({r.key.expiry for r in self._rows.values()
                       if r.segment == "NSE_FNO" and r.key.underlying == symbol and r.key.expiry is not None})

    def option_chain(self, underlying_security_id, underlying_seg, expiry):
        self._masters()
        symbol = self.canonical().underlying_symbol(underlying_seg, underlying_security_id)
        rows = self.canonical().option_rows(symbol, expiry) if symbol else []
        self._probe()
        quotes = self.quotes([r.security_id for r in rows], "NSE_FNO")
        return chain_from_master_quotes(rows, quotes)

    def quotes(self, security_ids, exchange_segment):
        if self._rejected_auth is not None:
            raise self._rejected_auth

        def call(ids):
            sid = int(ids[0])
            row = self.resolve(exchange_segment, sid)
            if row is None:
                return {}
            body = self._send("GetQuotes", {"exch": row.exchange, "token": row.token})
            if not isinstance(body, dict) or body.get("stat") != "Ok":
                raise BrokerError("Malformed Noren quote")
            # A mismatched echoed identity must not attach a quote to another contract.
            if str(body.get("token")) != row.token or body.get("exch") != row.exchange:
                raise BrokerError("Noren quote identity mismatch")
            price = wire.number(body.get("lp"))
            if price is None:
                return {}
            volume = self._quantity(exchange_segment, sid, body.get("v"))
            level = wire.integer(body.get("oi")) if exchange_segment == "NSE_FNO" else None
            return {sid: Quote(price, volume, level, None)}

        results, _ = batched(list(dict.fromkeys(security_ids)), wire.QUOTE_MAX_IDS, call, partial=True,
                             broker=self.host.broker)
        return {sid: quote for result in results for sid, quote in result.items()}

    def lot_size(self, segment, security_id):
        """TODAY's canonical lot (the lot the scanner's floor uses); the feed's U4 guard reads it (D726). _masters()
        first, so a new IST day reloads the canonical master before the lot is read."""
        self._masters()
        return self.canonical().lot_size(segment, int(security_id))

    def _quantity(self, segment, sid, value):
        """U4 / D726: quote `v` through the adapter's ONE volume_guard (units.py), shared with the feed."""
        if not self.host.volume_verified or value is None or self.volume_guard.off:
            return None
        return self.volume_guard.admit(value, self.lot_size(segment, sid), f"{segment}/{sid}")

    def ltp(self, security_ids, exchange_segment="NSE_EQ"):
        return {sid: q.ltp for sid, q in self.quotes(security_ids, exchange_segment).items() if q.ltp is not None}

    def _probe(self):
        # N6: one read-only price probe, no funds/holdings/account endpoint.
        body = self._send("GetQuotes", {"exch": "NSE", "token": "26000"})
        if not isinstance(body, dict) or body.get("stat") != "Ok" or wire.number(body.get("lp")) is None:
            raise BrokerError("Noren quote probe returned no price")

    def token_health(self):
        try:
            self._probe()
            return TokenHealth(True, None, None, prices=True)
        except BrokerError as exc:
            return health_from_error(exc)
