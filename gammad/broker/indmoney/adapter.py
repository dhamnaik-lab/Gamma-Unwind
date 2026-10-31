"""Read-only INDstocks adapter; all HTTP sends meet at _send for wave-2 policy adoption."""

import threading
import time
from datetime import date, timedelta

import httpx

from gammad.broker.base import (
    BrokerAdapter,
    BrokerAuthError,
    BrokerEntitlementError,
    BrokerError,
    ChainRow,
    TokenHealth,
    health_from_error,
)
from gammad.broker.batching import batched
from gammad.broker.canonical import CanonicalMaster, ContractKey, NativeIdMap
from gammad.broker.dhan import TokenBucket
from gammad.broker.indmoney import auth, wire
from gammad.config import now_ist
from gammad.core import session_clock
from gammad.watch.feed_protocol import Quote


class IndmoneyAdapter(BrokerAdapter):
    def __init__(self, creds, data_dir, *, transport=None, clock=None, sleep=None):
        self._token = auth.token_from(creds)
        self._data_dir = data_dir
        self._http = httpx.Client(base_url=wire.API_URL, transport=transport, timeout=30)
        self._bucket = TokenBucket(5, 1, clock=clock or time.monotonic, sleep=sleep or time.sleep)
        self._lock = threading.RLock()
        self._day = None
        self._used = 0
        self._map = NativeIdMap()
        self._instruments = None
        self._master = None
        self._batch = wire.QUOTE_BATCH

    def set_credentials(self, creds):
        self._token = auth.token_from(creds)

    def _send(self, path, params=None, *, csv=False):
        # IN7: conservatively share the 5/s + 100000/day budget across ALL read categories.
        # The budget is process-local; restarts/other clients are a wave-2 finding.
        with self._lock:
            today = now_ist().date()
            if self._day != today:
                self._day, self._used = today, 0
            if self._used >= wire.DAILY_QUOTA:
                raise BrokerError("INDmoney daily request budget reached", code="daily_quota")
            self._bucket.acquire()
            self._used += 1
            try:
                response = self._http.get(path, params=params, headers={"Authorization": self._token})
            except httpx.HTTPError as exc:
                raise BrokerError("INDmoney transport failed") from exc
        try:
            payload = response.json()
        except ValueError:
            payload = {}
        if not isinstance(payload, dict):
            payload = {}
        code = wire.auth_code(payload, response.status_code)
        if code == 806:
            raise BrokerEntitlementError(
                "INDmoney market data access rejected", broker="indmoney",
                native=payload.get("error_type") or response.status_code,
                hint=wire.ENTITLEMENT_HINT, status=response.status_code,
            )
        if code is not None:
            raise BrokerAuthError("INDmoney authentication/entitlement rejected", code=str(code),
                                  status=response.status_code)
        if response.is_error or payload.get("success") is False or payload.get("status") == "error":
            raise BrokerError("INDmoney read request failed", status=response.status_code)
        if csv:
            return response.text
        if not payload or not isinstance(payload, dict):
            raise BrokerError("INDmoney malformed response")
        return payload

    def instruments(self):
        with self._lock:
            if self._instruments is not None:
                return list(self._instruments)
            # Local cache only: never let CanonicalMaster.load download from an unrelated host.
            path = self._data_dir / f"scrip-master-{now_ist().date().isoformat()}.csv"
            if not path.is_file():
                raise BrokerError("INDmoney needs today's local canonical master")
            master = CanonicalMaster.from_csv(path)
            canon = {}
            for inst in master.sm.instruments():
                canon[(inst.exchange_segment, ContractKey.from_instrument(inst))] = inst
            mapping, result = NativeIdMap(), []
            for source in ("equity", "fno"):
                text = self._send("/market/instruments", {"source": source}, csv=True)
                for seg, native, key in wire.master_rows(text):
                    if (source == "equity") != (seg == "NSE_EQ"):
                        continue
                    inst = canon.get((seg, key))
                    if inst is None:
                        continue
                    mapping.add(seg, inst.security_id, f"{wire.PREFIX[seg]}_{native}")
                    result.append(inst)
            self._master, self._map, self._instruments = master, mapping, result
            return list(result)

    def native_id(self, segment, security_id):
        self.instruments()
        try:
            return str(self._map.native(segment, security_id))
        except KeyError:
            return None

    def _native(self, segment, security_id):
        native = self.native_id(segment, security_id)
        if native is None:
            raise BrokerError("INDmoney instrument identity unavailable")
        return native

    def feed_id(self, segment, security_id):
        native = self.native_id(segment, security_id)
        return native.replace("_", ":", 1) if native else None

    def _history(self, security_id, segment, start, end, interval, days):
        if start > end:
            raise ValueError("from_date is after to_date")
        native = self._native(segment, security_id)
        result = {}
        bound = end + timedelta(days=1)
        cursor = start
        while cursor < bound:
            stop = min(bound, cursor + timedelta(days=days))
            lo, hi = wire.midnight_ms(cursor), wire.midnight_ms(stop)
            payload = self._send(f"/market/historical/{interval}",
                                 {"scrip-codes": native, "start_time": lo, "end_time": hi})
            for bar in wire.candles(payload, native, lo, hi):
                result[bar.ts_ms] = bar
            cursor = stop
        return [result[k] for k in sorted(result)]

    def daily_candles(self, security_id, exchange_segment, from_date, to_date, instrument="EQUITY"):
        # IN4: leave one day below the limit to tolerate inclusive vendor range counting.
        return self._history(security_id, exchange_segment, from_date, to_date, "1day", 364)

    def intraday(self, security_id, exchange_segment, interval_min, from_date, to_date, oi=True,
                 instrument="OPTSTK"):
        if interval_min not in wire.MINUTES:
            raise ValueError("unsupported INDmoney interval")
        bars = self._history(security_id, exchange_segment, from_date, to_date,
                             f"{interval_min}minute", 6 if interval_min < 60 else 14)
        return [b for b in bars if session_clock.in_fo_session(b.ts_ms)]

    def expiries(self, underlying_security_id, underlying_seg):
        self._native(underlying_seg, underlying_security_id)
        symbol = self._master.underlying_symbol(underlying_seg, underlying_security_id)
        payload = self._send("/market/instruments/expiries", {"underlying": symbol, "segment": "DERIVATIVE"})
        return sorted({date.fromisoformat(d) for d in payload["data"]})

    def option_chain(self, underlying_security_id, underlying_seg, expiry):
        native = self._native(underlying_seg, underlying_security_id)
        payload = self._send("/market/option-chain", {"exchange": "NSE", "segment": "EQUITY",
                             "underlying-scrip": native.split("_")[1], "expiry": expiry.isoformat()})
        data = payload["data"]
        if data["expiry"] != expiry.isoformat():
            raise BrokerError("INDmoney chain expiry mismatch")
        symbol = self._master.underlying_symbol(underlying_seg, underlying_security_id)
        rows = []
        for strike, legs in data["strikes"].items():
            for side in ("ce", "pe"):
                leg = legs.get(side)
                if not leg or leg.get("oi") is None or leg.get("last_price") is None:
                    continue
                try:
                    seg, sid = self._map.canonical(f"NFO_{leg['security_id']}")
                except KeyError:
                    continue
                key = ContractKey(symbol, expiry, float(strike), side.upper())
                if self._master.key(seg, sid).mismatch(key):
                    continue
                rows.append(ChainRow(float(strike), side.upper(), sid, float(leg["last_price"]),
                                     int(leg["oi"]), int(leg.get("volume", 0)), leg.get("iv"),
                                     leg.get("previous_oi"), previous_close=leg.get("previous_close_price")))
        return sorted(rows, key=lambda r: (r.strike, r.option_type))

    def _quotes(self, ids, segment, full):
        natives = {self._native(segment, sid): sid for sid in dict.fromkeys(ids)}

        def call(batch):
            return self._send("/market/quotes/" + ("full" if full else "ltp"),
                              {"scrip-codes": ",".join(batch)})["data"]

        chunks, self._batch = batched(list(natives), self._batch, call, broker="indmoney", partial=not full)
        return {natives[k]: v for chunk in chunks for k, v in chunk.items() if k in natives}

    def ltp(self, security_ids, exchange_segment="NSE_EQ"):
        return {sid: float(q["live_price"]) for sid, q in self._quotes(security_ids, exchange_segment, False).items()
                if q.get("live_price") is not None}

    def quotes(self, security_ids, exchange_segment):
        return {sid: Quote(float(q["live_price"]) if q.get("live_price") is not None else None,
                           None,  # U4: quote volume units are not proved; never prime with guessed quantity
                           int(q["open_interest"]) if exchange_segment == "NSE_FNO"
                           and q.get("open_interest") is not None else None, None)
                for sid, q in self._quotes(security_ids, exchange_segment, True).items()}

    def token_health(self):
        try:
            payload = self._send("/user/profile")
            if payload.get("status") != "success" or not isinstance(payload.get("data"), dict):
                raise BrokerError("INDmoney malformed profile")
            return TokenHealth(True, None, None)
        except BrokerError as exc:
            return health_from_error(exc)

    def close(self):
        self._http.close()
