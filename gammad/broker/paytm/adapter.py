"""Read-only Paytm adapter. Unverified history fails explicitly (PM55-U1).

Uses the application's canonical master cache, never invents a Paytm CSV schema.
The documented scripId is the exchange token (PM55-2). Chain identities are
checked against full canonical contracts before accepting an OI row.
"""
from __future__ import annotations

import time
from datetime import datetime

import httpx

from gammad.broker.base import (
    BrokerAdapter,
    BrokerAuthError,
    BrokerEntitlementError,
    BrokerError,
    ChainRow,
    TokenHealth,
)
from gammad.broker.batching import batched
from gammad.broker.canonical import CanonicalMaster, ContractKey, check_identity
from gammad.broker.dhan import TokenBucket
from gammad.broker.paytm import auth, wire
from gammad.config import IST, now_ist
from gammad.watch.feed_protocol import Quote


class PaytmAdapter(BrokerAdapter):
    def __init__(self, creds, data_dir, *, transport=None, clock=None, sleep=None):
        self.set_credentials(creds)
        self._data_dir = data_dir
        self._master = None
        self._http = httpx.Client(base_url=wire.API_BASE, transport=transport, timeout=30)
        self._bucket = TokenBucket(wire.RATE, 1, clock=clock or time.monotonic, sleep=sleep or time.sleep)
        self._batch = wire.BATCH

    def set_credentials(self, creds):
        self._token = auth.paste(creds)

    @property
    def master(self):
        if self._master is None:
            # No implicit other-broker download: use only the app's current cache.
            path = self._data_dir / f"scrip-master-{now_ist().date().isoformat()}.csv"
            if not path.is_file():
                raise BrokerError("Paytm requires today's canonical master cache", code="master_missing")
            self._master = CanonicalMaster.from_csv(path)
        return self._master

    def _send(self, path, **params):
        self._bucket.acquire()
        try:
            response = self._http.get(path, params=params, headers={"x-jwt-token": self._token})
        except httpx.HTTPError as exc:
            raise BrokerError("Paytm transport failed") from exc
        try:
            body = response.json()
        except ValueError:
            body = {}
        if not isinstance(body, dict):
            body = {}
        code = wire.auth_code(response.status_code, body)
        if code == 806:
            raise BrokerEntitlementError("Paytm rejected data access", broker="paytm", native=403,
                                         hint=wire.ENTITLEMENT_HINT, status=response.status_code)
        if code is not None:
            raise BrokerAuthError("Paytm rejected data access", code=str(code), status=response.status_code)
        if response.status_code >= 400 or body.get("error_code"):
            raise BrokerError("Paytm data request failed", status=response.status_code)
        if "data" not in body:
            raise BrokerError("Paytm response has no data")
        return body["data"]

    def instruments(self):
        return self.master.sm.instruments()

    def resolve(self, segment, sid):
        row = self.master.instrument(segment, sid)
        if row is None or segment not in wire.SCRIP:
            return None
        if segment == "NSE_FNO" and row.option_type not in ("CE", "PE"):
            return None
        return int(sid)

    def daily_candles(self, security_id, exchange_segment, from_date, to_date, instrument="EQUITY"):
        raise BrokerError("Paytm history schema is not documented; history disabled", code="unsupported")

    def intraday(self, security_id, exchange_segment, interval_min, from_date, to_date,
                 oi=True, instrument="OPTSTK"):
        raise BrokerError("Paytm history schema is not documented; history disabled", code="unsupported")

    def _symbol(self, sid, segment):
        symbol = self.master.underlying_symbol(segment, sid)
        if not symbol:
            raise BrokerError("Unknown canonical underlying")
        return symbol

    def expiries(self, underlying_security_id, underlying_seg):
        data = self._send(wire.EXPIRIES, symbol=self._symbol(underlying_security_id, underlying_seg))
        return sorted({datetime.fromtimestamp(int(t) / 1000, IST).date() for t in data["expires"]})

    def option_chain(self, underlying_security_id, underlying_seg, expiry):
        symbol = self._symbol(underlying_security_id, underlying_seg)
        out = {}
        for side in ("CE", "PE"):
            data = self._send(wire.CHAIN, type="CALL" if side == "CE" else "PUT",
                              symbol=symbol, expiry=expiry.strftime("%d-%m-%Y"))
            for row in data["results"]:
                if row.get("exchange") != "NSE" or row.get("option_type") != side:
                    continue
                sid = int(row["security_id"])
                key = ContractKey(row["symbol"],
                                  datetime.fromisoformat(row["expiry_date"]).date(),
                                  float(row["stk_price"]), side)
                if key.expiry != expiry or check_identity(self.master, "NSE_FNO", sid, key):
                    continue
                oi = int(float(row["oi"]))
                if oi < 0:
                    continue
                out[sid] = ChainRow(float(row["stk_price"]), side, sid, float(row["price"]), oi,
                                    int(float(row["traded_vol"])),
                                    float(row["iv"]) if row.get("iv") else None, None)
        return sorted(out.values(), key=lambda r: (r.strike, r.option_type))

    def quotes(self, security_ids, exchange_segment="NSE_FNO"):
        ids = list(dict.fromkeys(sid for sid in security_ids if self.resolve(exchange_segment, sid) is not None))

        def call(batch):
            pref = ",".join(f"NSE:{sid}:{wire.SCRIP[exchange_segment]}" for sid in batch)
            data = self._send(wire.LIVE, mode="FULL", pref=pref)
            out = {}
            for row in data:
                sid = int(row["security_id"])
                if sid not in batch or row.get("found") is not True:
                    continue
                out[sid] = Quote(float(row["last_price"]),
                                 int(row["volume_traded"]) if "volume_traded" in row else None,
                                 int(row["oi"]) if exchange_segment == "NSE_FNO" and "oi" in row else None,
                                 wire.epoch_ms(int(row.get("last_trade_time", row.get("last_update_time", 0)))))
            return out

        results, self._batch = batched(ids, self._batch, call, broker="paytm", partial=True)
        return {sid: q for result in results for sid, q in result.items()}

    def ltp(self, security_ids, exchange_segment="NSE_EQ"):
        return {sid: q.ltp for sid, q in self.quotes(security_ids, exchange_segment).items() if q.ltp is not None}

    def token_health(self):
        try:
            data = self._send(wire.PROFILE)
            if not isinstance(data, dict) or "userId" not in data:
                raise BrokerError("Paytm profile shape invalid")
            return TokenHealth(True, None, None)
        except BrokerError as exc:
            return TokenHealth(False, None, str(exc), isinstance(exc, BrokerAuthError) and exc.code != "806")
