"""Read-only Breeze REST adapter. All HTTP requests pass through _send (A1/H1/M1)."""

from __future__ import annotations

import json
import re
import threading
import time
from datetime import UTC, datetime, timedelta
from datetime import time as dtime
from pathlib import Path

import httpx

from gammad.broker.base import (
    BrokerAdapter,
    BrokerAuthError,
    BrokerError,
    ChainRow,
    TokenHealth,
    health_from_error,
)
from gammad.broker.batching import batched
from gammad.broker.canonical import CanonicalMaster, ContractKey, NativeIdMap, check_identity
from gammad.broker.icici import wire
from gammad.broker.icici.budget import shared_budget
from gammad.config import IST
from gammad.core import session_clock


def _company(text):
    return re.sub(r"\b(LTD|LIMITED)\b", "", re.sub(r"[^A-Z0-9 ]", " ", text.upper())).split()


class IciciAdapter(BrokerAdapter):
    def __init__(self, creds, data_dir, *, transport=None, clock=time.monotonic, sleep=time.sleep):
        self.data_dir = Path(data_dir)
        self._http = httpx.Client(transport=transport, timeout=30)
        self._creds = dict(creds)
        self.budget = shared_budget(self.data_dir, clock=clock, sleep=sleep)
        self._canon = None
        self._map = None
        self._rows = {}
        self.rejected = []
        self._lock = threading.RLock()

    def set_credentials(self, creds):
        self._creds.update({k: v for k, v in creds.items() if v})

    def _send(self, url, body=None, *, params=None, public=False, kind="health", key=""):
        self.budget.charge(kind, key)
        payload = json.dumps(body or {}, separators=(",", ":"))
        headers = {}
        if not public:
            token = self._creds["ICICI_ACCESS_TOKEN"]
            if url == wire.HISTORY:
                headers = {
                    "Content-Type": "application/json",
                    "apikey": self._creds["ICICI_API_KEY"],
                    "X-SessionToken": token,
                }
            else:
                stamp = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.000Z")
                headers = wire.headers(
                    self._creds["ICICI_API_KEY"], self._creds["ICICI_API_SECRET"], token, payload, stamp
                )
        try:
            response = self._http.request(
                "GET",
                url,
                content=None if public or params is not None else payload,
                params=params,
                headers=headers,
            )
        except httpx.TransportError as exc:
            raise BrokerError("Breeze transport failure") from exc
        if response.status_code in (401, 403):
            raise BrokerAuthError("Breeze session rejected", code="809", status=response.status_code)
        if public:
            if response.status_code != 200:
                raise BrokerError("Breeze master unavailable", status=response.status_code)
            return response.content
        try:
            result = response.json()
        except ValueError as exc:
            raise BrokerError("Breeze returned non-JSON", status=response.status_code) from exc
        code = wire.auth_code(response.status_code, result)
        if code:
            raise BrokerAuthError("Breeze session rejected", code=code, status=response.status_code)
        if not isinstance(result, dict):
            raise BrokerError("Breeze returned invalid envelope")
        if response.status_code != 200 or result.get("Error") or result.get("Status", 200) != 200:
            raise BrokerError("Breeze data request refused", status=response.status_code)
        return result.get("Success")

    def canonical(self):
        if self._canon is None:
            self._canon = CanonicalMaster.load(self.data_dir)
        return self._canon

    def scrip_master(self, as_of=None):
        return self.canonical().sm

    def native_map(self):
        with self._lock:
            if self._map is not None:
                return self._map
            canon = self.canonical()
            rows = wire.master_rows(self._send(wire.MASTER, public=True))
            m = NativeIdMap()
            codes = {}
            # Match the vendor company name to the canonical company name, never guess RELIND -> RELIANCE.
            companies = {}
            for r in canon.sm.df.itertuples(index=False):
                if r.EXCH_ID == "NSE" and r.SEGMENT == "E":
                    companies.setdefault(tuple(_company(str(r.SYMBOL_NAME))), []).append(int(r.SECURITY_ID))
            for seg, row in rows:
                if seg != "NSE_EQ" or len(row) < 5:
                    continue
                candidates = companies.get(tuple(_company(row[3])), [])
                if len(candidates) != 1:
                    self.rejected.append((seg, row[0], "company not uniquely canonical"))
                    continue
                sid = candidates[0]
                inst = canon.instrument(seg, sid)
                if inst is None:
                    continue
                if row[1] in codes and codes[row[1]] != inst.underlying_symbol:
                    raise BrokerError("ambiguous Breeze stock code")
                codes[row[1]] = inst.underlying_symbol or inst.symbol
                m.add(seg, sid, "4.1!" + row[0])
                self._rows[(seg, sid)] = {
                    "stock_code": row[1],
                    "exchange_code": "NSE",
                    "product_type": "cash",
                }
            by_contract = {}
            for inst in canon.sm.instruments():
                if inst.exchange_segment == "NSE_FNO" and inst.option_type in ("CE", "PE"):
                    by_contract.setdefault(ContractKey.from_instrument(inst), []).append(inst.security_id)
            for seg, row in rows:
                if seg != "NSE_FNO":
                    continue
                try:
                    if (
                        len(row) < 7
                        or "OPT" not in row[3].upper()
                        or row[6].upper() not in ("CE", "PE", "C", "P")
                    ):
                        raise ValueError("unsupported contract")
                    symbol = codes[row[2]]
                    side = "CE" if row[6].upper().startswith("C") else "PE"
                    key = ContractKey(symbol, wire.expiry(row[4]), float(row[5]), side)
                    candidates = by_contract.get(key, [])
                    if len(candidates) != 1:
                        raise ValueError("contract not uniquely canonical")
                    sid = candidates[0]
                    if check_identity(canon, seg, sid, key):
                        raise ValueError("contract mismatch")
                    m.add(seg, sid, "4.1!" + row[0])
                    self._rows[(seg, sid)] = {
                        "stock_code": row[2],
                        "exchange_code": "NFO",
                        "product_type": "options",
                        "expiry_date": key.expiry.isoformat() + "T00:00:00.000Z",
                        "strike_price": format(key.strike, "g"),
                        "right": "call" if side == "CE" else "put",
                    }
                except (ValueError, KeyError) as exc:
                    self.rejected.append((seg, row[0], str(exc)))
            self._map = m
            return m

    def native_id(self, segment, security_id):
        try:
            return self.native_map().native(segment, security_id)
        except KeyError:
            return None

    def instruments(self):
        self.native_map()
        return [self.canonical().instrument(seg, sid) for seg, sid in self._rows]

    def _contract(self, sid, seg):
        self.native_map()
        try:
            return dict(self._rows[(seg, sid)])
        except KeyError as exc:
            raise BrokerError("unmapped Breeze contract") from exc

    def _history(self, sid, seg, start, end, interval, oi):
        if end < start:
            return []
        contract = self._contract(sid, seg)
        contract["exch_code"] = contract.pop("exchange_code")
        delta = (
            timedelta(days=1)
            if interval == "1day"
            else timedelta(minutes=int(interval.removesuffix("minute")))
        )
        daily = interval == "1day"
        cursor = datetime.combine(start, dtime.min if daily else dtime(9, 15))
        stop = datetime.combine(end, dtime(23, 59, 59))
        out = {}
        while cursor <= stop:
            # Include the entire last interval: a daily candle may be stamped at 09:15,
            # not midnight. Advancing by a whole day from midnight would otherwise lose it.
            last = min(stop, cursor + delta * 1000 - timedelta(seconds=1))
            if not daily:
                close = datetime.combine(cursor.date(), session_clock.fo_window(cursor.date())[1])
                last = min(last, close - timedelta(seconds=1))
            params = dict(
                contract,
                interval=interval,
                from_date=cursor.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                to_date=last.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            )
            rows = self._send(wire.HISTORY, params=params, kind="history")
            if not isinstance(rows, list) or len(rows) > 1000:
                raise BrokerError("invalid Breeze history page")
            for row in rows:
                bar = wire.candle(row, oi)
                dt = datetime.fromtimestamp(bar.ts_ms / 1000, IST).replace(tzinfo=None)
                if cursor <= dt <= last and (interval == "1day" or session_clock.in_fo_session(bar.ts_ms)):
                    out[bar.ts_ms] = bar
            cursor = (
                last + timedelta(seconds=1)
                if daily
                else datetime.combine(cursor.date() + timedelta(days=1), dtime(9, 15))
            )
        return [out[t] for t in sorted(out)]

    def daily_candles(self, security_id, exchange_segment, from_date, to_date, instrument="EQUITY"):
        return self._history(security_id, exchange_segment, from_date, to_date, "1day", False)

    def intraday(
        self, security_id, exchange_segment, interval_min, from_date, to_date, oi=True, instrument="OPTSTK"
    ):
        if interval_min not in (1, 5, 30):
            raise BrokerError("Breeze supports 1, 5 or 30 minute candles")
        return self._history(security_id, exchange_segment, from_date, to_date, f"{interval_min}minute", oi)

    def _probe(self):
        # Read-only known equity quote, independent of any master mapping; SDK example RELIND (A3).
        return self._send(
            wire.BASE + "quotes", {"stock_code": "RELIND", "exchange_code": "NSE", "product_type": "cash"}
        )

    def token_health(self):
        try:
            rows = self._probe()
            prices = any(r.get("ltp") is not None for r in rows or [])
            return TokenHealth(True, None, None, prices=True if prices else None)
        except BrokerError as exc:
            return health_from_error(exc)

    def expiries(self, underlying_security_id, underlying_seg):
        self._probe()  # fail honestly for expired credentials even though expiries are master-backed
        self.native_map()
        symbol = self.canonical().underlying_symbol(underlying_seg, underlying_security_id)
        return sorted(
            {i.expiry_date for i in self.instruments() if i.option_type and i.underlying_symbol == symbol}
        )

    def option_chain(self, underlying_security_id, underlying_seg, expiry):
        underlying = self._contract(underlying_security_id, underlying_seg)
        out = {}
        options = self.canonical().option_rows(
            self.canonical().underlying_symbol(underlying_seg, underlying_security_id), expiry
        )
        keys = {
            (i.strike, i.option_type): i.security_id
            for i in options
            if ("NSE_FNO", i.security_id) in self._rows
        }
        for side in ("call", "put"):
            body = {
                "stock_code": underlying["stock_code"],
                "exchange_code": "NFO",
                "product_type": "options",
                "expiry_date": expiry.isoformat() + "T00:00:00.000Z",
                "right": side,
            }
            rows = self._send(wire.BASE + "optionchain", body, kind="chain")
            for r in rows or []:
                if (
                    r.get("stock_code") != body["stock_code"]
                    or r.get("exchange_code", "").upper() != "NFO"
                    or wire.expiry(r["expiry_date"]) != expiry
                    or r.get("right", "").lower() != side
                ):
                    continue
                k = (float(r["strike_price"]), "CE" if side == "call" else "PE")
                if k not in keys:
                    continue
                q = wire.quote(r, True)
                if q.ltp is None or q.oi is None:
                    continue
                out[k] = ChainRow(
                    k[0],
                    k[1],
                    keys[k],
                    q.ltp,
                    q.oi,
                    q.volume_day or 0,
                    None,
                    None,
                    previous_close=wire.number(r.get("previous_close")),
                )
        return [out[k] for k in sorted(out)]

    def quotes(self, security_ids, exchange_segment="NSE_EQ"):
        self.native_map()
        ids = list(dict.fromkeys(i for i in security_ids if (exchange_segment, i) in self._rows))

        def call(batch):
            sid = batch[0]
            body = self._contract(sid, exchange_segment)
            rows = self._send(
                wire.BASE + "quotes",
                body,
                kind="option" if exchange_segment == "NSE_FNO" else "quote",
                key=f"{exchange_segment}:{sid}",
            )
            for row in rows or []:
                if (
                    row.get("stock_code") != body["stock_code"]
                    or row.get("exchange_code", "").upper() != body["exchange_code"]
                ):
                    continue
                if exchange_segment == "NSE_FNO" and (
                    wire.expiry(row["expiry_date"]) != wire.expiry(body["expiry_date"])
                    or row.get("right", "").lower() != body["right"]
                    or float(row["strike_price"]) != float(body["strike_price"])
                ):
                    continue
                return {sid: wire.quote(row, exchange_segment == "NSE_FNO")}
            return {}

        chunks, _ = batched(ids, 1, call, broker="icici")
        return {k: v for chunk in chunks for k, v in chunk.items()}

    def ltp(self, security_ids, exchange_segment="NSE_EQ"):
        return {
            sid: q.ltp for sid, q in self.quotes(security_ids, exchange_segment).items() if q.ltp is not None
        }
