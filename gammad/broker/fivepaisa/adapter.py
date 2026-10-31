"""Read-only 5paisa REST adapter. Sources and limitations: fixture README FP54-*.

All sends go through _send. Native IDs are matched by complete contract identity,
not numeric similarity. Quote OI priming uses the documented option-chain OI:
the snapshot page advertises OI but does not specify its response field.
"""

from __future__ import annotations

import threading
import time
from datetime import date, datetime
from pathlib import Path

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
from gammad.broker.canonical import CanonicalMaster, NativeIdMap
from gammad.broker.dhan import TokenBucket
from gammad.broker.fivepaisa import auth, wire
from gammad.config import IST, now_ist
from gammad.log import get_logger
from gammad.watch.feed_protocol import Quote

log = get_logger(__name__)


class FivePaisaAdapter(BrokerAdapter):
    def __init__(self, creds, data_dir, *, http=None, clock=time.monotonic, sleep=time.sleep):
        self._token, self._client, self._app = auth.credentials(creds)
        self.data_dir = Path(data_dir)
        self._http = http or httpx.Client(base_url=wire.API_BASE, timeout=30)
        self._bucket = TokenBucket(wire.RATE_PER_S, 1, clock, sleep)
        self._canon = None
        self._map = None
        self._map_day = None
        self._lock = threading.RLock()
        self.identity_dropped = []
        self._quote_batch = wire.QUOTE_BATCH

    def set_credentials(self, creds):
        values = {auth.KEY_TOKEN: self._token, auth.KEY_CLIENT: self._client, auth.KEY_APP: self._app}
        values.update({k: v for k, v in creds.items() if v})
        self._token, self._client, self._app = auth.credentials(values)

    def _send(self, method, path, *, payload=None, params=None, public=False):
        self._bucket.acquire()
        headers = {} if public else {"Authorization": f"Bearer {self._token}"}
        try:
            resp = self._http.request(method, path, json=payload, params=params, headers=headers)
        except httpx.TransportError as exc:
            raise BrokerError("5paisa transport failure") from exc
        try:
            body = resp.json()
        except ValueError:
            body = None
        code = wire.auth_code(body, resp.status_code)
        if code == 806:
            raise BrokerEntitlementError(
                "5paisa data access refused",
                broker="fivepaisa",
                native=403,
                hint="Check 5paisa data permissions",
                status=resp.status_code,
            )
        if code is not None:
            raise BrokerAuthError("5paisa session rejected; paste a fresh token", str(code), resp.status_code)
        if resp.status_code >= 400:
            raise BrokerError("5paisa HTTP request failed", status=resp.status_code)
        if isinstance(body, dict):
            for section, field in ((body.get("head"), "status"), (body.get("body"), "Status")):
                if isinstance(section, dict) and str(section.get(field, 0)) != "0":
                    raise BrokerError(
                        "5paisa refused data request", str(section.get(field)), resp.status_code
                    )
            if body.get("status") not in (None, "success"):
                raise BrokerError("5paisa historical request failed", status=resp.status_code)
        return resp

    def _post(self, path, body):
        payload = {"head": {"key": self._app}, "body": {"ClientCode": self._client, **body}}
        resp = self._send("POST", path, payload=payload)
        try:
            data = resp.json()["body"]
            if not isinstance(data, dict):
                raise TypeError
            return data
        except (ValueError, KeyError, TypeError) as exc:
            raise BrokerError("5paisa malformed response") from exc

    def canonical(self):
        if self._canon is None:
            self._canon = CanonicalMaster.load(self.data_dir)
        return self._canon

    def scrip_master(self, as_of=None):
        return self.canonical().sm

    def instruments(self):
        return self.canonical().sm.instruments()

    def native_map(self):
        with self._lock:
            today = now_ist().date()
            if self._map is not None and self._map_day == today:
                return self._map
            if self._map_day is not None and self._map_day != today:
                self._canon = None
            path = self.data_dir / f"fivepaisa-master-{today}.csv"
            if path.exists():
                text = path.read_text(encoding="utf-8-sig")
            else:
                text = self._send("GET", wire.MASTER, public=True).text
                if not wire.master_rows(text):
                    raise BrokerError("5paisa master contains no usable NSE rows")
                self.data_dir.mkdir(parents=True, exist_ok=True)
                path.write_text(text, encoding="utf-8")
            canon = self.canonical()
            candidates = {}
            for inst in canon._index().values():
                key = canon.key(inst.exchange_segment, inst.security_id)
                candidates.setdefault((inst.exchange_segment, key), []).append(inst.security_id)
            pairs = []
            self.identity_dropped = []
            for native, seg, key in wire.master_rows(text):
                ids = candidates.get((seg, key), [])
                if len(ids) == 1:
                    pairs.append(((seg, ids[0]), native))
                else:
                    self.identity_dropped.append(native)
            # Ambiguous aliases fail closed on BOTH sides; never keep a first winner.
            by_key, by_native = {}, {}
            for key, native in pairs:
                by_key.setdefault(key, set()).add(native)
                by_native.setdefault(native, set()).add(key)
            mapping = NativeIdMap()
            for key, native in pairs:
                if len(by_key[key]) == 1 and len(by_native[native]) == 1:
                    mapping.add(*key, native)
                else:
                    self.identity_dropped.append(native)
            if self.identity_dropped:
                log.error("5paisa identity guard dropped %d master rows", len(self.identity_dropped))
            self._map, self._map_day = mapping, today
            return mapping

    def native_id(self, segment, security_id):
        try:
            return self.native_map().native(segment, security_id)
        except KeyError:
            return None

    def _native(self, segment, security_id):
        native = self.native_id(segment, security_id)
        if native is None:
            raise BrokerError("5paisa instrument has no identity-checked native mapping")
        return native

    def _history(self, sid, seg, interval, start, end):
        if start > end:
            return []
        if interval != "1d" and (end - start).days > 180:
            raise BrokerError("5paisa intraday range exceeds conservative 180-day limit", "history_range")
        e, s, n = self._native(seg, sid)
        resp = self._send(
            "GET",
            f"/V2/historical/{e}/{s}/{n}/{interval}",
            params={"from": start.isoformat(), "end": end.isoformat()},
        )
        try:
            body = resp.json()
            if not isinstance(body, dict) or not isinstance(body.get("data"), dict):
                raise TypeError
            if not isinstance(body["data"].get("candles"), list):
                raise TypeError
        except (ValueError, TypeError) as exc:
            raise BrokerError("5paisa malformed historical response") from exc
        return [
            c
            for c in wire.candles(body)
            if start <= datetime.fromtimestamp(c.ts_ms / 1000, IST).date() <= end
        ]

    def daily_candles(self, security_id, exchange_segment, from_date, to_date, instrument="EQUITY"):
        return self._history(security_id, exchange_segment, "1d", from_date, to_date)

    def intraday(
        self, security_id, exchange_segment, interval_min, from_date, to_date, oi=True, instrument="OPTSTK"
    ):
        if interval_min not in wire.INTERVALS:
            raise BrokerError("5paisa unsupported candle interval")
        return self._history(security_id, exchange_segment, f"{interval_min}m", from_date, to_date)

    def _symbol(self, sid, seg):
        self._native(seg, sid)
        return self.canonical().underlying_symbol(seg, sid)

    def _expiry_values(self, sid, seg):
        data = self._post(wire.EXPIRIES, {"Exch": "N", "Symbol": self._symbol(sid, seg)})
        return {
            d: r["ExpiryDate"]
            for r in data.get("Expiry", [])
            if isinstance(r, dict) and (d := wire.ist_date(r.get("ExpiryDate"))) is not None
        }

    def expiries(self, underlying_security_id, underlying_seg):
        return sorted(self._expiry_values(underlying_security_id, underlying_seg))

    def option_chain(self, underlying_security_id, underlying_seg, expiry: date):
        symbol = self._symbol(underlying_security_id, underlying_seg)
        values = self._expiry_values(underlying_security_id, underlying_seg)
        if expiry not in values:
            return []
        data = self._post(wire.CHAIN, {"Exch": "N", "Symbol": symbol, "ExpiryDate": values[expiry]})
        out = {}
        for r in data.get("Options", []):
            try:
                native = (r["EXCH"], r["ExchType"], int(r["ScripCode"]))
                seg, sid = self.native_map().canonical(native)
                inst = self.canonical().instrument(seg, sid)
                if (
                    seg != "NSE_FNO"
                    or inst.underlying_symbol != symbol
                    or inst.expiry_date != expiry
                    or inst.option_type != r["CPType"]
                    or inst.strike != float(r["StrikeRate"])
                ):
                    continue
                ltp, oi, vol = (wire.number(r.get(k)) for k in ("LastRate", "OpenInterest", "Volume"))
                if ltp is None or oi is None or vol is None:
                    continue
                # Prev_OI exists in a vendor-repo issue sample, but semantics/correctness are unverified.
                out[sid] = ChainRow(inst.strike, inst.option_type, sid, ltp, int(oi), int(vol), None, None)
            except (KeyError, TypeError, ValueError, AttributeError):
                continue
        return sorted(out.values(), key=lambda r: (r.strike, r.option_type))

    def _snapshots(self, ids, seg, *, partial=False):
        natives = list(dict.fromkeys(n for sid in ids if (n := self.native_id(seg, sid)) is not None))
        out = {}

        def fetch(batch):
            data = self._post(
                wire.SNAPSHOT,
                {
                    "Data": [
                        {"Exchange": e, "ExchangeType": s, "ScripCode": str(n), "ScripData": ""}
                        for e, s, n in batch
                    ]
                },
            )
            result = {}
            for r in data.get("Data", []):
                try:
                    native = (r["Exch"], r["ExchType"], int(r["Token"]))
                    if native not in batch or str(r.get("Status", 0)) != "0":
                        continue
                    mapped_seg, sid = self.native_map().canonical(native)
                    if mapped_seg == seg:
                        result[sid] = r
                except (KeyError, TypeError, ValueError):
                    continue
            return result

        results, self._quote_batch = batched(
            natives, self._quote_batch, fetch, broker="5paisa", partial=partial
        )
        for result in results:
            out.update(result)
        return out

    def ltp(self, security_ids, exchange_segment="NSE_EQ"):
        return {
            sid: v
            for sid, r in self._snapshots(security_ids, exchange_segment, partial=True).items()
            if (v := wire.number(r.get("LastRate"))) is not None
        }

    def quotes(self, security_ids, exchange_segment):
        snapshots = self._snapshots(security_ids, exchange_segment)
        oi_by_id = {}
        if exchange_segment == "NSE_FNO":
            groups = set()
            for sid in snapshots:
                inst = self.canonical().instrument(exchange_segment, sid)
                if inst and inst.option_type in ("CE", "PE"):
                    groups.add((inst.underlying_symbol, inst.expiry_date))
            for symbol, expiry in groups:
                underlying = next(
                    (
                        i
                        for i in self.canonical()._index().values()
                        if i.exchange_segment in ("NSE_EQ", "IDX_I")
                        and self.canonical().underlying_symbol(i.exchange_segment, i.security_id) == symbol
                    ),
                    None,
                )
                if underlying is not None:
                    for row in self.option_chain(underlying.security_id, underlying.exchange_segment, expiry):
                        oi_by_id[row.security_id] = row.oi
        return {
            sid: Quote(
                wire.number(r.get("LastRate")),
                int(v) if (v := wire.number(r.get("TotalQty"))) is not None else None,
                oi_by_id.get(sid),
                None,
            )
            for sid, r in snapshots.items()
        }

    def token_health(self):
        try:
            # One documented market-data request; no master download, no funds/profile dependency.
            data = self._post(
                wire.SNAPSHOT,
                {
                    "Data": [
                        {"Exchange": "N", "ExchangeType": "C", "ScripCode": "0", "ScripData": "RELIANCE_EQ"}
                    ]
                },
            )
            if not data.get("Data"):
                return TokenHealth(False, None, "5paisa health probe returned no data")
        except BrokerError as exc:
            return health_from_error(exc)
        return TokenHealth(True, None, None)
