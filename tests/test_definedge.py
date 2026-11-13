"""Unit tests for Stage 1 Definedge Securities integration and decoupled history routing."""

from __future__ import annotations

import dataclasses
import io
import json
import tempfile
import unittest
import zipfile
from datetime import date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import httpx

from gammad.broker import config_loader, definedge, gamma_r14, gamma_r15, registry
from gammad.broker.base import BrokerAdapter, BrokerError, Candle, ChainRow, Instrument, TokenHealth
from gammad.broker.capabilities import assess
from gammad.broker.definedge import DefinedgeAdapter, DefinedgeAuthError, wire
from gammad.broker.dhan import MASTER_COLUMNS, candle_date
from gammad.config import now_ist, settings
from gammad.jobs import mark_levels
from gammad.store.db import connect as connect_db


def _make_zip_bytes(csv_text: str, filename: str = "allmaster.csv") -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(filename, csv_text)
    return buf.getvalue()


def _write_canonical_csv(data_dir: Path, trade_day: date) -> Path:
    data_dir.mkdir(parents=True, exist_ok=True)
    path = data_dir / f"scrip-master-{trade_day.isoformat()}.csv"
    rows = [
        ",".join(MASTER_COLUMNS),
        # NSE IDX NIFTY = 13
        "NSE,I,13,INDEX,13,NIFTY,NIFTY,Nifty 50,NA,1,,,NA,0.05",
        # NSE IDX BANKNIFTY = 25
        "NSE,I,25,INDEX,25,BANKNIFTY,BANKNIFTY,Nifty Bank,NA,1,,,NA,0.05",
        # NSE EQ RELIANCE = 2885
        "NSE,E,2885,EQUITY,2885,RELIANCE,RELIANCE,Reliance Industries,EQ,1,,,NA,5.0",
        # NSE EQ INFY = 1594
        "NSE,E,1594,EQUITY,1594,INFY,INFY,Infosys Ltd,EQ,1,,,NA,5.0",
        # NSE FNO RELIANCE FUTSTK (needed for F&O universe discovery) = 45820
        "NSE,D,45820,FUTSTK,2885,RELIANCE,RELIANCE,RELIANCE 26NOV FUT,NA,250,2026-11-26,,,5.0",
        # NSE FNO RELIANCE 123.75 CE (fractional strike) = 45821
        "NSE,D,45821,OPTSTK,2885,RELIANCE,RELIANCE,RELIANCE 26NOV 123.75 CE,NA,250,2026-11-26,123.75,CE,5.0",
        # NSE FNO NIFTY 24500 CE = 45822
        "NSE,D,45822,OPTIDX,13,NIFTY,NIFTY,NIFTY 26NOV 24500 CE,NA,75,2026-11-26,24500,CE,5.0",
    ]
    path.write_text("\n".join(rows) + "\n", encoding="utf-8")
    return path


_SAMPLE_ALLMASTER = "\n".join(
    [
        "NSE,26000,Nifty 50,Nifty 50,IDX,,5,1,XX,0,2,1,NA,1,0",
        "NSE,26009,Nifty Bank,Nifty Bank,IDX,,5,1,XX,0,2,1,NA,1,0",
        "NSE,26037,Nifty Fin Service,Nifty Fin Service,IDX,,5,1,XX,0,2,1,NA,1,0",
        "NSE,26074,NIFTY MID SELECT,NIFTY MID SELECT,IDX,,5,1,XX,0,2,1,NA,1,0",
        "NSE,26017,INDIA VIX,INDIA VIX,IDX,,5,1,XX,0,2,1,NA,1,0",
        "NSE,2885,RELIANCE,RELIANCE-EQ,EQ,,5,1,XX,0,2,1,INE002A01018,1,0",
        "NSE,2885,RELIANCE,RELIANCE-BL,EQ,,5,1,XX,0,2,1,INE002A01018,1,0",
        "NSE,1594,INFY,INFY-EQ,EQ,,5,1,XX,0,2,1,INE009A01021,1,0",
        "NFO,45821,RELIANCE,RELIANCE26NOV26C123.75,OPTSTK,26112026,5,250,CE,12375,2,1,,1,0",
        "NFO,45822,NIFTY,NIFTY26NOV26C24500,OPTIDX,26112026,5,75,CE,2450000,2,1,,1,0",
    ]
)


class TestDefinedgeWire(unittest.TestCase):
    def test_allmaster_zip_and_fractional_strike_and_indices(self) -> None:
        zbytes = _make_zip_bytes(_SAMPLE_ALLMASTER)
        text = wire.parse_allmaster_zip(zbytes)
        rows = wire.parse_allmaster_csv(text)
        nm = wire.DefinedgeMaster(rows)

        # Indices mapped cleanly via INDEX_MAP aliases
        self.assertEqual(nm.index_by_name["NIFTY"].token, 26000)
        self.assertEqual(nm.index_by_name["BANKNIFTY"].token, 26009)
        self.assertEqual(nm.index_by_name["FINNIFTY"].token, 26037)
        self.assertEqual(nm.index_by_name["MIDCPNIFTY"].token, 26074)
        self.assertEqual(nm.index_by_name["INDIAVIX"].token, 26017)

        # -EQ preferred over -BL
        rel = nm.by_code[("NSE_EQ", 2885)]
        self.assertEqual(rel.trading_symbol, "RELIANCE-EQ")
        self.assertAlmostEqual(rel.tick_size or 0.0, 0.05)

        # Fractional strike preserved in float (no int truncation)
        opt = nm.by_code[("NSE_FNO", 45821)]
        self.assertIsNotNone(opt.strike)
        self.assertAlmostEqual(opt.strike or 0.0, 123.75)
        self.assertEqual(opt.expiry_date, date(2026, 11, 26))
        self.assertEqual(opt.option_type, "CE")
        self.assertEqual(opt.lot_size, 250)

    def test_wire_hyphenated_eq_float_strike_empty_zip_and_not_ok(self) -> None:
        csv_text = "\n".join(
            [
                "NSE,16669,BAJAJ-AUTO,BAJAJ-AUTO,EQ,,0.05,1,,0,2,1,0,0,INE917I01010",
                "NSE,99999,BAJAJ-AUTO,BAJAJ-AUTO-BE,EQ,,0.05,1,,0,2,1,0,0,INE917I01010",
                "NFO,45822,NIFTY,NIFTY26NOV26C24000,OPTIDX,26112026,0.05,75,CE,24000.0,0,1,0,0,NA",
            ]
        )
        rows = wire.parse_allmaster_csv(csv_text)
        nm = wire.DefinedgeMaster(rows)
        self.assertIn(("NSE_EQ", 16669), nm.by_code)
        self.assertNotIn(("NSE_EQ", 99999), nm.by_code)
        self.assertAlmostEqual(nm.by_code[("NSE_FNO", 45822)].strike or 0.0, 24000.0)

        with self.assertRaises(BrokerError):
            wire.parse_allmaster_zip(_make_zip_bytes("   \n"))
        with self.assertRaises(BrokerError):
            wire.raise_for_http_or_payload(200, '{"stat": "Not_Ok", "emsg": "bad symbol"}')

    def test_sds_csv_zfill_clipping_and_daily_midnight(self) -> None:
        # Daily: 11-char unpadded ddMMyyyyHHmm ("11120260915" -> 2026-11-01 09:15 IST)
        csv_day = "\n".join(
            [
                "311020260915,100,105,99,104,1000",   # before from_date -> clipped
                "11120260915,104,110,103,109,2000",    # 2026-11-01 (zfill 12)
                "021120261530,109,112,108,111,3000",   # 2026-11-02
                "031120260915,111,115,110,114,4000",   # after to_date -> clipped
            ]
        )
        bars = wire.parse_sds_csv(
            csv_day,
            timeframe="day",
            has_oi=False,
            from_date=date(2026, 11, 1),
            to_date=date(2026, 11, 2),
        )
        self.assertEqual(len(bars), 2)
        self.assertEqual(candle_date(bars[0]), date(2026, 11, 1))
        self.assertEqual(candle_date(bars[1]), date(2026, 11, 2))
        self.assertIsNone(bars[0].oi)
        # Normalized to IST midnight
        dt0 = datetime.fromtimestamp(bars[0].ts_ms / 1000.0, wire.IST)
        self.assertEqual((dt0.hour, dt0.minute), (0, 0))

        # 7-col NFO CSV
        csv_nfo = "11120260915,10,12,9,11,500,120000\n"
        nfo_bars = wire.parse_sds_csv(
            csv_nfo,
            timeframe="minute",
            has_oi=True,
            from_date=date(2026, 11, 1),
            to_date=date(2026, 11, 1),
        )
        self.assertEqual(len(nfo_bars), 1)
        self.assertEqual(nfo_bars[0].oi, 120000.0)

    def test_resample_1m_to_5m_15m_60m(self) -> None:
        # 2026-11-02 09:15..10:20 IST (65 bars + 10:15..10:19)
        base = datetime(2026, 11, 2, 9, 15, tzinfo=wire.IST)
        bars_1m: list[Candle] = []
        for i in range(65):
            dt = base + timedelta(minutes=i)
            ts = int(dt.timestamp() * 1000)
            bars_1m.append(Candle(ts, 100.0 + i, 102.0 + i, 99.0 + i, 101.0 + i, 10.0, oi=1000.0 + i))

        b5 = wire.resample_minute_candles(bars_1m, 5)
        self.assertEqual(len(b5), 13)
        self.assertEqual(b5[0].open, 100.0)
        self.assertEqual(b5[0].high, 106.0)
        self.assertEqual(b5[0].low, 99.0)
        self.assertEqual(b5[0].close, 105.0)
        self.assertEqual(b5[0].volume, 50.0)
        self.assertEqual(b5[0].oi, 1004.0)

        b15 = wire.resample_minute_candles(bars_1m, 15)
        self.assertEqual(len(b15), 5)

        b60 = wire.resample_minute_candles(bars_1m, 60)
        # Bucket 1: 09:15..10:14 (60 mins), Bucket 2: 10:15..10:19 (5 mins)
        self.assertEqual(len(b60), 2)
        dt0 = datetime.fromtimestamp(b60[0].ts_ms / 1000.0, wire.IST)
        dt1 = datetime.fromtimestamp(b60[1].ts_ms / 1000.0, wire.IST)
        self.assertEqual((dt0.hour, dt0.minute), (9, 15))
        self.assertEqual((dt1.hour, dt1.minute), (10, 15))
        self.assertEqual(b60[0].volume, 600.0)
        self.assertEqual(b60[1].volume, 50.0)

        with self.assertRaises(BrokerError):
            wire.resample_minute_candles(bars_1m, 3)


class TestDefinedgeAdapterAndWiring(unittest.TestCase):
    def test_adapter_history_intraday_ltp_limits_and_master_cache(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            _write_canonical_csv(data_dir, now_ist().date())
            seen_auth: list[tuple[str, str | None]] = []
            zbytes = _make_zip_bytes(_SAMPLE_ALLMASTER)
            sds_urls: list[str] = []

            def handler(request: httpx.Request) -> httpx.Response:
                url = str(request.url)
                seen_auth.append((url, request.headers.get("authorization")))
                if url == wire.ALLMASTER_URL:
                    return httpx.Response(200, content=zbytes)
                if url.endswith("/dart/v1/limits"):
                    return httpx.Response(200, json={"status": "SUCCESS", "cash": "100000"})
                if url.endswith("/dart/v1/quotes/NSE/2885"):
                    return httpx.Response(200, json={"status": "SUCCESS", "ltp": "2950.5"})
                if url.endswith("/dart/v1/quotes/NSE/26000"):
                    return httpx.Response(200, json={"status": "SUCCESS", "ltp": "24500.25"})
                if "/sds/history/NSE/2885/day/" in url:
                    sds_urls.append(url)
                    return httpx.Response(
                        200,
                        text="020120250915,100,105,99,104,500\n020120260915,200,210,198,208,900\n",
                    )
                if "/sds/history/NFO/45821/minute/" in url:
                    sds_urls.append(url)
                    return httpx.Response(
                        200,
                        text=(
                            "21120260915,10,12,9,11,100,5000\n"
                            "021120260916,11,13,10,12,200,5500\n"
                            "021120260917,12,14,11,13,150,6000\n"
                        ),
                    )
                return httpx.Response(404, json={"status": "ERROR", "message": "not found"})

            t_now = [100.0]
            sleeps: list[float] = []
            wall = datetime(2026, 11, 2, 8, 0, tzinfo=wire.IST)
            ad = DefinedgeAdapter(
                "raw-session-key-123",
                data_dir,
                transport=httpx.MockTransport(handler),
                clock=lambda: t_now[0],
                wall_clock=lambda: wall,
                sleep=lambda s: (sleeps.append(s), t_now.__setitem__(0, t_now[0] + s)),
            )
            try:
                # 1. token_health
                th = ad.token_health()
                self.assertTrue(th.valid)
                self.assertFalse(th.auth_failed)

                # 2. daily_candles multi-year chunking (>365 days -> 2 chunks)
                daily = ad.daily_candles(2885, "NSE_EQ", date(2025, 1, 1), date(2026, 1, 5))
                self.assertEqual(len(daily), 2)
                self.assertEqual(len([u for u in sds_urls if "/day/" in u]), 2)

                # 3. intraday 5m resample with 7-col NFO OI
                bars5 = ad.intraday(45821, "NSE_FNO", 5, date(2026, 11, 2), date(2026, 11, 2), oi=True)
                self.assertEqual(len(bars5), 1)
                self.assertEqual(bars5[0].volume, 450.0)
                self.assertEqual(bars5[0].oi, 6000.0)

                # 4. ltp uses ltp_batch_size=1, skips unknown sid, works for index
                self.assertEqual(ad.ltp_batch_size, 1)
                prices = ad.ltp([2885, 999999], "NSE_EQ")
                self.assertEqual(prices, {2885: 2950.5})
                idx_prices = ad.ltp([13], "IDX_I")
                self.assertEqual(idx_prices, {13: 24500.25})

                # 5. Auth headers: raw token (no Bearer), allmaster.zip has NO Authorization
                for u, hdr in seen_auth:
                    if u == wire.ALLMASTER_URL:
                        self.assertIsNone(hdr)
                    else:
                        self.assertEqual(hdr, "raw-session-key-123")

                # 6. expiries / option_chain unsupported in Stage 1
                with self.assertRaises(BrokerError):
                    ad.expiries(13, "IDX_I")
                with self.assertRaises(BrokerError):
                    ad.option_chain(13, "IDX_I", date(2026, 11, 26))
            finally:
                ad.close()

    def test_auth_429_and_master_cache_fallback_and_pruning(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            _write_canonical_csv(data_dir, now_ist().date())
            # Seed 5 older cached allmaster CSVs -> pruning after download keeps 4 total (current + 3 prior)
            for d_off in range(1, 6):
                p = data_dir / f"definedge-allmaster-{(date(2026, 11, 2) - timedelta(days=d_off)).isoformat()}.csv"
                p.write_text(_SAMPLE_ALLMASTER, encoding="utf-8")

            zbytes = _make_zip_bytes(_SAMPLE_ALLMASTER)
            mode = {"limits": "ok"}

            def handler(request: httpx.Request) -> httpx.Response:
                url = str(request.url)
                if url == wire.ALLMASTER_URL:
                    return httpx.Response(200, content=zbytes)
                if mode["limits"] == "expired":
                    return httpx.Response(200, json={"status": "ERROR", "message": "Session Expired"})
                if mode["limits"] == "401":
                    return httpx.Response(401, json={"status": "ERROR", "message": "Invalid Session"})
                if mode["limits"] == "429":
                    return httpx.Response(429, headers={"Retry-After": "1.5"}, json={"status": "ERROR", "message": "Rate limit"})
                return httpx.Response(500, json={"status": "ERROR", "message": "Internal Error"})

            t_now = [10.0]
            sleeps: list[float] = []
            ad = DefinedgeAdapter(
                "tok",
                data_dir,
                transport=httpx.MockTransport(handler),
                clock=lambda: t_now[0],
                wall_clock=lambda: datetime(2026, 11, 2, 8, 0, tzinfo=wire.IST),
                sleep=lambda s: (sleeps.append(s), t_now.__setitem__(0, t_now[0] + s)),
            )
            try:
                ad.native_master()
                kept = sorted(data_dir.glob("definedge-allmaster-????-??-??.csv"))
                self.assertEqual(len(kept), 4)

                mode["limits"] = "expired"
                th_exp = ad.token_health()
                self.assertFalse(th_exp.valid)
                self.assertTrue(th_exp.auth_failed)

                mode["limits"] = "401"
                th_401 = ad.token_health()
                self.assertFalse(th_401.valid)
                self.assertTrue(th_401.auth_failed)

                mode["limits"] = "429"
                with self.assertRaises(BrokerError) as cm:
                    ad._get_json(wire.limits_url())
                self.assertEqual(cm.exception.status, 429)

                # Next call observes 429 cooldown
                mode["limits"] = "500"
                th_500 = ad.token_health()
                self.assertFalse(th_500.valid)
                self.assertFalse(th_500.auth_failed)
                self.assertTrue(any(s >= 1.5 for s in sleeps))
            finally:
                ad.close()

            # Pre-07:30 IST uses prior-day master date and falls back if download fails
            def fail_handler(request: httpx.Request) -> httpx.Response:
                return httpx.Response(503, text="unavailable")

            ad2 = DefinedgeAdapter(
                "tok",
                data_dir,
                transport=httpx.MockTransport(fail_handler),
                wall_clock=lambda: datetime(2026, 11, 3, 7, 15, tzinfo=wire.IST),
            )
            try:
                nm2 = ad2.native_master()
                self.assertIn(("NSE_EQ", 2885), nm2.by_code)
            finally:
                ad2.close()

    def test_identity_guard_rejects_mismatched_strike(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            _write_canonical_csv(data_dir, now_ist().date())
            bad_csv = _SAMPLE_ALLMASTER.replace("12375", "99999")
            zbytes = _make_zip_bytes(bad_csv)
            ad = DefinedgeAdapter(
                "tok",
                data_dir,
                transport=httpx.MockTransport(lambda req: httpx.Response(200, content=zbytes)),
                wall_clock=lambda: datetime(2026, 11, 2, 8, 0, tzinfo=wire.IST),
            )
            try:
                with self.assertRaises(BrokerError):
                    ad.intraday(45821, "NSE_FNO", 5, date(2026, 11, 2), date(2026, 11, 2))
                self.assertTrue(ad.identity_dropped)
                self.assertIn("strike", ad.identity_dropped[0][2])
            finally:
                ad.close()

    def test_registry_assess_and_mode_a_explicit_history_isolation(self) -> None:
        self.assertEqual(registry.BROKEN, {})
        self.assertIn("definedge", registry.BROKERS)
        self.assertEqual(registry.ENV_PREFIX["definedge"], "DEFINEDGE_")
        entry = registry.get("definedge")
        self.assertEqual(definedge.LOGIN_KEYS, ())
        self.assertEqual(registry.login_keys(entry), ())
        self.assertEqual(registry.first_day_missing(entry, {}), [])
        self.assertEqual(assess(entry.caps).mode, "BLOCKED")
        with self.assertRaises(DefinedgeAuthError):
            entry.make_adapter({}, Path("."))
        with self.assertRaises(BrokerError):
            entry.make_feed({}, Path("."))

        with tempfile.TemporaryDirectory() as tmp:
            cfg = SimpleNamespace(
                data_dir=Path(tmp),
                env=SimpleNamespace(
                    broker="fyers",
                    values={
                        "BROKER": "fyers",
                        "FYERS_APP_ID": "APP123",
                        "FYERS_ACCESS_TOKEN": "tok",
                        "GAMMA_HISTORY_PROVIDER": "definedge",
                    },
                ),
            )
            # Without DEFINEDGE_API_SESSION_KEY, make_adapter raises DefinedgeAuthError and
            # Mode A isolates history_adapter() as UnauthenticatedBrokerAdapter instead of falling back to fyers.
            with self.assertLogs("gammad.broker.config_loader", level="ERROR") as logs_missing:
                mgr_missing = config_loader.load_account_session_manager(cfg)
            self.assertTrue(any("definedge" in m for m in logs_missing.output))
            hist_missing = mgr_missing.history_adapter()
            self.assertIsInstance(hist_missing, config_loader.UnauthenticatedBrokerAdapter)
            self.assertEqual(hist_missing.broker, "definedge")

            orig_get = registry.get
            def _boom_adapter(*_a, **_kw):
                raise RuntimeError("bad definedge setup")
            with patch.object(
                registry,
                "get",
                side_effect=lambda name: dataclasses.replace(orig_get(name), make_adapter=_boom_adapter)
                if name == "definedge"
                else orig_get(name),
            ):
                with self.assertLogs("gammad.broker.config_loader", level="ERROR") as logs:
                    mgr = config_loader.load_account_session_manager(cfg)
            self.assertTrue(any("definedge" in m for m in logs.output))
            hist = mgr.history_adapter()
            self.assertIsInstance(hist, config_loader.UnauthenticatedBrokerAdapter)
            self.assertEqual(hist.broker, "definedge")
            self.assertIn("bad definedge setup", hist.reason)

    def test_mark_levels_splits_history_and_ltp_adapters(self) -> None:
        class _HistFake(BrokerAdapter):
            def __init__(self, data_dir: Path) -> None:
                self._sm_path = _write_canonical_csv(data_dir, now_ist().date())
                from gammad.broker.dhan import ScripMaster, _read_master_csv

                self._sm = ScripMaster(_read_master_csv(self._sm_path))
                self.ltp_calls = 0
                self.daily_calls = 0

            def scrip_master(self, as_of: date | None = None):
                return self._sm

            def instruments(self) -> list[Instrument]:
                return self._sm.instruments()

            def daily_candles(self, security_id: int, exchange_segment: str, from_date: date, to_date: date, instrument: str = "EQUITY") -> list[Candle]:
                self.daily_calls += 1
                base = datetime.combine(from_date, datetime.min.time(), tzinfo=wire.IST)
                return [
                    Candle(int((base + timedelta(days=i)).timestamp() * 1000), 2930.0 + i, 2945.0 + i, 2925.0 + i, 2935.0 + i, 1000.0)
                    for i in range(25)
                ]

            def intraday(self, security_id: int, exchange_segment: str, interval_min: int, from_date: date, to_date: date, oi: bool = True, instrument: str = "OPTSTK") -> list[Candle]:
                return []

            def expiries(self, underlying_security_id: int, underlying_seg: str) -> list[date]:
                return []

            def option_chain(self, underlying_security_id: int, underlying_seg: str, expiry: date) -> list[ChainRow]:
                return []

            def ltp(self, security_ids, exchange_segment: str = "NSE_EQ") -> dict[int, float]:
                self.ltp_calls += 1
                raise AssertionError("history adapter ltp() must not be called when ltp_adapter is supplied")

            def token_health(self) -> TokenHealth:
                return TokenHealth(True, None, None)

        class _LtpFake(_HistFake):
            def daily_candles(self, security_id: int, exchange_segment: str, from_date: date, to_date: date, instrument: str = "EQUITY") -> list[Candle]:
                raise AssertionError("ltp adapter daily_candles() must not be called")

            def ltp(self, security_ids, exchange_segment: str = "NSE_EQ") -> dict[int, float]:
                self.ltp_calls += 1
                return {int(sid): 2950.0 for sid in security_ids}

        with tempfile.TemporaryDirectory() as tmp:
            tdir = Path(tmp)
            conn = connect_db(tdir / "gamma.db")
            try:
                hist_ad = _HistFake(tdir)
                ltp_ad = _LtpFake(tdir)
                cfg = settings()
                summary = mark_levels.run(
                    now_ist().date(),
                    ref="ltp",
                    symbols=["RELIANCE"],
                    adapter=hist_ad,
                    ltp_adapter=ltp_ad,
                    conn=conn,
                    cfg=cfg,
                    daily_bars_limit=20,
                )
                self.assertEqual(summary["n_symbols"], 1)
                self.assertEqual(summary["n_failed"], 0)
                self.assertGreaterEqual(hist_ad.daily_calls, 1)
                self.assertEqual(hist_ad.ltp_calls, 0)
                self.assertEqual(ltp_ad.ltp_calls, 1)
            finally:
                conn.close()

    def test_master_rollover_clears_resolved_cache(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            _write_canonical_csv(data_dir, now_ist().date())
            z1 = _make_zip_bytes(_SAMPLE_ALLMASTER)
            z2 = _make_zip_bytes(_SAMPLE_ALLMASTER)
            current_now = [datetime(2026, 11, 2, 8, 0, tzinfo=wire.IST)]
            master_hits = [0]

            def handler(request: httpx.Request) -> httpx.Response:
                if str(request.url) == wire.ALLMASTER_URL:
                    master_hits[0] += 1
                    return httpx.Response(200, content=z1 if master_hits[0] == 1 else z2)
                return httpx.Response(404)

            ad = DefinedgeAdapter(
                "tok",
                data_dir,
                transport=httpx.MockTransport(handler),
                wall_clock=lambda: current_now[0],
            )
            try:
                r1 = ad._row("NSE_EQ", 2885)
                self.assertIsNotNone(r1)
                self.assertIn(("NSE_EQ", 2885), ad._resolved)
                current_now[0] = datetime(2026, 11, 3, 8, 0, tzinfo=wire.IST)
                nm2 = ad.native_master()
                self.assertIsNotNone(nm2)
                self.assertEqual(ad._resolved, {})
                self.assertEqual(master_hits[0], 2)
            finally:
                ad.close()

    def test_mark_levels_run_autoresolves_split_ltp_adapter(self) -> None:
        class _HistFake(BrokerAdapter):
            def __init__(self, data_dir: Path) -> None:
                from gammad.broker.dhan import ScripMaster, _read_master_csv
                self._sm = ScripMaster(_read_master_csv(_write_canonical_csv(data_dir, now_ist().date())))
                self.ltp_calls = 0

            def scrip_master(self, as_of: date | None = None):
                return self._sm

            def instruments(self) -> list[Instrument]:
                return self._sm.instruments()

            def daily_candles(self, security_id: int, exchange_segment: str, from_date: date, to_date: date, instrument: str = "EQUITY") -> list[Candle]:
                base = datetime.combine(from_date, datetime.min.time(), tzinfo=wire.IST)
                return [
                    Candle(int((base + timedelta(days=i)).timestamp() * 1000), 2930.0 + i, 2945.0 + i, 2925.0 + i, 2935.0 + i, 1000.0)
                    for i in range(25)
                ]

            def intraday(self, security_id: int, exchange_segment: str, interval_min: int, from_date: date, to_date: date, oi: bool = True, instrument: str = "OPTSTK") -> list[Candle]:
                return []

            def expiries(self, underlying_security_id: int, underlying_seg: str) -> list[date]:
                return []

            def option_chain(self, underlying_security_id: int, underlying_seg: str, expiry: date) -> list[ChainRow]:
                return []

            def ltp(self, security_ids, exchange_segment: str = "NSE_EQ") -> dict[int, float]:
                self.ltp_calls += 1
                raise AssertionError("history adapter ltp() must not be called")

            def token_health(self) -> TokenHealth:
                return TokenHealth(True, None, None)

        class _TradeFake(BrokerAdapter):
            def __init__(self) -> None:
                self.ltp_calls = 0

            def instruments(self) -> list[Instrument]:
                return []

            def daily_candles(self, *a, **kw) -> list[Candle]:
                return []

            def intraday(self, *a, **kw) -> list[Candle]:
                return []

            def expiries(self, *a, **kw) -> list[date]:
                return []

            def option_chain(self, *a, **kw) -> list[ChainRow]:
                return []

            def ltp(self, security_ids, exchange_segment: str = "NSE_EQ") -> dict[int, float]:
                self.ltp_calls += 1
                return {int(sid): 2950.0 for sid in security_ids}

            def token_health(self) -> TokenHealth:
                return TokenHealth(True, None, None)

        with tempfile.TemporaryDirectory() as tmp:
            tdir = Path(tmp)
            conn = connect_db(tdir / "gamma.db")
            try:
                hist_ad = _HistFake(tdir)
                trade_ad = _TradeFake()
                cfg = settings()
                with patch(
                    "gammad.broker.config_loader.resolve_configured_adapter",
                    side_effect=lambda _cfg, *, for_history=False: hist_ad if for_history else trade_ad,
                ):
                    summary = mark_levels.run(
                        now_ist().date(),
                        ref="ltp",
                        symbols=["RELIANCE"],
                        conn=conn,
                        cfg=cfg,
                        daily_bars_limit=20,
                    )
                self.assertEqual(summary["n_failed"], 0)
                self.assertEqual(hist_ad.ltp_calls, 0)
                self.assertEqual(trade_ad.ltp_calls, 1)
            finally:
                conn.close()

    def test_r14_r15_include_definedge_and_block_switch(self) -> None:
        self.assertIn("definedge", gamma_r14._IDS)
        st = gamma_r15.status()
        row = next(b for b in st["brokers"] if b["name"] == "definedge")
        self.assertFalse(row["switch_eligible"])
        # Verify FyersHistoryUnavailable hint includes definedge
        fyers_fake = type("FakeFyers", (), {"__module__": "gammad.broker.fyers.adapter"})()
        with self.assertRaises(mark_levels.FyersHistoryUnavailable) as cm:
            mark_levels._fyers_history_preflight(fyers_fake, now_ist().date(), 2000)  # type: ignore[arg-type]
        self.assertIn("definedge", str(cm.exception))

        seen_urls: list[str] = []

        def r15_handler(request: httpx.Request) -> httpx.Response:
            u = str(request.url)
            seen_urls.append(u)
            if u.endswith("/dart/v1/limits"):
                return httpx.Response(200, json={"status": "SUCCESS", "cash": "100"})
            if u.endswith("/dart/v1/quotes/NSE/26000"):
                return httpx.Response(200, json={"status": "SUCCESS", "ltp": "24500"})
            return httpx.Response(404)

        with patch.object(
            gamma_r15,
            "_merge_creds",
            return_value={"DEFINEDGE_API_SESSION_KEY": "tok-r15"},
        ):
            gamma_r15._LAST_TRY.pop("definedge", None)
            gamma_r15._PROOFS.pop("definedge", None)
            res = gamma_r15.auth_verify("definedge", transport=httpx.MockTransport(r15_handler))
        self.assertTrue(res["feed_verified"])
        self.assertFalse(res["switch_eligible"])
        self.assertNotIn(wire.ALLMASTER_URL, seen_urls)


if __name__ == "__main__":
    unittest.main()
