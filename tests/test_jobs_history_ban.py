"""Phase 1/2/4 regression tests for decoupled history, mark_levels preflight, httpx jobs, ban retries, merge_today & recorder."""

from __future__ import annotations

from datetime import date, datetime, time
import gzip
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import MagicMock, patch
from zoneinfo import ZoneInfo

import httpx

from gammad.app import Runtime
from gammad.broker.config_loader import resolve_configured_adapter
from gammad.config import settings
from gammad.core import lookahead
from gammad.jobs import ban_list, mark_levels, results_calendar
from gammad.record.recorder import Recorder
from gammad.watch.adapter import Bar as EngineBar, ContractBars
from gammad.watch.bars import Bar as WatchBar

IST = ZoneInfo("Asia/Kolkata")


class _MockFeedAdapter:
    name = "mock"

    def daily_candles(self, *args, **kwargs):
        return []

    def token_health(self):
        return MagicMock(valid=True)


class _DummyAdapterFyers(_MockFeedAdapter):
    pass


# Make type name look like FyersAdapter for preflight check
_DummyAdapterFyers.__name__ = "FyersAdapter"


class TestJobsHistoryBan(unittest.TestCase):
    def test_resolve_configured_adapter_uses_decoupled_history_provider(self) -> None:
        mock_cfg = SimpleNamespace(
            paths=SimpleNamespace(data_dir=Path(tempfile.gettempdir())),
            env=SimpleNamespace(
                broker="dhan",
                dhan_client_id="C1",
                dhan_access_token="T1",
                values={"GAMMA_HISTORY_PROVIDER": "zerodha", "KITE_API_KEY": "k1", "KITE_ACCESS_TOKEN": "t1"},
            ),
        )
        dhan_adapter = _MockFeedAdapter()
        dhan_entry = MagicMock(name="dhan", env_keys=(), key_names=())
        dhan_entry.name = "dhan"
        dhan_entry.make_adapter.return_value = dhan_adapter

        hist_adapter = _MockFeedAdapter()
        hist_entry = MagicMock(env_keys=(), key_names=("KITE_API_KEY", "KITE_ACCESS_TOKEN"))
        hist_entry.name = "zerodha"
        hist_entry.make_adapter.return_value = hist_adapter

        with (
            patch("gammad.broker.config_loader.load_history_provider_config") as hist_loader,
            patch("gammad.broker.registry.resolve", return_value=dhan_entry),
            patch("gammad.broker.registry.get", return_value=hist_entry) as reg_get,
        ):
            hist_loader.return_value = MagicMock(provider_name="zerodha", daily_bars_limit=1500, daily_request_budget=5)
            ad = resolve_configured_adapter(mock_cfg, for_history=True)
            self.assertIs(ad, hist_adapter)
            reg_get.assert_called_with("zerodha")

    def test_fyers_preflight_respects_daily_bars_limit_and_tier(self) -> None:
        adapter = _DummyAdapterFyers()
        d = date(2026, 11, 13)
        with (
            patch.object(_DummyAdapterFyers, "__module__", "gammad.broker.fyers.adapter"),
            patch.object(mark_levels, "FYERS_HISTORY_ACCESS_TIER", "standard"),
        ):
            mark_levels._fyers_history_preflight(adapter, d, 300)
            with self.assertRaises(mark_levels.FyersHistoryUnavailable):
                mark_levels._fyers_history_preflight(adapter, d, 2000)

    def test_ban_list_and_results_calendar_httpx_transports(self) -> None:
        d = date(2026, 11, 13)
        payload = b"Securities in Ban For Trade Date 13-NOV-2026:\n1,SAIL\n2,RBLBANK\n"
        compressed = gzip.compress(payload)

        def ban_handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                headers={"Content-Encoding": "gzip", "Content-Length": str(len(compressed))},
                content=compressed,
            )

        with patch("gammad.jobs.ban_list.httpx.Client",
                   return_value=httpx.Client(transport=httpx.MockTransport(ban_handler))):
            res = ban_list.transport("https://archives.nseindia.com/archives/fo/sec_ban/fo_secban_13112026.csv")
        self.assertEqual(res.status, 200, msg=res.reason)
        self.assertIn(b"SAIL", res.body)

        # Disallowed host and redirects must be blocked
        self.assertEqual(ban_list.transport("https://evil.example/steal").reason, "host refused")

        def redirect_handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(302, headers={"Location": "https://evil.example/steal"})

        with patch("gammad.jobs.ban_list.httpx.Client",
                   return_value=httpx.Client(transport=httpx.MockTransport(redirect_handler), follow_redirects=False)):
            redir = ban_list.transport("https://archives.nseindia.com/archives/fo/sec_ban/fo_secban_13112026.csv")
        self.assertEqual(redir.status, 0)
        self.assertIn("redirect refused", redir.reason)

        # ResultsCalendar transport closes cleanly & uses mock httpx transport
        cal_json = b'[{"bm_symbol":"RELIANCE","bm_date":"13-Nov-2026","bm_desc":"Financial Result"}]'

        def cal_handler(request: httpx.Request) -> httpx.Response:
            if "api/" in str(request.url):
                return httpx.Response(200, content=cal_json, headers={"Content-Type": "application/json"})
            return httpx.Response(200, content=b"<html></html>", headers={"Set-Cookie": "nsit=1; Path=/"})

        t = results_calendar.Transport()
        t.client.close()
        t.client = httpx.Client(transport=httpx.MockTransport(cal_handler))
        try:
            with tempfile.TemporaryDirectory(prefix="gamma_rcal_") as tmp:
                rc = results_calendar.fetch_results_calendar(d, Path(tmp) / "results_calendar.json", fetch=t)
                self.assertTrue(rc.results_today("RELIANCE", d))
        finally:
            t.close()

    def test_contract_bars_merge_today_and_recorder_close_day_isolation(self) -> None:
        cb = ContractBars(45001)
        cb.seed([EngineBar(1000, 10, 11, 9, 10, 5, 100)])
        cb.append(EngineBar(3000, 12, 13, 11, 12, 8, 120))
        cb.merge_today([EngineBar(2000, 11, 12, 10, 11, 6, 110), EngineBar(3000, 99, 99, 99, 99, 1, 999)])
        self.assertEqual([b.ts_ms for b in cb.today()], [2000, 3000])
        self.assertEqual(cb.today()[1].close, 12)  # live bar preserved over duplicate backfill

        with tempfile.TemporaryDirectory(prefix="gamma_rec_") as tmp:
            rec = Recorder(Path(tmp) / "ticks.db", Path(tmp) / "pq")
            rec.flush_db = MagicMock(side_effect=RuntimeError("db fail"))
            rec.flush_parquet = MagicMock()
            rec.close_day()
            rec.flush_parquet.assert_called_once()

    def test_lookahead_from_files_missing_optional_calendar_returns_empty(self) -> None:
        with tempfile.TemporaryDirectory(prefix="gamma_cal_") as tmp:
            with patch.object(lookahead, "DEFAULT_CALENDAR", Path(tmp) / "missing.yaml"):
                self.assertEqual(lookahead.from_files(date(2026, 11, 13)), ())

    def test_runtime_ban_bounded_retries_and_startup_oi_carry(self) -> None:
        import asyncio

        d = date(2026, 11, 13)
        with tempfile.TemporaryDirectory(prefix="gamma_rt_") as tmp:
            base = settings()
            cfg = base.__class__(
                **{
                    **base.__dict__,
                    "paths": base.paths.__class__(
                        **{**base.paths.__dict__, "data_dir": Path(tmp), "logs_dir": Path(tmp),
                           "db_path": Path(tmp) / "g.db"}
                    ),
                }
            )
            rt = Runtime(cfg)
            try:
                rt.scanners_089 = True
                rt.is_trading_day = lambda _d: True

                calls = {"n": 0, "now": 1_000_000}
                rt._wall_ms = lambda: calls["now"]

                def flaky_fetch(_d: date):
                    calls["n"] += 1
                    if calls["n"] < 3:
                        raise RuntimeError("transient nse timeout")
                    return ban_list.BanList(d, ban_list.STATUS_OK, frozenset({"SAIL"}), None, calls["now"])

                rt._sw_ban_fetch = flaky_fetch

                async def run_scenario():
                    # 1st failure records attempt 1, does NOT lock out _sw_ban_tried
                    await rt.run_sw_ban_job(d)
                    self.assertFalse(rt._sw_ban_due(d))  # within 60s cooldown
                    calls["now"] += 61_000
                    self.assertTrue(rt._sw_ban_due(d))   # retry allowed after cooldown
                    await rt.run_sw_ban_job(d)           # 2nd failure
                    calls["now"] += 61_000
                    self.assertTrue(rt._sw_ban_due(d))
                    await rt.run_sw_ban_job(d)           # 3rd attempt succeeds
                    self.assertTrue(rt._sw_ban_ok(d))
                    self.assertFalse(rt._sw_ban_due(d))  # no longer due once OK

                asyncio.run(run_scenario())

                # Test startup series merge_today without regressing live OI carry
                rt.trade_date = d
                rt.series[45001] = ContractBars(45001)
                live_ts = int(datetime.combine(d, time(9, 20), IST).timestamp() * 1000)
                early_ts = int(datetime.combine(d, time(9, 15), IST).timestamp() * 1000)
                rt.carry.bar(45001, WatchBar(live_ts, 5, 105, 110, 100, 108, 50, 900, 1))
                rt.series[45001].append(EngineBar(live_ts, 105, 110, 100, 108, 50, 900))
                early_wb = WatchBar(early_ts, 5, 100, 106, 98, 105, 40, 600, 1)

                added_row = SimpleNamespace(
                    security_id=45001,
                    underlying_security_id=2885,
                    scanner="V1N",
                    list_name="BREAKOUT-CE",
                    rank=1,
                    to_dict=lambda: {
                        "security_id": 45001,
                        "underlying_security_id": 2885,
                        "symbol": "RELIANCE26NOV1300CE",
                        "option_type": "CE",
                        "strike": 1300.0,
                        "expiry_date": "2026-11-26",
                        "lot_size": 100,
                        "tick_size": 0.05,
                        "scanner": "V1N",
                        "list_name": "BREAKOUT-CE",
                    },
                )
                hub = MagicMock()
                hub.day = None
                hub.prepare.return_value = [added_row]
                hub.v2 = None
                hub.states = {}
                hub.list_rows.return_value = [added_row]
                rt.s089 = hub
                rt.adapter = lambda: None
                rt._089_wait = {}
                rt._089_und_wait = {}
                rt._089_prices = {}

                async def _noop(*args, **kwargs):
                    return None

                rt._warmup_previous = _noop
                rt._read_today = MagicMock(return_value=(SimpleNamespace(bars={45001: [early_wb]}), None))

                asyncio.run(rt._prepare_089_body(d, []))

                self.assertEqual([b.ts_ms for b in rt.series[45001].today()], [early_ts, live_ts])
                self.assertEqual(rt.carry.last(45001), 900.0)
            finally:
                rt.conn.close()


if __name__ == "__main__":
    unittest.main()
