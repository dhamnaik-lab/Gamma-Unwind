"""Unit test suite for R1.9 Broker-Neutral Multi-Account and Durable Watermark.

Runs offline with Python 3.13. Zero live broker calls, zero real orders.
"""

from __future__ import annotations

import asyncio
import os
import shutil
import tempfile
import unittest
from datetime import date, datetime
from pathlib import Path

from gammad.broker.account import (
    AccountIdentity,
    AccountIdentityError,
    AccountSessionManager,
    HistoryProviderConfig,
    SessionRoutingError,
)
from gammad.broker.base import BrokerAdapter, Candle, ChainRow, Instrument, TokenHealth
from gammad.broker.durable_watermark import (
    DurableTickQueue,
    WatermarkHold,
    WatermarkStore,
    wrap_feed_queue,
)
from gammad.watch.feed_protocol import Tick


class DummyMockAdapter(BrokerAdapter):
    """Synthetic in-memory adapter for offline testing."""

    def __init__(self, name: str = "mock_adapter") -> None:
        self.name = name
        self.closed = False

    def instruments(self) -> list[Instrument]:
        return []

    def daily_candles(self, *args, **kwargs) -> list[Candle]:
        return []

    def intraday(self, *args, **kwargs) -> list[Candle]:
        return []

    def expiries(self, *args, **kwargs) -> list[date]:
        return []

    def option_chain(self, *args, **kwargs) -> list[ChainRow]:
        return []

    def ltp(self, *args, **kwargs) -> dict[int, float]:
        return {}

    def token_health(self) -> TokenHealth:
        return TokenHealth(valid=True, validity_ist="2026-11-02 23:59:59", error=None)

    def close(self) -> None:
        self.closed = True


class TestAccountIdentity(unittest.TestCase):
    def test_valid_identity(self) -> None:
        ident = AccountIdentity(broker="zerodha", account_id="AB1234", label="Primary", revision=2)
        self.assertEqual(ident.broker, "zerodha")
        self.assertEqual(ident.account_id, "AB1234")
        self.assertEqual(ident.account_key, "zerodha:AB1234")
        self.assertTrue(len(ident.namespace_hash) == 64)

    def test_mock_broker_allowed(self) -> None:
        ident = AccountIdentity(broker="mock_broker", account_id="TEST001")
        self.assertEqual(ident.broker, "mock_broker")

    def test_unregistered_broker_rejected(self) -> None:
        with self.assertRaises(AccountIdentityError):
            AccountIdentity(broker="unregistered_broker_xyz", account_id="ACC01")

    def test_invalid_account_id_format(self) -> None:
        # Too short
        with self.assertRaises(AccountIdentityError):
            AccountIdentity(broker="zerodha", account_id="A")
        # Whitespace
        with self.assertRaises(AccountIdentityError):
            AccountIdentity(broker="zerodha", account_id=" AB1234 ")
        # Disallowed special characters
        with self.assertRaises(AccountIdentityError):
            AccountIdentity(broker="zerodha", account_id="AB@1234!")

    def test_token_leakage_prevented(self) -> None:
        # Long string with token pattern
        token_like = "eyJhGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0"
        with self.assertRaises(AccountIdentityError):
            AccountIdentity(broker="fyers", account_id=token_like)


class TestAccountSessionManager(unittest.TestCase):
    def setUp(self) -> None:
        self.mgr = AccountSessionManager()

    def test_multi_account_registration_and_switching(self) -> None:
        acc1 = AccountIdentity(broker="zerodha", account_id="KITE_01")
        acc2 = AccountIdentity(broker="dhan", account_id="DHAN_02")
        adapter1 = DummyMockAdapter("kite")
        adapter2 = DummyMockAdapter("dhan")

        self.mgr.register_account(acc1, adapter1)
        self.mgr.register_account(acc2, adapter2)

        self.assertEqual(len(self.mgr.list_accounts()), 2)
        self.assertEqual(self.mgr.active_account().identity.account_key, "zerodha:KITE_01")

        # Explicitly switch active account
        self.mgr.set_active_account("dhan", "DHAN_02")
        self.assertEqual(self.mgr.active_account().identity.account_key, "dhan:DHAN_02")

    def test_failure_isolation(self) -> None:
        acc1 = AccountIdentity(broker="zerodha", account_id="KITE_01")
        acc2 = AccountIdentity(broker="dhan", account_id="DHAN_02")
        adapter1 = DummyMockAdapter("kite")
        adapter2 = DummyMockAdapter("dhan")

        self.mgr.register_account(acc1, adapter1)
        self.mgr.register_account(acc2, adapter2)

        # Isolate failure on Kite
        self.mgr.isolate_failure("zerodha", "KITE_01", "Feed connection timeout")
        s1 = self.mgr.get_account("zerodha", "KITE_01")
        s2 = self.mgr.get_account("dhan", "DHAN_02")

        self.assertFalse(s1.is_operational)
        self.assertEqual(s1.failure_count, 1)
        # Dhan remains fully operational!
        self.assertTrue(s2.is_operational)
        # Active account automatically fell back to the operational one
        self.assertEqual(self.mgr.active_account().identity.account_key, "dhan:DHAN_02")

    def test_history_provider_decoupling(self) -> None:
        cfg = HistoryProviderConfig(provider_name="dhan_history", daily_bars_limit=2000, daily_request_budget=6)
        hist_adapter = DummyMockAdapter("dhan_hist")

        self.mgr.register_history_provider(cfg, hist_adapter)
        self.assertEqual(self.mgr.history_config().provider_name, "dhan_history")
        self.assertIs(self.mgr.history_adapter(), hist_adapter)


class TestDurableWatermark(unittest.TestCase):
    def setUp(self) -> None:
        self.test_dir = tempfile.mkdtemp(prefix="gamma_watermark_test_")
        self.db_path = Path(self.test_dir) / "watermark.sqlite"

    def tearDown(self) -> None:
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def _make_tick(
        self,
        ltt_sec: int,
        recv_sec: int,
        ltp: float = 100.0,
        volume: int = 500,
        oi: int = 2000,
        sec_id: int = 45000,
    ) -> Tick:
        return Tick(
            ts_ms=ltt_sec * 1000,
            recv_ms=recv_sec * 1000,
            security_id=sec_id,
            segment="NSE_FNO",
            ltp=ltp,
            volume_day=volume,
            oi=oi,
            prev_oi=1900,
            prev_close=98.5,
            oi_day_high=2500,
            oi_day_low=1800,
        )

    def test_increasing_ltt_accepted(self) -> None:
        store = WatermarkStore(self.db_path, broker="mock_broker", account_scope="acc_001")
        t1 = self._make_tick(ltt_sec=1700000010, recv_sec=1700000011, ltp=101.0)
        t2 = self._make_tick(ltt_sec=1700000015, recv_sec=1700000016, ltp=102.5)

        self.assertTrue(store.claim(t1))
        self.assertTrue(store.claim(t2))

    def test_regression_rejected_after_reconnect(self) -> None:
        store1 = WatermarkStore(self.db_path, broker="mock_broker", account_scope="acc_001")
        t1 = self._make_tick(ltt_sec=1700000020, recv_sec=1700000021, ltp=105.0)
        self.assertTrue(store1.claim(t1))

        # Reopen store simulating process / feed restart
        store2 = WatermarkStore(self.db_path, broker="mock_broker", account_scope="acc_001")
        t_regress = self._make_tick(ltt_sec=1700000018, recv_sec=1700000022, ltp=104.0)
        self.assertFalse(store2.claim(t_regress))

    def test_duplicate_payload_rejected(self) -> None:
        store = WatermarkStore(self.db_path, broker="mock_broker", account_scope="acc_001")
        t1 = self._make_tick(ltt_sec=1700000030, recv_sec=1700000031, ltp=105.0, volume=1000, oi=5000)
        self.assertTrue(store.claim(t1))

        # Exact duplicate
        t1_dup = self._make_tick(ltt_sec=1700000030, recv_sec=1700000032, ltp=105.0, volume=1000, oi=5000)
        self.assertFalse(store.claim(t1_dup))

        # Same LTT but volume changed (valid sub-second update)
        t1_update = self._make_tick(ltt_sec=1700000030, recv_sec=1700000032, ltp=105.0, volume=1050, oi=5000)
        self.assertTrue(store.claim(t1_update))

    def test_multi_account_segregation(self) -> None:
        store_a = WatermarkStore(self.db_path, broker="zerodha", account_scope="user_A")
        store_b = WatermarkStore(self.db_path, broker="zerodha", account_scope="user_B")

        t_a = self._make_tick(ltt_sec=1700000040, recv_sec=1700000041, sec_id=99999, ltp=250.0)
        t_b = self._make_tick(ltt_sec=1700000040, recv_sec=1700000041, sec_id=99999, ltp=250.0)

        # Both accounts process the same symbol/LTT into separate namespaces without collision
        self.assertTrue(store_a.claim(t_a))
        self.assertTrue(store_b.claim(t_b))

    def test_unverifiable_and_stale_ltt_guards(self) -> None:
        store = WatermarkStore(self.db_path, broker="mock_broker", account_scope="acc_001")

        # Zero ts_ms
        t_zero = Tick(
            ts_ms=0,
            recv_ms=1700000000000,
            security_id=1,
            segment="NSE_FNO",
            ltp=1.0,
            volume_day=1,
            oi=1,
            prev_oi=1,
            prev_close=1.0,
            oi_day_high=1,
            oi_day_low=1,
        )
        with self.assertRaises(WatermarkHold):
            store.claim(t_zero)

        # Stale (> 300s)
        t_stale = self._make_tick(ltt_sec=1700000000, recv_sec=1700000400)
        with self.assertRaises(WatermarkHold):
            store.claim(t_stale)

        # Future (> 5s)
        t_future = self._make_tick(ltt_sec=1700000020, recv_sec=1700000010)
        with self.assertRaises(WatermarkHold):
            store.claim(t_future)

    def test_durable_tick_queue_integration(self) -> None:
        store = WatermarkStore(self.db_path, broker="mock_broker", account_scope="acc_001")
        queue: asyncio.Queue[Tick] = asyncio.Queue()
        d_queue = wrap_feed_queue(queue, store)

        t1 = self._make_tick(ltt_sec=1700000050, recv_sec=1700000051)
        d_queue.put_nowait(t1)
        self.assertEqual(d_queue.accepted, 1)
        self.assertEqual(queue.qsize(), 1)

        # Putting duplicate tick raises WatermarkHold and increments refused
        with self.assertRaises(WatermarkHold):
            d_queue.put_nowait(t1)
        self.assertEqual(d_queue.refused, 1)
        self.assertEqual(queue.qsize(), 1)

    def test_untrusted_parent_directory(self) -> None:
        invalid_path = Path(self.test_dir) / "non_existent_subdir" / "watermark.sqlite"
        with self.assertRaises(WatermarkHold):
            WatermarkStore(invalid_path, broker="mock_broker", account_scope="acc_001")

    def test_concurrent_multi_thread_claims(self) -> None:
        import concurrent.futures

        store = WatermarkStore(self.db_path, broker="mock_broker", account_scope="acc_001")

        def worker(thread_idx: int) -> int:
            accepted = 0
            for i in range(10):
                ltt = 1700000100 + i * 10
                sec_id = 50000 + thread_idx
                t = self._make_tick(ltt_sec=ltt, recv_sec=ltt + 1, sec_id=sec_id, ltp=100.0 + i)
                if store.claim(t):
                    accepted += 1
            return accepted

        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            futures = [executor.submit(worker, idx) for idx in range(4)]
            results = [f.result() for f in futures]

        self.assertEqual(sum(results), 40)

    def test_e2e_synthetic_multi_broker_stream(self) -> None:
        acc_z = AccountIdentity(broker="zerodha", account_id="USER_ZERODHA")
        acc_d = AccountIdentity(broker="dhan", account_id="USER_DHAN")

        store_z = WatermarkStore(self.db_path, broker=acc_z.broker, account_scope=acc_z.account_id)
        store_d = WatermarkStore(self.db_path, broker=acc_d.broker, account_scope=acc_d.account_id)

        q_z: asyncio.Queue[Tick] = asyncio.Queue()
        q_d: asyncio.Queue[Tick] = asyncio.Queue()

        feed_z = wrap_feed_queue(q_z, store_z)
        feed_d = wrap_feed_queue(q_d, store_d)

        # Stream ticks into both feeds concurrently
        for i in range(5):
            t_z = self._make_tick(ltt_sec=1700001000 + i * 5, recv_sec=1700001001 + i * 5, sec_id=60001, ltp=200.0 + i)
            t_d = self._make_tick(ltt_sec=1700001000 + i * 5, recv_sec=1700001001 + i * 5, sec_id=60002, ltp=300.0 + i)
            feed_z.put_nowait(t_z)
            feed_d.put_nowait(t_d)

        self.assertEqual(q_z.qsize(), 5)
        self.assertEqual(q_d.qsize(), 5)
        self.assertEqual(feed_z.accepted, 5)
        self.assertEqual(feed_d.accepted, 5)

        # Reconnect test: verify both feeds reject stale ticks from the past
        t_z_stale = self._make_tick(ltt_sec=1700001010, recv_sec=1700001025, sec_id=60001, ltp=199.0)
        t_d_stale = self._make_tick(ltt_sec=1700001010, recv_sec=1700001025, sec_id=60002, ltp=299.0)

        with self.assertRaises(WatermarkHold):
            feed_z.put_nowait(t_z_stale)
        with self.assertRaises(WatermarkHold):
            feed_d.put_nowait(t_d_stale)

        self.assertEqual(feed_z.refused, 1)
        self.assertEqual(feed_d.refused, 1)


class TestRuntimeIntegration(unittest.TestCase):
    def setUp(self) -> None:
        from dataclasses import replace
        from gammad.config import settings

        self.test_dir = tempfile.mkdtemp(prefix="gamma_rt_test_")
        base_cfg = settings()
        test_paths = replace(
            base_cfg.paths,
            data_dir=Path(self.test_dir),
            logs_dir=Path(self.test_dir),
            db_path=Path(self.test_dir) / "test_gamma.db",
        )
        self.cfg = replace(base_cfg, paths=test_paths)
        self.rts: list[Any] = []

    def tearDown(self) -> None:
        for rt in getattr(self, "rts", []):
            if rt is not None and getattr(rt, "conn", None) is not None:
                try:
                    rt.conn.close()
                except Exception:
                    pass
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_runtime_session_manager_wiring(self) -> None:
        from gammad.app import Runtime

        mgr = AccountSessionManager()
        ident = AccountIdentity("zerodha", "KITE_MAIN")
        adapter = DummyMockAdapter("kite_adapter")
        mgr.register_account(ident, adapter)

        rt = Runtime(self.cfg, session_manager=mgr)
        self.rts.append(rt)
        self.assertIs(rt.session_manager, mgr)
        self.assertIs(rt.adapter(), adapter)

    def test_runtime_history_adapter_decoupling(self) -> None:
        from gammad.app import Runtime

        mgr = AccountSessionManager()
        ident = AccountIdentity("zerodha", "KITE_MAIN")
        adapter = DummyMockAdapter("kite_adapter")
        mgr.register_account(ident, adapter)

        hist_cfg = HistoryProviderConfig("dhan_history")
        hist_adapter = DummyMockAdapter("dhan_hist_adapter")
        mgr.register_history_provider(hist_cfg, hist_adapter)

        rt = Runtime(self.cfg, session_manager=mgr)
        self.rts.append(rt)
        self.assertIs(rt.adapter(), adapter)
        self.assertIs(rt.history_adapter(), hist_adapter)

    def test_runtime_fallback_backward_compatibility(self) -> None:
        from gammad.app import Runtime

        fallback = DummyMockAdapter("fallback_adapter")
        rt = Runtime(self.cfg, adapter=fallback)
        self.rts.append(rt)

        self.assertIsNone(rt.session_manager)
        self.assertIs(rt.adapter(), fallback)
        self.assertIs(rt.history_adapter(), fallback)

    def test_registry_feed_dynamic_active_broker_resolution(self) -> None:
        from unittest.mock import MagicMock
        from gammad.app import Runtime
        from gammad.broker import registry

        mgr = AccountSessionManager()
        ident1 = AccountIdentity("zerodha", "KITE_01")
        adapter1 = DummyMockAdapter("kite_adapter")
        ident2 = AccountIdentity("dhan", "DHAN_02")
        adapter2 = DummyMockAdapter("dhan_adapter")

        mgr.register_account(ident1, adapter1, credentials={"KITE_API_KEY": "k1", "KITE_ACCESS_TOKEN": "t1"})
        mgr.register_account(ident2, adapter2, credentials={"DHAN_CLIENT_ID": "d2", "DHAN_ACCESS_TOKEN": "t2"})

        rt = Runtime(self.cfg, session_manager=mgr)
        self.rts.append(rt)

        # Mock the entry's make_feed to verify dynamically passed credentials and adapter
        q: asyncio.Queue = asyncio.Queue()
        mock_entry = registry.get("zerodha")
        original_make_feed = mock_entry.make_feed
        calls = []

        def spy_make_feed(creds, feed_q, auth_cb, adapter=None):
            calls.append((creds, adapter))
            return MagicMock()

        object.__setattr__(mock_entry, "make_feed", spy_make_feed)
        try:
            feed = rt._registry_feed(q, lambda c, m: None, self.cfg)
            self.assertEqual(len(calls), 1)
            self.assertEqual(calls[0][0].get("KITE_API_KEY"), "k1")
            self.assertIs(calls[0][1], adapter1)
        finally:
            object.__setattr__(mock_entry, "make_feed", original_make_feed)

    def test_reload_and_apply_credentials_target_active_adapter(self) -> None:
        from gammad.app import Runtime

        mgr = AccountSessionManager()
        ident = AccountIdentity("zerodha", "KITE_01")
        adapter = DummyMockAdapter("kite_adapter")
        adapter.creds_set = None

        def set_credentials(creds):
            adapter.creds_set = creds

        adapter.set_credentials = set_credentials
        mgr.register_account(ident, adapter, credentials={"KITE_API_KEY": "k_old"})

        rt = Runtime(self.cfg, session_manager=mgr)
        self.rts.append(rt)

        rt.apply_credentials({"KITE_API_KEY": "k_new", "KITE_ACCESS_TOKEN": "t_new"})
        self.assertIsNotNone(adapter.creds_set)
        self.assertEqual(adapter.creds_set.get("KITE_API_KEY"), "k_new")
        self.assertEqual(mgr.active_account().credentials.get("KITE_API_KEY"), "k_new")

    def test_serve_async_wiring(self) -> None:
        import inspect
        from gammad.app import serve_async

        src = inspect.getsource(serve_async)
        self.assertIn("load_account_session_manager(cfg)", src)
        self.assertIn("load_history_provider_config(cfg)", src)
        self.assertIn("watermark.db", src)
        self.assertIn("session_manager=session_manager", src)

    def test_dynamic_active_account_switching_updates_broker_properties(self) -> None:
        from gammad.app import Runtime

        mgr = AccountSessionManager()
        ident1 = AccountIdentity("zerodha", "KITE_01")
        adapter1 = DummyMockAdapter("kite_adapter")
        ident2 = AccountIdentity("dhan", "DHAN_02")
        adapter2 = DummyMockAdapter("dhan_adapter")

        mgr.register_account(ident1, adapter1, credentials={"KITE_API_KEY": "k1", "KITE_ACCESS_TOKEN": "t1"})
        mgr.register_account(ident2, adapter2, credentials={"DHAN_CLIENT_ID": "d2", "DHAN_ACCESS_TOKEN": "t2"})

        rt = Runtime(self.cfg, session_manager=mgr)
        self.rts.append(rt)

        # Initial active account is zerodha
        self.assertEqual(rt.broker_entry.name, "zerodha")
        self.assertEqual(rt.broker_name(), "zerodha")
        self.assertIs(rt.adapter(), adapter1)
        self.assertEqual(rt.broker_creds().get("KITE_API_KEY"), "k1")

        # Dynamically switch active account to dhan
        mgr.set_active_account("dhan", "DHAN_02")
        self.assertEqual(rt.broker_entry.name, "dhan")
        self.assertEqual(rt.broker_name(), "dhan")
        self.assertIs(rt.adapter(), adapter2)
        self.assertEqual(rt.broker_creds().get("DHAN_CLIENT_ID"), "d2")

    def test_adapter_returned_for_unauthenticated_or_expired_account(self) -> None:
        from gammad.app import Runtime

        mgr = AccountSessionManager()
        ident = AccountIdentity("zerodha", "KITE_EXP")
        adapter = DummyMockAdapter("kite_adapter")
        # Simulate expired / unauthenticated token
        adapter.token_health = lambda: TokenHealth(valid=False, error="token expired", auth_failed=True)

        session = mgr.register_account(ident, adapter, probe_health=True)
        self.assertFalse(session.is_operational)

        rt = Runtime(self.cfg, session_manager=mgr)
        self.rts.append(rt)

        # Runtime.adapter() must return the adapter even if token is expired so it can heal
        self.assertIs(rt.adapter(), adapter)

        # apply_credentials reaches adapter
        received_creds = {}
        adapter.set_credentials = lambda c: received_creds.update(c)
        rt.apply_credentials({"KITE_API_KEY": "k_fresh", "KITE_ACCESS_TOKEN": "t_fresh"})
        self.assertEqual(received_creds.get("KITE_API_KEY"), "k_fresh")

    def test_serve_async_lifecycle_bootstrap(self) -> None:
        import asyncio
        from dataclasses import replace
        from unittest.mock import patch
        from gammad.app import serve_async

        class StopServe(Exception):
            pass

        def run_bootstrap(cfg_obj):
            captured = None

            def mock_create_app(rt):
                nonlocal captured
                captured = rt
                raise StopServe("bootstrap_ok")

            with patch("gammad.api.server.create_app", side_effect=mock_create_app):
                with self.assertRaises(StopServe):
                    asyncio.run(serve_async(tray=False, cfg=cfg_obj))
            self.assertIsNotNone(captured)
            self.rts.append(captured)
            return captured

        # 1. No broker configured: app boots cleanly with no default broker, no engines, no history fallback
        no_broker_cfg = replace(self.cfg, env=replace(self.cfg.env, broker="", values={}))
        rt_none = run_bootstrap(no_broker_cfg)
        self.assertIsNotNone(rt_none.session_manager)
        self.assertIsNone(rt_none.broker_entry)
        self.assertIsNone(rt_none.history_config())
        self.assertFalse(rt_none.engines_allowed)
        self.assertEqual(rt_none.broker_name(), "none")
        self.assertTrue(str(rt_none.watermark_ledger_path).endswith("watermark.db"))

        # 2. Explicit broker configured: session_manager + history_config + watermark ledger wired
        zerodha_cfg = replace(
            self.cfg,
            env=replace(
                self.cfg.env,
                broker="zerodha",
                values={"KITE_API_KEY": "k1", "KITE_ACCESS_TOKEN": "t1"},
            ),
        )
        rt_z = run_bootstrap(zerodha_cfg)
        self.assertIsNotNone(rt_z.session_manager)
        self.assertIsNotNone(rt_z.history_config())
        self.assertEqual(rt_z.history_config().provider_name, "zerodha")
        self.assertTrue(rt_z.engines_allowed)
        self.assertTrue(str(rt_z.watermark_ledger_path).endswith("watermark.db"))


class TestConfigLoader(unittest.TestCase):
    def setUp(self) -> None:
        from dataclasses import replace
        from gammad.config import settings

        self.test_dir = tempfile.mkdtemp(prefix="gamma_cfg_test_")
        base_cfg = settings()
        test_paths = replace(
            base_cfg.paths,
            data_dir=Path(self.test_dir),
            logs_dir=Path(self.test_dir),
            db_path=Path(self.test_dir) / "test_gamma.db",
        )
        self.cfg = replace(base_cfg, paths=test_paths)
        self.rts: list[Any] = []

    def tearDown(self) -> None:
        for rt in getattr(self, "rts", []):
            if rt is not None and getattr(rt, "conn", None) is not None:
                try:
                    rt.conn.close()
                except Exception:
                    pass
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_mode_a_legacy_fallback(self) -> None:
        from dataclasses import replace
        from gammad.broker.config_loader import load_account_session_manager

        # Without GAMMA_ACCOUNTS_JSON, uses cfg.env.broker
        test_env = replace(
            self.cfg.env,
            broker="zerodha",
            values={"KITE_API_KEY": "test_key", "KITE_ACCESS_TOKEN": "test_tok"},
        )
        cfg = replace(self.cfg, env=test_env)
        mgr = load_account_session_manager(cfg)

        self.assertIsNotNone(mgr.active_account())
        self.assertEqual(mgr.active_account().identity.broker, "zerodha")
        self.assertEqual(mgr.active_account().identity.account_id, "primary")
        self.assertEqual(len(mgr.list_accounts()), 1)
        self.assertIsNotNone(mgr.history_adapter())

    def test_mode_b_multi_account_json(self) -> None:
        import json
        from dataclasses import replace
        from gammad.broker.config_loader import load_account_session_manager

        accounts = [
            {
                "account_id": "acc_01",
                "broker": "mock_broker_a",
                "credentials": {"API_KEY": "key1"},
                "is_active": True,
            },
            {
                "account_id": "acc_02",
                "broker": "mock_broker_b",
                "credentials": {"API_KEY": "key2"},
                "is_active": False,
            },
        ]
        test_env = replace(
            self.cfg.env,
            values={"GAMMA_ACCOUNTS_JSON": json.dumps(accounts)},
        )
        cfg = replace(self.cfg, env=test_env)
        mgr = load_account_session_manager(cfg)

        self.assertEqual(len(mgr.list_accounts()), 2)
        active = mgr.active_account()
        self.assertIsNotNone(active)
        self.assertEqual(active.identity.account_key, "mock_broker_a:acc_01")
        self.assertEqual(active.credentials.get("API_KEY"), "key1")

        # Switch active account
        mgr.set_active_account("mock_broker_b", "acc_02")
        self.assertEqual(mgr.active_account().identity.account_key, "mock_broker_b:acc_02")

    def test_invalid_config_rejection(self) -> None:
        from dataclasses import replace
        from gammad.broker.account import AccountIdentityError
        from gammad.broker.config_loader import load_account_session_manager

        # 1. Malformed JSON syntax
        bad_json_env = replace(self.cfg.env, values={"GAMMA_ACCOUNTS_JSON": "{malformed_json"})
        with self.assertRaises(AccountIdentityError):
            load_account_session_manager(replace(self.cfg, env=bad_json_env))

        # 2. Invalid account_id characters
        bad_id_env = replace(
            self.cfg.env,
            values={"GAMMA_ACCOUNTS_JSON": '[{"account_id": "bad@id!", "broker": "zerodha"}]'},
        )
        with self.assertRaises(AccountIdentityError):
            load_account_session_manager(replace(self.cfg, env=bad_id_env))

        # 3. Token pattern leakage rejection
        token_like = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.token_payload_longer_than_32_chars"
        leak_env = replace(
            self.cfg.env,
            values={"GAMMA_ACCOUNTS_JSON": f'[{{"account_id": "{token_like}", "broker": "zerodha"}}]'},
        )
        with self.assertRaises(AccountIdentityError):
            load_account_session_manager(replace(self.cfg, env=leak_env))

        # 4. Unregistered broker rejection
        bad_broker_env = replace(
            self.cfg.env,
            values={"GAMMA_ACCOUNTS_JSON": '[{"account_id": "acc_01", "broker": "unknown_broker_xyz"}]'},
        )
        with self.assertRaises(AccountIdentityError):
            load_account_session_manager(replace(self.cfg, env=bad_broker_env))

    def test_history_provider_decoupling(self) -> None:
        from dataclasses import replace
        from gammad.broker.config_loader import load_history_provider_config

        # 1. Explicit history provider configured
        hist_env = replace(
            self.cfg.env,
            values={
                "GAMMA_HISTORY_PROVIDER": "zerodha",
                "GAMMA_HISTORY_BARS_LIMIT": "1500",
                "GAMMA_HISTORY_DAILY_BUDGET": "10",
            },
        )
        h_cfg = load_history_provider_config(replace(self.cfg, env=hist_env))
        self.assertEqual(h_cfg.provider_name, "zerodha")
        self.assertEqual(h_cfg.daily_bars_limit, 1500)
        self.assertEqual(h_cfg.daily_request_budget, 10)

        # 2. Fallback when not set
        fallback_env = replace(self.cfg.env, broker="fyers", values={})
        h_fallback = load_history_provider_config(replace(self.cfg, env=fallback_env))
        self.assertEqual(h_fallback.provider_name, "fyers")
        self.assertEqual(h_fallback.daily_bars_limit, 2000)
        self.assertEqual(h_fallback.daily_request_budget, 6)

    def test_empty_array_and_disabled_accounts(self) -> None:
        import json
        from dataclasses import replace
        from gammad.broker.account import SessionRoutingError
        from gammad.broker.config_loader import load_account_session_manager

        # Empty array with no broker / no history provider -> no default broker fallback
        cfg_empty_no_hist = replace(self.cfg, env=replace(self.cfg.env, broker="", values={"GAMMA_ACCOUNTS_JSON": "[]"}))
        mgr_empty_no_hist = load_account_session_manager(cfg_empty_no_hist)
        self.assertEqual(len(mgr_empty_no_hist.list_accounts()), 0)
        self.assertIsNone(mgr_empty_no_hist.active_account())
        with self.assertRaises(SessionRoutingError):
            mgr_empty_no_hist.history_adapter()

        # Empty array with explicit decoupled history provider -> returns unauthenticated adapter without SessionRoutingError
        cfg_empty = replace(
            self.cfg,
            env=replace(
                self.cfg.env,
                broker="",
                values={"GAMMA_ACCOUNTS_JSON": "[]", "GAMMA_HISTORY_PROVIDER": "fyers"},
            ),
        )
        mgr_empty = load_account_session_manager(cfg_empty)
        self.assertEqual(len(mgr_empty.list_accounts()), 0)
        self.assertIsNone(mgr_empty.active_account())
        self.assertIsNotNone(mgr_empty.history_adapter())

        # All accounts disabled with explicit history/broker -> history_adapter remains available
        disabled_accounts = [
            {"account_id": "acc_dis", "broker": "mock_broker_a", "enabled": False}
        ]
        cfg_dis = replace(
            self.cfg,
            env=replace(
                self.cfg.env,
                broker="fyers",
                values={"GAMMA_ACCOUNTS_JSON": json.dumps(disabled_accounts)},
            ),
        )
        mgr_dis = load_account_session_manager(cfg_dis)
        self.assertEqual(len(mgr_dis.list_accounts()), 1)
        self.assertIsNone(mgr_dis.active_account())
        self.assertIsNotNone(mgr_dis.history_adapter())

    def test_dict_format_with_accounts_key(self) -> None:
        import json
        from dataclasses import replace
        from gammad.broker.config_loader import load_account_session_manager

        payload = {
            "accounts": [
                {"account_id": "acc_dict", "broker": "mock_broker_dict", "is_active": True}
            ]
        }
        cfg = replace(self.cfg, env=replace(self.cfg.env, values={"GAMMA_ACCOUNTS_JSON": json.dumps(payload)}))
        mgr = load_account_session_manager(cfg)
        self.assertEqual(len(mgr.list_accounts()), 1)
        self.assertEqual(mgr.active_account().identity.account_id, "acc_dict")

    def test_runtime_history_config_accessor(self) -> None:
        from gammad.app import Runtime

        mgr = AccountSessionManager()
        ident = AccountIdentity("mock_broker", "ACC_01")
        adapter = DummyMockAdapter("mock_adapter")
        mgr.register_account(ident, adapter)

        h_cfg = HistoryProviderConfig("mock_hist_p")
        mgr.register_history_provider(h_cfg, adapter)

        rt = Runtime(self.cfg, session_manager=mgr)
        self.rts.append(rt)
        self.assertEqual(rt.history_config().provider_name, "mock_hist_p")

    def test_env_file_ingestion(self) -> None:
        import json
        from gammad.config import load_settings
        from gammad.broker.config_loader import load_account_session_manager, load_history_provider_config

        repo_dir = Path(self.test_dir) / "test_repo"
        repo_dir.mkdir(parents=True, exist_ok=True)
        (repo_dir / "config").mkdir(parents=True, exist_ok=True)
        (repo_dir / "config" / "strategy.yaml").write_text("spec_version: '1.0'\n", encoding="utf-8")

        accounts = [
            {"account_id": "ACC_ENV_1", "broker": "mock_broker_env", "is_active": True}
        ]
        env_content = (
            f"GAMMA_ACCOUNTS_JSON='{json.dumps(accounts)}'\n"
            "GAMMA_HISTORY_PROVIDER=mock_hist_env\n"
            "GAMMA_HISTORY_BARS_LIMIT=1200\n"
            "GAMMA_HISTORY_DAILY_BUDGET=8\n"
            "GAMMA_ENFORCE_ACLS=1\n"
        )
        (repo_dir / ".env").write_text(env_content, encoding="utf-8")

        cfg = load_settings(repo_dir)
        self.assertIn("GAMMA_ACCOUNTS_JSON", cfg.env.values)
        self.assertEqual(cfg.env.values.get("GAMMA_HISTORY_PROVIDER"), "mock_hist_env")
        self.assertEqual(cfg.env.values.get("GAMMA_ENFORCE_ACLS"), "1")

        mgr = load_account_session_manager(cfg)
        self.assertEqual(len(mgr.list_accounts()), 1)
        self.assertEqual(mgr.active_account().identity.account_id, "ACC_ENV_1")

        h_cfg = load_history_provider_config(cfg)
        self.assertEqual(h_cfg.provider_name, "mock_hist_env")
        self.assertEqual(h_cfg.daily_bars_limit, 1200)
        self.assertEqual(h_cfg.daily_request_budget, 8)

    def test_accounts_json_file_path(self) -> None:
        import json
        from dataclasses import replace
        from gammad.broker.config_loader import load_account_session_manager

        accounts_file = Path(self.test_dir) / "accounts.json"
        accounts = [
            {"account_id": "ACC_FILE_1", "broker": "mock_broker_file", "is_active": True}
        ]
        accounts_file.write_text(json.dumps(accounts), encoding="utf-8")

        cfg = replace(self.cfg, env=replace(self.cfg.env, values={"GAMMA_ACCOUNTS_JSON": str(accounts_file)}))
        mgr = load_account_session_manager(cfg)
        self.assertEqual(len(mgr.list_accounts()), 1)
        self.assertEqual(mgr.active_account().identity.account_id, "ACC_FILE_1")

    def test_account_credentials_retention(self) -> None:
        import json
        from dataclasses import replace
        from gammad.broker.account import AccountCredentials
        from gammad.broker.config_loader import load_account_session_manager

        accounts = [
            {"account_id": "ACC_SEC_1", "broker": "mock_broker_sec", "credentials": {"SECRET_KEY": "val1"}}
        ]
        cfg = replace(self.cfg, env=replace(self.cfg.env, values={"GAMMA_ACCOUNTS_JSON": json.dumps(accounts)}))
        mgr = load_account_session_manager(cfg)
        session = mgr.active_account()

        self.assertIsNotNone(session.account_credentials)
        self.assertIsInstance(session.account_credentials, AccountCredentials)
        self.assertEqual(session.account_credentials.get("SECRET_KEY"), "val1")
        self.assertEqual(session.credentials.get("SECRET_KEY"), "val1")

    def test_paper_broker_mode_a_and_mode_b(self) -> None:
        import json
        from dataclasses import replace
        from gammad.broker.config_loader import load_account_session_manager

        # Mode A with broker="paper"
        cfg_paper_a = replace(self.cfg, env=replace(self.cfg.env, broker="paper", values={}))
        mgr_a = load_account_session_manager(cfg_paper_a)
        self.assertIsNotNone(mgr_a.active_account())
        self.assertEqual(mgr_a.active_account().identity.broker, "paper")

        # Mode B with broker="paper"
        accounts = [{"account_id": "PAPER_01", "broker": "paper", "is_active": True}]
        cfg_paper_b = replace(self.cfg, env=replace(self.cfg.env, values={"GAMMA_ACCOUNTS_JSON": json.dumps(accounts)}))
        mgr_b = load_account_session_manager(cfg_paper_b)
        self.assertIsNotNone(mgr_b.active_account())
        self.assertEqual(mgr_b.active_account().identity.broker, "paper")

    def test_decoupled_history_provider_independent_broker(self) -> None:
        import json
        from dataclasses import replace
        from gammad.broker.config_loader import load_account_session_manager

        # Trading account is mock_broker_trade, but history is mock_broker_history
        accounts = [{"account_id": "TRADE_01", "broker": "mock_broker_trade", "is_active": True}]
        cfg = replace(
            self.cfg,
            env=replace(
                self.cfg.env,
                values={
                    "GAMMA_ACCOUNTS_JSON": json.dumps(accounts),
                    "GAMMA_HISTORY_PROVIDER": "mock_broker_history",
                },
            ),
        )
        mgr = load_account_session_manager(cfg)
        self.assertEqual(mgr.active_account().identity.broker, "mock_broker_trade")
        self.assertEqual(mgr.history_config().provider_name, "mock_broker_history")
        hist_adapter = mgr.history_adapter()
        self.assertEqual(getattr(hist_adapter, "broker", ""), "mock_broker_history")


class TestNoDefaultBroker(unittest.TestCase):
    def setUp(self) -> None:
        from dataclasses import replace
        from gammad.config import settings

        self.test_dir = tempfile.mkdtemp(prefix="gamma_nobroker_")
        base_cfg = settings()
        test_paths = replace(
            base_cfg.paths,
            data_dir=Path(self.test_dir),
            logs_dir=Path(self.test_dir),
            db_path=Path(self.test_dir) / "test_gamma.db",
        )
        self.cfg = replace(
            base_cfg,
            paths=test_paths,
            env=replace(base_cfg.env, broker="", values={}),
        )
        self.rts: list[Any] = []

    def tearDown(self) -> None:
        for rt in self.rts:
            if getattr(rt, "conn", None) is not None:
                try:
                    rt.conn.close()
                except Exception:
                    pass
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_registry_resolve_never_defaults_in_any_edition(self) -> None:
        from gammad.broker import registry

        for ed in ("client", "owner"):
            with self.subTest(edition=ed):
                with self.assertRaises(registry.RegistryError):
                    registry.resolve(None, edition=ed)
                with self.assertRaises(registry.RegistryError):
                    registry.resolve("", edition=ed)
                with self.assertRaises(registry.RegistryError):
                    registry.resolve("   ", edition=ed)

    def test_config_loader_and_adapter_do_not_default_without_broker(self) -> None:
        from gammad.broker import registry
        from gammad.broker.config_loader import (
            load_account_session_manager,
            load_history_provider_config,
            resolve_configured_adapter,
        )

        self.assertIsNone(load_history_provider_config(self.cfg))
        mgr = load_account_session_manager(self.cfg)
        self.assertEqual(mgr.list_accounts(), [])
        self.assertIsNone(mgr.active_account())
        with self.assertRaises(registry.RegistryError):
            resolve_configured_adapter(self.cfg)

    def test_runtime_disables_engines_and_returns_none_broker_without_config(self) -> None:
        import asyncio
        from datetime import date
        from unittest.mock import patch
        from gammad.app import Runtime
        from gammad.api import server
        from gammad.auth.token_watch import TokenWatcher

        rt = Runtime(self.cfg, scanners_089=True)
        self.rts.append(rt)

        self.assertIsNone(rt.broker_entry)
        self.assertIsNone(rt.scanner)
        self.assertFalse(rt.engines_allowed)
        self.assertEqual(rt.broker_name(), "none")

        # ensure_day / load_day must not instantiate or arm engines (V1 or s089) when engines_allowed is False
        d = date(2026, 11, 13)
        rt.conn.execute(
            "INSERT INTO runs (run_id, kind, status, started_ist, finished_ist, note, spec_version)"
            " VALUES (?, ?, ?, ?, ?, ?, ?)",
            ("r_none", "build_watchlists", "ok", "2026-11-13T09:20:00+05:30", "2026-11-13T09:20:05+05:30", "", "1.0"),
        )
        rt.conn.execute(
            "INSERT INTO watchlist (trade_date, list_name, expiry_date, security_id, symbol,"
            " underlying_security_id, strike, option_type, tag, oi, candidates_n, ref_price,"
            " zone_edge, run_id, spec_version, lot_size)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (d.isoformat(), "NEAR_WEEKLY_PE", "2026-11-26", 12345, "INFY", 1594, 1500.0, "PE",
             "MAIN", 1000, 1, 1500.0, 1490.0, "r_none", "1.0", 400),
        )
        rt.conn.commit()
        with patch.object(rt, "is_trading_day", return_value=True):
            asyncio.run(rt.ensure_day(d))
        self.assertEqual(len(rt.pool), 0)
        self.assertIsNotNone(rt.s089)
        self.assertEqual(len(rt.s089.states), 0)

        # Server status/broker helpers return None in both editions without an active broker
        for ed in ("client", "owner"):
            with patch("gammad.edition.edition", return_value=ed), patch("gammad.api.server.is_client", return_value=(ed == "client")):
                self.assertIsNone(server.broker_entry(rt))
                self.assertIsNone(server.broker_block(rt))
                tp = server.token_page(rt)
                self.assertIsNone(tp["broker"])

        watcher = TokenWatcher(None, rt.conn, rt.router, self.cfg, entry=None)
        self.assertEqual(watcher.label, "Broker")
        self.assertEqual(watcher._client_id, "")
        self.assertEqual(watcher._token, "")
        self.assertEqual(watcher._creds, {})

    def test_reload_credentials_without_broker_ignores_dhan_fallback(self) -> None:
        from dataclasses import replace
        from unittest.mock import MagicMock
        from gammad.app import Runtime

        adapter = DummyMockAdapter("no_broker_adapter")
        adapter.set_token = MagicMock()
        adapter.set_credentials = MagicMock()
        feed = MagicMock()
        fresh_cfg = replace(
            self.cfg,
            env=replace(
                self.cfg.env,
                broker="",
                dhan_client_id="legacy_cid",
                dhan_access_token="legacy_tok",
                values={},
            ),
        )
        rt = Runtime(self.cfg, adapter=adapter, reload_settings=lambda: fresh_cfg)
        self.rts.append(rt)
        rt.feed = feed

        import warnings
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            rt.reload_credentials()
        self.assertFalse(
            [w for w in caught if issubclass(w.category, RuntimeWarning)],
            "reload_credentials() must not emit unawaited-coroutine RuntimeWarnings",
        )
        adapter.set_token.assert_not_called()
        adapter.set_credentials.assert_not_called()
        feed.reload_credentials.assert_not_called()

    def test_token_watcher_entry_none_ignores_dhan_settings(self) -> None:
        from dataclasses import replace
        from unittest.mock import MagicMock
        from gammad.app import Runtime
        from gammad.auth.token_watch import TokenRejected, TokenWatcher

        rt = Runtime(self.cfg)
        self.rts.append(rt)

        dhan_env_cfg = replace(
            self.cfg,
            env=replace(
                self.cfg.env,
                broker="",
                dhan_client_id="legacy_cid",
                dhan_access_token="legacy_tok",
                values={"DHAN_CLIENT_ID": "legacy_cid", "DHAN_ACCESS_TOKEN": "legacy_tok"},
            ),
        )
        fake_adapter = DummyMockAdapter("fake")
        fake_adapter.client_id = "adapter_cid"
        fake_adapter.set_credentials = MagicMock()
        watcher = TokenWatcher(fake_adapter, rt.conn, rt.router, dhan_env_cfg, entry=None)
        self.assertEqual(watcher.label, "Broker")
        self.assertEqual(watcher.token_key, "")
        self.assertEqual(watcher._client_id, "")
        self.assertEqual(watcher._token, "")
        self.assertEqual(watcher._creds, {})
        self.assertIsNone(watcher.token_view()["token_key"])
        self.assertEqual(watcher.evaluate(None), ("unknown", None, None, "no broker token configured"))
        with self.assertRaises(TokenRejected):
            asyncio.run(watcher.update_token("tok_new"))
        fake_adapter.set_credentials.assert_not_called()

    def test_history_provider_none_treated_as_unconfigured(self) -> None:
        import json
        from dataclasses import replace
        from gammad.broker.config_loader import load_account_session_manager, load_history_provider_config

        for raw in ("none", "NONE", " None "):
            for broker_val in ("", "fyers"):
                with self.subTest(raw=raw, broker=broker_val):
                    cfg_none = replace(
                        self.cfg,
                        env=replace(
                            self.cfg.env,
                            broker=broker_val,
                            values={"GAMMA_HISTORY_PROVIDER": raw},
                        ),
                    )
                    self.assertIsNone(load_history_provider_config(cfg_none))
                    if not broker_val:
                        mgr = load_account_session_manager(cfg_none)
                        self.assertEqual(mgr.list_accounts(), [])
                        with self.assertRaises(SessionRoutingError):
                            mgr.history_adapter()

        # Mode B with explicit GAMMA_HISTORY_PROVIDER="none" must also leave history unregistered
        accounts = [{"account_id": "ACC_1", "broker": "mock_broker_a", "is_active": True}]
        cfg_mode_b_none = replace(
            self.cfg,
            env=replace(
                self.cfg.env,
                broker="",
                values={
                    "GAMMA_ACCOUNTS_JSON": json.dumps(accounts),
                    "GAMMA_HISTORY_PROVIDER": "none",
                },
            ),
        )
        mgr_b = load_account_session_manager(cfg_mode_b_none)
        self.assertIsNotNone(mgr_b.active_account())
        with self.assertRaises(SessionRoutingError):
            mgr_b.history_adapter()

    def test_mode_a_unset_broker_registers_explicit_history_provider(self) -> None:
        from dataclasses import replace
        from gammad.broker.config_loader import (
            MockBrokerAdapter,
            UnauthenticatedBrokerAdapter,
            load_account_session_manager,
            resolve_configured_adapter,
        )
        from gammad.broker.definedge import DefinedgeAdapter

        # 1. Definedge history with valid key while BROKER=""
        cfg_ok = replace(
            self.cfg,
            env=replace(
                self.cfg.env,
                broker="",
                values={
                    "GAMMA_HISTORY_PROVIDER": "definedge",
                    "DEFINEDGE_API_SESSION_KEY": "sess_123",
                },
            ),
        )
        mgr_ok = load_account_session_manager(cfg_ok)
        self.assertEqual(mgr_ok.list_accounts(), [])
        self.assertIsNone(mgr_ok.active_account())
        self.assertIsNotNone(mgr_ok.history_config())
        self.assertEqual(mgr_ok.history_config().provider_name, "definedge")
        hist_ok = mgr_ok.history_adapter()
        self.assertIsInstance(hist_ok, DefinedgeAdapter)
        hist_ok.close()
        resolved = resolve_configured_adapter(cfg_ok, for_history=True)
        self.assertIsInstance(resolved, DefinedgeAdapter)
        resolved.close()

        # 2. Definedge history missing key while BROKER="" -> UnauthenticatedBrokerAdapter
        cfg_missing = replace(
            self.cfg,
            env=replace(
                self.cfg.env,
                broker="",
                values={"GAMMA_HISTORY_PROVIDER": "definedge"},
            ),
        )
        with self.assertLogs("gammad.broker.config_loader", level="ERROR"):
            mgr_missing = load_account_session_manager(cfg_missing)
        self.assertEqual(mgr_missing.list_accounts(), [])
        self.assertIsNone(mgr_missing.active_account())
        self.assertEqual(mgr_missing.history_config().provider_name, "definedge")
        self.assertIsInstance(mgr_missing.history_adapter(), UnauthenticatedBrokerAdapter)
        self.assertEqual(mgr_missing.history_adapter().broker, "definedge")

        # 3. Synthetic mock history provider while BROKER=""
        cfg_mock = replace(
            self.cfg,
            env=replace(
                self.cfg.env,
                broker="",
                values={"GAMMA_HISTORY_PROVIDER": "mock_hist"},
            ),
        )
        mgr_mock = load_account_session_manager(cfg_mock)
        self.assertEqual(mgr_mock.list_accounts(), [])
        self.assertEqual(mgr_mock.history_config().provider_name, "mock_hist")
        self.assertIsInstance(mgr_mock.history_adapter(), MockBrokerAdapter)

        # 4. Synthetic mock/paper primary broker with decoupled history provider
        for primary_broker in ("mock_broker", "paper"):
            cfg_mock_decoupled = replace(
                self.cfg,
                env=replace(
                    self.cfg.env,
                    broker=primary_broker,
                    values={
                        "GAMMA_HISTORY_PROVIDER": "definedge",
                        "DEFINEDGE_API_SESSION_KEY": "sess_456",
                    },
                ),
            )
            mgr_decoupled = load_account_session_manager(cfg_mock_decoupled)
            self.assertEqual(mgr_decoupled.active_account().identity.broker, primary_broker)
            self.assertEqual(mgr_decoupled.history_config().provider_name, "definedge")
            hist_decoupled = mgr_decoupled.history_adapter()
            self.assertIsInstance(hist_decoupled, DefinedgeAdapter)
            hist_decoupled.close()

        cfg_paper_mock_hist = replace(
            self.cfg,
            env=replace(
                self.cfg.env,
                broker="paper",
                values={"GAMMA_HISTORY_PROVIDER": "mock_hist"},
            ),
        )
        mgr_paper_hist = load_account_session_manager(cfg_paper_mock_hist)
        self.assertEqual(mgr_paper_hist.active_account().identity.broker, "paper")
        self.assertEqual(mgr_paper_hist.history_config().provider_name, "mock_hist")
        self.assertIsInstance(mgr_paper_hist.history_adapter(), MockBrokerAdapter)
        self.assertEqual(mgr_paper_hist.history_adapter().broker, "mock_hist")


if __name__ == "__main__":
    unittest.main()
