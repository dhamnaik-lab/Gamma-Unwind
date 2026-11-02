"""R1.9 Full-Lifecycle Synthetic Integration Test.

Validates the complete synthetic runtime path:
1. Independent historical backfill via decoupled history provider.
2. Streaming option ticks gated by DurableWatermark (LTT progression & regression guard).
3. V1 Signal generation, AlertRouter dispatch, and paper position state machine (T1 -> T2).
4. Outbox write-ahead persistence and ambiguous event crash-quarantine.

Runs offline with Python 3.13. Zero live broker calls, zero real orders.
"""

from __future__ import annotations

import asyncio
import os
import shutil
import sqlite3
import tempfile
import unittest
from contextlib import closing
from dataclasses import replace
from datetime import date, datetime
from pathlib import Path
from typing import Any

from gammad.alerts.router import AlertRouter
from gammad.app import Runtime
from gammad.broker.account import (
    AccountIdentity,
    AccountSessionManager,
    HistoryProviderConfig,
)
from gammad.broker.base import BrokerAdapter, Candle, ChainRow, Instrument, TokenHealth
from gammad.broker.durable_watermark import (
    DurableTickQueue,
    WatermarkHold,
    WatermarkStore,
    wrap_feed_queue,
)
from gammad.config import settings
from gammad.trade.tracker import ST_PENDING, TradeTracker
from gammad.watch.feed_protocol import Tick
from gammad.watch.types import ContractMeta, Signal, SignalContext


class SyntheticHistoryAdapter(BrokerAdapter):
    """Synthetic adapter returning mock historical candles and chains."""

    def __init__(self) -> None:
        self.provider_id = "synthetic_history_v1"

    def instruments(self) -> list[Instrument]:
        return []

    def daily_candles(
        self,
        security_id: int,
        exchange_segment: str,
        from_date: date,
        to_date: date,
        instrument: str = "EQUITY",
    ) -> list[Candle]:
        # Return 2,000 synthetic bars
        base_ts = 1600000000000
        candles = []
        for i in range(2000):
            candles.append(
                Candle(
                    ts_ms=base_ts + i * 86_400_000,
                    open=100.0 + (i % 20),
                    high=105.0 + (i % 20),
                    low=98.0 + (i % 20),
                    close=102.0 + (i % 20),
                    volume=50000.0 + i * 10,
                    oi=100000.0,
                )
            )
        return candles

    def intraday(self, *args: Any, **kwargs: Any) -> list[Candle]:
        return []

    def expiries(self, *args: Any, **kwargs: Any) -> list[date]:
        return [date(2026, 11, 26)]

    def option_chain(self, *args: Any, **kwargs: Any) -> list[ChainRow]:
        return []

    def ltp(self, *args: Any, **kwargs: Any) -> dict[int, float]:
        return {45001: 125.0}

    def token_health(self) -> TokenHealth:
        return TokenHealth(valid=True, validity_ist="2026-11-02 23:59:59", error=None)


class SyntheticExecutionAdapter(BrokerAdapter):
    """Synthetic execution adapter for paper trading."""

    def __init__(self, name: str = "synthetic_exec") -> None:
        self.name = name

    def instruments(self) -> list[Instrument]:
        return [
            Instrument(
                security_id=45001,
                symbol="NIFTY26NOV24000CE",
                exchange_segment="NSE_FNO",
                instrument_type="OPTSTK",
                underlying_symbol="NIFTY",
                expiry_date=date(2026, 11, 26),
                strike=24000.0,
                option_type="CE",
                lot_size=50,
                tick_size=0.05,
            )
        ]

    def daily_candles(self, *args: Any, **kwargs: Any) -> list[Candle]:
        return []

    def intraday(self, *args: Any, **kwargs: Any) -> list[Candle]:
        return []

    def expiries(self, *args: Any, **kwargs: Any) -> list[date]:
        return [date(2026, 11, 26)]

    def option_chain(self, *args: Any, **kwargs: Any) -> list[ChainRow]:
        return []

    def ltp(self, *args: Any, **kwargs: Any) -> dict[int, float]:
        return {45001: 125.0}

    def token_health(self) -> TokenHealth:
        return TokenHealth(valid=True, validity_ist="2026-11-02 23:59:59", error=None)


class TestR19RuntimeLifecycle(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.test_dir = tempfile.mkdtemp(prefix="gamma_lifecycle_")
        self.db_path = Path(self.test_dir) / "lifecycle_gamma.db"
        self.ledger_path = Path(self.test_dir) / "feed_watermark.sqlite"

        # Prepare isolated configuration
        base_cfg = settings()
        test_paths = replace(
            base_cfg.paths,
            data_dir=Path(self.test_dir),
            logs_dir=Path(self.test_dir),
            db_path=self.db_path,
        )
        self.cfg = replace(base_cfg, paths=test_paths)

        # Multi-Account Session Manager
        self.mgr = AccountSessionManager()

        # Decoupled Historical Data Provider
        self.hist_cfg = HistoryProviderConfig(
            provider_name="synthetic_history",
            daily_bars_limit=2000,
            daily_request_budget=6,
        )
        self.hist_adapter = SyntheticHistoryAdapter()
        self.mgr.register_history_provider(self.hist_cfg, self.hist_adapter)

        # Execution Broker Account
        self.exec_identity = AccountIdentity(broker="mock_broker", account_id="ACC_EXEC_01")
        self.exec_adapter = SyntheticExecutionAdapter("mock_exec")
        self.mgr.register_account(self.exec_identity, self.exec_adapter)

        # Runtime Instance
        self.rt = Runtime(
            self.cfg,
            session_manager=self.mgr,
            watermark_ledger_path=self.ledger_path,
        )

    def tearDown(self) -> None:
        if hasattr(self, "rt") and hasattr(self.rt, "conn"):
            self.rt.conn.close()
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def _make_tick(
        self,
        ltt_sec: int,
        recv_sec: int,
        ltp: float,
        volume: int = 1000,
        oi: int = 50000,
        sec_id: int = 45001,
    ) -> Tick:
        return Tick(
            ts_ms=ltt_sec * 1000,
            recv_ms=recv_sec * 1000,
            security_id=sec_id,
            segment="NSE_FNO",
            ltp=ltp,
            volume_day=volume,
            oi=oi,
            prev_oi=48000,
            prev_close=110.0,
            oi_day_high=55000,
            oi_day_low=45000,
        )

    async def test_01_decoupled_historical_backfill(self) -> None:
        """Verify historical data routing is independent of the trading account adapter."""
        # 1. Historical adapter provides 2,000 candles
        candles = self.rt.history_adapter().daily_candles(
            security_id=45001,
            exchange_segment="NSE_FNO",
            from_date=date(2020, 1, 1),
            to_date=date(2026, 11, 2),
        )
        self.assertEqual(len(candles), 2000)
        self.assertEqual(self.rt.history_adapter().provider_id, "synthetic_history_v1")

        # 2. Execution adapter remains distinct
        self.assertEqual(self.rt.adapter().name, "mock_exec")

    async def test_02_durable_watermark_stream_and_regression_guard(self) -> None:
        """Verify options tick streaming advances the durable ledger and rejects regressing ticks."""
        queue: asyncio.Queue[Tick] = asyncio.Queue()
        store = WatermarkStore(
            self.ledger_path,
            broker=self.exec_identity.broker,
            account_scope=self.exec_identity.account_id,
        )
        feed_queue = wrap_feed_queue(queue, store)

        # Ingest increasing ticks
        t1 = self._make_tick(ltt_sec=1700005000, recv_sec=1700005001, ltp=120.0)
        t2 = self._make_tick(ltt_sec=1700005010, recv_sec=1700005011, ltp=121.5)
        feed_queue.put_nowait(t1)
        feed_queue.put_nowait(t2)

        self.assertEqual(queue.qsize(), 2)
        self.assertEqual(feed_queue.accepted, 2)

        # Check ledger persistence
        with closing(sqlite3.connect(str(self.ledger_path))) as con:
            row = con.execute(
                "SELECT ltt_ms FROM feed_watermarks WHERE segment='NSE_FNO' AND security_id=45001"
            ).fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(row[0], 1700005010 * 1000)

        # Ingest regressing tick (e.g. from feed restart / out-of-order packet)
        t_stale = self._make_tick(ltt_sec=1700005005, recv_sec=1700005015, ltp=120.5)
        with self.assertRaises(WatermarkHold):
            feed_queue.put_nowait(t_stale)

        self.assertEqual(feed_queue.refused, 1)
        self.assertEqual(queue.qsize(), 2)  # Queue was NOT polluted with stale tick

    async def test_03_signal_generation_and_paper_position_lifecycle(self) -> None:
        """Verify V1 signal entry triggers trade hook and transitions from T1 to T2."""
        # 10:15 AM IST on 2026-11-02 (trading hours, after the 09:20 list build)
        now_ms = 1793594700000

        meta = ContractMeta(
            symbol="NIFTY",
            strike=24000.0,
            option_type="CE",
            list_name="BREAKOUT-CE",
            expiry_date="2026-11-26",
            lot_size=50,
            underlying_security_id=1,
            tick_size=0.05,
            tag="HOI-1",
        )

        # In client edition, V1 arming signal is E2 which client routes as entry
        signal = Signal(
            kind="E2",
            strong=True,
            ts_ms=now_ms,
            security_id=45001,
            ltp=125.0,
            oi_drop_pct=12.5,
            vol_ratio=2.5,
            consol_lo=120.0,
            consol_hi=124.0,
            consol_mid=122.0,
            sl=118.0,
            reason="synthetic V1 test breakout",
            latency_ms=10,
        )

        ctx = SignalContext(
            hold="swing",
            spot=24050.0,
            zone_edge=24000.0,
            gate_reasons=(),
            gate_unknown=(),
        )

        # 1. Dispatch signal through AlertRouter
        sig_id = await self.rt.router.on_signal(signal, meta, ctx)
        self.assertGreater(sig_id, 0)

        # 2. Verify signal persisted in signals table
        sig_row = self.rt.conn.execute("SELECT id, ltp, kind FROM signals WHERE id=?", (sig_id,)).fetchone()
        self.assertIsNotNone(sig_row)
        self.assertEqual(sig_row[1], 125.0)

        # 3. Verify paper trade was opened by trade_hook
        trade_row = self.rt.conn.execute(
            "SELECT id, state, entry, sl, t1, t2 FROM trades WHERE security_id=45001"
        ).fetchone()
        self.assertIsNotNone(trade_row)
        tid, state, entry_px, sl, t1, t2 = trade_row
        self.assertEqual(state, "OPEN")
        self.assertEqual(entry_px, 125.0)
        self.assertGreater(t1, entry_px)
        self.assertGreater(t2, t1)

        # 4. Stream tick crossing Target 1 (T1)
        t1_px = t1 + 0.5
        events_fired = self.rt.tracker.on_tick(45001, t1_px, now_ms + 10_000)
        self.assertGreaterEqual(events_fired, 1)

        # Verify transition to T1_TRAIL
        state_after_t1 = self.rt.conn.execute("SELECT state, sl FROM trades WHERE id=?", (tid,)).fetchone()
        self.assertEqual(state_after_t1[0], "T1_TRAIL")
        self.assertEqual(state_after_t1[1], entry_px)  # Stop moved to breakeven/entry

        # Verify T1 event committed in trade_events
        t1_event = self.rt.conn.execute(
            "SELECT kind, price FROM trade_events WHERE trade_id=? AND kind='T1'", (tid,)
        ).fetchone()
        self.assertIsNotNone(t1_event)

        # 5. Stream tick crossing Target 2 (T2)
        t2_px = t2 + 1.0
        events_t2 = self.rt.tracker.on_tick(45001, t2_px, now_ms + 20_000)
        self.assertGreaterEqual(events_t2, 1)

        # Verify trade closed at T2
        closed_row = self.rt.conn.execute("SELECT state, exit_px FROM trades WHERE id=?", (tid,)).fetchone()
        self.assertEqual(closed_row[0], "CLOSED_T2")
        self.assertEqual(closed_row[1], t2_px)

    async def test_04_ambiguous_outbox_crash_and_quarantine(self) -> None:
        """Verify ambiguous pending outbound event survives restart and is preserved without duplication."""
        now_ms = 1793594700000

        # Insert a mock open trade
        cur = self.rt.conn.execute(
            """
            INSERT INTO trades (
                entry_signal_id, security_id, list_name, expiry_date, option_type,
                kind, hold, state, entry_date, entry_ts_ms, entry, sl, t1, t2,
                lot_size, armed, updated_ms, spec_version
            ) VALUES (
                100, 45002, 'BREAKOUT-CE', '2026-11-26', 'CE',
                'E1', 'swing', 'OPEN', '2026-11-02', ?, 150.0, 140.0, 165.0, 180.0,
                50, 0, ?, 'client'
            )
            """,
            (now_ms, now_ms),
        )
        tid = cur.lastrowid

        # Insert an ambiguous outbound follow-up event in 'pending' status
        self.rt.conn.execute(
            """
            INSERT INTO trade_events (
                trade_id, kind, ts_ms, price, status, reply_to, fill_px
            ) VALUES (?, 'T1', ?, 165.0, ?, NULL, 165.0)
            """,
            (tid, now_ms + 5000, ST_PENDING),
        )
        self.rt.conn.commit()

        # Simulate process crash by closing tracker and reloading on fresh instance
        new_tracker = TradeTracker(
            self.rt.conn,
            self.rt.router,
            spec_version=self.cfg.spec_version,
            now=lambda: datetime.fromtimestamp(now_ms / 1000),
            wall_ms=lambda: now_ms + 100_000,
            gates_cfg=self.cfg.gates,
        )
        new_tracker.load(datetime.fromtimestamp(now_ms / 1000))

        # Check pending event survived restart in database
        ev_row = self.rt.conn.execute(
            "SELECT id, kind, status FROM trade_events WHERE trade_id=? AND kind='T1'", (tid,)
        ).fetchone()
        self.assertIsNotNone(ev_row)
        self.assertEqual(ev_row[1], "T1")
        self.assertEqual(ev_row[2], ST_PENDING)

        # Verify UNIQUE(trade_id, kind) constraint prevents accidental duplicate event creation
        with self.assertRaises(sqlite3.IntegrityError):
            self.rt.conn.execute(
                """
                INSERT INTO trade_events (
                    trade_id, kind, ts_ms, price, status, reply_to, fill_px
                ) VALUES (?, 'T1', ?, 165.0, ?, NULL, 165.0)
                """,
                (tid, now_ms + 6000, ST_PENDING),
            )


if __name__ == "__main__":
    unittest.main()
