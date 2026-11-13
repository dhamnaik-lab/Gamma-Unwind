"""Phase 1/2/4 regression tests for WatermarkStore, safe_put_tick, BarBuilder volume, and day_bar."""

from __future__ import annotations

import asyncio
from datetime import date, datetime, time
from pathlib import Path
import tempfile
import unittest
from unittest.mock import MagicMock
from zoneinfo import ZoneInfo

from gammad.broker.durable_watermark import WatermarkHold, WatermarkStore
from gammad.core import day_bar
from gammad.watch.bars import BarBuilder
from gammad.watch.feed import Tick, safe_put_tick

IST = ZoneInfo("Asia/Kolkata")


def _ms(d: date, hm: time, extra_ms: int = 0) -> int:
    return int(datetime.combine(d, hm, IST).timestamp() * 1000) + extra_ms


class TestFeedWatermarkBars(unittest.TestCase):
    def test_watermark_accepts_eq_recv_and_sub_ms_and_rejects_dup_or_regression(self) -> None:
        with tempfile.TemporaryDirectory(prefix="gamma_wm_") as tmp:
            db_path = Path(tmp) / "watermark.sqlite"
            store = WatermarkStore(db_path, broker="fyers", account_scope="DEFAULT")
            try:
                ts1 = 1_760_000_000_123
                t1 = Tick(ts1, ts1, 45001, "NSE_FNO", 100.0, 10, 50, 50, 95.0, None, None)
                # ts == recv and ts % 1000 != 0 MUST be accepted
                self.assertTrue(store.claim(t1))
                # Duplicate via memory fast cache & persistence returns False
                self.assertFalse(store.claim(Tick(ts1, ts1 + 5, 45001, "NSE_FNO", 100.0, 10, 50, 50, 95.0, None, None)))
                # Unverifiable zero LTT raises WatermarkHold
                with self.assertRaises(WatermarkHold):
                    store.claim(Tick(0, ts1 + 6, 45001, "NSE_FNO", 100.0, 10, 50, 50, 95.0, None, None))
                # Regression rejected even on reload
                store2 = WatermarkStore(db_path, broker="fyers", account_scope="DEFAULT")
                try:
                    self.assertFalse(store2.claim(Tick(ts1 - 1, ts1 + 10, 45001, "NSE_FNO", 100.0, 10, 50, 50, 95.0, None, None)))
                    self.assertTrue(store2.claim(Tick(ts1 + 50, ts1 + 50, 45001, "NSE_FNO", 101.0, 11, 50, 50, 95.0, None, None)))
                finally:
                    store2.close()
            finally:
                store.close()
            # Scoped connections must not leave db locked on Windows after close/calls
            db_path.unlink()

    def test_safe_put_tick_absorbs_watermark_hold_and_queue_full(self) -> None:
        q: asyncio.Queue[Tick] = asyncio.Queue(maxsize=1)
        tick = Tick(1_760_000_000_000, 1_760_000_000_000, 45001, "NSE_FNO", 100.0, 10, 50, 50, 95.0, None, None)

        held_out = MagicMock()
        held_out.put_nowait.side_effect = WatermarkHold("dup")
        ok_wm, reason_wm = safe_put_tick(held_out, tick)
        self.assertFalse(ok_wm)
        self.assertEqual(reason_wm, "dup")

        q.put_nowait(tick)
        ok_full, reason_full = safe_put_tick(q, tick)
        self.assertFalse(ok_full)
        self.assertEqual(reason_full, "queue_full")

    def test_bar_builder_preserves_volume_from_unpriced_ticks(self) -> None:
        d = date(2026, 11, 13)
        b = BarBuilder(45001, d, (5,), session_close=time(15, 30))
        # Unpriced cumulative volume warmup ticks before any LTP arrives
        b.on_tick(Tick(_ms(d, time(9, 15, 1)), _ms(d, time(9, 15, 1)), 45001, "NSE_FNO",
                       None, 1000, 500, None, None, None, None))
        b.on_tick(Tick(_ms(d, time(9, 15, 2)), _ms(d, time(9, 15, 2)), 45001, "NSE_FNO",
                       None, 1250, 500, None, None, None, None))
        # First priced tick opens bar and MUST include the 250 delta volume accumulated while unpriced
        b.on_tick(Tick(_ms(d, time(9, 15, 3)), _ms(d, time(9, 15, 3)), 45001, "NSE_FNO",
                       100.0, 1300, 500, None, None, None, None))
        forming = b.forming_bar(5)
        self.assertIsNotNone(forming)
        self.assertEqual(forming.volume, 300)

    def test_day_bar_uses_session_clock_fo_window(self) -> None:
        d = date(2026, 11, 13)
        self.assertTrue(day_bar._in_session(_ms(d, time(9, 20)), d))
        self.assertFalse(day_bar._in_session(_ms(d, time(16, 0)), d))


if __name__ == "__main__":
    unittest.main()
