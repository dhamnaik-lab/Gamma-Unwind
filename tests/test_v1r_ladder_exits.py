"""Phase 2/4 regression tests for V1N exits, LiveLadderBook, ZONE_STOP fallback & session slots."""

from __future__ import annotations

import asyncio
from datetime import date, datetime, time
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest
from unittest.mock import MagicMock, patch
from zoneinfo import ZoneInfo

from gammad.app import Runtime, plan_slots
from gammad.alerts import s089_dispatch as dispatch
from gammad.broker.base import Candle
from gammad.config import ScheduleCfg, settings
from gammad.core import session_clock, v1_hours
from gammad.scanners import contract as sc
from gammad.scanners.exits import LadderBar, _calc_charges, book
from gammad.scanners.live import book as live_book, charges as live_charges
from gammad.scanners.live.hub import LiveLadderBook, ScannerHub
from gammad.store import db as store_db
from gammad.watch.adapter import Bar as EngineBar
from gammad.watch.bars import BarBuilder, BarClose, Bar as WatchBar

IST = ZoneInfo("Asia/Kolkata")


def _ms(d: date, hm: time) -> int:
    return int(datetime.combine(d, hm, IST).timestamp() * 1000)


class TestV1nLadderExits(unittest.TestCase):
    def setUp(self) -> None:
        self.day = date(2026, 11, 13)
        self.expiry = date(2026, 11, 26)

    def _state(self, *, lots_total: int = 4, lots_open: int = 4, realized_rs: float = 0.0) -> sc.PositionState:
        return sc.PositionState(
            position_key=f"V1N|45001|{_ms(self.day, time(9, 25))}",
            scanner="V1N",
            list_name="BREAKOUT-CE",
            security_id=45001,
            underlying_security_id=2885,
            symbol="RELIANCE26NOV1300CE",
            expiry_date=self.expiry,
            strike=1300.0,
            option_type="CE",
            lot_size=100,
            entry_date=self.day,
            entry_ms=_ms(self.day, time(9, 30)),
            entry_px=100.0,
            lots_total=lots_total,
            lots_open=lots_open,
            leg_lots=(1, 1, 1, 1),
            targets=(110.0, 120.0, 130.0, 140.0),
            targets_hit=(),
            stop_kind=sc.STOP_ZONE,
            zone_edge=1250.0,
            prem_stop=80.0,
            r_premium=20.0,
            trail_peak=100.0,
            pending_exit=None,
            pending_decided_ms=None,
            oi_at_entry=1000,
            oi_low_since_entry=950,
            reached_1r=False,
            nights_carried=0,
            realized_rs=realized_rs,
            closed=False,
            updated_ms=_ms(self.day, time(9, 30)),
        )

    def test_calc_charges_supports_legacy_3arg_and_orders_kwarg(self) -> None:
        seen_orders: list[int] = []

        def fn_orders(e: float, x: float, q: float, *, orders: int = 2) -> float:
            seen_orders.append(orders)
            return 20.0 * orders

        def fn_legacy(e: float, x: float, q: float) -> float:
            return 42.0

        self.assertEqual(_calc_charges(fn_orders, 100.0, 110.0, 100, 1), 20.0)
        self.assertEqual(_calc_charges(fn_orders, 100.0, 110.0, 100, 2), 40.0)
        self.assertEqual(seen_orders, [1, 2])
        self.assertEqual(_calc_charges(fn_legacy, 100.0, 110.0, 100, 1), 42.0)

    def test_book_charges_two_orders_on_first_exit_and_one_on_subsequent(self) -> None:
        orders_used: list[int] = []

        def charges_spy(e: float, x: float, q: float, *, orders: int = 2) -> float:
            orders_used.append(orders)
            return float(orders * 20)

        bar = LadderBar(_ms(self.day, time(9, 35)), 110.0, 115.0, 108.0, 112.0, 1000)
        st0 = self._state(lots_total=4, lots_open=4)
        st1, ev1 = book(st0, bar, "TARGET", 1, 110.0, charges_spy, leg="T1")
        st2, ev2 = book(st1, bar, "TARGET", 1, 120.0, charges_spy, leg="T2")
        self.assertEqual(orders_used, [2, 1])
        self.assertEqual(ev1.lots, 1)
        self.assertEqual(ev2.lots, 1)

    def test_live_ladder_book_client_skips_risk_and_event_rs_uses_one_order_after_t1(self) -> None:
        with tempfile.TemporaryDirectory(prefix="gamma_hub_") as tmp:
            conn = store_db.connect(Path(tmp) / "hub.db")
            try:
                store_db.migrate(conn)
                lb_client = LiveLadderBook(conn, capital_rs=None, edition="client")
                self.assertEqual(lb_client._risk_snapshot, (0, {}))

                with patch.object(live_book, "load_capital", return_value=100_000.0):
                    hub = ScannerHub(
                        conn,
                        edition="owner",
                        engine=MagicMock(),
                        e1_cut=lambda d, ms: False,
                        sender=MagicMock(),
                        chat_ids={},
                        clock_ms=lambda: 0,
                        publish=lambda *a: None,
                        spawn=lambda c, name=None: None,
                        spec_version="0.8.9",
                        ltp=lambda sid: 100.0,
                        costs=live_charges.charges,
                    )
                st_after_t1 = self._state(lots_total=4, lots_open=3)
                hub.states[(st_after_t1.position_key, "OWN")] = st_after_t1
                ev_t2 = sc.ExitEvent(st_after_t1.position_key, "TARGET", "T2",
                                     _ms(self.day, time(9, 40)), _ms(self.day, time(9, 45)), 120.0, 1, None, "T2")
                expected = (120.0 - 100.0) * 100 - live_charges.charges(100.0, 120.0, 100, orders=1)
                self.assertAlmostEqual(hub.event_rs(ev_t2), expected, places=4)
            finally:
                conn.close()

    def test_hub_alerts_filters_known_exits_and_missing_signal_ids(self) -> None:
        with tempfile.TemporaryDirectory(prefix="gamma_hub_al_") as tmp:
            conn = store_db.connect(Path(tmp) / "alerts.db")
            try:
                store_db.migrate(conn)
                hub = ScannerHub(
                    conn,
                    edition="client",
                    engine=MagicMock(),
                    e1_cut=lambda d, ms: False,
                    sender=MagicMock(),
                    chat_ids={},
                    clock_ms=lambda: 0,
                    publish=lambda *a: None,
                    spawn=lambda c, name=None: None,
                    spec_version="0.8.9",
                    ltp=lambda sid: 100.0,
                    costs=live_charges.charges,
                )
                sig_missing = sc.EntrySignal(
                    scanner="V1N",
                    list_name="BREAKOUT-CE",
                    trade_date=self.day,
                    security_id=45001,
                    underlying_security_id=2885,
                    symbol="RELIANCE26NOV1300CE",
                    expiry_date=self.expiry,
                    strike=1300.0,
                    option_type="CE",
                    lot_size=100,
                    tick_size=0.05,
                    entry_no=1,
                    signal_bar_ms=_ms(self.day, time(9, 25)),
                    signal_ms=_ms(self.day, time(9, 30)),
                    signal_px=100.0,
                    fill_rule=sc.FILL_NEXT_OPEN,
                    stop_kind=sc.STOP_ZONE,
                    stop_level=1250.0,
                    risk_per_lot=2000.0,
                    risk_is_estimate=False,
                    lots=4,
                    tags=(),
                    results_today=False,
                    ban_reason=None,
                    alert_sent_ms=5000,
                )
                sig_sent = sc.EntrySignal(
                    scanner="V1N",
                    list_name="BREAKDOWN-PE",
                    trade_date=self.day,
                    security_id=45002,
                    underlying_security_id=2885,
                    symbol="RELIANCE26NOV1300PE",
                    expiry_date=self.expiry,
                    strike=1300.0,
                    option_type="PE",
                    lot_size=100,
                    tick_size=0.05,
                    entry_no=1,
                    signal_bar_ms=_ms(self.day, time(9, 25)),
                    signal_ms=_ms(self.day, time(9, 30)),
                    signal_px=100.0,
                    fill_rule=sc.FILL_NEXT_OPEN,
                    stop_kind=sc.STOP_ZONE,
                    stop_level=1250.0,
                    risk_per_lot=2000.0,
                    risk_is_estimate=False,
                    lots=4,
                    tags=(),
                    results_today=False,
                    ban_reason=None,
                    alert_sent_ms=5000,
                )
                hub._signals[sig_missing.key] = sig_missing
                hub._signals[sig_sent.key] = sig_sent
                hub.signal_ids[sig_sent.key] = 42
                ev = sc.ExitEvent(sig_sent.key, "TARGET", "T1", _ms(self.day, time(9, 35)),
                                  _ms(self.day, time(9, 40)), 110.0, 2, None, "T1", alert_sent_ms=9000)
                hub._events[(sig_sent.key, "GROUP")] = [ev]
                hub._deliveries.extend([
                    (42, dispatch.GROUP, 9000, dispatch.SENT),  # exit delivery recorded earlier must be filtered out
                    (42, dispatch.GROUP, 5000, dispatch.SENT),  # actual entry delivery
                ])
                rows = hub.alerts(self.day)
                by_key = {(r["key"], r["kind"]): r for r in rows}
                self.assertEqual(by_key[(sig_missing.key, "signal")]["chats"], [])
                self.assertEqual(by_key[(sig_sent.key, "signal")]["chats"], [{"chat": dispatch.GROUP, "sent_ms": 5000}])
                self.assertEqual(by_key[(sig_sent.key, "TARGET")]["chats"], [{"chat": dispatch.GROUP, "sent_ms": 9000}])
            finally:
                conn.close()

    def test_zone_stop_post_cas_ltp_and_prior_bar_fallback_and_session_slots(self) -> None:
        slots = plan_slots(ScheduleCfg(mark_levels=("09:20", "15:35", "16:00")), day=self.day)
        mark_1535 = next(s for s in slots if s.at == "15:35")
        mark_1600 = next(s for s in slots if s.at == "16:00")
        self.assertEqual(mark_1535.mark_ref, "ltp")  # 2026 F&O close is 15:40
        self.assertEqual(mark_1600.mark_ref, "close")
        self.assertEqual(session_clock.fo_close_hm(self.day), "15:40")
        self.assertEqual(session_clock.fo_close_hm(date(2025, 11, 13)), "15:30")

        # Runtime._on_bar_close post-15:15 CAS LTP fallback & prior closed bar fallback
        with tempfile.TemporaryDirectory(prefix="gamma_zstop_") as tmp:
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
                rt.is_trading_day = lambda _d: True
                self.assertTrue(rt.market_open(datetime.combine(self.day, time(10, 0), IST)))
                self.assertFalse(rt.market_open(datetime.combine(self.day, time(16, 0), IST)))

                rt.trade_date = self.day
                rt._089_wait = {}
                rt.meta[45001] = SimpleNamespace(underlying_security_id=2885, tick_size=0.05)
                rt.series[45001] = MagicMock()
                eb = EngineBar(_ms(self.day, time(15, 20)), 100, 105, 99, 102, 1000, 5000)
                rt.carry.bar = MagicMock(return_value=eb)
                rt.series[45001].closed.return_value = [eb]
                rt.builders[45001] = MagicMock()
                rt._oi = MagicMock(return_value=(5000, 5500))
                rt._record_bar = MagicMock()
                rt.tracker = MagicMock()
                rt.v1desk = None
                rt.pool = MagicMock()
                rt.pool.is_carry.return_value = False
                rt.pool.get_or_create.return_value = MagicMock(**{"on_bar_close.return_value": None})
                rt.sw_any3_pool = None
                rt.shadow_pools = {}

                ub = BarBuilder(2885, self.day, (5,), session_close=time(15, 30))
                ub.last_ltp = 1234.5
                rt.und_builders[2885] = ub
                rt.und_bars[2885] = [Candle(_ms(self.day, time(15, 10)), 1250, 1255, 1245, 1248.0, 100, None)]

                captured: list[LadderBar] = []
                rt.s089 = MagicMock()
                rt.s089.engine.p = SimpleNamespace(atr_len=14)
                rt.s089.option_bar.side_effect = lambda _sid, _opt, lb, entry=True: captured.append(lb)

                bc_post = BarClose(45001, 5, WatchBar(_ms(self.day, time(15, 20)), 5,
                                                      100, 105, 99, 102, 1000, 5000, 1), "tick")
                with (
                    patch.object(Runtime, "engines_allowed", new=property(lambda _self: True)),
                    patch.object(v1_hours, "cash_close_ms", return_value=_ms(self.day, time(15, 15))),
                ):
                    rt._on_bar_close(bc_post, recv_ms=bc_post.bar.end_ms)
                    self.assertEqual(captured[-1].stock_closes, (1234.5,))

                    # Earlier bar missing exact slot falls back to prior closed underlying bar
                    bc_mid = BarClose(45001, 5, WatchBar(_ms(self.day, time(15, 12)), 5,
                                                         100, 105, 99, 102, 1000, 5000, 1), "tick")
                    rt._on_bar_close(bc_mid, recv_ms=bc_mid.bar.end_ms)
                    self.assertEqual(captured[-1].stock_closes, (1248.0,))
            finally:
                rt.conn.close()


if __name__ == "__main__":
    unittest.main()
