"""Bar-fed entry engine with injected gates and a per-stock daily entry book."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable, Sequence
from dataclasses import replace
from datetime import date
from typing import Protocol, runtime_checkable

from gammad.core import sessions
from gammad.scanners import contract as sc
from gammad.scanners.v1r import entry as e
from gammad.scanners.v1r.entry import OptBar, StockBar, V1rParams

DEFAULT_PARAMS = V1rParams()


@runtime_checkable
class V1Gates(Protocol):
    def ban_block(self, symbol: str, day: date) -> str | None: ...
    def in_window(self, bar_start_ms: int) -> bool: ...
    def floor_ok(self, volume: int, lot_size: int, premium: float) -> bool: ...
    def expiry_cutoff_ok(self, list_name: str, dte: int, bar_start_ms: int) -> bool: ...


@runtime_checkable
class RiskPerLotFn(Protocol):
    def __call__(
        self, row: sc.ListRow, signal_px: float, stop_kind: str, stop_level: float, ts_ms: int
    ) -> tuple[float | None, bool]: ...


class V1rEngine:
    def __init__(
        self,
        *,
        edition: str,
        gates: V1Gates,
        results: sc.ResultsView,
        zones: sc.ZoneView,
        dte_fn: Callable[[date, date], int | None],
        risk_fn: RiskPerLotFn | None = None,
        params: V1rParams = DEFAULT_PARAMS,
        eras: Sequence[sessions.Era] | None = None,
    ) -> None:
        if edition not in ("owner", "client"):
            raise ValueError("unknown edition")
        self.edition, self.gates, self.results, self.zones = edition, gates, results, zones
        self.dte_fn, self.risk_fn, self.p, self.eras = dte_fn, risk_fn, params, eras
        self._options: dict[int, e.OptSeries] = defaultdict(lambda: e.OptSeries(params))
        self._stocks: dict[int, e.StockSeries] = defaultdict(e.StockSeries)
        self._rows: dict[str, sc.ListRow] = {}
        self._book: dict[str, tuple[sc.EntrySignal, bool]] = {}
        self._carried: dict[str, sc.EntrySignal] = {}
        self._day: date | None = None

    @staticmethod
    def _key(row):
        return f"{row.list_name}|{row.security_id}"

    def start_day(self, trade_date: date, rows: Sequence[sc.ListRow]) -> None:
        if any(r.trade_date != trade_date for r in rows):
            raise ValueError("row date differs from engine day")
        self._day = trade_date
        self._rows = {}
        self._book = {}
        self._carried = {}
        self.refresh_rows(rows)

    def refresh_rows(self, rows: Sequence[sc.ListRow]) -> None:
        if self._day is None or any(r.trade_date != self._day for r in rows):
            raise ValueError("row date differs from engine day")
        for row in rows:
            if self.edition == "client" and row.list_name not in sc.V1N_LISTS_CLIENT:
                continue
            key = self._key(row)
            if key in self._rows:
                self._rows[key] = replace(self._rows[key], oi=row.oi, zone_edge=row.zone_edge, rank=row.rank)
            else:
                self._rows[key] = row

    def seed_option(self, security_id: int, bars: Sequence[OptBar]) -> None:
        for bar in bars:
            self._options[security_id].push(bar)

    def seed_stock(self, underlying_security_id: int, bars: Sequence[StockBar]) -> None:
        for bar in bars:
            self._stocks[underlying_security_id].push(bar)

    def on_stock_bar(self, underlying_security_id: int, bar: StockBar) -> None:
        self._stocks[underlying_security_id].push(bar)

    @staticmethod
    def _day_index(bars, day, ts=None):
        for j in range(len(bars) - 1, -1, -1):
            stamp = e.ist_stamp(bars[j].start_ms).date()
            if stamp < day:
                break
            if stamp == day and (ts is None or bars[j].start_ms <= ts):
                return j
        return -1

    def _stock_at(self, row, ts):
        ser = self._stocks.get(row.underlying_security_id, e.StockSeries())
        return ser, self._day_index(ser.bars, e.ist_stamp(ts).date(), ts)

    def _a(self, row, ser, i):
        peak, oi = ser.day_peak[i], ser.bars[i].oi
        if row.list_name == sc.CARRY:
            return e.a_carry_from(peak, oi, self.p, row.carry_build_low_oi, row.carry_build_peak_oi)
        return e.a_fresh_from(peak, oi, self.p)

    def _c(self, ser, i):
        lo = max(0, i - 2)
        return e.c_from([b.volume for b in ser.bars[lo:i + 1]], ser.sma[lo:i + 1], self.p)

    def _confirms(self, ser, j, side, open_at):
        p, bars = self.p, ser.bars
        return j >= p.stock_back_bars and e.confirms_from(
            bars[j].close, bars[j - p.stock_back_bars].close, ser.vwap(j, open_at), side, p)

    def on_option_bar(self, security_id: int, bar: OptBar) -> sc.EntrySignal | None:
        ser = self._options[security_id]
        bars = ser.bars
        if not ser.push(bar) or e.ist_stamp(bar.start_ms).date() != self._day:
            return None
        i, p = len(bars) - 1, self.p
        for row in self._rows.values():
            if row.security_id != security_id:
                continue
            dte = self.dte_fn(self._day, row.expiry_date)
            if not self.gates.in_window(bar.start_ms) or not self.gates.floor_ok(
                bar.volume, row.lot_size, bar.close
            ):
                continue
            if row.list_name in (sc.EXPIRY_CE, sc.EXPIRY_PE) and (
                dte is None or not self.gates.expiry_cutoff_ok(row.list_name, dte, bar.start_ms)
            ):
                continue
            atr = ser.atr
            c2, c3 = e.cond_c2(bars, i, p, atr), e.cond_c3(bars, i, p)
            if not (
                self._a(row, ser, i) and e.cond_b(bars, i, p) and self._c(ser, i) and e.cond_d(c2, c3)
            ):
                continue
            stock_ser, j = self._stock_at(row, bar.start_ms)
            stocks = stock_ser.bars
            open_at = sessions.era_for(self._day, self.eras).cm_open
            if (
                j < 0
                or stocks[j].start_ms != bar.start_ms
                or not self._confirms(stock_ser, j, row.option_type, open_at)
            ):
                continue
            entries = [
                (s, closed)
                for s, closed in self._book.values()
                if s.underlying_security_id == row.underlying_security_id
            ]
            if len(entries) >= p.max_entries_per_stock:
                continue
            if any(not closed for _, closed in entries) or any(
                s.underlying_security_id == row.underlying_security_id for s in self._carried.values()
            ):
                continue
            ban = self.gates.ban_block(row.symbol, self._day)
            signal_ms = bar.start_ms + 300_000
            edge = self.zones.near_edge(row.symbol, row.option_type, stocks[j].close, signal_ms)
            kind, level = (
                (sc.STOP_ZONE, edge) if edge is not None else (sc.STOP_PCT, bar.close * (1 - p.pct_stop))
            )
            risk, estimate = (
                self.risk_fn(row, bar.close, kind, level, signal_ms) if self.risk_fn else (None, False)
            )
            peak = ser.day_peak[i]
            signal = sc.EntrySignal(
                scanner=sc.V1N,
                list_name=row.list_name,
                trade_date=self._day,
                security_id=row.security_id,
                underlying_security_id=row.underlying_security_id,
                symbol=row.symbol,
                expiry_date=row.expiry_date,
                strike=row.strike,
                option_type=row.option_type,
                lot_size=row.lot_size,
                tick_size=row.tick_size,
                entry_no=len(entries) + 1,
                signal_bar_ms=bar.start_ms,
                signal_ms=signal_ms,
                signal_px=bar.close,
                fill_rule=sc.FILL_NEXT_OPEN,
                stop_kind=kind,
                stop_level=level,
                risk_per_lot=risk,
                risk_is_estimate=estimate,
                lots=None,
                tags=row.tags,
                results_today=row.results_today,
                ban_reason=ban,
                metrics={
                    "oi_drop": (peak - bar.oi) / peak,
                    "oi_peak_today": float(peak),
                    "vol_sma": ser.sma[i],
                    "prev_atr": atr[i - 1] if i else None,
                    "c2": float(c2),
                    "c3": float(c3),
                    "stock_vwap": stock_ser.vwap(j, open_at),
                    "stock_close_3ago": stocks[j - p.stock_back_bars].close,
                    "dte": float(dte) if dte is not None else None,
                },
            )
            if not signal.blocked:
                self.restore_entry(signal, False)
            return signal
        return None

    def unbook(self, position_key: str) -> bool:
        if position_key in self._book:
            del self._book[position_key]
            return True
        if position_key in self._carried:
            del self._carried[position_key]
            return True
        return False

    def restore_carried(self, signal: sc.EntrySignal) -> None:
        if (self._day is None or signal.scanner != sc.V1N or signal.blocked
                or signal.trade_date >= self._day):
            raise ValueError("carried signal must be unblocked V1N from an earlier day")
        self._carried[signal.key] = signal

    def mark_closed(self, position_key: str) -> None:
        self._carried.pop(position_key, None)
        if position_key in self._book:
            signal, _ = self._book[position_key]
            self._book[position_key] = signal, True

    def restore_entry(self, signal: sc.EntrySignal, closed: bool) -> None:
        if signal.scanner == sc.V1N and signal.trade_date == self._day and not signal.blocked:
            self._book[signal.key] = signal, closed

    # list_rows / live_snapshot are read from API worker threads while the loop thread mutates the engine
    # dicts (T-642, D702 Q3): each reader iterates a C-level .copy() taken once, never the live dict.
    def list_rows(self) -> tuple[sc.ListRow, ...]:
        entered = {s.security_id for s, _ in self._book.copy().values()}
        return tuple(
            replace(r, state=sc.ENTERED) if r.security_id in entered else r for r in self._rows.copy().values()
        )

    def _checks(self, row, ser, i):
        checks = dict.fromkeys(("chk_a", "chk_b", "chk_c", "chk_d", "chk_stock"))
        if i < 0:
            return checks
        p, bars = self.p, ser.bars
        if (
            bars[i].oi is not None
            and ser.day_peak[i]
            and (row.list_name != sc.CARRY or row.carry_build_low_oi is not None)
        ):
            checks["chk_a"] = float(self._a(row, ser, i))
        if i >= 1:
            checks["chk_b"] = float(e.cond_b(bars, i, p))
        if bars[i].volume is not None and any(
            ser.sma[j] is not None for j in range(max(0, i - 2), i + 1)
        ):
            checks["chk_c"] = float(self._c(ser, i))
        atr = ser.atr
        c2, c3 = e.cond_c2(bars, i, p, atr), e.cond_c3(bars, i, p)
        if c2 or c3 or (i >= p.atr_len and i >= p.nr_lookback + 1):
            checks["chk_d"] = float(e.cond_d(c2, c3))
        stock_ser, j = self._stock_at(row, bars[i].start_ms)
        stocks = stock_ser.bars
        open_at = sessions.era_for(self._day, self.eras).cm_open
        if (
            j >= p.stock_back_bars
            and e.ist_stamp(stocks[j].start_ms).date() == self._day
            and stock_ser.vwap(j, open_at) is not None
        ):
            checks["chk_stock"] = float(self._confirms(stock_ser, j, row.option_type, open_at))
        return checks

    def live_snapshot(self) -> dict[str, dict[str, float | None]]:
        snapshot = {}
        for key, row in self._rows.copy().items():
            ser = self._options.get(row.security_id, e.OptSeries(self.p))
            bars = ser.bars
            i = self._day_index(bars, self._day)
            oi = bars[i].oi if i >= 0 else None
            peak = ser.day_peak[i] if i >= 0 else None
            fresh = row.list_name != sc.CARRY
            off = (peak - oi) / peak * 100 if fresh and oi is not None and peak and peak > 0 else None
            stocks = self._stocks.get(row.underlying_security_id, e.StockSeries()).bars
            j = self._day_index(stocks, self._day)
            close = stocks[j].close if j >= 0 else None
            zone = None
            if row.zone_edge is not None and close is not None and close != 0:
                zone = (row.zone_edge - close) / close * 100 * (1 if row.option_type == "CE" else -1)
            low, high = row.carry_build_low_oi, row.carry_build_peak_oi
            unwound = None
            if not fresh and low is not None and high is not None and high > low and oi is not None:
                unwound = (high - oi) / (high - low) * 100
            checks = self._checks(row, ser, i)
            met = float(sum(value == 1.0 for value in checks.values()))
            snapshot[key] = {
                "oi_off_high_pct": off,
                "to_zone_pct": zone,
                "build_unwound_pct": unwound,
                **checks,
                "checks_met": met,
                "near_entry": float(met >= 4 and checks["chk_a"] == 1.0),
            }
        return snapshot
