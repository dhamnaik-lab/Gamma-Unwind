"""Carried V1 break adapter. Pool construction stays unchanged; the caller supplies day inputs."""
from datetime import datetime, time

from gammad.config import IST
from gammad.core import v1_hours
from gammad.watch.engine import CarryEngine
from gammad.watch.types import Signal

from . import entry


def bar_array(bar):
    if isinstance(bar, (tuple, list)):
        return tuple(bar)
    return (bar.ts_ms, bar.open, bar.high, bar.low, bar.close, bar.volume, bar.oi)


def break_entered(snapshot):
    sw = snapshot.get("sw") if isinstance(snapshot, dict) else None
    return (isinstance(sw, dict) and sw.get("entry_mode") == "v1_break" and sw.get("entered") is True
            and isinstance(sw.get("gate"), dict) and sw["gate"].get("block") is None)


class V1BreakCarryEngine(CarryEngine):
    """No same-bar engine fallback: absent picks or stock history refuse an entry."""

    def configure(self, params, row, *, stock_seed=(), nifty=()):
        self.v1r = params
        self.row = row
        self.stock_seed = stock_seed
        self.nifty = nifty

    def on_bar_close(self, bars, oi_now, oi_peak, *, recv_ms=None, tick_size=None, und_bars=None, arm_ctx=None):
        params = getattr(self, "v1r", None)
        row = getattr(self, "row", {})
        side = row.get("option_type")
        if params is None or side not in ("PE", "CE") or not params.carry_ready(side) or not bars or not und_bars:
            return None
        opts = tuple(map(bar_array, bars))
        ts = opts[-1][0]
        day = datetime.fromtimestamp(ts / 1000, IST).date()
        if ts > v1_hours.last_entry_signal_ms(day):
            return None
        stocks = tuple(b for b in map(bar_array, und_bars) if b[0] <= ts)
        if not stocks or stocks[-1][0] != ts or row.get("zone_edge") is None:
            return None
        cand = entry.Candidate(str(row.get("symbol", "")), side, float(row["zone_edge"]),
                               int(datetime.combine(day, time(9, 15), IST).timestamp() * 1000),
                               "carry", str(self.security_id), self.oi_peak_prior)
        decision = entry.decide(cand, stock_today=stocks, stock_seed=self.stock_seed,
                                option_today=tuple(b for b in opts if datetime.fromtimestamp(b[0]/1000, IST).date() == day),
                                option_seed=tuple(b for b in opts if datetime.fromtimestamp(b[0]/1000, IST).date() < day),
                                rule=entry.rule_for(params, side, kind="carry"), base=entry.base_for(params.base(side)),
                                filters=entry.filters_for(params.filters(side)), nifty=self.nifty, day=day)
        if not decision.entered:
            return None
        peak, prior = self.sw_peak(bars, oi_peak)
        return Signal("E2", False, ts + 300_000, self.security_id, float(opts[-1][4]),
                      float(decision.oi_drop or 0) * 100, 0., 0., 0., 0., 0., "stock close through", 0,
                      {"bar_ts_ms": ts, "sw": {"peak_used": peak, "peak_today": oi_peak,
                       "peak_from_prior": prior, "entry_mode": "v1_break", "entered": True}})
