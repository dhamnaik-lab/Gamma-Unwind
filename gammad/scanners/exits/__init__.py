"""Pure bar-driven exit primitives; all durable memory is in the contract."""

from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import date, datetime, time, timedelta, timezone

from gammad.scanners import contract as sc

EPS = 1e-9
BAR_MS = 300_000
IST = timezone(timedelta(hours=5, minutes=30))
E1Cut = Callable[[date, int], bool]
Step = tuple[sc.PositionState, tuple[sc.ExitEvent, ...]]


def ist_date(ms: int) -> date:
    return datetime.fromtimestamp(ms / 1000, IST).date()


def ist_time(ms: int) -> time:
    return datetime.fromtimestamp(ms / 1000, IST).time()


def is_stale(last_trade_ms: int | None, bar_start_ms: int) -> bool:
    # Port of gammad/record/stale.py:17-20 (D274); no config import in this pure package.
    return (last_trade_ms is not None and last_trade_ms > 0
            and ist_date(last_trade_ms) < ist_date(bar_start_ms))


@dataclass(frozen=True)
class LadderBar:
    start_ms: int
    open: float
    high: float
    low: float
    close: float
    oi: int | None = None
    atr14: float | None = None
    stock_close: float | None = None
    stock_closes: tuple[float, ...] = ()
    last_trade_ms: int | None = None


def trim(legs: tuple[int, ...]) -> tuple[int, ...]:
    while legs and legs[-1] == 0:
        legs = legs[:-1]
    return legs


def initial(signal, fill_ms, fill_px, lots, legs, targets, zone_edge, prem_stop, r_premium):
    return sc.PositionState(
        position_key=signal.key, scanner=signal.scanner, list_name=signal.list_name,
        security_id=signal.security_id, underlying_security_id=signal.underlying_security_id,
        symbol=signal.symbol, expiry_date=signal.expiry_date, strike=signal.strike,
        option_type=signal.option_type, lot_size=signal.lot_size, entry_date=ist_date(fill_ms),
        entry_ms=fill_ms, entry_px=fill_px, lots_total=lots, lots_open=lots, leg_lots=legs,
        targets=targets, targets_hit=(), stop_kind=signal.stop_kind, zone_edge=zone_edge,
        prem_stop=prem_stop, r_premium=r_premium, trail_peak=None, pending_exit=None,
        pending_decided_ms=None, oi_at_entry=None, oi_low_since_entry=None, reached_1r=False,
        nights_carried=0, realized_rs=0.0, closed=False, updated_ms=fill_ms - BAR_MS,
    )


def event(state, bar, kind, *, px=None, lots=0, leg=None, new_stop=None, reason="", at_open=False):
    decided = state.pending_decided_ms
    ts = decided - BAR_MS if decided is not None else bar.start_ms
    if decided is None:
        decided = bar.start_ms if at_open else bar.start_ms + BAR_MS
    return sc.ExitEvent(state.position_key, kind, leg, ts, decided, px, lots, new_stop, reason)


def _calc_charges(charges, entry_px: float, exit_px: float, qty: float, orders: int) -> float:
    if charges is None:
        return 0.0
    try:
        return float(charges(entry_px, exit_px, qty, orders=orders))
    except TypeError:
        return float(charges(entry_px, exit_px, qty))


def book(state, bar, kind, lots, px, charges, *, leg=None, reason="", at_open=False):
    emitted = event(state, bar, kind, px=px, lots=lots, leg=leg, reason=reason, at_open=at_open)
    qty = lots * state.lot_size
    orders = 2 if state.lots_open == state.lots_total else 1
    cost = _calc_charges(charges, state.entry_px, px, qty, orders)
    remaining = state.lots_open - lots
    state = replace(state, lots_open=remaining, closed=remaining == 0,
                    realized_rs=state.realized_rs + (px - state.entry_px) * qty - cost)
    return state, emitted


def finish(state, bar, kind, charges):
    state, emitted = book(state, bar, kind, state.lots_open, bar.open, charges,
                          leg="RUNNER" if kind == "TRAIL" else None, at_open=True)
    return replace(state, pending_exit=None, pending_decided_ms=None), (emitted,)


def decide(state, bar, kind):
    return replace(state, pending_exit=kind, pending_decided_ms=bar.start_ms + BAR_MS)


def expiry_kind(state):
    return state.pending_exit if state.pending_exit in ("STOP", "ZONE_STOP", "TRAIL", "OI_RETURN") else "E1_EXIT"
