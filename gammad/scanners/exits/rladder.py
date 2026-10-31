"""Shared R-ladder, net targets and runner-only overnight carry (D622-D626)."""

from dataclasses import replace
from datetime import time

from gammad.scanners import contract as sc
from gammad.scanners.exits import (
    BAR_MS,
    EPS,
    E1Cut,
    Step,
    book,
    decide,
    event,
    expiry_kind,
    finish,
    initial,
    is_stale,
    ist_date,
    ist_time,
    trim,
)

MAX_RISK_FRAC = 0.20
OI_RETURN_FRAC = 0.03
CARRY_STOP_R = 0.5
MAX_NIGHTS = 2
DECIDE = time(15, 10)
CUT = time(15, 15)


def split_lots(n: int) -> tuple[int, int, int]:
    if n < 1:
        raise ValueError("lots must be positive")
    t1, runner = (n + 1) // 2, n // 4
    return t1, n - t1 - runner, runner


def open_position(signal, *, fill_ms, fill_px, lots, raw_stop: float,
                  charges: sc.ChargesFn) -> sc.PositionState:
    legs = trim(split_lots(lots))
    floor = fill_px * (1 - MAX_RISK_FRAC)
    stop = raw_stop if raw_stop >= floor - EPS else floor
    risk = fill_px - stop
    if risk <= 0:
        raise ValueError("R must be positive")
    qty = lots * signal.lot_size
    targets = tuple(fill_px + r * risk + charges(fill_px, fill_px + r * risk, qty) / qty for r in (1, 2))
    return initial(signal, fill_ms, fill_px, lots, legs, targets, None, stop, risk)


def _carry(state, bar, decided, events):
    """Carry the runner one night: stop = max(0.5R, current stop) (V2-X4 / D624 (1)); appends the CARRY event."""
    state = replace(state, prem_stop=max(state.entry_px + CARRY_STOP_R * state.r_premium, state.prem_stop),
                    nights_carried=state.nights_carried + 1)
    events.append(replace(event(state, bar, "CARRY", new_stop=state.prem_stop),
                          ts_ms=decided - BAR_MS, decided_ms=decided))
    return state


def step(state, bar, *, e1_cut: E1Cut, charges: sc.ChargesFn) -> Step:
    if state.closed or bar.start_ms <= state.updated_ms:
        return state, ()
    if is_stale(bar.last_trade_ms, bar.start_ms):
        return state, ()
    previous_ms = state.updated_ms
    state = replace(state, updated_ms=bar.start_ms)
    if bar.oi is not None:
        state = replace(state, oi_at_entry=bar.oi if state.oi_at_entry is None else state.oi_at_entry,
                        oi_low_since_entry=bar.oi if state.oi_low_since_entry is None
                        else min(state.oi_low_since_entry, bar.oi))
    if bar.start_ms < state.entry_ms:
        return state, ()
    if e1_cut(state.expiry_date, bar.start_ms):
        return finish(state, bar, expiry_kind(state), charges)
    events = []
    if state.pending_exit == "CARRY":
        nonrunner = state.lots_open - state.leg_lots[2]
        if nonrunner:
            state, emitted = book(state, bar, "DAY_END", nonrunner, bar.open, charges,
                                  reason="carry: 2R leg closed")
            events.append(emitted)
        decided = state.pending_decided_ms
        # D734 (T-621): no 15:15 bar, so the carry fills on a later session's first bar; the runner WAS held
        # overnight, so the night counts and the carry stop applies before this bar's close checks.
        overnight = ist_date(bar.start_ms) > ist_date(decided - BAR_MS)
        state = replace(state, pending_exit=None, pending_decided_ms=None,
                        trail_peak=max(state.trail_peak or bar.close, bar.close))
        if "T2" in state.targets_hit:
            cand = max(state.targets[0], state.trail_peak - state.r_premium)
            if cand > state.prem_stop + EPS:
                state = replace(state, prem_stop=cand)
                events.append(event(state, bar, "STOP_MOVE", new_stop=cand))
        if overnight:
            state = _carry(state, bar, decided, events)
        if bar.close <= state.prem_stop + EPS:
            return decide(state, bar, "TRAIL" if "T2" in state.targets_hit else "STOP"), tuple(events)
        if bar.oi is not None and bar.oi >= state.oi_low_since_entry * (1 + OI_RETURN_FRAC) - EPS:
            return decide(state, bar, "OI_RETURN"), tuple(events)
        if not overnight:
            state = _carry(state, bar, decided, events)
        return state, tuple(events)
    if state.pending_exit is not None:
        return finish(state, bar, state.pending_exit, charges)
    if ist_time(bar.start_ms) >= CUT and not (
        ist_date(previous_ms) == ist_date(bar.start_ms) and ist_time(previous_ms) >= CUT
    ):
        return finish(state, bar, "DAY_END", charges)
    state = replace(state, trail_peak=max(state.trail_peak or bar.close, bar.close))
    for k in range(min(2, len(state.leg_lots))):
        leg = f"T{k + 1}"
        # Carry already closed every non-runner lot at the cut, even if T2 never touched.
        if state.nights_carried or leg in state.targets_hit or bar.high < state.targets[k] - EPS:
            continue
        state, emitted = book(state, bar, "TARGET", state.leg_lots[k], max(state.targets[k], bar.open),
                              charges, leg=leg)
        state = replace(state, targets_hit=(*state.targets_hit, leg), reached_1r=True)
        events.append(emitted)
        if state.closed:
            return state, tuple(events)
        state = replace(state, prem_stop=max(state.prem_stop, state.entry_px if k == 0 else state.targets[0]))
        events.append(event(state, bar, "STOP_MOVE", new_stop=state.prem_stop))
    if "T2" in state.targets_hit:
        cand = max(state.targets[0], state.trail_peak - state.r_premium)
        if cand > state.prem_stop + EPS:
            state = replace(state, prem_stop=cand)
            events.append(event(state, bar, "STOP_MOVE", new_stop=cand))
    if bar.close <= state.prem_stop + EPS:
        state = decide(state, bar, "TRAIL" if "T2" in state.targets_hit else "STOP")
    elif bar.oi is not None and bar.oi >= state.oi_low_since_entry * (1 + OI_RETURN_FRAC) - EPS:
        state = decide(state, bar, "OI_RETURN")
    elif ist_time(bar.start_ms) == DECIDE:
        if state.nights_carried >= MAX_NIGHTS:
            kind = "DAY_END"
        elif state.nights_carried >= 1:
            kind = "CARRY"
        else:
            kind = "CARRY" if (state.reached_1r and bar.oi is not None and state.oi_at_entry is not None
                               and bar.oi < state.oi_at_entry and len(state.leg_lots) == 3
                               and state.lots_open >= state.leg_lots[2]) else "DAY_END"
        state = decide(state, bar, kind)
    return state, tuple(events)
