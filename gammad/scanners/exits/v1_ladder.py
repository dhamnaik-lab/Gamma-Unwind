"""V1 percentage ladder (D618, D630), with close-side stops and a TRB runner."""

from dataclasses import replace

from gammad.scanners import contract as sc
from gammad.scanners.exits import (
    EPS,
    E1Cut,
    LadderBar,  # noqa: F401 - re-export for the CX-201 parity driver (review CX-200 finding 1, D645 (4))
    Step,
    book,
    decide,
    event,
    expiry_kind,
    finish,
    initial,
    is_stale,
    trim,
)

NO_ZONE_STOP_FRAC = 0.40
TRB_ATR_MULT = 3.0


def split_lots(n: int) -> tuple[int, int, int, int]:
    if n < 1:
        raise ValueError("lots must be positive")
    base, remainder = divmod(n, 4)
    return tuple(base + (i < remainder) for i in range(4))


def open_position(signal: sc.EntrySignal, *, fill_ms: int, fill_px: float,
                  lots: int, targets: sc.TargetsConfig) -> sc.PositionState:
    if (signal.scanner != sc.V1N or signal.blocked
            or signal.stop_kind not in (sc.STOP_ZONE, sc.STOP_PCT) or fill_px <= 0):
        raise ValueError("invalid V1 entry")
    zone = signal.stop_kind == sc.STOP_ZONE
    return initial(signal, fill_ms, fill_px, lots, trim(split_lots(lots)),
                   tuple(fill_px * (1 + p / 100) for p in targets.as_tuple()),
                   signal.stop_level if zone else None,
                   None if zone else fill_px * (1 - NO_ZONE_STOP_FRAC), None)


def step(state, bar, *, e1_cut: E1Cut, charges: sc.ChargesFn | None = None) -> Step:
    if state.closed or bar.start_ms <= state.updated_ms:
        return state, ()
    if is_stale(bar.last_trade_ms, bar.start_ms):
        return state, ()
    state = replace(state, updated_ms=bar.start_ms)
    if bar.start_ms < state.entry_ms:
        return state, ()
    if e1_cut(state.expiry_date, bar.start_ms):
        return finish(state, bar, expiry_kind(state), charges)
    if state.pending_exit is not None:
        return finish(state, bar, state.pending_exit, charges)
    state = replace(state, trail_peak=max(state.trail_peak or bar.close, bar.close))
    events = []
    for k, lots in enumerate(state.leg_lots):
        leg = f"T{k + 1}"
        if leg in state.targets_hit or bar.high < state.targets[k] - EPS:
            continue
        reason = f"t1_min={(bar.start_ms - state.entry_ms) // 60000}" if k == 0 else ""
        state, emitted = book(state, bar, "TARGET", lots, max(state.targets[k], bar.open),
                              charges, leg=leg, reason=reason)
        state = replace(state, targets_hit=(*state.targets_hit, leg))
        events.append(emitted)
        if state.closed:
            return state, tuple(events)
        if k < 3:
            stop = state.entry_px if k == 0 else state.targets[k - 1]
            state = replace(state, prem_stop=max(state.prem_stop or stop, stop))
            events.append(event(state, bar, "STOP_MOVE", new_stop=state.prem_stop))
    if "T3" in state.targets_hit and bar.atr14 is not None:
        state = replace(state, prem_stop=max(state.prem_stop, state.trail_peak - TRB_ATR_MULT * bar.atr14))
    zone_closes = bar.stock_closes or ((bar.stock_close,) if bar.stock_close is not None else ())
    if state.zone_edge is not None and any(
        c <= state.zone_edge + EPS if state.option_type == "CE" else c >= state.zone_edge - EPS
        for c in zone_closes
    ):
        state = decide(state, bar, "ZONE_STOP")
    elif state.prem_stop is not None and bar.close <= state.prem_stop + EPS:
        kind = "TRAIL" if "T3" in state.targets_hit and state.prem_stop > state.targets[1] + EPS else "STOP"
        state = decide(state, bar, kind)
    return state, tuple(events)
