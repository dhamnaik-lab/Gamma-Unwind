"""Pure one-slot stop-first walk. Close decisions wait for the next supplied open."""
from dataclasses import dataclass, replace
from datetime import datetime

from .exits import BAR_MS, IST, Event, ExitState, build, history_of, trailing
from .states import CLOSED_STATE_OF_KIND
from .stops import decide_stop


@dataclass(frozen=True)
class Slot:
    stock_bar: tuple | None
    option_bar: tuple
    h1_closes: tuple | None
    last_of_session: bool
    expiry_day: bool
    daily_close: float | None


@dataclass(frozen=True)
class V1Position:
    entry_px: float
    entry_ts: int
    side: str
    resolved: object
    exit_pick: object
    expiry: object
    exit_state: ExitState | None = None
    state: str = "V1R_OPEN"
    remaining: float = 1.
    pending: Event | None = None
    last_ts: int | None = None
    last_px: float | None = None

    @classmethod
    def open(cls, entry_px, entry_ts, side, resolved, exit_pick, expiry, history):
        if resolved.level is None and resolved.cap is None:
            raise ValueError("no_stop_level")
        pos = cls(entry_px, entry_ts, side, resolved, exit_pick, expiry)
        return replace(pos, exit_state=build(exit_pick).begin(pos, history_of(history)))


def _finish(pos, event, px, ts):
    ev = replace(event, price=float(px), ts_ms=int(ts), fraction=pos.remaining)
    return replace(pos, state=CLOSED_STATE_OF_KIND[event.kind], remaining=0., pending=None), [ev]


def close_now(pos, px, ts, kind):
    if kind not in ("V1R_EOD", "V1R_SETTLED"):
        raise ValueError("clock kind must be V1R_EOD or V1R_SETTLED")
    if pos.remaining <= 0:
        return pos, []
    return _finish(pos, pos.pending or Event(kind, px, ts, decided_close_ms=ts), px, ts)


def on_daily_close(pos, close, ts):
    if pos.remaining <= 0 or pos.pending is not None or pos.resolved.candle != "daily":
        return pos, []
    reason = decide_stop(pos.side, pos.resolved, "daily", stock_close=None, hour_end=False,
                         daily_close=close, option_close=float("inf"), entry_px=pos.entry_px)
    if reason:
        return replace(pos, pending=Event("V1R_STOP", None, ts, decided_close_ms=ts)), []
    return pos, []


def step(pos, slot):
    b = slot.option_bar
    ts, end = int(b[0]), int(b[0]) + BAR_MS
    if slot.stock_bar is not None and slot.stock_bar[0] != ts:
        raise ValueError("stock and option slots must align")
    if pos.remaining <= 0 or (pos.last_ts is not None and ts <= pos.last_ts):
        return pos, []
    if pos.pending is not None:
        if ts < pos.pending.decided_close_ms:
            return pos, []
        return _finish(pos, pos.pending, b[1], ts)
    pos = replace(pos, last_ts=ts, last_px=b[4])
    events = []
    terminal = slot.last_of_session and (slot.expiry_day or pos.exit_pick.form == "intraday")
    if ts > pos.entry_ts:
        at = datetime.fromtimestamp(end / 1000, IST)
        hour = slot.last_of_session or (10 <= at.hour <= 15 and at.minute == 15 and at.second == 0)
        values = [slot.stock_bar[4]] if slot.stock_bar is not None else []
        if pos.resolved.candle == "daily":
            values = [None]  # daily stops read their own close, including option-only slots
        if pos.resolved.candle == "1h" and slot.h1_closes is not None:
            values = [x[4] if isinstance(x, (tuple, list)) else x for x in slot.h1_closes]
            hour = bool(values)
        reason = None
        # Level wins over cap even when several hourly closes arrived in this slot.
        level_only = replace(pos.resolved, cap=None)
        for value in values:
            reason = decide_stop(pos.side, level_only, pos.resolved.candle, stock_close=value,
                                 hour_end=hour, daily_close=slot.daily_close if slot.last_of_session else None,
                                 option_close=b[4], entry_px=pos.entry_px)
            if reason:
                break
        if reason is None and pos.resolved.cap is not None:
            reason = decide_stop(pos.side, replace(pos.resolved, level=None), "5m", stock_close=None,
                                 hour_end=False, daily_close=None, option_close=b[4], entry_px=pos.entry_px)
        if reason:
            ev = Event("V1R_STOP" if reason == "stop_level" else "V1R_CAP", None, end, decided_close_ms=end)
            if terminal:
                return _finish(pos, ev, b[4], end)
            return replace(pos, pending=ev), []
        state, proposed = build(pos.exit_pick).advance(pos, pos.exit_state, slot)
        pos = replace(pos, exit_state=state)
        for ev in proposed:
            if ev.kind == "V1R_HALF":
                ev = replace(ev, fraction=min(ev.fraction, pos.remaining))
                events.append(ev)
                pos = replace(pos, remaining=pos.remaining - ev.fraction, state="V1R_HALF")
            elif ev.kind == "V1R_TARGET" or terminal:
                pos, closed = _finish(pos, ev, ev.price if ev.price is not None else b[4],
                                      ev.ts_ms if ev.price is not None else end)
                return pos, events + closed
            else:
                return replace(pos, pending=ev), events
        if pos.state != "V1R_HALF" and trailing(state):
            pos = replace(pos, state="V1R_TRAILING")
    elif ts == pos.entry_ts:
        # CX-111's hist[:-1]: the entry bar is history for the first active slot.
        hist = pos.exit_state.history
        hist = replace(hist, option=(*hist.option, tuple(b)),
                       stock=hist.stock if slot.stock_bar is None else (*hist.stock, tuple(slot.stock_bar)))
        pos = replace(pos, exit_state=build(pos.exit_pick).begin(pos, hist))
    if slot.last_of_session:
        if pos.exit_pick.form == "intraday":
            pos, closed = close_now(pos, b[4], end, "V1R_EOD")
            return pos, events + closed
        if slot.expiry_day:
            pos, closed = close_now(pos, b[4], end, "V1R_SETTLED")
            return pos, events + closed
    return pos, events
