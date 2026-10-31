"""Immutable close-decision exits, ported from trails and v1trade.

``advance`` returns the next state and events; ``on_bar`` is its event-only view.
History contains pre-entry option/stock seeds and hourly bars. Hourly array bars
are (open_ms, o, h, l, c, close_ms); only confirmed swings can affect a decision.
Hours follow the dated session configuration; callers may supply explicit eras.
"""
from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta, timezone
from math import floor

from gammad.core.sessions import era_for
from gammad.watch.engine import atr_last
from gammad.watch.types import Bar

from .states import REASON_OF_KIND
from .stops import EPS, crossed

IST = timezone(timedelta(hours=5, minutes=30))
BAR_MS = 300_000


@dataclass(frozen=True)
class History:
    option: tuple = ()
    stock: tuple = ()
    h1: tuple = ()
    eras: tuple | None = None


@dataclass(frozen=True)
class Event:
    kind: str
    price: float | None
    ts_ms: int
    fraction: float = 1.
    level: float | None = None
    decided_close_ms: int | None = None

    @property
    def reason(self):
        return REASON_OF_KIND[self.kind]


@dataclass(frozen=True)
class ExitState:
    history: History
    high: float = 0.
    level: float | None = None
    active: bool = False
    reached: bool = False
    booked: bool = False
    days: tuple[date, ...] = ()
    members: tuple = ()


def history_of(history):
    if isinstance(history, History):
        return history
    if isinstance(history, dict):
        return History(**{k: tuple(v) if v is not None else None for k, v in history.items()})
    return History(option=tuple(history))


def _atr(bars):
    return atr_last([Bar(int(b[0]), *map(float, b[1:5]), float(b[5]), 0.) for b in bars], 14)


def _minutes(entry_ts, end, days, eras):
    total = 0.
    for day in days:
        era = era_for(day, eras)
        opening, closing = era.fo_open, era.fo_close
        lo = datetime.combine(day, opening, IST).timestamp() * 1000
        hi = datetime.combine(day, closing, IST).timestamp() * 1000
        total += max(0., min(end, hi) - max(entry_ts + BAR_MS, lo))
    return total / 60_000


def _swings(h1, w, side, known_at):
    col = 2 if side == "PE" else 3
    out = []
    for j in range(w, len(h1) - w):
        if h1[j + w][5] > known_at:
            continue
        price = h1[j][col]
        neighbors = [h1[n][col] for n in range(j - w, j + w + 1) if n != j]
        if all(price > p if side == "PE" else price < p for p in neighbors):
            out.append(price)
    return out


@dataclass(frozen=True)
class Exit:
    pick: object

    def begin(self, ctx, history):
        hist = history_of(history)
        p = self.pick
        children = (p.rest,) if p.family == "HALF" else p.members
        level = None
        if p.family == "CHAND":
            atr = _atr(hist.option)
            level = None if atr is None else ctx.entry_px - p.param * atr
        return ExitState(hist, ctx.entry_px, level, p.family == "CHAND" and p.start == 0,
                         members=tuple(build(c).begin(ctx, hist) for c in children))

    def on_bar(self, ctx, state, slot):
        return self.advance(ctx, state, slot)[1]

    def advance(self, ctx, state, slot):
        p, b = self.pick, slot.option_bar
        ts, end = int(b[0]), int(b[0]) + BAR_MS
        day = datetime.fromtimestamp(ts / 1000, IST).date()
        h1 = {b[0]: b for b in state.history.h1}
        for bar in slot.h1_closes or ():
            if isinstance(bar, (tuple, list)) and bar[5] <= end:
                h1[bar[0]] = tuple(bar)
        hist = replace(state.history, option=(*state.history.option, tuple(b)),
                       stock=state.history.stock if slot.stock_bar is None else
                       (*state.history.stock, tuple(slot.stock_bar)),
                       h1=tuple(h1[k] for k in sorted(h1)))
        st = replace(state, history=hist, days=tuple(sorted({*state.days, day})))

        def event(kind, level=None, fraction=1.):
            limit = kind in ("V1R_TARGET", "V1R_HALF")
            return Event(kind, max(level, b[1]) if limit else None, ts if limit else end,
                         fraction, level, end)

        if p.family in ("HALF", "BLEND"):
            picks = (p.rest,) if p.family == "HALF" else p.members
            events = []
            if p.family == "HALF" and not state.booked and b[2] >= ctx.entry_px * 1.2 - EPS:
                st = replace(st, booked=True)
                events.append(event("V1R_HALF", ctx.entry_px * 1.2, .5))
            children = []
            for pick, child in zip(picks, state.members, strict=True):
                updated, ev = build(pick).advance(ctx, child, slot)
                children.append(updated)
                events.extend(ev)
            # v1trade tests every target before invoking any trail, regardless of blend member order.
            targets = [e for e in events if e.kind == "V1R_TARGET"]
            if targets:
                events = [min(targets, key=lambda e: e.level)]
            return replace(st, members=tuple(children)), events
        if p.family == "TARGET":
            level = ctx.entry_px * (1 + p.param)
            return st, [event("V1R_TARGET", level)] if b[2] >= level - EPS else []
        if p.family == "NOPROG":
            st = replace(st, reached=state.reached or b[2] >= ctx.entry_px * 1.1 - EPS)
            hit = not st.reached and _minutes(ctx.entry_ts, end, st.days, hist.eras) >= p.param * 60 - EPS
            return st, [event("V1R_NOPROG")] if hit else []
        if p.family == "LADDER":
            if state.level is not None and b[4] <= state.level + EPS:
                return st, [event("V1R_TRAIL", state.level)]
            n = floor((b[2] / ctx.entry_px - 1) / .20 + EPS)
            if n >= 1:
                new = ctx.entry_px * (1 + .20 * (n - 1))
                st = replace(st, level=new if state.level is None else max(state.level, new), active=True)
            return st, []
        if p.family == "CHAND":
            if state.active and state.level is not None and b[4] <= state.level + EPS:
                return st, [event("V1R_TRAIL", state.level)]
            high, atr = max(state.high, b[2]), _atr(hist.option)
            level = state.level
            if atr is not None:
                new = high - p.param * atr
                level = new if level is None else max(level, new)
            return replace(st, high=high, level=level,
                           active=state.active or b[2] >= ctx.entry_px * (1 + p.start) - EPS), []
        if p.family == "SWING1H":
            level = state.level
            if state.active:
                known = _swings(hist.h1, int(p.param), ctx.side, ts)
                if known:
                    tighten = min if ctx.side == "PE" else max
                    level = known[-1] if level is None else tighten(level, known[-1])
                if slot.stock_bar is not None and level is not None and crossed(ctx.side, slot.stock_bar[4], level):
                    return replace(st, level=level), [event("V1R_TRAIL", level)]
            active = state.active or (b[4] > ctx.entry_px + EPS if p.start == 0 else
                                      b[2] >= ctx.entry_px * (1 + p.start) - EPS)
            return replace(st, level=level, active=active), []
        raise ValueError(f"unknown exit family {p.family}")


def build(exit_pick):
    return Exit(exit_pick)


def trailing(state):
    return state.active or any(trailing(s) for s in state.members)
