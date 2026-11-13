"""Live scanner books, delivery stamps and the shared dashboard source.

Decisions and ladder steps are synchronous and durable before delivery is queued.
Delivery may finish after a later step: stamps merge into the current stored history.
"""

import json
import logging
import sqlite3
from collections.abc import Mapping, Sequence
from dataclasses import replace
from datetime import date, datetime, time
from typing import Any

from gammad.alerts import s089_dispatch as dispatch
from gammad.alerts import s089_summary
from gammad.api import s089
from gammad.core.calendar import IST
from gammad.scanners import contract as sc
from gammad.scanners import late_start
from gammad.scanners.exits import ist_date, v1_ladder
from gammad.scanners.live import book
from gammad.scanners.live.charges import charges
from gammad.scanners.shared import gates, risk
from gammad.scanners.targets import load_targets
from gammad.scanners.v1r.lists import v1n_list_rows

log = logging.getLogger(__name__)
GROUP_LOTS = 4  # D651 (2): an independent notional client ladder.
BUILD_HM = time(9, 20)  # D704: the day's list build (= backtest/exact/carrylist.BUILD_HM)


def build_ms(day: date) -> int:
    return int(datetime.combine(day, BUILD_HM, IST).timestamp() * 1000)


def open_v1n_at(conn, at_ms: int) -> frozenset[int]:
    """V1N ids open at `at_ms` in either book: entry filled before it, and not closed by a fill before it (a closed
    ladder's updated_ms is the start of the bar it filled in, = the exact walk's last leg fill_ms). Either book counts
    (D660), so it equals exact's one-walk set only while the OWN and GROUP ladders close in the same bar."""
    if not conn.execute("PRAGMA table_info(ladder_state)").fetchall():
        return frozenset()
    rows = conn.execute("SELECT state_json FROM ladder_state WHERE scanner = ? AND (closed = 0 OR updated_ms >= ?)",
                        (sc.V1N, at_ms))
    return frozenset(int(s["security_id"]) for s in (json.loads(r[0]) for r in rows) if s["entry_ms"] < at_ms)


class LiveLadderBook(book.LadderBook):
    """The same store/risk book, with atomic snapshots for HTTP worker reads.

    All SQL stays on the runtime thread. Risk arithmetic is delegated to the
    merged LadderBook, including its stored whole-step deltas, unchanged.
    """

    def __init__(self, *args, **kwargs):
        self._track_risk = kwargs.get("edition", "owner") == "owner"
        super().__init__(*args, **kwargs)
        self._refresh_risk()

    def _refresh_risk(self):
        if not self._track_risk:
            self._risk_snapshot = 0, {}
            return
        # Keyed by each booking's FILL day, the same rule day_pnl_rs sums by (D732 (1)): keyed by the deciding
        # event's date instead, an overnight gap stop would show on neither day.
        days = {book.booked_day(item)
                for (payload,) in self.conn.execute("SELECT events_json FROM ladder_state WHERE book='OWN'")
                for item in json.loads(payload)}
        pnl = {day: super(LiveLadderBook, self).day_pnl_rs(day) for day in days}
        self._risk_snapshot = super().open_count(), pnl

    def save(self, state, book, events=()):
        super().save(state, book, events)
        self._refresh_risk()

    def open_count(self):
        return self._risk_snapshot[0]

    def day_pnl_rs(self, day):
        return self._risk_snapshot[1].get(day, 0.0)


class ScannerHub:
    def __init__(self, conn, *, edition, engine, e1_cut, sender, chat_ids, clock_ms,
                 publish, spawn, spec_version, ltp, config=None, costs=charges,
                 on_failure=None, on_no_group_chat=None):
        self.conn, self.edition, self.engine = conn, edition, engine
        # JOBS_ALERTS-02: the app's router escalation for a FAILED card / a GROUP card with no chat.
        self.on_failure, self.on_no_group_chat = on_failure, on_no_group_chat
        self._targets = load_targets()
        self.group_targets = sc.TargetsConfig() if edition == "owner" else self._targets
        # Audit P2B-DASH-CLIENT-01 ([OC] reversing D653 (3) O11 (a) for client installs): the API and the WS pushes
        # serve the book this edition runs. Owner: OWN only (GROUP stays Telegram-only there). Client: GROUP, the
        # only book a client opens, so its pages show the trades its group cards already report.
        self.served_book = "OWN" if edition == "owner" else "GROUP"
        capital = book.load_capital() if edition == "owner" else None
        self.book = LiveLadderBook(conn, capital_rs=capital, edition=edition)
        self.e1_cut, self.costs = e1_cut, costs
        self.sender, self.chat_ids, self.clock_ms = sender, chat_ids, clock_ms
        self.publish, self.spawn, self.spec_version = publish, spawn, spec_version
        self._ltp, self._config = ltp, config or {}
        self.day = None
        self._open_v1n = frozenset()
        self.v2 = None  # Owner adapter is injected; this shipped module never imports it.
        self.states = {}
        self.pending = {}
        self.signal_ids = {}
        self._entry_seen = {}  # T-737: key -> {chat: the ENTRY card's status} (this process; {} while in flight)
        self._signals = {}
        self._events = {}
        self._extras = {}
        self._caps = {}
        self._lost = set()
        self._deliveries = []
        self.out_of_band = s089.OutOfBand()

    def _push(self, kind, obj, **kwargs):
        self.publish(s089.WS_TYPE, s089.ws_payload(kind, obj, **kwargs))

    def prepare(self, day, rows, *, at_ms):
        fresh_day = day != self.day
        # D697 M2 / D704: the V1N ids open AT the day's 09:20 build serve the build and every refresh mark (= exact
        # open_v1n_by_day). Derived from the stored fills, so a restart or a pre-market first prepare reads the same set.
        self._open_v1n = open_v1n_at(self.conn, build_ms(day))
        built = v1n_list_rows(rows, trade_date=day, edition=self.edition,
                              dte_fn=self.engine.dte_fn, gates=self.engine.gates,
                              results=self.engine.results, params=self.engine.p, open_v1n=self._open_v1n)
        before = {f"{r.list_name}|{r.security_id}": r for r in self.engine.list_rows()}
        if fresh_day:
            self.day, self._extras = day, {}
            self.pending.clear()
            self.out_of_band.reset()
            self.engine.start_day(day, built.rows)
            self.restore(day)
            before = {}
        else:
            self.engine.refresh_rows(built.rows)
        new = []
        for row in built.rows:
            key = f"{row.list_name}|{row.security_id}"
            if key not in before:
                new.append(row)
                reason = None if fresh_day else (
                    "new top-2 strike" if any(r.symbol == row.symbol and r.list_name == row.list_name
                                              for r in before.values()) else "new band entry")
                self._extras[key] = {"added_at_ms": None if fresh_day else at_ms, "added_reason": reason,
                                         "out_of_band_since_ms": None, "complete": False}
        self.push_rows()
        return new

    def seed(self, rows, prior_options, prior_stocks, today_options, today_stocks, start_ms):
        for sid, bars in prior_stocks.items():
            self.engine.seed_stock(sid, sorted(bars, key=lambda b: b.start_ms))
        for sid, bars in prior_options.items():
            self.engine.seed_option(sid, sorted(bars, key=lambda b: b.start_ms))
        for sid, bars in today_stocks.items():
            self.engine.seed_stock(sid, sorted(bars, key=lambda b: b.start_ms))
        seeds = late_start.build_seeds(today_options, [r.security_id for r in rows], self.day, start_ms)
        late_start.seed_v1n(self.engine, seeds, lambda b: b)
        for row in rows:
            self._extras[f"{row.list_name}|{row.security_id}"]["complete"] = seeds[row.security_id].complete
        log.info("s089 seed %s", late_start.summary(seeds))

    def restore(self, day):
        self.states = {}
        self._events = {}
        self.signal_ids = {}
        self._signals = {}
        self._deliveries = [tuple(r) for r in self.conn.execute(
            "SELECT signal_id, channel, sent_ts_ms, status FROM alerts ORDER BY id")]
        for signal_id, payload in self.conn.execute(
                "SELECT id, payload_json FROM signals WHERE scanner IN (?, ?) ORDER BY id", sc.SCANNER_IDS):
            signal = sc.EntrySignal.from_dict(json.loads(payload))
            self.signal_ids[signal.key] = signal_id
            self._signals[signal.key] = signal
            if signal.risk_block == "DAY_CAP":
                self._caps.setdefault(signal.trade_date, signal.signal_ms)
        for key, name, state, events in self.conn.execute(
                "SELECT position_key, book, state_json, events_json FROM ladder_state"):
            self.states[key, name] = sc.PositionState.from_dict(json.loads(state))
            self._events[key, name] = [sc.ExitEvent.from_dict(x["event"]) for x in json.loads(events)]
        for key, signal in self._signals.items():
            if signal.scanner != sc.V1N:
                continue
            states = [p for (k, _), p in self.states.items() if k == key]
            if not states:
                if signal.ban_reason is not None or not gates.fill_in_time(signal.signal_ms):
                    continue
                if signal.trade_date == day and key not in self._lost:
                    log.warning("pending fill lost %s", key)
                    self._lost.add(key)
                continue
            restored = replace(signal, risk_block=None)
            if signal.trade_date < day and any(not p.closed for p in states):
                self.engine.restore_carried(restored)
            elif signal.trade_date == day:
                self.engine.restore_entry(restored, closed=all(p.closed for p in states))

    def remember(self, signal):
        self.signal_ids[signal.key] = self.book.insert_signal(signal, spec_version=self.spec_version)
        self._signals[signal.key] = signal
        if signal.risk_block == "DAY_CAP":
            self._caps.setdefault(signal.trade_date, signal.signal_ms)
        self._push("signal", signal)

    def accept(self, signal):
        if self.edition == "owner":
            lots, block = risk.g6_size(self.book, signal.trade_date, signal.risk_per_lot)
            signal = replace(signal, lots=lots, risk_block=block)
        self.remember(signal)
        if signal.ban_reason is None:
            self.pending[signal.key] = signal
        else:
            self.engine.unbook(signal.key)
        self.spawn(self.send_entry(signal))
        return signal

    async def send_entry(self, signal, v2_targets=None):
        self._entry_seen[signal.key] = {}
        stamped, deliveries = await dispatch.send_entry(
            signal, edition=self.edition, sender=self.sender, chat_ids=self.chat_ids, clock_ms=self.clock_ms,
            own_targets=self._targets, group_targets=self.group_targets, v2_targets=v2_targets,
            on_failure=self.on_failure)
        self._entry_seen[signal.key] = {d.chat: d.status for d in deliveries}
        self._record(signal.key, deliveries)
        with self.conn:
            self.conn.execute("UPDATE signals SET payload_json=?, latency_ms=? WHERE id=?",
                              (json.dumps(stamped.to_dict()), stamped.alert_latency_ms,
                               self.signal_ids[signal.key]))
        self._signals[signal.key] = stamped
        self._push("signal", stamped)

    def _record(self, key, deliveries):
        for delivery in deliveries:
            if delivery.error == "RuntimeError: paused":
                delivery = replace(delivery, error="paused")
            if (delivery.status == dispatch.NO_CHAT and delivery.chat == dispatch.GROUP
                    and self.on_no_group_chat is not None):
                try:
                    self.on_no_group_chat()
                except Exception:  # the escalation must never cost the delivery record
                    log.exception("s089 no-group-chat hook raised")
            self.book.record_delivery(self.signal_ids[key], delivery)
            self._deliveries.append((self.signal_ids[key], delivery.chat, delivery.sent_ms, delivery.status))

    def entry_status(self, key, chat):
        """T-737 (D732 (2)): the status of signal `key`'s ENTRY card on `chat`: recorded at send_entry (in flight ->
        None), else (after a restart) its first delivery row there (the alerts rows; entry delivery precedes exits, as
        alerts() reads them). A D48 hand send of the entry to that chat (manual_sends, s089 key) counts as SENT."""
        sid = self.signal_ids.get(key)
        if sid is None:
            return None
        seen = self._entry_seen.get(key)
        status = (seen.get(chat) if seen is not None else
                  next((st for s, c, _, st in self._deliveries if s == sid and c == chat), None))
        if status == dispatch.SENT:
            return status
        try:
            hand = self.conn.execute(
                "SELECT 1 FROM manual_sends WHERE kind = 'signal' AND dest = ? AND status = ? AND json_valid(ref)"
                " AND json_extract(ref, '$.id') = ? LIMIT 1", (chat, dispatch.SENT, key)).fetchone()
        except sqlite3.Error:
            hand = None
        return dispatch.SENT if hand is not None else status

    def fill(self, sid, start_ms, open_px):
        for key, signal in list(self.pending.items()):
            if signal.security_id != sid or start_ms < signal.signal_ms:
                continue
            del self.pending[key]
            if ist_date(start_ms) == signal.trade_date and gates.fill_in_time(start_ms):
                if self.edition == "owner" and not signal.blocked and (signal.lots or 0) >= 1:
                    self.open(v1_ladder.open_position(signal, fill_ms=start_ms, fill_px=open_px,
                                                      lots=signal.lots, targets=self._targets), "OWN")
                if signal.ban_reason is None:
                    group = replace(signal, risk_block=None, lots=GROUP_LOTS)
                    self.open(v1_ladder.open_position(group, fill_ms=start_ms, fill_px=open_px,
                                                      lots=GROUP_LOTS, targets=self.group_targets), "GROUP")
            if not any(k == key for k, _ in self.states):
                self.engine.unbook(key)

    def open(self, state, name):
        self.book.save(state, name)
        self.states[state.position_key, name] = state
        self._events.setdefault((state.position_key, name), [])
        if name == self.served_book:
            self._push("position", state, ltp=self.ltp(state.security_id))

    def option_bar(self, sid, option, ladder, *, entry=True):
        if entry:
            self.fill(sid, option.start_ms, option.open)
        for identity, state in list(self.states.items()):
            if state.closed or state.security_id != sid:
                continue
            if state.scanner == sc.V1N:
                result = v1_ladder.step(state, ladder, e1_cut=self.e1_cut, charges=self.costs)
            elif self.v2 is not None:
                result = self.v2.step(state, ladder)
            else:
                continue
            self.save_step(identity, *result)
        if entry:
            if self.v2 is not None:
                self.v2.on_bar(sid, option)
            signal = self.engine.on_option_bar(sid, option)
            if signal is not None:
                self.accept(signal)
        self.push_rows(option.start_ms, sid)

    def save_step(self, identity, state, events):
        key, name = identity
        self.book.save(state, name, events)
        self.states[identity] = state
        self._events.setdefault(identity, []).extend(events)
        if name == self.served_book:
            self._push("position", state, ltp=self.ltp(state.security_id))
        if events:
            self.spawn(self.send_exits(identity, state, events))
        if (state.closed and state.scanner == sc.V1N
                and all(p.closed for (k, _), p in self.states.items() if k == key)):
            self.engine.mark_closed(key)

    async def send_exits(self, identity, state, events):
        key, name = identity
        for seq, event in enumerate(events):
            chat = dispatch.PERSONAL if name == "OWN" else dispatch.GROUP
            posts = [p for p in dispatch.plan_exit(event, state, edition=self.edition, seq=seq)
                     if p.chat == chat]
            deliveries = await dispatch.deliver(posts, sender=self.sender, chat_ids=self.chat_ids,
                                                clock_ms=self.clock_ms, start_ms=event.decided_ms,
                                                on_failure=self.on_failure,
                                                entry_status=lambda post: self.entry_status(key, post.chat))
            self._record(key, deliveries)
            first = next((d for d in deliveries if d.status == dispatch.SENT), None)
            stamped = replace(event, alert_sent_ms=first.sent_ms) if first else event
            with self.conn:
                payload = self.conn.execute(
                    "SELECT events_json FROM ladder_state WHERE position_key=? AND book=?", identity).fetchone()
                history = json.loads(payload[0])
                for item in history:
                    if item["event"] == event.to_dict():
                        item["event"]["alert_sent_ms"] = stamped.alert_sent_ms
                        break
                self.conn.execute("UPDATE ladder_state SET events_json=? WHERE position_key=? AND book=?",
                                  (json.dumps(history), key, name))
            self._events[identity] = [stamped if x == event else x for x in self._events[identity]]
            if name == self.served_book:
                self._push("exit", stamped, scanner=state.scanner, list_name=state.list_name,
                           event_rs=self.event_rs(stamped))

    def stock_bar(self, sid, bar):
        self.engine.on_stock_bar(sid, bar)
        if self.v2 is not None and self.v2.engine is not None:
            self.v2.engine.on_stock_bar(sid, bar)

    def push_rows(self, ts_ms=None, security_id=None):
        live = self.live_snapshot(self.day)
        for row in self.list_rows(self.day):
            if security_id is not None and row.security_id != security_id:
                continue
            key = f"{row.list_name}|{row.security_id}"
            if ts_ms is not None:
                since = self.out_of_band.observe(key, live.get(key, {}).get("to_zone_pct"),
                                                 ts_ms, self._config.get("band_pct", 2.5))
                self._extras.setdefault(key, {})["out_of_band_since_ms"] = since
            self._push("row", row, live=live, extras=self._extras, ltp=self.ltp(row.security_id))

    def list_rows(self, day: date) -> Sequence[sc.ListRow]:
        if day != self.day:
            return ()
        return tuple(self.engine.list_rows()) + (tuple(self.v2.rows()) if self.v2 else ())

    def live_snapshot(self, day: date) -> Mapping[str, Mapping[str, float | None]]:
        return self.engine.live_snapshot() if day == self.day else {}

    def row_extras(self, day: date) -> Mapping[str, Mapping[str, Any]]:
        return self._extras.copy() if day == self.day else {}

    def ltp(self, security_id: int) -> float | None:
        return self._ltp(security_id)

    def signals(self, day: date) -> Sequence[sc.EntrySignal]:
        return tuple(s for s in self._signals.copy().values() if s.trade_date == day)

    def positions(self, day: date, open_only: bool) -> Sequence[sc.PositionState]:
        return tuple(p for (_, name), p in self.states.copy().items() if name == self.served_book
                     and (not p.closed or p.entry_date == day or ist_date(p.updated_ms) == day)
                     and p.entry_date <= day and (not open_only or not p.closed))

    def exits(self, day: date) -> Sequence[sc.ExitEvent]:
        return tuple(e for (_, name), events in self._events.copy().items() if name == self.served_book
                     for e in tuple(events) if ist_date(e.decided_ms) == day)

    def event_rs(self, event: sc.ExitEvent) -> float | None:
        state = self.states.get((event.position_key, self.served_book))
        if state is None or event.lots <= 0 or event.px is None:
            return None
        qty = event.lots * state.lot_size
        orders = 1 if event.leg in ("T2", "T3", "T4", "RUNNER") else 2
        try:
            cost = self.costs(state.entry_px, event.px, qty, orders=orders)
        except TypeError:
            cost = self.costs(state.entry_px, event.px, qty)
        return (event.px - state.entry_px) * qty - cost

    def targets(self) -> sc.TargetsConfig:
        return self._targets

    def config_view(self) -> Mapping[str, Any]:
        return self._config

    def risk_book(self) -> sc.RiskBook | None:
        return self.book if self.edition == "owner" else None

    def day_cap_hit_ms(self, day: date) -> int | None:
        return self._caps.get(day)

    def alerts(self, day: date) -> Sequence[Mapping[str, Any]]:
        out = []
        # Map known exit alert timestamps per signal key so exit rows never impersonate entry cards.
        exit_sent_by_key: dict[str, set[tuple[str, int]]] = {}
        for (ekey, name), events in self._events.copy().items():
            chat = dispatch.PERSONAL if name == "OWN" else dispatch.GROUP
            bucket = exit_sent_by_key.setdefault(ekey, set())
            for event in tuple(events):
                if event.alert_sent_ms:
                    bucket.add((chat, int(event.alert_sent_ms)))
        for key, signal in self._signals.copy().items():
            if signal.trade_date != day:
                continue
            plan = dispatch.plan_entry(signal, edition=self.edition, own_targets=self._targets,
                                       group_targets=self.group_targets)
            sig_id = self.signal_ids.get(key)
            known_exits = exit_sent_by_key.get(key, set())
            entry_rows = [
                r for r in self._deliveries
                if sig_id is not None and r[0] == sig_id and (r[1], r[2]) not in known_exits
            ]
            chats = []
            for post in plan:
                sent = next((ms for _, chat, ms, status in entry_rows
                             if chat == post.chat and status == dispatch.SENT), None)
                if sent is not None:
                    chats.append({"chat": post.chat, "sent_ms": sent})
            out.append({"key": key, "kind": "signal", "leg": None,
                        "decided_ms": signal.signal_ms, "chats": chats})
        for (key, name), events in self._events.copy().items():
            for event in tuple(events):
                if ist_date(event.decided_ms) != day:
                    continue
                chat = dispatch.PERSONAL if name == "OWN" else dispatch.GROUP
                chats = [{"chat": chat, "sent_ms": event.alert_sent_ms}] if event.alert_sent_ms else []
                out.append({"key": key, "kind": event.kind, "leg": event.leg,
                            "decided_ms": event.decided_ms, "chats": chats})
        return out

    def group_day_entries(self, day: date) -> list[s089_summary.Entry]:
        """Audit JOBS_ALERTS-03 (owner D714): the day's V1N entries whose GROUP card was SENT, each with its GROUP
        ladder (the bookings / exits the group saw); read from the store, so a restart keeps the day."""
        sent = {int(sid) for (sid,) in self.conn.execute(
            "SELECT DISTINCT signal_id FROM alerts WHERE channel=? AND status=?", (dispatch.GROUP, dispatch.SENT))}
        ladders = {key: (sc.PositionState.from_dict(json.loads(state)),
                         tuple(sc.ExitEvent.from_dict(x["event"]) for x in json.loads(events)))
                   for key, state, events in self.conn.execute(
                       "SELECT position_key, state_json, events_json FROM ladder_state WHERE book='GROUP'")}
        out = []
        for signal_id, payload in self.conn.execute(
                "SELECT id, payload_json FROM signals WHERE scanner=? AND trade_date=? ORDER BY id",
                (sc.V1N, day.isoformat())):
            if int(signal_id) not in sent:
                continue
            signal = sc.EntrySignal.from_dict(json.loads(payload))
            position, events = ladders.get(signal.key, (None, ()))
            out.append(s089_summary.Entry(signal, position, events))
        return out

    def grades(self, days: int) -> Sequence[Mapping[str, Any]]:
        return ()  # CX-205 O8: no new-scanner grader exists yet.
