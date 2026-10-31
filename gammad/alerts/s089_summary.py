"""The 0.8.9 GROUP DAY SUMMARY, rebuilt from the V1N book.

Audit JOBS_ALERTS-03 (owner D714, "Rebuild it from 0.8.9 trades"): the 15:35 group summary of a 0.8.9 install read the
legacy V1 signals and printed "No entries today." on every day with real 0.8.9 cards. This one lists the day's V1N
entries whose GROUP card was sent (client lists only, in both editions), each with the bookings / exits of its GROUP
ladder that the group saw as cards (s089_cards.CLIENT_EXIT_KINDS, the same words), in the 0.8.8 summary layout
(templates.daily_summary: TODAY count + tally, ENTRIES boxes, the footer, capped by length).
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from gammad.alerts import s089_cards as cards
from gammad.alerts import templates as tpl
from gammad.scanners import contract as sc

# = s089_cards.exit_card's icon and title per client exit kind.
_BADGE = {
    "TARGET": (tpl.E_T1, "{leg} HIT"),
    "STOP": (tpl.E_SL, "STOP HIT"),
    "ZONE_STOP": (tpl.E_EDGE, "STOCK STOP HIT"),
    "TRAIL": (tpl.ICON_TRAIL, "TRAILING STOP HIT"),
    "E1_EXIT": (tpl.ICON_EXIT, "EXPIRY-EVE EXIT"),
}
_STOPS = frozenset({"STOP", "ZONE_STOP", "TRAIL"})


@dataclass(frozen=True)
class Entry:
    signal: sc.EntrySignal
    position: sc.PositionState | None  # the GROUP ladder; None = never filled
    events: tuple[sc.ExitEvent, ...] = ()


def _shown(entry: Entry) -> list[sc.ExitEvent]:
    return [e for e in entry.events if e.kind in cards.CLIENT_EXIT_KINDS]


def tally(entry: Entry) -> str:
    """hit (a target booked) / stopped (a stop with no target) / closed (any other exit) / open / unknown (no fill)."""
    if entry.position is None:
        return "unknown"
    kinds = {e.kind for e in _shown(entry)}
    if "TARGET" in kinds:
        return "hit"
    if kinds & _STOPS:
        return "stopped"
    return "closed" if entry.position.closed else "open"


def result_lines(entry: Entry) -> list[str]:
    p = entry.position
    if p is None:
        return [tpl.RESULT_NOT_KNOWN]
    out = []
    for e in _shown(entry):
        icon, title = _BADGE[e.kind]
        pct = tpl._up(p.entry_px, e.px) if e.px is not None else None
        mark = tpl.M_PASS if e.kind == "TARGET" else tpl.sign_marker(pct)
        parts = [f"{icon} <b>{title.format(leg=e.leg)}</b> {tpl.ist_hm(e.decided_ms)}"]
        if e.px is not None:
            parts.append(tpl.rupees(e.px))
        if pct is not None:
            parts.append(tpl._pct_short(pct))
        out.append((f"{mark} " if mark else "") + tpl.DOT.join(parts))
    if not p.closed:
        out.append(f"{tpl.M_OPEN} {tpl.E_OPEN} <b>Still open</b>")
    elif not entry.events or entry.events[-1].kind not in cards.CLIENT_EXIT_KINDS:
        out.append(f"{tpl.E_CLOSED} <b>Position closed</b>")
    return out


def _box(entry: Entry, trade_date: str) -> str:
    s = entry.signal
    exp = s.expiry_date.isoformat()
    head = (f"{tpl.side_mark(s.option_type)} <b>{tpl._contract_text(s.symbol, s.strike, s.option_type)}</b>"
            f"{tpl.DOT}{tpl.disp_day(exp)}{tpl.expiry_mark(exp, trade_date)}")
    premium = entry.position.entry_px if entry.position is not None else s.signal_px
    line = f"{tpl.E_TIME} {tpl.ist_hm(s.signal_ms)}{tpl.DOT}{tpl.E_PRICE} Premium {tpl.rupees(premium)}"
    return tpl._quote([head, line, *result_lines(entry)])


def day_summary(trade_date: str, entries: Sequence[Entry]) -> str:
    rows = sorted((e for e in entries if cards.client_allowed(e.signal.scanner, e.signal.list_name)),
                  key=lambda e: (e.signal.signal_ms, e.signal.key))
    title = [f"{tpl.ICON_SUMMARY} <b>DAY SUMMARY</b>", f"{tpl.E_EXPIRY} <b>{tpl.disp_weekday(trade_date)}</b>",
             tpl.DIV, tpl.section("TODAY")]
    count = f"{tpl.E_ENTRY} <b>{len(rows)}</b> {'entry' if len(rows) == 1 else 'entries'}"
    if not rows:
        return "\n".join([*title, count, "No entries today."])
    buckets = [tally(e) for e in rows]
    hit = buckets.count("hit")
    lines = [f"{tpl.M_PASS} <b>{hit}</b> target{'' if hit == 1 else 's'} hit",
             f"{tpl.M_OPEN} <b>{buckets.count('open')}</b> still open",
             f"{tpl.M_FAIL} <b>{buckets.count('stopped')}</b> stopped"]
    if buckets.count("closed"):
        lines.append(f"{tpl.E_CLOSED} <b>{buckets.count('closed')}</b> closed")
    unknown = buckets.count("unknown")
    if unknown:
        lines.append(f"<i>{unknown} result{'' if unknown == 1 else 's'} not known yet</i>")
    boxes = [_box(e, trade_date) for e in rows]

    def compose(n: int) -> str:
        out = [*title, count, *lines, tpl.DIV, tpl.section("ENTRIES")]
        for i, box in enumerate(boxes[:n]):
            out += ["", box] if i else [box]
        out += [f"+{len(boxes) - n} more"] if n < len(boxes) else []
        return "\n".join([*out, "", tpl.SUMMARY_FOOTER])

    shown = 1
    while shown < len(boxes) and tpl._fits(compose(shown + 1)):
        shown += 1
    return compose(shown)
