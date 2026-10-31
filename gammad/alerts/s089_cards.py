"""Pure contract-record cards; shared display helpers preserve the alert layout."""

from __future__ import annotations

from collections.abc import Sequence

from gammad.alerts import templates as tpl
from gammad.scanners import contract as sc

OWNER_COPY = "owner"
CLIENT_COPY = "client"
LIST_LINE = {
    "BREAKOUT-CE": "BREAKOUT LIST", "BREAKDOWN-PE": "BREAKDOWN LIST",
    "EXPIRY-CE": "EXPIRY LIST", "EXPIRY-PE": "EXPIRY LIST",
    "CARRY": "CARRY LIST", "SQUEEZE": "SQUEEZE LIST",
}
# Audit TOOLS_B_PKG-02 (D628 (2) owner: "anywhere in the message mention as carry strike"; round 4 "Only 'CARRY
# STRIKE'"): the client card heads the carry list with the CARRY STRIKE tag, never the owner's list heading.
CLIENT_LIST_LINE = {**LIST_LINE, sc.CARRY: sc.TAG_CARRY_STRIKE}
CLIENT_EXIT_KINDS = ("TARGET", "STOP", "ZONE_STOP", "TRAIL", "E1_EXIT")
FILL_WORD = {"NEXT_OPEN": "next bar open", "BURST_TICK": "trigger tick"}


def client_allowed(scanner: str, list_name: str) -> bool:
    return scanner in sc.CLIENT_SCANNERS and list_name in sc.V1N_LISTS_CLIENT


def _check_copy(copy: str, scanner: str, list_name: str) -> None:
    if copy not in (OWNER_COPY, CLIENT_COPY):
        raise ValueError("unknown alert copy")
    if copy == CLIENT_COPY and not client_allowed(scanner, list_name):
        raise ValueError("signal is not available to this group")


def _six(px: float, tick: float | None) -> float:
    if tick is not None and not tick >= 0:
        raise ValueError(f"tick must be >= 0 (None / 0 = MIN_TICK), got {tick!r}")
    return round(float(px), 6)  # 7.35 x 2 = 14.700000000000001 stays on the 14.70 tick


def target_on_tick(px: float, tick: float | None) -> float:
    """Audit JOBS_ALERTS-01 (owner round 4 "Signal close, tick-rounded"; D80): a printed target rounded UP to the
    contract tick (MIN_TICK when unknown), in exact decimal arithmetic (templates._floor_tick); the ladder still
    books from the fill."""
    return -tpl._floor_tick(-_six(px, tick), tick)


def stop_on_tick(px: float, tick: float | None) -> float:
    """A printed % premium stop rounded DOWN to the contract tick (JOBS_ALERTS-01)."""
    return tpl._floor_tick(_six(px, tick), tick)


def _contract(record: sc.EntrySignal | sc.PositionState) -> str:
    return (f"{tpl.side_mark(record.option_type)} "
            f"<b>{tpl._contract_text(record.symbol, record.strike, record.option_type)}</b>")


def why_parts(signal: sc.EntrySignal) -> list[str]:
    m = signal.metrics
    parts = []
    if signal.scanner == sc.V1N:
        if m.get("oi_drop") is not None:
            parts.append(f"OI {tpl.pct_drop(m['oi_drop'] * 100)} from peak")
        parts.append("volume above its 20-bar average")
        if m.get("c2") == 1.0:
            parts.append("range-base break")
        if m.get("c3") == 1.0:
            parts.append("NR7 break")
        parts.append("stock above VWAP" if signal.option_type == "CE" else "stock below VWAP")
    else:
        for key, word in (("burst_high", "burst high"), ("burst_low", "burst low")):
            if m.get(key) is not None:
                parts.append(f"{word} {tpl.rupees(m[key])}")
        if not parts:
            parts.append("trigger crossed")
    return parts


def entry_card(signal: sc.EntrySignal, *, copy: str, targets: Sequence[float]) -> str:
    s = signal
    _check_copy(copy, s.scanner, s.list_name)
    if s.ban_reason is not None:
        raise ValueError("banned signal")
    if copy == OWNER_COPY and s.risk_block is not None:
        raise ValueError("paper risk blocked")
    if s.scanner == sc.V1N and len(targets) != 4:
        raise ValueError("V1N requires four targets")
    if s.scanner == sc.V2SQ and len(targets) not in (0, 2):
        raise ValueError("V2SQ requires zero or two targets")
    targets = tuple(target_on_tick(px, s.tick_size) for px in targets)
    heading = (CLIENT_LIST_LINE if copy == CLIENT_COPY else LIST_LINE)[s.list_name]
    tags = tuple(t for t in s.tags if t != heading)  # CARRY STRIKE heads the client card once
    lines = [f"<b>{tpl.esc(sc.SCANNER_LABELS[s.scanner])}</b>"] if copy == OWNER_COPY else []
    lines += [tpl.badge_line(tpl.E_ENTRY, "TRADE ALERT"), tpl.section(heading)]
    if tags:
        lines.append(tpl.DOT.join(f"<b>{tpl.esc(t)}</b>" for t in tags))
    lines.append(_contract(s))
    expiry = f"{tpl.E_EXPIRY} {tpl.disp_day(s.expiry_date)} expiry"
    if s.metrics.get("dte") is not None:
        d = int(s.metrics["dte"])
        expiry += tpl.DOT + ("today" if d == 0 else "tmrw" if d == 1 else f"{d} trading d")
    lines += [expiry, f"{tpl.E_TIME} {tpl.ist_hm(s.signal_ms)} IST", tpl.DIV]
    if copy == CLIENT_COPY:
        lines.append(f"{tpl.E_PRICE} Premium{tpl.BLEND_GAP}<b>{tpl.rupees(s.signal_px)}</b>")
    else:
        lines.append(f"{tpl.E_PRICE} Price{tpl.BLEND_GAP}<b>{tpl.rupees(s.signal_px)}</b>")
    if s.stop_kind == sc.STOP_ZONE:
        word = "below" if s.option_type == "CE" else "above"
        lines.append(f"{tpl.E_SL} Exit if{tpl.BLEND_GAP}<b>{tpl.edge_text(s.stop_level)}</b>"
                     f"{tpl.DOT}{tpl.esc(s.symbol)} 5-min close at or {word} (stock stop)")
    else:
        stop = stop_on_tick(s.stop_level, s.tick_size) if s.stop_kind == sc.STOP_PCT else s.stop_level
        lines.append(f"{tpl.E_SL} Exit if{tpl.BLEND_GAP}<b>{tpl.rupees(stop)}</b>"
                     f"{tpl.DOT}option at or below")
    for k, px in enumerate(targets, 1):
        icon = tpl.E_T2 if k == len(targets) else tpl.E_T1
        lines.append(f"{icon} T{k}{tpl.BLEND_GAP}<b>{tpl.rupees(px)}</b>")
    if s.risk_per_lot is not None:
        lines.append(f"Risk per lot{tpl.BLEND_GAP}<b>{tpl._inr(s.risk_per_lot)}</b>"
                     + (" est." if s.risk_is_estimate else ""))
    lines += [(f"{tpl.E_LOT} Lot{tpl.BLEND_GAP}{tpl.indian_int(s.lot_size)} qty"
               f"{tpl.DOT}{tpl._inr(s.signal_px * s.lot_size)}"), tpl.DIV]
    reason = tpl.DOT.join(why_parts(s))
    lines += [f"{tpl.E_WHY} {reason[:1].upper()}{reason[1:]}",
              tpl.copy_line(s) + (f" | limit {tpl._p2(s.signal_px)}" if copy == OWNER_COPY else "")
              + f" | qty {s.lot_size} (1 lot)",
              tpl.DIV, tpl.DISCLAIMER]
    if copy == OWNER_COPY:
        desk = []
        if s.lots is not None:
            desk.append(f"lots {s.lots}" if s.lots > 0 else
                        f"lots 0{tpl.DOT}not entered (size 0: risk per lot above 1 %)")
        desk += [f"signal bar {tpl.ist_hm(s.signal_bar_ms)}{tpl.DOT}fill {FILL_WORD[s.fill_rule]}",
                 f"entry {s.entry_no} of 2 today", f"stop {s.stop_kind}"]
        desk += [f"{k} {v:.4g}" for k, v in sorted(s.metrics.items()) if v is not None]
        lines += [tpl.DIV, tpl.section("DESK"), *(f"{tpl.BULLET} {x}" for x in desk)]
    return "\n".join(lines)


def exit_card(event: sc.ExitEvent, position: sc.PositionState, *, copy: str) -> str:
    e, p = event, position
    _check_copy(copy, p.scanner, p.list_name)
    if e.position_key != p.position_key:
        raise ValueError("event and position keys differ")
    if copy == CLIENT_COPY and e.kind not in CLIENT_EXIT_KINDS:
        raise ValueError("event is not available to this group")
    pct = tpl._up(p.entry_px, e.px) if e.px is not None else None
    icon, title, marker = {
        "TARGET": (tpl.E_T1, f"{e.leg} HIT", tpl.M_PASS),
        "STOP_MOVE": (tpl.E_SL, "STOP MOVED", ""),
        "STOP": (tpl.E_SL, "STOP HIT", tpl.sign_marker(pct)),
        "ZONE_STOP": (tpl.E_EDGE, "STOCK STOP HIT", tpl.sign_marker(pct)),
        "TRAIL": (tpl.ICON_TRAIL, "TRAILING STOP HIT", tpl.sign_marker(pct)),
        "E1_EXIT": (tpl.ICON_EXIT, "EXPIRY-EVE EXIT", tpl.sign_marker(pct)),
        "OI_RETURN": (tpl.ICON_EXIT, "OI RETURN EXIT", tpl.sign_marker(pct)),
        "CARRY": (tpl.ICON_EXIT, "RUNNER CARRIED", tpl.M_OPEN),
        "DAY_END": (tpl.ICON_EXIT, "DAY-END EXIT", tpl.sign_marker(pct)),
    }[e.kind]
    lines = [f"<b>{tpl.esc(sc.SCANNER_LABELS[p.scanner])}</b>"] if copy == OWNER_COPY else []
    lines += [tpl.badge_line(icon, title, marker), _contract(p),
              f"{tpl.E_TIME} {tpl.ist_hm(e.decided_ms)} IST", tpl.DIV]
    if e.px is not None:
        word = "Entry" if copy == OWNER_COPY else "Premium"
        lines.append(f"{tpl.E_PRICE} {word} {tpl.rupees(p.entry_px)}{tpl.BLEND_GAP}{tpl.ARROW}"
                     f"{tpl.BLEND_GAP}<b>{tpl.rupees(e.px)}</b>{tpl.BLEND_GAP}({tpl._pct_short(pct)})")
    if e.kind in ("STOP_MOVE", "CARRY"):
        lines.append(f"{tpl.E_SL} Stop now{tpl.BLEND_GAP}<b>{tpl.rupees(e.new_stop)}</b>")
    lines += ["Position closed: nothing left open" if p.closed else f"{tpl.M_OPEN} Still open",
              tpl.DIV, tpl.DISCLAIMER]
    if copy == OWNER_COPY:
        lines += [tpl.DIV, tpl.section("DESK"), f"{tpl.BULLET} lots closed {e.lots} of {p.lots_total}",
                  f"{tpl.BULLET} reason {tpl.esc(e.reason)}",
                  f"{tpl.BULLET} booked so far {tpl._inr(p.realized_rs, sign=True)}"]
    return "\n".join(lines)
