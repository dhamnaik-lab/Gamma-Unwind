"""Read-only dashboard contract, supplied by an injected source."""

from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence
from datetime import date as Date
from decimal import Decimal
from http import HTTPStatus
from typing import Any, Protocol, runtime_checkable

from fastapi import HTTPException

from gammad import edition
from gammad.scanners import contract as sc
from gammad.scanners.exits import rladder, v1_ladder
from gammad.scanners.shared import gates, risk
from gammad.scanners.v1r.entry import V1rParams

WS_TYPE = "s089_row"
CHAT_LABELS = ("group", "personal")
LIVE_METRICS = (
    "oi_off_high_pct", "to_zone_pct", "build_unwound_pct", "chk_a", "chk_b",
    "chk_c", "chk_d", "chk_stock", "checks_met", "near_entry",
)
SERVED_ROW_METRICS = LIVE_METRICS + ("ltp", "added_at_ms", "out_of_band_since_ms", "complete")
log = logging.getLogger(__name__)


@runtime_checkable
class S089Source(Protocol):
    def list_rows(self, day: Date) -> Sequence[sc.ListRow]: ...
    def live_snapshot(self, day: Date) -> Mapping[str, Mapping[str, float | None]]: ...
    def row_extras(self, day: Date) -> Mapping[str, Mapping[str, Any]]: ...
    def ltp(self, security_id: int) -> float | None: ...
    def signals(self, day: Date) -> Sequence[sc.EntrySignal]: ...
    def positions(self, day: Date, open_only: bool) -> Sequence[sc.PositionState]: ...
    def exits(self, day: Date) -> Sequence[sc.ExitEvent]: ...
    def event_rs(self, event: sc.ExitEvent) -> float | None: ...
    def targets(self) -> sc.TargetsConfig: ...
    def config_view(self) -> Mapping[str, Any]: ...
    def risk_book(self) -> sc.RiskBook | None: ...
    def day_cap_hit_ms(self, day: Date) -> int | None: ...
    def alerts(self, day: Date) -> Sequence[Mapping[str, Any]]: ...
    def grades(self, days: int) -> Sequence[Mapping[str, Any]]: ...


def row_dict(row, live=None, extras=None, *, ltp=None):
    key = f"{row.list_name}|{row.security_id}"
    out = row.to_dict()
    out["key"] = key
    extra = (extras or {}).get(key, {})
    out["added_reason"] = extra.get("added_reason")
    metrics = dict(out["metrics"])
    snapshot = (live or {}).get(key, {})
    if row.scanner == sc.V1N:
        metrics.update({k: snapshot[k] for k in LIVE_METRICS if k in snapshot})
    metrics.update(ltp=ltp, added_at_ms=extra.get("added_at_ms"),
                   out_of_band_since_ms=extra.get("out_of_band_since_ms"),
                   complete=None if extra.get("complete") is None else float(bool(extra["complete"])))
    out["metrics"] = metrics
    return out


def signal_dict(signal):
    return {**signal.to_dict(), "key": signal.key, "alert_latency_ms": signal.alert_latency_ms}


def position_dict(position, ltp):
    return {**position.to_dict(), "ltp": ltp}


def exit_dict(event, event_rs):
    return {**event.to_dict(), "event_rs": event_rs}


def ws_client_visible(data) -> bool:
    return (isinstance(data, Mapping) and data.get("scanner") in sc.CLIENT_SCANNERS
            and data.get("list_name") in sc.V1N_LISTS_CLIENT)


def _keep(obj):
    return not edition.is_client() or ws_client_visible({"scanner": obj.scanner, "list_name": obj.list_name})


def ws_payload(kind, obj, *, live=None, extras=None, ltp=None, event_rs=None, list_name=None, scanner=None):
    if kind == "row":
        row = row_dict(obj, live, extras, ltp=ltp)
    elif kind == "signal":
        row = signal_dict(obj)
    elif kind == "position":
        row = position_dict(obj, ltp)
    elif kind == "exit":
        if not scanner or not list_name:
            raise ValueError("exit requires scanner and list_name")
        row = exit_dict(obj, event_rs)
    else:
        raise ValueError("unknown push kind")
    return {"kind": kind, "scanner": scanner if kind == "exit" else obj.scanner,
            "list_name": list_name if kind == "exit" else obj.list_name, "row": row}


def _pct(value):
    return float(Decimal(str(value)) * 100)


def rules_body(edition, targets, config):
    p = V1rParams()
    config = config or {}
    thresholds = {
        "oi_drop_pct": _pct(p.oi_drop_alert), "oi_drop_tolerance_pct": _pct(p.oi_drop_tolerance),
        "carry_oi_drop_pct": _pct(p.carry_oi_drop), "carry_unwind_pct": _pct(p.carry_unwind_frac),
        "premium_jump_pct": _pct(p.premium_jump), "max_entries_per_stock": p.max_entries_per_stock,
        "no_zone_stop_pct": _pct(v1_ladder.NO_ZONE_STOP_FRAC), "trb_atr_mult": v1_ladder.TRB_ATR_MULT,
        "carry_cap": p.carry_cap, "roll_dte": gates.ROLL_DTE, "floor_lots": gates.FLOOR_LOTS,
        "floor_premium_rs": gates.FLOOR_PREMIUM_RS, "band_pct": config.get("band_pct"),
    }
    clocks = {k: getattr(gates, k.upper()).strftime("%H:%M")
              for k in ("signal_first_bar", "signal_last_bar", "fill_by", "e1_exit_at")}
    clocks.update(refresh=dict(config["refresh"]) if config.get("refresh") is not None else None,
                  group_additions=list(config["group_additions"])
                  if config.get("group_additions") is not None else None)
    out = {"edition": edition, "targets": targets.to_dict() if targets is not None else None,
           "thresholds": thresholds, "clocks": clocks}
    if edition == "owner":
        from gammad.scanners.v2sq import engine

        thresholds.update(expiry_dte_min=p.expiry_dte_min, expiry_dte_max=p.expiry_dte_max)
        clocks["expiry_e1_entry_until"] = gates.EXPIRY_E1_ENTRY_UNTIL.strftime("%H:%M")
        out["risk"] = {"risk_per_trade_pct": risk.RISK_PER_TRADE_PCT, "max_open": risk.MAX_OPEN,
                       "day_cap_pct": risk.DAY_LOSS_CAP_PCT, "no_iv_stop_pct": risk.NO_IV_STOP_PCT}
        out["v2"] = {
            "max_risk_pct": _pct(rladder.MAX_RISK_FRAC), "oi_return_pct": _pct(rladder.OI_RETURN_FRAC),
            "carry_stop_r": rladder.CARRY_STOP_R, "max_nights": rladder.MAX_NIGHTS,
            "decide_at": rladder.DECIDE.strftime("%H:%M"), "cut_at": rladder.CUT.strftime("%H:%M"),
            **{k: getattr(engine, k.upper()).strftime("%H:%M")
               for k in ("arm_first_bar", "arm_last_bar", "entry_last_bar", "tick_cutoff")},
            "floor_lots": engine.MIN_VOLUME_LOTS, "floor_premium_rs": engine.MIN_PREMIUM,
        }
    return out


def _owner_only():
    if edition.is_client():
        raise HTTPException(HTTPStatus.NOT_FOUND)


def _day(raw, today):
    if not raw:
        return today
    try:
        day = Date.fromisoformat(raw)
        if day.isoformat() != raw:
            raise ValueError
        return day
    except ValueError:
        raise HTTPException(HTTPStatus.UNPROCESSABLE_ENTITY,
                            detail=f"date must be YYYY-MM-DD, got '{raw}'") from None


def register(app, runtime) -> None:
    def context(raw):
        today = runtime.today()
        return _day(raw, today), today, getattr(runtime, "s089", None)

    @app.get("/api/s089/lists")
    def lists(date: str | None = None):
        day, today, source = context(date)
        rows = []
        if source is not None:
            objects = source.list_rows(day)
            live, extras = source.live_snapshot(day), source.row_extras(day)
            prices = {}
            for row in objects:
                if _keep(row):
                    if day == today and row.security_id not in prices:
                        prices[row.security_id] = source.ltp(row.security_id)
                    rows.append(row_dict(row, live, extras, ltp=prices.get(row.security_id)))
        return {"date": day.isoformat(), "available": source is not None, "rows": rows}

    @app.get("/api/s089/signals")
    def signals(date: str | None = None):
        day, _, source = context(date)
        items = [signal_dict(s) for s in source.signals(day) if _keep(s)] if source is not None else []
        items.sort(key=lambda s: (-s["signal_ms"], s["key"]))
        return {"date": day.isoformat(), "available": source is not None, "signals": items}

    @app.get("/api/s089/positions")
    def positions(date: str | None = None, open: str | None = None):
        day, today, source = context(date)
        items, prices = [], {}
        if source is not None:
            for p in source.positions(day, open == "1"):
                if _keep(p):
                    if day == today and p.security_id not in prices:
                        prices[p.security_id] = source.ltp(p.security_id)
                    items.append(position_dict(p, prices.get(p.security_id)))
        return {"date": day.isoformat(), "available": source is not None, "positions": items}

    @app.get("/api/s089/exits")
    def exits(date: str | None = None):
        day, _, source = context(date)
        items = []
        if source is not None:
            keys = ({p.position_key for p in source.positions(day, False) if _keep(p)}
                    if edition.is_client() else None)
            items = [exit_dict(e, source.event_rs(e)) for e in source.exits(day)
                     if keys is None or e.position_key in keys]
            items.sort(key=lambda e: e["decided_ms"])
        return {"date": day.isoformat(), "available": source is not None, "exits": items}

    @app.get("/api/s089/rules")
    def rules():
        source = getattr(runtime, "s089", None)
        body = rules_body(edition.edition(), source.targets() if source is not None else None,
                          source.config_view() if source is not None else None)
        return {**body, "available": source is not None}

    if edition.is_client():
        return

    @app.get("/api/s089/risk")
    def risk_route(date: str | None = None):
        _owner_only()
        day, _, source = context(date)
        book = source.risk_book() if source is not None else None
        values = dict.fromkeys(("capital_rs", "risk_per_trade_rs", "max_open", "open_count", "day_pnl_rs",
                                "day_cap_rs", "day_cap_hit_ms"))
        if book is not None:
            capital = book.capital_rs()
            values.update(capital_rs=capital, risk_per_trade_rs=capital * risk.RISK_PER_TRADE_PCT / 100,
                          max_open=risk.MAX_OPEN, open_count=book.open_count(), day_pnl_rs=book.day_pnl_rs(day),
                          day_cap_rs=capital * risk.DAY_LOSS_CAP_PCT / 100,
                          day_cap_hit_ms=source.day_cap_hit_ms(day))
        return {"date": day.isoformat(), "available": book is not None, **values}

    @app.get("/api/s089/alerts")
    def alerts(date: str | None = None):
        _owner_only()
        day, _, source = context(date)
        items = []
        warned = False
        for alert in source.alerts(day) if source is not None else ():
            names = ("key", "kind", "leg", "decided_ms", "chats")
            if not all(k in alert for k in names):
                if not warned:
                    log.warning("Incomplete alert omitted")
                    warned = True
                continue
            item = {k: alert[k] for k in names}
            item["chats"] = [{"chat": c["chat"], "sent_ms": c.get("sent_ms")}
                             for c in alert["chats"] if c.get("chat") in CHAT_LABELS]
            items.append(item)
        return {"date": day.isoformat(), "available": source is not None, "alerts": items}

    @app.get("/api/s089/grades")
    def grades(days: str | None = None):
        _owner_only()
        try:
            count = int(days)
            if count < 1:
                raise ValueError
        except (ValueError, TypeError):
            raise HTTPException(HTTPStatus.UNPROCESSABLE_ENTITY, detail="days must be an integer >= 1") from None
        source = getattr(runtime, "s089", None)
        return {"days": count, "available": source is not None,
                "rows": [dict(r) for r in source.grades(count)] if source is not None else []}


class OutOfBand:
    def __init__(self):
        self._since = {}

    def observe(self, key, to_zone_pct, bar_ms, band_pct) -> int | None:
        if to_zone_pct is None:
            return self._since.get(key)
        if to_zone_pct > band_pct:
            return self._since.setdefault(key, bar_ms)
        self._since.pop(key, None)
        return None

    def reset(self) -> None:
        self._since.clear()
