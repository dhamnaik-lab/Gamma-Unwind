"""Entry-time opposite levels, with strict daily edges and causal hourly fallback."""
from dataclasses import dataclass

EPS = 1e-9


@dataclass(frozen=True)
class Resolved:
    level: float | None
    source: str
    cap: float | None
    confirmed_ms: int | None
    candle: str = "5m"  # retained for V1Position.open: its contract has no separate stop pick


def resolve(side, spec, *, entry_close, daily_zones, h1_book, t_ms):
    level, source, confirmed = None, "none", None
    if spec.level == "daily":
        if side == "PE":
            levels = [z.bottom for z in daily_zones.live_sell if z.bottom > entry_close + EPS]
            level = min(levels, default=None)
        else:
            levels = [z.top for z in daily_zones.live_buy if z.top < entry_close - EPS]
            level = max(levels, default=None)
        if level is not None:
            source = "daily"
    if level is None and h1_book is not None:
        zone = h1_book.stop_zone(side, entry_close, t_ms)
        if zone is not None and zone.confirmed_ms <= t_ms:
            level = zone.bottom if side == "PE" else zone.top
            source = "1h_fallback" if spec.level == "daily" else "1h"
            confirmed = zone.confirmed_ms
    if level is None and spec.cap is not None:
        source = "cap_only"
    return Resolved(level, source, spec.cap, confirmed, spec.candle)


def crossed(side, value, level):
    return value > level + EPS if side == "PE" else value < level - EPS


def decide_stop(side, resolved, candle, *, stock_close, hour_end, daily_close, option_close, entry_px):
    value = daily_close if candle == "daily" else stock_close
    scheduled = candle == "5m" or (candle == "1h" and hour_end) or candle == "daily"
    if scheduled and value is not None and resolved.level is not None and crossed(side, value, resolved.level):
        return "stop_level"
    if resolved.cap is not None and option_close <= entry_px * (1 - resolved.cap) + EPS:
        return "stop_cap"
    return None
