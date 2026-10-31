"""Per-item first entries and one same-item stop retry per stock/side/day."""
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from gammad.core import v1_hours
from gammad.core.v1_params import all_pending

from .states import STOP_KINDS

IST = timezone(timedelta(hours=5, minutes=30))
BAR_MS = 300_000
# D543 (3): the first eligible re-entry signal bar is the one AFTER the stop-fill bar
# (= backtest/v1run/engine.py REENTRY_SCAN_BARS)
REENTRY_SCAN_BARS = 2
REENTRY_LIMIT = all_pending().fixed_rules()["reentry_max_per_stock_side_day"]


@dataclass(frozen=True)
class BudgetTrade:
    item: str
    symbol: str
    side: str
    open: bool
    exit_kind: str | None
    decided_close_ms: int | None


def reentry_listed_ms(last) -> int:
    return last.decided_close_ms + REENTRY_SCAN_BARS * BAR_MS


def may_enter(trades_today, item, *, symbol, side, slot_close_ms):
    trades = [t for t in trades_today if t.symbol == symbol and t.side == side]
    own = [t for t in trades if t.item == item]
    if any(t.open for t in own):
        return False, "skipped_open"
    if not own:
        return True, "first_entry"
    last = own[-1]
    if last.exit_kind not in STOP_KINDS:
        return False, "not_stop"
    used = sum(max(0, n - 1) for n in Counter(t.item for t in trades).values())
    if used >= REENTRY_LIMIT:
        return False, "reentry_limit"
    if last.decided_close_ms is None:
        return False, "no_decision_time"
    listed = reentry_listed_ms(last)
    at = datetime.fromtimestamp(listed / 1000, IST)
    now = datetime.fromtimestamp(slot_close_ms / 1000, IST)
    if at.date() != now.date() or listed > v1_hours.last_entry_signal_ms(now.date()) + BAR_MS:
        return False, "too_late"
    if slot_close_ms < listed:
        return False, "before_listing"
    return True, "reentry"
