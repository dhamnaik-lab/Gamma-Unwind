"""Pure G1-G4 / G7 and A5 gates (D617, D620, D622, D628, D633)."""

from collections.abc import Callable
from datetime import date, datetime, time
from zoneinfo import ZoneInfo

from gammad.core import v1_hours
from gammad.scanners import contract as sc

IST = ZoneInfo("Asia/Kolkata")
BAR_MS = 300_000  # C-4 / D633 (1): five-minute signal bar.
SIGNAL_FIRST_BAR = time(9, 20)  # G3, D617 (2), D622 (1).
SIGNAL_LAST_BAR = v1_hours.LAST_ENTRY_SIGNAL_BAR  # D549 (2), D634 (B), T-357.
FILL_BY = time(15, 15)  # G3, D622 (1).
E1_EXIT_AT = time(15, 15)  # G2, D617 (3).
EXPIRY_E1_ENTRY_UNTIL = time(13, 0)  # A5, D628 (18), D633 (1).
FLOOR_LOTS = 2  # G4, D617 (2).
FLOOR_PREMIUM_RS = 1.0  # G4, D617 (2).
ROLL_DTE = 4  # G7, D620 (2).
EXPIRY_LISTS = (sc.EXPIRY_CE, sc.EXPIRY_PE)
BAN_UNKNOWN_REASON = "ban list UNKNOWN (fail closed)"  # D630 (3).


def _ist(ts_ms: int) -> datetime:
    return datetime.fromtimestamp(ts_ms / 1000, IST)


def ban_block(ban: sc.BanView, symbol: str, day: date) -> str | None:
    if ban.status(day) != sc.BAN_OK:
        return BAN_UNKNOWN_REASON
    return ban.ban_reason(symbol, day)


def in_window(bar_start_ms: int) -> bool:
    return SIGNAL_FIRST_BAR <= _ist(bar_start_ms).time() <= SIGNAL_LAST_BAR


def fill_in_time(fill_ms: int) -> bool:
    return _ist(fill_ms).time() <= FILL_BY


def floor_ok(volume: int | None, lot_size: int, premium: float | None) -> bool:
    if volume is None or premium is None or lot_size <= 0:
        return False
    return volume >= FLOOR_LOTS * lot_size and premium >= FLOOR_PREMIUM_RS


def expiry_cutoff_ok(list_name: str, dte: int, bar_start_ms: int) -> bool:
    if list_name not in EXPIRY_LISTS or dte != 1:
        return True
    return _ist(bar_start_ms + BAR_MS).time() <= EXPIRY_E1_ENTRY_UNTIL


def roll_to_next(dte: int) -> bool:
    return dte <= ROLL_DTE


class SharedGates:
    def __init__(self, ban: sc.BanView):
        self.ban = ban

    def ban_block(self, symbol: str, day: date) -> str | None:
        return ban_block(self.ban, symbol, day)

    def in_window(self, bar_start_ms: int) -> bool:
        return in_window(bar_start_ms)

    def floor_ok(self, volume: int | None, lot_size: int, premium: float | None) -> bool:
        return floor_ok(volume, lot_size, premium)

    def expiry_cutoff_ok(self, list_name: str, dte: int, bar_start_ms: int) -> bool:
        return expiry_cutoff_ok(list_name, dte, bar_start_ms)


class E1Cut:
    def __init__(self, prev_trading_day: Callable[[date], date]):
        self.prev_trading_day = prev_trading_day

    def __call__(self, expiry_date: date, bar_start_ms: int) -> bool:
        e1 = self.prev_trading_day(expiry_date)
        bar = _ist(bar_start_ms)
        return bar.date() > e1 or (bar.date() == e1 and bar.time() >= E1_EXIT_AT)
