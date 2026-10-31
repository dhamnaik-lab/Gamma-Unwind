"""V1 session clocks: D549 (2), D550 B2, D426 and T-357."""
from datetime import datetime, time
from typing import Final
from zoneinfo import ZoneInfo

from gammad.core.sessions import SessionsError, era_for

IST = ZoneInfo("Asia/Kolkata")
BAR_MS = 300_000
# D549 (2): the last entry SIGNAL is the 15:10 stock bar (closes 15:15; fill by 15:15)
LAST_ENTRY_SIGNAL_BAR: Final = time(15, 10)


def _ms(day, at):
    return int(datetime.combine(day, at, IST).timestamp() * 1000)


def last_entry_signal_ms(day, eras=None):
    era = era_for(day, eras)
    ts = _ms(day, LAST_ENTRY_SIGNAL_BAR)
    if ts < _ms(day, era.cm_open) or ts + BAR_MS > _ms(day, era.cm_continuous_close):
        raise SessionsError("last entry signal outside continuous stock session")
    return ts


def fo_close_ms(day, eras=None):
    return _ms(day, era_for(day, eras).fo_close)


def last_bar_ms(day, eras=None):
    return fo_close_ms(day, eras) - BAR_MS


def cash_close_ms(day, eras=None):
    return _ms(day, era_for(day, eras).cm_continuous_close)


def day_mark_at(day, eras=None):
    return era_for(day, eras).fo_day_mark_at
