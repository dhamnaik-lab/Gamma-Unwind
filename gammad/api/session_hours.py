"""Read-only session clocks for the dashboard, from the shared era rules."""

import logging
from collections.abc import Sequence
from datetime import date, datetime

import yaml

from gammad.core import sessions, v1_hours

log = logging.getLogger(__name__)


def session_hours_body(d: date, eras: Sequence[sessions.Era] | None = None) -> dict:
    """An unreadable table is unavailable; an uncovered date remains an error."""
    if eras is None:
        try:
            eras = sessions.default_eras()
        except (OSError, yaml.YAMLError, sessions.SessionsError, ValueError, TypeError):
            log.warning("Session hours unavailable; could not load the era table")
            return {"date": d.isoformat(), "hours": None}

    era = sessions.era_for(d, eras)

    def clock(ms: int) -> str:
        return datetime.fromtimestamp(ms / 1000, v1_hours.IST).strftime("%H:%M")

    last_entry = v1_hours.last_entry_signal_ms(d, eras)
    return {"date": d.isoformat(), "hours": {
        "fo_open": era.fo_open.strftime("%H:%M"),
        "cm_open": era.cm_open.strftime("%H:%M"),
        "last_entry_signal": clock(last_entry),
        "entries_end": clock(last_entry + v1_hours.BAR_MS),
        "day_end": clock(v1_hours.last_bar_ms(d, eras)),
        "fo_close": clock(v1_hours.fo_close_ms(d, eras)),
        "cash_close": clock(v1_hours.cash_close_ms(d, eras)),
    }}
