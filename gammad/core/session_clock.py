"""The wall-clock face of config/market_sessions.yaml; no time literal lives outside it (T-357)."""
from collections.abc import Sequence
from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

from gammad.core.sessions import Era, SessionsError, default_eras, era_for
from gammad.core.v1_hours import day_mark_at

IST = ZoneInfo("Asia/Kolkata")


def _era(d: date, eras: Sequence[Era] | None = None) -> Era:
    return era_for(d, eras)


def fo_window(d: date, eras: Sequence[Era] | None = None) -> tuple[time, time]:
    era = _era(d, eras)
    return era.fo_open, era.fo_close


def fo_bounds_ms(d: date, eras: Sequence[Era] | None = None) -> tuple[int, int]:
    opening, close = fo_window(d, eras)
    return (int(datetime.combine(d, opening, IST).timestamp() * 1000),
            int(datetime.combine(d, close, IST).timestamp() * 1000))


def fo_close_hm(d: date, eras: Sequence[Era] | None = None) -> str:
    return fo_window(d, eras)[1].strftime("%H:%M")


def in_fo_session(ts_ms: int, eras: Sequence[Era] | None = None) -> bool:
    local = datetime.fromtimestamp(ts_ms / 1000, IST)
    seq = default_eras() if eras is None else eras
    era = seq[0] if seq and local.date() < seq[0].effective_from else _era(local.date(), seq)
    return era.fo_open <= local.time() < era.fo_close


def fo_market_hours(now_ms: int, eras: Sequence[Era] | None = None) -> bool:
    return datetime.fromtimestamp(now_ms / 1000, IST).weekday() < 5 and in_fo_session(now_ms, eras)


def day_mark_hm(d: date, eras: Sequence[Era] | None = None) -> str:
    return day_mark_at(d, eras).strftime("%H:%M")


def clock_after(d: date, base: str, offset: timedelta, eras: Sequence[Era] | None = None) -> time:
    if base not in ("fo_close", "fo_day_mark_at"):
        raise SessionsError("unknown session clock")
    if not isinstance(offset, timedelta) or offset < timedelta(0):
        raise SessionsError("offset must be a nonnegative timedelta")
    try:
        at = datetime.combine(d, getattr(_era(d, eras), base)) + offset
    except OverflowError as exc:
        raise SessionsError("session clock crosses midnight") from exc
    if at.date() != d:
        raise SessionsError("session clock crosses midnight")
    return at.time()
