"""NSE trading-day calendar: weekends plus the holidays listed in config/nse_holidays.yaml.

W14 P2 (audit #12): a date whose YEAR is not listed in the YAML is NOT an error: its weekdays are trading
days (a holiday then costs one quiet session, while "every day is a holiday" cost every session), and the
calendar logs ONE WARNING per (day it is asked on, missing year): "holiday list for <year> not loaded: update
GAMMA". It never raises CalendarYearMissing at build time any more; `strict=True` keeps the old refusal for a
caller that must not guess. Add next year's list the day NSE publishes it.

`trading_days_between(a, b)` counts trading days d with a < d <= b (a excluded, b included);
that is exactly the D5 DTE definition, so expiry.dte() is a thin wrapper over it.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from datetime import date, datetime, timedelta, timezone
from functools import lru_cache
from pathlib import Path

import yaml

from gammad import REPO_ROOT
from gammad.log import get_logger

HOLIDAYS_PATH = REPO_ROOT / "config" / "nse_holidays.yaml"


IST = timezone(timedelta(hours=5, minutes=30))
log = get_logger(__name__)


class CalendarYearMissing(LookupError):
    """The holiday list has no entry for this year (raised only by a strict calendar)."""


def _today_ist() -> date:
    return datetime.now(IST).date()


class TradingCalendar:
    def __init__(self, holidays_by_year: Mapping[int, Iterable[date]], *, strict: bool = False,
                 today: Callable[[], date] = _today_ist) -> None:  # fmt: skip
        """`strict` = a missing year raises CalendarYearMissing (the pre-W14 rule); `today` = the clock the
        once-a-day WARNING is keyed on (tests inject it)."""
        self.strict = strict
        self._today = today
        self._warned: set[tuple[date, int]] = set()  # (day asked on, missing year) already warned
        self._years: dict[int, frozenset[date]] = {}
        for year, days in holidays_by_year.items():
            ds = frozenset(days)
            wrong = sorted(d for d in ds if d.year != int(year))
            if wrong:
                raise ValueError(f"holiday list {year} holds dates from another year: {wrong}")
            self._years[int(year)] = ds

    @classmethod
    def from_yaml(cls, path: Path) -> TradingCalendar:
        with path.open(encoding="utf-8") as fh:
            raw = yaml.safe_load(fh) or {}
        if not isinstance(raw, dict):
            raise TypeError(f"{path}: expected a mapping of year -> [dates]")
        parsed: dict[int, list[date]] = {}
        for year, days in raw.items():
            out: list[date] = []
            for d in days or []:
                if isinstance(d, date):
                    out.append(d)
                else:
                    out.append(date.fromisoformat(str(d)))
            parsed[int(year)] = out
        return cls(parsed)

    @property
    def years(self) -> tuple[int, ...]:
        return tuple(sorted(self._years))

    def _holidays(self, year: int) -> frozenset[date]:
        hol = self._years.get(year)
        if hol is not None:
            return hol
        if self.strict:
            raise CalendarYearMissing(
                f"no NSE holiday list for {year} in config/nse_holidays.yaml (listed: {self.years})"
            )
        key = (self._today(), year)
        if key not in self._warned:  # the first check of the day warns; the rest of the day is quiet
            self._warned.add(key)
            log.warning("holiday list for %d not loaded: update GAMMA (weekdays of %d are treated as trading "
                        "days; listed years: %s)", year, year, self.years)  # fmt: skip
        return frozenset()

    def is_trading_day(self, d: date) -> bool:
        hol = self._holidays(d.year)
        return d.weekday() < 5 and d not in hol

    def trading_days_between(self, a: date, b: date) -> int:
        """Trading days d with a < d <= b. Raises ValueError when b < a."""
        if b < a:
            raise ValueError(f"trading_days_between: end {b} is before start {a}")
        n = 0
        d = a + timedelta(days=1)
        while d <= b:
            n += self.is_trading_day(d)
            d += timedelta(days=1)
        return n

    def next_trading_day(self, d: date) -> date:
        x = d + timedelta(days=1)
        while not self.is_trading_day(x):
            x += timedelta(days=1)
        return x

    def prev_trading_day(self, d: date) -> date:
        x = d - timedelta(days=1)
        while not self.is_trading_day(x):
            x -= timedelta(days=1)
        return x


@lru_cache(maxsize=1)
def default_calendar() -> TradingCalendar:
    return TradingCalendar.from_yaml(HOLIDAYS_PATH)


def is_trading_day(d: date) -> bool:
    return default_calendar().is_trading_day(d)


def trading_days_between(a: date, b: date) -> int:
    return default_calendar().trading_days_between(a, b)


def next_trading_day(d: date) -> date:
    return default_calendar().next_trading_day(d)


def prev_trading_day(d: date) -> date:
    return default_calendar().prev_trading_day(d)
