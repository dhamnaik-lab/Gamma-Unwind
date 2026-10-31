"""D5 -- the ONLY home of the expiry rule (spec Step 2.1).

Expiry dates are NEVER computed here: they come from the broker adapter's expiries() call and are
taken as given (a holiday-shifted expiry is whatever date the exchange/broker lists).

  current expiry = earliest listed expiry >= trade_date (expiry day itself is still "current";
                   the day after, the next month becomes current).
  DTE            = NSE trading days strictly after trade_date up to and including the expiry
                   (trade_date not counted, expiry counted). On expiry day itself DTE == 0,
                   which is <= threshold, so expiry day still gets the 4 lists.
  DTE >  threshold -> BREAKDOWN-PE + BREAKOUT-CE on CURRENT                       (2 lists)
  DTE <= threshold -> EXPIRY-PE + EXPIRY-CE on CURRENT
                      + BREAKDOWN-PE + BREAKOUT-CE on NEXT                        (4 lists)
threshold = config expiry.expiry_dte_threshold (7 since S1.1.0, D59; was 5).

validate_rows() is the gate's alignment check: every stored watchlist row's (list_name,
expiry_date) must be one lists_for() produces for its trade_date.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from typing import Any, Literal

from gammad.core.calendar import TradingCalendar, default_calendar

Side = Literal["PE", "CE"]
Series = Literal["CURRENT", "NEXT"]

BREAKDOWN_PE = "BREAKDOWN-PE"
BREAKOUT_CE = "BREAKOUT-CE"
EXPIRY_PE = "EXPIRY-PE"
EXPIRY_CE = "EXPIRY-CE"
LIST_NAMES: tuple[str, ...] = (BREAKDOWN_PE, BREAKOUT_CE, EXPIRY_PE, EXPIRY_CE)


class ExpiryMissing(LookupError):
    """The adapter's expiry list does not contain an expiry the rule needs."""


class ListAlignmentError(ValueError):
    """A watchlist row's (list_name, expiry_date) is not one lists_for() produces for its date."""


@dataclass(frozen=True)
class ListSpec:
    list_name: str
    side: Side
    expiry_date: date
    series: Series


def _as_date(x: date | str) -> date:
    return x if isinstance(x, date) else date.fromisoformat(str(x)[:10])


def _sorted_unique(expiries: Iterable[date | str]) -> list[date]:
    return sorted({_as_date(e) for e in expiries})


def _threshold(threshold: int | None) -> int:
    if threshold is not None:
        return int(threshold)
    from gammad.config import settings

    return int(settings().expiry.expiry_dte_threshold)


def current_expiry(expiries: Iterable[date | str], trade_date: date) -> date:
    for e in _sorted_unique(expiries):
        if e >= trade_date:
            return e
    raise ExpiryMissing(f"no listed expiry on or after {trade_date}")


def next_expiry(expiries: Iterable[date | str], trade_date: date) -> date:
    exps = _sorted_unique(expiries)
    cur = current_expiry(exps, trade_date)
    later = [e for e in exps if e > cur]
    if not later:
        raise ExpiryMissing(f"no listed expiry after the current one ({cur}) for {trade_date}")
    return later[0]


def dte(trade_date: date, expiry: date, cal: TradingCalendar | None = None) -> int:
    """Trading days strictly after trade_date up to and including expiry.

    The sequence ends ..., 2, 1 (day before expiry), 0 (expiry day). Expiry day (0) is still
    <= threshold, so it gets the 4 lists (coordinator ruling, W2 follow-up).
    """
    if expiry < trade_date:
        raise ValueError(f"expiry {expiry} is before trade_date {trade_date}")
    return (cal or default_calendar()).trading_days_between(trade_date, expiry)


def lists_for(
    trade_date: date,
    expiries: Iterable[date | str],
    *,
    threshold: int | None = None,
    cal: TradingCalendar | None = None,
) -> list[ListSpec]:
    exps = _sorted_unique(expiries)
    cur = current_expiry(exps, trade_date)
    days = dte(trade_date, cur, cal)
    if days > _threshold(threshold):
        return [
            ListSpec(BREAKDOWN_PE, "PE", cur, "CURRENT"),
            ListSpec(BREAKOUT_CE, "CE", cur, "CURRENT"),
        ]
    nxt = next_expiry(exps, trade_date)
    return [
        ListSpec(EXPIRY_PE, "PE", cur, "CURRENT"),
        ListSpec(EXPIRY_CE, "CE", cur, "CURRENT"),
        ListSpec(BREAKDOWN_PE, "PE", nxt, "NEXT"),
        ListSpec(BREAKOUT_CE, "CE", nxt, "NEXT"),
    ]


def validate_rows(
    rows: Iterable[Mapping[str, Any]],
    trade_date: date,
    expiries: Sequence[date | str],
    *,
    threshold: int | None = None,
    cal: TradingCalendar | None = None,
) -> int:
    """Raise ListAlignmentError if any row's (list_name, expiry_date) is not in lists_for.

    Rows carrying a `trade_date` different from `trade_date` are rejected too. Returns the
    number of rows checked.
    """
    allowed = {
        (s.list_name, s.expiry_date) for s in lists_for(trade_date, expiries, threshold=threshold, cal=cal)
    }
    bad: list[str] = []
    n = 0
    for r in rows:
        n += 1
        rd = r.get("trade_date")
        if rd is not None and _as_date(rd) != trade_date:
            bad.append(f"{r.get('list_name')}@{r.get('expiry_date')} trade_date={rd}")
            continue
        key = (str(r.get("list_name")), _as_date(r.get("expiry_date") or ""))
        if key not in allowed:
            bad.append(f"{key[0]}@{key[1].isoformat()} sid={r.get('security_id')}")
    if bad:
        want = ", ".join(f"{name}@{e.isoformat()}" for name, e in sorted(allowed))
        raise ListAlignmentError(
            f"{len(bad)} watchlist row(s) misaligned for {trade_date} (allowed: {want}): {bad[:10]}"
        )
    return n
