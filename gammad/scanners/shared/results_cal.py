"""G5 results calendar, PORTED (sync, D363) from the owner's market-data bot @92a8538 (2026-10-21).

The file:line provenance lives in docs/provenance/results_calendar.md (not shipped, D646).
"""

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, date, datetime

RESULTS_KEYS = ("financial result", "quarterly result", "audited result", "unaudited")


def is_results_meeting(agenda: str | None) -> bool:
    return any(k in (agenda or "").lower() for k in RESULTS_KEYS)


def parse_nse_date(value) -> date | None:
    for fmt in ("%d-%b-%Y", "%d-%B-%Y", "%Y-%m-%d", "%d-%m-%Y"):
        try:
            return datetime.strptime(str(value).strip(), fmt).replace(tzinfo=UTC).date()
        except ValueError:
            continue
    return None


def results_days(payload, start: date, end: date) -> dict[str, frozenset[date]]:
    rows = payload if isinstance(payload, list) else (
        payload.get("data") or payload.get("rows") or [] if isinstance(payload, dict) else []
    )
    days: dict[str, set[date]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        symbol = (row.get("bm_symbol") or "").strip().upper()
        if not symbol:
            continue
        day = parse_nse_date(row.get("bm_date") or "")
        if day is None or not start <= day <= end:
            continue
        agenda = f"{row.get('bm_purpose') or ''} {row.get('bm_desc') or ''}"
        if not is_results_meeting(agenda):
            continue
        days.setdefault(symbol, set()).add(day)
    return {symbol: frozenset(values) for symbol, values in days.items()}


@dataclass(frozen=True)
class ResultsCalendar:
    start: date
    end: date
    days: Mapping[str, frozenset[date]] = field(default_factory=dict)
    fetched_ms: int | None = None
    stale: bool = False
    reason: str | None = None

    def results_today(self, symbol, day) -> bool:
        return self.start <= day <= self.end and day in self.days.get(symbol.strip().upper(), frozenset())

    def to_dict(self) -> dict:
        return {
            "start": self.start.isoformat(), "end": self.end.isoformat(),
            "days": {symbol: sorted(d.isoformat() for d in days) for symbol, days in self.days.items()},
            "fetched_ms": self.fetched_ms, "stale": self.stale, "reason": self.reason,
        }

    @classmethod
    def from_dict(cls, d):
        return cls(date.fromisoformat(d["start"]), date.fromisoformat(d["end"]),
                   {symbol: frozenset(date.fromisoformat(day) for day in days)
                    for symbol, days in d["days"].items()}, d["fetched_ms"], d["stale"], d["reason"])
