"""Upcoming published calendar changes and matching session eras."""

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import yaml

from gammad import REPO_ROOT
from gammad.core import sessions

SCHEMA = "events-calendar/2"
DEFAULT_CALENDAR = REPO_ROOT / "calendars" / "events_calendar.yaml"
WINDOW_DAYS = 30
KIND_TOUCHES = {
    "market_hours_change": "session times",
    "expiry_rule_change": "expiry",
    "lot_size_change": "lots",
}


@dataclass(frozen=True)
class Coming:
    effective_from: date
    days: int
    kind: str
    name: str
    touches: str | None
    era_id: str | None

    @property
    def amber(self) -> bool:
        return self.touches == "session times" and self.era_id is None


def load_events(path: Path | None = None) -> list[dict]:
    doc = yaml.safe_load((DEFAULT_CALENDAR if path is None else path).read_text(encoding="utf-8"))
    if not isinstance(doc, Mapping) or doc.get("schema") != SCHEMA or not isinstance(doc.get("events"), list):
        raise ValueError("events calendar: schema must be events-calendar/2 with an events list")
    return doc["events"]


def coming(
    today: date, events: Iterable[Mapping], eras: Sequence[Any], *, days: int = WINDOW_DAYS
) -> tuple[Coming, ...]:
    result = []
    for row in events:
        if row.get("effective_from") is None:
            continue
        if row.get("status") == "withdrawn":
            continue
        if date.fromisoformat(str(row["date"])) > today:
            continue
        eff = date.fromisoformat(str(row["effective_from"]))
        n = (eff - today).days
        if not 0 <= n <= days:
            continue
        kind = row["kind"]
        touches = KIND_TOUCHES.get(kind)
        era_id = (
            next((e.era_id for e in eras if e.effective_from == eff), None)
            if touches == "session times"
            else None
        )
        result.append(Coming(eff, n, kind, row["name"], touches, era_id))
    return tuple(sorted(result, key=lambda r: (r.effective_from, r.kind, r.name)))


def from_files(today: date) -> tuple[Coming, ...]:
    return coming(today, load_events(), sessions.load_eras())
