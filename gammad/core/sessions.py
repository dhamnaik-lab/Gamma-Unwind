"""Dated NSE session eras; no reader is wired (0.8.9).

Times are naive IST wall times. Era boundaries are inclusive calendar dates,
independent of trading holidays or per-day special sessions.
"""

from __future__ import annotations

import re
from bisect import bisect_right
from collections.abc import Sequence
from dataclasses import dataclass, replace
from datetime import date, datetime, time, timedelta
from functools import lru_cache
from pathlib import Path

import yaml

from gammad import REPO_ROOT

SCHEMA = "gammad.market_sessions/1"
DEFAULT_PATH = REPO_ROOT / "config" / "market_sessions.yaml"
TIME_FIELDS = ("fo_open", "fo_close", "fo_day_mark_at", "cm_open", "cm_continuous_close")
FIELDS = (*TIME_FIELDS, "cm_closing_auction")
SOURCE_KEYS = ("effective_from", *FIELDS)
ERA_KEYS = frozenset({"era_id", "effective_from", "sources", *FIELDS})
_TIME = re.compile(r"^([01]\d|2[0-3]):[0-5]\d$")


class SessionsError(ValueError):
    """Invalid session-era configuration or uncovered date."""


@dataclass(frozen=True)
class Era:
    era_id: str
    effective_from: date
    effective_to: date | None
    fo_open: time
    fo_close: time
    fo_day_mark_at: time
    cm_open: time
    cm_continuous_close: time
    cm_closing_auction: bool
    sources: tuple[tuple[str, str], ...]


def load_eras(path: Path | None = None) -> tuple[Era, ...]:
    """Read and validate the complete era sequence without caching."""
    doc = yaml.safe_load((DEFAULT_PATH if path is None else path).read_text(encoding="utf-8"))
    if not isinstance(doc, dict) or set(doc) != {"schema", "eras"}:
        raise SessionsError("top level must contain exactly schema and eras")
    if doc["schema"] != SCHEMA:
        raise SessionsError(f"schema must be {SCHEMA}")
    rows = doc["eras"]
    if not isinstance(rows, list) or not rows:
        raise SessionsError("eras must be a non-empty list")

    eras: list[Era] = []
    ids: set[str] = set()
    for row in rows:
        if not isinstance(row, dict):
            raise SessionsError("eras entries must be mappings")
        era_id = row.get("era_id", "<unknown>")
        keys = row.keys()
        if keys != ERA_KEYS:
            raise SessionsError(f"{era_id}: keys {sorted(keys ^ ERA_KEYS)} missing or unknown")
        if not isinstance(era_id, str) or not era_id.strip():
            raise SessionsError("era_id must be a non-empty string")
        if era_id in ids:
            raise SessionsError(f"duplicate era_id: {era_id}")
        ids.add(era_id)
        start = row["effective_from"]
        if not isinstance(start, date) or isinstance(start, datetime):
            raise SessionsError(f"{era_id}: effective_from must be a YAML date")
        if eras and start <= eras[-1].effective_from:
            raise SessionsError("effective_from dates must be strictly ascending")
        sources = row["sources"]
        if not isinstance(sources, dict) or sources.keys() != set(SOURCE_KEYS):
            raise SessionsError(f"{era_id}: sources must contain exactly {SOURCE_KEYS}")
        for k in SOURCE_KEYS:
            cite = sources[k]
            if not isinstance(cite, str) or not cite.strip():
                raise SessionsError(f"{era_id}: sources.{k} must be a non-empty string")
        times = {}
        for key in TIME_FIELDS:
            v = row[key]
            if not isinstance(v, str) or not _TIME.fullmatch(v):
                raise SessionsError(f"{era_id}: {key} must be a quoted 'HH:MM' string, got {v!r}")
            times[key] = time.fromisoformat(v)
        auction = row["cm_closing_auction"]
        if not isinstance(auction, bool):
            raise SessionsError(f"{era_id}: cm_closing_auction must be bool")
        fo_open, fo_close, fo_day_mark_at = (times[k] for k in TIME_FIELDS[:3])
        cm_open, cm_continuous_close = (times[k] for k in TIME_FIELDS[3:])
        if not fo_open < fo_close <= fo_day_mark_at:
            raise SessionsError(f"{era_id}: require fo_open < fo_close <= fo_day_mark_at")
        if not cm_open < cm_continuous_close <= fo_close:
            raise SessionsError(f"{era_id}: require cm_open < cm_continuous_close <= fo_close")
        eras.append(Era(
            era_id=era_id,
            effective_from=start,
            effective_to=None,
            **times,
            cm_closing_auction=auction,
            sources=tuple((k, sources[k]) for k in SOURCE_KEYS),
        ))
    return tuple(
        replace(era, effective_to=eras[i + 1].effective_from - timedelta(days=1))
        if i + 1 < len(eras) else era
        for i, era in enumerate(eras)
    )


@lru_cache(maxsize=1)
def default_eras() -> tuple[Era, ...]:
    """Return the cached, immutable default era sequence."""
    return load_eras()


def era_for(d: date, eras: Sequence[Era] | None = None) -> Era:
    """Find the era covering d; dates before coverage are refused."""
    seq = default_eras() if eras is None else eras
    idx = bisect_right([e.effective_from for e in seq], d) - 1
    if idx < 0:
        raise SessionsError(f"{d} is before the first era")
    return seq[idx]
