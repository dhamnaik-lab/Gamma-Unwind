"""Validated alert targets."""

from __future__ import annotations

import math
from pathlib import Path
from typing import Any

import yaml

from gammad import REPO_ROOT
from gammad.scanners import contract as sc

DEFAULT_PATH: Path = REPO_ROOT / "config" / "v1_targets.yaml"
KEYS = ("t1_pct", "t2_pct", "t3_pct", "t4_pct")
RULE = "0 < t1_pct < t2_pct < t3_pct < t4_pct"


class TargetsError(ValueError):
    """The install must refuse a missing or invalid targets file."""


def _error(source: str, problem: str) -> TargetsError:
    return TargetsError(
        f"{source}: {problem}. Expected four numbers {RULE} (default 25 / 50 / 75 / 100)."
    )


def _check(doc: Any, source: str) -> None:
    if not isinstance(doc, dict):
        raise _error(source, "not a mapping of the target keys")
    for key in KEYS:
        if key not in doc:
            raise _error(source, f"missing {key}")
    for key in doc:
        if key not in KEYS:
            raise _error(source, f"unknown key {key}")
    for key in KEYS:
        value = doc[key]
        try:
            finite = math.isfinite(value) if isinstance(value, (int, float)) else False
        except OverflowError:
            finite = False
        if isinstance(value, bool) or not finite:
            raise _error(source, f"{key} is not a number")
    previous = 0
    for key in KEYS:
        if not previous < doc[key]:
            raise _error(source, f"{key} breaks the order")
        previous = doc[key]


def validate_targets(doc: Any, *, source: str) -> sc.TargetsConfig:
    _check(doc, source)
    return sc.TargetsConfig(*(float(doc[key]) for key in KEYS))


def _load(path: Path) -> Any:
    try:
        text = path.read_text(encoding="utf-8")
    except (OSError, UnicodeError) as exc:
        raise _error(str(path), f"cannot read the targets file ({exc})") from exc
    try:
        return yaml.safe_load(text)
    except yaml.YAMLError as exc:
        raise _error(str(path), f"invalid YAML ({exc})") from exc


def load_targets(path: Path | None = None) -> sc.TargetsConfig:
    path = DEFAULT_PATH if path is None else path
    return validate_targets(_load(path), source=str(path))


def v1_target_prices(px: float, cfg: sc.TargetsConfig) -> tuple[float, float, float, float]:
    t1, t2, t3, t4 = (px * (1 + p / 100) for p in cfg.as_tuple())
    return t1, t2, t3, t4
