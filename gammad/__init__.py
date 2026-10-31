"""GAMMA-UNWIND daemon package (strategy spec lives in docs/STRATEGY-SPEC.md)."""

from __future__ import annotations

from pathlib import Path

REPO_ROOT: Path = Path(__file__).resolve().parent.parent


def _read_version() -> str:
    try:
        return (REPO_ROOT / "VERSION").read_text(encoding="utf-8").strip() or "0.0.0"
    except OSError:
        return "0.0.0"


__version__: str = _read_version()

__all__ = ["REPO_ROOT", "__version__"]
