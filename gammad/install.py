"""T-198 (D165(3)): the owner's LIVE service runs from its own install of a tag, never from the development tree.

A development tree is a git checkout: `.git` is a folder (a clone) or a file (a worktree). An install made by
`tools/install_live.py` is a `git archive` of a tag: no `.git`, an INSTALL.json stamp (ref, sha, version). A superseded
install carries a RETIRED file naming its successor, so the old folder cannot be started by mistake after an upgrade.

`start_refusal` is checked by `python -m gammad tray|serve` (run_gamma.pyw goes through it and shows this text in its
error box). Only the OWNER edition is refused: client packages never carry `.git` (check L2). A development run of the
service (a probe, a debug session outside market hours) sets GAMMA_DEV_TREE_OK=1.
"""

from __future__ import annotations

import json
import os
from collections.abc import Mapping
from pathlib import Path
from typing import Any

INSTALL_NAME = "INSTALL.json"
RETIRED_NAME = "RETIRED"
DEV_OK_ENV = "GAMMA_DEV_TREE_OK"
REFUSED = "Start refused:"  # run_gamma.pyw shows a SystemExit text starting with it in the error box


def is_dev_tree(root: Path) -> bool:
    """True for a git checkout or worktree (`.git` folder or file)."""
    return (root / ".git").exists()


def read_install(root: Path) -> dict[str, Any] | None:
    """The INSTALL.json stamp of an installed copy, or None (a checkout, a client package, an unreadable file)."""
    path = root / INSTALL_NAME
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    return data if isinstance(data, dict) else None


def start_refusal(root: Path, edition: str, env: Mapping[str, str] | None = None) -> str | None:
    """The sentence that refuses this start, or None when the start may go ahead."""
    env = os.environ if env is None else env
    retired = root / RETIRED_NAME
    if retired.is_file():
        try:
            note = retired.read_text(encoding="utf-8").strip()
        except OSError:
            note = ""
        return (f"{REFUSED} this GAMMA-UNWIND folder was replaced by a newer install and must not start: {root}. "
                + (note or "Start the current install instead."))  # fmt: skip
    if edition == "owner" and is_dev_tree(root) and env.get(DEV_OK_ENV) != "1":
        return (f"{REFUSED} this is the DEVELOPMENT tree ({root}). The live service runs from its own install of a tag "
                "(D165); start it from that folder. For a development run outside market hours set "
                f"{DEV_OK_ENV}=1.")  # fmt: skip
    return None
