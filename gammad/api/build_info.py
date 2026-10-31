"""T-171 (PLAN-W17 §2.3): which build is running -- `/api/status.build` = {commit, bundle}.

commit  the short sha of the running code: an installed package's MANIFEST.json `git_sha` (the packager writes it;
        no new root file, review R1) cut to 7, else `git rev-parse --short=7 HEAD` in a checkout, else null.
        Resolved ONCE by create_app (review R4: never on a status call, which can be the WS path on the tick loop),
        windowless on Windows (the service has no console), bounded by a short timeout. Any failure is null and at
        most a DEBUG line: never an exception, never an ERROR line.
bundle  the parsed build.json of the SERVED dist folder ({version, commit, built_ist}; the vite build writes it), or
        null (every bundle built before W17). One os.stat per status body; parsed again only when the file's
        (mtime, size) changes (status_body runs on the event loop for the 2 s WS pushes).
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Any

from gammad.log import get_logger

log = get_logger(__name__)

GIT = "git"
GIT_TIMEOUT_S = 3.0
SHORT = 7
MANIFEST_NAME = "MANIFEST.json"
INSTALL_NAME = "INSTALL.json"  # T-198: an owner live install (tools/install_live.py); same `git_sha` key
BUNDLE_NAME = "build.json"
BUNDLE_KEYS = ("version", "commit", "built_ist")
BUNDLE_VALUE_MAX = 64
_SHA = re.compile(r"[0-9a-f]{7,40}")


def _manifest_sha(root: Path) -> str | None:
    for name in (MANIFEST_NAME, INSTALL_NAME):  # a client package's MANIFEST.json, an owner install's INSTALL.json
        sha = _stamp_sha(root / name)
        if sha is not None:
            return sha
    return None


def _stamp_sha(path: Path) -> str | None:
    if not path.is_file():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        log.debug("build info: %s unreadable (%s)", path.name, type(exc).__name__)
        return None
    sha = data.get("git_sha") if isinstance(data, dict) else None
    if isinstance(sha, str) and _SHA.fullmatch(sha.strip().lower()):
        return sha.strip().lower()[:SHORT]
    return None


def _git_sha(root: Path) -> str | None:
    if not (root / ".git").exists():  # never report an ENCLOSING repository's commit for an unzipped package
        return None
    flags = getattr(subprocess, "CREATE_NO_WINDOW", 0) if sys.platform == "win32" else 0
    try:
        done = subprocess.run(
            [GIT, "rev-parse", f"--short={SHORT}", "HEAD"],
            cwd=root, capture_output=True, text=True, timeout=GIT_TIMEOUT_S, stdin=subprocess.DEVNULL,
            creationflags=flags, check=False,
        )  # fmt: skip
    except (OSError, subprocess.SubprocessError, ValueError) as exc:
        log.debug("build info: git unavailable (%s)", type(exc).__name__)
        return None
    out = (done.stdout or "").strip().lower()
    if done.returncode != 0 or not _SHA.fullmatch(out):
        log.debug("build info: git rev-parse answered %s", done.returncode)
        return None
    return out


def resolve_commit(root: Path) -> str | None:
    """The running code's short sha: MANIFEST.json git_sha first, then git, else None. Never raises."""
    try:
        return _manifest_sha(root) or _git_sha(root)
    except Exception as exc:  # noqa: BLE001 -- a status key must never take the API down
        log.debug("build info: commit unresolved (%s)", type(exc).__name__)
        return None


def _clean(value: Any) -> str | None:
    if not isinstance(value, str) or not value.strip():
        return None
    return value.strip()[:BUNDLE_VALUE_MAX]


class BundleFile:
    """build.json of one dist folder, parsed once per (mtime_ns, size) change."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self._key: tuple[int, int] | None = None
        self._value: dict[str, str | None] | None = None
        self.parses = 0

    def read(self) -> dict[str, str | None] | None:
        try:
            st = os.stat(self.path)
        except OSError:
            self._key, self._value = None, None
            return None
        key = (st.st_mtime_ns, st.st_size)
        if key != self._key:
            self._key, self._value = key, self._parse()
        return None if self._value is None else dict(self._value)

    def _parse(self) -> dict[str, str | None] | None:
        self.parses += 1
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            log.debug("build info: %s unreadable (%s)", BUNDLE_NAME, type(exc).__name__)
            return None
        if not isinstance(data, dict):
            return None
        return {k: _clean(data.get(k)) for k in BUNDLE_KEYS}


class BuildInfo:
    """What create_app resolves once: the commit, and the served bundle's build.json (read now, then per change)."""

    def __init__(self, commit: str | None, dist: Path) -> None:
        self.commit = commit
        self.bundle = BundleFile(dist / BUNDLE_NAME)
        self.bundle.read()

    def block(self) -> dict[str, Any]:
        return {"commit": self.commit, "bundle": self.bundle.read()}


NULL_BLOCK: dict[str, Any] = {"commit": None, "bundle": None}
