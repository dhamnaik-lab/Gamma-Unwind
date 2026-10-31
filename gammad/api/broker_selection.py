"""Stage a broker switch in .env for next app restart; never read or expose secrets via API."""
from __future__ import annotations
import os
import re
import shutil
import tempfile
from datetime import datetime
from pathlib import Path

BROKER_LINE = re.compile(r"^\s*BROKER\s*=", re.I)


def stage_broker(path: Path, broker: str) -> Path | None:
    if not re.fullmatch(r"[a-z]{3,12}", broker):
        raise ValueError("invalid broker")
    path = Path(path)
    if not path.is_file() or path.is_symlink():
        raise ValueError("missing or symlinked .env")
    raw = path.read_bytes()
    if len(raw) > 1_000_000 or b"\x00" in raw:
        raise ValueError("unexpected .env")
    text = raw.decode("utf-8")
    newline = "\r\n" if "\r\n" in text else "\n"
    lines = text.splitlines(keepends=True)
    matches = [i for i, line in enumerate(lines) if BROKER_LINE.match(line)]
    if len(matches) != 1:
        raise ValueError("exactly one BROKER key required")
    i = matches[0]
    current = lines[i].split("=", 1)[1].strip().lower()
    if current == broker:
        return None
    lines[i] = f"BROKER={broker}" + (newline if lines[i].endswith(("\n", "\r")) else "")
    new = "".join(lines).encode("utf-8")
    backup = path.with_name(path.name + ".backup-broker-" + datetime.now().strftime("%Y%m%d-%H%M%S-%f"))
    shutil.copy2(path, backup)
    fd, temp = tempfile.mkstemp(prefix=".gamma-stage-", suffix=".env", dir=path.parent)
    try:
        os.fchmod(fd, 0o600) if hasattr(os, "fchmod") else None
        with os.fdopen(fd, "wb") as out:
            out.write(new)
            out.flush()
            os.fsync(out.fileno())
        os.replace(temp, path)
    finally:
        if os.path.exists(temp):
            os.unlink(temp)
    return backup
