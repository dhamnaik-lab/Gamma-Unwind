"""Stage a broker switch in .env for next app restart; never read or expose secrets via API."""
from __future__ import annotations

import os
import re
import tempfile
from datetime import datetime, timezone
from pathlib import Path

BROKER_LINE = re.compile(r"^\s*BROKER\s*=", re.I)


def write_env_backup(
    env_path: Path,
    raw_before: bytes,
    prefix: str = ".env.backup-",
    *,
    keep: int = 3,
) -> Path:
    """Write a timestamped .env backup inside an ACL-restricted `data/backups` folder and prune old backups."""
    root = Path(env_path).resolve().parent
    backup_dir = root / "data" / "backups"
    backup_dir.mkdir(parents=True, exist_ok=True)
    if os.name == "nt":
        from gammad.store.security import check_or_enforce_acls

        check_or_enforce_acls(backup_dir, enforce=True)
    else:
        os.chmod(backup_dir, 0o700)

    stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S-%f")
    backup = backup_dir / f"{prefix}{stamp}.bak"
    if backup.exists():
        raise ValueError("backup name conflict")

    fd, temp = tempfile.mkstemp(prefix=".gamma-bak-", suffix=".tmp", dir=backup_dir)
    try:
        if hasattr(os, "fchmod"):
            os.fchmod(fd, 0o600)
        with os.fdopen(fd, "wb") as out:
            out.write(raw_before)
            out.flush()
            os.fsync(out.fileno())
        os.replace(temp, backup)
    finally:
        if os.path.exists(temp):
            os.unlink(temp)

    if keep > 0:
        existing = sorted(
            (p for p in backup_dir.iterdir() if p.is_file() and p.name.startswith(".env.")),
            key=lambda p: (p.stat().st_mtime_ns, p.name),
            reverse=True,
        )
        for stale in existing[keep:]:
            try:
                stale.unlink()
            except OSError:
                pass
    return backup


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
    backup = write_env_backup(path, raw, prefix=".env.backup-broker-")
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
