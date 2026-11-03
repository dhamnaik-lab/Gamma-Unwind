"""R1.9 SQLite Pre-flight Integrity Checker & Corruption Quarantine Recovery Engine.

Architectural rules (ADR-004, ADR-013, G-R18-06):
- Pre-flight integrity verification via `PRAGMA quick_check(1)`, SQLite magic header validation, and WAL frame header inspection.
- All database connections wrapped in `contextlib.closing` with explicit `timeout=3.0` to guarantee zero handle leaks (`ResourceWarning`).
- Graceful quarantine moves damaged `.db`, `-wal`, and `-shm` files into `data/quarantine/<name>_<YYYYMMDD_HHMMSS>.corrupt*` and initializes a clean replacement database with `journal_mode=WAL` and `synchronous=FULL`.
"""

from __future__ import annotations

import gc
import os
import shutil
import sqlite3
import stat
import time
from collections.abc import Callable
from contextlib import closing
from datetime import datetime
from pathlib import Path
from typing import Any

from gammad.config import IST
from gammad.log import get_logger

log = get_logger(__name__)

SQLITE_HEADER_MAGIC = b"SQLite format 3\x00"
WAL_MAGIC_VALUES = (0x377F0682, 0x377F0683)
WAL_HEADER_SIZE = 32
WAL_FRAME_HEADER_SIZE = 24


def _verify_wal_file(wal_path: Path) -> bool:
    """Inspect a non-empty `-wal` file to ensure its header magic and page size are valid SQLite WAL format."""
    if not wal_path.exists():
        return True
    try:
        size = wal_path.stat().st_size
        if size == 0:
            return True
        if size < WAL_HEADER_SIZE:
            return False
        with wal_path.open("rb") as f:
            hdr = f.read(WAL_HEADER_SIZE)
        magic = int.from_bytes(hdr[0:4], "big")
        fmt_ver = int.from_bytes(hdr[4:8], "big")
        page_size = int.from_bytes(hdr[8:12], "big")
        if magic not in WAL_MAGIC_VALUES or fmt_ver != 3007000:
            return False
        if page_size < 512 or page_size > 65536 or (page_size & (page_size - 1)) != 0:
            return False
        # Check if remaining bytes after header are a multiple of (frame_header + page_size)
        rem = size - WAL_HEADER_SIZE
        frame_total = WAL_FRAME_HEADER_SIZE + page_size
        if rem > 0 and rem % frame_total != 0:
            return False
    except OSError:
        return False
    return True


def verify_database_integrity(db_path: Path | str) -> bool:
    """Verify SQLite database and WAL integrity.

    Returns `True` if the file does not exist yet (fresh start) or passes header, WAL, and
    `PRAGMA quick_check(1)` checks. Returns `False` if corruption or malformation is detected.
    """
    target = Path(db_path)
    if not target.exists():
        # Even if main .db does not exist, a stray corrupt -wal file is invalid
        wal_path = target.with_name(target.name + "-wal")
        if wal_path.exists() and wal_path.stat().st_size > 0:
            return False
        return True

    try:
        if not target.is_file():
            return False
        size = target.stat().st_size
        if size > 0:
            if size < 100:
                return False
            with target.open("rb") as f:
                magic = f.read(16)
            if magic != SQLITE_HEADER_MAGIC:
                return False
    except OSError:
        return False

    wal_path = target.with_name(target.name + "-wal")
    if not _verify_wal_file(wal_path):
        return False

    try:
        with closing(sqlite3.connect(str(target), timeout=3.0)) as conn:
            conn.execute("PRAGMA busy_timeout=3000")
            # Force SQLite to read sqlite_master and run quick_check(1)
            _ = conn.execute("SELECT count(*) FROM sqlite_master").fetchone()
            rows = [str(r[0]) for r in conn.execute("PRAGMA quick_check(1)").fetchall()]
            if rows != ["ok"]:
                return False
    except sqlite3.Error as exc:
        log.warning("store recovery: integrity check failed on %s (%s)", target, exc)
        return False

    return True


def init_watermark_schema(conn: sqlite3.Connection) -> None:
    """Initialize the durable watermark table schema on a recovered `watermark.db`."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS feed_watermarks (
            namespace TEXT NOT NULL,
            segment TEXT NOT NULL,
            security_id INTEGER NOT NULL,
            ltt_ms INTEGER NOT NULL,
            payload_digest TEXT NOT NULL,
            PRIMARY KEY(namespace, segment, security_id)
        ) WITHOUT ROWID
    """)
    conn.commit()


def quarantine_and_recover(
    db_path: Path | str,
    schema_init_fn: Callable[[sqlite3.Connection], Any] | None = None,
    *,
    quarantine_dir: Path | str | None = None,
    now_fn: Callable[[], datetime] | None = None,
) -> Path:
    """Isolate a damaged SQLite database (`.db`, `-wal`, `-shm`) to `data/quarantine/` and rebuild a clean store.

    Moves `<dbname>.db`, `<dbname>.db-wal`, and `<dbname>.db-shm` into:
      `data/quarantine/<dbname>_<YYYYMMDD_HHMMSS>.corrupt` (plus `-wal` / `-shm` suffixes).
    Creates a replacement database with `PRAGMA journal_mode=WAL` and `PRAGMA synchronous=FULL`,
    and runs `schema_init_fn(conn)` when provided.
    Returns the quarantine destination path of the primary `.corrupt` file.
    """
    target = Path(db_path).resolve()
    q_dir = Path(quarantine_dir).resolve() if quarantine_dir is not None else (target.parent / "quarantine")
    q_dir.mkdir(parents=True, exist_ok=True)

    ts = (now_fn() if now_fn is not None else datetime.now(IST)).strftime("%Y%m%d_%H%M%S")
    stem = target.name
    base_quarantine = q_dir / f"{stem}_{ts}.corrupt"
    # Avoid collision if called multiple times within the same second
    counter = 1
    while base_quarantine.exists() or Path(str(base_quarantine) + "-wal").exists():
        base_quarantine = q_dir / f"{stem}_{ts}_{counter}.corrupt"
        counter += 1

    moved_files: list[str] = []
    for suffix in ("", "-wal", "-shm"):
        src = target.with_name(target.name + suffix)
        if src.exists():
            dst = Path(str(base_quarantine) + suffix)
            for attempt in range(3):
                try:
                    os.chmod(src, stat.S_IWRITE | stat.S_IREAD)
                except OSError:
                    pass
                try:
                    shutil.move(str(src), str(dst))
                    break
                except PermissionError:
                    if attempt == 2:
                        raise
                    gc.collect()
                    time.sleep(0.05)
            moved_files.append(str(dst))

    log.error(
        "AUDIT ALERT [QUARANTINE]: Corrupted SQLite database %s isolated to %s (files: %s); rebuilding clean WAL store",
        target,
        base_quarantine,
        ", ".join(moved_files) if moved_files else "none",
    )

    target.parent.mkdir(parents=True, exist_ok=True)
    with closing(sqlite3.connect(str(target), timeout=10.0)) as conn:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=FULL")
        conn.execute("PRAGMA foreign_keys=ON")
        if schema_init_fn is not None:
            schema_init_fn(conn)
        conn.commit()

    return base_quarantine
