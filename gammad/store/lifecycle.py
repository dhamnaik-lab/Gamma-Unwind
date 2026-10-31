"""T-397 (D443 (4)): fold the SQLite WAL and close the store at a service stop.

13 Oct: after tray -> Quit the 0.8.7 service had stopped, yet data/gamma.db-wal stayed 6,258,312 B and
tools/install_live.py switch refused. SQLite folds and deletes the WAL only when the LAST connection on the file
closes; any other connection still open (a job on a worker thread, a leak) leaves it behind. `close_store()` runs
`PRAGMA wal_checkpoint(TRUNCATE)` first, which empties the WAL even then (unless a reader is mid-read).

Kept out of gammad/store/db.py on purpose: db.py is Lab-pinned (D343), so this module calls only its public API.
Every function here is idempotent and never raises (the shutdown path logs and carries on).
"""

from __future__ import annotations

import sqlite3
import sys
import threading
import weakref
from pathlib import Path

from gammad.log import get_logger

log = get_logger(__name__)

# a shutdown checkpoint waits at most this long for a busy reader / writer: the connection's own 30 s timeout would
# outlast the tray's 10 s stop wait, after which the tray TERMINATES the service and nothing is checkpointed
CHECKPOINT_BUSY_MS = 2000


class TrackedConnection(sqlite3.Connection):
    """A connection that knows its file, where it was opened, on which thread, and whether close() ran, so a stop
    can name every one still open on its store (sqlite3.Connection itself takes no attributes or weak references)."""

    db_file: str = ""
    opened_at: str = ""
    thread_id: int = 0
    closed: bool = False

    def close(self) -> None:
        super().close()
        self.closed = True


_OPEN: weakref.WeakSet[TrackedConnection] = weakref.WeakSet()
_OPEN_LOCK = threading.Lock()


def _resolved(path: Path | str) -> str:
    try:
        return str(Path(path).resolve())
    except OSError:
        return str(Path(path).absolute())


def open_tracked(path: Path | str, *, timeout: float = 30.0, depth: int = 1) -> TrackedConnection:
    """`sqlite3.connect(path, timeout=timeout)` as a TrackedConnection (no PRAGMA set, no row factory: exactly what a
    bare sqlite3.connect gives), registered for open_connections(). `depth` = the frame named in opened_at."""
    conn = sqlite3.connect(path, timeout=timeout, factory=TrackedConnection)
    try:
        f = sys._getframe(depth)
        conn.opened_at = f"{Path(f.f_code.co_filename).name}:{f.f_lineno} {f.f_code.co_name}"
    except ValueError:  # no such frame
        conn.opened_at = "?"
    conn.db_file = _resolved(path)
    conn.thread_id = threading.get_ident()
    with _OPEN_LOCK:
        _OPEN.add(conn)
    return conn


def open_connections(path: Path | str | None = None) -> list[TrackedConnection]:
    """Every tracked connection not yet closed (on `path`'s file when given)."""
    want = None if path is None else _resolved(path)
    with _OPEN_LOCK:
        live = list(_OPEN)
    return [c for c in live if not c.closed and (want is None or c.db_file == want)]


def wal_path(db_file: Path | str) -> Path:
    p = Path(db_file)
    return p.with_name(p.name + "-wal")


def wal_bytes(db_file: Path | str) -> int:
    """Size of the store's -wal file; 0 when absent."""
    try:
        return wal_path(db_file).stat().st_size
    except OSError:
        return 0


def is_closed(conn: sqlite3.Connection) -> bool:
    """True once close() ran (a connection of another thread reads as open: sqlite3 refuses to touch it)."""
    if getattr(conn, "closed", False):
        return True
    try:
        _ = conn.total_changes  # raises ProgrammingError on a closed connection
    except sqlite3.ProgrammingError as exc:
        return "closed" in str(exc).lower()
    return False


def checkpoint(conn: sqlite3.Connection, *, label: str = "store",
               busy_ms: int = CHECKPOINT_BUSY_MS) -> tuple[int, int, int] | None:  # fmt: skip
    """`PRAGMA wal_checkpoint(TRUNCATE)` on `conn` -> (busy, log frames, checkpointed frames), or None when it could
    not run. The connection's own busy timeout is restored after (it may still serve writes)."""
    try:
        if is_closed(conn):
            return None
        before = int(conn.execute("PRAGMA busy_timeout").fetchone()[0])
        conn.execute(f"PRAGMA busy_timeout = {int(busy_ms)}")
        try:
            row = conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
        finally:
            conn.execute(f"PRAGMA busy_timeout = {before}")
        res = (int(row[0]), int(row[1]), int(row[2]))
    except Exception as exc:  # noqa: BLE001 -- the shutdown path must never raise
        log.warning("store: %s checkpoint failed: %s", label, exc)
        return None
    if res[0]:
        log.warning("store: %s checkpoint BUSY (busy=%d log=%d checkpointed=%d): a reader holds the WAL",
                    label, *res)  # fmt: skip
    else:
        log.info("store: %s checkpoint ok (log=%d checkpointed=%d)", label, res[1], res[2])
    return res


def close_store(conn: sqlite3.Connection | None, *, label: str = "store") -> tuple[int, int, int] | None:
    """checkpoint(TRUNCATE) then close. A closed or None connection is a no-op. Returns the checkpoint result."""
    if conn is None or is_closed(conn):
        return None
    res = checkpoint(conn, label=label)
    try:
        conn.close()
    except Exception as exc:  # noqa: BLE001 -- the shutdown path must never raise
        log.warning("store: %s close failed: %s", label, exc)
    return res


def report_open(path: Path | str, *, label: str = "store") -> list[str]:
    """Name every tracked connection still open on `path` (a job on a worker thread: sqlite3 refuses a close from
    another thread), so a stop that leaves a WAL behind says who held the store. Never raises."""
    try:
        left = [f"{c.opened_at} (thread {c.thread_id})" for c in open_connections(path)]
    except Exception:  # the shutdown path must never raise
        log.exception("store: listing open connections failed")
        return []
    if left:
        log.warning("store: %s connection(s) still open on another thread at shutdown: %s", label, "; ".join(left))
    return left
