"""R1.9 Windows Storage ACL Hardening, Path Safety & Tamper/Rollback Protection.

Architectural rules (ADR-004, ADR-013, G-R18-06):
- Rejects path traversal, symlinks, NTFS junctions, and reparse points across all path prefixes before resolution.
- Inspects or enforces Windows NTFS ACLs via icacls.exe (audit mode by default; enforcement locks down to current user and SYSTEM).
- Tracks monotonic per-database generations and high-water marks in state_generation.json sealed with SHA-256 to detect backup-rollback attacks, tampering, or silent database deletion.
"""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import stat
import subprocess
from contextlib import closing
from pathlib import Path
from typing import Any

from gammad.log import get_logger

log = get_logger(__name__)

FILE_ATTRIBUTE_REPARSE_POINT = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)


class StorageSecurityError(RuntimeError):
    """Raised when a storage path, symlink/junction, or ACL security check fails."""


class StateRollbackError(StorageSecurityError):
    """Raised when a database rollback, missing database, or state generation tamper is detected."""


def _is_reparse_or_link(p: Path) -> bool:
    """Return True if path `p` is a symlink, NTFS junction, or carries FILE_ATTRIBUTE_REPARSE_POINT."""
    try:
        if p.is_symlink():
            return True
    except OSError:
        pass
    if hasattr(p, "is_junction"):
        try:
            if p.is_junction():
                return True
        except OSError:
            pass
    try:
        st = os.lstat(p)
        attrs = getattr(st, "st_file_attributes", 0)
        if attrs & FILE_ATTRIBUTE_REPARSE_POINT:
            return True
    except FileNotFoundError:
        return False
    except OSError:
        return True
    return False


def verify_safe_path(path: Path | str, allowed_root: Path | str) -> Path:
    """Verify that `path` is strictly within `allowed_root` and traverses no symlinks or NTFS junctions.

    Inspects every prefix from `allowed_root` to `path` before `.resolve()` to catch reparse points,
    then verifies `.resolve().is_relative_to(allowed_root.resolve())`.
    """
    raw_root = Path(allowed_root).absolute()
    raw_path = Path(path)
    if not raw_path.is_absolute():
        raw_path = raw_root / raw_path
    else:
        raw_path = raw_path.absolute()

    # Check if raw_root or any of its existing parents up to drive root is a reparse point?
    # Specifically check raw_root itself and all components between raw_root and raw_path.
    if _is_reparse_or_link(raw_root):
        raise StorageSecurityError(f"Allowed root is a symlink or reparse point: {raw_root}")

    try:
        resolved_root = raw_root.resolve(strict=False)
    except OSError as exc:
        raise StorageSecurityError(f"Cannot resolve allowed_root {raw_root}: {exc}") from exc

    # Reject raw traversal segments ("..") before resolution if they escape or attempt traversal
    if ".." in Path(path).parts or ".." in Path(allowed_root).parts:
        raise StorageSecurityError(f"Path traversal segment '..' rejected in path: {path}")

    # Verify lexical containment before walking prefix chain so we never walk above allowed_root
    if not (raw_path.is_relative_to(raw_root) or raw_path.is_relative_to(resolved_root)):
        raise StorageSecurityError(
            f"Path {raw_path} escapes allowed root {resolved_root}"
        )

    check_chain: list[Path] = []
    cur = raw_path
    while True:
        check_chain.append(cur)
        if cur == raw_root or cur == resolved_root or cur.parent == cur:
            break
        cur = cur.parent

    for prefix in reversed(check_chain):
        if _is_reparse_or_link(prefix):
            raise StorageSecurityError(f"Symlink, junction, or reparse point rejected in path hierarchy: {prefix}")

    try:
        resolved_path = raw_path.resolve(strict=False)
    except OSError as exc:
        raise StorageSecurityError(f"Cannot resolve target path {raw_path}: {exc}") from exc

    if not resolved_path.is_relative_to(resolved_root):
        raise StorageSecurityError(
            f"Path {resolved_path} escapes allowed root {resolved_root}"
        )

    if _is_reparse_or_link(resolved_path):
        raise StorageSecurityError(f"Resolved path is a symlink or reparse point: {resolved_path}")

    return resolved_path


def check_or_enforce_acls(data_dir: Path | str, enforce: bool = False) -> dict[str, Any]:
    """Inspect or restrict Windows NTFS ACLs on `data_dir` via `icacls.exe`.

    When `enforce=False` (default non-destructive audit mode), inspects ACLs and flags
    broad/insecure principals (`Everyone` / `*S-1-1-0`, `BUILTIN\\Users` / `*S-1-5-32-545`,
    `Authenticated Users` / `*S-1-5-11`) without modifying system permissions.
    When `enforce=True`, disables inheritance (`/inheritance:r`) and grants Full Control `(OI)(CI)(F)`
    solely to the current user (`USERNAME`) and `NT AUTHORITY\\SYSTEM` (`*S-1-5-18`).
    """
    target = Path(data_dir).resolve()
    if not target.exists():
        raise StorageSecurityError(f"ACL target directory does not exist: {target}")
    if _is_reparse_or_link(target):
        raise StorageSecurityError(f"ACL target directory is a reparse point or symlink: {target}")

    username = os.environ.get("USERNAME") or os.getlogin()
    icacls_exe = "icacls.exe"

    if enforce:
        cmd_enforce = [
            icacls_exe,
            str(target),
            "/inheritance:r",
            "/grant:r",
            f"{username}:(OI)(CI)(F)",
            "*S-1-5-18:(OI)(CI)(F)",
            "/remove",
            "*S-1-1-0",
            "*S-1-5-32-545",
            "*S-1-5-11",
            "*S-1-5-32-544",
        ]
        res = subprocess.run(
            cmd_enforce,
            capture_output=True,
            text=True,
            timeout=15.0,
            check=False,
        )
        if res.returncode != 0:
            raise StorageSecurityError(
                f"icacls enforcement failed (rc={res.returncode}): {res.stderr.strip() or res.stdout.strip()}"
            )

    cmd_check = [icacls_exe, str(target)]
    res_check = subprocess.run(
        cmd_check,
        capture_output=True,
        text=True,
        timeout=15.0,
        check=False,
    )
    if res_check.returncode != 0:
        raise StorageSecurityError(
            f"icacls inspection failed (rc={res_check.returncode}): {res_check.stderr.strip() or res_check.stdout.strip()}"
        )

    output = res_check.stdout
    insecure_principals: list[str] = []
    broad_markers = (
        "everyone:",
        "builtin\\users:",
        "nt authority\\authenticated users:",
        "authenticated users:",
        "*s-1-1-0:",
        "*s-1-5-32-545:",
        "*s-1-5-11:",
    )
    for line in output.splitlines():
        line_clean = line.strip().lower()
        for marker in broad_markers:
            if marker in line_clean:
                insecure_principals.append(line.strip())

    is_secure = len(insecure_principals) == 0
    return {
        "path": str(target),
        "enforced": bool(enforce),
        "secure": is_secure,
        "username": username,
        "insecure_entries": insecure_principals,
        "raw_acl": output.strip(),
    }


SEAL_APP_ID_MAGIC = 0x47550000  # 'GU' in upper 16 bits of SQLite PRAGMA application_id


def _decode_sealed_gen(app_id: int) -> int:
    """Decode the generation counter stamped in `PRAGMA application_id` (0 if unstamped)."""
    unsigned = app_id & 0xFFFFFFFF
    if (unsigned & 0xFFFF0000) == SEAL_APP_ID_MAGIC:
        return int(unsigned & 0xFFFF)
    return 0


def _stamp_db_generation(db_path: Path, generation: int) -> None:
    """Stamp `PRAGMA application_id` with `SEAL_APP_ID_MAGIC | generation` if `db_path` exists on disk."""
    if not db_path.exists() or not db_path.is_file():
        return
    try:
        if db_path.stat().st_size == 0:
            return
    except OSError:
        return
    app_id = SEAL_APP_ID_MAGIC | min(max(int(generation), 1), 0xFFFF)
    with closing(sqlite3.connect(str(db_path), timeout=3.0)) as conn:
        conn.execute("PRAGMA busy_timeout=3000")
        conn.execute(f"PRAGMA application_id = {app_id}")
        conn.commit()


def inspect_db_high_water_mark(db_path: Path) -> dict[str, int]:
    """Extract monotonic high-water mark metrics from a SQLite database (`gamma.db` or `watermark.db`).

    Returns a dictionary of `{metric_name: int_value}` that never regresses during normal operation.
    """
    if not db_path.exists():
        return {"exists": 0, "max_marker": 0, "row_count": 0, "sealed_gen": 0}

    metrics: dict[str, int] = {"exists": 1, "max_marker": 0, "row_count": 0, "sealed_gen": 0}
    with closing(sqlite3.connect(str(db_path), timeout=3.0)) as conn:
        conn.execute("PRAGMA busy_timeout=3000")
        app_id_row = conn.execute("PRAGMA application_id").fetchone()
        if app_id_row is not None:
            metrics["sealed_gen"] = _decode_sealed_gen(int(app_id_row[0] or 0))

        tables = {
            str(r[0])
            for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        }
        max_marker = 0
        total_rows = 0

        if "feed_watermarks" in tables:
            row = conn.execute(
                "SELECT COALESCE(MAX(ltt_ms), 0), COUNT(*) FROM feed_watermarks"
            ).fetchone()
            if row:
                max_marker = max(max_marker, int(row[0] or 0))
                total_rows += int(row[1] or 0)

        for tbl, col in (
            ("signals", "id"),
            ("trades", "id"),
            ("trade_events", "id"),
            ("alerts", "id"),
        ):
            if tbl in tables:
                row = conn.execute(
                    f"SELECT COALESCE(MAX({col}), 0), COUNT(*) FROM {tbl}"
                ).fetchone()
                if row:
                    max_marker = max(max_marker, int(row[0] or 0))
                    total_rows += int(row[1] or 0)

        if "runs" in tables:
            row = conn.execute("SELECT COUNT(*) FROM runs").fetchone()
            if row:
                total_rows += int(row[0] or 0)

        metrics["max_marker"] = max_marker
        metrics["row_count"] = total_rows
    return metrics


class StateGenerationManager:
    """Tracks monotonic per-DB generations and high-water marks in `state_generation.json` with a SHA-256 seal.

    Detects:
    - Manual tampering with `state_generation.json` (seal mismatch -> `StateRollbackError`).
    - Silent deletion of `state_generation.json` when previously sealed databases already exist on disk
      (via `PRAGMA application_id` generation stamp or `require_existing_seal=True` -> `StateRollbackError`),
      or unexpected deletion of a previously sealed database file.
    - Restoring an older database backup whose generation stamp or high-water mark (`max_marker` or `row_count`)
      regressed below the sealed state (`StateRollbackError`).
    """

    SEAL_DOMAIN = "GAMMA_UNWIND_R19_STATE_GENERATION_V1"

    def __init__(self, metadata_path: Path | str) -> None:
        self.metadata_path = Path(metadata_path).resolve()

    @classmethod
    def _compute_seal(cls, databases: dict[str, dict[str, Any]]) -> str:
        payload = {
            "domain": cls.SEAL_DOMAIN,
            "databases": databases,
        }
        canonical = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        return hashlib.sha256(canonical).hexdigest()

    def load_state(self) -> dict[str, Any]:
        """Load and verify the persisted generation state. Returns empty dict if file does not exist yet."""
        if not self.metadata_path.exists():
            return {"databases": {}, "seal": None}
        if _is_reparse_or_link(self.metadata_path):
            raise StateRollbackError(f"State generation file is a symlink or reparse point: {self.metadata_path}")
        try:
            raw = json.loads(self.metadata_path.read_text(encoding="utf-8"))
        except (OSError, ValueError, TypeError) as exc:
            raise StateRollbackError(f"Corrupt or unreadable state generation file: {exc}") from exc

        if not isinstance(raw, dict):
            raise StateRollbackError("State generation payload must be a JSON object")

        databases = raw.get("databases")
        seal = raw.get("seal")
        if not isinstance(databases, dict) or not isinstance(seal, str):
            raise StateRollbackError("State generation file missing 'databases' or 'seal'")

        expected_seal = self._compute_seal(databases)
        if seal != expected_seal:
            raise StateRollbackError("State generation seal mismatch: tampering detected")

        return {"databases": databases, "seal": seal}

    def _save_state(self, databases: dict[str, dict[str, Any]]) -> dict[str, Any]:
        self.metadata_path.parent.mkdir(parents=True, exist_ok=True)
        if _is_reparse_or_link(self.metadata_path):
            raise StateRollbackError(f"Refusing to write state generation to reparse point: {self.metadata_path}")
        seal = self._compute_seal(databases)
        doc = {
            "version": 1,
            "databases": databases,
            "seal": seal,
        }
        tmp_path = self.metadata_path.with_suffix(".json.tmp")
        tmp_path.write_text(json.dumps(doc, indent=2, sort_keys=True), encoding="utf-8")
        tmp_path.replace(self.metadata_path)
        return doc

    def verify_and_advance(
        self,
        db_path: Path | str,
        *,
        db_key: str | None = None,
        allow_quarantine_reset: bool = False,
        require_existing_seal: bool = False,
    ) -> dict[str, Any]:
        """Verify that `db_path` has not regressed relative to the sealed state, then advance and persist the seal.

        If `allow_quarantine_reset=True`, records an authorized quarantine reset event (incrementing generation
        and resetting high-water marks for the freshly rebuilt database).
        If `require_existing_seal=True`, raises `StateRollbackError` if `db_key` is not already sealed.
        Also raises `StateRollbackError` if `db_path` carries a prior `PRAGMA application_id` seal stamp
        while `state_generation.json` is missing its entry (detecting `state_generation.json` deletion).
        """
        target = Path(db_path).resolve()
        key = db_key or target.name
        state = self.load_state()
        databases: dict[str, dict[str, Any]] = dict(state["databases"])

        current_hwm = inspect_db_high_water_mark(target)
        prev = databases.get(key)

        if prev is None and not allow_quarantine_reset:
            if require_existing_seal:
                raise StateRollbackError(
                    f"State generation seal required for '{key}', but no sealed entry exists in {self.metadata_path}"
                )
            if current_hwm["sealed_gen"] > 0:
                raise StateRollbackError(
                    f"Database '{key}' carries sealed generation stamp {current_hwm['sealed_gen']} "
                    f"but '{self.metadata_path.name}' has no record for it (state_generation.json deletion detected)"
                )

        if prev is not None and not allow_quarantine_reset:
            prev_gen = int(prev.get("generation", 0))
            prev_exists = int(prev.get("exists", 0))
            prev_marker = int(prev.get("max_marker", 0))
            prev_rows = int(prev.get("row_count", 0))

            if prev_exists == 1 and current_hwm["exists"] == 0:
                raise StateRollbackError(
                    f"Database '{key}' was previously sealed (generation {prev_gen}) "
                    "but is now missing from disk"
                )
            if 0 < current_hwm["sealed_gen"] < prev_gen:
                raise StateRollbackError(
                    f"Database '{key}' generation stamp regressed: db_stamp={current_hwm['sealed_gen']} "
                    f"< sealed={prev_gen} (backup rollback detected)"
                )
            if current_hwm["max_marker"] < prev_marker:
                raise StateRollbackError(
                    f"Database '{key}' high-water mark regressed: current={current_hwm['max_marker']} "
                    f"< sealed={prev_marker} (backup rollback detected)"
                )
            if current_hwm["row_count"] < prev_rows:
                raise StateRollbackError(
                    f"Database '{key}' row count regressed: current={current_hwm['row_count']} "
                    f"< sealed={prev_rows} (backup rollback detected)"
                )

        next_gen = (int(prev.get("generation", 0)) if prev else 0) + 1
        if current_hwm["exists"] == 1:
            _stamp_db_generation(target, next_gen)
            current_hwm["sealed_gen"] = min(next_gen, 0xFFFF)

        entry: dict[str, Any] = {
            "generation": next_gen,
            "exists": current_hwm["exists"],
            "max_marker": current_hwm["max_marker"],
            "row_count": current_hwm["row_count"],
            "quarantine_resets": (int(prev.get("quarantine_resets", 0)) if prev else 0)
            + (1 if allow_quarantine_reset else 0),
        }
        databases[key] = entry
        self._save_state(databases)
        return entry
