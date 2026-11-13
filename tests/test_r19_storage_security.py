"""R1.9 Phase 5 Unit & Integration Tests: Storage Security, NTFS ACLs, Rollback Seal & Corruption Quarantine.

Validates:
1. `verify_safe_path()` path traversal, symlink, and Windows NTFS junction (`mklink /J`) rejection.
2. `check_or_enforce_acls()` Windows `icacls.exe` audit and lockdown enforcement on isolated temp dirs.
3. `StateGenerationManager` monotonic high-water mark progression, backup-rollback attack detection
   (`StateRollbackError`), tamper seal verification, and missing database detection.
4. `verify_database_integrity()` and `quarantine_and_recover()` detection of corrupted `.db` and `-wal`
   files, isolation into `data/quarantine/<name>_<YYYYMMDD_HHMMSS>.corrupt*`, and clean WAL/FULL rebuild.
5. `serve_async()` / `_preflight_storage_guard()` integration covering clean startup, corrupt database
   auto-quarantine recovery, and backup-rollback refusal prior to opening Runtime connections.

Runs offline with Windows Python 3.13. Zero live broker calls, zero real orders, zero ResourceWarnings.
"""

from __future__ import annotations

import json
import shutil
import sqlite3
import subprocess
import tempfile
import unittest
from contextlib import closing
from dataclasses import replace
from datetime import datetime
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

from gammad.app import _preflight_storage_guard, serve_async
from gammad.broker.durable_watermark import WatermarkStore
from gammad.config import IST, settings
from gammad.store import db
from gammad.store.recovery import (
    init_watermark_schema,
    quarantine_and_recover,
    verify_database_integrity,
)
from gammad.store.security import (
    StateGenerationManager,
    StateRollbackError,
    StorageSecurityError,
    check_or_enforce_acls,
    verify_safe_path,
)
from gammad.watch.feed_protocol import Tick


class TestStorageSecurityPathsAndACLs(unittest.TestCase):
    def setUp(self) -> None:
        self.base_dir = Path(tempfile.mkdtemp(prefix="gamma_p5_sec_")).resolve()
        self.allowed_root = self.base_dir / "data"
        self.allowed_root.mkdir(parents=True, exist_ok=True)
        self.outside_dir = self.base_dir / "outside"
        self.outside_dir.mkdir(parents=True, exist_ok=True)

    def tearDown(self) -> None:
        # Remove any junctions safely before rmtree
        for p in self.allowed_root.glob("*"):
            if hasattr(p, "is_junction") and p.is_junction():
                subprocess.run(["cmd.exe", "/c", "rmdir", str(p)], capture_output=True, check=False)
        shutil.rmtree(self.base_dir, ignore_errors=True)

    def test_verify_safe_path_valid(self) -> None:
        db_file = self.allowed_root / "gamma.db"
        resolved = verify_safe_path(db_file, self.allowed_root)
        self.assertEqual(resolved, db_file.resolve())

        # Subdirectory inside allowed_root
        sub = self.allowed_root / "nested" / "watermark.db"
        sub.parent.mkdir(parents=True, exist_ok=True)
        self.assertEqual(verify_safe_path(sub, self.allowed_root), sub.resolve())

    def test_verify_safe_path_rejects_traversal_and_outside_paths(self) -> None:
        # Direct outside path
        outside_db = self.outside_dir / "evil.db"
        with self.assertRaises(StorageSecurityError):
            verify_safe_path(outside_db, self.allowed_root)

        # Relative '..' traversal
        traversal_path = self.allowed_root / ".." / "outside" / "evil.db"
        with self.assertRaises(StorageSecurityError):
            verify_safe_path(traversal_path, self.allowed_root)

    def test_verify_safe_path_rejects_ntfs_junction(self) -> None:
        junction_dir = self.allowed_root / "junction_escape"
        res = subprocess.run(
            ["cmd.exe", "/c", "mklink", "/J", str(junction_dir), str(self.outside_dir)],
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(res.returncode, 0, f"mklink /J failed: {res.stderr or res.stdout}")

        # 1. The junction directory itself must be rejected
        with self.assertRaises(StorageSecurityError):
            verify_safe_path(junction_dir, self.allowed_root)

        # 2. A file inside the junction directory must be rejected (intermediate prefix is a junction)
        nested_in_junction = junction_dir / "gamma.db"
        with self.assertRaises(StorageSecurityError):
            verify_safe_path(nested_in_junction, self.allowed_root)

        # 3. Using a junction as allowed_root itself must be rejected
        with self.assertRaises(StorageSecurityError):
            verify_safe_path(nested_in_junction, junction_dir)

    def test_check_or_enforce_acls_audit_and_enforce(self) -> None:
        acl_dir = self.allowed_root / "acl_vault"
        acl_dir.mkdir(parents=True, exist_ok=True)

        # Grant Everyone read access first so we can verify audit mode detects broad ACLs
        subprocess.run(
            ["icacls.exe", str(acl_dir), "/grant", "Everyone:(OI)(CI)(R)"],
            capture_output=True,
            text=True,
            check=False,
        )
        audit_before = check_or_enforce_acls(acl_dir, enforce=False)
        self.assertFalse(audit_before["enforced"])
        self.assertFalse(audit_before["secure"])
        self.assertTrue(len(audit_before["insecure_entries"]) > 0)

        # Now enforce strict ACL lockdown
        enforced = check_or_enforce_acls(acl_dir, enforce=True)
        self.assertTrue(enforced["enforced"])
        self.assertTrue(enforced["secure"], f"Expected secure ACLs, got: {enforced['insecure_entries']}")
        self.assertEqual(enforced["insecure_entries"], [])
        self.assertIn(enforced["username"].lower(), enforced["raw_acl"].lower())


class TestStateGenerationAndRollbackGuard(unittest.TestCase):
    def setUp(self) -> None:
        self.test_dir = Path(tempfile.mkdtemp(prefix="gamma_p5_gen_")).resolve()
        self.meta_path = self.test_dir / "state_generation.json"
        self.wm_path = self.test_dir / "watermark.db"
        self.gamma_path = self.test_dir / "gamma.db"

    def tearDown(self) -> None:
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def _make_tick(self, ltt_sec: int, recv_sec: int, ltp: float = 120.0) -> Tick:
        return Tick(
            ts_ms=ltt_sec * 1000,
            recv_ms=recv_sec * 1000,
            security_id=45001,
            segment="NSE_FNO",
            ltp=ltp,
            volume_day=1000,
            oi=50000,
            prev_oi=48000,
            prev_close=110.0,
            oi_day_high=52000,
            oi_day_low=47000,
        )

    def test_watermark_backup_rollback_detected(self) -> None:
        mgr = StateGenerationManager(self.meta_path)
        store = WatermarkStore(self.wm_path, broker="mock_broker", account_scope="acc_01")

        # Claim initial tick at t=1700000100
        self.assertTrue(store.claim(self._make_tick(1700000100, 1700000101)))
        entry1 = mgr.verify_and_advance(self.wm_path, db_key="watermark.db")
        self.assertEqual(entry1["generation"], 1)
        self.assertEqual(entry1["max_marker"], 1700000100 * 1000)

        # Take a snapshot backup of watermark.db at generation 1
        backup_wm = self.test_dir / "watermark.db.bak_old"
        shutil.copy2(self.wm_path, backup_wm)

        # Advance watermark to t=1700000200 and seal generation 2
        self.assertTrue(store.claim(self._make_tick(1700000200, 1700000201)))
        entry2 = mgr.verify_and_advance(self.wm_path, db_key="watermark.db")
        self.assertEqual(entry2["generation"], 2)
        self.assertEqual(entry2["max_marker"], 1700000200 * 1000)

        # Simulate backup-rollback attack: overwrite watermark.db with older backup
        shutil.copy2(backup_wm, self.wm_path)
        for suf in ("-wal", "-shm"):
            p = self.wm_path.with_name(self.wm_path.name + suf)
            if p.exists():
                p.unlink()

        with self.assertRaises(StateRollbackError) as ctx:
            mgr.verify_and_advance(self.wm_path, db_key="watermark.db")
        self.assertIn("regressed", str(ctx.exception))

    def test_gamma_db_backup_rollback_and_deletion_detected(self) -> None:
        mgr = StateGenerationManager(self.meta_path)
        with closing(db.connect(self.gamma_path)) as conn:
            db.migrate(conn, backup=False)
            db.insert_run(conn, "run_001", "0920", "2026-11-02T09:20:00+05:30", "0.8.9")

        mgr.verify_and_advance(self.gamma_path, db_key="gamma.db")
        old_backup = self.test_dir / "gamma_old.db"
        shutil.copy2(self.gamma_path, old_backup)

        # Add a signal row and advance generation seal
        with closing(db.connect(self.gamma_path)) as conn:
            conn.execute(
                "INSERT INTO signals (trade_date, ts_ms, security_id, list_name, expiry_date, kind, ltp, spec_version) "
                "VALUES ('2026-11-02', 1700000500000, 45001, 'BREAKOUT-CE', '2026-11-26', 'E1', 120.0, '0.8.9')"
            )
            conn.commit()
        entry2 = mgr.verify_and_advance(self.gamma_path, db_key="gamma.db")
        self.assertGreaterEqual(entry2["max_marker"], 1)

        # 1. Restore older backup -> StateRollbackError
        shutil.copy2(old_backup, self.gamma_path)
        for suf in ("-wal", "-shm"):
            p = self.gamma_path.with_name(self.gamma_path.name + suf)
            if p.exists():
                p.unlink()
        with self.assertRaises(StateRollbackError):
            mgr.verify_and_advance(self.gamma_path, db_key="gamma.db")

        # 2. Delete sealed database file -> StateRollbackError
        self.gamma_path.unlink()
        with self.assertRaises(StateRollbackError) as ctx:
            mgr.verify_and_advance(self.gamma_path, db_key="gamma.db")
        self.assertIn("missing from disk", str(ctx.exception))

    def test_state_generation_seal_tamper_detected(self) -> None:
        mgr = StateGenerationManager(self.meta_path)
        store = WatermarkStore(self.wm_path, broker="mock_broker", account_scope="acc_01")
        self.assertTrue(store.claim(self._make_tick(1700000100, 1700000101)))
        mgr.verify_and_advance(self.wm_path, db_key="watermark.db")

        # Tamper with max_marker in state_generation.json without updating SHA-256 seal
        doc = json.loads(self.meta_path.read_text(encoding="utf-8"))
        doc["databases"]["watermark.db"]["max_marker"] = 0
        self.meta_path.write_text(json.dumps(doc), encoding="utf-8")

        with self.assertRaises(StateRollbackError) as ctx:
            mgr.load_state()
        self.assertIn("seal mismatch", str(ctx.exception))

    def test_state_generation_file_deletion_detected(self) -> None:
        mgr = StateGenerationManager(self.meta_path)
        store = WatermarkStore(self.wm_path, broker="mock_broker", account_scope="acc_01")
        self.assertTrue(store.claim(self._make_tick(1700000100, 1700000101)))
        entry = mgr.verify_and_advance(self.wm_path, db_key="watermark.db")
        self.assertEqual(entry["generation"], 1)

        # Attacker deletes state_generation.json while sealed watermark.db remains on disk
        self.meta_path.unlink()
        with self.assertRaises(StateRollbackError) as ctx:
            mgr.verify_and_advance(self.wm_path, db_key="watermark.db")
        self.assertIn("state_generation.json deletion detected", str(ctx.exception))

        # Also verify explicit require_existing_seal=True on an unsealed database
        unsealed_db = self.test_dir / "unsealed.db"
        with self.assertRaises(StateRollbackError) as ctx2:
            mgr.verify_and_advance(unsealed_db, db_key="unsealed.db", require_existing_seal=True)
        self.assertIn("seal required", str(ctx2.exception))


class TestDatabaseCorruptionAndQuarantineRecovery(unittest.TestCase):
    def setUp(self) -> None:
        self.test_dir = Path(tempfile.mkdtemp(prefix="gamma_p5_rec_")).resolve()

    def tearDown(self) -> None:
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_corrupt_main_db_quarantine_and_rebuild(self) -> None:
        db_path = self.test_dir / "gamma.db"
        db_path.write_bytes(b"CORRUPT_HEADER_NOT_SQLITE_3" * 20)
        wal_path = self.test_dir / "gamma.db-wal"
        shm_path = self.test_dir / "gamma.db-shm"
        wal_path.write_bytes(b"corrupt_wal_bytes")
        shm_path.write_bytes(b"corrupt_shm_bytes")

        self.assertFalse(verify_database_integrity(db_path))

        fixed_now = datetime(2026, 11, 2, 14, 30, 0, tzinfo=IST)
        q_path = quarantine_and_recover(
            db_path,
            schema_init_fn=lambda conn: db.migrate(conn, backup=False),
            now_fn=lambda: fixed_now,
        )

        # Verify quarantined artifacts exist in data/quarantine/
        self.assertTrue(q_path.exists())
        self.assertEqual(q_path.name, "gamma.db_20261102_143000.corrupt")
        self.assertTrue(Path(str(q_path) + "-wal").exists())
        self.assertTrue(Path(str(q_path) + "-shm").exists())

        # Verify rebuilt database is healthy and migrated
        self.assertTrue(verify_database_integrity(db_path))
        with closing(sqlite3.connect(str(db_path))) as conn:
            self.assertEqual(db.user_version(conn), db.SCHEMA_VERSION)
            jmode = conn.execute("PRAGMA journal_mode").fetchone()[0]
            self.assertEqual(str(jmode).lower(), "wal")

    def test_corrupt_btree_page_with_valid_sqlite_header_quarantined(self) -> None:
        db_path = self.test_dir / "gamma.db"
        with closing(sqlite3.connect(str(db_path))) as conn:
            conn.execute("PRAGMA journal_mode=DELETE")
            conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, payload TEXT NOT NULL)")
            for i in range(200):
                conn.execute("INSERT INTO items (payload) VALUES (?)", ("X" * 200 + str(i),))
            conn.commit()
        self.assertTrue(verify_database_integrity(db_path))

        # Preserve valid 100-byte SQLite header on page 1, but overwrite page 2+ B-tree bytes
        raw = bytearray(db_path.read_bytes())
        self.assertGreater(len(raw), 4096)
        raw[4096 : 4096 + 512] = b"\xFF\x00\xAA\x55" * 128
        db_path.write_bytes(bytes(raw))

        self.assertFalse(verify_database_integrity(db_path))
        q_path = quarantine_and_recover(
            db_path,
            schema_init_fn=lambda conn: db.migrate(conn, backup=False),
        )
        self.assertTrue(q_path.exists())
        self.assertTrue(verify_database_integrity(db_path))

    def test_corrupt_wal_frame_detected_and_quarantined(self) -> None:
        wm_path = self.test_dir / "watermark.db"
        store = WatermarkStore(wm_path, broker="mock_broker", account_scope="acc_01")
        t1 = Tick(
            ts_ms=1700000100 * 1000,
            recv_ms=1700000101 * 1000,
            security_id=45001,
            segment="NSE_FNO",
            ltp=100.0,
            volume_day=500,
            oi=1000,
            prev_oi=900,
            prev_close=95.0,
            oi_day_high=1100,
            oi_day_low=850,
        )
        self.assertTrue(store.claim(t1))
        self.assertTrue(verify_database_integrity(wm_path))

        # Write garbage into the -wal file to simulate torn/corrupted WAL frames on disk full / crash
        wal_path = wm_path.with_name("watermark.db-wal")
        wal_path.write_bytes(b"\x00\x01\x02\x03_MALFORMED_WAL_HEADER_AND_FRAMES_" * 15)

        self.assertFalse(verify_database_integrity(wm_path))

        q_path = quarantine_and_recover(wm_path, schema_init_fn=init_watermark_schema)
        self.assertTrue(q_path.exists())
        self.assertTrue(Path(str(q_path) + "-wal").exists())
        self.assertTrue(verify_database_integrity(wm_path))

        # Re-open WatermarkStore on recovered file and verify it operates cleanly
        store_recovered = WatermarkStore(wm_path, broker="mock_broker", account_scope="acc_01")
        self.assertTrue(store_recovered.claim(t1))


class TestServeAsyncPreflightIntegration(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.test_dir = Path(tempfile.mkdtemp(prefix="gamma_p5_serve_")).resolve()
        self.data_dir = self.test_dir / "data"
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = self.data_dir / "gamma.db"
        base_cfg = settings()
        test_paths = replace(
            base_cfg.paths,
            root=self.test_dir,
            data_dir=self.data_dir,
            logs_dir=self.test_dir / "logs",
            db_path=self.db_path,
        )
        test_env = replace(base_cfg.env, broker="paper", telegram_bot_token="", values={})
        self.cfg = replace(base_cfg, paths=test_paths, env=test_env)

    def tearDown(self) -> None:
        shutil.rmtree(self.test_dir, ignore_errors=True)

    async def test_serve_async_quarantines_corrupt_db_and_starts_cleanly(self) -> None:
        # Plant corrupted gamma.db and watermark.db before serve_async runs
        self.db_path.write_bytes(b"CORRUPTED_GAMMA_DB_IMAGE" * 30)
        wm_path = self.data_dir / "watermark.db"
        wm_path.write_bytes(b"CORRUPTED_WATERMARK_DB_IMAGE" * 30)

        fake_server = MagicMock()
        fake_server.serve = AsyncMock(return_value=None)
        fake_server.should_exit = False

        with (
            patch("gammad.api.server.create_app", return_value=MagicMock()),
            patch("gammad.api.server.make_server", return_value=fake_server),
            patch("gammad.app.Runtime.on_token_valid", return_value=None),
        ):
            rc = await serve_async(tray=False, cfg=self.cfg)
        self.assertEqual(rc, 0)

        # Both corrupt files were quarantined and rebuilt cleanly
        quarantine_dir = self.data_dir / "quarantine"
        self.assertTrue(quarantine_dir.is_dir())
        quarantined_files = [p.name for p in quarantine_dir.iterdir()]
        self.assertTrue(any(n.startswith("gamma.db_") and n.endswith(".corrupt") for n in quarantined_files))
        self.assertTrue(any(n.startswith("watermark.db_") and n.endswith(".corrupt") for n in quarantined_files))

        self.assertTrue(verify_database_integrity(self.db_path))
        self.assertTrue(verify_database_integrity(wm_path))
        self.assertTrue((self.data_dir / "state_generation.json").is_file())

    async def test_serve_async_blocks_backup_rollback_attack(self) -> None:
        wm_path = self.data_dir / "watermark.db"
        # First preflight + watermark progression
        gen_mgr, _ = _preflight_storage_guard(self.cfg, wm_path)
        store = WatermarkStore(wm_path, broker="paper", account_scope="paper_01")
        t1 = Tick(
            ts_ms=1700000100 * 1000,
            recv_ms=1700000101 * 1000,
            security_id=45001,
            segment="NSE_FNO",
            ltp=100.0,
            volume_day=500,
            oi=1000,
            prev_oi=900,
            prev_close=95.0,
            oi_day_high=1100,
            oi_day_low=850,
        )
        t2 = replace(t1, ts_ms=1700000200 * 1000, recv_ms=1700000201 * 1000, ltp=105.0)
        self.assertTrue(store.claim(t1))
        gen_mgr.verify_and_advance(wm_path, db_key="watermark.db")
        old_wm_copy = self.test_dir / "wm_old.db"
        shutil.copy2(wm_path, old_wm_copy)

        self.assertTrue(store.claim(t2))
        gen_mgr.verify_and_advance(wm_path, db_key="watermark.db")

        # Replace watermark.db with older copy
        shutil.copy2(old_wm_copy, wm_path)
        for suf in ("-wal", "-shm"):
            p = wm_path.with_name(wm_path.name + suf)
            if p.exists():
                p.unlink()

        with self.assertRaises(StateRollbackError):
            await serve_async(tray=False, cfg=self.cfg)

    async def test_serve_async_blocks_deleted_state_generation_file(self) -> None:
        wm_path = self.data_dir / "watermark.db"
        gen_mgr, _ = _preflight_storage_guard(self.cfg, wm_path)
        store = WatermarkStore(wm_path, broker="paper", account_scope="paper_01")
        t1 = Tick(
            ts_ms=1700000100 * 1000,
            recv_ms=1700000101 * 1000,
            security_id=45001,
            segment="NSE_FNO",
            ltp=100.0,
            volume_day=500,
            oi=1000,
            prev_oi=900,
            prev_close=95.0,
            oi_day_high=1100,
            oi_day_low=850,
        )
        self.assertTrue(store.claim(t1))
        gen_mgr.verify_and_advance(wm_path, db_key="watermark.db")

        # Delete state_generation.json while sealed watermark.db remains in data/
        (self.data_dir / "state_generation.json").unlink()
        with self.assertRaises(StateRollbackError):
            await serve_async(tray=False, cfg=self.cfg)

    async def test_serve_async_respects_gamma_enforce_acls(self) -> None:
        from types import MappingProxyType
        fake_server = MagicMock()
        fake_server.serve = AsyncMock(return_value=None)
        fake_server.should_exit = False

        # 1. Enforce ACLs enabled (GAMMA_ENFORCE_ACLS="1")
        cfg_with_acls = replace(
            self.cfg,
            env=replace(self.cfg.env, values=MappingProxyType({"GAMMA_ENFORCE_ACLS": "1"})),
        )
        with (
            patch("gammad.api.server.create_app", return_value=MagicMock()),
            patch("gammad.api.server.make_server", return_value=fake_server),
            patch("gammad.app.Runtime.on_token_valid", return_value=None),
            patch("gammad.app._preflight_storage_guard", wraps=_preflight_storage_guard) as mock_guard,
        ):
            rc = await serve_async(tray=False, cfg=cfg_with_acls)
        self.assertEqual(rc, 0)
        mock_guard.assert_called_once()
        _, kwargs = mock_guard.call_args
        self.assertTrue(kwargs.get("enforce_acls"))

        # 2. Enforce ACLs disabled (GAMMA_ENFORCE_ACLS="0")
        cfg_without_acls = replace(
            self.cfg,
            env=replace(self.cfg.env, values=MappingProxyType({"GAMMA_ENFORCE_ACLS": "0"})),
        )
        with (
            patch("gammad.api.server.create_app", return_value=MagicMock()),
            patch("gammad.api.server.make_server", return_value=fake_server),
            patch("gammad.app.Runtime.on_token_valid", return_value=None),
            patch("gammad.app._preflight_storage_guard", wraps=_preflight_storage_guard) as mock_guard_disabled,
        ):
            rc = await serve_async(tray=False, cfg=cfg_without_acls)
        self.assertEqual(rc, 0)
        mock_guard_disabled.assert_called_once()
        _, kwargs_disabled = mock_guard_disabled.call_args
        self.assertFalse(kwargs_disabled.get("enforce_acls"))


if __name__ == "__main__":
    unittest.main()
