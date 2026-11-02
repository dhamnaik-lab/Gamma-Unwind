# R1.9 Phase 5 Onboarding & Handoff Specification

**Document Date:** 2 November 2026 IST  
**Target Next Session:** Phase 5 Execution  
**Active Project Root:** `e:\TA Projects\Gamma Unwind`  
**Python Runtime:** Bundled Python 3.13.5 (`.\python\python.exe`)  
**Git Baseline:** Commit [`f30c0b1`](https://github.com/dhamnaik-lab/Gamma-Unwind/commit/f30c0b13350ebe2be90940faf192bd630d345fe1) on branch `main` (in sync with GitHub `origin/main`)  
**Strict Safety Invariants:** `LIVE_BROKER_REQUESTS=0`, `ORDERS=0`, `R10H_APPLY=HOLD`  

---

## 1. Executive Status & Context for New Conversation

This repository has completed and committed **Phases 1 through 4** of the **R1.9 Broker-Neutral Multi-Account and Durable Watermark** initiative. All changes are committed locally and pushed to GitHub. The test suite passes **42/42 tests** with 0 errors, 0 failures, and 0 `ResourceWarnings`.

The next session is dedicated to **Phase 5: Windows Storage ACL Hardening, Tamper Protection & SQLite Corruption Quarantine Recovery**, which closes out release gate `G-R18-06` and satisfies `ADR-004`.

---

## 2. Completed Phase Matrix (Phases 1–4)

| Phase | Deliverables / Modules | Test Scope & Status | Git Reference |
|---|---|---|---|
| **Phase 1: Multi-Account Core** | `gammad/broker/account.py` (`AccountIdentity`, `AccountCredentials`, `AccountSessionManager`, `HistoryProviderConfig`), `gammad/broker/durable_watermark.py` (`WatermarkStore`, `DurableTickQueue`), `gammad/app.py` | 20 unit tests in `tests/test_r19_broker_neutral.py` PASS | Committed in [`f30c0b1`](https://github.com/dhamnaik-lab/Gamma-Unwind/commit/f30c0b13350ebe2be90940faf192bd630d345fe1) |
| **Phase 2: Full Lifecycle Testing** | Synthetic lifecycle integration: decoupled history backfill -> feed watermark -> V1 signal -> AlertRouter -> paper trade -> T1/T2 lifecycle -> outbox crash-quarantine | 4 integration tests in `tests/test_r19_runtime_lifecycle.py` PASS | Committed in [`f30c0b1`](https://github.com/dhamnaik-lab/Gamma-Unwind/commit/f30c0b13350ebe2be90940faf192bd630d345fe1) |
| **Phase 3: Evidence & Notebook** | Snapshot `Gamma Unwind Source Code 2026-11-02_220241_590_IST_4049d809.zip` (SHA-256: `376A374ADE1B80812A974E5953D7B6B65029EB1EA8F48C8E780ABCE0F060D3F4`), `notebooks/r19_broker_neutral_audit.ipynb` | 5 interactive audit cells verified | Committed in [`f30c0b1`](https://github.com/dhamnaik-lab/Gamma-Unwind/commit/f30c0b13350ebe2be90940faf192bd630d345fe1) |
| **Phase 4: Multi-Account Config Ingestion & Bootstrap** | `gammad/broker/config_loader.py` (Mode A legacy `.env` fallback, Mode B multi-account JSON, decoupled history provider), `gammad/config.py` (`BASE_ENV_KEYS`), `serve_async()` bootstrap wiring, dynamic active feed/adapter binding, unauthenticated adapter revival, SQLite handle leak resolution | 42 unit and integration tests PASS in 3.65s (0 warnings) | Committed in [`f30c0b1`](https://github.com/dhamnaik-lab/Gamma-Unwind/commit/f30c0b13350ebe2be90940faf192bd630d345fe1) |

---

## 3. Phase 5 Implementation Blueprint (The Task for the Next Chat)

### Problem Statement (G-R18-06 & ADR-004)
1. **Windows NTFS ACLs**: Database files (`data/gamma.db`, `data/watermark.db`) currently inherit parent directory ACLs. On multi-user or shared Windows environments, non-privileged users or background processes can inspect secrets or tamper with databases.
2. **Path Traversal / Junction Vulnerability**: Symbolic links or NTFS junctions pointing outside the trusted `data_dir` could cause reads/writes to arbitrary filesystem locations.
3. **Backup-Rollback Attack / Silent State Regression**: Restoring an older backup `.db` regresses the durable high-water mark backward in time, which would cause historical ticks to be re-processed as fresh breakout signals.
4. **SQLite WAL Corruption Recovery**: A dirty shutdown, crash, or disk-full event can leave SQLite WAL files malformed, causing an infinite crash loop on startup.

---

### Step-by-Step Execution Plan for Phase 5

#### Step 5.1: Create Storage Security Module (`gammad/store/security.py`)
- **Path & Reparse Point Safety**:
  - Implement `verify_safe_path(path: Path, allowed_root: Path) -> Path`: verify that `path` is strictly within `allowed_root` and is not an NTFS junction (`os.path.islink` or Windows reparse point check).
- **Windows NTFS ACL Enforcement**:
  - Implement `check_or_enforce_acls(data_dir: Path, enforce: bool = False) -> dict[str, Any]`:
    - Use Windows `icacls.exe` to inspect or restrict permissions.
    - Recommended permissions: Disable inheritance (`/inheritance:r`), grant Full Control (`(F)`) solely to `$env:USERNAME` and `SYSTEM`.
    - Provide a non-destructive audit mode (`enforce=False`) that flags insecure permissions without altering system ACLs during test runs.
- **Ledger Generation & Rollback Guard**:
  - Implement `StateGenerationManager(metadata_path: Path)`:
    - Maintains a monotonic generational counter and SHA-256 state seal for the active ledger.
    - On startup, verify that the database generation matches or exceeds the persisted seal. If a reverted database is detected (e.g. earlier timestamp or regressed autoincrement ID), raise `StateRollbackError`.

#### Step 5.2: Create Corruption Recovery Engine (`gammad/store/recovery.py`)
- **Pre-flight Integrity Checker**:
  - Implement `verify_database_integrity(db_path: Path) -> bool`:
    - Connect with `timeout=3.0` and execute `PRAGMA quick_check(1)`.
    - Returns `True` if `ok`, `False` if corruption/malformation is detected.
- **Graceful Quarantine & Rebuild**:
  - Implement `quarantine_and_recover(db_path: Path, schema_init_fn: Callable[[sqlite3.Connection], None]) -> Path`:
    - Move damaged `<dbname>.db`, `<dbname>.db-wal`, and `<dbname>.db-shm` to `data/quarantine/<dbname>_<YYYYMMDD_HHMMSS>.corrupt`.
    - Create a clean replacement database, enable `PRAGMA journal_mode=WAL` and `PRAGMA synchronous=FULL`, and execute `schema_init_fn`.
    - Log an audit alert warning that quarantine occurred.

#### Step 5.3: Wire Pre-flight Security & Recovery into `serve_async()` in `gammad/app.py`
- In `serve_async()` (prior to instantiating `Runtime` and opening database connections):
  - Run `verify_safe_path()` on `cfg.paths.data_dir`, `cfg.paths.db_path`, and `watermark_ledger_path`.
  - Check database integrity via `verify_database_integrity()`. If corrupted, quarantine and re-initialize.
  - Check generational state seal to guard against backup rollback.

#### Step 5.4: Test Suite (`tests/test_r19_storage_security.py`)
- Write unit and integration tests:
  - Test safe path verification and junction rejection.
  - Test NTFS ACL inspection helper.
  - Test generational rollback detection (`StateRollbackError`).
  - Test corrupted SQLite database detection, quarantine isolation to `data/quarantine/`, and fresh database rebuild.

#### Step 5.5: Documentation & Closeout
- Update `CHANGELOG.md`, `docs/DECISION_LOG.md` (ADR-013), and `docs/KNOWN_ISSUES.md` (promote `G-R18-06` to **BOUNDED PASS**).

---

## 4. Mandatory Developer Rules to Follow in Next Chat

1. **Workspace Boundary**: Confine all reads, edits, and terminal commands strictly to `e:\TA Projects\Gamma Unwind`.
2. **Pre-edit Backup Rule**: Always create a `<file>.bak` backup before modifying any existing file.
3. **Explicit Approval Rule**: Never apply modifications, rewrites, or patches without prior user approval. Always propose the plan first.
4. **Shell Execution Rules**: Windows PowerShell 5.1 compatibility. Do NOT use Bash-style chaining (`&&` or `||`). Run commands separately. Always quote Windows paths with spaces.
5. **Validation Rule**: Run `.\python\python.exe -W default -m unittest discover -s tests -p "test_*.py"` before reporting any task complete. Verify 0 errors, 0 failures, and 0 warnings.
6. **Safety Invariants**:
   - `LIVE_BROKER_REQUESTS=0` (Zero live broker API calls)
   - `ORDERS=0` (Zero real orders placed)
   - `R10H_APPLY=HOLD` (Production deployment on strict hold)
   - Never run `git push` without explicit instruction.
