# Architecture decision record

This log distinguishes **accepted project constraints** from future design proposals. Add a dated entry for significant irreversible decisions. Do not use this file to grant a new authorization.

## ADR-001 — Preserve installed source and run R5 qualification disposably

- **Date:** 2026-10-31 · **Decision:** accepted.
- **Context:** installed GAMMA 0.8.9 continues to operate separately from R5/R1.8 source handoffs.
- **Rule:** hash accepted input, change only approved candidate files in isolated clones, test offline, capture evidence; keep installed writes and actual broker calls at zero until separately approved.
- **Consequence:** “offline PASS” does not equal installed; R10H Apply remains HOLD.

## ADR-002 — Broker-neutral identity and explicit staging

- **Date:** 2026-10-31 · **Decision:** design constraint.
- **Rule:** no hard-coded preferred/default broker in new R5 history paths; the installed catalog/staging APIs and provider capabilities are inspected before authorizing a session. Staging a broker is not an instantaneous active-feed change.
- **Consequence:** broker switching and concurrent secondary logins require isolation and live qualification.

## ADR-003 — Reuse authenticated backend session without exposing credentials

- **Date:** 2026-10-31 · **Decision:** target behavior; not fully production-qualified.
- **Rule:** broker session reuse is backend-trusted, read-only, freshness-checked, revision-scoped and never exported via unprotected APIs, frontend JS or evidence. Missing authorization fails closed.
- **Consequence:** browser login alone does not grant expired-contract history entitlement.

## ADR-004 — Durable request budgets must survive restart

- **Date:** 2026-10-31 · **Decision:** disposable implementation accepted for bounded testing only.
- **Context:** in-memory `WeakKeyDictionary` counting can reset on new Runtime/process.
- **Candidate correction:** SQLite `BEGIN IMMEDIATE` reservation before a request, six India-local requests per broker/application/day; refuse missing/corrupt ledger and explicit path required.
- **Still HOLD:** Windows ACLs, backup rollback/tampering, per-owner scope, new Runtime parity and deployment.

## ADR-005 — Preserve V1 signal semantics while proving parity

- **Date:** 2026-10-31 · **Decision:** accepted constraint.
- **Context:** actual `on_bar_close` E2 and `on_tick` forming-bar E1 are distinct paths.
- **Rule:** do not silently force all E1 events onto candle close or change rules/targets/stops during history integration; test completed data and forming tick timing faithfully.
- **Consequence:** a completed-bar-only E1 redesign needs separate approval, backtests and evidence.

## ADR-006 — Paper-only alerts; never orders

- **Date:** existing 0.8.9 product contract, reaffirmed 2026-10-31.
- **Decision:** real order placement is outside approved GAMMA scope. Scanner alerts, paper-state tracking and Telegram follow-ups remain distinguishable from actual broker transactions.
- **Consequence:** zero actual orders in test/evidence and no order API paths added.

## ADR-007 — Source snapshots stay separate and verifiable

- **Date:** 2026-10-31 · **Decision:** accepted.
- **Locations:** `scripts/Create-GammaUnwind-SourceSnapshot.ps1` in project; timestamped ZIPs in `E:\TA Projects\Gamma Unwind Source Code Audit`.
- **Integrity:** internal file and manifest hashes, ZIP hash printed by R3 (no new `.zip.sha256` sidecars).
- **Consequence:** source snapshots are not full credential/data backups; archive-only recovery requires review.

## ADR-008 — Documentation changes do not resume R1.8

- **Date:** 2026-10-31 · **Decision:** approved documentation-only work.
- **Rule:** add 16 Markdown documents; preserve existing installed code, DOCX guide, configuration, database and broker session; refuse overwriting a current Markdown file without separate review.
- **Consequence:** documentation delivery does not grant authority to enable historical calls, send messages, make trades or perform R10H Apply.

## Earlier proposed review (2026-10-31 historical checkpoint)

- Source-pinned, durable protected outbox and crash recovery, full continuous V1 trade-flow parity, stale-tick/process-restart recovery, and actual Windows Runtime qualification of revised R1.8 50-file source. After these gates, request **separate** authorization for genuine read-only provider history.

See [Project Status](PROJECT_STATUS.md), [Testing](TESTING_RELEASE_GATES.md) and [Known Issues](KNOWN_ISSUES.md).

## ADR-009 — True concurrent multi-broker operation (2026-11-02)

- **Decision:** accepted requirement, **implementation R1.9 not yet performed**.
- Multiple brokers and accounts must connect concurrently with independent sessions, explicit broker/account revision authorization, separate failures, feed/watermark namespaces and no privileged default broker. History providers must be independently selectable and qualified.
- FYERS is retained as an optional adapter. The user reports FYERS currently disabled due to inactivity; offline synthetic qualification may continue without login.

## ADR-010 — Promote accepted R1.8 evidence without claiming deployment (2026-11-02)

- **Decision:** document accepted 244-source actual Runtime bounded evidence and 245-file optional watermark Windows 10/10 PASS, while keeping default feed wiring, trusted account identity, full scheduler→notifications, genuine market history, production ACL and R10H Apply on HOLD.
- Quarantining ambiguous outbox sends prevents automatic retries in the tested case but is **not** a delivery guarantee.

## ADR-011 — Implementation workflow and documentation change control (2026-11-02)

- **Decision:** continue GAMMA implementation within ChatGPT plus user-executed Windows PowerShell 7 verification; **do not use Codex for this project**. Prefer consolidated R1.9 offline implementation and acceptance instead of repeated small releases.
- A documentation update is not a Runtime update. Preserve the original Word guide, app code, secrets and databases; document and separately rebaseline intentional Markdown-only hash changes before another source-gated stage.
- **No automatic authorization** for production writes, broker calls, real orders or Telegram external sends. `R10H_APPLY=HOLD`.

## ADR-012 — Implement R1.9 broker-neutral multi-account, configuration loader, and durable watermark core (2026-11-02)

- **Decision:** accepted candidate implementation in `gammad/broker/account.py`, `gammad/broker/config_loader.py`, `gammad/broker/durable_watermark.py`, `gammad/config.py`, and runtime lifecycle integration into `gammad/app.py`.
- **Scope:**
  - `AccountIdentity` & `AccountCredentials`: explicit, trusted broker account identity rejecting token leakage, whitespace, or invalid characters, supporting paper/mock/registered brokers, accompanied by structured credential encapsulation.
  - `AccountSessionManager`: true concurrent multi-broker account registration, active session routing, and independent failure isolation.
  - `HistoryProviderConfig`: decouples historical market data provider routing and request budgets (`GAMMA_HISTORY_PROVIDER`) from the active paper-monitoring broker.
  - `Multi-Account Configuration Loader` (`gammad/broker/config_loader.py`):
    - `Mode A` (Backward-Compatible): automatically synthesizes primary `AccountIdentity` and `AccountCredentials` from legacy single-broker configuration (`cfg.env.broker` and registry credentials) when `GAMMA_ACCOUNTS_JSON` is not set, with paper broker support.
    - `Mode B` (Multi-Account Configuration): parses structured JSON account list from `GAMMA_ACCOUNTS_JSON` (supporting raw JSON strings or JSON file paths), validates identities, instantiates adapters with case-normalized credential mapping, and registers them with `AccountSessionManager`. Decoupled history provider resolves independent broker adapters from registry.
  - `Environment Ingestion` (`gammad/config.py`): added `GAMMA_ACCOUNTS_JSON`, `GAMMA_HISTORY_PROVIDER`, `GAMMA_HISTORY_BARS_LIMIT`, `GAMMA_HISTORY_DAILY_BUDGET` to `BASE_ENV_KEYS`, enabling full `.env` file ingestion into `cfg.env.values`.
  - `WatermarkStore` & `DurableTickQueue`: broker-neutral SQLite WAL durable high-water mark (`synchronous=FULL`), enforcing strictly increasing LTT, rejecting duplicate payloads, and preventing stale tick regressions on restart.
  - Runtime Integration & Bootstrap:
    - `serve_async()` bootstraps `AccountSessionManager` and `HistoryProviderConfig` via `config_loader`, defaulting `watermark_ledger_path` to `cfg.paths.data_dir / "watermark.db"`.
    - `Runtime.broker_entry` dynamically resolves active account's broker entry and capabilities from `session_manager` rather than statically binding `cfg.env.broker`.
    - `Runtime._registry_feed` dynamically resolves active account's broker entry, credentials, and durable watermark.
    - `Runtime.adapter()` returns active session adapter across operational and expired states, enabling authentication renewal without masking.
    - `Runtime.apply_credentials()` and `Runtime.reload_credentials()` target `self.adapter()` and the active token key.
    - `_make_token_watcher()` gracefully uses active adapter without static broker coupling.
  - Test Harness & Fixture Leak Resolution:
    - Added `TestConfigLoader` in `tests/test_r19_broker_neutral.py` covering Mode A, Mode B, file-based JSON configs, real `.env` ingestion, paper broker, invalid config rejection, and history provider decoupling.
    - Updated `TestRuntimeIntegration` to test dynamic feed resolution, active account switching updating runtime properties, unauthenticated adapter revival, and `serve_async` startup wiring & bootstrap.
    - Resolved unclosed SQLite database connection leaks across both test suites (including fixture `tearDown` loops and `closing(sqlite3.connect(...))` in lifecycle tests).
    - 42/42 Windows Python 3.13 tests passed across `tests/test_r19_broker_neutral.py` and `tests/test_r19_runtime_lifecycle.py` with 0 failures, 0 errors, and 0 ResourceWarnings.
- **Consequence:** core R1.9 multi-account architecture and runtime bootstrap are fully verified in candidate scope. Live broker authorization and order placement remain on **HOLD**. `R10H_APPLY=HOLD`.

## ADR-013 — Windows storage ACL hardening, tamper-sealed rollback guard, and SQLite corruption quarantine recovery (2026-11-03)

- **Decision:** accepted candidate implementation in `gammad/store/security.py`, `gammad/store/recovery.py`, and pre-flight startup integration in `gammad/app.py` (`_preflight_storage_guard` / `serve_async()`), closing out gate `G-R18-06` in bounded candidate scope while preserving `gammad/store/db.py` untouched (`D343`).
- **Scope:**
  - `verify_safe_path(path, allowed_root)` (`gammad/store/security.py`): rejects relative traversal (`..`), symlinks (`is_symlink`), Windows NTFS junctions (`is_junction`), and reparse points (`stat.FILE_ATTRIBUTE_REPARSE_POINT`) across every prefix component prior to path resolution, and enforces `.resolve().is_relative_to(allowed_root.resolve())`.
  - `check_or_enforce_acls(data_dir, enforce)` (`gammad/store/security.py`): inspects Windows NTFS ACLs via `icacls.exe` in non-destructive audit mode (`enforce=False` default) to flag broad principals (`Everyone`, `BUILTIN\Users`, `Authenticated Users`), and supports strict lockdown (`enforce=True` or `GAMMA_ENFORCE_ACLS=1`) disabling inheritance (`/inheritance:r`) and restricting Full Control to the current user and `SYSTEM`.
  - `StateGenerationManager` (`gammad/store/security.py`): tracks monotonic per-database generations and high-water marks (`max_marker`, `row_count`, `exists`) in `data/state_generation.json` protected by a canonical SHA-256 tamper seal and stamps `PRAGMA application_id` on sealed SQLite databases. Raises `StateRollbackError` on seal tampering, silent deletion of `state_generation.json` or a sealed database, or generation/high-water mark regression caused by restoring an older `.db` backup.
  - `verify_database_integrity` & `quarantine_and_recover` (`gammad/store/recovery.py`): validates SQLite magic headers, WAL frame headers, and `PRAGMA quick_check(1)` with `contextlib.closing` (0 handle leaks). Quarantines damaged `.db`, `-wal`, and `-shm` files into `data/quarantine/<name>_<YYYYMMDD_HHMMSS>.corrupt*`, logs an `AUDIT ALERT [QUARANTINE]` error, and initializes a clean replacement store (`journal_mode=WAL`, `synchronous=FULL`).
  - `serve_async()` Pre-flight Integration (`gammad/app.py`): executes `_preflight_storage_guard` before instantiating `Runtime` or opening database connections, and updates the generational seal after migration and at shutdown.
  - Verification: 14 new tests in `tests/test_r19_storage_security.py` (56/56 total R1.9 tests PASS in Windows Python 3.13 with 0 warnings).
- **Consequence:** `G-R18-06` promoted to **BOUNDED PASS** in candidate scope. Production deployment (`R10H_APPLY=HOLD`), live broker requests (`LIVE_BROKER_REQUESTS=0`), and real orders (`ORDERS=0`) remain strictly on **HOLD**.

## ADR-014 — R1.9 candidate packaging, sandbox verification, and rollback qualification (2026-11-03)

- **Decision:** accepted release candidate source packaging and rollback rehearsal qualification for R1.9 under `scripts/Create-GammaUnwind-SourceSnapshot.ps1`.
- **Scope & Evidence:**
  - Candidate Archive: `E:\TA Projects\Gamma Unwind Source Code Audit\Gamma Unwind Source Code 2026-11-03_143234_106_IST_bad86e8f.zip`
  - Candidate Whole-ZIP SHA-256: `886A2F65C494AD975345550BEA34BD6DD34E1CC3602A46CA9AB565E3EAD4038B`
  - Archive Content: 249 source and documentation files (4,913,225 bytes); internal `SHA256SUMS.txt` (250 entries including `MANIFEST.json`) verified 100% with zero external sidecars.
  - Isolated Sandbox Verification: Extracted ZIP to disposable directory in `%TEMP%` outside project tree; verified all 250 internal checksums match (0 missing, 0 hash mismatches).
  - Isolated Test Suite Execution: Ran full 56/56 unit and integration test suite (`tests/test_r19_broker_neutral.py`, `tests/test_r19_runtime_lifecycle.py`, `tests/test_r19_storage_security.py`) directly against the extracted candidate package, executing in 3.89s with 0 errors, 0 failures, and 0 warnings, with `sys.path` isolated and `gammad` loaded strictly from the extracted sandbox directory.
  - Rollback Procedure Rehearsal: Verified release rollback safety via `scripts/update_gamma.py` (`KEEP_PREFIXES` covering `data/`, `logs/`, `.venv/`, `python/` and `KEEP_FILES` covering `.env`) and simulated emergency source reversion to baseline commit (`4a1d1d4`), confirming runtime databases (`data/gamma.db`, `data/watermark.db`, `data/state_generation.json`), environment secrets (`.env`), and runtime logs (`logs/`) remain 100% untouched and byte-identical (`STATUS: ROLLBACK_PROCEDURE_QUALIFIED_PASS`).
- **Consequence:** `G-RELEASE-01` candidate packaging and rollback qualification promoted to **BOUNDED PASS**. Production deployment (`R10H_APPLY=HOLD`), live broker requests (`LIVE_BROKER_REQUESTS=0`), and real orders (`ORDERS=0`) remain on strict **HOLD** pending separate user authorization.

