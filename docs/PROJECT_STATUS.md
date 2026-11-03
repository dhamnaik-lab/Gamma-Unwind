# Project status and release authority

**Updated 3 November 2026 IST · Installed client 0.8.9 · Development R1.8 bounded PASS, R1.9 Phases 1–5 bounded PASS, Phase 7 Candidate Packaging & Rollback Rehearsal BOUNDED PASS · R10H Apply HOLD.**

## Three separate authorities

| Authority | Evidence | Boundary |
|---|---|---|
| Installed application | 237/237 original source/document hashes captured by the 31 Oct R3 source snapshot | R1.8 modules/patches **not** installed |
| Accepted disposable source | 244-file integrated candidate, then a 245-file optional watermark extension | Testing/prototype state, not release or installed state |
| Windows evidence ZIPs | Actual Runtime, lifecycle and watermark reports with the hashes in [checkpoint](R18_EVIDENCE_AND_R19_HANDOFF.md) | Validates only bounded tests explicitly reported |

## Current gate matrix

| Gate | Accepted evidence | Status |
|---|---|---|
| Source fingerprint / archive integrity | Installed source 237/237, accepted candidate 244/244, 245-source optional watermark package, and 249-source R1.9 candidate (`886A2F65C494AD975345550BEA34BD6DD34E1CC3602A46CA9AB565E3EAD4038B`) validated | PASS — source-gated |
| Actual Runtime constructor / fake FYERS session / token guard | 244-source actual Runtime report | PASS — bounded |
| E2→E1, paper T1→T2, paused router, disposable SQLite recovery | 244-source final lifecycle report | PASS — bounded |
| Ambiguous outbox dispatch | Crash claim survives restart; automatic duplicate attempt not made | PASS — bounded; **not** guaranteed delivery |
| Durable FYERS timestamp watermark | New optional module, Windows Python 3.13, 10/10 tests | PASS — bounded **prototype only** |
| Trusted account identity & broker-neutral durable watermark (Phase 1) | R1.9 candidate (`gammad/broker/account.py`, `durable_watermark.py`, wired into `gammad/app.py`), Windows Python 3.13, committed `f30c0b1` | PASS — bounded actual Runtime integration; live provider authorization **HOLD** |
| Concurrent, isolated multi-broker sessions & history routing (Phase 1) | R1.9 `AccountSessionManager` + `HistoryProviderConfig` candidate (wired into `Runtime.adapter()` and `Runtime.history_adapter()`), committed `f30c0b1` | PASS — bounded candidate |
| Synthetic full lifecycle & validation notebook (Phase 2 & 3) | 42/42 tests PASS (`tests/test_r19_broker_neutral.py`, `tests/test_r19_runtime_lifecycle.py`), interactive notebook `notebooks/r19_broker_neutral_audit.ipynb`, committed `f30c0b1` | PASS — bounded synthetic lifecycle; external live transport **HOLD** |
| Multi-account config ingestion & runtime startup bootstrap (Phase 4) | R1.9 `config_loader.py` (Mode A legacy fallback, Mode B multi-account JSON, decoupled history provider), `gammad/config.py` .env ingestion, `serve_async` startup wiring, dynamic `Runtime.broker_entry` and `_registry_feed` resolution, committed `f30c0b1` | PASS — bounded candidate |
| Genuine history entitlement, expired contracts and validated 2,000 bars | No accepted live read-only provider evidence | **HOLD** |
| Production Windows ACL, backup rollback and migration (Phase 5) | R1.9 `gammad/store/security.py` (path/junction guard, `icacls` ACL audit/enforcement, SHA-256 `StateGenerationManager` + `PRAGMA application_id` stamp), `gammad/store/recovery.py` (`PRAGMA quick_check(1)`, quarantine & clean WAL rebuild), `serve_async` preflight wiring, 14/14 tests in `tests/test_r19_storage_security.py` (56/56 total R1.9 tests PASS, 0 warnings) | PASS — bounded candidate |
| Real Telegram receipt/idempotency (Phase 6) | Not qualified | **HOLD** (Deferred by user decision) |
| R10H production Apply / Verify / Rollback (Phase 7) | Candidate packaged (249 files, ZIP SHA-256 `886A2F65C494AD975345550BEA34BD6DD34E1CC3602A46CA9AB565E3EAD4038B`), isolated sandbox checksums 250/250 PASS, isolated test suite 56/56 PASS, rollback rehearsal verified with zero database/credential/log touch; production deployment on HOLD | PASS — candidate packaging & rollback qualified; **R10H_APPLY=HOLD** |

## Broker and operating policy

**No fixed primary/default broker.** Multi-broker and multiple concurrent accounts are mandatory. Historical market-data provider selection must remain independent of the currently selected trading broker. Broker catalog count is not live provider readiness. The user reports their FYERS account is disabled due to inactivity; offline development can continue without live FYERS access. Paper monitoring only; zero real orders.

## Completed Stages & Packaging Status

**Completed — Phase 5: Windows Storage ACL Hardening, Tamper Protection & SQLite Corruption Quarantine Recovery (`G-R18-06` BOUNDED PASS).**
- **Deliverables:** `gammad/store/security.py`, `gammad/store/recovery.py`, `_preflight_storage_guard` in `gammad/app.py`, and `tests/test_r19_storage_security.py` (56/56 total R1.9 tests PASS, 0 `ResourceWarning`s).

**Completed — Phase 7: R10H Release Packaging, Sandbox Verification & Rollback Qualification (`G-RELEASE-01` CANDIDATE PASS; APPLY HOLD).**
- **Candidate Archive:** `E:\TA Projects\Gamma Unwind Source Code Audit\Gamma Unwind Source Code 2026-11-03_143234_106_IST_bad86e8f.zip`
- **Candidate Whole-ZIP SHA-256:** `886A2F65C494AD975345550BEA34BD6DD34E1CC3602A46CA9AB565E3EAD4038B`
- **Candidate Composition:** 249 selected source files (4,913,225 bytes); internal SHA256SUMS.txt (250 entries including manifest) and MANIFEST.json verified 100%.
- **Isolated Sandbox Verification:** Extracted to temporary folder outside project tree; all 250 checksums matched (0 mismatches); 56/56 unit and integration tests passed in 3.99s with 0 errors, 0 failures, and 0 warnings.
- **Rollback Qualification:** Rehearsed source rollback to prior baseline; verified databases (`data/gamma.db`, `data/watermark.db`, `data/state_generation.json`), `.env` secrets, and logs are 100% untouched.
- **Next Stage (Deferred Phase 6):** Real Telegram receipt/idempotency and ambiguous retry/reconcile policy (currently **HOLD**).
- **Safety Invariants:** `LIVE_BROKER_REQUESTS=0`, `ORDERS=0`, `R10H_APPLY=HOLD`. Zero live broker requests, zero orders. Production Apply strictly on HOLD.

## Earlier acceptance history

The 31 October baseline recorded the R1.8 four-gate candidate (50 sources, 31/31 offline PASS), which preceded later 244/245-file candidates and actual Runtime tests. That historical evidence remains valid **for its own scope**, but is not the latest qualification. The R3 snapshot supersedes the previously documented R2 221-file count as the pre-application installed source authority. Exact archive names and checksums are listed in [R1.8 checkpoint](R18_EVIDENCE_AND_R19_HANDOFF.md).

Use **PASS — bounded**, **HOLD**, and **NOT APPLIED** precisely. An evidence ZIP/fixture PASS does not authorize production, provider calls, live notifications, history entitlement or real orders.
