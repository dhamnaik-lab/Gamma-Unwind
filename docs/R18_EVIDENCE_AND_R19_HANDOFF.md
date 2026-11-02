# GAMMA R1.8 evidence checkpoint and R1.9 development handoff

**As of 2 November 2026 (IST)** · **Paper monitoring only** · **Production Apply: HOLD**

## Identity and scope

| Authority | Exact state | Interpretation |
|---|---|---|
| Installed client | 0.8.9, 237 original pinned source/document files | No R1.8 prototype installed |
| R3 baseline snapshot | `Gamma Unwind Source Code 2026-10-31_123418_650_IST_cc136e62.zip` | SHA-256 `6AFF21F16381ADA2CDEBD0E706CC03FEA642E4BB262E2BE1FAD233A8D538897D`; source snapshot, not runtime/data backup |
| Three-gate R1.8 candidate | 244 source files | Source-gated offline proposal; two modified existing files in latest three-gate delta: `gammad/broker/fyers/feed.py`, `gammad/trade/tracker.py` |
| Experimental watermark candidate | 245 source files, original 244 + new `gammad/broker/fyers/durable_watermark.py` | No existing source files changed at this additive step; NOT wired to default Runtime |
| R1.9 | Proposed integrated broker-neutral multi-account work | **Not implemented, not installed, not release-authorized** |

## Acceptance evidence (all independently archive-hash/ZIP-integrity checked)

| Artifact | SHA-256 | Bounded outcome |
|---|---|---|
| `GAMMA_R18_THREE_GATES_EVIDENCE.zip` (accepted 31 Oct) | `354D967B2927A6F26F6A8006C18E53D26710705E9801E84C1AC61C39ADBA0AE4` | 15/15 offline tests, 244-source manifest, no live calls/orders reported |
| `GAMMA_R18_ACTUAL_RUNTIME_EVIDENCE.zip` (accepted 1 Nov) | `8EB4AF8F715FFA5D05F6469BE5EF8547F623284A3D0890528B707B27910E0CD4` | Actual `gammad.app.Runtime` constructor, fake FYERS session, paused send guard, tracker crash-state check |
| `GAMMA_R18_FINAL_LIFECYCLE_EVIDENCE.zip` (accepted 2 Nov) | `894ECE7737CB837D43F0A62F51A4C55F1C84178976E70C81713E82150F0FEC9E` | Bounded real-source V1 E2→E1, paper T1/T2, paused router, ambiguous event quarantine, disposable SQLite backup/rollback |
| `GAMMA_R18_WATERMARK_OFFLINE_EVIDENCE.zip` (accepted Windows run 2 Nov 17:20 IST) | `9DE6BC780D9E3B3F175C00DB1B508442F87846C9CDFA11EB27C47C54103ADE2F` | Windows Python 3.13, 10/10 tests, 0 errors/failures; persistent watermark prototype ONLY |
| `GAMMA_R18_WATERMARK_DISPOSABLE_SOURCE.zip` (accepted Windows run 2 Nov 17:20 IST) | `39C71BFA8F7ABD2C1D4ABBEE8BB1A88F52903E6EDF82A1F86E105E180111A1FE` | 245-file disposable candidate; source package integrity, not production authorization |

The earlier actual Runtime/final lifecycle result explicitly records that an unguarded feed **accepted** a regressing exchange timestamp after its state was recreated. The subsequent experimental guard's 10/10 tests do **not** resolve the installed/default-factory issue because no account-verified integration exists.

**Interpretation caution:** Archive checksums verify the archived artifact. Reports describe what a particular bounded test ran; they do not independently prove live market data validity, full cross-provider integration, real notifications or deployment safety. Where reports say no live broker calls/orders/installed writes, treat these as claims within the recorded offline test scope.

## Completed bounded test areas

- Exact installed baseline and disposable candidate source integrity before/after relevant Windows tests.
- Actual Runtime construction with synthetic brokers/session token watcher, no real network authorization.
- Genuine-source V1 nonzero E2/E1 and paper position transitions through T1/T2, including paused router behavior in bounded scenarios.
- SQLite ambiguous outbound event durable state and nonautomatic resend on restart (a **quarantine**, not proof of exactly-once delivery).
- Disposable SQLite transaction rollback and backup; **not** Windows production ACL/recovery proof.
- New optional FYERS watermark: persisted NSE_FNO exchange LTT with scope, concurrency, duplicate/stale timestamp checks and synthetic restart. This is not in installed feed factory.

## Unresolved requirements (release HOLD)

| Track | Remaining requirement | Why pending |
|---|---|---|
| Broker-neutral identity | Verified stable broker + account key; multiple independent concurrent authorized accounts | No certified account binding for optional watermark or broker-neutral runtime |
| Real Runtime integration | Explicit provider/feed factory wiring, account/revision isolation, broker switching, independent health and reconnection | 245-source optional module is not connected |
| Strategy/full lifecycle | Full scheduler → universe → history → feed → V1 → router → paper positions → exits → durable notifications, baseline parity | Bounded fixtures are not complete installed end-to-end evidence |
| Notifications | Real Telegram transport receipts, ambiguous dispatch manual reconciliation and idempotency guarantees | No external provider acceptance test |
| Market data | Independently authorized, rights-checked provider(s), expired options, validated 2,000 bars/sessions as required | FYERS real entitlement not demonstrated; user reports FYERS account disabled due inactivity |
| Security/storage | Production Windows private ACL, account-scoped ledger, corruption/backup-rollback hardening, migration, rollback | Disposable SQLite is insufficient |
| Provider coverage | Each adapter's real login/feed/history entitlement, mapping, failover policy | Catalog registration does not imply working provider |
| Release | Approved, reproducible Windows integration qualification, backup, verified Apply and rollback | `R10H_APPLY=HOLD` |

## R1.9 agreed design and execution workflow

1. **Broker-neutral core:** no hard-coded first/default/preferred broker. Explicit authorization and account/revision selection for each service. Separate broker-connected accounts and historical market-data providers.
2. **Multi-broker is mandatory:** allow concurrent independently authenticated brokers and multiple accounts; switching one must not invalidate another. Account state and feeds must be isolated by broker and trusted account identity; decline identity ambiguity.
3. **History independent of execution broker:** eligibility and official-data certification separate; never infer entitlement from login or app configuration. FYERS must be optional, not a blocker for offline work.
4. **Durable watermark:** bind the already tested optional prototype to a trusted per-account identity and actual feed lifecycle. Validate queue saturation, corrupted/replaced ledger, stale/missing exchange time, restart/switch/reconnect and candidate/baseline behavior before enabling.
5. **Implementation workflow:** continue in ChatGPT with evidence/review here and PowerShell 7 Windows runs by the user. No Codex for GAMMA. Favor one consolidated source-gated R1.9 candidate, test harness and evidence archive over repeated tiny production changes.
6. **Safety:** paper-signal monitoring, never actual orders. No live broker requests, Telegram external sends, installed-source modifications or R10H Apply without separate explicit authorization.

## Documentation/version-control note

This documentation refresh is **not R1.9 implementation**. An optional guarded docs-only apply replaces Markdown files but leaves the preserved `docs/GAMMA-UNWIND-Setup-Guide.docx`, code, databases, tokens, configurations and executables untouched. Documentation is inside the original **237 source hashes**: after any docs-only installation, record a separately approved documentation delta and rebaseline the source-pinned qualification inventory before running older 237-source verification tools. **Do not change an old expected hash silently or infer code parity after rebaseline.**
