# Known issues, gaps and release blockers

**Current 2 November 2026 IST.** A HOLD denotes an incomplete qualification; it is not automatically a reproduced bug. Historical issues that are now bounded-PASS are retained below rather than mislabelled as production-fixed.

| ID | Category / severity | Current classification | Required closeout |
|---|---|---|---|
| G-R18-01 | Actual Runtime constructor/lifecycle | **BOUNDED PASS** on 244-source disposable candidate; not installed | Full integrated release qualification remains separate |
| G-R18-02 | Persistent outbox crash safety | **BOUNDED PASS** for ambiguous-claim quarantine, not guaranteed delivery | External receipt/manual reconciliation/idempotency |
| G-R18-03 | V1-to-paper-position lifecycle | **BOUNDED PASS** for full synthetic V1 lifecycle (signal -> trade_hook -> T1 -> T2 -> exit) in `tests/test_r19_runtime_lifecycle.py` | Full live market data & external Telegram receipt remain **HOLD** |
| G-R18-04 | Feed restart stale-tick regression | **BOUNDED PASS**; broker-neutral candidate wired into `gammad/app.py` feed hook via `DurableTickQueue`, **24/24 bounded PASS** | Live market provider streaming qualification and backpressure testing |
| G-R18-05 | Expired-contract/history entitlement and 2,000 bars | **HOLD** | Approved rights-backed read-only provider data-quality certification |
| G-R18-06 | Production ledger ACL/rollback | **HOLD**; disposable backup/transaction rollback bounded PASS | Windows ACL, tamper/backup-rollback, secure migration and recovery |
| G-R19-01 | Concurrent independent multi-broker accounts | **BOUNDED PASS** in R1.9 candidate (`gammad/broker/account.py`, wired into `Runtime.adapter()`) | Production live multi-broker credential provisioning |
| G-R19-02 | Historical-data provider independence | **BOUNDED PASS** in R1.9 candidate (`HistoryProviderConfig`, wired into `Runtime.history_adapter()`) | Provider data contract validation and rate-budget enforcement |
| G-R18-07 | Real Telegram follow-ups and reconciliation | **HOLD** | Controlled external transport evidence and safe ambiguous retry/reconcile policy |
| G-AUTH-01 | Broker catalog vs verified adapters | **HOLD** (registered ≠ live-qualified) | Each adapter's authenticated feed/history capability test |
| G-OPS-01 | FYERS account availability | **USER-REPORTED UNAVAILABLE** due to inactivity | Continue offline without FYERS; account restoration separate from development |
| G-RELEASE-01 | Production deployment | **HOLD** | Complete integrated evidence, backup, authorized Apply/Verify/Rollback |

## Closed investigation / bounded acceptance (not installed deployment)

- Windows `subprocess.Popen` test-guard compatibility fixed in **test-only harness**, allowing 10/10 watermark tests to execute. No broker source modification was required for that harness issue.
- Duplicate `T1` inserted in a rollback fixture violated `UNIQUE(trade_id, kind)`; test-only fixture corrected, bounded SQLite rollback accepted. This was not proof of an application database defect.
- The unguarded FYERS feed process-reconstruction timestamp regression is real **within the tested fixture**. The optional durable prototype blocks it in synthetic scope, **not** in the installed Runtime.

## Decision boundary

Keep `R10H_APPLY=HOLD`, `LIVE_BROKER_CALLS=NONE`, `ORDERS=0`. Do not authorize production writes, real broker interactions or external Telegram sends from this document. See [current evidence](R18_EVIDENCE_AND_R19_HANDOFF.md).
