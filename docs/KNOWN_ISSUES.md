# Known issues, gaps and release blockers

**Current 2 November 2026 IST.** A HOLD denotes an incomplete qualification; it is not automatically a reproduced bug. Historical issues that are now bounded-PASS are retained below rather than mislabelled as production-fixed.

| ID | Category / severity | Current classification | Required closeout |
|---|---|---|---|
| G-R18-01 | Actual Runtime constructor/lifecycle | **BOUNDED PASS** on 244-source disposable candidate; not installed | Full integrated release qualification remains separate |
| G-R18-02 | Persistent outbox crash safety | **BOUNDED PASS** for ambiguous-claim quarantine, not guaranteed delivery | External receipt/manual reconciliation/idempotency |
| G-R18-03 | V1-to-paper-position lifecycle | **BOUNDED PASS** E2/E1 and T1/T2, but **full scheduler-to-notification HOLD** | Integrated nonzero full baseline/candidate parity |
| G-R18-04 | Feed restart stale-tick regression | **REPRODUCED** in unguarded feed; optional 245th module **10/10 bounded PASS**, default factory **HOLD** | Trusted account binding and integrated Runtime restart, queue/backpressure, stale/missing LTT qualification |
| G-R18-05 | Expired-contract/history entitlement and 2,000 bars | **HOLD** | Approved rights-backed read-only provider data-quality certification |
| G-R18-06 | Production ledger ACL/rollback | **HOLD**; disposable backup/transaction rollback bounded PASS | Windows ACL, tamper/backup-rollback, secure migration and recovery |
| G-R19-01 | Concurrent independent multi-broker accounts | **HOLD — NOT IMPLEMENTED** | Broker-neutral account identity, session routing, failure containment, per-account feeds |
| G-R19-02 | Historical-data provider independence | **HOLD — NOT QUALIFIED** | Per-provider capabilities, rights, instrument identity and feed/history source selection |
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
