# Known issues and unresolved release blockers

**As of 2026-10-31.** “HOLD” means evidence is missing or qualification incomplete; it is not automatically a reproduced defect. Record exact evidence if changing status.

| ID | Severity / classification | Description | Required evidence to close |
|---|---|---|---|
| G-R18-01 | Release blocker / HOLD | New 50-file R1.8 candidate has not passed full actual Runtime constructor/lifecycle tests | Source-gated baseline/candidate Windows Runtime, nonzero events and unchanged installed pins |
| G-R18-02 | Release blocker / HOLD | Protected outbox durable delivery, idempotency and process-crash recovery unqualified | Real schema/router/tracker/outbox replay with controlled crash injection |
| G-R18-03 | Release blocker / HOLD | Complete E2→E1→router→position→exit→outbox equivalence not demonstrated | Identical genuine nonzero baseline/candidate traces with persistent trade state |
| G-R18-04 | Release blocker / HOLD | Stale-tick handling across process restart and actual broker feed re-subscriptions unqualified | Fault-injection suite with timestamp, mapping, credentials and restart states |
| G-R18-05 | Release blocker / HOLD | Real FYERS expired contract and 2,000 official NSE sessions entitlement/availability unverified | Separately approved read-only provider call and data-quality evidence |
| G-R18-06 | Release blocker / HOLD | Durable ledger uses new SQLite candidate module, but installed Windows private ACL/account scoping and protection against backup rollback are not qualified | Verified explicit provisioning, ACLs, identity, corruption/rollback and Runtime integration |
| G-AUTH-01 | Qualification gap | Multi-broker availability does not imply ten live-qualified adapters | Provider-by-provider live capability and authenticated feed tests, separately authorized |
| G-DOC-01 | Documentation discrepancy | Old 0.8.9 plain-language guide describes candle-close alert checks; `ContractEngine.on_tick` supports forming-tick E1 | Reconcile exact engine flags and timing without modifying V1 strategy |
| G-OPS-01 | Operational gap | Existing source snapshot is an audit of code, not a full restoration of settings/runtime data | Separate private backup/restore procedure with confidentiality controls |

## Historical failures already repaired in the disposable harness

- Earlier R1.8 loop-lifetime test reused a closed event loop: corrected in qualification harness only.
- Baseline attempted to import candidate-only history budget module: corrected by explicit baseline/candidate scope handling.
- First consolidated Windows SQLite test retained an open fixture connection during `TemporaryDirectory` cleanup, causing `WinError 32`: corrected in test harness. **31/31 Windows tests subsequently passed**.

These are not evidence of a verified production FYERS feed failure. Retain previous failing evidence, label it superseded and avoid deleting or rewriting it.

## Issue update format

For new issues record: identifier, observed behavior, installed/candidate scope, version, reproduce steps, relevant safe log/error class, source and evidence SHA-256, impact on signals/orders, correction summary, exact tests and final PASS/HOLD. Exclude tokens/account data; no production writes without explicit authorization.

**Current decision: R10H Apply remains HOLD.**
