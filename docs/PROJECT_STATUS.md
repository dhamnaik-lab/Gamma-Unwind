# Project status and release authority

**Updated 2 November 2026 IST · Installed client 0.8.9 · Development R1.8 bounded PASS, R1.9 planned · R10H Apply HOLD.**

## Three separate authorities

| Authority | Evidence | Boundary |
|---|---|---|
| Installed application | 237/237 original source/document hashes captured by the 31 Oct R3 source snapshot | R1.8 modules/patches **not** installed |
| Accepted disposable source | 244-file integrated candidate, then a 245-file optional watermark extension | Testing/prototype state, not release or installed state |
| Windows evidence ZIPs | Actual Runtime, lifecycle and watermark reports with the hashes in [checkpoint](R18_EVIDENCE_AND_R19_HANDOFF.md) | Validates only bounded tests explicitly reported |

## Current gate matrix

| Gate | Accepted evidence | Status |
|---|---|---|
| Source fingerprint / archive integrity | Installed source 237/237, accepted candidate 244/244, 245-source optional watermark package validated | PASS — source-gated |
| Actual Runtime constructor / fake FYERS session / token guard | 244-source actual Runtime report | PASS — bounded |
| E2→E1, paper T1→T2, paused router, disposable SQLite recovery | 244-source final lifecycle report | PASS — bounded |
| Ambiguous outbox dispatch | Crash claim survives restart; automatic duplicate attempt not made | PASS — bounded; **not** guaranteed delivery |
| Durable FYERS timestamp watermark | New optional module, Windows Python 3.13, 10/10 tests | PASS — bounded **prototype only** |
| Default Runtime feed integration and trusted account identity | No connected implementation for 245th module | **HOLD** |
| Concurrent, isolated authenticated multi-broker sessions and feeds | Requirement confirmed, R1.9 not implemented | **HOLD** |
| Scheduler → full strategy → router → durable external notifications | Only bounded partial probes | **HOLD** |
| Genuine history entitlement, expired contracts and validated 2,000 bars | No accepted live read-only provider evidence | **HOLD** |
| Production Windows ACL, backup rollback and migration | Disposable SQLite checks only | **HOLD** |
| Real Telegram receipt/idempotency | Not qualified | **HOLD** |
| R10H production Apply / Verify / Rollback | No release authorization | **HOLD** |

## Broker and operating policy

**No fixed primary/default broker.** Multi-broker and multiple concurrent accounts are mandatory. Historical market-data provider selection must remain independent of the currently selected trading broker. Broker catalog count is not live provider readiness. The user reports their FYERS account is disabled due to inactivity; offline development can continue without live FYERS access. Paper monitoring only; zero real orders.

## Next stage (not already implemented)

**R1.9:** trusted account identity and permissions; concurrently isolated broker adapters and feed sessions; account-scoped durable watermarks; independent historical-data provider routing; robust restarts/fault injection; one consolidated offline source-gated Windows acceptance. Continue in ChatGPT/PowerShell, not Codex. Real broker tests or R10H Apply require separate approval.

## Earlier acceptance history

The 31 October baseline recorded the R1.8 four-gate candidate (50 sources, 31/31 offline PASS), which preceded later 244/245-file candidates and actual Runtime tests. That historical evidence remains valid **for its own scope**, but is not the latest qualification. The R3 snapshot supersedes the previously documented R2 221-file count as the pre-application installed source authority. Exact archive names and checksums are listed in [R1.8 checkpoint](R18_EVIDENCE_AND_R19_HANDOFF.md).

Use **PASS — bounded**, **HOLD**, and **NOT APPLIED** precisely. An evidence ZIP/fixture PASS does not authorize production, provider calls, live notifications, history entitlement or real orders.
