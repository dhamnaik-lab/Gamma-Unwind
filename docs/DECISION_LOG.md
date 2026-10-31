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

## Proposed next review (not approved)

- Source-pinned, durable protected outbox and crash recovery, full continuous V1 trade-flow parity, stale-tick/process-restart recovery, and actual Windows Runtime qualification of revised R1.8 50-file source. After these gates, request **separate** authorization for genuine read-only provider history.

See [Project Status](PROJECT_STATUS.md), [Testing](TESTING_RELEASE_GATES.md) and [Known Issues](KNOWN_ISSUES.md).
