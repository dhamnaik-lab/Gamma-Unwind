# Changelog

This records **evidenced** milestones; it is not a claim that every experimental patch was applied to the installed application. Historical events are summarized from the 0.8.9 source snapshot and supplied R5 acceptance reports. See [Project status](docs/PROJECT_STATUS.md) for current PASS/HOLD gates.

## Unreleased — documentation baseline (2026-10-31)

- Created 16 Markdown documents covering architecture, broker management, authentication, historical data, V1 signals, feed recovery, operations, paper monitoring, API boundaries, testing, snapshots, issues and decisions.
- Preserved the original `docs/GAMMA-UNWIND-Setup-Guide.docx`; no app logic, order routing, secrets, live broker calls or R10H deployment.
- **Not applied:** the R1.8 disposable durable-request-budget modification; full production qualification remains HOLD.

## R5 / R1.8 qualification track — 2026-10-30 to 2026-10-31

- **Accepted R1.7.1 / R1.8 bounded evidence:** actual Runtime construction under synthetic sessions; source integrity guards; bounded scheduler/history tests; nonzero E2/E1 and seven position-state scenarios; router exit-hold plus fake feed reconnect. All are **bounded offline** results, not production end-to-end parity.
- **R1.8 budget-scope acceptance:** constructor equivalence and bounded closeout PASS after distinguishing candidate-only history-budget logic from an untouched baseline.
- **Consolidated four-gate disposable candidate:** 31/31 offline tests PASS; source-gated candidate manifest 50/50 SHA-256 PASS. Added `gammad/r5_request_budget.py` and changed candidate `gammad/r5_session_history.py`. Original installation unaffected.
- **Outstanding:** revised 50-file candidate not yet qualified through full actual Runtime; durable ledger production storage/Windows ACL and rollback protections, protected outbox crash replay, complete V1 router parity, stale-feed process recovery, authorized FYERS expired-contract history and official 2,000-session coverage remain HOLD.
- **Production release decision:** R10H Apply/Verify/Rollback NOT authorized; zero reported live broker requests/orders in this qualification track.

## Source audit workflow — 2026-10-31

- First verified source-focused snapshot: 215/215 SHA-256 source checks; external ZIP sidecar.
- R2 verified snapshot: 221/221 source SHA-256 and 222/222 internal `SHA256SUMS.txt` checks; includes docs, source templates and snapshotter.
- R3 snapshotter installed in `scripts/Create-GammaUnwind-SourceSnapshot.ps1` per user confirmation; stops generating external `.zip.sha256` sidecars. **The R3 Windows run was not supplied for independent verification.**

## Installed distribution — 0.8.9 (built 2026-10-25)

- Source identifies `VERSION=0.8.9`, `EDITION=client`, `MANIFEST.json` tag `v0.8.9`.
- Bundled Windows start/stop/update commands, browser dashboard, Telegram-oriented alerts, V1 strategies, configuration and a multi-broker adapter registry.
- Existing 0.8.9 setup guide covers broker login, scanner lists, stop/target behavior and operating instructions; some prose may not express every implementation nuance (notably forming-tick E1). The source must be checked before claiming precise algorithmic parity.

## Change-control rule

Add an entry only after recording the date, installed-versus-disposable scope, exact modified files, source/evidence hash, test outcomes and outstanding HOLDs. Never rewrite prior PASS/HOLD history to imply deployment.
