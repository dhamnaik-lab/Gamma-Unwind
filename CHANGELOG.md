# Changelog

This records **evidenced** milestones; it is not a claim that every experimental patch was applied to the installed application. Historical events are summarized from the 0.8.9 source snapshot and supplied R5 acceptance reports. See [Project status](docs/PROJECT_STATUS.md) for current PASS/HOLD gates.

## Unreleased — accepted bounded qualification and documentation refresh (2026-11-02)

- Verified the R3 installed-source snapshot of **237 files** (ZIP SHA-256 `6AFF21F16381ADA2CDEBD0E706CC03FEA642E4BB262E2BE1FAD233A8D538897D`), superseding earlier R2 counts as the current pre-R1.8 installed source authority.
- Accepted R1.8 three-gate offline evidence (**15/15 tests**) for V1 paper flow, persistent outbox claim/quarantine and synthetic stale-feed handling; source manifest **244/244**. This remains disposable, not installed.
- Accepted bounded **actual Runtime constructor** evidence for the 244-source candidate: synthetic token watcher/FYERS session, paused router guard and ambiguous trade event after Runtime reconstruction.
- Accepted bounded **final Runtime lifecycle** evidence for nonzero E2/E1, paper T1/T2 transitions, paused notification outbox, crash/quarantine and disposable SQLite backup/rollback. The evidence also **reproduced an older timestamp being accepted after rebuilding the unguarded feed**.
- Added the optional **durable FYERS watermark prototype** to the 244-source candidate, producing **245 disposable files**. The Windows Python 3.13 source-gated test suite passed **10/10, no errors** after test-only `subprocess.Popen` compatibility correction. This module is **not wired into the default Runtime/broker factory**; trusted account identity and production ledger protections remain HOLD.
- Confirmed **multi-broker concurrent operation is mandatory** and **no broker is preferred**. FYERS currently unavailable according to the user, so offline development will not depend on its login. R1.9 multi-broker architecture/integration is **planned**, not delivered. Work continues in ChatGPT with Windows PowerShell verification; Codex is not part of this project's workflow.
- Updated Markdown documentation and added a dated evidence/checkpoint guide. The preserved setup-guide DOCX and installed application sources are not changed by the update package.
- **Release decision unchanged:** `R10H_APPLY=HOLD`; no real orders, live broker calls or installed-source writes reported by acceptance evidence.

### Evidence for this update

| Artifact | SHA-256 |
|---|---|
| 244-source actual Runtime evidence | `8EB4AF8F715FFA5D05F6469BE5EF8547F623284A3D0890528B707B27910E0CD4` |
| Final lifecycle evidence | `894ECE7737CB837D43F0A62F51A4C55F1C84178976E70C81713E82150F0FEC9E` |
| Optional 245-source watermark candidate | `39C71BFA8F7ABD2C1D4ABBEE8BB1A88F52903E6EDF82A1F86E105E180111A1FE` |
| Windows watermark evidence | `9DE6BC780D9E3B3F175C00DB1B508442F87846C9CDFA11EB27C47C54103ADE2F` |

The 31 October sections below are **historical checkpoints** and are retained to avoid rewriting earlier results as current installed functionality.

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
