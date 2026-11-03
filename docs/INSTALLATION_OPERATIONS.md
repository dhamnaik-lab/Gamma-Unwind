# Installation, daily operations and troubleshooting

## November 2026 operations and change-control notice

Installed GAMMA 0.8.9 remains the installed baseline until an authorized release deployment is performed. In the **R1.9 candidate**, multi-broker integration, preflight storage security, and corruption quarantine recovery are implemented and verified:
- **Storage Pre-flight Guards:** `_preflight_storage_guard` runs on startup, auditing NTFS permissions via `icacls.exe`, verifying SQLite integrity with `PRAGMA quick_check(1)`, quarantining corrupt databases, and verifying monotonic generation seals (`StateGenerationManager`).
- **Release Candidate Qualification:** The candidate package (249 source files, ZIP SHA-256 `886A2F65C494AD975345550BEA34BD6DD34E1CC3602A46CA9AB565E3EAD4038B`) passed all 56 offline tests in an isolated sandbox.
- **Change Control:** Live broker calls, real orders, and production installation remain unauthorized (`LIVE_BROKER_REQUESTS=0`, `ORDERS=0`, `R10H_APPLY=HOLD`). See [Project Status](PROJECT_STATUS.md).

**Installed source:** Windows `Start GAMMA.cmd`, `Stop GAMMA.cmd`, `Update GAMMA.cmd`, `Check Setup.cmd`, `run_gamma.pyw`; `gammad/app.py`, `gammad/install.py`, `gammad/tray/` and `gammad/jobs/readiness.py`. Full user instructions remain in the preserved `GAMMA-UNWIND-Setup-Guide.docx` (0.8.9).

## Windows client location

Project: `E:\TA Projects\Gamma Unwind`. The source snapshot has already moved from the old folder `GAMMA-UNWIND-client-0.8.9-win64`; old launch commands and historic evidence paths are not proof that the old folder still exists. Work from the actual new directory in PowerShell 7.

## Operator checklist

1. Inspect current installed `VERSION` and `EDITION`, then run `Check Setup.cmd`. The source snapshot reports version **0.8.9**, edition **client**.
2. Confirm supported configuration and broker-selection prerequisites before starting. Never paste credentials into reports or chat; store them only in approved local config/UI flows.
3. Start with `Start GAMMA.cmd`. Inspect tray/service status; consult the installed guide for Windows startup, wake and update behavior.
4. Open the local dashboard. The 0.8.9 guide documents `http://127.0.0.1:8760`; check actual current binding and local access policy instead of assuming remote access is safe.
5. Confirm broker login/validity, appropriate market session, last tick, live subscriptions, daily readiness, source data freshness, 5-minute completed bars and applicable job states.
6. Verify watchlists and alert tracking only after all prerequisite data is present; otherwise treat status as partial/blocked. A dashboard signal does not equal an executed trade.
7. Stop via `Stop GAMMA.cmd` for normal operations. Do not force-close while testing outbox/ledger recovery unless operating in a disposable test environment with documented crash-injection controls.

## Reference configuration

`config/` contains cost, market-session, holiday, strategy and V1-related YAML files. The source includes `.env.example`; actual `.env` files and private tokens are **excluded** from source audit snapshots. The intended per-broker environment keys can be inspected through the local setup guide and registry; do not treat the sample as a live credential store.

## Troubleshooting table

| Symptom | First checks | Safe action |
|---|---|---|
| Login OK, no ticks/subscriptions | Token expiry, provider connection, symbol mapping, entitlement, market-hours and subscription count | Mark feed BLOCKED/PARTIAL; do not infer a live trade signal |
| HTTP 429/rate limit or history failure | Provider quota, request budgets, retry policy and broker identity | Stop unauthorized retries; retain sanitized evidence |
| Incorrect contract/expiry | Canonical instrument mapping, series, expiry calendar and strike | Reject affected data until identity verified |
| Missing 5-minute candles | Recorder, closed-bar alignment, warmup/backfill, holiday/session boundaries | Use disposable replay rather than fabrication |
| GUI/tray/start failure | Check Setup and launch logs (sanitize) | Do not modify source or run arbitrary update scripts without acceptance |
| SQLite locking | Existing connections, tracked lifecycle and running process | Do not delete live DB/WAL; isolate source of lock |
| Failed source snapshot | Review preview selection, excluded paths, reparse/directory safety and free disk | Fix audit script independently; never force snapshot completion |

## Updating or restoring

Before a source change, use the installed R3 snapshot PS1 (`scripts/Create-GammaUnwind-SourceSnapshot.ps1`), verify the archive and record its printed SHA-256. Existing `Update GAMMA.cmd` is a separate application update workflow, **not authorized** simply by creating documentation. Preserve verified source/evidence, stateful databases and .env separately with restricted access.

**Live API calls, real orders and production R10H Apply remain unauthorized.**
