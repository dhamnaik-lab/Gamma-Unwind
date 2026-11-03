# GAMMA-UNWIND

**Windows client · installed baseline 0.8.9 · candidate qualification updated 2026-11-03**

GAMMA-UNWIND is a locally operated NSE F&O options scanner. It builds support/resistance-driven watchlists, observes option price/volume/open-interest activity, evaluates configured signal rules, and exposes a browser dashboard with optional Telegram alerts and follow-ups. It is **not an order-placement system**. Alerts are informational; paper-position tracking is not broker execution.

> **Release boundary (3 November 2026):** R1.9 candidate development is qualified through Phases 1–5 and Phase 7 (packaging/rollback qualification). The 249-source candidate archive (`886A2F65C494AD975345550BEA34BD6DD34E1CC3602A46CA9AB565E3EAD4038B`) passed all 56/56 offline unit, integration, and sandbox tests with zero warnings. Phase 6 (Telegram receipt/idempotency) was deferred by user decision. Production deployment, live broker transport, and real orders remain strictly on HOLD. **R10H Apply = HOLD, ORDERS = 0, LIVE_BROKER_REQUESTS = 0.**

## Quick start (installed Windows client)

1. Review [Installation & Operations](docs/INSTALLATION_OPERATIONS.md) and the preserved [`docs/GAMMA-UNWIND-Setup-Guide.docx`](docs/GAMMA-UNWIND-Setup-Guide.docx).
2. Open `Check Setup.cmd`; inspect actual broker readiness, not just a configured login URL.
3. Start with `Start GAMMA.cmd`, then use the local dashboard (the bundled 0.8.9 guide documents `http://127.0.0.1:8760`). Do not expose the dashboard to the public internet.
4. Confirm broker authorization, freshness, subscriptions and daily jobs in status before treating any signal as live-qualified.
5. Stop with the project's supported `Stop GAMMA.cmd` path. Do not terminate or restart during stateful testing without a documented recovery procedure.

The running application's actual endpoints/settings remain authoritative over examples in this documentation. Never add raw access tokens, broker secrets, local session stores or customer/account data to these Markdown files.

## Documentation index

| Area | Guide |
|---|---|
| Installed vs candidate status, known holds | [Project status](docs/PROJECT_STATUS.md) |
| Accepted R1.8 evidence and R1.9 multi-broker handoff | [Checkpoint — 2 November 2026](docs/R18_EVIDENCE_AND_R19_HANDOFF.md) |
| Version and changes | [Changelog](CHANGELOG.md) |
| Components and lifecycle | [Architecture](docs/ARCHITECTURE.md) |
| Broker selection and capabilities | [Broker Manager](docs/BROKER_MANAGER.md) |
| Browser login, tokens and boundaries | [Authentication & Security](docs/AUTHENTICATION_SECURITY.md) |
| History, expiries, request budgets | [Historical Data](docs/HISTORICAL_DATA.md) |
| E2/E1, closed bars and trade states | [V1 Strategy Engine](docs/V1_STRATEGY_ENGINE.md) |
| Subscriptions, reconnect and stale data | [Live Feed & Recovery](docs/LIVE_FEED_RECOVERY.md) |
| Tests, hashes, release controls | [Testing & Release Gates](docs/TESTING_RELEASE_GATES.md) |
| Verified source ZIPs, restoration | [Source Snapshot & Recovery](docs/SOURCE_SNAPSHOT_RECOVERY.md) |
| Current limitations | [Known Issues](docs/KNOWN_ISSUES.md) |
| Operations | [Installation & Operations](docs/INSTALLATION_OPERATIONS.md) |
| Paper monitoring (no orders) | [Paper Trading](docs/PAPER_TRADING.md) |
| Verified route inventory | [API Reference](docs/API_REFERENCE.md) |
| Important decisions | [Decision Log](docs/DECISION_LOG.md) |

## Current source of truth (3 November 2026)

- **Installed R3 source snapshot (baseline):** `Gamma Unwind Source Code 2026-10-31_123418_650_IST_cc136e62.zip`, SHA-256 `6AFF21F16381ADA2CDEBD0E706CC03FEA642E4BB262E2BE1FAD233A8D538897D`; 237 original source/document files.
- **R1.9 Qualified Candidate Source Snapshot (Phase 7):** `Gamma Unwind Source Code 2026-11-03_143234_106_IST_bad86e8f.zip`, SHA-256 `886A2F65C494AD975345550BEA34BD6DD34E1CC3602A46CA9AB565E3EAD4038B`; 249 source files (250 entries in internal `SHA256SUMS.txt`). Verified 100% checksum match and 56/56 tests PASS in isolated sandbox.
- **Installed distribution remains 0.8.9:** Candidate source has been qualified offline; production deployment remains strictly on hold (`R10H_APPLY=HOLD`).
- **Development architecture:** Broker-neutral multi-account session management (`AccountSessionManager`), decoupled historical data (`HistoryProviderConfig`), durable feed watermark (`DurableTickQueue`), Windows storage security (`verify_safe_path`, `check_or_enforce_acls`), and SQLite corruption quarantine recovery (`quarantine_and_recover`).

See [Project Status](docs/PROJECT_STATUS.md) and [Decision Log](docs/DECISION_LOG.md).
