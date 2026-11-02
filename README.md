# GAMMA-UNWIND

**Windows client · installed baseline 0.8.9 · documentation updated 2026-11-02**

GAMMA-UNWIND is a locally operated NSE F&O options scanner. It builds support/resistance-driven watchlists, observes option price/volume/open-interest activity, evaluates configured signal rules, and exposes a browser dashboard with optional Telegram alerts and follow-ups. It is **not an order-placement system**. Alerts are informational; paper-position tracking is not broker execution.

> **Release boundary (2 November 2026):** R1.8 bounded offline qualifications passed through the optional 245-file watermark prototype, but none of those changes is installed. R1.9 multi-broker integration is the next **planned** development stage and is not yet implemented. Real provider entitlement, full scheduler/notification delivery, trusted per-account routing, ACL and release readiness remain HOLD. **R10H Apply = HOLD.**

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

## Current source of truth (2 November 2026)

- **Installed R3 source snapshot (verified):** `Gamma Unwind Source Code 2026-10-31_123418_650_IST_cc136e62.zip`, SHA-256 `6AFF21F16381ADA2CDEBD0E706CC03FEA642E4BB262E2BE1FAD233A8D538897D`; 237 original source/document files with the two internal snapshot metadata entries. This supersedes the older 221-file R2 historical snapshot as the 31 October R3 baseline.
- **Installed distribution remains 0.8.9; no R1.8 candidate source applied.** Existing 0.8.9 routes, UI and behavior must not be described as R1.8 features merely because synthetic tests passed.
- **Latest accepted R1.8 watermark evidence:** `GAMMA_R18_WATERMARK_OFFLINE_EVIDENCE.zip` from the 17:20 IST Windows run, SHA-256 `9DE6BC780D9E3B3F175C00DB1B508442F87846C9CDFA11EB27C47C54103ADE2F`. The 245-source disposable candidate SHA-256 is `39C71BFA8F7ABD2C1D4ABBEE8BB1A88F52903E6EDF82A1F86E105E180111A1FE`. The new module is **not connected** to the installed Runtime feed factory.
- **Development direction:** broker-neutral GAMMA with multiple independent accounts/connections, data providers separate from brokers, no preferred broker, read-only paper monitoring. The user-reported FYERS inactivity does not block synthetic multi-broker development.

See [the exact evidence, gate matrix and remaining work](docs/R18_EVIDENCE_AND_R19_HANDOFF.md).

**Documentation-only update:** These Markdown changes preserve Python source, executable, credentials, token vaults, database, configuration and the existing Word setup guide. Applying them to the installed tree changes documentation SHA-256 values, so subsequent 237-file source-gated qualification must explicitly rebaseline the approved documentation delta; never silently weaken prior pins.
