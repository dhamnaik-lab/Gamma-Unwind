# GAMMA-UNWIND

**Windows client · baseline 0.8.9 + Phase 0–4 maintenance & R1.9 multi-broker hardening**

GAMMA-UNWIND is a locally operated NSE F&O options scanner. It builds support/resistance-driven watchlists, observes option price/volume/open-interest activity, evaluates configured `V1N` and V1 signal rules, and exposes a browser dashboard with optional Telegram alerts and follow-ups. It is **not an order-placement system** (`ORDERS = 0`). Alerts are informational; paper-position tracking is not broker execution.

## Simple daily workflow (local Windows client)

1. Run `Check Setup.cmd` (`scripts/smoke_broker.py --check-setup`) to verify `MANIFEST.json` file hashes, `.env` configuration, and broker readiness.
2. Double-click `Start GAMMA.cmd` and open the local dashboard (`http://127.0.0.1:<port>`, default `8760`; host/origin guards validate against the active server port or `GAMMA_PORT`). First start copies `.env.example` (`BROKER=` blank) to `.env` if missing; the app starts with **no default broker** (`Running: NONE (select a broker)`, strategy `engines_allowed=False`) until a broker is selected and staged. Never expose the dashboard to the public internet without `GAMMA_LAN=1` and a strong `GAMMA_LAN_PASSWORD`.
3. **Connecting or switching brokers (`#/token` — Broker Connection Manager R1.4 + R1.5):**
   - The legacy `Broker connections` panel has been removed; `#/token` shows only **Broker connection manager · R1.4** (and the active broker's top daily-login form once configured).
   - Select one of the 11 registered brokers (`dhan`, `zerodha`, `angel`, `fyers`, `upstox`, `nuvama`, `kotak`, `groww`, `shoonya`, `flattrade`, and Stage 1 history-only `definedge`), save API credentials (encrypted in Windows DPAPI), and complete its login step.
   - Click **Verify session & feed (R1.5)** to run an opt-in read-only profile and quote probe.
   - Click **Stage scanner switch** (confirm `SWITCH TO <BROKER>`) to back up `.env` (up to 3 ACL-locked backups in `data/backups/`), auto-create `.env` from `.env.example` if absent, and persist `BROKER` in `.env`, then restart via `Stop GAMMA.cmd` -> `Start GAMMA.cmd` so your last active login reloads automatically.
4. **Decoupled historical data (optional `.env` keys):** Set `GAMMA_HISTORY_PROVIDER` (e.g. `definedge` alongside `BROKER=fyers`, or `none` to disable fallback), `GAMMA_HISTORY_BARS_LIMIT`, and/or `FYERS_HISTORY_TIER` if your history source differs from your active broker (`resolve_configured_adapter(..., for_history=True)`).
5. Always stop via `Stop GAMMA.cmd` so SQLite WAL stores, `Recorder`, and outbox state shut down cleanly. Never commit `.env` or add raw access tokens/secrets to Markdown files.

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
