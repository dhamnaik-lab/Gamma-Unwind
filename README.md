# GAMMA-UNWIND

**NSE F&O options monitoring, open-interest unwind signals, and paper-trade tracking — with a local dashboard.**

**Repository baseline:** `0.8.9` (see [VERSION](VERSION))  
**Primary audience:** Indian-market traders and developers evaluating a signal-monitoring workflow  
**Status:** Development / controlled validation. **Not approved for unattended live trading or order execution.**

GAMMA-UNWIND monitors Indian stock-option contracts around support and resistance zones. It combines daily market structure with option-chain selection, intraday open interest (OI), volume, premium movement, and price action to identify potential setups. The application provides a local dashboard, optional Telegram alerts, and simulated trade lifecycle tracking.

> **Safety:** Alerts and virtual trade results are informational, not investment advice. The packaged client described in [README.txt](README.txt) does **not** place broker orders. Broker credentials must never be committed to GitHub. A successful offline test does not establish live broker compatibility, strategy profitability, or production readiness.

## What it does

- **Market structure:** Builds daily support/resistance zones for the NSE F&O stock universe using historical candles and a configurable lookback.
- **Options watchlists:** Selects eligible out-of-the-money CE/PE contracts near relevant zones, with open-interest and liquidity filters.
- **Intraday monitoring:** Processes market data and evaluates OI unwind, volume build-up, consolidation, breakout, and underlying-stock context.
- **Signal controls:** Uses strategy gates, completed-bar evaluation where specified, de-duplication, and explicit handling of missing or stale data.
- **Paper trade tracking:** Records simulated entries, stops, targets, trailing logic, follow-up events, and exits; it is not a broker order-management system.
- **Local dashboard and notifications:** Displays levels, watchlists, signals, feed status, trade events, and operational checks; can send Telegram alerts when configured.
- **Operational tools:** Includes broker setup checks, token management, scheduled jobs, local logs, and a persistent SQLite store.

The exact strategy behavior is defined by the code and [config/strategy.yaml](config/strategy.yaml), not by performance claims in this README. No win rate, expected return, or profitability is asserted.

## Architecture at a glance

| Area | Key files / directories |
| --- | --- |
| Supervisor, scheduling, and feed orchestration | [`gammad/app.py`](gammad/app.py) |
| Broker registry and capability assessment | [`gammad/broker/`](gammad/broker/) |
| Support/resistance and watchlist jobs | [`gammad/jobs/`](gammad/jobs/) |
| Intraday signal engine | [`gammad/watch/`](gammad/watch/) |
| Virtual trade lifecycle | [`gammad/trade/`](gammad/trade/) |
| Local dashboard API | [`gammad/api/server.py`](gammad/api/server.py) |
| SQLite persistence and migration | [`gammad/store/`](gammad/store/) |
| Strategy parameters | [`config/strategy.yaml`](config/strategy.yaml) |
| Broker and Telegram configuration template | [`.env.example`](.env.example) |

The local API runs on the loopback interface (`127.0.0.1`) rather than being intended as a public Internet service. The default packaged dashboard address documented for this release is **http://127.0.0.1:8760**.

## Broker integrations

The 0.8.9 registry declares the following broker names:

**Dhan, Zerodha, Angel One, FYERS, Upstox, Nuvama, Kotak Neo, Groww, Shoonya, and Flattrade.**

**Registered is not the same as live-qualified.** Availability of historical candles, options-chain data, OI, streaming subscriptions, and authentication varies by broker, account, entitlement, and API plan. Several adapters are labeled beta or unverified in the source. The application must refuse unsupported capabilities rather than silently treating them as available.

For the client edition, select a broker explicitly in `.env`. Do not assume that a connected feed or a successful login establishes sufficient historical-data rights or complete strategy coverage.

## Getting started (packaged Windows client)

These steps are for a **complete distributed client ZIP**, which includes its bundled Python runtime. A GitHub source checkout alone may not include that runtime or all packaged assets.

1. Extract the full ZIP into a short folder path on Windows 10/11 (64-bit); do not run it inside the archive.
2. Copy [`.env.example`](.env.example) to `.env` if necessary. Configure the Telegram settings, `BROKER`, and only the credentials required for your selected broker. Keep `.env` private.
3. Run **`Check Setup.cmd`** and resolve reported configuration or capability problems.
4. Run **`Start GAMMA.cmd`**. Use the tray icon to open the local dashboard at **http://127.0.0.1:8760**.
5. Complete the broker's supported manual/token login flow and check feed, subscription, and data readiness before monitoring.
6. Quit from the tray when finished; consult [README.txt](README.txt) for packaged-client instructions and troubleshooting.

For developers working from source, the Python command-line interface exposes `python -m gammad --help` and `python -m gammad --version` **after** an appropriate environment and dependencies are provisioned. Do not treat those commands as a complete source-install procedure.

## Configuration and data

- [`config/strategy.yaml`](config/strategy.yaml) holds zone, proximity, expiry, schedule, watcher, and signal-gate parameters.
- [`.env.example`](.env.example) lists broker-specific and Telegram environment variables; the real `.env` must remain untracked.
- The application uses a local SQLite database (normally `data/gamma.db`) and versioned migrations in [`gammad/store/schema.sql`](gammad/store/schema.sql).
- Timestamps and exchange-session scheduling are based on **Asia/Kolkata (IST)**.

Avoid editing the live database or swapping broker configurations during a market session without an explicit backup and controlled validation.

## Validation and release boundaries

Development work includes offline tests, deterministic replay, source-integrity checks, and staged integration qualification. **These are distinct from production approval.**

As of **31 October 2026**, the ongoing R5 / R10H integration work remains subject to release holds for items such as real historical-data entitlement, sufficiently deep official history, full strategy/runtime parity, broker/feed qualification, and protected exit transport. Passing isolated or synthetic tests does **not** clear those holds.

Accordingly:

- Do not enable live orders or assume production readiness from this repository.
- Treat unsupported market data, stale feeds, missing permissions, and incomplete broker capabilities as blockers.
- Verify reproducibility, no-lookahead behavior, market-session boundaries, and out-of-sample results before relying on any signal.
- Keep secrets, access tokens, user data, databases, and internal evidence archives out of commits and issue reports.

## Further reading

- [README.txt](README.txt) — release-specific usage, setup, and troubleshooting.
- [`.env.example`](.env.example) — supported configuration keys.
- [`gammad/__main__.py`](gammad/__main__.py) — available CLI commands.
- [`gammad/edition.py`](gammad/edition.py) — client/owner edition behavior.

**Disclaimer:** This software is for research, monitoring, and paper-trade evaluation. Option trading entails substantial risk; the presence of signals, targets, and simulated P&L is not evidence of profitability.
