# Historical market-data integration

## November 2026 current qualification and provider independence

In the **R1.9 candidate**, historical data providers are structurally decoupled from trading broker accounts:
- **Decoupled Architecture:** `HistoryProviderConfig` (`gammad/broker/account.py`) and `load_history_provider_config` (`gammad/broker/config_loader.py`) allow historical market data to be sourced from any designated provider independently of trading broker sessions.
- **Provider Rights HOLD:** Synthetic and offline request budget tests pass, but **genuine provider entitlement, expired-option history, and validated 2,000-bar coverage remain HOLD** (`G-R18-05`).
- **No FYERS Dependency:** The architecture does not rely on FYERS (the user reports FYERS account disabled due to inactivity). Offline development continues with decoupled synthetic feeds.

**Status:** R1.9 candidate architecture qualified offline (56/56 tests PASS); live provider entitlement remains on **HOLD**. See [Project Status](PROJECT_STATUS.md).

## Purpose and data contract

Historical data is needed for expiry discovery, expired option contracts, five-minute option OHLCV/OI, completed trading sessions, cold-start warmup and repeatable V1 strategy replay. A 2,000-bar/history requirement appears in R5 acceptance; do not silently reduce the requirement to satisfy a provider cap.

Every accepted dataset must record source broker/provider, actual instrument identity, contract expiry, strike, option type (CE/PE), timeframe, exchange-local timestamps, complete/partial bar status, OHLCV and OI units, token/session scope, retrieval dates, gaps and origin of any synthetic rows. Reject wrong expiry, missing rights, inconsistent mapping, duplicated/partial candles and apparent live-data shortcuts.

## Installed source references

- `gammad/core/expiry.py`, `chain_select.py`: expiry/strike/list selection.
- `gammad/watch/warmup.py`, `bars.py`, `backfill.py`: completed-candle reconstruction and restarts.
- `gammad/broker/fyers/adapter.py` and provider-specific adapters: market-data interfaces; exact available endpoints/call budgets require live verification.
- `gammad/core/calendar.py`, `session_clock.py`, `config/market_sessions.yaml`, `config/nse_holidays.yaml`: session boundaries and calendar inputs. Do not assume local holiday config proves full official calendar coverage.

## R1.8 budget correction — disposable candidate only

Accepted source changes: new `gammad/r5_request_budget.py` plus candidate `gammad/r5_session_history.py` (and manifest). SQLite ledger uses `BEGIN IMMEDIATE` reservations, six requests per India-local calendar day per broker/application scope, and commits before a request; retries/failures count. Tests cover process restart, concurrent attempts, deleted/corrupt ledger and clock rollback refusal. It requires explicit ledger provisioning/path. The earlier in-memory `WeakKeyDictionary` remains only for injected offline fakes in the derived candidate.

**Not qualified:** actual 50-file candidate Runtime integration, Windows private ACLs, ledger copy/rollback resistance, account-owner isolation, approved live request transport, FYERS historical entitlement and full 2,000 official NSE sessions. **Never place the ledger in production from these docs.**

## Offline-to-live gate sequence

1. Verify exact baseline and candidate hashes and read-only contract boundaries.
2. Validate synthetic request/session rotation and fail-closed budget behavior; run entire actual Runtime against the derived candidate in disposable mode.
3. Obtain certified source authority for real expired contracts and 2,000 official five-minute sessions, preserving all gaps and uncertainty.
4. Separately authorize and scope the first genuine **read-only** broker request; record entitlement and outcome without credentials.
5. Verify resulting data against complete-session timestamps, expiry alignment and signal traces; keep R10H on HOLD until all independent release gates pass.

References: [Project Status](PROJECT_STATUS.md), [Testing](TESTING_RELEASE_GATES.md), [Decision Log](DECISION_LOG.md).
