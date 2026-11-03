# Broker Manager, registry and switching

## November 2026 design decision — mandatory multi-broker

The installed 0.8.9 registry/profile UI does not constitute a fully concurrent, account-scoped broker Runtime. In the **R1.9 candidate**, multi-broker and multi-account concurrent operations are fully implemented in candidate scope:
- **Multi-Account Session Management:** `AccountSessionManager` (`gammad/broker/account.py`) manages concurrent accounts with failure isolation and dynamic fallback.
- **Config Ingestion:** `gammad/broker/config_loader.py` supports both Mode A (legacy `.env` fallback) and Mode B (multi-account JSON via `GAMMA_ACCOUNTS_JSON`).
- **Decoupled Historical Provider:** `HistoryProviderConfig` enables selecting historical data sources completely independently of trading broker sessions.
- **No Default Broker:** There is no hardcoded primary broker. The user-reported FYERS inactivity does not block offline multi-broker execution. Production deployment remains on hold (`R10H_APPLY=HOLD`). See [Project Status](PROJECT_STATUS.md).

**Source:** `gammad/broker/registry.py`, `gammad/broker/capabilities.py`, `gammad/api/server.py`, `gammad/api/broker_selection.py`, provider `auth.py` and `adapter.py` modules.

## Registry versus live qualification

The installed 0.8.9 manifest names **10 brokers**: Dhan, Zerodha, Angel, FYERS, Upstox, Nuvama, Kotak, Groww, Shoonya and Flattrade. Other source directories may exist; their mere presence does not certify a usable client integration. Broker capabilities are declarations; the real feed/history, OI, mapping and token rights require separate provider-specific acceptance. The guide describes Dhan as verified and several others as beta; do not silently generalize these statements to current sessions.

## Current source-backed controls

- `GET /api/brokers/catalog` returns registry metadata: active broker name, labels, capabilities/status, token-key names and login field names; the handler states that it must not return configured secret values.
- `POST /api/brokers/stage` stages `BROKER` selection in `.env`; it requires a **local loopback request**, same-origin and the documented custom control header. It returns `restart_required`, so **staging is not an immediate runtime switch**.
- `gammad/api/broker_selection.py:stage_broker` is the implementation reference for mutation constraints and backup behavior.
- OAuth and token logic are broker-specific and backed by the `TokenWatcher`/client-login facilities. The presence of multiple login modules does not prove that credentials can be independently held and used by all providers at once.

## Expected safe workflow

1. Inspect the catalog; select a broker explicitly. Do **not** adopt a hidden default/preferred provider.
2. Confirm capability prerequisites (read-only market data, quotes, volume, OI and expired options if required).
3. Stage any selection through the approved local UI; do not manually change active credentials in documentation.
4. Restart only under the normal lifecycle; confirm the active broker and correct token session afterward.
5. Test alternate broker logins in isolation and ensure a staged/secondary login does **not** accidentally replace the active FYERS feed.
6. Keep invalid/unentitled/missing-token capabilities blocked instead of falling back silently to another provider.

## R5 work not assumed installed

Previous GAMMA development included a unified Broker Manager interface, independent broker-login work and broker-neutral qualification. The 2026-10-31 uploaded **installed source snapshot** confirms the registry/staging interfaces but does not alone prove every earlier UI overlay is present or that all 10 brokers are live-qualified. Record those features as **previous development evidence pending installed-source reconciliation**.

## Acceptance obligations

Check explicit broker identity, correct mapping of underlying/option segments, token expiry/rotation, concurrent sessions, safe switch-back, feed readiness, historical entitlement and no orders. Capture only redacted results, error classes and source hashes. A “login OK” indicator is necessary but insufficient for history entitlement or valid live subscription data.
