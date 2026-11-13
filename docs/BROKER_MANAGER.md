# Broker Manager, registry and switching

## Architecture & installed multi-broker flow

- **Registry & No Default Broker:** 11 brokers (`dhan`, `zerodha`, `angel`, `fyers`, `upstox`, `nuvama`, `kotak`, `groww`, `shoonya`, `flattrade`, `definedge`) are registered in `gammad/broker/registry.py`. Unset/empty `BROKER` raises `RegistryError` in every edition (`owner` and `client`): strategy engines stay disabled (`Runtime.engines_allowed == False`), `Runtime.broker_name()` reports `"none"`, and `TokenWatcher` stays neutral until a broker is explicitly configured.
- **Multi-Account & Decoupled History:** `AccountSessionManager` (`gammad/broker/account.py`) and `gammad/broker/config_loader.py` support both Mode A (`.env` single-broker fallback + optional decoupled `GAMMA_HISTORY_PROVIDER`, e.g. `definedge` alongside `BROKER=fyers` or unset `BROKER`, with `"none"` disabling history fallback) and Mode B (`GAMMA_ACCOUNTS_JSON`), wired through `resolve_configured_adapter()` across runtime and offline jobs.
- **Unified Broker Manager (R1.4 + R1.5):** `gammad/broker/gamma_r14.py` and `gammad/broker/gamma_r15.py` (with `dashboard/dist/assets/gamma-r14-manager.js`, `gamma-r15.js`, and legacy `GU_BrokersInsideToken` removed from `dashboard/dist/assets/index-GU-R14.js`) provide encrypted credential staging in Windows DPAPI (`%LOCALAPPDATA%\GAMMA-UNWIND\broker-profiles-r14.dpapi`), read-only session + market-feed verification, neutral unselected initial state (`Running: NONE (select a broker)`), and confirmed `.env` switching.

**Source:** `gammad/broker/registry.py`, `gammad/broker/gamma_r14.py`, `gammad/broker/gamma_r15.py`, `gammad/api/broker_selection.py`, `gammad/api/server.py`, and provider `auth.py` / `adapter.py` modules.

## Controls & security boundaries

- `GET /api/brokers/catalog`: Read-only metadata for the 11 registered brokers; never returns configured secret values.
- `POST /api/brokers/stage`: **Disabled (`409`)** when Broker Manager R1.4 middleware is installed; all broker switches must go through verified R1.4/R1.5 endpoints.
- `GET /api/broker-manager/status`: Local-only status for configured profiles, R1.5 verification state (`auth_ok`, `feed_verified`, `switch_eligible`), and `can_stage`. Never triggers vault reads on R1.5 `GET /api/broker-manager/r15/status`.
- `POST /api/broker-manager/profile/{name}`, `login/{name}`, `disconnect/{name}`: Encrypted vault management for inactive brokers (never echoes secrets).
- `POST /api/broker-manager/r15/verify-auth/{broker}`: Opt-in read-only profile + quote probe (`_probe_adapter` / `_probe_quote`) across all 11 registered brokers (including `fyers` and Stage 1 `definedge`) bound by HMAC to the exact credential values.
- `POST /api/broker-manager/switch/{name}` & `POST /api/broker-manager/r15/activate/{broker}`: Requires a valid R1.5 proof (`switch_eligible == True` for non-beta brokers), `expected_active` match (`""` when starting with no active broker), exact `SWITCH TO <BROKER>` confirmation, `acknowledge: true`, JSON body, `X-Gamma-Control: 1`, and same-origin loopback headers matching the active server port (`gamma_r15.local_request_ok`). Creates an atomic ACL-locked `.env` backup in `data/backups/` (keeping the newest 3), creates `.env` from `.env.example` if absent, and stages `BROKER` + keys for restart so subsequent restarts load that last active login.

## Simple user workflow

1. On `#/token` (`Broker login`), open **Broker connection manager · R1.4** (starts with no broker selected by default: `Running: NONE (select a broker)`), save API keys, and complete login exchange for your chosen broker.
2. Click **Verify session & feed (R1.5)** to confirm both profile authentication and live quote capability. (`nuvama`, `shoonya`, `flattrade`, and Stage 1 `definedge` remain `beta-unverified` for auto-switch.)
3. Click **Stage scanner switch**, confirm `SWITCH TO <BROKER>`, then restart GAMMA using `Stop GAMMA.cmd` and `Start GAMMA.cmd` for the staged `.env` configuration to take effect.
4. Keep `Check Setup.cmd` green (`scripts.smoke_broker.check_modified(Path('.'))` `PASS`) before relying on live subscriptions or daily job outputs; remember WebSocket errors scrub `api_key` and `access_token` query params before logging (`_scrub_ws_err`).
