# Architecture (source-grounded)

## Development architecture checkpoint — 3 November 2026

Current installed architecture remains 0.8.9. The **R1.9 candidate architecture** is fully implemented and qualified across Phases 1–5 and Phase 7 (56/56 offline tests PASS, candidate ZIP SHA-256 `886A2F65C494AD975345550BEA34BD6DD34E1CC3602A46CA9AB565E3EAD4038B`). Key candidate additions include:
1. **Concurrent Broker & Account Sessions:** `AccountSessionManager` and `AccountIdentity` in `gammad/broker/account.py` providing failure isolation and dynamic switching.
2. **Decoupled History Provider:** `HistoryProviderConfig` and `load_history_provider_config` in `gammad/broker/config_loader.py` allowing market data retrieval independent of trading brokers.
3. **Broker-Neutral Durable Watermark:** `WatermarkStore` and `DurableTickQueue` in `gammad/broker/durable_watermark.py` enforcing monotonic exchange LTT progression across reconnects.
4. **Storage Security & Rollback Protection:** `verify_safe_path`, `check_or_enforce_acls`, and `StateGenerationManager` in `gammad/store/security.py` enforcing NTFS ACLs and preventing backup restoration rollbacks.
5. **SQLite Corruption Recovery:** `verify_database_integrity` and `quarantine_and_recover` in `gammad/store/recovery.py` isolating damaged databases and rebuilding clean WAL stores.

**Authority:** installed 0.8.9 client source snapshot, 2026-10-31; R1.9 candidate qualified offline 2026-11-03. Production deployment remains on hold (`R10H_APPLY=HOLD`).

## Component boundaries

```text
Windows launchers / run_gamma.pyw
             |
        gammad.app.Runtime
        /     |        \
  config / broker        FastAPI dashboard and controls
         registry         gammad/api/server.py
         |                  |
  broker adapters         query/status/control routes
  & session watcher       (permission/edition restrictions)
         |
   live feed / histories
         |
   gammad/watch/*  -> ContractEngine E2/E1, bar assembly, warmup
         |
   gammad/alerts/router.py -> Telegram/alert records & eligibility
         |
   gammad/trade/tracker.py -> simulated/follow-up trade lifecycle
         |
   gammad/store/db.py + schema.sql -> application state
```

## Installed modules and responsibilities

| Directory | Source authority | Responsibility |
|---|---|---|
| `gammad/app.py` | `Runtime`, scheduler/feed/recorder methods | Main orchestration, load_day, handle_tick and lifetime |
| `gammad/config.py` | `Settings`, `load_settings`, `EnvWatcher` | Environment/configuration and hot-reload behavior |
| `gammad/broker/` | `registry.py`, `capabilities.py`, `account.py`, `config_loader.py`, `durable_watermark.py` | Multi-account sessions, history decoupling, watermark engine, broker resolution |
| `gammad/auth/` | `oauth.py`, `token_watch.py` | OAuth state/redirect logic, token lifecycle and errors |
| `gammad/api/` | `server.py`, `broker_selection.py`, `lan.py` | Dashboard API, stage controls and local/remote restrictions |
| `gammad/core/` | calendar, expiry, session_clock, levels, zones | Trading sessions, strike/expiry selection, zone logic |
| `gammad/watch/` | engine, bars, warmup, subscriptions, reconnect | Five-minute candles and state-machine evaluation |
| `gammad/scanners/` | live, shared, v1r, targets | Scanner models and specific source-backed variants |
| `gammad/alerts/` | router, templates, telegram, gates | Alert selection, message dispatch, muted/held states |
| `gammad/trade/`, `gammad/v1/` | tracker, state/entry/exit/position | Tracking, stops/targets and V1 lifecycle |
| `gammad/store/` | `schema.sql`, `db.py`, `lifecycle.py`, `security.py`, `recovery.py` | SQLite schema, migrations, connection tracking, Windows ACLs, tamper seals, corruption recovery |
| `gammad/jobs/` | build_watchlists, mark_levels, readiness | Jobs and daily orchestration |
| `gammad/record/`, `gammad/tray/` | recorder/stale, Windows tray | Data recording and desktop lifecycle |

## Data correctness boundaries

- `ContractEngine.on_bar_close()` explicitly takes **closed** five-minute bars and can generate E2/arming.
- `ContractEngine.on_tick()` is a distinct **forming-bar** E1 breakout path for already armed contracts, using tick LTP and accumulated forming-bar volume. Never describe *all* E1 signals as bar-close-only.
- Broker-selected data are translated through broker-specific adapters and canonical types; source capability declarations do not prove live compatibility.
- Tracked trades and alert follow-ups in SQLite are **application state**, not submitted broker orders. The 0.8.9 guide expressly describes an information-only scanner.

## Candidate additions (qualified offline)

The R1.9 qualified candidate package adds multi-account sessions (`gammad/broker/account.py`), config loading (`config_loader.py`), broker-neutral watermarks (`durable_watermark.py`), storage security (`gammad/store/security.py`), and corruption recovery (`recovery.py`). These components have passed all 56 offline tests and sandbox packaging verification, but **production deployment remains HOLD** (`R10H_APPLY=HOLD`).

## Operations and source review

Start at `gammad/app.py:Runtime`, then follow `gammad/api/server.py`, `gammad/broker/registry.py`, `gammad/watch/engine.py`, `gammad/alerts/router.py`, and `gammad/trade/tracker.py`. Read `gammad/store/schema.sql` before altering persistent writes, and [Decision Log](DECISION_LOG.md) before changing a protected boundary. No architectural diagram here authorizes new endpoints, broker calls or order methods.
