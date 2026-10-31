# Architecture (source-grounded)

**Authority:** installed 0.8.9 client source snapshot, 2026-10-31. The R1.8 disposable bridge/ledger is **not** part of this installed-source architecture. References below are source paths, not claims that every branch has been live-qualified.

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
| `gammad/broker/` | `registry.py`, `capabilities.py`, named adapters | Capability declarations, broker resolution and provider-specific readers |
| `gammad/auth/` | `oauth.py`, `token_watch.py` | OAuth state/redirect logic, token lifecycle and errors |
| `gammad/api/` | `server.py`, `broker_selection.py`, `lan.py` | Dashboard API, stage controls and local/remote restrictions |
| `gammad/core/` | calendar, expiry, session_clock, levels, zones | Trading sessions, strike/expiry selection, zone logic |
| `gammad/watch/` | engine, bars, warmup, subscriptions, reconnect | Five-minute candles and state-machine evaluation |
| `gammad/scanners/` | live, shared, v1r, targets | Scanner models and specific source-backed variants |
| `gammad/alerts/` | router, templates, telegram, gates | Alert selection, message dispatch, muted/held states |
| `gammad/trade/`, `gammad/v1/` | tracker, state/entry/exit/position | Tracking, stops/targets and V1 lifecycle |
| `gammad/store/` | `schema.sql`, `db.py`, `lifecycle.py` | SQLite schema, migrations, tracked connection cleanup |
| `gammad/jobs/` | build_watchlists, mark_levels, readiness | Jobs and daily orchestration |
| `gammad/record/`, `gammad/tray/` | recorder/stale, Windows tray | Data recording and desktop lifecycle |

## Data correctness boundaries

- `ContractEngine.on_bar_close()` explicitly takes **closed** five-minute bars and can generate E2/arming.
- `ContractEngine.on_tick()` is a distinct **forming-bar** E1 breakout path for already armed contracts, using tick LTP and accumulated forming-bar volume. Never describe *all* E1 signals as bar-close-only.
- Broker-selected data are translated through broker-specific adapters and canonical types; source capability declarations do not prove live compatibility.
- Tracked trades and alert follow-ups in SQLite are **application state**, not submitted broker orders. The 0.8.9 guide expressly describes an information-only scanner.

## Installed vs disposable additions

The accepted R1.8 consolidated candidate adds `gammad/r5_request_budget.py` and modifies candidate `gammad/r5_session_history.py`. Its SQLite daily budget passed bounded tests but is **not installed** and its full Runtime integration remains HOLD. Do not add those imports to installed-runtime diagrams or assume the new ledger is already active.

## Operations and source review

Start at `gammad/app.py:Runtime`, then follow `gammad/api/server.py`, `gammad/broker/registry.py`, `gammad/watch/engine.py`, `gammad/alerts/router.py`, and `gammad/trade/tracker.py`. Read `gammad/store/schema.sql` before altering persistent writes, and [Decision Log](DECISION_LOG.md) before changing a protected boundary. No architectural diagram here authorizes new endpoints, broker calls or order methods.
