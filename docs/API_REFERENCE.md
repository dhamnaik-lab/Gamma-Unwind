# API route inventory (installed source 0.8.9)

## November 2026 applicability note

This inventory describes the verified dashboard API routes. The **R1.9 candidate** maintains full backwards compatibility with all declared 0.8.9 API endpoints while operating on the revised multi-broker `Runtime` and preflight storage engine. No unauthorized public endpoints have been added. See [Project Status](PROJECT_STATUS.md).

**Authority:** routes declared in `gammad/api/server.py` in the 2026-10-31 installed-source snapshot. This is an inventory, **not** a complete OpenAPI schema or permission audit. Return models, query options, authentication/edition guards and statuses must be checked against the exact handler before implementing an API client.

## Broker and login

| Method | Route | Intended use |
|---|---|---|
| GET | `/api/brokers/catalog` | Safe broker registry metadata; no configured secret values (legacy `Broker connections` panel removed from `#/token`) |
| POST | `/api/brokers/stage` | Disabled (`409`) by R1.4 middleware; use `/api/broker-manager/switch/{name}` |
| GET | `/api/token` | Broker/token status and event summary |
| POST | `/api/token` | Token update route; *sensitive control* |
| POST | `/api/token/login` | Exact-field client login; JSON and custom control header |
| POST | `/api/token/renew` | Renewal control; owner/feature restrictions |
| POST | `/api/token/auto-renew` | Renewal feature control |
| GET | `/auth/{broker}/callback` | OAuth callback, not a general-purpose data API |

## Status, configuration and data

| Method | Route | Description |
|---|---|---|
| GET | `/api/status` | Runtime/broker/scanner status |
| GET | `/api/calendar` | Market/event calendar view |
| GET | `/api/session-hours` | Current session-hours view |
| GET | `/api/levels` | Marked price levels |
| GET | `/api/watchlists` | Watched stock/contract lists |
| GET | `/api/trades` | Locally tracked trade state |
| GET | `/api/signals` | Scanner/alert signal records |
| GET | `/api/contract/{security_id}` | Contract-specific view |
| GET | `/api/user-symbols` | User-symbol selection state (not necessarily active scanning) |
| POST | `/api/user-symbols` | Change local user-symbol selection |

## Scanner, monitoring and controls

| Method | Route | Description |
|---|---|---|
| GET | `/api/scanner/today` | Current scanner summary |
| GET | `/api/scanner/daily` | Daily scanner results |
| GET | `/api/scanner/weekly` | Weekly scanner results |
| GET | `/api/scanner/signals` | Scanner signal results |
| GET | `/api/v2-scanner` | V2-scanner state |
| POST | `/api/v2-scanner` | Change V2-scanner setting; feature gates apply |
| GET | `/api/logs` | Sanitized operational log view; do not assume raw logs are safe |
| GET | `/api/preview` | Alert/card preview |
| GET | `/api/mutes` | Muted instrument records |
| POST | `/api/mute` | Change mute state |
| GET | `/api/export` | Local data export (review parameters/content) |
| POST | `/api/jobs/{name}/run` | Trigger supported job; guard/edition and network implications must be reviewed |
| POST | `/api/alerts/pause` | Pause/resume alert path |
| POST | `/api/send` | Manual alert/notification path; **not** a broker order path |
| POST | `/api/service/stop` | Local service stop control |

## Broker Manager (R1.4 + R1.5 local-only routes)

| Method | Route | Intended use |
|---|---|---|
| GET | `/api/broker-manager/status` | Safe status, DPAPI profile completeness, R1.5 verification flags & `can_stage` |
| GET | `/api/broker-manager/login-url/{name}` | Domain-allowlisted OAuth authorization URL for an inactive broker |
| POST | `/api/broker-manager/profile/{name}` | Store inactive broker credentials in encrypted DPAPI vault |
| POST | `/api/broker-manager/login/{name}` | Complete OAuth / field exchange for an inactive broker |
| POST | `/api/broker-manager/disconnect/{name}` | Clear an inactive broker profile with `DISCONNECT <BROKER>` confirmation |
| POST | `/api/broker-manager/r15/verify-auth/{broker}` | Opt-in read-only profile + quote probe across all 11 brokers |
| POST | `/api/broker-manager/switch/{name}` | R1.5-verified `.env` backup (`data/backups/`) & broker staging (restart required) |
| POST | `/api/broker-manager/r15/activate/{broker}` | R1.5-gated activation entrypoint delegating to `gamma_r14.switch` |
| GET | `/api/broker-manager/r15/status` | In-memory R1.5 session & feed proof summary (never reads secrets) |

## Security and testing rules

- **SPA mutating routes** (`POST /api/token`, `/api/jobs/{name}/run`, `/api/alerts/pause`, `/api/send`, `/api/mute`) use `_guard_spa_post`: requires `Content-Type: application/json`, blocks `Sec-Fetch-Site: cross-site`, and validates same-origin loopback `Origin` without requiring `X-Gamma-Control: 1`. Note `My Watchlist` (`/api/user-symbols`) displays *"Saved draft (scanner filtering inactive)"* when saved.
- **Control & Broker Manager routes** (`POST /api/service/stop`, `/api/user-symbols`, `/api/v2-scanner`, `/api/broker-manager/*`) enforce `gamma_r15.local_request_ok` / exact loopback server port matching (`scope["server"]` or `GAMMA_PORT`), reject proxy headers (`lan.is_remote`), require `Origin` matching `Host`, and require `X-Gamma-Control: 1` where documented.
- `POST /api/brokers/stage` is explicitly disabled (`409`) by R1.4 middleware in favor of verified R1.4/R1.5 switching.
- `POST /api/token/login` and WebSocket feed error logs (`_scrub_ws_err`) redact secrets; never print request bodies or access tokens when troubleshooting.
