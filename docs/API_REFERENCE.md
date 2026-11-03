# API route inventory (installed source 0.8.9)

## November 2026 applicability note

This inventory describes the verified dashboard API routes. The **R1.9 candidate** maintains full backwards compatibility with all declared 0.8.9 API endpoints while operating on the revised multi-broker `Runtime` and preflight storage engine. No unauthorized public endpoints have been added. See [Project Status](PROJECT_STATUS.md).

**Authority:** routes declared in `gammad/api/server.py` in the 2026-10-31 installed-source snapshot. This is an inventory, **not** a complete OpenAPI schema or permission audit. Return models, query options, authentication/edition guards and statuses must be checked against the exact handler before implementing an API client.

## Broker and login

| Method | Route | Intended use |
|---|---|---|
| GET | `/api/brokers/catalog` | Safe broker registry metadata; no configured secret values |
| POST | `/api/brokers/stage` | Local-only BROKER staging in `.env`, restart required; control/origin restrictions |
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

The server also has a catch-all dashboard route (`/{full_path:path}`), which is not a general-purpose API endpoint. Additional provider-specific or later R5 routes mentioned in previous development discussions must **not** be assumed installed unless present in the current verified source.

## Security and testing rules

- Use same-origin and appropriate `X-Gamma-Control: 1` controls where handlers require them; do not blindly attach this header to every route and assume that satisfies all access checks.
- `POST /api/brokers/stage` explicitly checks loopback origin, JSON content type, broker registry membership and requires a restart. Do not call it in a documentation validation run.
- `POST /api/token/login` avoids echoing typed values; never print request bodies or access tokens to troubleshoot errors.
- Status, manual-send, API export, token and service-control handlers can expose or change sensitive state. No automated calls are authorized by this document.
- Review the exact installed handler before asserting a response schema, side effect, quota or permission. Do not infer that an endpoint supports expired-option historical acquisition.

**Read-only documentation only.** No new routes are created by this package.
