# Authentication, session security and data boundaries

**Scope:** installed `gammad/auth/oauth.py`, `token_watch.py`, broker auth modules and `gammad/api/server.py`. This is a security reference, not a request to use real credentials.

## Authentication and token lifecycle

- The project contains broker-specific OAuth/login helpers and `TokenWatcher`, which tracks validity, updates, renewal attempts and feed-auth failures.
- `POST /api/token/login` parses the request body itself, validates exact expected login fields and requires `X-Gamma-Control: 1` with JSON content-type. The source avoids echoing malformed secret-field content in errors.
- `GET /api/token` is a **status** route; `POST /api/token` can update a token. These are different trust surfaces, so do not treat a reachable local route as automatically safe for remote clients.
- `/auth/{broker}/callback` is an OAuth callback route. Redirect/state validation must be maintained by the underlying flow; never expose raw authorization codes in docs or evidence.
- `POST /api/brokers/stage` checks loopback origin and local-control header before changing the staged broker.

## Security invariants

| Protected item | Required handling |
|---|---|
| Access/refresh tokens, client secrets, PINs | Backend-only; never stored in Markdown, logs, screenshots, downloadable evidence or query strings |
| OAuth codes, state and redirect | Scope to intended broker/session and validate against expected callback |
| Broker selection | Explicit; staging must not silently switch live feed or account identity |
| `*.env` and live configuration | Keep outside source snapshots; use only reviewed templates with dummy values |
| Evidence files | Save test status, hashes and sanitized structural errors only |
| Local HTTP routes | Preserve per-route origin, header and edition checks; localhost is not a substitute for authorization |
| Broker SDK/history access | Read-only scope, entitlement check and explicit separate live-call approval |
| Trading orders | Not permitted by this project acceptance scope |

## Recommended failure tests (not production claims)

Wrong OAuth state, expired token, rotated token while a watcher exists, stale watcher revision, invalid broker mapping, repeated login, unauthorized local route, remote route attempted with local-only control, malformed JSON and token-bearing exception text. Review outputs for redaction; avoid dumping `.env` or traceback locals.

## R5 candidate boundary

Offline history work includes a synthetic FYERS session-reuse bridge. R1.8 added a disposable SQLite request ledger keyed by broker/application scope. This is **not proof of secure production token-vault integration**; private Windows ACL, owner scope, file tampering/backup rollback and genuine historical rights are HOLD. Do not enable broker APIs solely because the ledger module exists.

For broader operational controls see [API Reference](API_REFERENCE.md), [Historical Data](HISTORICAL_DATA.md) and [Testing & Release Gates](TESTING_RELEASE_GATES.md).
