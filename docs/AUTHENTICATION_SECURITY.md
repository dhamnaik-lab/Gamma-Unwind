# Authentication, session security and data boundaries

## November 2026 account authority and storage security

A login URL, App ID, access token or currently selected broker name is not sufficient to establish a stable authorized broker/account identity for durable market-data state. In the **R1.9 candidate**:
- **Account Identity & Isolation:** `AccountIdentity` (`gammad/broker/account.py`) binds credentials, tokens, and active feeds to a distinct `broker:account_id` namespace, preventing cross-account state leakage.
- **Windows Storage ACLs:** `check_or_enforce_acls` (`gammad/store/security.py`) audits and restricts NTFS directory permissions via `icacls.exe` to `$env:USERNAME` and `SYSTEM`.
- **Tamper & Rollback Guard:** `StateGenerationManager` maintains a monotonic generation counter with a domain-separated SHA-256 seal and SQLite `PRAGMA application_id` stamping, blocking restored older database backups.
- **Path & Junction Protection:** `verify_safe_path` rejects directory traversal, symlinks, and NTFS junction reparse points before database access.

No real credentials or live calls are authorized by this documentation. `R10H_APPLY=HOLD`. See [Project Status](PROJECT_STATUS.md).

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

## Storage and token safety boundary

R1.9 Phase 5 implemented and verified production storage security (`gammad/store/security.py`) and SQLite corruption quarantine recovery (`gammad/store/recovery.py`), closing `G-R18-06` to BOUNDED PASS. However, genuine historical provider rights, live token vaults, and production installation remain on HOLD (`R10H_APPLY=HOLD`). Do not enable live broker calls without explicit approval.

For broader operational controls see [API Reference](API_REFERENCE.md), [Historical Data](HISTORICAL_DATA.md) and [Testing & Release Gates](TESTING_RELEASE_GATES.md).
