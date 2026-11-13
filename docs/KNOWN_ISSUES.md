# Known issues, gaps and release blockers

**Updated after Phases 0–4 hardening.** Historical items resolved in the current workspace are tracked accurately below; live external provider testing and credential rotation obligations remain explicit.

| ID | Category / status | Current classification | Required closeout |
|---|---|---|---|
| G-R18-01 | Actual Runtime constructor/lifecycle | **PASS (offline suite)** (`tests/test_r19_runtime_lifecycle.py`, `tests/test_v1r_ladder_exits.py`) | Live market session verification |
| G-R18-02 | Persistent outbox crash safety | **BOUNDED PASS** for ambiguous-claim quarantine | External Telegram receipt reconciliation |
| G-R18-03 | V1 & `V1N` lifecycle, CAS fallback, ladder fees & ban retry | **PASS (offline suite)** (`tests/test_r19_runtime_lifecycle.py`, `test_v1r_ladder_exits.py`, `test_jobs_history_ban.py`) | Live market stream replay check |
| G-R18-04 | Feed watermark (`ts == recv`, sub-ms), backpressure & bar volume | **PASS (offline suite)** (`tests/test_r19_broker_neutral.py`, `tests/test_feed_watermark_bars.py`) | Live provider burst soak test |
| G-R18-05 | Expired-contract/history entitlement & FYERS 5y cap | **MITIGATED (offline)** via decoupled `resolve_configured_adapter` + `_fyers_history_preflight` | Approved rights-backed live data test |
| G-R18-06 | Storage ACLs, rollback guard & `.env` backup containment | **PASS (offline suite)** (`tests/test_r19_storage_security.py`, `data/backups/` max-3 rotation) | Periodic disk ACL audit (`Check Setup.cmd`) |
| G-R19-01 | Concurrent multi-broker, no-default-broker & R1.4/R1.5 Manager | **PASS (offline suite)** (`tests/test_api_security_broker_mgr.py`: dynamic port, SPA JSON/Origin guard, WS redaction, 11-broker R1.5 verify, unset-broker startup & `.env` switch persistence) | Live per-broker session verification |
| G-R19-02 | Historical-data provider independence | **PASS (offline suite)** (`GAMMA_HISTORY_PROVIDER`, `definedge` Stage 1, `GAMMA_HISTORY_BARS_LIMIT`, `FYERS_HISTORY_TIER`, `"none"`) | Provider rate-budget live test |
| G-SEC-01 | Prior `.env` (`FYERS_*`) tracked in git history before `.gitignore` / `git rm --cached` | **ACTION REQUIRED (user)** | **Rotate exposed `FYERS_*` credentials** in FYERS portal and update local `.env` |
| G-AUTH-01 | Broker catalog vs live-qualified sessions | **OPEN** (11 registered; no default broker at startup; auto-switch allowed for 10 non-beta after R1.5 verify) | User click **Verify session & feed (R1.5)** & **Stage scanner switch** per broker |
| G-R18-07 | Real Telegram follow-ups and reconciliation | **HOLD** | Controlled external Telegram verification (`ORDERS=0`) |

## Closed investigation / bounded acceptance (not installed deployment)

- Windows `subprocess.Popen` test-guard compatibility fixed in **test-only harness**, allowing 10/10 watermark tests to execute. No broker source modification was required for that harness issue.
- Duplicate `T1` inserted in a rollback fixture violated `UNIQUE(trade_id, kind)`; test-only fixture corrected, bounded SQLite rollback accepted. This was not proof of an application database defect.
- The unguarded FYERS feed process-reconstruction timestamp regression is real **within the tested fixture**. The optional durable prototype blocks it in synthetic scope, **not** in the installed Runtime.

## Decision boundary

Keep `R10H_APPLY=HOLD`, `LIVE_BROKER_CALLS=NONE`, `ORDERS=0`. Do not authorize production writes, real broker interactions or external Telegram sends from this document. See [current evidence](R18_EVIDENCE_AND_R19_HANDOFF.md).
