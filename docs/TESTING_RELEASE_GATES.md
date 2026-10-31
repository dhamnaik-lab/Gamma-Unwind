# Testing, provenance and release gates

This is a **release-control document**, not a test result generator. Source references come from the installed 0.8.9 snapshot; R5 evidence comes from uploaded, SHA-256-verified disposable ZIPs. Keep provenance and status separate.

## Required stages

1. **Read-only intake:** hash exact source snapshots, candidate input and evidence files; refuse missing/reparse/unsafe ZIP entries or changed pin inventories.
2. **Offline source-gated construction:** modify only approved files in a disposable clone, never installed runtime; rehash every staged source member.
3. **Independent deterministic unit tests:** closed-bar timing, token/session identity, broker isolation, disabled transport, history limits, V1 state, outbox and feed-fault cases.
4. **Actual disposable Runtime acceptance:** construct both untouched baseline and candidate with the *new* source, compare state, lifecycle, session reuse and nonzero trading traces.
5. **Data qualification:** licensed entitlement, official calendar and genuine historical 2,000-session coverage without lookahead/gaps.
6. **Release authorization:** explicit approval for any real network test, and later for production R10H Apply/Verify/Rollback. Never merge these approvals.

## Evidence inventory — 2026-10-31

| Archive | SHA-256 / result | Authority |
|---|---|---|
| Installed source snapshot (`...023834_017_IST_7f5baf5a.zip`) | `7408392DDBB5B4E50854D796303328D90A4734654ED5351BB34038583D29BF44` | 221-file code snapshot; not runtime |
| R1.8 accepted input candidate | `F66186826996B5FB692F5176340F46FE03579481C0E36A43570A5E5FB10E0D05` | Previous bounded parity acceptance |
| R1.8 accepted input evidence | `36F28EB9BAA9DA1A3AD49D9242147EBEED7CE3F5C6A613017C446D3B75A62E68` | Prior R1.7/R1.8 bounded PASS/HOLD |
| Consolidated four-gate candidate | `0775530887E7900AB986310C7A04C48B8593E9EE90C6B2A100D54F4459AE25F4` | 50 source pins; candidate-only ledger/bridge edits |
| Consolidated four-gate evidence | `370C943F3F21B6AA9EF18D57A4F9A53FE0DC2C044EC17BD53EA8B71508762A5B` | 31/31 offline PASS, explicitly not full actual Runtime retest |

Do not interpret a raw ZIP checksum as evidence that every original provider or release condition passed. It proves integrity of that artifact, not correctness of the claims inside it.

## Mandatory HOLD conditions

- Missing/changed original source hashes, unsafe archive entries, test skips not authorized, or unreviewed dependencies.
- Missing session ownership revision, token exposure, wrong broker, unsafe history call or provider rights unclear.
- No complete 2,000-session official market history, incorrect expiry/series or insufficient data quality.
- No end-to-end E2→E1→router→position→exit→outbox/restart evidence; zero-signal-only parity is insufficient.
- Durable budget stored only in memory, unprotected ledger or unqualified crash/rollback recovery.
- Reconnect/contract mapping stale, live subscriptions absent or fake feed mistaken for real.
- Missing explicit authorization for real network calls or production deployment.

## Result template

```text
SOURCE_SHA256_GATE=PASS|HOLD
INSTALLED_WRITES=0
LIVE_BROKER_REQUESTS=0
REAL_ORDERS=0
BASELINE_RUNTIME=PASS|HOLD
CANDIDATE_RUNTIME=PASS|HOLD
NONZERO_V1_AND_OUTBOX_PARITY=PASS|HOLD
FEED_RESTART_RECOVERY=PASS|HOLD
DURABLE_BUDGET_AND_RESTART=PASS|HOLD
OFFICIAL_HISTORY_AND_ENTITLEMENT=PASS|HOLD
R10H_APPLY=HOLD
EVIDENCE_ZIP_SHA256=<computed value>
```

The template is not a filled result. Record precise test names, fixture scope, skipped tests and failure injection. **Do not assert success before the actual runner and evidence both confirm it.**

## Current decision

The accepted four-gate evidence explicitly has `actual_runtime_retested_with_new_candidate=false`; full outbox recovery, full strategy flow, stale-tick restart and FYERS real history remain HOLD. Preserve the R1.8 pause and separate production authorization.
