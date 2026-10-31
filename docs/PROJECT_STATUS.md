# Project status and release authority

**Status date:** 2026-10-31 IST · **Development:** paused · **R10H production Apply:** HOLD.

## Three independent authorities

| Authority | What it proves | What it does not prove |
|---|---|---|
| **Installed client** | Audited 0.8.9 Windows source captured in the 221-file R2 snapshot | That experimental R5 patches are installed |
| **Disposable qualification candidate** | Pin-verified, modified sources under offline tests | Live market entitlement, installed behavior or order-safe release |
| **Evidence archive** | Specific PASS/HOLD assertions and their qualification scope | Anything beyond the assertions/tests it actually records |

The supplied source archive is `Gamma Unwind Source Code 2026-10-31_023834_017_IST_7f5baf5a.zip` with SHA-256 `7408392DDBB5B4E50854D796303328D90A4734654ED5351BB34038583D29BF44`. Its internal manifest has 221 files. `VERSION=0.8.9`, `EDITION=client`. A later R3 snapshot script was installed; it is not evidence that installed source gained R5 candidate changes.

## Latest R1.8 consolidated offline evidence

- Disposable candidate SHA-256: `0775530887E7900AB986310C7A04C48B8593E9EE90C6B2A100D54F4459AE25F4`.
- Evidence ZIP SHA-256: `370C943F3F21B6AA9EF18D57A4F9A53FE0DC2C044EC17BD53EA8B71508762A5B`.
- Accepted previous candidate input SHA-256: `F66186826996B5FB692F5176340F46FE03579481C0E36A43570A5E5FB10E0D05`.
- Accepted previous evidence input SHA-256: `36F28EB9BAA9DA1A3AD49D9242147EBEED7CE3F5C6A613017C446D3B75A62E68`.
- 31/31 bounded offline tests PASS; derived 50/50 source member hashes PASS; zero production writes, live broker requests and orders **reported**. Actual Runtime with *new 50-file candidate* explicitly **not retested** in this evidence.

## Gate matrix

| Gate | Verified outcome | Release decision |
|---|---|---|
| R1.7 bounded lifecycle | Prior synthetic scheduler/history qualification PASS | Partial only |
| R1.8 natural E2→E1, duplicate refusal, seven position scenarios | Prior bounded genuine-source replay PASS | Partial only |
| Router exit holds and SQLite fixture reopened | Bounded source-backed tests PASS | No durable outbox certification |
| FYERS two-fake-socket reconnect | Fake transport tests PASS | Not real feed restart certification |
| Durable history budget | New disposable SQLite reservation, concurrency and subprocess restart PASS | Private Windows ACL, rollback resistance and real transport integration HOLD |
| Full actual Runtime of 50-file candidate | Not executed in accepted consolidated evidence | HOLD |
| Full E2→E1→router→position→exit→outbox replay | Not certified | HOLD |
| Durable delivery, idempotency and crash recovery | Not certified | HOLD |
| Complete stale-tick handling and process-restart re-subscription | Not certified | HOLD |
| Official NSE 2,000-session completed history | Not obtained and certified | HOLD |
| Genuine authorized FYERS expired-contract history | Not demonstrated | HOLD |
| R10H Apply/Verify/Rollback | Not authorized | **HOLD** |

## Next controlled milestone

Maintain the pause until work is deliberately resumed. On resumption: acquire exact source-pinned router/outbox/schema/recovery dependencies; qualify the updated 50-file candidate in the actual disposable Runtime; test a durable protected outbox and feed failure/restart cases; preserve untouched baseline V1 traces. Read-only live FYERS history requires **separate authorization** and confirmed entitlement; never infer permission from the presence of a token.

## Evidence classification language

Use **PASS** only for a test actually run and observed; **PASS — bounded** for synthetic/fake/isolated testing; **HOLD** for missing evidence or unqualified production behavior; **NOT APPLIED** for any disposable-only changes. Treat screenshots, a standalone test-suite PASS or a populated UI token field as insufficient to claim live readiness. Link exact evidence filename/hash when changing this document.
