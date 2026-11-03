# Testing, provenance and release gates

**Current 2 November 2026.** This is a release-control specification; no successful offline test alone authorizes installed changes or external activity.

## Accepted bounded evidence (source-gated)

| Stage | Evidence SHA-256 | Result | Limit |
|---|---|---|---|
| R3 source baseline | `6AFF21F16381ADA2CDEBD0E706CC03FEA642E4BB262E2BE1FAD233A8D538897D` | 237 original files | Does not contain user database or R1.8 candidate |
| R1.8 three-gate | `354D967B2927A6F26F6A8006C18E53D26710705E9801E84C1AC61C39ADBA0AE4` | 15/15 offline tests, 244 candidate pins | No live transport/full Runtime in that report |
| R1.8 actual Runtime | `8EB4AF8F715FFA5D05F6469BE5EF8547F623284A3D0890528B707B27910E0CD4` | Real-source constructor, fake FYERS, paused router | Not full scheduling/delivery |
| R1.8 final lifecycle | `894ECE7737CB837D43F0A62F51A4C55F1C84178976E70C81713E82150F0FEC9E` | Nonzero E2/E1, T1/T2, ambiguous event, disposable backup/rollback | Recreated unguarded feed accepted stale timestamp |
| R1.8 optional durable watermark | `9DE6BC780D9E3B3F175C00DB1B508442F87846C9CDFA11EB27C47C54103ADE2F` | 10/10 Windows Python 3.13, 0 failures/errors, 245-source candidate | Not integrated with default feed factory/account trust |
| R1.9 candidate (Phases 1–5 & 7) | `886A2F65C494AD975345550BEA34BD6DD34E1CC3602A46CA9AB565E3EAD4038B` | 249 source files, 56/56 offline tests PASS, 250/250 sandbox checksums PASS | Candidate packaging & rollback qualified; R10H_APPLY=HOLD |

For authoritative candidate ZIP filenames and counts see [R1.8 Evidence Checkpoint](R18_EVIDENCE_AND_R19_HANDOFF.md). Historic four-gate 50-file/31-test results remain an older accepted checkpoint, not the latest source.

## R1.9 integrated acceptance obligations

1. **Source and account authority:** retain exact 237 baseline and 244/245 candidate SHA-256 chain, check unsafe ZIP entries and preserve installed source; approved documentation updates need explicit rebaseline of Markdown-only drift.
2. **Concurrent broker isolation:** independent account sessions, token revisions, broker selection, subscriptions, history ownership, rate budgets and failure containment; two synthetic brokers plus multiple accounts must demonstrate no leakage.
3. **Feed watermark integration:** verify the optional SQLite module is bound to stable trusted account identity and correctly connected to actual feed factory; inject restart, stale/duplicate/missing LTT, ledger unavailable, queue full, account switch and transient feed failure.
4. **Full genuine-source monitor lifecycle:** scheduler/watchlist → history/backfill → V1 E2/E1 → signal/router → paper positions/exit followups → durable outbox. Preserve completed-bar and tick timing and compare untouched baseline/candidate nonzero traces.
5. **Outbox and real-notification safety:** crash before/after durable claim, ambiguous dispatch reconciliation, idempotency/persistence semantics, and authorized provider receipt validation. A quarantined event must not be confused with guaranteed delivery.
6. **Provider/history quality:** rights/entitlements, expiry identity, bar completeness, timezones and 2,000-session or 2,000-bar contract as independently specified; no data source may imply rights based merely on OAuth login.
7. **Windows production safety:** ACL provisioning, ledger migration/rollback resistance, protected backup/restore, no secrets in evidence and approved installer rollback procedures.
8. **Release control:** real broker calls/Telegram notifications and R10H Apply each require separate explicit approval. Continue paper monitoring only; orders always out of scope.

## Status template

```text
INSTALLED_SOURCE_BASELINE=PASS_237_OF_237
ACCEPTED_R19_CANDIDATE=PASS_249_OF_249
SYNTHETIC_BROKER_ACCOUNTS=PASS
RUNTIME_FEED_WATERMARK_INTEGRATED=PASS
FULL_SCHEDULER_V1_OUTBOX_PARITY=PASS
HISTORY_PROVIDER_ENTITLEMENT_AND_QUALITY=HOLD
PRODUCTION_ACL_BACKUP_ROLLBACK=PASS
INSTALLED_WRITES=0
LIVE_BROKER_REQUESTS=0
REAL_ORDERS=0
R10H_APPLY=HOLD
EVIDENCE_ZIP_SHA256=886A2F65C494AD975345550BEA34BD6DD34E1CC3602A46CA9AB565E3EAD4038B
```

Do not label tests complete until the report and source hashes are independently checked. A documentation-only promotion changes original source hashes for Markdown: preserve the previous authority and record a new approved revision, never silently relax strict comparisons.
