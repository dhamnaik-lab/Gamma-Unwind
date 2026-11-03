# V1 strategy engine: verified mechanics and parity boundaries

## November 2026 parity checkpoint

In the **R1.9 candidate**, full synthetic V1 strategy lifecycle parity is verified in candidate scope:
- **Full Lifecycle Integration:** Evaluated end-to-end in `tests/test_r19_runtime_lifecycle.py` from history backfill and feed ticks through V1 entry signal firing, paper position opening, Target 1 stop-trailing, Target 2 exit, and crash quarantine (`G-R18-03` promoted to **BOUNDED PASS**).
- **Preserved Signal Contracts:** Preserves installed mechanics: E2 is evaluated on closed five-minute bars; E1 breakout fires on forming-bar live ticks when armed. Stops and targets are tracked deterministically without silent strategy mutations.
- **Release Boundary:** Synthetic verification passes (56/56 tests PASS), but production deployment remains on hold (`R10H_APPLY=HOLD`). See [Project Status](PROJECT_STATUS.md).

**Source authority:** installed `gammad/watch/engine.py`, `gammad/v1/entry.py`, `states.py`, `position.py`, `stops.py`, `exits.py`, `gammad/trade/tracker.py`. Also consult the original setup guide, which describes the scanner in plain words but not every implementation branch.

## Signal state model

| Event/state | Source-backed behavior | Critical timing |
|---|---|---|
| Initial / warming | `ContractEngine` waits for configured completed-bar warmup | No output without sufficient closed bars |
| E2 / arming | `ContractEngine.on_bar_close()` evaluates an ordered series of **closed five-minute bars** and can arm a contract | Completed-bar evaluation; exact rule flags/config matter |
| E1 / breakout | `ContractEngine.on_tick()` fires only when state is ARMED; it checks tick LTP against consolidation high and forming-bar volume against closed-bar SMA | **Forming-bar live tick path**, not necessarily completed-candle confirmation |
| DONE / duplicate protection | E1 moves state to DONE and the same active arm cannot simply fire a second E1 | Must be verified through restart and revision scenarios |
| Position/exit lifecycle | `gammad/v1/position.py`, `exits.py`, `stops.py`, `trade/tracker.py` maintain synthetic position and follow-up state | Not a broker order; open/close semantics require bar-by-bar testing |

### Non-repainting interpretation

The term “non-repainting” must be qualified per signal path. E2 depends on completed bars, but the inspected E1 tick path can fire during a forming bar. A live tick-triggered E1 should be recorded as a timestamped event, **not rewritten later** as if it had waited for candle close. If completed-bar-only E1 is a new requirement, it is a *strategy change* requiring explicit authorization and fresh out-of-sample parity/backtests. Do not silently change V1 while certifying history integration.

## Engine interfaces to inspect before modifying

- `ContractEngine.on_bar_close(bars, oi_now, oi_peak, ...)`: closed 5-minute series, optional underlying bars and arming context.
- `ContractEngine.on_tick(tick, forming_bar, bars, oi_now, oi_peak)`: ARMED-only breakout path; volume SMA from prior closed bars, forming volume and tick LTP.
- `ContractEngine.force_armed(...)`: replay/restoration interface; simulated restart qualification must not be mistaken for durable crash recovery.
- `gammad/v1/entry.py:EntryRule` / `decide`: V1 input, base/filter/decision logic.
- `gammad/v1/exits.py:Exit`, `gammad/v1/position.py`: target/stop/cap/close state, with bar/time assumptions recorded.

## Separating versions and strategies

The 0.8.9 user guide describes Breakdown, Breakout and Carry watchlists and alert/follow-up behavior. The installed tree also has scanner-specific directories such as `gammad/scanners/v1r` and `gammad/watch/engine.py`; these are **not automatically the same decision graph**. Specify exact edition, flags, engine revision, broker data and whether inputs are completed bars or live ticks for any comparison.

## What has actually passed offline

The earlier R1.8 accepted evidence reports a natural synthetic E2→E1 cycle, duplicate E1 refusal, restart-from-arm fixture and seven genuine position event scenarios. It also reports a bounded router exit-hold check and fake feed restart. These are **not** a continuous E2→E1→position→router→persistent outbox trade trace; full V1 baseline/candidate equivalence remains HOLD.

## Required reproducible parity report

For each untouched baseline and source-gated candidate, capture the same ordered completed-bar prefix, forming ticks, date/time zone, underlying/contract identifiers, expiry, OI changes and rules/flags. Compare E2 time/level, E1 time/price, duplicate suppression, target/stop transitions, partial exits, owner/channel routing, write-ahead persistence, restarts and outbox delivery events. Report 0- or 1-signal runs explicitly; “same traces” without nonzero events is inadequate. Do not infer profitability from a synthetic scenario.

**R10H Apply = HOLD.** See [Project Status](PROJECT_STATUS.md) and [Testing](TESTING_RELEASE_GATES.md).
