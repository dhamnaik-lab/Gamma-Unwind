# Live feed, subscriptions and recovery

## November 2026 durable-feed qualification checkpoint

The 244-file actual Runtime lifecycle probe **reproduced** a timestamp regression after rebuilding the old FYERS feed state. A new optional `gammad/broker/fyers/durable_watermark.py` in the 245-file **disposable** candidate passed 10/10 Windows Python 3.13 offline tests for scoped persistence, concurrent claims and synthetic restart. **It is not wired into the default factory**, and the trusted account identity, production ACL, queue/drop throughput, missing-LTT semantics, switch/restart and provider transport are **not live-qualified**. Do not present synthetic watermark PASS as installed feed protection. See [checkpoint](R18_EVIDENCE_AND_R19_HANDOFF.md).

**Installed source:** `gammad/broker/fyers/feed.py`, `gammad/watch/feed.py`, `feed_protocol.py`, `bars.py`, `subscriptions.py`, `reconnect.py`, `warmup.py`, `feed_alarm.py`, `gammad/app.py`.

## Lifecycle

```text
Configured broker + valid authorized token
   → provider adapter / topic mapping
   → WebSocket connection & subscribe/pending sets
   → authenticated frames / canonical ticks
   → instrument state and 5-minute bar assembly
   → ContractEngine (closed-bar E2, tick E1)
   → route / follow-up / persisted application state
```

Each step must be checked; “connected” alone does not mean correct contract identity, valid OI or fresh quotes. The installed FYERS `FyersFeed` has `start`, `add_instruments`, `subscribed_ids`, `pending_ids`, `reload_credentials`, `_map_topics`, `_send_pending`, `_run`, `_ping` and frame emission methods. `ReconnectPolicy`/`FeedHealth` expose reconnect and health concepts, but complete production recovery has not been certified.

## Minimum feed health signals

| Signal | Expected fail-closed condition |
|---|---|
| Connected/authenticated | Stop claiming live readiness when token rejected or session rotated |
| Subscription count | Required contracts must be subscribed; 0 active subscriptions cannot qualify option signals |
| Last tick and bar freshness | Old timestamps must not be treated as a new trading opportunity |
| Correct mapping | Exchange, underlying, CE/PE, strike and expiry must agree with selected contract |
| 5-minute bar boundaries | No incomplete/duplicate candle promoted to a completed bar |
| Restart/resubscribe | Protected open positions and needed underlying topics remain attached, with no duplicate storms |
| Reconnect limits | Maintain bounded retries, rate limits, stop requests and meaningful error status |

## Validated offline, not live

R1.8 accepted tests exercised the **actual** FYERS feed `_run` path with two synthetic socket connections, injected topic mapping, duplicate subscription refusal and retention of protected subscription topics across reconnection. This is bounded proof of local feed code—not live FYERS WebSocket authentication, actual symbol mapping, stale-tick after process restart or actual market-hours behavior.

## Outstanding fault-injection checklist

- Disconnect before the first tick; during partial 5-minute formation; after E2 arming; immediately after E1; and while an exit is pending.
- Stale tick replay with previous-session timestamp, duplicated sequence, out-of-order OI and invalid quote units.
- Token expiry/rotation mid-subscription; broker staged or changed during a reconnect window.
- Network recovers but subscriptions are zero, partly mapped, capped by provider or incorrectly duplicated.
- Restart with persisted trade/exit state; reconnect and resume protected underlyings/contracts before resuming signals.
- Ensure **zero real orders** and no accidental Telegram sends during offline verification.

## Required evidence

Use fake broker clients/sockets first and pin both actual baseline and candidate source. Record deterministic event counts, reconnect attempts, subscribe/unsubscribe sets, bar IDs and trade/outbox state. If live qualification is later separately approved, mask all token/header values and use authorized read-only data only.

**Full stale-feed and process-restart recovery: HOLD.** See [Project Status](PROJECT_STATUS.md).
