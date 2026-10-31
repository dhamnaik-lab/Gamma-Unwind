# Paper trading and signal monitoring — no orders

## Product contract

GAMMA-UNWIND 0.8.9 is an informational option scanner with trade-like tracking and Telegram follow-ups. Its setup guide explicitly states that the application **does not place, modify or close broker orders**. The project goal includes observing live paper signals without executing orders; this requires verified feed data and accurate shadow-state tracking, not an order API.

## What a paper trade must contain

Each proposed trade must record: event ID and source revision, broker and instrument identity, underlying, CE/PE, strike/expiry, signal kind, event timestamp/time zone, premium and tick size, quantity/lot assumption, initial stop, targets, charges/slippage assumptions, data provenance, account/strategy namespace, and explicit *paper-only* status. Subsequent events must preserve original immutable signal identity.

Separate these three quantities:

- **Signal:** an E1/other alert issued from the rule engine. A signal is not an executed trade.
- **Paper position:** an assumed fill and subsequent mark-to-market/exit tracked by application rules. P&L is an estimate.
- **Actual broker position:** a real order/holding in a broker account. GAMMA acceptance explicitly must not create these.

## Paper lifecycle

1. Validate the signal's option identity and source, market session and data freshness.
2. Apply deterministic paper-fill assumptions and preserve them for replay. Never use future candle high/low to decide an entry fill.
3. Maintain stops, partial targets, trailing logic, expiry/session exits and duplicate refusal according to the **specific V1 version** under test.
4. Track expected commission, exchange charges, spreads and slippage separately from gross paper P&L; do not present backtest P&L as realized brokerage profit.
5. Persist event/order-like intent states to a **separate synthetic journal** and test crash/restart idempotency before labeling durable outbox behavior PASS.
6. Report unmatched/uncertain ticks and missing candles as incomplete, not as profitable/closed trades.

## Daily evidence to retain

- Signal count and reasons (including zero-signal days), enter/exit event times and source revision.
- Feed connection/subscription and last-tick freshness; false and delayed ticks.
- Positions open/closed, blocked or pending; pending alerts/outbox and restart retries.
- Gross/net paper P&L, costs, drawdowns and win/loss by day/week/month when data quality supports it.
- Accepted source/evidence hash, replay settings and provider identity. Mask all tokens and actual account IDs.

## Qualification remains incomplete

Prior R1.8 bounded tests passed a natural synthetic E2→E1 and seven position states but did **not** qualify continuous persisted trade-router/outbox, full feed recovery or genuine FYERS historic options data. Real intraday paper monitoring should not be claimed certified until live **read-only** feeds are authorized and each correctness gate is demonstrated.

**Zero real orders** is a hard invariant. This document does not introduce a broker order interface, trading permission, or investment performance promise.
