"""Trade follow-ups (spec S1.3.0 (ruled, being built), D80; docs/TRADE-TRACKER-SPEC.md).

rules.py   the PURE state machine (levels, ticks, daily closes, 15:20) -- no I/O, no DB, no Telegram.
tracker.py (W12 B2) persists it as `trades` / `trade_events` rows and routes the events.
"""
