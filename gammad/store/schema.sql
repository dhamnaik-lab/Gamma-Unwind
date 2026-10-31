-- GAMMA-UNWIND store, schema version 12. Applied idempotently by gammad/store/db.py::migrate().
-- v12 (0.8.9, D651 / A17): ladder_state persists the OWN / GROUP exit ladders; rollback leaves it harmless.
-- Timestamps: *_ist = ISO-8601 with +05:30 offset; *_ms = epoch milliseconds (D14).
-- Columns added after a table was first created are ALSO listed in db.ADDED_COLUMNS: CREATE TABLE IF NOT
-- EXISTS never alters an existing table, so migrate() ALTERs an older store before this script stamps the
-- version (v3: signals.hold / gate_status / gate_reasons / ctx_json, alerts.tg_message_id; v4: watchlist.tick_size;
-- v5: watchlist.chain_ltp; v6: watchlist.chain_ltp_at / retired_run_id, closed_cycles.bar_ts_ms;
-- v7: signals.scanner / tier; v8: trade_events.fill_px, trades.gate_status; v9: levels.atr_pct and the ten
-- watchlist carry-forward columns; v10: trades.scanner / stop_top / stop_bottom / stop_confirmed_ms / und_entry).
-- v4 (S1.3.0, D80; docs/TRADE-TRACKER-SPEC.md §5): trades + trade_events + ix_trades_state.
-- v5 (0.5.1, T-053): watchlist.chain_ltp = the option-chain LTP the list build selected the strike with.
-- v6 (0.5.2, T-107/T-063): watchlist.chain_ltp_at = IST time the chain that priced the row was fetched (the list
-- post's "(HH:MM)" label; NULL = pre-v6 row -> the run_id's start time); watchlist.retired_run_id = the scheduled
-- 09:20 run that retired a pre-market row whose zone was no longer LIVE (S1.3.1; NULL = live row);
-- closed_cycles.bar_ts_ms = the hidden entry's bar open, so a restart orders it by bar like a signals row.
-- v7 (0.6.0, W15 / D114, PLAN-W15 §2): signals.scanner = the scanner that produced the row ('V1' live; the owner's
-- shadow scanners are recorded under their own name); rows recorded before v7 read 'V1' although S1.2.0 / S1.3.1
-- rules produced them (their spec_version says which). signals.tier = ENTRY | STRONG_WATCHLIST | WATCHLIST for an
-- E1, NULL for an E2 and for pre-v7 rows. scanner_grades / scanner_daily = the accuracy grading of every E1 per
-- scanner x tier (gammad/scan/grader.py); they stay empty in a client store (harmless).
-- v8 (0.7.0, T-161 / D125 F5-F6, PLAN-W16 section 12 H6): trade_events.fill_px = the tick that crossed a stop
-- (SL / TRAIL; the event price and trades.exit_px are the STOP LEVEL, what grading and P&L read); NULL = any other
-- event kind or a pre-v8 row. trades.gate_status = pass | gated, the entry signal's gate verdict carried on the
-- trade row (the owner tracks every E2, gated included); NULL = pre-v8 row.
-- v9 (0.8.1, S1.5.0; docs/research/2026-09-30-workflow-0.8.1/CONTRACTS.md section 2, D159-D161): levels.atr_pct =
-- daily Wilder ATR14 over the 40 newest candles of the run's cut / the last close x 100, PERCENT (2.3 = 2.3 %),
-- 4 decimals, NULL below 15 candles (gammad/jobs/mark_levels.py atr_pct). watchlist gains the same atr_pct (copied
-- from the levels row at build) and the carry-forward columns: origin_run_id (the FIRST listing's run id),
-- first_listed / last_listed (YYYY-MM-DD), carry_k (sessions carried), carry_state (WATCH | BUILD-UP | PRIMED),
-- carry_flags ('broken' or NULL), oi_at_listing (contracts), prem_at_listing (rupees), carry_reason (the why text;
-- retirements: dead | spent | away | deep_itm | expiry | capacity | floor; floor = under the listing OI floor at the
-- 09:20 carry, T-282 (d2), S1.8.0, no schema change). A carried row has tag = 'CARRY'; PK unchanged.
-- Every pre-v9 row reads NULL in all eleven.
-- v10 (0.8.4, T-267 "V2 Scanner", S1.7.0; docs/PLAN-T267.md section 1 "Schema v10", D249): ADDITIVE only (trades is
-- never rebuilt, no PK changes). trades gains scanner (NOT NULL DEFAULT 'V1': every pre-v10 row is a V1 trade; a V2
-- Scanner trade reads 'V2T'), stop_top / stop_bottom / stop_confirmed_ms (the 1h stop zone in force, UNDERLYING
-- rupees; NULL = a V1 trade or a NO_ZONE V2T trade, which never adopts a zone, D251) and und_entry (the underlying's
-- price at entry; NULL = V1 / pre-v10). A V2T trade is stored hold 'swing', t1 = t2 = 0.0 (NOT NULL) and live state
-- Z_OPEN, which 0.8.3's tracker (state IN ('OPEN', 'T1_TRAIL')) never loads, so a rollback cannot fire a T1 at 0.
-- trade_zone_steps = one row per 1h trail step of a V2T trade (trade_events is UNIQUE (trade_id, kind), so it cannot
-- hold a second step). v2_regime = the daily Supertrend regime per (trade_date, underlying) the V2T tier reads;
-- st_dir NULL = unknown = no V2T entry (fails closed). Both new tables stay empty in a store that runs no V2T.
-- app_settings (D255) = per-install switches (the V2 Scanner ON / OFF); empty until a switch is first set.
-- v11 (0.8.9, T-419 "Still Watching", D471 / D472 / D480): watchlist.oi_peak_prior = the max 5-min bar OI of the
-- sessions BEFORE trade_date (a CARRY row's multi-day peak, gammad/jobs/carry.py); trades.target_level = a Still Watching
-- trade's daily target (underlying rupees), target_ms = the close of the 5-min stock bar that first touched it (NULL =
-- not yet), trail_level = the chandelier in force (NULL = not trailing), sw_bar_ms = the open of the last 5-min stock
-- bar it walked (a restart re-walks every completed bar after it). A Still Watching trade is scanner 'SW', hold
-- 'carry' (the hold CHECK admits it from v11: db._widen_trades_hold widens an older table's CHECK in place), states
-- SW_OPEN | SW_TRAILING | CLOSED_SW_STOP | CLOSED_SW_TRAIL | CLOSED_SW_EVE, its 1h stop zone (fixed at entry) in
-- stop_top / stop_bottom / stop_confirmed_ms, und_entry = the stock reference price the zone was picked at.

CREATE TABLE IF NOT EXISTS runs (
    run_id        TEXT PRIMARY KEY,
    kind          TEXT NOT NULL,
    started_ist   TEXT NOT NULL,
    finished_ist  TEXT,
    spec_version  TEXT NOT NULL,
    status        TEXT NOT NULL,
    note          TEXT
);

CREATE TABLE IF NOT EXISTS levels (
    trade_date           TEXT NOT NULL,
    run_id               TEXT NOT NULL,
    symbol               TEXT NOT NULL,
    security_id          INTEGER NOT NULL,
    support_lo           REAL,
    support_hi           REAL,
    resistance_lo        REAL,
    resistance_hi        REAL,
    ref_price            REAL,
    dist_support_pct     REAL,
    dist_resistance_pct  REAL,
    zones_json           TEXT,
    spec_version         TEXT NOT NULL,
    atr_pct              REAL,  -- v9: daily ATR14 (Wilder, 40 candles) / last close x 100, PERCENT; NULL = < 15 / pre-v9
    PRIMARY KEY (trade_date, run_id, symbol)
);

CREATE TABLE IF NOT EXISTS watchlist (
    trade_date              TEXT NOT NULL,
    list_name               TEXT NOT NULL,
    expiry_date             TEXT NOT NULL,
    security_id             INTEGER NOT NULL,
    symbol                  TEXT NOT NULL,
    underlying_security_id  INTEGER NOT NULL,
    strike                  REAL NOT NULL,
    option_type             TEXT NOT NULL,
    tag                     TEXT,
    oi                      INTEGER,
    candidates_n            INTEGER,
    ref_price               REAL,
    zone_edge               REAL,
    run_id                  TEXT,
    spec_version            TEXT NOT NULL,
    lot_size                INTEGER,
    tick_size               REAL,  -- v4 (S1.3.0): the contract's tick in RUPEES (Dhan master TICK_SIZE / 100)
    chain_ltp               REAL,  -- v5 (T-053): option-chain LTP at build time, rupees; NULL = none / built pre-v5
    chain_ltp_at            TEXT,  -- v6 (T-107): ISO IST when that chain was fetched; NULL = pre-v6 (label = run start)
    retired_run_id          TEXT,  -- v6 (T-063, S1.3.1): 09:20 run that retired this pre-market row; NULL = live
    atr_pct                 REAL,     -- v9: the levels row's atr_pct at build, PERCENT; NULL = unknown / pre-v9
    origin_run_id           TEXT,     -- v9: run id of the FIRST listing (a carried row keeps it)
    first_listed            TEXT,     -- v9: YYYY-MM-DD of the first listing
    last_listed             TEXT,     -- v9: YYYY-MM-DD of the latest (re-)listing; a re-list resets the clock
    carry_k                 INTEGER,  -- v9: sessions carried since last_listed; NULL = not carried
    carry_state             TEXT,     -- v9: WATCH | BUILD-UP | PRIMED; NULL = not carried
    carry_flags             TEXT,     -- v9: 'broken' or NULL
    oi_at_listing           REAL,     -- v9: OI (contracts) at the first listing
    prem_at_listing         REAL,     -- v9: premium (rupees) at the first listing
    carry_reason            TEXT,     -- v9: why text; retired dead | spent | away | deep_itm | expiry | capacity | floor
    oi_peak_prior           REAL,     -- v11: OI peak of the sessions before trade_date; NULL = unknown / pre-v11
    PRIMARY KEY (trade_date, list_name, expiry_date, security_id)
);

CREATE TABLE IF NOT EXISTS oi_series (
    ts_ms        INTEGER NOT NULL,
    security_id  INTEGER NOT NULL,
    ltp          REAL,
    volume       INTEGER,
    oi           INTEGER,
    PRIMARY KEY (ts_ms, security_id)
);

CREATE TABLE IF NOT EXISTS signals (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_date     TEXT NOT NULL,
    ts_ms          INTEGER NOT NULL,
    security_id    INTEGER NOT NULL,
    list_name      TEXT NOT NULL,
    expiry_date    TEXT NOT NULL,
    kind           TEXT NOT NULL CHECK (kind IN ('E1', 'E2')),
    strong         INTEGER NOT NULL DEFAULT 0,
    ltp            REAL,
    oi_drop_pct    REAL,
    vol_ratio      REAL,
    zone_dist_pct  REAL,
    sl             REAL,
    latency_ms     INTEGER,
    payload_json   TEXT,
    spec_version   TEXT NOT NULL,
    hold           TEXT,  -- v3 (D58): swing | intraday
    gate_status    TEXT,  -- v3 (D63): pass | gated; NULL = recorded before v3
    gate_reasons   TEXT,  -- v3 (D63): failed gates, comma-joined ("zone,liquidity"); '' = none
    ctx_json       TEXT,  -- v3: SignalContext.to_dict() as JSON
    scanner        TEXT NOT NULL DEFAULT 'V1',  -- v7: producing scanner; pre-v7 rows read 'V1'
    tier           TEXT   -- v7: ENTRY | STRONG_WATCHLIST | WATCHLIST (E1); NULL = E2 / pre-v7
);

CREATE TABLE IF NOT EXISTS alerts (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    signal_id      INTEGER,
    channel        TEXT NOT NULL,
    sent_ts_ms     INTEGER,
    status         TEXT NOT NULL,  -- sent | failed | muted | gated | paused
    error          TEXT,
    tg_message_id  INTEGER         -- v3 (D65): Telegram message_id of a sent message (E1 replies to its E2)
);

CREATE TABLE IF NOT EXISTS token_events (
    id      INTEGER PRIMARY KEY AUTOINCREMENT,
    ts_ist  TEXT NOT NULL,
    kind    TEXT NOT NULL,
    detail  TEXT
);

-- v2 (D42-D45): per-contract mute and the log of manual / auto (non-signal) Telegram sends.
CREATE TABLE IF NOT EXISTS mutes (
    security_id  INTEGER PRIMARY KEY,
    symbol       TEXT,
    muted_ist    TEXT NOT NULL,
    reason       TEXT
);

CREATE TABLE IF NOT EXISTS manual_sends (
    id      INTEGER PRIMARY KEY AUTOINCREMENT,
    ts_ist  TEXT NOT NULL,
    kind    TEXT NOT NULL,
    ref     TEXT,
    dest    TEXT NOT NULL,
    status  TEXT NOT NULL,
    error   TEXT
);

-- v4 (S1.3.0, D80): one row per tracked entry (gammad/trade/tracker.py); levels fixed at entry (TRACKER §2).
CREATE TABLE IF NOT EXISTS trades (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    entry_signal_id         INTEGER NOT NULL UNIQUE,
    kind                    TEXT NOT NULL CHECK (kind IN ('E1', 'E2')),
    hold                    TEXT NOT NULL CHECK (hold IN ('intraday', 'swing', 'carry')),
    entry_date              TEXT NOT NULL,
    entry_ts_ms             INTEGER NOT NULL,
    security_id             INTEGER NOT NULL,
    underlying_security_id  INTEGER,
    list_name               TEXT NOT NULL,
    expiry_date             TEXT NOT NULL,
    option_type             TEXT NOT NULL,
    lot_size                INTEGER,
    tick_size               REAL,
    entry                   REAL NOT NULL,
    sl                      REAL,
    t1                      REAL NOT NULL,
    t2                      REAL NOT NULL,
    zone_edge               REAL,
    armed                   INTEGER NOT NULL DEFAULT 0,
    armed_date              TEXT,
    last_close_date         TEXT,
    last_close_px           REAL,
    -- OPEN|T1_TRAIL|CLOSED_T2|CLOSED_TRAIL|CLOSED_SL|CLOSED_EOD|CLOSED_EOD_MISSED|CLOSED_STOCK_STOP|CLOSED_DAY5|CLOSED_EXPIRY
    -- v10 (V2T): Z_OPEN|CLOSED_ZSL|CLOSED_ZTRAIL|CLOSED_DAY5|CLOSED_EXPIRY
    -- v11 (SW): SW_OPEN|SW_TRAILING|CLOSED_SW_STOP|CLOSED_SW_TRAIL|CLOSED_SW_EVE
    -- v11 (V1R, CX-120): V1R_OPEN|V1R_TRAILING|V1R_HALF|CLOSED_V1R_<REASON>
    state                   TEXT NOT NULL,
    exit_px                 REAL,
    exit_ts_ms              INTEGER,
    entry_msg_id            INTEGER,
    spec_version            TEXT NOT NULL,
    updated_ms              INTEGER NOT NULL,
    gate_status             TEXT,  -- v8 (D125 F5): pass | gated; NULL = recorded before v8
    scanner                 TEXT NOT NULL DEFAULT 'V1',  -- v10: V1 | V2T; pre-v10 rows read 'V1'
    stop_top                REAL,     -- v10: V2T 1h stop zone top (underlying rupees); NULL = V1 / NO_ZONE
    stop_bottom             REAL,     -- v10: V2T 1h stop zone bottom (underlying rupees); NULL = V1 / NO_ZONE
    stop_confirmed_ms       INTEGER,  -- v10: epoch ms the stop zone in force was confirmed; NULL = V1 / NO_ZONE
    und_entry               REAL,     -- v10: underlying price at entry (rupees); NULL = V1 / pre-v10
    target_level            REAL,     -- v11: SW daily target (underlying rupees); NULL = none / not SW
                                      -- v11: NULL on a V1R row (F121-2)
    target_ms               INTEGER,  -- v11: SW first target touch (that 5-min bar's close, epoch ms); NULL = not yet
    trail_level             REAL,     -- v11: SW chandelier in force (underlying rupees); NULL = not trailing
                                      -- v11: NULL on a V1R row (F121-2)
    sw_bar_ms               INTEGER   -- v11: SW open of the last 5-min stock bar walked (restart re-walk); NULL = none
                                      -- v11: also the V1R re-walk cursor (CX-122)
);

-- v4: one row per follow-up, written 'pending' in the SAME transaction as its transition, BEFORE the send
-- (TRACKER §6). UNIQUE (trade_id, kind): a crash can at worst duplicate one message, never fire a kind twice.
CREATE TABLE IF NOT EXISTS trade_events (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_id       INTEGER NOT NULL,
    kind           TEXT NOT NULL,  -- T1|T2|TRAIL|SL|EOD|EOD_MISSED|SWING_ARMED|SWING_STOP|DAY5|EXPIRY; v10: ZSL|ZTRAIL;
                                   -- v11 (SW): SW_STOP|SW_TARGET (not an exit)|SW_TRAIL|SW_EVE
    ts_ms          INTEGER NOT NULL,
    price          REAL,
    und_close      REAL,
    status         TEXT NOT NULL,  -- pending|sent|failed|muted|paused|no_entry_msg|suppressed
    tg_message_id  INTEGER,
    reply_to       INTEGER,
    error          TEXT,
    fill_px        REAL,  -- v8 (D125 F6): SL / TRAIL crossing tick (price = the stop level); NULL otherwise / pre-v8
    UNIQUE (trade_id, kind)
);

-- v4 (D98(4), added before 0.5.0 shipped, so still v4: IF NOT EXISTS lands it on a store migrated to 4 already):
-- one row per arming cycle closed by an entry that left NO signals row (a gated CLIENT entry, hidden, D87), so a
-- restart's silent bar replay keeps that cycle closed (Runtime._close_cycle_already_entered). oi_now = the re-arm
-- reference. Holds no kind and no gate detail.
CREATE TABLE IF NOT EXISTS closed_cycles (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_date   TEXT NOT NULL,
    security_id  INTEGER NOT NULL,
    ts_ms        INTEGER NOT NULL,
    oi_now       REAL,
    bar_ts_ms    INTEGER  -- v6 (T-107): the entry's bar open; NULL = pre-v6 row (ordered by ts_ms)
);

-- v7 (W15, PLAN-W15 §6): one grade per E1 signal of every scanner, graded on live ticks: +20 % (t1) before the
-- signal's SL. status: open | t1 | sl | expiry | day5. Prices in rupees; *_pct in percent units.
CREATE TABLE IF NOT EXISTS scanner_grades (
    signal_id        INTEGER PRIMARY KEY,
    scanner          TEXT NOT NULL,
    tier             TEXT,
    trade_date       TEXT NOT NULL,
    security_id      INTEGER NOT NULL,
    symbol           TEXT,
    entry            REAL,
    sl               REAL,
    t1               REAL,
    mfe_pct          REAL,
    mae_pct          REAL,
    last_ltp         REAL,
    status           TEXT NOT NULL,
    first_hit_ts_ms  INTEGER,
    closed_ts_ms     INTEGER,
    spec_version     TEXT
);

-- v7: the day-end summary per (trade_date, scanner, tier), upserted at 15:35 until every grade of the day closes.
CREATE TABLE IF NOT EXISTS scanner_daily (
    trade_date      TEXT NOT NULL,
    scanner         TEXT NOT NULL,
    tier            TEXT NOT NULL,
    n               INTEGER NOT NULL,
    t1_hits         INTEGER NOT NULL,
    sl_first        INTEGER NOT NULL,
    open_n          INTEGER NOT NULL,
    avg_mfe_pct     REAL,
    avg_mae_pct     REAL,
    computed_ts_ms  INTEGER NOT NULL,
    PRIMARY KEY (trade_date, scanner, tier)
);

-- v10 (T-267): one row per 1h trail step of a V2T trade (TRADE-TRACKER-SPEC section 12). seq = 1, 2, ... per trade;
-- confirmed_ms = the 1h close that confirmed the new zone; applied_ms = the stock 5-min close it took effect from
-- (NULL = not applied yet); top / bottom / h1_close in underlying rupees. status / tg_message_id / reply_to / error =
-- the delivery of its (owner-only, silent) ZSTEP card, the trade_events vocabulary (pending|sent|failed|muted|...).
CREATE TABLE IF NOT EXISTS trade_zone_steps (
    trade_id       INTEGER NOT NULL,
    seq            INTEGER NOT NULL,
    confirmed_ms   INTEGER NOT NULL,
    applied_ms     INTEGER,
    top            REAL NOT NULL,
    bottom         REAL NOT NULL,
    h1_close       REAL,
    status         TEXT NOT NULL,
    tg_message_id  INTEGER,
    reply_to       INTEGER,
    error          TEXT,
    UNIQUE (trade_id, seq)
);

-- v10 (T-267): the daily Supertrend regime of each underlying for a trade_date, computed at the 09:20 build from
-- daily bars dated BEFORE trade_date (gammad/v2scan/regime.py). st_dir +1 | -1; NULL = unknown -> no V2T entry
-- (fails closed); st_value = the Supertrend line (rupees); asof_date = the last daily bar used; bars_n = how many
-- bars fed it; reason = why st_dir is NULL (or a note); computed_ms = when it was written.
CREATE TABLE IF NOT EXISTS v2_regime (
    trade_date              TEXT NOT NULL,
    underlying_security_id  INTEGER NOT NULL,
    symbol                  TEXT,
    st_dir                  INTEGER,
    st_value                REAL,
    asof_date               TEXT,
    bars_n                  INTEGER,
    reason                  TEXT,
    computed_ms             INTEGER NOT NULL,
    PRIMARY KEY (trade_date, underlying_security_id)
);

-- v10 (D255): app_settings = this install's switches, one row per key (db.get_v2_enabled / set_v2_enabled). Key
-- 'v2_enabled' = the V2 Scanner switch, value '1' (ON) | '0' (OFF); NO row = the edition's default (client OFF, owner
-- ON). updated_ms = when it was last set; source = who set it ('dashboard', a tool name). Lives in the store, so it
-- survives restarts and updates; an older build (rollback, user_version 9) never reads it.
CREATE TABLE IF NOT EXISTS app_settings (
    key         TEXT PRIMARY KEY,
    value       TEXT NOT NULL,
    updated_ms  INTEGER NOT NULL,
    source      TEXT
);

CREATE INDEX IF NOT EXISTS ix_levels_date ON levels (trade_date);
CREATE INDEX IF NOT EXISTS ix_closed_cycles ON closed_cycles (trade_date, security_id);
CREATE INDEX IF NOT EXISTS ix_signals_date ON signals (trade_date, security_id);
CREATE INDEX IF NOT EXISTS ix_oi_series_sec ON oi_series (security_id, ts_ms);
CREATE INDEX IF NOT EXISTS ix_alerts_signal ON alerts (signal_id);
CREATE INDEX IF NOT EXISTS ix_trades_state ON trades (state, hold);

CREATE INDEX IF NOT EXISTS ix_scanner_grades_status ON scanner_grades (status);

CREATE TABLE IF NOT EXISTS ladder_state (                                   -- v12 (0.8.9, D628 (17), A17)
    position_key  TEXT NOT NULL,                                            -- v12: EntrySignal.key
    book          TEXT NOT NULL,                                            -- v12: OWN | GROUP (no default)
    scanner       TEXT NOT NULL,                                            -- v12: V1N | V2SQ (no default, CX-203 F2)
    entry_date    TEXT NOT NULL,                                            -- v12: YYYY-MM-DD
    closed        INTEGER NOT NULL DEFAULT 0 CHECK (closed IN (0, 1)),      -- v12
    state_json    TEXT NOT NULL,                                            -- v12: PositionState.to_dict() JSON
    events_json   TEXT NOT NULL DEFAULT '[]',                               -- v12: [{event: ExitEvent.to_dict(), realized_rs, day}]; day = the fill day YYYY-MM-DD (D732 (1)); absent on pre-0.8.9 rows -> the event's date
    updated_ms    INTEGER NOT NULL,                                         -- v12
    PRIMARY KEY (position_key, book)                                        -- v12
);                                                                          -- v12
CREATE INDEX IF NOT EXISTS ix_ladder_open ON ladder_state (closed, scanner); -- v12

PRAGMA user_version = 12;
