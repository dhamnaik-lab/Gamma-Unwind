"""Local dashboard API: FastAPI on 127.0.0.1:<GAMMA_PORT>, served by uvicorn inside the supervisor's loop.

CONTRACT (the dashboard codes against exactly this; every shape is pinned by tests/test_api.py):
  GET  /api/status                      -> {version, spec_version, now_ist, market_open,
                                            feed:{connected,n_subscribed,last_tick_ms},
                                            token:{valid,validity_ist,hours_left,level}, alerts_paused,
                                            jobs:[{name,next_run_ist,last_run_ist,last_status}],
                                            latency:{p50_ms,p95_ms,last_ms}}
  GET  /api/levels?date=YYYY-MM-DD      -> {date, run_id, rows:[levels row]}
  GET  /api/watchlists?date=            -> {date, lists:[{list_name, expiry_date,
                                            rows:[watchlist row + ltp, oi_now, oi_drop_pct, engine_state]}]}
  GET  /api/signals?date=&limit=200     -> {rows:[signals row + payload (parsed payload_json)
                                            + hold ("swing"|"intraday"|null), gate_status ("pass"|"gated"|null =
                                            recorded before v3), gate_reasons (list), ctx (SignalContext dict
                                            or null)]}
  GET  /api/contract/{security_id}?tf=5 -> {bars:[{ts_ms,open,high,low,close,volume,oi}], oi_series:[[ts_ms,oi]],
                                            engine:{state,a,b,c,oi_drop_pct,vol_ratio,consol_lo,consol_hi,consol_mid}}
  GET  /api/token                       -> status.token + {events:[last 20 token_events]}
  POST /api/token {access_token}        -> runtime.token_updater(access_token); 501 when none is registered;
                                            answers {ok, token:<status.token>}
  POST /api/jobs/{mark_levels|build_watchlists}/run {kind?} -> {run_id}
  POST /api/alerts/pause {paused}       -> the /api/status body
  GET  /api/logs?lines=200              -> {lines:[...]} (tail of logs/gammad.log, secrets redacted)
  POST /api/jobs/build_watchlists/run {kind?, post?} -> post=true also posts the new rows to the GROUP (D45)
  POST /api/send {kind, ref, dest}      -> {ok, message_preview, parts, error}   (D42/D43)
         kind "watchlist_row"  ref {security_id, list_name?, expiry_date?, date?}
              "watchlist_list" ref {list_name, expiry_date, date?}
              "signal"         ref {id}
              "signals_table"  ref {date?}
         dest "group" (default) | "personal"; 422 bad kind/ref/dest, 404 nothing to send
  GET  /api/preview?kind=&ref=<JSON>&dest= -> {html, parts:[html...], dest} (the exact Telegram card(s); nothing is
                                            sent; dest 0.8.3, below)
  POST /api/mute {security_id, muted, reason?} -> {ok, muted:[security_id...]}
  GET  /api/mutes                       -> {rows:[{security_id, symbol, muted_ist, reason}]}
  GET  /api/export?what=watchlist|signals&format=tsv|line&date=&list_name=&expiry_date=&id=
                                        -> text/plain (D44): tsv = header + one line per row;
                                           line = one line (id = security_id / signal id), else one per row
  WS   /ws                              -> {type:"status"|"tick"|"bar"|"signal"|"token"|"log"|"mute"|
                                            "manual_send", data}
                                            (status on connect and every 2 s; ticks <= 4/s per contract,
                                            throttled by the runtime; every signal, with data.muted,
                                            data.ctx, data.hold, data.gate_status, data.gate_reasons (list);
                                            mute data {security_id, muted}; manual_send data
                                            {kind, ref, dest, ok, parts, error})
  GET  /api/watchlists rows also carry muted: bool. 0.8.1 (D162(5)): the owner rows carry the v9 columns (atr_pct +
                                            the carry fields) and tag-CARRY rows; the client edition gets neither
                                            (CLIENT_HIDDEN_WATCH_COLUMNS). /api/levels keeps atr_pct in both editions.
                                            0.8.9 T-419 (D471 Q8): the client gets the carried rows as the list
                                            "Still Watching" (no tag, no owner column) while still_watching is on.
  GET  /, /<path>                       -> dashboard/dist (SPA fallback to index.html), or a plain
                                            "dashboard not built" page.
No token value is ever returned: token_events details and log lines pass through `redact()`.

W12 (S1.3.0 / app 0.5.0) additions:
  /api/status (and every WS status) also carries  edition ("owner"|"client") and
                                            broker (registry.describe: name, label, status, beta, token_kinds,
                                            token_key, token_expiry_rule, capabilities, scanner = assess()) or null
                                            and telegram (T-062 cfg.telegram.to_dict(): ok, edition, group_ok,
                                            personal_ok, personal_configured, problems[]; absent only for a
                                            runtime without settings)
  GET  /api/trades?date=&state=open|all -> {date, state, rows:[trades row + ltp, pnl_pct, events:[trade_events]]}
                                            open = OPEN/T1_TRAIL of any entry date; all (default) = entered on
                                            `date`, closed on `date`, or still open (TRADE-TRACKER-SPEC §8)
  GET  /api/token                       -> also broker {name,label,status,beta} | null, token_kinds, token_key,
                                            token_masked, token_expiry_rule, client_login {fields, login_url} | null,
                                            chats {group, personal (both masked), verdict, ok} | null
  POST /api/token/login {fields:{..}}   -> entry.client_login.login -> D32 (D92, D95(5)); both editions.
                                            Header X-Gamma-Control: 1 (403 without) + Content-Type
                                            application/json (415 otherwise) required (D98(3)).
                                            404 no client_login, 400 fields mismatch / rejected, 501 no hook.
                                            The typed/pasted values are never logged, echoed or written.
  POST /api/service/stop                -> header X-Gamma-Control: 1 required (403 otherwise; D95(1));
                                            writes <data>/STOP (tray supervisor protocol) -> 202 {stopping: true}
  WS   /ws                              -> {type:"trade", data} forwarded like every hub message
CLIENT edition (CLIENT-PACKAGE-SPEC §2, D86, D87) -- server-side, defence in depth on top of the router:
  signals / export / preview / send / trades / WS never carry an E2 or a gated E1 (404 on such a ref);
  engine_state is projected (IDLE/ARMED -> WATCHING, DONE -> ENTERED only when that contract's LATEST entry
  today is a visible E1 -- no later hidden gated E1 closed another setup -- else WATCHING, D98(5)); contract engine a/b/c + consol_* are null until ENTERED; /api/logs and WS log
  pushes drop lines matching (?i)\\bE2\\b|ARMED (D98(6)); /api/status and every WS status carry no latency and
  no jobs (X8).
  D98: /api/status.broker also carries budget_status (Nuvama daily API budget line; null = OK / other brokers);
  every route answers 400 to a Host other than 127.0.0.1 / localhost (TrustedHostMiddleware, DNS rebinding).
  The owner edition's responses are unchanged apart from the three new /api/status keys and /api/token.chats.

W15 (app 0.6.0 / S1.4.0 / schema 7, D114, PLAN-W15 §2):
  signals / export / preview / send / trades / WS carry scanner 'V1' rows only, in BOTH editions (the owner's shadow
  scanners are read through /api/scanner/* alone); every signal row carries `tier` ('ENTRY' | 'STRONG_WATCHLIST' |
  'WATCHLIST'; V1 E2s carry a tier too since 0.8.5 (D287 (2)); null = no tier stored) and `scanner`; trade rows carry
  their entry signal's tier + scanner; the signals
  export gains a `tier` column (owner TSV / scanner CSV since CX-86: the D273 display word, then `tier_code` = the
  stored code). CLIENT: the D87 "hide gated" filter is GONE -- a client sees every V1 E1 with its
  tier (a gated E1 is a WATCHLIST / STRONG_WATCHLIST row); E2 stays hidden; a WS `scanner` push never reaches it.
  Owner only (404 in the client edition; gammad/scan is not in the client ZIP and is imported after the check):
  GET  /api/scanner/today               -> {date, summary:[scanner_daily-shaped rows computed live for today],
                                            signals:[as /api/scanner/signals for today]}
  GET  /api/scanner/daily?days=20       -> {days, rows:[scanner_daily rows of the last `days` trade dates]}
                                            (trade_date, scanner, tier, n, t1_hits, sl_first, open_n, avg_mfe_pct,
                                            avg_mae_pct, computed_ts_ms) + by_list / by_hour: [{scanner, key, n,
                                            t1_hits, sl_first, open_n, hit_rate_pct}] (T-156, review.hit_rates;
                                            0.8.3 C.6: a by_list row is one list + expiry and also names expiry_date)
  GET  /api/scanner/weekly?weeks=8      -> {weeks, rows:[{week_start, week_end, rows:[scanner x tier + hit_rate_pct],
                                            best:{scanner: signal}, worst:{scanner: signal}}]} newest first, weeks
                                            with no graded signal omitted
  GET  /api/scanner/signals?date=       -> {date, rows:[every E1 of every scanner on `date`: signals row (+ payload,
                                            symbol, tier, scanner) + its scanner_grades fields (entry, sl, t1, mfe_pct,
                                            mae_pct, last_ltp, status, first_hit_ts_ms, closed_ts_ms; null if ungraded)
                                            + trigger:{review.TRIGGER_FIELDS} (T-156, D127)]}
                                            ?format=csv -> the same rows as text/csv, trigger fields as columns, tier word + tier_code

W16 T-129 (PLAN-W16 13.3 B1, D119 G2-4, D122, D133; gammad/api/lan.py holds the rules), both editions:
  GET  /api/session                     -> {remote, authed, lan}
  POST /api/session/login {password}    -> 200 {ok} + the HttpOnly SameSite=Strict session cookie (30 days); 401 wrong,
                                            429 the 6th attempt in a minute per address, 415 not JSON, 403 no password set
  POST /api/session/logout              -> 200 {ok}, the cookie cleared
  A REMOTE request (non-loopback peer, or any X-Forwarded-For / X-Forwarded-Host / Forwarded / X-Real-IP header): 403
  page when GAMMA_LAN_PASSWORD is unset; else /api/token*, /auth/*, POST /api/service/stop 404, a non-GET whose Origin is
  not its Host 403, and without the cookie /api/* 401 and /ws close 4401 (the SPA shell still loads). Loopback without a
  forwarded header is unchanged. TrustedHost accepts this PC's private address only while the LAN bind is on.
  make_server(app, port, *, hosts=lan.bind_hosts(env)) binds one socket per address, loopback or RFC1918 only.

W17 (PLAN-W17 §2), both editions:
  GET  /api/trades rows also carry symbol, strike (gammad.trade.identity.resolve_contract: the entry-date watchlist
                                            row, else the newest row of the same expiry + option type, else the entry
                                            signal's payload; null rather than another contract's name). T-170
  GET  /api/contract/{id}               -> also contract {symbol, strike, option_type, expiry_date, list_name} | null
                                            (newest watchlist row dated <= `date`) and bars_source "live" | "recorded"
                                            | "none": without live bars (or for a date other than today) the bars are
                                            replayed from the recorded ticks (gammad.api.recorded_bars). T-170
  GET  /api/status (+ every WS status)  -> also build {commit, bundle {version, commit, built_ist} | null}
                                            (gammad.api.build_info; commit resolved once in create_app). T-171

0.8.2 (DESIGN-REVIEW C.9), owner only:
  GET  /api/scanner/signals rows        -> also fut {flag, cls, p5, oc5, agree_build, verdict} | null (gammad.scan.api
                                            fut_columns; null without a row or without the owner tables)
  owner extension routes                -> _owner_ext_routes: registered from gammad.scan in the OWNER edition only
                                            (ImportError-guarded; never imported by the client edition), each 404 when
                                            the edition reads client; shapes in that module. Their WS push type is in
                                            OWNER_WS_TYPES (the client WS drops it).

0.8.3 (CONTRACTS C.5, W0-5; B8):
  GET  /api/preview?kind=&ref=&dest=group|personal -> {html, parts, dest}; dest defaults to group, any other value 422
                                            before the runtime is called; runtime.preview(kind, ref, dest=dest) BY
                                            KEYWORD (a GROUP preview of a row the group refuses answers 422, like the
                                            send). Both editions; the client's hidden-ref 404 is unchanged.
  owner rows of /api/signals, /api/scanner/signals (+ /api/scanner/today signals) and /api/watchlists
                                         -> also group_block: str | null = the router's GROUP_BLOCK_TEXT sentence for
                                            router.group_refusal_reason's code (the SAME function the send path refuses
                                            with), null when the group may carry the row. A client row never carries
                                            the key.

0.8.4 (T-267 "V2 Scanner", S1.7.0; PLAN-T267 section 1 + M9 / M11 / M16, D241(2), D248, D249, D255), both editions:
  signals / export / preview / send / trades / WS carry the LIVE scanners' rows = LIVE_SCANNERS ("V1", "V2T"; read
                                            from the registry): V1 and the V2 Scanner. V1 B, V2 (= "V2-S131") and
                                            V1 PLUS stay on /api/scanner/* alone (the W15 rule above, widened by V2T).
  GET  /api/trades rows                 -> also scanner ("V1" | "V2T"), stop {top, bottom, confirmed_ms} | null (the
                                            1h stop zone in force, underlying rupees; null = V1 / NO_ZONE), steps
                                            [{trade_id, seq, confirmed_ms, applied_ms, top, bottom, h1_close, status,
                                            tg_message_id, reply_to, error}] in seq order ([] for V1), und_entry |
                                            null; the same four keys as the tracker's WS "trade" push. state=open =
                                            rules.OPEN_STATES (OPEN, T1_TRAIL, Z_OPEN).
  GET  /api/status (+ every WS status)  -> CLIENT: no spec_version (D241(2), M9); the owner keeps it. Owner: v2_enabled.
  /api/trades, /api/signals, /api/levels, /api/watchlists (+ every WS message) -> CLIENT: no spec_version key at any
                                            depth (D241(2), D256; client_rows; tests/test_client_no_spec_version.py);
                                            the owner keeps it.
  GET  /api/v2-scanner                  -> {enabled, default}: the V2 Scanner switch (D255; default client false,
                                            owner true; stored in app_settings, db.get_v2_enabled)
  POST /api/v2-scanner {enabled: bool}  -> {enabled, default}; 422 a non-bool; the change is logged and pushed as
                                            WS {type:"v2_scanner", data:{enabled, default}}

0.8.5 (D286(4), D287, PLAN-D287 I7 / I8 / AM7): the V2 Scanner is OWNER only. The routes read `live_scanners()` at CALL
time (owner ("V1", "V2T"), client ("V1",): a 0.8.4 client store's V2T rows and trades never show); GET / POST
/api/v2-scanner answer 404 in the client (`_owner_only`); the client status carries no v2_enabled (CLIENT_STATUS_DROP)
and its WS drops a "v2_scanner" push (OWNER_WS_TYPES). The client's ENTERED projection: ARMED or DONE + a visible entry
(its entry is the E2 bar close, stored as E1). An owner TIERED E2 has its WHY NOT, and /api/scanner/signals lists the
V1 tiered E2s beside the E1s (D288 Q6).

0.8.8 (T-376; D429 final (a) / D433 / D87): the CLIENT /api/export reads the page's rows (query_signals /
query_watchlists) in the client's words (`client_export_text`): a line = the dashboard's textfmt.ts client line, a table =
templates.CLIENT_SIGNAL_EXPORT_COLUMNS / CLIENT_WATCHLIST_EXPORT_COLUMNS (no kind, latency, id or ltp column). The owner
export is unchanged.
"""

from __future__ import annotations

import asyncio
import contextlib
import inspect
import json
import re
import sqlite3
import threading
import time
from collections import deque
from collections.abc import Callable, Generator, Mapping
from datetime import date, datetime, timedelta
from datetime import time as dtime
from pathlib import Path
from typing import Annotated, Any, Protocol

from fastapi import Body, Depends, FastAPI, HTTPException, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.exceptions import RequestValidationError
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, PlainTextResponse

from gammad import REPO_ROOT
from gammad import edition as _edition
from gammad.api import build_info, s089
from gammad.api.recorded_bars import replay_bars
from gammad.api.session_hours import session_hours_body
from gammad.config import IST
from gammad.log import get_logger
from gammad.scanners import contract as _sc
from gammad.trade import rules as _rules
from gammad.trade.identity import resolve_contract
from gammad.watch import engine_pool as _engine_pool

log = get_logger(__name__)

try:  # W5 token watcher; the API must still come up without it
    from gammad.auth.token_watch import TokenRejected
except ImportError:  # pragma: no cover - only when gammad/auth is absent

    class TokenRejected(Exception):  # type: ignore[no-redef]
        """Stand-in so the except clause below stays valid."""


DEFAULT_DIST = REPO_ROOT / "dashboard" / "dist"
STATUS_EVERY_S = 2.0
HUB_QUEUE_MAX = 2000
LOG_LINES_MAX = 2000
SIGNALS_LIMIT_MAX = 5000
JOB_NAMES = ("mark_levels", "build_watchlists")
SEND_DESTS = ("group", "personal")  # 0.8.3 C.5: GET /api/preview `dest` (router.DESTS; the route answers 422 otherwise)

_SECRET_PATTERNS = (
    re.compile(r"eyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}"),  # JWT (Dhan access token)
    re.compile(r"(?i)(token=|access[_-]?token[\"'=: ]+)[^&\s\"',]+"),
    re.compile(r"bot\d+:[A-Za-z0-9_-]+"),  # Telegram bot token in a URL
    # T-283 defence in depth: a lone opaque run of 40+ chars holding a letter AND a digit (a pasted broker key /
    # token). Not a path piece (no `/ \ . : -` neighbour) and never an all-letter identifier.
    re.compile(r"(?<![\w./\\:-])(?=[A-Za-z_-]*\d)(?=[0-9_-]*[A-Za-z])[A-Za-z0-9_-]{40,}(?![\w./\\:-])"),
)
# T-283: the 0.8.3-and-earlier `.env changed: keys [...]` line named EVERY changed key, and python-dotenv reads a bare
# pasted token line as a key. Clients update in place (logs/ kept), so the Logs page rewrites that list as config's
# allow-list summary: env_keys() names only, the rest counted. Greedy to the line's last `]` (a key may hold one).
_ENV_CHANGED_OLD = re.compile(r"(\.env changed: )keys \[(.*)\]")


def _env_changed_summary(m: re.Match[str]) -> str:
    import ast

    from gammad.config import safe_key_names

    try:
        keys = ast.literal_eval(f"[{m.group(2)}]")
    except (ValueError, SyntaxError, TypeError, MemoryError, RecursionError):
        keys = None
    if not isinstance(keys, list):
        return f"{m.group(1)}<redacted>"
    return m.group(1) + safe_key_names(keys)

NOT_BUILT_HTML = (
    "<!doctype html><html><head><meta charset='utf-8'><title>GAMMA-UNWIND</title></head>"
    "<body style='font-family:sans-serif;background:#111;color:#ddd;padding:2em'>"
    "<h1>GAMMA-UNWIND</h1><p>dashboard not built</p>"
    "<p>Build it with <code>npm run build</code> in <code>dashboard/</code>; the API is live at "
    "<a style='color:#8cf' href='/api/status'>/api/status</a>.</p></body></html>"
)
OAUTH_LANDING_HTML = (
    "<!doctype html><html><head><meta charset='utf-8'><title>GAMMA-UNWIND - broker login finished</title></head>"
    "<body style='font-family:sans-serif;background:#111;color:#ddd;padding:2em;max-width:40em'>"
    "<h1>Broker login finished</h1>"
    "<p>1. Copy the <b>whole address</b> from your browser's address bar above (it holds today's login code).</p>"
    "<p>2. Open the <a style='color:#8cf' href=\"/#/token\">Broker login page</a> and paste it there.</p>"
    "<p>The code works once and only for a few minutes, so paste it now.</p></body></html>"
)


def redact(text: str) -> str:
    out = _ENV_CHANGED_OLD.sub(_env_changed_summary, text) if ".env changed: keys [" in text else text
    for pat in _SECRET_PATTERNS:
        out = pat.sub(lambda m: (m.group(1) if m.re.groups else "") + "<redacted>", out)
    return out


# ------------------------------------------------------------------------------------------ edition filters
# CLIENT §2 /api/logs row (tools/edition_probe.py LOG_HIT is the probe's copy). Case-insensitive (D98(6)): an
# "engine ... re-armed" line would otherwise pass. ARMED only as a WHOLE word (T-090): "_" is a word character,
# so the tracker's client-visible "trade N -> OPEN (SWING_ARMED)" and an armed_date key pass, while the engine's
# E2 machinery ("re-armed", "ARMED", "state=ARMED") still hides.
# D241(2): + any line naming the strategy spec ("spec S1.6.0"): a 0.8.2 / 0.8.3 startup line survives in gammad.log
# after the update (app.startup_line no longer writes it for the client).
# D298 MEDIUM-2(a) / D286(4): + the 0.8.4 V2 Scanner lines a client's gammad.log keeps across the update -- V2 / V2T /
# V2-S131 / v2_* keys, the gammad.v2scan.* logger names and "V2 Scanner", h60_sync / h60 sync / h60_store, h1 zones,
# "zone seed" -- and re-arm / rearm in any form (removed in both editions, D286(1)). SWING_ARMED, armed_date, HOI-2,
# "V1 Scanner" and "scanner day-end" still pass (tests/test_d298_client_log_filter.py).
CLIENT_LOG_HIDE = re.compile(
    r"\bE2\b|\bARMED\b|\bspec S\d+\.\d+|\bre-?arm|\bV2(?:T|_\w*)?\b|\bv2[_ -]?scan\w*|\bh60(?:_\w+)?\b"
    r"|\bh1 ?zones?\b|\bzone seed|\b1h stop zone",
    re.IGNORECASE,
)
CLIENT_STATE = {"IDLE": "WATCHING", "ARMED": "WATCHING"}
ENTERED_STATES = ("DONE", "ENTERED")  # ENTERED: already projected by app.Runtime.live_row
_ENGINE_HIDDEN = ("a", "b", "c", "consol_lo", "consol_hi", "consol_mid")
CONTROL_HEADER = "x-gamma-control"
TRUSTED_HOSTS = ("127.0.0.1", "localhost", "testserver")  # D98(3): DNS rebinding; testserver = Starlette TestClient
# T-267 (M11): every live trade of either family = rules.OPEN_STATES (V1 OPEN / T1_TRAIL + the V2 Scanner's Z_OPEN)
TRADE_STATES_OPEN: tuple[str, ...] = tuple(sorted(_rules.OPEN_STATES))
# WS pushes only the owner edition builds (a client builds neither): PLAN-W15 §2's shadow-scanner push and 0.8.2's
# owner extension push (C.9: {date, upto_ms, books}, never rows). Defence in depth: the client WS drops both.
# 0.8.5 (D287(6), PLAN-D287 I7): + the V2 Scanner switch push (owner only: the client has no V2 Scanner)
OWNER_WS_TYPES: tuple[str, ...] = ("scanner", "shadow", "v2_scanner")


def registry_live_scanners() -> tuple[str, ...]:
    """PLAN-W15 §2 / PLAN-W16 H5: the strategies the CLIENT's signal / trade routes carry = the client registry's
    non-shadow scanners (gammad.watch.engine_pool.build_scanners), read from the registry instead of a literal. 0.8.5
    (D287(6), PLAN-D287 I7): ("V1",) -- the client registry builds no V2 Scanner; it must equal
    engine_pool.live_scanners(False) (RuntimeError otherwise, never a silent narrowing or widening). The client registry
    is built, so the owner-only scanners (and gammad.v2scan) are never built or imported here."""
    from types import SimpleNamespace

    from gammad.config import HoldCfg, HoldOverridesCfg, WatcherCfg
    from gammad.watch import engine_pool

    specs = engine_pool.build_scanners(
        SimpleNamespace(watcher=WatcherCfg(), hold=HoldCfg(), hold_overrides=HoldOverridesCfg()), owner=False
    )
    live = tuple(s.name for s in specs if not s.shadow)
    want = engine_pool.live_scanners(False)
    if live != want:
        raise RuntimeError(f"the client scanner registry's live scanners must be {want}, got {live}")
    return live


CLIENT_LIVE_SCANNERS: tuple[str, ...] = registry_live_scanners()
# Every LIVE scanner of any edition (V1 + the owner's V2T): the routes read `live_scanners()` (the running edition's,
# at CALL time); every other scanner (V1 B, V2 = "V2-S131", V1 PLUS) is read through the owner-only /api/scanner/* routes.
LIVE_SCANNERS: tuple[str, ...] = _engine_pool.LIVE_SCANNERS
LIVE_SCANNER = CLIENT_LIVE_SCANNERS[0]  # "V1": the scanner a row without the column reads (schema v7 default)


def live_scanners() -> tuple[str, ...]:
    """0.8.5 (PLAN-D287 I7): the running edition's live scanners, read at CALL time: ("V1", "V2T") for the owner,
    ("V1",) for the client (a 0.8.4 client store's V2T rows and trades never show)."""
    return _engine_pool.live_scanners(not is_client())


def _live_in() -> str:
    """SQL placeholders for `scanner IN (...)` over `live_scanners()`."""
    return ",".join("?" * len(live_scanners()))
LIST_ORDER = ("EXPIRY-PE", "EXPIRY-CE", "BREAKDOWN-PE", "BREAKOUT-CE")  # T-154 / D126, both dashboards
SCANNER_DAYS_MAX = 250
SCANNER_WEEKS_MAX = 52
GRADE_FIELDS = ("entry", "sl", "t1", "mfe_pct", "mae_pct", "last_ltp", "status", "first_hit_ts_ms", "closed_ts_ms")


def is_client() -> bool:
    """Read at CALL time (tests flip gammad.edition.EDITION)."""
    return _edition.is_client()


async def v2_switch_body(request: Request) -> bool:
    """POST /api/v2-scanner's strict-bool `enabled`, read only AFTER the owner check: a declared Body parameter is
    validated by FastAPI before the handler runs, so a CLIENT's malformed body answered 422 and revealed the route
    (PLAN-D287 I7: 404 first). The OWNER's 422 bodies are FastAPI's own (json_invalid / missing / bool_type at
    body.enabled), so the owner route behaves as before. Module level: the string annotations cannot see a closure."""
    if is_client():
        raise HTTPException(status_code=404)
    raw = await request.body()
    payload: Any = None
    if raw:
        ctype = (request.headers.get("content-type") or "").split(";")[0].strip().lower()
        if not ctype or ctype == "application/json" or (ctype.startswith("application/") and ctype.endswith("+json")):
            try:
                payload = json.loads(raw)
            except json.JSONDecodeError as exc:
                raise RequestValidationError([{"type": "json_invalid", "loc": ("body", exc.pos),
                                               "msg": "JSON decode error", "input": {},
                                               "ctx": {"error": exc.msg}}]) from exc  # fmt: skip
    value = payload.get("enabled") if isinstance(payload, dict) else None
    if value is None:
        raise RequestValidationError([{"type": "missing", "loc": ("body", "enabled"), "msg": "Field required",
                                       "input": None}])  # fmt: skip
    if not isinstance(value, bool):
        raise RequestValidationError([{"type": "bool_type", "loc": ("body", "enabled"),
                                       "msg": "Input should be a valid boolean", "input": value}])  # fmt: skip
    return value


def signal_visible(kind: Any, scanner: Any = None) -> bool:
    """Owner: everything. Client: E1 only (D81) of a live scanner (`live_scanners()`: V1 only since 0.8.5, D287(6); a
    row without a scanner reads V1). D114: a gated E1 is visible WITH its tier (the D87 filter is gone)."""
    if not is_client():
        return True
    return kind in _edition.visible_kinds() and (scanner is None or scanner in live_scanners())


def project_state(state: Any, entered: bool = False) -> Any:
    """Client engine_state: IDLE -> WATCHING; ARMED / DONE -> ENTERED only when `entered` (a VISIBLE entry is that
    contract's latest entry today, D98(5), entered_ids), else WATCHING -- a state left by a hidden signal must not
    announce an entry. 0.8.5 (D287(1), PLAN-D287 I8): the client's entry is the E2 bar close, after which the engine
    stays ARMED until its E1 (DONE), so ARMED + a visible entry = ENTERED too. Idempotent for the same `entered`;
    owner: unchanged."""
    if state is None or not is_client():
        return state
    s = str(state)
    if s in ENTERED_STATES or (s == "ARMED" and entered):
        return "ENTERED" if entered else "WATCHING"
    return CLIENT_STATE.get(s, state)


def latest_visible_e1(conn: sqlite3.Connection, d: date) -> dict[int, int]:
    """security_id -> ts_ms of its LATEST ENTRY-tier E1 of a live scanner recorded on `d` (W15, PLAN-W15 §5: only an
    ENTRY marks a contract ENTERED; a STRONG WATCHLIST / WATCHLIST E1 is visible with its tier, the contract stays
    WATCHING). T-267: a V2T ENTRY counts, as it does on the live path (app._push_from_router notes every ENTRY E1
    push), so a restart keeps the same evidence. Shadow-scanner rows never count (their engines are not the one the
    client sees). T-310: a row still held for the 09:20 list post is no evidence yet."""
    from gammad.alerts.router import NOT_HELD_PENDING_SQL

    rows = conn.execute(
        f"SELECT security_id, MAX(ts_ms) FROM signals WHERE trade_date = ? AND kind = 'E1' AND scanner IN ({_live_in()})"
        " AND tier = 'ENTRY'" + NOT_HELD_PENDING_SQL + " GROUP BY security_id",
        (d.isoformat(), *live_scanners()),
    ).fetchall()
    return {int(r[0]): int(r[1]) for r in rows}


def _latest_hidden_close(conn: sqlite3.Connection, d: date) -> dict[int, int]:
    """security_id -> ts_ms of its latest hidden (gated) CLIENT E1 on `d`: the closed_cycles row the router writes
    instead of a signals row (D98(4)). A client store never holds a gated E1 row, so this is the only trace."""
    try:
        rows = conn.execute(
            "SELECT security_id, MAX(ts_ms) FROM closed_cycles WHERE trade_date = ? GROUP BY security_id",
            (d.isoformat(),),
        ).fetchall()
    except sqlite3.OperationalError as exc:  # a store without the table (pre-D98 v4): nothing was hidden in it
        if "no such table" not in str(exc):
            raise
        return {}
    return {int(r[0]): int(r[1]) for r in rows}


def entered_e1_ts(conn: sqlite3.Connection, d: date) -> dict[int, int]:
    """security_id -> ts_ms of its latest visible E1 on `d`, for the contracts whose latest ENTRY on `d` is that
    visible E1 (D98(5); FIX2 F2): per contract, not per day -- a visible E1 of setup 1 is no evidence once a later
    setup of the same contract ended in a hidden gated E1 (its closed_cycles row is after that visible E1)."""
    # W15: one E1 per arming cycle, so a LATER non-ENTRY E1 on the same contract is a later cycle's entry and the
    # earlier ENTRY is no longer the contract's latest entry (= app.Runtime._entered_this_cycle, D98(5)/FIX2 F2).
    closed = _latest_hidden_close(conn, d)
    later = _latest_non_entry_e1(conn, d)
    return {
        sid: ts
        for sid, ts in latest_visible_e1(conn, d).items()
        if ts >= closed.get(sid, ts) and ts >= later.get(sid, ts)
    }


def _latest_non_entry_e1(conn: sqlite3.Connection, d: date) -> dict[int, int]:
    """security_id -> ts_ms of its latest live-scanner E1 on `d` that is NOT an ENTRY (W15): a later cycle's
    WATCHLIST / STRONG WATCHLIST E1 means the contract's latest entry is not one the client took. (A client never
    records a non-ENTRY V2T row, PLAN-T267 section 1 "Tier rule"; the owner's state is never projected.) T-310: a held
    row is left out until its release."""
    from gammad.alerts.router import NOT_HELD_PENDING_SQL

    rows = conn.execute(
        f"SELECT security_id, MAX(ts_ms) FROM signals WHERE trade_date = ? AND kind = 'E1' AND scanner IN ({_live_in()})"
        " AND (tier IS NULL OR tier != 'ENTRY')" + NOT_HELD_PENDING_SQL + " GROUP BY security_id",
        (d.isoformat(), *live_scanners()),
    ).fetchall()
    return {int(r[0]): int(r[1]) for r in rows}


def entered_ids(conn: sqlite3.Connection, d: date) -> set[int]:
    """The client's ENTERED evidence: contracts whose latest entry on `d` is a visible E1 (entered_e1_ts)."""
    return set(entered_e1_ts(conn, d))


def client_engine(engine: Mapping[str, Any], entered: bool = False) -> dict[str, Any]:
    """Client contract engine view: projected state; a/b/c + consol_* hidden until the entry (they would
    reveal a live E2 while ARMED, or a hidden gated E1's cycle while DONE without a visible E1)."""
    out = dict(engine)
    out["state"] = project_state(out.get("state"), entered)
    if out["state"] != "ENTERED":
        for k in _ENGINE_HIDDEN:
            if k in out:
                out[k] = None
    return out


def ws_visible(msg: Mapping[str, Any]) -> bool:
    """Client WS filter (defence in depth; the router already filters at source)."""
    if not is_client():
        return True
    kind = msg.get("type")
    data = msg.get("data")
    d = data if isinstance(data, Mapping) else {}
    if "kind" not in d and isinstance(d.get("trade"), Mapping):  # {trade: row, event: ...} push shape
        d = {**d["trade"], **{k: v for k, v in d.items() if k == "scanner"}}
    if kind in OWNER_WS_TYPES:  # PLAN-W15 §2 / 0.8.2: owner-only pushes (a client builds no source for either)
        return False
    if kind == s089.WS_TYPE:
        return s089.ws_client_visible(data)
    if kind in ("signal", "trade"):
        return signal_visible(d.get("kind"), d.get("scanner"))
    if kind == "log":
        try:
            text = json.dumps(data, default=str)
        except (TypeError, ValueError):
            text = str(data)
        return not CLIENT_LOG_HIDE.search(text)
    return True


def broker_block(runtime: Any) -> dict[str, Any] | None:
    """`/api/status.broker`: registry.describe(runtime.broker_entry); without one, the registry's own
    resolution of an absent BROKER (owner -> dhan, client -> none). Never raises."""
    entry = broker_entry(runtime)
    if entry is None:
        return None
    try:
        from gammad.broker import registry

        block: dict[str, Any] = dict(registry.describe(entry))
    except Exception:
        log.exception("broker describe failed")
        return None
    block["budget_status"] = budget_status_of(runtime)
    if not is_client():
        block["calls_today"] = calls_today_of(runtime, entry)
    return block


def calls_today_of(runtime: Any, entry: Any) -> dict | None:
    """Owner Dhan request totals; no adapter construction or broker I/O."""
    try:
        if entry.name != "dhan":
            return None
        from gammad.broker import dhan_calls

        return dhan_calls.COUNTER.snapshot()
    except Exception:
        log.exception("Dhan daily calls read failed")
        return None


def budget_status_of(runtime: Any) -> str | None:
    """D98(8): the broker's daily API budget line (Nuvama `budget_status`: None while OK, else the one-line
    reason), read from an adapter the runtime has ALREADY built (`runtime.broker_budget_status()`); None for
    every other broker, a runtime without the hook, or a failing read. Never builds an adapter."""
    hook = getattr(runtime, "broker_budget_status", None)
    if not callable(hook):
        return None
    try:
        value = hook()
    except Exception:  # a status poll must never fail on a budget read
        log.exception("broker budget status failed")
        return None
    return str(value) if value else None


def broker_entry(runtime: Any) -> Any:
    entry = getattr(runtime, "broker_entry", None)
    if entry is not None:
        return entry
    try:
        from gammad.broker import registry

        return registry.resolve(None)
    except Exception:  # noqa: BLE001 -- client with no BROKER / a broken registry: no broker block
        return None


# X8: E2 timings ride in latency; jobs are operator detail (D86, D87); 0.8.2: `owner` is the owner extension's block
# (a client Runtime never has one; dropped too should the edition read client while an owner Runtime runs)
# delivery: T-211, owner only (D189(2)); spec_version: D241(2) / PLAN-T267 M9, the strategy spec version is OWNER-only
# v2_enabled: 0.8.5 (D287(6), PLAN-D287 I7), the V2 Scanner switch is owner only (the client has no V2 Scanner)
CLIENT_STATUS_DROP = ("latency", "jobs", "owner", "delivery", "spec_version", "v2_enabled")
# D241(2) / D256 (T-267 wave 3): the trades, signals, levels and watchlist tables each keep a spec_version column (and
# a signal's payload repeats it); the client is served those rows WITHOUT it, at any depth, on their routes
# (/api/watchlists included) and on every WS message (client_rows; tests/test_client_no_spec_version.py). The store
# keeps the column: it is shared with the owner tooling.
CLIENT_ROW_DROP: tuple[str, ...] = ("spec_version",)


def _drop_keys(obj: Any, keys: tuple[str, ...]) -> Any:
    if isinstance(obj, Mapping):
        return {k: _drop_keys(v, keys) for k, v in obj.items() if k not in keys}
    if isinstance(obj, list | tuple):
        return [_drop_keys(v, keys) for v in obj]
    return obj


def client_rows(obj: Any) -> Any:
    """CLIENT: `obj` (a route body or a WS message) rebuilt without any CLIENT_ROW_DROP key at any depth -- new dicts and
    lists, the input is never mutated (a hub message is shared by every subscriber). OWNER: `obj` itself, unchanged."""
    if not is_client():
        return obj
    return _drop_keys(obj, CLIENT_ROW_DROP)


def still_watching_of(runtime: Any) -> bool:
    """0.8.9 T-419 (D471 Q10, D480): the Still Watching switch of the runtime's CURRENT settings
    (`edition.still_watching_on`: no settings / no block -> on)."""
    return _edition.still_watching_on(getattr(runtime, "cfg", None))


def telegram_check_of(runtime: Any) -> Any:
    """The T-062 verdict (config.TelegramCheck) of the runtime's CURRENT settings, or None."""
    cfg = getattr(runtime, "cfg", None)
    return getattr(cfg, "telegram", None) if cfg is not None else None


def telegram_block(runtime: Any) -> dict[str, Any] | None:
    """W2: `/api/status.telegram` = cfg.telegram.to_dict() {ok, edition, group_ok, personal_ok,
    personal_configured, problems[], last_error, token_present}; None when the runtime carries no settings. W14 P1:
    last_error = the router's escalated send failure (3 in a row) as a string, null once a send succeeds. W16 S0(1)
    (T-132, D118(6)): token_present = a non-blank TELEGRAM_BOT_TOKEN in the current settings (the value never
    leaves the process); the Overview Alerts tile reads NOT CONFIGURED from it."""
    check = telegram_check_of(runtime)
    if check is None:
        return None
    last = getattr(getattr(runtime, "router", None), "telegram_last_error", None)
    token = getattr(getattr(getattr(runtime, "cfg", None), "env", None), "telegram_bot_token", "")
    return {
        **check.to_dict(),
        "last_error": last if isinstance(last, str) else None,
        "token_present": bool(isinstance(token, str) and token.strip()),
    }


def chats_block(runtime: Any) -> dict[str, Any] | None:
    """D1: `/api/token.chats` = {group, personal, verdict, ok}: both chat ids MASKED (config.mask), the T-062
    verdict as one plain line ("ok" or the problems), ok = no problem. None without settings."""
    from gammad.config import mask

    cfg = getattr(runtime, "cfg", None)
    env = getattr(cfg, "env", None)
    check = telegram_check_of(runtime)
    if env is None or check is None:
        return None
    return {
        "group": mask(str(env.telegram_group_id or "")) or None,
        "personal": mask(str(env.telegram_personal_chat_id or "")) or None,
        "verdict": "ok" if check.ok else "; ".join(check.problems),
        "ok": bool(check.ok),
    }


def calendar_body(cal: Any) -> dict[str, Any]:
    """T-556: listed years and their weekday closures, using the engine's trading calendar."""
    years = list(cal.years)
    holidays: list[str] = []
    for year in years:
        day = date(year, 1, 1)
        while day.year == year:
            if day.weekday() < 5 and not cal.is_trading_day(day):
                holidays.append(day.isoformat())
            day += timedelta(days=1)
    return {"years": years, "holidays": holidays}


def status_body(
    runtime: Any, body: Mapping[str, Any] | None = None, *, build: build_info.BuildInfo | None = None
) -> dict[str, Any]:
    """runtime.status() + edition + broker + telegram + build + install + hub (existing keys are kept). `telegram` is
    present whenever the runtime carries settings (the real Runtime always does). CLIENT: no latency and no
    jobs (X8: the API is the boundary, D86), on /api/status and on every WS status.
    W17 T-171 (PLAN-W17 §2.3): `build` = {commit, bundle}, both editions; `build` is what create_app resolved ONCE
    (None here = a caller outside create_app: both null). Only a cheap stat of build.json happens per call."""
    out = dict(runtime.status() if body is None else body)
    out.setdefault("edition", _edition.edition())
    out.setdefault("install", install_block(runtime))
    out.setdefault("hub", hub_block(runtime))
    out.setdefault("build", build.block() if build is not None else dict(build_info.NULL_BLOCK))
    if "broker" not in out:
        out["broker"] = broker_block(runtime)
    if "telegram" not in out:
        tg = telegram_block(runtime)
        if tg is not None:
            out["telegram"] = tg
    if "v2_enabled" not in out and not is_client():  # D255: the V2 Scanner switch, OWNER only since 0.8.5 (D287(6))
        out["v2_enabled"] = v2_switch(runtime)["enabled"]
    if "still_watching" not in out and not is_client():
        out["still_watching"] = still_watching_of(runtime)
    if is_client():
        for k in CLIENT_STATUS_DROP:
            out.pop(k, None)
    return out


_SINCE: dict[Path, str] = {}
_SINCE_ERROR_LOGGED = False


def install_block(runtime: Any) -> dict[str, str | None]:
    """Install identity and first watched session; never create a store or cache an empty one."""
    global _SINCE_ERROR_LOGGED
    root = getattr(getattr(getattr(runtime, "cfg", None), "paths", None), "root", None)
    folder = None if root is None else (Path(root).name if is_client() else str(root))
    try:
        data_dir = None if is_client() else str(data_dir_of(runtime))
    except (AttributeError, TypeError, ValueError, OSError):
        data_dir = None
    since = None
    path = getattr(runtime, "db_path", None)
    if path is not None and Path(path).is_file():
        key = Path(path).resolve()
        since = _SINCE.get(key)
        if since is None:
            try:
                with _read(runtime) as conn:
                    since = conn.execute("SELECT MIN(trade_date) FROM watchlist").fetchone()[0]
                if since is not None:
                    _SINCE[key] = since
            except sqlite3.Error:
                if not _SINCE_ERROR_LOGGED:
                    _SINCE_ERROR_LOGGED = True
                    log.exception("First session could not be read; showing unknown")
    return {"folder": folder, "data_dir": data_dir, "since": since}


def hub_block(runtime: Any) -> dict[str, int]:
    return {"dropped": getattr(getattr(runtime, "hub", None), "dropped", 0)}


def v2_switch(runtime: Any) -> dict[str, bool]:
    """D255: {enabled, default} of this install's V2 Scanner switch (db.get_v2_enabled; default = client OFF, owner
    ON). Read-only and never creates a store: a runtime without a store file (or a failing read) answers the default.
    The status poll calls this every 2 s; one indexed SELECT on a short-lived WAL reader connection."""
    from gammad.store import db

    owner = not is_client()
    default = db.v2_enabled_default(owner=owner)
    path = getattr(runtime, "db_path", None)
    if path is None or not Path(path).is_file():
        return {"enabled": default, "default": default}
    try:
        with _read(runtime) as conn:
            enabled = db.get_v2_enabled(conn, owner=owner)
    except sqlite3.Error:
        log.exception("V2 Scanner switch could not be read; showing the default")
        enabled = default
    return {"enabled": enabled, "default": default}


def token_page(runtime: Any) -> dict[str, Any]:
    """The broker-aware half of GET /api/token (BROKER §5): static facts from the registry entry, the
    dynamic ones (masked token, login URL) from the token watcher's `token_view` hook. No secret."""
    entry = broker_entry(runtime)
    view_hook = getattr(runtime, "token_view", None)
    view: dict[str, Any] = {}
    if callable(view_hook):
        try:
            view = dict(view_hook())
        except Exception:
            log.exception("token_view failed")
    out: dict[str, Any] = {
        "broker": None,
        "token_kinds": [],
        "token_key": view.get("token_key"),
        "token_masked": view.get("token_masked"),
        "token_expiry_rule": None,
        "client_login": None,
        "auto_renew": False,
    }
    hook = getattr(runtime, "token_auto_renew_on", None)
    if not is_client():
        from gammad.auth.token_watch import get_auto_renew_enabled

        with _read(runtime) as conn:
            out["auto_renew_enabled"] = get_auto_renew_enabled(conn)
    if not is_client() and callable(hook):
        try:
            out["auto_renew"] = bool(hook())
        except Exception:
            log.exception("token_auto_renew_on failed")
    if entry is None:
        return out
    try:
        from gammad.broker import registry

        kinds = list(registry.token_kinds(entry))
    except Exception:  # noqa: BLE001 -- fall back to the kind every edition allows
        kinds = ["paste"]
    out.update(
        broker={
            "name": entry.name,
            "label": entry.label,
            "status": entry.caps.status,
            "beta": entry.caps.status != "live-verified",
        },
        token_kinds=kinds,
        token_key=entry.token_key,
        token_expiry_rule=entry.caps.token_expiry_rule,
    )
    cl = getattr(entry, "client_login", None)
    if cl is not None:
        out["client_login"] = {"fields": list(cl.fields), "login_url": view.get("login_url")}
    return out


def data_dir_of(runtime: Any) -> Path:
    """Where the tray supervisor's control files live: runtime.data_dir, else the store's directory."""
    d = getattr(runtime, "data_dir", None)
    return Path(d) if d is not None else Path(runtime.db_path).parent


# ------------------------------------------------------------------------------------------ event hub
class EventHub:
    """Fan-out of {type, data} messages to every WebSocket subscriber.

    `publish()` is safe from any thread: a subscriber's queue is only touched on the loop that created it.
    A full subscriber queue drops the message (a slow browser must never stall the tick path).
    """

    def __init__(self, maxsize: int = HUB_QUEUE_MAX) -> None:
        self.maxsize = maxsize
        self._subs: list[tuple[asyncio.AbstractEventLoop, asyncio.Queue[dict[str, Any]]]] = []
        self._lock = threading.Lock()
        self.dropped = 0
        self.published: dict[str, int] = {}

    def subscribe(self) -> asyncio.Queue[dict[str, Any]]:
        q: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=self.maxsize)
        with self._lock:
            self._subs.append((asyncio.get_running_loop(), q))
        return q

    def unsubscribe(self, q: asyncio.Queue[dict[str, Any]]) -> None:
        with self._lock:
            self._subs = [(lp, x) for lp, x in self._subs if x is not q]

    @property
    def n_subscribers(self) -> int:
        return len(self._subs)

    def _put(self, q: asyncio.Queue[dict[str, Any]], msg: dict[str, Any]) -> None:
        try:
            q.put_nowait(msg)
        except asyncio.QueueFull:
            self.dropped += 1

    def publish(self, type_: str, data: Any) -> None:
        msg = {"type": type_, "data": data}
        self.published[type_] = self.published.get(type_, 0) + 1
        try:
            current: asyncio.AbstractEventLoop | None = asyncio.get_running_loop()
        except RuntimeError:
            current = None
        with self._lock:
            subs = list(self._subs)
        for lp, q in subs:
            if lp is current:
                self._put(q, msg)
            elif not lp.is_closed():
                with contextlib.suppress(RuntimeError):
                    lp.call_soon_threadsafe(self._put, q, msg)


# ------------------------------------------------------------------------------------------ runtime seam
class RuntimeAPI(Protocol):
    """What the API needs from the supervisor (gammad.app.Runtime implements it; tests fake it).

    OPTIONAL (read with getattr; absent = the fallback in brackets):
      broker_entry: BrokerEntry            [registry.resolve(None): owner -> dhan, client -> none]
      token_login(fields) -> awaitable     [POST /api/token/login answers 501]  (TokenWatcher.attach sets it)
      token_view() -> {token_key, token_masked, login_url}  [nulls]            (TokenWatcher.attach sets it)
      data_dir: Path                       [db_path.parent]  -- where POST /api/service/stop writes STOP
    """

    db_path: Path
    logs_dir: Path
    hub: EventHub
    token_updater: Callable[[str], Any] | None

    def today(self) -> date: ...
    def status(self) -> dict[str, Any]: ...
    def token_status(self) -> dict[str, Any]: ...
    def live_row(self, security_id: int) -> dict[str, Any]: ...
    def contract_view(self, security_id: int, tf: int) -> dict[str, Any] | None: ...
    async def run_job(self, name: str, kind: str | None = None) -> str: ...
    def set_paused(self, paused: bool) -> None: ...
    def preview(self, kind: str, ref: Mapping[str, Any], dest: str = "group") -> list[str]: ...
    async def send_manual(
        self, kind: str, ref: Mapping[str, Any], dest: str = "group", *, who: str = "local"
    ) -> dict[str, Any]: ...
    def mute(self, security_id: int, muted: bool, reason: str | None = None) -> list[int]: ...


# ------------------------------------------------------------------------------------------ queries
@contextlib.contextmanager
def _read(runtime: RuntimeAPI) -> Generator[sqlite3.Connection]:
    from gammad.store import db

    conn = db.connect(runtime.db_path)
    try:
        yield conn
    finally:
        conn.close()


def _day_ms(d: date) -> tuple[int, int]:
    start = datetime.combine(d, dtime(0, 0), tzinfo=IST)
    return int(start.timestamp() * 1000), int(start.timestamp() * 1000) + 86_400_000


def query_levels(conn: sqlite3.Connection, d: date) -> dict[str, Any]:
    from gammad.store import db

    rows = db.fetch_levels(conn, d.isoformat())
    return {"date": d.isoformat(), "run_id": rows[0]["run_id"] if rows else None, "rows": rows}


# 0.8.1 (D162(5)): the store's v9 watchlist columns (db.ADDED_COLUMNS from atr_pct on) are OWNER data -- the carry
# state of a carried row (D159(4), D160(3)) and the ATR input of the owner's views. The client /api/watchlists drops
# them, and drops any tag-CARRY row (a client store never carries; the route defends anyway). /api/levels keeps its
# atr_pct in both editions: a plain number, the client's own V1 dynamic-zone input (D161(3)), no owner word.
CLIENT_HIDDEN_WATCH_COLUMNS: tuple[str, ...] = (
    "atr_pct", "origin_run_id", "first_listed", "last_listed", "carry_k", "carry_state", "carry_flags",
    "oi_at_listing", "prem_at_listing", "carry_reason",
    "oi_peak_prior",  # 0.8.9 T-419 (schema 11): the carry scanner's prior-session OI peak, an owner input
)  # fmt: skip
# T-594, CX-161 §1 H1, D437: client Still Watching dates only, no carry word.
CLIENT_SW_LISTING_COLUMNS: tuple[str, ...] = ("first_listed", "last_listed")
CARRY_TAG = _edition.CARRY_TAG
# 0.8.9 T-419 (D471 Q8, D480): a client sees a carried row ONLY as a row of its third list, named exactly this (D437: the
# internal tag CARRY and the words carry / carried never reach a client request or response)
CLIENT_STILL_WATCHING = _edition.CLIENT_STILL_WATCHING
SW_TRADE_SCANNER = "SW"  # = gammad.trade.rules.SCANNER_SW (T419-CONTRACT): trades.scanner of a Still Watching trade
# the levels a Still Watching trade row carries in BOTH editions (client-safe numbers; schema 11 adds the last three)
SW_TRADE_LEVELS: tuple[str, ...] = ("stop_top", "stop_bottom", "target_level", "trail_level")


def client_still_watching_row(row: dict[str, Any]) -> dict[str, Any]:
    """A carried watchlist row as the CLIENT sees it: list_name = "Still Watching", no tag (the internal CARRY) and no v9
    / owner column (the caller drops CLIENT_HIDDEN_WATCH_COLUMNS after the live merge). The row's own list, expiry and
    contract fields stay (its strike, option type and expiry are the contract the card names). Mutates and returns."""
    row["list_name"] = CLIENT_STILL_WATCHING
    row["tag"] = None
    return row


def _group_block(
    conn: sqlite3.Connection, row: Mapping[str, Any], memo: dict[tuple[Any, ...], Any], *, stored_signal: bool = False
) -> str | None:
    """0.8.3 C.5 (B8): an OWNER row's `group_block` = the router's sentence for `group_refusal_reason`'s code -- the SAME
    function the manual GROUP send refuses with, never a second copy of the rule -- or None when the group may carry the
    row. `row` = a stored watchlist row, or (`stored_signal`) a stored signals row (flattened by router.signal_row, the
    shape the send path reads). `memo` = one dict per request (the router reads the day's view once, not once per row).
    Called for the owner edition only: a client row never carries the key."""
    from gammad.alerts import router as rt

    code = rt.group_refusal_reason(conn, rt.signal_row(row) if stored_signal else row, owner=True, memo=memo)
    return None if code is None else rt.GROUP_BLOCK_TEXT[code]


def query_watchlists(conn: sqlite3.Connection, d: date, live: Callable[[int], dict[str, Any]],
                     client: bool = False, *, still_watching: bool = True) -> dict[str, Any]:  # fmt: skip
    """The day's live watchlist rows by list. `client` (the client edition): no v9 owner column.
    0.8.3 C.5: an owner row also carries `group_block` (`_group_block` of the stored row, before the live merge).
    D429 (0.8.8): `client` shows ONLY the BREAKDOWN-PE / BREAKOUT-CE lists (`edition.v1_lists_sql(owner=False)`), on
    every date: an EXPIRY list a store built before 0.8.8 holds is never shown. The owner query is unchanged.
    0.8.9 T-419 (D471 Q8, D480): `client` with `still_watching` (edition.still_watching_on: Settings.still_watching
    .enabled, default on) returns the CARRY-tagged rows as the third list CLIENT_STILL_WATCHING ("Still Watching", after
    the two V1 lists; `client_still_watching_row`: no tag, no owner column); `still_watching` False drops them, as
    before 0.8.9. The owner edition is unchanged (its CARRY rows keep their list and tag)."""
    rows = conn.execute(
        # S1.3.1 (T-063): a retired row (its zone died before the 09:20 build) is hidden in both editions
        "SELECT * FROM watchlist WHERE trade_date = ? AND retired_run_id IS NULL"
        + _edition.v1_lists_sql(owner=not client)
        + " ORDER BY list_name, expiry_date, symbol, tag, security_id",
        (d.isoformat(),),
    ).fetchall()
    muted = {int(r[0]) for r in conn.execute("SELECT security_id FROM mutes").fetchall()}
    lists: dict[tuple[str, str], list[dict[str, Any]]] = {}
    memo: dict[tuple[Any, ...], Any] = {}
    for r in rows:
        row = dict(r)
        carried = row.get("tag") == CARRY_TAG
        if client and carried and not still_watching:
            continue
        block = None if client else _group_block(conn, row, memo)
        row.update(live(int(row["security_id"])))
        row["muted"] = int(row["security_id"]) in muted
        if client:  # after the live merge, so no source can put a hidden column back
            for col in CLIENT_HIDDEN_WATCH_COLUMNS:
                if not (carried and col in CLIENT_SW_LISTING_COLUMNS):
                    row.pop(col, None)
            if carried:  # T-419: the client's third list (after the merge: no source can put the tag back)
                client_still_watching_row(row)
        else:
            row["group_block"] = block
        lists.setdefault((row["list_name"], row["expiry_date"]), []).append(row)
    # T-154 / D126: the owner's list order (expiry lists first), then expiry; rows stay symbol-ordered inside a list
    order = sorted(lists, key=lambda k: (LIST_ORDER.index(k[0]) if k[0] in LIST_ORDER else len(LIST_ORDER), k))
    return {
        "date": d.isoformat(),
        "lists": [{"list_name": k[0], "expiry_date": k[1], "rows": lists[k]} for k in order],
    }


def _loads(raw: Any) -> Any:
    if not raw:
        return None
    try:
        return json.loads(raw)
    except ValueError:
        return None


WHY_NOT_TIERS: tuple[str, ...] = ("STRONG_WATCHLIST", "WATCHLIST")  # the tiers that did not fully qualify
WHY_NOT_UNCHECKED = "could not be checked"  # D273: a check with no input ("<check> could not be checked")
WHY_NOT_DUP_STOCK = "stock already had an entry today (one entry per stock per day)"  # the R4 demotion, no failed check
# T-310 (D278(5)): a V1 signal before the 09:20 list post that failed its release check (router.HELD_INVALID, ctx held_why);
# client-safe words (both editions read them): no owner word
WHY_NOT_HELD: Mapping[str, str] = {
    "stop": "price hit its stop before the 09:20 list was posted, so it was not sent",
    "retired": "contract left the list at the 09:20 build, so it was not sent",
    # T-310 review L6 / L7
    "no_price": "no live price when the 09:20 list was posted, so it was not sent",
    "error": "could not be checked when the 09:20 list was posted, so it was not sent",
    "expired": "the 09:20 list was not posted that day, so it was not sent",
}
WHY_NOT_HELD_OTHER = "no longer valid when the 09:20 list was posted, so it was not sent"
_WHY_NOT_TAG = re.compile(r"<[^>]+>")


def signal_why_not(r: Mapping[str, Any]) -> list[str]:
    """D273 (0.8.5): a stored signals row's WHY NOT, the dashboard's "Setups that did not qualify" text -- each failed
    check as ONE plain sentence (no HTML, no emoji) with the numbers its context stored. ONE SOURCE: the Telegram cards'
    own CHECKS lines (templates.check_lines -> _check_fail_numbers / _check_text), each failed line with its marker,
    markup and "Name: " lead stripped; a check with no input reads "<check> could not be checked"; an R4-demoted row
    (dup_stock, every check passed) reads WHY_NOT_DUP_STOCK. [] for an ENTRY, an E2 / untiered row. A V2 Scanner row
    (its watch tiers are owner-only, and its tier reasons are not the four V1 checks) lists its stored reason tokens.
    Called on the stored row BEFORE any client trimming of `ctx`, so a client sees the finished sentence only."""
    import html

    from gammad.alerts import router as rt
    from gammad.alerts import templates as tpl

    flat = tpl.flatten_signal_row(dict(r))
    # 0.8.5 (D287(2)): an owner TIERED E2 has its WHY NOT too (an untiered E2 has no tier, so none)
    if str(flat.get("kind") or "") not in (_edition.KIND_E1, _edition.KIND_E2) or flat.get("tier") not in WHY_NOT_TIERS:
        return []
    ctx = tpl.context_from_row(flat)
    reasons = [str(x) for x in (ctx.gate_reasons if ctx is not None else rt.reasons_list(flat.get("gate_reasons")))]
    if (flat.get("scanner") or LIVE_SCANNER) != LIVE_SCANNER:
        return [*reasons, *(f"{u} {WHY_NOT_UNCHECKED}" for u in (ctx.gate_unknown if ctx is not None else ()))]
    out: list[str] = []
    for line in tpl.check_lines(rt.meta_from_row(flat), ctx):
        if not line.startswith(tpl.M_FAIL):
            continue
        text = html.unescape(_WHY_NOT_TAG.sub("", line[len(tpl.M_FAIL) :])).strip()
        name, sep, why = text.partition(": ")
        if not sep:
            out.append(text)
        elif why.startswith("not checked"):
            out.append(f"{name.lower()} {WHY_NOT_UNCHECKED}")
        else:
            out.append(why)
    if rt.DUP_STOCK in reasons:
        out.append(WHY_NOT_DUP_STOCK)
    if rt.HELD_INVALID in reasons:  # T-310: why a held signal was never sent (its stored ctx names the check)
        raw = flat.get("ctx_json") if flat.get("ctx_json") is not None else flat.get("ctx")
        stored = raw if isinstance(raw, Mapping) else _loads(raw)
        why = stored.get(rt.HELD_WHY_KEY) if isinstance(stored, Mapping) else None
        out.append(WHY_NOT_HELD.get(str(why), WHY_NOT_HELD_OTHER))
    # a non-qualifying row whose stored context names no failed check (missing / pre-v3 context): never an empty WHY NOT
    return out or [WHY_NOT_UNCHECKED]


def _api_signal_row(r: Mapping[str, Any]) -> dict[str, Any]:
    """A stored signals row as the API shows it: payload (parsed payload_json, {} if none), ctx (parsed ctx_json or
    null), gate_reasons as a list, and hold / gate_status / tier / scanner always present. D273: + `why_not`
    (signal_why_not, the finished sentences); a client row's ctx never carries the zone limit itself."""
    from gammad.alerts.router import reasons_list, wire_ctx

    why_not = signal_why_not(r)
    row = dict(r)
    payload = _loads(row.pop("payload_json", None))
    row["payload"] = payload if isinstance(payload, dict) else {}
    ctx = _loads(row.pop("ctx_json", None))
    row["ctx"] = ctx if isinstance(ctx, dict) else None
    row["hold"] = row.get("hold")
    row["gate_status"] = row.get("gate_status")
    row["gate_reasons"] = reasons_list(row.get("gate_reasons"))
    row["tier"] = row.get("tier")
    row["scanner"] = row.get("scanner") or LIVE_SCANNER
    row["why_not"] = why_not
    if is_client() and isinstance(row["ctx"], dict):  # D273: the sentence is the client's; the raw limit key never is
        row["ctx"] = wire_ctx(row["ctx"], client=True)  # T-305 / T-310: nor the hold keys its store keeps
    return row


def query_signals(conn: sqlite3.Connection, d: date, limit: int) -> dict[str, Any]:
    """Live-scanner (V1 + V2T, LIVE_SCANNERS) signals rows, newest first: the stored columns (tier, scanner included)
    + payload + the v3 context: hold, gate_status ('pass' | 'gated'; null = recorded before v3), gate_reasons (list)
    and ctx (parsed ctx_json, or null). Client: E1 only, EVERY tier (D114; the D87 gated filter is gone). 0.8.3 C.5:
    an owner row also carries `group_block` (`_group_block`); a client row never does."""
    from gammad.alerts.router import NOT_HELD_PENDING_SQL

    client = is_client()
    # T-310: a V1 row held for the 09:20 list post is not shown until its release (both editions)
    where = f"trade_date = ? AND scanner IN ({_live_in()})" + NOT_HELD_PENDING_SQL  # 0.8.5 I7: call-time scanners
    params = [d.isoformat(), *live_scanners()]
    if client:  # CLIENT §2: WHERE kind IN visible_kinds()
        kinds = _edition.visible_kinds()
        where += f" AND kind IN ({','.join('?' * len(kinds))})"
        params.extend(kinds)
    rows = conn.execute(
        f"SELECT * FROM signals WHERE {where} ORDER BY ts_ms DESC, id DESC LIMIT ?", (*params, limit)
    ).fetchall()
    if client:
        return {"rows": [_api_signal_row(r) for r in rows]}
    memo: dict[tuple[Any, ...], Any] = {}
    return {"rows": [_api_signal_row(r) | {"group_block": _group_block(conn, r, memo, stored_signal=True)}
                     for r in rows]}  # fmt: skip


SIGNAL_CONTEXT_EXPORT_COLUMNS: tuple[str, ...] = ("hold", "gate_status", "gate_reasons", "tier", "tier_code")


def signal_export_columns() -> tuple[str, ...]:
    """tpl.SIGNAL_EXPORT_COLUMNS plus the v3 context columns it does not list yet (read at call time, so
    the templates module stays the owner of the order)."""
    from gammad.alerts import templates as tpl

    base = tuple(tpl.SIGNAL_EXPORT_COLUMNS)
    return base + tuple(c for c in SIGNAL_CONTEXT_EXPORT_COLUMNS if c not in base)


def export_tier_cells(code: Any) -> dict[str, Any]:
    """R25 (T-306 (3)): an export is a display surface: tier = the D273 word (templates.TIER_LABEL,
    read at call time), tier_code = the stored code."""
    from gammad.alerts import templates as tpl

    if not code:
        return {"tier": None, "tier_code": None}
    return {"tier": tpl.TIER_LABEL.get(str(code), str(code)), "tier_code": code}


def query_oi_series(conn: sqlite3.Connection, security_id: int, d: date) -> list[list[int]]:
    lo, hi = _day_ms(d)
    rows = conn.execute(
        "SELECT ts_ms, oi FROM oi_series WHERE security_id = ? AND ts_ms >= ? AND ts_ms < ? AND oi IS NOT NULL "
        "ORDER BY ts_ms",
        (security_id, lo, hi),
    ).fetchall()
    return [[int(r[0]), int(r[1])] for r in rows]


CONTRACT_NAME_KEYS = ("symbol", "strike", "option_type", "expiry_date", "list_name")


def query_contract_name(conn: sqlite3.Connection, security_id: int, d: date) -> dict[str, Any] | None:
    """W17 T-170 (PLAN-W17 §2.2, review R5): /api/contract `contract` = the newest watchlist row of the id dated on or
    before `d` (a live row before a retired one of the same date), so the Contract page names a contract that has left
    today's lists; None when no row that old exists."""
    r = conn.execute(
        "SELECT symbol, strike, option_type, expiry_date, list_name FROM watchlist WHERE security_id = ?"
        " AND trade_date <= ? ORDER BY trade_date DESC, retired_run_id IS NOT NULL, list_name LIMIT 1",
        (int(security_id), d.isoformat()),
    ).fetchone()
    if r is None:
        return None
    out = dict(zip(CONTRACT_NAME_KEYS, tuple(r), strict=True))
    out["strike"] = None if out["strike"] is None else float(out["strike"])
    return out


def signal_ref_visible(conn: sqlite3.Connection, signal_id: Any) -> bool:
    """False when the signal `signal_id` exists but this edition may not see it (client: an E2 or a shadow-scanner
    row; a gated E1 is visible since D114). A missing row is left to the caller's own 404."""
    if not is_client():
        return True
    try:
        sid = int(signal_id)
    except (TypeError, ValueError):
        return True  # the route's own validation answers 422
    row = conn.execute("SELECT kind, scanner FROM signals WHERE id = ?", (sid,)).fetchone()
    return row is None or signal_visible(row["kind"], row["scanner"])


# T-637 (CX-216 review F3, D691): an s089 SIGNAL REF = a `signal` ref whose `id` is the served EntrySignal key
# (contract.EntrySignal.key "<scanner>|<security_id>|<signal_bar_ms>"); a legacy ref's id is the signals-table integer,
# so the two never collide. Owner only: the client edition answers 404 (`_ref_hidden`), as for an owner-only route.
S089_KEY_RE = re.compile(rf"^(?:{'|'.join(map(re.escape, _sc.SCANNER_IDS))})\|([0-9]+)\|([0-9]+)\Z")


def s089_signal_key(kind: str, ref: Mapping[str, Any] | None) -> str | None:
    """The s089 key a `signal` ref names, or None for every other ref (a legacy integer id included)."""
    raw = ref.get("id") if kind == "signal" and isinstance(ref, Mapping) else None
    return raw if isinstance(raw, str) and S089_KEY_RE.match(raw) else None


def s089_manual_card(source: Any, key: str, dest: str) -> str:
    """T-637: the 0.8.9 entry card a manual send of s089 signal `key` to `dest` posts = the card the dispatch planned for
    that chat (s089_dispatch.plan_entry; the same targets), so a hand send never says more than the automatic one.
    Refusals (before anything is sent): a banned contract (the dispatch posts it nowhere) -> ValueError; a GROUP send of
    a row the group never gets (another scanner, an EXPIRY list) -> router.GroupRefusal with the legacy code; a PERSONAL
    send of a row the owner's book blocked (MAX_OPEN / DAY_CAP: the owner card is never rendered) -> ValueError.
    LookupError = no 0.8.9 scanners on this install, or no such signal on its day."""
    from gammad.alerts import router as rt
    from gammad.alerts import s089_dispatch as dispatch
    from gammad.scanners.exits import ist_date

    if dest not in SEND_DESTS:
        raise ValueError(f"dest must be one of {SEND_DESTS}, got {dest!r}")
    match = S089_KEY_RE.match(key)
    if source is None or match is None:
        raise LookupError(f"no s089 signal {key}" if source is not None else "no 0.8.9 scanners on this install")
    try:
        day = ist_date(int(match.group(2)))  # the key's signal bar: its trade date
    except (OverflowError, OSError, ValueError):  # review F4: a bar ms no clock can hold is no signal (404, not 500)
        raise LookupError(f"no s089 signal {key}") from None
    signal = next((s for s in source.signals(day) if s.key == key), None)
    if signal is None:
        raise LookupError(f"no s089 signal {key}")

    def v2_targets(s: Any) -> tuple[float, ...]:  # the OWN ladder's targets (v2sq.live.accept), none before a fill
        return next((tuple(p.targets) for p in source.positions(day, False) if p.position_key == s.key), ())

    edition = "client" if is_client() else "owner"
    posts = {p.chat: p for p in dispatch.plan_entry(signal, edition=edition, own_targets=source.targets(),
                                                    group_targets=source.group_targets, v2_targets=v2_targets)}
    if dest in posts:
        return posts[dest].text
    if signal.ban_reason is not None:
        raise ValueError(f"signal {key}: banned ({signal.ban_reason}): no card is sent for a banned contract")
    if dest == dispatch.GROUP:
        code = rt.GROUP_BLOCK_SCANNER if signal.scanner not in _sc.CLIENT_SCANNERS else rt.GROUP_BLOCK_LIST
        raise rt.group_refusal_error(f"signal {key}", code)
    raise ValueError(f"signal {key}: {signal.risk_block or 'blocked'} (the owner's book): no owner card")


async def s089_send_manual(router: Any, source: Any, kind: str, ref: Mapping[str, Any], dest: str, *,
                           who: str) -> dict[str, Any]:
    """T-637: POST /api/send of an s089 signal ref (app.Runtime.send_manual's branch), with router.send_manual's own
    bookkeeping: one audit line per exit, a GROUP refusal stored as a refused manual_sends row with its code, a sent /
    failed row with the C1 keys. Like every manual send (D48) it ignores pause and mute; no buttons (the dispatch sends
    none). Returns {ok, message_preview, parts, error}."""
    from gammad.alerts import router as rt

    def audit(status: str, detail: str) -> None:
        rt.manual_audit(kind, ref, dest, who, status, detail)

    try:
        text = s089_manual_card(source, str(ref.get("id")), dest)
    except rt.GroupRefusal as exc:
        audit(rt.ST_REFUSED, exc.code)
        router._log_send(kind, dict(ref), dest, rt.ST_REFUSED, exc.code)
        raise
    except Exception as exc:
        audit(rt.REJECTED, type(exc).__name__)
        raise
    try:
        ok, error, sent = await router._send_parts(dest, [text])
    except Exception as exc:
        audit(rt.REJECTED, type(exc).__name__)
        raise
    if ok:
        audit(rt.ST_SENT, "tg " + (",".join(str(m) for m in sent[rt.REF_MESSAGE_IDS]) or "-"))
    else:
        audit(rt.ST_FAILED, str(error))
    router._log_send(kind, dict(ref), dest, rt.ST_SENT if ok else rt.ST_FAILED, error, sent)
    return {"ok": ok, "message_preview": text, "parts": 1, "error": error}


def query_trades(
    conn: sqlite3.Connection, d: date, state: str, live: Callable[[int], Mapping[str, Any]] | None = None
) -> dict[str, Any]:
    """TRADE-TRACKER-SPEC §8: trade rows + their events (ordered by ts_ms, id).

    state "open" = every live trade of any entry date (rules.OPEN_STATES: V1 OPEN / T1_TRAIL + the V2 Scanner's
    Z_OPEN; a swing stays open for days); "all" = entered on `d`, closed on `d`, or still open. Each row also carries
    `ltp` (live, or null) and `pnl_pct` (exit price when closed, else the live LTP, vs the entry; null when neither is
    known) and its entry signal's `tier` and `scanner`.
    Both editions: trades of the live scanners only (LIVE_SCANNERS = V1 + V2T; shadow scanners are never tracked,
    PLAN-W15 §2, and a trade whose entry signal is an owner-only scanner's never shows here). Client: E1 trades only
    (the D87 gated filter is gone, D114). A store without the v4 tables answers no rows.
    T-267 trade view (PLAN-T267 section 1, = tracker._view, the WS push): `scanner` (trades.scanner, else the entry
    signal's, else V1), `stop` {top, bottom, confirmed_ms} | null (the 1h stop zone in force; null = a V1 trade or a
    NO_ZONE V2T trade), `steps` [trade_zone_steps rows in seq order; [] for a V1 trade] and `und_entry` (the
    underlying's price at entry, or null).
    """
    from gammad.store import db

    lo, hi = _day_ms(d)
    placeholders = ",".join("?" * len(TRADE_STATES_OPEN))
    if state == "open":
        where, params = f"t.state IN ({placeholders})", list(TRADE_STATES_OPEN)
    else:
        where = f"(t.entry_date = ? OR (t.exit_ts_ms >= ? AND t.exit_ts_ms < ?) OR t.state IN ({placeholders}))"
        params = [d.isoformat(), lo, hi, *TRADE_STATES_OPEN]
    live_in, scanners = _live_in(), live_scanners()  # 0.8.5 (PLAN-D287 I7): read at CALL time (the client: V1 only)
    # 0.8.9 T-419: + the Still Watching trades (trades.scanner SW; their entry signal is a V1 row), both editions
    where += (f" AND (s.scanner IS NULL OR s.scanner IN ({live_in}))"
              f" AND (COALESCE(t.scanner, ?) IN ({live_in}) OR t.scanner = ?)")  # fmt: skip
    params.extend([*scanners, LIVE_SCANNER, *scanners, SW_TRADE_SCANNER])
    if is_client():
        kinds = _edition.visible_kinds()
        where += f" AND t.kind IN ({','.join('?' * len(kinds))})"
        params.extend(kinds)
    sql = (
        "SELECT t.*, s.tier AS tier, s.scanner AS entry_scanner, s.payload_json AS entry_payload_json"
        f" FROM trades t LEFT JOIN signals s ON s.id = t.entry_signal_id WHERE {where}"
        " ORDER BY t.entry_ts_ms DESC, t.id DESC"
    )
    try:
        trades = [dict(r) for r in conn.execute(sql, params).fetchall()]
    except sqlite3.OperationalError as exc:
        if "no such table" in str(exc):
            return {"date": d.isoformat(), "state": state, "rows": []}
        raise
    events: dict[int, list[dict[str, Any]]] = {}
    ids = [int(t["id"]) for t in trades]
    if ids:
        q = f"SELECT * FROM trade_events WHERE trade_id IN ({','.join('?' * len(ids))}) ORDER BY ts_ms, id"
        for e in conn.execute(q, ids).fetchall():
            events.setdefault(int(e["trade_id"]), []).append(dict(e))
    zone_ids = [int(t["id"]) for t in trades if t.get("scanner") == _rules.V2T]
    steps = db.zone_steps_by_trade(conn, zone_ids) if zone_ids else {}
    for t in trades:
        entry_scanner = t.pop("entry_scanner", None)
        t["scanner"] = t.get("scanner") or entry_scanner or LIVE_SCANNER
        stop = _rules.stop_zone_of(t.get("stop_top"), t.get("stop_bottom"), t.get("stop_confirmed_ms"))
        t["stop"] = None if stop is None else {"top": stop.top, "bottom": stop.bottom, "confirmed_ms": stop.confirmed_ms}
        t["steps"] = steps.get(int(t["id"]), [])
        t["und_entry"] = t.get("und_entry")
        # W17 T-170 (PLAN-W17 §2.1): the contract's name, from the ONE resolver the tracker's WS push uses too
        t["symbol"], t["strike"] = resolve_contract(
            conn, int(t["security_id"]), t.get("entry_date"), t.get("expiry_date"), t.get("option_type"),
            t.pop("entry_payload_json", None),
        )  # fmt: skip
        ltp = live(int(t["security_id"])).get("ltp") if live is not None else None
        t["ltp"] = ltp
        px = t.get("exit_px") if t.get("exit_px") is not None else ltp
        entry = t.get("entry")
        t["pnl_pct"] = round((float(px) / float(entry) - 1.0) * 100.0, 2) if px is not None and entry else None
        t["events"] = events.get(int(t["id"]), [])
        if t["scanner"] == SW_TRADE_SCANNER:  # T-419: the Still Watching levels (schema 11; read defensively)
            for col in SW_TRADE_LEVELS:
                t[col] = t.get(col)
            if is_client():  # D437: the client sees the list's name, never the internal scanner / hold word
                from gammad.alerts.router import client_hold

                t["scanner"] = t["list_name"] = CLIENT_STILL_WATCHING
                t["hold"] = client_hold(t.get("hold"))
    return {"date": d.isoformat(), "state": state, "rows": trades}


# ------------------------------------------------------------------------------------------ owner scanner queries
# Owner edition only: the routes answer 404 in the client edition BEFORE any of these runs, and gammad.scan (absent
# from the client ZIP) is imported inside them.


def query_scanner_signals(
    conn: sqlite3.Connection,
    d: date,
    live: Callable[[int], Mapping[str, Any]] | None = None,
    now_ms: int | None = None,
) -> dict[str, Any]:
    """Every E1 of every scanner on `d`, newest first, each with its scanner_grades fields (null when ungraded) and
    `trigger` = the whole trigger (T-156, gammad.scan.review.trigger_fields): build OI from the day's watchlist row of
    the same contract + list + expiry (retired rows included: the signal fired while it was live). 0.8.2 (C.9): `fut`
    = gammad.scan.api.fut_columns of the row, null when it has none. 0.8.3 C.5: `group_block` (`_group_block`)."""
    from gammad.alerts.router import NOT_HELD_PENDING_SQL
    from gammad.scan.api import fut_columns
    from gammad.scan.review import trigger_fields

    # T-310 review M1: a V1 row held for the 09:20 list post is not shown until its release. 0.8.5 (D288 Q6): a V1
    # tiered E2 is the group's V1 entry, listed (with its kind) beside the E1s
    stored = conn.execute(
        "SELECT * FROM signals WHERE trade_date = ? AND (kind = 'E1' OR (scanner = ? AND kind = ? AND tier IS NOT NULL))"
        + NOT_HELD_PENDING_SQL + " ORDER BY ts_ms DESC, id DESC",
        (d.isoformat(), LIVE_SCANNER, _edition.KIND_E2),
    ).fetchall()
    memo: dict[tuple[Any, ...], Any] = {}
    rows = [_api_signal_row(r) | {"group_block": _group_block(conn, r, memo, stored_signal=True)} for r in stored]
    grades: dict[int, dict[str, Any]] = {}
    if rows:
        ids = [int(r["id"]) for r in rows]
        q = f"SELECT * FROM scanner_grades WHERE signal_id IN ({','.join('?' * len(ids))})"
        grades = {int(g["signal_id"]): dict(g) for g in conn.execute(q, ids).fetchall()}
    for r in rows:
        g = grades.get(int(r["id"]), {})
        signal_sl = r.get("sl")
        r.update({k: g.get(k) for k in GRADE_FIELDS})
        if r["sl"] is None:  # ungraded: the signal's own SL (the grade copies it at on_signal)
            r["sl"] = signal_sl
        r["symbol"] = g.get("symbol") or r["payload"].get("symbol")
    build = {
        (int(b[0]), str(b[1]), str(b[2])): b[3]
        for b in conn.execute(
            "SELECT security_id, list_name, expiry_date, oi FROM watchlist WHERE trade_date = ? ORDER BY run_id",
            (d.isoformat(),),
        ).fetchall()
    }
    now = int(time.time() * 1000) if now_ms is None else now_ms
    fut = fut_columns(conn, [int(r["id"]) for r in rows])
    for r in rows:
        key = (int(r["security_id"]), str(r.get("list_name")), str(r.get("expiry_date")))
        lv = live(int(r["security_id"])) if live is not None else {}
        r["trigger"] = trigger_fields(r, build_oi=build.get(key), live=lv or {}, now_ms=now)
        r["fut"] = fut.get(int(r["id"]))
    return {"date": d.isoformat(), "rows": rows}


def _owner_ext_routes(app: Any, runtime: RuntimeAPI) -> bool:
    """0.8.2 (C.9): register the owner extension's routes (gammad.scan, never in a client package) on `app`, in the
    OWNER edition only: the client edition never imports it. ImportError-guarded (the app.py _make_grader pattern), so
    an owner tree without gammad/scan still starts. Called before the SPA catch-all. True when registered."""
    if is_client():
        return False
    try:
        from gammad.scan.api import register_owner_routes
    except ImportError:  # CLIENT_EXCLUDE: the module is not in the package
        log.warning("owner extension routes unavailable: gammad.scan.api did not import")
        return False
    register_owner_routes(app, runtime)
    return True


def scanner_signals_csv(rows: list[dict[str, Any]]) -> str:
    """T-156: /api/scanner/signals?format=csv, one line per signal: identity + grade + the trigger columns."""
    import csv
    import io

    from gammad.scan.review import TRIGGER_FIELDS, trigger_csv_cell

    head = ("id", "scanner", "tier", "tier_code", "trade_date", "ts_ms", "symbol", "list_name",
            "expiry_date", "kind", "ltp")
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    w.writerow((*head, *GRADE_FIELDS, *TRIGGER_FIELDS))
    for r in rows:
        c = {**r, **export_tier_cells(r.get("tier"))}
        t = r.get("trigger") or {}
        w.writerow([trigger_csv_cell(c.get(k)) for k in (*head, *GRADE_FIELDS)]
                   + [trigger_csv_cell(t.get(k)) for k in TRIGGER_FIELDS])  # fmt: skip
    return buf.getvalue()


def query_scanner_daily(conn: sqlite3.Connection, days: int) -> dict[str, Any]:
    """scanner_daily rows of the last `days` trade dates that have one, newest date first."""
    dates = [r[0] for r in conn.execute(
        "SELECT DISTINCT trade_date FROM scanner_daily ORDER BY trade_date DESC LIMIT ?", (days,)
    ).fetchall()]  # fmt: skip
    rows: list[dict[str, Any]] = []
    if dates:
        q = f"SELECT * FROM scanner_daily WHERE trade_date IN ({','.join('?' * len(dates))})"
        rows = [dict(r) for r in conn.execute(q + " ORDER BY trade_date DESC, scanner, tier", dates).fetchall()]
    from gammad.scan.review import hit_rates

    return {"days": days, "rows": rows, **hit_rates(conn, days)}


def query_scanner_weekly(conn: sqlite3.Connection, today: date, weeks: int) -> dict[str, Any]:
    """The last `weeks` Monday..Sunday weeks (this one first), each as gammad.scan.grader.weekly_summary; a week with
    no graded signal is left out."""
    from gammad.scan.grader import weekly_summary

    monday = today - timedelta(days=today.weekday())
    out = []
    for i in range(weeks):
        wk = weekly_summary(conn, monday - timedelta(days=7 * i) + timedelta(days=6))
        if wk["rows"]:
            out.append(wk)
    return {"weeks": weeks, "rows": out}


def query_scanner_today(
    conn: sqlite3.Connection, today: date, now_ms: int, live: Callable[[int], Mapping[str, Any]] | None = None
) -> dict[str, Any]:
    """Today's per scanner x tier summary, computed live from scanner_grades (scanner_daily-shaped), + the signals."""
    from gammad.scan.grader import daily_rows

    return {
        "date": today.isoformat(),
        "summary": daily_rows(conn, [today.isoformat()], now_ms),
        "signals": query_scanner_signals(conn, today, live, now_ms)["rows"],
    }


def query_token_events(conn: sqlite3.Connection, n: int = 20) -> list[dict[str, Any]]:
    rows = conn.execute("SELECT id, ts_ist, kind, detail FROM token_events ORDER BY id DESC LIMIT ?", (n,)).fetchall()
    return [{**dict(r), "detail": redact(str(r["detail"])) if r["detail"] is not None else None} for r in rows]


def export_text(
    conn: sqlite3.Connection,
    what: str,
    fmt: str,
    d: date,
    list_name: str | None,
    expiry_date: str | None,
    item_id: int | None,
    live: Callable[[int], Mapping[str, Any]],
    *,
    still_watching: bool = True,
) -> str | None:
    """D44 copy text. None = the requested single row does not exist. The CLIENT edition: `client_export_text`."""
    from gammad.alerts import templates as tpl
    from gammad.alerts.router import fetch_signals, fetch_watchlist_rows

    if is_client():
        return client_export_text(conn, what, fmt, d, list_name, expiry_date, item_id, live,
                                  still_watching=still_watching)  # fmt: skip
    if what == "watchlist":
        rows = fetch_watchlist_rows(
            conn, d.isoformat(), list_name=list_name, expiry_date=expiry_date,
            security_id=item_id if fmt == "line" else None,
        )  # fmt: skip
        for r in rows:
            ltp = live(int(r["security_id"])).get("ltp")
            if ltp is not None:
                r["ltp"] = ltp
            r["zone_dist_pct"] = tpl.zone_dist_pct(r)
        columns = tpl.WATCHLIST_EXPORT_COLUMNS
    else:
        # scanner=None: every row, filtered here to the live scanners (router.fetch_signals defaults to V1 alone)
        rows = [r for r in fetch_signals(conn, d.isoformat(), scanner=None)
                if (r.get("scanner") or LIVE_SCANNER) in live_scanners() and signal_visible(r.get("kind"))]  # fmt: skip
        if list_name:
            rows = [r for r in rows if r.get("list_name") == list_name]
        if expiry_date:
            rows = [r for r in rows if r.get("expiry_date") == expiry_date]
        if fmt == "line" and item_id is not None:
            rows = [r for r in rows if int(r["id"]) == item_id]
        for r in rows:  # gate_reasons is stored comma-joined; the TSV cell keeps that plain form
            r["gate_reasons"] = r.get("gate_reasons") or ""
            if fmt == "tsv":
                r.update(export_tier_cells(r.get("tier")))
        columns = signal_export_columns()
    if fmt == "tsv":
        return tpl.tsv(rows, columns)
    if item_id is not None and not rows:
        return None
    return "\n".join(tpl.row_line(r) for r in (rows[:1] if item_id is not None else rows))


def client_export_text(
    conn: sqlite3.Connection,
    what: str,
    fmt: str,
    d: date,
    list_name: str | None,
    expiry_date: str | None,
    item_id: int | None,
    live: Callable[[int], Mapping[str, Any]],
    *,
    still_watching: bool = True,
) -> str | None:
    """T-376 (the 0.8.8 client leak; D429 final (a) / D433 / D87): the CLIENT edition's D44 copy text. The rows are
    the ones the page shows -- `query_signals` (a past day keeps its EXPIRY entries: owner ruling 13 Oct 2026) and
    `query_watchlists(client=True)` (no EXPIRY list, carried rows as "Still Watching", T-419; live-merged) -- and the
    words are the dashboard's
    client ones: a line = textfmt.ts signalLine / watchLine (client build) byte for byte (tpl.client_signal_line /
    client_watch_line), a table = tpl.CLIENT_SIGNAL_EXPORT_COLUMNS / CLIENT_WATCHLIST_EXPORT_COLUMNS. Signals stay in
    time order (oldest first), as the owner export. None = the requested single row does not exist."""
    from gammad.alerts import templates as tpl

    single = fmt == "line" and item_id is not None
    if what == "watchlist":
        rows = [r for lst in query_watchlists(conn, d, lambda sid: dict(live(sid)), client=True,
                                              still_watching=still_watching)["lists"]
                for r in lst["rows"]
                if (list_name is None or r.get("list_name") == list_name)
                and (expiry_date is None or r.get("expiry_date") == expiry_date)
                and (not single or int(r["security_id"]) == item_id)]  # fmt: skip
        if fmt == "tsv":
            return tpl.tsv([tpl.client_watch_export_row(r, d.isoformat()) for r in rows],
                           tpl.CLIENT_WATCHLIST_EXPORT_COLUMNS)  # fmt: skip
        lines = [tpl.client_watch_line(r) for r in rows]
    else:
        rows = [r for r in reversed(query_signals(conn, d, SIGNALS_LIMIT_MAX)["rows"])
                if (not list_name or r.get("list_name") == list_name)
                and (not expiry_date or r.get("expiry_date") == expiry_date)
                and (not single or int(r["id"]) == item_id)]  # fmt: skip
        if fmt == "tsv":
            return tpl.tsv([tpl.client_signal_export_row(r) for r in rows], tpl.CLIENT_SIGNAL_EXPORT_COLUMNS)
        lines = [tpl.client_signal_line(r) for r in rows]
    if item_id is not None and not rows:
        return None
    return "\n".join(lines[:1] if item_id is not None else lines)


def tail_lines(path: Path, n: int) -> list[str]:
    if not path.exists():
        return []
    with path.open(encoding="utf-8", errors="replace") as fh:
        return [redact(line.rstrip("\n")) for line in deque(fh, maxlen=n)]


# ------------------------------------------------------------------------------------------ app
def create_app(runtime: RuntimeAPI, *, dist_dir: Path | None = None) -> Any:
    from starlette.middleware.trustedhost import TrustedHostMiddleware

    from gammad.api import lan

    dist = dist_dir or DEFAULT_DIST
    # W17 T-171 (review R4): the running commit is resolved HERE, once -- never on a status call (the WS path)
    build = build_info.BuildInfo(build_info.resolve_commit(REPO_ROOT), dist)
    app = FastAPI(title="GAMMA-UNWIND", docs_url=None, redoc_url=None, openapi_url=None)
    # T-129 (PLAN-W16 13.3 B1): the remote-request guard + /api/session*, registered before every route (the SPA
    # catch-all below must not shadow them) and added before TrustedHost, so TrustedHost stays the outer check.
    lan.install(app, runtime=runtime)
    # D98(3): a page on another origin that re-binds its DNS name to 127.0.0.1 still sends ITS host name: refused.
    # T-129: this PC's private address is accepted too, ONLY while the LAN bind is on (GAMMA_LAN=1 + a password).
    app.add_middleware(TrustedHostMiddleware, allowed_hosts=[*TRUSTED_HOSTS, *lan.extra_trusted_hosts(runtime)])

    def _date(s: str | None) -> date:
        if not s:
            return runtime.today()
        try:
            return date.fromisoformat(s)
        except ValueError:
            raise HTTPException(status_code=422, detail=f"date must be YYYY-MM-DD, got {s!r}") from None

    def _live(security_id: int, entered: bool = False) -> dict[str, Any]:
        row = dict(runtime.live_row(security_id))
        row["no_feed"] = ("NSE_FNO", security_id) in (getattr(runtime, "feed_refused", None) or {})  # 0.8.5 A1
        if "engine_state" in row:
            row["engine_state"] = project_state(row["engine_state"], entered)
        return row

    @app.get("/api/brokers/catalog")
    def broker_catalog() -> dict[str, Any]:
        """Read-only broker registry metadata. Never return configured secret values."""
        from gammad.broker import registry

        active = broker_entry(runtime)
        return {"active": active.name if active else None, "brokers": [
            {"name": key, "label": ent.label, "status": ent.caps.status,
             "active": bool(active and active.name == key),
             "token_key": ent.token_key,
             "login_fields": list(ent.client_login.fields) if ent.client_login else [],
             "setup_keys": [k.name for k in ent.env_keys if k.name != ent.token_key
                            and k.need != "auto_login"],
             "token_expiry_rule": ent.caps.token_expiry_rule}
            for key, ent in registry.BROKERS.items()
        ]}

    @app.post("/api/brokers/stage")
    async def broker_stage(request: Request) -> Any:
        """Local-PC-only config selection. Stage BROKER in .env; restart required. No secret editing."""
        from gammad.api.broker_selection import stage_broker
        from gammad.broker import registry
        from gammad.api import lan

        if lan.is_remote(request.scope) or request.client is None or request.client.host not in ("127.0.0.1", "::1"):
            return JSONResponse({"error": "local PC only"}, status_code=403)
        expected_origin = f"http://{request.headers.get('host', '')}"
        if request.headers.get("origin") != expected_origin or request.headers.get(CONTROL_HEADER) != "1":
            return JSONResponse({"error": "same-origin control required"}, status_code=403)
        if request.headers.get("content-type", "").split(";", 1)[0].strip().lower() != "application/json":
            return JSONResponse({"error": "JSON required"}, status_code=415)
        try:
            raw = await request.body()
            if len(raw) > 256:
                return JSONResponse({"error": "invalid selection"}, status_code=400)
            obj = json.loads(raw)
            name = obj.get("broker") if isinstance(obj, dict) else None
            if not isinstance(name, str) or name not in registry.BROKERS:
                return JSONResponse({"error": "unknown broker"}, status_code=400)
            backup = stage_broker(REPO_ROOT / ".env", name)
        except (OSError, ValueError):
            log.warning("broker selection staging failed (no secrets logged)")
            return JSONResponse({"error": "could not stage selection"}, status_code=409)
        return {"ok": True, "staged": name, "restart_required": True, "backup_made": bool(backup)}

    # GAMMA-DUAL-USER-SELECTIONS-R10B: save user inputs, never generated strategy watchlists.
    # Local-PC only. Neither route triggers a scan, feed, quote, option-chain or order.
    @app.get("/api/user-symbols")
    def user_symbols_get(request: Request) -> Any:
        from gammad.api import lan
        from gammad.store import user_selections

        if lan.is_remote(request.scope) or request.client is None or request.client.host not in ("127.0.0.1", "::1"):
            return JSONResponse({"error": "local PC only"}, status_code=403)
        try:
            with _read(runtime) as conn:
                return {**user_selections.load(conn), "active_for_scanning": False}
        except (sqlite3.Error, user_selections.SelectionError):
            return JSONResponse({"error": "selection store unavailable"}, status_code=503)

    @app.post("/api/user-symbols")
    async def user_symbols_post(request: Request) -> Any:
        from gammad.api import lan
        from gammad.store import user_selections

        if lan.is_remote(request.scope) or request.client is None or request.client.host not in ("127.0.0.1", "::1"):
            return JSONResponse({"error": "local PC only"}, status_code=403)
        if (request.headers.get("origin") != f"http://{request.headers.get('host', '')}"
                or request.headers.get(CONTROL_HEADER) != "1"):
            return JSONResponse({"error": "same-origin control required"}, status_code=403)
        if request.headers.get("content-type", "").split(";", 1)[0].strip().lower() != "application/json":
            return JSONResponse({"error": "JSON required"}, status_code=415)
        if request.headers.get("content-length"):
            try:
                if int(request.headers["content-length"]) > 8192:
                    return JSONResponse({"error": "invalid selection"}, status_code=413)
            except ValueError:
                return JSONResponse({"error": "invalid selection"}, status_code=400)
        raw = await request.body()
        if len(raw) > 8192:
            return JSONResponse({"error": "invalid selection"}, status_code=413)
        try:
            obj = json.loads(raw)
            if not isinstance(obj, dict) or set(obj) != {"revision", "stocks", "indices"}:
                return JSONResponse({"error": "invalid selection"}, status_code=422)
            with _read(runtime) as conn:
                saved = user_selections.save(conn, expected_revision=obj["revision"],
                                             stocks=obj["stocks"], indices=obj["indices"])
            return {**saved, "active_for_scanning": False}
        except user_selections.SelectionConflict:
            return JSONResponse({"error": "selection revision conflict; refresh first"}, status_code=409)
        except (json.JSONDecodeError, user_selections.SelectionError, TypeError, ValueError):
            return JSONResponse({"error": "invalid selection"}, status_code=422)
        except sqlite3.Error:
            return JSONResponse({"error": "selection store unavailable"}, status_code=503)

    @app.get("/api/status")
    def status() -> dict[str, Any]:
        return status_body(runtime, build=build)

    @app.get("/api/calendar")
    def calendar() -> dict[str, Any]:
        from gammad.core.calendar import default_calendar

        try:
            return calendar_body(default_calendar())
        except (OSError, ValueError, TypeError):
            log.warning("Trading calendar unavailable; using weekdays only")
            return {"years": [], "holidays": []}

    @app.get("/api/session-hours")
    def session_hours(date: str | None = None) -> dict[str, Any]:
        from gammad.core.sessions import SessionsError

        d = _date(date)
        try:
            return session_hours_body(d)
        except SessionsError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from None

    @app.get("/api/levels")
    def levels(date: str | None = None) -> dict[str, Any]:
        d = _date(date)
        with _read(runtime) as conn:
            return client_rows(query_levels(conn, d))

    @app.get("/api/watchlists")
    def watchlists(date: str | None = None) -> dict[str, Any]:
        d = _date(date)
        with _read(runtime) as conn:
            # engine states are TODAY's: the ENTERED evidence is today's visible E1s (D98(5))
            entered = entered_ids(conn, runtime.today()) if is_client() else set()
            return client_rows(query_watchlists(conn, d, lambda sid: _live(sid, sid in entered), client=is_client(),
                                                still_watching=still_watching_of(runtime)))  # fmt: skip

    @app.get("/api/trades")
    def trades(date: str | None = None, state: str = "all") -> dict[str, Any]:
        if state not in ("open", "all"):
            raise HTTPException(status_code=422, detail="state must be open or all")
        d = _date(date)
        with _read(runtime) as conn:
            return client_rows(query_trades(conn, d, state, runtime.live_row))

    @app.get("/api/signals")
    def signals(date: str | None = None, limit: int = Query(200, ge=1, le=SIGNALS_LIMIT_MAX)) -> dict[str, Any]:
        d = _date(date)
        with _read(runtime) as conn:
            return client_rows(query_signals(conn, d, limit))

    # -- PLAN-W15 §2 owner-only scanner routes: 404 in the client edition, checked before anything is read.
    def _owner_only() -> None:
        if is_client():
            raise HTTPException(status_code=404)

    @app.get("/api/scanner/today")
    def scanner_today() -> dict[str, Any]:
        _owner_only()
        with _read(runtime) as conn:
            return query_scanner_today(conn, runtime.today(), int(time.time() * 1000), runtime.live_row)

    @app.get("/api/scanner/daily")
    def scanner_daily(days: int = Query(20, ge=1, le=SCANNER_DAYS_MAX)) -> dict[str, Any]:
        _owner_only()
        with _read(runtime) as conn:
            return query_scanner_daily(conn, days)

    @app.get("/api/scanner/weekly")
    def scanner_weekly(weeks: int = Query(8, ge=1, le=SCANNER_WEEKS_MAX)) -> dict[str, Any]:
        _owner_only()
        with _read(runtime) as conn:
            return query_scanner_weekly(conn, runtime.today(), weeks)

    @app.get("/api/scanner/signals")
    def scanner_signals(date: str | None = None, format: str = "json") -> Any:
        _owner_only()
        if format not in ("json", "csv"):
            raise HTTPException(status_code=422, detail="format must be json or csv")
        d = _date(date)
        with _read(runtime) as conn:
            body = query_scanner_signals(conn, d, runtime.live_row)
        if format == "csv":
            return PlainTextResponse(
                scanner_signals_csv(body["rows"]),
                media_type="text/csv",
                headers={"Content-Disposition": f'attachment; filename="scanner-signals-{d.isoformat()}.csv"'},
            )
        return body

    _owner_ext_routes(app, runtime)  # 0.8.2 (C.9): owner edition only; before the SPA catch-all below
    s089.register(app, runtime)  # 0.8.9 D648 (3): /api/s089/* (owner-only routes not registered in the client)

    @app.get("/api/contract/{security_id}")
    def contract(security_id: int, tf: int = 5, date: str | None = None) -> dict[str, Any]:
        if tf not in (1, 5, 15):
            raise HTTPException(status_code=422, detail="tf must be 1, 5 or 15")
        d = _date(date)
        view = runtime.contract_view(security_id, tf)
        # W17 T-170 (PLAN-W17 §2.2): live bars only for today (contract_view ignores `date`); otherwise, or when the
        # runtime builds no bars for this id (an open trade off today's lists), the recorded ticks are replayed
        live_bars = list(view["bars"]) if view is not None and d == runtime.today() else []
        with _read(runtime) as conn:
            oi = query_oi_series(conn, security_id, d)
            known = conn.execute("SELECT 1 FROM watchlist WHERE security_id = ? LIMIT 1", (security_id,)).fetchone()
            entered = is_client() and security_id in entered_ids(conn, runtime.today())
            named = query_contract_name(conn, security_id, d)
            recorded = [] if live_bars else replay_bars(
                data_dir_of(runtime) / "parquet", conn, security_id, d, tf, use_parquet=d != runtime.today()
            )
        if view is None and known is None and not oi and not recorded:
            raise HTTPException(status_code=404, detail=f"unknown contract {security_id}")
        view = view or {
            "bars": [],
            "engine": dict.fromkeys(
                ("state", "a", "b", "c", "oi_drop_pct", "vol_ratio", "consol_lo", "consol_hi", "consol_mid")
            ),
        }
        engine = client_engine(view["engine"], entered) if is_client() else view["engine"]
        source = "live" if live_bars else "recorded" if recorded else "none"
        return {"bars": live_bars or recorded, "oi_series": oi, "engine": engine, "contract": named,
                "bars_source": source}  # fmt: skip

    @app.get("/api/token")
    def token_get() -> dict[str, Any]:
        with _read(runtime) as conn:
            events = query_token_events(conn)
        return {**runtime.token_status(), "events": events, **token_page(runtime), "chats": chats_block(runtime)}

    @app.post("/api/token")
    async def token_post(access_token: str = Body(..., embed=True, min_length=1)) -> Any:
        updater = runtime.token_updater
        if updater is None:
            return JSONResponse({"detail": "token updater not available"}, status_code=501)
        try:
            result = updater(access_token.strip())
            if inspect.isawaitable(result):
                result = await result
        except (TokenRejected, ValueError) as exc:
            log.warning("POST /api/token rejected: %s", redact(str(exc)))
            return JSONResponse({"error": "token rejected"}, status_code=400)
        except Exception:
            log.exception("POST /api/token: updater failed")
            return JSONResponse({"error": "token update failed"}, status_code=500)
        return {"ok": result is not False, "token": runtime.token_status()}

    @app.post("/api/token/login")
    async def token_login(request: Request) -> Any:
        """D92 paste/typed login. The raw body is parsed HERE (not by a pydantic model) so a malformed
        request can never be echoed back in a 422 validation error: every error names keys only.
        D98(3): like /api/service/stop, `X-Gamma-Control: 1` (403) and a JSON body (415) are required -- the
        custom header and the JSON content type both force a CORS preflight, so no other web page can post here."""
        if request.headers.get(CONTROL_HEADER) != "1":
            return JSONResponse({"error": "X-Gamma-Control: 1 header required"}, status_code=403)
        ctype = request.headers.get("content-type", "").split(";", 1)[0].strip().lower()
        if ctype != "application/json":
            return JSONResponse({"error": "Content-Type application/json required"}, status_code=415)
        entry = broker_entry(runtime)
        cl = getattr(entry, "client_login", None) if entry is not None else None
        if cl is None:
            return JSONResponse({"error": "this broker has no paste/typed login"}, status_code=404)
        try:
            body = json.loads(await request.body() or b"null")
        except ValueError:
            return JSONResponse({"error": "body must be JSON {\"fields\": {...}}"}, status_code=400)
        fields = body.get("fields") if isinstance(body, dict) else None
        expected = sorted(cl.fields)
        if not isinstance(fields, dict) or sorted(fields) != expected or not all(
            isinstance(v, str) and v.strip() for v in fields.values()
        ):
            return JSONResponse({"error": "fields mismatch", "expected": expected}, status_code=400)
        login = getattr(runtime, "token_login", None)
        if not callable(login):
            return JSONResponse({"detail": "token login not available"}, status_code=501)
        try:
            result = login(fields)
            if inspect.isawaitable(result):
                await result
        except LookupError:
            return JSONResponse({"error": "this broker has no paste/typed login"}, status_code=404)
        except (TokenRejected, ValueError, TypeError) as exc:
            log.warning("POST /api/token/login rejected (%s)", type(exc).__name__)  # never the message
            return JSONResponse({"error": "login rejected"}, status_code=400)
        except Exception as exc:  # noqa: BLE001 -- no traceback: a frame could carry the typed values
            log.error("POST /api/token/login: login failed (%s)", type(exc).__name__)
            return JSONResponse({"error": "login failed"}, status_code=500)
        return {"ok": True, "token": runtime.token_status()}

    @app.post("/api/token/renew")
    async def token_renew(request: Request) -> Any:
        _owner_only()
        if request.headers.get(CONTROL_HEADER) != "1":
            return JSONResponse({"error": "X-Gamma-Control: 1 header required"}, status_code=403)
        hook = getattr(runtime, "token_renew", None)
        if not callable(hook):
            return JSONResponse({"detail": "token renew not available"}, status_code=501)
        from gammad.auth.token_watch import RenewBusy

        try:
            result = hook()
            if inspect.isawaitable(result):
                await result
        except _edition.OwnerOnlyError as exc:
            log.warning("POST /api/token/renew refused (%s)", type(exc).__name__)
            raise HTTPException(status_code=404) from None
        except (LookupError, RenewBusy, TokenRejected, ValueError) as exc:
            log.warning("POST /api/token/renew refused (%s)", type(exc).__name__)
            if isinstance(exc, LookupError):
                code, error = 409, "automatic renewal is not set up"
            elif isinstance(exc, RenewBusy):
                code, error = 409, "a renewal is already running"
            else:
                code, error = 400, "renewal refused"
            return JSONResponse({"error": error}, status_code=code)
        except Exception as exc:  # noqa: BLE001 -- do not expose exception messages or tracebacks
            log.error("POST /api/token/renew failed (%s)", type(exc).__name__)
            return JSONResponse({"error": "renewal failed"}, status_code=500)
        return {"ok": True, "token": runtime.token_status()}

    @app.post("/api/token/auto-renew")
    async def token_auto_renew(request: Request) -> Any:
        _owner_only()
        if request.headers.get(CONTROL_HEADER) != "1":
            return JSONResponse({"error": "X-Gamma-Control: 1 header required"}, status_code=403)
        try:
            payload = await request.json()
        except ValueError:
            raise HTTPException(status_code=422, detail="JSON object with enabled: bool required") from None
        enabled = payload.get("enabled") if isinstance(payload, dict) else None
        if type(enabled) is not bool:
            raise HTTPException(status_code=422, detail="enabled must be bool")
        from gammad.auth.token_watch import set_auto_renew_enabled

        with _read(runtime) as conn:
            stored = set_auto_renew_enabled(conn, enabled, source="dashboard")
        return {"ok": True, "auto_renew_enabled": stored, "auto_renew": token_page(runtime)["auto_renew"]}

    @app.post("/api/service/stop")
    def service_stop(request: Request) -> JSONResponse:
        """T-087 / D95(1): the dashboard's Stop button. The custom header forces a CORS preflight, so
        another web page cannot stop the service; the tray supervisor's data/STOP protocol does the rest."""
        if request.headers.get(CONTROL_HEADER) != "1":
            return JSONResponse({"error": "X-Gamma-Control: 1 header required"}, status_code=403)
        from gammad.tray.supervisor import STOP_FILE, _touch

        path = data_dir_of(runtime) / STOP_FILE
        _touch(path)
        log.info("service stop requested from the dashboard (%s written)", path.name)
        return JSONResponse({"stopping": True}, status_code=202)

    @app.post("/api/jobs/{name}/run")
    async def job_run(name: str, body: Annotated[dict[str, Any] | None, Body()] = None) -> dict[str, Any]:
        if name not in JOB_NAMES:
            raise HTTPException(status_code=404, detail=f"unknown job {name!r}")
        kind = (body or {}).get("kind")
        extra: dict[str, Any] = {"post": True} if (body or {}).get("post") else {}
        try:
            run_id = await runtime.run_job(name, str(kind) if kind is not None else None, **extra)
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from None
        return {"run_id": run_id}

    @app.post("/api/alerts/pause")
    def alerts_pause(paused: bool = Body(..., embed=True)) -> dict[str, Any]:
        runtime.set_paused(paused)
        return status_body(runtime, build=build)

    # -- D255: the per-install V2 Scanner ON / OFF switch (the same rules as /api/alerts/pause: JSON body, the LAN guard
    #    of every non-GET route). The router gates NEW V2T entries on db.get_v2_enabled. 0.8.5 (D287(6), PLAN-D287 I7):
    #    OWNER only -- 404 in the client edition (`_owner_only`, before anything is read or written).
    @app.get("/api/v2-scanner")
    def v2_scanner_get() -> dict[str, bool]:
        _owner_only()
        return v2_switch(runtime)

    @app.post("/api/v2-scanner")
    def v2_scanner_set(enabled: Annotated[bool, Depends(v2_switch_body)]) -> dict[str, bool]:
        from gammad.store import db

        _owner_only()
        with _read(runtime) as conn:
            db.set_v2_enabled(conn, enabled, source="dashboard")
        out = v2_switch(runtime)
        runtime.hub.publish("v2_scanner", out)
        return out

    @app.get("/api/logs")
    def logs(lines: int = Query(200, ge=1, le=LOG_LINES_MAX)) -> dict[str, Any]:
        out = tail_lines(runtime.logs_dir / "gammad.log", lines)
        if is_client():
            out = [ln for ln in out if not CLIENT_LOG_HIDE.search(ln)]
        return {"lines": out}

    def _ref_hidden(kind: str, ref: Mapping[str, Any]) -> bool:
        """Client: a `signal` ref to an E2 / gated E1 answers 404 (CLIENT §2 preview/send rows). T-637: so does an s089
        signal ref (owner only, like `_owner_only`), before the runtime is reached."""
        if kind != "signal" or not is_client():
            return False
        if s089_signal_key(kind, ref) is not None:
            return True
        with _read(runtime) as conn:
            return not signal_ref_visible(conn, ref.get("id"))

    # -- D42-D44: manual sends, mutes, previews, copy/export. The runtime-backed routes are `async def`
    #    so they run on the loop that owns the router's SQLite connection (sqlite3 objects are thread-bound).

    @app.post("/api/send")
    async def send(
        request: Request,
        kind: str = Body(..., embed=True),
        ref: Annotated[dict[str, Any] | None, Body(embed=True)] = None,
        dest: str = Body("group", embed=True),
    ) -> dict[str, Any]:
        from gammad.alerts import router as rt

        who = rt.WHO_LAN if lan.is_remote(request.scope) else rt.WHO_LOCAL
        if _ref_hidden(kind, ref or {}):
            rt.manual_audit(kind, ref or {}, dest, who, rt.REJECTED, rt.HIDDEN_REF)
            raise HTTPException(status_code=404, detail="no such signal")
        try:
            return await runtime.send_manual(kind, ref or {}, dest, who=who)
        except (ValueError, TypeError) as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from None
        except LookupError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from None

    @app.get("/api/preview")
    async def preview(kind: str, ref: str = "{}", dest: str = "group") -> dict[str, Any]:
        if dest not in SEND_DESTS:  # 0.8.3 C.5: checked before anything reaches the runtime
            raise HTTPException(status_code=422, detail=f"dest must be one of {SEND_DESTS}, got {dest!r}")
        try:
            parsed = json.loads(ref)
        except ValueError:
            raise HTTPException(status_code=422, detail="ref must be a JSON object") from None
        if not isinstance(parsed, dict):
            raise HTTPException(status_code=422, detail="ref must be a JSON object")
        if _ref_hidden(kind, parsed):
            raise HTTPException(status_code=404, detail="no such signal")
        try:
            parts = runtime.preview(kind, parsed, dest=dest)  # W0-5: `dest` BY KEYWORD
        except (ValueError, TypeError) as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from None
        except LookupError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from None
        return {"html": parts[0], "parts": parts, "dest": dest}

    @app.post("/api/mute")
    async def mute(
        security_id: int = Body(..., embed=True),
        muted: bool = Body(..., embed=True),
        reason: str | None = Body(None, embed=True),
    ) -> dict[str, Any]:
        return {"ok": True, "muted": runtime.mute(security_id, muted, reason)}

    @app.get("/api/mutes")
    def mutes() -> dict[str, Any]:
        from gammad.alerts.router import fetch_mutes

        with _read(runtime) as conn:
            return {"rows": fetch_mutes(conn)}

    @app.get("/api/export")
    def export(
        what: str,
        format: str = "tsv",
        date: str | None = None,
        list_name: str | None = None,
        expiry_date: str | None = None,
        id: int | None = None,
    ) -> PlainTextResponse:
        if what not in ("watchlist", "signals"):
            raise HTTPException(status_code=422, detail="what must be watchlist or signals")
        if format not in ("tsv", "line"):
            raise HTTPException(status_code=422, detail="format must be tsv or line")
        d = _date(date)
        with _read(runtime) as conn:
            text = export_text(conn, what, format, d, list_name, expiry_date, id, runtime.live_row,
                               still_watching=still_watching_of(runtime))  # fmt: skip
        if text is None:
            raise HTTPException(status_code=404, detail=f"no {what} row {id} on {d.isoformat()}")
        return PlainTextResponse(text)

    @app.websocket("/ws")
    async def ws(websocket: WebSocket) -> None:
        await websocket.accept()
        q = runtime.hub.subscribe()
        try:
            await websocket.send_json({"type": "status", "data": status_body(runtime, build=build)})
            last = time.monotonic()
            while True:
                wait = max(0.0, STATUS_EVERY_S - (time.monotonic() - last))
                try:
                    msg = await asyncio.wait_for(q.get(), timeout=wait)
                except TimeoutError:
                    await websocket.send_json({"type": "status", "data": status_body(runtime, build=build)})
                    last = time.monotonic()
                    continue
                if not ws_visible(msg):
                    continue
                if msg.get("type") == "status" and isinstance(msg.get("data"), Mapping):
                    msg = {"type": "status", "data": status_body(runtime, msg["data"], build=build)}
                await websocket.send_json(client_rows(msg))  # D241(2): trade / signal pushes lose spec_version
        except (WebSocketDisconnect, ConnectionError, RuntimeError):
            pass
        finally:
            runtime.hub.unsubscribe(q)

    @app.get("/auth/{broker}/callback", include_in_schema=False)
    def oauth_landing(broker: str) -> Any:
        # D100: where a code broker's manual login lands (the guide's redirect URL). Static: the query (request
        # token / auth code) is never reflected or exchanged here; the client pastes the address on the Token page.
        return HTMLResponse(
            OAUTH_LANDING_HTML, headers={"Cache-Control": "no-store", "Referrer-Policy": "no-referrer"}
        )

    # GAMMA native independent Broker Manager R1.4: registered before SPA catch-all.
    if is_client():
        from gammad.broker.gamma_r14 import install_routes as _gamma_r14_install
        _gamma_r14_install(app, runtime)

    @app.get("/{full_path:path}", include_in_schema=False)
    def spa(full_path: str) -> Any:
        if full_path.startswith(("api/", "ws")) or full_path == "api":
            raise HTTPException(status_code=404)
        index = dist / "index.html"
        if not index.is_file():
            return HTMLResponse(NOT_BUILT_HTML)
        if full_path:
            candidate = (dist / full_path).resolve()
            if candidate.is_file() and candidate.is_relative_to(dist.resolve()):
                return FileResponse(candidate)
        return FileResponse(index)

    return app


def make_server(app: Any, port: int, host: str = "127.0.0.1", *, hosts: tuple[str, ...] | None = None) -> Any:
    """uvicorn.Server that runs inside an existing loop and leaves signal handling to the supervisor.

    T-129: `hosts` = gammad.api.lan.bind_hosts(env), one listening socket per address (serve() binds them itself, so
    the caller's `await server.serve()` is unchanged). Every address must be loopback or RFC1918: 0.0.0.0, ::, a name
    or a public address raises ValueError. A bind failure raises SystemExit, like uvicorn's own port-taken exit."""
    import socket

    import uvicorn

    from gammad.api import lan

    binds = tuple(hosts) if hosts else (host,)
    for h in binds:
        if not lan.bindable(h):
            raise ValueError(f"refusing to bind the dashboard to {h!r}: loopback or a private (RFC1918) address only")

    def _listen(h: str) -> socket.socket:
        s = socket.socket(socket.AF_INET6 if ":" in h else socket.AF_INET, socket.SOCK_STREAM)
        exclusive = getattr(socket, "SO_EXCLUSIVEADDRUSE", None)  # Windows: no other process may share the port
        s.setsockopt(socket.SOL_SOCKET, exclusive if exclusive is not None else socket.SO_REUSEADDR, 1)
        s.bind((h, port))
        return s

    class _Embedded(uvicorn.Server):
        @contextlib.contextmanager
        def capture_signals(self) -> Generator[None]:
            yield

        def install_signal_handlers(self) -> None:  # uvicorn < 0.29 name of the same hook
            return

        async def serve(self, sockets: list[socket.socket] | None = None) -> None:
            if sockets is not None or len(binds) == 1:
                await super().serve(sockets=sockets)
                return
            socks: list[socket.socket] = []
            try:
                for h in binds:
                    socks.append(_listen(h))
            except OSError as exc:
                for s in socks:
                    s.close()
                log.error("API could not bind %s:%d (%s)", h, port, type(exc).__name__)
                raise SystemExit(1) from None
            try:
                await super().serve(sockets=socks)
            finally:
                for s in socks:
                    with contextlib.suppress(OSError):
                        s.close()

    config = uvicorn.Config(app, host=binds[0], port=port, log_level="warning", access_log=False, lifespan="off")
    return _Embedded(config)
