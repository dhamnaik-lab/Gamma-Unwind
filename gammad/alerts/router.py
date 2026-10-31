"""Alert router (spec Step 4 "Alerts go to ..."): record every signal, then fan it out.

Order for an E1 / E2 signal (`on_signal(signal, meta, ctx, *, scanner="V1")`):
  0. W15 (S1.4.0, D114, PLAN-W15 §2): an E1 gets `tier = gates.tier(ctx, <router GatesCfg>)` -- ENTRY (4 gates
     passed), STRONG_WATCHLIST (3) or WATCHLIST (0-2; a missing input or a disabled gate is not a pass); an E2 is
     untiered (NULL). R4: one ENTRY per underlying (ContractMeta.symbol) per trade date for scanner V1 -- a later
     E1 that would be ENTRY is demoted to WATCHLIST with `dup_stock` appended to its gate_reasons.
     scanner != "V1" (an owner shadow scanner) -> `_record_shadow`: a signals row with its scanner + tier and a WS
     push of type `scanner`; NO alerts row, NO Telegram, NO trade hook (the client edition records none) -- except
     0.8.8 D382 (1): a V1 B E1 whose V1 verdict failed on zone alone gets ONE owner PERSONAL FYI card per contract-day.
  1. INSERT a `signals` row (always, even while paused, muted or gated): the payload_json has the Signal
     fields plus the caller's ContractMeta, trade_date and spec_version; the v3 columns carry the
     S1.1.0 context: hold (D58), gate_status / gate_reasons (D63) and ctx_json (SignalContext.to_dict());
     the v7 columns scanner + tier. ctx=None (a caller without a context) -> the gates.build_context of a
     signal with no spot and no volumes: hold from the list, zone + liquidity `unknown` (an E1 is WATCHLIST).
  2. Dashboard push (if a pusher is wired): the signal + muted + ctx / hold / gate_status / gate_reasons + tier /
     scanner / warnings (`tpl.warnings`, the expiry / late-day lines). Not rate limited, runs while paused /
     muted / gated: the dashboard is the record view. It writes an `alerts` row with channel 'dashboard'.
  3. Telegram GROUP, paced by sliding windows (1 msg/s per chat, 20 per rolling minute to the group).
     Decision order (D48; W15 replaces the D63 gate check for an E1 by its tier), one `alerts` row per chat:
        muted (D42)                        -> status 'muted'
        E1 tier WATCHLIST (D114(4))        -> status 'gated', error 'watchlist: zone,dup_stock' (dashboard only)
        E2 ctx.gate_reasons (D63, owner)   -> status 'gated', error 'gated: zone,liquidity'
        paused (tray)                      -> status 'paused'
        else send                          -> 'sent' (+ tg_message_id) / 'failed' (+ error)
     ENTRY and STRONG_WATCHLIST E1 cards go to the GROUP in BOTH editions whatever their failed gates, the same text
     in both editions and never the owner DESK block (T-178 / D148: clients read the group). The text is
     `tpl.signal_card(meta, signal, ctx=, e2=, tier=, warnings=)` where ctx is `card_context` (the D125 F7 hold at
     entry, T-162 T2). Buttons: a tiered E1 -> `tier_buttons` (ENTRY: Copy order "success" + Open contract "primary";
     STRONG WATCHLIST: a plain Open contract), an E2 -> Copy order + `tpl.signal_buttons(meta)`. A STRONG WATCHLIST
     card is sent silent (disable_notification, D129 T1); an ENTRY card with sound.
     D149(5), OWNER edition: (a) an E2 (arming) card goes to the PERSONAL chat ONLY (the decision order above on
     channel 'telegram_personal'; no usable personal chat -> 'suppressed'), with sound; nothing of it reaches the
     group, so its trade's follow-ups have no group message to reply to. (b) Every ENTRY / STRONG WATCHLIST card whose
     group decision was "send" ALSO goes to the PERSONAL chat, SILENT, WITH the DESK block (desk=True, incl. "After E2
     hh:mm @ Rs"), as a reply to its own E2's personal card when there is one (D65's thread, moved; an E2 still in
     flight is awaited). A group card replies to nothing. The client edition sends no personal copy.
TOKEN / SYSTEM messages go to the dashboard and the Telegram PERSONAL chat only, never the GROUP (a
PERSONAL chat id equal to the group id is refused). T-181 F7 (owner ruling 2026-09-29): the token watcher's
EXPIRED and critical (warn2) warnings go to the GROUP when no usable personal chat is set (send_system
group_fallback=True, the D119 rule of the readiness card), never to both. Pause does NOT suppress them: pause is for trade
alerts, and a token-expiry warning must still arrive. TOKEN with structured token data uses
`tpl.token_card`, TOKEN free text `tpl.system_card("TOKEN", ...)`, SYSTEM `tpl.system_card("SYSTEM", ...)`.

Manual sends (D42/D43/D48): `send_manual(kind, ref, dest)` renders a watchlist row, a whole list, a stored
signal or the day's signals table and sends it to the GROUP (default) or the PERSONAL chat, through the
same rate limiters. They are explicit operator actions, so neither pause, mute nor a gate blocks them.
Single-contract sends (watchlist_row, signal) carry the `tpl.signal_buttons` URL buttons. Each call writes
one audit log line and, except for rejected requests, one `manual_sends` row (GROUP refusals store their code).
Auto posts go to the GROUP, ARE blocked by pause (a 'paused' manual_sends row) and
are logged in `manual_sends` too: 09:20 = one `tpl.list_post` per list (T-162 T3, the six nearest + the dashboard
line; the manual "watchlist_list" send renders the same template); 15:35 = `tpl.daily_summary` to the GROUP (only the
signals whose GROUP alert was SENT, entries apart from STRONG WATCHLIST cards, each entry with its tracker result, T-178)
plus the operator counters `tpl.operator_summary` to PERSONAL (every
signal: sent / gated / muted / paused / failed; not blocked by pause, like token/system). D148(3) REMOVED the scheduled
13:15 post (app.run_slot posts for the 0920 kind only; D45's 13:15 clause is superseded): `post_additions` (ONE
`tpl.additions_list_post` for all lists, D64's shape) is reached only by an explicit caller -- `post_watchlists(
additions=True)` or a label ending "additions" (the API's run_job(..., post=True) on a 1315 build).

W12 (CLIENT-PACKAGE-SPEC §2-3, TRADE-TRACKER-SPEC §4/§7, D79, D81, D86, D87):
  * Edition. CLIENT: a signal whose kind is not in `edition.visible_kinds()` (an E2) is never INSERTed,
    pushed or sent -- counted in memory only (`stats.internal_arms`), so the DB never holds an E2; an E1 never
    replies to anything (it has no E2 to reply to). W15 (D114, superseding D87): every V1 E1 is recorded and
    shown with its tier, exactly as for the owner; `stats.hidden_gated` is kept and stays 0.
    T-105: the router logs a client E1 at INFO ("engine N E1: reason"). OWNER: unchanged.
  * trade_hook(signal_id, signal, meta, ctx, group_message_id | None) is awaited at the END of `on_signal`
    for every RECORDED V1 signal except a WATCHLIST E1 (the tracker opens E1 trades for ENTRY only); an
    exception is logged, never raised.
  * send_followup(trade, event): a tracker event to the GROUP as a reply to the entry message (EOD_MISSED:
    PERSONAL only), paced by the D26 windows, one trade's events strictly in order.
  * "Copy order" (D79): a copy_text button (first row) holding `templates.copy_order_text(signal, meta, ctx)`.
  * T-062: given `telegram_guard` (config.telegram_check), a refused group id sends nothing to the group and a
    refused personal id nothing to PERSONAL (an unset client personal chat = dashboard only).
  * Basic group -> supergroup migration (§3): a GROUP send answered with `migrate_to_chat_id` switches
    group_chat_id, persists TELEGRAM_GROUP_ID (config.write_env_value, only when `env_path` is given), writes a
    `group_migrated` token_events row (ids masked), retries the same message once, then a SYSTEM note. A hint
    for the PERSONAL chat (or any other chat) is logged and ignored.

0.8.1 (spec S1.5.0; D157(2), D159-D161; docs/research/2026-09-30-workflow-0.8.1/CONTRACTS.md 5). The group is read by
clients, so NOTHING owner-only reaches it: no `V1 PLUS`, `V1 B`, `CARRY` or `carried`, in either edition.
  * Per-scanner tiers: V1 / V2 by `gates.tier` (their gate RULES differ in build_context, ScannerSpec.rules), V1 B by
    `gates.v1b_tier`. R4 stays V1-only; V1 B records its FIRST ENTRY per stock-day as ENTRY and a later one as
    WATCHLIST + `dup_stock` (D161(4)). V1 B / V2 rows: recorded + graded, NO Telegram (D160(2)).
  * V1 PLUS (owner edition only) is derived by `plus.derive_plus` right after the origin's INSERT (no await between):
    a V1 ENTRY, or a V1 B signal that `gates.plus_candidate` accepts. A SHARED trade (origin V1): the group card stays
    `ENTRY · V1`; the `also V1 PLUS` line is on the owner's PERSONAL copy only (D161(1)). A PLUS-ONLY trade (origin
    V1 B): `_route_personal_only`, headline `ENTRY · V1 PLUS`, the PERSONAL chat only, never the group, never the trade
    hook (D160(1)). `pop_derived(signal_id)` hands the app the PLUS row ids to grade.
  * CARRY (D157(2), D160(3), D161(2)): a signal on a carried row (ctx.carried / meta.carried) is capped at STRONG
    WATCHLIST (gates.tier caps; the router caps again), an E1 gets the reason `carried`, the card goes SILENT to the
    PERSONAL chat only (never the group), and no trade hook runs. The client edition records none (it never carries).
    S1.9.0 (0.8.8, D380 (6) "Lift the cap now (owner edition)", D382 default): OWNER edition, the cap is gone
    (`gates.cap_carried` caps in the client edition only): a carried ENTRY goes to the PERSONAL chat WITH sound (never
    the group), runs the trade hook (a tracked trade, held by DTE like every entry), and takes R4 like an off_client
    ENTRY (demoted by ANY V1 ENTRY of the stock-day and kind; `has_client_entry_today` leaves carried rows out, so a
    carried ENTRY never demotes a client-view one). A carried NEAR ENTRY stays silent and WATCH ONLY stays on the
    dashboard; neither trades. Every carried row keeps the reason `carried`; V1 PLUS still never derives from one. The
    lift is the V1 live scanner's: a SHADOW row (V1 B, `_record_shadow`) stays capped in both editions (book_c8 reads it).
    `fetch_watchlist_rows` leaves CARRY rows out unless include_carry=True (the 09:20 post, the catch-up post and
    manual group sends), `post_watchlists` drops any it is handed, and `send_carry_line` sends `+N carried (P primed)`
    to the owner's PERSONAL chat only (KIND_CARRY_LINE, in OWNER_ONLY_KINDS).
  * A manual GROUP send of a signal that is not V1's or is carried is refused (ValueError); the group signals table
    leaves carried rows out.

0.8.2 T-202 (D171(1), D176(1), D179; DESIGN-REVIEW C.8): the GROUP = the CLIENT VIEW, OWNER edition only (the client path is
byte-identical to 0.8.1: it computes nothing below and its copies drop the key).
  * The client view of a day's lists (`client_view_ids`): per (list, expiry, stock) the HOI-1 row, else the first row in
    fetch_watchlist_rows order; a CARRY row never. The view is the UNION over every list of the day (D179 amendment): a
    V1 E1 whose contract is the view row of NO list it sits on (`in_client_view`) is `off_client`: ctx.off_client =
    True, stored in ctx_json (OFF_CLIENT). A carried E1 keeps the
    carried path (never evaluated); an E2 is never flagged (unchanged).
  * R4 splits: an off_client ENTRY is demoted by ANY V1 ENTRY of the stock-day (`has_entry_today`, unchanged); a
    client-view ENTRY only by a client-view one (`has_client_entry_today`: ctx_json off_client not true; a 0.8.1 row
    without the key counts, it went to the group). So an HOI-1 ENTRY after an HOI-2 ENTRY reaches the group with its
    trade; an HOI-2 after any ENTRY stays demoted. V1 PLUS is unchanged (first-wins by time, plus.py).
  * An off_client ENTRY / STRONG WATCHLIST -> `_route_personal_only` (ENTRY with sound, STRONG silent) with the line
    `tpl.add_not_in_group(text, meta.tag)` under the headline; never a group card, never the trade hook (the tracker
    refuses one too). An off_client WATCHLIST is dashboard only, as any WATCHLIST.
  * Manual GROUP sends: a `signal` whose ctx_json is off_client is refused, the `signals_table` drops such rows; the
    owner's PERSONAL sends keep them. `_one_strike_per_stock` (the group list posts) is built on `client_view_ids`.

0.8.2 FIX WAVE (D186, D189; docs/research/2026-09-30-workflow-0.8.2/FIX-WAVE.md, contracts C1-C3):
  * T-213 F2: a failed AUTOMATIC group send (list post, additions post, entry card, day summary) keeps its ERROR line and
    sends ONE personal SYSTEM line `GROUP SEND FAILED · <what> · <error>` per (IST day, what) through `send_system`
    (personal only; a client without a personal chat = dashboard only; never the group). The owner's personal copy of a
    group ENTRY / STRONG WATCHLIST card whose group send failed carries `tpl.add_group_failed` under the headline and is
    sent WITH sound. F9: the first time the Telegram client drops the forum topic (`topic_dead`), ONE personal SYSTEM line
    per process `group posts are landing in General: topic <id> not found`.
  * T-214 F3 (D186(2), D189(3), spec 4.4 as amended in S1.6.0): a follow-up whose trade has NO entry message is SENT,
    standalone (no reply), to the chat its entry was routed to -- an E2 trade -> PERSONAL, an E1 trade -> GROUP -- in both
    editions, after the muted / paused checks (unchanged outcomes). `no_entry_msg` is a legacy status only.
  * T-215 F5: manual GROUP sends refuse an E2 row and a signal row off the client view (ctx off_client true; OWNER
    edition, a row without the key -- pre-0.8.2 -- whose contract `in_client_view` rejects); the group `signals_table`
    renders the CLIENT layout (E1 only). F6: the group list / additions post footer says `one strike per stock` (never a
    tag name). F11 (C3): `post_watchlists(late_hm=)`. F13: `summary_data` counts `personal_only` (owner).
  * T-211 F7 (C1): every manual_sends row written after a Telegram attempt carries `tg_message_ids` (one per delivered
    part), `chat` (`…` + the last 4 characters of the chat id used) and, GROUP only, `thread` (the message_thread_id
    Telegram's response reports, else "General"); a 09:20 `watchlist_post` also `n_view`, `contracts`, `posted_late`.
    C2: `send_ops(..., ref=)` and KIND_DELIVERY_CHECK (owner only).

0.8.4 T-267 (S1.7.0; STRATEGY-SPEC 4.7; PLAN-T267 §1 + M4 / M12; D248, D249, D254, D255): the V2 Scanner (scanner "V2T")
is LIVE in both editions: `on_signal(..., scanner="V2T")` -> `_route_v2t` (never `_record_shadow`). The D255 switch OFF
records nothing; the client edition drops EXPIRY-list and non-ENTRY V2T signals unrecorded; one V2T E1 per
(trade_date, security_id) (`has_v2t_e1`, before the INSERT); the tier is `gates.v2t_verdict` (the stock's daily
Supertrend vs the side). A client-view ENTRY (`in_client_view(..., scanner="V2T")`: no EXPIRY lists) -> the GROUP card
headed "V2 Scanner" + a tracked trade; an owner-only ENTRY (HOI-2 / EXPIRY lists) -> the owner's PERSONAL card + a tracked
trade; its exits (ZSL / ZTRAIL / DAY5 / EXPIRY, `send_followup`) reply in the chat the entry went to; a trail step
(`send_zone_step`) -> the owner's PERSONAL chat only, silent, never the group; the 15:35 group summary gets the V2 rows as
`v2_rows` (`group_v2_summary_rows`). V2 rows never count in V1's R4, V1 PLUS, `summary_data` or `group_summary_data`.

0.8.5 V2 ROUTING (D287(6), PLAN-D287 I7, D296; SUPERSEDES the group half of the 0.8.4 paragraph above): the V2 Scanner
is OWNER-ONLY and never reaches the GROUP. The client edition builds no V2T scanner, records no V2T row and sends nothing
V2 (`_route_v2t` returns first; `send_followup` / `send_zone_step` answer 'suppressed' for a 0.8.4 store's V2 trade). The
owner's V2 ENTRY (a client-list contract's included: off_client True) -> his PERSONAL card with sound + a tracked trade;
its exits go PERSONAL, as a reply to that personal card (`sent_message_id(..., CH_PERSONAL)`), else standalone naming
the contract -- never a reply to a 0.8.4 GROUP entry card's id (D296 M2); trail steps PERSONAL, silent. The 15:35 group
summary carries no V2 row (`daily_summary` drops one; `group_v2_summary_rows` finds none since no V2 card is group-sent);
the owner gets a V2 Scanner day block in his PERSONAL chat after the operator summary (`owner_v2_summary_rows`,
`tpl.v2_day_block`, D296 M4). The manual GROUP-send refusal of an owner E1 (`GROUP_BLOCK_E1`) applies only to rows
written at spec >= S1.8.0 (D296 L1).

0.8.5 T-310 (owner ruling D278(5)): V1 BEFORE THE 09:20 LIST POST = HOLD, both editions. A V1 signal (an E1 of any tier, an
owner E2) the app's `v1_hold` holds is INSERTed at signal time with ctx_json `held: pending` (tier computed, R4 and the
client view NOT yet) and routes nothing: no push, card, V1 PLUS or trade. Every "today's signals" reader skips a pending row
(NOT_HELD_PENDING_SQL: R4, fetch_signals, the API). `release_held` (right after the day's first 09:20-kind list post, or at
the 09:20 run's last retry) reads them from the store, oldest first: invalid (the contract retired, or its price at / below
the signal's stop since) -> tier WATCHLIST + `held_invalid`, dashboard only, no trade; else R4 + client view now, the cards
with `tpl.add_held` (group: "sent after the 09:20 list · signal HH:MM", no owner word; owner copies: "held for ...") and the
card / trade at the release LTP. V1 B / V2 / V2T / FAR are never held. T-305 (D278(6)): a client row STORES zone_limit_pct
(its WHY NOT prints the card's limit); its push / API ctx never carry it.

0.8.8 GROUP R4 (owner, 13 Oct, label verbatim "Group counts only its own lists (Recommended)"; D432 took EXPIRY lists out
of the group): OWNER edition, the shared GROUP's one-entry-per-stock-day counts only the group's lists. An ENTRY the
owner's R4 demotes (stored WATCHLIST + dup_stock: dashboard and personal chat UNCHANGED) whose stock has no group-list
entry yet (`has_group_entry_today`: an earlier EXPIRY ENTRY does not count) is the GROUP's ENTRY (`_group_takes` ->
`_route_group_entry`: the client app's ENTRY card, no personal copy); its paper trade's follow-ups also go to the group
(`send_followup` -> `_group_followup_copy`, the personal ones unchanged) and the 15:35 group summary lists it as an entry.
"""

from __future__ import annotations

import asyncio
import html
import inspect
import json
import re
import sqlite3
import time
from collections import deque
from collections.abc import Awaitable, Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, fields, replace
from datetime import datetime
from pathlib import Path
from typing import Any, Literal, Protocol

from gammad import edition as _edition
from gammad.alerts import gates
from gammad.alerts import plus as plus_mod
from gammad.alerts import templates as tpl
from gammad.alerts.telegram import COPY_TEXT_MAX, copy_button, url_button
from gammad.config import (
    IST,
    AlertsCfg,
    GatesCfg,
    HoldCfg,
    TelegramCheck,
    V1BCfg,
    V2ScannerCfg,
    mask,
    write_env_value,
)
from gammad.log import get_logger
from gammad.trade import results as trade_results
from gammad.watch import engine_pool
from gammad.watch.types import ContractMeta, Signal, SignalContext

log = get_logger(__name__)

SystemKind = Literal["TOKEN", "SYSTEM"]
DashboardPush = Callable[[dict[str, Any]], Any]
LiveRow = Callable[[int], Mapping[str, Any]]
Buttons = list[list[dict[str, Any]]]
# TRADE-TRACKER-SPEC §4 (pinned for B2): (signal_id, signal, meta, ctx, GROUP message_id or None).
TradeHook = Callable[[int, Signal, ContractMeta, SignalContext, int | None], Awaitable[None]]
FollowupResult = tuple[str, int | None, str | None]  # (status, telegram message_id, error)

CH_DASHBOARD = "dashboard"
CH_GROUP = "telegram_group"
CH_PERSONAL = "telegram_personal"

ST_SENT = "sent"
ST_FAILED = "failed"
ST_REFUSED = "refused"
ST_MUTED = "muted"
ST_GATED = "gated"
ST_PAUSED = "paused"
ST_NO_ENTRY_MSG = "no_entry_msg"  # follow-up statuses (trade_events.status, §5); 0.8.2 T-214: legacy, never produced
ST_SUPPRESSED = "suppressed"

WHO_LOCAL = "local"
WHO_LAN = "lan"
REJECTED = "rejected"
HIDDEN_REF = "hidden-ref"

FOLLOWUP_STATUSES = (ST_SENT, ST_FAILED, ST_MUTED, ST_PAUSED, ST_NO_ENTRY_MSG, ST_SUPPRESSED)
FOLLOWUP_PERSONAL_KINDS = frozenset({"EOD_MISSED"})  # §7: PERSONAL / dashboard only, never the group
ZSTEP_KIND = "ZSTEP"  # T-267 (M4): a V2 trail step -- send_zone_step only (owner PERSONAL), never send_followup

COPY_ORDER_LABEL = "\N{CLIPBOARD} Copy order"  # D79; never BUY / SELL wording
TELEGRAM_GROUP_KEY = "TELEGRAM_GROUP_ID"
SUPERGROUP_RE = re.compile(r"-100\d+")
TOKEN_EVENT_GROUP_MIGRATED = "group_migrated"
# W14 P1 (audit #9): this many consecutive Telegram send failures -> ONE ERROR line (Check Setup scans ERROR+) and
# /api/status.telegram.last_error; the next successful send clears both.
TELEGRAM_FAIL_ESCALATE = 3
# T-181 F5 (R3): the refusal of an intentionally unset personal chat (dashboard only): NOT a failure that counts
NO_PERSONAL_CHAT = "no personal chat configured (dashboard only)"
# T-181 F5: the first failed AUTOMATIC group post of a day logs this ONE ERROR line (Check Setup scans ERROR lines)
AUTO_POST_FAILED = ("telegram: an automatic %s to the group failed (%s): today's alerts are NOT reaching Telegram; "
                    "run Check Setup")  # fmt: skip
# 0.8.2 T-213 F2 (D189(4)): the ONE personal SYSTEM line per (IST day, what) beside that ERROR line
GROUP_SEND_FAILED_LINE = tpl.GROUP_SEND_FAILED + " \N{MIDDLE DOT} {what} \N{MIDDLE DOT} {error}"
# 0.8.2 T-213 F9: the ONE personal SYSTEM line per process when the Telegram client drops the forum topic
TOPIC_DEAD_LINE = "group posts are landing in General: topic {topic} not found"
# 0.8.2 C1 (FIX-WAVE section 2): manual_sends.ref keys written after a Telegram attempt
REF_MESSAGE_IDS = "tg_message_ids"
REF_CHAT = "chat"
REF_THREAD = "thread"
THREAD_GENERAL = "General"  # a group send whose response names no message_thread_id
CHAT_TAIL_MARK = "\N{HORIZONTAL ELLIPSIS}"  # ref.chat = this + the last 4 characters of the chat id used

DEST_GROUP = "group"
DEST_PERSONAL = "personal"
DESTS = (DEST_GROUP, DEST_PERSONAL)
MANUAL_KINDS = ("watchlist_row", "watchlist_list", "signal", "signals_table")
BUTTON_KINDS = ("watchlist_row", "signal")
KIND_WATCHLIST_POST = "watchlist_post"
KIND_ADDITIONS_POST = "additions_post"
# T-277 (S1.8.0): `post_pending_additions` results; LABEL_0920_POST = the label of the day's 09:20 list post (the app's
# `_post_label("0920")`, which `_posted_0920` keys on too)
LABEL_0920_POST = "09:20 build"
ADD_SENT, ADD_NOTHING, ADD_NO_0920, ADD_ALREADY = "sent", "nothing", "no_0920_post", "already_posted"
ADD_PAUSED, ADD_FAILED, ADD_ERROR = "paused", "failed", "error"
ADDITIONS_STATUSES = (ADD_SENT, ADD_NOTHING, ADD_NO_0920, ADD_ALREADY, ADD_PAUSED, ADD_FAILED, ADD_ERROR)


class AdditionsResult(dict):  # type: ignore[type-arg]
    """`post_pending_additions`' result: a plain dict (status, label, count, n_view, lists, error) whose repr is the
    one readable log line the app writes (`refresh HH:MM: group additions: <repr>`)."""

    def __repr__(self) -> str:
        err = f" error={self.get('error')}" if self.get("error") else ""
        return (f"{self.get('status')} {self.get('label')}: {self.get('count')} row(s), {self.get('n_view')} shown"
                f"{err}")  # fmt: skip

    __str__ = __repr__
KIND_DAILY_SUMMARY = "daily_summary"
KIND_OPERATOR_SUMMARY = "operator_summary"
# W16 B4 (T-132 / T-134(2), PLAN-W16 section 13.3): the ops cards, sent by `send_ops`. They are KIND_* constants so
# tools/edition_probe.py observes them as cards post:readiness / post:install_card / post:start_line.
KIND_READINESS = "readiness"
KIND_INSTALL_CARD = "install_card"
KIND_START_LINE = "start_line"
# 0.8.2 T-211 (C2): the daily GROUP-delivery check's one line to the owner's PERSONAL chat (gammad/scan/delivery_check.py,
# owner edition only: in OWNER_ONLY_KINDS below; a client router refuses it)
KIND_DELIVERY_CHECK = "delivery_check"
OPS_KINDS = (KIND_READINESS, KIND_INSTALL_CARD, KIND_START_LINE, KIND_DELIVERY_CHECK)  # the probe reads KIND_* only
OPS_PERSONAL_OR_GROUP = "personal_or_group"
OPS_DESTS = (OPS_PERSONAL_OR_GROUP, DEST_GROUP, DEST_PERSONAL)
OpsDest = Literal["personal_or_group", "group", "personal"]

# W15 (D114, PLAN-W15 §2): the live scanner, the E1 tiers, the R4 demotion token and the shadow WS push type
SCANNER_LIVE = "V1"
TIER_ENTRY = "ENTRY"
TIER_STRONG_WATCHLIST = "STRONG_WATCHLIST"
TIER_WATCHLIST = "WATCHLIST"
GROUP_TIERS = frozenset({TIER_ENTRY, TIER_STRONG_WATCHLIST})  # D114(4): these E1 cards go to the group
DUP_STOCK = "dup_stock"  # R4 (D114(5)): gate_reasons token of a demoted second ENTRY on the same stock
# T-162 (D129 T1): ENTRY vs STRONG WATCHLIST differ in buttons (coloured, Bot API 9.4) and sound
STYLE_COPY_ORDER = "success"  # green
STYLE_OPEN_CONTRACT = "primary"  # blue
SILENT_TIERS = frozenset({TIER_STRONG_WATCHLIST})  # sent with disable_notification: watch only, no sound
WS_TYPE_SCANNER = "scanner"

# 0.8.1 (CONTRACTS 4-5): the scanner names are the registry's (engine_pool); V1 PLUS is derived (no engine)
SCANNER_V1B = engine_pool.V1_B
SCANNER_PLUS = engine_pool.V1_PLUS
SCANNER_V2 = engine_pool.V2
# S1.7.0 (T-267, D248, D249, D254, D255): the V2 Scanner -- a LIVE scanner beside V1 (`_route_v2t`); OWNER edition only
# and never posted to the GROUP since 0.8.5 (D286(4), D287(6), PLAN-D287 I7)
SCANNER_V2T = engine_pool.V2T
V2T_OFF_LINE = "V2 Scanner is switched OFF on this install: no new V2 entries (open V2 trades keep their exits)"
# D257 (STRATEGY-SPEC 4.7): the STRONG line of a V2 unwind (= v2_scanner.oi_strong_pct): the owner tags it STRONG
V2T_CLIENT_CAP_PCT = V2ScannerCfg().oi_strong_pct
TAG_CARRY = "CARRY"  # watchlist.tag of a carried row (CONTRACTS 2)
CARRIED = "carried"  # gate_reasons token of a carried E1 (D160(3): "reason `carried`")
# 0.8.9 T-419 "Still Watching" (D471 Q1 / Q7 / Q8 / Q10, D480): a carried ENTRY reaches the client app + bot + GROUP as a
# Still Watching card (edition.CLIENT_STILL_WATCHING) while `still_watching.enabled` (edition.still_watching_on). The
# words above (TAG_CARRY / CARRIED) stay INTERNAL: a client store / push / card never holds them (D437). A Still Watching
# row is marked in ctx_json by SW_CTX_KEY (a client-safe key: the client store drops `carried`, so the marker is how a
# client row -- and a held one at its release -- is known as Still Watching). SCANNER_SW = trades.scanner of these trades
# (= gammad.trade.rules.SCANNER_SW, T-419 contract); on_signal accepts it as the V1 route of a carried contract.
SW_CTX_KEY = "still_watching"
SCANNER_SW = "SW"
# D476 (1): the OWNER-only ANY-3 window shadow beside Still Watching (puts only; the engine's). It is a shadow scanner:
# `_record_shadow` records it (no card, no trade, never R4 / V1 counts) in the owner edition and the CLIENT edition drops
# it in code (by edition, never by still_watching.owner_any3_shadow, which a client yaml does not carry)
SCANNER_CARRY_ANY3 = "CARRY-ANY3"
SW_CLIENT_HOLD = "swing"  # a client row never stores / pushes the internal hold word `carry` (T-419 contract: trades.hold)
KIND_CARRY_LINE = "carry_line"  # the `+N carried (P primed)` personal line after the 09:20 build (D160(4))
CARRY_FLOOR_REASON = "floor"  # = gammad.jobs.carry.FLOOR: a carried row retired under the listing OI floor (T-282 d2)
# KIND_* values the CLIENT edition never produces (tools/edition_probe.py honours this set)
OWNER_ONLY_KINDS: frozenset[str] = frozenset({KIND_CARRY_LINE, KIND_DELIVERY_CHECK})
# 0.8.2 T-202 (D176(1), C.8): the ctx_json key (SignalContext.off_client) of an owner E1 on a contract not on the client's
# lists; the list row the client holds for a stock (else its first row, `client_view_ids`)
OFF_CLIENT = "off_client"
# 0.8.5 (D287(4), D288 Q4): the tags a client surface shows = its client strikes (HOI-1 .. HOI-CLIENT_TOP_N_OI)
CLIENT_TAGS: frozenset[str] = frozenset(f"HOI-{i}" for i in range(1, _edition.CLIENT_TOP_N_OI + 1))
# 0.8.3 C.5 (W0-6, B8): why a manual GROUP send refuses a row -- `group_refusal_reason` returns the FIRST that applies,
# in this precedence order; the send path raises with the sentence and the owner API rows carry it as `group_block`
GROUP_BLOCK_SCANNER = "scanner"
GROUP_BLOCK_E2 = "e2"  # 0.8.5: an UNTIERED (legacy) E2 only; a tiered E2 is the group's entry (PLAN-D287 I4)
GROUP_BLOCK_E1 = "e1"  # 0.8.5 (D287(1), PLAN-D287 I4): the owner's E1 is personal only, never the group
GROUP_BLOCK_CARRIED = "carried"
GROUP_BLOCK_OFF_CLIENT = "off_client"
GROUP_BLOCK_SECOND_STRIKE = "second_strike"
GROUP_BLOCK_LIST = "list"  # 0.8.8 (D429 extension, "Stop EXPIRY posts to the group"): an EXPIRY-PE / EXPIRY-CE row
GROUP_BLOCK_CODES: tuple[str, ...] = (GROUP_BLOCK_SCANNER, GROUP_BLOCK_E2, GROUP_BLOCK_E1, GROUP_BLOCK_CARRIED,
                                      GROUP_BLOCK_LIST, GROUP_BLOCK_OFF_CLIENT, GROUP_BLOCK_SECOND_STRIKE)  # fmt: skip
GROUP_BLOCK_TEXT: Mapping[str, str] = {
    GROUP_BLOCK_SCANNER: "another scanner's row (owner only): not in the group",
    GROUP_BLOCK_E2: "an untiered E2 (owner only): not in the group",
    GROUP_BLOCK_E1: "an E1 (owner only; the group's entry is the E2 bar close): not in the group",
    GROUP_BLOCK_CARRIED: "a carried contract (owner only): not in the group",
    GROUP_BLOCK_LIST: "an EXPIRY list (owner only; the clients trade BREAKDOWN-PE / BREAKOUT-CE): not in the group",
    GROUP_BLOCK_OFF_CLIENT: "not on the client's lists: not in the group",
    GROUP_BLOCK_SECOND_STRIKE: "not one of the stock's two strikes (the group shows two strikes per stock): not in the "
                               "group",
}

PERSONAL_IS_GROUP = "personal chat id equals the group chat id: token/system/operator messages never go to the group"


class TelegramSender(Protocol):
    """gammad.alerts.telegram.TelegramClient. `reply_to_message_id` / `buttons` are passed only when set,
    so a sender that takes (chat_id, text) keeps working for plain messages."""

    async def send(self, chat_id: str | int, text: str, **kwargs: Any) -> Any: ...


class SlidingWindow:
    """At most `max_events` sends in ANY `window_s` seconds (exact, unlike a refilling token bucket,
    which with capacity 20 and 20/min refill admits ~40 sends in the first minute).

    Two-phase so several limiters can be combined: `next_slot(now)` is the earliest permitted time
    (monotone: any later time is also permitted), `commit(at)` books it. There is no await between
    the two, so concurrent asyncio callers are queued in call order.
    """

    def __init__(self, max_events: int, window_s: float) -> None:
        if max_events < 1 or window_s <= 0:
            raise ValueError("max_events must be >= 1 and window_s > 0")
        self.max_events = max_events
        self.window_s = window_s
        self._times: deque[float] = deque(maxlen=max_events)

    def next_slot(self, now: float) -> float:
        if len(self._times) < self.max_events:
            return now
        return max(now, self._times[0] + self.window_s)

    def commit(self, at: float) -> None:
        self._times.append(at)


def ist_trade_date(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000, IST).date().isoformat()


def default_context(meta: ContractMeta, hold: HoldCfg) -> SignalContext:
    """The context of a signal whose caller supplied none: `gates.build_context` with no spot and no
    volumes. Hold comes from the list (D58); both gates land in gate_unknown, so nothing is gated."""
    return gates.build_context(
        meta, spot=None, volumes=(), oi_peak=None, oi_now=None, gates=GatesCfg(), hold=hold
    )


def _card_extras(**kw: Any) -> dict[str, Any]:
    """The 0.8.1 signal_card keywords (scanner / also_plus / carried) that are set -- only those are passed, so a card
    without them is rendered exactly as before."""
    return {k: v for k, v in kw.items() if v}


def format_signal(
    signal: Signal,
    meta: ContractMeta,
    ctx: SignalContext | None = None,
    *,
    e2: Signal | None = None,
    tier: str | None = None,
    warnings: Sequence[str] = (),
    scanner: str | None = None,
    sw_cfg: Any = None,
) -> str:
    """The Telegram text of an E1 / E2 signal for the GROUP: the D79 entry card. `e2` (owner E1 only, Y2) = the E2 of
    the same arming cycle (the owner E2's ARMING lines; None when there is none). W15 (PLAN-W15 §2): `tier` = the E1's
    tier (the card headline; None for an E2), `warnings` = tpl.warnings lines. T-178 (D148, owner: clients read the
    group): no `desk=` is passed, so the card carries NO owner DESK block in either edition (the template's default).
    0.8.1 (D159(2)): `scanner` = the ENTRY headline's name (the router passes V1 only: never an owner scanner here).
    0.8.9 T-419: `sw_cfg` = Settings.still_watching for a Still Watching card (a carried ENTRY; None = the defaults)."""
    return tpl.signal_card(meta, signal, ctx=ctx, e2=e2, tier=tier, warnings=list(warnings),
                           **_card_extras(scanner=scanner, sw_cfg=sw_cfg))  # fmt: skip


def signal_warnings(meta: ContractMeta, signal: Signal, alerts_cfg: AlertsCfg, cal: Any) -> list[str]:
    """`tpl.warnings(meta, signal, alerts_cfg, cal)` (the expiry / late-day lines, D114(1)); [] when the template
    is missing or fails -- a warning line must never cost the alert."""
    render = getattr(tpl, "warnings", None)
    if render is None:
        return []
    try:
        return [str(w) for w in (render(meta, signal, alerts_cfg, cal) or []) if w]
    except Exception as exc:  # noqa: BLE001 -- see docstring
        log.warning("signal warnings failed for %s (%s: %s); none shown", meta.symbol, type(exc).__name__, exc)
        return []


def format_system(
    kind: SystemKind,
    text: str,
    *,
    level: str | None = None,
    validity_ist: str | None = None,
    hours_left: float | None = None,
) -> str:
    """TOKEN with a token level -> tpl.token_card; TOKEN free text -> system_card titled TOKEN; SYSTEM ->
    system_card titled SYSTEM. A template failure falls back to escaped plain text: a token-expiry or
    feed-outage warning must never be lost to a rendering bug."""
    try:
        if kind == "TOKEN" and level is not None:
            return tpl.token_card(level, validity_ist, hours_left)
        if kind == "TOKEN":
            return tpl.system_card("TOKEN", [text], icon=tpl.ICON_TOKEN)
        return tpl.system_card("SYSTEM", [text])
    except Exception as exc:  # noqa: BLE001 -- see docstring
        log.warning("system template failed (%s: %s); sending plain text", type(exc).__name__, exc)
        return html.escape(f"{kind}: {text}", quote=False)


def delivery_text(line: str, misses: Any = None) -> str:
    """0.8.3 T-212 (K48): the delivery check's escaped one-line verdict (gammad/scan/delivery_check.run) rendered as the
    style B card `tpl.delivery_card` with the misses the check stores in the ref (they lived only in the ref / CLI,
    catalogue §4.4). A template failure sends the line unchanged: the verdict must never be lost to a rendering bug."""
    try:
        return tpl.delivery_card(line, [str(m) for m in misses or ()] if isinstance(misses, list | tuple) else ())
    except Exception as exc:  # noqa: BLE001 -- see docstring
        log.warning("delivery card failed (%s: %s); sending the line", type(exc).__name__, exc)
        return line


def _parts(rendered: str | Sequence[str] | None) -> list[str]:
    """A template result as a list of message parts (templates return one str or numbered parts)."""
    if rendered is None:
        return []
    if isinstance(rendered, str):
        return [rendered] if rendered else []
    return [p for p in rendered if p]


def _message_id(result: Any) -> int | None:
    """message_id of a sendMessage result (the sent Message); None when the sender returned none."""
    if isinstance(result, Mapping):
        mid = result.get("message_id")
        if isinstance(mid, int) and not isinstance(mid, bool):
            return mid
    return None


def _message_thread(result: Any) -> int | None:
    """0.8.2 C1: message_thread_id of a sendMessage result (the forum topic the message landed in); None when absent."""
    if isinstance(result, Mapping):
        tid = result.get("message_thread_id")
        if isinstance(tid, int) and not isinstance(tid, bool):
            return tid
    return None


def chat_tail(chat_id: Any) -> str:
    """0.8.2 C1: `…` + the last 4 characters of a chat id (manual_sends.ref.chat; never the whole id)."""
    return CHAT_TAIL_MARK + str(chat_id or "")[-4:]


@dataclass(frozen=True)
class Delivery:
    """One paced send's outcome (`AlertRouter._deliver_full`): status / error / message_id as `_deliver` returns them,
    plus the chat id actually used (the new id after a group migration) and the message_thread_id Telegram's response
    reported (None: none reported, or nothing delivered)."""

    status: str
    error: str | None
    message_id: int | None
    chat: str
    thread: int | None = None


def _json_or_none(raw: Any) -> Any:
    if raw in (None, ""):
        return None
    try:
        return json.loads(str(raw))
    except ValueError:
        return None


def reasons_list(raw: Any) -> list[str]:
    """signals.gate_reasons ("zone,liquidity" / "" / NULL) as a list."""
    if raw in (None, ""):
        return []
    return [r for r in str(raw).split(",") if r]


def is_carry_row(r: Mapping[str, Any]) -> bool:
    """A carried contract: a watchlist row tagged CARRY, or a signals row whose payload / ctx says carried."""
    ctx = r.get("ctx")
    return (
        r.get("tag") == TAG_CARRY
        or bool(r.get("carried"))
        or (isinstance(ctx, Mapping) and bool(ctx.get("carried")))
    )


def meta_from_row(r: Mapping[str, Any]) -> ContractMeta:
    """A ContractMeta from a watchlist row or a flattened signals row (for the D64 URL buttons). 0.8.1: also the v9
    atr_pct and the carry fields (carried = tag CARRY, carry_state, carry_k)."""

    def _f(key: str) -> float | None:
        return None if r.get(key) is None else float(r[key])

    def _i(key: str) -> int | None:
        return None if r.get(key) is None else int(r[key])

    return ContractMeta(
        symbol=str(r.get("symbol") or r.get("security_id") or ""),
        strike=float(r.get("strike") or 0.0),
        option_type=str(r.get("option_type") or ""),
        list_name=str(r.get("list_name") or ""),
        expiry_date=str(r.get("expiry_date") or ""),
        tag=r.get("tag"),
        zone_dist_pct=_f("zone_dist_pct"),
        zone_edge=_f("zone_edge"),
        lot_size=_i("lot_size"),
        underlying_security_id=_i("underlying_security_id"),
        atr_pct=_f("atr_pct"),
        carried=is_carry_row(r),
        carry_state=None if r.get("carry_state") is None else str(r["carry_state"]),
        carry_k=_i("carry_k"),
    )


def copy_order_row(
    signal: Signal, meta: ContractMeta, ctx: SignalContext | None, *, style: str | None = None
) -> list[dict[str, Any]] | None:
    """D79: [the "Copy order" copy_text button] holding `tpl.copy_order_text(signal, meta, ctx)`, or None when
    the template is missing, fails, returns nothing or returns more than Telegram's 256 characters (a bad
    copy_text makes Telegram refuse the WHOLE message, so the button is dropped, never the alert). `style` = the
    Bot API 9.4 button colour (T-162: "success" on an ENTRY card; None = no key)."""
    render = getattr(tpl, "copy_order_text", None)
    if render is None:
        log.warning("copy order: templates.copy_order_text missing; sending without the button")
        return None
    try:
        text = render(signal, meta, ctx)
    except Exception as exc:  # noqa: BLE001 -- see docstring
        log.warning("copy order failed for %s (%s: %s); sending without", meta.symbol, type(exc).__name__, exc)
        return None
    text = str(text or "").strip()
    if not text or len(text) > COPY_TEXT_MAX:
        log.warning("copy order for %s: %d characters (1..%d allowed); sending without", meta.symbol, len(text),
                    COPY_TEXT_MAX)
        return None
    return [copy_button(COPY_ORDER_LABEL, text, style=style)]


def signal_buttons(
    meta: ContractMeta, signal: Signal | None = None, ctx: SignalContext | None = None
) -> Buttons | None:
    """The keyboard under a ticket: with a `signal`, the D79 "Copy order" row first, then
    tpl.signal_buttons(meta) (chart / NSE URL buttons). A failing part is dropped with a warning: a broken
    button must never cost the alert. None when nothing is left."""
    rows: Buttons = []
    if signal is not None:
        copy_row = copy_order_row(signal, meta, ctx)
        if copy_row:
            rows.append(copy_row)
    try:
        rows.extend(tpl.signal_buttons(meta) or [])
    except Exception as exc:  # noqa: BLE001 -- see docstring
        log.warning("signal buttons failed for %s (%s: %s); sending without", meta.symbol, type(exc).__name__, exc)
    return rows or None


def tier_buttons(meta: ContractMeta, signal: Signal, ctx: SignalContext | None, tier: str) -> Buttons | None:
    """T-162 (D129 T1): the keyboard of a TIERED E1 card, one row. ENTRY: 📋 Copy order (style "success", green)
    + 📈 Open contract (style "primary", blue). STRONG_WATCHLIST / WATCHLIST: a plain 📈 Open contract and NO Copy
    order (watch only). A failing part is dropped with a warning (never the alert); None when nothing is left."""
    entry = tier == TIER_ENTRY
    row: list[dict[str, Any]] = []
    if entry:
        row.extend(copy_order_row(signal, meta, ctx, style=STYLE_COPY_ORDER) or [])
    try:
        row.append(url_button(tpl.OPEN_CONTRACT_LABEL, tpl.nse_url(meta.symbol),
                              style=STYLE_OPEN_CONTRACT if entry else None))  # fmt: skip
    except Exception as exc:  # noqa: BLE001 -- see docstring
        log.warning("open-contract button failed for %s (%s: %s); sending without", meta.symbol,
                    type(exc).__name__, exc)  # fmt: skip
    return [row] if row else None


def card_context(meta: ContractMeta, signal: Signal, ctx: SignalContext, cal: Any) -> SignalContext:
    """The context a group card renders with: `tpl.at_entry_context` (the D125 F7 hold at entry, the tracker's rule:
    DTE <= 1 -> intraday, so the card shows the option SL the trade is held on). Any failure -> ctx unchanged (a
    card detail must never cost the alert). The recorded / pushed context is never replaced."""
    try:
        return tpl.at_entry_context(meta, signal, ctx, cal) or ctx
    except Exception as exc:  # noqa: BLE001 -- see docstring
        log.warning("hold-at-entry for the card of %s failed (%s: %s); configured hold shown", meta.symbol,
                    type(exc).__name__, exc)  # fmt: skip
        return ctx


def _field(row: Any, key: str) -> Any:
    """row[key] for a dict / sqlite3.Row / any Mapping; None when absent."""
    try:
        return row[key]
    except (KeyError, IndexError, TypeError):
        return None


def _as_int(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


@dataclass
class RouterStats:
    """In-memory counters (never written to the DB). CLIENT edition: internal_arms = E2s the engine produced
    and the router dropped (CLIENT §2); hidden_gated = gated E1s hidden entirely (D87) -- superseded by the W15
    tiers (D114): every V1 E1 is shown, so it stays 0 (the name is kept for /api/status readers)."""

    internal_arms: int = 0
    hidden_gated: int = 0


# ------------------------------------------------------------------------------------------ store queries
def fetch_watchlist_rows(
    conn: sqlite3.Connection,
    trade_date: str,
    *,
    list_name: str | None = None,
    expiry_date: str | None = None,
    security_id: int | None = None,
    include_carry: bool = False,
) -> list[dict[str, Any]]:
    """The live watchlist rows of `trade_date`. 0.8.1 (D157(2), D160(4)): CARRY rows (tag 'CARRY', owner-only) are
    left out unless `include_carry` -- the 09:20 post, the catch-up post (app._new_rows_by_list) and the manual group
    sends all read this, and the group is read by clients. D429 (0.8.8): the CLIENT edition never reads an EXPIRY row
    (`edition.v1_lists_sql`; a store built before 0.8.8 may hold some): no list post, export or card shows one."""
    sql = "SELECT * FROM watchlist WHERE trade_date = ? AND retired_run_id IS NULL"  # T-063: a retired row is gone
    sql += _edition.v1_lists_sql()  # D429: "" in the owner edition (the query is unchanged)
    if not include_carry:
        sql += f" AND (tag IS NULL OR tag != '{TAG_CARRY}')"
    args: list[Any] = [trade_date]
    for col, val in (("list_name", list_name), ("expiry_date", expiry_date), ("security_id", security_id)):
        if val is not None:
            sql += f" AND {col} = ?"
            args.append(val)
    sql += " ORDER BY list_name, expiry_date, symbol, strike, tag, security_id"
    return [dict(r) for r in conn.execute(sql, args).fetchall()]


# D98(2): what a CLIENT signal payload (signals.payload_json / ctx_json, /api/signals, the WS push) never carries:
# the arming cycle's E2 inputs the engine copies into every snapshot (engine.ContractEngine._cycle_inputs) and the
# owner-only OI-price quadrant (D94(2)). The in-memory Signal / SignalContext are untouched (the card, the tracker).
CLIENT_SNAPSHOT_DROP_PREFIXES: tuple[str, ...] = ("cycle_", "oi_peak_bar_")
# 0.8.1 (CONTRACTS 7, D162(5)): the owner-only keys ContractMeta / SignalContext.to_dict() and the V1 B snapshot add
CLIENT_SNAPSHOT_DROP: tuple[str, ...] = ("turnover_rs", "breakout_ref", "oi_peak_close", "vol_sma")
CLIENT_PAYLOAD_DROP: tuple[str, ...] = ("carried", "carry_state", "carry_k", "atr_pct", "plus_origin", "plus_origin_id")
CLIENT_CTX_DROP: tuple[str, ...] = ("quadrant", "carried", "f1", "f2", "zone_limit_pct", OFF_CLIENT)  # + 0.8.2 T-202
# T-305 (D278(6)): the CLIENT STORE keeps these CLIENT_CTX_DROP keys (its WHY NOT prints the card's "(limit X%)", one
# source); the client's WS push and API rows still never carry them (CLIENT_WIRE_CTX_DROP, server._api_signal_row)
CLIENT_STORE_CTX_KEEP: tuple[str, ...] = ("zone_limit_pct",)
# T-310 (owner ruling D278(5)): V1 BEFORE THE 09:20 LIST POST = HOLD, both editions. A V1 signal (E1 any tier, owner E2)
# the app's `v1_hold` callable holds is RECORDED at signal time (resume, R4 and the restart need the row) with ctx_json
# HELD_KEY = "pending" and routes NOTHING (no push, card, V1 PLUS or trade); `release_held` (the app calls it right after
# the day's first 09:20-kind list post, or at the 09:20 run's last retry) checks it and stamps "released" (then routes it
# as live would, entry = the release LTP) or "invalid" (tier WATCHLIST + HELD_INVALID, never sent). Every reader of
# "today's signals" leaves a pending row out (NOT_HELD_PENDING_SQL).
HELD_KEY = "held"
HELD_PENDING, HELD_RELEASED, HELD_INVALID_STATE = "pending", "released", "invalid"
HELD_WHY_KEY = "held_why"  # an invalid row's reason: one of HELD_WHYS
HELD_RELEASE_MS_KEY = "held_release_ms"  # a released row: when it was sent (its signal time stays signals.ts_ms)
HELD_RELEASE_LTP_KEY = "held_release_ltp"  # a released row: the LTP its card / trade entered at
HELD_WHY_STOP, HELD_WHY_RETIRED = "stop", "retired"
# T-310 review (B7): no LTP at the release (L7), the release raised before its stamp (L6), still pending when its day was
# over (L6: stamped by the next day's first supervisor pass, `expire_held`)
HELD_WHY_NO_PRICE, HELD_WHY_ERROR, HELD_WHY_EXPIRED = "no_price", "error", "expired"
HELD_WHYS: tuple[str, ...] = (HELD_WHY_STOP, HELD_WHY_RETIRED, HELD_WHY_NO_PRICE, HELD_WHY_ERROR, HELD_WHY_EXPIRED)
HELD_INVALID = "held_invalid"  # gate_reasons token of a held signal that failed its release check (never sent)
HELD_CTX_KEYS: tuple[str, ...] = (HELD_KEY, HELD_WHY_KEY, HELD_RELEASE_MS_KEY, HELD_RELEASE_LTP_KEY)
# what a CLIENT's WS push / API ctx never carries although its store keeps it
CLIENT_WIRE_CTX_DROP: tuple[str, ...] = (*CLIENT_STORE_CTX_KEEP, *HELD_CTX_KEYS)
NOT_HELD_PENDING_SQL = (f" AND (CASE WHEN json_valid(ctx_json) THEN json_extract(ctx_json, '$.{HELD_KEY}') END)"
                        f" IS NOT '{HELD_PENDING}'")  # fmt: skip
# T-310 review L9: what became of the day's 09:20-kind list post when the held rows were released (the card words)
HELD_POST_SENT, HELD_POST_FAILED, HELD_POST_NONE = "sent", "failed", "none"


@dataclass(frozen=True)
class HeldLine:
    """The line a released held signal's cards carry (T-310). `post` = HELD_POST_SENT: group "sent after the 09:20 list
    · signal HH:MM", owner "held for the 09:20 list · signal HH:MM" (tpl.add_held); else (L9: the list post failed /
    never went) group "signal HH:MM" (no claim about a list), owner "sent at HH:MM (list post failed | no 09:20 list
    post) · signal HH:MM"."""

    signal_hm: str
    post: str = HELD_POST_SENT
    release_hm: str = ""


def held_line(card: str, held: HeldLine, *, owner: bool) -> str:
    """`card` with its HeldLine under the headline (HeldLine docstring). Idempotent like tpl.add_held."""
    if held.post == HELD_POST_SENT:
        return tpl.add_held(card, held.signal_hm, owner=owner)
    if not owner:
        return tpl.under_headline(card, f"signal {tpl.esc(held.signal_hm)}")
    why = "list post failed" if held.post == HELD_POST_FAILED else "no 09:20 list post"
    return tpl.under_headline(card, f"sent at {tpl.esc(held.release_hm)} ({why}){tpl.DOT}signal {tpl.esc(held.signal_hm)}")


def wire_ctx(ctx_d: Mapping[str, Any], *, client: bool) -> dict[str, Any]:
    """The ctx copy a push / API row carries: a client's drops CLIENT_WIRE_CTX_DROP (T-305, T-310); the owner's is whole."""
    return {k: v for k, v in ctx_d.items() if k not in CLIENT_WIRE_CTX_DROP} if client else dict(ctx_d)


def client_signal_dicts(
    payload: Mapping[str, Any], ctx_d: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any]]:
    """The client edition's copies of a signal's payload and ctx dicts (D98(2)); the inputs are not modified. A
    DENYLIST: 0.8.1 adds the carry keys, atr_pct, the V1 PLUS origin keys, f1 / f2 / zone_limit_pct and every V1 B
    snapshot key."""
    out = {k: v for k, v in payload.items() if k not in CLIENT_PAYLOAD_DROP}
    if out.get("tag") == TAG_CARRY:  # T-419 (D437): a Still Watching row's internal tag never reaches a client row
        out["tag"] = None
    snap = out.get("snapshot")
    if isinstance(snap, Mapping):
        out["snapshot"] = {k: v for k, v in snap.items()
                           if not str(k).startswith(CLIENT_SNAPSHOT_DROP_PREFIXES) and k not in CLIENT_SNAPSHOT_DROP}
    ctx_out = {k: v for k, v in ctx_d.items() if k not in CLIENT_CTX_DROP}
    if "hold" in ctx_out:
        ctx_out["hold"] = client_hold(ctx_out["hold"])
    return out, ctx_out


def client_hold(hold: Any) -> Any:
    """T-419 (D437): the hold word a CLIENT row stores / pushes: the internal `carry` (a Still Watching hold) reads
    SW_CLIENT_HOLD; any other value unchanged."""
    return SW_CLIENT_HOLD if str(hold or "").lower() == "carry" else hold


def signal_row(row: Mapping[str, Any]) -> dict[str, Any]:
    """A `signals` row flattened (tpl.flatten_signal_row) with ctx_json parsed into `ctx` (dict or None).
    gate_reasons stays the stored comma-joined text (the export cell); the API turns it into a list."""
    flat = tpl.flatten_signal_row(dict(row))
    flat["ctx"] = _json_or_none(flat.pop("ctx_json", None))
    return flat


def fetch_signal(conn: sqlite3.Connection, signal_id: int) -> dict[str, Any] | None:
    """One signals row by id (a manual send / preview, a V1 PLUS push). T-310 review M1: a row still held for the
    09:20 list post is no signal yet (None, as a missing id): never sent or previewed by id before its release."""
    row = conn.execute("SELECT * FROM signals WHERE id = ?" + NOT_HELD_PENDING_SQL, (signal_id,)).fetchone()
    return None if row is None else signal_row(row)


def fetch_signals(
    conn: sqlite3.Connection, trade_date: str, *, scanner: str | None = SCANNER_LIVE
) -> list[dict[str, Any]]:
    """The day's signals of `scanner` (default the live V1; None = every scanner, shadow rows included). T-310: a V1
    row still held for the 09:20 list post (NOT_HELD_PENDING_SQL) is not one of them yet."""
    sql = "SELECT * FROM signals WHERE trade_date = ?" + NOT_HELD_PENDING_SQL
    args: list[Any] = [trade_date]
    if scanner is not None:
        sql += " AND scanner = ?"
        args.append(scanner)
    rows = conn.execute(sql + " ORDER BY ts_ms, id", args).fetchall()
    return [signal_row(r) for r in rows]


def fetch_mutes(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = conn.execute("SELECT security_id, symbol, muted_ist, reason FROM mutes ORDER BY security_id").fetchall()
    return [dict(r) for r in rows]


def own_e2_id(conn: sqlite3.Connection, security_id: int, trade_date: str, before_id: int) -> int | None:
    """D65: the E2 an E1 belongs to = the latest E2 of the same contract and trade date recorded before it
    (the engine emits one E2 and one E1 per arming cycle, so the E1's own E2 is the most recent one).
    W15: V1 rows only -- a shadow scanner's E2 of the same contract is never a V1 E1's thread."""
    row = conn.execute(
        "SELECT id FROM signals WHERE security_id = ? AND trade_date = ? AND kind = 'E2' AND id < ?"
        " AND scanner = ? ORDER BY id DESC LIMIT 1",
        (security_id, trade_date, before_id, SCANNER_LIVE),
    ).fetchone()
    return None if row is None else int(row[0])


def has_entry_today(
    conn: sqlite3.Connection, trade_date: str, symbol: str, scanner: str = SCANNER_LIVE, kind: str = _edition.KIND_E1
) -> bool:
    """R4 (D114(5)): does the underlying `symbol` already have a V1 ENTRY of `kind` on `trade_date`? The underlying is
    ContractMeta.symbol, stored in every payload_json (a PE and a CE of one stock are the same underlying).
    `scanner` = whose ENTRY rows count (0.8.1: V1 B's first-ENTRY-per-stock-day rule reads its own rows). T-310: a row
    still held for the 09:20 list post does not count (release_held applies R4 to it in time order). 0.8.5 (PLAN-D287
    I5): `kind` = whose ENTRY rows count -- default E1 (the owner E1's own R4, Q3; V1 B); the router passes
    `edition.client_entry_kind(...)` for the group / client entries (the owner's E2)."""
    row = conn.execute(
        "SELECT 1 FROM signals WHERE trade_date = ? AND scanner = ? AND kind = ? AND tier = ?"
        " AND json_extract(payload_json, '$.symbol') = ?" + NOT_HELD_PENDING_SQL + " LIMIT 1",
        (trade_date, scanner, kind, TIER_ENTRY, symbol),
    ).fetchone()
    return row is not None


def v1b_zone_only(ctx: SignalContext | None) -> bool:
    """0.8.8 T-343 (a) (D382 (1)): did V1's gate verdict on this V1 B context fail ONLY on zone? gate_reasons is exactly
    (zone,) and gate_unknown is empty: the verdict gates.build_context RECORDED (its zone test, whatever gates defines it
    to be), never a re-test of the gap here. A row the router tagged `carried` or `dup_stock` is not zone-only (the
    owner's "All 34-type rows" count = stored gate_reasons 'zone' alone, no unknown input). None -> False."""
    return ctx is not None and tuple(ctx.gate_reasons) == (gates.GATE_ZONE,) and not tuple(ctx.gate_unknown)


# 0.8.9 T-419: the R4 clause that leaves an OWNER-ONLY carried row out (ctx_json carried true and NOT a Still Watching row,
# SW_CTX_KEY): a pre-0.8.9 / switched-off carried ENTRY takes R4 as an off_client one; a Still Watching ENTRY counts
_CARRIED_OFF_SQL = (
    " AND NOT ((CASE WHEN json_valid(ctx_json) THEN json_extract(ctx_json, '$.carried') ELSE NULL END) IS 1"
    f" AND (CASE WHEN json_valid(ctx_json) THEN json_extract(ctx_json, '$.{SW_CTX_KEY}') ELSE NULL END) IS NOT 1)"
)


def has_client_entry_today(conn: sqlite3.Connection, trade_date: str, symbol: str, kind: str | None = None) -> bool:
    """0.8.2 T-202 R4 for a CLIENT-VIEW entry (D176(1)): does the underlying `symbol` already have a V1 ENTRY of `kind`
    on `trade_date` that is not off_client? A row counts unless its ctx_json says `off_client: true`: a 0.8.1 row without
    the key, a NULL (pre-v3) or unreadable ctx_json counts (it went to the group). The CASE keeps json_extract off a
    malformed ctx_json, so this never raises on one. 0.8.5 (PLAN-D287 I5): `kind` None = `edition.client_entry_kind()`
    (the running edition's group / client entry rows: E1 in a client store, E2 in the owner's).
    S1.9.0 (D380 (6)): a carried ENTRY (owner edition; ctx_json carried true) does not count either -- it is not on the
    client's lists, so it takes R4 as an off_client one does (never demoting a client-view ENTRY).
    0.8.9 T-419 (D471 Q7, R4 across listed + carried): a STILL WATCHING ENTRY (ctx_json SW_CTX_KEY true) DOES count -- it
    is on the client's third list, so one entry per stock-day spans the listed and the carried rows (`_CARRIED_OFF_SQL`)."""
    kind = kind if kind is not None else _edition.client_entry_kind()
    row = conn.execute(
        "SELECT 1 FROM signals WHERE trade_date = ? AND scanner = ? AND kind = ? AND tier = ?"
        " AND json_extract(payload_json, '$.symbol') = ?"
        f" AND (CASE WHEN json_valid(ctx_json) THEN json_extract(ctx_json, '$.{OFF_CLIENT}') ELSE NULL END) IS NOT 1"
        + _CARRIED_OFF_SQL + NOT_HELD_PENDING_SQL + " LIMIT 1",  # T-310: as has_entry_today
        (trade_date, SCANNER_LIVE, kind, TIER_ENTRY, symbol),
    ).fetchone()
    return row is not None


def has_group_entry_today(conn: sqlite3.Connection, trade_date: str, symbol: str, kind: str) -> bool:
    """0.8.8 (owner, 13 Oct, label verbatim "Group counts only its own lists (Recommended)"): the shared GROUP's own R4 in
    the OWNER edition -- has the GROUP already had its ENTRY of `kind` for the underlying `symbol` on `trade_date`? Only
    the group's lists count (`group_list_ok`: BREAKDOWN-PE / BREAKOUT-CE, a row naming no list included), so an EXPIRY
    ENTRY (owner only since D432) never takes the group's one entry: the group gets the entry a client app takes.
    A row counts when it is a would-be ENTRY -- tier ENTRY, or an ENTRY the OWNER's R4 demoted (`dup_stock` in its
    gate_reasons, never a held-invalid row) -- in the client view (off_client / carried not true, as
    has_client_entry_today) and not still held. The first such row of the stock-day is the group's entry (sent as an
    ENTRY card although the owner's R4 stored it WATCHLIST: `AlertRouter._group_takes`), so every later one is a group
    dup. Read from the store (a restart routes the same way). The owner's own R4 (`has_client_entry_today`, the stored
    tier, the dashboard, the personal chat) is unchanged."""
    lists = tuple(_edition.v1_lists(owner=False))
    row = conn.execute(
        "SELECT 1 FROM signals WHERE trade_date = ? AND scanner = ? AND kind = ?"
        " AND json_extract(payload_json, '$.symbol') = ?"
        " AND (tier = ? OR (instr(',' || COALESCE(gate_reasons, '') || ',', ?) > 0"
        "      AND instr(',' || COALESCE(gate_reasons, '') || ',', ?) = 0))"
        f" AND COALESCE(list_name, '') IN ('', {', '.join('?' for _ in lists)})"
        f" AND (CASE WHEN json_valid(ctx_json) THEN json_extract(ctx_json, '$.{OFF_CLIENT}') ELSE NULL END) IS NOT 1"
        + _CARRIED_OFF_SQL + NOT_HELD_PENDING_SQL + " LIMIT 1",  # T-419: a Still Watching entry counts (as above)
        (trade_date, SCANNER_LIVE, kind, symbol, TIER_ENTRY, f",{DUP_STOCK},", f",{HELD_INVALID},", *lists),
    ).fetchone()
    return row is not None


def group_took_demoted(conn: sqlite3.Connection, signal_id: int | None) -> bool:
    """0.8.8 ("Group counts only its own lists"): was signal `signal_id` an OWNER-R4-demoted entry (stored WATCHLIST +
    `dup_stock`) that the GROUP took as its ENTRY card (a group alerts row that is not 'gated': sent / failed / muted /
    paused)? Its paper trade's follow-ups then also go to the GROUP (`send_followup`) and the 15:35 group summary lists it
    as an entry (`group_summary_data`). A pre-0.8.8 demoted row has a 'gated' group row: False. Never raises."""
    if signal_id is None:
        return False
    try:
        row = conn.execute("SELECT tier, gate_reasons FROM signals WHERE id = ?", (int(signal_id),)).fetchone()
        if row is None or row[0] != TIER_WATCHLIST or DUP_STOCK not in reasons_list(row[1]):
            return False
        card = conn.execute("SELECT 1 FROM alerts WHERE signal_id = ? AND channel = ? AND status != ? LIMIT 1",
                            (int(signal_id), CH_GROUP, ST_GATED)).fetchone()  # fmt: skip
    except sqlite3.Error:
        return False
    return card is not None


def has_v1_entry_signal(conn: sqlite3.Connection, trade_date: str, security_id: int, kind: str | None = None) -> bool:
    """0.8.5 (D287(1) "once per contract per day", PLAN-D287 I5 / AM8): does the contract already have a V1 row of `kind`
    on `trade_date` (any tier; a row still HELD counts, an invalid one too)? `kind` None = `edition.client_entry_kind()`.
    The router refuses a second one before its INSERT: an engine re-created mid-day (retired, then re-listed) or a
    restart can never enter a contract twice. Read from the store, like `has_v2t_e1`."""
    kind = kind if kind is not None else _edition.client_entry_kind()
    row = conn.execute(
        "SELECT 1 FROM signals WHERE trade_date = ? AND scanner = ? AND kind = ? AND security_id = ? LIMIT 1",
        (trade_date, SCANNER_LIVE, kind, int(security_id)),
    ).fetchone()
    return row is not None


_SIGNAL_FIELDS = tuple(f.name for f in fields(Signal))


def signal_from_row(conn: sqlite3.Connection, signal_id: int) -> Signal | None:
    """The recorded Signal of `signals.id` rebuilt from its payload_json (the router spreads Signal.to_dict()
    into it). None when the row is absent or its payload does not carry every Signal field. Never raises."""
    try:
        row = conn.execute("SELECT payload_json FROM signals WHERE id = ?", (signal_id,)).fetchone()
        if row is None or not row[0]:
            return None
        payload = json.loads(row[0])
        return Signal(**{k: payload[k] for k in _SIGNAL_FIELDS if k in payload})
    except (sqlite3.Error, ValueError, TypeError):
        log.warning("signal %s: payload could not be rebuilt into a Signal", signal_id)
        return None


def sent_group_message_id(conn: sqlite3.Connection, signal_id: int) -> int | None:
    """tg_message_id of the signal's GROUP alert when it was SENT; None if gated/muted/paused/failed/absent."""
    return sent_message_id(conn, signal_id, CH_GROUP)


def sent_message_id(conn: sqlite3.Connection, signal_id: int, channel: str) -> int | None:
    """tg_message_id of the signal's alert on `channel` when it was SENT; None otherwise."""
    row = conn.execute(
        "SELECT tg_message_id FROM alerts WHERE signal_id = ? AND channel = ? AND status = ?"
        " AND tg_message_id IS NOT NULL ORDER BY id DESC LIMIT 1",
        (signal_id, channel, ST_SENT),
    ).fetchone()
    return None if row is None else int(row[0])


def client_view_ids(rows: Iterable[Mapping[str, Any]], *, top_n: int = _edition.CLIENT_TOP_N_OI) -> set[int]:
    """0.8.2 T-202 (D176(1), D166): the security ids of the CLIENT VIEW of watchlist `rows` -- the contracts a client's
    lists hold: per (list_name, expiry_date, stock) the rows tagged HOI-1 .. HOI-`top_n` (0.8.5 D287(4), PLAN-D287 I6:
    top_n = edition.CLIENT_TOP_N_OI = 2, the client's two strikes per stock), else the FIRST `top_n` rows in the order
    given (callers hand fetch_watchlist_rows order); a CARRY row is never in it (clients never carry, D157(2)). The stock
    is the row's symbol (else its underlying id). A row without a usable security id is never in it."""
    tags = {f"HOI-{i}" for i in range(1, max(int(top_n), 0) + 1)}
    groups: dict[tuple[Any, Any, Any], list[Mapping[str, Any]]] = {}
    for r in rows:
        if is_carry_row(r):
            continue
        key = (r.get("list_name"), r.get("expiry_date"), r.get("symbol") or r.get("underlying_security_id"))
        groups.setdefault(key, []).append(r)
    out: set[int] = set()
    for group in groups.values():
        tagged = [r for r in group if r.get("tag") in tags]
        for r in tagged or group[: max(int(top_n), 0)]:
            sid = _as_int(r.get("security_id"))
            if sid is not None:
                out.add(sid)
    return out


def _same_contract(r: Mapping[str, Any], meta: ContractMeta) -> bool:
    """A watchlist row holds meta's contract (symbol, strike, option type, expiry): one option contract is one security
    id, so this pins the row's security id on whichever list the row sits."""
    try:
        return (str(r.get("symbol") or "") == meta.symbol and str(r.get("option_type") or "") == meta.option_type
                and str(r.get("expiry_date") or "") == meta.expiry_date
                and abs(float(r.get("strike") or 0.0) - float(meta.strike)) < 1e-6)  # fmt: skip
    except (TypeError, ValueError):
        return False


def _memo(memo: dict[tuple[Any, ...], Any] | None, key: tuple[Any, ...], make: Callable[[], Any]) -> Any:
    """`make()`, kept in `memo` under `key` when a memo is given (0.8.3: one API request's reads; None = no cache)."""
    if memo is None:
        return make()
    if key not in memo:
        memo[key] = make()
    return memo[key]


def in_client_view(
    conn: sqlite3.Connection, trade_date: str, meta: ContractMeta, *, memo: dict[tuple[Any, ...], Any] | None = None
) -> bool:
    """0.8.2 T-202 (D176(1); D179 amendment, orchestrator ruling on the DELIVERY note): is `meta`'s contract on the
    client's lists? The client view is the UNION of the per-(list, expiry, stock) view rows of ALL the day's live rows
    (`client_view_ids` over every list): a contract is in it when it is the view row of ANY list it sits on -- a client's
    build lists it wherever it is top-1 and its app signals on it, whatever list app.load_day kept for it (the first).
    A carried contract never is. meta's list + expiry with NO live row that day (nothing recorded to compare with: a
    store without lists) -> True, the 0.8.1 routing; live, every V1 contract comes from a live row of its list
    (app.load_day / _drop_retired), so this only spares callers without lists. Lists that exist but whose view rows
    never hold the contract -> False. `memo` (0.8.3, `group_refusal_reason` over many rows of one API request): the
    day's rows and view are read once per request; None (routing) reads them every call, as before.
    0.8.5 (PLAN-D287 I6, I7): the view holds the client's TWO strikes per stock (`client_view_ids`); the S1.7.0 V2T
    `scanner` parameter is gone (the client edition has no V2 Scanner, so the owner's V2 has no client view)."""
    if getattr(meta, "carried", False) or meta.tag == TAG_CARRY:
        return False
    rows = _memo(memo, ("day_rows", trade_date), lambda: fetch_watchlist_rows(conn, trade_date))
    if not any(r.get("list_name") == meta.list_name and r.get("expiry_date") == meta.expiry_date for r in rows):
        return True
    view = _memo(memo, ("day_view", trade_date), lambda: client_view_ids(rows))
    return any(_as_int(r.get("security_id")) in view for r in rows if _same_contract(r, meta))


def has_v2t_e1(conn: sqlite3.Connection, trade_date: str, security_id: int) -> bool:
    """T-267 router dedupe (PLAN-T267 §1): does the contract already have a V2 Scanner E1 row on `trade_date` (any tier)?
    Read from the store, so a restart (the engine's day replay firing the same trigger again) is refused the same way."""
    row = conn.execute(
        "SELECT 1 FROM signals WHERE trade_date = ? AND scanner = ? AND kind = 'E1' AND security_id = ? LIMIT 1",
        (trade_date, SCANNER_V2T, int(security_id)),
    ).fetchone()
    return row is not None


def v2_switch(conn: sqlite3.Connection, *, owner: bool) -> bool:
    """D255: is the V2 Scanner switched ON on this install? `gammad.store.db.get_v2_enabled(conn, owner=)` (persisted;
    client default OFF, owner default ON). A store without the helper, or a helper that raises, reads the edition's
    default (owner ON, client OFF: a client never trades V2 by accident)."""
    from gammad.store import db as store_db

    get = getattr(store_db, "get_v2_enabled", None)
    if get is None:
        return owner
    try:
        return bool(get(conn, owner=owner))
    except Exception as exc:  # noqa: BLE001 -- the switch must never break routing; fall back to the default
        log.warning("V2 Scanner switch unreadable (%s: %s); the edition default applies", type(exc).__name__, exc)
        return owner


def _is_off_client_row(r: Mapping[str, Any]) -> bool:
    """A flattened signals row (signal_row) whose ctx says off_client (T-202)."""
    ctx = r.get("ctx")
    return isinstance(ctx, Mapping) and ctx.get(OFF_CLIENT) is True


def client_strikes_per_stock(
    lists: Mapping[tuple[str, str], Sequence[Mapping[str, Any]]],
) -> dict[tuple[str, str], Sequence[Mapping[str, Any]]]:
    """A GROUP list post shows the CLIENT's strikes per stock in both editions (owner 2026-09-30; clients read the group,
    D148(4)): per list, each stock keeps its client-view rows (`client_view_ids`: HOI-1 and HOI-2 since 0.8.5 D287(4),
    else its first two rows). The owner's lists themselves (dashboard, personal) are untouched. 0.8.2 T-202: built on
    `client_view_ids` (one rule for the posts and the signal routing); CARRY rows are removed before (`_without_carry` /
    fetch_watchlist_rows). Was `_one_strike_per_stock` (one strike, T-153) until 0.8.5 (PLAN-D287 I6)."""
    out: dict[tuple[str, str], Sequence[Mapping[str, Any]]] = {}
    for key, rows in lists.items():
        view = client_view_ids(rows)
        out[key] = [r for r in rows if _as_int(r.get("security_id")) in view]  # the list's own order is kept
    return out


def _without_carry(
    lists: Mapping[tuple[str, str], Sequence[Mapping[str, Any]]],
) -> dict[tuple[str, str], Sequence[Mapping[str, Any]]]:
    """The lists minus their CARRY rows (a group post never shows one); a list that held only CARRY rows is dropped,
    a list that was empty already is kept as it was. D429 + its owner extension ("Stop EXPIRY posts to the group"): a
    list the GROUP does not carry (`group_list_ok`: EXPIRY-PE / EXPIRY-CE) is dropped whole, in both editions; this
    feeds only group posts (the 09:20 post, the additions posts), so the owner's personal chat is untouched."""
    out: dict[tuple[str, str], Sequence[Mapping[str, Any]]] = {}
    for key, rows in lists.items():
        if not group_list_ok(key[0]):  # D429 + extension: a group post never carries an EXPIRY list (both editions)
            continue
        kept = [r for r in rows if not is_carry_row(r)]
        if kept or not rows:
            out[key] = kept if len(kept) != len(rows) else rows
    return out


def _with_live(rows: list[dict[str, Any]], live: LiveRow | None) -> list[dict[str, Any]]:
    """Merge the live LTP into stored rows (the build-time OI stays as stored)."""
    if live is None:
        return rows
    for r in rows:
        ltp = live(int(r["security_id"])).get("ltp")
        if ltp is not None:
            r["ltp"] = ltp
    return rows


def _ref_int(ref: Mapping[str, Any], key: str) -> int:
    try:
        return int(ref[key])
    except KeyError:
        raise ValueError(f"ref.{key} is required") from None
    except (TypeError, ValueError):
        raise ValueError(f"ref.{key} must be an integer") from None


def _ref_str(ref: Mapping[str, Any], key: str, *, required: bool = False) -> str | None:
    val = ref.get(key)
    if val in (None, ""):
        if required:
            raise ValueError(f"ref.{key} is required")
        return None
    return str(val)


def _off_client_view(
    conn: sqlite3.Connection, r: Mapping[str, Any], *, owner: bool, memo: dict[tuple[Any, ...], Any] | None = None
) -> bool:
    """0.8.2 T-215 F5 (D186(3)): a flattened signals row that is NOT the client's contract: its ctx says off_client, or
    (OWNER edition only) its ctx carries no off_client key -- a pre-0.8.2 row, or an E2, never flagged -- and
    `in_client_view` rejects its contract on the signal's day. The client edition never evaluates the view (T-202: the
    client path computes nothing; its rows never hold the key). `memo`: in_client_view's."""
    if _is_off_client_row(r):
        return True
    ctx = r.get("ctx")
    if not owner or (isinstance(ctx, Mapping) and OFF_CLIENT in ctx):
        return False
    day = str(r.get("trade_date") or "")
    return bool(day) and not in_client_view(conn, day, meta_from_row(r), memo=memo)


def _second_strike(
    conn: sqlite3.Connection, r: Mapping[str, Any], memo: dict[tuple[Any, ...], Any] | None = None
) -> bool:
    """T-199 (D166): a watchlist row that is NOT its stock's one strike in its (list, expiry) on its day -- the pick
    `client_strikes_per_stock` makes over the day's live, non-CARRY rows of that list (the group's strikes rule).
    `memo`: the pick is read once per (day, list, expiry) of one API request (None = every call)."""
    day = str(r.get("trade_date") or "")
    key = (str(r.get("list_name") or ""), str(r.get("expiry_date") or ""))

    def pick() -> set[int | None]:
        same_list = fetch_watchlist_rows(conn, day, list_name=key[0], expiry_date=key[1])
        return {_as_int(x.get("security_id")) for x in client_strikes_per_stock({key: same_list})[key]}

    return _as_int(r.get("security_id")) not in _memo(memo, ("one_strike", day, *key), pick)


# D296 L1: the first strategy spec written under the D287 rules (= scan/delivery_check.D287_SPEC; the client package
# ships no gammad/scan, so the router keeps its own copy)
D287_SPEC: tuple[int, int, int] = (1, 8, 0)
_SPEC_RE = re.compile(r"\bS(\d+)\.(\d+)(?:\.(\d+))?\b")


def _spec_before(text: Any, spec: tuple[int, int, int]) -> bool:
    """True only when `text` reads as a strategy spec ("S1.7.0") strictly before `spec`, compared numerically
    (S1.10.0 > S1.8.0). No / an unreadable version is NOT before: the newer rule applies (fail closed)."""
    m = _SPEC_RE.search(str(text or ""))
    return m is not None and (int(m[1]), int(m[2]), int(m[3] or 0)) < spec


def group_list_ok(list_name: Any) -> bool:
    """0.8.8 (D429 and its owner extension "Stop EXPIRY posts to the group"): THE rule for what the shared GROUP (clients
    read it) may carry by list -- the CLIENT's lists, `edition.v1_lists(owner=False)` (BREAKDOWN-PE / BREAKOUT-CE), in
    BOTH editions. Every group destination reads it: list / additions posts (`_without_carry`), entry cards
    (`_route_group_signal`), follow-ups (`_send_followup`) and manual group sends (`group_refusal_reason`, code
    GROUP_BLOCK_LIST). The owner's PERSONAL chat and dashboard keep every list. A row / trade that names NO list (None or
    "") is not judged by this rule (every stored row names one; the other group rules still apply)."""
    return not list_name or _edition.v1_list_ok(list_name, owner=False)


def group_refusal_reason(
    conn: sqlite3.Connection, row: Mapping[str, Any], *, owner: bool, memo: dict[tuple[Any, ...], Any] | None = None
) -> str | None:
    """0.8.3 C.5 (W0-6, B8): why a manual GROUP send refuses `row` -- the FIRST of GROUP_BLOCK_CODES that applies -- or
    None when the group may carry it. The ONE rule: the send path raises with GROUP_BLOCK_TEXT[code] and the owner API
    rows carry that sentence as `group_block`.
    A signals row (it has `kind`; signal_row / fetch_signal shape): scanner = not the live V1 (0.8.1), e2 = an UNTIERED
    E2 (D149(5), 0.8.2 F5; 0.8.5: a tiered E2 is the group's entry), e1 = an OWNER E1 (0.8.5 D287(1): personal only),
    carried = a carried contract (0.8.1), off_client = off the client view (T-202 / F5; `owner` False never
    evaluates the view, `_off_client_view`). A watchlist row (no `kind`): carried = a CARRY row, second_strike = not one
    of its stock's client strikes (T-199, `_second_strike`); the client's lists ARE that pick, so off_client is never a
    watchlist row's code. `memo` (optional, one dict per API request or table render over many rows): the store reads
    behind the view / the one-strike pick happen once, not once per row; the codes are the same without it."""
    if "kind" not in row:  # a watchlist row: signals.kind is NOT NULL, the watchlist table has no such column
        if is_carry_row(row):
            return GROUP_BLOCK_CARRIED
        if owner and not group_list_ok(row.get("list_name")):  # D429 extension (a client has no EXPIRY row)
            return GROUP_BLOCK_LIST
        return GROUP_BLOCK_SECOND_STRIKE if _second_strike(conn, row, memo) else None
    if str(row.get("scanner") or SCANNER_LIVE) != SCANNER_LIVE:
        return GROUP_BLOCK_SCANNER
    kind = str(row.get("kind") or "")
    if kind == _edition.KIND_E2 and not row.get("tier"):  # 0.8.5 (PLAN-D287 I4): a tiered E2 is the group's entry
        return GROUP_BLOCK_E2
    # 0.8.5 (D287(1)): the owner E1 is personal only (a client E1 = its entry). D296 L1: only for a row written under
    # the D287 rules (spec >= S1.8.0); a pre-0.8.5 E1 was the group's entry and may be re-sent there by hand as before
    if owner and kind == _edition.KIND_E1 and not _spec_before(row.get("spec_version"), D287_SPEC):
        return GROUP_BLOCK_E1
    if is_carry_row(row):
        return GROUP_BLOCK_CARRIED
    if owner and not group_list_ok(row.get("list_name")):  # D429 extension: an EXPIRY-list signal is personal only
        return GROUP_BLOCK_LIST
    return GROUP_BLOCK_OFF_CLIENT if _off_client_view(conn, row, owner=owner, memo=memo) else None


def manual_audit(kind: str, ref: Any, dest: str, who: str, status: str, detail: str) -> None:
    ref_id = "-"
    if isinstance(ref, Mapping):
        if kind in ("signal", "watchlist_row"):
            ref_id = str(ref.get("id" if kind == "signal" else "security_id", "-"))
        elif kind == "watchlist_list":
            ref_id = f"{ref.get('list_name') or '-'} {ref.get('expiry_date') or '-'}"
    write = log.info if status == ST_SENT else log.warning
    write("manual send %s %s -> %s by %s: %s %s", kind, ref_id, dest, who, status, detail)


class GroupRefusal(ValueError):
    """A manual GROUP refusal with its stable audit code and the existing operator-facing message."""

    def __init__(self, message: str, code: str) -> None:
        super().__init__(message)
        self.code: str = code


def group_refusal_error(what: str, code: str) -> GroupRefusal:
    """The ValueError a refused manual GROUP send raises: `what` (the row), the code's sentence, and where it may go."""
    return GroupRefusal(f"{what}: {GROUP_BLOCK_TEXT[code]}; send it to the personal chat", code)


def _group_refusal(
    conn: sqlite3.Connection, r: Mapping[str, Any], *, owner: bool, memo: dict[tuple[Any, ...], Any] | None = None
) -> bool:
    """A signals row a manual GROUP send refuses (the signal) or drops (the table): `group_refusal_reason` names one
    (0.8.3 C.5; the same rows as 0.8.2: another scanner, an E2, a carried contract, a contract off the client view)."""
    return group_refusal_reason(conn, r, owner=owner, memo=memo) is not None


def _manual_rows(
    conn: sqlite3.Connection,
    kind: str,
    ref: Mapping[str, Any],
    *,
    today: str,
    live: LiveRow | None,
    dest: str = DEST_GROUP,
    edition: str | None = None,
) -> tuple[str, list[dict[str, Any]]]:
    """(day, rows) behind a manual send. ValueError = bad request, LookupError = not found.
    0.8.1 (D157(2), D159(1)): to the GROUP, CARRY watchlist rows are not found, a carried signal or a non-V1 signal
    (V1 PLUS / V1 B / V2 rows) is refused, and the signals table leaves carried rows out; the owner's PERSONAL chat
    gets them all. 0.8.2 T-202 (D172's open item): the same for an off_client signal (refused / left out of the table).
    0.8.2 T-215 F5: and for an E2 and, `edition` OWNER (None = gammad.edition), a row without the off_client key whose
    contract is off the client view (`_group_refusal`). 0.8.3 C.5: a refused GROUP send of a signal or a watchlist row
    raises `group_refusal_error` (GROUP_BLOCK_TEXT of `group_refusal_reason`'s code + `personal`); the same rows as 0.8.2."""
    if kind not in MANUAL_KINDS:
        raise ValueError(f"kind must be one of {MANUAL_KINDS}, got {kind!r}")
    if not isinstance(ref, Mapping):
        raise TypeError("ref must be an object")
    day = _ref_str(ref, "date") or today
    personal = dest == DEST_PERSONAL
    owner = (edition if edition is not None else _edition.edition()) == _edition.OWNER
    if kind == "watchlist_row":
        sid = _ref_int(ref, "security_id")
        rows = fetch_watchlist_rows(
            conn, day, list_name=_ref_str(ref, "list_name"), expiry_date=_ref_str(ref, "expiry_date"), security_id=sid,
            include_carry=personal,
        )  # fmt: skip
        if not rows:
            raise LookupError(f"no watchlist row for security_id {sid} on {day}")
        if not personal:  # T-199 (D166): the group carries each stock's ONE strike (the client's), never a second
            code = group_refusal_reason(conn, rows[0], owner=owner)  # 0.8.3 C.5: CARRY rows were not fetched above
            if code is not None:
                raise group_refusal_error(f"security_id {sid}", code)
        return day, _with_live(rows[:1], live)
    if kind == "watchlist_list":
        name = _ref_str(ref, "list_name", required=True)
        exp = _ref_str(ref, "expiry_date", required=True)
        rows = _with_live(fetch_watchlist_rows(conn, day, list_name=name, expiry_date=exp, include_carry=personal),
                          live)  # fmt: skip
        if not rows:
            raise LookupError(f"no watchlist {name} {exp} on {day}")
        if not personal:  # T-199 (D166): the same one-strike rule as the 09:20 / 13:15 group posts
            rows = list(client_strikes_per_stock({(name, exp): rows})[(name, exp)])
        return day, rows
    if kind == "signal":
        sig_id = _ref_int(ref, "id")
        row = fetch_signal(conn, sig_id)
        if row is None:
            raise LookupError(f"no signal {sig_id}")
        code = None if personal else group_refusal_reason(conn, row, owner=owner)
        if code is not None:
            raise group_refusal_error(f"signal {sig_id}", code)
        return day, [row]
    rows = fetch_signals(conn, day)
    memo: dict[tuple[Any, ...], Any] = {}  # one table: the day's view is read once (0.8.3; the same rows dropped)
    return day, rows if personal else [r for r in rows if not _group_refusal(conn, r, owner=owner, memo=memo)]


def _signal_hm(row: Mapping[str, Any], today: str | None) -> str:
    """The stored signal's clock for the G3 line: `12:35`, or `28 Sep 12:35` for a signal of another day."""
    hm = tpl.ist_hm(int(row.get("ts_ms") or 0))
    day = str(row.get("trade_date") or "")
    return f"{tpl.disp_day(day)} {hm}" if day and today and day != today else hm


def _render_rows(
    kind: str, ref: Mapping[str, Any], day: str, rows: list[dict[str, Any]], dest: str = DEST_GROUP, *,
    sent_hm: str | None = None, today: str | None = None,
) -> list[str]:  # fmt: skip
    """The manual send / preview text. 0.8.2 T-215: a GROUP render is the client's text -- the list footer says `one
    strike per stock` (F6), the signals table is the CLIENT layout (F5: E1 rows only, no `E2` word).
    0.8.3 G3 (ALERTS.md): `sent_hm` (the send / preview clock, HH:MM IST) puts `sent by you 13:15 · signal 12:35` under
    a PERSONAL signal card's headline (`tpl.add_sent_by_you`); never on a GROUP render (the group = the client text)."""
    group = dest != DEST_PERSONAL
    if kind == "watchlist_row":
        row = rows[0]
        # D166: no owner tag word (CARRY, HOI-3+) in the group; 0.8.5 (D288 Q4): HOI-1 / HOI-2 are client tags
        if group and str(row.get("tag") or "") not in CLIENT_TAGS:
            row = {**row, "tag": None}
        return [tpl.watchlist_row(row)]
    if kind == "watchlist_list":  # T-162 T3: the same template as the 09:20 auto post
        if group:
            return _parts(tpl.list_post(str(ref["list_name"]), str(ref["expiry_date"]), rows, day, group=True))
        return _parts(tpl.list_post(str(ref["list_name"]), str(ref["expiry_date"]), rows, day))
    if kind == "signal":
        # T-178 (D148): the owner DESK only on a PERSONAL send; a GROUP send never carries it (clients read the group)
        if group:
            return [tpl.signal_share(rows[0])]
        card = tpl.signal_share(rows[0], desk=True)
        return [tpl.add_sent_by_you(card, sent_hm, _signal_hm(rows[0], today)) if sent_hm else card]
    if group:
        return _parts(tpl.signals_table(day, rows, edition=_edition.CLIENT))
    return _parts(tpl.signals_table(day, rows))


def render_manual(
    conn: sqlite3.Connection,
    kind: str,
    ref: Mapping[str, Any],
    *,
    today: str,
    live: LiveRow | None = None,
    dest: str = DEST_GROUP,
    edition: str | None = None,
    sent_hm: str | None = None,
) -> list[str]:
    """The Telegram message(s) for a manual send / preview. ValueError = bad request, LookupError = not found.
    `dest` = where it goes (default the GROUP: a preview shows the group text); a signal card to PERSONAL carries the
    owner DESK section (owner edition only), a group one never does (T-178). `edition` (0.8.2 F5) = the router's
    edition (None = gammad.edition): the owner evaluates the client view for rows without the off_client key.
    `sent_hm` (0.8.3 G3) = the clock a PERSONAL signal card's `sent by you` line names (None = no line).

    kind            ref
    watchlist_row   {security_id, list_name?, expiry_date?, date?}  -> [watchlist_row card]
    watchlist_list  {list_name, expiry_date, date?}                  -> watchlist_post parts
    signal          {id}                                             -> [signal_share card]
    signals_table   {date?}                                          -> signals_table parts
    """
    if dest not in DESTS:
        raise ValueError(f"dest must be one of {DESTS}, got {dest!r}")
    day, rows = _manual_rows(conn, kind, ref, today=today, live=live, dest=dest, edition=edition)
    return _render_rows(kind, ref, day, rows, dest, sent_hm=sent_hm, today=today)


# ------------------------------------------------------------------------------------------ router
class AlertRouter:
    def __init__(
        self,
        db: sqlite3.Connection,
        telegram: TelegramSender | None,
        dashboard_push: DashboardPush | None = None,
        *,
        group_chat_id: str | int | None = None,
        personal_chat_id: str | int | None = None,
        spec_version: str | None = None,
        group_rate_per_min: int | None = None,
        per_chat_rate_per_sec: int | None = None,
        hold: HoldCfg | None = None,
        clock_ms: Callable[[], int] | None = None,
        monotonic: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        edition: str | None = None,
        trade_hook: TradeHook | None = None,
        telegram_guard: TelegramCheck | None = None,
        env_path: Path | str | None = None,
        gates: GatesCfg | None = None,
        alerts_cfg: AlertsCfg | None = None,
        calendar: Any = None,
        v1b: V1BCfg | None = None,
        still_watching: Any = None,
    ) -> None:
        """W12 keywords: `edition` "owner"/"client" (None = gammad.edition at construction); `trade_hook` (also
        settable later as `router.trade_hook`); `telegram_guard` = Settings.telegram (T-062; None = no
        chat-id policing beyond the personal==group refusal); `env_path` = the .env a group migration is
        persisted to (None = in memory only, logged).
        W15 keywords: `gates` = Settings.gates, the GatesCfg an E1 is tiered with (gates.tier); `alerts_cfg` =
        Settings.alerts and `calendar` = a TradingCalendar, both handed to tpl.warnings (None = the defaults /
        gammad.core.calendar.default_calendar() on first use). The runtime passes cfg.gates and cfg.alerts.
        0.8.1: `v1b` = Settings.v1b, the V1BCfg of V1 B's tier and the V1 PLUS component (None = the settings' when
        the router loads settings, else V1BCfg()).
        0.8.9 T-419: `still_watching` = Settings.still_watching (None = the settings' when the router loads settings,
        else the defaults: on, calls tagged 'higher risk', D471)."""
        if None in (group_chat_id, personal_chat_id, spec_version, group_rate_per_min, per_chat_rate_per_sec):
            from gammad.config import settings

            s = settings()
            if group_chat_id is None:
                group_chat_id = s.env.telegram_group_id
            if personal_chat_id is None:
                personal_chat_id = s.env.telegram_personal_chat_id
            spec_version = spec_version or s.spec_version
            group_rate_per_min = group_rate_per_min or s.alerts.group_rate_per_min
            per_chat_rate_per_sec = per_chat_rate_per_sec or s.alerts.per_chat_rate_per_sec
            hold = hold or s.hold
            gates = gates or s.gates
            alerts_cfg = alerts_cfg or s.alerts
            v1b = v1b or getattr(s, "v1b", None)
            still_watching = still_watching if still_watching is not None else getattr(s, "still_watching", None)
        assert spec_version is not None and group_rate_per_min and per_chat_rate_per_sec
        self.db = db
        self.telegram = telegram
        self.dashboard_push = dashboard_push
        self.group_chat_id = str(group_chat_id or "")
        self.personal_chat_id = str(personal_chat_id or "")
        self.spec_version = spec_version
        # Only used when a caller passes no SignalContext; HoldCfg() carries the same D58 lists as
        # config/strategy.yaml. The runtime passes cfg.hold.
        self.hold_cfg = hold or HoldCfg()
        self.gates_cfg = gates or GatesCfg()  # W15: the E1 tier (gates.tier)
        self.alerts_cfg = alerts_cfg or AlertsCfg()  # W15: tpl.warnings thresholds
        self.v1b_cfg = v1b or V1BCfg()  # 0.8.1: V1 B's tier + the V1 PLUS component (gates.v1b_tier / plus_candidate)
        # 0.8.9 T-419: Settings.still_watching (read defensively: edition.still_watching_on; None = on, D471 Q10)
        self.still_watching_cfg = still_watching
        # 0.8.1: origin signal id -> the V1 PLUS row ids it derived, until the app grades them (pop_derived)
        self._derived: dict[int, list[int]] = {}
        self._calendar = calendar
        self.paused = False
        self._clock_ms = clock_ms or (lambda: time.time_ns() // 1_000_000)
        self._mono = monotonic
        self._sleep = sleep
        self._per_chat_rate = int(per_chat_rate_per_sec)
        self._chat_limits: dict[str, SlidingWindow] = {}
        self._group_limit = SlidingWindow(int(group_rate_per_min), 60.0)
        self._muted: set[int] | None = None  # cache of mutes.security_id; None = reload on next use
        # D65: E2 signal_id -> a future set when its routing is done (D149(5): the E2 goes to PERSONAL only; the E1's
        # personal copy awaits it, then reads the E2's personal message id)
        self._e2_inflight: dict[int, asyncio.Future[int | None]] = {}
        ed = edition if edition is not None else _edition.edition()
        self.edition = _edition.OWNER if ed == _edition.OWNER else _edition.CLIENT  # fail closed, like edition.py
        self.trade_hook: TradeHook | None = trade_hook
        self.telegram_guard = telegram_guard
        self.env_path = Path(env_path) if env_path is not None else None
        self.stats = RouterStats()
        self._migrated_from: set[str] = set()  # old GROUP ids already migrated in this process (§3)
        self._trade_locks: dict[Any, asyncio.Lock] = {}  # one trade's follow-ups strictly in order
        # T-108: does closed_cycles carry the v6 bar_ts_ms column? None = not probed yet; only True is kept, so an
        # unmigrated store is re-probed per insert and a migration during this process is picked up
        self._cc_has_bar_ts: bool | None = None
        self.telegram_fail_streak = 0  # W14 P1: consecutive telegram.send exceptions (any chat)
        self.telegram_last_error: str | None = None  # set once the streak reaches TELEGRAM_FAIL_ESCALATE
        # T-181 F5 / 0.8.2 T-213 F2: the (IST day, what) pairs whose first failed auto post was logged + told
        self._auto_fail_keys: set[tuple[str, str]] = set()
        self._topic_dead_noted = False  # 0.8.2 T-213 F9: the dead-topic SYSTEM line went out (once per process)
        self._v2_off_days: set[str] = set()  # T-267 D255: the IST days the "V2 is OFF" line was logged (once a day)
        self._v1b_fyi_keys: set[tuple[str, int]] = set()  # D382 (1): (IST day, security_id) whose V1 B zone FYI is claimed
        # T-310 (D278(5)): the app's hold rule for a V1 signal (True = hold it until release_held); None = never hold
        self.v1_hold: Callable[[Signal], bool] | None = None
        # T-310 review M1: called (signal_id, kind, meta, the V1 PLUS ids it derived) once a held row is stamped and
        # routed, so the app grades it with its RELEASED tier (a held row is never graded at hold time); None = drained
        self.on_released: Callable[[int, str, ContractMeta, list[int]], Any] | None = None
        self._release_lock: asyncio.Lock | None = None  # one release_held at a time (made on the loop, lazily)

    @property
    def is_client(self) -> bool:
        return self.edition != _edition.OWNER

    @property
    def still_watching_on(self) -> bool:
        """0.8.9 T-419 (D471 Q10, D480): is the Still Watching list on (both editions)? Read at CALL time."""
        return _edition.still_watching_on(self.still_watching_cfg)

    def _sw_card_extras(self, meta: ContractMeta, ctx: SignalContext | None) -> dict[str, Any]:
        """The signal_card keyword a Still Watching card reads (`sw_cfg`: calls_tag / eve_exit_hm), only for a carried
        contract (an empty dict otherwise: every other card is rendered exactly as before)."""
        carried = bool(getattr(meta, "carried", False) or getattr(ctx, "carried", False))
        return {"sw_cfg": self.still_watching_cfg} if carried and self.still_watching_cfg is not None else {}

    @property
    def calendar(self) -> Any:
        """The TradingCalendar handed to tpl.warnings (lazy, like TradeTracker.calendar); None if it cannot load."""
        if self._calendar is None:
            try:
                from gammad.core.calendar import default_calendar

                self._calendar = default_calendar()
            except Exception as exc:  # noqa: BLE001 -- the warnings degrade, the alert does not
                log.warning("trading calendar unavailable for signal warnings (%s: %s)", type(exc).__name__, exc)
        return self._calendar

    def visible_kinds(self) -> tuple[str, ...]:
        return (_edition.KIND_E1,) if self.is_client else (_edition.KIND_E1, _edition.KIND_E2)

    # -- tray

    def set_paused(self, paused: bool) -> None:
        self.paused = paused
        log.info("alerts %s", "PAUSED (signals still recorded)" if paused else "resumed")

    # -- mutes (D42)

    def _mute_set(self) -> set[int]:
        if self._muted is None:
            self._muted = {int(r[0]) for r in self.db.execute("SELECT security_id FROM mutes").fetchall()}
        return self._muted

    def is_muted(self, security_id: int) -> bool:
        return int(security_id) in self._mute_set()

    def muted_ids(self) -> list[int]:
        return sorted(self._mute_set())

    def mutes(self) -> list[dict[str, Any]]:
        return fetch_mutes(self.db)

    def mute(self, security_id: int, muted: bool, reason: str | None = None) -> list[int]:
        """Mute / unmute one contract's GROUP signal alerts. Returns the muted security_ids after the change."""
        sid = int(security_id)
        if muted:
            row = self.db.execute(
                "SELECT symbol, strike, option_type FROM watchlist WHERE security_id = ? ORDER BY trade_date DESC"
                " LIMIT 1",
                (sid,),
            ).fetchone()
            symbol = f"{row[0]} {tpl.plain_num(row[1])} {row[2]}" if row is not None else None
            self.db.execute(
                "INSERT INTO mutes (security_id, symbol, muted_ist, reason) VALUES (?, ?, ?, ?)"
                " ON CONFLICT (security_id) DO UPDATE SET symbol = excluded.symbol, muted_ist = excluded.muted_ist,"
                " reason = excluded.reason",
                (sid, symbol, self._now_ist(), reason),
            )
        else:
            self.db.execute("DELETE FROM mutes WHERE security_id = ?", (sid,))
        self.db.commit()
        self._muted = None
        log.info("contract %s %s%s", sid, "MUTED" if muted else "unmuted", f" ({reason})" if reason else "")
        return self.muted_ids()

    # -- entry points

    async def on_signal(
        self, signal: Signal, meta: ContractMeta, ctx: SignalContext | None = None, *, scanner: str = SCANNER_LIVE
    ) -> int:
        """Record, push, route one E1/E2, then await the trade hook. Returns the signals.id, or 0 when the
        CLIENT edition dropped the signal before recording it (0.8.5: the engine's own E1 -- the client's entry is its E2,
        stored as kind E1 -- or any shadow-scanner signal), or when the contract already has a V1 row of that kind today
        (PLAN-D287 I5 / AM8: one per contract-day).
        `scanner` != "V1" -> `_record_shadow` (recorded + WS type `scanner` only; PLAN-W15 §2)."""
        if ctx is None:
            ctx = default_context(meta, self.hold_cfg)
        v1r = isinstance(signal.snapshot, dict) and "v1r" in signal.snapshot
        if scanner == SCANNER_SW:  # T-419: the Still Watching scanner id = V1's route on a carried contract
            scanner = SCANNER_LIVE
            if not getattr(meta, "carried", False):
                meta = replace(meta, carried=True)
        carried = bool(ctx.carried or getattr(meta, "carried", False))
        # 0.8.9 T-419 (D471 Q7 / Q10): a carried V1 signal is a STILL WATCHING one while the switch is on (both editions)
        sw = carried and scanner == SCANNER_LIVE and self.still_watching_on
        if carried and self.is_client and not sw:
            # 0.8.1 (D157(2)): with Still Watching off the client never carries; nothing of it is recorded, pushed or
            # sent (its tag / reason words would reach client surfaces)
            log.debug("client: a signal on a carried contract dropped (Still Watching off)")
            return 0
        if carried and not ctx.carried:
            ctx = replace(ctx, carried=True)  # gates.tier caps on ctx.carried (the tracker re-tiers with it too)
        if carried and not getattr(meta, "carried", False):
            meta = replace(meta, carried=True)  # T-419: the card reads meta.carried (a Still Watching card)
        if scanner == SCANNER_V2T:  # S1.7.0 (T-267): the V2 Scanner is LIVE in both editions, its own route
            return await self._route_v2t(signal, meta, ctx)
        if scanner != SCANNER_LIVE:
            return await self._record_shadow(signal, meta, ctx, scanner)
        if self.is_client:
            if signal.kind == _edition.KIND_E2:
                # 0.8.5 (D287(1), D288): the client's V1 entry is the E2 bar close, stored as kind E1, so every client
                # E2 guard (CLIENT §2, the package's leak checks) stays as it is: the DB never holds an E2
                signal = replace(signal, kind=_edition.KIND_E1)
            elif signal.kind not in self.visible_kinds() or (signal.kind == _edition.KIND_E1 and not v1r):
                # CLIENT §2 / 0.8.5: the engine's own E1 (and any other kind) is never INSERTed, pushed, sent or logged
                # at INFO (counted in memory only)
                self.stats.internal_arms += 1
                return 0
            # T-105: the engine logged this signal at DEBUG (it fired before the gates); W15 shows every V1 entry with
            # its tier (D114, superseding the D87 hidden-gated rule), so the client log carries it at INFO.
            log.info("engine %s %s%s: %s", signal.security_id, signal.kind, " STRONG" if signal.strong else "",
                     signal.reason)  # fmt: skip
        held = self._hold_v1(signal)  # T-310 (D278(5)): before the day's first 09:20 list post -> record, route nothing
        trade_date = ist_trade_date(signal.ts_ms)
        if not v1r and has_v1_entry_signal(self.db, trade_date, signal.security_id, kind=signal.kind):
            # 0.8.5 (D287(1), PLAN-D287 I5 / AM8): ONE client entry (and one owner E1) per contract per day, read from the
            # store (held rows count): an engine re-created mid-day or a restart never enters twice. Before the INSERT.
            (log.debug if self.is_client else log.info)(
                "V1: sec %s already has a %s row on %s; the repeat is dropped", signal.security_id, signal.kind,
                trade_date)  # fmt: skip
            return 0
        tier: str | None = None
        group_entry = False  # 0.8.8: an owner-R4-demoted entry the GROUP takes as its ENTRY (`_group_takes`)
        off_client = False  # 0.8.2 T-202: an OWNER V1 entry on a contract not on the client's lists (module docstring)
        if signal.kind in (_edition.KIND_E1, _edition.KIND_E2):  # 0.8.5 (D287(2), PLAN-D287 I2): an E2 is tiered too
            # T-419 (D471 Q10): ONE function decides the tier -- gates.tier lifts the carry cap for a Still Watching row
            # (both editions); the keyword is passed only then, so every other call is byte-identical to before
            v1r = isinstance(signal.snapshot, dict) and "v1r" in signal.snapshot
            tier = str(gates.v1r_tier(signal.snapshot) if v1r else
                       gates.tier(ctx, self.gates_cfg, **({"still_watching": True} if sw else {})))
            if sw:
                from gammad.v1.carry_break import break_entered

                if break_entered(signal.snapshot):
                    tier = TIER_ENTRY
            if carried:  # D160(3): reason `carried`; capped (belt and braces) in the CLIENT edition only (S1.9.0 D380 (6))
                if not sw:
                    tier = str(gates.cap_carried(tier, ctx))  # type: ignore[arg-type]
                if not self.is_client:  # T-419 (D437): the internal reason word never reaches a client row
                    ctx = replace(ctx, gate_reasons=(*ctx.gate_reasons, CARRIED))
                if sw and not self.is_client:  # T-419: the GROUP carries only the client's lists (an EXPIRY row: owner)
                    off_client = not group_list_ok(meta.list_name)
            elif not self.is_client and not held:  # T-202 (D176(1)); synchronous, like R4: no await before the INSERT
                off_client = not in_client_view(self.db, trade_date, meta)
            # R4 (D114(5)): one ENTRY per underlying per trade date. No await between this check and the INSERT
            # below, so two entries of one stock routed concurrently cannot both pass it. T-202 (D176(1)), owner: a
            # client-view ENTRY counts only the client-view ENTRYs; an off_client one counts every V1 ENTRY of the stock,
            # as does the client edition. 0.8.5 (PLAN-D287 I5, D288 Q3): per KIND -- the group's (the owner's E2, the
            # client's E1) and the owner E1's own R4 never demote each other.
            # T-310: a HELD row takes R4 and its client view at its release (`release_held`, in signal-time order).
            # S1.9.0 (D380 (6)): an OWNER carried ENTRY counts like an off_client one (its stored off_client stays False).
            # T-419 (D471 Q7): a Still Watching ENTRY is a client-view one -- one entry per stock-day across listed +
            # carried (has_client_entry_today counts SW rows)
            owner_only_carry = carried and not sw
            if not v1r and not held and tier == TIER_ENTRY and self._r4_taken(trade_date, meta.symbol, off_client or owner_only_carry,
                                                                  signal.kind):  # fmt: skip
                # 0.8.8 ("Group counts only its own lists"): asked BEFORE the demotion and the INSERT, no await between
                group_entry = self._group_takes(trade_date, meta, signal.kind, off_client=off_client,
                                                carried=owner_only_carry)  # fmt: skip
                tier = TIER_WATCHLIST
                ctx = replace(ctx, gate_reasons=(*ctx.gate_reasons, DUP_STOCK))
                log.info("R4: second entry on %s today (%s): shown as WATCHLIST%s", meta.symbol, signal.security_id,
                         "; the GROUP's first entry of the stock (its own lists only): sent there as ENTRY"
                         if group_entry else "")  # fmt: skip
        if ctx.off_client != off_client:  # the stored flag is the router's decision (never a caller's; client False)
            ctx = replace(ctx, off_client=off_client)
        warnings = signal_warnings(meta, signal, self.alerts_cfg, self.calendar)
        sw_extra = {SW_CTX_KEY: True} if sw else {}  # T-419: the Still Watching marker (client-safe, both editions)
        if held:
            signal_id, _p, _c = self._insert_signal(signal, meta, ctx, trade_date, SCANNER_LIVE, tier, warnings,
                                                    extra_ctx={HELD_KEY: HELD_PENDING, **sw_extra})  # fmt: skip
            (log.debug if self.is_client else log.info)(
                "T-310: V1 %s %s (signal %s, %s) held until the 09:20 list post", signal.kind, meta.symbol, signal_id,
                tier or "-")  # fmt: skip
            return signal_id
        signal_id, payload, ctx_d = self._insert_signal(signal, meta, ctx, trade_date, SCANNER_LIVE, tier, warnings,
                                                        extra_ctx=sw_extra or None)  # fmt: skip
        return await self._route_recorded(signal_id, signal, meta, ctx, trade_date, tier=tier, carried=carried,
                                          off_client=off_client, payload=payload, ctx_d=ctx_d, warnings=warnings,
                                          group_entry=group_entry, sw=sw)  # fmt: skip

    def _group_takes(self, trade_date: str, meta: ContractMeta, kind: str, *, off_client: bool, carried: bool) -> bool:
        """0.8.8 (owner, 13 Oct, label verbatim "Group counts only its own lists (Recommended)"), OWNER edition: is this
        ENTRY -- which the owner's R4 is demoting right now (`_r4_taken` said yes) -- nonetheless the GROUP's first entry
        of the stock-day? Yes when it is the group's entry kind (the owner E2), in the client view (not off_client, not
        carried), on a group list (`group_list_ok`) and the group has no entry of the stock yet (`has_group_entry_today`:
        the group's lists only, so an earlier EXPIRY ENTRY -- owner only, D432 -- does not count). Then the group gets
        the ENTRY card a client app sends; the stored tier (WATCHLIST + dup_stock), the dashboard and the owner's
        personal chat are exactly as before. Asked before the INSERT with no await in between (as R4). Client: never."""
        if self.is_client or off_client or carried or kind != _edition.client_entry_kind(client=False):
            return False
        if not group_list_ok(meta.list_name):
            return False
        return not has_group_entry_today(self.db, trade_date, meta.symbol, kind)

    def _r4_taken(self, trade_date: str, symbol: str, off_client: bool, kind: str = _edition.KIND_E1) -> bool:
        """R4's question for an ENTRY of `kind` (on_signal and release_held ask it the same way). 0.8.5 (PLAN-D287 I5):
        only the rows of the same kind count (the group's / client's entries, and the owner E1's own R4, Q3)."""
        if self.is_client or off_client:
            return has_entry_today(self.db, trade_date, symbol, kind=kind)
        return has_client_entry_today(self.db, trade_date, symbol, kind=kind)

    def _hold_v1(self, signal: Signal) -> bool:
        """T-310: the app's hold rule (`v1_hold`); none wired = never hold. A broken rule fails OPEN (routes as 0.8.4)."""
        hook = self.v1_hold
        if hook is None:
            return False
        try:
            return bool(hook(signal))
        except Exception:
            log.exception("V1 hold check failed for sec %s; routed at once", signal.security_id)
            return False

    async def _route_recorded(
        self,
        signal_id: int,
        signal: Signal,
        meta: ContractMeta,
        ctx: SignalContext,
        trade_date: str,
        *,
        tier: str | None,
        carried: bool,
        off_client: bool,
        payload: Mapping[str, Any],
        ctx_d: Mapping[str, Any],
        warnings: Sequence[str],
        held_hm: HeldLine | None = None,
        trade: bool = True,
        group_entry: bool = False,
        sw: bool = False,
    ) -> int:
        """A RECORDED V1 signal's fan-out (on_signal; T-310 `release_held` for a held row): V1 PLUS, the dashboard push,
        the Telegram decision, then the trade hook. `held_hm` = a released held signal's HeldLine (its cards carry the
        held line); `trade=False` = never the trade hook (an invalid held row). `group_entry` (0.8.8, `_group_takes`):
        an owner-R4-demoted entry the GROUP takes as its ENTRY card (`_route_group_signal`); everything else -- the push,
        the stored tier, no personal copy, the paper trade -- is the demoted row's, unchanged.
        0.8.9 T-419 `sw` (D471 Q7): a carried row while Still Watching is on. Its ENTRY routes as a LISTED entry does --
        the group's entry (owner E2 / client E1) to the GROUP as the Still Watching card (+ the owner's personal copy),
        the owner E1 to his personal chat, an off-group-list (EXPIRY) row personal only -- and runs the trade hook. Below
        ENTRY: the owner's silent personal card as before (`_route_carried`); the client's dashboard only."""
        # 0.8.1 V1 PLUS (owner): derived right after the INSERT, still with no await in between (module docstring).
        # T-310 review L8: a released held signal never re-points a PLUS row whose card already went out.
        plus_id = None
        if tier == TIER_ENTRY and not carried and signal.kind == _edition.KIND_E1:  # D288 Q6: V1 PLUS from the E1 only
            plus_id = self._derive_plus(signal_id, trade_date, meta.symbol, plus=True, repoint_sent=held_hm is None)
        reasons = list(ctx.gate_reasons)
        muted = self.is_muted(signal.security_id)
        # Registered before the first await: an E1 routed while this E2 is still queued waits for it.
        inflight: asyncio.Future[int | None] | None = None
        if signal.kind == "E2":
            inflight = asyncio.get_running_loop().create_future()
            self._e2_inflight[signal_id] = inflight
        message_id: int | None = None
        try:
            await self._push(
                {
                    "type": "signal",
                    "signal_id": signal_id,
                    **payload,
                    "muted": muted,
                    "ctx": ctx_d,
                    "hold": client_hold(ctx.hold) if self.is_client else ctx.hold,
                    "gate_status": ctx.gate_status,
                    "gate_reasons": reasons,
                    "tier": tier,
                    "scanner": SCANNER_LIVE,
                    "warnings": warnings,
                },
                signal_id,
            )
            if plus_id is not None:
                await self._push_derived(plus_id)
            if carried and sw and tier != TIER_ENTRY and self.is_client:
                # T-419: a client's Still Watching row below ENTRY is dashboard only (no NEAR ENTRY / WATCH card)
                self._record(signal_id, CH_GROUP, ST_GATED, None,
                             "watchlist: " + (",".join((*ctx.gate_reasons, *ctx.gate_unknown)) or "-"))  # fmt: skip
            elif carried and not (sw and tier == TIER_ENTRY):
                # D160(3): a silent PERSONAL card only, never the group (T-419: an owner Still Watching row below ENTRY,
                # or any carried row while Still Watching is off)
                message_id = await self._route_carried(signal_id, signal, meta, ctx, muted, tier=tier,
                                                       warnings=warnings, held_hm=held_hm)  # fmt: skip
            elif not self.is_client and signal.kind == _edition.KIND_E1:
                # 0.8.5 (D287(1), PLAN-D287 I4): the OWNER E1 goes to the PERSONAL chat only, never the group (ENTRY with
                # sound, NEAR ENTRY silent, WATCH ONLY on the dashboard)
                message_id = await self._route_owner_e1(signal_id, signal, meta, ctx, trade_date, muted, tier=tier,
                                                        warnings=warnings, off_client=off_client,
                                                        also_plus=plus_id is not None, held_hm=held_hm)  # fmt: skip
            elif off_client and tier in GROUP_TIERS:  # T-202 (D176(1)): the owner's PERSONAL chat only, with the line
                message_id = await self._route_personal_only(
                    signal_id, signal, meta, ctx, muted, tier=tier, warnings=warnings, silent=tier in SILENT_TIERS,
                    not_in_group=True, also_plus=plus_id is not None, held_hm=held_hm,
                )  # fmt: skip
            else:
                message_id = await self._route_group_signal(
                    signal_id, signal, meta, ctx, trade_date, muted, tier=tier, warnings=warnings,
                    also_plus=plus_id is not None, held_hm=held_hm, group_entry=group_entry,
                )  # fmt: skip
        finally:
            if inflight is not None:
                self._e2_inflight.pop(signal_id, None)
                if not inflight.done():
                    inflight.set_result(message_id)
        # D114(4): a WATCHLIST E1 is dashboard only -- no trade either. 0.8.1 (D157(2), D161(2)): never a carried
        # contract, E1 or E2 (the tracker refuses them too). 0.8.2 T-202 (D176(1)): never an off_client E1 (ditto).
        # S1.9.0 (D380 (6), D382 default): OWNER edition, a carried ENTRY (E1 or E2) is tracked -- its entry message is
        # the personal card; a carried row below ENTRY never trades (no paper E2 trade either)
        # 0.8.9 T-419 (D471 Q7): a Still Watching ENTRY is tracked in BOTH editions (trades.scanner SW, the tracker's)
        if not trade or (carried and (tier != TIER_ENTRY or (self.is_client and not sw))):
            return signal_id
        if not self.is_client and signal.kind == _edition.KIND_E2:
            # 0.8.5 (D288 Q5): the owner keeps an E2 trade on EVERY E2 -- a tier-ENTRY one in the client view is the real
            # trade (its entry message = the GROUP card); any other is the day's paper trade, whose follow-ups reply to
            # the owner's PERSONAL card (the copy or the personal-only card), never the group
            routed_mid = tier in (TIER_ENTRY, None) or off_client  # the routed message IS its entry message
            hook_mid = message_id if routed_mid else sent_message_id(self.db, signal_id, CH_PERSONAL)
            await self._run_trade_hook(signal_id, signal, meta, ctx, hook_mid)
        elif tier != TIER_WATCHLIST and not off_client:
            await self._run_trade_hook(signal_id, signal, meta, ctx, message_id)
        return signal_id

    # -- T-310 (D278(5)): the V1 hold's release

    def _held_rows(self, trade_date: str) -> list[sqlite3.Row | tuple[Any, ...]]:
        """The day's V1 rows still held (ctx_json held = pending), in signal-time order."""
        return self.db.execute(
            "SELECT id, ts_ms, security_id, kind, tier, gate_reasons, payload_json, ctx_json FROM signals"
            " WHERE trade_date = ? AND scanner = ? AND json_valid(ctx_json)"
            f" AND json_extract(ctx_json, '$.{HELD_KEY}') = ? ORDER BY ts_ms, id",
            (trade_date, SCANNER_LIVE, HELD_PENDING),
        ).fetchall()

    def has_held(self, trade_date: str) -> bool:
        """Is any V1 row of `trade_date` still held? (the app's restart / backstop pass asks before spawning a release)"""
        return bool(self._held_rows(trade_date))

    def held_keys(self, trade_date: str) -> list[tuple[int, int]]:
        """(security_id, ts_ms) of every row still held (a restarted app tracks their lows from its first tick)."""
        return [(int(r[2]), int(r[1])) for r in self._held_rows(trade_date)]

    def held_pending(self, signal_id: int) -> bool:
        """Is the signals row `signal_id` still held for the 09:20 list post? (the app grades a held row at its release,
        T-310 review M1, never at hold time)"""
        row = self.db.execute(
            f"SELECT CASE WHEN json_valid(ctx_json) THEN json_extract(ctx_json, '$.{HELD_KEY}') END FROM signals"
            " WHERE id = ?",
            (int(signal_id),),
        ).fetchone()
        return row is not None and row[0] == HELD_PENDING

    def _stamp_invalid(self, row: Any, stored_ctx: Mapping[str, Any], why: str) -> None:
        """Stamp a held row (id, kind, tier, gate_reasons as `_held_rows` / `expire_held` read them) INVALID with `why`,
        routing nothing: tier WATCHLIST (an E1, a tiered E2: 0.8.5 AM1) + HELD_INVALID + ctx held_why. Commits. (L6: a release that raised before
        its own stamp; a past day's row still pending.)"""
        signal_id, kind, tier = int(row[0]), str(row[1] or ""), row[2]
        reasons = [x for x in reasons_list(row[3]) if x != HELD_INVALID] + [HELD_INVALID]
        new_ctx = {**stored_ctx, "gate_reasons": reasons, HELD_KEY: HELD_INVALID_STATE, HELD_WHY_KEY: why}
        self.db.execute("UPDATE signals SET tier = ?, gate_reasons = ?, ctx_json = ? WHERE id = ?",
                        (TIER_WATCHLIST if kind == _edition.KIND_E1 or tier is not None else tier, ",".join(reasons),
                         json.dumps(new_ctx, sort_keys=True), signal_id))  # fmt: skip
        self.db.commit()

    def expire_held(self, today: str) -> int:
        """T-310 review L6: every V1 row of a day BEFORE `today` still held (its process never reached a release that day)
        is stamped INVALID `expired` -- never sent, never a trade, its WHY NOT says so. The app calls this at a day's
        first supervisor pass. Returns the rows stamped. Never raises."""
        try:
            rows = self.db.execute(
                "SELECT id, kind, tier, gate_reasons, ctx_json, trade_date FROM signals WHERE trade_date < ?"
                f" AND scanner = ? AND json_valid(ctx_json) AND json_extract(ctx_json, '$.{HELD_KEY}') = ?",
                (today, SCANNER_LIVE, HELD_PENDING),
            ).fetchall()
            for r in rows:
                self._stamp_invalid(r, _json_or_none(r[4]) or {}, HELD_WHY_EXPIRED)
        except Exception:
            log.exception("T-310: the held V1 signals before %s could not be expired", today)
            return 0
        if rows:
            (log.debug if self.is_client else log.warning)(
                "T-310: %d held V1 signal(s) of an earlier day never released: stamped invalid (expired)", len(rows))
        return len(rows)

    async def release_held(
        self,
        trade_date: str,
        *,
        ltp_of: Callable[[int], float | None],
        low_since: Callable[[int, int], float | None],
        now_ms: int | None = None,
        list_post: str = HELD_POST_SENT,
    ) -> dict[str, int]:
        """T-310 (D278(5)): release the day's held V1 rows, oldest first, read back from the STORE (a restart between
        the hold and this call loses nothing; each row is stamped before it is routed, so it is never sent twice).
        A row is INVALID -- stored tier WATCHLIST (an E1) + HELD_INVALID + ctx held_why, routed as a dashboard-only row,
        never a card, never a trade -- when (a) its contract has no live listed row today (retired by the 09:20 build:
        HELD_WHY_RETIRED), (b) the option traded at or below the signal's stop since the signal (`low_since`:
        HELD_WHY_STOP) or (c) there is no LTP to enter at (`ltp_of` None: HELD_WHY_NO_PRICE, review L7 -- never the stale
        signal price). Else it is RELEASED as live would route it now: an E1 re-checked for the client view (owner) and
        R4 (a stock that already has an ENTRY = WATCHLIST + dup_stock), its cards carry the HeldLine (the signal time;
        `list_post` = what became of the day's 09:20-kind list post, review L9) and the card / trade price is the release
        LTP dated `now_ms`; the stored row keeps the signal's time and price, its ctx the release LTP and time.
        Review fixes: ONE release at a time (a lock), and the store is RE-READ after every row, so a V1 signal the app
        held while this ran (M2: a live signal never routes ahead of an earlier held one) is released in signal-time
        order by this same call; a row whose release raises before its stamp is stamped INVALID `error` (L6: never left
        pending); each routed row is handed to `on_released` with the V1 PLUS ids it derived (M1: graded at release).
        Returns {released, invalid} counts."""
        out = {HELD_RELEASED: 0, HELD_INVALID_STATE: 0}
        if self._release_lock is None:
            self._release_lock = asyncio.Lock()
        async with self._release_lock:
            stuck: set[int] = set()  # rows whose error stamp failed too: never retried in this call (no endless loop)
            while True:
                rows = [r for r in self._held_rows(trade_date) if int(r[0]) not in stuck]
                if not rows:
                    break
                r = rows[0]
                signal_id = int(r[0])
                stored_ctx = _json_or_none(r[7])
                try:
                    await self._release_one(r, stored_ctx if isinstance(stored_ctx, dict) else {}, trade_date,
                                            ltp_of=ltp_of, low_since=low_since, now_ms=now_ms, out=out,
                                            list_post=list_post)  # fmt: skip
                except Exception:  # one bad row never keeps the others held, and is never left pending itself
                    log.exception("T-310: held signal %s could not be released", signal_id)
                    try:
                        if self.held_pending(signal_id):
                            self._stamp_invalid((signal_id, r[3], r[4], r[5]), stored_ctx or {}, HELD_WHY_ERROR)
                            out[HELD_INVALID_STATE] += 1
                    except Exception:
                        log.exception("T-310: held signal %s could not be stamped invalid", signal_id)
                    if self.held_pending(signal_id):
                        stuck.add(signal_id)
        if any(out.values()):
            (log.debug if self.is_client else log.info)("T-310: held V1 signals of %s: %d released, %d invalid",
                                                         trade_date, out[HELD_RELEASED], out[HELD_INVALID_STATE])  # fmt: skip
        return out

    def _released(self, signal_id: int, kind: str, meta: ContractMeta) -> None:
        """A held row is stamped and routed: drain the V1 PLUS ids it derived and hand them to `on_released` (M1)."""
        derived = self.pop_derived(signal_id)
        hook = self.on_released
        if hook is None:
            return
        try:
            hook(signal_id, kind, meta, derived)
        except Exception:
            log.exception("T-310: the released signal %s could not be handed on", signal_id)

    async def _release_one(
        self,
        r: Any,
        stored_ctx: dict[str, Any],
        trade_date: str,
        *,
        ltp_of: Callable[[int], float | None],
        low_since: Callable[[int, int], float | None],
        now_ms: int | None,
        out: dict[str, int],
        list_post: str = HELD_POST_SENT,
    ) -> int:
        signal_id, ts_ms, sec = int(r[0]), int(r[1]), int(r[2])
        payload = _json_or_none(r[6]) or {}
        signal = Signal(**{k: payload[k] for k in _SIGNAL_FIELDS if k in payload})
        meta = ContractMeta(**{f.name: payload[f.name] for f in fields(ContractMeta) if f.name in payload})
        names = {f.name for f in fields(SignalContext)}
        ctx = SignalContext(**{k: (tuple(v) if k in ("gate_reasons", "gate_unknown") else v)
                               for k, v in stored_ctx.items() if k in names})  # fmt: skip
        # T-419: a Still Watching row is known by its stored marker (a client row stores no `carried` key / CARRY tag); it
        # was recorded as one, so it is released as one
        sw = bool(stored_ctx.get(SW_CTX_KEY))
        if sw:
            ctx = replace(ctx, carried=True)
            meta = replace(meta, carried=True)
        carried = bool(ctx.carried or meta.carried)
        tier = None if r[4] is None else str(r[4])
        live = self.db.execute(  # D429: a CLIENT's EXPIRY row is no live row (held before 0.8.8 -> not sent, no trade)
            "SELECT 1 FROM watchlist WHERE trade_date = ? AND security_id = ? AND retired_run_id IS NULL"
            + _edition.v1_lists_sql(owner=not self.is_client) + " LIMIT 1",
            (trade_date, sec),
        ).fetchone()
        low = None
        ltp: float | None = None
        why: str | None = None
        if live is None:
            why = HELD_WHY_RETIRED
        else:
            low = low_since(sec, ts_ms)
            if low is not None and signal.sl > 0 and float(low) <= float(signal.sl):
                why = HELD_WHY_STOP
        if why is None:  # L7: no price to enter at = never sent (the signal's own price is minutes stale)
            got = ltp_of(sec)
            if got is not None and float(got) > 0:
                ltp = float(got)
            else:
                why = HELD_WHY_NO_PRICE
        off_client = False
        group_entry = False  # 0.8.8: an owner-R4-demoted release the GROUP takes as its ENTRY (`_group_takes`)
        stamp: dict[str, Any]
        if why is not None:  # invalid: never sent, never a trade; a dashboard-only row with its reason
            ctx = replace(ctx, gate_reasons=(*ctx.gate_reasons, HELD_INVALID))
            if signal.kind == _edition.KIND_E1 or tier is not None:  # 0.8.5 AM1: a tiered E2 is demoted too
                tier = TIER_WATCHLIST
            stamp = {HELD_KEY: HELD_INVALID_STATE, HELD_WHY_KEY: why}
            out[HELD_INVALID_STATE] += 1
            log.info("T-310: held V1 signal %s on %s not sent: %s (low %s, stop %s)", signal_id, meta.symbol, why,
                     low, signal.sl)  # fmt: skip
        else:
            # 0.8.5 AM1: a released held E2 (tiered) passes the client view and R4 as an E1 does, per kind (I5)
            # S1.9.0 (D380 (6)): an OWNER carried ENTRY takes R4 here too, as an off_client one (stored off_client False)
            if signal.kind in (_edition.KIND_E1, _edition.KIND_E2) and tier is not None:
                if not self.is_client and not carried:  # the client view the 09:20 list post just showed (T-202)
                    off_client = not in_client_view(self.db, trade_date, meta)
                elif sw and not self.is_client:  # T-419: as on_signal (the group carries the client's lists only)
                    off_client = not group_list_ok(meta.list_name)
                owner_only_carry = carried and not sw  # T-419: a Still Watching ENTRY takes R4 as a client-view one
                if not (isinstance(signal.snapshot, dict) and "v1r" in signal.snapshot) and tier == TIER_ENTRY and self._r4_taken(trade_date, meta.symbol, off_client or owner_only_carry,
                                                         signal.kind):  # fmt: skip
                    # 0.8.8 ("Group counts only its own lists"): before the stamp below (this row is still pending)
                    group_entry = self._group_takes(trade_date, meta, signal.kind, off_client=off_client,
                                                    carried=owner_only_carry)  # fmt: skip
                    tier = TIER_WATCHLIST
                    ctx = replace(ctx, gate_reasons=(*ctx.gate_reasons, DUP_STOCK))
                    log.info("R4: held entry %s on %s: the stock already has an entry: shown as WATCHLIST", signal_id,
                             meta.symbol)  # fmt: skip
            release_ms = int(now_ms) if now_ms is not None else int(self._clock_ms())
            stamp = {HELD_KEY: HELD_RELEASED, HELD_RELEASE_MS_KEY: release_ms, HELD_RELEASE_LTP_KEY: ltp}
            out[HELD_RELEASED] += 1
        if not self.is_client:
            ctx = replace(ctx, off_client=off_client)
        new_ctx = {**stored_ctx, "gate_reasons": list(ctx.gate_reasons), "gate_status": ctx.gate_status, **stamp}
        if not self.is_client:
            new_ctx[OFF_CLIENT] = off_client
        # stamped (committed) BEFORE the first await: a crash after this never sends it twice
        self.db.execute("UPDATE signals SET tier = ?, gate_status = ?, gate_reasons = ?, ctx_json = ? WHERE id = ?",
                        (tier, ctx.gate_status, ",".join(ctx.gate_reasons), json.dumps(new_ctx, sort_keys=True),
                         signal_id))  # fmt: skip
        self.db.commit()
        ctx_d = wire_ctx(new_ctx, client=self.is_client)
        if why is not None:
            await self._route_recorded(signal_id, signal, meta, ctx, trade_date, tier=tier, carried=carried,
                                       off_client=False, payload=payload, ctx_d=ctx_d,
                                       warnings=list(payload.get("warnings") or ()), trade=False, sw=sw)  # fmt: skip
            self._released(signal_id, signal.kind, meta)
            return signal_id
        rsig = replace(signal, ltp=stamp[HELD_RELEASE_LTP_KEY], ts_ms=stamp[HELD_RELEASE_MS_KEY])
        warnings = signal_warnings(meta, rsig, self.alerts_cfg, self.calendar)
        held = HeldLine(tpl.ist_hm(ts_ms), list_post, tpl.ist_hm(stamp[HELD_RELEASE_MS_KEY]))
        await self._route_recorded(signal_id, rsig, meta, ctx, trade_date, tier=tier, carried=carried,
                                   off_client=off_client, payload=payload, ctx_d=ctx_d, warnings=warnings,
                                   held_hm=held, group_entry=group_entry, sw=sw)  # fmt: skip
        self._released(signal_id, signal.kind, meta)
        return signal_id

    # -- 0.8.1: V1 PLUS + carry

    def _derive_plus(
        self, origin_id: int, trade_date: str, symbol: str, *, plus: bool, repoint_sent: bool = True
    ) -> int | None:
        """plus.derive_plus for an owner origin row (synchronous: called between the origin's INSERT and the first
        await). Records the id for `pop_derived`. The client edition derives nothing. Never raises. `repoint_sent`
        False (a released held signal, T-310 review L8) = a PLUS row whose card already went out is never re-pointed."""
        if self.is_client:
            return None
        try:
            pid = plus_mod.derive_plus(self.db, trade_date, symbol, {"id": origin_id, "plus": plus},
                                       last_bar_close=self.v1b_cfg.last_bar_close,
                                       repoint_sent=repoint_sent)  # fmt: skip
        except Exception:  # the origin is already recorded: a PLUS bug must never cost its card or its trade
            log.exception("V1 PLUS derivation failed for signal %s", origin_id)
            return None
        if pid is not None:
            self._derived.setdefault(origin_id, []).append(pid)
        return pid

    def pop_derived(self, signal_id: int) -> list[int]:
        """The V1 PLUS row ids `signal_id` derived (CONTRACTS 5): the app grades them after the origin, once."""
        return self._derived.pop(int(signal_id), [])

    async def _push_derived(self, plus_id: int) -> None:
        """The dashboard push of a V1 PLUS row (type `scanner`, like a shadow row). Never raises."""
        if self.dashboard_push is None:
            return
        row = fetch_signal(self.db, plus_id)
        if row is None:
            return
        ctx = row.pop("ctx", None) or {}
        message = {"type": WS_TYPE_SCANNER, "signal_id": plus_id, **row, "ctx": ctx, "scanner": SCANNER_PLUS,
                   "gate_reasons": reasons_list(row.get("gate_reasons"))}  # fmt: skip
        try:
            result = self.dashboard_push(message)
            if inspect.isawaitable(result):
                await result
        except Exception as exc:  # noqa: BLE001 -- a broken pusher must not cost the record
            log.warning("dashboard push (V1 PLUS %s) failed: %s", plus_id, exc)

    async def _route_carried(
        self,
        signal_id: int,
        signal: Signal,
        meta: ContractMeta,
        ctx: SignalContext,
        muted: bool,
        *,
        tier: str | None,
        warnings: Sequence[str],
        held_hm: HeldLine | None = None,
    ) -> int | None:
        """D160(3): a V1 signal on a CARRY row. An E1 at STRONG WATCHLIST (the cap) -> a SILENT card to the PERSONAL
        chat only (`_route_personal_only`); a WATCHLIST E1 is dashboard only (a 'gated' personal row); an owner E2 ->
        its personal card as today but SILENT. Never the group (no group alerts row is written at all). 0.8.5 (PLAN-D287
        I2 / AM1): a TIERED E2 takes the E1's path here (capped at STRONG, a silent personal card); only an untiered
        (legacy) E2 keeps `_route_personal_e2`. S1.9.0 (D380 (6), D382 default "carried ENTRY = personal chat, tracked,
        never the group"): an OWNER carried ENTRY (E1 or tiered E2) -> the same personal-only card WITH sound; the
        caller runs its trade hook with the returned message id. NEAR ENTRY stays silent."""
        if tier is None and signal.kind != _edition.KIND_E1:
            return await self._route_personal_e2(signal_id, signal, meta, ctx, muted, warnings, silent=True,
                                                 held_hm=held_hm)  # fmt: skip
        if tier not in GROUP_TIERS:
            why = ",".join((*ctx.gate_reasons, *ctx.gate_unknown)) or "-"
            self._record(signal_id, CH_PERSONAL, ST_GATED, None, "watchlist: " + why)
            return None
        return await self._route_personal_only(signal_id, signal, meta, ctx, muted, tier=tier, warnings=warnings,
                                               silent=tier != TIER_ENTRY, held_hm=held_hm)  # fmt: skip

    async def _route_owner_e1(
        self,
        signal_id: int,
        signal: Signal,
        meta: ContractMeta,
        ctx: SignalContext,
        trade_date: str,
        muted: bool,
        *,
        tier: str | None,
        warnings: Sequence[str],
        off_client: bool,
        also_plus: bool,
        held_hm: HeldLine | None = None,
    ) -> int | None:
        """0.8.5 (D287(1), PLAN-D287 I4, D288 Q2): the OWNER E1 -> the PERSONAL chat ONLY, never the group (the group's
        entry is the E2 bar close): ENTRY with sound, NEAR ENTRY (STRONG_WATCHLIST) silent, both with the DESK block
        (`After E2 hh:mm @ Rs`) and as a reply to its own E2's personal card (D65's thread); WATCH ONLY = the dashboard
        (a 'gated' personal row). An off_client E1 keeps the `not in the group: <tag>` line. Returns the personal
        message id when sent (the E1 trade's entry message: its follow-ups reply to it there), else None."""
        if tier not in GROUP_TIERS:
            if muted:
                self._record(signal_id, CH_PERSONAL, ST_MUTED, None, None)
                return None
            why = ",".join((*ctx.gate_reasons, *ctx.gate_unknown)) or "-"
            self._record(signal_id, CH_PERSONAL, ST_GATED, None, "watchlist: " + why)
            return None
        e2 = self._own_e2_signal(signal_id, signal, trade_date)
        reply_to = await self._e2_personal_target(signal_id, signal, trade_date)
        return await self._route_personal_only(
            signal_id, signal, meta, ctx, muted, tier=tier, warnings=warnings, silent=tier in SILENT_TIERS,
            not_in_group=off_client, also_plus=also_plus, held_hm=held_hm, e2=e2, reply_to=reply_to,
        )  # fmt: skip

    async def _route_personal_only(
        self,
        signal_id: int,
        signal: Signal,
        meta: ContractMeta,
        ctx: SignalContext,
        muted: bool,
        *,
        tier: str | None,
        warnings: Sequence[str],
        silent: bool,
        scanner: str | None = None,
        not_in_group: bool = False,
        also_plus: bool = False,
        not_in_group_tag: str | None = None,
        held_hm: HeldLine | None = None,
        e2: Signal | None = None,
        reply_to: int | None = None,
        render: Callable[[], str] | None = None,
    ) -> int | None:
        """0.8.8 T-343 (a) (D382 (1)): `render` = a card of its own instead of the signal card (the V1 B zone FYI,
        `_send_v1b_zone_fyi`): rendered inside the same try (a template bug = a 'failed' row) and sent with NO keyboard
        (an FYI is not a trade: no Copy order); every other branch (client, muted, paused, no personal chat) is as below.
        0.8.5 (PLAN-D287 I4): or the OWNER E1 (`_route_owner_e1`): `e2` = its own E2 (the DESK's `After E2` line),
        `reply_to` = that E2's personal message (D65's thread).
        0.8.1 (D160(1)(3)): an owner card that goes to the PERSONAL chat ONLY -- a V1 PLUS-only trade (`scanner`
        V1 PLUS, with sound) or a carried signal (silent; the template adds the carried line from ctx / meta.carried).
        0.8.2 T-202 (D176(1)): or an off_client V1 ENTRY (sound) / STRONG WATCHLIST (silent): `not_in_group` puts
        `tpl.add_not_in_group(text, meta.tag)` under the headline, `also_plus` the D161(1) `also V1 PLUS` line (ENTRY).
        S1.7.0 (T-267, D249 / D254): or an owner-only V2 Scanner ENTRY (`scanner` V2T, sound; HOI-2 / EXPIRY lists):
        `not_in_group_tag` names why in that line (the EXPIRY list, else the tag); the V2 card renders with the
        recorded context (no V1 hold-at-entry). The CALLER runs the trade hook for it (D249: tracked as backtested).
        Modelled on `_route_personal_e2`: muted ->
        'muted', paused -> 'paused', no usable personal chat -> 'suppressed' (NEVER a group fallback), else the card
        WITH the DESK block and the tier keyboard. Rows on channel telegram_personal; the client edition sends none.
        Returns the personal message_id when sent, else None; the caller never runs the trade hook for it."""
        ch = CH_PERSONAL
        if self.is_client:  # defensive: the client edition has no V1 PLUS and no carry (D159(1), D157(2))
            return None
        if muted:
            self._record(signal_id, ch, ST_MUTED, None, None)
            return None
        if self.paused:
            self._record(signal_id, ch, ST_PAUSED, None, None)
            return None
        if not self.has_personal_chat():
            self._record(signal_id, ch, ST_SUPPRESSED, None, "no personal chat: an owner-only card never goes to the "
                                                             "group")  # fmt: skip
            return None
        if render is not None:  # D382 (1): an owner card of its own (no keyboard), recorded like every other
            try:
                text = render()
            except Exception as exc:  # a template bug is a 'failed' row, never a lost record
                log.exception("personal-only card of signal %s failed to render", signal_id)
                self._record(signal_id, ch, ST_FAILED, None, f"render: {type(exc).__name__}: {exc}")
                return None
            status, error, mid = await self._deliver(ch, self.personal_chat_id, text, silent=silent)
            sent = status == ST_SENT
            self._record(signal_id, ch, status, self._clock_ms() if sent else None, error, mid)
            return mid if sent else None
        card_ctx = ctx if scanner == SCANNER_V2T else card_context(meta, signal, ctx, self.calendar)
        try:
            # a carried card: the template reads ctx / meta.carried (the carried line); a PLUS-only card: its headline
            text = tpl.signal_card(meta, signal, ctx=card_ctx, tier=tier, warnings=list(warnings), desk=True,
                                   **_card_extras(scanner=scanner if tier == TIER_ENTRY else None, e2=e2),
                                   **self._sw_card_extras(meta, card_ctx))  # fmt: skip
            if also_plus and tier == TIER_ENTRY:
                text = tpl.add_also_plus(text)
            if held_hm is not None:  # T-310: an owner-only card (personal chat): the owner's words
                text = held_line(text, held_hm, owner=True)
            if not_in_group:  # applied last: the first line under the headline
                text = tpl.add_not_in_group(text, not_in_group_tag if not_in_group_tag is not None else meta.tag)
        except Exception as exc:  # a template bug is a 'failed' row, never a lost record
            log.exception("personal-only card of signal %s failed to render", signal_id)
            self._record(signal_id, ch, ST_FAILED, None, f"render: {type(exc).__name__}: {exc}")
            return None
        buttons = tier_buttons(meta, signal, card_ctx, tier) if tier is not None else signal_buttons(meta, signal,
                                                                                                    card_ctx)
        status, error, mid = await self._deliver(ch, self.personal_chat_id, text, reply_to=reply_to, buttons=buttons,
                                                 silent=silent)  # fmt: skip
        sent = status == ST_SENT
        self._record(signal_id, ch, status, self._clock_ms() if sent else None, error, mid)
        return mid if sent else None

    async def send_carry_line(self, trade_date: str) -> bool:
        """D160(4) / D161(6): after the 09:20 build, OWNER edition: `+N carried (P primed)` (tpl.carry_line) to the
        PERSONAL chat only -- never the group, not even without a personal chat ('suppressed' manual_sends row).
        N = the day's live CARRY rows, P = those whose carry_state is PRIMED. Nothing carried -> nothing sent. The
        client edition never sends it and records nothing. Not blocked by pause (an ops line, like the operator
        summary). Returns whether it was sent. Never raises for a send failure."""
        if self.is_client:
            return False
        try:
            n, primed, floor = self.db.execute(
                "SELECT COALESCE(SUM(retired_run_id IS NULL), 0),"
                " COALESCE(SUM(retired_run_id IS NULL AND carry_state = 'PRIMED'), 0),"
                " COALESCE(SUM(retired_run_id IS NOT NULL AND carry_reason = ?), 0)"
                " FROM watchlist WHERE trade_date = ? AND tag = ?",
                (CARRY_FLOOR_REASON, trade_date, TAG_CARRY),
            ).fetchone()  # T-282 (d2): + the carried rows retired under the listing OI floor today
        except sqlite3.OperationalError:  # a pre-v9 store has no carry_state (and no CARRY rows either)
            return False
        if not n and not floor:
            return False
        ref: dict[str, Any] = {"date": trade_date, "carried": int(n), "primed": int(primed)}
        if floor:
            ref["floor"] = int(floor)
        if not self.has_personal_chat():
            log.info("carry line: no personal chat configured; not sent (never to the group)")
            self._log_send(KIND_CARRY_LINE, ref, DEST_PERSONAL, ST_SUPPRESSED, "no personal chat configured")
            return False
        text = tpl.carry_line(int(n), int(primed), floor=int(floor)) if floor else tpl.carry_line(int(n), int(primed))
        d = await self._deliver_full(CH_PERSONAL, self.personal_chat_id, text, silent=True)
        sent = d.status == ST_SENT
        self._record(None, CH_PERSONAL, d.status, self._clock_ms() if sent else None, d.error, d.message_id)
        self._log_send(KIND_CARRY_LINE, ref, DEST_PERSONAL, d.status, d.error, self._delivery_ref(CH_PERSONAL, d))
        return sent

    def _insert_signal(
        self,
        signal: Signal,
        meta: ContractMeta,
        ctx: SignalContext,
        trade_date: str,
        scanner: str,
        tier: str | None,
        warnings: Sequence[str] = (),
        extra_ctx: Mapping[str, Any] | None = None,
    ) -> tuple[int, dict[str, Any], dict[str, Any]]:
        """INSERT one `signals` row (v3 context + v7 scanner / tier) and commit. Returns (signals.id, the payload
        dict, the ctx dict) -- the client edition's copies (D98(2)) when this router is the client's. `extra_ctx` = keys
        stored in ctx_json beside SignalContext.to_dict() (T-310: the hold state, HELD_KEY). T-305 (D278(6)): a client
        row STORES zone_limit_pct (CLIENT_STORE_CTX_KEEP); the returned (pushed) ctx copy never carries it."""
        payload = {
            **signal.to_dict(),
            **meta.to_dict(),
            "trade_date": trade_date,
            "spec_version": self.spec_version,
            "warnings": list(warnings),  # W15: the REST row carries the card's warning lines (a reload keeps them)
        }
        ctx_d = ctx.to_dict()
        stored_ctx = {**ctx_d, **dict(extra_ctx or {})}
        if self.is_client:  # D98(2): the arming cycle's E2 inputs never reach a client DB row, API or WS push
            payload, ctx_d = client_signal_dicts(payload, ctx_d)
            # T-305 (D278(6)): the stored copy keeps the zone limit (the client WHY NOT prints it); the push copy not
            # (a key with no value is not stored: a client row with nothing to print stays byte-identical to 0.8.4)
            stored_ctx = {**ctx_d, **{k: v for k, v in stored_ctx.items() if k in CLIENT_STORE_CTX_KEEP and v is not None},
                          **dict(extra_ctx or {})}  # fmt: skip
            ctx_d = wire_ctx(stored_ctx, client=True)
        else:
            ctx_d = stored_ctx
        cur = self.db.execute(
            "INSERT INTO signals (trade_date, ts_ms, security_id, list_name, expiry_date, kind, strong, ltp,"
            " oi_drop_pct, vol_ratio, zone_dist_pct, sl, latency_ms, payload_json, spec_version,"
            " hold, gate_status, gate_reasons, ctx_json, scanner, tier)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                trade_date,
                signal.ts_ms,
                signal.security_id,
                meta.list_name,
                meta.expiry_date,
                signal.kind,
                int(signal.strong),
                signal.ltp,
                signal.oi_drop_pct,
                signal.vol_ratio,
                meta.zone_dist_pct,
                signal.sl,
                signal.latency_ms,
                json.dumps(payload, sort_keys=True),
                self.spec_version,
                client_hold(ctx.hold) if self.is_client else ctx.hold,  # T-419 (D437): no `carry` in a client row
                ctx.gate_status,
                ",".join(ctx.gate_reasons),
                json.dumps(stored_ctx, sort_keys=True),
                scanner,
                tier,
            ),
        )
        self.db.commit()
        return int(cur.lastrowid or 0), payload, ctx_d

    async def _record_shadow(self, signal: Signal, meta: ContractMeta, ctx: SignalContext, scanner: str) -> int:
        """PLAN-W15 §2: a shadow scanner's signal (owner only) -> a signals row with its scanner + tier (E1; E2 NULL)
        and a dashboard push of type `scanner`. No alerts row (not even the dashboard's), no Telegram, no trade
        hook, no R4. The CLIENT edition runs no shadow scanner: anything that arrives is dropped (returns 0).
        0.8.8 T-343 (a) (D382 (1)): after the push, a V1 B E1 whose V1 verdict failed on zone alone (`v1b_zone_only`)
        -> `_send_v1b_zone_fyi` (one owner PERSONAL FYI card per contract-day; its alerts row is the only one a V1 B
        row gets)."""
        if self.is_client:
            log.debug("client: a %s signal from a non-live scanner dropped", signal.kind)
            return 0
        trade_date = ist_trade_date(signal.ts_ms)
        tier: str | None = None
        plus_ok = False
        if signal.kind == _edition.KIND_E1:
            if scanner == SCANNER_V1B:  # 0.8.1 (D161(4)): V1 B's own tier + the V1 PLUS component test
                tier = str(gates.v1b_tier(ctx, signal.snapshot, self.v1b_cfg, self.gates_cfg))
                plus_ok = gates.plus_candidate(ctx, signal.snapshot, self.v1b_cfg, self.gates_cfg)
            else:
                tier = str(gates.tier(ctx, self.gates_cfg))
            if ctx.carried:  # D157(2): capped (belt and braces) with the reason `carried`. S1.9.0 (D380 (6)): a shadow row
                # keeps the cap in BOTH editions (no card, no trade; scan/book_c8.py selects the capped V1 B rows)
                tier = str(gates.cap_carried(tier, ctx, client=True))  # type: ignore[arg-type]
                ctx = replace(ctx, gate_reasons=(*ctx.gate_reasons, CARRIED))
            # D161(4): the FIRST V1 B ENTRY per stock-day is the ENTRY (as measured); a later one is WATCHLIST +
            # dup_stock. V1's R4 is unchanged (it reads V1 rows only). No await between this check and the INSERT.
            if scanner == SCANNER_V1B and tier == TIER_ENTRY and has_entry_today(self.db, trade_date, meta.symbol,
                                                                                 SCANNER_V1B):  # fmt: skip
                tier = TIER_WATCHLIST
                ctx = replace(ctx, gate_reasons=(*ctx.gate_reasons, DUP_STOCK))
        signal_id, payload, ctx_d = self._insert_signal(signal, meta, ctx, trade_date, scanner, tier)
        plus_id = None
        if plus_ok and not ctx.carried:  # still no await since the INSERT (plus.py)
            plus_id = self._derive_plus(signal_id, trade_date, meta.symbol, plus=True)
        if self.dashboard_push is not None:
            message = {
                "type": WS_TYPE_SCANNER,
                "signal_id": signal_id,
                **payload,
                "ctx": ctx_d,
                "hold": ctx.hold,
                "gate_status": ctx.gate_status,
                "gate_reasons": list(ctx.gate_reasons),
                "tier": tier,
                "scanner": scanner,
                "warnings": signal_warnings(meta, signal, self.alerts_cfg, self.calendar),
            }
            try:
                result = self.dashboard_push(message)
                if inspect.isawaitable(result):
                    await result
            except Exception as exc:  # noqa: BLE001 -- a broken pusher must not cost the record
                log.warning("dashboard push (scanner %s) failed: %s", scanner, exc)
        if scanner == SCANNER_V1B and signal.kind == _edition.KIND_E1 and v1b_zone_only(ctx):  # D382 (1)
            await self._send_v1b_zone_fyi(signal_id, signal, meta, ctx, trade_date, tier)
        if plus_id is not None:  # D160(1): a V1 PLUS-only trade -> the owner's PERSONAL chat, never the group or a trade
            await self._push_derived(plus_id)
            await self._route_personal_only(  # the PLUS row is this signal's copy (plus.py)
                plus_id, signal, meta, ctx, self.is_muted(signal.security_id), tier=TIER_ENTRY,
                warnings=signal_warnings(meta, signal, self.alerts_cfg, self.calendar), silent=False,
                scanner=SCANNER_PLUS,
            )  # fmt: skip
        return signal_id

    def _v1b_zone_fyi_sent(self, trade_date: str, security_id: int) -> bool:
        """D382 (1): has this contract already had its V1 B zone FYI card SENT on `trade_date`? Read from the store (a
        restart keeps the one-a-day cap): a V1 B row has no alerts row of its own (PLAN-W15 §2), so a personal 'sent'
        row on a V1 B signal of that contract-day is the FYI."""
        return (
            self.db.execute(
                "SELECT 1 FROM alerts a JOIN signals s ON s.id = a.signal_id WHERE s.trade_date = ? AND"
                " s.security_id = ? AND s.scanner = ? AND a.channel = ? AND a.status = ? LIMIT 1",
                (trade_date, int(security_id), SCANNER_V1B, CH_PERSONAL, ST_SENT),
            ).fetchone()
            is not None
        )

    async def _send_v1b_zone_fyi(
        self, signal_id: int, signal: Signal, meta: ContractMeta, ctx: SignalContext, trade_date: str, tier: str | None
    ) -> int | None:
        """0.8.8 T-343 (a) (D378 (a) as corrected by D382 (1), owner: "All 34-type rows"): a V1 B E1 whose V1 gate
        verdict failed ONLY on zone (`v1b_zone_only`: the verdict gates recorded on the V1 B context -- the zone test is
        gates', never re-derived here), at ANY turnover -> ONE owner FYI card per contract per trading day
        (`tpl.v1b_zone_fyi_card`) to the PERSONAL chat, silent, through `_route_personal_only` (the client edition sends
        none; muted / paused / no personal chat as there). Channel telegram_personal; no schema change, no rule, tier,
        trade or R4 effect. The day's cap: the store (`_v1b_zone_fyi_sent`) + this process's claimed keys (claimed before
        the await, released when the card did not go, so a later V1 B row of the contract-day may still send it)."""
        if self.is_client:
            return None
        key = (trade_date, int(signal.security_id))
        if key in self._v1b_fyi_keys or self._v1b_zone_fyi_sent(trade_date, signal.security_id):
            return None
        self._v1b_fyi_keys.add(key)
        floor = float(self.v1b_cfg.owner_min_turnover_rs)
        mid = await self._route_personal_only(
            signal_id, signal, meta, ctx, self.is_muted(signal.security_id), tier=tier, warnings=(), silent=True,
            render=lambda: tpl.v1b_zone_fyi_card(meta, signal, ctx, tier=tier, floor_rs=floor),
        )  # fmt: skip
        if mid is None:
            self._v1b_fyi_keys.discard(key)
        return mid

    # -- S1.7.0 (T-267): the V2 Scanner (STRATEGY-SPEC 4.7; PLAN-T267 §1 "Tier rule" + M4 / M12; D249, D254, D255)

    def v2_enabled(self) -> bool:
        """D255: the install's V2 Scanner switch (`v2_switch`: client default OFF, owner default ON)."""
        return v2_switch(self.db, owner=not self.is_client)

    def _v2_off(self, trade_date: str) -> None:
        """D255: ONE log line per IST day that a V2 trigger met the OFF switch (nothing of it was recorded)."""
        if trade_date not in self._v2_off_days:
            self._v2_off_days.add(trade_date)
            log.info("%s (%s)", V2T_OFF_LINE, trade_date)

    @staticmethod
    def _v2t_stamped(signal: Signal, meta: ContractMeta) -> Signal:
        """The V2T signal with snapshot["v2t"]["option_type"] = the contract's side, so the stored payload and the
        tracker's one-argument `gates.v2t_tier(snapshot)` read the side the router tiered on (the trigger engine does
        not know the side)."""
        snap = dict(signal.snapshot) if isinstance(signal.snapshot, Mapping) else {}
        block = dict(snap["v2t"]) if isinstance(snap.get("v2t"), Mapping) else {}
        side = str(meta.option_type or "").upper()
        if "v2t" in snap and block.get("option_type") == side:
            return signal
        snap["v2t"] = {**block, "option_type": side}
        return replace(signal, snapshot=snap)

    @staticmethod
    def _v2t_strong(signal: Signal) -> bool:
        """D257: a V2 trigger whose OI unwind is >= the STRONG line (the trigger's `strong` flag, set from
        v2_scanner.oi_strong_pct; or, should a caller not set the flag, snapshot.v2t.drop >= V2T_CLIENT_CAP_PCT).
        OWNER: tagged STRONG and tracked (the owner's V2 is personal only since 0.8.5)."""
        if signal.strong:
            return True
        block = signal.snapshot.get("v2t") if isinstance(signal.snapshot, Mapping) else None
        drop = block.get("drop") if isinstance(block, Mapping) else None
        if not isinstance(drop, int | float) or isinstance(drop, bool) or drop != drop:  # noqa: PLR0124 -- NaN
            return False
        return float(drop) >= V2T_CLIENT_CAP_PCT - 1e-9

    async def _route_v2t(self, signal: Signal, meta: ContractMeta, ctx: SignalContext) -> int:
        """One V2 Scanner signal (scanner "V2T"), OWNER edition only since 0.8.5. Returns the signals.id, or 0 when
        nothing was recorded. In order, all synchronous up to the INSERT (no await before it):
          client    0.8.5 (D286(4), D287(6), PLAN-D287 I7 / AM7): the CLIENT edition has no V2 Scanner -- returns 0
                    before any V2 work (nothing recorded, pushed, sent or tracked; the switch is not even read).
          kind      an E2 never comes from V2 (dropped, 0).
          switch    D255: the install's switch OFF -> nothing recorded, pushed, sent or tracked (one log line a day);
                    the open V2 trades keep their exits (send_followup / send_zone_step never read the switch).
          tier      `gates.v2t_verdict` (the stock's daily Supertrend vs the side; the 1h zone book never blocks, M12);
                    a carried contract -> WATCHLIST + `carried` (watch only, never a trade, D157(2)).
          strong    D257: an unwind >= V2T_CLIENT_CAP_PCT (12 %, `_v2t_strong`) is kept, tagged STRONG and tracked.
          dedupe   one V2T E1 per (trade_date, security_id), any tier, read from the store (`has_v2t_e1`): a
                    repeat (a restart's replay) -> dropped, 0.
        Then the INSERT (scanner V2T, gate_reasons = the WATCHLIST reason, ctx off_client = True: 0.8.5 D287(6), the
        V2 never posts to the GROUP), the dashboard push (type `signal`, scanner V2T) and the delivery: an ENTRY -> the
        owner's PERSONAL card only (`_route_personal_only(scanner="V2T")`, with sound and the DESK); a WATCHLIST row ->
        dashboard only (a 'gated' personal alerts row). Every ENTRY that is not carried opens a tracked trade through the
        trade hook (the personal message id) -- muted / paused / failed included, as V1. V2 never touches V1's R4, V1
        PLUS (`plus.derive_plus` is never called) or V1's summary lines."""
        if self.is_client:  # AM7: no V2 work of any kind in the client edition (it never builds a V2 engine either)
            log.debug("client: a %s signal is owner-edition only; dropped", SCANNER_V2T)
            return 0
        if signal.kind != _edition.KIND_E1:
            log.debug("a %s from the V2 Scanner dropped (it fires E1 only)", signal.kind)
            return 0
        trade_date = ist_trade_date(signal.ts_ms)
        if not self.v2_enabled():
            self._v2_off(trade_date)
            return 0
        signal = self._v2t_stamped(signal, meta)
        tier, reason = gates.v2t_verdict(signal.snapshot, meta.option_type)
        carried = bool(ctx.carried)
        if carried:  # watch only
            tier, reason = TIER_WATCHLIST, CARRIED
        strong = self._v2t_strong(signal)
        if has_v2t_e1(self.db, trade_date, signal.security_id):
            log.info("V2T: sec %s already has a V2 Scanner signal on %s; the repeat is dropped", signal.security_id,
                     trade_date)  # fmt: skip
            return 0
        # 0.8.5 (D287(6)): the GROUP (= the client view) never carries a V2 entry: every owner V2 row is off the group
        ctx = replace(ctx, gate_reasons=(reason,) if reason else (), gate_unknown=(), off_client=True)
        warnings = signal_warnings(meta, signal, self.alerts_cfg, self.calendar)
        signal_id, payload, ctx_d = self._insert_signal(signal, meta, ctx, trade_date, SCANNER_V2T, tier, warnings)
        muted = self.is_muted(signal.security_id)
        await self._push(
            {
                "type": "signal",
                "signal_id": signal_id,
                **payload,
                "muted": muted,
                "ctx": ctx_d,
                "hold": ctx.hold,
                "gate_status": ctx.gate_status,
                "gate_reasons": list(ctx.gate_reasons),
                "tier": tier,
                "scanner": SCANNER_V2T,
                "warnings": warnings,
            },
            signal_id,
        )
        message_id: int | None = None
        if tier != TIER_ENTRY:  # dashboard only: the decision row on the chat it would have gone to
            self._record(signal_id, CH_PERSONAL, ST_GATED, None, "watchlist: " + (reason or "-"))
        else:
            message_id = await self._route_personal_only(
                signal_id, signal, meta, ctx, muted, tier=TIER_ENTRY, warnings=warnings, silent=False,
                scanner=SCANNER_V2T,
            )  # fmt: skip
        if tier == TIER_ENTRY and not carried:  # D249 / D254: the owner's V2 entries are tracked
            await self._run_trade_hook(signal_id, signal, meta, ctx, message_id)
        if strong:
            log.debug("V2T: signal %s is a STRONG unwind (D257)", signal_id)
        return signal_id

    async def send_zone_step(
        self, trade: Mapping[str, Any], step: Mapping[str, Any]
    ) -> tuple[str, int | None, str | None, int | None]:
        """T-267 (M4, D249(4), D254): one V2 trail step (the tracker's ZSTEP) -> the owner's PERSONAL chat, SILENT
        (`tpl.zone_step_card`), as a reply to the owner's own card of the entry (the personal-only card, else the
        personal copy of the group card) when there is one. NEVER the group, in either edition. The CLIENT edition
        sends nothing: ('suppressed', None, None, None). No usable personal chat -> 'suppressed'; held by Mute
        (`_exit_held` on the PERSONAL channel, T-737 / D732 (2): never for a trade whose entry card was sent there) ->
        'muted'; paused -> 'paused'. Not gated by the D255 switch (an open trade's steps are never orphaned). Returns
        (status, message_id, error, replied_to) -- send_followup's triple plus T-275(2) the message id this step was
        sent as a reply to (the owner's PERSONAL card, never the trade's GROUP entry_msg_id), None when it was sent
        standalone or not sent; the tracker stores it as trade_zone_steps.reply_to. One trade's messages strictly in
        order (its lock)."""
        if self.is_client:
            return ST_SUPPRESSED, None, None, None
        key = _field(trade, "id")
        if key is None:
            key = ("signal", _field(trade, "entry_signal_id"))
        lock = self._trade_locks.get(key)
        if lock is None:
            lock = self._trade_locks[key] = asyncio.Lock()
        async with lock:
            if not self.has_personal_chat():
                return ST_SUPPRESSED, None, "no personal chat (a V2 trail step never goes to the group)", None
            if self._exit_held(trade, CH_PERSONAL):
                return ST_MUTED, None, None, None
            if self.paused:
                return ST_PAUSED, None, None, None
            try:
                text = tpl.zone_step_card(trade, step, edition=self.edition)
            except Exception as exc:  # a template bug is a 'failed' step, never a crash in the tracker
                log.exception("V2 trail step render failed for trade %s", _field(trade, "id"))
                return ST_FAILED, None, f"render: {type(exc).__name__}: {exc}", None
            sig_id = _as_int(_field(trade, "entry_signal_id"))
            reply_to = sent_message_id(self.db, sig_id, CH_PERSONAL) if sig_id is not None else None
            status, error, mid = await self._deliver(CH_PERSONAL, self.personal_chat_id, text, reply_to=reply_to,
                                                     silent=True)  # fmt: skip
            return (ST_SENT, mid, None, reply_to) if status == ST_SENT else (ST_FAILED, None, error, None)

    def group_v2_summary_rows(self, trade_date: str) -> list[dict[str, Any]]:
        """The 15:35 GROUP summary's V2 Scanner rows (D249(4), STRATEGY-SPEC 4.7): the day's V2T E1 rows whose GROUP
        alert was SENT (an owner-only V2 entry never reached the group, so never its summary), in time order, each with
        its V2 trade as `trade` (`trades_by_signal(scanners=("V2T",))`) when it has one. Every row carries `scanner`
        ("V2T"): tpl.daily_summary keeps them out of the V1 lines and renders its own section."""
        sent_ids = {
            int(r[0])
            for r in self.db.execute(
                "SELECT DISTINCT a.signal_id FROM alerts a JOIN signals s ON s.id = a.signal_id"
                " WHERE s.trade_date = ? AND s.scanner = ? AND a.channel = ? AND a.status = ?",
                (trade_date, SCANNER_V2T, CH_GROUP, ST_SENT),
            ).fetchall()
        }
        if not sent_ids:
            return []
        trades = trade_results.trades_by_signal(self.db, trade_date, kinds=self.visible_kinds(),
                                                scanners=(SCANNER_V2T,))  # fmt: skip
        rows: list[dict[str, Any]] = []
        for r in fetch_signals(self.db, trade_date, scanner=SCANNER_V2T):
            rid = int(r["id"])
            if rid in sent_ids and r.get("kind") == _edition.KIND_E1:
                row = {**r, "scanner": SCANNER_V2T}
                if rid in trades:
                    row["trade"] = trades[rid]
                rows.append(row)
        return rows

    def owner_v2_summary_rows(self, trade_date: str) -> list[dict[str, Any]]:
        """0.8.5 D296 M4: the rows of the OWNER's 15:35 PERSONAL V2 Scanner day block (`tpl.v2_day_block`): the day's
        V2T E1 rows of tier ENTRY (the entries; since D287(6) none reaches the group, so `group_v2_summary_rows` is
        empty), in time order, each with its V2 trade as `trade` when it has one. [] in the CLIENT edition."""
        if self.is_client:
            return []
        trades = trade_results.trades_by_signal(self.db, trade_date, kinds=self.visible_kinds(),
                                                scanners=(SCANNER_V2T,))  # fmt: skip
        rows: list[dict[str, Any]] = []
        for r in fetch_signals(self.db, trade_date, scanner=SCANNER_V2T):
            if r.get("kind") != _edition.KIND_E1 or r.get("tier") != TIER_ENTRY:
                continue
            row = {**r, "scanner": SCANNER_V2T}
            if int(r["id"]) in trades:
                row["trade"] = trades[int(r["id"])]
            rows.append(row)
        return rows

    def _record_closed_cycle(self, signal: Signal) -> None:
        """D98(4): a hidden (gated) CLIENT E1 leaves no signals row, so a restart's silent bar replay would re-arm
        its cycle and a later tick could fire a second E1 for it. This row (no kind, no gate detail: nothing a
        client surface reads) is what Runtime._close_cycle_already_entered finds instead. Never raises."""
        snap = signal.snapshot if isinstance(signal.snapshot, Mapping) else {}
        oi_now = snap.get("oi_now")
        bar_ts = snap.get("bar_ts_ms")  # T-107: the bar it fired in, so a restart orders it as a signals-row E1
        row = (ist_trade_date(signal.ts_ms), int(signal.security_id), int(signal.ts_ms),
               float(oi_now) if isinstance(oi_now, int | float) else None,
               int(bar_ts) if isinstance(bar_ts, int | float) and not isinstance(bar_ts, bool) else None)  # fmt: skip
        try:
            try:
                self._insert_closed_cycle(row)
            except sqlite3.OperationalError:
                self._cc_has_bar_ts = None  # T-108: the store's shape may have changed under us: re-probe once
                self._insert_closed_cycle(row)
            self.db.commit()
        except sqlite3.Error as exc:  # a store without the table (pre-D98 v4) must never break routing
            # only a HIDDEN client entry lands here: never name the contract (it would reveal the entry, D86/D99)
            log.error("cycle bookkeeping not recorded (%s); a restart may re-open one cycle", type(exc).__name__)

    def _insert_closed_cycle(self, row: tuple[Any, ...]) -> None:
        """T-108: bar_ts_ms is a v6 column; a store without it (unmigrated) still gets the 4-column row, which is
        what a restart's replay needs to keep the cycle closed. The probe raises nothing on a missing table."""
        if self._cc_has_bar_ts is not True:
            cols = {r[1] for r in self.db.execute("PRAGMA table_info(closed_cycles)").fetchall()}
            self._cc_has_bar_ts = "bar_ts_ms" in cols
        if self._cc_has_bar_ts:
            self.db.execute(
                "INSERT INTO closed_cycles (trade_date, security_id, ts_ms, oi_now, bar_ts_ms) VALUES (?, ?, ?, ?, ?)",
                row,
            )
        else:
            self.db.execute(
                "INSERT INTO closed_cycles (trade_date, security_id, ts_ms, oi_now) VALUES (?, ?, ?, ?)", row[:4]
            )

    async def _run_trade_hook(
        self, signal_id: int, signal: Signal, meta: ContractMeta, ctx: SignalContext, message_id: int | None
    ) -> None:
        """TRADE-TRACKER-SPEC §4: the tracker's hook, after the alert path is complete. Never raises."""
        hook = self.trade_hook
        if hook is None:
            return
        try:
            result = hook(signal_id, signal, meta, ctx, message_id)
            if inspect.isawaitable(result):
                await result
        except Exception:
            log.exception("trade hook failed for signal %s", signal_id)

    async def _route_group_signal(
        self,
        signal_id: int,
        signal: Signal,
        meta: ContractMeta,
        ctx: SignalContext,
        trade_date: str,
        muted: bool,
        *,
        tier: str | None = None,
        warnings: Sequence[str] = (),
        also_plus: bool = False,
        held_hm: HeldLine | None = None,
        group_entry: bool = False,
    ) -> int | None:
        """0.8.8 (owner "Group counts only its own lists"): `group_entry` = this WATCHLIST + dup_stock row is the GROUP's
        first entry of the stock (`_group_takes`): the GROUP gets the ENTRY card a client app sends (`_route_group_entry`);
        no personal copy (the owner's personal chat is the demoted row's: nothing), the stored tier is unchanged.
        T-310: `held_hm` = a held signal released after the 09:20 list post (its own time): the group card carries
        `tpl.add_held(owner=False)` (no owner word: the clients read it), the owner's personal copy `owner=True`.
        `also_plus` (0.8.1, D161(1)): this V1 ENTRY is also the stock-day's V1 PLUS trade -- the group card stays
        `ENTRY · V1` with no V1 PLUS word; only the owner's PERSONAL copy carries the `also V1 PLUS` line.
        The Telegram decision for one signal. Returns the ENTRY message_id the trade hook stores (the GROUP card's
        for an E1, the PERSONAL card's for an owner E2) when sent, else None.
        An E1 is decided by its tier (D114(4)): ENTRY / STRONG_WATCHLIST are sent whatever their failed gates,
        WATCHLIST is dashboard only. D149(5): an E2 (owner, untiered) goes to the owner's PERSONAL chat only
        (`_route_personal_e2`, the D63 gate check kept); nothing of it reaches the group, and its trade's follow-ups
        reply to it in the PERSONAL chat (`_send_followup`). In the OWNER edition a sent-to-group ENTRY / STRONG
        WATCHLIST card is followed by its silent PERSONAL copy with the DESK block (`_personal_copy`).
        0.8.5 (D287(1)(2), PLAN-D287 I2 / I4): a TIERED owner E2 takes this E1 route (it is the group's entry; the card
        text does not depend on the kind); only an untiered (legacy) E2 keeps `_route_personal_e2`. The owner E1 never
        reaches here (`_route_owner_e1`, personal only)."""
        if tier is None and signal.kind != _edition.KIND_E1:
            return await self._route_personal_e2(signal_id, signal, meta, ctx, muted, warnings, held_hm=held_hm)
        if muted:
            self._record(signal_id, CH_GROUP, ST_MUTED, None, None)
            return None
        if group_entry and tier == TIER_WATCHLIST and not self.is_client:
            return await self._route_group_entry(signal_id, signal, meta, ctx, warnings=warnings, held_hm=held_hm)
        if tier not in GROUP_TIERS:
            why = ",".join((*ctx.gate_reasons, *ctx.gate_unknown)) or "-"
            self._record(signal_id, CH_GROUP, ST_GATED, None, "watchlist: " + why)
            return None
        if self.paused:
            self._record(signal_id, CH_GROUP, ST_PAUSED, None, None)
            return None
        # Y2: an owner E1 card names its own E2 ("After E2 hh:mm @ Rs"); found exactly as the D65 reply thread
        # finds it (own_e2_id). The client edition has no E2 (CLIENT §2).
        e2 = None
        if signal.kind == "E1" and not self.is_client:
            e2 = self._own_e2_signal(signal_id, signal, trade_date)
        card_ctx = card_context(meta, signal, ctx, self.calendar)  # T-162 T2: the D125 F7 hold the trade is held on
        try:  # D159(2): the template heads an ENTRY `ENTRY · V1` (its default scanner), both editions
            text = format_signal(signal, meta, card_ctx, e2=e2, tier=tier, warnings=warnings,
                                 **self._sw_card_extras(meta, card_ctx))  # fmt: skip
            if held_hm is not None:  # T-310: the GROUP words (clients read it): no owner word
                text = held_line(text, held_hm, owner=False)
        except Exception as exc:  # a template bug is a 'failed' row, never a lost record
            log.exception("signal card failed for signal %s", signal_id)
            self._record(signal_id, CH_GROUP, ST_FAILED, None, f"render: {type(exc).__name__}: {exc}")
            return None
        # T-162 (D129 T1): a tiered E1 card gets the v3 keyboard (ENTRY coloured Copy order + Open contract, STRONG
        # a plain Open contract) and a STRONG WATCHLIST goes out silent; an E2 (owner) keeps the D79 keyboard.
        buttons = tier_buttons(meta, signal, card_ctx, tier) if tier is not None else signal_buttons(meta, signal,
                                                                                                    card_ctx)
        if not self.is_client and not group_list_ok(meta.list_name):
            # 0.8.8 (D429 extension "Stop EXPIRY posts to the group"): an EXPIRY-list card never reaches the GROUP. The
            # group decision is recorded 'gated' (so its trade's follow-ups go PERSONAL, `_send_followup`) and the
            # owner's PERSONAL copy goes out exactly as it did beside a sent group card (silent, DESK, no failed line).
            # The trade's entry message is that personal copy. The tier, the trade and the dashboard row are unchanged.
            self._record(signal_id, CH_GROUP, ST_GATED, None, f"list: {meta.list_name} is not in the group (D429)")
            await self._personal_copy(signal_id, signal, meta, card_ctx, trade_date, e2=e2, tier=tier,
                                      warnings=warnings, buttons=buttons, also_plus=also_plus, held_hm=held_hm)  # fmt: skip
            return sent_message_id(self.db, signal_id, CH_PERSONAL)
        # D149(5): an E2 never reaches the group, so a group card replies to nothing (the D65 thread lives on in the
        # owner's PERSONAL chat: the personal copy replies to its E2's personal card, `_e2_personal_target`).
        status, error, message_id = await self._deliver(
            CH_GROUP, self.group_chat_id, text, buttons=buttons, silent=tier in SILENT_TIERS
        )
        sent = status == ST_SENT
        self._record(signal_id, CH_GROUP, status, self._clock_ms() if sent else None, error, message_id)
        if not self.is_client:  # D149(5): the owner's silent PERSONAL copy WITH the DESK block
            await self._personal_copy(signal_id, signal, meta, card_ctx, trade_date, e2=e2, tier=tier,
                                      warnings=warnings, buttons=buttons, also_plus=also_plus,
                                      group_error=None if sent else (error or "no detail"), held_hm=held_hm)  # fmt: skip
        if not sent:  # T-213 F2: after the (marked, sounding) copy, the day's one SYSTEM line for failed entry cards
            await self._auto_post_failed("entry card", error)
        return message_id if sent else None

    async def _route_group_entry(
        self,
        signal_id: int,
        signal: Signal,
        meta: ContractMeta,
        ctx: SignalContext,
        *,
        warnings: Sequence[str],
        held_hm: HeldLine | None,
    ) -> int | None:
        """0.8.8 (owner, 13 Oct "Group counts only its own lists"), OWNER edition: the GROUP's ENTRY card for an entry the
        owner's R4 demoted only because of an entry on a list the group does not carry (an EXPIRY list, D432). The text
        is the ENTRY card a client app sends for it (tier ENTRY, rendered from the context WITHOUT the owner's
        `dup_stock`; with sound; the group held line when released after 09:20). Paused -> 'paused'. NO personal copy:
        the owner's personal chat and dashboard keep the demoted row exactly as before (no card). Its paper trade's
        follow-ups also reach the group (`send_followup`, `group_took_demoted`). Returns the group message_id when sent."""
        if self.paused:
            self._record(signal_id, CH_GROUP, ST_PAUSED, None, None)
            return None
        gctx = replace(ctx, gate_reasons=tuple(x for x in ctx.gate_reasons if x != DUP_STOCK))
        card_ctx = card_context(meta, signal, gctx, self.calendar)
        try:
            text = format_signal(signal, meta, card_ctx, tier=TIER_ENTRY, warnings=warnings,
                                 **self._sw_card_extras(meta, card_ctx))  # fmt: skip
            if held_hm is not None:  # T-310: the GROUP words (clients read it)
                text = held_line(text, held_hm, owner=False)
        except Exception as exc:  # a template bug is a 'failed' row, never a lost record
            log.exception("group entry card failed for signal %s", signal_id)
            self._record(signal_id, CH_GROUP, ST_FAILED, None, f"render: {type(exc).__name__}: {exc}")
            return None
        status, error, message_id = await self._deliver(
            CH_GROUP, self.group_chat_id, text, buttons=tier_buttons(meta, signal, card_ctx, TIER_ENTRY), silent=False
        )
        sent = status == ST_SENT
        self._record(signal_id, CH_GROUP, status, self._clock_ms() if sent else None, error, message_id)
        if not sent:
            await self._auto_post_failed("entry card", error)
        return message_id if sent else None

    async def _personal_copy(
        self,
        signal_id: int,
        signal: Signal,
        meta: ContractMeta,
        card_ctx: SignalContext,
        trade_date: str,
        *,
        e2: Signal | None,
        tier: str | None,
        warnings: Sequence[str],
        buttons: Buttons | None,
        also_plus: bool = False,
        group_error: str | None = None,
        scanner: str | None = None,
        held_hm: HeldLine | None = None,
    ) -> None:
        """S1.7.0 (T-267): `scanner` V2T = the copy of a V2 Scanner group card (the V2 card + its owner DESK).
        0.8.1 (D161(1)): `also_plus` adds the `also V1 PLUS` line (the group card never has it).
        D149(5), OWNER edition: the group card again, to the PERSONAL chat, SILENT, WITH the owner DESK block
        (desk=True: quadrant, OI, "After E2 hh:mm @ Rs" when the cycle has an E2, list, gates), the same buttons, as a
        reply to that E2's PERSONAL message when there is one. Called only once the group decision was "send" (a
        muted, paused or WATCHLIST card has no copy); a group DELIVERY failure does not stop it. No usable personal
        chat -> nothing sent, nothing recorded. One `alerts` row, channel telegram_personal.
        0.8.2 T-213 F2 (D189(4)): `group_error` = the group send FAILED with this error: the copy carries
        `tpl.add_group_failed` right under the headline and is sent WITH sound (the owner must not read a normal card
        and assume the clients got it)."""
        if not self.has_personal_chat():
            return
        try:
            text = tpl.signal_card(meta, signal, ctx=card_ctx, e2=e2, tier=tier, warnings=list(warnings), desk=True,
                                   **_card_extras(scanner=scanner), **self._sw_card_extras(meta, card_ctx))  # fmt: skip
            if also_plus and tier == TIER_ENTRY:  # D161(1): the PERSONAL copy only, right under the headline
                text = tpl.add_also_plus(text)
            if held_hm is not None:  # T-310: the owner's words on his own copy
                text = held_line(text, held_hm, owner=True)
            if group_error is not None:  # applied last: the first line under the headline
                text = tpl.add_group_failed(text, group_error)
        except Exception as exc:  # a template bug is a 'failed' row, never a lost group card
            log.exception("personal copy of signal %s failed to render", signal_id)
            self._record(signal_id, CH_PERSONAL, ST_FAILED, None, f"render: {type(exc).__name__}: {exc}")
            return
        # a V2 card has no E2 (a V1 E2 of the same contract is never its thread)
        reply_to = None if scanner == SCANNER_V2T else await self._e2_personal_target(signal_id, signal, trade_date)
        status, error, mid = await self._deliver(
            CH_PERSONAL, self.personal_chat_id, text, reply_to=reply_to, buttons=buttons, silent=group_error is None
        )
        sent = status == ST_SENT
        self._record(signal_id, CH_PERSONAL, status, self._clock_ms() if sent else None, error, mid)

    async def _e2_personal_target(self, signal_id: int, signal: Signal, trade_date: str) -> int | None:
        """The PERSONAL message_id of this E1's own E2 card (D65's thread, moved to the personal chat by D149(5))."""
        e2_id = own_e2_id(self.db, signal.security_id, trade_date, signal_id)
        if e2_id is None:
            return None
        pending = self._e2_inflight.get(e2_id)
        if pending is not None:
            await asyncio.shield(pending)
        return sent_message_id(self.db, e2_id, CH_PERSONAL)

    async def _route_personal_e2(
        self,
        signal_id: int,
        signal: Signal,
        meta: ContractMeta,
        ctx: SignalContext,
        muted: bool,
        warnings: Sequence[str],
        *,
        silent: bool = False,
        held_hm: HeldLine | None = None,
    ) -> int | None:
        """T-310: `held_hm` = a held E2 released after the 09:20 list post (the owner's held line).
        0.8.1 (D160(3)): `silent` = an E2 on a CARRY row (sent without sound, the card says carried).
        D149(5): the OWNER E2 (arming) card goes to the PERSONAL chat only, never the group. The D48 decision
        order is kept -- muted -> 'muted', D63 gate reasons -> 'gated', paused -> 'paused' -- with the rows on channel
        telegram_personal; no usable personal chat -> 'suppressed' (the card goes nowhere, never to the group). Sent
        with sound, the D79 keyboard (Copy order + links). Returns the personal message_id when sent (the E2 trade's
        entry message: its follow-ups reply to it in the personal chat), else None."""
        ch = CH_PERSONAL
        if muted:
            self._record(signal_id, ch, ST_MUTED, None, None)
            return None
        if ctx.gate_reasons:
            self._record(signal_id, ch, ST_GATED, None, "gated: " + ",".join(ctx.gate_reasons))
            return None
        if self.paused:
            self._record(signal_id, ch, ST_PAUSED, None, None)
            return None
        if not self.has_personal_chat():
            self._record(signal_id, ch, ST_SUPPRESSED, None, "no personal chat: an E2 card never goes to the group")
            return None
        card_ctx = card_context(meta, signal, ctx, self.calendar)
        try:
            text = format_signal(signal, meta, card_ctx, warnings=warnings)
            if held_hm is not None:
                text = held_line(text, held_hm, owner=True)
        except Exception as exc:
            log.exception("signal card failed for signal %s", signal_id)
            self._record(signal_id, ch, ST_FAILED, None, f"render: {type(exc).__name__}: {exc}")
            return None
        buttons = signal_buttons(meta, signal, card_ctx)
        status, error, mid = await self._deliver(ch, self.personal_chat_id, text, buttons=buttons, silent=silent)
        sent = status == ST_SENT
        self._record(signal_id, ch, status, self._clock_ms() if sent else None, error, mid)
        return mid if sent else None

    def _own_e2_signal(self, signal_id: int, signal: Signal, trade_date: str) -> Signal | None:
        """Y2: the E2 Signal of this E1's arming cycle (D65 own_e2_id), rebuilt from its row; None when absent."""
        e2_id = own_e2_id(self.db, signal.security_id, trade_date, signal_id)
        return None if e2_id is None else signal_from_row(self.db, e2_id)

    async def send_system(
        self,
        text: str,
        kind: SystemKind = "SYSTEM",
        *,
        level: str | None = None,
        validity_ist: str | None = None,
        hours_left: float | None = None,
        group_fallback: bool = False,
        card: str | None = None,
    ) -> None:
        """Token-expiry and system messages: dashboard + PERSONAL chat only, never the group.
        kind TOKEN = the Dhan token itself (with `level` etc. the structured tpl.token_card); SYSTEM = any
        other operator message (feed outage, job failure).
        T-181 F7 (owner ruling 2026-09-29): `group_fallback=True` (the token watcher's EXPIRED and critical warn2
        warnings) sends to the GROUP when there is no usable personal chat -- the D119 rule the readiness card follows
        (`ops_target`); with a personal chat it goes there only, never both. Both editions.
        0.8.3 T-212: `card` = a pre-rendered style B HTML card (a template's output, e.g. the scanner review cards of
        gammad/scan/review.py) sent to Telegram INSTEAD of `format_system(kind, text)`; the dashboard push keeps the
        plain `text`."""
        await self._push({"type": kind.lower(), "text": text, "ts_ms": self._clock_ms()}, None)
        if group_fallback and not self.has_personal_chat():
            body = card if card is not None else format_system(kind, text, level=level, validity_ist=validity_ist,
                                                               hours_left=hours_left)  # fmt: skip
            log.info("%s message: no personal chat configured; sent to the group (urgent)", kind)
            await self._telegram(None, CH_GROUP, self.group_chat_id, body)
            return
        if not self._personal_configured():  # client without a personal chat: dashboard only (CLIENT §3)
            log.info("%s message: no personal chat configured; dashboard only", kind)
            return
        if self._personal_is_group():
            log.error("%s message not sent: %s", kind, PERSONAL_IS_GROUP)
            self._record(None, CH_PERSONAL, ST_FAILED, None, PERSONAL_IS_GROUP)
            return
        body = card if card is not None else format_system(kind, text, level=level, validity_ist=validity_ist,
                                                           hours_left=hours_left)  # fmt: skip
        await self._telegram(None, CH_PERSONAL, self.personal_chat_id, body)

    # -- ops cards (W16 B4: T-132 readiness, T-134(2) install card + start line)

    def has_personal_chat(self) -> bool:
        """True when an ops card can go to the PERSONAL chat: a personal id is set, the edition / T-062 guard does not
        mark it absent (`_personal_configured`), and it is not the group's id (PERSONAL_IS_GROUP)."""
        return bool(self.personal_chat_id) and self._personal_configured() and not self._personal_is_group()

    def ops_target(self, dest: str) -> str | None:
        """The DEST_* an ops card with `dest` goes to, or None (nothing to send to). personal_or_group = PERSONAL when
        `has_personal_chat()`, else the GROUP (D119: a client without a personal chat falls back to the group);
        personal = PERSONAL or None (never the group); group = the GROUP."""
        if dest not in OPS_DESTS:
            raise ValueError(f"dest must be one of {OPS_DESTS}, got {dest!r}")
        if dest == DEST_GROUP:
            return DEST_GROUP
        if self.has_personal_chat():
            return DEST_PERSONAL
        return DEST_GROUP if dest == OPS_PERSONAL_OR_GROUP else None

    async def send_ops(
        self,
        kind: str,
        text: str,
        *,
        dest: OpsDest,
        silent: bool = False,
        ref: Mapping[str, Any] | None = None,
    ) -> tuple[bool, int | None]:
        """One ops card (OPS_KINDS). Returns (sent, telegram message_id). Ignores `paused` and mutes, like the token
        cards (ops messages; PLAN-W16 section 13.3 decision). Records an `alerts` row (signal_id NULL, like
        send_system) and a `manual_sends` row whose kind is `kind` and whose ref names the requested dest. Never
        raises for a send failure (the result is (False, None) and a 'failed' row); ValueError for an unknown
        kind / dest, before anything is sent or recorded.
        0.8.2 C2: `ref` is merged into the logged ref after {"requested": dest} (then the C1 keys of the send). An
        OWNER_ONLY_KINDS kind (KIND_DELIVERY_CHECK) in the CLIENT edition -> (False, None), nothing sent or recorded."""
        if kind not in OPS_KINDS:
            raise ValueError(f"kind must be one of {OPS_KINDS}, got {kind!r}")
        target = self.ops_target(dest)
        if kind in OWNER_ONLY_KINDS and self.is_client:
            log.debug("client: the owner-only ops card %s is never sent", kind)
            return False, None
        logged: dict[str, Any] = {"requested": dest, **dict(ref or {})}
        if target is None:
            log.info("%s: no personal chat configured; not sent", kind)
            self._log_send(kind, logged, DEST_PERSONAL, ST_SUPPRESSED, "no personal chat configured")
            return False, None
        channel, chat_id = self._dest_chat(target)
        if kind == KIND_DELIVERY_CHECK:  # 0.8.3 T-212: the verdict line + its stored misses as the style B card
            text = delivery_text(text, (ref or {}).get("misses"))
        d = await self._deliver_full(channel, chat_id, text, silent=silent)
        sent = d.status == ST_SENT
        self._record(None, channel, d.status, self._clock_ms() if sent else None, d.error, d.message_id)
        self._log_send(kind, logged, target, d.status, d.error, self._delivery_ref(channel, d))
        return sent, d.message_id if sent else None

    async def send_followup(self, trade: Mapping[str, Any], event: Mapping[str, Any]) -> FollowupResult:
        """TRADE-TRACKER-SPEC §7: one tracker event as a Telegram message. Returns (status, message_id, error),
        status in FOLLOWUP_STATUSES; the tracker writes it to trade_events (this writes no alerts row: those
        belong to signals, and a follow-up must never become an E1's D65 reply target).

        Text = templates.trade_followup(event["kind"], trade, edition=<edition>, event=event).
        EOD_MISSED -> PERSONAL only ('suppressed' when no personal chat is usable: dashboard only).
        Any other kind -> the GROUP as a reply to trade["entry_msg_id"] (allow_sending_without_reply):
            held by Mute (`_exit_held` on the follow-up's channel) -> 'muted'; paused -> 'paused' (checked FIRST).
            T-737 (D732 (2), amends D42 / D48 for exits): Mute blocks new entry cards only -- the follow-up of a trade
            whose entry card was SENT to that chat always goes out, even when the contract is muted later.
            0.8.5 (PLAN-D287 I4, D288 Q2 / Q5; was D149(5) "an E2 trade -> PERSONAL"), OWNER: the chat the entry card
            went to (`_v1_entry_personal`): the group's ENTRY E2 trade -> the GROUP (rendered as the client's text of its
            E1 entry: no kind word); an owner E1 trade, a paper E2 trade and every V2 trade -> the PERSONAL chat (no
            usable personal chat -> 'suppressed', never the group). CLIENT: every V1 trade -> the GROUP; a V2 trade ->
            'suppressed' (AM7).
            0.8.2 T-214 F3 (D186(2), D189(3), both editions): no entry_msg_id -> SENT all the same, as a standalone
            message (no reply) to the chat the entry was routed to (as above); the
            template's headline names the contract (`SYM STRIKE OT`, then the expiry), so a standalone follow-up never
            relies on a quoted card. 'no_entry_msg' is no longer produced (a legacy trade_events status).
        A CLIENT-edition trade of a hidden kind (E2), or an empty render -> 'suppressed'. Render / send
        failure -> 'failed' + error. Paced by the D26 windows; one trade's events are sent strictly in order
        (a per-trade lock keyed by trade["id"], else entry_signal_id)."""
        key = _field(trade, "id")
        if key is None:
            key = ("signal", _field(trade, "entry_signal_id"))
        lock = self._trade_locks.get(key)
        if lock is None:
            lock = self._trade_locks[key] = asyncio.Lock()
        async with lock:
            result = await self._send_followup(trade, event)
            if self._group_copy_due(trade, event):  # 0.8.8: the group's entry the owner's R4 demoted (paper trade)
                await self._group_followup_copy(trade, event)
            return result

    def _group_copy_due(self, trade: Mapping[str, Any], event: Mapping[str, Any]) -> bool:
        """0.8.8 (owner "Group counts only its own lists"), OWNER: does this V1 trade's follow-up ALSO go to the GROUP?
        Yes for the paper trade of an owner-R4-demoted entry the group took as its ENTRY card (`group_took_demoted`): the
        group sees the exits a client app sends for the entry it was shown. Its follow-up to the owner (PERSONAL, the
        paper trade's chat) is unchanged and is the result the tracker records. Never for EOD_MISSED (personal only), a
        V2 trade, an EXPIRY-list trade or the client edition."""
        if self.is_client or str(_field(event, "kind") or "") in FOLLOWUP_PERSONAL_KINDS:
            return False
        if str(_field(trade, "scanner") or "") == SCANNER_V2T or not group_list_ok(_field(trade, "list_name")):
            return False
        if str(_field(trade, "kind") or _edition.KIND_E1) != _edition.client_entry_kind(client=False):
            return False
        return group_took_demoted(self.db, _as_int(_field(trade, "entry_signal_id")))

    async def _group_followup_copy(self, trade: Mapping[str, Any], event: Mapping[str, Any]) -> None:
        """The GROUP copy of `_group_copy_due`'s follow-up: the CLIENT's text of its E1 entry (PLAN-D287 I9, as every
        group follow-up of the group's E2 entry), a reply to the group's ENTRY card (standalone when that card has no
        message: T-214 F3). Held by Mute (`_exit_held` on the GROUP channel, T-737 / D732 (2)) / paused -> nothing. A
        failure is logged with the day's one GROUP SEND FAILED line; it never changes the recorded (personal) result.
        Never raises."""
        kind = str(_field(event, "kind") or "")
        if self._exit_held(trade, CH_GROUP) or self.paused:
            return
        try:
            shown = {**dict(trade), "kind": _edition.KIND_E1} if isinstance(trade, Mapping) else trade
            parts = _parts(tpl.trade_followup(kind, shown, edition=_edition.CLIENT, event=event))
        except Exception:
            log.exception("group copy of follow-up %s failed to render for trade %s", kind, _field(trade, "id"))
            return
        sig_id = _as_int(_field(trade, "entry_signal_id"))
        reply_to = sent_message_id(self.db, sig_id, CH_GROUP) if sig_id is not None else None
        for text in parts:
            status, error, _mid = await self._deliver(CH_GROUP, self.group_chat_id, text, reply_to=reply_to)
            if status != ST_SENT:
                log.warning("group copy of follow-up %s for trade %s: %s", kind, _field(trade, "id"), error)
                await self._auto_post_failed("follow-up", error)
                return

    def _exit_held(self, trade: Mapping[str, Any], channel: str) -> bool:
        """T-737 (D732 (2), amends D42 / D48 for exits; = s089_dispatch._held): does Mute hold this open trade's card on
        `channel`? Never when its entry signal has a SENT alerts row there (an already-sent trade: a later mute never
        silences its exits); always when its first row there is 'muted' (the chat never saw the entry -- also after an
        unmute, nothing is replayed); otherwise (paused / failed / no row) the D42 rule: while the contract is muted.
        Follow-ups write no alerts rows, so the rows read here are the entry's. A D48 hand send of the entry signal to
        that chat writes no alerts row (send_manual logs manual_sends only): a SENT manual_sends row of kind 'signal'
        whose ref id is the entry signal counts as already-sent too (review F1)."""
        sid = _as_int(_field(trade, "security_id"))
        if sid is None:
            return False
        sig_id = _as_int(_field(trade, "entry_signal_id"))
        dest = DEST_GROUP if channel == CH_GROUP else DEST_PERSONAL
        try:
            statuses = [] if sig_id is None else [str(r[0]) for r in self.db.execute(
                "SELECT status FROM alerts WHERE signal_id = ? AND channel = ? ORDER BY id", (sig_id, channel))]
            hand = sig_id is not None and ST_SENT not in statuses and self.db.execute(
                "SELECT 1 FROM manual_sends WHERE kind = 'signal' AND dest = ? AND status = ? AND json_valid(ref)"
                " AND CAST(json_extract(ref, '$.id') AS INTEGER) = ? LIMIT 1", (dest, ST_SENT, sig_id)).fetchone()
        except sqlite3.Error:  # an unreadable store keeps the D42 rule
            statuses, hand = [], None
        if ST_SENT in statuses or hand:
            return False
        return statuses[:1] == [ST_MUTED] or self.is_muted(sid)

    def _v1_entry_personal(self, trade: Mapping[str, Any], trade_kind: str) -> bool:
        """0.8.5 (T-214 "the chat the entry card went to", PLAN-D287 I4, D288 Q2 / Q5), OWNER: does this V1 trade's
        follow-up go to the PERSONAL chat? No (the GROUP) only when its entry signal is an ENTRY-tier row whose GROUP
        decision was a card (an alerts row on the group channel that is not 'gated': sent / failed / muted / paused, the
        rows the client's own entry would have): the group's ENTRY E2 (and a legacy group E1). Yes for an owner E1
        (personal only), a paper E2 (NEAR ENTRY / WATCH ONLY / demoted / off the client view) and an untiered E2. Read
        from the store, so a restart routes the same way; an unreadable store falls back to the kind (an E2 PERSONAL)."""
        sig_id = _as_int(_field(trade, "entry_signal_id"))
        try:
            if sig_id is None:
                raise LookupError("no entry signal")
            row = self.db.execute("SELECT tier FROM signals WHERE id = ?", (sig_id,)).fetchone()
            if row is None:
                raise LookupError("entry signal not found")
            group_card = self.db.execute(
                "SELECT 1 FROM alerts WHERE signal_id = ? AND channel = ? AND status != ? LIMIT 1",
                (sig_id, CH_GROUP, ST_GATED),
            ).fetchone()
        except (LookupError, sqlite3.Error):
            return trade_kind == _edition.KIND_E2
        return not (row[0] == TIER_ENTRY and group_card is not None)

    async def _send_followup(self, trade: Mapping[str, Any], event: Mapping[str, Any]) -> FollowupResult:
        kind = str(_field(event, "kind") or "")
        trade_kind = str(_field(trade, "kind") or _edition.KIND_E1)
        if self.is_client and trade_kind not in self.visible_kinds():
            return ST_SUPPRESSED, None, f"{trade_kind} trade in the client edition"
        v2 = str(_field(trade, "scanner") or "") == SCANNER_V2T
        if self.is_client and v2:  # 0.8.5 (AM7): the client never sends V2 (a 0.8.4 store's open trade included)
            return ST_SUPPRESSED, None, "an owner-edition-only trade in the client edition"
        # T-267 (M4): a V2 trail step is never a follow-up (router.send_zone_step, owner PERSONAL only)
        if kind == ZSTEP_KIND:
            return ST_SUPPRESSED, None, "a V2 trail step is owner PERSONAL only (send_zone_step), never a follow-up"
        # T-214 (spec 4.4): a follow-up goes to the chat the entry card went to. 0.8.5 (PLAN-D287 I4, D287(6)): OWNER --
        # the V2 Scanner never posts to the group (every V2 trade's exits go PERSONAL); a V1 trade goes to the GROUP only
        # when its entry was the group's ENTRY card (`_v1_entry_personal`: an owner E1, a paper E2 -- NEAR ENTRY / WATCH
        # ONLY / R4-demoted / off the client view -- and a legacy untiered E2 go PERSONAL)
        # 0.8.8 (D429 extension): an EXPIRY-list trade never posts to the GROUP -- also one whose entry card went there
        # before the update: its exits go PERSONAL, as a reply to the entry's personal copy (below), never orphaned
        list_personal = not self.is_client and not group_list_ok(_field(trade, "list_name"))
        owner_personal = not self.is_client and (v2 or list_personal or self._v1_entry_personal(trade, trade_kind))
        personal = kind in FOLLOWUP_PERSONAL_KINDS or owner_personal
        reply_to: int | None = None
        if kind in FOLLOWUP_PERSONAL_KINDS:
            if not self._personal_configured() or not self.personal_chat_id or self._personal_is_group():
                return ST_SUPPRESSED, None, "no personal chat (dashboard only)"
        else:
            if owner_personal and not self.has_personal_chat():
                return ST_SUPPRESSED, None, "no personal chat (an owner-only trade's follow-ups never go to the group)"
            if self._exit_held(trade, CH_PERSONAL if owner_personal else CH_GROUP):
                return ST_MUTED, None, None
            if self.paused:
                return ST_PAUSED, None, None
            # T-214 F3: None -> a standalone message to the same chat (the entry card's), never dropped
            reply_to = _as_int(_field(trade, "entry_msg_id"))
            if v2 or list_personal:  # D296 M2: an OWNER V2 exit replies to the PERSONAL entry copy (as send_zone_step),
                # else standalone: a 0.8.4 V2 trade entered by a GROUP card holds that group message id as entry_msg_id,
                # and a personal reply to it would point at a message the personal chat does not have. D429 extension:
                # the same for an EXPIRY-list trade whose entry card went to the group before the update
                sig_id = _as_int(_field(trade, "entry_signal_id"))
                reply_to = sent_message_id(self.db, sig_id, CH_PERSONAL) if sig_id is not None else None
        render = getattr(tpl, "trade_followup", None)
        if render is None:
            log.error("follow-up %s: templates.trade_followup missing", kind)
            return ST_FAILED, None, "render: templates.trade_followup missing"
        edition, shown = self.edition, trade
        if not personal and not self.is_client and trade_kind == _edition.KIND_E2 and isinstance(trade, Mapping):
            # 0.8.5 (PLAN-D287 I9, the HARD line): the owner's GROUP follow-up of the group's E2 entry is the CLIENT's
            # text of its E1 entry (no kind word: clients read the group)
            edition, shown = _edition.CLIENT, {**dict(trade), "kind": _edition.KIND_E1}
        try:
            parts = _parts(render(kind, shown, edition=edition, event=event))
        except Exception as exc:  # a template bug is a 'failed' event, never a crash in the tracker
            log.exception("follow-up %s render failed for trade %s", kind, _field(trade, "id"))
            return ST_FAILED, None, f"render: {type(exc).__name__}: {exc}"
        if not parts:
            return ST_SUPPRESSED, None, "empty follow-up"
        channel = CH_PERSONAL if personal else CH_GROUP
        first_id: int | None = None
        for i, text in enumerate(parts):
            chat_id = self.personal_chat_id if personal else self.group_chat_id  # re-read: a migration may move it
            status, error, mid = await self._deliver(channel, chat_id, text, reply_to=reply_to)
            if status != ST_SENT:
                where = f"part {i + 1}/{len(parts)}: " if len(parts) > 1 else ""
                return ST_FAILED, first_id, f"{where}{error}"
            if first_id is None:
                first_id = mid
        return ST_SENT, first_id, None

    def today(self) -> str:
        return ist_trade_date(self._clock_ms())

    def render(
        self,
        kind: str,
        ref: Mapping[str, Any],
        *,
        live: LiveRow | None = None,
        today: str | None = None,
        dest: str = DEST_GROUP,
    ) -> list[str]:
        """`today` = the date a ref without `date` means (the runtime passes its own clock's date); `dest` = the chat
        the preview is for (default the GROUP, T-178: its text carries no owner DESK). 0.8.3 G3: a PERSONAL signal card
        carries `sent by you <now> · signal <its time>` (the preview names the clock it was rendered at)."""
        return render_manual(self.db, kind, ref, today=today or self.today(), live=live, dest=dest, edition=self.edition,
                             sent_hm=self._now_hm())  # fmt: skip

    def _now_hm(self) -> str:
        """HH:MM IST of the router's clock (the G3 `sent by you` line)."""
        return datetime.fromtimestamp(self._clock_ms() / 1000, IST).strftime("%H:%M")

    async def send_manual(
        self,
        kind: str,
        ref: Mapping[str, Any],
        dest: str = DEST_GROUP,
        *,
        who: str = WHO_LOCAL,
        live: LiveRow | None = None,
        today: str | None = None,
    ) -> dict[str, Any]:
        """Operator send (D42/D43). Ignores pause, mute and the D63 gates (D48). Raises ValueError (bad
        kind/ref/dest) or LookupError (nothing to send) before anything is sent.
        Every exit writes one audit log line; GROUP refusals also store a refused row with their code.
        `who` is `lan` / `local` (R15); the line is written before the manual_sends row (R16).
        Returns {ok, message_preview, parts, error}. 0.8.2: the row's ref carries the C1 keys after the send; the GROUP
        refusals and the client text of `render` apply (F5 / F6), so a preview and its send are the same text."""
        def audit(status: str, detail: str) -> None:
            manual_audit(kind, ref, dest, who, status, detail)

        try:
            if dest not in DESTS:
                raise ValueError(f"dest must be one of {DESTS}, got {dest!r}")
            day, rows = _manual_rows(self.db, kind, ref, today=today or self.today(), live=live, dest=dest,
                                     edition=self.edition)  # fmt: skip
            messages = _render_rows(kind, ref, day, rows, dest, sent_hm=self._now_hm(), today=today or self.today())
            buttons = signal_buttons(meta_from_row(rows[0])) if kind in BUTTON_KINDS else None
        except GroupRefusal as exc:
            audit(ST_REFUSED, exc.code)
            self._log_send(kind, dict(ref), dest, ST_REFUSED, exc.code)
            raise
        except Exception as exc:
            audit(REJECTED, type(exc).__name__)
            raise
        try:
            ok, error, sent = await self._send_parts(dest, messages, buttons=buttons)
        except Exception as exc:
            audit(REJECTED, type(exc).__name__)
            raise
        if ok:
            audit(ST_SENT, "tg " + (",".join(str(mid) for mid in sent[REF_MESSAGE_IDS]) or "-"))
        else:
            audit(ST_FAILED, str(error))
        self._log_send(kind, dict(ref), dest, ST_SENT if ok else ST_FAILED, error, sent)
        return {"ok": ok, "message_preview": messages[0], "parts": len(messages), "error": error}

    async def post_watchlists(
        self,
        trade_date: str,
        rows_by_list: Mapping[tuple[str, str], Sequence[Mapping[str, Any]]],
        *,
        label: str | None = None,
        additions: bool | None = None,
        late_hm: str | None = None,
    ) -> int:
        """Auto post after a build. 09:20 (D45): one message (or parts) per (list_name, expiry_date) to the
        GROUP. D148(3) removed the scheduled 13:15 post: `additions=True` (or `additions=None` with a label ending
        "additions", the API's run_job(..., post=True) on a 1315 build) routes to `post_additions` (ONE message for all
        lists, D64's shape). Paused -> nothing sent, 'paused' manual_sends row(s). Returns the lists posted.
        0.8.1 (D160(4)): a CARRY row is never in a group post -- any this is handed is dropped (defensive: the app
        builds the lists from fetch_watchlist_rows, which already leaves them out); a list left empty is not posted.
        0.8.2: the GROUP footer says `two strikes per stock` (0.8.5, D287(4)) (T-215 F6). C3 (F11): `late_hm` "HH:MM" = a late post (a
        catch-up or manual 09:20 build): the label line reads `09:20 build · posted HH:MM`, `ref.label` is unchanged
        (app._posted_0920 keys on it) and `ref.posted_late = late_hm`. C1 (F7): each row after a Telegram attempt carries
        tg_message_ids / chat / thread, plus `n_view` (the rows after the one-strike pick) and `contracts` (the rows the
        message SHOWS, `tpl.contract_label`, at most tpl.LIST_POST_SHOWN)."""
        lists = client_strikes_per_stock(_without_carry(rows_by_list))
        if additions is None:
            additions = bool(label) and str(label).strip().endswith("additions")
        if additions:
            return await self.post_additions(trade_date, lists, label=label)
        posted = 0
        for name, exp in tpl.list_post_order(lists):  # T-154 (D126): EXPIRY-PE, EXPIRY-CE, BREAKDOWN-PE, BREAKOUT-CE
            rows = lists[(name, exp)]
            late = {"posted_hm": late_hm} if late_hm else {}  # an on-time post calls the template as before
            messages = _parts(tpl.list_post(name, exp, rows, trade_date, label=label, group=True, **late))  # T-162 T3
            if not messages:
                continue
            ref: dict[str, Any] = {"list_name": name, "expiry_date": exp, "date": trade_date, "n_rows": len(rows),
                                   "label": label, "n_view": len(rows),
                                   "contracts": [tpl.contract_label(r) for r in tpl.list_post_shown(rows)]}  # fmt: skip
            if late_hm:
                ref["posted_late"] = late_hm
            if self.paused:
                self._log_send(KIND_WATCHLIST_POST, ref, DEST_GROUP, ST_PAUSED, None)
                continue
            ok, error, sent = await self._send_parts(DEST_GROUP, messages)
            self._log_send(KIND_WATCHLIST_POST, ref, DEST_GROUP, ST_SENT if ok else ST_FAILED, error, sent)
            if not ok:
                await self._auto_post_failed("list post", error)
            posted += ok
        return posted

    async def post_additions(
        self,
        trade_date: str,
        lists: Mapping[tuple[str, str], Sequence[Mapping[str, Any]]],
        *,
        label: str | None = None,
        extra_ref: Mapping[str, Any] | None = None,
    ) -> int:
        """D64: the additions of every list in ONE GROUP message (`tpl.additions_list_post` since T-162, the D129
        list-post shape; split by length only). Reached through `post_watchlists(additions=...)` and, since T-277
        (S1.8.0), the scheduled 11:15 / 13:15 / 14:15 batches (`post_pending_additions`, whose `extra_ref` = the
        batch's covered rows lands in the same manual_sends row). Paused -> one 'paused' manual_sends row. Returns the
        lists posted (0 if not sent). 0.8.1: CARRY rows are dropped (never the group, D160(4)). 0.8.2: the footer says
        `two strikes per stock` (F6; 0.8.5 D287(4)); the row carries the C1 keys after a Telegram attempt."""
        lists = client_strikes_per_stock(_without_carry(lists))
        rows: list[dict[str, Any]] = []
        names: list[str] = []
        for name, exp in tpl.list_post_order(lists):  # T-154: the same order as the 09:20 posts
            list_rows = lists[(name, exp)]
            if not list_rows:
                continue
            names.append(f"{name} {exp}")
            for r in list_rows:
                rows.append({**r, "list_name": r.get("list_name") or name, "expiry_date": r.get("expiry_date") or exp})
        if not rows:
            return 0
        # T-162 T3 (D129), wired by the orchestrator: the list-post shape ("ADDED 13:15", six nearest per list)
        since = {"since": str(extra_ref["since"])} if extra_ref and extra_ref.get("since") else {}  # the batch's 'HH:MM'
        rendered = (
            tpl.additions_list_post(trade_date, rows, label=label, group=True, **since) if label
            else tpl.additions_list_post(trade_date, rows, group=True, **since)
        )  # fmt: skip
        messages = _parts(rendered)
        if not messages:
            return 0
        ref = {**dict(extra_ref or {}), "date": trade_date, "lists": names, "n_rows": len(rows), "label": label}
        if self.paused:
            self._log_send(KIND_ADDITIONS_POST, ref, DEST_GROUP, ST_PAUSED, None)
            return 0
        ok, error, sent = await self._send_parts(DEST_GROUP, messages)
        self._log_send(KIND_ADDITIONS_POST, ref, DEST_GROUP, ST_SENT if ok else ST_FAILED, error, sent)
        if not ok:
            await self._auto_post_failed("additions post", error)
        return len(names) if ok else 0

    # -- T-277 (0.8.5, S1.8.0; D268, owner ruling 5 Oct: GROUP additions in three batches, 11:15 / 13:15 / 14:15)
    async def post_pending_additions(self, trade_date: Any, hm: str) -> dict[str, Any]:
        """One scheduled additions batch (`_additions_batch`). `trade_date` = "YYYY-MM-DD" (or a date); `hm` = "1115" /
        "11:15". The result is an AdditionsResult: a dict whose repr is one readable line."""
        return AdditionsResult(await self._additions_batch(trade_date, hm))

    async def _additions_batch(self, trade_date: Any, hm: str) -> dict[str, Any]:
        """One scheduled additions batch:every live non-CARRY row added since the day's last list post, in ONE GROUP
        message through `post_additions` (the same client view as every list post: CARRY dropped, the client strikes per stock;
        label `HH:MM additions`). "Since" is DERIVED from the store, never from memory (a restart between two batches
        loses nothing and posts nothing twice): a row is pending when its build run STARTED after the build run of the
        day's sent 09:20 post (`_additions_state`) and no SENT additions post of the day covers it (each records
        `covered` = [list_name, expiry_date, security_id] of every row it handled, hidden second strikes included; a
        legacy additions row without `covered` covers the rows built before its own time). A paused / failed batch
        covers nothing: its rows ride the next batch. No sent 09:20 post that day -> nothing (that post carries every
        row). One sent post per label a day: a re-fired batch with new rows leaves them for the next batch.

        Returns {"status", "label", "count", "n_view", "lists", "error"}: status in ADDITIONS_STATUSES; count = the
        pending rows this call handled (0 for nothing / no_0920_post; for already_posted the rows left pending);
        n_view = the rows the group message shows (the client strikes per stock); lists = lists posted (post_additions).
        Never raises (a crash = status "error" + the exception text)."""
        out: dict[str, Any] = {"status": ADD_ERROR, "label": f"{hm} additions", "count": 0, "n_view": 0, "lists": 0,
                               "error": None}  # fmt: skip
        try:
            out["label"] = label = f"{self._batch_hm(hm)} additions"
            day = str(trade_date)[:10]
            lock = self.__dict__.setdefault("_additions_lock", asyncio.Lock())  # never two batches at once
            async with lock:
                state = self._additions_state(day)
                if state["anchor"] is None:
                    log.info("additions %s %s: no 09:20 list post sent: nothing posted", day, label)
                    return {**out, "status": ADD_NO_0920}
                rows = self._pending_additions(day, state)
                if not rows:
                    return {**out, "status": ADD_NOTHING}
                if label in state["sent_labels"]:
                    log.warning("additions %s %s: already posted today; %d row(s) wait for the next batch", day,
                                label, len(rows))  # fmt: skip
                    return {**out, "status": ADD_ALREADY, "count": len(rows)}
                lists: dict[tuple[str, str], list[dict[str, Any]]] = {}
                for r in rows:
                    lists.setdefault((str(r["list_name"]), str(r["expiry_date"])), []).append(r)
                view = client_strikes_per_stock(_without_carry(lists))
                extra = {
                    "covered": [[str(r["list_name"]), str(r["expiry_date"]), int(r["security_id"])] for r in rows],
                    "n_view": sum(len(v) for v in view.values()),
                    "contracts": [tpl.contract_label(r) for v in view.values() for r in tpl.list_post_shown(v)],
                    "since": state["since"],
                }
                last_id = int(self.db.execute("SELECT COALESCE(MAX(id), 0) FROM manual_sends").fetchone()[0])
                n_lists = await self.post_additions(day, lists, label=label, extra_ref=extra)
                logged = self.db.execute(
                    "SELECT status, error FROM manual_sends WHERE id > ? AND kind = ? ORDER BY id DESC LIMIT 1",
                    (last_id, KIND_ADDITIONS_POST),
                ).fetchone()
                if logged is None:  # the template rendered nothing: nothing was attempted
                    return {**out, "status": ADD_NOTHING}
                return {**out, "status": str(logged[0]), "count": len(rows), "n_view": extra["n_view"],
                        "lists": int(n_lists), "error": logged[1]}  # fmt: skip
        except Exception as exc:  # the scheduler gets a result, never an exception
            log.exception("additions batch %s failed", hm)
            return {**out, "status": ADD_ERROR, "error": f"{type(exc).__name__}: {exc}"}

    @staticmethod
    def _batch_hm(hm: str) -> str:
        """'11:15' / '1115' / '9:30' -> 'HH:MM'; ValueError otherwise."""
        raw = str(hm).strip().replace(":", "")
        if not raw.isdigit() or len(raw) not in (3, 4):
            raise ValueError(f"batch time must be HH:MM, got {hm!r}")
        h, m = int(raw[:-2]), int(raw[-2:])
        if h > 23 or m > 59:
            raise ValueError(f"batch time must be HH:MM, got {hm!r}")
        return f"{h:02d}:{m:02d}"

    @staticmethod
    def _iso_dt(raw: Any) -> datetime | None:
        try:
            t = datetime.fromisoformat(str(raw))
        except (TypeError, ValueError):
            return None
        return t if t.tzinfo is not None else t.replace(tzinfo=IST)

    def _run_starts(self, day: str) -> dict[str, datetime | None]:
        """run_id -> started_ist of the day's list builds (`watchlists-<day>-<kind>-<ts>`); the run id's own stamp
        when the runs row is missing (a crashed build) is read by `_born`."""
        rows = self.db.execute("SELECT run_id, started_ist FROM runs WHERE run_id LIKE ?",
                               (f"watchlists-{day}-%",)).fetchall()  # fmt: skip
        return {str(r[0]): self._iso_dt(r[1]) for r in rows}

    def _born(self, run_id: Any, starts: Mapping[str, datetime | None]) -> datetime | None:
        rid = str(run_id or "")
        t = starts.get(rid)
        if t is None:
            m = re.search(r"-(\d{8}T\d{6})(\d{0,6})$", rid)
            if m is not None:
                t = datetime.strptime(m[1], "%Y%m%dT%H%M%S").replace(tzinfo=IST)
                t = t.replace(microsecond=int(m[2].ljust(6, "0")) if m[2] else 0)
        return t

    def _additions_state(self, day: str) -> dict[str, Any]:
        """The day's list posts as the store records them: `anchor` = the start of the 0920 build run the first SENT
        09:20 group post followed (its log time when no such run exists; None = no 09:20 post sent), `since` = 'HH:MM'
        of the last SENT list post, `covered` = the rows the SENT additions posts handled, `legacy` = the times of
        SENT additions posts without `covered`, `sent_labels` = their labels, `shown` = (list_name, expiry_date,
        SYMBOL) -> the newest time a later SENT list post (a manual build's `post: true`, label `manual build HH:MM`;
        never a `09:20 build` row) SHOWED that stock in that list (T-309: its rows built by then are not posted again;
        the group got the client strikes per stock)."""
        state: dict[str, Any] = {"anchor": None, "since": "09:20", "covered": set(), "legacy": [],
                                 "sent_labels": set(), "shown": {}}  # fmt: skip
        sends = self.db.execute(
            "SELECT ts_ist, kind, ref FROM manual_sends WHERE kind IN (?, ?) AND dest = ? AND status = ?"
            " AND instr(COALESCE(ref, ''), ?) > 0 ORDER BY id",
            (KIND_WATCHLIST_POST, KIND_ADDITIONS_POST, DEST_GROUP, ST_SENT, day),
        ).fetchall()
        post_ts: datetime | None = None
        for ts_ist, kind, raw in sends:
            try:
                ref = json.loads(raw or "{}")
            except ValueError:
                continue
            if not isinstance(ref, dict) or ref.get("date") != day:
                continue
            at = self._iso_dt(ts_ist)
            if kind == KIND_WATCHLIST_POST and ref.get("label") == LABEL_0920_POST and post_ts is None:
                post_ts = at
            elif kind == KIND_ADDITIONS_POST and post_ts is not None:
                state["sent_labels"].add(str(ref.get("label")))
                state["since"] = str(ref.get("label") or "").removesuffix(" additions") or state["since"]
                covered = ref.get("covered")
                if isinstance(covered, list):
                    state["covered"] |= {(str(c[0]), str(c[1]), int(c[2])) for c in covered
                                         if isinstance(c, list) and len(c) == 3}  # fmt: skip
                elif at is not None:
                    state["legacy"].append(at)
            elif (kind == KIND_WATCHLIST_POST and post_ts is not None and at is not None
                  and ref.get("label") != LABEL_0920_POST):  # fmt: skip
                name, exp = str(ref.get("list_name") or ""), str(ref.get("expiry_date") or "")
                for c in ref.get("contracts") or []:  # 'SYM STRIKE OT' (tpl.contract_label)
                    sym = str(c).strip().rsplit(" ", 2)[0].strip().upper()
                    if name and exp and sym:
                        key = (name, exp, sym)
                        state["shown"][key] = max(at, state["shown"].get(key, at))
        if post_ts is None:
            return state
        starts = self._run_starts(day)
        builds = [t for rid, t in starts.items() if f"-{day}-0920-" in rid and t is not None and t <= post_ts]
        state["anchor"] = max(builds) if builds else post_ts
        return state

    def _pending_additions(self, day: str, state: Mapping[str, Any]) -> list[dict[str, Any]]:
        """The live non-CARRY rows (fetch_watchlist_rows) built after the 09:20 post's build and covered by no sent
        additions post. A row whose build start is unknown counts as covered by the 09:20 post (never a double post)."""
        starts = self._run_starts(day)
        out = []
        for r in fetch_watchlist_rows(self.db, day):
            born = self._born(r.get("run_id"), starts)
            if born is None or born <= state["anchor"]:
                continue
            if (str(r["list_name"]), str(r["expiry_date"]), int(r["security_id"])) in state["covered"]:
                continue
            if any(born <= t for t in state["legacy"]):
                continue
            shown = state.get("shown", {}).get((str(r["list_name"]), str(r["expiry_date"]),
                                                str(r.get("symbol") or "").upper()))  # fmt: skip
            if shown is not None and born <= shown:  # T-309: a manual build's post already showed this stock
                continue
            out.append(r)
        return out

    def summary_data(self, trade_date: str) -> tuple[dict[str, int], list[dict[str, Any]]]:
        """(counts, the day's signals, most important first: E1, then STRONG, then the larger OI drop).
        counts: E1, E2, strong, watchlist, muted (contracts muted now) and the GROUP alert statuses
        sent / failed / paused / gated / muted_alerts (always present, 0 when none). W15: scanner V1 rows only
        (the shadow scanners are graded in gammad/scan, never counted here).
        0.8.2 T-215 F13 (OWNER edition only; the client never routes a card to PERSONAL only): `personal_only` = the V1
        group-entry-kind signals (0.8.5: the E2, `edition.client_entry_kind`; the owner E1 is personal by design) with a
        telegram_personal alert row that is not 'gated' (a card, whatever became of its send) and NO telegram_group row
        -- an off-client or carried card; `operator_summary` prints it `personal-only N`."""
        counts: dict[str, int] = {"E1": 0, "E2": 0, "strong": 0}
        for kind, strong, n in self.db.execute(  # T-310 review M1: a row still held is no signal of the day yet
            "SELECT kind, strong, COUNT(*) FROM signals WHERE trade_date = ? AND scanner = ?" + NOT_HELD_PENDING_SQL
            + " GROUP BY kind, strong",
            (trade_date, SCANNER_LIVE),
        ).fetchall():
            counts[str(kind)] = counts.get(str(kind), 0) + int(n)
            if strong:
                counts["strong"] += int(n)
        counts["watchlist"] = int(
            self.db.execute(
                "SELECT COUNT(*) FROM watchlist WHERE trade_date = ? AND retired_run_id IS NULL"
                + _edition.v1_lists_sql(owner=not self.is_client), (trade_date,)  # D429: a client counts its own lists
            ).fetchone()[0]
        )
        counts["muted"] = int(self.db.execute("SELECT COUNT(*) FROM mutes").fetchone()[0])
        for key in (ST_SENT, ST_FAILED, ST_PAUSED, ST_GATED, "muted_alerts"):
            counts[key] = 0
        for status, n in self.db.execute(
            "SELECT a.status, COUNT(*) FROM alerts a JOIN signals s ON s.id = a.signal_id"
            " WHERE s.trade_date = ? AND a.channel = ? AND s.scanner = ? GROUP BY a.status",
            (trade_date, CH_GROUP, SCANNER_LIVE),
        ).fetchall():
            counts["muted_alerts" if status == ST_MUTED else str(status)] = int(n)
        if not self.is_client:
            counts[tpl.PERSONAL_ONLY_KEY] = int(self.db.execute(
                "SELECT COUNT(DISTINCT s.id) FROM signals s JOIN alerts p ON p.signal_id = s.id"
                " WHERE s.trade_date = ? AND s.scanner = ? AND s.kind = ? AND p.channel = ? AND p.status != ?"
                " AND NOT EXISTS (SELECT 1 FROM alerts g WHERE g.signal_id = s.id AND g.channel = ?)",
                (trade_date, SCANNER_LIVE, _edition.client_entry_kind(client=False), CH_PERSONAL, ST_GATED, CH_GROUP),
            ).fetchone()[0])  # fmt: skip
        return counts, self._ranked_signals(trade_date)

    def group_summary_data(self, trade_date: str) -> tuple[dict[str, int], list[dict[str, Any]]]:
        """What the GROUP 15:35 summary may show: only the day's signals whose GROUP alert row is `sent`
        (ranked like `summary_data`) and the E1 / E2 / strong counts of exactly those. A gated, muted, paused
        or failed signal never reached the group, so its contract and premium must not reach it at 15:35
        either (D63 shadow mode). No operator counters: those are `summary_data`'s, for PERSONAL only.
        T-178 (D148): a row whose signal opened a tracker trade carries it as `trade` (the trades row + its events,
        `gammad.trade.results`, read-only, this edition's kinds): the summary's RESULT line; no trade = no key."""
        sent_ids = {
            int(r[0])
            for r in self.db.execute(
                "SELECT DISTINCT a.signal_id FROM alerts a JOIN signals s ON s.id = a.signal_id"
                " WHERE s.trade_date = ? AND a.channel = ? AND a.status = ?",
                (trade_date, CH_GROUP, ST_SENT),
            ).fetchall()
        }
        trades = trade_results.trades_by_signal(self.db, trade_date, kinds=self.visible_kinds())
        shown = [
            {**r, "trade": trades[int(r["id"])]} if int(r["id"]) in trades else r
            for r in (self._as_group_entry(x) for x in self._ranked_signals(trade_date))
            if int(r["id"]) in sent_ids
        ]
        counts: dict[str, int] = {"E1": 0, "E2": 0, "strong": 0}
        for r in shown:
            counts[str(r.get("kind"))] = counts.get(str(r.get("kind")), 0) + 1
            counts["strong"] += int(bool(r.get("strong")))
        return counts, shown

    def _as_group_entry(self, r: dict[str, Any]) -> dict[str, Any]:
        """0.8.8 ("Group counts only its own lists"): a row the GROUP took as its ENTRY although the owner's R4 stored it
        WATCHLIST + dup_stock (`group_took_demoted`) is the group summary's ENTRY (tier ENTRY, no dup_stock), as the
        client app's summary lists it; every other row as stored. OWNER only (a client row is never promoted)."""
        if self.is_client or r.get("tier") != TIER_WATCHLIST or DUP_STOCK not in reasons_list(r.get("gate_reasons")):
            return r
        if not group_took_demoted(self.db, _as_int(r.get("id"))):
            return r
        kept = ",".join(x for x in reasons_list(r.get("gate_reasons")) if x != DUP_STOCK)
        return {**r, "tier": TIER_ENTRY, "gate_reasons": kept}

    def _ranked_signals(self, trade_date: str) -> list[dict[str, Any]]:
        """The day's signals, most important first: the group / client entry kind (0.8.5 PLAN-D287 I3: E1 in a client
        store, E2 in the owner's), then STRONG, then the larger OI drop."""
        entry_kind = _edition.client_entry_kind(self.is_client)
        return sorted(
            fetch_signals(self.db, trade_date),
            key=lambda r: (r.get("kind") == entry_kind, bool(r.get("strong")), float(r.get("oi_drop_pct") or 0.0)),
            reverse=True,
        )

    async def send_daily_summary(self, trade_date: str, *, group_text: str | None = None) -> bool:
        """15:35 (D64): `tpl.daily_summary` to the GROUP with only the SENT signals and their counts
        (`group_summary_data`; paused -> not sent, 'paused' manual_sends row), then the full operator counters
        `tpl.operator_summary` to PERSONAL (`summary_data`; sent even while paused: an ops message, like
        token/system), OWNER + the V2 Scanner day block after it (`tpl.v2_day_block`, D296 M4; none on a day without a
        V2 entry). Returns whether the GROUP summary was sent."""
        counts, _ = self.summary_data(trade_date)
        ref = {"date": trade_date}
        ok = False
        if self.paused:
            self._log_send(KIND_DAILY_SUMMARY, ref, DEST_GROUP, ST_PAUSED, None)
        else:
            if group_text is None:
                group_counts, sent = self.group_summary_data(trade_date)  # V1 only (V2 never counts in its lines)
                # 0.8.5 (D287(6), PLAN-D287 I7): no `v2_rows` -- the V2 Scanner never reaches the group, nor its summary
                rendered = tpl.daily_summary(trade_date, group_counts, sent)
            else:  # audit JOBS_ALERTS-03 (D714): the 0.8.9 summary, rendered by the caller from the V1N book
                rendered = group_text
            ok, error, c1 = await self._send_parts(DEST_GROUP, _parts(rendered))
            self._log_send(KIND_DAILY_SUMMARY, ref, DEST_GROUP, ST_SENT if ok else ST_FAILED, error, c1)
            if not ok:
                await self._auto_post_failed("day summary", error)
        await self._send_operator_summary(trade_date, counts)
        return ok

    async def _send_operator_summary(self, trade_date: str, counts: Mapping[str, int]) -> bool:
        ref = {"date": trade_date}
        if not self._personal_configured():  # CLIENT §2: operator counters go to PERSONAL only if configured
            return False
        if self._personal_is_group():
            log.error("operator summary not sent: %s", PERSONAL_IS_GROUP)
            self._log_send(KIND_OPERATOR_SUMMARY, ref, DEST_PERSONAL, ST_FAILED, PERSONAL_IS_GROUP)
            return False
        try:
            messages = _parts(tpl.operator_summary(trade_date, dict(counts)))
        except Exception as exc:  # the GROUP summary is already out; log and move on
            log.exception("operator summary template failed")
            self._log_send(KIND_OPERATOR_SUMMARY, ref, DEST_PERSONAL, ST_FAILED, f"render: {type(exc).__name__}")
            return False
        if not messages:
            return False
        if not self.is_client:  # D296 M4: the owner's V2 Scanner day block rides here, PERSONAL only, never the group
            try:
                block = tpl.v2_day_block(trade_date, self.owner_v2_summary_rows(trade_date), edition=self.edition)
            except Exception:  # the ops counters still go out without it
                log.exception("V2 Scanner day block failed; the operator summary is sent without it")
                block = ""
            if block:
                messages = [*messages, *_parts(block)]
        ok, error, sent = await self._send_parts(DEST_PERSONAL, messages)
        self._log_send(KIND_OPERATOR_SUMMARY, ref, DEST_PERSONAL, ST_SENT if ok else ST_FAILED, error, sent)
        return ok

    # -- internals

    def _now_ist(self) -> str:
        return datetime.fromtimestamp(self._clock_ms() / 1000, IST).isoformat(timespec="seconds")

    def _personal_is_group(self) -> bool:
        return bool(self.personal_chat_id) and self.personal_chat_id == self.group_chat_id

    def _dest_chat(self, dest: str) -> tuple[str, str]:
        return (CH_GROUP, self.group_chat_id) if dest == DEST_GROUP else (CH_PERSONAL, self.personal_chat_id)

    async def _send_parts(
        self, dest: str, messages: Sequence[str], *, buttons: Buttons | None = None
    ) -> tuple[bool, str | None, dict[str, Any]]:
        """Send the parts in order; `buttons` ride on the LAST part. Stops at the first failure. Returns (ok, error, the
        C1 ref keys: the delivered parts' message ids in order, the chat used, the group thread; `_c1_ref`)."""
        channel, chat_id = self._dest_chat(dest)
        if not messages:
            return False, "nothing to send", self._c1_ref(channel, chat_id, [], None)
        ids: list[int] = []
        thread: int | None = None
        for i, text in enumerate(messages, 1):
            channel, chat_id = self._dest_chat(dest)  # per part: a group migration may move the chat mid-post
            last = i == len(messages)
            d = await self._deliver_full(channel, chat_id, text, buttons=buttons if last else None)
            chat_id = d.chat
            if d.status != ST_SENT:
                where = f"part {i}/{len(messages)}: " if len(messages) > 1 else ""
                return False, f"{where}{d.error}", self._c1_ref(channel, chat_id, ids, thread)
            if d.message_id is not None:
                ids.append(d.message_id)
            if thread is None:
                thread = d.thread  # the first delivered part's topic (C1: the thread Telegram reported)
        return True, None, self._c1_ref(channel, chat_id, ids, thread)

    @staticmethod
    def _c1_ref(channel: str, chat_id: str, ids: Sequence[int], thread: int | None) -> dict[str, Any]:
        """0.8.2 C1 (FIX-WAVE section 2): the manual_sends.ref keys of a row written after a Telegram attempt --
        `tg_message_ids` (one per delivered part, in order; [] when nothing was delivered), `chat` (`…` + the last 4
        characters of the chat id used) and, GROUP sends only, `thread` (Telegram's reported message_thread_id, else
        "General")."""
        out: dict[str, Any] = {REF_MESSAGE_IDS: list(ids), REF_CHAT: chat_tail(chat_id)}
        if channel == CH_GROUP:
            out[REF_THREAD] = thread if thread is not None else THREAD_GENERAL
        return out

    def _delivery_ref(self, channel: str, d: Delivery) -> dict[str, Any]:
        """C1 keys of ONE `_deliver_full` outcome."""
        ids = [d.message_id] if d.status == ST_SENT and d.message_id is not None else []
        return self._c1_ref(channel, d.chat, ids, d.thread if d.status == ST_SENT else None)

    def _log_send(
        self,
        kind: str,
        ref: Mapping[str, Any],
        dest: str,
        status: str,
        error: str | None,
        sent: Mapping[str, Any] | None = None,
    ) -> None:
        """One manual_sends row. `sent` = the C1 keys (`_c1_ref`) of a row written after a Telegram attempt, merged over
        `ref`; None for a row with no attempt (paused / suppressed)."""
        logged = {**dict(ref), **dict(sent or {})}
        self.db.execute(
            "INSERT INTO manual_sends (ts_ist, kind, ref, dest, status, error) VALUES (?, ?, ?, ?, ?, ?)",
            (self._now_ist(), kind, json.dumps(logged, sort_keys=True, default=str), dest, status, error),
        )
        self.db.commit()

    async def _push(self, message: dict[str, Any], signal_id: int | None) -> None:
        if self.dashboard_push is None:
            return
        try:
            result = self.dashboard_push(message)
            if inspect.isawaitable(result):
                await result
        except Exception as exc:  # noqa: BLE001 -- a broken pusher must not cost the Telegram send
            log.warning("dashboard push failed: %s", exc)
            self._record(signal_id, CH_DASHBOARD, ST_FAILED, None, f"{type(exc).__name__}: {exc}")
        else:
            self._record(signal_id, CH_DASHBOARD, ST_SENT, self._clock_ms(), None)

    async def _deliver(
        self,
        channel: str,
        chat_id: str,
        text: str,
        *,
        reply_to: int | None = None,
        buttons: Buttons | None = None,
        silent: bool = False,
    ) -> tuple[str, str | None, int | None]:
        """One paced send. Returns ('sent', None, message_id | None) or ('failed', error, None).
        reply_to_message_id / buttons / silent (T-162: disable_notification) reach the sender only when set.
        `_deliver_full` is the same send with the chat used and the reported thread (0.8.2 C1)."""
        d = await self._deliver_full(channel, chat_id, text, reply_to=reply_to, buttons=buttons, silent=silent)
        return d.status, d.error, d.message_id

    async def _deliver_full(
        self,
        channel: str,
        chat_id: str,
        text: str,
        *,
        reply_to: int | None = None,
        buttons: Buttons | None = None,
        silent: bool = False,
    ) -> Delivery:
        """`_deliver` returning a `Delivery`: + the chat id actually used (the new id after a group migration) and the
        message_thread_id of Telegram's response (C1). 0.8.2 T-213 F9: after any GROUP send attempt, a Telegram client
        that has just dropped the forum topic (`topic_dead`) triggers `_topic_dead_note` (once per process)."""
        d = await self._deliver_once(channel, chat_id, text, reply_to=reply_to, buttons=buttons, silent=silent)
        if channel == CH_GROUP:
            await self._topic_dead_note()
        return d

    async def _deliver_once(
        self,
        channel: str,
        chat_id: str,
        text: str,
        *,
        reply_to: int | None = None,
        buttons: Buttons | None = None,
        silent: bool = False,
    ) -> Delivery:
        if self.telegram is None or not chat_id:
            if self.telegram is None or channel != CH_PERSONAL:  # an unset personal chat is a choice, not a failure
                self._telegram_failed(channel, "telegram not configured")  # T-181 F5 (R3): the dashboard sees it
            return Delivery(ST_FAILED, "telegram not configured", None, chat_id)
        refused = self._guard_refusal(channel)
        if refused is not None:
            if refused != NO_PERSONAL_CHAT:
                self._telegram_failed(channel, refused)  # T-181 F5 (R3): a refused id counts like a failed send
            return Delivery(ST_FAILED, refused, None, chat_id)
        await self._wait_for_slot(chat_id, is_group=channel == CH_GROUP)
        extra: dict[str, Any] = {}
        if reply_to is not None:
            extra["reply_to_message_id"] = reply_to
        if buttons:
            extra["buttons"] = buttons
        if silent:
            extra["silent"] = True
        try:
            result = await self.telegram.send(chat_id, text, **extra)
        except Exception as exc:  # noqa: BLE001 -- any send failure becomes a 'failed' row
            new_chat = self._migration_target(channel, chat_id, exc)
            if new_chat is None:
                log.warning("telegram %s send failed: %s", channel, exc)
                self._telegram_failed(channel, f"{type(exc).__name__}: {exc}")
                return Delivery(ST_FAILED, f"{type(exc).__name__}: {exc}", None, chat_id)
            self._migrate_group(chat_id, new_chat)
            await self._wait_for_slot(new_chat, is_group=True)
            outcome: Delivery
            try:  # the SAME message, once, to the new id (reply ids do not carry over; allow_sending_without_reply)
                again = await self.telegram.send(new_chat, text, **extra)
                outcome = Delivery(ST_SENT, None, _message_id(again), new_chat, _message_thread(again))
                self._telegram_ok()
            except Exception as exc2:  # noqa: BLE001
                log.warning("telegram %s send failed after the group migration: %s", channel, exc2)
                outcome = Delivery(ST_FAILED, f"{type(exc2).__name__}: {exc2}", None, new_chat)
                self._telegram_failed(channel, outcome.error or "")
            await self._migration_note(chat_id, new_chat)
            return outcome
        self._telegram_ok()
        return Delivery(ST_SENT, None, _message_id(result), chat_id, _message_thread(result))

    async def _topic_dead_note(self) -> None:
        """0.8.2 T-213 F9: the Telegram client dropped TELEGRAM_TOPIC_ID after Telegram answered "message thread not
        found" (T-181 F3: one ERROR line, group sends land in General for the rest of the process) -> ONE personal SYSTEM
        line per process, `group posts are landing in General: topic <id> not found` (send_system: personal only,
        never the group). The latch is set before the await. Never raises."""
        if self._topic_dead_noted or getattr(self.telegram, "topic_dead", False) is not True:
            return
        self._topic_dead_noted = True
        topic = getattr(self.telegram, "topic_id", None)
        try:
            await self.send_system(TOPIC_DEAD_LINE.format(topic=topic if topic is not None else "?"), "SYSTEM")
        except Exception:  # a note about the group must never break the group send it follows
            log.exception("telegram: the dead-topic SYSTEM line failed")

    def _telegram_failed(self, channel: str, error: str) -> None:
        """W14 P1: count a failed send; the TELEGRAM_FAIL_ESCALATE-th in a row logs ONE ERROR and raises the
        dashboard flag (later failures keep the flag current, never a second ERROR until a send succeeds)."""
        self.telegram_fail_streak += 1
        if self.telegram_fail_streak < TELEGRAM_FAIL_ESCALATE:
            return
        first = self.telegram_last_error is None
        self.telegram_last_error = f"{channel}: {error}"[:300]
        if first:
            log.error("telegram: %d sends in a row failed, alerts are NOT reaching Telegram (last: %s)",
                      self.telegram_fail_streak, self.telegram_last_error)  # fmt: skip

    async def _auto_post_failed(self, what: str, error: str | None) -> None:
        """T-181 F5 (R3): ONE ERROR line at the first failed AUTOMATIC group post (a list post, an entry card, the day
        summary): Check Setup's `errors` check scans ERROR lines, and before this a client whose group refused every post
        had no ERROR in gammad.log unless three sends in a row failed after start. 0.8.2 T-213 F2 (D189(4)): latched per
        (IST day, what), and beside the ERROR line ONE personal SYSTEM line `GROUP SEND FAILED · <what> · <error ≤200>`
        through `send_system` (personal only; a client without a personal chat = dashboard only; never the group).
        Awaited by its async callers; never raises into the send path."""
        key = (self.today(), what)
        if key in self._auto_fail_keys:
            return
        self._auto_fail_keys.add(key)
        detail = (error or "no detail")[:200]
        log.error(AUTO_POST_FAILED, what, detail)
        try:
            await self.send_system(GROUP_SEND_FAILED_LINE.format(what=what, error=detail), "SYSTEM")
        except Exception:  # the failure line must never cost the caller's own bookkeeping
            log.exception("telegram: the GROUP SEND FAILED line for %s failed", what)

    def _telegram_ok(self) -> None:
        if self.telegram_last_error is not None:
            log.info("telegram: sending works again after %d failures", self.telegram_fail_streak)
        self.telegram_fail_streak = 0
        self.telegram_last_error = None

    # -- T-062 guard + basic-group migration (CLIENT-PACKAGE-SPEC §3)

    def _guard_refusal(self, channel: str) -> str | None:
        """The T-062 refusal for a send on `channel`, or None when allowed (no guard = allowed)."""
        guard = self.telegram_guard
        if guard is None:
            return None
        if channel == CH_GROUP and not guard.group_ok:
            return "group chat id refused (T-062): " + "; ".join(p for p in guard.problems if "GROUP" in p)
        if channel == CH_PERSONAL and not guard.personal_ok:
            if not guard.personal_set:
                return NO_PERSONAL_CHAT
            return "personal chat id refused (T-062): " + "; ".join(p for p in guard.problems if "PERSONAL" in p)
        return None

    def _personal_configured(self) -> bool:
        """False when the PERSONAL chat is intentionally absent (an unset client personal id, T-062): those
        messages reach the dashboard only and no 'failed' row is written for them."""
        guard = self.telegram_guard
        if guard is not None and not guard.personal_set:
            return False
        return not (self.is_client and not self.personal_chat_id)  # CLIENT §3: optional for a client

    def _migration_target(self, channel: str, chat_id: str, exc: BaseException) -> str | None:
        """The supergroup id to follow, or None. Followed ONLY for the configured GROUP chat on the group
        channel, only to a ^-100\\d+$ id and only once per old id per process; a hint for the PERSONAL chat
        (or any other chat) is logged and ignored -- it must never re-point that chat."""
        raw = getattr(exc, "migrate_to_chat_id", None)
        if raw is None:
            return None
        new_chat = str(raw).strip()
        if channel != CH_GROUP or not chat_id or chat_id != self.group_chat_id:
            log.warning("telegram: migrate hint for a non-group chat (%s) ignored", channel)
            return None
        if not SUPERGROUP_RE.fullmatch(new_chat):
            log.warning("telegram: group migrate hint to a non-supergroup id ignored")
            return None
        if chat_id in self._migrated_from:
            log.warning("telegram: group %s already migrated once in this process; hint ignored", mask(chat_id))
            return None
        return new_chat

    def _migrate_group(self, old_chat: str, new_chat: str) -> None:
        """Steps 1-2 of §3: switch the GROUP id in memory, persist TELEGRAM_GROUP_ID, log a token_events row."""
        self._migrated_from.add(old_chat)
        self.group_chat_id = new_chat
        detail = f"{mask(old_chat)} -> {mask(new_chat)}"
        log.warning("telegram: group upgraded to a supergroup (%s); group chat id switched", detail)
        if self.env_path is None:
            log.warning("telegram: group migration not persisted (no .env path given); update %s by hand",
                        TELEGRAM_GROUP_KEY)
        else:
            try:
                write_env_value(self.env_path, TELEGRAM_GROUP_KEY, new_chat)
            except Exception:
                log.exception("telegram: could not persist %s after the group migration", TELEGRAM_GROUP_KEY)
        try:
            self.db.execute(
                "INSERT INTO token_events (ts_ist, kind, detail) VALUES (?, ?, ?)",
                (self._now_ist(), TOKEN_EVENT_GROUP_MIGRATED, detail),
            )
            self.db.commit()
        except sqlite3.Error:
            log.exception("telegram: could not record the group migration")

    async def _migration_note(self, old_chat: str, new_chat: str) -> None:
        """Step 4 of §3: a SYSTEM note to PERSONAL + dashboard, after the retry. Never raises."""
        text = (
            f"Telegram group upgraded to a supergroup: alerts now go to the new group id ({mask(new_chat)}, "
            f"was {mask(old_chat)}); {TELEGRAM_GROUP_KEY} updated. Replies to older messages may arrive unthreaded."
        )
        try:
            await self.send_system(text, "SYSTEM")
        except Exception:
            log.exception("telegram: group migration note failed")

    async def _telegram(self, signal_id: int | None, channel: str, chat_id: str, text: str) -> None:
        status, error, message_id = await self._deliver(channel, chat_id, text)
        sent = status == ST_SENT
        self._record(signal_id, channel, status, self._clock_ms() if sent else None, error, message_id)

    async def _wait_for_slot(self, chat_id: str, *, is_group: bool) -> None:
        chat = self._chat_limits.get(chat_id)
        if chat is None:
            chat = SlidingWindow(self._per_chat_rate, 1.0)
            self._chat_limits[chat_id] = chat
        limits = [chat, self._group_limit] if is_group else [chat]
        now = self._mono()
        at = max(lim.next_slot(now) for lim in limits)
        for lim in limits:
            lim.commit(at)
        if at > now:
            await self._sleep(at - now)

    def _record(
        self,
        signal_id: int | None,
        channel: str,
        status: str,
        sent_ts_ms: int | None,
        error: str | None,
        tg_message_id: int | None = None,
    ) -> None:
        self.db.execute(
            "INSERT INTO alerts (signal_id, channel, sent_ts_ms, status, error, tg_message_id)"
            " VALUES (?, ?, ?, ?, ?, ?)",
            (signal_id, channel, sent_ts_ms, status, error, tg_message_id),
        )
        self.db.commit()
