"""Telegram message templates (D64 redesign, S1.1.0) and the copy/export text forms (D44). Pure, no I/O.

T-178 (D148, the owner's "L1 + C2 blend", docs/design-previews/telegram-v5/blend.html) supersedes the T-162 layout
below for the TIERED E1 cards (ENTRY / NEAR ENTRY / WATCH ONLY, D273 names) and the 15:35 day summary: a headline, a
<blockquote> contract box with labelled lines, a tap-to-copy code line (`copy_line`), the checks with the passed ones on
one line, heads-up sentences; the summary keeps entries apart from watch-only cards and reads each entry's result from
the trade tracker. The owner DESK block is added ONLY on request (`desk=True`, the owner's PERSONAL send): a card sent
to the GROUP never carries it (clients read the group). The untiered E1 and the owner E2 keep the T-162 sections.

T-162 (D129, the owner's pick = docs/telegram-previews-v3/ candidate 2 "Sections"; final.source.html is rendered from
this module): every SIGNAL card opens with a two-line headline (T4) -- tier emoji + bold tier word + side emoji + bold
contract, then "📅 expiry · 🕒 time IST" -- followed by <u><b>SECTION</b></u> headers. ENTRY (and an untiered E1 / the
owner E2): PLAN (entry + max, the option SL only for an intraday hold, the stock stop ALWAYS (T2), T1, T2, lot),
WHY IT FIRED (a quote), HEADS-UP (the D114(1) warnings), then the disclaimer (client) or DESK / ARMING (owner). STRONG
WATCHLIST / WATCHLIST: "Watch only — n of 4 checks passed", CHECKS (the four gates in GATE_NAMES order, the failed one
bold), RIGHT NOW, HEADS-UP. One emoji vocabulary (VOCABULARY), one meaning each. The 09:20 list post (`list_post`,
T3) = the six contracts nearest the zone + "N more on the dashboard"; `additions_list_post` is its 13:15 form.
Trade follow-ups and the day summary keep their content with the same headline grammar and vocabulary.

The earlier designs, still here where the v3 pick does not reach, are the owner's picks from docs/telegram-previews/
(D57, D64):
- List posts and 13:15 additions: the D64 B / C forms (watchlist_post / additions_post) were retired by T-165; the routed forms are list_post / additions_list_post (T-162 T3, D129).
- Entries (E2 / E1): the D79 C "Sheet" (docs/telegram-previews-v2/messages/C-03*), per EDITION (D81, D86, D87):
  a headline, a plan box (ENTRY · max / SL / T1 / T2 / LOT / RISK; T1/T2 = gammad.trade.rules levels, D80), WHY IT
  FIRED, then the OWNER-only DESK block (the D94 OI-price quadrant, OI peak -> now, list/tag/time, gate verdict)
  or the CLIENT disclaimer. The client sees E1 only, as "ENTRY"; an E2 card for the client raises. SWING cards
  (D58) show the D68 stock stop; same-day cards the 5-min base low. NO "BUY"/"SELL" in any alert (D79): the
  order line is the "Copy order" button text (`copy_order_text`). URL buttons: `signal_buttons`.
  W15 (D114, PLAN-W15 §2): an E1 card's headline is its tier (TIER_LABEL: ENTRY / NEAR ENTRY / WATCH ONLY, D273; both
  editions); `warnings()` builds the near-expiry and late-day warnings, each its own card line under the hold line.
  The E2 card (untiered) is unchanged. No client-visible string says "intraday" (the strategy is a hold).
- Trade follow-ups (TRACKER §7): `trade_followup`, the same C style, replies to the entry.
- Watchlist rows sent by hand keep the D ticket (a price ladder in a <pre> of at most PRE_MAX_COLS columns).
- Signals table: B, as contract chains, so each E2 is followed by its E1.
- Daily summary: B for the GROUP, with no operator counters and no 15:30 price. `operator_summary` is the new
  PERSONAL ops message that holds those counters.
- Token / system messages: a bold title and one plain sentence that includes the fix.
No message carries latency or spec_version, which are operator telemetry. No message says "BUY" or "SELL" (D79,
both editions); no client message says "algo"/"algorithm" or refers to an E2 (D87).

Splitting (D64) is by LENGTH, never by row count. A part takes whole blocks (a stock, a contract chain) while its
visible text stays within PART_MAX_CHARS UTF-16 units and PART_MAX_ENTITIES entities. Telegram's own limits are
4096 characters after entity parsing and 100 entities; the entity limit comes only from the unofficial limits page.
`tg_measure` measures each part as rendered with a worst-case "part 99/99" tag. Parts are numbered "· part i/n".
A <pre> or a quote never spans two messages.
Entity count: one per opening tag, except a <code> directly inside a <pre>, which Telegram merges into the pre entity.

The export forms (`tsv`, `row_line`) are PLAIN text for the clipboard: no HTML, no emoji.
"""

from __future__ import annotations

import html
import json
import math
import re
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import date, datetime, time
from decimal import ROUND_FLOOR, ROUND_HALF_UP, Decimal
from html.parser import HTMLParser
from typing import TYPE_CHECKING, Any
from urllib.parse import quote

from gammad import edition as edition_mod
from gammad.alerts.gates import GATE_NAMES, hold_for
from gammad.config import IST, GatesCfg, HoldCfg
from gammad.trade.rules import (  # 0.8.4 (T-267): the V2 Scanner zone family
    LIVE_STATES,
    MIN_TICK,
    SW_STATES,
    T1_PCT,
    T2_PCT,
    V1R_STATES,
    V2T,
    ZONE_HOLD_DAYS,
    ZONE_STATES,
    ZSTEP,
    hold_at_entry,
    target_price,
    trading_dte,
)
from gammad.watch.types import ContractMeta, Signal, SignalContext

if TYPE_CHECKING:
    from gammad.core.calendar import TradingCalendar

# 0.8.3 T-212 (D195(5)): PE / CE = ▼ / ▲ everywhere, so red / green only ever mean failed / passed (catalogue §4.3)
ICON_PE = "\N{BLACK DOWN-POINTING TRIANGLE}"
ICON_CE = "\N{BLACK UP-POINTING TRIANGLE}"
ICON_STRONG = "\N{BLACK STAR}"
ICON_E1 = "\N{HIGH VOLTAGE SIGN}"
ICON_E2 = "\N{DIRECT HIT}"
ICON_TOKEN = "\N{KEY}"
ICON_SYSTEM = "\N{SATELLITE}\N{VARIATION SELECTOR-16}"
ICON_LIST = "\N{CLIPBOARD}"
ICON_SUMMARY = "\N{RECEIPT}"  # T-162: 📊 means volume in the v3 vocabulary
ICON_ADD = "\N{HEAVY PLUS SIGN}"
ICON_CHART = "\N{CHART WITH UPWARDS TREND}"

DOT = " \N{MIDDLE DOT} "
MINUS = "\N{MINUS SIGN}"
TIMES = "\N{MULTIPLICATION SIGN}"
EN_DASH = "\N{EN DASH}"
ARROW = "\N{RIGHTWARDS ARROW}"
RUPEE = "\N{INDIAN RUPEE SIGN}"
EM_DASH = "\N{EM DASH}"

# T-162 (D129): the card vocabulary of docs/telegram-previews-v3/DESIGN-OPTIONS-telegram-v3.md, one meaning each.
E_ENTRY = "\N{HIGH VOLTAGE SIGN}"
E_WATCH = "\N{EYES}"
E_PUT = ICON_PE  # 0.8.3 T-212 (D195(5)): ▼ (was 🔻)
E_CALL = ICON_CE  # ▲ (was 🔺)
E_PRICE = "\N{MONEY BAG}"
E_SL = "\N{OCTAGONAL SIGN}"
E_T1 = "\N{DIRECT HIT}"
E_T2 = "\N{CHEQUERED FLAG}"
E_EDGE = "\N{BRICK}"
E_OI = "\N{CHART WITH DOWNWARDS TREND}"
E_VOL = "\N{BAR CHART}"
E_EXPIRY = "\N{CALENDAR}"
E_TIME = "\N{CLOCK FACE THREE OCLOCK}"
E_DTE = "\N{HOURGLASS WITH FLOWING SAND}"
E_LATE = "\N{SUNSET OVER BUILDINGS}"
# 0.8.3 T-212 (D195(5), catalogue §5): Telegram has no text colour, so colour = five emoji markers, ONE meaning each
M_PASS = "\N{LARGE GREEN CIRCLE}"  # passed / gain / delivered
M_FAIL = "\N{LARGE RED CIRCLE}"  # failed / loss / missed
M_OPEN = "\N{LARGE YELLOW CIRCLE}"  # still open / heads-up
M_WATCH = "\N{LARGE BLUE CIRCLE}"  # not an entry
NOT_ENTRY = "not an entry"  # D273: M_WATCH's words on a NEAR ENTRY / WATCH ONLY card (was "watch only": a tier name now)
M_EXPIRY_WEEK = "\N{LARGE PURPLE CIRCLE}"  # the contract expires within EXPIRY_WEEK_DAYS (the dashboard's H3, D182(1))
EXPIRY_WEEK_DAYS = 5
E_PASS = M_PASS  # T-212: a passed check is the green marker (was ✅)
E_FAIL = M_FAIL  # a failed check is the red marker (was ❌)
# T-212 (catalogue §4.3): the owner E2 (arming) card's own icon; 👀 = the watch tier only. Not ⏱ (a latency stopwatch:
# tests/test_templates.py bans it, no message carries operator telemetry)
E_ARMING = "\N{SOON WITH RIGHTWARDS ARROW ABOVE}"
E_TABLE = "\N{SPIRAL NOTE PAD}\N{VARIATION SELECTOR-16}"  # T-212: the signals table (📋 = the tap-to-copy line)
E_WHY = "\N{ELECTRIC LIGHT BULB}"
E_MORE = "\N{LEFT-POINTING MAGNIFYING GLASS}"
E_LIST = "\N{CARD INDEX DIVIDERS}\N{VARIATION SELECTOR-16}"
E_LOT = "\N{PACKAGE}"
E_COPY = "\N{CLIPBOARD}"  # T-178 (D148): the tap-to-copy contract line under a card's box
E_OPEN = "\N{DOUBLE VERTICAL BAR}"  # T-178 day summary: an entry still open at the day end
E_CLOSED = "\N{BELL}"  # T-178 day summary: an entry closed at the day end / expiry / day 5
VOCABULARY: dict[str, str] = {
    E_ENTRY: "entry tier", E_WATCH: "near entry / watch only tier", E_PUT: "put", E_CALL: "call", E_PRICE: "entry / premium",
    E_SL: "SL", E_T1: "T1", E_T2: "T2", E_EDGE: "support / resistance edge", E_OI: "OI drop", E_VOL: "volume",
    E_EXPIRY: "expiry", E_TIME: "time", E_DTE: "expiry warning", E_LATE: "day-closing warning",
    E_WHY: "why", E_MORE: "details", E_LIST: "list post",
    E_LOT: "lot", E_COPY: "tap to copy the contract", E_OPEN: "still open (result)", E_CLOSED: "closed at a set time",
    M_PASS: "passed / gain / delivered", M_FAIL: "failed / loss / missed", M_OPEN: "still open / heads-up",
    M_WATCH: NOT_ENTRY, M_EXPIRY_WEEK: "expires within 5 trading days", E_ARMING: "arming (owner possible entry)",
    E_TABLE: "signals table",
}  # fmt: skip
BLEND_GAP = "  "  # T-178 (D148): the blend's gap between a label and its value (Telegram keeps both spaces)
OPEN_CONTRACT_LABEL = "\N{CHART WITH UPWARDS TREND} Open contract"  # the NSE F&O page (D129 T1 buttons)
ICON_ARMING = E_ARMING  # the owner E2 (arming) card: T-212 gives it its own icon (👀 is the watch tier's)
# 0.8.3 T-212 style B (D195(5)): a ━ divider between sections. 12 glyphs = 216 px at 18 px, one line in the 250 px text
# column of a 360 dp phone (catalogue §5, measured on the previews)
DIV = "\N{BOX DRAWINGS HEAVY HORIZONTAL}" * 12
NBSP = "\N{NO-BREAK SPACE}"  # inside a DISPLAY date only (`29 Sep` never splits, catalogue §4.5); never in copy text
BADGE_GAP = "   "  # the gap between a headline word and its marker / 4-dot badge (the style B previews)
LIST_POST_SHOWN = 6  # D129 T3: the six contracts nearest the zone; the rest are on the dashboard

# W15 (D114, PLAN-W15 §2): the E1 tier -> its card headline; `gates.tier` gives the key (B1), the router passes it.
# D273 (0.8.5, owner ruling): the DISPLAY names, both editions, every surface: STRONG_WATCHLIST reads NEAR ENTRY and
# WATCHLIST reads WATCH ONLY (the word "watchlist" means the LISTS of contracts only). The keys = the stored tier
# codes, unchanged. This is the ONE mapping: every card headline, count line, section, table label and the install
# card read it (tests/test_t273_cards.py swaps the words and finds them on every surface).
TIER_LABEL: dict[str, str] = {"ENTRY": "ENTRY", "STRONG_WATCHLIST": "NEAR ENTRY", "WATCHLIST": "WATCH ONLY"}
# 0.8.1 (S1.5.0, D159(2)): an ENTRY headline names its scanner, `ENTRY · V1` / `ENTRY · V1 PLUS`. The CLIENT edition
# shows V1 only in 0.8.1 (D159(1)): any other scanner label, the `also V1 PLUS` line and a carried card raise there.
SCANNER_V1 = "V1"
SCANNER_V1_PLUS = "V1 PLUS"
# The scanner line (`scanner_line`: "V1 Scanner" / "V2 Scanner") is on the OWNER's PERSONAL cards only -- never a
# client-edition text, never a GROUP card (`scanner_shown`; 0.8.8, owner 13 Oct "Remove in group too" + D429 + D433,
# superseding 0.8.4 T-267 / D248(3)'s "the first line of every card names its scanner, both editions"). The tier line
# never repeats the name (was `ENTRY · V1`). SCANNER_TITLE mirrors engine_pool.SCANNER_LABELS (a test pins the two
# equal; the stored id "V2" = the old S1.3.1 shadow, shown as V2-S131 on owner surfaces, D248(2)); an unmapped id is its
# own title ("V1 PLUS", owner PERSONAL cards only). The V2 Scanner (internal id V2T = rules.V2T) is owner-edition only
# and never posts to the group (0.8.5 D287(6)).
SCANNER_V2 = V2T
CLIENT_SCANNERS = frozenset({SCANNER_V1})
SCANNER_TITLE: dict[str, str] = {SCANNER_V1: "V1 Scanner", SCANNER_V2: "V2 Scanner", "V2": "V2-S131"}
ALSO_PLUS_LINE = f"{ICON_ADD} also {SCANNER_V1_PLUS}"  # D161(1): the owner's PERSONAL copy of a shared V1 card only
NOT_IN_GROUP = "not in the group"  # 0.8.2 T-202 (D176(1)): the owner's PERSONAL-only card line (add_not_in_group)
PLUS_HEAD_SUFFIX = "breakout at the zone"  # a V1 PLUS-only trade passed V1 B + the zone, not V1's four checks
# D159(3): pre-written, used only when V1 PLUS reaches clients (a later 0.8.x); the wording is asked at the switch-on
PLUS_RISK_LINE = "\N{WARNING SIGN} V1 PLUS: more trades than V1, deeper losing runs. Take a smaller size."
E_CARRY = "\N{CLOCKWISE RIGHTWARDS AND LEFTWARDS OPEN CIRCLE ARROWS}"  # a carried contract (owner PERSONAL cards)
TAG_CARRY = "CARRY"  # watchlist.tag of a carried row (CONTRACTS 2)
# 0.8.9 T-419 "Still Watching" (D471 Q1 / Q5 / Q6 / Q7 / Q8, D472 X9A, D480; contract T419-CONTRACT.md): a carried ENTRY
# is the Still Watching card in BOTH editions (`sw_entry_card`); its follow-ups are the SW_* kinds (`trade_followup`).
# The client words never say carry / carried / V1 / E1 (D437); the internal words stay on the owner's DESK only.
STILL_WATCHING = edition_mod.CLIENT_STILL_WATCHING  # the client list name, exactly "Still Watching"
SCANNER_SW = "SW"  # = gammad.trade.rules.SCANNER_SW: trades.scanner of a Still Watching trade (never shown)
SW_HOLD = "carry"  # trades.hold of a Still Watching trade (never shown: the card reads STILL_WATCHING)
# The levels an entry card reads from the signal's snapshot (the engine writes them; SwPosition's field names):
# snapshot["sw"] = {"near_edge": float, "far_edge": float, "target": float | None}
SW_SNAPSHOT_KEY = "sw"
SW_STOP, SW_TARGET, SW_TRAIL, SW_EVE = "SW_STOP", "SW_TARGET", "SW_TRAIL", "SW_EVE"
SW_FOLLOWUP_KINDS: tuple[str, ...] = (SW_STOP, SW_TARGET, SW_TRAIL, SW_EVE)  # SW_TARGET is NOT an exit (the trail starts)
SW_EXIT_KINDS = frozenset({SW_STOP, SW_TRAIL, SW_EVE})
# The card warnings (D114(1): no time cut-off, a warning instead). AlertsCfg keys (B1), read with these defaults.
LATE_DAY_WARN_IST_DEFAULT = "14:30"  # alerts.late_day_warn_ist
EXPIRY_WARN_DTE_DEFAULT = 7  # alerts.expiry_warn_dte (trading days to expiry, inclusive)

TG_MAX_CHARS = 4096  # Telegram: 1-4096 characters after entity parsing
TG_MAX_ENTITIES = 100  # the unofficial per-message entity ceiling (DESIGN-NOTES.md)
PART_MAX_CHARS = 3800  # the packing budget: a margin under TG_MAX_CHARS (DESIGN-NOTES.md splitting rule)
PART_MAX_ENTITIES = 90  # the packing budget: a margin under TG_MAX_ENTITIES
PRE_MAX_COLS = 25  # a group <pre> fits 25 monospace columns on a 375 pt iPhone (D57)
CARD_MAX_LINES = 12
DAILY_TOP_MAX = 5

KIND_ICON = {"E1": ICON_E1, "E2": ICON_E2}
E1_TRIGGER = {"breakout": "Broke above the range", "shakeout": "Dipped under the range, reclaimed mid"}
# T-154 (D126): near-expiry lists first, everywhere (list posts, additions, summaries; both dashboards follow it)
LIST_ORDER = ("EXPIRY-PE", "EXPIRY-CE", "BREAKDOWN-PE", "BREAKOUT-CE")

WATCHLIST_EXPORT_COLUMNS: tuple[str, ...] = (
    "trade_date", "list_name", "expiry_date", "symbol", "strike", "option_type", "tag", "oi", "lot_size",
    "ltp", "zone_dist_pct", "ref_price", "zone_edge", "security_id",
)  # fmt: skip
SIGNAL_EXPORT_COLUMNS: tuple[str, ...] = (
    "id", "trade_date", "time_ist", "kind", "strong", "symbol", "strike", "option_type", "tag", "list_name",
    "expiry_date", "ltp", "oi_drop_pct", "vol_ratio", "zone_dist_pct", "sl", "latency_ms", "security_id",
)  # fmt: skip
# T-376 (0.8.8 client leak; D429 final (a) / D433 / D87): the CLIENT edition's copy / export words. The tier names are
# the dashboard's CLIENT ones (dashboard/src/signalctx.ts TIER_LABEL: ENTRY reads ENTRY TRIGGERED there, so this is NOT
# TIER_LABEL above), an untiered row reads "Entry"; STRONG is a client word. The client TSV columns carry no kind, latency,
# row / security id, payload / context JSON or raw gate reasons; dashboard/src/textfmt.ts holds the same strings.
CLIENT_TIER_LABEL: dict[str, str] = {"ENTRY": "ENTRY TRIGGERED", "STRONG_WATCHLIST": "NEAR ENTRY", "WATCHLIST": "WATCH ONLY"}
CLIENT_TIER_NONE = "Entry"
CLIENT_SIGNAL_EXPORT_COLUMNS: tuple[str, ...] = (
    "trade_date", "time_ist", "tier", "strong", "symbol", "strike", "option_type", "tag", "list_name", "expiry_date",
    "last_price", "oi_drop_pct", "volume_x", "zone_dist_pct", "sl",
)  # fmt: skip
CLIENT_WATCHLIST_EXPORT_COLUMNS: tuple[str, ...] = (
    "trade_date", "list_name", "expiry_date", "symbol", "strike", "option_type", "tag", "oi", "lot_size", "last_price",
    "zone_dist_pct", "ref_price", "zone_edge",
)  # fmt: skip

Render = Callable[[Sequence[Any], int, int], str]


# ------------------------------------------------------------------------------------------ number formats
def esc(value: Any) -> str:
    return html.escape(str(value), quote=False)


def indian_int(n: float) -> str:
    """1,58,350 (Indian digit grouping: last three, then pairs). Rounds to the nearest integer."""
    v = round(float(n))
    s = str(abs(v))
    head, tail = s[:-3], s[-3:]
    groups: list[str] = []
    while len(head) > 2:
        groups.insert(0, head[-2:])
        head = head[:-2]
    if head:
        groups.insert(0, head)
    out = ",".join([*groups, tail]) if groups else tail
    return f"-{out}" if v < 0 else out


def indian_short(n: float) -> str:
    """1.58L / 2.35Cr; below one lakh the grouped integer (58,350)."""
    v = float(n)
    a = abs(v)
    sign = "-" if v < 0 else ""
    if a >= 1e7:
        return f"{sign}{a / 1e7:.2f}Cr"
    if a >= 1e5:
        return f"{sign}{a / 1e5:.2f}L"
    return indian_int(v)


def oi_short(n: float) -> str:
    """OI for a phone line: 2.35Cr / 3.24L / 58.4K / 950."""
    v = float(n)
    a = abs(v)
    sign = "-" if v < 0 else ""
    if a >= 1e7:
        return f"{sign}{a / 1e7:.2f}Cr"
    if a >= 1e5:
        return f"{sign}{a / 1e5:.2f}L"
    if a >= 1e3:
        return f"{sign}{a / 1e3:.1f}K"
    return f"{v:.0f}"


def rupees(x: float) -> str:
    """₹55.20 / ₹1,234.50 (two decimals, Indian grouping)."""
    v = float(x)
    whole, frac = f"{abs(v):.2f}".split(".")
    return f"{'-' if v < 0 else ''}{RUPEE}{indian_int(int(whole))}.{frac}"


def plain_num(x: float) -> str:
    """Strike / generic number: 12000, 3100.5 (no grouping, no trailing .0)."""
    v = float(x)
    return f"{v:.0f}" if v.is_integer() else f"{v:g}"


def price_text(x: float) -> str:
    """A stock price in proportional text: 219.20 / 1,533.9 / 10,878 (decimals shrink with the magnitude)."""
    v = float(x)
    if abs(v) >= 10000:
        return indian_int(v)
    if abs(v) >= 1000:
        whole, frac = f"{v:.1f}".split(".")
        return f"{indian_int(int(whole))}.{frac}" if frac != "0" else indian_int(int(whole))
    return f"{v:.2f}"


def price_plain(x: float) -> str:
    """A stock price without grouping: 219.80 / 3427.7 / 10917."""
    v = float(x)
    return f"{v:.0f}" if abs(v) >= 10000 else f"{v:.1f}" if abs(v) >= 1000 else f"{v:.2f}"


def zone_price(x: float) -> str:
    """A zone edge on a list card: 1 decimal, whole rupees from 1,000 (219.2 / 3454)."""
    v = float(x)
    return f"{v:.0f}" if abs(v) >= 1000 else f"{v:.1f}"


def ist_hms(ts_ms: int) -> str:
    return datetime.fromtimestamp(int(ts_ms) / 1000, IST).strftime("%H:%M:%S")


def ist_hm(ts_ms: int) -> str:
    return datetime.fromtimestamp(int(ts_ms) / 1000, IST).strftime("%H:%M")


def expiry_short(iso: str) -> str:
    try:
        return date.fromisoformat(str(iso)).strftime("%d-%b")
    except ValueError:
        return str(iso)


def date_long(iso: str | date) -> str:
    try:
        d = iso if isinstance(iso, date) else date.fromisoformat(str(iso))
    except ValueError:
        return str(iso)
    return d.strftime("%d-%b-%Y")


def day_short(iso: str | date) -> str:
    """'4 Sep' / '29 Sep' (a phone-length date). Returns the ESCAPED input if it is not an ISO date."""
    try:
        d = iso if isinstance(iso, date) else date.fromisoformat(str(iso))
    except ValueError:
        return esc(iso)
    return f"{d.day} {d:%b}"


def _as_date(value: Any) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value))
    except ValueError:
        return None


def disp_day(iso: str | date) -> str:
    """T-212: `day_short` for DISPLAY, a NO-BREAK SPACE inside (`29 Sep` never splits on a phone, catalogue §4.5).
    Never used in the copy-order / code-span text (a broker paste must stay plain)."""
    return day_short(iso).replace(" ", NBSP)


def disp_weekday(iso: str | date) -> str:
    """'Mon 28 Sep' with NO-BREAK SPACEs (T-212); the ESCAPED input if it is not an ISO date."""
    d = _as_date(iso)
    return esc(iso) if d is None else f"{d:%a}{NBSP}{d.day}{NBSP}{d:%b}"


def expiry_week(expiry: Any, ref: Any) -> bool:
    """T-212 🟣 (D195(5), the dashboard's H3, D182(1)): the contract expires within EXPIRY_WEEK_DAYS trading days of
    `ref` (the card's / post's own day), expiry day included. False when either date is unreadable."""
    e, r = _as_date(expiry), _as_date(ref)
    from gammad.core.calendar import default_calendar

    return (e is not None and r is not None and e >= r
            and 0 <= trading_dte(default_calendar(), r, e) <= EXPIRY_WEEK_DAYS)


def expiry_mark(expiry: Any, ref: Any) -> str:
    """' 🟣' after a displayed expiry in expiry week, else ''."""
    return f" {M_EXPIRY_WEEK}" if expiry_week(expiry, ref) else ""


def _ms_day(ts_ms: Any) -> date | None:
    return datetime.fromtimestamp(int(ts_ms) / 1000, IST).date() if ts_ms else None


def month_short(iso: str) -> str:
    try:
        return date.fromisoformat(str(iso)).strftime("%b")
    except ValueError:
        return esc(iso)


def side_icon(option_type_or_list: str | None) -> str:
    s = str(option_type_or_list or "").upper()
    return ICON_PE if s == "PE" or s.endswith("-PE") else ICON_CE


def pct_drop(drop_pct: float) -> str:
    """OI drop in percent units: 10.4 -> −10.4% (a rise shows as +)."""
    v = float(drop_pct)
    return f"{MINUS}{v:.1f}%" if v >= 0 else f"+{-v:.1f}%"


def zone_dist_pct(row: Mapping[str, Any]) -> float | None:
    """Stock distance to the zone edge in percent units (D20): the row's own value, else from ref/edge."""
    if row.get("zone_dist_pct") is not None:
        return float(row["zone_dist_pct"])
    ref, edge = row.get("ref_price"), row.get("zone_edge")
    if ref is None or edge is None or float(ref) <= 0:
        return None
    ref_f, edge_f = float(ref), float(edge)
    d = (ref_f - edge_f) if str(row.get("option_type")).upper() == "PE" else (edge_f - ref_f)
    return round(d / ref_f * 100.0, 4)


def rel_word(option_type: str, gap_pct: float) -> str:
    """Where the stock is relative to the zone edge. The gap is signed as in gates.zone_gap_pct:
    PE + = above the support top, CE + = below the resistance bottom."""
    pe = str(option_type).upper() == "PE"
    return ("above" if gap_pct >= 0 else "below") if pe else ("below" if gap_pct >= 0 else "above")


def zone_word(option_type: str) -> str:
    return "support" if str(option_type).upper() == "PE" else "resistance"


def _num(v: Any) -> float | None:
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _plural(n: int, word: str) -> str:
    return f"{n} {word}{'' if n == 1 else 's'}"


# ------------------------------------------------------------------------------------------ measure + split
class _Measure(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.stack: list[str] = []
        self.entities = 0
        self.units = 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if not (tag == "code" and self.stack and self.stack[-1] == "pre"):
            self.entities += 1
        self.stack.append(tag)

    def handle_endtag(self, tag: str) -> None:
        if self.stack and self.stack[-1] == tag:
            self.stack.pop()

    def handle_data(self, data: str) -> None:
        self.units += len(data.encode("utf-16-le")) // 2


def tg_measure(text: str) -> tuple[int, int]:
    """(visible length in UTF-16 units after entity parsing, formatting entity count) of a Telegram HTML text.
    Every opening tag is one entity (<b>, <i>, <u>, <blockquote> / <blockquote expandable>, ...), except a <code>
    directly inside a <pre>, which Telegram merges into the pre entity."""
    p = _Measure()
    p.feed(text)
    p.close()
    return p.units, p.entities


def _fits(text: str) -> bool:
    units, entities = tg_measure(text)
    return units <= PART_MAX_CHARS and entities <= PART_MAX_ENTITIES


def _part_tag(i: int, n: int) -> str:
    return f"{DOT}part {i}/{n}" if n > 1 else ""


def split_by_length(blocks: Sequence[Any], render: Render) -> list[str]:
    """Greedy: each part takes whole blocks while render(part, 99, 99) (the longest part tag) still fits
    PART_MAX_CHARS / PART_MAX_ENTITIES; then every part is rendered with its real 'part i/n'.
    A block is never split; a block that alone exceeds the budget still gets its own part."""
    chunks: list[list[Any]] = []
    cur: list[Any] = []
    for b in blocks:
        trial = [*cur, b]
        if cur and not _fits(render(trial, 99, 99)):
            chunks.append(cur)
            cur = [b]
        else:
            cur = trial
    if cur or not chunks:
        chunks.append(cur)
    n = len(chunks)
    return [render(c, i, n) for i, c in enumerate(chunks, 1)]


# ------------------------------------------------------------------------------------------ price ladder
def price_ladder(levels: Sequence[tuple[float, str]], mark: str) -> str:
    """Plain-text price ladder for a <pre>: highest first, '>' on the `mark` line, <= PRE_MAX_COLS columns.
    Equal prices share a line ('PREMIUM = mid') only when the joined line still fits the width."""
    top = max((abs(p) for p, _ in levels), default=0.0)
    dec = 2 if top < 1000 else 1 if top < 10000 else 0
    ordered = sorted(levels, key=lambda t: (-round(float(t[0]), dec), t[1] != mark))
    width = max(len(f"{float(p):.{dec}f}") for p, _ in levels)

    def line(p: float, labels: list[str]) -> str:
        return f"{'>' if mark in labels else ' '} {float(p):>{width}.{dec}f}  {' = '.join(labels)}"

    rows: list[tuple[float, list[str]]] = []
    for p, label in ordered:
        if rows and f"{rows[-1][0]:.{dec}f}" == f"{float(p):.{dec}f}":
            merged = [*rows[-1][1], label]
            if len(line(p, merged)) <= PRE_MAX_COLS:
                rows[-1] = (rows[-1][0], merged)
                continue
        rows.append((float(p), [label]))
    return "\n".join(line(p, labels) for p, labels in rows)


# ------------------------------------------------------------------------------------------ E1 / E2 ticket (D)
def e1_path(kind: str, reason: str | None) -> str:
    """'breakout' / 'shakeout' for an E1 (from the engine's reason text), '' otherwise."""
    if kind != "E1":
        return ""
    r = str(reason or "").lower()
    return "shakeout" if "shakeout" in r else "breakout" if "breakout" in r else ""


def event_title(kind: str, path: str, strong: bool) -> str:
    name = f"E2{DOT}OI UNWIND" if kind == "E2" else f"E1{DOT}{path.upper()}" if path else esc(kind)
    star = f" {ICON_STRONG} STRONG" if strong else ""
    return f"{KIND_ICON.get(kind, ICON_E2)} <b>{name}{star}</b>"


def _contract_text(symbol: Any, strike: Any, option_type: Any) -> str:
    return f"{esc(symbol)} {plain_num(strike or 0)} {esc(option_type or '')}"


def _hold(meta: ContractMeta, ctx: SignalContext | None) -> str:
    return ctx.hold if ctx is not None else hold_for(meta.list_name, HoldCfg())


# ------------------------------------------------------------------------------------------ entry card: C "Sheet"
ICON_COPY = "\N{CLIPBOARD}"
BULLET = "\N{BULLET}"
WJ = "\N{WORD JOINER}"  # between a sign and the rupee: a phone never breaks "+₹95" apart
DISCLAIMER = "<i>Alert only, not advice. Size to your own risk.</i>"
COPY_ORDER_LABEL = f"{ICON_COPY} Copy order"
COPY_TEXT_MAX = 256  # Bot API copy_text: 1-256 characters
# The "max" on the plan: +5% over the alert premium, rounded DOWN to the tick (a placeholder rule, PREVIEWS open
# call 2; DOWN so it never reads above +5%; omitted when the tick leaves no room above the entry).
CHASE_PCT = 0.05
QUADRANT_TEXT = {  # D94(2): OWNER edition only; FLAT / None -> no line
    "SC": "Short covering (OI down, premium up)",
    "LU": "Long unwinding (OI down, premium down)",
    "LB": "Long buildup (OI up, premium up)",
    "SB": "Short buildup (OI up, premium down)",
}
GATE_ORDER = GATE_NAMES  # zone, liquidity, direction, unwind (gates.GATE_NAMES, S1.4.0)


def _edition(edition: str | None) -> str:
    """The edition a template renders for: the argument, else gammad.edition.edition(). Anything but an explicit
    "owner" is the client (fail closed, as gammad.edition does)."""
    value = edition_mod.edition() if edition is None else edition
    return edition_mod.OWNER if str(value).strip().lower() == edition_mod.OWNER else edition_mod.CLIENT


def _is_client(edition: str | None) -> bool:
    return _edition(edition) == edition_mod.CLIENT


def _tick(meta: ContractMeta) -> float | None:
    tick = _num(getattr(meta, "tick_size", None))  # S1.3.0 adds ContractMeta.tick_size
    return tick if tick and tick > 0 else None


def _pct(x: float, dp: int = 0) -> str:
    v = round(float(x), dp)
    return f"{'+' if v > 0 else MINUS if v < 0 else ''}{abs(v):.{dp}f}%"


def _inr(n: float, *, sign: bool = False) -> str:
    """₹473 / +⁠₹95 / −⁠₹79 (whole rupees, half-up, Indian grouping). The float is cleaned to 6 decimals first, so
    0.30 x 1575 (= 472.4999...) is ₹473, as the arithmetic reads."""
    d = Decimal(repr(round(float(n), 6))).quantize(Decimal(1), rounding=ROUND_HALF_UP)
    v = int(d)
    s = ("+" if v > 0 else MINUS if v < 0 else "") if sign else ""
    return f"{s}{WJ if s else ''}{RUPEE}{indian_int(abs(v))}"


def level_text(x: float) -> str:
    """A zone edge / stock level on a C card: 306.8 / 1,158.3 / 46,673 (one decimal below 10,000)."""
    v = float(x)
    return f"{v:.1f}" if abs(v) < 1000 else price_text(v)


def edge_text(x: float) -> str:
    """A zone edge on a v3 signal card (stock stop, WHY, CHECKS, the D68 note): 263.05 / 306.8 / 1,537.7 / 46,673
    (two decimals below 1,000 with trailing zeros dropped; price_text from 1,000)."""
    v = float(x)
    if abs(v) >= 1000:
        return price_text(v)
    return f"{v:.2f}".rstrip("0").rstrip(".")


def _p2(x: float) -> str:
    return f"{float(x):.2f}"


def _floor_tick(x: float, tick: float | None) -> float:
    t = Decimal(repr(float(tick or MIN_TICK)))
    return float((Decimal(repr(float(x))) / t).to_integral_value(rounding=ROUND_FLOOR) * t)


@dataclass(frozen=True, slots=True)
class Plan:
    """The levels an entry card, its copy-order text and the trade tracker share (TRACKER §2; D80). T1/T2 are
    gammad.trade.rules.target_price (rounded UP to the contract tick), so the card shows the tracker's levels."""

    hold: str
    entry: float
    cap: float | None  # "max": +CHASE_PCT rounded DOWN to the tick; None when that is not above the entry
    sl: float | None  # intraday: the base low (signal.sl); swing: None (the D68 stock stop guards it)
    edge: float | None  # the D68 zone edge (the stock stop), both holds (T-162 T2: the card shows it always)
    t1: float
    t2: float
    lot: int | None

    @property
    def per_lot(self) -> float | None:
        return None if self.lot is None else self.entry * self.lot

    @property
    def risk_lot(self) -> float | None:
        if self.sl is None or self.lot is None:
            return None
        return max(self.entry - self.sl, 0.0) * self.lot


def entry_plan(signal: Signal, meta: ContractMeta, ctx: SignalContext | None = None) -> Plan:
    """The plan of an E1 / E2 entry at signal.ltp (TRACKER §2)."""
    hold = _hold(meta, ctx)
    tick = _tick(meta)
    entry = float(signal.ltp)
    cap = _floor_tick(entry * (1 + CHASE_PCT), tick) if entry > 0 else None
    edge = next((v for v in (ctx and ctx.invalidation, ctx and ctx.zone_edge, meta.zone_edge) if v), None)
    lot = meta.lot_size or (ctx.lot_size if ctx is not None else None)
    swing = hold == "swing"
    return Plan(
        hold=hold,
        entry=entry,
        cap=cap if cap is not None and cap > entry + 1e-9 else None,
        sl=None if swing else float(signal.sl),
        edge=edge,
        t1=target_price(entry, T1_PCT, tick) if entry > 0 else 0.0,
        t2=target_price(entry, T2_PCT, tick) if entry > 0 else 0.0,
        lot=int(lot) if lot else None,
    )


def _up(entry: float, level: float) -> float:
    return (level / entry - 1.0) * 100.0 if entry > 0 else 0.0


def at_entry_context(
    meta: ContractMeta, signal: Signal, ctx: SignalContext | None, cal: TradingCalendar | None
) -> SignalContext | None:
    """The context a CARD renders with: ctx with its hold replaced by the D125 F7 hold at entry, exactly as the
    tracker opens the trade (gammad.trade.rules.hold_at_entry over rules.trading_dte from the signal's IST date to the
    expiry): DTE <= 1 -> "intraday" (the option SL shows), else ctx.hold. ctx None, no calendar or an unreadable
    expiry -> ctx unchanged (the card falls back to the configured hold)."""
    if ctx is None or cal is None:
        return ctx
    try:
        expiry = date.fromisoformat(str(meta.expiry_date))
    except ValueError:
        return ctx
    day = datetime.fromtimestamp(signal.ts_ms / 1000, IST).date()
    hold = hold_at_entry(ctx.hold, trading_dte(cal, day, expiry))
    return ctx if hold == ctx.hold else replace(ctx, hold=hold)


def side_mark(option_type: str | None) -> str:
    """▼ a put, ▲ a call (T-212, D195(5); the v3 side emoji 🔻 / 🔺 until 0.8.2)."""
    return E_PUT if str(option_type or "").upper() == "PE" else E_CALL


def contract_head(symbol: Any, strike: Any, option_type: Any, expiry_date: Any, ref: Any) -> str:
    """T-212 style B headline line 2: "▼ <b>POWERGRID 265 PE</b> · 📅 <b>29 Sep</b> 🟣" (🟣 = expiry within 5 trading days of
    `ref`, the message's own day; every contract shows its own expiry, D182(1))."""
    return (f"{side_mark(option_type)} <b>{_contract_text(symbol, strike, option_type)}</b>{DOT}{E_EXPIRY} "
            f"<b>{disp_day(str(expiry_date or ''))}</b>{expiry_mark(expiry_date, ref)}")  # fmt: skip


def badge_line(icon: str, word: str, badge: str = "") -> str:
    """T-212 style B headline line 1: icon + bold word, then the marker / 4-dot badge after BADGE_GAP."""
    return f"{icon} <b>{word}</b>" + (f"{BADGE_GAP}{badge}" if badge else "")


def sign_marker(pct: float | None) -> str:
    """🟢 a gain, 🔴 a loss, '' at zero / unknown (one meaning each, D195(5))."""
    if pct is None or round(float(pct), 1) == 0:
        return ""
    return M_PASS if pct > 0 else M_FAIL


def section(title: str) -> str:
    """A v3 section header: <u><b>TITLE</b></u>."""
    return f"<u><b>{title}</b></u>"


def headline(icon: str, word: str, symbol: Any, strike: Any, option_type: Any) -> str:
    """T4 line 1: tier emoji + bold tier word + side emoji + bold contract."""
    return f"{icon} <b>{word}</b>{DOT}{side_mark(option_type)} <b>{_contract_text(symbol, strike, option_type)}</b>"


def when_line(expiry_date: Any, when: str | None, *extra: str) -> str:
    """T4 line 2: 📅 expiry · 🕒 time IST (then any `extra` parts)."""
    parts = [f"{E_EXPIRY} {day_short(str(expiry_date or ''))} expiry"]
    if when:
        parts.append(f"{E_TIME} {when} IST")
    return DOT.join([*parts, *(x for x in extra if x)])


def stock_stop_line(edge: float | None, option_type: str) -> str:
    """T2 (D129): the D68 stock stop, always on an ENTRY card: "🧱 Stock stop 263.05 (support edge)"."""
    zone = zone_word(option_type)
    if not edge:
        return f"{E_EDGE} Stock stop at the {zone} edge"
    return f"{E_EDGE} Stock stop <b>{edge_text(edge)}</b> ({zone} edge)"


def _plan_lines(p: Plan, option_type: str, *, expiry_cap: bool) -> list[str]:
    """PLAN: entry + max, the option SL (intraday hold only) and the stock stop (always, T2), T1, T2, the lot with
    money and risk; a swing hold adds the D68 note."""
    q = [f"{E_PRICE} Entry <b>{rupees(p.entry)}</b>" + (f"{DOT}max {rupees(p.cap)}" if p.cap is not None else "")]
    if p.hold != "swing" and p.sl is not None:
        q.append(f"{E_SL} SL <b>{rupees(p.sl)}</b>{DOT}"
                 + ("= entry" if p.sl >= p.entry else _pct(_up(p.entry, p.sl), 1)))  # fmt: skip
    q.append(stock_stop_line(p.edge, option_type))
    q.append(f"{E_T1} T1 <b>{rupees(p.t1)}</b>{DOT}{_pct(_up(p.entry, p.t1), 1)}")
    q.append(f"{E_T2} T2 <b>{rupees(p.t2)}</b>{DOT}{_pct(_up(p.entry, p.t2), 1)}")
    if p.per_lot is not None:
        lot = [f"{E_LOT} Lot {indian_int(p.lot or 0)}", _inr(p.per_lot)]
        if p.risk_lot is not None:
            lot.append(f"risk {_inr(p.risk_lot)}")
        q.append(DOT.join(lot))
    if p.hold == "swing":
        q.append(_swing_note(p, option_type, expiry_cap=expiry_cap))
    return q


def _plan_sections(p: Plan, option_type: str) -> list[str]:
    """T-212 style B of the untiered / E2 card's plan: PLAN (entry + max, T1, T2), STOPS (the option SL on an intraday
    hold only, the stock stop always, the D68 note on a swing hold), SIZE (lot, money, risk)."""
    out = [section("PLAN"),
           f"{E_PRICE} Entry <b>{rupees(p.entry)}</b>" + (f"{DOT}max {rupees(p.cap)}" if p.cap is not None else ""),
           f"{E_T1} T1 <b>{rupees(p.t1)}</b>{DOT}{_pct(_up(p.entry, p.t1), 1)}",
           f"{E_T2} T2 <b>{rupees(p.t2)}</b>{DOT}{_pct(_up(p.entry, p.t2), 1)}", section("STOPS")]  # fmt: skip
    if p.hold != "swing" and p.sl is not None:
        out.append(f"{E_SL} SL <b>{rupees(p.sl)}</b>{DOT}"
                   + ("= entry" if p.sl >= p.entry else _pct(_up(p.entry, p.sl), 1)))  # fmt: skip
    out.append(stock_stop_line(p.edge, option_type))
    if p.hold == "swing":
        out.append(_swing_note(p, option_type))
    if p.per_lot is not None:
        lot = [f"{E_LOT} Lot {indian_int(p.lot or 0)}", _inr(p.per_lot)]
        if p.risk_lot is not None:
            lot.append(f"risk {_inr(p.risk_lot)}")
        out += ["", section("SIZE"), DOT.join(lot)]
    return out


def _swing_note(p: Plan, option_type: str, *, expiry_cap: bool = False) -> str:
    """D68 ("armed after the break"): the stop ARMS on a daily close THROUGH the edge (PE: below the support top,
    CE: above the resistance bottom); once armed, a daily close back over the edge exits; never armed = day 5.
    `expiry_cap` (W15 tiered E1 cards): every list is swing, so an EXPIRY-list trade can reach its expiry before
    day 5; the tracker then exits at 15:20 on the expiry day (TRADE-TRACKER-SPEC §11), and the note says so."""
    arm, back = ("below", "above") if option_type.upper() == "PE" else ("above", "below")
    where = edge_text(p.edge) if p.edge else f"the {zone_word(option_type)} edge"
    cap = ", or at 15:20 on the expiry day if that comes first" if expiry_cap else ""
    return (
        f"<i>Stock stop arms on a daily close {arm} {where}, then exits on a close back {back}; "
        f"never armed = exit at the day-5 close{cap}.</i>"
    )


def _why_zone(meta: ContractMeta, ctx: SignalContext | None) -> str | None:
    """The LIVE spot against the zone edge when ctx has a spot ("Spot at support <b>263.05</b>" when the gap rounds
    to 0.0 %, else "Spot 0.2% below resistance <b>1,537.7</b>"); else the list-build distance (degraded)."""
    ot = meta.option_type
    zone = zone_word(ot)
    edge = (ctx.zone_edge if ctx is not None and ctx.zone_edge else None) or meta.zone_edge
    if ctx is not None and ctx.spot:
        gap = ctx.zone_gap_pct
        if gap is None and edge:
            gap = ((ctx.spot - edge) if ot.upper() == "PE" else (edge - ctx.spot)) / ctx.spot * 100.0
        if gap is not None and edge:
            if f"{abs(gap):.1f}" == "0.0":
                return f"Spot at {zone} <b>{edge_text(edge)}</b>"
            return f"Spot {abs(gap):.1f}% {rel_word(ot, gap)} {zone} <b>{edge_text(edge)}</b>"
    z = meta.zone_dist_pct
    if z is None:
        return None
    where = f" <b>{edge_text(edge)}</b>" if edge else " edge"
    return f"Spot {abs(z):.1f}% {rel_word(ot, z)} {zone}{where} at the list build"


def peak_line(signal: Signal, ctx: SignalContext | None) -> str | None:
    """D125 F9 (T-161), the OWNER E2 card's line under the reason: "peak HH:MM · premium +y% since peak = short
    covering" ("−y% ... = long unwinding"; "premium flat since peak" at 0.0%), from signal.snapshot
    (oi_peak_bar_ts_ms = the OI-peak bar's open, IST; oi_peak_bar_close; signal_price, else signal.ltp). With
    ctx.build_oi (the OI at the list build; H7 adds the field) and an OI peak, "peak HH:MM (+x% over build)".
    None when the snapshot lacks the peak bar (older rows, a peak not found)."""
    snap = signal.snapshot or {}
    peak_ms, peak_close = _num(snap.get("oi_peak_bar_ts_ms")), _num(snap.get("oi_peak_bar_close"))
    price = _num(snap.get("signal_price"))
    price = price if price is not None else _num(signal.ltp)
    if peak_ms is None or peak_close is None or not peak_close > 0 or price is None:
        return None
    head = f"peak {ist_hm(int(peak_ms))}"
    build = _num(getattr(ctx, "build_oi", None))
    oi_peak = _num(getattr(ctx, "oi_peak", None))
    if oi_peak is None:
        oi_peak = _num(snap.get("oi_peak"))
    if build is not None and build > 0 and oi_peak is not None:
        head += f" ({_pct((oi_peak / build - 1.0) * 100.0, 1)} over build)"
    y = round((price / peak_close - 1.0) * 100.0, 1)
    if y == 0:
        return f"{head}{DOT}premium flat since peak"
    return f"{head}{DOT}premium {_pct(y, 1)} since peak = {'short covering' if y > 0 else 'long unwinding'}"


def _strong_word(signal: Signal) -> str:
    return f"{DOT}STRONG drop" if signal.strong else ""


def _why_quote(signal: Signal, meta: ContractMeta, ctx: SignalContext | None, *, all_checks: bool) -> str:
    """WHY IT FIRED: one quote with the key numbers in bold, ending "✅ All 4 checks passed" on an ENTRY."""
    lines = [
        f"{E_OI} OI <b>{pct_drop(signal.oi_drop_pct)}</b> from its peak{_strong_word(signal)}",
        f"{E_VOL} Volume <b>{signal.vol_ratio:.1f}{TIMES}</b> the 20-bar average",
    ]
    zone = _why_zone(meta, ctx)
    if zone:
        lines.append(f"{E_EDGE} {zone}")
    if all_checks:
        lines.append(f"{E_PASS} All {len(GATE_NAMES)} checks passed")
    return "<blockquote>" + "\n".join(lines) + "</blockquote>"


CHECK_LABEL = {"zone": "Zone", "liquidity": "Liquidity", "direction": "Direction", "unwind": "Unwind"}


def _check_text(gate: str, ok: bool, meta: ContractMeta, ctx: SignalContext | None, *, subject: str = "spot") -> str:
    """The plain words of one gate verdict (STRATEGY-SPEC 4.2 / gammad.alerts.gates). `subject` names the underlying
    ("spot" on the v3 CHECKS list, "stock" on the T-178 blend card)."""
    ot = meta.option_type
    zone = zone_word(ot)
    if gate == "zone":
        edge = (ctx.zone_edge if ctx is not None and ctx.zone_edge else None) or meta.zone_edge
        where = f"{zone} {edge_text(edge)}" if edge else f"the {zone} edge"
        # S1.5.0: the limit the context was gated with (V1's dynamic clamp); none recorded = the fixed S1.4.0 rule
        lim = getattr(ctx, "zone_limit_pct", None) if ctx is not None else None
        limit = plain_num(round(float(lim), 2) if lim is not None else GatesCfg().zone_max_gap_pct)
        return f"{subject} within {limit}% of {where}" if ok else f"{subject} more than {limit}% from {where}"
    if gate == "liquidity":
        return "enough lots traded per bar" if ok else "too few lots traded per bar"
    if gate == "direction":
        want = "below VWAP and falling" if ot.upper() == "PE" else "above VWAP and rising"
        return f"{subject} {want}" if ok else f"{subject} not yet {want}"
    if gate == "unwind":
        return "premium up since OI peaked" if ok else "premium not up since OI peaked"
    return esc(gate)


def heads_up_lines(warns: Sequence[str]) -> list[str]:
    """HEADS-UP: each `warnings()` line with its emoji (⏳ expiry, 🌇 day closing) and the lead words bold."""
    out = []
    for w in warns:
        lead, sep, rest = str(w).partition(f" {EM_DASH} ")
        icon = E_LATE if lead.startswith("Day is closing") else E_DTE
        out.append(f"{icon} <b>{esc(lead)}</b>{sep}{esc(rest)}")
    return out


def quadrant_line(ctx: SignalContext | None) -> str | None:
    """D94(2) OI-price quadrant label ("SC" / "LU" / "LB" / "SB"); FLAT, None or unknown -> None. OWNER only."""
    q = str(getattr(ctx, "quadrant", None) or "").upper()
    return QUADRANT_TEXT.get(q)


CARRIED_REASON = "carried"  # = gammad.alerts.router.CARRIED: the carried token in gate_reasons (not a gate)


def gate_line(ctx: SignalContext | None, tier: str | None = None) -> str:
    """OWNER DESK: the D63 / S1.3.0 group-gate verdict. Failed gates in GATE_ORDER (zone, liquidity, direction,
    unwind), then any other name; gates that could not be evaluated are named as not checked (fail-open).
    `tier` given (W15, a tiered E1): the headline carries the verdict and a STRONG WATCHLIST card IS the group
    post, so a failure reads "Gate failed: x" with no "not sent to the group" claim."""
    if ctx is None:
        return "Gates not checked"

    def ordered(names: Iterable[str]) -> list[str]:
        uniq = list(dict.fromkeys(str(n) for n in names))
        return sorted(uniq, key=lambda n: (GATE_ORDER.index(n) if n in GATE_ORDER else len(GATE_ORDER), n))

    failed, unknown = ordered(ctx.gate_reasons), ordered(ctx.gate_unknown)
    note = ""
    if tier == "ENTRY" and CARRIED_REASON in failed:
        # S1.9.0 (D380 (6)): an OWNER carried ENTRY (the only carried row that reaches ENTRY): `carried` is the row's
        # tag, not a failed gate. A capped row (tier below ENTRY: V1 B, a shadow, a client or pre-S1.9.0 row) keeps
        # "Gate failed: carried" -- there the cap is what held it back
        failed = [g for g in failed if g != CARRIED_REASON]
        note = f"{DOT}carried contract"
    if failed and tier is not None:
        head = f"Gate{'s' if len(failed) > 1 else ''} failed: {esc(', '.join(failed))}"
    elif failed:
        head = f"GATED: {esc(', '.join(failed))}{DOT}not sent to the group"
    else:
        facts = []
        if ctx.zone_gap_pct is not None:
            facts.append(f"zone {abs(ctx.zone_gap_pct):.1f}%")
        if ctx.avg_vol_lots is not None:
            facts.append(f"{ctx.avg_vol_lots:.1f} lots/bar")
        head = DOT.join(["Gates pass", *facts])
    return head + note + (f"{DOT}not checked: {esc(', '.join(unknown))}" if unknown else "")


def _desk_lines(
    meta: ContractMeta, signal: Signal, ctx: SignalContext | None, when: str, e2: Signal | None,
    tier: str | None = None,
) -> list[str]:  # fmt: skip
    """OWNER only (D79, D87): quadrant (D94), OI peak -> now + spot, the E1 trigger / E2 link, list · tag · time,
    the gate verdict."""
    out: list[str] = []
    quad = quadrant_line(ctx)
    if quad:
        out.append(quad)
    oi = []
    if ctx is not None and ctx.oi_peak is not None and ctx.oi_now is not None:
        oi.append(f"OI {oi_short(ctx.oi_peak)} {ARROW} {oi_short(ctx.oi_now)}")
    if ctx is not None and ctx.spot:
        oi.append(f"spot {price_text(ctx.spot)}")
    if oi:
        out.append(DOT.join(oi))
    if signal.kind == "E2":
        out.append(f"E1 on a break above {rupees(signal.consol_hi)}")
    elif e2 is not None:
        out.append(f"After E2 {ist_hm(e2.ts_ms)} @ {rupees(e2.ltp)}")
    else:
        path = e1_path(signal.kind, signal.reason)
        rng = f"range {rupees(signal.consol_lo)}{EN_DASH}{rupees(signal.consol_hi)}"
        out.append(f"E1 {path}{DOT}{rng}" if path else rng)
    out.append(DOT.join([*(esc(x) for x in (meta.list_name, meta.tag) if x), when]))
    out.append(gate_line(ctx, tier))
    return out


def entry_title(kind: str, edition: str | None = None) -> str:
    """OWNER: E2 "POSSIBLE ENTRY", E1 "BEST ENTRY"; CLIENT: "ENTRY" (an E1; nothing refers to E2)."""
    if _is_client(edition):
        return "ENTRY"
    return "POSSIBLE ENTRY" if kind == "E2" else "BEST ENTRY"


# ------------------------------------------------------------------------------------------ T-178 "L1 + C2 blend"
def _expiry_upper(expiry_date: Any) -> str:
    """'29 SEP' (the copy line's expiry: day + upper-case month); an unreadable date is escaped as given."""
    try:
        d = expiry_date if isinstance(expiry_date, date) else date.fromisoformat(str(expiry_date))
    except ValueError:
        return esc(str(expiry_date or "").upper())
    return f"{d.day} {d:%b}".upper()


def copy_line(meta: ContractMeta) -> str:
    """D555: tap to copy only the contract, in the broker search's token order."""
    name = f"{esc(meta.symbol)} {_expiry_upper(meta.expiry_date)}"
    return f"{E_COPY} <code>{name} {plain_num(meta.strike or 0)} {esc(meta.option_type or '')}</code>"


def _pct_short(x: float) -> str:
    """+20% / −12.2% / 0%: one decimal with a trailing .0 dropped (the blend's plan and result percentages)."""
    v = round(float(x), 1)
    body = f"{abs(v):.1f}".removesuffix(".0")
    return f"{'+' if v > 0 else MINUS if v < 0 else ''}{body}%"


def _quote(lines: Iterable[str]) -> str:
    return "<blockquote>" + "\n".join(lines) + "</blockquote>"


def _box_head(meta: ContractMeta, when: str) -> str:
    """The box's first line: "🔻 <b>BDL 1100 PE</b> · 📅 29 Sep · 🕒 12:31"."""
    contract = _contract_text(meta.symbol, meta.strike, meta.option_type)
    return (f"{side_mark(meta.option_type)} <b>{contract}</b>{DOT}{E_EXPIRY} {day_short(str(meta.expiry_date or ''))}"
            f"{DOT}{E_TIME} {when}")  # fmt: skip


def heads_up_sentences(warns: Sequence[str]) -> list[str]:
    """The blend's heads-up lines: each `warnings()` line as a sentence, lead words bold ("⏳ <b>Expires today.</b> Plan
    accordingly."; ⏳ expiry, 🌇 day closing). The warning TEXT is `warnings()`'s, only its case and stops change."""
    out = []
    for w in warns:
        lead, _, rest = str(w).partition(f" {EM_DASH} ")
        icon = E_LATE if lead.startswith("Day is closing") else E_DTE
        lead = (lead.capitalize() if lead.isupper() else lead).rstrip(".")
        line = f"{icon} <b>{esc(lead)}.</b>"
        rest = rest.strip().rstrip(".")
        if rest:
            line += f" {esc(rest[:1].upper() + rest[1:])}."
        out.append(line)
    return out


def _check_fail_numbers(gate: str, meta: ContractMeta, ctx: SignalContext | None) -> str | None:
    """T-282 (b), owner ruling 5 Oct: a failed check's words WITH the numbers its context stored, or None when it
    stored none (the caller then keeps `_check_text`'s words). Zone = the stock's gap to the zone edge (ctx.zone_gap_pct,
    signed as gates.zone_gap_pct) and the limit it was checked against (ctx.zone_limit_pct, printed only when the context
    has it: both editions' cards and STORED rows carry it since T-305 (D278(6): router.CLIENT_STORE_CTX_KEEP), so the
    client WHY NOT prints the card's words; a pre-0.8.5 client row has none and prints the gap alone; the client's WS /
    API ctx never carries the raw key, router.CLIENT_WIRE_CTX_DROP); liquidity = ctx.avg_vol_lots and ctx.zero_vol_bars. Direction
    (f1 / f2: owner-only keys) and unwind (the cycle_ / oi_peak_bar_ snapshot prices: never in a client payload, D98(2))
    have no client-safe number: None. Nothing is recomputed here."""
    if ctx is None:
        return None
    if gate == "zone" and ctx.zone_gap_pct is not None:
        ot = meta.option_type
        edge = ctx.zone_edge or meta.zone_edge
        zone = zone_word(ot)
        where = f"{zone} {edge_text(edge)}" if edge else zone
        gap = float(ctx.zone_gap_pct)
        lim = ctx.zone_limit_pct
        limit = f" (limit {plain_num(round(float(lim), 2))}%)" if lim is not None else ""
        return f"stock {abs(gap):.1f}% {rel_word(ot, gap)} {where}{limit}"
    if gate == "liquidity" and (ctx.avg_vol_lots is not None or ctx.zero_vol_bars is not None):
        parts = []
        if ctx.avg_vol_lots is not None:
            parts.append(f"{float(ctx.avg_vol_lots):.1f} lots on average")
        if ctx.zero_vol_bars is not None:
            parts.append(f"{_plural(int(ctx.zero_vol_bars), 'bar')} with no trades")
        return f"{_check_text(gate, False, meta, ctx)} ({', '.join(parts)})"
    return None


def check_lines(meta: ContractMeta, ctx: SignalContext | None) -> list[str]:
    """T-212 style B CHECKS: one line per gate in GATE_NAMES order, "🟢 Zone" when passed, "🔴 <b>Direction</b>: stock
    not yet below VWAP and falling" when failed, "🔴 <b>X</b>: not checked (no data)" when it had no input.
    T-282 (b): a failed zone / liquidity line carries its stored numbers (`_check_fail_numbers`): "🔴 <b>Zone</b>: stock
    3.9% above support 263.05 (limit 1.6%)", "🔴 <b>Liquidity</b>: too few lots traded per bar (0.8 lots on average, 15
    bars with no trades)"."""
    failed = set(ctx.gate_reasons) if ctx is not None else set()
    unknown = set(ctx.gate_unknown) if ctx is not None else set(GATE_NAMES)
    out = []
    for gate in GATE_NAMES:
        name = CHECK_LABEL.get(gate, esc(gate))
        if gate in failed:
            why = _check_fail_numbers(gate, meta, ctx) or _check_text(gate, False, meta, ctx, subject="stock")
            out.append(f"{M_FAIL} <b>{name}</b>: {why}")
        elif gate in unknown:
            out.append(f"{M_FAIL} <b>{name}</b>: not checked (no data)")
        else:
            out.append(f"{M_PASS} {name}")
    return out


def check_badge(ctx: SignalContext | None, *, all_passed: bool = False) -> str:
    """T-212 (D195(5)): the 4-dot badge in GATE_NAMES order (Zone · Liquidity · Direction · Unwind), 🟢 passed, 🔴 failed
    or not checked; `all_passed` (an ENTRY: the tier IS four passes) = 🟢🟢🟢🟢."""
    if all_passed:
        return M_PASS * len(GATE_NAMES)
    bad = set(ctx.gate_reasons) | set(ctx.gate_unknown) if ctx is not None else set(GATE_NAMES)
    return "".join(M_FAIL if g in bad else M_PASS for g in GATE_NAMES)


def check_verdicts(meta: ContractMeta, ctx: SignalContext | None) -> tuple[list[str], list[str]]:
    """The blend's checks: (the passed gates' names, one line per failed gate "❌ <b>Direction</b>: stock not yet below
    VWAP and falling"), in GATE_NAMES order. A gate with no input is not a pass (gates.passed): "not checked (no
    data)"."""
    failed = set(ctx.gate_reasons) if ctx is not None else set()
    unknown = set(ctx.gate_unknown) if ctx is not None else set(GATE_NAMES)
    passed: list[str] = []
    fails: list[str] = []
    for gate in GATE_NAMES:
        name = CHECK_LABEL.get(gate, esc(gate))
        if gate in failed:
            fails.append(f"{E_FAIL} <b>{name}</b>: {_check_text(gate, False, meta, ctx, subject='stock')}")
        elif gate in unknown:
            fails.append(f"{E_FAIL} <b>{name}</b>: not checked (no data)")
        else:
            passed.append(name)
    return passed, fails


def _why_zone_plain(meta: ContractMeta, ctx: SignalContext | None) -> str | None:
    """The blend's "Why" zone part: "stock 0.1% above support" / "stock at support" (live spot), else the list-build
    distance ("stock 0.3% above support at the list build"); None when neither is known."""
    ot = meta.option_type
    zone = zone_word(ot)
    edge = (ctx.zone_edge if ctx is not None and ctx.zone_edge else None) or meta.zone_edge
    if ctx is not None and ctx.spot:
        gap = ctx.zone_gap_pct
        if gap is None and edge:
            gap = ((ctx.spot - edge) if ot.upper() == "PE" else (edge - ctx.spot)) / ctx.spot * 100.0
        if gap is not None:
            if f"{abs(gap):.1f}" == "0.0":
                return f"stock at {zone}"
            return f"stock {abs(gap):.1f}% {rel_word(ot, gap)} {zone}"
    z = meta.zone_dist_pct
    if z is None:
        return None
    return f"stock {abs(z):.1f}% {rel_word(ot, z)} {zone} at the list build"


def carried_line(meta: ContractMeta, *, client: bool | None = None) -> str:
    """0.8.1 (D157(2), D160(3)): the OWNER card line of a signal on a CARRY row: "🔄 carried · k 3 · PRIMED · never
    an entry" (k / state when known; D273 dropped "watch only", now a tier name).
    S1.9.0 (D380 (6) "Lift the cap now (owner edition)", D382): in the OWNER edition a carried contract may be an ENTRY
    (personal chat, tracked), so the owner's line is "🔄 carried · k 3 · PRIMED" -- the tier headline says whether this
    one is an entry; "never an entry" stays the CLIENT edition's (where the cap holds; a client never renders a carried
    card: `_sheet` raises OwnerOnlyError). `client` None = the running edition. 0.8.9 T-419 (D437): no CLIENT card
    calls this any more (a client's carried card is the Still Watching card / `sw_list_line`); the client form is kept
    for the direct call only."""
    parts = ["carried"]
    k = getattr(meta, "carry_k", None)
    if k is not None:
        parts.append(f"k {int(k)}")
    state = getattr(meta, "carry_state", None)
    if state:
        parts.append(esc(state))
    if edition_mod.is_client() if client is None else client:
        parts.append("never an entry")  # D273: "watch only" is the WATCH ONLY tier's name now (a capped card is NEAR ENTRY)
    return f"{E_CARRY} " + DOT.join(parts)


HEADLINE_SCAN = 6  # D555: scanner (owner personal), title, list, contract, expiry, then the clock


def under_headline(card: str, line: str) -> str:
    """T-212 (catalogue §6 trap): insert `line` right under a rendered card's HEADLINE BLOCK -- after the style B
    "🕒 time" line within the first HEADLINE_SCAN lines (a two-line headline must never be split), else after line 1
    (a one-line headline). The last line inserted sits first under the headline. Idempotent (an equal line present =
    unchanged)."""
    lines = card.split("\n")
    if line in lines:
        return card
    at = next((i + 1 for i, ln in enumerate(lines[:HEADLINE_SCAN]) if ln.startswith(f"{E_TIME} ")), 1)
    return "\n".join([*lines[:at], line, *lines[at:]])


def add_also_plus(card: str) -> str:
    """D161(1): the `also V1 PLUS` line inserted right under a rendered card's headline (the owner's PERSONAL copy of
    a shared V1 ENTRY; the group card never gets it). Idempotent."""
    return under_headline(card, ALSO_PLUS_LINE)


def add_not_in_group(card: str, tag: str | None) -> str:
    """0.8.2 T-202 (D176(1)): the line `<NOT_IN_GROUP>: <tag>` (e.g. HOI-2; no tag -> the words alone) inserted right under a
    rendered card's headline: the owner's PERSONAL-only card of a contract that is not on the client's lists (the group
    never gets that card, nor this line). Idempotent."""
    return under_headline(card, f"{NOT_IN_GROUP}: {esc(tag)}" if tag else NOT_IN_GROUP)


SENT_BY_YOU = "sent by you"  # 0.8.3 G3 (ALERTS.md): a manual PERSONAL card says it was sent by hand


def add_sent_by_you(card: str, sent_hm: str, signal_hm: str) -> str:
    """0.8.3 G3 (docs/research/2026-10-01-forensic/ALERTS.md): the line `sent by you 13:15 · signal 12:35` under a
    MANUALLY sent personal card's headline, so a card sent at 13:15 for a 12:35 signal is never read as a late alert.
    `signal_hm` may carry the day (`28 Sep 12:35`) for a signal of another day. Never on a GROUP card. Idempotent."""
    return under_headline(card, f"{SENT_BY_YOU} {esc(sent_hm)}{DOT}signal {signal_hm}")


# T-310 (owner ruling D278(5)): a V1 signal held until the day's 09:20 list post and sent right after it. The GROUP
# words carry no owner word (clients read the group, both editions); the owner's personal copy may say held.
HELD_GROUP_WORDS = "sent after the 09:20 list"
HELD_OWNER_WORDS = "held for the 09:20 list"


def add_held(card: str, signal_hm: str, *, owner: bool) -> str:
    """T-310: the line `sent after the 09:20 list · signal 09:17` (owner=False: a GROUP card, both editions) or
    `held for the 09:20 list · signal 09:17` (owner=True: the owner's PERSONAL cards) under a rendered card's headline,
    so a card sent at 09:21 for a 09:17 signal is read with its own time (its price is the release LTP). Idempotent."""
    words = HELD_OWNER_WORDS if owner else HELD_GROUP_WORDS
    return under_headline(card, f"{words}{DOT}signal {esc(signal_hm)}")


GROUP_SEND_FAILED = "GROUP SEND FAILED"  # 0.8.2 T-213 F2 (D186(1), D189(4)): owner PERSONAL surfaces only


def add_group_failed(card: str, error: str | None) -> str:
    """0.8.2 T-213 F2 (D189(4)): the line `🔴 GROUP SEND FAILED: <error>` (T-212: 🔴 = failed; was ⚠️) inserted right
    under a rendered card's headline:
    the owner's PERSONAL copy of a group ENTRY / STRONG WATCHLIST card whose GROUP send failed (the group never gets
    this card, nor this line). The error is escaped and cut at 200 characters. Idempotent."""
    return under_headline(card, f"{M_FAIL} {GROUP_SEND_FAILED}: {esc(str(error or 'no detail')[:200])}")


def carry_line(n: int, primed: int, *, floor: int = 0) -> str:
    """0.8.1 (D160(4)): the owner's PERSONAL one-liner after the 09:20 build: "➕ <b>+N carried</b> (P primed)".
    T-282 (d2) / S1.8.0: `floor` = the carried rows retired under the listing OI floor today, appended as
    " · F retired under the OI floor"; 0 leaves the line as it was."""
    line = f"{ICON_ADD} <b>+{int(n)} carried</b> ({int(primed)} primed)"
    return line + (f"{DOT}{int(floor)} retired under the OI floor" if int(floor) else "")


# 0.8.8 T-343 (a) (D378 (a) as corrected by D382 (1)): the owner's PERSONAL FYI when V1 B fired and V1's own gate
# verdict on that V1 B context failed ONLY on zone. A reading aid, never a trade: no rule, tier or tracker reads it.
V1B_ZONE_FYI_HEAD = "FYI \N{MIDDLE DOT} V1 B"
V1B_ZONE_FYI_WHY = "V1 B fired; V1 held it on ZONE only"
V1B_ZONE_FYI_NOT_A_TRADE = "FYI only, not a trade: nothing is tracked and the group gets nothing."


def _lakh_floor(n: float) -> str:
    """A rupee floor in plain lakh / crore words: 1000000 -> ₹10L, 2500000 -> ₹25L, 1250000 -> ₹12.50L."""
    s = indian_short(n)
    return RUPEE + (s.replace(".00", "") if s.endswith(("L", "Cr")) else s)


def v1b_zone_fyi_card(
    meta: ContractMeta,
    signal: Signal,
    ctx: SignalContext | None,
    *,
    tier: str | None,
    floor_rs: float,
    edition: str | None = None,
) -> str:
    """D382 (1): the OWNER's personal FYI card of a V1 B E1 whose V1 gate verdict (ctx.gate_reasons, as gates recorded
    it on the V1 B context) failed on zone alone. Plain words, in this order: the headline, the contract (side mark +
    expiry), the time and price (snapshot signal_price, else the LTP), "V1 B fired; V1 held it on ZONE only", the zone
    gap vs the limit the context was checked with (signed as gates.zone_gap_pct; the stock's side of the edge in words),
    the bar turnover (snapshot turnover_rs) and whether it met V1 B's owner floor (`floor_rs` = V1BCfg
    .owner_min_turnover_rs, Rs 10 lakh), the V1 B tier it got (TIER_LABEL) and that it is FYI, not a trade. Nothing is
    recomputed here. The CLIENT edition raises OwnerOnlyError (no client message ever names V1 B)."""
    if _is_client(edition):
        raise edition_mod.OwnerOnlyError("the V1 B zone FYI is owner PERSONAL only (D378 (a), D382 (1))")
    snap = signal.snapshot if isinstance(signal.snapshot, Mapping) else {}
    price = _num(snap.get("signal_price"))
    price = price if price is not None else _num(signal.ltp)
    ot = str(meta.option_type or "")
    gap = _num(ctx.zone_gap_pct) if ctx is not None else None
    lim = _num(ctx.zone_limit_pct) if ctx is not None else None
    edge = (ctx.zone_edge if ctx is not None and ctx.zone_edge else None) or meta.zone_edge
    if gap is None:
        zone = "Zone gap not recorded"
    else:
        where = f"{zone_word(ot)} {edge_text(edge)}" if edge else f"the {zone_word(ot)} edge"
        limit = f"<b>{lim:.2f}%</b>" if lim is not None else "a limit not recorded"
        zone = f"Zone gap <b>{gap:.2f}%</b> vs limit {limit} (stock {rel_word(ot, gap)} {where})"
    turnover = _num(snap.get("turnover_rs"))
    floor = _lakh_floor(floor_rs)
    if turnover is None:
        turn = f"Bar turnover not known (V1 B's owner floor {floor})"
    else:
        met = turnover >= float(floor_rs) - 1e-9  # gates.v1b_tier's own test (EPS)
        turn = (f"Bar turnover <b>{RUPEE}{indian_short(turnover)}</b>: "
                + (f"meets V1 B's {floor} owner floor" if met else f"below V1 B's {floor} owner floor"))  # fmt: skip
    lines = [
        badge_line(E_WHY, V1B_ZONE_FYI_HEAD, M_WATCH),
        contract_head(meta.symbol, meta.strike, meta.option_type, meta.expiry_date, _ms_day(signal.ts_ms)),
        f"{E_TIME} {ist_hm(signal.ts_ms)}{DOT}{E_PRICE} " + (rupees(price) if price is not None else "price not known"),
        DIV,
        f"<b>{V1B_ZONE_FYI_WHY}</b>",
        zone,
        turn,
        f"V1 B tier: <b>{TIER_LABEL.get(str(tier), esc(tier or 'none'))}</b>",
        DIV,
        f"<i>{V1B_ZONE_FYI_NOT_A_TRADE}</i>",
    ]
    return "\n".join(lines)


def _plus_why_parts(signal: Signal) -> list[str]:
    """The V1 PLUS reasons (a V1 B origin), one WHY IT FIRED line each (T-212; was one `<b>Why</b>:` line):
    "breakout 1.06× the 2-bar high", "OI −13.3% from peak", "turnover ₹31.00L" from the V1 B snapshot (close =
    signal_price else ltp, breakout_ref = the 2-bar high, turnover_rs); a part whose input is missing is left out (the
    OI drop always shows)."""
    snap = signal.snapshot or {}
    parts = []
    close = _num(snap.get("signal_price"))
    close = close if close is not None else _num(signal.ltp)
    ref = _num(snap.get("breakout_ref"))
    if close is not None and ref is not None and ref > 0:
        parts.append(f"breakout {close / ref:.2f}{TIMES} the 2-bar high")
    parts.append(f"OI {pct_drop(signal.oi_drop_pct)} from peak")
    turnover = _num(snap.get("turnover_rs"))
    if turnover is not None:
        parts.append(f"turnover {RUPEE}{indian_short(turnover)}")
    return parts


def _card_head(meta: ContractMeta, signal: Signal, icon: str, word: str, badge: str, sub: str) -> list[str]:
    """T-212 style B card headline block: "⚡ <b>ENTRY</b>   🟢🟢🟢🟢" (the tier alone since 0.8.4, D248(3); the scanner
    is named by `scanner_line` on the owner's PERSONAL cards only, D433) / "▼ <b>BDL 1100 PE</b> · 📅 <b>29 Sep</b> 🟣" /
    "🕒 12:40 · …" (the 🕒 line ends the block: `under_headline` inserts below it)."""
    return [badge_line(icon, word, badge), contract_head(meta.symbol, meta.strike, meta.option_type, meta.expiry_date,
                                                         _ms_day(signal.ts_ms)), sub]  # fmt: skip


def _blend_watch(
    meta: ContractMeta, signal: Signal, ctx: SignalContext | None, when: str, tier: str, *, cards: bool = False,
) -> list[str]:  # fmt: skip
    """NEAR ENTRY / WATCH ONLY (D273 names), style B (T-212, D195(5)): the two-line headline + the 4-dot badge,
    "🕒 time · 🔵 not an entry", the copy line, NOW (a quote: premium, OI, volume), CHECKS (one line per gate in
    GATE_NAMES order).
    T-324 `cards` (a GROUP card, candidate B · Cards, D320 / D321): CHECKS is a quote card too and follows the NOW
    card directly (no rule between them), exactly as previewed; the PERSONAL card (cards False) is unchanged (D319)."""
    box = [
        f"{E_PRICE} Premium{BLEND_GAP}<b>{rupees(signal.ltp)}</b>",
        f"{E_OI} Open interest{BLEND_GAP}<b>{pct_drop(signal.oi_drop_pct)}</b> from today's peak"
        + (f"{DOT}strong drop" if signal.strong else ""),  # D149(5) / D145: the STRONG marker kept
        f"{E_VOL} Volume{BLEND_GAP}<b>{signal.vol_ratio:.1f}{TIMES}</b> normal",
    ]
    passed, _ = check_verdicts(meta, ctx)
    head = _card_head(meta, signal, E_WATCH, TIER_LABEL[tier], check_badge(ctx),
                      f"{E_TIME} {when}{DOT}{M_WATCH} {NOT_ENTRY}")  # fmt: skip
    checks_head = f"{section('CHECKS')}{DOT}{len(passed)} of {len(GATE_NAMES)} passed"
    if cards:
        return [*head, copy_line(meta), DIV, section("NOW"), _quote(box), checks_head, _quote(check_lines(meta, ctx))]
    return [*head, copy_line(meta), DIV, section("NOW"), _quote(box), DIV, checks_head, *check_lines(meta, ctx)]


def trade_alert_card(
    meta: ContractMeta, signal: Signal, ctx: SignalContext | None, when: str, *, client: bool,
    warnings: Sequence[str], desk: bool, tier: str | None, scanner: str | None, also_plus: bool,
    e2: Signal | None, sw: bool, cal: TradingCalendar | None = None, carried: bool = False,
) -> str:
    """D555: one entry card for both chats and editions; levels reuse the existing plans. `carried` without `sw` (an
    owner carried ENTRY with also_plus, an untiered carried E1 re-send; CX-163 review finding 2): a client card names
    the Still Watching list, an owner card keeps its carried line under the contract."""
    from gammad.core.calendar import default_calendar

    plus = scanner == SCANNER_V1_PLUS
    badge = check_badge(ctx, all_passed=True) if tier == "ENTRY" and not sw and not plus else ""
    lines = [badge_line(E_ENTRY, "TRADE ALERT", badge)]
    # Expiry lists are routed only in the owner edition; the card body is shared.
    names = {"BREAKDOWN-PE": "BREAKDOWN LIST", "BREAKOUT-CE": "BREAKOUT LIST",
             "EXPIRY-PE": "EXPIRY LIST", "EXPIRY-CE": "EXPIRY LIST"}
    name = "STILL WATCHING LIST" if sw or (carried and client) else names.get(meta.list_name)
    if name:
        lines.append(section(name))
    lines.append(f"{side_mark(meta.option_type)} <b>{_contract_text(meta.symbol, meta.strike, meta.option_type)}</b>")
    if carried and not sw and not client:
        lines.append(carried_line(meta, client=False))
    expiry, day = _as_date(meta.expiry_date), _as_date(_ms_day(signal.ts_ms))
    count = ""
    if expiry is not None and day is not None and expiry >= day:
        dte = trading_dte(cal or default_calendar(), day, expiry)
        count = DOT + ("today" if dte == 0 else "tmrw" if dte == 1 else f"{dte} trading d")
    lines += [f"{E_EXPIRY} {disp_day(meta.expiry_date)} expiry{count}",
              f"{E_TIME} {when} IST" + (f"{DOT}{PLUS_HEAD_SUFFIX}" if plus else "")]
    if also_plus:
        lines.append(ALSO_PLUS_LINE)
    lines = _named(lines, scanner if scanner_shown(client=client, desk=desk, kind=signal.kind, tier=tier) else None)
    p = entry_plan(signal, meta, ctx)
    cap = f"{BLEND_GAP}(max {rupees(p.cap)})" if p.cap is not None else ""
    lines += [DIV, f"{E_PRICE} Price{BLEND_GAP}<b>{rupees(p.entry)}</b>{cap}"]
    sym = esc(meta.symbol)
    if sw:
        lv = sw_levels(signal)
        near, target = lv["near_edge"], lv["target"]
        side = "below" if str(meta.option_type).upper() == "CE" else "above"
        lines.append(f"{E_SL} Exit if{BLEND_GAP}<b>{edge_text(near)}</b>{DOT}{sym} 5-min close at or {side}"
                     if near is not None else f"{E_SL} Exit if a {sym} 5-min close passes the stop level")
        lines.append(f"{E_EDGE} Trail from{BLEND_GAP}<b>{edge_text(target)}</b>{DOT}then the stop follows {sym}"
                     if target is not None else f"{E_EDGE} No trail level: the stop stays where it is")
        why = ["From an earlier list, still watched", f"OI {pct_drop(signal.oi_drop_pct)} from peak"]
    else:
        if p.hold != "swing" and p.sl is not None:
            off = "= entry" if p.sl >= p.entry else _pct_short(_up(p.entry, p.sl))
            lines.append(f"{E_SL} Exit if{BLEND_GAP}<b>{rupees(p.sl)}</b>{DOT}option at or below ({off})")
        zone = zone_word(meta.option_type)
        lines.append(f"{E_SL} Exit if{BLEND_GAP}<b>{edge_text(p.edge)}</b>{DOT}{sym} {zone} edge (stock stop)"
                     if p.edge else f"{E_SL} Exit if {sym} breaks the {zone} edge (stock stop)")
        lines += [
            (f"{E_T1} Safe at{BLEND_GAP}<b>{rupees(p.t1)}</b>{BLEND_GAP}({_pct_short(_up(p.entry, p.t1))})"
             f"{DOT}stop moves to your entry"),
            f"{E_T2} Book at{BLEND_GAP}<b>{rupees(p.t2)}</b>{BLEND_GAP}({_pct_short(_up(p.entry, p.t2))})",
        ]
        if plus:
            why = _plus_why_parts(signal)
        else:
            strong = " (strong drop)" if signal.strong else ""
            why = [f"OI {pct_drop(signal.oi_drop_pct)} from peak{strong}", f"Volume {signal.vol_ratio:.1f}{TIMES}"]
            zone_part = _why_zone_plain(meta, ctx)
            if zone_part:
                why.append(zone_part)
    if p.lot:
        lines.append(f"{E_LOT} Lot{BLEND_GAP}{indian_int(p.lot)} qty{DOT}{_inr(p.entry * p.lot)}")
    reason = DOT.join(why)
    copy = copy_line(meta) + f" | limit {_p2(p.entry)}" + (f" | qty {p.lot} (1 lot)" if p.lot else "")
    lines += [DIV, f"{E_WHY} {reason[:1].upper()}{reason[1:]}", copy]
    heads = heads_up_sentences(warnings)
    if heads:
        lines += [DIV, f"{section('HEADS-UP')} {M_OPEN}", *heads]
    lines += [DIV, DISCLAIMER]
    if desk and not client:
        owner = _desk_lines(meta, signal, ctx, when, e2, tier)
        if sw:
            owner = [carried_line(meta, client=False), *owner]
            if lv["far_edge"] is not None:
                owner.insert(1, f"stop zone far edge {edge_text(lv['far_edge'])}")
        lines += [DIV, section("DESK"), *(f"{BULLET} {x}" for x in owner)]
    return "\n".join(lines)


def _blend_entry(
    meta: ContractMeta, signal: Signal, ctx: SignalContext | None, when: str, tier: str, *,
    scanner: str | None = None, also_plus: bool = False, cards: bool = False,
) -> list[str]:  # fmt: skip
    """ENTRY, style B (T-212, D195(5)): the two-line headline + the 4-dot badge, "🕒 time · all 4 checks passed", the
    copy line, PLAN (a quote: entry + max, T1, T2), STOPS (the option stop loss on an intraday hold only, then the stock
    stop, always, T2; the D68 note on a swing hold), SIZE (the lot line), WHY IT FIRED (one 🟢 line per reason).
    `scanner` (V1 PLUS: no badge, "· breakout at the zone" and the V1 PLUS reasons; the 0.8.1 "ENTRY · V1" headline
    became the tier alone in 0.8.4, D248(3), the scanner named on owner PERSONAL cards only, D433); `also_plus` adds "➕ also V1 PLUS" under the headline (owner PERSONAL copy).
    T-324 `cards` (a GROUP card, candidate B · Cards, D320 / D321): STOPS and WHY IT FIRED are quote cards too; the
    PERSONAL card (cards False) is unchanged (D319)."""
    p = entry_plan(signal, meta, ctx)
    cap = f"{BLEND_GAP}(max {rupees(p.cap)})" if p.cap is not None else ""
    plan = [f"{E_PRICE} Entry{BLEND_GAP}<b>{rupees(p.entry)}</b>{cap}",
            f"{E_T1} Target 1{BLEND_GAP}<b>{rupees(p.t1)}</b>{BLEND_GAP}({_pct_short(_up(p.entry, p.t1))})",
            f"{E_T2} Target 2{BLEND_GAP}<b>{rupees(p.t2)}</b>{BLEND_GAP}({_pct_short(_up(p.entry, p.t2))})"]  # fmt: skip
    stops = []
    if p.hold != "swing" and p.sl is not None:
        off = "= entry" if p.sl >= p.entry else _pct_short(_up(p.entry, p.sl))
        stops.append(f"{E_SL} Stop loss{BLEND_GAP}<b>{rupees(p.sl)}</b>{BLEND_GAP}({off})")
    zone = zone_word(meta.option_type)
    stops.append(f"{E_EDGE} Stock stop{BLEND_GAP}<b>{edge_text(p.edge)}</b>{BLEND_GAP}({zone} edge)" if p.edge
                 else f"{E_EDGE} Stock stop at the {zone} edge")  # fmt: skip
    if p.hold == "swing":
        stops.append(_swing_note(p, meta.option_type, expiry_cap=True))
    plus = scanner == SCANNER_V1_PLUS
    # 0.8.4 (D248(3)): the scanner is named on the card's first line (`scanner_line`, added by _blend_card; owner
    # PERSONAL cards only since D433), so the
    # tier line is the tier alone (was "ENTRY · V1" / "ENTRY · V1 PLUS")
    suffix = PLUS_HEAD_SUFFIX if plus else f"all {len(GATE_NAMES)} checks passed"
    head = _card_head(meta, signal, E_ENTRY, TIER_LABEL[tier], "" if plus else check_badge(ctx, all_passed=True),
                      f"{E_TIME} {when}{DOT}{suffix}")  # fmt: skip
    lines = [*head, *([ALSO_PLUS_LINE] if also_plus else []), copy_line(meta), DIV,
             section("PLAN"), _quote(plan), section("STOPS"), *([_quote(stops)] if cards else stops)]  # fmt: skip
    if p.per_lot is not None:
        lot = [f"{E_LOT} 1 lot = {indian_int(p.lot or 0)} qty", _inr(p.per_lot)]
        if p.risk_lot is not None:
            lot.append(f"risk {_inr(p.risk_lot)}")
        lines += ["", section("SIZE"), DOT.join(lot)]
    if plus:
        why = _plus_why_parts(signal)
    else:
        strong = " (strong drop)" if signal.strong else ""  # D149(5) / D145: the STRONG marker kept
        why = [f"OI {pct_drop(signal.oi_drop_pct)} from peak{strong}", f"Volume {signal.vol_ratio:.1f}{TIMES}"]
        zone_part = _why_zone_plain(meta, ctx)
        if zone_part:
            why.append(zone_part)
    why_lines = [f"{M_PASS} {x[:1].upper()}{x[1:]}" for x in why]
    return [*lines, DIV, section("WHY IT FIRED"), *([_quote(why_lines)] if cards else why_lines)]


def scanner_title(scanner: str) -> str:
    """The display name of a scanner id: "V1" -> "V1 Scanner", "V2T" -> "V2 Scanner" (SCANNER_TITLE, the mirror of
    engine_pool.SCANNER_LABELS); any other id is its own name ("V1 PLUS")."""
    return SCANNER_TITLE.get(str(scanner), str(scanner))


def scanner_line(scanner: str) -> str:
    """The scanner line "<b>V1 Scanner</b>" / "<b>V2 Scanner</b>": line 1 of the OWNER's PERSONAL signal cards only
    (0.8.8, D433 / D429; `scanner_shown` decides; never a client-edition text, never a GROUP card)."""
    return f"<b>{esc(scanner_title(scanner))}</b>"


def _named(lines: list[str], scanner: str | None) -> list[str]:
    """The card lines with `scanner_line` first (a None scanner = the unnamed card, unchanged)."""
    return [scanner_line(scanner), *lines] if scanner else lines


def scanner_shown(*, client: bool, desk: bool, kind: str, tier: str | None) -> bool:
    """0.8.8 (owner, 13 Oct, label verbatim "Remove in group too (Recommended)" + D429 "remove v1 scanner tag everywhere
    client"; supersedes D248(3)'s "every card names its scanner, both editions"): may a V1-family card carry its
    `scanner_line`? ONLY the OWNER's PERSONAL cards: a `desk` card (every personal send of a tiered card and every manual
    personal send asks desk=True; a GROUP card never does, T-178) or the owner's untiered E2 arming card (PERSONAL only by
    construction, router `_route_personal_e2`; the client edition raises on an E2). Never in the CLIENT edition (any
    chat: its own-bot cards, follow-ups, summaries) and never on a GROUP card of the owner edition (clients read it).
    The V2 Scanner card (`_v2_card`, owner PERSONAL only, never the group since 0.8.5) keeps its own line."""
    if client:
        return False
    return desk or (kind == "E2" and tier is None)


# ------------------------------------------------------------------------------------------ the V2 Scanner card
# 0.8.4 (T-267; STRATEGY-SPEC 4.7, D248(5), D249(4), D251 / M7, M15). The client guide (tools/client_docs/content/
# guide.mjs, section 3.7) quotes two of these lines, so they are its words exactly: the stop sentence "CE exits on a
# 5-min close below 1,240.50" and the NO_ZONE line "no 1h SL zone: exits on day 5 or expiry". No T1 / T2, no spec
# version (D241(2)), no study numbers, no owner-only words on the client card.
V2_HOLD_DAYS = ZONE_HOLD_DAYS  # rules.ZONE_HOLD_DAYS (v2_scanner.hold_days): the ENTRY day is day 1
V2_EXIT_HM = "15:20"  # DAY5 / EXPIRY exit clock (D251 / M15, rules.on_zone_session_end)
V2_NO_ZONE_LINE = f"no 1h SL zone: exits on day {V2_HOLD_DAYS} or expiry"  # M7 / D251; the guide's own words
V2_TRAIL_LINE = "<i>Trail: the stop moves to each new 1-hour zone on the trade's side, never back.</i>"
V2_SUFFIX = "OI unwind + premium jump"
V2_HOLD_LINE = f"{E_CLOSED} At most {V2_HOLD_DAYS} trading days: exits at {V2_EXIT_HM} on day {V2_HOLD_DAYS}"
V2_STRONG_BADGE = f"{ICON_STRONG} STRONG"  # D243 / D248: >= 12 % unwind; OWNER cards only (D257: no client entry)


def v2t_block(signal: Signal) -> dict[str, Any] | None:
    """The V2 Scanner block of a signal's snapshot (gammad.v2scan.trigger: snapshot["v2t"]); None = not a V2 signal."""
    snap = signal.snapshot if isinstance(signal.snapshot, Mapping) else None
    block = snap.get("v2t") if snap is not None else None
    return dict(block) if isinstance(block, Mapping) else None


def v2_stop(block: Mapping[str, Any] | None) -> tuple[float, float] | None:
    """(bottom, top) of the 1-hour stop zone at entry (snapshot["v2t"]["stop"] = {top, bottom, confirmed_ms}); None =
    NO_ZONE (an absent / null / unreadable stop: the tracker opens such a trade as NO_ZONE too, M7)."""
    stop = block.get("stop") if isinstance(block, Mapping) else None
    if not isinstance(stop, Mapping):
        return None
    top, bottom = _num(stop.get("top")), _num(stop.get("bottom"))
    if top is None or bottom is None or not top > 0 or not bottom > 0:
        return None
    return (min(top, bottom), max(top, bottom))


def v2_far_edge(option_type: Any, bottom: float, top: float) -> float:
    """The stop zone's FAR edge (STRATEGY-SPEC 4.7): CE exits below its bottom, PE above its top."""
    return float(top) if str(option_type or "").upper() == "PE" else float(bottom)


def v2_stop_sentence(option_type: Any, edge: float) -> str:
    """"CE exits on a 5-min close below 1,238.4" / "PE exits on a 5-min close above 1,240.5" (guide 3.7)."""
    ot = str(option_type or "").upper()
    return f"{esc(ot)} exits on a 5-min close {'above' if ot == 'PE' else 'below'} {edge_text(edge)}"


def v2_stop_away_pct(option_type: Any, bottom: float, top: float, und_px: Any) -> float | None:
    """T-282 (e), owner ruling 5 Oct: how far the stop zone's NEAR edge is from the stock price stored at the trigger
    (snapshot["v2t"]["und_px"]), in percent of that price: a PE's zone sits above the stock (near = its bottom), a CE's
    below it (near = its top). #583 TIINDIA: (2,631.41 - 2,340.0) / 2,340.0 = 12.5 %. None = no stored price, or the
    stock at / inside / beyond the near edge (no distance to claim). The stop itself is unchanged."""
    px = _num(und_px)
    if px is None or not px > 0:
        return None
    pe = str(option_type or "").upper() == "PE"
    away = (float(bottom) - px) if pe else (px - float(top))
    return away / px * 100.0 if away > 0 else None


def v2_zone_word(option_type: Any) -> str:
    """A V2 stop zone's kind: a CE is protected by the support BELOW the stock, a PE by the resistance ABOVE it (the
    opposite of V1's list zone, `zone_word`)."""
    return "resistance" if str(option_type or "").upper() == "PE" else "support"


def v2_expiry_in_hold(meta: ContractMeta, signal: Signal, cal: TradingCalendar | None = None) -> bool:
    """True when the contract expires on hold day 1..V2_HOLD_DAYS (the entry day = day 1, rules.zone_hold_day): the
    trade then exits on the expiry day at 15:20 (EXPIRY wins over DAY5). An unreadable expiry -> False."""
    try:
        expiry = date.fromisoformat(str(meta.expiry_date))
    except ValueError:
        return False
    if cal is None:
        from gammad.core.calendar import default_calendar

        cal = default_calendar()
    day = datetime.fromtimestamp(signal.ts_ms / 1000, IST).date()
    return expiry >= day and trading_dte(cal, day, expiry) + 1 <= V2_HOLD_DAYS


def _v2_why(signal: Signal, block: Mapping[str, Any], option_type: str, *, strong: bool) -> list[str]:
    """WHY IT FIRED of a V2 card: the OI unwind (STRONG: owner only, D257), the volume run, the premium jump, the
    stock's daily trend (an ENTRY exists only when it agrees, gates.v2t_tier)."""
    drop = _num(block.get("drop"))
    drop = drop if drop is not None else _num(signal.oi_drop_pct)
    out = []
    if drop is not None:
        out.append(f"OI {pct_drop(drop)} from today's peak" + (" (STRONG)" if strong else ""))
    run = block.get("candles")
    n = len(run) if isinstance(run, list | tuple) and run else 3
    out.append(f"Volume {float(signal.vol_ratio or 0.0):.1f}{TIMES} normal, above average {n} candles in a row")
    jump = _num(block.get("jump_pct"))
    prev = _num(block.get("prev_close"))
    if jump is None and prev and prev > 0 and signal.ltp:
        jump = (float(signal.ltp) / prev - 1.0) * 100.0
    if jump is not None:
        out.append(f"Premium {_pct_short(jump)} on the candle" + (f" (from {rupees(prev)})" if prev else ""))
    out.append(f"Stock's daily trend {'down' if option_type.upper() == 'PE' else 'up'}")
    return out


def _v2_desk_lines(meta: ContractMeta, signal: Signal, ctx: SignalContext | None, when: str,
                   block: Mapping[str, Any]) -> list[str]:  # fmt: skip
    """OWNER DESK of a V2 card (a PERSONAL send only): OI peak -> now, the stock at the trigger, the regime, the stop
    zone's confirmation time, list · tag · time, the tier reasons."""
    out: list[str] = []
    peak, oi = _num(block.get("oi_peak")), _num(block.get("oi"))
    if peak is not None and oi is not None:
        out.append(f"OI {oi_short(peak)} {ARROW} {oi_short(oi)} (bar-close peak)")
    und = _num(block.get("und_px"))
    if und is not None:
        out.append(f"stock {price_text(und)} at the trigger")
    regime = block.get("regime")
    if isinstance(regime, Mapping):
        regime = regime.get("st_dir")
    if regime is not None:
        word = {1: "green", -1: "red"}.get(int(regime) if isinstance(regime, int | float) else 0, str(regime))
        out.append(f"daily Supertrend {esc(word)}")
    stop = block.get("stop")
    cms = _num(stop.get("confirmed_ms")) if isinstance(stop, Mapping) else None
    if cms:
        out.append(f"stop zone confirmed {_day_of(int(cms))} {ist_hm(int(cms))}")
    out.append(DOT.join([*(esc(x) for x in (meta.list_name, meta.tag) if x), when]))
    if ctx is not None and (ctx.gate_reasons or ctx.gate_unknown):
        out.append(f"tier reasons: {esc(', '.join([*ctx.gate_reasons, *ctx.gate_unknown]))}")
    return out


def _v2_card(
    meta: ContractMeta, signal: Signal, ctx: SignalContext | None, when: str, *, client: bool, tier: str,
    warnings: Sequence[str], desk: bool, cal: TradingCalendar | None = None,
) -> str:  # fmt: skip
    """0.8.4 (T-267): a V2 Scanner card, the same text in both editions (the owner DESK only on a PERSONAL send; the
    STRONG badge and "(STRONG)" owner only, D257: the client never enters one and never reads the word):
    "<b>V2 Scanner</b>" / "⚡ <b>ENTRY</b>   ★ STRONG" / contract · 📅 expiry / "🕒 time · OI unwind + premium jump" /
    the copy line / PLAN (the entry = the trigger close, + max) / STOP (the stock's 1-hour stop zone and the exit
    sentence, the trail; NO_ZONE: `V2_NO_ZONE_LINE`) / HOLD (at most 5 trading days; the expiry-day 15:20 exit only
    when the contract expires inside the hold) / SIZE / WHY IT FIRED / HEADS-UP / the disclaimer. A watch tier is
    owner-only (the client never records a non-ENTRY V2 row, PLAN-T267 §1 "Tier rule")."""
    watch = tier in ("STRONG_WATCHLIST", "WATCHLIST")
    if client and watch:
        raise edition_mod.OwnerOnlyError("a V2 Scanner watch card is owner-edition only (PLAN-T267 §1 Tier rule)")
    block = v2t_block(signal) or {}
    ot = str(meta.option_type or "")
    entry = float(signal.ltp)
    cap = _floor_tick(entry * (1 + CHASE_PCT), _tick(meta)) if entry > 0 else None
    cap_txt = f"{BLEND_GAP}(max {rupees(cap)})" if cap is not None and cap > entry + 1e-9 else ""
    strong = bool(signal.strong) and not client  # D257: no client surface shows STRONG for V2 (the router drops them)
    badge = V2_STRONG_BADGE if strong else ""
    if watch:
        head = _card_head(meta, signal, E_WATCH, TIER_LABEL[tier], badge,
                          f"{E_TIME} {when}{DOT}{M_WATCH} {NOT_ENTRY}")  # fmt: skip
    else:
        head = _card_head(meta, signal, E_ENTRY, TIER_LABEL[tier], badge, f"{E_TIME} {when}{DOT}{V2_SUFFIX}")
    zone = v2_stop(block)
    if zone is None:
        stop_lines = [f"{E_EDGE} {V2_NO_ZONE_LINE}"]
    else:
        bottom, top = zone
        stop_lines = [
            (f"{E_EDGE} Stock stop zone{BLEND_GAP}<b>{edge_text(bottom)} {EN_DASH} {edge_text(top)}</b>"
             f"{BLEND_GAP}(1-hour {v2_zone_word(ot)})"),
            f"<b>{v2_stop_sentence(ot, v2_far_edge(ot, bottom, top))}</b>",
            V2_TRAIL_LINE,
        ]  # fmt: skip
        away = v2_stop_away_pct(ot, bottom, top, block.get("und_px"))
        if away is not None:  # T-282 (e): "Stop 12.5% away from the stock at 2,340", under the zone line
            stop_lines.insert(1, f"Stop {away:.1f}% away from the stock at {edge_text(float(block['und_px']))}")
    hold = [V2_HOLD_LINE]
    if v2_expiry_in_hold(meta, signal, cal):
        hold.append(f"{E_CLOSED} Expires {disp_day(str(meta.expiry_date))}: exits on the expiry day at {V2_EXIT_HM}")
    lines = [scanner_line(SCANNER_V2), *head, copy_line(meta), DIV,
             section("PLAN"), _quote([f"{E_PRICE} Entry{BLEND_GAP}<b>{rupees(entry)}</b>{cap_txt}"]),
             section("STOP"), *stop_lines, section("HOLD"), *hold]  # fmt: skip
    lot = meta.lot_size or (ctx.lot_size if ctx is not None else None)
    if lot:
        lines += ["", section("SIZE"), DOT.join([f"{E_LOT} 1 lot = {indian_int(int(lot))} qty", _inr(entry * lot)])]
    lines += [DIV, section("WHY IT FIRED"), *(f"{M_PASS} {x}" for x in _v2_why(signal, block, ot, strong=strong))]
    heads = heads_up_sentences(warnings)
    if heads:
        lines += [DIV, f"{section('HEADS-UP')} {M_OPEN}", *heads]
    if not watch:
        lines += ["", DISCLAIMER]
    if desk and not client:
        lines += [DIV, section("DESK"), *(f"{BULLET} {x}" for x in _v2_desk_lines(meta, signal, ctx, when, block))]
    return "\n".join(lines)


def _blend_card(
    meta: ContractMeta, signal: Signal, ctx: SignalContext | None, when: str, *, client: bool, tier: str,
    warnings: Sequence[str], desk: bool, e2: Signal | None = None, scanner: str | None = None,
    also_plus: bool = False, carried: bool = False, plus_risk: bool = False,
) -> str:  # fmt: skip
    """T-178 (D148): a tiered E1 card, the same text in both editions; the owner DESK only when `desk` (the router
    asks for it on a PERSONAL send only: a card sent to the GROUP never carries it, clients read the group).
    0.8.1: `scanner` / `also_plus` (ENTRY headline, `_blend_entry`), `carried` (the `carried_line` under the
    headline, owner only); `plus_risk` is accepted and ignored (D555).
    T-324 (D320, D321): a GROUP card (no `desk`: the router's group paths, format_signal / signal_share, never pass
    it) gets candidate B · Cards (CHECKS / STOPS / WHY IT FIRED as quote cards); a PERSONAL card (`desk` True, every
    personal send in both editions) is unchanged, byte for byte (D319: the personal chat is the model)."""
    watch = tier in ("STRONG_WATCHLIST", "WATCHLIST")
    cards = not desk
    if watch:
        lines = _blend_watch(meta, signal, ctx, when, tier, cards=cards)
    else:
        lines = _blend_entry(meta, signal, ctx, when, tier, scanner=scanner, also_plus=also_plus, cards=cards)
    if carried:  # under the three-line headline block (T-212); T-419 (D437): a client card names its list, not the carry
        lines.insert(3, sw_list_line() if client else carried_line(meta, client=False))
    # 0.8.8 (owner 13 Oct "Remove in group too"): the scanner is named on the owner's PERSONAL card only
    lines = _named(lines, scanner if scanner_shown(client=client, desk=desk, kind=signal.kind, tier=tier) else None)
    heads = heads_up_sentences(warnings)
    if heads:
        lines += [DIV, f"{section('HEADS-UP')} {M_OPEN}", *heads]
    if not watch:
        lines += ["", DISCLAIMER]
    if desk and not client:
        lines += [DIV, section("DESK"), *(f"{BULLET} {x}" for x in _desk_lines(meta, signal, ctx, when, e2, tier))]
    return "\n".join(lines)


# ------------------------------------------------------------------------------------------ 0.8.9 T-419 Still Watching
def sw_list_line() -> str:
    """D555: the client's carried watch card names the Still Watching list without an eye icon."""
    return f"{STILL_WATCHING} list"


def sw_levels(signal: Signal) -> dict[str, float | None]:
    """The Still Watching levels of an entry signal: snapshot[SW_SNAPSHOT_KEY] = {near_edge, far_edge, target} (stock
    levels: the 1-hour stop zone's NEAR / FAR edge and the daily-zone target; T419-CONTRACT SwPosition). A missing /
    unreadable value is None (the card then words the line without a number)."""
    snap = signal.snapshot if isinstance(signal.snapshot, Mapping) else None
    block = snap.get(SW_SNAPSHOT_KEY) if snap is not None else None
    block = block if isinstance(block, Mapping) else {}
    out: dict[str, float | None] = {}
    for key in ("near_edge", "far_edge", "target"):
        v = _num(block.get(key))
        out[key] = v if v is not None and v > 0 else None
    return out


def sw_stop_line(near: float | None) -> str:
    """The plain stop sentence (D471 Q5, S2-CLOSE: a 5-min stock close at / beyond the 1-hour zone's NEAR edge)."""
    where = f"<b>{edge_text(near)}</b>" if near is not None else "the stock stop level"
    return f"{E_EDGE} Exit if a 5-min candle closes past {where}"


def sw_target_line(target: float | None) -> str:
    """The plain target sentence (D471 Q6 + D472 X9A: the daily-zone target is NOT an exit; from it the stop trails)."""
    if target is None:
        return f"{E_T1} No target level: the stop stays where it is"
    return f"{E_T1} At <b>{edge_text(target)}</b> the stop starts trailing"


def sw_entry_card(
    meta: ContractMeta, signal: Signal, ctx: SignalContext | None, when: str, *, client: bool,
    warnings: Sequence[str], desk: bool, sw_cfg: Any = None, tier: str = "ENTRY", e2: Signal | None = None,
) -> str:  # fmt: skip
    """The Still Watching entry wrapper; sw_cfg is accepted and ignored (D555)."""
    return trade_alert_card(meta, signal, ctx, when, client=client, warnings=warnings, desk=desk,
                            tier=tier, scanner=SCANNER_V1, also_plus=False, e2=e2, sw=True)


def _sheet(
    meta: ContractMeta,
    signal: Signal,
    ctx: SignalContext | None,
    when: str,
    *,
    edition: str | None,
    e2: Signal | None = None,
    tier: str | None = None,
    warnings: Sequence[str] = (),
    desk: bool = False,
    scanner: str | None = SCANNER_V1,
    also_plus: bool = False,
    carried: bool = False,
    plus_risk: bool = False,
    cal: TradingCalendar | None = None,
    sw_cfg: Any = None,
) -> str:
    """Route display cards; sw_cfg is accepted and ignored (D555)."""
    client = _is_client(edition)
    if client and signal.kind != "E1":
        raise edition_mod.OwnerOnlyError(f"a {signal.kind} entry card is owner-edition only (D81, D86)")
    carried = carried or bool(getattr(meta, "carried", False)) or bool(getattr(ctx, "carried", False))
    if client and (also_plus or (scanner is not None and scanner not in CLIENT_SCANNERS)):
        # 0.8.1 (D159(1)): clients get V1 (0.8.5 D287(6): no V2 Scanner); a V1 PLUS card is owner-only. 0.8.9 T-419
        # (D471 Q7, overruling D157(2) for the client): a carried card is the client's Still Watching card
        raise edition_mod.OwnerOnlyError("a V1 PLUS or V2 card is owner-edition only (D159(1), D287(6))")
    if tier is not None:
        if signal.kind not in ("E1", "E2"):  # 0.8.5 (D287(2), PLAN-D287 I2): the owner's E2 is tiered too
            raise ValueError(f"a {signal.kind} is untiered (tiers are for V1 E1 / E2 only); got tier {tier!r}")
        if tier not in TIER_LABEL:
            raise ValueError(f"unknown tier {tier!r}; expected one of {sorted(TIER_LABEL)}")
    if scanner == SCANNER_V2:  # 0.8.4 (T-267): the V2 Scanner's own card (no targets; the stock's 1-hour zone stop)
        if client:  # 0.8.5 (D287(6), PLAN-D287 AM7): the client edition has no V2 Scanner (before any V2 work)
            raise edition_mod.OwnerOnlyError("a V2 Scanner card is owner-edition only (D287(6))")
        if signal.kind != "E1":
            raise ValueError(f"the V2 Scanner fires E1 only; got {signal.kind}")
        return _v2_card(meta, signal, ctx, when, client=client, tier=tier or "ENTRY", warnings=warnings, desk=desk,
                        cal=cal)  # fmt: skip
    if tier == "ENTRY" or (tier is None and signal.kind == "E1"):
        return trade_alert_card(meta, signal, ctx, when, client=client, warnings=warnings, desk=desk,
                                tier=tier, scanner=scanner, also_plus=also_plus, e2=e2,
                                sw=carried and tier == "ENTRY" and not also_plus, cal=cal, carried=carried)
    if tier is not None:
        return _blend_card(meta, signal, ctx, when, client=client, tier=tier, warnings=warnings, desk=desk, e2=e2,
                           scanner=scanner, also_plus=also_plus, carried=carried, plus_risk=plus_risk)  # fmt: skip
    # T-162 v3 sections in style B (T-212): the untiered E1 (older rows re-sent by hand) and the owner E2 (arming) card
    if signal.kind == "E2":
        icon, word = ICON_ARMING, entry_title("E2", edition)
    else:
        icon, word = E_ENTRY, entry_title(signal.kind, edition)
    shown = scanner if scanner_shown(client=client, desk=desk, kind=signal.kind, tier=None) else None  # 0.8.8
    lines = _named(_card_head(meta, signal, icon, word, "", f"{E_TIME} {when} IST"), shown)
    if carried:  # 0.8.1 (D160(3)): an owner E2 / untiered card on a CARRY row says so (T-419: a client's, its list)
        lines.append(sw_list_line() if client else carried_line(meta, client=False))
    p = entry_plan(signal, meta, ctx)
    lines += [DIV, *_plan_sections(p, meta.option_type)]
    lines += [DIV, section("WHY IT FIRED"), _why_quote(signal, meta, ctx, all_checks=False)]
    heads = heads_up_lines(warnings)
    if heads:
        lines += [DIV, f"{section('HEADS-UP')} {M_OPEN}", *heads]
    if client:
        lines += ["", DISCLAIMER]
        return "\n".join(lines)
    if signal.kind == "E2":  # the arming card (OWNER only by construction, D81 / D86): D125 F9 peak line first
        owner = _desk_lines(meta, signal, ctx, when, e2, tier)
        peak = peak_line(signal, ctx)
        owner = [peak, *owner] if peak else owner
        lines += [DIV, section("ARMING"), *(f"{BULLET} {x}" for x in owner)]
    elif desk:  # T-178: the owner DESK on a PERSONAL send only, never on a GROUP card
        lines += [DIV, section("DESK"), *(f"{BULLET} {x}" for x in _desk_lines(meta, signal, ctx, when, e2, tier))]
    return "\n".join(lines)


def _hold_word(hold: str) -> str:
    """The hold line's word: SWING (every W15 list, a hold for the move) or SAME-DAY (the legacy same-day style
    of older specs / the V2 shadow params; never on a V1 card). Never "intraday" (D114)."""
    return "SWING" if hold == "swing" else "SAME-DAY"


def signal_card(
    meta: ContractMeta,
    signal: Signal,
    *,
    ctx: SignalContext | None = None,
    spec_version: str = "",
    edition: str | None = None,
    e2: Signal | None = None,
    tier: str | None = None,
    warnings: Sequence[str] | None = None,
    desk: bool = False,
    scanner: str | None = SCANNER_V1,
    also_plus: bool = False,
    carried: bool = False,
    plus_risk: bool = False,
    cal: TradingCalendar | None = None,
    sw_cfg: Any = None,
) -> str:
    """The entry card for an E1 / E2, per edition (default gammad.edition.edition()).
    D555 supersedes the historical ENTRY layouts below: tiered ENTRY and untiered E1 use trade_alert_card,
    without a risk or hold-exit line. Watch cards and the owner untiered E2 retain their layouts.
    0.8.9 T-419 (D471 Q7, overruling D157(2) for the client): a CARRIED ENTRY (carried / meta.carried / ctx.carried, tier
    ENTRY) is the Still Watching card in BOTH editions; sw_cfg is accepted and ignored (D555).
    A carried card below ENTRY keeps its watch card, with `sw_list_line` (client) or
    the owner's `carried_line`. The client edition no longer raises for a carried card.
    0.8.4 (T-267, D248(3)): line 1 of every card names its scanner (`scanner_line`): "<b>V1 Scanner</b>" (default,
    both editions, every tier, the untiered E1 and the owner E2) or "<b>V2 Scanner</b>" (scanner "V2T": `_v2_card`, no
    targets, the stock's 1-hour zone stop, at most 5 trading days, the expiry-day exit when the expiry falls inside the
    hold -- `cal` counts it, None = the default calendar). The tier line under it is the tier alone ("⚡ <b>ENTRY</b>").
    0.8.8 (owner 13 Oct "Remove in group too" + D429, superseding D248(3) for V1): the V1-family scanner line is on the
    OWNER's PERSONAL cards only (`scanner_shown`: `desk`, or the untiered owner E2); a CLIENT-edition card and a GROUP
    card (no `desk`) carry no scanner name.
    0.8.1 (S1.5.0, D159-D161), all owner-side unless said:
      scanner    names the card on line 1 (above): default V1 (both editions, the group card); "V1 PLUS" = line 1
                 "<b>V1 PLUS</b>", "⚡ <b>ENTRY</b>" and "· breakout at the zone" with the V1 PLUS Why line (breakout x,
                 OI drop, turnover); None = no scanner line (the unnamed card). signal_share passes the stored row's
                 scanner.
      also_plus  "➕ also V1 PLUS" under the headline: ONLY the owner's PERSONAL copy of a shared V1 card (D161(1)); the
                 router adds it with `add_also_plus` after rendering.
      carried    (or meta.carried / ctx.carried) the `carried_line` under the headline: a signal on a CARRY row (owner
                 PERSONAL).
      plus_risk  accepted and ignored (D555).
    The CLIENT edition raises OwnerOnlyError for a scanner other than V1 / V2T or also_plus (T-419: not for carried).
    A TIERED E1 (tier = gates.tier's value) is the T-178 "L1 + C2 blend" (D148, docs/design-previews/telegram-v5/
    blend.html), the same text in both editions: ENTRY = "⚡ <b>ENTRY</b> · all 4 checks passed", a <blockquote> box
    (contract · 📅 expiry · 🕒 time, then Entry (max), Stop loss (intraday hold only; the router hands the D125 F7 hold
    at entry, `at_entry_context`), Target 1, Target 2), the tap-to-copy line (`copy_line`), the stock stop (always, T2),
    the lot line, the D68 note on a swing hold, one "Why" line, the heads-up sentences, the disclaimer.
    STRONG_WATCHLIST / WATCHLIST = "👀 <b>NEAR ENTRY</b>" / "👀 <b>WATCH ONLY</b>" (TIER_LABEL, D273) · "🔵 not an
    entry", a box (premium, open interest, volume),
    the copy line, "N of 4 checks passed", the passed checks on ONE line, each failed check on its own line, the
    heads-up sentences (no levels, no disclaimer).
    An untiered E1 (older rows re-sent by hand) and the OWNER E2 (arming; the client raises OwnerOnlyError) keep the
    T-162 v3 sections (PLAN / WHY IT FIRED / HEADS-UP; the E2 ends with ARMING, the D125 F9 peak line + the DESK lines).
    `desk` (T-178, owner rule "clients read the group"): the OWNER DESK section (quadrant, OI, trigger / "After E2",
    list, gates) is added ONLY when True and only in the owner edition; the router asks for it on a PERSONAL send and
    never on a GROUP card. No "BUY"/"SELL" (D79): the order text is behind the Copy order button (`copy_order_text`).
    `spec_version` is accepted for compatibility and never printed. `tier` (E1 only): an E2 with a tier or an unknown
    tier raises ValueError. `warnings` = `warnings()`'s lines."""
    del spec_version
    return _sheet(meta, signal, ctx, ist_hm(signal.ts_ms), edition=edition, e2=e2, tier=tier,
                  warnings=tuple(warnings or ()), desk=desk, scanner=scanner, also_plus=also_plus, carried=carried,
                  plus_risk=plus_risk, cal=cal, sw_cfg=sw_cfg)  # fmt: skip


def warnings(meta: ContractMeta, signal: Signal, alerts_cfg: Any, cal: TradingCalendar | None = None) -> list[str]:
    """The card warnings of D114(1) (a warning, never a cut-off), in this order:
    expiry  dte = trading days after the signal's IST date up to and including the expiry (the repo calendar,
            gammad.core.calendar: weekends + config/nse_holidays.yaml); dte 0 -> "EXPIRES TODAY — plan
            accordingly"; dte 1 -> "Expires tomorrow — plan the hold accordingly" (0.8.1, D161(7): dte 0 and 1 warn
            whatever expiry_warn_dte says, both editions); 2 <= dte <= alerts.expiry_warn_dte -> "Expires in N
            trading days — plan the hold accordingly".
    late    the signal's IST time >= alerts.late_day_warn_ist ("HH:MM") -> "Day is closing — plan accordingly".
    `alerts_cfg` = Settings.alerts; a key it does not carry yet reads its default (14:30 / 7). `cal` None = the
    default calendar."""
    if cal is None:
        from gammad.core.calendar import default_calendar

        cal = default_calendar()
    warn_dte = int(getattr(alerts_cfg, "expiry_warn_dte", EXPIRY_WARN_DTE_DEFAULT))
    late_at = time.fromisoformat(str(getattr(alerts_cfg, "late_day_warn_ist", LATE_DAY_WARN_IST_DEFAULT)))
    at = datetime.fromtimestamp(signal.ts_ms / 1000, IST)
    out: list[str] = []
    expiry = date.fromisoformat(str(meta.expiry_date))
    dte = trading_dte(cal, at.date(), expiry)  # the tracker's D125 F7 hold rule reads the same count
    if dte == 0:
        out.append(f"EXPIRES TODAY {EM_DASH} plan accordingly")
    elif dte == 1:
        out.append(f"Expires tomorrow {EM_DASH} plan the hold accordingly")
    elif dte <= warn_dte:
        out.append(f"Expires in {_plural(dte, 'trading day')} {EM_DASH} plan the hold accordingly")
    if at.time() >= late_at:
        out.append(f"Day is closing {EM_DASH} plan accordingly")
    return out


def e2_card(
    meta: ContractMeta,
    signal: Signal,
    *,
    ctx: SignalContext | None = None,
    spec_version: str = "",
    edition: str | None = None,
) -> str:
    if signal.kind != "E2":
        raise ValueError(f"e2_card got a {signal.kind} signal")
    return signal_card(meta, signal, ctx=ctx, spec_version=spec_version, edition=edition)


def e1_card(
    meta: ContractMeta,
    signal: Signal,
    *,
    ctx: SignalContext | None = None,
    spec_version: str = "",
    edition: str | None = None,
    e2: Signal | None = None,
    desk: bool = False,
    scanner: str | None = SCANNER_V1,
    tier: str | None = None,
    warnings: Sequence[str] | None = None,
    cal: TradingCalendar | None = None,
) -> str:
    """An E1 card (signal_card): `scanner` names line 1 ("V1" -> "V1 Scanner", "V2T" -> "V2 Scanner", D248(3))."""
    if signal.kind != "E1":
        raise ValueError(f"e1_card got a {signal.kind} signal")
    return signal_card(meta, signal, ctx=ctx, spec_version=spec_version, edition=edition, e2=e2, desk=desk,
                       scanner=scanner, tier=tier, warnings=warnings, cal=cal)  # fmt: skip


def copy_order_text(signal: Signal, meta: ContractMeta, ctx: SignalContext | None = None) -> str:
    """The plain text behind the entry card's "Copy order" button (D79; Bot API copy_text, <= 256 characters):
    'RECLTD 29 SEP 300 PE | limit 0.30 | qty 1575 (1 lot) | SL 0.25 | T1 0.36 | T2 0.42'. Swing: 'stock stop
    1158.3' instead of the SL. No "BUY"/"SELL", no HTML, no emoji, no grouping commas (it is pasted into a broker).
    0.8.4 (T-267): a V2 Scanner signal (snapshot["v2t"]) has no targets: 'BDL 29 SEP 1100 PE | limit 4.00 | qty 1200
    (1 lot) | stock stop above 1240.5 | max 5 trading days' (NO_ZONE: 'no 1h SL zone')."""
    exp = day_short(meta.expiry_date).upper()
    head = f"{meta.symbol} {exp} {plain_num(meta.strike)} {meta.option_type}".strip()
    block = v2t_block(signal)
    if block is not None:
        lot = meta.lot_size or (ctx.lot_size if ctx is not None else None)
        v2 = [" ".join(head.split()), f"limit {_p2(signal.ltp)}", *([f"qty {lot} (1 lot)"] if lot else [])]
        zone = v2_stop(block)
        if zone is None:
            v2.append("no 1h SL zone")
        else:
            side = "above" if str(meta.option_type).upper() == "PE" else "below"
            v2.append(f"stock stop {side} {price_plain(v2_far_edge(meta.option_type, *zone))}")
        v2.append(f"max {V2_HOLD_DAYS} trading days")
        return " | ".join(v2).replace("\n", " ").replace("\r", " ")[:COPY_TEXT_MAX]
    p = entry_plan(signal, meta, ctx)
    parts = [" ".join(head.split()), f"limit {_p2(p.entry)}"]
    if p.lot:
        parts.append(f"qty {p.lot} (1 lot)")
    if p.hold == "swing":
        parts.append(f"stock stop {price_plain(p.edge)}" if p.edge else "stock stop at the zone edge")
    elif p.sl is not None:
        parts.append(f"SL {_p2(p.sl)}")
    parts += [f"T1 {_p2(p.t1)}", f"T2 {_p2(p.t2)}"]
    text = " | ".join(parts).replace("\n", " ").replace("\r", " ")
    return text[:COPY_TEXT_MAX]


def copy_order_button(signal: Signal, meta: ContractMeta, ctx: SignalContext | None = None) -> dict[str, Any]:
    """The Bot API InlineKeyboardButton for the D79 "Copy order" (a copy_text button, Bot API 7.11)."""
    return {"text": COPY_ORDER_LABEL, "copy_text": {"text": copy_order_text(signal, meta, ctx)}}


def nse_url(symbol: Any) -> str:
    """The NSE F&O quote page of a stock (the "NSE F&O" / v3 "📈 Open contract" button)."""
    return f"https://www.nseindia.com/get-quotes/derivatives?symbol={quote(str(symbol), safe='')}"


def signal_buttons(meta: ContractMeta) -> list[list[dict[str, str]]]:
    """URL buttons under a ticket: TradingView chart (NSE:M_M style symbol) and the NSE F&O quote page."""
    sym = str(meta.symbol)
    tv = sym.replace("&", "_").replace("-", "_")
    return [
        [
            {
                "text": f"{ICON_CHART} Chart",
                "url": f"https://www.tradingview.com/chart/?symbol=NSE:{quote(tv, safe='')}",
            },
            {
                "text": "NSE F&O",
                "url": nse_url(sym),
            },
        ]
    ]


from gammad.v1.states import CLOSED_STATE_OF_KIND, V1R_EVENT_KINDS

_V1R_WORDS = {
    "V1R_STOP": "Stock stop hit", "V1R_CAP": "Premium loss limit reached",
    "V1R_NOPROG": "Closed after no progress", "V1R_TRAIL": "Trailing stop hit",
    "V1R_TARGET": "Target reached", "V1R_HALF": "Half booked",
    "V1R_EOD": "Day-end close", "V1R_SETTLED": "Closed at expiry settlement",
}

# ------------------------------------------------------------------------------------------ trade follow-ups (§7)
ICON_T1_HIT = E_T1  # T-162: ✅ means "check passed" in the v3 vocabulary, so a target hit carries its own emoji
ICON_T2_HIT = E_T2
ICON_SL_HIT = E_SL
ICON_STOCK_STOP = E_EDGE
ICON_TRAIL = "\N{LEFTWARDS ARROW WITH HOOK}\N{VARIATION SELECTOR-16}"
ICON_EXIT = "\N{BELL}"
ICON_ARMED = "\N{WARNING SIGN}\N{VARIATION SELECTOR-16}"
FOLLOWUP_KINDS: tuple[str, ...] = (
    *V1R_EVENT_KINDS,
    "T1", "T2", "TRAIL", "SL", "EOD", "DAY5", "SWING_ARMED", "SWING_STOP", "EOD_MISSED", "EXPIRY",
    "ZSL", "ZTRAIL",  # 0.8.4 (T-267): the V2 Scanner's stop exits (rules.ZONE_EVENT_KINDS = ZSL / ZTRAIL / DAY5 / EXPIRY)
    *SW_FOLLOWUP_KINDS,  # 0.8.9 T-419: a Still Watching trade's stop / target (not an exit) / trail / expiry-eve
)  # fmt: skip
PERSONAL_ONLY_KINDS = frozenset({"EOD_MISSED"})  # TRACKER §7: PERSONAL / dashboard only, never the group
# 0.8.4 (T-267, M4 / D249(4)): a V2 trail step (rules.ZSTEP) is NOT a follow-up kind -- trade_followup refuses it, so no
# group / client template can ever render one; its only text is `zone_step_card` (owner edition, PERSONAL, silent).
ZSTEP_KIND = ZSTEP
SETUP_CLOSED = "<i>Setup closed.</i>"


def _field(obj: Any, name: str, default: Any = None) -> Any:
    """A trade / event field from a Mapping (a DB row) or an object (rules.Position / rules.Event / a dataclass)."""
    if obj is None:
        return default
    if isinstance(obj, Mapping):
        v = obj.get(name, default)
    else:
        v = getattr(obj, name, default)
    return default if v is None else v


def _day_of(ts_ms: Any) -> str:
    return day_short(datetime.fromtimestamp(int(ts_ms) / 1000, IST).date()) if ts_ms else ""


EOD_DAY_END_FROM = time(15, 30)  # an EOD event at/after this IST clock came from close_day (15:35), not the 15:20 job


def _is_day_end(ts_ms: Any) -> bool:
    """T-092: trade_events has no reason column, so the EOD card reads the event's own IST clock (a re-send from
    the DB renders identically). No ts_ms -> False (the 15:20 card)."""
    return bool(ts_ms) and datetime.fromtimestamp(int(ts_ms) / 1000, IST).time() >= EOD_DAY_END_FROM


def trade_followup(kind: str, trade: Any, *, edition: str | None = None, event: Any = None) -> str:
    """The Telegram text of one trade follow-up (TRADE-TRACKER-SPEC §7), the D79 C "Sheet" style of
    messages/C-04*, C-05* and C-06*. Sent as a REPLY to the trade's entry message (router.send_followup); 0.8.2 T-214
    (D186(2)): standalone (no reply) when the trade has no entry message -- the headline names the contract (`SYM
    STRIKE OT`) and line 2 its expiry, so the text never relies on the quoted card.

    kind     one of FOLLOWUP_KINDS: T1 ("T1 hit: SL moved to entry on the whole lot": nothing is booked, D380 (2)
             X03), T2, TRAIL (stopped at entry after
             T1), SL, EOD (15:20 same-day exit; an event at/after 15:30 IST = close_day -> "DAY-END EXIT", T-092),
             DAY5 (swing day-5 exit), SWING_ARMED, SWING_STOP, EOD_MISSED (PERSONAL / dashboard only), EXPIRY (W15:
             a swing closed at 15:20 of its expiry day, before day 5). 0.8.4 (T-267), a V2 Scanner trade
             (trade["scanner"] == "V2T"): ZSL (a stock 5-min close beyond the far edge of the 1-hour stop zone,
             event["level"] = that edge, event["und_close"] = the stock close), ZTRAIL (the same on a trailed zone),
             DAY5 / EXPIRY (15:20; line 3 reads "V2 Scanner" instead of the hold). ZSTEP raises ValueError: a trail
             step is never a group follow-up (`zone_step_card`, owner PERSONAL only).
    trade    a `trades` row (Mapping) or an object with those attributes (TRACKER §5) PLUS `symbol` and `strike`
             (join them from the watchlist / signal; `security_id` is the fallback name). Read: kind ('E1'|'E2'),
             hold, option_type, expiry_date, entry, t1, t2, sl, lot_size, zone_edge, armed_date, entry_ts_ms,
             exit_px, exit_ts_ms; optional `e2_entry` (OWNER, an E1 trade: the premium of its E2 -> "From the
             E2 ₹x: +y%") and `e2_ts_ms`.
    event    a gammad.trade.rules.Event or a `trade_events` row (price, ts_ms, level, und_close); None -> the
             trade's exit_px / exit_ts_ms. The price is the triggering LTP (SWING_*: that day's option close).
    edition  default gammad.edition.edition(). CLIENT: an E2 trade raises OwnerOnlyError (E2 never reaches a
             client surface), no "E2" anywhere, no quadrant/DESK lines.
    Content (§7): contract, hold, entry -> level, % and ₹ per lot, what remains open. Never "BUY"/"SELL" (D79).
    T-162 (D129): the v3 headline -- "🎯 <b>T1 HIT · 🔻 SYM 300 PE</b>" (one bold span, so the title word keeps its
    trailing space) then "📅 29 Sep expiry · 🕒 13:45 IST · SAME-DAY[ · E1 entry]" -- and the vocabulary in the
    quote (💰 entry, 🎯 T1, 🏁 T2, 🛑 SL, 🧱 stock stop); the event time moved from the body to line 2 (the daily-close
    kinds SWING_ARMED / SWING_STOP carry their day in the body and no clock).
    LANE LOG A3 (0.8.9): that day is the daily CLOSE's, not the run's (the daily stop check runs 09:21 next morning):
    SWING_ARMED -> trade armed_date (else the event day); SWING_STOP -> trade last_close_date when the event has its
    price (that snapshot's close; else the event day). The CLIENT armed card reads "STOCK STOP SET" / "No exit yet:
    exits only if the stock closes back ..." (never read as a stop hit); the owner's words are unchanged. Wording and
    dates only: no V1 stop / exit rule changed (lane log A4).
    0.8.9 T-419 (D471 Q5 / Q6, D472): a Still Watching trade (trades.scanner SW / hold carry; line 3 reads "Still
    Watching") has SW_STOP (a 5-min stock close past the 1-hour zone's near edge = exit), SW_TARGET (the daily target
    reached: NOT an exit, the stop now trails), SW_TRAIL (a 5-min close past the trailing stop = exit) and SW_EVE (the
    expiry-eve exit); event level = the level crossed (else the trade's stop_* / target_level / trail_level)."""
    k = str(kind).upper()
    if k == ZSTEP_KIND:
        raise ValueError("ZSTEP is not a group follow-up (M4, D249(4)): render it with zone_step_card (owner PERSONAL)")
    if k not in FOLLOWUP_KINDS:
        raise ValueError(f"unknown follow-up kind {kind!r}; expected one of {FOLLOWUP_KINDS}")
    client = _is_client(edition)
    tkind = str(_field(trade, "kind", "E1"))
    if client and tkind != "E1":
        raise edition_mod.OwnerOnlyError(f"a {tkind} trade follow-up is owner-edition only (D81, D86)")
    v2 = str(_field(trade, "scanner", SCANNER_V1)) == SCANNER_V2  # 0.8.4: a V2 Scanner trade (zone family)
    if client and v2:  # 0.8.5 (D287(6), PLAN-D287 AM7 / I9): the client edition has no V2 Scanner trade
        raise edition_mod.OwnerOnlyError("a V2 Scanner trade follow-up is owner-edition only (D287(6))")

    ot = str(_field(trade, "option_type", ""))
    hold = str(_field(trade, "hold", "intraday"))
    # 0.8.9 T-419: a Still Watching trade (trades.scanner SW / hold carry) reads the list's name, never the hold word
    sw = str(_field(trade, "scanner", "")) == SCANNER_SW or hold.lower() == SW_HOLD
    style = scanner_title(SCANNER_V2) if v2 else STILL_WATCHING if sw else _hold_word(hold)
    entry = float(_field(trade, "entry", 0.0))
    t1, t2 = _num(_field(trade, "t1")), _num(_field(trade, "t2"))
    lot = _num(_field(trade, "lot_size"))
    edge = _num(_field(trade, "zone_edge"))
    price = _num(_field(event, "price", _field(trade, "exit_px")))
    ts_ms = _field(event, "ts_ms", _field(trade, "exit_ts_ms", 0))
    und_close = _num(_field(event, "und_close"))
    at = ist_hm(int(ts_ms)) if ts_ms else ""
    arm, back = ("below", "above") if ot.upper() == "PE" else ("above", "below")
    edge_txt = level_text(edge) if edge else f"the {zone_word(ot)} edge"

    def pct_of(px: float | None) -> float | None:
        return None if px is None or entry <= 0 else _up(entry, px)

    def result(px: float | None, *, mark: bool = True) -> str:
        """'🟢 +20.0% · +₹95 a lot' (₹ per lot when the lot size is known; T-212: 🟢 gain / 🔴 loss, none at 0)."""
        if px is None or entry <= 0:
            return "exit price not known"
        parts = [f"<b>{_pct(_up(entry, px), 1)}</b>"]
        if lot:
            parts.append(f"{_inr((px - entry) * lot, sign=True)} a lot")
        marker = sign_marker(_up(entry, px)) if mark else ""
        return (f"{marker} " if marker else "") + DOT.join(parts)

    def arrow(label: str, px: float | None) -> str:
        mark = {"T1": E_T1, "T2": E_T2, "SL": E_SL}.get(label)
        name = f"{mark} {label}" if mark else label.capitalize()
        where = f"{name} <b>{rupees(px)}</b>" if px is not None else name
        return f"{E_PRICE} Entry {rupees(entry)} {ARROW} {where}"

    def from_e2(px: float | None) -> list[str]:
        e2 = _num(_field(trade, "e2_entry"))
        if client or tkind != "E1" or e2 is None or e2 <= 0 or px is None:
            return []
        return [f"From the E2 {rupees(e2)}: {_pct(_up(e2, px))}"]

    owner_kind = "" if client or v2 else f"{tkind} entry"  # a V2 trade is one E1 (the trigger close)
    hold_part = style
    closed = True
    still_open: list[str] = []  # T-212 style B: the STILL OPEN section (🟡)
    timed = k not in ("SWING_ARMED", "SWING_STOP", "EOD_MISSED")  # daily-close / start-up kinds: no clock
    if k in V1R_EVENT_KINDS:
        icon, title, badge = ICON_EXIT, _V1R_WORDS[k].upper(), sign_marker(pct_of(price))
        body = [arrow("EXIT" if k != "V1R_HALF" else "BOOKED", price)]
        if k == "V1R_HALF":
            closed = False
            body.append("Half the position booked; the remaining half stays open.")
            still_open = ["Remaining half follows the trailing stop."]
        else:
            body += [result(price), "Position closed: nothing left open."]
    elif k == "T1":
        state = str(_field(trade, "state", ""))
        exit_ts, event_ts = _field(trade, "exit_ts_ms"), _field(event, "ts_ms")
        if (state.startswith("CLOSED_") and exit_ts is not None and event_ts is not None
                and int(exit_ts) == int(event_ts)):
            return ""
        level = _num(_field(event, "level"))
        level = t1 if level is None else level
        fill = _num(_field(event, "price"))
        fill = level if fill is None else fill
        icon, title, badge = ICON_T1_HIT, "T1 HIT", M_PASS
        # D380 (2) X03: nothing is booked at T1 (rules.on_tick moves the SL of the WHOLE lot to the entry)
        body = [arrow("T1", level)]
        if fill is not None and level is not None and abs(fill - level) >= 0.005:
            body.append(f"Filled <b>{rupees(fill)}</b>")
        body += [result(fill), f"whole lot still open, SL moved to entry {rupees(entry)}"]
        body += from_e2(fill)
        if state in ("", "OPEN", "T1_TRAIL"):
            still_open = [f"{E_T2} T2 {rupees(t2)}" if t2 else f"{E_T2} T2"]
        closed = False
    elif k == "T2":
        level = _num(_field(event, "level"))
        level = t2 if level is None else level
        fill = _num(_field(event, "price"))
        fill = level if fill is None else fill
        icon, title, badge = ICON_T2_HIT, "T2 HIT", M_PASS
        body = [arrow("T2", level)]
        if fill is not None and level is not None and abs(fill - level) >= 0.005:
            body.append(f"Filled <b>{rupees(fill)}</b>")
        body += [result(fill),
                 f"{E_T1} T1 {rupees(t1)} and {E_T2} T2 {rupees(t2)} reached: nothing left open"
                 if t1 is not None and t2 is not None else "final target: nothing left open"]
        body += from_e2(fill)
    elif k == "TRAIL":
        icon, title, badge = ICON_TRAIL, "STOPPED AT ENTRY", sign_marker(pct_of(price))
        body = [arrow("EXIT", price), result(price),  # D380 (2) X03: no part was booked at T1
                f"back to the entry after T1 {rupees(t1)}" if t1 else "back to the entry after T1",
                "The whole lot closed at the entry; nothing left open"]  # fmt: skip
        body += from_e2(price)
    elif k == "SL":
        icon, title, badge = ICON_SL_HIT, "SL HIT", M_FAIL
        body = [arrow("SL", price), result(price), "base low broken"]
        body += from_e2(price)
    elif k == "EOD":
        badge = sign_marker(pct_of(price))
        if _is_day_end(ts_ms):  # close_day (15:35, D98(1)) closed it, not the 15:20 job (T-092)
            icon, title = ICON_EXIT, "DAY-END EXIT"
            body = [arrow("EXIT", price), result(price),
                    "closed at day end at the last price seen: nothing left open"]  # fmt: skip
        else:
            icon, title = ICON_EXIT, f"{at} EXIT" if at else "SAME-DAY EXIT"
            body = [arrow("EXIT", price), result(price), "same-day close: nothing left open"]
        body += from_e2(price)
    elif k == "DAY5":
        icon, title, badge = ICON_EXIT, "DAY-5 EXIT", sign_marker(pct_of(price))
        why = (f"day {V2_HOLD_DAYS} of {V2_HOLD_DAYS}, closed at {V2_EXIT_HM}: nothing left open" if v2
               else "swing hold over (day 5): nothing left open")  # fmt: skip
        body = [arrow("EXIT", price), result(price), why]
        body += from_e2(price)
    elif k == "EXPIRY":
        icon, title, badge = ICON_EXIT, "EXPIRY EXIT", sign_marker(pct_of(price))
        why = (f"expiry day, closed at {V2_EXIT_HM}: nothing left open" if v2
               else "expiry day, before day 5: nothing left open")  # fmt: skip
        body = [arrow("EXIT", price), result(price), why]
        body += from_e2(price)
    elif k in ("ZSL", "ZTRAIL"):  # 0.8.4 (T-267): a stock 5-min close beyond the FAR edge of the 1-hour stop zone
        trail = k == "ZTRAIL"
        icon, title, badge = (ICON_TRAIL, "TRAILED STOP HIT", sign_marker(pct_of(price))) if trail else (
            ICON_STOCK_STOP, "STOP HIT", sign_marker(pct_of(price)))  # fmt: skip
        level = _num(_field(event, "level"))
        if level is None:  # the far edge of the stored (newest) stop zone, as tracker.event_level derives it
            level = _num(_field(trade, "stop_top" if ot.upper() == "PE" else "stop_bottom"))
        side = "above" if ot.upper() == "PE" else "below"
        where = f"{side} {edge_text(level)}" if level is not None else f"{side} the stop zone"
        close = f"Stock 5-min close {edge_text(und_close)}, {where}" if und_close is not None else f"Stock closed {where}"
        zone = "trailed 1-hour stop zone" if trail else "1-hour stop zone"
        body = [f"{E_EDGE} {close}", arrow("EXIT", price), result(price), f"{zone} broken: nothing left open"]
    elif k in SW_FOLLOWUP_KINDS:  # 0.8.9 T-419 (D471 Q5 / Q6, D472 X9A): a Still Watching trade's events
        level = _num(_field(event, "level"))
        close = f"Stock 5-min close {edge_text(und_close)}" if und_close is not None else "Stock 5-min close"
        if k == SW_STOP:
            if level is None:  # the zone's NEAR edge (a PE's zone sits above the stock: near = its bottom)
                level = _num(_field(trade, "stop_bottom" if ot.upper() == "PE" else "stop_top"))
            icon, title, badge = ICON_STOCK_STOP, "STOP HIT", sign_marker(pct_of(price))
            where = f"past the stop {edge_text(level)}" if level is not None else "past the stop"
            body = [f"{E_EDGE} {close}, {where}", arrow("EXIT", price), result(price), "stop hit: nothing left open"]
        elif k == SW_TRAIL:
            if level is None:
                level = _num(_field(trade, "trail_level"))
            icon, title, badge = ICON_TRAIL, "TRAILING STOP HIT", sign_marker(pct_of(price))
            where = f"past the trailing stop {edge_text(level)}" if level is not None else "past the trailing stop"
            body = [f"{E_EDGE} {close}, {where}", arrow("EXIT", price), result(price),
                    "trailing stop hit: nothing left open"]  # fmt: skip
        elif k == SW_EVE:
            icon, title, badge = ICON_EXIT, "EXPIRY-EVE EXIT", sign_marker(pct_of(price))
            eve_why = (f"day before expiry, closed at {at}: nothing left open" if at
                       else "day before expiry: nothing left open")
            body = [arrow("EXIT", price), result(price), eve_why]
        else:  # SW_TARGET: NOT an exit (D472 X9A) -- the stop starts trailing the stock from here
            if level is None:
                level = _num(_field(trade, "target_level"))
            icon, title, badge = E_T1, "TRAILING", M_PASS
            where = f"Stock reached the target {edge_text(level)}" if level is not None else "Stock reached the target"
            body = [f"{E_T1} {where}", "Not an exit: the stop now trails the stock"]
            if price is not None:
                body.append(f"{E_PRICE} Option {rupees(price)}{DOT}{result(price, mark=False)}")
            still_open = ["Exits if a 5-min candle closes past the trailing stop"]
            closed = False
    elif k == "SWING_ARMED":
        # LANE LOG A3 (b): the client words must not read as "stop hit" (owner keeps his words); A3 (a): the day printed is
        # the daily CLOSE's (trades.armed_date, the tracker writes the close's day), not the 09:21 run day of the event
        icon, title, badge = (ICON_STOCK_STOP, "STOCK STOP SET", M_OPEN) if client else (
            ICON_ARMED, "STOCK STOP ARMED", M_OPEN)  # fmt: skip
        armed_on = _field(trade, "armed_date")
        day = day_short(str(armed_on)) if armed_on else _day_of(ts_ms)
        close = f"close {level_text(und_close)}" if und_close is not None else "close"
        exits = (f"No exit yet: exits only if the stock closes back {back} {edge_txt} on a daily close" if client
                 else f"Exits on a daily close back {back} {edge_txt}")  # fmt: skip
        body = [f"{E_EDGE} {day + ': ' if day else ''}{close} {arm} {edge_txt}", exits]
        if price is not None:
            body.append(f"{E_PRICE} Option close {rupees(price)}{DOT}{result(price, mark=False)}")
        still_open = [DOT.join(x for x in (f"{E_T1} T1 {rupees(t1)}" if t1 else f"{E_T1} T1",
                                           f"{E_T2} T2 {rupees(t2)}" if t2 else f"{E_T2} T2") if x)]  # fmt: skip
        closed = False
    elif k == "SWING_STOP":
        icon, title, badge = ICON_STOCK_STOP, "STOP HIT", sign_marker(pct_of(price))
        hold_part = f"{style} stock stop"
        armed = _field(trade, "armed_date")
        # LANE LOG A3 (a): the close the stop used is the option-close snapshot's day (trades.last_close_date; the event
        # price IS that snapshot's close, so it is trusted only with a price); else the event day
        snap_day = _field(trade, "last_close_date")
        day = day_short(str(snap_day)) if snap_day and price is not None else _day_of(ts_ms)
        close = f"close {level_text(und_close)}" if und_close is not None else "close"
        body = [f"{E_EDGE} Armed {disp_day(str(armed))}: a close {arm} {edge_txt}" if armed
                else f"{E_EDGE} Armed: a close {arm} {edge_txt}",
                f"{day + ': ' if day else ''}{close}, back {back}",
                f"{E_PRICE} Exit at the option close {rupees(price)}" if price is not None
                else f"{E_PRICE} Exit at the option close",
                result(price)]  # fmt: skip
        body += from_e2(price)
    else:  # EOD_MISSED (PERSONAL)
        icon, title, badge = ICON_ARMED, "EOD EXIT MISSED", M_OPEN
        entry_day = _day_of(_field(trade, "entry_ts_ms"))
        body = [f"{E_PRICE} Entry {rupees(entry)}" + (f" on {entry_day}" if entry_day else ""),
                "The service was not running at the same-day exit, so it was never sent",
                "Closed at start-up as EOD missed" + (f" at {rupees(price)}" if price is not None else "")]  # fmt: skip
    expiry = _field(trade, "expiry_date", "")
    sub = DOT.join(x for x in (f"{E_TIME} {at} IST" if timed and at else "", hold_part, owner_kind) if x)
    lines = [
        badge_line(icon, title, badge),
        contract_head(_field(trade, "symbol", _field(trade, "security_id", "")), _field(trade, "strike", 0), ot,
                      expiry, _ms_day(ts_ms)),
        sub,
        DIV,
        *body,
    ]  # fmt: skip
    if still_open:
        lines += [DIV, f"{section('STILL OPEN')} {M_OPEN}", *still_open]
    if closed:
        lines += [DIV, SETUP_CLOSED]
    elif k == "SWING_ARMED":
        lines += ["", "<i>No exit today.</i>"]
    elif k == SW_TARGET:  # T-419: not an exit (the trade stays open, the stop trails)
        lines += ["", "<i>No exit yet.</i>"]
    return "\n".join(lines)


def zone_step_card(trade: Any, step: Any, *, edition: str | None = None) -> str:
    """0.8.4 (T-267, M4 / D249(4)): the owner's PERSONAL (silent) note of a V2 trail step -- the stop moved to a new
    1-hour zone. OWNER edition only (the client edition raises OwnerOnlyError: no trail-step message ever reaches a
    client or the group; the dashboard shows the steps). `step` = the tracker's ZSTEP dict / a trade_zone_steps row
    (top, bottom, confirmed_ms, level = the new far edge, h1_close, seq); `trade` = the trades row + symbol / strike.
    "🧱 <b>STOP TRAILED</b>   🟡" / contract · 📅 expiry / "🕒 13:15 IST · V2 Scanner · step 2" / ━ / the new zone,
    the new exit sentence, the 1-hour close / The stop never moves back."""
    if _is_client(edition):
        raise edition_mod.OwnerOnlyError("a V2 trail step is owner PERSONAL only, never a client message (M4, D249(4))")
    ot = str(_field(trade, "option_type", ""))
    top, bottom = _num(_field(step, "top")), _num(_field(step, "bottom"))
    ts_ms = _field(step, "confirmed_ms", _field(step, "ts_ms", 0))
    seq = _field(step, "seq")
    sub = DOT.join(x for x in (f"{E_TIME} {ist_hm(int(ts_ms))} IST" if ts_ms else "", scanner_title(SCANNER_V2),
                               f"step {int(seq)}" if seq is not None else "") if x)  # fmt: skip
    level = _num(_field(step, "level"))
    if level is None and top is not None and bottom is not None:
        level = v2_far_edge(ot, min(top, bottom), max(top, bottom))
    body = []
    if top is not None and bottom is not None:
        body.append(f"{E_EDGE} New 1-hour {v2_zone_word(ot)}{BLEND_GAP}"
                    f"<b>{edge_text(min(top, bottom))} {EN_DASH} {edge_text(max(top, bottom))}</b>")  # fmt: skip
    if level is not None:
        body.append(f"<b>{v2_stop_sentence(ot, level)}</b>")
    h1 = _num(_field(step, "h1_close"))
    if h1 is not None:
        body.append(f"1-hour close {edge_text(h1)}")
    lines = [badge_line(E_EDGE, "STOP TRAILED", M_OPEN),
             contract_head(_field(trade, "symbol", _field(trade, "security_id", "")), _field(trade, "strike", 0), ot,
                           _field(trade, "expiry_date", ""), _ms_day(ts_ms)),
             sub, DIV, *body, "", "<i>The stop never moves back.</i>"]  # fmt: skip
    return "\n".join(lines)


# ------------------------------------------------------------------------------------------ stored signals
def flatten_signal_row(row: Mapping[str, Any]) -> dict[str, Any]:
    """A `signals` row (payload_json or parsed payload) as one flat dict; DB columns win over the payload."""
    out: dict[str, Any] = {}
    payload = row.get("payload")
    if payload is None and row.get("payload_json"):
        try:
            payload = json.loads(str(row["payload_json"]))
        except ValueError:
            payload = None
    if isinstance(payload, Mapping):
        out.update(payload)
    out.update({k: v for k, v in row.items() if k not in ("payload", "payload_json")})
    if out.get("ts_ms") is not None:
        out["time_ist"] = ist_hms(int(out["ts_ms"]))
    return out


def _signal_from_row(r: Mapping[str, Any]) -> tuple[ContractMeta, Signal]:
    lot = _num(r.get("lot_size"))
    meta = ContractMeta(
        symbol=str(r.get("symbol") or r.get("security_id")),
        strike=float(r.get("strike") or 0.0),
        option_type=str(r.get("option_type") or ""),
        list_name=str(r.get("list_name") or ""),
        expiry_date=str(r.get("expiry_date") or ""),
        tag=r.get("tag"),
        zone_dist_pct=_num(r.get("zone_dist_pct")),
        zone_edge=_num(r.get("zone_edge")),
        lot_size=None if lot is None else int(lot),
        **_supported(
            ContractMeta,
            tick_size=_num(r.get("tick_size")),
            carry_state=r.get("carry_state"),
            carry_k=None if _num(r.get("carry_k")) is None else int(r["carry_k"]),
            carried=bool(r.get("carried")) or r.get("tag") == TAG_CARRY,
        ),
    )
    sig = Signal(
        kind=str(r["kind"]),  # type: ignore[arg-type]
        strong=bool(r.get("strong")),
        ts_ms=int(r.get("ts_ms") or 0),
        security_id=int(r.get("security_id") or 0),
        ltp=float(r.get("ltp") or 0.0),
        oi_drop_pct=float(r.get("oi_drop_pct") or 0.0),
        vol_ratio=float(r.get("vol_ratio") or 0.0),
        consol_lo=float(r.get("consol_lo") or 0.0),
        consol_hi=float(r.get("consol_hi") or 0.0),
        consol_mid=float(r.get("consol_mid") or 0.0),
        sl=float(r.get("sl") or 0.0),
        reason=str(r.get("reason") or ""),
        latency_ms=int(r.get("latency_ms") or 0),
    )
    return meta, sig


def context_from_row(r: Mapping[str, Any]) -> SignalContext | None:
    """The SignalContext of a stored signal: `ctx_json` (SignalContext.to_dict() as JSON) or a parsed `ctx`
    mapping; else a bare context from a `hold` column; else None."""
    raw = r.get("ctx_json") if r.get("ctx_json") is not None else r.get("ctx")
    d: Any = raw
    if isinstance(raw, str | bytes):
        try:
            d = json.loads(raw)
        except ValueError:
            d = None
    if isinstance(d, Mapping) and d.get("hold") in ("swing", "intraday"):
        zeros = _num(d.get("zero_vol_bars"))
        lot = _num(d.get("lot_size"))
        return SignalContext(
            hold=d["hold"],
            spot=_num(d.get("spot")),
            zone_edge=_num(d.get("zone_edge")),
            zone_gap_pct=_num(d.get("zone_gap_pct")),
            oi_peak=_num(d.get("oi_peak")),
            oi_now=_num(d.get("oi_now")),
            avg_vol_lots=_num(d.get("avg_vol_lots")),
            zero_vol_bars=None if zeros is None else int(zeros),
            lot_size=None if lot is None else int(lot),
            invalidation=_num(d.get("invalidation")),
            gate_reasons=tuple(str(x) for x in d.get("gate_reasons") or ()),
            gate_unknown=tuple(str(x) for x in d.get("gate_unknown") or ()),
            # T-282 (b): the zone limit the signal was checked with (stored by both editions since T-305, D278(6))
            **_supported(SignalContext, quadrant=d.get("quadrant"), zone_limit_pct=_num(d.get("zone_limit_pct"))),
        )
    if r.get("hold") in ("swing", "intraday"):
        return SignalContext(hold=r["hold"])
    return None


def _supported(cls: type, **values: Any) -> dict[str, Any]:
    """The keyword arguments `cls` (a dataclass) declares, among `values` with a non-None value: S1.3.0 adds
    ContractMeta.tick_size and SignalContext.quadrant; this module reads them without requiring them."""
    fields = getattr(cls, "__dataclass_fields__", {})
    return {k: v for k, v in values.items() if k in fields and v is not None}


def signal_share(signal_row: Mapping[str, Any], *, edition: str | None = None, desk: bool = False) -> str:
    """A recorded signal re-sent by hand: the same card (per edition), its time carries the date. An E1 row with a
    stored tier (schema v7 `tier`) renders that tier's card; no tier = the untiered E1 card. `desk` = the owner DESK
    section (T-178: the router asks for it on a PERSONAL send only; a GROUP send never carries it)."""
    r = flatten_signal_row(signal_row)
    meta, sig = _signal_from_row(r)
    day = str(r.get("trade_date") or "")
    when = f"{disp_day(day)} {ist_hm(sig.ts_ms)}" if day else ist_hm(sig.ts_ms)
    tier = str(r.get("tier") or "")
    scanner = str(r.get("scanner") or SCANNER_V1)  # 0.8.1: the stored row's own name on an ENTRY headline (D159(2))
    snap = r.get("snapshot")
    if scanner == SCANNER_V2 and isinstance(snap, Mapping):  # 0.8.4: the V2 card reads its stop zone from the snapshot
        sig = replace(sig, snapshot=dict(snap))
    return _sheet(meta, sig, context_from_row(r), when, edition=edition, desk=desk,
                  tier=tier if sig.kind in ("E1", "E2") and tier in TIER_LABEL else None, scanner=scanner)  # fmt: skip


def signal_line(signal_row: Mapping[str, Any]) -> str:
    """One stored signal on one line: <b>MARUTI 12000 PE</b> ★ · E1 13:42 · ₹55.20 · OI −10.4%."""
    r = flatten_signal_row(signal_row)
    star = f" {ICON_STRONG}" if r.get("strong") else ""
    parts = [
        f"<b>{_contract_text(r.get('symbol') or r.get('security_id'), r.get('strike'), r.get('option_type'))}</b>{star}",
        f"{esc(r.get('kind'))} {ist_hm(int(r.get('ts_ms') or 0))}",
    ]
    if r.get("ltp") is not None:
        parts.append(rupees(r["ltp"]))
    if r.get("oi_drop_pct") is not None:
        parts.append(f"OI {pct_drop(r['oi_drop_pct'])}")
    return DOT.join(parts)


def _chains(rows: Iterable[Mapping[str, Any]]) -> list[list[dict[str, Any]]]:
    """Signals grouped by contract (security_id), contracts in first-signal order, each chain in time order."""
    flat = sorted(
        (flatten_signal_row(r) for r in rows), key=lambda r: (int(r.get("ts_ms") or 0), r.get("id") or 0)
    )
    by: dict[Any, list[dict[str, Any]]] = {}
    for r in flat:
        key = r.get("security_id") or (
            r.get("symbol"),
            r.get("strike"),
            r.get("option_type"),
            r.get("expiry_date"),
        )
        by.setdefault(key, []).append(r)
    return list(by.values())


def _strong_shown(r: Mapping[str, Any], *, client: bool) -> bool:
    """Does a stored signal row show its STRONG mark? Yes when signals.strong, except a V2 Scanner row on a client
    surface (D257: the client's V2 enters only under a 12 % unwind and never reads STRONG). V1 is unchanged."""
    return bool(r.get("strong")) and not (client and str(r.get("scanner") or "") == SCANNER_V2)


def _chain_head(chain: Sequence[Mapping[str, Any]], trade_date: str | None = None, *, client: bool = False) -> str:
    """'<b>BDL 1100 PE</b> ★ · 📅 29 Sep 🟣' (T-212: every contract shows its own expiry, D182(1), catalogue §4.2)."""
    s0 = chain[0]
    star = f" {ICON_STRONG}" if any(_strong_shown(x, client=client) for x in chain) else ""
    name = _contract_text(s0.get("symbol") or s0.get("security_id"), s0.get("strike"), s0.get("option_type"))
    exp = s0.get("expiry_date")
    when = f"{DOT}{E_EXPIRY} {disp_day(str(exp))}{expiry_mark(exp, trade_date)}" if exp else ""
    return f"<b>{name}</b>{star}{when}"


def _client_label(x: Mapping[str, Any]) -> str:
    """T-212 (catalogue §4.6): the client table names a row by its tier (TIER_LABEL: ENTRY / NEAR ENTRY / WATCH
    ONLY, D273), never `E1`."""
    return TIER_LABEL.get(str(x.get("tier") or ""), "ENTRY")


def _chain_block(chain: Sequence[Mapping[str, Any]], trade_date: str | None = None, *, client: bool = False) -> str:
    lines = [_chain_head(chain, trade_date, client=client)]
    for x in chain:
        kind = str(x.get("kind") or "")
        if client:  # 0.8.5 (PLAN-D287 I4 / I9): the client layout never depends on the stored kind (E1 / tiered E2)
            what = e1_path("E1", x.get("reason"))
        else:
            what = e1_path(kind, x.get("reason")) if kind == "E1" else f"OI {pct_drop(x.get('oi_drop_pct') or 0.0)}"
        if client:
            label = _client_label(x)
            label = f"{E_ENTRY if label == 'ENTRY' else E_WATCH} {label}"
        else:
            label = esc(kind)
        parts = [f"{label} {ist_hm(int(x.get('ts_ms') or 0))}", rupees(x.get("ltp") or 0.0)]
        if what:
            parts.append(what)
        lines.append(DOT.join(parts))
    return "\n".join(lines)


def _is_client_entry_row(r: Mapping[str, Any]) -> bool:
    """A V1 row a CLIENT surface may show as an entry / watch card: an E1 (a client store's entry, D288), or a TIERED E2
    (0.8.5 D287(1): the owner store's group entry, rendered in the client layout, never with its kind)."""
    kind = str(r.get("kind"))
    return kind == "E1" or (kind == "E2" and str(r.get("tier") or "") in TIER_LABEL)


def _visible_rows(rows: Iterable[Mapping[str, Any]], edition: str | None) -> list[Mapping[str, Any]]:
    """CLIENT layout (D81): V1 entry rows only (`_is_client_entry_row`) -- defence in depth, the client DB never holds
    an E2; the owner's GROUP table (rendered in the client layout) shows its tiered E2s, the group's entries. 0.8.5
    (D287(6), PLAN-D287 AM7 / I7): never a V2 Scanner row (a 0.8.4 client store's row, or one handed over by mistake)."""
    rows = list(rows)
    if not _is_client(edition):
        return rows
    return [r for r in rows if _is_client_entry_row(r) and str(r.get("scanner") or SCANNER_V1) != SCANNER_V2]


def _kind_counts(e2: int, e1: int, edition: str | None, *, bold: bool = False) -> str:
    b = (lambda n: f"<b>{n}</b>") if bold else str
    if _is_client(edition):
        return f"{b(e1)} {'entry' if e1 == 1 else 'entries'}"
    return f"{b(e2)} E2{DOT}{b(e1)} E1"


def signals_table(
    trade_date: str, rows: Sequence[Mapping[str, Any]], *, edition: str | None = None
) -> list[str]:
    """The day's signals as contract chains (B): each E2 followed by the E1 that came after it. CLIENT: E1 rows
    only, counted as entries (a watch-tier row is counted apart and named by its tier, never `E1`, T-212).
    T-212 style B: "🗒️ <b>SIGNALS</b>" / "📅 <b>Mon 28 Sep</b> · counts" / the ★ legend / ━ divider / the chains, each
    head with its contract's own expiry (D182(1)), one blank line between two chains."""
    client = _is_client(edition)
    chains = _chains(_visible_rows(rows, edition))
    date_line = f"{E_EXPIRY} <b>{disp_weekday(trade_date)}</b>"
    if not chains:
        return [f"{E_TABLE} <b>SIGNALS</b>\n{date_line}\n{DIV}\nNo signals."]
    kinds = [str(x.get("kind")) for c in chains for x in c]
    if client:
        n_watch = sum(1 for c in chains for x in c if _client_label(x) != "ENTRY")
        counts = _kind_counts(0, len(kinds) - n_watch, edition, bold=True)  # 0.8.5: every visible row, any stored kind
        for code in ("STRONG_WATCHLIST", "WATCHLIST"):  # D273: each non-entry tier counted under its own name
            n = sum(1 for c in chains for x in c if str(x.get("tier") or "") == code)
            counts += f"{DOT}<b>{n}</b> {TIER_LABEL[code].lower()}" if n else ""
    else:
        counts = _kind_counts(kinds.count("E2"), kinds.count("E1"), edition, bold=True)
    counts += f"{DOT}{_plural(len(chains), 'contract')}"
    strong = any(_strong_shown(x, client=client) for c in chains for x in c)
    legend = [f"<i>{ICON_STRONG} = STRONG OI drop</i>"] if strong else []

    def render(chunk: Sequence[str], i: int, n: int) -> str:
        return "\n".join([f"{E_TABLE} <b>SIGNALS</b>{_part_tag(i, n)}", f"{date_line}{DOT}{counts}", *legend, DIV,
                          "\n\n".join(chunk)])  # fmt: skip

    return split_by_length([_chain_block(c, trade_date, client=client) for c in chains], render)


# ------------------------------------------------------------------------------------------ watchlists
def _tag_key(r: Mapping[str, Any]) -> tuple[int, str, float]:
    tag = str(r.get("tag") or "")
    return (0 if tag else 1, tag, float(r.get("strike") or 0.0))


def _is_pe(list_name: str, rows: Sequence[Mapping[str, Any]]) -> bool:
    name = str(list_name).upper()
    if name.endswith(("PE", "CE")):
        return name.endswith("PE")
    return bool(rows) and str(rows[0].get("option_type") or "").upper() == "PE"


# build_watchlists run_id: watchlists-<date>-<kind>-<YYYYMMDD>T<HHMMSS><ffffff> (the build's start, IST)
_BUILD_HM = re.compile(r"-\d{8}T(\d{2})(\d{2})\d*$")
MARKET_OPEN_HM = "09:15"  # IST; a chain fetched earlier holds the chain's last price = the previous close
NO_PRICE = chr(0x2014)  # EM DASH: a strike with no price ('n/a' would collide with the '/' separator)


def _chain_hm(raw: Any) -> str | None:
    """HH:MM (IST) of watchlist.chain_ltp_at (schema v6, T-107); None when absent (pre-v6 row) or unreadable."""
    if not raw:
        return None
    try:
        at = datetime.fromisoformat(str(raw))
    except ValueError:
        return None
    if at.tzinfo is not None:
        at = at.astimezone(IST)
    return f"{at:%H:%M}"


def build_hm(row: Mapping[str, Any]) -> str:
    """The label of a stored build price: '09:23' = the IST time the option chain that priced the row was fetched
    (watchlist.chain_ltp_at, T-107: a 09:20 build spends minutes on chain calls); a pre-v6 row without it falls back
    to the IST start of the list build that wrote it (its run_id). 'prev close' when that time is before 09:15
    (pre-market: the chain's last price is the previous session's close); 'build' when neither is readable."""
    hm = _chain_hm(row.get("chain_ltp_at"))
    if hm is None:
        m = _BUILD_HM.search(str(row.get("run_id") or ""))
        if not m:
            return "build"
        hm = f"{m[1]}:{m[2]}"
    return "prev close" if hm < MARKET_OPEN_HM else hm


def premium(row: Mapping[str, Any]) -> tuple[float | None, str | None]:
    """T-053: (price, when). The live LTP when present (when None), else the option-chain LTP the list build
    stored (watchlist.chain_ltp; when = build_hm), else (None, None)."""
    live = _num(row.get("ltp"))
    if live is not None:
        return live, None
    stored = _num(row.get("chain_ltp"))
    if stored is not None and stored > 0:
        return stored, build_hm(row)
    return None, None


def _list_rank(list_name: str) -> tuple[int, str]:
    return (LIST_ORDER.index(list_name) if list_name in LIST_ORDER else len(LIST_ORDER), list_name)


def list_post_order(keys: Iterable[tuple[str, str]]) -> list[tuple[str, str]]:
    """The 09:20 posts' order (T-154, D126): (list_name, expiry_date) keys by LIST_ORDER, then expiry ascending; an
    unknown list name goes last. The router posts one list_post per key in this order."""
    return sorted(keys, key=lambda k: (*_list_rank(str(k[0])), str(k[1])))


# ------------------------------------------------------------------------------------------ v3 list posts (T-162 T3)
LIST_ROW_INDENT = " " * 6  # the gap line under a numbered row (candidate-2.source.html)


def weekday_day(iso: str | date) -> str:
    """'Mon 28 Sep'. Returns the ESCAPED input if it is not an ISO date."""
    try:
        d = iso if isinstance(iso, date) else date.fromisoformat(str(iso))
    except ValueError:
        return esc(iso)
    return f"{d:%a} {d.day} {d:%b}"


def nearest_rows(rows: Iterable[Mapping[str, Any]]) -> list[Mapping[str, Any]]:
    """Contract rows nearest the zone first (D129 T3): |zone_dist_pct| ascending (a row with no distance last), then
    the symbol, HOI-1 before HOI-2. The absolute value: a stock just through its edge is as near as one just short."""

    def key(r: Mapping[str, Any]) -> tuple[bool, float, str, tuple[int, str, float]]:
        gap = zone_dist_pct(r)
        return (gap is None, abs(gap) if gap is not None else 0.0, str(r.get("symbol") or ""), _tag_key(r))

    return sorted(rows, key=key)


def _premium_phrase(rows: Sequence[Mapping[str, Any]]) -> tuple[str | None, bool]:
    """(the header's premium phrase, label every price) over the rows SHOWN (T-053 / T-107 `premium`): build prices of
    one chain time -> "premiums at the 09:20 build"; of several -> "premiums at the 09:21–09:25 build"; all the
    previous close -> "premiums = previous close"; all live -> "live premiums"; live and build mixed (or a pre-market
    price among timed ones) -> (None, True): each price carries its own "(live)" / "(09:20)"; no price -> (None,
    False)."""
    whens = {w for v, w in (premium(r) for r in rows) if v is not None}
    if not whens:
        return None, False
    if whens == {None}:
        return "live premiums", False
    if None in whens:
        return None, True
    if whens == {"prev close"}:
        return "premiums = previous close", False
    if whens == {"build"}:
        return "premiums at the list build", False
    if {"prev close", "build"} & whens:
        return None, True
    times = sorted(str(w) for w in whens)
    span = times[0] if len(times) == 1 else f"{times[0]}{EN_DASH}{times[-1]}"
    return f"premiums at the {span} build", False


def _list_row(i: int, r: Mapping[str, Any], *, priced: bool, labelled: bool) -> str:
    """T-212 style B: '1. <b>JIOFIN</b> 220 PE · 29 Sep' / '      ₹0.52 · 0.0% above 225.1' -- the contract with ITS
    OWN expiry on line 1 (D182(1), catalogue §4.2), the premium moved to line 2 so a long name still fits. The symbol
    alone is bold (`<b>SYM</b> STRIKE OT`): tools/group_client_parity.py LIST_ROW reads the rows in that shape."""
    ot = str(r.get("option_type") or "").upper()
    name = f"<b>{esc(r.get('symbol') or r.get('security_id'))}</b> {plain_num(r.get('strike') or 0)} {esc(ot)}"
    exp = r.get("expiry_date")
    head = f"{i}. {name}" + (f"{DOT}{disp_day(str(exp))}" if exp else "")
    price, when = premium(r)
    second = []
    if price is not None:
        second.append(rupees(price) + (f" ({when or 'live'})" if labelled else ""))
    elif priced:
        second.append(NO_PRICE)
    gap, edge = zone_dist_pct(r), _num(r.get("zone_edge"))
    second.append(f"{abs(gap):.1f}% {rel_word(ot, gap)} {level_text(edge)}" if gap is not None and edge else "zone n/a")
    return f"{head}\n{LIST_ROW_INDENT}{DOT.join(second)}"


def _list_card(r: Mapping[str, Any], *, priced: bool, labelled: bool) -> str:
    """T-324 (D320, D321: candidate B · Cards, no tint): one GROUP list / ADDED row as its own quote card,
    "▼ <b>JIOFIN 220 PE</b> · 29 Sep" / "<b>₹0.52</b> · 0.0% above 225.1" -- the side mark, the contract bold with ITS
    OWN expiry (D182(1)), then the premium bold and the gap. The words and numbers are `_list_row`'s; the row number
    goes. tools/group_client_parity.py LIST_ROW reads the card's first line."""
    ot = str(r.get("option_type") or "").upper()
    name = f"<b>{esc(r.get('symbol') or r.get('security_id'))} {plain_num(r.get('strike') or 0)} {esc(ot)}</b>"
    exp = r.get("expiry_date")
    head = f"{side_mark(ot)} {name}" + (f"{DOT}{disp_day(str(exp))}" if exp else "")
    price, when = premium(r)
    second = []
    if price is not None:
        second.append("<b>" + rupees(price) + (f" ({when or 'live'})" if labelled else "") + "</b>")
    elif priced:
        second.append(f"<b>{NO_PRICE}</b>")
    gap, edge = zone_dist_pct(r), _num(r.get("zone_edge"))
    second.append(f"{abs(gap):.1f}% {rel_word(ot, gap)} {level_text(edge)}" if gap is not None and edge else "zone n/a")
    return _quote([head, DOT.join(second)])


def _spaced(items: Iterable[str]) -> list[str]:
    """The items with a blank line between two (two touching quote cards are not proven to draw apart on every
    Telegram client: the daily_summary rule)."""
    out: list[str] = []
    for k, it in enumerate(items):
        out += ["", it] if k else [it]
    return out


def _tags_phrase(rows: Iterable[Mapping[str, Any]]) -> str | None:
    tags = sorted({esc(r.get("tag")) for r in rows if r.get("tag")})
    if not tags:
        return None
    return f"{tags[0]} strike per stock" if len(tags) == 1 else f"{'/'.join(tags)} strikes per stock"


# 0.8.2 T-215 F6 (D186(3), absorbs T-206): the GROUP list post's footer phrase -- fixed, never a tag name (a stock-list
# with no HOI-1 row shows its first row and the tag phrase named it); personal keeps `_tags_phrase`. 0.8.5 (D287(4),
# PLAN-D287 I6): the client's lists carry TWO strikes per stock (HOI-1 and HOI-2), and the group says so
CLIENT_STRIKES_PHRASE = "two strikes per stock"


def _footer_phrase(rows: Iterable[Mapping[str, Any]], group: bool) -> str:
    return CLIENT_STRIKES_PHRASE if group else (_tags_phrase(rows) or "")


def list_post_shown(rows: Sequence[Mapping[str, Any]], shown: int = LIST_POST_SHOWN) -> list[Mapping[str, Any]]:
    """The contract rows `list_post` prints (the `shown` nearest the zone, `nearest_rows`): the router records them as
    the post's `contracts` (0.8.2 C1)."""
    return nearest_rows(rows)[: max(int(shown), 1)]


def contract_label(r: Mapping[str, Any]) -> str:
    """"SYM STRIKE OT" of a watchlist row, plain text (0.8.2 C1 `contracts`): 'BDL 1100 PE', 'M&M 2950.5 CE'."""
    ot = str(r.get("option_type") or "").upper()
    return " ".join(f"{r.get('symbol') or r.get('security_id')} {plain_num(r.get('strike') or 0)} {ot}".split())


def _more_phrase(more: int) -> str:
    return f"{more} more on the dashboard" if more > 0 else "full list on the dashboard"


def list_post(
    list_name: str,
    expiry_date: str,
    rows: Sequence[Mapping[str, Any]],
    trade_date: str,
    *,
    label: str | None = None,
    shown: int = LIST_POST_SHOWN,
    group: bool = False,
    posted_hm: str | None = None,
) -> list[str]:
    """The 09:20 list post of ONE list (D129 T3, candidate 2): headline "🗂️ <b>EXPIRY-PE WATCHLIST</b> · 📅 29 Sep",
    an italic "Mon 28 Sep · premiums at the 09:20 build" line (`label`, e.g. "09:20 build", when no price is
    shown), "<u><b>NEAREST SUPPORT</b></u> · N stocks" (RESISTANCE for a CE list), the `shown` contract rows nearest
    the zone (`nearest_rows`), numbered, then HOW TO READ ending "N more on the dashboard" (or "full list on the
    dashboard"). One message on the personal send (six rows always fit); [] for no rows. The router's 09:20 auto post
    and the manual "watchlist_list" send / preview both use it.
    0.8.2: `group` (T-215 F6) = a GROUP post: the footer says `two strikes per stock` (0.8.5; `one strike per stock` before), never a tag name (the default
    keeps the tag phrase: the owner's personal send). `posted_hm` (F11, C3) = a LATE post's clock: the italic line
    reads "Mon 28 Sep · 09:20 build · posted 09:58" (then the premium phrase, when there is one).
    T-324 (D320, D321: candidate B · Cards, both editions, no tint): a GROUP post prints each row as a quote card
    (`_list_card`), a blank line between cards, and ends with ONE italic footer line (no HOW TO READ heading); it goes
    through `split_by_length`, so a post that ever printed every row splits into parts ("· part i/n" on line 1) that
    each fit PART_MAX_CHARS / PART_MAX_ENTITIES. The personal post (group False) is unchanged (D319)."""
    if not rows:
        return []
    zone = "support" if _is_pe(list_name, rows) else "resistance"
    near = list_post_shown(rows, shown)
    n_stocks = len({str(r.get("symbol") or r.get("security_id")) for r in rows})
    phrase, labelled = _premium_phrase(near)
    if posted_hm:
        sub = [disp_weekday(trade_date), esc(label) if label else "", f"posted {esc(posted_hm)}", phrase or ""]
    else:
        sub = [disp_weekday(trade_date), phrase or (esc(label) if label else "")]
    read = [f"% = spot's gap to {zone}", _footer_phrase(rows, group), _more_phrase(len(rows) - len(near))]
    priced = phrase is not None or labelled
    # line 1 keeps `NAME WATCHLIST</b> · 📅 <expiry>`: tools/group_client_parity.py LIST_HEAD keys the post by it
    head = (f"{E_LIST} <b>{esc(list_name)} WATCHLIST</b>{DOT}{E_EXPIRY} <b>{disp_day(expiry_date)}</b>"
            f"{expiry_mark(expiry_date, trade_date)}")  # fmt: skip
    top = [f"<i>{DOT.join(x for x in sub if x)}</i>", DIV,
           f"{section('NEAREST ' + zone.upper())}{DOT}{_plural(n_stocks, 'stock')}"]  # fmt: skip
    footer = f"<i>{DOT.join(x for x in read if x)}</i>"
    if group:  # T-324: candidate B · Cards
        cards = [_list_card(r, priced=priced, labelled=labelled) for r in near]
        return split_by_length(cards, lambda chunk, i, n: "\n".join([head + _part_tag(i, n), *top, *_spaced(chunk),
                                                                       DIV, footer]))  # fmt: skip
    shown_rows: list[str] = []
    for i, r in enumerate(near, 1):
        shown_rows += ([""] if i > 1 else []) + [_list_row(i, r, priced=priced, labelled=labelled)]
    return ["\n".join([head, *top, "", *shown_rows, DIV, section("HOW TO READ"), footer])]


def additions_list_post(
    trade_date: str,
    rows: Sequence[Mapping[str, Any]],
    *,
    label: str = "13:15 additions",
    shown: int = LIST_POST_SHOWN,
    group: bool = False,
    since: str = "09:20",
) -> list[str]:
    """The 13:15 additions in the list-post shape (T-162 T3): ONE message, headline "🗂️ <b>ADDED 13:15</b> · N new
    stocks", then per list in LIST_ORDER a "<u><b>EXPIRY-PE</b></u> · 📅 29 Sep · N stocks near support" block with its
    `shown` nearest rows, then HOW TO READ. [] for no rows. The router's `post_additions` (the manual / API additions
    path; D148(3) removed the scheduled 13:15 post) sends it. 0.8.2 T-215 F6: `group` = the footer says `one strike per
    stock`, never a tag name (as `list_post`). 0.8.5: `since` = 'HH:MM' of the previous list post the rows were added
    after (the router's batch `since`): "reached a zone since 11:15" on the batch after an 11:15 post.
    T-324 (D320, D321: candidate B · Cards): a GROUP post prints each row as a quote card (`_list_card`), a blank line
    between cards, and ends with ONE italic footer line (no HOW TO READ heading). A card costs 3 entities, so a post of
    many list blocks splits BY LIST BLOCK (`split_by_length`; "· part i/n" on line 1; every part repeats the header
    lines and the footer): six blocks of six cards would be 108 entities, over Telegram's 100. The personal post
    (group False) is unchanged (D319)."""
    if not rows:
        return []
    groups: dict[tuple[str, str], list[Mapping[str, Any]]] = {}
    for r in rows:
        groups.setdefault((str(r.get("list_name") or ""), str(r.get("expiry_date") or "")), []).append(r)
    stamp = label.removesuffix(" additions") if label.endswith(" additions") else label
    shown_rows: list[Mapping[str, Any]] = []
    blocks: list[tuple[str, str, str, int, list[Mapping[str, Any]]]] = []
    more = 0
    for name, exp in list_post_order(groups):
        grp = groups[(name, exp)]
        zone = "support" if _is_pe(name, grp) else "resistance"
        near = nearest_rows(grp)[: max(int(shown), 1)]
        shown_rows += near
        more += len(grp) - len(near)
        n = len({str(r.get("symbol") or r.get("security_id")) for r in grp})
        blocks.append((name, exp, zone, n, near))
    phrase, labelled = _premium_phrase(shown_rows)
    priced = phrase is not None or labelled
    n_new = len({(k[0], k[1], str(r.get("symbol") or r.get("security_id"))) for k, g in groups.items() for r in g})
    lines = [
        f"{E_LIST} <b>ADDED {esc(stamp)}</b>{DOT}{_plural(n_new, 'new stock')}",
        f"{E_EXPIRY} <b>{disp_weekday(trade_date)}</b>{DOT}reached a zone since {esc(since)}",
        *([f"<i>{phrase}</i>"] if phrase else []),
    ]
    read = ["% = spot's gap to the zone edge", _footer_phrase(rows, group), _more_phrase(more)]
    if group:  # T-324: candidate B · Cards
        footer = [DIV, f"<i>{DOT.join(x for x in read if x)}</i>"]

        def render(chunk: Sequence[list[str]], i: int, n_parts: int) -> str:
            return "\n".join([lines[0] + _part_tag(i, n_parts), *lines[1:], *(x for b in chunk for x in b), *footer])

        # 0.8.8 T-328 L3: split_by_length never splits a block, so one list block over PART_MAX_CHARS /
        # PART_MAX_ENTITIES alone (a `shown` above LIST_POST_SHOWN) went out oversize. A block that does not fit
        # alone is cut between its cards here, each piece repeating the block's heading; a block that fits (every
        # block at shown = LIST_POST_SHOWN) is one unit exactly as before, so today's posts are unchanged.
        parts: list[list[str]] = []
        for name, exp, zone, n, near in blocks:
            heading = [DIV, (f"{section(esc(name))}{DOT}{E_EXPIRY} {disp_day(exp)}{expiry_mark(exp, trade_date)}{DOT}"
                             f"{_plural(n, 'stock')} near {zone}")]  # fmt: skip
            piece: list[str] = []
            for card in (_list_card(r, priced=priced, labelled=labelled) for r in near):
                if piece and not _fits(render([[*heading, *_spaced([*piece, card])]], 99, 99)):
                    parts.append([*heading, *_spaced(piece)])
                    piece = []
                piece.append(card)
            parts.append([*heading, *_spaced(piece)])
        return split_by_length(parts, render)
    for name, exp, zone, n, near in blocks:
        lines += [DIV, (f"{section(esc(name))}{DOT}{E_EXPIRY} {disp_day(exp)}{expiry_mark(exp, trade_date)}{DOT}"
                        f"{_plural(n, 'stock')} near {zone}"), ""]  # fmt: skip
        for i, r in enumerate(near, 1):
            lines += ([""] if i > 1 else []) + [_list_row(i, r, priced=priced, labelled=labelled)]
    lines += [DIV, section("HOW TO READ"), f"<i>{DOT.join(x for x in read if x)}</i>"]
    return ["\n".join(lines)]


def watchlist_row(row: Mapping[str, Any]) -> str:
    """One watchlist contract as a D ticket: the zone edge, the strike and the spot on a price ladder. T-212 style B:
    the headline is the style B contract line ("▼ <b>ADANIENT 2800 PE</b> · 📅 <b>29 Sep</b> 🟣"; ▼ / ▲ replace the old
    🔴 / 🟢 side icon, which now mean failed / passed), the list context in italics, a ━ divider, then the ladder."""
    ot = str(row.get("option_type") or "").upper()
    zone = zone_word(ot)
    title = contract_head(row.get("symbol"), row.get("strike"), ot, row.get("expiry_date"), row.get("trade_date"))
    context = [esc(x) for x in (row.get("list_name"), row.get("tag")) if x]
    if row.get("trade_date"):
        context.append(f"on the {disp_day(str(row['trade_date']))} list")
    lines = [title, f"<i>{DOT.join(context)}</i>"] if context else [title]
    lines.append(DIV)
    spot, edge = _num(row.get("ref_price")), _num(row.get("zone_edge"))
    levels: list[tuple[float, str]] = [(float(row.get("strike") or 0.0), "strike")]
    if edge:
        levels.append((edge, zone))
    if spot:
        levels.append((spot, "SPOT"))
    lines.append(f"<pre>{esc(price_ladder(levels, 'SPOT'))}</pre>")
    price, when = premium(row)
    if price is not None:
        lines.append(f"{E_PRICE} Premium {rupees(price)}" + (f" ({when})" if when else ""))
    facts: list[str] = []
    gap = zone_dist_pct(row)
    if gap is not None:
        facts.append(f"Spot <b>{abs(gap):.1f}%</b> {rel_word(ot, gap)} {zone}")
    if row.get("oi") is not None:
        facts.append(f"strike OI {oi_short(row['oi'])}")
    if row.get("lot_size"):
        facts.append(f"lot {indian_int(row['lot_size'])}")
    if facts:
        lines.append(DOT.join(facts))
    return "\n".join(lines)


# ------------------------------------------------------------------------------------------ token / system
def _hours_text(hours: float | None) -> str | None:
    if hours is None:
        return None
    h = max(float(hours), 0.0)
    if h < 1:
        return f"{round(h * 60)} min"
    if h < 10:
        return f"{h:.1f}".removesuffix(".0") + " h"
    return f"{h:.0f} h"


def _until_text(validity_ist: str | None) -> str | None:
    if not validity_ist:
        return None
    try:
        dt = datetime.fromisoformat(str(validity_ist)).astimezone(IST)
    except ValueError:
        return f"<b>{esc(validity_ist)}</b>"
    return f"<b>{dt:%H:%M}</b> on {disp_day(dt.date())}"


def token_card(
    level: str,
    validity_ist: str | None,
    hours_left: float | None,
    *,
    edition: str | None = None,
    broker_label: str | None = None,
) -> str:
    """PERSONAL: a bold title and one plain sentence with the fix (dashboard Token page). T-376 (C21, D87): the CLIENT
    text never names a signal kind (E1 / E2) and never a hard-coded broker: `broker_label` (the configured broker's
    registry label, e.g. "Zerodha") or the neutral "Broker". The OWNER card is unchanged ("Dhan" unless a label is
    given; its "No E1/E2 alerts" sentence stays)."""
    lvl = str(level).lower()
    client = _is_client(edition)
    broker = esc(broker_label) if broker_label else ("Broker" if client else "Dhan")
    until, left = _until_text(validity_ist), _hours_text(hours_left)
    if lvl == "expired":
        title = f"{broker} token expired"
        text = f"No {'' if client else 'E1/E2 '}alerts until a new token is pasted on the dashboard Token page."
    elif lvl in ("warn24", "warn2"):
        title = f"{broker} token expires in {left}" if left else f"{broker} token expires soon"
        text = (
            f"It stops working at {until}; paste a new token on the dashboard Token page before then."
            if until
            else "Paste a new token on the dashboard Token page before it expires."
        )
    elif lvl == "ok":
        title = f"{broker} token accepted"
        text = (
            f"Alerts are live again; the token is valid until {until}." if until else "Alerts are live again."
        )
    elif lvl == "unknown":
        title = f"{broker} token status unknown"
        text = "The token check failed; open the dashboard Token page to check the token."
    else:
        title = f"{broker} token {esc(level)}"
        text = "Open the dashboard Token page to check the token."
    mark = {"expired": M_FAIL, "warn24": M_OPEN, "warn2": M_OPEN, "ok": M_PASS}.get(lvl, M_OPEN)
    return f"{badge_line(ICON_TOKEN, title, mark)}\n{DIV}\n{text}"


# T-212 (D195(5)): the SYSTEM / TOKEN headline marker, read from the caller's free text (first match wins): a failure
# 🔴, a recovery 🟢, a heads-up 🟡; nothing matched = no marker
_SYSTEM_MARKERS: tuple[tuple[re.Pattern[str], str], ...] = (
    (re.compile(r"\b(FAILED|failed|crashed|EXPIRED|expired|rejected|stopped|not sent|refused)\b"), M_FAIL),
    (re.compile(r"\b(valid again|accepted|recovered)\b"), M_PASS),
    (re.compile(r"\b(expires in|expires soon|landing in General|supergroup|warning)\b", re.IGNORECASE), M_OPEN),
)
_ISO_TS = re.compile(r"\b\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(?::\d{2}(?:\.\d+)?)?(?:[+-]\d{2}:\d{2}|Z)?")
_HOURS = re.compile(r"\b(\d+(?:\.\d+)?) h\b")


def system_marker(text: str) -> str:
    return next((mark for rx, mark in _SYSTEM_MARKERS if rx.search(text)), "")


def humanize_times(text: str) -> str:
    """T-212 (catalogue §4.4): a raw ISO time in a TOKEN / SYSTEM sentence reads `08:31 on 1 Oct` (IST) and a decimal
    hour count `1.84 h` reads `1 h 50 min`, so the free-text token lines (token_watch.py) read like the structured
    token_card. Anything unparseable is left as written."""

    def iso(m: re.Match[str]) -> str:
        try:
            at = datetime.fromisoformat(m.group(0))  # Python 3.11+ parses a trailing `Z` itself
        except ValueError:
            return m.group(0)
        at = at.astimezone(IST) if at.tzinfo is not None else at
        return f"{at:%H:%M} on {disp_day(at.date())}"

    def hours(m: re.Match[str]) -> str:
        mins = round(float(m.group(1)) * 60)
        h, mm = divmod(mins, 60)
        return f"{h} h {mm} min" if h and mm else f"{h} h" if h else f"{mm} min"

    return _HOURS.sub(hours, _ISO_TS.sub(iso, text))


def system_card(title: str, lines: Iterable[str], *, icon: str = ICON_SYSTEM) -> str:
    """PERSONAL: a bold title and the caller's text as one plain paragraph (it should name the fix). T-212 style B:
    the title carries a marker read from the text (`system_marker`: 🔴 failed / 🟢 recovered / 🟡 heads-up), then a ━
    divider, then the text with its ISO times humanized (`humanize_times`)."""
    parts = [str(x).strip() for x in list(lines)[: CARD_MAX_LINES - 1]]
    raw = humanize_times(" ".join(p for p in parts if p))
    text = esc(raw[: PART_MAX_CHARS - 200])  # cut BEFORE escaping: never half an &amp;
    head = badge_line(icon, esc(title), system_marker(raw) if raw else "")
    return head + (f"\n{DIV}\n{text}" if text else "")


# ------------------------------------------------------------------------------------------ ops cards (T-132, T-134(2))
# PLAN-W16 section 13.3 B4, D119 G2-G3, D129 grammar: two-line headline, <u><b>SECTION</b></u> headers, one meaning
# per emoji. The three new emoji live in OPS_VOCABULARY (kept out of VOCABULARY so final.html's legend is unchanged);
# every other emoji on these cards is a VOCABULARY emoji with its VOCABULARY meaning.
E_READY = "\N{VERTICAL TRAFFIC LIGHT}"
E_CONNECTED = "\N{ELECTRIC PLUG}"
E_WARN = "\N{WARNING SIGN}\N{VARIATION SELECTOR-16}"
OPS_VOCABULARY: dict[str, str] = {
    E_READY: "readiness check", E_CONNECTED: "install connected / service started", E_WARN: "setup warning",
}  # fmt: skip
# A passing check whose detail carries this marker renders with E_WARN (gammad/jobs/readiness.py writes it for the
# forum-topic warning: the group works, but cards land in General).
WARN_MARK = "WARNING: "
READINESS_LABEL = {
    "token": "Broker token", "env": "Settings (.env)", "telegram": "Telegram", "broker": "Broker connection",
    "disk": "Disk space", "clock": "PC clock", "data_dir": "Data folder",
}  # fmt: skip
OWNER_READINESS_LABEL = {"calendar": "Rule calendar"}


def _readiness_dict(r: Any) -> dict[str, Any]:
    """A Readiness (gammad/jobs/readiness.py) or its to_dict() mapping {ok, at_ist, checks: [{name, ok, detail}]}."""
    to_dict = getattr(r, "to_dict", None)
    return dict(to_dict()) if callable(to_dict) else dict(r)


def _ist_hm_text(iso: Any) -> str:
    """"09:00" from an ISO timestamp (an aware one is converted to IST); "" when it does not parse."""
    try:
        at = datetime.fromisoformat(str(iso))
    except (TypeError, ValueError):
        return ""
    if at.tzinfo is not None:
        at = at.astimezone(IST)
    return at.strftime("%H:%M")


def readiness_label(name: Any) -> str:
    n = str(name)
    return READINESS_LABEL.get(n) or OWNER_READINESS_LABEL.get(n, n)


def readiness_check_lines(checks: Iterable[Mapping[str, Any]]) -> list[str]:
    """One line per check: 🟢 passed / 🟡 passed with a setup warning (heads-up) / 🔴 failed (T-212 markers; were
    ✅ / ⚠️ / ❌), the label bold, then the detail."""
    out = []
    for c in checks:
        detail = str(c.get("detail") or "")
        icon = (M_OPEN if WARN_MARK in detail else M_PASS) if c.get("ok") else M_FAIL  # T-212: the markers
        out.append(f"{icon} <b>{esc(readiness_label(c.get('name')))}</b>" + (f" {EM_DASH} {esc(detail)}" if detail else ""))
    return out


def readiness_first_problem(r: Any) -> dict[str, Any] | None:
    """The first failed check in CHECK_NAMES order (the order the checks are listed in), or None."""
    return next((dict(c) for c in _readiness_dict(r).get("checks") or [] if not c.get("ok")), None)


def readiness_card(r: Any) -> str:
    """The 09:00 card (T-132, D119 G2): READY (sent silently) or NOT READY: <first problem> (with sound).
    Line 1 = 🚦 + the verdict + the first problem's label; line 2 = 🕒 time · the count; then FIX FIRST (NOT READY
    only) and CHECKS (every check, in order)."""
    d = _readiness_dict(r)
    checks = list(d.get("checks") or [])
    when = _ist_hm_text(d.get("at_ist"))
    failed = [c for c in checks if not c.get("ok")]
    clock = f"{E_TIME} {when} IST" if when else ""
    if d.get("ok") and not failed:
        head = f"{E_READY} <b>READY</b>{DOT}<b>GAMMA-UNWIND</b>{BADGE_GAP}{M_PASS}"
        count = f"all {_plural(len(checks), 'check')} passed" if len(checks) != 1 else "the check passed"
        return "\n".join([head, DOT.join(x for x in (clock, count) if x), DIV, section("CHECKS"),
                          *readiness_check_lines(checks)])  # fmt: skip
    first = failed[0] if failed else {"name": "check", "detail": "a check did not pass"}
    label = readiness_label(first.get("name"))
    detail = str(first.get("detail") or "did not pass")
    return "\n".join([
        f"{E_READY} <b>NOT READY</b>{DOT}<b>{esc(label)}</b>{BADGE_GAP}{M_FAIL}",
        DOT.join(x for x in (clock, f"{len(failed)} of {_plural(len(checks), 'check')} failed") if x),
        DIV,
        section("FIX FIRST"),
        f"{M_FAIL} <b>{esc(label)}</b> {EM_DASH} {esc(detail)}",
        DIV,
        section("CHECKS"),
        *readiness_check_lines(checks),
        DIV,
        "<i>Checked again at 09:10; an all-clear card follows once every check passes.</i>",
    ])  # fmt: skip


def readiness_all_clear(r: Any) -> str:
    """The 09:10 all-clear (sent silently, and only after a 09:00 NOT READY that is READY now, D119 G2)."""
    d = _readiness_dict(r)
    checks = list(d.get("checks") or [])
    when = _ist_hm_text(d.get("at_ist"))
    clock = f"{E_TIME} {when} IST" if when else ""
    return "\n".join([
        f"{E_READY} <b>READY</b>{DOT}<b>all clear</b>{BADGE_GAP}{M_PASS}",
        DOT.join(x for x in (clock, "fixed since the 09:00 check") if x),
        DIV,
        section("CHECKS"),
        *readiness_check_lines(checks),
    ])  # fmt: skip


def install_card(version: Any, edition: Any, broker: Any) -> str:
    """The first start EVER of an install (T-134(2), D119 G3): ONE group card, with sound. Line 1 reads
    "GAMMA-UNWIND <version> connected" (bold). 0.8.2 T-215 F12 (D186(3)): the GROUP card names no edition and no broker
    (it printed `· owner · dhan`: owner-only structure in the group, D166); the personal `start_line` keeps both. The
    `edition` / `broker` parameters stay for the callers (jobs/announce.py)."""
    del edition, broker
    return "\n".join([
        f"{E_CONNECTED} <b>GAMMA-UNWIND {esc(version)} connected</b>",
        "This group now receives this install's alerts.",
        DIV,
        section("WHAT POSTS HERE"),
        f"{E_LIST} The lists after the 09:20 IST build",
        f"{E_ENTRY} Trade alerts, with sound",
        f"{E_WATCH} {TIER_LABEL['STRONG_WATCHLIST']} cards, silent",  # D273 (was "Watch-only cards")
        DIV,
        section("MARKERS"),  # T-212 (catalogue §6): the marker legend belongs on the install card and in the guide
        f"{M_PASS} passed / gain{DOT}{M_FAIL} failed / loss",
        f"{M_OPEN} still open / heads-up{DOT}{M_WATCH} {NOT_ENTRY}",
        f"{M_EXPIRY_WEEK} expires within {EXPIRY_WEEK_DAYS} trading days{DOT}{E_PUT} put{DOT}{E_CALL} call",
        DIV,
        "<i>Posted once, when this install first connected.</i>",
    ])  # fmt: skip


def start_line(version: Any, edition: Any, broker: Any, now: datetime | None) -> str:
    """Every later start (T-134(2)): to the PERSONAL chat, sent silently. T-212 style B: the bold headline with its
    marker, then `edition · broker · 🕒 HH:MM IST` on line 2 (was one line)."""
    when = ""
    if now is not None:
        at = now.astimezone(IST) if now.tzinfo is not None else now
        when = f"{DOT}{E_TIME} {at.strftime('%H:%M')} IST"
    return (f"{badge_line(E_CONNECTED, f'GAMMA-UNWIND {esc(version)} started', M_PASS)}\n"
            f"{esc(edition)}{DOT}{esc(broker)}{when}")  # fmt: skip


# ------------------------------------------------------------------------------------------ daily summaries
SUMMARY_FOOTER = "<i>Results from the trade tracker. Alert only, not advice.</i>"
RESULT_NOT_KNOWN = "<i>result not known yet</i>"  # D144(5): no trade row = no result, never an invented one
# a trade event -> (emoji, words) on the summary's result line; T1 / T2 / SL / TRAIL carry the event's own price
# (the fill, or the stop level, D125 F6), the day-end exits read the trade's exit price
_RESULT_WORDS: dict[str, tuple[str, str]] = {
    "T1": (E_T1, "Target 1 hit"), "T2": (E_T2, "Target 2 hit"), "SL": (E_SL, "Stop loss hit"),
    "TRAIL": (E_SL, "Stopped at entry"), "SWING_STOP": (E_EDGE, "Stock stop hit"),
    "EOD": (E_CLOSED, "Closed at day end"), "EOD_MISSED": (E_CLOSED, "Closed at day end"),
    "EXPIRY": (E_CLOSED, "Closed on expiry day"), "DAY5": (E_CLOSED, "Closed on day 5"),
    # 0.8.4 (T-267): the V2 Scanner's stop exits (only a V2T trade carries them; the V2 Scanner section)
    "ZSL": (E_EDGE, "Stop zone hit"), "ZTRAIL": (E_EDGE, "Trailed stop hit"),
    # 0.8.9 T-419: a Still Watching trade (SW_TARGET is not an exit: its line reads the event's own price)
    SW_STOP: (E_EDGE, "Stop hit"), SW_TARGET: (E_T1, "Trailing"),
    SW_TRAIL: (E_EDGE, "Trailing stop hit"), SW_EVE: (E_CLOSED, "Expiry-eve exit"),
}  # fmt: skip
_RESULT_WORDS.update({k: (E_CLOSED, v) for k, v in _V1R_WORDS.items()})
_OPEN_STATES = LIVE_STATES | ZONE_STATES | V1R_STATES  # a live V1 trade or a live V2 (Z_OPEN) trade: "Still open"
_EXIT_PRICED = frozenset({"T1", "T2", "SL", "TRAIL", SW_TARGET})


def _contract_key(r: Mapping[str, Any]) -> Any:
    return r.get("security_id") or (r.get("symbol"), r.get("strike"), r.get("option_type"), r.get("expiry_date"))


def summary_result_lines(trade: Mapping[str, Any] | None, trade_date: str, entry: float | None) -> list[str]:
    """The RESULT of one entry from its tracker trade (a trades row + its `events`, `gammad.trade.results`): one line
    per target / stop / exit event in order ("🟢 🎯 <b>Target 1 hit</b> 13:05 · +20%"); a live trade with no event reads
    "🟡 ⏸ <b>Still open</b>" (+ % vs entry when today's close is known). No trade -> "result not known yet".
    T-212 (D195(5)): each line opens with its marker -- a target 🟢, the stop loss 🔴, a timed / stock-stop exit by
    the sign of its result (🟢 gain, 🔴 loss, none at 0), still open 🟡."""
    if not isinstance(trade, Mapping):
        return [RESULT_NOT_KNOWN]
    base = _num(trade.get("entry")) or entry
    out: list[str] = []
    for ev in trade.get("events") or ():
        kind = str(ev.get("kind") or "")
        if kind not in _RESULT_WORDS:
            continue
        icon, words = _RESULT_WORDS[kind]
        px = _num(ev.get("price")) if kind in _EXIT_PRICED else _num(trade.get("exit_px"))
        parts = [f"{icon} <b>{words}</b> {ist_hm(int(ev.get('ts_ms') or 0))}"]
        if px is not None and kind not in _EXIT_PRICED:
            parts.append(rupees(px))
        pct = _up(base, px) if px is not None and base else None
        if pct is not None:
            parts.append(_pct_short(pct))
        mark = _RESULT_MARK.get(kind) or sign_marker(pct)
        out.append((f"{mark} " if mark else "") + DOT.join(parts))
    if not out and str(trade.get("state") or "") in _OPEN_STATES | SW_STATES:
        close = _today_close(trade, trade_date)
        pct_txt = f"{DOT}{_pct_short(_up(base, close))}" if close is not None and base else ""
        word = "Open" if str(trade.get("state") or "") in SW_STATES else "Still open"
        out.append(f"{M_OPEN} {E_OPEN} <b>{word}</b>{pct_txt}")
    if not out and str(trade.get("state") or "") in CLOSED_STATE_OF_KIND.values():
        out.append(f"{E_CLOSED} <b>Position closed</b>")
    return out or [RESULT_NOT_KNOWN]


_RESULT_MARK = {"T1": M_PASS, "T2": M_PASS, "SL": M_FAIL}  # the rest: by the sign of the result (T-212)
_TALLY_HIT, _TALLY_STOP, _TALLY_CLOSED = frozenset({"T1", "T2", SW_TARGET, "V1R_TARGET", "V1R_HALF"}), frozenset(
    {"SL", "TRAIL", "SWING_STOP", "ZSL", "ZTRAIL", SW_STOP, SW_TRAIL, "V1R_STOP", "V1R_CAP", "V1R_TRAIL"}), frozenset(
    {"EOD", "EOD_MISSED", "EXPIRY", "DAY5", SW_EVE, "V1R_NOPROG", "V1R_EOD", "V1R_SETTLED"})  # fmt: skip


def summary_tally(trade: Mapping[str, Any] | None) -> str:
    """T-212 (D195(5), the summary opens with the TALLY): the ONE bucket of an entry, from the same tracker result the
    box prints -- `hit` (a target was reached), `stopped` (a stop with no target before it), `closed` (a timed exit),
    `open` (live, no event), `unknown` (no trade row / nothing readable)."""
    if not isinstance(trade, Mapping):
        return "unknown"
    kinds = {str(ev.get("kind") or "") for ev in trade.get("events") or ()}
    state = str(trade.get("state") or "")
    if state in CLOSED_STATE_OF_KIND.values():  # a closed V1R row: its closing kind (CX-120 review finding 2, D526)
        kinds.add(state.removeprefix("CLOSED_"))
    if kinds & _TALLY_HIT:
        return "hit"
    if kinds & _TALLY_STOP:
        return "stopped"
    if kinds & _TALLY_CLOSED:
        return "closed"
    return "open" if str(trade.get("state") or "") in _OPEN_STATES else "unknown"


def _today_close(trade: Mapping[str, Any] | None, trade_date: str) -> float | None:
    """The contract's close of `trade_date` as the tracker snapshots it at 15:35 (trades.last_close_px of that date,
    swing trades); None when it holds none for that day."""
    if not isinstance(trade, Mapping) or str(trade.get("last_close_date") or "") != str(trade_date):
        return None
    return _num(trade.get("last_close_px"))


def _star(r: Mapping[str, Any]) -> str:
    """D149(5) / D145: " ★" beside a contract whose card fired on a STRONG OI drop (signals.strong). Never on a V2
    Scanner row (D257): the day summary is a GROUP text, the group is the client view, and no client surface shows a
    V2 STRONG (the router keeps every such entry off the group)."""
    return f" {ICON_STRONG}" if _strong_shown(r, client=True) else ""


def _summary_entry(r: Mapping[str, Any], trade_date: str) -> str:
    """One entry's box (T-212 style B): "▼ <b>M&amp;M 2950 PE</b> ★ · 29 Sep 🟣" / "🕒 11:05 · 💰 Entry ₹4.00" (the ENTRY's
    price: the trade's entry, else the ENTRY signal's premium; never a watch card's) / "close ₹4.95" (when today's close
    is known) / the result lines."""
    trade = r.get("trade") if isinstance(r.get("trade"), Mapping) else None
    entry = (_num(trade.get("entry")) if trade else None) or _num(r.get("ltp")) or 0.0
    contract = _contract_text(r.get("symbol") or r.get("security_id"), r.get("strike"), r.get("option_type"))
    exp = str(r.get("expiry_date") or "")
    head = f"{side_mark(r.get('option_type'))} <b>{contract}</b>{_star(r)}{DOT}{disp_day(exp)}{expiry_mark(exp, trade_date)}"
    close = _today_close(trade, trade_date)
    line = f"{E_TIME} {ist_hm(int(r.get('ts_ms') or 0))}{DOT}{E_PRICE} Entry {rupees(entry)}"
    closing = [f"close {rupees(close)}"] if close is not None else []
    return _quote([head, line, *closing, *summary_result_lines(trade, trade_date, entry)])


def _summary_item(r: Mapping[str, Any], trade_date: str | None = None) -> str:
    """T-324 (D320, D321: candidate B · Cards): one near entry as a quote card like the entry boxes,
    "▼ <b>HINDALCO 940 PE</b> ★ · 29 Sep 🟣" / "🕒 13:52 · ₹1.75" (a watch-only card: its premium at the card)."""
    contract = _contract_text(r.get("symbol") or r.get("security_id"), r.get("strike"), r.get("option_type"))
    exp = str(r.get("expiry_date") or "")
    head = f"{side_mark(r.get('option_type'))} <b>{contract}</b>{_star(r)}{DOT}{disp_day(exp)}{expiry_mark(exp, trade_date)}"
    return _quote([head, f"{E_TIME} {ist_hm(int(r.get('ts_ms') or 0))}{DOT}{rupees(r.get('ltp') or 0.0)}"])


def daily_summary(
    trade_date: str,
    counts: Mapping[str, int],
    top_signals: Sequence[Mapping[str, Any]],
    *,
    edition: str | None = None,
    v2_rows: Sequence[Mapping[str, Any]] = (),
) -> str:
    """GROUP 15:35 summary. T-212 style B, SUMMARY FIRST (D195(5)): "🧾 <b>DAY SUMMARY</b>" / "📅 <b>Tue 29 Sep</b>" / ━ /
    TODAY = the counts ("⚡ <b>2</b> entries · 👀 <b>1</b> near entry", D273) then the TALLY of the entries' tracker
    results ("🟢 <b>1</b> target hit", "🟡 <b>1</b> still open", "🔴 <b>0</b> stopped"; "🔔 n closed at a set time" and
    "n result not known yet" only when non-zero; `summary_tally`) / ━ / ENTRIES -- one box per ENTRY card (an untiered
    E1 row counts as one): contract · expiry, 🕒 time · 💰 Entry, close, and its RESULT from the trade tracker (each row's
    optional `trade`, attached by router.group_summary_data; none -> "result not known yet") / ━ / NEAR ENTRY
    (🔵 not entries, not tracked; the tier code STRONG_WATCHLIST, D273 name) one quote card each (T-324, candidate B ·
    Cards: contract · expiry, 🕒 time · premium), a blank line between two / the footer. Every number is the T-178 summary's (only the
    layout moved). A watch card never counts as an entry and its premium is never printed as the entry price; a
    contract that went on to an ENTRY is listed as the entry only. ★ beside a contract whose card fired on a STRONG
    drop (D149(5)). One blank line between two entry boxes (two adjacent quotes are not proven to draw apart on
    every Telegram client). NO E2 row in either edition (D149(5): E2 cards go to the owner's PERSONAL chat only;
    the operator summary keeps the count). `counts` is kept for the signature: every number here is derived from the
    rows, which are exactly the signals the group was sent. No operator counters (operator_summary). Capped by
    LENGTH ("+N more", the tally inside the measured text), never by a row count. Empty day: TODAY with the zero
    counts and "No entries today.".
    0.8.5 (D287(6), PLAN-D287 I7): the 0.8.4 "V2 Scanner" section is gone -- the group never shows a V2 row. A V2 row
    handed over anyway (`v2_rows`, kept for the signature, or inside `top_signals` with scanner == "V2T") is dropped:
    it never counts in the TODAY line, the tally, ENTRIES or NEAR ENTRY."""
    del counts, edition
    # 0.8.5 (D287(1), PLAN-D287 AM1): the group's entries are the client store's E1 rows and the owner store's TIERED E2
    # rows (`_is_client_entry_row`); the text never depends on which (the HARD line: client sends == owner group sends)
    # 0.8.5 (D287(6), PLAN-D287 I7 / I9): this is a GROUP (client) surface -- a V2 Scanner row is DROPPED, wherever it
    # arrives (`top_signals` or `v2_rows`): no V2 section, never counted in TODAY, the tally or ENTRIES
    del v2_rows
    picked = [flatten_signal_row(r) for r in top_signals if _is_client_entry_row(r)]
    rows = sorted((r for r in picked if str(r.get("scanner") or "") != SCANNER_V2),
                  key=lambda r: (int(r.get("ts_ms") or 0), r.get("id") or 0))  # fmt: skip
    title = [f"{ICON_SUMMARY} <b>DAY SUMMARY</b>", f"{E_EXPIRY} <b>{disp_weekday(trade_date)}</b>", DIV,
             section("TODAY")]  # fmt: skip
    entries = [r for r in rows if str(r.get("tier") or "") in ("", "ENTRY")]  # `picked` holds entry-kind rows only
    seen = {_contract_key(r) for r in entries}  # a contract that went on to an ENTRY is listed as the entry only
    watch: list[dict[str, Any]] = []
    for r in rows:
        if str(r.get("tier") or "") not in ("STRONG_WATCHLIST", "WATCHLIST"):
            continue
        if _contract_key(r) not in seen:  # one line per contract: its first watch card
            seen.add(_contract_key(r))
            watch.append(r)
    near = TIER_LABEL["STRONG_WATCHLIST"]  # D273: the group's watch cards are NEAR ENTRY cards
    count = (f"{E_ENTRY} <b>{len(entries)}</b> {'entry' if len(entries) == 1 else 'entries'}{DOT}{E_WATCH} "
             f"<b>{len(watch)}</b> {near.lower()}")  # fmt: skip
    if not (entries or watch):
        return "\n".join([*title, count, "No entries today."])
    buckets = [summary_tally(r.get("trade") if isinstance(r.get("trade"), Mapping) else None) for r in entries]
    tally: list[str] = []
    if entries:
        hit = buckets.count("hit")
        tally = [f"{M_PASS} <b>{hit}</b> target{'' if hit == 1 else 's'} hit",
                 f"{M_OPEN} <b>{buckets.count('open')}</b> still open", f"{M_FAIL} <b>{buckets.count('stopped')}</b> stopped"]
        if buckets.count("closed"):
            tally.append(f"{E_CLOSED} <b>{buckets.count('closed')}</b> closed at a set time")
        unknown = buckets.count("unknown")
        if unknown:
            tally.append(f"<i>{unknown} result{'' if unknown == 1 else 's'} not known yet</i>")
    boxes = [_summary_entry(r, trade_date) for r in entries]
    items = [_summary_item(r, trade_date) for r in watch]

    def compose(ne: int, nw: int) -> str:
        out = [*title, count, *tally]
        if boxes:
            out += [DIV, section("ENTRIES")]
            for i, box in enumerate(boxes[:ne]):
                out += ["", box] if i else [box]
            out += [f"+{len(boxes) - ne} more"] if ne < len(boxes) else []
        if items:
            out += [DIV, section(near), f"{M_WATCH} not entries, not tracked", *_spaced(items[:nw])]
            out += [f"+{len(items) - nw} more"] if nw < len(items) else []
        return "\n".join([*out, "", SUMMARY_FOOTER])

    shown = [0, 0]
    for k, n in enumerate((len(boxes), len(items))):
        while shown[k] < n:
            shown[k] += 1
            if shown[k] > 1 and not _fits(compose(*shown)):
                shown[k] -= 1
                break
    return compose(*shown)


def v2_summary_count(entries: Sequence[Mapping[str, Any]]) -> str:
    """The V2 Scanner section's count line: "⚡ <b>2</b> entries · 🟡 <b>1</b> still open · 🔴 <b>1</b> stopped" (+ "🔔 <b>n</b>
    closed at a set time" and "n results not known yet" when non-zero; the V2 Scanner has no targets, so no "hit")."""
    buckets = [summary_tally(r.get("trade") if isinstance(r.get("trade"), Mapping) else None) for r in entries]
    n = len(entries)
    parts = [f"{E_ENTRY} <b>{n}</b> {'entry' if n == 1 else 'entries'}", f"{M_OPEN} <b>{buckets.count('open')}</b> still open",
             f"{M_FAIL} <b>{buckets.count('stopped')}</b> stopped"]  # fmt: skip
    if buckets.count("closed"):
        parts.append(f"{E_CLOSED} <b>{buckets.count('closed')}</b> closed at a set time")
    unknown = buckets.count("unknown")
    if unknown:
        parts.append(f"<i>{unknown} result{'' if unknown == 1 else 's'} not known yet</i>")
    return DOT.join(parts)


def v2_day_block(trade_date: str, v2_rows: Sequence[Mapping[str, Any]], *, edition: str | None = None) -> str:
    """0.8.5 D296 M4: the OWNER's 15:35 PERSONAL V2 Scanner day block (D287(6) keeps V2 in his personal chat; the 0.8.4
    section lived in the group summary, now V2-free): "🧾 <b>V2 Scanner · DAY</b>" / "📅 <b>Tue 29 Sep</b>" / ━ /
    `v2_summary_count` / ━ / one `_summary_entry` box per V2 ENTRY row (router.owner_v2_summary_rows, each with its
    optional `trade`), "+N more" past the length cap. "" when no row is a V2 ENTRY (nothing to say). The CLIENT
    edition raises OwnerOnlyError (no client message ever names the V2 Scanner)."""
    if _is_client(edition):
        raise edition_mod.OwnerOnlyError("the V2 Scanner day block is owner PERSONAL only (D287(6), D296 M4)")
    seen: set[Any] = set()
    entries: list[dict[str, Any]] = []
    for r in sorted((flatten_signal_row(x) for x in v2_rows), key=lambda r: (int(r.get("ts_ms") or 0), r.get("id") or 0)):
        key = r.get("id") if r.get("id") is not None else _contract_key(r)
        if str(r.get("tier") or "") in ("", "ENTRY") and key not in seen:
            seen.add(key)
            entries.append(r)
    if not entries:
        return ""
    boxes = [_summary_entry(r, trade_date) for r in entries]
    head = [f"{ICON_SUMMARY} <b>{scanner_title(SCANNER_V2)}{DOT}DAY</b>", f"{E_EXPIRY} <b>{disp_weekday(trade_date)}</b>",
            DIV, v2_summary_count(entries), DIV]  # fmt: skip

    def compose(n: int) -> str:
        out = list(head)
        for i, box in enumerate(boxes[:n]):
            out += ["", box] if i else [box]
        out += [f"+{len(boxes) - n} more"] if n < len(boxes) else []
        return "\n".join([*out, "", SUMMARY_FOOTER])

    shown = 1
    while shown < len(boxes) and _fits(compose(shown + 1)):
        shown += 1
    return compose(shown)


_OPS_ALERT_KEYS = (("sent", "sent"), ("gated", "gated"), ("paused", "paused"), ("muted_alerts", "muted"),
                   ("failed", "failed"))  # fmt: skip
PERSONAL_ONLY_KEY = "personal_only"  # 0.8.2 T-215 F13: router.summary_data's count (owner edition)
_OPS_OTHER_KEYS = frozenset({"E1", "E2", "strong", "watchlist", "muted", "contracts", PERSONAL_ONLY_KEY})


def operator_summary(trade_date: str, counts: Mapping[str, int], *, edition: str | None = None) -> str:
    """PERSONAL 15:35 ops message: the GROUP alert counters (sent / gated / paused / muted / failed and any other
    status), the signal counts and the watchlist / mute sizes. CLIENT: no E2 count (D81).
    0.8.2 T-215 F13 (D186(3)): `counts["personal_only"]` (owner: V1 E1 cards sent to the PERSONAL chat only, no group
    row -- an off-client or carried card) ends the alert line as `personal-only N`, so a personal-only card is never
    read as a lost one. Absent key (the client edition) = nothing printed."""
    c = {k: int(v or 0) for k, v in counts.items()}
    client = _is_client(edition)
    if client:
        c.pop("E2", None)
    known = {k for k, _ in _OPS_ALERT_KEYS} | _OPS_OTHER_KEYS
    alert_parts = [f"{label} {c.get(k, 0)}" for k, label in _OPS_ALERT_KEYS]
    alert_parts += [f"{esc(k)} {v}" for k, v in sorted(c.items()) if k not in known]
    if PERSONAL_ONLY_KEY in c and not client:
        alert_parts.append(f"personal-only {c[PERSONAL_ONLY_KEY]}")
    # T-212 style B: the headline marker = 🔴 when any group alert failed, else 🟢; a heading per counter group
    return "\n".join([
        badge_line(ICON_SYSTEM, f"Ops{DOT}{disp_day(trade_date)}", M_FAIL if c.get("failed", 0) else M_PASS),
        DIV,
        section("GROUP ALERTS"),
        DOT.join(alert_parts),
        section("SIGNALS"),
        f"{_kind_counts(c.get('E2', 0), c.get('E1', 0), edition)}{DOT}{c.get('strong', 0)} STRONG",
        section("WATCHLIST"),
        f"{_plural(c.get('watchlist', 0), 'contract')}{DOT}{c.get('muted', 0)} muted",
    ])  # fmt: skip


# ------------------------------------------------------------------------------------------ delivery card (K48)
DELIVERY_TITLE = "GROUP DELIVERY"


def _delivery_part_marker(part: str) -> str:
    """One section of gammad/scan/delivery_check.Verdict.line(): 🟢 clean, 🔴 a miss, 🟡 warnings (T-212)."""
    m = re.match(r"(lists|cards) (\d+)/(\d+)(.*)$", part)
    if m:
        return M_PASS if m[2] == m[3] and not m[4].strip() else M_FAIL
    if part.startswith("summary"):
        return M_PASS if part.strip() == "summary \N{CHECK MARK}" else M_FAIL
    if part.startswith("follow-ups"):
        return M_PASS if part.startswith("follow-ups 0 ") else M_FAIL
    if re.match(r"\d+ warnings?$", part):
        return M_OPEN
    return M_FAIL


def delivery_card(line: str, misses: Sequence[str] = ()) -> str:
    """K48 (0.8.2 T-211) in T-212 style B for the owner's PERSONAL chat: the plain, already HTML-escaped one-line
    verdict of gammad/scan/delivery_check.Verdict.line() (`✅ GROUP DELIVERY 30 Sep · lists 2/2 · ...`) becomes a
    headline that keeps `<icon> GROUP DELIVERY <day>` (+ 🟢 / 🔴), a CHECKS section with one marked line per part and,
    on a miss, a MISSED section listing the stored misses (catalogue §4.4: they lived only in the ref / CLI; escaped
    here, at most MAX_DELIVERY_MISSES). A line that does not read as a verdict is returned unchanged (never lost)."""
    parts = line.split(DOT)
    m = re.match(rf"^(\S+) {DELIVERY_TITLE} (.+)$", parts[0])
    if not m:
        return line
    icon, day = m[1], m[2]
    marks = [_delivery_part_marker(p) for p in parts[1:]]
    bad = M_FAIL in marks or bool(misses)
    lines = [f"{icon} <b>{DELIVERY_TITLE} {day}</b>{BADGE_GAP}{M_FAIL if bad else M_PASS}"]
    if misses:
        lines.append(f"{M_FAIL} {len(misses)} missed")
    lines += [DIV, section("CHECKS"), *(f"{mk} {p}" for mk, p in zip(marks, parts[1:], strict=True))]
    if misses:
        shown = [esc(x) for x in list(misses)[:MAX_DELIVERY_MISSES]]
        more = [f"+{len(misses) - len(shown)} more (tools/delivery_check.py)"] if len(misses) > len(shown) else []
        lines += [DIV, section("MISSED"), *(f"{M_FAIL} {x}" for x in shown), *more]
    return "\n".join(lines)


MAX_DELIVERY_MISSES = 8


# ------------------------------------------------------------------------------------------ copy / export (D44)
def _cell(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, bool):
        return "1" if v else "0"
    if isinstance(v, float):
        return f"{v:.0f}" if v.is_integer() else repr(round(v, 6))
    return " ".join(str(v).split("\t")).replace("\r", " ").replace("\n", " ")


def tsv(rows: Iterable[Mapping[str, Any]], columns: Sequence[str]) -> str:
    """Header line + one tab-separated line per row. Tabs/newlines inside values become spaces."""
    lines = ["\t".join(columns)]
    lines.extend("\t".join(_cell(r.get(c)) for c in columns) for r in rows)
    return "\n".join(lines)


# T-376 (0.8.8 client leak; D429 final (a) / D433 / D87): the CLIENT copy line is the dashboard's own
# (dashboard/src/textfmt.ts signalLine / watchLine, client build) byte for byte, so these helpers port its JavaScript
# number words exactly: tests/fixtures/client_copy_lines.json is the shared proof (python + node read it).
def _js_finite(v: Any) -> bool:
    """JavaScript Number.isFinite: a real, finite number (a bool, a string or None is not)."""
    return isinstance(v, int | float) and not isinstance(v, bool) and math.isfinite(v)


def js_fixed(v: float, digits: int) -> str:
    """JavaScript Number.prototype.toFixed: the EXACT binary value rounded half UP (3.125 -> "3.13"; Python's format
    rounds that tie to even, "3.12"); a negative value keeps its sign ("-0.00" for -0.001), a negative zero has none."""
    sign = "-" if v < 0 else ""
    q = Decimal(abs(float(v))).quantize(Decimal(1).scaleb(-digits), rounding=ROUND_HALF_UP)
    return f"{sign}{q:f}"


def _js_number(v: float) -> str:
    """JavaScript String(number) for the plain range (|v| < 1e21, not tiny): integers bare, else the shortest repr."""
    if math.isnan(v):
        return "NaN"
    if math.isinf(v):
        return "Infinity" if v > 0 else "-Infinity"
    return str(int(v)) if float(v).is_integer() else repr(float(v))


def _js_text(v: Any) -> str:
    """A value in a JavaScript template literal (`${v}`): null -> "null", a bool -> "true" / "false", a number as
    String(number), a string as itself."""
    if v is None:
        return "null"
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, int | float):
        return _js_number(v)
    return str(v)


def js_f2(v: Any) -> str:
    """textfmt.ts f2: "-" for null / a non-finite value, else toFixed(2)."""
    return js_fixed(v, 2) if _js_finite(v) else "-"


def js_strike(v: Any) -> str:
    """format.ts strikeText: "-" for null / non-finite; an integer bare (12000); else at most two decimals (152.5)."""
    if not _js_finite(v):
        return "-"
    if float(v).is_integer():
        return _js_number(v)
    return _js_number(float(js_fixed(v, 2)))


def js_inr(v: Any) -> str:
    """textfmt.ts inr: Math.round (a tie goes UP, toward +infinity) then en-IN grouping (1,58,350); "-" if none."""
    if not _js_finite(v):
        return "-"
    n = math.floor(Decimal(float(v)) + Decimal("0.5"))
    return "-0" if n == 0 and v < 0 else indian_int(n)


def _js_zone_dist_pct(ref: Any, edge: Any) -> float | None:
    """format.ts zoneDistPct: |ref - edge| / ref * 100 (unsigned), null when either is missing or ref is 0."""
    if ref is None or edge is None or not _js_finite(ref) or not _js_finite(edge) or not ref:
        return None
    return (abs(float(ref) - float(edge)) / float(ref)) * 100


def _payload_of(row: Mapping[str, Any]) -> Mapping[str, Any]:
    """A signals row's payload as the dashboard reads it (`s.payload`): the parsed payload, else payload_json parsed,
    else (a flattened row) the row itself."""
    p = row.get("payload")
    if isinstance(p, Mapping):
        return p
    if row.get("payload_json"):
        try:
            parsed = json.loads(str(row["payload_json"]))
        except ValueError:
            return {}
        return parsed if isinstance(parsed, Mapping) else {}
    return row


def client_tier_label(tier: Any) -> str:
    """The tier word a client sees (signalctx.ts TIER_LABEL, client build), "Entry" for an untiered row."""
    return CLIENT_TIER_LABEL.get(str(tier), CLIENT_TIER_NONE) if tier else CLIENT_TIER_NONE


def client_signal_line(s: Mapping[str, Any]) -> str:
    """The CLIENT Copy line of a signals row (an /api/signals row, or a stored row): textfmt.ts signalLine, client build
    -- the tier word (never the kind code), " STRONG", "Last price", "volume x"; no E1 / E2 / LTP / latency / scanner."""
    p = _payload_of(s)

    def pv(key: str) -> str:
        v = p.get(key)
        return "-" if v is None else _js_text(v)

    ts = s.get("ts_ms")
    return " | ".join([
        f"{client_tier_label(s.get('tier'))}{' STRONG' if s.get('strong') else ''}",
        f"{pv('symbol')} {js_strike(p.get('strike'))} {pv('option_type')} {pv('tag')}",
        f"{_js_text(s.get('list_name'))} exp {_js_text(s.get('expiry_date'))}",
        f"{ist_hms(int(ts)) if _js_finite(ts) else '-'} IST",
        f"Last price {js_f2(s.get('ltp'))}",
        f"OI drop {js_f2(s.get('oi_drop_pct'))}%",
        f"volume x{js_f2(s.get('vol_ratio'))}",
        f"SL {js_f2(s.get('sl'))}",
    ])  # fmt: skip


def client_watch_line(r: Mapping[str, Any], muted: bool | None = None) -> str:
    """The CLIENT Copy line of a watchlist row AFTER the live merge (an /api/watchlists row: ltp / oi_now / oi_drop_pct
    live, `muted`): textfmt.ts watchLine, client build -- "Last price", no engine state word, MUTED last when muted."""
    is_muted = bool(r.get("muted")) if muted is None else muted
    return " | ".join([
        f"{_js_text(r.get('symbol'))} {js_strike(r.get('strike'))} {_js_text(r.get('option_type'))} {_js_text(r.get('tag'))}",
        f"{_js_text(r.get('list_name'))} exp {_js_text(r.get('expiry_date'))}",
        f"Last price {js_f2(r.get('ltp'))}",
        f"OI {js_inr(r.get('oi_now'))} drop {js_f2(r.get('oi_drop_pct'))}%",
        f"zone {js_f2(r.get('zone_edge'))} ({js_f2(_js_zone_dist_pct(r.get('ref_price'), r.get('zone_edge')))}%)",
        *(["MUTED"] if is_muted else []),
    ])  # fmt: skip


def client_signal_export_row(s: Mapping[str, Any]) -> dict[str, Any]:
    """One CLIENT_SIGNAL_EXPORT_COLUMNS row of a signals row: the client tier word, strong 1 / blank, last_price,
    volume_x (the stored ltp / vol_ratio)."""
    r = flatten_signal_row(s)
    return {
        "trade_date": r.get("trade_date"), "time_ist": r.get("time_ist"), "tier": client_tier_label(r.get("tier")),
        "strong": "1" if r.get("strong") else "", "symbol": r.get("symbol"), "strike": r.get("strike"),
        "option_type": r.get("option_type"), "tag": r.get("tag"), "list_name": r.get("list_name"),
        "expiry_date": r.get("expiry_date"), "last_price": r.get("ltp"), "oi_drop_pct": r.get("oi_drop_pct"),
        "volume_x": r.get("vol_ratio"), "zone_dist_pct": r.get("zone_dist_pct"), "sl": r.get("sl"),
    }  # fmt: skip


def client_watch_export_row(r: Mapping[str, Any], trade_date: str) -> dict[str, Any]:
    """One CLIENT_WATCHLIST_EXPORT_COLUMNS row of a live-merged watchlist row: last_price = the row's (live) ltp,
    zone_dist_pct = the dashboard's unsigned distance (format.ts zoneDistPct, the number the Copy line shows)."""
    return {
        "trade_date": trade_date, "list_name": r.get("list_name"), "expiry_date": r.get("expiry_date"),
        "symbol": r.get("symbol"), "strike": r.get("strike"), "option_type": r.get("option_type"), "tag": r.get("tag"),
        "oi": r.get("oi"), "lot_size": r.get("lot_size"), "last_price": r.get("ltp"),
        "zone_dist_pct": _js_zone_dist_pct(r.get("ref_price"), r.get("zone_edge")), "ref_price": r.get("ref_price"),
        "zone_edge": r.get("zone_edge"),
    }  # fmt: skip


def row_line(row: Mapping[str, Any], edition: str | None = None) -> str:
    """One plain-text line for a watchlist row or a signals row (clipboard copy). T-376: the CLIENT edition gets the
    dashboard's client line (`client_signal_line` / `client_watch_line`); the OWNER line below is unchanged."""
    if _is_client(edition):
        return client_signal_line(row) if str(row.get("kind")) in ("E1", "E2") else client_watch_line(row)
    if str(row.get("kind")) in ("E1", "E2"):
        r = flatten_signal_row(row)
        head = f"{r['kind']}{' STRONG' if r.get('strong') else ''} {r.get('symbol') or r.get('security_id')}"
        contract = (
            f"{head} {expiry_short(str(r.get('expiry_date') or ''))} {plain_num(r.get('strike') or 0)} "
            f"{r.get('option_type') or ''}{' ' + str(r['tag']) if r.get('tag') else ''}"
        )
        parts = [contract, str(r.get("list_name") or "")]
        if r.get("ltp") is not None:
            parts.append(f"LTP {rupees(r['ltp'])}")
        if r.get("oi_drop_pct") is not None:
            parts.append(f"OI -{float(r['oi_drop_pct']):.1f}%")
        if r.get("vol_ratio") is not None:
            parts.append(f"Vol {float(r['vol_ratio']):.1f}x")
        if r.get("zone_dist_pct") is not None:
            parts.append(f"zone {float(r['zone_dist_pct']):.1f}%")
        if r.get("sl") is not None:
            parts.append(f"SL {rupees(r['sl'])}")
        parts.append(f"{date_long(str(r.get('trade_date') or ''))} {r.get('time_ist', '')} IST")
        return " | ".join(parts)
    contract = (
        f"{row['symbol']} {expiry_short(str(row['expiry_date']))} {plain_num(row['strike'])} "
        f"{row.get('option_type') or ''}{' ' + str(row['tag']) if row.get('tag') else ''}"
    )
    parts = [contract, str(row.get("list_name") or "")]
    if row.get("oi") is not None:
        parts.append(f"OI {indian_int(row['oi'])}")
    if row.get("ltp") is not None:
        parts.append(f"LTP {rupees(row['ltp'])}")
    z = zone_dist_pct(row)
    if z is not None:
        parts.append(f"zone {z:.1f}%")
    if row.get("lot_size"):
        parts.append(f"lot {int(row['lot_size'])}")
    return " | ".join(parts)
