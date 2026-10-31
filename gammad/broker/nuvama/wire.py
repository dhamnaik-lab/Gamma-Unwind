"""Nuvama (APIConnect) wire facts, limits and PURE parsers (BROKER-ADAPTER-SPEC §8). Status: BETA-UNVERIFIED.

Evidence (read 2026-09-25; D-12: thin -- every fact below not marked MEASURED is PROVISIONAL until the first live
`scripts/smoke_broker.py --broker nuvama` run confirms it):
  SDK  = APIConnect 2.0.12 wheel from PyPI (MIT, sha256 a19b6f79...532b), READ for wire facts, never installed
         (spec §7): constants/constants.py:9-14 hosts, constants/router.py:166-216 paths, APIConnect/http.py:45-145
         headers + status handling, APIConnect/login_helper.py:55-121 login, APIConnect/APIConnect.py:994-1149
         charts, services/quote_service.py:17-40 quote, resources/chart_response_formatter.py:13-45 chart shape.
  DOC  = https://www.nuvamawealth.com/library-documentation/ (the API Connect reference page).
  SPEC = the same page, saved 2026-09-25 as docs/research/2026-09-25-nuvama-swagger/library-documentation.html
         (sha256 b4cf6cdd...e3f9, Last-Modified 2026-08-04): Nuvama's own spec-generated rendering of its OpenAPI
         document. Cited as `#<anchor>` / `opId<operationId>` / `#tocs_<schema>` (the brief in that folder, §3, is
         the verdict table). It declares NO servers (paths only) and its only full URLs are UAT
         (emtuat.edelweiss.in/edelmw-eq-uat, not publicly resolvable): this module NEVER points at a UAT host --
         the PROD host + prefixes below stay SDK-sourced (PROVISIONAL), the SPEC paths are exact suffixes of them.
  NV1/NV2, R7 N1-N14 = docs/research/BROKER-API-RESEARCH.md and the R7 report it cites.

MEASURED 2026-09-25 (BR4, scratch script, no credentials): the public instrument master
  https://nc.nuvamawealth.com/app/toccontracts/instruments.zip (3,413,294 bytes, Last-Modified 03:00:20 GMT, NV2)
  -> instruments.csv, column `exchangetoken` = "<NSE exchange token>_<exchange>". Against the canonical Dhan master
  of the same day, joined on the token: NSE OPTSTK 68,374 of 68,374 present, 0 mismatches of (underlying, expiry,
  strike, CE/PE), lot sizes identical; NSE OPTIDX 12,254 of 12,254, 0 mismatches; NSE_EQ series EQ 2,667 of 2,692
  present, symbols identical. Indices use negative ids: "Nifty 50" = -29 (canonical IDX_I 13, Dhan display "Nifty
  50"), "Nifty Bank" = -21 (IDX_I 25, "Nifty Bank"); no other index was matched by exact name.
  => the native id is a pure function of the canonical (segment, token): NATIVE_SUFFIX / INDEX_NATIVE below; the
  adapter still runs the spec §1 runtime identity guard against that day's instruments.zip (`parse_master`).

Verdicts against SPEC (2026-09-25). CONFIRMED = the SPEC states it; NOT IN SPEC = still PROVISIONAL, the smoke
run (`scripts/smoke_broker.py --broker nuvama`) must confirm it; CONTRADICTED facts were changed here.
  P1 NOT IN SPEC. The quote path answers index ids ("-29") like instrument ids (SDK docstring: "11536_NSE,
     4963_BSE, -29"). SPEC opIdgetMarketDepth `symbol` = "symbol of the scrip"; indices are never mentioned.
  P2 NOT IN SPEC (unit). Quote `data.qt.vol` is cumulative day volume in QUANTITY (shares), not lots. SPEC types
     REST `qt.vol` "string"; #reduced-quotes-streamer-responses calls `vol` "TOTAL VOLUME"; the only numeric sample
     is an equity (2885_NSE, 8589768), so F&O shares-vs-lots stays open.
  P3 NOT IN SPEC for REST (`"ltt": "string"`), but the SPEC's streamer samples carry TWO text formats, both IST
     wall clock: #reduced-quotes-streamer-responses "21 Jul 2023, 12:44:55 PM" (added to LTT_FORMATS 2026-09-25:
     it used to fall through to receive time silently) and #market-depth-streamer-responses "21/07/2023 12:35:39".
     Accepted: epoch digits (s or ms, TRUE epoch) or LTT_FORMATS text; anything else -> None (receive time).
  P4 format CONFIRMED, IST + bar-OPEN NOT IN SPEC. Chart `ltt` "YYYY-MM-DD HH:MM:SS" (#intraday-historical-data
     200 example rows ["2021-05-27 00:00:00", 11, 11.22, 11, 11, 56087]) is IST wall clock and marks the bar OPEN.
  P5 CONFIRMED: the documented WIRE shape is a list of rows `data: [[ltt, o, h, l, c, v], ...]` + `nextTillDate`
     (#intraday-historical-data 200 example; 6 columns, NO OI -> candle_oi False CONFIRMED). The data.pltPnts
     {ltt, open, high, low, close, vol} arrays shape is the SDK's reading (chart_response_formatter.py:18-28) and is
     NOT IN SPEC; both shapes parse.
  P6 Till_Date exclusive CONFIRMED (#intraday-historical-data: "the data is fetched until a day prior to the date
     selected") -> the first page sends to_date + 1 day. "One page covers the window" was CONTRADICTED by the
     paging token `nextTillDate` (SPEC sample "2006-05-29 00:00:00", older than every row: pages walk BACKWARDS):
     the adapter now pages with it (MAX_CHART_PAGES). NOT IN SPEC: the body field names (chTyp, conti, ltt -- the
     SPEC mis-documents the body as EQPlaceTradeRequestDao) and whether a datetime Till_Date (nextTillDate
     verbatim, what we send on page 2+) is honoured to the second or truncated to its day.
  P7 NOT IN SPEC. Daily bars via the SDK's custom-period body {chTyp, conti, frmDt, toDt, prdTyp: "CUST"} on D1
     (APIConnect.py:1067-1072); #eod-historical-data documents only Till_Date. Bars outside [from, to] are
     filtered here. The smoke run must see >= 30 D1 bars; the fallback is Till_Date + nextTillDate paging.
  P8 401 CONFIRMED (#tocs_sessionexpired; #place-trade 401 example {actCd "52", errCd "EGN0011", errMsg "Session
     Expired"}) -> canonical 807. 222 CONFIRMED for the quote (opIdgetMarketDepth "222 Unknown -- Empty reponse",
     errCd ETRD0002 "Seems like there are an issue with symbol.") -> no data, never a fabricated zero; for
     logindata 222 = "Request id expired" (opIdloginData, auth.py). 403 -> 809 is NOT IN SPEC.
  P9 SPEC declares ONE scheme, header `Authorization` (#authentication) -- CONFIRMED. Source, SourceToken, AppIdKey
     and X-Forwarded-For are NOT IN SPEC for data calls (SDK http.py:50-56; AppIdKey refreshed from every response
     header, http.py:67-68); sent anyway, harmless. STATIC IP: "optional" was CONTRADICTED -- #changelog Python
     2.0.11 / Node 2.0.15 (2026-03-30) "Static IP Mandatory": calls must leave from the public IP the client
     registered with Nuvama. X-Forwarded-For only REPEATS NUVAMA_STATIC_IP (it cannot change the egress IP); the SDK
     looks it up at api.ipify.org, which this app never calls. NUVAMA_STATIC_IP is now REQUIRED (__init__.py).
  P10 CONFIRMED (numbers): SPEC "API Usage Threshold" (h1 without id, saved page line 466): "Total API requests
      allowed per day 86400"; "Continuous API requests 3000 / 5 min / IP -> blocked for 1 min"; a daily breach
      blocks the app 15 min / 30 min / 1 h / 2 h / full day (5th breach) -- the "If breached?" cell is rowspan=4
      and covers the 86400 row. The daily budget was UNGUARDED until 2026-09-25: see the arithmetic at
      DAILY_REQUEST_CAP below and budget.py.
  P11 NOT IN SPEC: when the daily count resets. Assumed the IST calendar day ("per day"); if Nuvama counts a
      rolling 24 h, yesterday's afternoon counts against this morning -- the smoke run must watch a 2nd day.
"""

from __future__ import annotations

import csv
import io
import math
import re
from collections.abc import Mapping, Sequence
from datetime import date, datetime, time, timedelta
from typing import Any

from gammad.broker.base import Candle
from gammad.broker.canonical import ContractKey
from gammad.config import IST
from gammad.core import session_clock
from gammad.watch.feed_protocol import Quote

# ------------------------------------------------------------------------------------------ hosts + paths (SDK)
HOST = "https://nc.nuvamawealth.com"  # constants.py:9-14
PATH_LOGIN_VENDOR = "/edelmw-login/login/accounts/loginvendor/{api_key}/"  # router.py:196-197 (+ BASE_LOGIN)
PATH_LOGIN_DATA = "/edelmw-login/login/accounts/logindata/"  # router.py:202-203
PATH_QUOTE = "/edelmw-content/content/quote/scrip/{symbol}/"  # router.py:210-211, quote_service.py:20
PATH_CHART = "/edelmw-content/content/charts/v2/main/{interval}/{exc}/{atyp}/{symbol}"  # router.py:166-167
MASTER_URL = "https://nc.nuvamawealth.com/app/toccontracts/instruments.zip"  # constants.py:14, NV2
LOGIN_PAGE = "https://www.nuvamawealth.com/api-connect/login?api_key={api_key}"  # DOC "User Login Flow"
DOC_URL = "https://www.nuvamawealth.com/library-documentation/"

# ------------------------------------------------------------------------------------------ limits (P10)
RATE_PER_S = 3.0
POLL_S = 75.0
MAX_FEED_INSTRUMENTS = 225
QUOTE_BATCH = 25  # ids per PollFeed quotes() call; the adapter still sends ONE HTTP call per id

# SPEC "API Usage Threshold" (P10). The arithmetic that sizes RATE_PER_S / POLL_S / MAX_FEED_INSTRUMENTS and the
# guard stages (budget.py), every number derived by hand:
#   5-minute window: the adapter's TokenBucket (RATE_PER_S 3/s, capacity 3) admits at most 3 x 300 + 3 = 903 calls
#     per 300 s = 30% of 3,000. budget.py adds a process-wide window guard at WINDOW_GUARD 2,400 (80%) for the case
#     of several adapters on one data_dir (3 buckets = 2,709 would still pass; 4 would not).
#   One trading day at FULL load: the PollFeed polls 09:15-15:30 IST = 6 h 15 min = 22,500 s. 225 instruments / 3
#     per s = 75 s per cycle = POLL_S, so the feed never waits and spends 3/s x 22,500 s = 67,500 calls.
#   The rest of the day: warm-up (one M5 chart per subscribed contract per day, <= 225, x <= MAX_CHART_PAGES 5 =
#     1,125 worst case), the pre-market list build (one quote per strike per chain: ~50 chains x ~40 strikes =
#     ~2,000 a run), token_health probes and the daily instrument master. 86,400 - 67,500 = 18,900 left for them.
#   Pre-0.8.5 stages (SUPERSEDED by the re-size below; kept for the record): SLOW_AT 72,000 (83%) is above a full-load feed day plus 4,500 of build/warm-up, so a normal day never
#     degrades. Worst case modelled in tests (test_budget_simulated_day_*): a 20,000-call build before 09:15 ->
#     the full-load feed reaches 72,000 at 09:15 + 52,000/3 s = 14:04 IST; SLOW (1.5/s) reaches 78,000 at 15:11;
#     SHED (underlyings only, 25 ids / 150 s = 0.17/s) ends 15:30 near 78,200. STOP_AT 82,000 is never reached
#     by that day; it keeps BUDGET_RESERVE = 4,400 (5.1%) for the calls this process cannot count: the 2 login
#     calls, a smoke run or a second process on the same API key, <= 100 counts lost to a hard kill (budget.py).
# 0.8.5 RE-SIZE (adapter audit D4; simulated end to end in tests/test_t085_adapters_nuvama_budget.py). The 0.8.5
#   day: 09:10 prevclose run (213 daily charts) + 09:10 build, 09:20 re-mark + build, 22 refreshes 09:30-14:45 (each
#   ltp(213) + 0-8 chains, ~47 extra chains on a trend day, ONE QUOTE PER LEG), the poll feed 09:15-15:30, the
#   16:00 close run (213 daily + 213 day-bar charts). Demand on a trend day: feed 67,500 + ltp 23 x 213 = 4,899
#   (minus the feed's ~60-90 underlyings, answered from the quote cache since D3: ~23 x 140 = ~3,200 left) +
#   87 chains x 80 legs = 6,960 + warm-up ~200 + daily 213 = ~78,000 > 3/s x 22,800 s.
#   But the app, its jobs and the poll feed share ONE adapter (app.py adapter()), so ONE 3/s bucket: the day is
#   bounded by the BUCKET, not by demand. 09:10-15:30 = 22,800 s x 3 = 68,400 (+3 banked) + 16:00 close run 426 +
#   master/health ~30 = ~68,860 max for one process on any 0.8.5 day; REST work beyond the feed's share stretches
#   the feed cycle instead of adding calls. MEASURED in the simulation (2026-10-05, LEGS 80, 87 chains): 53,831
#   calls by 14:45, 62,004 by 15:30, 62,430 after the 16:00 run; ltp 23 snapshots = 3,527 wire calls (not 4,899).
#   Stages, in the order they degrade:
#     LEAN  70,000 (81 %): refresh CHAINS slowed to LEAN_CHAINS_PER_MARK per LEAN_MARK_S (new listings wait a mark);
#           every listed contract and underlying is still polled every POLL_S. Above the one-process ceiling
#           (bound ~68,860, headroom >= 1,140; simulated 62,430, headroom 7,570): only calls this process cannot
#           see get a day here.
#     SLOW  74,000: each id quoted at most every SLOW_POLL_S (the feed halves; ltp() is served from the cache).
#     SHED  79,000: option quotes, candles and chains refused; underlyings slow; health + master allowed.
#     STOP  82,000: everything refused until 00:00 IST; BUDGET_RESERVE 4,400 kept for what no process counts.
#   With X calls spent unseen before 09:15, the count at 14:45 is ~X + 60,300 (903 by 09:15 + 3/s x 19,800 s):
#   SHED (option quotes stop) before 14:45 needs X > ~18,700 by the bound (> 25,169 simulated: 79,000 - 53,831),
#   more once SLOW halves the feed; LEAN always comes first (simulated with X = 19,000: LEAN by 14:45, 4 chains
#   slowed, every listed contract in every cycle, SLOW after 14:45, no SHED all day).
DAILY_REQUEST_CAP = 86_400  # SPEC: "Total API requests allowed per day"
WINDOW_REQUEST_CAP = 3_000  # SPEC: "Continuous API requests 3000 / 5 min / IP"
WINDOW_S = 300.0
WINDOW_GUARD = 2_400
BUDGET_LEAN_AT = 70_000
BUDGET_SLOW_AT = 74_000
BUDGET_SHED_AT = 79_000
BUDGET_STOP_AT = 82_000
LEAN_CHAINS_PER_MARK = 1  # LEAN: option chains admitted per LEAN_MARK_S, process-wide (a trend day asks 2-3)
LEAN_MARK_S = 900.0  # one 0.8.5 refresh mark (15 min)
BUDGET_RESERVE = DAILY_REQUEST_CAP - BUDGET_STOP_AT
SLOW_POLL_S = 2 * POLL_S  # SLOW/SHED: an id is quoted at most every 150 s (half the feed's call rate)
# 0.8.5 D3: the freshness window. In SLOW / SHED the feed skips an id quoted less than this ago (the half-cycle slack
# absorbs cycle jitter), and ltp() answers such an id from the adapter's own last quote instead of the wire: a price
# at most 112.5 s old IS the current price for a 15-minute re-mark, and the skipped id would otherwise come back
# MISSING -> stale -> the re-mark refuses itself above 20 % stale.
QUOTE_FRESH_S = SLOW_POLL_S - 0.5 * POLL_S
LTP_BATCH = 1  # ids per ltp() HTTP call: there is no batch quote endpoint (one GET quote/scrip/{symbol}/ per id)

# SPEC opIdgetMarketDepth: "222 Unknown -- Empty reponse" (CommonErrorResponse, errCd ETRD0002)
HTTP_EMPTY = 222
HTTP_REQUEST_ID_EXPIRED = 222  # SPEC opIdloginData: "222 Unknown -- Request id expired" (CommonErrorResponse)
MAX_CHART_PAGES = 5  # P6: nextTillDate pages per intraday() call (a 1-day M5 window is expected to need 1-2)

# ------------------------------------------------------------------------------------------ native ids (MEASURED)
SEG_OPT = "NSE_FNO"
SEG_EQ = "NSE_EQ"
SEG_IDX = "IDX_I"
NATIVE_SUFFIX: dict[str, str] = {SEG_OPT: "_NFO", SEG_EQ: "_NSE"}
INDEX_NATIVE: dict[int, str] = {13: "-29", 25: "-21"}  # exact display-name matches only (module docstring)
CHART_EXCHANGE: dict[str, str] = {SEG_OPT: "NFO", SEG_EQ: "NSE", SEG_IDX: "INDEX"}  # ChartExchangeEnum
ASSET_TYPES = frozenset(
    {"FUTSTK", "FUTIDX", "OPTSTK", "OPTIDX", "EQUITY", "INDEX"}
)  # AssetTypeEnum (NSE ones)
INTRADAY_INTERVAL: dict[int, str] = {1: "M1", 3: "M3", 5: "M5", 15: "M15", 30: "M30", 60: "H1"}  # DOC
DAILY_INTERVAL = "D1"

SESSION_OPEN = time(9, 15)


def native_symbol(segment: str, security_id: int) -> str | None:
    """Canonical (segment, NSE token) -> Nuvama streaming symbol, or None when Nuvama has no known id for it."""
    sid = int(security_id)
    if segment == SEG_IDX:
        return INDEX_NATIVE.get(sid)
    suffix = NATIVE_SUFFIX.get(segment)
    return None if suffix is None else f"{sid}{suffix}"


def canonical_of(symbol: str) -> tuple[str, int] | None:
    """Nuvama streaming symbol -> canonical (segment, token), None for anything this app does not trade."""
    s = (symbol or "").strip()
    for seg, suffix in NATIVE_SUFFIX.items():
        if s.endswith(suffix) and s[: -len(suffix)].isdigit():
            return seg, int(s[: -len(suffix)])
    for sid, native in INDEX_NATIVE.items():
        if s == native:
            return SEG_IDX, sid
    return None


def chart_asset_type(segment: str, instrument: str) -> str:
    if segment == SEG_EQ:
        return "EQUITY"
    if segment == SEG_IDX:
        return "INDEX"
    inst = (instrument or "").strip().upper()
    return inst if inst in ASSET_TYPES else "OPTSTK"


# ------------------------------------------------------------------------------------------ master (identity guard)
MASTER_MEMBER = "instruments.csv"  # the one CSV inside instruments.zip (login_helper.py:141-146, MEASURED)
_MASTER_KINDS = {
    ("NFO", "OPTSTK"),
    ("NFO", "OPTIDX"),
    ("NFO", "FUTSTK"),
    ("NFO", "FUTIDX"),
    ("NSE", "EQUITY"),
}


def parse_master(text: str) -> dict[str, ContractKey]:
    """instruments.csv (MEASURED header: exchangetoken, tradingsymbol, symbolname, description, expiry "29/SEP/26",
    strikeprice, ticksize, lotsize, optiontype, series, assettype, exchange, ...) -> {streaming symbol: identity}
    for the NSE F&O and NSE equity rows (what the runtime identity guard compares with the canonical master)."""
    out: dict[str, ContractKey] = {}
    for r in csv.DictReader(io.StringIO(text)):
        exch = (r.get("exchange") or "").strip()
        kind = (r.get("assettype") or "").strip()
        if (exch, kind) not in _MASTER_KINDS:
            continue
        sym = (r.get("exchangetoken") or "").strip()
        name = (r.get("symbolname") or "").strip()
        if not sym or not name:
            continue
        if kind == "EQUITY":
            out[sym] = ContractKey(name)
            continue
        try:
            expiry = datetime.strptime((r.get("expiry") or "").strip(), "%d/%b/%y").date()  # noqa: DTZ007 -- a date
        except ValueError:
            continue
        opt = (r.get("optiontype") or "").strip().upper()
        if opt in ("CE", "PE"):
            strike = num(r.get("strikeprice"))
            if strike is None:
                continue
            out[sym] = ContractKey(name, expiry, strike, opt)
        else:
            out[sym] = ContractKey(name, expiry, None, None)
    return out


# ------------------------------------------------------------------------------------------ time (IST handling)
LTT_FORMATS = (
    "%Y-%m-%d %H:%M:%S",
    "%d-%m-%Y %H:%M:%S",
    "%d/%m/%Y %H:%M:%S",
    "%d %b %Y %H:%M:%S",
    "%d-%b-%Y %H:%M:%S",
    "%d/%b/%Y %H:%M:%S",
    "%d %b %Y, %I:%M:%S %p",  # SPEC #reduced-quotes-streamer-responses: "21 Jul 2023, 12:44:55 PM" (P3)
)


def ist_wall_to_epoch_ms(dt: datetime) -> int:
    """An IST wall-clock datetime (naive = IST) -> TRUE epoch ms (09:15:00 IST == 03:45:00 UTC)."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=IST)
    return int(dt.timestamp() * 1000)


def parse_ist_text(text: Any) -> datetime | None:
    """Nuvama date-time text (IST wall clock, P3/P4) -> IST-aware datetime, None when no known format fits."""
    s = str(text or "").strip()
    if not s:
        return None
    for fmt in LTT_FORMATS:
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=IST)
        except ValueError:
            continue
    return None


def parse_ltt_ms(value: Any) -> int | None:
    """Quote last-trade time (P3) -> TRUE epoch ms, or None (the feed then stamps receive time)."""
    if value is None or isinstance(value, bool):
        return None
    s = str(value).strip()
    if not s:
        return None
    if re.fullmatch(r"\d{9,13}", s):
        n = int(s)
        return n if n >= 10**12 else n * 1000  # epoch digits are TRUE epoch (s or ms by magnitude)
    dt = parse_ist_text(s)
    return None if dt is None else ist_wall_to_epoch_ms(dt)


# ------------------------------------------------------------------------------------------ numbers
def num(value: Any) -> float | None:
    """DOC types every quote field "string": '1388.60' / '' / '-' / None -> float or None."""
    if value is None or isinstance(value, bool):
        return None
    try:
        f = float(str(value).replace(",", "").strip())
    except ValueError:
        return None
    return None if math.isnan(f) or math.isinf(f) else f


def int_or_none(value: Any) -> int | None:
    f = num(value)
    return None if f is None else round(f)


# ------------------------------------------------------------------------------------------ quote (DOC, P1-P3)
def parse_quote(payload: Any, segment: str) -> Quote | None:
    """GET quote/scrip/{symbol}/ body -> Quote, None when it carries no quote. OI only for NSE_FNO (an equity or
    index has no open interest -- never reported as 0)."""
    if not isinstance(payload, Mapping):
        return None
    data = payload.get("data")
    qt = data.get("qt") if isinstance(data, Mapping) else None
    if not isinstance(qt, Mapping):
        return None
    ltp = num(qt.get("ltp"))
    vol = int_or_none(qt.get("vol"))
    oi = int_or_none(qt.get("opInt")) if segment == SEG_OPT else None
    if ltp is None and vol is None and oi is None:
        return None
    return Quote(ltp=ltp, volume_day=vol, oi=oi, ltt_ms=parse_ltt_ms(qt.get("ltt")))


# ------------------------------------------------------------------------------------------ charts (P4-P7)
def _chart_rows(payload: Any) -> list[tuple[Any, Any, Any, Any, Any, Any]]:
    if not isinstance(payload, Mapping):
        return []
    data = payload.get("data")
    if isinstance(data, Mapping) and isinstance(data.get("pltPnts"), Mapping):
        p = data["pltPnts"]
        cols = [p.get(k) or [] for k in ("ltt", "open", "high", "low", "close", "vol")]
        n = len(cols[0])
        if any(len(c) != n for c in cols[1:]):
            raise ValueError("nuvama chart: pltPnts arrays differ in length")
        return list(zip(*cols, strict=True))
    if isinstance(data, list):
        return [tuple(r[:6]) for r in data if isinstance(r, Sequence) and len(r) >= 6]
    return []


def parse_intraday(payload: Any, from_date: date, to_date: date) -> list[Candle]:
    """Chart body -> bars whose IST OPEN time is inside [from_date, to_date] and 09:15-15:30, oldest first."""
    out: dict[int, Candle] = {}
    for ltt, o, h, lo, c, v in _chart_rows(payload):
        dt = parse_ist_text(ltt)
        if dt is None or not (from_date <= dt.date() <= to_date):
            continue
        if not session_clock.in_fo_session(ist_wall_to_epoch_ms(dt)):
            continue
        prices = [num(x) for x in (o, h, lo, c)]
        if any(p is None for p in prices):
            continue
        ts = ist_wall_to_epoch_ms(dt)
        out[ts] = Candle(ts, prices[0], prices[1], prices[2], prices[3], float(num(v) or 0.0), None)
    return [out[k] for k in sorted(out)]


def parse_daily(payload: Any, from_date: date, to_date: date) -> list[Candle]:
    """Chart body (D1) -> one bar per IST trade date inside [from_date, to_date] (inclusive), ts = IST midnight."""
    out: dict[int, Candle] = {}
    for ltt, o, h, lo, c, v in _chart_rows(payload):
        dt = parse_ist_text(ltt)
        if dt is None:
            d = _date_only(ltt)
            if d is None:
                continue
        else:
            d = dt.date()
        if not (from_date <= d <= to_date):
            continue
        prices = [num(x) for x in (o, h, lo, c)]
        if any(p is None for p in prices):
            continue
        ts = ist_wall_to_epoch_ms(datetime.combine(d, time(0, 0)))
        out[ts] = Candle(ts, prices[0], prices[1], prices[2], prices[3], float(num(v) or 0.0), None)
    return [out[k] for k in sorted(out)]


def _date_only(value: Any) -> date | None:
    try:
        return date.fromisoformat(str(value).strip()[:10])
    except ValueError:
        return None


def intraday_body(to_date: date, till: str | None = None) -> dict[str, Any]:
    """SDK APIConnect.py:1002-1005. Page 1: Till_Date = to_date + 1 day because SPEC #intraday-historical-data says
    data ends the day BEFORE it (P6). Page 2+: `till` = the previous page's nextTillDate, sent verbatim."""
    ltt = till if till else (to_date + timedelta(days=1)).isoformat()
    return {"chTyp": "Interval", "conti": False, "ltt": ltt}


def next_till_date(payload: Any) -> str | None:
    """SPEC #intraday-historical-data 200: top-level `nextTillDate` (e.g. "2006-05-29 00:00:00"), the paging token
    for the next (OLDER) page. None when absent/blank (the SDK's pltPnts shape carries none: one page)."""
    if not isinstance(payload, Mapping):
        return None
    v = payload.get("nextTillDate")
    s = str(v).strip() if v is not None else ""
    return s or None


def oldest_chart_time(payload: Any) -> datetime | None:
    """The oldest parseable row time of a chart body (any time of day, session or not) -- what paging compares
    with the window start. None for an empty/unparseable page."""
    times = [dt for dt in (parse_ist_text(r[0]) for r in _chart_rows(payload)) if dt is not None]
    return min(times) if times else None


def window_start(from_date: date) -> datetime:
    """The first bar the intraday window can hold: from_date 09:15 IST."""
    return datetime.combine(from_date, SESSION_OPEN, tzinfo=IST)


def daily_body(from_date: date, to_date: date) -> dict[str, Any]:
    """SDK APIConnect.py:1067-1072 custom-period body (P7)."""
    return {
        "chTyp": "Interval",
        "conti": False,
        "frmDt": from_date.isoformat(),
        "toDt": to_date.isoformat(),
        "prdTyp": "CUST",
    }


# ------------------------------------------------------------------------------------------ errors / login
def error_text(payload: Any) -> str:
    """CommonErrorResponse {error: {errCd, errMsg}} -> short text (never echoes request data)."""
    if isinstance(payload, Mapping):
        err = payload.get("error")
        if isinstance(err, Mapping):
            return f"{err.get('errCd') or ''} {err.get('errMsg') or ''}".strip()[:200]
        if isinstance(err, str):
            return err[:200]
        msg = payload.get("message") or payload.get("msg")
        if isinstance(msg, str) and payload.get("success") is False:
            return msg[:200]
    return ""


def vendor_session(payload: Any) -> str:
    """loginvendor 200 body {"msg": <vendor session>, "success": ...} (DOC; login_helper.py:66-77)."""
    if isinstance(payload, Mapping) and "error" not in payload:
        return str(payload.get("msg") or "").strip()
    return ""


def session_auth(payload: Any) -> str:
    """logindata 200 body -> data.auth (DOC; login_helper.py:108)."""
    if isinstance(payload, Mapping):
        data = payload.get("data")
        if isinstance(data, Mapping):
            return str(data.get("auth") or "").strip()
    return ""
