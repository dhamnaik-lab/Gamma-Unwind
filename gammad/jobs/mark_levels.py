"""Step 1 job: mark support / resistance for every F&O stock and write `runs` + `levels`.

Bar cut-off (deliberate, see the builder report):
  ref="close" -> bars dated <= trade_date (16:00 / historical runs; the day's candle is closed).
                 T-221 (D195(c)+(d)): when the broker has not yet published trade_date's daily candle (Dhan: missing
                 at 16:00 AND 22:01 on 28 Sep), it is aggregated from the day's 5-min candles
                 (gammad/core/day_bar.py); an empty or failed intraday reply keeps the bars through D-1.
  ref="ltp"   -> bars dated <  trade_date (09:20 / 13:15; today's daily candle is still forming,
                 and a forming close must not retire a zone). Unchanged by T-221.
  ref="prevclose" (T-277, the 09:10 run) -> the ltp cut (bars dated < trade_date); reference price = the last bar's
                 close, no ltp call (at 09:10 a broker LTP is the 09:08 call-auction price, not the close).

T-277 price-only RE-MARK (`ref="ltp", base="auto" | <run id>`): when a full ok / partial prevclose or ltp run of the
SAME trade date exists (not refused by build_watchlists.levels_refusal), no daily candle is fetched: each stock's zones
come from that base run's zones_json, ONE adapter.ltp call prices every stock, pick_levels_daily (T-279) recomputes
support / resistance / distances, atr_pct is copied. A stock missing from the ltp reply keeps its previous row
(counted `stale=<N>`); more than MAX_STALE_PCT stale refuses the run (status failed, nothing written). No base -> the
full ltp run. `base=None` (the default) is the full run every caller had before.
T-309 (0.8.5 release audit): a re-mark that is not a refresh (09:20 / 13:15) FULLY marks every universe stock the base
lacks (failed or late at 09:10) in the same run, priced by the same ltp call (` full=<N>: ...`); later re-marks
re-price those rows' zones. A refresh stays price-only (_remark).
"""

from __future__ import annotations

import json
import math
import sqlite3
import time
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from itertools import pairwise
from typing import Any, Literal

import httpx

from gammad.broker.base import BrokerAdapter, BrokerAuthError, BrokerError, Candle
from gammad.broker.dhan import candle_date
from gammad.config import IST, Settings, now_ist, settings
from gammad.core.bar_guard import split_gap
from gammad.core.calendar import is_trading_day
from gammad.core.day_bar import with_day_bar
from gammad.core.levels import Levels, pick_levels_daily
from gammad.core.universe import UniverseRow, build_universe, cross_check
from gammad.core.zones import Zone, ZoneParams, ZoneState, compute_zones
from gammad.log import get_logger
from gammad.store import db

log = get_logger(__name__)

RefKind = Literal["ltp", "close", "prevclose"]
REFS: tuple[str, ...] = ("ltp", "close", "prevclose")
BEFORE_TRADE_DATE: frozenset[str] = frozenset({"ltp", "prevclose"})  # the bar cut: strictly before the trade date
BASE_REFS: tuple[str, ...] = ("prevclose", "ltp")  # a re-mark's base: a FULL run of one of these refs
AUTO_BASE = "auto"
LATE_BAR = "late_bar"  # R1: a prevclose stock whose last daily bar is not the previous trading day's
# T-277: a re-mark with more than this percent of its stocks missing from the ltp reply is refused (the build's
# MAX_LEVELS_FAILED_PCT, build_watchlists.py, for the same reason: a list built on stale prices looks like a quiet day)
MAX_STALE_PCT = 20


@dataclass(frozen=True)
class SymbolResult:
    row: UniverseRow
    candles: list[Candle]
    state: ZoneState
    levels: Levels | None


def lookback_start(trade_date: date, daily_bars: int) -> date:
    # ~248 NSE sessions a year -> 1.6 calendar days per bar leaves headroom for holidays.
    return trade_date - timedelta(days=math.ceil(daily_bars * 1.6) + 10)


def fetch_bars(
    adapter: BrokerAdapter, security_id: int, trade_date: date, ref: RefKind, daily_bars: int
) -> list[Candle]:
    raw = adapter.daily_candles(security_id, "NSE_EQ", lookback_start(trade_date, daily_bars), trade_date)
    if ref in BEFORE_TRADE_DATE:
        bars = [c for c in raw if candle_date(c) < trade_date]
    else:
        bars = [c for c in raw if candle_date(c) <= trade_date]
    return bars[-daily_bars:]


def load_universe(adapter: BrokerAdapter, universe_as_of: date | None = None) -> list[UniverseRow]:
    master = getattr(adapter, "scrip_master", None)
    if master is None:
        raise BrokerError("adapter exposes no scrip_master(); cannot build the F&O universe")
    as_of = universe_as_of or now_ist().date()
    rows = build_universe(master().df, as_of)
    cross_check([r.symbol for r in rows])
    return rows


def _zone_list(zs: Sequence[Zone]) -> list[list[float | int | None]]:
    def r(x: float) -> float | None:
        return round(x, 4) if math.isfinite(x) else None

    return [[r(z.top), r(z.bottom), z.formed_bar_index, z.confirmed_bar_index] for z in zs]


def zones_json(state: ZoneState, candles: Sequence[Candle]) -> str:
    """T-277: `last_close` (the last bar's close, unrounded) lets a price-only re-mark run the split guard's
    "reference price vs last close" test with no bars (bar_guard.split_gap check 2)."""
    atr_last = round(state.atr_last, 4) if math.isfinite(state.atr_last) else None
    last_close = float(candles[-1].close) if candles else None
    doc: dict[str, Any] = {
        "sell": _zone_list(state.live_sell),
        "buy": _zone_list(state.live_buy),
        "atr": atr_last,
        "n_bars": state.n_bars,
        "first_bar": candle_date(candles[0]).isoformat() if candles else None,
        "last_bar": candle_date(candles[-1]).isoformat() if candles else None,
        "last_close": last_close if last_close is not None and math.isfinite(last_close) else None,
    }
    return json.dumps(doc, separators=(",", ":"), allow_nan=False)


def zone_state_of(doc: dict[str, Any]) -> ZoneState:
    """The live zones a zones_json document holds (T-277 re-mark): edges as stored (4 dp), a null edge = NaN (never
    picked), the stored order kept (newest first: pick's ties go to the newer zone)."""

    def f(x: Any) -> float:
        return math.nan if x is None else float(x)

    def zs(side: Literal["sell", "buy"]) -> list[Zone]:
        out = []
        for top, bottom, formed, confirmed in doc.get(side) or []:
            t, b = f(top), f(bottom)
            out.append(Zone(t, b, (t + b) / 2, int(formed), int(confirmed), side))
        return out

    atr = doc.get("atr")
    return ZoneState(live_sell=zs("sell"), live_buy=zs("buy"), n_bars=int(doc.get("n_bars") or 0),
                     atr_last=math.nan if atr is None else float(atr))  # fmt: skip


ATR_LEN = 14  # Wilder ATR length (daily)
ATR_WINDOW = 40  # the newest candles of the cut the ATR is seeded and smoothed over


def atr_pct(candles: Sequence[Candle], n: int = ATR_LEN, window: int = ATR_WINDOW) -> float | None:
    """Daily ATR as PERCENT of the last close (2.3 = 2.3 %), 4 decimals; None below n + 1 candles or on a
    non-positive / non-finite close. S1.5.0 levels.atr_pct (CONTRACTS.md section 2), reproducing the research EXACTLY
    (docs/research/2026-09-29-v1-variants/capture.py wilder_atr 283-293 + AtrBook.get 336-337): take the `window`
    newest candles of the run's cut (fetch_bars: ref ltp = strictly before the trade date, ref close = through it);
    true range from the 2nd candle on (max(H-L, |H-prevC|, |L-prevC|)); seed = mean of the first n TRs; then
    atr = (atr x (n-1) + tr) / n; / the last candle's close x 100."""
    cs = list(candles)[-window:]
    if len(cs) < n + 1:
        return None
    trs = [max(c.high - c.low, abs(c.high - p.close), abs(c.low - p.close)) for p, c in pairwise(cs)]
    atr = sum(trs[:n]) / n
    for tr in trs[n:]:
        atr = (atr * (n - 1) + tr) / n
    last = cs[-1].close
    if not (math.isfinite(atr) and math.isfinite(last)) or last <= 0:
        return None
    return round(atr / last * 100.0, 4)


def level_row(
    trade_date: date,
    run_id: str,
    res: SymbolResult,
    spec_version: str,
) -> dict[str, Any]:
    lv = res.levels
    sup = lv.support if lv else None
    rst = lv.resistance if lv else None
    return {
        "trade_date": trade_date.isoformat(),
        "run_id": run_id,
        "symbol": res.row.symbol,
        "security_id": res.row.eq_security_id,
        "support_lo": sup.bottom if sup else None,
        "support_hi": sup.top if sup else None,
        "resistance_lo": rst.bottom if rst else None,
        "resistance_hi": rst.top if rst else None,
        "ref_price": lv.ref_price if lv else None,
        "dist_support_pct": lv.dist_support_pct if lv else None,
        "dist_resistance_pct": lv.dist_resistance_pct if lv else None,
        "zones_json": zones_json(res.state, res.candles),
        "spec_version": spec_version,
        "atr_pct": atr_pct(res.candles),
    }


FAILED_NAMES_MAX = 10  # T-150(c): names listed in the runs note; the COUNT is always exact
# 0.8.3: seconds to wait before the one retry pass over symbols whose candles failed transiently (lets a broker's rate
# window drain). Tests patch `_pause` / `_clock`; nothing else in the job sleeps.
SECOND_PASS_PAUSE_S = 15.0
# The second pass is skipped when the main pass already took longer than this: a fully throttled universe can cost
# ~40 s a symbol through the adapter's own retries, and a second pass would double a run that is already late.
SECOND_PASS_MAX_ELAPSED_S = 600.0
TRANSIENT_STATUS = frozenset({403, 429})  # plus any 5xx; Angel's rate limit is a 403 "exceeding access rate"
_pause = time.sleep
_clock = time.monotonic


def _fyers_rate_block(exc: BrokerError, adapter: BrokerAdapter) -> bool:
    """Only stop the full symbol loop on FYERS rate limits (other brokers unchanged)."""
    return exc.status == 429 and type(adapter).__module__.startswith("gammad.broker.fyers")


def _fyers_history_invalid(exc: BrokerError, adapter: BrokerAdapter) -> bool:
    """Terminal FYERS historical-data parameter refusal: fail the job, not 200+ individual symbols.

    A 422/-50 is not a rate limit and cannot be repaired by retrying the same
    parameters. Keep the requested history horizon and strategy unchanged.
    """
    return (exc.status == 422 and str(exc.code) == "-50"
            and type(adapter).__module__.startswith("gammad.broker.fyers"))


# R9: Current installation's owner confirmed FYERS Standard history entitlement.
# This explicit, source-pinned tier is NOT inferred from token claims or broker name.
# Revisit the policy under source review if the account later becomes Prime.
FYERS_HISTORY_ACCESS_TIER = "standard"
FYERS_STANDARD_HISTORY_YEARS = 5


class FyersHistoryUnavailable(BrokerError):
    """FYERS Standard cannot satisfy the configured daily-bar history horizon."""


def _fyers_history_preflight(adapter: BrokerAdapter, trade_date: date, daily_bars: int) -> None:
    """Stop clearly unsupported Standard history before any candle request.

    Does not change the 2,000-bar strategy, the existing R4 rejection handler,
    or the behavior of other brokers. A price-only re-mark needs no history.
    """
    if FYERS_HISTORY_ACCESS_TIER != "standard" or not type(adapter).__module__.startswith("gammad.broker.fyers"):
        return
    today = now_ist().date()
    try:
        oldest = today.replace(year=today.year - FYERS_STANDARD_HISTORY_YEARS)
    except ValueError:  # Feb 29 in a leap year
        oldest = today.replace(year=today.year - FYERS_STANDARD_HISTORY_YEARS, day=28)
    wanted = lookback_start(trade_date, daily_bars)
    if wanted < oldest:
        raise FyersHistoryUnavailable(
            "FYERS Standard daily history insufficient: "
            f"requested_from={wanted.isoformat()} available_from_approx={oldest.isoformat()} "
            f"daily_bars={daily_bars}; obtain authorized full history or appropriate entitlement; "
            "not shortening bars or sending history requests"
        )


def is_transient(exc: BrokerError) -> bool:
    """A failure a retry can fix: BrokerError.status 403 / 429 / >= 500, or raised from an httpx transport error
    (connect / read / timeout / dropped connection). Anything else -- an identity miss, a bad request, an empty
    reply -- fails the same way every time and is not retried."""
    status = exc.status or 0
    if status in TRANSIENT_STATUS or status >= 500:
        return True
    cause: BaseException | None = exc
    while cause is not None:
        if isinstance(cause, httpx.TransportError):
            return True
        cause = cause.__cause__
    return False


def failed_note(ref: str, failed: Sequence[str]) -> str:
    """runs.note of a finished run: 'ref=<r>' or 'ref=<r> failed=<N>: A,B,...,J,+k' (at most FAILED_NAMES_MAX names).
    build_watchlists.levels_refusal reads N to refuse a build on a mostly-failed run (T-150(a))."""
    if not failed:
        return f"ref={ref}"
    names = ",".join(failed[:FAILED_NAMES_MAX])
    more = len(failed) - FAILED_NAMES_MAX
    return f"ref={ref} failed={len(failed)}: {names}" + (f",+{more}" if more > 0 else "")


def _names(label: str, names: Sequence[str]) -> str:
    """' <label>=<N>: A,B,...,+k' (at most FAILED_NAMES_MAX names; the count is exact), '' for none."""
    if not names:
        return ""
    more = len(names) - FAILED_NAMES_MAX
    return f" {label}={len(names)}: " + ",".join(names[:FAILED_NAMES_MAX]) + (f",+{more}" if more > 0 else "")


def _is_remark(note: str | None) -> bool:
    return " base=" in f" {note or ''}"


def _ref_of(note: str | None) -> str:
    first = str(note or "").split(" ", 1)[0]
    return first.split("=", 1)[1] if first.startswith("ref=") else ""


def _all_price_runs(conn: sqlite3.Connection, trade_date: date) -> list[tuple[str, str, str]]:
    """(run_id, note, started_ist) of every ok / partial prevclose / ltp levels run of trade_date that wrote levels
    (full or re-mark), newest first."""
    cur = conn.execute(
        "SELECT r.run_id, r.note, r.started_ist FROM runs r WHERE r.kind = 'mark_levels' "
        "AND r.status IN ('ok', 'partial') "
        "AND EXISTS (SELECT 1 FROM levels l WHERE l.run_id = r.run_id AND l.trade_date = ?) "
        "ORDER BY r.started_ist DESC, r.run_id DESC",
        (trade_date.isoformat(),),
    )
    return [(str(a), str(b or ""), str(c or "")) for a, b, c in cur.fetchall() if _ref_of(b) in BASE_REFS]


def _full_runs(conn: sqlite3.Connection, trade_date: date) -> list[tuple[str, str, str]]:
    """The FULL runs of _all_price_runs. A re-mark (note ' base=') is never a base: its zones are a copy."""
    return [r for r in _all_price_runs(conn, trade_date) if not _is_remark(r[1])]


def find_base(conn: sqlite3.Connection, trade_date: date) -> str | None:
    """The run a price-only re-mark of trade_date reads its zones from: the NEWEST full prevclose / ltp run of that
    date, or None when there is none or that run is refused (build_watchlists.levels_refusal: > 20 % failed): the
    caller then makes today's full ltp run (design C1: "no base -> the full run does both jobs")."""
    from gammad.jobs.build_watchlists import levels_refusal  # lazy: build_watchlists is the consumer

    runs = _full_runs(conn, trade_date)
    if not runs:
        return None
    run_id = runs[0][0]
    refused = levels_refusal(conn, run_id)
    if refused:
        log.warning("mark_levels: the newest base %s is refused (%s): a full ltp run instead", run_id, refused)
        return None
    return run_id


def _ref_split_gap(doc: dict[str, Any], ref_price: float) -> str | None:
    """bar_guard.split_gap's "reference price vs last close" check (check 2) on a zones_json document, with the
    guard's own wording. None when the document has no last_close / last_bar (a base written before 0.8.5)."""
    last_close, last_bar = doc.get("last_close"), doc.get("last_bar")
    if last_close is None or not last_bar:
        return None
    d = date.fromisoformat(str(last_bar))
    ts = int(datetime(d.year, d.month, d.day, tzinfo=IST).timestamp() * 1000)
    px = float(last_close)
    return split_gap([Candle(ts, px, px, px, px, 0.0)], ref_price)


def _mark_one(
    trade_date: date,
    run_id: str,
    u: UniverseRow,
    bars: list[Candle],
    ref_price: float | None,
    params: ZoneParams,
    spec_version: str,
) -> tuple[dict[str, Any], str | None, Levels | None]:
    """One stock's FULL mark from its bars -> (levels row, the split guard's reason or None, levels or None). A full
    run's per-stock step, shared with the 09:20 full mark of a stock the base lacks (T-309)."""
    gap = split_gap(bars, ref_price)  # T-285: an unadjusted split / bonus -> the row is refused, not zoned
    if gap is not None:
        log.warning("mark_levels: %s bars not trusted: %s", u.symbol, gap)
        doc = {**json.loads(zones_json(ZoneState(), bars)), "untrusted": gap}
        row = {**level_row(trade_date, run_id, SymbolResult(u, [], ZoneState(), None), spec_version),
               "ref_price": ref_price, "zones_json": json.dumps(doc, separators=(",", ":"))}  # fmt: skip
        return row, gap, None
    state = compute_zones(bars, params)
    lv = pick_levels_daily(state, ref_price) if ref_price and ref_price > 0 else None  # T-279: inside = AT
    if lv is None:
        log.warning("mark_levels: %s has no reference price (bars=%d)", u.symbol, len(bars))
    return level_row(trade_date, run_id, SymbolResult(u, bars, state, lv), spec_version), None, lv


def _remark(
    conn: sqlite3.Connection,
    trade_date: date,
    run_id: str,
    base_id: str,
    symbols: Sequence[str] | None,
    adapter: BrokerAdapter,
    spec_version: str,
    refresh_hm: str | None,
    params: ZoneParams,
    daily_bars: int,
    universe_as_of: date | None,
    bars_sink: dict[int, list[Candle]] | None = None,
) -> tuple[str, str, dict[str, Any]]:
    """T-277 price-only re-mark of `base_id`'s stocks -> (status, runs.note, summary fields). Writes the rows unless
    the run is refused (> MAX_STALE_PCT stale: status 'failed', nothing written).

    T-309 (0.8.5 release audit): the re-priced stocks are the base's PLUS any stock the base lacks that a later price
    run of the date carries (the 09:20 full mark below), re-priced from that row's zones the same way. A mark that is
    NOT a refresh (`refresh_hm` None: the 09:20 / 13:15 mark) also gives every universe stock still absent (failed or
    late at 09:10) a FULL mark -- its daily candles + zones, exactly as a full ltp run -- priced by the SAME single ltp
    call; counted ` full=<N>: ...`, a stock that fails again ` failed=<N>: ...` (status partial). A refresh stays
    price-only. A universe that cannot be loaded leaves the re-mark price-only (` universe=unavailable`).
    T-309 follow-up: each fully marked stock's daily bars (the full ltp run's cut) go into `bars_sink` exactly as a full
    run puts them, so the V2 regime step can compute ITS regime (summary `regime_bars` = how many; the base stocks'
    regimes came from the run that fetched their bars and are not in the sink)."""
    base_rows = db.fetch_levels(conn, trade_date.isoformat(), base_id)
    # a stale stock keeps its PREVIOUS row: the newest ok / partial ltp / prevclose run of the date at or after the base
    # (the last re-mark, or the base itself)
    base_started = str(conn.execute("SELECT started_ist FROM runs WHERE run_id = ?", (base_id,)).fetchone()[0] or "")
    prev_run = next((rid for rid, note, started in _all_price_runs(conn, trade_date) if started >= base_started), base_id)
    prev = {str(r["symbol"]): r for r in db.fetch_levels(conn, trade_date.isoformat(), prev_run)}
    in_base = {str(r["symbol"]) for r in base_rows}
    base_rows = base_rows + [r for s, r in prev.items() if s not in in_base and r.get("zones_json")]  # T-309 carried
    wanted = {s.strip().upper() for s in symbols if s.strip()} if symbols else None
    if wanted is not None:
        base_rows = [r for r in base_rows if r["symbol"] in wanted]
    extra: list[UniverseRow] = []
    universe_error: str | None = None
    if refresh_hm is None:
        try:
            universe = load_universe(adapter, universe_as_of)
        except BrokerAuthError:
            raise
        except BrokerError as exc:
            universe_error = str(exc)
            log.warning("mark_levels: re-mark %s: universe not loaded (%s); no stock the base lacks is marked",
                        run_id, exc)  # fmt: skip
            universe = []
        have = {str(r["symbol"]) for r in base_rows}
        extra = [u for u in universe if u.symbol not in have and (wanted is None or u.symbol in wanted)]
    if wanted is not None:
        unknown = wanted - {str(r["symbol"]) for r in base_rows} - {u.symbol for u in extra}
        if unknown:
            log.warning("mark_levels: not in the base run %s, skipped: %s", base_id, ", ".join(sorted(unknown)))
    if extra:  # T-309 can fetch missing stocks' FULL daily history even inside a price re-mark.
        _fyers_history_preflight(adapter, trade_date, daily_bars)
    ids = [int(r["security_id"]) for r in base_rows] + [u.eq_security_id for u in extra]
    ltps = adapter.ltp(ids, "NSE_EQ") if ids else {}  # ONE price call for the base AND the stocks it lacks
    rows: list[dict[str, Any]] = []
    stale: list[str] = []
    bad: list[str] = []
    n_sup = n_res = 0
    for b in base_rows:
        sym = str(b["symbol"])
        px = ltps.get(int(b["security_id"]))
        if px is None or not math.isfinite(float(px)) or float(px) <= 0:
            stale.append(sym)
            row = {**prev.get(sym, b), "run_id": run_id, "spec_version": spec_version}
            if "untrusted" in json.loads(row["zones_json"] or "{}"):
                bad.append(sym)
            n_sup += row.get("support_hi") is not None
            n_res += row.get("resistance_lo") is not None
            rows.append(row)
            continue
        px = float(px)
        doc = json.loads(b["zones_json"] or "{}")
        out = {"trade_date": trade_date.isoformat(), "run_id": run_id, "symbol": sym,
               "security_id": int(b["security_id"]), "support_lo": None, "support_hi": None, "resistance_lo": None,
               "resistance_hi": None, "ref_price": px, "dist_support_pct": None, "dist_resistance_pct": None,
               "zones_json": b["zones_json"], "spec_version": spec_version, "atr_pct": None}  # fmt: skip
        gap = None if "untrusted" in doc else _ref_split_gap(doc, px)
        if "untrusted" in doc or gap is not None:  # T-285: an untrusted base row is copied untrusted; a new gap refuses
            bad.append(sym)
            if gap is not None:
                log.warning("mark_levels: %s bars not trusted: %s", sym, gap)
                refused = {**doc, "sell": [], "buy": [], "atr": None, "n_bars": 0, "untrusted": gap}
                out["zones_json"] = json.dumps(refused, separators=(",", ":"))
            rows.append(out)
            continue
        lv = pick_levels_daily(zone_state_of(doc), px)  # T-279: inside = AT
        sup, rst = lv.support, lv.resistance
        out.update(support_lo=sup.bottom if sup else None, support_hi=sup.top if sup else None,
                   resistance_lo=rst.bottom if rst else None, resistance_hi=rst.top if rst else None,
                   dist_support_pct=lv.dist_support_pct, dist_resistance_pct=lv.dist_resistance_pct,
                   atr_pct=b.get("atr_pct"))  # fmt: skip
        n_sup += sup is not None
        n_res += rst is not None
        rows.append(out)
    n = len(base_rows)
    fields: dict[str, Any] = {"n_symbols": len(rows), "n_with_support": n_sup, "n_with_resistance": n_res,
                              "n_failed": 0, "n_untrusted": len(bad), "untrusted": bad[:FAILED_NAMES_MAX],
                              "n_stale": len(stale), "stale": stale[:FAILED_NAMES_MAX], "n_full": 0, "full": [],
                              "universe_error": universe_error, "regime_bars": 0}  # fmt: skip
    head = f"ref=ltp base={base_id}" + (f" refresh={refresh_hm}" if refresh_hm else "")
    pct = 100.0 * len(stale) / n if n else 0.0
    if pct > MAX_STALE_PCT:
        why = f"stale={len(stale)}/{n} ({pct:.0f}%) > {MAX_STALE_PCT}%"
        log.error("mark_levels %s: refused, %s: no levels written; the build must not run", run_id, why)
        fields.update(n_symbols=0, error=f"re-mark refused: {why}")
        return "failed", f"{head} refused: {why}" + _names("stale", stale), fields
    full: list[str] = []
    failed: list[str] = []
    sunk = 0
    for u in extra:  # T-309: the stocks the base lacks, each FULLY marked (no second pass: the next 13:15 mark retries)
        try:
            bars = fetch_bars(adapter, u.eq_security_id, trade_date, "ltp", daily_bars)
        except BrokerAuthError:
            raise
        except BrokerError as exc:
            if _fyers_rate_block(exc, adapter):
                raise  # abort the job; do not count 200+ locally suppressed calls as symbol failures
            if _fyers_history_invalid(exc, adapter):
                raise  # terminal -50/422: fail the whole job; do not produce partial levels
            log.warning("mark_levels: %s candles failed: %s", u.symbol, exc)
            failed.append(u.symbol)
            continue
        if bars_sink is not None:  # the V2 regime's bars, as a full ltp run hands them (T-309 follow-up)
            bars_sink[u.eq_security_id] = list(bars)
            sunk += 1
        px = ltps.get(u.eq_security_id)
        px = float(px) if px is not None and math.isfinite(float(px)) and float(px) > 0 else None
        row, gap, lv = _mark_one(trade_date, run_id, u, bars, px, params, spec_version)
        if gap is not None:
            bad.append(u.symbol)
        n_sup += bool(lv and lv.support)
        n_res += bool(lv and lv.resistance)
        rows.append(row)
        full.append(u.symbol)
    if extra:
        log.info("mark_levels %s: %d stocks fully marked (missing from base %s), %d failed", run_id, len(full),
                 base_id, len(failed))  # fmt: skip
    fields.update(n_symbols=len(rows), n_with_support=n_sup, n_with_resistance=n_res, n_failed=len(failed),
                  n_untrusted=len(bad), untrusted=bad[:FAILED_NAMES_MAX], n_full=len(full),
                  full=full[:FAILED_NAMES_MAX], regime_bars=sunk)  # fmt: skip
    db.upsert_levels(conn, rows)
    note = (head + _names("full", full) + _names("failed", failed) + _names("stale", stale) + _names("untrusted", bad)
            + (" universe=unavailable" if universe_error else ""))  # fmt: skip
    return ("partial" if failed else "ok"), note, fields


def run(
    trade_date: date,
    ref: RefKind = "close",
    symbols: Sequence[str] | None = None,
    *,
    adapter: BrokerAdapter | None = None,
    conn: sqlite3.Connection | None = None,
    cfg: Settings | None = None,
    universe_as_of: date | None = None,
    bars_sink: dict[int, list[Candle]] | None = None,
    base: str | None = None,
    refresh_hm: str | None = None,
    prev_trading_day: date | None = None,
) -> dict[str, Any]:
    """`bars_sink` (S1.7.0, T-267 hunk A): when given, each symbol's daily candles as fetched (eq_security_id ->
    candles, the fetch_bars cut) are put in it, so the app's V2 regime step reads the SAME bars with no second
    fetch (PLAN-T267 M14). It changes nothing about the levels. A re-mark fetches no bars for its base stocks; only
    the stocks its 09:20 full mark adds (T-309) are put in it (summary `regime_bars`, 0 for a refresh).

    T-277: `base` (ref ltp only) = None: a full run (fetch every stock's daily candles; the behaviour before 0.8.5);
    "auto": a price-only re-mark on find_base(), or the full run when there is no usable base; a run id: a re-mark
    on that run (ValueError unless it is a full ok / partial prevclose / ltp run of trade_date). `refresh_hm` ("HHMM",
    the 15-minute mark) is written into the note as ` refresh=<HHMM>`.
    The summary says what ran: mode "full" | "remark", bars_fetched (True only for a full run: the app runs the V2
    regime step on those only), base_run_id, n_stale / stale (re-mark), refresh_hm.

    R1 (broker audit S-7): `prev_trading_day` (ref prevclose only; the caller passes the calendar's previous trading
    day, None = no check) -- a stock whose last bar is not dated that day FAILS with reason `late_bar` (named
    `SYM(late_bar)` in the note's failed list) and writes no row; > 20 % failed refuses the run as before."""
    if ref not in REFS:
        raise ValueError(f"ref must be one of {REFS}")
    if base is not None and ref != "ltp":
        raise ValueError("base (a price-only re-mark) applies to ref='ltp' only")
    if prev_trading_day is not None and ref != "prevclose":
        raise ValueError("prev_trading_day (the late_bar check) applies to ref='prevclose' only")
    if refresh_hm is not None and not (len(refresh_hm) == 4 and refresh_hm.isdigit()):
        raise ValueError(f"refresh_hm must be 'HHMM', got {refresh_hm!r}")
    t0 = time.perf_counter()
    cfg = cfg or settings()
    if adapter is None:
        from gammad.broker.dhan import DhanAdapter

        adapter = DhanAdapter.from_settings()
    own_conn = conn is None
    conn = conn or db.connect(cfg.paths.db_path)
    db.migrate(conn)

    base_id: str | None = None
    if base == AUTO_BASE:
        base_id = find_base(conn, trade_date)
    elif base is not None:
        if base not in {r[0] for r in _full_runs(conn, trade_date)}:
            if own_conn:
                conn.close()
            raise ValueError(f"{base} is not a full ok / partial prevclose or ltp levels run of {trade_date}")
        base_id = base

    params = ZoneParams.from_config(cfg.zones)
    started = now_ist()
    run_id = f"levels-{trade_date.isoformat()}-{ref}-{started:%Y%m%dT%H%M%S%f}"
    head = f"ref={ref}" + (f" base={base_id}" if base_id else "") + (f" refresh={refresh_hm}" if refresh_hm else "")
    db.insert_run(conn, run_id, "mark_levels", started.isoformat(), cfg.spec_version, note=head)

    status, note = "failed", None
    summary: dict[str, Any] = {"run_id": run_id, "trade_date": trade_date.isoformat(), "ref": ref,
                               "mode": "remark" if base_id else "full", "bars_fetched": False,
                               "base_run_id": base_id, "refresh_hm": refresh_hm}  # fmt: skip
    try:
        if base_id is not None:  # T-277: the price-only re-mark
            status, note, fields = _remark(conn, trade_date, run_id, base_id, symbols, adapter, cfg.spec_version,
                                           refresh_hm, params, cfg.zones.daily_bars, universe_as_of,
                                           bars_sink)  # fmt: skip
            summary.update(fields)
            return summary
        _fyers_history_preflight(adapter, trade_date, cfg.zones.daily_bars)
        summary["bars_fetched"] = True
        universe = load_universe(adapter, universe_as_of)
        if symbols:
            wanted = {s.strip().upper() for s in symbols if s.strip()}
            unknown = wanted - {r.symbol for r in universe}
            if unknown:
                log.warning("mark_levels: not in the F&O universe, skipped: %s", ", ".join(sorted(unknown)))
            universe = [r for r in universe if r.symbol in wanted]

        ltps: dict[int, float] = {}
        if ref == "ltp" and universe:
            ltps = adapter.ltp([r.eq_security_id for r in universe], "NSE_EQ")

        rows: list[dict[str, Any]] = []
        n_sup = n_res = 0
        # T-221: complete trade_date's daily bar from its intraday candles (ref close only; a weekend / NSE holiday
        # has no session, so no intraday call is made for it)
        want_day_bar = ref == "close" and is_trading_day(trade_date)
        if ref == "close" and not want_day_bar:
            log.info("mark_levels: %s is not a trading day; no day bar is fetched", trade_date.isoformat())
        day_bar = {"intraday": 0, "missing": 0, "failed": 0}

        def complete_day_bar(u: UniverseRow, bars: list[Candle]) -> list[Candle]:
            """`bars` plus trade_date's bar when the daily series lacks it. A non-auth intraday failure keeps `bars`:
            the symbol's daily candles were fine, so it is never a failed symbol (counted, WARNING)."""
            try:
                full, how = with_day_bar(adapter, u.eq_security_id, bars, trade_date)
            except BrokerAuthError:
                raise
            except BrokerError as exc:
                if _fyers_rate_block(exc, adapter) or _fyers_history_invalid(exc, adapter):
                    raise  # terminal FYERS history rejection must not become stale daily bars
                day_bar["failed"] += 1
                log.warning("mark_levels: %s day bar %s: intraday failed (%s); zones use the daily bars only",
                            u.symbol, trade_date.isoformat(), exc)  # fmt: skip
                return bars
            if how == "missing":
                day_bar["missing"] += 1
                log.warning("mark_levels: %s day bar %s: no daily candle and no intraday candles; zones use the "
                            "daily bars only", u.symbol, trade_date.isoformat())  # fmt: skip
            elif how == "intraday":
                day_bar["intraday"] += 1
            return full[-cfg.zones.daily_bars :]

        untrusted: set[str] = set()  # T-285: symbols whose bars failed the split guard this run
        late: set[str] = set()  # R1: prevclose symbols whose last bar is not the previous trading day

        def one(u: UniverseRow) -> BrokerError | None:
            """Fetch + mark one symbol; the BrokerError when its candles failed, else None. Auth errors propagate."""
            nonlocal n_sup, n_res
            try:
                bars = fetch_bars(adapter, u.eq_security_id, trade_date, ref, cfg.zones.daily_bars)
            except BrokerAuthError:
                raise
            except BrokerError as exc:
                if _fyers_rate_block(exc, adapter):
                    raise
                if _fyers_history_invalid(exc, adapter):
                    raise  # terminal -50/422: halt the full scan instead of per-symbol failures
                log.warning("mark_levels: %s candles failed: %s", u.symbol, exc)
                return exc
            if prev_trading_day is not None and (not bars or candle_date(bars[-1]) != prev_trading_day):
                # R1 (broker audit S-7): a prevclose reference must be the previous trading day's close; a missing D-1
                # bar would silently make it D-2's. A failure with its own reason, never a row (nor regime bars).
                late.add(u.symbol)
                log.warning("mark_levels: %s last bar %s is not the previous trading day %s (%s)", u.symbol,
                            candle_date(bars[-1]).isoformat() if bars else None, prev_trading_day, LATE_BAR)  # fmt: skip
                return BrokerError(f"{LATE_BAR}: last bar is not {prev_trading_day.isoformat()}")
            if bars_sink is not None:
                bars_sink[u.eq_security_id] = list(bars)
            if want_day_bar:
                bars = complete_day_bar(u, bars)
            ref_price = ltps.get(u.eq_security_id) if ref == "ltp" else (bars[-1].close if bars else None)
            row, gap, lv = _mark_one(trade_date, run_id, u, bars, ref_price, params, cfg.spec_version)
            if gap is not None:
                untrusted.add(u.symbol)
            n_sup += bool(lv and lv.support)
            n_res += bool(lv and lv.resistance)
            rows.append(row)
            return None

        main_t0 = _clock()
        errors = {u.symbol: e for u in universe if (e := one(u)) is not None}
        main_s = _clock() - main_t0
        transient = [u for u in universe if u.symbol in errors and is_transient(errors[u.symbol])]
        if transient and main_s > SECOND_PASS_MAX_ELAPSED_S:
            log.info("mark_levels second pass: skipped (main pass took %.0fs)", main_s)
        elif transient:
            # 0.8.3: ONE retry pass over the symbols whose candles failed TRANSIENTLY (rate limit / 5xx / transport),
            # after a pause; broker-neutral. An auth error here still aborts the run.
            _pause(SECOND_PASS_PAUSE_S)
            recovered = [u.symbol for u in transient if one(u) is None]
            for sym in recovered:
                del errors[sym]
            log.info("mark_levels second pass: retried %d, recovered %d", len(transient), len(recovered))
        failed = [u.symbol for u in universe if u.symbol in errors]
        late &= set(failed)  # a late bar is not transient (never retried); kept honest if that ever changes
        failed_names = [f"{s}({LATE_BAR})" if s in late else s for s in failed]  # R1: the reason rides on the name

        bad = [u.symbol for u in universe if u.symbol in untrusted]  # T-285: refused rows, never a failure

        db.upsert_levels(conn, rows)
        status = "ok" if not failed else "partial"
        note = head + failed_note(ref, failed_names)[len(f"ref={ref}") :]  # T-277: ` refresh=<HHMM>` after the ref
        if bad:  # " untrusted=<N>: A,B,...,+k" (build_watchlists' `failed=<N>:` reader does not match it)
            more = len(bad) - FAILED_NAMES_MAX
            note += f" untrusted={len(bad)}: " + ",".join(bad[:FAILED_NAMES_MAX]) + (f",+{more}" if more > 0 else "")
        summary.update(
            n_symbols=len(rows),
            n_with_support=n_sup,
            n_with_resistance=n_res,
            n_failed=len(failed),
            n_untrusted=len(bad),
            untrusted=bad[:FAILED_NAMES_MAX],
            n_stale=0,
            stale=[],
        )
        if ref == "close":
            summary.update(
                n_day_bar_intraday=day_bar["intraday"],
                n_day_bar_missing=day_bar["missing"],
                n_day_bar_failed=day_bar["failed"],
            )
            log.info("mark_levels day bar %s: %d from intraday, %d missing, %d intraday failed", trade_date.isoformat(),
                     day_bar["intraday"], day_bar["missing"], day_bar["failed"])  # fmt: skip
        return summary
    except Exception as exc:
        note = f"{head} error={type(exc).__name__}: {exc}"
        raise
    finally:
        summary["elapsed"] = round(time.perf_counter() - t0, 3)
        summary["status"] = status
        db.finish_run(conn, run_id, now_ist().isoformat(), status, note)
        log.info("mark_levels %s: %s", run_id, summary)
        if own_conn:
            conn.close()
