"""D24 warm-up: seed each contract's closed-5m series from the PREVIOUS session's intraday history.

At subscription time, per option contract: `adapter.intraday(sid, "NSE_FNO", 5, prev, prev, oi=True)`
where prev = `core.calendar.prev_trading_day(trade_date)`; keep candles that closed inside that
session (open 09:15 <= ts, ts + 5 min <= 15:30), convert through `OiCarry` (OI carried / bar skipped,
see adapter.py) and keep the LAST `n_bars` (20 = SMA20 window; ATR14 needs 15). The contract's
`ContractBars` gets them as its seed, so SMA20 / ATR14 are valid at the first bar close of the day.

Pacing: the Dhan adapter's data bucket already allows 5 requests/s; `min_interval_s` (0.2 s) paces
this loop to the same 5/s on its own so a fake or a future adapter without a limiter cannot burst.
Blocking (the adapter is synchronous): the app runs `fetch_warmup` in a worker thread.

Missing history is not an error: an empty answer or a broker error leaves the contract unseeded and
it warms up live (the engine logs its own "warming" line); both are logged here with the reason.
An auth error stops the batch (every later call would fail the same way) and is re-raised.

T-086 (D93) resume: a restart AFTER 09:15 also loads TODAY's recorded closed 5m bars (the recorder's
bars-5m.parquet) and the in-session OI peak (oi_series) with `load_today`; the app appends them after the
previous-session seed and replays them through the engines silently, so a Stop -> Start keeps today's bars,
the OI peak and the engine's arming state. The bar in progress at the restart is partial (live from then on).

M8 (PLAN-T267, S1.7.0): the V2 Scanner's opening bars. The app subscribes after the 09:20 build, so today's 09:15 bar
(and the bar the subscription landed in) is partial or missing live, and the V2T OI peak was at the 09:20 close in 25
Run-2 trades. `fetch_opening_bars` asks the broker for TODAY's 5-min bars with OI and `apply_opening` hands each V2T
engine the slots before `opening_cutoff_ms(subscribed)`; the engine replaces those slots in what IT evaluates (the
shared ContractBars series V1 reads is never modified, and the live OiCarry is only read).
"""

from __future__ import annotations

import math
import sqlite3
import time
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from gammad.broker.base import BrokerAdapter, BrokerAuthError, BrokerError, Candle
from gammad.core.calendar import prev_trading_day
from gammad.log import get_logger
from gammad.watch.adapter import ContractBars, OiCarry
from gammad.watch.bars import MINUTE_MS, session_bounds_ms
from gammad.watch.bars import Bar as WatchBar
from gammad.watch.types import Bar

log = get_logger(__name__)

OPTION_SEGMENT = "NSE_FNO"
OPTION_INSTRUMENT = "OPTSTK"
WARMUP_BARS = 20
WARMUP_INTERVAL_MIN = 5
DATA_RATE_PER_S = 5.0


@dataclass
class WarmupResult:
    session: date | None
    bars: dict[int, list[Bar]] = field(default_factory=dict)
    missing: list[int] = field(default_factory=list)
    errors: dict[int, str] = field(default_factory=dict)
    calls: int = 0
    cancelled: bool = False  # T-107: stopped early by `cancelled()` (STOP); `bars` holds what was fetched before

    def summary(self) -> dict[str, object]:
        return {
            "session": self.session.isoformat() if self.session else None,
            "seeded": len(self.bars),
            "missing": len(self.missing),
            "errors": len(self.errors),
            "calls": self.calls,
        }


def fetch_warmup(
    adapter: BrokerAdapter,
    security_ids: Iterable[int],
    trade_date: date,
    carry: OiCarry,
    *,
    n_bars: int = WARMUP_BARS,
    interval_min: int = WARMUP_INTERVAL_MIN,
    min_interval_s: float = 1.0 / DATA_RATE_PER_S,
    sleep: Callable[[float], None] = time.sleep,
    monotonic: Callable[[], float] = time.monotonic,
    prev_day: Callable[[date], date] = prev_trading_day,
    cancelled: Callable[[], bool] = lambda: False,
) -> WarmupResult:
    """Fetch and convert the last `n_bars` closed bars of the previous session per contract.

    T-107: `cancelled` is checked before every contract (and after its pacing sleep). Cancelling the task that awaits
    this worker thread does not stop the thread; the app sets the flag instead, and the loop returns the partial
    result (`cancelled=True`) without another broker call."""
    sids = list(dict.fromkeys(int(s) for s in security_ids))
    if not sids:
        return WarmupResult(session=None)
    session = prev_day(trade_date)
    open_ms, close_ms = session_bounds_ms(session)
    span = interval_min * MINUTE_MS
    res = WarmupResult(session=session)
    last_call: float | None = None
    for sid in sids:
        if cancelled():
            res.cancelled = True
            break
        if last_call is not None and min_interval_s > 0:
            wait = min_interval_s - (monotonic() - last_call)
            if wait > 0:
                sleep(wait)
                if cancelled():
                    res.cancelled = True
                    break
        last_call = monotonic()
        res.calls += 1
        try:
            candles = adapter.intraday(
                sid, OPTION_SEGMENT, interval_min, session, session, oi=True, instrument=OPTION_INSTRUMENT
            )
        except BrokerAuthError:
            log.error("warmup: auth rejected after %d/%d contracts; the rest warm up live", res.calls - 1, len(sids))
            raise
        except BrokerError as exc:
            res.errors[sid] = str(exc)
            log.warning("warmup: sec %s history for %s failed (%s); it warms up live", sid, session, exc)
            continue
        # T-647 (SCAN-01 review F3): key each candle by its slot (a Dhan 09:16 first candle is the 09:15 bar),
        # then keep the session's closed slots.
        closed = [
            Candle(slot, c.open, c.high, c.low, c.close, c.volume, getattr(c, "oi", None))
            for slot, c in sorted(slot_candles(candles, open_ms, span)[0].items())
            if open_ms <= slot and slot + span <= close_ms
        ]
        bars = carry.candles(sid, closed)[-n_bars:]
        if not bars:
            res.missing.append(sid)
            log.info("warmup: sec %s has no %s 5m history (got %d candles); it warms up live", sid, session,
                     len(candles))
            continue
        if len(bars) < n_bars:
            log.info("warmup: sec %s only %d/%d closed bars on %s; the rest fill live", sid, len(bars), n_bars,
                     session)
        res.bars[sid] = bars
    if res.cancelled:
        log.info("warmup %s -> %s: cancelled after %d/%d contracts: %s", trade_date, session, res.calls, len(sids),
                 res.summary())
        return res
    log.info("warmup %s -> %s: %s", trade_date, session, res.summary())
    return res


@dataclass
class TodayResume:
    """T-086 (D93): what a mid-session restart recovers of TODAY, per contract.

    bars       today's recorded closed 5m bars (watch bars, oldest first), from bars-5m.parquet.
    peaks      the in-session tick OI peak as of each bar's END (same length as `bars`), from oi_series:
               the running max of per-bucket MAX(oi) -- the value BarBuilder.oi_peak_today held at that close.
    oi_peak    the in-session tick OI peak up to `now` (primes BarBuilder.oi_peak_today).
    last_oi    the last recorded tick OI (primes OiCarry and BarBuilder.oi_now).
    inputs     T-102: per bar (same length as `bars`), the (oi_now, oi_peak) the LIVE engine was handed at that close
               (bars-5m.parquet eng_oi_now / eng_oi_peak), None where not recorded (older file, no engine call).
    """

    trade_date: date | None
    bars: dict[int, list[WatchBar]] = field(default_factory=dict)
    peaks: dict[int, list[float]] = field(default_factory=dict)
    inputs: dict[int, list[tuple[float, float] | None]] = field(default_factory=dict)
    oi_peak: dict[int, int] = field(default_factory=dict)
    last_oi: dict[int, int] = field(default_factory=dict)
    filled: dict[int, int] = field(default_factory=dict)
    backfill_errors: dict[int, str] = field(default_factory=dict)
    slot_dupes: dict[int, int] = field(default_factory=dict)  # SCAN-01: broker candles dropped as a filled slot's 2nd

    def summary(self) -> dict[str, object]:
        return {
            "date": self.trade_date.isoformat() if self.trade_date else None,
            "contracts": len(self.bars),
            "bars": sum(len(b) for b in self.bars.values()),
            "peaks": len(self.oi_peak),
            "filled": sum(self.filled.values()),
        }


def _engine_inputs(oi_now: object, oi_peak: object) -> tuple[float, float] | None:
    """T-102: a recorded (eng_oi_now, eng_oi_peak) pair, or None unless BOTH are finite numbers (a half-recorded or
    damaged pair is no evidence: the replay falls back to the signal rows, then the bar's own OI)."""
    try:
        if isinstance(oi_now, bool) or isinstance(oi_peak, bool):
            return None
        now, peak = float(oi_now), float(oi_peak)  # type: ignore[arg-type]
    except (TypeError, ValueError, OverflowError):
        return None
    return (now, peak) if math.isfinite(now) and math.isfinite(peak) else None


def load_today(
    parquet_root: Path,
    conn: sqlite3.Connection | None,
    security_ids: Iterable[int],
    trade_date: date,
    now_ms: int,
    *,
    interval_min: int = WARMUP_INTERVAL_MIN,
) -> TodayResume:
    """T-086: today's recorded closed bars + OI peak for a restart AFTER 09:15 (before it: nothing, the previous
    session's warm-up is all there is). Missing or unreadable files are not errors: whatever is found is used."""
    sids = list(dict.fromkeys(int(s) for s in security_ids))
    open_ms, close_ms = session_bounds_ms(trade_date)
    res = TodayResume(trade_date=trade_date)
    if not sids or now_ms < open_ms:
        return res
    span = interval_min * MINUTE_MS
    end_ms = min(now_ms, close_ms)
    wanted = set(sids)

    path = parquet_root / trade_date.isoformat() / "bars-5m.parquet"
    by_sid: dict[int, dict[int, WatchBar]] = {}
    eng_in: dict[int, dict[int, tuple[float, float] | None]] = {}
    if path.exists():
        try:
            rows = pq.read_table(path).to_pylist()
        except (OSError, ValueError, pa.ArrowException) as exc:  # a damaged file must never stop the start-up
            log.error("resume: %s unreadable (%s); today's bars warm up live", path, exc)
            rows = []
        for r in rows:
            sid, start = int(r["security_id"]), int(r["start_ms"])
            if sid not in wanted or int(r["tf_min"]) != interval_min:
                continue
            if start < open_ms or start + span > end_ms:
                continue
            oi = r.get("oi")
            by_sid.setdefault(sid, {})[start] = WatchBar(  # a re-written row for the same bar: the last wins
                start_ms=start, tf_min=interval_min, open=float(r["open"]), high=float(r["high"]),
                low=float(r["low"]), close=float(r["close"]), volume=int(r["volume"] or 0),
                oi=None if oi is None else int(oi),
            )
            eng_in.setdefault(sid, {})[start] = _engine_inputs(r.get("eng_oi_now"), r.get("eng_oi_peak"))

    bucket_max: dict[int, dict[int, int]] = {}
    if conn is not None:
        marks = ",".join("?" * len(sids))
        try:
            for sid, bucket, mx in conn.execute(
                f"SELECT security_id, (ts_ms - ?) / ? AS b, MAX(oi) FROM oi_series "
                f"WHERE security_id IN ({marks}) AND ts_ms >= ? AND ts_ms < ? AND oi > 0 GROUP BY security_id, b",
                (open_ms, span, *sids, open_ms, end_ms),
            ):
                bucket_max.setdefault(int(sid), {})[int(bucket)] = int(mx)
            # D98(7): today's window bounds the scan itself (was: every day's rows of the contract, each against a
            # correlated MAX subquery). SQLite's bare-column rule: with one MAX() aggregate, `oi` comes from the
            # row holding MAX(ts_ms).
            for sid, oi, _ts in conn.execute(
                f"SELECT security_id, oi, MAX(ts_ms) FROM oi_series WHERE security_id IN ({marks}) AND oi > 0 "
                "AND ts_ms >= ? AND ts_ms < ? GROUP BY security_id",
                (*sids, open_ms, end_ms),
            ):
                res.last_oi[int(sid)] = int(oi)
        except sqlite3.Error as exc:
            log.error("resume: oi_series unreadable (%s); OI peaks restart from live ticks", exc)
            bucket_max = {}

    for sid in sids:
        buckets = bucket_max.get(sid, {})
        bars = [by_sid[sid][k] for k in sorted(by_sid.get(sid, {}))]
        if buckets:
            res.oi_peak[sid] = max(buckets.values())
        if not bars:
            continue
        peaks: list[float] = []
        running = 0
        for b in bars:
            last_bucket = (b.start_ms + span - open_ms) // span - 1
            for k in [k for k in buckets if k <= last_bucket]:
                running = max(running, buckets.pop(k))
            if b.oi is not None:  # oi_series may be thinner than the bars (1 s dedupe); never below a bar's OI
                running = max(running, b.oi)
            peaks.append(float(running))
        res.bars[sid] = bars
        res.peaks[sid] = peaks
        res.inputs[sid] = [eng_in.get(sid, {}).get(b.start_ms) for b in bars]
        res.oi_peak[sid] = max(res.oi_peak.get(sid, 0), int(peaks[-1]))
    log.info("resume %s: %s", trade_date, res.summary())
    return res


def bar_close_peak(closed_bars: Iterable[Bar | WatchBar], trade_date: date) -> float | None:
    """T-585: today's closed bars use bar-close OI, never the tick max; exclude the prior-session seed.

    The caller supplies only closed bars. Adapt both live and resume shapes to the existing V1 peak rule.
    """
    from gammad.v1.entry import bar_max_oi

    open_ms, close_ms = session_bounds_ms(trade_date)
    rows = []
    for bar in closed_bars:
        ts = bar.start_ms if isinstance(bar, WatchBar) else bar.ts_ms
        if open_ms <= ts < close_ms and bar.oi is not None:
            rows.append((ts, bar.open, bar.high, bar.low, bar.close, bar.volume, bar.oi))
    return float(bar_max_oi(rows, trade_date)) if rows else None


def slot_candles(candles: Iterable[Any], open_ms: int, span: int) -> tuple[dict[int, Any], dict[int, int]]:
    """SCAN-01 (D705): broker candles keyed by the bar SLOT their stamp falls in, not the raw stamp.

    Dhan stamps a day's FIRST candle at its first trade minute (09:16, 09:17 ...) on 48 % of contract-days while every
    later candle sits on the 09:15 grid (measured, gammad/scan/far_inputs.py docstring); no broker wire parser floors.
    A 09:16 stamp is the 09:15-09:20 bar (closing 09:20), so it floors to 09:15 -- book_fut.py _dhan_slots' rule, here
    09:15-relative so any interval keeps the session grid. Every broker's candles pass through this one point.
    Two candles in one slot: an on-grid one beats an off-grid one, else the LAST wins (a re-sent, corrected candle; the
    last-wins rule of load_today, late_start.build_seed and _dhan_slots); the extras are counted per slot (never merged
    into an invented bar). The caller applies its closed / cutoff rule to the SLOT (no look-ahead: 09:15 closes at
    09:20). SCAN-01 review F2: last-wins, not first-wins."""
    out: dict[int, Any] = {}
    extra: dict[int, int] = {}
    for c in candles:
        ts = int(c.ts_ms)
        slot = open_ms + (ts - open_ms) // span * span
        prev = out.get(slot)
        if prev is None:
            out[slot] = c
            continue
        extra[slot] = extra.get(slot, 0) + 1
        if not (int(prev.ts_ms) == slot and ts != slot):
            out[slot] = c
    return out, extra


def backfill_today(
    resume: TodayResume,
    adapter: BrokerAdapter | None,
    security_ids: Iterable[int],
    trade_date: date,
    now_ms: int,
    *,
    interval_min: int = WARMUP_INTERVAL_MIN,
    min_interval_s: float = 1.0 / DATA_RATE_PER_S,
    sleep: Callable[[float], None] = time.sleep,
    monotonic: Callable[[], float] = time.monotonic,
    cancelled: Callable[[], bool] = lambda: False,
) -> TodayResume:
    """T-585: fill missing closed slots; recorded bars/inputs win, all peaks use bar-close OI.

    No alerts are sent here. Authentication stops requests but never prevents the day from resuming.
    """
    open_ms, close_ms = session_bounds_ms(trade_date)
    span = interval_min * MINUTE_MS
    expected = set(range(open_ms, min(now_ms, close_ms) - span + 1, span))
    last_call: float | None = None
    calls = 0
    # An auth rejection or a cancel stops broker REQUESTS only: the loop still visits every contract so its peak is
    # re-derived from the recorded bars (bar-close OI), never left at the old tick-max peak (CX-158 review finding 1).
    requests_on = adapter is not None
    for sid in dict.fromkeys(int(s) for s in security_ids):
        if requests_on and cancelled():
            requests_on = False
        recorded = resume.bars.get(sid, [])
        inputs = resume.inputs.get(sid, [])
        by_ts = {b.start_ms: b for b in recorded}
        by_input = {b.start_ms: inputs[i] if i < len(inputs) else None for i, b in enumerate(recorded)}
        filled: dict[int, WatchBar] = {}
        if requests_on and expected - by_ts.keys():
            if last_call is not None and min_interval_s > 0:
                wait = min_interval_s - (monotonic() - last_call)
                if wait > 0:
                    sleep(wait)
            candles = []
            if cancelled():
                requests_on = False
            else:
                last_call = monotonic()
                calls += 1
                try:
                    candles = adapter.intraday(
                        sid, OPTION_SEGMENT, interval_min, trade_date, trade_date,
                        oi=True, instrument=OPTION_INSTRUMENT,
                    )
                except BrokerAuthError as exc:
                    resume.backfill_errors[sid] = str(exc)
                    log.error("backfill: auth rejected for sec %s; stopping requests (%s)", sid, exc)
                    candles, requests_on = [], False
                except BrokerError as exc:
                    resume.backfill_errors[sid] = str(exc)
                    candles = []
            # SCAN-01 (D705): floor each stamp to its slot first (a Dhan 09:16 first candle was dropped here, and the
            # opening OI peak with it); `expected` holds only slots CLOSED by now, and a recorded bar still wins.
            slots, extra = slot_candles(candles, open_ms, span)
            for slot, c in slots.items():
                if slot in expected and slot not in by_ts:
                    filled[slot] = WatchBar(
                        start_ms=slot, tf_min=interval_min, open=c.open, high=c.high, low=c.low,
                        close=c.close, volume=int(c.volume), oi=None if c.oi is None else int(c.oi),
                    )
            dupes = sum(extra.get(s, 0) for s in filled)
            if dupes:
                resume.slot_dupes[sid] = resume.slot_dupes.get(sid, 0) + dupes
        by_ts.update(filled)
        bars = [by_ts[ts] for ts in sorted(by_ts)]
        if filled:
            resume.filled[sid] = resume.filled.get(sid, 0) + len(filled)
            last = filled[max(filled)].oi
            if sid not in resume.last_oi and last is not None:
                resume.last_oi[sid] = last
        if bars:
            resume.bars[sid] = bars
            resume.inputs[sid] = [by_input.get(b.start_ms) for b in bars]
            resume.peaks[sid] = [bar_close_peak(bars[:i + 1], trade_date) or 0.0 for i in range(len(bars))]
            resume.oi_peak[sid] = int(resume.peaks[sid][-1])
        else:
            resume.oi_peak.pop(sid, None)
    log.info("backfill %s: %s, calls %d, errors %d, slot dupes %d", trade_date, resume.summary(), calls,
             len(resume.backfill_errors), sum(resume.slot_dupes.values()))
    return resume


def read_today(
    parquet_root: Path,
    conn: sqlite3.Connection | None,
    security_ids: Iterable[int],
    trade_date: date,
    now_ms: int,
    *,
    adapter: BrokerAdapter | None = None,
    cancelled: Callable[[], bool] = lambda: False,
    **backfill_kw: Any,
) -> TodayResume:
    """T-585: the resume entry point, including bar-close peaks when broker warmup is disabled."""
    sids = list(security_ids)
    resume = load_today(parquet_root, conn, sids, trade_date, now_ms,
                        interval_min=backfill_kw.get("interval_min", WARMUP_INTERVAL_MIN))
    return backfill_today(resume, adapter, sids, trade_date, now_ms, cancelled=cancelled, **backfill_kw)


def missed_breakout(bars: list[Bar], armed_close_ms: int, consol_hi: float, vol_sma_len: int) -> int | None:
    """First post-arm bar that broke the high with volume above the prior closed-bar SMA (T-585)."""
    for i, bar in enumerate(bars):
        prior = bars[max(0, i - vol_sma_len):i]
        if (bar.ts_ms >= armed_close_ms and len(prior) == vol_sma_len and bar.high > consol_hi
                and bar.volume > sum(b.volume for b in prior) / vol_sma_len):
            return bar.ts_ms
    return None


# ---------------------------------------------------------------- M8 (PLAN-T267): the V2T opening bars


def opening_cutoff_ms(subscribed_ms: int, trade_date: date) -> int:
    """The end of the 5-min slot a contract's live subscription landed in: every slot opening before it is partial or
    missing in the live series. Never before 09:15 (a subscription before the open leaves nothing partial) and never
    after the 15:30 close."""
    open_ms, close_ms = session_bounds_ms(trade_date)
    span = WARMUP_INTERVAL_MIN * MINUTE_MS
    end = int(subscribed_ms) - int(subscribed_ms) % span + span
    return max(open_ms, min(end, close_ms))


@dataclass
class OpeningResult:
    """M8: today's broker 5-min bars per contract for the slots opening before `cutoff_ms` (engine bars, OI resolved).

    bars      sid -> the broker's closed bars of today with open >= 09:15 and open + 5 min <= cutoff_ms, oldest first
              (an empty list = nothing to replace: the cutoff is at or before 09:15).
    missing   sids the broker answered with no such bar (the engine keeps its live bars).
    errors    sid -> the broker error (the engine is told opening_failed and runs on its live bars).
    """

    trade_date: date
    cutoff_ms: int
    bars: dict[int, list[Bar]] = field(default_factory=dict)
    missing: list[int] = field(default_factory=list)
    errors: dict[int, str] = field(default_factory=dict)
    calls: int = 0
    cancelled: bool = False

    def summary(self) -> dict[str, object]:
        return {"date": self.trade_date.isoformat(), "cutoff_ms": self.cutoff_ms, "fetched": len(self.bars),
                "missing": len(self.missing), "errors": len(self.errors), "calls": self.calls}  # fmt: skip


def _opening_bars(candles: Iterable[Any], open_ms: int, cutoff_ms: int, prior_oi: float | None) -> list[Bar]:
    """Broker candles -> engine bars for [09:15, cutoff). A candle with no OI carries the previous candle's OI (then the
    contract's last known OI); before any OI is known it is skipped -- OiCarry's rule, WITHOUT writing to the live
    OiCarry (the fetch must never move the live OI state V1 reads)."""
    span = WARMUP_INTERVAL_MIN * MINUTE_MS
    # SCAN-01 (D705): keyed by the SLOT a stamp floors to (a Dhan 09:16 first candle is the 09:15 bar), the cutoff
    # rule applied to the slot; one per slot, the on-grid candle else the first (slot_candles).
    slots, extra = slot_candles(candles, open_ms, span)
    by_ts = {s: c for s, c in slots.items() if open_ms <= s and s + span <= cutoff_ms}
    dupes = sum(extra.get(s, 0) for s in by_ts)
    if dupes:
        log.info("opening bars: %d broker candle(s) shared a slot with another; one kept per slot", dupes)
    out: list[Bar] = []
    last_oi = prior_oi
    for ts in sorted(by_ts):
        c = by_ts[ts]
        oi = None if c.oi is None else float(c.oi)
        if oi is None or (isinstance(oi, float) and math.isnan(oi)):
            oi = last_oi
        if oi is None:
            continue
        last_oi = oi
        out.append(Bar(ts_ms=ts, open=float(c.open), high=float(c.high), low=float(c.low), close=float(c.close),
                       volume=float(c.volume or 0.0), oi=oi))  # fmt: skip
    return out


def fetch_opening_bars(
    adapter: BrokerAdapter,
    security_ids: Iterable[int],
    trade_date: date,
    cutoff_ms: int,
    *,
    carry: OiCarry | None = None,
    min_interval_s: float = 1.0 / DATA_RATE_PER_S,
    sleep: Callable[[float], None] = time.sleep,
    monotonic: Callable[[], float] = time.monotonic,
    cancelled: Callable[[], bool] = lambda: False,
) -> OpeningResult:
    """M8 (PLAN-T267): when a contract is registered for V2T, `adapter.intraday(sid, "NSE_FNO", 5, today, today,
    oi=True)` and keep the closed bars opening in [09:15, cutoff_ms) -- the slots the live series holds partial or not
    at all (opening_cutoff_ms). Call it once the cutoff slot has closed. Blocking (the adapter is synchronous): run it
    in a worker thread, then `apply_opening(pool, result)` on the loop. `carry` is READ only (a leading candle with no
    OI takes the contract's last known OI). An auth error stops the batch and is re-raised (as fetch_warmup); a broker
    error is recorded per contract."""
    sids = list(dict.fromkeys(int(s) for s in security_ids))
    open_ms, _close_ms = session_bounds_ms(trade_date)
    res = OpeningResult(trade_date=trade_date, cutoff_ms=int(cutoff_ms))
    if int(cutoff_ms) <= open_ms:  # subscribed before the open: nothing is partial, no call (an empty overlay each)
        res.bars = {sid: [] for sid in sids}
        return res
    last_call: float | None = None
    for sid in sids:
        if cancelled():
            res.cancelled = True
            break
        if last_call is not None and min_interval_s > 0:
            wait = min_interval_s - (monotonic() - last_call)
            if wait > 0:
                sleep(wait)
                if cancelled():
                    res.cancelled = True
                    break
        last_call = monotonic()
        res.calls += 1
        try:
            candles = adapter.intraday(
                sid, OPTION_SEGMENT, WARMUP_INTERVAL_MIN, trade_date, trade_date, oi=True, instrument=OPTION_INSTRUMENT
            )
        except BrokerAuthError:
            log.error("opening bars: auth rejected after %d/%d contracts", res.calls - 1, len(sids))
            raise
        except BrokerError as exc:
            res.errors[sid] = str(exc)
            log.warning("opening bars: sec %s %s failed (%s); V2T evaluates its live bars", sid, trade_date, exc)
            continue
        bars = _opening_bars(candles, open_ms, int(cutoff_ms), carry.last(sid) if carry is not None else None)
        if bars:
            res.bars[sid] = bars
        else:
            res.missing.append(sid)
    log.info("opening bars %s: %s", trade_date, res.summary())
    return res


def apply_opening(pool: Any, result: OpeningResult) -> int:
    """Hand each contract's opening bars to its engine in `pool` (an EnginePool whose engines have apply_opening, i.e.
    V2T). A contract the broker had no bar for gets an empty overlay (its live bars stand); a broker error ->
    opening_failed. Contracts with no engine in the pool are skipped. Returns how many engines were updated."""
    n = 0
    sids = {*result.bars, *result.missing, *result.errors}
    for sid in sorted(sids):
        eng = pool.get(sid)
        if eng is None or not hasattr(eng, "apply_opening"):
            continue
        if sid in result.errors:
            eng.opening_failed(result.errors[sid])
        else:
            eng.apply_opening(result.bars.get(sid, []), result.cutoff_ms)
        n += 1
    return n


def preload(series: Mapping[int, ContractBars], result: WarmupResult) -> int:
    """Seed each contract's series with its warm-up bars. Returns how many contracts were seeded."""
    n = 0
    for sid, bars in result.bars.items():
        s = series.get(sid)
        if s is None:
            continue
        s.seed(bars)
        n += 1
    return n
