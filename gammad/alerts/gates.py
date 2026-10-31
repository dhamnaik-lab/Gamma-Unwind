"""GROUP alert gates (D63, S1.3.0 D85/D94) and the signal context builder -- ONE definition for live and backtest.

Shadow mode: a signal that fails a gate is still recorded and pushed to the dashboard; only the GROUP
send is skipped (alert status `gated`, reasons in the row). A gate whose input is missing is recorded in
`gate_unknown` and does NOT block (fail-open: missing data must not silence an alert).

S1.4.0 (D114(3)): four gates on every list (GATE_NAMES; DIRECTION and UNWIND run on every list when
GatesCfg.all_lists, else on INTRADAY-hold lists only, the S1.3.x rule kept for older-spec research yamls). The tier of
an E1 = its passes (`tier`): 4 ENTRY, 3 STRONG_WATCHLIST, 0-2 WATCHLIST; a missing input or a disabled gate is NOT a
pass. The router decides the send from the tier; build_context still records failed / unknown exactly as before.

zone       `zone_ok` (S1.9.0, D380 (1) / D382: ONE signed test): gap >= 0 (approach side) passes when gap <= the limit,
           gap < 0 (past the edge) when -gap <= limit + ZONE_PAST_EDGE_ATR_K x ATR % (k 0 = abs(gap) <= limit). PE:
           gap = (spot - edge) / spot * 100 (edge = support top; + = spot above it); CE: gap = (edge - spot) / spot * 100
           (edge = resistance bottom; + = spot below it). The Tier 0 evidence's measure (docs/TIER0-FINDINGS.md).
liquidity  over the last GatesCfg.liq_sma_bars closed 5-min bars ending at the signal bar:
           mean(volume) / lot_size >= liq_min_avg_lots AND count(volume == 0) <= liq_max_zero_vol_bars.
           Fewer bars than liq_sma_bars -> evaluated on what exists (the D24 seed normally fills 20).
direction  S1.3.0 (D85(2)); every list under S1.4.0 `all_lists`, else INTRADAY-hold lists only: over the
           UNDERLYING's 5-min bars of the signal's IST date that CLOSED at/before the signal time,
           VWAP = sum(typical price x volume) / sum(volume), typical = (high + low + close) / 3;
           F1 PE: spot < VWAP, CE: spot > VWAP (spot = the live underlying price at the signal, else the last
           closed bar's close); F2 = close[last] - close[last - 5] (6 bar closes = 25 min, M08; fewer -> the
           first of today's bars) < 0 for PE, > 0 for CE. pass = F1 AND F2. Fewer than 2 bars, or zero volume
           -> unknown. Reference: docs/research/2026-09-25-live-forensics/scripts/lf_sept.py:93-112.
unwind     S1.3.0 (D94(1), "E2-A"), lists as direction: pass when the arming cycle's E2 bar close >= the
           OI-peak bar's close + GatesCfg.unwind_min_ticks (1) x the contract's tick (the premium rose since OI
           peaked: writers buying back). The E1 of the same arming cycle carries the same inputs, so it inherits
           its E2's verdict. OI-peak bar = the FIRST of today's closed 5m bars whose OI equals oi_peak_today
           (engine.oi_peak_bar; Signal.snapshot carries it). No freshness window. Missing snapshot / tick ->
           unknown.
quadrant   A LABEL (D94(2)), never a gate: SignalContext.quadrant over OI-peak bar close -> signal.

The S1.3.0 keys (GatesCfg.direction_enabled / unwind_enabled / unwind_min_ticks) are read with getattr so this
module works on a GatesCfg that does not carry them yet; the defaults below are the S1.3.0 values.

S1.5.0 (0.8.1, D149(4), D161(3)): each scanner carries its own gate RULES (`build_context(..., rules=)`,
ScannerSpec.rules): "V1" (and "V1B", whose zone is V1 PLUS's zone confirmation) = DIRECTION passes on F1 OR F2
(GatesCfg.v1_direction_mode "any"; "all" = the S1.4.0 AND, pinned by research yamls) and the ZONE limit =
`zone_limit` = clamp(v1_zone_atr_k x ContractMeta.atr_pct, zone_max_gap_pct, v1_zone_max_pct) (docs/research/
2026-09-29-v1-variants/variants.py:56-59; an unknown ATR -> the fixed zone_max_gap_pct, never "unknown"); "V2" =
the S1.4.0 AND + the fixed zone_max_gap_pct, unchanged. The context records zone_limit_pct, f1, f2 and `carried`.
Carry (D157(2), D161(2)): a carried contract's tier is capped at STRONG_WATCHLIST, in `tier` and `v1b_tier`.
S1.9.0 (0.8.8, D380 (6) "Lift the cap now (owner edition)", D382): the cap holds in the CLIENT edition only
(`cap_carried` reads edition.is_client() at call time); in the OWNER edition a carried contract tiers like any other
in `tier` (the V1 live scanner). `v1b_tier` keeps the cap in both editions (no card, no trade; scan/book_c8.py reads it).
V1 B (D161(4)) is tiered by `v1b_tier` (liquidity, premium >= Rs 2, bar turnover >= Rs 10 lakh, before 15:20), and
`plus_candidate` says whether a V1 B signal is a V1 PLUS component (V1's dynamic zone, liquidity, bar turnover >=
Rs 25 lakh, before 15:20, not carried, no Rs 2 floor). "Before 15:20" is the measured book's cut
(journal.py:139: a signal whose IST time is >= last_bar_close is dropped), so a bar CLOSING at 15:20 is out.

S1.7.0 (T-267, PLAN-T267 §1 "Tier rule" + M12): rules "V2T" = the V2 Scanner. build_context records its numbers and no
gate verdict (hold swing); `v2t_verdict` / `v2t_tier` tier it: ENTRY only when the stock's daily Supertrend agrees with
the side (CE +1, PE -1), else WATCHLIST + supertrend_red / supertrend_green / supertrend_unknown (owner only: the client
edition never records a non-ENTRY V2T row, router._route_v2t). The 1h zone book never blocks an ENTRY (NO_ZONE).
"""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import Any, Protocol

from gammad import edition as _edition
from gammad.config import IST, GatesCfg, HoldCfg, V1BCfg
from gammad.watch.types import ContractMeta, Hold, Quadrant, SignalContext, Tier

GATE_ZONE = "zone"
GATE_LIQUIDITY = "liquidity"
GATE_DIRECTION = "direction"
GATE_UNWIND = "unwind"
GATE_NAMES: tuple[str, ...] = (GATE_ZONE, GATE_LIQUIDITY, GATE_DIRECTION, GATE_UNWIND)  # S1.4.0: the four

DIRECTION_ENABLED_DEFAULT = True
UNWIND_ENABLED_DEFAULT = True
UNWIND_MIN_TICKS_DEFAULT = 1.0
ALL_LISTS_DEFAULT = True  # S1.4.0 (D114(3)); a GatesCfg without the key = the S1.4.0 rule

TIER_ENTRY: Tier = "ENTRY"
TIER_STRONG_WATCHLIST: Tier = "STRONG_WATCHLIST"
TIER_WATCHLIST: Tier = "WATCHLIST"
DIRECTION_DRIFT_BARS = 6  # F2: close-to-close over the last 6 closed 5-min bars = 25 minutes (M08)
TF_MS = 5 * 60_000
EPS = 1e-9

# S1.5.0 (D161(3)): the gate rules a scanner is built with (engine_pool.ScannerSpec.rules)
RULES_V1 = "V1"
RULES_V2 = "V2"
RULES_V1B = "V1B"
RULES_V2T = "V2T"  # S1.7.0 (T-267): the V2 Scanner -- tiered by `v2t_tier`, never by the four gates
RULES: tuple[str, ...] = (RULES_V1, RULES_V2, RULES_V1B, RULES_V2T)
V1_ZONE_RULES = frozenset({RULES_V1, RULES_V1B})  # the dynamic zone + V1's direction mode
DIRECTION_ANY = "any"
V1_DIRECTION_MODE_DEFAULT = "all"  # a GatesCfg without the S1.5.0 keys = the S1.4.0 rule
V1_ZONE_ATR_K_DEFAULT = 0.0  # ... and the fixed zone limit

# S1.7.0 (T-267, PLAN-T267 §1 "Tier rule" + M12, D246 / D247): the V2 Scanner's WATCHLIST reasons (owner only)
V2T_SUPERTREND_RED = "supertrend_red"  # a CE whose stock's daily Supertrend is red (bearish)
V2T_SUPERTREND_GREEN = "supertrend_green"  # a PE whose stock's daily Supertrend is green (bullish)
V2T_SUPERTREND_UNKNOWN = "supertrend_unknown"  # no regime (fail closed, M14), or the side cannot be read
V2T_REASONS: tuple[str, ...] = (V2T_SUPERTREND_RED, V2T_SUPERTREND_GREEN, V2T_SUPERTREND_UNKNOWN)
V2T_WANTS: Mapping[str, int] = {"CE": 1, "PE": -1}  # CE only when green (+1), PE only when red (-1)


class OHLCV(Protocol):
    """Any 5-min bar with an OPEN-time stamp: gammad.watch.types.Bar (ts_ms) fits."""

    @property
    def ts_ms(self) -> int: ...
    @property
    def high(self) -> float: ...
    @property
    def low(self) -> float: ...
    @property
    def close(self) -> float: ...
    @property
    def volume(self) -> float: ...


def hold_for(list_name: str, hold: HoldCfg) -> Hold:
    """D58: swing for the monthly lists, intraday otherwise (EXPIRY-* and anything unknown)."""
    return "swing" if list_name in hold.swing_lists else "intraday"


def zone_gap_pct(option_type: str, spot: float | None, edge: float | None) -> float | None:
    if not spot or not edge or spot <= 0:
        return None
    if option_type.upper() == "PE":
        return (spot - edge) / spot * 100.0
    return (edge - spot) / spot * 100.0


def liquidity(volumes: Sequence[float], lot_size: int | None, bars: int) -> tuple[float | None, int | None]:
    """(average volume in lots, zero-volume bar count) over the last `bars` volumes; (None, None) if unknown."""
    window = [float(v) for v in volumes[-bars:]] if bars > 0 else []
    if not window or not lot_size or lot_size <= 0:
        return None, None
    avg_lots = sum(window) / len(window) / lot_size
    zeros = sum(1 for v in window if v <= 0)
    return avg_lots, zeros


def zone_limit(atr_pct: float | None, cfg: GatesCfg, rules: str = RULES_V1) -> float:
    """S1.5.0 (D149(4), D161(3)): the ZONE limit in percent. Rules V1 / V1B: clamp(v1_zone_atr_k x atr_pct,
    zone_max_gap_pct, v1_zone_max_pct), exactly variants.py:56-59 (max(1.0, min(k x atr, 2.5))); an unknown ATR, or
    k 0, is the fixed zone_max_gap_pct (never "unknown": the ATR is not a gate input of its own). Rules V2: the fixed
    zone_max_gap_pct."""
    base = float(cfg.zone_max_gap_pct)
    if rules not in V1_ZONE_RULES:
        return base
    k = float(getattr(cfg, "v1_zone_atr_k", V1_ZONE_ATR_K_DEFAULT))
    if k <= 0 or atr_pct is None or isinstance(atr_pct, bool):
        return base
    atr = float(atr_pct)
    if math.isnan(atr):
        return base
    hi = float(getattr(cfg, "v1_zone_max_pct", base))
    return max(base, min(k * atr, hi))


def evaluate(
    gap_pct: float | None,
    avg_vol_lots: float | None,
    zero_vol_bars: int | None,
    cfg: GatesCfg,
    *,
    zone_limit: float | None = None,
    atr_pct: float | None = None,
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    """(failed gates, gates that could not be evaluated) for zone + liquidity. Disabled gates -> ((), ()). `zone_limit` =
    the limit in percent (None = cfg.zone_max_gap_pct); zone = `zone_ok` (D382, signed; atr_pct = its past-edge ATR %)."""
    if not cfg.enabled:
        return (), ()
    failed: list[str] = []
    unknown: list[str] = []
    if gap_pct is None or math.isnan(gap_pct):  # D382: NaN = a missing input (as `_num`), never a silent pass
        unknown.append(GATE_ZONE)
    elif not zone_ok(gap_pct, cfg.zone_max_gap_pct if zone_limit is None else zone_limit, atr_pct):
        failed.append(GATE_ZONE)
    if avg_vol_lots is None or zero_vol_bars is None:
        unknown.append(GATE_LIQUIDITY)
    elif avg_vol_lots < cfg.liq_min_avg_lots - 1e-9 or zero_vol_bars > cfg.liq_max_zero_vol_bars:
        failed.append(GATE_LIQUIDITY)
    return tuple(failed), tuple(unknown)


# ---------------------------------------------------------------- S1.4.0 tiers (D114(3))


def disabled(cfg: GatesCfg) -> frozenset[str]:
    """The gates this config switches off: `enabled` False -> all four; `direction_enabled` / `unwind_enabled`
    False -> that one. A disabled gate is never a pass (tier)."""
    if not cfg.enabled:
        return frozenset(GATE_NAMES)
    off: set[str] = set()
    if not getattr(cfg, "direction_enabled", DIRECTION_ENABLED_DEFAULT):
        off.add(GATE_DIRECTION)
    if not getattr(cfg, "unwind_enabled", UNWIND_ENABLED_DEFAULT):
        off.add(GATE_UNWIND)
    return frozenset(off)


def passed(ctx: SignalContext | None, cfg: GatesCfg) -> tuple[str, ...]:
    """The gates an E1 PASSED, in GATE_NAMES order: GATE_NAMES minus ctx.gate_reasons (failed) minus
    ctx.gate_unknown (missing input) minus disabled(cfg). A gate the context never evaluated (S1.3.1 swing lists
    under `all_lists: false`) is in none of those sets, so it counts as passed there; with `all_lists: true` every
    list evaluates all four. No context -> ()."""
    if ctx is None:
        return ()
    out = set(GATE_NAMES) - set(ctx.gate_reasons) - set(ctx.gate_unknown) - disabled(cfg)
    return tuple(g for g in GATE_NAMES if g in out)


def cap_carried(t: Tier, ctx: SignalContext | None, *, client: bool | None = None,
                still_watching: bool = False) -> Tier:  # fmt: skip
    """D157(2) / D161(2): a carried contract is WATCH-ONLY -- an ENTRY becomes STRONG_WATCHLIST; lower tiers stay.
    S1.9.0 (D380 (6), D382): CLIENT edition only. In the OWNER edition a carried contract may reach ENTRY (routed to
    the PERSONAL chat, tracked, never the group: the router). `client` None = the running edition (read at CALL time,
    like edition.client_entry_kind); True / False pins it (a replay of a store). The lift is the V1 live scanner's
    (the one that trades): V1 B (`v1b_tier`) and every shadow scanner's row (router._record_shadow, client=True) keep
    the cap in BOTH editions -- they never send a card or open a trade, and the owner's carry shadow book selects exactly
    the capped carried V1 B rows (scan/book_c8.py: stored STRONG_WATCHLIST + a `v1b_tier` re-run saying STRONG).
    0.8.9 T-419 (D471 Q10, D480): `still_watching` True = the caller's `still_watching.enabled` is on and this is the V1
    live scanner's carried ENTRY (a Still Watching entry): NO cap in either edition. Default False = unchanged, so every
    caller that does not pass it (V1 B, the shadow rows, a replay) keeps the cap exactly as before."""
    if still_watching:
        return t
    is_client_ed = _edition.is_client() if client is None else bool(client)
    if is_client_ed and t == TIER_ENTRY and ctx is not None and getattr(ctx, "carried", False):
        return TIER_STRONG_WATCHLIST
    return t


def tier(ctx: SignalContext | None, cfg: GatesCfg, *, still_watching: bool = False) -> Tier:
    """S1.4.0 (D114(3)) tier of an E1 = its passes: 4 -> ENTRY, 3 -> STRONG_WATCHLIST, 0-2 -> WATCHLIST.
    A missing input is not a pass, a disabled gate is not a pass, `ctx is None` -> WATCHLIST. E2 is untiered
    (the caller never asks). S1.5.0 (D161(2)): `ctx.carried` caps at STRONG_WATCHLIST (the tracker re-tiers with this
    same function, so the cap cannot be bypassed by a caller that skips the router); S1.9.0 (D380 (6)): in the client
    edition only (`cap_carried`). 0.8.9 T-419: `still_watching` = Settings.still_watching.enabled as the caller read it
    (`edition.still_watching_on`): True lifts the carry cap in BOTH editions (a carried ENTRY is a Still Watching
    entry); False (the default) = unchanged. With it True the tier equals `tier(replace(ctx, carried=False), cfg)`."""
    n = _v1r_passes(ctx, cfg)
    if n >= len(GATE_NAMES):
        return cap_carried(TIER_ENTRY, ctx, still_watching=still_watching)
    if n == len(GATE_NAMES) - 1:
        return TIER_STRONG_WATCHLIST
    return TIER_WATCHLIST


# ---------------------------------------------------------------- S1.5.0 V1 B / V1 PLUS (D161(4))


def _hhmm(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000, IST).strftime("%H:%M")


def signal_time_ms(snapshot: Mapping[str, Any] | None) -> int | None:
    """The time a bar-close signal is dated by (the measured books' ts = the bar's CLOSE, tune_capture.py:91):
    snapshot bar_ts_ms (the bar's open) + 5 min, else snapshot signal_ts_ms; None when neither is known."""
    bar = _num(snapshot, "bar_ts_ms")
    if bar is not None:
        return int(bar) + TF_MS
    sig = _num(snapshot, "signal_ts_ms")
    return int(sig) if sig is not None else None


def before_cutoff(ts_ms: int | None, last_bar_close: str) -> bool:
    """journal.py:139 exactly: a signal whose IST HH:MM is >= last_bar_close ("15:20") is outside the book. Unknown
    time -> False (a missing input is not a pass)."""
    return ts_ms is not None and _hhmm(ts_ms) < last_bar_close


def liquidity_ok(ctx: SignalContext, gates_cfg: GatesCfg | None = None) -> bool:
    """The research's liquidity test (tune_capture.py:96/104, journal.py:104), computed from the context's numbers:
    ctx.avg_vol_lots / ctx.zero_vol_bars = gates.liquidity over the liq_sma_bars (20) closed 5-min bars ending at the
    signal bar, seed bars included (what build_context records); avg >= liq_min_avg_lots (5) AND zero-volume bars <=
    liq_max_zero_vol_bars (5). Independent of GatesCfg.enabled (V1 B's rule, not a V1 gate). Unknown -> False."""
    g = gates_cfg or GatesCfg()
    avg, zeros = ctx.avg_vol_lots, ctx.zero_vol_bars
    return avg is not None and zeros is not None and avg >= g.liq_min_avg_lots - EPS and zeros <= g.liq_max_zero_vol_bars


def v1b_tier(
    ctx: SignalContext | None, snapshot: Mapping[str, Any] | None, cfg: V1BCfg, gates_cfg: GatesCfg | None = None
) -> Tier:
    """D161(4) / DESIGN-REVIEW A: a V1 B E1 is ENTRY when liquidity passes (`liquidity_ok`, the research's
    numbers; `gates_cfg` None = the GatesCfg defaults), the close (snapshot signal_price) >= cfg.owner_min_premium_rs,
    the bar turnover (snapshot turnover_rs, rupees) >= cfg.owner_min_turnover_rs and the bar is strictly before the cut
    (`before_cutoff`: a bar closing at 15:20 is out); otherwise WATCHLIST. V1's zone / direction / unwind verdicts
    are not V1 B's (journal.py:104-107). A missing input is not a pass; ctx None -> WATCHLIST; a carried contract caps
    at STRONG_WATCHLIST in BOTH editions (S1.9.0 D380 (6) lifts the V1 cap only; `cap_carried`)."""
    if ctx is None:
        return TIER_WATCHLIST
    close = _num(snapshot, "signal_price")
    turnover = _num(snapshot, "turnover_rs")
    ok = (
        liquidity_ok(ctx, gates_cfg)
        and close is not None
        and close >= cfg.owner_min_premium_rs - EPS
        and turnover is not None
        and turnover >= cfg.owner_min_turnover_rs - EPS
        and before_cutoff(signal_time_ms(snapshot), cfg.last_bar_close)
    )
    return cap_carried(TIER_ENTRY, ctx, client=True) if ok else TIER_WATCHLIST  # S1.9.0: V1 B keeps the cap


def plus_candidate(
    ctx: SignalContext | None, snapshot: Mapping[str, Any] | None, cfg: V1BCfg, gates_cfg: GatesCfg,
    atr_pct: float | None = None,
) -> bool:
    """D159(2) / D161(4): is this V1 B signal a V1 PLUS component? `zone_ok` (D382, signed; atr_pct = stock ATR %) of
    the zone gap vs the context's zone limit (V1's dynamic limit: a V1 B context is built with rules V1B; no recorded
    limit -> `zone_limit(None, gates_cfg)`), liquidity passes (`liquidity_ok`), bar turnover >= plus_min_turnover_rs,
    strictly before the cut, not carried. NO Rs 2 floor (journal.py:146 `v1b(turn=2.5e6, zone=True)`). Missing: False."""
    if ctx is None or getattr(ctx, "carried", False):
        return False
    limit = ctx.zone_limit_pct if ctx.zone_limit_pct is not None else zone_limit(None, gates_cfg)
    turnover = _num(snapshot, "turnover_rs")
    return (
        ctx.zone_gap_pct is not None
        and zone_ok(ctx.zone_gap_pct, limit, atr_pct)
        and liquidity_ok(ctx, gates_cfg)
        and turnover is not None
        and turnover >= cfg.plus_min_turnover_rs - EPS
        and before_cutoff(signal_time_ms(snapshot), cfg.last_bar_close)
    )


# ---------------------------------------------------------------- S1.7.0 V2 Scanner (T-267)


def _v2t_st_dir(block: Mapping[str, Any]) -> int | None:
    """The regime's st_dir from snapshot["v2t"]["regime"] (a {st_dir, ...} map, or the bare number): +1 / -1, else None
    (missing, null, a bool, NaN, 0 or anything else = unknown: never a defaulted +1, M14)."""
    regime = block.get("regime")
    raw = regime.get("st_dir") if isinstance(regime, Mapping) else regime
    if raw is None or isinstance(raw, bool):
        return None
    try:
        f = float(raw)
    except (TypeError, ValueError):
        return None
    return int(f) if f in (1.0, -1.0) else None


def v2t_verdict(snapshot: Mapping[str, Any] | None, option_type: str | None = None) -> tuple[Tier, str | None]:
    """PLAN-T267 §1 "Tier rule" (+ M12, D246 / D247, STRATEGY-SPEC 4.7): (tier, reason) of a V2 Scanner E1.
    ENTRY (reason None) only when the stock's daily Supertrend (snapshot["v2t"]["regime"]["st_dir"], as of the previous
    session's close) agrees with the side: a CE needs +1 (green), a PE -1 (red). Otherwise WATCHLIST with the reason
    `supertrend_red` (a CE on a red stock), `supertrend_green` (a PE on a green stock) or `supertrend_unknown` (no
    regime: fail closed; or the side cannot be read). The side = `option_type`, else snapshot["v2t"]["option_type"]
    (the router stamps it before the INSERT, so the tracker's one-argument `v2t_tier(snapshot)` reads the same side).
    The 1-hour zone book NEVER blocks an ENTRY (M12: no zone = NO_ZONE, still an ENTRY); the four V1 gates are not
    V2's. Never raises."""
    block = snapshot.get("v2t") if isinstance(snapshot, Mapping) else None
    if not isinstance(block, Mapping):
        return TIER_WATCHLIST, V2T_SUPERTREND_UNKNOWN
    side = str(option_type or block.get("option_type") or "").upper()
    want = V2T_WANTS.get(side)
    st = _v2t_st_dir(block)
    if want is None or st is None:
        return TIER_WATCHLIST, V2T_SUPERTREND_UNKNOWN
    if st == want:
        return TIER_ENTRY, None
    return TIER_WATCHLIST, (V2T_SUPERTREND_RED if st < 0 else V2T_SUPERTREND_GREEN)


def v2t_tier(snapshot: Mapping[str, Any] | None, option_type: str | None = None) -> Tier:
    """The tier alone of `v2t_verdict` (ENTRY / WATCHLIST; V2 has no STRONG WATCHLIST tier): the router's and the
    tracker's one rule (tracker._open_zone_trade calls it with the stored snapshot)."""
    return v2t_verdict(snapshot, option_type)[0]


# ---------------------------------------------------------------- S1.3.0 shadow gates (pure)


def _ist_date(ts_ms: int) -> Any:
    return datetime.fromtimestamp(ts_ms / 1000, IST).date()


def direction(
    und_bars: Sequence[OHLCV], signal_ts_ms: int | None, option_type: str, spot: float | None
) -> dict[str, Any] | None:
    """D85(2) inputs and verdict: {"vwap", "drift6", "spot", "n_bars", "f1", "f2", "pass"}; None = input missing
    (no signal time, fewer than 2 of the signal date's bars closed at/before it, or zero total volume)."""
    if signal_ts_ms is None:
        return None
    day = _ist_date(signal_ts_ms)
    bs = sorted(
        (b for b in und_bars if b.ts_ms + TF_MS <= signal_ts_ms and _ist_date(b.ts_ms) == day),
        key=lambda b: b.ts_ms,
    )
    if len(bs) < 2:
        return None
    vv = sum(float(b.volume) for b in bs)
    if vv <= 0:
        return None
    vwap = sum((b.high + b.low + b.close) / 3.0 * float(b.volume) for b in bs) / vv
    sp = spot if spot else bs[-1].close
    w = bs[-DIRECTION_DRIFT_BARS:]
    drift6 = w[-1].close - w[0].close
    sgn = -1.0 if option_type.upper() == "PE" else 1.0
    f1 = sgn * (sp - vwap) > 0
    f2 = sgn * drift6 > 0
    return {"vwap": vwap, "drift6": drift6, "spot": sp, "n_bars": len(bs), "f1": f1, "f2": f2, "pass": f1 and f2}


def _num(d: Mapping[str, Any] | None, key: str) -> float | None:
    if not d:
        return None
    v = d.get(key)
    if v is None or isinstance(v, bool):
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return None if math.isnan(f) else f  # NaN -> missing


def unwind(snapshot: Mapping[str, Any] | None, tick_size: float | None, min_ticks: float) -> bool | None:
    """D94(1): cycle E2 close >= OI-peak bar close + min_ticks x tick. None = an input is missing."""
    e2 = _num(snapshot, "cycle_e2_close")
    pk = _num(snapshot, "oi_peak_bar_close")
    if e2 is None or pk is None or tick_size is None or tick_size <= 0:
        return None
    return e2 >= pk + min_ticks * tick_size - EPS


def quadrant(
    snapshot: Mapping[str, Any] | None, tick_size: float | None, oi_now: float | None = None
) -> Quadrant | None:
    """D94(2) label over the unwind window: premium change (signal price - OI-peak bar close, +/- 1 tick) x OI
    change (OI at the signal - the OI-peak bar's OI). None when an input is missing."""
    pk_close = _num(snapshot, "oi_peak_bar_close")
    pk_oi = _num(snapshot, "oi_peak_bar_oi")
    price = _num(snapshot, "signal_price")
    now = oi_now if oi_now is not None else _num(snapshot, "oi_now")
    if pk_close is None or pk_oi is None or price is None or now is None or not tick_size or tick_size <= 0:
        return None
    dp = price - pk_close
    d_oi = now - pk_oi
    p_dir = 1 if dp >= tick_size - EPS else (-1 if dp <= -tick_size + EPS else 0)
    if p_dir == 0 or d_oi == 0:
        return "FLAT"
    if d_oi < 0:
        return "SC" if p_dir > 0 else "LU"
    return "LB" if p_dir > 0 else "SB"


# ---------------------------------------------------------------- S1.9.0 the signed zone test (D380 (1), D382)

# D382 (orchestrator default): the 'past the edge' allowance k, in ATR % units. A CODE constant, never a strategy.yaml
# key: 0 = today's abs(gap) <= limit on both sides; a k > 0 ships only through a passed CONFIRM and a spec bump.
ZONE_PAST_EDGE_ATR_K = 0.0


def _atr_usable(atr_pct: Any) -> bool:
    """An ATR % that can widen the past-edge bound: a finite number > 0 (None, a bool, NaN / inf, <= 0 -> no)."""
    if atr_pct is None or isinstance(atr_pct, bool):
        return False
    try:
        a = float(atr_pct)
    except (TypeError, ValueError):
        return False
    return math.isfinite(a) and a > 0


def zone_ok(gap_pct: float | None, limit: float, atr_pct: float | None = None, k: float | None = None) -> bool:
    """S1.9.0 (D380 (1), D382; T-334 picks M01 + M06): THE zone test, one definition for V1's / V2's zone gate
    (`evaluate`), V1 PLUS's zone confirmation (`plus_candidate`) and the V4 arming test (engine.arm_checks).
    `gap_pct` is `zone_gap_pct`, SIGNED: + = the APPROACH side (the stock short of the zone edge: a PE's above its
    support top, a CE's below its resistance bottom); - = PAST the edge in the trade's direction.
      approach  gap >= 0: pass when gap <= limit + 1e-9
      past      gap < 0:  pass when -gap <= limit + k x atr_pct + 1e-9
    k None = ZONE_PAST_EDGE_ATR_K (read at call time); a k <= 0 or an unusable ATR (`_atr_usable`) adds 0, so the
    allowance only ever WIDENS the past side, never the approach side (= t320b_core.dk_pass / the Lab's D'(k)).
    At k 0 this is exactly the S1.4.0 test abs(gap) <= limit + 1e-9. A None / NaN gap -> False (never inside)."""
    if gap_pct is None or isinstance(gap_pct, bool):
        return False
    gap = float(gap_pct)
    if math.isnan(gap):
        return False
    if gap >= 0:
        return gap <= float(limit) + EPS
    kk = ZONE_PAST_EDGE_ATR_K if k is None else float(k)
    allow = kk * float(atr_pct) if kk > 0 and atr_pct is not None and _atr_usable(atr_pct) else 0.0
    return -gap <= float(limit) + allow + EPS


def build_context(
    meta: ContractMeta,
    *,
    spot: float | None,
    volumes: Sequence[float],
    oi_peak: float | None,
    oi_now: float | None,
    gates: GatesCfg,
    hold: HoldCfg,
    und_bars: Sequence[OHLCV] = (),
    tick_size: float | None = None,
    snapshot: Mapping[str, Any] | None = None,
    signal_ts_ms: int | None = None,
    rules: str = RULES_V1,
) -> SignalContext:
    """Everything the router and the templates need at signal time, gates evaluated. Pure.

    S1.3.0 inputs (all optional, so an S1.2.0 caller keeps working; a missing one -> that gate is UNKNOWN):
      und_bars      the UNDERLYING's closed 5-min bars today (any object with ts_ms/high/low/close/volume).
      tick_size     the contract's tick in rupees; None -> meta.tick_size, then snapshot["tick_size"].
      snapshot      Signal.snapshot (engine): the unwind-gate and quadrant inputs.
      signal_ts_ms  the signal time; None -> snapshot["signal_ts_ms"].
    S1.5.0 (D161(3)):
      rules         the scanner's gate rules (RULES): "V1" / "V1B" = DIRECTION per GatesCfg.v1_direction_mode ("any"
                    = F1 OR F2) and the dynamic zone limit (`zone_limit`, from meta.atr_pct); "V2" = F1 AND F2 and
                    the fixed zone_max_gap_pct. Anything else raises ValueError. The context records zone_limit_pct,
                    f1 / f2 (None = direction not evaluated) and carried (= meta.carried).
    S1.7.0 (T-267): rules "V2T" (the V2 Scanner) = the context's numbers only (spot, zone gap, liquidity, OI, lot,
                    quadrant): NONE of the four gates is evaluated (gate_reasons / gate_unknown empty; its tier is
                    `v2t_tier`, the router writes its WATCHLIST reason), hold always "swing" (D254: a V2 trade is held
                    on its own zone family, whatever the list) and no invalidation (the stop is the stock's 1h zone,
                    snapshot["v2t"]["stop"], never the list's zone edge).
    """
    if rules not in RULES:
        raise ValueError(f"build_context: rules must be one of {list(RULES)}, got {rules!r}")
    if rules == RULES_V2T:
        return _v2t_context(meta, spot=spot, volumes=volumes, oi_peak=oi_peak, oi_now=oi_now, gates=gates,
                            tick_size=tick_size, snapshot=snapshot)  # fmt: skip
    style = hold_for(meta.list_name, hold)
    gap = zone_gap_pct(meta.option_type, spot, meta.zone_edge)
    limit = zone_limit(getattr(meta, "atr_pct", None), gates, rules)
    avg_lots, zeros = liquidity(volumes, meta.lot_size, gates.liq_sma_bars)
    failed_t, unknown_t = evaluate(gap, avg_lots, zeros, gates, zone_limit=limit, atr_pct=getattr(meta, "atr_pct", None))
    failed, unknown = list(failed_t), list(unknown_t)
    any_mode = rules in V1_ZONE_RULES and getattr(gates, "v1_direction_mode", V1_DIRECTION_MODE_DEFAULT) == DIRECTION_ANY
    f1: bool | None = None
    f2: bool | None = None
    tick = tick_size if tick_size is not None else meta.tick_size
    if tick is None:
        tick = _num(snapshot, "tick_size")
    sig_ts = signal_ts_ms
    if sig_ts is None:
        ts = _num(snapshot, "signal_ts_ms")
        sig_ts = int(ts) if ts is not None else None
    if gates.enabled and (style == "intraday" or getattr(gates, "all_lists", ALL_LISTS_DEFAULT)):
        if getattr(gates, "direction_enabled", DIRECTION_ENABLED_DEFAULT):
            d = direction(und_bars, sig_ts, meta.option_type, spot)
            if d is None:
                unknown.append(GATE_DIRECTION)
            else:
                f1, f2 = bool(d["f1"]), bool(d["f2"])
                if not ((f1 or f2) if any_mode else d["pass"]):
                    failed.append(GATE_DIRECTION)
        if getattr(gates, "unwind_enabled", UNWIND_ENABLED_DEFAULT):
            min_ticks = float(getattr(gates, "unwind_min_ticks", UNWIND_MIN_TICKS_DEFAULT))
            u = unwind(snapshot, tick, min_ticks)
            if u is None:
                unknown.append(GATE_UNWIND)
            elif not u:
                failed.append(GATE_UNWIND)
    return SignalContext(
        hold=style,
        spot=spot,
        zone_edge=meta.zone_edge,
        zone_gap_pct=gap,
        oi_peak=oi_peak,
        oi_now=oi_now,
        avg_vol_lots=avg_lots,
        zero_vol_bars=zeros,
        lot_size=meta.lot_size,
        invalidation=meta.zone_edge if style == "swing" else None,
        gate_reasons=tuple(failed),
        gate_unknown=tuple(unknown),
        quadrant=quadrant(snapshot, tick, oi_now),
        build_oi=meta.build_oi,  # T-160 (D125 F9): the 09:20 build OI, a card input only (no gate reads it)
        zone_limit_pct=limit,
        f1=f1,
        f2=f2,
        carried=bool(getattr(meta, "carried", False)),
    )


def _v2t_context(
    meta: ContractMeta,
    *,
    spot: float | None,
    volumes: Sequence[float],
    oi_peak: float | None,
    oi_now: float | None,
    gates: GatesCfg,
    tick_size: float | None,
    snapshot: Mapping[str, Any] | None,
) -> SignalContext:
    """build_context(rules="V2T"): the V2 Scanner's context -- numbers only, no V1 gate verdicts (see build_context)."""
    tick = tick_size if tick_size is not None else meta.tick_size
    if tick is None:
        tick = _num(snapshot, "tick_size")
    avg_lots, zeros = liquidity(volumes, meta.lot_size, gates.liq_sma_bars)
    return SignalContext(
        hold="swing",
        spot=spot,
        zone_edge=meta.zone_edge,
        zone_gap_pct=zone_gap_pct(meta.option_type, spot, meta.zone_edge),
        oi_peak=oi_peak,
        oi_now=oi_now,
        avg_vol_lots=avg_lots,
        zero_vol_bars=zeros,
        lot_size=meta.lot_size,
        invalidation=None,
        gate_reasons=(),
        gate_unknown=(),
        quadrant=quadrant(snapshot, tick, oi_now),
        build_oi=meta.build_oi,
        zone_limit_pct=None,
        f1=None,
        f2=None,
        carried=bool(getattr(meta, "carried", False)),
    )


def v1r_tier(snapshot) -> Tier:
    """Explicit decision bit; missing/malformed values fail closed."""
    block = snapshot.get("v1r") if isinstance(snapshot, Mapping) else None
    return TIER_ENTRY if isinstance(block, Mapping) and block.get("entered") is True else TIER_WATCHLIST


def _v1r_passes(ctx, cfg) -> int:
    snapshot = getattr(ctx, "snapshot", None)
    if isinstance(snapshot, Mapping) and "v1r" in snapshot:
        return len(GATE_NAMES) if v1r_tier(snapshot) == TIER_ENTRY else 0
    return len(passed(ctx, cfg))
