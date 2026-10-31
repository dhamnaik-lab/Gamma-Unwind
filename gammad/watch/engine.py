"""Strategy Step 4 (spec S1.x): per-contract E2 / E1 state machine on closed 5m bars + live ticks.

The SAME engine runs live (watcher) and in replay (backtest, spec Step 5), so it is pure: no I/O,
no feed, no bars.py import. The caller passes today's closed 5m bars (oldest first), the forming
bar, the tick, `oi_now` and `oi_peak_today` (Step 3 tracks the peak from 09:15).

Conditions, evaluated on the LAST CLOSED bar (`bars[-1]`):

A  oi_unwind   drop = (oi_peak - oi_now) / oi_peak >= oi_drop_alert - oi_drop_tolerance (S1.4.0, D116(4): the
               8 % line with 1 pt of tolerance = 7 %); STRONG when >= oi_drop_strong (never tolerated).
B  vol_build   ONE reference value SMA20 = mean volume of 20 closed bars. With `sma_inclusive=True`
               (default) the window is the 20 bars ENDING AT the tested bar, inclusive; with False it
               is the 20 bars BEFORE the tested bar. B holds when bars[-1].volume > SMA20 AND at least
               `vol_build_min_of_last3` of the last `vol_build_window` (3) closed bars (bars[-1]
               included) have volume > that same SMA20.
C  consol      over the last `consol_bars` (6) closed bars: range = max(high) - min(low);
               drift = close[-1] - close[-6] (first and last of the window). C holds when
               range <= consol_range_atr * ATR14, OR (drift <= 0 AND range <= consol_drift_range_atr
               * ATR14). ATR14 = Pine ta.atr (RMA of true range) over the closed bars, value at bars[-1]
               (gammad.core.zones.atr, the one ATR implementation in the repo).
H  hygiene     S1.3.0 (D85(1), "R2"), on BOTH holds: the same 6-bar base is VALID only if its range >=
               `h_min_range_ticks` (2) x the contract's tick AND at most `h_max_zero_vol_slots` (1) of its
               6 five-minute SLOTS has zero volume. Slots are counted by CLOCK (09:15-aligned session slots
               ending at the tested bar; before today's first slot the count continues into the previous
               session's last slots), and a slot with NO bar counts as zero volume -- a missing bar must not
               silently lengthen the base. Tick = the caller's `tick_size` (watchlist.tick_size, rupees);
               unknown -> H_FALLBACK_TICK (Rs 0.01, the smallest NSE F&O tick: never rejects a real base
               for want of the tick, still rejects a zero-range one).

States: IDLE -> (E2 at a bar close where A and B and C and H) -> ARMED -> (E1) -> DONE.
  E2   records consol_lo / consol_hi / consol_mid from the same 6 bars and oi_at_alert = oi_now.
  E1   (i)  shakeout, evaluated at bar close: among the bars closed AFTER arming, some bar had
            low < consol_lo, and the bar closing now closes > consol_mid, at most
            `shakeout_recover_bars` bars after that undercut bar (0 = the undercut bar itself).
       (ii) breakout, evaluated per tick: tick.ltp > consol_hi AND forming_bar.volume > SMA20 of the
            last 20 CLOSED bars (the forming bar is never part of its own SMA).
  sl   = consol_lo.
Dedupe (S1.8.0, D287 (1)): ONE E2 and ONE E1 per contract per day. A contract in ARMED (E1 never came) or DONE
returns to IDLE only at `reset_day()`, which clears everything; the same in both editions.
  Re-arm (RESEARCH only, `oi_rearm_extra` set; None in live, D288 Q1): the pre-S1.8.0 rule -- ARMED / DONE returns to
  IDLE when, at a bar close, oi_now <= oi_at_alert * (1 - oi_rearm_extra), where oi_at_alert is the OI at the most
  recent alert (E2 or E1).
Restart (I8): `force_armed` puts a recorded E2 the restart replay did not reproduce back on the engine.
Snapshot (S1.3.0, D95(4)): every Signal carries `snapshot` = the Evaluation.to_dict() of the bar it was
  decided on + bar_ts_ms (the signal bar's open) / signal_ts_ms / signal_price / oi_now / oi_peak /
  tick_size + the ARMING CYCLE's unwind inputs, fixed at the E2 and inherited by that cycle's E1 (D94(1)):
  cycle_e2_ts_ms / cycle_e2_close and the OI-peak bar = the FIRST of today's closed bars (IST date of the
  E2 bar) whose OI equals oi_peak (oi_peak_bar_ts_ms / _close / _oi, oi_peak_bar_exact=True). When no
  bar's OI equals the peak (live: the peak is a tick maximum that can fall inside a bar) the first bar
  holding today's highest BAR OI is used and oi_peak_bar_exact=False. gammad.alerts.gates reads these.
e1_mode (EngineParams / WatcherCfg): "breakout" is everything above. The T-119 standalone trigger "oi_vol_premium" (V3)
  was removed in 0.8.1 (D149(1)).
V1 B (S1.5.0, D151(2), D161(4); e1_mode "v1b", the owner's V1 B scanner, knobs = V1BCfg / yaml `v1b:`): no base, no
  arming, never an E2, no breakout tick, no shakeout. Built AS MEASURED (docs/research/2026-09-29-tuning/
  tune_capture.py features() + docs/research/2026-09-29-journal/journal.py v1b() / first_per_stock()). At each bar
  close, bars[-1] fires an E1 when ALL hold on it:
    close >= v1b_breakout_mult (1.05) x max(high of bars[-2], bars[-3])  (the previous session's bars when today's
      first bars are the tested one's neighbours, exactly as the research series seed + today);
    (peak - bars[-1].oi) / peak >= v1b_oi_drop_pct (10) %, peak = the highest BAR OI of today's bars (IST date of
      bars[-1]) up to and including bars[-1]: the bar-CLOSE peak, never the seed's and never the caller's `oi_peak`
      (live's is a tick maximum that would fire more often, DESIGN-REVIEW risk 4); the caller's oi_now is not read
      either (the research used the bar's OI);
    bars[-1].volume > SMA(v1b_vol_sma_len = 20) of volume over the bars ENDING AT bars[-1] (inclusive);
    close >= v1b_min_premium_ticks (20) x the contract's tick (G1 at signal time; unknown tick -> H_FALLBACK_TICK);
    bars[-1] closes BEFORE v1b_last_bar_close (15:20 IST): the journal's book drops a bar whose close reads >= 15:20.
  entry = close; sl = consol_lo = min(bars[-1].low, bars[-2].low); consol_hi = the breakout reference;
  strong = the bar-close drop >= oi_drop_strong (the STRONG-drop marker). NO LOCK: every qualifying bar fires (the V1 B
  ENTRY tier and the V1 PLUS component pick different bars; the router records the firsts); the contract stays IDLE
  and holds no re-arm reference, so V1 B is stateless across bars (a restart needs no replay). Liquidity, the Rs 2
  floor and the turnover tiers are the router's (gammad.alerts.gates), not the trigger's. Its snapshot adds
  turnover_rs (close x volume: RUPEES, because a bar's volume is a QUANTITY -- live bars are deltas of the feed's
  cumulative day volume; measured on Dhan-fed bars 2026-09-30: 6,662 of 6,662 non-zero recorded 28 Sep 5-min option
  bar volumes are whole lot multiples, and 295,975 of 295,975 in the Dhan history cache the research also read; the
  other feeds document
  volume_day as quantity, D162(1)), breakout_ref, oi_peak_close and vol_sma, and the unwind
  inputs with the signal bar standing in for the arming bar (cycle_e2_close = its close; OI-peak bar = the first of
  today's bars at oi_peak_close). Warm-up: max(v1b_vol_sma_len, 3) closed bars.
V4 arming tests (T-160, D125 F2-F4 / F8; research arming tests kept by D161(8), no live scanner sets them since V4a / V4b
  left the registry in 0.8.1; all OFF by default so V1 / V2 arm exactly as above): after A, B, C and H held on an IDLE
  contract, each SET test must also hold or there is no E2 (the contract stays IDLE and may arm on a later bar):
  arm_requires_covering     bars[-1].close >= the OI-peak bar's close + 1 tick (gammad.alerts.gates.unwind, the tier
                            gate's test moved to arming; OI-peak bar as in the snapshot below; unknown tick -> fails).
  min_build_over_open_pct   oi_peak >= ArmContext.build_oi x (1 + pct / 100) (the 09:20 build OI).
  arm_max_zone_gap_pct      gates.zone_ok(gates.zone_gap_pct(option_type, spot, zone_edge), pct) (D382, signed).
  min_dte                   ArmContext.dte >= min_dte.
  The last three read the caller's `arm_ctx`; with arm_ctx None they are skipped. A missing input inside a given
  ArmContext (no build OI, no spot, no DTE) FAILS its test. The inputs and verdicts go into the cycle's snapshot
  (arm_covering / arm_build_ratio / arm_zone_gap_pct / arm_dte), only for the tests that are set.
Warm-up: no signal until max(SMA window, ATR14 + 1, consol_bars) closed bars exist; logged once.
Latency: latency_ms = clock() - recv_ms, where recv_ms is the tick's receive time for a breakout, and
for a bar-close signal the caller's `recv_ms` (the event that closed the bar) or, if not given, the
bar's nominal close time (bars[-1].ts_ms + signal_tf_min).
"""

from __future__ import annotations

import math
import time
from collections.abc import Callable, Sequence
from dataclasses import asdict, dataclass, replace
from datetime import date, datetime
from enum import StrEnum
from itertools import pairwise
from typing import Any

import numpy as np

from gammad import edition
from gammad.alerts.gates import UNWIND_MIN_TICKS_DEFAULT, unwind, zone_gap_pct, zone_ok
from gammad.config import CONSOL_MODES, E1_MODES, IST, V1BCfg, WatcherCfg
from gammad.core.zones import atr as pine_atr
from gammad.log import get_logger
from gammad.watch.types import ArmContext, Bar, Signal, SignalKind, TickView

try:  # D110: an owner-edition module, not shipped in the client package; only ITS absence is tolerated
    from gammad.watch.shakeout import shakeout_trigger
except ModuleNotFoundError as _exc:
    if _exc.name != "gammad.watch.shakeout":
        raise
    shakeout_trigger = None  # type: ignore[assignment]

log = get_logger(__name__)

H_FALLBACK_TICK = 0.01  # rupees; the smallest NSE F&O option tick (D84), used by H when the contract's tick is unknown
SESSION_OPEN_MIN = 9 * 60 + 15  # 09:15 IST, minutes after midnight


def wall_clock_ms() -> int:
    return time.time_ns() // 1_000_000


def hhmm_minutes(hhmm: str) -> int:
    """Minutes after midnight of an "HH:MM" string (V1B's last_bar_close); ValueError on anything else."""
    if not isinstance(hhmm, str) or len(hhmm) != 5 or hhmm[2] != ":" or not (hhmm[:2] + hhmm[3:]).isdigit():
        raise ValueError(f"EngineParams: v1b_last_bar_close must be \"HH:MM\", got {hhmm!r}")
    h, m = int(hhmm[:2]), int(hhmm[3:])
    if h > 23 or m > 59:
        raise ValueError(f"EngineParams: v1b_last_bar_close must be \"HH:MM\", got {hhmm!r}")
    return h * 60 + m


class State(StrEnum):
    IDLE = "IDLE"
    ARMED = "ARMED"
    DONE = "DONE"


@dataclass(frozen=True, slots=True)
class EngineParams:
    signal_tf_min: int = 5
    oi_drop_alert: float = 0.08
    oi_drop_strong: float = 0.12
    # D287 (1): None (default, live) = no re-arm: IDLE -> ARMED -> DONE, IDLE again only at reset_day(). A float is the
    # RESEARCH-only pre-S1.8.0 re-arm (module doc "Dedupe / re-arm"; D288 Q1)
    oi_rearm_extra: float | None = None
    # S1.4.0 (D116(4)): A holds at oi_drop_alert - oi_drop_tolerance; STRONG and the re-arm extra are not tolerated
    oi_drop_tolerance: float = 0.01
    vol_sma_len: int = 20
    vol_build_window: int = 3
    vol_build_min_of_last3: int = 2
    consol_bars: int = 6
    consol_atr_len: int = 14
    consol_range_atr: float = 1.0
    consol_drift_range_atr: float = 1.5
    shakeout_recover_bars: int = 3
    sma_inclusive: bool = True
    # S1.3.0 test H (D85(1)); h_min_range_ticks=0 with h_max_zero_vol_slots=consol_bars switches H off (S1.2.0).
    h_min_range_ticks: float = 2.0
    h_max_zero_vol_slots: int = 1
    # the E1 trigger: "breakout" (V1 / V2) or "v1b" (V1 B, module doc "V1 B"); V3's "oi_vol_premium" was removed in 0.8.1
    e1_mode: str = "breakout"
    # S1.5.0 (D161(4)): the V1 B trigger's knobs (gammad.config.V1BCfg, yaml `v1b:`); read only when e1_mode == "v1b"
    v1b_breakout_mult: float = V1BCfg.breakout_mult
    v1b_oi_drop_pct: float = V1BCfg.oi_drop_pct
    v1b_vol_sma_len: int = V1BCfg.vol_sma_len
    v1b_min_premium_ticks: float = V1BCfg.min_premium_ticks
    v1b_last_bar_close: str = V1BCfg.last_bar_close  # IST "HH:MM": a bar closing at or after it never fires
    # RESEARCH 2026-09-27 (dynamic base, owner question on MARUTI 24-Sep): how condition C measures the base.
    #   "atr_fixed"  = the spec rule above (default; every other mode is research only, never a client rule)
    #   "percentile" = the option's 6-bar range is within the lowest consol_pctile % of TODAY's rolling 6-bar
    #                  ranges so far (>= consol_pctile_min_n ranges needed), no ATR multiple
    #   "variable"   = the LONGEST window of consol_var_min..consol_var_max bars whose range <= consol_range_atr
    #                  x ATR14 is the base (consol_lo/hi = that window); none -> C fails
    #   "underlying" = the UNDERLYING's 6-bar range <= consol_und_range_atr x its own ATR14 (its 5-min bars closed
    #                  at/before this bar); the premium need not be flat; consol_lo/hi still = the option's 6 bars
    #   "hybrid"     = "underlying" AND the option's 6-bar drift >= -consol_hybrid_min_drift_atr x ATR14 (the
    #                  premium is not sliding against a buyer)
    consol_mode: str = "atr_fixed"
    consol_pctile: float = 30.0
    consol_pctile_min_n: int = 6
    consol_var_min: int = 4
    consol_var_max: int = 12
    consol_und_range_atr: float = 1.0
    consol_hybrid_min_drift_atr: float = 0.0
    # RESEARCH 2026-09-27: False = E2 arms on A and C and H alone (B stays on the E1 breakout tick, whose forming
    # bar must already out-trade SMA20). A quiet base has low volume by definition; measured on MARUTI 24-Sep.
    e2_requires_b: bool = True
    # T-160 (D125 F2-F4, F8), research arming tests (the V4a / V4b shadows, retired from the registry in 0.8.1, D161(8)
    # keeps them; no live scanner sets them): four OPTIONAL tests, OFF by default (V1 / V2 set none, so their E2 is
    # decided exactly as before). Read only when set; checked after A, B, C and H held
    # (module doc, "V4 arming tests"). An E2 they refuse leaves the contract IDLE (it may arm on a later bar).
    arm_requires_covering: bool = False  # F3: E2 bar close >= OI-peak bar close + 1 tick (gates.unwind's test)
    min_build_over_open_pct: float | None = None  # F2: oi_peak >= build OI x (1 + pct / 100); needs ArmContext
    arm_max_zone_gap_pct: float | None = None  # F8: gates.zone_ok(zone_gap_pct, pct) at the E2 bar; needs ArmContext
    min_dte: int | None = None  # V4b: no E2 while DTE < min_dte; needs ArmContext

    def __post_init__(self) -> None:
        if self.e1_mode not in E1_MODES:
            raise ValueError(f"EngineParams: e1_mode must be one of {list(E1_MODES)}, got {self.e1_mode!r}")
        if self.consol_mode not in CONSOL_MODES:
            raise ValueError(
                f"EngineParams: consol_mode must be one of {list(CONSOL_MODES)}, got {self.consol_mode!r}"
            )
        hhmm_minutes(self.v1b_last_bar_close)  # raises on anything but "HH:MM"

    @classmethod
    def from_config(
        cls, cfg: WatcherCfg, *, sma_inclusive: bool = True, v1b: V1BCfg | None = None
    ) -> EngineParams:
        # The H keys are read when gammad.config.WatcherCfg carries them (watcher.h_min_range_ticks /
        # watcher.h_max_zero_vol_slots); until then the S1.3.0 values above apply.
        # `v1b` = Settings.v1b (None -> the V1BCfg defaults); the registry sets e1_mode "v1b" on the V1 B scanner only,
        # so the knobs are inert on every other params object built here.
        vb = v1b if v1b is not None else V1BCfg()
        return cls(
            v1b_breakout_mult=float(vb.breakout_mult),
            v1b_oi_drop_pct=float(vb.oi_drop_pct),
            v1b_vol_sma_len=int(vb.vol_sma_len),
            v1b_min_premium_ticks=float(vb.min_premium_ticks),
            v1b_last_bar_close=str(vb.last_bar_close),
            h_min_range_ticks=float(getattr(cfg, "h_min_range_ticks", 2.0)),
            h_max_zero_vol_slots=int(getattr(cfg, "h_max_zero_vol_slots", 1)),
            e1_mode=cfg.e1_mode,
            consol_mode=str(getattr(cfg, "consol_mode", "atr_fixed")),
            consol_pctile=float(getattr(cfg, "consol_pctile", 30.0)),
            consol_pctile_min_n=int(getattr(cfg, "consol_pctile_min_n", 6)),
            consol_var_min=int(getattr(cfg, "consol_var_min", 4)),
            consol_var_max=int(getattr(cfg, "consol_var_max", 12)),
            consol_und_range_atr=float(getattr(cfg, "consol_und_range_atr", 1.0)),
            consol_hybrid_min_drift_atr=float(getattr(cfg, "consol_hybrid_min_drift_atr", 0.0)),
            e2_requires_b=bool(getattr(cfg, "e2_requires_b", True)),
            # T-160: the four V4 arming tests are NOT read from a config (no yaml key; engine_pool's registry sets
            # them on the V4 shadows), so every params object built here has them OFF
            signal_tf_min=cfg.signal_tf_min,
            oi_drop_alert=cfg.oi_drop_alert,
            oi_drop_strong=cfg.oi_drop_strong,
            oi_drop_tolerance=float(getattr(cfg, "oi_drop_tolerance", 0.01)),
            oi_rearm_extra=cfg.oi_rearm_extra,
            vol_sma_len=cfg.vol_sma_len,
            vol_build_min_of_last3=cfg.vol_build_min_of_last3,
            consol_bars=cfg.consol_bars,
            consol_atr_len=cfg.consol_atr_len,
            consol_range_atr=cfg.consol_range_atr,
            consol_drift_range_atr=cfg.consol_drift_range_atr,
            shakeout_recover_bars=cfg.shakeout_recover_bars,
            sma_inclusive=sma_inclusive,
        )

    @property
    def arm_tests_on(self) -> bool:
        """True when any T-160 arming test is set (research only, no live scanner); False for V1 / V2."""
        return (
            self.arm_requires_covering
            or self.min_build_over_open_pct is not None
            or self.arm_max_zone_gap_pct is not None
            or self.min_dte is not None
        )

    @property
    def warmup_bars(self) -> int:
        if self.e1_mode == "v1b":  # its SMA window (inclusive) and the tested bar + the two before it
            return max(self.v1b_vol_sma_len, 3)
        sma_need = self.vol_sma_len + (0 if self.sma_inclusive else 1)
        return max(sma_need, self.consol_atr_len + 1, self.consol_bars, self.vol_build_window)


# ---------------------------------------------------------------- pure condition helpers


def oi_drop(oi_now: float, oi_peak: float) -> float | None:
    """Fractional drop from the day's peak; None when the peak is unusable."""
    if not oi_peak or oi_peak <= 0 or math.isnan(oi_peak) or math.isnan(oi_now):
        return None
    return (oi_peak - oi_now) / oi_peak


A_TOL_EPS = 1e-9  # 8 % - 1 pt must admit a drop of exactly 7.0 % despite float noise (D116(4))


def a_holds(drop: float, p: EngineParams) -> bool:
    """Condition A (S1.4.0, D116(4)): drop >= oi_drop_alert - oi_drop_tolerance. A drop over the line itself is
    decided exactly as before the tolerance existed; only the band below it is new."""
    if drop >= p.oi_drop_alert:
        return True
    return p.oi_drop_tolerance > 0 and drop >= p.oi_drop_alert - p.oi_drop_tolerance - A_TOL_EPS


def a_reason(drop: float, p: EngineParams) -> str:
    """The A part of an E2 reason. A drop over the line keeps the pre-S1.4.0 text byte for byte; a pass that
    needed the tolerance names it: `A oi -7.8% off peak (>= 8% line, tolerance 1 pt)`."""
    if drop >= p.oi_drop_alert or p.oi_drop_tolerance <= 0:
        return f"A oi -{drop * 100:.1f}% off peak (>= {p.oi_drop_alert * 100:g}%)"
    return (
        f"A oi -{drop * 100:.1f}% off peak (>= {p.oi_drop_alert * 100:g}% line,"
        f" tolerance {p.oi_drop_tolerance * 100:g} pt)"
    )


def vol_sma(bars: Sequence[Bar], length: int, inclusive: bool) -> float | None:
    """SMA of volume for the test of bars[-1]: the `length` bars ending at bars[-1] (inclusive) or
    the `length` bars before it (exclusive). None when there are not enough bars."""
    window = bars[-length:] if inclusive else bars[-length - 1 : -1]
    if len(window) < length:
        return None
    return sum(b.volume for b in window) / length


def atr_last(bars: Sequence[Bar], length: int) -> float | None:
    """Pine ta.atr(length) at bars[-1]; needs length + 1 closed bars (one prior close for a full TR)."""
    if len(bars) < length + 1:
        return None
    high = np.fromiter((b.high for b in bars), dtype=float, count=len(bars))
    low = np.fromiter((b.low for b in bars), dtype=float, count=len(bars))
    close = np.fromiter((b.close for b in bars), dtype=float, count=len(bars))
    value = float(pine_atr(high, low, close, length)[-1])
    return None if math.isnan(value) else value


def ist_date(ts_ms: int) -> date:
    return datetime.fromtimestamp(ts_ms / 1000, IST).date()


def _session_open_ms(d: date) -> int:
    base = datetime(d.year, d.month, d.day, tzinfo=IST)
    return int(base.timestamp() * 1000) + SESSION_OPEN_MIN * 60_000


def zero_volume_slots(bars: Sequence[Bar], n_slots: int, tf_min: int = 5) -> int:
    """H's zero-volume count: of the `n_slots` consecutive `tf_min` slots ENDING at bars[-1]'s slot, how many
    have no bar or a bar with volume <= 0. Within a session slots are 09:15-aligned CLOCK slots, so a missing bar
    counts as zero volume. Across the overnight boundary the count continues into the previous session present in
    `bars` (the D24 seed) from ITS LAST RECORDED BAR backwards (the slot just before today's 09:15 is that bar):
    a seed whose history ends before 15:25 (Dhan 5m history can end early) is not read as an illiquid close.
    Slots older than the oldest bar count as zero volume."""
    if not bars or n_slots <= 0:
        return max(n_slots, 0)
    tf_ms = tf_min * 60_000
    by_date: dict[date, list[Bar]] = {}
    for b in bars:
        by_date.setdefault(ist_date(b.ts_ms), []).append(b)
    dates = sorted(by_date)
    # start[d] = the global position of session d's 09:15 slot; the last session starts at 0 and each earlier
    # session is placed so that its last bar sits one slot before the next session's 09:15 slot.
    start: dict[date, int] = {dates[-1]: 0}
    for k in range(len(dates) - 2, -1, -1):
        d = dates[k]
        last_slot = max((b.ts_ms - _session_open_ms(d)) // tf_ms for b in by_date[d])
        start[d] = start[dates[k + 1]] - 1 - last_slot
    opens = {d: _session_open_ms(d) for d in dates}

    def pos(b: Bar) -> int:
        d = ist_date(b.ts_ms)
        return start[d] + (b.ts_ms - opens[d]) // tf_ms

    end = pos(bars[-1])
    first = end - n_slots + 1
    traded: set[int] = set()
    for b in reversed(bars):
        p = pos(b)
        if p < first:
            break
        if p <= end and b.volume > 0:
            traded.add(p)
    return n_slots - len(traded)


def oi_peak_bar(bars: Sequence[Bar], oi_peak: float | None) -> tuple[Bar, bool] | None:
    """(the OI-peak bar, exact) for D94's unwind gate: the FIRST of today's bars (IST date of bars[-1]) whose OI
    equals `oi_peak` (exact=True); when none does, the first bar holding today's highest bar OI (exact=False).
    None when today has no bar with a positive OI."""
    if not bars:
        return None
    today_d = ist_date(bars[-1].ts_ms)
    today = [b for b in bars if ist_date(b.ts_ms) == today_d]
    if oi_peak:
        for b in today:
            if abs(b.oi - oi_peak) < 0.5:  # OI is a contract count: an integer carried as float
                return b, True
    best: Bar | None = None
    for b in today:
        if b.oi > 0 and (best is None or b.oi > best.oi):
            best = b
    return (best, False) if best is not None else None


V1B_EPS = 1e-9  # the research's float slack on every threshold (tune_capture.py features, journal.py v1b)


@dataclass(frozen=True, slots=True)
class V1BHit:
    """What V1 B measured on the bar that fired (module doc, "V1 B")."""

    breakout_ref: float  # max(high of bars[-2], bars[-3])
    oi_peak_close: float  # today's highest bar OI up to and including bars[-1]
    drop: float  # fraction off oi_peak_close
    vol_sma: float  # SMA(v1b_vol_sma_len) of volume, bars[-1] included
    stop: float  # min(bars[-1].low, bars[-2].low)
    turnover_rs: float  # bars[-1].close x bars[-1].volume (volume = quantity -> rupees)


def v1b_trigger(bars: Sequence[Bar], p: EngineParams, tick_size: float | None) -> V1BHit | None:
    """V1 B on bars[-1] (module doc, "V1 B"): the V1BHit when every test holds, else None. Pure; the same expressions
    as the research (ratio >= mult - eps, drop >= pct / 100 - eps, volume > SMA, close >= ticks x tick - eps)."""
    if len(bars) < max(p.v1b_vol_sma_len, 3):
        return None
    last, prev1, prev2 = bars[-1], bars[-2], bars[-3]
    d = ist_date(last.ts_ms)
    day_start = int(datetime(d.year, d.month, d.day, tzinfo=IST).timestamp() * 1000)
    if last.ts_ms + p.signal_tf_min * 60_000 >= day_start + hhmm_minutes(p.v1b_last_bar_close) * 60_000:
        return None
    peak = 0.0
    for b in reversed(bars):  # today's bars only (oldest first in `bars`): the seed never holds the peak
        if b.ts_ms < day_start:
            break
        peak = max(peak, b.oi)  # max keeps its first argument unless b.oi is greater: a NaN OI never becomes the peak
    if peak <= 0:
        return None
    hh = max(prev1.high, prev2.high)
    if hh <= 0 or not last.close / hh >= p.v1b_breakout_mult - V1B_EPS:
        return None
    drop = (peak - last.oi) / peak
    if not drop >= p.v1b_oi_drop_pct / 100.0 - V1B_EPS:
        return None
    sma = vol_sma(bars, p.v1b_vol_sma_len, True)
    if sma is None or not last.volume > sma:
        return None
    tick = tick_size if tick_size is not None and tick_size > 0 else H_FALLBACK_TICK
    if not last.close >= p.v1b_min_premium_ticks * tick - V1B_EPS:
        return None
    return V1BHit(
        breakout_ref=float(hh),
        oi_peak_close=float(peak),
        drop=float(drop),
        vol_sma=float(sma),
        stop=float(min(last.low, prev1.low)),
        turnover_rs=float(last.close * last.volume),
    )


def arm_checks(
    bars: Sequence[Bar],
    oi_peak: float,
    p: EngineParams,
    tick_size: float | None,
    arm_ctx: ArmContext | None,
) -> tuple[bool, dict[str, Any]]:
    """T-160 (D125 F2-F4, F8): the V4 arming tests set in `p`, on bars[-1] (module doc, "V4 arming tests").
    Returns (ok, record); record holds each SET test's input and verdict (it goes into the E2 snapshot). A test whose
    input is missing FAILS (the shadow never arms on a test it could not run); the three ArmContext tests are skipped
    when `arm_ctx` is None, so a caller that passes none keeps its pre-T-160 behaviour."""
    rec: dict[str, Any] = {}
    ok = True
    if p.arm_requires_covering:
        found = oi_peak_bar(bars, oi_peak)
        pk_close = found[0].close if found is not None else None
        snap = {"cycle_e2_close": bars[-1].close, "oi_peak_bar_close": pk_close}
        cover = unwind(snap, tick_size, UNWIND_MIN_TICKS_DEFAULT)
        rec["arm_covering"] = cover
        ok = ok and cover is True
    if arm_ctx is None:
        return ok, rec
    if p.min_build_over_open_pct is not None:
        b = arm_ctx.build_oi
        ratio = oi_peak / b if b and b > 0 and oi_peak and oi_peak > 0 else None
        rec["arm_build_ratio"] = ratio
        ok = ok and ratio is not None and ratio >= 1.0 + p.min_build_over_open_pct / 100.0 - A_TOL_EPS
    if p.arm_max_zone_gap_pct is not None:
        gap = zone_gap_pct(arm_ctx.option_type, arm_ctx.spot, arm_ctx.zone_edge)
        rec["arm_zone_gap_pct"] = gap
        ok = ok and zone_ok(gap, p.arm_max_zone_gap_pct)  # D382: the signed test (None -> False; k 0 = abs <= pct)
    if p.min_dte is not None:
        rec["arm_dte"] = arm_ctx.dte
        ok = ok and arm_ctx.dte is not None and arm_ctx.dte >= p.min_dte
    return ok, rec


@dataclass(frozen=True, slots=True)
class Evaluation:
    """The last A / B / C values seen at a bar close (dashboard `describe()`)."""

    ts_ms: int
    oi_drop: float | None = None
    a: bool = False
    strong: bool = False
    sma: float | None = None
    vol_ratio: float | None = None
    b_count: int = 0
    b: bool = False
    atr: float | None = None
    range: float | None = None
    drift: float | None = None
    c: bool = False
    consol_lo: float | None = None
    consol_hi: float | None = None
    # S1.3.0 test H over the same 6-bar base (evaluated whenever the base exists, whatever C says)
    h: bool = False
    zero_vol_slots: int | None = None
    h_tick: float | None = None  # the tick H used (the contract's, or H_FALLBACK_TICK when unknown)
    # RESEARCH 2026-09-27 (consol_mode != "atr_fixed"): the dynamic-base inputs of this bar
    base_bars: int | None = None  # bars in the base C used (consol_bars, or the "variable" window length)
    range_pctile: float | None = None  # "percentile": this range's percentile among today's rolling ranges
    und_range: float | None = None  # "underlying"/"hybrid": the underlying's 6-bar range
    und_atr: float | None = None  # ... and its ATR14 (both None when the underlying's bars are missing)

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        # the research fields stay out of the snapshot while unused, so the spec mode's payload is byte-identical
        for k in _RESEARCH_EVAL_FIELDS:
            if d.get(k) is None:
                d.pop(k, None)
        return d


_RESEARCH_EVAL_FIELDS = ("base_bars", "range_pctile", "und_range", "und_atr")


def _today_rolling_ranges(bars: Sequence[Bar], n: int) -> list[float]:
    """The n-bar ranges of every n-bar window ENDING on a bar of bars[-1]'s IST session (oldest first)."""
    d = ist_date(bars[-1].ts_ms)
    out: list[float] = []
    for end in range(n - 1, len(bars)):
        if ist_date(bars[end].ts_ms) != d:
            continue
        w = bars[end - n + 1 : end + 1]
        out.append(max(b.high for b in w) - min(b.low for b in w))
    return out


def _dynamic_c(
    bars: Sequence[Bar], atr_v: float, p: EngineParams, und_bars: Sequence[Bar] | None
) -> tuple[bool, dict[str, Any]]:
    """Condition C for the research modes (EngineParams.consol_mode doc). Returns (c_ok, extra Evaluation fields).
    The extra fields may re-point consol_lo / consol_hi ("variable")."""
    window = bars[-p.consol_bars :]
    rng = max(b.high for b in window) - min(b.low for b in window)
    drift = window[-1].close - window[0].close
    extra: dict[str, Any] = {"base_bars": p.consol_bars}
    mode = p.consol_mode
    if mode == "percentile":
        ranges = _today_rolling_ranges(bars, p.consol_bars)
        if len(ranges) < p.consol_pctile_min_n:
            return False, extra
        rank = sum(1 for r in ranges if r <= rng) / len(ranges) * 100.0  # inclusive percentile of this range
        extra["range_pctile"] = rank
        return rank <= p.consol_pctile, extra
    if mode == "variable":
        for n in range(min(p.consol_var_max, len(bars)), p.consol_var_min - 1, -1):
            w = bars[-n:]
            lo, hi = min(b.low for b in w), max(b.high for b in w)
            if hi - lo <= p.consol_range_atr * atr_v:
                extra.update(base_bars=n, consol_lo=lo, consol_hi=hi, range=hi - lo, drift=w[-1].close - w[0].close)
                return True, extra
        return False, extra
    # "underlying" / "hybrid": the underlying's bars closed at/before this bar
    if not und_bars:
        return False, extra
    close_ms = bars[-1].ts_ms + p.signal_tf_min * 60_000
    und = [b for b in und_bars if b.ts_ms + p.signal_tf_min * 60_000 <= close_ms]
    if len(und) < max(p.consol_bars, p.consol_atr_len + 1):
        return False, extra
    und_atr = atr_last(und, p.consol_atr_len)
    uw = und[-p.consol_bars :]
    und_rng = max(b.high for b in uw) - min(b.low for b in uw)
    extra.update(und_range=und_rng, und_atr=und_atr)
    if und_atr is None or und_atr <= 0:
        return False, extra
    ok = und_rng <= p.consol_und_range_atr * und_atr
    if mode == "hybrid":
        ok = ok and drift >= -p.consol_hybrid_min_drift_atr * atr_v
    return ok, extra


def evaluate(
    bars: Sequence[Bar],
    oi_now: float,
    oi_peak: float,
    p: EngineParams,
    tick_size: float | None = None,
    und_bars: Sequence[Bar] | None = None,
) -> Evaluation:
    """A, B, C and H on bars[-1]. Assumes the caller checked warm-up; missing inputs evaluate False.
    `und_bars` = the UNDERLYING's 5-min bars of the day (research consol_mode "underlying" / "hybrid" only;
    ignored by every other mode, so the spec rule never reads them)."""
    last = bars[-1]
    ev = Evaluation(ts_ms=last.ts_ms)

    drop = oi_drop(oi_now, oi_peak)
    if drop is not None:
        ev = replace(ev, oi_drop=drop, a=a_holds(drop, p), strong=drop >= p.oi_drop_strong)

    sma = vol_sma(bars, p.vol_sma_len, p.sma_inclusive)
    if sma is not None and sma > 0:
        recent = bars[-p.vol_build_window :]
        count = sum(1 for b in recent if b.volume > sma)
        b_ok = last.volume > sma and count >= p.vol_build_min_of_last3
        ev = replace(ev, sma=sma, vol_ratio=last.volume / sma, b_count=count, b=b_ok)

    atr_v = atr_last(bars, p.consol_atr_len)
    window = bars[-p.consol_bars :]
    if atr_v is not None and atr_v > 0 and len(window) == p.consol_bars:
        lo = min(b.low for b in window)
        hi = max(b.high for b in window)
        rng = hi - lo
        drift = window[-1].close - window[0].close
        c_ok = rng <= p.consol_range_atr * atr_v or (drift <= 0 and rng <= p.consol_drift_range_atr * atr_v)
        extra: dict[str, Any] = {}
        if p.consol_mode != "atr_fixed":  # RESEARCH 2026-09-27: the dynamic-base modes replace the rule above
            c_ok, extra = _dynamic_c(bars, atr_v, p, und_bars)
            lo = extra.pop("consol_lo", lo)
            hi = extra.pop("consol_hi", hi)
            rng = extra.pop("range", rng)
            drift = extra.pop("drift", drift)
        tick = tick_size if tick_size is not None and tick_size > 0 else H_FALLBACK_TICK
        zv = zero_volume_slots(bars, p.consol_bars, p.signal_tf_min)
        h_ok = rng >= p.h_min_range_ticks * tick - 1e-9 and zv <= p.h_max_zero_vol_slots
        ev = replace(
            ev,
            atr=atr_v,
            range=rng,
            drift=drift,
            c=c_ok,
            consol_lo=lo,
            consol_hi=hi,
            h=h_ok,
            zero_vol_slots=zv,
            h_tick=tick,
            **extra,
        )
    return ev


# ---------------------------------------------------------------- the per-contract state machine


class ContractEngine:
    def __init__(
        self,
        security_id: int,
        params: EngineParams | None = None,
        *,
        clock: Callable[[], int] | None = None,
        tick_size: float | None = None,
        client_rules: bool | None = None,
    ) -> None:
        self.security_id = security_id
        self.params = params or EngineParams()
        self._clock = clock or wall_clock_ms
        # D110: the edition's signal rules. None = follow edition.is_client() at call time (live); a backtest pins
        # False (owner research, D103) so a checkout's EDITION flag cannot change what a replay fires.
        self.client_rules = client_rules
        # S1.3.0: the contract's tick in rupees (watchlist.tick_size); on_bar_close(tick_size=...) updates it.
        self.tick_size: float | None = tick_size
        self.reset_day()

    # -- lifecycle

    def reset_day(self) -> None:
        self.state = State.IDLE
        self.consol_lo: float | None = None
        self.consol_hi: float | None = None
        self.consol_mid: float | None = None
        self.oi_at_alert: float | None = None
        self.armed_bar_ts: int | None = None
        self.last_eval: Evaluation | None = None
        self.last_signal: Signal | None = None
        self.warming = True
        self._warm_logged = False
        self.signals_today = 0
        self._cycle: dict[str, Any] = {}

    # -- inputs

    def on_bar_close(
        self,
        bars: Sequence[Bar],
        oi_now: float,
        oi_peak: float,
        *,
        recv_ms: int | None = None,
        tick_size: float | None = None,
        und_bars: Sequence[Bar] | None = None,
        arm_ctx: ArmContext | None = None,
    ) -> Signal | None:
        """Call once per closed 5m bar with every closed bar of the day, oldest first. `tick_size` (rupees,
        watchlist.tick_size) feeds test H; None keeps the engine's current tick (H_FALLBACK_TICK if never set).
        `und_bars` = the underlying's 5-min bars (research consol_mode "underlying" / "hybrid" only).
        `arm_ctx` = the contract's build OI / DTE / spot / zone at this close (T-160); read only by the V4 arming
        tests (EngineParams.arm_tests_on), so V1 / V2 ignore it."""
        if tick_size is not None and tick_size > 0:
            self.tick_size = tick_size
        p = self.params
        if len(bars) < p.warmup_bars:
            self.warming = True
            if not self._warm_logged:
                log.info("engine %s warming: %d/%d closed bars", self.security_id, len(bars), p.warmup_bars)
                self._warm_logged = True
            return None
        self.warming = False
        last = bars[-1]
        close_ms = last.ts_ms + p.signal_tf_min * 60_000
        start_ms = recv_ms if recv_ms is not None else close_ms

        if p.e1_mode == "v1b":  # S1.5.0 V1 B: no base, no arming, never an E2 (module doc, "V1 B")
            return self._v1b_e1(bars, oi_now, oi_peak, close_ms, start_ms)

        client = edition.is_client() if self.client_rules is None else self.client_rules
        if self.state is State.ARMED and shakeout_trigger is not None and not client:  # D110
            sig = self._shakeout(bars, oi_now, oi_peak, close_ms, start_ms)
            if sig is not None:
                return sig

        if (  # D287 (1): research knob only; None (live, both editions) never re-arms
            p.oi_rearm_extra is not None
            and self.state in (State.ARMED, State.DONE)
            and self.oi_at_alert is not None
            and oi_now <= self.oi_at_alert * (1.0 - p.oi_rearm_extra)
        ):
            if "E2" in edition.visible_kinds():  # D98(6): owner only -- a re-arm is E2 machinery (D86)
                log.info(
                    "engine %s re-armed: oi %.0f <= %.0f x (1 - %.2f)",
                    self.security_id,
                    oi_now,
                    self.oi_at_alert,
                    p.oi_rearm_extra,
                )
            self._clear_arm()

        ev = evaluate(bars, oi_now, oi_peak, p, self.tick_size, und_bars)
        self.last_eval = ev
        b_ok = ev.b or not p.e2_requires_b  # research knob; the spec always requires B here
        if self.state is not State.IDLE or not (ev.a and b_ok and ev.c and ev.h):
            return None
        arm_rec: dict[str, Any] = {}
        if p.arm_tests_on:  # T-160: research arming tests only; V1 / V2 never enter this branch
            arm_ok, arm_rec = arm_checks(bars, oi_peak, p, self.tick_size, arm_ctx)
            if not arm_ok:
                log.debug("engine %s E2 refused by the V4 arming tests: %s", self.security_id, arm_rec)
                return None
        assert ev.consol_lo is not None and ev.consol_hi is not None  # C held
        assert ev.oi_drop is not None and ev.vol_ratio is not None and ev.atr is not None
        assert ev.range is not None and ev.drift is not None
        self.state = State.ARMED
        self.consol_lo, self.consol_hi = ev.consol_lo, ev.consol_hi
        self.consol_mid = (ev.consol_lo + ev.consol_hi) / 2.0
        self.oi_at_alert = oi_now
        self.armed_bar_ts = last.ts_ms
        # arm_rec is empty unless a V4 arming test is set, so the V1 / V2 cycle (and snapshot) is unchanged; a V4
        # cycle's E1 inherits its E2's arming record exactly as it inherits the unwind inputs (D94(1))
        self._cycle = {**self._cycle_inputs(bars, oi_peak), **arm_rec}
        reason = (
            a_reason(ev.oi_drop, p)
            + f" | B vol {ev.vol_ratio:.2f}x SMA{p.vol_sma_len}, {ev.b_count}/{p.vol_build_window} above"
            f" | C range {ev.range:.2f} vs ATR{p.consol_atr_len} {ev.atr:.2f}, drift {ev.drift:+.2f}"
        )
        snap = self._snapshot(ev, last.ts_ms, close_ms, last.close, oi_now, oi_peak)
        return self._emit(
            "E2", ev.strong, close_ms, last.close, ev.oi_drop, ev.vol_ratio, reason, start_ms, snap
        )

    def on_tick(
        self,
        tick: TickView,
        forming_bar: Bar,
        bars: Sequence[Bar],
        oi_now: float,
        oi_peak: float,
    ) -> Signal | None:
        """Breakout path of E1. Only an ARMED contract can fire; everything else is a no-op."""
        if self.state is not State.ARMED or self.consol_hi is None:
            return None
        p = self.params
        window = bars[-p.vol_sma_len :]
        if len(window) < p.vol_sma_len:
            return None
        sma = sum(b.volume for b in window) / p.vol_sma_len
        if sma <= 0 or tick.ltp <= self.consol_hi or forming_bar.volume <= sma:
            return None
        drop = oi_drop(oi_now, oi_peak) or 0.0
        ratio = forming_bar.volume / sma
        reason = (
            f"E1 breakout: tick {tick.ltp:g} > consol_hi {self.consol_hi:g},"
            f" forming vol {ratio:.2f}x SMA{p.vol_sma_len}"
        ) + self._arm_tolerance_suffix()
        strong = drop >= p.oi_drop_strong
        # The signal bar is the FORMING bar; the evaluation is the last closed bar's (no bar closed since).
        snap = self._snapshot(self.last_eval, forming_bar.ts_ms, tick.ts_ms, tick.ltp, oi_now, oi_peak)
        sig = self._emit("E1", strong, tick.ts_ms, tick.ltp, drop, ratio, reason, tick.recv_ms, snap)
        self.oi_at_alert = oi_now
        self.state = State.DONE
        return sig

    # -- dashboard

    def describe(self) -> dict[str, Any]:
        return {
            "security_id": self.security_id,
            "state": str(self.state),
            "warming": self.warming,
            "consol_lo": self.consol_lo,
            "consol_hi": self.consol_hi,
            "consol_mid": self.consol_mid,
            "oi_at_alert": self.oi_at_alert,
            "signals_today": self.signals_today,
            "last_signal": self.last_signal.to_dict() if self.last_signal else None,
            "last_eval": self.last_eval.to_dict() if self.last_eval else None,
        }

    # -- internals

    def _shakeout(
        self, bars: Sequence[Bar], oi_now: float, oi_peak: float, close_ms: int, start_ms: int
    ) -> Signal | None:
        assert self.consol_lo is not None and self.consol_mid is not None and self.armed_bar_ts is not None
        assert shakeout_trigger is not None  # the caller gates on it (D110)
        p = self.params
        hit = shakeout_trigger(
            bars,
            consol_lo=self.consol_lo,
            consol_mid=self.consol_mid,
            armed_bar_ts=self.armed_bar_ts,
            recover_bars=p.shakeout_recover_bars,
        )
        if hit is None:
            return None
        last, reason = hit.bar, hit.reason + self._arm_tolerance_suffix()
        sma = vol_sma(bars, p.vol_sma_len, p.sma_inclusive)
        ratio = last.volume / sma if sma else 0.0
        drop = oi_drop(oi_now, oi_peak) or 0.0
        ev = evaluate(bars, oi_now, oi_peak, p, self.tick_size)  # the E1 bar's own A/B/C/H, for the snapshot
        snap = self._snapshot(ev, last.ts_ms, close_ms, last.close, oi_now, oi_peak)
        sig = self._emit(
            "E1", drop >= p.oi_drop_strong, close_ms, last.close, drop, ratio, reason, start_ms, snap
        )
        self.oi_at_alert = oi_now
        self.state = State.DONE
        return sig

    def _v1b_e1(
        self, bars: Sequence[Bar], oi_now: float, oi_peak: float, close_ms: int, start_ms: int
    ) -> Signal | None:
        """S1.5.0 (D161(4)) e1_mode "v1b": the V1 B E1 on bars[-1] (module doc, "V1 B"). Fires on EVERY qualifying bar;
        the contract stays IDLE. The caller's oi_now / oi_peak are recorded in the snapshot as handed (T-102) and fill
        the dashboard evaluation; the rule reads the bars' own OI only."""
        p = self.params
        ev = evaluate(bars, oi_now, oi_peak, p, self.tick_size)  # the dashboard / snapshot view of this bar (D95(4))
        self.last_eval = ev
        hit = v1b_trigger(bars, p, self.tick_size)
        if hit is None:
            return None
        last = bars[-1]
        # No base: the signal's levels are the stop (consol_lo = sl) and the breakout reference (consol_hi)
        self.consol_lo, self.consol_hi = hit.stop, hit.breakout_ref
        self.consol_mid = (hit.stop + hit.breakout_ref) / 2.0
        # the unwind / quadrant inputs gates.build_context reads, with the signal bar standing in for the arming bar
        self._cycle = self._cycle_inputs(bars, hit.oi_peak_close)
        ratio = last.volume / hit.vol_sma
        reason = (
            f"E1 v1b: close {last.close:g} >= {p.v1b_breakout_mult:g} x 2-bar high {hit.breakout_ref:g}"
            f" ({last.close / hit.breakout_ref:.3f}x), oi -{hit.drop * 100:.1f}% off bar-close peak"
            f" {hit.oi_peak_close:.0f}, vol {ratio:.2f}x SMA{p.v1b_vol_sma_len}, turnover Rs {hit.turnover_rs:,.0f}"
        )
        snap = self._snapshot(ev, last.ts_ms, close_ms, last.close, oi_now, oi_peak)
        snap.update(
            turnover_rs=hit.turnover_rs,
            breakout_ref=hit.breakout_ref,
            oi_peak_close=hit.oi_peak_close,
            vol_sma=hit.vol_sma,
        )
        return self._emit(
            "E1", hit.drop >= p.oi_drop_strong, close_ms, last.close, hit.drop, ratio, reason, start_ms, snap
        )

    def force_armed(
        self,
        consol: tuple[float, float, float],
        armed_bar_ts: int,
        *,
        cycle: dict[str, Any] | None = None,
        oi_at_alert: float | None = None,
    ) -> None:
        """I8 (PLAN-D287-085, restart replay): live recorded an E2 whose bar the silent replay did not reproduce (a bar
        missing from the file, inputs that differ from live's). Put the engine where live's E2 left it: ARMED on the
        recorded base (consol_lo, consol_hi, consol_mid) and arming bar, with the cycle's unwind inputs from the
        recorded snapshot (D94(1): the E1 inherits them) and the E2's oi_now. Nothing is emitted."""
        self.state = State.ARMED
        self.consol_lo, self.consol_hi, self.consol_mid = consol
        self.armed_bar_ts = armed_bar_ts
        self._cycle = dict(cycle or {})
        if oi_at_alert is not None:
            self.oi_at_alert = oi_at_alert
        self.signals_today += 1

    def _clear_arm(self) -> None:
        self.state = State.IDLE
        self.consol_lo = self.consol_hi = self.consol_mid = None
        self.armed_bar_ts = None
        self._cycle = {}

    @staticmethod
    def _cycle_inputs(bars: Sequence[Bar], oi_peak: float) -> dict[str, Any]:
        """D94(1): what the unwind gate needs, fixed at the E2 and inherited by the same cycle's E1."""
        last = bars[-1]
        found = oi_peak_bar(bars, oi_peak)
        pk, exact = found if found is not None else (None, None)
        return {
            "cycle_e2_ts_ms": last.ts_ms,
            "cycle_e2_close": last.close,
            "oi_peak_bar_ts_ms": pk.ts_ms if pk is not None else None,
            "oi_peak_bar_close": pk.close if pk is not None else None,
            "oi_peak_bar_oi": pk.oi if pk is not None else None,
            "oi_peak_bar_exact": exact,
        }

    def _snapshot(
        self,
        ev: Evaluation | None,
        bar_ts_ms: int,
        signal_ts_ms: int,
        signal_price: float,
        oi_now: float,
        oi_peak: float,
    ) -> dict[str, Any]:
        snap: dict[str, Any] = ev.to_dict() if ev is not None else {}
        snap.update(
            bar_ts_ms=bar_ts_ms,
            signal_ts_ms=signal_ts_ms,
            signal_price=signal_price,
            oi_now=oi_now,
            oi_peak=oi_peak,
            tick_size=self.tick_size,
        )
        snap.update(self._cycle)
        return snap

    def _emit(
        self,
        kind: SignalKind,
        strong: bool,
        ts_ms: int,
        ltp: float,
        drop: float,
        vol_ratio: float,
        reason: str,
        start_ms: int,
        snapshot: dict[str, Any] | None = None,
    ) -> Signal:
        assert self.consol_lo is not None and self.consol_hi is not None and self.consol_mid is not None
        sig = Signal(
            kind=kind,
            strong=strong,
            ts_ms=ts_ms,
            security_id=self.security_id,
            ltp=ltp,
            oi_drop_pct=drop * 100.0,
            vol_ratio=vol_ratio,
            consol_lo=self.consol_lo,
            consol_hi=self.consol_hi,
            consol_mid=self.consol_mid,
            sl=self.consol_lo,
            reason=reason,
            latency_ms=self._clock() - start_ms,
            snapshot=snapshot,
        )
        self.last_signal = sig
        self.signals_today += 1
        if kind in edition.visible_kinds():  # CLIENT: an E2 never reaches the log file either (D86, PLAN-W12 risk 2)
            # T-105: a CLIENT E1 fires BEFORE the router's gates (zone / liquidity / direction / unwind); a gated one is
            # hidden (D87), so INFO here would name it on gammad.log, /api/logs and the WS log feed. DEBUG; the router
            # logs the same line at INFO once it has decided to show it. OWNER: INFO, unchanged.
            emit_log = log.debug if edition.is_client() else log.info
            emit_log("engine %s %s%s: %s", self.security_id, kind, " STRONG" if strong else "", reason)
        return sig

    def _arm_tolerance_suffix(self) -> str:
        """T-128 (4): an E1 whose arming signal needed condition A's tolerance says so, with a_reason's words. Read
        from this cycle's arming signal (last_signal, matched on the arming bar) BEFORE the E1 is emitted; "" for an
        untolerated arming and for an engine put ARMED by force_armed (I8 restart: no arming signal in memory)."""
        p = self.params
        arm = self.last_signal
        snap = arm.snapshot if arm is not None and arm.kind == "E2" else None
        if not snap or snap.get("bar_ts_ms") != self.armed_bar_ts:
            return ""
        drop = snap.get("oi_drop")
        if drop is None or drop >= p.oi_drop_alert or p.oi_drop_tolerance <= 0:
            return ""
        return f" | A tolerance {p.oi_drop_tolerance * 100:g} pt"


# ---------------------------------------------------------------- T-419 "Still Watching" (carried rows, both editions)
# Spec 4.8 (D471 Q3 / Q4, D476 (1), D480). The entry of a CARRY row = the SAME-BAR rule of the replayed truth, PORTED here
# line for line (never imported at runtime) from docs/research/2026-10-13-carry-peak/scripts/cp_engine.py, the cell
# IDEA2-8PCT-2-TODAY (cp_common.py:126-132: PEAK_CARRY, relax_x 2.0, relax_on A, UNWIND_TODAY) that T-417 Phase 2 graded
# (zx_common.py:78-83 'IDEA 1 . U-TODAY', short id I1-TODAY), times the ST-PREV side filter
# (docs/research/2026-10-14-zone-exits/scripts/zx_common.py:89-93, zx_levels.py:97-117: v1x_rules.v2_keep). NOTE: the
# T-419 brief placed both files under next/phase2/; they live in the carry-peak and zone-exits scripts folders.
# The listed V1 engine (ContractEngine above) is untouched: a CarryEngine is built ONLY for a CARRY row (engine_pool).

SW_RELAX_X = 2.0  # still_watching.base_atr_max (owner Q3): C's range up to 2.0 x ATR14 on the 8 % unwind bar
SW_PRIOR_TIE = 0.5  # cp_engine.py:78: a prior peak within half a contract of today's is the FIRST bar at it (a tie)
SW_ENTRY_CUTOFF_HM = "15:20"  # cp_select.py:43-45 (cp_common.ENTRY_CUT): a fire whose signal bar CLOSES at / after it
SW_BAN_OK, SW_BAN_UNKNOWN = "OK", "UNKNOWN"  # gammad/jobs/ban_list.py statuses (D480 (2): unknown = fail closed)
# why an E2 of a carried row is not an entry (sw_entry_block); the order is the check order
SW_BLOCK_BAN_UNKNOWN = "ban_unknown"
SW_BLOCK_BAN = "ban"
SW_BLOCK_ST_UNKNOWN = "st_unknown"  # zx_report.py:126 "ST-PREV regime unknown: counted, not traded"
SW_BLOCK_ST_SIDE = "st_side"
SW_BLOCK_CUTOFF = "cutoff"
SW_BLOCK_ENTERED = "entered"
SW_ST_MIN_BARS = 200  # = gammad/v2scan/regime.py MIN_BARS (D250: TradingView parity from bar 162)
SW_ST_GAP_MAX = 0.25  # = regime.py GAP_MAX (M14: an unadjusted split / bonus in a broker's daily series)


def sw_arms(ev: Evaluation, p: EngineParams) -> bool:
    """ContractEngine.on_bar_close's E2 arming test (:739-740) = cp_engine._arms (cp_engine.py:57-59)."""
    return bool(ev.a and (ev.b or not p.e2_requires_b) and ev.c and ev.h)


def sw_keep(option_type: str, regime: int | None) -> bool:
    """ST-PREV side filter = zx_levels.keep (zx_levels.py:99-103) = v1x_rules.v2_keep: CE needs +1 (green), PE needs -1
    (red), an unknown regime never keeps."""
    return regime is not None and regime == (1 if str(option_type).upper() == "CE" else -1)


def sw_regime_prev(bars: Sequence[Any], day: date, prev_session: date | None = None) -> tuple[int | None, str | None]:
    """ST-PREV for trade date `day`: (trend +1 / -1, None) or (None, reason). The daily Supertrend (ATR 10, x 2, hl2;
    gammad.core.supertrend, = zx_levels.regime_prev -> supertrend.regime_as_of) of the LAST daily bar dated BEFORE `day`
    (no look-ahead: bars dated `day` or later are dropped here, whatever the caller handed in). FAIL CLOSED like the V2
    regime (gammad/v2scan/regime.py:142-177, ported, not imported: that package is owner-only): fewer than 200 bars,
    a bar older than `prev_session`, a non-positive price, or a > 25 % close-to-close gap in the last 200 bars (a broker
    series without the split adjustment the research read) -> None. These checks can only refuse, never flip a side."""
    from gammad.core.supertrend import supertrend  # local: the listed engine path never needs it

    prior = [b for b in bars if ist_date(int(b.ts_ms)) < day]
    if len(prior) < SW_ST_MIN_BARS:
        return None, f"short_history: {len(prior)} daily bars < {SW_ST_MIN_BARS}"
    asof = ist_date(int(prior[-1].ts_ms))
    if prev_session is not None and asof < prev_session:
        return None, f"stale: last bar {asof.isoformat()}, previous session {prev_session.isoformat()}"
    for b in prior:
        px = (float(b.open), float(b.high), float(b.low), float(b.close))
        if not all(math.isfinite(x) and x > 0 for x in px):
            return None, f"bad_bars: non-positive / non-finite price on {ist_date(int(b.ts_ms)).isoformat()}"
    tail = prior[-SW_ST_MIN_BARS:]
    for a, b in pairwise(tail):
        if abs(float(b.close) / float(a.close) - 1.0) > SW_ST_GAP_MAX:
            return None, f"split_gap: close {a.close:g} -> {b.close:g} on {ist_date(int(b.ts_ms)).isoformat()}"
    try:
        last = supertrend(prior)[-1]
    except ValueError as exc:
        return None, f"bad_bars: {exc}"
    if last.value is None or not math.isfinite(last.value):
        return None, "bad_bars: Supertrend line still na"
    return int(last.trend), None


def sw_entry_block(
    option_type: str,
    signal_ts_ms: int,
    *,
    regime_prev: int | None,
    ban_status: str | None,
    banned: bool,
    entered: bool = False,
    cutoff_hm: str = SW_ENTRY_CUTOFF_HM,
) -> str | None:
    """Why the E2 of a carried row is NOT a Still Watching entry (None = it is one; the router's tier / R4 still apply).
    Checks, in order: the F&O ban list for the trade day (D480 (2): anything but status OK = UNKNOWN = no entry; a
    banned stock = no entry); ST-PREV (sw_keep; zx_entries filter_select runs it BEFORE select_book, so a refused fire
    never takes the stock-day's R4 slot); the 15:20 cut on the signal bar's CLOSE (cp_select.py:43-45: hhmm(signal_ms)
    >= ENTRY_CUT); one entry per option (cp_select.py:47-49)."""
    if ban_status != SW_BAN_OK:
        return SW_BLOCK_BAN_UNKNOWN
    if banned:
        return SW_BLOCK_BAN
    if regime_prev is None:
        return SW_BLOCK_ST_UNKNOWN
    if not sw_keep(option_type, regime_prev):
        return SW_BLOCK_ST_SIDE
    t = datetime.fromtimestamp(signal_ts_ms / 1000, IST)
    if t.hour * 60 + t.minute >= hhmm_minutes(cutoff_hm):
        return SW_BLOCK_CUTOFF
    if entered:
        return SW_BLOCK_ENTERED
    return None


class CarryEngine(ContractEngine):
    """The engine of a CARRY row (T-419 Still Watching; engine_pool builds it only for a contract primed as carried).
    Port of cp_engine.carry_engine_class (cp_engine.py:62-125) for the cell IDEA2-8PCT-2-TODAY:
      PEAK  condition A (and STRONG) read oi_peak_eff = max(oi_peak_prior, today's peak) when the prior peak is > 0 and
            not below today's by more than SW_PRIOR_TIE (cp_engine.py:75-79); `oi_peak_prior` = the max 5-min bar OI of
            the sessions BEFORE today (watchlist.oi_peak_prior, written by gammad/jobs/carry.py). None -> today's only.
      RELAX on an IDLE, warm, non-V1B bar that fails today's arming test while A holds (relax_on A, D439 (2)), C is
            re-tested at consol_range_atr = base_atr_max (2.0) for that ONE call; the params are restored in `finally`
            (cp_engine.py:80-103). Defined on consol_mode 'atr_fixed' only (another mode: no relax).
      U-TODAY  no override: _cycle_inputs -> oi_peak_bar(bars, oi_peak_eff) falls back to TODAY's highest-OI bar when the
            multi-day peak is a prior day's (cp_engine.py:20-22), so gates.unwind tests price-up vs TODAY's highest-OI bar.
    The entry is the E2 bar close itself (the SAME-BAR rule, D476 (1)); a carried contract fires no E1 (on_tick returns
    None: the research books E2 fires only, cp_select.py:38-40). Every E2 snapshot gains `sw` = the facts of the port
    (peak_used / peak_today / peak_from_prior / oi_peak_prior / relaxed / base_atr_max; arms dict, cp_engine.py:113-121).
    `carry_on` False (a carried contract re-listed by a later build, D161(6)) = ContractEngine exactly."""

    def __init__(
        self,
        security_id: int,
        params: EngineParams | None = None,
        *,
        clock: Callable[[], int] | None = None,
        tick_size: float | None = None,
        client_rules: bool | None = None,
        oi_peak_prior: float | None = None,
        base_atr_max: float = SW_RELAX_X,
    ) -> None:
        self.oi_peak_prior: float | None = oi_peak_prior
        self.base_atr_max = float(base_atr_max)
        self.carry_on = True
        super().__init__(security_id, params, clock=clock, tick_size=tick_size, client_rules=client_rules)

    def set_prior_peak(self, oi_peak_prior: float | None) -> None:
        self.oi_peak_prior = None if oi_peak_prior is None else float(oi_peak_prior)

    def sw_peak(self, bars: Sequence[Bar], oi_peak: float) -> tuple[float, bool]:
        """(oi_peak_eff, from_prior): cp_engine.py:73-79 with the prior peak known for today."""
        pp = self.oi_peak_prior
        if bars and pp is not None and pp > 0 and (not oi_peak or pp >= float(oi_peak) - SW_PRIOR_TIE):
            return max(float(pp), float(oi_peak or 0.0)), True
        return oi_peak, False

    def _sw_eval(self, bars: Sequence[Bar], oi_now: float, peak: float, p: EngineParams, tick: float | None,
                 und_bars: Sequence[Bar] | None) -> Evaluation:  # fmt: skip
        """The relax probe's evaluation of THIS bar with today's params (the window shadow overrides it)."""
        return evaluate(bars, oi_now, peak, p, tick, und_bars)

    def _sw_close(self, bars: Sequence[Bar], oi_now: float, peak: float, kw: dict[str, Any]) -> Signal | None:
        """The pinned bar close with the effective peak (the window shadow overrides it)."""
        return super().on_bar_close(bars, oi_now, peak, **kw)

    def on_bar_close(
        self,
        bars: Sequence[Bar],
        oi_now: float,
        oi_peak: float,
        *,
        recv_ms: int | None = None,
        tick_size: float | None = None,
        und_bars: Sequence[Bar] | None = None,
        arm_ctx: ArmContext | None = None,
    ) -> Signal | None:
        kw: dict[str, Any] = {"recv_ms": recv_ms, "tick_size": tick_size, "und_bars": und_bars, "arm_ctx": arm_ctx}
        if not self.carry_on:
            return super().on_bar_close(bars, oi_now, oi_peak, **kw)
        p = self.params
        peak, from_prior = self.sw_peak(bars, oi_peak)
        relaxed = False
        px = p
        if (self.state is State.IDLE and p.e1_mode != "v1b" and len(bars) >= p.warmup_bars
                and p.consol_mode == "atr_fixed"):  # fmt: skip
            tick = tick_size if tick_size is not None and tick_size > 0 else self.tick_size  # on_bar_close's tick rule
            ev = self._sw_eval(bars, oi_now, peak, p, tick, und_bars)
            # D439 (2) relax_on A: the trigger = condition A itself (A does not read consol_range_atr)
            if not sw_arms(ev, p) and ev.a:
                px = replace(p, consol_range_atr=float(self.base_atr_max))
                relaxed = sw_arms(evaluate(bars, oi_now, peak, px, tick, und_bars), px)
        if relaxed:
            self.params = px
            try:
                sig = ContractEngine.on_bar_close(self, bars, oi_now, peak, **kw)
            finally:
                self.params = p
        else:
            sig = self._sw_close(bars, oi_now, peak, kw)
        if sig is None or str(sig.kind) != "E2":
            return sig
        if isinstance(sig.snapshot, dict):
            sig.snapshot["sw"] = {
                "peak_used": float(peak), "peak_today": float(oi_peak or 0.0), "peak_from_prior": from_prior,
                "oi_peak_prior": self.oi_peak_prior, "relaxed": relaxed, "base_atr_max": self.base_atr_max,
            }  # fmt: skip
        return sig

    def on_tick(
        self,
        tick: TickView,
        forming_bar: Bar,
        bars: Sequence[Bar],
        oi_now: float,
        oi_peak: float,
    ) -> Signal | None:
        if not self.carry_on:
            return super().on_tick(tick, forming_bar, bars, oi_now, oi_peak)
        return None  # the same-bar E2 is the entry (D476 (1)); no E1 breakout on a carried contract


# -- the OWNER live shadow CARRY-ANY3 (D474 / D476 (1)): "3 conditions now, the 4th on the same candle or in the last 2-3
# bars". PORT of docs/research/2026-10-15-entry-window/scripts/ew_engine.py (pick :65-91, _Rec / _Past :95-142, in_reach
# :153-156, WindowEngine._ew_decide :230-276) for the cell CARRY-ANY-3 (ew_common.py:151: rule ANY, k 3, over the carry
# scanner). The research swapped the module's `evaluate` for one parent call; here the forced Evaluation arms through
# _sw_arm_forced, a copy of ContractEngine.on_bar_close's IDLE arm block (:742-767). OWNER edition only, never a client
# row, never sent (gammad.app records it as a shadow scanner).
SW_ANY3_SCANNER = "CARRY-ANY3"
SW_ANY3_K = 3
_SW_BASE_FIELDS = ("consol_lo", "consol_hi", "range", "drift", "atr", "zero_vol_slots", "h_tick")


@dataclass(frozen=True)
class SwPick:
    """ew_engine.Pick: late = the check true only on an earlier bar (None = all three now); late_age = bars back it was
    true; base_age = bars back of the frozen base (0 = the 6 bars ending now)."""

    late: str | None
    late_age: int
    base_age: int


def sw_pick_any(k: int, now: Any, past: Sequence[Any]) -> SwPick | None:
    """ew_engine.pick for rule ANY: two of A / B / base true now and the third true now or on one of the last k bars
    (past[i - 1] = the bar i slots back, same day; None = no evaluated bar there)."""
    if len(past) > k:
        raise ValueError(f"sw_pick_any: {len(past)} past bars for k {k}")
    if now.a and now.b and now.base:
        return SwPick(None, 0, 0)
    if k == 0:
        return None
    missing = [n for n in ("a", "b", "base") if not getattr(now, n)]
    if len(missing) != 1:
        return None
    m = missing[0]
    for i, c in enumerate(past, 1):
        if c is not None and getattr(c, m):
            return SwPick(m, i, i if m == "base" else 0)
    return None


def sw_in_reach(now_ts: int, cand_ts: int, k: int, tf_ms: int) -> bool:
    """ew_engine.in_reach: a candidate bar counts only when it is of the SAME IST day and at most k slots before."""
    return ist_date(cand_ts) == ist_date(now_ts) and 0 < now_ts - cand_ts <= k * tf_ms


class _SwRec:
    """One evaluated bar (ew_engine._Rec): its checks + the arguments to re-evaluate it relaxed (lazily, once)."""

    __slots__ = ("_rel_ev", "a", "args", "b", "ev", "std", "ts")

    def __init__(self, ts: int, a: bool, b: bool, std: bool, ev: Evaluation, args: tuple[Any, ...]) -> None:
        self.ts, self.a, self.b, self.std, self.ev, self.args = ts, a, b, std, ev, args
        self._rel_ev: Evaluation | None = None

    def rel_ev(self, px: EngineParams) -> Evaluation:
        if self._rel_ev is None:
            bars, oi_now, peak, tick, und = self.args
            self._rel_ev = evaluate(bars, oi_now, peak, px, tick, und)
        return self._rel_ev


class _SwPast:
    """ew_engine._Past: a bar's checks for sw_pick_any; .base = std (1.0 x) or, when the ALERT bar's A holds (`gate`),
    C and H at the relaxed base, computed on first read."""

    __slots__ = ("_base", "gate", "px", "rec")

    def __init__(self, rec: _SwRec, gate: bool, px: EngineParams | None) -> None:
        self.rec, self.gate, self.px = rec, gate, px
        self._base: bool | None = None

    @property
    def a(self) -> bool:
        return self.rec.a

    @property
    def b(self) -> bool:
        return self.rec.b

    @property
    def base(self) -> bool:
        if self._base is None:
            rel = False
            if self.gate and self.px is not None:
                ev = self.rec.rel_ev(self.px)
                rel = bool(ev.c and ev.h)
            self._base = bool(self.rec.std or rel)
        return self._base

    def base_ev(self) -> tuple[Evaluation, bool]:
        if self.rec.std or self.px is None:
            return self.rec.ev, False
        return self.rec.rel_ev(self.px), True


class CarryAny3Engine(CarryEngine):
    """The OWNER shadow CARRY-ANY3 (module comment above): a CarryEngine whose IDLE bar may also arm on the ANY-3 window.
    Every E2 snapshot gains `sw_window` (rule, k, late, late_age, base_age, base_ts, base_relaxed, forced)."""

    def reset_day(self) -> None:
        super().reset_day()
        self._w_hist: dict[int, _SwRec] = {}
        self._w_px: tuple[EngineParams, EngineParams] | None = None
        self._w_last_ts: int | None = None
        self._w_forced: tuple[int, Evaluation, dict[str, Any]] | None = None

    def _w_relaxed(self, p: EngineParams) -> EngineParams:
        if self._w_px is None or self._w_px[0] is not p:
            self._w_px = (p, replace(p, consol_range_atr=float(self.base_atr_max)))
        return self._w_px[1]

    def _sw_eval(self, bars: Sequence[Bar], oi_now: float, peak: float, p: EngineParams, tick: float | None,
                 und_bars: Sequence[Bar] | None) -> Evaluation:  # fmt: skip
        """ew_engine._ew_decide (:230-276): the real evaluation, or the forced one when ANY-3 fires and today's rule
        does not. A bar older than the last evaluated one (out of order) is never windowed (the research raised)."""
        self._w_forced = None
        ts = int(bars[-1].ts_ms)
        ev = evaluate(bars, oi_now, peak, p, tick, und_bars)
        if self._w_last_ts is not None and ts < self._w_last_ts:
            log.warning("engine %s ANY-3 shadow: bar %s after %s (out of order); not windowed", self.security_id, ts,
                        self._w_last_ts)  # fmt: skip
            return ev
        self._w_last_ts = ts  # an EQUAL ts (a duplicate slot) is re-evaluated and replaces its record (ew :236-239)
        rec = _SwRec(ts, bool(ev.a), bool(ev.b or not p.e2_requires_b), bool(ev.c and ev.h), ev,
                     (bars, oi_now, peak, tick, und_bars))  # fmt: skip
        self._w_hist[ts] = rec
        px = self._w_relaxed(p)
        gate = bool(ev.a)  # relax_on A: the base may be relaxed only when A holds on the ALERT bar
        now = _SwPast(rec, gate, px)
        tf_ms = p.signal_tf_min * 60_000
        past: list[_SwPast | None] = []
        for i in range(1, SW_ANY3_K + 1):  # the last k CLOSED bars before this one, most recent first
            j = len(bars) - 1 - i
            if j < 0:
                break
            bt = int(bars[j].ts_ms)
            r = self._w_hist.get(bt)
            past.append(_SwPast(r, gate, px) if r is not None and sw_in_reach(ts, bt, SW_ANY3_K, tf_ms) else None)
        pk = sw_pick_any(SW_ANY3_K, now, past)
        if pk is None or pk.late is None:
            return ev  # no window alert, or the same-bar rule arms on its own: the CarryEngine path
        src = now if pk.base_age == 0 else past[pk.base_age - 1]
        assert src is not None
        base_ev, relaxed = src.base_ev()
        fields: dict[str, Any] = {f: getattr(base_ev, f) for f in _SW_BASE_FIELDS}
        fields.update(c=True, h=True)
        if pk.late == "a":
            fields["a"] = True
        if pk.late == "b":
            fields["b"] = True
        if ev.oi_drop is None or ev.vol_ratio is None or any(fields[f] is None for f in _SW_BASE_FIELDS[:5]):
            return ev  # the arm block needs these; such a bar never arms
        forced = replace(ev, **fields)
        self._w_forced = (ts, forced, {"rule": "ANY", "k": SW_ANY3_K, "late": pk.late, "late_age": pk.late_age,
                                       "base_age": pk.base_age, "base_ts": int(src.rec.ts), "base_relaxed": relaxed,
                                       "forced": True})  # fmt: skip
        return forced

    def on_bar_close(
        self,
        bars: Sequence[Bar],
        oi_now: float,
        oi_peak: float,
        *,
        recv_ms: int | None = None,
        tick_size: float | None = None,
        und_bars: Sequence[Bar] | None = None,
        arm_ctx: ArmContext | None = None,
    ) -> Signal | None:
        sig = super().on_bar_close(bars, oi_now, oi_peak, recv_ms=recv_ms, tick_size=tick_size, und_bars=und_bars,
                                   arm_ctx=arm_ctx)  # fmt: skip
        if sig is not None and str(sig.kind) == "E2" and isinstance(sig.snapshot, dict):
            # the same-bar rule armed on its own (ew_engine.no_window_record, :159-161)
            sig.snapshot.setdefault("sw_window", {"rule": "ANY", "k": SW_ANY3_K, "late": None, "late_age": 0,
                                                  "base_age": 0, "base_ts": int(bars[-1].ts_ms), "base_relaxed": None,
                                                  "forced": False})  # fmt: skip
        return sig

    def _sw_close(self, bars: Sequence[Bar], oi_now: float, peak: float, kw: dict[str, Any]) -> Signal | None:
        forced, self._w_forced = self._w_forced, None
        if forced is None or forced[0] != int(bars[-1].ts_ms) or self.state is not State.IDLE:
            return super()._sw_close(bars, oi_now, peak, kw)
        sig = self._sw_arm_forced(bars, oi_now, peak, forced[1], kw)
        if sig is not None and isinstance(sig.snapshot, dict):
            sig.snapshot["sw_window"] = forced[2]
        return sig

    def _sw_arm_forced(self, bars: Sequence[Bar], oi_now: float, oi_peak: float, ev: Evaluation,
                       kw: dict[str, Any]) -> Signal | None:  # fmt: skip
        """ContractEngine.on_bar_close (:698-767) on a warm IDLE bar whose evaluation is the forced `ev` (the research
        answered the pinned call's evaluate with it). arm_checks run when the params set any (V1's set none)."""
        tick_size = kw.get("tick_size")
        if tick_size is not None and tick_size > 0:
            self.tick_size = tick_size
        p = self.params
        self.warming = False
        last = bars[-1]
        close_ms = last.ts_ms + p.signal_tf_min * 60_000
        recv_ms = kw.get("recv_ms")
        start_ms = recv_ms if recv_ms is not None else close_ms
        self.last_eval = ev
        if not sw_arms(ev, p):
            return None
        arm_rec: dict[str, Any] = {}
        if p.arm_tests_on:
            arm_ok, arm_rec = arm_checks(bars, oi_peak, p, self.tick_size, kw.get("arm_ctx"))
            if not arm_ok:
                return None
        assert ev.consol_lo is not None and ev.consol_hi is not None
        assert ev.oi_drop is not None and ev.vol_ratio is not None and ev.atr is not None
        assert ev.range is not None and ev.drift is not None
        self.state = State.ARMED
        self.consol_lo, self.consol_hi = ev.consol_lo, ev.consol_hi
        self.consol_mid = (ev.consol_lo + ev.consol_hi) / 2.0
        self.oi_at_alert = oi_now
        self.armed_bar_ts = last.ts_ms
        self._cycle = {**self._cycle_inputs(bars, oi_peak), **arm_rec}
        reason = (
            a_reason(ev.oi_drop, p)
            + f" | B vol {ev.vol_ratio:.2f}x SMA{p.vol_sma_len}, {ev.b_count}/{p.vol_build_window} above"
            f" | C range {ev.range:.2f} vs ATR{p.consol_atr_len} {ev.atr:.2f}, drift {ev.drift:+.2f}"
        )
        snap = self._snapshot(ev, last.ts_ms, close_ms, last.close, oi_now, oi_peak)
        return self._emit("E2", ev.strong, close_ms, last.close, ev.oi_drop, ev.vol_ratio, reason, start_ms, snap)
