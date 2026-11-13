"""Settings: `.env` (secrets, via python-dotenv) + `config/strategy.yaml` (strategy parameters).

`settings()` returns one frozen dataclass tree, cached. `reload_settings()` drops the cache
(the token hot-reload calls it). For every key in ENV_KEYS (= `env_keys()`: the base keys + the broker
registry's keys, W12) a NON-EMPTY value in the repo `.env` outranks the process environment, which is only
the fallback (D54); each key's source is logged, never its value. `load_settings` also runs the T-062
Telegram chat-id guard for the running edition (`telegram_check`, `Settings.telegram`).
"""

from __future__ import annotations

import logging
import os
import re
import tempfile
import threading
import time
import urllib.parse
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field, replace
from datetime import datetime
from functools import lru_cache
from pathlib import Path
from types import MappingProxyType
from typing import Any
from zoneinfo import ZoneInfo

import yaml
from dotenv import dotenv_values
from watchdog.events import FileSystemEvent, FileSystemEventHandler
from watchdog.observers import Observer

from gammad import REPO_ROOT
from gammad import edition as _edition

IST =ZoneInfo("Asia/Kolkata")

_log = logging.getLogger("gammad.config")


def now_ist() -> datetime:
    """Timezone-aware current time in Asia/Kolkata."""
    return datetime.now(IST)


@dataclass(frozen=True)
class ZonesCfg:
    pivot_len: int = 10
    atr_len: int = 50
    zone_depth: float = 2.5
    guard_mult: float = 2.0
    zone_memory: int = 20
    break_on_wick: bool = False
    daily_bars: int = 2000
    tie_rule: str = "right_strict"


@dataclass(frozen=True)
class ProximityCfg:
    stock_band_pct: float = 2.5
    strike_band_pct: float = 2.5
    top_n_oi: int = 2
    # S1.3.0 (D85(3)) list-build liquidity floor: skip a strike whose chain premium < min_premium_ticks x its tick.
    # S1.5.0 (D157(1)) drops the premium minimum (0 = off; the key stays, research yamls of older specs pin 20).
    min_premium_ticks: float = 0.0
    # S1.5.0 (D157(1)) liquidity floor, replacing the premium one: a strike needs OI >= min_oi_lots x lot size AND a
    # previous-session traded value >= min_prev_turnover_rs rupees (unknown -> fail-open, D161(5)). 0 = that test off:
    # research yamls of older specs pin 0 so they reproduce exactly.
    min_oi_lots: float = 50.0
    min_prev_turnover_rs: float = 100000.0


@dataclass(frozen=True)
class ExpiryCfg:
    expiry_dte_threshold: int = 4  # = config/strategy.yaml expiry (D59; D554 0.8.9: 7 -> 4 trading days)


REFRESH_EVERY_MIN: tuple[int, ...] = (5, 10, 15, 30, 60)  # T-277: the refresh steps a schedule may use
REFRESH_AFTER_HM = "09:20"  # T-277: the first refresh mark comes after the 09:20 build ...
REFRESH_BEFORE_HM = "15:30"  # ... and the last one before the session closes
PREOPEN_BEFORE_HM = "09:15"  # T-277: the pre-open build runs before the session opens


@dataclass(frozen=True)
class RefreshCfg:
    """T-277 (S1.8.0): the list refresh marks `first`, first + every_min, ... up to and including `last` (IST).
    yaml `schedule.refresh: {first: "09:30", every_min: 15, last: "14:45"}`."""

    first: str = "09:30"
    every_min: int = 15
    last: str = "14:45"

    def __post_init__(self) -> None:
        for name in ("first", "last"):
            v = getattr(self, name)
            if not isinstance(v, str) or not _HHMM_RE.fullmatch(v):
                raise ValueError(f"schedule.refresh.{name} must be a quoted \"HH:MM\", got {v!r}")
        if isinstance(self.every_min, bool) or self.every_min not in REFRESH_EVERY_MIN:
            raise ValueError(f"schedule.refresh.every_min must be one of {list(REFRESH_EVERY_MIN)}, got {self.every_min!r}")
        if not self.first > REFRESH_AFTER_HM:
            raise ValueError(f"schedule.refresh.first {self.first} must be after the {REFRESH_AFTER_HM} build")
        if self.last < self.first:
            raise ValueError(f"schedule.refresh.last {self.last} is before schedule.refresh.first {self.first}")
        if not self.last < REFRESH_BEFORE_HM:
            raise ValueError(f"schedule.refresh.last {self.last} must be before the {REFRESH_BEFORE_HM} close")

    def marks(self) -> tuple[str, ...]:
        """Every refresh mark "HH:MM", in order."""
        h, m = (int(x) for x in self.first.split(":"))
        t, out = h * 60 + m, []
        while f"{t // 60:02d}:{t % 60:02d}" <= self.last:
            out.append(f"{t // 60:02d}:{t % 60:02d}")
            t += self.every_min
        return tuple(out)


@dataclass(frozen=True)
class ScheduleCfg:
    mark_levels: tuple[str, ...] = ("09:20", "16:00")
    # T-277: a build-only time that is also a refresh mark (the old 13:15) is run BY the refresh (no second job)
    build_watchlists: tuple[str, ...] = ("09:20", "13:15")
    token_poll_minutes: int = 10
    # S1.7.0 (T-267, PLAN-T267 §1 Config): the V2 Scanner's 60-min stock-bar sync (08:30) + the 16:05 top-up
    h60_sync: tuple[str, ...] = ("08:30", "16:05")
    # T-277 (S1.8.0, D268): the pre-open list build on the previous close ("" = off: the 09:20 build does it all),
    # the 15-minute refresh, and the refresh marks after which the rows added since the last list post go to the group
    preopen_build: str = "09:10"
    refresh: RefreshCfg = field(default_factory=RefreshCfg)
    group_additions: tuple[str, ...] = ("10:15", "11:15", "12:15", "13:15", "14:15")

    def __post_init__(self) -> None:
        if isinstance(self.refresh, Mapping):  # the yaml mapping (`_section` hands sections over as read)
            object.__setattr__(self, "refresh", _section(RefreshCfg, dict(self.refresh)))
        if not isinstance(self.refresh, RefreshCfg):
            raise ValueError(  # noqa: TRY004 -- a bad strategy.yaml value: ValueError like every other key here
                f"schedule.refresh must be a mapping {{first, every_min, last}}, got {self.refresh!r}"
            )
        v = self.preopen_build
        if v != "" and (not isinstance(v, str) or not _HHMM_RE.fullmatch(v) or not v < PREOPEN_BEFORE_HM):
            raise ValueError(f"schedule.preopen_build must be a quoted \"HH:MM\" before {PREOPEN_BEFORE_HM} (or \"\" = "
                             f"off), got {v!r}")  # fmt: skip
        adds = self.group_additions
        if isinstance(adds, str) or not isinstance(adds, tuple | list):
            raise ValueError(  # noqa: TRY004 -- a bad strategy.yaml value: ValueError like every other key here
                f"schedule.group_additions must be a list of \"HH:MM\" refresh marks, got {adds!r}"
            )
        marks = self.refresh.marks()
        bad = [a for a in adds if a not in marks]
        if bad:
            raise ValueError(f"schedule.group_additions {bad!r} must be refresh marks (schedule.refresh: "
                             f"{marks[0]}..{marks[-1]} every {self.refresh.every_min} min)")  # fmt: skip
        object.__setattr__(self, "group_additions", tuple(adds))


@dataclass(frozen=True)
class WatcherCfg:
    base_tf_min: int = 1
    signal_tf_min: int = 5
    display_tf_min: int = 15
    oi_drop_alert: float = 0.08
    oi_drop_strong: float = 0.12
    # D287 (1) / D288 Q1: the 4 % re-arm is REMOVED in both editions (after its E2 a contract cannot arm again that
    # day). RESEARCH-only knob: None (the default; the live yaml carries no key) = never re-arm; a research yaml that
    # sets it (config/research/*.yaml pin 0.04) reproduces the pre-S1.8.0 re-arm exactly.
    oi_rearm_extra: float | None = None
    # S1.4.0 (D116(4)): condition A holds at oi_drop_alert - oi_drop_tolerance (8 % - 1 pt = 7 %); STRONG and the
    # re-arm extra are NOT tolerated. Research yamls of older specs pin 0.0 so they reproduce exactly.
    oi_drop_tolerance: float = 0.01
    vol_sma_len: int = 20
    vol_build_min_of_last3: int = 2
    consol_bars: int = 6
    consol_atr_len: int = 14
    consol_range_atr: float = 1.0
    consol_drift_range_atr: float = 1.5
    shakeout_recover_bars: int = 3
    # S1.3.0 (D85(1)) test H, both holds: base range >= h_min_range_ticks x tick AND <= h_max_zero_vol_slots
    # zero-volume 5-min slots of the 6 (by clock; a missing bar = zero volume)
    h_min_range_ticks: float = 2.0
    h_max_zero_vol_slots: int = 1
    # The E1 trigger. "breakout" = S1.3.1 (E2 arms, E1 = breakout tick / owner shakeout), the only mode.
    e1_mode: str = "breakout"
    # RESEARCH 2026-09-27 (dynamic base): how condition C measures the base; "atr_fixed" = the spec rule (default).
    # The other modes are research-only (gammad/watch/engine.py EngineParams doc).
    consol_mode: str = "atr_fixed"
    consol_pctile: float = 30.0
    consol_pctile_min_n: int = 6
    consol_var_min: int = 4
    consol_var_max: int = 12
    consol_und_range_atr: float = 1.0
    consol_hybrid_min_drift_atr: float = 0.0
    e2_requires_b: bool = True  # research: False = B tested only on the E1 breakout tick

    def __post_init__(self) -> None:
        if self.e1_mode not in E1_MODES:
            raise ValueError(f"WatcherCfg: e1_mode must be one of {list(E1_MODES)}, got {self.e1_mode!r}")
        if self.consol_mode not in CONSOL_MODES:
            raise ValueError(
                f"WatcherCfg: consol_mode must be one of {list(CONSOL_MODES)}, got {self.consol_mode!r}"
            )


# S1.5.0 (D161(4)): "v1b" = the V1 B trigger (V1BCfg); its engine path is ContractEngine._v1b_e1 (gammad/watch/engine.py).
E1_MODES: tuple[str, ...] = ("breakout", "v1b")
CONSOL_MODES: tuple[str, ...] = ("atr_fixed", "percentile", "variable", "underlying", "hybrid")


@dataclass(frozen=True)
class AlertsCfg:
    group_rate_per_min: int = 20
    per_chat_rate_per_sec: int = 1
    token_warn_hours: tuple[int, ...] = (24, 2)
    # S1.4.0 (D114(1)): card warnings, never a cut-off. A signal at/after this IST time carries "Day is closing";
    # a contract with DTE <= expiry_warn_dte carries "Expires in N trading days" (gammad.alerts.templates.warnings).
    late_day_warn_ist: str = "14:30"
    expiry_warn_dte: int = 7


# S1.4.0 (D114(1)): ONE hold style -- every list is a swing hold, rules.on_expiry caps it at the contract's expiry.
HOLD_ALL_LISTS: tuple[str, ...] = ("BREAKDOWN-PE", "BREAKOUT-CE", "EXPIRY-PE", "EXPIRY-CE")


@dataclass(frozen=True)
class HoldCfg:
    """D58/D60/D61/D68: holding style per list. SWING = 1..swing_days day hold; the stock stop ARMS on a DAILY
    close through the zone edge and exits on a daily close back over it (never armed -> day-5 close);
    INTRADAY = same-day, stop = the 5-min base low (D8).

    S1.4.0 (D114(1)): the defaults put EVERY list on swing and `intraday_lists` is empty (the key and the intraday
    code path stay: the owner's V2 shadow scanner and older-spec backtests use S1.3.1's split). These defaults
    EQUAL config/strategy.yaml so a `HoldCfg()` fallback agrees with the live config."""

    swing_lists: tuple[str, ...] = HOLD_ALL_LISTS
    intraday_lists: tuple[str, ...] = ()
    swing_days: int = 5


@dataclass(frozen=True)
class ConsolOverrideCfg:
    """S1.2.0 (T-061/MARUTI D73, ruled D74): the condition-C thresholds for ONE hold style. None = the `watcher` base value."""

    consol_range_atr: float | None = None
    consol_drift_range_atr: float | None = None


@dataclass(frozen=True)
class HoldOverridesCfg:
    """S1.2.0 (owner ruling 2026-09-25, T-061/MARUTI D73, ruled D74): per-hold overrides of the condition-C thresholds,
    applied over `watcher` to the engine of every contract of that hold style (style = gammad.alerts.gates.hold_for
    of the contract's list_name; gammad.watch.engine_pool.HoldParams). strategy.yaml:
        hold_overrides: {intraday: {consol_range_atr: 2.5, consol_drift_range_atr: 3.75}}
    A missing block, or a missing style, = no override (the S1.1.0 behaviour). S1.4.0 (D116(1)) removed the block
    from config/strategy.yaml: V1 runs the base condition C on every list; the owner's V2 shadow scanner and the
    research yamls still carry S1.2.0's intraday override."""

    swing: ConsolOverrideCfg = field(default_factory=ConsolOverrideCfg)
    intraday: ConsolOverrideCfg = field(default_factory=ConsolOverrideCfg)


@dataclass(frozen=True)
class GatesCfg:
    """D63: GROUP alert gates in SHADOW mode (the signal is always recorded; only the group send is skipped).
    zone: gates.zone_ok(gap, limit), signed (D382; k 0 = abs(gap) <= zone_max_gap_pct); gap = gates.zone_gap_pct.
    liquidity: the liq_sma_bars closed 5-min bars ending at the signal bar average >= liq_min_avg_lots of
    volume AND hold <= liq_max_zero_vol_bars zero-volume bars."""

    enabled: bool = True
    zone_max_gap_pct: float = 1.0
    liq_sma_bars: int = 20
    liq_min_avg_lots: float = 5.0
    liq_max_zero_vol_bars: int = 5
    # S1.3.0 gates: direction (D85(2)) and unwind (D94(1), E2-A)
    direction_enabled: bool = True
    unwind_enabled: bool = True
    unwind_min_ticks: float = 1.0
    # S1.4.0 (D114(3)): True = DIRECTION and UNWIND run on EVERY list (the tier counts four gates everywhere);
    # False = INTRADAY-hold lists only (S1.3.0 / S1.3.1), so a research yaml of an older spec reproduces exactly.
    all_lists: bool = True
    # S1.5.0 (D161(3)): V1's own gate rules (a scanner's ScannerSpec.rules == "V1"; V2 keeps AND + the fixed limit).
    # direction: "any" = F1 OR F2 (D1), "all" = F1 AND F2 (S1.4.0). zone: limit = clamp(v1_zone_atr_k x atr_pct,
    # zone_max_gap_pct, v1_zone_max_pct); k 0 = the fixed zone_max_gap_pct. Research yamls pin "all" and 0.
    v1_direction_mode: str = "any"
    v1_zone_atr_k: float = 0.5
    v1_zone_max_pct: float = 2.5

    def __post_init__(self) -> None:
        if self.v1_direction_mode not in V1_DIRECTION_MODES:
            raise ValueError(
                f"GatesCfg: v1_direction_mode must be one of {list(V1_DIRECTION_MODES)}, got {self.v1_direction_mode!r}"
            )


V1_DIRECTION_MODES: tuple[str, ...] = ("any", "all")
_HHMM_RE = re.compile(r"([01]\d|2[0-3]):[0-5]\d")


@dataclass(frozen=True)
class V1BCfg:
    """S1.5.0 (D161(4)): the V1 B trigger, built AS MEASURED (docs/research/2026-09-29-journal/journal.py) and the
    tiers of V1 B / V1 PLUS (gammad.alerts.gates, 0.8.1 wave 3). yaml `v1b:`, `Settings.v1b`."""

    breakout_mult: float = 1.05  # close >= breakout_mult x max(high of the 2 previous bars)
    oi_drop_pct: float = 10.0  # drop from the day's BAR-CLOSE OI peak, percent
    vol_sma_len: int = 20  # volume > SMA(vol_sma_len), the current bar included (as measured)
    min_premium_ticks: float = 20.0  # G1 at signal time (as measured)
    last_bar_close: str = "15:20"  # IST; a bar closing AT OR AFTER this never fires (strict, as the journal measured)
    owner_min_premium_rs: float = 2.0  # V1 B ENTRY tier: close >= Rs 2
    owner_min_turnover_rs: float = 1000000.0  # V1 B ENTRY tier: bar turnover (close x volume) >= Rs 10 lakh
    plus_min_turnover_rs: float = 2500000.0  # V1 PLUS component: bar turnover >= Rs 25 lakh

    def __post_init__(self) -> None:
        # YAML 1.1 reads an unquoted 15:20 as the base-60 int 920: refuse it rather than compare a number to a time
        if not isinstance(self.last_bar_close, str) or not _HHMM_RE.fullmatch(self.last_bar_close):
            raise ValueError(f"V1BCfg: last_bar_close must be a quoted \"HH:MM\", got {self.last_bar_close!r}")


@dataclass(frozen=True)
class CarryCfg:
    """S1.5.0 (D157(2), D159(4), D161(6)): carry-forward of listed contracts: OWNER edition on `enabled`, BOTH
    editions while still_watching.enabled (0.8.9 T-419, D480). yaml `carry_forward:`, `Settings.carry_fwd` -- never
    `carry` (app.py's `self.carry` is the OI carry). NO max_sessions (D159(4)): a carried row lives to expiry unless
    DEAD / SPENT / AWAY / DEEP-ITM / capacity retires it. Distances are in daily ATRs."""

    enabled: bool = True
    alive_min_ticks: float = 10.0  # DEAD below this many ticks of premium
    spent_ratio: float = 0.50  # SPENT when oi_prev < spent_ratio x the OI peak
    away_atr: float = 3.0  # AWAY: the strike's OTM distance from spot > this many daily ATRs (% of spot / atr_pct)
    deep_itm_atr: float = 2.0  # DEEP-ITM: the strike this many daily ATRs in the money (same distance, negative)
    near_atr: float = 1.5  # PRIMED: the strike within this many daily ATRs of spot (|OTM distance| / atr_pct)
    build_ratio: float = 1.00  # BUILD-UP when oi_prev >= build_ratio x oi_at_listing
    strong_ratio: float = 1.10  # strong build-up
    max_contracts: int = 120  # carried contracts per day, highest stage first (D157(2); T-282 (a): 60 -> 120)


SUPERTREND_SRCS: tuple[str, ...] = ("hl2", "hlc3", "close")  # = gammad.core.supertrend.SRC_KINDS
_ISO_DATE_RE = re.compile(r"\d{4}-\d{2}-\d{2}")


@dataclass(frozen=True)
class SupertrendCfg:
    """S1.7.0 (D246 addendum, D247): the stock's DAILY Supertrend side filter of the V2 Scanner (classic v4: ATR =
    RMA, bands carried on close[1]). The rule is frozen at hl2 (D247); hlc3 / close exist for research only."""

    period: int = 10
    mult: float = 2.0
    src: str = "hl2"

    def __post_init__(self) -> None:
        if self.src not in SUPERTREND_SRCS:
            raise ValueError(f"SupertrendCfg: src must be one of {list(SUPERTREND_SRCS)}, got {self.src!r}")
        if not isinstance(self.period, int) or isinstance(self.period, bool) or self.period < 1:
            raise ValueError(f"SupertrendCfg: period must be a positive int, got {self.period!r}")
        if not float(self.mult) > 0:
            raise ValueError(f"SupertrendCfg: mult must be > 0, got {self.mult!r}")


@dataclass(frozen=True)
class V2ScannerCfg:
    """S1.7.0 (T-267, STRATEGY-SPEC 4.7, D248): the V2 Scanner (scanner id V2T). yaml `v2_scanner:`,
    `Settings.v2_scanner`. These defaults EQUAL config/strategy.yaml (a `V2ScannerCfg()` fallback agrees with the
    live config). Trigger = gammad.v2scan.trigger (a port of t263_study.find_trigger); exits = the 1h zones."""

    first_close: str = "09:35"  # the first trigger bar CLOSE (IST, inclusive)
    last_close: str = "15:10"  # the last trigger bar CLOSE (IST, inclusive)
    oi_drop_pct: float = 7.0  # (today's bar-close OI peak - the bar's OI) / peak >= this, percent
    oi_strong_pct: float = 12.0  # the STRONG tag (a tag, not a filter), percent
    vol_sma_len: int = 20  # SMA of volume, the bar included, warmed across the previous session
    vol_run: int = 3  # this many contiguous candles each with volume > its own SMA
    premium_jump: float = 1.04  # close >= the previous 5-min close x this
    supertrend: SupertrendCfg = field(default_factory=SupertrendCfg)
    h1_history_start: str = "2017-04-03"  # the 1h zone book's first 60-min bar (D249(2))
    stock_last_slot: str = "15:10"  # a 5-min STOCK bar opening after this never counts as a stop breach (A5)
    hold_days: int = 5  # carry <= this many trading days, the entry day = day 1

    def __post_init__(self) -> None:
        # YAML 1.1 reads an unquoted 15:10 as the base-60 int 910 and an unquoted 2017-04-03 as a date: refuse both
        for name in ("first_close", "last_close", "stock_last_slot"):
            v = getattr(self, name)
            if not isinstance(v, str) or not _HHMM_RE.fullmatch(v):
                raise ValueError(f"V2ScannerCfg: {name} must be a quoted \"HH:MM\", got {v!r}")
        if self.first_close > self.last_close:
            raise ValueError(f"V2ScannerCfg: first_close {self.first_close} is after last_close {self.last_close}")
        if not isinstance(self.h1_history_start, str) or not _ISO_DATE_RE.fullmatch(self.h1_history_start):
            raise ValueError(
                f"V2ScannerCfg: h1_history_start must be a quoted \"YYYY-MM-DD\", got {self.h1_history_start!r}"
            )
        for name in ("vol_sma_len", "vol_run", "hold_days"):
            v = getattr(self, name)
            if not isinstance(v, int) or isinstance(v, bool) or v < 1:
                raise ValueError(f"V2ScannerCfg: {name} must be a positive int, got {v!r}")
        if not 0 < float(self.oi_drop_pct) <= float(self.oi_strong_pct) < 100:
            raise ValueError(
                f"V2ScannerCfg: need 0 < oi_drop_pct <= oi_strong_pct < 100, got {self.oi_drop_pct!r} / "
                f"{self.oi_strong_pct!r}"
            )
        if not float(self.premium_jump) > 1.0:
            raise ValueError(f"V2ScannerCfg: premium_jump must be > 1, got {self.premium_jump!r}")
        if not isinstance(self.supertrend, SupertrendCfg):
            raise TypeError(f"V2ScannerCfg: supertrend must be a SupertrendCfg, got {self.supertrend!r}")


@dataclass(frozen=True)
class SwTrailCfg:
    """`still_watching.trail` (D472 X9A): chandelier = the stock extreme since the target touch -/+ k x ATR(atr_len) of
    the COMPLETED tf_min-minute 09:15-anchored stock bars (gammad/trade/atr15.py, gammad/trade/zone_exit.py)."""

    mode: str = "x9a"
    k: float = 3.0
    atr_len: int = 14
    tf_min: int = 15

    def __post_init__(self) -> None:
        if self.mode != "x9a":  # the one rule built and replayed (D472); any other mode is not implemented
            raise ValueError(f"SwTrailCfg: mode must be 'x9a', got {self.mode!r}")
        if isinstance(self.k, bool) or not isinstance(self.k, (int, float)) or not float(self.k) > 0:
            raise ValueError(f"SwTrailCfg: k must be > 0, got {self.k!r}")
        for name in ("atr_len", "tf_min"):
            v = getattr(self, name)
            if not isinstance(v, int) or isinstance(v, bool) or v < 1:
                raise ValueError(f"SwTrailCfg: {name} must be a positive int, got {v!r}")
        if self.tf_min % 5:
            raise ValueError(f"SwTrailCfg: tf_min must be a multiple of 5 (built from 5-min bars), got {self.tf_min!r}")


@dataclass(frozen=True)
class SwBanListCfg:
    """`still_watching.ban_list` (D480 (2)): NSE fo_secban_<D> fetched once a day; a failed / missing file = ban
    UNKNOWN = no Still Watching entry that day (fail closed)."""

    enabled: bool = True
    fetch_hm: str = "08:45"
    fail: str = "closed"

    def __post_init__(self) -> None:
        if not isinstance(self.enabled, bool):
            raise TypeError(f"SwBanListCfg: enabled must be true / false, got {self.enabled!r}")
        if not isinstance(self.fetch_hm, str) or not _HHMM_RE.fullmatch(self.fetch_hm):
            raise ValueError(f"SwBanListCfg: fetch_hm must be a quoted \"HH:MM\", got {self.fetch_hm!r}")
        if self.fail != "closed":  # D480 (2): fail closed is the ruling; fail open is not offered
            raise ValueError(f"SwBanListCfg: fail must be 'closed', got {self.fail!r}")


@dataclass(frozen=True)
class StillWatchingCfg:
    """0.8.9 (T-419; D471 / D472 / D476 (1) / D480): the "Still Watching" list (internal tag CARRY), both editions.
    yaml `still_watching:`, `Settings.still_watching`. These defaults EQUAL config/strategy.yaml (a
    `StillWatchingCfg()` fallback agrees with the live config); unknown keys at either level are refused."""

    enabled: bool = True
    stop: str = "s2_close"  # a 5-min stock CLOSE at / beyond the 1h zone NEAR edge (owner Q5)
    target: str = "daily_zone"  # first touch = trailing starts, not an exit (owner Q6)
    trail: SwTrailCfg = field(default_factory=SwTrailCfg)
    eve_exit_hm: str = "era_day_end"  # D555: the expiry eve exits at the era day-end clock
    dte_max: int = 4  # DTE <= dte_max retires the row (owner Q2; D557 (3) 0.8.9: 7 -> 4, the D554 roll)
    base_atr_max: float = 2.0  # the base up to this many x ATR (owner Q3)
    entry_cutoff_hm: str = "15:20"
    ban_list: SwBanListCfg = field(default_factory=SwBanListCfg)
    owner_any3_shadow: bool = True  # OWNER edition only (D476 (1))

    def __post_init__(self) -> None:
        for name in ("enabled", "owner_any3_shadow"):
            if not isinstance(getattr(self, name), bool):
                raise TypeError(f"StillWatchingCfg: {name} must be true / false, got {getattr(self, name)!r}")
        if self.stop != "s2_close":  # the built + replayed rule (D471 Q5); nothing else is implemented
            raise ValueError(f"StillWatchingCfg: stop must be 's2_close', got {self.stop!r}")
        if self.target != "daily_zone":
            raise ValueError(f"StillWatchingCfg: target must be 'daily_zone', got {self.target!r}")
        for name in ("eve_exit_hm", "entry_cutoff_hm"):
            v = getattr(self, name)
            if name == "eve_exit_hm" and v == "era_day_end":
                continue
            if not isinstance(v, str) or not _HHMM_RE.fullmatch(v):
                allowed = '\"era_day_end\" or a quoted \"HH:MM\"' if name == "eve_exit_hm" else 'a quoted \"HH:MM\"'
                raise ValueError(f"StillWatchingCfg: {name} must be {allowed}, got {v!r}")
        # no `cap` key here: the carried-row cap has ONE source, carry_forward.max_contracts (orchestrator, T-419)
        d = self.dte_max
        if not isinstance(d, int) or isinstance(d, bool) or d < 0:
            raise ValueError(f"StillWatchingCfg: dte_max must be an int >= 0, got {d!r}")
        b = self.base_atr_max
        if isinstance(b, bool) or not isinstance(b, (int, float)) or not float(b) > 0:
            raise ValueError(f"StillWatchingCfg: base_atr_max must be > 0, got {b!r}")
        if not isinstance(self.trail, SwTrailCfg) or not isinstance(self.ban_list, SwBanListCfg):
            raise TypeError("StillWatchingCfg: trail / ban_list must be built by still_watching_section")


def still_watching_section(raw: dict[str, Any] | None) -> StillWatchingCfg:
    """`still_watching` (0.8.9, T-419). Missing = the defaults. The nested `trail` / `ban_list` blocks and every other
    key go through _section, so an unknown key at either level fails loudly."""
    raw = dict(raw or {})
    for key, cls in (("trail", SwTrailCfg), ("ban_list", SwBanListCfg)):
        sub = raw.pop(key, None)
        if sub is not None and not isinstance(sub, dict):
            raise ValueError(f"StillWatchingCfg: {key} must be a mapping, got {sub!r}")
        if sub is not None:
            raw[key] = _section(cls, sub)
    return _section(StillWatchingCfg, raw)


def v2_scanner_section(raw: dict[str, Any] | None) -> V2ScannerCfg:
    """`v2_scanner` (S1.7.0). Missing = the defaults. The nested `supertrend` block and every other key go through
    _section, so an unknown key at either level fails loudly (a typo must not silently fall back to a default)."""
    raw = dict(raw or {})
    st = raw.pop("supertrend", None)
    if st is not None and not isinstance(st, dict):
        raise ValueError(f"V2ScannerCfg: supertrend must be a mapping, got {st!r}")
    if st is not None:
        raw["supertrend"] = _section(SupertrendCfg, st)
    return _section(V2ScannerCfg, raw)


@dataclass(frozen=True)
class EnvCfg:
    dhan_client_id: str = ""
    dhan_access_token: str = field(default="", repr=False)
    telegram_bot_token: str = field(default="", repr=False)
    telegram_group_id: str = ""
    telegram_personal_chat_id: str = ""
    # T-134(1), D119 G3: forum topic = message_thread_id on EVERY group send; blank = General (repr-hidden like the ids)
    telegram_topic_id: str = field(default="", repr=False)
    port: int = 8760
    tz: str = "Asia/Kolkata"
    # T-129 (PLAN-W16 13.3 B1, D119 G2-4, D133): GAMMA_LAN=1 opens the dashboard to this PC's private (RFC1918) address
    # too, ONLY when GAMMA_LAN_PASSWORD is set (gammad/api/lan.py); the password never reprs, never logs.
    lan: bool = False
    lan_password: str = field(default="", repr=False)
    # W12 (BROKER-ADAPTER-SPEC §6): the `BROKER` key as written ("" = absent; the registry applies the edition
    # rule) and EVERY resolved env_keys() value under the D54 rule, broker blocks included -- feed it to
    # `gammad.broker.registry.creds_from_env`. Secrets: never repr'd, never logged.
    broker: str = ""
    values: Mapping[str, str] = field(default_factory=dict, repr=False, compare=False)


# T-062 / D71 / CLIENT-PACKAGE-SPEC §3: the Telegram chat-id startup guard.
OWNER_GROUP_RE = re.compile(r"-100\d+")  # the D71 supergroup
CLIENT_GROUP_RE = re.compile(r"-\d+")  # a STANDARD (basic) group "-4987654321" or a supergroup "-100..."
# T-181 F9: a group id typed after a `#` (".env" reads the rest of the line as a comment, so the value is blank)
HASHED_ID_RE = re.compile(r"#\s*-?\d+")
HASH_COMMENT_HINT = "a # starts a comment in .env; remove it"
PERSONAL_RE = re.compile(r"\d+")


@dataclass(frozen=True)
class TelegramCheck:
    """The T-062 verdict on the configured chat ids (values are never stored here, only the verdict).

    group_ok      False -> no GROUP sends at all (the router refuses them when given this check).
    personal_ok   False -> no PERSONAL sends (a malformed id, or one equal to the group).
    personal_set  False -> no PERSONAL chat configured: TOKEN/SYSTEM go to the dashboard only (client; in the
                  owner edition an empty personal id is also a problem, so personal_ok is False there).
    problems      one plain line per failed rule, for the ERROR log and the dashboard banner
                  (`/api/status.telegram` = `to_dict()`).
    checked       False only for the default instance (a Settings built without load_settings)."""

    edition: str = "owner"
    group_ok: bool = True
    personal_ok: bool = True
    personal_set: bool = True
    problems: tuple[str, ...] = ()
    checked: bool = False

    @property
    def ok(self) -> bool:
        return not self.problems

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "edition": self.edition,
            "group_ok": self.group_ok,
            "personal_ok": self.personal_ok,
            "personal_configured": self.personal_set,
            "problems": list(self.problems),
        }


def telegram_check(group_id: str | int | None, personal_id: str | int | None, *, edition: str) -> TelegramCheck:
    """T-062 (CLIENT-PACKAGE-SPEC §3): validate the chat ids for `edition` ("owner" / "client").

    owner   group must match ^-100\\d+$ (D71 supergroup); personal required, ^\\d+$, != group.
    client  group must match ^-\\d+$ (basic or supergroup); personal optional (empty -> dashboard only);
            if set: ^\\d+$, != group.
    Any other edition string is treated as client (edition.py fails closed the same way)."""
    group = str(group_id or "").strip()
    personal = str(personal_id or "").strip()
    owner = edition == "owner"
    problems: list[str] = []
    group_re = OWNER_GROUP_RE if owner else CLIENT_GROUP_RE
    if not group:
        group_ok = False
        problems.append("TELEGRAM_GROUP_ID is not set: no group alerts will be sent")
    elif HASHED_ID_RE.fullmatch(group):  # T-181 F9: `TELEGRAM_GROUP_ID=#-4987654321` reads as a comment
        group_ok = False
        problems.append(f"TELEGRAM_GROUP_ID starts with #: {HASH_COMMENT_HINT} (no group alerts will be sent)")
    elif not group_re.fullmatch(group):
        group_ok = False
        want = "a supergroup id starting -100" if owner else "a group id (a group id starts with a minus sign)"
        problems.append(f"TELEGRAM_GROUP_ID is not {want}: no group alerts will be sent")
    else:
        group_ok = True
    personal_set = bool(personal)
    if not personal_set:
        personal_ok = False
        if owner:
            problems.append("TELEGRAM_PERSONAL_CHAT_ID is not set: token/system messages reach the dashboard only")
    elif not PERSONAL_RE.fullmatch(personal):
        personal_ok = False
        problems.append("TELEGRAM_PERSONAL_CHAT_ID must be a positive number (your own chat with the bot)")
    elif personal == group:
        personal_ok = False
        problems.append("TELEGRAM_PERSONAL_CHAT_ID equals the group id: token/system messages never go to the group")
    else:
        personal_ok = True
    return TelegramCheck(
        edition="owner" if owner else "client",
        group_ok=group_ok,
        personal_ok=personal_ok,
        personal_set=personal_set,
        problems=tuple(problems),
        checked=True,
    )


@dataclass(frozen=True)
class PathsCfg:
    root: Path
    data_dir: Path
    logs_dir: Path
    db_path: Path
    strategy_yaml: Path


# D241(2): Settings.spec_version of a CLIENT install whose strategy.yaml has no spec_version line (the client package's
# copy never has one); an internal value only -- the client API and Telegram never show spec_version.
CLIENT_SPEC_VERSION = "client"


@dataclass(frozen=True)
class Settings:
    spec_version: str
    zones: ZonesCfg
    proximity: ProximityCfg
    expiry: ExpiryCfg
    schedule: ScheduleCfg
    watcher: WatcherCfg
    alerts: AlertsCfg
    env: EnvCfg
    paths: PathsCfg
    hold: HoldCfg = field(default_factory=HoldCfg)
    gates: GatesCfg = field(default_factory=GatesCfg)
    hold_overrides: HoldOverridesCfg = field(default_factory=HoldOverridesCfg)
    telegram: TelegramCheck = field(default_factory=TelegramCheck)  # T-062 verdict (load_settings)
    v1b: V1BCfg = field(default_factory=V1BCfg)  # S1.5.0 (D161(4)), yaml `v1b:`
    carry_fwd: CarryCfg = field(default_factory=CarryCfg)  # S1.5.0 (D157(2)), yaml `carry_forward:`
    v2_scanner: V2ScannerCfg = field(default_factory=V2ScannerCfg)  # S1.7.0 (T-267), yaml `v2_scanner:`
    still_watching: StillWatchingCfg = field(default_factory=StillWatchingCfg)  # 0.8.9 (T-419), `still_watching:`


def _section(cls: type, raw: dict[str, Any] | None) -> Any:
    """Build a frozen section, refusing unknown keys (a typo must not silently fall back to a default)."""
    raw = dict(raw or {})
    known = set(cls.__dataclass_fields__)  # type: ignore[attr-defined]
    unknown = set(raw) - known
    if unknown:
        raise ValueError(f"{cls.__name__}: unknown keys in strategy.yaml: {sorted(unknown)}")
    for k, v in list(raw.items()):
        if isinstance(v, list):
            raw[k] = tuple(v)
    return cls(**raw)


def hold_overrides_section(raw: dict[str, Any] | None) -> HoldOverridesCfg:
    """`hold_overrides` (S1.2.0, T-061/MARUTI D73, ruled D74). Missing / empty = no override. An unknown hold style or param key fails
    loudly, exactly as _section: a typo must not silently put a hold style back on the base thresholds.
    Values must be positive numbers."""
    raw = dict(raw or {})
    styles = set(HoldOverridesCfg.__dataclass_fields__)
    unknown = set(raw) - styles
    if unknown:
        raise ValueError(f"HoldOverridesCfg: unknown hold styles in strategy.yaml: {sorted(unknown)}")
    parts: dict[str, ConsolOverrideCfg] = {}
    for style, block in raw.items():
        if block is not None and not isinstance(block, dict):
            raise ValueError(f"hold_overrides.{style} must be a mapping, got {type(block).__name__}")
        values = dict(block or {})
        for k, v in values.items():
            if v is None:
                continue
            if isinstance(v, bool) or not isinstance(v, int | float) or v <= 0:
                raise ValueError(f"hold_overrides.{style}.{k} must be a positive number, got {v!r}")
            values[k] = float(v)
        parts[style] = _section(ConsolOverrideCfg, values)
    return HoldOverridesCfg(**parts)


def _strip_inline_comment(value: str) -> str:
    # .env.example uses `KEY=   # comment` for blank values. python-dotenv 1.2.3 does NOT read that
    # as blank: it drops the pad and returns "# comment" (measured 2026-09-25), so a value that
    # STARTS with "#" is a comment. Text after whitespace-hash is also cut, defensively.
    value = value.strip()
    if value.startswith("#"):
        return ""
    if " #" in value:
        value = value.split(" #", 1)[0]
    return value.strip()


# The keys load_settings reads. D54: for each, a NON-EMPTY value in the repo .env wins; the process
# environment is only the fallback for a key the file omits or leaves blank. Measured 2026-09-25: a
# Windows User-scope TELEGRAM_CHAT_ID left by another project (a group this bot cannot see) beat the
# repo .env under load_dotenv(override=False), and every group send failed "400: chat not found".
#
# W12 (BROKER-ADAPTER-SPEC §6): the full list is BASE_ENV_KEYS + `gammad.broker.registry.all_env_keys()`
# (BROKER + every registered broker's block), built LAZILY by `env_keys()` -- the registry imports the broker
# entries, and gammad.broker.dhan imports this module, so an import-time call would be a cycle.
# `config.ENV_KEYS` still works (module __getattr__ below) and is that full list.
BASE_ENV_KEYS: tuple[str, ...] = (
    "DHAN_CLIENT_ID",
    "DHAN_ACCESS_TOKEN",
    "TELEGRAM_BOT_TOKEN",
    "TELEGRAM_GROUP_ID",
    "TELEGRAM_CHAT_ID",
    "TELEGRAM_PERSONAL_CHAT_ID",
    "TELEGRAM_TOPIC_ID",
    "GAMMA_PORT",
    "GAMMA_TZ",
    "GAMMA_LAN",  # T-129: 0/1, the LAN bind (gammad/api/lan.py); NOT in the client .env.example (L5), the guide has it
    "GAMMA_LAN_PASSWORD",  # T-129: the dashboard login for any remote request (LAN or a proxy, D133)
    "GAMMA_ACCOUNTS_JSON",  # R1.9: multi-account structured JSON configuration
    "GAMMA_HISTORY_PROVIDER",  # R1.9: decoupled historical data provider
    "GAMMA_HISTORY_BARS_LIMIT",  # R1.9: daily bars query limit
    "GAMMA_HISTORY_DAILY_BUDGET",  # R1.9: daily historical request budget
    "GAMMA_ENFORCE_ACLS",  # R1.9: storage security NTFS ACL enforcement
)
LAN_ON_VALUES = frozenset({"1", "true", "yes", "on"})
SOURCE_ENV_FILE = ".env"
SOURCE_OS_ENV = "os-env"
BROKER_KEY = "BROKER"


@lru_cache(maxsize=1)
def env_keys() -> tuple[str, ...]:
    """BASE_ENV_KEYS, then the registry's keys (BROKER first) not already listed. A registry that cannot be
    imported leaves the core keys (plus BROKER) and one ERROR line: the owner's Dhan keys are core, so the
    owner's service still starts."""
    keys = list(BASE_ENV_KEYS)
    try:
        from gammad.broker.registry import all_env_keys

        extra = all_env_keys()
    except Exception:  # noqa: BLE001 -- see docstring
        _log.exception("settings: broker registry unavailable; resolving the core env keys only")
        extra = (BROKER_KEY,)
    keys.extend(k for k in extra if k not in keys)
    return tuple(keys)


def __getattr__(name: str) -> Any:  # PEP 562: `config.ENV_KEYS` is computed on first use
    if name == "ENV_KEYS":
        return env_keys()
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def _resolve_env(env_file: Path) -> dict[str, str]:
    """Resolve every env_keys() key: non-empty .env value, else process environment, else "".

    The file is parsed with dotenv_values, never load_dotenv: os.environ is NOT mutated, so nothing
    this call reads can later masquerade as an OS value. Logs one INFO line of `KEY<-source` for the
    keys that resolved non-empty and a WARNING per key set to DIFFERENT values in both places --
    key names and sources only, never a value (they are secrets).
    """
    from_file = _read_env_file(env_file)  # a missing file parses as {}
    values: dict[str, str] = {}
    sources: list[str] = []
    for key in env_keys():
        file_val = from_file.get(key, "")
        os_val = _strip_inline_comment(os.environ.get(key, ""))
        if file_val:
            values[key] = file_val
            sources.append(f"{key}<-{SOURCE_ENV_FILE}")
            if os_val and os_val != file_val:
                _log.warning(
                    "settings: %s differs between .env and the process environment; using the .env value (D54)",
                    key,
                )
        elif os_val:
            values[key] = os_val
            sources.append(f"{key}<-{SOURCE_OS_ENV}")
        else:
            values[key] = ""
    _log.info("settings: %s", " ".join(sources) if sources else "no env keys set")
    return values


_REARM_IGNORED_LOGGED = False  # LOW-1: the client's "re-arm key ignored" warning is logged once per process


def _client_watcher(w: WatcherCfg, yaml_path: Path) -> WatcherCfg:
    """0.8.5 audit LOW-1 (D287(1)): the CLIENT edition never re-arms. A 0.8.4 client yaml carries
    `watcher.oi_rearm_extra: 0.04` (the updater keeps it as strategy.yaml.bak-0.8.4); restoring it must not bring the
    re-arm back, so the client forces the key to None (one WARNING per process). The owner edition reads it as
    written (research configs). The warning is shown on the client's Logs page, so it is NEUTRAL: no key name, no
    edition word, no decision number (tests/test_d297_client_guards.py pins the banned words)."""
    global _REARM_IGNORED_LOGGED
    if w.oi_rearm_extra is None or not _edition.is_client():
        return w
    if not _REARM_IGNORED_LOGGED:
        _REARM_IGNORED_LOGGED = True
        _log.warning("settings: an unused watcher setting in %s was ignored", yaml_path.name)
    return replace(w, oi_rearm_extra=None)


def load_settings(root: Path | None = None) -> Settings:
    root = root or REPO_ROOT
    env_vals = _resolve_env(root / ".env")
    yaml_path = root / "config" / "strategy.yaml"
    with yaml_path.open(encoding="utf-8") as fh:
        raw: dict[str, Any] = yaml.safe_load(fh) or {}
    spec_version = str(raw.get("spec_version") or "")
    if not spec_version:
        if not _edition.is_client():  # the owner's yaml always names its spec (tools/version.py keeps it in step)
            raise ValueError(f"{yaml_path}: spec_version missing")
        # D241(2): the CLIENT package's strategy.yaml carries no spec version (owner-only; the packager replaces the
        # line). The DB spec_version columns are NOT NULL and the client API strips the key, so a neutral value.
        spec_version = CLIENT_SPEC_VERSION
    port_raw = env_vals["GAMMA_PORT"] or "8760"
    env = EnvCfg(
        dhan_client_id=env_vals["DHAN_CLIENT_ID"],
        dhan_access_token=env_vals["DHAN_ACCESS_TOKEN"],
        telegram_bot_token=env_vals["TELEGRAM_BOT_TOKEN"],
        # TELEGRAM_CHAT_ID is accepted as an alias for the GROUP only (the owner's .env names the
        # supergroup that way); TELEGRAM_GROUP_ID wins when both are set. Each is resolved first.
        telegram_group_id=env_vals["TELEGRAM_GROUP_ID"] or env_vals["TELEGRAM_CHAT_ID"],
        # Personal has NO fallback: token/system messages must never reach the group.
        telegram_personal_chat_id=env_vals["TELEGRAM_PERSONAL_CHAT_ID"],
        telegram_topic_id=env_vals["TELEGRAM_TOPIC_ID"],
        port=int(port_raw),
        tz=env_vals["GAMMA_TZ"] or "Asia/Kolkata",
        lan=env_vals["GAMMA_LAN"].strip().lower() in LAN_ON_VALUES,
        lan_password=env_vals["GAMMA_LAN_PASSWORD"],
        broker=env_vals.get(BROKER_KEY, "").strip().lower(),
        values=MappingProxyType(dict(env_vals)),
    )
    # T-181 F9: a group id written after a `#` resolves blank (a comment); the verdict names the `#` instead of "not set"
    tg_check = telegram_check(env.telegram_group_id or _hashed_value(root / ".env", "TELEGRAM_GROUP_ID"),
                              env.telegram_personal_chat_id, edition=_edition.edition())  # fmt: skip
    for problem in tg_check.problems:  # T-062: ERROR + the dashboard banner (/api/status.telegram)
        _log.error("settings: telegram (%s edition): %s", tg_check.edition, problem)
    data_dir = root / "data"
    paths = PathsCfg(
        root=root,
        data_dir=data_dir,
        logs_dir=root / "logs",
        db_path=data_dir / "gamma.db",
        strategy_yaml=yaml_path,
    )
    return Settings(
        spec_version=spec_version,
        zones=_section(ZonesCfg, raw.get("zones")),
        proximity=_section(ProximityCfg, raw.get("proximity")),
        expiry=_section(ExpiryCfg, raw.get("expiry")),
        schedule=_section(ScheduleCfg, raw.get("schedule")),
        watcher=_client_watcher(_section(WatcherCfg, raw.get("watcher")), yaml_path),
        alerts=_section(AlertsCfg, raw.get("alerts")),
        env=env,
        paths=paths,
        hold=_section(HoldCfg, raw.get("hold")),
        gates=_section(GatesCfg, raw.get("gates")),
        hold_overrides=hold_overrides_section(raw.get("hold_overrides")),
        telegram=tg_check,
        v1b=_section(V1BCfg, raw.get("v1b")),
        carry_fwd=_section(CarryCfg, raw.get("carry_forward")),
        v2_scanner=v2_scanner_section(raw.get("v2_scanner")),
        still_watching=still_watching_section(raw.get("still_watching")),
    )


@lru_cache(maxsize=1)
def settings() -> Settings:
    return load_settings()


def reload_settings() -> Settings:
    settings.cache_clear()
    return settings()


# --------------------------------------------------------------------------------------------
# .env hot reload + in-place edit (token hot-swap). Values are secrets: log KEY NAMES only.
# --------------------------------------------------------------------------------------------


def mask(token: str | None) -> str:
    """First 4 + ellipsis + last 4. A token of 8 chars or fewer is fully hidden (it would leak whole)."""
    if not token:
        return ""
    if len(token) <= 8:
        return "\N{HORIZONTAL ELLIPSIS}"
    return f"{token[:4]}\N{HORIZONTAL ELLIPSIS}{token[-4:]}"


def safe_key_names(keys: Iterable[object]) -> str:
    """T-283: a loggable summary of .env key NAMES. Only env_keys() names are printed (sorted); every other key is
    COUNTED, never named: python-dotenv parses a bare pasted line (no `=`) as a KEY, and no name pattern tells a key
    from a token (a 32-char lowercase broker token matches `[A-Za-z_][A-Za-z0-9_]*`). `;` (never ` +`) separates
    the count: server.redact's `access_token[ :=]+<value>` pattern would eat a `+` after DHAN_ACCESS_TOKEN."""
    allowed = set(env_keys())
    distinct = {str(k) for k in keys}
    known = sorted(distinct & allowed)
    other = len(distinct - allowed)
    parts = [f"keys {', '.join(known)}"] if known else []
    if other:
        parts.append(f"{other} other key(s)")
    return "; ".join(parts) or "no keys"


def _read_env_file(path: Path) -> dict[str, str]:
    return {k: _strip_inline_comment(v or "") for k, v in dotenv_values(path).items()}


def _hashed_value(path: Path, key: str) -> str:
    """T-181 F9: the raw `key` value of the .env file when it is an id typed after a `#` ("#-4987654321", which
    _strip_inline_comment reads as a comment = blank), else "". Only for the verdict's sentence: never used as an id.
    A template-style `KEY=   # your group id` is not an id, so it stays blank here too."""
    try:
        raw = (dotenv_values(path).get(key) or "").strip()
    except Exception:  # noqa: BLE001 -- an unreadable .env is reported elsewhere; the verdict falls back to "not set"
        return ""
    return raw if HASHED_ID_RE.fullmatch(raw) else ""


def _env_line_pattern(key: str) -> re.Pattern[str]:
    # prefix (optional `export`, key, `=`), value (up to an inline ` #` comment), comment, line ending.
    # The pad after `=` is LAZY: in `KEY=     # note` (blank value, .env.example style) the spaces
    # must stay with the comment, or a greedy pad eats them and the note becomes the "value".
    return re.compile(
        r"^(?P<prefix>[ \t]*(?:export[ \t]+)?" + re.escape(key) + r"[ \t]*=)[ \t]*?"
        r"(?P<value>.*?)(?P<comment>[ \t]+#.*?)?(?P<eol>\r?\n)?$",
        re.DOTALL,
    )


def _format_env_value(value: str) -> str:
    if any(c in value for c in ("\n", "\r", "\x00")):
        raise ValueError("an .env value cannot contain a newline or NUL")
    if value == "" or re.fullmatch(r"[A-Za-z0-9_./:+=@-]+", value):
        return value
    if "'" not in value:
        return f"'{value}'"
    escaped = value.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def write_env_value(path: Path | str, key: str, value: str) -> None:
    """Set KEY=value in an .env file atomically (temp file in the same directory + os.replace).

    Every other line, comment, blank line, line ending and the key order are preserved byte for
    byte. Every existing assignment of KEY is rewritten (python-dotenv is last-wins, so leaving a
    duplicate would silently keep the old value); an inline `  # comment` on that line is kept.
    An absent key is appended. The file never exists half-written.
    """
    path = Path(path)
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key):
        raise ValueError(f"invalid .env key: {key!r}")
    rendered = _format_env_value(value)
    text = path.read_bytes().decode("utf-8") if path.exists() else ""
    pattern = _env_line_pattern(key)
    lines = text.splitlines(keepends=True)
    found = False
    out: list[str] = []
    for line in lines:
        m = pattern.match(line)
        if m is None:
            out.append(line)
            continue
        found = True
        out.append(f"{m['prefix']}{rendered}{m['comment'] or ''}{m['eol'] or ''}")
    if not found:
        eol = "\r\n" if "\r\n" in text else "\n"
        if out and not out[-1].endswith(("\n", "\r")):
            out[-1] += eol
        out.append(f"{key}={rendered}{eol}")
    data = "".join(out).encode("utf-8")

    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    tmp = Path(tmp_name)
    try:
        with os.fdopen(fd, "wb") as fh:
            fh.write(data)
            fh.flush()
            os.fsync(fh.fileno())
        for attempt in range(5):  # Windows: an editor / AV scanner can hold the target briefly
            try:
                os.replace(tmp, path)
                break
            except PermissionError:
                if attempt == 4:
                    raise
                time.sleep(0.1 * (attempt + 1))
    finally:
        tmp.unlink(missing_ok=True)
    _log.info(".env: wrote key %s (%s)", key, "updated" if found else "added")


def _norm_path(p: str | bytes | Path) -> str:
    return os.path.normcase(os.path.abspath(os.fsdecode(p)))


class _EnvEventHandler(FileSystemEventHandler):
    def __init__(self, watcher: EnvWatcher) -> None:
        super().__init__()
        self._watcher = watcher

    def on_any_event(self, event: FileSystemEvent) -> None:
        candidates = [event.src_path, getattr(event, "dest_path", "") or ""]
        if any(c and _norm_path(c) == self._watcher.norm_path for c in candidates):
            self._watcher._schedule()


class EnvWatcher:
    """Watch one .env file and call `on_change({key: new_value})` when values really change.

    The DIRECTORY is observed, not the file, because editors save by writing a temp file and
    renaming it over the original (the original inode disappears). Events are debounced
    (`debounce_s`, a burst of saves = one reload). `on_change` receives only the keys whose value
    changed (a removed key maps to ""); it runs on the watcher's timer thread. A missing or
    unreadable file keeps the previous values (an editor mid-save must not "remove" every key).
    """

    def __init__(
        self,
        path: Path | str,
        on_change: Callable[[dict[str, str]], None],
        debounce_s: float = 1.0,
    ) -> None:
        self.path = Path(path).resolve()
        self.norm_path = _norm_path(self.path)
        self.on_change = on_change
        self.debounce_s = debounce_s
        self._lock = threading.Lock()
        self._timer: threading.Timer | None = None
        self._observer: Any = None
        self._values: dict[str, str] = self._read() or {}

    def _read(self) -> dict[str, str] | None:
        try:
            if not self.path.is_file():
                return None
            return _read_env_file(self.path)
        except (OSError, UnicodeDecodeError) as exc:
            _log.warning(".env watcher: could not read %s (%s); keeping previous values", self.path.name, exc)
            return None

    def start(self) -> None:
        if self._observer is not None:
            return
        observer = Observer()
        observer.schedule(_EnvEventHandler(self), str(self.path.parent), recursive=False)
        observer.daemon = True
        observer.start()
        self._observer = observer
        _log.info(".env watcher: watching %s", self.path)

    def stop(self) -> None:
        with self._lock:
            if self._timer is not None:
                self._timer.cancel()
                self._timer = None
        observer, self._observer = self._observer, None
        if observer is not None:
            observer.stop()
            observer.join(timeout=5.0)

    def _schedule(self) -> None:
        with self._lock:
            if self._timer is not None:
                self._timer.cancel()
            self._timer = threading.Timer(self.debounce_s, self._reload)
            self._timer.daemon = True
            self._timer.start()

    def _reload(self) -> dict[str, str]:
        """Re-parse the file and fire `on_change` with the changed keys. Returns that diff."""
        new = self._read()
        if new is None:
            return {}
        with self._lock:
            old = self._values
            changed = {k: new.get(k, "") for k in sorted(set(old) | set(new)) if old.get(k) != new.get(k)}
            self._values = new
        if changed:
            _log.info(".env changed: %s", safe_key_names(changed))  # never a value; T-283: never an unknown key
            try:
                self.on_change(changed)
            except Exception:  # noqa: BLE001 -- a consumer bug must not kill the watcher thread
                _log.exception(".env watcher: on_change callback raised")
        return changed


# --------------------------------------------------------------------------------------------
# Historical data catalogue (D37): read-only DATA paths of the older projects, by key.
# --------------------------------------------------------------------------------------------

CATALOGUE_YAML = REPO_ROOT / "config" / "data_catalogue.yaml"
_CATALOGUE_KEYS = {"catalogue_version", "roots", "datasets", "excluded_derived", "sqlite_uri_suffix", "tiers"}
_PLACEHOLDER = re.compile(r"\{([A-Za-z_][A-Za-z0-9_]*)\}")


class CatalogueMissing(FileNotFoundError):
    """A catalogue dataset path does not exist. Never fall back to another source (D37)."""


@dataclass(frozen=True)
class Catalogue:
    """Resolved `config/data_catalogue.yaml`. Every path is absolute; nothing here is ever written to."""

    version: str
    roots: Mapping[str, Path]
    datasets: Mapping[str, Path]
    excluded_derived: tuple[Path, ...]
    sqlite_uri_suffix: str
    tiers: Mapping[str, Mapping[str, Any]]
    source: Path

    def path(self, key: str) -> Path:
        """The dataset path for `key` (not checked for existence). Unknown key -> KeyError."""
        try:
            return self.datasets[key]
        except KeyError:
            raise KeyError(f"data catalogue {self.source}: no dataset key {key!r} (known: {sorted(self.datasets)})") from None

    def require(self, key: str) -> Path:
        """The dataset path for `key`; raises CatalogueMissing naming the path when it does not exist."""
        p = self.path(key)
        if not p.exists():
            raise CatalogueMissing(f"data catalogue dataset {key!r} not found at {p} (catalogue {self.source})")
        return p

    def sqlite_uri(self, key: str) -> str:
        """`file:<path>?mode=ro` for `sqlite3.connect(uri, uri=True)`; the path must exist."""
        p = self.require(key)
        quoted = urllib.parse.quote(p.as_posix(), safe="/:")
        return f"file:{quoted}{self.sqlite_uri_suffix}"


def _resolve_placeholders(raw: str, roots: Mapping[str, str], where: str) -> str:
    def sub(m: re.Match[str]) -> str:
        name = m.group(1)
        if name not in roots:
            raise ValueError(f"data catalogue {where}: unknown root placeholder {{{name}}} (roots: {sorted(roots)})")
        return roots[name]

    out = _PLACEHOLDER.sub(sub, raw)
    if "{" in out or "}" in out:
        raise ValueError(f"data catalogue {where}: unresolved brace in {raw!r}")
    return out


def _catalogue_path(raw: str, base: Path) -> Path:
    p = Path(raw)
    return p if p.is_absolute() else (base / p).resolve()


def catalogue(path: Path | str = CATALOGUE_YAML) -> Catalogue:
    """Load and resolve the data catalogue. Unknown top-level keys, unknown placeholders and a dataset
    lying inside an excluded (derived) path all fail loudly. Existence is checked per key by `require`."""
    path = Path(path)
    with path.open(encoding="utf-8") as fh:
        raw: dict[str, Any] = yaml.safe_load(fh) or {}
    unknown = set(raw) - _CATALOGUE_KEYS
    if unknown:
        raise ValueError(f"data catalogue {path}: unknown keys {sorted(unknown)}")
    for req in ("catalogue_version", "roots", "datasets"):
        if not raw.get(req):
            raise ValueError(f"data catalogue {path}: {req} missing")
    base = path.resolve().parent.parent  # config/ lives in the repo root
    roots_raw = {str(k): str(v) for k, v in raw["roots"].items()}
    for k, v in roots_raw.items():
        if _PLACEHOLDER.search(v):
            raise ValueError(f"data catalogue {path}: root {k!r} may not contain a placeholder")
    roots = {k: _catalogue_path(v, base) for k, v in roots_raw.items()}
    datasets = {
        str(k): _catalogue_path(_resolve_placeholders(str(v), roots_raw, f"{path} datasets.{k}"), base)
        for k, v in raw["datasets"].items()
    }
    excluded = tuple(
        _catalogue_path(_resolve_placeholders(str(v), roots_raw, f"{path} excluded_derived"), base)
        for v in raw.get("excluded_derived") or ()
    )
    for k, p in datasets.items():
        for ex in excluded:
            if p == ex or ex in p.parents:
                raise ValueError(f"data catalogue {path}: dataset {k!r} ({p}) lies inside excluded derived path {ex}")
    suffix = str(raw.get("sqlite_uri_suffix") or "?mode=ro")
    if "mode=ro" not in suffix:
        raise ValueError(f"data catalogue {path}: sqlite_uri_suffix {suffix!r} is not read-only (D37)")
    tiers = {
        str(k): MappingProxyType(dict(v or {})) for k, v in (raw.get("tiers") or {}).items()
    }
    return Catalogue(
        version=str(raw["catalogue_version"]),
        roots=MappingProxyType(roots),
        datasets=MappingProxyType(datasets),
        excluded_derived=excluded,
        sqlite_uri_suffix=suffix,
        tiers=MappingProxyType(tiers),
        source=path,
    )
