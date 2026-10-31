"""Pure closed-slot entry. Base/filter arithmetic ported from v1entry; OI from v1trade.

Array bars are (open_ms, open, high, low, close, volume, oi).
Only supplied closed prefixes are used; seeds warm indicators, never today's OI.
"""
from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Literal, Protocol

from gammad.watch.engine import atr_last
from gammad.watch.types import Bar as EngineBar

EPS = 1e-9
BAR_MS = 300_000
Bar = Sequence[float]
Side = Literal["PE", "CE"]
_IST = timezone(timedelta(hours=5, minutes=30))

# engine.py:161 and :608-610 gate even the variable mode on six available bars.
ENGINE_CONSOL_BARS = 6


def ebars(bars: Sequence[Bar]) -> list[EngineBar]:
    """Adapt array bars, supplying zero for absent volume and OI."""
    return [EngineBar(b[0], b[1], b[2], b[3], b[4], b[5] if len(b) > 5 else 0.0,
                      b[6] if len(b) > 6 else 0.0) for b in bars]


def _span(bars: Sequence[Bar]) -> float:
    return max(b[2] for b in bars) - min(b[3] for b in bars)


@dataclass(frozen=True, slots=True)
class BaseInput:
    side: Side
    option: Sequence[Bar]
    stock: Sequence[Bar]
    oi_peak_index: int | None


class BaseForm(Protocol):
    name: str

    def ok(self, b: BaseInput) -> bool: ...


@dataclass(frozen=True, slots=True)
class NoBase:
    name: str = field(default="C-none", init=False)

    def ok(self, b: BaseInput) -> bool:
        return True


@dataclass(frozen=True, slots=True)
class Adaptive:
    """Port of engine.py:556-563, including evaluate's six-bar guard."""
    var_min: int
    var_max: int
    range_atr: float
    atr_len: int
    name: str = field(init=False)

    def __post_init__(self) -> None:
        if not 1 <= self.var_min <= self.var_max:
            raise ValueError("require 1 <= var_min <= var_max")
        object.__setattr__(self, "name", f"C-adaptive-{self.var_min}-{self.var_max}x{self.range_atr:g}")

    def ok(self, b: BaseInput) -> bool:
        if len(b.option) < ENGINE_CONSOL_BARS:
            return False
        atr = atr_last(ebars(b.option), self.atr_len)
        if atr is None or atr <= 0:
            return False
        return any(_span(b.option[-n:]) <= self.range_atr * atr
                   for n in range(min(self.var_max, len(b.option)), self.var_min - 1, -1))


@dataclass(frozen=True, slots=True)
class VolBox:
    n: int
    k: float
    atr_len: int
    name: str = field(init=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "name", f"C-volbox-{self.n}x{self.k:g}")

    def ok(self, b: BaseInput) -> bool:
        if len(b.stock) < self.n:
            return False
        atr = atr_last(ebars(b.stock), self.atr_len)
        return atr is not None and atr > 0 and _span(b.stock[-self.n:]) <= self.k * atr + EPS


@dataclass(frozen=True, slots=True)
class OiPeakBase:
    min_bars: int
    k: float
    atr_len: int
    name: str = field(init=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "name", f"C-oipeak-{self.min_bars}x{self.k:g}")

    def ok(self, b: BaseInput) -> bool:
        if b.oi_peak_index is None:
            return False
        window = b.option[b.oi_peak_index:]
        if len(window) < self.min_bars:
            return False
        atr = atr_last(ebars(b.option), self.atr_len)
        return atr is not None and atr > 0 and _span(window) <= self.k * atr + EPS


@dataclass(frozen=True, slots=True)
class FilterCtx:
    cand: Candidate
    day: date
    entry_close_ms: int
    stock: Sequence[Bar]
    option: Sequence[Bar]
    nifty: Sequence[tuple[int, float]]


class Filter(Protocol):
    name: str

    def ok(self, ctx: FilterCtx) -> bool: ...


@dataclass(frozen=True, slots=True)
class VolumeBuild:
    """Port of engine.py:309-315 and :601-605 (condition B)."""
    series: Literal["option", "stock"]
    sma_len: int
    window: int
    min_of_window: int
    inclusive: bool
    name: str = field(init=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "name", f"VOL-{self.series}-{self.sma_len}-{self.min_of_window}of{self.window}")

    def ok(self, ctx: FilterCtx) -> bool:
        bars = ctx.option if self.series == "option" else ctx.stock
        window = bars[-self.sma_len:] if self.inclusive else bars[-self.sma_len - 1:-1]
        if len(window) < self.sma_len:
            return False
        sma = sum(b[5] for b in window) / self.sma_len
        if sma <= 0:
            return False
        return bars[-1][5] > sma and sum(b[5] > sma for b in bars[-self.window:]) >= self.min_of_window


@dataclass(frozen=True, slots=True)
class TimeOfDay:
    start: str
    end: str
    name: str = field(init=False)

    def __post_init__(self) -> None:
        if self.start > self.end:
            raise ValueError("start must be <= end")
        object.__setattr__(self, "name", f"TOD-{self.start}-{self.end}")

    def ok(self, ctx: FilterCtx) -> bool:
        hhmm = datetime.fromtimestamp(ctx.entry_close_ms / 1000, _IST).strftime("%H:%M")
        return self.start <= hhmm <= self.end


@dataclass(frozen=True, slots=True)
class NiftyTrend:
    sma_len: int
    name: str = field(init=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "name", "NIFTY-D20")

    def ok(self, ctx: FilterCtx) -> bool:
        if len(ctx.nifty) < self.sma_len:
            return False
        sma = sum(c for _, c in ctx.nifty[-self.sma_len:]) / self.sma_len
        last = ctx.nifty[-1][1]
        return last < sma - EPS if ctx.cand.side == "PE" else last > sma + EPS


@dataclass(frozen=True, slots=True)
class OiInARow:
    min_drop: float
    falling_bars: int
    seed: Literal["today", "with_prior"] = "today"
    name: str = field(init=False)

    def __post_init__(self):
        if not 0 < self.min_drop < 1 or not isinstance(self.falling_bars, int) or self.falling_bars < 1:
            raise ValueError("require 0 < min_drop < 1 and positive integer falling_bars")
        if self.seed not in ("today", "with_prior"):
            raise ValueError("unknown OI seed")
        suffix = "-prior" if self.seed == "with_prior" else ""
        object.__setattr__(self, "name", f"OI-{self.min_drop*100:g}-r{self.falling_bars}{suffix}")

    def holds(self, oi, i, prior_peak) -> tuple[bool, float | None]:
        peak = max(oi[:i + 1])
        if self.seed == "with_prior" and prior_peak is not None:
            peak = max(peak, prior_peak)
        if peak <= 0:
            return False, None
        drop = (peak - oi[i]) / peak
        n = self.falling_bars
        falling = i >= n and all(oi[i-k+1] < oi[i-k] - EPS for k in range(1, n+1))
        return drop >= self.min_drop - EPS and falling, drop


@dataclass(frozen=True)
class Candidate:
    symbol: str
    side: str
    level: float
    listed_ms: int
    kind: str
    item: str
    prior_oi_peak: float | None


@dataclass(frozen=True)
class Decision:
    entered: bool
    reason: str
    break_ts: int | None = None
    entry_ts: int | None = None
    entry_px: float | None = None
    stock_close: float | None = None
    oi_drop: float | None = None
    base: str | None = None
    filters: tuple[str, ...] = ()


@dataclass(frozen=True)
class EntryRule:
    oi: OiInARow
    unwind_window_bars: int
    rearm: str


def through(side, close, level):
    return close < level - EPS if side == "PE" else close > level + EPS


def base_for(name):
    bases = (NoBase(), Adaptive(4, 12, 1., 14), VolBox(6, 1., 14), OiPeakBase(6, 1., 14))
    return {b.name: b for b in bases}[name]


def filters_for(names):
    filters = (VolumeBuild("option", 20, 3, 2, True), VolumeBuild("stock", 20, 3, 2, True),
               TimeOfDay("09:30", "14:30"), NiftyTrend(20))
    by_name = {f.name: f for f in filters}
    return tuple(by_name[n] for n in names)


def rule_for(params, side, *, kind):
    if kind not in ("list", "carry"):
        raise ValueError("kind must be list or carry")
    pick = params.oi_unwind(side)
    fixed = params.fixed_rules()
    seed = fixed["oi_seed_listed"] if kind == "list" else "with_prior"
    return EntryRule(OiInARow(pick.min_drop, pick.falling_bars, seed),
                     fixed["unwind_window_bars"], fixed["entry_rearm"])


def bar_max_oi(bars, day):
    return max((b[6] for b in bars if datetime.fromtimestamp(b[0] / 1000, _IST).date() == day), default=0.)


def decide(cand, *, stock_today, stock_seed, option_today, option_seed, rule, base, filters, nifty, day):
    if not stock_today:
        return Decision(False, "no_break")
    stock = stock_today[-1]
    ts, close = stock[0], stock[4]
    if ts + BAR_MS < cand.listed_ms:
        return Decision(False, "before_listing")
    if not through(cand.side, close, cand.level):
        return Decision(False, "no_break")
    before = tuple(b for b in option_today if b[0] < ts)
    peak_index = None
    if before:
        peak = max(b[6] for b in before)
        peak_index = len(option_seed) + next(i for i, b in enumerate(before) if b[6] == peak)
    binput = BaseInput(cand.side, (*option_seed, *before),
                       (*stock_seed, *(b for b in stock_today if b[0] < ts)), peak_index)
    if not base.ok(binput):
        return Decision(False, f"base:{base.name}")
    option = tuple(b for b in option_today if b[0] <= ts)
    if not option or option[-1][0] != ts:
        return Decision(False, "no_option_bar")
    ok, drop = rule.oi.holds([b[6] for b in option], len(option) - 1, cand.prior_oi_peak)
    if not ok:
        return Decision(False, "no_unwind", oi_drop=drop)
    # NIFTY-D20 is explicitly D-1 daily, even if a caller supplies today's close.
    prior_nifty = tuple(p for p in nifty if datetime.fromtimestamp(p[0] / 1000, _IST).date() < day)
    ctx = FilterCtx(cand, day, ts + BAR_MS, (*stock_seed, *stock_today), (*option_seed, *option), prior_nifty)
    failed = next((f for f in filters if not f.ok(ctx)), None)
    if failed is not None:
        return Decision(False, f"filter:{failed.name}", oi_drop=drop)
    return Decision(True, "entered", ts, ts, option[-1][4], close, drop, base.name,
                    tuple(f.name for f in filters))
