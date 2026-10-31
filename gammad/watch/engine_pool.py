"""One ContractEngine per watchlist contract, keyed by security_id.

S1.2.0 (owner ruling 2026-09-25, T-061/MARUTI D73, ruled D74): an engine's EngineParams follow its contract's HOLD STYLE.
HoldParams maps a watchlist list_name to params with gammad.alerts.gates.hold_for -- THE D58 mapping the signal
context uses -- and applies config `hold_overrides` for that style over the `watcher` base. A list in neither
HoldCfg.swing_lists nor HoldCfg.intraday_lists gets the base params and one warning. Live (gammad.app) and the
backtest (backtest.runner) both resolve through HoldParams.for_list.

S1.4.0 (PLAN-W15): every list is swing and the shipped yaml has no override, so V1's params are the base everywhere.
`build_scanners` returns the scanners a Runtime runs, one EnginePool each (ScannerSpec): V1 in both editions, and for
the owner V1 B (S1.5.0, D151(2), D161(4): e1_mode "v1b", Settings.v1b) and the shadow V2 from the registry below.
V1 PLUS is DERIVED: a scanner name with NO engine (the router writes its rows from V1 and V1 B signals); it is in
`all_scanner_names`, the one list the dashboard, cards and tests compare to, and never in `build_scanners`.
V3, V4a and V4b were retired from the registry in 0.8.1 (D149(1), D151(2), D161(8)); their history rows stay in the
store. The engine's V4 arming tests are kept (D161(8)) and `arm_context` (T-160), their per-bar input, is still handed
to every engine.

S1.7.0 (T-267, D248, D249): the V2 Scanner, id "V2T", label "V2 Scanner", is a second LIVE scanner (LIVE_SCANNERS),
OWNER edition only since 0.8.5 (D287(6), PLAN-D287 I7: `live_scanners(owner)`, `all_scanner_names(False)` = ("V1",)): its
EnginePool builds gammad.v2scan.trigger.V2TEngine through `engine_factory` and runs only the lists of
edition.v2_lists() (ScannerSpec.lists). The old shadow keeps id "V2" and is labelled "V2-S131" through SCANNER_LABELS,
the one id -> label map.

0.8.9 (T-419 "Still Watching", spec 4.8, D471 / D476 (1) / D480): a contract primed with `EnginePool.set_carry` (a
CARRY row, both editions) gets a gammad.watch.engine.CarryEngine (the multi-day OI peak, relax-C 2.0 x ATR on the 8 %
unwind bar, U-TODAY); every other contract keeps the ContractEngine exactly. `make_any3_pool` = the owner's CARRY-ANY3
live shadow (carried contracts only).
"""

from __future__ import annotations

import math
from collections.abc import Callable, Iterator, Mapping
from dataclasses import asdict, dataclass, replace
from datetime import date
from functools import lru_cache
from typing import Any

from gammad import edition as _edition
from gammad.alerts.gates import hold_for
from gammad.config import ConsolOverrideCfg, HoldCfg, HoldOverridesCfg, WatcherCfg
from gammad.core.calendar import default_calendar
from gammad.log import get_logger
from gammad.watch.engine import (
    SW_RELAX_X,
    CarryAny3Engine,
    CarryEngine,
    ContractEngine,
    EngineParams,
    ist_date,
)
from gammad.watch.types import ArmContext, Hold

log = get_logger(__name__)


def apply_override(base: EngineParams, override: ConsolOverrideCfg) -> EngineParams:
    """base with every non-None override value; the same object when nothing is overridden."""
    changes = {k: float(v) for k, v in asdict(override).items() if v is not None}
    return replace(base, **changes) if changes else base


class HoldParams:
    """EngineParams per hold style, looked up by a contract's watchlist list_name."""

    def __init__(
        self, base: EngineParams, hold: HoldCfg | None = None, overrides: HoldOverridesCfg | None = None
    ) -> None:
        self.base = base
        self.hold = hold or HoldCfg()
        ov = overrides or HoldOverridesCfg()
        self.by_hold: dict[Hold, EngineParams] = {
            "swing": apply_override(base, ov.swing),
            "intraday": apply_override(base, ov.intraday),
        }
        self._warned: set[str] = set()

    @classmethod
    def from_config(
        cls,
        watcher: WatcherCfg,
        hold: HoldCfg | None = None,
        overrides: HoldOverridesCfg | None = None,
        *,
        sma_inclusive: bool = True,
    ) -> HoldParams:
        return cls(EngineParams.from_config(watcher, sma_inclusive=sma_inclusive), hold, overrides)

    def style_of(self, list_name: str) -> Hold | None:
        """hold_for(list_name) for a configured list; None for a list in neither HoldCfg list."""
        if list_name in self.hold.swing_lists or list_name in self.hold.intraday_lists:
            return hold_for(list_name, self.hold)
        return None

    def for_list(self, list_name: str) -> EngineParams:
        style = self.style_of(list_name)
        if style is None:
            if list_name not in self._warned:
                self._warned.add(list_name)
                log.warning(
                    "engine params: list %r is in neither hold.swing_lists nor hold.intraday_lists; "
                    "using the base (watcher) params, no hold override",
                    list_name,
                )
            return self.base
        return self.by_hold[style]


class _InertEngine:
    """S1.7.0: what EnginePool.get_or_create hands back for a contract whose list the pool's scanner does not run
    (EnginePool.lists): never fires, never stored, so a caller that forgets `accepts()` cannot trade a list the
    scanner excludes (the client's V2T never sees an EXPIRY list, D248(7))."""

    state = "INERT"
    last_eval = None
    last_signal = None
    consol_lo = consol_hi = consol_mid = None

    def on_bar_close(self, *_a: Any, **_k: Any) -> None:
        return None

    def on_tick(self, *_a: Any, **_k: Any) -> None:
        return None

    def reset_day(self) -> None:
        return None


_INERT = _InertEngine()


class EnginePool:
    def __init__(
        self,
        params: Any = None,
        *,
        clock: Callable[[], int] | None = None,
        by_list: HoldParams | None = None,
        client_rules: bool | None = None,
        engine_factory: Callable[..., Any] | None = None,
        lists: tuple[str, ...] | None = None,
    ) -> None:
        """`engine_factory` (S1.7.0, T-267): builds a NEW engine as factory(security_id, params, *, clock,
        client_rules, listed_ms) -- e.g. gammad.v2scan.trigger.V2TEngine; None = ContractEngine (V1 / V1 B / V2,
        unchanged). `params` is then the factory's params object (V2TParams), not EngineParams. `lists` = the watchlist
        lists the scanner runs on (ScannerSpec.lists); None = every list (unchanged)."""
        self.params = params or (by_list.base if by_list is not None else EngineParams())
        self.by_list = by_list
        self._clock = clock
        # D110 / PLAN-W15 §1(8): passed to every ContractEngine; None = the edition decides at call time
        self.client_rules = client_rules
        self.engine_factory = engine_factory
        self.lists = tuple(lists) if lists is not None else None
        self._engines: dict[int, Any] = {}
        # 0.8.9 T-419 (Still Watching): security_id -> oi_peak_prior of the contracts primed as CARRIED (set_carry). A
        # primed contract gets a CarryEngine (`carry_cls`); every other contract the unchanged ContractEngine.
        # `carry_only` (the owner's CARRY-ANY3 shadow pool) runs ONLY primed contracts (any other gets the inert engine).
        self._carry: dict[int, float | None] = {}
        self.carry_cls: type[CarryEngine] = CarryEngine
        self.base_atr_max: float = SW_RELAX_X
        self.carry_only = False

    def params_for(self, list_name: str | None) -> Any:
        """The params a NEW engine for this list gets: by hold style when both are known, else the base."""
        if list_name is None or self.by_list is None:
            return self.params
        return self.by_list.for_list(list_name)

    def accepts(self, list_name: str | None) -> bool:
        """True when this pool's scanner runs on `list_name` (always, when the pool has no `lists`). An unknown list
        (None) is refused by a pool WITH lists: fail closed."""
        return self.lists is None or (list_name is not None and list_name in self.lists)

    def get_or_create(
        self, security_id: int, list_name: str | None = None, *, listed_ms: int | None = None
    ) -> Any:
        """The contract's engine, created on first use. A pool with `lists` returns a shared inert engine (never stored,
        never fires) for a list it does not run. `listed_ms` (A1, V2T only) = the row's listing bar open: passed to a
        new factory engine, or to an existing one's set_listed (the earliest listing wins); ContractEngine ignores it."""
        if not self.accepts(list_name) or (self.carry_only and security_id not in self._carry):
            return _INERT
        eng = self._engines.get(security_id)
        if eng is None:
            if self.engine_factory is None and security_id in self._carry:  # T-419: a carried contract
                eng = self.carry_cls(
                    security_id, self.params_for(list_name), clock=self._clock, client_rules=self.client_rules,
                    oi_peak_prior=self._carry[security_id], base_atr_max=self.base_atr_max,
                )  # fmt: skip
            elif self.engine_factory is None:
                eng = ContractEngine(
                    security_id, self.params_for(list_name), clock=self._clock, client_rules=self.client_rules
                )
            else:
                eng = self.engine_factory(
                    security_id, self.params_for(list_name), clock=self._clock, client_rules=self.client_rules,
                    listed_ms=listed_ms,
                )  # fmt: skip
            self._engines[security_id] = eng
        elif listed_ms is not None and hasattr(eng, "set_listed"):
            eng.set_listed(listed_ms)
        return eng

    def get(self, security_id: int) -> Any:
        return self._engines.get(security_id)

    def reset_all_day(self) -> None:
        for eng in self._engines.values():
            eng.reset_day()

    def describe(self) -> list[dict[str, Any]]:
        """State per contract plus the last A/B/C values, sorted by security_id (dashboard)."""
        return [self._engines[k].describe() for k in sorted(self._engines)]

    def __len__(self) -> int:
        return len(self._engines)

    def __contains__(self, security_id: object) -> bool:
        return security_id in self._engines

    def discard(self, security_id: int) -> bool:
        """Drop a contract's engine (T-063: a row the 09:20 build retired). True when one was removed."""
        self._carry.pop(security_id, None)
        return self._engines.pop(security_id, None) is not None

    # -- T-419 Still Watching: carried contracts

    def set_carry(self, security_id: int, oi_peak_prior: float | None) -> bool:
        """Prime a contract as CARRIED with its multi-day OI peak before today (watchlist.oi_peak_prior; None = today's
        peak only). Call BEFORE get_or_create: the contract's engine is then a CarryEngine. An engine that already
        exists is updated when it is one; a plain ContractEngine is left as it is (False, logged: it already ran bars
        under the V1 rule and is never swapped mid-day)."""
        prior = None if oi_peak_prior is None else float(oi_peak_prior)
        self._carry[security_id] = prior
        eng = self._engines.get(security_id)
        if eng is None:
            return True
        if isinstance(eng, CarryEngine):
            eng.set_prior_peak(prior)
            eng.carry_on = True
            return True
        log.warning("engine pool: sec %s is primed as carried but already runs the listed engine; left unchanged",
                    security_id)  # fmt: skip
        return False

    def clear_carry(self, security_id: int) -> None:
        """A carried contract RE-LISTED by a later build (D161(6)): its CarryEngine runs the listed rule from now on."""
        self._carry.pop(security_id, None)
        eng = self._engines.get(security_id)
        if isinstance(eng, CarryEngine):
            eng.carry_on = False

    def is_carry(self, security_id: int) -> bool:
        return security_id in self._carry

    def carry_ids(self) -> list[int]:
        return sorted(self._carry)

    def __iter__(self) -> Iterator[Any]:
        return iter(list(self._engines.values()))


# ---------------------------------------------------------------- scanners (S1.4.0, PLAN-W15 §1(6), §2, §5)

V1 = "V1"
V1_B = "V1 B"
V1_PLUS = "V1 PLUS"
V2 = "V2"  # the S1.3.1 shadow; its signals rows keep this id, shown as "V2-S131" (D248(2))
V2T = "V2T"  # S1.7.0 (T-267, D248(1), D249): the V2 Scanner, a LIVE scanner beside V1, owner edition only (D287(6))
# CONTRACTS.md section 4: a DERIVED name has no engine (the router writes its rows); never in build_scanners
DERIVED: tuple[str, ...] = (V1_PLUS,)
# S1.7.0: the scanners whose signals alert and trade (the union; per edition: live_scanners); every other built scanner is a shadow
LIVE_SCANNERS: tuple[str, ...] = (V1, V2T)
# The ONE id -> display label map (PLAN-T267 §1; dashboard/src/edition.ts mirrors it). An id not here shows as itself.
SCANNER_LABELS: dict[str, str] = {V1: "V1 Scanner", V2T: "V2 Scanner", V2: "V2-S131"}
# the gate rules a scanner's signals are tiered by (gammad.alerts.gates.build_context(rules=...), D161(3))
SCANNER_RULES: tuple[str, ...] = ("V1", "V2", "V1B", "V2T")
_OWNER_SCANNER_NAMES: tuple[str, ...] = (V1, V2T, V1_PLUS, V1_B, V2)
_CLIENT_SCANNER_NAMES: tuple[str, ...] = (V1,)  # 0.8.5 (D286(4), D287(6)): no V2 Scanner in the client edition


def all_scanner_names(owner: bool) -> tuple[str, ...]:
    """Every scanner name of an edition, engine-less DERIVED ones included, in display order: owner ("V1", "V2T",
    "V1 PLUS", "V1 B", "V2"); client ("V1",) (D159(1): V1 PLUS reaches clients only in a later 0.8.x; 0.8.5 D287(6):
    the V2 Scanner is stripped from the client edition, PLAN-D287 I7)."""
    return _OWNER_SCANNER_NAMES if owner else _CLIENT_SCANNER_NAMES


def live_scanners(owner: bool) -> tuple[str, ...]:
    """0.8.5 (PLAN-D287 I7): the LIVE scanners (alerting + trading) of an edition: ("V1", "V2T") for the owner, ("V1",)
    for the client. LIVE_SCANNERS stays the union (the ScannerSpec shadow check); the API reads this at CALL time."""
    return LIVE_SCANNERS if owner else (V1,)


def scanner_label(name: str) -> str:
    """The display label of a scanner id (SCANNER_LABELS); an unmapped id is its own label ("V1 PLUS", "V1 B")."""
    return SCANNER_LABELS.get(name, name)


@dataclass(frozen=True)
class ScannerSpec:
    """One live scanner: the params its EnginePool is built with. V1 and V2T are LIVE (LIVE_SCANNERS: their signals
    alert and trade); a `shadow` scanner is recorded and graded, never sent or traded.
    `rules` = the gate rules its signals are tiered by, one of SCANNER_RULES (D161(3)).
    S1.7.0: `lists` = the watchlist lists the scanner runs on (None = every list; V2T = edition.v2_lists()), and
    `engine_factory` = how its EnginePool builds an engine (None = ContractEngine; V2T = V2TEngine, `params` then a
    V2TParams and `by_list` None). Build the pool with `make_pool(spec)`."""

    name: str
    params: Any
    by_list: HoldParams | None
    client_rules: bool
    shadow: bool
    rules: str = "V1"
    lists: tuple[str, ...] | None = None
    engine_factory: Callable[..., Any] | None = None

    def __post_init__(self) -> None:
        if self.rules not in SCANNER_RULES:
            raise ValueError(f"ScannerSpec {self.name!r}: rules must be one of {list(SCANNER_RULES)}, got {self.rules!r}")
        if self.shadow == (self.name in LIVE_SCANNERS):
            raise ValueError(f"ScannerSpec {self.name!r}: shadow={self.shadow} contradicts LIVE_SCANNERS {LIVE_SCANNERS}")


def make_pool(spec: ScannerSpec, *, clock: Callable[[], int] | None = None) -> EnginePool:
    """The EnginePool of one registry scanner, factory and lists included (app.Runtime._make_pool's non-V1 branch)."""
    return EnginePool(
        spec.params, clock=clock, by_list=spec.by_list, client_rules=spec.client_rules,
        engine_factory=spec.engine_factory, lists=spec.lists,
    )  # fmt: skip


def make_any3_pool(v1: EnginePool, *, base_atr_max: float = SW_RELAX_X) -> EnginePool:
    """0.8.9 T-419 (D474 / D476 (1)): the OWNER's live shadow CARRY-ANY3 (engine.SW_ANY3_SCANNER): V1's params / hold
    map / rules, CarryAny3Engine engines, carried contracts ONLY (`carry_only`; prime each with set_carry). Never in
    build_scanners (no registry entry, no client package path): gammad.app builds it in the owner edition only."""
    pool = EnginePool(v1.params, clock=v1._clock, by_list=v1.by_list, client_rules=v1.client_rules)
    pool.carry_cls = CarryAny3Engine
    pool.carry_only = True
    pool.base_atr_max = float(base_atr_max)
    return pool


# The owner-only shadow scanner, hard-coded here (no yaml block: the shipped yaml needs no scanner key).
# V2 = S1.3.1 as shipped in 0.5.2: EXPIRY lists on the intraday hold with condition C at 2.5 / 3.75 x ATR14, no A
#      tolerance, owner rules (the shakeout trigger on). Kept equal to config/research/v2_relaxed_s131.yaml.
V2_HOLD = HoldCfg(swing_lists=("BREAKDOWN-PE", "BREAKOUT-CE"), intraday_lists=("EXPIRY-PE", "EXPIRY-CE"))
V2_OVERRIDES = HoldOverridesCfg(intraday=ConsolOverrideCfg(consol_range_atr=2.5, consol_drift_range_atr=3.75))
V2_OI_DROP_TOLERANCE = 0.0
# 0.8.5 (D287(1), D288 Q1): the live V1 lost its re-arm (watcher.oi_rearm_extra absent = None = off), but this shadow
# is S1.3.1 AS RELEASED, which re-armed at +4 % (config/research/v2_relaxed_s131.yaml `oi_rearm_extra: 0.04`): it keeps
# its own value instead of inheriting the live watcher's None (the research knob, owner-only, never the client).
# D296 M3: the V1 B shadow (`_v1b`) pins the same measured 0.04.
V2_OI_REARM_EXTRA = 0.04


def _v1(settings: Any) -> ScannerSpec:
    params = EngineParams.from_config(settings.watcher)
    by_list = HoldParams(params, settings.hold, getattr(settings, "hold_overrides", None))
    return ScannerSpec(V1, params, by_list, client_rules=True, shadow=False, rules="V1")


def _v1b(settings: Any) -> ScannerSpec:
    """S1.5.0 (D151(2), D161(4)): the V1 B trigger as measured (engine module doc, "V1 B"), knobs from Settings.v1b
    (yaml `v1b:`; absent -> the V1BCfg defaults). The same params on every list: no hold override reaches it.
    0.8.5 (D296 M3, D294(1)): V1 B keeps its MEASURED re-arm (V2_OI_REARM_EXTRA, 0.04) as `_v2` does, so the owner
    research lanes it feeds (the futures flags and the other owner lanes, D296 M3) stay as measured; D287(1) binds the
    live V1."""
    params = replace(EngineParams.from_config(settings.watcher, v1b=getattr(settings, "v1b", None)), e1_mode="v1b",
                     oi_rearm_extra=V2_OI_REARM_EXTRA)  # fmt: skip
    return ScannerSpec(V1_B, params, HoldParams(params, settings.hold), client_rules=False, shadow=True, rules="V1B")


def _v2(settings: Any) -> ScannerSpec:
    params = replace(EngineParams.from_config(settings.watcher), oi_drop_tolerance=V2_OI_DROP_TOLERANCE,
                     oi_rearm_extra=V2_OI_REARM_EXTRA)  # fmt: skip
    return ScannerSpec(
        V2, params, HoldParams(params, V2_HOLD, V2_OVERRIDES), client_rules=False, shadow=True, rules="V2"
    )


def _v2t(settings: Any, *, owner: bool) -> ScannerSpec:
    """S1.7.0 (T-267, STRATEGY-SPEC 4.7): the V2 Scanner. LIVE (D248(1)), OWNER edition only since 0.8.5 (D287(6):
    build_scanners never calls this for a client); knobs from Settings.v2_scanner (absent -> the V2ScannerCfg
    defaults); lists = edition.v2_lists(owner); engines = V2TEngine waiting for the M8 opening bars
    (trigger.live_engine_factory)."""
    from gammad.v2scan.trigger import V2TParams, live_engine_factory

    params = V2TParams.from_config(getattr(settings, "v2_scanner", None))
    return ScannerSpec(
        V2T, params, None, client_rules=True, shadow=False, rules="V2T", lists=_edition.v2_lists(owner),
        engine_factory=live_engine_factory,
    )  # fmt: skip


def build_scanners(settings: Any, *, owner: bool) -> tuple[ScannerSpec, ...]:
    """(V1,) for a client; (V1, V2T, V1 B, V2) for the owner. V1 is always first and runs client rules in BOTH
    editions (PLAN-W15 §1(8): the owner group gets exactly the client's entries). V2T (S1.7.0, D248(1)) is the second
    LIVE scanner (LIVE_SCANNERS), OWNER only since 0.8.5 (D287(6), PLAN-D287 I7: the client never builds it, so it never
    imports gammad.v2scan). V1 B (D151(2): owner only, never in the client package's registry) and
    V2 are shadows (recorded + graded, never sent to the group, never traded). V1 PLUS is DERIVED (no engine, never
    built here). V3 / V4a / V4b are retired (D149(1), D151(2), D161(8)). `settings` = gammad.config.Settings (or any
    object with .watcher / .hold / .hold_overrides, and .v1b / .v2_scanner when present)."""
    v1 = _v1(settings)
    if not owner:
        return (v1,)
    return (v1, _v2t(settings, owner=owner), _v1b(settings), _v2(settings))


def _dte_cached(day: date, expiry: str) -> int | None:
    try:
        exp = date.fromisoformat(expiry)
        if exp < day:
            return 0
        return default_calendar().trading_days_between(day, exp)
    except (ValueError, LookupError, OSError):  # a malformed expiry / a calendar year not in the holiday file
        return None


_dte = lru_cache(maxsize=512)(_dte_cached)


def arm_context(
    row: Mapping[str, Any] | None, bar_ts_ms: int, last_ltp: Mapping[int, float]
) -> ArmContext | None:
    """T-160: the ArmContext of a watchlist row at the close of the bar opened at `bar_ts_ms` (ArmContext doc).
    dte = the card warning's rule (spec 4.3: trading days after the bar's IST date up to and including the expiry,
    0 on or after expiry day); spot = last_ltp[the row's underlying_security_id]. None when there is no row. Never
    raises: an unreadable field is None (the V4 test that needs it then fails; V1 / V2 / V3 never read it)."""
    if row is None:
        return None

    def num(key: str) -> float | None:
        try:
            v = row.get(key)
            f = float(v) if v is not None else None
        except (TypeError, ValueError):
            return None
        return f if f is not None and not math.isnan(f) else None

    und = num("underlying_security_id")
    spot = last_ltp.get(int(und)) if und is not None else None
    expiry = row.get("expiry_date")
    return ArmContext(
        build_oi=num("oi"),
        expiry_date=str(expiry) if expiry is not None else None,
        dte=_dte(ist_date(bar_ts_ms), str(expiry)) if expiry is not None else None,
        spot=float(spot) if spot is not None else None,
        zone_edge=num("zone_edge"),
        option_type=str(row.get("option_type") or ""),
    )
