"""The V1 rebuild picks: config/v1_rebuild.yaml -> one frozen tree (D519 (3); a Claude contract, read-only for CX-120 / 121).

Every value the V1 rebuild backtest chooses (D513 / D517) is the literal string PENDING-OWNER-PICK until the owner picks it;
the allowed choices carry the SAME names the backtest prints (docs/research/2026-10-17-v1-workflow/HARNESS.md, RUN-PLAN.md,
backtest/v1entry, the CX-110 / CX-111 / CX-112 grids), so a pick is a 1:1 copy of a carried version name. Owner rules
(the `fixed:` block) accept exactly one value each.

Fail closed: `V1Params.get(key)` (and every typed accessor) RAISES `PendingPick` naming the key when the value is pending,
so live code can never trade on a pick that does not exist. `side_ready(side)` / `carry_ready(side)` say whether a whole
decision path is resolved. A bad value raises ValueError naming the key and its allowed set; an unknown or missing key
raises ValueError too (a typo must never read as "pending").

OFF (D564): `enabled: false` makes every SIDE pick pending, clears private side slots, and raises `RebuildOff` on
side reads. `carry.entry_mode` is its own required pick, read in both states (D571 / D575): while off only `same_bar`
loads; `v1_break` is refused because it reads the side entry picks. Supplied sides and required fixed rules are still
validated. An absent `enabled` keeps the enabled contract; re-enable with side picks filled and `enabled: true`.

The default path is `<root>/config/v1_rebuild.yaml` with root = gammad.REPO_ROOT, exactly as gammad.config.load_settings
finds config/strategy.yaml (a checkout and a client install both keep config/ beside gammad/). Never reads .env.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Final

import yaml

from gammad import REPO_ROOT

PENDING: Final = "PENDING-OWNER-PICK"
YAML_REL: Final = Path("config") / "v1_rebuild.yaml"
SIDES: Final[tuple[str, ...]] = ("PE", "CE")

# --- the allowed choices (names as the backtest prints them) -----------------------------------------------------------
# HARNESS.md §2 CADENCES (CX-110 R5): H1 = hourly from 09:20; M15 = 09:10 then 09:20 and every 15 min to 15:20 (D268).
CADENCES: Final[dict[str, tuple[str, ...]]] = {
    "H1": ("09:20", "10:20", "11:20", "12:20", "13:20", "14:20", "15:20"),
    "M15": ("09:10", *(f"{m // 60:02d}:{m % 60:02d}" for m in range(9 * 60 + 20, 15 * 60 + 21, 15))),
}
BANDS: Final[tuple[float, ...]] = (2.5, 4.0, 6.0)  # HARNESS §2 BANDS; Band.fixed(x)
LIST_METHODS: Final[tuple[str, ...]] = ("BAND",)  # L3 methods join after their candidates page (RUN-PLAN §2)
OI_DROPS_PCT: Final[tuple[int, ...]] = (3, 5, 7, 10)  # F1
OI_FALLING_BARS: Final[tuple[int, ...]] = (1, 2)  # F1: bars IN A ROW (CX-111 OiInARow, D517)
OI_UNWINDS: Final[tuple[str, ...]] = tuple(f"OI-{d}-r{n}" for d in OI_DROPS_PCT for n in OI_FALLING_BARS)
BASES: Final[tuple[str, ...]] = ("C-none", "C-adaptive-4-12x1", "C-volbox-6x1", "C-oipeak-6x1")  # E3 (v1entry names)
FILTERS: Final[tuple[str, ...]] = ("VOL-option-20-2of3", "VOL-stock-20-2of3", "TOD-09:30-14:30", "NIFTY-D20")  # E4
MAX_FILTERS: Final = 2  # E4: one blend of the two best single filters (HARNESS §4.2 2d)
STOP_LEVELS: Final[tuple[str, ...]] = ("1h", "daily")  # S3
STOP_CANDLES: Final[tuple[str, ...]] = ("5m", "1h", "daily")  # S3
STOP_CAPS_PCT: Final[tuple[int | None, ...]] = (None, 20, 25, 30)  # S4; None = no cap
EXIT_FORMS: Final[tuple[str, ...]] = ("swing", "intraday")  # F4
NOPROG_HOURS: Final[tuple[int, ...]] = (3, 6, 12)  # X2
CHAND_KS: Final[tuple[int, ...]] = (2, 3)  # X3
TRAIL_STARTS_PCT: Final[tuple[int, ...]] = (0, 20)  # X4: at entry / at +20 %
TARGETS_PCT: Final[tuple[int, ...]] = (20, 40, 60)  # F3
CARRY_ENTRY_MODES: Final[tuple[str, ...]] = ("same_bar", "v1_break")  # spec section 7, D513 F2

FIXED: Final[dict[str, Any]] = {
    "strike_window_pct": 2.5,
    "strikes": ["HOI-1", "HOI-2"],
    "zones": "daily_pivots",
    "break_on": "close_5m",
    "oi_unwind_required": True,
    "oi_seed_listed": "today",
    "entry_rearm": "every_close_through",
    "unwind_window_bars": 0,
    "reentry_max_per_stock_side_day": 1,
    "reentry_after": "stop",
    "latest_entry_time": "15:10",
    "expiry_selection": "D59",
    "stop_pe_level": "resistance",
    "stop_ce_level": "support",
    "broken_edge_stop": False,
    "no_level_fallback": ["nearest_1h_zone", "cap_only"],
    "day5_exit": False,
    "expiry_exit": False,
    "settlement_close": "settled_at_expiry",
    "no_progress_gain_pct": 10,
    "intraday_close": "last_5m_bar",
}

# the per-side pick keys, relative to the side ("PE.list.cadence" is a full key)
SIDE_KEYS: Final[tuple[str, ...]] = ("list.cadence", "list.band_pct", "list.method", "entry.oi_unwind", "entry.base",
                                     "entry.filters", "stop", "exit")
ENTRY_KEYS: Final[tuple[str, ...]] = ("entry.oi_unwind", "entry.base", "entry.filters")
CARRY_KEY: Final = "carry.entry_mode"


class PendingPick(LookupError):  # the contract's name (D519 (3)): a read of an unpicked value
    """Live code read a value that is still PENDING-OWNER-PICK: fail closed (no entry on that path)."""

    def __init__(self, key: str) -> None:
        super().__init__(f"v1_rebuild {key} is {PENDING}: no V1 rebuild decision may read it (D519 (3))")
        self.key = key


class RebuildOff(PendingPick):
    """The V1 rebuild is explicitly OFF; existing pending handlers still fail closed."""

    def __init__(self, key: str) -> None:
        LookupError.__init__(self, f"v1_rebuild {key}: the V1 rebuild is OFF (enabled: false, D564); "
                             f"every pick reads as {PENDING}")
        self.key = key


# --- typed picks ---------------------------------------------------------------------------------------------------------
@dataclass(frozen=True)
class OiPick:
    min_drop: float  # fraction (0.05 = 5 %)
    falling_bars: int  # OI fell on EACH of the last n bars up to and including the entry bar
    name: str


@dataclass(frozen=True)
class StopPick:
    level: str  # "1h" | "daily"
    candle: str  # "5m" | "1h" | "daily"
    cap: float | None  # fraction of the entry premium (0.25), None = no cap
    name: str


@dataclass(frozen=True)
class ExitPick:
    family: str  # NOPROG | SWING1H | CHAND | LADDER | TARGET | HALF | BLEND
    param: float | None  # NOPROG hours, SWING1H w, CHAND k, TARGET fraction; None for LADDER / HALF / BLEND
    start: float  # trail start as a fraction of entry (0.0 = at entry, 0.20 = at +20 %)
    form: str  # "swing" | "intraday"
    rest: ExitPick | None  # HALF: the trail of the other half
    members: tuple[ExitPick, ...]  # BLEND: the two members (their own forms are ignored, CX-111 Q1)
    name: str


@dataclass(frozen=True)
class SidePicks:
    side: str
    cadence: str | None  # None = pending (read through V1Params.get / the typed accessors, which raise)
    band_pct: float | None
    method: str | None
    oi_unwind: OiPick | None
    base: str | None
    filters: tuple[str, ...] | None
    stop: StopPick | None
    exit: ExitPick | None


@dataclass(frozen=True)
class V1Params:
    """The ONLY read paths are `get` / the typed accessors / `side_ready` / `carry_ready` / `fixed_rules`: the parsed
    picks sit in PRIVATE fields (`_sides`, `_carry_entry_mode`) whose pending slots hold None, so a direct read would
    bypass the fail-closed raise (preflight F120-10)."""

    path: str
    _sides: Mapping[str, SidePicks]
    _carry_entry_mode: str | None
    _fixed: Mapping[str, Any]
    pending: frozenset[str] = field(default_factory=frozenset)
    enabled: bool = True

    def fixed_rules(self) -> dict[str, Any]:
        """A copy of the owner rules (`fixed:`; never pending)."""
        return dict(self._fixed)

    # -- fail-closed reads --
    def is_pending(self, key: str) -> bool:
        _check_key(key)
        return key in self.pending

    def pending_keys(self) -> tuple[str, ...]:
        return tuple(k for k in all_keys() if k in self.pending)

    def get(self, key: str) -> Any:
        """The parsed value of a full key ("PE.stop", "CE.entry.filters", "carry.entry_mode", "fixed.day5_exit").
        Raises PendingPick when it is pending, ValueError for an unknown key."""
        _check_key(key)
        if key in self.pending:
            raise RebuildOff(key) if not self.enabled and key != CARRY_KEY else PendingPick(key)
        if key == CARRY_KEY:
            return self._carry_entry_mode
        if key.startswith("fixed."):
            return self._fixed[key[len("fixed."):]]
        side, rel = key.split(".", 1)
        return getattr(self._sides[side], rel.rsplit(".", 1)[-1])

    def side_ready(self, side: str) -> bool:
        """Every pick of `side` is resolved (the V1 rebuild may list, enter, stop and exit on that side)."""
        if side not in SIDES:
            raise ValueError(f"side must be one of {SIDES} (got {side!r})")
        return not any(f"{side}.{k}" in self.pending for k in SIDE_KEYS)

    def carry_ready(self, side: str) -> bool:
        """A carried (Still Watching) entry on `side` may fire: the entry mode is picked and, in v1_break mode, the
        side's ENTRY picks are too (the break entry reads them)."""
        if side not in SIDES:
            raise ValueError(f"side must be one of {SIDES} (got {side!r})")
        if CARRY_KEY in self.pending:
            return False
        if self._carry_entry_mode == "same_bar":
            return True
        return not any(f"{side}.{k}" in self.pending for k in ENTRY_KEYS)

    def cadence(self, side: str) -> str:
        return self.get(f"{side}.list.cadence")

    def scan_marks(self, side: str) -> tuple[str, ...]:
        return CADENCES[self.cadence(side)]

    def band_pct(self, side: str) -> float:
        return self.get(f"{side}.list.band_pct")

    def oi_unwind(self, side: str) -> OiPick:
        return self.get(f"{side}.entry.oi_unwind")

    def base(self, side: str) -> str:
        return self.get(f"{side}.entry.base")

    def filters(self, side: str) -> tuple[str, ...]:
        return self.get(f"{side}.entry.filters")

    def stop(self, side: str) -> StopPick:
        return self.get(f"{side}.stop")

    def exit(self, side: str) -> ExitPick:
        return self.get(f"{side}.exit")

    def carry_mode(self) -> str:
        return self.get(CARRY_KEY)


def all_keys() -> tuple[str, ...]:
    """Every pick key (sides, then carry), in yaml order; `fixed.*` keys are not picks."""
    return (*(f"{s}.{k}" for s in SIDES for k in SIDE_KEYS), CARRY_KEY)


def _check_key(key: str) -> None:
    if key in all_keys() or (key.startswith("fixed.") and key[len("fixed."):] in FIXED):
        return
    raise ValueError(f"v1_rebuild: unknown key {key!r}")


# --- parsing -------------------------------------------------------------------------------------------------------------
def _bad(key: str, value: Any, allowed: Any) -> ValueError:
    return ValueError(f"v1_rebuild {key}: {value!r} is not one of the allowed choices {allowed}")


def _choice(key: str, value: Any, allowed: tuple[Any, ...]) -> Any:
    if isinstance(value, bool) or value not in allowed:
        raise _bad(key, value, allowed)
    return value


def parse_band(key: str, value: Any) -> float:
    if isinstance(value, bool) or not isinstance(value, int | float) or float(value) not in BANDS:
        raise _bad(key, value, BANDS)
    return float(value)


_OI = re.compile(r"OI-(\d+)-r(\d+)")


def parse_oi(key: str, value: Any) -> OiPick:
    if not isinstance(value, str) or value not in OI_UNWINDS or not (m := _OI.fullmatch(value)):
        raise _bad(key, value, OI_UNWINDS)
    return OiPick(int(m.group(1)) / 100.0, int(m.group(2)), value)


def parse_filters(key: str, value: Any) -> tuple[str, ...]:
    allowed = f"a list of 0..{MAX_FILTERS} distinct names of {FILTERS}"
    if not isinstance(value, list) or len(value) > MAX_FILTERS or len(set(map(str, value))) != len(value):
        raise _bad(key, value, allowed)
    for v in value:
        if v not in FILTERS:
            raise _bad(key, value, allowed)
    return tuple(value)


_STOP = re.compile(r"STOP-(1h|daily)-(5m|1h|daily)-cap(none|\d+)")
STOPS: Final[tuple[str, ...]] = tuple(f"STOP-{lv}-{c}-cap{'none' if cap is None else cap}"
                                      for lv in STOP_LEVELS for c in STOP_CANDLES for cap in STOP_CAPS_PCT)


def parse_stop(key: str, value: Any) -> StopPick:
    if not isinstance(value, str) or value not in STOPS or not (m := _STOP.fullmatch(value)):
        raise _bad(key, value, "STOP-{1h|daily}-{5m|1h|daily}-cap{none|20|25|30}")
    cap = None if m.group(3) == "none" else int(m.group(3)) / 100.0
    return StopPick(m.group(1), m.group(2), cap, value)


# exit names exactly as CX-111 backtest/v1trade/exits.py ExitSpec.name builds them
_EXIT_GRAMMAR = ("{NOPROG-3h|6h|12h | SWING1H-w2-s0|s20 | CHAND-k2|k3-s0|s20 | LADDER-20 | TARGET-20|40|60}-{swing|intraday}, "
                 "HALF20+{SWING1H-w2-s0|s20 | CHAND-k2|k3-s0|s20 | LADDER-20}-{form}, "
                 "BLEND(<member>-{form}|<member>-{form})-{form} with two members of different families (no HALF / BLEND)")


def _stem(stem: str, form: str) -> ExitPick | None:
    """A non-HALF, non-BLEND stem ("CHAND-k2-s20") + a form -> ExitPick, or None when the stem is not allowed."""
    if m := re.fullmatch(r"NOPROG-(\d+)h", stem):
        h = int(m.group(1))
        return ExitPick("NOPROG", float(h), 0.0, form, None, (), f"{stem}-{form}") if h in NOPROG_HOURS else None
    if m := re.fullmatch(r"SWING1H-w2-s(\d+)", stem):
        s = int(m.group(1))
        return ExitPick("SWING1H", 2.0, s / 100.0, form, None, (), f"{stem}-{form}") if s in TRAIL_STARTS_PCT else None
    if m := re.fullmatch(r"CHAND-k(\d+)-s(\d+)", stem):
        k, s = int(m.group(1)), int(m.group(2))
        if k in CHAND_KS and s in TRAIL_STARTS_PCT:
            return ExitPick("CHAND", float(k), s / 100.0, form, None, (), f"{stem}-{form}")
        return None
    if stem == "LADDER-20":
        return ExitPick("LADDER", None, 0.0, form, None, (), f"{stem}-{form}")
    if m := re.fullmatch(r"TARGET-(\d+)", stem):
        x = int(m.group(1))
        return ExitPick("TARGET", x / 100.0, 0.0, form, None, (), f"{stem}-{form}") if x in TARGETS_PCT else None
    return None


def _split_form(name: str) -> tuple[str, str] | None:
    stem, _, form = name.rpartition("-")
    return (stem, form) if stem and form in EXIT_FORMS else None


def parse_exit(key: str, value: Any) -> ExitPick:
    bad = _bad(key, value, _EXIT_GRAMMAR)
    if not isinstance(value, str) or not (sf := _split_form(value)):
        raise bad
    stem, form = sf
    if stem.startswith("HALF20+"):
        rest = _stem(stem[len("HALF20+"):], form)
        if rest is None or rest.family not in ("SWING1H", "CHAND", "LADDER"):
            raise bad
        return ExitPick("HALF", None, rest.start, form, rest, (), value)
    if m := re.fullmatch(r"BLEND\(([^|()]+)\|([^|()]+)\)", stem):
        members = []
        for part in m.groups():
            psf = _split_form(part)
            member = _stem(*psf) if psf else None
            if member is None:
                raise bad
            members.append(member)
        if members[0].family == members[1].family:
            raise bad  # HARNESS §4.2 step 4: a blend of the two best DISTINCT families
        return ExitPick("BLEND", None, 0.0, form, None, tuple(members), value)
    pick = _stem(stem, form)
    if pick is None:
        raise bad
    return pick


# --- loading -------------------------------------------------------------------------------------------------------------
def default_path(root: Path | None = None) -> Path:
    return (root or REPO_ROOT) / YAML_REL


def _exact_keys(where: str, raw: Any, keys: tuple[str, ...]) -> Mapping[str, Any]:
    if not isinstance(raw, Mapping):
        raise ValueError(  # noqa: TRY004 -- a bad yaml value: ValueError like gammad.config's yaml checks
            f"v1_rebuild {where}: must be a mapping (got {type(raw).__name__})")
    missing = [k for k in keys if k not in raw]
    unknown = [k for k in raw if k not in keys]
    if missing or unknown:
        raise ValueError(f"v1_rebuild {where}: missing keys {missing}, unknown keys {unknown}")
    return raw


def _side(side: str, raw: Any, pending: set[str]) -> SidePicks:
    r = _exact_keys(f"sides.{side}", raw, ("list", "entry", "stop", "exit"))
    lst = _exact_keys(f"sides.{side}.list", r["list"], ("cadence", "band_pct", "method"))
    ent = _exact_keys(f"sides.{side}.entry", r["entry"], ("oi_unwind", "base", "filters"))

    def val(rel: str, v: Any, parse: Any) -> Any:
        key = f"{side}.{rel}"
        if v == PENDING:
            pending.add(key)
            return None
        return parse(key, v)

    return SidePicks(
        side=side,
        cadence=val("list.cadence", lst["cadence"], lambda k, v: _choice(k, v, tuple(CADENCES))),
        band_pct=val("list.band_pct", lst["band_pct"], parse_band),
        method=val("list.method", lst["method"], lambda k, v: _choice(k, v, LIST_METHODS)),
        oi_unwind=val("entry.oi_unwind", ent["oi_unwind"], parse_oi),
        base=val("entry.base", ent["base"], lambda k, v: _choice(k, v, BASES)),
        filters=val("entry.filters", ent["filters"], parse_filters),
        stop=val("stop", r["stop"], parse_stop),
        exit=val("exit", r["exit"], parse_exit),
    )


def parse(raw: Any, path: str = "<memory>") -> V1Params:
    """A parsed yaml mapping -> V1Params (every value validated; see the module docstring)."""
    if not isinstance(raw, Mapping):
        _exact_keys("(top level)", raw, ("sides", "carry", "fixed"))
    enabled = raw.get("enabled", True)
    if not isinstance(enabled, bool):
        raise _bad("enabled", enabled, (True, False))
    keys = (("sides", "carry", "fixed") + (("enabled",) if "enabled" in raw else ()) if enabled else
            ("enabled", "carry", "fixed") + (("sides",) if "sides" in raw else ()))
    top = _exact_keys("(top level)", raw, keys)
    pending: set[str] = set()
    sides = {}
    if "sides" in top:
        sides_raw = _exact_keys("sides", top["sides"], SIDES)
        sides = {s: _side(s, sides_raw[s], pending) for s in SIDES}
    mode = None
    if "carry" in top:
        carry = _exact_keys("carry", top["carry"], ("entry_mode",))
        mode = carry["entry_mode"]
        if mode == PENDING:
            pending.add(CARRY_KEY)
            mode = None
        else:
            mode = _choice(CARRY_KEY, mode, CARRY_ENTRY_MODES)
    fixed = _exact_keys("fixed", top["fixed"], tuple(FIXED))
    for k, want in FIXED.items():
        got = fixed[k]
        if got != want or type(got) is not type(want):
            raise _bad(f"fixed.{k}", got, (want,))
    if not enabled:
        if mode == "v1_break":
            raise ValueError("v1_rebuild carry.entry_mode: 'v1_break' reads the V1 rebuild entry picks "
                             "(entry.oi_unwind / base / filters), which are OFF (enabled: false, D564); "
                             "only same_bar runs while the rebuild is off (D575)")
        pending = {k for k in all_keys() if k != CARRY_KEY} | (pending & {CARRY_KEY})
        sides = {s: SidePicks(s, None, None, None, None, None, None, None, None) for s in SIDES}
    return V1Params(path, sides, mode, dict(fixed), frozenset(pending), enabled)


def load(path: Path | str | None = None, *, root: Path | None = None) -> V1Params:
    """Read + validate config/v1_rebuild.yaml. A missing / unreadable file raises (OSError); the caller fails closed."""
    p = Path(path) if path is not None else default_path(root)
    with p.open(encoding="utf-8") as fh:
        raw = yaml.safe_load(fh)
    return parse(raw, str(p))


def all_pending(path: str = "<all pending>") -> V1Params:
    """Every pick pending (the fail-closed stand-in a caller uses when load() raised)."""
    picks = {"list": {"cadence": PENDING, "band_pct": PENDING, "method": PENDING},
             "entry": {"oi_unwind": PENDING, "base": PENDING, "filters": PENDING}, "stop": PENDING, "exit": PENDING}
    raw = {"sides": {s: picks for s in SIDES}, "carry": {"entry_mode": PENDING}, "fixed": dict(FIXED)}
    return parse(raw, path)


def is_pending(key: str, params: V1Params | None = None) -> bool:
    """`key` is still PENDING-OWNER-PICK in `params` (default: a fresh load() of the shipped file)."""
    return (params or load()).is_pending(key)


def pending_keys(params: V1Params | None = None) -> tuple[str, ...]:
    """Every pick key still pending in `params` (default: a fresh load() of the shipped file), in yaml order."""
    return (params or load()).pending_keys()


__all__ = [
    "BANDS", "BASES", "CADENCES", "CARRY_ENTRY_MODES", "CARRY_KEY", "ENTRY_KEYS", "EXIT_FORMS", "FILTERS", "FIXED",
    "LIST_METHODS", "MAX_FILTERS", "OI_UNWINDS", "PENDING", "SIDES", "SIDE_KEYS", "STOPS", "ExitPick", "OiPick",
    "PendingPick", "RebuildOff", "SidePicks", "StopPick", "V1Params", "all_keys", "all_pending", "default_path",
    "is_pending", "load",
    "parse", "parse_exit", "parse_oi", "parse_stop", "pending_keys",
]
