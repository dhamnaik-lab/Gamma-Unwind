"""CX-197: shared scanner records and injected views.
C-1: stdlib-only client interface, with no trading thresholds.
"""

from __future__ import annotations

from dataclasses import dataclass, field, fields
from datetime import date
from typing import Any, ClassVar, Protocol, Self, runtime_checkable

V1N = "V1N"
V2SQ = "V2SQ"
SCANNER_IDS = ("V1N", "V2SQ")
SCANNER_LABELS = {"V1N": "V1 REVAMPED", "V2SQ": "V2 SQUEEZE"}
CLIENT_SCANNERS = ("V1N",)
BREAKOUT_CE = "BREAKOUT-CE"
BREAKDOWN_PE = "BREAKDOWN-PE"
EXPIRY_CE = "EXPIRY-CE"
EXPIRY_PE = "EXPIRY-PE"
CARRY = "CARRY"
SQUEEZE = "SQUEEZE"
V1N_LISTS_OWNER = ("BREAKOUT-CE", "BREAKDOWN-PE", "EXPIRY-CE", "EXPIRY-PE", "CARRY")
V1N_LISTS_CLIENT = ("BREAKOUT-CE", "BREAKDOWN-PE", "CARRY")
V2SQ_LISTS = ("SQUEEZE",)
LISTS_BY_SCANNER = {"V1N": V1N_LISTS_OWNER, "V2SQ": V2SQ_LISTS}
SIDES = ("CE", "PE")
TAG_CARRY_STRIKE = "CARRY STRIKE"
TAG_RESULTS_TODAY = "RESULTS TODAY"
TAG_FRESH = "FRESH"
TAG_FUTURES = "FUTURES-CONFIRMED"
TAG_LEVEL_BREAK = "LEVEL-BREAK"
TAG_FLAT_IV = "FLAT-IV"
TAG_COVERING = "COVERING"
TAGS = (TAG_CARRY_STRIKE, TAG_RESULTS_TODAY, TAG_FRESH, TAG_FUTURES, TAG_LEVEL_BREAK, TAG_FLAT_IV, TAG_COVERING)
WATCHING, ARMED, ENTERED, BOOKED, EXITED = "watching", "armed", "entered", "booked", "exited"
ROW_STATES = (WATCHING, ARMED, ENTERED, BOOKED, EXITED)
FILL_NEXT_OPEN = "NEXT_OPEN"
FILL_BURST_TICK = "BURST_TICK"
FILL_RULES = (FILL_NEXT_OPEN, FILL_BURST_TICK)
STOP_ZONE = "ZONE"
STOP_PCT = "PCT"
STOP_BURST = "BURST"
STOP_KINDS = (STOP_ZONE, STOP_PCT, STOP_BURST)
EXIT_KINDS = ("TARGET", "STOP_MOVE", "STOP", "ZONE_STOP", "OI_RETURN", "TRAIL", "CARRY", "E1_EXIT", "DAY_END")
LEGS = ("T1", "T2", "T3", "T4", "RUNNER")
MAX_LEGS = {"V1N": 4, "V2SQ": 3}
RISK_BLOCKS = ("MAX_OPEN", "DAY_CAP")
POSITION_STATE_SCHEMA = "position-state/1"
BAN_OK = "OK"
BAN_UNKNOWN = "UNKNOWN"

__all__ = [
    "ARMED",
    "BAN_OK",
    "BAN_UNKNOWN",
    "BOOKED",
    "BREAKDOWN_PE",
    "BREAKOUT_CE",
    "CARRY",
    "CLIENT_SCANNERS",
    "ENTERED",
    "EXITED",
    "EXIT_KINDS",
    "EXPIRY_CE",
    "EXPIRY_PE",
    "FILL_BURST_TICK",
    "FILL_NEXT_OPEN",
    "FILL_RULES",
    "LEGS",
    "LISTS_BY_SCANNER",
    "MAX_LEGS",
    "POSITION_STATE_SCHEMA",
    "RISK_BLOCKS",
    "ROW_STATES",
    "SCANNER_IDS",
    "SCANNER_LABELS",
    "SIDES",
    "SQUEEZE",
    "STOP_BURST",
    "STOP_KINDS",
    "STOP_PCT",
    "STOP_ZONE",
    "TAGS",
    "TAG_CARRY_STRIKE",
    "TAG_COVERING",
    "TAG_FLAT_IV",
    "TAG_FRESH",
    "TAG_FUTURES",
    "TAG_LEVEL_BREAK",
    "TAG_RESULTS_TODAY",
    "V1N",
    "V1N_LISTS_CLIENT",
    "V1N_LISTS_OWNER",
    "V2SQ",
    "V2SQ_LISTS",
    "WATCHING",
    "BanView",
    "ChargesFn",
    "ContractError",
    "EntrySignal",
    "ExitEvent",
    "FutSnap",
    "FuturesView",
    "IvView",
    "ListRow",
    "PositionState",
    "ResultsView",
    "RiskBook",
    "TargetsConfig",
    "ZoneView",
]


class ContractError(ValueError):
    """A record violates the shared interface."""


def _dump(obj: Any) -> dict[str, Any]:
    result = {}
    for f in fields(obj):
        value = getattr(obj, f.name)
        if isinstance(value, date):
            value = value.isoformat()
        elif isinstance(value, tuple):
            value = list(value)
        elif isinstance(value, dict):
            value = value.copy()
        result[f.name] = value
    return result


def _load[T: _Record](cls: type[T], d: dict[str, Any]) -> T:
    expected = {f.name for f in fields(cls)}
    missing, extra = expected - d.keys(), d.keys() - expected
    if missing or extra:
        raise ContractError(f"{cls.__name__}: missing keys {sorted(missing)}, extra keys {sorted(extra)}")
    values = d.copy()
    for name in cls._dates:
        values[name] = date.fromisoformat(values[name])
    for name in cls._tuples:
        values[name] = tuple(values[name])
    for name, value in values.items():
        if isinstance(value, dict):
            values[name] = value.copy()
    return cls(**values)


class _Record:
    _dates: ClassVar[frozenset[str]] = frozenset()
    _tuples: ClassVar[frozenset[str]] = frozenset()

    def to_dict(self) -> dict[str, Any]:
        return _dump(self)

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> Self:
        return _load(cls, d)


def _member(name: str, value: Any, allowed: tuple) -> None:
    if value not in allowed:
        raise ContractError(f"{name}: invalid value {value!r}")


def _identity(scanner: str, list_name: str, option_type: str) -> None:
    _member("scanner", scanner, SCANNER_IDS)
    _member("list_name", list_name, LISTS_BY_SCANNER[scanner])
    _member("option_type", option_type, SIDES)


def _tags(tags: tuple[str, ...], results_today: bool) -> None:
    for tag in tags:
        _member("tags", tag, TAGS)
    if (TAG_RESULTS_TODAY in tags) != results_today:
        raise ContractError("results_today must match the RESULTS TODAY tag")


@dataclass(frozen=True)
class TargetsConfig(_Record):
    t1_pct: float = 25.0
    t2_pct: float = 50.0
    t3_pct: float = 75.0
    t4_pct: float = 100.0

    def as_tuple(self) -> tuple[float, float, float, float]:
        return self.t1_pct, self.t2_pct, self.t3_pct, self.t4_pct


@dataclass(frozen=True)
class FutSnap(_Record):
    """One completed five-minute current-month futures bar."""

    ts_ms: int
    close: float
    prev_close: float | None
    oi: int | None
    oi_day_high: int | None


@dataclass(frozen=True)
class ListRow(_Record):
    """Dashboard row; use .key, never the object, as a set / dict key (EntrySignal supplies .key)."""

    scanner: str
    list_name: str
    trade_date: date
    security_id: int
    underlying_security_id: int
    symbol: str
    expiry_date: date
    strike: float
    option_type: str
    lot_size: int
    tick_size: float
    rank: int
    oi: int | None
    zone_edge: float | None
    origin_list: str | None
    carry_build_low_oi: int | None
    carry_build_peak_oi: int | None
    state: str
    tags: tuple[str, ...]
    results_today: bool
    ban_reason: str | None
    metrics: dict[str, float | None] = field(default_factory=dict)

    _dates: ClassVar[frozenset[str]] = frozenset({"trade_date", "expiry_date"})
    _tuples: ClassVar[frozenset[str]] = frozenset({"tags"})

    def __post_init__(self) -> None:
        _identity(self.scanner, self.list_name, self.option_type)
        _member("state", self.state, ROW_STATES)
        _tags(self.tags, self.results_today)
        if self.list_name != CARRY and (
            self.carry_build_low_oi is not None or self.carry_build_peak_oi is not None
        ):
            raise ContractError("carry_build_* must be None outside CARRY")


@dataclass(frozen=True)
class EntrySignal(_Record):
    """Entry decision; use .key, never the object, as a set / dict key."""

    scanner: str
    list_name: str
    trade_date: date
    security_id: int
    underlying_security_id: int
    symbol: str
    expiry_date: date
    strike: float
    option_type: str
    lot_size: int
    tick_size: float
    entry_no: int
    signal_bar_ms: int
    signal_ms: int
    signal_px: float
    fill_rule: str
    stop_kind: str
    stop_level: float
    risk_per_lot: float | None
    risk_is_estimate: bool
    lots: int | None
    tags: tuple[str, ...]
    results_today: bool
    ban_reason: str | None
    risk_block: str | None = None
    alert_sent_ms: int | None = None
    metrics: dict[str, float | None] = field(default_factory=dict)

    _dates: ClassVar[frozenset[str]] = frozenset({"trade_date", "expiry_date"})
    _tuples: ClassVar[frozenset[str]] = frozenset({"tags"})

    def __post_init__(self) -> None:
        _identity(self.scanner, self.list_name, self.option_type)
        _member("fill_rule", self.fill_rule, FILL_RULES)
        _member("stop_kind", self.stop_kind, STOP_KINDS)
        _member("entry_no", self.entry_no, (1, 2))
        _tags(self.tags, self.results_today)
        if self.risk_block is not None:
            _member("risk_block", self.risk_block, RISK_BLOCKS)

    @property
    def key(self) -> str:
        return f"{self.scanner}|{self.security_id}|{self.signal_bar_ms}"

    @property
    def blocked(self) -> bool:
        return self.ban_reason is not None or self.risk_block is not None

    @property
    def alert_latency_ms(self) -> int | None:
        return None if self.alert_sent_ms is None else self.alert_sent_ms - self.signal_ms


@dataclass(frozen=True)
class ExitEvent(_Record):
    """Exit decision; ts_ms is bar start, decided_ms is the deciding close or tick."""

    position_key: str
    kind: str
    leg: str | None
    ts_ms: int
    decided_ms: int
    px: float | None
    lots: int
    new_stop: float | None
    reason: str
    alert_sent_ms: int | None = None

    def __post_init__(self) -> None:
        _member("kind", self.kind, EXIT_KINDS)
        if self.leg is not None:
            _member("leg", self.leg, LEGS)
        if self.lots < 0:
            raise ContractError("lots must be nonnegative")


@dataclass(frozen=True)
class PositionState(_Record):
    """Schema-12 ladder state; dates and tuple fields round-trip through JSON."""

    position_key: str
    scanner: str
    list_name: str
    security_id: int
    underlying_security_id: int
    symbol: str
    expiry_date: date
    strike: float
    option_type: str
    lot_size: int
    entry_date: date
    entry_ms: int
    entry_px: float
    lots_total: int
    lots_open: int
    leg_lots: tuple[int, ...]
    targets: tuple[float, ...]
    targets_hit: tuple[str, ...]
    stop_kind: str
    zone_edge: float | None
    prem_stop: float | None
    r_premium: float | None
    trail_peak: float | None
    pending_exit: str | None
    pending_decided_ms: int | None
    oi_at_entry: int | None
    oi_low_since_entry: int | None
    reached_1r: bool
    nights_carried: int
    realized_rs: float
    closed: bool
    updated_ms: int

    _dates: ClassVar[frozenset[str]] = frozenset({"expiry_date", "entry_date"})
    _tuples: ClassVar[frozenset[str]] = frozenset({"leg_lots", "targets", "targets_hit"})

    def __post_init__(self) -> None:
        _identity(self.scanner, self.list_name, self.option_type)
        _member("stop_kind", self.stop_kind, STOP_KINDS)
        for leg in self.targets_hit:
            _member("targets_hit", leg, LEGS)
        if sum(self.leg_lots) != self.lots_total:
            raise ContractError("leg_lots must sum to lots_total")
        if not 0 <= self.lots_open <= self.lots_total:
            raise ContractError("lots_open must be between zero and lots_total")
        if len(self.leg_lots) > MAX_LEGS[self.scanner] or (self.leg_lots and self.leg_lots[-1] == 0):
            raise ContractError("leg_lots exceeds MAX_LEGS or has a trailing zero")
        if (self.pending_exit is None) != (self.pending_decided_ms is None):
            raise ContractError("pending_exit and pending_decided_ms must be set together")
        if self.pending_exit is not None:
            _member("pending_exit", self.pending_exit, EXIT_KINDS)

    def to_dict(self) -> dict[str, Any]:
        return {"schema": POSITION_STATE_SCHEMA, **_dump(self)}

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> Self:
        if d.get("schema") != POSITION_STATE_SCHEMA:
            raise ContractError("schema missing or differs from position-state/1")
        values = d.copy()
        del values["schema"]
        return _load(cls, values)


@runtime_checkable
class BanView(Protocol):
    """UNKNOWN status fails closed; a None reason means the symbol is not banned."""

    def status(self, day: date) -> str: ...
    def ban_reason(self, symbol: str, day: date) -> str | None: ...


@runtime_checkable
class ResultsView(Protocol):
    def results_today(self, symbol: str, day: date) -> bool: ...


@runtime_checkable
class ZoneView(Protocol):
    """Confirmed 1H near edge at ts_ms: support top for CE, resistance bottom for PE.

    Relative to stock price, using only zones confirmed at or before ts_ms.
    None means no zone, requiring the premium-stop fallback.
    """

    def near_edge(self, symbol: str, side: str, price: float, ts_ms: int) -> float | None: ...


@runtime_checkable
class FuturesView(Protocol):
    """Current-month futures bar starting at ts_ms."""

    def fut_snap(self, symbol: str, ts_ms: int) -> FutSnap | None: ...


@runtime_checkable
class IvView(Protocol):
    """Vol points from the option's LAST TRADED price, never the exchange close."""

    def iv_pct(self, security_id: int, ts_ms: int) -> float | None: ...


@runtime_checkable
class ChargesFn(Protocol):
    """Round-trip rupees for qty units; charges_total fits positionally."""

    def __call__(self, entry_px: float, exit_px: float, qty: float) -> float: ...


@runtime_checkable
class RiskBook(Protocol):
    """Owner-configured capital, combined scanner open count, realized-only day P&L."""

    def capital_rs(self) -> float:
        """Read the owner config value; never a contract constant."""
        ...

    def open_count(self) -> int:
        """Open V1N and V2SQ positions together."""
        ...

    def day_pnl_rs(self, day: date) -> float:
        """Net P&L after charges for legs closed that day; excludes open MTM."""
        ...
