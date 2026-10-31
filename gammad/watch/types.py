"""Minimal value types for the Step 4 signal engine and the alert router.

Deliberately independent of `gammad/watch/bars.py` and `gammad/watch/feed.py` (built in parallel):
the engine consumes these shapes, and the watcher adapts its own bar/tick objects into them.

Conventions:
- `ts_ms` is epoch milliseconds (D14). A `Bar.ts_ms` is the bar's OPEN time (interval start).
- `Bar.volume` is the volume traded INSIDE the bar (not the day's cumulative volume).
- `TickView.volume_day` is the day's cumulative volume as sent by the feed.
- `Signal.oi_drop_pct` is in PERCENT units (10.4 = 10.4 %), matching D20; the thresholds in
  config/strategy.yaml are fractions (0.08).
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Literal

SignalKind = Literal["E1", "E2"]


@dataclass(frozen=True, slots=True)
class Bar:
    ts_ms: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    oi: float


@dataclass(frozen=True, slots=True)
class TickView:
    ts_ms: int  # exchange time of the tick
    recv_ms: int  # local receive time (start of the latency clock)
    ltp: float
    volume_day: float
    oi: float


@dataclass(frozen=True, slots=True)
class Signal:
    kind: SignalKind
    strong: bool
    ts_ms: int
    security_id: int
    ltp: float
    oi_drop_pct: float
    vol_ratio: float
    consol_lo: float
    consol_hi: float
    consol_mid: float
    sl: float
    reason: str
    latency_ms: int
    # S1.3.0 (D95(4)): the structured at-signal snapshot = gammad.watch.engine.Evaluation.to_dict() of the bar the
    # engine evaluated + `bar_ts_ms` (the signal bar's OPEN time) + `signal_ts_ms` / `signal_price` + the arming
    # cycle's unwind inputs (`cycle_e2_close`, `oi_peak_bar_close`, ...; see engine.ContractEngine._snapshot). The
    # router spreads Signal.to_dict() into signals.payload_json, so it is stored with no schema change.
    snapshot: dict[str, Any] | None = field(default=None, hash=False)  # a dict is unhashable: kept out of __hash__

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class ContractMeta:
    """What the caller knows about a watchlist contract; the engine never needs it, the router does."""

    symbol: str
    strike: float
    option_type: str  # 'PE' | 'CE'
    list_name: str  # BREAKDOWN-PE | BREAKOUT-CE | EXPIRY-PE | EXPIRY-CE
    expiry_date: str  # YYYY-MM-DD
    tag: str | None = None  # HOI-1 | HOI-2
    zone_dist_pct: float | None = None  # percent units (D20), measured at the list BUILD (09:20 / 13:15)
    zone_edge: float | None = None  # watchlist.zone_edge: support top (PE lists) / resistance bottom (CE lists)
    lot_size: int | None = None
    underlying_security_id: int | None = None
    tick_size: float | None = None  # S1.3.0: watchlist.tick_size, RUPEES (scrip master TICK_SIZE is paise / 100)
    # T-160 (D125 F2, F9): watchlist.oi = the contract's OI at the list BUILD (the 09:20 chain OI); None = unknown
    build_oi: float | None = None
    # 0.8.1 (CONTRACTS.md 3; nothing reads them in Wave 2): watchlist.atr_pct (PERCENT units, 2.3 = 2.3 %), and the
    # carry-forward state of a tag = 'CARRY' row (D157(2), OWNER-ONLY)
    atr_pct: float | None = None
    carried: bool = False
    carry_state: str | None = None  # WATCH | BUILD-UP | PRIMED
    carry_k: int | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class ArmContext:
    """T-160 (D125 F2-F4, F8): what the caller knows about a contract AT A BAR CLOSE beyond its bars, for the
    OPTIONAL arming tests of the owner's V4 shadow scanners (engine.EngineParams arm_* / min_build_over_open_pct /
    min_dte). V1 / V2 set none of those params, so they never read it.

    build_oi     watchlist.oi, the contract's OI at the list build (09:20 chain OI).
    expiry_date  YYYY-MM-DD (watchlist.expiry_date).
    dte          trading days after the bar's IST date up to and including the expiry (gammad.core.calendar, the
                 rule of the card's expiry warning, spec 4.3); None = unknown.
    spot         the underlying's last price at the bar close (live: the last underlying tick).
    zone_edge    watchlist.zone_edge (support top for PE lists, resistance bottom for CE lists).
    option_type  'PE' | 'CE'.
    """

    build_oi: float | None = None
    expiry_date: str | None = None
    dte: int | None = None
    spot: float | None = None
    zone_edge: float | None = None
    option_type: str = ""


Hold =Literal["swing", "intraday"]
Quadrant = Literal["SC", "LU", "LB", "SB", "FLAT"]
# S1.4.0 (D114(3)): the tier of an E1 = its gate passes (gammad.alerts.gates.tier); stored in signals.tier.
Tier = Literal["ENTRY", "STRONG_WATCHLIST", "WATCHLIST"]


@dataclass(frozen=True, slots=True)
class SignalContext:
    """What the live watcher (or the backtest) knows AT SIGNAL TIME beyond the Signal itself (S1.1.0).

    hold          D58: "swing" (BREAKDOWN/BREAKOUT, 1..5 day hold) or "intraday" (EXPIRY-*).
    spot          the underlying's last price at the signal (live: last underlying tick).
    zone_edge     = ContractMeta.zone_edge.
    zone_gap_pct  gammad.alerts.gates.zone_gap_pct(option_type, spot, zone_edge); percent units, signed
                  (PE: + = spot above the support top; CE: + = spot below the resistance bottom).
    oi_peak/oi_now  the D7 intraday peak OI and the OI at the signal.
    avg_vol_lots / zero_vol_bars  gammad.alerts.gates.liquidity() over the GatesCfg.liq_sma_bars closed
                  5-min bars ending at the signal bar.
    invalidation  D60: swing = the zone edge the underlying must not CLOSE back over (daily close);
                  intraday = None (the stop is Signal.sl, the 5-min base low).
    gate_reasons  D63: gates that FAILED ("zone", "liquidity"; S1.3.0 adds "direction" and "unwind", D85(2) /
                  D94(1); S1.4.0 runs them on every list when GatesCfg.all_lists, else INTRADAY lists only).
    gate_unknown  gates that could not be evaluated (a missing input). S1.4.0: not a pass for the tier
                  (gammad.alerts.gates.tier); S1.3.x read them as fail-open.
    quadrant      S1.3.0 (D94(2), a LABEL, never a gate): the OI-price quadrant over the unwind window (OI-peak
                  bar close -> signal): OI down + premium up >= 1 tick = "SC" short covering, down/down = "LU" long
                  unwinding, up/up = "LB" long buildup, OI up + premium down = "SB" short buildup, premium within
                  one tick or OI unchanged = "FLAT"; None when an input (snapshot, tick) is missing.
    """

    hold: Hold
    spot: float | None = None
    zone_edge: float | None = None
    zone_gap_pct: float | None = None
    oi_peak: float | None = None
    oi_now: float | None = None
    avg_vol_lots: float | None = None
    zero_vol_bars: int | None = None
    lot_size: int | None = None
    invalidation: float | None = None
    gate_reasons: tuple[str, ...] = ()
    gate_unknown: tuple[str, ...] = ()
    quadrant: Quadrant | None = None
    # T-160 (D125 F9): = ContractMeta.build_oi (the 09:20 build OI); the owner E2 card's "+x % over build" reads it
    build_oi: float | None = None
    # 0.8.1 (CONTRACTS.md 3; nothing reads them in Wave 2): V1's dynamic zone limit (percent), its F1 / F2 direction
    # tests (None = not evaluated), and whether the contract is a carried row (the carry cap, D161(2))
    zone_limit_pct: float | None = None
    f1: bool | None = None
    f2: bool | None = None
    carried: bool = False
    # 0.8.2 T-202 (D176(1), DESIGN-REVIEW C.8): the OWNER router sets it on an E1 whose contract is not on the client's
    # lists (router.in_client_view); stored in ctx_json, dropped from every client copy (router.CLIENT_CTX_DROP)
    off_client: bool = False

    @property
    def gate_status(self) -> str:
        return "gated" if self.gate_reasons else "pass"

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["gate_reasons"] = list(self.gate_reasons)
        d["gate_unknown"] = list(self.gate_unknown)
        d["gate_status"] = self.gate_status
        return d
