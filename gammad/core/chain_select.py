"""Spec Step 2 strike selection (pure).

Candidates for one side of one chain:
  PE: strike < ref_price (OTM) AND |strike - zone_edge| / zone_edge * 100 <= band_pct, edge = support_hi
  CE: strike > ref_price (OTM) AND |strike - zone_edge| / zone_edge * 100 <= band_pct, edge = resistance_lo
Rows with no security_id or OI None/0 are not candidates. Ranked by OI desc; ties go to the strike
nearer the zone edge, then the lower strike (deterministic). The band is never widened: fewer than
top_n candidates means fewer picks, and every pick records candidates_n.

S1.5.0 LIQUIDITY FLOOR (D157(1), replaces S1.3.0 D85(3)): `passes_floor` -- a candidate needs OI > 0 (always),
OI >= min_oi_lots x lot size, and a previous-session traded value (`prev_turnover_rs` = previous_volume x
previous_close, rupees: Dhan's previous_volume is QUANTITY, measured 2026-09-30) >= min_prev_turnover_rs. A lot size
that is unknown skips the OI-lots test (stats["lot_unknown"]); a previous-session value the broker's chain does not
carry FAILS OPEN (stats["prev_unknown"], D161(5): fail-closed empties client lists). The S1.3.0 premium test (premium
>= min_premium_ticks x tick) runs only when min_premium_ticks > 0. A test whose threshold is 0 is not run. A failing
strike is not a candidate, so the next strike by OI takes its place; candidates_n counts eligible strikes only.

select_strikes reads the thresholds from the ProximityCfg it is given (`cfg=`). WITHOUT a cfg it is the S1.3.0
premium floor alone (`min_premium_ticks=`, default off): no OI-lots and no turnover test, whatever the rows carry, so
a caller that has not moved to `cfg=` keeps its results. A strike's tick comes from `ticks` (security_id -> rupees,
the scrip master); unknown -> default_tick(ref_price) (NSE/FAOP/70382, D84: Rs 0.01 when the underlying is below
Rs 250, else Rs 0.05).

The band test is done as |strike - edge| * 100 <= band_pct * edge so an exact 2.5 % strike is not
lost to float rounding of a division.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, MutableMapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

from gammad.broker.base import ChainRow

if TYPE_CHECKING:
    from gammad.config import ProximityCfg

Side = Literal["PE", "CE"]

SMALL_TICK_BELOW = 250.0  # D84: NSE stock options on an underlying below Rs 250 tick at Rs 0.01
SMALL_TICK = 0.01
STD_TICK = 0.05
_EPS = 1e-9  # float step: an exact threshold (Rs 1.00 at 20 x 0.05, exactly Rs 1 lakh) passes


@dataclass(frozen=True)
class Pick:
    security_id: int
    strike: float
    oi: int
    tag: str  # HOI-1, HOI-2, ...
    candidates_n: int
    tick_size: float | None = None  # the scrip master's tick (rupees); None = unknown (the floor used default_tick)


@dataclass(frozen=True)
class _PremiumOnly:
    """The floor of a select_strikes call with no cfg: the S1.3.0 premium test alone (duck-types ProximityCfg)."""

    min_premium_ticks: float = 0.0
    min_oi_lots: float = 0.0
    min_prev_turnover_rs: float = 0.0


def in_band(strike: float, zone_edge: float, band_pct: float) -> bool:
    return abs(strike - zone_edge) * 100.0 <= band_pct * zone_edge


def default_tick(ref_price: float) -> float:
    """The NSE stock-option tick when the master's is unknown (D84, NSE/FAOP/70382)."""
    return SMALL_TICK if ref_price < SMALL_TICK_BELOW else STD_TICK


def above_floor(premium: float | None, tick: float, min_premium_ticks: float) -> bool:
    """DEPRECATED (S1.5.0): the premium test alone; list builds call passes_floor. Kept for research scripts.
    premium >= min_premium_ticks x tick (a float step of 1e-9 so an exact Rs 1.00 at 0.05 passes)."""
    if min_premium_ticks <= 0:
        return True
    return premium is not None and premium >= min_premium_ticks * tick - _EPS


def prev_turnover_rs(row: ChainRow) -> float | None:
    """The previous session's traded value in RUPEES = previous_volume (quantity) x previous_close; None = unknown
    (either field missing). A known zero volume is 0.0, a value, not unknown."""
    if row.previous_volume is None or row.previous_close is None:
        return None
    return float(row.previous_volume) * float(row.previous_close)


def _count(stats: MutableMapping[str, int] | None, key: str) -> None:
    if stats is not None:
        stats[key] = stats.get(key, 0) + 1


def passes_floor(
    row: ChainRow,
    *,
    lot_size: float | None,
    tick_size: float | None,
    cfg: ProximityCfg,
    stats: MutableMapping[str, int] | None = None,
) -> bool:
    """The S1.5.0 list-build liquidity floor for one chain row (module docstring). tick_size None (only read when
    min_premium_ticks > 0) = STD_TICK; select_strikes always resolves it (the master's, else default_tick)."""
    if not row.oi or row.oi <= 0:
        return False
    min_lots = float(cfg.min_oi_lots)
    if min_lots > 0:
        if not lot_size or lot_size <= 0:
            _count(stats, "lot_unknown")
        elif row.oi < min_lots * float(lot_size) - _EPS:
            return False
    min_turnover = float(cfg.min_prev_turnover_rs)
    if min_turnover > 0:
        value = prev_turnover_rs(row)
        if value is None:
            _count(stats, "prev_unknown")  # D161(5): FAIL-OPEN, counted for the build summary
        elif value < min_turnover - _EPS:
            return False
    min_ticks = float(cfg.min_premium_ticks)
    return min_ticks <= 0 or above_floor(row.ltp, tick_size or STD_TICK, min_ticks)


def select_strikes(
    chain_rows: Iterable[ChainRow],
    side: Side,
    ref_price: float,
    zone_edge: float,
    band_pct: float,
    top_n: int,
    *,
    cfg: ProximityCfg | None = None,
    lot_size: float | None = None,
    stats: MutableMapping[str, int] | None = None,
    min_premium_ticks: float | None = None,
    ticks: Mapping[int, float | None] | None = None,
) -> list[Pick]:
    """cfg = the thresholds (ProximityCfg: min_oi_lots, min_prev_turnover_rs, min_premium_ticks); None = the S1.3.0
    premium floor alone at `min_premium_ticks` (legacy callers). Passing both is an error: one source of truth.
    lot_size = the list's lot (None -> the OI-lots test is skipped and counted); stats collects the floor's
    counters (lot_unknown, prev_unknown) over the candidates that reached it."""
    if side not in ("PE", "CE"):
        raise ValueError(f"side must be 'PE' or 'CE', got {side!r}")
    if not (ref_price > 0 and zone_edge > 0):
        raise ValueError(f"ref_price and zone_edge must be positive (got {ref_price}, {zone_edge})")
    if band_pct < 0 or top_n < 1:
        raise ValueError("band_pct must be >= 0 and top_n >= 1")
    if cfg is not None and min_premium_ticks is not None:
        raise ValueError("pass the floor thresholds as cfg OR min_premium_ticks, not both")
    floor_cfg = cfg if cfg is not None else _PremiumOnly(float(min_premium_ticks or 0.0))
    ticks = ticks or {}
    fallback = default_tick(ref_price)
    cands: list[ChainRow] = []
    for r in chain_rows:
        if r.option_type != side or r.security_id is None or not r.oi or r.oi <= 0:
            continue
        otm = r.strike < ref_price if side == "PE" else r.strike > ref_price
        if not (otm and in_band(r.strike, zone_edge, band_pct)):
            continue
        tick = ticks.get(int(r.security_id)) or fallback
        if not passes_floor(r, lot_size=lot_size, tick_size=tick, cfg=floor_cfg, stats=stats):  # type: ignore[arg-type]
            continue
        cands.append(r)
    cands.sort(key=lambda r: (-r.oi, abs(r.strike - zone_edge), r.strike))
    n = len(cands)
    return [
        Pick(int(r.security_id), r.strike, int(r.oi), f"HOI-{i}", n, ticks.get(int(r.security_id)))  # type: ignore[arg-type]
        for i, r in enumerate(cands[:top_n], start=1)
    ]
