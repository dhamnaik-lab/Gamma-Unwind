"""Spec Step 1: nearest LIVE support / resistance around a reference price.

support    = LIVE buy zone entirely below ref (top < ref), nearest = highest top.
resistance = LIVE sell zone entirely above ref (bottom > ref), nearest = lowest bottom.
Distances are PERCENT (1.8 means 1.8 %), measured to the near edge:
  dist_support_pct    = (ref - support.top) / ref * 100
  dist_resistance_pct = (resistance.bottom - ref) / ref * 100
Ties on the edge go to the NEWER zone (live lists are newest-first). Zones with a NaN edge
(opened before ATR existed) are never picked.

`pick_levels` is ALSO the V2 1-hour stop rule (gammad/v2scan/h1zones.py) and is imported by the frozen T-263 research:
it is never changed (tests/test_t279_inside_zone.py pins its source and its outputs). The DAILY levels path
(gammad/jobs/mark_levels.py) uses `pick_levels_daily` (T-279, S1.8.0): a stock INSIDE its zone is AT the zone.
"""

from __future__ import annotations

import math
from dataclasses import dataclass

from gammad.core.zones import Zone, ZoneState


@dataclass(frozen=True)
class Levels:
    ref_price: float
    support: Zone | None
    resistance: Zone | None
    dist_support_pct: float | None
    dist_resistance_pct: float | None


def _finite(*xs: float) -> bool:
    return all(math.isfinite(x) for x in xs)


def pick_levels(zone_state: ZoneState, ref_price: float) -> Levels:
    if not (math.isfinite(ref_price) and ref_price > 0):
        raise ValueError(f"ref_price must be a positive number, got {ref_price!r}")
    support: Zone | None = None
    for z in zone_state.live_buy:
        if _finite(z.top, z.bottom) and z.top < ref_price and (support is None or z.top > support.top):
            support = z
    resistance: Zone | None = None
    for z in zone_state.live_sell:
        if _finite(z.top, z.bottom) and z.bottom > ref_price and (
            resistance is None or z.bottom < resistance.bottom
        ):
            resistance = z
    return Levels(
        ref_price=ref_price,
        support=support,
        resistance=resistance,
        dist_support_pct=None if support is None else (ref_price - support.top) / ref_price * 100.0,
        dist_resistance_pct=None if resistance is None else (resistance.bottom - ref_price) / ref_price * 100.0,
    )


def pick_levels_daily(zone_state: ZoneState, ref_price: float) -> Levels:
    """T-279 (D266(3), rule R1): `pick_levels` with a zone that CONTAINS the reference price counted as AT it.

    support    = LIVE buy zone with bottom <= ref, nearest = highest top (a containing zone beats every zone below,
                 since its top >= ref > theirs); distance = (ref - top) / ref * 100, 0.0 when ref <= top (inside).
    resistance = LIVE sell zone with top >= ref, nearest = lowest bottom; distance 0.0 when ref >= bottom.
    Inside a buy AND a sell zone = both sides at distance 0. The edge stays the NEAR edge (support top / resistance
    bottom), so the strike window and the zone gate read what they read before. Outside every zone this returns
    exactly what pick_levels returns (same zones, same distances). Ties go to the newer zone; NaN edges never pick."""
    if not (math.isfinite(ref_price) and ref_price > 0):
        raise ValueError(f"ref_price must be a positive number, got {ref_price!r}")
    support: Zone | None = None
    for z in zone_state.live_buy:
        if _finite(z.top, z.bottom) and z.bottom <= ref_price and (support is None or z.top > support.top):
            support = z
    resistance: Zone | None = None
    for z in zone_state.live_sell:
        if _finite(z.top, z.bottom) and z.top >= ref_price and (
            resistance is None or z.bottom < resistance.bottom
        ):
            resistance = z
    return Levels(
        ref_price=ref_price,
        support=support,
        resistance=resistance,
        dist_support_pct=None if support is None else max(0.0, (ref_price - support.top) / ref_price * 100.0),
        dist_resistance_pct=None
        if resistance is None
        else max(0.0, (resistance.bottom - ref_price) / ref_price * 100.0),
    )
