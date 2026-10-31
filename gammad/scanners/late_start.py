"""CX-208 / T-291 / D634 (A): today's closed bars seed late-starting engines.

Preserve the day OI peak and replay history before the first live bar.
"""

from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any
from zoneinfo import ZoneInfo

from gammad.core import sessions

SEED_INTERVAL_MIN = 5
MINUTE_MS = 60_000
IST = ZoneInfo("Asia/Kolkata")


@dataclass(frozen=True)
class ContractSeed:
    security_id: int
    bars: tuple[Any, ...]
    expected_slots: int
    oi_missing: int
    peak_oi: int | None

    @property
    def complete(self) -> bool:
        return self.oi_missing == 0 and len(self.bars) == self.expected_slots


def closed_slots(
    trade_date: date, start_ms: int, *, interval_min: int = SEED_INTERVAL_MIN,
    eras: Sequence[sessions.Era] | None = None,
) -> tuple[int, ...]:
    """Slot starts whose bars closed by startup, bounded by the day's session."""
    era = sessions.era_for(trade_date, eras)
    open_ms = int(datetime.combine(trade_date, era.fo_open, IST).timestamp() * 1000)
    close_ms = int(datetime.combine(trade_date, era.fo_close, IST).timestamp() * 1000)
    span = interval_min * MINUTE_MS
    end = min(start_ms, close_ms)
    return tuple(range(open_ms, end - span + 1, span))


def build_seed(
    security_id: int, bars: Iterable[Any], trade_date: date, start_ms: int, *,
    interval_min: int = SEED_INTERVAL_MIN, eras: Sequence[sessions.Era] | None = None,
) -> ContractSeed:
    """Keep the last supplied bar per closed slot and measure missing OI."""
    slots = closed_slots(trade_date, start_ms, interval_min=interval_min, eras=eras)
    allowed = set(slots)
    by_slot = {}
    for bar in bars:
        if bar.start_ms in allowed:
            by_slot[bar.start_ms] = bar
    kept = tuple(by_slot[ts] for ts in sorted(by_slot))
    oi = [bar.oi for bar in kept if bar.oi is not None]
    return ContractSeed(security_id, kept, len(slots), len(slots) - len(oi), max(oi, default=None))


def build_seeds(
    bars_by_sid: Mapping[int, Iterable[Any]], security_ids: Iterable[int], trade_date: date,
    start_ms: int, *, interval_min: int = SEED_INTERVAL_MIN,
    eras: Sequence[sessions.Era] | None = None,
) -> dict[int, ContractSeed]:
    """Build one seed per requested id, including contracts with no history."""
    return {
        sid: build_seed(sid, bars_by_sid.get(sid, ()), trade_date, start_ms,
                        interval_min=interval_min, eras=eras)
        for sid in dict.fromkeys(security_ids)
    }


def seed_v1n(engine: Any, seeds: Mapping[int, ContractSeed], to_bar: Callable[[Any], Any]) -> int:
    """Store option history without evaluating it (CX-199)."""
    count = 0
    for sid in sorted(seeds):
        seed = seeds[sid]
        if seed.bars:
            engine.seed_option(sid, [to_bar(bar) for bar in seed.bars])
            count += len(seed.bars)
    return count


def _identity(bar: Any) -> Any:
    return bar


def replay_squeeze(
    engine: Any, seeds: Mapping[int, ContractSeed], to_bar: Callable[[Any], Any] = _identity,
) -> list[Any]:
    """Replay through CX-202's seed hook; callers must suppress returned signals.

    The squeeze engine spends arms crossed before startup (D641 (3)).
    """
    ordered = [(bar.start_ms, sid, bar) for sid, seed in seeds.items() for bar in seed.bars]
    ordered.sort(key=lambda item: (item[0], item[1]))
    suppressed = []
    for _, sid, bar in ordered:
        suppressed.extend(engine.on_seed_bar(sid, to_bar(bar)))
    return suppressed


def summary(seeds: Mapping[int, ContractSeed]) -> dict[str, Any]:
    """Return startup diagnostics for the caller to log."""
    return {
        "contracts": len(seeds),
        "bars": sum(len(seed.bars) for seed in seeds.values()),
        "incomplete": sorted(sid for sid, seed in seeds.items() if not seed.complete),
    }
