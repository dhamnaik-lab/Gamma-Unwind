"""Read-side mapping of builder rows; no list rebuilding or store access."""

from collections import Counter, defaultdict
from collections.abc import Callable, Collection, Iterable, Mapping
from dataclasses import dataclass, replace
from datetime import date
from typing import Any

from gammad.scanners import contract as sc
from gammad.scanners.v1r.engine import V1Gates
from gammad.scanners.v1r.entry import V1rParams

DEFAULT_PARAMS = V1rParams()


@dataclass(frozen=True)
class ListBuild:
    rows: tuple[sc.ListRow, ...]
    dropped: dict[str, int]


def _date(value):
    return date.fromisoformat(value) if isinstance(value, str) else value


def v1n_list_rows(
    rows: Iterable[Mapping[str, Any]],
    *,
    trade_date: date,
    edition: str,
    dte_fn: Callable[[date, date], int | None],
    gates: V1Gates,
    results: sc.ResultsView,
    params: V1rParams = DEFAULT_PARAMS,
    open_v1n: Collection[int] = frozenset(),
) -> ListBuild:
    if edition not in ("owner", "client"):
        raise ValueError("unknown edition")
    dropped = Counter()
    mapped = []
    for source in rows:
        carry = source.get("tag") == "CARRY"
        retired = source.get("retired_run_id") is not None
        if _date(source["trade_date"]) != trade_date:
            dropped["other_date"] += 1
            continue
        if retired and not (carry and source.get("carry_reason") == "entered"):
            dropped["retired"] += 1
            continue
        if (
            not source.get("lot_size")
            or source["lot_size"] <= 0
            or not source.get("tick_size")
            or source["tick_size"] <= 0
        ):
            dropped["no_lot_or_tick"] += 1
            continue
        expiry = _date(source["expiry_date"])
        dte = dte_fn(trade_date, expiry)
        name = source["list_name"]
        if (
            dte is None
            or (carry and dte <= params.expiry_dte_max)
            or (
                name in (sc.EXPIRY_CE, sc.EXPIRY_PE)
                and not params.expiry_dte_min <= dte <= params.expiry_dte_max
            )
        ):
            dropped["dte"] += 1
            continue
        if edition == "client" and name in (sc.EXPIRY_CE, sc.EXPIRY_PE):
            dropped["edition"] += 1
            continue
        if carry and name not in sc.V1N_LISTS_OWNER[:-1]:
            raise ValueError("carry origin must be a V1 list")
        flagged = results.results_today(source["symbol"], trade_date)
        tags = ((sc.TAG_CARRY_STRIKE,) if carry else ()) + ((sc.TAG_RESULTS_TODAY,) if flagged else ())
        low, peak = source.get("oi_at_listing"), source.get("oi_peak_prior")
        row = sc.ListRow(
            scanner=sc.V1N,
            list_name=sc.CARRY if carry else name,
            trade_date=trade_date,
            security_id=source["security_id"],
            underlying_security_id=source["underlying_security_id"],
            symbol=source["symbol"],
            expiry_date=expiry,
            strike=source["strike"],
            option_type=source["option_type"],
            lot_size=source["lot_size"],
            tick_size=source["tick_size"],
            rank=1,
            oi=source.get("oi"),
            zone_edge=source.get("zone_edge"),
            origin_list=name if carry else None,
            carry_build_low_oi=int(low) if carry and low is not None else None,
            carry_build_peak_oi=int(peak) if carry and peak is not None else None,
            state=sc.WATCHING,
            tags=tags,
            results_today=flagged,
            ban_reason=gates.ban_block(source["symbol"], trade_date),
            metrics={"dte": float(dte)},
        )
        mapped.append((row, retired))
    # Rank the mapped cohort before the cap, without disturbing its input order.
    groups = defaultdict(list)
    for index, (row, _) in enumerate(mapped):
        groups[row.list_name, row.underlying_security_id].append(index)
    for group in groups.values():
        group.sort(
            key=lambda index: (
                mapped[index][0].oi is None,
                -(mapped[index][0].oi or 0),
                mapped[index][0].security_id,
            )
        )
        for rank, index in enumerate(group, 1):
            row, retired = mapped[index]
            mapped[index] = replace(row, rank=rank), retired
    carry_rows = [r for r, retired in mapped if r.list_name == sc.CARRY and not retired]
    carry_rows += [r for r, retired in mapped if r.list_name == sc.CARRY and retired]
    in_trade = {int(sid) for sid in open_v1n}
    held = [r for r in carry_rows if int(r.security_id) in in_trade]
    rest = [r for r in carry_rows if int(r.security_id) not in in_trade]
    dropped["cap"] += max(0, len(rest) - params.carry_cap)
    kept = [r for r, _ in mapped if r.list_name != sc.CARRY] + held + rest[: params.carry_cap]
    for row in kept:
        if (row.origin_list is not None) != (row.list_name == sc.CARRY):
            raise ValueError("origin_list must be set exactly for carry")
    kept.sort(key=lambda r: (sc.V1N_LISTS_OWNER.index(r.list_name), r.underlying_security_id, r.rank))
    return ListBuild(tuple(kept), {k: v for k, v in dropped.items() if v})
