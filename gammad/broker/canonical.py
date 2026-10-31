"""Canonical instrument identity (BROKER-ADAPTER-SPEC §1, measured 2026-09-25).

Rule: the DB keeps `security_id` = the NSE exchange token (Dhan's NSE SECURITY_ID IS that token; 0 mismatches
against Zerodha, Upstox, Groww, Fyers, Angel public files) and the canonical segment names already in the code:
NSE_FNO options, NSE_EQ underlyings, IDX_I indices. The canonical master is Dhan's PUBLIC detailed scrip master
(no auth; `gammad/broker/dhan.py` SCRIP_MASTER_URL, cached per IST day, 3 kept) for every broker: universe, lot
sizes, and expiries for adapters without an expiry endpoint.

Each non-Dhan adapter maps canonical (segment, token) -> its native id from its own public master (`NativeIdMap`)
and runs the RUNTIME IDENTITY GUARD at subscription: every contract's native row must match the canonical
(underlying, expiry, strike, CE/PE); a mismatch DROPS that contract with an ERROR (+ dashboard flag), never a silent
substitute (`guard_contracts`). Kotak Neo and Nuvama were NOT measured -- their builders measure on first login.

`chain_from_master_quotes` is the shared "master+quotes" chain for brokers without a chain endpoint (capabilities
chain="master+quotes"): canonical master rows + one quote batch; iv and previous_oi are None; a row the quote
batch lacks, or that has no LTP or OI, is skipped (never a fabricated zero).
"""

from __future__ import annotations

import math
from collections.abc import Hashable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from gammad.broker.base import ChainRow, Instrument
from gammad.log import get_logger
from gammad.watch.feed_protocol import Quote

log = get_logger(__name__)

SEG_OPT = "NSE_FNO"
SEG_EQ = "NSE_EQ"
SEG_IDX = "IDX_I"
CANONICAL_SEGMENTS: tuple[str, ...] = (SEG_OPT, SEG_EQ, SEG_IDX)

STRIKE_TOL = 1e-6


def norm_underlying(symbol: str | None) -> str:
    """Upper-case, strip, and drop an NSE `-EQ` series suffix (Groww lists 2 EQ names that way, §1)."""
    s = (symbol or "").strip().upper()
    return s.removesuffix("-EQ")


@dataclass(frozen=True)
class ContractKey:
    """What identity means for one row: (underlying, expiry, strike, CE/PE). Equity/index rows carry only the
    underlying (expiry, strike, option_type None)."""

    underlying: str
    expiry: date | None = None
    strike: float | None = None
    option_type: str | None = None

    @classmethod
    def from_instrument(cls, inst: Instrument) -> ContractKey:
        return cls(
            norm_underlying(inst.underlying_symbol or inst.symbol),
            inst.expiry_date,
            inst.strike,
            inst.option_type,
        )

    def mismatch(self, other: ContractKey) -> str | None:
        """None when `other` is the same contract, else the first field that differs."""
        if norm_underlying(self.underlying) != norm_underlying(other.underlying):
            return f"underlying {self.underlying!r} != {other.underlying!r}"
        if self.expiry != other.expiry:
            return f"expiry {self.expiry} != {other.expiry}"
        if (self.strike is None) != (other.strike is None) or (
            self.strike is not None and other.strike is not None and abs(self.strike - other.strike) > STRIKE_TOL
        ):
            return f"strike {self.strike} != {other.strike}"
        if (self.option_type or None) != (other.option_type or None):
            return f"option type {self.option_type} != {other.option_type}"
        return None


class CanonicalMaster:
    """Lookup view over Dhan's public master (`gammad.broker.dhan.ScripMaster`), NSE rows only."""

    def __init__(self, scrip_master: object) -> None:
        self.sm = scrip_master
        self._by_key: dict[tuple[str, int], Instrument] | None = None

    @classmethod
    def load(cls, data_dir: Path, as_of: date | None = None) -> CanonicalMaster:
        """Today's master from `data_dir` (downloaded once per IST day with NO auth headers)."""
        from gammad.broker.dhan import ScripMaster, load_scrip_master

        return cls(ScripMaster(load_scrip_master(data_dir, as_of)))

    @classmethod
    def from_csv(cls, path: Path) -> CanonicalMaster:
        """A master file on disk (tests: tests/fixtures/scrip-master-sample.csv)."""
        from gammad.broker.dhan import ScripMaster, _read_master_csv

        return cls(ScripMaster(_read_master_csv(path)))

    def _index(self) -> dict[tuple[str, int], Instrument]:
        if self._by_key is None:
            idx: dict[tuple[str, int], Instrument] = {}
            for inst in self.sm.instruments():  # type: ignore[attr-defined]
                idx[(inst.exchange_segment, inst.security_id)] = inst
            d = self.sm.df  # type: ignore[attr-defined]
            for r in d[(d.EXCH_ID == "NSE") & (d.SEGMENT == "I")].itertuples(index=False):
                inst = self.sm._row_to_instrument(r)  # type: ignore[attr-defined]
                idx[(inst.exchange_segment, inst.security_id)] = inst
            self._by_key = idx
        return self._by_key

    def instrument(self, segment: str, security_id: int) -> Instrument | None:
        return self._index().get((segment, int(security_id)))

    def key(self, segment: str, security_id: int) -> ContractKey | None:
        inst = self.instrument(segment, security_id)
        return None if inst is None else ContractKey.from_instrument(inst)

    def lot_size(self, segment: str, security_id: int) -> int | None:
        inst = self.instrument(segment, security_id)
        return None if inst is None else inst.lot_size

    def underlying_symbol(self, segment: str, security_id: int) -> str | None:
        inst = self.instrument(segment, security_id)
        if inst is None:
            return None
        return inst.underlying_symbol or inst.symbol

    def option_rows(self, underlying_symbol: str, expiry: date) -> list[Instrument]:
        return self.sm.option_rows(underlying_symbol, expiry)  # type: ignore[attr-defined]

    def expiries(self, underlying_symbol: str, on_or_after: date | None = None) -> list[date]:
        """Ascending distinct OPTSTK/OPTIDX expiries of one underlying (for adapters with no expiry endpoint)."""
        want = norm_underlying(underlying_symbol)
        out = {
            i.expiry_date
            for i in self._index().values()
            if i.exchange_segment == SEG_OPT
            and i.option_type in ("CE", "PE")
            and i.expiry_date is not None
            and norm_underlying(i.underlying_symbol) == want
            and (on_or_after is None or i.expiry_date >= on_or_after)
        }
        return sorted(d for d in out if d is not None)


class NativeIdMap:
    """Bijective canonical (segment, token) <-> native id map, built from a broker's own public master.
    `add` refuses a second, different mapping for either side (ValueError)."""

    def __init__(self) -> None:
        self._to_native: dict[tuple[str, int], Hashable] = {}
        self._to_canon: dict[Hashable, tuple[str, int]] = {}

    def add(self, segment: str, security_id: int, native: Hashable) -> None:
        key = (segment, int(security_id))
        have = self._to_native.get(key)
        if have is not None and have != native:
            raise ValueError(f"{key} already maps to {have!r}, not {native!r}")
        back = self._to_canon.get(native)
        if back is not None and back != key:
            raise ValueError(f"native id {native!r} already maps to {back}, not {key}")
        self._to_native[key] = native
        self._to_canon[native] = key

    def native(self, segment: str, security_id: int) -> Hashable:
        """KeyError when the broker has no row for this contract (the caller drops it, never substitutes)."""
        return self._to_native[(segment, int(security_id))]

    def canonical(self, native: Hashable) -> tuple[str, int]:
        return self._to_canon[native]

    def __len__(self) -> int:
        return len(self._to_native)

    def __contains__(self, key: object) -> bool:
        return key in self._to_native


@dataclass(frozen=True)
class IdentityMismatch:
    segment: str
    security_id: int
    reason: str


def check_identity(
    master: CanonicalMaster, segment: str, security_id: int, native: ContractKey | None
) -> IdentityMismatch | None:
    """None when the broker's native row is the canonical contract; else why not."""
    canon = master.key(segment, security_id)
    if canon is None:
        return IdentityMismatch(segment, int(security_id), "not in the canonical master")
    if native is None:
        return IdentityMismatch(segment, int(security_id), "no native row at this token")
    why = canon.mismatch(native)
    return None if why is None else IdentityMismatch(segment, int(security_id), why)


def guard_contracts(
    master: CanonicalMaster, rows: Iterable[tuple[str, int, ContractKey | None]], *, broker: str
) -> tuple[list[tuple[str, int]], list[IdentityMismatch]]:
    """The runtime identity guard: (kept pairs, dropped mismatches). Logs one ERROR per dropped contract."""
    kept: list[tuple[str, int]] = []
    dropped: list[IdentityMismatch] = []
    for segment, sid, native in rows:
        bad = check_identity(master, segment, sid, native)
        if bad is None:
            kept.append((segment, int(sid)))
            continue
        dropped.append(bad)
        log.error("identity guard (%s): %s %d dropped: %s", broker, segment, int(sid), bad.reason)
    return kept, dropped


def chain_from_master_quotes(rows: Sequence[Instrument], quotes: Mapping[int, Quote]) -> list[ChainRow]:
    """ChainRows for one expiry from canonical master option rows + one quote batch (module docstring)."""
    out: list[ChainRow] = []
    skipped = 0
    for inst in rows:
        q = quotes.get(inst.security_id)
        if (
            inst.option_type not in ("CE", "PE")
            or inst.strike is None
            or q is None
            or q.ltp is None
            or q.oi is None
            or (isinstance(q.ltp, float) and math.isnan(q.ltp))
        ):
            skipped += 1
            continue
        out.append(
            ChainRow(
                strike=float(inst.strike),
                option_type=inst.option_type,
                security_id=inst.security_id,
                ltp=float(q.ltp),
                oi=int(q.oi),
                volume=int(q.volume_day or 0),
                iv=None,
                previous_oi=None,
            )
        )
    if skipped:
        log.info("master+quotes chain: %d of %d rows skipped (no quote / LTP / OI)", skipped, len(rows))
    out.sort(key=lambda r: (r.strike, r.option_type))
    return out
