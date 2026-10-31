"""F&O STOCK universe from the Dhan detailed scrip master.

Rule: distinct UNDERLYING_SYMBOL of NSE / segment D / INSTRUMENT FUTSTK rows whose expiry is
live (as_of <= expiry <= as_of + horizon_days), minus index underlyings (named list, plus anything
that also carries FUTIDX/OPTIDX rows), minus the exchange's `*NSETEST` placeholder contracts.

Why the expiry window: the 2026-09-24 master holds 228 distinct FUTSTK underlyings, but 18 of them
are `011NSETEST`..`181NSETEST` dummies expiring 2036-11-27 (ISIN `DUMMY...`). The live monthly
series (2026-09-29 / 10-27 / 11-23) carry 210 each. The window drops the dummies and any stale
expired-only symbol without relying on the name alone; the name filter is a second fence.

eq_security_id is UNDERLYING_SECURITY_ID of the FUTSTK row (== the NSE_EQ SECURITY_ID; RELIANCE
2885 in both). lot_size is taken from the NEAREST live expiry (lots can differ by series).
"""

from __future__ import annotations

import os
from collections.abc import Iterable
from datetime import date, timedelta
from pathlib import Path
from typing import NamedTuple

import pandas as pd

from gammad import REPO_ROOT
from gammad.log import get_logger

log = get_logger(__name__)

INDEX_UNDERLYINGS = frozenset({"NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "NIFTYNXT50", "NIFTYFPI"})
EXPECTED_RANGE = (200, 230)
DEFAULT_HORIZON_DAYS = 120

# Cross-check only, never a data source. NSE's fo_mktlots.csv is located by the GAMMA_FO_MKTLOTS environment
# variable, else `data/fo_mktlots.csv` in this checkout; neither present -> the cross-check is skipped (INFO).
# No machine-specific path lives in the source: it would ship inside every client package (PLAN-W12 risk 6).
CROSSCHECK_ENV = "GAMMA_FO_MKTLOTS"
DEFAULT_CROSSCHECK = REPO_ROOT / "data" / "fo_mktlots.csv"
_CROSSCHECK_HEADER = "derivatives on individual securities"


def crosscheck_path() -> Path:
    """GAMMA_FO_MKTLOTS when set (read at call time), else DEFAULT_CROSSCHECK."""
    override = os.environ.get(CROSSCHECK_ENV, "").strip()
    return Path(override) if override else DEFAULT_CROSSCHECK


class UniverseRow(NamedTuple):
    symbol: str
    eq_security_id: int
    lot_size: int


def build_universe(
    master: pd.DataFrame, as_of: date, horizon_days: int = DEFAULT_HORIZON_DAYS
) -> list[UniverseRow]:
    d = master
    nse_fno = d[(d.EXCH_ID == "NSE") & (d.SEGMENT == "D")]
    index_like = set(nse_fno[nse_fno.INSTRUMENT.isin(["FUTIDX", "OPTIDX"])].UNDERLYING_SYMBOL) | INDEX_UNDERLYINGS

    fut = nse_fno[nse_fno.INSTRUMENT == "FUTSTK"].copy()
    fut["_expiry"] = pd.to_datetime(fut.SM_EXPIRY_DATE.str[:10], errors="coerce").dt.date
    total_distinct = fut.UNDERLYING_SYMBOL.nunique()
    live = fut[(fut._expiry >= as_of) & (fut._expiry <= as_of + timedelta(days=horizon_days))]
    live = live[~live.UNDERLYING_SYMBOL.isin(index_like)]
    live = live[~live.UNDERLYING_SYMBOL.str.contains("NSETEST", regex=False)]

    eq = d[(d.EXCH_ID == "NSE") & (d.SEGMENT == "E") & (d.INSTRUMENT == "EQUITY")]
    eq_ids = set(eq.SECURITY_ID.astype(str))
    eq_by_symbol = dict(zip(eq[eq.SERIES == "EQ"].UNDERLYING_SYMBOL, eq[eq.SERIES == "EQ"].SECURITY_ID, strict=True))

    rows: list[UniverseRow] = []
    dropped: list[str] = []
    for sym, grp in live.sort_values("_expiry").groupby("UNDERLYING_SYMBOL", sort=True):
        near = grp.iloc[0]
        eq_id = str(near.UNDERLYING_SECURITY_ID).strip()
        if eq_id not in eq_ids:
            fallback = eq_by_symbol.get(sym)
            if fallback is None:
                dropped.append(str(sym))
                continue
            log.warning("universe: %s FUTSTK underlying id %s has no NSE_EQ row; using %s", sym, eq_id, fallback)
            eq_id = str(fallback)
        rows.append(UniverseRow(str(sym), int(eq_id), int(float(near.LOT_SIZE))))

    if dropped:
        log.warning("universe: dropped %d symbols with no NSE_EQ row: %s", len(dropped), ", ".join(dropped))
    n = len(rows)
    lo, hi = EXPECTED_RANGE
    level = log.info if lo <= n <= hi else log.warning
    level(
        "universe: %d F&O stocks (as_of %s; %d distinct FUTSTK underlyings before expiry/index/test filters)",
        n,
        as_of.isoformat(),
        total_distinct,
    )
    return rows


def read_crosscheck_symbols(path: Path) -> set[str]:
    """Column 2 of NSE fo_mktlots.csv, below the 'Derivatives on Individual Securities' row."""
    out: set[str] = set()
    seen_header = False
    with path.open(encoding="utf-8-sig", errors="replace") as fh:
        for line in fh:
            parts = [p.strip() for p in line.split(",")]
            if len(parts) < 2:
                continue
            if not seen_header:
                seen_header = parts[0].lower() == _CROSSCHECK_HEADER
                continue
            if parts[1] and parts[1].lower() != "symbol":
                out.add(parts[1].upper())
    return out


def cross_check(symbols: Iterable[str], path: Path | None = None) -> tuple[set[str], set[str]] | None:
    """Log (only_ours, only_nse). Returns None when the file is absent. Never alters the universe."""
    path = path or crosscheck_path()
    if not path.exists():
        log.info("universe cross-check skipped: %s not found", path)
        return None
    ours = {s.upper() for s in symbols}
    theirs = read_crosscheck_symbols(path)
    only_ours, only_nse = ours - theirs, theirs - ours
    log.info(
        "universe cross-check vs %s: ours=%d nse=%d only_ours=%d only_nse=%d",
        path.name,
        len(ours),
        len(theirs),
        len(only_ours),
        len(only_nse),
    )
    if only_ours:
        log.warning("universe: in scrip master but not fo_mktlots: %s", ", ".join(sorted(only_ours)))
    if only_nse:
        log.warning("universe: in fo_mktlots but not scrip master: %s", ", ".join(sorted(only_nse)))
    return only_ours, only_nse
