"""H60 sync: top up the 60-min stock bar store the V2 Scanner's 1h zones read (T-267, PLAN-T267 §1, M2, M10).

run_h60_sync(settings, adapter, usids, through, *, start=None) walks each underlying's MISSING fixed-grid 90-day
chunks between start and through (gammad/v2scan/h60_store.H60Store.missing_ranges) and fetches each with ONE
adapter.intraday(usid, "NSE_EQ", 60, s, min(e, through), oi=False, instrument="EQUITY") call:

  - an EMPTY HTTP 200 (no candles) = the window is before the stock's first bar (or a holiday-only window), NOT an
    error (LESSONS:247): the empty chunk is written so it is never fetched again;
  - Dhan's HTTP 400 DH-907 ("no data in this window") is the same fact (backtest dhan_history.NO_DATA_CODE);
  - any other BrokerError fails THAT underlying (logged; the next one is tried); a BrokerAuthError stops the sync
    (every later call would fail the same way).

`through` = the last COMPLETE day wanted (yesterday at startup / 08:30; today after the 16:05 close). A chunk ending
after `through` is written as `<s>_<e>.asof-<through + 1>.parquet`.

start: the client starts AFTER the packaged zone seed's as_of (it never syncs from 2017, M1); the default is
settings.v2_scanner.h1_history_start when that block exists, else 2017-04-03 (the owner's full store).
M2: a failed sync never blocks V2 -- the books run on what exists; the report is for the owner's log only.

0.8.9 (T-419, D480 (3)) top_up(underlyings, adapter, *, through, ...): the CLIENT install's morning top-up for the
Still Watching list. The client ships a 1h zone SEED (h1zones.load_seed) and fetches, through its own broker, ONLY
the 60-min bars after the seed's as_of, ONLY for the given (carried) underlyings that the seed holds a book for. An
underlying outside the seed is reported, never back-filled from 2017 (no full-history download at install, D480 (3)
rejected alternative); no seed = nothing fetched. run_h60_sync (the owner's path) is unchanged.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path
from typing import Any

from gammad.broker.base import BrokerAdapter, BrokerAuthError, BrokerError
from gammad.core.h1zones import H1_HISTORY_START, Seed, load_seed
from gammad.core.h60_store import H60Store, as_date
from gammad.core.zones import ZoneParams
from gammad.log import get_logger

log = get_logger(__name__)

NO_DATA_CODE = "DH-907"
INTERVAL_MIN = 60


@dataclass
class SyncReport:
    calls: int = 0
    bars: int = 0
    ok: list[int] = field(default_factory=list)  # usids whose wanted range is fully held after the sync
    failed: dict[int, str] = field(default_factory=dict)  # usid -> reason
    empty: int = 0  # chunks answered empty (200 [] or DH-907), cached as empty

    def line(self) -> str:
        return (f"h60_sync: {len(self.ok)} ok, {len(self.failed)} failed, {self.calls} calls, {self.bars} bars, "
                f"{self.empty} empty chunks")


def history_root(settings: Any) -> Path:
    return Path(settings.paths.data_dir) / "history"


def default_start(settings: Any) -> date:
    v2 = getattr(settings, "v2_scanner", None)
    raw = getattr(v2, "h1_history_start", None) if v2 is not None else None
    return as_date(raw or H1_HISTORY_START)


def run_h60_sync(settings: Any, adapter: BrokerAdapter, usids: Iterable[int], through: date, *,
                 start: date | str | None = None, store: H60Store | None = None) -> SyncReport:
    store = store or H60Store(history_root(settings))
    a = as_date(start) if start is not None else default_start(settings)
    rep = SyncReport()
    ids = sorted({int(u) for u in usids})
    for idx, usid in enumerate(ids):
        try:
            for s, e in store.missing_ranges(usid, a, through):
                to = min(e, through)
                rep.calls += 1
                try:
                    candles = adapter.intraday(usid, "NSE_EQ", INTERVAL_MIN, s, to, oi=False, instrument="EQUITY")
                except BrokerAuthError:
                    raise
                except BrokerError as exc:
                    if str(exc.code or "") != NO_DATA_CODE:
                        raise
                    candles = []
                if not candles:
                    rep.empty += 1
                rep.bars += len(candles)
                store.write_chunk(usid, s, e, candles, through=through)
            rep.ok.append(usid)
        except BrokerAuthError as exc:
            for u in ids[idx:]:
                rep.failed[u] = f"auth: {exc}"
            log.error("h60_sync: broker refused the login (%s): sync stopped at %s; V2 runs on what is held", exc,
                      usid)
            break
        except (BrokerError, OSError, ValueError) as exc:
            rep.failed[usid] = f"{type(exc).__name__}: {exc}"
            log.warning("h60_sync: %s failed (%s); V2 runs on what is held", usid, exc)
    log.info(rep.line())
    return rep


NO_SEED = "no zone seed"
NOT_IN_SEED = "not in the zone seed"


def top_up(underlyings: Iterable[int], adapter: BrokerAdapter, *, through: date, settings: Any = None,
           store: H60Store | None = None, seed: Seed | None = None,
           params: ZoneParams | None = None) -> SyncReport:
    """D480 (3): fetch ONLY the missing 60-min bars after the zone seed's as_of for the given (carried) underlyings.
    `seed` None = h1zones.load_seed() (the packaged seed). Underlyings without a seed book (or every one, when there is
    no seed) land in report.failed and are never fetched. Same fetch / cache / error rules as run_h60_sync.
    `params` = the zone params the seed must match (the app passes Runtime._zone_params(), T-422);
    None = h1zones.default_params()."""
    if store is None:
        if settings is None:
            raise ValueError("top_up: pass settings or store")
        store = H60Store(history_root(settings))
    ids = sorted({int(u) for u in underlyings})
    seed = seed if seed is not None else load_seed(params=params)
    if seed is None:
        rep = SyncReport(failed={u: NO_SEED for u in ids})
        log.warning("h60_sync top-up: no zone seed: %d underlyings not topped up", len(ids))
        return rep
    held = [u for u in ids if u in seed.books]
    rep = run_h60_sync(settings, adapter, held, through, start=seed.as_of + timedelta(days=1), store=store)
    for u in ids:
        if u not in seed.books:
            rep.failed[u] = NOT_IN_SEED
    if len(held) < len(ids):
        log.warning("h60_sync top-up: %d of %d underlyings have no seed book (not fetched)", len(ids) - len(held),
                    len(ids))
    return rep
