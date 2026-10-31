"""T-170 (PLAN-W17 §2.2): a contract's bars for a day, REPLAYED from what the recorder wrote.

Used by GET /api/contract/{id} when the runtime has no live bars for the id (its BarBuilders exist only for today's
list rows, Runtime.builders in gammad/app.py, so an open trade whose contract left today's lists drew an empty
chart) or when the asked date is not today (Runtime.contract_view ignores the date).

Sources, in order (gammad/record/recorder.py): `<parquet_root>/<date>/ticks-<sid>.parquet` (every tick), then the
`oi_series` rows stamped AFTER the last parquet tick (the parquet file is flushed every 60 s; oi_series every 1 s, so
the tail of the day may exist only there). Each row becomes a feed Tick and goes through the pure
gammad.watch.bars.BarBuilder -- the live bar rules, not a second implementation. `volume` in both stores is the
CUMULATIVE day volume (recorder.py:8): the builder turns it into per-bar deltas (the first tick seeds); it is never
summed or grouped raw. Same bar shape as the live view: {ts_ms, open, high, low, close, volume, oi}, the forming bar
last. Read-only: a missing or unreadable file is simply no ticks.

TODAY's tick file is NEVER opened (`use_parquet=False`, D142): the live recorder rewrites it every 60 s with
os.replace, which fails on Windows while any reader holds the file, and that failure is an ERROR line in the live
log. Today's bars come from oi_series alone (one row per second: a same-second tick is not seen, so a bar's high /
low can miss a print inside a second). A past day's file is closed and is read in full.
"""

from __future__ import annotations

import sqlite3
from datetime import date, datetime, time
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from gammad.config import IST
from gammad.core import session_clock
from gammad.log import get_logger
from gammad.record.recorder import only_stale_rows, stale_tail_start
from gammad.watch.bars import BarBuilder

log = get_logger(__name__)

TICK_COLUMNS = ("ts_ms", "ltp", "volume", "oi", "prev_oi")
SEGMENT = "NSE_FNO"


def _day_bounds_ms(d: date) -> tuple[int, int]:
    start = int(datetime.combine(d, time(0, 0), tzinfo=IST).timestamp() * 1000)
    return start, start + 86_400_000


def _parquet_rows(path: Path) -> list[tuple[int, Any, Any, Any, Any]]:
    if not path.is_file():
        return []
    try:
        # read_bytes: the handle is held for one read, not for the parse -- on Windows the recorder's os.replace of
        # this file fails while any reader holds it open (its flush then retries a minute later)
        table = pq.read_table(pa.BufferReader(path.read_bytes()), columns=list(TICK_COLUMNS))
    except (OSError, ValueError, pa.ArrowException) as exc:  # a file mid-rewrite / damaged: oi_series still answers
        log.warning("contract bars: %s unreadable (%s); oi_series only", path.name, type(exc).__name__)
        return []
    cols = [table.column(c).to_pylist() for c in TICK_COLUMNS]
    return [r for r in zip(*cols, strict=True) if r[0] is not None]  # RECORDED order (the stale rule reads it)


def _drop_stale_tail(rows: list[tuple[Any, ...]], *, whole_file: bool) -> tuple[list[tuple[Any, ...]], int]:
    ts, volume = [int(r[0]) for r in rows], [r[2] for r in rows]
    if whole_file and only_stale_rows(ts, volume):
        return [], len(rows)
    k = stale_tail_start(ts, volume)
    return rows[:k], len(rows) - k


def recorded_ticks(
    parquet_root: Path, conn: sqlite3.Connection, security_id: int, d: date, *, use_parquet: bool = True
) -> list[tuple[Any, ...]]:
    """(ts_ms, ltp, cumulative volume, oi, prev_oi) rows of `d`, oldest first: the parquet ticks, then oi_series rows
    after the last of them (all of that day's oi_series rows when there is no parquet file).

    0.8.5 (D274): the rows a pre-0.8.5 recorder appended to a PAST day (a later session's stale quote: volume 0, one
    repeated last-trade time, recorder.stale_tail_start) and any row stamped outside `d` are ignored and counted, in
    the file and in the oi_series tail (oi_series kept the first stale row of a second it had not recorded)."""
    lo, hi = _day_bounds_ms(d)
    path = parquet_root / d.isoformat() / f"ticks-{int(security_id)}.parquet"
    raw = _parquet_rows(path) if use_parquet else []
    kept, stale = _drop_stale_tail(raw, whole_file=True)
    rows = [r for r in kept if lo <= int(r[0]) < hi]
    stale += len(kept) - len(rows)
    rows.sort(key=lambda r: r[0])  # stable: same-stamp ticks keep their recorded order
    after = int(rows[-1][0]) if rows else lo - 1
    tail = conn.execute(
        "SELECT ts_ms, ltp, volume, oi FROM oi_series WHERE security_id = ? AND ts_ms > ? AND ts_ms < ? ORDER BY ts_ms",
        (int(security_id), after, hi),
    ).fetchall()
    out, tail_stale = _drop_stale_tail(
        [*rows, *((int(t[0]), t[1], t[2], t[3], None) for t in tail)], whole_file=use_parquet
    )
    if stale + tail_stale:
        # one contract's file per call (its oi_series tail included): M is 1 here, the line keeps the day-level wording
        log.info("recorded ticks %s: %d stale rows ignored in 1 files", d.isoformat(), stale + tail_stale)
    return out


def replay_bars(
    parquet_root: Path, conn: sqlite3.Connection, security_id: int, d: date, tf: int, *, use_parquet: bool = True
) -> list[dict[str, Any]]:
    """`tf`-minute bars of `security_id` on `d` from the recorded ticks (closed bars, then the forming one)."""
    from gammad.watch.feed import Tick

    sid = int(security_id)
    o, c = session_clock.fo_window(d)
    builder = BarBuilder(sid, d, (tf,), quiet=True, session_open=o, session_close=c)
    for ts, ltp, vol, oi, prev_oi in recorded_ticks(parquet_root, conn, sid, d, use_parquet=use_parquet):
        builder.on_tick(
            Tick(
                ts_ms=int(ts), recv_ms=int(ts), security_id=sid, segment=SEGMENT,
                ltp=None if ltp is None else float(ltp), volume_day=None if vol is None else int(vol),
                oi=None if oi is None else int(oi), prev_oi=None if prev_oi is None else int(prev_oi),
                prev_close=None, oi_day_high=None, oi_day_low=None,
            )
        )  # fmt: skip
    forming = builder.forming_bar(tf)
    return [
        {"ts_ms": b.start_ms, "open": b.open, "high": b.high, "low": b.low, "close": b.close, "volume": b.volume,
         "oi": b.oi}
        for b in [*builder.series(tf), *([forming] if forming is not None else [])]
    ]  # fmt: skip
