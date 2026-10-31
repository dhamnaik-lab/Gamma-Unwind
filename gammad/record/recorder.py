"""Tick recorder (D10): SQLite `oi_series` + a parquet file per contract per trade date.

Never blocks the feed: `submit()` is a put_nowait onto the recorder's own bounded queue (a full queue
drops and counts, it never waits), and every disk write runs on ONE worker thread so the SQLite
connection is always used from the thread that opened it.

- SQLite: `INSERT OR IGNORE INTO oi_series (ts_ms, security_id, ltp, volume, oi)` every 1 s or 500 rows.
  `volume` is the cumulative day volume as sent by the exchange. The primary key is (ts_ms, security_id)
  and a quote/full tick's ts_ms is the exchange LTT, which has 1-second resolution, so several trades in
  the same second collapse to the FIRST one in SQLite. The parquet file keeps every tick.
- Parquet: `data/parquet/YYYY-MM-DD/ticks-<security_id>.parquet`, schema TICK_SCHEMA, flushed every 60 s
  and by `close_day()`. Each flush rewrites the file atomically (read + append + temp file + os.replace),
  so a crash never leaves a footer-less, unreadable file. A failed write keeps its rows for the next flush.
- `bars-5m.parquet` in the same folder: every closed 5m bar handed to `submit_bar()`, all contracts.
  T-102: two nullable columns eng_oi_now / eng_oi_peak hold exactly the oi_now / oi_peak the live engine was handed
  at that close (null = no engine call, or a file written before 0.5.1); an older file reads with them null.
The day folder of a tick is the IST date it ARRIVED on (`tick.recv_ms`, the feed's receive clock; the recorder's
wall clock when a tick carries none), never its exchange last-trade date (0.8.5, D274): a contract that has not
traded yet today reports the previous session's last-trade time, and filing by that time appended a volume-0 row to
the PAST day's file on every flush. A tick whose last-trade date is earlier than its arrival date is STALE: written to
no tick file and not to oi_series, counted in `stats.stale_skipped`. A genuine tick keeps its exchange time in ts_ms.
`stale_tail_start` / `only_stale_rows` are the ONE description of the rows the old recorder left in past folders (the
readers ignore them; tools/repair_stale_ticks.py removes them).

The session tripwire (T-357 (c)(2)) counts F&O exchange ticks and bars outside the dated session,
logging once per date for each kind. Flagged data is still buffered and stored unchanged.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import sqlite3
import time
from collections.abc import Callable, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from gammad.config import IST
from gammad.core import sessions
from gammad.core.sessions import Era, SessionsError
from gammad.log import get_logger
from gammad.record.stale import arrival_ms, is_stale, ist_date
from gammad.store import db
from gammad.watch.bars import Bar
from gammad.watch.feed import Tick

log = get_logger(__name__)
FO_SEGMENT = "NSE_FNO"

TICK_SCHEMA = pa.schema(
    [
        ("ts_ms", pa.int64()),
        ("ltp", pa.float64()),
        ("volume", pa.int64()),
        ("oi", pa.int64()),
        ("prev_oi", pa.int64()),
    ]
)
BAR_SCHEMA = pa.schema(
    [
        ("security_id", pa.int64()),
        ("start_ms", pa.int64()),
        ("tf_min", pa.int32()),
        ("open", pa.float64()),
        ("high", pa.float64()),
        ("low", pa.float64()),
        ("close", pa.float64()),
        ("volume", pa.int64()),
        ("oi", pa.int64()),
        ("eng_oi_now", pa.float64()),  # T-102: the engine's inputs at this close (nullable)
        ("eng_oi_peak", pa.float64()),
    ]
)
INSERT_SQL = "INSERT OR IGNORE INTO oi_series (ts_ms, security_id, ltp, volume, oi) VALUES (?, ?, ?, ?, ?)"

DbRow = tuple[int, int, float | None, int | None, int | None]
TickRow = tuple[int, float | None, int | None, int | None, int | None]
BarRow = tuple[int, int, int, float, float, float, float, int, int | None, float | None, float | None]


def stale_tail_start(ts: Sequence[int], volume: Sequence[int | None]) -> int:
    """Where a tick file's STALE TAIL starts, in recorded (file) order; len(ts) = no stale tail.

    The old recorder's signature (D274, measured on 25 / 29 / 30 Sep and 1 Oct: every stale row has volume 0 and ONE
    repeated timestamp, appended after the day's genuine rows): the trailing run of rows whose volume is exactly 0,
    all sharing one timestamp, after at least one positive-volume row. None (a packet without a volume) is never read
    as 0, and a file with no positive-volume row has no tail by this rule (see `only_stale_rows`)."""
    n = k = len(volume)
    while k > 0 and volume[k - 1] == 0:
        k -= 1
    if k == n or len({ts[i] for i in range(k, n)}) != 1:
        return n
    return k if any(v is not None and v > 0 for v in volume[:k]) else n


def only_stale_rows(ts: Sequence[int], volume: Sequence[int | None]) -> bool:
    """A tick file made ONLY of stale rows (D274: 4 of the 15 rewritten 1 Oct files exist only because of them):
    at least one row, every volume exactly 0, one timestamp (a genuine zero-volume stream, e.g. an index, advances)."""
    return bool(volume) and all(v == 0 for v in volume) and len(set(ts)) == 1


@dataclass
class RecorderStats:
    ticks_in: int = 0
    dropped: int = 0
    db_rows_written: int = 0
    db_rows_ignored: int = 0
    parquet_rows_written: int = 0
    write_errors: int = 0  # failing flushes (sqlite batches + parquet flushes), as before T-284
    # T-284 (3): the health block's counts. parquet_failed = FILES that failed (tick files + bars files, per flush);
    # db_failed = sqlite BATCHES that failed; last_failed_ms = wall-clock ms of the latest failure of either kind
    parquet_failed: int = 0
    db_failed: int = 0
    last_failed_ms: int | None = None
    # 0.8.5 (D274): ticks whose last-trade date is earlier than their arrival date, written nowhere
    stale_skipped: int = 0
    off_session_ticks: int = 0
    off_session_bars: int = 0
    by_date: dict[str, int] = field(default_factory=dict)  # ARRIVAL date -> ticks filed


def _wall_ms() -> int:
    return int(time.time() * 1000)


def _atomic_write(table: pa.Table, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    pq.write_table(table, tmp)
    os.replace(tmp, path)


class Recorder:
    # T-102: submit_bar takes the engine's inputs (the app hands them only to a recorder that declares this)
    records_engine_inputs = True

    def __init__(
        self,
        db_path: Path | str,
        parquet_root: Path | str,
        *,
        db_batch_rows: int = 500,
        db_interval_s: float = 1.0,
        parquet_interval_s: float = 60.0,
        queue_max: int = 200_000,
        clock: Callable[[], float] = time.monotonic,
        wall_ms: Callable[[], int] = _wall_ms,
        eras: Sequence[Era] | None = None,
    ) -> None:
        self.db_path = Path(db_path)
        # T-284 (3): stamps stats.last_failed_ms (the monotonic clock is not a time of day)
        self._wall_ms = wall_ms
        self.parquet_root = Path(parquet_root)
        self.db_batch_rows = db_batch_rows
        self.db_interval_s = db_interval_s
        self.parquet_interval_s = parquet_interval_s
        self._clock = clock
        self.queue: asyncio.Queue[Tick] = asyncio.Queue(maxsize=queue_max)
        self.stats = RecorderStats()
        self._db_pending: list[DbRow] = []
        self._pq_pending: dict[tuple[str, int], list[TickRow]] = {}
        self._bars: dict[str, list[BarRow]] = {}
        self._bars_dirty: set[str] = set()
        self._conn: sqlite3.Connection | None = None
        self._executor: ThreadPoolExecutor | None = None
        self._last_db = clock()
        self._last_pq = clock()
        self._eras = ()
        try:
            self._eras = sessions.default_eras() if eras is None else eras
        except FileNotFoundError as exc:
            log.info("session tripwire off: config/market_sessions.yaml not read (%s)", type(exc).__name__)
        except Exception as exc:  # noqa: BLE001 - detection must not prevent recording
            log.warning("session tripwire off: config/market_sessions.yaml not read (%s)", type(exc).__name__)
        self._windows: dict[str, tuple[int, int, Era] | None] = {}
        self._tripped: set[tuple[str, str]] = set()

    def _window(self, day: str) -> tuple[int, int, Era] | None:
        if day not in self._windows:
            self._windows[day] = None
            if self._eras:
                d = date.fromisoformat(day)
                try:
                    era = sessions.era_for(d, self._eras)
                except SessionsError:
                    return None
                open_ms = int(datetime.combine(d, era.fo_open, IST).timestamp() * 1000)
                close_ms = int(datetime.combine(d, era.fo_close, IST).timestamp() * 1000)
                self._windows[day] = (open_ms, close_ms, era)
        return self._windows[day]

    # ---------------------------------------------------------------- intake (event-loop side, never blocks)
    def submit(self, tick: Tick) -> bool:
        try:
            self.queue.put_nowait(tick)
            return True
        except asyncio.QueueFull:
            self.stats.dropped += 1
            return False

    def add(self, tick: Tick) -> None:
        """Buffer one tick (synchronous core of the consumer), filed under its ARRIVAL date; a stale tick is dropped."""
        self.stats.ticks_in += 1
        recv = arrival_ms(tick.recv_ms, self._wall_ms)
        d = ist_date(recv)
        if is_stale(tick.ts_ms, d):
            self.stats.stale_skipped += 1
            return
        if tick.segment == FO_SEGMENT and tick.ts_ms > 0 and tick.ts_ms != tick.recv_ms:
            day = ist_date(tick.ts_ms)
            window = self._window(day)
            if window is not None:
                open_ms, close_ms, era = window
                if tick.ts_ms < open_ms or tick.ts_ms >= close_ms + 1000:
                    self.stats.off_session_ticks += 1
                    if (day, "tick") not in self._tripped:
                        self._tripped.add((day, "tick"))
                        log.warning(
                            "tripwire: F&O tick at %s IST on %s is outside the configured session %s-%s "
                            "(market_sessions era %s); check for an NSE rule change",
                            datetime.fromtimestamp(tick.ts_ms / 1000, IST).strftime("%H:%M:%S"), day,
                            era.fo_open.strftime("%H:%M"), era.fo_close.strftime("%H:%M"), era.era_id,
                        )
        self._db_pending.append((tick.ts_ms, tick.security_id, tick.ltp, tick.volume_day, tick.oi))
        self._pq_pending.setdefault((d, tick.security_id), []).append(
            (tick.ts_ms, tick.ltp, tick.volume_day, tick.oi, tick.prev_oi)
        )
        self.stats.by_date[d] = self.stats.by_date.get(d, 0) + 1

    def submit_bar(
        self, security_id: int, bar: Bar, *, eng_oi_now: float | None = None, eng_oi_peak: float | None = None
    ) -> None:
        """A CLOSED 5m bar (the caller filters on tf); written to bars-5m.parquet. T-102: `eng_oi_now` /
        `eng_oi_peak` = what the live engine was handed at this close (None = no engine call). Filed by the bar's own
        start (D274 cannot reach a bar: BarBuilder.on_tick drops any tick stamped before its day's open)."""
        d = ist_date(bar.start_ms)
        self._bars.setdefault(d, []).append(
            (
                security_id,
                bar.start_ms,
                bar.tf_min,
                bar.open,
                bar.high,
                bar.low,
                bar.close,
                bar.volume,
                bar.oi,
                None if eng_oi_now is None else float(eng_oi_now),
                None if eng_oi_peak is None else float(eng_oi_peak),
            )
        )
        self._bars_dirty.add(d)
        window = self._window(d)
        if window is not None:
            open_ms, close_ms, era = window
            if bar.start_ms < open_ms or bar.start_ms >= close_ms:
                self.stats.off_session_bars += 1
                if (d, "bar") not in self._tripped:
                    self._tripped.add((d, "bar"))
                    log.warning(
                        "tripwire: %sm bar at %s IST on %s is outside the configured session %s-%s "
                        "(market_sessions era %s); check for an NSE rule change",
                        bar.tf_min, datetime.fromtimestamp(bar.start_ms / 1000, IST).strftime("%H:%M"), d,
                        era.fo_open.strftime("%H:%M"), era.fo_close.strftime("%H:%M"), era.era_id,
                    )

    # ---------------------------------------------------------------- synchronous flushes (tests + worker)
    def _db(self) -> sqlite3.Connection:
        if self._conn is None:
            self._conn = db.connect(self.db_path)
        return self._conn

    def _write_db(self, rows: list[DbRow]) -> int:
        conn = self._db()
        before = conn.total_changes
        conn.executemany(INSERT_SQL, rows)
        conn.commit()
        return conn.total_changes - before

    def _take_db(self) -> list[DbRow]:
        rows, self._db_pending = self._db_pending, []
        return rows

    def _count_db_failure(self) -> None:
        self.stats.write_errors += 1
        self.stats.db_failed += 1
        self.stats.last_failed_ms = self._wall_ms()

    def _account_db(self, rows: list[DbRow], written: int) -> None:
        self.stats.db_rows_written += written
        self.stats.db_rows_ignored += len(rows) - written

    def flush_db(self) -> int:
        rows = self._take_db()
        if not rows:
            return 0
        try:
            written = self._write_db(rows)
        except Exception:
            self._db_pending[:0] = rows
            self._count_db_failure()
            raise
        self._account_db(rows, written)
        return written

    def _take_parquet(self) -> tuple[dict[tuple[str, int], list[TickRow]], dict[str, list[BarRow]]]:
        ticks, self._pq_pending = self._pq_pending, {}
        bars = {d: list(self._bars[d]) for d in self._bars_dirty}
        self._bars_dirty = set()
        return ticks, bars

    def _write_parquet(
        self, ticks: dict[tuple[str, int], list[TickRow]], bars: dict[str, list[BarRow]]
    ) -> tuple[int, dict[tuple[str, int], list[TickRow]], set[str]]:
        """Returns (rows written, tick groups that failed, bar dates that failed)."""
        written = 0
        failed: dict[tuple[str, int], list[TickRow]] = {}
        failed_bars: set[str] = set()
        for (d, sid), rows in ticks.items():
            path = self.parquet_root / d / f"ticks-{sid}.parquet"
            try:
                cols = list(zip(*rows, strict=True))
                new = pa.Table.from_arrays(
                    [pa.array(c, type=f.type) for c, f in zip(cols, TICK_SCHEMA, strict=True)],
                    schema=TICK_SCHEMA,
                )
                table = (
                    pa.concat_tables([pq.read_table(path, schema=TICK_SCHEMA), new]) if path.exists() else new
                )
                _atomic_write(table, path)
                written += len(rows)
            except Exception:
                log.exception("recorder: parquet write failed for %s", path)
                failed[d, sid] = rows
        for d, rows in bars.items():
            path = self.parquet_root / d / "bars-5m.parquet"
            try:
                if path.exists():  # a restarted process must not erase the morning's bars (T-086): merge the file
                    ours = {(r[0], r[1], r[2]) for r in rows}
                    kept = [
                        tuple(r[f.name] for f in BAR_SCHEMA)
                        for r in pq.read_table(path, schema=BAR_SCHEMA).to_pylist()
                        if (r["security_id"], r["start_ms"], r["tf_min"]) not in ours
                    ]
                    rows = sorted([*kept, *rows], key=lambda r: (r[1], r[0], r[2]))
                cols = list(zip(*rows, strict=True))
                table = pa.Table.from_arrays(
                    [pa.array(c, type=f.type) for c, f in zip(cols, BAR_SCHEMA, strict=True)],
                    schema=BAR_SCHEMA,
                )
                _atomic_write(table, path)
            except Exception:
                log.exception("recorder: bar parquet write failed for %s", path)
                failed_bars.add(d)
        return written, failed, failed_bars

    def _requeue_parquet(self, failed: dict[tuple[str, int], list[TickRow]], failed_bars: set[str]) -> None:
        for k, rows in failed.items():
            self._pq_pending[k] = rows + self._pq_pending.get(k, [])
        self._bars_dirty |= failed_bars
        if failed or failed_bars:
            self.stats.write_errors += 1
            self.stats.parquet_failed += len(failed) + len(failed_bars)  # T-284 (3): one per FILE that failed
            self.stats.last_failed_ms = self._wall_ms()

    def flush_parquet(self) -> int:
        ticks, bars = self._take_parquet()
        written, failed, failed_bars = self._write_parquet(ticks, bars)
        self._requeue_parquet(failed, failed_bars)
        self.stats.parquet_rows_written += written
        return written

    def close_day(self, trade_date: str | None = None) -> None:
        """Flush everything, then forget the in-memory bars of `trade_date` (or of every date)."""
        self.flush_db()
        self.flush_parquet()
        for d in [trade_date] if trade_date else list(self._bars):
            self._bars.pop(d, None)
            self._bars_dirty.discard(d)

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    # ---------------------------------------------------------------- async consumer
    async def run(self, stop: asyncio.Event) -> None:
        """Consume the queue until `stop` is set, then drain, flush and close. Run as its own task."""
        loop = asyncio.get_running_loop()
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="recorder")
        try:
            while not stop.is_set():
                with contextlib.suppress(TimeoutError):
                    tick = await asyncio.wait_for(self.queue.get(), timeout=0.25)
                    self.add(tick)
                    while not self.queue.empty() and len(self._db_pending) < self.db_batch_rows:
                        self.add(self.queue.get_nowait())
                await self._maybe_flush(loop)
            while not self.queue.empty():
                self.add(self.queue.get_nowait())
            await loop.run_in_executor(self._executor, self.close_day)
            await loop.run_in_executor(self._executor, self.close)
        finally:
            if self._conn is not None:  # T-397: a failed drain still closes the store connection, on ITS thread
                try:
                    await loop.run_in_executor(self._executor, self.close)
                except Exception:  # logged; the stop goes on
                    log.exception("recorder: store close failed")
            self._executor.shutdown(wait=True)
            self._executor = None

    async def _maybe_flush(self, loop: asyncio.AbstractEventLoop) -> None:
        assert self._executor is not None
        now = self._clock()
        if self._db_pending and (
            len(self._db_pending) >= self.db_batch_rows or now - self._last_db >= self.db_interval_s
        ):
            rows = self._take_db()
            self._last_db = now
            try:
                written = await loop.run_in_executor(self._executor, self._write_db, rows)
                self._account_db(rows, written)
            except Exception:
                log.exception("recorder: sqlite batch failed; %d rows kept for retry", len(rows))
                self._db_pending[:0] = rows
                self._count_db_failure()
        if now - self._last_pq >= self.parquet_interval_s:
            self._last_pq = now
            ticks, bars = self._take_parquet()
            if ticks or bars:
                written, failed, failed_bars = await loop.run_in_executor(
                    self._executor, self._write_parquet, ticks, bars
                )
                self._requeue_parquet(failed, failed_bars)
                self.stats.parquet_rows_written += written
