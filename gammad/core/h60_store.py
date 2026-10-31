"""The 60-min stock bar store the 1h zones read (T-267, PLAN-T267 §1 "H60 store").

0.8.9 (T-419, D480 (3)): MOVED here from gammad/v2scan/h60_store.py (content unchanged; that module is now a shim that
IS this module) so a client install can top up its carried underlyings' 60-min bars (gammad.jobs.h60_sync.top_up).

Layout (the backtest's dhan_history cache layout, so the research store and the live store are interchangeable):

    <history>/intraday60/NSE_EQ-<usid>/<s>_<e>.parquet               a complete fixed-grid chunk
    <history>/intraday60/NSE_EQ-<usid>/<s>_<e>.asof-<D>.parquet      a chunk still forming: complete for days < D

Chunks sit on a FIXED 90-day calendar grid (day 0 = 2000-01-01; the Dhan intraday window), so every fetch of one
window lands in one file. Columns ts_ms, open, high, low, close, volume, oi, strike (Dhan's raw bars, unaligned).

rows(usid, start, last_day) = the research `t234_levels.load_h60` exactly: per chunk the complete file, else the newest
partial; raw bars aligned to the 09:15-anchored hourly slots (`h60_align`: ts floored to its slot, bars in one slot
merged open-first / max high / min low / close-last, slots before 09:15 or at / after 15:30 dropped, the 15:15 slot
closes at 15:30); rows [ts_open, o, h, l, c, close_ms, "h60"] with start 00:00 <= ts < (last_day + 1) 00:00 IST.
"""

from __future__ import annotations

import os
from collections.abc import Iterable, Sequence
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from gammad.core.h1zones import DAY_MS, H1_HISTORY_START, H_MS, SESSION_MIN, day_anchor, ist_ms
from gammad.log import get_logger

log = get_logger(__name__)

KIND = "intraday60"
GRID_EPOCH = date(2000, 1, 1)
CHUNK_DAYS = 90
CANDLE_SCHEMA = pa.schema(
    [
        ("ts_ms", pa.int64()),
        ("open", pa.float64()),
        ("high", pa.float64()),
        ("low", pa.float64()),
        ("close", pa.float64()),
        ("volume", pa.float64()),
        ("oi", pa.float64()),
        ("strike", pa.float64()),
    ]
)


def as_date(d: date | str) -> date:
    return d if isinstance(d, date) else date.fromisoformat(str(d)[:10])


def grid_chunks(a: date, b: date, span: int = CHUNK_DAYS) -> list[tuple[date, date]]:
    """Fixed-grid windows [s, s + span - 1] covering a..b (day 0 = GRID_EPOCH; dhan_history.grid_chunks)."""
    if b < a:
        return []
    k = (a - GRID_EPOCH).days // span
    out: list[tuple[date, date]] = []
    s = GRID_EPOCH + timedelta(days=k * span)
    while s <= b:
        out.append((s, s + timedelta(days=span - 1)))
        s += timedelta(days=span)
    return out


def _parse_name(p: Path) -> tuple[date, date, date | None] | None:
    """`<s>_<e>.parquet` -> (s, e, None); `<s>_<e>.asof-<D>.parquet` -> (s, e, D); anything else None."""
    if not p.name.endswith(".parquet"):
        return None
    rng, _, asof = p.name[: -len(".parquet")].partition(".asof-")
    try:
        s, e = (date.fromisoformat(x) for x in rng.split("_"))
        return s, e, (date.fromisoformat(asof[:10]) if asof else None)
    except ValueError:
        return None


def h60_align(raw: Iterable[Sequence[Any]]) -> list[list]:
    """Raw 60-min rows [ts, o, h, l, c, ...] -> one bar per 09:15-anchored hourly slot (t234_levels.h60_align)."""
    out: dict[int, list] = {}
    for b in sorted(raw, key=lambda x: int(x[0])):
        ts = int(b[0])
        a = day_anchor(ts)
        if ts < a:
            continue
        slot = a + ((ts - a) // H_MS) * H_MS
        if slot - a >= SESSION_MIN * 60_000:
            continue
        p = out.get(slot)
        if p is None:
            out[slot] = [slot, float(b[1]), float(b[2]), float(b[3]), float(b[4]),
                         min(slot + H_MS, a + SESSION_MIN * 60_000), "h60"]
        else:
            p[2], p[3], p[4] = max(p[2], float(b[2])), min(p[3], float(b[3])), float(b[4])
    return [out[k] for k in sorted(out)]


class H60Store:
    def __init__(self, root: Path | str) -> None:
        self.root = Path(root)
        self.base = self.root / KIND

    def folder(self, usid: int) -> Path:
        return self.base / f"NSE_EQ-{int(usid)}"

    def usids(self) -> list[int]:
        if not self.base.exists():
            return []
        out = []
        for p in self.base.iterdir():
            if p.is_dir() and p.name.startswith("NSE_EQ-"):
                try:
                    out.append(int(p.name[len("NSE_EQ-"):]))
                except ValueError:
                    continue
        return sorted(out)

    def files(self, usid: int) -> list[Path]:
        """One file per chunk: the complete file when present, else the newest partial (t234_levels.h60_files)."""
        best: dict[tuple[date, date], tuple[tuple[int, str], Path]] = {}
        folder = self.folder(usid)
        if not folder.exists():
            return []
        for p in folder.glob("*.parquet"):
            parsed = _parse_name(p)
            if parsed is None:
                continue
            s, e, asof = parsed
            rank = (1, "9999-99-99") if asof is None else (0, asof.isoformat())
            if (s, e) not in best or rank > best[(s, e)][0]:
                best[(s, e)] = (rank, p)
        return [best[k][1] for k in sorted(best)]

    def held_through(self, usid: int) -> date | None:
        """The last day the store holds COMPLETE (a complete chunk's end, a partial's as-of - 1), or None."""
        last: date | None = None
        for p in self.files(usid):
            parsed = _parse_name(p)
            if parsed is None:
                continue
            _s, e, asof = parsed
            d = e if asof is None else min(e, asof - timedelta(days=1))
            if last is None or d > last:
                last = d
        return last

    def raw(self, usid: int, lo_ms: int | None = None, hi_ms: int | None = None) -> list[tuple]:
        out: list[tuple] = []
        for p in self.files(usid):
            t = pq.read_table(p, columns=["ts_ms", "open", "high", "low", "close"])
            cols = [t.column(k).to_pylist() for k in ("ts_ms", "open", "high", "low", "close")]
            out += [r for r in zip(*cols, strict=True) if (lo_ms is None or lo_ms <= int(r[0]))
                    and (hi_ms is None or int(r[0]) < hi_ms)]
        return out

    def rows(self, usid: int, start: date | str = H1_HISTORY_START, last_day: date | str | None = None) -> list[list]:
        """Aligned rows [ts_open, o, h, l, c, close_ms, "h60"] from start through last_day (None = all held)."""
        lo = ist_ms(as_date(start), "00:00")
        hi = None if last_day is None else ist_ms(as_date(last_day), "00:00") + DAY_MS
        return [r for r in h60_align(self.raw(usid, lo, hi)) if lo <= r[0] and (hi is None or r[0] < hi)]

    def rows_after(self, usid: int, after_day: date | str | None, last_day: date | str | None = None,
                   *, start: date | str = H1_HISTORY_START) -> list[list]:
        """Rows of the days AFTER after_day (None = from start): what a restored seed book appends."""
        first = as_date(start) if after_day is None else as_date(after_day) + timedelta(days=1)
        return self.rows(usid, first, last_day)

    def missing_ranges(self, usid: int, start: date | str, through: date | str) -> list[tuple[date, date]]:
        """Grid chunks covering start..through with no usable file: a complete file covers its chunk; a partial
        covers it when its as-of is after min(chunk end, through)."""
        a, b = as_date(start), as_date(through)
        have: dict[tuple[date, date], list[date | None]] = {}
        folder = self.folder(usid)
        if folder.exists():
            for p in folder.glob("*.parquet"):
                parsed = _parse_name(p)
                if parsed is not None:
                    have.setdefault((parsed[0], parsed[1]), []).append(parsed[2])
        out = []
        for s, e in grid_chunks(a, b):
            asofs = have.get((s, e), [])
            need = min(e, b)
            if None in asofs or any(x is not None and x > need for x in asofs):
                continue
            out.append((s, e))
        return out

    def write_chunk(self, usid: int, s: date, e: date, candles: Sequence[Any], *, through: date) -> Path:
        """Write one fetched chunk (Candle objects or rows [ts, o, h, l, c, v, oi]). through >= e -> the complete
        file; else `<s>_<e>.asof-<through + 1>.parquet` (complete for days <= through). An empty list is written
        too (an empty HTTP 200 = before the first bar: cached, never refetched). Superseded partials are removed."""
        folder = self.folder(usid)
        folder.mkdir(parents=True, exist_ok=True)
        complete = through >= e
        name = f"{s.isoformat()}_{e.isoformat()}" + ("" if complete else f".asof-{(through + timedelta(days=1)).isoformat()}")
        path = folder / f"{name}.parquet"
        recs: list[dict[str, Any]] = []
        for c in candles:
            if hasattr(c, "ts_ms"):
                recs.append({"ts_ms": int(c.ts_ms), "open": c.open, "high": c.high, "low": c.low, "close": c.close,
                             "volume": c.volume, "oi": c.oi, "strike": None})
            else:
                recs.append({"ts_ms": int(c[0]), "open": c[1], "high": c[2], "low": c[3], "close": c[4],
                             "volume": c[5] if len(c) > 5 else None, "oi": c[6] if len(c) > 6 else None,
                             "strike": None})
        table = pa.Table.from_pydict({f.name: [r.get(f.name) for r in recs] for f in CANDLE_SCHEMA},
                                     schema=CANDLE_SCHEMA)
        tmp = path.with_name(path.name + ".tmp")
        pq.write_table(table, tmp)
        os.replace(tmp, path)
        for p in folder.glob(f"{s.isoformat()}_{e.isoformat()}.asof-*.parquet"):
            if p != path:
                parsed = _parse_name(p)
                if complete or (parsed and parsed[2] is not None and parsed[2] < through + timedelta(days=1)):
                    try:
                        p.unlink()
                    except OSError as exc:  # a reader holds it: the newest file still wins (files())
                        log.info("h60_store: could not remove superseded %s (%s)", p, exc)
        return path
