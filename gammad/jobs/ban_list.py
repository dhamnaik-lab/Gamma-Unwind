"""0.8.9 T-419 (D480 (2), spec 4.8): the NSE F&O ban list for trade date D, fetched once a day by EVERY install.

NSE publishes `fo_secban_<DDMMYYYY>.csv` "For Trade Date D" on the evening of D-1. The daily job (~08:45 IST on D,
still_watching.ban_list.fetch_hm) reads ONLY D's own file (no look-ahead, nothing later than D) and keeps a BanList:
  status OK       the file was read and its header names trade date D: `symbols` = the stocks in the ban period
                  ("NIL" = none);
  status UNKNOWN  anything else -- no answer, HTTP != 200, an HTML error page, a truncated / unparseable file, a header
                  for another date. FAIL CLOSED: a Still Watching entry needs status OK and the stock not banned
                  (gammad.watch.engine.sw_entry_block); UNKNOWN = no Still Watching entry that day.
Politeness: ONE request, plus ONE retry after `retry_wait_s` when the first fails; the NSE request headers of the owner's
history tool; redirects refused (only the archive host).

PROVENANCE (ported, not imported -- tools/ is not in the client package): tools/history/nse_files.py HOST (:39), the
`fo_ban` PATTERN (:42), url_for's compact date (:61-63), UA (:53), transport + ArchiveOnly + read_response
(:390-428), the HTML / terminal-newline traps of _validate (:296-302, :346-347) and its ban header check (:382-383);
backtest/lab/wf/labels.py _ban_symbol (:244-252) and read_bans' header regex (:273-284).
"""

from __future__ import annotations

import re
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from typing import Any, NamedTuple
from urllib.parse import urlsplit

import httpx

from gammad.log import get_logger

log = get_logger(__name__)

HOST = "archives.nseindia.com"  # nse_files.py:39
PATTERN = "archives/fo/sec_ban/fo_secban_{compact}.csv"  # nse_files.py:42 PATTERNS["fo_ban"]
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/130.0 Safari/537.36"  # nse_files.py:53
MAX_BYTES = 1024 * 1024  # a ban list is a few hundred bytes; anything near this is not one
TIMEOUT_S = 30.0
RETRY_WAIT_S = 5.0
STATUS_OK = "OK"  # = gammad.watch.engine.SW_BAN_OK
STATUS_UNKNOWN = "UNKNOWN"  # = gammad.watch.engine.SW_BAN_UNKNOWN
_HEADER = re.compile(r"Securities in Ban For Trade Date (\d{2}-[A-Za-z]{3}-\d{4}):\s*(NIL)?\s*")  # labels.py:273


class BanParseError(ValueError):
    """The body is not the ban list of the asked trade date (the reason is the message)."""


class FetchResult(NamedTuple):  # = nse_files.FetchResult
    status: int
    body: bytes
    content_type: str
    reason: str = ""


@dataclass(frozen=True)
class BanList:
    """The F&O ban list of ONE trade date. `symbols` is meaningful only with status OK."""

    date: date
    status: str
    symbols: frozenset[str] = field(default_factory=frozenset)
    reason: str | None = None
    fetched_ms: int | None = None

    @property
    def ok(self) -> bool:
        return self.status == STATUS_OK

    def banned(self, symbol: str) -> bool:
        return str(symbol).strip().upper() in self.symbols

    def to_dict(self) -> dict[str, Any]:
        return {"date": self.date.isoformat(), "status": self.status, "symbols": sorted(self.symbols),
                "reason": self.reason, "fetched_ms": self.fetched_ms}  # fmt: skip


def unknown(day: date, reason: str) -> BanList:
    return BanList(day, STATUS_UNKNOWN, frozenset(), reason)


def url_for(day: date) -> str:
    """nse_files.url_for('fo_ban', day): https://archives.nseindia.com/archives/fo/sec_ban/fo_secban_DDMMYYYY.csv."""
    return f"https://{HOST}/" + PATTERN.format(compact=day.strftime("%d%m%Y"))


def _symbol(line: str) -> str | None:
    """labels._ban_symbol: the SYMBOL of a body line `n,SYMBOL` (trailing empty fields dropped); None = unparseable."""
    parts = line.split(",")
    while len(parts) > 2 and not parts[-1].strip():
        parts.pop()
    if len(parts) == 2 and parts[0].strip().isdigit() and parts[1].strip():
        return parts[1].strip().upper()
    return None


def parse(body: bytes, day: date, content_type: str = "") -> frozenset[str]:
    """The banned symbols of trade date `day` from a fo_secban body. Raises BanParseError on anything that is not
    exactly that file (an HTML page, a truncated body, another date's header, a line that is not `n,SYMBOL`)."""
    if not body:
        raise BanParseError("zero-byte file")
    head = body.removeprefix(b"\xef\xbb\xbf").lstrip()[:512].lower()
    if "html" in content_type.lower() or head.startswith((b"<!doctype html", b"<html", b"<head", b"<body", b"<!--",
                                                          b"<?xml")):  # fmt: skip
        raise BanParseError("HTML error page")
    if not body.endswith(b"\n"):
        raise BanParseError("truncated CSV: missing terminal newline")
    try:
        text = body.decode("utf-8-sig")
    except UnicodeDecodeError:
        text = body.decode("cp1252")
    lines = [ln for ln in text.splitlines() if ln.strip()]
    m = _HEADER.fullmatch(lines[0].strip()) if lines else None
    if m is None:
        raise BanParseError("unexpected ban-list header")
    try:
        header_day = datetime.strptime(m[1], "%d-%b-%Y").replace(tzinfo=UTC).date()  # a date only (labels.py:276)
    except ValueError as exc:
        raise BanParseError(f"unreadable header date {m[1]!r}") from exc
    if header_day != day:
        raise BanParseError(f"header date {header_day.isoformat()} is not trade date {day.isoformat()}")
    if m[2]:  # "NIL": no stock in the ban period
        if len(lines) > 1:
            raise BanParseError("NIL header followed by rows")
        return frozenset()
    out: set[str] = set()
    for ln in lines[1:]:
        sym = _symbol(ln)
        if sym is None:
            raise BanParseError(f"unparseable row {ln.strip()[:40]!r}")
        out.add(sym)
    return frozenset(out)


def _read_httpx(response: httpx.Response) -> FetchResult:
    content_type = response.headers.get("Content-Type", "")
    chunks: list[bytes] = []
    size = 0
    for chunk in response.iter_bytes():
        size += len(chunk)
        if size > MAX_BYTES:
            return FetchResult(0, b"", content_type, "oversized")
        chunks.append(chunk)
    body = b"".join(chunks)
    length = response.headers.get("Content-Length")
    if length is not None and not response.headers.get("Content-Encoding"):
        try:
            declared = int(length)
        except (TypeError, ValueError):
            return FetchResult(0, body, content_type, "invalid Content-Length")
        if declared != len(body):
            return FetchResult(0, body, content_type, "truncated")
    return FetchResult(response.status_code, body, content_type)


def transport(url: str) -> FetchResult:
    """One GET with NSE browser-compatible headers over HTTPX; never raises."""
    parsed = urlsplit(url)
    if parsed.scheme != "https" or parsed.hostname != HOST:
        return FetchResult(0, b"", "", "host refused")
    headers = {
        "User-Agent": UA,
        "Accept": "text/csv,text/plain,*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer": f"https://{HOST}/",
    }
    try:
        with httpx.Client(timeout=TIMEOUT_S, follow_redirects=False, headers=headers) as client:
            with client.stream("GET", url) as response:
                if 300 <= response.status_code < 400:
                    return FetchResult(0, b"", "", "redirect refused; only the NSE archive host is fetched")
                return _read_httpx(response)
    except httpx.HTTPError as exc:
        return FetchResult(0, b"", "", "transport failure: " + type(exc).__name__)
    except OSError as exc:
        return FetchResult(0, b"", "", "transport failure: " + type(exc).__name__)


def fetch_ban_list(
    day: date,
    *,
    fetch: Callable[[str], FetchResult] = transport,
    sleep: Callable[[float], None] = time.sleep,
    retry_wait_s: float = RETRY_WAIT_S,
    clock_ms: Callable[[], int] | None = None,
) -> BanList:
    """The ban list of trade date `day` (module doc). One request + one retry; NEVER raises: every failure is a
    BanList with status UNKNOWN and the reason (fail closed)."""
    url = url_for(day)
    reason = "not fetched"
    for attempt in (1, 2):
        if attempt == 2:
            sleep(retry_wait_s)
        try:
            res = fetch(url)
        except Exception as exc:  # noqa: BLE001 -- a fetch fault is an UNKNOWN list, never the job
            res = FetchResult(0, b"", "", f"transport failure: {type(exc).__name__}")
        if res.status != 200:
            reason = f"HTTP {res.status}" + (f" ({res.reason})" if res.reason else "")
            continue
        try:
            symbols = parse(res.body, day, res.content_type)
        except BanParseError as exc:
            reason = str(exc)
            continue
        now = clock_ms() if clock_ms is not None else time.time_ns() // 1_000_000
        out = BanList(day, STATUS_OK, symbols, None, now)
        log.info("ban list %s: %d stock(s) in the F&O ban period%s", day.isoformat(), len(symbols),
                 f" ({', '.join(sorted(symbols))})" if symbols else "")  # fmt: skip
        return out
    log.warning("ban list %s: UNKNOWN after 2 attempts (%s); no Still Watching entry today (fail closed)",
                day.isoformat(), reason)  # fmt: skip
    return unknown(day, reason)
