"""G5 results calendar, PORTED (sync, D363) from the owner's market-data bot @92a8538 (2026-10-21).

The file:line provenance lives in docs/provenance/results_calendar.md (not shipped, D646). Transport shape after
gammad/jobs/ban_list.py @c0f21a7a :136-170 (refused redirects, never raises).
"""

import http.client
import json
import os
import time
import urllib.error
import urllib.request
from dataclasses import replace
from datetime import date, timedelta
from http.cookiejar import CookieJar
from pathlib import Path
from typing import NamedTuple
from urllib.parse import urlsplit

from gammad.log import get_logger
from gammad.scanners.shared.results_cal import ResultsCalendar, results_days

log = get_logger(__name__)
HOST = "www.nseindia.com"
BASE = "https://" + HOST
HOME_URL = BASE + "/"
WARM_URL = BASE + "/market-data/live-equity-market"
API_PATH = "/api/corporate-board-meetings?index=equities"
REFERER = BASE + "/companies-listing/corporate-filings-board-meetings"
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/130.0 Safari/537.36"
MIN_INTERVAL_S = 2.0  # D629 (1), the source bot's token bucket (one request per 2 s).
AHEAD_DAYS = 7  # D617 (4), the source bot's results-ahead window.
TIMEOUT_S = 20.0  # D617 (4), bounded transport.
MAX_BYTES = 8 * 1024 * 1024  # D617 (4), bounded response.


class FetchResult(NamedTuple):
    status: int
    body: bytes
    content_type: str
    reason: str = ""


def allowed_url(url) -> bool:
    parsed = urlsplit(url)
    return parsed.scheme == "https" and parsed.hostname == HOST


def api_url(day, ahead_days=AHEAD_DAYS) -> str:
    end = day + timedelta(days=ahead_days + 1)
    return f"{BASE}{API_PATH}&from_date={day:%d-%m-%Y}&to_date={end:%d-%m-%Y}"


class RateQueue:
    def __init__(self, min_interval_s=MIN_INTERVAL_S, *, clock=time.monotonic, sleep=time.sleep):
        self.min_interval_s = min_interval_s
        self.clock = clock
        self.sleep = sleep
        self.last = None

    def wait(self):
        now = self.clock()
        if self.last is not None:
            delay = self.min_interval_s - (now - self.last)
            if delay > 0:
                self.sleep(delay)
        self.last = self.clock()


class _NseOnly(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        if not allowed_url(newurl):
            raise urllib.error.URLError("host refused")
        return super().redirect_request(req, fp, code, msg, headers, newurl)


class Transport:
    def __init__(self):
        self.opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(CookieJar()), _NseOnly())

    def __call__(self, url, referer) -> FetchResult:
        try:
            if not allowed_url(url):
                return FetchResult(0, b"", "", "host refused")
            request = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "*/*", "Referer": referer})
            with self.opener.open(request, timeout=TIMEOUT_S) as response:
                body = response.read(MAX_BYTES + 1)
                if len(body) > MAX_BYTES:
                    return FetchResult(0, b"", "", "oversized")
                return FetchResult(response.status, body, response.headers.get("Content-Type", ""))
        except urllib.error.HTTPError as exc:
            with exc:
                return FetchResult(exc.code, b"", "")
        except (urllib.error.URLError, OSError, http.client.HTTPException, ValueError) as exc:
            return FetchResult(0, b"", "", "transport failure: " + type(exc).__name__)


def _cached(cache_path: Path) -> ResultsCalendar | None:
    try:
        return ResultsCalendar.from_dict(json.loads(cache_path.read_text(encoding="utf-8")))
    except Exception:  # noqa: BLE001 -- an unusable cache must not interrupt scanners.
        return None


def fetch_results_calendar(day: date, cache_path: Path, *, fetch=None, rate=None,
                           now_ms=None, ahead_days=AHEAD_DAYS) -> ResultsCalendar:
    """Fetch politely; on any failure retain the last good window and flag it stale."""
    try:
        fetch = Transport() if fetch is None else fetch
        rate = RateQueue() if rate is None else rate
        rate.wait()
        fetch(HOME_URL, BASE)
        rate.wait()
        fetch(WARM_URL, HOME_URL)
        rate.wait()
        res = fetch(api_url(day, ahead_days), REFERER)
        if res.status != 200:
            reason = f"HTTP {res.status}" + (f" ({res.reason})" if res.reason else "")
        else:
            try:
                payload = json.loads(res.body.decode("utf-8"))
                if not isinstance(payload, (list, dict)):
                    raise TypeError("expected list or object")
                rows = payload
                if isinstance(payload, dict):
                    lists = [payload[key] for key in ("data", "rows") if isinstance(payload.get(key), list)]
                    rows = next((found for found in lists if found), lists[0] if lists else None)
                end = day + timedelta(days=ahead_days)
                days = results_days(rows, day, end)
            except Exception as exc:  # noqa: BLE001 -- external payloads must fall back to last-good.
                reason = "unreadable body: " + type(exc).__name__
            else:
                if rows is None:
                    reason = "unreadable body: no data list"
                elif not rows:
                    reason = "empty body"
                else:
                    fetched_ms = now_ms() if now_ms is not None else int(time.time() * 1000)
                    cal = ResultsCalendar(day, end, days, fetched_ms, False, None)
                    cache_path.parent.mkdir(parents=True, exist_ok=True)
                    part = cache_path.with_name(cache_path.name + ".part")
                    part.write_text(json.dumps(cal.to_dict()), encoding="utf-8", newline="\n")
                    os.replace(part, cache_path)
                    return cal
    except Exception as exc:  # noqa: BLE001 -- job boundary includes injected transport / clock and cache I/O.
        reason = "transport failure: " + type(exc).__name__
    log.warning("Results calendar: %s", reason)
    cached = _cached(cache_path)
    if cached is not None:
        return replace(cached, stale=True, reason=reason)
    return ResultsCalendar(day, day, {}, None, True, reason)
