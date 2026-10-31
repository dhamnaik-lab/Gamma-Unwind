"""Logging: rotating file logs/gammad.log + stderr. Nothing in gammad uses the print builtin."""

from __future__ import annotations

import logging
import re
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path

from gammad import REPO_ROOT

_FMT = "%(asctime)s %(levelname)-7s %(name)s: %(message)s"
_configured = False


def setup_logging(level: int = logging.INFO, logs_dir: Path | None = None) -> None:
    """Idempotent. Attaches handlers to the `gammad` logger and (WARNING and above, T-181 F6) the `apscheduler`
    logger, never the root logger."""
    global _configured
    if _configured:
        return
    logs_dir = logs_dir or (REPO_ROOT / "logs")
    logs_dir.mkdir(parents=True, exist_ok=True)
    root = logging.getLogger("gammad")
    root.setLevel(level)
    fmt = logging.Formatter(_FMT)
    fh = RotatingFileHandler(logs_dir / "gammad.log", maxBytes=5_000_000, backupCount=5, encoding="utf-8")
    fh.setFormatter(fmt)
    sh = logging.StreamHandler(sys.stderr)
    sh.setFormatter(fmt)
    root.addHandler(fh)
    root.addHandler(sh)
    root.propagate = False
    # T-181 F6: APScheduler's own WARNINGs ("Run time of job ... was missed by ...", a job that raised) reach the same
    # file: a 09:20 slot that never fired left no trace in gammad.log before. Its INFO chatter stays out.
    aps = logging.getLogger("apscheduler")
    aps.setLevel(logging.WARNING)
    aps.addHandler(fh)
    aps.addHandler(sh)
    aps.propagate = False
    _configured = True


_URL_TAIL = re.compile(r"(https?://[^/\s\"']+)[^\s\"']*")


class _UrlTailRedactor(logging.Filter):
    """httpx/httpcore log every request URL at INFO ("HTTP Request: POST https://host/path?query ..."). Brokers put
    credentials in the path (Nuvama's api key, Telegram's bot token) or the query, so ONLY scheme + host survive:
    `https://host/…`. One boundary for every broker (W12 F1 class, D97); nothing else about the record changes."""

    def filter(self, record: logging.LogRecord) -> bool:
        try:
            text = record.getMessage()
        except Exception:  # noqa: BLE001 -- a malformed record is not ours to break
            return True
        if "://" in text:
            record.msg, record.args = _URL_TAIL.sub(r"\1/…", text), ()
        return True


def install_url_redactor() -> None:
    """Idempotent: attach the URL-tail redactor to the loggers httpx writes request lines to."""
    for name in ("httpx", "httpcore"):
        lg = logging.getLogger(name)
        if not any(isinstance(f, _UrlTailRedactor) for f in lg.filters):
            lg.addFilter(_UrlTailRedactor())


install_url_redactor()


def get_logger(name: str) -> logging.Logger:
    """Module loggers live under `gammad.`; handlers are attached by setup_logging()."""
    if not name.startswith("gammad"):
        name = f"gammad.{name}"
    return logging.getLogger(name)
