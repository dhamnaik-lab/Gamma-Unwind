"""GAMMA-UNWIND launcher for pythonw.exe (no console window): `gammad tray`, the tray supervisor (D66).

The tray runs the service (`gammad serve --no-tray`) as a child and gives it Start / Stop service and Quit.
A second launch while a tray is running asks that tray to start the service and exits.
Works when started by the venv's pythonw.exe (scripts/start.cmd, the logon task) and, as a fallback,
by a system pythonw: the repo's .venv site-packages are put on sys.path first either way.

Silent-start capture (D104): pythonw has no console, so a start that dies before the tray icon appears used to
leave no trace at all. `run()` writes one line per start attempt to logs/launch.log and, when the start raises,
logs/startup-error.log (time, version, Python, OS, the traceback with every .env value and URL tail hidden) plus
a message box that points to Check Setup. This file uses the standard library only: it must work when the
program files it would import are the thing that is broken.
"""

from __future__ import annotations

import datetime as _dt
import os
import platform
import re
import sys
import tempfile
import traceback
from collections.abc import Callable
from pathlib import Path

ROOT = Path(__file__).resolve().parent
MIN_SECRET_LEN = 6
NON_SECRET_KEYS = frozenset({"GAMMA_PORT", "GAMMA_TZ", "BROKER"})
APP_ENV_PREFIXES = ("TELEGRAM_", "DHAN_", "GAMMA_", "KITE_", "ZERODHA_", "UPSTOX_", "ANGEL_", "FYERS_", "KOTAK_",
                    "GROWW_", "NUVAMA_")  # fmt: skip
_URL_TAIL = re.compile(r"(https?://[^/\s\"']+)[/?#][^\s\"']*")
_JWT = re.compile(r"\beyJ[\w-]{4,}\.[\w-]{4,}(?:\.[\w-]+)?")  # most broker access tokens, even if not in .env
LAUNCH_LOG_MAX = 200_000
BOX_TITLE = "GAMMA-UNWIND"
BOX_TEXT = (
    "GAMMA could not start.\n\n"
    "Double-click Check Setup, then send us the file logs\\setup-report-....txt\n\n"
    "(The details are in logs\\startup-error.log.)"
)


def _prepare() -> None:
    os.chdir(ROOT)
    site = ROOT / ".venv" / "Lib" / "site-packages"
    for p in (str(site), str(ROOT)):
        if p not in sys.path:
            sys.path.insert(0, p)
    # pythonw has no console: stdout / stderr are None and any stray write would raise.
    if sys.stdout is None or sys.stderr is None:
        sink = open(os.devnull, "w", encoding="utf-8")  # noqa: SIM115 -- lives for the whole process
        sys.stdout = sys.stdout or sink
        sys.stderr = sys.stderr or sink


def main() -> int:
    _prepare()
    from gammad.__main__ import main as gammad_main

    return gammad_main(["tray"])


# ------------------------------------------------------------------------------ silent-start capture (D104)
def _now() -> str:
    return _dt.datetime.now(_dt.UTC).astimezone().strftime("%Y-%m-%d %H:%M:%S")


def _read(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8", errors="replace").strip()
    except OSError:
        return ""


def secret_values(root: Path) -> list[str]:
    """Every .env value (and every app-named environment variable) long enough to identify a secret, longest
    first. The values stay in memory; only the placeholder is ever written."""
    raw: list[str] = []
    for line in _read(root / ".env").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, val = line.split("=", 1)
        key = key.strip().removeprefix("export ").strip()
        if key in NON_SECRET_KEYS:
            continue
        val = val.strip()
        raw += [val, val.strip("\"'"), val.split(" #", 1)[0].strip().strip("\"'")]
    for key, val in os.environ.items():
        if key.upper().startswith(APP_ENV_PREFIXES) and key.upper() not in NON_SECRET_KEYS:
            raw.append(val.strip())
    out: set[str] = set()
    for v in raw:
        if len(v) >= MIN_SECRET_LEN:
            out.add(v)
            if v.startswith("-100") and len(v) - 4 >= MIN_SECRET_LEN:
                out.add(v[4:])  # a supergroup id is also written without its -100 prefix
    return sorted(out, key=len, reverse=True)


def redact(text: str, secrets: list[str]) -> str:
    for s in secrets:
        text = text.replace(s, "[hidden]")
    text = _URL_TAIL.sub(r"\1/[hidden]", text)  # brokers and Telegram put credentials in URL paths (D97)
    return _JWT.sub("[hidden]", text)


def _append_launch(root: Path, line: str) -> None:
    try:
        logs = root / "logs"
        logs.mkdir(parents=True, exist_ok=True)
        path = logs / "launch.log"
        if path.is_file() and path.stat().st_size > LAUNCH_LOG_MAX:
            keep = path.read_text(encoding="utf-8", errors="replace").splitlines()[-200:]
            path.write_text("\n".join(keep) + "\n", encoding="utf-8")
        with path.open("a", encoding="utf-8") as fh:
            fh.write(f"{_now()} {line}\n")
    except OSError:
        pass  # a read-only folder must not stop the start; Check Setup reports it


def write_startup_error(root: Path, exc: BaseException) -> Path | None:
    """logs/startup-error.log (the LAST failed start; %TEMP% when logs/ is not writable). Never raises."""
    try:
        body = "\n".join([
            "GAMMA-UNWIND could not start",
            f"time:     {_now()}",
            f"version:  {_read(root / 'VERSION') or 'unknown'} ({_read(root / 'EDITION') or 'owner'})",
            f"python:   {platform.python_version()} ({sys.executable})",
            f"os:       {platform.platform()}",
            f"folder:   {root}",
            "",
            "".join(traceback.format_exception(exc)).rstrip(),
            "",
        ])  # fmt: skip
        body = redact(body, secret_values(root))
    except Exception:  # noqa: BLE001 -- the capture must never replace the error it captures
        body = f"GAMMA-UNWIND could not start\ntime: {_now()}\nerror: {type(exc).__name__}\n"
    for folder in (root / "logs", Path(tempfile.gettempdir())):
        try:
            folder.mkdir(parents=True, exist_ok=True)
            path = folder / ("startup-error.log" if folder == root / "logs" else "GAMMA-startup-error.log")
            path.write_text(body, encoding="utf-8")
            return path
        except OSError:
            continue
    return None


def message_box(text: str) -> None:
    """A Windows message box; silently nothing elsewhere or when ctypes is unusable."""
    if os.name != "nt":
        return
    try:
        import ctypes

        ctypes.windll.user32.MessageBoxW(None, text, BOX_TITLE, 0x10 | 0x10000)  # MB_ICONERROR | MB_SETFOREGROUND
    except Exception:  # noqa: BLE001, S110 -- the log file is already written; the box is a courtesy
        pass


def run(root: Path = ROOT, entry: Callable[[], int] | None = None,
        notify: Callable[[str], None] | None = None) -> int:  # fmt: skip
    """The pythonw entry point: main() with a launch line before it and the capture around it."""
    _append_launch(root, f"start attempt: version {_read(root / 'VERSION') or 'unknown'}, folder {root}, "
                         f"python {sys.executable} {platform.python_version()}")  # fmt: skip
    try:
        return (entry or main)()
    except SystemExit as exc:
        if exc.code is None or isinstance(exc.code, int):
            raise
        err: BaseException = exc  # SystemExit("message"): a refusal nobody would see under pythonw
    except Exception as exc:  # noqa: BLE001 -- every start failure is captured, then reported
        err = exc
    path = write_startup_error(root, err)
    _append_launch(root, f"start failed: {type(err).__name__} (details: {path or 'not written'})")
    text = BOX_TEXT
    if isinstance(err, SystemExit) and str(err.code).startswith("Start refused:"):  # T-198: gammad/install.py
        text = redact(str(err.code), secret_values(root))  # the refusal itself says what to do; never a secret
    try:
        (notify or message_box)(text)
    except Exception:  # noqa: BLE001, S110 -- never mask the original error
        pass
    return 1


if __name__ == "__main__":
    sys.exit(run())
