"""Tray supervisor (D66): `gammad tray`, launched by run_gamma.pyw (scripts/start.cmd, the logon task).

The service -- feed, scheduler, alerts AND dashboard -- is a CHILD process
`<venv pythonw> -m gammad serve --no-tray` (cwd = repo root, no window, stdio to DEVNULL: it logs to
logs/gammad.log itself). The tray icon stays when the service is stopped; the menu drives the running
service over its own HTTP API on 127.0.0.1:<GAMMA_PORT> with 1 s timeouts.

  states  stopped (grey) -> starting (amber: child alive, /api/status not answered yet) -> running (blue)
          -> stopping (amber) -> stopped.  The child exiting unasked: code 0 (someone wrote data/STOP)
          -> stopped; any other code -> error (red; 4 = port busy, D31).
  restart T-284 / D268(5) (overrides D66's "no auto-restart"): a crashed child is restarted a BOUNDED number of
          times, only when ALL hold: exit code not in NO_RESTART_EXITS {0, 2, 3, 4}; `install.start_refusal` is None
          (re-checked at every try); no data/STOP, data/QUIT, data/update/journal.json; inside the keep-awake window
          (08:50-15:40 on a trading day, `keepawake.want_awake`); fewer than MAX_RESTARTS restarts today. Scheduled
          by the clock (RESTART_BACKOFF_S; the loop never sleeps for it); a child that dies within
          RESTART_MIN_UPTIME_S of its restart is a failed try and steps the backoff up. After MAX_RESTARTS: give up.
          An owner Start resets the day's budget and cancels a pending restart; Stop / Quit cancel it. A hung child
          (alive, API silent) is NEVER killed: alert only.
  phone   T-284: ONE line to the PERSONAL chat (gammad/tray/notify.py; None = tray.log only) per crash, restart
          outcome and API-silence episode (silent API_SILENT_S while RUNNING, START_SILENT_S while STARTING and
          never answered, in the window; one line when it answers again). Every line is also logged in tray.log.
          The sender is rebuilt from <root>/.env at every owner Start (menu, page, data/START), so a bot token or
          personal chat written after the tray started takes effect without restarting the tray.
  Stop    write data/STOP (the service polls it every 2 s and exits cleanly), wait up to 10 s for the
          child, else terminate it (the venv pythonw stub takes its interpreter with it, measured
          2026-09-25). Quit = Stop, then exit.
  files   polled every 1 s: data/QUIT = Stop + exit (scripts/stop.cmd writes STOP and QUIT);
          data/START = start the service when stopped / in error (a second `gammad tray` writes it and
          exits 0, so start.cmd also restarts a stopped service). Stale QUIT / START are cleared when the
          tray starts, a stale STOP before every service start.
  page    T-087 / D95(1): while the service is STOPPED or in ERROR (no child left), the supervisor serves a
          minimal "Service stopped -- Start service . Open dashboard" page on the SAME 127.0.0.1:<GAMMA_PORT>
          (gammad/tray/stopped_page.py). Its POST /supervisor/start (header X-Gamma-Control: 1) queues "start";
          `start_service` CLOSES the page and waits until the port is free BEFORE launching the child (else
          the child exits 4, D31). After the child exits (data/STOP from the dashboard's Stop button) the
          page is back on the next control-loop pass (~1-2 s). Quit closes it.
  single  data/tray.lock plus a per-user tray-<edition>.lock: 1-byte locks far past EOF (Windows locks
          are mandatory, so the PID / folder at offset 0 stay readable); the OS drops both when we exit.
  logs    logs/tray.log, NOT logs/gammad.log: two processes on one RotatingFileHandler break its
          rotation on Windows (renaming a file open in another process = WinError 32, measured 2026-09-25).
  awake   T-133 / D119: the control loop holds SetThreadExecutionState(ES_SYSTEM_REQUIRED) 08:50-15:40 IST on
          trading days while the service is RUNNING (gammad/tray/keepawake.py; the flag is per thread, so only
          this loop thread touches it); the tooltip says "awake until 15:40" or "PC may sleep". The menu's
          "Wake PC at 08:55" registers / removes the per-user task GAMMA-UNWIND-WAKE (gammad/tray/wake.py),
          in BOTH editions; "Autostart on login" stays owner-only.

Every state change runs on ONE thread (the control loop, `Supervisor.run`); menu clicks only queue a
command, except the API calls (run job, pause), which touch nothing but the cached status body.
The menu is a pure function of a `View` (`menu_spec`), so it is tested without a window.
"""

from __future__ import annotations

import contextlib
import logging
import os
import queue
import subprocess
import sys
import threading
import time
import webbrowser
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Protocol

from gammad import REPO_ROOT
from gammad import edition as _edition
from gammad.core.calendar import is_trading_day as calendar_is_trading_day
from gammad.install import REFUSED, start_refusal
from gammad.log import get_logger
from gammad.tray import wake as wake_task
from gammad.tray.keepawake import IST, OFF, ON, KeepAwake, awake_until, want_awake
from gammad.tray.stopped_page import StoppedPage, wait_port_free

log = get_logger(__name__)

STOPPED, STARTING, RUNNING, STOPPING, ERROR = "stopped", "starting", "running", "stopping", "error"
STATES = (STOPPED, STARTING, RUNNING, STOPPING, ERROR)

STOP_FILE, START_FILE, QUIT_FILE, LOCK_FILE = "STOP", "START", "QUIT", "tray.lock"
LOCK_OFFSET = 1 << 20
EDITION_LOCK_FMT = "tray-{edition}.lock"  # T-337 (b) / D425 (3)
SECOND_COPY_OK_ENV = "GAMMA_SECOND_COPY_OK"  # "1" = the T-149 broker test day (5 client copies on one PC)
TICK_S = 1.0  # control files + queued menu commands
STATUS_EVERY = 2  # ticks between /api/status probes (= every 2 s)
STOP_WAIT_S = 10.0
KILL_WAIT_S = 5.0
HTTP_TIMEOUT_S = 1.0
EXIT_PORT_BUSY = 4  # D31
DEFAULT_PORT = 8760
TRAY_LOG = "tray.log"
PORT_FREE_WAIT_S = 5.0  # T-087: after closing the stopped page, wait this long for the port to be free

# T-284 / D268(5): the owner's numbers (change them here). The restart window is the keep-awake window
# (keepawake.AWAKE_FROM / AWAKE_UNTIL on a trading day), not a copy of its times.
API_SILENT_S = 90  # /api/status unanswered this long while RUNNING in the window -> one phone line
# audit 0.8.5 (1): a child that never answered at all (wake / auto-restart / Start) gets API_SILENT_S plus this start
# allowance (the import, the store open, the broker login) before the same one line; it is never killed either.
START_ALLOWANCE_S = 120
START_SILENT_S = API_SILENT_S + START_ALLOWANCE_S
MAX_RESTARTS = 3  # auto-restarts per trading day, then give up
RESTART_BACKOFF_S: tuple[int, ...] = (10, 60, 300)  # wait before try 1 / 2 / 3 (the last repeats)
RESTART_MIN_UPTIME_S = 120  # a restarted child that dies sooner is a failed try (the backoff steps up)
# 0 = data/STOP (a normal stop); 2 = calendar / expiry missing, 3 = broker login, 4 = port busy: a restart cannot help
NO_RESTART_EXITS = frozenset({0, 2, 3, EXIT_PORT_BUSY})
NO_RESTART_WHY = {2: "calendar or expiry data missing", 3: "broker login failed", EXIT_PORT_BUSY: "port busy"}
UPDATE_JOURNAL = ("update", "journal.json")  # scripts/update_gamma.py's in-flight marker under data/
OUTSIDE_WINDOW = "outside market hours"

# (menu label, job name, kind): the same jobs the in-process tray runs.
JOBS: tuple[tuple[str, str, str], ...] = (
    ("Run 09:20 now", "mark_levels", "0920"),
    ("Run a refresh now", "build_watchlists", "1315"),  # 0.8.5: kind 1315 = a refresh (re-mark + add-only build)
    ("Run 16:00 now", "mark_levels", "1600"),
)

STATE_COLOURS: dict[str, tuple[int, int, int]] = {
    RUNNING: (88, 166, 255),
    STARTING: (210, 153, 34),
    STOPPING: (210, 153, 34),
    STOPPED: (110, 118, 129),
    ERROR: (248, 81, 73),
}


class Proc(Protocol):
    pid: int

    def poll(self) -> int | None: ...
    def wait(self, timeout: float | None = None) -> int: ...
    def terminate(self) -> None: ...


Launcher = Callable[[list[str], Path], Proc]


# ------------------------------------------------------------------------------------ helpers
def service_command(root: Path = REPO_ROOT, executable: str | None = None) -> list[str]:
    pyw = root / ".venv" / "Scripts" / "pythonw.exe"
    exe = str(pyw) if pyw.exists() else (executable or sys.executable)
    return [exe, "-m", "gammad", "serve", "--no-tray"]


def popen_launcher(cmd: list[str], cwd: Path) -> Proc:
    return subprocess.Popen(
        cmd,
        cwd=cwd,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
    )


def make_http(port: int) -> Any:
    import httpx

    # trust_env=False: a proxy from the environment must never sit between us and 127.0.0.1
    return httpx.Client(base_url=f"http://127.0.0.1:{port}", timeout=HTTP_TIMEOUT_S, trust_env=False)


def describe_exit(code: int, port: int) -> str:
    if code == EXIT_PORT_BUSY:
        return f"exit {code}: port {port} busy (another gammad running?)"
    return f"exit {code}" if -256 < code < 256 else f"exit 0x{code & 0xFFFFFFFF:08X}"


def _unlink(path: Path) -> None:
    with contextlib.suppress(OSError):
        path.unlink(missing_ok=True)


def _consume(path: Path) -> bool:
    """True when the request file existed (it is deleted either way)."""
    if not path.exists():
        return False
    _unlink(path)
    return True


def _touch(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.touch()


# ------------------------------------------------------------------------------ single instance
def _lock_byte(fd: int) -> None:
    if os.name == "nt":
        import msvcrt

        os.lseek(fd, LOCK_OFFSET, os.SEEK_SET)
        msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)  # one attempt; OSError when another handle holds it
    else:
        import fcntl

        fcntl.lockf(fd, fcntl.LOCK_EX | fcntl.LOCK_NB, 1, LOCK_OFFSET)


def _unlock_byte(fd: int) -> None:
    if os.name == "nt":
        import msvcrt

        os.lseek(fd, LOCK_OFFSET, os.SEEK_SET)
        msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
    else:
        import fcntl

        fcntl.lockf(fd, fcntl.LOCK_UN, 1, LOCK_OFFSET)


class InstanceLock:
    """One tray per data dir. The lock dies with the process, so a crashed tray never blocks the next."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self._fd: int | None = None

    def acquire(self, note: str = "") -> bool:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(self.path, os.O_RDWR | os.O_CREAT | getattr(os, "O_BINARY", 0), 0o644)
        try:
            _lock_byte(fd)
        except OSError:
            os.close(fd)
            return False
        os.ftruncate(fd, 0)
        os.lseek(fd, 0, os.SEEK_SET)
        os.write(fd, (f"{os.getpid()}\n" + (f"{note}\n" if note else "")).encode("utf-8"))
        self._fd = fd
        return True

    def holder_pid(self) -> int | None:
        try:
            return int(self.path.read_text(encoding="utf-8", errors="replace").split("\n", 1)[0].strip()) or None
        except (OSError, ValueError):
            return None

    def holder_note(self) -> str:
        try:
            lines = self.path.read_text(encoding="utf-8", errors="replace").split("\n")
            return lines[1].strip() if len(lines) > 1 else ""
        except OSError:
            return ""

    def release(self) -> None:
        fd, self._fd = self._fd, None
        if fd is None:
            return
        with contextlib.suppress(OSError):
            _unlock_byte(fd)
        os.close(fd)


def default_edition_lock_dir() -> Path:
    base = os.environ.get("LOCALAPPDATA")
    return (Path(base) if base else Path.home() / ".cache") / "gamma-unwind" / "locks"


# ---------------------------------------------------------------------------------- menu model
@dataclass(frozen=True)
class View:
    state: str
    port: int = DEFAULT_PORT
    alerts_paused: bool = False
    token_level: str = "unknown"
    autostart: bool = False
    detail: str = ""
    autostart_available: bool = True  # False in the client edition / without tools/autostart.py (T-085)
    # T-133 / D119: the "Wake PC at 08:55" task (gammad/tray/wake.py) is offered in BOTH editions wherever Task
    # Scheduler exists; the Supervisor sets wake_available = wake.available() (Windows). The default is True,
    # like autostart_available, so tools/edition_probe.py sees the item in both editions on any OS.
    wake: bool = False
    wake_available: bool = True
    awake: str = OFF  # keep-awake (SetThreadExecutionState) as held by the loop thread: "on" / "off"
    day: date | None = None


@dataclass(frozen=True)
class Item:
    label: str
    action: str | None  # key for the tray's action table; None = a plain label
    enabled: bool = True
    checked: bool | None = None  # None = not a check item
    default: bool = False


def status_line(v: View) -> str:
    text = {
        RUNNING: f"running on port {v.port}",
        STARTING: "starting...",
        STOPPING: "stopping...",
        STOPPED: "stopped",
    }.get(v.state, f"error - {v.detail}" if v.detail else "error")
    return f"Service: {text}"


TOOLTIP_MAX = 120  # the Windows tip is capped at 128 chars (pystray's limit); keep a margin


def awake_line(v: View) -> str:
    return f"awake until {awake_until(v.day or datetime.now(IST).date()):%H:%M}" if v.awake == ON else "PC may sleep"


def tooltip(v: View) -> str:
    """"GAMMA-UNWIND - Service: ... · awake until 15:40": the status part is cut (a long error detail) so the
    keep-awake part always survives and the whole tip stays within TOOLTIP_MAX."""
    tail = f" · {awake_line(v)}"
    return f"GAMMA-UNWIND - {status_line(v)}"[: TOOLTIP_MAX - len(tail)] + tail


def menu_spec(v: View) -> list[Item | None]:
    """The tray menu for one View; None = separator."""
    live = v.state == RUNNING
    return [
        Item(status_line(v), None, enabled=False),
        Item("Open dashboard", "open", enabled=live, default=True),
        Item("Start service", "start", enabled=v.state in (STOPPED, ERROR)),
        Item("Stop service", "stop", enabled=v.state in (STARTING, RUNNING)),
        None,
        *(Item(label, f"job:{name}:{kind}", enabled=live) for label, name, kind in JOBS),
        Item("Pause alerts", "pause", enabled=live, checked=v.alerts_paused),
        Item(f"Token: {v.token_level}", None, enabled=False),
        *([Item("Autostart on login", "autostart", checked=v.autostart)] if v.autostart_available else []),
        *([Item(f"Wake PC at {wake_task.WAKE_AT}", "wake", checked=v.wake)] if v.wake_available else []),
        Item("Open logs folder", "logs"),
        None,
        Item("Quit", "quit"),
    ]


# ---------------------------------------------------------------------------------- supervisor
class Supervisor:
    def __init__(
        self,
        *,
        root: Path,
        data_dir: Path,
        port: int,
        launcher: Launcher = popen_launcher,
        http: Any = None,
        command: list[str] | None = None,
        stop_wait_s: float = STOP_WAIT_S,
        kill_wait_s: float = KILL_WAIT_S,
        clock: Callable[[], float] = time.monotonic,
        stopped_page: bool = False,
        port_free_wait_s: float = PORT_FREE_WAIT_S,
        keep_awake: KeepAwake | None = None,
        now: Callable[[], datetime] = lambda: datetime.now(IST),
        is_trading_day: Callable[[date], bool] = calendar_is_trading_day,
        notify: Callable[[str], None] | None = None,
        notifier_factory: Callable[[], Callable[[str], None] | None] | None = None,
    ) -> None:
        self.root = root
        self.data_dir = data_dir
        self.port = port
        self.launcher = launcher
        self.http = http if http is not None else make_http(port)
        self.command = command or service_command(root)
        self.stop_wait_s = stop_wait_s
        self.kill_wait_s = kill_wait_s
        self.clock = clock
        self.state = STOPPED
        self.detail = ""
        self.proc: Proc | None = None
        # W14 H3: wall-clock time (time.time, comparable with a file mtime) the current child was launched;
        # None while no child. A data/START with `.env` newer than this restarts the service.
        self.started_at: float | None = None
        self.status: dict[str, Any] = {}
        self.autostart = False
        self.autostart_available = True
        # T-133 / D119: keep-awake is synced by the control loop (`run`) only, so the per-thread
        # SetThreadExecutionState flag lives on that one long-lived thread. None = never held (unit tests).
        self.keep_awake = keep_awake
        self.now = now
        self.is_trading_day = is_trading_day
        self.awake = OFF
        self._awake_warned = False
        self.wake_task = False  # the "Wake PC at 08:55" task is registered (read by run_tray)
        self.wake_available = wake_task.available()
        self.on_change: Callable[[], None] = lambda: None
        self._commands: queue.SimpleQueue[str] = queue.SimpleQueue()
        self._wake = threading.Event()
        self._ticks = 0
        # T-284: the phone (gammad/tray/notify.py, PERSONAL chat only; None = tray.log only)
        self.notify = notify
        # audit 0.8.5 (4): rebuilds `notify` from the current .env at every owner Start; None = keep `notify` as given
        self.notifier_factory = notifier_factory
        # API-silence episode: clock() / wall time of the first unanswered probe; `_alerted` holds "api" once the
        # episode's one line went out (cleared when it answers again or the child goes away)
        self._api_quiet_since: float | None = None
        self._api_quiet_wall: datetime | None = None
        self._alerted: set[str] = set()
        # bounded auto-restart (T-284 / D268(5)): restarts spent on `_restart_day`, the pending one (clock time),
        # the backoff step (consecutive failed tries), the clock time the current auto-restarted child was launched
        self._restart_day: date | None = None
        self._restarts = 0
        self._restart_at: float | None = None
        self._restart_detail = ""
        self._backoff_i = 0
        self._auto_launched_at: float | None = None
        self._back_pending = False  # say "running again" when the restarted child first answers
        # T-087 / D95(1): the "Service stopped" page on the service's own port while it is down (opt-in:
        # `supervise` turns it on; unit tests of the state machine never bind a port).
        self.port_free_wait_s = port_free_wait_s
        self.page: StoppedPage | None = (
            StoppedPage(port, state=lambda: (self.state, self.detail), on_start=lambda: self.request("start"))
            if stopped_page
            else None
        )

    # ---- state
    def _set(self, state: str, detail: str = "") -> None:
        if (state, detail) == (self.state, self.detail):
            return
        self.state, self.detail = state, detail
        with contextlib.suppress(Exception):
            self.on_change()

    def view(self) -> View:
        body = self.status if self.state == RUNNING else {}
        token = body.get("token") if isinstance(body.get("token"), dict) else {}
        return View(
            state=self.state,
            port=self.port,
            alerts_paused=bool(body.get("alerts_paused", False)),
            token_level=str((token or {}).get("level") or "unknown"),
            autostart=self.autostart,
            detail=self.detail,
            autostart_available=self.autostart_available,
            wake=self.wake_task,
            wake_available=self.wake_available,
            awake=self.awake,
            day=self._wall().astimezone(IST).date(),
        )

    # ---- keep-awake (T-133)
    def sync_awake(self, release: bool = False) -> str:
        """Control loop only: hold the system awake 08:50-15:40 IST on trading days while RUNNING, else let go
        (`release` = the loop is ending). A failing clock / calendar never breaks the loop; it lets go."""
        if self.keep_awake is None:
            return self.awake
        want = False
        if not release:
            try:
                want = want_awake(self.now(), self.is_trading_day, self.state == RUNNING)
            except Exception:
                if not self._awake_warned:
                    self._awake_warned = True
                    log.warning("tray: keep-awake check failed; letting the PC sleep", exc_info=True)
        self.awake = self.keep_awake.sync(want)
        return self.awake

    # ---- stopped page (T-087)
    def sync_page(self) -> None:
        """Control loop: serve the stopped page while the service is down (STOPPED, or ERROR with no child
        left holding the port); close it in any other state. A bind failure is retried on the next pass."""
        if self.page is None:
            return
        down = self.proc is None and self.state in (STOPPED, ERROR)
        if down and not self.page.is_open:
            self.page.open()
        elif not down and self.page.is_open:
            self.release_port()

    def release_port(self) -> bool:
        """Close the stopped page and wait until 127.0.0.1:<port> is free (the child must find it free, else
        it exits 4, D31). True when free (or no page was open: a port held by someone else is the child's
        exit 4 to report, not ours to wait on), False after `port_free_wait_s`."""
        if self.page is None or not self.page.is_open:
            return True
        self.page.close()
        if wait_port_free(self.port, self.port_free_wait_s):
            return True
        log.warning("tray: port %d still busy %.1f s after closing the stopped page", self.port,
                    self.port_free_wait_s)
        return False

    # ---- lifecycle
    def start_service(self) -> bool:
        if self.proc is not None or self.state in (STARTING, RUNNING, STOPPING):
            return False
        self.data_dir.mkdir(parents=True, exist_ok=True)
        _unlink(self.data_dir / STOP_FILE)  # a stale STOP would stop the new child within 2 s
        self.release_port()  # T-087: the stopped page gives the port back BEFORE the child binds it
        try:
            self.proc = self.launcher(self.command, self.root)
        except OSError as exc:
            log.exception("tray: cannot start the service: %s", self.command)
            self._set(ERROR, f"cannot start: {exc}")
            return False
        self.started_at = time.time()
        self.status = {}
        self._end_api_episode()
        self._set(STARTING)
        log.info("tray: service started, pid %s: %s", self.proc.pid, " ".join(self.command))
        return True

    def stop_service(self) -> bool:
        proc = self.proc
        if proc is None:
            if self.state != ERROR:
                self._set(STOPPED)
            return False
        self._set(STOPPING)
        t0 = self.clock()
        _touch(self.data_dir / STOP_FILE)
        how = "clean"
        try:
            code: int | None = proc.wait(timeout=self.stop_wait_s)
        except subprocess.TimeoutExpired:
            how = "terminated"
            log.warning("tray: service pid %s did not stop in %.0f s; terminating", proc.pid,
                        self.stop_wait_s)
            proc.terminate()
            try:
                code = proc.wait(timeout=self.kill_wait_s)
            except subprocess.TimeoutExpired:
                code = None
        _unlink(self.data_dir / STOP_FILE)  # not consumed when the child was terminated
        self.status = {}
        if code is None:
            log.error("tray: service pid %s survived terminate", proc.pid)
            self._set(ERROR, f"pid {proc.pid} did not stop")
            return False
        self.proc = None
        self.started_at = None
        self._end_api_episode()
        self._set(STOPPED)
        log.info("tray: service stopped (%s, exit %s, %.1f s)", how, code, self.clock() - t0)
        return True

    def poll(self, probe: bool = True) -> None:
        """Child exit check; with `probe`, one GET /api/status."""
        proc = self.proc
        if proc is None or self.state == STOPPING:
            return
        code = proc.poll()
        if code is not None:
            self.proc = None
            self.started_at = None
            self.status = {}
            self._end_api_episode()  # the crash line covers it: no "answering again" for a dead child
            if code == 0:
                log.info("tray: service exited on its own (exit 0, data/STOP?); not restarting")
                self._auto_launched_at = None
                self._back_pending = False
                self._set(STOPPED)
            else:
                self._failed(code, describe_exit(code, self.port))
            return
        if not probe:
            return
        body = self.probe()
        if body is None:
            self._api_silent(proc.pid)
            return  # starting stays starting; running stays running (the process is the truth)
        self._api_answering()
        self.status = body
        if self.state == STARTING:
            log.info("tray: service running on port %d", self.port)
            if self._back_pending:
                self._back_pending = False
                self._say(f"GAMMA-UNWIND running again at {self._hhmm()} (restart {self._restarts}/{MAX_RESTARTS})")
        self._set(RUNNING)

    # ---- T-284: phone lines
    def _say(self, line: str) -> None:
        """One phone line: always in tray.log; to the PERSONAL chat when a notifier is set. Never raises."""
        log.warning("tray: phone: %s", line)
        if self.notify is None:
            return
        try:
            self.notify(line)
        except Exception:  # the phone must never break the control loop
            log.warning("tray: phone line failed", exc_info=True)

    def _wall(self) -> datetime:
        try:
            return self.now()
        except Exception:  # noqa: BLE001 -- a broken clock still leaves a time on the line
            return datetime.now(IST)

    def _hhmm(self, at: datetime | None = None) -> str:
        at = at or self._wall()
        return f"{at.astimezone(IST) if at.tzinfo is not None else at:%H:%M}"

    def in_window(self, at: datetime | None = None) -> bool:
        """The restart / API-alert window = the keep-awake window (08:50-15:40 on a trading day). A failing calendar
        (a missing year) is outside: no restart on a guess."""
        try:
            return want_awake(at or self.now(), self.is_trading_day, True)
        except Exception:
            log.warning("tray: market-hours check failed; treating it as outside", exc_info=True)
            return False

    # ---- T-284: API silence (alert only: a hung child is never killed)
    def _api_silent(self, pid: int) -> None:
        if self._api_quiet_since is None:
            if self.state == RUNNING:
                log.warning("tray: service pid %s alive but /api/status not answering", pid)
            self._api_quiet_since = self.clock()
            self._api_quiet_wall = self._wall()
        # RUNNING: silent API_SILENT_S. STARTING (never answered yet): START_SILENT_S, so a child that hangs before its
        # first answer alerts too (audit 0.8.5 (1)); the episode starts at the first unanswered probe after launch.
        limit = API_SILENT_S if self.state == RUNNING else START_SILENT_S if self.state == STARTING else None
        if (
            limit is not None
            and "api" not in self._alerted
            and self.clock() - self._api_quiet_since >= limit
            and self.in_window()
        ):
            self._alerted.add("api")
            self._say(f"GAMMA-UNWIND not answering since {self._hhmm(self._api_quiet_wall)} (no reply for "
                      f"{limit} s); the dashboard and alerts may be down")  # fmt: skip

    def _api_answering(self) -> None:
        if "api" in self._alerted and self._api_quiet_since is not None:
            silent_min = max(1, int((self.clock() - self._api_quiet_since) // 60))
            self._say(f"GAMMA-UNWIND answering again at {self._hhmm()} (silent {silent_min} min)")
        self._end_api_episode()

    def _end_api_episode(self) -> None:
        self._api_quiet_since = None
        self._api_quiet_wall = None
        self._alerted.discard("api")

    # ---- T-284 / D268(5): bounded auto-restart
    def _blocked(self, at: datetime) -> str | None:
        """Why a restart must not happen now (every guard but the exit code and the budget), or None."""
        refusal = start_refusal(self.root, _edition.EDITION)  # the replaced-folder / dev-tree guard, every try
        if refusal is not None:
            log.error("tray: %s", refusal)
            return "this folder must not start"
        if (self.data_dir / STOP_FILE).exists() or (self.data_dir / QUIT_FILE).exists():
            return "a stop was requested"
        journals = {self.data_dir.joinpath(*UPDATE_JOURNAL), (self.root / "data").joinpath(*UPDATE_JOURNAL)}
        if any(p.exists() for p in journals):
            return "an update is in progress"
        if not self.in_window(at):
            return OUTSIDE_WINDOW
        return None

    def _roll_day(self, at: datetime) -> None:
        day = (at.astimezone(IST) if at.tzinfo is not None else at).date()
        if day != self._restart_day:
            self._restart_day, self._restarts, self._backoff_i = day, 0, 0

    def _reset_restarts(self) -> None:
        """An owner Start: a fresh budget, no pending restart, the first backoff step."""
        self._restarts = 0
        self._backoff_i = 0
        self._restart_at = None
        self._auto_launched_at = None
        self._back_pending = False

    def _cancel_restart(self, why: str) -> None:
        if self._restart_at is None:
            return
        self._restart_at = None
        log.info("tray: pending restart cancelled (%s)", why)
        if self.proc is None and self.state == ERROR:
            self._set(ERROR, self._restart_detail)

    def _failed(self, code: int | None, detail: str) -> None:
        """The child died (`code`) or an auto-restart could not launch (`code` None): restart, refuse or give up,
        with ONE phone line either way. Never sleeps: a restart is a clock time for `tick`."""
        at = self._wall()
        hhmm = self._hhmm(at)
        quick = self._auto_launched_at is not None and self.clock() - self._auto_launched_at < RESTART_MIN_UPTIME_S
        self._auto_launched_at = None
        self._back_pending = False
        self._restart_at = None
        self._restart_detail = detail
        self._roll_day(at)
        self._backoff_i = self._backoff_i + 1 if quick else 0
        head = (f"GAMMA-UNWIND could not start at {hhmm} ({detail})" if code is None
                else f"GAMMA-UNWIND stopped unexpectedly at {hhmm} ({detail})")  # fmt: skip
        why = NO_RESTART_WHY.get(code) if code is not None and code in NO_RESTART_EXITS else None
        why = why or self._blocked(at)
        if why is not None:
            log.error("tray: service died (%s); not restarting (%s) -- use Start service", detail, why)
            self._set(ERROR, detail)
            self._say(f"{head}; not restarted {why}" if why == OUTSIDE_WINDOW
                      else f"{head}; not restarted ({why}); use Start service")  # fmt: skip
            return
        if self._restarts >= MAX_RESTARTS:
            log.error("tray: service died (%s); gave up after %d restarts today -- use Start service", detail,
                      MAX_RESTARTS)
            self._set(ERROR, f"{detail}; gave up after {MAX_RESTARTS} restarts; use Start service")
            self._say(f"{head}; gave up after {MAX_RESTARTS} restarts today; use Start service")
            return
        self._restarts += 1
        wait = RESTART_BACKOFF_S[min(self._backoff_i, len(RESTART_BACKOFF_S) - 1)]
        self._restart_at = self.clock() + wait
        log.error("tray: service died (%s); restarting in %d s (%d/%d)", detail, wait, self._restarts, MAX_RESTARTS)
        self._set(ERROR, f"{detail}; restarting in {wait} s ({self._restarts}/{MAX_RESTARTS})")
        self._say(f"{head}; restarting ({self._restarts}/{MAX_RESTARTS})")

    def _restart_due(self) -> None:
        """Control loop: launch the pending restart once its clock time has come, after re-checking every guard."""
        if self._restart_at is None or self.clock() < self._restart_at:
            return
        self._restart_at = None
        if self.proc is not None or self.state != ERROR:  # someone else started it meanwhile
            return
        at = self._wall()
        why = self._blocked(at)
        if why is not None:
            log.error("tray: pending restart not done (%s)", why)
            self._set(ERROR, self._restart_detail)
            self._say(f"GAMMA-UNWIND not restarted at {self._hhmm(at)} ({why}); use Start service")
            return
        log.info("tray: auto-restart %d/%d", self._restarts, MAX_RESTARTS)
        if self.start_service():
            self._auto_launched_at = self.clock()
            self._back_pending = True
            return
        self._auto_launched_at = self.clock()  # a launch that failed at once is a failed try
        self._failed(None, self.detail)

    def probe(self) -> dict[str, Any] | None:
        try:
            r = self.http.get("/api/status")
            body = r.json() if r.status_code == 200 else None
        except Exception:  # noqa: BLE001 -- refused / timeout / bad JSON: all mean "not answering"
            return None
        return body if isinstance(body, dict) else None

    # ---- API actions (menu thread)
    def _post(self, path: str, payload: dict[str, Any]) -> dict[str, Any] | None:
        if self.state != RUNNING:
            return None
        try:
            r = self.http.post(path, json=payload)
            body = r.json()
        except Exception:  # a menu click must never crash the tray
            log.warning("tray: POST %s failed", path, exc_info=True)
            return None
        if r.status_code != 200 or not isinstance(body, dict):
            log.warning("tray: POST %s -> HTTP %s %s", path, r.status_code, body)
            return None
        return body

    def run_job(self, name: str, kind: str) -> str | None:
        body = self._post(f"/api/jobs/{name}/run", {"kind": kind})
        run_id = str(body["run_id"]) if body and body.get("run_id") else None
        log.info("tray: run %s %s -> %s", name, kind, run_id)
        return run_id

    def toggle_pause(self) -> bool | None:
        want = not bool(self.status.get("alerts_paused", False))
        body = self._post("/api/alerts/pause", {"paused": want})
        if body is None:
            return None
        self.status = body
        log.info("tray: alerts paused -> %s", body.get("alerts_paused"))
        with contextlib.suppress(Exception):
            self.on_change()
        return bool(body.get("alerts_paused"))

    # ---- control loop
    def request(self, cmd: str) -> None:
        """Menu thread: queue start / stop / quit for the control loop."""
        self._commands.put(cmd)
        self._wake.set()

    def owner_start(self) -> bool:
        """The owner's Start (menu, stopped page, data/START): a fresh restart budget and no pending restart (T-284)."""
        if self._restart_at is not None or self._restarts:
            log.info("tray: owner Start: restart budget reset")
        self._reset_restarts()
        self.refresh_notifier()
        return self.start_service()

    def refresh_notifier(self) -> None:
        """Rebuild the phone sender from the current .env (a token / personal chat written after the tray started:
        W14 H3's first start on an empty .env). A failing rebuild keeps the sender it had."""
        if self.notifier_factory is None:
            return
        try:
            fresh = self.notifier_factory()
        except Exception:  # a broken .env / yaml must never block the Start itself
            log.warning("tray: phone sender not rebuilt; keeping the previous one", exc_info=True)
            return
        if (fresh is None) != (self.notify is None):
            log.info("tray: phone lines to the personal chat: %s", "on" if fresh is not None else "off")
        self.notify = fresh

    def env_changed_since_start(self) -> bool:
        """True when `<root>/.env` was modified after the current child was launched (W14 H3)."""
        if self.started_at is None:
            return False
        try:
            return (self.root / ".env").stat().st_mtime > self.started_at
        except OSError:  # no .env (or unreadable): nothing new to load
            return False

    def check_files(self) -> bool:
        """data/QUIT -> True (stop and exit). data/START -> start the service when it is down; while it is up,
        restart it when `.env` changed after it started (W14 H3: the first Start ran on an empty .env), else
        ignore the START."""
        if _consume(self.data_dir / QUIT_FILE):
            log.info("tray: data/QUIT found; stopping the service and exiting")
            return True
        if _consume(self.data_dir / START_FILE):
            if self.state in (STOPPED, ERROR):
                log.info("tray: data/START found; starting the service")
                self.owner_start()
            elif self.state in (STARTING, RUNNING) and self.env_changed_since_start():
                log.info("tray: data/START found; restarting: .env changed after start")
                if self.stop_service():
                    self.owner_start()
            else:
                log.info("tray: data/START found; service already %s", self.state)
        return False

    def tick(self) -> bool:
        """One control-loop pass. True = quit (the child is already stopped)."""
        quit_now = False
        while True:
            try:
                cmd = self._commands.get_nowait()
            except queue.Empty:
                break
            if cmd == "start":
                self.owner_start()
            elif cmd == "stop":
                self._cancel_restart("Stop service")
                self.stop_service()
            elif cmd == "quit":
                quit_now = True
        if quit_now or self.check_files():
            self._cancel_restart("Quit")
            self.stop_service()
            return True
        self._restart_due()
        self.poll(probe=self._ticks % STATUS_EVERY == 0)
        self._ticks += 1
        return False

    def run(self, stop_evt: threading.Event | None = None) -> None:
        last: View | None = None
        try:
            while stop_evt is None or not stop_evt.is_set():
                try:
                    done = self.tick()
                    if not done:
                        self.sync_page()
                        self.sync_awake()
                except Exception:  # the loop is the supervisor; it must survive a bad tick
                    log.exception("tray: control loop tick failed")
                    done = False
                v = self.view()
                if v != last:
                    last = v
                    with contextlib.suppress(Exception):
                        self.on_change()
                if done:
                    return
                self._wake.wait(TICK_S)
                self._wake.clear()
        finally:
            with contextlib.suppress(Exception):  # let the PC sleep again, from the thread that held it
                self.sync_awake(release=True)
            if self.page is not None:  # Quit (or the loop ending) closes the stopped page
                self.page.close()


# ---------------------------------------------------------------------------------------- tray
def make_state_icon(state: str, size: int | None = None) -> Any:
    """The in-process tray's gamma circle, with the ring and glyph in the state colour."""
    from PIL import Image, ImageDraw, ImageFont

    from gammad.tray.tray import FONT_CANDIDATES, GLYPH, ICON_PX

    size = size or ICON_PX
    accent = (*STATE_COLOURS.get(state, STATE_COLOURS[ERROR]), 255)
    img = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)
    pad = max(1, size // 32)
    draw.ellipse((pad, pad, size - pad - 1, size - pad - 1), fill=(22, 27, 34, 255), outline=accent,
                 width=max(1, size // 21))
    font: Any = None
    for name in FONT_CANDIDATES:
        try:
            font = ImageFont.truetype(name, int(size * 0.62))
            break
        except OSError:
            continue
    if font is None:
        font = ImageFont.load_default()
    box = draw.textbbox((0, 0), GLYPH, font=font)
    w, h = box[2] - box[0], box[3] - box[1]
    draw.text(((size - w) / 2 - box[0], (size - h) / 2 - box[1]), GLYPH, font=font, fill=accent)
    return img


def run_tray(sup: Supervisor, logs_dir: Path) -> int:
    """pystray icon on this thread; the control loop in pystray's setup thread. Headless without pystray."""
    from gammad.tray.tray import _autostart_module

    autostart = _autostart_module()  # None in the client edition (T-085), no traceback
    sup.autostart_available = autostart is not None
    with contextlib.suppress(Exception):
        sup.autostart = bool(autostart and autostart.is_enabled())
    with contextlib.suppress(Exception):  # T-133: both editions (the client may register this one task)
        sup.wake_task = sup.wake_available and wake_task.is_enabled()
    try:
        import pystray

        icons = {s: make_state_icon(s) for s in STATES}
    except Exception:  # no tray is not fatal: stop.cmd (data/QUIT) still ends us
        log.exception("tray: no system tray available; running headless")
        sup.run()
        return 0

    def toggle_autostart() -> None:
        if autostart is None:
            log.warning("tray: tools/autostart.py unavailable")
            return
        want = not sup.autostart
        ok = autostart.enable() if want else autostart.disable()
        sup.autostart = autostart.is_enabled()
        log.info("tray: autostart %s -> %s (ok=%s)", "enable" if want else "disable", sup.autostart, ok)

    def toggle_wake() -> None:
        if not sup.wake_available:
            log.warning("tray: the wake task needs Windows Task Scheduler")
            return
        want = not sup.wake_task
        ok = wake_task.enable() if want else wake_task.disable()
        sup.wake_task = wake_task.is_enabled()
        log.info("tray: wake task %s -> %s (ok=%s)", "enable" if want else "disable", sup.wake_task, ok)
        with contextlib.suppress(Exception):
            sup.on_change()

    def open_logs() -> None:
        logs_dir.mkdir(parents=True, exist_ok=True)
        startfile = getattr(os, "startfile", None)
        if startfile is not None:
            startfile(logs_dir)
        else:
            webbrowser.open(logs_dir.as_uri())

    actions: dict[str, Callable[[], Any]] = {
        "open": lambda: webbrowser.open(f"http://127.0.0.1:{sup.port}"),
        "start": lambda: sup.request("start"),
        "stop": lambda: sup.request("stop"),
        "pause": sup.toggle_pause,
        "autostart": toggle_autostart,
        "wake": toggle_wake,
        "logs": open_logs,
        "quit": lambda: sup.request("quit"),
    }

    def action_for(key: str) -> Callable[[], None]:
        def act() -> None:  # zero-arg: pystray picks the call shape from the arg count
            try:
                if key.startswith("job:"):
                    _, name, kind = key.split(":", 2)
                    sup.run_job(name, kind)
                else:
                    actions[key]()
            except Exception:  # a menu click must never kill the tray
                log.exception("tray: menu action %s failed", key)

        return act

    def checked_as(value: bool) -> Callable[[Any], bool]:
        return lambda _item: value

    def to_item(spec: Item | None) -> Any:
        if spec is None:
            return pystray.Menu.SEPARATOR
        kwargs: dict[str, Any] = {"enabled": spec.enabled, "default": spec.default}
        if spec.checked is not None:
            kwargs["checked"] = checked_as(spec.checked)
        return pystray.MenuItem(spec.label, action_for(spec.action) if spec.action else None, **kwargs)

    menu = pystray.Menu(lambda: [to_item(s) for s in menu_spec(sup.view())])
    icon = pystray.Icon("gamma-unwind", icons[sup.state], tooltip(sup.view()), menu)

    def refresh() -> None:
        v = sup.view()
        icon.icon = icons.get(v.state, icons[ERROR])
        icon.title = tooltip(v)
        icon.update_menu()

    def setup(ic: Any) -> None:
        ic.visible = True
        sup.on_change = refresh
        try:
            refresh()
            sup.run()
        finally:
            ic.stop()

    icon.run(setup=setup)
    return 0


# ---------------------------------------------------------------------------------------- entry
def setup_tray_logging(logs_dir: Path) -> None:
    """The `gammad` logger to logs/tray.log (+ stderr when there is one). Idempotent."""
    root = logging.getLogger("gammad")
    target = str((logs_dir / TRAY_LOG).resolve())
    if any(getattr(h, "baseFilename", None) == target for h in root.handlers):
        return
    logs_dir.mkdir(parents=True, exist_ok=True)
    fmt = logging.Formatter("%(asctime)s %(levelname)-7s %(name)s: %(message)s")
    fh = RotatingFileHandler(logs_dir / TRAY_LOG, maxBytes=1_000_000, backupCount=3, encoding="utf-8")
    fh.setFormatter(fmt)
    root.addHandler(fh)
    if sys.stderr is not None:
        sh = logging.StreamHandler(sys.stderr)
        sh.setFormatter(fmt)
        root.addHandler(sh)
    root.setLevel(logging.INFO)
    root.propagate = False


def supervise(
    *,
    root: Path,
    data_dir: Path,
    logs_dir: Path,
    port: int,
    launcher: Launcher = popen_launcher,
    http: Any = None,
    ui: Callable[[Supervisor, Path], int] = run_tray,
    stopped_page: bool = True,
    notify: Callable[[str], None] | None = None,
    notifier_factory: Callable[[], Callable[[str], None] | None] | None = None,
    edition_lock_dir: Path | None = None,
) -> int:
    lock = InstanceLock(data_dir / LOCK_FILE)
    if not lock.acquire():
        _touch(data_dir / START_FILE)
        log.info("tray: already running (pid %s); asked it to start the service (data/START)",
                 lock.holder_pid())
        return 0
    elock: InstanceLock | None = None
    if edition_lock_dir is not None and os.environ.get(SECOND_COPY_OK_ENV) == "1":
        log.info("tray: %s=1: the one-copy-per-edition guard is off (broker test day)", SECOND_COPY_OK_ENV)
    elif edition_lock_dir is not None:
        ed = _edition.edition()
        cand = InstanceLock(edition_lock_dir / EDITION_LOCK_FMT.format(edition=ed))
        try:
            held = cand.acquire(note=str(root))
        except OSError as exc:
            log.warning("tray: edition lock unavailable (%s); one-copy-per-edition guard off", exc)
        else:
            if not held:
                pid, other = cand.holder_pid(), cand.holder_note() or "another folder"
                lock.release()
                raise SystemExit(
                    f"{REFUSED} GAMMA-UNWIND ({ed} edition) is already running on this PC from {other} (process "
                    f"{pid or 'unknown'}). Only one {ed} copy may run at a time: Quit that one from its tray icon "
                    "first, then start this one."
                )
            elock = cand
    sup: Supervisor | None = None
    try:
        for name in (QUIT_FILE, START_FILE):  # requests left over from a previous run
            _unlink(data_dir / name)
        sup = Supervisor(root=root, data_dir=data_dir, port=port, launcher=launcher, http=http,
                         stopped_page=stopped_page, keep_awake=KeepAwake(), notify=notify,
                         notifier_factory=notifier_factory)  # fmt: skip
        log.info("tray: supervisor pid %s, port %d", os.getpid(), port)
        sup.start_service()
        return ui(sup, logs_dir)
    finally:
        if sup is not None and sup.proc is not None:
            sup.stop_service()
        if sup is not None and sup.page is not None:
            sup.page.close()
        if elock is not None:
            elock.release()
        lock.release()


def notifier_from_env(root: Path, *, post: Any = None, spawn: Any = None) -> Callable[[str], None] | None:
    """The tray's PERSONAL-chat sender from `<root>/.env` as it is NOW (load_settings, never the cached settings()):
    None = no bot token / no usable personal chat. Raises when the config cannot be read. `post` / `spawn` are
    injected by tests only (gammad.tray.notify.make_notifier: never a real send there)."""
    from gammad.config import load_settings
    from gammad.tray.notify import make_notifier

    return make_notifier(load_settings(root), post=post, spawn=spawn)


def main() -> int:
    logs_dir = REPO_ROOT / "logs"
    setup_tray_logging(logs_dir)
    notify: Callable[[str], None] | None = None
    try:
        from gammad.config import settings

        cfg = settings()
        port, data_dir, logs_dir = cfg.env.port, cfg.paths.data_dir, cfg.paths.logs_dir
    except Exception:  # a broken config must still leave a tray that shows the failure
        log.exception("tray: settings failed; using port %d and %s", DEFAULT_PORT, REPO_ROOT / "data")
        port, data_dir = DEFAULT_PORT, REPO_ROOT / "data"
    else:
        try:  # T-284: the tray's own PERSONAL-chat sender (None = tray.log only)
            from gammad.tray.notify import make_notifier

            notify = make_notifier(cfg)
        except Exception:
            log.exception("tray: phone lines off (notifier setup failed)")
        log.info("tray: phone lines to the personal chat: %s", "on" if notify is not None else "off")
    return supervise(root=REPO_ROOT, data_dir=data_dir, logs_dir=logs_dir, port=port, notify=notify,
                     notifier_factory=lambda: notifier_from_env(REPO_ROOT),
                     edition_lock_dir=default_edition_lock_dir())  # fmt: skip
