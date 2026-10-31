"""The "Wake PC at 08:55" Task Scheduler entry (T-133 / D119): one per-user task, no admin, BOTH editions.

The CLIENT edition may register exactly this one task (it never loads tools/autostart.py, which the package
does not ship; that is why this lives in gammad/tray/, not in tools/). The task "GAMMA-UNWIND-WAKE":
  trigger   weekly, Monday-Friday, 08:55 IST (StartBoundary carries +05:30, so a PC clock in another zone still
            fires at 08:55 IST); on an NSE weekday holiday it fires too, which is harmless: keep-awake is not
            held on a holiday, so the PC goes back to sleep.
  wake      <WakeToRun>true</WakeToRun>: Windows wakes the PC from sleep for it (not from shutdown).
  action    `<pythonw next to sys.executable> "<root>\\run_gamma.pyw"` in the package root. Idempotent: when a
            tray is already running, the second launch writes data/START (start the service if stopped) and
            exits 0 -- a second Start hands over.
  who       the CURRENT user, InteractiveToken, LeastPrivilege (runs only while that user is signed in; the PC
            asleep with the user signed in is exactly the case it is for).
Registered from an XML definition (schtasks /Create /XML), the pattern of tools/autostart.py. Every schtasks
call goes through an injectable `runner`, so tests never spawn the real one.
"""

from __future__ import annotations

import contextlib
import getpass
import os
import subprocess
import sys
import tempfile
from collections.abc import Callable
from pathlib import Path
from xml.sax.saxutils import escape

from gammad import REPO_ROOT
from gammad.log import get_logger

log = get_logger(__name__)

WAKE_TASK_NAME = "GAMMA-UNWIND-WAKE"
WAKE_AT = "08:55"
LAUNCHER = "run_gamma.pyw"

Runner = Callable[..., "subprocess.CompletedProcess[str]"]


def available() -> bool:
    """Task Scheduler exists on Windows only; the edition does not matter (both editions get the item)."""
    return os.name == "nt"


def _user_id() -> str:
    domain = os.environ.get("USERDOMAIN", "")
    user = os.environ.get("USERNAME") or getpass.getuser()
    return f"{domain}\\{user}" if domain else user


def task_paths(root: Path = REPO_ROOT, executable: str | None = None) -> tuple[Path, Path]:
    """(pythonw next to the running interpreter, <root>/run_gamma.pyw)."""
    return Path(executable or sys.executable).with_name("pythonw.exe"), root / LAUNCHER


def build_wake_xml(root: Path = REPO_ROOT, user_id: str | None = None, *, executable: str | None = None) -> str:
    pythonw, launcher = task_paths(root, executable)
    uid = escape(user_id or _user_id())
    return f"""<?xml version="1.0" encoding="UTF-16"?>
<Task version="1.2" xmlns="http://schemas.microsoft.com/windows/2004/02/mit/task">
  <RegistrationInfo>
    <Description>GAMMA-UNWIND: wake the PC at {WAKE_AT} on weekdays and start GAMMA</Description>
  </RegistrationInfo>
  <Triggers>
    <CalendarTrigger>
      <StartBoundary>2026-01-01T{WAKE_AT}:00+05:30</StartBoundary>
      <Enabled>true</Enabled>
      <ScheduleByWeek>
        <DaysOfWeek>
          <Monday />
          <Tuesday />
          <Wednesday />
          <Thursday />
          <Friday />
        </DaysOfWeek>
        <WeeksInterval>1</WeeksInterval>
      </ScheduleByWeek>
    </CalendarTrigger>
  </Triggers>
  <Principals>
    <Principal id="Author">
      <UserId>{uid}</UserId>
      <LogonType>InteractiveToken</LogonType>
      <RunLevel>LeastPrivilege</RunLevel>
    </Principal>
  </Principals>
  <Settings>
    <MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy>
    <DisallowStartIfOnBatteries>false</DisallowStartIfOnBatteries>
    <StopIfGoingOnBatteries>false</StopIfGoingOnBatteries>
    <WakeToRun>true</WakeToRun>
    <ExecutionTimeLimit>PT0S</ExecutionTimeLimit>
    <Enabled>true</Enabled>
  </Settings>
  <Actions Context="Author">
    <Exec>
      <Command>{escape(str(pythonw))}</Command>
      <Arguments>"{escape(str(launcher))}"</Arguments>
      <WorkingDirectory>{escape(str(root))}</WorkingDirectory>
    </Exec>
  </Actions>
</Task>
"""


def _schtasks(*args: str) -> subprocess.CompletedProcess[str]:
    """The real runner. Off Windows: rc 1, nothing spawned. No console window (the tray is pythonw)."""
    if not available():
        return subprocess.CompletedProcess(["schtasks", *args], 1, "", "not Windows")
    return subprocess.run(
        ["schtasks", *args],
        capture_output=True,
        text=True,
        check=False,
        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
    )


def _out(cp: subprocess.CompletedProcess[str]) -> str:
    return ((cp.stdout or "") + (cp.stderr or "")).strip()


def is_enabled(runner: Runner = _schtasks) -> bool:
    return runner("/Query", "/TN", WAKE_TASK_NAME).returncode == 0


def enable(runner: Runner = _schtasks, *, root: Path = REPO_ROOT, user_id: str | None = None,
           executable: str | None = None) -> bool:  # fmt: skip
    """Register (or replace, /F) the wake task. True on schtasks exit 0 (its text is localised: not parsed)."""
    fd, tmp = tempfile.mkstemp(suffix=".xml", prefix="gamma-wake-")
    os.close(fd)
    xml_path = Path(tmp)
    try:
        xml_path.write_text(build_wake_xml(root, user_id, executable=executable), encoding="utf-16")
        cp = runner("/Create", "/TN", WAKE_TASK_NAME, "/XML", str(xml_path), "/F")
    finally:
        with contextlib.suppress(OSError):
            xml_path.unlink(missing_ok=True)
    log.info("tray: wake task enable: rc=%s %s", cp.returncode, _out(cp))
    return cp.returncode == 0


def disable(runner: Runner = _schtasks) -> bool:
    cp = runner("/Delete", "/TN", WAKE_TASK_NAME, "/F")
    log.info("tray: wake task disable: rc=%s %s", cp.returncode, _out(cp))
    return cp.returncode == 0 or not is_enabled(runner)
