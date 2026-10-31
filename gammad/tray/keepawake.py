"""Keep the PC awake while the market is open (T-133 / D119): SetThreadExecutionState, no admin, both editions.

`SetThreadExecutionState(ES_CONTINUOUS | ES_SYSTEM_REQUIRED)` tells Windows not to sleep for idleness until the
SAME THREAD calls `SetThreadExecutionState(ES_CONTINUOUS)` or ends. The flag is per thread, so the tray
supervisor calls `KeepAwake.sync` from its own long-lived control-loop thread on every tick (never from a menu
click or a short worker thread, which would drop the flag when it ends). The display is still allowed to turn
off (no ES_DISPLAY_REQUIRED): only the system is held. Off Windows every call is a no-op and the state is "off".
"""

from __future__ import annotations

import os
from collections.abc import Callable
from datetime import date, datetime, time, timedelta, timezone

from gammad.core import session_clock
from gammad.log import get_logger

log = get_logger(__name__)

ES_CONTINUOUS = 0x80000000
ES_SYSTEM_REQUIRED = 0x00000001
IST = timezone(timedelta(hours=5, minutes=30))
AWAKE_FROM = time(8, 50)
AWAKE_AFTER_CLOSE = timedelta(minutes=10)


def awake_until(d: date) -> time:
    return session_clock.clock_after(d, "fo_close", AWAKE_AFTER_CLOSE)

ON, OFF = "on", "off"

SetState = Callable[[int], int]


def want_awake(now: datetime, is_trading_day: Callable[[date], bool], running: bool) -> bool:
    """True within the keep-awake window on a trading day while the service is RUNNING.

    08:50, not 09:00: the "Wake PC at 08:55" task wakes a sleeping PC, and Windows puts a PC that a timer woke
    with nobody at it back to sleep after about 2 min of idleness. Holding the system from 08:50 means the
    08:55 wake finds the request already due on the next tick, so the PC stays up through the 09:15 open.
    The era close plus AWAKE_AFTER_CLOSE leaves the post-close jobs time to finish. A naive `now` is IST.
    """
    if not running:
        return False
    local = now.astimezone(IST) if now.tzinfo is not None else now
    minute = local.time().replace(second=0, microsecond=0)
    if not AWAKE_FROM <= minute <= awake_until(local.date()):
        return False
    return bool(is_trading_day(local.date()))


def _windows_set_state() -> SetState | None:
    if os.name != "nt":
        return None
    import ctypes

    fn = ctypes.windll.kernel32.SetThreadExecutionState  # type: ignore[attr-defined]
    fn.argtypes = [ctypes.c_uint32]
    fn.restype = ctypes.c_uint32
    return fn  # type: ignore[no-any-return]


class KeepAwake:
    """`sync(want)` holds or releases the system-required flag on the CALLING thread; it only calls the API on
    a change, so calling it every tick is cheap. Returns "on" / "off" (what the tray tooltip shows)."""

    def __init__(self, set_state: SetState | None = None) -> None:
        self._set_state = set_state if set_state is not None else _windows_set_state()
        self.state = OFF
        self._warned = False

    def sync(self, want: bool) -> str:
        if self._set_state is None or want == (self.state == ON):
            return self.state
        flags = ES_CONTINUOUS | ES_SYSTEM_REQUIRED if want else ES_CONTINUOUS
        try:
            ok = self._set_state(flags) != 0  # 0 = the call failed; else the previous state
        except Exception:  # the tray must never die over a power hint
            log.warning("tray: SetThreadExecutionState(0x%08X) raised", flags, exc_info=True)
            ok = False
        if ok:
            self.state = ON if want else OFF
            log.info("tray: keep-awake %s", self.state)
        elif not self._warned:
            self._warned = True
            log.warning("tray: SetThreadExecutionState(0x%08X) failed; keep-awake stays %s", flags, self.state)
        return self.state
