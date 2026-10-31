"""T-284 (1), service side: ONE line when live prices go silent in market hours, ONE when they return.

Pure: no clock, no I/O. The Runtime calls `observe` from its supervise loop and sends a returned line through
`router.send_system` (the PERSONAL chat; a client's own chat; never the group).

- Arms only while the market is open AND a feed runs AND the feed is not auth-blocked (the token path already
  speaks for a parked feed). The silence baseline is max(arm time, last tick), so yesterday's last tick or a
  feed started mid-session never reads as silence.
- Silent for MORE than `silent_s` (FEED_SILENT_S, or 1.5x a slow poll feed's own gap: set_feed_gap) -> ONE
  "stopped" line; nothing more until a tick newer than the silence start arrives -> ONE "back" line with the
  silent minutes.
- Market close, a stopped feed or an auth block disarms silently: an episode still open then gets no "back" line,
  and the next session arms afresh.
- State is in memory only: a restart re-arms (the service's own start line covers a restart).

The lines ship to clients: plain words only, no strategy or owner terms (CLIENT_LOG_HIDE-safe).
"""

from __future__ import annotations

from datetime import datetime

from gammad.config import IST

# owner number (D268(5) / T-284): DhanFeed reconnects at 60 s idle, so 120 s = a failed reconnect
FEED_SILENT_S = 120
# T-308 (4): a feed that declares its own longest normal silence (`max_tick_gap_s`; PollFeed: an id may be quoted
# only every 2nd cycle, Nuvama's SLOW / SHED = 150 s) alarms only past 1.5x that gap (half a gap of slack for a
# cycle's own duration and HTTP latency); never below FEED_SILENT_S. A feed without one (WebSocket) keeps 120 s.
FEED_GAP_SLACK = 1.5


def _hhmm(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000, IST).strftime("%H:%M")


class FeedAlarm:
    def __init__(self, silent_s: int = FEED_SILENT_S) -> None:
        self.base_s = silent_s
        self.silent_s = silent_s
        self._armed_ms: int | None = None  # when the current watch began (market open + feed running)
        self._silent_since_ms: int | None = None  # silence start of the episode a "stopped" line went out for

    def set_feed_gap(self, gap_s: object) -> None:
        """T-308 (4): the running feed's own longest normal tick gap in seconds (None / not a positive number = no
        declared gap): the limit becomes max(base, FEED_GAP_SLACK x gap). Called on every feed start."""
        ok = isinstance(gap_s, int | float) and not isinstance(gap_s, bool) and gap_s > 0
        self.silent_s = max(self.base_s, int(FEED_GAP_SLACK * gap_s)) if ok else self.base_s  # type: ignore[operator]

    @property
    def silent_since_ms(self) -> int | None:
        """The start of the silence an alarm is open for (None = no open episode). Read by the status block."""
        return self._silent_since_ms

    def _disarm(self) -> None:
        self._armed_ms = None
        self._silent_since_ms = None

    def observe(
        self, now_ms: int, market_open: bool, running: bool, auth_blocked: bool, last_tick_ms: int | None
    ) -> str | None:
        """One supervise pass. Returns the line to send, or None."""
        if not market_open or not running or auth_blocked:
            self._disarm()
            return None
        if self._armed_ms is None:
            self._armed_ms = now_ms
        if self._silent_since_ms is not None:
            if last_tick_ms is not None and last_tick_ms > self._silent_since_ms:
                gap_ms = last_tick_ms - self._silent_since_ms
                self._silent_since_ms = None
                minutes = max(1, (gap_ms + 30_000) // 60_000)
                return f"live prices are back at {_hhmm(last_tick_ms)} (stopped {minutes} min)"
            return None
        baseline = self._armed_ms if last_tick_ms is None else max(self._armed_ms, last_tick_ms)
        silent_ms = now_ms - baseline
        if silent_ms > self.silent_s * 1000:
            self._silent_since_ms = baseline
            return (
                f"live prices stopped at {_hhmm(baseline)}: no price for {silent_ms // 1000} s in market hours; "
                "alerts cannot fire until they return"
            )
        return None
