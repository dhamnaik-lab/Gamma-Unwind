"""U4 volume-unit guard, ONE per adapter (audit P2 brokers-beta fix round, D726).

Noren documents `v` only as "Volume" (shares or lots is not stated). A value is admitted as QUANTITY only when it
is a whole multiple of the contract's canonical lot (every F&O trade is whole lots, so day quantity always is). The
first value that is not switches volume off for the rest of the IST day, for EVERY contract of this broker:
  * the adapter owns the guard and the feed borrows it (`adapter.volume_guard`), so a feed rebuilt on restart keeps
    the switch-off (app.py builds a new feed on every feed start);
  * the switch-off is logged once, kept in this process for `/api/status` (registry.describe -> runtime_reasons),
    and written to `<data_dir>/state/<broker>-volume-unit.json` so Check Setup (another process) and a same-day
    restart see it too.
Bounded effect when a lots-unit stream happens to pass (every value a coincidental multiple): the volume reads TOO LOW
by the lot factor, never too high, and every volume consumer is a lower floor (lots x lot size) or a scale-free ratio
(volume vs its own average, VWAP weights), so the miss can only cost signals, never add one.
"""

from __future__ import annotations

import json
import threading
from collections.abc import Callable
from pathlib import Path

from gammad.broker.noren import wire
from gammad.config import now_ist
from gammad.log import get_logger

log = get_logger(__name__)

STATE_DIR = "state"
_OFF: dict[str, str] = {}  # broker -> IST day (iso) its volume was switched off in THIS process
_OFF_LOCK = threading.Lock()


def reason(broker: str) -> str:
    """The one plain line status and Check Setup show (no internal condition names, D429 (2))."""
    return (f"scanner idle: {broker} volume could not be read reliably today (not a whole number of lots): "
            "volume checks are off, so no new alerts until tomorrow")


def today_iso() -> str:
    return now_ist().date().isoformat()


def state_path(data_dir: Path | str, broker: str) -> Path:
    return Path(data_dir) / STATE_DIR / f"{broker}-volume-unit.json"


def _read_day(data_dir: Path | str | None, broker: str) -> str | None:
    if data_dir is None:
        return None
    try:
        day = json.loads(state_path(data_dir, broker).read_text(encoding="utf-8")).get("day")
    except (OSError, ValueError, AttributeError):
        return None
    return day if isinstance(day, str) else None


def runtime_reasons(broker: str, data_dir: Path | str | None = None, *,
                    today: Callable[[], str] = today_iso) -> list[str]:
    """[reason] when this broker's volume is switched off today (this process, or the state file), else []."""
    day = today()
    with _OFF_LOCK:
        hit = _OFF.get(broker) == day
    return [reason(broker)] if hit or _read_day(data_dir, broker) == day else []


class VolumeUnitGuard:
    def __init__(self, broker: str, data_dir: Path | str | None = None, *,
                 today: Callable[[], str] = today_iso) -> None:
        self.broker = broker
        self.data_dir = Path(data_dir) if data_dir is not None else None
        self._today = today
        self._lock = threading.Lock()
        self.rejects = 0
        self._off_day = _read_day(self.data_dir, broker)  # a same-day restart keeps the switch-off
        if self._off_day == today():
            with _OFF_LOCK:
                _OFF[broker] = self._off_day

    @property
    def off(self) -> bool:
        return self._off_day == self._today()

    def admit(self, value: object, lot: int | None, where: str) -> int | None:
        """`value` as QUANTITY, or None (switched off, unknown lot, or not a lot multiple -> switch off)."""
        if self.off:
            return None
        volume = wire.quantity_volume(value, lot)
        if volume is None and lot and wire.integer(value) is not None:
            self._switch_off(value, lot, where)
        return volume

    def _switch_off(self, value: object, lot: int, where: str) -> None:
        day = self._today()
        with self._lock:
            self.rejects += 1
            if self._off_day == day:
                return
            self._off_day = day
        with _OFF_LOCK:
            _OFF[self.broker] = day
        log.warning("%s: volume %s is not a multiple of lot %s for %s -- not in shares; volume switched off for %s",
                    self.broker, value, lot, where, day)
        if self.data_dir is not None:
            path = state_path(self.data_dir, self.broker)
            try:
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(json.dumps({"day": day, "value": str(value), "lot": lot, "where": where}),
                                encoding="utf-8")
            except OSError:
                log.exception("%s: volume switch-off could not be saved for Check Setup", self.broker)
