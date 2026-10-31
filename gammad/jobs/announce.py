"""The start announcement (T-134(2), D119 G3, PLAN-W16 section 13.3 B4).

`announce_start(router, data_dir, *, version, edition, broker, now)` runs once per service start:
  first start EVER of this install (no data/install.json): ONE group card "GAMMA-UNWIND <version> connected ·
      <edition> · <broker>" with sound. The marker is written ONLY after the send succeeds, so a failed send is
      retried on the next start. -> "first"
  every later start: ONE line to the PERSONAL chat, silent. A blank personal chat gets nothing (D119 G3: never the
      group), and no line is sent within START_LINE_THROTTLE_S of the last line (or of the install card), so a
      crash-restart loop cannot spam. -> "line"
  anything else (throttled, no personal chat, a failed send, an error) -> "none"

Marker data/install.json = {installed_at_ist, version, edition, first_card_message_id, last_line_ist}. An unreadable
marker counts as PRESENT (a second group card is the worse failure); it is rewritten on the next line sent.
Never raises: the runtime spawns it as a background task.
"""

from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Literal

from gammad.alerts import router as router_mod
from gammad.alerts import templates as tpl
from gammad.config import IST
from gammad.log import get_logger

log = get_logger(__name__)

MARKER_NAME = "install.json"
START_LINE_THROTTLE_S = 600  # 10 minutes
Outcome = Literal["first", "line", "none"]


def marker_path(data_dir: Path | str) -> Path:
    return Path(data_dir) / MARKER_NAME


def read_marker(data_dir: Path | str) -> dict[str, Any] | None:
    """The marker as a dict; {} when the file exists but does not parse; None when there is no file."""
    path = marker_path(data_dir)
    if not path.is_file():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        log.warning("start announce: %s unreadable (%s); treated as installed", path.name, type(exc).__name__)
        return {}
    return data if isinstance(data, dict) else {}


def write_marker(data_dir: Path | str, data: dict[str, Any]) -> None:
    """Atomic: a temp file then os.replace, so a crash never leaves half a marker."""
    path = marker_path(data_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(tmp, path)


def _ist(dt: datetime) -> datetime:
    return dt.replace(tzinfo=IST) if dt.tzinfo is None else dt.astimezone(IST)


def _parse(raw: Any) -> datetime | None:
    try:
        return _ist(datetime.fromisoformat(str(raw))) if raw else None
    except ValueError:
        return None


async def announce_start(
    router: Any, data_dir: Path | str, *, version: str, edition: str, broker: str, now: datetime
) -> Outcome:
    try:
        return await _announce(
            router, Path(data_dir), version=version, edition=edition, broker=broker, now=_ist(now)
        )
    except Exception:
        log.exception("start announce failed")
        return "none"


async def _announce(
    router: Any, data_dir: Path, *, version: str, edition: str, broker: str, now: datetime
) -> Outcome:
    stamp = now.isoformat(timespec="seconds")
    marker = read_marker(data_dir)
    if marker is None:
        ok, message_id = await router.send_ops(router_mod.KIND_INSTALL_CARD, tpl.install_card(version, edition, broker),
                                               dest=router_mod.DEST_GROUP, silent=False)  # fmt: skip
        if not ok:
            log.warning("start announce: the install card was not sent; it is retried on the next start")
            return "none"
        write_marker(data_dir, {"installed_at_ist": stamp, "version": version, "edition": edition,
                                "first_card_message_id": message_id, "last_line_ist": None})  # fmt: skip
        log.info("start announce: install card sent to the group")
        return "first"
    if not router.has_personal_chat():
        return "none"
    last = _parse(marker.get("last_line_ist")) or _parse(marker.get("installed_at_ist"))
    if last is not None and 0 <= (now - last).total_seconds() < START_LINE_THROTTLE_S:
        log.info("start announce: a start line went out at %s; none within %d min", last.strftime("%H:%M"),
                 START_LINE_THROTTLE_S // 60)  # fmt: skip
        return "none"
    ok, _ = await router.send_ops(router_mod.KIND_START_LINE, tpl.start_line(version, edition, broker, now),
                                  dest=router_mod.DEST_PERSONAL, silent=True)  # fmt: skip
    if not ok:
        return "none"
    write_marker(data_dir, {**marker, "last_line_ist": stamp})
    return "line"
