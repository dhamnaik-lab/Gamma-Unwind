"""System-tray icon (pystray) for the supervisor. Runs pystray in its own daemon thread.

Every action that touches the runtime is marshalled into the asyncio loop with
`loop.call_soon_threadsafe`; the tray thread only reads plain attributes (paused flag, token level).
pystray / Pillow are imported inside the functions, so importing this module never needs a display.
"""

from __future__ import annotations

import contextlib
import importlib.util
import os
import threading
import webbrowser
from collections.abc import Callable
from pathlib import Path
from types import ModuleType
from typing import TYPE_CHECKING, Any

from gammad import REPO_ROOT
from gammad.log import get_logger

if TYPE_CHECKING:
    import asyncio

    from gammad.app import Runtime
    from gammad.config import Settings

log = get_logger(__name__)

ICON_PX = 64
TOKEN_REFRESH_S = 30.0
GLYPH = "\N{GREEK SMALL LETTER GAMMA}"
FONT_CANDIDATES = ("seguisym.ttf", "segoeui.ttf", "arial.ttf", "DejaVuSans.ttf")


def make_icon_image(size: int = ICON_PX) -> Any:
    """Dark circle with a gamma glyph, as a Pillow RGBA image."""
    from PIL import Image, ImageDraw, ImageFont

    img = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)
    pad = max(1, size // 32)
    draw.ellipse((pad, pad, size - pad - 1, size - pad - 1), fill=(22, 27, 34, 255), outline=(88, 166, 255, 255),
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
    draw.text(((size - w) / 2 - box[0], (size - h) / 2 - box[1]), GLYPH, font=font, fill=(88, 166, 255, 255))
    return img


def _autostart_module() -> ModuleType | None:
    """tools/autostart.py (the owner's logon task), or None. T-085 / D90(4): the CLIENT edition never loads
    it (the package does not ship tools/; a client starts GAMMA itself) and an absent file is not an error --
    both return None with one INFO line, never an ERROR traceback."""
    from gammad import edition

    if edition.is_client():
        log.info("tray: autostart is owner-edition only; skipped (%s edition)", edition.edition())
        return None
    path = REPO_ROOT / "tools" / "autostart.py"
    if not path.is_file():
        log.info("tray: %s not present; autostart unavailable", path.name)
        return None
    spec = importlib.util.spec_from_file_location("gamma_tools_autostart", path)
    if spec is None or spec.loader is None:
        return None
    mod = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(mod)
    except (OSError, ImportError):
        log.exception("tray: cannot load %s", path)
        return None
    return mod


class TrayHandle:
    def __init__(self, icon: Any, thread: threading.Thread, stop_refresh: threading.Event) -> None:
        self.icon = icon
        self.thread = thread
        self._stop_refresh = stop_refresh

    def stop(self) -> None:
        self._stop_refresh.set()
        with contextlib.suppress(Exception):
            self.icon.stop()


def start_tray(runtime: Runtime, loop: asyncio.AbstractEventLoop, cfg: Settings) -> TrayHandle:
    import pystray

    port = cfg.env.port
    logs_dir: Path = cfg.paths.logs_dir
    autostart = _autostart_module()
    state = {"autostart": bool(autostart and autostart.is_enabled())}

    def in_loop(fn: Callable[..., Any], *args: Any) -> None:
        loop.call_soon_threadsafe(fn, *args)

    def open_dashboard(_icon: Any = None, _item: Any = None) -> None:
        webbrowser.open(f"http://127.0.0.1:{port}")

    def run(name: str, kind: str) -> Callable[[Any, Any], None]:
        def _go(_icon: Any = None, _item: Any = None) -> None:
            in_loop(runtime.spawn_job, name, kind)

        return _go

    def toggle_pause(_icon: Any = None, _item: Any = None) -> None:
        in_loop(runtime.set_paused, not runtime.alerts_paused)

    def token_label(_item: Any = None) -> str:
        try:
            return f"Token: {runtime.token_status()['level']}"
        except Exception:  # noqa: BLE001 -- the label must never crash the menu
            return "Token: unknown"

    def toggle_autostart(_icon: Any = None, _item: Any = None) -> None:
        if autostart is None:
            log.warning("tray: tools/autostart.py unavailable")
            return
        want = not state["autostart"]
        ok = autostart.enable() if want else autostart.disable()
        state["autostart"] = autostart.is_enabled()
        log.info("tray: autostart %s -> %s (ok=%s)", "enable" if want else "disable", state["autostart"], ok)

    def open_logs(_icon: Any = None, _item: Any = None) -> None:
        logs_dir.mkdir(parents=True, exist_ok=True)
        startfile = getattr(os, "startfile", None)
        if startfile is not None:
            startfile(logs_dir)
        else:
            webbrowser.open(logs_dir.as_uri())

    def quit_app(icon: Any, _item: Any = None) -> None:
        in_loop(runtime.request_stop)
        icon.stop()

    menu = pystray.Menu(
        pystray.MenuItem("Open dashboard", open_dashboard, default=True),
        pystray.Menu.SEPARATOR,
        pystray.MenuItem("Run 09:20 now", run("mark_levels", "0920")),
        pystray.MenuItem("Run a refresh now", run("build_watchlists", "1315")),
        pystray.MenuItem("Run 16:00 now", run("mark_levels", "1600")),
        pystray.Menu.SEPARATOR,
        pystray.MenuItem("Pause alerts", toggle_pause, checked=lambda _i: runtime.alerts_paused),
        pystray.MenuItem(token_label, None, enabled=False),
        pystray.MenuItem("Autostart on login", toggle_autostart, checked=lambda _i: state["autostart"],
                         visible=autostart is not None),
        pystray.MenuItem("Open logs folder", open_logs),
        pystray.Menu.SEPARATOR,
        pystray.MenuItem("Quit", quit_app),
    )
    icon = pystray.Icon("gamma-unwind", make_icon_image(), "GAMMA-UNWIND", menu)
    stop_refresh = threading.Event()

    def refresh() -> None:
        while not stop_refresh.wait(TOKEN_REFRESH_S):
            with contextlib.suppress(Exception):
                icon.update_menu()

    thread = threading.Thread(target=icon.run, name="tray", daemon=True)
    thread.start()
    threading.Thread(target=refresh, name="tray-refresh", daemon=True).start()
    log.info("tray started")
    return TrayHandle(icon, thread, stop_refresh)
