"""T-284 (D268(5)): the tray's own minimal Telegram sender -- the service is the thing that is down, so it cannot speak.

`make_notifier(cfg)` -> a `send(text)` callable, or None:
  None    no bot token, or no usable PERSONAL chat (a client without one = tray.log only). There is NO group path:
          the group id is read only to refuse a personal id equal to it.
  send    ONE `sendMessage {chat_id: <personal>, text}` (plain text, no parse mode) on a DAEMON thread: 10 s timeout,
          one try, never raises. A failure is logged with the exception TYPE or the HTTP status only, never the URL
          (the bot token is in the path; `gammad.log` also strips URL paths from the httpx loggers).

The personal chat follows the service's rules (`router.send_system` / `has_personal_chat`, the T-062 check
`cfg.telegram`): the check must say `personal_ok` AND `personal_set`, and the id is re-checked here (a positive number,
`config.PERSONAL_RE`, never equal to the group id), so even a wrong check object cannot point it at a group (a group
id is negative).
"""

from __future__ import annotations

import contextlib
import threading
from collections.abc import Callable
from typing import Any

from gammad.config import PERSONAL_RE
from gammad.log import get_logger

log = get_logger(__name__)

API_BASE = "https://api.telegram.org"
SEND_TIMEOUT_S = 10.0

Post = Callable[..., Any]
Spawn = Callable[[Callable[[], None]], None]


def personal_chat(cfg: Any) -> str | None:
    """The PERSONAL chat id the tray may send to, or None. Never the group: a group id is negative (PERSONAL_RE is
    digits only) and an id equal to the group's is refused outright."""
    env = getattr(cfg, "env", None)
    check = getattr(cfg, "telegram", None)
    if env is None or check is None:
        return None
    if not (getattr(check, "personal_ok", False) and getattr(check, "personal_set", False)):
        return None
    personal = str(getattr(env, "telegram_personal_chat_id", "") or "").strip()
    group = str(getattr(env, "telegram_group_id", "") or "").strip()
    if not PERSONAL_RE.fullmatch(personal) or personal == group:
        return None
    return personal


def _httpx_post(url: str, *, json: dict[str, Any], timeout: float) -> Any:
    import httpx

    return httpx.post(url, json=json, timeout=timeout)


def _daemon(fn: Callable[[], None]) -> None:
    threading.Thread(target=fn, name="tray-notify", daemon=True).start()


def make_notifier(cfg: Any, *, post: Post | None = None, spawn: Spawn | None = None) -> Callable[[str], None] | None:
    """`post` / `spawn` are injected by tests (never a real send there); the defaults are httpx and a daemon thread."""
    token = str(getattr(getattr(cfg, "env", None), "telegram_bot_token", "") or "").strip()
    chat = personal_chat(cfg)
    if not token or chat is None:
        return None
    url = f"{API_BASE}/bot{token}/sendMessage"
    do_post = post if post is not None else _httpx_post
    do_spawn = spawn if spawn is not None else _daemon

    def deliver(text: str) -> None:
        try:
            r = do_post(url, json={"chat_id": chat, "text": text}, timeout=SEND_TIMEOUT_S)
        except Exception as exc:  # noqa: BLE001 -- one try; the type only (an httpx message carries the URL)
            log.warning("tray: Telegram line not sent: %s", type(exc).__name__)
            return
        status = getattr(r, "status_code", None)
        if status != 200:
            desc = ""
            with contextlib.suppress(Exception):
                body = r.json()
                desc = str(body.get("description") or "")[:200] if isinstance(body, dict) else ""
            log.warning("tray: Telegram line not sent: HTTP %s %s", status, desc)
            return
        log.info("tray: Telegram line sent to the personal chat")

    def send(text: str) -> None:
        try:
            do_spawn(lambda: deliver(text))
        except Exception:  # noqa: BLE001 -- a phone line must never break the control loop
            log.warning("tray: Telegram line not sent: could not start the sender thread")

    return send
