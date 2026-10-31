"""Telegram Bot API sender over httpx.AsyncClient.

- `send(chat_id, text, *, reply_to_message_id=None, buttons=None, silent=False)` posts sendMessage (HTML parse
  mode, no link preview; optional reply_parameters / inline keyboard of URL and copy_text buttons, each button
  optionally coloured by `style` (Bot API 9.4); `silent` = disable_notification) and returns the sent Message.
- A 400 carrying `parameters.migrate_to_chat_id` (basic group upgraded to a supergroup) raises TelegramError
  with `migrate_to_chat_id` set; following it is the router's decision (GROUP only, CLIENT-PACKAGE-SPEC §3).
- T-181 F3: a GROUP send whose TELEGRAM_TOPIC_ID names a missing / closed / deleted topic (400 "message thread not
  found", TOPIC_CLOSED, TOPIC_DELETED) is retried ONCE without message_thread_id (it lands in General), one ERROR
  names the key, and later group sends of this process skip the dead topic. `send_chat_action` lets the readiness
  check and Check Setup test the topic without posting anything. 0.8.2 T-213 F9: `topic_dead` / `topic_id` let the
  router send the owner ONE personal SYSTEM line per process when that happens.
- HTTP 429: sleeps `parameters.retry_after` (or the Retry-After header) and retries, up to
  `max_429_retries` times.
- HTTP 5xx and transport errors: retried up to `max_5xx_retries` (3) times after the first attempt,
  backoff 0.5 s, 1 s, 2 s.
- Any other failure raises TelegramError at once.
- The bot token is part of the URL path. It never appears in our log lines or exception text, and a
  filter on the `httpx` logger redacts it from httpx's own "HTTP Request: POST ..." INFO line, which
  would otherwise carry it to any root handler (uvicorn, pytest caplog, a later basicConfig).
"""

from __future__ import annotations

import asyncio
import logging
import re
from collections.abc import Awaitable, Callable
from typing import Any

import httpx

from gammad.log import get_logger

log = get_logger(__name__)

API_BASE = "https://api.telegram.org"
_TOKEN_RE = re.compile(r"bot\d+:[A-Za-z0-9_-]+")
_REDACTED = "bot<redacted>"


def redact(text: str) -> str:
    return _TOKEN_RE.sub(_REDACTED, text)


class _TokenRedactor(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        try:
            message = record.getMessage()
        except (TypeError, ValueError):
            return True  # a malformed record is logging's problem, not ours to swallow
        record.msg = redact(message)
        record.args = None
        return True


_REDACTOR = _TokenRedactor()


def install_httpx_redactor() -> None:
    """Idempotent: attach the redacting filter to the loggers httpx writes request lines to."""
    for name in ("httpx", "httpcore"):
        lg = logging.getLogger(name)
        if _REDACTOR not in lg.filters:
            lg.addFilter(_REDACTOR)


# Bot API 7.11 `copy_text` inline button: the text to copy must be 1..256 characters.
COPY_TEXT_MAX = 256
Button = dict[str, Any]
Keyboard = list[list[Button]]


# Bot API 9.4 InlineKeyboardButton.style (T-162, D129): the button colour. An older Bot API server or client ignores
# an unknown key and draws the default button, so sending it is safe everywhere.
BUTTON_STYLES = frozenset({"success", "primary", "danger"})


def _styled(button: Button, style: str | None) -> Button:
    if style is None:
        return button
    if style not in BUTTON_STYLES:
        raise ValueError(f"button style must be one of {sorted(BUTTON_STYLES)}, got {style!r}")
    return {**button, "style": style}


def url_button(text: str, url: str, *, style: str | None = None) -> Button:
    """An inline keyboard button that opens `url` (D64 ticket: chart / NSE quote page). `style` (Bot API 9.4:
    "success" green, "primary" blue, "danger" red) adds the "style" key; None = no key (the default grey button).
    Servers / apps older than 9.4 ignore the key, so a styled button still works there, uncoloured."""
    return _styled({"text": text, "url": url}, style)


def copy_button(text: str, copy: str, *, style: str | None = None) -> Button:
    """An inline `copy_text` button (Bot API 7.11, D79 "Copy order"): a tap copies `copy` to the clipboard.
    ValueError when `copy` is empty or longer than COPY_TEXT_MAX (Telegram would refuse the WHOLE message).
    `style` as in `url_button` (T-162: the ENTRY card's Copy order is "success")."""
    if not copy or len(copy) > COPY_TEXT_MAX:
        raise ValueError(f"copy_text must be 1..{COPY_TEXT_MAX} characters, got {len(copy or '')}")
    return _styled({"text": text, "copy_text": {"text": copy}}, style)


class TelegramError(Exception):
    """A failed Bot API call. `migrate_to_chat_id` is set when Telegram answered that the chat was upgraded
    to a supergroup (`parameters.migrate_to_chat_id`, CLIENT-PACKAGE-SPEC §3); the caller decides whether to
    follow it (the router follows it for the configured GROUP only, never for the PERSONAL chat)."""

    def __init__(
        self,
        status: int | None,
        description: str,
        retry_after: float | None = None,
        migrate_to_chat_id: int | None = None,
    ) -> None:
        self.status = status
        self.description = redact(description)
        self.retry_after = retry_after
        self.migrate_to_chat_id = migrate_to_chat_id
        super().__init__(f"telegram {status}: {self.description}")


class TelegramClient:
    def __init__(
        self,
        token: str,
        *,
        client: httpx.AsyncClient | None = None,
        base_url: str = API_BASE,
        max_5xx_retries: int = 3,
        max_429_retries: int = 5,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        timeout: float = 15.0,
        group_id: str | int | None = None,
        topic_id: str | int | None = None,
    ) -> None:
        """group_id + topic_id (T-134(1), D119 G3, D131): when both are set, every send to `group_id` carries
        `message_thread_id = topic_id` (a forum supergroup's topic); any other chat, or a blank topic, is unchanged."""
        if not token:
            raise ValueError("telegram bot token is empty")
        install_httpx_redactor()
        self._token = token
        self._group_id = str(group_id).strip() if group_id not in (None, "") else ""
        try:
            self._topic_id: int | None = int(str(topic_id).strip()) if topic_id not in (None, "") else None
        except ValueError:
            log.warning("TELEGRAM_TOPIC_ID %r is not a number: group sends go to General", topic_id)
            self._topic_id = None
        self._client = client or httpx.AsyncClient(timeout=timeout)
        self._owns_client = client is None
        self._base = base_url.rstrip("/")
        self._max_5xx = max_5xx_retries
        self._max_429 = max_429_retries
        self._sleep = sleep
        self._topic_dead = False  # T-181 F3: the topic answered "missing" once; later group sends skip it
        self._topic_error_logged = False

    def __repr__(self) -> str:
        return f"TelegramClient(base={self._base!r}, token=<redacted>)"

    @property
    def topic_id(self) -> int | None:
        """The TELEGRAM_TOPIC_ID group sends carry (None = blank / not a number: General)."""
        return self._topic_id

    @property
    def topic_dead(self) -> bool:
        """0.8.2 T-213 F9: True once the topic answered "missing" in this process (group sends now land in General);
        the router reads it after a group send to tell the owner once."""
        return self._topic_dead

    async def aclose(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    async def get_me(self) -> dict[str, Any]:
        """Startup check: returns the bot's user object (id, username)."""
        return await self._call("getMe", None)

    async def get_chat(self, chat_id: str | int) -> dict[str, Any]:
        """getChat: the chat object when the bot can see the chat (it is a member); a TelegramError otherwise
        (400 "chat not found" / 403 "bot is not a member"). Check Setup's group check (W14 P1)."""
        return await self._call("getChat", {"chat_id": chat_id})

    async def send(
        self,
        chat_id: str | int,
        text: str,
        *,
        parse_mode: str = "HTML",
        reply_to_message_id: int | None = None,
        buttons: Keyboard | None = None,
        silent: bool = False,
    ) -> dict[str, Any]:
        """sendMessage. Returns Telegram's `result` (the Message: has `message_id`).

        reply_to_message_id  -> reply_parameters {message_id, allow_sending_without_reply: true} (D65: an E1
                                replies to its E2; a deleted E2 must not cost the E1, hence the flag).
        buttons              -> reply_markup {inline_keyboard: buttons}: rows of `url_button` (D64) and
                                `copy_button` (D79 "Copy order") dicts.
        silent               -> disable_notification: true (T-162, D129: a STRONG WATCHLIST card arrives
                                without sound); the key is absent otherwise.
        Each key is sent only when given (an empty button list counts as not given). The link preview stays off
        with the legacy `disable_web_page_preview` (tests/test_telegram.py pins it; Bot API 7.0 renamed it
        `link_preview_options`, and the servers still accept the old key)."""
        payload: dict[str, Any] = {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": parse_mode,
            "disable_web_page_preview": True,
        }
        if silent:
            payload["disable_notification"] = True
        topic = self._topic_for(chat_id)
        if topic is not None:
            payload["message_thread_id"] = topic  # D119 G3: every group send lands in the topic
        if reply_to_message_id is not None:
            payload["reply_parameters"] = {
                "message_id": int(reply_to_message_id),
                "allow_sending_without_reply": True,
            }
        if buttons:
            payload["reply_markup"] = {"inline_keyboard": buttons}
        try:
            return await self._call("sendMessage", payload)
        except TelegramError as exc:
            if topic is None or not is_topic_error(exc):
                raise
            # T-181 F3 (R4): the topic is missing / closed / deleted, so EVERY group send would fail: this one is
            # retried ONCE without the thread id (it lands in General) and later group sends skip the dead topic
            self._topic_dead = True
            if not self._topic_error_logged:
                self._topic_error_logged = True
                log.error("telegram: the topic in %s (%s) is missing, closed or deleted (%s): group messages go to "
                          "General until %s is fixed (or left blank) and GAMMA is restarted",
                          TOPIC_KEY, self._topic_id, exc.description, TOPIC_KEY)  # fmt: skip
            payload.pop("message_thread_id", None)
            return await self._call("sendMessage", payload)

    def _topic_for(self, chat_id: str | int) -> int | None:
        """The message_thread_id a send to `chat_id` carries: the topic for the GROUP (unless it failed as missing in
        this process, F3), None for any other chat or a blank topic."""
        if self._topic_id is None or self._topic_dead or not self._group_id:
            return None
        return self._topic_id if str(chat_id).strip() == self._group_id else None

    async def send_chat_action(
        self, chat_id: str | int, action: str = "typing", *, message_thread_id: int | None = None
    ) -> Any:
        """sendChatAction (T-181 F3): the cheapest call that names a topic. With `message_thread_id` Telegram answers
        400 "message thread not found" (or TOPIC_CLOSED / TOPIC_DELETED) when the group has no such open topic, so the
        readiness check and Check Setup test TELEGRAM_TOPIC_ID with it; nothing is posted (a 'typing...' hint)."""
        payload: dict[str, Any] = {"chat_id": chat_id, "action": action}
        if message_thread_id is not None:
            payload["message_thread_id"] = int(message_thread_id)
        return await self._call("sendChatAction", payload)

    async def _call(self, method: str, payload: dict[str, Any] | None) -> dict[str, Any]:
        url = f"{self._base}/bot{self._token}/{method}"
        fails_5xx = 0
        fails_429 = 0
        while True:
            try:
                if payload is None:
                    resp = await self._client.get(url)
                else:
                    resp = await self._client.post(url, json=payload)
            except httpx.TransportError as exc:
                fails_5xx += 1
                detail = redact(f"{type(exc).__name__}: {exc}")
                if fails_5xx > self._max_5xx:
                    raise TelegramError(None, detail) from None
                log.warning("telegram %s transport error (%s), retry %d", method, detail, fails_5xx)
                await self._sleep(0.5 * 2 ** (fails_5xx - 1))
                continue

            body = _json(resp)
            if resp.status_code == 200 and body.get("ok"):
                result = body.get("result")
                return result if isinstance(result, dict) else {"result": result}

            description = str(body.get("description") or resp.reason_phrase or "")
            if resp.status_code == 429:
                fails_429 += 1
                retry_after = _retry_after(resp, body)
                if fails_429 > self._max_429:
                    raise TelegramError(429, description, retry_after)
                log.warning("telegram %s 429, retry_after %.1fs (attempt %d)", method, retry_after, fails_429)
                await self._sleep(retry_after)
                continue
            if resp.status_code >= 500:
                fails_5xx += 1
                if fails_5xx > self._max_5xx:
                    raise TelegramError(resp.status_code, description)
                log.warning("telegram %s %d, retry %d", method, resp.status_code, fails_5xx)
                await self._sleep(0.5 * 2 ** (fails_5xx - 1))
                continue
            raise TelegramError(resp.status_code, description, migrate_to_chat_id=_migrate_to(body))


def _json(resp: httpx.Response) -> dict[str, Any]:
    try:
        data = resp.json()
    except ValueError:
        return {}
    return data if isinstance(data, dict) else {}


def _migrate_to(body: dict[str, Any]) -> int | None:
    """`parameters.migrate_to_chat_id` of an error body (a basic group upgraded to a supergroup), else None."""
    params = body.get("parameters")
    if not isinstance(params, dict):
        return None
    raw = params.get("migrate_to_chat_id")
    if isinstance(raw, bool):
        return None
    try:
        return int(raw) if raw is not None else None
    except (TypeError, ValueError):
        return None


def _retry_after(resp: httpx.Response, body: dict[str, Any]) -> float:
    params = body.get("parameters")
    if isinstance(params, dict) and params.get("retry_after") is not None:
        try:
            return float(params["retry_after"])
        except (TypeError, ValueError):
            pass
    header = resp.headers.get("Retry-After")
    if header:
        try:
            return float(header)
        except ValueError:
            pass
    return 1.0


TOPIC_KEY = "TELEGRAM_TOPIC_ID"
# T-181 F3: what Telegram answers (400) when message_thread_id names no open topic of the chat
_TOPIC_ERROR_MARKS = ("thread not found", "topic_closed", "topic_deleted", "topic_id_invalid")
TOPIC_MISSING_TEXT = (
    f"the topic in {TOPIC_KEY} does not exist in the group (deleted, closed or a wrong number): every group message "
    f"fails; copy the topic id again, or leave {TOPIC_KEY} blank to post in General"
)


def is_topic_error(exc: BaseException) -> bool:
    """True when `exc` is Telegram refusing a message_thread_id (missing / closed / deleted topic), a 400."""
    if not isinstance(exc, TelegramError) or exc.status != 400:
        return False
    text = exc.description.lower()
    return any(mark in text for mark in _TOPIC_ERROR_MARKS)


def forum_topic_warning(chat: Any, topic_id: Any) -> str | None:
    """PLAN-W16 section 13 S0(4) (T-132 / T-134): the one sentence Check Setup and the 09:00 readiness check both
    print when the GROUP is a forum supergroup (getChat.is_forum) and TELEGRAM_TOPIC_ID is blank, so every group
    send lands in General; None otherwise (not a forum, a topic set, or a getChat body we cannot read)."""
    try:
        is_forum = bool(chat.get("is_forum")) if hasattr(chat, "get") else False
    except (AttributeError, TypeError, ValueError):
        return None
    if not is_forum or (topic_id is not None and str(topic_id).strip()):
        return None
    return "the group is a forum (topics) and TELEGRAM_TOPIC_ID is blank: cards land in General; set the topic id in .env"
