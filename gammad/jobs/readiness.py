"""The 09:00 readiness check (T-132, D119 G2, PLAN-W16 section 13.3 B4).

`ReadinessJob.run(now)` runs the seven checks in CHECK_NAMES order and sends ONE card: READY (silent) or
NOT READY: <first problem> (with sound), to the PERSONAL chat, or the group when the personal chat is blank
(router.send_ops dest personal_or_group). `recheck(now)` (09:10) runs only when today's last result was NOT READY;
it sends the all-clear card only when the result is now READY, and nothing on a second NOT READY (the dashboard
tile shows it). `status_block()` is /api/status.readiness: {ok, at_ist, checks: [{name, ok, detail}]} or None.
T-346 (D386 orchestrator fix, both editions): `refresh(now, why)` is the same re-check, run by the Runtime after every accepted
login / token change and after every successful list build (`stale(now)` says whether one is due), so a problem fixed
after 09:10 clears itself instead of leaving the 09:00 NOT READY on the dashboard all day; it never sends a second
NOT READY card the same day (only the all-clear, once, when the result turns READY).

The checks:
  token     the broker token must outlive today's era close (token_must_outlive): `token_status()` (the runtime's
            token block), falling back to the broker's own token_health() when that block does not know yet.
  env       `reload_env()` (config.reload_settings) re-reads .env without raising.
  telegram  getMe, then getChat(group); the detail carries telegram.forum_topic_warning(...) (a forum group with a
            blank TELEGRAM_TOPIC_ID: still ok, cards land in General) and that warning is logged ONCE per check as a
            WARNING line (the service never calls getChat at start, so this stands in for the settings-log WARN).
            T-181 F3: with TELEGRAM_TOPIC_ID set, sendChatAction into that topic; a missing / closed / deleted topic
            is a FAILED check (every group send would fail), in telegram.TOPIC_MISSING_TEXT.
  (card)    T-181 F9: while alerts are paused the card carries PAUSED_LINE.
  broker    adapter.token_health() (base.py; never a universe ltp(), D129 narrowed K3).
  disk      free space on the data folder's drive >= MIN_FREE_BYTES.
  clock     |Telegram server Date header - the PC clock| <= MAX_SKEW_S. An unreachable Telegram, or no Date header,
            is ok with the detail "not checked".
  data_dir  the data folder accepts a file write.
  calendar  (owner, when a lookahead is passed) published rule changes due within 30 days.

Nothing here raises into the scheduler: a check that errors is a failed check, a send that errors is logged.
"""

from __future__ import annotations

import asyncio
import shutil
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, time
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any

import httpx

from gammad.alerts import router as router_mod
from gammad.alerts import templates as tpl
from gammad.alerts.telegram import (
    API_BASE,
    TOPIC_MISSING_TEXT,
    TelegramError,
    forum_topic_warning,
    is_topic_error,
)
from gammad.config import IST
from gammad.core import session_clock
from gammad.log import get_logger

log = get_logger(__name__)

# T-181 F9: the line the 09:00 card (and the all-clear) carries while alerts are paused
PAUSED_LINE = (
    "\N{DOUBLE VERTICAL BAR} <b>ALERTS PAUSED</b>: entry cards do not reach the group until you resume them "
    "(the tray menu or the dashboard's Pause alerts button)."
)


def _topic_int(value: Any) -> int | None:
    """TELEGRAM_TOPIC_ID as the int Telegram takes; None when blank or not a number (the client then posts in General)."""
    try:
        return int(str(value).strip()) if value not in (None, "") and str(value).strip() else None
    except ValueError:
        return None

CHECK_NAMES = ("token", "env", "telegram", "broker", "disk", "clock", "data_dir")
MIN_FREE_BYTES = 500 * 1024 * 1024  # 500 MB
MAX_SKEW_S = 60


def token_must_outlive(d: date) -> str:
    return session_clock.fo_close_hm(d)


NOT_CHECKED = "not checked"
PROBE_NAME = ".readiness-probe"
DETAIL_MAX = 200
CALENDAR_CHECK = "calendar"
LOOKAHEAD_SHOWN = 3
NO_RULE_CHANGE = "no rule change in the next 30 days"


@dataclass(frozen=True)
class Check:
    name: str
    ok: bool
    detail: str = ""


@dataclass(frozen=True)
class Readiness:
    ok: bool
    at_ist: str
    checks: tuple[Check, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "at_ist": self.at_ist,
            "checks": [{"name": c.name, "ok": c.ok, "detail": c.detail} for c in self.checks],
        }

    def first_problem(self) -> Check | None:
        return next((c for c in self.checks if not c.ok), None)


def _short(exc: BaseException) -> str:
    return f"{type(exc).__name__}: {exc}"[:DETAIL_MAX]


def _ist(dt: datetime) -> datetime:
    return dt.replace(tzinfo=IST) if dt.tzinfo is None else dt.astimezone(IST)


async def telegram_server_date(
    client: httpx.AsyncClient | None = None, *, timeout: float = 5.0
) -> datetime | None:
    """The Date header of the Telegram Bot API host (a HEAD to API_BASE: no token, no body). None when the host
    answers without a Date header. Raises on a transport error (the caller turns it into "not checked").
    Why not getMe's own header: telegram.TelegramClient.get_me returns only the `result` object (the response
    headers never reach a caller) and telegram.py is not this module's to change; the host and its clock are the same."""
    own = client is None
    c = client or httpx.AsyncClient(timeout=timeout)
    try:
        resp = await c.head(API_BASE)
    finally:
        if own:
            await c.aclose()
    raw = resp.headers.get("Date")
    return parsedate_to_datetime(raw) if raw else None


def _disk_free(path: Path) -> int:
    p = path
    while (
        not p.exists() and p.parent != p
    ):  # a data folder not created yet: measure the drive it will live on
        p = p.parent
    return shutil.disk_usage(p).free


class ReadinessJob:
    def __init__(
        self,
        *,
        token_status: Callable[[], Mapping[str, Any]],
        reload_env: Callable[[], Any],
        telegram: Any,
        group_id: Any,
        topic_id: Any,
        adapter_source: Callable[[], Any],
        data_dir: Path | str,
        router: Any,
        clock: Callable[[], datetime],
        server_date: Callable[[], Awaitable[datetime | None]] | None = None,
        disk_free: Callable[[Path], int] | None = None,
        lookahead: Callable[[date], Sequence[Any]] | None = None,
    ) -> None:
        """token_status   Runtime.token_status (the /api/status token block: {valid, validity_ist, hours_left, level}).
        reload_env        config.reload_settings (any callable that raises when .env does not load).
        telegram          the TelegramClient (get_me / get_chat) or None when TELEGRAM_BOT_TOKEN is blank; OR a
                          zero-argument callable returning the current one (a new token rebuilds the client).
        group_id, topic_id the TELEGRAM_GROUP_ID / TELEGRAM_TOPIC_ID values, or zero-argument callables returning them.
        adapter_source    Runtime.adapter (returns the broker adapter or None).
        data_dir          the data folder (disk space + the write probe).
        router            the AlertRouter (send_ops).
        clock             the current time (aware; compared with the Telegram Date header).
        server_date       optional: the async Date-header source (default telegram_server_date; tests pass a fake).
        disk_free         optional: bytes free for a path (default shutil.disk_usage; tests pass a fake).
        lookahead         the rule-change look-ahead (gammad.core.lookahead.from_files) or None = no calendar check"""
        self.lookahead = lookahead
        self._token_status = token_status
        self._reload_env = reload_env
        self._telegram = telegram
        self._group_id = group_id
        self._topic_id = topic_id
        self._adapter_source = adapter_source
        self.data_dir = Path(data_dir)
        self.router = router
        self._clock = clock
        self._server_date = server_date or telegram_server_date
        self._disk_free = disk_free or _disk_free
        self._last: Readiness | None = None
        self._last_day: date | None = None
        self._lock = asyncio.Lock()

    # -- the public contract

    @property
    def last(self) -> Readiness | None:
        return self._last

    def status_block(self) -> dict[str, Any] | None:
        """/api/status.readiness: the last result's to_dict(), None before the first check."""
        return self._last.to_dict() if self._last is not None else None

    async def run(self, now: datetime) -> Readiness:
        """The 09:00 check: ONE card, READY (silent) or NOT READY (sound)."""
        async with self._lock:
            r = await self.check(now)
            self._remember(r, now)
            await self._send(self._with_pause(tpl.readiness_card(r)), silent=r.ok)
            return r

    async def recheck(self, now: datetime) -> Readiness | None:
        """The 09:10 re-check: None (nothing run) unless today's last result is NOT READY. Sends the all-clear
        (silent) only when the result is now READY; a second NOT READY sends nothing."""
        async with self._lock:
            last = self._last
            if last is None or last.ok or self._last_day != _ist(now).date():
                return None
            r = await self.check(now)
            self._remember(r, now)
            if r.ok:
                await self._send(self._with_pause(tpl.readiness_all_clear(r)), silent=True)
            return r

    def stale(self, now: datetime) -> bool:
        """T-346 (D386 orchestrator fix): today's last result is NOT READY, so a re-run could clear it (the Runtime asks before it
        spawns a refresh; a READY day, or no check yet today, needs none)."""
        last = self._last
        return last is not None and not last.ok and self._last_day == _ist(now).date()

    async def refresh(self, now: datetime, why: str = "") -> Readiness | None:
        """T-346 (D386 orchestrator fix), both editions: re-run after an accepted login / token change or a successful list build.
        Exactly recheck(): None unless today's last result is NOT READY; the all-clear (silent) when it is READY now;
        a second NOT READY sends nothing (today's anti-spam: one NOT READY card a day, the 09:00 one)."""
        r = await self.recheck(now)
        if r is not None:
            log.info("readiness re-run after %s: %s", why or "a change", "READY" if r.ok else "still NOT READY")
        return r

    async def check(self, now: datetime) -> Readiness:
        """Run the seven checks (no card, nothing remembered)."""
        health, broker = await self._broker()
        tg_check, reachable = await self._telegram_check()
        by_name = {
            "token": self._token_check(now, health),
            "env": self._env_check(),
            "telegram": tg_check,
            "broker": broker,
            "disk": self._disk_check(),
            "clock": await self._clock_check(reachable),
            "data_dir": self._data_dir_check(),
        }
        checks = tuple(by_name[n] for n in CHECK_NAMES)
        if self.lookahead is not None:
            checks += (await self._calendar_check(now),)
        return Readiness(all(c.ok for c in checks), _ist(now).isoformat(timespec="seconds"), checks)

    async def _calendar_check(self, now: datetime) -> Check:
        try:
            rows = await asyncio.to_thread(self.lookahead, _ist(now).date())
            if not rows:
                return Check(CALENDAR_CHECK, True, NO_RULE_CHANGE)
            texts = []
            for row in rows[:LOOKAHEAD_SHOWN]:
                when = ("rule change today" if row.days == 0 else "rule change in 1 day" if row.days == 1
                        else f"rule change in {row.days} days")
                texts.append(f"{when}: {row.name[:80]} ({row.effective_from.isoformat()}"
                             + (f", {row.touches}" if row.touches else "")
                             + ("; no market_sessions era" if row.amber else "") + ")")
            detail = "; ".join(texts)
            if len(rows) > LOOKAHEAD_SHOWN:
                detail += f"; +{len(rows) - LOOKAHEAD_SHOWN} more"
            if any(row.amber for row in rows):
                detail = tpl.WARN_MARK + detail
            return Check(CALENDAR_CHECK, True, detail)
        except Exception as exc:  # noqa: BLE001 - calendar failures must never stop readiness
            log.warning("readiness: rule-change calendar not read: %s", _short(exc))
            return Check(CALENDAR_CHECK, True,
                         f"{tpl.WARN_MARK}rule-change calendar not read ({type(exc).__name__})")

    # -- internals

    def _remember(self, r: Readiness, now: datetime) -> None:
        self._last = r
        self._last_day = _ist(now).date()

    async def _send(self, text: str, *, silent: bool) -> None:
        try:
            await self.router.send_ops(router_mod.KIND_READINESS, text, dest=router_mod.OPS_PERSONAL_OR_GROUP,
                                       silent=silent)  # fmt: skip
        except Exception:
            log.exception("readiness card not sent")

    @staticmethod
    def _current(value: Any, *, probe: str | None = None) -> Any:
        """`value`, or value() when it is a zero-argument source (for the telegram client: a callable WITHOUT the
        client's `probe` method)."""
        if callable(value) and (probe is None or not hasattr(value, probe)):
            return value()
        return value

    def _token_check(self, now: datetime, health: Any) -> Check:
        try:
            st = dict(self._token_status() or {})
        except Exception as exc:  # noqa: BLE001 -- a broken token block is a failed check, not a crash
            log.warning("readiness: token status failed: %s", _short(exc))
            st = {}
        valid, validity = st.get("valid"), st.get("validity_ist")
        if (
            valid is None and health is not None
        ):  # the runtime's block does not know yet: the broker's own answer
            valid, validity = getattr(health, "valid", None), getattr(health, "validity_ist", None)
        if valid is False or st.get("level") == "expired":
            return Check("token", False, "expired or rejected: renew it on the Broker login page")
        if valid is None:
            return Check("token", False, "state unknown: open the Broker login page")
        if not validity:
            return Check("token", True, "valid (the broker reports no expiry)")
        try:
            expires = _ist(datetime.fromisoformat(str(validity)))
        except ValueError:
            return Check("token", True, f"valid (expiry {str(validity)[:40]!r} unreadable)")
        today = _ist(now).date()
        must = datetime.combine(today, time.fromisoformat(token_must_outlive(today)), IST)
        when = expires.strftime("%H:%M") if expires.date() == today else expires.strftime("%d %b %H:%M")
        if expires < must:
            return Check(
                "token", False, f"expires {when}, before {token_must_outlive(today)}: renew it on the Broker login page"
            )
        return Check("token", True, f"valid until {when}")

    def _env_check(self) -> Check:
        try:
            self._reload_env()
        except Exception as exc:  # noqa: BLE001
            return Check("env", False, f".env did not load ({type(exc).__name__}): fix it, then restart")
        return Check("env", True, "loaded")

    async def _telegram_check(self) -> tuple[Check, bool]:
        """(the check, whether the Telegram host answered)."""
        tg = self._current(self._telegram, probe="get_me")
        if tg is None:
            return Check("telegram", False, "no bot token: set TELEGRAM_BOT_TOKEN in .env"), False
        try:
            me = await tg.get_me()
        except Exception as exc:  # noqa: BLE001 -- unreachable / rejected token: NOT READY, never a crash
            return Check(
                "telegram", False, f"Telegram unreachable or the bot token was refused ({_short(exc)})"
            ), False
        group = str(self._current(self._group_id) or "").strip()
        if not group:
            return Check("telegram", False, "TELEGRAM_GROUP_ID is blank"), True
        try:
            chat = await tg.get_chat(group)
        except Exception as exc:  # noqa: BLE001
            return Check("telegram", False, f"the bot cannot see the group ({_short(exc)})"), True
        name = me.get("username") if isinstance(me, Mapping) else None
        detail = f"@{name} · group ok" if name else "bot ok · group ok"
        topic_raw = self._current(self._topic_id)
        topic_fail = await self._topic_check(tg, group, topic_raw)
        if topic_fail is not None:
            return topic_fail, True
        if _topic_int(topic_raw) is not None and callable(getattr(tg, "send_chat_action", None)):
            detail = f"{detail} · topic ok"
        warning = forum_topic_warning(chat, topic_raw)
        if warning:
            log.warning("readiness: %s", warning)
            detail = f"{detail}; {tpl.WARN_MARK}{warning}"
        return Check("telegram", True, detail), True

    @staticmethod
    async def _topic_check(tg: Any, group: str, topic_raw: Any) -> Check | None:
        """T-181 F3 (R4): with TELEGRAM_TOPIC_ID set, sendChatAction into that topic (nothing is posted). A missing /
        closed / deleted topic fails every group send, so it is a failed check in a plain sentence; another refusal
        (4xx) fails too; an unreachable host or a client without send_chat_action = not checked (None)."""
        topic = _topic_int(topic_raw)
        probe = getattr(tg, "send_chat_action", None)
        if topic is None or not callable(probe):
            return None
        try:
            await probe(group, "typing", message_thread_id=topic)
        except TelegramError as exc:
            if is_topic_error(exc):
                log.warning("readiness: %s", TOPIC_MISSING_TEXT)
                return Check("telegram", False, TOPIC_MISSING_TEXT)
            if exc.status is not None and exc.status < 500 and exc.status != 429:
                return Check("telegram", False, f"the bot cannot post in the group's topic ({_short(exc)})")
            log.info("readiness: topic not checked (%s)", _short(exc))
        except Exception as exc:  # noqa: BLE001 -- the topic probe never crashes the check
            log.info("readiness: topic not checked (%s)", _short(exc))
        return None

    def _with_pause(self, text: str) -> str:
        """T-181 F9: the card says ALERTS PAUSED while the router's alerts are paused (the tray / dashboard switch;
        D141(4) persists it): READY or not, no entry card reaches the group until they are resumed."""
        if not getattr(self.router, "paused", False):
            return text
        return f"{text}\n\n{PAUSED_LINE}"

    async def _broker(self) -> tuple[Any, Check]:
        """(the adapter's TokenHealth or None, the broker check)."""
        try:
            adapter = self._adapter_source()
        except Exception as exc:  # noqa: BLE001
            return None, Check("broker", False, f"broker adapter unavailable ({_short(exc)})")
        if adapter is None:
            return None, Check("broker", False, "broker adapter unavailable: check the broker keys in .env")
        try:
            health = await asyncio.to_thread(adapter.token_health)
        except Exception as exc:  # noqa: BLE001
            return None, Check("broker", False, f"broker did not answer ({_short(exc)})")
        if getattr(health, "valid", False):
            return health, Check("broker", True, "reachable, token accepted")
        err = str(getattr(health, "error", None) or "token not accepted")[:DETAIL_MAX]
        return health, Check("broker", False, f"broker refused: {err}")

    def _disk_check(self) -> Check:
        try:
            free = int(self._disk_free(self.data_dir))
        except Exception as exc:  # noqa: BLE001
            return Check("disk", False, f"free space unreadable ({_short(exc)})")
        mb = free // (1024 * 1024)
        need = MIN_FREE_BYTES // (1024 * 1024)
        if free < MIN_FREE_BYTES:
            return Check("disk", False, f"{mb} MB free, needs {need} MB: free some space")
        return Check("disk", True, f"{free / 1024**3:.1f} GB free")

    async def _clock_check(self, reachable: bool) -> Check:
        if not reachable:
            return Check("clock", True, NOT_CHECKED)
        try:
            server = await self._server_date()
        except Exception as exc:  # noqa: BLE001 -- unreachable host: the clock is not checked, not failed
            log.info("readiness: clock not checked (%s)", _short(exc))
            return Check("clock", True, NOT_CHECKED)
        if server is None:
            return Check("clock", True, NOT_CHECKED)
        skew = (_ist(server) - _ist(self._clock())).total_seconds()
        if abs(skew) > MAX_SKEW_S:
            way = "behind" if skew > 0 else "ahead"
            return Check("clock", False, f"the PC clock is {abs(skew):.0f} s {way}: sync the Windows time")
        return Check("clock", True, f"within {abs(skew):.0f} s")

    def _data_dir_check(self) -> Check:
        probe = self.data_dir / PROBE_NAME
        try:
            self.data_dir.mkdir(parents=True, exist_ok=True)
            probe.write_text("ok", encoding="utf-8")
            probe.unlink()
        except OSError as exc:
            return Check("data_dir", False, f"the data folder is not writable ({type(exc).__name__})")
        return Check("data_dir", True, "writable")
