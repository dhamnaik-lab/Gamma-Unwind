"""Dhan access-token watch (spec "Token expiry"; docs/BROKERS.md token facts).

* `poll()` probes GET /v2/profile through `adapter.token_health()`; `tokenValidity` is
  "DD/MM/YYYY HH:MM" IST (the Dhan adapter already hands it over as ISO-8601; both are accepted).
* Levels: ok (> 24 h left), warn24 (<= 24 h), warn2 (<= 2 h), expired (<= 0 h, or the broker
  rejected the token: REST DH-901 / feed 807-809), unknown (no token, or the probe failed for a
  non-auth reason). The 24 / 2 thresholds come from `alerts.token_warn_hours`.
* Every level CHANGE writes a `token_events` row (kind = level). Only entering warn24 / warn2 /
  expired, and recovering to ok after expired, sends an alert, and only as a TOKEN message
  through `AlertRouter.send_system` (dashboard + PERSONAL chat). At most one expired alert per hour.
  T-181 F7 (owner ruling 2026-09-29): an EXPIRED or warn2 alert goes to the GROUP instead when no personal chat is
  set (send_system group_fallback=True), in both editions; never to both.
* T-346 (D386 (2), owner ruling 9 Oct 2026): the CLIENT edition has its own ladder: ok (> 1 h left), warn1 (<= 1 h,
  CLIENT_WARN_HOURS), expired. So a client gets ONE calm card an hour before the login ends, one when it ends (or is
  refused), and "login OK again" after a fix; never the 24 h card (it fired on every fresh Dhan login: a new token has
  < 24 h) nor the 2 h one. All three client cards fall back to the group without a personal chat (T-181 F7). The owner
  edition keeps the 24 h / 2 h ladder and its wording exactly. `client=None` reads gammad.edition at construction.
* T-181 F1: the first VALID poll (from none / unknown / expired) calls `on_token_valid` (attach() takes the
  runtime's): a service started after 09:20 with a valid token in .env catches the 09:20 build up.
* `update_token()` probes the NEW token first; only an accepted token is written to .env, set on
  the live adapter and handed to the feed (`on_credentials_changed`). A rejected one raises
  `TokenRejected` and nothing is written to .env or left on the adapter.
* The .env watcher routes an on-disk DHAN_ACCESS_TOKEN change through the same flow (source
  "env", no write-back). Our own write echoes back through the watcher and is ignored.
Token values are never logged; `mask()` is used wherever one is recorded.

Broker-aware mode (BROKER-ADAPTER-SPEC §5; W12): pass `entry=<BrokerEntry>` (+ `creds`, the entry's
{env key: value}; default = the .env file, OS env as the D54 fallback). Then the key written is
`entry.token_key` (not ENV_KEY), `_probe` hands the adapter a creds MAPPING (`set_credentials(creds)`),
`on_credentials_changed(creds_dict)` gets ONE argument, and `login_with_fields()` runs the entry's
`client_login` (D92 / D95(5): a pasted redirect URL / auth code, or a typed TOTP code + PIN) and then the
same D32 order. Without `entry` everything is exactly the legacy Dhan path (2-arg set_credentials /
on_credentials_changed(client_id, token)). Typed / pasted login values are used once: never logged,
recorded, echoed or written.

Owner automated renewal (T-339, D364, D86): after each poll, eligible owner brokers renew once per
trading morning at/after 08:45 IST unless the token has at least eight hours left. The daily claim
survives restarts in token_events. Registry owner gates protect automated login; accepted credentials
follow D32 through the probe, .env, adapter, settings and running feed. Failure sends one personal
SYSTEM alert without group fallback. Client login behaviour is unchanged.
"""

from __future__ import annotations

import asyncio
import hashlib
import os
import threading
import time
from collections.abc import Callable, Mapping
from datetime import date, datetime, timedelta
from datetime import time as dtime
from pathlib import Path
from typing import Any

from gammad import edition as _edition
from gammad.broker import registry
from gammad.broker.base import TokenHealth
from gammad.broker.dhan import parse_token_validity
from gammad.config import IST, EnvWatcher, Settings, mask, now_ist, reload_settings, write_env_value
from gammad.core.calendar import is_trading_day
from gammad.log import get_logger

log = get_logger(__name__)

ENV_KEY = "DHAN_ACCESS_TOKEN"  # the legacy (no-entry) token key
LEGACY_CLIENT_ID_KEY = "DHAN_CLIENT_ID"

LEVEL_OK = "ok"
LEVEL_WARN24 = "warn24"
LEVEL_WARN2 = "warn2"
LEVEL_WARN1 = "warn1"  # T-346 (D386 (2)): the CLIENT edition's only heads-up (CLIENT_WARN_HOURS before)
LEVEL_EXPIRED = "expired"
LEVEL_UNKNOWN = "unknown"
VALID_LEVELS = frozenset({LEVEL_OK, LEVEL_WARN24, LEVEL_WARN2, LEVEL_WARN1})
WARN_LEVELS = frozenset({LEVEL_WARN24, LEVEL_WARN2, LEVEL_WARN1})
# T-181 F7 (owner ruling 2026-09-29): the levels whose alert falls back to the GROUP without a personal chat
URGENT_LEVELS = frozenset({LEVEL_EXPIRED, LEVEL_WARN2, LEVEL_WARN1})
# T-346 (D386 (2)): "1 h before"; not alerts.token_warn_hours (that ladder is the owner's)
CLIENT_WARN_HOURS = 1.0
AUTO_RENEW_AT = dtime(8, 45)
AUTO_RENEW_MIN_HOURS = 8.0
SOURCE_AUTO = "auto"
SOURCE_RENEW_NOW = "renew_now"
RENEWED_HOW = {"auto": "automatically", "renew_now": "on request"}
RENEW_SCAN_DAYS = 14

EXPIRED_ALERT_EVERY_S = 3600.0
# Feed disconnect codes that mean the credentials are bad (gammad/watch/feed.py DISCONNECT_REASONS).
# 806 ("Data API subscription required") is an entitlement problem, not a token one.
FEED_TOKEN_CODES = frozenset({807, 808, 809})
# DhanAdapter._request raises DhanAuthError("<endpoint>: auth rejected (<code>): ...") and
# token_health() returns str(exc) as the error: that literal is how an auth failure is told apart
# from a transport / server failure (which is "unknown", not "expired").
AUTH_MARKER = "auth rejected"


class TokenRejected(Exception):
    """The broker did not accept a proposed token; nothing was written."""


def get_auto_renew_enabled(conn: Any) -> bool:
    """Per-install morning-attempt preference; absent tables / rows preserve the existing ON default."""
    import sqlite3

    try:
        row = conn.execute("SELECT value FROM app_settings WHERE key = ?", ("token_auto_renew",)).fetchone()
    except sqlite3.OperationalError as exc:
        if "no such table" not in str(exc):
            raise
        return True
    return row is None or str(row[0]) != "0"


def set_auto_renew_enabled(conn: Any, enabled: bool, *, source: str) -> bool:
    """Persist only the preference, never broker credentials. The next watcher tick reads it."""
    if type(enabled) is not bool:
        raise TypeError("enabled must be bool")
    conn.execute(
        "INSERT INTO app_settings (key, value, updated_ms, source) VALUES (?, ?, ?, ?) ON CONFLICT (key) DO UPDATE SET"
        " value = excluded.value, updated_ms = excluded.updated_ms, source = excluded.source",
        ("token_auto_renew", "1" if enabled else "0", int(time.time() * 1000), str(source)),
    )
    conn.commit()
    log.info("Token auto renewal switch set %s (source %s)", "ON" if enabled else "OFF", source)
    return enabled


class RenewBusy(RuntimeError):
    """A renewal is already running."""


def parse_validity(value: str | None) -> datetime | None:
    """ISO-8601 (adapter output) or Dhan's raw 'DD/MM/YYYY HH:MM' (IST) -> aware datetime."""
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value)
    except ValueError:
        iso = parse_token_validity(value)
        if iso is None:
            return None
        dt = datetime.fromisoformat(iso)
    return dt if dt.tzinfo is not None else dt.replace(tzinfo=IST)


def classify(hours_left: float, warn_hi: float = 24.0, warn_lo: float = 2.0) -> str:
    if hours_left <= 0:
        return LEVEL_EXPIRED
    if hours_left <= warn_lo:
        return LEVEL_WARN2
    if hours_left <= warn_hi:
        return LEVEL_WARN24
    return LEVEL_OK


def classify_client(hours_left: float, warn: float = CLIENT_WARN_HOURS) -> str:
    """T-346 (D386 (2)): the CLIENT edition's ladder: expired (<= 0 h), warn1 (<= `warn` h), else ok. No 24 h / 2 h
    level, so a fresh login (a new Dhan token has < 24 h) is plain ok and sends nothing."""
    if hours_left <= 0:
        return LEVEL_EXPIRED
    if hours_left <= warn:
        return LEVEL_WARN1
    return LEVEL_OK


def _hm(validity_iso: str | None) -> str | None:
    """'06:00' from an ISO validity (IST), or None."""
    dt = parse_validity(validity_iso)
    return None if dt is None else dt.astimezone(IST).strftime("%H:%M")


def is_auth_failure(health: TokenHealth) -> bool:
    """W14 H4: by TYPE first (`auth_failed`, set by every adapter that caught its BrokerAuthError); the legacy
    "auth rejected" text still counts (a TokenHealth built without the flag, e.g. an old fake)."""
    if getattr(health, "auth_failed", False):
        return True
    return bool(health.error) and AUTH_MARKER in str(health.error)


def token_key_hash(token: str) -> str:
    """A 12-hex sha256 prefix of a token: the persisted once-per-token warn key (W14 H7). Never the token."""
    return hashlib.sha256(str(token or "").encode("utf-8")).hexdigest()[:12]


def scrub(text: str, secrets: Any) -> str:
    """`text` with every non-empty value of `secrets` replaced by <redacted> (longest first)."""
    out = str(text)
    for s in sorted({str(v) for v in secrets if v}, key=len, reverse=True):
        out = out.replace(s, "<redacted>")
    return out


def secret_parts(value: str) -> set[str]:
    """Everything of a typed/pasted value that must never be echoed: the value, its URL-decoded form and
    every query/fragment parameter value inside it (the broker extracts the one-time request/auth code from a
    pasted redirect URL and may echo THAT, not the whole URL; F1, D97). Parts shorter than 4 chars are skipped
    (they would redact ordinary words such as 'login' flags or '1')."""
    from urllib.parse import parse_qsl, unquote, urlsplit

    raw = str(value or "").strip()
    out = {raw, unquote(raw)} - {""}
    for text in (raw, unquote(raw)):
        parts = urlsplit(text) if "://" in text else None
        blobs = [parts.query, parts.fragment] if parts else [text.split("?", 1)[1]] if "?" in text else []
        for blob in blobs:
            for _k, v in parse_qsl(blob, keep_blank_values=False):
                if len(v.strip()) >= 4:
                    out.add(v.strip())
    return out


def check_login_fields(expected: tuple[str, ...], fields: Any) -> dict[str, str]:
    """The typed/pasted fields, stripped, exactly `expected` and all non-empty. Raises ValueError
    naming EXPECTED keys only (a value must never reach an error message); TypeError when not a mapping.
    T-283: an unexpected key is COUNTED, never named -- a token pasted as a field name must not reach the log."""
    if not isinstance(fields, Mapping):
        raise TypeError("fields must be an object")
    got = set(fields)
    if got != set(expected):
        missing = sorted(set(expected) - got)
        unexpected = len(got - set(expected))
        raise ValueError(
            f"fields must be exactly {sorted(expected)}: {unexpected} unexpected key(s), missing {missing}"
        )
    out: dict[str, str] = {}
    for k in expected:
        v = fields[k]
        if not isinstance(v, str) or not v.strip():
            raise ValueError(f"field {k!r} must be a non-empty string")
        out[k] = v.strip()
    return out


def _env_creds(entry: Any, env_path: Path) -> dict[str, str]:
    """The entry's keys from the .env file; OS env fills a key the file leaves empty (D54)."""
    from dotenv import dotenv_values

    try:
        file_vals = dotenv_values(env_path) if env_path.is_file() else {}
    except (OSError, UnicodeDecodeError):
        file_vals = {}
    return {
        k: ((file_vals.get(k) or "").strip() or (os.environ.get(k) or "").strip()) for k in entry.key_names
    }


class TokenWatcher:
    def __init__(
        self,
        adapter: Any,
        db: Any,
        router: Any,
        settings: Settings,
        on_credentials_changed: Callable[[str, str], None] | None = None,
        *,
        env_path: Path | None = None,
        clock: Callable[[], datetime] = now_ist,
        monotonic: Callable[[], float] = time.monotonic,
        env_debounce_s: float = 1.0,
        entry: Any = None,
        creds: Mapping[str, str] | None = None,
        adapter_source: Callable[[], Any] | None = None,
        probe_factory: Callable[[Mapping[str, str]], Any] | None = None,
        client: bool | None = None,
    ) -> None:
        # W14 H1: `adapter` may be None (day one: the broker entry exists, no token yet). Then the probe loop idles,
        # login / paste still work (they need only entry + creds), and a NEW token is probed on a throwaway adapter
        # built from the new creds (`probe_factory`, default entry.make_adapter). `adapter_source` (the runtime's
        # adapter()) is re-read after every applied login, so the watcher then probes the runtime's own adapter.
        self._adapter = adapter
        self._adapter_source = adapter_source
        self._probe_factory = probe_factory
        self.db = db
        self.router = router
        self.settings = settings
        self.on_credentials_changed: Callable[..., None] | None = on_credentials_changed
        self.on_token_valid: Callable[[], Any] | None = None  # T-181 F1: set by attach() (Runtime.on_token_valid)
        self.on_prices_ok: Callable[[], Any] | None = None  # 0.8.3 L1: set by attach() (Runtime.on_prices_ok)
        self.env_path = env_path or (settings.paths.root / ".env")
        self._clock = clock
        self._mono = monotonic
        self._env_debounce_s = env_debounce_s
        warn = sorted((float(h) for h in settings.alerts.token_warn_hours), reverse=True) or [24.0, 2.0]
        self.warn_hi = warn[0]
        self.warn_lo = warn[-1]
        # T-346 (D386 (2)): the client ladder (1 h / expired) and calm card words; None = this install's edition
        self.client: bool = _edition.is_client() if client is None else bool(client)
        self.entry = entry  # a gammad.broker.capabilities.BrokerEntry, or None = the legacy Dhan path
        self.token_key: str = entry.token_key if entry is not None else ENV_KEY
        self.label: str = entry.label if entry is not None else "Dhan"
        if entry is not None:
            src = creds if creds is not None else _env_creds(entry, self.env_path)
            self._creds: dict[str, str] = {k: (src.get(k) or "").strip() for k in entry.key_names}
            self._client_id = ""
            self._token = self._creds.get(self.token_key, "")
        else:
            self._client_id = settings.env.dhan_client_id or str(getattr(adapter, "client_id", "") or "")
            self._token = settings.env.dhan_access_token
            self._creds = {LEGACY_CLIENT_ID_KEY: self._client_id, ENV_KEY: self._token or ""}

        self.level: str | None = None
        self.validity_ist: str | None = None
        self.hours_left: float | None = None
        self.last_checked_ist: str | None = None
        self.problem: str | None = None  # 0.8.3 T-229 (3): the last probe's TokenHealth.problem (a client hint)
        self._was_expired = False
        self._last_expired_alert: float | None = None
        self._pending_warn: tuple[int, str] | None = None  # (token_events id, warn key) until the send succeeds

        self._state_lock = threading.Lock()
        self._update_lock: asyncio.Lock | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._tasks: set[asyncio.Task[bool]] = set()
        self._env_watcher: EnvWatcher | None = None
        self._auto_renew_day: date | None = None
        self._held: tuple[str, bool, tuple[int, str] | None] | None = None

    # ------------------------------------------------------------------ adapter (W14 H1)

    @property
    def adapter(self) -> Any:
        """The live adapter: the one given, else the runtime's (adapter_source) once it can build one."""
        if self._adapter is None and self._adapter_source is not None:
            try:
                self._adapter = self._adapter_source()
            except Exception as exc:  # noqa: BLE001 -- no adapter yet is a state, not a crash
                log.warning("%s adapter unavailable: %s", self.label, type(exc).__name__)
                self._adapter = None
        return self._adapter

    @adapter.setter
    def adapter(self, value: Any) -> None:
        self._adapter = value

    def _make_probe_adapter(self, creds: Mapping[str, str]) -> Any:
        """A throwaway adapter over `creds` (no live adapter yet). Raises what make_adapter raises."""
        if self._probe_factory is not None:
            return self._probe_factory(dict(creds))
        if self.entry is None:
            raise TokenRejected(f"{self.label}: no adapter to probe the token with")
        return self.entry.make_adapter(dict(creds), self.settings.paths.data_dir)

    # ------------------------------------------------------------------ wiring

    def attach(self, runtime: Any) -> None:
        """Duck-typed registration: only hooks the runtime actually exposes are set. A runtime that
        takes `token_updater` (the dashboard API seam) also gets `token_login` (POST /api/token/login)
        and `token_view` (the dynamic half of GET /api/token)."""
        if hasattr(runtime, "token_updater"):
            runtime.token_updater = self.update_token
            runtime.token_login = self.login_with_fields
            runtime.token_view = self.token_view
            runtime.token_renew = self.renew_now
            runtime.token_auto_renew_on = self.auto_renew_on
            runtime.token_auto_renew_enabled = lambda: get_auto_renew_enabled(self.db)
        on_valid = getattr(runtime, "on_token_valid", None)  # T-181 F1: the first valid poll catches 09:20 up
        if callable(on_valid):
            self.on_token_valid = on_valid
        on_prices = getattr(runtime, "on_prices_ok", None)  # 0.8.3 L1: a good price check clears a job's refusal
        if callable(on_prices):
            self.on_prices_ok = on_prices
        register = getattr(runtime, "register_token_watcher", None)
        if callable(register):
            register(self)

    def start_env_watch(self) -> None:
        """Start the .env watcher. Call from the event loop (the loop is captured for the handoff)."""
        self._loop = asyncio.get_running_loop()
        if self._env_watcher is None:
            self._env_watcher = EnvWatcher(self.env_path, self._on_env_change, self._env_debounce_s)
            self._env_watcher.start()

    def stop(self) -> None:
        if self._env_watcher is not None:
            self._env_watcher.stop()
            self._env_watcher = None

    async def run(self, interval_s: float | None = None, *, watch_env: bool = True) -> None:
        """Poll now and then every `schedule.token_poll_minutes`; optionally watch .env meanwhile."""
        interval = interval_s or self.settings.schedule.token_poll_minutes * 60.0
        if watch_env:
            self.start_env_watch()
        try:
            while True:
                try:
                    await self.poll()
                    await self.maybe_auto_renew()
                except Exception:
                    log.exception("token poll failed")
                await asyncio.sleep(interval)
        finally:
            self.stop()

    # ------------------------------------------------------------------ state

    def snapshot(self) -> dict[str, Any]:
        with self._state_lock:
            return {
                "valid": self.level in VALID_LEVELS,
                "validity_ist": self.validity_ist,
                "hours_left": None if self.hours_left is None else round(self.hours_left, 2),
                "level": self.level or LEVEL_UNKNOWN,
                "last_checked_ist": self.last_checked_ist,
                "problem": self.problem,
            }

    def evaluate(self, health: TokenHealth | None) -> tuple[str, str | None, float | None, str]:
        """(level, validity_iso, hours_left, reason) for one probe result. None = no token."""
        if health is None:
            return LEVEL_UNKNOWN, None, None, f"no {self.token_key} configured"
        validity = parse_validity(health.validity_ist)
        if validity is not None:
            hours = (validity - self._clock()).total_seconds() / 3600.0
            level = classify_client(hours) if self.client else classify(hours, self.warn_hi, self.warn_lo)
            if is_auth_failure(health):
                level = LEVEL_EXPIRED
            return level, validity.isoformat(), hours, health.error or ""
        if is_auth_failure(health):
            return LEVEL_EXPIRED, None, None, str(health.error or "auth rejected")
        if health.valid and not health.error:  # W14 H7: accepted, the broker states no expiry (Kotak/Nuvama/Groww)
            return LEVEL_OK, None, None, ""
        return LEVEL_UNKNOWN, None, None, health.error or "no tokenValidity"

    def _already_warned(self, level: str, warn_key: str) -> bool:
        """A `token_events` row of this level already carries warned=<warn_key> (any watcher, any run)."""
        try:
            row = self.db.execute(
                "SELECT 1 FROM token_events WHERE kind = ? AND detail LIKE ? LIMIT 1", (level, f"% warned={warn_key}%")
            ).fetchone()
        except Exception:  # an unreadable history must not swallow the alert
            log.exception("token_events read failed; warn alert sent")
            return False
        return row is not None

    def _event(self, kind: str, detail: str) -> int | None:
        cur = self.db.execute(
            "INSERT INTO token_events (ts_ist, kind, detail) VALUES (?, ?, ?)",
            (self._clock().isoformat(timespec="seconds"), kind, detail),
        )
        self.db.commit()
        return getattr(cur, "lastrowid", None)

    def _transition(self, level: str, validity: str | None, hours: float | None, reason: str) -> str | None:
        """Apply a probe result. Writes a token_events row on a level change; returns the alert text
        to send, or None."""
        with self._state_lock:
            prev = self.level
            self.level = level
            self.validity_ist = validity
            self.hours_left = hours
            self.last_checked_ist = self._clock().isoformat(timespec="seconds")
            if level == prev:
                return None
            hrs = "-" if hours is None else f"{hours:.2f}"
            detail = f"validity={validity or '-'} hours_left={hrs}"
            if reason:
                detail += f" reason={reason[:200]}"
            text: str | None = None
            pending_warn: str | None = None
            if level in WARN_LEVELS:
                # W14 H7: once per TOKEN per warn level, persisted (a restart or a re-login with the same token
                # must not alert again): the level row carries warned=<sha256 prefix of the token>
                warn_key = f"{level}:{token_key_hash(self._token or '')}"
                if self._already_warned(level, warn_key):
                    log.info("token %s already alerted for this token; not re-sent", level)
                elif level == LEVEL_WARN1:  # T-346 (D386 (2)): the client's one calm heads-up, plain words
                    text = self._client_text(LEVEL_WARN1, validity, hours)
                    pending_warn = warn_key
                else:
                    limit = self.warn_hi if level == LEVEL_WARN24 else self.warn_lo
                    text = (
                        f"{self.label} access token expires in {hrs} h (at {validity}), within {limit:g} h. "
                        "Update it on the dashboard Broker login page or in .env."
                    )
                    pending_warn = warn_key  # persisted by _alert only once the send succeeded
            elif level == LEVEL_EXPIRED:
                now = self._mono()
                if (
                    self._last_expired_alert is None
                    or now - self._last_expired_alert >= EXPIRED_ALERT_EVERY_S
                ):
                    self._last_expired_alert = now
                    text = (
                        self._client_text(LEVEL_EXPIRED, validity, hours) if self.client else
                        f"{self.label} access token EXPIRED or rejected ({reason or 'no detail'}). "
                        "Feed and REST calls fail until a new token is set (dashboard Broker login page or .env)."
                    )  # fmt: skip
                else:
                    log.info("token expired again within the hour; alert suppressed (dedupe)")
            elif level == LEVEL_OK and self._was_expired:
                when = f"expires {validity}, {hrs} h left" if validity else "the broker states no expiry"
                text = (
                    self._client_text(LEVEL_OK, validity, hours) if self.client
                    else f"{self.label} access token is valid again ({when})."
                )  # fmt: skip
            for source, how in RENEWED_HOW.items():
                if text and reason == f"updated via {source}":
                    text = (
                        f"{self.label} access token renewed {how}: "
                        f"valid until {validity or '-'}, {hrs} h left."
                    )
            if level == LEVEL_EXPIRED:
                self._was_expired = True
            elif level in VALID_LEVELS:
                self._was_expired = False
        row_id = self._event(level, detail)
        self._pending_warn = (row_id, pending_warn) if pending_warn is not None and row_id is not None else None
        log.info("token level %s -> %s (%s)", prev, level, detail)
        return text

    def _client_text(self, level: str, validity: str | None, hours: float | None) -> str:
        """T-346 (D386 (2)): the CLIENT edition's three calm card sentences, plain words with the fix. They carry no word
        templates.system_marker reads as a failure (expired / rejected / refused / failed / stopped), so the card
        shows no red marker; no token, no technical reason."""
        if level == LEVEL_WARN1:
            at = _hm(validity)
            mins = max(1, round((hours or 0.0) * 60))
            when = f"at {at}, in about {mins} min" if at else f"in about {mins} min"
            return (
                f"Your {self.label} login ends {when}. Log in again on the dashboard Broker login page before then "
                "so signals keep coming."
            )
        if level == LEVEL_EXPIRED:
            return (
                f"Your {self.label} login has ended. Log in again on the dashboard Broker login page; "
                "signals pause until you do."
            )
        return f"Your {self.label} login OK again: signals carry on as normal."

    def _urgent(self, level: str) -> bool:
        """T-181 F7: EXPIRED / warn2 fall back to the GROUP without a personal chat. T-346 (D386 (2)): in the client
        edition all three login cards do (warn1, expired, and "login OK again" so the fix reaches the same chat)."""
        return level in URGENT_LEVELS or (self.client and level in VALID_LEVELS)

    # ------------------------------------------------------------------ alerts

    async def _send(self, text: str, *, urgent: bool = False, kind: str = "TOKEN") -> bool:
        """True when the router accepted the alert (it raised nothing). T-181 F7: `urgent` (EXPIRED / warn2) asks the
        router to fall back to the GROUP when no personal chat is set (owner ruling 2026-09-29, both editions)."""
        try:
            if urgent:
                await self.router.send_system(text, kind, group_fallback=True)
            else:
                await self.router.send_system(text, kind)
        except Exception:
            log.exception("token alert send failed")
            return False
        return True

    async def _alert(self, text: str, *, urgent: bool = False) -> None:
        """Send a _transition alert; a warn alert's once-per-token key (H7) is persisted only AFTER the send
        succeeded, so a failed send is retried by the next watcher / restart."""
        pending, self._pending_warn = self._pending_warn, None
        if await self._send(text, urgent=urgent) and pending is not None:
            row_id, warn_key = pending
            try:
                self.db.execute(
                    "UPDATE token_events SET detail = COALESCE(detail, '') || ? WHERE id = ?",
                    (f" warned={warn_key}", row_id),
                )
                self.db.commit()
            except Exception:
                log.exception("token_events warned key not persisted")

    def _dispatch(self, text: str, *, urgent: bool = False) -> asyncio.Task[bool] | None:
        """Send from sync code: on the running loop if there is one, else onto the captured loop."""
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None
        if loop is not None:
            task = loop.create_task(self._send(text, urgent=urgent))
            self._tasks.add(task)
            task.add_done_callback(self._tasks.discard)
            return task
        if self._loop is not None and self._loop.is_running():
            asyncio.run_coroutine_threadsafe(self._send(text, urgent=urgent), self._loop)
            return None
        log.warning("token alert not sent: no event loop")
        return None

    # ------------------------------------------------------------------ probes

    async def poll(self) -> dict[str, Any]:
        self._loop = asyncio.get_running_loop()
        adapter = self.adapter if self._token else None
        if self._token and adapter is None:  # W14 H1: a token but no adapter could be built -- unknown, say why
            health: TokenHealth | None = TokenHealth(False, None, f"{self.label} adapter unavailable")
        else:
            health = await asyncio.to_thread(adapter.token_health) if adapter is not None else None
        level, validity, hours, reason = self.evaluate(health)
        self._take_prices(health)
        was_valid = self.level in VALID_LEVELS
        prev_validity = self.validity_ist
        text = self._transition(level, validity, hours, reason)
        if text and not self._hold_card(text, level, hours, prev_validity):
            await self._alert(text, urgent=self._urgent(level))
        if level in VALID_LEVELS and not was_valid:
            self._token_became_valid()
        return self.snapshot()

    def _take_prices(self, health: TokenHealth | None) -> None:
        """0.8.3 T-229 (3), skeptic L1: the probe's three-way price check. Refused (a hint / prices False) -> the
        problem; prices True -> cleared here AND in the runtime (on_prices_ok: a job's refusal); None = not judged
        (no check, a timeout, a 429) -> the standing problem stays."""
        prices = getattr(health, "prices", None)
        problem = getattr(health, "problem", None)
        if problem or prices is False:
            self.problem = problem
            return
        if prices is not True:
            return
        self.problem = None
        hook = self.on_prices_ok
        if hook is not None:
            try:
                hook()
            except Exception:
                log.exception("on_prices_ok raised")

    def _token_became_valid(self) -> None:
        """T-181 F1 (R1): the first valid poll (at start, or after an expired / unknown one) hands over to the
        runtime's `on_token_valid` (Runtime.catch_up_0920 behind a validity check): a service started after 09:20
        with a token already in .env builds and posts the day's lists. Never raises into the poll loop."""
        hook = self.on_token_valid
        if hook is None:
            return
        try:
            hook()
        except Exception:
            log.exception("on_token_valid raised")

    def on_feed_auth_error(self, code: int, msg: str) -> asyncio.Task[bool] | None:
        """Feed `on_auth_error` hook (sync, called on the loop). 807/808/809 -> expired now."""
        if int(code) not in FEED_TOKEN_CODES:
            log.warning("feed blocked by %s (%s): not a token failure, token level unchanged", code, msg)
            return None
        prev_validity = self.validity_ist
        text = self._transition(LEVEL_EXPIRED, self.validity_ist, self.hours_left, f"feed {code}: {msg}")
        if text and self._hold_card(text, LEVEL_EXPIRED, self.hours_left, prev_validity):
            return None
        return self._dispatch(text, urgent=True) if text else None

    # ------------------------------------------------------------------ token update

    def _set_creds(self, creds: Mapping[str, str]) -> None:
        """Put `creds` on the live adapter: a mapping (broker-aware) or the legacy (client_id, token).
        No live adapter yet (W14 H1): nothing to set; the runtime builds one from the applied creds."""
        adapter = self.adapter
        if adapter is None:
            return
        if self.entry is not None:
            adapter.set_credentials(dict(creds))
        else:
            adapter.set_credentials(self._client_id, creds.get(ENV_KEY, ""))

    def _probe(self, token: str | Mapping[str, str]) -> TokenHealth:
        """Probe the broker with the NEW token (or {key: value} updates) on the live adapter, then put
        the current credentials back. No live adapter (W14 H1): build a throwaway adapter from the current creds
        + the updates, probe on it and close it. If it cannot be built, the answer is TokenHealth(valid=False,
        error="adapter: <type>: <message>", auth_failed=False) -- the caller then rejects the update (not valid)."""
        updates = {self.token_key: token} if isinstance(token, str) else dict(token)
        if self.adapter is None:
            try:
                probe = self._make_probe_adapter({**self._creds, **updates})
            except Exception as exc:  # noqa: BLE001 -- the reason is recorded (scrubbed) by the caller
                return TokenHealth(False, None, f"adapter: {type(exc).__name__}: {exc}"[:200])
            try:
                return probe.token_health()
            finally:  # W14 L1: the throwaway adapter's HTTP client is closed, best effort
                close = getattr(probe, "close", None)
                if callable(close):
                    try:
                        close()
                    except Exception:  # noqa: BLE001 -- a failed close must not turn a probe into an error
                        log.warning("%s probe adapter close failed", self.label)
        self._set_creds({**self._creds, **updates})
        try:
            return self.adapter.token_health()
        finally:
            self._set_creds(self._creds)

    def _lock(self) -> asyncio.Lock:
        if self._update_lock is None:
            self._update_lock = asyncio.Lock()
        return self._update_lock

    async def update_token(self, new_token: str, source: str = "dashboard") -> dict[str, Any]:
        """Validate, then persist and apply a new access token. Returns the new snapshot.

        Order: probe with the NEW token -> write .env (skipped for source "env") -> adapter
        set_credentials -> process env + settings cache -> on_credentials_changed (feed reload)
        -> token_events "updated". Raises TokenRejected (nothing written) if the probe fails.
        """
        new_token = (new_token or "").strip()
        async with self._lock():
            if source == "env" and new_token == self._token:
                return self.snapshot()  # our own .env write echoing back through the watcher
            if not new_token:
                raise TokenRejected("empty token")
            # W14 L2: a broker error echoing the pasted token must never reach token_events, the log or the error
            secrets = tuple(sorted(secret_parts(new_token)))
            text = await self._apply_locked({self.token_key: new_token}, source, secrets)
        if text:
            await self._alert(text, urgent=self.client)  # T-346: a client's "login OK again" reaches the same chat
        return self.snapshot()

    async def _apply_locked(
        self, updates: dict[str, str], source: str, secrets: tuple[str, ...] = ()
    ) -> str | None:
        """D32 with the update lock held: probe -> .env -> adapter -> settings -> feed reload -> event.
        `updates` = {env key: value}, the token key included. Returns the alert text (or None).
        `secrets` = typed/pasted login values, scrubbed out of anything recorded."""
        new_token = updates[self.token_key]
        health = await asyncio.to_thread(self._probe, updates)
        level, validity, hours, reason = self.evaluate(health)
        if not health.valid or level == LEVEL_EXPIRED:
            why = scrub(reason or level, secrets)[:200]
            self._event("rejected", f"token={mask(new_token)} source={source} reason={why}")
            log.warning("token update from %s rejected: %s", source, why)
            raise TokenRejected(why or f"token level {level}")
        if source != "env":
            # the token key LAST: when the watcher echoes our write back, every other key is in place
            for key in sorted(updates, key=lambda k: k == self.token_key):
                write_env_value(self.env_path, key, updates[key])
        self._creds.update(updates)
        self._token = new_token
        self._take_prices(health)  # 0.8.3 T-229 (3): before the runtime publishes its status
        self._set_creds(self._creds)
        self._publish_to_settings(updates)
        if self.on_credentials_changed is not None:
            try:
                if self.entry is not None:
                    self.on_credentials_changed(dict(self._creds))
                else:
                    self.on_credentials_changed(self._client_id, new_token)
            except Exception:
                log.exception("on_credentials_changed raised")
        if self._adapter is None and self.adapter is not None:  # W14 H1: the runtime built its adapter from these
            log.info("%s token watch now probes the runtime's adapter", self.label)
        self._event("updated", f"token={mask(new_token)} source={source}")
        self._held = None
        log.info("token updated from %s (%s)", source, mask(new_token))
        return self._transition(level, validity, hours, f"updated via {source}")

    def _publish_to_settings(self, updates: Mapping[str, str] | str) -> None:
        # load_dotenv(override=False) copied the old token into os.environ at start-up, so a plain
        # reload_settings() would read the OLD value back. The operator just supplied this one.
        values = {self.token_key: updates} if isinstance(updates, str) else updates
        for key, value in values.items():
            os.environ[key] = value
        reload_settings()

    # ------------------------------------------------------------------ owner automated login (T-339)

    def _auto_secrets(self) -> dict[str, str]:
        """Stored broker secrets, keyed so an unchanged value can retain its own key."""
        return {
            k: str(v).strip() for k, v in self._creds.items()
            if self.entry is not None and k in self.entry.key_names and self.entry.key(k).secret
            and len(str(v or "").strip()) >= 4
        }

    async def auto_renew(self, *, source: str = "auto", transport: Any = None) -> dict[str, Any]:
        """Owner-only automated login, followed by the existing D32 credential handoff."""
        if self.entry is None:
            raise LookupError("automated login needs a broker entry")
        async with self._lock():
            stored = self._auto_secrets()
            secrets = tuple(sorted(stored.values()))
            kwargs: dict[str, Any] = {} if transport is None else {"transport": transport}
            try:
                got = await asyncio.to_thread(registry.auto_login, self.entry, dict(self._creds), **kwargs)
            except _edition.OwnerOnlyError:
                raise
            except Exception as exc:  # noqa: BLE001 -- broker failures must not expose stored secrets
                why = f"{type(exc).__name__}: {scrub(str(exc), secrets)}"[:200]
                self._event("rejected", f"auto source={source} reason={why}")
                log.warning("%s auto login from %s rejected: %s", self.label, source, why)
                raise TokenRejected(why) from None
            updates: dict[str, str] = {}
            for key, value in dict(got or {}).items():
                v = str(value or "").strip()
                if key not in self.entry.key_names or not v:
                    continue
                if any(v == secret for other, secret in stored.items() if other != key):
                    continue
                updates[key] = v
            if self.token_key not in updates:
                self._event("rejected", f"auto source={source} reason=no {self.token_key} returned")
                raise TokenRejected(f"auto login returned no {self.token_key}")
            text = await self._apply_locked(updates, source, secrets)
        if text:
            await self._alert(text)
        return self.snapshot()

    def auto_renew_on(self) -> bool:
        try:
            return not self.client and self.entry is not None and registry.can_auto_login(self.entry, self._creds)
        except Exception as exc:  # noqa: BLE001 -- capability failures must fail closed without secrets
            log.warning("auto renewal availability failed (%s)", type(exc).__name__)
            return False

    def auto_attempt_on(self) -> bool:
        return self.auto_renew_on() and get_auto_renew_enabled(self.db)

    def _renewal_day(self, now: datetime) -> date:
        for offset in range(RENEW_SCAN_DAYS + 1):
            day = now.date() + timedelta(days=offset)
            try:
                trading = is_trading_day(day)
            except Exception:  # noqa: BLE001 -- same weekday fallback as the morning job
                trading = day.weekday() < 5
            if trading:
                return day
        raise LookupError("no trading day in renewal scan")

    def _claim_ts(self, day: date) -> str | None:
        row = self.db.execute(
            "SELECT ts_ist FROM token_events WHERE kind = 'auto_attempt' AND ts_ist LIKE ? ORDER BY id LIMIT 1",
            (f"{day.isoformat()}%",),
        ).fetchone()
        return str(row[0]) if row is not None else None

    def _fresh_claim(self, ts_ist: str) -> bool:
        """T-525: derive fresh coverage from the durable attempt's timestamp."""
        validity = parse_validity(self.validity_ist)
        claimed = datetime.fromisoformat(ts_ist)
        return validity is not None and validity >= claimed + timedelta(hours=AUTO_RENEW_MIN_HOURS)

    def _pending_day(self, now: datetime) -> date | None:
        """Renewal day whose automated attempt is still to come, or None.

        A fresh claim hands the hold to the next renewal day's attempt;
        a failed day still releases (CX-82 Q2).
        """
        try:
            if not self.auto_attempt_on():
                return None
            day = self._renewal_day(now)
            claim = self._claim_ts(day)
            if claim is not None and self._fresh_claim(claim):
                day = self._renewal_day(datetime.combine(
                    day + timedelta(days=1), dtime.min, tzinfo=now.tzinfo,
                ))
                claim = self._claim_ts(day)
            if self._auto_renew_day == day or claim is not None:
                return None
            return day
        except Exception:
            log.exception("renewal history read failed; card not held")
            return None

    def _renewal_pending(self, now: datetime) -> bool:
        return self._pending_day(now) is not None

    def _hold_card(self, text: str, level: str, hours: float | None, prev_validity: str | None) -> bool:
        now = self._clock()
        previous = parse_validity(prev_validity)
        due = (hours is not None and hours <= 0) or (previous is not None and previous <= now)
        day = self._pending_day(now)
        if day is not None and (
            level in {LEVEL_WARN24, LEVEL_WARN2} or (level == LEVEL_EXPIRED and due)
        ):
            self._held = (text, self._urgent(level), self._pending_warn)
            self._pending_warn = None
            log.info("token %s card held: the %s automated renewal is due", level, day)
            return True
        self._held = None
        return False

    async def _release_held(self) -> None:
        try:
            if self._held is not None and not self._renewal_pending(self._clock()):
                text, urgent, self._pending_warn = self._held
                await self._alert(text, urgent=urgent)
                self._held = None
        except Exception:
            log.exception("held token card release failed")

    async def renew_now(self) -> dict[str, Any]:
        if not self.auto_renew_on():
            raise LookupError(f"{self.label}: automated renewal is not set up")
        if self._lock().locked():
            raise RenewBusy("a renewal is already running")
        return await self.auto_renew(source=SOURCE_RENEW_NOW)

    async def maybe_auto_renew(self) -> str:
        outcome = await self._maybe_auto_renew_once()
        await self._release_held()
        return outcome

    async def _maybe_auto_renew_once(self) -> str:
        """Decide once per trading day, durably before any await; failures never retry that day."""
        now: datetime | None = None
        secrets: tuple[str, ...] = ()
        try:
            secrets = tuple(self._auto_secrets().values())
            now = self._clock()
            d = now.date()
            if self._auto_renew_day == d:
                return "done"
            if not self.auto_renew_on():
                return "off"
            if now.time() < AUTO_RENEW_AT:
                return "early"
            try:
                trading = is_trading_day(d)
            except Exception:  # noqa: BLE001 -- calendar failure falls back to weekdays per T-339
                trading = d.weekday() < 5
            if not trading:
                return "holiday"
            # CX-218 review F1 (fixed at merge): the stored-switch read runs on every tick while the switch is OFF, so a
            # transient store error here is no failed login: no SYSTEM alert, the day is not consumed, the next tick re-reads.
            try:
                attempt_on = self.auto_attempt_on()
            except Exception as exc:  # noqa: BLE001 -- type name only, never the message (it may carry a path)
                log.warning("%s auto-renew preference read failed (%s); treated as off this tick", self.label, type(exc).__name__)
                return "off"
            if not attempt_on:
                return "off"
            attempted = self.db.execute(
                "SELECT 1 FROM token_events WHERE kind = 'auto_attempt' AND ts_ist LIKE ? LIMIT 1",
                (f"{d.isoformat()}%",),
            ).fetchone()
            if attempted is not None:
                self._auto_renew_day = d
                return "done"
            self._auto_renew_day = d
            self._event("auto_attempt", f"source=auto day={d.isoformat()}")
            if self.hours_left is not None and self.hours_left >= AUTO_RENEW_MIN_HOURS:
                log.info("%s auto login skipped: token covers the trading day", self.label)
                return "fresh"
            await self.auto_renew(source="auto")
            return "renewed"
        except Exception as exc:  # noqa: BLE001 -- keep the watcher running, without a TOTP retry loop
            if now is not None:
                self._auto_renew_day = now.date()
            why = scrub(str(exc), secrets)[:200]
            when = f"{now:%H:%M}" if now is not None else "unknown time"
            await self._send(
                f"{self.label} automated login failed at {when} IST: {why}. Log in on the Broker login page.",
                kind="SYSTEM", urgent=False,
            )
            return "failed"

    # ------------------------------------------------------------------ client login (D92)

    def token_view(self) -> dict[str, Any]:
        """The dynamic half of GET /api/token: which key, the MASKED current token, and the broker
        login page the user opens by hand (client_login.login_url), or None."""
        url: str | None = None
        cl = getattr(self.entry, "client_login", None)
        if cl is not None and callable(cl.login_url):
            try:
                url = cl.login_url(dict(self._creds))
            except Exception as exc:  # noqa: BLE001 -- a broken URL builder must not break the page
                log.warning("%s login_url failed: %s", self.label, type(exc).__name__)
        return {"token_key": self.token_key, "token_masked": mask(self._token), "login_url": url}

    async def login_with_fields(
        self, fields: Mapping[str, Any], *, source: str = "login", transport: Any = None
    ) -> dict[str, Any]:
        """ONE-SHOT client login (D92, D95(5)): `entry.client_login.login(creds, fields)` -> new
        {env key: value} -> D32 (probe the NEW token first; nothing written when it is rejected).

        Raises LookupError (no client_login for this broker), ValueError (fields are not exactly
        client_login.fields, or empty) and TokenRejected (the broker refused the login or the new
        token). The typed / pasted values are never logged, recorded, written or put in an error.
        """
        cl = getattr(self.entry, "client_login", None)
        if cl is None:
            raise LookupError(f"{self.label} has no paste/typed login")
        clean = check_login_fields(tuple(cl.fields), fields)
        typed = {p for v in clean.values() for p in secret_parts(v)}
        # + every STORED secret of this broker (.env keys the registry marks secret): a broker may put one in the
        # request URL/body and echo it back in an error (SEAM-T: Nuvama's api key rides in the URL path)
        stored = {
            str(v).strip() for k, v in self._creds.items()
            if len(str(v or "").strip()) >= 4 and k in self.entry.key_names and self.entry.key(k).secret
        }
        secrets = tuple(sorted(typed | stored))
        async with self._lock():
            kwargs: dict[str, Any] = {} if transport is None else {"transport": transport}
            try:
                got = await asyncio.to_thread(cl.login, dict(self._creds), dict(clean), **kwargs)
            except Exception as exc:  # noqa: BLE001 -- any failure is a rejection; the message is scrubbed
                why = f"{type(exc).__name__}: {scrub(str(exc), secrets)}"[:200]
                self._event("rejected", f"login source={source} reason={why}")
                log.warning("%s login from %s rejected: %s", self.label, source, why)
                raise TokenRejected(why) from None
            updates: dict[str, str] = {}
            for key, value in dict(got or {}).items():
                v = str(value or "").strip()
                if key not in self.entry.key_names or not v:
                    log.warning("%s login: key %s is not the entry's (or empty); ignored", self.label, key)
                    continue
                if v in secrets:  # a typed/pasted value must never be written to .env
                    log.warning("%s login returned a typed value as %s; refused", self.label, key)
                    continue
                updates[key] = v
            if self.token_key not in updates:
                self._event("rejected", f"login source={source} reason=no {self.token_key} returned")
                raise TokenRejected(f"login returned no {self.token_key}")
            text = await self._apply_locked(updates, source, secrets)
        if text:
            await self._alert(text, urgent=self.client)  # T-346: a client's "login OK again" reaches the same chat
        return self.snapshot()

    # ------------------------------------------------------------------ .env watcher

    def _on_env_change(self, changed: dict[str, str]) -> None:
        """EnvWatcher callback (timer thread): hand a token-key change (DHAN_ACCESS_TOKEN, or the
        entry's token_key) to the loop."""
        key = self.token_key
        if key not in changed:
            return
        new = changed[key].strip()
        if not new:
            log.warning(".env: %s is blank or removed; keeping the current token", key)
            return
        loop = self._loop
        if loop is None or not loop.is_running():
            log.warning(".env: %s changed but no event loop is running; ignored", key)
            return
        asyncio.run_coroutine_threadsafe(self.apply_env_token(new), loop)

    async def apply_env_token(self, new_token: str) -> dict[str, Any] | None:
        try:
            return await self.update_token(new_token, source="env")
        except TokenRejected as exc:
            log.warning(".env token rejected: %s", exc)
            return None
