"""Dhan's registry ENTRY (BROKER-ADAPTER-SPEC §2, §4, §8): `DhanAdapter` and `DhanFeed` UNCHANGED, wrapped to the
registry contract.

  * `DhanRegistryAdapter.set_credentials` accepts the credential MAPPING ({env key: value}, spec §5) AND the
    legacy positional (client_id, access_token) that token_watch.py:295 and app.py still call.
  * `DhanWsFeed` adds `caps` and a `reload_credentials(creds)` that maps the MAPPING to DhanFeed's
    (client_id, access_token); the legacy positional call keeps working. Canonical auth codes are Dhan's own
    (feed.py:112-120), so no code map is needed. LTT is IST wall-clock seconds, shifted to epoch by
    feed.ltt_to_epoch_ms (D28/D77, confirmed live 09:15/09:20 bars).
  * `renew_with_totp` -- OWNER EDITION ONLY (D86): TOTP login at `POST https://auth.dhan.co/app/generateAccessToken`
    with dhanClientId + pin + totp (docs/BROKERS.md:38). UNCONFIRMED wire: fields sent as query parameters and the
    response key `accessToken` follow Dhan's v2 auth docs and are pinned only by a hand-built test fixture; the
    first owner run must confirm them. `gammad.auth.totp` is imported INSIDE the function, so a client process that
    imports this module never loads it.
Identity: Dhan's NSE SECURITY_ID IS the canonical exchange token (§1) -- no native-id map, no identity guard.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

import httpx

from gammad.broker import dhan_calls
from gammad.broker.capabilities import NEED_AUTO_LOGIN, NEED_REQUIRED, BrokerEntry, Capabilities, EnvKey
from gammad.broker.dhan import API_BASE_URL, LTP_MAX_IDS, DhanAdapter, DhanAuthError
from gammad.core import session_clock
from gammad.watch.feed import (
    MAX_CONNECTIONS,
    MAX_PER_CONNECTION,
    AuthErrorCallback,
    Connector,
    DhanFeed,
    Tick,
)
from gammad.watch.feed_protocol import FeedCaps

BROKER = "dhan"
KEY_CLIENT_ID = "DHAN_CLIENT_ID"
KEY_TOKEN = "DHAN_ACCESS_TOKEN"
KEY_PIN = "DHAN_PIN"
KEY_TOTP = "DHAN_TOTP_SECRET"
AUTH_URL = "https://auth.dhan.co/app/generateAccessToken"
DOC_URL = "https://dhanhq.co/docs/v2/"

DAILY_DATA_QUOTA = 100_000  # docs/BROKERS.md "Data API limits": 5 requests / s, 100 000 / day (0.8.5 D14)

CAPS = Capabilities(
    broker=BROKER,
    status="live-verified",
    chain="native",
    expiries="native",
    candle_oi=True,
    candle_volume=True,
    daily_history=True,
    feed="ws",
    feed_oi="native",
    feed_volume="native",
    feed_ltt=True,
    poll_s=None,
    max_feed_instruments=MAX_PER_CONNECTION * MAX_CONNECTIONS,  # = the DhanFeed wrapper's FeedCaps (below)
    ltp_batch=LTP_MAX_IDS,  # DhanAdapter.ltp batches
    chain_calls="1",  # one /optionchain request per chain
    daily_quota=DAILY_DATA_QUOTA,
    token=("paste", "totp"),
    token_expiry_rule="24 h after generation (GET /profile tokenValidity)",
    data_cost="Rs 499/mo Data API plan vs free (disputed)",
    evidence=("DH1", "DH2", "DH3", "DH4", "DH5", "DH6", "D28", "D77"),
)

ENV = (
    EnvKey(KEY_CLIENT_ID, NEED_REQUIRED, secret=False, note="Dhan client id (web.dhan.co profile)"),
    EnvKey(KEY_TOKEN, NEED_REQUIRED, secret=True, written=True, note="access token (web.dhan.co > DhanHQ APIs), 24 h"),
    EnvKey(KEY_PIN, NEED_AUTO_LOGIN, secret=True, note="owner only: PIN for automated TOTP login"),
    EnvKey(KEY_TOTP, NEED_AUTO_LOGIN, secret=True, note="owner only: TOTP secret for automated login"),
)


class DhanRegistryAdapter(DhanAdapter):
    def set_credentials(  # type: ignore[override]
        self, creds: Mapping[str, str] | str, access_token: str | None = None
    ) -> None:
        """Mapping {DHAN_CLIENT_ID, DHAN_ACCESS_TOKEN} (spec §5), or legacy (client_id, access_token).
        A key absent / empty in the mapping keeps the current value."""
        if isinstance(creds, Mapping):
            client_id = (creds.get(KEY_CLIENT_ID) or "").strip() or self.client_id
            token = (creds.get(KEY_TOKEN) or "").strip() or self._token
        else:
            client_id = creds or self.client_id
            token = access_token if access_token is not None else self._token
        super().set_credentials(client_id, token)


class DhanWsFeed(DhanFeed):
    caps = FeedCaps("ws", None, MAX_PER_CONNECTION * MAX_CONNECTIONS)

    def reload_credentials(  # type: ignore[override]
        self, creds: Mapping[str, str] | str | None = None, access_token: str | None = None
    ) -> None:
        if isinstance(creds, Mapping):
            super().reload_credentials(creds.get(KEY_CLIENT_ID) or None, creds.get(KEY_TOKEN) or None)
        else:
            super().reload_credentials(creds, access_token)


def make_adapter(
    creds: Mapping[str, str],
    data_dir: Path,
    *,
    transport: httpx.BaseTransport | None = None,
    clock: Callable[[], float] | None = None,
    sleep: Callable[[float], None] | None = None,
) -> DhanRegistryAdapter:
    client_id = (creds.get(KEY_CLIENT_ID) or "").strip()
    token = (creds.get(KEY_TOKEN) or "").strip()
    if not client_id or not token:
        raise DhanAuthError(f"{KEY_CLIENT_ID} / {KEY_TOKEN} not set (.env)")
    kw: dict[str, Any] = {}
    kw["http"] = httpx.Client(
        base_url=API_BASE_URL, timeout=30.0, transport=transport,
        event_hooks={"request": [dhan_calls.request_hook], "response": [dhan_calls.response_hook]},
    )
    if clock is not None:
        kw["clock"] = clock
    if sleep is not None:
        kw["sleep"] = sleep
    return DhanRegistryAdapter(client_id, token, Path(data_dir), **kw)


def make_feed(
    creds: Mapping[str, str],
    out: asyncio.Queue[Tick],
    on_auth_error: AuthErrorCallback | None = None,
    *,
    adapter: object | None = None,
    connect: Connector | None = None,
    clock_ms: Callable[[], int] | None = None,
) -> DhanWsFeed:
    del adapter  # a WebSocket feed has its own connection; kept for the uniform entry signature
    kw: dict[str, Any] = {}
    if connect is not None:
        kw["connect"] = connect
    if clock_ms is not None:
        kw["clock_ms"] = clock_ms
    kw.setdefault("market_hours", session_clock.fo_market_hours)
    return DhanWsFeed(
        (creds.get(KEY_CLIENT_ID) or "").strip(),
        (creds.get(KEY_TOKEN) or "").strip(),
        out,
        on_auth_error=on_auth_error,
        **kw,
    )


def renew_with_totp(
    creds: Mapping[str, str],
    *,
    transport: httpx.BaseTransport | None = None,
    now: float | None = None,
) -> dict[str, str]:
    """OWNER EDITION ONLY. PIN + TOTP -> a fresh access token {DHAN_CLIENT_ID, DHAN_ACCESS_TOKEN}.
    Raises DhanAuthError when a key is missing or Dhan refuses; secrets are never logged."""
    from gammad.edition import require_owner

    require_owner("Dhan TOTP login")
    from gammad.auth.totp import totp  # owner-only module: imported here, never at module import

    client_id = (creds.get(KEY_CLIENT_ID) or "").strip()
    pin = (creds.get(KEY_PIN) or "").strip()
    secret = (creds.get(KEY_TOTP) or "").strip()
    missing = [k for k, v in ((KEY_CLIENT_ID, client_id), (KEY_PIN, pin), (KEY_TOTP, secret)) if not v]
    if missing:
        raise DhanAuthError(f"TOTP login needs {', '.join(missing)} in .env")
    code = totp(secret, at=now)
    with httpx.Client(timeout=30.0, transport=transport) as http:
        try:
            resp = http.post(AUTH_URL, params={"dhanClientId": client_id, "pin": pin, "totp": code})
        except httpx.TransportError as exc:
            raise DhanAuthError(f"TOTP login: transport error {type(exc).__name__}") from exc
    try:
        body: Any = resp.json()
    except ValueError:
        body = None
    token = ""
    if isinstance(body, dict):
        token = str(body.get("accessToken") or body.get("access_token") or "").strip()
    if resp.status_code >= 400 or not token:
        msg = ""
        if isinstance(body, dict):
            msg = str(body.get("errorMessage") or body.get("message") or body.get("errorCode") or "")[:200]
        raise DhanAuthError(f"TOTP login rejected (HTTP {resp.status_code}): {msg or 'no accessToken'}")
    return {KEY_CLIENT_ID: client_id, KEY_TOKEN: token}


ENTRY = BrokerEntry(
    name=BROKER,
    label="Dhan",
    caps=CAPS,
    env_keys=ENV,
    token_key=KEY_TOKEN,
    make_adapter=make_adapter,
    make_feed=make_feed,
    doc_url=DOC_URL,
    auto_login=renew_with_totp,
    oauth=None,
)
