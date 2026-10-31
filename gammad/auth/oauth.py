"""OWNER EDITION ONLY: OAuth2 redirect login for brokers (BROKER-ADAPTER-SPEC §5 "oauth" row; D86).

D86: in the CLIENT edition broker login is never automated -- the client pastes the access token (or the broker's
required credentials) in `.env` or on the Token page. This module refuses to import in the client edition
(`edition.require_owner` below raises `OwnerOnlyError`) and every public method re-checks at call time.

Flow (the API server owns the routes; this module owns the state and the order):
    GET /auth/<broker>/login    -> OAuthFlow.begin(entry, creds, port)       -> 302 to the returned broker URL
    broker redirects to          http://127.0.0.1:<GAMMA_PORT>/auth/<broker>/callback   (redirect_uri(); the
                                 client registers it in the broker's app console; the server binds 127.0.0.1)
    GET /auth/<broker>/callback -> OAuthFlow.complete(entry, creds, query, port)
                                   state checked: known, unexpired (10-min TTL), bound to THIS broker, single-use
                                   (popped before the exchange, so a replay fails even if the exchange raised)
                                -> entry.oauth.exchange(creds, query, redirect_uri, transport=...) server-side
                                -> new creds {env key: value}; the caller then runs the D32 order (validate with
                                   the NEW token -> write .env -> swap adapter creds -> reload feed)
                                -> redirect to /#/token?ok=<broker>
A wrong, expired, replayed or other-broker state raises OAuthStateError and NOTHING is exchanged or written.
Helpers for the per-broker `exchange` hooks: `kite_checksum` (sha256(api_key + request_token + api_secret)) and
`post_form` (form POST -> JSON dict). Values are never logged.
"""

from __future__ import annotations

import hashlib
import secrets
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

import httpx

from gammad.edition import require_owner
from gammad.log import get_logger

require_owner("gammad.auth.oauth (automated broker login)")

log = get_logger(__name__)

STATE_TTL_S = 600.0
MAX_PENDING = 32


class OAuthError(RuntimeError):
    """The broker refused the code exchange, or the callback carried an error."""


class OAuthStateError(OAuthError):
    """Unknown, expired, replayed or other-broker `state`: nothing is exchanged."""


def redirect_uri(port: int, broker: str) -> str:
    require_owner("gammad.auth.oauth")
    return f"http://127.0.0.1:{int(port)}/auth/{broker}/callback"


def kite_checksum(api_key: str, request_token: str, api_secret: str) -> str:
    """Kite Connect session checksum: SHA-256 hex of api_key + request_token + api_secret."""
    require_owner("gammad.auth.oauth")
    return hashlib.sha256(f"{api_key}{request_token}{api_secret}".encode()).hexdigest()


def post_form(
    url: str,
    data: Mapping[str, str],
    *,
    transport: httpx.BaseTransport | None = None,
    headers: Mapping[str, str] | None = None,
    timeout: float = 30.0,
) -> dict[str, Any]:
    """Form-encoded POST -> JSON object. OAuthError on transport failure, non-2xx or a non-object body."""
    require_owner("gammad.auth.oauth")
    with httpx.Client(timeout=timeout, transport=transport) as http:
        try:
            resp = http.post(url, data=dict(data), headers=dict(headers or {}))
        except httpx.TransportError as exc:
            raise OAuthError(f"token exchange: transport error {type(exc).__name__}") from exc
    try:
        body = resp.json()
    except ValueError:
        body = None
    if not 200 <= resp.status_code < 300 or not isinstance(body, dict):
        raise OAuthError(f"token exchange refused: HTTP {resp.status_code}")
    return body


@dataclass
class _Pending:
    broker: str
    expires: float


class OAuthFlow:
    """Single-use, broker-bound, TTL-limited `state` store + the begin/complete order. One per server."""

    def __init__(
        self,
        *,
        ttl_s: float = STATE_TTL_S,
        clock: Callable[[], float] = time.time,
        new_state: Callable[[], str] = lambda: secrets.token_urlsafe(32),
    ) -> None:
        require_owner("gammad.auth.oauth")
        self.ttl_s = ttl_s
        self._clock = clock
        self._new_state = new_state
        self._pending: dict[str, _Pending] = {}

    def _purge(self) -> None:
        now = self._clock()
        for s in [s for s, p in self._pending.items() if p.expires <= now]:
            del self._pending[s]

    @property
    def pending(self) -> int:
        self._purge()
        return len(self._pending)

    def begin(self, entry: Any, creds: Mapping[str, str], port: int) -> str:
        """A fresh state + the broker's authorize URL carrying it."""
        require_owner("gammad.auth.oauth")
        hooks = getattr(entry, "oauth", None)
        if hooks is None:
            raise OAuthError(f"{entry.name} has no OAuth login")
        self._purge()
        while len(self._pending) >= MAX_PENDING:  # oldest first
            del self._pending[min(self._pending, key=lambda s: self._pending[s].expires)]
        state = self._new_state()
        self._pending[state] = _Pending(entry.name, self._clock() + self.ttl_s)
        return hooks.login_url(dict(creds), state, redirect_uri(port, entry.name))

    def complete(
        self,
        entry: Any,
        creds: Mapping[str, str],
        query: Mapping[str, str],
        port: int,
        *,
        transport: httpx.BaseTransport | None = None,
    ) -> dict[str, str]:
        """Check `state`, then exchange the code. Returns the new credentials {env key: value}."""
        require_owner("gammad.auth.oauth")
        hooks = getattr(entry, "oauth", None)
        if hooks is None:
            raise OAuthError(f"{entry.name} has no OAuth login")
        state = str(query.get("state") or "")
        self._purge()
        pending = self._pending.pop(state, None) if state else None
        if pending is None:
            log.warning("oauth %s: callback with an unknown/expired/replayed state refused", entry.name)
            raise OAuthStateError("unknown, expired or already-used state")
        if pending.broker != entry.name:
            log.warning("oauth %s: callback carried a state issued for %s; refused", entry.name, pending.broker)
            raise OAuthStateError("state was issued for another broker")
        if query.get("error"):
            raise OAuthError(f"broker returned error: {str(query.get('error'))[:100]}")
        new = hooks.exchange(dict(creds), dict(query), redirect_uri(port, entry.name), transport=transport)
        token = (new or {}).get(entry.token_key, "")
        if not isinstance(new, dict) or not str(token).strip():
            raise OAuthError(f"{entry.name} exchange returned no {entry.token_key}")
        return {str(k): str(v) for k, v in new.items()}
