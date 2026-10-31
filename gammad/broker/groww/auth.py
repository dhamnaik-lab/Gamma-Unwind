"""Groww logins (BROKER-ADAPTER-SPEC §5; D86, D92, D95(5)). BETA: not verified live.

Groww's TOTP flow [SDK client.py:1514-1580; https://groww.in/trade-api/docs/python-sdk "2nd Approach: TOTP Flow"]:
    POST https://api.groww.in/v1/token/api/access
    Authorization: Bearer <GROWW_API_KEY = the "TOTP token" from the Groww Cloud API Keys page (no expiry)>
    {"key_type": "totp", "totp": "<today's 6-digit code>"}          -> {"token": "<access token>", ...}
The TOTP flow takes NO api secret and NO PIN (the SDK makes `totp` and `secret` mutually exclusive,
client.py:1520-1521), so the client form asks for the code only.

  * `client_login` -- BOTH editions (D92 (c)): the user TYPES today's code from their authenticator app; it is used
    once for this exchange and is never returned, logged or written. Nothing is scheduled.
  * `auto_login`   -- OWNER EDITION ONLY (D86): the code is generated from GROWW_TOTP_SECRET by gammad.auth.totp,
    imported inside the function so a client process never loads it.
The key+secret "approval" flow (needs a daily click on the Groww Cloud page) is not wired: OWED, see the report.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any

import httpx

from gammad.broker.groww import wire
from gammad.broker.groww.adapter import KEY_TOKEN, GrowwAuthError

KEY_API_KEY = "GROWW_API_KEY"
KEY_TOTP_SECRET = "GROWW_TOTP_SECRET"
FIELD_TOTP = "totp_code"
_CODE_RE = re.compile(r"\d{6}")


def exchange_totp(api_key: str, code: str, *, transport: httpx.BaseTransport | None = None) -> str:
    """API key + a 6-digit TOTP code -> access token. Raises GrowwAuthError; the key and code never appear in it."""
    if not api_key:
        raise GrowwAuthError(f"Groww login needs {KEY_API_KEY} in .env (the TOTP token from Groww Cloud)")
    if not _CODE_RE.fullmatch(code or ""):
        raise GrowwAuthError("Groww login needs today's 6-digit authenticator code")
    headers = {"Authorization": f"Bearer {api_key}", "Accept": "application/json", "x-api-version": "1.0"}
    with httpx.Client(timeout=15.0, transport=transport) as http:
        try:
            resp = http.post(wire.TOKEN_URL, json={"key_type": "totp", "totp": code}, headers=headers)
        except httpx.TransportError as exc:
            raise GrowwAuthError(f"Groww login: transport error {type(exc).__name__}") from exc
    try:
        body: Any = resp.json()
    except ValueError:
        body = None
    token = ""
    if isinstance(body, dict):
        inner = wire.unwrap(body)
        token = str((inner.get("token") if isinstance(inner, dict) else "") or body.get("token") or "").strip()
    if resp.status_code >= 400 or not token:
        fail = wire.failure(body)
        why = fail[1] if fail else ""
        if not why and isinstance(body, dict) and isinstance(body.get("error"), dict):
            why = str(body["error"].get("displayMessage") or "")[:200]
        raise GrowwAuthError(f"Groww login rejected (HTTP {resp.status_code}): {why or 'no token'}", "809",
                             resp.status_code)
    return token


def client_login(
    creds: Mapping[str, str], fields: Mapping[str, str], *, transport: httpx.BaseTransport | None = None
) -> dict[str, str]:
    """ONE-SHOT (both editions): GROWW_API_KEY from .env + the code the user typed -> {GROWW_ACCESS_TOKEN: ...}."""
    code = re.sub(r"\s+", "", str(fields.get(FIELD_TOTP) or ""))
    token = exchange_totp((creds.get(KEY_API_KEY) or "").strip(), code, transport=transport)
    return {KEY_TOKEN: token}


def login_url(creds: Mapping[str, str]) -> str | None:
    """No browser step: the user reads the code from their authenticator app."""
    del creds
    return None


def auto_login(
    creds: Mapping[str, str], *, transport: httpx.BaseTransport | None = None, now: float | None = None
) -> dict[str, str]:
    """OWNER EDITION ONLY: GROWW_API_KEY + GROWW_TOTP_SECRET -> {GROWW_ACCESS_TOKEN: ...}."""
    from gammad.edition import require_owner

    require_owner("Groww TOTP login")
    from gammad.auth.totp import totp  # owner-only module: imported here, never at module import

    secret = (creds.get(KEY_TOTP_SECRET) or "").strip()
    if not secret:
        raise GrowwAuthError(f"Groww TOTP login needs {KEY_TOTP_SECRET} in .env")
    token = exchange_totp((creds.get(KEY_API_KEY) or "").strip(), totp(secret, at=now), transport=transport)
    return {KEY_TOKEN: token}
