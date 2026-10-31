"""Angel One login (BROKER-ADAPTER-SPEC §5, D86, D92, D95(5)).

ONE wire call for both flows: `POST https://apiconnect.angelone.in/rest/auth/angelbroking/user/v1/loginByPassword`
with the X-PrivateKey (API key) header and body {clientcode, password: <PIN>, totp: <6-digit code>}; the response
`data` carries jwtToken + feedToken (+ refreshToken, not kept) -- smartapi-python smartConnect.py L26, L276-298.
The PIN rides in the `password` field (Angel's MPIN login); login is rate-limited to 1/s (A9).

  * `client_login(creds, fields)` -- BOTH editions: the user TYPES today's authenticator code + PIN on the Token
    page; used once, never stored, returned, logged or written. Returns {ANGEL_ACCESS_TOKEN, ANGEL_FEED_TOKEN}.
  * `auto_login(creds, *, transport, now)` -- OWNER EDITION ONLY: the code comes from ANGEL_TOTP_SECRET via
    gammad.auth.totp (imported INSIDE the function, so a client process never loads it) and the PIN from ANGEL_PIN.
    Reached only through gammad.broker.registry.auto_login, which calls edition.require_owner first; this function
    checks again.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any

import httpx

from gammad.broker.angel import wire as W
from gammad.broker.angel.adapter import KEY_API, KEY_CLIENT, KEY_FEED, KEY_TOKEN, AngelAuthError, base_headers

KEY_PIN = "ANGEL_PIN"
KEY_TOTP = "ANGEL_TOTP_SECRET"
_CODE_RE = re.compile(r"\d{6}")


def login(
    api_key: str, client_code: str, pin: str, code: str, *, transport: httpx.BaseTransport | None = None
) -> dict[str, str]:
    """One loginByPassword call -> {ANGEL_ACCESS_TOKEN, ANGEL_FEED_TOKEN}. Raises AngelAuthError; the PIN and the
    code never appear in any message."""
    body = {"clientcode": client_code, "password": pin, "totp": code}
    with httpx.Client(base_url=W.API_BASE_URL, timeout=30.0, transport=transport) as http:
        try:
            resp = http.post(W.EP_LOGIN, json=body, headers=base_headers(api_key))
        except httpx.TransportError as exc:
            raise AngelAuthError(f"Angel login: transport error {type(exc).__name__}") from exc
    try:
        parsed: Any = resp.json()
    except ValueError:
        parsed = None
    data = parsed.get("data") if isinstance(parsed, dict) else None
    jwt = str((data or {}).get("jwtToken") or "").strip() if isinstance(data, dict) else ""
    feed = str((data or {}).get("feedToken") or "").strip() if isinstance(data, dict) else ""
    if resp.status_code >= 400 or W.is_failure(parsed) or not jwt or not feed:
        code_txt = W.error_code(parsed) or resp.status_code
        msg = W.error_message(parsed, "no jwtToken / feedToken in the response")
        for secret in (pin, code):
            if secret:
                msg = msg.replace(secret, "***")
        raise AngelAuthError(f"Angel login rejected ({code_txt}): {msg}", W.error_code(parsed), resp.status_code)
    if jwt.lower().startswith("bearer "):
        jwt = jwt[7:].strip()
    return {KEY_TOKEN: jwt, KEY_FEED: feed}


def _ids(creds: Mapping[str, str]) -> tuple[str, str]:
    api_key = (creds.get(KEY_API) or "").strip()
    client_code = (creds.get(KEY_CLIENT) or "").strip()
    missing = [k for k, v in ((KEY_API, api_key), (KEY_CLIENT, client_code)) if not v]
    if missing:
        raise AngelAuthError(f"Angel login needs {', '.join(missing)} in .env")
    return api_key, client_code


def client_login(
    creds: Mapping[str, str], fields: Mapping[str, str], *, transport: httpx.BaseTransport | None = None
) -> dict[str, str]:
    """D95(5): today's 6-digit authenticator code + PIN, typed by the user -> new token keys (both editions)."""
    api_key, client_code = _ids(creds)
    code = "".join(str(fields.get("totp_code") or "").split())
    pin = str(fields.get("pin") or "").strip()
    if not _CODE_RE.fullmatch(code):
        raise AngelAuthError("Angel login: the authenticator code must be 6 digits")
    if not pin:
        raise AngelAuthError("Angel login: the PIN is empty")
    return login(api_key, client_code, pin, code, transport=transport)


def client_login_url(creds: Mapping[str, str]) -> str | None:
    del creds
    return None  # the code + PIN are typed on the Token page; no browser step


def auto_login(
    creds: Mapping[str, str], *, transport: httpx.BaseTransport | None = None, now: float | None = None
) -> dict[str, str]:
    """OWNER EDITION ONLY (D86): ANGEL_TOTP_SECRET + ANGEL_PIN from .env -> new token keys."""
    from gammad.edition import require_owner

    require_owner("Angel One TOTP login")
    from gammad.auth.totp import totp  # owner-only module: imported here, never at module import

    api_key, client_code = _ids(creds)
    pin = (creds.get(KEY_PIN) or "").strip()
    secret = (creds.get(KEY_TOTP) or "").strip()
    missing = [k for k, v in ((KEY_PIN, pin), (KEY_TOTP, secret)) if not v]
    if missing:
        raise AngelAuthError(f"Angel TOTP login needs {', '.join(missing)} in .env")
    try:
        code = totp(secret, at=now)
    except ValueError as exc:
        raise AngelAuthError(f"{KEY_TOTP} is not a valid base32 secret") from exc
    return login(api_key, client_code, pin, code, transport=transport)
