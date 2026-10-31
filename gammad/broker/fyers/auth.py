"""Fyers logins (BROKER-ADAPTER-SPEC §5; D86, D89(1), D90(1), D92 (b), D95(5)). BETA: not verified live.

Wire [SKILL auth.md; SDK fyersModel.py:643-695]:
    login page  GET  https://api-t1.fyers.in/api/v3/generate-authcode?client_id&redirect_uri&response_type=code&state
                -> the broker redirects to <redirect_uri>?s=ok&code=200&auth_code=<CODE>&state=<...>
    exchange    POST https://api-t1.fyers.in/api/v3/validate-authcode
                {"grant_type": "authorization_code", "appIdHash": sha256("<app_id>:<secret>"), "code": "<auth_code>"}
                -> {"s": "ok", "access_token": ..., "refresh_token": ...}
    refresh     POST https://api-t1.fyers.in/api/v3/validate-refresh-token
                {"grant_type": "refresh_token", "appIdHash", "refresh_token", "pin"} -> {"access_token"} (15 days, F8;
                the vendor skill warns it "may be discontinued")
  * `client_login` -- BOTH editions (D89(1), D92 (b)): the user logs in MANUALLY in their own browser and pastes the
    redirect URL (or the bare auth_code); ONE exchange with FYERS_SECRET_KEY from .env. The pasted value is never
    returned, logged or written; only the new FYERS_ACCESS_TOKEN comes back.
  * `oauth_login_url` / `oauth_exchange` -- OWNER EDITION ONLY hooks for gammad/auth/oauth.py (redirect server).
  * `refresh_with_pin` -- OWNER EDITION ONLY auto login (FYERS_REFRESH_TOKEN + FYERS_PIN).
This module imports no gammad.auth.* code (the entry module is imported in the client edition).
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping
from typing import Any
from urllib.parse import parse_qs, urlencode, urlsplit

import httpx

from gammad.broker.fyers import wire
from gammad.broker.fyers.adapter import KEY_APP_ID, KEY_TOKEN, FyersAuthError

KEY_SECRET = "FYERS_SECRET_KEY"
KEY_REDIRECT = "FYERS_REDIRECT_URI"
KEY_REFRESH = "FYERS_REFRESH_TOKEN"
KEY_PIN = "FYERS_PIN"
FIELD_REDIRECT = "redirect_url"
_CODE_RE = re.compile(r"[A-Za-z0-9._\-]{8,}")


def app_id_hash(app_id: str, secret: str) -> str:
    """sha256("<app_id>:<secret>") hex [SDK fyersModel.py:679-681]."""
    return hashlib.sha256(f"{app_id}:{secret}".encode()).hexdigest()


def auth_code_from(pasted: str) -> str:
    """The auth_code from a pasted redirect URL (?auth_code=...) or a bare code. Raises FyersAuthError (the
    pasted value is never echoed)."""
    text = (pasted or "").strip()
    if "auth_code=" in text or text.lower().startswith(("http://", "https://")):
        q = parse_qs(urlsplit(text).query)
        if (q.get("s") or ["ok"])[0].lower() == "error":
            raise FyersAuthError("Fyers login page reported an error; log in again and paste the new URL")
        code = (q.get("auth_code") or [""])[0].strip()
    else:
        code = text
    if not _CODE_RE.fullmatch(code):
        raise FyersAuthError("paste the full redirect URL from the Fyers login (it contains auth_code=...)")
    return code


def _post(url: str, payload: dict[str, str], transport: httpx.BaseTransport | None) -> dict[str, Any]:
    with httpx.Client(timeout=20.0, transport=transport) as http:
        try:
            resp = http.post(url, json=payload, headers={"Accept": "application/json"})
        except httpx.TransportError as exc:
            raise FyersAuthError(f"Fyers login: transport error {type(exc).__name__}") from exc
    try:
        body: Any = resp.json()
    except ValueError:
        body = None
    if resp.status_code >= 400 or not isinstance(body, dict) or str(body.get("s", "")).lower() != "ok" or not str(
        body.get("access_token") or ""
    ).strip():
        why = str(body.get("message") or "")[:200] if isinstance(body, dict) else ""
        code = str(body.get("code") or "") if isinstance(body, dict) else ""
        raise FyersAuthError(f"Fyers login rejected (HTTP {resp.status_code} {code}): {why or 'no access_token'}",
                             "809", resp.status_code)
    return body


def exchange_code(
    creds: Mapping[str, str], auth_code: str, *, transport: httpx.BaseTransport | None = None
) -> dict[str, str]:
    """auth_code -> {FYERS_ACCESS_TOKEN (+ FYERS_REFRESH_TOKEN when Fyers returns one)}."""
    app_id = (creds.get(KEY_APP_ID) or "").strip()
    secret = (creds.get(KEY_SECRET) or "").strip()
    missing = [k for k, v in ((KEY_APP_ID, app_id), (KEY_SECRET, secret)) if not v]
    if missing:
        raise FyersAuthError(f"Fyers login needs {', '.join(missing)} in .env")
    body = _post(
        wire.VALIDATE_AUTHCODE_URL,
        {"grant_type": "authorization_code", "appIdHash": app_id_hash(app_id, secret), "code": auth_code},
        transport,
    )
    out = {KEY_TOKEN: str(body["access_token"]).strip()}
    if str(body.get("refresh_token") or "").strip():
        out[KEY_REFRESH] = str(body["refresh_token"]).strip()
    return out


def client_login(
    creds: Mapping[str, str], fields: Mapping[str, str], *, transport: httpx.BaseTransport | None = None
) -> dict[str, str]:
    """ONE-SHOT (both editions): the pasted redirect URL / auth_code -> {FYERS_ACCESS_TOKEN}. The refresh token is
    NOT returned: nothing in the client edition may re-login on its own (D86)."""
    code = auth_code_from(str(fields.get(FIELD_REDIRECT) or ""))
    new = exchange_code(creds, code, transport=transport)
    return {KEY_TOKEN: new[KEY_TOKEN]}


def login_url(creds: Mapping[str, str], state: str = "gamma-unwind", redirect_uri: str | None = None) -> str | None:
    """The Fyers login page the user opens by hand (None when FYERS_APP_ID / the redirect URI are not set)."""
    app_id = (creds.get(KEY_APP_ID) or "").strip()
    redirect = (redirect_uri or creds.get(KEY_REDIRECT) or "").strip()
    if not app_id or not redirect:
        return None
    q = {"client_id": app_id, "redirect_uri": redirect, "response_type": "code", "state": state}
    return f"{wire.AUTHCODE_URL}?{urlencode(q)}"


def client_login_url(creds: Mapping[str, str]) -> str | None:
    return login_url(creds)


def oauth_login_url(creds: Mapping[str, str], state: str, redirect_uri: str) -> str:
    """OWNER EDITION ONLY (OAuthHooks.login_url)."""
    from gammad.edition import require_owner

    require_owner("Fyers OAuth login")
    url = login_url(creds, state, redirect_uri)
    if url is None:
        raise FyersAuthError(f"Fyers login needs {KEY_APP_ID} in .env")
    return url


def oauth_exchange(
    creds: Mapping[str, str], query: Mapping[str, Any], redirect_uri: str, *,
    transport: httpx.BaseTransport | None = None,
) -> dict[str, str]:
    """OWNER EDITION ONLY (OAuthHooks.exchange): the callback query's auth_code -> access + refresh token."""
    from gammad.edition import require_owner

    require_owner("Fyers OAuth login")
    del redirect_uri  # validate-authcode does not take it (SKILL auth.md step 2)
    raw = query.get("auth_code")
    code = raw[0] if isinstance(raw, list | tuple) else raw
    return exchange_code(creds, auth_code_from(str(code or "")), transport=transport)


def refresh_with_pin(
    creds: Mapping[str, str], *, transport: httpx.BaseTransport | None = None, now: float | None = None
) -> dict[str, str]:
    """OWNER EDITION ONLY auto login: refresh token (15 days) + PIN -> a new FYERS_ACCESS_TOKEN."""
    from gammad.edition import require_owner

    require_owner("Fyers PIN refresh")
    del now
    app_id = (creds.get(KEY_APP_ID) or "").strip()
    secret = (creds.get(KEY_SECRET) or "").strip()
    refresh = (creds.get(KEY_REFRESH) or "").strip()
    pin = (creds.get(KEY_PIN) or "").strip()
    missing = [k for k, v in ((KEY_APP_ID, app_id), (KEY_SECRET, secret), (KEY_REFRESH, refresh), (KEY_PIN, pin))
               if not v]
    if missing:
        raise FyersAuthError(f"Fyers PIN refresh needs {', '.join(missing)} in .env")
    body = _post(
        wire.VALIDATE_REFRESH_URL,
        {"grant_type": "refresh_token", "appIdHash": app_id_hash(app_id, secret), "refresh_token": refresh,
         "pin": pin},
        transport,
    )
    return {KEY_TOKEN: str(body["access_token"]).strip()}
