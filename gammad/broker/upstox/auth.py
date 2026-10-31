"""Upstox login halves (BROKER-ADAPTER-SPEC §5; D86, D89(1), D90(1), D92, D95(5)).

Three ways in, none automated in the client edition:
  paste token    the daily access token OR the 1-year read-only Analytics token (U9, D86) pasted into
                 UPSTOX_ACCESS_TOKEN (Token page / .env) -- both are plain Bearer tokens, no exchange needed.
  client_login   BOTH editions. The user opens `client_login_url` (the authorization dialog for the app's
                 client_id + registered redirect URI), logs in by hand, and pastes the URL the browser lands on
                 (…?code=XXXX&state=…) or the bare code. ONE-SHOT exchange: POST /v2/login/authorization/token
                 form {code, client_id, client_secret, redirect_uri, grant_type=authorization_code}
                 (https://upstox.com/developer/api-documentation/get-token/). redirect_uri must equal the one
                 registered: taken from the pasted URL itself (scheme://host/path), else UPSTOX_REDIRECT_URI. The
                 pasted value is used once: never returned, logged or written; errors never quote it.
  oauth_*        OWNER EDITION ONLY redirect flow driven by gammad/auth/oauth.py (imported INSIDE the functions).
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any
from urllib.parse import parse_qs, urlencode, urlsplit, urlunsplit

import httpx

from gammad.broker.upstox import wire
from gammad.broker.upstox.adapter import (
    KEY_API_KEY,
    KEY_API_SECRET,
    KEY_REDIRECT_URI,
    KEY_TOKEN,
    UpstoxAuthError,
)

FIELD = "redirect_url"
_CODE_RE = re.compile(r"[A-Za-z0-9_\-]{4,256}")
_FORM_HEADERS = {"Accept": "application/json", "Content-Type": "application/x-www-form-urlencoded"}


def parse_code(pasted: str) -> tuple[str, str | None]:
    """Pasted redirect URL or bare code -> (code, redirect_uri taken from the URL or None).
    Raises UpstoxAuthError WITHOUT echoing the pasted text."""
    text = (pasted or "").strip()
    if not text:
        raise UpstoxAuthError("paste the URL Upstox redirected to (or its code) after logging in")
    redirect: str | None = None
    if "://" in text:
        parts = urlsplit(text)
        q = {k: v[0] for k, v in parse_qs(parts.query).items() if v}
        if q.get("error"):
            raise UpstoxAuthError("the Upstox login returned an error; log in again")
        code = (q.get("code") or "").strip()
        if not code:
            raise UpstoxAuthError("the pasted URL carries no code; paste the full URL Upstox redirected to")
        redirect = urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))
    elif "code=" in text:
        q = {k: v[0] for k, v in parse_qs(text.lstrip("?")).items() if v}
        code = (q.get("code") or "").strip()
    else:
        code = text
    if not _CODE_RE.fullmatch(code):
        raise UpstoxAuthError(
            "that does not look like an Upstox authorization code; paste the full redirect URL"
        )
    return code, redirect


def _app(creds: Mapping[str, str]) -> tuple[str, str]:
    key = (creds.get(KEY_API_KEY) or "").strip()
    secret = (creds.get(KEY_API_SECRET) or "").strip()
    missing = [k for k, v in ((KEY_API_KEY, key), (KEY_API_SECRET, secret)) if not v]
    if missing:
        raise UpstoxAuthError(f"the Upstox login exchange needs {', '.join(missing)} in .env")
    return key, secret


def exchange_code(
    creds: Mapping[str, str], code: str, redirect_uri: str, *, transport: httpx.BaseTransport | None = None
) -> dict[str, str]:
    """POST /v2/login/authorization/token -> {UPSTOX_ACCESS_TOKEN}. UpstoxAuthError on refusal (no secrets echoed)."""
    key, secret = _app(creds)
    form = {
        "code": code,
        "client_id": key,
        "client_secret": secret,
        "redirect_uri": redirect_uri,
        "grant_type": "authorization_code",
    }
    with httpx.Client(base_url=wire.API_BASE_URL, timeout=30.0, transport=transport) as http:
        try:
            resp = http.post(wire.EP_TOKEN, data=form, headers=_FORM_HEADERS)
        except httpx.TransportError as exc:
            raise UpstoxAuthError(f"Upstox token exchange: transport error {type(exc).__name__}") from exc
    try:
        body: Any = resp.json()
    except ValueError:
        body = None
    token = str(body.get("access_token") or "").strip() if isinstance(body, dict) else ""
    if resp.status_code >= 400 or not token:
        err, msg = wire.error_info(body)
        raise UpstoxAuthError(
            f"Upstox refused the code exchange (HTTP {resp.status_code} {err or ''}): {msg or 'no access_token'}"
            " -- a code works once; log in again",
            wire.AUTH_FAILED,
            resp.status_code,
        )
    return {KEY_TOKEN: token}


def client_login(
    creds: Mapping[str, str], fields: Mapping[str, str], *, transport: httpx.BaseTransport | None = None
) -> dict[str, str]:
    """ClientLogin.login: fields {"redirect_url": pasted URL or code} -> {UPSTOX_ACCESS_TOKEN}."""
    code, redirect = parse_code(str(fields.get(FIELD) or ""))
    redirect = redirect or (creds.get(KEY_REDIRECT_URI) or "").strip()
    if not redirect:
        raise UpstoxAuthError(
            f"paste the full redirect URL, or set {KEY_REDIRECT_URI} in .env for a bare code"
        )
    return exchange_code(creds, code, redirect, transport=transport)


def client_login_url(creds: Mapping[str, str]) -> str | None:
    """The Upstox authorization dialog the user opens by hand, or None without API key + redirect URI."""
    key = (creds.get(KEY_API_KEY) or "").strip()
    redirect = (creds.get(KEY_REDIRECT_URI) or "").strip()
    if not key or not redirect:
        return None
    return f"{wire.LOGIN_DIALOG_URL}?{urlencode({'response_type': 'code', 'client_id': key, 'redirect_uri': redirect})}"


# ------------------------------------------------------------------------------------------ owner-only OAuth hooks
def oauth_login_url(creds: Mapping[str, str], state: str, redirect_uri: str) -> str:
    """OWNER ONLY. The authorization dialog with the local callback and the single-use state."""
    from gammad.edition import require_owner

    require_owner("Upstox OAuth login")
    key = (creds.get(KEY_API_KEY) or "").strip()
    if not key:
        raise UpstoxAuthError(f"{KEY_API_KEY} is not set (.env)")
    q = {"response_type": "code", "client_id": key, "redirect_uri": redirect_uri, "state": state}
    return f"{wire.LOGIN_DIALOG_URL}?{urlencode(q)}"


def oauth_exchange(
    creds: Mapping[str, str],
    query: Mapping[str, str],
    redirect_uri: str,
    *,
    transport: httpx.BaseTransport | None = None,
) -> dict[str, str]:
    """OWNER ONLY. The callback query (code, state) -> {UPSTOX_ACCESS_TOKEN}, via gammad.auth.oauth.post_form."""
    from gammad.auth.oauth import OAuthError, post_form  # owner-only module, imported here

    code = str(query.get("code") or "").strip()
    if not code:
        raise OAuthError("Upstox callback carried no code")
    key, secret = _app(creds)
    body = post_form(
        wire.API_BASE_URL + wire.EP_TOKEN,
        {
            "code": code,
            "client_id": key,
            "client_secret": secret,
            "redirect_uri": redirect_uri,
            "grant_type": "authorization_code",
        },
        transport=transport,
        headers={"Accept": "application/json"},
    )
    token = str(body.get("access_token") or "").strip()
    if not token:
        raise OAuthError("Upstox exchange returned no access_token")
    return {KEY_TOKEN: token}
