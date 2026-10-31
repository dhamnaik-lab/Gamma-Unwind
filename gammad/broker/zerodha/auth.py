"""Zerodha Kite login halves (BROKER-ADAPTER-SPEC §5; D86, D89(1), D90(1), D92, D95(5)).

Kite login is MANUAL every day (K7 06:00 expiry; K8 staff advise against automating it). Two ways in:

  client_login   BOTH editions. The user logs in by hand at `client_login_url` (Kite's login page for the app's
                 api_key), Kite redirects to the app's registered redirect URL with `request_token=...`; the user
                 pastes that URL (or the bare request_token) on the Token page / in .env. ONE-SHOT exchange:
                 POST /session/token {api_key, request_token, checksum = sha256(api_key + request_token +
                 api_secret)} (https://kite.trade/docs/connect/v3/user/#authentication-and-token-exchange). The
                 pasted value is used once: never returned, logged or written; errors never quote it.
  oauth_*        OWNER EDITION ONLY redirect flow driven by gammad/auth/oauth.py; `state` rides Kite's
                 `redirect_params` (returned verbatim on the redirect). gammad.auth.oauth is imported INSIDE the
                 functions (it refuses to import in the client edition).
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping
from typing import Any
from urllib.parse import parse_qs, urlencode, urlsplit

import httpx

from gammad.broker.zerodha import wire
from gammad.broker.zerodha.adapter import KEY_API_KEY, KEY_API_SECRET, KEY_TOKEN, KiteAuthError

FIELD = "redirect_url"
_TOKEN_RE = re.compile(r"[A-Za-z0-9]{6,128}")


def checksum(api_key: str, request_token: str, api_secret: str) -> str:
    """Kite session checksum: SHA-256 hex of api_key + request_token + api_secret (stdlib; client-safe)."""
    return hashlib.sha256(f"{api_key}{request_token}{api_secret}".encode()).hexdigest()


def parse_request_token(pasted: str) -> str:
    """The redirect URL (…?request_token=X&action=login&status=success) or a bare request_token -> X.
    Raises KiteAuthError WITHOUT echoing the pasted text."""
    text = (pasted or "").strip()
    if not text:
        raise KiteAuthError("paste the redirect URL (or its request_token) after logging in to Kite")
    if "request_token" in text or "://" in text or "?" in text:
        query = urlsplit(text).query if "://" in text or text.startswith("?") else text.split("?", 1)[-1]
        q = {k: v[0] for k, v in parse_qs(query).items() if v}
        status = q.get("status")
        if status and status != "success":
            raise KiteAuthError(f"Kite login did not succeed (status={status[:20]}); log in again")
        token = (q.get("request_token") or "").strip()
        if not token:
            raise KiteAuthError(
                "the pasted URL carries no request_token; paste the full URL Kite redirected to"
            )
    else:
        token = text
    if not _TOKEN_RE.fullmatch(token):
        raise KiteAuthError("that does not look like a Kite request_token; paste the full redirect URL")
    return token


def _creds(creds: Mapping[str, str]) -> tuple[str, str]:
    api_key = (creds.get(KEY_API_KEY) or "").strip()
    secret = (creds.get(KEY_API_SECRET) or "").strip()
    missing = [k for k, v in ((KEY_API_KEY, api_key), (KEY_API_SECRET, secret)) if not v]
    if missing:
        raise KiteAuthError(f"the Kite login exchange needs {', '.join(missing)} in .env")
    return api_key, secret


def exchange_request_token(
    creds: Mapping[str, str], request_token: str, *, transport: httpx.BaseTransport | None = None
) -> dict[str, str]:
    """POST /session/token -> {KITE_API_KEY, KITE_ACCESS_TOKEN}. KiteAuthError on refusal (no secrets echoed)."""
    api_key, secret = _creds(creds)
    form = {
        "api_key": api_key,
        "request_token": request_token,
        "checksum": checksum(api_key, request_token, secret),
    }
    with httpx.Client(base_url=wire.API_BASE_URL, timeout=30.0, transport=transport) as http:
        try:
            resp = http.post(wire.EP_SESSION, data=form, headers={"X-Kite-Version": wire.KITE_VERSION})
        except httpx.TransportError as exc:
            raise KiteAuthError(f"Kite token exchange: transport error {type(exc).__name__}") from exc
    try:
        body: Any = resp.json()
    except ValueError:
        body = None
    data = body.get("data") if isinstance(body, dict) else None
    token = str(data.get("access_token") or "").strip() if isinstance(data, dict) else ""
    if resp.status_code >= 400 or not token:
        etype, msg = wire.error_info(body)
        raise KiteAuthError(
            f"Kite refused the token exchange (HTTP {resp.status_code} {etype or ''}): {msg or 'no access_token'}"
            " -- a request_token works once and only for a few minutes; log in again",
            wire.auth_code_for_error(resp.status_code, etype),
            resp.status_code,
        )
    return {KEY_API_KEY: api_key, KEY_TOKEN: token}


def client_login(
    creds: Mapping[str, str], fields: Mapping[str, str], *, transport: httpx.BaseTransport | None = None
) -> dict[str, str]:
    """ClientLogin.login: fields {"redirect_url": pasted URL or request_token} -> new credentials."""
    return exchange_request_token(
        creds, parse_request_token(str(fields.get(FIELD) or "")), transport=transport
    )


def client_login_url(creds: Mapping[str, str]) -> str | None:
    """The Kite login page for this app (the user opens it by hand), or None without KITE_API_KEY."""
    api_key = (creds.get(KEY_API_KEY) or "").strip()
    if not api_key:
        return None
    return f"{wire.LOGIN_URL}?{urlencode({'v': wire.KITE_VERSION, 'api_key': api_key})}"


# ------------------------------------------------------------------------------------------ owner-only OAuth hooks
def oauth_login_url(creds: Mapping[str, str], state: str, redirect_uri: str) -> str:
    """OWNER ONLY. Kite ignores a per-request redirect_uri (the app console's URL is used, so the owner registers
    http://127.0.0.1:<GAMMA_PORT>/auth/zerodha/callback); `state` is carried back via redirect_params."""
    from gammad.edition import require_owner

    require_owner("Kite OAuth login")
    del redirect_uri
    api_key = (creds.get(KEY_API_KEY) or "").strip()
    if not api_key:
        raise KiteAuthError(f"{KEY_API_KEY} is not set (.env)")
    q = {"v": wire.KITE_VERSION, "api_key": api_key, "redirect_params": urlencode({"state": state})}
    return f"{wire.LOGIN_URL}?{urlencode(q)}"


def oauth_exchange(
    creds: Mapping[str, str],
    query: Mapping[str, str],
    redirect_uri: str,
    *,
    transport: httpx.BaseTransport | None = None,
) -> dict[str, str]:
    """OWNER ONLY. The callback query (request_token, status, state) -> new credentials, via gammad.auth.oauth."""
    from gammad.auth.oauth import OAuthError, kite_checksum, post_form  # owner-only module, imported here

    del redirect_uri
    if (query.get("status") or "success") != "success":
        raise OAuthError("Kite login did not succeed")
    request_token = str(query.get("request_token") or "").strip()
    if not request_token:
        raise OAuthError("Kite callback carried no request_token")
    api_key, secret = _creds(creds)
    body = post_form(
        wire.API_BASE_URL + wire.EP_SESSION,
        {
            "api_key": api_key,
            "request_token": request_token,
            "checksum": kite_checksum(api_key, request_token, secret),
        },
        transport=transport,
        headers={"X-Kite-Version": wire.KITE_VERSION},
    )
    data = body.get("data")
    token = str(data.get("access_token") or "").strip() if isinstance(data, dict) else ""
    if not token:
        raise OAuthError("Kite exchange returned no access_token")
    return {KEY_API_KEY: api_key, KEY_TOKEN: token}
