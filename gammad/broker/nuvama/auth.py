"""Nuvama login: the ONE-SHOT redirect-URL paste (D89(1), D92(b), D95(5)) -- both editions, nothing automated.

Why `ClientLogin(fields=("redirect_url",))` and not a plain token paste: Nuvama's login IS a redirect flow (DOC
"User Login Flow": open https://www.nuvamawealth.com/api-connect/login?api_key=..., log in, "the Request id will be
mentioned in the address bar after redirection to the Redirect URL (default - https://127.0.0.1/)"; R7 N1/N2), and
no Nuvama page shows the user a ready access token to paste. A data call needs THREE values the user never sees
(SDK http.py:50-56): the session `auth`, the vendor session (SourceToken) and the AppIdKey. So the user pastes the
redirect URL (or the bare request id); `client_login` exchanges it ONCE:
  1. POST loginvendor/{api_key} {"pwd": <API secret>}, header Source = API key -> body.msg = vendor session; the
     response header AppIdKey is kept (SPEC opIdvalidateVendorCrdntials declares only the `source` header; the
     GnrResDao body {msg, success} has no AppIdKey field, so the header is where it comes from -- SDK http.py:67-68).
  2. POST logindata {"reqId": <request id>}, headers SourceToken + AppIdKey -> body.data.auth = session. SPEC
     opIdloginData declares BOTH headers `required: true` ("key obtained from the generate sesion"): AppIdKey was
     sent only "when issued" before 2026-09-25 and is now always sent; when step 1 issued none, the refusal says so
     (the call still goes out: whether Nuvama enforces the header is for the first live login to show).
     HTTP 222 there = "Request id expired" (SPEC) -> "log in again and paste the new URL".
and returns {NUVAMA_ACCESS_TOKEN: pack(Session)}. The request id, the pasted URL and the API secret are never
returned, logged or written anywhere by this module (D92, D95(5)).
Static IP (SPEC #changelog 2.0.11 "Static IP Mandatory"): NUVAMA_STATIC_IP is required before any call is made;
from any other public IP Nuvama is expected to refuse, and that refusal would look like a bad key.

No OAuthHooks (owner redirect server): the login URL documents neither a `state` nor a `redirect_uri` parameter,
and the redirect goes to the URL registered with Nuvama (default https://127.0.0.1/ -- no port), so the owner's
callback route could not verify `state`. The evidence does not support it; the owner uses this same paste flow.

PROVISIONAL (first live login confirms): the request-id query key name in the redirect URL (accepted: requestId,
request_id, reqId, reqid, request-id; or the bare id -- SPEC says only "mentioned in the address bar"); that
loginvendor answers an AppIdKey header to a caller that sent none (the SDK sends its own baked-in one,
APIConnect.py:109; SPEC lists only `source` for loginvendor, so we send none); Nuvama's error text for an
unregistered IP (no sample in the SPEC); the pack format is ours (not Nuvama's).
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any
from urllib.parse import parse_qsl, urlsplit

import httpx

from gammad.broker.base import BrokerAuthError
from gammad.broker.nuvama import wire

KEY_API_KEY = "NUVAMA_API_KEY"
KEY_API_SECRET = "NUVAMA_API_SECRET"
KEY_TOKEN = "NUVAMA_ACCESS_TOKEN"
KEY_STATIC_IP = "NUVAMA_STATIC_IP"
FIELD_REDIRECT = "redirect_url"

SEP = "|"
_REQ_KEYS = ("requestid", "request_id", "reqid", "request-id")

STATIC_IP_NEED = (
    f"{KEY_STATIC_IP} is not set: Nuvama requires every API call to come from a static public IP registered with "
    "Nuvama (API Connect changelog 2.0.11, 'Static IP Mandatory'). Register one with Nuvama, put it in .env and "
    "run the app on that connection"
)
_ID_RE = re.compile(r"[A-Za-z0-9._\-]{4,512}")


class NuvamaAuthError(BrokerAuthError):
    """Nuvama rejected (or was never given) the session. `code` is the canonical feed code ("807" / "809")."""

    def __init__(self, message: str, code: str | None = "809", status: int | None = None) -> None:
        super().__init__(message, code, status)


@dataclass(frozen=True)
class Session:
    """What every data call carries (SDK http.py:50-56). Stored as ONE .env value (NUVAMA_ACCESS_TOKEN) so the
    generic D32 path (validate -> write token_key -> set_credentials -> reload feed) works unchanged."""

    auth: str
    source_token: str
    app_id_key: str = ""

    def pack(self) -> str:
        return SEP.join((self.auth, self.source_token, self.app_id_key)).rstrip(SEP)


def unpack(value: str | None) -> Session:
    """NUVAMA_ACCESS_TOKEN -> Session. Raises NuvamaAuthError (never echoing the value) when malformed."""
    parts = [p.strip() for p in (value or "").strip().split(SEP)]
    if len(parts) < 2 or not parts[0] or not parts[1] or len(parts) > 3:
        raise NuvamaAuthError(
            f"{KEY_TOKEN} is not a Nuvama session: paste the redirect URL on the Broker login page after logging in",
            "809",
        )
    return Session(parts[0], parts[1], parts[2] if len(parts) == 3 else "")


def request_id_from(pasted: str) -> str:
    """The redirect URL (or a bare request id) -> request id. Raises NuvamaAuthError without echoing the input."""
    s = (pasted or "").strip()
    if not s:
        raise NuvamaAuthError(
            "paste the address-bar URL you were redirected to after the Nuvama login", "809"
        )
    if "://" in s or "?" in s or "=" in s:
        parts = urlsplit(s if "://" in s else "x://x/?" + s.lstrip("?"))
        for text in (parts.query, parts.fragment):
            for k, v in parse_qsl(text, keep_blank_values=False):
                if k.strip().lower() in _REQ_KEYS and _ID_RE.fullmatch(v.strip()):
                    return v.strip()
        raise NuvamaAuthError(
            "no request id found in the pasted URL (log in again and copy the full address)", "809"
        )
    if _ID_RE.fullmatch(s):
        return s
    raise NuvamaAuthError("the pasted text is neither the redirect URL nor a request id", "809")


def login_url(creds: Mapping[str, str]) -> str | None:
    """The Nuvama login page the user opens by hand (DOC)."""
    key = (creds.get(KEY_API_KEY) or "").strip()
    return wire.LOGIN_PAGE.format(api_key=key) if key else None


def _post(
    http: httpx.Client, path: str, body: dict[str, Any], headers: dict[str, str], what: str
) -> httpx.Response:
    try:
        return http.post(path, json=body, headers=headers)
    except httpx.TransportError as exc:
        raise NuvamaAuthError(f"Nuvama {what}: transport error {type(exc).__name__}", "809") from exc


def _json(resp: httpx.Response) -> Any:
    try:
        return resp.json()
    except ValueError:
        return None


def client_login(
    creds: Mapping[str, str], fields: Mapping[str, str], *, transport: httpx.BaseTransport | None = None
) -> dict[str, str]:
    """ONE-SHOT: pasted redirect URL / request id + the API key and secret from .env -> {NUVAMA_ACCESS_TOKEN}.
    Both editions (D92(b)). Raises NuvamaAuthError; the pasted value and the secret are never logged/returned."""
    api_key = (creds.get(KEY_API_KEY) or "").strip()
    secret = (creds.get(KEY_API_SECRET) or "").strip()
    missing = [k for k, v in ((KEY_API_KEY, api_key), (KEY_API_SECRET, secret)) if not v]
    if missing:
        raise NuvamaAuthError(f"Nuvama login needs {', '.join(missing)} in .env", "809")
    static_ip = (creds.get(KEY_STATIC_IP) or "").strip()
    if not static_ip:
        raise NuvamaAuthError(STATIC_IP_NEED, "809")
    req_id = request_id_from(str(fields.get(FIELD_REDIRECT) or ""))
    base: dict[str, str] = {"Accept": "application/json", "X-Forwarded-For": static_ip}
    with httpx.Client(base_url=wire.HOST, timeout=30.0, transport=transport) as http:
        r1 = _post(
            http,
            wire.PATH_LOGIN_VENDOR.format(api_key=api_key),
            {"pwd": secret},
            base | {"Source": api_key},
            "vendor login",
        )
        b1 = _json(r1)
        vt = wire.vendor_session(b1) if r1.status_code == 200 else ""
        if not vt:
            why = wire.error_text(b1) or "no vendor session"
            raise NuvamaAuthError(
                f"Nuvama refused the API key/secret (HTTP {r1.status_code}): {why}", "809", r1.status_code
            )
        app_id_key = (r1.headers.get("AppIdKey") or "").strip()
        # SPEC opIdloginData: SourceToken AND AppIdKey are required headers. Both always go out; an AppIdKey the
        # vendor login did not issue goes out empty and, if Nuvama refuses, the refusal names it (below).
        h2 = base | {"SourceToken": vt, "AppIdKey": app_id_key}
        r2 = _post(http, wire.PATH_LOGIN_DATA, {"reqId": req_id}, h2, "session")
        b2 = _json(r2)
        auth = wire.session_auth(b2) if r2.status_code == 200 else ""
        if not auth:
            if r2.status_code == wire.HTTP_REQUEST_ID_EXPIRED:
                raise NuvamaAuthError(
                    "Nuvama: request id expired (HTTP 222): log in again and paste the new URL",
                    "809",
                    r2.status_code,
                )
            why = wire.error_text(b2) or "the request id has expired or was already used; log in again"
            if not app_id_key:
                why += (
                    " -- note: Nuvama's vendor login issued no AppIdKey header, and the session step requires one"
                    " (API Connect spec loginData)"
                )
            raise NuvamaAuthError(
                f"Nuvama refused the request id (HTTP {r2.status_code}): {why}", "809", r2.status_code
            )
        app_id_key = (r2.headers.get("AppIdKey") or "").strip() or app_id_key
    return {KEY_TOKEN: Session(auth, vt, app_id_key).pack()}
