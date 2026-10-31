"""Kotak Neo login (BROKER-ADAPTER-SPEC §5, D86, D92, D95(5)). Wire: docs/authentication.md (see wire.py).

Two fixed calls, both with `Authorization: <KOTAK_CONSUMER_KEY>` + `neo-fin-key: neotradeapi`:
  1. POST https://mis.kotaksecurities.com/login/1.0/tradeApiLogin   {mobileNumber, ucc, totp} -> view token + view sid
  2. POST https://mis.kotaksecurities.com/login/1.0/tradeApiValidate {mpin} + headers sid / Auth = the view pair
     -> session sid, baseUrl, dataCenter, maybe feedUrl (and a trade token that can place orders: DISCARDED, never
     returned or stored)
  3. the live-feed URL in kotakneoapi 3.0.7's order (wire.py FEED_URL_FALLBACK note): GET the config service keyed by
     dataCenter (no credentials sent; any failure falls through) -> data.feedUrl -> wss://sfeed.kotaksecurities.com/apifeed.
     One INFO line names the source.
Returns {KOTAK_ACCESS_TOKEN: session sid, KOTAK_BASE_URL, KOTAK_FEED_URL}.

  * `client_login(creds, fields)` -- BOTH editions: the user TYPES today's authenticator code + MPIN; used once,
    never stored, returned, logged or written. Needs KOTAK_CONSUMER_KEY, KOTAK_UCC and KOTAK_MOBILE in .env
    (step 1 requires the registered mobile number, so KOTAK_MOBILE is not owner-only -- builder report).
  * `auto_login(creds, *, transport, now)` -- OWNER EDITION ONLY: code from KOTAK_TOTP_SECRET via gammad.auth.totp
    (imported INSIDE the function), MPIN from KOTAK_MPIN. Reached only through registry.auto_login.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any
from urllib.parse import urlsplit

import httpx

from gammad.broker.kotak import wire as W
from gammad.broker.kotak.adapter import (
    KEY_BASE,
    KEY_CONSUMER,
    KEY_FEED_URL,
    KEY_MOBILE,
    KEY_TOKEN,
    KEY_UCC,
    KotakAuthError,
)
from gammad.log import get_logger

log = get_logger(__name__)

KEY_MPIN = "KOTAK_MPIN"
KEY_TOTP = "KOTAK_TOTP_SECRET"
_CODE_RE = re.compile(r"\d{6}")


def normalize_mobile(text: str) -> str:
    """'98xxxxxxxx' / '9198xxxxxxxx' / '+91 98xxx xxxxx' -> '+9198xxxxxxxx' (authentication.md: with ISD)."""
    digits = re.sub(r"\D", "", text or "")
    if len(digits) == 10:
        return "+91" + digits
    if len(digits) == 12 and digits.startswith("91"):
        return "+" + digits
    return ("+" + digits) if digits else ""


def _post(http: httpx.Client, path: str, body: dict[str, str], headers: dict[str, str], secrets: tuple[str, ...]
          ) -> dict[str, Any]:
    try:
        resp = http.post(path, json=body, headers=headers)
    except httpx.TransportError as exc:
        raise KotakAuthError(f"Kotak login: transport error {type(exc).__name__}") from exc
    try:
        parsed: Any = resp.json()
    except ValueError:
        parsed = None
    data = parsed.get("data") if isinstance(parsed, dict) else None
    ok = resp.status_code < 400 and isinstance(data, dict) and str(data.get("status") or "success") == "success"
    if not ok or not data.get("token") or not data.get("sid"):  # type: ignore[union-attr]
        code, msg = W.error_text(parsed, "no token / sid in the response")
        for s in secrets:
            if s:
                msg = msg.replace(s, "***")
        raise KotakAuthError(f"Kotak login rejected at {path} ({code or resp.status_code}): {msg}", code,
                             resp.status_code)
    return data  # type: ignore[return-value]


def login(
    consumer_key: str, ucc: str, mobile: str, code: str, mpin: str, *, transport: httpx.BaseTransport | None = None
) -> dict[str, str]:
    base = {"Authorization": consumer_key, "neo-fin-key": W.NEO_FIN_KEY, "Content-Type": "application/json"}
    secrets = (code, mpin)
    with httpx.Client(base_url=W.LOGIN_BASE_URL, timeout=30.0, transport=transport) as http:
        view = _post(http, W.EP_LOGIN, {"mobileNumber": mobile, "ucc": ucc, "totp": code}, base, secrets)
        sess = _post(http, W.EP_VALIDATE, {"mpin": mpin},
                     {**base, "sid": str(view["sid"]), "Auth": str(view["token"])}, secrets)
        base_url = str(sess.get("baseUrl") or "").strip()
        if not base_url:
            raise KotakAuthError("Kotak login: tradeApiValidate returned no baseUrl")
        feed_url = resolve_feed_url(http, sess)
    return {KEY_TOKEN: str(sess["sid"]).strip(), KEY_BASE: base_url, KEY_FEED_URL: feed_url}


def resolve_feed_url(http: httpx.Client, sess: Mapping[str, Any]) -> str:
    """kotakneoapi 3.0.7's order: config service by dataCenter -> data.feedUrl -> the SDK's sfeed URL. Never raises."""
    dc = str(sess.get("dataCenter") or "").strip()
    note = "no dataCenter"
    if dc:
        try:
            resp = http.get(W.CONFIG_SERVICE_URL, params=W.CONFIG_SERVICE_PARAMS,
                            headers={"Content-Type": "application/json"}, timeout=W.CONFIG_SERVICE_TIMEOUT_S)
            url = W.feed_url_from_config(resp.json(), dc) if resp.status_code < 400 else None
            note = "no entry for it" if resp.status_code < 400 else f"HTTP {resp.status_code}"
        except Exception as exc:  # noqa: BLE001 -- best effort, like the SDK: any failure falls through
            url, note = None, type(exc).__name__
        if url:
            _log_feed_source("the config service", dc, url)
            return url
    feed = str(sess.get("feedUrl") or "").strip()
    if feed:
        _log_feed_source(f"tradeApiValidate feedUrl (config service: {note})", dc, feed)
        return feed
    _log_feed_source(f"the SDK fallback (config service: {note}; no feedUrl)", dc, W.FEED_URL_FALLBACK)
    return W.FEED_URL_FALLBACK


def _log_feed_source(source: str, data_center: str, url: str) -> None:
    log.info("kotak login: live-feed URL from %s, data center %s, host %s", source, data_center or "-",
             urlsplit(url).hostname or "?")


def _ids(creds: Mapping[str, str]) -> tuple[str, str, str]:
    consumer = (creds.get(KEY_CONSUMER) or "").strip()
    ucc = (creds.get(KEY_UCC) or "").strip()
    mobile = normalize_mobile(creds.get(KEY_MOBILE) or "")
    missing = [k for k, v in ((KEY_CONSUMER, consumer), (KEY_UCC, ucc), (KEY_MOBILE, mobile)) if not v]
    if missing:
        raise KotakAuthError(f"Kotak login needs {', '.join(missing)} in .env")
    return consumer, ucc, mobile


def client_login(
    creds: Mapping[str, str], fields: Mapping[str, str], *, transport: httpx.BaseTransport | None = None
) -> dict[str, str]:
    """D95(5): today's 6-digit authenticator code + MPIN, typed by the user -> new session keys (both editions)."""
    consumer, ucc, mobile = _ids(creds)
    code = "".join(str(fields.get("totp_code") or "").split())
    mpin = str(fields.get("pin") or "").strip()
    if not _CODE_RE.fullmatch(code):
        raise KotakAuthError("Kotak login: the authenticator code must be 6 digits")
    if not mpin:
        raise KotakAuthError("Kotak login: the MPIN is empty")
    return login(consumer, ucc, mobile, code, mpin, transport=transport)


def client_login_url(creds: Mapping[str, str]) -> str | None:
    del creds
    return None  # the code + MPIN are typed on the Token page; no browser step


def auto_login(
    creds: Mapping[str, str], *, transport: httpx.BaseTransport | None = None, now: float | None = None
) -> dict[str, str]:
    """OWNER EDITION ONLY (D86): KOTAK_TOTP_SECRET + KOTAK_MPIN from .env -> new session keys."""
    from gammad.edition import require_owner

    require_owner("Kotak Neo TOTP login")
    from gammad.auth.totp import totp  # owner-only module: imported here, never at module import

    consumer, ucc, mobile = _ids(creds)
    mpin = (creds.get(KEY_MPIN) or "").strip()
    secret = (creds.get(KEY_TOTP) or "").strip()
    missing = [k for k, v in ((KEY_MPIN, mpin), (KEY_TOTP, secret)) if not v]
    if missing:
        raise KotakAuthError(f"Kotak TOTP login needs {', '.join(missing)} in .env")
    try:
        code = totp(secret, at=now)
    except ValueError as exc:
        raise KotakAuthError(f"{KEY_TOTP} is not a valid base32 secret") from exc
    return login(consumer, ucc, mobile, code, mpin, transport=transport)
