"""Upstox registry ENTRY -- BETA, not verified live (BROKER-ADAPTER-SPEC §4 row "Upstox").

Capabilities (evidence U1-U12, docs/research/BROKER-API-RESEARCH.md §2B): native chain (/option/chain, U5) and
expiries (/option/contract, U6), candles with OI (last element, U3), WebSocket v3 protobuf with native OI /
volume / `ltt` (U1, U2), daily token until 03:30 IST next day (U8) or a 1-year Analytics token (U9), data free.
Client edition: paste a token (daily or Analytics, D86) or paste the redirect URL / code after a MANUAL login
(ClientLogin, D89(1)/D95(5)); owner edition adds the redirect-server OAuth hooks. This module imports no
gammad.auth.* code (D86); auth.py imports gammad.auth.oauth only inside the owner-only functions.
Env keys confirmed against https://upstox.com/developer/api-documentation/get-token/ (client_id = API key,
client_secret = API secret, redirect_uri).
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

import httpx

from gammad.broker.capabilities import (
    NEED_OPTIONAL,
    NEED_REQUIRED,
    BrokerEntry,
    Capabilities,
    ClientLogin,
    EnvKey,
    OAuthHooks,
)
from gammad.broker.upstox import auth, wire
from gammad.broker.upstox.adapter import (
    KEY_API_KEY,
    KEY_API_SECRET,
    KEY_REDIRECT_URI,
    KEY_TOKEN,
    UpstoxAdapter,
    UpstoxAuthError,
)
from gammad.broker.upstox.feed import UpstoxFeed, make_resolver
from gammad.watch.feed_protocol import AuthErrorCallback, Connector, Tick

BROKER = "upstox"

CAPS = Capabilities(
    broker=BROKER,
    status="beta",
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
    max_feed_instruments=UpstoxFeed.caps.max_instruments,  # 0.8.5 D14: the feed's own cap (wire.WS_MAX_INSTRUMENTS)
    ltp_batch=wire.QUOTE_MAX_KEYS,  # adapter._market_quote batches
    chain_calls="1",  # one /v2/option/chain request per chain
    daily_quota=None,  # 2,000 per 30 min (wire.py), no daily figure documented
    token=("oauth", "paste"),
    token_expiry_rule="03:30 IST the next day (daily token); 1 year (Analytics token)",
    data_cost="free",
    evidence=("U1", "U2", "U3", "U4", "U4m", "U5", "U6", "U7", "U8", "U9", "U10", "U12"),
)

ENV = (
    EnvKey(
        KEY_API_KEY,
        NEED_OPTIONAL,
        secret=False,
        note="app API key (client_id): only for the redirect-URL login",
    ),
    EnvKey(
        KEY_API_SECRET, NEED_OPTIONAL, secret=True, note="app API secret: only for the redirect-URL login"
    ),
    EnvKey(
        KEY_REDIRECT_URI,
        NEED_OPTIONAL,
        secret=False,
        note="the app's registered redirect URL: only when pasting a bare code",
    ),
    EnvKey(
        KEY_TOKEN,
        NEED_REQUIRED,
        secret=True,
        written=True,
        note="daily access token (until 03:30 IST) OR your 1-year Analytics token",
    ),
)

# T-112 need=login (registry.NEED_LOGIN): the service starts without these; Check Setup requires them while no
# token is set, because the Token-page login cannot run without them.
LOGIN_KEYS = (KEY_API_KEY, KEY_API_SECRET)


def make_adapter(
    creds: Mapping[str, str],
    data_dir: Path,
    *,
    transport: httpx.BaseTransport | None = None,
    clock: Callable[[], float] | None = None,
    sleep: Callable[[float], None] | None = None,
) -> UpstoxAdapter:
    token = (creds.get(KEY_TOKEN) or "").strip()
    if not token:
        raise UpstoxAuthError(f"{KEY_TOKEN} not set (.env)")
    kw: dict[str, Any] = {}
    if transport is not None:
        kw["http"] = httpx.Client(base_url=wire.API_BASE_URL, timeout=30.0, transport=transport)
    if clock is not None:
        kw["clock"] = clock
    if sleep is not None:
        kw["sleep"] = sleep
    return UpstoxAdapter(token, Path(data_dir), **kw)


def make_feed(
    creds: Mapping[str, str],
    out: asyncio.Queue[Tick],
    on_auth_error: AuthErrorCallback | None = None,
    *,
    adapter: object | None = None,
    connect: Connector | None = None,
    clock_ms: Callable[[], int] | None = None,
) -> UpstoxFeed:
    """Pass the live `adapter`: equities (ISIN keys) and indices resolve only through its identity-guarded map."""
    return UpstoxFeed(
        (creds.get(KEY_TOKEN) or "").strip(),
        out,
        on_auth_error=on_auth_error,
        connect=connect,
        clock_ms=clock_ms,
        resolve=make_resolver(adapter),
    )


ENTRY = BrokerEntry(
    name=BROKER,
    label="Upstox",
    caps=CAPS,
    env_keys=ENV,
    token_key=KEY_TOKEN,
    make_adapter=make_adapter,
    make_feed=make_feed,
    doc_url=wire.DOC_URL,
    auto_login=None,
    oauth=OAuthHooks(login_url=auth.oauth_login_url, exchange=auth.oauth_exchange),
    client_login=ClientLogin(fields=(auth.FIELD,), login=auth.client_login, login_url=auth.client_login_url),
)
