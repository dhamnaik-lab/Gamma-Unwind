"""Zerodha (Kite Connect v3) registry ENTRY -- BETA, not verified live (BROKER-ADAPTER-SPEC §4 row "Zerodha").

Capabilities (evidence K1-K12, docs/research/BROKER-API-RESEARCH.md §2B): chain master+quotes (no chain endpoint,
/quote 500 per call at 1/s), expiries from the canonical master, candles with OI (`oi=1`), WebSocket `full`
mode with native OI / volume / last trade time, token expires 06:00 IST next day, data Rs 500/mo.
Client edition: paste the redirect URL / request_token after a MANUAL login (ClientLogin, D89(1)/D95(5)) or
paste a ready access token; owner edition adds the redirect-server OAuth hooks. This module imports no
gammad.auth.* code (D86); auth.py imports gammad.auth.oauth only inside the owner-only functions.
Env keys confirmed against https://kite.trade/docs/connect/v3/user/ (api_key, api_secret, access_token).
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
from gammad.broker.zerodha import auth, wire
from gammad.broker.zerodha.adapter import KEY_API_KEY, KEY_API_SECRET, KEY_TOKEN, KiteAdapter, KiteAuthError
from gammad.broker.zerodha.feed import KiteFeed, make_resolver
from gammad.watch.feed_protocol import AuthErrorCallback, Connector, Tick

BROKER = "zerodha"

CAPS = Capabilities(
    broker=BROKER,
    status="beta",
    chain="master+quotes",
    expiries="master",
    candle_oi=True,
    candle_volume=True,
    daily_history=True,
    feed="ws",
    feed_oi="native",
    feed_volume="native",
    feed_ltt=True,
    poll_s=None,
    max_feed_instruments=KiteFeed.caps.max_instruments,  # 0.8.5 D14: the feed's own cap (WS_MAX_PER_CONNECTION)
    ltp_batch=wire.LTP_MAX_IDS,  # adapter.ltp_batch_size's first try
    chain_calls="per_batch",  # option_chain = master rows + /quote in QUOTE_MAX_IDS batches
    chain_batch=wire.QUOTE_MAX_IDS,
    daily_quota=None,  # none documented in the repo
    token=("oauth", "paste"),
    token_expiry_rule="06:00 IST the next day (Kite rule; no per-token expiry is served)",
    data_cost="Rs 500/mo (Kite Connect)",
    evidence=("K1", "K2", "K3", "K4", "K5", "K5m", "K6", "K7", "K8", "K9", "K10", "K11", "K12"),
)

ENV = (
    EnvKey(KEY_API_KEY, NEED_REQUIRED, secret=False, note="Kite Connect app api_key (developers.kite.trade)"),
    EnvKey(
        KEY_API_SECRET,
        NEED_OPTIONAL,
        secret=True,
        note="app api_secret: only to exchange the pasted redirect URL / request_token",
    ),
    EnvKey(
        KEY_TOKEN,
        NEED_REQUIRED,
        secret=True,
        written=True,
        note="access token (paste, or paste the redirect URL on the Broker login page); expires 06:00 IST",
    ),
)

# T-112 need=login (registry.NEED_LOGIN): the service starts without these; Check Setup requires them while no
# token is set, because the Token-page login cannot run without them.
LOGIN_KEYS = (KEY_API_SECRET,)


def make_adapter(
    creds: Mapping[str, str],
    data_dir: Path,
    *,
    transport: httpx.BaseTransport | None = None,
    clock: Callable[[], float] | None = None,
    sleep: Callable[[float], None] | None = None,
) -> KiteAdapter:
    api_key = (creds.get(KEY_API_KEY) or "").strip()
    token = (creds.get(KEY_TOKEN) or "").strip()
    if not api_key or not token:
        raise KiteAuthError(f"{KEY_API_KEY} / {KEY_TOKEN} not set (.env)")
    kw: dict[str, Any] = {}
    if transport is not None:
        kw["http"] = httpx.Client(base_url=wire.API_BASE_URL, timeout=30.0, transport=transport)
    if clock is not None:
        kw["clock"] = clock
    if sleep is not None:
        kw["sleep"] = sleep
    return KiteAdapter(api_key, token, Path(data_dir), **kw)


def make_feed(
    creds: Mapping[str, str],
    out: asyncio.Queue[Tick],
    on_auth_error: AuthErrorCallback | None = None,
    *,
    adapter: object | None = None,
    connect: Connector | None = None,
    clock_ms: Callable[[], int] | None = None,
) -> KiteFeed:
    """Pass the live `adapter` so subscriptions go through its identity-guarded instrument map (spec §1)."""
    return KiteFeed(
        (creds.get(KEY_API_KEY) or "").strip(),
        (creds.get(KEY_TOKEN) or "").strip(),
        out,
        on_auth_error=on_auth_error,
        connect=connect,
        clock_ms=clock_ms,
        resolve=make_resolver(adapter),
    )


ENTRY = BrokerEntry(
    name=BROKER,
    label="Zerodha (Kite)",
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
