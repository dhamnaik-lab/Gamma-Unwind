"""Fyers registry ENTRY (BROKER-ADAPTER-SPEC §4, §8; research F1-F9, D-6; D81, D86, D89, D92, D95(5)). Status BETA.

Capabilities: native options-chain-v3 (+ its expiryData), candles with `oi_flag` (F4), and the HSM data socket in
full mode carrying LTP, cumulative volume, last-trade time and OI (D-6 settled from the SDK wire decoder, see
wire.py) -> assess() = FULL. If the live smoke run (spec §10) shows the socket's OI empty, the declared fallback is
feed "poll" -- but Fyers quotes carry no OI, so that change is a spec decision, not a flag flip.

Logins: client_login = paste the redirect URL after a MANUAL browser login (D89(1)); owner: OAuth redirect hooks
and the optional 15-day PIN refresh (F8). This module imports no gammad.auth.* code.
Feed auth codes: 807 expired JWT / HTTP 401 / -8; 809 invalid token / socket auth refused / -15 -16 -17;
806 HTTP 403 (the app lacks the Market Data permission).
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

import httpx

from gammad.broker.capabilities import (
    NEED_AUTO_LOGIN,
    NEED_OPTIONAL,
    NEED_REQUIRED,
    BrokerEntry,
    Capabilities,
    ClientLogin,
    EnvKey,
    OAuthHooks,
)
from gammad.broker.fyers import auth, wire
from gammad.broker.fyers.adapter import KEY_APP_ID, KEY_TOKEN, FyersAdapter, FyersAuthError
from gammad.broker.fyers.feed import FyersFeed
from gammad.watch.feed_protocol import AuthErrorCallback, Connector, Tick

BROKER = "fyers"

DAILY_REQUEST_QUOTA = 4_000  # conservative FYERS Standard data budget; persisted by limits.py

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
    max_feed_instruments=FyersFeed.caps.max_instruments,  # 0.8.5 D14: the feed's own cap
    ltp_batch=wire.QUOTES_MAX,  # adapter.ltp_batch_size's first try
    chain_calls="1",  # one options-chain-v3 request per chain
    daily_quota=DAILY_REQUEST_QUOTA,
    token=("oauth", "paste"),
    token_expiry_rule="daily (end of the trading day); optional 15-day refresh with the PIN (owner)",
    data_cost="free",
    evidence=("F1", "F2", "F3", "F4", "F5", "F6", "F7", "F8", "F9", "D-6", "D81", "D86", "D89", "D92", "D95"),
)

ENV = (
    # support.fyers.in "activate the new app for API trading after April 1, 2026" (read 2026-09-29): new algo apps
    # end in -200; an app made before 1 Apr 2026 works for data only.
    EnvKey(KEY_APP_ID, NEED_REQUIRED, secret=False,
           note="Fyers App ID from myapi.fyers.in (XXXXXXXXXX-200; an app made before 1 Apr 2026 works for data only)"),
    EnvKey(auth.KEY_SECRET, NEED_OPTIONAL, secret=True,
           note="Fyers app secret: needed to turn the pasted login URL into a token"),
    EnvKey(KEY_TOKEN, NEED_REQUIRED, secret=True, written=True,
           note="access token (paste it, or paste the redirect URL on the Broker login page); daily"),
    EnvKey(auth.KEY_REDIRECT, NEED_OPTIONAL, secret=False,
           note="https://trade.fyers.in/api-login/redirect-uri/index.html = the Redirect URL to register on your "
                "Fyers app (FYERS's own page; it lets the Broker login page open the login link)"),
    EnvKey(auth.KEY_REFRESH, NEED_AUTO_LOGIN, secret=True, written=True,
           note="owner only: 15-day refresh token (written by the OAuth login)"),
    EnvKey(auth.KEY_PIN, NEED_AUTO_LOGIN, secret=True, note="owner only: Fyers PIN for the 15-day refresh"),
)

# T-112 need=login (registry.NEED_LOGIN): the service starts without these; Check Setup requires them while no
# token is set, because the Token-page login cannot run without them.
LOGIN_KEYS = (auth.KEY_SECRET,)


def make_adapter(
    creds: Mapping[str, str],
    data_dir: Path,
    *,
    transport: httpx.BaseTransport | None = None,
    clock: Callable[[], float] | None = None,
    sleep: Callable[[float], None] | None = None,
) -> FyersAdapter:
    app_id = (creds.get(KEY_APP_ID) or "").strip()
    token = (creds.get(KEY_TOKEN) or "").strip()
    if not app_id or not token:
        raise FyersAuthError(f"{KEY_APP_ID} / {KEY_TOKEN} not set (.env or the Broker login page)")
    kw: dict[str, Any] = {}
    if transport is not None:
        kw["http"] = httpx.Client(timeout=30.0, transport=transport)
    if clock is not None:
        kw["clock"] = clock
    if sleep is not None:
        kw["sleep"] = sleep
    return FyersAdapter(app_id, token, Path(data_dir), **kw)


def make_feed(
    creds: Mapping[str, str],
    out: asyncio.Queue[Tick],
    on_auth_error: AuthErrorCallback | None = None,
    *,
    adapter: FyersAdapter | None = None,
    connect: Connector | None = None,
    clock_ms: Callable[[], int] | None = None,
) -> FyersFeed:
    """The HSM socket feed. With `adapter`, every topic passes the adapter's runtime identity guard first."""
    kw: dict[str, Any] = {}
    if connect is not None:
        kw["connect"] = connect
    if clock_ms is not None:
        kw["clock_ms"] = clock_ms
    if adapter is not None:
        kw["guard"] = adapter.topic
    return FyersFeed(
        (creds.get(KEY_APP_ID) or "").strip(),
        (creds.get(KEY_TOKEN) or "").strip(),
        out,
        on_auth_error=on_auth_error,
        **kw,
    )


ENTRY = BrokerEntry(
    name=BROKER,
    label="Fyers",
    caps=CAPS,
    env_keys=ENV,
    token_key=KEY_TOKEN,
    make_adapter=make_adapter,
    make_feed=make_feed,
    doc_url=wire.DOC_URL,
    auto_login=auth.refresh_with_pin,
    oauth=OAuthHooks(login_url=auth.oauth_login_url, exchange=auth.oauth_exchange),
    client_login=ClientLogin(fields=(auth.FIELD_REDIRECT,), login=auth.client_login,
                             login_url=auth.client_login_url),
)
