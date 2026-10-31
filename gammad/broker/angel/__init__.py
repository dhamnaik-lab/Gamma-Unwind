"""Angel One (SmartAPI) registry ENTRY -- BETA, not verified live (BROKER-ADAPTER-SPEC §4, §6, §8; D81, D86).

Capabilities (evidence rows A1-A11, D-3 in docs/research/BROKER-API-RESEARCH.md):
  chain master+quotes, expiries master (no chain / expiry route, A6); candles carry NO OI (A4) -> the app primes
  warm-up OI from quotes() (flag warmup_oi="quote"); feed ws SmartStream SNAP_QUOTE with native OI, volume and
  exchange time (A1, A2); 1000 tokens per session (A3); token expiry 05:00 or 00:00 IST, disputed (D-3); free (A11).

.env (spec §6; names confirmed against smartapi-python's own parameters -- api_key / clientCode / jwtToken /
feedToken, smartConnect.py L276-298, smartWebSocketV2.py L54, L289-294):
  ANGEL_API_KEY       required   SmartAPI app key (smartapi.angelone.in > My apps), sent as X-PrivateKey
  ANGEL_CLIENT_CODE   required   Angel client code (login id)
  ANGEL_ACCESS_TOKEN  required   the day's JWT (written by the Token-page login or the owner's TOTP login)
  ANGEL_FEED_TOKEN    optional   the day's SmartStream feed token -- written by the same login; the live feed
                                 cannot connect without it (spec §6 table did not list it: see the builder report)
  ANGEL_PIN, ANGEL_TOTP_SECRET   owner only (auto_login), never in a client .env
Importing this module never loads gammad.auth.* (the harness checks it in a client-edition interpreter).
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

import httpx

from gammad.broker.angel import wire as W
from gammad.broker.angel.adapter import KEY_API, KEY_CLIENT, KEY_FEED, KEY_TOKEN, AngelAdapter, AngelAuthError
from gammad.broker.angel.auth import KEY_PIN, KEY_TOTP, auto_login, client_login, client_login_url
from gammad.broker.angel.feed import AngelFeed, adapter_resolver, default_resolver
from gammad.broker.capabilities import (
    NEED_AUTO_LOGIN,
    NEED_OPTIONAL,
    NEED_REQUIRED,
    BrokerEntry,
    Capabilities,
    ClientLogin,
    EnvKey,
)
from gammad.watch.feed_protocol import AuthErrorCallback, Connector, Tick

BROKER = "angel"

CAPS = Capabilities(
    broker=BROKER,
    status="beta",
    chain="master+quotes",
    expiries="master",
    candle_oi=False,
    candle_volume=True,
    daily_history=True,
    feed="ws",
    feed_oi="native",
    feed_volume="native",
    feed_ltt=True,
    poll_s=None,
    max_feed_instruments=AngelFeed.caps.max_instruments,  # 0.8.5 D14: the feed's own cap (W.MAX_FEED_TOKENS)
    ltp_batch=W.QUOTE_MAX_TOKENS,  # adapter._market batches
    chain_calls="per_batch",  # option_chain = master rows + FULL quotes in QUOTE_MAX_TOKENS batches
    chain_batch=W.QUOTE_MAX_TOKENS,
    daily_quota=None,  # none documented in the repo
    token=("totp", "paste"),
    token_expiry_rule="05:00 or 00:00 IST next day (two Angel sources disagree, D-3)",
    data_cost="free (A11)",
    evidence=("A1", "A2", "A3", "A4", "A5", "A6", "A7", "A8", "A9", "A10", "A11", "D-3"),
)

ENV = (
    EnvKey(KEY_API, NEED_REQUIRED, secret=True, note="SmartAPI app key (smartapi.angelone.in > My apps)"),
    EnvKey(KEY_CLIENT, NEED_REQUIRED, secret=False, note="Angel One client code (your login id)"),
    EnvKey(KEY_TOKEN, NEED_REQUIRED, secret=True, written=True,
           note="the day's access token (JWT); the login on the Broker login page writes it"),
    EnvKey(KEY_FEED, NEED_OPTIONAL, secret=True, written=True,
           note="the day's live-feed token; written by the same login, the feed needs it"),
    EnvKey(KEY_PIN, NEED_AUTO_LOGIN, secret=True, note="owner only: PIN for automated TOTP login"),
    EnvKey(KEY_TOTP, NEED_AUTO_LOGIN, secret=True, note="owner only: TOTP secret for automated login"),
)


def make_adapter(
    creds: Mapping[str, str],
    data_dir: Path,
    *,
    transport: httpx.BaseTransport | None = None,
    clock: Callable[[], float] | None = None,
    sleep: Callable[[float], None] | None = None,
) -> AngelAdapter:
    api_key = (creds.get(KEY_API) or "").strip()
    client_code = (creds.get(KEY_CLIENT) or "").strip()
    token = (creds.get(KEY_TOKEN) or "").strip()
    missing = [k for k, v in ((KEY_API, api_key), (KEY_CLIENT, client_code), (KEY_TOKEN, token)) if not v]
    if missing:
        raise AngelAuthError(f"{', '.join(missing)} not set (.env)")
    kw: dict[str, Any] = {}
    if transport is not None:
        kw["http"] = httpx.Client(base_url=W.API_BASE_URL, timeout=30.0, transport=transport)
    if clock is not None:
        kw["clock"] = clock
    if sleep is not None:
        kw["sleep"] = sleep
    return AngelAdapter(
        api_key, client_code, token, Path(data_dir), feed_token=(creds.get(KEY_FEED) or "").strip(), **kw
    )


def make_feed(
    creds: Mapping[str, str],
    out: asyncio.Queue[Tick],
    on_auth_error: AuthErrorCallback | None = None,
    *,
    adapter: object | None = None,
    connect: Connector | None = None,
    clock_ms: Callable[[], int] | None = None,
) -> AngelFeed:
    kw: dict[str, Any] = {}
    if connect is not None:
        kw["connect"] = connect
    if clock_ms is not None:
        kw["clock_ms"] = clock_ms
    resolve = adapter_resolver(adapter) if adapter is not None and hasattr(adapter, "native") else default_resolver
    return AngelFeed(creds, out, on_auth_error=on_auth_error, resolve=resolve, **kw)


ENTRY = BrokerEntry(
    name=BROKER,
    label="Angel One",
    caps=CAPS,
    env_keys=ENV,
    token_key=KEY_TOKEN,
    make_adapter=make_adapter,
    make_feed=make_feed,
    doc_url=W.DOC_URL,
    auto_login=auto_login,
    oauth=None,
    client_login=ClientLogin(fields=("totp_code", "pin"), login=client_login, login_url=client_login_url),
)
