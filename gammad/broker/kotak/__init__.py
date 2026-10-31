"""Kotak Neo (Trade API) registry ENTRY -- BETA, not verified live (BROKER-ADAPTER-SPEC §4, §6, §8; D81, D86).

Capabilities (evidence rows KN1-KN6, KN8 in docs/research/BROKER-API-RESEARCH.md; wire facts + doc cites in wire.py):
  chain native + expiries native (KN4); candle OI "not yet populated" (KN2) -> the app primes warm-up OI from
  quotes() (flag warmup_oi="quote"); feed ws with native OI, day volume and last_trade_time (KN1); 3000 tokens;
  token lifetime not stated (KN5); free (KN8).

.env (spec §6 names kept; confirmed against docs/authentication.md + troubleshooting.md Q3/Q8):
  KOTAK_CONSUMER_KEY  required  the "access token" from More > TradeAPI > API Dashboard (plain, no Bearer):
                                the Authorization header of login and of every market-data call
  KOTAK_UCC           required  unique client code
  KOTAK_ACCESS_TOKEN  required  the day's session sid (tradeApiValidate data.sid) -- the live feed's `auth`;
                                written by the Broker login page login or the owner's TOTP login
  KOTAK_BASE_URL      optional  per-account REST base (tradeApiValidate data.baseUrl), written by the login
  KOTAK_FEED_URL      optional  live-feed URL written by the login (config service by dataCenter, else data.feedUrl,
                                else the SDK's sfeed URL; auth.resolve_feed_url); empty -> the feed uses the sfeed URL
  KOTAK_MOBILE        required  registered mobile (+91...): the login's first step needs it in BOTH editions
  KOTAK_MPIN, KOTAK_TOTP_SECRET  owner only (auto_login), never in a client .env
Importing this module never loads gammad.auth.* (the harness checks it in a client-edition interpreter).
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
)
from gammad.broker.kotak import wire as W
from gammad.broker.kotak.adapter import (
    KEY_BASE,
    KEY_CONSUMER,
    KEY_FEED_URL,
    KEY_MOBILE,
    KEY_TOKEN,
    KEY_UCC,
    KotakAdapter,
    KotakAuthError,
)
from gammad.broker.kotak.auth import KEY_MPIN, KEY_TOTP, auto_login, client_login, client_login_url
from gammad.broker.kotak.feed import KotakFeed, adapter_resolver, default_resolver
from gammad.watch.feed_protocol import AuthErrorCallback, Connector, Tick

BROKER = "kotak"

CAPS = Capabilities(
    broker=BROKER,
    status="beta",
    chain="native",
    expiries="native",
    candle_oi=False,
    candle_volume=True,
    daily_history=True,
    feed="ws",
    feed_oi="native",
    feed_volume="native",
    feed_ltt=True,
    poll_s=None,
    max_feed_instruments=KotakFeed.caps.max_instruments,  # 0.8.5 D14: the feed's own cap (W.MAX_FEED_TOKENS)
    ltp_batch=W.QUOTE_MAX_IDS,  # adapter.ltp_batch_size's first try (measured 20, D125 K1)
    chain_calls="1",  # one native option-chain request per chain (option quotes are served from it)
    daily_quota=None,  # none documented in the repo
    token=("totp", "paste"),
    token_expiry_rule="not stated by Kotak (KN5); log in again each trading day",
    data_cost="free (KN8)",
    evidence=("KN1", "KN2", "KN3", "KN4", "KN5", "KN6", "KN8"),
)

ENV = (
    EnvKey(KEY_CONSUMER, NEED_REQUIRED, secret=True, note="access token from More > TradeAPI > API Dashboard (Kotak Neo)"),
    EnvKey(KEY_UCC, NEED_REQUIRED, secret=False, note="Kotak unique client code (UCC)"),
    EnvKey(KEY_TOKEN, NEED_REQUIRED, secret=True, written=True,
           note="the day's session id; the login on the Broker login page writes it"),
    EnvKey(KEY_BASE, NEED_OPTIONAL, secret=False, written=True, note="written by the login (per-account API base)"),
    EnvKey(KEY_FEED_URL, NEED_OPTIONAL, secret=False, written=True, note="written by the login (live-feed URL)"),
    # T-093: required -- auth._ids refuses every login (Token page and owner TOTP) without it
    EnvKey(KEY_MOBILE, NEED_REQUIRED, secret=True, note="registered mobile number (+91...), needed to log in"),
    EnvKey(KEY_MPIN, NEED_AUTO_LOGIN, secret=True, note="owner only: MPIN for automated TOTP login"),
    EnvKey(KEY_TOTP, NEED_AUTO_LOGIN, secret=True, note="owner only: TOTP secret for automated login"),
)


def make_adapter(
    creds: Mapping[str, str],
    data_dir: Path,
    *,
    transport: httpx.BaseTransport | None = None,
    clock: Callable[[], float] | None = None,
    sleep: Callable[[float], None] | None = None,
) -> KotakAdapter:
    consumer = (creds.get(KEY_CONSUMER) or "").strip()
    ucc = (creds.get(KEY_UCC) or "").strip()
    sid = (creds.get(KEY_TOKEN) or "").strip()
    base = (creds.get(KEY_BASE) or "").strip().rstrip("/")
    missing = [k for k, v in ((KEY_CONSUMER, consumer), (KEY_UCC, ucc), (KEY_TOKEN, sid)) if not v]
    if missing:
        raise KotakAuthError(f"{', '.join(missing)} not set (.env; the login on the Broker login page writes {KEY_TOKEN})")
    if not base:
        # T-101: KEY_BASE stays optional+written (a fresh client never types it); a session id pasted on its own
        # carries no per-account base, and Check Setup (scripts/smoke_broker.py --doctor) prints this line.
        raise KotakAuthError(f"{KEY_BASE} not set: a pasted session id alone is not enough -- run the login on the "
                             f"Broker login page (authenticator code + MPIN); it writes {KEY_TOKEN}, {KEY_BASE} and {KEY_FEED_URL}")
    kw: dict[str, Any] = {}
    if transport is not None:
        kw["http"] = httpx.Client(base_url=base, timeout=30.0, transport=transport)
    if clock is not None:
        kw["clock"] = clock
    if sleep is not None:
        kw["sleep"] = sleep
    return KotakAdapter(consumer, ucc, sid, base, Path(data_dir), feed_url=(creds.get(KEY_FEED_URL) or "").strip(),
                        **kw)


def make_feed(
    creds: Mapping[str, str],
    out: asyncio.Queue[Tick],
    on_auth_error: AuthErrorCallback | None = None,
    *,
    adapter: object | None = None,
    connect: Connector | None = None,
    clock_ms: Callable[[], int] | None = None,
) -> KotakFeed:
    kw: dict[str, Any] = {}
    if connect is not None:
        kw["connect"] = connect
    if clock_ms is not None:
        kw["clock_ms"] = clock_ms
    resolve = adapter_resolver(adapter) if adapter is not None and hasattr(adapter, "native") else default_resolver
    return KotakFeed(creds, out, on_auth_error=on_auth_error, resolve=resolve, **kw)


ENTRY = BrokerEntry(
    name=BROKER,
    label="Kotak Neo",
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
