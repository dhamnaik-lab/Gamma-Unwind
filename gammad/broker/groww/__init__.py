"""Groww registry ENTRY (BROKER-ADAPTER-SPEC §4, §8; research G1-G9; D81, D86, D92, D95(5)). Status BETA.

Groww's live feed carries LTP + timestamp only (G1), so the scanner runs on the broker-neutral PollFeed
(gammad/watch/pollfeed.py): every subscribed contract AND underlying is quoted every 60 s (T-083: underlyings via
quotes() for VWAP volume) -> assess() = DEGRADED "OI/volume polled every 60 s". The feed reuses the adapter so it
shares the adapter's Live Data rate bucket (wire.py). Feed auth: GrowwAuthError code "809" (HTTP 401) -> 809 TOKEN
alert + park; "806" (HTTP 403, not subscribed) -> 806 log + park; any other code -> 809 (feed_protocol.auth_code_for).

Logins: paste the access token (both editions); client_login = today's authenticator code typed on the Token page
(D92 (c)); owner-only auto_login from GROWW_TOTP_SECRET (D86). This module imports no gammad.auth.* code.
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
from gammad.broker.groww import auth, wire
from gammad.broker.groww.adapter import KEY_TOKEN, GrowwAdapter, GrowwAuthError
from gammad.watch.feed_protocol import AuthErrorCallback, Tick
from gammad.watch.pollfeed import PollFeed

BROKER = "groww"
QUOTE_BATCH = 25  # ids per quotes() call from PollFeed (the adapter still sends one HTTP call per id)

CAPS = Capabilities(
    broker=BROKER,
    status="beta",
    chain="native",
    expiries="native",
    candle_oi=True,
    candle_volume=True,
    daily_history=True,
    feed="poll",
    feed_oi="poll",
    feed_volume="poll",
    feed_ltt=True,
    poll_s=wire.POLL_S,
    max_feed_instruments=wire.MAX_FEED_INSTRUMENTS,  # = make_feed's PollFeed max_instruments (below)
    ltp_batch=wire.LTP_MAX_IDS,  # adapter.ltp batches
    chain_calls="1",  # one native option-chain request per chain
    daily_quota=None,  # per-second / per-minute limits only (wire G6): daily UNKNOWN
    token=("paste", "totp"),
    token_expiry_rule="not stated in Groww's docs (daily login assumed)",
    data_cost="Rs 499/mo Trade API",
    evidence=("G1", "G2", "G3", "G4", "G5", "G6", "G7", "G9", "D81", "D86", "D89", "D92", "D95"),
)

ENV = (
    EnvKey(auth.KEY_API_KEY, NEED_OPTIONAL, secret=True,
           note="Groww Cloud 'TOTP token' (API key); needed to log in with today's authenticator code"),
    EnvKey(KEY_TOKEN, NEED_REQUIRED, secret=True, written=True,
           note="access token (paste it, or log in with the authenticator code on the Broker login page)"),
    EnvKey(auth.KEY_TOTP_SECRET, NEED_AUTO_LOGIN, secret=True, note="owner only: TOTP secret for automated login"),
)

# T-112 need=login (registry.NEED_LOGIN): the service starts without these; Check Setup requires them while no
# token is set, because the Token-page login cannot run without them.
LOGIN_KEYS = (auth.KEY_API_KEY,)


def make_adapter(
    creds: Mapping[str, str],
    data_dir: Path,
    *,
    transport: httpx.BaseTransport | None = None,
    clock: Callable[[], float] | None = None,
    sleep: Callable[[float], None] | None = None,
) -> GrowwAdapter:
    token = (creds.get(KEY_TOKEN) or "").strip()
    if not token:
        raise GrowwAuthError(f"{KEY_TOKEN} not set (.env or the Broker login page)")
    kw: dict[str, Any] = {}
    if transport is not None:
        kw["http"] = httpx.Client(base_url=wire.API_BASE_URL, timeout=30.0, transport=transport)
    if clock is not None:
        kw["clock"] = clock
    if sleep is not None:
        kw["sleep"] = sleep
    return GrowwAdapter(token, Path(data_dir), **kw)


def make_feed(
    creds: Mapping[str, str],
    out: asyncio.Queue[Tick],
    on_auth_error: AuthErrorCallback | None = None,
    *,
    adapter: GrowwAdapter | None = None,
    connect: object | None = None,
    clock_ms: Callable[[], int] | None = None,
) -> PollFeed:
    """A PollFeed over `adapter` (required: the feed must share the adapter's rate buckets)."""
    del connect  # no WebSocket: Groww's stream is LTP-only (G1)
    if adapter is None:
        raise ValueError("groww: make_feed needs adapter= (the poll feed shares the adapter's rate buckets)")
    adapter.set_credentials(creds)
    kw: dict[str, Any] = {}
    if clock_ms is not None:
        kw["clock_ms"] = clock_ms
    return PollFeed(
        adapter,
        out,
        poll_s=wire.POLL_S,
        batch_size=QUOTE_BATCH,
        on_auth_error=on_auth_error,
        max_instruments=wire.MAX_FEED_INSTRUMENTS,
        ltp_batch_size=wire.LTP_MAX_IDS,
        **kw,
    )


ENTRY = BrokerEntry(
    name=BROKER,
    label="Groww",
    caps=CAPS,
    env_keys=ENV,
    token_key=KEY_TOKEN,
    make_adapter=make_adapter,
    make_feed=make_feed,
    doc_url=wire.DOC_URL,
    auto_login=auth.auto_login,
    oauth=None,
    client_login=ClientLogin(fields=(auth.FIELD_TOTP,), login=auth.client_login, login_url=auth.login_url),
)
