"""Nuvama registry ENTRY (BROKER-ADAPTER-SPEC §4, §8; research NV1-NV2, D-12; D81, D86, D87, D89, D92, D95(5)).
Status: BETA-UNVERIFIED -- every wire fact not MEASURED is PROVISIONAL (list: gammad/broker/nuvama/wire.py).

Feed = the broker-neutral PollFeed: every subscribed contract AND underlying is quoted every wire.POLL_S (75 s; one
HTTP call per id at 3/s, wire P10) -> assess() = DEGRADED "OI/volume polled every 75 s". The feed reuses the adapter
so it shares the adapter's rate bucket AND the daily request budget (budget.py). Why still no native stream: the
SPEC (wire.py) now documents the stream FIELDS (#reduced-quotes-streamer: ltp, vol, oI, ltt; host
ncst.nuvamawealth.com:9443) but not the transport, framing, subscribe message or socket auth -- a decoder must not
be built from field tables alone (research brief §4/§6 item 9: read the SDK's feed/ sources first).
Static IP (SPEC #changelog 2.0.11 "Static IP Mandatory"): NUVAMA_STATIC_IP is REQUIRED -- the registry refuses to
start without it and make_adapter / client_login refuse with auth.STATIC_IP_NEED before any call.
Feed auth codes: NuvamaAuthError code "807" (HTTP 401 "Session Expired") -> 807 TOKEN alert + park; "809" (403 or
a malformed session) -> 809; anything else -> 809 (feed_protocol.auth_code_for).

Login (auth.py): the user pastes the redirect URL after a MANUAL Nuvama login -> one-shot exchange with the API key +
secret from .env (ClientLogin fields ("redirect_url",), both editions, D92(b)). No automated login, no OAuth hooks
(the evidence documents no `state`/redirect_uri, auth.py docstring). This module imports no gammad.auth.* code.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

import httpx

from gammad.broker.capabilities import (
    NEED_REQUIRED,
    BrokerEntry,
    Capabilities,
    ClientLogin,
    EnvKey,
)
from gammad.broker.nuvama import auth, wire
from gammad.broker.nuvama.adapter import NuvamaAdapter
from gammad.broker.nuvama.auth import KEY_API_KEY, KEY_API_SECRET, KEY_STATIC_IP, KEY_TOKEN, NuvamaAuthError
from gammad.watch.feed_protocol import AuthErrorCallback, Tick
from gammad.watch.pollfeed import PollFeed

BROKER = "nuvama"

CAPS = Capabilities(
    broker=BROKER,
    status="beta-unverified",
    chain="master+quotes",
    expiries="master",
    candle_oi=False,  # the chart carries no OI field (wire P5); spec §4 "unverified" -> declared False
    candle_volume=True,
    daily_history=True,
    feed="poll",
    feed_oi="poll",
    feed_volume="poll",
    feed_ltt=True,  # quote `ltt` is documented (format PROVISIONAL, wire P3); unparseable -> receive time
    poll_s=wire.POLL_S,
    max_feed_instruments=wire.MAX_FEED_INSTRUMENTS,  # = make_feed's PollFeed max_instruments (below)
    ltp_batch=wire.LTP_BATCH,  # one quote per id (ltp: cached ids cost 0, 0.8.5 D3)
    chain_calls="per_leg",  # option_chain = master rows + one quote per leg
    daily_quota=wire.DAILY_REQUEST_CAP,  # SPEC 86,400/day; budget.py stages (wire P10)
    token=("paste",),
    token_expiry_rule="about 12 h (API Connect FAQ) vs 8 h idle / 00:30 (partner docs) -- disputed (D-12)",
    data_cost="free in year 1 (promotional, R7 N12)",
    evidence=("NV1", "NV2", "D-12", "D81", "D86", "D87", "D89", "D92", "D95"),
)

ENV = (
    EnvKey(KEY_API_KEY, NEED_REQUIRED, secret=False, note="API key from Nuvama API Connect (a public app id: it rides in the login page URL, like Kite api_key)"),
    EnvKey(KEY_API_SECRET, NEED_REQUIRED, secret=True, note="API secret from Nuvama; used once per login"),
    EnvKey(
        KEY_TOKEN,
        NEED_REQUIRED,
        secret=True,
        written=True,
        note="session, written by the Broker login page after you paste the redirect URL (not typed by hand)",
    ),
    EnvKey(
        KEY_STATIC_IP,
        NEED_REQUIRED,
        secret=False,
        note=(
            "the static public IP you registered with Nuvama (mandatory since API Connect 2.0.11): run the app "
            "on that connection, Nuvama refuses calls from any other IP"
        ),
    ),
)


def make_adapter(
    creds: Mapping[str, str],
    data_dir: Path,
    *,
    transport: httpx.BaseTransport | None = None,
    clock: Callable[[], float] | None = None,
    sleep: Callable[[float], None] | None = None,
) -> NuvamaAdapter:
    api_key = (creds.get(KEY_API_KEY) or "").strip()
    token = (creds.get(KEY_TOKEN) or "").strip()
    if not api_key or not token:
        raise NuvamaAuthError(
            f"{KEY_API_KEY} / {KEY_TOKEN} not set (.env, or paste the redirect URL on the Broker login page)"
        )
    if not (creds.get(KEY_STATIC_IP) or "").strip():
        raise NuvamaAuthError(auth.STATIC_IP_NEED)
    kw: dict[str, Any] = {}
    if transport is not None:
        kw["http"] = httpx.Client(base_url=wire.HOST, timeout=30.0, transport=transport)
    if clock is not None:
        kw["clock"] = clock
    if sleep is not None:
        kw["sleep"] = sleep
    return NuvamaAdapter(
        api_key, auth.unpack(token), Path(data_dir), static_ip=(creds.get(KEY_STATIC_IP) or "").strip(), **kw
    )


def make_feed(
    creds: Mapping[str, str],
    out: asyncio.Queue[Tick],
    on_auth_error: AuthErrorCallback | None = None,
    *,
    adapter: NuvamaAdapter | None = None,
    connect: object | None = None,
    clock_ms: Callable[[], int] | None = None,
) -> PollFeed:
    """A PollFeed over `adapter` (required: the feed must share the adapter's rate bucket)."""
    del connect  # no native stream: the SPEC documents its fields, not its protocol (module docstring)
    if adapter is None:
        raise ValueError("nuvama: make_feed needs adapter= (the poll feed shares the adapter's rate bucket)")
    adapter.set_credentials(creds)
    kw: dict[str, Any] = {}
    if clock_ms is not None:
        kw["clock_ms"] = clock_ms
    return PollFeed(
        adapter,
        out,
        poll_s=wire.POLL_S,
        batch_size=wire.QUOTE_BATCH,
        on_auth_error=on_auth_error,
        max_instruments=wire.MAX_FEED_INSTRUMENTS,
        **kw,
    )


ENTRY = BrokerEntry(
    name=BROKER,
    label="Nuvama",
    caps=CAPS,
    env_keys=ENV,
    token_key=KEY_TOKEN,
    make_adapter=make_adapter,
    make_feed=make_feed,
    doc_url=wire.DOC_URL,
    auto_login=None,
    oauth=None,
    client_login=ClientLogin(
        fields=(auth.FIELD_REDIRECT,), login=auth.client_login, login_url=auth.login_url
    ),
)
