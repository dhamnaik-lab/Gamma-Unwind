"""Unregistered wave-1 entry; evidence: tests/fixtures/fivepaisa/README.md."""

import httpx

from gammad.broker.capabilities import BrokerEntry, Capabilities, EnvKey
from gammad.broker.fivepaisa import auth, wire
from gammad.broker.fivepaisa.adapter import FivePaisaAdapter
from gammad.broker.fivepaisa.feed import FivePaisaFeed

CAPS = Capabilities(
    broker="fivepaisa",
    status="beta-unverified",
    chain="native",
    expiries="native",
    candle_oi=False,
    candle_volume=True,
    daily_history=True,
    intraday_history=True,
    feed="ws",
    feed_oi="native",
    feed_volume="native",
    feed_ltt=False,
    poll_s=None,
    max_feed_instruments=wire.MAX_FEED,
    token=("paste",),
    token_expiry_rule="23:59 IST daily",
    data_cost="UNSOURCED; verify plan with 5paisa",
    evidence=("FP54-A", "FP54-W", "FP54-H", "FP54-M", "FP54-E", "FP54-C", "FP54-Q", "FP54-U"),
    ltp_batch=wire.QUOTE_BATCH,
    chain_calls="1",
    daily_quota=None,
)
ENV = (EnvKey(auth.KEY_TOKEN, written=True), EnvKey(auth.KEY_CLIENT), EnvKey(auth.KEY_APP))
LOGIN_KEYS = ()


def make_adapter(creds, data_dir, *, transport=None, clock=None, sleep=None):
    auth.credentials(creds)
    kw = {}
    if clock is not None:
        kw["clock"] = clock
    if sleep is not None:
        kw["sleep"] = sleep
    return FivePaisaAdapter(
        creds, data_dir, http=httpx.Client(base_url=wire.API_BASE, transport=transport, timeout=30), **kw
    )


def make_feed(creds, out, on_auth_error=None, *, adapter=None, connect=None, clock_ms=None):
    return FivePaisaFeed(
        creds,
        out,
        on_auth_error,
        resolve=adapter.native_id if adapter is not None else None,
        connect=connect,
        clock_ms=clock_ms,
    )


ENTRY = BrokerEntry("fivepaisa", "5paisa", CAPS, ENV, auth.KEY_TOKEN, make_adapter, make_feed, wire.DOC_URL)
