"""Unregistered wave-1 Breeze entry. Evidence: tests/fixtures/icici/README.md."""

import time

from gammad.broker.capabilities import BrokerEntry, Capabilities, EnvKey
from gammad.broker.icici import wire
from gammad.broker.icici.auth import LOGIN_KEYS as LOGIN_KEYS
from gammad.broker.icici.auth import validate

CAPS = Capabilities(
    broker="icici",
    status="beta-unverified",
    chain="native",
    expiries="master",
    candle_oi=True,
    candle_volume=True,
    daily_history=True,
    feed="ws",
    feed_oi="native",
    feed_volume="native",
    feed_ltt=True,
    poll_s=None,
    max_feed_instruments=wire.MAX_FEED,
    token=("paste",),
    token_expiry_rule="Not documented; rejected sessions require a fresh manual paste",
    data_cost="UNSOURCED in inspected API references; verify current broker terms",
    evidence=(
        "IC53-A1",
        "IC53-A2",
        "IC53-A3",
        "IC53-F1",
        "IC53-F2",
        "IC53-H1",
        "IC53-M1",
        "IC53-C1",
        "IC53-Q1",
    ),
    ltp_batch=1,
    daily_quota=5000,
)
ENV = (EnvKey("ICICI_API_KEY"), EnvKey("ICICI_API_SECRET"), EnvKey("ICICI_ACCESS_TOKEN", written=True))


def make_adapter(creds, data_dir, *, transport=None, clock=None, sleep=None):
    from gammad.broker.icici.adapter import IciciAdapter

    return IciciAdapter(
        validate(creds),
        data_dir,
        transport=transport,
        clock=clock or time.monotonic,
        sleep=sleep or time.sleep,
    )


def make_feed(creds, out, on_auth_error, *, adapter=None, connect=None, clock_ms=None):
    from gammad.broker.icici.feed import IciciFeed

    return IciciFeed(
        validate(creds),
        out,
        on_auth_error=on_auth_error,
        connect=connect,
        clock_ms=clock_ms,
        resolve=adapter.native_id if adapter is not None else None,
    )


ENTRY = BrokerEntry(
    "icici",
    "ICICI Direct (Breeze)",
    CAPS,
    ENV,
    "ICICI_ACCESS_TOKEN",
    make_adapter,
    make_feed,
    "https://api.icicidirect.com/breezeapi/documents/index.html",
)
