"""Shared factories; broker modules contain configuration and declarations only."""

from functools import partial

from gammad.broker.capabilities import BrokerEntry, Capabilities, EnvKey
from gammad.broker.noren.wire import QUOTE_MAX_IDS, NorenHost


def runtime_reasons(host, data_dir=None):
    """registry.runtime_reasons hook: the volume switch-off of today (units.py, D726 fix round F1)."""
    from gammad.broker.noren import units

    return units.runtime_reasons(host.broker, data_dir)


def make_adapter(host, creds, data_dir, *, transport=None, clock=None, sleep=None):
    from gammad.broker.noren.adapter import NorenAdapter

    return NorenAdapter(host, creds, data_dir, transport=transport, clock=clock, sleep=sleep)


def make_feed(host, creds, out, on_auth_error=None, *, adapter=None, connect=None, clock_ms=None):
    from gammad.broker.noren.feed import NorenFeed

    return NorenFeed(host, creds, out, on_auth_error, adapter=adapter, connect=connect, clock_ms=clock_ms)


def declaration(host: NorenHost, *, expiry: str, cost: str):
    return Capabilities(
        broker=host.broker, status="beta-unverified", chain="master+quotes", expiries="master",
        candle_oi=True, candle_volume=False, daily_history=True, feed="ws",
        feed_oi="native" if host.feed_oi else "none",
        feed_volume="native" if host.volume_verified else "none", feed_ltt=False,
        poll_s=None, max_feed_instruments=host.max_feed_instruments, token=("paste",),
        token_expiry_rule=expiry, data_cost=cost,
        evidence=tuple(f"{host.broker}/N{i}" for i in range(1, 13)) + (f"{host.broker}/U1-U5",),
        ltp_batch=QUOTE_MAX_IDS, chain_calls="per_leg",
    )


def environment(host):
    return (EnvKey(f"{host.env_prefix}_USER_ID"),
            EnvKey(f"{host.env_prefix}_ACCESS_TOKEN", written=True, note="Paste session token; no auto-login"))


def entry(host, caps, env, label, doc_url):
    return BrokerEntry(host.broker, label, caps, env, f"{host.env_prefix}_ACCESS_TOKEN",
                       partial(make_adapter, host), partial(make_feed, host), doc_url)
