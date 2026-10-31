"""INDmoney wave-1 entry, deliberately unregistered. Evidence: fixtures/indmoney/README.md."""

from pathlib import Path

from gammad.broker.capabilities import BrokerEntry, Capabilities, EnvKey
from gammad.broker.indmoney import auth, wire
from gammad.broker.indmoney.adapter import IndmoneyAdapter
from gammad.broker.indmoney.feed import IndmoneyFeed

CAPS = Capabilities(
    broker="indmoney", status="beta-unverified", chain="native", expiries="native",
    candle_oi=False, candle_volume=False, daily_history=True, intraday_history=True,
    feed="ws", feed_oi="none", feed_volume="none", feed_ltt=False, poll_s=None,
    max_feed_instruments=wire.MAX_FEED, token=("paste",), token_expiry_rule="24 hours",
    data_cost="Free API access", evidence=("IN1", "IN2", "IN3", "IN4", "IN5", "IN6", "IN7", "IN8", "IN9",
                                          "U1", "U2", "U3", "U4", "U5"),
    ltp_batch=wire.QUOTE_BATCH, chain_calls="1", daily_quota=wire.DAILY_QUOTA,
)
ENV = (EnvKey(auth.KEY_TOKEN, written=True, note="Paste a dashboard-generated INDstocks access token"),)
LOGIN_KEYS = ()


def make_adapter(creds, data_dir, *, transport=None, clock=None, sleep=None):
    return IndmoneyAdapter(creds, Path(data_dir), transport=transport, clock=clock, sleep=sleep)


def make_feed(creds, out, on_auth_error=None, *, adapter=None, connect=None, clock_ms=None):
    return IndmoneyFeed(creds, out, on_auth_error, connect=connect, clock_ms=clock_ms,
                        resolve=adapter.feed_id if adapter is not None else None)


ENTRY = BrokerEntry(name="indmoney", label="INDmoney", caps=CAPS, env_keys=ENV, token_key=auth.KEY_TOKEN,
                    make_adapter=make_adapter, make_feed=make_feed, doc_url=wire.DOC_URL)
