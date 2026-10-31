"""Unregistered Paytm Money entry; primary-source evidence in fixture README."""
from gammad.broker.capabilities import NEED_REQUIRED, BrokerEntry, Capabilities, EnvKey
from gammad.broker.paytm import auth, wire
from gammad.broker.paytm.adapter import PaytmAdapter
from gammad.broker.paytm.feed import PaytmFeed

CAPS = Capabilities(
    broker="paytm", status="beta-unverified", chain="native", expiries="native",
    candle_oi=False, candle_volume=False, daily_history=False, intraday_history=False,
    feed="ws", feed_oi="native", feed_volume="native", feed_ltt=True,
    poll_s=None, max_feed_instruments=500, token=("paste",),
    token_expiry_rule="Generated access token valid until midnight; renew both pasted tokens daily",
    data_cost="UNSOURCED", evidence=("PM55-1", "PM55-2", "PM55-3", "PM55-4", "PM55-U1", "PM55-U3"),
    ltp_batch=wire.BATCH, chain_calls=None, daily_quota=None,
)
ENV = (
    EnvKey(auth.KEY_TOKEN, NEED_REQUIRED, secret=True, written=True, note="Paste read_access_token for REST"),
    EnvKey(auth.KEY_PUBLIC, NEED_REQUIRED, secret=True, written=True, note="Paste public_access_token for WS"),
)
LOGIN_KEYS = ()


def make_adapter(creds, data_dir, *, transport=None, clock=None, sleep=None):
    return PaytmAdapter(creds, data_dir, transport=transport, clock=clock, sleep=sleep)


def make_feed(creds, out, on_auth_error=None, *, adapter=None, connect=None, clock_ms=None):
    return PaytmFeed(creds, out, on_auth_error, connect=connect, clock_ms=clock_ms,
                     resolve=adapter.resolve if adapter is not None else None)


ENTRY = BrokerEntry(name="paytm", label="Paytm Money", caps=CAPS, env_keys=ENV,
                    token_key=auth.KEY_TOKEN, make_adapter=make_adapter, make_feed=make_feed,
                    doc_url=wire.DOC_URL)
