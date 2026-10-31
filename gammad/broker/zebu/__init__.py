"""Zebu MYNT legacy session API. Production WS unavailable until its URL is sourced (U5)."""

from gammad.broker.noren import NorenHost, declaration, entry, environment

HOST = NorenHost(
    broker="zebu", rest_base="https://go.mynt.in/NorenWClientTP/", ws_url=None,
    master_urls=("https://go.mynt.in/NSE_symbols.txt.zip", "https://go.mynt.in/NFO_symbols.txt.zip"),
    login_style="paste-susertoken", env_prefix="ZEBU",
)
CAPS = declaration(HOST, expiry="UNSOURCED: session expiry time", cost="UNSOURCED")
ENV = environment(HOST)
LOGIN_KEYS = ()
ENTRY = entry(HOST, CAPS, ENV, "Zebu MYNT", "https://api.zebuetrade.com/Base/")
make_adapter, make_feed = ENTRY.make_adapter, ENTRY.make_feed
