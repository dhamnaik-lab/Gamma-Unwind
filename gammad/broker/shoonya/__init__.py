"""Shoonya configuration; evidence in tests/fixtures/shoonya/README.md."""

from functools import partial

from gammad.broker.noren import NorenHost, declaration, entry, environment
from gammad.broker.noren import runtime_reasons as noren_runtime_reasons

HOST = NorenHost(
    broker="shoonya", rest_base="https://api.shoonya.com/NorenWClientTP/",
    ws_url="wss://api.shoonya.com/NorenWSTP/",
    master_urls=("https://api.shoonya.com/NSE_symbols.txt.zip", "https://api.shoonya.com/NFO_symbols.txt.zip"),
    login_style="paste-susertoken", env_prefix="SHOONYA", heartbeat_s=3,
    # D726 BETA: the documented touchline/GetQuotes `v` is admitted through wire.quantity_volume (U4 lot guard).
    volume_verified=True,
)
CAPS = declaration(HOST, expiry="UNSOURCED: session expiry time", cost="UNSOURCED")
ENV = environment(HOST)
LOGIN_KEYS = ()
ENTRY = entry(HOST, CAPS, ENV, "Shoonya", "https://github.com/Shoonya-Dev/ShoonyaApi-py")
make_adapter, make_feed = ENTRY.make_adapter, ENTRY.make_feed
runtime_reasons = partial(noren_runtime_reasons, HOST)  # registry.runtime_reasons: status + Check Setup
# registry.setup_notes -> Check Setup (integrator, D726): GetQuotes is one contract per call at the local 0.5 req/s
# (README N11), so a 128-leg chain takes ~256 s; the first lists of the day are slow on a big universe.
SETUP_NOTES = ("Shoonya: the first lists of the day can take much longer; best for a small number of stocks",)
