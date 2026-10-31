"""Flattrade current endpoints and authentication (N1/N3, breaking-change notice)."""

from functools import partial

from gammad.broker.noren import NorenHost, declaration, entry, environment
from gammad.broker.noren import runtime_reasons as noren_runtime_reasons

HOST = NorenHost(
    broker="flattrade", rest_base="https://piconnect.flattrade.in/PiConnectAPI/",
    ws_url="wss://piconnect.flattrade.in/PiConnectWSAPI/",
    master_urls=(
        "https://flattrade.s3.ap-south-1.amazonaws.com/scripmaster/NSE_Equity.csv",
        "https://flattrade.s3.ap-south-1.amazonaws.com/scripmaster/Nfo_Equity_Derivatives.csv",
        "https://flattrade.s3.ap-south-1.amazonaws.com/scripmaster/Nfo_Index_Derivatives.csv",
    ),
    login_style="paste-api-token", env_prefix="FLATTRADE", heartbeat_s=30,
    connect_type="a", token_field="accesstoken",
    rate_per_s=40, requests_per_minute=200,  # fixtures/flattrade/README.md N11
    # D726 BETA: the documented touchline/GetQuotes `v` is admitted through wire.quantity_volume (U4 lot guard).
    volume_verified=True,
)
CAPS = declaration(HOST, expiry="05:00 IST daily", cost="free")
ENV = environment(HOST)
LOGIN_KEYS = ()
ENTRY = entry(HOST, CAPS, ENV, "Flattrade", "https://pi.flattrade.in/docs")
make_adapter, make_feed = ENTRY.make_adapter, ENTRY.make_feed
runtime_reasons = partial(noren_runtime_reasons, HOST)  # registry.runtime_reasons: status + Check Setup
