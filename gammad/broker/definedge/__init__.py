"""Definedge Securities broker package (Stage 1: historical data provider + limits/quotes)."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, Mapping

import httpx

from gammad.broker.base import BrokerError
from gammad.broker.capabilities import (
    BrokerEntry,
    Capabilities,
    EnvKey,
    NEED_OPTIONAL,
    NEED_REQUIRED,
    assess,
)
from gammad.broker.definedge import wire
from gammad.broker.definedge.adapter import DefinedgeAdapter, DefinedgeAuthError

BROKER = "definedge"

CAPS = Capabilities(
    broker=BROKER,
    status="beta-unverified",
    chain="none",
    expiries="none",
    candle_oi=False,
    candle_volume=True,
    daily_history=True,
    intraday_history=True,
    feed="ws",
    feed_oi="none",
    feed_volume="none",
    feed_ltt=False,
    poll_s=None,
    max_feed_instruments=500,
    ltp_batch=1,
    daily_quota=None,
    token=("paste",),
    token_expiry_rule="daily session reset; paste api_session_key in .env",
    data_cost="free",
    evidence=("Definedge Integrate docs (SDS history CSV, DART limits/quotes, public allmaster.zip)",),
)

KEY_TOKEN = EnvKey(
    "DEFINEDGE_API_SESSION_KEY",
    NEED_REQUIRED,
    secret=True,
    written=True,
    note="Definedge Integrate API session key (daily session reset; paste in .env)",
)
KEY_API_TOKEN = EnvKey(
    "DEFINEDGE_API_TOKEN",
    NEED_OPTIONAL,
    secret=False,
    written=False,
    note="Optional Integrate API token (for Stage 2 login readiness)",
)
KEY_API_SECRET = EnvKey(
    "DEFINEDGE_API_SECRET",
    NEED_OPTIONAL,
    secret=True,
    written=False,
    note="Optional Integrate API secret (for Stage 2 login readiness)",
)

ENV: tuple[EnvKey, ...] = (KEY_TOKEN, KEY_API_TOKEN, KEY_API_SECRET)
LOGIN_KEYS: tuple[str, ...] = ()


def make_adapter(
    creds: Mapping[str, str],
    data_dir: Path,
    *,
    transport: httpx.BaseTransport | None = None,
    clock: Callable[[], float] | None = None,
    sleep: Callable[[float], None] | None = None,
) -> DefinedgeAdapter:
    session_key = str(creds.get(KEY_TOKEN.name, "") or "").strip()
    if not session_key:
        raise DefinedgeAuthError(f"{KEY_TOKEN.name} not set (.env)", code="809")
    return DefinedgeAdapter(
        session_key,
        data_dir,
        api_token=creds.get(KEY_API_TOKEN.name, ""),
        api_secret=creds.get(KEY_API_SECRET.name, ""),
        transport=transport,
        clock=clock,
        sleep=sleep,
    )


def make_feed(
    creds: Mapping[str, str],
    out: Any = None,
    on_auth_error: Callable[..., Any] | None = None,
    *,
    adapter: Any = None,
    connect: Any = None,
    clock_ms: Callable[[], int] | None = None,
    **kwargs: Any,
) -> Any:
    del creds, out, on_auth_error, adapter, connect, clock_ms, kwargs
    raise BrokerError(
        "definedge: live WebSocket feed is deferred to Stage 2; "
        "use GAMMA_HISTORY_PROVIDER=definedge alongside a supported trading broker"
    )


ENTRY = BrokerEntry(
    name=BROKER,
    label="Definedge",
    caps=CAPS,
    env_keys=ENV,
    token_key=KEY_TOKEN.name,
    make_adapter=make_adapter,
    make_feed=make_feed,
    doc_url=wire.DOC_URL,
)

__all__ = [
    "BROKER",
    "CAPS",
    "ENTRY",
    "ENV",
    "KEY_API_SECRET",
    "KEY_API_TOKEN",
    "KEY_TOKEN",
    "LOGIN_KEYS",
    "DefinedgeAdapter",
    "DefinedgeAuthError",
    "assess",
    "make_adapter",
    "make_feed",
]
