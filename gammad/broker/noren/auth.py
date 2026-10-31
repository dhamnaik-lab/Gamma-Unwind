"""Client-pasted session only. No login, refresh, owner secret, or credential persistence."""

from collections.abc import Mapping

from gammad.broker.base import BrokerAuthError
from gammad.broker.noren.wire import NorenHost


def credentials(host: NorenHost, creds: Mapping[str, str]) -> tuple[str, str]:
    uid = (creds.get(f"{host.env_prefix}_USER_ID") or "").strip()
    token = (creds.get(f"{host.env_prefix}_ACCESS_TOKEN") or "").strip()
    if not uid or not token:
        raise BrokerAuthError(f"{host.broker}: paste user id and session token", "809")
    return uid, token


def validate(adapter):
    """The caller owns persistence; probe only, return TokenHealth."""
    return adapter.token_health()
