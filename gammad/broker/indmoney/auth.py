"""Client paste validation (IN1). No automated login or credential persistence."""

from gammad.broker.base import BrokerAuthError

KEY_TOKEN = "INDMONEY_ACCESS_TOKEN"


def token_from(creds):
    token = str(creds.get(KEY_TOKEN) or "").strip()
    if not token or any(c in token for c in "\r\n"):
        raise BrokerAuthError("INDmoney access token is missing or malformed", code="809")
    return token


def validate(creds, data_dir, *, transport=None):
    """Validate the pasted token using the same cheap profile probe as token_health."""
    from gammad.broker.indmoney import make_adapter

    adapter = make_adapter(creds, data_dir, transport=transport)
    try:
        return adapter.token_health()
    finally:
        adapter.close()
