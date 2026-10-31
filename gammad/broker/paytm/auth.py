"""Paste-only read/public tokens. No login automation or token persistence."""
from gammad.broker.base import BrokerAuthError

KEY_TOKEN = "PAYTM_READ_ACCESS_TOKEN"
KEY_PUBLIC = "PAYTM_PUBLIC_ACCESS_TOKEN"


def paste(creds, key=KEY_TOKEN):
    token = (creds.get(key) or "").strip()
    if not token or any(c.isspace() for c in token):
        raise BrokerAuthError(f"{key}: paste a non-empty token", code="809")
    return token
