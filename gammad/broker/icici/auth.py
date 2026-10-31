"""Client paste only: paste Success.session_token (base64 user:key), not the browser apisession.

The manual browser -> customerdetails exchange is documented in README A1.
No login automation, credential persistence or account-profile endpoint is exposed here.
"""

from gammad.broker.base import BrokerAuthError
from gammad.broker.icici.wire import session_parts

LOGIN_KEYS = ("ICICI_API_KEY", "ICICI_API_SECRET", "ICICI_ACCESS_TOKEN")


def validate(creds):
    if any(not str(creds.get(key) or "").strip() for key in LOGIN_KEYS):
        raise BrokerAuthError("Breeze requires app key, secret and pasted session token", code="809")
    session_parts(creds["ICICI_ACCESS_TOKEN"])
    return dict(creds)
