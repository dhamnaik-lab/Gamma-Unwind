"""Client token paste only. FP54-A: paste AccessToken, client code and app key.

No credentials are logged, persisted, or exchanged here. The app validates a
new paste with adapter.token_health before saving it. No automated login.
"""

from collections.abc import Mapping

from gammad.broker.base import BrokerAuthError

KEY_TOKEN = "FIVEPAISA_ACCESS_TOKEN"
KEY_CLIENT = "FIVEPAISA_CLIENT_CODE"
KEY_APP = "FIVEPAISA_APP_KEY"


def credentials(creds: Mapping[str, str]) -> tuple[str, str, str]:
    values = tuple((creds.get(k) or "").strip() for k in (KEY_TOKEN, KEY_CLIENT, KEY_APP))
    if any(not v or any(c.isspace() for c in v) for v in values):
        raise BrokerAuthError("5paisa requires a pasted access token, client code and app key", "809")
    return values
