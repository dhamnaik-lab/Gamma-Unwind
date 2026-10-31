"""Client-facing broker hints (0.8.3 T-229 (2)-(4), D230; D197 client exception #2). The ONE place these strings live:
the adapters put the hint on BrokerEntitlementError / TokenHealth.problem, app.py sends the TOKEN line and fills
/api/status token.problem, scripts/smoke_broker.py prints the hint in Check Setup. The dashboard's generic no-prices
text is the dashboard's own (D230 (1)); it is not repeated here.

Wording is the owner's (D230 (2)-(4)); tests/test_083_entitlement.py pins every string to the D230 row.
"""

from __future__ import annotations

FYERS_HINT = (
    "FYERS accepted your login but refused price data: your API app has no Market Data permission. Open "
    "myapi.fyers.in, edit your app, tick Market Data, save, then log in again on the Broker login page."
)
ANGEL_HINT = (
    "Angel One refused your API key for prices. Step 1: log in again here. Step 2, if it repeats: copy the Trading "
    "API app's key from smartapi.angelone.in → My Apps into ANGEL_API_KEY in .env, restart GAMMA, log in, then "
    "run Check Setup and send us the report."
)
# The D230 TOKEN lines = line + " " + NOT_BUILT_SUFFIX; the suffix is sent only while today's 09:20 lists are not built
FYERS_TOKEN_LINE = "FYERS refused price data: turn on Market Data for your app at myapi.fyers.in, then log in again."
ANGEL_TOKEN_LINE = "Angel One refused your API key for prices: log in again on the Broker login page."
NOT_BUILT_SUFFIX = "Today's lists were not built yet."

HINTS: dict[str, str] = {"fyers": FYERS_HINT, "angel": ANGEL_HINT}
TOKEN_LINES: dict[str, str] = {"fyers": FYERS_TOKEN_LINE, "angel": ANGEL_TOKEN_LINE}

# token_events kind for a refused data call (the dashboard's recent-logins word: "prices refused", D230 (4))
EVENT_DATA_REFUSED = "data_refused"


def token_line(broker: str | None, *, lists_built: bool = False) -> str | None:
    """The Telegram TOKEN line for `broker`'s entitlement refusal, or None (a broker without a D230 line). The
    "not built yet" sentence rides only while today's 09:20 lists are NOT built (`lists_built` False)."""
    line = TOKEN_LINES.get(str(broker or "").lower())
    if line is None or lists_built:
        return line
    return f"{line} {NOT_BUILT_SUFFIX}"
