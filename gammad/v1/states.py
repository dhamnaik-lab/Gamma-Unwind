"""Public state and reason vocabulary for the rebuild's independent machine."""

V1R_LIVE = ("V1R_OPEN", "V1R_TRAILING", "V1R_HALF")
REASON_OF_KIND = {
    "V1R_STOP": "stop_level", "V1R_CAP": "stop_cap", "V1R_NOPROG": "noprog",
    "V1R_TRAIL": "trail", "V1R_TARGET": "target", "V1R_HALF": "half_book",
    "V1R_EOD": "intraday_close", "V1R_SETTLED": "settled_at_expiry",
}
V1R_EVENT_KINDS = tuple(REASON_OF_KIND)
V1R_EXIT_KINDS = tuple(k for k in V1R_EVENT_KINDS if k != "V1R_HALF")
CLOSED_STATE_OF_KIND = {k: "CLOSED_" + k for k in V1R_EXIT_KINDS}
STOP_KINDS = frozenset({"V1R_STOP", "V1R_CAP"})
