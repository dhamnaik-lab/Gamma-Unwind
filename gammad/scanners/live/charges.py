"""Port of the owner charges model, same rates file and order of terms."""

from functools import lru_cache

import yaml

from gammad import REPO_ROOT

COSTS_PATH = REPO_ROOT / "config" / "costs.yaml"
RATE_KEYS = (
    "brokerage_per_order", "stt_sell_pct", "exchange_pct", "sebi_per_crore",
    "stamp_buy_pct", "ipft_per_crore", "gst_pct",
)
CRORE = 10_000_000.0


@lru_cache(maxsize=4)
def _rates(path: str) -> dict[str, float]:
    with open(path, encoding="utf-8") as fh:
        raw = yaml.safe_load(fh) or {}
    return {key: float(raw["rates"][key]["value"]) for key in RATE_KEYS}


def charges(entry_px: float, exit_px: float, qty: float, orders: int = 2) -> float:
    if not qty or qty <= 0:
        return 0.0
    r = _rates(str(COSTS_PATH))
    buy, sell = float(entry_px) * qty, float(exit_px) * qty
    turnover = buy + sell
    parts = {
        "brokerage": r["brokerage_per_order"] * orders,
        "stt": sell * r["stt_sell_pct"] / 100.0,
        "exchange": turnover * r["exchange_pct"] / 100.0,
        "sebi": turnover * r["sebi_per_crore"] / CRORE,
        "stamp": buy * r["stamp_buy_pct"] / 100.0,
        "ipft": turnover * r["ipft_per_crore"] / CRORE,
    }
    parts["gst"] = (parts["brokerage"] + parts["exchange"] + parts["sebi"] + parts["ipft"]) * r["gst_pct"] / 100.0
    return sum(parts.values())
