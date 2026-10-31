"""A7 Black-Scholes + IV solver PORTED (not imported: gammad/scan/ is CLIENT_EXCLUDE,
contract F-1) from GAMMA-UNWIND gammad/scan/owner_hooks.py @c0f21a7a:
RISK_FREE_RATE / DIVIDEND_YIELD / YEAR_MS / IV_LO, IV_HI / IV_ITERS / IV_PRICE_TOL
:103-108, _norm_cdf :117-118, bs_price :121-131, iv_calc :134-174 (D382 assumptions),
years_to_expiry :177-180. Flat rate, zero dividend, European exercise, ACT/365.
"""

import math
from collections.abc import Callable
from datetime import date
from typing import NamedTuple

from gammad.core import session_clock
from gammad.scanners import contract as sc

RISK_FREE_RATE = 0.055  # D382 assumptions.
DIVIDEND_YIELD = 0.0  # D382 assumptions.
YEAR_MS = 365 * 86_400_000  # D382: ACT/365.
IV_LO, IV_HI = 1e-4, 5.0  # D382 solver bracket.
IV_ITERS = 200  # D382 solver bound.
IV_PRICE_TOL = 1e-6  # D382 repricing tolerance.
RISK_PER_TRADE_PCT = 1.0  # G6, D617 (4), D624 (3).
MAX_OPEN = 6  # G6, D617 (4), D624 (3).
DAY_LOSS_CAP_PCT = 3.0  # G6, D617 (4), D624 (3).
NO_IV_STOP_PCT = 40.0  # A7, D628 (3).
BASIS_BS = "BS_ZONE"
BASIS_NO_ZONE = "NO_ZONE_PCT"
BASIS_NO_IV = "NO_IV_PCT"


def _norm_cdf(x: float) -> float:
    return 0.5 * math.erfc(-x / math.sqrt(2.0))


def bs_price(spot, strike, t_years, vol, option_type, *, rate=RISK_FREE_RATE, div=DIVIDEND_YIELD) -> float:
    sq = vol * math.sqrt(t_years)
    d1 = (math.log(spot / strike) + (rate - div + 0.5 * vol * vol) * t_years) / sq
    d2 = d1 - sq
    fwd_s = spot * math.exp(-div * t_years)
    disc_k = strike * math.exp(-rate * t_years)
    if option_type == "CE":
        return fwd_s * _norm_cdf(d1) - disc_k * _norm_cdf(d2)
    return disc_k * _norm_cdf(-d2) - fwd_s * _norm_cdf(-d1)


def iv_from_ltp(ltp, spot, strike, t_years, option_type) -> float | None:
    """The ported bisection, returning vol points for the IvView (D629 (1))."""
    rate, div = RISK_FREE_RATE, DIVIDEND_YIELD
    vals = (ltp, spot, strike, t_years)
    if option_type not in ("CE", "PE") or any(v is None for v in vals):
        return None
    p, s, k, t = (float(v) for v in vals)
    if not all(math.isfinite(v) and v > 0 for v in (p, s, k, t)):
        return None
    fwd_s, disc_k = s * math.exp(-div * t), k * math.exp(-rate * t)
    lo_px, hi_px = (max(0.0, fwd_s - disc_k), fwd_s) if option_type == "CE" else (max(0.0, disc_k - fwd_s), disc_k)
    if not lo_px < p < hi_px:
        return None

    def px(v: float) -> float:
        return bs_price(s, k, t, v, option_type, rate=rate, div=div)

    lo, hi = IV_LO, IV_HI
    if not px(lo) <= p <= px(hi):
        return None
    for _ in range(IV_ITERS):
        mid = 0.5 * (lo + hi)
        if px(mid) < p:
            lo = mid
        else:
            hi = mid
        if hi - lo < 1e-12:
            break
    vol = 0.5 * (lo + hi)
    return vol * 100.0 if abs(px(vol) - p) <= IV_PRICE_TOL * max(1.0, p) else None


def years_to_expiry(ts_ms: int, expiry_date: date) -> float:
    return (session_clock.fo_bounds_ms(expiry_date)[1] - ts_ms) / YEAR_MS


class LotRisk(NamedTuple):
    rs_per_lot: float
    stop_premium: float
    is_estimate: bool
    basis: str


def v1_lot_risk(entry_px, lot_size, strike, option_type, zone_edge: float | None,
                t_years: float | None, iv_pct: float | None) -> LotRisk:
    stop = entry_px * (1 - NO_IV_STOP_PCT / 100)
    loss = (entry_px - stop) * lot_size
    if zone_edge is None:
        return LotRisk(loss, stop, False, BASIS_NO_ZONE)
    if iv_pct is None or not iv_pct > 0 or t_years is None or not t_years > 0 or not zone_edge > 0:
        return LotRisk(loss, stop, False, BASIS_NO_IV)
    est = bs_price(zone_edge, strike, t_years, iv_pct / 100, option_type)
    if est >= entry_px:
        return LotRisk(loss, stop, False, BASIS_NO_IV)
    return LotRisk((entry_px - est) * lot_size, est, True, BASIS_BS)


class RiskPerLot:
    def __init__(self, iv: sc.IvView, years_fn: Callable[[int, date], float] = years_to_expiry):
        self.iv = iv
        self.years_fn = years_fn

    def __call__(self, row, signal_px: float, stop_kind: str, stop_level: float,
                 ts_ms: int) -> tuple[float | None, bool]:
        if stop_kind == sc.STOP_ZONE:
            risk = v1_lot_risk(signal_px, row.lot_size, row.strike, row.option_type, stop_level,
                               self.years_fn(ts_ms, row.expiry_date), self.iv.iv_pct(row.security_id, ts_ms))
            return risk.rs_per_lot, risk.is_estimate
        per_lot = (signal_px - stop_level) * row.lot_size
        return (per_lot, False) if per_lot > 0 else (None, False)


def size_lots(capital_rs: float, rs_per_lot: float | None) -> int:
    if rs_per_lot is None or not rs_per_lot > 0 or not capital_rs > 0:
        return 0
    return math.floor(capital_rs * RISK_PER_TRADE_PCT / 100 / rs_per_lot)


def g6_size(book: sc.RiskBook, day: date, rs_per_lot: float | None) -> tuple[int, str | None]:
    if book.open_count() >= MAX_OPEN:
        return 0, "MAX_OPEN"
    capital = book.capital_rs()
    if book.day_pnl_rs(day) <= -capital * DAY_LOSS_CAP_PCT / 100:
        return 0, "DAY_CAP"
    return size_lots(capital, rs_per_lot), None
