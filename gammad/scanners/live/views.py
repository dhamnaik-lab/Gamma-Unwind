"""Injected, time-aware feed views for the pure scanners."""

from collections.abc import Callable, Mapping
from datetime import date

from gammad.core.h1zones import H1ZoneBook, near_far, stop_zone_for
from gammad.jobs.ban_list import BanList
from gammad.scanners import contract as sc
from gammad.scanners.shared.risk import iv_from_ltp, years_to_expiry


class LiveBanView:
    def __init__(self, get_list: Callable[[date], BanList | None]):
        self.get_list = get_list

    def status(self, day: date) -> str:
        listing = self.get_list(day)
        return sc.BAN_OK if listing is not None and listing.date == day and listing.ok else sc.BAN_UNKNOWN

    def ban_reason(self, symbol: str, day: date) -> str | None:
        listing = self.get_list(day)
        if listing is not None and listing.date == day and listing.ok and listing.banned(symbol):
            return "F&O ban"
        return None


class LiveZoneView:
    def __init__(self, book_of: Callable[[str], H1ZoneBook | None]):
        self.book_of = book_of

    def near_edge(self, symbol: str, side: str, price: float, ts_ms: int) -> float | None:
        book = self.book_of(symbol)
        # M12 via stop_zone_for: a zone fault is NO_ZONE (the -40 % stop, D618), never an aborted bar pass
        # (audit P2-LIVE-BOOK-02).
        zone = stop_zone_for(book, side, price, ts_ms)
        return None if zone is None else near_far(side, zone)[0]


class LiveIvView:
    """ltp_of / spot_of take (security_id, ts_ms), using observations known at ts_ms.

    meta_of(security_id) returns a ListRow-like object or mapping with
    underlying_security_id, strike, expiry_date (date), and option_type.
    """

    def __init__(self, ltp_of, spot_of, meta_of):
        self.ltp_of, self.spot_of, self.meta_of = ltp_of, spot_of, meta_of

    def iv_pct(self, security_id: int, ts_ms: int) -> float | None:
        meta = self.meta_of(security_id)
        ltp = self.ltp_of(security_id, ts_ms)
        if meta is None or ltp is None:
            return None
        get = meta.get if isinstance(meta, Mapping) else lambda key: getattr(meta, key, None)
        underlying, strike, expiry, side = (get(k) for k in (
            "underlying_security_id", "strike", "expiry_date", "option_type"))
        if any(value is None for value in (underlying, strike, expiry, side)):
            return None
        spot = self.spot_of(underlying, ts_ms)
        if spot is None:
            return None
        return iv_from_ltp(ltp, spot, strike, years_to_expiry(ts_ms, expiry), side)
