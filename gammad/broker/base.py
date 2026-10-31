"""Broker-neutral contract. Every broker adapter returns these dataclasses and nothing else."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date


class BrokerError(RuntimeError):
    """A broker call failed for a non-auth reason (bad input, rate limit, server error)."""

    def __init__(self, message: str, code: str | None = None, status: int | None = None) -> None:
        super().__init__(message)
        self.code = code
        self.status = status


class BrokerAuthError(BrokerError):
    """The broker rejected the credentials (expired / invalid token). Never retried blindly."""


class BrokerEntitlementError(BrokerAuthError):
    """0.8.3 T-229 (2), D230: the broker ACCEPTED the login but refused price data (FYERS HTTP 403 without the
    Market Data permission, Angel AG8004). Still an auth error for every auth branch (never retried, mark_levels
    re-raises it), with the canonical code "806" so is_token_rejection() stays False and the token watcher never
    flips to EXPIRED. `broker` = registry name, `native` = the broker's own code (403 / "AG8004"), `hint` = the
    client sentence from gammad/broker/hints.py."""

    def __init__(self, message: str, *, broker: str, native: int | str, hint: str, status: int | None = None) -> None:
        super().__init__(message, AUTH_ENTITLEMENT_CODE, status)
        self.broker = broker
        self.native = native
        self.hint = hint


@dataclass(frozen=True)
class Instrument:
    security_id: int
    symbol: str
    exchange_segment: str
    instrument_type: str
    underlying_symbol: str | None
    expiry_date: date | None
    strike: float | None
    option_type: str | None
    lot_size: int
    # S1.3.0: the minimum price step in RUPEES (Dhan TICK_SIZE is in paise: 5.0000 -> 0.05); None = unknown.
    tick_size: float | None = None


@dataclass(frozen=True)
class Candle:
    ts_ms: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    oi: float | None = None


@dataclass(frozen=True)
class ChainRow:
    strike: float
    option_type: str  # "CE" | "PE"
    security_id: int | None
    ltp: float
    oi: int
    volume: int
    iv: float | None
    previous_oi: int | None
    # S1.5.0 list floor input (D157(1), D161(5)); LAST with defaults because Kotak / the backtest build rows
    # positionally. previous_volume = the previous session's traded QUANTITY (shares, never lots: Dhan measured
    # 2026-09-30); previous_close = that session's close, rupees. None = the broker's chain does not carry it.
    previous_volume: int | None = None
    previous_close: float | None = None


# The canonical auth code every adapter uses for "the app/plan is not entitled" (a 403 PermissionException,
# Groww/Fyers 403, Dhan feed 806): a real rejection, but NOT an expired token -- a new login does not fix it.
AUTH_ENTITLEMENT_CODE = "806"


@dataclass(frozen=True)
class TokenHealth:
    valid: bool
    validity_ist: str | None
    error: str | None
    # W14 H4: True = the broker rejected the TOKEN (the adapter caught its BrokerAuthError). The token watcher
    # classifies by this flag, not by the error text (only Dhan/Angel/Kotak say "auth rejected").
    auth_failed: bool = False
    # 0.8.3 T-229 (3): the login is valid but ONE direct quote was refused for entitlement (FYERS 403 / Angel
    # AG8004) -> the client hint (gammad/broker/hints.py); None = prices flowed or could not be judged
    problem: str | None = None
    # three-way price check (skeptic L1): True = the quote returned (prices flow: clear a standing problem), False =
    # refused for entitlement (`problem` = the hint), None = not judged (no check, or it failed for another reason)
    prices: bool | None = None


def is_token_rejection(exc: BaseException | None) -> bool:
    """True when `exc` is the broker rejecting the credentials (a BrokerAuthError that is not an entitlement 806)."""
    return isinstance(exc, BrokerAuthError) and str(exc.code or "") != AUTH_ENTITLEMENT_CODE


def health_from_error(exc: BrokerError, error: str | None = None, validity_ist: str | None = None) -> TokenHealth:
    """token_health()'s answer for a failed probe: invalid, `error` (default str(exc)), auth_failed by TYPE."""
    return TokenHealth(False, validity_ist, str(exc) if error is None else error, is_token_rejection(exc))


class BrokerAdapter(ABC):
    @abstractmethod
    def instruments(self) -> list[Instrument]:
        """NSE cash equities and NSE F&O contracts from today's instrument master."""

    @abstractmethod
    def daily_candles(
        self,
        security_id: int,
        exchange_segment: str,
        from_date: date,
        to_date: date,
        instrument: str = "EQUITY",
    ) -> list[Candle]:
        """Daily candles with from_date <= bar date <= to_date (inclusive both ends), oldest first."""

    @abstractmethod
    def intraday(
        self,
        security_id: int,
        exchange_segment: str,
        interval_min: int,
        from_date: date,
        to_date: date,
        oi: bool = True,
        instrument: str = "OPTSTK",
    ) -> list[Candle]:
        """Intraday candles, oldest first."""

    @abstractmethod
    def expiries(self, underlying_security_id: int, underlying_seg: str) -> list[date]:
        """Option expiries for an underlying, ascending."""

    @abstractmethod
    def option_chain(self, underlying_security_id: int, underlying_seg: str, expiry: date) -> list[ChainRow]:
        """Every strike x CE/PE for one expiry."""

    @abstractmethod
    def ltp(self, security_ids: Sequence[int], exchange_segment: str = "NSE_EQ") -> dict[int, float]:
        """Last traded price per security_id. Missing ids are absent from the result."""

    @abstractmethod
    def token_health(self) -> TokenHealth:
        """Whether the configured token is accepted and when it expires."""

    def close(self) -> None:
        """Best effort: close the adapter's own HTTP client (`_http` / `http`) when it has one. For a throwaway
        adapter (the token watcher's day-one probe, W14); never raises."""
        for name in ("_http", "http"):
            client = getattr(self, name, None)
            closer = getattr(client, "close", None)
            if callable(closer):
                try:
                    closer()
                except Exception:  # noqa: BLE001, S110 -- best effort by contract
                    pass
