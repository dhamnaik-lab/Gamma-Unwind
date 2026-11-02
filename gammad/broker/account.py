"""R1.9 Broker-Neutral Multi-Account Identity & Session Routing Core.

Architectural boundaries (ADR-002, ADR-009):
- Broker-neutral: no hard-coded preferred or default broker.
- Multi-account concurrent operation: independent sessions, isolated credentials,
  separate feeds and watermarks per (broker, account_id).
- Separation of concerns: Historical market data providers are managed independently
  from trading / paper monitoring accounts.
- Zero live orders: purely informational and paper-trade tracking.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime
from types import MappingProxyType
from typing import Any

from gammad.broker.base import BrokerAdapter, Candle, ChainRow, Instrument, TokenHealth
from gammad.broker.registry import KNOWN_BROKERS


class AccountIdentityError(ValueError):
    """Raised when an account identity or credential payload is invalid or ambiguous."""


class SessionRoutingError(RuntimeError):
    """Raised when an account session or provider cannot be resolved or routed."""


_ACCOUNT_ID_RE = re.compile(r"^[A-Za-z0-9_-]{3,64}$")
_DISALLOWED_TOKEN_SUBSTRINGS = (
    "eyj", "token", "bearer", "secret", "session", "auth", "oauth",
)


@dataclass(frozen=True)
class AccountIdentity:
    """Explicit, trusted identity for a broker account.
    
    Never pass access tokens, secret keys, or auth headers as account_id.
    """
    broker: str
    account_id: str
    label: str = ""
    revision: int = 1
    enabled: bool = True

    def __post_init__(self) -> None:
        if not isinstance(self.broker, str) or not self.broker:
            raise AccountIdentityError("broker must be a non-empty string")
        b_clean = self.broker.strip().lower()
        if (
            b_clean not in KNOWN_BROKERS
            and not b_clean.startswith("mock_")
            and not b_clean.startswith("test_")
            and not b_clean.startswith("paper")
        ):
            raise AccountIdentityError(f"unrecognized broker {self.broker!r}; must be in registry")
        object.__setattr__(self, "broker", b_clean)

        if not isinstance(self.account_id, str) or not self.account_id:
            raise AccountIdentityError("account_id must be a non-empty string")
        acc_clean = self.account_id.strip()
        if len(acc_clean) != len(self.account_id):
            raise AccountIdentityError("account_id must not have leading or trailing whitespace")
        if not _ACCOUNT_ID_RE.match(acc_clean):
            raise AccountIdentityError(
                f"account_id {self.account_id!r} contains invalid characters or length; "
                "must be 3-64 alphanumeric characters, underscores, or hyphens"
            )

        # Security check: prevent accidental exposure of tokens as account_id
        acc_lower = acc_clean.lower()
        for pattern in _DISALLOWED_TOKEN_SUBSTRINGS:
            if pattern in acc_lower and len(acc_clean) >= 32:
                raise AccountIdentityError(
                    f"account_id {self.account_id!r} appears to contain an access token or secret; rejected"
                )

        if not isinstance(self.revision, int) or self.revision < 1:
            raise AccountIdentityError("revision must be a positive integer >= 1")

    @property
    def account_key(self) -> str:
        """Unique canonical composite key: broker:account_id."""
        return f"{self.broker}:{self.account_id}"

    @property
    def namespace_hash(self) -> str:
        """Deterministic digest used for database and watermark segregation."""
        payload = json.dumps([self.broker, self.account_id], separators=(",", ":"))
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class AccountCredentials:
    """Explicit, structured credentials for a broker account identity."""
    broker: str
    account_id: str = "primary"
    credentials: Mapping[str, str] = field(default_factory=dict, repr=False)

    def __post_init__(self) -> None:
        if not isinstance(self.broker, str) or not self.broker.strip():
            raise AccountIdentityError("broker must be a non-empty string")
        object.__setattr__(self, "broker", self.broker.strip().lower())
        if not isinstance(self.account_id, str) or not self.account_id.strip():
            raise AccountIdentityError("account_id must be a non-empty string")
        object.__setattr__(self, "account_id", self.account_id.strip())
        if isinstance(self.credentials, dict):
            sanitized = {str(k).strip(): str(v).strip() for k, v in self.credentials.items()}
            object.__setattr__(self, "credentials", MappingProxyType(sanitized))

    def get(self, key: str, default: str = "") -> str:
        return self.credentials.get(key, default)

    def __getitem__(self, key: str) -> str:
        return self.credentials[key]


@dataclass(frozen=True)
class HistoryProviderConfig:
    """Configuration for an independent historical market-data provider.
    
    Decoupled from trading accounts (ADR-002, ADR-009).
    """
    provider_name: str
    daily_bars_limit: int = 2000
    entitlements: frozenset[str] = field(default_factory=lambda: frozenset({"CANDLES", "CHAIN"}))
    daily_request_budget: int = 6
    requires_market_data_subscription: bool = True

    def __post_init__(self) -> None:
        if not self.provider_name or not self.provider_name.strip():
            raise AccountIdentityError("provider_name must be non-empty")
        object.__setattr__(self, "provider_name", self.provider_name.strip().lower())
        if self.daily_bars_limit < 1:
            raise AccountIdentityError("daily_bars_limit must be >= 1")
        if self.daily_request_budget < 1:
            raise AccountIdentityError("daily_request_budget must be >= 1")


@dataclass
class AccountSession:
    """Stateful container for an active authenticated broker account session."""
    identity: AccountIdentity
    adapter: BrokerAdapter
    last_health: TokenHealth | None = None
    last_active: datetime | None = None
    failure_count: int = 0
    isolated_failure_reason: str | None = None
    credentials: Mapping[str, str] = field(default_factory=dict, repr=False)
    account_credentials: AccountCredentials | None = field(default=None, repr=False)

    @property
    def is_operational(self) -> bool:
        if not self.identity.enabled:
            return False
        if self.isolated_failure_reason is not None:
            return False
        if self.last_health is not None and not self.last_health.valid:
            return False
        return True


class AccountSessionManager:
    """Manages concurrent, independent broker accounts and history providers.
    
    Guarantees:
    - Multi-broker isolation: failure in one account does not tear down others.
    - Explicit routing: no implicit primary or fallback broker.
    - History provider decoupling: historical data fetches use dedicated adapters.
    """

    def __init__(self, active_account_id: str | None = None) -> None:
        self._accounts: dict[str, AccountSession] = {}  # key: broker:account_id
        self._history_providers: dict[str, tuple[HistoryProviderConfig, BrokerAdapter]] = {}
        self._active_trading_key: str | None = None
        self._active_history_provider: str | None = None
        self._initial_active_account_id: str | None = (active_account_id.strip() if active_account_id else None)

    def register_account(
        self,
        identity: AccountIdentity,
        adapter: BrokerAdapter,
        probe_health: bool = False,
        credentials: Mapping[str, str] | AccountCredentials | None = None,
    ) -> AccountSession:
        """Register a broker account session."""
        key = identity.account_key
        health: TokenHealth | None = None
        if probe_health:
            try:
                health = adapter.token_health()
            except Exception as exc:  # noqa: BLE001
                health = TokenHealth(valid=False, validity_ist=None, error=str(exc), auth_failed=True)

        if isinstance(credentials, AccountCredentials):
            acc_creds = credentials
            creds_map = dict(credentials.credentials)
        else:
            creds_map = dict(credentials or {})
            acc_creds = AccountCredentials(
                broker=identity.broker,
                account_id=identity.account_id,
                credentials=creds_map,
            )

        session = AccountSession(
            identity=identity,
            adapter=adapter,
            last_health=health,
            last_active=datetime.now(),
            credentials=creds_map,
            account_credentials=acc_creds,
        )
        self._accounts[key] = session
        if self._initial_active_account_id and (
            identity.account_id == self._initial_active_account_id
            or key == self._initial_active_account_id
        ):
            self._active_trading_key = key
        elif self._active_trading_key is None and identity.enabled:
            self._active_trading_key = key
        return session

    def deregister_account(self, broker: str, account_id: str) -> bool:
        """Deregister an account session and close adapter resources cleanly."""
        key = f"{broker.strip().lower()}:{account_id.strip()}"
        session = self._accounts.pop(key, None)
        if session is not None:
            session.adapter.close()
            if self._active_trading_key == key:
                remaining = [k for k, s in self._accounts.items() if s.is_operational]
                self._active_trading_key = remaining[0] if remaining else None
            return True
        return False

    def get_account(self, broker: str, account_id: str) -> AccountSession:
        """Retrieve an account session by composite key."""
        key = f"{broker.strip().lower()}:{account_id.strip()}"
        session = self._accounts.get(key)
        if session is None:
            raise SessionRoutingError(f"no session registered for account {key!r}")
        return session

    def list_accounts(self) -> list[AccountSession]:
        """Return all registered account sessions."""
        return list(self._accounts.values())

    def set_active_account(self, broker: str, account_id: str | None = None) -> None:
        """Explicitly select the active account for paper monitoring."""
        if account_id is None:
            if ":" in broker:
                b, a = broker.split(":", 1)
                key = f"{b.strip().lower()}:{a.strip()}"
            else:
                target = broker.strip().lower()
                matching = [k for k, s in self._accounts.items() if s.identity.account_id.lower() == target or k == target]
                if not matching:
                    raise SessionRoutingError(f"cannot resolve account {broker!r}")
                key = matching[0]
        else:
            key = f"{broker.strip().lower()}:{account_id.strip()}"
        if key not in self._accounts:
            raise SessionRoutingError(f"cannot select unknown account {key!r}")
        if not self._accounts[key].is_operational:
            raise SessionRoutingError(f"cannot select non-operational account {key!r}")
        self._active_trading_key = key

    def active_account(self) -> AccountSession | None:
        """Return the currently selected active account session, if any."""
        if self._active_trading_key is None:
            return None
        return self._accounts.get(self._active_trading_key)

    def isolate_failure(self, broker: str, account_id: str, reason: str) -> None:
        """Isolate a broker/account failure without affecting other accounts."""
        key = f"{broker.strip().lower()}:{account_id.strip()}"
        session = self._accounts.get(key)
        if session is not None:
            session.failure_count += 1
            session.isolated_failure_reason = reason
            if self._active_trading_key == key:
                operational = [k for k, s in self._accounts.items() if s.is_operational]
                self._active_trading_key = operational[0] if operational else None

    # -------------------------------------------------------------------------
    # History Provider Separation (ADR-002, ADR-009)
    # -------------------------------------------------------------------------

    def register_history_provider(
        self,
        config: HistoryProviderConfig,
        adapter: BrokerAdapter,
    ) -> None:
        """Register an independent historical data provider and adapter."""
        self._history_providers[config.provider_name] = (config, adapter)
        if self._active_history_provider is None:
            self._active_history_provider = config.provider_name

    def set_active_history_provider(self, provider_name: str) -> None:
        """Explicitly select the active historical data provider."""
        p_clean = provider_name.strip().lower()
        if p_clean not in self._history_providers:
            raise SessionRoutingError(f"unregistered history provider {p_clean!r}")
        self._active_history_provider = p_clean

    def history_adapter(self, provider_name: str | None = None) -> BrokerAdapter:
        """Retrieve the adapter for historical data queries."""
        target = (provider_name.strip().lower() if provider_name else self._active_history_provider)
        if not target or target not in self._history_providers:
            raise SessionRoutingError(f"no historical data provider available (requested: {target!r})")
        return self._history_providers[target][1]

    def history_config(self, provider_name: str | None = None) -> HistoryProviderConfig:
        """Retrieve the configuration for historical data queries."""
        target = (provider_name.strip().lower() if provider_name else self._active_history_provider)
        if not target or target not in self._history_providers:
            raise SessionRoutingError(f"no historical data provider available (requested: {target!r})")
        return self._history_providers[target][0]
