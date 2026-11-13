"""Multi-Account Configuration Loader (R1.9 Broker-Neutral).

Loads AccountSessionManager and HistoryProviderConfig from runtime settings / environment.
Supports:
- Mode A (Backward-Compatible): Synthesizes primary account identity from legacy single-broker configuration.
- Mode B (Multi-Account Configuration): Parses structured account list from GAMMA_ACCOUNTS_JSON.
- History Provider Decoupling: Ingests GAMMA_HISTORY_PROVIDER independently from trading accounts.
"""

from __future__ import annotations

import json
import os
from collections.abc import Mapping, Sequence
from datetime import date
from pathlib import Path
from typing import Any

from gammad.broker import registry
from gammad.broker.account import (
    AccountCredentials,
    AccountIdentity,
    AccountIdentityError,
    AccountSession,
    AccountSessionManager,
    HistoryProviderConfig,
    SessionRoutingError,
)
from gammad.broker.base import (
    BrokerAdapter,
    BrokerAuthError,
    Candle,
    ChainRow,
    Instrument,
    TokenHealth,
)
from gammad.log import get_logger

log = get_logger(__name__)

ENV_ACCOUNTS_JSON = "GAMMA_ACCOUNTS_JSON"
ENV_HISTORY_PROVIDER = "GAMMA_HISTORY_PROVIDER"
ENV_HISTORY_BARS_LIMIT = "GAMMA_HISTORY_BARS_LIMIT"
ENV_HISTORY_DAILY_BUDGET = "GAMMA_HISTORY_DAILY_BUDGET"


class UnauthenticatedBrokerAdapter(BrokerAdapter):
    """Placeholder adapter for unconfigured or unauthenticated broker sessions."""

    def __init__(self, broker: str, reason: str = "unauthenticated") -> None:
        self.broker = broker
        self.reason = reason

    def instruments(self) -> list[Instrument]:
        raise BrokerAuthError(f"{self.broker}: {self.reason}")

    def daily_candles(
        self,
        security_id: int,
        exchange_segment: str,
        from_date: date,
        to_date: date,
        instrument: str = "EQUITY",
    ) -> list[Candle]:
        raise BrokerAuthError(f"{self.broker}: {self.reason}")

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
        raise BrokerAuthError(f"{self.broker}: {self.reason}")

    def expiries(self, underlying_security_id: int, underlying_seg: str) -> list[date]:
        raise BrokerAuthError(f"{self.broker}: {self.reason}")

    def option_chain(self, underlying_security_id: int, underlying_seg: str, expiry: date) -> list[ChainRow]:
        raise BrokerAuthError(f"{self.broker}: {self.reason}")

    def ltp(self, security_ids: Sequence[int], exchange_segment: str = "NSE_EQ") -> dict[int, float]:
        raise BrokerAuthError(f"{self.broker}: {self.reason}")

    def token_health(self) -> TokenHealth:
        return TokenHealth(valid=False, validity_ist=None, error=self.reason, auth_failed=True)

    def close(self) -> None:
        pass


class MockBrokerAdapter(BrokerAdapter):
    """Synthetic adapter for test/mock broker identifiers."""

    def __init__(self, broker: str = "mock_broker") -> None:
        self.broker = broker
        self.closed = False

    def instruments(self) -> list[Instrument]:
        return []

    def daily_candles(self, *args: Any, **kwargs: Any) -> list[Candle]:
        return []

    def intraday(self, *args: Any, **kwargs: Any) -> list[Candle]:
        return []

    def expiries(self, *args: Any, **kwargs: Any) -> list[date]:
        return []

    def option_chain(self, *args: Any, **kwargs: Any) -> list[ChainRow]:
        return []

    def ltp(self, *args: Any, **kwargs: Any) -> dict[int, float]:
        return {}

    def token_health(self) -> TokenHealth:
        return TokenHealth(valid=True, validity_ist="2026-11-02 23:59:59", error=None)

    def close(self) -> None:
        self.closed = True


def _get_env_val(cfg: Any, key: str) -> str:
    """Helper to extract an environment setting from cfg or os.environ."""
    if hasattr(cfg, "env"):
        env = cfg.env
        # Check direct attribute (lowercase or exact)
        attr = key.lower()
        if hasattr(env, attr) and getattr(env, attr):
            return str(getattr(env, attr)).strip()
        # Check values mapping
        if hasattr(env, "values") and isinstance(env.values, Mapping):
            v = env.values.get(key)
            if v:
                return str(v).strip()
    return os.environ.get(key, "").strip()


def load_history_provider_config(cfg: Any) -> HistoryProviderConfig | None:
    """Load decoupled HistoryProviderConfig.

    Ingests GAMMA_HISTORY_PROVIDER, falling back to configured broker (None when unconfigured).
    """
    raw_provider = _get_env_val(cfg, ENV_HISTORY_PROVIDER)
    if raw_provider:
        provider_name = raw_provider.lower()
        if provider_name == "none":
            return None
    else:
        # Fallback to explicitly configured broker; never default to a hardcoded broker
        broker = ""
        if hasattr(cfg, "env") and hasattr(cfg.env, "broker"):
            broker = (cfg.env.broker or "").strip().lower()
        if not broker or broker == "none":
            return None
        provider_name = broker

    # Ingest limits / budget if configured
    raw_bars = _get_env_val(cfg, ENV_HISTORY_BARS_LIMIT)
    raw_budget = _get_env_val(cfg, ENV_HISTORY_DAILY_BUDGET)
    bars_limit = int(raw_bars) if raw_bars and raw_bars.isdigit() else 2000
    budget = int(raw_budget) if raw_budget and raw_budget.isdigit() else 6

    return HistoryProviderConfig(
        provider_name=provider_name,
        daily_bars_limit=bars_limit,
        daily_request_budget=budget,
    )


def load_account_session_manager(cfg: Any, *, data_dir: Path | None = None) -> AccountSessionManager:
    """Load and bootstrap AccountSessionManager from configuration.
    
    Mode A: Backward-compatible fallback when GAMMA_ACCOUNTS_JSON is not configured.
    Mode B: Parses multi-account JSON list when GAMMA_ACCOUNTS_JSON is present.
    """
    raw_accounts_json = _get_env_val(cfg, ENV_ACCOUNTS_JSON)
    d_dir = data_dir or (cfg.paths.data_dir if hasattr(cfg, "paths") else Path("data"))

    if not raw_accounts_json:
        # Mode A: Backward-Compatible Single-Broker Mode
        return _load_mode_a(cfg, d_dir)
    else:
        # Mode B: Multi-Account Configuration Mode
        return _load_mode_b(cfg, raw_accounts_json, d_dir)


def _register_mode_a_history(
    mgr: AccountSessionManager,
    cfg: Any,
    data_dir: Path,
    vals: dict[str, str],
) -> None:
    hist_cfg = load_history_provider_config(cfg)
    if hist_cfg is None:
        return
    if hist_cfg.provider_name in registry.BROKERS:
        hist_vals = dict(vals)
        if hist_cfg.provider_name == "dhan" and hasattr(cfg, "env"):
            if getattr(cfg.env, "dhan_client_id", None):
                hist_vals.setdefault("DHAN_CLIENT_ID", cfg.env.dhan_client_id)
            if getattr(cfg.env, "dhan_access_token", None):
                hist_vals.setdefault("DHAN_ACCESS_TOKEN", cfg.env.dhan_access_token)
        try:
            hist_entry = registry.get(hist_cfg.provider_name)
            hist_creds = registry.creds_from_env(hist_entry, hist_vals)
            hist_adapter = hist_entry.make_adapter(hist_creds, data_dir)
            mgr.register_history_provider(hist_cfg, hist_adapter)
        except Exception as exc:
            log.error(
                "Mode A: failed to initialize history provider %r (%s); registering unauthenticated adapter",
                hist_cfg.provider_name,
                exc,
            )
            mgr.register_history_provider(
                hist_cfg,
                UnauthenticatedBrokerAdapter(hist_cfg.provider_name, reason=str(exc)),
            )
    elif (
        hist_cfg.provider_name.startswith("mock_")
        or hist_cfg.provider_name.startswith("test_")
        or hist_cfg.provider_name.startswith("paper")
    ):
        mgr.register_history_provider(hist_cfg, MockBrokerAdapter(hist_cfg.provider_name))
    else:
        log.error(
            "Mode A: failed to initialize history provider %r (unregistered); registering unauthenticated adapter",
            hist_cfg.provider_name,
        )
        mgr.register_history_provider(
            hist_cfg,
            UnauthenticatedBrokerAdapter(hist_cfg.provider_name, reason="unregistered history provider"),
        )


def _load_mode_a(cfg: Any, data_dir: Path) -> AccountSessionManager:
    """Synthesize AccountSessionManager from legacy single-broker configuration."""
    mgr = AccountSessionManager(active_account_id="primary")
    broker_name = ""
    if hasattr(cfg, "env") and hasattr(cfg.env, "broker"):
        broker_name = (cfg.env.broker or "").strip().lower()

    vals: dict[str, str] = {}
    if hasattr(cfg, "env") and hasattr(cfg.env, "values") and isinstance(cfg.env.values, Mapping):
        vals = dict(cfg.env.values)

    try:
        entry = registry.resolve(broker_name or None)
    except Exception as exc:
        if broker_name and (
            broker_name.startswith("mock")
            or broker_name.startswith("test")
            or broker_name.startswith("paper")
        ):
            ident = AccountIdentity(broker=broker_name, account_id="primary", label=broker_name.title())
            adapter = MockBrokerAdapter(broker_name)
            acc_creds = AccountCredentials(broker=broker_name, account_id="primary", credentials={})
            mgr.register_account(ident, adapter, probe_health=False, credentials=acc_creds)
            hist_cfg = load_history_provider_config(cfg)
            if hist_cfg is not None:
                if hist_cfg.provider_name == broker_name:
                    mgr.register_history_provider(hist_cfg, adapter)
                else:
                    _register_mode_a_history(mgr, cfg, data_dir, vals)
            return mgr
        if _get_env_val(cfg, ENV_HISTORY_PROVIDER):
            _register_mode_a_history(mgr, cfg, data_dir, vals)
        log.warning("Mode A: broker registry resolution failed (%s); returning manager without trading account", exc)
        return mgr

    # Extract credentials mapping
    if entry.name == "dhan" and hasattr(cfg, "env"):
        if getattr(cfg.env, "dhan_client_id", None):
            vals["DHAN_CLIENT_ID"] = cfg.env.dhan_client_id
        if getattr(cfg.env, "dhan_access_token", None):
            vals["DHAN_ACCESS_TOKEN"] = cfg.env.dhan_access_token
    creds = registry.creds_from_env(entry, vals)

    account_id = "primary"
    ident = AccountIdentity(
        broker=entry.name,
        account_id=account_id,
        label=entry.label or "Primary",
    )
    acc_creds = AccountCredentials(
        broker=entry.name,
        account_id=account_id,
        credentials=creds,
    )

    try:
        adapter = entry.make_adapter(creds, data_dir)
        probe = False
    except Exception as exc:
        log.warning("Mode A: adapter instantiation for %s failed (%s); using unauthenticated adapter", entry.name, exc)
        adapter = UnauthenticatedBrokerAdapter(entry.name, reason=str(exc))
        probe = False

    mgr.register_account(ident, adapter, probe_health=probe, credentials=acc_creds)

    # Register decoupled history provider
    hist_cfg = load_history_provider_config(cfg)
    if hist_cfg is None:
        return mgr
    if hist_cfg.provider_name == entry.name:
        mgr.register_history_provider(hist_cfg, adapter)
    else:
        _register_mode_a_history(mgr, cfg, data_dir, vals)

    return mgr


def _load_mode_b(cfg: Any, accounts_json_str: str, data_dir: Path) -> AccountSessionManager:
    """Parse structured accounts list from GAMMA_ACCOUNTS_JSON."""
    json_str = accounts_json_str.strip()
    if not (json_str.startswith("{") or json_str.startswith("[")):
        p = Path(json_str)
        if hasattr(cfg, "paths") and hasattr(cfg.paths, "root") and not p.is_absolute():
            candidate = cfg.paths.root / p
            if candidate.is_file():
                p = candidate
        if p.is_file():
            json_str = p.read_text(encoding="utf-8").strip()

    try:
        data = json.loads(json_str)
    except Exception as exc:
        raise AccountIdentityError(f"invalid GAMMA_ACCOUNTS_JSON syntax: {exc}") from exc

    if isinstance(data, dict) and "accounts" in data:
        items = data["accounts"]
    elif isinstance(data, list):
        items = data
    else:
        raise AccountIdentityError("GAMMA_ACCOUNTS_JSON must be a JSON array or object with an 'accounts' array")

    mgr = AccountSessionManager()
    active_to_set: tuple[str, str] | None = None

    for idx, item in enumerate(items):
        if not isinstance(item, dict):
            raise AccountIdentityError(f"account at index {idx} must be a JSON object")

        raw_id = str(item.get("account_id") or "").strip()
        raw_broker = str(item.get("broker") or "").strip().lower()
        label = str(item.get("label") or "").strip()
        revision = int(item.get("revision") or 1)
        enabled = bool(item.get("enabled", True))
        is_active = bool(item.get("is_active", False) or item.get("active", False))
        creds_raw = item.get("credentials") or {}
        if not isinstance(creds_raw, dict):
            raise AccountIdentityError(f"account {raw_id!r} credentials must be a JSON object")

        # Key and value sanitization
        sanitized_creds = {str(k).strip(): str(v).strip() for k, v in creds_raw.items()}

        # AccountIdentity validation (rejection of token patterns, length, charset)
        ident = AccountIdentity(
            broker=raw_broker,
            account_id=raw_id,
            label=label or raw_id,
            revision=revision,
            enabled=enabled,
        )

        acc_creds = AccountCredentials(
            broker=raw_broker,
            account_id=raw_id,
            credentials=sanitized_creds,
        )

        # Adapter instantiation
        if raw_broker in registry.BROKERS:
            entry = registry.get(raw_broker)
            merged_creds = dict(cfg.env.values if hasattr(cfg, "env") and hasattr(cfg.env, "values") and isinstance(cfg.env.values, Mapping) else {})
            for k, v in sanitized_creds.items():
                merged_creds[k] = v
                merged_creds[k.upper()] = v
                prefix = registry.ENV_PREFIX.get(raw_broker, "")
                if prefix and not k.upper().startswith(prefix):
                    merged_creds[f"{prefix}{k.upper()}"] = v
            adapter_creds = registry.creds_from_env(entry, merged_creds)
            try:
                adapter = entry.make_adapter(adapter_creds, data_dir)
                probe = False
            except Exception as exc:
                log.warning("Mode B: adapter instantiation failed for %s (%s)", ident.account_key, exc)
                adapter = UnauthenticatedBrokerAdapter(raw_broker, reason=str(exc))
                probe = False
        elif raw_broker.startswith("mock_") or raw_broker.startswith("test_") or raw_broker.startswith("paper"):
            adapter = MockBrokerAdapter(raw_broker)
            probe = False
        else:
            adapter = UnauthenticatedBrokerAdapter(raw_broker, reason=f"broker {raw_broker!r} not in registry")
            probe = False

        session = mgr.register_account(ident, adapter, probe_health=probe, credentials=acc_creds)
        if is_active and enabled:
            active_to_set = (raw_broker, raw_id)

    if active_to_set is not None:
        try:
            mgr.set_active_account(active_to_set[0], active_to_set[1])
        except Exception as exc:
            log.warning("could not set active account %s: %s", active_to_set, exc)

    # Register decoupled history provider (respect explicit GAMMA_HISTORY_PROVIDER="none")
    hist_cfg = load_history_provider_config(cfg)
    explicit_hist_none = _get_env_val(cfg, ENV_HISTORY_PROVIDER).lower() == "none"
    if hist_cfg is None and not explicit_hist_none:
        active = mgr.active_account()
        if active is not None:
            raw_bars = _get_env_val(cfg, ENV_HISTORY_BARS_LIMIT)
            raw_budget = _get_env_val(cfg, ENV_HISTORY_DAILY_BUDGET)
            bars_limit = int(raw_bars) if raw_bars and raw_bars.isdigit() else 2000
            budget = int(raw_budget) if raw_budget and raw_budget.isdigit() else 6
            hist_cfg = HistoryProviderConfig(
                provider_name=active.identity.broker,
                daily_bars_limit=bars_limit,
                daily_request_budget=budget,
            )
    if hist_cfg is not None:
        matching = [s for s in mgr.list_accounts() if s.identity.broker == hist_cfg.provider_name and s.adapter is not None]
        if matching:
            mgr.register_history_provider(hist_cfg, matching[0].adapter)
        elif hist_cfg.provider_name in registry.BROKERS:
            try:
                hist_entry = registry.get(hist_cfg.provider_name)
                merged_env = dict(cfg.env.values if hasattr(cfg, "env") and hasattr(cfg.env, "values") and isinstance(cfg.env.values, Mapping) else {})
                hist_creds = registry.creds_from_env(hist_entry, merged_env)
                hist_adapter = hist_entry.make_adapter(hist_creds, data_dir)
                mgr.register_history_provider(hist_cfg, hist_adapter)
            except Exception as exc:
                log.warning("Mode B: decoupled history adapter instantiation for %s failed (%s); using unauthenticated", hist_cfg.provider_name, exc)
                active = mgr.active_account()
                if active is not None and active.adapter is not None:
                    mgr.register_history_provider(hist_cfg, active.adapter)
                else:
                    mgr.register_history_provider(hist_cfg, UnauthenticatedBrokerAdapter(hist_cfg.provider_name, reason=str(exc)))
        elif hist_cfg.provider_name.startswith("mock_") or hist_cfg.provider_name.startswith("test_") or hist_cfg.provider_name.startswith("paper"):
            mgr.register_history_provider(hist_cfg, MockBrokerAdapter(hist_cfg.provider_name))
        else:
            active = mgr.active_account()
            if active is not None and active.adapter is not None:
                mgr.register_history_provider(hist_cfg, active.adapter)
            else:
                mgr.register_history_provider(hist_cfg, UnauthenticatedBrokerAdapter(hist_cfg.provider_name, reason="unregistered history provider"))

    return mgr


def resolve_configured_adapter(cfg: Any | None = None, *, for_history: bool = False) -> BrokerAdapter:
    """Resolve the active or decoupled history BrokerAdapter from settings without hardcoding Dhan."""
    if cfg is None:
        from gammad.config import settings

        cfg = settings()
    mgr = load_account_session_manager(cfg)
    if for_history:
        try:
            return mgr.history_adapter()
        except SessionRoutingError:
            pass
    active = mgr.active_account()
    if active is not None and active.adapter is not None:
        return active.adapter
    entry = registry.resolve(getattr(getattr(cfg, "env", None), "broker", None) or None)
    return entry.make_adapter(
        registry.creds_from_env(
            entry,
            getattr(getattr(cfg, "env", None), "values", {}) or {},
        ),
        cfg.paths.data_dir,
    )
