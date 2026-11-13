"""Broker registry: which brokers exist, how the app picks one, and which credentials each needs (BROKER §4-§8).

Entries are imported at registry import from `ENTRY_MODULES` (Dhan: `gammad.broker.dhan_entry`; the others:
`gammad.broker.<name>` packages exposing `ENTRY`, written by the BR builders). An entry package that does not
exist yet is simply absent (`MISSING`); one that raises on import, or whose ENTRY is malformed or collides with
another entry, is kept out and recorded in `BROKEN` with the reason -- the owner's Dhan service must never fail
to start because a BETA module is broken, and tests/test_broker_contract.py fails on any BROKEN entry.

Edition rules (D86 / no-default-broker):
  * owner & client: `BROKER` absent -> RegistryError (never guesses a default broker; the user chooses).
  * client token lifecycle is always ("paste",): the client pastes the token / credentials in .env or on the
    Token page. Automated login (TOTP, OAuth) is owner-only: `auto_login` calls `edition.require_owner` before
    touching the entry, and entries import gammad.auth.* only inside their owner-only functions.
"""

from __future__ import annotations

import importlib
import sys
from collections.abc import Mapping
from typing import Any

from gammad import edition as _edition
from gammad.broker.capabilities import (
    CORE_ENV_KEYS,
    NEED_AUTO_LOGIN,
    NEED_REQUIRED,
    BrokerEntry,
    CapabilityError,
)
from gammad.log import get_logger

log = get_logger(__name__)

# Spec §6 order (the .env template lists brokers in this order).
# D726 (amends D488 (3)): Shoonya + Flattrade ship in 0.8.9 as BETA (status beta-unverified, no live smoke yet);
# Zebu / ICICI / 5paisa / Paytm / INDmoney stay unregistered (0.9.0 after a live smoke each).
KNOWN_BROKERS: tuple[str, ...] = (
    "dhan", "zerodha", "angel", "fyers", "upstox", "nuvama", "kotak", "groww", "shoonya", "flattrade", "definedge",
)
ENTRY_MODULES: dict[str, str] = {
    b: ("gammad.broker.dhan_entry" if b == "dhan" else f"gammad.broker.{b}") for b in KNOWN_BROKERS
}
# Every key of a broker's .env block starts with its prefix (spec §6); no two brokers share a prefix.
ENV_PREFIX: dict[str, str] = {
    "dhan": "DHAN_",
    "zerodha": "KITE_",
    "angel": "ANGEL_",
    "fyers": "FYERS_",
    "upstox": "UPSTOX_",
    "nuvama": "NUVAMA_",
    "kotak": "KOTAK_",
    "groww": "GROWW_",
    "shoonya": "SHOONYA_",
    "flattrade": "FLATTRADE_",
    "definedge": "DEFINEDGE_",
}
BROKER_ENV_KEY = "BROKER"
CLIENT_TOKEN_KINDS: tuple[str, ...] = ("paste",)
# T-112: a key the service starts without but the daily Token-page login needs. Declared per entry package as
# `LOGIN_KEYS` (EnvKey.need stays "optional": capabilities.NEEDS is the start rule, this is Check Setup's rule).
NEED_LOGIN = "login"


class RegistryError(LookupError):
    """Unknown, missing, broken or unset broker."""


def registry_problems(entry: BrokerEntry, others: Mapping[str, BrokerEntry]) -> list[str]:
    """Cross-entry rules on top of BrokerEntry's own checks."""
    p: list[str] = []
    if entry.name not in KNOWN_BROKERS:
        p.append(f"{entry.name!r} is not in KNOWN_BROKERS")
        return p
    prefix = ENV_PREFIX[entry.name]
    for k in entry.env_keys:
        if not k.name.startswith(prefix):
            p.append(f"env key {k.name!r} must start with {prefix!r}")
        if k.name in CORE_ENV_KEYS:
            p.append(f"env key {k.name!r} belongs to the core")
    mod = sys.modules.get(ENTRY_MODULES[entry.name])
    for k in getattr(mod, "LOGIN_KEYS", ()) or ():
        if k not in entry.key_names or k == entry.token_key:
            p.append(f"LOGIN_KEYS {k!r} is not a non-token env key of {entry.name!r}")
    for other in others.values():
        if other.name == entry.name:
            continue
        clash = set(entry.key_names) & set(other.key_names)
        if clash:
            p.append(f"env keys {sorted(clash)} also declared by {other.name!r}")
    return p


def load_entry(name: str, module: str | None = None) -> BrokerEntry:
    """Import one entry module and return its validated ENTRY (raises; no registry side effects)."""
    mod = importlib.import_module(module or ENTRY_MODULES[name])
    entry = getattr(mod, "ENTRY", None)
    if not isinstance(entry, BrokerEntry):
        raise CapabilityError(f"{mod.__name__}.ENTRY is missing or not a BrokerEntry")
    if entry.name != name:
        raise CapabilityError(f"{mod.__name__}.ENTRY.name is {entry.name!r}, expected {name!r}")
    return entry


def _load_all() -> tuple[dict[str, BrokerEntry], dict[str, str], tuple[str, ...]]:
    loaded: dict[str, BrokerEntry] = {}
    broken: dict[str, str] = {}
    missing: list[str] = []
    for name in KNOWN_BROKERS:
        module = ENTRY_MODULES[name]
        try:
            entry = load_entry(name, module)
        except ModuleNotFoundError as exc:
            if exc.name == module:  # the package itself is absent: not built yet / left out of the package
                missing.append(name)
                continue
            broken[name] = f"{type(exc).__name__}: {exc}"
            log.error("broker registry: %s failed to import: %s", name, broken[name])
            continue
        except Exception as exc:  # noqa: BLE001 -- a broken BETA entry must not stop the service
            broken[name] = f"{type(exc).__name__}: {exc}"
            log.error("broker registry: %s rejected: %s", name, broken[name])
            continue
        loaded[name] = entry
    for name in list(loaded):
        problems = registry_problems(loaded[name], loaded)
        if problems:
            broken[name] = "; ".join(problems)
            log.error("broker registry: %s rejected: %s", name, broken[name])
    brokers = {n: e for n, e in loaded.items() if n not in broken}
    return brokers, broken, tuple(missing)


BROKERS: dict[str, BrokerEntry]
BROKEN: dict[str, str]
MISSING: tuple[str, ...]
BROKERS, BROKEN, MISSING = _load_all()


def get(name: str) -> BrokerEntry:
    key = (name or "").strip().lower()
    if key in BROKERS:
        return BROKERS[key]
    if key in BROKEN:
        raise RegistryError(f"broker {key!r} is unavailable: {BROKEN[key]}")
    if key in KNOWN_BROKERS:
        raise RegistryError(f"broker {key!r} is not in this package")
    raise RegistryError(f"unknown broker {name!r}; one of: {', '.join(BROKERS)}")


def resolve(broker_value: str | None, edition: str | None = None) -> BrokerEntry:
    """`.env` BROKER -> entry. Absent/empty BROKER -> RegistryError in every edition (never guessed)."""
    _ = edition or _edition.edition()
    value = (broker_value or "").strip().lower()
    if not value:
        raise RegistryError(f"BROKER is not set in .env ({' | '.join(KNOWN_BROKERS)})")
    return get(value)


def all_env_keys() -> tuple[str, ...]:
    """BROKER + every registered broker's keys, in registry order (ENV_KEYS grows by these, spec §6)."""
    keys: list[str] = [BROKER_ENV_KEY]
    for entry in BROKERS.values():
        keys.extend(k for k in entry.key_names if k not in keys)
    return tuple(keys)


def creds_from_env(entry: BrokerEntry, values: Mapping[str, str]) -> dict[str, str]:
    """{key: value} for the entry's keys only; absent keys map to "" (values are secrets: never log them)."""
    return {k: (values.get(k) or "").strip() for k in entry.key_names}


def token_kinds(entry: BrokerEntry, edition: str | None = None) -> tuple[str, ...]:
    """Token lifecycle kinds the Token page may offer: the client edition is always paste-only (D86)."""
    ed = edition or _edition.edition()
    return entry.caps.token if ed == _edition.OWNER else CLIENT_TOKEN_KINDS


def required_keys(entry: BrokerEntry, edition: str | None = None) -> tuple[str, ...]:
    """Keys that must be non-empty to start. Same in both editions; auto_login keys are never required
    (they only enable the owner's automated login)."""
    del edition  # kept in the signature: the rule is edition-independent today, callers pass it anyway
    return tuple(k.name for k in entry.env_keys if k.need == NEED_REQUIRED)


def missing_credentials(entry: BrokerEntry, creds: Mapping[str, str], edition: str | None = None) -> list[str]:
    return [k for k in required_keys(entry, edition) if not (creds.get(k) or "").strip()]


def declared_login_keys(entry: BrokerEntry) -> tuple[str, ...]:
    """The entry package's `LOGIN_KEYS` (need=login, T-112): keys its EnvKey marks optional because the service
    starts without them, but that the Token-page login cannot run without. Only names the entry declares count."""
    mod = sys.modules.get(ENTRY_MODULES.get(entry.name, ""))
    return tuple(k for k in (getattr(mod, "LOGIN_KEYS", ()) or ()) if k in entry.key_names and k != entry.token_key)


def need_of(entry: BrokerEntry, key: str) -> str:
    """The need Check Setup shows for one key: NEED_LOGIN for a declared login key, else the EnvKey's own."""
    return NEED_LOGIN if key in declared_login_keys(entry) else entry.key(key).need


def login_keys(entry: BrokerEntry, edition: str | None = None) -> tuple[str, ...]:
    """Keys that must be non-empty BEFORE the first login / token paste: every required key except the token
    (the login writes it) plus the declared need=login keys. The runtime start rule (required_keys) is unchanged."""
    keys = [k for k in required_keys(entry, edition) if k != entry.token_key]
    keys.extend(k for k in declared_login_keys(entry) if k not in keys)
    return tuple(keys)


def first_token_path(entry: BrokerEntry, edition: str | None = None) -> str | None:
    """How a client gets the FIRST token with the app: "login" (Token-page client_login), "paste" (Token-page
    paste), or None (no path: the token must be filled into .env by hand)."""
    if entry.client_login is not None:
        return "login"
    return "paste" if "paste" in token_kinds(entry, edition) else None


def first_day_missing(entry: BrokerEntry, creds: Mapping[str, str], edition: str | None = None) -> list[str]:
    """Day one (no token yet): the login keys still empty. [] when the token is present -- then the login keys
    are not needed and missing_credentials is the whole rule."""
    if (creds.get(entry.token_key) or "").strip():
        return []
    return [k for k in login_keys(entry, edition) if not (creds.get(k) or "").strip()]


def can_auto_login(entry: BrokerEntry, creds: Mapping[str, str], edition: str | None = None) -> bool:
    """True only in the owner edition, for an entry with auto_login whose auto_login keys are all set."""
    ed = edition or _edition.edition()
    if ed != _edition.OWNER or entry.auto_login is None:
        return False
    need = [k.name for k in entry.env_keys if k.need == NEED_AUTO_LOGIN]
    return bool(need) and all((creds.get(k) or "").strip() for k in need)


def auto_login(entry: BrokerEntry, creds: Mapping[str, str], **kwargs: Any) -> dict[str, str]:
    """OWNER EDITION ONLY: run the entry's automated login -> new credentials {env key: value}.
    Raises edition.OwnerOnlyError in the client edition BEFORE the entry is touched."""
    _edition.require_owner(f"automated {entry.name} login")
    if entry.auto_login is None:
        raise RegistryError(f"{entry.name} has no automated login")
    return entry.auto_login(dict(creds), **kwargs)


def describe(entry: BrokerEntry, edition: str | None = None) -> dict[str, Any]:
    """The `/api/status.broker` block (no secrets)."""
    from gammad.broker.capabilities import assess

    scanner = assess(entry.caps).to_dict()
    extra = runtime_reasons(entry)
    if extra:  # a runtime switch-off (D726 fix round F1): the status shows what the scanner can really do
        scanner.update(mode="BLOCKED", level="red", reasons=[*extra, *scanner["reasons"]])
    return {
        "name": entry.name,
        "label": entry.label,
        "status": entry.caps.status,
        "beta": entry.caps.status != "live-verified",
        "token_kinds": list(token_kinds(entry, edition)),
        "token_key": entry.token_key,
        "token_expiry_rule": entry.caps.token_expiry_rule,
        "capabilities": entry.caps.to_dict(),
        "scanner": scanner,
    }


def runtime_reasons(entry: BrokerEntry, data_dir: Any = None) -> tuple[str, ...]:
    """Scanner lines from what the broker SENT today, beyond its declaration: the entry module's optional
    `runtime_reasons(data_dir)` (Noren: volume not in shares, D726). `/api/status` asks this process; Check Setup
    passes the data folder so it sees a running service's switch-off. Never raises."""
    fn = getattr(sys.modules.get(ENTRY_MODULES.get(entry.name, "")), "runtime_reasons", None)
    if not callable(fn):
        return ()
    try:
        return tuple(str(r) for r in fn(data_dir))
    except Exception:  # status must render
        log.exception("broker %s runtime_reasons failed", entry.name)
        return ()


def setup_notes(entry: BrokerEntry) -> tuple[str, ...]:
    """Plain Check Setup notes the entry module declares as `SETUP_NOTES` (Shoonya's slow first lists, D726)."""
    notes = getattr(sys.modules.get(ENTRY_MODULES.get(entry.name, "")), "SETUP_NOTES", ()) or ()
    return tuple(str(n) for n in notes)
