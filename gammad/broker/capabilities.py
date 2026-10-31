"""Broker capability declaration, registry entry types, and the scanner's reaction (BROKER-ADAPTER-SPEC §4, §8).

Every broker ENTRY (`gammad/broker/<b>/__init__.py`, Dhan: `gammad/broker/dhan_entry.py`) declares one frozen
`Capabilities` and one `BrokerEntry`. This module imports no broker and no auth code, so an entry module can
import it without a cycle and without pulling owner-only login code into a client process (D86).

`assess(caps) -> ScannerMode` is the ONLY place that turns a declaration into behaviour:

    BLOCKED   a condition input the engine cannot run without is missing -> no engines, no signals; levels and
              watchlists still build. Red banner "scanner idle: <broker> ...".
                - feed_oi == "none"      (condition A needs live OI)
                - feed_volume == "none"  (condition B needs live volume)
                - daily_history False    (mark_levels fails -> no lists)
                - chain == "none" or expiries == "none" (build_watchlists cannot pick strikes)
    DEGRADED  OI or volume arrives by REST polling: the scanner runs; amber banner "OI/volume polled every N s --
              an alert can come up to N s late; candle highs/lows are sampled"; flag feed_poll_s = N.
    FULL      every input the engine reads is native.

Flags (any mode; the seam reads them, PLAN-W12 §2 item 2):
    feed_poll_s=N          record ctx_json.feed_poll_s on every signal (DEGRADED)
    warmup_oi="quote"      candle_oi False: prime OiCarry from one quotes() snapshot before fetch_warmup
    warmup=False           candle_volume False or intraday_history False: no warm-up (D24 off), signals from the
                           20th live bar (~10:55)
    feed_ltt=False         ticks are stamped at receive time
    chain="master+quotes"  ChainRows built from the canonical master + one quote batch (iv, previous_oi None)

Declaration rule: a capability the broker does not DOCUMENT is declared "none" / False. Unknown values and
contradictions raise `CapabilityError` at construction -- the app refuses rather than guesses.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from typing import Any

STATUS = ("live-verified", "beta", "beta-unverified")
CHAIN = ("native", "master+quotes", "none")
EXPIRIES = ("native", "master", "none")
FEED = ("ws", "poll", "ws+poll")
FEED_FIELD = ("native", "poll", "none")
TOKEN_KINDS = ("paste", "oauth", "totp")
CHAIN_ONE = "1"  # one request returns the whole chain (a native chain endpoint)
CHAIN_PER_BATCH = "per_batch"  # master+quotes: ceil(legs / chain_batch) quote requests
CHAIN_PER_LEG = "per_leg"  # master+quotes with no batch quote: one request per leg (Nuvama)
CHAIN_CALLS = (CHAIN_ONE, CHAIN_PER_BATCH, CHAIN_PER_LEG)

MODE_FULL = "FULL"
MODE_DEGRADED = "DEGRADED"
MODE_BLOCKED = "BLOCKED"
MODES = (MODE_FULL, MODE_DEGRADED, MODE_BLOCKED)
_LEVEL = {MODE_FULL: "ok", MODE_DEGRADED: "amber", MODE_BLOCKED: "red"}

NEED_REQUIRED = "required"  # both editions; the client pastes it
NEED_OPTIONAL = "optional"
NEED_AUTO_LOGIN = "auto_login"  # read only by owner-edition automated login (TOTP secret, PIN, API secret)
NEEDS = (NEED_REQUIRED, NEED_OPTIONAL, NEED_AUTO_LOGIN)

# Keys the core owns (gammad/config.py ENV_KEYS + BROKER); no broker block may declare one.
CORE_ENV_KEYS = frozenset(
    {
        "BROKER",
        "TELEGRAM_BOT_TOKEN",
        "TELEGRAM_GROUP_ID",
        "TELEGRAM_CHAT_ID",
        "TELEGRAM_PERSONAL_CHAT_ID",
        "TELEGRAM_TOPIC_ID",
        "GAMMA_PORT",
        "GAMMA_TZ",
        "GAMMA_LAN",  # T-129 (PLAN-W16 13.3 B1)
        "GAMMA_LAN_PASSWORD",
    }
)
_KEY_RE = re.compile(r"[A-Z][A-Z0-9_]*")
_NAME_RE = re.compile(r"[a-z][a-z0-9]*")


class CapabilityError(ValueError):
    """A capability declaration or registry entry is malformed (all problems listed in the message)."""


@dataclass(frozen=True)
class Capabilities:
    broker: str
    status: str  # STATUS
    chain: str  # CHAIN
    expiries: str  # EXPIRIES
    candle_oi: bool
    candle_volume: bool
    daily_history: bool
    feed: str  # FEED
    feed_oi: str  # FEED_FIELD
    feed_volume: str  # FEED_FIELD
    feed_ltt: bool
    poll_s: float | None
    max_feed_instruments: int
    token: tuple[str, ...]  # owner-edition lifecycle kinds, default first; the client edition is always paste
    token_expiry_rule: str
    data_cost: str
    evidence: tuple[str, ...]
    intraday_history: bool = True
    # 0.8.5 (adapter audit D14): the request COST of the calls the 0.8.5 day makes, as data read from each adapter's
    # own constants in the entry (tests/test_t085_adapters_caps.py pins every value to that constant and to the
    # requests the adapter really sends). Declaration only: nothing in the service branches on these yet.
    ltp_batch: int | None = None  # ids per ltp() request (a halving batcher's first try, batching.py); None = undeclared
    chain_calls: str | None = None  # CHAIN_CALLS: requests per option chain -- see chain_calls_for()
    chain_batch: int | None = None  # legs per request when chain_calls == "per_batch" (master+quotes batchers)
    daily_quota: int | None = None  # documented requests per day; None = no daily quota is documented (unknown)

    def ltp_calls_for(self, n: int) -> int | None:
        """Requests one ltp() of `n` ids sends (no failures, no 400 halving); None when ltp_batch is undeclared."""
        if self.ltp_batch is None:
            return None
        return -(-max(0, int(n)) // self.ltp_batch)

    def chain_calls_for(self, legs: int) -> int | None:
        """Requests one option_chain() of `legs` legs (strikes x CE/PE) sends, expiry lookups excluded."""
        if self.chain_calls == CHAIN_ONE:
            return 1
        if self.chain_calls == CHAIN_PER_LEG:
            return max(0, int(legs))
        if self.chain_calls == CHAIN_PER_BATCH and self.chain_batch:
            return -(-max(0, int(legs)) // self.chain_batch)
        return None

    def __post_init__(self) -> None:
        problems = _caps_problems(self)
        if problems:
            raise CapabilityError(f"capabilities for {self.broker!r}: " + "; ".join(problems))

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["token"] = list(self.token)
        d["evidence"] = list(self.evidence)
        return d


def _caps_problems(c: Capabilities) -> list[str]:
    p: list[str] = []
    if not isinstance(c.broker, str) or not _NAME_RE.fullmatch(c.broker):
        p.append(f"broker {c.broker!r} must be a lowercase registry name")
    for fname, allowed in (
        ("status", STATUS),
        ("chain", CHAIN),
        ("expiries", EXPIRIES),
        ("feed", FEED),
        ("feed_oi", FEED_FIELD),
        ("feed_volume", FEED_FIELD),
    ):
        if getattr(c, fname) not in allowed:
            p.append(f"{fname}={getattr(c, fname)!r} not in {allowed}")
    for fname in ("candle_oi", "candle_volume", "daily_history", "feed_ltt", "intraday_history"):
        if not isinstance(getattr(c, fname), bool):
            p.append(f"{fname} must be a bool")
    ws = c.feed in ("ws", "ws+poll")
    poll = c.feed in ("poll", "ws+poll")
    for fname in ("feed_oi", "feed_volume"):
        v = getattr(c, fname)
        if v == "native" and not ws:
            p.append(f"{fname}='native' needs a ws feed (feed={c.feed!r})")
        if v == "poll" and not poll:
            p.append(f"{fname}='poll' needs feed 'poll' or 'ws+poll' (feed={c.feed!r})")
    if poll:
        if isinstance(c.poll_s, bool) or not isinstance(c.poll_s, int | float) or c.poll_s <= 0:
            p.append(f"feed {c.feed!r} needs poll_s > 0 (got {c.poll_s!r})")
    elif c.poll_s is not None:
        p.append(f"poll_s must be None for feed {c.feed!r}")
    if isinstance(c.max_feed_instruments, bool) or not isinstance(c.max_feed_instruments, int):
        p.append("max_feed_instruments must be an int")
    elif c.max_feed_instruments <= 0:
        p.append("max_feed_instruments must be > 0")
    if not isinstance(c.token, tuple) or not c.token:
        p.append("token must be a non-empty tuple of lifecycle kinds")
    else:
        bad = [k for k in c.token if k not in TOKEN_KINDS]
        if bad:
            p.append(f"token kinds {bad} not in {TOKEN_KINDS}")
        if len(set(c.token)) != len(c.token):
            p.append("token kinds repeat")
    for fname in ("token_expiry_rule", "data_cost"):
        if not isinstance(getattr(c, fname), str) or not getattr(c, fname).strip():
            p.append(f"{fname} must be a non-empty string")
    if not isinstance(c.evidence, tuple) or not c.evidence or not all(isinstance(e, str) and e for e in c.evidence):
        p.append("evidence must be a non-empty tuple of research row ids / decision ids")
    for fname in ("ltp_batch", "chain_batch", "daily_quota"):
        v = getattr(c, fname)
        if v is not None and (isinstance(v, bool) or not isinstance(v, int) or v <= 0):
            p.append(f"{fname} must be None or an int > 0 (got {v!r})")
    if c.chain_calls is not None and c.chain_calls not in CHAIN_CALLS:
        p.append(f"chain_calls={c.chain_calls!r} not in {CHAIN_CALLS}")
    if (c.chain_calls == CHAIN_PER_BATCH) != (c.chain_batch is not None):
        p.append("chain_batch is required with chain_calls='per_batch' and only then")
    return p


# ------------------------------------------------------------------------------------------ scanner mode
@dataclass(frozen=True)
class ScannerMode:
    mode: str  # MODES
    reasons: tuple[str, ...]  # banner lines, most severe first
    flags: dict[str, Any] = field(default_factory=dict)

    @property
    def engines_allowed(self) -> bool:
        return self.mode != MODE_BLOCKED

    @property
    def level(self) -> str:
        return _LEVEL[self.mode]

    def to_dict(self) -> dict[str, Any]:
        """The `/api/status.broker.scanner` block."""
        return {"mode": self.mode, "level": self.level, "reasons": list(self.reasons), "flags": dict(self.flags)}


def assess(caps: Capabilities) -> ScannerMode:
    """Declaration -> scanner mode + flags (module docstring). Pure; never raises for a valid Capabilities."""
    b = caps.broker
    blocked: list[str] = []
    degraded: list[str] = []
    notes: list[str] = []
    flags: dict[str, Any] = {}
    # P2-CLIENT-DOCS-06 (D429 (2), D628): these lines reach the client through Check Setup (smoke_broker.py prints
    # them) and its setup report, so they use the guide's words (section 3.4: the OI check, the volume check), never
    # an internal condition letter or signal kind
    if caps.feed_oi == "none":
        blocked.append(f"scanner idle: {b} sends no live OI (the OI check cannot be made)")
    if caps.feed_volume == "none":
        blocked.append(f"scanner idle: {b} sends no live volume (the volume check cannot be made)")
    if not caps.daily_history:
        blocked.append(f"scanner idle: {b} serves no daily history (levels cannot be marked, no watchlists)")
    if caps.chain == "none":
        blocked.append(f"scanner idle: {b} has no option chain (watchlists cannot pick strikes)")
    if caps.expiries == "none":
        blocked.append(f"scanner idle: {b} has no expiry source (watchlists cannot pick an expiry)")
    polled = [name for name, v in (("OI", caps.feed_oi), ("volume", caps.feed_volume)) if v == "poll"]
    if polled:
        n = f"{float(caps.poll_s or 0):g}"
        degraded.append(
            f"{'/'.join(polled)} polled every {n} s -- an alert can come up to {n} s late; "
            "candle highs/lows are sampled"
        )
        flags["feed_poll_s"] = caps.poll_s
    if not caps.candle_oi:
        flags["warmup_oi"] = "quote"
        notes.append(f"{b} candles carry no OI: warm-up OI primed from one quote snapshot")
    if not caps.candle_volume or not caps.intraday_history:
        flags["warmup"] = False
        # audit P2 brokers-guide (D726, D114 (1)): Shoonya / Flattrade print this to the client; "intraday" is a word
        # the client rules forbid (forbidden.json INTRADAY), so the line says "history candles"
        notes.append(f"{b} has no usable history candles: no warm-up, signals from the 20th live bar (~10:55)")
    if not caps.feed_ltt:
        flags["feed_ltt"] = False
        notes.append(f"{b} feed carries no trade time: ticks stamped at receive time")
    if caps.chain == "master+quotes":
        flags["chain"] = "master+quotes"
    mode = MODE_BLOCKED if blocked else MODE_DEGRADED if degraded else MODE_FULL
    return ScannerMode(mode, tuple(blocked + degraded + notes), flags)


# ------------------------------------------------------------------------------------------ entry types
@dataclass(frozen=True)
class EnvKey:
    """One `.env` key of a broker block (BROKER-ADAPTER-SPEC §6). Values are secrets unless secret=False."""

    name: str
    need: str = NEED_REQUIRED  # NEEDS
    secret: bool = True
    written: bool = False  # the app writes it (Token page paste, OAuth callback, automated login)
    note: str = ""  # one-line comment for the generated .env template


@dataclass(frozen=True)
class OAuthHooks:
    """OWNER-EDITION ONLY broker-specific halves of the redirect login; `gammad/auth/oauth.py` drives them.

    login_url(creds, state, redirect_uri) -> the broker's authorize URL carrying `state` back to the callback.
    exchange(creds, query, redirect_uri, *, transport=None) -> new credentials {env key: value}, at least the
    entry's token_key. Implementations import `gammad.auth.oauth` helpers INSIDE the function, never at module
    import (the entry module is imported in the client edition).
    """

    login_url: Callable[..., str]
    exchange: Callable[..., dict[str, str]]


CLIENT_LOGIN_FIELDS = ("redirect_url", "totp_code", "pin")


@dataclass(frozen=True)
class ClientLogin:
    """ONE-SHOT login from what the user pasted or typed on the Token page / in .env (D89(1), D90(1), D92).

    No redirect server, no `state`, no stored secret, nothing scheduled: allowed in the CLIENT edition.
    fields  the inputs the Token page asks for, from CLIENT_LOGIN_FIELDS: ("redirect_url",) = the redirect URL
            (or bare request/auth code) after a MANUAL browser login (Zerodha, Upstox, Fyers, Nuvama);
            ("totp_code", "pin") = today's 6-digit authenticator code + PIN/MPIN typed by the user (Angel One,
            Kotak Neo, Groww).
    login(creds, fields, *, transport=None) -> new credentials {env key: value}, at least the entry's token_key.
            The pasted/typed values are used once: never returned, logged or written to .env.
    login_url(creds) -> the broker login page the user opens by hand, or None.
    """

    fields: tuple[str, ...]
    login: Callable[..., dict[str, str]]
    login_url: Callable[..., str | None] | None = None


@dataclass(frozen=True)
class BrokerEntry:
    """What the registry knows about one broker.

    make_adapter(creds, data_dir, *, transport=None, clock=None, sleep=None) -> BrokerAdapter
        creds = {env key: value}; transport = an httpx transport (the harness injects httpx.MockTransport);
        clock/sleep drive the adapter's TokenBuckets (the harness never sleeps). Raises BrokerAuthError when a
        required credential is missing.
    make_feed(creds, out, on_auth_error, *, adapter=None, connect=None, clock_ms=None) -> Feed
        (gammad/watch/feed_protocol.py). connect = the WS Connector injection point (feed.py:340); a poll feed
        reuses `adapter` so it shares the adapter's rate buckets.
    auto_login(creds, *, transport=None, now=None) -> {env key: value}   OWNER EDITION ONLY (TOTP etc., D86);
        reached only through gammad.broker.registry.auto_login, which calls edition.require_owner first.
    oauth: OAuthHooks | None                                           OWNER EDITION ONLY.
    client_login: ClientLogin | None                                   BOTH editions (D92 paste/typed flows).
    """

    name: str
    label: str
    caps: Capabilities
    env_keys: tuple[EnvKey, ...]
    token_key: str
    make_adapter: Callable[..., Any]
    make_feed: Callable[..., Any]
    doc_url: str
    auto_login: Callable[..., dict[str, str]] | None = None
    oauth: OAuthHooks | None = None
    client_login: ClientLogin | None = None

    def __post_init__(self) -> None:
        problems = entry_problems(self)
        if problems:
            raise CapabilityError(f"entry {self.name!r}: " + "; ".join(problems))

    def key(self, name: str) -> EnvKey:
        for k in self.env_keys:
            if k.name == name:
                return k
        raise KeyError(name)

    @property
    def key_names(self) -> tuple[str, ...]:
        return tuple(k.name for k in self.env_keys)


def entry_problems(e: BrokerEntry) -> list[str]:
    """Self-consistency of one entry (cross-entry checks live in the registry)."""
    p: list[str] = []
    if not isinstance(e.caps, Capabilities):
        return ["caps must be a Capabilities"]
    if e.name != e.caps.broker:
        p.append(f"name {e.name!r} != caps.broker {e.caps.broker!r}")
    if not e.label.strip():
        p.append("label is empty")
    if not e.doc_url.startswith("https://"):
        p.append("doc_url must be an https URL")
    names = [k.name for k in e.env_keys]
    if not names:
        p.append("env_keys is empty")
    if len(set(names)) != len(names):
        p.append("env key names repeat")
    for k in e.env_keys:
        if not _KEY_RE.fullmatch(k.name):
            p.append(f"env key {k.name!r} is not UPPER_SNAKE")
        if k.name in CORE_ENV_KEYS:
            p.append(f"env key {k.name!r} belongs to the core")
        if k.need not in NEEDS:
            p.append(f"env key {k.name!r} need={k.need!r} not in {NEEDS}")
    if e.token_key not in names:
        p.append(f"token_key {e.token_key!r} is not one of env_keys")
    else:
        tk = e.key(e.token_key)
        if tk.need != NEED_REQUIRED or not tk.written or not tk.secret:
            p.append(f"token_key {e.token_key!r} must be need=required, written=True, secret=True (paste target)")
    for fname in ("make_adapter", "make_feed"):
        if not callable(getattr(e, fname)):
            p.append(f"{fname} is not callable")
    if "totp" in e.caps.token and e.auto_login is None:
        p.append("caps.token has 'totp' but auto_login is None")
    if "oauth" in e.caps.token and e.oauth is None:
        p.append("caps.token has 'oauth' but oauth hooks are None")
    if e.auto_login is not None and not any(k.need == NEED_AUTO_LOGIN for k in e.env_keys):
        p.append("auto_login is set but no env key has need='auto_login'")
    if e.client_login is not None:
        cl = e.client_login
        if not cl.fields or any(f not in CLIENT_LOGIN_FIELDS for f in cl.fields):
            p.append(f"client_login.fields {cl.fields!r} must be a non-empty subset of {CLIENT_LOGIN_FIELDS}")
        if not callable(cl.login):
            p.append("client_login.login is not callable")
    return p
