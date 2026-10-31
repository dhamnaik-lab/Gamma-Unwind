"""Live smoke for one broker (BROKER-ADAPTER-SPEC §10; CLIENT-PACKAGE-SPEC §5 `--doctor`). READ-ONLY calls only.

usage (from the package root):
  python scripts/smoke_broker.py --list                  registry table -- no .env read, no network
  python scripts/smoke_broker.py [--broker B] [--no-feed] [--feed-seconds 60] [--out DIR]
  python scripts/smoke_broker.py --doctor [--broker B]   setup check behind "Check Setup.cmd" / ./check.sh

Default run: token_health; ltp for 2 underlyings (RELIANCE, HDFCBANK); expiries + option chain for RELIANCE;
5-min intraday of the chain's highest-OI contract for the previous session; ~30 daily bars; feed for N seconds on 2
contracts + 1 underlying in market hours (handshake + auth only otherwise). Every chain row is run through the
canonical identity check (gammad/broker/canonical.py). Writes <out>/smoke-<broker>-<date>.json (declared vs observed
capabilities, identity verdict), prints a table, exits 0 only when every declared capability was observed
(capabilities that cannot be observed outside market hours are reported "n/a", not failed).
--doctor: the BROKER block of .env (the client edition hides the owner-only auto_login keys, D86), the dashboard
port (busy by another program -> noted, the other checks still run, then exit 4, D31 / T-107; busy by a running
GAMMA -> ok with its version, D100), the Telegram
T-062 verdict (config.telegram_check; bot token present; from the command line also getMe + getChat(group)
over the network, --offline skips them), token health + one LTP (the data plan, P4), capability table + scanner
mode.
Exit: 0 ok | 1 a declared capability not observed or a call failed | 2 configuration | 4 port busy.
Values are never printed -- key names, sources and masked tokens only (D54).
--check-setup [--no-fix]: "Check Setup.cmd" / ./check.sh (D104) -- the --doctor, then the first-time-setup checks
(install folder, program files, Windows download mark, writable folder, .env, port, left-over stop files, recent
errors) and logs/setup-report-<stamp>.txt + .json. Fixes run only after ONE Y answer. Exit: 0 all PASS | 2 a
configuration / fixable problem remains | 4 port held by another program (D31) | 1 anything else.
T-159 (D125 K3): after a valid token the doctor sends ONE price request of exactly the broker's batch size (real
NSE_EQ ids from the F&O universe) and reports the size that worked, then -- on a trading day -- the full-universe
ltp() the 09:20 mark_levels job sends (count returned vs asked). Check Setup adds "modified files" (every
MANIFEST.json path's sha256 vs the install; skipped in a repo checkout).
--bundle <zip> (T-150(e), D125 L2): ONE support zip -- logs/gammad.log tail (2000 lines), the runs rows of the last 5
run days, watchlist counts per run_id, the modified-files list and the .env KEY NAMES (never a value); B6 adds
data/remote_seen.json as host + count only when it exists. T-181 F8: telegram_sends.json = per-day manual_sends /
alerts counts (kind or channel, dest, status, the first error) of the last 5 days that sent anything (read-only).
B6 (PLAN-W16 13.3): the online Telegram check WARNs a forum group with a blank TELEGRAM_TOPIC_ID; Check Setup's
`remote` check WARNs data/remote_seen.json without GAMMA_LAN_PASSWORD; GAMMA_LAN=1 without GAMMA_LAN_PASSWORD is an
env_keys FAIL (exit 2); the leftovers fix never deletes data/install.json, remote_seen.json or lan_secret.
"""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import hashlib
import importlib
import json
import os
import platform
import re
import shutil
import socket
import sqlite3
import sys
import tempfile
import urllib.request
import zipfile
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from datetime import timedelta
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

GAMMAD_IMPORT_ERROR: str | None = None
try:  # Check Setup must still report on a package whose program files are damaged (D104)
    from gammad import edition
    from gammad.broker import registry
    from gammad.broker.base import BrokerAuthError, BrokerEntitlementError, BrokerError
    from gammad.broker.capabilities import BrokerEntry, assess
    from gammad.config import mask, now_ist, telegram_check
except Exception as _exc:  # noqa: BLE001
    GAMMAD_IMPORT_ERROR = f"{type(_exc).__name__}: {_exc}"

UNDERLYINGS = (("RELIANCE", 2885), ("HDFCBANK", 1333))
EXIT_OK, EXIT_FAIL, EXIT_CONFIG, EXIT_PORT = 0, 1, 2, 4
TELEGRAM_KEYS = ("TELEGRAM_BOT_TOKEN", "TELEGRAM_GROUP_ID", "TELEGRAM_CHAT_ID", "TELEGRAM_PERSONAL_CHAT_ID")
TOPIC_KEY = "TELEGRAM_TOPIC_ID"  # D131(2): a forum group's topic; blank = General (B6 forum WARN)
LAN_KEY, LAN_PASSWORD_KEY = "GAMMA_LAN", "GAMMA_LAN_PASSWORD"  # PLAN-W16 13.3 B1 contract (0/1; the login password)
LAN_PASSWORD_MIN_LEN = 12
LAN_ON_VALUES = frozenset({"1", "true", "yes", "on"})  # = gammad.config.LAN_ON_VALUES (B1 parses the same set)
TELEGRAM_TRANSPORT: Any = None  # P1: tests hand telegram_online() an httpx.MockTransport here (None = network)


def read_env(root: Path) -> dict[str, str]:
    """D54: a non-empty .env value wins; the process environment is the fallback. Values stay in memory."""
    from dotenv import dotenv_values

    file_vals = {k: (v or "").strip() for k, v in dotenv_values(root / ".env").items()} if (root / ".env").exists() else {}
    keys = set(registry.all_env_keys()) | {"GAMMA_PORT", *TELEGRAM_KEYS, TOPIC_KEY, LAN_KEY, LAN_PASSWORD_KEY}
    return {k: file_vals.get(k) or os.environ.get(k, "").strip() for k in keys}


def print_table(rows: list[tuple[str, ...]]) -> None:
    widths = [max(len(str(r[i])) for r in rows) for i in range(len(rows[0]))]
    for r in rows:
        print("  ".join(str(c).ljust(w) for c, w in zip(r, widths, strict=False)))


def cmd_list() -> int:
    rows = [("broker", "status", "scanner", "token (owner)", "token (client)", "feed")]
    for name, e in registry.BROKERS.items():
        rows.append((name, e.caps.status, assess(e.caps).mode, "+".join(e.caps.token),
                     "+".join(registry.token_kinds(e, edition.CLIENT)), e.caps.feed))
    for name, why in registry.BROKEN.items():
        rows.append((name, "BROKEN", "-", "-", "-", why[:60]))
    for name in registry.MISSING:
        rows.append((name, "not in package", "-", "-", "-", "-"))
    print_table(rows)
    return EXIT_OK


def resolve(args: argparse.Namespace, env: dict[str, str], results: list[dict[str, str]] | None = None
            ) -> BrokerEntry | None:  # fmt: skip
    try:
        return registry.resolve(args.broker or env.get("BROKER"), edition.edition())
    except registry.RegistryError as exc:
        print(f"CONFIG: {exc}")
        if results is not None:
            results.append({"id": "broker", "status": "FAIL", "text": str(exc)})
        return None


def port_busy(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.bind(("127.0.0.1", port))
        except OSError:
            return True
    return False


def running_gamma(port: int) -> dict[str, Any] | None:
    """GAMMA's own /api/status on a busy port, else None (another program holds it). Two GAMMA answers: the
    service ({version, edition}) and, while the service is stopped, the tray's stopped page ({supervisor: true},
    gammad/tray/stopped_page.py) -- the second one is GAMMA too, never "another program" (D104)."""
    try:
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))  # a system proxy must not answer
        with opener.open(f"http://127.0.0.1:{port}/api/status", timeout=3.0) as r:
            body = json.loads(r.read(65536).decode("utf-8", errors="replace"))
    except (OSError, ValueError):
        return None
    if isinstance(body, dict) and body.get("version") and body.get("edition"):
        return body
    if isinstance(body, dict) and body.get("supervisor") is True:
        return {"version": "tray", "edition": f"service {body.get('service') or 'stopped'}", "supervisor": True}
    return None


def cmd_doctor(args: argparse.Namespace, results: list[dict[str, str]] | None = None) -> int:
    """The setup check. `results` (Check Setup's report, D104) receives one {id, status, text} per verdict
    printed; the texts are the printed ones (key names, never values)."""
    res: list[dict[str, str]] = results if results is not None else []

    def note(cid: str, status: str, text: str) -> None:
        res.append({"id": cid, "status": status, "text": text})

    env = read_env(ROOT)
    entry = resolve(args, env, res)
    if entry is None:
        return EXIT_CONFIG
    creds = registry.creds_from_env(entry, env)
    print(f"edition {edition.edition()}   broker {entry.name} ({entry.caps.status})")
    rows = [("key", "need", "state")]
    for k in entry.env_keys:
        if k.need == "auto_login" and edition.is_client():
            continue  # owner-only keys (D86): never shown to a client
        rows.append((k.name, registry.need_of(entry, k.name),
                     mask(creds[k.name]) if k.secret and creds[k.name] else creds[k.name] or "-"))  # fmt: skip
    print_table(rows)
    missing = registry.missing_credentials(entry, creds)
    # T-112: day one has no token yet. With a Token-page path (login or paste) the token is not a setup failure:
    # the keys that path needs are (need=login included); without a path the token key itself stays missing.
    first_path = registry.first_token_path(entry, edition.edition())
    no_token = entry.token_key in missing and first_path is not None
    if no_token:
        missing = registry.first_day_missing(entry, creds, edition.edition())
    port = int(env.get("GAMMA_PORT") or 8760)
    code = EXIT_OK
    foreign_port = False  # T-107: noted, the checks that need no port still run; exit 4 outranks the rest

    def done(rc: int) -> int:
        """exit_code()'s precedence (T-107): 4 foreign port > 2 fixable configuration > 1 other > 0."""
        found = {rc, code} | ({EXIT_PORT} if foreign_port else set())
        return next((c for c in (EXIT_PORT, EXIT_CONFIG, EXIT_FAIL) if c in found), EXIT_OK)

    if missing:
        why = " (needed to log in on the Broker login page)" if no_token else ""
        if entry.token_key in missing and first_path is None:
            why = f" (fill in {entry.token_key}: {entry.name} has no login or paste on the Broker login page)"
        print(f"FAIL  .env is missing {', '.join(missing)}{why}")
        note("env_keys", "FAIL", f".env is missing {', '.join(missing)}{why}")
        code = EXIT_CONFIG
    elif no_token:
        note("env_keys", "PASS", f"every key {entry.name} needs before its first login is filled in")
    else:
        note("env_keys", "PASS", f"every key {entry.name} needs is filled in")
    lan_text = lan_problem(env)
    if lan_text is not None:
        print(f"FAIL  {lan_text}")
        note("env_keys", "FAIL", lan_text)
        code = EXIT_CONFIG
    gamma_up = False  # skeptic M2: the service runs (its /api/status answered on GAMMA_PORT)
    if port_busy(port):
        running = running_gamma(port)
        gamma_up = service_up(running)
        if running is None:
            print(f"FAIL  port {port} is busy (another program is using it)")
            note("port", "FAIL", f"port {port} is busy (another program is using it)")
            foreign_port = True
        else:
            print(f"ok    port {port}: GAMMA {running['version']} ({running['edition']}) is running on it")
    else:
        print(f"ok    port {port} free")
    tg = telegram_check(env.get("TELEGRAM_GROUP_ID") or env.get("TELEGRAM_CHAT_ID"),
                        env.get("TELEGRAM_PERSONAL_CHAT_ID"), edition=edition.edition())  # fmt: skip
    tg_problems = list(tg.problems) + ([] if env.get("TELEGRAM_BOT_TOKEN") else ["TELEGRAM_BOT_TOKEN is not set"])
    for p in tg_problems:
        print(f"FAIL  telegram: {p}")
        note("telegram", "FAIL", p)
    if tg_problems:
        code = EXIT_CONFIG
    else:
        personal = "set" if tg.personal_set else "not set (token/system messages on the dashboard only)"
        verdict, said = "PASS", "bot token set, group id ok"
        if getattr(args, "online", False):  # P1: the real Check Setup asks Telegram itself (--offline skips)
            verdict, said = telegram_online(env.get("TELEGRAM_BOT_TOKEN", ""),
                                            env.get("TELEGRAM_GROUP_ID") or env.get("TELEGRAM_CHAT_ID") or "",
                                            topic=env.get(TOPIC_KEY, ""))  # fmt: skip
        if verdict == "FAIL":
            print(f"FAIL  telegram: {said}")
            note("telegram", "FAIL", said)
            code = EXIT_CONFIG
        else:
            print(f"{'ok  ' if verdict == 'PASS' else 'WARN'}  telegram: {said}, personal chat {personal}")
            note("telegram", verdict, f"{said}, personal chat {personal}")
    scan = assess(entry.caps)
    print(f"scanner {scan.mode}: " + ("; ".join(scan.reasons) or "all inputs native"))
    for line in registry.runtime_reasons(entry, ROOT / "data"):  # D726 fix round F1: a running service's switch-off
        print(f"WARN  scanner: {line}")
        note("scanner", "WARN", line)
    for line in registry.setup_notes(entry):  # D726: a broker's own plain caution (Shoonya: slow first lists)
        print(f"WARN  broker: {line}")
        note("broker_note", "WARN", line)
    if missing:
        note("token", "WARN", "not checked: fill in the broker keys first")
        return done(code)
    if no_token:
        how = "log in" if first_path == "login" else "paste it"
        text = f"no {entry.token_key} yet: {how} on the Broker login page after Start (Setup Guide: the Broker login page)"
        print(f"WARN  token: {text}")
        note("token", "WARN", text)
        return done(code)
    try:
        adapter = entry.make_adapter(creds, ROOT / "data")
        th = adapter.token_health()
    except BrokerError as exc:
        print(f"FAIL  token: {exc}")
        note("token", "FAIL", str(exc))
        return done(EXIT_FAIL)
    problem = getattr(th, "problem", None)  # 0.8.3 T-229 (3): token_health's own price check was refused (D230 hint)
    if th.valid and problem:
        print(f"FAIL  token: {problem}")
        note("token", "FAIL", problem)
        return done(EXIT_FAIL)
    if th.valid:
        data_fail = data_plan_problem(entry.name, adapter)  # P4: the profile passes without a data plan
        if data_fail is not None:
            status, text = data_fail
            print(f"{status:<6}token: {text}")
            note("token", status, text)
            return done(code if status == "WARN" else EXIT_FAIL)
    print(("ok    " if th.valid else "FAIL  ") + f"token valid={th.valid} until={th.validity_ist} {th.error or ''}")
    note("token", "PASS" if th.valid else "FAIL",
         f"valid until {th.validity_ist}" if th.valid else f"not valid {th.error or ''}".strip())  # fmt: skip
    if th.valid and entry.name == "kotak" and kotak_session_check(creds, note, gamma_running=gamma_up):
        return done(EXIT_FAIL)
    if th.valid and doctor_batch_steps(entry.name, adapter, note):
        return done(EXIT_FAIL)
    return done(code if th.valid else EXIT_FAIL)


def service_up(running: dict[str, Any] | None) -> bool:
    """running_gamma()'s answer -> the GAMMA SERVICE is up (its /api/status answered). The tray's stopped page
    ({supervisor: true}) means the service is stopped; None = the port is free or another program holds it."""
    return running is not None and not running.get("supervisor")


def kotak_session_check(creds: dict[str, str], note: Callable[[str, str, str], None], *,
                        gamma_running: bool = False) -> bool:  # fmt: skip
    """0.8.3 T-229 (5): Kotak's own session probe (gammad.broker.kotak.feed.probe_session(creds, timeout=8.0) ->
    True / False / None = could not test), one line under the report id "token". Absent in an older package: no line.
    `gamma_running` (skeptic M2): the probe opens a second feed socket with the live session, which may drop the
    running service's feed -- never probed then. True when the session was REJECTED."""
    try:
        from gammad.broker.kotak.feed import probe_session
    except ImportError:
        return False
    if gamma_running:
        text = "Kotak session: not tested (GAMMA is running; stop it to test)"
        print(f"WARN  {text}")
        note("token", "WARN", text)
        return False
    try:
        ok = probe_session(dict(creds), timeout=8.0)
        if asyncio.iscoroutine(ok):  # probe_session is `async def`; Check Setup is synchronous
            ok = asyncio.run(ok)
    except Exception:  # noqa: BLE001 -- a probe that cannot run is "not tested", never a crash
        ok = None
    if ok is True:
        status, text = "PASS", "Kotak session: OK"
    elif ok is False:
        status, text = "FAIL", "Kotak session: REJECTED (log in again)"
    else:
        status, text = "WARN", "Kotak session: not tested"
    print(f"{'ok  ' if status == 'PASS' else status:<6}{text}")
    note("token", status, text)
    return status == "FAIL"


def data_plan_problem(broker: str, adapter: Any) -> tuple[str, str] | None:
    """P4 (audit #16): ONE cheap market-data call (an LTP for RELIANCE) after the profile probe. A token can pass
    the profile endpoint and still be refused every price at 09:15 (Dhan without the Data API: DH-902 / 806; a
    Kite Personal app: PermissionException -> 806). None = data flows (or the adapter has no ltp: a stub);
    ("FAIL", text) = the broker refused the data call; ("WARN", text) = the call could not be judged."""
    ltp = getattr(adapter, "ltp", None)
    if not callable(ltp):
        return None
    try:
        prices = ltp([UNDERLYINGS[0][1]], "NSE_EQ")
    except BrokerEntitlementError as exc:  # 0.8.3 T-229 (2), D230: the broker's own fix, not the data-plan guess
        return "FAIL", exc.hint
    except BrokerError as exc:
        why = f" ({exc.code})" if exc.code else ""
        text = (f"the login works, but {broker} refused the market-data call{why}: this account needs the "
                f"broker's data plan / API subscription for prices (broker said: {str(exc)[:160]})")  # fmt: skip
        return "FAIL", text
    except Exception as exc:  # noqa: BLE001 -- not the broker's verdict; the profile already passed
        return "WARN", f"token valid, but the market-data check could not run ({type(exc).__name__})"
    if not prices:
        return "WARN", f"token valid, but {broker} returned no price for the market-data check"
    return None


# T-159: the per-request id limit the 09:20 ltp() is split by. The adapter's own remembered size wins (Kotak /
# Zerodha / FYERS halve it on an HTTP 400, gammad/broker/batching.py); the others use their documented constant.
DOC_BATCH = {
    "kotak": ("gammad.broker.kotak.wire", "QUOTE_MAX_IDS"),
    "zerodha": ("gammad.broker.zerodha.wire", "LTP_MAX_IDS"),
    "fyers": ("gammad.broker.fyers.wire", "QUOTES_MAX"),
    "angel": ("gammad.broker.angel.wire", "QUOTE_MAX_TOKENS"),
    "dhan": ("gammad.broker.dhan", "LTP_MAX_IDS"),
    "groww": ("gammad.broker.groww.wire", "LTP_MAX_IDS"),
    "upstox": ("gammad.broker.upstox.wire", "QUOTE_MAX_KEYS"),
    "shoonya": ("gammad.broker.noren.wire", "QUOTE_MAX_IDS"),  # D726: GetQuotes takes one exch/token
    "flattrade": ("gammad.broker.noren.wire", "QUOTE_MAX_IDS"),
}


def batch_size_for(broker: str, adapter: Any) -> int | None:
    size = getattr(adapter, "ltp_batch_size", None)
    if isinstance(size, int) and size > 0:
        return size
    mod_attr = DOC_BATCH.get(broker)
    if mod_attr is None:
        return None
    try:
        return int(getattr(importlib.import_module(mod_attr[0]), mod_attr[1]))
    except (ImportError, AttributeError, TypeError, ValueError):
        return None


def check_quote_batch(adapter: Any, universe_ids: list[int], size: int | None) -> tuple[str, str]:
    """(a) ONE ltp() of exactly `size` real NSE_EQ ids (the whole universe when it is smaller)."""
    if not size:
        return "WARN", "not checked: this broker states no price batch size"
    ids = list(universe_ids[:size])
    if not ids:
        return "WARN", "not checked: the F&O universe is empty"
    try:
        got = adapter.ltp(ids, "NSE_EQ")
    except BrokerAuthError as exc:
        return "FAIL", f"the broker refused the login on a {len(ids)}-id price request: {str(exc)[:160]}"
    except BrokerError as exc:
        return "FAIL", (f"the broker refused ONE price request of {len(ids)} ids (the 09:20 job sends these): "
                        f"{str(exc)[:160]}")  # fmt: skip
    after = getattr(adapter, "ltp_batch_size", size)
    whole = " (the whole universe)" if len(ids) < size else ""
    if isinstance(after, int) and after < len(ids):
        return "WARN", (f"a batch of {len(ids)} ids was refused; {after} per request works and is used for this "
                        f"session ({len(got)} of {len(ids)} prices). Tell support the size that worked: {after}")
    status = "PASS" if len(got) == len(ids) else "WARN"
    return status, f"one request of {len(ids)} ids{whole}: {len(got)} of {len(ids)} prices (batch size {size} works)"


def check_universe_ltp(adapter: Any, universe_ids: list[int], *, trading_day: bool) -> tuple[str, str]:
    """(b) the full-universe ltp() mark_levels sends at 09:20 (gammad/jobs/mark_levels.py), trading days only."""
    if not trading_day:
        return "PASS", "full-universe price check not run: today is not a trading day"
    ids = list(universe_ids)
    try:
        got = adapter.ltp(ids, "NSE_EQ")
    except BrokerError as exc:
        return "FAIL", f"the 09:20 price call for all {len(ids)} F&O stocks failed: {str(exc)[:160]}"
    size = getattr(adapter, "ltp_batch_size", None)
    per = f" ({size} per request)" if isinstance(size, int) else ""
    status = "PASS" if len(got) == len(ids) else "WARN"
    return status, f"the 09:20 price call: {len(got)} of {len(ids)} prices returned{per}"


def doctor_batch_steps(broker: str, adapter: Any, note: Callable[[str, str, str], None],
                       universe_ids: list[int] | None = None, trading_day: bool | None = None) -> bool:  # fmt: skip
    """(a) + (b) under the one report id "quote_batch"; True when either FAILED."""
    if universe_ids is None:
        try:
            from gammad.jobs.mark_levels import load_universe

            universe_ids = [r.eq_security_id for r in load_universe(adapter)]
        except Exception as exc:  # noqa: BLE001 -- a check that cannot run is a WARN, never a crash
            text = f"price batch not checked: the F&O universe could not be loaded ({type(exc).__name__})"
            print(f"WARN  {text}")
            note("quote_batch", "WARN", text)
            return False
    if trading_day is None:
        from gammad.core.calendar import is_trading_day

        trading_day = is_trading_day(now_ist().date())
    failed = False
    for status, text in (check_quote_batch(adapter, universe_ids, batch_size_for(broker, adapter)),
                         check_universe_ltp(adapter, universe_ids, trading_day=trading_day)):  # fmt: skip
        print(f"{'ok  ' if status == 'PASS' else status:<6}prices: {text}")
        note("quote_batch", status, text)
        failed = failed or status == "FAIL"
    return failed


def telegram_online(token: str, group: str, transport: Any = None, *, topic: str = "") -> tuple[str, str]:
    """P1 (audit #8): ask Telegram itself -- getMe (is the bot token real?) then getChat(group) (is the bot in
    the group?). Returns (PASS | WARN | FAIL, text); the text never carries the token or the id. No answer from
    Telegram (network, 5xx, 429) is a WARN "could not reach Telegram", never a FAIL. `transport` = tests
    (httpx.MockTransport); TELEGRAM_TRANSPORT is the module-level override the tests set.
    B6 (T-134, D131(2)): getChat says is_forum and `topic` (TELEGRAM_TOPIC_ID) is blank -> WARN with the S0
    sentence forum_topic_warning(); a forum with a topic and a plain group PASS.
    T-181 F3 (R4): with `topic` set, sendChatAction into that topic (nothing is posted); a missing / closed / deleted
    topic is a FAIL in telegram.TOPIC_MISSING_TEXT, the sentence the 09:00 readiness check prints too."""
    import httpx

    from gammad.alerts.telegram import (
        TOPIC_MISSING_TEXT,
        TelegramClient,
        TelegramError,
        forum_topic_warning,
        is_topic_error,
    )

    unreachable = "could not reach Telegram to check the bot token and group (check the internet connection)"

    def silent(exc: TelegramError) -> bool:
        return exc.status is None or exc.status == 429 or exc.status >= 500

    async def probe() -> tuple[str, str]:
        async with httpx.AsyncClient(transport=transport or TELEGRAM_TRANSPORT, timeout=10.0) as http:
            bot = TelegramClient(token, client=http, max_5xx_retries=1, max_429_retries=0)
            try:
                await bot.get_me()
            except TelegramError as exc:
                if silent(exc):
                    return "WARN", unreachable
                return "FAIL", (f"Telegram refused TELEGRAM_BOT_TOKEN (HTTP {exc.status}): copy the bot token "
                                "from @BotFather again")  # fmt: skip
            try:
                chat = await bot.get_chat(group)
            except TelegramError as exc:
                if silent(exc):
                    return "WARN", unreachable
                return "FAIL", (f"the bot cannot see the group in TELEGRAM_GROUP_ID (HTTP {exc.status}): add the "
                                "bot to the group, or copy the group id again")  # fmt: skip
            thread = (topic or "").strip()
            if thread:  # T-181 F3 (R4): a topic that does not exist fails EVERY group send
                try:
                    await bot.send_chat_action(group, "typing", message_thread_id=int(thread))
                except ValueError:
                    return "FAIL", "TELEGRAM_TOPIC_ID must be a number (the topic id), or blank to post in General"
                except TelegramError as exc:
                    if silent(exc):
                        return "WARN", unreachable
                    if is_topic_error(exc):
                        return "FAIL", TOPIC_MISSING_TEXT
                    return "FAIL", (f"the bot cannot post in the topic in TELEGRAM_TOPIC_ID (HTTP {exc.status}): "
                                    "give the bot the right to post in the group")  # fmt: skip
        forum = forum_topic_warning(chat, topic)
        if forum is not None:
            return "WARN", forum
        return "PASS", "bot token accepted by Telegram, the bot is in the group"

    try:
        return asyncio.run(probe())
    except Exception as exc:  # noqa: BLE001 -- a probe failure is never a setup failure
        return "WARN", f"{unreachable} ({type(exc).__name__})"


async def run_feed(entry: BrokerEntry, creds: dict[str, str], adapter: Any, subs: list, seconds: float) -> dict:
    q: asyncio.Queue = asyncio.Queue(maxsize=100_000)
    auth: list[tuple[int, str]] = []
    feed = entry.make_feed(creds, q, lambda c, m: auth.append((c, m)), adapter=adapter)
    await feed.start(subs)
    try:
        await asyncio.sleep(seconds)
    finally:
        await feed.stop()
    ticks = []
    while not q.empty():
        ticks.append(q.get_nowait())
    full = {(s, i) for s, i, m in subs if m == "FULL"}
    opt = [t for t in ticks if (t.segment, t.security_id) in full]
    return {
        "ticks": len(ticks),
        "auth_errors": auth,
        "oi": any(t.oi is not None for t in opt),
        "volume": any(t.volume_day for t in ticks),
        "ltt": any(t.ts_ms != t.recv_ms for t in ticks),
        "ids_ok": all((t.segment, t.security_id) in {(s, i) for s, i, _ in subs} for t in ticks),
    }


def cmd_smoke(args: argparse.Namespace) -> int:
    from gammad.broker.canonical import CanonicalMaster, ContractKey, check_identity
    from gammad.core.calendar import prev_trading_day

    env = read_env(ROOT)
    entry = resolve(args, env)
    if entry is None:
        return EXIT_CONFIG
    creds = registry.creds_from_env(entry, env)
    missing = registry.missing_credentials(entry, creds)
    if missing:
        print(f"CONFIG: .env is missing {', '.join(missing)}")
        return EXIT_CONFIG
    caps = entry.caps
    today = now_ist().date()
    prev = prev_trading_day(today)
    out: dict[str, Any] = {"broker": entry.name, "date": today.isoformat(), "declared": caps.to_dict(), "observed": {}}
    obs = out["observed"]
    fails: list[str] = []

    def step(name: str, fn: Any) -> Any:
        try:
            return fn()
        except BrokerAuthError as exc:
            fails.append(f"{name}: auth rejected ({exc})")
        except (BrokerError, ValueError, LookupError) as exc:
            fails.append(f"{name}: {type(exc).__name__}: {exc}")
        return None

    adapter = entry.make_adapter(creds, ROOT / "data")
    th = step("token_health", adapter.token_health)
    obs["token_valid"] = bool(th and th.valid)
    ltp = step("ltp", lambda: adapter.ltp([sid for _, sid in UNDERLYINGS], "NSE_EQ")) or {}
    obs["ltp"] = len(ltp) == len(UNDERLYINGS)
    exps = step("expiries", lambda: adapter.expiries(UNDERLYINGS[0][1], "NSE_EQ")) or []
    obs["expiries"] = bool(exps) and exps == sorted(set(exps))
    chain = (step("option_chain", lambda: adapter.option_chain(UNDERLYINGS[0][1], "NSE_EQ", exps[0])) or []) if exps else []
    obs["chain"] = any(r.oi > 0 for r in chain)
    master = step("canonical master", lambda: CanonicalMaster.load(ROOT / "data"))
    bad = []
    if master is not None:
        for r in chain:
            native = ContractKey(UNDERLYINGS[0][0], exps[0], r.strike, r.option_type)
            verdict = check_identity(master, "NSE_FNO", int(r.security_id or 0), native)
            if verdict is not None:
                bad.append(f"{verdict.security_id}: {verdict.reason}")
    obs["identity_mismatches"] = bad
    top = sorted((r for r in chain if r.security_id), key=lambda r: -r.oi)[:2]
    bars = []
    if top:
        bars = step("intraday", lambda: adapter.intraday(int(top[0].security_id), "NSE_FNO", 5, prev, prev)) or []
    obs["intraday_bars"] = len(bars)
    obs["candle_oi"] = any(b.oi is not None for b in bars)
    obs["candle_volume"] = any(b.volume > 0 for b in bars)
    daily = step("daily", lambda: adapter.daily_candles(UNDERLYINGS[0][1], "NSE_EQ", prev - timedelta(days=45), prev))
    obs["daily_bars"] = len(daily or [])
    from gammad.watch.feed import _market_hours, _wall_ms

    live = _market_hours(_wall_ms())
    if not args.no_feed and top:
        subs = [("NSE_FNO", int(r.security_id), "FULL") for r in top] + [("NSE_EQ", UNDERLYINGS[0][1], "QUOTE")]
        obs["feed"] = asyncio.run(run_feed(entry, creds, adapter, subs, args.feed_seconds if live else 10.0))
        obs["feed"]["market_open"] = live
    checks = [
        ("token", True, obs["token_valid"]),
        ("ltp", True, obs["ltp"]),
        ("expiries", caps.expiries != "none", obs["expiries"]),
        ("chain", caps.chain != "none", obs["chain"]),
        ("identity", True, not bad),
        ("candle_oi", caps.candle_oi, obs["candle_oi"]),
        ("candle_volume", caps.candle_volume, obs["candle_volume"]),
        ("daily_history", caps.daily_history, obs["daily_bars"] >= 20),
    ]
    if "feed" in obs:
        f = obs["feed"]
        checks.append(("feed_auth", True, not f["auth_errors"]))
        checks.append(("feed_identity", True, f["ids_ok"]))
        for name, declared, seen in (
            ("feed_oi", caps.feed_oi != "none", f["oi"]),
            ("feed_volume", caps.feed_volume != "none", f["volume"]),
            ("feed_ltt", caps.feed_ltt, f["ltt"]),
        ):
            checks.append((name, declared, seen if live else None))
    rows = [("capability", "declared", "observed", "verdict")]
    ok = True
    for name, declared, seen in checks:
        verdict = "n/a" if seen is None else ("ok" if (seen or not declared) else "FAIL")
        if seen and not declared:
            verdict = "ok (undeclared)"
        ok &= verdict != "FAIL"
        rows.append((name, str(declared), str(seen), verdict))
    print_table(rows)
    for f in fails + bad:
        print(f"  ! {f}")
    out["verdict"] = {n: {"declared": d, "observed": s} for n, d, s in checks}
    out["errors"] = fails
    out_dir = Path(args.out) if args.out else ROOT / "data"
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"smoke-{entry.name}-{today.isoformat()}.json"
    path.write_text(json.dumps(out, indent=2, default=str), encoding="utf-8")
    print(f"wrote {path}")
    return EXIT_OK if ok and not fails else EXIT_FAIL


# ============================================================================ Check Setup report (D104)
# Standard library only below this line (plus the doctor above when the program files load): the report must
# work on exactly the installs that are broken. Every text that leaves this process through the report goes
# through sanitize(): .env values and app environment values -> [hidden], URL tails, the user's home folder,
# and the words a client text never carries.
PASS, WARN, FAIL = "PASS", "WARN", "FAIL"
KIND_CONFIG, KIND_PORT, KIND_OTHER = "config", "port", "other"
NON_SECRET_KEYS = frozenset({"GAMMA_PORT", "GAMMA_TZ", "BROKER"})
APP_ENV_PREFIXES = ("TELEGRAM_", "DHAN_", "GAMMA_", "KITE_", "ZERODHA_", "UPSTOX_", "ANGEL_", "FYERS_", "KOTAK_",
                    "GROWW_", "NUVAMA_")  # fmt: skip
MIN_SECRET_LEN = 6
URL_TAIL = re.compile(r"(https?://[^/\s\"']+)[/?#][^\s\"']*")  # a bare host:port stays readable
JWT = re.compile(r"\beyJ[\w-]{4,}\.[\w-]{4,}(?:\.[\w-]+)?")  # most broker access tokens, even if not in .env
CLIENT_WORDS = re.compile(r"\b(E2|ARMED|BUY|SELL|algo\w*)\b|possible\s+entry", re.IGNORECASE)
# every auto_login key name today (D86: owner-only, never shown to a client); tests/test_p_setup_report.py
# reddens when the registry grows one this pattern misses
OWNER_ONLY_LINE = re.compile(r"TOTP|_M?PIN\b|auto_login|REFRESH_TOKEN", re.IGNORECASE)
REQUEST_FILES = ("STOP", "QUIT", "START")
# B6 (PLAN-W16 13.4): data/ files the leftovers fix must NEVER delete, whatever REQUEST_FILES grows to --
# install.json (the install card's once-only mark, B4), remote_seen.json and lan_secret (the LAN login, B1)
KEEP_DATA_FILES = ("install.json", "remote_seen.json", "lan_secret")
REMOTE_SEEN = "remote_seen.json"
COMMON_REQUIRED = ("gammad/__init__.py", "gammad/__main__.py", "dashboard/dist/index.html", ".env.example", "VERSION")
WIN_REQUIRED = ("python/python.exe", "python/pythonw.exe", "run_gamma.pyw")
LOG_ERROR_LINES = 20
STARTUP_ERROR_LINES = 40
DEFAULT_PORT = 8760


@dataclass
class Check:
    id: str
    title: str
    status: str
    facts: list[str] = field(default_factory=list)
    steps: list[str] = field(default_factory=list)
    kind: str = KIND_CONFIG
    fix_label: str = ""
    fix: Callable[[], None] | None = field(default=None, repr=False)
    fixed: str = ""  # "done" | "failed: <why>" once a fix ran


def env_file_values(root: Path) -> dict[str, str]:
    """.env KEY=value pairs, standard library only (python-dotenv may be the broken part)."""
    out: dict[str, str] = {}
    try:
        text = (root / ".env").read_text(encoding="utf-8", errors="replace")
    except OSError:
        return out
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, val = line.split("=", 1)
        val = val.strip()
        if val[:1] not in ("'", '"'):
            val = val.split(" #", 1)[0].strip()
        out[key.strip().removeprefix("export ").strip()] = val.strip("\"'")
    return out


def secret_values(root: Path) -> list[str]:
    vals = [v for k, v in env_file_values(root).items() if k not in NON_SECRET_KEYS]
    vals += [v for k, v in os.environ.items() if k.upper().startswith(APP_ENV_PREFIXES) and k not in NON_SECRET_KEYS]
    out: set[str] = set()
    for v in (x.strip() for x in vals):
        if len(v) >= MIN_SECRET_LEN:
            out.add(v)
            if v.startswith("-100") and len(v) - 4 >= MIN_SECRET_LEN:
                out.add(v[4:])
    return sorted(out, key=len, reverse=True)


def sanitize(text: str, secrets: list[str]) -> str:
    for s in secrets:
        text = text.replace(s, "[hidden]")
    text = JWT.sub("[hidden]", URL_TAIL.sub(r"\1/[hidden]", text))
    home = str(Path.home())
    if len(home) > 3:
        text = re.sub(re.escape(home), "%USERPROFILE%" if os.name == "nt" else "~", text, flags=re.IGNORECASE)
    text = CLIENT_WORDS.sub("[-]", text)
    return "\n".join("[line hidden]" if OWNER_ONLY_LINE.search(ln) else ln for ln in text.split("\n"))


def sanitize_obj(obj: Any, secrets: list[str]) -> Any:
    if isinstance(obj, str):
        return sanitize(obj, secrets)
    if isinstance(obj, dict):
        return {k: sanitize_obj(v, secrets) for k, v in obj.items()}
    if isinstance(obj, list):
        return [sanitize_obj(v, secrets) for v in obj]
    return obj


def temp_dirs() -> list[Path]:
    dirs = [Path(tempfile.gettempdir())]
    dirs += [Path(os.environ[k]) for k in ("TEMP", "TMP") if os.environ.get(k)]
    return dirs


def _inside(p: Path, parent: Path) -> bool:
    try:
        return p.resolve().is_relative_to(parent.resolve())
    except OSError:
        return False


def _show(rel: str) -> str:
    return rel.replace("/", "\\") if os.name == "nt" else rel


def check_location(root: Path, temps: list[Path]) -> Check:
    c = Check("location", "Where GAMMA is installed", PASS, [f"folder: {root}"])
    in_zip = any(part.lower().endswith(".zip") for part in root.parts)
    if in_zip or any(_inside(root, t) for t in temps):
        c.status = FAIL
        c.facts.insert(0, "GAMMA is running from inside the ZIP file or from a temporary folder; files there "
                          "can vanish and your settings are lost.")  # fmt: skip
        c.steps = [
            "Close this window.",
            "Find the GAMMA ZIP you downloaded (usually in Downloads).",
            "Right-click it > Extract All..., choose a short folder such as C:\\GAMMA, then click Extract.",
            "Open the extracted folder and double-click Check Setup there.",
        ]
    return c


def _owner_checkout(root: Path) -> bool:
    """EDITION says owner (the owner's repo tracks EDITION=owner, W14 H6). Anything else -- client, missing,
    unreadable, unknown -- is a client package, fail closed like gammad.edition.read_edition. Read here, not via
    gammad, so a damaged package whose gammad cannot import still gets its files checked."""
    try:
        text = (root / "EDITION").read_bytes().decode("utf-8-sig", errors="replace").strip().lower()
    except OSError:
        return False
    return text == "owner"


def check_files(root: Path, windows: bool) -> Check:
    c = Check("files", "Program files present", PASS)
    if _owner_checkout(root):
        c.status, c.facts = WARN, ["this folder is not a GAMMA client package (EDITION says owner); files not checked"]
        return c
    need = COMMON_REQUIRED + (WIN_REQUIRED if windows else ())
    missing = [r for r in need if not (root / r).is_file()]
    bundled = (root / "python" / "GAMMA-BUNDLE.txt").is_file()  # 0.8.2 T-216: the Mac ZIP carries its own Python
    if not windows and bundled and not (root / "python" / "bin" / "python3.13").exists():
        c.status, c.facts = FAIL, ["the bundled python/bin/python3.13 is missing: the ZIP was not extracted whole"]
        c.steps = ["Extract the whole ZIP again into a NEW folder and copy your .env and data folder across.",
                   "Then run ./check.sh again."]  # fmt: skip
    elif not windows and not bundled and not (root / ".venv" / "bin" / "python").exists():
        c.status, c.facts = FAIL, ["the .venv folder is missing: run ./setup.sh first"]
        c.steps = ["Open a terminal in the GAMMA folder and run ./setup.sh", "Then run ./check.sh again."]
    if not missing:
        if c.status == PASS:
            c.facts = ["every required program file is present"]
        return c
    c.status = FAIL
    c.facts.append("missing: " + ", ".join(_show(r) for r in missing))
    c.steps = []
    if any(r.startswith("python/") for r in missing):
        c.steps.append("Open Windows Security > Virus & threat protection > Protection history. If it lists a "
                       "GAMMA file, choose Restore (or Allow on device).")  # fmt: skip
    c.steps += [
        "Copy your .env file somewhere safe (it holds your settings).",
        "Delete this GAMMA folder and extract the whole ZIP again (right-click > Extract All).",
        "Copy your .env back into the new folder, then run Check Setup again.",
    ]
    return c


def has_download_mark(p: Path) -> bool:
    """Windows' mark of the web: the Zone.Identifier alternate data stream (what Unblock-File removes)."""
    try:
        with open(f"{p}:Zone.Identifier", "rb"):
            return True
    except OSError:
        return False


def check_download_mark(root: Path) -> Check:
    c = Check("blocked", "Windows download block", PASS, ["no launcher carries the download mark"])
    cands = [*root.glob("*.cmd"), *root.glob("*.pyw"), *(root / "python").glob("*.exe")]
    marked = [p for p in sorted(cands) if p.is_file() and has_download_mark(p)]
    if not marked:
        return c
    names = [_show(p.relative_to(root).as_posix()) for p in marked]
    c.status = FAIL
    c.facts = [f"{len(marked)} file(s) carry Windows' 'downloaded from the internet' mark: " + ", ".join(names)]
    c.steps = [
        "Answer Y when Check Setup offers to remove the mark (it changes only files inside the GAMMA folder), or:",
        "Right-click each file listed under Details > Properties > tick Unblock at the bottom > OK.",
        "Next time, before extracting: right-click the ZIP > Properties > tick Unblock > OK.",
    ]
    c.fix_label = f"Remove the download mark from {len(marked)} GAMMA file(s)"

    def unblock() -> None:
        for p in marked:
            if _inside(p, root):  # never a file outside the package folder
                os.remove(f"{p}:Zone.Identifier")

    c.fix = unblock
    return c


def can_write(folder: Path) -> bool:
    try:
        folder.mkdir(parents=True, exist_ok=True)
        probe = folder / f".write-test-{os.getpid()}"
        probe.write_bytes(b"")
        probe.unlink()
        return True
    except OSError:
        return False


def check_writable(root: Path, probe: Callable[[Path], bool] = can_write) -> Check:
    bad = [d for d in (root, root / "data", root / "logs") if not probe(d)]
    c = Check("writable", "GAMMA can save its files", PASS, ["the folder, data and logs can be written to"])
    if bad:
        c.status = FAIL
        c.facts = ["Windows does not let GAMMA write into: " + ", ".join(str(d) for d in bad)]
        c.steps = [
            "Quit GAMMA (tray icon > Quit, or Stop GAMMA).",
            ("Move the whole GAMMA folder to a short folder you own, such as C:\\GAMMA (not inside Program "
             "Files and not inside a OneDrive folder)."),
            "Start GAMMA from the new folder.",
        ]
    return c


def check_env_file(root: Path) -> Check:
    env, example = root / ".env", root / ".env.example"
    c = Check("env_file", "Settings file (.env)", PASS, [".env exists"])
    if env.is_file():
        return c
    c.status = FAIL
    if (root / ".env.txt").is_file():
        c.facts = ["there is a file .env.txt: Windows hid the .txt ending, so GAMMA cannot find .env"]
        c.steps = ["In the folder window: View > Show > File name extensions.", "Rename .env.txt to .env.",
                   "Run Check Setup again."]  # fmt: skip
        return c
    if not example.is_file():
        c.facts = [".env and .env.example are both missing"]
        c.steps = ["Extract the whole ZIP again (right-click > Extract All).", "Run Check Setup again."]
        return c
    c.facts = [".env is missing"]
    c.steps = ["Answer Y to create .env from .env.example, or copy .env.example to .env yourself.",
               "Open .env with Notepad and fill in the Telegram block and your broker's block (setup guide).",
               "Save it and run Check Setup again."]  # fmt: skip
    c.fix_label = "Create .env from .env.example (you then fill it in with Notepad)"

    def create() -> None:
        if not env.exists():
            shutil.copyfile(example, env)

    c.fix = create
    return c


def _doctor_rows(results: list[dict[str, str]], cid: str) -> list[dict[str, str]]:
    return [r for r in results if r["id"] == cid]


def lan_password_short(values: dict[str, str]) -> bool:
    """Check the stripped password without disclosing it or its length."""
    password = (values.get(LAN_PASSWORD_KEY) or "").strip()
    return bool(password) and len(password) < LAN_PASSWORD_MIN_LEN


def lan_problem(values: dict[str, str]) -> str | None:
    """B6: GAMMA_LAN=1 with a blank GAMMA_LAN_PASSWORD is a configuration FAIL (B1 then binds this PC only, and
    D133 Q3 refuses every proxied request). Standard library only; the password value is never returned."""
    # the same truthy set as gammad.config.LAN_ON_VALUES (B1): 1 / true / yes / on, case-insensitive
    if (values.get(LAN_KEY) or "").strip().lower() not in LAN_ON_VALUES or (values.get(LAN_PASSWORD_KEY) or "").strip():
        return None
    return (f"{LAN_KEY}=1 without {LAN_PASSWORD_KEY}: the dashboard stays on this PC only and remote access is "
            f"refused until you set {LAN_PASSWORD_KEY} in .env")  # fmt: skip


def _env_or_process(root: Path, keys: tuple[str, ...]) -> dict[str, str]:
    """D54 order without python-dotenv: a non-empty .env value wins, the process environment is the fallback."""
    vals = env_file_values(root)
    return {k: vals.get(k) or os.environ.get(k, "").strip() for k in keys}


def check_env_keys(root: Path, results: list[dict[str, str]] | None) -> Check:
    c = Check("env_keys", "Settings filled in", PASS)
    steps = ["Open .env with Notepad (right-click > Open with > Notepad).", "Fill in the keys named above.",
             "Save it and run Check Setup again."]  # fmt: skip
    broker = _doctor_rows(results or [], "broker")
    rows = _doctor_rows(results or [], "env_keys")
    if broker:
        c.status, c.facts = FAIL, [f"BROKER: {broker[0]['text']}"]
        c.steps = ["Open .env with Notepad.", "Set BROKER= to the name of your broker (the setup guide lists them).",
                   "Save it and run Check Setup again."]  # fmt: skip
    elif rows:  # the doctor's rows: the broker keys, then (B6) GAMMA_LAN without a password
        fails = [r for r in rows if r["status"] == FAIL]
        c.status = FAIL if fails else rows[0]["status"]
        c.facts = [r["text"] for r in (fails or rows[:1])]
        c.steps = steps if c.status == FAIL else []
    else:  # the doctor did not run: the three keys every setup needs
        vals = env_file_values(root)
        empty = [k for k in ("BROKER", "TELEGRAM_BOT_TOKEN", "TELEGRAM_GROUP_ID") if not vals.get(k)]
        c.status = FAIL if empty else WARN
        c.facts = [f".env is missing {', '.join(empty)}" if empty else "broker keys not checked (see Setup check)"]
        c.steps = steps if empty else []
        lan = lan_problem(_env_or_process(root, (LAN_KEY, LAN_PASSWORD_KEY)))
        if lan is not None:
            c.facts = ([] if c.status == WARN else c.facts) + [lan]
            c.status, c.steps = FAIL, steps
    return c


def check_remote(root: Path) -> Check:
    """B6 (D118(5)(7), D133 Q3): data/remote_seen.json (written by gammad/api/lan.py, {host, first_seen_ist,
    last_seen_ist, count}) means the dashboard was reached from another device or through a proxy. Without
    GAMMA_LAN_PASSWORD that is a WARN (remote access is refused); with 12+ characters it is a PASS that names it.
    A non-empty shorter password always warns, even before a remote visit."""
    values = _env_or_process(root, (LAN_PASSWORD_KEY,))
    c = Check("remote", "Dashboard reached from another device", PASS, kind=KIND_CONFIG)
    if lan_password_short(values):
        c.status = WARN
        c.facts = [("GAMMA_LAN_PASSWORD is shorter than 12 characters: choose a password of 12 or "
                   "more characters that you use nowhere else")]
        c.steps = [("Open .env with Notepad and set GAMMA_LAN_PASSWORD= to a password of 12 or more characters "
                   "that you use nowhere else."),
                   "Save it and restart GAMMA (tray icon > Quit, then Start GAMMA)."]
    path = root / "data" / REMOTE_SEEN
    if not path.is_file():
        c.facts.insert(0, f"no remote visit recorded ({_show('data/' + REMOTE_SEEN)} absent)")
        return c
    seen = _remote_seen(path)
    where = (f"the dashboard was reached through {seen['host']} ({seen['count']} times, last "
             f"{seen['last_seen_ist']})" if seen else
             f"the dashboard was reached from another device (data file {REMOTE_SEEN} could not be read)")
    if values[LAN_PASSWORD_KEY]:
        c.facts.insert(0, (f"{where}; {LAN_PASSWORD_KEY} is set, so remote sessions need the login and the Broker login page "
                           "is hidden from them"))  # fmt: skip
        return c
    c.status = WARN
    c.facts = [(f"{where}: set {LAN_PASSWORD_KEY} in .env; without it remote access is refused, and the Broker login page "
                "is hidden from remote sessions")]  # fmt: skip
    c.steps = ["Open .env with Notepad and set GAMMA_LAN_PASSWORD= to a password only you know.",
               "Save it and restart GAMMA (tray icon > Quit, then Start GAMMA)."]  # fmt: skip
    return c


def _remote_seen(path: Path) -> dict[str, Any] | None:
    """{host, count, last_seen_ist} from B1's remote_seen.json, or None when it cannot be read."""
    try:
        doc = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    if not isinstance(doc, dict) or not isinstance(doc.get("host"), str) or not doc["host"].strip():
        return None
    count = doc.get("count")
    return {"host": doc["host"].strip()[:200], "count": count if isinstance(count, int) else "?",
            "last_seen_ist": str(doc.get("last_seen_ist") or "?")[:40]}  # fmt: skip


def check_port(root: Path) -> tuple[Check, bool]:
    """(check, GAMMA is running on its port)."""
    raw = env_file_values(root).get("GAMMA_PORT") or os.environ.get("GAMMA_PORT", "").strip()
    c = Check("port", "Dashboard port", PASS, kind=KIND_PORT)
    try:
        port = int(raw or DEFAULT_PORT)
    except ValueError:
        c.status, c.kind, c.facts = FAIL, KIND_CONFIG, ["GAMMA_PORT in .env is not a number"]
        c.steps = ["Open .env with Notepad and set GAMMA_PORT=8760 (or leave it empty).", "Save it."]
        return c, False
    url = f"http://127.0.0.1:{port}"
    if not port_busy(port):
        c.facts = [f"port {port} is free; the dashboard opens at {url} once GAMMA runs"]
        return c, False
    running = running_gamma(port)
    if running is not None:
        what = "GAMMA's tray is running" if running.get("supervisor") else (
            f"GAMMA {running['version']} ({running['edition']}) is running")
        c.facts = [f"{what} on port {port}; dashboard: {url}"]
        return c, True
    c.status = FAIL
    c.facts = [f"port {port} is used by another program, so GAMMA cannot open its dashboard"]
    c.steps = [
        "Close the other program that uses this port (often a second copy of GAMMA: tray icon > Quit), or:",
        "Open .env with Notepad and set GAMMA_PORT=8770 (any free number between 1025 and 65000), then save it.",
        "Start GAMMA again; with 8770 the dashboard is at http://127.0.0.1:8770",
    ]
    return c, False


def _pid_alive(pidfile: Path) -> bool:
    """POSIX only (./start.sh writes data/gammad.pid). NEVER os.kill on Windows: signal 0 there TERMINATES."""
    if os.name != "posix":
        return False
    try:
        os.kill(int(pidfile.read_text(encoding="ascii").strip()), 0)
        return True
    except (OSError, ValueError):
        return False


def check_leftovers(root: Path, gamma_running: bool) -> Check:
    c = Check("leftovers", "Left-over stop files", PASS, ["none"])
    if gamma_running:
        c.facts = ["GAMMA is running; its request files are left to it"]
        return c
    data = root / "data"
    found = [data / n for n in REQUEST_FILES if n not in KEEP_DATA_FILES and (data / n).is_file()]
    pidf = data / "gammad.pid"
    if pidf.is_file() and not _pid_alive(pidf):
        found.append(pidf)
    if not found:
        return c
    names = [_show(p.relative_to(root).as_posix()) for p in found]
    c.status = FAIL
    c.facts = ["GAMMA is not running but these request files are left over: " + ", ".join(names)]
    c.steps = ["Answer Y to delete them, or delete them yourself: " + ", ".join(names) + ".",
               "Then start GAMMA."]  # fmt: skip
    c.fix_label = "Delete the left-over files " + ", ".join(names)

    def remove() -> None:
        for p in found:
            if _inside(p, root) and p.name not in KEEP_DATA_FILES:  # B6: never install.json / remote_seen / lan_secret
                p.unlink(missing_ok=True)

    c.fix = remove
    return c


def check_from_doctor(results: list[dict[str, str]] | None, cid: str, title: str, kind: str,
                      steps: list[str], not_run: str) -> Check:  # fmt: skip
    rows = _doctor_rows(results or [], cid)
    c = Check(cid, title, PASS, kind=kind)
    if not rows:
        c.status, c.facts = WARN, [not_run]
        return c
    c.facts = [r["text"] for r in rows]
    c.status = FAIL if any(r["status"] == FAIL for r in rows) else (
        WARN if any(r["status"] == WARN for r in rows) else PASS)
    c.steps = steps if c.status == FAIL else []
    return c


def _tail_lines(path: Path, max_bytes: int = 1_000_000) -> list[str]:
    try:
        with path.open("rb") as fh:
            size = fh.seek(0, os.SEEK_END)
            fh.seek(max(0, size - max_bytes))
            return fh.read().decode("utf-8", errors="replace").splitlines()
    except OSError:
        return []


def check_errors(root: Path) -> tuple[Check, dict[str, list[str]]]:
    logs = root / "logs"
    details: dict[str, list[str]] = {}
    facts: list[str] = []
    se = logs / "startup-error.log"
    if se.is_file():
        when = dt.datetime.fromtimestamp(se.stat().st_mtime, dt.UTC).astimezone().strftime("%Y-%m-%d %H:%M")
        details["startup-error.log"] = _tail_lines(se)[:STARTUP_ERROR_LINES]
        facts.append(f"GAMMA could not start on {when} (logs\\startup-error.log, copied under Details)")
    for name in ("tray.log", "gammad.log"):
        errs = [ln for ln in _tail_lines(logs / name) if " ERROR " in ln or " CRITICAL " in ln]
        if errs:
            details[name] = errs[-LOG_ERROR_LINES:]
            facts.append(f"{len(errs)} error line(s) in logs\\{name} (the last {len(details[name])} under Details)")
    launches = _tail_lines(logs / "launch.log")[-5:]
    if launches:
        details["launch.log"] = launches
    c = Check("errors", "Recent error messages", WARN if facts else PASS, facts or ["no error lines in the logs"],
              kind=KIND_OTHER)  # fmt: skip
    return c, details


def _sha256(path: Path) -> str | None:
    h = hashlib.sha256()
    try:
        with path.open("rb") as fh:
            for block in iter(lambda: fh.read(1 << 20), b""):
                h.update(block)
    except OSError:
        return None
    return h.hexdigest()


def modified_files(root: Path) -> list[str] | None:
    """(c) every MANIFEST.json path whose sha256 differs from the install ("<path> (missing)" when absent);
    None when there is no MANIFEST.json (a repo checkout) or it cannot be read."""
    try:
        doc = json.loads((root / "MANIFEST.json").read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    out: list[str] = []
    for f in doc.get("files") or [] if isinstance(doc, dict) else []:
        rel, want = (f.get("path"), f.get("sha256")) if isinstance(f, dict) else (None, None)
        if not isinstance(rel, str) or not isinstance(want, str):
            continue
        have = _sha256(root / rel)
        if have is None:
            out.append(f"{rel} (missing)")
        elif have != want:
            out.append(rel)
    return sorted(out)


def check_modified(root: Path) -> Check:
    mods = modified_files(root)
    if mods is None:
        return Check("modified", "Modified program files", PASS,
                     ["not checked: no MANIFEST.json (not an installed package)"], kind=KIND_OTHER)  # fmt: skip
    if not mods:
        return Check("modified", "Modified program files", PASS, ["every program file matches the package"],
                     kind=KIND_OTHER)  # fmt: skip
    return Check("modified", "Modified program files", WARN,
                 [f"{len(mods)} program file(s) differ from the package: " + ", ".join(mods[:20])], kind=KIND_OTHER)


BUNDLE_LOG_LINES = 2000
BUNDLE_RUN_DAYS = 5


def _mask_secrets(text: str, secrets: list[str]) -> str:
    """The bundle is for support: .env values and bearer tokens are hidden, the log words are kept."""
    for s in secrets:
        text = text.replace(s, "[hidden]")
    return JWT.sub("[hidden]", text)


def _db_rows(db: Path) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """(runs rows of the last BUNDLE_RUN_DAYS run days, watchlist counts per run_id) -- opened READ-ONLY."""
    if not db.is_file():
        return [], []
    conn = sqlite3.connect(db.resolve().as_uri() + "?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        days = [r[0] for r in conn.execute(
            "SELECT DISTINCT substr(started_ist, 1, 10) AS d FROM runs ORDER BY d DESC LIMIT ?", (BUNDLE_RUN_DAYS,))]
        marks = ",".join("?" * len(days))
        runs = [dict(r) for r in conn.execute(
            f"SELECT * FROM runs WHERE substr(started_ist, 1, 10) IN ({marks}) ORDER BY started_ist", days)]
        cols = {r[1] for r in conn.execute("PRAGMA table_info(watchlist)")}
        retired = ", SUM(retired_run_id IS NOT NULL) AS retired" if "retired_run_id" in cols else ""
        counts = [dict(r) for r in conn.execute(
            f"SELECT run_id, trade_date, list_name, COUNT(*) AS rows{retired} FROM watchlist "
            f"WHERE trade_date IN ({marks}) GROUP BY run_id, trade_date, list_name "
            "ORDER BY trade_date, run_id, list_name", days)] if cols else []
    except sqlite3.Error:
        return [], []
    finally:
        conn.close()
    return runs, counts


def _send_counts(db: Path) -> dict[str, list[Any]]:
    """T-181 F8: what reached Telegram per day, for the BUNDLE_RUN_DAYS latest days that sent (or tried) anything --
    manual_sends per (day, kind, dest, status) and alerts per (day, channel, status), each with its count and the
    FIRST error of the group. An alerts row's day is its signal's trade_date, else the day it was sent (an ops row
    that failed carries no time and is left out). Opened READ-ONLY; an unreadable store = empty lists."""
    empty: dict[str, list[Any]] = {"days": [], "manual_sends": [], "alerts": []}
    if not db.is_file():
        return empty
    conn = sqlite3.connect(db.resolve().as_uri() + "?mode=ro", uri=True)
    try:
        sends = conn.execute(
            "SELECT substr(ts_ist, 1, 10), kind, dest, status, error FROM manual_sends ORDER BY id").fetchall()
        alerts = conn.execute(
            "SELECT COALESCE(s.trade_date, CASE WHEN a.sent_ts_ms IS NOT NULL THEN "
            "date(a.sent_ts_ms / 1000, 'unixepoch', '+330 minutes') END), a.channel, a.status, a.error "
            "FROM alerts a LEFT JOIN signals s ON s.id = a.signal_id WHERE a.channel != 'dashboard' "
            "ORDER BY a.id").fetchall()
    except sqlite3.Error:
        return empty
    finally:
        conn.close()
    days = sorted({str(r[0]) for r in (*sends, *alerts) if r[0]}, reverse=True)[:BUNDLE_RUN_DAYS]

    def group(rows: list[Any], names: tuple[str, ...]) -> list[dict[str, Any]]:
        out: dict[tuple[Any, ...], dict[str, Any]] = {}
        for day, *keys, error in rows:
            if day not in days:
                continue
            k = (day, *keys)
            row = out.setdefault(k, {"day": day, **dict(zip(names, keys, strict=True)), "n": 0, "first_error": None})
            row["n"] += 1
            if row["first_error"] is None and error:
                row["first_error"] = str(error)[:300]
        return sorted(out.values(), key=lambda r: tuple(str(r[x]) for x in ("day", *names)))

    return {"days": sorted(days), "manual_sends": group(sends, ("kind", "dest", "status")),
            "alerts": group(alerts, ("channel", "status"))}  # fmt: skip


def write_bundle(root: Path, out: Path) -> Path:
    """(d) ONE support zip. Standard library only (it must work when the program files are damaged)."""
    secrets = secret_values(root)
    log_tail = _tail_lines(root / "logs" / "gammad.log", max_bytes=8_000_000)[-BUNDLE_LOG_LINES:]
    runs, counts = _db_rows(root / "data" / "gamma.db")
    sends = _send_counts(root / "data" / "gamma.db")
    meta = {
        "created": dt.datetime.now(dt.UTC).astimezone().isoformat(timespec="seconds"),
        "version": (root / "VERSION").read_text(encoding="utf-8").strip() if (root / "VERSION").is_file() else "?",
        "edition": (root / "EDITION").read_text(encoding="utf-8").strip() if (root / "EDITION").is_file() else "?",
        "os": platform.platform(),
    }  # fmt: skip
    parts = {
        "bundle.json": json.dumps(meta, indent=1),
        "gammad.log.tail.txt": "\n".join(log_tail) + ("\n" if log_tail else ""),
        "runs.json": json.dumps(runs, indent=1, ensure_ascii=False),
        "watchlist_counts.json": json.dumps(counts, indent=1),
        "telegram_sends.json": json.dumps(sends, indent=1, ensure_ascii=False),  # T-181 F8
        "modified_files.json": json.dumps(modified_files(root), indent=1),
        "env_keys.txt": "".join(f"{k}\n" for k in env_file_values(root)),
    }
    seen = _remote_seen(root / "data" / REMOTE_SEEN) if (root / "data" / REMOTE_SEEN).is_file() else None
    if seen is not None:  # B6: host + count only (the visit times stay on the PC)
        parts[REMOTE_SEEN] = json.dumps({"host": seen["host"], "count": seen["count"]}, indent=1)
    out = Path(out)
    out.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(out, "w", compression=zipfile.ZIP_DEFLATED) as z:
        for name, text in parts.items():
            z.writestr(name, _mask_secrets(text, secrets) if name != "env_keys.txt" else text)
    return out


def ask_yes(prompt: str) -> str:
    try:
        return input(prompt)
    except (EOFError, KeyboardInterrupt):  # no console (non-interactive run): no fixes
        return ""


def exit_code(checks: list[Check]) -> int:
    fails = [c for c in checks if c.status == FAIL]
    if any(c.kind == KIND_PORT for c in fails):
        return EXIT_PORT
    if any(c.kind == KIND_CONFIG for c in fails):
        return EXIT_CONFIG
    return EXIT_FAIL if fails else EXIT_OK


def render_report(checks: list[Check], details: dict[str, list[str]], meta: dict[str, Any]) -> str:
    L = ["GAMMA-UNWIND setup report", "=========================", ""]
    L += [(f"Created {meta['created']}   version {meta['version']} ({meta['edition']})   result code "
           f"{meta['exit_code']} (0 = all good)"), "", "Summary", "-------"]  # fmt: skip
    for c in checks:
        L.append(f"  {c.status:<5} {c.title}: {c.facts[0] if c.facts else ''}")
    L += ["", "Fixed automatically", "-------------------"]
    done = [c for c in checks if c.fixed]
    L += [f"  - {c.fix_label}: {c.fixed}" for c in done] or ["  Nothing was changed."]
    offered = [c for c in checks if c.fix and not c.fixed and c.status == FAIL]
    if offered:
        L.append(f"  {len(offered)} fix(es) were offered and not applied. Run Check Setup again and answer Y.")
    L += ["", "What you need to do", "-------------------"]
    todo = [c for c in checks if c.status == FAIL]
    if not todo:
        L.append("  Nothing. Everything Check Setup looks at is in order.")
    for c in todo:
        L.append(f"  {c.title}")
        L += [f"    {i}. {s}" for i, s in enumerate(c.steps, 1)]
    L += ["", "Details for support", "-------------------"]
    for k in ("version", "edition", "os", "python", "folder", "exit_code"):
        L.append(f"  {k}: {meta[k]}")
    for c in checks:
        L.append(f"  [{c.id}] {c.status}")
        L += [f"      {f}" for f in c.facts]
    for name, lines in details.items():
        L += ["", f"  --- logs\\{name} ---"] + [f"  {ln}" for ln in lines]
    L += ["", "If a problem remains, send this file to support. Check Setup hides your tokens and passwords in it;",
          "never send your .env file to anyone."]  # fmt: skip
    return "\n".join(L) + "\n"


def cmd_check_setup(args: argparse.Namespace, root: Path | None = None, *, ask: Callable[[str], str] = ask_yes,
                    windows: bool | None = None, temps: list[Path] | None = None,
                    executable: str | None = None) -> int:  # fmt: skip
    root = root or ROOT
    windows = os.name == "nt" if windows is None else windows
    with_reconfigure = getattr(sys.stdout, "reconfigure", None)
    if with_reconfigure is not None:
        with_reconfigure(errors="replace")
    results: list[dict[str, str]] = []
    doctor_error = GAMMAD_IMPORT_ERROR
    if doctor_error is None:
        try:
            cmd_doctor(argparse.Namespace(broker=getattr(args, "broker", None),
                                          online=getattr(args, "online", False)), results)  # fmt: skip
        except Exception as exc:  # noqa: BLE001 -- the report still runs; the doctor's failure is a finding
            doctor_error = f"{type(exc).__name__}: {exc}"
    print("\n--- Check Setup report ---")
    checks = [check_location(root, temps if temps is not None else temp_dirs()), check_files(root, windows)]
    if windows:
        checks.append(check_download_mark(root))
    checks += [check_writable(root), check_env_file(root), check_env_keys(root, None if doctor_error else results)]
    port_check, running = check_port(root)
    checks += [port_check, check_leftovers(root, running), check_remote(root)]
    not_run = "not checked: the setup check stopped earlier (see the other lines)"
    checks.append(check_from_doctor(results, "telegram", "Telegram", KIND_CONFIG, [
        ("Open .env with Notepad and check the Telegram block: the bot token from @BotFather and your group "
         "id (a negative number; keep its minus sign)."), "Save it and run Check Setup again."], not_run))  # fmt: skip
    checks.append(check_from_doctor(results, "token", "Broker login (token)", KIND_OTHER, [
        ("Open the dashboard's Broker login page (or .env) and paste today's broker token; most brokers issue a new "
         "one every day."), "Run Check Setup again."], not_run))  # fmt: skip
    if doctor_error:
        c = Check("doctor", "Setup check program", FAIL, [f"the setup check could not run: {doctor_error}"],
                  kind=KIND_OTHER)  # fmt: skip
        c.steps = ["Copy your .env somewhere safe, delete this GAMMA folder and extract the whole ZIP again.",
                   "Copy your .env back and run Check Setup again. If it still fails, send this file to support."]
        checks.append(c)
    checks.append(check_from_doctor(results, "quote_batch", "Broker prices (the 09:20 batch)", KIND_OTHER, [
        "Send the setup report (and Check Setup --bundle) to support: it names the batch size that worked."],
        "not checked: the broker login did not pass (see the other lines)"))  # fmt: skip
    checks.append(check_modified(root))
    err_check, details = check_errors(root)
    checks.append(err_check)

    fixes = [c for c in checks if c.status == FAIL and c.fix is not None]
    if fixes and not getattr(args, "no_fix", False):
        print("Check Setup can fix these for you:")
        for i, c in enumerate(fixes, 1):
            print(f"  {i}. {c.fix_label}")
        if ask("Apply these fixes now? Type Y and press Enter (anything else = no): ").strip().lower() in ("y", "yes"):
            for c in fixes:
                try:
                    assert c.fix is not None
                    c.fix()
                    c.fixed, c.status = "done", PASS
                    c.facts.insert(0, f"fixed by Check Setup: {c.fix_label}")
                except OSError as exc:
                    c.fixed = f"failed: {type(exc).__name__}"
        else:
            print("No changes made.")

    secrets = secret_values(root)
    code = exit_code(checks)
    exe = executable or sys.executable
    meta = {
        "created": dt.datetime.now(dt.UTC).astimezone().strftime("%Y-%m-%d %H:%M:%S"),
        "version": (root / "VERSION").read_text(encoding="utf-8").strip() if (root / "VERSION").is_file() else "?",
        "edition": (root / "EDITION").read_text(encoding="utf-8").strip() if (root / "EDITION").is_file() else "?",
        "os": platform.platform(),
        "python": f"{platform.python_version()} ({'packaged' if _inside(Path(exe), root) else exe})",
        "folder": str(root),
        "exit_code": code,
    }  # fmt: skip
    text = sanitize(render_report(checks, details, meta), secrets)
    payload = {
        **meta,
        "checks": [{k: v for k, v in asdict(c).items() if k != "fix"} for c in checks],
        "details": details,
    }
    body = json.dumps(sanitize_obj(payload, secrets), indent=1, ensure_ascii=False)  # BEFORE encoding: escaped
    # backslashes / quotes in the JSON text would no longer match a value or the home folder
    stamp = dt.datetime.now(dt.UTC).astimezone().strftime("%Y%m%d-%H%M%S")
    out_dir = root / "logs" if can_write(root / "logs") else Path(tempfile.gettempdir())
    txt = out_dir / f"setup-report-{stamp}.txt"
    n = 2
    while txt.exists():
        txt, n = out_dir / f"setup-report-{stamp}-{n}.txt", n + 1
    txt.write_text(text, encoding="utf-8")
    txt.with_suffix(".json").write_text(body, encoding="utf-8")
    summary = text.split("\nFixed automatically", 1)[0].split("\nSummary\n-------\n", 1)[-1]
    print("Summary\n" + summary.rstrip())
    todo = sum(1 for c in checks if c.status == FAIL)
    print(f"\n{todo} problem(s) to fix. " if todo else "\nEverything checked is in order. ", end="")
    print(f"Report: {txt}")
    print(f"If a problem remains, send this file to support: {txt}")
    return code


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--broker", help="override .env BROKER")
    p.add_argument("--list", action="store_true", help="registry table only (no .env, no network)")
    p.add_argument("--doctor", action="store_true", help="setup check only (the first half of Check Setup)")
    p.add_argument("--no-feed", action="store_true")
    p.add_argument("--feed-seconds", type=float, default=60.0)
    p.add_argument("--out", help="directory for smoke-<broker>-<date>.json (default: data/)")
    p.add_argument("--check-setup", action="store_true", help="doctor + setup report (Check Setup.cmd, D104)")
    p.add_argument("--no-fix", action="store_true", help="with --check-setup: never offer the fixes")
    p.add_argument("--offline", action="store_true", help="--doctor / --check-setup: skip the Telegram calls")
    p.add_argument("--bundle", metavar="ZIP", help="write ONE support zip (log tail, runs, watchlist counts, "
                   "modified files, .env key names only) and exit")  # fmt: skip
    args = p.parse_args(argv)
    args.online = not args.offline  # P1: Check Setup asks Telegram (getMe + getChat) unless told not to
    if args.bundle:
        print(f"Support bundle: {write_bundle(ROOT, Path(args.bundle))}")
        return EXIT_OK
    if args.check_setup:
        return cmd_check_setup(args)
    if GAMMAD_IMPORT_ERROR is not None:
        print(f"the program files could not be loaded: {GAMMAD_IMPORT_ERROR}")
        print("Run Check Setup, or extract the whole ZIP again.")
        return EXIT_FAIL
    if args.list:
        return cmd_list()
    if args.doctor:
        return cmd_doctor(args)
    return cmd_smoke(args)


if __name__ == "__main__":
    raise SystemExit(main())
