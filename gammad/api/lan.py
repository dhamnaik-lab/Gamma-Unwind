"""T-129 (PLAN-W16 13.3 B1; D118(1), D119 G2-4, D122, D123(3), D133 Q3): the dashboard's LAN bind, login and proxy guard.

CONTRACT
  lan_enabled(env) -> bool            GAMMA_LAN=1 (EnvCfg.lan).
  bind_hosts(env) -> tuple[str, ...]  ("127.0.0.1",), or ("127.0.0.1", <this PC's RFC1918 IPv4>) when GAMMA_LAN=1 AND
                                      GAMMA_LAN_PASSWORD is set. NEVER 0.0.0.0, never a public / CGNAT / link-local
                                      address. GAMMA_LAN=1 without a password = loopback only + ONE ERROR line.
  install(app, *, runtime)            the middleware + GET /api/session, POST /api/session/login, POST
                                      /api/session/logout. gammad.api.server.create_app calls it (its signature is
                                      unchanged), BEFORE the SPA catch-all route and INSIDE TrustedHostMiddleware.

REMOTE = the ASGI client address is not loopback, OR the request carries any of FORWARD_HEADERS: X-Forwarded-For /
X-Forwarded-Host / Forwarded / X-Real-IP / Via / X-Forwarded-Proto / X-Forwarded-Port / CF-Connecting-IP /
True-Client-IP / X-ARR-LOG-ID / X-Original-URL (a tunnel or reverse proxy on this PC rewrites Host to 127.0.0.1, D122;
the last seven are API_STORE-03, owner pick round 4). A proxy that adds NONE of these headers (e.g. a bare nginx
`proxy_pass` with no proxy_set_header) is indistinguishable from a local browser and stays undetectable here: such a
proxy must add X-Forwarded-For (the client guide is to state this, audit-client-guide); the Host-name rule for that
case is a 0.9.0 design row.
A loopback request with none of those headers passes through untouched, with ONE rule (API_STORE-01 / -02): when it
carries an Origin header (only a browser page sends one), the Origin must be the request's own Host (the dashboard
this app serves, and the tray's stopped page, which binds the same port) or one of VITE_ORIGINS (`vite dev` /
`vite preview`, which keep the browser's Origin while changeOrigin rewrites Host). Any other Origin (a foreign site,
"null", a malformed value, ANOTHER local server's port) is refused: /ws is closed before the handshake is accepted, a
POST/PUT/PATCH/DELETE answers 403. No Origin (the tray's httpx, curl, scripts) passes as before.

A remote request:
  - no GAMMA_LAN_PASSWORD configured (D133 Q3, fail closed): 403 page "remote access needs GAMMA_LAN_PASSWORD" (JSON on
    /api/*), /ws closes 4403; nothing else is served;
  - /api/token*, /auth/*, POST /api/service/stop: 404 (a remote session never sees the broker login or stops the PC);
  - a POST/PUT/PATCH/DELETE whose Origin is not its Host (or the proxy's X-Forwarded-Host): 403;
  - without a valid session cookie: /api/* 401, /ws accepted then closed 4401 (a browser only sees a close code after
    the handshake), every other path (the SPA shell, its assets) is served so #/login can load;
  - /api/session* always reaches its handler (login is limited to 5 attempts a minute per client address; 6th = 429).
A forwarded request whose host (X-Forwarded-Host, Forwarded host=, else Host) is not a loopback name is recorded in
data/remote_seen.json {host, first_seen_ist, last_seen_ist, count} (at most one write a minute) with one WARNING per
host per process; scripts/smoke_broker.py (Check Setup) reads the file.

THE TOKEN (D119 G2-4 as interpreted in 13.3): a 32-byte per-install secret in data/lan_secret (hex, created on the first
LAN start or the first remote request, mode 0600 where the OS allows) signs an HttpOnly SameSite=Strict cookie valid 30
days. The HMAC key mixes the secret with a hash of the password, so a password change invalidates every old cookie and
a cookie signed by another install's secret is refused. The password, the cookie and the secret are never logged,
echoed or written anywhere else.
"""

from __future__ import annotations

import contextlib
import hashlib
import hmac
import ipaddress
import json
import os
import re
import secrets
import socket
import threading
import time
from collections import deque
from collections.abc import Callable
from datetime import datetime
from functools import lru_cache
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from fastapi import Request
from fastapi.responses import HTMLResponse, JSONResponse

from gammad.config import IST
from gammad.log import get_logger

log = get_logger(__name__)

LOOPBACK = "127.0.0.1"
COOKIE_NAME = "gamma_session"
COOKIE_MAX_AGE_S = 30 * 24 * 3600
SECRET_FILE = "lan_secret"
SECRET_BYTES = 32
SEEN_FILE = "remote_seen.json"
SEEN_WRITE_EVERY_S = 60.0
LOGIN_LIMIT = 5
LOGIN_WINDOW_S = 60.0
WS_CLOSE_UNAUTHED = 4401
WS_CLOSE_NO_PASSWORD = 4403
WS_CLOSE_FOREIGN_ORIGIN = 4403  # API_STORE-01: a loopback /ws opened by a cross-site page (closed before accept)
# API_STORE-01 fix round 1: the only loopback Origins besides the request's own Host = dashboard/vite.config.ts
# `server` / `preview` ports (tests/test_audit_lan_origin.py reads that file, so this cannot drift). Never "any port":
# a page of another local server (a dev server, Jupyter, a saved HTML report) must not reach /ws or run jobs.
VITE_PORTS = (5173, 4173)
VITE_ORIGINS = frozenset(f"http://{h}:{p}" for h in ("127.0.0.1", "localhost", "[::1]") for p in VITE_PORTS)
# API_STORE-03 (owner pick, round 4): the first four + seven more that common proxies / CDNs / IIS ARR add. A proxy that
# sends none of them stays undetectable (see the module docstring).
FORWARD_HEADERS = frozenset({
    b"x-forwarded-for", b"x-forwarded-host", b"forwarded", b"x-real-ip",
    b"via", b"x-forwarded-proto", b"x-forwarded-port", b"cf-connecting-ip", b"true-client-ip", b"x-arr-log-id",
    b"x-original-url",
})  # fmt: skip
SAFE_METHODS = frozenset({"GET", "HEAD", "OPTIONS"})
SESSION_PATHS = frozenset({"/api/session", "/api/session/login", "/api/session/logout"})
# Starlette's TestClient peer / Host; a real server scope always carries an IP literal, so these never match a socket.
TEST_CLIENT_HOST = "testclient"
LOOPBACK_NAMES = frozenset({"localhost", "testserver"})
RFC1918 = tuple(ipaddress.ip_network(n) for n in ("10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16"))
_HOST_OK = re.compile(r"[A-Za-z0-9.\-:\[\]_]{1,253}")
_FORWARDED_HOST = re.compile(r'(?i)(?:^|[;,\s])host="?([^";,\s]+)')

NO_PASSWORD_ERROR = "remote access needs GAMMA_LAN_PASSWORD"
NO_PASSWORD_HTML = (
    "<!doctype html><html><head><meta charset='utf-8'><title>GAMMA-UNWIND - remote access is off</title></head>"
    "<body style='font-family:sans-serif;background:#111;color:#ddd;padding:2em;max-width:40em'>"
    "<h1>Remote access is off</h1>"
    "<p>This dashboard was opened from another device or through a proxy. Remote access needs "
    "<b>GAMMA_LAN_PASSWORD</b> in the <code>.env</code> file of the PC that runs GAMMA-UNWIND.</p>"
    "<p>On that PC the dashboard stays at <code>http://127.0.0.1</code> with its usual port. To open it elsewhere, "
    "set a password in <code>.env</code> and restart the service; this page then becomes a login page.</p>"
    "</body></html>"
)


# ------------------------------------------------------------------------------------------ env + addresses
def lan_enabled(env: Any) -> bool:
    return bool(getattr(env, "lan", False))


def _password(env: Any) -> str:
    pw = getattr(env, "lan_password", "")
    return pw if isinstance(pw, str) and pw.strip() else ""


def is_rfc1918(host: Any) -> bool:
    try:
        ip = ipaddress.ip_address(str(host))
    except ValueError:
        return False
    return ip.version == 4 and any(ip in net for net in RFC1918)


def _is_loopback_ip(host: str) -> bool:
    try:
        ip = ipaddress.ip_address(host.strip("[]"))
    except ValueError:
        return False
    mapped = getattr(ip, "ipv4_mapped", None)
    return bool(ip.is_loopback or (mapped is not None and mapped.is_loopback))


def bindable(host: Any) -> bool:
    """make_server's guard: a loopback IP or this PC's private IPv4 only (never 0.0.0.0, ::, a name or a public IP)."""
    return isinstance(host, str) and (_is_loopback_ip(host) or is_rfc1918(host))


def lan_ipv4() -> str | None:
    """This PC's RFC1918 IPv4: the address of the default route (a UDP connect sends no packet), else any private
    address the host name resolves to. None when the PC has none (a VPN-only or offline PC)."""
    for target in (("192.168.0.1", 9), ("10.255.255.255", 9), ("172.16.0.1", 9)):
        with contextlib.suppress(OSError), socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.connect(target)
            ip = s.getsockname()[0]
            if is_rfc1918(ip):
                return str(ip)
    with contextlib.suppress(OSError):
        for ip in socket.gethostbyname_ex(socket.gethostname())[2]:
            if is_rfc1918(ip):
                return str(ip)
    return None


@lru_cache(maxsize=1)
def _lan_ipv4_once() -> str | None:
    return lan_ipv4()


def _discovered() -> str | None:
    # lan_ipv4 is looked up at call time so a test can pin it; the real lookup runs once per process (create_app's
    # TrustedHost list and serve_async's bind must name the same address).
    return _lan_ipv4_once() if lan_ipv4 is _REAL_LAN_IPV4 else lan_ipv4()


def lan_host(env: Any) -> str | None:
    """The private address the LAN bind adds, or None (LAN off, no password, or no RFC1918 address). Never logs."""
    if not lan_enabled(env) or not _password(env):
        return None
    ip = _discovered()
    return ip if is_rfc1918(ip) else None


def bind_hosts(env: Any) -> tuple[str, ...]:
    """The addresses uvicorn binds: loopback always, the LAN address only with GAMMA_LAN=1 AND a password."""
    if not lan_enabled(env):
        return (LOOPBACK,)
    if not _password(env):
        log.error(
            "GAMMA_LAN=1 but GAMMA_LAN_PASSWORD is not set: the dashboard stays on 127.0.0.1 only "
            "(set GAMMA_LAN_PASSWORD in .env and restart to open it to the local network)"
        )
        return (LOOPBACK,)
    ip = lan_host(env)
    if ip is None:
        log.error("GAMMA_LAN=1 but this PC has no private (192.168.x / 10.x / 172.16-31.x) IPv4 address: 127.0.0.1 only")
        return (LOOPBACK,)
    return (LOOPBACK, ip)


def extra_trusted_hosts(runtime: Any) -> tuple[str, ...]:
    """TrustedHostMiddleware's extra name: the LAN address, only when the LAN bind is on."""
    ip = lan_host(_env(runtime))
    return (ip,) if ip else ()


def _env(runtime: Any) -> Any:
    try:  # tools/edition_probe's refusing runtime raises on every attribute: that is "no settings", not a crash
        return getattr(getattr(runtime, "cfg", None), "env", None)
    except Exception:  # noqa: BLE001
        return None


def _data_dir(runtime: Any) -> Path | None:
    try:
        d = getattr(runtime, "data_dir", None)
        return Path(d) if d is not None else Path(runtime.db_path).parent
    except Exception:  # noqa: BLE001
        return None


# ------------------------------------------------------------------------------------------ request classification
def _headers(scope: dict[str, Any]) -> dict[bytes, str]:
    out: dict[bytes, str] = {}
    for k, v in scope.get("headers") or ():
        key = bytes(k).lower()
        if key not in out:
            out[key] = bytes(v).decode("latin-1")
    return out


def _strip_port(host: str) -> str:
    h = host.strip().lower()
    if h.startswith("["):
        return h[1 : h.find("]")] if "]" in h else h
    return h.rsplit(":", 1)[0] if h.count(":") == 1 else h


def _loopback_name(host: str) -> bool:
    return host in LOOPBACK_NAMES or _is_loopback_ip(host)


def _client_is_loopback(scope: dict[str, Any]) -> bool:
    client = scope.get("client")
    host = str(client[0]) if client else ""
    return host == TEST_CLIENT_HOST or _is_loopback_ip(host)


def is_remote(scope: dict[str, Any]) -> bool:
    hdrs = _headers(scope)
    return not _client_is_loopback(scope) or any(h in hdrs for h in FORWARD_HEADERS)


def forwarded_host(scope: dict[str, Any]) -> str | None:
    """The public name a proxied request was addressed to, when it is not a loopback name; None otherwise."""
    hdrs = _headers(scope)
    if not any(h in hdrs for h in FORWARD_HEADERS):
        return None
    raw = hdrs.get(b"x-forwarded-host", "").split(",", 1)[0]
    if not raw.strip():
        m = _FORWARDED_HOST.search(hdrs.get(b"forwarded", ""))
        raw = m.group(1) if m else hdrs.get(b"host", "")
    host = _strip_port(raw)
    if not host or _loopback_name(host):
        return None
    return host if _HOST_OK.fullmatch(host) else "<unreadable host>"


def _origin_ok(scope: dict[str, Any]) -> bool:
    hdrs = _headers(scope)
    origin = hdrs.get(b"origin", "").strip()
    if not origin or origin == "null":
        return False
    try:  # API_STORE-01 fix round 1: a malformed Origin (e.g. "http://[::1") is refused, never a 500
        netloc = urlsplit(origin).netloc.lower()
    except ValueError:
        return False
    allowed = {hdrs.get(b"host", "").strip().lower(), hdrs.get(b"x-forwarded-host", "").split(",", 1)[0].strip().lower()}
    return bool(netloc) and netloc in allowed - {""}


def _loopback_origin_ok(scope: dict[str, Any]) -> bool:
    """API_STORE-01 / -02 (D95, D98): a loopback request is refused only when a browser page of ANOTHER site sent it.
    No Origin (tray httpx, curl) passes; the same origin as Host passes; VITE_ORIGINS pass (`vite dev` / `vite
    preview` keep the browser's Origin while changeOrigin rewrites Host to the API port). Anything else fails: a
    foreign site, "null" (sandboxed iframe, file://), a malformed value, another local server's port."""
    origin = _headers(scope).get(b"origin", "").strip()
    if not origin:
        return True
    return _origin_ok(scope) or origin.lower() in VITE_ORIGINS


def _hidden(method: str, path: str) -> bool:
    return path.startswith(("/api/token", "/auth/")) or (method == "POST" and path == "/api/service/stop")


def _is_api(path: str) -> bool:
    return path == "/api" or path.startswith("/api/")


def _cookie(scope: dict[str, Any]) -> str:
    from http.cookies import SimpleCookie

    raw = _headers(scope).get(b"cookie", "")
    if not raw:
        return ""
    jar: SimpleCookie = SimpleCookie()
    with contextlib.suppress(Exception):
        jar.load(raw)
    morsel = jar.get(COOKIE_NAME)
    return morsel.value if morsel is not None else ""


# ------------------------------------------------------------------------------------------ secret + cookie
class _Secret:
    """data/lan_secret: 32 random bytes as hex, created once per install; cached per process."""

    def __init__(self) -> None:
        self._cache: dict[Path, bytes] = {}
        self._lock = threading.Lock()

    def get(self, data_dir: Path) -> bytes:
        path = data_dir / SECRET_FILE
        with self._lock:
            if path not in self._cache:
                self._cache[path] = self._load_or_create(path)
            return self._cache[path]

    @staticmethod
    def _load_or_create(path: Path) -> bytes:
        with contextlib.suppress(OSError, ValueError):
            value = bytes.fromhex(path.read_text(encoding="ascii").strip())
            if len(value) == SECRET_BYTES:
                return value
            log.warning("%s is not a %d-byte secret; writing a new one (old dashboard logins end)", path.name, SECRET_BYTES)
        value = secrets.token_bytes(SECRET_BYTES)
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
        fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC | getattr(os, "O_BINARY", 0), 0o600)
        with os.fdopen(fd, "w", encoding="ascii") as fh:
            fh.write(value.hex())
        os.replace(tmp, path)
        with contextlib.suppress(OSError):
            os.chmod(path, 0o600)  # POSIX: owner read/write only; Windows keeps the user-profile ACL
        log.info("created %s (the per-install dashboard login key)", path.name)
        return value


def _key(secret: bytes, password: str) -> bytes:
    pw_hash = hashlib.sha256(password.encode("utf-8")).digest()
    return hmac.new(secret, b"gamma-lan-cookie-v1|" + pw_hash, hashlib.sha256).digest()


def sign(secret: bytes, password: str, issued_s: int) -> str:
    sig = hmac.new(_key(secret, password), f"v1|{issued_s}".encode(), hashlib.sha256).hexdigest()
    return f"{issued_s}.{sig}"


def verify(secret: bytes, password: str, value: str, now_s: float) -> bool:
    issued_raw, _, sig = value.partition(".")
    if not issued_raw.isdigit() or not sig:
        return False
    issued = int(issued_raw)
    if not (issued - 60 <= now_s < issued + COOKIE_MAX_AGE_S):
        return False
    return hmac.compare_digest(sign(secret, password, issued), value)


def password_matches(given: str, password: str) -> bool:
    return hmac.compare_digest(hashlib.sha256(given.encode("utf-8")).digest(), hashlib.sha256(password.encode("utf-8")).digest())


# ------------------------------------------------------------------------------------------ remote_seen + limiter
class RemoteSeen:
    """data/remote_seen.json = the latest proxied host {host, first_seen_ist, last_seen_ist, count}; counts live in
    memory, the file is written at most once a minute; one WARNING per host per process."""

    def __init__(self, clock: Callable[[], float] = time.monotonic) -> None:
        self.hosts: dict[str, dict[str, Any]] = {}
        self.warned: set[str] = set()
        self.writes = 0
        self._last_write: float | None = None
        self._clock = clock
        self._lock = threading.Lock()

    def hit(self, host: str, data_dir: Path | None, *, password_set: bool) -> None:
        now = datetime.now(IST).isoformat(timespec="seconds")
        with self._lock:
            rec = self.hosts.get(host)
            if rec is None:
                rec = self._seed(host, data_dir, now)
                self.hosts[host] = rec
            rec["count"] += 1
            rec["last_seen_ist"] = now
            if host not in self.warned:
                self.warned.add(host)
                log.warning(
                    "dashboard reached through a proxy or tunnel as %s (%s); %s",
                    host,
                    SEEN_FILE,
                    "remote visitors must log in with GAMMA_LAN_PASSWORD" if password_set
                    else "GAMMA_LAN_PASSWORD is not set, so every remote request is refused (403)",
                )
            t = self._clock()
            if data_dir is None or (self._last_write is not None and t - self._last_write < SEEN_WRITE_EVERY_S):
                return
            self._last_write = t
            snapshot = dict(rec)
        self._write(data_dir / SEEN_FILE, snapshot)

    @staticmethod
    def _seed(host: str, data_dir: Path | None, now: str) -> dict[str, Any]:
        rec: dict[str, Any] = {"host": host, "first_seen_ist": now, "last_seen_ist": now, "count": 0}
        if data_dir is not None:  # a restart keeps the first sighting and the running count of the same host
            with contextlib.suppress(OSError, ValueError, TypeError, AttributeError):
                old = json.loads((data_dir / SEEN_FILE).read_text(encoding="utf-8"))
                if old.get("host") == host:
                    rec["first_seen_ist"] = str(old.get("first_seen_ist") or now)
                    rec["count"] = max(0, int(old.get("count") or 0))
        return rec

    def _write(self, path: Path, rec: dict[str, Any]) -> None:
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
            tmp.write_text(json.dumps(rec, indent=1) + "\n", encoding="utf-8")
            os.replace(tmp, path)
            self.writes += 1
        except OSError as exc:
            log.warning("could not write %s (%s)", path.name, type(exc).__name__)


class LoginLimiter:
    def __init__(self, limit: int = LOGIN_LIMIT, window_s: float = LOGIN_WINDOW_S,
                 clock: Callable[[], float] = time.monotonic) -> None:  # fmt: skip
        self.limit, self.window_s, self._clock = limit, window_s, clock
        self._hits: dict[str, deque[float]] = {}
        self._lock = threading.Lock()

    def allow(self, key: str) -> bool:
        now = self._clock()
        with self._lock:
            q = self._hits.setdefault(key, deque())
            while q and now - q[0] >= self.window_s:
                q.popleft()
            if len(q) >= self.limit:
                return False
            q.append(now)
            return True


# ------------------------------------------------------------------------------------------ the guard
class LanGuard:
    """Per-app state (app.state.lan): the runtime seam, the secret cache, remote_seen and the login limiter."""

    def __init__(self, runtime: Any) -> None:
        self.runtime = runtime
        self.secret = _Secret()
        self.seen = RemoteSeen()
        self.limiter = LoginLimiter()

    def env(self) -> Any:
        return _env(self.runtime)

    def password(self) -> str:
        return _password(self.env())

    def data_dir(self) -> Path | None:
        return _data_dir(self.runtime)

    def issue(self, now_s: float | None = None) -> str | None:
        d, pw = self.data_dir(), self.password()
        if d is None or not pw:
            return None
        return sign(self.secret.get(d), pw, int(time.time() if now_s is None else now_s))

    def authed(self, scope: dict[str, Any]) -> bool:
        value, d, pw = _cookie(scope), self.data_dir(), self.password()
        if not value or d is None or not pw:
            return False
        try:
            return verify(self.secret.get(d), pw, value, time.time())
        except OSError:
            return False

    def session(self, scope: dict[str, Any]) -> dict[str, bool]:
        remote = is_remote(scope)
        env = self.env()
        return {
            "remote": remote,
            "authed": (not remote) or self.authed(scope),
            "lan": lan_enabled(env) and bool(_password(env)),
        }


class LanMiddleware:
    """Pure ASGI (http + websocket); see the module docstring for the rules and their order."""

    def __init__(self, app: Any, guard: LanGuard) -> None:
        self.app = app
        self.guard = guard

    async def __call__(self, scope: dict[str, Any], receive: Any, send: Any) -> None:
        kind = scope.get("type")
        if kind not in ("http", "websocket"):
            await self.app(scope, receive, send)
            return
        if not is_remote(scope):
            # API_STORE-01 / -02: the Origin rule also covers loopback (a cross-site page in this PC's browser)
            if not _loopback_origin_ok(scope):
                if kind == "websocket":
                    await _ws_refuse(receive, send, WS_CLOSE_FOREIGN_ORIGIN)
                    return
                if str(scope.get("method") or "GET").upper() not in SAFE_METHODS:
                    await _json(scope, receive, send, 403, {"error": "cross-origin request refused"})
                    return
            await self.app(scope, receive, send)
            return
        g = self.guard
        pw_set = bool(g.password())
        fwd = forwarded_host(scope)
        if fwd is not None:
            g.seen.hit(fwd, g.data_dir(), password_set=pw_set)
        path = str(scope.get("path") or "/")
        method = str(scope.get("method") or "GET").upper()
        if not pw_set:  # D133 Q3: fail closed
            if kind == "websocket":
                await _ws_close(receive, send, WS_CLOSE_NO_PASSWORD)
            elif _is_api(path):
                await _json(scope, receive, send, 403, {"error": NO_PASSWORD_ERROR})
            else:
                await _html(scope, receive, send, 403, NO_PASSWORD_HTML)
            return
        if kind == "http" and _hidden(method, path):
            await _json(scope, receive, send, 404, {"detail": "Not Found"})
            return
        if kind == "http" and method not in SAFE_METHODS and not _origin_ok(scope):
            await _json(scope, receive, send, 403, {"error": "cross-origin request refused"})
            return
        if kind == "http" and path in SESSION_PATHS:
            await self.app(scope, receive, send)
            return
        if not g.authed(scope):
            if kind == "websocket":
                await _ws_close(receive, send, WS_CLOSE_UNAUTHED)
            elif _is_api(path) or path.startswith("/auth/"):
                await _json(scope, receive, send, 401, {"error": "login required"})
            else:
                await self.app(scope, receive, send)  # the SPA shell + assets, so #/login can load
            return
        await self.app(scope, receive, send)


async def _json(scope: Any, receive: Any, send: Any, status: int, body: dict[str, Any]) -> None:
    await JSONResponse(body, status_code=status, headers={"Cache-Control": "no-store"})(scope, receive, send)


async def _html(scope: Any, receive: Any, send: Any, status: int, body: str) -> None:
    await HTMLResponse(body, status_code=status, headers={"Cache-Control": "no-store"})(scope, receive, send)


async def _ws_close(receive: Any, send: Any, code: int) -> None:
    msg = await receive()
    if msg.get("type") != "websocket.connect":
        return
    await send({"type": "websocket.accept"})
    await send({"type": "websocket.close", "code": code})


async def _ws_refuse(receive: Any, send: Any, code: int) -> None:
    """Close BEFORE accept (the server answers the handshake with HTTP 403): a cross-site page never gets a socket."""
    msg = await receive()
    if msg.get("type") != "websocket.connect":
        return
    await send({"type": "websocket.close", "code": code})


# ------------------------------------------------------------------------------------------ install
def install(app: Any, *, runtime: Any) -> None:
    """Add the guard and the three session endpoints to a FastAPI app (create_app calls this before any route)."""
    guard = LanGuard(runtime)
    app.state.lan = guard
    app.add_middleware(LanMiddleware, guard=guard)
    env = guard.env()
    d = guard.data_dir()
    if lan_enabled(env) and _password(env) and d is not None:  # "created on the first LAN start"
        with contextlib.suppress(OSError):
            guard.secret.get(d)

    @app.get("/api/session")
    def session(request: Request) -> dict[str, bool]:
        return guard.session(request.scope)

    @app.post("/api/session/login")
    async def session_login(request: Request) -> Any:
        addr = request.client.host if request.client else "?"
        if not guard.limiter.allow(addr):
            return JSONResponse({"error": "too many attempts; wait a minute"}, status_code=429,
                                headers={"Retry-After": str(int(LOGIN_WINDOW_S))})  # fmt: skip
        ctype = request.headers.get("content-type", "").split(";", 1)[0].strip().lower()
        if ctype != "application/json":
            return JSONResponse({"error": "Content-Type application/json required"}, status_code=415)
        password = guard.password()
        if not password:
            return JSONResponse({"error": NO_PASSWORD_ERROR}, status_code=403)
        try:
            body = json.loads(await request.body() or b"null")
        except ValueError:
            body = None
        given = body.get("password") if isinstance(body, dict) else None
        if not isinstance(given, str):
            return JSONResponse({"error": 'body must be JSON {"password": "..."}'}, status_code=400)
        if not password_matches(given, password):
            log.warning("dashboard login refused (wrong password) from %s", addr)
            return JSONResponse({"error": "wrong password"}, status_code=401)
        token = guard.issue()
        if token is None:
            return JSONResponse({"error": "login unavailable (no data folder)"}, status_code=500)
        resp = JSONResponse({"ok": True}, headers={"Cache-Control": "no-store"})
        resp.set_cookie(COOKIE_NAME, token, max_age=COOKIE_MAX_AGE_S, path="/", httponly=True, samesite="strict",
                        secure=_https(request.scope))  # fmt: skip
        log.info("dashboard login from %s", addr)
        return resp

    @app.post("/api/session/logout")
    def session_logout(request: Request) -> Any:
        resp = JSONResponse({"ok": True}, headers={"Cache-Control": "no-store"})
        resp.delete_cookie(COOKIE_NAME, path="/", httponly=True, samesite="strict", secure=_https(request.scope))
        return resp


def _https(scope: dict[str, Any]) -> bool:
    proto = _headers(scope).get(b"x-forwarded-proto", "").split(",", 1)[0].strip().lower()
    return scope.get("scheme") in ("https", "wss") or proto == "https"


_REAL_LAN_IPV4 = lan_ipv4
