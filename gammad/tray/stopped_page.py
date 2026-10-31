"""T-087 / D93 / D95(1): the page the tray supervisor serves while the service is STOPPED or in ERROR.

The service (feed, scheduler, alerts AND dashboard) is a child process on 127.0.0.1:<GAMMA_PORT>. When it is
down, nothing answers that port, so an open dashboard tab has nowhere to go and "Start service" cannot be a
button on a page the service itself serves. The supervisor therefore binds the SAME address while the service
is down (no new port: 8761 is the smoke port) and serves one minimal page:

    Service stopped -- [Start service] . [Open dashboard]      (+ the error text when the state is ERROR)

Routes (stdlib http.server in a daemon thread; no new dependency):
    GET  /supervisor/status  200 {"service": "stopped"|"error"|..., "detail": str, "supervisor": true}
    GET  /api/...            503 the same JSON (the dashboard's REST calls see "service down", not a 404)
    GET  anything else       200 the page (a dashboard tab reloading lands here)
    POST /supervisor/start   header `X-Gamma-Control: 1` required, else 403 -> 202 and the supervisor is asked
                             to start the service (it CLOSES this listener and waits for the port to be free
                             before launching the child, else the child exits 4 = port busy, D31)
    OPTIONS / other POST     403 / 404, never any Access-Control-* header

Why the custom header (D95(1)): a cross-origin page cannot send it without a CORS preflight, and no preflight is
ever answered here, so another web page cannot start (or, on the service, stop) GAMMA. A request whose Host is
not 127.0.0.1:<port> / localhost:<port> is refused too (DNS rebinding), and so is a POST carrying a foreign Origin.

Binding: 127.0.0.1 only, never 0.0.0.0. `allow_reuse_address` is off and, on Windows, SO_EXCLUSIVEADDRUSE is set,
so nothing can co-bind the port while the page holds it (the service must find the port FREE, not shared).
"""

from __future__ import annotations

import contextlib
import html
import json
import os
import socket
import threading
import time
from collections.abc import Callable
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from gammad.log import get_logger

log = get_logger(__name__)

HOST = "127.0.0.1"
CONTROL_HEADER = "X-Gamma-Control"
CONTROL_VALUE = "1"
START_PATH = "/supervisor/start"
STATUS_PATH = "/supervisor/status"

StateFn = Callable[[], tuple[str, str]]  # -> (state, detail)


def _exclusive(sock: socket.socket) -> None:
    """Windows: nobody may co-bind this port while we hold it (SO_REUSEADDR elsewhere would allow it)."""
    opt = getattr(socket, "SO_EXCLUSIVEADDRUSE", None)
    if os.name == "nt" and opt is not None:
        sock.setsockopt(socket.SOL_SOCKET, opt, 1)


def port_free(port: int, host: str = HOST) -> bool:
    """True when a plain listener could bind host:port right now (the probe socket is closed at once)."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        _exclusive(s)
        s.bind((host, port))
    except OSError:
        return False
    finally:
        s.close()
    return True


def wait_port_free(port: int, timeout_s: float, *, host: str = HOST, step_s: float = 0.05) -> bool:
    """Poll `port_free` until it is True or `timeout_s` passes. Returns the last answer."""
    deadline = time.monotonic() + timeout_s
    while True:
        if port_free(port, host):
            return True
        if time.monotonic() >= deadline:
            return False
        time.sleep(step_s)


class _Server(ThreadingHTTPServer):
    allow_reuse_address = False
    daemon_threads = True
    block_on_close = False

    def server_bind(self) -> None:
        _exclusive(self.socket)
        super().server_bind()


PAGE = """<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>GAMMA-UNWIND - service {state}</title>
<style>
body{{margin:0;min-height:100vh;display:flex;align-items:center;justify-content:center;background:#0d1117;
color:#c9d1d9;font:15px/1.5 "Segoe UI",system-ui,sans-serif}}
main{{background:#161b22;border:1px solid #30363d;border-radius:10px;padding:28px 32px;max-width:520px}}
h1{{font-size:20px;margin:0 0 6px}} p{{margin:6px 0 18px;color:#8b949e}}
.err{{color:#f85149;white-space:pre-wrap;word-break:break-word}}
button,a.btn{{font:inherit;padding:8px 16px;border-radius:6px;border:1px solid #30363d;cursor:pointer;
text-decoration:none;display:inline-block;margin-right:8px}}
button{{background:#238636;color:#fff;border-color:#2ea043}} a.btn{{background:#21262d;color:#c9d1d9}}
button[disabled]{{opacity:.6;cursor:wait}} #msg{{margin-top:14px;color:#8b949e;min-height:1.5em}}
</style></head>
<body><main>
<h1>Service {state}</h1>
<p>GAMMA-UNWIND is not running. Alerts and the dashboard come back when the service starts.</p>
{error}
<button id="start" type="button">Start service</button> <a class="btn" href="/">Open dashboard</a>
<div id="msg"></div>
</main>
<script>
const msg = document.getElementById("msg"), btn = document.getElementById("start");
async function waitForService() {{
  for (let i = 0; i < 120; i++) {{
    await new Promise(r => setTimeout(r, 1000));
    try {{
      const r = await fetch("/api/status", {{cache: "no-store"}});
      if (r.ok) {{ location.href = "/"; return; }}
    }} catch (e) {{ /* the port is changing hands */ }}
  }}
  msg.textContent = "Still starting - check the tray icon, or reload this page.";
  btn.disabled = false;
}}
btn.addEventListener("click", async () => {{
  btn.disabled = true; msg.textContent = "Starting...";
  try {{
    const r = await fetch("{start}", {{method: "POST", headers: {{"{header}": "{value}"}}}});
    if (!r.ok) throw new Error("HTTP " + r.status);
  }} catch (e) {{ msg.textContent = "Could not ask the tray to start the service (" + e.message + ")."; btn.disabled = false; return; }}
  waitForService();
}});
</script>
</body></html>
"""


def render_page(state: str, detail: str) -> str:
    word = "error" if state == "error" else "stopped"
    error = f'<p class="err">{html.escape(detail)}</p>' if detail else ""
    return PAGE.format(
        state=word, error=error, start=START_PATH, header=CONTROL_HEADER, value=CONTROL_VALUE
    )


def _handler(port: int, state: StateFn, on_start: Callable[[], None]) -> type[BaseHTTPRequestHandler]:
    hosts = {f"{HOST}:{port}", f"localhost:{port}"}
    origins = {f"http://{h}" for h in hosts}

    class Handler(BaseHTTPRequestHandler):
        server_version = "gamma-supervisor"
        sys_version = ""

        def log_message(self, fmt: str, *args: Any) -> None:  # stay out of stderr (pythonw has none)
            log.debug("stopped page: " + fmt, *args)

        def _send(self, code: int, body: bytes, ctype: str) -> None:
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.end_headers()
            if self.command != "HEAD":
                self.wfile.write(body)

        def _json(self, code: int, obj: dict[str, Any]) -> None:
            self._send(code, json.dumps(obj).encode("utf-8"), "application/json")

        def _status_body(self) -> dict[str, Any]:
            st, detail = state()
            return {"service": st, "detail": detail, "supervisor": True}

        def _host_ok(self) -> bool:
            if (self.headers.get("Host") or "").strip().lower() in hosts:
                return True
            self._json(403, {"detail": "forbidden host"})
            return False

        def do_GET(self) -> None:
            if not self._host_ok():
                return
            path = self.path.split("?", 1)[0]
            if path == STATUS_PATH:
                self._json(200, self._status_body())
            elif path.startswith("/api/") or path == "/api" or path == "/ws":
                self._json(503, self._status_body())
            else:
                st, detail = state()
                self._send(200, render_page(st, detail).encode("utf-8"), "text/html; charset=utf-8")

        do_HEAD = do_GET

        def do_POST(self) -> None:
            if not self._host_ok():
                return
            origin = self.headers.get("Origin")
            if origin is not None and origin.strip().lower() not in origins:
                self._json(403, {"detail": "forbidden origin"})
                return
            path = self.path.split("?", 1)[0]
            if path != START_PATH:
                self._json(503 if path.startswith("/api/") else 404, self._status_body())
                return
            if (self.headers.get(CONTROL_HEADER) or "").strip() != CONTROL_VALUE:
                self._json(403, {"detail": f"{CONTROL_HEADER}: {CONTROL_VALUE} header required"})
                return
            # T-097: queue the start FIRST, answer after it returned -- a control-loop tick right after the 202
            # must find the command (the old answer-then-queue order raced it).
            try:
                on_start()
            except Exception:
                log.exception("stopped page: start request failed")
                self._json(500, {"ok": False, "detail": "start request failed"})  # never the exception text
                return
            self._json(202, {"ok": True, "starting": True})

        def do_OPTIONS(self) -> None:
            self._json(403, {"detail": "no cross-origin control"})

    return Handler


class StoppedPage:
    """The listener on 127.0.0.1:<port> while the service is down. `open()` / `close()` are called from the
    supervisor's control loop only; `state` and `on_start` run on the page's request threads."""

    def __init__(self, port: int, *, state: StateFn, on_start: Callable[[], None]) -> None:
        self.port = port
        self._state = state
        self._on_start = on_start
        self._server: _Server | None = None
        self._thread: threading.Thread | None = None
        self._bind_failing = False

    @property
    def is_open(self) -> bool:
        return self._server is not None

    def open(self) -> bool:
        """Bind and serve. False (logged once per failing streak) when the port is taken."""
        if self._server is not None:
            return True
        try:
            server = _Server((HOST, self.port), _handler(self.port, self._state, self._on_start))
        except OSError as exc:
            if not self._bind_failing:
                log.warning("tray: stopped page cannot bind %s:%d (%s); retrying", HOST, self.port, exc)
            self._bind_failing = True
            return False
        self._bind_failing = False
        self._server = server
        self._thread = threading.Thread(
            target=server.serve_forever, kwargs={"poll_interval": 0.2}, name="stopped-page", daemon=True
        )
        self._thread.start()
        log.info("tray: stopped page on http://%s:%d", HOST, self.port)
        return True

    def close(self) -> None:
        """Stop serving and close the listener (the port is free when this returns, bar the OS)."""
        server, self._server = self._server, None
        thread, self._thread = self._thread, None
        if server is None:
            return
        with contextlib.suppress(Exception):
            server.shutdown()
        with contextlib.suppress(Exception):
            server.server_close()
        if thread is not None:
            thread.join(timeout=2.0)
        log.info("tray: stopped page closed")
