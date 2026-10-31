"""IN5 LTP-only WS feed. Undocumented OI, volume and LTT stay absent; ts_ms uses receipt.

HTTP 401 / TokenException => 809; HTTP 403 / UserException => 806. Park until reload.
"""

import asyncio
import contextlib
import json
import time

from gammad.broker.indmoney import auth, wire
from gammad.watch.feed_protocol import FeedBase, FeedCaps, Tick, normalize_instruments

RANK = {"TICKER": 0, "QUOTE": 1, "FULL": 2}


class IndmoneyFeed(FeedBase):
    caps = FeedCaps("ws", None, wire.MAX_FEED)

    def __init__(self, creds, out, on_auth_error=None, *, connect=None, clock_ms=None, resolve=None):
        super().__init__(out, on_auth_error=on_auth_error)
        self._token = auth.token_from(creds)
        self._connect = connect or self._connector
        self._clock_ms = clock_ms or (lambda: int(time.time() * 1000))
        self._resolve = resolve or self._passthrough
        self._subs = {}
        self._natives = {}
        # Frames omit segment: None marks a token shared by multiple accepted subscriptions.
        self._by_native_id: dict[str, tuple[str, int] | None] = {}
        self._ws = None
        self._task = None
        self._pending = set()
        self._change_lock = asyncio.Lock()

    async def _connector(self, url):
        import websockets

        return await websockets.connect(url, additional_headers={"Authorization": self._token},
                                        ping_interval=30, ping_timeout=30)

    @staticmethod
    def _passthrough(segment, sid):
        # Common E: test-only fallback. Production always supplies the adapter's identity resolver.
        prefix = wire.PREFIX.get(segment)
        return f"{prefix}:{sid}" if prefix else None

    def subscribed_ids(self):
        return frozenset(self._subs)

    def pending_ids(self):
        return frozenset(self._pending)

    async def add_instruments(self, instruments):
        async with self._change_lock:
            changes = {}
            for seg, sid, mode in normalize_instruments(instruments):
                key = (seg, sid)
                old = changes.get(key, self._subs.get(key))
                if old is None or RANK[mode] > RANK[old]:
                    changes[key] = mode
            fresh = [k for k in changes if k not in self._subs]
            natives = await self._resolve_off_loop(fresh, self._resolve, "indmoney feed")
            if len(self._subs) + len(natives) > self.caps.max_instruments:
                raise self._capacity_error(len(self._subs), [(s, i, changes[s, i]) for s, i in natives],
                                           "indmoney feed")
            accepted = {k: v for k, v in changes.items() if k in self._subs or k in natives}
            self._subs.update(accepted)
            self._natives.update(natives)
            for key, native in natives.items():
                token = native.split(":", 1)[1]
                if token not in self._by_native_id:
                    self._by_native_id[token] = key
                elif self._by_native_id[token] != key:
                    self._by_native_id[token] = None
            self._accept(accepted)
            self._pending.update(accepted)
            if self._ws is not None and accepted:
                try:
                    await self._ws.send(json.dumps(wire.subscribe_message([self._natives[k] for k in accepted])))
                    self._pending.difference_update(accepted)
                except Exception:  # noqa: BLE001 -- any send failure must reconnect and replay
                    # Force reconnect/full replay; keep pending until a send succeeds.
                    await self._ws.close()
            return len(accepted)

    async def start(self, instruments=()):
        await self.add_instruments(instruments)
        if self._task is None:
            self._task = asyncio.create_task(self._run())

    def reload_credentials(self, creds):
        self._token = auth.token_from(creds)
        self._unblock()

    async def stop(self):
        self._stop.set()
        self._auth_ok.set()
        if self._task:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)

    def decode(self, message, recv_ms):
        try:
            body = json.loads(message)
            if isinstance(body, str):
                body = json.loads(body)
            if not isinstance(body, dict):
                return []
            code = wire.auth_code(body)
            if code:
                self._block(code, "INDmoney feed authentication rejected")
                return []
            if body.get("mode") != "ltp" or body.get("data", {}).get("ltp") is None:
                return []
            # Price frames omit segment. Ambiguous native IDs across segments are never guessed.
            key = self._by_native_id.get(str(body.get("instrument")))
            if key is None:
                return []
            seg, sid = key
            return [Tick(recv_ms, recv_ms, sid, seg, float(body["data"]["ltp"]), None, None,
                         None, None, None, None)]
        except (ValueError, TypeError, AttributeError, KeyError):
            return []

    async def _run(self):
        delay = 1
        while not self._stop.is_set():
            await self._auth_ok.wait()
            ws = None
            try:
                ws = await self._connect(wire.WS_URL)
                async with self._change_lock:
                    self._ws = ws
                    keys = list(self._subs)
                    if keys:
                        await ws.send(json.dumps(wire.subscribe_message([self._natives[k] for k in keys])))
                    self._pending.difference_update(keys)
                while not self._stop.is_set() and not self.auth_blocked:
                    message = await asyncio.wait_for(ws.recv(), timeout=60)
                    for tick in self.decode(message, self._clock_ms()):
                        self._emit(tick)
                        delay = 1
            except Exception as exc:  # noqa: BLE001 -- network failures reconnect, auth parks
                response = getattr(exc, "response", None)
                status = getattr(exc, "status_code", getattr(response, "status_code", 0))
                body = getattr(exc, "body", None) or getattr(response, "body", None)
                try:
                    payload = json.loads(body) if body else {}
                except (ValueError, TypeError):
                    payload = {}
                code = wire.auth_code(payload if isinstance(payload, dict) else {}, status)
                if code:
                    self._block(code, "INDmoney feed handshake rejected")
            finally:
                self._ws = None
                self._pending.update(self._subs)
                if ws:
                    with contextlib.suppress(Exception):
                        await ws.close()
            if not self.auth_blocked:
                await self._sleep(delay)
                delay = min(30, delay * 2)
