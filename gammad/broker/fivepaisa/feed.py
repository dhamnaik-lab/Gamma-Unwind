"""FP54-W price/OI join, with canonical identities and receive-time ticks.

OI-first frames wait for price; FULL derivatives wait for their initial OI.
Later OI changes emit the carried price, never an OI-only tick. Unknown Time
units and fetch-time TickDt are not promoted into exchange last-trade times.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import time

from gammad.broker.fivepaisa import auth, wire
from gammad.log import get_logger
from gammad.watch.feed_protocol import FeedBase, FeedCaps, Tick, normalize_instruments

_RANK = {"TICKER": 0, "QUOTE": 1, "FULL": 2}
log = get_logger(__name__)


async def connector(url):
    import websockets

    # Standard WS ping/pong only. An application heartbeat is UNSOURCED (FP54-U).
    return await websockets.connect(url, ping_interval=20, ping_timeout=20, max_size=2**20)


class FivePaisaFeed(FeedBase):
    caps = FeedCaps("ws", None, wire.MAX_FEED)

    def __init__(self, creds, out, on_auth_error=None, *, resolve=None, connect=None, clock_ms=None):
        super().__init__(out, on_auth_error=on_auth_error)
        self._token, self._client, self._app = auth.credentials(creds)
        self._resolve = resolve
        self._connect = connect or connector
        self._clock_ms = clock_ms or (lambda: int(time.time() * 1000))
        self._subs = {}
        self._native = {}
        self._canon = {}
        self._state = {}
        self._sent = set()
        self._oi_wait_since = {}
        self._oi_seen = set()
        self._oi_warned = set()
        self._task = None
        self._retired = set()
        self._ws = None
        self._started = False
        self._lock = asyncio.Lock()
        self.bad_frames = 0
        self.connects = 0

    def _carried(self):
        return self._subs.keys()

    def pending_ids(self):
        return frozenset(set(self._subs) - self._sent)

    async def start(self, instruments=()):
        self._stop.clear()
        self._started = True
        await self.add_instruments(instruments)
        self._spawn()

    async def add_instruments(self, instruments):
        async with self._lock:
            wanted = {}
            for seg, sid, mode in normalize_instruments(instruments):
                key = (seg, sid)
                if _RANK[mode] > _RANK.get(wanted.get(key, self._subs.get(key)), -1):
                    wanted[key] = mode
            resolver = self._resolve or self._passthrough
            mapped = await self._resolve_off_loop(list(wanted), resolver, "5paisa")
            fresh = [(s, i, wanted[s, i]) for s, i in mapped if (s, i) not in self._subs]
            if len(self._subs) + len(fresh) > self.caps.max_instruments:
                raise self._capacity_error(len(self._subs), fresh, "5paisa")
            for key, native in mapped.items():
                self._native[key] = native
                self._canon[native] = key
                self._subs[key] = wanted[key]
            self._accept(mapped)
            self._sent.difference_update(mapped)
            if self._ws is not None and not self.auth_blocked:
                try:
                    await self._subscribe(self._ws, list(mapped))
                except Exception:  # noqa: BLE001 -- any failed send requires a fresh socket
                    await self._ws.close()
            self._spawn()
            return len(mapped)

    @staticmethod
    def _passthrough(seg, sid):
        """AMENDED common E: harness-only path; production always supplies adapter."""
        return ("N", wire.SEGMENTS[seg], sid) if seg in wire.SEGMENTS else None

    def _spawn(self):
        if self._started and self._task is None and self._subs and not self._stop.is_set():
            self._task = asyncio.create_task(self._run())

    async def _subscribe(self, ws, keys):
        for msg in wire.subscribe_messages(self._client, [self._native[k] for k in keys]):
            await ws.send(msg)
        self._sent.update(keys)
        for key in keys:
            if key[0] == "NSE_FNO" and self._subs[key] == "FULL" and key not in self._oi_seen:
                self._oi_wait_since.setdefault(key, self._clock_ms())

    def _warn_oi_wait(self, now_ms):
        """Once per canonical id per feed lifetime, including across reconnects."""
        for key, since in self._oi_wait_since.items():
            if key not in self._oi_warned and now_ms - since > 60_000:
                self._oi_warned.add(key)
                log.warning(
                    "5paisa FULL %s %d waiting >60 s for first OI; ticks remain withheld. "
                    "Verify option OI support in GetScripInfoForFuture during live smoke.",
                    key[0],
                    key[1],
                )

    async def _run(self):
        delay = 1
        while not self._stop.is_set():
            await self._auth_ok.wait()
            ws = None
            try:
                ws = await self._connect(wire.ws_url(self._token, self._client))
                self._ws = ws
                self._sent.clear()
                # Reconnection must obtain fresh state, not join a new price to yesterday's OI.
                self._state.clear()
                self.connects += 1
                async with self._lock:
                    await self._subscribe(ws, list(self._subs))
                while not self._stop.is_set():
                    raw = await asyncio.wait_for(ws.recv(), timeout=60)
                    for tick in self.decode(raw, self._clock_ms()):
                        self._emit(tick)
                        delay = 1
                    self._warn_oi_wait(self._clock_ms())
                    if self.auth_blocked:
                        break
            except TimeoutError:
                self._warn_oi_wait(self._clock_ms())
                self.idle_reconnects += 1
            except Exception as exc:  # noqa: BLE001 -- injected/vendor socket exceptions vary
                status = getattr(exc, "status_code", None)
                if status is None:
                    status = getattr(getattr(exc, "response", None), "status_code", None)
                code = wire.auth_code(None, status)
                if code is not None:
                    self._block(code, "5paisa WebSocket handshake rejected")
                # Never log the exception or URL: the URL contains the access token.
            finally:
                if self._ws is ws:
                    self._ws = None
                    self._sent.clear()
                if ws is not None:
                    with contextlib.suppress(Exception):
                        await ws.close()
            if not self.auth_blocked:
                await self._sleep(delay)
                delay = min(30, delay * 2)

    def decode(self, raw, recv_ms):
        try:
            body = json.loads(raw)
        except (ValueError, TypeError, UnicodeError):
            self.bad_frames += 1
            return []
        code = wire.auth_code(body)
        if code is not None:
            self._block(code, "5paisa session rejected")
            return []
        out = []
        for row in body if isinstance(body, list) else [body]:
            if not isinstance(row, dict):
                continue
            try:
                key = self._canon.get((row["Exch"], row["ExchType"], int(row["Token"])))
            except (KeyError, ValueError, TypeError):
                continue
            if key is None:
                continue
            state = self._state.setdefault(key, {})
            changed = False
            for field, target in (("LastRate", "ltp"), ("TotalQty", "volume"), ("OpenInterest", "oi")):
                value = wire.number(row.get(field))
                if target == "oi" and key[0] == "NSE_FNO" and value == 0:
                    self.oi_zero_ignored += 1
                if value is None or (target == "oi" and (key[0] != "NSE_FNO" or value == 0)):
                    continue
                value = value if target == "ltp" else int(value)
                changed |= state.get(target) != value
                state[target] = value
                if target == "oi":
                    self._oi_seen.add(key)
                    self._oi_wait_since.pop(key, None)
            if not changed or state.get("ltp") is None:
                continue
            if key[0] == "NSE_FNO" and self._subs[key] == "FULL" and state.get("oi") is None:
                continue
            # True receive epoch milliseconds; no invented correction for the undocumented Time field.
            out.append(
                Tick(
                    recv_ms,
                    recv_ms,
                    key[1],
                    key[0],
                    state["ltp"],
                    state.get("volume"),
                    state.get("oi"),
                    None,
                    None,
                    None,
                    None,
                )
            )
        return out

    def reload_credentials(self, creds):
        values = {auth.KEY_TOKEN: self._token, auth.KEY_CLIENT: self._client, auth.KEY_APP: self._app}
        values.update({k: v for k, v in creds.items() if v})
        self._token, self._client, self._app = auth.credentials(values)
        self._unblock()
        self._state.clear()
        if self._task is not None:
            old = self._task
            self._retired.add(old)
            old.add_done_callback(self._retired.discard)
            old.cancel()
            self._task = None
        self._spawn()

    async def stop(self):
        self._started = False
        self._stop.set()
        self._auth_ok.set()
        tasks = set(self._retired)
        if self._task is not None:
            tasks.add(self._task)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._task = None
        self._sent.clear()
