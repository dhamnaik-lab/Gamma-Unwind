"""Paytm WS feed. HTTP 401 -> 809; 403 -> 806; park until paste reload.

No token-bearing URL or vendor exception body is logged. PM55-1/2/3.
"""
from __future__ import annotations

import asyncio
import contextlib
import time
from urllib.parse import urlencode

from gammad.broker.paytm import auth, wire
from gammad.watch.feed_protocol import FeedBase, FeedCaps, Tick, normalize_instruments


async def connector(url):
    from websockets.asyncio.client import connect

    return await connect(url, open_timeout=15, max_size=2**20)


class PaytmFeed(FeedBase):
    caps = FeedCaps("ws", None, wire.MAX_FEED)

    def __init__(self, creds, out, on_auth_error=None, *, connect=None, clock_ms=None, resolve=None):
        super().__init__(out, on_auth_error=on_auth_error)
        self._token = auth.paste(creds, auth.KEY_PUBLIC)
        self._connect = connect or connector
        self._clock = clock_ms or (lambda: int(time.time() * 1000))
        self._resolve = resolve or (lambda seg, sid: sid if seg in wire.SCRIP else None)
        self._subs = {}
        self._native = {}
        self._state = {}
        self._ws = None
        self._task = None
        self._synced = False
        self.bad_frames = 0

    def _carried(self):
        return self._subs

    def pending_ids(self):
        return frozenset() if self._synced else self.subscribed_ids()

    async def start(self, instruments=()):
        await self.add_instruments(instruments)
        if self._task is None and not self._stop.is_set():
            self._task = asyncio.create_task(self._run())

    async def add_instruments(self, instruments):
        rank = {"TICKER": 0, "QUOTE": 1, "FULL": 2}
        changes = {}
        for seg, sid, mode in normalize_instruments(instruments):
            key = (seg, sid)
            have = changes.get(key, self._subs.get(key))
            if have is None or rank[mode] > rank[have]:
                changes[key] = mode
        fresh = [k for k in changes if k not in self._subs]
        mapped = await self._resolve_off_loop(fresh, self._resolve, "paytm")
        changes = {k: m for k, m in changes.items() if k in self._subs or k in mapped}
        if len(self._subs) + len(mapped) > self.caps.max_instruments:
            raise self._capacity_error(len(self._subs), [(s, i, changes[s, i]) for s, i in mapped], "paytm")
        self._subs.update(changes)
        self._native.update(mapped)
        self._accept(changes)
        if changes and self._ws is not None:
            try:
                await self._ws.send(self._subscription(changes))
            except Exception:  # noqa: BLE001 -- socket failure must resubscribe on reconnect
                self._synced = False
                await self._ws.close()
        return len(changes)

    def _subscription(self, keys):
        return wire.subscribe([(s, self._native[s, i], self._subs[s, i]) for s, i in keys])

    def reload_credentials(self, creds):
        self._token = auth.paste(creds, auth.KEY_PUBLIC)
        self._state.clear()
        self._unblock()
        if self._ws is not None:
            asyncio.create_task(self._ws.close())

    async def stop(self):
        self._stop.set()
        self._auth_ok.set()
        if self._task is not None:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
        if self._ws is not None:
            with contextlib.suppress(Exception):
                await self._ws.close()

    async def _run(self):
        delay = 1
        while not self._stop.is_set():
            await self._auth_ok.wait()
            if self._stop.is_set():
                break
            ws = None
            try:
                url = wire.WS_URL + "?" + urlencode({"x_jwt_token": self._token})
                ws = await self._connect(url)
                self._ws = ws
                await ws.send(self._subscription(self._subs))
                self._synced = True
                while not self._stop.is_set():
                    msg = await asyncio.wait_for(ws.recv(), timeout=90)
                    if isinstance(msg, str):
                        code = wire.text_auth(msg)
                        if code:
                            self._block(code, "Paytm rejected feed access")
                            break
                    else:
                        for tick in self.decode(bytes(msg), self._clock()):
                            self._emit(tick)
                        delay = 1
            except Exception as exc:  # noqa: BLE001 -- vendor/network failure must not kill the feed
                status = getattr(getattr(exc, "response", None), "status_code", None)
                status = status or getattr(exc, "status_code", 0)
                code = wire.auth_code(status, {})
                if code:
                    self._block(code, "Paytm rejected feed handshake")
            finally:
                self._ws = None
                self._synced = False
                if ws is not None:
                    with contextlib.suppress(Exception):
                        await ws.close()
            if not self.auth_blocked:
                await self._sleep(delay)
                delay = min(delay * 2, 30)

    def decode(self, data, recv_ms):
        rows = wire.decode(data)
        if not rows:
            self.bad_frames += 1
        out = []
        for row in rows:
            # Index and exchange instruments can share a numeric token.
            keys = [k for k, native in self._native.items()
                    if native == row["security_id"] and (k[0] == "IDX_I") == row["index"]]
            if len(keys) != 1:
                continue
            key = keys[0]
            state = self._state.setdefault(key, {})
            state.update(row)
            out.append(Tick(row.get("ts_ms") or recv_ms, recv_ms, key[1], key[0], state["last_price"],
                            state.get("volume_traded"), state.get("oi") if key[0] == "NSE_FNO" else None,
                            None, state.get("close"), None, None))
        return out
