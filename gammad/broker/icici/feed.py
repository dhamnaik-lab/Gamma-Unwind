"""Minimal Engine.IO v4 / Socket.IO stock feed; wire evidence F1-F4.

Numeric wire timestamps are Unix seconds; dictionary LTT examples are IST text.
CONNECT_ERROR maps to 809, HTTP 401/403 to 809; explicit session-expired text to 807.
Ping timing comes from the server open packet, never a guessed broker cadence.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import time

from gammad.broker.icici import wire
from gammad.watch.feed_protocol import FeedBase, FeedCaps, Tick, normalize_instruments


def authorizing_connector():
    async def connect(url):
        import websockets

        return await websockets.connect(url, ping_interval=None, max_size=2**22)

    return connect


class IciciFeed(FeedBase):
    caps = FeedCaps("ws", None, wire.MAX_FEED)

    def __init__(self, creds, out, *, on_auth_error=None, connect=None, clock_ms=None, resolve=None):
        super().__init__(out, on_auth_error=on_auth_error)
        self._creds = dict(creds)
        self._connect = connect or authorizing_connector()
        self._clock_ms = clock_ms or (lambda: int(time.time() * 1000))
        self._resolve = resolve
        self._subs = {}
        self._native = {}
        self._canon = {}
        self._state = {}
        self._task = None
        self._ws = None
        self._synced = False
        self.bad_frames = 0

    def subscribed_ids(self):
        return frozenset(self._subs)

    def pending_ids(self):
        return frozenset() if self._synced else self.subscribed_ids()

    def _resolve_id(self, seg, sid):
        if self._resolve is not None:
            return self._resolve(seg, sid)  # Never fall back after a real map rejection.
        return f"4.1!{sid}" if seg in ("NSE_EQ", "NSE_FNO") else None  # common-E test-only path

    async def add_instruments(self, instruments):
        rank = {"TICKER": 0, "QUOTE": 1, "FULL": 2}
        wanted = {}
        for seg, sid, mode in normalize_instruments(instruments):
            key = (seg, sid)
            if rank[mode] > rank.get(wanted.get(key, self._subs.get(key)), -1):
                wanted[key] = mode
        fresh = [key for key in wanted if key not in self._subs]
        mapped = await self._resolve_off_loop(fresh, self._resolve_id, "Breeze feed")
        if len(self._subs) + len(mapped) > self.caps.max_instruments:
            raise self._capacity_error(
                len(self._subs), [(s, i, wanted[(s, i)]) for s, i in mapped], "Breeze feed"
            )
        changed = 0
        for key, mode in wanted.items():
            if key in mapped:
                native = mapped[key]
                if native in self._canon and self._canon[native] != key:
                    self._reject(key, "native collision")
                    continue
                self._native[key] = native
                self._canon[native] = key
            elif key not in self._subs:
                continue
            self._subs[key] = mode
            self._accept([key])
            changed += 1
        if changed and self._ws is not None and self._synced:
            try:
                await self._subscribe()
            except Exception:  # noqa: BLE001 -- reconnect resends all accepted ids
                self._synced = False
                await self._ws.close()
        return changed

    async def _subscribe(self):
        await self._ws.send(wire.event("join", list(self._native.values())))
        self._synced = True

    async def start(self, instruments=()):
        await self.add_instruments(instruments)
        if self._task is None:
            self._task = asyncio.create_task(self._run())

    async def stop(self):
        self._stop.set()
        self._auth_ok.set()
        if self._task:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
            self._task = None

    def reload_credentials(self, creds):
        self._creds.update({k: v for k, v in creds.items() if v})
        self._state.clear()
        self._unblock()
        if self._ws is not None:
            asyncio.create_task(self._ws.close())

    def decode(self, data, recv_ms):
        fields = wire.tick_fields(data)
        key = self._canon.get(fields.get("symbol"))
        if key is None:
            return []
        st = self._state.setdefault(key, {})
        for name in ("last", "ttq", "OI", "close"):
            value = wire.number(fields.get(name))
            if value is not None and not (name == "OI" and value <= 0):
                st[name] = value
        ts = wire.epoch(fields["ltt"]) if fields.get("ltt") else recv_ms
        vol = st.get("ttq")
        oi = st.get("OI") if key[0] == "NSE_FNO" else None
        return [
            Tick(
                ts,
                recv_ms,
                key[1],
                key[0],
                st.get("last"),
                int(vol) if vol is not None else None,
                int(oi) if oi is not None else None,
                None,
                st.get("close"),
                None,
                None,
            )
        ]

    async def _pump(self):
        timeout = 30.0
        opened = False
        while not self._stop.is_set():
            raw = await asyncio.wait_for(self._ws.recv(), timeout)
            try:
                kind, data = wire.packet(raw)
                if kind == "open":
                    timeout = (float(data["pingInterval"]) + float(data["pingTimeout"])) / 1000
                    if not 0 < timeout <= 300:
                        raise ValueError("invalid heartbeat window")
                    user, token = wire.session_parts(self._creds["ICICI_ACCESS_TOKEN"])
                    await self._ws.send(
                        "40" + json.dumps({"user": user, "token": token}, separators=(",", ":"))
                    )
                    opened = True
                elif kind == "ping":
                    await self._ws.send("3" + data)
                elif kind == "connected" and opened:
                    await self._subscribe()
                elif kind == "error":
                    text = str(data).lower()
                    self._block(807 if "expired" in text else 809, "Breeze socket authentication rejected")
                    return
                elif kind == "close":
                    return
                elif kind == "event" and self._synced and data[0] == "stock":
                    for tick in self.decode(data[1], self._clock_ms()):
                        self._emit(tick)
            except (ValueError, KeyError, TypeError, IndexError):
                self.bad_frames += 1

    async def _run(self):
        backoff = 1
        while not self._stop.is_set():
            await self._auth_ok.wait()
            if self._stop.is_set():
                return
            try:
                self._ws = await self._connect(wire.WS)
                await self._pump()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 -- transport failures reconnect; auth parks
                status = getattr(exc, "status_code", None) or getattr(
                    getattr(exc, "response", None), "status_code", None
                )
                if status in (401, 403):
                    self._block(809, "Breeze socket handshake rejected")
            finally:
                self._synced = False
                if self._ws:
                    with contextlib.suppress(Exception):
                        await self._ws.close()
                self._ws = None
            if not self.auth_blocked:
                await self._sleep(backoff)
                backoff = min(30, backoff * 2)
