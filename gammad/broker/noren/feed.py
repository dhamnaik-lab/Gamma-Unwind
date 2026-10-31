"""Noren touchline state machine (N3/N5), with host-specific authentication.

Only subscribed canonical identities emit. Sparse updates carry prior fields;
reconnects and credential reloads discard old state. Feed time is receive time
until ft/ltt encoding is sourced. No inferred epoch offset is used (U3).
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import time
from datetime import datetime

from gammad.broker.base import BrokerError
from gammad.broker.noren import auth, units, wire
from gammad.config import IST
from gammad.log import get_logger
from gammad.watch.feed_protocol import FeedBase, FeedCaps, Tick, normalize_instruments

log = get_logger(__name__)

RANK = {"TICKER": 0, "QUOTE": 1, "FULL": 2}


class NorenFeed(FeedBase):
    def __init__(self, host, creds, out, on_auth_error=None, *, adapter=None, connect=None, clock_ms=None):
        super().__init__(out, on_auth_error=on_auth_error)
        self.host = host
        self.caps = FeedCaps("ws", None, host.max_feed_instruments)
        self._uid, self._token = auth.credentials(host, creds)
        self._resolve = adapter.native_id if adapter is not None else self._test_resolve
        # U4 guard (D726): the canonical lot each volume is checked against (adapter-backed; the offline harness
        # without an adapter falls back to the frame's documented `ls`). The switch-off is the ADAPTER's guard
        # (fix round F2: one per adapter for the IST day, so a feed rebuilt on restart cannot reset it).
        self._lot_source = getattr(adapter, "lot_size", None) if adapter is not None else None
        guard = getattr(adapter, "volume_guard", None)
        self.volume_guard = guard if guard is not None else units.VolumeUnitGuard(host.broker)
        self._lots = {}
        self._lots_day = None  # IST day the cached lots belong to (fix round F1: refreshed on a new day)
        self._lots_refresh = None
        self._connect = connect or self._default_connect
        self._clock_ms = clock_ms or (lambda: round(time.time() * 1000))
        self._subs = {}
        self._native = {}
        self._reverse = {}
        self._state = {}
        self._pending = set()
        self._ws = None
        self._ready = False
        self._task = None
        self._tasks = set()
        self._generation = 0
        self.bad_frames = 0

    @staticmethod
    def _test_resolve(seg, sid):
        # AMENDED common E: only when no adapter is supplied (offline harness).
        exchange = {"NSE_EQ": "NSE", "NSE_FNO": "NFO"}.get(seg)
        return f"{exchange}|{sid}" if exchange else None

    async def _default_connect(self, url):
        if not url:
            raise BrokerError("Noren WebSocket URL is UNSOURCED; production feed disabled")
        import websockets

        return await websockets.connect(url, max_size=2**20)

    def _resolve_with_lot(self, seg, sid):
        # Runs in _resolve_off_loop's worker thread (D8), so the canonical lot lookup never blocks the loop.
        native = self._resolve(seg, sid)
        if native is not None and self._lot_source is not None:
            self._lots[(seg, sid)] = self._lot_source(seg, sid)
        return native

    def _day(self):
        return datetime.fromtimestamp(self._clock_ms() / 1000, IST).date()

    async def _reload_lots(self, day):
        # Off the loop (D8): the adapter's lot_size reloads its masters on a new IST day.
        keys = list(self._subs)
        try:
            lots = await asyncio.to_thread(lambda: {k: self._lot_source(*k) for k in keys})
        except Exception:  # keep yesterday's lots out; volume stays None until a reload works
            log.exception("%s feed: lot reload for %s failed", self.host.broker, day)
            return
        finally:
            self._lots_refresh = None
        self._lots, self._lots_day = lots, day

    def _volume(self, key, state):
        """U4 / D726: cumulative day volume in QUANTITY, or None, through the adapter's volume_guard (units.py).
        Lots are TODAY's: on a new IST day the cached lots are reloaded off the loop, and volume is None (never a
        switch-off) until they are, so a lot-size change cannot trip the guard."""
        if not self.host.volume_verified or "v" not in state or self.volume_guard.off:
            return None
        if self._lot_source is None:
            lot = state.get("ls")
        else:
            day = self._day()
            if self._lots_day != day:
                if self._lots_refresh is None:
                    self._lots_refresh = asyncio.get_running_loop().create_task(self._reload_lots(day))
                    self._tasks.add(self._lots_refresh)
                    self._lots_refresh.add_done_callback(self._tasks.discard)
                return None
            lot = self._lots.get(key)
        return self.volume_guard.admit(state["v"], lot, f"{key[0]}/{key[1]}")

    def subscribed_ids(self):
        return frozenset(self._subs)

    def pending_ids(self):
        return frozenset(self._pending)

    async def add_instruments(self, instruments):
        proposed = dict(self._subs)
        for seg, sid, mode in normalize_instruments(instruments):
            key = (seg, sid)
            if key not in proposed or RANK[mode] > RANK[proposed[key]]:
                proposed[key] = mode
        fresh = [k for k in proposed if k not in self._subs]
        mapped = await self._resolve_off_loop(fresh, self._resolve_with_lot, "noren feed")
        if self._lots_day is None and self._lot_source is not None:
            self._lots_day = self._day()  # later days reload every subscribed lot (_volume)
        for key in fresh:
            if key not in mapped:
                proposed.pop(key)
        if len(proposed) > self.caps.max_instruments:
            raise self._capacity_error(len(self._subs), [(s, i, proposed[(s, i)]) for s, i in mapped], "noren")
        changed = {k for k, mode in proposed.items() if self._subs.get(k) != mode}
        self._subs = proposed
        self._native.update(mapped)
        self._reverse.update({native: key for key, native in mapped.items()})
        self._accept(mapped)
        self._pending.update(changed)
        if self._ready and changed:
            try:
                await self._subscribe(changed)
            except Exception:  # noqa: BLE001 -- leave pending; reconnect resends full set
                if self._ws is not None:
                    await self._ws.close()
        return len(changed)

    async def _subscribe(self, keys):
        keys = set(keys)
        if keys:
            # Touchline carries oi/poi. FULL/QUOTE both need touchline; no depth required.
            await self._ws.send(json.dumps({"t": "t", "k": "#".join(self._native[k] for k in sorted(keys))}))
            self._pending.difference_update(keys)

    async def start(self, instruments=()):
        if self._connect == self._default_connect and not self.host.ws_url:
            raise BrokerError("Noren WebSocket URL is UNSOURCED; production feed disabled")
        await self.add_instruments(instruments)
        if self._task is None or self._task.done():
            self._stop.clear()
            self._spawn()

    def _spawn(self):
        self._task = asyncio.create_task(self._run())
        self._tasks.add(self._task)
        self._task.add_done_callback(self._tasks.discard)

    async def stop(self):
        self._stop.set()
        tasks = list(self._tasks)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self._task = None

    def reload_credentials(self, creds):
        self._uid, self._token = auth.credentials(self.host, creds)
        self._generation += 1
        self._state.clear()
        self._ready = False
        self._pending.update(self._subs)
        self._unblock()
        if self._task is not None:
            self._task.cancel()
            self._spawn()

    async def _heartbeat(self, ws, *, sleep=asyncio.sleep):
        while not self._stop.is_set():
            await sleep(self.host.heartbeat_s)
            await ws.send('{"t":"h"}')

    def _decode(self, payload, *, data_allowed=True):
        try:
            frame = json.loads(payload)
            if not isinstance(frame, dict):
                raise TypeError("frame")
        except (ValueError, TypeError, UnicodeError):
            self.bad_frames += 1
            return None
        code = wire.auth_code(frame)
        if code is not None:
            self._block(int(code), "Noren session rejected")
            return None
        if frame.get("t") in ("ck", "ak"):
            return frame
        if frame.get("t") not in ("tk", "tf", "dk", "df"):
            return None
        if not data_allowed:
            return None
        key = self._reverse.get(f"{frame.get('e')}|{frame.get('tk')}")
        if key is None:
            return None
        previous = self._state.get(key, {})
        state = dict(previous)
        fields = ("lp", "oi", "poi", "v") if self.host.volume_verified else ("lp", "oi", "poi")
        for name in fields:
            if name in frame:
                value = wire.number(frame[name]) if name == "lp" else wire.integer(frame[name])
                if value is None:
                    self.bad_frames += 1
                    return None
                if name == "oi" and value == 0:
                    continue  # zero is no update by FeedBase contract
                state[name] = value
        lot = wire.integer(frame.get("ls")) if self.host.volume_verified else None
        if lot:  # documented touchline "ls" (lot size); only the no-adapter path reads it (_volume)
            state["ls"] = lot
        self._state[key] = state
        if "lp" not in state:
            return None
        received = self._clock_ms()
        ts_ms = received  # U3: unknown ft/ltt encoding; never guess a timezone offset
        option = key[0] == "NSE_FNO"
        self._emit(Tick(
            ts_ms, received, key[1], key[0], state["lp"],
            self._volume(key, state),
            state.get("oi") if option and self.host.feed_oi else None,
            state.get("poi") if option and self.host.feed_oi else None,
            None, None, None,
        ))
        return None

    async def _run(self):
        backoff = 1.0
        while not self._stop.is_set():
            await self._auth_ok.wait()
            generation = self._generation
            ws = None
            heartbeat = None
            try:
                ws = await self._connect(self.host.ws_url or "")
                self._ws = ws
                self._ready = False
                self._state.clear()
                self._pending.update(self._subs)
                await ws.send(json.dumps({"t": self.host.connect_type, "uid": self._uid, "actid": self._uid,
                                          self.host.token_field: self._token, "source": "API"}))
                heartbeat = asyncio.create_task(self._heartbeat(ws))
                while not self._stop.is_set() and not self.auth_blocked and generation == self._generation:
                    # Bound silence and detect heartbeat failure; reconnect without stale state.
                    payload = await asyncio.wait_for(ws.recv(), timeout=60)
                    if heartbeat.done():
                        heartbeat.result()
                    frame = self._decode(payload, data_allowed=self._ready)
                    if frame is not None and str(frame.get("s", "")).casefold() == "ok":
                        self._ready = True
                        await self._subscribe(self._subs)
                        backoff = 1.0
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 -- connection failures retry with bounded backoff
                status = getattr(exc, "status_code", None) or getattr(getattr(exc, "response", None), "status_code", None)
                if status in (401, 403):
                    self._block(809, "Noren WebSocket handshake rejected")
            finally:
                if heartbeat is not None:
                    heartbeat.cancel()
                    await asyncio.gather(heartbeat, return_exceptions=True)
                if ws is not None:
                    with contextlib.suppress(Exception):
                        await ws.close()
                if self._ws is ws:
                    self._ws = None
                    self._ready = False
                    self._pending.update(self._subs)
            if not self.auth_blocked:
                await self._sleep(backoff)  # FeedBase: returns early on stop() (CX-51 F1 / F3, D726)
                backoff = min(backoff * 2, 30)
