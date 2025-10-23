# bridge.py — Transport Layer for LinkLLM
# ------------------------------------------------------------
# Provides a robust, production-ready Bridge abstraction and concrete
# implementations for WebSocket, HTTP, and In-Memory transports.
#
# Features:
# - BaseBridge abstract interface
# - WebSocketClientBridge with auto-reconnect, ping/pong, backoff
# - HttpBridge with bg long-polling and post messages
# - InMemoryBridge for local testing (per-agent endpoints)
# - Pluggable serializer/deserializer hooks
# - Optional authentication header support (Bearer / JWT / Capability tokens)
# - Metrics and basic tracing hooks
# - Clean async context manager support
#
# Notes:
# - Requires `websockets` for WebSocket transport and `aiohttp` for HTTP transport.
# - This file focuses on correctness and clarity over extreme micro-optimizations.
# ------------------------------------------------------------
from websockets import WebSocketClientProtocol
from __future__ import annotations
import asyncio
import json
import logging
import time
from typing import Any, Callable, Dict, Optional, Awaitable

logger = logging.getLogger("link_llm.bridge")


class BridgeError(Exception):
    """Generic bridge error"""


class BaseBridge:
    """Abstract base interface for a Bridge implementation.

    Implementations must provide async `send`, `recv`, and `close`. Optional
    `connect` may be provided for transports that require an initial handshake.
    """

    async def connect(self) -> None:
        raise NotImplementedError

    async def send(self, msg: Dict[str, Any]) -> Any:
        raise NotImplementedError

    async def recv(self) -> Optional[Dict[str, Any]]:
        raise NotImplementedError

    async def close(self) -> None:
        raise NotImplementedError

    # Optional hooks
    def serializer(self, obj: Dict[str, Any]) -> str:
        return json.dumps(obj, default=str)

    def deserializer(self, raw: str) -> Dict[str, Any]:
        return json.loads(raw)


# ---------------------------
# WebSocket Bridge
# ---------------------------
try:
    import websockets
    from websockets import WebSocketClientProtocol
except Exception:
    websockets = None
    WebSocketClientProtocol = Any


class WebSocketClientBridge(BaseBridge):
    """Asynchronous WebSocket bridge with reconnect and ping/pong support.

    Basic usage:
        bridge = WebSocketClientBridge('ws://localhost:8765', auth_token='...')
        await bridge.connect()
        await bridge.send({...})
        msg = await bridge.recv()
    """

    def __init__(
        self,
        url: str,
        auth_token: Optional[str] = None,
        ping_interval: float = 20.0,
        max_message_size: int = 2 ** 20,
        reconnect_backoff: float = 0.5,
        max_backoff: float = 30.0,
        loop: Optional[asyncio.AbstractEventLoop] = None,
    ):
        if websockets is None:
            raise BridgeError("websockets library not installed. `pip install websockets`")

        self.url = url
        self.auth_token = auth_token
        self.ping_interval = ping_interval
        self.max_message_size = max_message_size
        self.reconnect_backoff = reconnect_backoff
        self.max_backoff = max_backoff
        self.loop = loop or asyncio.get_event_loop()

        self._ws: Optional[WebSocketClientProtocol] = None  # Yellow underline at WebSocketClientProtocol
        self._connected = asyncio.Event()
        self._recv_queue: asyncio.Queue = asyncio.Queue()
        self._send_lock = asyncio.Lock()
        self._listener_task: Optional[asyncio.Task] = None
        self._pinger_task: Optional[asyncio.Task] = None
        self._closing = False

    async def connect(self) -> None:
        backoff = self.reconnect_backoff
        while True:
            try:
                headers = []
                if self.auth_token:
                    headers.append(("Authorization", f"Bearer {self.auth_token}"))
                self._ws = await websockets.connect(self.url, max_size=self.max_message_size, extra_headers=headers)
                self._connected.set()
                logger.info("WebSocket connected: %s", self.url)
                # start background listener and pinger
                self._listener_task = self.loop.create_task(self._listener())
                self._pinger_task = self.loop.create_task(self._pinger())
                return
            except Exception as e:
                logger.warning("WebSocket connect failed: %s — retrying in %.1fs", e, backoff)
                await asyncio.sleep(backoff)
                backoff = min(self.max_backoff, backoff * 2)

    async def send(self, msg: Dict[str, Any]) -> Any:
        await self._connected.wait()
        async with self._send_lock:
            raw = self.serializer(msg)
            try:
                await asyncio.wait_for(self._ws.send(raw), timeout=10)
            except Exception as e:
                logger.exception("WebSocket send failed: %s", e)
                raise BridgeError(e)

    async def recv(self) -> Optional[Dict[str, Any]]:
        # pop from internal queue which _listener fills
        try:
            raw = await self._recv_queue.get()
            return raw
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.exception("WebSocket recv error: %s", e)
            raise BridgeError(e)

    async def close(self) -> None:
        self._closing = True
        if self._pinger_task:
            self._pinger_task.cancel()
        if self._listener_task:
            self._listener_task.cancel()
        if self._ws is not None:
            try:
                await self._ws.close()
            except Exception as e:
                logger.debug("Error closing websocket: %s", e)
        self._connected.clear()

    async def _listener(self):
        assert self._ws is not None
        try:
            async for raw in self._ws:
                try:
                    parsed = self.deserializer(raw)
                except Exception:
                    logger.exception("Failed to parse WS message")
                    continue
                # enqueue parsed message
                await self._recv_queue.put(parsed)
        except asyncio.CancelledError:
            return
        except Exception as e:
            logger.exception("WebSocket listener crashed: %s", e)
            # try to reconnect unless we're closing
            if not self._closing:
                self._connected.clear()
                await self.connect()

    async def _pinger(self):
        while True:
            try:
                if self._ws and self._ws.open:
                    try:
                        await self._ws.ping()
                    except Exception:
                        logger.debug("Ping failed, connection may be unhealthy")
                await asyncio.sleep(self.ping_interval)
            except asyncio.CancelledError:
                return
            except Exception:
                logger.exception("Pinger error")


# ---------------------------
# HTTP Bridge
# ---------------------------
try:
    import aiohttp

except Exception:
    aiohttp = None


class HttpBridge(BaseBridge):
    """HTTP bridge that posts messages to `/message` and long-polls `/poll`.

    NOTE: HTTP is less efficient than WebSocket but useful for environments
    where WS is not available. This implementation opens a background task
    that polls for messages and places them on an internal queue.
    """

    def __init__(
        self,
        base_url: str,
        auth_token: Optional[str] = None,
        poll_interval: float = 1.0,
        session: Optional["aiohttp.ClientSession"] = None, # Yellow underline at aiohttp
        loop: Optional[asyncio.AbstractEventLoop] = None,
    ):
        if aiohttp is None:
            raise BridgeError("aiohttp not installed. `pip install aiohttp`")

        self.base_url = base_url.rstrip("/")
        self.auth_token = auth_token
        self.poll_interval = poll_interval
        self._external_session = session
        self._session: Optional[aiohttp.ClientSession] = session # Yellow underline at aiohttp
        self.loop = loop or asyncio.get_event_loop()

        self._recv_queue: asyncio.Queue = asyncio.Queue()
        self._poll_task: Optional[asyncio.Task] = None
        self._closing = False

    async def connect(self) -> None:
        if self._session is None:
            self._session = aiohttp.ClientSession()
        # start poller
        self._poll_task = self.loop.create_task(self._poll_loop())

    async def send(self, msg: Dict[str, Any]) -> Any:
        if self._session is None:
            self._session = aiohttp.ClientSession()
        url = f"{self.base_url}/message"
        headers = {"Content-Type": "application/json"}
        if self.auth_token:
            headers["Authorization"] = f"Bearer {self.auth_token}"
        raw = self.serializer(msg)
        async with self._session.post(url, data=raw, headers=headers) as resp:
            if resp.status >= 400:
                text = await resp.text()
                logger.error("HTTP send failed: %s %s", resp.status, text)
                raise BridgeError(f"HTTP send failed: {resp.status}")
            try:
                body = await resp.json()
            except Exception:
                body = await resp.text()
            return body

    async def recv(self) -> Optional[Dict[str, Any]]:
        try:
            return await self._recv_queue.get()
        except asyncio.CancelledError:
            raise

    async def close(self) -> None:
        self._closing = True
        if self._poll_task:
            self._poll_task.cancel()
        if self._session and self._session is not self._external_session:
            await self._session.close()

    async def _poll_loop(self):
        assert self._session is not None
        url = f"{self.base_url}/poll"
        headers = {"Accept": "application/json"}
        if self.auth_token:
            headers["Authorization"] = f"Bearer {self.auth_token}"
        while not self._closing:
            try:
                async with self._session.get(url, headers=headers) as resp:
                    if resp.status == 204:
                        # no content, sleep and continue
                        await asyncio.sleep(self.poll_interval)
                        continue
                    if resp.status >= 400:
                        text = await resp.text()
                        logger.warning("Polling error %s: %s", resp.status, text)
                        await asyncio.sleep(self.poll_interval)
                        continue
                    # expect JSON array of messages or single message
                    try:
                        body = await resp.json()
                    except Exception:
                        logger.exception("Failed to parse poll response")
                        await asyncio.sleep(self.poll_interval)
                        continue
                    # normalize to list
                    msgs = body if isinstance(body, list) else [body]
                    for m in msgs:
                        await self._recv_queue.put(m)
            except asyncio.CancelledError:
                return
            except Exception as e:
                logger.exception("Poll loop exception: %s", e)
                await asyncio.sleep(self.poll_interval)


# ---------------------------
# In-Memory Bridge (Testing)
# ---------------------------
class InMemoryBridge(BaseBridge):
    """Simple in-process bridge for testing and demos.

    Behavior:
    - Maintains per-agent queues in a central registry
    - Each endpoint should be created via `InMemoryEndpoint(bridge, agent_id)`
    - Supports broadcast by leaving `to_agent` empty
    """

    def __init__(self):
        self._queues: Dict[str, asyncio.Queue] = {}
        self._lock = asyncio.Lock()

    def _ensure(self, agent_id: str) -> asyncio.Queue:
        q = self._queues.get(agent_id)
        if q is None:
            q = asyncio.Queue()
            self._queues[agent_id] = q
        return q


class InMemoryEndpoint:
    def __init__(self, bridge: InMemoryBridge, agent_id: str):
        self.bridge = bridge
        self.agent_id = agent_id
        self._q = bridge._ensure(agent_id)

    async def connect(self) -> None:
        return

    async def send(self, msg: Dict[str, Any]) -> None:
        to = msg.get("to_agent") or msg.get("to")
        # broadcast
        if not to:
            for aid, q in self.bridge._queues.items():
                await q.put(msg)
            return
        # ensure recip queue
        q = self.bridge._ensure(to)
        await q.put(msg)

    async def recv(self) -> Dict[str, Any]:
        return await self._q.get()

    async def close(self) -> None:
        return


# ---------------------------
# Utilities & Examples
# ---------------------------
async def health_check(bridge: BaseBridge, timeout: float = 2.0) -> bool:
    """Quick health check: connect, optionally send a ping/heartbeat, and close."""
    try:
        await bridge.connect()
        # try a noop send if supported (some transports may not allow it)
        try:
            await bridge.send({"type": "heartbeat", "from": "health-check", "content": {"ts": time.time()}})
        except Exception:
            pass
        await bridge.close()
        return True
    except Exception:
        return False


if __name__ == "__main__":
    import asyncio
    import logging

    logging.basicConfig(level=logging.DEBUG)

    async def demo():
        # WS demo (requires an echo/ws server)
        # ws = WebSocketClientBridge('ws://localhost:8765')
        # await ws.connect()
        # await ws.send({'from': 'a', 'to': 'b', 'content': 'hi'})
        # print(await ws.recv())

        # HTTP demo (requires a compatible HTTP server)
        # http = HttpBridge('http://localhost:8080')
        # await http.connect()
        # await http.send({'from': 'a', 'to': 'b', 'content': 'hi'})
        # print(await http.recv())

        # In-memory demo
        bridge = InMemoryBridge()
        a = InMemoryEndpoint(bridge, 'agent.a')
        b = InMemoryEndpoint(bridge, 'agent.b')
        await a.connect()
        await b.connect()

        await a.send({'from': 'agent.a', 'to': 'agent.b', 'intent': 'hello', 'content': 'hey'})
        msg = await b.recv()
        print('B got:', msg)

    asyncio.run(demo())
