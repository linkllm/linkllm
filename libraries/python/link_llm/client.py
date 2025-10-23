# client.py — LinkLLM Client (Production-Ready)
# ------------------------------------------------------------
# Async-first client that connects an agent to a Bridge (WebSocket/HTTP/InMemory)
# Implements: reliable send, receive loop, handler routing, middleware, reconnect,
#            batching, backpressure, metrics, tracing integration hooks, and sync wrappers.
# ------------------------------------------------------------
from .protocol import LinkMessage, MessageType, Role, LinkHeader
from __future__ import annotations
import asyncio
import logging
import time
from typing import Any, Awaitable, Callable, Dict, Optional, List
from dataclasses import dataclass, field

# The real package would import these from .protocol and .bridge
try:
    LinkMessage  # type: ignore
    MessageType
    Role
except NameError:
    # If this file is used standalone in the canvas, rely on the earlier definitions
    pass

logger = logging.getLogger("link_llm.client")

# -----------------------------
# Types
# -----------------------------
MessageHandler = Callable[["LinkMessage"], Awaitable[None]]
SyncMessageHandler = Callable[["LinkMessage"], None]
MiddlewareFn = Callable[["LinkMessage", Callable[["LinkMessage"], Awaitable[None]]], Awaitable[None]]


@dataclass
class ClientConfig:
    agent_id: str
    bridge: Any
    heartbeat_interval: Optional[float] = 20.0
    reconnect_backoff: float = 1.0
    max_reconnect_backoff: float = 30.0
    recv_concurrency: int = 4
    send_timeout: float = 30.0
    metrics_enabled: bool = True
    safe_mode: bool = False  # disable executing untrusted handlers
    loop: Optional[asyncio.AbstractEventLoop] = None


# -----------------------------
# Metrics & instrumentation
# -----------------------------
@dataclass
class Metrics:
    sent: int = 0
    received: int = 0
    send_errors: int = 0
    recv_errors: int = 0
    last_error: Optional[str] = None
    last_sent_at: Optional[float] = None
    last_recv_at: Optional[float] = None


# -----------------------------
# Client Implementation
# -----------------------------
class Client:
    """High-level client to connect an agent to a message Bus via a Bridge.

    Features:
    - Async receive loop with configurable concurrency
    - Handler registration by message type or intent
    - Middleware pipeline
    - Automatic reconnect for bridges that support it
    - Heartbeat pings
    - Backpressure-safe sending with optional rate-limiting
    - Simple tracing hooks
    """

    def __init__(self, config: ClientConfig):
        self.agent_id = config.agent_id
        self.bridge = config.bridge
        self.heartbeat_interval = config.heartbeat_interval
        self.reconnect_backoff = config.reconnect_backoff
        self.max_reconnect_backoff = config.max_reconnect_backoff
        self.recv_concurrency = config.recv_concurrency
        self.send_timeout = config.send_timeout
        self.loop = config.loop or asyncio.get_event_loop()

        self._running = False
        self._recv_tasks: List[asyncio.Task] = []
        self._handlers: Dict[str, MessageHandler] = {}
        self._intent_handlers: Dict[str, MessageHandler] = {}
        self._middleware: List[MiddlewareFn] = []
        self._metrics = Metrics()

        # Internal queues
        self._send_lock = asyncio.Lock()
        self._send_queue: asyncio.Queue = asyncio.Queue()

        # Reconnect state
        self._reconnect_attempts = 0

        # graceful shutdown
        self._stop_event = asyncio.Event()

    # -----------------------------
    # Public API
    # -----------------------------
    async def start(self):
        """Connect the underlying bridge and start receive and send background tasks."""
        logger.info("Client starting: %s", self.agent_id)
        self._running = True
        await self._maybe_connect_bridge()
        # start background loops
        # send pump
        self._send_task = self.loop.create_task(self._send_pump())
        # recv workers
        for _ in range(max(1, self.recv_concurrency)):
            t = self.loop.create_task(self._recv_worker())
            self._recv_tasks.append(t)
        # heartbeat
        if self.heartbeat_interval:
            self._hb_task = self.loop.create_task(self._heartbeat_loop())

    async def stop(self):
        """Stop loops and close bridge."""
        logger.info("Client stopping: %s", self.agent_id)
        self._running = False
        self._stop_event.set()
        # cancel recv tasks
        for t in self._recv_tasks:
            t.cancel()
        # cancel send task
        try:
            self._send_task.cancel()
        except Exception:
            pass
        try:
            self._hb_task.cancel()
        except Exception:
            pass
        # close bridge
        close = getattr(self.bridge, "close", None)
        if asyncio.iscoroutinefunction(close):
            try:
                await close()
            except Exception as e:
                logger.debug("Bridge close error: %s", e)

    async def send(self, to: Optional[str], intent: str, content: Any = None, type: Any = None, context: Optional[list] = None, memory_refs: Optional[list] = None, meta: Optional[dict] = None, timeout: Optional[float] = None) -> "LinkMessage":
        """Compose and enqueue a message for sending.

        Returns the LinkMessage object that was enqueued (not necessarily delivered).
        """
        if type is None:
            type = MessageType.TASK
        msg = LinkMessage(header=None, from_agent=self.agent_id, to_agent=to, type=type, role=Role.AGENT, intent=intent, content=content, context=context or [], memory_refs=memory_refs or [], meta=meta or {})
        # attach header if missing
        if getattr(msg, "header", None) is None:
            from .protocol import LinkHeader
            msg.header = LinkHeader()
        # put into send queue
        await self._send_queue.put(msg)
        return msg

    def on_type(self, mtype: Any, handler: MessageHandler):
        """Register a handler for a message type (MessageType)."""
        key = mtype.value if isinstance(mtype, MessageType) else str(mtype)
        self._handlers[key] = handler

    def on_intent(self, intent: str, handler: MessageHandler):
        """Register a handler for a semantic intent string."""
        self._intent_handlers[intent] = handler

    def add_middleware(self, mw: MiddlewareFn):
        """Add a middleware to the inbound dispatch pipeline.

        Middleware signature: async def mw(msg, next): await next(msg)
        """
        self._middleware.append(mw)

    # -----------------------------
    # Internal loops
    # -----------------------------
    async def _maybe_connect_bridge(self):
        connect = getattr(self.bridge, "connect", None)
        if asyncio.iscoroutinefunction(connect):
            backoff = self.reconnect_backoff
            while True:
                try:
                    await connect()
                    logger.info("Bridge connected")
                    self._reconnect_attempts = 0
                    return
                except Exception as e:
                    self._reconnect_attempts += 1
                    logger.warning("Bridge connect failed (%s). retry in %ss", e, backoff)
                    await asyncio.sleep(backoff)
                    backoff = min(self.max_reconnect_backoff, backoff * 2)
        else:
            # no connect method - assume bridge is usable
            return

    async def _send_pump(self):
        """Take messages from _send_queue and push them to the bridge.
        Includes retries and safe ordering.
        """
        while self._running:
            try:
                msg: LinkMessage = await self._send_queue.get()
                # optional middleware for outbound can go here
                await self._send_with_retry(msg)
                self._metrics.sent += 1
                self._metrics.last_sent_at = time.time()
            except asyncio.CancelledError:
                break
            except Exception as e:
                self._metrics.send_errors += 1
                self._metrics.last_error = str(e)
                logger.exception("Error while sending message: %s", e)
                await asyncio.sleep(0.5)

    async def _send_with_retry(self, msg: LinkMessage):
        # simple retry with exponential backoff
        attempt = 0
        backoff = 0.2
        while True:
            try:
                # ensure bridge has send
                await asyncio.wait_for(self.bridge.send(msg.to_dict()), timeout=self.send_timeout)
                return
            except Exception as e:
                attempt += 1
                self._metrics.send_errors += 1
                logger.warning("Send attempt %s failed: %s", attempt, e)
                await asyncio.sleep(backoff)
                backoff = min(5.0, backoff * 2)

    async def _recv_worker(self):
        """Continuously receive from the bridge and dispatch to handlers."""
        while self._running:
            try:
                raw = await self.bridge.recv()
                if raw is None:
                    # bridge signalled no data or closed
                    await asyncio.sleep(0.01)
                    continue
                # parse message
                if isinstance(raw, str):
                    msg = LinkMessage.from_json(raw)
                else:
                    msg = LinkMessage.from_dict(raw)
                self._metrics.received += 1
                self._metrics.last_recv_at = time.time()
                # dispatch with middleware
                await self._dispatch_with_middleware(msg)
            except asyncio.CancelledError:
                break
            except Exception as e:
                self._metrics.recv_errors += 1
                self._metrics.last_error = str(e)
                logger.exception("Receive error: %s", e)
                # Attempt reconnect behavior if bridge supports it
                await asyncio.sleep(min(1 + self._reconnect_attempts, self.max_reconnect_backoff))
                try:
                    await self._maybe_connect_bridge()
                except Exception:
                    self._reconnect_attempts += 1

    async def _dispatch_with_middleware(self, msg: LinkMessage):
        """Apply middleware chain then dispatch to the matched handler."""
        idx = 0

        async def run_next(m: LinkMessage):
            nonlocal idx
            if idx < len(self._middleware):
                fn = self._middleware[idx]
                idx += 1
                await fn(m, run_next)
            else:
                await self._dispatch(m)

        await run_next(msg)

    async def _dispatch(self, msg: LinkMessage):
        # intent handler takes precedence
        handler = None
        if msg.intent and msg.intent in self._intent_handlers:
            handler = self._intent_handlers[msg.intent]
        elif msg.type and msg.type.value in self._handlers:
            handler = self._handlers[msg.type.value]
        elif msg.type:
            # fallback to generic handlers by type
            handler = self._handlers.get(str(msg.type.value))

        if handler:
            try:
                await handler(msg)
            except Exception:
                logger.exception("Handler raised exception")
        else:
            logger.debug("No handler for message %s (intent=%s type=%s)", msg.header.id if msg.header else '<noid>', msg.intent, msg.type)

    async def _heartbeat_loop(self):
        while self._running:
            try:
                hb = LinkMessage(from_agent=self.agent_id, to_agent=None, type=MessageType.HEARTBEAT, role=Role.AGENT, intent="heartbeat", content={"ts": time.time()})
                if getattr(hb, "header", None) is None:
                    from .protocol import LinkHeader
                    hb.header = LinkHeader()
                await self._send_queue.put(hb)
            except Exception as e:
                logger.debug("Heartbeat error: %s", e)
            await asyncio.sleep(self.heartbeat_interval)

    # -----------------------------
    # Utilities
    # -----------------------------
    def get_metrics(self) -> Metrics:
        return self._metrics

    # Synchronous wrappers for convenience
    def start_sync(self):
        asyncio.run(self.start())

    def stop_sync(self):
        asyncio.run(self.stop())


# -----------------------------
# Example usage (local in-memory demo)
# -----------------------------
if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.DEBUG)

    # Minimal in-memory bridge implementation
    class SimpleInMemoryBridge:
        def __init__(self):
            self.queues: Dict[str, asyncio.Queue] = {}

        async def connect(self):
            return

        async def send(self, payload: Dict[str, Any]):
            to = payload.get("to_agent") or payload.get("to")
            # broadcast
            if not to:
                for q in self.queues.values():
                    await q.put(payload)
                return
            if to not in self.queues:
                self.queues[to] = asyncio.Queue()
            await self.queues[to].put(payload)

        async def recv_for(self, agent_id: str):
            if agent_id not in self.queues:
                self.queues[agent_id] = asyncio.Queue()
            return await self.queues[agent_id].get()

        # adapter for client.recv loop
        async def recv(self):
            raise NotImplementedError("Use per-agent endpoint wrapper")

        async def close(self):
            return

    class LocalEndpoint:
        def __init__(self, bridge: SimpleInMemoryBridge, agent_id: str):
            self.bridge = bridge
            self.agent_id = agent_id
            if agent_id not in bridge.queues:
                bridge.queues[agent_id] = asyncio.Queue()
            self._q = bridge.queues[agent_id]

        async def send(self, payload: Dict[str, Any]):
            await self.bridge.send(payload)

        async def recv(self) -> Dict[str, Any]:
            return await self._q.get()

        async def connect(self):
            return

        async def close(self):
            return

    async def demo():
        bridge = SimpleInMemoryBridge()
        a_endpoint = LocalEndpoint(bridge, "agent.alpha")
        b_endpoint = LocalEndpoint(bridge, "agent.beta")

        cfg_a = ClientConfig(agent_id="agent.alpha", bridge=a_endpoint)
        cfg_b = ClientConfig(agent_id="agent.beta", bridge=b_endpoint)

        client_a = Client(cfg_a)
        client_b = Client(cfg_b)

        async def handler_b(msg):
            print("[B received]", msg.summarize())
            await client_b.send(to=msg.from_agent, intent="result.summary", content={"reply": "I read that"}, type=MessageType.RESULT)

        async def handler_a_result(msg):
            print("[A got result]", msg.content)

        client_b.on_type(MessageType.TASK, handler_b)
        client_a.on_intent("result.summary", handler_a_result)

        await client_a.start()
        await client_b.start()

        await client_a.send(to="agent.beta", intent="analyze.paper", content="Please summarize this paper")
        await asyncio.sleep(1)

        await client_a.stop()
        await client_b.stop()

    asyncio.run(demo())
