# bus.py — Message Bus for LinkLLM
# ------------------------------------------------------------
# Asynchronous, production-oriented message router for LinkLLM.
# Responsibilities:
# - Accept messages from bridges/endpoints (WebSocket/HTTP/InMemory)
# - Route messages to target agents (direct, broadcast, topic-based)
# - Persist messages / memory pointers via a pluggable Registry
# - Provide hooks for observability, tracing and policy checks
# - Backpressure, rate-limiting, and graceful shutdown
#
# Design notes:
# - The Bus is intentionally transport-agnostic: bridges produce/consume
#   plain dict payloads (conforming to link_llm.protocol.LinkMessage schema).
# - The Bus maintains an in-memory registry of connected sessions (AgentSession)
#   and per-agent asyncio Queues as delivery buffers. Endpoints call `register_session`
#   to provide a callback/endpoint that can `send` messages to a particular agent.
# - The Bus delegates durable storage to a Registry implementation (Redis or SQLite)
#   via the RegistryInterface.
#
# Usage (high-level):
#   bus = Bus(registry=SomeRegistry())
#   await bus.start()
#   await bus.register_session(agent_id, send_callable)
#   await bus.handle_incoming(payload_dict)
#
# ------------------------------------------------------------
from .protocol import LinkMessage
from __future__ import annotations
import asyncio
import logging
import time
import uuid
from typing import Any, Awaitable, Callable, Dict, Optional, List, Iterable
from dataclasses import dataclass


from pydantic import ValidationError

# Import canonical message model
try:
    from .protocol import LinkMessage
except Exception:
    # allow running the file standalone in isolated contexts for tests
    LinkMessage = Any

logger = logging.getLogger("link_llm.bus")

# -----------------------------
# Registry Interface
# -----------------------------
class RegistryInterface:
    """Pluggable storage interface for persisting memories, messages, and indexes.

    Implementations MUST be fully async-friendly.
    """

    async def store_message(self, message: Dict[str, Any]) -> None:
        raise NotImplementedError

    async def fetch_messages_for_agent(self, agent_id: str, limit: int = 100) -> List[Dict[str, Any]]:
        raise NotImplementedError

    async def save_memory(self, key: str, value: Dict[str, Any]) -> None:
        raise NotImplementedError

    async def get_memory(self, key: str) -> Optional[Dict[str, Any]]:
        raise NotImplementedError


# -----------------------------
# In-process simple SQLite Registry (async wrapper)
# -----------------------------
class SQLiteRegistry(RegistryInterface):
    """Lightweight SQLite-backed registry using aiosqlite when available.
    This is intended for local/dev use. For production, prefer Redis or a proper DB.
    """

    def __init__(self, path: str = "linkllm_registry.db"):
        try:
            import aiosqlite  # type: ignore
        except Exception:
            raise RuntimeError("aiosqlite is required for SQLiteRegistry: pip install aiosqlite")
        self.path = path
        self._conn = None
        self._init_lock = asyncio.Lock()

    async def _ensure(self):
        if self._conn is not None:
            return
        async with self._init_lock:
            if self._conn is not None:
                return
            import aiosqlite  # type: ignore
            self._conn = await aiosqlite.connect(self.path)
            await self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS messages (
                    id TEXT PRIMARY KEY,
                    agent TEXT,
                    payload TEXT,
                    created_at REAL
                )
                """
            )
            await self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS memory (
                    key TEXT PRIMARY KEY,
                    value TEXT,
                    updated_at REAL
                )
                """
            )
            await self._conn.commit()

    async def store_message(self, message: Dict[str, Any]) -> None:
        await self._ensure()
        import json
        id_ = message.get("header", {}).get("id") or str(uuid.uuid4())
        agent = message.get("to_agent") or message.get("to") or "__broadcast__"
        raw = json.dumps(message, default=str)
        ts = time.time()
        await self._conn.execute("INSERT OR REPLACE INTO messages (id, agent, payload, created_at) VALUES (?, ?, ?, ?)", (id_, agent, raw, ts))
        await self._conn.commit()

    async def fetch_messages_for_agent(self, agent_id: str, limit: int = 100) -> List[Dict[str, Any]]:
        await self._ensure()
        import json
        cur = await self._conn.execute("SELECT payload FROM messages WHERE agent = ? ORDER BY created_at DESC LIMIT ?", (agent_id, limit))
        rows = await cur.fetchall()
        return [json.loads(r[0]) for r in rows]

    async def save_memory(self, key: str, value: Dict[str, Any]) -> None:
        await self._ensure()
        import json
        raw = json.dumps(value, default=str)
        ts = time.time()
        await self._conn.execute("INSERT OR REPLACE INTO memory (key, value, updated_at) VALUES (?, ?, ?)", (key, raw, ts))
        await self._conn.commit()

    async def get_memory(self, key: str) -> Optional[Dict[str, Any]]:
        await self._ensure()
        import json
        cur = await self._conn.execute("SELECT value FROM memory WHERE key = ?", (key,))
        row = await cur.fetchone()
        return json.loads(row[0]) if row else None


# -----------------------------
# Minimal Redis Registry (optional production-grade)
# -----------------------------
class RedisRegistry(RegistryInterface):
    def __init__(self, url: str = "redis://localhost:6379/0"):
        try:
            import aioredis  # type: ignore
        except Exception:
            raise RuntimeError("aioredis is required for RedisRegistry: pip install aioredis")
        import aioredis as _aioredis
        self._url = url
        self._client = _aioredis.from_url(url)

    async def store_message(self, message: Dict[str, Any]) -> None:
        import json
        id_ = message.get("header", {}).get("id") or str(uuid.uuid4())
        to = message.get("to_agent") or message.get("to") or "__broadcast__"
        raw = json.dumps(message, default=str)
        # store in a list per agent
        await self._client.lpush(f"messages:{to}", raw)
        # also keep a global stream (optional)
        await self._client.lpush("messages:global", raw)

    async def fetch_messages_for_agent(self, agent_id: str, limit: int = 100) -> List[Dict[str, Any]]:
        import json
        raws = await self._client.lrange(f"messages:{agent_id}", 0, limit - 1)
        return [json.loads(r) for r in raws]

    async def save_memory(self, key: str, value: Dict[str, Any]) -> None:
        import json
        await self._client.set(f"memory:{key}", json.dumps(value, default=str))

    async def get_memory(self, key: str) -> Optional[Dict[str, Any]]:
        import json
        raw = await self._client.get(f"memory:{key}")
        return json.loads(raw) if raw else None


# -----------------------------
# Agent Session
# -----------------------------
@dataclass
class AgentSession:
    agent_id: str
    # send_callable: provided by the endpoint (LocalEndpoint / WebSocket handler / HTTP push)
    send_callable: Callable[[Dict[str, Any]], Awaitable[None]]
    inbox: asyncio.Queue
    last_seen: float = 0.0
    metadata: Dict[str, Any] = None

    def touch(self):
        self.last_seen = time.time()


# -----------------------------
# Bus
# -----------------------------
class Bus:
    """Core message bus that routes LinkMessage dict payloads between agents.

    Main entrypoints:
      - register_session(agent_id, send_callable, metadata=None)
      - unregister_session(agent_id)
      - handle_incoming(payload_dict)
      - start() / stop()

    The Bus will validate incoming payloads against LinkMessage and persist
    messages via Registry if configured.
    """

    def __init__(self, registry: Optional[RegistryInterface] = None, *, max_queue_size: int = 1024, delivery_workers: int = 4):
        self._sessions: Dict[str, AgentSession] = {}
        self._session_lock = asyncio.Lock()
        self._registry = registry
        self._max_queue_size = max_queue_size
        self._delivery_workers = delivery_workers
        self._delivery_tasks: List[asyncio.Task] = []
        self._incoming_queue: asyncio.Queue = asyncio.Queue()
        self._running = False
        self._stop_event = asyncio.Event()
        self._metrics = {
            "inbound": 0,
            "routed": 0,
            "dropped": 0,
        }

    # -----------------------------
    # Lifecycle
    # -----------------------------
    async def start(self):
        if self._running:
            return
        logger.info("Bus starting")
        self._running = True
        # start delivery workers
        for i in range(self._delivery_workers):
            t = asyncio.create_task(self._delivery_loop(i))
            self._delivery_tasks.append(t)
        # start incoming queue pump
        self._pump_task = asyncio.create_task(self._incoming_pump())

    async def stop(self):
        if not self._running:
            return
        logger.info("Bus stopping")
        self._running = False
        self._stop_event.set()
        # cancel pump
        try:
            self._pump_task.cancel()
        except Exception:
            pass
        # cancel delivery tasks
        for t in self._delivery_tasks:
            t.cancel()
        # drain sessions
        async with self._session_lock:
            for s in self._sessions.values():
                # best-effort notify
                try:
                    await s.send_callable({"type": "shutdown", "reason": "bus_stopping"})
                except Exception:
                    pass
        logger.info("Bus stopped")

    # -----------------------------
    # Session management
    # -----------------------------
    async def register_session(self, agent_id: str, send_callable: Callable[[Dict[str, Any]], Awaitable[None]], metadata: Optional[Dict[str, Any]] = None) -> AgentSession:
        """Register an active agent session and return an AgentSession object.

        `send_callable` should be an async function that accepts a JSON-serializable dict
        and delivers it to the agent's endpoint (WebSocket send, HTTP push, local queue, etc.).
        """
        async with self._session_lock:
            if agent_id in self._sessions:
                # refresh existing session
                sess = self._sessions[agent_id]
                sess.send_callable = send_callable
                sess.metadata = metadata or {}
                sess.touch()
                logger.debug("Session refreshed for %s", agent_id)
                return sess
            q = asyncio.Queue(maxsize=self._max_queue_size)
            sess = AgentSession(agent_id=agent_id, send_callable=send_callable, inbox=q, last_seen=time.time(), metadata=metadata or {})
            self._sessions[agent_id] = sess
            logger.info("Registered session for %s", agent_id)
            return sess

    async def unregister_session(self, agent_id: str) -> None:
        async with self._session_lock:
            sess = self._sessions.pop(agent_id, None)
            if sess:
                # drain queue
                try:
                    while not sess.inbox.empty():
                        sess.inbox.get_nowait()
                        sess.inbox.task_done()
                except Exception:
                    pass
                logger.info("Unregistered session for %s", agent_id)

    async def list_sessions(self) -> List[str]:
        async with self._session_lock:
            return list(self._sessions.keys())

    # -----------------------------
    # Incoming handling
    # -----------------------------
    async def handle_incoming(self, raw_payload: Dict[str, Any]) -> None:
        """Accept a payload (likely from a bridge) and enqueue for routing.

        This method performs quick validation and then places the payload into the
        Bus internal queue for eventual delivery by delivery workers.
        """
        self._metrics["inbound"] += 1
        # Validate/normalize LinkMessage
        try:
            if LinkMessage is not Any:
                msg = LinkMessage.from_dict(raw_payload) if hasattr(LinkMessage, 'from_dict') else LinkMessage.parse_obj(raw_payload)
            else:
                msg = raw_payload
        except ValidationError as e:
            logger.warning("Invalid message received: %s", e)
            return
        except Exception as e:
            logger.exception("Failed to parse incoming message: %s", e)
            return

        # persist incoming message (best-effort)
        if self._registry:
            try:
                await self._registry.store_message(msg.to_dict() if hasattr(msg, 'to_dict') else dict(msg))
            except Exception:
                logger.exception("Registry store_message failed; continuing")

        # enqueue for routing
        await self._incoming_queue.put(msg)

    async def _incoming_pump(self):
        while self._running:
            try:
                msg = await self._incoming_queue.get()
                # simple routing decision
                await self._route_message(msg)
                self._incoming_queue.task_done()
            except asyncio.CancelledError:
                return
            except Exception as e:
                logger.exception("Error in incoming pump: %s", e)
                await asyncio.sleep(0.1)

    # -----------------------------
    # Routing logic
    # -----------------------------
    async def _route_message(self, msg: Any) -> None:
        """Decide destination(s) and enqueue message into per-agent inboxes.

        Supports the following semantics for LinkMessage.to_agent / to:
          - None or empty -> broadcast to all connected agents
          - single agent id -> direct delivery
          - topic: prefix 'topic:' -> deliver to sessions subscribed to that topic (not yet implemented)
        """
        try:
            to_agent = getattr(msg, 'to_agent', None) or msg.get('to', None) if isinstance(msg, dict) else None
            from_agent = getattr(msg, 'from_agent', None) or msg.get('from', None) if isinstance(msg, dict) else None
            # Direct delivery
            if to_agent:
                await self._deliver_to_agent(str(to_agent), msg)
                self._metrics['routed'] += 1
                return

            # Broadcast
            # Deliver to all connected sessions except the sender
            async with self._session_lock:
                targets = [s for aid, s in self._sessions.items() if aid != from_agent]
            if not targets:
                logger.debug("Broadcast message dropped: no connected targets")
                self._metrics['dropped'] += 1
                return
            for s in targets:
                await self._enqueue_for_session(s, msg)
            self._metrics['routed'] += len(targets)
        except Exception:
            logger.exception("Routing failure for message: %s", getattr(msg, 'to_dict', lambda: msg)())

    async def _deliver_to_agent(self, agent_id: str, msg: Any) -> None:
        async with self._session_lock:
            sess = self._sessions.get(agent_id)
        if sess:
            await self._enqueue_for_session(sess, msg)
        else:
            # Agent offline: store for retrieval if registry available
            logger.debug("Agent %s offline — persisting for later (if registry) or dropping", agent_id)
            self._metrics['dropped'] += 1
            if self._registry:
                try:
                    await self._registry.store_message(msg.to_dict() if hasattr(msg, 'to_dict') else dict(msg))
                except Exception:
                    logger.exception("Failed to persist message for offline agent %s", agent_id)

    async def _enqueue_for_session(self, sess: AgentSession, msg: Any) -> None:
        # non-blocking enqueue with backpressure handling
        try:
            if sess.inbox.full():
                # backpressure policy: drop oldest to make room (ring buffer style)
                try:
                    _ = sess.inbox.get_nowait()
                    sess.inbox.task_done()
                except Exception:
                    pass
            await sess.inbox.put(msg)
        except asyncio.QueueFull:
            logger.warning("Session inbox full for %s — dropping message", sess.agent_id)
            self._metrics['dropped'] += 1

    # -----------------------------
    # Delivery workers: read per-session queues and call send_callable
    # -----------------------------
    async def _delivery_loop(self, worker_idx: int):
        logger.info("Delivery worker %d starting", worker_idx)
        while self._running:
            try:
                # pick a session with messages (naive round-robin)
                target = None
                async with self._session_lock:
                    # simple strategy: find first non-empty inbox
                    for s in self._sessions.values():
                        if not s.inbox.empty():
                            target = s
                            break
                if target is None:
                    # nothing to deliver
                    await asyncio.sleep(0.02)
                    continue
                # deliver up to a small batch
                batch = []
                while not target.inbox.empty() and len(batch) < 8:
                    batch.append(target.inbox.get_nowait())
                # send messages in order
                for m in batch:
                    try:
                        # send_callable is an async function that knows how to deliver msg to endpoint
                        await target.send_callable(m.to_dict() if hasattr(m, 'to_dict') else (m if isinstance(m, dict) else dict(m)))
                        # bookkeeping
                        target.inbox.task_done()
                    except Exception:
                        logger.exception("Failed to deliver message to %s — requeueing once", target.agent_id)
                        # best-effort single requeue
                        try:
                            await target.inbox.put(m)
                        except Exception:
                            logger.exception("Requeue failed; dropping message for %s", target.agent_id)
            except asyncio.CancelledError:
                return
            except Exception:
                logger.exception("Delivery worker %d crashed", worker_idx)
                await asyncio.sleep(0.1)
        logger.info("Delivery worker %d exiting", worker_idx)

    # -----------------------------
    # Utilities
    # -----------------------------
    def get_metrics(self) -> Dict[str, Any]:
        return dict(self._metrics)

    async def replay_for_agent(self, agent_id: str, limit: int = 100) -> List[Dict[str, Any]]:
        """Fetch persisted messages for an agent (from registry) for catch-up.

        Returns a list of raw message dicts in chronological order (oldest first).
        """
        if not self._registry:
            return []
        raws = await self._registry.fetch_messages_for_agent(agent_id, limit)
        return list(reversed(raws))


# -----------------------------
# Example: Minimal in-process HTTP/WebSocket Server Adapter
# -----------------------------
# NOTE: This section is intentionally minimal — actual servers should live in
# `server/` folder and wire frameworks (aiohttp, fastapi, websockets) to the Bus.
# The adapter below demonstrates how a WebSocket handler could register a session.

async def minimal_ws_handler(websocket, path, bus: Bus, agent_id: str):
    """Example handler signature for websockets (e.g. websockets.serve handler).

    - `websocket` is a connected websocket (implements .send and .recv)
    - `agent_id` is an identifier supplied by the client (auth recommended)
    """
    async def send_callable(payload: Dict[str, Any]):
        # The websocket library may expect strings — serialize
        import json
        await websocket.send(json.dumps(payload, default=str))

    sess = await bus.register_session(agent_id, send_callable, metadata={"transport": "websocket"})
    try:
        # optionally replay queued/persisted messages
        for msg in await bus.replay_for_agent(agent_id):
            await send_callable(msg)

        # main receive loop
        async for raw in websocket:
            try:
                import json
                payload = json.loads(raw)
            except Exception:
                logger.debug("Received non-json payload from %s", agent_id)
                continue
            await bus.handle_incoming(payload)
    finally:
        await bus.unregister_session(agent_id)


# ------------------------------------------------------------
# End of bus.py
# ------------------------------------------------------------
