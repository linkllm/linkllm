# registry.py — Persistent Memory & Message Store for LinkLLM
# ------------------------------------------------------------
# Provides a uniform async interface to store and retrieve messages,
# context fragments, and agent memories across sessions.
# Supports pluggable backends: SQLite (local), Redis (distributed).
# ------------------------------------------------------------

from __future__ import annotations
import asyncio
import json
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

try:
    import aiosqlite
except ImportError:
    aiosqlite = None

try:
    import aioredis
except ImportError:
    aioredis = None

from link_llm.protocol import LinkMessage

# ------------------------------------------------------------
# 🧩 Abstract Registry Interface
# ------------------------------------------------------------

class RegistryInterface(ABC):
    """Defines the persistence interface for message and memory storage."""

    @abstractmethod
    async def store_message(self, msg: LinkMessage):
        pass

    @abstractmethod
    async def get_recent_messages(self, agent_id: str, limit: int = 50) -> List[Dict[str, Any]]:
        pass

    @abstractmethod
    async def store_memory(self, key: str, value: Any):
        pass

    @abstractmethod
    async def get_memory(self, key: str) -> Optional[Any]:
        pass

    @abstractmethod
    async def delete_memory(self, key: str):
        pass

    @abstractmethod
    async def close(self):
        pass

# ------------------------------------------------------------
# 💾 SQLite Implementation
# ------------------------------------------------------------

class SQLiteRegistry(RegistryInterface):
    def __init__(self, path: str = ":memory:"):
        if not aiosqlite:
            raise ImportError("aiosqlite is required for SQLiteRegistry")
        self.path = path
        self.db: Optional[aiosqlite.Connection] = None

    async def start(self):
        self.db = await aiosqlite.connect(self.path)
        await self.db.execute(
            """CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts REAL,
            from_agent TEXT,
            to_agent TEXT,
            intent TEXT,
            type TEXT,
            content TEXT
            )"""
        )
        await self.db.execute(
            """CREATE TABLE IF NOT EXISTS memory (
            key TEXT PRIMARY KEY,
            value TEXT,
            updated_at REAL
            )"""
        )
        await self.db.commit()

    async def store_message(self, msg: LinkMessage):
        if not self.db:
            raise RuntimeError("SQLiteRegistry not started")
        await self.db.execute(
            "INSERT INTO messages (ts, from_agent, to_agent, intent, type, content) VALUES (?, ?, ?, ?, ?, ?)",
            (time.time(), msg.from_agent, msg.to_agent, msg.intent, msg.type.value, json.dumps(msg.to_dict())),
        )
        await self.db.commit()

    async def get_recent_messages(self, agent_id: str, limit: int = 50):
        if not self.db:
            raise RuntimeError("SQLiteRegistry not started")
        async with self.db.execute(
            "SELECT content FROM messages WHERE from_agent=? OR to_agent=? ORDER BY ts DESC LIMIT ?",
            (agent_id, agent_id, limit),
        ) as cur:
            rows = await cur.fetchall()
        return [json.loads(r[0]) for r in rows]

    async def store_memory(self, key: str, value: Any):
        if not self.db:
            raise RuntimeError("SQLiteRegistry not started")
        await self.db.execute(
            "REPLACE INTO memory (key, value, updated_at) VALUES (?, ?, ?)",
            (key, json.dumps(value), time.time()),
        )
        await self.db.commit()

    async def get_memory(self, key: str):
        if not self.db:
            raise RuntimeError("SQLiteRegistry not started")
        async with self.db.execute("SELECT value FROM memory WHERE key=?", (key,)) as cur:
            row = await cur.fetchone()
        return json.loads(row[0]) if row else None

    async def delete_memory(self, key: str):
        if not self.db:
            raise RuntimeError("SQLiteRegistry not started")
        await self.db.execute("DELETE FROM memory WHERE key=?", (key,))
        await self.db.commit()

    async def close(self):
        if self.db:
            await self.db.close()

# ------------------------------------------------------------
# ☁️ Redis Implementation
# ------------------------------------------------------------

class RedisRegistry(RegistryInterface):
    def __init__(self, url: str = "redis://localhost:6379/0"):
        if not aioredis:
            raise ImportError("aioredis is required for RedisRegistry")
        self.url = url
        self.redis: Optional[aioredis.Redis] = None

    async def start(self):
        self.redis = await aioredis.from_url(self.url, decode_responses=True)

    async def store_message(self, msg: LinkMessage):
        if not self.redis:
            raise RuntimeError("RedisRegistry not started")
        key = f"messages:{msg.to_agent or 'broadcast'}"
        await self.redis.lpush(key, msg.to_json())
        await self.redis.ltrim(key, 0, 999)

    async def get_recent_messages(self, agent_id: str, limit: int = 50):
        if not self.redis:
            raise RuntimeError("RedisRegistry not started")
        key = f"messages:{agent_id}"
        data = await self.redis.lrange(key, 0, limit - 1)
        return [json.loads(d) for d in data]

    async def store_memory(self, key: str, value: Any):
        if not self.redis:
            raise RuntimeError("RedisRegistry not started")
        await self.redis.set(key, json.dumps(value))

    async def get_memory(self, key: str):
        if not self.redis:
            raise RuntimeError("RedisRegistry not started")
        data = await self.redis.get(key)
        return json.loads(data) if data else None

    async def delete_memory(self, key: str):
        if not self.redis:
            raise RuntimeError("RedisRegistry not started")
        await self.redis.delete(key)

    async def close(self):
        if self.redis:
            await self.redis.close()

# ------------------------------------------------------------
# 🧠 Simple In-Memory Registry (for testing)
# ------------------------------------------------------------

class InMemoryRegistry(RegistryInterface):
    def __init__(self):
        self.messages: List[Dict[str, Any]] = []
        self.memory: Dict[str, Any] = {}
        self.lock = asyncio.Lock()

    async def store_message(self, msg: LinkMessage):
        async with self.lock:
            self.messages.append(msg.to_dict())

    async def get_recent_messages(self, agent_id: str, limit: int = 50):
        async with self.lock:
            filtered = [m for m in self.messages if m.get("from_agent") == agent_id or m.get("to_agent") == agent_id]
            return list(reversed(filtered[-limit:]))

    async def store_memory(self, key: str, value: Any):
        async with self.lock:
            self.memory[key] = value

    async def get_memory(self, key: str):
        async with self.lock:
            return self.memory.get(key)

    async def delete_memory(self, key: str):
        async with self.lock:
            if key in self.memory:
                del self.memory[key]

    async def close(self):
        return

# ------------------------------------------------------------
# 🌟 Example Usage
# ------------------------------------------------------------

if __name__ == "__main__":
    import asyncio

    async def main():
        reg = InMemoryRegistry()
        msg = LinkMessage(from_agent="agent.alpha", to_agent="agent.beta", intent="demo", content="Hello!")
        await reg.store_message(msg)
        print(await reg.get_recent_messages("agent.alpha"))

        await reg.store_memory("foo", {"bar": 42})
        print(await reg.get_memory("foo"))

    asyncio.run(main())
