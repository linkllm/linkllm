"""
link_llm.server.registry
~~~~~~~~~~~~~~~~~~~~~~~~

This module implements the Memory Registry, which is responsible for
persistent and session-level storage of context and memory blocks.

It abstracts the storage layer, allowing the Context Bus to manage
memory through simple asynchronous methods without worrying about
the underlying database (currently in-memory, designed for later
expansion to Redis, SQLite, or other persistent storage).
"""

import asyncio
import logging
from typing import Any, Dict, Optional, Tuple, List

# The relative import is correct for the `link_llm` SDK package structure
from link_llm.protocol import MemoryUpdatePayload, MemoryResponsePayload, MemoryPersistence

logger = logging.getLogger(__name__)


# Structure to store memory: (data, persistence)
MemoryEntry = Tuple[Any, MemoryPersistence]


class MemoryRegistry:
    """
    Manages the storage and retrieval of key-value memory blocks.

    This implementation uses a simple in-memory dictionary, protected
    by a Lock for concurrency safety within the asyncio event loop.
    """
    
    def __init__(self):
        # Key is str (memory key), Value is MemoryEntry
        self._memory: Dict[str, MemoryEntry] = {}
        self._lock = asyncio.Lock()
        logger.info("MemoryRegistry initialized (In-memory mode).")

    async def update_memory(self, payload: MemoryUpdatePayload) -> bool:
        """
        Stores or updates a memory block.
        """
        async with self._lock:
            try:
                # Store the raw data and the persistence enum value
                self._memory[payload.key] = (payload.data, payload.persistence)
                logger.debug(f"Memory key '{payload.key}' updated with persistence '{payload.persistence.value}'.")
                return True
            except Exception as e:
                logger.error(f"Failed to update memory for key '{payload.key}': {e}")
                return False

    async def query_memory(self, key: str) -> MemoryResponsePayload:
        """
        Retrieves a memory block by its key.
        """
        async with self._lock:
            entry = self._memory.get(key)
            
            if entry is not None:
                data, persistence = entry
                logger.debug(f"Memory key '{key}' found (Persistence: {persistence.value}).")
                return MemoryResponsePayload(
                    key=key,
                    found=True,
                    data=data
                )
            else:
                logger.debug(f"Memory key '{key}' not found.")
                return MemoryResponsePayload(
                    key=key,
                    found=False,
                    data=None
                )
    
    async def cleanup_session_memory(self) -> int:
        """
        Removes all memory blocks with SESSION persistence.
        """
        async with self._lock:
            keys_to_delete = [
                key for key, (_, persistence) in self._memory.items()
                if persistence == MemoryPersistence.SESSION
            ]
            
            for key in keys_to_delete:
                del self._memory[key]
                
            logger.info(f"Cleaned up {len(keys_to_delete)} session memory entries.")
            return len(keys_to_delete)

    async def get_all_keys(self) -> List[str]:
        """Returns a list of all currently stored memory keys."""
        async with self._lock:
            return list(self._memory.keys())

__all__ = ["MemoryRegistry"]