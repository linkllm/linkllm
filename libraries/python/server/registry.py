"""
link_llm.server.registry
~~~~~~~~~~~~~~~~~~~~~~~~

This module implements the Memory Registry, a thread-safe, asynchronous
key-value store for shared context (memory blocks).

It uses an in-memory dictionary protected by an asyncio.Lock for concurrency.
It correctly handles different persistence levels (PERMANENT, SESSION, VOLATILE)
and is responsible for cleaning up SESSION memory when an agent disconnects.
"""

import asyncio
import logging
from typing import Any, Dict, Optional, Tuple, List

# Import necessary protocol components
# Assumes 'link_llm' is in the PYTHONPATH or relative import works
from link_llm.protocol import (
    MemoryUpdatePayload,
    MemoryResponsePayload,
    MemoryPersistence,
    BROADCAST_ID,
    TaskStatus
)

logger = logging.getLogger(__name__)

# Structure to store memory: (data, persistence, owner_id)
# owner_id is only required for SESSION persistence for cleanup purposes.
MemoryEntry = Tuple[Any, MemoryPersistence, Optional[str]]

class MemoryRegistry:
    """
    Manages the storage and retrieval of key-value memory blocks.

    This implementation uses a simple in-memory dictionary, protected
    by a Lock for concurrency safety within the asyncio event loop.
    
    It tracks the 'owner' of session-level memory for automatic cleanup.
    """
    
    def __init__(self):
        # Key is str (memory key), Value is MemoryEntry
        self._memory: Dict[str, MemoryEntry] = {}
        self._lock = asyncio.Lock()
        logger.info("MemoryRegistry initialized (In-memory mode).")

    async def update_memory(self, payload: MemoryUpdatePayload) -> bool:
        """
        Stores or updates a memory block using the provided payload.
        
        Session memory must have an owner_id for later cleanup.
        
        Args:
            payload: The MemoryUpdatePayload containing key, data, persistence, and owner_id.

        Returns:
            True if the memory was successfully updated/stored.
        """
        key = payload.key
        persistence = payload.persistence
        owner_id = payload.owner_id
        
        # Validation check: SESSION persistence MUST have an owner_id
        if persistence == MemoryPersistence.SESSION and not owner_id:
            logger.error(f"Cannot store SESSION memory block '{key}': owner_id is required.")
            return False

        async with self._lock:
            self._memory[key] = (payload.data, persistence, owner_id)
            logger.debug(f"Memory key '{key}' updated with persistence '{persistence.value}'.")
            return True

    async def query_memory(self, key: str) -> MemoryResponsePayload:
        """
        Retrieves a memory block by its key.
        
        Args:
            key: The unique identifier for the memory block.

        Returns:
            A MemoryResponsePayload indicating if the key was found and the data.
        """
        async with self._lock:
            entry = self._memory.get(key)
            
            if entry:
                data, persistence, _ = entry
                
                # If VOLATILE memory is retrieved, it is immediately deleted.
                if persistence == MemoryPersistence.VOLATILE:
                    del self._memory[key]
                    logger.debug(f"Volatile memory key '{key}' retrieved and deleted.")

                return MemoryResponsePayload(
                    key=key,
                    found=True,
                    data=data,
                    persistence=persistence.value,
                    error_message=None
                )
            else:
                logger.debug(f"Memory key '{key}' not found.")
                return MemoryResponsePayload(
                    key=key,
                    found=False,
                    data=None,
                    persistence=None,
                    error_message=f"Memory key '{key}' not found in registry."
                )

    async def cleanup_session_memory(self, agent_id: str) -> int:
        """
        Removes all memory blocks with SESSION persistence
        owned by a specific agent_id.
        
        This is called by the ContextBus when an agent disconnects.
        
        Args:
            agent_id: The ID of the agent that disconnected.

        Returns:
            The number of memory entries deleted.
        """
        if not agent_id:
            return 0
            
        async with self._lock:
            keys_to_delete = []
            
            # Iterate and identify keys to delete
            for key, (_, persistence, owner) in self._memory.items():
                if persistence == MemoryPersistence.SESSION and owner == agent_id:
                    keys_to_delete.append(key)
            
            # Perform deletion
            for key in keys_to_delete:
                del self._memory[key]
                
            if keys_to_delete:
                logger.info(f"Cleaned up {len(keys_to_delete)} session memory entries for agent {agent_id}.")
                
            return len(keys_to_delete)

    async def get_all_keys(self) -> List[str]:
        """Returns a list of all currently stored memory keys."""
        async with self._lock:
            return list(self._memory.keys())

    async def clear_all(self):
        """DANGEROUS: Clears all memory blocks, regardless of persistence."""
        async with self._lock:
            count = len(self._memory)
            self._memory.clear()
            logger.warning(f"DANGER: Cleared {count} memory blocks from the registry.")

__all__ = ["MemoryRegistry"]
