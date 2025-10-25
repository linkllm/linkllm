"""
link_llm.server.registry
~~~~~~~~~~~~~~~~~~~~~~~~\

This module implements the Memory Registry. It has been updated
to handle agent-specific session memory cleanup.
"""

import asyncio
import logging
from typing import Any, Dict, Optional, Tuple, List

# Import from the link_llm package (assuming 'python' dir is in PYTHONPATH)
from link_llm.protocol import MemoryUpdatePayload, MemoryResponsePayload, MemoryPersistence

logger = logging.getLogger(__name__)


# Structure to store memory: (data, persistence, owner_id)
MemoryEntry = Tuple[Any, MemoryPersistence, Optional[str]]


class MemoryRegistry:
    """
    Manages the storage and retrieval of key-value memory blocks.

    This implementation uses a simple in-memory dictionary, protected
    by a Lock for concurrency safety within the asyncio event loop.
    
    It now tracks the 'owner' of session-level memory for cleanup.
    """
    
    def __init__(self):
        # Key is str (memory key), Value is MemoryEntry
        self._memory: Dict[str, MemoryEntry] = {}
        self._lock = asyncio.Lock()
        logger.info("MemoryRegistry initialized (In-memory mode).")

    async def update_memory(self, payload: MemoryUpdatePayload) -> bool:
        """
        Stores or updates a memory block.
        
        The `owner_id` from the payload is now used to tag session memory.
        """
        if not payload.key:
            logger.warning("Memory update rejected: Key cannot be empty.")
            return False
            
        owner_id = payload.owner_id if payload.persistence == MemoryPersistence.SESSION else None
        
        entry: MemoryEntry = (payload.data, payload.persistence, owner_id)
        
        async with self._lock:
            self._memory[payload.key] = entry
            
        logger.debug(f"Memory key '{payload.key}' updated by {payload.owner_id} "
                   f"(Persistence: {payload.persistence.value})")
        return True

    async def query_memory(self, key: str) -> MemoryResponsePayload:
        """
        Retrieves a memory block by its key.
        """
        async with self._lock:
            entry = self._memory.get(key)
            
            if entry is not None:
                data, persistence, owner = entry
                logger.debug(f"Memory key '{key}' found (Persistence: {persistence.value}, Owner: {owner}).")
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
    
    async def cleanup_session_memory(self, agent_id: str) -> int:
        """
        Removes all memory blocks with SESSION persistence
        owned by a specific agent_id.
        
        This is called by the ContextBus when an agent disconnects.
        """
        if not agent_id:
            return 0
            
        async with self._lock:
            keys_to_delete = [
                key for key, (_, persistence, owner) in self._memory.items()
                if persistence == MemoryPersistence.SESSION and owner == agent_id
            ]
            
            for key in keys_to_delete:
                del self._memory[key]
                
            if keys_to_delete:
                logger.info(f"Cleaned up {len(keys_to_delete)} session memory entries for agent {agent_id}.")
            return len(keys_to_delete)

    async def get_all_keys(self) -> List[str]:
        """Returns a list of all currently stored memory keys."""
        async with self._lock:
            return list(self._memory.keys())

__all__ = ["MemoryRegistry"]
