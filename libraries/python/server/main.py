"""
main.py — Runs the LinkLLM Context Bus Server
"""

import asyncio
import logging
import sys
import os

# --- Path Fix ---
# Add the parent directory ('python') to the system path
# This allows us to correctly import the 'link_llm' package
script_dir = os.path.dirname(__file__)
parent_dir = os.path.abspath(os.path.join(script_dir, '..'))
sys.path.insert(0, parent_dir)
# --- End Path Fix ---

try:
    # This import should now work thanks to the path fix
    from link_llm.protocol import PROTOCOL_VERSION
except ImportError:
    print("Error: Could not find the 'link_llm' package.")
    print("Please ensure 'main.py' is inside 'server/' and 'link_llm/' is inside 'python/'.")
    sys.exit(1)

from server.registry import MemoryRegistry
from server.bus import ContextBus

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def main():
    """Initializes and starts all server components."""
    logger.info(f"--- LLM-Link Context Bus v{PROTOCOL_VERSION} ---")
    
    # 1. Initialize the Memory Registry
    #    (Currently in-memory, swappable with Redis/SQLite later)
    registry = MemoryRegistry()
    
    # 2. Initialize the Context Bus
    bus = ContextBus(
        host="localhost", 
        port=8765, 
        registry=registry
    )
    
    # 3. Start the server and run forever
    try:
        await bus.start()
    except KeyboardInterrupt:
        logger.info("Shutdown signal received.")
    finally:
        await bus.stop()
        logger.info("Server shut down gracefully.")

if __name__ == "__main__":
    asyncio.run(main())
