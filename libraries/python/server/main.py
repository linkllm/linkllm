"""
main.py — Runs the LinkLLM Context Bus Server

This is the main entry point to start the Context Bus, which acts as the
central message broker and state manager for the LLM-Link network.

It initializes the MemoryRegistry and the ContextBus, then runs the
asynchronous server loop.
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
# Ensure the 'python' directory is in the path
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)
# --- End Path Fix ---\

try:
    # Import necessary component from the protocol package
    from link_llm.protocol import PROTOCOL_VERSION
except ImportError:
    print("Error: Could not find the 'link_llm' package.")
    print("Please ensure 'main.py' is inside 'server/' and the 'link_llm/' package is available.")
    sys.exit(1)

# Import our server components
from .registry import MemoryRegistry
from .bus import ContextBus

# Configure logging to display time, logger name, level, and message
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def main():
    """Initializes and starts all server components."""
    # Define server configuration
    HOST = "0.0.0.0"  # Listen on all available interfaces
    PORT = 8765

    logger.info(f"--- LLM-Link Context Bus v{PROTOCOL_VERSION} ---")

    # 1. Initialize the Memory Registry
    #    This is currently an in-memory storage, ready to be swapped with
    #    a persistent store (like Redis/SQLite) in future versions.
    registry = MemoryRegistry()

    # 2. Initialize the Context Bus
    bus = ContextBus(
        host=HOST,
        port=PORT,
        registry=registry
    )

    # 3. Start the Context Bus server
    logger.info(f"Starting ContextBus server on ws://{HOST}:{PORT}...")
    try:
        # The 'run_server' method handles the websocket server startup
        await bus.run_server()
    except KeyboardInterrupt:
        logger.info("Context Bus server received interrupt. Shutting down...")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}", exc_info=True)
    finally:
        # Clean shutdown (currently handled internally by run_server/serve)
        logger.info("Context Bus server stopped.")


if __name__ == "__main__":
    # Windows fix: On Windows, use a different policy for asyncio
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
    try:
        # Run the main asynchronous function
        asyncio.run(main())
    except Exception as e:
        # Catch any exceptions that escape the main loop
        logger.critical(f"Fatal error during startup or shutdown: {e}")
