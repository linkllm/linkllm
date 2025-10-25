# main.py — Runs the LinkLLM Context Bus
import asyncio
import logging
from link_llm.protocol import *
from .registry import MemoryRegistry
from .bus import ContextBus

logging.basicConfig(level=logging.INFO)

async def main():
    registry = MemoryRegistry()
    bus = ContextBus(host="localhost", port=8765, registry=registry)
    await bus.start()  # starts the websocket server and keeps it running

if __name__ == "__main__":
    asyncio.run(main())
