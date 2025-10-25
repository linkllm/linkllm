# linkllm/libraries/python/server/main.py
import asyncio
import websockets
import json
from .registry import MemoryRegistry

clients = set()
registry = MemoryRegistry()

async def handler(websocket):
    clients.add(websocket)
    try:
        async for message in websocket:
            for c in clients:
                if c is not websocket:
                    await c.send(message)
    finally:
        clients.remove(websocket)

async def main():
    async with websockets.serve(handler, "localhost", 8765):
        print("Bus running on ws://localhost:8765")
        await asyncio.Future()  # keep running

if __name__ == "__main__":
    asyncio.run(main())
