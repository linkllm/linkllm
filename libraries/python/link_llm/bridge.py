# llm_link/bridge.py
import asyncio
import websockets
from .protocol import Message

class Bridge:
    """Future WebSocket bridge for connecting distributed agents."""

    def __init__(self, uri: str = "ws://localhost:8765"):
        self.uri = uri

    async def send(self, message: Message):
        async with websockets.connect(self.uri) as websocket:
            await websocket.send(message.to_json())
            print(f"[Bridge] Sent message to {self.uri}")

    async def listen(self):
        async with websockets.serve(self.handle_message, "localhost", 8765):
            print("[Bridge] Listening for incoming messages on ws://localhost:8765")
            await asyncio.Future()  # Run forever

    async def handle_message(self, websocket, path):
        data = await websocket.recv()
        msg = Message.from_json(data)
        print(f"[Bridge] Received from {msg.from_agent}: {msg.task}")
