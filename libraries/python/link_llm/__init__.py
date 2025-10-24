"""
LLM-Link Core SDK
~~~~~~~~~~~~~~~~~

This package provides the core client-side utilities for agents to connect
to and interact with the LLM-Link Context Bus.

It exposes the main protocol definitions, the connection bridge, and the
high-level client.

Example:
    from link_llm import LinkClient, LinkMessage, MessageType, TaskRequestPayload

    client = LinkClient(agent_id="my-agent")

    @client.on_task("do_work")
    async def handle_work(payload: TaskRequestPayload):
        print(f"Received task: {payload.task_name}")
        return {"status": "done", "result": payload.content * 2}
    
    await client.connect("ws://localhost:8765")

"""

import logging

# Set up a default logger for the library.
# The user can override this by configuring their own logging.
logging.getLogger(__name__).addHandler(logging.NullHandler())

# Import and expose key components from the protocol module
# to the top level of the package.
from .protocol import (
    # --- Enums ---
    MessageType,
    TaskStatus,
    MemoryPersistence,
    
    # --- Constants ---
    BROADCAST_ID,
    PROTOCOL_VERSION,
    
    # --- Core Models ---
    LinkMessage,
    BasePayload,
    
    # --- Payload Models ---
    AgentRegisterPayload,
    TaskRequestPayload,
    TaskResponsePayload,
    ContextSharePayload,
    MemoryUpdatePayload,
    MemoryQueryPayload,
    MemoryResponsePayload,
    ErrorPayload,
    
    # --- Helper Functions ---
    create_message,
    serialize_message,
    parse_message,
    parse_payload,
    ValidationError
)

# We will add imports from `bridge.py` and `client.py` here
# once they are created.
# from .client import LinkClient
# from .bridge import WebsocketBridge

# Define __all__ to control `from link_llm import *`
__all__ = [
    # from protocol.py
    "MessageType",
    "TaskStatus",
    "MemoryPersistence",
    "BROADCAST_ID",
    "PROTOCOL_VERSION",
    "LinkMessage",
    "BasePayload",
    "AgentRegisterPayload",
    "TaskRequestPayload",
    "TaskResponsePayload",
    "ContextSharePayload",
    "MemoryUpdatePayload",
    "MemoryQueryPayload",
    "MemoryResponsePayload",
    "ErrorPayload",
    "create_message",
    "serialize_message",
    "parse_message",
    "parse_payload",
    "ValidationError",
    
    # to be added from client.py
    # "LinkClient",
    
    # to be added from bridge.py
    # "WebsocketBridge",
]

# Set the package version
__version__ = PROTOCOL_VERSION
