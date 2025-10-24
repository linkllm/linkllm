"""
link_llm
========

The LLM-Link Python SDK provides a foundation for building multi-agent systems
that can communicate, coordinate, and share context via the LLM-Link Protocol.

Key components:
- LinkClient: The high-level, developer-friendly agent interface.
- WebsocketBridge: The low-level, self-healing WebSocket connection layer.
- Protocol: Data models and utilities for the LLM-Link message specification.

:copyright: (c) 2024 by LLM-Link Foundation.
:license: MIT, see LICENSE for more details.
"""

from .protocol import (
    LinkMessage,
    MessageType,
    TaskStatus,
    MemoryPersistence,
    ValidationError,
    BROADCAST_ID,
    PROTOCOL_VERSION
)
from .bridge import WebsocketBridge, MessageCallback
from .client import LinkClient, TaskHandler


# Define the public API for the SDK
__all__ = [
    # Core Client
    "LinkClient",
    "TaskHandler",
    
    # Low-Level Bridge (if needed)
    "WebsocketBridge",
    "MessageCallback",
    
    # Protocol Types & Utilities
    "LinkMessage",
    "MessageType",
    "TaskStatus",
    "MemoryPersistence",
    "ValidationError",
    "BROADCAST_ID",
    "PROTOCOL_VERSION",
]

# Set the version of the SDK
__version__ = "0.1.0-alpha"
