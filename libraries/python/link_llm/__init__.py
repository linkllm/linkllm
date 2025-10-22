# llm_link/__init__.py

"""
LLM-Link: The Open Protocol for Agent-to-Agent Collaboration
------------------------------------------------------------
Core Python SDK for LLM-Link, enabling seamless communication
and context-sharing between multiple AI agents.

Modules:
- protocol: Defines message format
- client:   Base client for agent communication
- bridge:   WebSocket / HTTP bridge (future)
"""

__version__ = "0.0.1"

from .protocol import Message
from .client import AgentClient
