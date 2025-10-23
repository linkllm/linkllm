# __init__.py for link_llm core SDK
# This file defines package exports and initializes default logging/configurations.

"""link_llm: Core SDK for the LLM-Link protocol

The `link_llm` package provides the foundation for the LLM-Link ecosystem.
It defines the cross-agent communication protocol, bridges, and client logic
for message routing, context sharing, and interoperability between LLMs.

Example:
    >>> from link_llm import Client, WebSocketClientBridge
    >>> bridge = WebSocketClientBridge('ws://localhost:8765')
    >>> client = Client('agent.alpha', bridge)
    >>> await client.start()
    >>> await client.send(to='agent.beta', intent='summarize', content='Hello world')

"""

import logging

# Configure default logger for link_llm
def _configure_logging():
    logger = logging.getLogger('link_llm')
    if not logger.handlers:
        handler = logging.StreamHandler()
        fmt = logging.Formatter('[%(levelname)s] %(asctime)s | %(name)s | %(message)s', "%H:%M:%S")
        handler.setFormatter(fmt)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
_configure_logging()

__version__ = '0.1.0'
__author__ = 'linkllm org'
__license__ = 'MIT'
__description__ = 'Core SDK for the LLM-Link protocol'

# Re-exports
from .protocol import LinkMessage, MessageType, Role
from .client import Client
from .bridge import WebSocketClientBridge, HttpBridge

__all__ = [
    'LinkMessage',
    'MessageType',
    'Role',
    'Client',
    'WebSocketClientBridge',
    'HttpBridge',
]
