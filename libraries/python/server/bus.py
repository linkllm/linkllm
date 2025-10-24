"""
link_llm.bridge
~~~~~~~~~~~~~~~

This module implements the low-level communication bridge.
It handles the raw WebSocket connection, JSON serialization/deserialization,
and automatic reconnection logic.

The bridge is designed to be a reliable transport layer that the
high-level `LinkClient` can use without worrying about connection
drops or network errors.
"""

import asyncio
import logging
from typing import Callable, Coroutine, Optional

# We use the 'websockets' library for client connections.
# The try/except block ensures a helpful message if the dependency is missing.
try:
    import websockets
    from websockets.client import WebSocketClientProtocol
    from websockets.exceptions import ConnectionClosed, WebSocketException
except ImportError:
    print("Websockets library not installed. Please run 'pip install websockets'")
    raise

# Import protocol utilities and types
from link_llm.protocol import (
    LinkMessage,
    parse_message,
    serialize_message,
    ValidationError
)

# Set up a logger for this module
logger = logging.getLogger(__name__)

# Type hint for the message handling callback
# The handler must be a coroutine that accepts a LinkMessage
MessageCallback = Callable[[LinkMessage], Coroutine[None, None, None]]


class WebsocketBridge:
    """
    Manages the WebSocket connection to the LLM-Link Context Bus.

    This class provides:
    - Connection and disconnection logic.
    - Automatic reconnection with exponential backoff.
    - A `send` method to send `LinkMessage` objects.
    - A mechanism to listen for incoming messages and pass them to a handler.
    """

    # --- Configuration Constants ---
    initial_retry_delay = 1.0  # seconds
    max_retry_delay = 30.0  # seconds

    def __init__(self, uri: str, agent_id: str):
        """
        Initializes the bridge.

        Args:
            uri: The WebSocket URI of the LLM-Link Context Bus (e.g., "ws://localhost:8765").
            agent_id: The unique identifier of the agent using this bridge.
        """
        self.uri = uri
        self.agent_id = agent_id
        
        # Internal state
        self._websocket: Optional[WebSocketClientProtocol] = None
        self._is_connected: bool = False
        self._should_reconnect: bool = True
        self._listener_task: Optional[asyncio.Task] = None

    @property
    def is_connected(self) -> bool:
        """Returns True if the WebSocket connection is currently active."""
        return self._is_connected and self._websocket is not None

    def connect(self, message_handler: MessageCallback):
        """
        Starts the connection listener in the background.

        Args:
            message_handler: The coroutine function to call for every incoming LinkMessage.
        """
        if self._listener_task and not self._listener_task.done():
            logger.warning(f"[{self.agent_id}] Bridge is already running.")
            return

        self._should_reconnect = True
        self._listener_task = asyncio.create_task(
            self._connection_listener(message_handler)
        )
        logger.info(f"[{self.agent_id}] Bridge started.")

    async def disconnect(self):
        """
        Closes the WebSocket connection and stops the connection listener.
        """
        logger.info(f"[{self.agent_id}] Disconnecting bridge...")
        self._should_reconnect = False
        self._is_connected = False

        if self._websocket:
            # Attempt a clean closure
            try:
                await self._websocket.close()
            except Exception as e:
                logger.debug(f"[{self.agent_id}] Error closing socket: {e}")
            self._websocket = None

        if self._listener_task:
            # Cancel the listener task (which is likely blocked on recv or sleep)
            self._listener_task.cancel()
            try:
                # Wait for the cancellation to complete
                await self._listener_task
            except asyncio.CancelledError:
                # Expected when cancelling
                pass
            finally:
                self._listener_task = None
        
        logger.info(f"[{self.agent_id}] Bridge disconnected.")


    async def _connection_listener(self, message_handler: MessageCallback):
        """
        Main loop to maintain the connection, handle reconnections,
        and listen for incoming messages.
        """
        retry_delay = self.initial_retry_delay
        
        while self._should_reconnect:
            try:
                # 1. Attempt Connection
                logger.info(f"[{self.agent_id}] Attempting connection to {self.uri}...")
                
                # Use a new connection context manager. The `async with` handles closing
                # the websocket automatically upon exiting the block (success or failure).
                async with websockets.connect(self.uri, 
                                              logger=logger,
                                              close_timeout=5.0) as websocket:
                    
                    self._websocket = websocket
                    self._is_connected = True
                    logger.info(f"[{self.agent_id}] Connected successfully.")
                    
                    # Reset retry delay upon successful connection
                    retry_delay = self.initial_retry_delay

                    # 2. Main Listen Loop (while connected)
                    while self._is_connected and self._should_reconnect:
                        try:
                            # Wait for a message (this is the blocking call)
                            raw_message = await self._websocket.recv()
                            
                            # Deserialize and validate
                            message = parse_message(raw_message)
                            
                            # Handle message in a non-blocking task to prevent backlog
                            asyncio.create_task(message_handler(message))

                        except ConnectionClosed as e:
                            logger.warning(f"[{self.agent_id}] Connection closed gracefully. Code: {e.code}, Reason: {e.reason}")
                            break # Exit inner listening loop to trigger reconnection logic
                        except ValidationError as e:
                            logger.error(f"[{self.agent_id}] Protocol validation failed on incoming message: {e}")
                            # Keep connection alive, just log the bad message
                        except Exception as e:
                            logger.error(f"[{self.agent_id}] Unexpected error during receive: {e}", exc_info=True)
                            break # Exit inner listening loop to trigger reconnection logic

            except ConnectionRefusedError:
                logger.error(f"[{self.agent_id}] Connection refused. Retrying in {retry_delay:.1f}s...")
            except websockets.exceptions.InvalidURI as e:
                logger.error(f"[{self.agent_id}] Invalid WebSocket URI: {e}. Stopping reconnection.")
                self._should_reconnect = False # Fatal error
                break
            except asyncio.CancelledError:
                # This exception is expected when disconnect() is called
                logger.info(f"[{self.agent_id}] Connection listener task cancelled.")
                self._should_reconnect = False
                break
            except Exception as e:
                logger.error(f"[{self.agent_id}] General connection error: {e}. Retrying in {retry_delay:.1f}s...")
            
            finally:
                # Clean up connection state
                self._websocket = None 
                self._is_connected = False
                
            # --- Reconnection Logic ---
            if self._should_reconnect:
                logger.info(f"[{self.agent_id}] Waiting for {retry_delay:.1f}s before next attempt.")
                try:
                    # Wait for the delay or until cancelled by disconnect()
                    await asyncio.sleep(retry_delay)
                except asyncio.CancelledError:
                    logger.info(f"[{self.agent_id}] Retry wait cancelled.")
                    break # Exit the outer while loop
                
                # Exponential backoff
                retry_delay = min(retry_delay * 2, self.max_retry_delay)
        
        logger.info(f"[{self.agent_id}] Connection listener terminated.")


    async def send(self, message: LinkMessage) -> bool:
        """
        Sends a LinkMessage over the WebSocket connection.

        Args:
            message: The `LinkMessage` object to send.

        Returns:
            True if the message was sent successfully, False otherwise.
        """
        if not self.is_connected or not self._websocket:
            logger.error(f"[{self.agent_id}] Cannot send: Not connected.")
            return False

        try:
            # Serialize the LinkMessage into a JSON string
            json_message = serialize_message(message)
            await self._websocket.send(json_message)
            return True
        except ConnectionClosed as e:
            logger.error(f"[{self.agent_id}] Failed to send: Connection closed. {e}")
            self._is_connected = False # Trigger reconnect in the listener
        except WebSocketException as e:
            logger.error(f"[{self.agent_id}] Failed to send: WebSocket error. {e}")
        except Exception as e:
            logger.error(f"[{self.agent_id}] Failed to serialize or send: {e}")

        return False

# Define what is exposed when `from .bridge import *` is used
__all__ = ["WebsocketBridge", "MessageCallback"]
