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
import json
import logging
from typing import AsyncGenerator, Callable, Coroutine, Optional

# We will use the 'websockets' library for client connections
try:
    import websockets
    from websockets.client import WebSocketClientProtocol
    from websockets.exceptions import ConnectionClosed, WebSocketException
except ImportError:
    print("Websockets library not installed. Please run 'pip install websockets'")
    raise

from .protocol import (
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
    - A `send` method to send `LinkMessage` objects (as JSON).
    - A `listen` method that continuously receives messages and
      passes them to a registered callback.
    """

    def __init__(
        self,
        agent_id: str,
        uri: str,
        on_message: MessageCallback,
        on_connect: Optional[Callable[[], Coroutine[None, None, None]]] = None,
        max_retry_delay: int = 60,
        initial_retry_delay: int = 1,
    ):
        """
        Initializes the WebsocketBridge.

        Args:
            agent_id: The ID of the agent this bridge serves. Used for logging.
            uri: The WebSocket URI of the Context Bus (e.g., "ws://localhost:8765").
            on_message: An async function (coroutine) that will be called
                        with each `LinkMessage` received from the server.
            on_connect: An optional async function (coroutine) to be called
                        upon a successful (re)connection.
            max_retry_delay: The maximum delay (in seconds) between reconn attempts.
            initial_retry_delay: The initial delay (in seconds) for reconn.
        """
        self.agent_id = agent_id
        self.uri = uri
        self.on_message_callback = on_message
        self.on_connect_callback = on_connect
        
        self.max_retry_delay = max_retry_delay
        self.initial_retry_delay = initial_retry_delay
        
        self._websocket: Optional[WebSocketClientProtocol] = None
        self._is_connected: bool = False
        self._listen_task: Optional[asyncio.Task] = None
        self._stop_event: asyncio.Event = asyncio.Event()

    @property
    def is_connected(self) -> bool:
        """Returns True if the bridge is currently connected, False otherwise."""
        return self._is_connected

    async def connect(self) -> None:
        """
        Creates and starts the persistent listening task.

        If the task is already running, this does nothing.
        """
        if self._listen_task and not self._listen_task.done():
            logger.warning(f"[{self.agent_id}] Already connected or connecting.")
            return

        self._stop_event.clear()
        self._listen_task = asyncio.create_task(
            self._connection_listener(),
            name=f"llm-link-bridge-{self.agent_id}"
        )
        logger.info(f"[{self.agent_id}] Connection listener started.")

    async def disconnect(self) -> None:
        """
        Signals the connection listener to stop and closes the connection.
        """
        if not self._listen_task or self._stop_event.is_set():
            logger.info(f"[{self.agent_id}] Already disconnected.")
            return

        logger.info(f"[{self.agent_id}] Disconnecting...")
        self._stop_event.set()  # Signal the listener to stop
        
        if self._websocket and self._is_connected:
            try:
                await self._websocket.close(code=1000, reason="Client shutting down")
            except WebSocketException as e:
                logger.warning(f"[{self.agent_id}] Error during close: {e}")

        try:
            # Wait for the listener task to fully stop
            await asyncio.wait_for(self._listen_task, timeout=5.0)
        except asyncio.TimeoutError:
            logger.warning(f"[{self.agent_id}] Listener task did not stop gracefully.")
            self._listen_task.cancel()
            
        self._listen_task = None
        self._is_connected = False
        logger.info(f"[{self.agent_id}] Disconnected.")

    async def _connection_listener(self) -> None:
        """
        The main loop that handles connection, reception, and reconnection.
        """
        retry_delay = self.initial_retry_delay

        while not self._stop_event.is_set():
            try:
                # --- 1. Attempt Connection ---
                logger.info(f"[{self.agent_id}] Connecting to {self.uri}...")
                async with websockets.connect(self.uri) as ws:
                    self._websocket = ws
                    self._is_connected = True
                    logger.info(f"[{self.agent_id}] Connection established.")
                    
                    # Reset retry delay on successful connection
                    retry_delay = self.initial_retry_delay

                    # --- 2. Run Connection Hook ---
                    if self.on_connect_callback:
                        try:
                            await self.on_connect_callback()
                        except Exception as e:
                            logger.error(f"[{self.agent_id}] Error in on_connect hook: {e}")

                    # --- 3. Listen for Messages ---
                    # This loop runs as long as the connection is open
                    async for raw_message in self._websocket:
                        if self._stop_event.is_set():
                            break  # Exit if disconnect was called
                        
                        try:
                            message = parse_message(raw_message)
                            # Pass the valid message to the client's handler
                            await self.on_message_callback(message)
                            
                        except (ValidationError, json.JSONDecodeError) as e:
                            logger.error(f"[{self.agent_id}] Failed to parse message: {e}")
                            logger.debug(f"[{self.agent_id}] Raw invalid message: {raw_message}")
                        except Exception as e:
                            logger.error(f"[{self.agent_id}] Error in on_message callback: {e}")

            except (ConnectionClosed, WebSocketException, OSError) as e:
                logger.warning(f"[{self.agent_id}] Connection lost: {type(e).__name__} {e}")
            
            except Exception as e:
                logger.error(f"[{self.agent_id}] Unhandled bridge error: {e}", exc_info=True)
                
            finally:
                # --- START: MODIFIED SECTION ---
                # The finally block ONLY does cleanup. It does not control the loop.
                self._is_connected = False
                self._websocket = None
                # --- END: MODIFIED SECTION ---
            
            # --- START: NEW SECTION ---
            # The loop control and reconnection logic is now OUTSIDE the finally block.
            if self._stop_event.is_set():
                logger.info(f"[{self.agent_id}] Listener loop stopping as requested.")
                break # This break is now valid.

            # Reconnection Logic
            logger.info(f"[{self.agent_id}] Reconnecting in {retry_delay}s...")
            try:
                # Wait for the retry delay, but check stop_event every second
                for _ in range(retry_delay):
                    if self._stop_event.is_set():
                        break
                    await asyncio.sleep(1)
            except asyncio.CancelledError:
                logger.info(f"[{self.agent_id}] Reconnect wait cancelled.")
                break # This break is also valid.
            
            # Exponential backoff
            retry_delay = min(retry_delay * 2, self.max_retry_delay)
            # --- END: NEW SECTION ---
        
        logger.info(f"[{self.agent_id}] Connection listener terminated.")


    async def send(self, message: LinkMessage) -> bool:
        """
        Sends a LinkMessage over the WebSocket connection.

        Args:
            message: The `LinkMessage` object to send.

        Returns:
            True if the message was sent successfully, False otherwise.
        """
        if not self._is_connected or not self._websocket:
            logger.error(f"[{self.agent_id}] Cannot send: Not connected.")
            return False

        try:
            json_message = serialize_message(message)
            await self._websocket.send(json_message)
            return True
        except ConnectionClosed as e:
            logger.error(f"[{self.agent_id}] Failed to send: Connection closed. {e}")
            self._is_connected = False # Trigger reconnect
        except WebSocketException as e:
            logger.error(f"[{self.agent_id}] Failed to send: WebSocket error. {e}")
        except Exception as e:
            logger.error(f"[{self.agent_id}] Failed to serialize or send: {e}")

        return False
# Define what is exposed when `from .bridge import *` is used
__all__ = ["WebsocketBridge", "MessageCallback"]