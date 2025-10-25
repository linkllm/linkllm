"""
link_llm.server.bus
~~~~~~~~~~~~~~~~~~~

This module implements the `ContextBus`, the central WebSocket server that
acts as the message broker and state manager for the LLM-Link network.
"""

import asyncio
import logging
from typing import Dict, Optional

# Use 'websockets' for the server implementation
try:
    import websockets
    from websockets.server import WebSocketServerProtocol
    from websockets.exceptions import ConnectionClosed
except ImportError:
    print("Websockets library not installed. Please run 'pip install websockets'")
    raise

from pydantic import ValidationError

# Import all necessary protocol components
from link_llm.protocol import (
    LinkMessage,
    MessageType,
    AgentRegisterPayload,
    MemoryQueryPayload,
    MemoryUpdatePayload,
    ErrorPayload,
    TaskStatus,
    TaskResponsePayload,
    MemoryPersistence,
    BROADCAST_ID,
    PROTOCOL_VERSION,
    parse_message,
    serialize_message,
    create_message,
    parse_payload
)
# Import the MemoryRegistry from its sibling file
from .registry import MemoryRegistry

logger = logging.getLogger(__name__)

# Type hint for the client registry
ClientRegistry = Dict[str, WebSocketServerProtocol]


class ContextBus:
    """
    The main Context Bus server.
    Manages all agent connections, message routing, and memory interfacing.
    """

    def __init__(self, host: str, port: int, registry: MemoryRegistry):
        self.host = host
        self.port = port
        self.registry = registry
        self._server: Optional[websockets.WebSocketServer] = None
        
        # FIX: The clients dictionary must be an instance attribute (self._clients)
        # It is the core of the routing system.
        self._clients: ClientRegistry = {}
        self._clients_lock = asyncio.Lock()
        logger.info(f"ContextBus initialized to run on {host}:{port}.")

    async def start(self):
        """Starts the WebSocket server and keeps it running."""
        logger.info(f"Starting ContextBus server on ws://{self.host}:{self.port}...")
        
        # NOTE: Removed the unnecessary local variable assignment 'self_clients = {}' here.
            
        try:
            self._server = await websockets.serve(
                self._connection_handler,
                self.host,
                self.port
            )
            logger.info("ContextBus server started successfully.")
            # Keep the server running indefinitely
            await asyncio.Future()
        except OSError as e:
            logger.error(f"Failed to start server: {e}")
        except Exception as e:
            logger.error(f"An unexpected error occurred: {e}")

    async def stop(self):
        """Stops the WebSocket server gracefully."""
        if self._server:
            self._server.close()
            await self._server.wait_closed()
            logger.info("ContextBus server stopped.")

    async def _connection_handler(self, websocket: WebSocketServerProtocol, path: str):
        """
        Handles a new client connection and its entire message lifecycle.
        
        This coroutine is created for each new agent that connects.
        """
        agent_id: Optional[str] = None
        try:
            # 1. Wait for the first message, which MUST be AGENT_REGISTER
            agent_id = await self._register_agent(websocket)
            if not agent_id:
                # _register_agent handles closing the connection
                return

            # 2. Loop and process messages from the registered agent
            async for raw_message in websocket:
                try:
                    message = parse_message(raw_message)
                    
                    # Ensure sender_id matches the registered agent
                    if message.sender_id != agent_id:
                        logger.warning(f"Sender ID mismatch for {agent_id}. "
                                     f"Got {message.sender_id}. Ignoring.")
                        continue
                        
                    # Route the message to its destination
                    await self._route_message(message)
                    
                except ValidationError as e:
                    logger.error(f"Invalid message from {agent_id}: {e}")
                    # Send an error message back to the sender
                    await self._send_error(
                        websocket,
                        f"Invalid message: {e}",
                        message.message_id if 'message' in locals() else None
                    )
                except Exception as e:
                    logger.error(f"Error processing message from {agent_id}: {e}", exc_info=True)
                    await self._send_error(
                        websocket,
                        f"Internal server error: {e}",
                        message.message_id if 'message' in locals() else None
                    )

        except ConnectionClosed as e:
            logger.info(f"Agent {agent_id or 'unknown'} disconnected: {e.code} {e.reason}")
        except Exception as e:
            logger.error(f"Unhandled error in connection handler for {agent_id}: {e}", exc_info=True)
        finally:
            # 3. Cleanup: Unregister the agent and clean session memory
            if agent_id:
                await self._unregister_agent(agent_id)

    async def _register_agent(self, websocket: WebSocketServerProtocol) -> Optional[str]:
        """
        Handles the initial AGENT_REGISTER message from a new connection.
        """
        try:
            raw_message = await asyncio.wait_for(websocket.recv(), timeout=10.0)
            message = parse_message(raw_message)

            if message.message_type != MessageType.AGENT_REGISTER:
                await self._send_error(websocket, "First message must be AGENT_REGISTER", message.message_id)
                await websocket.close(1002, "Invalid protocol: expected AGENT_REGISTER")
                return None

            payload = parse_payload(message, AgentRegisterPayload)
            
            if payload.protocol_version != PROTOCOL_VERSION:
                await self._send_error(websocket, f"Protocol version mismatch. Server: {PROTOCOL_VERSION}, Client: {payload.protocol_version}", message.message_id)
                await websocket.close(1002, "Protocol mismatch")
                return None
                
            agent_id = payload.agent_id

            async with self._clients_lock:
                # FIX: Use the instance attribute self._clients
                if agent_id in self._clients: 
                    logger.warning(f"Agent {agent_id} tried to connect but is already registered. Closing new connection.")
                    await self._send_error(websocket, f"Agent ID '{agent_id}' already registered.", message.message_id)
                    await websocket.close(1008, "Agent ID already registered")
                    return None
                
                # Add to registry
                # FIX: Use the instance attribute self._clients
                self._clients[agent_id] = websocket 
                
            logger.info(f"Agent registered: {agent_id} from {websocket.remote_address}")
            
            # Send acknowledgement (e.g., a TASK_RESPONSE to the implicit register task)
            ack_response = TaskResponsePayload(
                task_id=message.message_id, # Correlate to the register message
                status=TaskStatus.SUCCESS,
                result={"message": f"Agent {agent_id} registered successfully."}
            )
            ack_msg = create_message(
                sender_id="ContextBus",
                receiver_id=agent_id,
                message_type=MessageType.TASK_RESPONSE,
                payload=ack_response,
                correlation_id=message.message_id
            )
            await self._send_message_to_socket(websocket, ack_msg)
            return agent_id

        except asyncio.TimeoutError:
            logger.warning("Agent registration timed out. Closing connection.")
            await websocket.close(1002, "Registration timeout")
        except ValidationError as e:
            logger.error(f"Invalid AGENT_REGISTER message: {e}")
            await self._send_error(websocket, f"Invalid registration payload: {e}")
            await websocket.close(1002, "Invalid registration payload")
        except Exception as e:
            logger.error(f"Error during agent registration: {e}", exc_info=True)
            await websocket.close(1011, "Server error during registration")
        
        return None

    async def _unregister_agent(self, agent_id: str):
        """Removes an agent from the registry and cleans up its session memory."""
        async with self._clients_lock:
            # FIX: Use the instance attribute self._clients
            if agent_id in self._clients:
                del self._clients[agent_id]
                logger.info(f"Agent unregistered: {agent_id}")
            
        # Clean up session-level memory
        cleaned_count = await self.registry.cleanup_session_memory(agent_id)
        if cleaned_count > 0:
            logger.info(f"Cleaned up {cleaned_count} session memory entries for {agent_id}.")

    async def _route_message(self, message: LinkMessage):
        """
        The core routing logic. Determines where a message should go
        based on its type and receiver_id.
        """
        msg_type = message.message_type
        
        if msg_type in (MessageType.TASK_REQUEST, MessageType.TASK_RESPONSE, MessageType.CONTEXT_SHARE):
            # Direct peer-to-peer or broadcast routing
            if message.receiver_id == BROADCAST_ID:
                await self._broadcast(message)
            else:
                await self._send_to_agent(message.receiver_id, message)
                
        elif msg_type == MessageType.MEMORY_UPDATE:
            # Handle memory storage
            await self._handle_memory_update(message)
            
        elif msg_type == MessageType.MEMORY_QUERY:
            # Handle memory retrieval
            await self._handle_memory_query(message)
            
        elif msg_type == MessageType.AGENT_REGISTER:
            # This should have been handled already, but we'll ignore it
            logger.warning(f"Received unexpected AGENT_REGISTER from {message.sender_id}")
            
        else:
            # Unhandled message type
            logger.error(f"Unhandled message type: {msg_type} from {message.sender_id}")
            await self._send_error_to_agent(
                agent_id=message.sender_id,
                error_message=f"Server does not handle message type: {msg_type}",
                correlation_id=message.message_id
            )

    async def _send_to_agent(self, agent_id: str, message: LinkMessage):
        """Sends a LinkMessage to a specific registered agent."""
        async with self._clients_lock:
            # FIX: Use the instance attribute self._clients
            websocket = self._clients.get(agent_id)
            
        if websocket:
            await self._send_message_to_socket(websocket, message)
        else:
            logger.warning(f"Agent {agent_id} not found. Message from {message.sender_id} dropped.")
            # Send an error back to the original sender
            await self._send_error_to_agent(
                agent_id=message.sender_id,
                error_message=f"Receiver agent '{agent_id}' is not connected.",
                correlation_id=message.message_id
            )

    async def _broadcast(self, message: LinkMessage):
        """Broadcasts a message to all connected agents except the sender."""
        logger.info(f"Broadcasting message from {message.sender_id}")
        async with self._clients_lock:
            # Create a list of coroutines to run concurrently
            tasks = [
                self._send_message_to_socket(ws, message)
                # FIX: Use the instance attribute self._clients.items()
                for client_id, ws in self._clients.items()
                if client_id != message.sender_id # Don't send back to sender
            ]
            await asyncio.gather(*tasks)

    async def _handle_memory_update(self, message: LinkMessage):
        """Processes a MEMORY_UPDATE message and stores it in the registry."""
        try:
            payload = parse_payload(message, MemoryUpdatePayload)
            # Add sender_id to payload so registry can track session memory
            payload.owner_id = message.sender_id
            
            await self.registry.update_memory(payload)
            logger.debug(f"Memory updated by {message.sender_id} for key {payload.key}")
        except ValidationError as e:
            logger.error(f"Invalid MemoryUpdatePayload from {message.sender_id}: {e}")
            await self._send_error_to_agent(message.sender_id, f"Invalid payload: {e}", message.message_id)
        except Exception as e:
            logger.error(f"Error updating memory: {e}", exc_info=True)
            await self._send_error_to_agent(message.sender_id, "Internal server error", message.message_id)

    async def _handle_memory_query(self, message: LinkMessage):
        """Processes a MEMORY_QUERY message, retrieves data, and sends a response."""
        response_payload = None
        try:
            payload = parse_payload(message, MemoryQueryPayload)
            response_payload = await self.registry.query_memory(payload.key)
            
        except ValidationError as e:
            logger.error(f"Invalid MemoryQueryPayload from {message.sender_id}: {e}")
            await self._send_error_to_agent(message.sender_id, f"Invalid payload: {e}", message.message_id)
            return
        except Exception as e:
            logger.error(f"Error querying memory: {e}", exc_info=True)
            await self._send_error_to_agent(message.sender_id, "Internal server error", message.message_id)
            return

        # Send the memory response back to the original querier
        response_message = create_message(
            sender_id="ContextBus",
            receiver_id=message.sender_id,
            message_type=MessageType.MEMORY_RESPONSE,
            payload=response_payload,
            correlation_id=message.message_id
        )
        await self._send_to_agent(message.sender_id, response_message)

    async def _send_error(self, websocket: WebSocketServerProtocol, error_message: str, correlation_id: Optional[str] = None):
        """Sends a standardized ERROR message over a raw websocket."""
        error_payload = ErrorPayload(error_message=error_message)
        error_msg = create_message(
            sender_id="ContextBus",
            receiver_id=None, # Receiver is unknown or not yet registered
            message_type=MessageType.ERROR,
            payload=error_payload,
            correlation_id=correlation_id
        )
        await self._send_message_to_socket(websocket, error_msg)

    async def _send_error_to_agent(self, agent_id: str, error_message: str, correlation_id: Optional[str] = None):
        """Sends a standardized ERROR message to a registered agent by ID."""
        error_payload = ErrorPayload(error_message=error_message)
        error_msg = create_message(
            sender_id="ContextBus",
            receiver_id=agent_id,
            message_type=MessageType.ERROR,
            payload=error_payload,
            correlation_id=correlation_id
        )
        await self._send_to_agent(agent_id, error_msg)

    async def _send_message_to_socket(self, websocket: WebSocketServerProtocol, message: LinkMessage):
        """Safely serializes and sends a LinkMessage over a websocket."""
        if not websocket or websocket.closed:
            logger.warning(f"Attempted to send to closed websocket (agent {message.receiver_id}).")
            return
        
        try:
            json_message = serialize_message(message)
            await websocket.send(json_message)
        except ConnectionClosed:
            logger.warning(f"Failed to send to {message.receiver_id}: Connection closed.")
        except Exception as e:
            logger.error(f"Error sending message to {message.receiver_id}: {e}", exc_info=True)

__all__ = ["ContextBus"]