"""
link_llm.server.bus
~~~~~~~~~~~~~~~~~~~

This module implements the `ContextBus`, the central WebSocket server that
acts as the message broker and state manager for the LLM-Link network.

It manages all agent connections, message routing, and memory interfacing,
ensuring communication adheres strictly to the LLM-Link Protocol.
"""

import asyncio
import logging
from typing import Tuple, Dict, Optional, Set

# Use 'websockets' for the server implementation
try:
    import websockets
    from websockets.server import WebSocketServerProtocol, serve
    from websockets.exceptions import ConnectionClosed
except ImportError:
    # A cleaner error handler than a simple print for production setup
    raise ImportError("Websockets library not found. Please install with: 'pip install websockets'")

from pydantic import ValidationError

# Import all necessary protocol components
from link_llm.protocol import (
    LinkMessage,
    MessageType,
    AgentRegisterPayload,
    MemoryQueryPayload,
    MemoryUpdatePayload,
    TaskRequestPayload,
    TaskResponsePayload,
    ContextSharePayload,
    ErrorPayload,
    TaskStatus,
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

# Type hint for the client registry: {agent_id: websocket}
ClientRegistry = Dict[str, WebSocketServerProtocol]


class ContextBus:
    """
    The main Context Bus server.
    Manages all agent connections, message routing, and memory interfacing.
    """

    def __init__(self, host: str, port: int, registry: MemoryRegistry):
        """
        Initializes the ContextBus instance.
        
        Args:
            host: The IP address or hostname to bind to.
            port: The port number to listen on.
            registry: The initialized MemoryRegistry instance.
        """
        self.host = host
        self.port = port
        self.registry = registry
        
        # Registry for active clients: agent_id -> websocket
        self._clients: ClientRegistry = {}
        # Lock to protect the _clients dictionary during registration/deregistration
        self._lock = asyncio.Lock()
        
        # Server task handle
        self._server_task: Optional[asyncio.Future] = None
        logger.info(f"ContextBus configured for ws://{self.host}:{self.port}")

    # --- SERVER LIFECYCLE MANAGEMENT ---

    async def start(self):
        """Starts the WebSocket server."""
        try:
            logger.info(f"Starting ContextBus server on {self.host}:{self.port}...")
            # Use 'serve' which is the main entry point for a websockets server
            self._server_task = await serve(
                self._connection_handler, 
                self.host, 
                self.port
            )
            logger.info("ContextBus is running.")
            await self._server_task.wait_closed() # Keep the main task alive until server closes
        except OSError as e:
            logger.critical(f"Failed to start server: {e}")
        except Exception as e:
            logger.critical(f"An unexpected error occurred during server start: {e}")

    async def stop(self):
        """Closes all connections and stops the WebSocket server."""
        if self._server_task:
            logger.info("Shutting down ContextBus...")
            
            # 1. Close the server listener
            self._server_task.close()
            await self._server_task.wait_closed()
            
            # 2. Close all active client connections safely
            async with self._lock:
                # Get a copy of the websockets to iterate over
                sockets_to_close = list(self._clients.values())
                
                # Use ensure_future to avoid blocking on close, set a timeout
                await asyncio.gather(
                    *(ws.close() for ws in sockets_to_close),
                    return_exceptions=True
                )
                self._clients.clear() # Clear the registry after closing attempts
            
            logger.info("ContextBus successfully shut down.")

    # --- CONNECTION HANDLING ---

    async def _connection_handler(self, websocket: WebSocketServerProtocol, path: str):
        """
        The coroutine that runs for every new WebSocket connection.
        
        It is responsible for:
        1. Receiving the initial AGENT_REGISTER message.
        2. Entering the main message listening loop.
        3. Handling disconnection and cleanup.
        """
        agent_id: Optional[str] = None
        
        try:
            # 1. Wait for AGENT_REGISTER message (blocking)
            registration_data = await websocket.recv()
            if not registration_data:
                return # Connection closed immediately
            
            # 2. Attempt to register
            register_success, agent_id = await self._handle_register(
                websocket, 
                registration_data
            )
            
            if not register_success:
                return # Registration failed, connection closed by handler
            
            logger.info(f"Agent {agent_id} connected and registered.")
            
            # 3. Main message listening loop
            await self._listen_for_messages(websocket, agent_id)

        except ConnectionClosed:
            # This is the expected way a client disconnects cleanly
            logger.info(f"Connection for {agent_id or 'unregistered agent'} closed by client.")
        except asyncio.CancelledError:
            logger.warning(f"Connection for {agent_id or 'unregistered agent'} was cancelled (server shutdown).")
        except Exception as e:
            logger.error(f"Unexpected error in connection handler for {agent_id or 'unregistered agent'}: {e}")
        finally:
            # 4. Cleanup on exit (whether success or error)
            if agent_id:
                await self._handle_disconnect(agent_id, websocket)


    async def _listen_for_messages(self, websocket: WebSocketServerProtocol, agent_id: str):
        """Infinite loop to receive and process messages."""
        async for raw_message in websocket:
            try:
                # 1. Parse JSON to LinkMessage object
                message = parse_message(raw_message)
                
                # Check sender ID consistency (Security/Integrity check)
                if message.sender_id != agent_id:
                    error_msg = f"Message sender ID '{message.sender_id}' does not match registered ID '{agent_id}'. Message rejected."
                    logger.error(error_msg)
                    await self._send_error_to_socket(websocket, error_msg, message.message_id)
                    continue

                # 2. Route the message to the appropriate handler
                await self._handle_message(message)

            except ValidationError as e:
                # Invalid LinkMessage structure (e.g., missing fields)
                error_msg = f"Protocol validation error for incoming message from {agent_id}: {e}"
                logger.error(error_msg)
                # Note: We can't always get the message_id for correlation here
                await self._send_error_to_socket(websocket, error_msg) 
            except Exception as e:
                logger.error(f"Error processing message from {agent_id}: {e}", exc_info=True)
                await self._send_error_to_socket(websocket, "Internal ContextBus error.", str(message.message_id) if 'message' in locals() else None)
                

    # --- MESSAGE ROUTING AND HANDLING ---

    async def _handle_message(self, message: LinkMessage):
        """Routes a validated LinkMessage to the correct internal handler."""
        handler_map = {
            MessageType.AGENT_REGISTER: self._handle_register, # Only used on first message, but included for completeness
            MessageType.TASK_REQUEST: self._handle_task_request,
            MessageType.TASK_RESPONSE: self._handle_task_response,
            MessageType.MEMORY_QUERY: self._handle_memory_query,
            MessageType.MEMORY_UPDATE: self._handle_memory_update,
            MessageType.CONTEXT_SHARE: self._handle_context_share,
            MessageType.STATUS_UPDATE: self._handle_status_update,
            MessageType.ERROR: self._handle_error_message, # Log and ignore incoming errors
        }

        # Select the handler or default to routing messages
        handler = handler_map.get(message.message_type)
        
        if handler == self._handle_register:
            # Log, but do nothing, as re-register is not allowed in the main loop
            logger.warning(f"Agent {message.sender_id} attempted re-registration.")
            return

        elif handler:
            # Specific payload handling (like memory operations)
            await handler(message)
        else:
            # Default: Route the message to the receiver
            await self._route_message(message)

    
    async def _handle_register(self, websocket: WebSocketServerProtocol, raw_message: str) -> Tuple[bool, Optional[str]]: # i am getting yellow underline at "Tuple"
        """Handles the initial AGENT_REGISTER message."""
        try:
            message = parse_message(raw_message)
            payload: AgentRegisterPayload = parse_payload(message, AgentRegisterPayload)
            
            agent_id = payload.agent_id
            
            async with self._lock:
                if agent_id in self._clients:
                    error_msg = f"Agent ID '{agent_id}' is already registered and active."
                    await self._send_error_to_socket(websocket, error_msg, message.message_id)
                    await websocket.close()
                    return False, None
                    
                # Store the websocket connection
                self._clients[agent_id] = websocket
                
            # Send success response
            response_payload = AgentRegisterPayload(
                agent_id=agent_id, 
                agent_type=payload.agent_type, 
                capabilities=payload.capabilities
            ) # Re-use register payload for success confirmation
            
            success_msg = create_message(
                sender_id="ContextBus",
                receiver_id=agent_id,
                message_type=MessageType.AGENT_REGISTER,
                payload=response_payload,
                correlation_id=message.message_id
            )
            await self._send_message_to_socket(websocket, success_msg)
            
            return True, agent_id

        except ValidationError as e:
            error_msg = f"Invalid AGENT_REGISTER payload received: {e}"
            logger.error(error_msg)
            await self._send_error_to_socket(websocket, error_msg)
            return False, None
        except Exception as e:
            logger.error(f"Error during registration: {e}")
            await self._send_error_to_socket(websocket, "Internal registration error.")
            return False, None


    async def _handle_task_request(self, message: LinkMessage):
        """Routes a TASK_REQUEST message to the recipient."""
        # Simple routing, as the message contains all necessary info
        await self._route_message(message)

    async def _handle_task_response(self, message: LinkMessage):
        """Routes a TASK_RESPONSE message to the original sender."""
        # Task responses are just routed back
        await self._route_message(message)

    async def _handle_context_share(self, message: LinkMessage):
        """Routes a CONTEXT_SHARE message to the recipient, or broadcasts it."""
        # Context share usually updates the recipient or is a general broadcast
        await self._route_message(message)

    async def _handle_status_update(self, message: LinkMessage):
        """Routes a STATUS_UPDATE message."""
        await self._route_message(message)
        
    async def _handle_error_message(self, message: LinkMessage):
        """Logs and ignores incoming ERROR messages."""
        logger.error(f"Received ERROR message from {message.sender_id} (Correlating to {message.correlation_id}): {message.payload.get('message', 'No error detail')}")
        # Errors received from agents are logged, not routed.

    async def _handle_memory_update(self, message: LinkMessage):
        """Updates the MemoryRegistry and sends a confirmation back."""
        try:
            payload: MemoryUpdatePayload = parse_payload(message, MemoryUpdatePayload)
            
            # IMPORTANT: Override the owner_id from the sender for security/integrity
            payload.owner_id = message.sender_id 
            
            success = await self.registry.update_memory(payload)
            
            # Send confirmation response
            response_status = TaskStatus.SUCCESS if success else TaskStatus.FAILURE
            response_message = create_message(
                sender_id="ContextBus",
                receiver_id=message.sender_id,
                message_type=MessageType.TASK_RESPONSE, # Using TASK_RESPONSE for general status
                payload=TaskResponsePayload(
                    task_id=message.message_id, # Re-using message_id as task_id
                    status=response_status,
                    result={"key": payload.key},
                    error_message=None if success else f"Failed to update memory key {payload.key}"
                ),
                correlation_id=message.message_id
            )
            await self._send_to_agent(message.sender_id, response_message)

        except ValidationError as e:
            await self._send_error_to_agent(message.sender_id, f"Invalid MEMORY_UPDATE payload: {e}", message.message_id)
        except Exception as e:
            await self._send_error_to_agent(message.sender_id, f"Bus internal error during memory update: {e}", message.message_id)

    async def _handle_memory_query(self, message: LinkMessage):
        """Queries the MemoryRegistry and sends the result back."""
        try:
            payload: MemoryQueryPayload = parse_payload(message, MemoryQueryPayload)
            
            # Query the registry
            response_payload = await self.registry.query_memory(payload.key)
            
            # Send the response back
            response_message = create_message(
                sender_id="ContextBus",
                receiver_id=message.sender_id,
                message_type=MessageType.MEMORY_RESPONSE,
                payload=response_payload,
                correlation_id=message.message_id
            )
            await self._send_to_agent(message.sender_id, response_message)

        except ValidationError as e:
            await self._send_error_to_agent(message.sender_id, f"Invalid MEMORY_QUERY payload: {e}", message.message_id)
        except Exception as e:
            await self._send_error_to_agent(message.sender_id, f"Bus internal error during memory query: {e}", message.message_id)


    # --- DISCONNECT AND CLEANUP ---

    async def _handle_disconnect(self, agent_id: str, websocket: WebSocketServerProtocol):
        """Removes the agent and cleans up their session memory."""
        async with self._lock:
            if agent_id in self._clients and self._clients[agent_id] == websocket:
                del self._clients[agent_id]
                logger.info(f"Agent {agent_id} unregistered from ContextBus.")
                
        # Cleanup memory regardless of whether they were in _clients 
        # (in case they were removed by another process)
        await self.registry.cleanup_session_memory(agent_id)


    # --- MESSAGE SENDING UTILITIES ---
    
    async def _route_message(self, message: LinkMessage):
        """
        Routes a message to the specified recipient or performs a broadcast.
        
        This is the general-purpose router used by most message handlers.
        """
        if message.receiver_id == BROADCAST_ID:
            # Broadcast to everyone except the sender
            await self._broadcast(message, exclude_sender=True)
        else:
            # Point-to-point routing
            await self._send_to_agent(message.receiver_id, message)
    
    async def _broadcast(self, message: LinkMessage, exclude_sender: bool = False):
        """Sends a LinkMessage to all registered clients."""
        async with self._lock:
            targets = [
                ws for id, ws in self._clients.items() 
                if not exclude_sender or id != message.sender_id
            ]
            
        # Use gather to send concurrently
        await asyncio.gather(
            *(self._send_message_to_socket(ws, message) for ws in targets),
            return_exceptions=True
        )
        logger.debug(f"Broadcast message {message.message_type} sent to {len(targets)} agents.")

    async def _send_to_agent(self, agent_id: str, message: LinkMessage) -> bool:
        """Looks up the agent's websocket and sends the message."""
        async with self._lock:
            websocket = self._clients.get(agent_id)
            
        if not websocket:
            error_msg = f"Cannot send message to {agent_id}: Agent is not connected or registered."
            logger.warning(error_msg)
            # Send an error back to the original sender
            await self._send_error_to_agent(message.sender_id, error_msg, message.message_id)
            return False
            
        return await self._send_message_to_socket(websocket, message)
    
    async def _send_error_to_socket(self, websocket: WebSocketServerProtocol, error_message: str, correlation_id: Optional[str] = None):
        """Sends a standardized ERROR message over a specific websocket."""
        error_payload = ErrorPayload(message=error_message)
        error_msg = create_message(
            sender_id="ContextBus",
            receiver_id="unknown_socket", # Placeholder
            message_type=MessageType.ERROR,
            payload=error_payload,
            correlation_id=correlation_id
        )
        # Attempt to send the error. Don't worry if it fails, as the connection is likely bad anyway.
        await self._send_message_to_socket(websocket, error_msg)

    async def _send_error_to_agent(self, agent_id: str, error_message: str, correlation_id: Optional[str] = None):
        """Sends a standardized ERROR message to a registered agent by ID."""
        error_payload = ErrorPayload(message=error_message)
        error_msg = create_message(
            sender_id="ContextBus",
            receiver_id=agent_id,
            message_type=MessageType.ERROR,
            payload=error_payload,
            correlation_id=correlation_id
        )
        await self._send_to_agent(agent_id, error_msg)


    async def _send_message_to_socket(self, websocket: WebSocketServerProtocol, message: LinkMessage) -> bool:
        """Safely serializes and sends a LinkMessage over a websocket."""
        if not websocket or websocket.closed:
            logger.warning(f"Attempted to send to closed websocket (agent {message.receiver_id}).")
            return False
        
        try:
            json_message = serialize_message(message)
            await websocket.send(json_message)
            return True
        except ConnectionClosed:
            logger.warning(f"Failed to send to {message.receiver_id}: Connection closed. Connection will be cleaned up.")
            # Do not trigger cleanup here; let the disconnect handler manage it.
        except Exception as e:
            logger.error(f"Error sending message to {message.receiver_id}: {e}")

        return False
