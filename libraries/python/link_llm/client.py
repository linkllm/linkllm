"""
link_llm.client
~~~~~~~~~~~~~~~

This module implements the high-level `LinkClient`, which acts as the
primary interface for an AI agent to interact with the LLM-Link Context Bus.

It utilizes the `WebsocketBridge` for reliable transport and provides
developer-friendly methods for:
1. Connecting and automatically registering the agent.
2. Handling incoming protocol messages (tasks, context queries, etc.)
   using a decorator-based routing system.
3. Sending outbound protocol messages (task requests, memory updates).
"""

import asyncio
import logging
from typing import Callable, Any, Dict, List, Optional, Type, Coroutine

from pydantic import ValidationError

from .bridge import WebsocketBridge, MessageCallback
from .protocol import (
    PROTOCOL_VERSION, 
    LinkMessage,
    MessageType,
    TaskRequestPayload,
    TaskResponsePayload,
    ContextSharePayload,
    MemoryUpdatePayload,
    MemoryQueryPayload,
    MemoryResponsePayload,
    AgentRegisterPayload,
    ErrorPayload,
    BasePayload,
    TaskStatus,
    MemoryPersistence,
    BROADCAST_ID,
    create_message,
    parse_payload
)

logger = logging.getLogger(__name__)

# Type for Task Handler: A coroutine that accepts a payload and returns a result
TaskHandler = Callable[[BasePayload], Coroutine[None, None, Optional[Dict[str, Any]]]]


class LinkClient:
    """
    The main client for an LLM-Link agent.

    Provides a simple, async interface for multi-agent communication.
    """
    
    def __init__(self, agent_id: str, capabilities: List[str] = None):
        """
        Initializes the LinkClient.

        Args:
            agent_id: A unique identifier for this agent instance.
            capabilities: A list of tasks this agent is capable of handling
                          (e.g., ["summarize", "research_query"]).
        """
        if not agent_id:
            raise ValueError("agent_id cannot be empty.")
            
        self.agent_id: str = agent_id
        self.capabilities: List[str] = capabilities or []
        self._handlers: Dict[MessageType, TaskHandler] = {}
        self._task_handlers: Dict[str, TaskHandler] = {}
        self._response_futures: Dict[str, asyncio.Future] = {}
        self._bridge: Optional[WebsocketBridge] = None

    @property
    def is_connected(self) -> bool:
        """Checks if the client is currently connected to the Context Bus."""
        return self._bridge.is_connected if self._bridge else False

    # --- Connection Management ---

    async def connect(self, uri: str = "ws://localhost:8765") -> None:
        """
        Connects the client to the LLM-Link Context Bus.

        Args:
            uri: The WebSocket URI of the Context Bus.
        """
        logger.info(f"[{self.agent_id}] Initializing connection to {uri}")
        
        # Initialize the bridge, passing our message_router as the callback
        self._bridge = WebsocketBridge(
            agent_id=self.agent_id,
            uri=uri,
            on_message=self._message_router,
            on_connect=self._on_connect_hook
        )
        await self._bridge.connect()

    async def disconnect(self) -> None:
        """Closes the connection to the Context Bus."""
        if self._bridge:
            await self._bridge.disconnect()
            
    async def _on_connect_hook(self) -> None:
        """Called by the bridge upon successful connection/reconnection."""
        logger.info(f"[{self.agent_id}] Connection hook triggered. Registering agent.")
        await self.register_agent()

    # --- Handler Registration (Developer API) ---

    def on_message(self, message_type: MessageType) -> Callable[[TaskHandler], TaskHandler]:
        """
        Decorator to register a handler function for a specific LinkMessage type.
        
        Example:
            @client.on_message(MessageType.CONTEXT_QUERY)
            async def handle_query(payload: ContextQueryPayload):
                ...
        """
        def decorator(func: TaskHandler) -> TaskHandler:
            self._handlers[message_type] = func
            logger.debug(f"[{self.agent_id}] Registered general handler for {message_type.value}")
            return func
        return decorator

    def on_task(self, task_name: str) -> Callable[[TaskHandler], TaskHandler]:
        """
        Decorator to register a handler function for a specific TaskRequest name.
        
        Example:
            @client.on_task("summarize_document")
            async def handle_summary(payload: TaskRequestPayload):
                ...
        """
        def decorator(func: TaskHandler) -> TaskHandler:
            self._task_handlers[task_name] = func
            logger.debug(f"[{self.agent_id}] Registered task handler for '{task_name}'")
            return func
        return decorator

    # --- Outbound Messaging (Client API) ---

    async def register_agent(self) -> bool:
        """Sends the AGENT_REGISTER message to the Context Bus."""
        payload = AgentRegisterPayload(
            agent_id=self.agent_id,
            capabilities=list(self._task_handlers.keys()),
            metadata={"sdk_version": PROTOCOL_VERSION}  #yellow underline "PROTOCOL_VERSION" is not defined
        )
        message = create_message(
            sender_id=self.agent_id,
            message_type=MessageType.AGENT_REGISTER,
            payload=payload,
            receiver_id=BROADCAST_ID # Typically sent to the Bus
        )
        return await self._send(message)

    async def request_task(
        self,
        receiver_id: str,
        task_name: str,
        content: Any,
        context: Optional[Dict[str, Any]] = None,
        timeout: Optional[float] = None
    ) -> TaskResponsePayload:
        """
        Requests another agent to perform a specific task.
        
        Args:
            receiver_id: The ID of the agent to send the request to.
            task_name: The name of the task (must match a capability).
            content: The primary data/content for the task.
            context: Additional supporting context.
            timeout: If set, waits for the response for this duration.

        Returns:
            The TaskResponsePayload from the receiving agent.
            
        Raises:
            asyncio.TimeoutError: If the response is not received in time.
        """
        payload = TaskRequestPayload(
            task_name=task_name,
            content=content,
            context=context or {}
        )
        
        request_message = create_message(
            sender_id=self.agent_id,
            receiver_id=receiver_id,
            message_type=MessageType.TASK_REQUEST,
            payload=payload
        )
        
        # Create a Future to wait for the corresponding TASK_RESPONSE
        response_future = asyncio.get_event_loop().create_future()
        self._response_futures[request_message.message_id] = response_future
        
        await self._send(request_message)

        try:
            response_payload: TaskResponsePayload = await asyncio.wait_for(
                response_future, timeout=timeout
            )
            return response_payload
        finally:
            # Clean up the future whether it succeeded or timed out
            self._response_futures.pop(request_message.message_id, None)

    async def share_context(self, receiver_id: str, data: Dict[str, Any]) -> bool:
        """Sends a piece of contextual state to another agent."""
        payload = ContextSharePayload(data=data)
        message = create_message(
            sender_id=self.agent_id,
            receiver_id=receiver_id,
            message_type=MessageType.CONTEXT_SHARE,
            payload=payload
        )
        return await self._send(message)

    async def update_memory(self, key: str, data: Any, persistence: MemoryPersistence = MemoryPersistence.ETERNAL) -> bool:
        """Requests the Memory Registry (via the Bus) to store information."""
        payload = MemoryUpdatePayload(
            key=key,
            data=data,
            persistence=persistence
        )
        message = create_message(
            sender_id=self.agent_id,
            receiver_id=BROADCAST_ID, # Typically handled by the Bus/Registry
            message_type=MessageType.MEMORY_UPDATE,
            payload=payload
        )
        return await self._send(message)
    
    # --- Internal Message Processing ---

    async def _send(self, message: LinkMessage) -> bool:
        """Internal helper to serialize and send a message via the bridge."""
        if not self.is_connected:
            logger.warning(f"[{self.agent_id}] Tried to send message but not connected.")
            return False
            
        logger.debug(f"[{self.agent_id}] Sending {message.message_type.value} to {message.receiver_id}")
        return await self._bridge.send(message)

    async def _message_router(self, message: LinkMessage) -> None:
        """
        The central handler for all incoming messages from the Context Bus.
        Passed directly to the WebsocketBridge.
        """
        
        # 1. Handle Response Futures (Correlate task responses with requests)
        if message.correlation_id in self._response_futures:
            future = self._response_futures.get(message.correlation_id)
            if future and not future.done():
                try:
                    # Expecting a TaskResponsePayload or similar for correlation
                    payload = parse_payload(message, TaskResponsePayload)
                    future.set_result(payload)
                except (ValidationError, KeyError) as e:
                    logger.error(f"[{self.agent_id}] Correlated message {message.correlation_id} had invalid payload: {e}")
                    future.set_exception(e)
            return
            
        # 2. Handle Task Requests (Primary agent work)
        if message.message_type == MessageType.TASK_REQUEST:
            await self._handle_task_request(message)
            return
            
        # 3. Handle General Message Types (Registered by user via @on_message)
        handler = self._handlers.get(message.message_type)
        if handler:
            # Determine the expected payload type for dynamic parsing
            payload_map: Dict[MessageType, Type[BasePayload]] = {
                MessageType.TASK_RESPONSE: TaskResponsePayload,
                MessageType.CONTEXT_SHARE: ContextSharePayload,
                MessageType.MEMORY_RESPONSE: MemoryResponsePayload,
                MessageType.ERROR: ErrorPayload,
                # AGENT_REGISTER is usually outbound, but useful for discovery
                MessageType.AGENT_REGISTER: AgentRegisterPayload
            }
            
            payload_type = payload_map.get(message.message_type, BasePayload)
            
            try:
                # Parse payload and call the general handler
                payload = parse_payload(message, payload_type)
                await handler(payload)
            except ValidationError as e:
                logger.error(f"[{self.agent_id}] Failed to validate payload for {message.message_type}: {e}")
            except Exception as e:
                logger.error(f"[{self.agent_id}] Unhandled error in {message.message_type} handler: {e}")
        else:
            logger.warning(f"[{self.agent_id}] No handler registered for incoming message type: {message.message_type.value}")

    async def _handle_task_request(self, message: LinkMessage) -> None:
        """Processes an incoming TASK_REQUEST, executes the handler, and sends a response."""
        task_response = None
        
        try:
            # 1. Parse the Task Request
            request_payload = parse_payload(message, TaskRequestPayload)
            task_name = request_payload.task_name
            
            handler = self._task_handlers.get(task_name)
            if not handler:
                raise NotImplementedError(f"Task handler not registered for '{task_name}'")
                
            # 2. Execute the Task Handler (Sends status PENDING first if possible)
            logger.info(f"[{self.agent_id}] Executing task: {task_name} (ID: {request_payload.task_id})")

            # A simple synchronous way to send PENDING status is challenging in this setup
            # We'll just execute and send the final SUCCESS/FAILURE
            
            result = await handler(request_payload) # Execute the user's task logic
            
            # 3. Create Success Response
            task_response = TaskResponsePayload(
                task_id=request_payload.task_id,
                status=TaskStatus.SUCCESS,
                result=result
            )

        except NotImplementedError as e:
            logger.warning(f"[{self.agent_id}] Task not implemented: {e}")
            task_response = TaskResponsePayload(
                task_id=request_payload.task_id if 'request_payload' in locals() else 'unknown',
                status=TaskStatus.FAILURE,
                error_message=f"Task not implemented by this agent: {e}"
            )
        except Exception as e:
            logger.error(f"[{self.agent_id}] Error while processing task: {e}", exc_info=True)
            # 4. Create Failure Response
            task_response = TaskResponsePayload(
                task_id=request_payload.task_id if 'request_payload' in locals() else 'unknown',
                status=TaskStatus.FAILURE,
                error_message=f"Internal agent error: {e}"
            )
        finally:
            if task_response:
                response_message = create_message(
                    sender_id=self.agent_id,
                    receiver_id=message.sender_id, # Send back to the original sender
                    message_type=MessageType.TASK_RESPONSE,
                    payload=task_response,
                    correlation_id=message.message_id # Correlate with original request
                )
                await self._send(response_message)
            
# Update __init__.py with the new components
# The user will do this implicitly when we give them the code for the next step.

__all__ = ["LinkClient"]
