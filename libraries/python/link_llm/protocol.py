"""
link_llm.protocol
~~~~~~~~~~~~~~~~~

This module defines the core LLM-Link Protocol Specification (v0.1).

It establishes the data structures and message types that all components
in the LLM-Link ecosystem (Agents, Context Bus, Memory Registry)
use to communicate.

The protocol is built on Pydantic `BaseModel` classes, which provide
robust data validation, type enforcement, and easy JSON serialization/
deserialization.

Core Concepts:
1.  **LinkMessage (Envelope):** The main wrapper for all communication.
    It contains metadata about the message, such as sender, receiver,
    message ID, and the `message_type`.
2.  **Payloads (Content):** A set of Pydantic models, one for each
    `message_type`. The `payload` field of a `LinkMessage` will
    contain a dictionary that can be validated against one of these
    payload models.
3.  **Enums (Controlled Vocabularies):** Enums are used for message
    types, task statuses, etc., to prevent errors from typos and
    ensure all components agree on terminology.
"""

import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Type, TypeVar

# Pydantic is used for robust data validation and serialization
try:
    from pydantic import BaseModel, Field, ValidationError
except ImportError:
    print("Pydantic not installed. Please run 'pip install pydantic'")
    raise

# --- Constants ---

BROADCAST_ID = "*"
"""A special receiver_id to send a message to all connected agents."""

PROTOCOL_VERSION = "0.1.0"
"""The current version of the LLM-Link protocol."""

# --- Controlled Vocabularies (Enums) ---

class MessageType(str, Enum):
    """
    Defines the type of action or information being sent.
    The `message_type` field in `LinkMessage` determines which
    Payload model should be used to interpret the `payload` field.
    """
    # Agent Lifecycle
    AGENT_REGISTER = "AGENT_REGISTER"
    AGENT_DEREGISTER = "AGENT_DEREGISTER"
    AGENT_HEARTBEAT = "AGENT_HEARTBEAT"

    # Task Execution
    TASK_REQUEST = "TASK_REQUEST"
    TASK_RESPONSE = "TASK_RESPONSE"
    
    # Context & State
    CONTEXT_SHARE = "CONTEXT_SHARE"
    CONTEXT_QUERY = "CONTEXT_QUERY"
    CONTEXT_RESPONSE = "CONTEXT_RESPONSE"
    
    # Persistent Memory
    MEMORY_UPDATE = "MEMORY_UPDATE"
    MEMORY_QUERY = "MEMORY_QUERY"
    MEMORY_RESPONSE = "MEMORY_RESPONSE"

    # General
    ERROR = "ERROR"

class TaskStatus(str, Enum):
    """Indicates the status of a long-running or asynchronous task."""
    PENDING = "PENDING"
    IN_PROGRESS = "IN_PROGRESS"
    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"

class MemoryPersistence(str, Enum):
    """Defines the persistence level for a memory update."""
    SESSION = "SESSION"    # Lives only for the duration of the bus
    ETERNAL = "ETERNAL"  # Persisted to the Memory Registry (Redis/SQLite)

# --- Payload Models ---

# Base class for type hinting. Not strictly necessary but good practice.
class BasePayload(BaseModel):
    """Base class for all message payloads."""
    pass

class AgentRegisterPayload(BasePayload):
    """
    Payload for AGENT_REGISTER.
    Sent by an agent to the Context Bus to announce its presence.
    """
    agent_id: str = Field(..., description="The unique ID of the agent.")
    capabilities: List[str] = Field(
        default_factory=list,
        description="A list of tasks or functions this agent can perform."
    )
    metadata: Dict[str, Any] = Field(
        default_factory=dict,
        description="Optional metadata about the agent (e.g., model, version)."
    )

class TaskRequestPayload(BasePayload):
    """
    Payload for TASK_REQUEST.
    Sent by one agent to another to request a task be performed.
    (Corresponds to the example in the documentation)
    """
    task_id: str = Field(
        default_factory=lambda: str(uuid.uuid4()),
        description="Unique ID for this specific task instance."
    )
    task_name: str = Field(
        ...,
        description="The name of the task to be performed (e.g., 'summarize_research_paper')."
    )
    content: Any = Field(
        ...,
        description="The primary content/data for the task (e.g., text, URL, complex object)."
    )
    context: Dict[str, Any] = Field(
        default_factory=dict,
        description="Supporting context for the task (e.g., {'source': 'https://...'})"
    )

class TaskResponsePayload(BasePayload):
    """
    Payload for TASK_RESPONSE.
    Sent by a worker agent back to the requesting agent.
    """
    task_id: str = Field(
        ...,
        description="The `task_id` from the corresponding TaskRequestPayload."
    )
    status: TaskStatus = Field(
        ...,
        description="The final or intermediate status of the task."
    )
    result: Optional[Any] = Field(
        default=None,
        description="The output/result of the task if `status` is SUCCESS."
    )
    error_message: Optional[str] = Field(
        default=None,
        description="An error message if `status` is FAILURE."
    )

class ContextSharePayload(BasePayload):
    """
    Payload for CONTEXT_SHARE.
    Used to share a piece of information (state, result) with
    another agent or the bus.
    """
    context_id: str = Field(
        default_factory=lambda: str(uuid.uuid4()),
        description="A unique ID for this piece of context."
    )
    data: Dict[str, Any] = Field(
        ...,
        description="The context data being shared."
    )
    metadata: Dict[str, Any] = Field(
        default_factory=dict,
        description="Metadata about the context (e.g., source_agent_id, creation_time)."
    )

class MemoryUpdatePayload(BasePayload):
    """
    Payload for MEMORY_UPDATE.
    Requests the Memory Registry to save or update a piece of information.
    """
    key: str = Field(..., description="The key for the memory entry (e.g., 'project_x_summary').")
    data: Any = Field(..., description="The data to be stored.")
    persistence: MemoryPersistence = Field(
        default=MemoryPersistence.SESSION,
        description="The desired persistence level (session or eternal)."
    )

class MemoryQueryPayload(BasePayload):
    """
    Payload for MEMORY_QUERY.
    Requests a piece of information from the Memory Registry.
    """
    key: str = Field(..., description="The key of the memory to retrieve.")

class MemoryResponsePayload(BasePayload):
    """
    Payload for MEMORY_RESPONSE.
    The response from the Memory Registry for a MEMORY_QUERY.
    """
    key: str = Field(..., description="The key from the original query.")
    found: bool = Field(..., description="Whether the memory entry was found.")
    data: Optional[Any] = Field(
        default=None,
        description="The retrieved data, if found."
    )

class ErrorPayload(BasePayload):
    """
    Payload for ERROR.
    Sent when a message could not be processed or an error occurred.
    """
    code: str = Field(..., description="An internal error code (e.g., '404_NOT_FOUND').")
    message: str = Field(..., description="A human-readable error message.")
    details: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Optional dictionary with more error details."
    )

# --- Main Protocol Message (Envelope) ---

class LinkMessage(BaseModel):
    """
    The main envelope for all LLM-Link communication.

    This structure is serialized to JSON and sent over the wire
    (e.g., via WebSocket).
    """
    message_id: str = Field(
        default_factory=lambda: str(uuid.uuid4()),
        description="Unique identifier for this specific message.",
        alias="messageId"
    )
    timestamp: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="UTC timestamp when the message was created."
    )
    sender_id: str = Field(
        ...,
        description="The ID of the agent sending the message.",
        alias="senderId"
    )
    receiver_id: str = Field(
        default=BROADCAST_ID,
        description="The ID of the intended recipient. '*' means broadcast.",
        alias="receiverId"
    )
    message_type: MessageType = Field(
        ...,
        description="The type of message, determining the payload structure.",
        alias="messageType"
    )
    payload: Dict[str, Any] = Field(
        ...,
        description="The content of the message. Must be parsed based on `message_type`."
    )
    correlation_id: Optional[str] = Field(
        default=None,
        description="Used to correlate a response with a request (e.g., set to the `message_id` of the request).",
        alias="correlationId"
    )
    version: str = Field(
        default=PROTOCOL_VERSION,
        description="The protocol version this message adheres to."
    )

    class Config:
        """Pydantic model configuration."""
        # Use aliases for camelCase JSON compatibility
        populate_by_name = True
        # Allow extra fields to be ignored during parsing
        extra = 'ignore'

# --- Helper Functions ---

PayloadType = TypeVar('PayloadType', bound=BasePayload)

def create_message(
    sender_id: str,
    message_type: MessageType,
    payload: BasePayload,
    receiver_id: str = BROADCAST_ID,
    correlation_id: Optional[str] = None
) -> LinkMessage:
    """
    Factory function to easily create a new LinkMessage.

    Args:
        sender_id: The ID of the agent sending the message.
        message_type: The `MessageType` enum.
        payload: A Pydantic model instance (e.g., `TaskRequestPayload`).
                 This function will automatically serialize it.
        receiver_id: The recipient agent ID. Defaults to BROADCAST.
        correlation_id: The ID of a message this one is in response to.

    Returns:
        A fully-formed `LinkMessage` instance.
    """
    return LinkMessage(
        sender_id=sender_id,
        receiver_id=receiver_id,
        message_type=message_type,
        payload=payload.model_dump(),  # Serialize the payload model to a dict
        correlation_id=correlation_id
    )

def serialize_message(message: LinkMessage) -> str:
    """
    Serializes a LinkMessage instance into a JSON string.
    Uses Pydantic's `model_dump_json` for efficient serialization
    and adherence to aliases (camelCase).
    
    Args:
        message: The `LinkMessage` instance.

    Returns:
        A JSON string representation of the message.
    """
    return message.model_dump_json(by_alias=True)

def parse_message(data: str | bytes) -> LinkMessage:
    """
    Parses a JSON string or bytes into a LinkMessage instance.
    
    Args:
        data: The raw JSON string or bytes.

    Returns:
        A `LinkMessage` instance.
    
    Raises:
        ValidationError: If the JSON data does not match the
                         `LinkMessage` schema.
    """
    try:
        return LinkMessage.model_validate_json(data)
    except ValidationError as e:
        print(f"Failed to parse message: {e}")
        raise

def parse_payload(
    message: LinkMessage,
    payload_type: Type[PayloadType]
) -> PayloadType:
    """
    Safely parses the `payload` dict from a LinkMessage into its
    specific Pydantic model.

    Example:
        message = parse_message(raw_json)
        if message.message_type == MessageType.TASK_REQUEST:
            try:
                payload = parse_payload(message, TaskRequestPayload)
                # now you have a fully-typed `payload` object
                print(payload.task_name)
            except ValidationError as e:
                print(f"Invalid TaskRequest payload: {e}")

    Args:
        message: The `LinkMessage` containing the payload dict.
        payload_type: The Pydantic `BasePayload` subclass to validate against
                      (e.g., `TaskRequestPayload`).

    Returns:
        An instance of the specified `payload_type`.

    Raises:
        ValidationError: If the payload dict does not match the
                         `payload_type` schema.
    """
    try:
        return payload_type.model_validate(message.payload)
    except ValidationError as e:
        print(f"Failed to parse payload for {message.message_type}: {e}")
        raise

# Define what is exposed when `from .protocol import *` is used
__all__ = [
    # Models
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
    
    # Enums
    "MessageType",
    "TaskStatus",
    "MemoryPersistence",
    
    # Constants
    "BROADCAST_ID",
    "PROTOCOL_VERSION",
    
    # Helpers
    "create_message",
    "serialize_message",
    "parse_message",
    "parse_payload",
    "ValidationError"
]
