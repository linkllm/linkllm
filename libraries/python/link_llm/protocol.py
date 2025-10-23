# protocol.py — The Heart of LinkLLM
# ------------------------------------------------------------
# Defines the canonical JSON-based message protocol used for LLM↔LLM
# communication, memory sharing, and context synchronization.
# This file is the foundation — all bridges, clients, and registries
# must adhere to this schema.
# ------------------------------------------------------------

from __future__ import annotations
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional, Union
from enum import Enum
from pydantic import BaseModel, Field, root_validator, validator

# ------------------------------------------------------------
# 🧩 ENUMS — Core Constants
# ------------------------------------------------------------

class Role(str, Enum):
    """Defines the role of the sender in a LinkLLM message."""

    SYSTEM = "system"
    AGENT = "agent"
    USER = "user"


class MessageType(str, Enum):
    """Represents the intent category of a message."""

    TASK = "task"          # Request to perform an action
    RESULT = "result"      # Response to a task or query
    HEARTBEAT = "heartbeat"# Health or status ping
    MEMORY = "memory"      # Memory registration or retrieval
    SYNC = "sync"          # Context or state synchronization


# ------------------------------------------------------------
# 🧠 CORE DATA MODELS
# ------------------------------------------------------------

class MemoryPointer(BaseModel):
    """Pointer to a memory object stored in a registry or database."""

    uri: str = Field(..., description="Unique identifier or URI for memory record")
    summary: Optional[str] = Field(None, description="Human-readable memory summary")


class ContextFragment(BaseModel):
    """A fragment of contextual data passed between agents."""

    key: str
    value: Any
    origin: Optional[str] = Field(None, description="Source of this context value")
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class LinkHeader(BaseModel):
    """Standardized metadata header for messages."""

    protocol: str = Field("LLM-LINK/1.0", description="Protocol version")
    encoding: str = Field("json", description="Serialization format")
    id: str = Field(default_factory=lambda: str(uuid.uuid4()), description="Message ID")
    created_at: datetime = Field(default_factory=datetime.utcnow)
    trace_id: Optional[str] = Field(None, description="Tracing / correlation ID")


# ------------------------------------------------------------
# 📦 MESSAGE SPECIFICATION
# ------------------------------------------------------------

class LinkMessage(BaseModel):
    """Primary data object for LinkLLM communication."""

    header: LinkHeader = Field(default_factory=LinkHeader)

    # Routing
    from_agent: str = Field(..., alias="from", description="Sender agent ID")
    to_agent: Optional[str] = Field(None, description="Target agent ID or broadcast")

    # Classification
    type: MessageType = Field(MessageType.TASK, description="Type of message")
    role: Role = Field(Role.AGENT, description="Role of sender")

    # Payload
    intent: Optional[str] = Field(None, description="Intent or semantic label")
    content: Optional[Union[str, Dict[str, Any]]] = Field(
        None, description="Primary data payload — may be text or structured object"
    )

    # Contextual data
    context: List[ContextFragment] = Field(default_factory=list)
    memory_refs: List[MemoryPointer] = Field(default_factory=list)

    # Optional metadata
    meta: Dict[str, Any] = Field(default_factory=dict)

    # ------------------------------------------------------------
    # 🧩 VALIDATION LOGIC
    # ------------------------------------------------------------

    @validator("from_agent")
    def validate_sender(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("'from' (sender) cannot be empty")
        return v

    @root_validator
    def normalize_content(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        content = values.get("content")
        meta = values.get("meta", {})

        if isinstance(content, dict):
            meta["content_type"] = "object"
        elif isinstance(content, str):
            meta["content_type"] = "text"
        else:
            meta["content_type"] = "none"

        values["meta"] = meta
        return values

    # ------------------------------------------------------------
    # 🔄 SERIALIZATION / DESERIALIZATION
    # ------------------------------------------------------------

    def to_dict(self) -> Dict[str, Any]:
        """Return a JSON-serializable dict representation."""
        return self.dict(by_alias=True, exclude_none=True)

    def to_json(self, **kwargs) -> str:
        import json

        return json.dumps(self.to_dict(), default=str, **kwargs)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "LinkMessage":
        # Accept alias flexibility
        if "from" in data and "from_agent" not in data:
            data["from_agent"] = data.pop("from")
        return cls.parse_obj(data)

    @classmethod
    def from_json(cls, raw: str) -> "LinkMessage":
        import json

        return cls.from_dict(json.loads(raw))

    # ------------------------------------------------------------
    # 🧠 INTELLIGENT UTILITIES
    # ------------------------------------------------------------

    def summarize(self) -> str:
        """Return a human-readable summary for logs or inspection."""
        preview = (
            (self.content[:100] + "...")
            if isinstance(self.content, str) and len(self.content) > 100
            else self.content
        )
        return f"[{self.type}] {self.from_agent} → {self.to_agent or 'broadcast'} | {self.intent} | {preview}"

    def add_context(self, key: str, value: Any, origin: Optional[str] = None):
        frag = ContextFragment(key=key, value=value, origin=origin)
        self.context.append(frag)

    def add_memory_ref(self, uri: str, summary: Optional[str] = None):
        ref = MemoryPointer(uri=uri, summary=summary)
        self.memory_refs.append(ref)

# ------------------------------------------------------------
# 🌟 EXAMPLE USAGE
# ------------------------------------------------------------

if __name__ == "__main__":
    # Minimal demonstration
    msg = LinkMessage(
        from_agent="gpt-4",
        to_agent="claude-3",
        intent="summarize.paper",
        content={"title": "Quantum AI", "url": "https://arxiv.org/..."},
    )

    msg.add_context("topic", "research")
    msg.add_memory_ref("mem://12345", "Previous summary context")

    print("Serialized JSON:\n", msg.to_json(indent=2))
    print("\nSummary:", msg.summarize())
