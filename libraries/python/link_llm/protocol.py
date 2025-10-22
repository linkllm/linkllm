# llm_link/protocol.py
from pydantic import BaseModel
from typing import Dict, Any
import json

class Message(BaseModel):
    """Defines the core JSON schema for inter-agent communication."""
    from_agent: str
    to_agent: str
    task: str
    context: Dict[str, Any]

    def to_json(self) -> str:
        """Return message as formatted JSON string."""
        return json.dumps(self.model_dump(), indent=2)

    @staticmethod
    def from_json(data: str):
        """Create a Message object from a JSON string."""
        return Message(**json.loads(data))
