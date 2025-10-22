# llm_link/client.py
from .protocol import Message

class AgentClient:
    """Base class representing an LLM or AI Agent client."""

    def __init__(self, name: str):
        self.name = name
        self.inbox = []

    def send_message(self, to: str, task: str, context: dict) -> Message:
        """Create a message to send to another agent."""
        msg = Message(from_agent=self.name, to_agent=to, task=task, context=context)
        print(f"[{self.name}] Sending → {to}")
        print(msg.to_json())
        return msg

    def receive_message(self, message: Message):
        """Receive a message and add to inbox."""
        self.inbox.append(message)
        print(f"[{self.name}] Received message from {message.from_agent}: {message.task}")
