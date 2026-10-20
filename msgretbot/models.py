"""Transport-independent message and delivery types."""
from dataclasses import dataclass
from enum import Enum
from typing import Protocol

TTL = 24 * 60 * 60


@dataclass(frozen=True)
class Message:
    account: int
    chat: int
    id: int
    author: int | None
    name: str
    username: str | None
    sent_at: float
    text: str | None
    kind: str = "text"
    own_alert: bool = False
    revision_at: float = 0


class Presence(Enum):
    PRESENT = "present"
    DELETED = "deleted"
    UNKNOWN = "unknown"


class RetryDelivery(Exception):
    def __init__(self, delay: float = 5):
        self.delay = max(1, delay)
        super().__init__("Delivery must be retried")


class PermanentDelivery(Exception):
    """Delivery is forbidden or the destination is unavailable."""


class Reader(Protocol):
    async def presence(self, chat: int, ids: list[int]) -> dict[int, Presence]: ...


class Sender(Protocol):
    async def send(self, chat: int, html: str, delivery_key: str, reply_to: int | None = None) -> int: ...
    async def close(self) -> None: ...


class ContextFormatter(Protocol):
    def render(self, messages: list[Message]) -> str: ...
