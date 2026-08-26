"""Transport-neutral public surface for the AFC Telegram assistant plugin."""

from .contracts import (
    AssistantAttachment,
    AssistantBackend,
    AssistantCapabilities,
    AssistantSessionError,
    AssistantTurnCancelled,
    AssistantTurnResult,
)

__all__ = [
    "AssistantAttachment",
    "AssistantBackend",
    "AssistantCapabilities",
    "AssistantSessionError",
    "AssistantTurnCancelled",
    "AssistantTurnResult",
]

__version__ = "0.1.0"
