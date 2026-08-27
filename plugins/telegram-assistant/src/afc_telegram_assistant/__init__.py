"""Transport-neutral public surface for the AFC Telegram assistant plugin."""

from .contracts import (
    AssistantAttachment,
    AssistantBackend,
    AssistantCapabilities,
    AssistantSessionError,
    AssistantTurnCancelled,
    AssistantTurnResult,
)
from .service import (
    GatewayContext,
    GatewayFactory,
    GatewayFactoryError,
    GatewayService,
    load_gateway_factory,
    run_gateway,
    run_gateway_specification,
)
from .transport import TelegramAPIError, TelegramBotAPI, split_text

__all__ = [
    "AssistantAttachment",
    "AssistantBackend",
    "AssistantCapabilities",
    "AssistantSessionError",
    "AssistantTurnCancelled",
    "AssistantTurnResult",
    "GatewayContext",
    "GatewayFactory",
    "GatewayFactoryError",
    "GatewayService",
    "TelegramAPIError",
    "TelegramBotAPI",
    "load_gateway_factory",
    "run_gateway",
    "run_gateway_specification",
    "split_text",
]

__version__ = "0.2.0"
