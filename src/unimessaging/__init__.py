"""Public API for the unimessaging package."""

from importlib.metadata import PackageNotFoundError, version as _dist_version

try:
    __version__ = _dist_version("unimessaging")
except PackageNotFoundError:  # running from a source tree that was never installed
    __version__ = "0.0.0"

# Domain
from .domain.entities import Message
from .domain.exceptions import InvalidMessageError
from .domain.ports import AsyncMessagingPort, MessageHandler, NotificationGateway

# Application
from .application.dto import SendMessageRequest, SendMessageResponse
from .application.use_cases import SendMessageUseCase

# Adapters
from .adapters import InMemoryNotificationGateway, InMemoryBrokerAdapter

# Broker
from .broker import (
    HandlerRegistry,
    MessagingConfig,
    UnifiedMessageBroker,
    UnifiedMessaging,
    register_handler,
    register_rpc,
    resolve_handler,
    resolve_rpc_handler,
    create_messaging_client,
    prepare_notification_payload,
    build_notification_headers,
)

# Facade
from .integrations.common.facade import send_message

__all__ = [
    # Domain
    "Message",
    "InvalidMessageError",
    "NotificationGateway",
    "AsyncMessagingPort",
    "MessageHandler",
    # Application
    "SendMessageRequest",
    "SendMessageResponse",
    "SendMessageUseCase",
    # Adapters
    "InMemoryNotificationGateway",
    "InMemoryBrokerAdapter",
    # Broker
    "HandlerRegistry",
    "MessagingConfig",
    "UnifiedMessageBroker",
    "UnifiedMessaging",
    "register_handler",
    "register_rpc",
    "resolve_handler",
    "resolve_rpc_handler",
    "create_messaging_client",
    "prepare_notification_payload",
    "build_notification_headers",
    # Facade
    "send_message",
    # Outbox (requires sqlalchemy — import via unimessaging.outbox)
    "outbox",
]
