"""Async pub/sub broker infrastructure."""

from ..adapters.nats.async_adapter import ConsumerLimitsRefused
from .broker import UnifiedMessageBroker
from .client import UnifiedMessaging
from .config import JetStreamConsumer, MessagingConfig, RetryLimits
from .registry import (
    HandlerRegistry,
    register_handler,
    register_rpc,
    resolve_handler,
    resolve_rpc_handler,
)
from .utils import (
    build_notification_headers,
    create_messaging_client,
    prepare_notification_payload,
)

__all__ = [
    "ConsumerLimitsRefused",
    "HandlerRegistry",
    "JetStreamConsumer",
    "MessagingConfig",
    "RetryLimits",
    "UnifiedMessageBroker",
    "UnifiedMessaging",
    "build_notification_headers",
    "create_messaging_client",
    "prepare_notification_payload",
    "register_handler",
    "register_rpc",
    "resolve_handler",
    "resolve_rpc_handler",
]