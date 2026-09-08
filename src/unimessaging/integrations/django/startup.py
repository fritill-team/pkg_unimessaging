"""Django messaging startup / shutdown helpers.

Unlike the FastAPI integration (which attaches to ``app.state``), Django
has no request-scoped application object, so the broker and client are
stored at module level::

    from unimessaging.integrations.django import start_messaging, get_messaging

    # At startup (e.g. AppConfig.ready or a management command):
    await start_messaging(subjects=[...], service_name="my-service")

    # Later, in views or use cases:
    messaging = get_messaging()
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, List, Optional

from unimessaging.broker.broker import UnifiedMessageBroker
from unimessaging.broker.config import JetStreamConsumer
from unimessaging.broker.registry import HandlerRegistry

if TYPE_CHECKING:
    from unimessaging.broker.client import UnifiedMessaging

logger = logging.getLogger(__name__)

_broker: Optional[UnifiedMessageBroker] = None
_client: Optional["UnifiedMessaging"] = None


async def start_messaging(
    *,
    subjects: List[str],
    service_name: str,
    url: str = "nats://localhost:4222",
    enable_durable: bool = False,
    stream_name: Optional[str] = None,
    stream_subjects: Optional[List[str]] = None,
    consumers: Optional[List[JetStreamConsumer]] = None,
    pull_batch: int = 10,
    pull_timeout: float = 1.0,
    registry: Optional[HandlerRegistry] = None,
    queue_group: Optional[str] = None,
) -> UnifiedMessageBroker:
    """Create and start a :class:`UnifiedMessageBroker`, storing it at module level."""
    global _broker, _client
    broker = UnifiedMessageBroker(
        subjects=subjects,
        service_name=service_name,
        url=url,
        enable_durable=enable_durable,
        stream_name=stream_name,
        stream_subjects=stream_subjects,
        consumers=consumers,
        pull_batch=pull_batch,
        pull_timeout=pull_timeout,
        registry=registry,
        queue_group=queue_group,
    )
    await broker.start()
    _broker = broker
    _client = broker.client
    logger.info("Messaging broker started")
    return broker


async def stop_messaging() -> None:
    """Stop the broker previously started by :func:`start_messaging`."""
    global _broker, _client
    if _broker is not None:
        try:
            await _broker.stop()
        except Exception:
            pass
    _broker = None
    _client = None
    logger.info("Messaging broker stopped")


def get_messaging() -> Optional["UnifiedMessaging"]:
    """Return the ``UnifiedMessaging`` client (or ``None`` if not started)."""
    return _client


def get_broker() -> Optional[UnifiedMessageBroker]:
    """Return the ``UnifiedMessageBroker`` (or ``None`` if not started)."""
    return _broker