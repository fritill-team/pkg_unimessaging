from __future__ import annotations

import logging
from typing import TYPE_CHECKING, List, Optional

from unimessaging.broker.broker import UnifiedMessageBroker
from unimessaging.broker.config import JetStreamConsumer
from unimessaging.broker.registry import HandlerRegistry

if TYPE_CHECKING:
    from fastapi import FastAPI

logger = logging.getLogger(__name__)


async def start_messaging(
    app: "FastAPI",
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
    """Create and start a :class:`UnifiedMessageBroker`, attaching it to *app.state*."""
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
    app.state.messaging_broker = broker
    app.state.messaging = broker.client
    logger.info("Messaging broker started")
    return broker


async def stop_messaging(app: "FastAPI") -> None:
    """Stop the broker previously attached by :func:`start_messaging`."""
    broker = getattr(app.state, "messaging_broker", None)
    if broker is not None:
        try:
            await broker.stop()
        except Exception:
            pass
    for attr in ("messaging", "messaging_broker"):
        if hasattr(app.state, attr):
            delattr(app.state, attr)
    logger.info("Messaging broker stopped")
