from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass
from typing import Optional

from unimessaging.adapters.nats.redact import mask_url
from unimessaging.domain.ports import NotificationGateway
from unimessaging.domain.entities import Message

try:  # Optional dependency to avoid forcing nats-py on all users.
    from nats.aio.client import Client as NATS
except ModuleNotFoundError as exc:  # pragma: no cover - executed only without nats-py
    NATS = None  # type: ignore[assignment]
    _IMPORT_ERROR = exc
else:
    _IMPORT_ERROR = None


@dataclass(frozen=True)
class NATSConfig:
    url: str = "nats://localhost:4222"
    subject: str = "notifications.default"
    client_name: str = "unimessaging"
    flush_timeout: float = 2.0


class NATSNotificationGateway(NotificationGateway):
    """Notification gateway implemented on top of NATS JetStream/Core."""

    def __init__(self, config: Optional[NATSConfig] = None):
        if NATS is None:  # pragma: no cover - depends on optional import
            raise RuntimeError(
                "nats-py is required to use NATSNotificationGateway. Install via 'pip install nats-py'."
            ) from _IMPORT_ERROR
        self._config = config or NATSConfig()
        self._safe_url = mask_url(self._config.url)
        self._logger = logging.getLogger(__name__)
        self._logger.debug(
            "Initialized NATSNotificationGateway (url=%s, subject=%s, client=%s)",
            self._safe_url,
            self._config.subject,
            self._config.client_name,
        )

    def deliver(self, message: Message) -> dict:
        """Publish the message synchronously, hiding the async details."""

        try:
            asyncio.get_running_loop()
        except RuntimeError:
            # Safe to create a new loop when not already inside one.
            return asyncio.run(self._publish(message))
        raise RuntimeError(
            "NATSNotificationGateway cannot run inside an active event loop. "
            "Invoke it from a sync context (FastAPI sync endpoint or to_thread)."
        )

    async def _publish(self, message: Message) -> dict:
        assert NATS is not None  # for type-checkers
        nc = NATS()
        self._logger.debug("Connecting to NATS at %s", self._safe_url)
        await nc.connect(self._config.url, name=self._config.client_name)
        payload = message.to_dict()
        self._logger.info(
            "Publishing message to %s (recipient=%s)",
            self._config.subject,
            payload.get("to"),
        )
        await nc.publish(self._config.subject, json.dumps(payload).encode("utf-8"))
        await nc.flush(timeout=self._config.flush_timeout)
        await nc.close()
        enriched = dict(payload)
        enriched["subject"] = self._config.subject
        return enriched
