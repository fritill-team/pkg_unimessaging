from __future__ import annotations

import asyncio
import inspect
import json
import logging
from typing import Callable, List, Optional

from .client import UnifiedMessaging
from .config import JetStreamConsumer
from .registry import HandlerRegistry, _default_registry
from .utils import create_messaging_client

logger = logging.getLogger(__name__)


class UnifiedMessageBroker:
    """Transport-agnostic messaging broker facade.

    Manages client lifecycle, subscribes to subjects, dispatches
    incoming messages to registered handlers via a ``HandlerRegistry``,
    and optionally runs JetStream durable pull consumers.
    """

    def __init__(
        self,
        *,
        subjects: Optional[List[str]] = None,
        service_name: str = "service",
        url: str = "nats://localhost:4222",
        enable_durable: bool = False,
        stream_name: Optional[str] = None,
        stream_subjects: Optional[List[str]] = None,
        consumers: Optional[List[JetStreamConsumer]] = None,
        pull_batch: int = 10,
        pull_timeout: float = 1.0,
        registry: Optional[HandlerRegistry] = None,
        client: Optional[UnifiedMessaging] = None,
        queue_group: Optional[str] = None,
    ) -> None:
        # An empty subject list means "subscribe to nothing". This used to fall
        # back to ["notifications.>"], so any service that declared no core
        # subjects silently subscribed to the entire notifications tree — and
        # every lifespan invented a "__<service>_internal.none" placeholder
        # subject purely to stop that firing. Both are gone; start() already
        # handles the empty case. See B2b in
        # .agent/planning/replica-safety-nats-investigation.md.
        self.subjects = [s.strip() for s in (subjects or []) if s and s.strip()]
        self.service_name = service_name
        # Core NATS subscriptions and RPC responders join a queue group so that
        # ONE replica handles each message, instead of every replica receiving
        # every message (plain fan-out) and N responders answering one request.
        # Defaults to the service name: each service forms its own group, which
        # is the load-balancing unit. Pass an explicit value only to split a
        # service's replicas into separate groups on purpose.
        self.queue_group = queue_group or service_name
        self.registry = registry or _default_registry
        self._consumers = consumers or []
        self._pull_batch = pull_batch
        self._pull_timeout = pull_timeout
        self._consumer_tasks: List[asyncio.Task] = []
        self.client = client or create_messaging_client(
            service_name,
            url=url,
            enable_durable=enable_durable,
            stream_name=stream_name,
            stream_subjects=stream_subjects,
            consumers=self._consumers,
            pull_batch=pull_batch,
            pull_timeout=pull_timeout,
        )
        self._started = False

    async def start(self) -> None:
        if self._started:
            logger.info("Broker already running; skip start.")
            return
        # Connect unconditionally. There used to be an early return when
        # ``self.subjects`` was empty, unreachable only because the removed
        # ["notifications.>"] fallback made that list never empty. Returning
        # here leaves ``_started`` true with no connection behind it, so a
        # broker that only publishes or serves RPC — nothing inbound to
        # subscribe to — fails on a None client at its first call. Having
        # nothing to listen on is not a reason not to connect; both loops
        # below are simply empty. See B2b in
        # .agent/planning/replica-safety-nats-investigation.md.
        await self.client.start()
        if not self.subjects and not self._consumers:
            logger.info("No subjects or consumers configured; nothing to listen on.")
        for subject in self.subjects:
            await self.client.subscribe(
                subject, self._on_message, queue=self.queue_group
            )
            logger.info(
                "Subscribed to '%s' (queue=%s)", subject, self.queue_group
            )

        # Start JetStream durable consumers
        for consumer in self._consumers:
            task = asyncio.create_task(self._run_consumer(consumer))
            self._consumer_tasks.append(task)
            logger.info(
                "JetStream %s consumer started (subject=%s, durable=%s)",
                consumer.label,
                consumer.subject,
                consumer.durable,
            )

        self._started = True

    async def stop(self) -> None:
        if not self._started:
            logger.info("Broker not running; skip stop.")
            return

        # Cancel consumer tasks first
        for task in self._consumer_tasks:
            task.cancel()
        for task in self._consumer_tasks:
            try:
                await task
            except asyncio.CancelledError:
                pass
        if self._consumer_tasks:
            logger.info("JetStream consumers stopped")
        self._consumer_tasks.clear()

        await self.client.stop()
        self._started = False

    async def publish(self, subject: str, message: dict) -> None:
        if not self._started:
            raise RuntimeError("Broker.publish called before start().")
        logger.debug("Publish -> %s: %s", subject, message)
        await self.client.publish(subject, message)

    async def reply(self, subject: str, handler: Callable) -> None:
        async def _on_req(data: bytes, meta: dict) -> None:
            try:
                payload = json.loads(data.decode()) if data else None
            except Exception:
                payload = data
            return await handler(payload, meta)

        await self.client.reply(subject, _on_req, queue=self.queue_group)

    # ── JetStream Consumer Runner ────────────────────────────────────

    async def _run_consumer(self, consumer: JetStreamConsumer) -> None:
        """Run a durable pull consumer with handler dispatch."""

        async def _on_message(data: bytes, meta: dict) -> None:
            subject = meta.get("subject", "")
            if not data:
                return
            try:
                payload = json.loads(data.decode())
            except Exception:
                logger.error(
                    "Invalid JSON on JetStream %s; dropping", subject
                )
                return
            handler = self.registry.resolve_handler(subject)
            if handler is None:
                logger.warning(
                    "No handler for JetStream subject %s", subject
                )
                return
            result = handler(payload, subject)
            if inspect.isawaitable(result):
                await result

        try:
            await self.client.pull_consume(
                subject=consumer.subject,
                durable=consumer.durable,
                handler=_on_message,
                batch=self._pull_batch,
                timeout=self._pull_timeout,
                deliver_policy=consumer.deliver_policy,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            message = str(exc).lower()
            if "stream" in message and (
                "not found" in message or "no response" in message
            ):
                logger.warning(
                    "JetStream %s consumer skipped; stream unavailable: %s",
                    consumer.label,
                    exc,
                )
                return
            raise

    # ── Core Message Handler ─────────────────────────────────────────

    async def _on_message(self, data: bytes, meta: dict) -> None:
        subject = meta.get("subject", "")
        if not data:
            logger.debug("Skip empty payload on %s", subject)
            return
        try:
            text = data.decode()
        except UnicodeDecodeError:
            logger.error("Non-UTF8 payload on %s; dropping.", subject)
            return
        try:
            payload = json.loads(text)
        except json.JSONDecodeError:
            logger.error("Invalid JSON on %s: %s", subject, text)
            return

        handler = self.registry.resolve_handler(subject)
        if handler is None:
            logger.debug("No handler for subject %s; ignoring.", subject)
            return
        try:
            result = handler(payload, subject)
            if callable(getattr(result, "__await__", None)):
                await result
        except Exception as exc:
            logger.error("Handler error for %s: %s", subject, exc)