from __future__ import annotations

from typing import Any

from .config import MessagingConfig
from ..adapters.nats.async_adapter import NATSAdapter


class UnifiedMessaging:
    """Thin facade that delegates to the async NATS adapter."""

    def __init__(self, cfg: MessagingConfig) -> None:
        self.cfg = cfg
        self.adapter = NATSAdapter(cfg)

    async def start(self) -> None:
        await self.adapter.start()

    async def stop(self) -> None:
        await self.adapter.stop()

    async def publish(self, *a: Any, **kw: Any) -> Any:
        return await self.adapter.publish(*a, **kw)

    async def subscribe(self, *a: Any, **kw: Any) -> Any:
        return await self.adapter.subscribe(*a, **kw)

    async def request(self, *a: Any, **kw: Any) -> Any:
        return await self.adapter.request(*a, **kw)

    async def reply(self, *a: Any, **kw: Any) -> Any:
        return await self.adapter.reply(*a, **kw)

    async def scatter_gather(self, *a: Any, **kw: Any) -> Any:
        return await self.adapter.scatter_gather(*a, **kw)

    async def pull_consume(self, *a: Any, **kw: Any) -> Any:
        return await self.adapter.pull_consume(*a, **kw)

    async def ensure_consumer_limits(self, *a: Any, **kw: Any) -> Any:
        return await self.adapter.ensure_consumer_limits(*a, **kw)
