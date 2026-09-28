"""Transactional outbox for reliable domain event publishing.

Provides the full outbox infrastructure:

- ``OutboxMixin`` / ``OutboxStatus`` — table schema (mixin for your Base)
- ``OutboxRepository`` — writes rows within the caller's transaction
- ``OutboxEventBus`` — serializes dataclass events into outbox rows
- ``OutboxRelay`` / ``relay_loop`` — polls and publishes to messaging
- ``run_standalone_relay`` / ``relay_in_process`` — run the relay as its own
  process, and read ``OUTBOX_RELAY_IN_PROCESS`` for the web pod's opt-out
- ``python -m unimessaging.outbox.healthcheck`` — its exec liveness probe
"""

from .models import OutboxMixin, OutboxStatus
from .repository import OutboxRepository
from .event_bus import OutboxEventBus
from .relay import OutboxRelay, relay_loop
from .standalone import (
    OUTBOX_RELAY_IN_PROCESS_ENV,
    relay_in_process,
    run_standalone_relay,
)

__all__ = [
    "OutboxMixin",
    "OutboxStatus",
    "OutboxRepository",
    "OutboxEventBus",
    "OutboxRelay",
    "relay_loop",
    "run_standalone_relay",
    "relay_in_process",
    "OUTBOX_RELAY_IN_PROCESS_ENV",
]
