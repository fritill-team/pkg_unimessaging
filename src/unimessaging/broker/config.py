from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional


@dataclass
class JetStreamConsumer:
    """A durable pull consumer definition."""

    label: str
    subject: str
    durable: str
    # Pins where a *newly created* durable starts reading ("all"/"new"/"last"/…);
    # only applied at consumer creation, so an existing durable keeps its stored
    # policy and None preserves the historical bare bind. The estate:AD-15
    # collapses rename durables (a rename == a new consumer), and set this to
    # "new"/"last" so the fresh durable does not replay the whole retained stream
    # through already-collapsed handlers. Not settable via parse_many's env DSL
    # yet — construct the consumer in code to pin it.
    deliver_policy: Optional[str] = None

    @staticmethod
    def parse_many(raw_items: List[str]) -> List["JetStreamConsumer"]:
        """Parse consumer definitions from env-style strings.

        Accepts ``"label|subject|durable"`` or ``"subject|durable"`` format.
        When *label* is omitted the first segment of the subject is used.
        """
        consumers: List[JetStreamConsumer] = []
        for raw in raw_items:
            parts = [p.strip() for p in raw.split("|")]
            if len(parts) == 3:
                label, subject, durable = parts
            elif len(parts) == 2:
                subject, durable = parts
                label = subject.split(".", 1)[0] or "consumer"
            else:
                raise ValueError(
                    "Invalid consumer entry. Use 'label|subject|durable' or "
                    f"'subject|durable'. Got: {raw}"
                )
            if not subject or not durable:
                raise ValueError(
                    "Consumer entry requires subject and durable. "
                    f"Got: {raw}"
                )
            if not label:
                label = subject.split(".", 1)[0] or "consumer"
            consumers.append(JetStreamConsumer(label=label, subject=subject, durable=durable))
        return consumers


@dataclass
class MessagingConfig:
    backend: str = "nats"
    url: str = "nats://localhost:4222"
    name: Optional[str] = None
    enable_durable: bool = False
    stream_name: Optional[str] = None
    stream_subjects: Iterable[str] = field(default_factory=list)
    request_timeout: float = 2.0
    max_inflight: int = 64
    default_headers: Dict[str, str] = field(default_factory=dict)

    # Reconnection
    max_reconnect_attempts: int = -1  # -1 = infinite
    reconnect_time_wait: float = 2.0  # seconds between reconnect attempts

    # Pull consumer retry
    pull_consumer_retry_delay: float = 2.0  # seconds before re-subscribing on error

    # JetStream consumers
    consumers: List[JetStreamConsumer] = field(default_factory=list)
    pull_batch: int = 10
    pull_timeout: float = 1.0