from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional, Tuple


def _is_finite_number(value: object) -> bool:
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(value)
    )


@dataclass(frozen=True)
class RetryLimits:
    """Opt-in redelivery bounds for one durable (estate:AD-22).

    ``max_deliver`` and ``ack_wait`` (seconds) are set on the durable itself;
    ``nak_delays`` are client-side ``nak(delay=)`` values for failed deliveries
    1, 2, 3, …, the last reused for every later one. Server ``backoff`` is never
    set: its first entry would replace ``ack_wait``.
    """

    max_deliver: int = 5
    ack_wait: float = 60.0
    nak_delays: Tuple[float, ...] = (1.0, 5.0, 30.0, 120.0)

    def __post_init__(self) -> None:
        if (
            isinstance(self.max_deliver, bool)
            or not isinstance(self.max_deliver, int)
            or self.max_deliver <= 0
        ):
            raise ValueError(
                f"max_deliver must be a positive int; got {self.max_deliver!r}"
            )
        if not _is_finite_number(self.ack_wait) or self.ack_wait <= 0:
            raise ValueError(
                f"ack_wait must be a finite positive number; got {self.ack_wait!r}"
            )
        if not isinstance(self.nak_delays, (tuple, list)):
            raise ValueError(
                f"nak_delays must be a tuple or list; got {self.nak_delays!r}"
            )
        delays = tuple(self.nak_delays)
        if not delays:
            raise ValueError("nak_delays must not be empty")
        for d in delays:
            if not _is_finite_number(d) or d < 0:
                raise ValueError(
                    f"nak_delays must be finite numbers >= 0; got {d!r}"
                )
        object.__setattr__(self, "nak_delays", delays)


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
    # None binds exactly as before (no max_deliver, immediate nak). When set,
    # a new durable is created with the limits and an existing one is updated
    # in place at broker start. Not settable via parse_many's env DSL.
    limits: Optional[RetryLimits] = None

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

    # Credentials outside the URL, each passed to connect() only when set.
    # Precedence is nats-py's (2.13.1, client.py:1546-1571), not re-implemented
    # here: creds_file wins, then user+password, then token, then URL userinfo.
    user: Optional[str] = None
    password: Optional[str] = None
    token: Optional[str] = None
    creds_file: Optional[str] = None

    # Reconnection
    max_reconnect_attempts: int = -1  # -1 = infinite
    reconnect_time_wait: float = 2.0  # seconds between reconnect attempts

    # Pull consumer retry
    pull_consumer_retry_delay: float = 2.0  # seconds before re-subscribing on error

    # JetStream consumers
    consumers: List[JetStreamConsumer] = field(default_factory=list)
    pull_batch: int = 10
    pull_timeout: float = 1.0