"""Run the outbox relay as its own process, outside any web framework.

A service that splits publishing off its web pods runs::

    asyncio.run(run_standalone_relay(
        lambda messaging: MyOutboxRelay(sessionmaker, messaging, subject_prefix="courses"),
        service_name="courses",
        url=settings.NATS_URL,
        enable_durable=True,
        stream_name=contract.STREAM_NAME,
        stream_subjects=contract.STREAM_SUBJECTS,
        heartbeat_path="/tmp/outbox-relay.heartbeat",
    ))

and guards its in-process relay with :func:`relay_in_process`.  ``SKIP LOCKED``
makes it safe for both to run at once, which is what a cut-over relies on.
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal
from pathlib import Path
from typing import Callable, List, Mapping, Optional

from .relay import OutboxRelay, relay_loop

logger = logging.getLogger("unimessaging.outbox")

OUTBOX_RELAY_IN_PROCESS_ENV = "OUTBOX_RELAY_IN_PROCESS"

_FALSE_VALUES = frozenset({"false", "0", "no", "off"})
_TRUE_VALUES = frozenset({"true", "1", "yes", "on", ""})

_STOP_SIGNALS = (signal.SIGTERM, signal.SIGINT)


def relay_in_process(environ: Optional[Mapping[str, str]] = None) -> bool:
    """Whether this process should run the outbox relay in-process.

    Reads ``OUTBOX_RELAY_IN_PROCESS`` (default ``true``).  ``false`` means
    "the relay runs elsewhere", never "the relay is off".

    Case-insensitive, surrounding whitespace ignored.  Only ``false``, ``0``,
    ``no`` and ``off`` return ``False``.  ``true``, ``1``, ``yes``, ``on``,
    empty and unset return ``True``.  Any other value also returns ``True``
    with a warning: a typo then leaves two relays running (safe, because
    rows are claimed with ``SKIP LOCKED``) instead of possibly zero.
    """
    env = os.environ if environ is None else environ
    raw = env.get(OUTBOX_RELAY_IN_PROCESS_ENV)
    if raw is None:
        return True
    value = raw.strip().lower()
    if value in _FALSE_VALUES:
        return False
    if value not in _TRUE_VALUES:
        logger.warning(
            "Unrecognised %s=%r; running the relay in-process",
            OUTBOX_RELAY_IN_PROCESS_ENV,
            raw,
        )
    return True


def _heartbeat(path: str) -> Callable[[], None]:
    target = Path(path)

    def touch() -> None:
        target.touch()

    return touch


async def run_standalone_relay(
    relay_factory: Callable[..., OutboxRelay],
    *,
    service_name: str,
    url: str,
    enable_durable: bool,
    stream_name: Optional[str] = None,
    stream_subjects: Optional[List[str]] = None,
    poll_interval: float = 0.5,
    heartbeat_path: Optional[str] = None,
) -> None:
    """Publish outbox rows until SIGTERM or SIGINT, then return.

    Connects a publish-only :class:`UnifiedMessageBroker` (no subjects, no
    consumers, its own empty registry), declares *stream_name* /
    *stream_subjects* create-if-absent exactly as the web pod would, and
    runs ``relay_loop(relay_factory(messaging))``.

    On a stop signal the loop is cancelled and awaited, so the in-flight
    batch's transaction rolls back and its claimed rows become claimable
    again; then the broker is stopped.  A row published but not yet marked
    is published again later: the outbox is at-least-once.

    Must run on the main thread's event loop (signal handlers need it).
    """
    # Imported here so ``unimessaging.outbox`` stays importable without the
    # broker's transport dependencies.
    from unimessaging.broker.broker import UnifiedMessageBroker
    from unimessaging.broker.registry import HandlerRegistry

    broker = UnifiedMessageBroker(
        subjects=[],
        service_name=service_name,
        url=url,
        enable_durable=enable_durable,
        stream_name=stream_name,
        stream_subjects=stream_subjects,
        consumers=[],
        registry=HandlerRegistry(),
    )

    loop = asyncio.get_running_loop()
    stop = asyncio.Event()
    previous = {sig: signal.getsignal(sig) for sig in _STOP_SIGNALS}
    for sig in _STOP_SIGNALS:
        loop.add_signal_handler(sig, stop.set)

    relay_task: Optional[asyncio.Task] = None
    try:
        await broker.start()
        relay = relay_factory(broker.client)
        on_tick = _heartbeat(heartbeat_path) if heartbeat_path else None
        relay_task = asyncio.create_task(
            relay_loop(relay, poll_interval=poll_interval, on_tick=on_tick),
            name="outbox-relay",
        )
        stop_task = asyncio.create_task(stop.wait())
        logger.info("Standalone outbox relay running: service=%s", service_name)
        try:
            await asyncio.wait(
                {relay_task, stop_task}, return_when=asyncio.FIRST_COMPLETED
            )
        finally:
            stop_task.cancel()
        if relay_task.done():
            # relay_loop only ends by raising; surface it.
            relay_task.result()
        logger.info("Standalone outbox relay stopping: service=%s", service_name)
    finally:
        if relay_task is not None and not relay_task.done():
            relay_task.cancel()
            try:
                await relay_task
            except asyncio.CancelledError:
                pass
        try:
            await broker.stop()
            logger.info("Standalone outbox relay stopped: service=%s", service_name)
        finally:
            for sig in _STOP_SIGNALS:
                loop.remove_signal_handler(sig)
                if previous[sig] is not None:
                    signal.signal(sig, previous[sig])
