"""``pull_consume(on_tick=...)`` fires once per fetch iteration, idle or busy.

A consumer process touches its liveness heartbeat from this callback, so it must
fire on empty fetch timeouts too (a stuck loop then goes stale), must run before
each fetch, and a raising callback must not stop messages being handled and
acked. Proven against a real ``nats-server`` (estate:AD-22, NFR4).
"""

import asyncio
import contextlib
import logging

import pytest

from unimessaging.broker.broker import UnifiedMessageBroker
from unimessaging.broker.client import UnifiedMessaging
from unimessaging.broker.config import JetStreamConsumer, MessagingConfig
from unimessaging.broker.registry import HandlerRegistry

STREAM = "TICKTEST"
SUBJECT = "tick.event"


async def _wait_for(predicate, timeout: float = 5.0) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not predicate():
        if loop.time() > deadline:
            raise TimeoutError("timed out waiting for condition")
        await asyncio.sleep(0.02)


@contextlib.asynccontextmanager
async def _messaging(url):
    m = UnifiedMessaging(
        MessagingConfig(
            url=url,
            name="tick-test",
            enable_durable=True,
            stream_name=STREAM,
            stream_subjects=["tick.>"],
        )
    )
    await m.start()
    try:
        yield m
    finally:
        # close, not drain: a fetch cancelled mid-wait leaves its pull inbox
        # open and drain then waits out its 30s timeout.
        await m.adapter.nc.close()


@contextlib.asynccontextmanager
async def _consuming(m, handler, **kwargs):
    task = asyncio.create_task(
        m.pull_consume(SUBJECT, "tick-durable", handler, timeout=0.2, **kwargs)
    )
    try:
        yield task
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


async def _wait_ack_floor(m, seq: int) -> int:
    floor = 0
    for _ in range(100):
        info = await m.adapter.js.consumer_info(STREAM, "tick-durable")
        floor = info.ack_floor.stream_seq
        if floor == seq:
            break
        await asyncio.sleep(0.05)
    return floor


async def _publish(m, n):
    for i in range(n):
        await m.publish(SUBJECT, {"i": i})


@pytest.mark.asyncio
async def test_tick_fires_on_idle_fetch_timeouts(nats_server):
    ticks = []
    async with _messaging(nats_server) as m:
        async with _consuming(m, lambda d, meta: asyncio.sleep(0), on_tick=lambda: ticks.append(1)):
            # Nothing is published: every fetch times out empty, and each
            # iteration still ticks.
            await _wait_for(lambda: len(ticks) >= 3)


@pytest.mark.asyncio
async def test_tick_fires_once_per_busy_iteration_before_fetch(nats_server):
    events = []

    async def handler(data, meta):
        events.append("msg")

    async with _messaging(nats_server) as m:
        await _publish(m, 3)
        async with _consuming(
            m, handler, batch=1, on_tick=lambda: events.append("tick")
        ):
            await _wait_for(lambda: events.count("msg") == 3)
            await _wait_for(lambda: events.count("tick") >= 4)
        # batch=1: every message is fetched by its own iteration, so each is
        # immediately preceded by a tick. An early fetch may time out empty and
        # add an extra tick, so the exact sequence is not fixed.
        assert events[0] == "tick"
        for i, e in enumerate(events):
            if e == "msg":
                assert events[i - 1] == "tick"


@pytest.mark.asyncio
async def test_awaitable_tick_is_awaited(nats_server):
    ticks = []

    async def on_tick():
        ticks.append(1)

    async with _messaging(nats_server) as m:
        async with _consuming(m, lambda d, meta: asyncio.sleep(0), on_tick=on_tick):
            await _wait_for(lambda: len(ticks) >= 2)


@pytest.mark.asyncio
async def test_raising_tick_is_logged_and_messages_still_acked(nats_server, caplog):
    handled = []

    async def handler(data, meta):
        handled.append(data)

    def on_tick():
        raise RuntimeError("heartbeat disk full")

    caplog.set_level(logging.ERROR, logger="unimessaging.nats")
    async with _messaging(nats_server) as m:
        await _publish(m, 3)
        async with _consuming(m, handler, on_tick=on_tick):
            await _wait_for(lambda: len(handled) == 3)
            assert await _wait_ack_floor(m, 3) == 3
    assert any("on_tick callback failed" in r.getMessage() for r in caplog.records)


@pytest.mark.asyncio
async def test_no_tick_consumes_as_before(nats_server):
    handled = []

    async def handler(data, meta):
        handled.append(data)

    async with _messaging(nats_server) as m:
        await _publish(m, 2)
        async with _consuming(m, handler):
            await _wait_for(lambda: len(handled) == 2)
            assert await _wait_ack_floor(m, 2) == 2


@pytest.mark.asyncio
async def test_broker_forwards_on_tick_to_every_pull_loop(nats_server):
    # Each durable runs in its own task; recording the ticking task shows the
    # callback reached both loops, not just one.
    ticking_tasks = set()
    received = []
    registry = HandlerRegistry()
    registry.register_handler("tick.>", lambda payload, subject: received.append(subject))

    broker = UnifiedMessageBroker(
        service_name="tick-broker",
        url=nats_server,
        enable_durable=True,
        stream_name=STREAM,
        stream_subjects=["tick.>"],
        consumers=[
            JetStreamConsumer(label="tick", subject=SUBJECT, durable="tick-durable"),
            JetStreamConsumer(label="tock", subject="tick.other", durable="tock-durable"),
        ],
        pull_timeout=0.2,
        registry=registry,
        on_tick=lambda: ticking_tasks.add(asyncio.current_task()),
    )
    await broker.start()
    try:
        await broker.publish(SUBJECT, {"i": 1})
        await broker.publish("tick.other", {"i": 2})
        await _wait_for(lambda: sorted(received) == [SUBJECT, "tick.other"])
        await _wait_for(lambda: set(broker._consumer_tasks) <= ticking_tasks)
        assert len(broker._consumer_tasks) == 2
    finally:
        await broker.stop()
