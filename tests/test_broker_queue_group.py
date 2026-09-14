"""Replica-safety regressions for the broker's core-NATS surface.

Covers B2b (the removed ``notifications.>`` fallback) and B2c (queue groups) of
``.agent/planning/replica-safety-nats-investigation.md``. Both are invisible at
one replica and only misbehave at N>1, so they need tests rather than a live
cluster to stay fixed.
"""

import asyncio
from uuid import uuid4

import pytest
from nats.errors import NoServersError

from unimessaging.broker.broker import UnifiedMessageBroker
from unimessaging.broker.config import JetStreamConsumer
from unimessaging.broker.registry import HandlerRegistry


class FakeClient:
    """Records what the broker asks of the transport."""

    def __init__(self):
        self.subscribes = []
        self.replies = []
        self.pull_consumes = []
        self.started = False

    async def start(self):
        self.started = True

    async def stop(self):
        self.started = False

    async def subscribe(self, subject, handler, queue=None):
        self.subscribes.append((subject, queue))

    async def reply(self, subject, fn, queue=None):
        self.replies.append((subject, queue))

    async def pull_consume(self, **kwargs):
        self.pull_consumes.append(kwargs)
        # A real pull consumer never returns; the broker runs it as a task.
        await asyncio.sleep(3600)


def _broker(**kwargs):
    client = FakeClient()
    return UnifiedMessageBroker(client=client, **kwargs), client


@pytest.mark.asyncio
@pytest.mark.parametrize("subjects", [None, [], ["", " "]])
async def test_empty_subjects_do_not_fall_back_to_the_notifications_tree(subjects):
    broker, client = _broker(subjects=subjects, service_name="courses")
    await broker.start()
    assert broker.subjects == []
    assert client.subscribes == []
    # Still connected: a broker with nothing inbound may still publish or serve
    # RPC, and an unconnected client fails on None at the first call.
    assert client.started is True


@pytest.mark.asyncio
async def test_omitted_subjects_do_not_fall_back_to_the_notifications_tree():
    broker, client = _broker(service_name="courses")
    await broker.start()
    assert broker.subjects == []
    assert client.subscribes == []


@pytest.mark.asyncio
async def test_core_subscriptions_join_a_queue_group_named_for_the_service():
    broker, client = _broker(subjects=["orders.order"], service_name="courses")
    await broker.start()
    assert client.subscribes == [("orders.order", "courses")]


@pytest.mark.asyncio
async def test_an_explicit_queue_group_overrides_the_service_name():
    broker, client = _broker(
        subjects=["orders.order"], service_name="courses", queue_group="courses-readers"
    )
    await broker.start()
    assert client.subscribes == [("orders.order", "courses-readers")]


@pytest.mark.asyncio
async def test_rpc_responders_join_the_queue_group():
    """Without this, N replicas each answer one request and the caller keeps
    the first reply while the rest are published to a dead inbox."""

    async def _handler(payload, meta):
        return {}

    broker, client = _broker(subjects=[], service_name="courses")
    await broker.reply("entitlements.courses.check", _handler)
    assert client.replies == [("entitlements.courses.check", "courses")]


@pytest.mark.asyncio
async def test_a_service_with_only_durables_still_starts_them():
    """The guard in start() tested ``self.subjects`` alone, which was only safe
    because the removed fallback made that list never empty. Most services
    declare consumers and no core subjects."""
    consumer = JetStreamConsumer(
        label="orders", subject="orders.order", durable="courses-orders-order-consumer"
    )
    broker, client = _broker(subjects=[], service_name="courses", consumers=[consumer])
    await broker.start()
    await asyncio.sleep(0)  # let the consumer task reach pull_consume
    assert client.started is True
    assert [c["durable"] for c in client.pull_consumes] == [
        "courses-orders-order-consumer"
    ]
    await broker.stop()


async def _live_brokers(subject, *, registries=None):
    brokers = [
        UnifiedMessageBroker(
            subjects=[subject] if registries else [],
            service_name="queue-transport-itest",
            url="nats://localhost:4222",
            registry=registry,
        )
        for registry in (registries or [None, None])
    ]
    try:
        for broker in brokers:
            await broker.start()
        for broker in brokers:
            await broker.client.adapter.nc.flush()
    except NoServersError as exc:
        for broker in brokers:
            if broker._started:
                await broker.stop()
        pytest.skip(f"nats-server unavailable at nats://localhost:4222: {exc}")
    return brokers


@pytest.mark.asyncio
async def test_live_same_queue_subscribers_process_each_message_once():
    subject = f"itest.queue.subscribe.{uuid4().hex}"
    received = []
    complete = asyncio.Event()

    async def handler(payload, _subject):
        received.append(payload["sequence"])
        if len(received) == 20:
            complete.set()

    registries = [HandlerRegistry(), HandlerRegistry()]
    for registry in registries:
        registry.register_handler(subject, handler)
    brokers = await _live_brokers(subject, registries=registries)
    try:
        for sequence in range(20):
            await brokers[0].publish(subject, {"sequence": sequence})
        await brokers[0].client.adapter.nc.flush()
        await asyncio.wait_for(complete.wait(), timeout=2)
        await asyncio.sleep(0.05)
        assert sorted(received) == list(range(20))
    finally:
        for broker in brokers:
            await broker.stop()


@pytest.mark.asyncio
async def test_live_same_queue_responders_handle_one_request_once():
    subject = f"itest.queue.reply.{uuid4().hex}"
    calls = []

    async def handler(payload, _meta):
        calls.append(payload)
        return {"handled": True}

    brokers = await _live_brokers(subject)
    try:
        for broker in brokers:
            await broker.reply(subject, handler)
            await broker.client.adapter.nc.flush()
        response = await brokers[0].client.request(subject, {"request": 1}, timeout=2)
        await asyncio.sleep(0.05)
        assert response["data"] == b'{"handled": true}'
        assert calls == [{"request": 1}]
    finally:
        for broker in brokers:
            await broker.stop()
