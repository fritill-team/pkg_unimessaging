"""`pull_consume` must pin `deliver_policy` at bind time when asked, and must
otherwise bind exactly as it always has.

Why this exists: the `estate:AD-15` durable collapses rename durables, and a
renamed durable is a brand-new NATS consumer. Without a pinned `deliver_policy`
the new consumer replays the whole retained stream through handlers that have
already deleted those rows. These tests verify the pin reaches `pull_subscribe`
at its call site — not that a manifest declares it.
"""

import asyncio

import pytest
from nats.js.api import ConsumerConfig, DeliverPolicy

from unimessaging.broker.broker import UnifiedMessageBroker
from unimessaging.broker.config import JetStreamConsumer, MessagingConfig
from unimessaging.adapters.nats.async_adapter import NATSAdapter


class _FakeJetStream:
    """Records the bind, then cancels so pull_consume's loop exits.

    pull_consume swallows generic exceptions and retries forever, so a plain
    error cannot end the loop from a test — only CancelledError, which the outer
    handler re-raises, does. Raising it here means the bind we care about has
    already been recorded before the loop unwinds.
    """

    def __init__(self):
        self.calls = []

    async def pull_subscribe(self, subject, durable=None, config=None, **kwargs):
        self.calls.append({"subject": subject, "durable": durable, "config": config})
        raise asyncio.CancelledError


def _adapter_with_fake_js():
    adapter = NATSAdapter(MessagingConfig())
    adapter.js = _FakeJetStream()
    return adapter


async def _handler(_data, _meta):  # pragma: no cover - never invoked here
    return None


@pytest.mark.asyncio
async def test_omitting_deliver_policy_binds_bare_as_before():
    adapter = _adapter_with_fake_js()
    with pytest.raises(asyncio.CancelledError):
        await adapter.pull_consume("courses.course.*", "d1", _handler)
    call = adapter.js.calls[0]
    assert call["durable"] == "d1"
    # No policy asked for -> no ConsumerConfig -> nats-py keeps its historical
    # default (config=None is identical to omitting the argument). Existing
    # consumers are untouched.
    assert call["config"] is None


@pytest.mark.asyncio
async def test_deliver_policy_is_threaded_into_a_consumer_config():
    adapter = _adapter_with_fake_js()
    with pytest.raises(asyncio.CancelledError):
        await adapter.pull_consume(
            "courses.course.*", "d2", _handler, deliver_policy="new"
        )
    cfg = adapter.js.calls[0]["config"]
    assert isinstance(cfg, ConsumerConfig)
    assert cfg.deliver_policy == DeliverPolicy.NEW


@pytest.mark.asyncio
async def test_unknown_deliver_policy_is_rejected_before_binding():
    adapter = _adapter_with_fake_js()
    with pytest.raises(ValueError):
        await adapter.pull_consume(
            "courses.course.*", "d3", _handler, deliver_policy="whenever"
        )
    # Rejected while building the config, so nothing was bound.
    assert adapter.js.calls == []


class _CaptureClient:
    """Records what the broker asks pull_consume for."""

    def __init__(self):
        self.pull_consumes = []
        self.started = False

    async def start(self):
        self.started = True

    async def stop(self):
        self.started = False

    async def pull_consume(self, **kwargs):
        self.pull_consumes.append(kwargs)
        await asyncio.sleep(3600)  # a real pull consumer never returns


@pytest.mark.asyncio
async def test_broker_threads_consumer_deliver_policy_to_pull_consume():
    """A JetStreamConsumer's deliver_policy must reach the bind, so a collapse
    can pin an env/registry-declared durable, not only a direct pull_consume."""
    client = _CaptureClient()
    consumer = JetStreamConsumer(
        label="courses",
        subject="taxonomy.category.*",
        durable="courses-taxonomy-category-consumer-v2",
        deliver_policy="new",
    )
    broker = UnifiedMessageBroker(
        client=client, subjects=[], service_name="courses", consumers=[consumer]
    )
    await broker.start()
    await asyncio.sleep(0)  # let the consumer task reach pull_consume
    assert client.pull_consumes[0]["deliver_policy"] == "new"
    await broker.stop()


@pytest.mark.asyncio
async def test_broker_defaults_consumer_deliver_policy_to_none():
    """An unset deliver_policy stays None end-to-end, preserving the bare bind."""
    client = _CaptureClient()
    consumer = JetStreamConsumer(
        label="orders", subject="orders.order", durable="courses-orders-order-consumer"
    )
    broker = UnifiedMessageBroker(
        client=client, subjects=[], service_name="courses", consumers=[consumer]
    )
    await broker.start()
    await asyncio.sleep(0)
    assert client.pull_consumes[0]["deliver_policy"] is None
    await broker.stop()
