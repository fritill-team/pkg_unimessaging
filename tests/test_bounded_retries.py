"""Opt-in bounded retries and terminate-in-place (estate:AD-22).

A durable that declares ``RetryLimits`` carries ``max_deliver``/``ack_wait``
(never server ``backoff``), naks failures with a client-side delay, caps each
handler call below ``ack_wait``, and on the server's final delivery ``term()``s
a failing message and names it once in the error log without its payload. The
message stays in the stream. Delivery-safety cases run against a real
``nats-server`` (NFR4).
"""

import asyncio
import contextlib
import logging

import pytest
from nats.js.api import AckPolicy, DeliverPolicy
from nats.js.client import JetStreamContext
from nats.js.errors import APIError, NotFoundError

from unimessaging.adapters.nats.async_adapter import ConsumerLimitsRefused
from unimessaging.broker.broker import UnifiedMessageBroker
from unimessaging.broker.client import UnifiedMessaging
from unimessaging.broker.config import JetStreamConsumer, MessagingConfig, RetryLimits
from unimessaging.broker.registry import HandlerRegistry

STREAM = "RETRYTEST"
SUBJECT = "retry.event"
DURABLE = "retry-durable"
MARKER = "learner-secret-marker"


async def _wait_for(predicate, timeout: float = 10.0) -> None:
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
            name="retry-test",
            enable_durable=True,
            stream_name=STREAM,
            stream_subjects=["retry.>"],
        )
    )
    await m.start()
    try:
        yield m
    finally:
        await m.adapter.nc.close()


@contextlib.asynccontextmanager
async def _consuming(m, handler, **kwargs):
    task = asyncio.create_task(
        m.pull_consume(SUBJECT, DURABLE, handler, timeout=0.2, **kwargs)
    )
    try:
        yield task
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


async def _info(m):
    return await m.adapter.js.consumer_info(STREAM, DURABLE)


async def _wait_info(m, timeout: float = 5.0):
    """Wait for pull_consume to create the durable; only NotFound is retried."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while True:
        try:
            return (await _info(m)).config
        except NotFoundError:
            if loop.time() > deadline:
                raise TimeoutError("durable was never created")
            await asyncio.sleep(0.05)


# ── RetryLimits validation (unit) ────────────────────────────────────


def test_retry_limits_defaults():
    limits = RetryLimits()
    assert limits.max_deliver == 5
    assert limits.ack_wait == 60.0
    assert limits.nak_delays == (1.0, 5.0, 30.0, 120.0)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"max_deliver": -1},
        {"max_deliver": 0},
        {"max_deliver": True},
        {"max_deliver": 2.5},
        {"ack_wait": 0},
        {"ack_wait": -1.0},
        {"ack_wait": True},
        {"ack_wait": "60"},
        {"ack_wait": float("nan")},
        {"ack_wait": float("inf")},
        {"nak_delays": ()},
        {"nak_delays": []},
        {"nak_delays": "15"},
        {"nak_delays": (1.0, -0.5)},
        {"nak_delays": (1.0, float("nan"))},
        {"nak_delays": (float("inf"),)},
        {"nak_delays": (True,)},
        {"nak_delays": ("1",)},
    ],
)
def test_retry_limits_refuses_unbounded_or_invalid(kwargs):
    with pytest.raises(ValueError):
        RetryLimits(**kwargs)


def test_retry_limits_accepts_zero_delay_and_list():
    assert RetryLimits(ack_wait=1, nak_delays=[0, 2]).nak_delays == (0, 2)


def test_consumer_limits_default_none_and_parse_many_untouched():
    (c,) = JetStreamConsumer.parse_many(["a|retry.event|d"])
    assert c.limits is None


# ── Real NATS ────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_unbounded_consumer_binds_as_before(nats_server):
    seen = []

    async def handler(data, meta):
        seen.append(data)
        if len(seen) == 1:
            raise RuntimeError("boom")

    async with _messaging(nats_server) as m:
        await m.publish(SUBJECT, {"i": 1})
        async with _consuming(m, handler):
            # Immediate nak: the second delivery follows without a delay.
            await _wait_for(lambda: len(seen) == 2, timeout=3.0)
        cfg = (await _info(m)).config
        assert cfg.max_deliver == -1
        assert cfg.ack_wait == 30.0
        assert not cfg.backoff


@pytest.mark.asyncio
async def test_new_bounded_durable_carries_limits_and_no_backoff(nats_server):
    async with _messaging(nats_server) as m:
        limits = RetryLimits(max_deliver=4, ack_wait=7.0)
        async with _consuming(m, lambda d, meta: asyncio.sleep(0), limits=limits):
            cfg = await _wait_info(m)
        assert cfg.max_deliver == 4
        assert cfg.ack_wait == 7.0
        assert not cfg.backoff


@pytest.mark.asyncio
async def test_always_failing_message_terminated_in_place(nats_server, caplog):
    handler_log = logging.getLogger("unimessaging.nats.testhandler")
    deliveries = []

    async def handler(data, meta):
        loop = asyncio.get_running_loop()
        deliveries.append(loop.time())
        handler_log.info("handler-call %d", len(deliveries))
        raise ValueError("handler bug")

    caplog.set_level(logging.DEBUG, logger="unimessaging.nats")
    async with _messaging(nats_server) as m:
        ack = await m.adapter.js.publish(SUBJECT, f'{{"who":"{MARKER}"}}'.encode())
        limits = RetryLimits(max_deliver=3, ack_wait=2.0, nak_delays=(0.3, 0.5))
        async with _consuming(m, handler, limits=limits):
            await _wait_for(lambda: len(deliveries) == 3)
            # Past the last delay and ack_wait: no fourth delivery.
            await asyncio.sleep(2.5)
        assert len(deliveries) == 3
        # Each redelivery waits at least the configured nak delay.
        assert deliveries[1] - deliveries[0] >= 0.3
        assert deliveries[2] - deliveries[1] >= 0.5

        info = await _info(m)
        assert info.num_ack_pending == 0
        stored = await m.adapter.js.get_msg(STREAM, ack.seq)
        assert MARKER.encode() in stored.data

    msgs = [r.getMessage() for r in caplog.records]
    finals = [i for i, t in enumerate(msgs) if "final delivery" in t]
    errors = [r for r in caplog.records if r.levelno >= logging.ERROR]
    naks = [t for t in msgs if "JetStream nak" in t]
    assert len(finals) == 1 and "delivered=3" in msgs[finals[0]]
    # The final-delivery line is logged before the handler's last call.
    assert finals[0] < msgs.index("handler-call 3")
    assert msgs.index("handler-call 2") < finals[0]
    assert len(naks) == 2
    assert "delay=0.3s" in naks[0]
    assert "delay=0.5s" in naks[1]
    assert len(errors) == 1
    line = errors[0].getMessage()
    for part in (
        f"stream={STREAM}",
        f"seq={ack.seq}",
        f"subject={SUBJECT}",
        f"durable={DURABLE}",
        "delivered=3",
        "error=ValueError",
    ):
        assert part in line
    assert all(MARKER not in t for t in msgs)


@pytest.mark.asyncio
async def test_last_delay_reused_for_later_deliveries(nats_server, caplog):
    calls = []

    async def handler(data, meta):
        calls.append(1)
        raise RuntimeError("always")

    caplog.set_level(logging.WARNING, logger="unimessaging.nats")
    async with _messaging(nats_server) as m:
        await m.publish(SUBJECT, {"i": 1})
        limits = RetryLimits(max_deliver=4, ack_wait=1.0, nak_delays=(0.1,))
        async with _consuming(m, handler, limits=limits):
            await _wait_for(
                lambda: any(r.levelno >= logging.ERROR for r in caplog.records)
            )
        assert len(calls) == 4
    naks = [r.getMessage() for r in caplog.records if "JetStream nak" in r.getMessage()]
    assert len(naks) == 3
    assert all("delay=0.1s" in t for t in naks)
    errors = [r for r in caplog.records if r.levelno >= logging.ERROR]
    assert len(errors) == 1 and "delivered=4" in errors[0].getMessage()


@pytest.mark.asyncio
async def test_queued_batch_messages_not_redelivered_by_ack_wait(nats_server):
    ack_wait = 1.0
    delivered = []

    async def handler(data, meta):
        delivered.append(data)
        await asyncio.sleep(0.6 * ack_wait)

    async with _messaging(nats_server) as m:
        for i in range(4):
            await m.publish(SUBJECT, {"i": i})
        limits = RetryLimits(max_deliver=5, ack_wait=ack_wait, nak_delays=(0.1,))
        # Without per-call progress the fourth message waits 1.8s > ack_wait.
        async with _consuming(m, handler, batch=4, limits=limits):
            await _wait_for(lambda: len(delivered) >= 4)
            for _ in range(100):
                if (await _info(m)).ack_floor.stream_seq == 4:
                    break
                await asyncio.sleep(0.05)
            await asyncio.sleep(0.5)
        info = await _info(m)
    assert len(delivered) == 4
    assert len(set(delivered)) == 4
    assert info.ack_floor.stream_seq == 4
    assert info.num_redelivered == 0


@pytest.mark.asyncio
async def test_new_bounded_durable_with_deliver_policy_new(nats_server):
    async with _messaging(nats_server) as m:
        limits = RetryLimits(max_deliver=3, ack_wait=4.0)
        async with _consuming(
            m, lambda d, meta: asyncio.sleep(0), limits=limits, deliver_policy="new"
        ):
            cfg = await _wait_info(m)
    assert cfg.deliver_policy == DeliverPolicy.NEW
    assert cfg.max_deliver == 3
    assert cfg.ack_wait == 4.0
    assert not cfg.backoff


@pytest.mark.asyncio
async def test_final_delivery_success_is_acked(nats_server, caplog):
    calls = []

    async def handler(data, meta):
        calls.append(1)
        if len(calls) < 2:
            raise RuntimeError("transient")

    caplog.set_level(logging.WARNING, logger="unimessaging.nats")
    async with _messaging(nats_server) as m:
        await m.publish(SUBJECT, {"i": 1})
        limits = RetryLimits(max_deliver=2, ack_wait=1.0, nak_delays=(0.1,))
        async with _consuming(m, handler, limits=limits):
            await _wait_for(lambda: len(calls) == 2)
            for _ in range(100):
                if (await _info(m)).ack_floor.stream_seq == 1:
                    break
                await asyncio.sleep(0.05)
        assert (await _info(m)).ack_floor.stream_seq == 1
    assert any("final delivery" in r.getMessage() for r in caplog.records)
    assert not [r for r in caplog.records if r.levelno >= logging.ERROR]


@pytest.mark.asyncio
async def test_hanging_handler_counted_as_failed(nats_server, caplog):
    calls = []

    async def handler(data, meta):
        calls.append(1)
        await asyncio.sleep(3600)

    caplog.set_level(logging.WARNING, logger="unimessaging.nats")
    async with _messaging(nats_server) as m:
        await m.publish(SUBJECT, {"i": 1})
        limits = RetryLimits(max_deliver=2, ack_wait=1.0, nak_delays=(0.1,))
        async with _consuming(m, handler, limits=limits):
            await _wait_for(lambda: len(calls) == 2)
            await _wait_for(
                lambda: any(r.levelno >= logging.ERROR for r in caplog.records)
            )
            await asyncio.sleep(1.5)
        assert len(calls) == 2
        assert (await _info(m)).num_ack_pending == 0
    errors = [r for r in caplog.records if r.levelno >= logging.ERROR]
    assert len(errors) == 1
    assert "error=TimeoutError" in errors[0].getMessage()


@pytest.mark.asyncio
async def test_existing_deliver_new_durable_updated_in_place(nats_server):
    async with _messaging(nats_server) as m:
        await m.adapter.js.add_consumer(
            STREAM,
            durable_name=DURABLE,
            deliver_policy=DeliverPolicy.NEW,
            ack_policy=AckPolicy.EXPLICIT,
            filter_subject=SUBJECT,
        )
        before = (await _info(m)).config
        assert before.max_deliver == -1

    registry = HandlerRegistry()
    registry.register_handler("retry.>", lambda payload, subject: None)
    broker = UnifiedMessageBroker(
        service_name="retry-broker",
        url=nats_server,
        enable_durable=True,
        stream_name=STREAM,
        stream_subjects=["retry.>"],
        consumers=[
            JetStreamConsumer(
                label="retry",
                subject=SUBJECT,
                durable=DURABLE,
                limits=RetryLimits(),
            )
        ],
        pull_timeout=0.2,
        registry=registry,
    )
    await broker.start()
    try:
        after = (
            await broker.client.adapter.js.consumer_info(STREAM, DURABLE)
        ).config
    finally:
        await broker.stop()
    assert after.max_deliver == 5
    assert after.ack_wait == 60.0
    assert not after.backoff
    assert after.deliver_policy == DeliverPolicy.NEW
    assert after.filter_subject == SUBJECT
    assert after.ack_policy == before.ack_policy


@pytest.mark.asyncio
async def test_ensure_limits_noop_when_already_applied(nats_server, monkeypatch):
    async with _messaging(nats_server) as m:
        limits = RetryLimits(max_deliver=3, ack_wait=2.5)
        await m.adapter.js.add_consumer(
            STREAM,
            durable_name=DURABLE,
            ack_policy=AckPolicy.EXPLICIT,
            filter_subject=SUBJECT,
            max_deliver=3,
            ack_wait=2.5,
        )

        async def fail(*a, **kw):
            raise AssertionError("add_consumer must not be called")

        monkeypatch.setattr(m.adapter.js, "add_consumer", fail)
        await m.ensure_consumer_limits(SUBJECT, DURABLE, limits)


@pytest.mark.asyncio
async def test_ensure_limits_skips_missing_durable_and_stream(nats_server, caplog):
    caplog.set_level(logging.WARNING, logger="unimessaging.nats")
    async with _messaging(nats_server) as m:
        await m.ensure_consumer_limits(SUBJECT, "absent-durable", RetryLimits())
        await m.ensure_consumer_limits("nostream.x", DURABLE, RetryLimits())
        with pytest.raises(NotFoundError):
            await m.adapter.js.consumer_info(STREAM, "absent-durable")
    assert any("stream not found" in r.getMessage() for r in caplog.records)


@pytest.mark.asyncio
async def test_refused_update_fails_broker_start(nats_server, monkeypatch):
    async with _messaging(nats_server) as m:
        await m.adapter.js.add_consumer(
            STREAM,
            durable_name=DURABLE,
            ack_policy=AckPolicy.EXPLICIT,
            filter_subject=SUBJECT,
        )

    async def refuse(self, *a, **kw):
        raise APIError(code=400, description="consumer update refused")

    monkeypatch.setattr(JetStreamContext, "add_consumer", refuse)
    broker = UnifiedMessageBroker(
        service_name="retry-broker",
        url=nats_server,
        enable_durable=True,
        stream_name=STREAM,
        stream_subjects=["retry.>"],
        consumers=[
            JetStreamConsumer(
                label="retry", subject=SUBJECT, durable=DURABLE, limits=RetryLimits()
            )
        ],
        registry=HandlerRegistry(),
    )
    with pytest.raises(ConsumerLimitsRefused, match=DURABLE):
        await broker.start()
    assert not broker._started
    assert not broker._consumer_tasks
    assert broker.client.adapter.nc.is_closed


@pytest.mark.asyncio
async def test_refused_start_propagates_despite_failing_stop(nats_server, monkeypatch):
    async def refuse(self, *a, **kw):
        raise ConsumerLimitsRefused("durable 'x' refused")

    broker = UnifiedMessageBroker(
        service_name="retry-broker",
        url=nats_server,
        enable_durable=True,
        stream_name=STREAM,
        stream_subjects=["retry.>"],
        consumers=[
            JetStreamConsumer(
                label="retry", subject=SUBJECT, durable="x", limits=RetryLimits()
            )
        ],
        registry=HandlerRegistry(),
    )
    monkeypatch.setattr(type(broker.client.adapter), "ensure_consumer_limits", refuse)

    async def bad_stop():
        await broker.client.adapter.nc.close()
        raise RuntimeError("stop failed")

    monkeypatch.setattr(broker.client, "stop", bad_stop)
    with pytest.raises(ConsumerLimitsRefused):
        await broker.start()


@pytest.mark.asyncio
async def test_stored_backoff_refused(nats_server, monkeypatch):
    async with _messaging(nats_server) as m:
        await m.adapter.js.add_consumer(
            STREAM,
            durable_name=DURABLE,
            ack_policy=AckPolicy.EXPLICIT,
            filter_subject=SUBJECT,
        )
        real_info = m.adapter.js.consumer_info

        async def info_with_backoff(stream, durable, *a, **kw):
            info = await real_info(stream, durable, *a, **kw)
            info.config.backoff = [1.0, 2.0]
            return info

        monkeypatch.setattr(m.adapter.js, "consumer_info", info_with_backoff)
        with pytest.raises(ConsumerLimitsRefused, match="backoff"):
            await m.ensure_consumer_limits(SUBJECT, DURABLE, RetryLimits())


@pytest.mark.asyncio
async def test_differing_deliver_policy_logged_and_kept(nats_server, caplog):
    caplog.set_level(logging.WARNING, logger="unimessaging.nats")
    async with _messaging(nats_server) as m:
        await m.adapter.js.add_consumer(
            STREAM,
            durable_name=DURABLE,
            deliver_policy=DeliverPolicy.NEW,
            ack_policy=AckPolicy.EXPLICIT,
            filter_subject=SUBJECT,
        )
        await m.ensure_consumer_limits(
            SUBJECT, DURABLE, RetryLimits(), deliver_policy="all"
        )
        cfg = (await _info(m)).config
    assert cfg.deliver_policy == DeliverPolicy.NEW
    assert cfg.max_deliver == 5
    line = next(r.getMessage() for r in caplog.records if "deliver_policy" in r.getMessage())
    assert DURABLE in line and "declared=all" in line and "stored=new" in line


@pytest.mark.asyncio
async def test_direct_bind_warns_when_limits_not_applied(nats_server, caplog):
    caplog.set_level(logging.WARNING, logger="unimessaging.nats")
    async with _messaging(nats_server) as m:
        await m.adapter.js.add_consumer(
            STREAM,
            durable_name=DURABLE,
            ack_policy=AckPolicy.EXPLICIT,
            filter_subject=SUBJECT,
        )
        async with _consuming(
            m, lambda d, meta: asyncio.sleep(0), limits=RetryLimits()
        ):
            await _wait_for(
                lambda: any(
                    "limits not applied" in r.getMessage() for r in caplog.records
                )
            )
    line = next(
        r.getMessage() for r in caplog.records if "limits not applied" in r.getMessage()
    )
    assert DURABLE in line


# ── Broker forwarding (no server) ────────────────────────────────────


class _CaptureClient:
    def __init__(self):
        self.pull_consumes = []
        self.ensured = []

    async def start(self):
        pass

    async def stop(self):
        pass

    async def ensure_consumer_limits(self, *args, **kwargs):
        self.ensured.append((args, kwargs))

    async def pull_consume(self, **kwargs):
        self.pull_consumes.append(kwargs)
        await asyncio.sleep(3600)


@pytest.mark.asyncio
async def test_broker_forwards_limits_only_when_set():
    limits = RetryLimits(max_deliver=3)
    client = _CaptureClient()
    broker = UnifiedMessageBroker(
        client=client,
        subjects=[],
        service_name="svc",
        consumers=[
            JetStreamConsumer(label="a", subject="a.x", durable="a-d", limits=limits),
            JetStreamConsumer(label="b", subject="b.x", durable="b-d"),
        ],
    )
    await broker.start()
    await asyncio.sleep(0)
    try:
        by_durable = {c["durable"]: c for c in client.pull_consumes}
        assert by_durable["a-d"]["limits"] is limits
        assert "limits" not in by_durable["b-d"]
        assert client.ensured == [
            (("a.x", "a-d", limits), {"deliver_policy": None})
        ]
    finally:
        await broker.stop()


@pytest.mark.asyncio
async def test_broker_never_ensures_limits_for_unbounded_consumers():
    client = _CaptureClient()
    broker = UnifiedMessageBroker(
        client=client,
        subjects=[],
        service_name="svc",
        consumers=[JetStreamConsumer(label="b", subject="b.x", durable="b-d")],
    )
    await broker.start()
    await asyncio.sleep(0)
    try:
        assert client.ensured == []
        assert "limits" not in client.pull_consumes[0]
    finally:
        await broker.stop()
