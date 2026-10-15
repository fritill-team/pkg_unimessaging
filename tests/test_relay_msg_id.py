"""Every outbox publish carries ``Nats-Msg-Id`` = the outbox row id.

A relay crash between publishing a batch and committing it republishes the
batch; JetStream drops the repeats inside the stream's duplicate window only if
each publish carries the same ``Nats-Msg-Id`` (estate:AD-22). The dedup case
runs against a real ``nats-server``.
"""

import asyncio
import uuid

import pytest

from unimessaging.adapters.in_memory_broker import InMemoryBrokerAdapter
from unimessaging.broker.client import UnifiedMessaging
from unimessaging.broker.config import MessagingConfig
from unimessaging.outbox import OutboxRelay


def _row(row_id, aggregate_type="thing"):
    return {
        "id": row_id,
        "aggregate_type": aggregate_type,
        "payload": {"k": "v"},
        "retries": 0,
    }


# ── async relay: a fake session that hands back the same rows every batch ──


class _Result:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return [(r["id"],) for r in self._rows]

    def mappings(self):
        return self

    def all(self):
        return self._rows


class _Session:
    def __init__(self, rows):
        self._rows = rows

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    def begin(self):
        return self

    async def execute(self, stmt, params=None):
        return _Result(self._rows)


def _session_factory(rows):
    return lambda: _Session(rows)


class _ArticlesLikeRelay(OutboxRelay):
    """Overrides only the subject, as ``ArticlesOutboxRelay`` does."""

    def _build_subject(self, aggregate_type: str) -> str:
        return aggregate_type


@pytest.mark.asyncio
async def test_outbox_relay_publishes_row_id_as_msg_id():
    row_id = uuid.uuid4()
    broker = InMemoryBrokerAdapter()
    relay = OutboxRelay(_session_factory([_row(row_id)]), broker, subject_prefix="svc")

    assert await relay.process_batch() == 1

    subject, _payload, headers = broker._published[0]
    assert subject == "svc.thing"
    assert headers == {"Nats-Msg-Id": str(row_id)}


@pytest.mark.asyncio
async def test_subject_override_subclass_keeps_msg_id():
    row_id = uuid.uuid4()
    broker = InMemoryBrokerAdapter()
    relay = _ArticlesLikeRelay(
        _session_factory([_row(row_id, "articles.article.published")]),
        broker,
        subject_prefix="articles",
    )

    await relay.process_batch()

    subject, _payload, headers = broker._published[0]
    assert subject == "articles.article.published"
    assert headers["Nats-Msg-Id"] == str(row_id)


# ── Django relay: a fake cursor on the module's connection ──


class _Cursor:
    def __init__(self, rows):
        self._rows = rows
        self._last = None
        self.description = [(c,) for c in rows[0]]

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        self._last = sql

    def fetchall(self):
        if "SELECT id" in self._last:
            return [(r["id"],) for r in self._rows]
        return [tuple(r.values()) for r in self._rows]


class _Connection:
    def __init__(self, rows):
        self._rows = rows

    def cursor(self):
        return _Cursor(self._rows)


def test_django_relay_publishes_row_id_as_msg_id(monkeypatch):
    from unimessaging.outbox_django import relay as django_relay

    row_id = uuid.uuid4()
    monkeypatch.setattr(django_relay, "connection", _Connection([_row(row_id)]))
    broker = InMemoryBrokerAdapter()
    relay = django_relay.DjangoOutboxRelay(broker, subject_prefix="reservations")

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        assert relay.process_batch() == 1
    finally:
        loop.close()
        asyncio.set_event_loop(None)

    subject, _payload, headers = broker._published[0]
    assert subject == "reservations.thing"
    assert headers == {"Nats-Msg-Id": str(row_id)}


# ── real NATS: the same row published twice leaves one stream message ──


@pytest.mark.asyncio
async def test_republished_row_is_deduplicated_by_the_stream(nats_server):
    messaging = UnifiedMessaging(
        MessagingConfig(
            url=nats_server,
            name="dedup-test",
            enable_durable=True,
            stream_name="DEDUPTEST",
            stream_subjects=["dedup.>"],
        )
    )
    await messaging.start()
    try:
        row_id = uuid.uuid4()
        relay = OutboxRelay(
            _session_factory([_row(row_id)]), messaging, subject_prefix="dedup"
        )
        # A crash before commit leaves the row PENDING; the next batch
        # publishes it again.
        assert await relay.process_batch() == 1
        assert await relay.process_batch() == 1

        info = await messaging.adapter.js.stream_info("DEDUPTEST")
        assert info.state.messages == 1

        # A different row is not swallowed by the first one's id.
        other = OutboxRelay(
            _session_factory([_row(uuid.uuid4())]), messaging, subject_prefix="dedup"
        )
        await other.process_batch()
        info = await messaging.adapter.js.stream_info("DEDUPTEST")
        assert info.state.messages == 2
    finally:
        await messaging.stop()
