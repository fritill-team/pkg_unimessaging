"""``verbatim_namespaces`` publishes a namespace's rows under ``aggregate_type`` alone.

Both relays share the matrix: rows outside a configured namespace keep the
``<subject_prefix>.`` prefix, and matching is on whole dot-separated tokens.
"""

import asyncio
import uuid

import pytest

from unimessaging.adapters.in_memory_broker import InMemoryBrokerAdapter
from unimessaging.outbox import OutboxRelay

from test_relay_msg_id import _ArticlesLikeRelay, _Connection, _row, _session_factory

MATRIX = [
    ((), "notifications.trigger.k", "articles.notifications.trigger.k"),
    (("notifications",), "notifications.trigger.k", "notifications.trigger.k"),
    (("notifications",), "notifications", "notifications"),
    (("notifications",), "article.published", "articles.article.published"),
    (("notifications",), "notificationsx.y", "articles.notificationsx.y"),
    (("notifications.",), "notifications.trigger.k", "notifications.trigger.k"),
]


def _publish_async(rows, **kwargs):
    broker = InMemoryBrokerAdapter()
    relay = OutboxRelay(
        _session_factory(rows), broker, subject_prefix="articles", **kwargs
    )
    asyncio.run(relay.process_batch())
    return broker._published


def _publish_django(monkeypatch, rows, **kwargs):
    from unimessaging.outbox_django import relay as django_relay

    monkeypatch.setattr(django_relay, "connection", _Connection(rows))
    broker = InMemoryBrokerAdapter()
    relay = django_relay.DjangoOutboxRelay(broker, subject_prefix="articles", **kwargs)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        relay.process_batch()
    finally:
        loop.close()
        asyncio.set_event_loop(None)
    return broker._published


@pytest.mark.parametrize("namespaces,aggregate_type,expected", MATRIX)
def test_async_relay_subject(namespaces, aggregate_type, expected):
    published = _publish_async(
        [_row(uuid.uuid4(), aggregate_type)], verbatim_namespaces=namespaces
    )
    assert published[0][0] == expected


@pytest.mark.parametrize("namespaces,aggregate_type,expected", MATRIX)
def test_django_relay_subject(monkeypatch, namespaces, aggregate_type, expected):
    published = _publish_django(
        monkeypatch, [_row(uuid.uuid4(), aggregate_type)], verbatim_namespaces=namespaces
    )
    assert published[0][0] == expected


def test_option_omitted_keeps_prefix(monkeypatch):
    row = _row(uuid.uuid4(), "notifications.trigger.k")
    assert _publish_async([row])[0][0] == "articles.notifications.trigger.k"
    assert _publish_django(monkeypatch, [row])[0][0] == "articles.notifications.trigger.k"


def test_verbatim_row_keeps_row_id_as_msg_id(monkeypatch):
    row_id = uuid.uuid4()
    row = _row(row_id, "notifications.trigger.k")
    for published in (
        _publish_async([row], verbatim_namespaces=("notifications",)),
        _publish_django(monkeypatch, [row], verbatim_namespaces=("notifications",)),
    ):
        subject, _payload, headers = published[0]
        assert subject == "notifications.trigger.k"
        assert headers == {"Nats-Msg-Id": str(row_id)}


@pytest.mark.parametrize("bad", [("",), (".",), ("notifications", "")])
def test_empty_namespace_rejected(bad):
    from unimessaging.outbox_django.relay import DjangoOutboxRelay

    with pytest.raises(ValueError):
        OutboxRelay(_session_factory([]), object(), subject_prefix="a", verbatim_namespaces=bad)
    with pytest.raises(ValueError):
        DjangoOutboxRelay(object(), subject_prefix="a", verbatim_namespaces=bad)


@pytest.mark.parametrize("bad", ["notifications", b"notifications", (None,), (b"x",)])
def test_non_string_namespaces_rejected(bad):
    from unimessaging.outbox_django.relay import DjangoOutboxRelay

    with pytest.raises(TypeError):
        OutboxRelay(_session_factory([]), object(), subject_prefix="a", verbatim_namespaces=bad)
    with pytest.raises(TypeError):
        DjangoOutboxRelay(object(), subject_prefix="a", verbatim_namespaces=bad)


def test_build_subject_override_still_wins():
    broker = InMemoryBrokerAdapter()
    relay = _ArticlesLikeRelay(
        _session_factory([_row(uuid.uuid4(), "articles.article.published")]),
        broker,
        subject_prefix="articles",
        verbatim_namespaces=("notifications",),
    )
    asyncio.run(relay.process_batch())
    assert broker._published[0][0] == "articles.article.published"
