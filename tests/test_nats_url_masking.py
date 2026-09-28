"""No credential embedded in a NATS URL may reach a log line the package writes.

Why this exists: the NATS auth cutover moves every service onto a cluster that
requires credentials, and during the rollout those credentials still ride in
``NATS_URL``. The adapter logs that URL at INFO on every start, so without
masking every pod would print its password. These tests cover the masker's
edge cases and each log site that prints a URL, and pin that ``connect()``
still receives the URL untouched.
"""

import logging

import pytest

from unimessaging.adapters.nats import async_adapter, gateway
from unimessaging.adapters.nats.async_adapter import NATSAdapter
from unimessaging.adapters.nats.gateway import NATSConfig, NATSNotificationGateway
from unimessaging.adapters.nats.redact import mask_url
from unimessaging.broker.config import MessagingConfig
from unimessaging.domain.entities import Message


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        ("tls://svc:s3cret@nats.internal:4222", "tls://svc:***@nats.internal:4222"),
        ("nats://t0ken@nats.internal:4222", "nats://***@nats.internal:4222"),
        (
            "tls://a:p1@h1:4222,tls://b:p2@h2:4222",
            "tls://a:***@h1:4222,tls://b:***@h2:4222",
        ),
        ("nats://u:pw@[::1]:4222", "nats://u:***@[::1]:4222"),
        ("nats://u:p@ss@host:4222", "nats://u:***@host:4222"),
        ("u:pw@host:4222", "u:***@host:4222"),
        ("nats://u:pw@[::1:4222", "nats://***@[::1:4222"),
    ],
)
def test_mask_url_hides_the_secret_and_keeps_the_address(url, expected):
    assert mask_url(url) == expected


@pytest.mark.parametrize(
    "url",
    [
        "nats://localhost:4222",
        "tls://nats.internal:4222",
        "nats://[::1]:4222",
        "nats://h1:4222,nats://h2:4222",
        "",
    ],
)
def test_mask_url_leaves_a_credential_free_url_unchanged(url):
    assert mask_url(url) == url


class _FakeNATS:
    def __init__(self):
        self.connect_args = None
        self.published = []

    async def connect(self, *args, **kwargs):
        self.connect_args = (args, kwargs)

    async def publish(self, subject, data):
        self.published.append((subject, data))

    async def flush(self, timeout=None):
        return None

    async def close(self):
        return None


def _no_secret_logged(caplog, secret):
    assert caplog.records, "expected the package to log something"
    for record in caplog.records:
        assert secret not in record.getMessage()


@pytest.mark.asyncio
async def test_adapter_start_masks_the_url_and_connects_with_it_unchanged(
    monkeypatch, caplog
):
    fake = _FakeNATS()
    monkeypatch.setattr(async_adapter, "NATS", lambda: fake)
    url = "tls://svc:s3cret@nats.internal:4222"
    adapter = NATSAdapter(MessagingConfig(url=url, name="svc"))

    with caplog.at_level(logging.DEBUG, logger="unimessaging"):
        await adapter.start()

    _no_secret_logged(caplog, "s3cret")
    assert any("svc:***@nats.internal:4222" in r.getMessage() for r in caplog.records)
    args, _kwargs = fake.connect_args
    assert args == (url,)


def test_gateway_masks_the_url_at_init_and_connect(monkeypatch, caplog):
    fake = _FakeNATS()
    monkeypatch.setattr(gateway, "NATS", lambda: fake)
    url = "nats://t0ken@nats.internal:4222"

    with caplog.at_level(logging.DEBUG, logger="unimessaging"):
        gw = NATSNotificationGateway(NATSConfig(url=url))
        gw.deliver(Message(content="hello", recipient="u1"))

    _no_secret_logged(caplog, "t0ken")
    connect_lines = [r for r in caplog.records if "Connecting to NATS" in r.getMessage()]
    assert connect_lines and "***@nats.internal:4222" in connect_lines[0].getMessage()
    args, _kwargs = fake.connect_args
    assert args == (url,)
