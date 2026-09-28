"""NATS credentials can be supplied outside the URL, without changing the URL-only path.

Why this exists: the NATS auth cutover lets a service move its credentials out
of ``NATS_URL`` into ``NATS_USER``/``NATS_PASSWORD`` (or a token or creds file).
The rollout bumps every service first with credentials still in the URL, so a
config that sets none of the new fields must make exactly the connect() call
it made before. These tests pin that, the env fallback in the client factory,
the pass-through from every entry point, and the ERROR level for permission
violations.
"""

import logging

import pytest

from unimessaging.adapters.nats import async_adapter
from unimessaging.adapters.nats.async_adapter import NATSAdapter
from unimessaging.broker import broker as broker_mod
from unimessaging.broker.broker import UnifiedMessageBroker
from unimessaging.broker.config import MessagingConfig
from unimessaging.broker.utils import create_messaging_client
from unimessaging.integrations.django import startup as django_startup
from unimessaging.integrations.fastapi import startup as fastapi_startup

CRED_ENV = ("NATS_USER", "NATS_PASSWORD", "NATS_TOKEN", "NATS_CREDS_FILE")
URL_ONLY_KWARGS = {
    "name",
    "max_reconnect_attempts",
    "reconnect_time_wait",
    "disconnected_cb",
    "reconnected_cb",
    "closed_cb",
    "error_cb",
}


@pytest.fixture(autouse=True)
def _clear_cred_env(monkeypatch):
    for name in CRED_ENV:
        monkeypatch.delenv(name, raising=False)


class _FakeNATS:
    def __init__(self):
        self.connect_args = None

    async def connect(self, *args, **kwargs):
        self.connect_args = (args, kwargs)


async def _connect(monkeypatch, cfg):
    fake = _FakeNATS()
    monkeypatch.setattr(async_adapter, "NATS", lambda: fake)
    await NATSAdapter(cfg).start()
    return fake.connect_args


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "url",
    [
        "nats://localhost:4222",
        "tls://nats.internal:4222",
        "tls://svc:s3cret@nats.internal:4222",
    ],
)
async def test_url_only_config_makes_the_unchanged_connect_call(monkeypatch, url):
    args, kwargs = await _connect(monkeypatch, MessagingConfig(url=url, name="svc"))

    assert args == (url,)
    assert set(kwargs) == URL_ONLY_KWARGS


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("fields", "expected"),
    [
        ({"user": "svc", "password": "pw"}, {"user": "svc", "password": "pw"}),
        ({"token": "t0ken"}, {"token": "t0ken"}),
        ({"creds_file": "/etc/nats/svc.creds"}, {"user_credentials": "/etc/nats/svc.creds"}),
    ],
)
async def test_each_credential_reaches_connect_only_when_set(
    monkeypatch, fields, expected
):
    cfg = MessagingConfig(url="tls://nats.internal:4222", name="svc", **fields)
    args, kwargs = await _connect(monkeypatch, cfg)

    assert args == ("tls://nats.internal:4222",)
    assert set(kwargs) == URL_ONLY_KWARGS | set(expected)
    assert {k: kwargs[k] for k in expected} == expected


def test_factory_falls_back_to_nats_env(monkeypatch):
    monkeypatch.setenv("NATS_USER", "svc")
    monkeypatch.setenv("NATS_PASSWORD", "pw")
    monkeypatch.setenv("NATS_TOKEN", "t0ken")
    monkeypatch.setenv("NATS_CREDS_FILE", "/etc/nats/svc.creds")

    cfg = create_messaging_client("svc", url="tls://h:4222").cfg

    assert (cfg.user, cfg.password, cfg.token, cfg.creds_file) == (
        "svc",
        "pw",
        "t0ken",
        "/etc/nats/svc.creds",
    )


def test_factory_explicit_argument_beats_env(monkeypatch):
    monkeypatch.setenv("NATS_USER", "from-env")
    monkeypatch.setenv("NATS_PASSWORD", "env-pw")

    cfg = create_messaging_client(
        "svc", url="tls://h:4222", user="explicit", password="arg-pw"
    ).cfg

    assert (cfg.user, cfg.password) == ("explicit", "arg-pw")


def test_factory_treats_empty_env_as_unset(monkeypatch):
    for name in CRED_ENV:
        monkeypatch.setenv(name, "")

    cfg = create_messaging_client("svc", url="tls://h:4222").cfg

    assert (cfg.user, cfg.password, cfg.token, cfg.creds_file) == (None,) * 4


def test_factory_without_credentials_leaves_them_unset():
    cfg = create_messaging_client("svc", url="tls://h:4222").cfg

    assert (cfg.user, cfg.password, cfg.token, cfg.creds_file) == (None,) * 4


def _capture_factory(monkeypatch):
    seen = {}
    real = broker_mod.create_messaging_client

    def spy(service_name, **kwargs):
        seen.update(kwargs)
        return real(service_name, **kwargs)

    monkeypatch.setattr(broker_mod, "create_messaging_client", spy)

    async def no_start(self):
        return None

    monkeypatch.setattr(UnifiedMessageBroker, "start", no_start)
    return seen


CREDS = {
    "user": "svc",
    "password": "pw",
    "token": "t0ken",
    "creds_file": "/etc/nats/svc.creds",
}


def test_broker_passes_credentials_to_the_factory(monkeypatch):
    seen = _capture_factory(monkeypatch)

    UnifiedMessageBroker(service_name="svc", **CREDS)

    assert {k: seen[k] for k in CREDS} == CREDS


@pytest.mark.asyncio
async def test_fastapi_start_messaging_passes_credentials(monkeypatch):
    seen = _capture_factory(monkeypatch)

    class _State:
        pass

    class _App:
        state = _State()

    await fastapi_startup.start_messaging(
        _App(), subjects=[], service_name="svc", **CREDS
    )

    assert {k: seen[k] for k in CREDS} == CREDS


@pytest.mark.asyncio
async def test_django_start_messaging_passes_credentials(monkeypatch):
    seen = _capture_factory(monkeypatch)
    monkeypatch.setattr(django_startup, "_broker", None)
    monkeypatch.setattr(django_startup, "_client", None)

    await django_startup.start_messaging(subjects=[], service_name="svc", **CREDS)

    assert {k: seen[k] for k in CREDS} == CREDS


@pytest.mark.asyncio
async def test_permission_violation_logs_at_error(caplog):
    adapter = NATSAdapter(MessagingConfig(name="svc"))
    err = Exception('nats: permissions violation for publish to "orders.created"')

    with caplog.at_level(logging.DEBUG, logger="unimessaging"):
        await adapter._on_error(err)

    [record] = caplog.records
    assert record.levelno == logging.ERROR
    assert "permission violation" in record.getMessage()


@pytest.mark.asyncio
async def test_other_async_errors_stay_at_warning(caplog):
    adapter = NATSAdapter(MessagingConfig(name="svc"))

    with caplog.at_level(logging.DEBUG, logger="unimessaging"):
        await adapter._on_error(Exception("nats: slow consumer"))

    [record] = caplog.records
    assert record.levelno == logging.WARNING
