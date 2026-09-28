"""The standalone outbox relay runner, its heartbeat probe and its opt-out flag.

The runner tests use a real ``nats-server`` and a throwaway PostgreSQL cluster
(``initdb`` into a temp dir), because the properties that matter live there:
which subscriptions the server sees on the relay's connection, and whether a
row claimed with ``FOR UPDATE SKIP LOCKED`` in an interrupted batch is really
released and published afterwards. Both are skipped when the binary is absent.
"""

import asyncio
import glob
import inspect
import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import time
import urllib.request
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pytest
import pytest_asyncio

from unimessaging.outbox import (
    OutboxMixin,
    OutboxRelay,
    relay_in_process,
    relay_loop,
    run_standalone_relay,
)
from unimessaging.outbox import healthcheck

NATS_SERVER = shutil.which("nats-server") or "/usr/local/bin/nats-server"
_PG_BIN = sorted(glob.glob("/usr/lib/postgresql/*/bin"))
PG_BIN = Path(_PG_BIN[-1]) if _PG_BIN else None

SERVICE = "relaytest"
STREAM = "RELAYTEST"
SUBJECTS = ["relaytest.>"]


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_for_port(port: int, timeout: float = 10.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                return
        except OSError:
            time.sleep(0.05)
    raise RuntimeError(f"port {port} never opened")


# ── relay_loop(on_tick=...) ──────────────────────────────────────────


class ScriptedRelay:
    """``process_batch`` returns (or raises) from a script, then blocks."""

    def __init__(self, script):
        self.script = list(script)
        self.calls = 0

    async def process_batch(self):
        self.calls += 1
        if not self.script:
            await asyncio.sleep(3600)
        step = self.script.pop(0)
        if isinstance(step, Exception):
            raise step
        return step


async def _run_until(relay, predicate, **kwargs):
    task = asyncio.create_task(relay_loop(relay, poll_interval=0.001, **kwargs))
    for _ in range(500):
        if predicate():
            break
        await asyncio.sleep(0.002)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


@pytest.mark.asyncio
async def test_on_tick_runs_once_per_iteration_idle_busy_or_failing():
    relay = ScriptedRelay([3, 0, RuntimeError("db down"), 0])
    ticks = []
    await _run_until(relay, lambda: relay.calls == 5, on_tick=lambda: ticks.append(1))
    # Four scripted iterations plus the fifth, now blocked in process_batch.
    assert len(ticks) == 5


@pytest.mark.asyncio
async def test_on_tick_may_be_async():
    relay = ScriptedRelay([0, 0])
    ticks = []

    async def tick():
        ticks.append(1)

    await _run_until(relay, lambda: relay.calls == 3, on_tick=tick)
    assert len(ticks) == 3


@pytest.mark.asyncio
async def test_a_failing_on_tick_does_not_stop_publishing():
    relay = ScriptedRelay([1, 1, 1])

    def tick():
        raise OSError("read-only filesystem")

    await _run_until(relay, lambda: relay.calls == 4, on_tick=tick)
    assert relay.calls == 4


@pytest.mark.asyncio
async def test_relay_loop_without_on_tick_behaves_as_before():
    relay = ScriptedRelay([2, 0, RuntimeError("boom"), 0])
    await _run_until(relay, lambda: relay.calls == 5)
    assert relay.calls == 5
    # Positional relay only; everything else keyword-only with the old default.
    params = inspect.signature(relay_loop).parameters
    assert list(params) == ["relay", "poll_interval", "on_tick"]
    assert params["poll_interval"].kind is inspect.Parameter.KEYWORD_ONLY
    assert params["poll_interval"].default == 0.5
    assert params["on_tick"].kind is inspect.Parameter.KEYWORD_ONLY
    assert params["on_tick"].default is None


# ── OUTBOX_RELAY_IN_PROCESS ──────────────────────────────────────────


@pytest.mark.parametrize("value", ["false", "FALSE", " False ", "0", "no", "off", "OFF"])
def test_relay_in_process_false_values(value):
    assert relay_in_process({"OUTBOX_RELAY_IN_PROCESS": value}) is False


@pytest.mark.parametrize("value", ["true", "TRUE", "1", "yes", "on", "", "  "])
def test_relay_in_process_true_values(value):
    assert relay_in_process({"OUTBOX_RELAY_IN_PROCESS": value}) is True


def test_relay_in_process_defaults_to_true_when_unset():
    assert relay_in_process({}) is True


def test_relay_in_process_unrecognised_value_keeps_the_relay_in_process(caplog):
    assert relay_in_process({"OUTBOX_RELAY_IN_PROCESS": "flase"}) is True
    assert "Unrecognised OUTBOX_RELAY_IN_PROCESS" in caplog.text


def test_relay_in_process_reads_os_environ(monkeypatch):
    monkeypatch.setenv("OUTBOX_RELAY_IN_PROCESS", "false")
    assert relay_in_process() is False
    monkeypatch.delenv("OUTBOX_RELAY_IN_PROCESS")
    assert relay_in_process() is True


# ── healthcheck ──────────────────────────────────────────────────────


def _probe(*args):
    src = str(Path(healthcheck.__file__).resolve().parents[2])
    env = {**os.environ, "PYTHONPATH": src}
    return subprocess.run(
        [sys.executable, "-m", "unimessaging.outbox.healthcheck", *args],
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
    )


def test_healthcheck_fails_when_heartbeat_missing(tmp_path):
    result = _probe("--path", str(tmp_path / "nope"), "--max-age", "30")
    assert result.returncode == 1
    assert "missing" in result.stderr


def test_healthcheck_fails_when_heartbeat_stale(tmp_path):
    beat = tmp_path / "beat"
    beat.touch()
    old = time.time() - 120
    os.utime(beat, (old, old))
    result = _probe("--path", str(beat), "--max-age", "30")
    assert result.returncode == 1
    assert "stale" in result.stderr


def test_healthcheck_passes_when_heartbeat_fresh(tmp_path):
    beat = tmp_path / "beat"
    beat.touch()
    assert _probe("--path", str(beat), "--max-age", "30").returncode == 0


def test_healthcheck_rejects_bad_arguments(tmp_path):
    assert _probe("--path", str(tmp_path / "beat")).returncode != 0
    with pytest.raises(SystemExit) as exc:
        healthcheck.main(["--path", "x", "--max-age", "0"])
    assert exc.value.code != 0


# ── fixtures: real NATS, throwaway PostgreSQL ────────────────────────


@pytest.fixture
def nats_server(tmp_path):
    if not os.path.exists(NATS_SERVER):
        pytest.skip("nats-server binary not available")
    port, monitor = _free_port(), _free_port()
    proc = subprocess.Popen(
        [NATS_SERVER, "-js", "-a", "127.0.0.1", "-p", str(port),
         "-m", str(monitor), "-sd", str(tmp_path / "js")],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        _wait_for_port(port)
        _wait_for_port(monitor)
        yield {"url": f"nats://127.0.0.1:{port}", "monitor": monitor}
    finally:
        proc.terminate()
        proc.wait(timeout=10)


@pytest.fixture(scope="module")
def pg_dsn(tmp_path_factory):
    if PG_BIN is None or not (PG_BIN / "initdb").exists():
        pytest.skip("PostgreSQL server binaries not available")
    pytest.importorskip("asyncpg")
    root = tmp_path_factory.mktemp("pg")
    data = root / "data"
    port = _free_port()
    subprocess.run(
        [str(PG_BIN / "initdb"), "-D", str(data), "-U", "postgres",
         "--auth=trust", "-E", "UTF8"],
        check=True, capture_output=True,
    )
    subprocess.run(
        [str(PG_BIN / "pg_ctl"), "-D", str(data), "-l", str(root / "log"), "-w",
         "-o", f"-k {root} -c listen_addresses=127.0.0.1 -p {port}", "start"],
        check=True, capture_output=True,
    )
    try:
        yield f"postgresql+asyncpg://postgres@127.0.0.1:{port}/postgres"
    finally:
        subprocess.run(
            [str(PG_BIN / "pg_ctl"), "-D", str(data), "-m", "immediate", "stop"],
            capture_output=True,
        )


@pytest_asyncio.fixture
async def outbox_db(pg_dsn):
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
    from sqlalchemy.orm import DeclarativeBase

    class Base(DeclarativeBase):
        pass

    class Outbox(OutboxMixin, Base):
        pass

    engine = create_async_engine(pg_dsn)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
    try:
        yield engine, async_sessionmaker(engine, expire_on_commit=False)
    finally:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.drop_all)
        await engine.dispose()


async def _insert_row(engine, aggregate_type="thing", payload=None):
    from sqlalchemy import text

    row_id = uuid.uuid4()
    async with engine.begin() as conn:
        await conn.execute(
            text(
                "INSERT INTO outbox (id, aggregate_type, aggregate_id, event_type,"
                " payload, headers, status, retries, occurred_at)"
                " VALUES (:id, :at, 'a1', 'created', CAST(:p AS jsonb),"
                " '{}'::jsonb, 'PENDING', 0, :ts)"
            ),
            {"id": row_id, "at": aggregate_type, "p": json.dumps(payload or {"n": 1}),
             "ts": datetime.now(timezone.utc).replace(tzinfo=None)},
        )
    return row_id


async def _row(engine, row_id):
    from sqlalchemy import text

    async with engine.connect() as conn:
        result = await conn.execute(
            text("SELECT status, retries FROM outbox WHERE id = :id"), {"id": row_id}
        )
        return result.one()


def _relay_connections(monitor_port):
    with urllib.request.urlopen(
        f"http://127.0.0.1:{monitor_port}/connz?subs=1", timeout=5
    ) as resp:
        conns = json.load(resp).get("connections") or []
    return [c for c in conns if c.get("name") == SERVICE]


async def _stream_messages(url):
    import nats

    nc = await nats.connect(url)
    try:
        info = await nc.jetstream().stream_info(STREAM)
        return info.state.messages, list(info.config.subjects)
    finally:
        await nc.close()


async def _eventually(predicate, timeout=10.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = predicate()
        if inspect.isawaitable(result):
            result = await result
        if result:
            return
        await asyncio.sleep(0.05)
    raise AssertionError("condition not met in time")


def _start_runner(session_factory, url, messaging_wrapper=None, **kwargs):
    def factory(messaging):
        if messaging_wrapper is not None:
            messaging = messaging_wrapper(messaging)
        return OutboxRelay(session_factory, messaging, subject_prefix=SERVICE)

    return asyncio.create_task(
        run_standalone_relay(
            factory,
            service_name=SERVICE,
            url=url,
            enable_durable=True,
            stream_name=STREAM,
            stream_subjects=SUBJECTS,
            poll_interval=0.05,
            **kwargs,
        )
    )


def _sigterm():
    os.kill(os.getpid(), signal.SIGTERM)


# ── run_standalone_relay ─────────────────────────────────────────────


@pytest.mark.asyncio
async def test_runner_publishes_a_row_and_holds_no_subscriptions(
    nats_server, outbox_db, tmp_path
):
    engine, session_factory = outbox_db
    row_id = await _insert_row(engine)
    beat = tmp_path / "heartbeat"

    runner = _start_runner(session_factory, nats_server["url"], heartbeat_path=str(beat))

    async def published():
        return (await _row(engine, row_id)).status == "PUBLISHED"

    await _eventually(published)

    # Create-if-absent: the runner declared its owner's stream itself.
    messages, subjects = await _stream_messages(nats_server["url"])
    assert messages == 1
    assert subjects == SUBJECTS

    conns = _relay_connections(nats_server["monitor"])
    assert len(conns) == 1
    # The only interest a JetStream publisher can hold is nats-py's private
    # reply inbox, which is how publish acks come back. Nothing that could
    # receive another service's messages.
    subs = conns[0].get("subscriptions_list") or []
    assert all(s.startswith("_INBOX.") for s in subs), subs
    assert len(subs) <= 1, subs

    assert healthcheck.main(["--path", str(beat), "--max-age", "5"]) == 0

    _sigterm()
    await asyncio.wait_for(runner, timeout=10)
    assert runner.exception() is None
    # Broker stopped: the server no longer sees the relay's connection.
    await _eventually(lambda: not _relay_connections(nats_server["monitor"]))


@pytest.mark.asyncio
async def test_core_nats_runner_holds_zero_subscriptions(nats_server):
    class IdleRelay:
        async def process_batch(self):
            return 0

    seen = {}

    def factory(messaging):
        seen["messaging"] = messaging
        return IdleRelay()

    runner = asyncio.create_task(
        run_standalone_relay(
            factory, service_name=SERVICE, url=nats_server["url"],
            enable_durable=False, poll_interval=0.01,
        )
    )
    await _eventually(lambda: bool(_relay_connections(nats_server["monitor"])))
    await seen["messaging"].publish("relaytest.ping", b"{}")
    conns = _relay_connections(nats_server["monitor"])
    assert conns[0].get("subscriptions", 0) == 0
    assert not conns[0].get("subscriptions_list")
    assert seen["messaging"].adapter._subs == []

    _sigterm()
    await asyncio.wait_for(runner, timeout=10)
    assert runner.exception() is None


@pytest.mark.asyncio
async def test_sigterm_mid_batch_releases_the_claim_and_the_row_is_published_later(
    nats_server, outbox_db
):
    engine, session_factory = outbox_db
    row_id = await _insert_row(engine)
    in_publish = asyncio.Event()

    class StuckMessaging:
        """Claims the row, then hangs inside the publish."""

        def __init__(self, inner):
            self.inner = inner

        async def publish(self, subject, data):
            in_publish.set()
            await asyncio.sleep(3600)

    runner = _start_runner(session_factory, nats_server["url"], messaging_wrapper=StuckMessaging)
    await asyncio.wait_for(in_publish.wait(), timeout=10)

    # The row is claimed: a second claimer skips it.
    from sqlalchemy import text

    async with engine.begin() as conn:
        claimed = await conn.execute(
            text("SELECT id FROM outbox FOR UPDATE SKIP LOCKED")
        )
        assert claimed.fetchall() == []

    _sigterm()
    await asyncio.wait_for(runner, timeout=10)
    assert runner.exception() is None
    await _eventually(lambda: not _relay_connections(nats_server["monitor"]))

    # Rolled back, not marked as a failed attempt, and no longer locked.
    assert tuple(await _row(engine, row_id)) == ("PENDING", 0)
    async with engine.begin() as conn:
        claimed = await conn.execute(
            text("SELECT id FROM outbox FOR UPDATE NOWAIT")
        )
        assert [r[0] for r in claimed.fetchall()] == [row_id]

    # A fresh runner re-claims the row and publishes it.
    runner = _start_runner(session_factory, nats_server["url"])

    async def published():
        return (await _row(engine, row_id)).status == "PUBLISHED"

    await _eventually(published)
    messages, _ = await _stream_messages(nats_server["url"])
    assert messages == 1

    _sigterm()
    await asyncio.wait_for(runner, timeout=10)
    assert runner.exception() is None


@pytest.mark.asyncio
async def test_runner_restores_previous_signal_handlers(nats_server):
    class IdleRelay:
        async def process_batch(self):
            return 0

    def marker(signum, frame):  # pragma: no cover - never delivered
        pass

    before_int = signal.getsignal(signal.SIGINT)
    signal.signal(signal.SIGTERM, marker)
    try:
        runner = asyncio.create_task(
            run_standalone_relay(
                lambda m: IdleRelay(), service_name=SERVICE,
                url=nats_server["url"], enable_durable=False, poll_interval=0.01,
            )
        )
        await _eventually(
            lambda: bool(_relay_connections(nats_server["monitor"]))
        )
        _sigterm()
        await asyncio.wait_for(runner, timeout=10)
        assert signal.getsignal(signal.SIGTERM) is marker
        assert signal.getsignal(signal.SIGINT) is before_int
    finally:
        signal.signal(signal.SIGTERM, signal.SIG_DFL)
