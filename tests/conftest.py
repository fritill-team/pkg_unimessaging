"""Shared fixtures: a real JetStream-enabled ``nats-server`` per test.

Delivery-safety behaviour (de-duplication, fetch-loop liveness) is proven
against a real server, not mocks (estate:AD-22). The fixture skips when the
binary is absent or local sockets are not permitted.
"""

import asyncio
import shutil
import socket

import pytest
import pytest_asyncio

NATS_SERVER_BIN = shutil.which("nats-server") or "/usr/local/bin/nats-server"


def _free_tcp_port() -> int:
    try:
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            return int(sock.getsockname()[1])
    except PermissionError as exc:
        pytest.skip(f"local socket allocation is not permitted: {exc}")


@pytest_asyncio.fixture
async def nats_server(tmp_path):
    if not shutil.which(NATS_SERVER_BIN):
        pytest.skip("nats-server binary not available")
    port = _free_tcp_port()
    proc = await asyncio.create_subprocess_exec(
        NATS_SERVER_BIN,
        "-js",
        "-sd",
        str(tmp_path / "js"),
        "-a",
        "127.0.0.1",
        "-p",
        str(port),
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
    )
    url = f"nats://127.0.0.1:{port}"
    try:
        for _ in range(100):
            if proc.returncode is not None:
                raise RuntimeError(
                    f"nats-server exited early with code {proc.returncode}"
                )
            try:
                _reader, writer = await asyncio.open_connection("127.0.0.1", port)
                writer.close()
                await writer.wait_closed()
                break
            except OSError:
                await asyncio.sleep(0.05)
        else:
            raise RuntimeError("nats-server did not become ready")
        yield url
    finally:
        if proc.returncode is None:
            proc.terminate()
            try:
                await asyncio.wait_for(proc.wait(), timeout=5)
            except asyncio.TimeoutError:
                proc.kill()
                await proc.wait()
