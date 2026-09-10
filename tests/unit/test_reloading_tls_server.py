from __future__ import annotations

import socket
import ssl
import threading
import time
from collections.abc import Callable
from http.server import BaseHTTPRequestHandler

import pytest

from cra_dell_recovery.reloading_tls_server import ReloadingTLSHTTPServer


class _NoopHandler(BaseHTTPRequestHandler):
    def log_message(self, _format: str, *_args: object) -> None:
        return


def _wait_until(predicate: Callable[[], bool], *, timeout_seconds: float = 2.0) -> None:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.01)
    raise AssertionError("CONDITION_NOT_REACHED")


def test_tls_context_reload_is_atomic_and_rejects_invalid_replacement() -> None:
    initial = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    replacement = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    server = ReloadingTLSHTTPServer(("127.0.0.1", 0), _NoopHandler, initial)
    try:
        assert server.tls_generation == 1
        assert server.reload_tls(replacement) == 2
        assert server.tls_generation == 2
        with pytest.raises(TypeError, match="TLS_SERVER_CONTEXT_INVALID"):
            server.reload_tls(object())  # type: ignore[arg-type]
        assert server.tls_generation == 2
    finally:
        server.server_close()


@pytest.mark.parametrize("timeout", [0.0, 30.1])
def test_tls_server_rejects_unbounded_connection_timeout(timeout: float) -> None:
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    with pytest.raises(ValueError, match="TLS_SERVER_CONNECTION_TIMEOUT_INVALID"):
        ReloadingTLSHTTPServer(("127.0.0.1", 0), _NoopHandler, context, connection_timeout_seconds=timeout)


@pytest.mark.parametrize("maximum", [0, 257])
def test_tls_server_rejects_unbounded_request_capacity(maximum: int) -> None:
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    with pytest.raises(ValueError, match="TLS_SERVER_MAXIMUM_CONCURRENT_REQUESTS_INVALID"):
        ReloadingTLSHTTPServer(("127.0.0.1", 0), _NoopHandler, context, maximum_concurrent_requests=maximum)


def test_stalled_tls_peers_are_bounded_and_do_not_grow_request_threads() -> None:
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    server = ReloadingTLSHTTPServer(
        ("127.0.0.1", 0),
        _NoopHandler,
        context,
        connection_timeout_seconds=0.5,
        maximum_concurrent_requests=1,
    )
    worker = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)
    worker.start()
    first: socket.socket | None = None
    second: socket.socket | None = None
    try:
        address = (str(server.server_address[0]), int(server.server_address[1]))
        first = socket.create_connection(address, timeout=1)
        _wait_until(lambda: server.active_request_count == 1)
        second = socket.create_connection(address, timeout=1)
        _wait_until(lambda: server.rejected_connection_count == 1)
        assert server.active_request_count == 1
        assert server.maximum_concurrent_requests == 1
    finally:
        if first is not None:
            first.close()
        if second is not None:
            second.close()
        server.shutdown()
        server.server_close()
        worker.join(timeout=2)
