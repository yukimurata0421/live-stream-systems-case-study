from __future__ import annotations

import ssl
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, TypeVar

Handler = TypeVar("Handler", bound=BaseHTTPRequestHandler)


class ReloadingTLSHTTPServer(ThreadingHTTPServer):
    """Atomically swap validated TLS contexts for future connections.

    TLS handshakes run in request threads rather than the accept loop, so one
    stalled peer cannot prevent every other client from being accepted.
    """

    daemon_threads = True
    request_queue_size = 32

    def __init__(
        self,
        server_address: tuple[str, int],
        handler: type[Handler],
        ssl_context: ssl.SSLContext,
        *,
        connection_timeout_seconds: float = 5.0,
        maximum_concurrent_requests: int = 64,
    ) -> None:
        if not isinstance(ssl_context, ssl.SSLContext):
            raise TypeError("TLS_SERVER_CONTEXT_INVALID")
        if not 0.1 <= connection_timeout_seconds <= 30.0:
            raise ValueError("TLS_SERVER_CONNECTION_TIMEOUT_INVALID")
        if not 1 <= maximum_concurrent_requests <= 256:
            raise ValueError("TLS_SERVER_MAXIMUM_CONCURRENT_REQUESTS_INVALID")
        self._tls_lock = threading.Lock()
        self._tls_context = ssl_context
        self._tls_generation = 1
        self._connection_timeout_seconds = connection_timeout_seconds
        self._maximum_concurrent_requests = maximum_concurrent_requests
        self._request_slots = threading.BoundedSemaphore(maximum_concurrent_requests)
        self._capacity_lock = threading.Lock()
        self._active_request_count = 0
        self._rejected_connection_count = 0
        super().__init__(server_address, handler)

    @property
    def tls_generation(self) -> int:
        with self._tls_lock:
            return self._tls_generation

    @property
    def maximum_concurrent_requests(self) -> int:
        return self._maximum_concurrent_requests

    @property
    def active_request_count(self) -> int:
        with self._capacity_lock:
            return self._active_request_count

    @property
    def rejected_connection_count(self) -> int:
        with self._capacity_lock:
            return self._rejected_connection_count

    def reload_tls(self, ssl_context: ssl.SSLContext) -> int:
        if not isinstance(ssl_context, ssl.SSLContext):
            raise TypeError("TLS_SERVER_CONTEXT_INVALID")
        with self._tls_lock:
            self._tls_context = ssl_context
            self._tls_generation += 1
            return self._tls_generation

    def get_request(self) -> tuple[ssl.SSLSocket, object]:
        raw_socket, address = super().get_request()
        raw_socket.settimeout(self._connection_timeout_seconds)
        with self._tls_lock:
            context = self._tls_context
        try:
            wrapped = context.wrap_socket(raw_socket, server_side=True, do_handshake_on_connect=False)
        except BaseException:
            raw_socket.close()
            raise
        return wrapped, address

    def process_request(self, request: Any, client_address: Any) -> None:
        if not self._request_slots.acquire(blocking=False):
            with self._capacity_lock:
                self._rejected_connection_count += 1
            self.shutdown_request(request)
            return
        with self._capacity_lock:
            self._active_request_count += 1
        try:
            super().process_request(request, client_address)
        except BaseException:
            with self._capacity_lock:
                self._active_request_count -= 1
            self._request_slots.release()
            raise

    def process_request_thread(self, request: Any, client_address: Any) -> None:
        try:
            super().process_request_thread(request, client_address)
        finally:
            with self._capacity_lock:
                self._active_request_count -= 1
            self._request_slots.release()
