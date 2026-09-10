from __future__ import annotations

import ssl
import threading
import time
import urllib.request
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any

import pytest

from cra_authority.projection_pull import ProjectionPuller, _open_projection_url
from tests.authority.test_external_incident_regressions import Response


@pytest.fixture
def local_server() -> Iterator[tuple[str, list[str]]]:
    requests: list[str] = []

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args: object) -> None:
            pass

        def do_GET(self) -> None:
            requests.append(self.path)
            if self.path.startswith("/redirect/"):
                self.send_response(int(self.path.rsplit("/", 1)[1]))
                self.send_header("Location", "/forbidden")
                self.end_headers()
                return
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", "100")
            self.end_headers()
            try:
                for _ in range(100):
                    self.wfile.write(b" ")
                    self.wfile.flush()
                    time.sleep(0.02)
            except (BrokenPipeError, ConnectionResetError):
                pass

    server = HTTPServer(("127.0.0.1", 0), Handler)
    worker = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)
    worker.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}", requests
    finally:
        server.shutdown()
        server.server_close()
        worker.join(timeout=3)
        assert not worker.is_alive()


@pytest.mark.parametrize("code", [301, 302, 303, 307, 308])
def test_default_transport_never_follows_redirect(local_server: tuple[str, list[str]], code: int) -> None:
    url, requests = local_server
    with pytest.raises(ValueError, match="REDIRECT_FORBIDDEN"):
        _open_projection_url(urllib.request.Request(f"{url}/redirect/{code}"), context=ssl.create_default_context(), timeout=0.2)
    assert requests == [f"/redirect/{code}"]


def test_real_slow_trickle_body_retries_only_within_budget(local_server: tuple[str, list[str]], tmp_path: Path) -> None:
    url, requests = local_server
    puller = ProjectionPuller(
        endpoint_url=url,
        ssl_context=ssl.create_default_context(),
        contract=object(),  # type: ignore[arg-type]
        projection_file=tmp_path / "inbox",
        timeout_seconds=0.1,
        transient_retry_delays_seconds=(0.01,),
        maximum_retry_elapsed_seconds=0.5,
        jitter=lambda cap: cap,
    )
    started = time.monotonic()
    with pytest.raises((ValueError, TimeoutError)) as captured:
        puller._fetch_body(urllib.request.Request(url))
    if isinstance(captured.value, ValueError):
        assert "RESPONSE_DEADLINE_EXCEEDED" in str(captured.value)
    assert time.monotonic() - started < 1.0
    assert 2 <= len(requests) <= 6
    assert not puller.projection_file.exists()


@pytest.mark.parametrize(
    "headers",
    [
        [("Content-Length", "2"), ("Content-Length", "2")],
        [("Content-Encoding", "gzip")],
        [("Transfer-Encoding", "chunked"), ("Content-Length", "2")],
        [("Transfer-Encoding", "gzip")],
    ],
)
def test_ambiguous_framing_is_closed_and_not_retried(tmp_path: Path, headers: list[tuple[str, str]]) -> None:
    response = Response()
    for name, value in headers:
        response.headers[name] = value
    attempts: list[int] = []

    def open_url(*args: Any, **kwargs: Any) -> Response:
        attempts.append(1)
        return response

    puller = ProjectionPuller(
        endpoint_url="https://test.invalid",
        ssl_context=ssl.create_default_context(),
        contract=object(),  # type: ignore[arg-type]
        projection_file=tmp_path / "inbox",
        open_url=open_url,
    )
    with pytest.raises(ValueError, match="MONITORING_PROJECTION_"):
        puller._fetch_body(urllib.request.Request(puller.endpoint_url))
    assert attempts == [1]
    assert response.closed


def test_oneshot_supervisor_bounds_dns_and_header_stalls() -> None:
    unit = Path(__file__).resolve().parents[2] / "ops/systemd/cra-monitoring-projection-pull@.service"
    text = unit.read_text()
    assert "TimeoutStartSec=30s" in text
    assert "TimeoutStopSec=5s" in text


def test_transport_oracle_reports_injected_illegal_retry(monkeypatch: pytest.MonkeyPatch) -> None:
    from cra_harness.projection_transport_chaos import SCENARIOS, run_projection_transport_chaos

    monkeypatch.setattr(ProjectionPuller, "_transient_transport_error", staticmethod(lambda error: True))
    report = run_projection_transport_chaos(case_count=len(SCENARIOS), seed=20260902)
    assert report["pass"] is False
    assert report["security_or_semantic_retry_count"] > 0
