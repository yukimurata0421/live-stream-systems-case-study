from __future__ import annotations

import json
import ssl
import threading
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

import pytest

from cra_dell_recovery.reloading_tls_server import ReloadingTLSHTTPServer
from cra_no_action_soak import recovery_transport as module
from cra_no_action_soak.host_status import atomic_write_json
from tests.integration.test_mtls_transport import mtls_contexts


@pytest.fixture
def transport(tmp_path: Path) -> Any:
    server_context, client, no_certificate = mtls_contexts(tmp_path)
    source = tmp_path / "source.json"
    packet = {"observed_at": "2026-09-01T00:00:00Z", "signature": "original-even-if-invalid", "sequence": 8}
    atomic_write_json(source, packet)
    server = ReloadingTLSHTTPServer(("127.0.0.1", 0), module.handler_for({"dell": str(source)}), server_context)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"https://127.0.0.1:{server.server_address[1]}", client, no_certificate, source, packet
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def test_mtls_preserves_original_packet_and_independent_missing_role(transport: Any, tmp_path: Path) -> None:
    url, client, _, source, original = transport
    config = {
        "sources": {
            "dell": {"url": url + module.ROUTES["dell"], "output_file": str(tmp_path / "dell-inbox.json")},
            "arena": {"url": url + module.ROUTES["arena"], "output_file": str(tmp_path / "arena-inbox.json")},
        },
        "status_file": str(tmp_path / "transport.json"),
    }
    status = module.pull(config, client)
    assert status["roles"] == {"dell": "RECEIVED_UNTRUSTED", "arena": "UNAVAILABLE"}
    assert json.loads((tmp_path / "dell-inbox.json").read_bytes()) == original
    source.unlink()
    assert module.pull(config, client)["roles"]["dell"] == "UNAVAILABLE"
    assert json.loads((tmp_path / "dell-inbox.json").read_bytes()) == original


@pytest.mark.parametrize("method", ["POST", "PUT", "PATCH", "DELETE"])
def test_mutation_methods_rejected(transport: Any, method: str) -> None:
    url, client, *_ = transport
    with pytest.raises(urllib.error.HTTPError) as error:
        urllib.request.urlopen(urllib.request.Request(url + module.ROUTES["dell"], method=method), context=client, timeout=2)
    assert error.value.code == 405


@pytest.mark.parametrize("path", ["/", "/v1/recovery-facts/dell?fresh=true", "/etc/passwd", "/v1/commands"])
def test_fixed_route_only(transport: Any, path: str) -> None:
    url, client, *_ = transport
    with pytest.raises(urllib.error.HTTPError) as error:
        urllib.request.urlopen(url + path, context=client, timeout=2)
    assert error.value.code == 404


def test_missing_client_certificate_rejected(transport: Any) -> None:
    url, _, no_certificate, *_ = transport
    with pytest.raises((ssl.SSLError, urllib.error.URLError)):
        urllib.request.urlopen(url + module.ROUTES["dell"], context=no_certificate, timeout=2)


@pytest.mark.parametrize("content", [b'{"x":1,"x":2}', b"[]", b'{"x":NaN}', b"x" * (64 * 1024 + 1)])
def test_bad_source_not_served(transport: Any, content: bytes) -> None:
    url, client, _, source, _ = transport
    source.write_bytes(content)
    with pytest.raises(urllib.error.HTTPError) as error:
        urllib.request.urlopen(url + module.ROUTES["dell"], context=client, timeout=2)
    assert error.value.code == 503


def test_redirect_is_never_followed() -> None:
    class Response:
        closed = False

        def close(self) -> None:
            self.closed = True

    response = Response()
    with pytest.raises(ValueError, match="REDIRECT_FORBIDDEN"):
        module.NoRedirect().redirect_request(None, response, 302, "", {}, "https://127.0.0.1/elsewhere")
    assert response.closed
