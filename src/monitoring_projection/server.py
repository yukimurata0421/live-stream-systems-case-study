from __future__ import annotations

import argparse
import ipaddress
import json
import os
import signal
import ssl
import stat
import sys
import threading
from collections.abc import Callable
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler
from pathlib import Path
from typing import Any

from cra_dell_recovery.release_identity import require_runtime_release
from cra_dell_recovery.reloading_tls_server import ReloadingTLSHTTPServer
from cra_dell_recovery.tls_credentials import validate_ca_certificate, validate_leaf_certificate
from cra_no_action_soak.host_status import STATUS_PATH, served_host_status_bytes, write_server_resource_status
from cra_no_action_soak.resilient_host_status import (
    STATUS_PATH as RESILIENT_STATUS_PATH,
)
from cra_no_action_soak.resilient_host_status import served_resilient_status_bytes

CONFIG_SCHEMA = "monitoring_v4.cra_projection_server_config.v3"
V2_CONFIG_SCHEMA = "monitoring_v4.cra_projection_server_config.v2"
LEGACY_CONFIG_SCHEMA = "monitoring_v4.cra_projection_server_config.v1"
PROJECTION_PATH = "/v1/monitoring-evidence/latest"
SERVER_RESOURCE_REFRESH_SECONDS = 5.0


def _projection_bytes(path: Path, maximum_bytes: int) -> bytes:
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            raise ValueError("PROJECTION_FILE_NOT_REGULAR")
        if stat.S_IMODE(metadata.st_mode) & 0o022:
            raise ValueError("PROJECTION_FILE_PERMISSIONS_UNSAFE")
        if metadata.st_size > maximum_bytes:
            raise ValueError("PROJECTION_FILE_TOO_LARGE")
        with os.fdopen(descriptor, "rb") as handle:
            descriptor = -1
            payload = handle.read(maximum_bytes + 1)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    if len(payload) > maximum_bytes:
        raise ValueError("PROJECTION_FILE_TOO_LARGE")
    value = json.loads(payload)
    if not isinstance(value, dict) or value.get("schema") != "monitoring_v4.evidence_projection.v1":
        raise ValueError("PROJECTION_FILE_SCHEMA_INVALID")
    return payload


def _read_config(path: Path) -> dict[str, Any]:
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            raise ValueError("PROJECTION_SERVER_CONFIG_NOT_REGULAR")
        if stat.S_IMODE(metadata.st_mode) & 0o022:
            raise ValueError("PROJECTION_SERVER_CONFIG_PERMISSIONS_UNSAFE")
        if metadata.st_size > 64 * 1024:
            raise ValueError("PROJECTION_SERVER_CONFIG_TOO_LARGE")
        with os.fdopen(descriptor, "rb") as handle:
            descriptor = -1
            raw = handle.read(64 * 1024 + 1)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    if len(raw) > 64 * 1024:
        raise ValueError("PROJECTION_SERVER_CONFIG_TOO_LARGE")
    value = json.loads(raw)
    if not isinstance(value, dict):
        raise ValueError("PROJECTION_SERVER_CONFIG_FIELDS_NOT_EXACT")
    required = {
        "schema",
        "server_release_id",
        "projection_file",
        "tls_certificate",
        "tls_private_key",
        "client_ca_certificate",
        "listen_host",
        "listen_port",
        "maximum_response_bytes",
    }
    if value.get("schema") in {CONFIG_SCHEMA, V2_CONFIG_SCHEMA}:
        required |= {"host_status_file", "host_status_host_id", "server_resource_status_file"}
    if value.get("schema") == CONFIG_SCHEMA:
        required.add("resilient_host_status_file")
    if set(value) != required:
        raise ValueError("PROJECTION_SERVER_CONFIG_FIELDS_NOT_EXACT")
    if value["schema"] not in {CONFIG_SCHEMA, V2_CONFIG_SCHEMA, LEGACY_CONFIG_SCHEMA}:
        raise ValueError("PROJECTION_SERVER_CONFIG_SCHEMA_UNSUPPORTED")
    require_runtime_release(value["server_release_id"], "PROJECTION_SERVER_RUNTIME_RELEASE_MISMATCH")
    try:
        listen_address = ipaddress.ip_address(str(value["listen_host"]))
    except ValueError as error:
        raise ValueError("PROJECTION_SERVER_LISTEN_ADDRESS_INVALID") from error
    if (
        not listen_address.is_private
        or listen_address.is_loopback
        or listen_address.is_link_local
        or listen_address.is_multicast
        or listen_address.is_unspecified
    ):
        raise ValueError("PROJECTION_SERVER_LISTEN_ADDRESS_NOT_PRIVATE_LAN")
    if not 1 <= int(value["listen_port"]) <= 65_535:
        raise ValueError("PROJECTION_SERVER_PORT_OUT_OF_RANGE")
    if not 1024 <= int(value["maximum_response_bytes"]) <= 2 * 1024 * 1024:
        raise ValueError("PROJECTION_SERVER_MAXIMUM_BYTES_OUT_OF_RANGE")
    if value["schema"] in {CONFIG_SCHEMA, V2_CONFIG_SCHEMA}:
        if not isinstance(value["host_status_file"], str) or not str(value["host_status_file"]).startswith("/"):
            raise ValueError("PROJECTION_SERVER_HOST_STATUS_FILE_INVALID")
        if not isinstance(value["host_status_host_id"], str) or not value["host_status_host_id"].strip():
            raise ValueError("PROJECTION_SERVER_HOST_STATUS_HOST_ID_INVALID")
        if not isinstance(value["server_resource_status_file"], str) or not str(value["server_resource_status_file"]).startswith("/"):
            raise ValueError("PROJECTION_SERVER_RESOURCE_STATUS_FILE_INVALID")
    if value["schema"] == CONFIG_SCHEMA and (
        not isinstance(value["resilient_host_status_file"], str) or not str(value["resilient_host_status_file"]).startswith("/")
    ):
        raise ValueError("PROJECTION_SERVER_RESILIENT_HOST_STATUS_FILE_INVALID")
    return dict(value)


class ProjectionHTTPSServer:
    """Read-only mTLS endpoint for one already-signed projection file."""

    def __init__(
        self,
        projection_file: Path,
        ssl_context: ssl.SSLContext,
        *,
        host: str = "127.0.0.1",
        port: int = 0,
        maximum_bytes: int = 2 * 1024 * 1024,
        host_status_file: Path | None = None,
        resilient_host_status_file: Path | None = None,
        host_status_host_id: str | None = None,
        host_status_release_id: str | None = None,
        server_resource_status_file: Path | None = None,
    ) -> None:
        projection_ref = projection_file
        maximum_ref = maximum_bytes
        host_status_ref = host_status_file
        resilient_status_ref = resilient_host_status_file
        host_id_ref = host_status_host_id
        release_id_ref = host_status_release_id
        resource_ref = server_resource_status_file

        def report_resource_failure(error: Exception) -> None:
            resource_failure = {
                "schema": "cra.no_action_server_resource_persistence_failure.v1",
                "status": "SOAK_STATUS_SAFE_BLOCKED",
                "error_class": type(error).__name__,
                "control_capability_count": 0,
                "physical_effect_count": 0,
            }
            print(
                json.dumps(resource_failure, separators=(",", ":"), sort_keys=True),
                file=sys.stderr,
                flush=True,
            )

        resource_writer: Callable[[], None] | None = None
        if resource_ref is not None:

            def write_resources() -> None:
                write_server_resource_status(
                    resource_ref,
                    role="arena",
                    host_id=str(host_id_ref),
                    release_id=str(release_id_ref),
                )

            resource_writer = write_resources

        class Handler(BaseHTTPRequestHandler):
            def _json(
                self,
                status: int,
                value: dict[str, Any],
                *,
                retry_after_seconds: int | None = None,
            ) -> None:
                body = json.dumps(value, separators=(",", ":"), sort_keys=True).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Cache-Control", "no-store")
                if retry_after_seconds is not None:
                    self.send_header("Retry-After", str(retry_after_seconds))
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_GET(self) -> None:  # noqa: N802
                if self.path == RESILIENT_STATUS_PATH and resilient_status_ref is not None:
                    try:
                        body = served_resilient_status_bytes(
                            resilient_status_ref,
                            expected_role="arena",
                            expected_host_id=str(host_id_ref),
                            expected_release_id=str(release_id_ref),
                            maximum_bytes=maximum_ref,
                        )
                    except (OSError, ValueError, json.JSONDecodeError) as error:
                        self._json(
                            HTTPStatus.SERVICE_UNAVAILABLE,
                            {"error": type(error).__name__, "status": "SAFE_BLOCKED"},
                            retry_after_seconds=1,
                        )
                        return
                    self.send_response(HTTPStatus.OK)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Cache-Control", "no-store")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                    return
                if self.path == STATUS_PATH and host_status_ref is not None:
                    try:
                        body = served_host_status_bytes(
                            host_status_ref,
                            expected_role="arena",
                            expected_host_id=str(host_id_ref),
                            expected_release_id=str(release_id_ref),
                            maximum_bytes=maximum_ref,
                        )
                    except (OSError, ValueError, json.JSONDecodeError) as error:
                        self._json(
                            HTTPStatus.SERVICE_UNAVAILABLE,
                            {"error": type(error).__name__, "status": "SAFE_BLOCKED"},
                            retry_after_seconds=1,
                        )
                        return
                    self.send_response(HTTPStatus.OK)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Cache-Control", "no-store")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                    return
                if self.path != PROJECTION_PATH:
                    self._json(HTTPStatus.NOT_FOUND, {"error": "NOT_FOUND"})
                    return
                try:
                    body = _projection_bytes(projection_ref, maximum_ref)
                except (OSError, ValueError, json.JSONDecodeError) as error:
                    self._json(
                        HTTPStatus.SERVICE_UNAVAILABLE,
                        {"error": type(error).__name__, "status": "SAFE_BLOCKED"},
                    )
                    return
                self.send_response(HTTPStatus.OK)
                self.send_header("Content-Type", "application/json")
                self.send_header("Cache-Control", "no-store")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_POST(self) -> None:  # noqa: N802
                self._json(HTTPStatus.METHOD_NOT_ALLOWED, {"error": "READ_ONLY_ENDPOINT"})

            do_PUT = do_POST
            do_PATCH = do_POST
            do_DELETE = do_POST

            def log_message(self, _format: str, *_args: object) -> None:
                return

        self.server = ReloadingTLSHTTPServer((host, port), Handler, ssl_context)
        bound_host, bound_port = self.server.server_address[:2]
        host_text = bound_host.decode() if isinstance(bound_host, bytes) else str(bound_host)
        self.base_url = f"https://{host_text}:{int(bound_port)}"
        self._thread: threading.Thread | None = None
        self._resource_writer = resource_writer
        self._resource_failure = report_resource_failure
        self._resource_stop = threading.Event()
        self._resource_thread: threading.Thread | None = None

    @property
    def tls_generation(self) -> int:
        return self.server.tls_generation

    def reload_tls(self, ssl_context: ssl.SSLContext) -> int:
        return self.server.reload_tls(ssl_context)

    def __enter__(self) -> ProjectionHTTPSServer:
        if self._resource_writer is not None:
            try:
                self._resource_writer()
            except Exception as error:
                self._resource_failure(error)

            def refresh_resources() -> None:
                while not self._resource_stop.wait(SERVER_RESOURCE_REFRESH_SECONDS):
                    try:
                        assert self._resource_writer is not None
                        self._resource_writer()
                    except Exception as error:
                        self._resource_failure(error)

            self._resource_thread = threading.Thread(target=refresh_resources, daemon=True)
            self._resource_thread.start()
        self._thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self._thread.start()
        return self

    def __exit__(self, *_: object) -> None:
        self._resource_stop.set()
        self.server.shutdown()
        self.server.server_close()
        if self._thread is not None:
            self._thread.join(timeout=5)
        if self._resource_thread is not None:
            self._resource_thread.join(timeout=5)


def _server_context(certificate: Path, private_key: Path, client_ca: Path) -> ssl.SSLContext:
    _validate_tls_path(certificate, "PROJECTION_TLS_CERTIFICATE", secret=False)
    _validate_tls_path(private_key, "PROJECTION_TLS_PRIVATE_KEY", secret=True)
    _validate_tls_path(client_ca, "PROJECTION_CLIENT_CA", secret=False)
    validate_leaf_certificate(certificate, "PROJECTION_TLS_CERTIFICATE", usage="server")
    validate_ca_certificate(client_ca, "PROJECTION_CLIENT_CA")
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.minimum_version = ssl.TLSVersion.TLSv1_3
    context.load_cert_chain(certificate, private_key)
    context.load_verify_locations(cafile=client_ca)
    context.verify_mode = ssl.CERT_REQUIRED
    return context


def _validate_tls_path(path: Path, code: str, *, secret: bool) -> None:
    try:
        metadata = path.lstat()
    except OSError as error:
        raise ValueError(f"{code}_UNAVAILABLE") from error
    if not stat.S_ISREG(metadata.st_mode):
        raise ValueError(f"{code}_NOT_REGULAR_FILE")
    if stat.S_IMODE(metadata.st_mode) & (0o077 if secret else 0o022):
        raise ValueError(f"{code}_PERMISSIONS_UNSAFE")


def main() -> None:
    parser = argparse.ArgumentParser(description="Read-only mTLS server for signed Monitoring evidence")
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--check-config", action="store_true")
    args = parser.parse_args()
    value = _read_config(args.config)
    context = _server_context(
        Path(value["tls_certificate"]),
        Path(value["tls_private_key"]),
        Path(value["client_ca_certificate"]),
    )
    if args.check_config:
        print(
            json.dumps(
                {
                    "config": "VALID",
                    "schema": value["schema"],
                    "control_capability_count": 0,
                    "physical_effect_count": 0,
                },
                sort_keys=True,
            )
        )
        return
    certificate = Path(value["tls_certificate"])
    private_key = Path(value["tls_private_key"])
    client_ca = Path(value["client_ca_certificate"])
    server = ProjectionHTTPSServer(
        Path(value["projection_file"]),
        context,
        host=str(value["listen_host"]),
        port=int(value["listen_port"]),
        maximum_bytes=int(value["maximum_response_bytes"]),
        host_status_file=Path(value["host_status_file"]) if value["schema"] in {CONFIG_SCHEMA, V2_CONFIG_SCHEMA} else None,
        resilient_host_status_file=Path(value["resilient_host_status_file"]) if value["schema"] == CONFIG_SCHEMA else None,
        host_status_host_id=str(value["host_status_host_id"]) if value["schema"] in {CONFIG_SCHEMA, V2_CONFIG_SCHEMA} else None,
        host_status_release_id=str(value["server_release_id"]) if value["schema"] in {CONFIG_SCHEMA, V2_CONFIG_SCHEMA} else None,
        server_resource_status_file=(
            Path(value["server_resource_status_file"]) if value["schema"] in {CONFIG_SCHEMA, V2_CONFIG_SCHEMA} else None
        ),
    )
    stopped = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stopped.set())
    signal.signal(signal.SIGINT, lambda *_: stopped.set())

    def reload_credentials(*_: object) -> None:
        try:
            replacement = _server_context(certificate, private_key, client_ca)
            generation = server.reload_tls(replacement)
            event = {
                "schema": "monitoring_v4.projection_tls_reload.v1",
                "status": "RELOADED",
                "tls_generation": generation,
                "control_capability_count": 0,
                "physical_effect_count": 0,
            }
            print(json.dumps(event, separators=(",", ":"), sort_keys=True), flush=True)
        except Exception as error:
            event = {
                "schema": "monitoring_v4.projection_tls_reload.v1",
                "status": "LAST_KNOWN_GOOD_RETAINED",
                "error_class": type(error).__name__,
                "tls_generation": server.tls_generation,
                "control_capability_count": 0,
                "physical_effect_count": 0,
            }
            print(json.dumps(event, separators=(",", ":"), sort_keys=True), file=sys.stderr, flush=True)

    signal.signal(signal.SIGHUP, reload_credentials)
    with server:
        stopped.wait()


if __name__ == "__main__":
    main()
