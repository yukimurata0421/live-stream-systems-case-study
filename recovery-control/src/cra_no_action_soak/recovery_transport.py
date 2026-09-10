"""Fixed-path mTLS transport of original recovery packets; no signing/control.

The relay never refreshes a producer timestamp or hides an invalid signature.
Each independently fetched role is written as an untrusted inbox packet; the
collector admits it using its pinned key/release contract. No shell, DB, action
credential, arbitrary URL, or public listener is supported.
"""

from __future__ import annotations

import argparse
import http.client
import ipaddress
import json
import ssl
import time
import urllib.parse
import urllib.request
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler
from pathlib import Path
from typing import Any

from cra_dell_recovery.bounded_http import fetch_bounded_json
from cra_dell_recovery.release_identity import require_runtime_release
from cra_dell_recovery.reloading_tls_server import ReloadingTLSHTTPServer
from cra_dell_recovery.tls_credentials import validate_ca_certificate, validate_leaf_certificate

from .host_status import atomic_write_json, read_regular_bytes
from .recovery_facts import strict_json_object, strict_object

SCHEMA = "cra.recovery_transport_config.v1"
ROUTES = {role: f"/v1/recovery-facts/{role}" for role in ("dell", "arena")}
MAXIMUM_BYTES = 64 * 1024
# Kept as an explicit seam so the harness can prove that protocol-exception
# isolation is necessary without rewriting/importing a mutated production
# module.  The tuple is immutable in production; tests may temporarily replace
# the module attribute with the historical, incomplete exception set.
ROLE_LOCAL_FAILURES = (OSError, ValueError, http.client.HTTPException)


def validate_config(value: dict[str, Any]) -> None:
    common = {"schema", "mode", "release_id", "host_id", "host_contract_file", "tls_certificate", "tls_private_key", "peer_ca_certificate"}
    extra = {"listen_host", "listen_port", "packets"} if value.get("mode") == "serve" else {"sources", "status_file"}
    if set(value) != common | extra or value.get("schema") != SCHEMA or value.get("mode") not in {"serve", "pull"}:
        raise ValueError("RECOVERY_TRANSPORT_CONFIG_INVALID")
    require_runtime_release(value["release_id"], "RECOVERY_TRANSPORT_RELEASE_MISMATCH")
    for name in ("host_contract_file", "tls_certificate", "tls_private_key", "peer_ca_certificate"):
        if not isinstance(value[name], str) or not Path(value[name]).is_absolute():
            raise ValueError("RECOVERY_TRANSPORT_PATH_INVALID")
    contract_path = Path(value["host_contract_file"])
    contract = strict_json_object(read_regular_bytes(contract_path, maximum_bytes=MAXIMUM_BYTES, expected_owner_uid=0))
    if contract.get("host_id") != value["host_id"]:
        raise ValueError("RECOVERY_TRANSPORT_HOST_MISMATCH")
    permitted = {
        ("dell-stream-runtime", "serve"): {"dell"},
        ("arena-monitoring-facts", "serve"): {"dell", "arena"},
        ("arena-monitoring-facts", "pull"): {"dell"},
        ("cra-01-central-authority", "pull"): {"dell", "arena"},
    }
    roles = value.get("packets" if value["mode"] == "serve" else "sources")
    if not isinstance(roles, dict) or set(roles) != permitted.get((value["host_id"], value["mode"])):
        raise ValueError("RECOVERY_TRANSPORT_HOST_ROLE_BOUNDARY")
    if value["mode"] == "serve":
        address = ipaddress.ip_address(value["listen_host"])
        if not address.is_private or address.is_unspecified or address.is_multicast or address.version != 4:
            raise ValueError("RECOVERY_TRANSPORT_PRIVATE_LISTENER_REQUIRED")
        if type(value["listen_port"]) is not int or not 1024 <= value["listen_port"] <= 65535:
            raise ValueError("RECOVERY_TRANSPORT_PORT_INVALID")
        packets = value["packets"]
        if not isinstance(packets, dict) or not packets or not set(packets) <= ROUTES.keys():
            raise ValueError("RECOVERY_TRANSPORT_ROLES_INVALID")
        if any(not isinstance(p, str) or not Path(p).is_absolute() for p in packets.values()):
            raise ValueError("RECOVERY_TRANSPORT_PATH_INVALID")
    else:
        sources = value["sources"]
        if not isinstance(sources, dict) or not sources or not set(sources) <= ROUTES.keys():
            raise ValueError("RECOVERY_TRANSPORT_ROLES_INVALID")
        outputs = [value["status_file"]]
        for role, item in sources.items():
            if not isinstance(item, dict) or set(item) != {"url", "output_file"}:
                raise ValueError("RECOVERY_TRANSPORT_SOURCE_INVALID")
            url = urllib.parse.urlsplit(item["url"])
            if url.scheme != "https" or url.path != ROUTES[role] or url.query or url.fragment or url.username or url.password:
                raise ValueError("RECOVERY_TRANSPORT_URL_INVALID")
            address = ipaddress.ip_address(str(url.hostname))
            if not address.is_private or address.is_unspecified or address.is_multicast:
                raise ValueError("RECOVERY_TRANSPORT_PRIVATE_PEER_REQUIRED")
            outputs.append(item["output_file"])
        if any(not isinstance(p, str) or not Path(p).is_absolute() for p in outputs) or len(set(outputs)) != len(outputs):
            raise ValueError("RECOVERY_TRANSPORT_OUTPUT_INVALID")
        inputs = {value[k] for k in common if k.endswith("_file") or k.endswith("certificate") or k == "tls_private_key"}
        if inputs.intersection(outputs):
            raise ValueError("RECOVERY_TRANSPORT_PATH_COLLISION")


def tls_context(value: dict[str, Any]) -> ssl.SSLContext:
    certificate, key, ca = (Path(value[k]) for k in ("tls_certificate", "tls_private_key", "peer_ca_certificate"))
    read_regular_bytes(key, maximum_bytes=MAXIMUM_BYTES, secret=True)
    read_regular_bytes(certificate, maximum_bytes=MAXIMUM_BYTES)
    read_regular_bytes(ca, maximum_bytes=MAXIMUM_BYTES)
    serving = value["mode"] == "serve"
    validate_leaf_certificate(certificate, "RECOVERY_TRANSPORT_CERTIFICATE", usage="server" if serving else "client")
    validate_ca_certificate(ca, "RECOVERY_TRANSPORT_PEER_CA")
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER) if serving else ssl.create_default_context(cafile=ca)
    context.minimum_version = ssl.TLSVersion.TLSv1_3
    context.load_cert_chain(certificate, key)
    if serving:
        context.load_verify_locations(cafile=ca)
        context.verify_mode = ssl.CERT_REQUIRED
    return context


def handler_for(packets: dict[str, str]) -> type[BaseHTTPRequestHandler]:
    routes = {ROUTES[role]: Path(path) for role, path in packets.items()}

    class Handler(BaseHTTPRequestHandler):
        def send_body(self, status: int, body: bytes) -> None:
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self) -> None:  # noqa: N802
            source = routes.get(self.path)
            if source is None:
                self.send_body(404, b'{"error":"NOT_FOUND"}')
                return
            try:
                body = read_regular_bytes(source, maximum_bytes=MAXIMUM_BYTES)
                strict_json_object(body)
            except (OSError, ValueError):
                self.send_body(503, b'{"error":"SOURCE_UNAVAILABLE"}')
                return
            self.send_body(200, body)

        def read_only(self) -> None:
            self.send_body(405, b'{"error":"READ_ONLY"}')

        do_POST = do_PUT = do_PATCH = do_DELETE = read_only

        def log_message(self, _format: str, *_args: object) -> None:
            return

    return Handler


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req: Any, fp: Any, code: int, msg: str, headers: Any, newurl: str) -> None:
        fp.close()
        raise ValueError("RECOVERY_TRANSPORT_REDIRECT_FORBIDDEN")


def pull(value: dict[str, Any], context: ssl.SSLContext) -> dict[str, Any]:
    # Independent roles: a missing Dell packet must not hide arena's local
    # outage evidence. systemd additionally bounds the whole invocation.
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), urllib.request.HTTPSHandler(context=context), NoRedirect())
    results = {}
    for role, item in value["sources"].items():
        try:
            raw, _ = fetch_bounded_json(
                urllib.request.Request(item["url"], headers={"Accept": "application/json"}, method="GET"),
                context=context,
                open_url=lambda request, **kw: opener.open(request, timeout=kw["timeout"]),
                timeout_seconds=2,
                transient_retry_delays_seconds=(),
                maximum_retry_elapsed_seconds=3,
                maximum_bytes=MAXIMUM_BYTES,
                monotonic=time.monotonic,
                wait=time.sleep,
                jitter=lambda cap: cap,
                error_prefix="RECOVERY_TRANSPORT",
                retryable=lambda error: False,
                failure_observer=None,
            )
            packet = strict_json_object(raw)
            # Preserve source timestamps/signature even if expired or invalid;
            # the formal oracle must see, not silently discard, bad evidence.
            atomic_write_json(Path(item["output_file"]), packet)
            results[role] = "RECEIVED_UNTRUSTED"
        except ROLE_LOCAL_FAILURES:
            results[role] = "UNAVAILABLE"
    result = {"schema": "cra.recovery_transport_status.v1", "observed_at": datetime.now(UTC).isoformat(), "roles": results}
    atomic_write_json(Path(value["status_file"]), result)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    args = parser.parse_args()
    value = strict_object(args.config, maximum_bytes=MAXIMUM_BYTES)
    validate_config(value)
    context = tls_context(value)
    if value["mode"] == "pull":
        print(json.dumps(pull(value, context)))
    else:
        with ReloadingTLSHTTPServer(
            (value["listen_host"], value["listen_port"]),
            handler_for(value["packets"]),
            context,
            connection_timeout_seconds=3,
            maximum_concurrent_requests=8,
        ) as server:
            server.serve_forever(poll_interval=0.5)


if __name__ == "__main__":
    main()
