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
import os
import ssl
import stat
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

from .append_only_jsonl import _append_line, _complete_pending_line, _last_complete_before_partial, _last_line, _serialized_line
from .host_status import atomic_write_json, read_regular_bytes
from .recovery_facts import strict_json_object, strict_object

SCHEMA = "cra.recovery_transport_config.v1"
HISTORY_SCHEMA = "cra.recovery_transport_config.v2"
ROUTES = {role: f"/v1/recovery-facts/{role}" for role in ("dell", "arena")}
HISTORY_ROUTES = {role: f"/v1/recovery-history/{role}" for role in ROUTES}
MAXIMUM_BYTES = 64 * 1024
MAXIMUM_HISTORY_BYTES = 2 * 1024 * 1024 * 1024
MAXIMUM_HISTORY_RESPONSE_BYTES = 2 * 1024 * 1024
HISTORY_PAGE_SIZE = 16
# Kept as an explicit seam so the harness can prove that protocol-exception
# isolation is necessary without rewriting/importing a mutated production
# module.  The tuple is immutable in production; tests may temporarily replace
# the module attribute with the historical, incomplete exception set.
ROLE_LOCAL_FAILURES = (OSError, ValueError, http.client.HTTPException)


def validate_config(value: dict[str, Any]) -> None:
    common = {"schema", "mode", "release_id", "host_id", "host_contract_file", "tls_certificate", "tls_private_key", "peer_ca_certificate"}
    extra = {"listen_host", "listen_port", "packets"} if value.get("mode") == "serve" else {"sources", "status_file"}
    history = value.get("schema") == HISTORY_SCHEMA
    if history and value.get("mode") == "serve":
        extra.add("histories")
    if set(value) != common | extra or value.get("schema") not in {SCHEMA, HISTORY_SCHEMA} or value.get("mode") not in {"serve", "pull"}:
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
        if history:
            histories = value["histories"]
            if not isinstance(histories, dict) or set(histories) != set(packets):
                raise ValueError("RECOVERY_TRANSPORT_HISTORY_ROLES_INVALID")
            if any(not isinstance(p, str) or not Path(p).is_absolute() for p in histories.values()):
                raise ValueError("RECOVERY_TRANSPORT_HISTORY_PATH_INVALID")
            if len(set(histories.values()) | set(packets.values())) != len(histories) + len(packets):
                raise ValueError("RECOVERY_TRANSPORT_HISTORY_PATH_COLLISION")
    else:
        sources = value["sources"]
        if not isinstance(sources, dict) or not sources or not set(sources) <= ROUTES.keys():
            raise ValueError("RECOVERY_TRANSPORT_ROLES_INVALID")
        outputs = [value["status_file"]]
        for role, item in sources.items():
            required = {"url", "output_file", "history_url", "history_output_file"} if history else {"url", "output_file"}
            if not isinstance(item, dict) or set(item) != required:
                raise ValueError("RECOVERY_TRANSPORT_SOURCE_INVALID")
            url = urllib.parse.urlsplit(item["url"])
            if url.scheme != "https" or url.path != ROUTES[role] or url.query or url.fragment or url.username or url.password:
                raise ValueError("RECOVERY_TRANSPORT_URL_INVALID")
            address = ipaddress.ip_address(str(url.hostname))
            if not address.is_private or address.is_unspecified or address.is_multicast:
                raise ValueError("RECOVERY_TRANSPORT_PRIVATE_PEER_REQUIRED")
            outputs.append(item["output_file"])
            if history:
                history_url = urllib.parse.urlsplit(item["history_url"])
                if (
                    history_url.scheme != "https"
                    or history_url.path != HISTORY_ROUTES[role]
                    or history_url.query
                    or history_url.fragment
                    or history_url.username
                    or history_url.password
                    or history_url.hostname != url.hostname
                    or history_url.port != url.port
                ):
                    raise ValueError("RECOVERY_TRANSPORT_HISTORY_URL_INVALID")
                outputs.append(item["history_output_file"])
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


def history_page(path: Path, *, role: str, after: int) -> dict[str, Any]:
    """Return a bounded page from any retained sequence, even after a long outage."""
    if type(after) is not int or not 0 <= after < 2**63:
        raise ValueError("RECOVERY_HISTORY_CURSOR_INVALID")
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_size > MAXIMUM_HISTORY_BYTES:
            raise ValueError("RECOVERY_HISTORY_FILE_INVALID")
        if metadata.st_size == 0:
            if after != 0:
                raise ValueError("RECOVERY_HISTORY_RANGE_UNAVAILABLE")
            return {"schema": "cra.recovery_history_page.v1", "role": role, "after": after, "packets": [], "more": False}
        size = metadata.st_size

        def row_at(offset: int) -> tuple[int, int, dict[str, Any]] | None:
            if offset >= size:
                return None
            start = offset
            if start and os.pread(descriptor, 1, start - 1) != b"\n":
                skip = os.pread(descriptor, min(MAXIMUM_BYTES + 1, size - start), start)
                boundary = skip.find(b"\n")
                if boundary < 0:
                    raise ValueError("RECOVERY_HISTORY_OVERSIZE_FRAME")
                start += boundary + 1
            if start >= size:
                return None
            raw = os.pread(descriptor, min(MAXIMUM_BYTES + 1, size - start), start)
            boundary = raw.find(b"\n")
            if boundary < 0 or boundary > MAXIMUM_BYTES or start + boundary + 1 > size:
                raise ValueError("RECOVERY_HISTORY_PARTIAL_OR_OVERSIZE_FRAME")
            row = strict_json_object(raw[:boundary])
            if row.get("role") != role or type(row.get("sequence")) is not int or row["sequence"] < 1:
                raise ValueError("RECOVERY_HISTORY_ROLE_OR_SEQUENCE_INVALID")
            return start, start + boundary + 1, row

        first = row_at(0)
        if first is None:
            raise ValueError("RECOVERY_HISTORY_EMPTY_SUFFIX")
        tail = os.pread(descriptor, min(MAXIMUM_BYTES + 2, size), size - min(MAXIMUM_BYTES + 2, size))
        if not tail.endswith(b"\n"):
            raise ValueError("RECOVERY_HISTORY_PARTIAL_TAIL")
        last_raw = tail[:-1].split(b"\n")[-1]
        if not last_raw or len(last_raw) > MAXIMUM_BYTES:
            raise ValueError("RECOVERY_HISTORY_OVERSIZE_FRAME")
        last = strict_json_object(last_raw)
        if last.get("role") != role or type(last.get("sequence")) is not int:
            raise ValueError("RECOVERY_HISTORY_ROLE_OR_SEQUENCE_INVALID")
        if after > last["sequence"] or after < first[2]["sequence"] - 1:
            raise ValueError("RECOVERY_HISTORY_RANGE_UNAVAILABLE")
        packets = []
        if after < last["sequence"]:
            lower, upper = 0, size
            while lower < upper:
                middle = (lower + upper) // 2
                found = row_at(middle)
                if found is None or found[2]["sequence"] > after:
                    upper = middle
                else:
                    lower = found[1]
            found = row_at(lower)
            if found is None or found[2]["sequence"] != after + 1:
                raise ValueError("RECOVERY_HISTORY_SEQUENCE_GAP")
            position = found[0]
            for sequence in range(after + 1, after + 1 + HISTORY_PAGE_SIZE):
                found = row_at(position)
                if found is None:
                    break
                if found[2]["sequence"] != sequence:
                    raise ValueError("RECOVERY_HISTORY_SEQUENCE_GAP")
                packets.append(found[2])
                position = found[1]
        page = {
            "schema": "cra.recovery_history_page.v1",
            "role": role,
            "after": after,
            "packets": packets,
            "more": bool(packets and packets[-1]["sequence"] < last["sequence"]),
        }
        if len(json.dumps(page, separators=(",", ":")).encode()) > MAXIMUM_HISTORY_RESPONSE_BYTES:
            raise ValueError("RECOVERY_HISTORY_RESPONSE_TOO_LARGE")
        return page
    finally:
        os.close(descriptor)


def handler_for(packets: dict[str, str], histories: dict[str, str] | None = None) -> type[BaseHTTPRequestHandler]:
    routes = {ROUTES[role]: Path(path) for role, path in packets.items()}
    history_routes = {HISTORY_ROUTES[role]: (role, Path(path)) for role, path in (histories or {}).items()}

    class Handler(BaseHTTPRequestHandler):
        def send_body(self, status: int, body: bytes) -> None:
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self) -> None:  # noqa: N802
            parsed = urllib.parse.urlsplit(self.path)
            historical = history_routes.get(parsed.path)
            if historical is not None:
                try:
                    query = urllib.parse.parse_qs(parsed.query, strict_parsing=True)
                    if (
                        set(query) != {"after"}
                        or len(query["after"]) != 1
                        or not query["after"][0].isascii()
                        or not query["after"][0].isdecimal()
                    ):
                        raise ValueError("RECOVERY_HISTORY_QUERY_INVALID")
                    cursor = int(query["after"][0])
                    if str(cursor) != query["after"][0]:
                        raise ValueError("RECOVERY_HISTORY_QUERY_INVALID")
                    body = json.dumps(history_page(historical[1], role=historical[0], after=cursor), separators=(",", ":")).encode()
                except (OSError, ValueError):
                    self.send_body(503, b'{"error":"HISTORY_UNAVAILABLE"}')
                    return
                self.send_body(200, body)
                return
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
    history_results: dict[str, str] = {}
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
        if value.get("schema") == HISTORY_SCHEMA:
            try:
                archive = Path(item["history_output_file"])
                partial_prefix = b""
                try:
                    tail = _last_line(archive, maximum_bytes=MAXIMUM_HISTORY_BYTES)
                except ValueError as error:
                    if str(error) != "RESILIENT_SOAK_EVIDENCE_PARTIAL_TAIL":
                        raise
                    tail, partial_prefix = _last_complete_before_partial(archive, maximum_bytes=MAXIMUM_HISTORY_BYTES)
                after = tail["sequence"] if tail is not None else 0
                if type(after) is not int or after < 0 or (tail is not None and tail.get("role") != role):
                    raise ValueError("RECOVERY_HISTORY_LOCAL_CURSOR_INVALID")
                request = urllib.request.Request(
                    item["history_url"] + f"?after={after}", headers={"Accept": "application/json"}, method="GET"
                )
                raw, _ = fetch_bounded_json(
                    request,
                    context=context,
                    open_url=lambda req, **kw: opener.open(req, timeout=kw["timeout"]),
                    timeout_seconds=2,
                    transient_retry_delays_seconds=(),
                    maximum_retry_elapsed_seconds=3,
                    maximum_bytes=MAXIMUM_HISTORY_RESPONSE_BYTES,
                    monotonic=time.monotonic,
                    wait=time.sleep,
                    jitter=lambda cap: cap,
                    error_prefix="RECOVERY_HISTORY_TRANSPORT",
                    retryable=lambda error: False,
                    failure_observer=None,
                )
                page = strict_json_object(raw)
                packets = page.get("packets")
                if (
                    page.get("schema") != "cra.recovery_history_page.v1"
                    or page.get("role") != role
                    or page.get("after") != after
                    or not isinstance(packets, list)
                    or len(packets) > HISTORY_PAGE_SIZE
                ):
                    raise ValueError("RECOVERY_HISTORY_PAGE_INVALID")
                for sequence, packet in enumerate(packets, start=after + 1):
                    if not isinstance(packet, dict) or packet.get("role") != role or packet.get("sequence") != sequence:
                        raise ValueError("RECOVERY_HISTORY_PAGE_SEQUENCE_INVALID")
                if partial_prefix:
                    if not packets or not _serialized_line(packets[0]).startswith(partial_prefix):
                        raise ValueError("RECOVERY_HISTORY_PARTIAL_PREFIX_CONFLICT")
                    _complete_pending_line(archive, packets[0], maximum_bytes=MAXIMUM_HISTORY_BYTES)
                    packets = packets[1:]
                for packet in packets:
                    _append_line(archive, packet, maximum_bytes=MAXIMUM_HISTORY_BYTES)
                history_results[role] = "APPENDED_UNTRUSTED" if packets else "CURRENT"
            except ROLE_LOCAL_FAILURES:
                history_results[role] = "UNAVAILABLE"
    result = {"schema": "cra.recovery_transport_status.v1", "observed_at": datetime.now(UTC).isoformat(), "roles": results}
    if value.get("schema") == HISTORY_SCHEMA:
        result["schema"] = "cra.recovery_transport_status.v2"
        result["history_roles"] = history_results
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
            handler_for(value["packets"], value.get("histories")),
            context,
            connection_timeout_seconds=3,
            maximum_concurrent_requests=8,
        ) as server:
            server.serve_forever(poll_interval=0.5)


if __name__ == "__main__":
    main()
