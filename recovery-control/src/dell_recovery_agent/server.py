from __future__ import annotations

import json
import ssl
import threading
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, cast
from urllib.parse import unquote

import jsonschema

from cra_dell_recovery.errors import RecoveryControlError
from dell_recovery_agent.authority import AgentAuthorityLease
from dell_recovery_agent.execution import AgentService, FakePhysicalAdapter
from dell_recovery_agent.maintenance_snapshot import ShadowMaintenanceSnapshotCache
from dell_recovery_agent.reconciliation import DellReconciliationService


class ShadowAgentHTTPServer:
    """mTLS-capable test/shadow endpoint. The service only owns a fake adapter."""

    def __init__(
        self,
        service: AgentService,
        ssl_context: ssl.SSLContext,
        *,
        host: str = "127.0.0.1",
        port: int = 0,
        shadow_only: bool = False,
        authority_lease: AgentAuthorityLease | None = None,
        maintenance_snapshot_cache: ShadowMaintenanceSnapshotCache | None = None,
    ) -> None:
        self.service = service
        service_ref = service
        shadow_only_ref = shadow_only
        authority_lease_ref = authority_lease
        maintenance_snapshot_cache_ref = maintenance_snapshot_cache
        reconciliation_ref = DellReconciliationService(service.store, service.codec, service.observer)

        class Handler(BaseHTTPRequestHandler):
            def _json(self, status: int, payload: dict[str, Any]) -> None:
                body = json.dumps(payload, separators=(",", ":")).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_POST(self) -> None:  # noqa: N802
                if self.path == "/v1/shadow/maintenance-snapshots" and shadow_only_ref and maintenance_snapshot_cache_ref is not None:
                    try:
                        length = int(self.headers.get("Content-Length", "0"))
                        request = json.loads(self.rfile.read(length))
                        self._json(HTTPStatus.OK, maintenance_snapshot_cache_ref.accept(dict(request)))
                    except (KeyError, TypeError, ValueError, OSError, jsonschema.ValidationError) as exc:
                        self._json(HTTPStatus.BAD_REQUEST, {"error": type(exc).__name__})
                    return
                if self.path == "/v1/shadow/reconciliation/challenge" and shadow_only_ref:
                    try:
                        length = int(self.headers.get("Content-Length", "0"))
                        request = json.loads(self.rfile.read(length))
                        self._json(HTTPStatus.OK, reconciliation_ref.issue_challenge(str(request["target_id"])))
                    except (KeyError, ValueError, RecoveryControlError) as exc:
                        self._json(HTTPStatus.BAD_REQUEST, {"error": type(exc).__name__})
                    return
                if self.path == "/v1/shadow/reconciliation/commit" and shadow_only_ref:
                    try:
                        length = int(self.headers.get("Content-Length", "0"))
                        request = json.loads(self.rfile.read(length))
                        result = reconciliation_ref.commit(dict(request))
                        if authority_lease_ref is not None:
                            authority_lease_ref.arm_reconciliation_grace(str(result["target_id"]))
                        self._json(HTTPStatus.OK, result)
                    except (KeyError, ValueError, RecoveryControlError) as exc:
                        self._json(HTTPStatus.BAD_REQUEST, {"error": type(exc).__name__})
                    return
                if self.path == "/v1/shadow/reconciliation/journal-page" and shadow_only_ref:
                    try:
                        length = int(self.headers.get("Content-Length", "0"))
                        request = json.loads(self.rfile.read(length))
                        self._json(HTTPStatus.OK, reconciliation_ref.journal_page(dict(request)))
                    except (KeyError, ValueError, RecoveryControlError) as exc:
                        self._json(HTTPStatus.BAD_REQUEST, {"error": type(exc).__name__})
                    return
                if self.path == "/v1/shadow/heartbeats" and shadow_only_ref and authority_lease_ref is not None:
                    try:
                        length = int(self.headers.get("Content-Length", "0"))
                        request = json.loads(self.rfile.read(length))
                        state = authority_lease_ref.receive(dict(request))
                        self._json(
                            HTTPStatus.OK,
                            {
                                "authority_state": state,
                                "physical_adapter": "FAKE_COUNTER_ONLY",
                                "physical_attempt_count": (
                                    service_ref.adapter.attempt_count if isinstance(service_ref.adapter, FakePhysicalAdapter) else None
                                ),
                            },
                        )
                    except (ValueError, RecoveryControlError) as exc:
                        self._json(HTTPStatus.BAD_REQUEST, {"error": type(exc).__name__})
                    return
                allowed_path = "/v1/shadow/commands" if shadow_only_ref else "/v1/commands"
                if self.path != allowed_path:
                    self._json(HTTPStatus.NOT_FOUND, {"error": "NOT_FOUND"})
                    return
                try:
                    length = int(self.headers.get("Content-Length", "0"))
                    request = json.loads(self.rfile.read(length))
                    response = service_ref.evaluate_shadow(dict(request)) if shadow_only_ref else service_ref.handle_command(dict(request))
                    self._json(HTTPStatus.OK, response)
                except (ValueError, RecoveryControlError) as exc:
                    self._json(HTTPStatus.BAD_REQUEST, {"error": type(exc).__name__})

            def do_GET(self) -> None:  # noqa: N802
                if self.path == "/v1/shadow/status" and shadow_only_ref:
                    try:
                        fence, shadow_evaluation_count = service_ref.store.shadow_status_snapshot()
                        self._json(
                            HTTPStatus.OK,
                            {
                                "authority_state": str(fence["authority_state"]),
                                "state_reason": str(fence["state_reason"]),
                                "heartbeat_seq": int(fence["heartbeat_seq"]),
                                "highest_authority_epoch_seen": int(fence["highest_authority_epoch_seen"]),
                                "highest_command_seq_consumed": int(fence["highest_command_seq_consumed"]),
                                "last_heartbeat_received_at": fence["last_heartbeat_received_at"],
                                "shadow_evaluation_count": shadow_evaluation_count,
                                "physical_attempt_count": (
                                    service_ref.adapter.attempt_count if isinstance(service_ref.adapter, FakePhysicalAdapter) else None
                                ),
                                "physical_adapter": "FAKE_COUNTER_ONLY",
                            },
                        )
                    except RecoveryControlError as exc:
                        self._json(
                            HTTPStatus.SERVICE_UNAVAILABLE,
                            {
                                "authority_state": "SAFE_BLOCKED",
                                "physical_attempt_count": 0,
                                "physical_adapter": "FAKE_COUNTER_ONLY",
                                "error": type(exc).__name__,
                            },
                        )
                    return
                if self.path == "/v1/shadow/target" and shadow_only_ref:
                    observed = service_ref.observer.observe()
                    snapshot = getattr(service_ref.observer, "last_snapshot", None)
                    self._json(
                        HTTPStatus.OK,
                        {
                            "available": observed is not None,
                            "reason_code": getattr(service_ref.observer, "last_reason_code", "TARGET_OBSERVATION_UNAVAILABLE"),
                            "snapshot": snapshot,
                        },
                    )
                    return
                prefix = "/v1/commands/"
                if not self.path.startswith(prefix):
                    self._json(HTTPStatus.NOT_FOUND, {"error": "NOT_FOUND"})
                    return
                try:
                    self._json(HTTPStatus.OK, service_ref.command_status(unquote(self.path[len(prefix) :])))
                except KeyError:
                    self._json(HTTPStatus.NOT_FOUND, {"error": "COMMAND_NOT_FOUND"})

            def log_message(self, _: str, *args: object) -> None:
                return

        self._server = ThreadingHTTPServer((host, port), Handler)
        self._server.daemon_threads = True
        self._server.socket = ssl_context.wrap_socket(self._server.socket, server_side=True)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)

    @property
    def base_url(self) -> str:
        host, port = cast(tuple[str, int], self._server.server_address)
        return f"https://{host}:{port}"

    def start(self) -> None:
        self._thread.start()

    def close(self) -> None:
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=2)

    def __enter__(self) -> ShadowAgentHTTPServer:
        self.start()
        return self

    def __exit__(self, *_: object) -> None:
        self.close()
