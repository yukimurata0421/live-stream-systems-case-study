from __future__ import annotations

import json
import socket
from collections.abc import Mapping
from pathlib import Path
from typing import Any


class EffectClient:
    def __init__(self, socket_path: Path, *, timeout_seconds: float = 2.0) -> None:
        self.socket_path = socket_path
        self.timeout_seconds = timeout_seconds

    def execute(self, request: Mapping[str, Any]) -> dict[str, Any]:
        payload = json.dumps(dict(request), sort_keys=True, separators=(",", ":")).encode() + b"\n"
        if len(payload) > 65536:
            raise ValueError("REQUEST_TOO_LARGE")
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
            connection.settimeout(self.timeout_seconds)
            connection.connect(str(self.socket_path))
            connection.sendall(payload)
            response = bytearray()
            while len(response) <= 65536:
                chunk = connection.recv(4096)
                if not chunk:
                    break
                response.extend(chunk)
                if b"\n" in chunk:
                    break
        if len(response) > 65536:
            raise ValueError("RESPONSE_TOO_LARGE")
        value = json.loads(bytes(response).split(b"\n", 1)[0])
        if not isinstance(value, dict):
            raise ValueError("RESPONSE_INVALID")
        return value

    def unresolved(self) -> dict[str, Any]:
        return self.execute({"schema_version": "runtime.effect_unresolved_query.v1"})

    def status(self, request_id: str) -> dict[str, Any]:
        return self.execute(
            {
                "schema_version": "runtime.effect_request_status_query.v1",
                "request_id": request_id,
            }
        )

    def status_by_correlation(self, correlation_id: str) -> dict[str, Any]:
        return self.execute(
            {
                "schema_version": "runtime.effect_correlation_status_query.v1",
                "correlation_id": correlation_id,
            }
        )

    def reconcile(self, request: Mapping[str, Any]) -> dict[str, Any]:
        value = dict(request)
        if value.get("schema_version") != "runtime.effect_reconciliation_request.v1":
            raise ValueError("RECONCILIATION_SCHEMA_INVALID")
        return self.execute(value)
