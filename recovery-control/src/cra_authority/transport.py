from __future__ import annotations

import json
import ssl
import urllib.error
import urllib.request
from typing import Any


class SignedHTTPSClient:
    def __init__(self, base_url: str, ssl_context: ssl.SSLContext, *, timeout: float = 2.0) -> None:
        if not base_url.startswith("https://"):
            raise ValueError("Protocol v1 transport requires HTTPS")
        self.base_url = base_url.rstrip("/")
        self.ssl_context = ssl_context
        self.timeout = timeout

    def post_command(self, envelope: dict[str, Any]) -> dict[str, Any]:
        return self._post("/v1/commands", envelope)

    def post_shadow_command(self, envelope: dict[str, Any]) -> dict[str, Any]:
        return self._post("/v1/shadow/commands", envelope)

    def post_shadow_heartbeat(self, envelope: dict[str, Any]) -> dict[str, Any]:
        return self._post("/v1/shadow/heartbeats", envelope)

    def post_shadow_maintenance_snapshot(self, snapshot: dict[str, Any]) -> dict[str, Any]:
        return self._post("/v1/shadow/maintenance-snapshots", snapshot)

    def issue_challenge(self, target_id: str) -> dict[str, Any]:
        return self._post("/v1/shadow/reconciliation/challenge", {"target_id": target_id})

    def commit(self, envelope: dict[str, Any]) -> dict[str, Any]:
        return self._post("/v1/shadow/reconciliation/commit", envelope)

    def journal_page(self, envelope: dict[str, Any]) -> dict[str, Any]:
        return self._post("/v1/shadow/reconciliation/journal-page", envelope)

    def _post(self, path: str, envelope: dict[str, Any]) -> dict[str, Any]:
        request = urllib.request.Request(
            f"{self.base_url}{path}",
            data=json.dumps(envelope, separators=(",", ":")).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(request, context=self.ssl_context, timeout=self.timeout) as response:
            return dict(json.loads(response.read()))

    def get_status(self, command_id: str) -> dict[str, Any]:
        request = urllib.request.Request(
            f"{self.base_url}/v1/commands/{command_id}",
            method="GET",
        )
        with urllib.request.urlopen(request, context=self.ssl_context, timeout=self.timeout) as response:
            return dict(json.loads(response.read()))

    def get_shadow_status(self) -> dict[str, Any]:
        return self._get("/v1/shadow/status")

    def get_shadow_target(self) -> dict[str, Any]:
        return self._get("/v1/shadow/target")

    def _get(self, path: str) -> dict[str, Any]:
        request = urllib.request.Request(f"{self.base_url}{path}", method="GET")
        with urllib.request.urlopen(request, context=self.ssl_context, timeout=self.timeout) as response:
            return dict(json.loads(response.read()))
