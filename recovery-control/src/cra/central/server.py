"""CRA HTTP endpoint. Read-only for policy, append-only for audit. It never talks to the Executor."""

from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from cra import audit_ack, signing
from cra.central.store import CentralStore

MAX_BODY = 4 << 20


def make_handler(
    store: CentralStore, policy_path: Path, agent_keys: dict[str, str], ack_key: Ed25519PrivateKey
) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        server_version = "cra-central/1"

        def _reply(self, code: int, value: Any) -> None:
            body = json.dumps(value, separators=(",", ":")).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self) -> None:
            if self.path == "/v1/policy":
                try:
                    self._reply(200, json.loads(policy_path.read_text()))
                except (OSError, json.JSONDecodeError):
                    self._reply(503, {"error": "POLICY_UNAVAILABLE"})
            elif self.path == "/v1/health":
                self._reply(200, {"ok": True})
            else:
                self._reply(404, {"error": "NOT_FOUND"})

        def do_POST(self) -> None:
            if self.path != "/v1/audit":
                self._reply(404, {"error": "NOT_FOUND"})
                return
            length = int(self.headers.get("Content-Length") or 0)
            if not 0 < length <= MAX_BODY:
                self._reply(413, {"error": "BODY_SIZE"})
                return
            try:
                envelope = json.loads(self.rfile.read(length))
                body, signature = envelope["body"], envelope["signature"]
                agent_id, instance_id, records = body["agent_id"], body["instance_id"], body["records"]
                if not isinstance(records, list) or not isinstance(instance_id, str) or not instance_id:
                    raise ValueError
            except (ValueError, KeyError, TypeError):
                self._reply(400, {"error": "MALFORMED"})
                return
            public_key = agent_keys.get(agent_id)
            if public_key is None or not signing.verify(public_key, body, signature):
                self._reply(403, {"error": "SIGNATURE_INVALID"})
                return
            try:
                acked = store.ingest(agent_id, instance_id, records)
            except (KeyError, TypeError, ValueError):
                self._reply(400, {"error": "RECORD_INVALID"})
                return
            self._reply(200, audit_ack.sign_ack(ack_key, body, acked))

        def log_message(self, format: str, *args: Any) -> None:
            if not (args and str(args[1]) == "200"):
                super().log_message(format, *args)

    return Handler


def serve(store: CentralStore, policy_path: Path, agent_keys: dict[str, str], host: str, port: int, *, ack_key: Ed25519PrivateKey) -> None:
    server = ThreadingHTTPServer((host, port), make_handler(store, policy_path, agent_keys, ack_key))
    try:
        server.serve_forever()
    finally:
        server.server_close()
