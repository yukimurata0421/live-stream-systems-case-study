from __future__ import annotations

import argparse
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

from stream_monitoring_v4.exporter.metrics import render_metrics
from stream_monitoring_v4.storage.factory import (
    Repository,
    add_repository_arguments,
    repository_from_args,
    wait_for_integrity,
)


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description="Serve read-only Monitoring v4 shadow metrics")
    add_repository_arguments(result)
    result.add_argument("--host", default="127.0.0.1")
    result.add_argument("--port", type=int, default=9118)
    result.add_argument("--build-revision", required=True)
    result.add_argument("--startup-db-timeout-sec", type=int, default=180)
    return result


def _render_safely(repository: Repository, build_revision: str) -> tuple[bool, bytes]:
    try:
        body = render_metrics(
            repository,
            now_ts=int(time.time()),
            build_revision=build_revision,
        ).encode("utf-8")
    except Exception:
        return False, b"metrics unavailable\n"
    return True, body


def _health_safely(repository: Repository, build_revision: str) -> tuple[bool, bytes]:
    if not repository.ping():
        return False, b"database unavailable\n"
    metrics_ok, _metrics_body = _render_safely(repository, build_revision)
    if not metrics_ok:
        return False, b"metrics unavailable\n"
    return True, b"ok\n"


def handler(repository: Repository, build_revision: str) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            if self.path == "/livez":
                body = b"alive\n"
                self.send_response(200)
            elif self.path == "/healthz":
                ok, body = _health_safely(repository, build_revision)
                self.send_response(200 if ok else 503)
            elif self.path in {"/", "/metrics"}:
                ok, body = _render_safely(repository, build_revision)
                self.send_response(200 if ok else 503)
                self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
            else:
                self.send_error(404)
                return
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, fmt: str, *args: Any) -> None:
            return

    return Handler


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    repository = repository_from_args(args, application_name="stream-monitoring-v4-exporter")
    try:
        wait_for_integrity(
            repository,
            timeout_sec=max(0, int(args.startup_db_timeout_sec)),
        )
        server = ThreadingHTTPServer((args.host, args.port), handler(repository, args.build_revision))
        server.serve_forever()
        return 0
    finally:
        close = getattr(repository, "close", None)
        if callable(close):
            close()


if __name__ == "__main__":
    raise SystemExit(main())
