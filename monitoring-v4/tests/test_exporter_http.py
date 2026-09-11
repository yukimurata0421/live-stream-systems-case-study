from __future__ import annotations

import http.client
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from stream_contracts.monitoring_v4.time import utc_text
from stream_monitoring_v4.commands import exporter
from stream_monitoring_v4.storage.repository import MonitoringRepository

from tests.helpers import BASE_TS


class ExporterHttpTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.repository = MonitoringRepository(Path(self.temporary.name) / "monitoring.sqlite3")
        self.repository.initialize(applied_at=utc_text(BASE_TS))

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _request(self, path: str) -> tuple[int, bytes, str]:
        server = exporter.ThreadingHTTPServer(
            ("127.0.0.1", 0),
            exporter.handler(self.repository, "test-revision"),
        )
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            connection = http.client.HTTPConnection(
                "127.0.0.1",
                server.server_address[1],
                timeout=2,
            )
            try:
                connection.request("GET", path)
                response = connection.getresponse()
                body = response.read()
                return response.status, body, response.getheader("Content-Type", "")
            finally:
                connection.close()
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)
            self.assertFalse(thread.is_alive())

    def test_liveness_health_metrics_and_unknown_routes(self) -> None:
        self.assertEqual(self._request("/livez")[:2], (200, b"alive\n"))
        self.assertEqual(self._request("/healthz")[:2], (200, b"ok\n"))
        status, body, content_type = self._request("/metrics")
        self.assertEqual(status, 200)
        self.assertIn(b"stream_v3_monitoring_v4_build_info", body)
        self.assertIn("text/plain", content_type)
        self.assertEqual(self._request("/not-found")[0], 404)

    def test_health_distinguishes_database_and_metric_failures(self) -> None:
        with patch.object(self.repository, "ping", return_value=False), patch.object(
            exporter, "render_metrics"
        ) as render:
            self.assertEqual(
                exporter._health_safely(self.repository, "test-revision"),
                (False, b"database unavailable\n"),
            )
        render.assert_not_called()

        with patch.object(
            exporter,
            "render_metrics",
            side_effect=ValueError("corrupt persisted JSON"),
        ):
            self.assertEqual(
                exporter._health_safely(self.repository, "test-revision"),
                (False, b"metrics unavailable\n"),
            )

    def test_main_closes_repository_after_server_returns(self) -> None:
        class Repository:
            def __init__(self) -> None:
                self.close_count = 0

            def close(self) -> None:
                self.close_count += 1

        class Server:
            def __init__(self, address, handler_class) -> None:
                self.address = address
                self.handler_class = handler_class

            def serve_forever(self) -> None:
                return None

        repository = Repository()
        with patch.object(exporter, "repository_from_args", return_value=repository), patch.object(
            exporter, "wait_for_integrity"
        ) as wait, patch.object(exporter, "ThreadingHTTPServer", Server):
            self.assertEqual(
                exporter.main(
                    [
                        "--db",
                        str(Path(self.temporary.name) / "monitoring.sqlite3"),
                        "--build-revision",
                        "test-revision",
                        "--port",
                        "19118",
                    ]
                ),
                0,
            )
        wait.assert_called_once()
        self.assertEqual(repository.close_count, 1)


if __name__ == "__main__":
    unittest.main()
