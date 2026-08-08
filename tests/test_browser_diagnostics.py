from __future__ import annotations

import tempfile
import unittest
import os
from pathlib import Path
from unittest import mock

from stream_core.engine.browser_diagnostics import read_new_diagnostics
from stream_core.engine.config import load_config


class BrowserDiagnosticsTests(unittest.TestCase):
    def test_runtime_log_dir_is_default_for_persistent_runtime_logs(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            with mock.patch.dict(
                os.environ,
                {"BASE_DIR": str(root / "app"), "STREAM_RUNTIME_LOG_DIR": str(root / "state" / "logs")},
                clear=True,
            ):
                cfg = load_config()

        self.assertEqual(cfg.browser_log_file, root / "state" / "logs" / "browser.log")
        self.assertEqual(cfg.event_log_file, root / "state" / "logs" / "stream_engine_events.jsonl")
        self.assertEqual(cfg.overlay_server_log_file, root / "state" / "logs" / "overlay_server.log")
        self.assertEqual(cfg.xvfb_log_file, root / "state" / "logs" / "xvfb.log")

    def test_only_new_known_diagnostics_are_structured(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "browser.log"
            path.write_text("startup https://signed.invalid/x?secret=yes\n", encoding="utf-8")
            offset = path.stat().st_size
            with path.open("a", encoding="utf-8") as handle:
                handle.write("ContextResult::kFatalFailure\n")

            new_offset, found = read_new_diagnostics(path, offset)

        self.assertGreater(new_offset, offset)
        self.assertEqual(found, [{"diagnostic": "webgl_context_fatal", "count": "1"}])

    def test_log_truncation_restarts_scan_from_beginning(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "browser.log"
            path.write_text("x" * 200, encoding="utf-8")
            old_offset = path.stat().st_size
            path.write_text("GPU process crashed\n", encoding="utf-8")

            _new_offset, found = read_new_diagnostics(path, old_offset)

        self.assertEqual(found, [{"diagnostic": "gpu_process_crashed", "count": "1"}])


if __name__ == "__main__":
    unittest.main()
