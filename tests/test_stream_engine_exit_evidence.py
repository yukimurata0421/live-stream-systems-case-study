from __future__ import annotations

import json
import os
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src" / "stream_core"))

import stream_engine  # type: ignore


class StreamEngineExitEvidenceTests(unittest.TestCase):
    def test_fast_recovery_restart_reason_path_is_shared_by_default(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            shared_path = Path(td) / "restart_reason.json"
            env = {
                "BASE_DIR": td,
                "TEST_MODE": "1",
                "FR_RESTART_REASON_FILE": str(shared_path),
            }
            with mock.patch.dict(os.environ, env, clear=True):
                cfg = stream_engine.load_config()

            self.assertEqual(cfg.restart_reason_file, shared_path)

    def test_correlates_recovery_action_transport_and_stderr(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            restart_reason = base / "restart_reason.json"
            transport_snapshot = base / "transport.json"
            now = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
            restart_reason.write_text(
                json.dumps(
                    {
                        "ts_utc": now,
                        "ffmpeg_pid": 222,
                        "reason": "tcp_stall",
                        "source": "fast_recovery",
                        "controller_id": "dell_fast_recovery",
                        "recovery_action_id": "fra-test",
                        "idempotency_key": "dell_fast_recovery:ffmpeg_child:222:tcp_stall:1",
                        "requested_signal": "SIGTERM",
                        "recovery_scope": "ffmpeg_child",
                    }
                ),
                encoding="utf-8",
            )
            transport_snapshot.write_text(
                json.dumps(
                    {
                        "ts_utc": now,
                        "ffmpeg_pid": 222,
                        "metrics": {
                            "bytes_acked": 123,
                            "send_q": 456,
                            "notsent": 456,
                            "unacked": 7,
                            "rto_ms": 204,
                        },
                    }
                ),
                encoding="utf-8",
            )
            env = {
                "BASE_DIR": td,
                "TEST_MODE": "1",
                "RESTART_REASON_FILE": str(restart_reason),
                "FR_TRANSPORT_SNAPSHOT_FILE": str(transport_snapshot),
                "PRE_FFMPEG_RESTART_CONTEXT_MAX_AGE_SEC": "300",
            }
            with mock.patch.dict(os.environ, env, clear=False):
                engine = stream_engine.StreamEngine(stream_engine.load_config())

            evidence = engine.ffmpeg_exit_evidence(
                ffmpeg_pid=222,
                exit_code=255,
                ffmpeg_uptime_sec=91,
                stderr_summary={"last_line": "Immediate exit requested", "line_count": 4},
            )

            self.assertTrue(evidence["expected_exit"])
            self.assertEqual(evidence["exit_class"], "recovery_requested")
            self.assertEqual(evidence["termination_initiator"], "dell_fast_recovery")
            self.assertEqual(evidence["recovery_action_id"], "fra-test")
            self.assertEqual(evidence["requested_signal"], "SIGTERM")
            self.assertEqual(evidence["last_transport_snapshot"]["metrics"]["rto_ms"], 204)
            self.assertEqual(evidence["stderr"]["last_line"], "Immediate exit requested")

    def test_classifies_unmatched_nonzero_exit_as_process_error(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            env = {
                "BASE_DIR": td,
                "TEST_MODE": "1",
                "RESTART_REASON_FILE": str(Path(td) / "missing-reason.json"),
                "FR_TRANSPORT_SNAPSHOT_FILE": str(Path(td) / "missing-transport.json"),
            }
            with mock.patch.dict(os.environ, env, clear=False):
                engine = stream_engine.StreamEngine(stream_engine.load_config())

            evidence = engine.ffmpeg_exit_evidence(
                ffmpeg_pid=333,
                exit_code=255,
                ffmpeg_uptime_sec=12,
                stderr_summary={},
            )

            self.assertFalse(evidence["expected_exit"])
            self.assertEqual(evidence["exit_class"], "uncorrelated_process_error")
            self.assertEqual(evidence["termination_initiator"], "unknown")


if __name__ == "__main__":
    unittest.main()
