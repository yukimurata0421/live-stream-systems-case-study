from __future__ import annotations

import io
import json
import tempfile
import unittest
from contextlib import redirect_stderr
from pathlib import Path

from stream_core.engine.ffmpeg_stderr import FFmpegStderrCapture, redact_stderr_line


class FFmpegStderrCaptureTests(unittest.TestCase):
    def test_redacts_stream_key_and_persists_timestamped_jsonl(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "ffmpeg_stderr.jsonl"
            capture = FFmpegStderrCapture(
                path=path,
                max_bytes=8192,
                backup_count=2,
                run_id="run-1",
                ffmpeg_pid=321,
                stream_generation="run-1:0:321",
            )
            with redirect_stderr(io.StringIO()):
                capture.start(
                    io.BytesIO(
                        b"Error writing rtmps://a.rtmps.youtube.com:443/live2/secret-key\n"
                        b"Immediate exit requested\n"
                    )
                )
                summary = capture.stop()

            rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
            self.assertEqual(summary["line_count"], 2)
            self.assertEqual(rows[0]["ffmpeg_pid"], 321)
            self.assertIn("/live2/***", rows[0]["line"])
            self.assertNotIn("secret-key", path.read_text(encoding="utf-8"))
            self.assertEqual(rows[-1]["line"], "Immediate exit requested")

    def test_rotates_while_capture_is_active(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "ffmpeg_stderr.jsonl"
            capture = FFmpegStderrCapture(
                path=path,
                max_bytes=4096,
                backup_count=2,
                run_id="run-2",
                ffmpeg_pid=654,
                stream_generation="run-2:0:654",
            )
            with redirect_stderr(io.StringIO()):
                capture.start(io.BytesIO((("x" * 3000) + "\n" + ("y" * 3000) + "\n").encode()))
                summary = capture.stop()

            self.assertGreaterEqual(int(summary["rotated_count"]), 1)
            self.assertTrue(path.exists())
            self.assertTrue(path.with_name(path.name + ".1").exists())

    def test_redaction_leaves_non_secret_url_context(self) -> None:
        line = redact_stderr_line("rtmps://host:443/live2/key: Immediate exit requested")
        self.assertEqual(line, "rtmps://host:443/live2/*** Immediate exit requested")

    def test_keeps_draining_when_persistence_cannot_be_opened(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            parent_file = Path(td) / "not-a-directory"
            parent_file.write_text("occupied", encoding="utf-8")
            capture = FFmpegStderrCapture(
                path=parent_file / "ffmpeg_stderr.jsonl",
                max_bytes=8192,
                backup_count=2,
                run_id="run-3",
                ffmpeg_pid=987,
                stream_generation="run-3:0:987",
            )
            mirrored = io.StringIO()

            with redirect_stderr(mirrored):
                capture.start(io.BytesIO(b"first line\nsecond line\n"))
                summary = capture.stop()

            self.assertEqual(summary["line_count"], 2)
            self.assertIn("persistence disabled", str(summary["capture_error"]))
            self.assertEqual(mirrored.getvalue(), "first line\nsecond line\n")


if __name__ == "__main__":
    unittest.main()
