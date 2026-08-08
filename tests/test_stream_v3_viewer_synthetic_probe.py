from __future__ import annotations

import importlib.util
import json
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock


def load_probe():
    path = Path(__file__).resolve().parents[1] / "ops" / "scripts" / "stream_v3_viewer_synthetic_probe.py"
    spec = importlib.util.spec_from_file_location("stream_v3_viewer_synthetic_probe", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class ViewerSyntheticProbeTests(unittest.TestCase):
    def test_default_paths_are_derived_from_public_checkout(self) -> None:
        probe = load_probe()
        expected_repo_root = Path(__file__).resolve().parents[1]

        self.assertEqual(probe.DEFAULT_REPO_ROOT, expected_repo_root)
        self.assertEqual(
            probe.DEFAULT_STATE_ROOT,
            expected_repo_root / ".state" / "observability-monitor",
        )
        self.assertNotIn("/home/yuki/projects/stream_v3", str(probe.DEFAULT_REPO_ROOT))

    def test_visual_failure_requires_repeated_samples_for_critical_counter(self) -> None:
        probe = load_probe()
        first = probe.evaluate_result(
            {},
            checked_at_utc="2026-08-07T00:00:00Z",
            video_id="abcdefghijk",
            api_live_state="live",
            capture={"frame_ok": True, "black_detected": True, "freeze_detected": False},
            error="",
            duration_sec=8,
        )
        second = probe.evaluate_result(
            first,
            checked_at_utc="2026-08-07T00:05:00Z",
            video_id="abcdefghijk",
            api_live_state="live",
            capture={"frame_ok": True, "black_detected": True, "freeze_detected": False},
            error="",
            duration_sec=8,
        )

        self.assertEqual(first["status"], "failed")
        self.assertEqual(first["consecutive_visual_failures"], 1)
        self.assertEqual(second["consecutive_visual_failures"], 2)
        self.assertEqual(second["consecutive_probe_failures"], 0)

    def test_healthy_frame_resets_failure_counters(self) -> None:
        probe = load_probe()
        payload = probe.evaluate_result(
            {"consecutive_probe_failures": 3, "consecutive_visual_failures": 2},
            checked_at_utc="2026-08-07T00:10:00Z",
            video_id="abcdefghijk",
            api_live_state="live",
            capture={"frame_ok": True, "black_detected": False, "freeze_detected": False},
            error="",
            duration_sec=8,
        )

        self.assertEqual(payload["status"], "healthy")
        self.assertEqual(payload["consecutive_probe_failures"], 0)
        self.assertEqual(payload["consecutive_visual_failures"], 0)

    def test_safe_detail_redacts_signed_viewer_url(self) -> None:
        probe = load_probe()
        value = probe.safe_detail("failed https://example.invalid/video.m3u8?signature=secret now")
        self.assertNotIn("signature", value)
        self.assertIn("<redacted-url>", value)

    def test_identical_frame_hash_marks_cross_sample_freeze(self) -> None:
        probe = load_probe()
        fingerprint = "0" * 36
        payload = probe.evaluate_result(
            {
                "capture_fingerprint": fingerprint,
                "capture_sha256": "same-frame",
                "consecutive_visual_failures": 0,
            },
            checked_at_utc="2026-08-07T00:05:00Z",
            video_id="abcdefghijk",
            api_live_state="live",
            capture={
                "frame_ok": True,
                "black_detected": False,
                "capture_fingerprint": fingerprint,
                "capture_sha256": "same-frame",
            },
            error="",
            duration_sec=8,
        )

        self.assertTrue(payload["freeze_detected"])
        self.assertEqual(payload["consecutive_visual_failures"], 1)

    def test_probe_error_increments_probe_counter_and_preserves_last_good_evidence(self) -> None:
        probe = load_probe()
        payload = probe.evaluate_result(
            {
                "consecutive_probe_failures": 2,
                "consecutive_visual_failures": 3,
                "last_success_at_utc": "2026-08-07T00:00:00Z",
                "last_visual_ok_at_utc": "2026-08-07T00:00:00Z",
            },
            checked_at_utc="2026-08-07T00:15:00Z",
            video_id="abcdefghijk",
            api_live_state="live",
            capture={},
            error="failed https://example.invalid/video.m3u8?signature=secret",
            duration_sec=25,
        )

        self.assertEqual(payload["status"], "degraded")
        self.assertFalse(payload["frame_ok"])
        self.assertEqual(payload["consecutive_probe_failures"], 3)
        self.assertEqual(payload["consecutive_visual_failures"], 0)
        self.assertEqual(payload["last_success_at_utc"], "2026-08-07T00:00:00Z")
        self.assertEqual(payload["last_visual_ok_at_utc"], "2026-08-07T00:00:00Z")
        self.assertNotIn("signature", payload["reason"])
        self.assertIn("<redacted-url>", payload["reason"])

    def test_changed_frame_is_healthy_and_reports_fingerprint_delta(self) -> None:
        probe = load_probe()
        payload = probe.evaluate_result(
            {
                "capture_fingerprint": "0" * 36,
                "capture_sha256": "old-frame",
                "consecutive_visual_failures": 2,
            },
            checked_at_utc="2026-08-07T00:10:00Z",
            video_id="abcdefghijk",
            api_live_state="live",
            capture={
                "frame_ok": True,
                "black_detected": False,
                "capture_fingerprint": "f" * 36,
                "capture_sha256": "new-frame",
            },
            error="",
            duration_sec=8,
        )

        self.assertEqual(payload["status"], "healthy")
        self.assertFalse(payload["freeze_detected"])
        self.assertEqual(payload["fingerprint_delta"], 144)
        self.assertEqual(payload["consecutive_visual_failures"], 0)

    def test_resolve_viewer_url_timeout_is_classified(self) -> None:
        probe = load_probe()
        timeout = subprocess.TimeoutExpired(cmd=["yt-dlp"], timeout=5)

        with mock.patch.object(probe, "run", side_effect=timeout):
            viewer_url, error = probe.resolve_viewer_url(
                "abcdefghijk",
                yt_dlp="yt-dlp",
                timeout_sec=5,
            )

        self.assertEqual(viewer_url, "")
        self.assertEqual(error, "yt_dlp_timeout")

    def test_capture_timeout_preserves_last_good_capture(self) -> None:
        probe = load_probe()
        with tempfile.TemporaryDirectory() as td:
            output_path = Path(td) / "latest.jpg"
            output_path.write_bytes(b"last-good-frame")
            timeout = subprocess.TimeoutExpired(cmd=["ffmpeg"], timeout=10)

            with mock.patch.object(probe, "run", side_effect=timeout):
                capture, error = probe.capture_frame(
                    "https://example.invalid/viewer.m3u8",
                    output_path,
                    ffmpeg="ffmpeg",
                    timeout_sec=10,
                )

            self.assertEqual(capture, {})
            self.assertEqual(error, "ffmpeg_timeout")
            self.assertEqual(output_path.read_bytes(), b"last-good-frame")
            self.assertEqual(list(output_path.parent.glob(".latest.*.jpg")), [])

    def test_main_persists_not_live_resolver_state_without_external_probe(self) -> None:
        probe = load_probe()
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            resolver_file = root / "resolver.json"
            state_file = root / "viewer.json"
            history_file = root / "viewer.jsonl"
            capture_file = root / "capture" / "latest.jpg"
            resolver_file.write_text(
                json.dumps({"video_id": "abcdefghijk", "api_live_state": "ended"}),
                encoding="utf-8",
            )

            with (
                mock.patch.object(probe, "resolve_viewer_url") as resolve,
                mock.patch.object(probe, "capture_frame") as capture,
                mock.patch.object(probe, "utc_now", return_value="2026-08-07T00:20:00Z"),
                mock.patch.object(probe.time, "monotonic", side_effect=[10.0, 11.25]),
                mock.patch("builtins.print"),
            ):
                rc = probe.main(
                    [
                        "--resolver-state-file",
                        str(resolver_file),
                        "--state-file",
                        str(state_file),
                        "--history-file",
                        str(history_file),
                        "--capture-file",
                        str(capture_file),
                    ]
                )

            resolve.assert_not_called()
            capture.assert_not_called()
            state = json.loads(state_file.read_text(encoding="utf-8"))
            history = json.loads(history_file.read_text(encoding="utf-8").strip())
            self.assertEqual(rc, 0)
            self.assertEqual(state["status"], "degraded")
            self.assertEqual(state["reason"], "resolver_state_not_live:ended")
            self.assertEqual(state["consecutive_probe_failures"], 1)
            self.assertEqual(state["duration_sec"], 1.25)
            self.assertEqual(history["status"], "degraded")
            self.assertEqual(history["reason"], "resolver_state_not_live:ended")


if __name__ == "__main__":
    unittest.main()
