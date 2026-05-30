from __future__ import annotations

import importlib
import json
import os
import sys
import tempfile
from contextlib import contextmanager
from pathlib import Path
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src" / "watchers"))

import youtube_watchdog_config  # type: ignore
import youtube_watchdog_state  # type: ignore


class YouTubeWatchdogStateTests(unittest.TestCase):
    def test_classify_judgment(self) -> None:
        self.assertEqual(youtube_watchdog_state.classify_judgment("ok", True), ("ok", "availability_healthy"))
        self.assertEqual(
            youtube_watchdog_state.classify_judgment("startup_grace", True),
            ("deferred", "non_actionable_observation"),
        )
        self.assertEqual(
            youtube_watchdog_state.classify_judgment("degraded_public", True),
            ("deferred", "non_actionable_observation"),
        )
        self.assertEqual(
            youtube_watchdog_state.classify_judgment("quota_guard", True),
            ("deferred", "non_actionable_observation"),
        )
        self.assertEqual(
            youtube_watchdog_state.classify_judgment("warn", False),
            ("ng", "availability_unhealthy"),
        )

    def test_ok_event_throttle(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            with mock.patch.dict(
                os.environ,
                {
                    "STREAM_RUNTIME_STATE_DIR": td,
                    "YTW_OK_LOG_EVERY_SEC": "300",
                },
                clear=False,
            ):
                importlib.reload(youtube_watchdog_config)
                mod = importlib.reload(youtube_watchdog_state)
                self.assertTrue(mod.should_emit_ok_event(now_ts=1000))
                self.assertFalse(mod.should_emit_ok_event(now_ts=1200))
                self.assertTrue(mod.should_emit_ok_event(now_ts=1300))

    def test_write_stats_updates_json(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            with mock.patch.dict(
                os.environ,
                {
                    "STREAM_RUNTIME_STATE_DIR": td,
                },
                clear=False,
            ):
                cfg = importlib.reload(youtube_watchdog_config)
                mod = importlib.reload(youtube_watchdog_state)
                mod.write_stats({"status": "ok", "healthy": True})
                stats_path = Path(cfg.STATS_FILE)
                self.assertTrue(stats_path.exists())
                raw = stats_path.read_text(encoding="utf-8")
                self.assertIn('"status":"ok"', raw)
                self.assertIn('"healthy":true', raw.replace(" ", ""))
                payload = json.loads(raw)
                self.assertEqual(payload.get("ts_utc"), payload.get("stats_file_updated_at_utc"))

    def test_write_stats_derives_remote_probe_sample_fields(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            with mock.patch.dict(
                os.environ,
                {
                    "STREAM_RUNTIME_STATE_DIR": td,
                },
                clear=False,
            ):
                cfg = importlib.reload(youtube_watchdog_config)
                mod = importlib.reload(youtube_watchdog_state)
                mod.write_stats(
                    {
                        "status": "warn",
                        "video_id": "VID123",
                        "ffmpeg_pid": 222,
                        "oauth_checked_ts_utc": "2026-05-08T01:00:00Z",
                        "data_api_checked_ts_utc": "2026-05-08T01:00:05Z",
                    }
                )
                payload = json.loads(Path(cfg.STATS_FILE).read_text(encoding="utf-8"))
                self.assertEqual(payload.get("remote_probe_ts_utc"), "2026-05-08T01:00:05Z")
                self.assertEqual(payload.get("remote_source"), "data_api_oauth")
                self.assertEqual(payload.get("remote_probe_source"), "data_api_oauth")
                self.assertEqual(payload.get("remote_sample_source"), "data_api_oauth")
                self.assertEqual(payload.get("ffmpeg_generation"), "ffmpeg_pid=222")
                self.assertRegex(str(payload.get("remote_sample_id", "")), r"^rps-[0-9a-f]{16}$")

    def test_video_resolver_state_roundtrip(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            with mock.patch.dict(
                os.environ,
                {
                    "STREAM_RUNTIME_STATE_DIR": td,
                },
                clear=False,
            ):
                cfg = importlib.reload(youtube_watchdog_config)
                mod = importlib.reload(youtube_watchdog_state)
                payload = {"video_id": "abc123", "resolved_ts": 12345}
                mod.save_video_resolver_state(payload)
                loaded = mod.load_video_resolver_state()
                self.assertEqual(loaded.get("video_id"), "abc123")
                self.assertEqual(int(loaded.get("resolved_ts", 0)), 12345)
                self.assertTrue(Path(cfg.VIDEO_RESOLVER_STATE_FILE).exists())

    def test_quota_state_auto_clears_after_expiry(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            with mock.patch.dict(
                os.environ,
                {
                    "STREAM_RUNTIME_STATE_DIR": td,
                },
                clear=False,
            ):
                cfg = importlib.reload(youtube_watchdog_config)
                mod = importlib.reload(youtube_watchdog_state)
                mod.save_quota_state(
                    {
                        "quota_exhausted": True,
                        "quota_exhausted_until_ts": 100,
                    }
                )
                active, state = mod.quota_exhausted_active(now_ts=101)
                self.assertFalse(active)
                self.assertFalse(bool(state.get("quota_exhausted")))
                self.assertTrue(Path(cfg.QUOTA_STATE_FILE).exists())

    def test_update_quota_state_does_not_reenter_lock(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            with mock.patch.dict(
                os.environ,
                {
                    "STREAM_RUNTIME_STATE_DIR": td,
                },
                clear=False,
            ):
                mod = importlib.reload(youtube_watchdog_state)
                lock_depth = {"value": 0}

                @contextmanager
                def fake_lock(_path):
                    lock_depth["value"] += 1
                    if lock_depth["value"] > 1:
                        raise AssertionError("lock re-entered")
                    try:
                        yield
                    finally:
                        lock_depth["value"] -= 1

                with mock.patch.object(mod, "_file_lock", side_effect=fake_lock):
                    with mock.patch.object(mod, "_load_json_file_unlocked", return_value={"quota_exhausted": False}):
                        saved = {}

                        def save_hook(_path, payload):
                            saved.clear()
                            saved.update(payload)

                        with mock.patch.object(mod, "_write_json_file_unlocked", side_effect=save_hook):
                            result = mod.update_quota_state(
                                lambda state: ({**state, "quota_exhausted": True, "quota_exhausted_until_ts": 123}, "ok")
                            )

                self.assertEqual(result, "ok")
                self.assertTrue(bool(saved.get("quota_exhausted")))
                self.assertEqual(int(saved.get("quota_exhausted_until_ts", 0)), 123)


if __name__ == "__main__":
    unittest.main()
