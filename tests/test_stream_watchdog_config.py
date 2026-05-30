from __future__ import annotations

import importlib
import os
import sys
import tempfile
from contextlib import ExitStack
from datetime import datetime, timezone
from pathlib import Path
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src" / "watchers"))

import stream_watchdog  # type: ignore


class StreamWatchdogConfigTests(unittest.TestCase):
    def _invoke_audio_low(
        self,
        stage_state: dict[str, int],
        *,
        fails: int,
        now_ts: int = 1000,
        transition_age: int | None = None,
        now_playing_heartbeat: bool = False,
    ) -> list[tuple[str, str]]:
        calls: list[tuple[str, str]] = []

        def read_int(path, default=0):
            if path == stream_watchdog.AUDIO_FAIL_COUNT_FILE:
                return fails
            return 0

        transition_detail = {
            "track_transition_age_sec": transition_age,
            "track_transition_within_grace": transition_age is not None and transition_age <= 30,
            "track_transition_grace_sec": 30,
            "bucket_boundary_nearest": "",
            "bucket_boundary_delta_sec": None,
            "bucket_boundary_abs_delta_sec": None,
            "bucket_boundary_within_grace": False,
            "bucket_boundary_grace_sec": 90,
            "now_playing_heartbeat": now_playing_heartbeat,
            "now_playing_status": "playing" if now_playing_heartbeat else "",
            "now_playing_title": "heartbeat-track" if now_playing_heartbeat else "",
        }

        with tempfile.TemporaryDirectory() as td, ExitStack() as stack:
            for patcher in (
                mock.patch.object(stream_watchdog, "WORK_DIR", Path(td)),
                mock.patch.object(stream_watchdog, "ENABLE_AUDIO_PROBE", True),
                mock.patch.object(stream_watchdog, "ENABLE_PULSE_PRECISION_PROBE", False),
                mock.patch.object(stream_watchdog, "ENABLE_VIDEO_FRAME_PROBE", False),
                mock.patch.object(stream_watchdog, "AUDIO_DJ_RESTART_FAILS", 2),
                mock.patch.object(stream_watchdog, "AUDIO_STREAM_RESTART_FAILS", 3),
                mock.patch.object(stream_watchdog, "AUDIO_TRACK_TRANSITION_GRACE_SEC", 30),
                mock.patch.object(stream_watchdog, "AUDIO_STAGE_WINDOW_SEC", 600),
                mock.patch.object(stream_watchdog, "now_epoch", return_value=now_ts),
                mock.patch.object(stream_watchdog, "is_service_stable", return_value=True),
                mock.patch.object(stream_watchdog, "service_uptime_sec", return_value=1000),
                mock.patch.object(stream_watchdog, "pulse_memfd_warning_recent", return_value=False),
                mock.patch.object(stream_watchdog, "pulse_server_ok", return_value=True),
                mock.patch.object(stream_watchdog, "stream_ffmpeg_count", return_value=1),
                mock.patch.object(stream_watchdog, "check_overlay_detail", return_value=(True, "ok")),
                mock.patch.object(stream_watchdog, "runtime_snapshot_age_sec", return_value=0),
                mock.patch.object(stream_watchdog, "pulse_source_exists", return_value=True),
                mock.patch.object(stream_watchdog, "audio_has_energy", return_value=False),
                mock.patch.object(stream_watchdog, "now_playing_transition_detail", return_value=transition_detail),
                mock.patch.object(stream_watchdog, "read_int_file", side_effect=read_int),
                mock.patch.object(stream_watchdog, "write_int_file"),
                mock.patch.object(stream_watchdog, "load_recovery_stage_state", return_value=stage_state),
                mock.patch.object(stream_watchdog, "save_recovery_stage_state"),
                mock.patch.object(stream_watchdog, "append_snapshot_timeline"),
                mock.patch.object(stream_watchdog, "append_event"),
                mock.patch.object(stream_watchdog, "record_watchdog_stats"),
                mock.patch.object(
                    stream_watchdog,
                    "restart_service",
                    side_effect=lambda unit, component, reason: calls.append((component, reason)),
                ),
            ):
                stack.enter_context(patcher)
            self.assertEqual(stream_watchdog.main(), 0)
        return calls

    def _invoke_pulse_source_missing(self, stage_state: dict[str, int], *, fails: int, now_ts: int = 1000) -> list[tuple[str, str]]:
        calls: list[tuple[str, str]] = []

        def read_int(path, default=0):
            if path == stream_watchdog.PULSE_SOURCE_MISSING_COUNT_FILE:
                return fails
            return 0

        with tempfile.TemporaryDirectory() as td, ExitStack() as stack:
            for patcher in (
                mock.patch.object(stream_watchdog, "WORK_DIR", Path(td)),
                mock.patch.object(stream_watchdog, "ENABLE_AUDIO_PROBE", True),
                mock.patch.object(stream_watchdog, "ENABLE_PULSE_PRECISION_PROBE", False),
                mock.patch.object(stream_watchdog, "ENABLE_VIDEO_FRAME_PROBE", False),
                mock.patch.object(stream_watchdog, "AUDIO_FAIL_THRESHOLD", 2),
                mock.patch.object(stream_watchdog, "AUDIO_STAGE_WINDOW_SEC", 600),
                mock.patch.object(stream_watchdog, "now_epoch", return_value=now_ts),
                mock.patch.object(stream_watchdog, "is_service_stable", return_value=True),
                mock.patch.object(stream_watchdog, "service_uptime_sec", return_value=1000),
                mock.patch.object(stream_watchdog, "pulse_memfd_warning_recent", return_value=False),
                mock.patch.object(stream_watchdog, "pulse_server_ok", return_value=True),
                mock.patch.object(stream_watchdog, "stream_ffmpeg_count", return_value=1),
                mock.patch.object(stream_watchdog, "check_overlay_detail", return_value=(True, "ok")),
                mock.patch.object(stream_watchdog, "runtime_snapshot_age_sec", return_value=0),
                mock.patch.object(stream_watchdog, "pulse_source_exists", return_value=False),
                mock.patch.object(stream_watchdog, "read_int_file", side_effect=read_int),
                mock.patch.object(stream_watchdog, "write_int_file"),
                mock.patch.object(stream_watchdog, "load_recovery_stage_state", return_value=stage_state),
                mock.patch.object(stream_watchdog, "save_recovery_stage_state"),
                mock.patch.object(stream_watchdog, "append_snapshot_timeline"),
                mock.patch.object(stream_watchdog, "append_event"),
                mock.patch.object(stream_watchdog, "record_watchdog_stats"),
                mock.patch.object(
                    stream_watchdog,
                    "restart_service",
                    side_effect=lambda unit, component, reason: calls.append((component, reason)),
                ),
            ):
                stack.enter_context(patcher)
            self.assertEqual(stream_watchdog.main(), 0)
        return calls

    def test_state_root_overrides_work_and_log_paths(self) -> None:
        with mock.patch.dict(
            os.environ,
            {
                "STREAM_RUNTIME_STATE_DIR": "/tmp/stream-watchdog-state",
            },
            clear=False,
        ):
            for key in (
                "WATCHDOG_WORK_DIR",
                "WATCHDOG_EVENT_LOG_FILE",
                "WATCHDOG_SNAPSHOT_TIMELINE_FILE",
                "SLO_FILE",
                "YTW_STATE_FILE",
                "RESTART_REASON_FILE",
            ):
                os.environ.pop(key, None)
            mod = importlib.reload(stream_watchdog)
            self.assertEqual(mod.WORK_DIR, Path("/tmp/stream-watchdog-state/watchdog"))
            self.assertEqual(mod.EVENT_LOG_FILE, Path("/tmp/stream-watchdog-state/logs/stream_watchdog_events.jsonl"))
            self.assertEqual(mod.SNAPSHOT_TIMELINE_FILE, Path("/tmp/stream-watchdog-state/logs/watchdog_state_timeline.jsonl"))
            self.assertEqual(mod.SLO_FILE, Path("/tmp/stream-watchdog-state/slo_snapshot.json"))
            self.assertEqual(mod.WATCHDOG_STATS_FILE, Path("/tmp/stream-watchdog-state/stream_watchdog_stats.json"))

    def test_default_state_root_uses_repo_local_v2_state_root(self) -> None:
        with mock.patch.dict(os.environ, {"HOME": "/home/testuser"}, clear=False):
            os.environ.pop("STREAM_RUNTIME_STATE_DIR", None)
            os.environ.pop("WATCHDOG_WORK_DIR", None)
            mod = importlib.reload(stream_watchdog)
            self.assertEqual(mod.WORK_DIR, ROOT / ".state" / "adsb-streamnew-v2" / "watchdog")

    def test_runtime_state_glob_supports_multiple_patterns(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            d1 = root / "a"
            d2 = root / "b"
            d1.mkdir()
            d2.mkdir()
            f1 = d1 / "stream_runtime_state_old.json"
            f2 = d2 / "stream_runtime_state_new.json"
            f1.write_text("{}", encoding="utf-8")
            f2.write_text("{}", encoding="utf-8")
            os.utime(f1, (1000, 1000))
            os.utime(f2, (2000, 2000))
            joined = f"{d1}/stream_runtime_state_*.json:{d2}/stream_runtime_state_*.json"

            with mock.patch.dict(os.environ, {"RUNTIME_STATE_GLOB": joined}, clear=False):
                mod = importlib.reload(stream_watchdog)
                picked = mod.pick_runtime_state_path()
                self.assertEqual(picked, f2)

    def test_watchdog_ok_logging_is_throttled(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            with mock.patch.dict(
                os.environ,
                {
                    "STREAM_RUNTIME_STATE_DIR": td,
                    "WATCHDOG_WORK_DIR": str(Path(td) / "watchdog"),
                    "WATCHDOG_OK_LOG_EVERY_SEC": "300",
                },
                clear=False,
            ):
                mod = importlib.reload(stream_watchdog)
                self.assertTrue(mod.should_emit_watchdog_ok(now_ts=1000))
                self.assertFalse(mod.should_emit_watchdog_ok(now_ts=1200))
                self.assertTrue(mod.should_emit_watchdog_ok(now_ts=1300))

    def test_watchdog_judgment_classification(self) -> None:
        self.assertEqual(stream_watchdog.classify_watchdog_judgment("ok"), "ok")
        self.assertEqual(stream_watchdog.classify_watchdog_judgment("warmup_grace"), "deferred")
        self.assertEqual(stream_watchdog.classify_watchdog_judgment("anomaly"), "ng")

    def test_overlay_detail_requires_map_proxy_and_adsb_json(self) -> None:
        responses = [
            mock.Mock(returncode=0, stdout='<title>Stream1090 Overlay</title><iframe id="map"></iframe>', stderr=""),
            mock.Mock(returncode=0, stdout="<html>tar1090</html>", stderr=""),
            mock.Mock(returncode=0, stdout='{"now":1000,"messages":10,"aircraft":[]}', stderr=""),
            mock.Mock(returncode=0, stdout='{"actualRange":{"last24h":{"points":[[36.0,140.0,30000]]}}}', stderr=""),
        ]
        with (
            mock.patch.object(stream_watchdog, "run", side_effect=responses),
            mock.patch.object(stream_watchdog, "now_epoch", return_value=1000),
            mock.patch.object(stream_watchdog, "write_json_file"),
        ):
            ok, reason = stream_watchdog.check_overlay_detail()
        self.assertTrue(ok)
        self.assertIn("outline ok", reason)

    def test_overlay_detail_rejects_missing_adsb_aircraft_list(self) -> None:
        responses = [
            mock.Mock(returncode=0, stdout='<title>Stream1090 Overlay</title><iframe id="map"></iframe>', stderr=""),
            mock.Mock(returncode=0, stdout="<html>tar1090</html>", stderr=""),
            mock.Mock(returncode=0, stdout='{"error":"bad gateway"}', stderr=""),
        ]
        with mock.patch.object(stream_watchdog, "run", side_effect=responses):
            ok, reason = stream_watchdog.check_overlay_detail()
        self.assertFalse(ok)
        self.assertIn("missing aircraft list", reason)

    def test_overlay_detail_rejects_missing_outline_points(self) -> None:
        responses = [
            mock.Mock(returncode=0, stdout='<title>Stream1090 Overlay</title><iframe id="map"></iframe>', stderr=""),
            mock.Mock(returncode=0, stdout="<html>tar1090</html>", stderr=""),
            mock.Mock(returncode=0, stdout='{"now":1000,"messages":10,"aircraft":[]}', stderr=""),
            mock.Mock(returncode=0, stdout='{"actualRange":{"last24h":{}}}', stderr=""),
        ]
        with (
            mock.patch.object(stream_watchdog, "run", side_effect=responses),
            mock.patch.object(stream_watchdog, "now_epoch", return_value=1000),
            mock.patch.object(stream_watchdog, "write_json_file"),
        ):
            ok, reason = stream_watchdog.check_overlay_detail()
        self.assertFalse(ok)
        self.assertIn("outline json missing points", reason)

    def test_overlay_outline_json_rejects_invalid_point_coordinates(self) -> None:
        ok, reason = stream_watchdog.check_overlay_outline_json(
            {"actualRange": {"last24h": {"points": [["bad", 140.0, 30000]]}}}
        )
        self.assertFalse(ok)
        self.assertIn("invalid point coordinates", reason)

    def test_overlay_unavailable_recovers_overlay_before_stream_restart(self) -> None:
        with (
            mock.patch.object(stream_watchdog, "OVERLAY_RECOVER_BEFORE_STREAM_RESTART", True),
            mock.patch.object(stream_watchdog, "append_event") as append_event,
            mock.patch.object(stream_watchdog, "append_snapshot_timeline"),
            mock.patch.object(stream_watchdog, "recover_overlay_server", return_value=(True, "overlay ok")) as recover,
            mock.patch.object(stream_watchdog, "restart_service") as restart_service,
            mock.patch.object(stream_watchdog, "record_watchdog_stats") as record_stats,
        ):
            stream_watchdog.handle_overlay_unavailable("overlay stream1090 proxy unavailable")

        recover.assert_called_once()
        restart_service.assert_not_called()
        append_event.assert_any_call(
            "overlay_unavailable",
            overlay_url=stream_watchdog.OVERLAY_URL,
            overlay_reason="overlay stream1090 proxy unavailable",
        )
        record_stats.assert_called_once()
        self.assertEqual(record_stats.call_args.args[0], "ok")

    def test_overlay_unavailable_restarts_stream_when_overlay_recovery_fails(self) -> None:
        with (
            mock.patch.object(stream_watchdog, "OVERLAY_RECOVER_BEFORE_STREAM_RESTART", True),
            mock.patch.object(stream_watchdog, "append_event"),
            mock.patch.object(stream_watchdog, "append_snapshot_timeline"),
            mock.patch.object(stream_watchdog, "recover_overlay_server", return_value=(False, "still bad")),
            mock.patch.object(stream_watchdog, "restart_service") as restart_service,
            mock.patch.object(stream_watchdog, "record_watchdog_stats") as record_stats,
        ):
            stream_watchdog.handle_overlay_unavailable("overlay index unavailable")

        restart_service.assert_called_once()
        self.assertEqual(restart_service.call_args.args[0], stream_watchdog.STREAM_SERVICE)
        self.assertEqual(restart_service.call_args.args[1], "stream")
        self.assertIn("overlay-only recovery failed", restart_service.call_args.args[2])
        record_stats.assert_called_once()
        self.assertEqual(record_stats.call_args.args[0], "anomaly")

    def test_adsb_freshness_rejects_stale_now_timestamp(self) -> None:
        with mock.patch.object(stream_watchdog, "ADSB_JSON_MAX_AGE_SEC", 30):
            ok, reason = stream_watchdog.check_adsb_freshness(
                {"now": 1000, "messages": 10, "aircraft": []},
                current_ts=1031,
            )
        self.assertFalse(ok)
        self.assertIn("stale", reason)

    def test_adsb_freshness_rejects_stalled_messages(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            state_file = Path(td) / "adsb_freshness.json"
            state_file.write_text('{"last_messages":10,"last_change_ts":1000}', encoding="utf-8")
            with (
                mock.patch.object(stream_watchdog, "ADSB_FRESHNESS_STATE_FILE", state_file),
                mock.patch.object(stream_watchdog, "ADSB_JSON_MAX_AGE_SEC", 30),
                mock.patch.object(stream_watchdog, "ADSB_MESSAGE_STALL_SEC", 120),
            ):
                ok, reason = stream_watchdog.check_adsb_freshness(
                    {"now": 1130, "messages": 10, "aircraft": []},
                    current_ts=1130,
                )
        self.assertFalse(ok)
        self.assertIn("messages stalled", reason)

    def test_adsb_freshness_accepts_messages_counter_reset(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            state_file = Path(td) / "adsb_freshness.json"
            state_file.write_text('{"last_messages":1000,"last_change_ts":1000}', encoding="utf-8")
            with (
                mock.patch.object(stream_watchdog, "ADSB_FRESHNESS_STATE_FILE", state_file),
                mock.patch.object(stream_watchdog, "ADSB_JSON_MAX_AGE_SEC", 30),
                mock.patch.object(stream_watchdog, "ADSB_MESSAGE_STALL_SEC", 120),
                mock.patch.object(stream_watchdog, "append_event") as append_event,
            ):
                ok, reason = stream_watchdog.check_adsb_freshness(
                    {"now": 1130, "messages": 12, "aircraft": []},
                    current_ts=1130,
                )
        self.assertTrue(ok)
        self.assertIn("counter reset", reason)
        append_event.assert_called_once()

    def test_now_playing_transition_age_ignores_heartbeat_updates(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            nowp = Path(td) / "now_playing.json"
            nowp.write_text(
                '{"updated_at_utc":"1970-01-01T00:16:30Z","note":"Heartbeat update while track is playing."}',
                encoding="utf-8",
            )
            with mock.patch.object(stream_watchdog, "NOW_PLAYING_JSON", nowp):
                self.assertIsNone(stream_watchdog.now_playing_transition_age_sec(current_ts=1000))

    def test_now_playing_transition_age_uses_non_heartbeat_update(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            nowp = Path(td) / "now_playing.json"
            nowp.write_text(
                '{"updated_at_utc":"1970-01-01T00:16:20Z","note":""}',
                encoding="utf-8",
            )
            with mock.patch.object(stream_watchdog, "NOW_PLAYING_JSON", nowp):
                self.assertEqual(stream_watchdog.now_playing_transition_age_sec(current_ts=1000), 20)

    def test_now_playing_transition_detail_marks_jst_bucket_boundary(self) -> None:
        current_ts = int(datetime(2026, 5, 8, 7, 0, 18, tzinfo=timezone.utc).timestamp())
        with tempfile.TemporaryDirectory() as td:
            nowp = Path(td) / "now_playing.json"
            nowp.write_text(
                '{"updated_at_utc":"2026-05-08T06:58:00Z","note":"Heartbeat update while track is playing.",'
                '"now_playing":{"title":"test","bucket":"day","prefix":"minor"}}',
                encoding="utf-8",
            )
            with (
                mock.patch.object(stream_watchdog, "NOW_PLAYING_JSON", nowp),
                mock.patch.object(stream_watchdog, "AUDIO_BUCKET_BOUNDARY_GRACE_SEC", 90),
            ):
                detail = stream_watchdog.now_playing_transition_detail(current_ts=current_ts)
        self.assertEqual(detail["bucket_boundary_nearest"], "evening")
        self.assertEqual(detail["bucket_boundary_delta_sec"], 18)
        self.assertTrue(detail["bucket_boundary_within_grace"])
        self.assertTrue(detail["now_playing_heartbeat"])
        self.assertIsNone(detail["track_transition_age_sec"])

    def test_video_frame_detail_uses_luma_threshold(self) -> None:
        with mock.patch.object(
            stream_watchdog,
            "run",
            return_value=mock.Mock(returncode=0, stdout="lavfi.signalstats.YAVG=12.5\n", stderr=""),
        ):
            ok, reason = stream_watchdog.check_video_frame_detail()
        self.assertTrue(ok)
        self.assertIn("luma ok", reason)

    def test_video_frame_detail_rejects_dark_frame(self) -> None:
        with mock.patch.object(
            stream_watchdog,
            "run",
            return_value=mock.Mock(returncode=0, stdout="lavfi.signalstats.YAVG=1.0\n", stderr=""),
        ):
            ok, reason = stream_watchdog.check_video_frame_detail()
        self.assertFalse(ok)
        self.assertIn("too dark", reason)

    def test_audio_low_first_observation_records_only(self) -> None:
        stage_state = {"pulse_stage": 0, "pulse_last_ts": 0, "audio_stage": 0, "audio_last_ts": 0}
        calls = self._invoke_audio_low(stage_state, fails=0)
        self.assertEqual(calls, [])

    def test_audio_low_second_observation_restarts_dj_only(self) -> None:
        stage_state = {"pulse_stage": 0, "pulse_last_ts": 0, "audio_stage": 1, "audio_last_ts": 1000}
        calls = self._invoke_audio_low(stage_state, fails=1, now_ts=1010)
        self.assertEqual([c[0] for c in calls], ["dj"])

    def test_audio_low_third_observation_restarts_stream_only(self) -> None:
        stage_state = {"pulse_stage": 0, "pulse_last_ts": 0, "audio_stage": 2, "audio_last_ts": 1000}
        calls = self._invoke_audio_low(stage_state, fails=2, now_ts=1010)
        self.assertEqual([c[0] for c in calls], ["stream"])

    def test_audio_low_during_track_transition_grace_does_not_restart(self) -> None:
        stage_state = {"pulse_stage": 0, "pulse_last_ts": 0, "audio_stage": 0, "audio_last_ts": 0}
        calls = self._invoke_audio_low(stage_state, fails=0, transition_age=12)
        self.assertEqual(calls, [])

    def test_audio_low_single_observation_during_now_playing_heartbeat_does_not_restart(self) -> None:
        stage_state = {"pulse_stage": 0, "pulse_last_ts": 0, "audio_stage": 0, "audio_last_ts": 0}
        calls = self._invoke_audio_low(stage_state, fails=0, now_playing_heartbeat=True)
        self.assertEqual(calls, [])

    def test_pulse_source_missing_first_stage_restarts_dj_only(self) -> None:
        stage_state = {"pulse_stage": 0, "pulse_last_ts": 0, "audio_stage": 0, "audio_last_ts": 0}
        calls = self._invoke_pulse_source_missing(stage_state, fails=0)
        self.assertEqual([c[0] for c in calls], ["dj"])

    def test_pulse_source_missing_second_stage_restarts_stream_only(self) -> None:
        stage_state = {"pulse_stage": 0, "pulse_last_ts": 0, "audio_stage": 1, "audio_last_ts": 1000}
        calls = self._invoke_pulse_source_missing(stage_state, fails=1, now_ts=1010)
        self.assertEqual([c[0] for c in calls], ["stream"])


if __name__ == "__main__":
    unittest.main()
