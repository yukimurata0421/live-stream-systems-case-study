from __future__ import annotations

import json
import importlib
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from stream_v2.aggregator import SubsystemAggregator
from stream_v2.config import RuntimeConfig
from stream_v2.source_reader import SourceReader

NOW = datetime(2026, 5, 6, 12, 0, 0, tzinfo=timezone.utc)
TS = "2026-05-06T11:59:30Z"


def write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, separators=(",", ":")) + "\n", encoding="utf-8")


def append_jsonl(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload, separators=(",", ":")) + "\n")


def healthy_source(root: Path) -> None:
    write_json(root / "youtube_watchdog_stats.json", {
        "ts_utc": TS,
        "status": "ok",
        "healthy": True,
        "video_id": "vid-current",
        "expected_video_id": "vid-current",
        "public_ok": True,
        "availability_ok": True,
        "api_live_state": "live",
        "oauth_life_cycle_status": "live",
        "oauth_checked_ts_utc": TS,
        "data_api_checked_ts_utc": TS,
        "stream_active": True,
        "ffmpeg_pid": 123,
        "ingest_connected": True,
        "local_ok": True,
        "oauth_broadcast_id": "vid-current",
        "oauth_bound_stream_id": "stream-1",
        "oauth_enable_auto_stop": False,
        "force_current_broadcast_live_allowed": True,
        "api_cost_burn_rate_active": False,
        "failure_kind": "none",
    })
    write_json(root / "youtube_video_id_resolver_state.json", {
        "ts_utc": TS,
        "video_id": "vid-current",
        "expected_video_id": "vid-current",
        "candidate_new_video_id": "",
    })
    write_json(root / "stream_watchdog_stats.json", {
        "ts_utc": TS,
        "status": "ok",
        "judgment": "ok",
        "ffmpeg_count": 1,
        "runtime_snapshot_age_sec": 10,
    })
    write_json(root / "stream_runtime_state_abc.json", {
        "run_id": "run-1",
        "updated_at_utc": TS,
        "status": "running",
        "ffmpeg_pid": "123",
        "last_health_ok": "true",
        "last_event_id": "evt-runtime-1",
    })
    append_jsonl(root / "logs" / "watchdog_state_timeline.jsonl", {
        "ts_utc": TS,
        "event_id": "evt-timeline-1",
        "stream_service_substate": "running",
        "dj_service_substate": "running",
        "ffmpeg_count": 1,
        "runtime_snapshot": {
            "age_sec": 10,
            "run_id": "run-1",
            "status": "running",
            "ffmpeg_pid": "123",
            "updated_at_utc": TS,
        },
        "now_playing_state": {
            "updated_at_utc": "2026-05-06T11:59:45Z",
            "status": "playing",
            "title": "NCS Track",
        },
    })
    append_jsonl(root / "logs" / "stream1090_report.jsonl", {
        "ts_utc": TS,
        "mode": "report_only",
        "target": "overlay_stream1090",
        "judgment": "report_only_ok",
        "checks": {
            "html_reachable": True,
            "html_has_map_markers": True,
            "outline_json_ok": True,
            "actual_range_points": 360,
            "aircraft_json_ok": True,
            "aircraft_count_first": 10,
            "aircraft_count_second": 12,
            "position_change_count": 3,
            "messages_delta": 120,
            "sample_sec": 5,
            "warnings": [],
        },
    })
    append_jsonl(root / "logs" / "upstream_stream1090_report.jsonl", {
        "ts_utc": TS,
        "mode": "report_only",
        "target": "upstream_stream1090",
        "judgment": "report_only_ok",
        "checks": {
            "html_reachable": True,
            "html_has_map_markers": True,
            "outline_json_ok": True,
            "actual_range_points": 360,
            "aircraft_json_ok": True,
            "aircraft_count_first": 8,
            "aircraft_count_second": 9,
            "position_change_count": 2,
            "messages_delta": 90,
            "sample_sec": 5,
            "warnings": [],
        },
    })
    append_jsonl(root / "logs" / "fast_recovery_events.jsonl", {
        "ts_utc": TS,
        "event_id": "evt-tcp-1",
        "kind": "tcp_send_sample",
        "bytes_sent_delta": 2900000,
        "mbps": 4.6,
        "notsent": 0,
        "unacked": 0,
        "lastsnd_ms": 100,
        "conn": "ESTAB",
    })
    append_jsonl(root / "logs" / "stream_engine_events.jsonl", {
        "ts_utc": TS,
        "event_id": "evt-engine-1",
        "event": "ffmpeg_started",
        "ffmpeg_pid": 123,
        "encoder_profile": {"video_bitrate": "3500k", "audio_bitrate": "192k"},
    })
    append_jsonl(root / "logs" / "play_history.jsonl", {
        "ts_utc": TS,
        "event": "track_selected",
        "title": "NCS Track",
        "bucket": "day",
    })
    write_json(root / "watchdog" / "pulse_health_state.json", {
        "ts_utc": TS,
        "status": "ok",
        "dj_missing_count": 0,
        "capture_missing_count": 0,
        "dj_latency_high_count": 0,
        "capture_latency_high_count": 0,
    })
    write_json(root / "watchdog" / "adsb_freshness_state.json", {
        "ts_utc": TS,
        "status": "ok",
        "messages_stalled": False,
        "aircraft_json_stale": False,
    })
    (root / "watchdog" / "audio_fail_count").write_text("0\n", encoding="utf-8")
    (root / "watchdog" / "pulse_source_missing_count").write_text("0\n", encoding="utf-8")
    write_json(root / "reports" / "youtube_api_cost" / "open_day_latest.json", {
        "ts_utc": TS,
        "status": "ok",
        "coverage_ok": True,
    })


class TestSubsystemAggregation(unittest.TestCase):
    def aggregate(self, root: Path):
        config = RuntimeConfig(source_state_root=root, state_root=root / "v2")
        inputs = SourceReader(root).read()
        return SubsystemAggregator(config).aggregate(inputs, now=NOW).to_dict()

    def test_subsystems_all_healthy_when_public_oauth_local_and_overlay_are_fresh(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            healthy_source(root)
            payload = self.aggregate(root)
        self.assertEqual(payload["overall"]["state"], "healthy")
        self.assertEqual(payload["overall"]["stream_public_state"], "same_url_live")
        self.assertEqual(payload["music"]["state"], "healthy")
        self.assertEqual(payload["youtube_lifecycle"]["replacement_allowed"], False)
        self.assertEqual(payload["youtube_lifecycle"]["replacement_policy"]["reason"], "expected_url_live_or_recoverable")

    def test_youtube_lifecycle_expected_url_live_blocks_replacement_broadcast(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            healthy_source(root)
            payload = self.aggregate(root)
        blocked = payload["youtube_lifecycle"]["blocked_actions"]
        self.assertEqual(blocked[0]["action"], "create_replacement_broadcast")
        self.assertIn("current_url_unrecoverable", payload["youtube_lifecycle"]["replacement_policy"]["required_missing"])

    def test_candidate_video_id_is_not_promoted_to_expected_video_id(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            healthy_source(root)
            stats = json.loads((root / "youtube_watchdog_stats.json").read_text())
            stats["candidate_new_video_id"] = "vid-candidate"
            stats["candidate_new_url_found"] = True
            write_json(root / "youtube_watchdog_stats.json", stats)
            payload = self.aggregate(root)
        self.assertEqual(payload["overall"]["expected_video_id"], "vid-current")
        self.assertEqual(payload["youtube_lifecycle"]["candidate_video_id"], "vid-candidate")
        self.assertIn("candidate_new_url_found_not_promoted", payload["youtube_lifecycle"]["evidence"])

    def test_music_audio_low_first_observation_does_not_restart_stream(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            healthy_source(root)
            # Remove now_playing freshness and leave the old restart reason as weak music evidence.
            (root / "logs" / "watchdog_state_timeline.jsonl").write_text("", encoding="utf-8")
            write_json(root / "restart_reason.json", {
                "ts_utc": TS,
                "source": "stream_watchdog",
                "event_id": "evt-audio-low-1",
                "component": "dj",
                "reason": "audio energy missing (first check)",
            })
            payload = self.aggregate(root)
        self.assertEqual(payload["music"]["state"], "degraded")
        self.assertNotEqual(payload["music"]["recommended_action"], "restart_stream")
        self.assertIn("audio_failure_never_authorizes_youtube_lifecycle_action", payload["music"]["blocked_by"])

    def test_local_delivery_ffmpeg_missing_recommends_ffmpeg_or_stream_restart_not_replacement(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            healthy_source(root)
            stats = json.loads((root / "youtube_watchdog_stats.json").read_text())
            stats["ffmpeg_pid"] = 0
            stats["ingest_connected"] = False
            stats["stream_active"] = False
            write_json(root / "youtube_watchdog_stats.json", stats)
            runtime = json.loads((root / "stream_runtime_state_abc.json").read_text())
            runtime["ffmpeg_pid"] = ""
            write_json(root / "stream_runtime_state_abc.json", runtime)
            payload = self.aggregate(root)
        self.assertEqual(payload["local_delivery"]["state"], "failed")
        self.assertEqual(payload["local_delivery"]["recommended_action"], "restart_ffmpeg")
        self.assertEqual(payload["youtube_lifecycle"]["replacement_allowed"], False)

    def test_youtube_lifecycle_public_live_with_stale_remote_ended_is_inconsistent_not_restart(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            healthy_source(root)
            stats = json.loads((root / "youtube_watchdog_stats.json").read_text())
            stats["api_live_state"] = "ended"
            stats["oauth_life_cycle_status"] = "complete"
            stats["public_ok"] = True
            write_json(root / "youtube_watchdog_stats.json", stats)
            payload = self.aggregate(root)
        self.assertEqual(payload["youtube_lifecycle"]["state"], "degraded")
        self.assertIn("inconsistent_remote", payload["youtube_lifecycle"]["evidence"])
        self.assertEqual(payload["youtube_lifecycle"]["recommended_action"], "resync_resolver")

    def test_each_subsystem_has_signals_policy_and_actions_modules(self) -> None:
        for subsystem in ["rendering", "music", "local_delivery", "youtube_lifecycle", "monitoring"]:
            for module in ["signals", "policy", "actions"]:
                imported = importlib.import_module(f"stream_v2.subsystems.{subsystem}.{module}")
                self.assertIsNotNone(imported, f"{subsystem}.{module}")

    def test_rendering_overlay_failure_stays_inside_rendering_scope(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            healthy_source(root)
            stats = json.loads((root / "stream_watchdog_stats.json").read_text())
            stats["status"] = "fail"
            stats["judgment"] = "restart"
            stats["reason"] = "overlay unhealthy: overlay index unavailable"
            write_json(root / "stream_watchdog_stats.json", stats)
            payload = self.aggregate(root)
        self.assertEqual(payload["rendering"]["state"], "failed")
        self.assertEqual(payload["rendering"]["recommended_action"], "reload_overlay")
        self.assertIn("rendering_failure_never_authorizes_youtube_lifecycle_action", payload["rendering"]["blocked_by"])
        self.assertEqual(payload["youtube_lifecycle"]["replacement_allowed"], False)

    def test_rendering_stream1090_report_detects_aircraft_stall_even_when_http_ok(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            healthy_source(root)
            report_path = root / "logs" / "stream1090_report.jsonl"
            report_path.write_text("", encoding="utf-8")
            append_jsonl(report_path, {
                "ts_utc": TS,
                "mode": "report_only",
                "target": "overlay_stream1090",
                "judgment": "report_only_ok",
                "checks": {
                    "html_reachable": True,
                    "aircraft_json_ok": True,
                    "aircraft_count_first": 12,
                    "aircraft_count_second": 12,
                    "position_change_count": 0,
                    "messages_delta": 0,
                    "warnings": [],
                },
            })
            payload = self.aggregate(root)
        self.assertEqual(payload["rendering"]["state"], "failed")
        self.assertIn("adsb_freshness_stall", payload["rendering"]["evidence"])
        self.assertEqual(payload["rendering"]["recommended_action"], "reload_overlay")

    def test_rendering_reports_messages_delta_as_fresh_when_positions_are_static(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            healthy_source(root)
            for filename, target, aircraft_count, messages_delta in [
                ("stream1090_report.jsonl", "overlay_stream1090", 13, 15),
                ("upstream_stream1090_report.jsonl", "upstream_stream1090", 1, 2),
            ]:
                report_path = root / "logs" / filename
                report_path.write_text("", encoding="utf-8")
                append_jsonl(report_path, {
                    "ts_utc": TS,
                    "mode": "report_only",
                    "target": target,
                    "judgment": "report_only_ok",
                    "checks": {
                        "html_reachable": True,
                        "aircraft_json_ok": True,
                        "aircraft_count_first": aircraft_count,
                        "aircraft_count_second": aircraft_count,
                        "position_change_count": 0,
                        "messages_delta": messages_delta,
                        "warnings": [],
                    },
                })
            payload = self.aggregate(root)
        self.assertEqual(payload["rendering"]["state"], "healthy")
        self.assertEqual(payload["rendering"]["recommended_action"], "none")
        self.assertEqual(payload["rendering"]["stream1090_report_ok"], True)
        self.assertEqual(payload["rendering"]["upstream_stream1090_report_ok"], True)
        self.assertEqual(payload["rendering"]["aircraft_messages_moving"], True)
        self.assertEqual(payload["rendering"]["aircraft_positions_moving"], False)
        self.assertNotIn("adsb_freshness_stall", payload["rendering"]["evidence"])
        self.assertNotIn("upstream_stream1090_unavailable", payload["rendering"]["evidence"])

    def test_rendering_overlay_bad_upstream_ok_marks_overlay_scope(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            healthy_source(root)
            report_path = root / "logs" / "stream1090_report.jsonl"
            report_path.write_text("", encoding="utf-8")
            append_jsonl(report_path, {
                "ts_utc": TS,
                "mode": "report_only",
                "target": "overlay_stream1090",
                "judgment": "report_only_ok",
                "checks": {
                    "html_reachable": False,
                    "aircraft_json_ok": True,
                    "aircraft_count_first": 8,
                    "aircraft_count_second": 9,
                    "position_change_count": 2,
                    "messages_delta": 50,
                    "warnings": [],
                },
            })
            payload = self.aggregate(root)
        self.assertEqual(payload["rendering"]["state"], "failed")
        self.assertIn("overlay_unavailable", payload["rendering"]["evidence"])
        self.assertEqual(payload["rendering"]["upstream_stream1090_report_ok"], True)

    def test_music_pulse_source_missing_affects_local_delivery_but_not_youtube_lifecycle(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            healthy_source(root)
            (root / "logs" / "watchdog_state_timeline.jsonl").write_text("", encoding="utf-8")
            write_json(root / "restart_reason.json", {
                "ts_utc": TS,
                "source": "stream_watchdog",
                "event_id": "evt-pulse-1",
                "component": "stream",
                "reason": "pulse_source_missing: stream_capture_source_output_missing",
            })
            payload = self.aggregate(root)
        self.assertEqual(payload["music"]["state"], "failed")
        self.assertEqual(payload["music"]["recommended_action"], "repair_pulse")
        self.assertIn("local_delivery", payload["music"]["affects_subsystems"])
        self.assertIn("audio_failure_never_authorizes_youtube_lifecycle_action", payload["music"]["blocked_by"])

    def test_music_audio_low_second_observation_recommends_dj_restart(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            healthy_source(root)
            write_json(root / "restart_reason.json", {
                "ts_utc": TS,
                "source": "stream_watchdog",
                "event_id": "evt-audio-low-2",
                "component": "dj",
                "reason": "audio_energy_low confirmed",
                "consecutive_fail_count": 2,
            })
            payload = self.aggregate(root)
        self.assertEqual(payload["music"]["state"], "degraded")
        self.assertEqual(payload["music"]["recommended_action"], "restart_dj")
        self.assertEqual(payload["music"]["audio_fail_count"], 2)

    def test_music_transition_grace_defers_audio_low(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            healthy_source(root)
            write_json(root / "restart_reason.json", {
                "ts_utc": TS,
                "source": "stream_watchdog",
                "event_id": "evt-audio-grace-1",
                "component": "dj",
                "reason": "audio energy low during track transition grace",
                "consecutive_fail_count": 1,
                "track_transition_within_grace": True,
                "track_transition_age_sec": 12,
            })
            payload = self.aggregate(root)
        self.assertEqual(payload["music"]["state"], "recovering")
        self.assertEqual(payload["music"]["recommended_action"], "defer")
        self.assertIn("audio_energy_low_transition_grace", payload["music"]["evidence"])

    def test_local_delivery_tcp_stall_is_recovering_and_same_url_preserving(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            healthy_source(root)
            append_jsonl(root / "logs" / "fast_recovery_events.jsonl", {
                "ts_utc": TS,
                "kind": "restart",
                "trigger": "tcp_stall",
                "message": "tcp stall: bytes_delta=0",
                "ffmpeg_pid": 123,
            })
            payload = self.aggregate(root)
        self.assertEqual(payload["local_delivery"]["state"], "recovering")
        self.assertEqual(payload["local_delivery"]["recommended_action"], "restart_ffmpeg")
        self.assertIn("local_delivery_failure_never_authorizes_replacement_broadcast", payload["local_delivery"]["blocked_by"])
        self.assertEqual(payload["youtube_lifecycle"]["replacement_allowed"], False)

    def test_local_delivery_tcp_send_sample_healthy_records_tcp_metrics(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            healthy_source(root)
            payload = self.aggregate(root)
        self.assertEqual(payload["local_delivery"]["state"], "healthy")
        self.assertIn("tcp_send_healthy", payload["local_delivery"]["evidence"])
        self.assertEqual(payload["local_delivery"]["tcp_mbps"], 4.6)

    def test_local_delivery_duplicate_ffmpeg_is_failed_same_url_only(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            healthy_source(root)
            stats = json.loads((root / "stream_watchdog_stats.json").read_text())
            stats["ffmpeg_count"] = 2
            write_json(root / "stream_watchdog_stats.json", stats)
            payload = self.aggregate(root)
        self.assertEqual(payload["local_delivery"]["state"], "failed")
        self.assertIn("stream_ffmpeg_duplicate", payload["local_delivery"]["evidence"])
        self.assertEqual(payload["local_delivery"]["recommended_action"], "restart_stream")
        self.assertEqual(payload["youtube_lifecycle"]["replacement_allowed"], False)

    def test_youtube_lifecycle_records_identity_and_freshness(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            healthy_source(root)
            payload = self.aggregate(root)
        self.assertIn("expected_video_id_match", payload["youtube_lifecycle"]["evidence"])
        self.assertEqual(payload["youtube_lifecycle"]["expected_identity_match"], True)
        self.assertEqual(payload["youtube_lifecycle"]["public_probe_age_sec"], 30.0)
        self.assertEqual(payload["youtube_lifecycle"]["data_api_age_sec"], 30.0)

    def test_monitoring_tracks_stream1090_and_engine_freshness(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            healthy_source(root)
            payload = self.aggregate(root)
        self.assertIn("stream1090_report_fresh", payload["monitoring"]["fresh_sources"])
        self.assertIn("upstream_stream1090_report_fresh", payload["monitoring"]["fresh_sources"])
        self.assertIn("stream_engine_event_fresh", payload["monitoring"]["fresh_sources"])

    def test_monitoring_stale_sources_are_unknown_not_failed(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            old = "2026-05-06T10:00:00Z"
            write_json(root / "youtube_watchdog_stats.json", {"ts_utc": old, "status": "ok"})
            write_json(root / "youtube_video_id_resolver_state.json", {"ts_utc": old})
            write_json(root / "stream_watchdog_stats.json", {"ts_utc": old, "status": "ok", "judgment": "ok"})
            write_json(root / "reports" / "youtube_api_cost" / "open_day_latest.json", {"ts_utc": old, "status": "ok"})
            payload = self.aggregate(root)
        self.assertEqual(payload["monitoring"]["state"], "unknown")
        self.assertIn("monitoring_unknown_never_authorizes_destructive_action", payload["monitoring"]["blocked_by"])
        self.assertIn("watchdog_stats_fresh", payload["monitoring"]["stale_sources"])

    def test_consistency_window_exceeding_threshold_makes_overall_unknown(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            healthy_source(root)
            stats = json.loads((root / "youtube_watchdog_stats.json").read_text())
            stats["ts_utc"] = "2026-05-06T11:00:00Z"
            write_json(root / "youtube_watchdog_stats.json", stats)
            config = RuntimeConfig(source_state_root=root, state_root=root / "v2", max_consistency_window_sec=10)
            payload = SubsystemAggregator(config).aggregate(SourceReader(root).read(), now=NOW).to_dict()
        self.assertEqual(payload["overall"]["state"], "unknown")
        self.assertGreater(payload["overall"]["consistency_window_sec"], 10)

    def test_subsystems_status_is_written_by_aggregator_only_reader_has_no_write_api(self) -> None:
        self.assertFalse(hasattr(SourceReader, "write"))
        self.assertFalse(hasattr(SourceReader, "append"))


if __name__ == "__main__":
    unittest.main()
