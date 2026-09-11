from __future__ import annotations

import json
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from stream_contracts.monitoring_v4.time import utc_text
from stream_monitoring_v4.adapters.factory import SHADOW_DOMAINS, state_file_adapters
from stream_monitoring_v4.adapters.monitoring import CHECK_ALLOWLIST
from stream_monitoring_v4.adapters.json_file import SnapshotReadError, read_json_snapshot
from stream_monitoring_v4.adapters.youtube import YouTubeStateAdapter
from stream_monitoring_v4.current.service import CurrentReducerService
from stream_monitoring_v4.domains.source_policy import DEFAULT_POLICIES
from stream_monitoring_v4.reliability.projector import FORMAL_OBJECTIVES, ReliabilityProjector
from stream_monitoring_v4.runtime.observer import Observer
from stream_monitoring_v4.runtime.safe_input import (
    LIFECYCLE_MAX_LINES,
    SAFE_GENERATION_MANIFEST,
    SAFE_JSON_FILES,
    SAFE_OUTBOX_FILE,
    SAFE_LIFECYCLE_FILE,
    SAFE_ROLLOUT_FILE,
    ProjectionResult,
    project_safe_inputs,
    validate_safe_input_generation,
)
from stream_monitoring_v4.runtime.projection_lock import projection_lock
from stream_monitoring_v4.runtime.safe_inputs.stable_io import stable_text_lines
from stream_monitoring_v4.runtime.safe_inputs.outbox import notification_outbox_projection
from stream_monitoring_v4.runtime.safe_inputs.generation import projected_file_names
from stream_monitoring_v4.storage.repository import MonitoringRepository

from tests.helpers import BASE_TS, write_json


class ReceiptClockTests(unittest.TestCase):
    def test_live_receipt_is_captured_after_read_but_fixed_replay_stays_strict(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            source_at = utc_text(BASE_TS + 1)
            write_json(
                root / "youtube_watchdog_stats.json",
                {
                    "stats_file_updated_at_utc": source_at,
                    "remote_probe_ts_utc": source_at,
                    "oauth_checked_ts_utc": source_at,
                    "api_live_state": "live",
                    "oauth_life_cycle_status": "live",
                    "oauth_stream_status": "active",
                    "oauth_probe_ok": True,
                    "oauth_stream_health_status": "good",
                    "stream_active": True,
                    "ingest_connected": True,
                    "local_ok": True,
                },
            )
            write_json(
                root / "youtube_video_id_resolver_state.json",
                {"ts_utc": source_at, "video_id": "safe", "expected_video_id": "safe"},
            )
            adapter = YouTubeStateAdapter(
                watchdog_stats_file=root / "youtube_watchdog_stats.json",
                resolver_state_file=root / "youtube_video_id_resolver_state.json",
            )
            fixed = adapter.collect(received_at=utc_text(BASE_TS))
            self.assertTrue(fixed.rejections)
            self.assertEqual({item.reason_code for item in fixed.rejections}, {"source_timestamp_future"})
            with patch("stream_monitoring_v4.adapters.json_file.time.time", return_value=BASE_TS + 1):
                live = adapter.collect()
            self.assertEqual(live.rejections, ())
            self.assertEqual({item.received_at for item in live.observations}, {source_at})


class SafeInputProjectionTests(unittest.TestCase):
    def test_ready_requires_exact_member_partition_and_generation(self) -> None:
        members = projected_file_names()
        complete = ProjectionResult(
            projected=members,
            unchanged=(),
            rejected={},
            source_revision="stream-v3-test",
            generation_id="sig_complete",
        )

        self.assertTrue(complete.partition_valid)
        self.assertTrue(complete.ready)
        self.assertFalse(replace(complete, projected=members[:-1]).ready)
        self.assertFalse(
            replace(
                complete,
                unchanged=(members[0],),
            ).ready
        )
        self.assertFalse(replace(complete, generation_id="").ready)
        self.assertFalse(replace(complete, source_revision="unsafe/revision").ready)

    def test_maximum_valid_source_revision_round_trips_through_generation(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            raw = root / "raw"
            safe = root / "safe"
            raw.mkdir()
            self._write_raw(raw, utc_text(BASE_TS))
            expected = f"{'r' * 160}@{'a' * 64}"
            (raw / "deployed-revision.env").write_text(
                "STREAM_V3_DEPLOYED_REVISION=" + "r" * 160 + "\n"
                "STREAM_V3_DEPLOYED_COMMIT=" + "a" * 64 + "\n",
                encoding="utf-8",
            )

            result = project_safe_inputs(raw, safe)
            generation = validate_safe_input_generation(safe)

        self.assertTrue(result.ready, result.to_dict())
        self.assertEqual(result.source_revision, expected)
        self.assertEqual(generation.source_revision, expected)

    def test_notification_outbox_projection_is_content_free_and_marks_malformed_rows(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "outbox.jsonl"
            path.write_text(
                "\n".join(
                    (
                        json.dumps(
                            {
                                "message_id": "valid",
                                "route": "discord",
                                "status": "pending",
                                "attempts": 2,
                                "created_ts": BASE_TS - 30,
                                "updated_ts": BASE_TS - 10,
                                "content": "TOP_SECRET_MESSAGE",
                                "webhook_url": "TOP_SECRET_WEBHOOK",
                            }
                        ),
                        "{broken-json",
                        json.dumps(
                            {
                                "message_id": "bad-route",
                                "route": "private-route",
                                "status": "pending",
                                "attempts": True,
                                "created_ts": 0,
                                "content": "TOP_SECRET_SECOND",
                            }
                        ),
                        json.dumps({"status": "sent", "content": "TOP_SECRET_THIRD"}),
                    )
                )
                + "\n",
                encoding="utf-8",
            )

            projected = notification_outbox_projection(path, projected_at_ts=BASE_TS)

        self.assertEqual(projected["pending_count"], 2)
        self.assertEqual(projected["invalid_row_count"], 3)
        self.assertEqual(projected["max_attempts"], 2)
        self.assertEqual(
            projected["route_counts"], {"discord": 1, "unrecognized": 1}
        )
        self.assertFalse(projected["content_projected"])
        self.assertFalse(projected["credential_fields_projected"])
        self.assertNotIn("TOP_SECRET", repr(projected))

    def test_absent_notification_outbox_remains_explicitly_unavailable(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            projected = notification_outbox_projection(
                Path(td) / "missing.jsonl",
                projected_at_ts=BASE_TS,
            )

        self.assertFalse(projected["source_present"])
        self.assertEqual(projected["pending_count"], 0)
        self.assertEqual(projected["invalid_row_count"], 0)

    def test_nonfinite_json_number_is_rejected_at_the_source_boundary(self) -> None:
        for raw in (
            '{"sli_pct":NaN}\n',
            '{"sli_pct":1e999}\n',
            '{"counter":9223372036854775808}\n',
        ):
            with self.subTest(raw=raw), tempfile.TemporaryDirectory() as td:
                source = Path(td) / "invalid.json"
                source.write_text(raw, encoding="utf-8")
                with self.assertRaises(SnapshotReadError) as context:
                    read_json_snapshot(source, received_at=utc_text(BASE_TS))
                self.assertEqual(context.exception.reason_code, "source_json_invalid")

    def test_snapshot_reader_rejects_symlink_nonregular_and_bounded_overflow(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            victim = root / "victim.json"
            victim.write_text("{}\n", encoding="utf-8")
            link = root / "link.json"
            link.symlink_to(victim)
            with self.assertRaises(SnapshotReadError) as symlink_error:
                read_json_snapshot(link)
            self.assertEqual(symlink_error.exception.reason_code, "source_symlink_rejected")

            directory = root / "directory.json"
            directory.mkdir()
            with self.assertRaises(SnapshotReadError) as regular_error:
                read_json_snapshot(directory)
            self.assertEqual(regular_error.exception.reason_code, "source_not_regular")

            oversized = root / "oversized.json"
            oversized.write_bytes(b"{" + b"x" * 32 + b"}")
            with self.assertRaises(SnapshotReadError) as size_error:
                read_json_snapshot(oversized, max_bytes=16)
            self.assertEqual(size_error.exception.reason_code, "source_too_large")

    def test_stable_jsonl_read_rejects_path_replacement_on_another_device(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            source = Path(td) / "events.jsonl"
            source.write_text('{"event":"ok"}\n', encoding="utf-8")
            before = source.stat()
            replaced = SimpleNamespace(
                st_dev=before.st_dev + 1,
                st_ino=before.st_ino,
                st_size=before.st_size,
                st_mtime_ns=before.st_mtime_ns,
                st_mode=before.st_mode,
            )
            with patch.object(Path, "lstat", return_value=replaced):
                with self.assertRaises(SnapshotReadError) as context:
                    list(stable_text_lines(source, required=True))
            self.assertEqual(
                context.exception.reason_code,
                "source_changed_during_read",
            )

    def test_sanitized_target_symlink_is_never_followed(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            raw = root / "raw"
            safe = root / "safe"
            raw.mkdir()
            at = utc_text(BASE_TS)
            self._write_raw(raw, at)
            (raw / "deployed-revision.env").write_text(
                "STREAM_V3_DEPLOYED_REVISION=stream-v3-test\n",
                encoding="utf-8",
            )
            self.assertTrue(project_safe_inputs(raw, safe).ready)
            victim = root / "must-not-change.txt"
            victim.write_text("preserved\n", encoding="utf-8")
            target = safe / "map_runtime_status.json"
            target.unlink()
            target.symlink_to(victim)

            result = project_safe_inputs(raw, safe)
            self.assertFalse(result.ready)
            self.assertEqual(result.rejected["map_runtime_status.json"], "OSError")
            self.assertEqual(victim.read_text(encoding="utf-8"), "preserved\n")
            self.assertTrue(target.is_symlink())

    def test_unchanged_sanitized_file_still_repairs_overbroad_mode(self) -> None:
        from stream_monitoring_v4.runtime.atomic_file import atomic_write_text

        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "sanitized.json"
            path.write_text("{}\n", encoding="utf-8")
            path.chmod(0o666)
            changed = atomic_write_text(
                path,
                "{}\n",
                mode=0o600,
                skip_if_unchanged=True,
            )
            self.assertTrue(changed)
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)

    def test_large_rotated_lifecycle_log_uses_bounded_tail(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            raw = root / "raw"
            safe = root / "safe"
            raw.mkdir()
            at = utc_text(BASE_TS)
            self._write_raw(raw, at)
            (raw / "deployed-revision.env").write_text(
                "STREAM_V3_DEPLOYED_REVISION=stream-v3-test\n",
                encoding="utf-8",
            )
            lifecycle = raw / "logs" / "stream_engine_events.jsonl"
            noise = '{"event_type":"noise","padding":"' + "x" * 900 + '"}\n'
            scheduled = (
                '{"ts_utc":"%s","event_id":"evt-large-scheduled","event_type":"ffmpeg_restart_scheduled",'
                '"run_id":"run-large","restart_count":2,"exit_code":1,"delay_sec":5}\n'
                % utc_text(BASE_TS - 5)
            )
            started = (
                '{"ts_utc":"%s","event_id":"evt-large-started","event_type":"ffmpeg_started",'
                '"run_id":"run-large","restart_count":2}\n'
                % at
            )
            lifecycle.write_text(noise * 6000 + scheduled + started, encoding="utf-8")
            self.assertGreater(lifecycle.stat().st_size, 4 * 1024 * 1024)

            result = project_safe_inputs(raw, safe)
            self.assertTrue(result.ready, result.to_dict())
            self.assertEqual(
                result.to_dict()["schema"],
                "monitoring_v4.safe_input_projection.v4",
            )
            projected = (safe / SAFE_LIFECYCLE_FILE).read_text(encoding="utf-8")
            self.assertIn("evt-large-scheduled", projected)
            self.assertIn("evt-large-started", projected)

    def test_lifecycle_projection_rejects_type_coercion_and_excessive_line_count(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            raw = root / "raw"
            safe = root / "safe"
            raw.mkdir()
            at = utc_text(BASE_TS)
            self._write_raw(raw, at)
            (raw / "deployed-revision.env").write_text(
                "STREAM_V3_DEPLOYED_REVISION=stream-v3-test\n",
                encoding="utf-8",
            )
            lifecycle = raw / "logs" / "stream_engine_events.jsonl"
            lifecycle.write_text(
                lifecycle.read_text(encoding="utf-8").replace(
                    '"restart_count":1',
                    '"restart_count":true',
                    1,
                ),
                encoding="utf-8",
            )
            coerced = project_safe_inputs(raw, safe)
            self.assertEqual(
                coerced.rejected[SAFE_LIFECYCLE_FILE],
                "source_value_invalid",
            )

            self._write_raw(raw, at)
            lifecycle.write_text(
                lifecycle.read_text(encoding="utf-8").replace(
                    '"event_id":"evt-scheduled-1"',
                    '"event_id":1',
                    1,
                ),
                encoding="utf-8",
            )
            numeric_id = project_safe_inputs(raw, safe)
            self.assertEqual(
                numeric_id.rejected[SAFE_LIFECYCLE_FILE],
                "source_value_invalid",
            )

            lifecycle.write_text("{}\n" * (LIFECYCLE_MAX_LINES + 1), encoding="utf-8")
            excessive = project_safe_inputs(raw, safe)
            self.assertEqual(
                excessive.rejected[SAFE_LIFECYCLE_FILE],
                "source_line_count_exceeded",
            )

    def test_projection_is_fixed_allowlist_secret_free_and_adapter_compatible(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            raw = root / "raw"
            safe = root / "safe"
            raw.mkdir()
            at = utc_text(BASE_TS)
            self._write_raw(raw, at)
            (raw / "youtube_oauth_token_state.json").write_text(
                '{"access_token":"TOP_SECRET_TOKEN"}\n', encoding="utf-8"
            )
            (raw / "recovery-token-secret.yaml").write_text(
                "token: TOP_SECRET_RECOVERY\n", encoding="utf-8"
            )
            (raw / "deployed-revision.env").write_text(
                "STREAM_V3_DEPLOYED_REVISION=stream-v3-test\n"
                "STREAM_V3_DEPLOYED_COMMIT=0123456789abcdef0123456789abcdef01234567\n"
                "STREAM_KEY=TOP_SECRET_ENV\n",
                encoding="utf-8",
            )

            result = project_safe_inputs(raw, safe)
            self.assertTrue(result.ready, result.to_dict())
            self.assertFalse(result.to_dict()["credential_fields_projected"])
            self.assertNotIn("credential_values_projected", result.to_dict())
            self.assertEqual(
                {path.name for path in safe.iterdir()},
                set(SAFE_JSON_FILES)
                | {
                    SAFE_OUTBOX_FILE,
                    SAFE_GENERATION_MANIFEST,
                    SAFE_LIFECYCLE_FILE,
                    SAFE_ROLLOUT_FILE,
                    "deployed-revision.env",
                    ".projection.lock",
                },
            )
            rendered = "\n".join(
                path.read_text(encoding="utf-8") for path in sorted(safe.iterdir())
            )
            self.assertNotIn("TOP_SECRET", rendered)
            self.assertNotIn("private-endpoint", rendered)
            self.assertNotIn("capture-file", rendered)
            self.assertNotIn("stream_key", rendered)
            self.assertIn("ffmpeg_auto_recovered", rendered)
            safe_map = json.loads((safe / "map_runtime_status.json").read_text(encoding="utf-8"))
            self.assertEqual(safe_map["schema"], "stream_v3.map_runtime_monitor.v2")
            self.assertEqual(
                safe_map["conditions"],
                {
                    "deployment_ready": True,
                    "precipitation_data_ok": True,
                    "precipitation_generation_integrity": True,
                    "precipitation_render_applied": True,
                    "precipitation_validtime_match": True,
                },
            )
            safe_resource = json.loads(
                (safe / "resource_memory.json").read_text(encoding="utf-8")
            )
            self.assertEqual(safe_resource["assessment"]["baseline_coverage_sec"], 700000)
            self.assertEqual(safe_resource["current_pressure"]["oom_event_delta"], 0)
            self.assertTrue(safe_resource["swap_capacity"]["warn"])
            self.assertEqual(safe_resource["host_memory"]["swap_used_mb"], 3072.0)
            safe_memory_status = json.loads(
                (safe / "memory_status.json").read_text(encoding="utf-8")
            )
            self.assertEqual(safe_memory_status["swap_capacity"]["severity"], "warn")
            self.assertEqual(safe_memory_status["host"]["mem_available_bytes"], 8 * 1024**3)

            generation = validate_safe_input_generation(safe)
            self.assertEqual(generation.generation_id, result.generation_id)
            self.assertEqual(generation.source_revision, result.source_revision)

            repository = MonitoringRepository(root / "monitoring.sqlite3")
            repository.initialize(applied_at=at)
            observer = Observer(repository).run_once(state_file_adapters(safe), now_ts=BASE_TS)
            self.assertEqual(observer.inserted_rejections, 0)
            currents = CurrentReducerService(repository, DEFAULT_POLICIES).reduce(
                SHADOW_DOMAINS, now_ts=BASE_TS
            )
            self.assertEqual({item.state for item in currents}, {"good"})
            projection = ReliabilityProjector(
                repository,
                formal_status_file=safe / "operational_reliability_status.json",
                burn_status_file=safe / "operational_reliability_burn_status.json",
            ).run_once(received_at=at)
            self.assertEqual(len(projection.projections), len(FORMAL_OBJECTIVES) + 1)
            self.assertEqual(projection.rejections, ())

    def test_projection_lock_prevents_mixed_generation_reads(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            lock = Path(td) / ".projection.lock"
            with projection_lock(lock, exclusive=True, timeout_sec=1):
                with self.assertRaisesRegex(TimeoutError, "projection lock"):
                    with projection_lock(lock, exclusive=False, timeout_sec=0.01):
                        self.fail("shared reader entered an active projection")
            with projection_lock(lock, exclusive=False, timeout_sec=0.1):
                pass
            self.assertEqual(lock.stat().st_mode & 0o777, 0o600)

    def test_rejected_source_preserves_the_complete_previous_generation(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            raw = root / "raw"
            safe = root / "safe"
            raw.mkdir()
            at = utc_text(BASE_TS)
            self._write_raw(raw, at)
            (raw / "deployed-revision.env").write_text(
                "STREAM_V3_DEPLOYED_REVISION=stream-v3-test\n",
                encoding="utf-8",
            )
            first = project_safe_inputs(raw, safe)
            self.assertTrue(first.ready)
            before = {
                path.name: path.read_bytes()
                for path in safe.iterdir()
                if path.name != ".projection.lock"
            }

            write_json(
                raw / "map_runtime_status.json",
                {
                    "checked_at_utc": utc_text(BASE_TS + 1),
                    "schema": "stream_v3.map_runtime_monitor.v1",
                    "status": "degraded",
                    "delivery_critical_ok": False,
                    "weather_ok": True,
                    "conditions": {"deployment_ready": False},
                },
            )
            (raw / "youtube_video_id_resolver_state.json").write_text(
                "{broken-json\n",
                encoding="utf-8",
            )
            rejected = project_safe_inputs(raw, safe)

            self.assertFalse(rejected.ready)
            self.assertIn("youtube_video_id_resolver_state.json", rejected.rejected)
            after = {
                path.name: path.read_bytes()
                for path in safe.iterdir()
                if path.name != ".projection.lock"
            }
            self.assertEqual(after, before)
            self.assertEqual(
                validate_safe_input_generation(safe).generation_id,
                first.generation_id,
            )

    def test_generation_manifest_detects_a_crash_partial_write(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            raw = root / "raw"
            safe = root / "safe"
            raw.mkdir()
            at = utc_text(BASE_TS)
            self._write_raw(raw, at)
            (raw / "deployed-revision.env").write_text(
                "STREAM_V3_DEPLOYED_REVISION=stream-v3-test\n",
                encoding="utf-8",
            )
            self.assertTrue(project_safe_inputs(raw, safe).ready)
            validate_safe_input_generation(safe)

            changed = safe / "map_runtime_status.json"
            changed.write_text("{}\n", encoding="utf-8")
            changed.chmod(0o600)
            with self.assertRaisesRegex(RuntimeError, "digest mismatch"):
                validate_safe_input_generation(safe)

    @staticmethod
    def _write_raw(root: Path, at: str) -> None:
        event_root = root / "logs"
        event_root.mkdir(parents=True, exist_ok=True)
        scheduled_at = utc_text(BASE_TS - 5)
        (event_root / "stream_engine_events.jsonl").write_text(
            "\n".join(
                (
                    '{"ts_utc":"%s","event_id":"evt-scheduled-1","event_type":"ffmpeg_restart_scheduled","run_id":"run-1","restart_count":1,"exit_code":1,"delay_sec":5,"stream_key_hash":"TOP_SECRET_HASH","rtmp_url_masked":"private-endpoint"}'
                    % scheduled_at,
                    '{"ts_utc":"%s","event_id":"evt-started-1","event_type":"ffmpeg_started","run_id":"run-1","restart_count":1,"ffmpeg_pid":123,"stream_key_hash":"TOP_SECRET_HASH"}'
                    % at,
                )
            )
            + "\n",
            encoding="utf-8",
        )
        write_json(
            root / "watchdog" / "k8s_container_restart_counts.json",
            {
                "updated_at_utc": at,
                "pods": [
                    {
                        "phase": "Running",
                        "uid": "pod-uid-1",
                        "started_at_utc": at,
                        "planned_rollout_annotations": {
                            "stream-v3.yukimurata.dev/planned-rollout-id": "rollout-1",
                            "stream-v3.yukimurata.dev/planned-rollout-at": utc_text(BASE_TS - 60),
                            "stream-v3.yukimurata.dev/planned-rollout-expires-at": utc_text(
                                BASE_TS + 840
                            ),
                            "stream-v3.yukimurata.dev/planned-rollout-reason": "test_rollout",
                        },
                    }
                ],
            },
        )
        write_json(
            root / "youtube_watchdog_stats.json",
            {
                "stats_file_updated_at_utc": at,
                "remote_probe_ts_utc": at,
                "oauth_checked_ts_utc": at,
                "remote_sample_id": "sample-1",
                "api_live_state": "live",
                "oauth_life_cycle_status": "live",
                "oauth_stream_status": "active",
                "oauth_probe_ok": True,
                "oauth_stream_health_status": "good",
                "stream_active": True,
                "ingest_connected": True,
                "local_ok": True,
                "access_token": "TOP_SECRET_TOKEN",
                "oauth_stream_health_issue_details": [],
            },
        )
        write_json(
            root / "youtube_video_id_resolver_state.json",
            {"ts_utc": at, "video_id": "video-1", "expected_video_id": "video-1"},
        )
        write_json(
            root / "map_runtime_status.json",
            {
                "checked_at_utc": at,
                "schema": "stream_v3.map_runtime_monitor.v2",
                "status": "healthy",
                "delivery_critical_ok": True,
                "weather_ok": True,
                "conditions": {
                    "deployment_ready": True,
                    "precipitation_data_ok": True,
                    "precipitation_generation_integrity": True,
                    "precipitation_render_applied": True,
                    "precipitation_validtime_match": True,
                },
                "probe_errors": ["private-endpoint"],
            },
        )
        write_json(
            root / "subsystems_status.json",
            {
                "ts_utc": at,
                "youtube_lifecycle": {"state": "healthy", "token": "TOP_SECRET"},
                "local_delivery": {"state": "healthy"},
                "music": {"state": "healthy", "now_playing_title": "TOP_SECRET_TITLE"},
            },
        )
        write_json(
            root / "viewer_synthetic_status.json",
            {
                "checked_at_utc": at,
                "status": "healthy",
                "frame_ok": True,
                "black_detected": False,
                "freeze_detected": False,
                "capture_file": "capture-file-private",
            },
        )
        write_json(
            root / "external_blackbox_status.json",
            {
                "checked_at_utc": at,
                "evidence_at_utc": at,
                "status": "ok",
                "reason": "all_external_targets_passed",
                "targets": {
                    "public_status": {"status": "ok", "host": "private-endpoint"},
                    "youtube_public_video": {"status": "ok"},
                },
            },
        )
        write_json(
            root / "monitoring_watchdog_state.json",
            {
                "updated_at_utc": at,
                "checks": {
                    name: {"ok": True, "endpoint": "private-endpoint"}
                    for name in CHECK_ALLOWLIST
                },
            },
        )
        feedback = {
            "measurement_status": "valid",
            "measurement_unknown_reasons": [],
            "target_met_on_observed_samples": True,
            "sli_pct": 100.0,
            "coverage_pct": 100.0,
            "source_freshness_pct": 100.0,
            "prometheus_current": {
                "available": True,
                "eligible": True,
                "good": True,
                "ts_utc": at,
                "query": "TOP_SECRET_QUERY",
            },
            "raw_current": {
                "available": True,
                "eligible": True,
                "classification": "good",
                "ts_utc": at,
                "credential": "TOP_SECRET_RAW",
            },
        }
        write_json(
            root / "operational_reliability_burn_status.json",
            {
                "checked_at_utc": at,
                "no_automatic_recovery": True,
                "fast_feedback": {"youtube_input_quality": feedback},
            },
        )
        gate = {
            "compliance_status": "met",
            "measurement_status": "valid",
            "measurement_unknown_reasons": [],
            "sli_pct": 100.0,
            "coverage_pct": 100.0,
            "source_freshness_pct": 100.0,
            "private_evidence": "TOP_SECRET_FORMAL",
        }
        write_json(
            root / "operational_reliability_status.json",
            {
                "checked_at_utc": at,
                "no_automatic_recovery": True,
                "formal_gates": {name: gate for name in FORMAL_OBJECTIVES},
            },
        )
        write_json(
            root / "network_observer_latest.json",
            {
                "schema": "stream_v2_network_observer/v1",
                "ts_utc": at,
                "rtmps_host": "private-endpoint",
                "addresses": {"ipv4": ["TOP_SECRET_ADDRESS"]},
                "classification": {
                    "status": "ok",
                    "cause_layer": "none",
                    "cause": "ok",
                    "affected_path": "none",
                    "signals": {
                        "v4_route_changed": False,
                        "tcp_connect_ipv4_ok": True,
                        "private_signal": "TOP_SECRET_SIGNAL",
                    },
                },
            },
        )
        write_json(
            root / "resource_memory.json",
            {
                "schema_version": "resource_memory.v1",
                "ts_utc": at,
                "assessment": {
                    "status": "ok",
                    "memory_is_sli": False,
                    "restart_allowed_by_memory_alone": False,
                    "baseline_ready": True,
                    "baseline_coverage_sec": 700000,
                    "current_pressure": {
                        "mem_available_warn": False,
                        "mem_available_critical": False,
                        "mem_available_emergency": False,
                        "swap_growth": False,
                        "sustained_swap_growth": False,
                        "psi_some": False,
                        "psi_full": False,
                        "oom_event_delta": 0,
                    },
                    "swap_capacity": {
                        "used_ratio": 0.75,
                        "observe": True,
                        "warn": True,
                        "critical": False,
                    },
                    "reason": "private detail",
                },
                "host_memory": {
                    "mem_available_mb": 8192.0,
                    "mem_available_ratio": 0.5,
                    "swap_used_mb": 3072.0,
                    "swap_used_ratio": 0.75,
                    "raw": "TOP_SECRET_MEMORY",
                },
                "memory_pressure": {"some_avg10": 0.0, "full_avg10": 0.0},
                "processes": [{"cmdline": "TOP_SECRET_COMMAND"}],
            },
        )
        write_json(
            root / "memory_status.json",
            {
                "schema_version": 3,
                "classification_policy_version": "memory-status-v3",
                "generated_at_utc": at,
                "metric_classification": "guardrail",
                "overall": {
                    "severity": "ok",
                    "current_incident": False,
                    "warn": False,
                    "operational_adequacy_severity": "ok",
                    "private_detail": "TOP_SECRET_MEMORY_STATUS",
                },
                "host": {
                    "mem_available_bytes": 8 * 1024**3,
                    "mem_available_reference_pct": 50.0,
                    "swap_total_bytes": 4 * 1024**3,
                    "swap_used_bytes": 3 * 1024**3,
                    "swap_used_reference_pct": 75.0,
                    "swap_capacity": {
                        "severity": "warn",
                        "observe_ratio": 0.5,
                        "warn_ratio": 0.75,
                        "critical_ratio": 0.9,
                        "observe_bytes": 2 * 1024**3,
                        "warn_bytes": 3 * 1024**3,
                        "critical_bytes": int(4 * 1024**3 * 0.9),
                        "contributes_to_current_severity": False,
                        "reasons": ["private detail"],
                    },
                },
                "services": [{"unit": "private-unit", "command": "TOP_SECRET_COMMAND"}],
            },
        )
        write_json(
            root / "recovery_action_plan.json",
            {
                "ts_utc": at,
                "action": "none",
                "scope": "none",
                "mode": "shadow",
                "executable": False,
                "execute": False,
                "reason": "no_action",
                "blocked_by": ["shadow_mode"],
                "steps": [{"command": ["TOP_SECRET_COMMAND"]}],
            },
        )
        write_json(
            root / "stream_notify_state.json",
            {
                "updated_ts_utc": at,
                "maintenance_active": False,
                "active": {},
                "webhook_url": "TOP_SECRET_WEBHOOK",
            },
        )
        (root / "stream_notify_outbox.jsonl").write_text("", encoding="utf-8")
        write_json(
            root / "watchdog" / "adsb_freshness_state.json",
            {
                "ts_utc": at,
                "status": "ok",
                "aircraft_count": 12,
                "source_messages": 100,
                "sample_ts": BASE_TS,
                "source_now": BASE_TS,
                "reason": "",
                "source_url": "private-endpoint",
            },
        )
        write_json(
            root / "reports" / "youtube_api_cost" / "open_day_latest.json",
            {
                "status": "ok",
                "target_day": "2026-08-15",
                "window": {
                    "open_day": True,
                    "start_utc": utc_text(BASE_TS - 600),
                    "end_utc": utc_text(BASE_TS + 600),
                    "effective_end_utc": at,
                    "lag_sec": 120,
                },
                "totals": {"calls": 10, "units": 10, "quota_exceeded_events": 0},
                "ingest": {
                    "log_exists": True,
                    "coverage_ok": True,
                    "parse_errors": 0,
                    "missing_ts": 0,
                    "coverage_observed_ratio": 1.0,
                    "log_file": "private-endpoint",
                },
            },
        )
        write_json(
            root / "v3_control_state.json",
            {
                "schema": "stream_v3.control_loop_state.v2",
                "updated_at_utc": at,
                "mode": "monitor",
                "configured_task_count": 1,
                "all_tasks_observed": True,
                "fresh": True,
                "ok": True,
                "failed_tasks": [],
                "stale_or_unobserved_tasks": [],
                "tasks": {
                    "monitor": {
                        "status": "good",
                        "interval_sec": 60,
                        "timeout_sec": 20,
                        "run_count": 2,
                        "consecutive_failures": 0,
                        "last_returncode": 0,
                        "last_duration_sec": 1.0,
                        "last_completed_at_utc": at,
                        "last_success_at_utc": at,
                        "last_failure_at_utc": None,
                        "next_due_at_utc": utc_text(BASE_TS + 60),
                        "fresh_until_utc": utc_text(BASE_TS + 95),
                        "command": ["TOP_SECRET_COMMAND"],
                    }
                },
            },
        )


if __name__ == "__main__":
    unittest.main()
