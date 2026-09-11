from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from stream_contracts.monitoring_v4.time import utc_text
from stream_monitoring_v4.adapters.audio import LegacySubsystemAudioAdapter, legacy_audio_status
from stream_monitoring_v4.adapters.external import ExternalBlackboxAdapter
from stream_monitoring_v4.adapters.factory import SHADOW_DOMAINS, state_file_adapters
from stream_monitoring_v4.adapters.lifecycle import RuntimeLifecycleAdapter
from stream_monitoring_v4.adapters.monitoring import CHECK_ALLOWLIST, MonitoringSelfAdapter
from stream_monitoring_v4.adapters.operations import (
    MemoryStatusAdapter,
    RuntimeResourceAdapter,
    control_status,
    memory_status,
    network_status,
    notification_status,
    quota_status,
    recovery_status,
)
from stream_monitoring_v4.adapters.reliability import InputQualityProjectionAdapter
from stream_monitoring_v4.adapters.rendering import MapRuntimeAdapter, map_runtime_status
from stream_monitoring_v4.adapters.viewer import ViewerSyntheticAdapter, viewer_status
from stream_monitoring_v4.current.service import CurrentReducerService
from stream_monitoring_v4.domains.source_policy import DEFAULT_POLICIES
from stream_monitoring_v4.runtime.observer import Observer
from stream_monitoring_v4.storage.repository import MonitoringRepository

from tests.helpers import BASE_TS, write_json


class OperationalAdapterTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.at = utc_text(BASE_TS)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_new_operational_statuses_fail_closed_on_malformed_or_inconsistent_input(self) -> None:
        self.assertEqual(
            network_status({"classification": {"status": "route_change_observed"}}),
            "bad",
        )
        self.assertEqual(
            network_status({"classification": {"status": "incident_candidate"}}),
            "bad",
        )
        self.assertEqual(
            network_status({"classification": {"status": "unrecognized"}}),
            "unknown",
        )
        self.assertEqual(recovery_status({"action": "none", "execute": False}), "good")
        self.assertEqual(recovery_status({"action": "restart", "execute": False}), "bad")
        self.assertEqual(recovery_status({"action": "none", "execute": True}), "bad")
        self.assertEqual(recovery_status({"action": "none"}), "unknown")
        self.assertEqual(memory_status({"overall": {"severity": "ok"}}), "good")
        self.assertEqual(memory_status({"overall": {"severity": "warn"}}), "bad")
        self.assertEqual(memory_status({"overall": {"severity": "mystery"}}), "unknown")
        self.assertEqual(
            quota_status(
                {
                    "status": "ok",
                    "totals": {"quota_exceeded_events": "not-an-integer"},
                    "ingest": {"coverage_ok": True},
                }
            ),
            "unknown",
        )
        self.assertEqual(
            notification_status(
                {"state_contract_valid": True, "active_incident_count": 0},
                {
                    "source_present": False,
                    "pending_count": 0,
                    "invalid_row_count": 0,
                },
            ),
            "unknown",
        )
        self.assertEqual(
            notification_status(
                {"state_contract_valid": True, "active_incident_count": 0},
                {
                    "source_present": True,
                    "pending_count": True,
                    "invalid_row_count": 0,
                },
            ),
            "unknown",
        )
        self.assertEqual(
            control_status(
                {
                    "schema": "stream_v3.control_loop_state.v2",
                    "configured_task_count": 2,
                    "all_tasks_observed": True,
                    "fresh": True,
                    "ok": True,
                    "failed_tasks": [],
                    "stale_or_unobserved_tasks": [],
                    "tasks": {"only-one": {"status": "good"}},
                }
            ),
            "unknown",
        )
        self.assertEqual(
            control_status(
                {
                    "schema": "stream_v3.control_loop_state.v2",
                    "configured_task_count": 1,
                    "all_tasks_observed": True,
                    "fresh": True,
                    "ok": True,
                    "failed_tasks": [],
                    "stale_or_unobserved_tasks": [],
                    "tasks": {"one": {"status": "failed"}},
                }
            ),
            "bad",
        )

    def test_map_keeps_delivery_and_weather_semantics_without_raw_probe_errors(self) -> None:
        path = self.root / "map_runtime_status.json"
        write_json(
            path,
            {
                "schema": "stream_v3.map_runtime_monitor.v1",
                "checked_at_utc": self.at,
                "status": "degraded",
                "delivery_critical_ok": True,
                "weather_ok": False,
                "conditions": {
                    "deployment_ready": True,
                    "precipitation_data_ok": False,
                    "precipitation_generation_integrity": False,
                    "precipitation_status": False,
                    "precipitation_render_applied": False,
                    "precipitation_validtime_match": False,
                },
                "critical_reasons": [],
                "weather_reasons": ["precipitation_status"],
                "probe_errors": ["private endpoint must not leave adapter"],
                "pod": {"uid": "must-not-leave-adapter"},
            },
        )
        batch = MapRuntimeAdapter(state_file=path).collect(received_at=self.at)
        self.assertEqual(batch.rejections, ())
        self.assertEqual(batch.observations[0].status, "bad")
        self.assertEqual(batch.observations[0].payload["probe_error_count"], 1)
        self.assertEqual(
            batch.observations[0].payload["conditions"],
            {
                "deployment_ready": True,
                "precipitation_data_ok": False,
                "precipitation_generation_integrity": False,
                "precipitation_render_applied": False,
                "precipitation_status": False,
                "precipitation_validtime_match": False,
            },
        )
        rendered = repr(batch.observations[0].payload)
        self.assertNotIn("private endpoint", rendered)
        self.assertNotIn("must-not-leave", rendered)
        self.assertEqual(
            map_runtime_status({"status": "unknown", "delivery_critical_ok": None}),
            "unknown",
        )

    def test_memory_guardrail_is_current_authority_and_resource_assessment_is_diagnostic(self) -> None:
        write_json(
            self.root / "resource_memory.json",
            {
                "ts_utc": self.at,
                "assessment": {
                    "status": "observe",
                    "baseline_ready": True,
                    "baseline_coverage_sec": 604800,
                    "current_pressure": {
                        "swap_growth": False,
                        "oom_event_delta": 0,
                    },
                    "swap_capacity": {
                        "used_ratio": 0.75,
                        "observe": True,
                        "warn": True,
                        "critical": False,
                    },
                },
                "host_memory": {
                    "mem_available_mb": 8192.0,
                    "mem_available_ratio": 0.5,
                    "swap_used_mb": 3072.0,
                    "swap_used_ratio": 0.75,
                },
            },
        )
        write_json(
            self.root / "memory_status.json",
            {
                "generated_at_utc": self.at,
                "overall": {"severity": "warn"},
                "host": {
                    "mem_available_bytes": 3 * 1024**3,
                    "swap_capacity": {
                        "severity": "warn",
                        "warn_ratio": 0.75,
                        "contributes_to_current_severity": False,
                    },
                },
            },
        )
        resource_adapter = RuntimeResourceAdapter(
            state_file=self.root / "resource_memory.json"
        )
        memory_adapter = MemoryStatusAdapter(
            state_file=self.root / "memory_status.json"
        )
        resource_payload = resource_adapter.collect(received_at=self.at).observations[0].payload
        memory_payload = memory_adapter.collect(received_at=self.at).observations[0].payload
        self.assertEqual(resource_payload["assessment"]["baseline_coverage_sec"], 604800)
        self.assertTrue(resource_payload["swap_capacity"]["warn"])
        self.assertEqual(memory_payload["swap_capacity"]["severity"], "warn")

        repository = MonitoringRepository(self.root / "memory.sqlite3")
        repository.initialize(applied_at=self.at)
        Observer(repository).run_once(
            (resource_adapter, memory_adapter),
            now_ts=BASE_TS,
        )

        current = CurrentReducerService(repository, DEFAULT_POLICIES).reduce(
            ("runtime_resource",), now_ts=BASE_TS
        )[0]

        self.assertEqual(current.state, "bad")
        self.assertEqual(current.payload["selected"][0]["source"], "memory_status")
        self.assertEqual(
            current.payload["diagnostics"][0]["source"], "resource_memory"
        )

    def test_audio_bridge_is_explicitly_legacy_and_drops_title_and_target(self) -> None:
        path = self.root / "subsystems_status.json"
        write_json(
            path,
            {
                "ts_utc": self.at,
                "run_id": "must-not-leave-adapter",
                "music": {
                    "state": "healthy",
                    "confidence": "high",
                    "evidence": ["now_playing_fresh"],
                    "now_playing_title": "private-ish title",
                    "now_playing_status": "playing",
                    "audio_fail_count": 0,
                    "target": {"stream_key_hash": "must-not-leave-adapter"},
                },
            },
        )
        batch = LegacySubsystemAudioAdapter(state_file=path).collect(received_at=self.at)
        item = batch.observations[0]
        self.assertEqual((item.source, item.status), ("legacy_subsystem_audio", "good"))
        self.assertEqual(item.payload["origin"], "subsystems_status.music")
        self.assertNotIn("title", repr(item.payload).lower())
        self.assertNotIn("stream_key", repr(item.payload))
        self.assertEqual(legacy_audio_status({}), "unknown")

    def test_viewer_capture_failure_is_unknown_but_confirmed_visual_failure_is_bad(self) -> None:
        self.assertEqual(viewer_status({"frame_ok": False}), "unknown")
        self.assertEqual(viewer_status({"frame_ok": True, "black_detected": True}), "bad")
        path = self.root / "viewer_synthetic_status.json"
        write_json(
            path,
            {
                "checked_at_utc": self.at,
                "status": "healthy",
                "frame_ok": True,
                "black_detected": False,
                "freeze_detected": False,
                "capture_sha256": "must-not-leave-adapter",
                "capture_file": "/private/path",
                "video_id": "must-not-leave-adapter",
                "reason": "",
            },
        )
        batch = ViewerSyntheticAdapter(state_file=path).collect(received_at=self.at)
        self.assertEqual(len(batch.observations), 2)
        self.assertEqual({item.domain for item in batch.observations}, {"rendering", "viewer_external"})
        self.assertEqual({item.status for item in batch.observations}, {"good"})
        rendered = repr([item.payload for item in batch.observations])
        self.assertNotIn("capture_sha256", rendered)
        self.assertNotIn("video_id", rendered)

    def test_external_uses_source_evidence_time_and_fixed_target_allowlist(self) -> None:
        path = self.root / "external_blackbox_status.json"
        write_json(
            path,
            {
                "checked_at_utc": self.at,
                "evidence_at_utc": self.at,
                "status": "ok",
                "reason": "all_external_targets_passed",
                "execution_boundary": "must-not-leave-adapter",
                "targets": {
                    "public_status": {
                        "status": "ok",
                        "host": "must-not-leave-adapter",
                        "observed_locations": 3,
                        "passed_locations": 3,
                    },
                    "youtube_public_video": {"status": "ok", "observed_locations": 3},
                    "unexpected_target": {"status": "failed"},
                },
            },
        )
        item = ExternalBlackboxAdapter(state_file=path).collect(received_at=self.at).observations[0]
        self.assertEqual(item.status, "good")
        self.assertEqual(item.observed_at, self.at)
        self.assertEqual(set(item.payload["targets"]), {"public_status", "youtube_public_video"})
        self.assertNotIn("host", repr(item.payload))
        self.assertNotIn("execution_boundary", repr(item.payload))

    def test_monitoring_requires_all_fixed_checks_and_drops_repair_commands(self) -> None:
        path = self.root / "monitoring_watchdog_state.json"
        checks = {
            name: {"ok": True, "reason": "may contain an endpoint", "repair": "must-not-leave-adapter"}
            for name in CHECK_ALLOWLIST
        }
        write_json(
            path,
            {
                "updated_at_utc": self.at,
                "repair_enabled": True,
                "checks": checks,
                "repairs": ["must-not-leave-adapter"],
            },
        )
        item = MonitoringSelfAdapter(state_file=path).collect(received_at=self.at).observations[0]
        self.assertEqual(item.status, "good")
        self.assertEqual(item.payload["ok_count"], len(CHECK_ALLOWLIST))
        self.assertNotIn("repair", repr(item.payload).replace("repair_enabled", ""))
        checks[CHECK_ALLOWLIST[0]]["ok"] = False
        write_json(path, {"updated_at_utc": self.at, "checks": checks})
        item = MonitoringSelfAdapter(state_file=path).collect(received_at=self.at).observations[0]
        self.assertEqual(item.status, "bad")
        self.assertEqual(item.payload["failed_checks"], [CHECK_ALLOWLIST[0]])

    def test_cached_input_quality_projections_are_never_current_authority(self) -> None:
        path = self.root / "operational_reliability_burn_status.json"
        write_json(
            path,
            {
                "checked_at_utc": self.at,
                "fast_feedback": {
                    "youtube_input_quality": {
                        "measurement_status": "valid",
                        "measurement_unknown_reasons": [],
                        "target_met_on_observed_samples": False,
                        "sli_pct": 89.0,
                        "coverage_pct": 100.0,
                        "source_freshness_pct": 100.0,
                        "prometheus_current": {
                            "available": True,
                            "eligible": True,
                            "good": False,
                            "ts_utc": self.at,
                        },
                    }
                },
            },
        )
        batch = InputQualityProjectionAdapter(burn_status_file=path).collect(received_at=self.at)
        self.assertEqual(len(batch.observations), 2)
        roles = {item.source: item.evidence_role for item in batch.observations}
        self.assertEqual(roles["youtube_input_quality_prometheus"], "current_correlated")
        self.assertEqual(roles["youtube_input_quality_rolling"], "historical")
        self.assertNotIn("current_authoritative", roles.values())

    def test_runtime_lifecycle_adapter_is_historical_and_rejects_future_recovery(self) -> None:
        path = self.root / "runtime_lifecycle_events.json"

        def event(*, event_id: str, recovered_at: str) -> dict[str, object]:
            return {
                "schema": "monitoring_v4.runtime_lifecycle_event.v1",
                "event_id": event_id,
                "event_type": "ffmpeg_auto_recovered",
                "opened_at": utc_text(BASE_TS - 5),
                "recovered_at": recovered_at,
                "run_id": "run-lifecycle",
                "restart_count": 1,
                "exit_code": 1,
                "delay_sec": 5,
                "scheduled_event_id": f"{event_id}-scheduled",
                "recovered_event_id": f"{event_id}-recovered",
            }

        write_json(
            path,
            {
                "schema": "monitoring_v4.runtime_lifecycle_projection.v1",
                "events": [event(event_id="evt-ok", recovered_at=self.at)],
            },
        )
        accepted = RuntimeLifecycleAdapter(state_file=path).collect(received_at=self.at)
        self.assertEqual(accepted.rejections, ())
        self.assertEqual(len(accepted.observations), 1)
        self.assertEqual(accepted.observations[0].evidence_role, "historical")
        self.assertEqual(accepted.observations[0].status, "not_applicable")

        write_json(
            path,
            {
                "schema": "monitoring_v4.runtime_lifecycle_projection.v1",
                "events": [
                    event(
                        event_id="evt-future",
                        recovered_at=utc_text(BASE_TS + 1),
                    )
                ],
            },
        )
        rejected = RuntimeLifecycleAdapter(state_file=path).collect(received_at=self.at)
        self.assertEqual(rejected.observations, ())
        self.assertEqual(
            {item.reason_code for item in rejected.rejections},
            {"source_timestamp_future"},
        )

        write_json(
            path,
            {
                "schema": "monitoring_v4.runtime_lifecycle_projection.v1",
                "events": "bad",
            },
        )
        invalid = RuntimeLifecycleAdapter(state_file=path).collect(received_at=self.at)
        self.assertEqual(
            {item.reason_code for item in invalid.rejections},
            {"runtime_lifecycle_contract_invalid"},
        )

    def test_factory_reduces_complete_operational_domain_surface_from_read_only_files(self) -> None:
        self._write_complete_healthy_state()
        repository = MonitoringRepository(self.root / "v4" / "monitoring.sqlite3")
        repository.initialize(applied_at=self.at)
        run = Observer(repository).run_once(state_file_adapters(self.root), now_ts=BASE_TS)
        self.assertEqual(run.adapters, 16)
        self.assertEqual(run.inserted_rejections, 0)
        currents = CurrentReducerService(repository, DEFAULT_POLICIES).reduce(
            SHADOW_DOMAINS,
            now_ts=BASE_TS,
        )
        self.assertEqual({item.domain for item in currents}, set(SHADOW_DOMAINS))
        self.assertEqual({item.state for item in currents}, {"good"})

    def _write_complete_healthy_state(self) -> None:
        write_json(
            self.root / "youtube_watchdog_stats.json",
            {
                "stats_file_updated_at_utc": self.at,
                "remote_probe_ts_utc": self.at,
                "oauth_checked_ts_utc": self.at,
                "remote_sample_id": "safe-sample",
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
            self.root / "youtube_video_id_resolver_state.json",
            {"ts_utc": self.at, "video_id": "safe", "expected_video_id": "safe"},
        )
        write_json(
            self.root / "map_runtime_status.json",
            {
                "checked_at_utc": self.at,
                "status": "healthy",
                "delivery_critical_ok": True,
                "weather_ok": True,
            },
        )
        write_json(
            self.root / "subsystems_status.json",
            {"ts_utc": self.at, "music": {"state": "healthy"}},
        )
        write_json(
            self.root / "viewer_synthetic_status.json",
            {
                "checked_at_utc": self.at,
                "status": "healthy",
                "frame_ok": True,
                "black_detected": False,
                "freeze_detected": False,
            },
        )
        write_json(
            self.root / "external_blackbox_status.json",
            {"checked_at_utc": self.at, "evidence_at_utc": self.at, "status": "ok"},
        )
        write_json(
            self.root / "monitoring_watchdog_state.json",
            {
                "updated_at_utc": self.at,
                "checks": {name: {"ok": True} for name in CHECK_ALLOWLIST},
            },
        )
        write_json(
            self.root / "operational_reliability_burn_status.json",
            {
                "checked_at_utc": self.at,
                "fast_feedback": {
                    "youtube_input_quality": {
                        "measurement_status": "valid",
                        "target_met_on_observed_samples": True,
                        "prometheus_current": {
                            "available": True,
                            "eligible": True,
                            "good": True,
                            "ts_utc": self.at,
                        },
                    }
                },
            },
        )
        write_json(
            self.root / "runtime_lifecycle_events.json",
            {
                "schema": "monitoring_v4.runtime_lifecycle_projection.v1",
                "events": [],
            },
        )
        write_json(
            self.root / "network_observer.json",
            {
                "ts_utc": self.at,
                "classification": {"status": "ok", "signals": {"tcp_connect_ipv4_ok": True}},
            },
        )
        write_json(
            self.root / "resource_memory.json",
            {"ts_utc": self.at, "assessment": {"status": "ok"}},
        )
        write_json(
            self.root / "memory_status.json",
            {
                "generated_at_utc": self.at,
                "schema_version": 3,
                "classification_policy_version": "memory-status-v3",
                "metric_classification": "guardrail",
                "overall": {"severity": "ok", "current_incident": False, "warn": False},
            },
        )
        write_json(
            self.root / "adsb_freshness_state.json",
            {"ts_utc": self.at, "status": "ok", "aircraft_count": 10},
        )
        write_json(
            self.root / "youtube_api_quota_state.json",
            {
                "status": "ok",
                "window": {"effective_end_utc": self.at},
                "totals": {"quota_exceeded_events": 0},
                "ingest": {"coverage_ok": True},
            },
        )
        write_json(
            self.root / "recovery_action_plan.json",
            {"ts_utc": self.at, "action": "none", "mode": "shadow", "execute": False},
        )
        write_json(
            self.root / "notification_state.json",
            {
                "updated_ts_utc": self.at,
                "maintenance_active": False,
                "active_incident_count": 0,
                "state_contract_valid": True,
            },
        )
        write_json(
            self.root / "notification_outbox_status.json",
            {
                "pending_count": 0,
                "invalid_row_count": 0,
                "max_attempts": 0,
                "source_present": True,
            },
        )
        write_json(
            self.root / "control_loop_state.json",
            {
                "schema": "stream_v3.control_loop_state.v2",
                "updated_at_utc": self.at,
                "configured_task_count": 2,
                "all_tasks_observed": True,
                "fresh": True,
                "ok": True,
                "failed_tasks": [],
                "stale_or_unobserved_tasks": [],
                "tasks": {
                    "one": {"status": "good", "consecutive_failures": 0},
                    "two": {"status": "good", "consecutive_failures": 0},
                },
            },
        )


if __name__ == "__main__":
    unittest.main()
