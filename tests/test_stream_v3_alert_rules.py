from __future__ import annotations

import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
RULE_ROOT = ROOT / "ops" / "monitoring" / "prometheus" / "rules"
CORE_RULES = RULE_ROOT / "stream_v3.yml"
MAP_RULES = RULE_ROOT / "stream_v3_map.yml"
ALERT_PATTERN = re.compile(r"(?m)^\s*-\s+alert:\s*(StreamV3[A-Za-z0-9_]+)\s*$")


class StreamV3AlertRuleTests(unittest.TestCase):
    def test_core_rule_source_retains_the_complete_operational_surface(self) -> None:
        source = CORE_RULES.read_text(encoding="utf-8")
        expected = {
            "StreamV3ObservabilityMonitorExporterDown",
            "StreamV3ObservabilityMonitorExporterFailing",
            "StreamV3CurrentFail",
            "StreamV3ExporterCacheStale",
            "StreamV3ExporterRefreshFailing",
            "StreamV3HealthPassMissingOrFalse",
            "StreamV3HealthSnapshotMissingOrStale",
            "StreamV3IngestDisconnected",
            "StreamV3MemoryCurrentNotOk",
            "StreamV3MonitoringV4SentinelUnhealthy",
            "StreamV3MonitoringWatchdogFailed",
            "StreamV3MonitoringWatchdogStale",
            "StreamV3NetworkObserverNotOk",
            "StreamV3NetworkObserverStale",
            "StreamV3NotifyActiveIncident",
            "StreamV3NotifyBacklog",
            "StreamV3ObjectiveSnapshotMissingOrStale",
            "StreamV3RecoveryActionPending",
            "StreamV3RequiredMetricMissing",
            "StreamV3RuntimeMemoryOverWarning",
            "StreamV3RuntimeMemorySampleMissing",
            "StreamV3SameUrlNotLive",
            "StreamV3StreamWatchdogNotOk",
            "StreamV3SubsystemsUnhealthy",
            "StreamV3YouTubeApiBurnRateActive",
            "StreamV3YouTubeWatchdogUnhealthy",
        }
        self.assertEqual(set(ALERT_PATTERN.findall(source)), expected)

    def test_every_core_alert_metric_dependency_has_absence_detection(self) -> None:
        source = CORE_RULES.read_text(encoding="utf-8")
        block = source.split("- alert: StreamV3RequiredMetricMissing", 1)[1].split(
            "- alert: StreamV3ObservabilityMonitorExporterDown",
            1,
        )[0]
        required = {
            "up",
            "stream_v3_exporter_up",
            "stream_v3_exporter_last_refresh_success",
            "stream_v3_exporter_cache_age_seconds",
            "stream_v3_health_snapshot_used",
            "stream_v3_health_snapshot_available",
            "stream_v3_health_snapshot_age_seconds",
            "stream_v3_objective_snapshot_used",
            "stream_v3_objective_snapshot_available",
            "stream_v3_objective_snapshot_age_seconds",
            "stream_v3_monitoring_watchdog_age_seconds",
            "stream_v3_monitoring_watchdog_all_ok",
            "stream_v3_monitoring_watchdog_check_ok",
            "stream_v3_current_fail",
            "stream_v3_health_pass",
            "stream_v3_subsystems_healthy",
            "stream_v3_same_url_live",
            "stream_v3_youtube_ingest_connected",
            "stream_v3_youtube_watchdog_healthy",
            "stream_v3_stream_watchdog_ok",
            "stream_v3_network_ok",
            "stream_v3_network_observer_age_seconds",
            "stream_v3_notify_active_incidents",
            "stream_v3_notify_pending",
            "stream_v3_memory_current_ok",
            "stream_v3_runtime_memory_sample_available",
            "stream_v3_runtime_memory_current_ok",
            "stream_v3_youtube_api_burn_rate_active",
            "stream_v3_recovery_action_pending",
        }
        for metric in required:
            with self.subTest(metric=metric):
                self.assertIn(f"absent({metric}", block)

    def test_alert_names_are_unique_across_core_and_map_groups(self) -> None:
        names: list[str] = []
        for path in (CORE_RULES, MAP_RULES):
            names.extend(ALERT_PATTERN.findall(path.read_text(encoding="utf-8")))

        self.assertEqual(len(names), 37)
        self.assertEqual(len(names), len(set(names)))

    def test_v4_sentinel_alert_is_detection_only_and_not_runtime_recovery(self) -> None:
        source = CORE_RULES.read_text(encoding="utf-8")
        block = source.split("- alert: StreamV3MonitoringV4SentinelUnhealthy", 1)[
            1
        ].split("- alert: StreamV3CurrentFail", 1)[0]

        self.assertIn('check="monitoring_v4_host_sentinel"', block)
        self.assertIn("absent(", block)
        self.assertIn("== 0", block)
        self.assertIn("no k3s restart", block)
        self.assertIn("do not infer a stream delivery failure", block)


if __name__ == "__main__":
    unittest.main()
