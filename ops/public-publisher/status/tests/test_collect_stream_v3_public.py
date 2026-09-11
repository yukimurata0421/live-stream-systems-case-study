from __future__ import annotations

import importlib.util
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock


SAME_DIR_MODULE = Path(__file__).with_name("collect_stream_v3_public.py")
PARENT_DIR_MODULE = Path(__file__).resolve().parents[1] / "collect_stream_v3_public.py"
MODULE_PATH = SAME_DIR_MODULE if SAME_DIR_MODULE.exists() else PARENT_DIR_MODULE


def load_collector():
    spec = importlib.util.spec_from_file_location("collect_stream_v3_public_tested", MODULE_PATH)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


class PublicMapCollectorTests(unittest.TestCase):
    def test_map_queries_are_public_aggregates(self) -> None:
        collector = load_collector()
        map_queries = [item for item in collector.PROM_QUERIES if item["id"].startswith("map_")]

        self.assertEqual(len(map_queries), 8)
        for item in map_queries:
            query = item["query"]
            self.assertIn("max(", query)
            self.assertNotIn("pod=", query)
            self.assertNotIn("container=", query)
            self.assertNotIn(" by (", query)

    def test_optional_map_degradation_is_warning_not_delivery_failure(self) -> None:
        collector = load_collector()
        weather = next(item for item in collector.PROM_QUERIES if item["id"] == "map_weather_issue")
        restarts = next(item for item in collector.PROM_QUERIES if item["id"] == "map_runtime_restarts")
        delivery = next(item for item in collector.PROM_QUERIES if item["id"] == "map_delivery_issue")

        self.assertEqual(collector.classify_prom(weather, 1), "warn")
        self.assertEqual(collector.classify_prom(restarts, 1), "warn")
        self.assertEqual(collector.classify_prom(delivery, 1), "bad")
        self.assertEqual(collector.classify_prom(weather, 0), "ok")

    def test_generated_snapshot_contains_map_runtime_group(self) -> None:
        collector = load_collector()

        def fake_query(query: str):
            value = "0" if query.startswith("1 - max(") else "1"
            return [{"metric": {}, "value": [1_785_832_400, value]}]

        with (
            mock.patch.object(collector, "prom_query", side_effect=fake_query),
            mock.patch.object(collector, "collect_prometheus_trends", return_value=[]),
            mock.patch.object(collector.time, "time", return_value=1_785_832_400.0),
        ):
            payload = collector.collect_prometheus()

        items = {item["id"]: item for item in payload["items"]}
        self.assertEqual(payload["summary"]["total"], 23)
        self.assertEqual(items["map_delivery_issue"]["state"], "ok")
        self.assertEqual(items["map_weather_issue"]["state"], "ok")
        self.assertEqual(items["map_sample_age"]["group"], "Map Runtime")
        self.assertEqual(
            items["map_runtime_restarts"]["label"],
            "Kubernetes container restarts (current Pod)",
        )
        self.assertNotIn("pod", items["map_runtime_restarts"]["series"][0]["metric"])
        self.assertNotIn("container", items["map_runtime_restarts"]["series"][0]["metric"])

        self.assertEqual(items["restarts_1h"]["label"], "Fast Recovery dispatches 1h")
        self.assertEqual(items["ffmpeg_clusters_1h"]["label"], "FFmpeg incident clusters 1h")


class RecoveryActivityTests(unittest.TestCase):
    def test_ffmpeg_child_dispatch_attempt_is_not_counted_as_stream_restart(self) -> None:
        collector = load_collector()
        event = {
            "kind": "restart",
            "recovery_action_id": "fra-1787473384-c2d0d385853f",
            "recovery_scope": "ffmpeg_child",
            "requested_signal": "SIGTERM",
        }

        self.assertEqual(collector.recovery_action_for_event("fast_recovery", event), "")

    def test_fast_recovery_runtime_restart_remains_a_stream_restart(self) -> None:
        collector = load_collector()
        event = {
            "kind": "restart",
            "recovery_action_id": "fra-runtime-restart",
            "recovery_scope": "runtime",
        }

        self.assertEqual(
            collector.recovery_action_for_event("fast_recovery", event),
            "restart_stream",
        )

    def test_ffmpeg_dispatch_retries_do_not_inflate_the_actual_child_restart(self) -> None:
        collector = load_collector()
        raw_events = [
            (
                "fast_recovery",
                {
                    "kind": "restart",
                    "recovery_action_id": "fra-attempt-1",
                    "recovery_scope": "ffmpeg_child",
                },
                datetime(2026, 8, 23, 8, 22, 31, tzinfo=timezone.utc),
            ),
            (
                "fast_recovery",
                {
                    "kind": "restart",
                    "recovery_action_id": "fra-attempt-2",
                    "recovery_scope": "ffmpeg_child",
                },
                datetime(2026, 8, 23, 8, 23, 7, tzinfo=timezone.utc),
            ),
            (
                "stream_engine",
                {
                    "event_type": "ffmpeg_restart_scheduled",
                    "run_id": "20260823T052419Z-7",
                    "restart_count": 2,
                },
                datetime(2026, 8, 23, 8, 23, 6, tzinfo=timezone.utc),
            ),
        ]
        mapped = [
            {"ts": ts, "action": action, "source": source}
            for source, event, ts in raw_events
            if (action := collector.recovery_action_for_event(source, event))
        ]

        self.assertEqual(
            collector.dedupe_recovery_events(mapped),
            [
                {
                    "ts": datetime(2026, 8, 23, 8, 23, 6, tzinfo=timezone.utc),
                    "action": "restart_ffmpeg",
                    "source": "stream_engine",
                }
            ],
        )

    def test_runtime_lifecycle_notification_maps_to_stream_restart_at_event_time(self) -> None:
        collector = load_collector()
        event = {
            "ts_utc": "2026-08-25T01:44:15Z",
            "phase": "auto_recovered",
            "incident_ids": ["runtime:lifecycle:20260825T014206Z-7:1787622126"],
            "route": "discord",
        }

        self.assertEqual(
            collector.recovery_action_for_event("stream_notify_runtime", event),
            "restart_stream",
        )
        self.assertEqual(
            collector.recovery_event_datetime("stream_notify_runtime", event, "0"),
            datetime(2026, 8, 25, 1, 42, 6, tzinfo=timezone.utc),
        )

    def test_runtime_lifecycle_routes_and_matching_action_are_deduplicated(self) -> None:
        collector = load_collector()
        action_ts = datetime(2026, 8, 21, 8, 21, 45, tzinfo=timezone.utc)
        lifecycle_ts = datetime(2026, 8, 21, 8, 21, 49, tzinfo=timezone.utc)
        events = [
            {"ts": action_ts, "action": "restart_stream", "source": "remote_recovery"},
            {"ts": lifecycle_ts, "action": "restart_stream", "source": "stream_notify_runtime"},
            {"ts": lifecycle_ts, "action": "restart_stream", "source": "stream_notify_runtime"},
        ]

        self.assertEqual(collector.dedupe_recovery_events(events), [events[0]])

    def test_planned_rollout_notification_is_not_recovery_activity(self) -> None:
        collector = load_collector()
        event = {
            "phase": "planned_rollout",
            "incident_ids": ["runtime:planned_rollout:rollout-1:1787462659"],
        }

        self.assertEqual(collector.recovery_action_for_event("stream_notify_runtime", event), "")


if __name__ == "__main__":
    unittest.main()
