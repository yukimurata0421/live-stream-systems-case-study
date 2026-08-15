from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = (
    ROOT
    / "ops"
    / "public-publisher"
    / "status"
    / "collect_stream_v3_public.py"
)


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
            return [
                {
                    "metric": {
                        "job": "stream_v3_arena_monitor",
                        "instance": "192.0.2.10:9108",
                        "window_hours": "1",
                    },
                    "value": [1_785_832_400, value],
                }
            ]

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
            items["map_runtime_restarts"]["series"][0]["metric"],
            {"window_hours": "1"},
        )
        self.assertNotIn("192.0.2.10", str(payload))

    def test_public_text_redacts_secret_values_urls_and_addresses(self) -> None:
        collector = load_collector()
        value = collector.safe_text(
            "Authorization: Bearer abc123 token=hidden "
            "url=https://internal.invalid/path host=192.0.2.20"
        )

        self.assertNotIn("abc123", value)
        self.assertNotIn("hidden", value)
        self.assertNotIn("internal.invalid", value)
        self.assertNotIn("192.0.2.20", value)

    def test_public_loki_sections_do_not_publish_queries(self) -> None:
        collector = load_collector()
        with mock.patch.object(collector, "loki_query", return_value=[]):
            payload = collector.collect_loki()

        self.assertTrue(payload["sections"])
        self.assertTrue(all("query" not in section for section in payload["sections"]))


if __name__ == "__main__":
    unittest.main()
