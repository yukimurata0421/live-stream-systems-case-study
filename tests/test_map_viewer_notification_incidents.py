from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from stream_core.notifications import incidents


NOW = 1_786_060_800


def write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def append_jsonl(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload) + "\n")


class MapViewerNotificationIncidentTests(unittest.TestCase):
    def test_map_delivery_critical_waits_two_minutes_then_escalates(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            state = root / "map.json"
            history = root / "map.jsonl"
            write_json(
                state,
                {
                    "schema": "stream_v3.map_runtime_monitor.v1",
                    "checked_at_utc": "2026-08-07T00:00:00Z",
                    "delivery_critical_ok": False,
                    "weather_ok": True,
                    "critical_reasons": ["render_heartbeat"],
                    "browser": {},
                    "pod": {"containers": {}},
                },
            )
            for ts in ("2026-08-06T23:58:00Z", "2026-08-06T23:59:00Z", "2026-08-07T00:00:00Z"):
                append_jsonl(history, {"checked_at_utc": ts, "delivery_critical_ok": False, "weather_ok": True})

            found = incidents.map_runtime_incidents(status_file=state, history_file=history, now_ts=NOW)

        item = next(row for row in found if row["id"] == "map:delivery_critical")
        self.assertEqual(item["severity"], "critical")

    def test_precipitation_warning_is_not_critical(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            state = root / "map.json"
            history = root / "map.jsonl"
            write_json(
                state,
                {
                    "schema": "stream_v3.map_runtime_monitor.v1",
                    "checked_at_utc": "2026-08-07T00:00:00Z",
                    "delivery_critical_ok": True,
                    "weather_ok": False,
                    "weather_reasons": ["precipitation_fetcher_health"],
                    "browser": {},
                    "pod": {"containers": {}},
                },
            )
            for minute in range(21, -1, -1):
                ts = NOW - minute * 60
                append_jsonl(
                    history,
                    {
                        "checked_at_utc": __import__("datetime").datetime.fromtimestamp(
                            ts, __import__("datetime").timezone.utc
                        ).isoformat().replace("+00:00", "Z"),
                        "delivery_critical_ok": True,
                        "weather_ok": False,
                    },
                )

            found = incidents.map_runtime_incidents(status_file=state, history_file=history, now_ts=NOW)

        item = next(row for row in found if row["id"] == "map:precipitation_unavailable")
        self.assertEqual(item["severity"], "warning")

    def test_viewer_visual_failure_is_critical_after_two_samples(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "viewer.json"
            write_json(
                path,
                {
                    "schema": "stream_v3.viewer_synthetic.v1",
                    "checked_at_utc": "2026-08-07T00:00:00Z",
                    "status": "failed",
                    "frame_ok": True,
                    "black_detected": True,
                    "freeze_detected": False,
                    "consecutive_visual_failures": 2,
                    "consecutive_probe_failures": 0,
                },
            )
            found = incidents.viewer_synthetic_incidents(status_file=path, now_ts=NOW)

        self.assertEqual(found[0]["id"], "viewer:visual_failure")
        self.assertEqual(found[0]["severity"], "critical")


if __name__ == "__main__":
    unittest.main()
