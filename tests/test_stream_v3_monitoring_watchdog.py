from __future__ import annotations

import importlib.util
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock


def load_watchdog_module():
    path = Path(__file__).resolve().parents[1] / "ops" / "scripts" / "stream_v3_monitoring_watchdog.py"
    spec = importlib.util.spec_from_file_location("stream_v3_monitoring_watchdog", path)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


class StreamV3MonitoringWatchdogTests(unittest.TestCase):
    def test_run_checks_writes_ok_state_when_metrics_and_snapshots_are_valid(self) -> None:
        module = load_watchdog_module()
        metrics = "\n".join(
            [
                "stream_v3_exporter_up 1",
                'stream_v3_health_pass{window_hours="1"} 1',
                "stream_v3_youtube_watchdog_healthy 1",
                "stream_v3_recovery_action_pending 0",
            ]
        )
        with tempfile.TemporaryDirectory() as td:
            state_root = Path(td)
            state_file = state_root / "monitoring_watchdog_state.json"
            for name in ("health_summary_snapshot.json", "objective_sli_snapshot.json"):
                (state_root / name).write_text(
                    json.dumps({"_snapshot": {"snapshot_ts_utc": "2099-01-01T00:00:00Z"}}),
                    encoding="utf-8",
                )

            with mock.patch.object(module, "http_get_text", return_value=(200, metrics)):
                payload = module.run_checks(
                    state_root=state_root,
                    state_file=state_file,
                    exporter_url="http://127.0.0.1:9108/metrics",
                    prometheus_url="",
                    grafana_url="",
                    timeout_sec=1,
                    snapshot_max_age_sec=600,
                    required_metrics=module.DEFAULT_REQUIRED_METRICS,
                    repair_enabled=False,
                    repair_command="",
                    repair_timeout_sec=1,
                    now=4070908860.0,
                )

            written = json.loads(state_file.read_text(encoding="utf-8"))

        self.assertTrue(payload["ok"])
        self.assertTrue(written["checks"]["exporter_http"]["ok"])
        self.assertTrue(written["checks"]["metrics_contract"]["ok"])
        self.assertTrue(written["checks"]["snapshot_freshness"]["ok"])
        self.assertFalse(written["repair_attempted"])

    def test_missing_required_metric_fails_without_repair_by_default(self) -> None:
        module = load_watchdog_module()
        with tempfile.TemporaryDirectory() as td:
            state_root = Path(td)
            state_file = state_root / "monitoring_watchdog_state.json"
            for name in ("health_summary_snapshot.json", "objective_sli_snapshot.json"):
                (state_root / name).write_text(
                    json.dumps({"_snapshot": {"snapshot_ts_utc": "2099-01-01T00:00:00Z"}}),
                    encoding="utf-8",
                )

            with mock.patch.object(module, "http_get_text", return_value=(200, "stream_v3_exporter_up 1\n")):
                payload = module.run_checks(
                    state_root=state_root,
                    state_file=state_file,
                    exporter_url="http://127.0.0.1:9108/metrics",
                    prometheus_url="",
                    grafana_url="",
                    timeout_sec=1,
                    snapshot_max_age_sec=600,
                    required_metrics=module.DEFAULT_REQUIRED_METRICS,
                    repair_enabled=False,
                    repair_command="",
                    repair_timeout_sec=1,
                    now=4070908860.0,
                )

        self.assertFalse(payload["ok"])
        self.assertFalse(payload["checks"]["metrics_contract"]["ok"])
        self.assertIn("stream_v3_health_pass", payload["checks"]["metrics_contract"]["missing"])
        self.assertFalse(payload["repair_attempted"])


if __name__ == "__main__":
    unittest.main()
