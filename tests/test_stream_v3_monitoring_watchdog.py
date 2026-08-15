from __future__ import annotations

import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


def load_watchdog():
    path = Path(__file__).resolve().parents[1] / "ops" / "scripts" / "stream_v3_monitoring_watchdog.py"
    spec = importlib.util.spec_from_file_location("stream_v3_monitoring_watchdog", path)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class StreamV3MonitoringWatchdogTests(unittest.TestCase):
    def test_alert_rule_contract_requires_exact_source_and_live_name_sets(self) -> None:
        watchdog = load_watchdog()
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "rules.yml"
            path.write_text(
                "groups:\n  - rules:\n      - alert: StreamV3Expected\n",
                encoding="utf-8",
            )
            payload = {
                "status": "success",
                "data": {
                    "groups": [
                        {
                            "rules": [
                                {"name": "StreamV3Expected", "type": "alerting"},
                                {"name": "UnrelatedAlert", "type": "alerting"},
                            ]
                        }
                    ]
                },
            }
            with mock.patch.object(
                watchdog,
                "fetch_json",
                return_value=(True, payload, ""),
            ):
                matched = watchdog.alert_rule_contract_check((path,))
            self.assertTrue(matched.ok)
            self.assertEqual(matched.repair, "")

            payload["data"]["groups"][0]["rules"] = [
                {"name": "StreamV3Unexpected", "type": "alerting"}
            ]
            with mock.patch.object(
                watchdog,
                "fetch_json",
                return_value=(True, payload, ""),
            ):
                mismatched = watchdog.alert_rule_contract_check((path,))
            self.assertFalse(mismatched.ok)
            self.assertIn("missing=StreamV3Expected", mismatched.reason)
            self.assertIn("unexpected=StreamV3Unexpected", mismatched.reason)

    def test_alert_rule_source_rejects_duplicates_and_symlinks(self) -> None:
        watchdog = load_watchdog()
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            first = root / "first.yml"
            second = root / "second.yml"
            content = "groups:\n  - rules:\n      - alert: StreamV3Duplicate\n"
            first.write_text(content, encoding="utf-8")
            second.write_text(content, encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "duplicate"):
                watchdog.source_alert_rule_names((first, second))

            link = root / "link.yml"
            link.symlink_to(first)
            with self.assertRaisesRegex(ValueError, "linked"):
                watchdog.source_alert_rule_names((link,))

    def test_monitoring_v4_sentinel_requires_fresh_detection_only_good_contract(self) -> None:
        watchdog = load_watchdog()
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "sentinel.json"
            path.write_text(
                json.dumps(
                    {
                        "schema": "monitoring_v4.k3s_host_sentinel.v6",
                        "checked_at": "2026-08-15T00:00:00Z",
                        "status": "good",
                        "detection_only": True,
                        "automatic_k3s_restart_enabled": False,
                        "automatic_runtime_mutation_enabled": False,
                    }
                ),
                encoding="utf-8",
            )
            checked_ts = watchdog.parse_utc("2026-08-15T00:00:00Z")
            assert checked_ts is not None
            with mock.patch.object(watchdog.time, "time", return_value=checked_ts + 120):
                result = watchdog.monitoring_v4_sentinel_check(path, 360.0)

            self.assertTrue(result.ok)
            self.assertEqual(result.name, "monitoring_v4_host_sentinel")
            self.assertEqual(result.repair, "")

            payload = json.loads(path.read_text(encoding="utf-8"))
            payload["automatic_runtime_mutation_enabled"] = True
            path.write_text(json.dumps(payload), encoding="utf-8")
            with mock.patch.object(watchdog.time, "time", return_value=checked_ts + 120):
                unsafe = watchdog.monitoring_v4_sentinel_check(path, 360.0)
            self.assertFalse(unsafe.ok)
            self.assertIn("contract_ok=false", unsafe.reason)

    def test_monitoring_v4_sentinel_rejects_stale_future_and_missing_evidence(self) -> None:
        watchdog = load_watchdog()
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "sentinel.json"
            base = {
                "schema": "monitoring_v4.k3s_host_sentinel.v6",
                "status": "good",
                "detection_only": True,
                "automatic_k3s_restart_enabled": False,
                "automatic_runtime_mutation_enabled": False,
            }
            checked_ts = watchdog.parse_utc("2026-08-15T00:00:00Z")
            assert checked_ts is not None
            with mock.patch.object(watchdog.time, "time", return_value=checked_ts + 600):
                self.assertFalse(
                    watchdog.monitoring_v4_sentinel_check(path, 360.0).ok
                )

                path.write_text(
                    json.dumps({**base, "checked_at": "2026-08-15T00:00:00Z"}),
                    encoding="utf-8",
                )
                stale = watchdog.monitoring_v4_sentinel_check(path, 360.0)
                self.assertFalse(stale.ok)
                self.assertIn("age=600.0", stale.reason)

                path.write_text(
                    json.dumps({**base, "checked_at": "2026-08-15T00:12:00Z"}),
                    encoding="utf-8",
                )
                future = watchdog.monitoring_v4_sentinel_check(path, 360.0)
                self.assertFalse(future.ok)
                self.assertIn("future checked_at", future.reason)

    def test_update_state_repairs_after_threshold_and_resets_fail_count(self) -> None:
        watchdog = load_watchdog()
        with tempfile.TemporaryDirectory() as td:
            compose_file = Path(td) / "docker-compose.yml"
            check = watchdog.Check("stream_v3_exporter_up", False, "value=0", "systemd:adsb-streamnew-prometheus-exporter.service")
            state = {
                "checks": {
                    "stream_v3_exporter_up": {
                        "fail_count": 2,
                        "last_repair_ts": 0,
                        "last_repair_utc": "",
                    }
                }
            }

            with (
                mock.patch.object(watchdog, "time") as time_mock,
                mock.patch.object(watchdog, "repair", return_value=(True, "restarted")) as repair_mock,
            ):
                time_mock.time.return_value = 1000.0
                time_mock.strftime.side_effect = lambda fmt, tm: "2099-01-01T00:00:00Z"
                time_mock.gmtime.return_value = object()
                updated = watchdog.update_state(
                    state,
                    [check],
                    repair_enabled=True,
                    threshold=3,
                    cooldown_sec=600,
                    compose_file=compose_file,
                )

        repair_mock.assert_called_once_with("systemd:adsb-streamnew-prometheus-exporter.service", compose_file)
        entry = updated["checks"]["stream_v3_exporter_up"]
        self.assertEqual(entry["fail_count"], 0)
        self.assertEqual(updated["repairs"][0]["action"], "systemd:adsb-streamnew-prometheus-exporter.service")

    def test_missing_series_reports_missing_but_not_false_values(self) -> None:
        watchdog = load_watchdog()

        def query_fn(query: str, *, timeout_sec: float):
            if "missing" in query:
                return True, [], ""
            return True, [{"value": [1, "0"]}], ""

        missing, errors = watchdog.missing_series(
            query_fn,
            {
                "healthy_zero_value": "stream_v3_same_url_live",
                "missing_value": "missing_metric",
            },
        )

        self.assertEqual(missing, ["missing_value"])
        self.assertEqual(errors, [])

    def test_repair_k3s_deployment_waits_for_rollout(self) -> None:
        watchdog = load_watchdog()
        calls: list[list[str]] = []

        def fake_run(command: list[str], *, timeout_sec: float):
            calls.append(command)
            return mock.Mock(returncode=0, stdout="ok", stderr="")

        with mock.patch.object(watchdog, "run", side_effect=fake_run):
            ok, detail = watchdog.repair("k3s:arena-monitoring:arena-monitoring-grafana", Path("/unused"))

        self.assertTrue(ok)
        self.assertEqual(detail, "ok")
        self.assertEqual(
            calls[0],
            [
                "k3s",
                "kubectl",
                "-n",
                "arena-monitoring",
                "rollout",
                "restart",
                "deployment",
                "arena-monitoring-grafana",
            ],
        )
        self.assertEqual(calls[1][:7], ["k3s", "kubectl", "-n", "arena-monitoring", "rollout", "status", "deployment"])


if __name__ == "__main__":
    unittest.main()
