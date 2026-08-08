from __future__ import annotations

import importlib.util
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock


def load_probe():
    path = Path(__file__).resolve().parents[1] / "ops" / "scripts" / "stream_v3_map_runtime_probe.py"
    spec = importlib.util.spec_from_file_location("stream_v3_map_runtime_probe", path)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


NOW = 1_785_831_000.0


def healthy_sample() -> dict:
    containers = {
        name: {"ready": True, "restart_count": 0, "state": "running"}
        for name in (
            "stream-engine",
            "precipitation-fetcher",
            "auto-dj",
            "fast-recovery-loop",
        )
    }
    return {
        "checked_at_utc": "2026-08-04T08:10:00Z",
        "probe_errors": [],
        "deployment": {
            "generation": 290,
            "observed_generation": 290,
            "desired_replicas": 1,
            "ready_replicas": 1,
            "available_replicas": 1,
            "containers": {name: {"image": "stream-v3:test"} for name in containers},
        },
        "pods": [
            {
                "name": "stream-v3-runtime-test",
                "uid": "pod-uid",
                "phase": "Running",
                "containers": containers,
            }
        ],
        "readiness": {
            "ready": True,
            "gpu_ok": True,
            "nvenc_active": True,
            "rtmp_socket_established": True,
            "ffmpeg_pid": 614,
        },
        "process": {
            "browser": {
                "main_process_count": 1,
                "swiftshader_main_process_count": 1,
                "adsb_map_main_process_count": 1,
            },
            "browser_log": {
                "present": True,
                "webgl2_blocklisted": False,
                "context_fatal_failure": False,
            },
            "render": {
                "payload": {
                    "ready": True,
                    "state": "ready",
                    "age_sec": 4.2,
                    "map_tiles_ready": True,
                    "aircraft_sample_ready": True,
                },
                "error": "",
            },
            "weather_status": {
                "payload": {
                    "available": True,
                    "analysis_only": True,
                    "processed": True,
                    "forecast_minutes": 0,
                    "observed_at_utc": "2026-08-04T08:05:00Z",
                    "stale_after_sec": 900,
                },
                "error": "",
            },
            "weather_health": {
                "payload": {"success": True, "state": "current", "consecutive_failures": 0},
                "error": "",
            },
        },
    }


class StreamV3MapRuntimeProbeTests(unittest.TestCase):
    def test_default_paths_are_derived_from_public_checkout(self) -> None:
        probe = load_probe()
        expected_repo_root = Path(__file__).resolve().parents[1]

        self.assertEqual(probe.DEFAULT_REPO_ROOT, expected_repo_root)
        self.assertEqual(
            probe.DEFAULT_STATE_ROOT,
            expected_repo_root / ".state" / "observability-monitor",
        )
        self.assertNotIn("/home/yuki/projects/stream_v3", str(probe.DEFAULT_REPO_ROOT))

    def test_healthy_sample_passes_delivery_and_weather_contracts(self) -> None:
        probe = load_probe()

        payload = probe.evaluate_sample(healthy_sample(), now_epoch=NOW)

        self.assertEqual(payload["status"], "healthy")
        self.assertTrue(payload["delivery_critical_ok"])
        self.assertTrue(payload["weather_ok"])
        self.assertTrue(payload["conditions"]["render_heartbeat"])
        self.assertTrue(payload["browser"]["contract_ok"])

    def test_weather_failure_degrades_without_failing_delivery(self) -> None:
        probe = load_probe()
        sample = healthy_sample()
        sample["process"]["weather_health"]["payload"] = {
            "success": False,
            "state": "retrying",
            "consecutive_failures": 2,
        }

        payload = probe.evaluate_sample(sample, now_epoch=NOW)

        self.assertEqual(payload["status"], "degraded")
        self.assertTrue(payload["delivery_critical_ok"])
        self.assertFalse(payload["weather_ok"])
        self.assertEqual(payload["critical_reasons"], [])

    def test_expired_render_heartbeat_is_delivery_failure(self) -> None:
        probe = load_probe()
        sample = healthy_sample()
        sample["process"]["render"]["payload"]["age_sec"] = 31.0

        payload = probe.evaluate_sample(sample, now_epoch=NOW)

        self.assertEqual(payload["status"], "failed")
        self.assertFalse(payload["delivery_critical_ok"])
        self.assertIn("render_heartbeat", payload["critical_reasons"])

    def test_missing_expected_container_is_delivery_failure(self) -> None:
        probe = load_probe()
        sample = healthy_sample()
        del sample["pods"][0]["containers"]["precipitation-fetcher"]

        payload = probe.evaluate_sample(sample, now_epoch=NOW)

        self.assertEqual(payload["status"], "failed")
        self.assertIn("pod_topology_ready", payload["critical_reasons"])

    def test_each_delivery_critical_dependency_fails_closed(self) -> None:
        probe = load_probe()
        cases = (
            (
                "deployment generation",
                "deployment_ready",
                lambda sample: sample["deployment"].__setitem__("observed_generation", 289),
            ),
            (
                "multiple runtime pods",
                "pod_topology_ready",
                lambda sample: sample["pods"].append({**sample["pods"][0], "uid": "second-pod"}),
            ),
            (
                "pod phase",
                "pod_topology_ready",
                lambda sample: sample["pods"][0].__setitem__("phase", "Pending"),
            ),
            (
                "runtime ready",
                "runtime_readiness",
                lambda sample: sample["readiness"].__setitem__("ready", False),
            ),
            (
                "gpu readiness",
                "runtime_readiness",
                lambda sample: sample["readiness"].__setitem__("gpu_ok", False),
            ),
            (
                "nvenc active",
                "runtime_readiness",
                lambda sample: sample["readiness"].__setitem__("nvenc_active", False),
            ),
            (
                "rtmp socket",
                "runtime_readiness",
                lambda sample: sample["readiness"].__setitem__("rtmp_socket_established", False),
            ),
            (
                "map tiles",
                "render_heartbeat",
                lambda sample: sample["process"]["render"]["payload"].__setitem__("map_tiles_ready", False),
            ),
            (
                "aircraft sample",
                "render_heartbeat",
                lambda sample: sample["process"]["render"]["payload"].__setitem__("aircraft_sample_ready", False),
            ),
            (
                "render probe error",
                "render_heartbeat",
                lambda sample: sample["process"]["render"].__setitem__("error", "timeout"),
            ),
            (
                "browser process",
                "browser_contract",
                lambda sample: sample["process"]["browser"].__setitem__("main_process_count", 0),
            ),
            (
                "swiftshader process",
                "browser_contract",
                lambda sample: sample["process"]["browser"].__setitem__("swiftshader_main_process_count", 0),
            ),
            (
                "adsb map process",
                "browser_contract",
                lambda sample: sample["process"]["browser"].__setitem__("adsb_map_main_process_count", 0),
            ),
            (
                "browser log",
                "browser_contract",
                lambda sample: sample["process"]["browser_log"].__setitem__("present", False),
            ),
            (
                "webgl blocklist",
                "browser_contract",
                lambda sample: sample["process"]["browser_log"].__setitem__("webgl2_blocklisted", True),
            ),
            (
                "webgl fatal context",
                "browser_contract",
                lambda sample: sample["process"]["browser_log"].__setitem__("context_fatal_failure", True),
            ),
        )

        for label, expected_reason, mutate in cases:
            with self.subTest(label=label):
                sample = healthy_sample()
                mutate(sample)
                payload = probe.evaluate_sample(sample, now_epoch=NOW)
                self.assertEqual(payload["status"], "failed")
                self.assertFalse(payload["delivery_critical_ok"])
                self.assertEqual(payload["critical_reasons"], [expected_reason])

    def test_container_restart_count_is_preserved_without_failing_ready_pod(self) -> None:
        probe = load_probe()
        sample = healthy_sample()
        sample["pods"][0]["containers"]["stream-engine"]["restart_count"] = 2

        payload = probe.evaluate_sample(sample, now_epoch=NOW)
        history = probe.history_row(payload)

        self.assertEqual(payload["status"], "healthy")
        self.assertTrue(payload["delivery_critical_ok"])
        self.assertEqual(history["container_restart_counts"]["stream-engine"], 2)

    def test_stale_precipitation_degrades_weather_only(self) -> None:
        probe = load_probe()
        sample = healthy_sample()
        sample["process"]["weather_status"]["payload"]["observed_at_utc"] = "2026-08-04T07:00:00Z"

        payload = probe.evaluate_sample(sample, now_epoch=NOW)

        self.assertEqual(payload["status"], "degraded")
        self.assertTrue(payload["delivery_critical_ok"])
        self.assertFalse(payload["weather_ok"])
        self.assertEqual(payload["critical_reasons"], [])
        self.assertEqual(payload["weather_reasons"], ["precipitation_status"])
        self.assertGreater(
            payload["precipitation"]["observed_age_sec"],
            payload["precipitation"]["stale_after_sec"],
        )

    def test_discovery_error_is_unknown_and_persisted(self) -> None:
        probe = load_probe()
        sample = healthy_sample()
        sample["deployment"] = {}
        sample["probe_errors"] = ["deployment:TimeoutExpired:kubectl"]
        with tempfile.TemporaryDirectory() as td:
            state_file = Path(td) / "status.json"
            history_file = Path(td) / "history.jsonl"
            with (
                mock.patch.object(probe, "collect_sample", return_value=sample),
                mock.patch.object(probe.time, "time", return_value=NOW),
            ):
                rc = probe.main(
                    [
                        "--state-file",
                        str(state_file),
                        "--history-file",
                        str(history_file),
                    ]
                )
            state = json.loads(state_file.read_text(encoding="utf-8"))
            history = json.loads(history_file.read_text(encoding="utf-8").splitlines()[0])

        self.assertEqual(rc, 0)
        self.assertEqual(state["status"], "unknown")
        self.assertEqual(history["status"], "unknown")
        self.assertEqual(history["pod_uid"], "pod-uid")


if __name__ == "__main__":
    unittest.main()
