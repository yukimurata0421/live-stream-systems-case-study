from __future__ import annotations

import importlib.util
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock


def load_exporter():
    path = Path(__file__).resolve().parents[1] / "ops" / "scripts" / "stream_v3_prometheus_exporter.py"
    spec = importlib.util.spec_from_file_location("stream_v3_prometheus_exporter", path)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def metric_value(payload: str, name: str, label_text: str = "") -> float:
    prefix = f"{name}{label_text} "
    for line in payload.splitlines():
        if line.startswith(prefix):
            return float(line.rsplit(" ", 1)[1])
    raise AssertionError(f"metric not found: {prefix}")


class StreamV3PrometheusExporterTests(unittest.TestCase):
    def test_default_repo_root_is_derived_from_public_checkout(self) -> None:
        exporter = load_exporter()

        self.assertTrue(str(exporter.DEFAULT_REPO_ROOT).endswith("stream_v3"))
        self.assertNotIn("/home/yuki/projects/stream_v3", str(exporter.DEFAULT_REPO_ROOT))
        self.assertEqual(exporter.DEFAULT_STATE_ROOT, exporter.DEFAULT_REPO_ROOT / ".state" / "observability-monitor")

    def test_run_json_passes_v3_state_environment_to_cli(self) -> None:
        exporter = load_exporter()
        repo_root = Path("/tmp/stream-v3")
        state_root = Path("/tmp/stream-v3/.state/observability-monitor")

        with mock.patch.object(exporter.subprocess, "run") as run_mock:
            run_mock.return_value = mock.Mock(returncode=0, stdout='{"ok": true}', stderr="")
            payload = exporter.run_json(repo_root, state_root, ["/bin/true"], timeout_sec=3)

        self.assertEqual(payload, {"ok": True})
        env = run_mock.call_args.kwargs["env"]
        self.assertEqual(env["STREAM_BASE_DIR"], str(repo_root))
        self.assertEqual(env["STREAM_RUNTIME_STATE_DIR"], str(state_root))
        self.assertEqual(env["STREAM_RUNTIME_LOG_DIR"], str(state_root / "logs"))
        self.assertEqual(env["STREAM_V3_STATE_ROOT"], str(state_root))
        self.assertEqual(env["PYTHONPATH"], str(repo_root / "src"))

    def test_missing_network_and_memory_snapshots_use_live_v3_evidence_fallbacks(self) -> None:
        exporter = load_exporter()
        with tempfile.TemporaryDirectory() as td:
            state_root = Path(td)
            logs = state_root / "logs"
            logs.mkdir(parents=True)
            (state_root / "youtube_watchdog_stats.json").write_text(
                json.dumps({"healthy": True, "ingest_connected": True}),
                encoding="utf-8",
            )
            (state_root / "stream_watchdog_stats.json").write_text(
                json.dumps({"status": "ok"}),
                encoding="utf-8",
            )
            (state_root / "subsystems_status.json").write_text(
                json.dumps({"overall": {"state": "healthy", "stream_public_state": "same_url_live"}}),
                encoding="utf-8",
            )
            (logs / "fast_recovery_events.jsonl").write_text(
                "\n".join(
                    [
                        json.dumps(
                            {
                                "ts_utc": "2099-01-01T00:00:00Z",
                                "kind": "tcp_send_sample",
                                "sample_interval_sec": 60,
                                "mbps": 4.5,
                                "notsent": 0,
                                "unacked": 0,
                                "lastsnd_ms": 10,
                            }
                        ),
                        json.dumps(
                            {
                                "ts_utc": "2099-01-01T00:01:00Z",
                                "kind": "tcp_send_sample",
                                "sample_interval_sec": 60,
                                "mbps": 5.2,
                                "notsent": 0,
                                "unacked": 0,
                                "lastsnd_ms": 12,
                            }
                        ),
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            health = {
                "windows": [
                    {
                        "hours": 1,
                        "observe": {
                            "pass": True,
                            "checks": {"current_fail": False},
                            "ffmpeg_tcp_send_mbps_24h_p95": None,
                            "ffmpeg_tcp_send_mbps_24h_max": None,
                            "ffmpeg_tcp_send_mbps_24h_over_budget_duration_sec": None,
                        },
                    }
                ]
            }
            objective = {"metrics": {}}

            with (
                mock.patch.object(exporter, "run_json", side_effect=[health, objective]),
                mock.patch.object(exporter, "time") as time_mock,
                mock.patch.object(
                    exporter,
                    "host_memory_snapshot",
                    return_value={"mem_available_mb": 4096.0, "mem_available_ratio": 0.50},
                ),
            ):
                time_mock.time.return_value = 4070908860.0
                payload = exporter.build_metrics(repo_root=Path(td), state_root=state_root, timeout_sec=1)

        self.assertEqual(metric_value(payload, "stream_v3_health_pass", '{window_hours="1"}'), 1.0)
        self.assertEqual(metric_value(payload, "stream_v3_current_fail", '{window_hours="1"}'), 0.0)
        self.assertEqual(metric_value(payload, "stream_v3_network_ok"), 1.0)
        self.assertEqual(metric_value(payload, "stream_v3_memory_current_ok"), 1.0)
        self.assertEqual(metric_value(payload, "stream_v3_upload_p95_mbps", '{window_hours="1"}'), 5.2)
        self.assertNotIn("stream_v2_health_pass", payload)

    def test_network_socket_uses_remote_tcp_sample_when_observer_is_local_only(self) -> None:
        exporter = load_exporter()
        with tempfile.TemporaryDirectory() as td:
            state_root = Path(td)
            logs = state_root / "logs"
            logs.mkdir(parents=True)
            (state_root / "youtube_watchdog_stats.json").write_text(
                json.dumps({"healthy": True, "ingest_connected": True}),
                encoding="utf-8",
            )
            (state_root / "stream_watchdog_stats.json").write_text(
                json.dumps({"status": "ok"}),
                encoding="utf-8",
            )
            (state_root / "network_observer_latest.json").write_text(
                json.dumps(
                    {
                        "ts_utc": "2099-01-01T00:01:30Z",
                        "classification": {"status": "ok"},
                        "ffmpeg_socket": {"connected": False, "notsent": 0, "unacked": 0, "lastsnd_ms": 0},
                    }
                ),
                encoding="utf-8",
            )
            (logs / "fast_recovery_events.jsonl").write_text(
                json.dumps(
                    {
                        "ts_utc": "2099-01-01T00:01:00Z",
                        "kind": "tcp_send_sample",
                        "sample_interval_sec": 60,
                        "mbps": 3.05,
                        "notsent": 42,
                        "unacked": 7,
                        "lastsnd_ms": 11,
                    }
                )
                + "\n",
                encoding="utf-8",
            )

            with (
                mock.patch.object(exporter, "run_json", side_effect=[{"windows": []}, {"metrics": {}}]),
                mock.patch.object(exporter, "time") as time_mock,
                mock.patch.object(exporter, "host_memory_snapshot", return_value={}),
            ):
                time_mock.time.return_value = 4070908860.0
                payload = exporter.build_metrics(repo_root=Path(td), state_root=state_root, timeout_sec=1)

        self.assertEqual(metric_value(payload, "stream_v3_network_ffmpeg_socket_connected"), 1.0)
        self.assertEqual(metric_value(payload, "stream_v3_network_ffmpeg_socket_notsent_bytes"), 42.0)
        self.assertEqual(metric_value(payload, "stream_v3_network_ffmpeg_socket_unacked"), 7.0)
        self.assertEqual(metric_value(payload, "stream_v3_network_ffmpeg_socket_lastsnd_ms"), 11.0)
        self.assertEqual(metric_value(payload, "stream_v3_upload_latest_mbps"), 3.05)

    def test_upload_windows_prefer_windowed_tcp_samples_over_24h_health_fields(self) -> None:
        exporter = load_exporter()
        with tempfile.TemporaryDirectory() as td:
            state_root = Path(td)
            logs = state_root / "logs"
            logs.mkdir(parents=True)
            (logs / "fast_recovery_events.jsonl").write_text(
                "\n".join(
                    [
                        json.dumps(
                            {
                                "ts_utc": "2099-01-01T00:00:00Z",
                                "kind": "tcp_send_sample",
                                "sample_interval_sec": 60,
                                "mbps": 3.1,
                            }
                        ),
                        json.dumps(
                            {
                                "ts_utc": "2099-01-01T00:01:00Z",
                                "kind": "tcp_send_sample",
                                "sample_interval_sec": 60,
                                "mbps": 3.2,
                            }
                        ),
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            health = {
                "windows": [
                    {
                        "hours": 1,
                        "observe": {
                            "pass": True,
                            "checks": {"current_fail": False},
                            "ffmpeg_tcp_send_mbps_24h_p95": 4.94,
                            "ffmpeg_tcp_send_mbps_24h_max": 5.4,
                            "ffmpeg_tcp_send_mbps_24h_over_budget_duration_sec": 300,
                        },
                    }
                ]
            }

            with (
                mock.patch.object(exporter, "run_json", side_effect=[health, {"metrics": {}}]),
                mock.patch.object(exporter, "time") as time_mock,
                mock.patch.object(exporter, "host_memory_snapshot", return_value={}),
            ):
                time_mock.time.return_value = 4070908920.0
                payload = exporter.build_metrics(repo_root=Path(td), state_root=state_root, timeout_sec=1)

        self.assertEqual(metric_value(payload, "stream_v3_upload_p95_mbps", '{window_hours="1"}'), 3.2)
        self.assertEqual(metric_value(payload, "stream_v3_upload_max_mbps", '{window_hours="1"}'), 3.2)
        self.assertEqual(metric_value(payload, "stream_v3_upload_over_budget_seconds", '{window_hours="1"}'), 0.0)

    def test_runtime_memory_metrics_target_stream_v3_runtime_pod(self) -> None:
        exporter = load_exporter()
        deployment_json = {
            "spec": {
                "template": {
                    "spec": {
                        "containers": [
                            {
                                "name": "stream-engine",
                                "resources": {
                                    "requests": {"memory": "2Gi"},
                                    "limits": {"memory": "6Gi"},
                                },
                            },
                            {
                                "name": "auto-dj",
                                "resources": {
                                    "requests": {"memory": "256Mi"},
                                    "limits": {"memory": "1Gi"},
                                },
                            },
                        ]
                    }
                }
            }
        }
        metrics_json = {
            "items": [
                {
                    "metadata": {"name": "stream-v3-runtime-abc123"},
                    "timestamp": "2099-01-01T00:00:00Z",
                    "containers": [
                        {"name": "stream-engine", "usage": {"memory": "3145728Ki"}},
                        {"name": "auto-dj", "usage": {"memory": "128Mi"}},
                    ],
                }
            ]
        }

        with (
            mock.patch.object(exporter, "kubectl_json", side_effect=[deployment_json, metrics_json]),
            mock.patch.object(exporter, "time") as time_mock,
        ):
            time_mock.time.return_value = 4070908860.0
            snapshot = exporter.runtime_memory_snapshot(timeout_sec=1, now=4070908860.0)

        self.assertTrue(snapshot["available"])
        self.assertTrue(snapshot["current_ok"])
        self.assertEqual(len(snapshot["containers"]), 2)
        stream_engine = [item for item in snapshot["containers"] if item["container"] == "stream-engine"][0]
        self.assertEqual(stream_engine["pod"], "stream-v3-runtime-abc123")
        self.assertEqual(stream_engine["current_mib"], 3072.0)
        self.assertEqual(stream_engine["limit_mib"], 6144.0)
        self.assertEqual(stream_engine["usage_ratio"], 0.5)

    def test_build_metrics_exports_runtime_memory_guardrail_separate_from_host_guardrail(self) -> None:
        exporter = load_exporter()
        runtime_memory = {
            "available": True,
            "current_ok": True,
            "sample_age_seconds": 15.0,
            "warning_ratio": 0.85,
            "containers": [
                {
                    "namespace": "stream-v3",
                    "pod": "stream-v3-runtime-abc123",
                    "container": "stream-engine",
                    "current_mib": 3072.0,
                    "limit_mib": 6144.0,
                    "request_mib": 2048.0,
                    "usage_ratio": 0.5,
                    "over_warning": False,
                }
            ],
        }
        with tempfile.TemporaryDirectory() as td:
            state_root = Path(td)
            with (
                mock.patch.object(exporter, "run_json", side_effect=[{"windows": []}, {"metrics": {}}]),
                mock.patch.object(exporter, "host_memory_snapshot", return_value={}),
                mock.patch.object(exporter, "runtime_memory_snapshot", return_value=runtime_memory),
            ):
                payload = exporter.build_metrics(repo_root=Path(td), state_root=state_root, timeout_sec=1)

        self.assertEqual(metric_value(payload, "stream_v3_runtime_memory_current_ok"), 1.0)
        self.assertEqual(metric_value(payload, "stream_v3_runtime_memory_sample_available"), 1.0)
        self.assertEqual(metric_value(payload, "stream_v3_runtime_memory_current_mib", '{container="stream-engine",namespace="stream-v3",pod="stream-v3-runtime-abc123"}'), 3072.0)
        self.assertEqual(metric_value(payload, "stream_v3_runtime_memory_usage_ratio", '{container="stream-engine",namespace="stream-v3",pod="stream-v3-runtime-abc123"}'), 0.5)

    def test_build_metrics_uses_snapshots_when_live_cli_collection_fails(self) -> None:
        exporter = load_exporter()
        with tempfile.TemporaryDirectory() as td:
            state_root = Path(td)
            (state_root / "health_summary_snapshot.json").write_text(
                json.dumps(
                    {
                        "windows": [
                            {
                                "hours": 1,
                                "observe": {"pass": True, "checks": {"current_fail": False}},
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )
            (state_root / "objective_sli_snapshot.json").write_text(json.dumps({"metrics": {}}), encoding="utf-8")

            with (
                mock.patch.object(exporter, "run_json", side_effect=RuntimeError("timeout")),
                mock.patch.object(exporter, "host_memory_snapshot", return_value={}),
                mock.patch.object(exporter, "runtime_memory_snapshot", return_value={"containers": []}),
            ):
                payload = exporter.build_metrics(repo_root=Path(td), state_root=state_root, timeout_sec=1)

        self.assertEqual(metric_value(payload, "stream_v3_exporter_up"), 1.0)
        self.assertEqual(metric_value(payload, "stream_v3_exporter_snapshot_fallback"), 1.0)
        self.assertEqual(metric_value(payload, "stream_v3_exporter_health_summary_snapshot_used"), 1.0)
        self.assertEqual(metric_value(payload, "stream_v3_health_pass", '{window_hours="1"}'), 1.0)

    def test_cache_serves_last_good_payload_when_refresh_fails(self) -> None:
        exporter = load_exporter()
        cache = exporter.MetricsCache(repo_root=Path("/tmp/repo"), state_root=Path("/tmp/state"), ttl_sec=0, timeout_sec=1)
        first_payload = "\n".join(
            [
                "# HELP stream_v3_exporter_up Exporter scrape success.",
                "# TYPE stream_v3_exporter_up gauge",
                "stream_v3_exporter_up 1.0",
                "# HELP stream_v3_health_pass Health summary pass by window.",
                "# TYPE stream_v3_health_pass gauge",
                'stream_v3_health_pass{window_hours="1"} 1.0',
            ]
        ) + "\n"

        with mock.patch.object(exporter, "build_metrics", side_effect=[first_payload, RuntimeError("refresh failed")]):
            payload1, error1 = cache.get()
            payload2, error2 = cache.get()

        self.assertEqual(error1, "")
        self.assertIn('stream_v3_health_pass{window_hours="1"} 1.0', payload1)
        self.assertIn('stream_v3_health_pass{window_hours="1"} 1.0', payload2)
        self.assertEqual(metric_value(payload2, "stream_v3_exporter_up"), 0.0)
        self.assertEqual(metric_value(payload2, "stream_v3_exporter_last_good_payload"), 1.0)
        self.assertIn("refresh failed", error2)

    def test_build_metrics_exports_monitoring_watchdog_state(self) -> None:
        exporter = load_exporter()
        with tempfile.TemporaryDirectory() as td:
            state_root = Path(td)
            (state_root / "monitoring_watchdog_state.json").write_text(
                json.dumps(
                    {
                        "ts_utc": "2099-01-01T00:00:00Z",
                        "ok": True,
                        "repair_enabled": False,
                        "repair_attempted": False,
                        "repair_count": 0,
                        "checks": {
                            "exporter_http": {"ok": True},
                            "metrics_contract": {"ok": True},
                        },
                    }
                ),
                encoding="utf-8",
            )
            with (
                mock.patch.object(exporter, "run_json", side_effect=[{"windows": []}, {"metrics": {}}]),
                mock.patch.object(exporter, "time") as time_mock,
                mock.patch.object(exporter, "host_memory_snapshot", return_value={}),
                mock.patch.object(exporter, "runtime_memory_snapshot", return_value={"containers": []}),
            ):
                time_mock.time.return_value = 4070908860.0
                payload = exporter.build_metrics(repo_root=Path(td), state_root=state_root, timeout_sec=1)

        self.assertEqual(metric_value(payload, "stream_v3_monitoring_watchdog_ok"), 1.0)
        self.assertEqual(metric_value(payload, "stream_v3_monitoring_watchdog_repair_enabled"), 0.0)
        self.assertEqual(metric_value(payload, "stream_v3_monitoring_watchdog_check_ok", '{check="exporter_http"}'), 1.0)


if __name__ == "__main__":
    unittest.main()
