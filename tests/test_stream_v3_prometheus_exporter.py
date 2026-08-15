from __future__ import annotations

import importlib.util
import json
import subprocess
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
    def test_run_json_passes_v3_state_environment_to_cli(self) -> None:
        exporter = load_exporter()
        repo_root = Path("/tmp/stream-v3")
        state_root = Path("/tmp/stream-v3/.state/arena-monitor")

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

    def test_exports_oauth_input_quality_without_reusing_watchdog_warn_count(self) -> None:
        exporter = load_exporter()
        with tempfile.TemporaryDirectory() as td:
            state_root = Path(td)
            (state_root / "logs").mkdir(parents=True)
            (state_root / "youtube_watchdog_stats.json").write_text(
                json.dumps(
                    {
                        "ts_utc": "2026-08-10T00:00:00Z",
                        "oauth_checked_ts_utc": "2026-08-10T00:00:00Z",
                        "oauth_probe_ok": True,
                        "oauth_stream_status": "active",
                        "oauth_stream_health_status": "ok",
                        "oauth_stream_health_issues": 1,
                        "oauth_stream_health_issue_details": [
                            {"type": "bitrateLow", "severity": "warning"}
                        ],
                        "ingest_connected": True,
                        "healthy": True,
                    }
                ),
                encoding="utf-8",
            )

            with (
                mock.patch.object(exporter, "run_json", side_effect=[{"windows": []}, {"metrics": {}}]),
                mock.patch.object(exporter, "time") as time_mock,
                mock.patch.object(exporter, "host_memory_snapshot", return_value={}),
                mock.patch.object(exporter, "runtime_gpu_snapshot", return_value={}),
            ):
                time_mock.time.return_value = 1_786_320_060.0
                payload = exporter.build_metrics(repo_root=Path(td), state_root=state_root, timeout_sec=1)

        self.assertEqual(metric_value(payload, "stream_v3_youtube_input_quality_probe_fresh"), 1.0)
        self.assertEqual(metric_value(payload, "stream_v3_youtube_input_quality_eligible"), 1.0)
        self.assertEqual(metric_value(payload, "stream_v3_youtube_input_quality_good"), 0.0)
        self.assertEqual(metric_value(payload, "stream_v3_youtube_input_quality_issue_count"), 1.0)
        self.assertEqual(
            metric_value(payload, "stream_v3_youtube_input_quality_warning_or_error_issue_count"),
            1.0,
        )
        self.assertEqual(
            metric_value(
                payload,
                "stream_v3_youtube_input_quality_state",
                '{classification="bad_health_warning",health_status="ok"}',
            ),
            1.0,
        )

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
                mock.patch.object(exporter, "runtime_gpu_snapshot", return_value={}),
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
                mock.patch.object(exporter, "runtime_gpu_snapshot", return_value={}),
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
                mock.patch.object(exporter, "runtime_gpu_snapshot", return_value={}),
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
                mock.patch.object(exporter, "runtime_gpu_snapshot", return_value={}),
            ):
                payload = exporter.build_metrics(repo_root=Path(td), state_root=state_root, timeout_sec=1)

        self.assertEqual(metric_value(payload, "stream_v3_runtime_memory_current_ok"), 1.0)
        self.assertEqual(metric_value(payload, "stream_v3_runtime_memory_sample_available"), 1.0)
        self.assertEqual(metric_value(payload, "stream_v3_runtime_memory_current_mib", '{container="stream-engine",namespace="stream-v3",pod="stream-v3-runtime-abc123"}'), 3072.0)
        self.assertEqual(metric_value(payload, "stream_v3_runtime_memory_usage_ratio", '{container="stream-engine",namespace="stream-v3",pod="stream-v3-runtime-abc123"}'), 0.5)

    def test_build_metrics_separates_swap_capacity_from_current_pressure(self) -> None:
        exporter = load_exporter()
        with tempfile.TemporaryDirectory() as td:
            state_root = Path(td)
            (state_root / "memory_status.json").write_text(
                json.dumps(
                    {
                        "overall": {"severity": "ok"},
                        "host": {"swap_capacity": {"severity": "warn"}},
                    }
                ),
                encoding="utf-8",
            )
            (state_root / "resource_memory.json").write_text(
                json.dumps(
                    {
                        "ts_utc": "2099-01-01T00:01:00Z",
                        "assessment": {
                            "status": "observe",
                            "baseline_ready": True,
                            "baseline_coverage_sec": 604800,
                        },
                    }
                ),
                encoding="utf-8",
            )
            with (
                mock.patch.object(exporter, "run_json", side_effect=[{"windows": []}, {"metrics": {}}]),
                mock.patch.object(exporter, "host_memory_snapshot", return_value={}),
                mock.patch.object(exporter, "runtime_memory_snapshot", return_value={}),
                mock.patch.object(exporter, "runtime_gpu_snapshot", return_value={}),
                mock.patch.object(exporter, "time") as time_mock,
            ):
                time_mock.time.return_value = 4070908860.0
                payload = exporter.build_metrics(repo_root=Path(td), state_root=state_root, timeout_sec=1)

        self.assertEqual(metric_value(payload, "stream_v3_memory_current_ok"), 1.0)
        self.assertEqual(metric_value(payload, "stream_v3_host_swap_capacity_level"), 2.0)
        self.assertEqual(
            metric_value(payload, "stream_v3_host_swap_capacity_status", '{status="warn"}'),
            1.0,
        )
        self.assertEqual(metric_value(payload, "stream_v3_resource_memory_baseline_ready"), 1.0)
        self.assertEqual(
            metric_value(payload, "stream_v3_resource_memory_assessment_status", '{status="observe"}'),
            1.0,
        )

    def test_runtime_gpu_snapshot_detects_nvml_driver_library_mismatch_from_pod_status(self) -> None:
        exporter = load_exporter()
        deployment_json = {
            "spec": {
                "template": {
                    "spec": {
                        "containers": [
                            {
                                "name": "stream-engine",
                                "resources": {"limits": {"nvidia.com/gpu": "1"}},
                            }
                        ]
                    }
                }
            }
        }
        pods_json = {
            "items": [
                {
                    "metadata": {"name": "stream-v3-runtime-abc123"},
                    "status": {
                        "phase": "Running",
                        "containerStatuses": [
                            {
                                "name": "stream-engine",
                                "ready": False,
                                "state": {
                                    "waiting": {
                                        "reason": "RunContainerError",
                                        "message": "Failed to initialize NVML: Driver/library version mismatch",
                                    }
                                },
                            }
                        ],
                    },
                }
            ]
        }

        with mock.patch.object(exporter, "kubectl_json", side_effect=[deployment_json, pods_json]):
            snapshot = exporter.runtime_gpu_snapshot(timeout_sec=1, now=4070908860.0)

        self.assertEqual(snapshot["status"], "driver_mismatch")
        self.assertTrue(snapshot["restart_blocked"])
        self.assertTrue(snapshot["driver_mismatch"])

    def test_build_metrics_exports_runtime_gpu_guardrail(self) -> None:
        exporter = load_exporter()
        runtime_gpu = {
            "available": True,
            "status": "driver_mismatch",
            "status_ok": False,
            "restart_blocked": True,
            "driver_mismatch": True,
            "gpu_runtime_error": True,
            "gpu_requested": True,
            "stream_engine_ready": False,
            "stream_engine_running": False,
            "container_waiting": True,
            "pod_count": 1,
            "pods": [
                {
                    "pod": "stream-v3-runtime-abc123",
                    "container": "stream-engine",
                    "state": "waiting",
                    "reason": "RunContainerError",
                }
            ],
        }
        with tempfile.TemporaryDirectory() as td:
            state_root = Path(td)
            with (
                mock.patch.object(exporter, "run_json", side_effect=[{"windows": []}, {"metrics": {}}]),
                mock.patch.object(exporter, "host_memory_snapshot", return_value={}),
                mock.patch.object(exporter, "runtime_memory_snapshot", return_value={}),
                mock.patch.object(exporter, "runtime_gpu_snapshot", return_value=runtime_gpu),
            ):
                payload = exporter.build_metrics(repo_root=Path(td), state_root=state_root, timeout_sec=1)

        self.assertEqual(metric_value(payload, "stream_v3_runtime_gpu_status_ok"), 0.0)
        self.assertEqual(metric_value(payload, "stream_v3_runtime_gpu_restart_blocked"), 1.0)
        self.assertEqual(metric_value(payload, "stream_v3_runtime_gpu_driver_mismatch"), 1.0)
        self.assertEqual(metric_value(payload, "stream_v3_runtime_gpu_pod_state", '{container="stream-engine",pod="stream-v3-runtime-abc123",reason="RunContainerError",state="waiting"}'), 1.0)

    def test_build_metrics_replaces_missing_legacy_watchdog_files_with_subsystem_evidence(self) -> None:
        exporter = load_exporter()
        with tempfile.TemporaryDirectory() as td:
            state_root = Path(td)
            (state_root / "subsystems_status.json").write_text(
                json.dumps(
                    {
                        "overall": {"state": "healthy", "stream_public_state": "same_url_live"},
                        "rendering": {
                            "state": "healthy",
                            "last_ok_ts_utc": "2099-01-01T00:00:30Z",
                            "aircraft_json_ok": True,
                            "aircraft_messages_moving": True,
                            "aircraft_positions_moving": True,
                            "stream1090_report_ok": True,
                            "upstream_stream1090_report_ok": True,
                        },
                        "music": {
                            "state": "healthy",
                            "last_ok_ts_utc": "2099-01-01T00:00:40Z",
                            "audio_fail_count": 0,
                            "pulse_source_missing_count": 0,
                        },
                        "local_delivery": {
                            "state": "healthy",
                            "runtime_age_sec": 17,
                            "evidence": ["ffmpeg_alive"],
                        },
                    }
                ),
                encoding="utf-8",
            )
            (state_root / "stream_runtime_state_remote.json").write_text(
                json.dumps({"status": "running", "updated_at_utc": "2099-01-01T00:00:50Z"}),
                encoding="utf-8",
            )
            (state_root / "watchdog").mkdir()
            (state_root / "watchdog" / "adsb_freshness_state.json").write_text(
                json.dumps(
                    {
                        "last_messages": 1234,
                        "last_change_ts": 4070908850,
                        "sample_ts": 4070908855,
                        "status": "ok",
                    }
                ),
                encoding="utf-8",
            )

            with (
                mock.patch.object(exporter, "run_json", side_effect=[{"windows": []}, {"metrics": {}}]),
                mock.patch.object(exporter, "host_memory_snapshot", return_value={}),
                mock.patch.object(exporter, "runtime_memory_snapshot", return_value={}),
                mock.patch.object(exporter, "runtime_gpu_snapshot", return_value={}),
                mock.patch.object(exporter, "time") as time_mock,
            ):
                time_mock.time.return_value = 4070908860.0
                payload = exporter.build_metrics(repo_root=Path(td), state_root=state_root, timeout_sec=1)

        self.assertEqual(metric_value(payload, "stream_v3_adsb_evidence_available"), 1.0)
        self.assertEqual(metric_value(payload, "stream_v3_adsb_rendering_ok"), 1.0)
        self.assertEqual(metric_value(payload, "stream_v3_adsb_evidence_age_seconds"), 10.0)
        self.assertEqual(metric_value(payload, "stream_v3_adsb_source_age_seconds"), 10.0)
        self.assertEqual(metric_value(payload, "stream_v3_adsb_source_sample_age_seconds"), 5.0)
        self.assertEqual(metric_value(payload, "stream_v3_adsb_rendering_evidence_age_seconds"), 30.0)
        self.assertEqual(metric_value(payload, "stream_v3_audio_evidence_available"), 1.0)
        self.assertEqual(metric_value(payload, "stream_v3_audio_ok"), 1.0)
        self.assertEqual(metric_value(payload, "stream_v3_audio_fault_count"), 0.0)
        self.assertEqual(metric_value(payload, "stream_v3_stream_watchdog_ffmpeg_count"), 1.0)
        self.assertEqual(metric_value(payload, "stream_v3_runtime_heartbeat_age_seconds"), 17.0)
        self.assertEqual(metric_value(payload, "stream_v3_slo_snapshot_available"), 0.0)
        self.assertNotIn("stream_v3_audio_dj_missing_count", payload)
        self.assertNotIn("stream_v3_slo_pulse_unavailable_count", payload)

    def test_build_metrics_does_not_use_rendering_heartbeat_as_adsb_source_age(self) -> None:
        exporter = load_exporter()
        with tempfile.TemporaryDirectory() as td:
            state_root = Path(td)
            (state_root / "subsystems_status.json").write_text(
                json.dumps(
                    {
                        "rendering": {
                            "state": "healthy",
                            "last_ok_ts_utc": "2099-01-01T00:00:30Z",
                            "aircraft_json_ok": True,
                            "aircraft_messages_moving": True,
                            "aircraft_positions_moving": True,
                            "stream1090_report_ok": True,
                            "upstream_stream1090_report_ok": True,
                        }
                    }
                ),
                encoding="utf-8",
            )
            with (
                mock.patch.object(exporter, "run_json", side_effect=[{"windows": []}, {"metrics": {}}]),
                mock.patch.object(exporter, "host_memory_snapshot", return_value={}),
                mock.patch.object(exporter, "runtime_memory_snapshot", return_value={}),
                mock.patch.object(exporter, "runtime_gpu_snapshot", return_value={}),
                mock.patch.object(exporter, "time") as time_mock,
            ):
                time_mock.time.return_value = 4070908860.0
                payload = exporter.build_metrics(repo_root=Path(td), state_root=state_root, timeout_sec=1)

        self.assertEqual(metric_value(payload, "stream_v3_adsb_evidence_available"), 0.0)
        self.assertEqual(metric_value(payload, "stream_v3_adsb_rendering_ok"), 0.0)
        self.assertNotIn("\nstream_v3_adsb_evidence_age_seconds ", f"\n{payload}")
        self.assertEqual(metric_value(payload, "stream_v3_adsb_rendering_evidence_age_seconds"), 30.0)

    def test_adsb_rendering_accepts_either_message_or_position_movement(self) -> None:
        exporter = load_exporter()
        for messages, positions, expected in (
            (True, False, 1.0),
            (False, True, 1.0),
            (False, False, 0.0),
        ):
            with self.subTest(messages=messages, positions=positions):
                with tempfile.TemporaryDirectory() as td:
                    state_root = Path(td)
                    (state_root / "subsystems_status.json").write_text(
                        json.dumps(
                            {
                                "rendering": {
                                    "state": "healthy",
                                    "last_ok_ts_utc": "2099-01-01T00:00:30Z",
                                    "aircraft_json_ok": True,
                                    "aircraft_messages_moving": messages,
                                    "aircraft_positions_moving": positions,
                                    "stream1090_report_ok": True,
                                    "upstream_stream1090_report_ok": True,
                                }
                            }
                        ),
                        encoding="utf-8",
                    )
                    (state_root / "watchdog").mkdir()
                    (state_root / "watchdog" / "adsb_freshness_state.json").write_text(
                        json.dumps(
                            {
                                "last_change_ts": 4070908850,
                                "sample_ts": 4070908855,
                                "status": "ok",
                            }
                        ),
                        encoding="utf-8",
                    )
                    with (
                        mock.patch.object(exporter, "run_json", side_effect=[{"windows": []}, {"metrics": {}}]),
                        mock.patch.object(exporter, "host_memory_snapshot", return_value={}),
                        mock.patch.object(exporter, "runtime_memory_snapshot", return_value={}),
                        mock.patch.object(exporter, "runtime_gpu_snapshot", return_value={}),
                        mock.patch.object(exporter, "time") as time_mock,
                    ):
                        time_mock.time.return_value = 4070908860.0
                        payload = exporter.build_metrics(
                            repo_root=Path(td), state_root=state_root, timeout_sec=1
                        )

                self.assertEqual(
                    metric_value(payload, "stream_v3_adsb_rendering_ok"), expected
                )

    def test_open_day_api_units_are_exported_once_without_window_duplication(self) -> None:
        exporter = load_exporter()
        health = {
            "windows": [
                {
                    "hours": 1,
                    "observe": {
                        "pass": True,
                        "checks": {"current_fail": False},
                        "api_cost_reports": {"open_day_latest": {"units": 111, "fresh": True, "effective_end_age_sec": 10}},
                    },
                },
                {
                    "hours": 24,
                    "observe": {
                        "pass": True,
                        "checks": {"current_fail": False},
                        "api_cost_reports": {"open_day_latest": {"units": 222, "fresh": True, "effective_end_age_sec": 20}},
                    },
                },
            ]
        }
        with tempfile.TemporaryDirectory() as td:
            state_root = Path(td)
            with (
                mock.patch.object(exporter, "run_json", side_effect=[health, {"metrics": {}}]),
                mock.patch.object(exporter, "host_memory_snapshot", return_value={}),
                mock.patch.object(exporter, "runtime_memory_snapshot", return_value={}),
                mock.patch.object(exporter, "runtime_gpu_snapshot", return_value={}),
            ):
                payload = exporter.build_metrics(repo_root=Path(td), state_root=state_root, timeout_sec=1)

        self.assertEqual(metric_value(payload, "stream_v3_youtube_api_open_day_units"), 222.0)
        self.assertEqual(metric_value(payload, "stream_v3_youtube_api_open_day_report_fresh"), 1.0)
        self.assertEqual(metric_value(payload, "stream_v3_api_open_day_units", '{window_hours="24"}'), 222.0)
        self.assertNotIn('stream_v3_api_open_day_units{window_hours="1"}', payload)

    def test_build_metrics_uses_fresh_health_snapshot_instead_of_live_scan(self) -> None:
        exporter = load_exporter()
        health = {
            "windows": [
                {
                    "hours": 24,
                    "observe": {
                        "pass": True,
                        "checks": {"current_fail": False},
                    },
                }
            ]
        }
        with tempfile.TemporaryDirectory() as td:
            state_root = Path(td)
            snapshot = state_root / "health_summary_snapshot.json"
            objective_snapshot = state_root / "objective_sli_snapshot.json"
            snapshot.write_text(
                json.dumps(
                    {
                        "updated_at_utc": "2099-01-01T00:00:00Z",
                        "payload": health,
                    }
                ),
                encoding="utf-8",
            )
            objective_snapshot.write_text(
                json.dumps(
                    {
                        "updated_at_utc": "2099-01-01T00:00:00Z",
                        "payload": {"metrics": {}},
                    }
                ),
                encoding="utf-8",
            )
            (state_root / "monitoring_watchdog_state.json").write_text(
                json.dumps(
                    {
                        "updated_at_utc": "2099-01-01T00:01:00Z",
                        "repair_enabled": True,
                        "repairs": [],
                        "checks": {
                            "stream_v3_metrics_contract_present": {
                                "ok": True,
                                "repair": "systemd:adsb-streamnew-prometheus-exporter.service",
                                "fail_count": 0,
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )
            with (
                mock.patch.object(exporter, "run_json", return_value={"metrics": {}}) as run_json_mock,
                mock.patch.object(exporter, "host_memory_snapshot", return_value={}),
                mock.patch.object(exporter, "runtime_memory_snapshot", return_value={}),
                mock.patch.object(exporter, "runtime_gpu_snapshot", return_value={}),
                mock.patch.object(exporter, "time") as time_mock,
            ):
                time_mock.time.return_value = 4070908860.0
                payload = exporter.build_metrics(
                    repo_root=Path(td),
                    state_root=state_root,
                    timeout_sec=1,
                    health_snapshot_file=snapshot,
                    max_health_snapshot_age_sec=600,
                    objective_snapshot_file=objective_snapshot,
                    max_objective_snapshot_age_sec=600,
                )

        self.assertEqual(run_json_mock.call_count, 0)
        self.assertEqual(metric_value(payload, "stream_v3_health_snapshot_used"), 1.0)
        self.assertEqual(metric_value(payload, "stream_v3_objective_snapshot_used"), 1.0)
        self.assertEqual(metric_value(payload, "stream_v3_health_pass", '{window_hours="24"}'), 1.0)
        self.assertEqual(metric_value(payload, "stream_v3_monitoring_watchdog_all_ok"), 1.0)
        self.assertEqual(
            metric_value(
                payload,
                "stream_v3_monitoring_watchdog_check_ok",
                '{check="stream_v3_metrics_contract_present",repair="systemd:adsb-streamnew-prometheus-exporter.service"}',
            ),
            1.0,
        )

    def test_cache_keeps_last_good_payload_when_refresh_fails(self) -> None:
        exporter = load_exporter()
        good_payload = "\n".join(
            [
                "# HELP stream_v3_exporter_up Exporter scrape success.",
                "# TYPE stream_v3_exporter_up gauge",
                "stream_v3_exporter_up 1.0",
                "# HELP stream_v3_health_pass Health summary pass by window.",
                "# TYPE stream_v3_health_pass gauge",
                'stream_v3_health_pass{window_hours="24"} 1.0',
            ]
        ) + "\n"
        cache = exporter.MetricsCache(
            repo_root=Path("/tmp/stream-v3"),
            state_root=Path("/tmp/stream-v3/.state/arena-monitor"),
            ttl_sec=0,
            timeout_sec=1,
            health_snapshot_file=None,
            max_health_snapshot_age_sec=600,
            objective_snapshot_file=None,
            max_objective_snapshot_age_sec=600,
        )

        with mock.patch.object(
            exporter,
            "build_metrics",
            side_effect=[good_payload, subprocess.TimeoutExpired(["stream-prod"], 45)],
        ):
            first, first_error = cache.get()
            second, second_error = cache.get()

        self.assertEqual(first_error, "")
        self.assertIn('stream_v3_health_pass{window_hours="24"} 1.0', first)
        self.assertIn('stream_v3_health_pass{window_hours="24"} 1.0', second)
        self.assertIn("TimeoutExpired", second_error)
        self.assertEqual(metric_value(second, "stream_v3_exporter_up"), 0.0)
        self.assertEqual(metric_value(second, "stream_v3_exporter_last_refresh_success"), 0.0)
        self.assertIn('stream_v3_exporter_error{error="TimeoutExpired:', second)

    def test_external_blackbox_metrics_keep_unknown_distinct_from_failed_and_use_source_age(self) -> None:
        exporter = load_exporter()
        writer = exporter.MetricWriter()
        status = {
            "checked_at_utc": "2026-08-10T00:10:00Z",
            "evidence_at_utc": "2026-08-10T00:09:00Z",
            "status": "unknown",
            "targets": {
                "public_status": {
                    "status": "ok",
                    "fresh_locations": 3,
                    "pass_ratio": 1.0,
                    "sample_age_seconds": 60,
                }
            },
        }

        exporter.write_external_blackbox_metrics(writer, status, now=1_786_320_660.0)
        payload = writer.render()

        self.assertEqual(metric_value(payload, "stream_v3_external_blackbox_ok"), 0.0)
        self.assertEqual(metric_value(payload, "stream_v3_external_blackbox_status"), -1.0)
        self.assertEqual(metric_value(payload, "stream_v3_external_blackbox_sample_available"), 1.0)
        self.assertEqual(metric_value(payload, "stream_v3_external_blackbox_age_seconds"), 120.0)
        self.assertEqual(
            metric_value(
                payload,
                "stream_v3_external_blackbox_target_status",
                '{target="public_status"}',
            ),
            1.0,
        )

    def test_map_runtime_metrics_export_delivery_weather_and_container_contracts(self) -> None:
        exporter = load_exporter()
        writer = exporter.MetricWriter()
        status = {
            "schema": "stream_v3.map_runtime_monitor.v2",
            "checked_at_utc": "2026-08-04T08:10:00Z",
            "status": "healthy",
            "delivery_critical_ok": True,
            "weather_ok": True,
            "expected_containers": ["stream-engine", "precipitation-fetcher"],
            "readiness": {
                "ready": True,
                "nvenc_active": True,
                "rtmp_socket_established": True,
            },
            "conditions": {
                "render_heartbeat": True,
                "precipitation_data_ok": True,
                "precipitation_generation_integrity": True,
                "precipitation_render_applied": True,
                "precipitation_validtime_match": True,
                "semantic_visual_contract": True,
                "asset_identity": True,
            },
            "render": {
                "age_sec": 4.2,
                "map_tiles_ready": True,
                "aircraft_sample_ready": True,
            },
            "browser": {
                "contract_ok": True,
                "webgl2_blocklisted": False,
                "context_fatal_failure": False,
            },
            "precipitation": {
                "observed_age_sec": 300,
                "status": {"available": True, "has_precipitation": True},
                "health": {"state": "current", "consecutive_failures": 0},
                "render": {"layer_loaded": True},
            },
            "pod": {
                "containers": {
                    "stream-engine": {"ready": True, "restart_count": 0},
                    "precipitation-fetcher": {"ready": True, "restart_count": 1},
                }
            },
        }

        exporter.write_map_runtime_metrics(writer, status, now=1_785_831_030.0)
        payload = writer.render()

        self.assertEqual(metric_value(payload, "stream_v3_map_monitor_sample_available"), 1.0)
        self.assertEqual(metric_value(payload, "stream_v3_map_monitor_delivery_critical_ok"), 1.0)
        self.assertEqual(metric_value(payload, "stream_v3_map_monitor_weather_ok"), 1.0)
        self.assertEqual(metric_value(payload, "stream_v3_map_precipitation_data_ok"), 1.0)
        self.assertEqual(
            metric_value(payload, "stream_v3_map_precipitation_generation_integrity"),
            1.0,
        )
        self.assertEqual(metric_value(payload, "stream_v3_map_precipitation_render_applied"), 1.0)
        self.assertEqual(metric_value(payload, "stream_v3_map_precipitation_validtime_match"), 1.0)
        self.assertEqual(metric_value(payload, "stream_v3_map_semantic_visual_contract_ok"), 1.0)
        self.assertEqual(metric_value(payload, "stream_v3_map_asset_identity_ok"), 1.0)
        self.assertEqual(metric_value(payload, "stream_v3_map_precipitation_expected"), 1.0)
        self.assertEqual(metric_value(payload, "stream_v3_map_precipitation_layer_loaded"), 1.0)
        self.assertEqual(metric_value(payload, "stream_v3_map_nvenc_active"), 1.0)
        self.assertEqual(metric_value(payload, "stream_v3_map_render_ready"), 1.0)
        self.assertEqual(
            metric_value(
                payload,
                "stream_v3_map_container_restart_count",
                '{container="precipitation-fetcher"}',
            ),
            1.0,
        )

    def test_map_runtime_exporter_accepts_v1_during_v2_rollout(self) -> None:
        exporter = load_exporter()
        writer = exporter.MetricWriter()
        exporter.write_map_runtime_metrics(
            writer,
            {
                "schema": "stream_v3.map_runtime_monitor.v1",
                "checked_at_utc": "2026-08-04T08:10:00Z",
            },
            now=1_785_831_030.0,
        )

        self.assertEqual(
            metric_value(writer.render(), "stream_v3_map_monitor_sample_available"),
            1.0,
        )

    def test_viewer_synthetic_metrics_export_failure_counters(self) -> None:
        exporter = load_exporter()
        writer = exporter.MetricWriter()
        exporter.write_viewer_synthetic_metrics(
            writer,
            {
                "schema": "stream_v3.viewer_synthetic.v1",
                "checked_at_utc": "2026-08-04T08:10:00Z",
                "status": "failed",
                "frame_ok": True,
                "black_detected": True,
                "freeze_detected": False,
                "consecutive_probe_failures": 0,
                "consecutive_visual_failures": 2,
            },
            now=1_785_831_030.0,
        )
        payload = writer.render()
        self.assertEqual(metric_value(payload, "stream_v3_viewer_synthetic_sample_available"), 1.0)
        self.assertEqual(metric_value(payload, "stream_v3_viewer_synthetic_black_detected"), 1.0)
        self.assertEqual(metric_value(payload, "stream_v3_viewer_synthetic_consecutive_visual_failures"), 2.0)


if __name__ == "__main__":
    unittest.main()
