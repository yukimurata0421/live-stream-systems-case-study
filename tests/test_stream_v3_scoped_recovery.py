from __future__ import annotations

import importlib.util
import subprocess
import sys
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "ops" / "scripts" / "stream_v3_scoped_recovery.py"


def load_module():
    spec = importlib.util.spec_from_file_location("stream_v3_scoped_recovery_under_test", SCRIPT)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def cp(returncode: int, stdout: str = "", stderr: str = "") -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(args=["cmd"], returncode=returncode, stdout=stdout, stderr=stderr)


def pod_payload(*, auto_dj_restart_count: int = 0, stream_engine_restart_count: int = 0) -> dict:
    return {
        "items": [
            {
                "metadata": {"name": "stream-v3-runtime-abc"},
                "status": {
                    "phase": "Running",
                    "containerStatuses": [
                        {
                            "name": "stream-engine",
                            "restartCount": stream_engine_restart_count,
                            "containerID": "containerd://stream-engine-1",
                            "ready": True,
                        },
                        {
                            "name": "auto-dj",
                            "restartCount": auto_dj_restart_count,
                            "containerID": f"containerd://auto-dj-{auto_dj_restart_count}",
                            "ready": True,
                        },
                    ],
                },
            }
        ]
    }


class StreamV3ScopedRecoveryTests(unittest.TestCase):
    def test_low_upload_reason_blocks_before_kubectl(self) -> None:
        module = load_module()

        with mock.patch.object(module, "runtime_pod_json") as pod_json:
            rc = module.restart_ffmpeg(reason="low_upload_pressure send_mbps=2.1", dry_run=False, timeout_sec=5)

        self.assertEqual(rc, 2)
        pod_json.assert_not_called()

    def test_restart_dj_targets_auto_dj_container_and_waits_for_only_that_container(self) -> None:
        module = load_module()
        before = pod_payload(auto_dj_restart_count=0, stream_engine_restart_count=0)
        after = pod_payload(auto_dj_restart_count=1, stream_engine_restart_count=0)

        with (
            mock.patch.object(module, "runtime_pod_json", side_effect=[before, after]),
            mock.patch.object(module, "exec_in_container", return_value=cp(143, stderr="terminated")) as exec_in_container,
        ):
            rc = module.restart_dj(reason="audio_energy_low confirmed", dry_run=False, timeout_sec=5)

        self.assertEqual(rc, 0)
        exec_in_container.assert_called_once()
        self.assertEqual(exec_in_container.call_args.args[0], "stream-v3-runtime-abc")
        self.assertEqual(exec_in_container.call_args.args[1], "auto-dj")
        self.assertIn("kill -TERM 1", exec_in_container.call_args.args[2])

    def test_restart_ffmpeg_targets_stream_engine_rtmps_child(self) -> None:
        module = load_module()

        with (
            mock.patch.object(module, "runtime_pod_json", return_value=pod_payload()),
            mock.patch.object(
                module,
                "exec_in_container",
                side_effect=[
                    cp(0, stdout="123\n"),
                    cp(0, stdout="terminated_rtmps_ffmpeg_pid=123\n"),
                    cp(0, stdout="456\n"),
                ],
            ) as exec_in_container,
        ):
            rc = module.restart_ffmpeg(reason="tcp_stall", dry_run=False, timeout_sec=5)

        self.assertEqual(rc, 0)
        self.assertEqual(len(exec_in_container.call_args_list), 3)
        kill_call = exec_in_container.call_args_list[1]
        self.assertEqual(kill_call.args[0], "stream-v3-runtime-abc")
        self.assertEqual(kill_call.args[1], "stream-engine")
        script = kill_call.args[2]
        self.assertIn("pgrep -a ffmpeg", script)
        self.assertIn("rtmp://|rtmps://", script)
        self.assertIn("kill -TERM", script)

    def test_restart_ffmpeg_falls_back_to_stream_engine_container_when_child_is_missing(self) -> None:
        module = load_module()
        before = pod_payload(auto_dj_restart_count=0, stream_engine_restart_count=0)
        after = pod_payload(auto_dj_restart_count=0, stream_engine_restart_count=1)

        with (
            mock.patch.object(module, "runtime_pod_json", side_effect=[before, after]),
            mock.patch.object(
                module,
                "exec_in_container",
                side_effect=[
                    cp(10, stdout="rtmps_ffmpeg_count=0\n"),
                    cp(143, stderr="terminated"),
                ],
            ) as exec_in_container,
        ):
            rc = module.restart_ffmpeg(reason="ffmpeg_missing", dry_run=False, timeout_sec=5)

        self.assertEqual(rc, 0)
        self.assertEqual(len(exec_in_container.call_args_list), 2)
        fallback_call = exec_in_container.call_args_list[1]
        self.assertEqual(fallback_call.args[0], "stream-v3-runtime-abc")
        self.assertEqual(fallback_call.args[1], "stream-engine")
        self.assertIn("kill -TERM 1", fallback_call.args[2])


if __name__ == "__main__":
    unittest.main()
