from __future__ import annotations

import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from stream_core import runtime_readiness


def completed(command: list[str], returncode: int = 0, stdout: str = "", stderr: str = "") -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(command, returncode, stdout=stdout, stderr=stderr)


class RuntimeReadinessTests(unittest.TestCase):
    def test_readiness_requires_gpu_nvenc_ffmpeg_uptime_and_rtmp_socket(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            proc = Path(td)
            (proc / "sys" / "kernel" / "random").mkdir(parents=True)
            (proc / "sys" / "kernel" / "random" / "boot_id").write_text("boot-1\n", encoding="utf-8")
            (proc / "uptime").write_text("100.0 50.0\n", encoding="utf-8")
            process = proc / "123"
            process.mkdir()
            args = [
                "ffmpeg",
                "-c:v",
                "h264_nvenc",
                "-f",
                "flv",
                "rtmps://a.rtmps.youtube.com:443/live2/key",
            ]
            (process / "cmdline").write_bytes(b"\0".join(part.encode() for part in args) + b"\0")
            ticks = int(os.sysconf("SC_CLK_TCK"))
            fields = ["0"] * 52
            fields[0] = "123"
            fields[1] = "(ffmpeg)"
            fields[21] = str(50 * ticks)
            (process / "stat").write_text(" ".join(fields), encoding="utf-8")

            def runner(command: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
                if command[0] == "nvidia-smi":
                    return completed(command, stdout="580.173.02\n")
                if command[0] == "ss":
                    return completed(
                        command,
                        stdout='0 0 10.42.0.10:40000 142.250.1.1:443 users:(("ffmpeg",pid=123,fd=8))\n',
                    )
                raise AssertionError(command)

            with mock.patch.dict(
                os.environ,
                {"STREAM_V3_POD_NAME": "runtime-abc", "STREAM_V3_POD_UID": "uid-abc"},
                clear=False,
            ):
                status = runtime_readiness.readiness_status(
                    proc_root=proc,
                    min_ffmpeg_uptime_sec=10,
                    rtmp_ports=(443,),
                    runner=runner,
                )

        self.assertTrue(status["ready"])
        self.assertTrue(status["nvenc_active"])
        self.assertTrue(status["rtmp_socket_established"])
        self.assertEqual(status["ffmpeg_uptime_sec"], 50)
        self.assertEqual(status["driver_version"], "580.173.02")

    def test_current_pod_establishment_rejects_previous_pod_marker(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            boot_id = root / "boot_id"
            marker = root / "marker.json"
            boot_id.write_text("boot-current\n", encoding="utf-8")
            marker.write_text(
                json.dumps(
                    {
                        "ready": True,
                        "boot_id": "boot-current",
                        "pod_name": "runtime-old",
                        "pod_uid": "uid-old",
                    }
                ),
                encoding="utf-8",
            )

            status = runtime_readiness.current_pod_establishment(
                marker,
                pod_uid="uid-new",
                pod_name="runtime-new",
                boot_id_file=boot_id,
            )

        self.assertFalse(status["established"])
        self.assertIn("another Pod", status["reason"])


if __name__ == "__main__":
    unittest.main()
