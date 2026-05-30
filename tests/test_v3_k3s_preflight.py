from __future__ import annotations

import subprocess
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "ops" / "scripts"))

import v3_k3s_preflight  # type: ignore


def cp(returncode: int, stdout: str = "", stderr: str = "") -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(args=["cmd"], returncode=returncode, stdout=stdout, stderr=stderr)


class V3K3sPreflightTests(unittest.TestCase):
    def test_preflight_reports_missing_cluster_tools_as_blockers(self) -> None:
        def which(name: str) -> str | None:
            return {"docker": "/usr/bin/docker"}.get(name)

        def runner(command: list[str]) -> subprocess.CompletedProcess[str]:
            if command[:2] == ["python3", "ops/scripts/validate_k3s_manifests.py"]:
                return cp(0, stdout="[ok] manifest\n")
            if command[:2] == ["docker", "info"]:
                return cp(1, stderr="permission denied")
            return cp(99, stderr="unexpected")

        report = v3_k3s_preflight.preflight(runner=runner, which=which)

        self.assertFalse(report["ok"])
        check_names = {item["name"] for item in report["checks"]}
        self.assertIn("build-context:dockerignore", check_names)
        blocker_names = {item["name"] for item in report["blockers"]}
        self.assertIn("command:nvidia-smi", blocker_names)
        self.assertIn("command:ffmpeg", blocker_names)
        self.assertIn("command:kubectl", blocker_names)
        self.assertIn("docker:daemon", blocker_names)
        self.assertNotIn("command:k3s", blocker_names)

    def test_preflight_can_pass_when_required_tools_and_cluster_are_ready(self) -> None:
        def which(name: str) -> str | None:
            return f"/usr/bin/{name}"

        def runner(command: list[str]) -> subprocess.CompletedProcess[str]:
            if command[:3] == ["ffmpeg", "-hide_banner", "-encoders"]:
                return cp(0, stdout=" V....D h264_nvenc           NVIDIA NVENC H.264 encoder\n")
            return cp(0, stdout="ok\n")

        report = v3_k3s_preflight.preflight(runner=runner, which=which)

        self.assertTrue(report["ok"])
        self.assertEqual(
            report["next_apply_command"],
            "kubectl kustomize --load-restrictor=LoadRestrictionsNone deploy/k3s/shadow | kubectl apply -f -",
        )
        self.assertTrue(str(report["next_build_command"]).startswith("nerdctl "))

    def test_preflight_can_target_cutover_overlay(self) -> None:
        def which(name: str) -> str | None:
            return f"/usr/bin/{name}"

        def runner(command: list[str]) -> subprocess.CompletedProcess[str]:
            if command[:3] == ["ffmpeg", "-hide_banner", "-encoders"]:
                return cp(0, stdout=" V....D h264_nvenc           NVIDIA NVENC H.264 encoder\n")
            if command[:2] == ["python3", "ops/scripts/validate_k3s_manifests.py"]:
                self.assertEqual(command[-2:], ["--overlay", "cutover"])
            return cp(0, stdout="ok\n")

        report = v3_k3s_preflight.preflight(overlay="cutover", runner=runner, which=which)

        self.assertTrue(report["ok"])
        self.assertEqual(
            report["next_apply_command"],
            "kubectl kustomize --load-restrictor=LoadRestrictionsNone deploy/k3s/cutover | kubectl apply -f -",
        )

    def test_preflight_can_target_streaming_overlay(self) -> None:
        def which(name: str) -> str | None:
            return f"/usr/bin/{name}"

        def runner(command: list[str]) -> subprocess.CompletedProcess[str]:
            if command[:3] == ["ffmpeg", "-hide_banner", "-encoders"]:
                return cp(0, stdout=" V....D h264_nvenc           NVIDIA NVENC H.264 encoder\n")
            if command[:2] == ["python3", "ops/scripts/validate_k3s_manifests.py"]:
                self.assertEqual(command[-2:], ["--overlay", "streaming"])
            return cp(0, stdout="ok\n")

        report = v3_k3s_preflight.preflight(overlay="streaming", runner=runner, which=which)

        self.assertTrue(report["ok"])
        self.assertEqual(
            report["next_apply_command"],
            "kubectl kustomize --load-restrictor=LoadRestrictionsNone deploy/k3s/streaming | kubectl apply -f -",
        )

    def test_preflight_uses_docker_build_command_when_docker_is_the_available_builder(self) -> None:
        def which(name: str) -> str | None:
            return {
                "kubectl": "/usr/bin/kubectl",
                "docker": "/usr/bin/docker",
                "nvidia-smi": "/usr/bin/nvidia-smi",
                "ffmpeg": "/usr/bin/ffmpeg",
            }.get(name)

        def runner(command: list[str]) -> subprocess.CompletedProcess[str]:
            if command[:3] == ["ffmpeg", "-hide_banner", "-encoders"]:
                return cp(0, stdout=" V....D h264_nvenc           NVIDIA NVENC H.264 encoder\n")
            return cp(0, stdout="ok\n")

        report = v3_k3s_preflight.preflight(runner=runner, which=which)

        self.assertTrue(report["ok"])
        self.assertEqual(
            report["next_build_command"],
            "docker build -f deploy/k3s/Containerfile -t stream-v3:local .",
        )

    def test_ffmpeg_nvenc_check_blocks_when_encoder_is_missing(self) -> None:
        check = v3_k3s_preflight.ffmpeg_nvenc_check(runner=lambda _cmd: cp(0, stdout=" V....D libx264\n"))

        self.assertFalse(check.ok)
        self.assertTrue(check.blocker)
        self.assertEqual(check.name, "ffmpeg:h264_nvenc")
        self.assertIn("missing", check.detail)

    def test_dockerignore_check_blocks_missing_context_excludes(self) -> None:
        path = ROOT / ".state" / "test_missing_dockerignore"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(".git/\n", encoding="utf-8")
        try:
            check = v3_k3s_preflight.dockerignore_check(path)
        finally:
            path.unlink(missing_ok=True)

        self.assertFalse(check.ok)
        self.assertTrue(check.blocker)
        self.assertIn(".state/", check.detail)


if __name__ == "__main__":
    unittest.main()
