from __future__ import annotations

import os
import subprocess
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class StreamV3BootstrapTests(unittest.TestCase):
    def test_stream_prod_defaults_to_v3_state_and_blocks_mutation(self) -> None:
        launcher = ROOT / "bin" / "stream-prod"
        text = launcher.read_text(encoding="utf-8")

        self.assertIn(".state/adsb-streamnew-v3", text)
        self.assertIn("STREAM_V3_CUTOVER_ENABLE", text)
        self.assertIn("STREAM_V2_SOURCE_STATE_ROOT", text)
        self.assertNotIn("export STREAM_V2_ALLOW_MUTATING_SYSTEMD=1\n\nexec", text)

        env = os.environ.copy()
        env.pop("STREAM_V3_CUTOVER_ENABLE", None)
        completed = subprocess.run(
            [str(launcher), "restart"],
            cwd=ROOT,
            env=env,
            text=True,
            capture_output=True,
            check=False,
        )

        self.assertEqual(completed.returncode, 2)
        self.assertIn("refusing stream_v3 mutating command", completed.stderr)

    def test_stream_prod_non_mutating_uses_v3_state_root(self) -> None:
        launcher = ROOT / "bin" / "stream-prod"
        with tempfile.TemporaryDirectory() as tmp:
            env = os.environ.copy()
            env["STREAM_RUNTIME_STATE_DIR"] = str(Path(tmp) / "state")
            completed = subprocess.run(
                [str(launcher), "status"],
                cwd=ROOT,
                env=env,
                text=True,
                capture_output=True,
                check=False,
            )

        self.assertNotEqual(completed.returncode, 2)
        self.assertNotIn("refusing stream_v3 mutating command", completed.stderr)

    def test_k3s_shadow_manifests_exist_and_stay_test_mode(self) -> None:
        required = [
            ".dockerignore",
            "deploy/k3s/Containerfile",
            "deploy/k3s/shadow/kustomization.yaml",
            "deploy/k3s/cutover/kustomization.yaml",
            "deploy/k3s/cutover/patch-configmap-cutover.yaml",
            "deploy/k3s/streaming/kustomization.yaml",
            "deploy/k3s/streaming/patch-configmap-streaming.yaml",
            "deploy/k3s/base/configmap-shadow.yaml",
            "deploy/k3s/base/namespace.yaml",
            "deploy/k3s/base/secret.example.yaml",
            "deploy/k3s/v2-state-mirror/cronjob.yaml",
            "deploy/k3s/v2-state-mirror/source-state-pvc.yaml",
            "deploy/k3s/v2-state-mirror/secret.example.yaml",
            "deploy/k3s/v3-reports/kustomization.yaml",
            "deploy/k3s/v3-reports/youtube-api-cost-open-day-cronjob.yaml",
            "deploy/k3s/v3-reports/youtube-api-cost-closed-day-cronjob.yaml",
            "deploy/k3s/v3-reports/stream1090-report-cronjob.yaml",
            "deploy/k3s/v3-reports/upstream-report-cronjob.yaml",
            "deploy/k3s/v3-runtime/deployment.yaml",
            "deploy/k3s/v3-runtime/state-pvc.yaml",
            "deploy/k3s/v3-runtime/music-pvc.yaml",
            "deploy/k3s/v3-control/deployment.yaml",
            "deploy/k3s/v3-observer/deployment.yaml",
            "deploy/k3s/v3-observer/service.yaml",
            "ops/systemd/stream-v3-remote-recovery.env.example",
        ]
        for rel in required:
            self.assertTrue((ROOT / rel).is_file(), rel)

        configmap = (ROOT / "deploy/k3s/base/configmap-shadow.yaml").read_text(encoding="utf-8")
        runtime = (ROOT / "deploy/k3s/v3-runtime/deployment.yaml").read_text(encoding="utf-8")
        control = (ROOT / "deploy/k3s/v3-control/deployment.yaml").read_text(encoding="utf-8")
        secret = (ROOT / "deploy/k3s/base/secret.example.yaml").read_text(encoding="utf-8")
        mirror = (ROOT / "deploy/k3s/v2-state-mirror/cronjob.yaml").read_text(encoding="utf-8")
        reports = (ROOT / "deploy/k3s/v3-reports/stream1090-report-cronjob.yaml").read_text(encoding="utf-8")

        self.assertIn("namespace: stream-v3", configmap)
        self.assertIn("TEST_MODE: \"1\"", configmap)
        self.assertIn("STREAM_V3_CUTOVER_ENABLE: \"0\"", configmap)
        self.assertIn("STREAM_RUNTIME_SUPERVISOR: k8s", configmap)
        self.assertIn("STREAM_K8S_DRY_RUN: \"1\"", configmap)
        self.assertIn("STREAM_V2_SOURCE_STATE_ROOT: /source-v2-readonly", configmap)
        self.assertIn("STREAM_V2_MIRROR_SOURCE:", configmap)
        self.assertIn("VIDEO_ENCODER: h264_nvenc", configmap)
        self.assertIn("VIDEO_NVENC_PRESET: p4", configmap)
        self.assertIn("VIDEO_NVENC_RC: cbr", configmap)
        self.assertIn("VIDEO_NVENC_CQ: \"\"", configmap)
        self.assertIn("VIDEO_NVENC_MULTIPASS: \"\"", configmap)
        self.assertIn("VIDEO_NVENC_RC_LOOKAHEAD: \"0\"", configmap)
        self.assertIn("VIDEO_NVENC_SPATIAL_AQ: \"0\"", configmap)
        self.assertIn("VIDEO_NVENC_TEMPORAL_AQ: \"0\"", configmap)
        self.assertIn("VIDEO_NVENC_BFRAMES: \"0\"", configmap)
        self.assertIn("VIDEO_NVENC_B_REF_MODE: \"\"", configmap)
        self.assertIn("FRAME_RATE: \"5\"", configmap)
        self.assertIn("VIDEO_BITRATE: 3400k", configmap)
        self.assertIn("VIDEO_MAXRATE: 3400k", configmap)
        self.assertIn("VIDEO_BUFSIZE: 6800k", configmap)
        self.assertIn("PULSE_SERVER: unix:/run/stream-pulse/native", configmap)
        self.assertIn("AUTO_DJ_KEEP_PULSE_SERVER: \"1\"", configmap)
        self.assertIn("MUSIC_ROOT: /music/time_tags", configmap)
        self.assertIn("RUNTIME_HEARTBEAT_SEC: \"10\"", configmap)
        self.assertIn("CAPTURE_HELPER_MEMORY_GUARD_ENABLED: \"1\"", configmap)
        self.assertIn("XVFB_MEMORY_GUARD_RSS_MIB: \"2048\"", configmap)
        self.assertIn("XVFB_MEMORY_GUARD_SHMEM_MIB: \"1536\"", configmap)
        self.assertIn("FR_RTMP_HOST: a.rtmps.youtube.com", configmap)
        self.assertIn("FR_RTMP_PORTS: \"443\"", configmap)
        self.assertIn("FR_EVENT_LOG_FILE: /state/logs/fast_recovery_events.jsonl", configmap)
        self.assertIn("V3_FAST_RECOVERY_INTERVAL_SEC: \"10\"", configmap)
        self.assertIn("stream-v3:local", runtime)
        self.assertIn("/app/src/stream_v3/runtime_entrypoint.sh", runtime)
        self.assertIn("NVIDIA_DRIVER_CAPABILITIES", runtime)
        self.assertIn("value: video,utility", runtime)
        self.assertIn("nvidia.com/gpu: \"1\"", runtime)
        self.assertIn("mountPath: /run/stream-pulse", runtime)
        self.assertIn("name: pulse-run", runtime)
        self.assertIn("--music-root /music/time_tags", runtime)
        self.assertIn("python3 -m stream_v3.control_loop", control)
        self.assertIn("mountPath: /source-v2-readonly", control)
        self.assertIn("readOnly: true", control)
        self.assertNotIn("/home/yuki/projects/stream_v2/.state", runtime)
        self.assertIn("replace-with-youtube-stream-key", secret)
        self.assertIn("YTW_API_KEY", secret)
        self.assertIn("YTW_OAUTH_CLIENT_ID", secret)
        self.assertIn("YTW_OAUTH_CLIENT_SECRET", secret)
        self.assertIn("YTW_OAUTH_REFRESH_TOKEN", secret)
        self.assertIn("STREAM_NOTIFY_DISCORD_WEBHOOK_URL", secret)
        self.assertIn("secretRef:", runtime)
        self.assertIn("name: stream-v3-secrets", runtime)
        self.assertIn("optional: true", runtime)
        self.assertIn("suspend: true", mirror)
        self.assertIn("rsync -az --delete", mirror)
        self.assertIn("suspend: true", reports)
        self.assertIn("/app/bin/stream-prod stream1090-report", reports)

        containerfile = (ROOT / "deploy/k3s/Containerfile").read_text(encoding="utf-8")
        self.assertIn("pulseaudio \\", containerfile)
        entrypoint = (ROOT / "src/stream_v3/runtime_entrypoint.sh").read_text(encoding="utf-8")
        self.assertIn("--disable-shm=yes", entrypoint)
        self.assertIn("--enable-memfd=no", entrypoint)

        dockerignore = (ROOT / ".dockerignore").read_text(encoding="utf-8")
        for pattern in (".state/", "venv/", ".venv/", "ncs_music/*", "logs/", "runtime/"):
            self.assertIn(pattern, dockerignore)

        observer = (ROOT / "deploy/k3s/v3-observer/deployment.yaml").read_text(encoding="utf-8")
        self.assertIn("--repo-root /app", observer)
        self.assertIn("--host 0.0.0.0", observer)

    def test_remote_recovery_unit_uses_repo_dir_env_override(self) -> None:
        unit = (ROOT / "ops" / "systemd" / "stream-v3-remote-recovery.service").read_text(encoding="utf-8")
        env_example = (ROOT / "ops" / "systemd" / "stream-v3-remote-recovery.env.example").read_text(encoding="utf-8")

        self.assertIn("EnvironmentFile=-/etc/default/stream-v3-remote-recovery", unit)
        self.assertIn("STREAM_V3_REPO_DIR", unit)
        self.assertIn("STREAM_V3_REMOTE_RECOVERY_APPLY_ACTION_PLAN=1", unit)
        self.assertIn("STREAM_V3_REMOTE_RECOVERY_ACTION_PLAN_MAX_AGE_SEC=180", unit)
        self.assertIn('"$${STREAM_V3_REPO_DIR}/ops/scripts/stream_v3_remote_recovery.py"', unit)
        legacy_repo_path = "/home/" + "yuki/projects/stream_v3"
        self.assertNotIn(f"ExecStart=/usr/bin/python3 {legacy_repo_path}", unit)
        self.assertNotIn(legacy_repo_path, unit)
        self.assertIn("STREAM_V3_REMOTE_RECOVERY_ACTION_PLAN_FILE=", env_example)

    def test_k3s_manifest_validator_passes_shadow_overlay(self) -> None:
        completed = subprocess.run(
            ["python3", "ops/scripts/validate_k3s_manifests.py"],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=False,
        )

        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertIn("overlay=shadow", completed.stdout)

    def test_k3s_manifest_validator_passes_cutover_overlay(self) -> None:
        completed = subprocess.run(
            ["python3", "ops/scripts/validate_k3s_manifests.py", "--overlay", "cutover"],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=False,
        )

        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertIn("overlay=cutover", completed.stdout)

    def test_k3s_manifest_validator_passes_streaming_overlay(self) -> None:
        completed = subprocess.run(
            ["python3", "ops/scripts/validate_k3s_manifests.py", "--overlay", "streaming"],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=False,
        )

        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertIn("overlay=streaming", completed.stdout)

    def test_k3s_yaml_files_parse(self) -> None:
        try:
            import yaml  # type: ignore
        except ModuleNotFoundError:
            self.skipTest("PyYAML is not installed")

        for path in sorted((ROOT / "deploy/k3s").rglob("*.yaml")):
            with self.subTest(path=path.relative_to(ROOT)):
                with path.open(encoding="utf-8") as fh:
                    docs = list(yaml.safe_load_all(fh))
                self.assertTrue(docs)


if __name__ == "__main__":
    unittest.main()
