from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from stream_v2.cli import main
from stream_v2.local_runtime import (
    DEFAULT_OVERLAY_PORT,
    DEFAULT_PULSE_SINK,
    LocalRuntimeConfig,
    build_local_env,
    local_runtime_summary,
    prepare_local_runtime,
    run_local_smoke,
    write_env_file,
)


class LocalRuntimeTests(unittest.TestCase):
    def test_local_env_is_isolated_from_production_runtime(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td) / "stream_v2"
            state_root = Path(td) / "state"
            config = LocalRuntimeConfig(repo_root=repo_root, state_root=state_root)

            env = build_local_env(config)

        self.assertEqual(env["TEST_MODE"], "1")
        self.assertEqual(env["TEST_OUTPUT"], "null")
        self.assertEqual(env["BASE_DIR"], str(repo_root.resolve()))
        self.assertEqual(env["STREAM_RUNTIME_STATE_DIR"], str(state_root.resolve()))
        self.assertEqual(env["VIDEO_SIZE"], "1920x1080")
        self.assertEqual(env["OUTPUT_SIZE"], "1920x1080")
        self.assertEqual(env["BROWSER_WINDOW_SIZE"], "1920,1080")
        self.assertEqual(env["OVERLAY_PORT"], str(DEFAULT_OVERLAY_PORT))
        self.assertEqual(env["PULSE_SINK"], DEFAULT_PULSE_SINK)
        self.assertEqual(env["PULSE_SOURCE"], f"{DEFAULT_PULSE_SINK}.monitor")
        self.assertEqual(env["REQUIRE_SYSTEMD_LAUNCH"], "0")
        self.assertEqual(env["ALLOW_DIRECT_STREAM_SH"], "1")
        self.assertEqual(env["HEALTH_GATE_ABORT_ON_FOREIGN"], "0")
        self.assertNotEqual(env["PULSE_SINK"], "stream_sink")
        self.assertNotEqual(env["DISPLAY_NAME"], ":99")
        self.assertNotIn("/home/yuki/projects/stream/", env["OVERLAY_DIR"])

    def test_prepare_local_runtime_copies_overlay_and_seeds_now_playing(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td) / "repo"
            source_overlay = repo_root / "ui" / "overlay"
            source_overlay.mkdir(parents=True)
            (source_overlay / "index.html").write_text("<html>overlay</html>\n", encoding="utf-8")
            state_root = Path(td) / "local-state"
            config = LocalRuntimeConfig(repo_root=repo_root, state_root=state_root)

            paths = prepare_local_runtime(config)

            self.assertTrue((paths.overlay_dir / "index.html").exists())
            self.assertTrue(paths.now_playing_file.exists())
            snapshot = json.loads(paths.now_playing_snapshot_file.read_text(encoding="utf-8"))
            self.assertEqual(snapshot["status"], "local_ready")
            self.assertIn("stream_v2 local smoke test", snapshot["now_playing"]["title"])

    def test_write_env_file_uses_0600_permissions(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            config = LocalRuntimeConfig(repo_root=Path(td) / "repo", state_root=Path(td) / "state")

            env_path = write_env_file(config)

            self.assertEqual(env_path.stat().st_mode & 0o777, 0o600)
            text = env_path.read_text(encoding="utf-8")
            self.assertIn("TEST_MODE=1", text)
            self.assertIn("STREAM_KEY=LOCAL_TEST_ONLY", text)

    def test_local_summary_keeps_rendering_under_overlay_runtime(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            config = LocalRuntimeConfig(repo_root=Path(td) / "repo", state_root=Path(td) / "state", with_dj=True)

            payload = local_runtime_summary(config)

            self.assertEqual(payload["safety"]["youtube_rtmp"], "disabled by TEST_MODE")
            self.assertEqual(payload["rendering"]["overlay_url"], f"http://127.0.0.1:{DEFAULT_OVERLAY_PORT}/index.html")
            self.assertTrue(str(payload["rendering"]["overlay_dir"]).endswith("/overlay"))
            self.assertIn("auto_dj.py", " ".join(payload["commands"]["auto_dj"]))
            self.assertIn("--max-track-sec", payload["commands"]["auto_dj"])
            self.assertIn("0", payload["commands"]["auto_dj"])
            self.assertEqual(payload["audio"]["max_track_sec"], 0)

    def test_local_smoke_dry_run_does_not_spawn_processes(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            config = LocalRuntimeConfig(
                repo_root=Path(td) / "repo",
                state_root=Path(td) / "state",
                dry_run=True,
                duration_sec=1,
            )
            with mock.patch("stream_v2.local_runtime.subprocess.Popen") as popen:
                rc = run_local_smoke(config)

        self.assertEqual(rc, 0)
        popen.assert_not_called()

    def test_local_smoke_restarts_auto_dj_child(self) -> None:
        class FakeProc:
            def __init__(self, polls: list[int | None]) -> None:
                self.polls = polls
                self.pid = id(self)
                self.signals: list[int] = []

            def poll(self) -> int | None:
                if self.polls:
                    return self.polls.pop(0)
                return None

            def send_signal(self, signum: int) -> None:
                self.signals.append(signum)

            def wait(self, timeout: float | None = None) -> int:
                return 0

            def kill(self) -> None:
                self.signals.append(-9)

        with tempfile.TemporaryDirectory() as td:
            config = LocalRuntimeConfig(
                repo_root=Path(td) / "repo",
                state_root=Path(td) / "state",
                with_dj=True,
                duration_sec=0.1,
                dj_start_delay_sec=0,
                dj_restart_delay_sec=0,
            )
            engine = FakeProc([None, None, None])
            dj_first = FakeProc([0])
            dj_second = FakeProc([None, None])
            with mock.patch(
                "stream_v2.local_runtime._start_process",
                side_effect=[engine, dj_first, dj_second],
            ) as start_process:
                rc = run_local_smoke(config)

        self.assertEqual(rc, 0)
        self.assertEqual(start_process.call_count, 3)
        self.assertEqual(start_process.call_args_list[1].kwargs["label"], "auto_dj")
        self.assertEqual(start_process.call_args_list[2].kwargs["label"], "auto_dj")

    def test_cli_local_env_prints_json(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            state_root = Path(td) / "state"
            with mock.patch("builtins.print") as printed:
                rc = main(["local-env", "--state-root", str(state_root), "--pretty"])

            payload = json.loads(str(printed.call_args.args[0]))

        self.assertEqual(rc, 0)
        self.assertEqual(payload["mode"], "local_test")
        self.assertEqual(payload["env"]["TEST_MODE"], "1")

    def test_cli_local_smoke_dry_run(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            with mock.patch("stream_v2.local_runtime.subprocess.Popen") as popen:
                rc = main(["local-smoke", "--state-root", str(Path(td) / "state"), "--dry-run", "--duration-sec", "1"])

        self.assertEqual(rc, 0)
        popen.assert_not_called()


if __name__ == "__main__":
    unittest.main()
