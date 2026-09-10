from __future__ import annotations

import importlib.util
import io
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest import mock

SCRIPT = Path(__file__).parents[1] / "tools" / "deploy_stream_v3_evidence_binding.py"


def load_module():
    spec = importlib.util.spec_from_file_location("deploy_stream_v3_evidence_binding", SCRIPT)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"unable to load {SCRIPT}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class DeployStreamV3EvidenceBindingTest(unittest.TestCase):
    def make_release(self, root: Path) -> Path:
        release = root / "release"
        for relative in (
            "ops/scripts/stream_v3_remote_recovery.py",
            "ops/scripts/stream_v3_scoped_recovery.py",
            "src/maintenance_audit/__init__.py",
        ):
            path = release / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("# fixture\n", encoding="utf-8")
        return release

    def run_stage(self, stage: str):
        module = load_module()
        installed: list[tuple[Path, list[str]]] = []
        commands: list[tuple[str, ...]] = []
        with tempfile.TemporaryDirectory() as temporary:
            release = self.make_release(Path(temporary))
            stdout = io.StringIO()
            with (
                mock.patch.object(module, "install_dropin", side_effect=lambda path, lines: installed.append((path, lines))),
                mock.patch.object(module.subprocess, "run", side_effect=lambda command, check: commands.append(tuple(command))),
                mock.patch.object(sys, "argv", [str(SCRIPT), "--stage", stage, "--release", str(release)]),
                redirect_stdout(stdout),
            ):
                self.assertEqual(module.main(), 0)
        return installed, commands, stdout.getvalue()

    def test_stage_c_binds_only_remote_recovery_without_shared_current(self) -> None:
        installed, commands, output = self.run_stage("C")
        self.assertEqual(len(installed), 1)
        self.assertIn("stream-v3-remote-recovery.service.d", str(installed[0][0]))
        self.assertNotIn("/opt/stream-v3/releases/current", "\n".join(installed[0][1]))
        self.assertEqual(commands, [("/usr/bin/systemctl", "daemon-reload")])
        self.assertIn("shared_current_modified=false", output)
        self.assertIn("service_restart_count=0", output)

    def test_stage_d_restarts_only_arena_monitor(self) -> None:
        installed, commands, output = self.run_stage("D")
        self.assertEqual(len(installed), 1)
        self.assertIn("stream-v3-arena-monitor.service.d", str(installed[0][0]))
        self.assertNotIn("/opt/stream-v3/releases/current", "\n".join(installed[0][1]))
        self.assertEqual(
            commands,
            [
                ("/usr/bin/systemctl", "daemon-reload"),
                ("/usr/bin/systemctl", "restart", "stream-v3-arena-monitor.service"),
            ],
        )
        self.assertIn("shared_current_modified=false", output)
        self.assertIn("service_restart_count=1", output)


if __name__ == "__main__":
    unittest.main()
