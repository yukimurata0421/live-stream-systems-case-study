from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "ops" / "scripts"))

import v3_shadow_acceptance  # type: ignore


def cp(returncode: int, stdout: str = "", stderr: str = "") -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(args=["cmd"], returncode=returncode, stdout=stdout, stderr=stderr)


class V3ShadowAcceptanceTests(unittest.TestCase):
    def test_acceptance_passes_when_manifest_control_loop_and_shadow_plan_are_safe(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            state_root = Path(td) / "state"

            def runner(command: list[str], _env: dict[str, str]) -> subprocess.CompletedProcess[str]:
                if command[:2] == ["python3", "ops/scripts/validate_k3s_manifests.py"]:
                    return cp(0, stdout="[ok] manifest\n")
                if command[:3] == [sys.executable, "-m", "stream_v3.control_loop"]:
                    state_root.mkdir(parents=True)
                    (state_root / "recovery_action_plan.json").write_text(
                        json.dumps({"execute": False, "executable": False, "blocked_by": ["shadow_mode"]}),
                        encoding="utf-8",
                    )
                    return cp(
                        0,
                        stdout=json.dumps(
                            {
                                "results": [
                                    {"name": "shadow_once", "ok": True},
                                    {"name": "subsystems_status", "ok": True},
                                ]
                            }
                        ),
                    )
                return cp(99, stderr="unexpected command")

            report = v3_shadow_acceptance.acceptance(
                state_root=state_root,
                source_state_root=Path("/source"),
                runner=runner,
                base_env={},
            )

        self.assertTrue(report["ok"])
        names = {item["name"] for item in report["checks"]}
        self.assertEqual(names, {"manifest:shadow", "control-loop:shadow-once", "action-plan:shadow-safe"})

    def test_action_plan_check_rejects_executable_shadow_plan(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "recovery_action_plan.json"
            path.write_text(
                json.dumps({"execute": True, "executable": True, "blocked_by": []}),
                encoding="utf-8",
            )

            check = v3_shadow_acceptance._action_plan_check(path)

        self.assertFalse(check.ok)
        self.assertIn("execute=true", check.detail)


if __name__ == "__main__":
    unittest.main()
