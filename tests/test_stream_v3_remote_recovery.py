from __future__ import annotations

import importlib.util
import subprocess
import sys
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "ops" / "scripts" / "stream_v3_remote_recovery.py"


def load_module():
    spec = importlib.util.spec_from_file_location("stream_v3_remote_recovery_under_test", SCRIPT)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def cp(returncode: int, stdout: str = "", stderr: str = "") -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(args=["cmd"], returncode=returncode, stdout=stdout, stderr=stderr)


class StreamV3RemoteRecoveryTests(unittest.TestCase):
    def test_low_upload_reason_never_restarts_runtime(self) -> None:
        module = load_module()
        state: dict[str, int] = {}

        with (
            mock.patch.object(module, "restart_workload", return_value=True) as restart,
            mock.patch.object(module, "save_state") as save_state,
        ):
            ok = module.maybe_restart(
                "deployment/stream-v3-runtime",
                "low_upload_pressure: send_mbps=2.1",
                state,
                1000,
            )

        self.assertFalse(ok)
        restart.assert_not_called()
        save_state.assert_not_called()
        self.assertEqual(state, {})

    def test_public_defaults_do_not_mutate(self) -> None:
        module = load_module()

        self.assertFalse(module.APPLY)
        self.assertFalse(module.APPLY_ACTION_PLAN)

    def test_non_url_preserving_workload_is_blocked(self) -> None:
        module = load_module()

        with mock.patch.object(module, "restart_workload", return_value=True) as restart:
            ok = module.maybe_restart("deployment/stream-v3-control", "workload inactive", {}, 1000)

        self.assertFalse(ok)
        restart.assert_not_called()

    def test_runtime_workload_inactive_can_restart_with_cooldown_state(self) -> None:
        module = load_module()
        module.APPLY = True
        module.COOLDOWN_SEC = 600
        state: dict[str, int] = {}

        with (
            mock.patch.object(module, "restart_workload", return_value=True) as restart,
            mock.patch.object(module, "save_state") as save_state,
            mock.patch.object(module, "runtime_startup_restart_blocked", return_value=(False, "")),
            mock.patch.object(module, "runtime_gpu_restart_blocked", return_value=(False, "")),
        ):
            ok = module.maybe_restart("deployment/stream-v3-runtime", "workload inactive: ready=0", state, 1000)

        self.assertTrue(ok)
        restart.assert_called_once()
        save_state.assert_called_once()
        self.assertEqual(state["last_restart_ts:deployment/stream-v3-runtime"], 1000)

    def test_runtime_gpu_restart_blocked_detects_driver_mismatch(self) -> None:
        module = load_module()
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
                        ]
                    },
                }
            ]
        }

        with mock.patch.object(module, "kubectl_json", side_effect=[deployment_json, pods_json]):
            blocked, detail = module.runtime_gpu_restart_blocked("deployment/stream-v3-runtime")

        self.assertTrue(blocked)
        self.assertIn("Driver/library version mismatch", detail)

    def test_maybe_restart_records_gpu_block_without_restart(self) -> None:
        module = load_module()
        state: dict[str, int | str] = {}

        with (
            mock.patch.object(module, "runtime_startup_restart_blocked", return_value=(False, "")),
            mock.patch.object(
                module,
                "runtime_gpu_restart_blocked",
                return_value=(True, "RunContainerError: Failed to initialize NVML"),
            ),
            mock.patch.object(module, "restart_workload") as restart,
            mock.patch.object(module, "save_state") as save_state,
        ):
            ok = module.maybe_restart("deployment/stream-v3-runtime", "workload inactive: ready=0", state, 1000)

        self.assertTrue(ok)
        restart.assert_not_called()
        save_state.assert_called_once()
        self.assertEqual(state["last_gpu_restart_block_ts:deployment/stream-v3-runtime"], 1000)

    def test_maybe_restart_blocks_while_current_pod_has_not_established(self) -> None:
        module = load_module()
        state: dict[str, int | str] = {}

        with (
            mock.patch.object(
                module,
                "runtime_startup_restart_blocked",
                return_value=(True, "current Pod has not established NVENC RTMPS delivery"),
            ),
            mock.patch.object(module, "restart_workload") as restart,
            mock.patch.object(module, "save_state") as save_state,
        ):
            ok = module.maybe_restart("deployment/stream-v3-runtime", "workload inactive: ready=0", state, 1000)

        self.assertTrue(ok)
        restart.assert_not_called()
        save_state.assert_called_once()
        self.assertEqual(state["last_startup_restart_block_ts:deployment/stream-v3-runtime"], 1000)

    def test_startup_preflight_fails_closed_when_kubernetes_api_is_unavailable(self) -> None:
        module = load_module()

        with mock.patch.object(
            module,
            "kubectl_json",
            side_effect=RuntimeError("Error from server (ServiceUnavailable)"),
        ):
            blocked, detail = module.runtime_startup_restart_blocked(
                "deployment/stream-v3-runtime"
            )

        self.assertTrue(blocked)
        self.assertIn("startup state unavailable", detail)
        self.assertIn("ServiceUnavailable", detail)

    def test_startup_preflight_blocks_pod_from_previous_node_boot(self) -> None:
        module = load_module()
        pods = {
            "items": [
                {
                    "metadata": {
                        "name": "stream-v3-runtime-abc",
                        "annotations": {
                            "stream-v3.io/gpu-gate-boot-id": "boot-1",
                            "stream-v3.io/stream-established-boot-id": "boot-1",
                        },
                    },
                    "spec": {"nodeName": "yuki"},
                }
            ]
        }
        node = {"status": {"nodeInfo": {"bootID": "boot-2"}}}

        with mock.patch.object(module, "kubectl_json", side_effect=[pods, node]):
            blocked, detail = module.runtime_startup_restart_blocked(
                "deployment/stream-v3-runtime"
            )

        self.assertTrue(blocked)
        self.assertIn("previous host boot", detail)

    def test_action_plan_executes_allowed_scoped_shadow_plan(self) -> None:
        module = load_module()
        module.APPLY = True
        module.APPLY_ACTION_PLAN = True
        module.ACTION_PLAN_MAX_AGE_SEC = 300
        state: dict[str, int | str] = {}
        plan = {
            "ts_utc": "1970-01-01T00:16:40Z",
            "event_id": "evt-dj",
            "action": "restart_dj",
            "executable": True,
            "blocked_by": ["shadow_mode"],
            "reason": "shadow_mode_plan_only",
            "steps": [{"description": "Restart only Auto DJ"}],
        }

        with (
            mock.patch.object(module, "run", return_value=cp(0, stdout="ok scoped\n")) as run,
            mock.patch.object(module, "save_state") as save_state,
        ):
            ok = module.execute_action_plan(plan, state, 1000)

        self.assertTrue(ok)
        self.assertIn("stream_v3_scoped_recovery.py", run.call_args.args[0][1])
        self.assertIn("restart-dj", run.call_args.args[0])
        self.assertEqual(state["last_action_ts:restart_dj"], 1000)
        self.assertEqual(state["last_action_plan_event_id"], "evt-dj")
        save_state.assert_called_once()

    def test_action_plan_blocks_low_upload_reason(self) -> None:
        module = load_module()
        module.APPLY_ACTION_PLAN = True
        plan = {
            "ts_utc": "1970-01-01T00:16:40Z",
            "event_id": "evt-low",
            "action": "restart_ffmpeg",
            "executable": True,
            "blocked_by": ["shadow_mode"],
            "reason": "low_upload_pressure send_mbps=2.1",
            "steps": [],
        }

        with mock.patch.object(module, "run") as run:
            ok = module.execute_action_plan(plan, {}, 1000)

        self.assertTrue(ok)
        run.assert_not_called()

    def test_action_plan_blocks_unapproved_actions(self) -> None:
        module = load_module()
        plan = {
            "ts_utc": "1970-01-01T00:16:40Z",
            "event_id": "evt-replace",
            "action": "create_replacement_broadcast",
            "executable": True,
            "blocked_by": [],
        }

        with mock.patch.object(module, "run") as run:
            ok = module.execute_action_plan(plan, {}, 1000)

        self.assertTrue(ok)
        run.assert_not_called()


if __name__ == "__main__":
    unittest.main()
