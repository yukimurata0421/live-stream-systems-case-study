from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from stream_core.k8s_gpu_guard import summarize_runtime_gpu, summarize_runtime_startup


def deployment() -> dict[str, object]:
    return {
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


class K8sGpuGuardTests(unittest.TestCase):
    def test_startup_restart_is_blocked_until_current_boot_is_established(self) -> None:
        pods = {
            "items": [
                {
                    "metadata": {
                        "name": "stream-v3-runtime-abc",
                        "annotations": {"stream-v3.io/gpu-gate-boot-id": "boot-2"},
                    },
                    "spec": {},
                }
            ]
        }

        summary = summarize_runtime_startup(pods)

        self.assertEqual(summary["status"], "starting")
        self.assertTrue(summary["restart_blocked"])
        self.assertIn("not established", str(summary["restart_block_reason"]))

    def test_startup_restart_is_allowed_after_current_boot_is_established(self) -> None:
        pods = {
            "items": [
                {
                    "metadata": {
                        "name": "stream-v3-runtime-abc",
                        "annotations": {
                            "stream-v3.io/gpu-gate-boot-id": "boot-2",
                            "stream-v3.io/stream-established-boot-id": "boot-2",
                        },
                    },
                    "spec": {},
                }
            ]
        }

        summary = summarize_runtime_startup(pods)

        self.assertEqual(summary["status"], "established")
        self.assertFalse(summary["restart_blocked"])

    def test_startup_restart_is_blocked_while_pod_is_scheduling_gated(self) -> None:
        pods = {
            "items": [
                {
                    "metadata": {"name": "stream-v3-runtime-abc"},
                    "spec": {"schedulingGates": [{"name": "stream-v3.io/gpu-ready"}]},
                }
            ]
        }

        summary = summarize_runtime_startup(pods)

        self.assertTrue(summary["restart_blocked"])
        self.assertIn("device-plugin", str(summary["restart_block_reason"]))

    def test_startup_restart_is_blocked_when_pod_belongs_to_previous_host_boot(self) -> None:
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

        summary = summarize_runtime_startup(pods, expected_boot_id="boot-2")

        self.assertTrue(summary["restart_blocked"])
        self.assertIn("previous host boot", str(summary["restart_block_reason"]))

    def test_blocks_restart_on_current_nvml_driver_library_mismatch(self) -> None:
        pods = {
            "items": [
                {
                    "metadata": {"name": "stream-v3-runtime-abc"},
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

        summary = summarize_runtime_gpu(deployment(), pods)

        self.assertEqual(summary["status"], "driver_mismatch")
        self.assertTrue(summary["restart_blocked"])
        self.assertTrue(summary["driver_mismatch"])
        self.assertIn("Driver/library version mismatch", str(summary["restart_block_reason"]))

    def test_ignores_old_last_state_when_current_container_is_ready(self) -> None:
        pods = {
            "items": [
                {
                    "metadata": {"name": "stream-v3-runtime-def"},
                    "status": {
                        "phase": "Running",
                        "containerStatuses": [
                            {
                                "name": "stream-engine",
                                "ready": True,
                                "state": {"running": {"startedAt": "2099-01-01T00:00:00Z"}},
                                "lastState": {
                                    "terminated": {
                                        "reason": "Error",
                                        "message": "Failed to initialize NVML: Driver/library version mismatch",
                                    }
                                },
                            }
                        ],
                    },
                }
            ]
        }

        summary = summarize_runtime_gpu(deployment(), pods)

        self.assertEqual(summary["status"], "ok")
        self.assertFalse(summary["restart_blocked"])
        self.assertFalse(summary["driver_mismatch"])


if __name__ == "__main__":
    unittest.main()
