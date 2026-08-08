from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "ops" / "host-maintenance" / "bin"))

import stream_v3_gpu_startup_gate as gate


def pod(*, scheduling_gate: bool, released_boot_id: str = "") -> dict[str, object]:
    annotations = {gate.BOOT_ANNOTATION: released_boot_id} if released_boot_id else {}
    scheduling_gates = [{"name": gate.GATE_NAME}] if scheduling_gate else []
    return {
        "metadata": {"name": "stream-v3-runtime-abc", "annotations": annotations},
        "spec": {"schedulingGates": scheduling_gates},
    }


class GpuStartupGateTests(unittest.TestCase):
    def test_streaming_manifest_keeps_gpu_gate_and_four_runtime_containers(self) -> None:
        base = (ROOT / "deploy" / "k3s" / "v3-runtime" / "deployment.yaml").read_text(encoding="utf-8")
        streaming = (ROOT / "deploy" / "k3s" / "streaming" / "kustomization.yaml").read_text(encoding="utf-8")

        self.assertIn("stream-v3.io/gpu-ready", streaming)
        for name in ("stream-engine", "precipitation-fetcher", "auto-dj"):
            self.assertIn(f"- name: {name}", base)
        self.assertIn("name: fast-recovery-loop", streaming)
        self.assertIn("/app/src/stream_core/runtime_readiness.py", streaming)

    def test_host_units_are_public_templates_without_private_checkout_paths(self) -> None:
        unit_root = ROOT / "ops" / "host-maintenance" / "systemd"
        units = "\n".join(
            path.read_text(encoding="utf-8") for path in sorted(unit_root.glob("*.service"))
        )

        self.assertNotIn("/home/yuki/", units)
        self.assertIn("/usr/local/libexec/stream-v3-gpu-startup-gate", units)
        self.assertIn("/usr/local/libexec/stream-v3-postboot-verify", units)
        self.assertIn("/usr/local/libexec/stream-v3-nvidia-driver-check", units)

    def test_gated_pod_waits_until_gpu_is_ready(self) -> None:
        self.assertEqual(gate.pod_gate_action(pod(scheduling_gate=True), boot_id="boot-2", gpu_ready=False), "wait")
        self.assertEqual(gate.pod_gate_action(pod(scheduling_gate=True), boot_id="boot-2", gpu_ready=True), "release")

    def test_previous_boot_pod_is_deleted_before_runtime_recreation(self) -> None:
        self.assertEqual(
            gate.pod_gate_action(
                pod(scheduling_gate=False, released_boot_id="boot-1"),
                boot_id="boot-2",
                gpu_ready=True,
            ),
            "delete_stale",
        )

    def test_current_boot_released_pod_is_kept(self) -> None:
        self.assertEqual(
            gate.pod_gate_action(
                pod(scheduling_gate=False, released_boot_id="boot-2"),
                boot_id="boot-2",
                gpu_ready=True,
            ),
            "keep",
        )

    def test_recovery_role_restriction_removes_only_deployment_mutation_verbs(self) -> None:
        rules = [
            {"apiGroups": [""], "resources": ["pods"], "verbs": ["get", "list", "watch"]},
            {
                "apiGroups": ["apps"],
                "resources": ["deployments", "deployments/scale"],
                "verbs": ["get", "list", "watch", "patch", "update"],
            },
        ]

        restricted = gate.restricted_role_rules(rules)

        self.assertFalse(gate.has_deployment_mutation_verbs(restricted))
        self.assertEqual(restricted[0], rules[0])
        self.assertEqual(restricted[1]["verbs"], ["get", "list", "watch"])
        self.assertTrue(gate.has_deployment_mutation_verbs(rules))

    def test_mark_pod_established_uses_current_boot_annotation(self) -> None:
        with mock.patch.object(gate, "run", return_value=mock.Mock(returncode=0)) as run:
            gate.mark_pod_established("stream-v3-runtime-abc", "boot-2")

        self.assertIn(
            f"{gate.ESTABLISHED_ANNOTATION}=boot-2",
            run.call_args.args[0],
        )


if __name__ == "__main__":
    unittest.main()
