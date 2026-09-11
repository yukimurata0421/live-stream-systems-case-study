from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "ops/host-maintenance/bin/stream_v3_postboot_verify.py"
SPEC = importlib.util.spec_from_file_location("stream_v3_postboot_verify", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
module = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(module)


def containers(*, ready: bool = True) -> list[dict[str, object]]:
    return [
        {"name": name, "ready": ready, "restart_count": 0, "state": "running"}
        for name in module.EXPECTED_RUNTIME_CONTAINERS
    ]


class RuntimeContainerContractTests(unittest.TestCase):
    def test_accepts_exact_four_container_runtime_contract(self) -> None:
        self.assertEqual(module.runtime_container_contract_failure(containers()), "")

    def test_rejects_missing_fast_recovery_container(self) -> None:
        sample = [
            item for item in containers() if item["name"] != "fast-recovery-loop"
        ]
        failure = module.runtime_container_contract_failure(sample)
        self.assertIn("missing=fast-recovery-loop", failure)

    def test_rejects_unexpected_or_unready_container(self) -> None:
        sample = containers()
        sample[0]["ready"] = False
        sample.append(
            {"name": "unknown-sidecar", "ready": True, "restart_count": 0, "state": "running"}
        )
        failure = module.runtime_container_contract_failure(sample)
        self.assertIn("unexpected=unknown-sidecar", failure)
        self.assertIn("not_ready=auto-dj", failure)


if __name__ == "__main__":
    unittest.main()
