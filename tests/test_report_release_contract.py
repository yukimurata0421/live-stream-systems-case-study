from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from ops.scripts.verify_report_release_contract import ContractError, verify_release


ROOT = Path(__file__).resolve().parents[1]


class ReportReleaseContractTests(unittest.TestCase):
    def test_repository_release_contract_passes_without_physical_probe(self) -> None:
        result = verify_release(ROOT, ROOT / "ops" / "systemd")

        self.assertEqual(result["result"], "PASS")
        self.assertFalse(result["production_behavior_modified"])
        self.assertEqual(len(result["checks"]), 2)
        self.assertTrue(all(not check["physical_probe_executed"] for check in result["checks"]))

    def test_unit_without_explicit_record_target_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            unit_root = Path(td)
            for unit in (
                "adsb-streamnew-stream1090-report.service",
                "adsb-streamnew-upstream-report.service",
            ):
                source = (ROOT / "ops" / "systemd" / unit).read_text(encoding="utf-8")
                (unit_root / unit).write_text(source.replace(" --record", ""), encoding="utf-8")

            with self.assertRaisesRegex(ContractError, "explicit record target"):
                verify_release(ROOT, unit_root)


if __name__ == "__main__":
    unittest.main()
