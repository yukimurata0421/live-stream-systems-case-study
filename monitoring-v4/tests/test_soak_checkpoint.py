from __future__ import annotations

import importlib.util
import tempfile
import unittest
from pathlib import Path

from stream_contracts.monitoring_v4.ids import stable_id
from stream_contracts.monitoring_v4.time import utc_text
from stream_monitoring_v4.storage.repository import MonitoringRepository


ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "ops/scripts/monitoring_v4_soak_checkpoint.py"
SPEC = importlib.util.spec_from_file_location("monitoring_v4_soak_checkpoint", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
module = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(module)


class SoakCheckpointTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.repository = MonitoringRepository(Path(self.temp.name) / "monitoring.sqlite3")
        self.repository.initialize(applied_at="2026-08-26T00:00:00Z")
        self.epoch = 1_777_334_400
        self.build_revision = "a" * 40
        self.source_revision = "source-release@" + "b" * 40

    def tearDown(self) -> None:
        self.temp.cleanup()

    def _append_clean_cycle(self, timestamp: int) -> None:
        self.repository.append_shadow_cycle(
            cycle_id=stable_id("cycle", timestamp),
            started_at=utc_text(timestamp),
            completed_at=utc_text(timestamp + 1),
            build_revision=self.build_revision,
            source_revision=self.source_revision,
            observer={"source_results": []},
            current_states={},
            parity={
                "schema": "monitoring_v4.parity.v1",
                "equivalent": True,
                "accepted_difference_count": 0,
                "unclassified_contract_difference_count": 0,
                "projection_integrity": {
                    "complete": True,
                    "projection_count": 14,
                    "expected_count": 14,
                    "rejection_count": 0,
                },
            },
            notification_intent_count=0,
        )

    def test_new_epoch_excludes_older_revision_and_is_not_yet_eligible(self) -> None:
        self._append_clean_cycle(self.epoch)
        report = module.build_soak_checkpoint(
            self.repository,
            epoch_start_ts=self.epoch,
            build_revision=self.build_revision,
            source_revision=self.source_revision,
            now_ts=self.epoch + 120,
        )
        self.assertEqual(report["decision"], "SOAK_CONTINUE")
        self.assertTrue(report["epoch"]["anchored_within_one_cycle"])
        self.assertEqual(report["parity_epoch"]["cycles"], 1)
        self.assertEqual(
            {item["status"] for item in report["checkpoints"]},
            {"NOT_YET_ELIGIBLE"},
        )
        self.assertTrue(report["safety_boundary"]["database_mutation_enabled"] is False)

    def test_missing_epoch_rows_remains_evidence_pending(self) -> None:
        report = module.build_soak_checkpoint(
            self.repository,
            epoch_start_ts=self.epoch,
            build_revision=self.build_revision,
            source_revision=self.source_revision,
            now_ts=self.epoch + 120,
        )
        self.assertEqual(report["decision"], "EVIDENCE_PENDING")
        self.assertFalse(report["epoch"]["anchored_within_one_cycle"])

    def test_unknown_source_revision_invalidates_identity(self) -> None:
        report = module.build_soak_checkpoint(
            self.repository,
            epoch_start_ts=self.epoch,
            build_revision=self.build_revision,
            source_revision="unknown-source-revision",
            now_ts=self.epoch + 120,
        )
        self.assertEqual(report["decision"], "INVALID_REVISION_IDENTITY")
        self.assertFalse(report["identity"]["revision_gate"])


if __name__ == "__main__":
    unittest.main()
