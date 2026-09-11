from __future__ import annotations

import json
import unittest
from pathlib import Path

from tests.historical_replay_harness import (
    FIXTURE_SCHEMA,
    ReplayHarnessError,
    assert_sanitized_fixture,
    build_artifact,
    load_fixture,
    replay_fixture,
)
from tests.integration.historical_dump_extractor import (
    EXPECTED_DUMP_SHA256,
    HISTORICAL_BUILD,
    HISTORICAL_SOURCE,
    _fixture_from_rows,
)


FIXTURE = Path(__file__).parent / "fixtures/monitoring_v4/2026-08-13_runtime_rollout_observation_baseline.json"


class HistoricalReplayHarnessTests(unittest.TestCase):
    def setUp(self) -> None:
        self.fixture = load_fixture(FIXTURE)

    def test_schema_and_provenance_contract(self) -> None:
        self.assertEqual(self.fixture["schema"], FIXTURE_SCHEMA)
        self.assertEqual(self.fixture["provenance"]["dump_sha256"], EXPECTED_DUMP_SHA256)
        self.assertEqual(self.fixture["provenance"]["historical_revision"]["build"], HISTORICAL_BUILD)
        self.assertEqual(self.fixture["provenance"]["historical_revision"]["source"], HISTORICAL_SOURCE)

    def test_fixture_has_no_sensitive_leakage(self) -> None:
        assert_sanitized_fixture(self.fixture)

    def test_extractor_contract_preserves_only_observation_boundary(self) -> None:
        rows = [
            {"domain": domain, "source": f"source_{domain}", "evidence_role": "current_authoritative", "state": state, "reason_code": f"reason_{state}", "observed_at": stamp, "received_at": stamp, "freshness_limit_sec": 180, "producer_revision": "producer-r1", "payload_json": json.dumps({"boolean": True, "numeric": 2, "secret": "drop", "video_id": "drop"})}
            for domain, state, stamp in (("delivery", "good", "2026-08-12T16:48:32Z"), ("rendering", "good", "2026-08-12T16:48:32Z"), ("rendering", "bad", "2026-08-12T16:49:51Z"), ("delivery", "bad", "2026-08-12T16:50:12Z"), ("delivery", "good", "2026-08-12T16:50:58Z"), ("rendering", "good", "2026-08-12T16:50:58Z"))
        ]
        fixture = _fixture_from_rows(rows, dump_sha256=EXPECTED_DUMP_SHA256)
        self.assertEqual(fixture["observations"][0]["payload"], {"boolean": True, "numeric": 2})
        assert_sanitized_fixture(fixture)

    def test_baseline_replay_is_exact_match(self) -> None:
        result = replay_fixture(self.fixture, mode="baseline")
        self.assertEqual(result["semantic_difference"]["classification"], "exact_match")
        self.assertEqual(len(result["baseline"]["episodes"]), 2)
        self.assertEqual(len(result["baseline"]["transitions"]), 4)
        self.assertEqual(len(result["baseline"]["intents"]), 6)

    def test_old_bad_after_reopen_does_not_regress_current(self) -> None:
        result = replay_fixture(self.fixture, mode="duplicate_old_bad")
        perturbation = result["perturbation"]
        self.assertIsNotNone(perturbation)
        assert perturbation is not None
        self.assertEqual(perturbation["classification"], "pass")
        self.assertEqual(perturbation["suppression"], "duplicate_observation_id")
        self.assertTrue(all(perturbation["invariants"].values()))

    def test_new_identity_delayed_old_bad_cannot_regress_current(self) -> None:
        result = replay_fixture(self.fixture, mode="delayed_old_bad")
        perturbation = result["perturbation"]
        assert perturbation is not None
        self.assertEqual(perturbation["classification"], "pass")
        self.assertTrue(perturbation["append_accepted"])
        self.assertEqual(perturbation["suppression"], "older_observed_at_canonical_non_regression")
        self.assertTrue(all(perturbation["invariants"].values()))

    def test_delivery_recovery_drop_fails_closed(self) -> None:
        result = replay_fixture(self.fixture, mode="recovery_drop")
        perturbation = result["perturbation"]
        assert perturbation is not None
        self.assertEqual(perturbation["classification"], "pass")
        self.assertEqual(perturbation["suppression"], "recovery_evidence_absent_fail_closed")
        self.assertTrue(all(perturbation["invariants"].values()))

    def test_artifact_contains_reproduction_command(self) -> None:
        artifact = build_artifact(FIXTURE, mode="delayed_old_bad")
        self.assertEqual(artifact["classification"], "pass")
        self.assertIn("--mode delayed_old_bad", artifact["reproduction_command"])


if __name__ == "__main__":
    unittest.main()
