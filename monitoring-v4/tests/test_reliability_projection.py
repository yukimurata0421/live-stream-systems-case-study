from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from stream_contracts.monitoring_v4.time import utc_text
from stream_contracts.monitoring_v4.sli import SLIProjection
from stream_monitoring_v4.compatibility.public_safe import (
    public_safe_projection,
    validate_public_safe_projection,
)
from stream_monitoring_v4.reliability.projector import ReliabilityProjector
from stream_monitoring_v4.storage.repository import MonitoringRepository

from tests.helpers import BASE_TS, write_json


class ReliabilityProjectionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.formal = self.root / "operational_reliability_status.json"
        self.fast = self.root / "operational_reliability_burn_status.json"
        self.at = utc_text(BASE_TS)
        self.repository = MonitoringRepository(self.root / "v4" / "monitoring.sqlite3")
        self.repository.initialize(applied_at=self.at)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def _write_sources(self) -> None:
        gates = {}
        for name in (
            "youtube_availability",
            "same_url_preservation",
            "upload_ceiling",
            "youtube_input_quality",
            "audio_correctness",
        ):
            gates[name] = {
                "compliance_status": "met",
                "measurement_status": "valid",
                "measurement_unknown_reasons": [],
                "sli_pct": 100.0,
                "coverage_pct": 100.0,
                "source_freshness_pct": 100.0,
                "source_disagreement": False,
            }
        write_json(
            self.formal,
            {
                "checked_at_utc": self.at,
                "no_automatic_recovery": True,
                "formal_gates": gates,
                "database": "/must/not/leave/adapter.sqlite3",
                "row_counts": {"secret-like-id": 1},
            },
        )
        write_json(
            self.fast,
            {
                "checked_at_utc": self.at,
                "no_automatic_recovery": True,
                "fast_feedback": {
                    "youtube_input_quality": {
                        "measurement_status": "valid",
                        "measurement_unknown_reasons": [],
                        "target_met_on_observed_samples": True,
                        "sli_pct": 100.0,
                        "coverage_pct": 100.0,
                        "source_freshness_pct": 100.0,
                        "source_disagreement": False,
                    }
                },
                "multi_window_burn_alerts": [],
            },
        )

    def test_formal_and_fast_are_separate_and_persisted_without_source_mutation(self) -> None:
        self._write_sources()
        before = {
            path: (path.read_bytes(), path.stat().st_mtime_ns)
            for path in (self.formal, self.fast)
        }
        result = ReliabilityProjector(
            self.repository,
            formal_status_file=self.formal,
            burn_status_file=self.fast,
        ).run_once(received_at=self.at)
        self.assertEqual(len(result.projections), 6)
        self.assertEqual(result.inserted, 6)
        self.assertEqual(result.rejections, ())
        formal = [item for item in result.projections if item.assessment_scope == "formal"]
        fast = [item for item in result.projections if item.assessment_scope == "fast"]
        self.assertEqual(len(formal), 5)
        self.assertEqual(len(fast), 1)
        self.assertTrue(all(item.is_official_window for item in formal))
        self.assertEqual({item.compliance_status for item in formal}, {"met"})
        self.assertEqual(fast[0].compliance_status, "unknown")
        self.assertTrue(all(item.no_automatic_recovery for item in result.projections))
        self.assertEqual(len(self.repository.current_sli_projections()), 6)
        selected = next(
            item
            for item in result.projections
            if item.objective_id == "youtube_input_quality" and item.assessment_scope == "formal"
        )
        older = SLIProjection.create(
            objective_id=selected.objective_id,
            window=selected.window,
            assessment_scope=selected.assessment_scope,
            is_official_window=True,
            observed=None,
            eligible=None,
            bad=None,
            missing=None,
            coverage_pct=99,
            source_freshness_pct=99,
            source_disagreement=False,
            compliance_status="met",
            measurement_unknown_reasons=(),
            window_start=utc_text(BASE_TS - 8 * 86400),
            window_end=utc_text(BASE_TS - 86400),
            evaluated_at=self.at,
            policy_revision="test-older-r6",
            evidence_ids=selected.evidence_ids,
            payload={"sli_pct": 99.0},
        )
        self.repository.save_sli_projection(older)
        current = next(
            item
            for item in self.repository.current_sli_projections()
            if item.objective_id == "youtube_input_quality" and item.assessment_scope == "formal"
        )
        self.assertEqual(current.projection_id, selected.projection_id)
        rendered = repr([item.payload for item in result.projections])
        self.assertNotIn("must/not/leave", rendered)
        self.assertNotIn("secret-like-id", rendered)
        for path, (content, mtime_ns) in before.items():
            self.assertEqual(path.read_bytes(), content)
            self.assertEqual(path.stat().st_mtime_ns, mtime_ns)

    def test_missing_no_automatic_recovery_boundary_rejects_projection(self) -> None:
        self._write_sources()
        payload = {
            "checked_at_utc": self.at,
            "formal_gates": {},
            "no_automatic_recovery": False,
        }
        write_json(self.formal, payload)
        result = ReliabilityProjector(
            self.repository,
            formal_status_file=self.formal,
            burn_status_file=self.fast,
        ).run_once(received_at=self.at)
        self.assertEqual(len(result.projections), 1)
        self.assertIn(
            "reliability_recovery_boundary_invalid",
            {item.reason_code for item in result.rejections},
        )

    def test_stale_sources_cannot_refresh_projection_or_public_artifact(self) -> None:
        self._write_sources()
        first = ReliabilityProjector(
            self.repository,
            formal_status_file=self.formal,
            burn_status_file=self.fast,
        ).run_once(received_at=self.at)
        self.assertEqual(len(first.projections), 6)

        stale_at = utc_text(BASE_TS + 2 * 3600 + 1)
        stale = ReliabilityProjector(
            self.repository,
            formal_status_file=self.formal,
            burn_status_file=self.fast,
        ).run_once(received_at=stale_at)
        self.assertEqual(stale.projections, ())
        self.assertEqual(
            {item.reason_code for item in stale.rejections},
            {"reliability_source_stale"},
        )
        self.assertEqual(len(self.repository.current_sli_projections()), 6)

    def test_fresh_but_older_source_cannot_roll_back_returned_projection(self) -> None:
        self._write_sources()
        projector = ReliabilityProjector(
            self.repository,
            formal_status_file=self.formal,
            burn_status_file=self.fast,
        )
        projector.run_once(received_at=self.at)

        newer_at = utc_text(BASE_TS + 600)
        formal = json.loads(self.formal.read_text(encoding="utf-8"))
        formal["checked_at_utc"] = newer_at
        for gate in formal["formal_gates"].values():
            gate["sli_pct"] = 98.0
        write_json(self.formal, formal)
        fast = json.loads(self.fast.read_text(encoding="utf-8"))
        fast["checked_at_utc"] = newer_at
        fast["fast_feedback"]["youtube_input_quality"]["sli_pct"] = 98.0
        write_json(self.fast, fast)
        newer = projector.run_once(received_at=newer_at)
        self.assertEqual({item.window_end for item in newer.projections}, {newer_at})
        self.assertEqual({item.payload["sli_pct"] for item in newer.projections}, {98.0})

        self._write_sources()
        rolled_back = projector.run_once(received_at=newer_at)
        self.assertEqual({item.window_end for item in rolled_back.projections}, {newer_at})
        self.assertEqual(
            {item.payload["sli_pct"] for item in rolled_back.projections},
            {98.0},
        )

    def test_public_shadow_projection_has_fixed_allowlist_and_no_internal_ids(self) -> None:
        self._write_sources()
        result = ReliabilityProjector(
            self.repository,
            formal_status_file=self.formal,
            burn_status_file=self.fast,
        ).run_once(received_at=self.at)
        payload = public_safe_projection(result.projections, generated_at=self.at)
        validate_public_safe_projection(payload)
        rendered = repr(payload)
        self.assertNotIn("projection_id", rendered)
        self.assertNotIn("evidence_id", rendered)
        self.assertNotIn("payload_sha256", rendered)
        self.assertNotIn("compliance_status", rendered)
        self.assertTrue(all(item["no_automatic_recovery"] for item in payload["items"]))


if __name__ == "__main__":
    unittest.main()
