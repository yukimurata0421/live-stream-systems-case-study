from __future__ import annotations

import json
from pathlib import Path

from cra_harness.coverage_closure import run_coverage_closure

ROOT = Path(__file__).resolve().parents[3]


def test_coverage_closure_writes_revision_bound_non_effect_artifacts(tmp_path: Path) -> None:
    report = run_coverage_closure(ROOT, tmp_path / "coverage", seeds=(20260911,), cases_per_seed=4)

    assert report["classification"] == "PASS"
    assert report["scenario_count"] == 2 + 127 + 8 + 4 + 8 + 8
    assert report["negative_control_detection_rate"] == 1.0
    assert report["safety"]["physical_effect_count"] == 0
    assert report["runtime_versions"]["sqlite"] == "3.51.3"
    stored = json.loads((tmp_path / "coverage/manifest.json").read_text(encoding="utf-8"))
    assert stored["artifact_sha256"] == report["artifact_sha256"]
