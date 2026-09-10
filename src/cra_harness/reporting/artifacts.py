from __future__ import annotations

import json
from collections import Counter
from pathlib import Path
from typing import Any

from cra_dell_recovery.time import parse_utc
from cra_harness.classifiers.taxonomy import Classification


def _json(path: Path, value: Any) -> None:
    with path.open("x", encoding="utf-8") as handle:
        json.dump(value, handle, ensure_ascii=False, indent=2, sort_keys=True)
        handle.write("\n")


class ArtifactWriter:
    def __init__(self, artifact_root: Path, run_id: str) -> None:
        self.run_dir = artifact_root / run_id
        self.run_dir.mkdir(parents=True, exist_ok=False)
        for name in ("evidence", "failures", "negative_controls", "regressions"):
            (self.run_dir / name).mkdir()

    def write_evidence(self, outcome: dict[str, Any]) -> None:
        path = self.run_dir / "evidence" / f"{outcome['scenario_id']}.jsonl"
        with path.open("x", encoding="utf-8") as handle:
            handle.write(
                json.dumps(
                    {
                        "record_type": "bundle",
                        "run_id": outcome["run_id"],
                        "scenario_id": outcome["scenario_id"],
                        "started_at": outcome["evidence_started_at"],
                        "finished_at": outcome["evidence_finished_at"],
                    },
                    sort_keys=True,
                )
                + "\n"
            )
            for event in outcome["events"]:
                handle.write(json.dumps({"record_type": "event", **event}, sort_keys=True) + "\n")

    def write_all(
        self,
        *,
        manifest: dict[str, Any],
        outcomes: list[dict[str, Any]],
        verification: dict[str, Any],
        summary: dict[str, Any],
        report: str,
        registry_hash: str,
    ) -> None:
        for outcome in outcomes:
            self.write_evidence(outcome)
            if outcome["profile"] == "negative_control":
                _json(self.run_dir / "negative_controls" / f"{outcome['scenario_id']}.json", outcome)
            if outcome["classification"] not in {Classification.PASS.value, Classification.EXPECTED_INJECTED_FAILURE.value}:
                _json(self.run_dir / "failures" / f"{outcome['scenario_id']}.json", outcome)
        matrix = [
            {
                "scenario_id": item["scenario_id"],
                "profile": item["profile"],
                "deterministic": item["deterministic"],
                "source": item["source"],
                "expected": item["expected"],
                "classification": item["classification"],
            }
            for item in outcomes
        ]
        classifications = [
            {
                "scenario_id": item["scenario_id"],
                "classification": item["classification"],
                "reason_codes": item["reason_codes"],
                "matches_expectation": item["matches_expectation"],
            }
            for item in outcomes
        ]
        _json(self.run_dir / "manifest.json", manifest)
        _json(self.run_dir / "matrix.json", matrix)
        _json(self.run_dir / "classification.json", classifications)
        _json(
            self.run_dir / "regressions" / "index.json",
            {
                "registry_sha256": registry_hash,
                "promotion_rule": "再現可能な探索caseを、由来・seed・最小fixture・期待不変条件付きでdeterministic registryへ昇格する",
                "promoted_in_this_run": [
                    {
                        "regression_id": "REG-HARNESS-2026-08-23-001",
                        "fixture": "tests/fixtures/regressions/2026-08-23_manifest_git_identity.json",
                        "origin_run_id": "20260823T_harness_engineering_v1",
                    },
                    {
                        "regression_id": "REG-HARNESS-2026-08-23-002",
                        "fixture": "tests/fixtures/regressions/2026-08-23_manifest_start_order.json",
                        "origin_run_id": "20260823T_harness_engineering_v4",
                    },
                ],
            },
        )
        _json(self.run_dir / "verification.json", verification)
        _json(self.run_dir / "summary.json", summary)
        with (self.run_dir / "report.md").open("x", encoding="utf-8") as handle:
            handle.write(report)


def verify_artifact_consistency(run_dir: Path) -> tuple[bool, tuple[str, ...]]:
    errors: list[str] = []
    try:
        manifest = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))
        summary = json.loads((run_dir / "summary.json").read_text(encoding="utf-8"))
        matrix = json.loads((run_dir / "matrix.json").read_text(encoding="utf-8"))
        classifications = json.loads((run_dir / "classification.json").read_text(encoding="utf-8"))
        verification = json.loads((run_dir / "verification.json").read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        return False, (f"ARTIFACT_READ_ERROR:{type(error).__name__}",)
    matrix_ids = [item["scenario_id"] for item in matrix]
    classification_ids = [item["scenario_id"] for item in classifications]
    evidence_ids = sorted(path.stem for path in (run_dir / "evidence").glob("*.jsonl"))
    if sorted(matrix_ids) != sorted(classification_ids) or sorted(matrix_ids) != evidence_ids:
        errors.append("SCENARIO_ID_SET_MISMATCH")
    observed_counts = Counter(item["classification"] for item in classifications)
    counts = {item.value: observed_counts.get(item.value, 0) for item in Classification}
    if counts != summary.get("classification_counts"):
        errors.append("CLASSIFICATION_COUNT_MISMATCH")
    if len(matrix) != summary.get("scenario_count"):
        errors.append("SCENARIO_COUNT_MISMATCH")
    if sum(counts.values()) != summary.get("total"):
        errors.append("CLASSIFICATION_TOTAL_MISMATCH")
    profile_counts = dict(sorted(Counter(item["profile"] for item in matrix).items()))
    if profile_counts != summary.get("profile_counts"):
        errors.append("PROFILE_COUNT_MISMATCH")
    if any(not item.get("classification") for item in classifications):
        errors.append("NULL_CLASSIFICATION")
    if manifest.get("run_id") != summary.get("run_id"):
        errors.append("RUN_ID_MISMATCH")
    try:
        manifest_started = parse_utc(str(manifest["started_at"]))
        manifest_finished = parse_utc(str(manifest["finished_at"]))
        verification_started = parse_utc(str(verification["started_at"]))
        verification_finished = parse_utc(str(verification["finished_at"]))
        if not manifest_started <= verification_started <= verification_finished <= manifest_finished:
            errors.append("RUN_TIMELINE_MISMATCH")
    except (KeyError, TypeError, ValueError):
        errors.append("RUN_TIMELINE_MISSING")
    if len(matrix_ids) != len(set(matrix_ids)):
        errors.append("DUPLICATE_SCENARIO_ID")
    return not errors, tuple(errors)
