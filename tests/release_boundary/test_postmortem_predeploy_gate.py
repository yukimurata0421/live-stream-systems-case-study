from __future__ import annotations

import gzip
import hashlib
import io
import json
import tarfile
from pathlib import Path
from typing import Any

import pytest

from tools import evaluate_postmortem_predeploy_gate as predeploy
from tools.evaluate_postmortem_predeploy_gate import evaluate
from tools.freeze_recovery_soak_checkpoint import MAXIMUM_CHECKPOINT_BYTES
from tools.run_candidate_full_validation import COVERAGE_FILES
from tools.run_postmortem_io_harness import EXPECTED_IDS

COMMIT = "a" * 40
RELEASE_FILES = {
    "src/cra_dell_recovery/json_input.py",
    "src/cra_dell_recovery/bounded_http.py",
    "src/cra_no_action_soak/recovery_facts.py",
    "src/cra_no_action_soak/recovery_transport.py",
    "src/cra_no_action_soak/operator_status.py",
    "src/cra_no_action_soak/recovery_soak.py",
    "src/cra_no_action_soak/resilient_soak.py",
    "src/cra_no_action_soak/recovery_observer.py",
    "src/cra_no_action_soak/recovery_live.py",
    "src/cra_no_action_soak/recovery_window.py",
    "src/cra_dell_recovery/recovery_history.py",
}


def _write(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, sort_keys=True) + "\n", encoding="utf-8")


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _hash_artifacts(root: Path) -> None:
    _write(
        root / "artifact_hashes.json",
        {path.name: _sha(path) for path in root.iterdir() if path.is_file() and path.name != "artifact_hashes.json"},
    )


def _release_fixture(manifest_path: Path, archive_path: Path) -> dict[str, str]:
    payloads = {relative: f"test payload for {relative}\n".encode() for relative in RELEASE_FILES}
    files = {
        relative: {
            "sha256": hashlib.sha256(raw).hexdigest(),
            "size": len(raw),
            "executable": False,
            "origin": "tracked_source",
        }
        for relative, raw in payloads.items()
    }
    tree = hashlib.sha256(b"".join(f"{relative}\0{files[relative]['sha256']}\0".encode() for relative in sorted(files))).hexdigest()
    release_id = "cra-no-action-test-v1"
    manifest = {
        "schema": "cra.no_action_release_manifest.v1",
        "release_id": release_id,
        "source_commit": COMMIT,
        "source_tree_sha256": tree,
        "operating_mode": "NO_ACTION",
        "production_action_enabled": False,
        "control_capability_count": 0,
        "physical_effect_count": 0,
        "central_command_store_packaged": False,
        "effect_adapter_packaged": False,
        "dell_agent_packaged": False,
        "monitoring_private_key_packaged": False,
        "file_count": len(files),
        "files": files,
        "excluded_runtime_state": True,
        "secret_values_recorded": False,
    }
    _write(manifest_path, manifest)
    prefix = f"stream-recovery-control-{release_id}"
    tar_buffer = io.BytesIO()
    with tarfile.open(fileobj=tar_buffer, mode="w", format=tarfile.PAX_FORMAT) as archive:
        members = {**payloads, "release_manifest.json": manifest_path.read_bytes()}
        for relative, raw in sorted(members.items()):
            info = tarfile.TarInfo(f"{prefix}/{relative}")
            info.size = len(raw)
            info.mode = 0o644
            info.mtime = 0
            info.uid = 0
            info.gid = 0
            info.uname = "root"
            info.gname = "root"
            archive.addfile(info, io.BytesIO(raw))
    with archive_path.open("wb") as raw, gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=0) as compressed:
        compressed.write(tar_buffer.getvalue())
    return {relative: metadata["sha256"] for relative, metadata in files.items()}


@pytest.fixture
def evidence(tmp_path: Path) -> dict[str, Path]:
    checkpoint = tmp_path / "checkpoint"
    for name, raw in (("config.json", b"{}\n"), ("state.json", b"{}\n"), ("samples.committed-prefix.jsonl", b"{}\n")):
        path = checkpoint / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(raw)
    identity_entries = {}
    for label in ("runtime", "public_key_dell", "public_key_arena", "public_key_cra"):
        identity_artifact = checkpoint / f"identity-{label}.bin"
        identity_artifact.write_bytes(label.encode())
        identity_entries[label] = {
            "source_path": str((tmp_path / f"{label}.json").resolve()),
            "artifact": identity_artifact.name,
            "sha256": _sha(identity_artifact),
            "size": identity_artifact.stat().st_size,
        }
    _write(
        checkpoint / "checkpoint.json",
        {
            "schema": "cra.recovery_soak_24h_checkpoint.v1",
            "classification": "24H_OBSERVATION_FROZEN",
            "epoch_id": "historical-epoch",
            "minimum_observation_seconds": 86400,
            "candidate_soak_reusable_seconds": 0,
            "candidate_identity_validated_by_this_checkpoint": False,
            "identity_files": identity_entries,
            "sha256": {
                name: _sha(checkpoint / name)
                for name in (
                    "config.json",
                    "state.json",
                    "samples.committed-prefix.jsonl",
                    *(item["artifact"] for item in identity_entries.values()),
                )
            },
            "evaluation": {
                "harness_classification": "PASS",
                "live_health": "READY",
                "verified_duration_seconds": 86400,
                "maximum_sample_gap_seconds": 45,
                "blockers": [],
                "unknown_reasons": [],
            },
        },
    )

    release_manifest = tmp_path / "release.manifest.json"
    archive = tmp_path / "release.tar.gz"
    release_hashes = _release_fixture(release_manifest, archive)

    postmortem = tmp_path / "postmortem"
    _write(
        postmortem / "summary.json",
        {
            "schema": "cra.postmortem_io_harness_summary.v1",
            "trusted": True,
            "deployable_clean_identity": True,
            "source_stable": True,
            "scenario_results": [
                {
                    "scenario_id": scenario_id,
                    "classification": "PASS",
                    "collected_test_count": 1,
                    "fault_reachability_asserted_by_tests": True,
                    "negative_control_detected": True,
                    "junit": f"{scenario_id.lower()}.xml",
                    "junit_counts": {"tests": 1, "failures": 0, "errors": 0, "skipped": 0},
                }
                for scenario_id in EXPECTED_IDS
            ],
        },
    )
    _write(
        postmortem / "manifest.json",
        {
            "git_head": COMMIT,
            "git_status_porcelain_sha256": hashlib.sha256(b"").hexdigest(),
            "input_sha256": release_hashes,
        },
    )
    for scenario_id in EXPECTED_IDS:
        (postmortem / f"{scenario_id.lower()}.xml").write_text(
            '<testsuite tests="1" failures="0" errors="0" skipped="0"/>\n', encoding="utf-8"
        )
    (postmortem / "empty.stderr.txt").write_bytes(b"")
    _hash_artifacts(postmortem)

    full = tmp_path / "full"
    _write(
        full / "summary.json",
        {
            "schema": "cra.candidate_full_validation.v1",
            "trusted": True,
            "deployable_clean_identity": True,
            "source_stable": True,
            "branch_coverage_measured": True,
            "missing_coverage_files": [],
            "coverage_files": list(COVERAGE_FILES),
            "git_head": COMMIT,
            "git_status_porcelain_sha256": hashlib.sha256(b"").hexdigest(),
            "junit_counts": {"tests": 100, "failures": 0, "errors": 0, "skipped": 0},
        },
    )
    (full / "full.xml").write_text('<testsuite tests="100" failures="0" errors="0" skipped="0"/>\n', encoding="utf-8")
    _write(
        full / "coverage.json",
        {
            "meta": {"branch_coverage": True},
            "files": {relative: {"summary": {"num_branches": 1}} for relative in COVERAGE_FILES},
        },
    )
    _write(full / "source-before.json", release_hashes)
    _hash_artifacts(full)
    return {
        "checkpoint": checkpoint,
        "postmortem": postmortem,
        "full": full,
        "release_manifest": release_manifest,
        "archive": archive,
    }


def _evaluate(paths: dict[str, Path]) -> dict[str, Any]:
    return evaluate(
        paths["checkpoint"],
        paths["postmortem"],
        paths["full"],
        paths["release_manifest"],
        paths["archive"],
    )


@pytest.mark.parametrize(
    "reason",
    [
        "TARGET_BOUND_EFFECT_EVIDENCE_GAP",
        "OWNER_TARGET_READ_CLOCK_RACE",
        "OWNER_LIFECYCLE_EVIDENCE_GAP",
        "OWNER_OBSERVATION_UNKNOWN_WITHOUT_DIAGNOSTIC",
        "OWNER_PROC_ESRCH_READ_RACE",
        "OWNER_UNREAPED_EXIT_AND_EMPTY_READ",
        "OWNER_REGISTERED_CHILD_REAP_RACE",
        "OWNER_AUXILIARY_SCHEDULER_STATE_CLASSIFICATION",
    ],
)
@pytest.mark.parametrize(
    "fault",
    [None, "epoch", "hash", "reuse", "approval", "blocker", "oracle", "other-unknown", "wrong-reason", "bool-reuse", "int-approval"],
)
def test_diagnostic_disposition_preserves_unknown_and_requires_exact_binding(
    evidence: dict[str, Path], tmp_path: Path, fault: str | None, reason: str
) -> None:
    path = evidence["checkpoint"] / "checkpoint.json"
    checkpoint = json.loads(path.read_text())
    checkpoint["classification"] = "EVIDENCE_FROZEN_FOR_DIAGNOSIS"
    checkpoint["evaluation"].update(
        harness_classification="MISSING_EVIDENCE",
        formal_evaluation_performed=False,
        soak_status=None,
        verified_duration_seconds=78000,
        unknown_reasons={
            "TARGET_BOUND_EFFECT_EVIDENCE_GAP": ["DELL_EFFECT_EVIDENCE_MISSING_OR_STALE"],
            "OWNER_TARGET_READ_CLOCK_RACE": [
                "DELL_ACTIVATION_EVIDENCE_UNKNOWN_OUTSIDE_RECOVERY",
                "RECOVERY_SOURCE_GAP_OUTSIDE_KNOWN_OUTAGE",
            ],
            "OWNER_OBSERVATION_UNKNOWN_WITHOUT_DIAGNOSTIC": [
                "DELL_OWNER_LIFECYCLE_UNKNOWN_OUTSIDE_RECOVERY",
                "RECOVERY_SOURCE_GAP_OUTSIDE_KNOWN_OUTAGE",
            ],
            "OWNER_PROC_ESRCH_READ_RACE": [
                "DELL_OWNER_LIFECYCLE_UNKNOWN_OUTSIDE_RECOVERY",
                "RECOVERY_SOURCE_GAP_OUTSIDE_KNOWN_OUTAGE",
            ],
            "OWNER_UNREAPED_EXIT_AND_EMPTY_READ": [
                "DELL_OWNER_LIFECYCLE_UNKNOWN_OUTSIDE_RECOVERY",
                "RECOVERY_SOURCE_GAP_OUTSIDE_KNOWN_OUTAGE",
            ],
            "OWNER_REGISTERED_CHILD_REAP_RACE": [
                "DELL_OWNER_LIFECYCLE_UNKNOWN_OUTSIDE_RECOVERY",
                "RECOVERY_SOURCE_GAP_OUTSIDE_KNOWN_OUTAGE",
            ],
            "OWNER_AUXILIARY_SCHEDULER_STATE_CLASSIFICATION": [
                "DELL_OWNER_LIFECYCLE_UNKNOWN_OUTSIDE_RECOVERY",
                "RECOVERY_SOURCE_GAP_OUTSIDE_KNOWN_OUTAGE",
            ],
            "OWNER_LIFECYCLE_EVIDENCE_GAP": [
                "DELL_ACTIVATION_EVIDENCE_UNKNOWN_OUTSIDE_RECOVERY",
                "RECOVERY_SOURCE_GAP_OUTSIDE_KNOWN_OUTAGE",
                "SOURCE_EVIDENCE_UNKNOWN_OUTSIDE_KNOWN_OUTAGE",
            ],
        }[reason],
        oracle_errors=[],
    )
    if fault in ("blocker", "oracle", "other-unknown"):
        field = {"blocker": "blockers", "oracle": "oracle_errors", "other-unknown": "unknown_reasons"}[fault]
        checkpoint["evaluation"][field] = ["UNREVIEWED_FAILURE"]
    _write(path, checkpoint)
    assert _evaluate(evidence)["status"] == "HOLD"
    disposition = {
        "schema": "cra.recovery_epoch_disposition.v1",
        "epoch_id": checkpoint["epoch_id"],
        "checkpoint_sha256": _sha(path),
        "old_epoch_status": "UNCERTIFIED_MISSING_EVIDENCE",
        "reason": reason,
        "raw_evidence_preserved": True,
        "replacement_requested": True,
        "reusable_seconds": 0,
    }
    if fault == "epoch":
        disposition["epoch_id"] = "other-epoch"
    elif fault == "hash":
        disposition["checkpoint_sha256"] = "0" * 64
    elif fault == "reuse":
        disposition["reusable_seconds"] = 1
    elif fault == "approval":
        disposition["replacement_requested"] = False
    elif fault == "wrong-reason":
        disposition["reason"] = (
            "OWNER_TARGET_READ_CLOCK_RACE" if reason == "TARGET_BOUND_EFFECT_EVIDENCE_GAP" else "TARGET_BOUND_EFFECT_EVIDENCE_GAP"
        )
    elif fault == "bool-reuse":
        disposition["reusable_seconds"] = False
    elif fault == "int-approval":
        disposition["replacement_requested"] = 1
    record = tmp_path / "disposition.json"
    _write(record, disposition)
    result = evaluate(
        evidence["checkpoint"],
        evidence["postmortem"],
        evidence["full"],
        evidence["release_manifest"],
        evidence["archive"],
        superseded_epoch_disposition=record,
    )
    assert result["status"] == ("READY_FOR_EXPLICIT_DEPLOYMENT_REVIEW" if fault is None else "HOLD")
    assert result["deployment_authorized"] is False
    assert result["checkpoint"]["candidate_soak_reusable_seconds"] == 0
    assert json.loads(path.read_text()) == checkpoint


def test_complete_evidence_is_ready_for_review_but_never_authorizes_deploy(evidence: dict[str, Path]) -> None:
    result = _evaluate(evidence)
    assert result["status"] == "READY_FOR_EXPLICIT_DEPLOYMENT_REVIEW"
    assert result["deployment_authorized"] is False
    assert result["checkpoint"]["candidate_soak_reusable_seconds"] == 0
    assert result["next_epoch"] == {"required": True, "initial_reusable_seconds": 0, "minimum_formal_duration_seconds": 604800}


def test_recovery_window_must_match_the_tested_release(evidence: dict[str, Path]) -> None:
    path = evidence["full"] / "source-before.json"
    sources = json.loads(path.read_text())
    sources["src/cra_no_action_soak/recovery_window.py"] = "0" * 64
    _write(path, sources)
    _hash_artifacts(evidence["full"])
    result = _evaluate(evidence)
    assert result["status"] == "HOLD"
    assert "CANDIDATE_VALIDATION_IDENTITY_MISMATCH" in result["blockers"]
    assert result["checkpoint"]["candidate_soak_reusable_seconds"] == 0
    assert result["next_epoch"] == {"required": True, "initial_reusable_seconds": 0, "minimum_formal_duration_seconds": 604800}


@pytest.mark.parametrize(
    "source,count,accepted", [("coverage.json", 40000, True), ("coverage.json", 262144, False), ("summary.json", 40000, False)]
)
def test_coverage_node_budget_is_bounded_and_does_not_expand_other_inputs(
    evidence: dict[str, Path], source: str, count: int, accepted: bool
) -> None:
    path = evidence["full"] / source
    value = json.loads(path.read_text())
    value["detailed_measurements"] = [0] * count
    _write(path, value)
    _hash_artifacts(evidence["full"])
    result = _evaluate(evidence)
    assert result["status"] == ("READY_FOR_EXPLICIT_DEPLOYMENT_REVIEW" if accepted else "HOLD")
    if not accepted:
        assert "FULL_VALIDATION_INVALID_OR_DIRTY" in result["blockers"]


@pytest.mark.parametrize("limit", [True, 0, -1, None])
def test_json_node_budget_requires_a_positive_integer(limit: Any) -> None:
    from cra_dell_recovery.json_input import load_object

    with pytest.raises(ValueError, match="JSON_INPUT_NODE_LIMIT_INVALID"):
        load_object(b"{}", maximum_bytes=1024, maximum_nodes=limit)


def test_old_soak_cannot_be_relabelled_as_candidate_soak(evidence: dict[str, Path]) -> None:
    path = evidence["checkpoint"] / "checkpoint.json"
    value = json.loads(path.read_text())
    value["candidate_soak_reusable_seconds"] = 86400
    _write(path, value)
    result = _evaluate(evidence)
    assert result["status"] == "HOLD"
    assert "24H_CHECKPOINT_INVALID_OR_INCOMPLETE" in result["blockers"]


def test_gate_uses_the_checkpoint_size_contract(evidence: dict[str, Path], monkeypatch: pytest.MonkeyPatch) -> None:
    original = predeploy._sha256
    limits: list[int] = []

    def record_limit(path: Path, maximum_bytes: int = predeploy.MAXIMUM_HASHED_ARTIFACT_BYTES) -> str:
        if path.name == "samples.committed-prefix.jsonl":
            limits.append(maximum_bytes)
        return original(path, maximum_bytes)

    monkeypatch.setattr(predeploy, "_sha256", record_limit)
    assert _evaluate(evidence)["status"] == "READY_FOR_EXPLICIT_DEPLOYMENT_REVIEW"
    assert limits == [MAXIMUM_CHECKPOINT_BYTES]
    assert MAXIMUM_CHECKPOINT_BYTES == 2 * 1024**3


def test_diagnostic_checkpoint_never_opens_deployment_gate(evidence: dict[str, Path]) -> None:
    path = evidence["checkpoint"] / "checkpoint.json"
    value = json.loads(path.read_bytes())
    value["classification"] = "EVIDENCE_FROZEN_FOR_DIAGNOSIS"
    _write(path, value)
    result = _evaluate(evidence)
    assert result["status"] == "HOLD"
    assert "24H_CHECKPOINT_INVALID_OR_INCOMPLETE" in result["blockers"]


def test_checkpoint_identity_bytes_are_part_of_the_frozen_artifact(evidence: dict[str, Path]) -> None:
    (evidence["checkpoint"] / "identity-runtime.bin").write_bytes(b"changed")
    result = _evaluate(evidence)
    assert result["status"] == "HOLD"
    assert "24H_CHECKPOINT_INVALID_OR_INCOMPLETE" in result["blockers"]


def test_checkpoint_must_preserve_all_core_public_keys(evidence: dict[str, Path]) -> None:
    path = evidence["checkpoint"] / "checkpoint.json"
    value = json.loads(path.read_text())
    item = value["identity_files"].pop("public_key_cra")
    value["sha256"].pop(item["artifact"])
    _write(path, value)
    result = _evaluate(evidence)
    assert result["status"] == "HOLD"
    assert "24H_CHECKPOINT_INVALID_OR_INCOMPLETE" in result["blockers"]


def test_missing_negative_control_holds_deployment_review(evidence: dict[str, Path]) -> None:
    path = evidence["postmortem"] / "summary.json"
    value = json.loads(path.read_text())
    value["scenario_results"][3]["negative_control_detected"] = False
    _write(path, value)
    _hash_artifacts(evidence["postmortem"])
    result = _evaluate(evidence)
    assert result["status"] == "HOLD"
    assert "POSTMORTEM_HARNESS_INVALID_OR_INCOMPLETE" in result["blockers"]


def test_candidate_commit_must_match_both_validation_runs(evidence: dict[str, Path]) -> None:
    path = evidence["release_manifest"]
    value = json.loads(path.read_text())
    value["source_commit"] = "b" * 40
    _write(path, value)
    result = _evaluate(evidence)
    assert result["status"] == "HOLD"
    assert "CANDIDATE_VALIDATION_IDENTITY_MISMATCH" in result["blockers"]


def test_dirty_full_validation_cannot_reach_review(evidence: dict[str, Path]) -> None:
    path = evidence["full"] / "summary.json"
    value = json.loads(path.read_text())
    value["deployable_clean_identity"] = False
    _write(path, value)
    _hash_artifacts(evidence["full"])
    result = _evaluate(evidence)
    assert result["status"] == "HOLD"
    assert "FULL_VALIDATION_INVALID_OR_DIRTY" in result["blockers"]


def test_dirty_postmortem_harness_cannot_reach_review(evidence: dict[str, Path]) -> None:
    path = evidence["postmortem"] / "summary.json"
    value = json.loads(path.read_text())
    value["deployable_clean_identity"] = False
    _write(path, value)
    _hash_artifacts(evidence["postmortem"])
    result = _evaluate(evidence)
    assert result["status"] == "HOLD"
    assert "POSTMORTEM_HARNESS_INVALID_OR_INCOMPLETE" in result["blockers"]


def test_release_archive_must_match_manifest(evidence: dict[str, Path]) -> None:
    evidence["archive"].write_bytes(b"substituted archive")
    result = _evaluate(evidence)
    assert result["status"] == "HOLD"
    assert "IMMUTABLE_NO_ACTION_RELEASE_INVALID" in result["blockers"]


def test_candidate_source_hashes_must_match_release(evidence: dict[str, Path]) -> None:
    path = evidence["full"] / "source-before.json"
    value = json.loads(path.read_text())
    value[next(iter(COVERAGE_FILES))] = "0" * 64
    _write(path, value)
    _hash_artifacts(evidence["full"])
    result = _evaluate(evidence)
    assert result["status"] == "HOLD"
    assert "CANDIDATE_VALIDATION_IDENTITY_MISMATCH" in result["blockers"]


def test_full_junit_counts_are_recomputed(evidence: dict[str, Path]) -> None:
    (evidence["full"] / "full.xml").write_text('<testsuite tests="99" failures="0" errors="0" skipped="0"/>\n', encoding="utf-8")
    _hash_artifacts(evidence["full"])
    result = _evaluate(evidence)
    assert result["status"] == "HOLD"
    assert "FULL_VALIDATION_INVALID_OR_DIRTY" in result["blockers"]


def test_raw_coverage_must_contain_every_measured_module(evidence: dict[str, Path]) -> None:
    path = evidence["full"] / "coverage.json"
    value = json.loads(path.read_text())
    value["files"].pop(next(iter(COVERAGE_FILES)))
    _write(path, value)
    _hash_artifacts(evidence["full"])
    result = _evaluate(evidence)
    assert result["status"] == "HOLD"
    assert "FULL_VALIDATION_INVALID_OR_DIRTY" in result["blockers"]
