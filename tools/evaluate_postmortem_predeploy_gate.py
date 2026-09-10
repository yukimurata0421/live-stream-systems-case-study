from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import stat
import tarfile
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any

from cra_dell_recovery.json_input import MAXIMUM_NODES, load_object
from tools.freeze_recovery_soak_checkpoint import MAXIMUM_CHECKPOINT_BYTES
from tools.run_candidate_full_validation import COVERAGE_FILES
from tools.run_candidate_full_validation import SCHEMA as FULL_VALIDATION_SCHEMA
from tools.run_postmortem_io_harness import EXPECTED_IDS
from tools.run_postmortem_io_harness import SUMMARY_SCHEMA as POSTMORTEM_SUMMARY_SCHEMA

SCHEMA = "cra.postmortem_io_predeploy_gate.v1"
CHECKPOINT_SCHEMA = "cra.recovery_soak_24h_checkpoint.v1"
RELEASE_SCHEMA = "cra.no_action_release_manifest.v1"
SHA256 = re.compile(r"[0-9a-f]{64}")
EMPTY_GIT_STATUS_SHA256 = hashlib.sha256(b"").hexdigest()
MAXIMUM_RELEASE_ARCHIVE_BYTES = 256 * 1024 * 1024
MAXIMUM_RELEASE_MEMBER_BYTES = 64 * 1024 * 1024
MAXIMUM_RELEASE_UNCOMPRESSED_BYTES = 512 * 1024 * 1024
MAXIMUM_RELEASE_MEMBER_COUNT = 4096
MAXIMUM_GATE_JSON_BYTES = 64 * 1024 * 1024
MAXIMUM_COVERAGE_NODES = 262144
MAXIMUM_GATE_XML_BYTES = 64 * 1024 * 1024
MAXIMUM_HASHED_ARTIFACT_BYTES = 512 * 1024 * 1024
CHECKPOINT_CORE_FILES = {"config.json", "state.json", "samples.committed-prefix.jsonl"}
REQUIRED_CHECKPOINT_PUBLIC_KEYS = {"public_key_dell", "public_key_arena", "public_key_cra"}


def _read_regular(path: Path, maximum_bytes: int) -> bytes:
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode) or not 0 < metadata.st_size <= maximum_bytes:
            raise ValueError(f"PREDEPLOY_BOUNDED_REGULAR_FILE_REQUIRED:{path}")
        chunks: list[bytes] = []
        remaining = metadata.st_size
        while remaining:
            chunk = os.read(descriptor, min(1024 * 1024, remaining))
            if not chunk:
                raise ValueError(f"PREDEPLOY_FILE_SHORT_READ:{path}")
            chunks.append(chunk)
            remaining -= len(chunk)
        return b"".join(chunks)
    finally:
        os.close(descriptor)


def _sha256(path: Path, maximum_bytes: int = MAXIMUM_HASHED_ARTIFACT_BYTES) -> str:
    digest = hashlib.sha256()
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode) or not 0 <= metadata.st_size <= maximum_bytes:
            raise ValueError(f"PREDEPLOY_BOUNDED_REGULAR_FILE_REQUIRED:{path}")
        remaining = metadata.st_size
        while remaining:
            chunk = os.read(descriptor, min(1024 * 1024, remaining))
            if not chunk:
                raise ValueError(f"PREDEPLOY_FILE_SHORT_READ:{path}")
            digest.update(chunk)
            remaining -= len(chunk)
        return digest.hexdigest()
    finally:
        os.close(descriptor)


def _regular_size(path: Path, maximum_bytes: int = MAXIMUM_HASHED_ARTIFACT_BYTES) -> int:
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode) or not 0 <= metadata.st_size <= maximum_bytes:
            raise ValueError(f"PREDEPLOY_BOUNDED_REGULAR_FILE_REQUIRED:{path}")
        return metadata.st_size
    finally:
        os.close(descriptor)


def _object(path: Path, *, maximum_nodes: int = MAXIMUM_NODES) -> dict[str, Any]:
    return load_object(_read_regular(path, MAXIMUM_GATE_JSON_BYTES), maximum_bytes=MAXIMUM_GATE_JSON_BYTES, maximum_nodes=maximum_nodes)


def _junit_counts(path: Path) -> dict[str, int]:
    root = ET.fromstring(_read_regular(path, MAXIMUM_GATE_XML_BYTES))
    suites = [root] if root.tag == "testsuite" else list(root.findall("testsuite"))
    return {name: sum(int(suite.attrib.get(name, "0")) for suite in suites) for name in ("tests", "failures", "errors", "skipped")}


def _artifact_set_valid(root: Path, required: set[str]) -> bool:
    try:
        recorded = _object(root / "artifact_hashes.json")
        return required <= set(recorded) and all(
            isinstance(name, str)
            and Path(name).name == name
            and name != "artifact_hashes.json"
            and SHA256.fullmatch(str(digest)) is not None
            and _sha256(root / name) == digest
            for name, digest in recorded.items()
        )
    except (OSError, ValueError, TypeError, json.JSONDecodeError):
        return False


def _release_archive_valid(manifest_path: Path, archive_path: Path, manifest: dict[str, Any]) -> bool:
    """Bind every archive member to the sidecar manifest without extracting it."""

    try:
        if archive_path.is_symlink() or not archive_path.is_file() or not 0 < archive_path.stat().st_size <= MAXIMUM_RELEASE_ARCHIVE_BYTES:
            return False
        release_id = manifest.get("release_id")
        files = manifest.get("files")
        if not isinstance(release_id, str) or not isinstance(files, dict) or not files:
            return False
        if manifest.get("file_count") != len(files) or len(files) + 1 > MAXIMUM_RELEASE_MEMBER_COUNT:
            return False
        tree_input = bytearray()
        for relative, metadata in sorted(files.items()):
            if (
                not isinstance(relative, str)
                or not relative
                or relative.startswith("/")
                or ".." in Path(relative).parts
                or not isinstance(metadata, dict)
                or set(metadata) != {"sha256", "size", "executable", "origin"}
                or SHA256.fullmatch(str(metadata.get("sha256", ""))) is None
                or type(metadata.get("size")) is not int
                or not 0 <= metadata["size"] <= MAXIMUM_RELEASE_MEMBER_BYTES
                or type(metadata.get("executable")) is not bool
                or not isinstance(metadata.get("origin"), str)
            ):
                return False
            tree_input.extend(f"{relative}\0{metadata['sha256']}\0".encode())
        if manifest.get("source_tree_sha256") != hashlib.sha256(tree_input).hexdigest():
            return False
        sidecar = _read_regular(manifest_path, MAXIMUM_GATE_JSON_BYTES)
        prefix = f"stream-recovery-control-{release_id}"
        expected = {f"{prefix}/{relative}": metadata for relative, metadata in files.items()}
        manifest_member = f"{prefix}/release_manifest.json"
        expected_names = set(expected) | {manifest_member}
        total_size = 0
        with tarfile.open(archive_path, mode="r:gz") as archive:
            members = archive.getmembers()
            if len(members) != len(expected_names) or {member.name for member in members} != expected_names:
                return False
            for member in members:
                if (
                    not member.isfile()
                    or member.uid != 0
                    or member.gid != 0
                    or member.uname != "root"
                    or member.gname != "root"
                    or member.mtime != 0
                    or member.size > MAXIMUM_RELEASE_MEMBER_BYTES
                ):
                    return False
                total_size += member.size
                if total_size > MAXIMUM_RELEASE_UNCOMPRESSED_BYTES:
                    return False
                stream = archive.extractfile(member)
                if stream is None:
                    return False
                raw = stream.read(member.size + 1)
                if len(raw) != member.size:
                    return False
                if member.name == manifest_member:
                    if member.mode != 0o644 or raw != sidecar:
                        return False
                    continue
                metadata = expected[member.name]
                if (
                    member.size != metadata["size"]
                    or member.mode != (0o755 if metadata["executable"] else 0o644)
                    or _sha256_bytes(raw) != metadata["sha256"]
                ):
                    return False
        return True
    except (OSError, ValueError, KeyError, tarfile.TarError):
        return False


def _sha256_bytes(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def evaluate(
    checkpoint_root: Path,
    postmortem_root: Path,
    full_validation_root: Path,
    release_manifest_path: Path,
    release_archive_path: Path,
    *,
    superseded_epoch_disposition: Path | None = None,
) -> dict[str, Any]:
    blockers: set[str] = set()
    try:
        checkpoint = _object(checkpoint_root / "checkpoint.json")
        hashes = checkpoint.get("sha256", {})
        identity_files = checkpoint.get("identity_files")
        identity_mapping: dict[str, Any] = identity_files if isinstance(identity_files, dict) else {}
        identity_files_valid = (
            bool(identity_mapping)
            and set(identity_mapping) >= REQUIRED_CHECKPOINT_PUBLIC_KEYS
            and all(
                isinstance(label, str)
                and isinstance(item, dict)
                and set(item) == {"source_path", "artifact", "sha256", "size"}
                and isinstance(item["source_path"], str)
                and Path(item["source_path"]).is_absolute()
                and isinstance(item["artifact"], str)
                and item["artifact"] == f"identity-{label}.bin"
                and SHA256.fullmatch(str(item["sha256"])) is not None
                and type(item["size"]) is int
                and item["size"] > 0
                for label, item in identity_mapping.items()
            )
        )
        identity_artifacts = {item["artifact"] for item in identity_mapping.values()} if identity_files_valid else set()
        checkpoint_files_valid = (
            isinstance(hashes, dict)
            and set(hashes) == CHECKPOINT_CORE_FILES | identity_artifacts
            and all(
                _sha256(
                    checkpoint_root / name,
                    MAXIMUM_CHECKPOINT_BYTES if name == "samples.committed-prefix.jsonl" else MAXIMUM_HASHED_ARTIFACT_BYTES,
                )
                == digest
                for name, digest in hashes.items()
            )
            and all(_regular_size(checkpoint_root / name, MAXIMUM_CHECKPOINT_BYTES) > 0 for name in CHECKPOINT_CORE_FILES)
            and all(_regular_size(checkpoint_root / item["artifact"]) == item["size"] for item in identity_mapping.values())
            and all(hashes[item["artifact"]] == item["sha256"] for item in identity_mapping.values())
        )
        evaluation = checkpoint.get("evaluation", {})
        checkpoint_valid = (
            checkpoint.get("schema") == CHECKPOINT_SCHEMA
            and checkpoint.get("classification") == "24H_OBSERVATION_FROZEN"
            and float(checkpoint.get("minimum_observation_seconds", 0)) == 86_400
            and checkpoint.get("candidate_soak_reusable_seconds") == 0
            and checkpoint.get("candidate_identity_validated_by_this_checkpoint") is False
            and identity_files_valid
            and checkpoint_files_valid
            and isinstance(evaluation, dict)
            and evaluation.get("harness_classification") == "PASS"
            and evaluation.get("live_health") == "READY"
            and float(evaluation.get("verified_duration_seconds", 0)) >= 86_400
            and float(evaluation.get("maximum_sample_gap_seconds", 46)) <= 45
            and not evaluation.get("blockers")
            and not evaluation.get("unknown_reasons")
        )
        if superseded_epoch_disposition is not None:
            disposition = _object(superseded_epoch_disposition)
            reason = disposition.get("reason")
            reviewed_gaps = {
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
            }
            # A replacement may preserve an uncertifiable old epoch. This
            # admits its disposition, never its missing evidence as success.
            checkpoint_valid = (
                checkpoint.get("schema") == CHECKPOINT_SCHEMA
                and checkpoint.get("classification") == "EVIDENCE_FROZEN_FOR_DIAGNOSIS"
                and checkpoint.get("candidate_soak_reusable_seconds") == 0
                and checkpoint.get("candidate_identity_validated_by_this_checkpoint") is False
                and identity_files_valid
                and checkpoint_files_valid
                and isinstance(evaluation, dict)
                and evaluation.get("harness_classification") == "MISSING_EVIDENCE"
                and evaluation.get("formal_evaluation_performed") is False
                and evaluation.get("soak_status") is None
                and evaluation.get("live_health") == "READY"
                and not evaluation.get("blockers")
                and not evaluation.get("oracle_errors")
                and isinstance(reason, str)
                and reason in reviewed_gaps
                and evaluation.get("unknown_reasons") == reviewed_gaps[reason]
                and isinstance(checkpoint.get("epoch_id"), str)
                and bool(checkpoint["epoch_id"])
                and disposition.get("replacement_requested") is True
                and disposition.get("raw_evidence_preserved") is True
                and type(disposition.get("reusable_seconds")) is int
                and disposition
                == {
                    "schema": "cra.recovery_epoch_disposition.v1",
                    "epoch_id": checkpoint.get("epoch_id"),
                    "checkpoint_sha256": _sha256(checkpoint_root / "checkpoint.json"),
                    "old_epoch_status": "UNCERTIFIED_MISSING_EVIDENCE",
                    "reason": reason,
                    "raw_evidence_preserved": True,
                    "replacement_requested": True,
                    "reusable_seconds": 0,
                }
            )
    except (AttributeError, KeyError, OSError, OverflowError, TypeError, ValueError, json.JSONDecodeError):
        checkpoint, checkpoint_valid = {}, False
    if not checkpoint_valid:
        blockers.add("24H_CHECKPOINT_INVALID_OR_INCOMPLETE")

    try:
        postmortem = _object(postmortem_root / "summary.json")
        postmortem_manifest = _object(postmortem_root / "manifest.json")
        scenarios = postmortem.get("scenario_results", [])
        required_postmortem_artifacts = {
            "summary.json",
            "manifest.json",
            *(str(item.get("junit")) for item in scenarios if isinstance(item, dict)),
        }
        scenario_junits_valid = (
            isinstance(scenarios, list)
            and len(scenarios) == len(EXPECTED_IDS)
            and all(
                isinstance(item, dict)
                and isinstance(item.get("junit"), str)
                and Path(item["junit"]).name == item["junit"]
                and (actual := _junit_counts(postmortem_root / item["junit"])) == item.get("junit_counts")
                and actual["tests"] >= int(item.get("collected_test_count", 0)) > 0
                and all(actual[name] == 0 for name in ("failures", "errors", "skipped"))
                for item in scenarios
            )
        )
        postmortem_valid = (
            len(required_postmortem_artifacts) == 2 + len(EXPECTED_IDS)
            and _artifact_set_valid(postmortem_root, required_postmortem_artifacts)
            and postmortem.get("schema") == POSTMORTEM_SUMMARY_SCHEMA
            and postmortem.get("trusted") is True
            and postmortem.get("deployable_clean_identity") is True
            and postmortem.get("source_stable") is True
            and postmortem_manifest.get("git_status_porcelain_sha256") == EMPTY_GIT_STATUS_SHA256
            and scenario_junits_valid
            and tuple(item.get("scenario_id") for item in scenarios) == EXPECTED_IDS
            and all(
                item.get("classification") == "PASS"
                and item.get("fault_reachability_asserted_by_tests") is True
                and item.get("negative_control_detected") is True
                for item in scenarios
            )
        )
    except (AttributeError, ET.ParseError, KeyError, OSError, OverflowError, TypeError, ValueError, json.JSONDecodeError):
        postmortem, postmortem_manifest, postmortem_valid = {}, {}, False
    if not postmortem_valid:
        blockers.add("POSTMORTEM_HARNESS_INVALID_OR_INCOMPLETE")

    try:
        full = _object(full_validation_root / "summary.json")
        full_sources = _object(full_validation_root / "source-before.json")
        raw_coverage = _object(full_validation_root / "coverage.json", maximum_nodes=MAXIMUM_COVERAGE_NODES)
        counts = full.get("junit_counts", {})
        junit_counts = _junit_counts(full_validation_root / "full.xml")
        coverage_files = raw_coverage.get("files", {})
        coverage_meta = raw_coverage.get("meta", {})
        raw_coverage_valid = (
            isinstance(coverage_meta, dict)
            and coverage_meta.get("branch_coverage") is True
            and isinstance(coverage_files, dict)
            and all(
                isinstance(coverage_files.get(relative), dict)
                and isinstance(coverage_files[relative].get("summary"), dict)
                and int(coverage_files[relative]["summary"].get("num_branches", 0)) > 0
                for relative in COVERAGE_FILES
            )
        )
        full_valid = (
            _artifact_set_valid(
                full_validation_root,
                {"summary.json", "full.xml", "coverage.json", "source-before.json"},
            )
            and full.get("schema") == FULL_VALIDATION_SCHEMA
            and full.get("trusted") is True
            and full.get("deployable_clean_identity") is True
            and full.get("source_stable") is True
            and full.get("git_status_porcelain_sha256") == EMPTY_GIT_STATUS_SHA256
            and full.get("branch_coverage_measured") is True
            and not full.get("missing_coverage_files")
            and set(full.get("coverage_files", [])) == set(COVERAGE_FILES)
            and raw_coverage_valid
            and junit_counts == counts
            and int(counts.get("tests", 0)) > 0
            and all(int(counts.get(name, -1)) == 0 for name in ("failures", "errors", "skipped"))
        )
    except (AttributeError, ET.ParseError, KeyError, OSError, OverflowError, TypeError, ValueError, json.JSONDecodeError):
        full, full_sources, full_valid = {}, {}, False
    if not full_valid:
        blockers.add("FULL_VALIDATION_INVALID_OR_DIRTY")

    try:
        required_release_files = {
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
        release = _object(release_manifest_path)
        release_valid = (
            release.get("schema") == RELEASE_SCHEMA
            and re.fullmatch(r"[0-9a-f]{40}", str(release.get("source_commit", ""))) is not None
            and release.get("operating_mode") == "NO_ACTION"
            and release.get("production_action_enabled") is False
            and release.get("control_capability_count") == 0
            and release.get("physical_effect_count") == 0
            and release.get("central_command_store_packaged") is False
            and release.get("effect_adapter_packaged") is False
            and release.get("dell_agent_packaged") is False
            and release.get("monitoring_private_key_packaged") is False
            and release.get("excluded_runtime_state") is True
            and release.get("secret_values_recorded") is False
            and required_release_files <= set(release.get("files", {}))
            and _release_archive_valid(release_manifest_path, release_archive_path, release)
        )
    except (AttributeError, KeyError, OSError, OverflowError, TypeError, ValueError, json.JSONDecodeError):
        release, release_valid = {}, False
    if not release_valid:
        blockers.add("IMMUTABLE_NO_ACTION_RELEASE_INVALID")

    source_commit = release.get("source_commit")
    release_files = release.get("files", {})
    postmortem_inputs = postmortem_manifest.get("input_sha256", {})
    candidate_sources_match = (
        isinstance(release_files, dict)
        and isinstance(postmortem_inputs, dict)
        and all(
            isinstance(release_files.get(relative), dict)
            and full_sources.get(relative) == release_files[relative].get("sha256")
            and postmortem_inputs.get(relative) == release_files[relative].get("sha256")
            for relative in required_release_files
        )
    )
    if (
        not full_valid
        or not postmortem_valid
        or source_commit != full.get("git_head")
        or source_commit != postmortem_manifest.get("git_head")
        or not candidate_sources_match
    ):
        blockers.add("CANDIDATE_VALIDATION_IDENTITY_MISMATCH")
    return {
        "schema": SCHEMA,
        "status": "READY_FOR_EXPLICIT_DEPLOYMENT_REVIEW" if not blockers else "HOLD",
        "deployment_authorized": False,
        "blockers": sorted(blockers),
        "checkpoint": {
            "epoch_id": checkpoint.get("epoch_id"),
            "checkpoint_sha256": _sha256(checkpoint_root / "checkpoint.json") if checkpoint_valid else None,
            "candidate_soak_reusable_seconds": 0,
            "acceptance": "SUPERSEDED_UNCERTIFIED_EPOCH" if superseded_epoch_disposition is not None else "HEALTHY_24H_OBSERVATION",
            "disposition_sha256": _sha256(superseded_epoch_disposition)
            if checkpoint_valid and superseded_epoch_disposition is not None
            else None,
        },
        "candidate": {
            "release_id": release.get("release_id"),
            "source_commit": source_commit,
            "archive_sha256": _sha256(release_archive_path) if release_valid else None,
        },
        "next_epoch": {
            "required": True,
            "initial_reusable_seconds": 0,
            "minimum_formal_duration_seconds": 604800,
        },
        "claim_boundary": [
            "READY_FOR_EXPLICIT_DEPLOYMENT_REVIEW is not deployment authorization",
            "the checkpoint belongs to the pre-change identity; a diagnostic disposition never certifies that epoch",
            "the deployed candidate requires a fresh seven-day epoch starting at zero",
        ],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Evaluate the fail-closed predeploy evidence gate without deploying")
    parser.add_argument("--checkpoint-root", type=Path, required=True)
    parser.add_argument("--postmortem-root", type=Path, required=True)
    parser.add_argument("--full-validation-root", type=Path, required=True)
    parser.add_argument("--release-manifest", type=Path, required=True)
    parser.add_argument("--release-archive", type=Path, required=True)
    parser.add_argument("--superseded-epoch-disposition", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        raise FileExistsError("PREDEPLOY_GATE_OUTPUT_EXISTS")
    result = evaluate(
        args.checkpoint_root,
        args.postmortem_root,
        args.full_validation_root,
        args.release_manifest,
        args.release_archive,
        superseded_epoch_disposition=args.superseded_epoch_disposition,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(result, sort_keys=True))
    if result["status"] != "READY_FOR_EXPLICIT_DEPLOYMENT_REVIEW":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
