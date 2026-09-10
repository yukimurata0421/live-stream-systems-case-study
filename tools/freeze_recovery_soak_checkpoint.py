from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import stat
import tempfile
from collections.abc import Iterable, Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from cra_dell_recovery.time import isoformat_utc, parse_utc
from cra_no_action_soak import recovery_soak
from cra_no_action_soak.recovery_facts import nonnegative_integer, strict_json_object

SCHEMA = "cra.recovery_soak_24h_checkpoint.v1"
MINIMUM_OBSERVATION_SECONDS = 86_400
ALLOWED_PENDING = frozenset({"SOAK_DURATION_INSUFFICIENT", "RECOVERY_NOT_EXERCISED"})
MAXIMUM_IDENTITY_BYTES = 2 * 1024 * 1024
MAXIMUM_CHECKPOINT_BYTES = recovery_soak.MAXIMUM_BYTES
MINIMUM_FREE_RESERVE_BYTES = 64 * 1024 * 1024


def _sha256_bytes(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_regular(path: Path, *, maximum_bytes: int) -> bytes:
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode) or not 0 < metadata.st_size <= maximum_bytes:
            raise ValueError(f"CHECKPOINT_INPUT_NOT_BOUNDED_REGULAR_FILE:{path}")
        with os.fdopen(descriptor, "rb") as stream:
            descriptor = -1
            raw = stream.read(maximum_bytes + 1)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    if len(raw) > maximum_bytes:
        raise ValueError(f"CHECKPOINT_INPUT_TOO_LARGE:{path}")
    return raw


def _strict_object(raw: bytes, *, code: str) -> dict[str, Any]:
    try:
        value = strict_json_object(raw)
    except (UnicodeError, ValueError) as error:
        raise ValueError(code) from error
    return value


def _validate_state(config: recovery_soak.Config, state: dict[str, Any]) -> None:
    initial = recovery_soak._initial_state(config)
    if (
        set(state) != set(initial)
        or state.get("schema") != recovery_soak.STATE_SCHEMA
        or state.get("epoch_id") != config.value["epoch_id"]
        or state.get("config_sha256") != config.identity
        or not nonnegative_integer(state.get("sample_count"))
        or not nonnegative_integer(state.get("verified_bytes"))
        or int(state["sample_count"]) < 1
        or int(state["verified_bytes"]) < 1
    ):
        raise ValueError("CHECKPOINT_STATE_INVALID")


def _copy_prefix(source: Path, destination: Path, length: int, maximum_bytes: int) -> str:
    if not 0 < length <= maximum_bytes:
        raise ValueError("CHECKPOINT_PREFIX_SIZE_INVALID")
    descriptor = os.open(source, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    digest = hashlib.sha256()
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_size < length:
            raise ValueError("CHECKPOINT_EVIDENCE_PREFIX_UNAVAILABLE")
        remaining = length
        with destination.open("xb") as output:
            while remaining:
                chunk = os.read(descriptor, min(1024 * 1024, remaining))
                if not chunk:
                    raise ValueError("CHECKPOINT_EVIDENCE_PREFIX_SHORT_READ")
                output.write(chunk)
                digest.update(chunk)
                remaining -= len(chunk)
            output.flush()
            os.fsync(output.fileno())
        # The source is append-only by contract, but the freeze tool does not
        # trust that declaration. Re-read the pinned descriptor and reject an
        # in-place rewrite observed during capture.
        os.lseek(descriptor, 0, os.SEEK_SET)
        if _hash_descriptor_prefix(descriptor, length) != digest.hexdigest():
            raise ValueError("CHECKPOINT_EVIDENCE_CHANGED_DURING_CAPTURE")
    finally:
        os.close(descriptor)
    return digest.hexdigest()


def _hash_descriptor_prefix(descriptor: int, length: int) -> str:
    digest = hashlib.sha256()
    remaining = length
    while remaining:
        chunk = os.read(descriptor, min(1024 * 1024, remaining))
        if not chunk:
            raise ValueError("CHECKPOINT_EVIDENCE_PREFIX_SHORT_READ")
        digest.update(chunk)
        remaining -= len(chunk)
    return digest.hexdigest()


def _rows(path: Path, maximum_frame_bytes: int) -> Iterator[dict[str, Any]]:
    with path.open("rb") as stream:
        while raw := stream.readline(maximum_frame_bytes + 1):
            if len(raw) > maximum_frame_bytes or not raw.endswith(b"\n"):
                raise ValueError("CHECKPOINT_EVIDENCE_PARTIAL_OR_OVERSIZE_FRAME")
            yield _strict_object(raw, code="CHECKPOINT_EVIDENCE_FRAME_INVALID")


def _check_capacity(source: Path, destination_parent: Path, length: int, evidence_limit: int) -> None:
    # A separate directory may still share the active collector's filesystem.
    # Leave room for its remaining configured evidence budget, not just our copy.
    metadata = source.stat()
    remaining_soak = max(0, evidence_limit - metadata.st_size) if metadata.st_dev == destination_parent.stat().st_dev else 0
    required = length + remaining_soak + MINIMUM_FREE_RESERVE_BYTES
    if shutil.disk_usage(destination_parent).free < required:
        raise ValueError("CHECKPOINT_DESTINATION_CAPACITY_INSUFFICIENT")


def _identity_material(paths: dict[str, Path]) -> tuple[dict[str, dict[str, Any]], dict[str, bytes]]:
    result: dict[str, dict[str, Any]] = {}
    payloads: dict[str, bytes] = {}
    for label, path in sorted(paths.items()):
        if not re.fullmatch(r"[a-z][a-z0-9_-]{0,63}", label) or label in result:
            raise ValueError("CHECKPOINT_IDENTITY_LABEL_INVALID")
        raw = _read_regular(path, maximum_bytes=MAXIMUM_IDENTITY_BYTES)
        artifact = f"identity-{label}.bin"
        result[label] = {
            "source_path": str(path.resolve()),
            "artifact": artifact,
            "sha256": _sha256_bytes(raw),
            "size": len(raw),
        }
        payloads[label] = raw
    return result, payloads


def _overlaps(left: Path, right: Path) -> bool:
    left, right = left.resolve(), right.resolve()
    return left == right or left in right.parents or right in left.parents


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _write_fsynced(path: Path, raw: bytes) -> None:
    with path.open("xb") as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())
    os.chmod(path, 0o600)


def _boundary(rows: Iterable[dict[str, Any]], baseline_at: datetime, minimum_seconds: float) -> dict[str, Any] | None:
    threshold = baseline_at + timedelta(seconds=minimum_seconds)
    for row in rows:
        observed = parse_utc(str(row.get("observed_at", "")))
        if observed >= threshold:
            return {
                "sample_sequence": row["sample_sequence"],
                "sample_hash": row["sample_hash"],
                "observed_at": isoformat_utc(observed),
                "threshold_at": isoformat_utc(threshold),
                "overshoot_seconds": (observed - threshold).total_seconds(),
            }
    return None


def freeze_checkpoint(
    config_path: Path,
    output: Path,
    identity_files: dict[str, Path],
    *,
    captured_at: datetime | None = None,
    minimum_observation_seconds: float = MINIMUM_OBSERVATION_SECONDS,
    preserve_incomplete: bool = False,
) -> dict[str, Any]:
    """Freeze a lock-free, read-only copy of one already committed prefix."""

    if minimum_observation_seconds <= 0:
        raise ValueError("CHECKPOINT_MINIMUM_DURATION_INVALID")
    config_path, output = config_path.resolve(), output.resolve()
    if output.exists():
        raise FileExistsError("CHECKPOINT_OUTPUT_EXISTS")
    config_raw = _read_regular(config_path, maximum_bytes=128 * 1024)
    config = recovery_soak.Config.load(config_path)
    material_paths = dict(identity_files)
    for role, host in config.value["hosts"].items():
        if host is None:
            continue
        label = f"public_key_{role}"
        if label in material_paths:
            raise ValueError("CHECKPOINT_IDENTITY_LABEL_RESERVED")
        material_paths[label] = Path(host["public_key_file"])
    sources = [
        config_path,
        Path(config.value["evidence_file"]),
        Path(config.value["state_file"]),
        Path(config.value["gate_file"]),
        *material_paths.values(),
    ]
    if any(_overlaps(output, source) or output.parent == source.resolve().parent for source in sources):
        raise ValueError("CHECKPOINT_OUTPUT_SOURCE_COLLISION")
    identity_before, identity_payloads = _identity_material(material_paths)
    state_path = Path(config.value["state_file"])
    state_raw = _read_regular(state_path, maximum_bytes=64 * 1024)
    state = _strict_object(state_raw, code="CHECKPOINT_STATE_JSON_INVALID")
    _validate_state(config, state)
    output.parent.mkdir(parents=True, exist_ok=True)
    _check_capacity(
        Path(config.value["evidence_file"]), output.parent, int(state["verified_bytes"]), int(config.value["maximum_evidence_bytes"])
    )
    temporary = Path(tempfile.mkdtemp(prefix=f".{output.name}.", dir=output.parent))
    try:
        evidence_copy = temporary / "samples.committed-prefix.jsonl"
        evidence_sha256 = _copy_prefix(
            Path(config.value["evidence_file"]),
            evidence_copy,
            int(state["verified_bytes"]),
            int(config.value["maximum_evidence_bytes"]),
        )
        last_sample_at = parse_utc(str(state["last_observed_at"]))
        evaluation = recovery_soak.evaluate(_rows(evidence_copy, recovery_soak.MAXIMUM_FRAME_BYTES), config=config, now=last_sample_at)
        if (
            evaluation["sample_count"] != state["sample_count"]
            or evaluation["last_sample_hash"] != state["last_sample_hash"]
            or evaluation["last_sample_at"] != state["last_observed_at"]
            or evaluation["oracle_errors"]
        ):
            raise ValueError("CHECKPOINT_FULL_REPLAY_MISMATCH")
        baseline_raw = evaluation["recovery"]["baseline_at"]
        if baseline_raw is None and not preserve_incomplete:
            raise ValueError("CHECKPOINT_BASELINE_MISSING")
        boundary = (
            _boundary(_rows(evidence_copy, recovery_soak.MAXIMUM_FRAME_BYTES), parse_utc(str(baseline_raw)), minimum_observation_seconds)
            if baseline_raw is not None
            else None
        )
        duration_met = boundary is not None and float(evaluation["verified_duration_seconds"]) >= minimum_observation_seconds
        if not duration_met and not preserve_incomplete:
            raise ValueError("CHECKPOINT_24H_NOT_YET_ELIGIBLE")
        unhealthy = (
            evaluation["harness_classification"] != "PASS"
            or evaluation["blockers"]
            or evaluation["unknown_reasons"]
            or evaluation["live_health"] != "READY"
            or float(evaluation["maximum_sample_gap_seconds"]) > float(config.value["maximum_sample_gap_seconds"])
            or not set(evaluation["pending_reasons"]) <= ALLOWED_PENDING
        )
        if unhealthy and not preserve_incomplete:
            raise ValueError("CHECKPOINT_OBSERVATION_NOT_HEALTHY")
        # Re-read every mutable identity input after the potentially long copy.
        # A collector may append later frames; the captured committed prefix is
        # still valid. Config/key/manifest mutation, however, invalidates it.
        if _read_regular(config_path, maximum_bytes=128 * 1024) != config_raw:
            raise ValueError("CHECKPOINT_CONFIG_CHANGED_DURING_CAPTURE")
        if recovery_soak.Config.load(config_path).identity != config.identity:
            raise ValueError("CHECKPOINT_KEY_CHANGED_DURING_CAPTURE")
        if _identity_material(material_paths) != (identity_before, identity_payloads):
            raise ValueError("CHECKPOINT_IDENTITY_CHANGED_DURING_CAPTURE")
        state_after = _strict_object(
            _read_regular(state_path, maximum_bytes=64 * 1024),
            code="CHECKPOINT_STATE_JSON_INVALID",
        )
        _validate_state(config, state_after)
        if (
            int(state_after["sample_count"]) < int(state["sample_count"])
            or int(state_after["verified_bytes"]) < int(state["verified_bytes"])
            or (
                state_after["sample_count"] == state["sample_count"]
                and (
                    state_after["last_sample_hash"] != state["last_sample_hash"] or state_after["verified_bytes"] != state["verified_bytes"]
                )
            )
        ):
            raise ValueError("CHECKPOINT_STATE_REGRESSED_DURING_CAPTURE")
        _write_fsynced(temporary / "config.json", config_raw)
        _write_fsynced(temporary / "state.json", state_raw)
        for label, raw in identity_payloads.items():
            _write_fsynced(temporary / identity_before[label]["artifact"], raw)
        captured = captured_at or datetime.now(UTC)
        if captured.tzinfo is None:
            raise ValueError("CHECKPOINT_CAPTURE_TIME_NAIVE")
        if captured.astimezone(UTC) < last_sample_at:
            raise ValueError("CHECKPOINT_CAPTURE_TIME_BEFORE_PREFIX")
        manifest = {
            "schema": SCHEMA,
            "classification": "EVIDENCE_FROZEN_FOR_DIAGNOSIS" if preserve_incomplete else "24H_OBSERVATION_FROZEN",
            "observation_acceptable": bool(duration_met and not unhealthy),
            "epoch_id": config.value["epoch_id"],
            "config_sha256": config.identity,
            "captured_at": isoformat_utc(captured),
            "historical_evaluation_at": isoformat_utc(last_sample_at),
            "minimum_observation_seconds": minimum_observation_seconds,
            "formal_soak_status": evaluation["soak_status"],
            "formal_soak_eligible": evaluation["eligible"],
            "candidate_soak_reusable_seconds": 0,
            "candidate_identity_validated_by_this_checkpoint": False,
            "source_state": {
                "sample_count": state["sample_count"],
                "verified_bytes": state["verified_bytes"],
                "last_sample_hash": state["last_sample_hash"],
                "last_observed_at": state["last_observed_at"],
            },
            "checkpoint_boundary": boundary,
            "sha256": {
                "config.json": _sha256_bytes(config_raw),
                "state.json": _sha256_bytes(state_raw),
                "samples.committed-prefix.jsonl": evidence_sha256,
                **{item["artifact"]: item["sha256"] for item in identity_before.values()},
            },
            "identity_files": identity_before,
            "evaluation": evaluation,
            "claim_boundary": [
                "read-only copy of an already committed prefix; the active collector was not locked or stopped",
                "diagnostic prefix; duration and health acceptance are not asserted"
                if preserve_incomplete
                else "24-hour historical observation checkpoint, not a seven-day PASS",
                "not evidence for a different candidate identity",
                "not production fault injection or physical action evidence",
            ],
        }
        manifest_path = temporary / "checkpoint.json"
        manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        with manifest_path.open("rb") as stream:
            os.fsync(stream.fileno())
        _fsync_directory(temporary)
        os.replace(temporary, output)
        _fsync_directory(output.parent)
        return manifest
    finally:
        if temporary.exists():
            shutil.rmtree(temporary)


def _identity_arguments(values: list[str]) -> dict[str, Path]:
    result: dict[str, Path] = {}
    for item in values:
        label, separator, raw_path = item.partition("=")
        if not separator or not label or label in result or not Path(raw_path).is_absolute():
            raise ValueError("CHECKPOINT_IDENTITY_ARGUMENT_INVALID")
        result[label] = Path(raw_path)
    if not result:
        raise ValueError("CHECKPOINT_IDENTITY_ARGUMENT_REQUIRED")
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description="Freeze a read-only 24-hour checkpoint of a live recovery-soak committed prefix")
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--identity-file", action="append", default=[], metavar="LABEL=/ABSOLUTE/PATH")
    parser.add_argument(
        "--preserve-incomplete",
        action="store_true",
        help="Preserve diagnostic evidence without claiming 24h acceptance; deployment gate remains HOLD",
    )
    args = parser.parse_args()
    manifest = freeze_checkpoint(
        args.config, args.output, _identity_arguments(args.identity_file), preserve_incomplete=args.preserve_incomplete
    )
    print(json.dumps(manifest, sort_keys=True))


if __name__ == "__main__":
    main()
