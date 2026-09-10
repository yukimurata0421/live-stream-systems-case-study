from __future__ import annotations

import ast
import json
from contextlib import suppress
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import jsonschema

from snapshot_projection import ProjectionProjector, ProjectionReader

TARGET = {
    "host_id": "dell-yuki",
    "host_boot_id": "boot-1",
    "namespace": "stream-v3",
    "pod_uid": "pod-1",
    "container_name": "stream-engine",
    "container_id": "containerd://engine-1",
    "ffmpeg_generation": "stream-1:4:4100",
    "ffmpeg_pid": 4100,
}


def source_snapshot(now: datetime, **updates: Any) -> dict[str, Any]:
    value: dict[str, Any] = {
        "schema_version": "maintenance.audit_state_snapshot.v2",
        "available": True,
        "producer_id": "arena-maintenance-shadow",
        "producer_instance_id": "coordinator-1",
        "snapshot_id": "snapshot-1",
        "observed_at": now.isoformat().replace("+00:00", "Z"),
        "fresh_until": (now + timedelta(seconds=10)).isoformat().replace("+00:00", "Z"),
        "maintenance_state": "INACTIVE",
        "maintenance_id": "",
        "maintenance_generation": 2,
        "authority_epoch": 34,
        "authority_session_id": "session-1",
        "transaction_state": "INACTIVE",
        "authorization_id": "",
        "authorization_state": "NONE",
        "source_target_identity": TARGET,
        "target_identity": TARGET,
        "target_snapshot_id": "target-1",
        "target_snapshot_status": "VALID",
        "target_snapshot_reason": "TARGET_VALID",
        "target_snapshot_age_seconds": 0.1,
        "reconciliation_state": "POSITIVE_INACTIVE_RECONCILIATION",
        "authorizations": [],
        "proof": {"positive_inactive_proof": True, "integrity_ok": True},
        "physical_effect_count": 0,
        "production_behavior_modified": False,
    }
    value.update(updates)
    return value


def publish(tmp_path: Path, now: datetime, **source_updates: Any) -> tuple[Path, Path, dict[str, Any]]:
    source = tmp_path / "source.json"
    output = tmp_path / "projection.json"
    source.write_text(json.dumps(source_snapshot(now, **source_updates)), encoding="utf-8")
    envelope = ProjectionProjector(
        source_path=source,
        output_path=output,
        sequence_path=tmp_path / "producer-sequence.json",
        producer_id="dell-maintenance-snapshot-projector",
        producer_instance_id="projector-1",
        expected_source_producer_id="arena-maintenance-shadow",
        ttl_seconds=5,
    ).publish(now=now)
    return source, output, envelope


def reader(path: Path, high_water: Path | None = None) -> ProjectionReader:
    return ProjectionReader(
        path=path,
        expected_producer_id="dell-maintenance-snapshot-projector",
        expected_source_producer_id="arena-maintenance-shadow",
        high_water_path=high_water,
    )


def test_projection_schema_and_valid_read(tmp_path: Path) -> None:
    now = datetime(2026, 8, 24, 12, 0, tzinfo=UTC)
    _, output, envelope = publish(tmp_path, now)
    schema = json.loads(
        (Path(__file__).parents[2] / "contracts/maintenance/v2/snapshot_projection.v1.schema.json").read_text(encoding="utf-8")
    )
    jsonschema.Draft202012Validator(schema).validate(envelope)
    decision = reader(output, tmp_path / "consumer-high-water.json").read(
        now=now + timedelta(seconds=1),
        expected_target_identity=TARGET,
        expected_maintenance_generation=2,
    )
    assert decision.available is True
    assert decision.reason_code == "PROJECTION_VALID"
    assert decision.snapshot is not None
    assert decision.snapshot["production_behavior_modified"] is False


def test_nc_sp_01_missing_snapshot_never_becomes_inactive(tmp_path: Path) -> None:
    decision = reader(tmp_path / "missing.json").read()
    assert decision.available is False
    assert decision.snapshot is None


def test_nc_sp_02_stale_snapshot_is_unknown(tmp_path: Path) -> None:
    now = datetime(2026, 8, 24, 12, 0, tzinfo=UTC)
    _, output, _ = publish(tmp_path, now)
    assert reader(output).read(now=now + timedelta(seconds=6)).reason_code == "PROJECTION_STALE_OR_FUTURE"


def test_nc_sp_03_partial_write_is_rejected(tmp_path: Path) -> None:
    path = tmp_path / "projection.json"
    path.write_text('{"schema_version":', encoding="utf-8")
    assert reader(path).read().reason_code == "PROJECTION_MISSING_OR_CORRUPT"


def test_nc_sp_04_wrong_producer_is_rejected(tmp_path: Path) -> None:
    now = datetime(2026, 8, 24, 12, 0, tzinfo=UTC)
    _, output, envelope = publish(tmp_path, now)
    envelope["producer_id"] = "untrusted-projector"
    output.write_text(json.dumps(envelope), encoding="utf-8")
    assert reader(output).read(now=now + timedelta(seconds=1)).reason_code == "PROJECTION_PRODUCER_MISMATCH"


def test_nc_sp_05_old_generation_is_rejected(tmp_path: Path) -> None:
    now = datetime(2026, 8, 24, 12, 0, tzinfo=UTC)
    _, output, _ = publish(tmp_path, now)
    decision = reader(output).read(now=now + timedelta(seconds=1), expected_maintenance_generation=3)
    assert decision.reason_code == "MAINTENANCE_GENERATION_MISMATCH"


def test_nc_sp_06_wrong_target_is_rejected(tmp_path: Path) -> None:
    now = datetime(2026, 8, 24, 12, 0, tzinfo=UTC)
    _, output, _ = publish(tmp_path, now)
    wrong = {**TARGET, "ffmpeg_pid": 4200}
    assert reader(output).read(now=now + timedelta(seconds=1), expected_target_identity=wrong).reason_code == "TARGET_IDENTITY_MISMATCH"


def test_nc_sp_07_writable_consumer_boundary_is_rejected() -> None:
    assert ProjectionReader.mount_contract(read_only=False, consumer_has_write_credential=False).available is False
    assert ProjectionReader.mount_contract(read_only=True, consumer_has_write_credential=True).available is False
    assert ProjectionReader.mount_contract(read_only=True, consumer_has_write_credential=False).available is True


def test_nc_sp_08_restart_rechecks_corrupt_projection(tmp_path: Path) -> None:
    now = datetime(2026, 8, 24, 12, 0, tzinfo=UTC)
    _, output, _ = publish(tmp_path, now)
    high_water = tmp_path / "consumer-high-water.json"
    assert reader(output, high_water).read(now=now + timedelta(seconds=1)).available is True
    output.write_text("{}", encoding="utf-8")
    restarted = reader(output, high_water)
    assert restarted.read(now=now + timedelta(seconds=2)).available is False


def test_nc_sp_09_refresh_failure_does_not_extend_freshness(tmp_path: Path) -> None:
    now = datetime(2026, 8, 24, 12, 0, tzinfo=UTC)
    source, output, envelope = publish(tmp_path, now)
    source.unlink()
    projector = ProjectionProjector(
        source_path=source,
        output_path=output,
        sequence_path=tmp_path / "producer-sequence.json",
        producer_id="dell-maintenance-snapshot-projector",
        producer_instance_id="projector-1",
        expected_source_producer_id="arena-maintenance-shadow",
        ttl_seconds=5,
    )
    with suppress(ValueError):
        projector.publish(now=now + timedelta(seconds=2))
    persisted = json.loads(output.read_text(encoding="utf-8"))
    assert persisted["fresh_until"] == envelope["fresh_until"]
    assert reader(output).read(now=now + timedelta(seconds=6)).available is False


def test_nc_sp_10_reader_has_no_synchronous_remote_dependency() -> None:
    source = (Path(__file__).parents[2] / "src/snapshot_projection/model.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    imported = {alias.name.split(".", 1)[0] for node in ast.walk(tree) if isinstance(node, ast.Import) for alias in node.names}
    imported.update(str(node.module or "").split(".", 1)[0] for node in ast.walk(tree) if isinstance(node, ast.ImportFrom))
    assert imported.isdisjoint({"socket", "requests", "urllib", "http", "subprocess"})


def test_target_unknown_is_preserved(tmp_path: Path) -> None:
    now = datetime(2026, 8, 24, 12, 0, tzinfo=UTC)
    _, output, _ = publish(tmp_path, now, target_snapshot_status="UNSTABLE", target_identity=None)
    decision = reader(output).read(now=now + timedelta(seconds=1))
    assert decision.available is False
    assert decision.reason_code == "TARGET_SNAPSHOT_NOT_VALID"


def test_digest_detects_payload_tampering(tmp_path: Path) -> None:
    now = datetime(2026, 8, 24, 12, 0, tzinfo=UTC)
    _, output, envelope = publish(tmp_path, now)
    envelope["payload"]["maintenance_state"] = "ESTABLISHED"
    output.write_text(json.dumps(envelope), encoding="utf-8")
    assert reader(output).read(now=now + timedelta(seconds=1)).reason_code == "PROJECTION_INTEGRITY_FAILURE"
