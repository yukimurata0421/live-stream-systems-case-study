from __future__ import annotations

import json
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from jsonschema import ValidationError

from cra_authority.monitoring_evidence import MonitoringEvidenceContract
from cra_dell_recovery.canonical import KeyRing
from cra_dell_recovery.time import isoformat_utc
from monitoring_projection import MonitoringProjectionProducer, ProjectionProducerConfig

ROOT = Path(__file__).resolve().parents[2]
NOW = datetime(2026, 8, 30, 6, 0, tzinfo=UTC)


def _write_json(path: Path, value: dict[str, Any]) -> None:
    path.write_text(json.dumps(value), encoding="utf-8")
    os.chmod(path, 0o644)


def _facts(*, sequence: int = 7) -> dict[str, Any]:
    observed = NOW - timedelta(seconds=2)
    return {
        "schema": "monitoring_v4.cra_fact_bundle.v1",
        "target_id": "stream-target",
        "source_instance_id": "arena-monitoring-v4",
        "source_release_id": "monitoring-release-a",
        "monitoring_cycle_id": f"cycle-{sequence}",
        "observation_revision": f"revision-{sequence}",
        "observation_sequence": sequence,
        "observed_at": isoformat_utc(observed),
        "valid_until": isoformat_utc(NOW + timedelta(seconds=40)),
        "readiness": {
            "input_fresh": True,
            "parity_clean": True,
            "projection_clean": True,
            "source_ready": True,
        },
        "incident": {
            "incident_id": "incident-7",
            "source_episode_id": "episode-7",
            "domain": "network_transport",
            "state": "CONFIRMED",
            "reason_codes": ["rtmps_tcp_stall"],
        },
        "checks": {
            name: {
                "status": status,
                "observed_at": isoformat_utc(observed),
                "evidence_ref": f"arena/check/{name}/7",
            }
            for name, status in {
                "tcp_stall": "CONFIRMED",
                "network_down": "FALSE",
                "ffmpeg_present": "TRUE",
                "target_stable": "TRUE",
                "maintenance": "FALSE",
                "delivery_bad": "TRUE",
            }.items()
        },
        "measurements": [
            {
                "name": "rtmps_bytes_delta",
                "value": 0,
                "unit": "bytes",
                "evidence_ref": "arena/measurement/rtmps-bytes/7",
            }
        ],
        "evidence_refs": ["arena/cycle/7", "dell/target/snapshot-7"],
    }


def _target(*, host_id: str = "dell-stream-runtime") -> dict[str, Any]:
    observed = NOW - timedelta(seconds=1)
    return {
        "schema": "cra_dell_recovery.target_snapshot.v1",
        "snapshot_id": "snapshot-7",
        "observed_at": isoformat_utc(observed),
        "valid_until": isoformat_utc(NOW + timedelta(seconds=9)),
        "source_revision": "a" * 64,
        "read_started_at": isoformat_utc(observed - timedelta(milliseconds=20)),
        "read_finished_at": isoformat_utc(observed),
        "status": "VALID",
        "reason_code": "SNAPSHOT_CONSISTENT",
        "target_identity": {
            "host_id": host_id,
            "host_boot_id": "boot-a",
            "namespace": "stream-v3",
            "pod_uid": "pod-a",
            "container_name": "stream-engine",
            "container_id": "containerd://a",
            "ffmpeg_generation": "ffmpeg-generation-a",
            "ffmpeg_pid": 4100,
        },
        "runtime_snapshot_id": "runtime-snapshot-7",
        "runtime_status": "VALID",
        "runtime_reason_code": "RUNTIME_SNAPSHOT_CONSISTENT",
        "runtime_identity": {
            "host_id": host_id,
            "host_boot_id": "boot-a",
            "namespace": "stream-v3",
            "pod_uid": "pod-a",
            "stream_engine_container_name": "stream-engine",
            "stream_engine_container_id": "containerd://a",
            "runtime_generation": "runtime-generation-a",
        },
        "runtime_container_ready": True,
    }


def _environment(tmp_path: Path) -> tuple[ProjectionProducerConfig, Ed25519PrivateKey]:
    private = Ed25519PrivateKey.from_private_bytes(bytes(range(121, 153)))
    key = tmp_path / "monitoring-private.pem"
    key.write_bytes(
        private.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    os.chmod(key, 0o600)
    facts = tmp_path / "facts.json"
    target = tmp_path / "target.json"
    _write_json(facts, _facts())
    _write_json(target, _target())
    config_path = tmp_path / "producer.json"
    config = {
        "schema": "monitoring_v4.cra_projection_producer_config.v1",
        "producer_release_id": "projection-producer-release-a",
        "source_instance_id": "arena-monitoring-v4",
        "source_release_id": "monitoring-release-a",
        "target_id": "stream-target",
        "expected_target": {
            "host_id": "dell-stream-runtime",
            "namespace": "stream-v3",
            "container_name": "stream-engine",
        },
        "fact_bundle_file": str(facts),
        "target_snapshot_file": str(target),
        "fact_bundle_schema_file": str(ROOT / "contracts/monitoring_v4/cra_fact_bundle.v1.schema.json"),
        "target_snapshot_schema_file": str(ROOT / "contracts/cra_dell_recovery/target_snapshot.v1.schema.json"),
        "projection_schema_file": str(ROOT / "contracts/monitoring_v4/evidence_projection.v1.schema.json"),
        "private_key_file": str(key),
        "key_id": "monitoring-key-a",
        "output_file": str(tmp_path / "projection.json"),
        "status_file": str(tmp_path / "producer-status.json"),
        "state_database": str(tmp_path / "projection-state.sqlite3"),
        "projection_ttl_seconds": 30,
        "maximum_fact_age_seconds": 60,
        "maximum_target_age_seconds": 10,
        "maximum_observation_skew_seconds": 10,
    }
    _write_json(config_path, config)
    return ProjectionProducerConfig.load(config_path), private


def test_producer_builds_signed_facts_only_projection_and_replays_exactly(tmp_path: Path) -> None:
    config, private = _environment(tmp_path)
    producer = MonitoringProjectionProducer(config)
    try:
        first = producer.produce(now=NOW)
        replay = producer.produce(now=NOW + timedelta(seconds=1))
    finally:
        producer.close()

    contract = MonitoringEvidenceContract(
        ROOT / "contracts/monitoring_v4/evidence_projection.v1.schema.json",
        KeyRing({"monitoring-key-a": private.public_key()}),
        allowed_sources={"arena-monitoring-v4": "monitoring-release-a"},
    )
    decoded = contract.decode(first, now=NOW)
    assert decoded.projection_id == replay["projection_id"]
    assert first == replay
    assert first["observed_target"]["ffmpeg_pid"] == 4100
    assert "dell/target/snapshot-7" in first["evidence_refs"]
    assert "action" not in first
    assert "authorization" not in first
    assert "verdict" not in first
    assert not hasattr(producer, "create_command")
    assert not hasattr(producer, "perform_effect")
    status = json.loads(Path(config.value["status_file"]).read_text(encoding="utf-8"))
    assert status["replayed"] is True
    assert status["control_capability_count"] == 0


def test_producer_rejects_sequence_regression_and_same_sequence_conflict(tmp_path: Path) -> None:
    config, _ = _environment(tmp_path)
    producer = MonitoringProjectionProducer(config)
    try:
        producer.produce(now=NOW)
        _write_json(Path(config.value["fact_bundle_file"]), _facts(sequence=6))
        with pytest.raises(ValueError, match="MONITORING_FACT_SEQUENCE_REGRESSION"):
            producer.produce(now=NOW)
        conflicting = _facts(sequence=7)
        conflicting["incident"]["reason_codes"] = ["different_fact"]
        _write_json(Path(config.value["fact_bundle_file"]), conflicting)
        with pytest.raises(ValueError, match="MONITORING_FACT_SEQUENCE_CONFLICT"):
            producer.produce(now=NOW)
    finally:
        producer.close()


def test_producer_rejects_control_semantics_and_stale_or_wrong_target(tmp_path: Path) -> None:
    config, _ = _environment(tmp_path)
    facts = _facts()
    facts["action"] = "restart_ffmpeg"
    _write_json(Path(config.value["fact_bundle_file"]), facts)
    producer = MonitoringProjectionProducer(config)
    try:
        with pytest.raises(ValidationError):
            producer.produce(now=NOW)
        _write_json(Path(config.value["fact_bundle_file"]), _facts())
        stale = _target()
        stale["valid_until"] = isoformat_utc(NOW - timedelta(seconds=1))
        _write_json(Path(config.value["target_snapshot_file"]), stale)
        with pytest.raises(ValueError, match="DELL_TARGET_SNAPSHOT_EXPIRED"):
            producer.produce(now=NOW)
        _write_json(Path(config.value["target_snapshot_file"]), _target(host_id="wrong-host"))
        with pytest.raises(ValueError, match="DELL_TARGET_SNAPSHOT_IDENTITY_MISMATCH"):
            producer.produce(now=NOW)
    finally:
        producer.close()


def test_producer_rejects_unsafe_private_key_permissions(tmp_path: Path) -> None:
    config, _ = _environment(tmp_path)
    os.chmod(Path(config.value["private_key_file"]), 0o640)
    with pytest.raises(ValueError, match="MONITORING_SIGNING_KEY_PERMISSIONS_UNSAFE"):
        MonitoringProjectionProducer(config)


def test_producer_rejects_fact_and_target_snapshot_mixed_generation(tmp_path: Path) -> None:
    config, _ = _environment(tmp_path)
    facts = _facts()
    facts["evidence_refs"] = ["arena/cycle/7", "dell/target/different-snapshot"]
    _write_json(Path(config.value["fact_bundle_file"]), facts)
    producer = MonitoringProjectionProducer(config)
    try:
        with pytest.raises(ValueError, match="MONITORING_FACT_TARGET_SNAPSHOT_REFERENCE_MISMATCH"):
            producer.produce(now=NOW)
    finally:
        producer.close()
