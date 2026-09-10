from __future__ import annotations

import json
import os
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from cra_authority.monitoring_evidence import reject_control_semantics
from cra_dell_recovery.canonical import KeyRing
from cra_dell_recovery.observation import DellObservationContract
from cra_dell_recovery.time import isoformat_utc
from dell_recovery_agent.observation_publisher import (
    DellObservationPublisher,
    DellObservationPublisherConfig,
)
from monitoring_projection.live_adapter import (
    MonitoringLiveAdapter,
    MonitoringLiveAdapterConfig,
    MonitoringLiveSnapshot,
    PostgresMonitoringRepository,
)

ROOT = Path(__file__).resolve().parents[2]
NOW = datetime(2026, 8, 30, 12, 0, tzinfo=UTC)


def _write_json(path: Path, value: dict[str, Any], *, mode: int = 0o644) -> None:
    path.write_text(json.dumps(value), encoding="utf-8")
    os.chmod(path, mode)


def _target_identity(*, host_id: str = "dell-stream-runtime") -> dict[str, Any]:
    return {
        "host_id": host_id,
        "host_boot_id": "boot-a",
        "namespace": "stream-v3",
        "pod_uid": "pod-a",
        "container_name": "stream-engine",
        "container_id": "containerd://stream-engine-a",
        "ffmpeg_generation": "ffmpeg-generation-a",
        "ffmpeg_pid": 4100,
    }


def _target_snapshot(*, host_id: str = "dell-stream-runtime") -> dict[str, Any]:
    observed = NOW - timedelta(seconds=3)
    target = _target_identity(host_id=host_id)
    return {
        "schema": "cra_dell_recovery.target_snapshot.v1",
        "snapshot_id": "target-snapshot-a",
        "observed_at": isoformat_utc(observed),
        "valid_until": isoformat_utc(NOW + timedelta(seconds=20)),
        "source_revision": "a" * 64,
        "read_started_at": isoformat_utc(observed - timedelta(milliseconds=20)),
        "read_finished_at": isoformat_utc(observed),
        "status": "VALID",
        "reason_code": "SNAPSHOT_CONSISTENT",
        "target_identity": target,
        "runtime_snapshot_id": "runtime-snapshot-a",
        "runtime_status": "VALID",
        "runtime_reason_code": "RUNTIME_SNAPSHOT_CONSISTENT",
        "runtime_identity": {
            "host_id": host_id,
            "host_boot_id": target["host_boot_id"],
            "namespace": target["namespace"],
            "pod_uid": target["pod_uid"],
            "stream_engine_container_name": target["container_name"],
            "stream_engine_container_id": target["container_id"],
            "runtime_generation": "runtime-generation-a",
        },
        "runtime_container_ready": True,
    }


def _transport() -> dict[str, Any]:
    return {
        "ts_utc": isoformat_utc(NOW - timedelta(seconds=2)),
        "controller_id": "stream-recovery-controller-v11",
        "ffmpeg_pid": 4100,
        "ffmpeg_uptime_sec": 900,
        "metrics": {
            "bytes_sent_delta": 0,
            "bytes_sent": 20_000_000,
            "bytes_acked": 10_000_000,
            "bytes_elapsed_sec": 5,
            "send_mbps": 0.0,
            "send_q": 2_000_000,
            "lastsnd_ms": 7_000,
            "notsent": 2_000_000,
            "unacked": 32,
            "rto_ms": 1_000,
            "network_down": False,
            "remote_warning": True,
            "low_upload_pressure": True,
        },
        "network": {
            "gateway_ok": True,
            "public_ok_count": 2,
            "dns_ok": True,
            "tcp_probe_ok": True,
            "network_down": False,
        },
    }


def _controller_state() -> dict[str, Any]:
    return {
        "observed_ts": (NOW - timedelta(seconds=2)).timestamp(),
        "last_pid": 4100,
        "stall_streak": 4,
        "net_fail_streak": 0,
        "authorization": {"must_not_escape": True},
    }


def _maintenance() -> dict[str, Any]:
    observed = NOW - timedelta(seconds=2)
    fresh = NOW + timedelta(seconds=20)
    target = _target_identity()
    return {
        "schema_version": "maintenance.snapshot_projection.v1",
        "projection_id": "maintenance-projection-a",
        "source_observed_at": isoformat_utc(observed),
        "source_fresh_until": isoformat_utc(fresh),
        "fresh_until": isoformat_utc(fresh),
        "payload": {
            "observed_at": isoformat_utc(observed),
            "fresh_until": isoformat_utc(fresh),
            "available": True,
            "maintenance_state": "INACTIVE",
            "transaction_state": "INACTIVE",
            "source_target_identity": target,
            "target_identity": target,
            "authorization_id": "must-not-escape",
            "proof": {
                "integrity_ok": True,
                "positive_inactive_proof": True,
                "startup_reconciled": True,
                "transaction_uncertain": False,
                "unresolved_transaction_count": 0,
            },
        },
    }


def _publisher_environment(
    tmp_path: Path,
) -> tuple[DellObservationPublisher, DellObservationContract, Ed25519PrivateKey, dict[str, Path]]:
    private = Ed25519PrivateKey.from_private_bytes(bytes(range(11, 43)))
    private_path = tmp_path / "dell-observation-private.pem"
    private_path.write_bytes(
        private.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    os.chmod(private_path, 0o600)
    files = {
        "target": tmp_path / "target.json",
        "transport": tmp_path / "transport.json",
        "state": tmp_path / "controller-state.json",
        "maintenance": tmp_path / "maintenance.json",
        "private": private_path,
    }
    _write_json(files["target"], _target_snapshot())
    _write_json(files["transport"], _transport())
    _write_json(files["state"], _controller_state())
    _write_json(files["maintenance"], _maintenance())
    config_path = tmp_path / "publisher.json"
    _write_json(
        config_path,
        {
            "schema": "cra_dell_recovery.observation_publisher_config.v1",
            "publisher_release_id": "dell-observer-release-a",
            "source_instance_id": "dell-stream-runtime-observer",
            "source_release_id": "dell-observer-release-a",
            "expected_target": {
                "host_id": "dell-stream-runtime",
                "namespace": "stream-v3",
                "container_name": "stream-engine",
            },
            "target_snapshot_file": str(files["target"]),
            "transport_snapshot_file": str(files["transport"]),
            "controller_state_file": str(files["state"]),
            "maintenance_snapshot_file": str(files["maintenance"]),
            "target_snapshot_schema_file": str(ROOT / "contracts/cra_dell_recovery/target_snapshot.v1.schema.json"),
            "observation_schema_file": str(ROOT / "contracts/cra_dell_recovery/v1/observation_bundle.schema.json"),
            "signing_private_key_file": str(private_path),
            "key_id": "dell-observation-key-a",
            "maximum_transport_age_seconds": 10,
            "maximum_state_age_seconds": 10,
            "maximum_bundle_ttl_seconds": 10,
            "stall_confirm_threshold": 3,
            "stall_lastsnd_ms": 5_000,
            "stall_notsent_bytes": 1_000_000,
            "stall_unacked": 16,
            "healthy_lastsnd_ms": 2_000,
            "minimum_upload_progress_bytes": 1,
            "minimum_healthy_send_mbps": 0.1,
            "startup_gate_uptime_seconds": 30,
        },
    )
    publisher = DellObservationPublisher(DellObservationPublisherConfig.load(config_path))
    contract = DellObservationContract(
        ROOT / "contracts/cra_dell_recovery/v1/observation_bundle.schema.json",
        KeyRing({"dell-observation-key-a": private.public_key()}),
        allowed_sources={"dell-stream-runtime-observer": "dell-observer-release-a"},
        expected_host_id="dell-stream-runtime",
        expected_namespace="stream-v3",
    )
    return publisher, contract, private, files


def test_dell_publisher_emits_signed_sanitized_read_only_facts(tmp_path: Path) -> None:
    publisher, contract, _, _ = _publisher_environment(tmp_path)
    bundle = publisher.build(now=NOW)
    decoded = contract.decode(bundle, now=NOW)

    assert decoded.checks["tcp_stall"]["status"] == "CONFIRMED"
    assert decoded.checks["network_down"]["status"] == "FALSE"
    assert decoded.checks["maintenance"]["status"] == "FALSE"
    assert decoded.target.host_id == "dell-stream-runtime"
    assert "authorization" not in json.dumps(bundle).lower()
    assert "command" not in json.dumps(bundle).lower()
    reject_control_semantics(bundle)


def test_dell_publisher_uses_issuance_time_when_payload_changes_at_same_evidence_time(tmp_path: Path) -> None:
    publisher, _, _, files = _publisher_environment(tmp_path)
    first = publisher.build(now=NOW)
    maintenance = _maintenance()
    maintenance["projection_id"] = "maintenance-projection-b"
    _write_json(files["maintenance"], maintenance)
    second = publisher.build(now=NOW + timedelta(milliseconds=1))

    assert second["observed_at"] == first["observed_at"]
    assert second["observation_revision"] != first["observation_revision"]
    assert second["observation_sequence"] > first["observation_sequence"]


def test_dell_publisher_and_contract_fail_closed_on_target_key_and_payload_faults(tmp_path: Path) -> None:
    publisher, contract, _, files = _publisher_environment(tmp_path)
    bundle = publisher.build(now=NOW)
    tampered = json.loads(json.dumps(bundle))
    tampered["transport"]["metrics"]["notsent"] += 1
    with pytest.raises(Exception, match="signature|digest|SIGNATURE|PAYLOAD"):
        contract.decode(tampered, now=NOW)
    with pytest.raises(ValueError, match="DELL_OBSERVATION_EXPIRED"):
        contract.decode(bundle, now=NOW + timedelta(seconds=30))

    _write_json(files["target"], _target_snapshot(host_id="wrong-host"))
    with pytest.raises(ValueError, match="DELL_OBSERVATION_TARGET_IDENTITY_MISMATCH"):
        publisher.build(now=NOW)
    _write_json(files["target"], _target_snapshot())
    os.chmod(files["private"], 0o640)
    with pytest.raises(ValueError, match="DELL_OBSERVATION_SIGNING_KEY_PERMISSIONS_UNSAFE"):
        DellObservationPublisher(publisher.config)


def test_dell_publisher_emits_positive_post_recovery_health_only_from_fresh_progress(tmp_path: Path) -> None:
    publisher, contract, _, files = _publisher_environment(tmp_path)
    transport = _transport()
    transport["metrics"].update(
        {
            "bytes_sent_delta": 2_000_000,
            "send_mbps": 6.4,
            "lastsnd_ms": 100,
            "notsent": 0,
            "unacked": 0,
            "remote_warning": False,
            "low_upload_pressure": False,
        }
    )
    state = _controller_state()
    state["stall_streak"] = 0
    _write_json(files["transport"], transport)
    _write_json(files["state"], state)

    bundle = contract.decode(publisher.build(now=NOW), now=NOW)

    assert bundle.checks["tcp_stall"]["status"] == "FALSE"
    assert bundle.checks["stream_engine_ready"]["status"] == "TRUE"
    assert bundle.checks["tcp_flow_healthy"]["status"] == "TRUE"
    assert bundle.checks["upload_progress_healthy"]["status"] == "TRUE"
    assert bundle.checks["startup_gate"]["status"] == "TRUE"


def test_dell_publisher_preserves_unknown_rate_without_dropping_other_fresh_facts(tmp_path: Path) -> None:
    publisher, contract, _, files = _publisher_environment(tmp_path)
    transport = _transport()
    transport["metrics"].update(
        {
            "bytes_sent_delta": 2_000_000,
            "send_mbps": None,
            "lastsnd_ms": 100,
            "notsent": 0,
            "unacked": 0,
            "remote_warning": False,
            "low_upload_pressure": False,
        }
    )
    state = _controller_state()
    state["stall_streak"] = 0
    _write_json(files["transport"], transport)
    _write_json(files["state"], state)

    bundle = contract.decode(publisher.build(now=NOW), now=NOW)

    assert bundle.value["transport"]["metrics"]["send_mbps"] is None
    assert bundle.checks["tcp_flow_healthy"]["status"] == "TRUE"
    assert bundle.checks["upload_progress_healthy"]["status"] == "UNKNOWN"
    assert "rtmps_send_mbps" not in {item["name"] for item in bundle.value["measurements"]}


@pytest.mark.parametrize("invalid", [True, "not-a-number", -0.1, float("inf"), float("nan")])
def test_dell_publisher_rejects_non_null_invalid_rates(tmp_path: Path, invalid: object) -> None:
    publisher, _, _, files = _publisher_environment(tmp_path)
    transport = _transport()
    transport["metrics"]["send_mbps"] = invalid
    _write_json(files["transport"], transport)

    with pytest.raises(ValueError, match="DELL_OBSERVATION_METRIC_SEND_MBPS_INVALID"):
        publisher.build(now=NOW)


class FakeMonitoringRepository:
    def __init__(self, snapshot: MonitoringLiveSnapshot) -> None:
        self.snapshot = snapshot

    def latest(self) -> MonitoringLiveSnapshot:
        return self.snapshot


def _monitoring_snapshot(*, parity_equivalent: bool = True, current_state: str = "bad") -> MonitoringLiveSnapshot:
    current_observed = NOW - timedelta(seconds=4)
    parity = {
        "schema": "monitoring_v4.live_parity.v2",
        "equivalent": parity_equivalent,
        "accepted_difference_count": 0,
        "unclassified_contract_difference_count": 0 if parity_equivalent else 1,
        "input_integrity_errors": [],
        "domains": {
            "delivery": {
                "match": parity_equivalent,
                "classification": "equivalent" if parity_equivalent else "unclassified_contract_difference",
                "expected_state": current_state,
                "actual_state": current_state,
                "actual_observed_at": isoformat_utc(current_observed),
            }
        },
        "projection_integrity": {
            "complete": True,
            "expected_count": 6,
            "projection_count": 6,
            "rejection_count": 0,
            "missing_keys": [],
            "unexpected_keys": [],
        },
    }
    current = {
        "domain": "delivery",
        "snapshot_id": "delivery-snapshot-a",
        "state": current_state,
        "observed_at": isoformat_utc(current_observed),
        "reduced_at": isoformat_utc(NOW - timedelta(seconds=3)),
        "valid_until": isoformat_utc(NOW + timedelta(seconds=20)),
        "policy_revision": "delivery-policy-a",
        "reducer_revision": "delivery-reducer-a",
        "reason_codes_json": json.dumps(["delivery_stalled"] if current_state == "bad" else []),
        "source_observation_ids_json": json.dumps(["delivery-observation-a"]),
        "payload_json": "{}",
    }
    cycle = {
        "cycle_id": "monitoring-cycle-a",
        "started_at": isoformat_utc(NOW - timedelta(seconds=5)),
        "completed_at": isoformat_utc(NOW - timedelta(seconds=1)),
        "completed_ts": int((NOW - timedelta(seconds=1)).timestamp()),
        "build_revision": "monitoring-build-a",
        "source_revision": "stream-v3-source-a",
        "observer_json": "{}",
        "current_states_json": json.dumps({"delivery": current_state}),
        "parity_json": json.dumps(parity),
        "notification_intent_count": 0,
        "real_delivery_enabled": 0,
        "runtime_mutation_enabled": 0,
    }
    components = tuple(
        {
            "component": name,
            "status": "good",
            "checked_at": isoformat_utc(NOW - timedelta(seconds=1)),
            "detail": "ready",
        }
        for name in ("current_reducer", "observer", "reliability_projector")
    )
    return MonitoringLiveSnapshot(
        cycle=cycle,
        current=current,
        active_episode={
            "episode_id": "delivery-episode-a",
            "domain": "delivery",
            "status": "active",
            "severity": "critical",
            "opened_at": isoformat_utc(NOW - timedelta(minutes=1)),
            "last_bad_at": isoformat_utc(NOW - timedelta(seconds=4)),
            "last_transition_at": isoformat_utc(NOW - timedelta(seconds=4)),
            "policy_revision": "incident-policy-a",
            "summary": "delivery unhealthy",
            "reason_codes_json": json.dumps(["delivery_stalled"]),
        },
        latest_closed_episode=None,
        candidate=None,
        observations=(
            {
                "observation_id": "delivery-observation-a",
                "domain": "delivery",
                "source": "legacy-delivery",
                "source_generation": "source-generation-a",
                "evidence_role": "current_correlated",
                "status": "bad" if current_state == "bad" else "good",
                "reason_code": "delivery_stalled" if current_state == "bad" else "healthy",
                "observed_at": isoformat_utc(current_observed),
                "producer_revision": "producer-a",
                "payload_sha256": "b" * 64,
            },
        ),
        component_health=components,
        database_user="monitoring_cra_ro",
    )


def _adapter(
    tmp_path: Path,
    snapshot: MonitoringLiveSnapshot,
    *,
    consistency_retry_delays: tuple[float, ...] = (),
    validity_retry_delays: tuple[float, ...] = (),
) -> MonitoringLiveAdapter:
    publisher, _, private, _ = _publisher_environment(tmp_path)
    dell_file = tmp_path / "dell-observation.json"
    _write_json(dell_file, publisher.build(now=NOW), mode=0o600)
    public_path = tmp_path / "dell-observation-public.pem"
    public_path.write_bytes(
        private.public_key().public_bytes(
            serialization.Encoding.PEM,
            serialization.PublicFormat.SubjectPublicKeyInfo,
        )
    )
    os.chmod(public_path, 0o644)
    config_path = tmp_path / "live-adapter.json"
    _write_json(
        config_path,
        {
            "schema": "monitoring_v4.cra_live_adapter_config.v1",
            "adapter_release_id": "arena-live-adapter-a",
            "source_instance_id": "arena-monitoring-v4",
            "source_release_id": "arena-projection-release-a",
            "expected_monitoring_build_revision": "monitoring-build-a",
            "expected_monitoring_source_revision": "stream-v3-source-a",
            "target_id": "stream-target",
            "expected_database_role": "stream_monitoring_cra_projection_ro",
            "postgres_conninfo_file": str(tmp_path / "unused-postgres-conninfo"),
            "dell_observation_file": str(dell_file),
            "dell_observation_schema_file": str(ROOT / "contracts/cra_dell_recovery/v1/observation_bundle.schema.json"),
            "dell_key_id": "dell-observation-key-a",
            "dell_public_key_file": str(public_path),
            "dell_source_instance_id": "dell-stream-runtime-observer",
            "dell_source_release_id": "dell-observer-release-a",
            "expected_target": {
                "host_id": "dell-stream-runtime",
                "namespace": "stream-v3",
                "container_name": "stream-engine",
            },
            "fact_bundle_schema_file": str(ROOT / "contracts/monitoring_v4/cra_fact_bundle.v1.schema.json"),
            "output_file": str(tmp_path / "facts.json"),
            "target_snapshot_output_file": str(tmp_path / "verified-target-snapshot.json"),
            "status_file": str(tmp_path / "adapter-status.json"),
            "required_components": ["current_reducer", "observer", "reliability_projector"],
            "maximum_cycle_age_seconds": 90,
            "maximum_component_age_seconds": 90,
            "fact_ttl_seconds": 10,
            "statement_timeout_ms": 2_000,
        },
    )
    return MonitoringLiveAdapter(
        MonitoringLiveAdapterConfig.load(config_path),
        FakeMonitoringRepository(snapshot),
        consistency_retry_delays=consistency_retry_delays,
        validity_retry_delays=validity_retry_delays,
    )


def test_live_adapter_binds_cycle_parity_incident_and_dell_facts(tmp_path: Path) -> None:
    adapter = _adapter(tmp_path, _monitoring_snapshot())
    facts = adapter.build(now=NOW)

    assert facts["readiness"] == {
        "input_fresh": True,
        "parity_clean": True,
        "projection_clean": True,
        "source_ready": True,
    }
    assert facts["incident"]["state"] == "CONFIRMED"
    assert "confirmed_tcp_stall" in facts["incident"]["reason_codes"]
    assert facts["checks"]["tcp_stall"]["status"] == "CONFIRMED"
    assert facts["checks"]["delivery_bad"]["status"] == "TRUE"
    assert facts["checks"]["maintenance"]["status"] == "FALSE"
    assert not hasattr(adapter, "authorize")
    assert not hasattr(adapter, "create_command")
    assert not hasattr(adapter, "perform_effect")
    reject_control_semantics(facts)
    status = adapter.run_once(now=NOW)
    assert status["status"] == "READY"
    assert status["schema"] == "monitoring_v4.cra_live_adapter_status.v2"
    assert status["parity"]["schema"] == "monitoring_v4.live_parity.v2"
    assert status["control_capability_count"] == 0
    assert json.loads(Path(adapter.config.value["output_file"]).read_text())["observation_revision"] == facts["observation_revision"]
    assert json.loads(Path(adapter.config.value["target_snapshot_output_file"]).read_text())["snapshot_id"] == ("target-snapshot-a")


def test_live_adapter_sequences_repeated_evidence_by_adapter_issuance_time(tmp_path: Path) -> None:
    adapter = _adapter(tmp_path, _monitoring_snapshot())
    first = adapter.build(now=NOW)
    second = adapter.build(now=NOW + timedelta(milliseconds=1))

    assert second["observed_at"] == first["observed_at"]
    assert second["observation_sequence"] > first["observation_sequence"]


def test_live_adapter_preserves_not_ready_and_blocks_cycle_or_source_mismatch(tmp_path: Path) -> None:
    snapshot = _monitoring_snapshot(parity_equivalent=False)
    adapter = _adapter(tmp_path, snapshot)
    assert adapter.build(now=NOW)["readiness"]["parity_clean"] is False

    mismatched_cycle = dict(snapshot.cycle)
    mismatched_cycle["current_states_json"] = json.dumps({"delivery": "good"})
    adapter.repository = FakeMonitoringRepository(replace(snapshot, cycle=mismatched_cycle))
    with pytest.raises(ValueError, match="LIVE_ADAPTER_CYCLE_CURRENT_STATE_MISMATCH"):
        adapter.build(now=NOW)

    wrong_source = dict(snapshot.cycle)
    wrong_source["source_revision"] = "unexpected-source"
    adapter.repository = FakeMonitoringRepository(replace(snapshot, cycle=wrong_source))
    with pytest.raises(ValueError, match="LIVE_ADAPTER_MONITORING_SOURCE_REVISION_MISMATCH"):
        adapter.build(now=NOW)


def test_live_adapter_requires_delivery_bad_for_action_eligibility(tmp_path: Path) -> None:
    snapshot = _monitoring_snapshot(current_state="good")
    adapter = _adapter(tmp_path, snapshot)
    facts = adapter.build(now=NOW)

    assert facts["incident"]["state"] == "CONFIRMED"
    assert facts["checks"]["tcp_stall"]["status"] == "CONFIRMED"
    assert facts["checks"]["delivery_bad"]["status"] == "FALSE"
    assert "confirmed_tcp_stall" not in facts["incident"]["reason_codes"]


def test_live_adapter_preserves_closed_episode_lineage_for_cra_verifier(tmp_path: Path) -> None:
    snapshot = _monitoring_snapshot(current_state="good")
    assert snapshot.active_episode is not None
    closed = {
        **snapshot.active_episode,
        "status": "closed",
        "closed_at": isoformat_utc(NOW - timedelta(seconds=2)),
    }
    post_snapshot = replace(
        snapshot,
        active_episode=None,
        latest_closed_episode=closed,
    )
    facts = _adapter(tmp_path, post_snapshot).build(now=NOW)

    assert facts["incident"]["state"] == "CLEAR"
    assert facts["incident"]["incident_id"] == "delivery-episode-a"
    assert facts["incident"]["source_episode_id"] == "delivery-episode-a"


class _Cursor:
    def __init__(self, *, one: dict[str, Any] | None = None, all_rows: list[dict[str, Any]] | None = None) -> None:
        self.one = one
        self.all_rows = all_rows or []

    def fetchone(self) -> dict[str, Any] | None:
        return self.one

    def fetchall(self) -> list[dict[str, Any]]:
        return self.all_rows


class _FakePostgresConnection:
    def __init__(
        self,
        snapshot: MonitoringLiveSnapshot,
        *,
        can_mutate: bool = False,
        multiple_active: bool = False,
        database_role: str = "stream_monitoring_cra_projection_ro",
        unsafe_capability: bool = False,
    ) -> None:
        self.snapshot = snapshot
        self.can_mutate = can_mutate
        self.multiple_active = multiple_active
        self.database_role = database_role
        self.unsafe_capability = unsafe_capability
        self.statements: list[str] = []

    def __enter__(self) -> _FakePostgresConnection:
        return self

    def __exit__(self, *_: object) -> None:
        return None

    def transaction(self) -> _FakePostgresConnection:
        return self

    def execute(self, statement: str, _parameters: object = None) -> _Cursor:
        normalized = " ".join(statement.split())
        self.statements.append(normalized)
        if "current_user AS database_user" in normalized:
            return _Cursor(
                one={
                    "database_user": self.database_role,
                    "transaction_read_only": "on",
                    "is_superuser": self.unsafe_capability,
                    "can_create_role": False,
                    "can_create_database": False,
                    "inherits_privileges": False,
                    "can_replicate": False,
                    "can_bypass_rls": False,
                    "can_login": True,
                    "has_role_membership": False,
                    "can_create_in_database": False,
                    "can_create_in_schema": False,
                }
            )
        if "has_table_privilege" in normalized:
            return _Cursor(one={"can_mutate": self.can_mutate})
        if "FROM public.shadow_cycles" in normalized:
            return _Cursor(one=self.snapshot.cycle)
        if "FROM public.domain_current" in normalized:
            return _Cursor(one=self.snapshot.current)
        if "status='active'" in normalized:
            rows = [self.snapshot.active_episode] if self.snapshot.active_episode is not None else []
            return _Cursor(all_rows=rows * (2 if self.multiple_active else 1))
        if "status='closed'" in normalized:
            return _Cursor(one=self.snapshot.latest_closed_episode)
        if "FROM public.incident_candidates" in normalized:
            return _Cursor(one=self.snapshot.candidate)
        if "FROM public.component_health" in normalized:
            return _Cursor(all_rows=list(self.snapshot.component_health))
        if "FROM public.observations" in normalized:
            return _Cursor(all_rows=list(self.snapshot.observations))
        return _Cursor()


def test_postgres_repository_uses_one_read_only_snapshot_and_fixed_selects(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conninfo = tmp_path / "postgres.conninfo"
    conninfo.write_text("host=postgres.invalid dbname=monitoring user=readonly", encoding="utf-8")
    os.chmod(conninfo, 0o600)
    connection = _FakePostgresConnection(_monitoring_snapshot())
    monkeypatch.setattr("monitoring_projection.live_adapter.psycopg.connect", lambda *_args, **_kwargs: connection)

    snapshot = PostgresMonitoringRepository(
        conninfo,
        statement_timeout_ms=2_000,
        expected_database_role="stream_monitoring_cra_projection_ro",
    ).latest()

    assert snapshot.current["snapshot_id"] == "delivery-snapshot-a"
    assert connection.statements[0] == "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"
    forbidden_prefixes = ("INSERT ", "UPDATE ", "DELETE ", "TRUNCATE ", "ALTER ", "CREATE ", "DROP ")
    assert not [statement for statement in connection.statements if statement.upper().startswith(forbidden_prefixes)]
    assert sum("has_table_privilege" in statement for statement in connection.statements) == 6


def test_postgres_repository_rejects_role_with_any_mutation_privilege(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conninfo = tmp_path / "postgres.conninfo"
    conninfo.write_text("host=postgres.invalid dbname=monitoring user=unsafe", encoding="utf-8")
    os.chmod(conninfo, 0o600)
    connection = _FakePostgresConnection(_monitoring_snapshot(), can_mutate=True)
    monkeypatch.setattr("monitoring_projection.live_adapter.psycopg.connect", lambda *_args, **_kwargs: connection)

    with pytest.raises(ValueError, match="LIVE_ADAPTER_DATABASE_ROLE_CAN_MUTATE"):
        PostgresMonitoringRepository(
            conninfo,
            statement_timeout_ms=2_000,
            expected_database_role="stream_monitoring_cra_projection_ro",
        ).latest()


@pytest.mark.parametrize(
    ("connection", "error_code"),
    [
        (
            _FakePostgresConnection(_monitoring_snapshot(), database_role="unexpected_role"),
            "LIVE_ADAPTER_DATABASE_ROLE_MISMATCH",
        ),
        (
            _FakePostgresConnection(_monitoring_snapshot(), unsafe_capability=True),
            "LIVE_ADAPTER_DATABASE_ROLE_CAPABILITY_UNSAFE",
        ),
    ],
)
def test_postgres_repository_rejects_wrong_or_powerful_database_role(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    connection: _FakePostgresConnection,
    error_code: str,
) -> None:
    conninfo = tmp_path / "postgres.conninfo"
    conninfo.write_text("host=postgres.invalid dbname=monitoring user=unsafe", encoding="utf-8")
    os.chmod(conninfo, 0o600)
    monkeypatch.setattr("monitoring_projection.live_adapter.psycopg.connect", lambda *_args, **_kwargs: connection)

    with pytest.raises(ValueError, match=error_code):
        PostgresMonitoringRepository(
            conninfo,
            statement_timeout_ms=2_000,
            expected_database_role="stream_monitoring_cra_projection_ro",
        ).latest()


def test_postgres_repository_rejects_ambiguous_active_delivery_episode(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conninfo = tmp_path / "postgres.conninfo"
    conninfo.write_text("host=postgres.invalid dbname=monitoring user=readonly", encoding="utf-8")
    os.chmod(conninfo, 0o600)
    connection = _FakePostgresConnection(_monitoring_snapshot(), multiple_active=True)
    monkeypatch.setattr("monitoring_projection.live_adapter.psycopg.connect", lambda *_args, **_kwargs: connection)

    with pytest.raises(ValueError, match="LIVE_ADAPTER_MULTIPLE_ACTIVE_DELIVERY_EPISODES"):
        PostgresMonitoringRepository(
            conninfo,
            statement_timeout_ms=2_000,
            expected_database_role="stream_monitoring_cra_projection_ro",
        ).latest()


def test_live_adapter_rejects_current_reduced_after_selected_cycle(tmp_path: Path) -> None:
    snapshot = _monitoring_snapshot()
    snapshot.current["reduced_at"] = isoformat_utc(NOW)

    with pytest.raises(ValueError, match="LIVE_ADAPTER_DELIVERY_CURRENT_REDUCED_AFTER_CYCLE"):
        _adapter(tmp_path, snapshot).build(now=NOW)


class _SequenceMonitoringRepository:
    def __init__(self, snapshots: list[MonitoringLiveSnapshot]) -> None:
        self.snapshots = list(snapshots)
        self.calls = 0

    def latest(self) -> MonitoringLiveSnapshot:
        self.calls += 1
        if len(self.snapshots) > 1:
            return self.snapshots.pop(0)
        return self.snapshots[0]


def test_live_adapter_retries_only_bounded_temporal_cycle_race(tmp_path: Path) -> None:
    coherent = _monitoring_snapshot()
    racing_current = dict(coherent.current)
    racing_current["state"] = "good"
    racing_current["observed_at"] = isoformat_utc(NOW)
    racing_current["reduced_at"] = isoformat_utc(NOW)
    racing = replace(coherent, current=racing_current)
    repository = _SequenceMonitoringRepository([racing, coherent])
    adapter = _adapter(tmp_path, coherent, consistency_retry_delays=(0.0,))
    adapter.repository = repository

    facts = adapter.build(now=NOW)

    assert facts["monitoring_cycle_id"] == coherent.cycle["cycle_id"]
    assert repository.calls == 2


def test_live_adapter_stays_fail_closed_when_temporal_cycle_race_persists(tmp_path: Path) -> None:
    racing = _monitoring_snapshot()
    racing.current["observed_at"] = isoformat_utc(NOW)
    repository = _SequenceMonitoringRepository([racing])
    adapter = _adapter(tmp_path, racing, consistency_retry_delays=(0.0, 0.0))
    adapter.repository = repository

    with pytest.raises(ValueError, match="LIVE_ADAPTER_DELIVERY_CURRENT_NEWER_THAN_CYCLE"):
        adapter.build(now=NOW)

    assert repository.calls == 3


def test_live_adapter_retries_expired_snapshot_until_next_cycle_is_valid(tmp_path: Path) -> None:
    coherent = _monitoring_snapshot()
    expired_current = dict(coherent.current)
    expired_current["valid_until"] = isoformat_utc(NOW)
    expired = replace(coherent, current=expired_current)
    repository = _SequenceMonitoringRepository([expired, coherent])
    adapter = _adapter(tmp_path, coherent, validity_retry_delays=(0.0,))
    adapter.repository = repository

    status = adapter.run_once(now=NOW)

    assert status["status"] == "READY"
    assert status["validity_retry_count"] == 1
    assert repository.calls == 2


def test_live_adapter_run_once_does_not_freeze_production_time_before_retry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = _adapter(tmp_path, _monitoring_snapshot())
    received_times: list[datetime | None] = []

    def fake_build(*, now: datetime | None = None) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
        received_times.append(now)
        adapter._last_validity_retry_count = 2
        return (
            {
                "monitoring_cycle_id": "monitoring-cycle-after-retry",
                "observation_sequence": 2,
                "readiness": {
                    "input_fresh": True,
                    "parity_clean": True,
                    "projection_clean": True,
                    "source_ready": True,
                },
            },
            {"snapshot_id": "target-after-retry"},
            {"schema": "monitoring_v4.live_parity.v2"},
        )

    monkeypatch.setattr(adapter, "_build_artifacts", fake_build)
    before = datetime.now(UTC)
    status = adapter.run_once()
    after = datetime.now(UTC)

    assert received_times == [None]
    assert status["validity_retry_count"] == 2
    observed = datetime.fromisoformat(status["observed_at"].replace("Z", "+00:00"))
    assert before - timedelta(milliseconds=1) <= observed <= after


def test_live_adapter_stays_fail_closed_when_validity_gap_persists(tmp_path: Path) -> None:
    expired = _monitoring_snapshot()
    expired.current["valid_until"] = isoformat_utc(NOW)
    repository = _SequenceMonitoringRepository([expired])
    adapter = _adapter(tmp_path, expired, validity_retry_delays=(0.0, 0.0))
    adapter.repository = repository

    with pytest.raises(ValueError, match="LIVE_ADAPTER_FACT_HAS_NO_VALIDITY_WINDOW"):
        adapter.build(now=NOW)

    assert repository.calls == 3


def test_live_adapter_does_not_retry_future_evidence_as_validity_gap(tmp_path: Path) -> None:
    future = _monitoring_snapshot()
    future_components = tuple({**item, "checked_at": isoformat_utc(NOW + timedelta(seconds=1))} for item in future.component_health)
    future = replace(future, component_health=future_components)
    repository = _SequenceMonitoringRepository([future, _monitoring_snapshot()])
    adapter = _adapter(tmp_path, future, validity_retry_delays=(0.0,))
    adapter.repository = repository

    with pytest.raises(ValueError, match="LIVE_ADAPTER_FACT_OBSERVED_IN_FUTURE"):
        adapter.build(now=NOW)

    assert repository.calls == 1


@pytest.mark.parametrize("retry_delay", [float("nan"), float("inf"), -0.1, True])
def test_live_adapter_rejects_invalid_consistency_retry_delay(tmp_path: Path, retry_delay: float) -> None:
    with pytest.raises(ValueError, match="LIVE_ADAPTER_CONSISTENCY_RETRY_DELAY_INVALID"):
        _adapter(tmp_path, _monitoring_snapshot(), consistency_retry_delays=(retry_delay,))


@pytest.mark.parametrize("retry_delay", [float("nan"), float("inf"), -0.1, True])
def test_live_adapter_rejects_invalid_validity_retry_delay(tmp_path: Path, retry_delay: float) -> None:
    with pytest.raises(ValueError, match="LIVE_ADAPTER_VALIDITY_RETRY_DELAY_INVALID"):
        _adapter(tmp_path, _monitoring_snapshot(), validity_retry_delays=(retry_delay,))


@pytest.mark.parametrize(
    ("field", "value", "error_code"),
    [
        ("fact_ttl_seconds", True, "LIVE_ADAPTER_CONFIG_FACT_TTL_SECONDS_OUT_OF_RANGE"),
        ("maximum_cycle_age_seconds", "90", "LIVE_ADAPTER_CONFIG_MAXIMUM_CYCLE_AGE_SECONDS_OUT_OF_RANGE"),
        ("statement_timeout_ms", "2000", "LIVE_ADAPTER_CONFIG_STATEMENT_TIMEOUT_MS_OUT_OF_RANGE"),
    ],
)
def test_live_adapter_config_rejects_boolean_or_string_numeric_values(
    tmp_path: Path,
    field: str,
    value: object,
    error_code: str,
) -> None:
    adapter = _adapter(tmp_path, _monitoring_snapshot())
    config_path = adapter.config.path
    config = json.loads(config_path.read_text(encoding="utf-8"))
    config[field] = value
    _write_json(config_path, config)

    with pytest.raises(ValueError, match=error_code):
        MonitoringLiveAdapterConfig.load(config_path)


def test_live_adapter_does_not_retry_release_identity_mismatch_hidden_by_temporal_race(tmp_path: Path) -> None:
    coherent = _monitoring_snapshot()
    unsafe_cycle = dict(coherent.cycle)
    unsafe_cycle["build_revision"] = "unexpected-build"
    racing_current = dict(coherent.current)
    racing_current["observed_at"] = isoformat_utc(NOW)
    unsafe = replace(coherent, cycle=unsafe_cycle, current=racing_current)
    repository = _SequenceMonitoringRepository([unsafe, coherent])
    adapter = _adapter(tmp_path, coherent, consistency_retry_delays=(0.0,))
    adapter.repository = repository

    with pytest.raises(ValueError, match="LIVE_ADAPTER_MONITORING_BUILD_REVISION_MISMATCH"):
        adapter.build(now=NOW)

    assert repository.calls == 1
