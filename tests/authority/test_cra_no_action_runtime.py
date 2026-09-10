from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import timedelta
from pathlib import Path

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from cra_authority.authorizer import CraAuthorizer, RecoveryPolicy
from cra_authority.no_action_store import NoActionCentralStore
from cra_authority.runtime import CraNoActionRuntime, RuntimeConfig, SingletonLock
from cra_dell_recovery.canonical import Signer
from cra_dell_recovery.time import isoformat_utc, utc_now

ROOT = Path(__file__).resolve().parents[2]


def _write_runtime_fixture(
    tmp_path: Path,
    *,
    provisioned: bool = True,
    delivery_check_age_seconds: float = 1,
) -> Path:
    private = Ed25519PrivateKey.from_private_bytes(bytes(range(121, 153)))
    public_path = tmp_path / "monitoring-public.pem"
    public_path.write_bytes(
        private.public_key().public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo,
        )
    )
    os.chmod(public_path, 0o644)
    now = utc_now()
    observed = now - timedelta(seconds=1)
    target = {
        "host_id": "dell",
        "host_boot_id": "boot-a",
        "namespace": "stream-v3",
        "pod_uid": "pod-a",
        "container_name": "stream-engine",
        "container_id": "containerd://a",
        "ffmpeg_generation": "generation-a",
        "ffmpeg_pid": 4100,
    }
    statuses = {
        "tcp_stall": "CONFIRMED",
        "network_down": "FALSE",
        "ffmpeg_present": "TRUE",
        "target_stable": "TRUE",
        "maintenance": "FALSE",
        "delivery_bad": "TRUE",
    }
    unsigned = {
        "schema": "monitoring_v4.evidence_projection.v1",
        "projection_id": "runtime-projection-1",
        "target_id": "stream-target",
        "source_instance_id": "arena-monitoring-v4",
        "source_release_id": "monitoring-release-a",
        "monitoring_cycle_id": "runtime-cycle-1",
        "observation_revision": "runtime-revision-1",
        "observation_sequence": 1,
        "observed_at": isoformat_utc(observed),
        "issued_at": isoformat_utc(now),
        "expires_at": isoformat_utc(now + timedelta(seconds=45)),
        "readiness": {"input_fresh": True, "parity_clean": True, "projection_clean": True, "source_ready": True},
        "incident": {
            "incident_id": "runtime-incident-1",
            "source_episode_id": "runtime-episode-1",
            "domain": "network_transport",
            "state": "CONFIRMED",
            "reason_codes": ["confirmed_tcp_stall"],
        },
        "observed_target": target,
        "checks": {
            name: {
                "status": status,
                "observed_at": isoformat_utc(observed),
                "evidence_ref": f"arena/runtime/{name}",
            }
            for name, status in statuses.items()
        },
        "measurements": [],
        "evidence_refs": ["arena/runtime/cycle-1"],
        "key_id": "monitoring-key-a",
    }
    unsigned["checks"]["delivery_bad"]["observed_at"] = isoformat_utc(now - timedelta(seconds=delivery_check_age_seconds))
    projection_path = tmp_path / "inbox/latest.json"
    projection_path.parent.mkdir()
    projection_path.write_text(json.dumps(Signer("monitoring-key-a", private).sign(unsigned)), encoding="utf-8")
    os.chmod(projection_path, 0o600)
    config = {
        "schema": "cra.runtime.v1",
        "operating_mode": "NO_ACTION",
        "runtime_release_id": "test-release-a",
        "database": str(tmp_path / "cra/central.db"),
        "migration": str(ROOT / "migrations/central/001_initial.sql"),
        "target_id": "stream-target",
        "lock_file": str(tmp_path / "cra/runtime.lock"),
        "status_file": str(tmp_path / "cra/status.json"),
        "cycle_interval_seconds": 2,
        "monitoring": {
            "projection_file": str(projection_path),
            "schema_file": str(ROOT / "contracts/monitoring_v4/evidence_projection.v1.schema.json"),
            "source_instance_id": "arena-monitoring-v4",
            "source_release_id": "monitoring-release-a",
            "key_id": "monitoring-key-a",
            "public_key_file": str(public_path),
            "maximum_ttl_seconds": 60,
            "maximum_check_age_seconds": 180,
        },
        "policy": {
            "revision": "shadow-policy-v1",
            "authorization_lifetime_seconds": 30,
            "minimum_action_interval_seconds": 60,
            "hourly_action_limit": 24,
            "daily_action_limit": 96,
        },
    }
    path = tmp_path / "cra-runtime.json"
    path.write_text(json.dumps(config), encoding="utf-8")
    os.chmod(path, 0o600)
    provisioning = tmp_path / "cra-provisioning.json"
    provisioning.write_text(
        json.dumps(
            {
                "host_id": "dell",
                "agent_id": "dell-agent",
                "controller_instance_id": "cra-controller",
                "agent_installation_id": "dell-installation-a",
            }
        ),
        encoding="utf-8",
    )
    os.chmod(provisioning, 0o600)
    if provisioned:
        store = NoActionCentralStore(Path(config["database"]), Path(config["migration"]))
        store.bootstrap(
            target_id="stream-target",
            host_id="dell",
            agent_id="dell-agent",
            controller_instance_id="cra-controller",
            agent_installation_id="dell-installation-a",
        )
        store.close()
    return path


def test_runtime_evaluates_real_contract_but_cannot_materialize_or_deliver_command(tmp_path: Path) -> None:
    config = RuntimeConfig.load(_write_runtime_fixture(tmp_path))
    runtime = CraNoActionRuntime(config)
    try:
        status = runtime.run_once()
        assert status["policy_decision"] == "WOULD_AUTHORIZE"
        assert status["policy_candidate_reason_code"] == "confirmed_tcp_stall"
        assert status["policy_decision_reason_code"] == "CRA_OPERATING_MODE_NO_ACTION"
        assert status["policy_reason_code"] == status["policy_decision_reason_code"]
        assert status["policy_reason_binding"] == "CANDIDATE_AND_DECISION_BOUND_V1"
        assert len(status["policy_decision_digest"]) == 64
        assert status["command_delivery_enabled"] is False
        assert status["physical_effect_count"] == 0
        assert status["authority_state"] == "SAFE_BLOCKED"
        assert status["reconciliation_required"] is True
        assert runtime.store.read_one("SELECT count(*) FROM cra_policy_decisions")[0] == 1
        decision_row = runtime.store.read_one("SELECT candidate_reason_code,decision_reason_code,decision_digest FROM cra_policy_decisions")
        assert decision_row is not None
        assert decision_row["candidate_reason_code"] == status["policy_candidate_reason_code"]
        assert decision_row["decision_reason_code"] == status["policy_decision_reason_code"]
        assert decision_row["decision_digest"] == status["policy_decision_digest"]
        assert runtime.store.read_one("SELECT count(*) FROM recovery_authorizations")[0] == 0
        assert runtime.store.read_one("SELECT count(*) FROM commands")[0] == 0
        assert not hasattr(runtime.store, "create_command")
        assert not hasattr(runtime.store, "add_authorization")
        assert not hasattr(runtime.store, "deliver_command")
        assert not hasattr(runtime.store, "record_status")
        assert not hasattr(runtime.store, "write")
        assert not hasattr(runtime.store, "connection")
        runtime.authorizer = CraAuthorizer(
            runtime.store,
            RecoveryPolicy("negative-control-apply-policy", materialize_authorization=True),
        )
        with pytest.raises(ValueError, match="NO_ACTION_STORE_REFUSES"):
            runtime.authorizer.evaluate(runtime.source.latest())
        assert runtime.store.read_one("SELECT count(*) FROM recovery_authorizations")[0] == 0
    finally:
        runtime.close()


def test_runtime_uses_explicit_source_validity_age_for_slow_delivery_current(tmp_path: Path) -> None:
    config = RuntimeConfig.load(_write_runtime_fixture(tmp_path, delivery_check_age_seconds=120))
    runtime = CraNoActionRuntime(config)
    try:
        status = runtime.run_once()
    finally:
        runtime.close()

    assert status["policy_decision"] == "WOULD_AUTHORIZE"
    assert status["projection_id"] == "runtime-projection-1"
    assert status["command_delivery_enabled"] is False
    assert status["physical_effect_count"] == 0


def test_provision_and_runtime_cli_complete_a_no_action_cycle(tmp_path: Path) -> None:
    config = _write_runtime_fixture(tmp_path, provisioned=False)
    fixed_python = [str(ROOT / "tools/sqlite_runtime/run-fixed.sh"), sys.executable]
    provision = subprocess.run(
        [*fixed_python, "-m", "cra_authority.provision", "--config", str(config)],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    provisioned = json.loads(provision.stdout)
    assert provisioned["result"] == "INITIALIZED"
    assert provisioned["authority_state"] == "SAFE_BLOCKED"
    assert provisioned["external_action_count"] == 0
    repeated = subprocess.run(
        [*fixed_python, "-m", "cra_authority.provision", "--config", str(config)],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    assert json.loads(repeated.stdout)["result"] == "ALREADY_INITIALIZED"

    checked = subprocess.run(
        [*fixed_python, "-m", "cra_authority.runtime", "--config", str(config), "--check-config"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    assert json.loads(checked.stdout)["operating_mode"] == "NO_ACTION"
    cycle = subprocess.run(
        [*fixed_python, "-m", "cra_authority.runtime", "--config", str(config), "--once"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    status = json.loads(cycle.stdout)
    assert status["policy_decision"] == "WOULD_AUTHORIZE"
    assert status["authority_state"] == "SAFE_BLOCKED"
    assert status["readiness"] == "NO_ACTION_READY"
    assert status["sqlite_production_gate"] == "PASS"
    assert status["command_delivery_enabled"] is False
    assert status["physical_effect_count"] == 0

    store = NoActionCentralStore(tmp_path / "cra/central.db", ROOT / "migrations/central/001_initial.sql")
    try:
        assert store.read_one("SELECT count(*) FROM cra_policy_decisions")[0] == 1
        assert store.read_one("SELECT count(*) FROM recovery_authorizations")[0] == 0
        assert store.read_one("SELECT count(*) FROM commands")[0] == 0
    finally:
        store.close()


def test_runtime_config_rejects_apply_mode_and_singleton_lock_rejects_second_writer(tmp_path: Path) -> None:
    path = _write_runtime_fixture(tmp_path)
    value = json.loads(path.read_text(encoding="utf-8"))
    value["operating_mode"] = "LIMITED_APPLY"
    path.write_text(json.dumps(value), encoding="utf-8")
    with pytest.raises(ValueError, match="NO_ACTION"):
        RuntimeConfig.load(path)

    value["operating_mode"] = "NO_ACTION"
    value["monitoring"]["maximum_check_age_seconds"] = 301
    path.write_text(json.dumps(value), encoding="utf-8")
    with pytest.raises(ValueError, match="maximum_check_age_seconds"):
        RuntimeConfig.load(path)

    lock_path = tmp_path / "separate.lock"
    with SingletonLock(lock_path), pytest.raises(RuntimeError, match="SINGLE_WRITER"), SingletonLock(lock_path):
        pass

    release_root = tmp_path / "release-scoped"
    release_root.mkdir()
    release_scoped = _write_runtime_fixture(release_root)
    release_value = json.loads(release_scoped.read_text(encoding="utf-8"))
    release_value["database"] = str(tmp_path / "cra/releases/release-a/central.db")
    release_scoped.write_text(json.dumps(release_value), encoding="utf-8")
    with pytest.raises(ValueError, match="CRA_RELEASE_SCOPED_STATE_FORBIDDEN:database"):
        RuntimeConfig.load(release_scoped)


@pytest.mark.parametrize(
    ("field_path", "invalid"),
    [
        (("cycle_interval_seconds",), True),
        (("cycle_interval_seconds",), "1"),
        (("monitoring", "maximum_ttl_seconds"), True),
        (("monitoring", "maximum_check_age_seconds"), "180"),
        (("policy", "authorization_lifetime_seconds"), True),
        (("policy", "minimum_action_interval_seconds"), 0),
        (("policy", "hourly_action_limit"), 1.0),
        (("policy", "daily_action_limit"), -1),
        (("monitoring", "source_release_id"), True),
        (("database",), "relative/central.db"),
    ],
)
def test_runtime_config_rejects_ambiguous_types_and_unsafe_ranges(
    tmp_path: Path,
    field_path: tuple[str, ...],
    invalid: object,
) -> None:
    path = _write_runtime_fixture(tmp_path)
    value = json.loads(path.read_text(encoding="utf-8"))
    current = value
    for field in field_path[:-1]:
        current = current[field]
    current[field_path[-1]] = invalid
    path.write_text(json.dumps(value), encoding="utf-8")

    with pytest.raises(ValueError):
        RuntimeConfig.load(path)


def test_runtime_config_rejects_daily_limit_below_hourly_limit(tmp_path: Path) -> None:
    path = _write_runtime_fixture(tmp_path)
    value = json.loads(path.read_text(encoding="utf-8"))
    value["policy"]["hourly_action_limit"] = 25
    value["policy"]["daily_action_limit"] = 24
    path.write_text(json.dumps(value), encoding="utf-8")

    with pytest.raises(ValueError, match="daily_action_limit"):
        RuntimeConfig.load(path)


def test_runtime_safe_blocks_group_writable_projection_inbox(tmp_path: Path) -> None:
    path = _write_runtime_fixture(tmp_path)
    config = RuntimeConfig.load(path)
    projection = Path(config.value["monitoring"]["projection_file"])
    os.chmod(projection, 0o660)
    runtime = CraNoActionRuntime(config)
    try:
        status = runtime.run_once()
        assert status["readiness"] == "SAFE_BLOCKED"
        assert status["policy_decision"] == "NO_ACTION"
        assert status["physical_effect_count"] == 0
        assert runtime.store.read_one("SELECT count(*) FROM cra_policy_decisions")[0] == 0
    finally:
        runtime.close()


def test_runtime_v2_preserves_safe_reason_and_throttles_unchanged_events(tmp_path: Path) -> None:
    path = _write_runtime_fixture(tmp_path)
    value = json.loads(path.read_text(encoding="utf-8"))
    archive_private = Ed25519PrivateKey.generate()
    archive_key = tmp_path / "archive-private.pem"
    archive_key.write_bytes(
        archive_private.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    os.chmod(archive_key, 0o600)
    value.update(
        {
            "schema": "cra.runtime.v2",
            "event_file": str(tmp_path / "cra/runtime-events.jsonl"),
            "heartbeat_interval_seconds": 60,
            "retention": {
                "archive_database": str(tmp_path / "cra/archive.db"),
                "archive_signing_private_key": str(archive_key),
                "archive_key_id": "archive-key-a",
                "retain_decision_count": 16,
                "compact_interval_seconds": 60,
            },
        }
    )
    path.write_text(json.dumps(value), encoding="utf-8")
    projection = Path(value["monitoring"]["projection_file"])
    os.chmod(projection, 0o660)
    runtime = CraNoActionRuntime(RuntimeConfig.load(path))
    try:
        first = runtime.run_once()
        second = runtime.run_once()
    finally:
        runtime.close()

    assert first["readiness"] == second["readiness"] == "SAFE_BLOCKED"
    assert "PERMISSIONS_UNSAFE" in first["error_reason_code"]
    assert first["error_reason_code"] in first["policy_blockers"]
    events = [json.loads(line) for line in Path(value["event_file"]).read_text(encoding="utf-8").splitlines()]
    assert len(events) == 1
    assert events[0]["event_type"] == "STATE_CHANGED"
    assert events[0]["error_reason_code"] == first["error_reason_code"]
