from __future__ import annotations

import copy
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from jsonschema import ValidationError

from cra_dell_recovery.canonical import KeyRing
from cra_dell_recovery.errors import SignatureValidationError, UnknownKeyError
from cra_no_action_soak.resilient_host_status import ComponentStatus, ResilientHostStatusContract, build_resilient_status

ROOT = Path(__file__).resolve().parents[3]
SCHEMA = ROOT / "contracts/cra_no_action_soak/resilient_host_status.v3.schema.json"
CURRENT = datetime(2026, 9, 3, 0, 0, tzinfo=UTC)
IDENTITY = {
    "release_manifest_sha256": "1" * 64,
    "runtime_manifest_sha256": "2" * 64,
    "configuration_set_sha256": "3" * 64,
    "maintenance_restart_policy_sha256": "4" * 64,
}
COMPONENT_FAULTS = (
    ("target_snapshot", "ERROR", "K3S_API_UNAVAILABLE"),
    ("runtime_identity", "INVALID", "RUNTIME_IDENTITY_INVALID"),
    ("ffmpeg_identity", "MISSING", "FFMPEG_IDENTITY_MISSING"),
    ("dell_observation", "STALE", "DELL_OBSERVATION_STALE"),
    ("effect_evidence", "ERROR", "EFFECT_DATABASE_UNAVAILABLE"),
    ("k3s_service", "INVALID", "K3S_SERVICE_NOT_ACTIVE"),
    ("target_snapshot_producer", "INVALID", "TARGET_SNAPSHOT_PRODUCER_NOT_ACTIVE"),
    ("dell_observation_server", "INVALID", "DELL_OBSERVATION_SERVER_NOT_ACTIVE"),
    ("dell_resilient_host_status_publisher", "INVALID", "DELL_RESILIENT_HOST_STATUS_PUBLISHER_NOT_ACTIVE"),
    ("local_storage", "INVALID", "LOCAL_STORAGE_EXHAUSTED"),
    ("credentials", "INVALID", "CREDENTIALS_EXPIRING"),
    ("clock", "INVALID", "CLOCK_UNCERTAIN"),
    ("publisher_resources", "ERROR", "PUBLISHER_RESOURCES_UNAVAILABLE"),
)


def _contract(key: Ed25519PrivateKey) -> ResilientHostStatusContract:
    return ResilientHostStatusContract(
        SCHEMA,
        KeyRing({"dell-key": key.public_key()}),
        key_id="dell-key",
        expected_role="dell",
        expected_host_id="dell-stream-runtime",
        expected_release_id="dell-observation-0123456789ab",
    )


def _build(
    tmp_path: Path,
    key: Ed25519PrivateKey,
    *,
    components: list[ComponentStatus],
    current_source: bool,
    explicit_state: str | None = None,
    clock_state: str = "SYNCED",
) -> dict[str, object]:
    return build_resilient_status(
        state_file=tmp_path / "state.json",
        transition_journal=tmp_path / "events.jsonl",
        schema_file=SCHEMA,
        private_key=key,
        key_id="dell-key",
        role="dell",
        host_id="dell-stream-runtime",
        release_id="dell-observation-0123456789ab",
        host_boot_id="boot-one",
        identity=IDENTITY,
        components=components,
        source_observed_at=CURRENT if current_source else None,
        source_valid_until=CURRENT + timedelta(seconds=20) if current_source else None,
        target_identity_sha256="6" * 64 if current_source else None,
        source_payload_sha256="7" * 64 if current_source else None,
        origin_host_id="dell-stream-runtime" if current_source else None,
        physical_effect_count=0,
        explicit_state=explicit_state,
        clock_state=clock_state,
        clock_uncertainty_ms=1000 if clock_state == "SYNCED" else None,
        now=CURRENT,
    )


@pytest.mark.parametrize(("name", "state", "reason"), COMPONENT_FAULTS)
def test_component_faults_remain_freshly_signed_and_never_gain_control(
    tmp_path: Path,
    name: str,
    state: str,
    reason: str,
) -> None:
    key = Ed25519PrivateKey.from_private_bytes(bytes(range(1, 33)))
    times = (CURRENT - timedelta(seconds=20), CURRENT - timedelta(seconds=1)) if state == "STALE" else (None, None)
    component = ComponentStatus(name, state, reason, times[0], times[1])

    status = _build(tmp_path, key, components=[component], current_source=False)

    verified = _contract(key).decode(status, now=CURRENT)
    assert verified["state"] == "DEGRADED_SOURCE"
    assert verified["control_capability_count"] == 0
    assert verified["physical_effect_count"] == 0
    assert verified["last_good"]["diagnostic_only"] is True


def test_resource_only_fault_is_observability_degradation_not_source_failure(tmp_path: Path) -> None:
    key = Ed25519PrivateKey.from_private_bytes(bytes(range(1, 33)))
    status = _build(
        tmp_path,
        key,
        components=[ComponentStatus("publisher_resources", "ERROR", "SERVER_RESOURCE_STATUS_STALE")],
        current_source=True,
    )

    verified = _contract(key).decode(status, now=CURRENT)
    assert verified["state"] == "DEGRADED_OBSERVABILITY"
    assert verified["reason_codes"] == ["SERVER_RESOURCE_STATUS_STALE"]
    assert verified["current"]["target_identity_sha256"] == "6" * 64
    assert verified["control_capability_count"] == 0
    assert verified["physical_effect_count"] == 0


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("role", "arena"),
        ("host_id", "other-host"),
        ("release_id", "dell-observation-other0000"),
        ("producer_instance_id", "forked-instance"),
        ("producer_sequence", 99),
        ("host_boot_id", "other-boot"),
        ("report_lease_until", "2026-09-03T01:00:00Z"),
        ("control_capability_count", 1),
    ),
)
def test_signed_contract_rejects_identity_time_sequence_and_capability_tampering(
    tmp_path: Path,
    field: str,
    value: object,
) -> None:
    key = Ed25519PrivateKey.from_private_bytes(bytes(range(1, 33)))
    ready = ComponentStatus(
        "target_snapshot",
        "FRESH",
        "TARGET_SNAPSHOT_FRESH",
        CURRENT,
        CURRENT + timedelta(seconds=20),
        "5" * 64,
    )
    status = _build(tmp_path, key, components=[ready], current_source=True)
    tampered = copy.deepcopy(status)
    tampered[field] = value

    with pytest.raises((SignatureValidationError, UnknownKeyError, ValidationError, ValueError)):
        _contract(key).decode(tampered, now=CURRENT)


@pytest.mark.parametrize("state", ("COMMUNICATION_DEGRADED", "SOURCE_UNREACHABLE", "MAINTENANCE"))
def test_explicit_nonready_reporting_states_never_reuse_last_good_as_current(
    tmp_path: Path,
    state: str,
) -> None:
    key = Ed25519PrivateKey.from_private_bytes(bytes(range(1, 33)))
    ready = ComponentStatus(
        "target_snapshot",
        "FRESH",
        "TARGET_SNAPSHOT_FRESH",
        CURRENT,
        CURRENT + timedelta(seconds=20),
        "5" * 64,
    )
    _build(tmp_path, key, components=[ready], current_source=True)
    missing = ComponentStatus("target_snapshot", "MISSING", "TARGET_SNAPSHOT_MISSING")

    degraded = _build(tmp_path, key, components=[missing], current_source=False, explicit_state=state)

    verified = _contract(key).decode(degraded, now=CURRENT)
    assert verified["state"] == state
    assert verified["current"]["target_identity_sha256"] is None
    assert verified["last_good"]["present"] is True
    assert verified["last_good"]["diagnostic_only"] is True
