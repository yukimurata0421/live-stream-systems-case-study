from __future__ import annotations

import json
import ssl
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import cast

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from cra_dell_recovery.canonical import KeyRing
from cra_dell_recovery.errors import SignatureValidationError
from cra_no_action_soak import resilient_host_status_pull as pull_module
from cra_no_action_soak.resilient_host_status import ComponentStatus, ResilientHostStatusContract, build_resilient_status
from cra_no_action_soak.resilient_host_status_pull import PullConfig, ResilientStatusPuller, _contract

ROOT = Path(__file__).resolve().parents[3]
SCHEMA = ROOT / "contracts/cra_no_action_soak/resilient_host_status.v3.schema.json"
CURRENT = datetime(2026, 9, 3, 0, 0, tzinfo=UTC)
IDENTITY = {
    "release_manifest_sha256": "1" * 64,
    "runtime_manifest_sha256": "2" * 64,
    "configuration_set_sha256": "3" * 64,
    "maintenance_restart_policy_sha256": "4" * 64,
}


def _status(
    tmp_path: Path,
    key: Ed25519PrivateKey,
    *,
    now: datetime,
    boot_id: str = "boot-one",
    component_ttl_seconds: int = 20,
    source_ttl_seconds: int = 20,
    reporter_lease_seconds: float = 45,
) -> dict[str, object]:
    return build_resilient_status(
        state_file=tmp_path / f"producer-{boot_id}.json",
        transition_journal=tmp_path / f"producer-{boot_id}.jsonl",
        schema_file=SCHEMA,
        private_key=key,
        key_id="dell-key",
        role="dell",
        host_id="dell-stream-runtime",
        release_id="dell-observation-0123456789ab",
        host_boot_id=boot_id,
        identity=IDENTITY,
        components=[
            ComponentStatus(
                "target_snapshot",
                "FRESH",
                "TARGET_SNAPSHOT_FRESH",
                now,
                now + timedelta(seconds=component_ttl_seconds),
                "5" * 64,
            )
        ],
        source_observed_at=now,
        source_valid_until=now + timedelta(seconds=source_ttl_seconds),
        target_identity_sha256="6" * 64,
        source_payload_sha256="7" * 64,
        origin_host_id="dell-stream-runtime",
        physical_effect_count=0,
        reporter_lease_seconds=reporter_lease_seconds,
        now=now,
    )


def _puller(tmp_path: Path, key: Ed25519PrivateKey) -> ResilientStatusPuller:
    contract = ResilientHostStatusContract(
        SCHEMA,
        KeyRing({"dell-key": key.public_key()}),
        key_id="dell-key",
        expected_role="dell",
        expected_host_id="dell-stream-runtime",
        expected_release_id="dell-observation-0123456789ab",
    )
    return ResilientStatusPuller(
        endpoint_url="https://dell.invalid/v3/no-action-soak-status/latest",
        ssl_context=ssl.create_default_context(),
        contract=contract,
        output_file=tmp_path / "inbox.json",
        admission_state_file=tmp_path / "admission.json",
        timeout_seconds=1,
        transient_retry_delays_seconds=(0.1,),
        maximum_retry_elapsed_seconds=1,
        minimum_remaining_lease_seconds=5,
    )


def _public_key(path: Path, key: Ed25519PrivateKey) -> Path:
    path.write_bytes(
        key.public_key().public_bytes(
            serialization.Encoding.PEM,
            serialization.PublicFormat.SubjectPublicKeyInfo,
        )
    )
    path.chmod(0o600)
    return path


def _arena_status(
    tmp_path: Path,
    arena_key: Ed25519PrivateKey,
    dell_status: dict[str, object],
    *,
    now: datetime = CURRENT,
) -> dict[str, object]:
    return build_resilient_status(
        state_file=tmp_path / "arena-state.json",
        transition_journal=tmp_path / "arena-events.jsonl",
        schema_file=SCHEMA,
        private_key=arena_key,
        key_id="arena-key",
        role="arena",
        host_id="arena-monitoring-facts",
        release_id="arena-cra-projection-0123456789ab",
        host_boot_id="arena-boot",
        identity=IDENTITY,
        components=[
            ComponentStatus(
                "dell_signed_status",
                "FRESH",
                "DELL_SIGNED_STATUS_FRESH",
                now,
                now + timedelta(seconds=20),
                str(dell_status["payload_sha256"]),
            )
        ],
        source_observed_at=CURRENT,
        source_valid_until=CURRENT + timedelta(seconds=20),
        target_identity_sha256="6" * 64,
        source_payload_sha256="7" * 64,
        origin_host_id="dell-stream-runtime",
        physical_effect_count=0,
        upstream_status=dell_status,
        track_target_transitions=False,
        now=now,
    )


def _arena_contract_config(
    tmp_path: Path,
    arena_key: Ed25519PrivateKey,
    dell_key: Ed25519PrivateKey,
) -> PullConfig:
    return PullConfig(
        {
            "resilient_host_status_schema_file": str(SCHEMA),
            "source_key_id": "arena-key",
            "source_public_key_file": str(_public_key(tmp_path / "arena-public.pem", arena_key)),
            "source_role": "arena",
            "source_host_id": "arena-monitoring-facts",
            "source_release_id": "arena-cra-projection-0123456789ab",
            "upstream_key_id": "dell-key",
            "upstream_public_key_file": str(_public_key(tmp_path / "dell-public.pem", dell_key)),
            "upstream_role": "dell",
            "upstream_host_id": "dell-stream-runtime",
            "upstream_release_id": "dell-observation-0123456789ab",
            "maximum_report_lease_seconds": 45,
            "maximum_clock_skew_seconds": 5,
        },
        tmp_path / "config.json",
    )


def test_admission_accepts_monotonic_sequence_and_new_boot(tmp_path: Path) -> None:
    key = Ed25519PrivateKey.from_private_bytes(bytes(range(1, 33)))
    puller = _puller(tmp_path, key)
    first = _status(tmp_path, key, now=CURRENT)
    second = _status(tmp_path, key, now=CURRENT + timedelta(seconds=1))
    after_boot = _status(tmp_path, key, now=CURRENT + timedelta(seconds=2), boot_id="boot-two")

    assert puller._admit(first) == "UPDATED"
    assert puller._admit(first) == "UNCHANGED"
    assert puller._admit(second) == "UPDATED"
    assert puller._admit(after_boot) == "UPDATED"


def test_admission_rejects_same_boot_instance_change_and_sequence_conflict(tmp_path: Path) -> None:
    key = Ed25519PrivateKey.from_private_bytes(bytes(range(1, 33)))
    puller = _puller(tmp_path, key)
    first = _status(tmp_path / "first", key, now=CURRENT)
    conflicting_instance = _status(tmp_path / "other", key, now=CURRENT, boot_id="boot-one")
    assert puller._admit(first) == "UPDATED"

    with pytest.raises(ValueError, match="PRODUCER_INSTANCE_CONFLICT"):
        puller._admit(conflicting_instance)

    conflict = dict(first)
    conflict["payload_sha256"] = "f" * 64
    with pytest.raises(ValueError, match="SEQUENCE_CONFLICT"):
        puller._admit(conflict)


def test_admission_state_loss_never_trusts_existing_inbox(tmp_path: Path) -> None:
    key = Ed25519PrivateKey.from_private_bytes(bytes(range(1, 33)))
    puller = _puller(tmp_path, key)
    first = _status(tmp_path, key, now=CURRENT)
    assert puller._admit(first) == "UPDATED"
    (tmp_path / "admission.json").unlink()

    with pytest.raises(ValueError, match="ADMISSION_STATE_MISSING"):
        puller._admit(first)


def test_cra_pull_contract_recursively_verifies_nested_dell_status(tmp_path: Path) -> None:
    dell_key = Ed25519PrivateKey.from_private_bytes(bytes(range(1, 33)))
    arena_key = Ed25519PrivateKey.from_private_bytes(bytes(range(33, 65)))
    dell = _status(tmp_path / "dell", dell_key, now=CURRENT)
    arena = _arena_status(tmp_path, arena_key, dell)

    verified = _contract(_arena_contract_config(tmp_path, arena_key, dell_key)).decode(arena, now=CURRENT)

    assert verified["upstream_status"] == dell


def test_outer_report_expires_no_later_than_nested_component_during_relay_transit(tmp_path: Path) -> None:
    dell_key = Ed25519PrivateKey.from_private_bytes(bytes(range(1, 33)))
    arena_key = Ed25519PrivateKey.from_private_bytes(bytes(range(33, 65)))
    dell = _status(
        tmp_path / "dell",
        dell_key,
        now=CURRENT,
        component_ttl_seconds=5,
        source_ttl_seconds=20,
    )
    arena = _arena_status(tmp_path, arena_key, dell, now=CURRENT + timedelta(seconds=4))

    assert arena["report_lease_until"] == "2026-09-03T00:00:05.000Z"
    with pytest.raises(ValueError, match="RESILIENT_STATUS_REPORT_EXPIRED"):
        _contract(_arena_contract_config(tmp_path, arena_key, dell_key)).decode(
            arena,
            now=CURRENT + timedelta(seconds=6),
        )


def test_relay_refuses_to_extend_an_already_expired_nested_component_lease(
    tmp_path: Path,
) -> None:
    dell_key = Ed25519PrivateKey.from_private_bytes(bytes(range(1, 33)))
    arena_key = Ed25519PrivateKey.from_private_bytes(bytes(range(33, 65)))
    dell = _status(
        tmp_path / "dell",
        dell_key,
        now=CURRENT,
        component_ttl_seconds=5,
        source_ttl_seconds=20,
    )
    with pytest.raises(ValueError, match="RESILIENT_STATUS_REPORT_EVIDENCE_LEASE_EXHAUSTED"):
        _arena_status(tmp_path, arena_key, dell, now=CURRENT + timedelta(seconds=6))


def test_cra_pull_contract_rejects_nested_status_signed_by_another_dell_key(tmp_path: Path) -> None:
    expected_dell_key = Ed25519PrivateKey.from_private_bytes(bytes(range(1, 33)))
    wrong_dell_key = Ed25519PrivateKey.from_private_bytes(bytes(range(2, 34)))
    arena_key = Ed25519PrivateKey.from_private_bytes(bytes(range(33, 65)))
    dell = _status(tmp_path / "dell", wrong_dell_key, now=CURRENT)
    arena = _arena_status(tmp_path, arena_key, dell)

    with pytest.raises(SignatureValidationError, match="signature verification failed"):
        _contract(_arena_contract_config(tmp_path, arena_key, expected_dell_key)).decode(arena, now=CURRENT)


def _complete_pull_value() -> dict[str, object]:
    raw = (ROOT / "ops/systemd/cra-arena-resilient-status-pull.example.json").read_text(encoding="utf-8")
    raw = raw.replace("replace-with-immutable-cra-release-id", "cra-no-action-0123456789ab")
    raw = raw.replace("replace-with-immutable-arena-projection-release-id", "arena-cra-projection-0123456789ab")
    raw = raw.replace("replace-with-immutable-dell-observation-release-id", "dell-observation-0123456789ab")
    raw = raw.replace("replace-with-monitoring-projection-key-id", "arena-key")
    raw = raw.replace("replace-with-dell-observation-key-id", "dell-key")
    return cast(dict[str, object], json.loads(raw))


def test_pull_config_load_accepts_complete_arena_to_cra_contract(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "pull.json"
    path.write_text(json.dumps(_complete_pull_value()) + "\n", encoding="utf-8")
    path.chmod(0o600)
    monkeypatch.setattr(pull_module, "require_runtime_release", lambda *_args: None)

    loaded = PullConfig.load(path)

    assert loaded.value["component"] == "cra_arena_resilient_status_pull"
    assert loaded.value["source_role"] == "arena"


def test_pull_fetches_verifies_and_admits_signed_status(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    key = Ed25519PrivateKey.from_private_bytes(bytes(range(1, 33)))
    status = _status(tmp_path / "producer", key, now=CURRENT)
    puller = _puller(tmp_path, key)
    monkeypatch.setattr(
        pull_module,
        "fetch_bounded_json",
        lambda *_args, **_kwargs: (json.dumps(status).encode(), 2),
    )

    result = puller.pull(now=CURRENT + timedelta(seconds=1))

    assert result["disposition"] == "UPDATED"
    assert result["transport_attempt_count"] == 2
    assert result["transport_retry_count"] == 1
    assert json.loads((tmp_path / "inbox.json").read_text(encoding="utf-8"))["payload_sha256"] == status["payload_sha256"]

    puller.minimum_remaining_lease_seconds = 45
    with pytest.raises(ValueError, match="INSUFFICIENT_EVIDENCE_VALIDITY"):
        puller.pull(now=CURRENT + timedelta(seconds=1))

    monkeypatch.setattr(pull_module, "fetch_bounded_json", lambda *_args, **_kwargs: (b"[]", 1))
    with pytest.raises(ValueError, match="RESPONSE_NOT_OBJECT"):
        puller.pull(now=CURRENT)


def test_production_ttl_and_pull_floor_preserve_cross_host_handoff_headroom(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    publisher_config = json.loads((ROOT / "ops/systemd/dell-resilient-host-status-publisher.example.json").read_text(encoding="utf-8"))
    arena_pull_config = json.loads((ROOT / "ops/systemd/monitoring-v4-dell-resilient-status-pull.example.json").read_text(encoding="utf-8"))
    relay_config = json.loads((ROOT / "ops/systemd/monitoring-v4-resilient-status-publisher.example.json").read_text(encoding="utf-8"))
    cra_pull_config = json.loads((ROOT / "ops/systemd/cra-arena-resilient-status-pull.example.json").read_text(encoding="utf-8"))
    assert publisher_config["component_validity_seconds"] == 20
    assert publisher_config["reporter_lease_seconds"] == 45
    assert arena_pull_config["minimum_remaining_lease_seconds"] == 10
    assert arena_pull_config["maximum_report_lease_seconds"] == 45
    assert relay_config["component_validity_seconds"] == 20
    assert relay_config["reporter_lease_seconds"] == 45
    assert cra_pull_config["minimum_remaining_lease_seconds"] == 5
    assert cra_pull_config["maximum_report_lease_seconds"] == 45

    key = Ed25519PrivateKey.from_private_bytes(bytes(range(1, 33)))
    status = _status(
        tmp_path / "producer",
        key,
        now=CURRENT,
        component_ttl_seconds=int(publisher_config["component_validity_seconds"]),
        source_ttl_seconds=int(publisher_config["component_validity_seconds"]),
        reporter_lease_seconds=float(publisher_config["reporter_lease_seconds"]),
    )
    puller = _puller(tmp_path / "puller", key)
    puller.minimum_remaining_lease_seconds = float(arena_pull_config["minimum_remaining_lease_seconds"])
    monkeypatch.setattr(
        pull_module,
        "fetch_bounded_json",
        lambda *_args, **_kwargs: (json.dumps(status).encode(), 1),
    )

    arena_pull = puller.pull(now=CURRENT + timedelta(seconds=3))

    assert status["report_lease_until"] == "2026-09-03T00:00:20.000Z"
    assert arena_pull["source_remaining_lease_seconds"] == 17.0
    assert arena_pull["source_remaining_evidence_seconds"] == 17.0

    arena_key = Ed25519PrivateKey.from_private_bytes(bytes(range(33, 65)))
    source = status["current"]
    assert isinstance(source, dict)
    relay_time = CURRENT + timedelta(seconds=3)
    arena = build_resilient_status(
        state_file=tmp_path / "arena-state.json",
        transition_journal=tmp_path / "arena-events.jsonl",
        schema_file=SCHEMA,
        private_key=arena_key,
        key_id="arena-key",
        role="arena",
        host_id="arena-monitoring-facts",
        release_id="arena-cra-projection-0123456789ab",
        host_boot_id="arena-boot",
        identity=IDENTITY,
        components=[
            ComponentStatus(
                "dell_signed_status",
                "FRESH",
                "DELL_SIGNED_STATUS_FRESH",
                relay_time,
                relay_time + timedelta(seconds=int(relay_config["component_validity_seconds"])),
                str(status["payload_sha256"]),
            )
        ],
        source_observed_at=datetime.fromisoformat(str(source["source_observed_at"]).replace("Z", "+00:00")),
        source_valid_until=datetime.fromisoformat(str(source["source_valid_until"]).replace("Z", "+00:00")),
        target_identity_sha256=str(source["target_identity_sha256"]),
        source_payload_sha256=str(source["source_payload_sha256"]),
        origin_host_id=str(source["origin_host_id"]),
        physical_effect_count=0,
        upstream_status=status,
        track_target_transitions=False,
        reporter_lease_seconds=float(relay_config["reporter_lease_seconds"]),
        now=relay_time,
    )
    cra_puller = ResilientStatusPuller(
        endpoint_url="https://arena.invalid/v3/no-action-soak-status/latest",
        ssl_context=ssl.create_default_context(),
        contract=_contract(_arena_contract_config(tmp_path, arena_key, key)),
        output_file=tmp_path / "cra-inbox.json",
        admission_state_file=tmp_path / "cra-admission.json",
        timeout_seconds=1,
        transient_retry_delays_seconds=(0.1,),
        maximum_retry_elapsed_seconds=1,
        minimum_remaining_lease_seconds=float(cra_pull_config["minimum_remaining_lease_seconds"]),
    )
    monkeypatch.setattr(
        pull_module,
        "fetch_bounded_json",
        lambda *_args, **_kwargs: (json.dumps(arena).encode(), 1),
    )

    cra_pull = cra_puller.pull(now=CURRENT + timedelta(seconds=6))

    assert arena["report_lease_until"] == "2026-09-03T00:00:20.000Z"
    assert cra_pull["source_remaining_lease_seconds"] == 14.0
    assert cra_pull["source_remaining_evidence_seconds"] == 14.0


def test_pull_main_covers_config_check_and_ready_status(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    config = PullConfig(_complete_pull_value(), tmp_path / "pull.json")
    config.value["status_file"] = str(tmp_path / "pull-status.json")
    config.value["recovery_state_file"] = str(tmp_path / "recovery-state.json")
    config.value["recovery_event_file"] = str(tmp_path / "recovery-events.jsonl")

    class Health:
        def __init__(self, **_kwargs: object) -> None:
            self.state = "READY"

        def record_success(self, *, attempt_count: int) -> None:
            assert attempt_count == 2

        def record_failure(self, *_args: object, **_kwargs: object) -> None:
            self.state = "FAILED"

        def snapshot(self) -> dict[str, str]:
            return {"current_state": self.state}

    class Puller:
        def __init__(self, **_kwargs: object) -> None:
            pass

        def pull(self) -> dict[str, object]:
            return {
                "host_status": {
                    "state": "READY",
                    "producer_instance_id": "arena-instance",
                    "producer_sequence": 9,
                },
                "disposition": "UPDATED",
                "pulled_at": "2026-09-03T00:00:00Z",
                "transport_attempt_count": 2,
                "transport_retry_count": 1,
                "source_remaining_lease_seconds": 30.0,
                "source_remaining_evidence_seconds": 20.0,
            }

    monkeypatch.setattr(pull_module.PullConfig, "load", lambda _path: config)
    monkeypatch.setattr(pull_module, "RecoveryHealthStore", Health)
    monkeypatch.setattr(pull_module, "_client_context", lambda *_args: ssl.create_default_context())
    monkeypatch.setattr(pull_module, "_contract", lambda _config: object())
    monkeypatch.setattr(pull_module, "ResilientStatusPuller", Puller)

    monkeypatch.setattr(sys, "argv", ["pull", "--config", "/config.json", "--check-config"])
    pull_module.main()
    assert json.loads(capsys.readouterr().out)["config"] == "VALID"

    monkeypatch.setattr(sys, "argv", ["pull", "--config", "/config.json"])
    pull_module.main()
    status = json.loads(capsys.readouterr().out)
    assert status["status"] == "READY"
    assert status["source_producer_sequence"] == 9
