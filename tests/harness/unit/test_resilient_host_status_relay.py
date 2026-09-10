from __future__ import annotations

import grp
import json
import os
import pwd
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import cast

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from cra_dell_recovery.canonical import KeyRing
from cra_no_action_soak import resilient_host_status_relay as relay_module
from cra_no_action_soak.host_status import atomic_write_json
from cra_no_action_soak.resilient_host_status import ComponentStatus, ResilientHostStatusContract, build_resilient_status
from cra_no_action_soak.resilient_host_status_relay import RelayConfig, _pipeline_status_component, _upstream_components

ROOT = Path(__file__).resolve().parents[3]
SCHEMA = ROOT / "contracts/cra_no_action_soak/resilient_host_status.v3.schema.json"
CURRENT = datetime(2026, 9, 3, 0, 0, tzinfo=UTC)
IDENTITY = {
    "release_manifest_sha256": "1" * 64,
    "runtime_manifest_sha256": "2" * 64,
    "configuration_set_sha256": "3" * 64,
    "maintenance_restart_policy_sha256": "4" * 64,
}


def _fresh(name: str) -> ComponentStatus:
    return ComponentStatus(
        name,
        "FRESH",
        f"{name.upper()}_FRESH",
        CURRENT,
        CURRENT + timedelta(seconds=20),
        "a" * 64,
    )


def _dell_status(tmp_path: Path, key: Ed25519PrivateKey) -> dict[str, object]:
    return build_resilient_status(
        state_file=tmp_path / "dell-state.json",
        transition_journal=tmp_path / "dell-events.jsonl",
        schema_file=SCHEMA,
        private_key=key,
        key_id="dell-key",
        role="dell",
        host_id="dell-stream-runtime",
        release_id="dell-observation-0123456789ab",
        host_boot_id="dell-boot",
        identity=IDENTITY,
        components=[
            ComponentStatus(
                "target_snapshot",
                "FRESH",
                "TARGET_SNAPSHOT_FRESH",
                CURRENT,
                CURRENT + timedelta(seconds=20),
                "5" * 64,
            )
        ],
        source_observed_at=CURRENT,
        source_valid_until=CURRENT + timedelta(seconds=20),
        target_identity_sha256="6" * 64,
        source_payload_sha256="7" * 64,
        origin_host_id="dell-stream-runtime",
        physical_effect_count=0,
        now=CURRENT,
    )


def _relay_values(tmp_path: Path, key: Ed25519PrivateKey, status: dict[str, object]) -> dict[str, object]:
    public = tmp_path / "dell-public.pem"
    public.write_bytes(
        key.public_key().public_bytes(
            serialization.Encoding.PEM,
            serialization.PublicFormat.SubjectPublicKeyInfo,
        )
    )
    public.chmod(0o600)
    status_file = tmp_path / "dell-status.json"
    atomic_write_json(status_file, status)
    pull_file = tmp_path / "pull-status.json"
    atomic_write_json(
        pull_file,
        {
            "schema": "cra.resilient_host_status_pull_status.v2",
            "component": "arena_dell_resilient_status_pull",
            "status": "READY",
            "source_state": "READY",
            "source_release_id": "dell-observation-0123456789ab",
            "source_producer_instance_id": status["producer_instance_id"],
            "source_producer_sequence": status["producer_sequence"],
            "disposition": "UPDATED",
            "observed_at": status["observed_at"],
            "transport_attempt_count": 1,
            "transport_retry_count": 0,
            "source_remaining_lease_seconds": 45,
            "control_capability_count": 0,
            "physical_effect_count": 0,
        },
    )
    return {
        "resilient_host_status_schema_file": str(SCHEMA),
        "dell_key_id": "dell-key",
        "dell_public_key_file": str(public),
        "dell_host_id": "dell-stream-runtime",
        "dell_release_id": "dell-observation-0123456789ab",
        "dell_pull_status_file": str(pull_file),
        "dell_status_file": str(status_file),
        "maximum_input_age_seconds": 30,
        "communication_unreachable_seconds": 45,
    }


def test_relay_accepts_current_nested_dell_signature(tmp_path: Path) -> None:
    key = Ed25519PrivateKey.from_private_bytes(bytes(range(1, 33)))
    status = _dell_status(tmp_path, key)
    values = _relay_values(tmp_path, key, status)

    components, upstream, communication_state = _upstream_components(values, current=CURRENT)

    assert [component.state for component in components] == ["FRESH", "FRESH"]
    assert upstream == status
    assert communication_state is None


def test_pipeline_status_uses_atomic_completion_fact_instead_of_oneshot_active_state(tmp_path: Path) -> None:
    path = tmp_path / "live-adapter-status.json"
    atomic_write_json(
        path,
        {
            "schema": "monitoring_v4.cra_live_adapter_status.v2",
            "status": "READY",
            "adapter_release_id": "arena-cra-projection-0123456789ab",
            "observed_at": "2026-09-02T23:59:55Z",
            "control_capability_count": 0,
            "physical_effect_count": 0,
        },
    )

    component = _pipeline_status_component(
        "arena_live_adapter",
        path,
        release_id="arena-cra-projection-0123456789ab",
        current=CURRENT,
        maximum_age_seconds=30,
    )

    assert component.state == "FRESH"
    assert component.reason_code == "ARENA_LIVE_ADAPTER_STATUS_FRESH"


def test_pipeline_status_rejects_future_timestamp_when_validation_time_is_explicit(
    tmp_path: Path,
) -> None:
    path = tmp_path / "live-adapter-status.json"
    atomic_write_json(
        path,
        {
            "schema": "monitoring_v4.cra_live_adapter_status.v2",
            "status": "READY",
            "adapter_release_id": "arena-cra-projection-0123456789ab",
            "observed_at": "2026-09-03T00:00:00.500Z",
            "control_capability_count": 0,
            "physical_effect_count": 0,
        },
    )

    component = _pipeline_status_component(
        "arena_live_adapter",
        path,
        release_id="arena-cra-projection-0123456789ab",
        current=CURRENT,
        maximum_age_seconds=30,
    )

    assert component.state == "ERROR"
    assert component.reason_code == "ARENA_LIVE_ADAPTER_STATUS_FROM_FUTURE"


def test_pipeline_status_fails_closed_for_stale_or_effectful_completion_fact(tmp_path: Path) -> None:
    path = tmp_path / "projection-status.json"
    value = {
        "schema": "monitoring_v4.cra_projection_producer_status.v1",
        "status": "READY",
        "producer_release_id": "arena-cra-projection-0123456789ab",
        "observed_at": "2026-09-02T23:58:00Z",
        "control_capability_count": 0,
        "physical_effect_count": 0,
    }
    atomic_write_json(path, value)

    stale = _pipeline_status_component(
        "arena_projection",
        path,
        release_id="arena-cra-projection-0123456789ab",
        current=CURRENT,
        maximum_age_seconds=30,
    )
    value["observed_at"] = "2026-09-02T23:59:55Z"
    value["physical_effect_count"] = 1
    atomic_write_json(path, value)
    effectful = _pipeline_status_component(
        "arena_projection",
        path,
        release_id="arena-cra-projection-0123456789ab",
        current=CURRENT,
        maximum_age_seconds=30,
    )

    assert stale.state == "STALE"
    assert stale.reason_code == "ARENA_PROJECTION_STATUS_STALE"
    assert effectful.state == "ERROR"


def test_relay_reports_fresh_communication_degradation_when_upstream_lease_expires(tmp_path: Path) -> None:
    key = Ed25519PrivateKey.from_private_bytes(bytes(range(1, 33)))
    status = _dell_status(tmp_path, key)
    values = _relay_values(tmp_path, key, status)
    failure_time = CURRENT + timedelta(seconds=50)
    pull_file = Path(str(values["dell_pull_status_file"]))
    atomic_write_json(
        pull_file,
        {
            "schema": "cra.resilient_host_status_pull_status.v2",
            "component": "arena_dell_resilient_status_pull",
            "status": "SAFE_BLOCKED",
            "source_release_id": "dell-observation-0123456789ab",
            "error_class": "TimeoutError",
            "observed_at": failure_time.isoformat().replace("+00:00", "Z"),
            "control_capability_count": 0,
            "physical_effect_count": 0,
        },
    )

    components, upstream, communication_state = _upstream_components(values, current=failure_time)

    assert components[0].state == "INVALID"
    assert components[1].state == "ERROR"
    assert upstream is None
    assert communication_state == "COMMUNICATION_DEGRADED"


def test_relay_turns_upstream_signature_failure_into_signed_communication_state_input(tmp_path: Path) -> None:
    key = Ed25519PrivateKey.from_private_bytes(bytes(range(1, 33)))
    status = _dell_status(tmp_path, key)
    values = _relay_values(tmp_path, key, status)
    tampered = dict(status)
    tampered["host_boot_id"] = "tampered-boot"
    atomic_write_json(Path(str(values["dell_status_file"])), tampered)

    components, upstream, communication_state = _upstream_components(values, current=CURRENT)

    assert components[0].state == "FRESH"
    assert components[1].state == "ERROR"
    assert upstream is None
    assert communication_state == "COMMUNICATION_DEGRADED"


def test_arena_contract_recursively_verifies_nested_dell_signature(tmp_path: Path) -> None:
    dell_key = Ed25519PrivateKey.from_private_bytes(bytes(range(1, 33)))
    arena_key = Ed25519PrivateKey.from_private_bytes(bytes(range(33, 65)))
    dell = _dell_status(tmp_path, dell_key)
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
                CURRENT,
                CURRENT + timedelta(seconds=20),
                str(dell["payload_sha256"]),
            )
        ],
        source_observed_at=CURRENT,
        source_valid_until=CURRENT + timedelta(seconds=20),
        target_identity_sha256="6" * 64,
        source_payload_sha256="7" * 64,
        origin_host_id="dell-stream-runtime",
        physical_effect_count=0,
        upstream_status=dell,
        track_target_transitions=False,
        now=CURRENT,
    )
    dell_contract = ResilientHostStatusContract(
        SCHEMA,
        KeyRing({"dell-key": dell_key.public_key()}),
        key_id="dell-key",
        expected_role="dell",
        expected_host_id="dell-stream-runtime",
        expected_release_id="dell-observation-0123456789ab",
    )
    arena_contract = ResilientHostStatusContract(
        SCHEMA,
        KeyRing({"arena-key": arena_key.public_key()}),
        key_id="arena-key",
        expected_role="arena",
        expected_host_id="arena-monitoring-facts",
        expected_release_id="arena-cra-projection-0123456789ab",
        upstream_contract=dell_contract,
    )

    assert arena_contract.decode(arena, now=CURRENT)["upstream_status"] == dell


def _complete_relay_value() -> dict[str, object]:
    raw = (ROOT / "ops/systemd/monitoring-v4-resilient-status-publisher.example.json").read_text(encoding="utf-8")
    raw = raw.replace("replace-with-immutable-arena-projection-release-id", "arena-cra-projection-0123456789ab")
    raw = raw.replace("replace-with-immutable-dell-observation-release-id", "dell-observation-0123456789ab")
    raw = raw.replace("replace-with-monitoring-projection-key-id", "arena-key")
    raw = raw.replace("replace-with-dell-observation-key-id", "dell-key")
    return cast(dict[str, object], json.loads(raw))


def test_relay_config_load_validates_the_complete_runtime_contract(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    value = _complete_relay_value()
    config_path = tmp_path / "relay.json"

    def read(path: Path, *, maximum_bytes: int = 0) -> dict[str, object]:
        del maximum_bytes
        if path == config_path:
            return value
        if path == Path(str(value["host_contract_file"])):
            return {"host_id": value["host_id"]}
        if path == Path(str(value["release_manifest_file"])):
            return {"release_id": value["release_id"], "component": "arena"}
        raise AssertionError(path)

    monkeypatch.setattr(relay_module, "read_object", read)
    monkeypatch.setattr(relay_module, "require_runtime_release", lambda *_args: None)

    loaded = RelayConfig.load(config_path)

    assert loaded.value == value
    assert loaded.path == config_path


def test_relay_publish_preserves_verified_nested_status(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dell_key = Ed25519PrivateKey.from_private_bytes(bytes(range(1, 33)))
    arena_key = Ed25519PrivateKey.from_private_bytes(bytes(range(33, 65)))
    dell = _dell_status(tmp_path / "dell", dell_key)
    value = _complete_relay_value()
    value.update(_relay_values(tmp_path, dell_key, dell))
    key_file = tmp_path / "arena-private.pem"
    key_file.write_bytes(
        arena_key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    key_file.chmod(0o600)
    value.update(
        {
            "signing_private_key_file": str(key_file),
            "resilient_host_status_schema_file": str(SCHEMA),
            "state_file": str(tmp_path / "relay-state.json"),
            "transition_journal_file": str(tmp_path / "relay-events.jsonl"),
            "output_file": str(tmp_path / "relay-status.json"),
            "output_owner": pwd.getpwuid(os.geteuid()).pw_name,
            "output_group": grp.getgrgid(os.getegid()).gr_name,
        }
    )
    config = RelayConfig(value, tmp_path / "relay.json")
    monkeypatch.setattr(relay_module, "_service_component", lambda name, *_args, **_kwargs: _fresh(name))
    monkeypatch.setattr(relay_module, "_pipeline_status_component", lambda name, *_args, **_kwargs: _fresh(name))
    monkeypatch.setattr(relay_module, "_clock_status", lambda **_kwargs: ("SYNCED", 1000, _fresh("clock")))
    monkeypatch.setattr(relay_module, "_disk_component", lambda *_args, **_kwargs: _fresh("local_storage"))
    monkeypatch.setattr(relay_module, "_credential_component", lambda *_args, **_kwargs: _fresh("credentials"))
    monkeypatch.setattr(relay_module, "_resource_component", lambda *_args, **_kwargs: _fresh("publisher_resources"))
    monkeypatch.setattr(relay_module, "_sha256_file", lambda _path: "8" * 64)
    monkeypatch.setattr(relay_module, "_configuration_digest", lambda _path: "9" * 64)
    original_read_text = Path.read_text
    monkeypatch.setattr(
        Path,
        "read_text",
        lambda self, **kwargs: "arena-boot" if str(self) == "/proc/sys/kernel/random/boot_id" else original_read_text(self, **kwargs),
    )

    result = relay_module.publish(config, now=CURRENT)

    assert result["state"] == "READY"
    assert result["upstream_status"] == dell
    assert json.loads((tmp_path / "relay-status.json").read_text(encoding="utf-8"))["payload_sha256"] == result["payload_sha256"]


def test_relay_bounded_refresh_waits_for_upstream_evidence_headroom(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    value = _complete_relay_value()
    config = RelayConfig(value, tmp_path / "relay.json")
    upstream = {
        "state": "READY",
        "current": {},
        "physical_effect_count": 0,
    }
    reads = iter(
        [
            ([ComponentStatus("dell_signed_status", "ERROR", "DELL_STATUS_UNAVAILABLE")], None, "COMMUNICATION_DEGRADED"),
            ([_fresh("dell_status_pull"), _fresh("dell_signed_status")], upstream, None),
        ]
    )
    waits: list[float] = []
    captured: dict[str, object] = {}

    monkeypatch.setattr(relay_module, "_upstream_components", lambda *_args, **_kwargs: next(reads))
    remaining = iter([None, 12.0])
    monkeypatch.setattr(relay_module, "_upstream_evidence_remaining", lambda *_args, **_kwargs: next(remaining))
    monkeypatch.setattr(relay_module.time, "sleep", waits.append)
    monkeypatch.setattr(relay_module, "_service_component", lambda name, *_args, **_kwargs: _fresh(name))
    monkeypatch.setattr(relay_module, "_pipeline_status_component", lambda name, *_args, **_kwargs: _fresh(name))
    monkeypatch.setattr(relay_module, "_clock_status", lambda **_kwargs: ("SYNCED", 1, _fresh("clock")))
    monkeypatch.setattr(relay_module, "_disk_component", lambda *_args, **_kwargs: _fresh("local_storage"))
    monkeypatch.setattr(relay_module, "_credential_component", lambda *_args, **_kwargs: _fresh("credentials"))
    monkeypatch.setattr(relay_module, "_resource_component", lambda *_args, **_kwargs: _fresh("publisher_resources"))
    monkeypatch.setattr(relay_module, "_sha256_file", lambda _path: "8" * 64)
    monkeypatch.setattr(relay_module, "_configuration_digest", lambda _path: "9" * 64)
    monkeypatch.setattr(relay_module, "_private_key", lambda _path: Ed25519PrivateKey.from_private_bytes(bytes(range(33, 65))))
    monkeypatch.setattr(Path, "read_text", lambda *_args, **_kwargs: "arena-boot")

    def build(**kwargs: object) -> dict[str, object]:
        captured.update(kwargs)
        return {
            "schema": "cra.resilient_host_status.v3",
            "state": "READY",
            "producer_sequence": 1,
            "observed_at": "2026-09-03T00:00:00Z",
            "reason_codes": ["ALL_REQUIRED_SOURCES_FRESH"],
            "components": [],
        }

    monkeypatch.setattr(relay_module, "build_resilient_status", build)
    monkeypatch.setattr(relay_module, "_write_owned", lambda *_args, **_kwargs: None)

    relay_module.publish(config)

    assert waits == [0.5]
    assert captured["upstream_status"] is upstream
    assert captured["explicit_state"] is None


def test_relay_main_covers_config_check_success_and_safe_block(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    value = _complete_relay_value()
    key = Ed25519PrivateKey.from_private_bytes(bytes(range(33, 65)))
    key_file = tmp_path / "arena-private.pem"
    key_file.write_bytes(
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    key_file.chmod(0o600)
    value["signing_private_key_file"] = str(key_file)
    config = RelayConfig(value, tmp_path / "relay.json")
    monkeypatch.setattr(relay_module.RelayConfig, "load", lambda _path: config)

    monkeypatch.setattr(sys, "argv", ["relay", "--config", "/config.json", "--check-config"])
    relay_module.main()
    assert json.loads(capsys.readouterr().out)["config"] == "VALID"

    result = {
        "schema": "cra.resilient_host_status.v3",
        "state": "READY",
        "producer_sequence": 8,
        "observed_at": "2026-09-03T00:00:00Z",
        "reason_codes": ["ALL_REQUIRED_SOURCES_FRESH"],
        "components": [{"name": "dell_signed_status", "state": "FRESH"}],
    }
    monkeypatch.setattr(relay_module, "publish", lambda _config: result)
    monkeypatch.setattr(sys, "argv", ["relay", "--config", "/config.json"])
    relay_module.main()
    assert json.loads(capsys.readouterr().out)["producer_sequence"] == 8

    recorded_failures: list[dict[str, object]] = []
    monkeypatch.setattr(
        relay_module,
        "record_publisher_failure",
        lambda **kwargs: recorded_failures.append(kwargs),
    )
    monkeypatch.setattr(relay_module, "publish", lambda _config: (_ for _ in ()).throw(RuntimeError("blocked")))
    with pytest.raises(RuntimeError, match="blocked"):
        relay_module.main()
    assert json.loads(capsys.readouterr().err)["status"] == "SAFE_BLOCKED"
    assert len(recorded_failures) == 1
    assert recorded_failures[0]["state_file"] == Path(str(config.value["state_file"]))
