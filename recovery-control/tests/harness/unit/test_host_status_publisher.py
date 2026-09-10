from __future__ import annotations

import grp
import hashlib
import json
import os
import pwd
import subprocess
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from cra_dell_recovery.canonical import KeyRing, canonical_json
from cra_dell_recovery.time import isoformat_utc
from cra_no_action_soak.host_status import HostStatusContract
from cra_no_action_soak.host_status_publisher import (
    FAILURE_MESSAGE_ID,
    SIGNING_KEY_CREDENTIAL,
    SIGNING_KEY_OVERRIDE_ENV,
    PublisherConfig,
    _configuration_digest,
    _fresh_status,
    _journal_counters,
    _role_payload,
    _signing_private_key_path,
    publish,
)

ROOT = Path(__file__).resolve().parents[3]


def test_dell_signing_key_override_is_bound_to_systemd_credential_directory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    configured = tmp_path / "configured.pem"
    credentials = tmp_path / "credentials"
    override = credentials / SIGNING_KEY_CREDENTIAL
    value = {"role": "dell", "signing_private_key_file": str(configured)}
    monkeypatch.setenv("CREDENTIALS_DIRECTORY", str(credentials))
    monkeypatch.setenv(SIGNING_KEY_OVERRIDE_ENV, str(override))

    assert _signing_private_key_path(value) == override

    monkeypatch.setenv(SIGNING_KEY_OVERRIDE_ENV, str(tmp_path / "unexpected.pem"))
    with pytest.raises(ValueError, match="HOST_STATUS_SIGNING_KEY_OVERRIDE_MISMATCH"):
        _signing_private_key_path(value)


def test_signing_key_override_is_rejected_for_arena(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CREDENTIALS_DIRECTORY", "/run/credentials/example.service")
    monkeypatch.setenv(
        SIGNING_KEY_OVERRIDE_ENV,
        "/run/credentials/example.service/dell-host-status-signing-key.pem",
    )
    with pytest.raises(ValueError, match="HOST_STATUS_SIGNING_KEY_OVERRIDE_ROLE_INVALID"):
        _signing_private_key_path({"role": "arena", "signing_private_key_file": "/key.pem"})


def test_configuration_digest_matches_existing_ssh_audit_algorithm(tmp_path: Path) -> None:
    config = tmp_path / "config"
    config.mkdir(mode=0o700)
    (config / "a.json").write_text('{"a":1}\n', encoding="utf-8")
    (config / "b.json").write_text('{"b":2}\n', encoding="utf-8")
    for path in config.iterdir():
        path.chmod(0o600)
    expected = subprocess.run(
        f"find {config} -maxdepth 1 -type f -print0 | LC_ALL=C sort -z | xargs -0 sha256sum | sha256sum",
        check=True,
        capture_output=True,
        shell=True,
        text=True,
    ).stdout.split()[0]

    assert _configuration_digest(config) == expected


def test_live_component_freshness_uses_time_after_atomic_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    before_read = datetime(2026, 9, 2, 12, 0, tzinfo=UTC)
    component_time = before_read + timedelta(seconds=1)
    path = tmp_path / "status.json"
    path.write_text("{}\n", encoding="utf-8")
    value = {"observed_at": isoformat_utc(component_time), "status": "READY"}
    monkeypatch.setattr("cra_no_action_soak.host_status_publisher.read_object", lambda *_args, **_kwargs: value)
    monkeypatch.setattr(
        "cra_no_action_soak.host_status_publisher.datetime",
        type(
            "Clock",
            (),
            {"now": staticmethod(lambda _tz: component_time + timedelta(milliseconds=1))},
        ),
    )

    assert _fresh_status(path, None, 30) == value
    with pytest.raises(ValueError, match="HOST_STATUS_COMPONENT_STALE"):
        _fresh_status(path, before_read, 30)


def test_arena_publisher_waits_only_for_expired_upstream_status(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = PublisherConfig({"role": "arena"}, tmp_path / "publisher.json")
    valid_until = datetime(2026, 9, 2, 12, 1, tzinfo=UTC)
    attempts = 0
    waits: list[float] = []

    def payload(_config: PublisherConfig, _current: datetime | None) -> tuple[dict[str, object], datetime]:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise ValueError("HOST_STATUS_EXPIRED")
        return {"status": "CURRENT"}, valid_until

    monkeypatch.setattr("cra_no_action_soak.host_status_publisher._arena_payload", payload)

    assert _role_payload(config, None, wait=waits.append) == ({"status": "CURRENT"}, valid_until)
    assert waits == [0.5]


def test_arena_publisher_does_not_retry_trust_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = PublisherConfig({"role": "arena"}, tmp_path / "publisher.json")
    waits: list[float] = []
    monkeypatch.setattr(
        "cra_no_action_soak.host_status_publisher._arena_payload",
        lambda *_args: (_ for _ in ()).throw(ValueError("HOST_STATUS_SIGNATURE_INVALID")),
    )

    with pytest.raises(ValueError, match="HOST_STATUS_SIGNATURE_INVALID"):
        _role_payload(config, None, wait=waits.append)
    assert waits == []


def test_arena_publisher_waits_for_sufficient_upstream_headroom(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = PublisherConfig({"role": "arena"}, tmp_path / "publisher.json")
    current = datetime(2026, 9, 2, 12, 0, tzinfo=UTC)
    attempts = 0
    waits: list[float] = []

    def clock() -> datetime:
        return current + timedelta(seconds=sum(waits))

    def wait(delay: float) -> None:
        waits.append(delay)

    def payload(_config: PublisherConfig, _current: datetime | None) -> tuple[dict[str, object], datetime]:
        nonlocal attempts
        attempts += 1
        remaining = 5 if attempts == 1 else 20
        return {"status": "CURRENT"}, clock() + timedelta(seconds=remaining)

    monkeypatch.setattr("cra_no_action_soak.host_status_publisher._arena_payload", payload)

    value, valid_until = _role_payload(
        config,
        None,
        minimum_remaining_seconds=10,
        clock=clock,
        wait=wait,
    )

    assert value == {"status": "CURRENT"}
    assert valid_until == clock() + timedelta(seconds=20)
    assert attempts == 2
    assert waits == [0.5]


def test_arena_publisher_waits_through_delayed_pull_completion(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = PublisherConfig({"role": "arena"}, tmp_path / "publisher.json")
    current = datetime(2026, 9, 2, 12, 0, tzinfo=UTC)
    attempts = 0
    waits: list[float] = []

    def clock() -> datetime:
        return current + timedelta(seconds=sum(waits))

    def wait(delay: float) -> None:
        waits.append(delay)

    def payload(_config: PublisherConfig, _current: datetime | None) -> tuple[dict[str, object], datetime]:
        nonlocal attempts
        attempts += 1
        remaining = 5 if attempts <= 18 else 20
        return {"status": "CURRENT"}, clock() + timedelta(seconds=remaining)

    monkeypatch.setattr("cra_no_action_soak.host_status_publisher._arena_payload", payload)

    value, valid_until = _role_payload(
        config,
        None,
        minimum_remaining_seconds=11,
        clock=clock,
        wait=wait,
    )

    assert value == {"status": "CURRENT"}
    assert valid_until == clock() + timedelta(seconds=20)
    assert attempts == 19
    assert waits == [0.5] * 18


def test_journal_counters_advance_from_persisted_cursor(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    user = pwd.getpwuid(os.geteuid()).pw_name
    group = grp.getgrgid(os.getegid()).gr_name
    units = {"alpha": "alpha.service", "beta": "beta.service"}
    legacy_unit = "legacy.service"
    invocation_id = "c" * 32
    next_invocation_id = "d" * 32
    responses = [
        "\n".join(
            (
                json.dumps(
                    {
                        "_SYSTEMD_UNIT": "alpha.service",
                        "MESSAGE_ID": FAILURE_MESSAGE_ID,
                        "MESSAGE": "Failed with result 'resources'",
                    }
                ),
                json.dumps({"_SYSTEMD_UNIT": legacy_unit, "_SYSTEMD_INVOCATION_ID": invocation_id}),
                json.dumps({"_SYSTEMD_UNIT": legacy_unit, "_SYSTEMD_INVOCATION_ID": invocation_id}),
                "-- cursor: cursor-one",
            )
        ),
        "\n".join(
            (
                json.dumps({"_SYSTEMD_UNIT": "beta.service", "MESSAGE": "oom-kill"}),
                json.dumps({"_SYSTEMD_UNIT": legacy_unit, "_SYSTEMD_INVOCATION_ID": next_invocation_id}),
                json.dumps({"_SYSTEMD_UNIT": legacy_unit, "_SYSTEMD_INVOCATION_ID": next_invocation_id}),
                "-- cursor: cursor-two",
            )
        ),
    ]
    calls: list[list[str]] = []

    def run(arguments: list[str], **_kwargs: object) -> str:
        calls.append(arguments)
        return responses.pop(0)

    monkeypatch.setattr("cra_no_action_soak.host_status_publisher._run", run)
    state = tmp_path / "journal-counters.json"

    first = _journal_counters(
        units,
        legacy_service_unit=legacy_unit,
        state_path=state,
        boot_id="boot-one",
        owner=user,
        group=group,
    )
    second = _journal_counters(
        units,
        legacy_service_unit=legacy_unit,
        state_path=state,
        boot_id="boot-one",
        owner=user,
        group=group,
    )

    assert first == ({"alpha": 1, "beta": 0}, 1, 1)
    assert second == ({"alpha": 1, "beta": 0}, 2, 2)
    assert "--after-cursor=cursor-one" in calls[1]
    persisted = json.loads(state.read_text(encoding="utf-8"))
    assert persisted["legacy_invocation_count"] == 2
    assert persisted["legacy_last_invocation_id"] == next_invocation_id
    assert "legacy_invocation_ids" not in persisted


def test_dell_publisher_writes_schema_valid_signed_owned_status(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    private = Ed25519PrivateKey.from_private_bytes(bytes(range(1, 33)))
    private_path = tmp_path / "signing.pem"
    private_path.write_bytes(
        private.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    private_path.chmod(0o600)
    identity_file = tmp_path / "identity"
    identity_file.write_text("immutable\n", encoding="utf-8")
    identity_file.chmod(0o600)
    config_directory = tmp_path / "config"
    config_directory.mkdir(mode=0o700)
    (config_directory / "runtime.json").write_text("{}\n", encoding="utf-8")
    (config_directory / "runtime.json").chmod(0o600)
    output = tmp_path / "host-status.json"
    current = datetime(2026, 9, 2, 12, 0, tzinfo=UTC)
    release_id = "dell-observation-0123456789ab"
    user = pwd.getpwuid(os.geteuid()).pw_name
    group = grp.getgrgid(os.getegid()).gr_name
    value = {
        "role": "dell",
        "host_id": "dell-stream-runtime",
        "release_id": release_id,
        "release_manifest_file": str(identity_file),
        "runtime_manifest_file": str(identity_file),
        "configuration_directory": str(config_directory),
        "maintenance_restart_policy_file": str(identity_file),
        "disk_path": str(tmp_path),
        "service_units": {
            "dell_host_status_publisher": f"dell-no-action-host-status-publisher@{release_id}.service",
            "dell_observation_server": f"dell-observation-server@{release_id}.service",
        },
        "persistent_service_name": "dell_observation_server",
        "credential_files": {"dell_target_observer": str(identity_file)},
        "signing_private_key_file": str(private_path),
        "key_id": "dell-host-status-key",
        "host_status_schema_file": str(ROOT / "contracts/cra_no_action_soak/host_status.v1.schema.json"),
        "output_file": str(output),
        "output_owner": user,
        "output_group": group,
        "maximum_ttl_seconds": 30,
        "maximum_input_age_seconds": 30,
        "minimum_remaining_validity_seconds": 5,
        "role_inputs": {"server_resource_status_file": str(tmp_path / "server-resource-status.json")},
    }
    config = PublisherConfig(value, tmp_path / "publisher.json")
    service = {
        "unit": f"dell-observation-server@{release_id}.service",
        "active_state": "active",
        "sub_state": "running",
        "restart_count": 0,
        "invocation_id": "a" * 32,
    }
    payload = {
        "observation": {
            "schema": "cra_dell_recovery.observation_bundle.v1",
            "source_release_id": release_id,
            "observation_id": "dell-observation-1",
            "observation_revision": "1" * 64,
            "observation_sequence": 1,
            "observed_at": isoformat_utc(current),
            "valid_until": isoformat_utc(current + timedelta(seconds=30)),
            "payload_sha256": "2" * 64,
        },
        "target_snapshot_valid": True,
        "target_identity_sha256": "3" * 64,
        "control_capability_count": 0,
        "physical_effect_count": 0,
        "effect_counters": {
            "effect_request_count": 0,
            "effect_boundary_count": 0,
            "effect_scope_count": 0,
            "raw_outcome_unknown_count": 0,
            "unresolved_scope_count": 0,
            "reconciliation_count": 0,
            "duplicate_scope_count": 0,
            "oldest_unresolved_age_seconds": 0,
        },
    }
    monkeypatch.setattr(
        "cra_no_action_soak.host_status_publisher._service_snapshot",
        lambda _units: {
            "dell_host_status_publisher": {
                "unit": f"dell-no-action-host-status-publisher@{release_id}.service",
                "active_state": "activating",
                "sub_state": "start",
                "restart_count": 0,
                "invocation_id": "b" * 32,
            },
            "dell_observation_server": service,
        },
    )
    monkeypatch.setattr("cra_no_action_soak.host_status_publisher.read_server_resource_status", lambda *_args, **_kwargs: (1024, 4))
    monkeypatch.setattr(
        "cra_no_action_soak.host_status_publisher._dell_payload",
        lambda _config, _now: (payload, current + timedelta(seconds=30)),
    )
    monkeypatch.setattr(
        "cra_no_action_soak.host_status_publisher._journal_counters",
        lambda *_args, **_kwargs: ({"dell_host_status_publisher": 0, "dell_observation_server": 0}, 0, 0),
    )
    monkeypatch.setattr("cra_no_action_soak.host_status_publisher._certificate_remaining", lambda _path, _now: 86_400)

    result = publish(config, now=current)

    assert output.stat().st_mode & 0o777 == 0o640
    assert result["payload_sha256"] == hashlib.sha256(canonical_json(result)).hexdigest()
    contract = HostStatusContract(
        ROOT / "contracts/cra_no_action_soak/host_status.v1.schema.json",
        KeyRing({"dell-host-status-key": private.public_key()}),
        key_id="dell-host-status-key",
        expected_role="dell",
        expected_host_id="dell-stream-runtime",
        expected_release_id=release_id,
        maximum_ttl_seconds=30,
    )
    assert contract.decode(result, now=current).value["payload"] == payload
