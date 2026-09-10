from __future__ import annotations

import copy
import grp
import json
import os
import pwd
import sqlite3
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from cra_dell_recovery.canonical import KeyRing
from cra_no_action_soak import resilient_host_status_publisher as publisher_module
from cra_no_action_soak.resilient_host_status import ComponentStatus, ResilientHostStatusContract
from cra_no_action_soak.resilient_host_status_publisher import (
    PublisherConfig,
    _chronyc_uncertainty,
    _clock_status,
    _target_component,
    publish,
)

ROOT = Path(__file__).resolve().parents[3]
CURRENT = datetime(2026, 9, 3, 0, 0, tzinfo=UTC)


def _fresh(name: str) -> ComponentStatus:
    return ComponentStatus(
        name,
        "FRESH",
        f"{name.upper()}_FRESH",
        CURRENT,
        CURRENT + timedelta(seconds=20),
        "a" * 64,
    )


def _config(tmp_path: Path, key: Ed25519PrivateKey) -> PublisherConfig:
    key_file = tmp_path / "signing.pem"
    key_file.write_bytes(
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    key_file.chmod(0o600)
    identity = tmp_path / "identity.json"
    identity.write_text("{}\n", encoding="utf-8")
    identity.chmod(0o600)
    configuration = tmp_path / "config"
    configuration.mkdir(mode=0o700)
    (configuration / "publisher.json").write_text("{}\n", encoding="utf-8")
    (configuration / "publisher.json").chmod(0o600)
    user = pwd.getpwuid(os.geteuid()).pw_name
    group = grp.getgrgid(os.getegid()).gr_name
    release_id = "dell-observation-0123456789ab"
    value = {
        "role": "dell",
        "host_id": "dell-stream-runtime",
        "target_source_host_id": "dell-yuki",
        "release_id": release_id,
        "release_manifest_file": str(identity),
        "runtime_manifest_file": str(identity),
        "configuration_directory": str(configuration),
        "maintenance_restart_policy_file": str(identity),
        "disk_path": str(tmp_path),
        "service_units": {
            "k3s_service": "k3s.service",
            "target_snapshot_producer": "dell-target-snapshot-shadow.service",
            "dell_observation_server": f"dell-observation-server@{release_id}.service",
            "dell_resilient_host_status_publisher": f"dell-resilient-host-status-publisher@{release_id}.service",
        },
        "credential_files": {"dell_target_observer": str(identity)},
        "signing_private_key_file": str(key_file),
        "key_id": "dell-resilient-key",
        "resilient_host_status_schema_file": str(ROOT / "contracts/cra_no_action_soak/resilient_host_status.v3.schema.json"),
        "output_file": str(tmp_path / "status.json"),
        "state_file": str(tmp_path / "state.json"),
        "transition_journal_file": str(tmp_path / "transitions.jsonl"),
        "output_owner": user,
        "output_group": group,
        "reporter_lease_seconds": 45,
        "last_good_retention_seconds": 300,
        "component_validity_seconds": 20,
        "maximum_input_age_seconds": 30,
        "minimum_disk_free_bytes": 64 * 1024 * 1024,
        "minimum_credential_remaining_seconds": 3600,
        "clock_uncertainty_bound_ms": 1000,
        "maximum_clock_tracking_age_seconds": 10,
        "clock_tracking_file": str(tmp_path / "clock.json"),
        "role_inputs": {
            "target_snapshot_file": str(tmp_path / "target.json"),
            "target_snapshot_schema_file": str(ROOT / "contracts/cra_dell_recovery/target_snapshot.v1.schema.json"),
            "observation_file": str(tmp_path / "observation.json"),
            "observation_schema_file": str(ROOT / "contracts/cra_dell_recovery/v1/observation_bundle.schema.json"),
            "effect_database": str(tmp_path / "effects.sqlite3"),
            "server_resource_status_file": str(tmp_path / "resources.json"),
        },
    }
    return PublisherConfig(value, tmp_path / "config.json")


def test_publisher_emits_fresh_signed_degraded_status_instead_of_going_silent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    key = Ed25519PrivateKey.from_private_bytes(bytes(range(1, 33)))
    config = _config(tmp_path, key)
    target = {
        "host_id": "dell-yuki",
        "host_boot_id": "boot-one",
        "namespace": "stream-v3",
        "pod_uid": "pod-one",
        "container_name": "stream-engine",
        "container_id": "containerd://one",
        "ffmpeg_generation": "generation-one",
        "ffmpeg_pid": 123,
    }
    target_components = [_fresh("target_snapshot"), _fresh("runtime_identity"), _fresh("ffmpeg_identity")]
    observation = {"payload_sha256": "b" * 64}
    monkeypatch.setattr(
        "cra_no_action_soak.resilient_host_status_publisher._target_component",
        lambda *_args, **_kwargs: (
            target_components,
            target,
            CURRENT,
            CURRENT + timedelta(seconds=20),
        ),
    )
    observation_state: list[ComponentStatus] = [_fresh("dell_observation")]
    monkeypatch.setattr(
        "cra_no_action_soak.resilient_host_status_publisher._observation_component",
        lambda *_args, **_kwargs: (
            observation_state[0],
            observation if observation_state[0].state == "FRESH" else None,
            CURRENT if observation_state[0].state == "FRESH" else None,
            CURRENT + timedelta(seconds=20) if observation_state[0].state == "FRESH" else None,
        ),
    )
    monkeypatch.setattr(
        "cra_no_action_soak.resilient_host_status_publisher._effect_component",
        lambda *_args, **_kwargs: (_fresh("effect_evidence"), {"effect_boundary_count": 0}),
    )
    monkeypatch.setattr(
        "cra_no_action_soak.resilient_host_status_publisher._clock_status",
        lambda **_kwargs: ("SYNCED", 1000, _fresh("clock")),
    )
    monkeypatch.setattr(
        "cra_no_action_soak.resilient_host_status_publisher._service_component",
        lambda name, *_args, **_kwargs: _fresh(name),
    )
    monkeypatch.setattr(
        "cra_no_action_soak.resilient_host_status_publisher._disk_component",
        lambda *_args, **_kwargs: _fresh("local_storage"),
    )
    monkeypatch.setattr(
        "cra_no_action_soak.resilient_host_status_publisher._credential_component",
        lambda *_args, **_kwargs: _fresh("credentials"),
    )
    monkeypatch.setattr(
        "cra_no_action_soak.resilient_host_status_publisher._resource_component",
        lambda *_args, **_kwargs: _fresh("publisher_resources"),
    )
    original_read_text = Path.read_text
    monkeypatch.setattr(
        "cra_no_action_soak.resilient_host_status_publisher.Path.read_text",
        lambda self, **_kwargs: "boot-one" if str(self) == "/proc/sys/kernel/random/boot_id" else original_read_text(self, **_kwargs),
    )

    ready = publish(config, now=CURRENT)
    observation_state[0] = ComponentStatus("dell_observation", "ERROR", "DELL_OBSERVATION_UNAVAILABLE")
    degraded = publish(config, now=CURRENT + timedelta(seconds=5))

    assert ready["state"] == "READY"
    assert degraded["state"] == "DEGRADED_SOURCE"
    assert degraded["producer_sequence"] == 2
    assert degraded["last_good"]["present"] is True
    assert degraded["last_good"]["diagnostic_only"] is True
    contract = ResilientHostStatusContract(
        ROOT / "contracts/cra_no_action_soak/resilient_host_status.v3.schema.json",
        KeyRing({"dell-resilient-key": key.public_key()}),
        key_id="dell-resilient-key",
        expected_role="dell",
        expected_host_id="dell-stream-runtime",
        expected_release_id="dell-observation-0123456789ab",
    )
    assert contract.decode(degraded, now=CURRENT + timedelta(seconds=5))["state"] == "DEGRADED_SOURCE"


def test_publisher_signs_disjoint_source_windows_as_degraded_without_tracking_a_transition(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    key = Ed25519PrivateKey.from_private_bytes(bytes(range(1, 33)))
    config = _config(tmp_path, key)
    report_time = CURRENT + timedelta(seconds=25)
    target = {
        "host_id": "dell-yuki",
        "host_boot_id": "boot-one",
        "namespace": "stream-v3",
        "pod_uid": "pod-one",
        "container_name": "stream-engine",
        "container_id": "containerd://one",
        "ffmpeg_generation": "generation-one",
        "ffmpeg_pid": 123,
    }

    def fresh(name: str) -> ComponentStatus:
        return ComponentStatus(
            name,
            "FRESH",
            f"{name.upper()}_FRESH",
            report_time,
            report_time + timedelta(seconds=20),
            "a" * 64,
        )

    monkeypatch.setattr(
        publisher_module,
        "_target_component",
        lambda *_args, **_kwargs: (
            [fresh("target_snapshot"), fresh("runtime_identity"), fresh("ffmpeg_identity")],
            target,
            report_time,
            report_time + timedelta(seconds=20),
        ),
    )
    observation = {"payload_sha256": "b" * 64}
    monkeypatch.setattr(
        publisher_module,
        "_observation_component",
        lambda *_args, **_kwargs: (
            ComponentStatus(
                "dell_observation",
                "STALE",
                "DELL_OBSERVATION_STALE",
                CURRENT,
                CURRENT + timedelta(seconds=20),
                "b" * 64,
            ),
            observation,
            CURRENT,
            CURRENT + timedelta(seconds=20),
        ),
    )
    monkeypatch.setattr(
        publisher_module,
        "_effect_component",
        lambda *_args, **_kwargs: (fresh("effect_evidence"), {"effect_boundary_count": 0}),
    )
    monkeypatch.setattr(publisher_module, "_clock_status", lambda **_kwargs: ("SYNCED", 1000, fresh("clock")))
    monkeypatch.setattr(publisher_module, "_service_component", lambda name, *_args, **_kwargs: fresh(name))
    monkeypatch.setattr(publisher_module, "_disk_component", lambda *_args, **_kwargs: fresh("local_storage"))
    monkeypatch.setattr(publisher_module, "_credential_component", lambda *_args, **_kwargs: fresh("credentials"))
    monkeypatch.setattr(publisher_module, "_resource_component", lambda *_args, **_kwargs: fresh("publisher_resources"))
    original_read_text = Path.read_text
    monkeypatch.setattr(
        publisher_module.Path,
        "read_text",
        lambda self, **_kwargs: "boot-one" if str(self) == "/proc/sys/kernel/random/boot_id" else original_read_text(self, **_kwargs),
    )

    degraded = publish(config, now=report_time)

    assert degraded["state"] == "DEGRADED_SOURCE"
    assert degraded["reason_codes"] == ["DELL_OBSERVATION_STALE", "SOURCE_TIME_WINDOW_DISJOINT"]
    assert degraded["current"] == {
        "source_observed_at": None,
        "source_valid_until": None,
        "target_identity_sha256": None,
        "source_payload_sha256": None,
        "origin_host_id": None,
    }
    assert degraded["transition"]["open_episode_count"] == 0
    contract = ResilientHostStatusContract(
        ROOT / "contracts/cra_no_action_soak/resilient_host_status.v3.schema.json",
        KeyRing({"dell-resilient-key": key.public_key()}),
        key_id="dell-resilient-key",
        expected_role="dell",
        expected_host_id="dell-stream-runtime",
        expected_release_id="dell-observation-0123456789ab",
    )
    assert contract.decode(degraded, now=report_time)["state"] == "DEGRADED_SOURCE"


def test_target_component_separates_snapshot_runtime_and_ffmpeg_identity(tmp_path: Path) -> None:
    target = {
        "host_id": "dell-yuki",
        "host_boot_id": "boot-one",
        "namespace": "stream-v3",
        "pod_uid": "pod-one",
        "container_name": "stream-engine",
        "container_id": "containerd://one",
        "ffmpeg_generation": "generation-one",
        "ffmpeg_pid": 123,
    }
    runtime = {
        "host_id": "dell-yuki",
        "host_boot_id": "boot-one",
        "namespace": "stream-v3",
        "pod_uid": "pod-one",
        "stream_engine_container_name": "stream-engine",
        "stream_engine_container_id": "containerd://one",
        "runtime_generation": "runtime-one",
    }
    snapshot = {
        "schema": "cra_dell_recovery.target_snapshot.v1",
        "snapshot_id": "snapshot-one",
        "observed_at": CURRENT.isoformat().replace("+00:00", "Z"),
        "valid_until": (CURRENT + timedelta(seconds=20)).isoformat().replace("+00:00", "Z"),
        "source_revision": "1" * 64,
        "read_started_at": CURRENT.isoformat().replace("+00:00", "Z"),
        "read_finished_at": (CURRENT + timedelta(milliseconds=1)).isoformat().replace("+00:00", "Z"),
        "status": "VALID",
        "reason_code": "SNAPSHOT_CONSISTENT",
        "target_identity": target,
        "runtime_snapshot_id": "runtime-snapshot-one",
        "runtime_status": "VALID",
        "runtime_reason_code": "RUNTIME_SNAPSHOT_CONSISTENT",
        "runtime_identity": runtime,
        "runtime_container_ready": True,
    }
    path = tmp_path / "target.json"
    path.write_text(json.dumps(snapshot) + "\n", encoding="utf-8")
    path.chmod(0o600)

    components, actual, observed, valid_until = _target_component(
        path,
        ROOT / "contracts/cra_dell_recovery/target_snapshot.v1.schema.json",
        expected_source_host_id="dell-yuki",
        current=CURRENT,
    )

    assert [component.name for component in components] == ["target_snapshot", "runtime_identity", "ffmpeg_identity"]
    assert [component.state for component in components] == ["FRESH", "FRESH", "FRESH"]
    assert actual == target
    assert observed == CURRENT
    assert valid_until == CURRENT + timedelta(seconds=20)

    snapshot["status"] = "BROKEN"
    path.write_text(json.dumps(snapshot) + "\n", encoding="utf-8")
    components, actual, _, _ = _target_component(
        path,
        ROOT / "contracts/cra_dell_recovery/target_snapshot.v1.schema.json",
        expected_source_host_id="dell-yuki",
        current=CURRENT,
    )
    assert [component.state for component in components] == ["ERROR", "ERROR", "ERROR"]
    assert actual is None


def test_publisher_config_load_validates_the_complete_runtime_contract(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    key = Ed25519PrivateKey.from_private_bytes(bytes(range(1, 33)))
    value = copy.deepcopy(_config(tmp_path, key).value)
    value["schema"] = publisher_module.CONFIG_SCHEMA
    value["host_contract_file"] = str(tmp_path / "host.json")
    config_path = tmp_path / "publisher-config.json"

    def read(path: Path, *, maximum_bytes: int = 0) -> dict[str, object]:
        del maximum_bytes
        if path == config_path:
            return value
        if path == Path(str(value["host_contract_file"])):
            return {"host_id": value["host_id"]}
        if path == Path(str(value["release_manifest_file"])):
            return {"release_id": value["release_id"], "component": "dell"}
        raise AssertionError(path)

    monkeypatch.setattr(publisher_module, "read_object", read)
    monkeypatch.setattr(publisher_module, "require_runtime_release", lambda *_args: None)

    loaded = PublisherConfig.load(config_path)

    assert loaded.value == value
    assert loaded.path == config_path


def test_publisher_main_covers_config_check_success_and_safe_block(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    key = Ed25519PrivateKey.from_private_bytes(bytes(range(1, 33)))
    config = _config(tmp_path, key)
    config.value["schema"] = publisher_module.CONFIG_SCHEMA
    monkeypatch.setattr(publisher_module.PublisherConfig, "load", lambda _path: config)

    monkeypatch.setattr(sys, "argv", ["publisher", "--config", "/config.json", "--check-config"])
    publisher_module.main()
    assert json.loads(capsys.readouterr().out)["config"] == "VALID"

    result = {
        "schema": "cra.resilient_host_status.v3",
        "state": "READY",
        "producer_sequence": 7,
        "observed_at": "2026-09-03T00:00:00Z",
        "reason_codes": ["ALL_REQUIRED_SOURCES_FRESH"],
        "components": [{"name": "target_snapshot", "state": "FRESH"}],
    }
    monkeypatch.setattr(publisher_module, "publish", lambda _config: result)
    monkeypatch.setattr(sys, "argv", ["publisher", "--config", "/config.json"])
    publisher_module.main()
    assert json.loads(capsys.readouterr().out)["producer_sequence"] == 7

    recorded_failures: list[dict[str, object]] = []
    monkeypatch.setattr(
        publisher_module,
        "record_publisher_failure",
        lambda **kwargs: recorded_failures.append(kwargs),
    )
    monkeypatch.setattr(publisher_module, "publish", lambda _config: (_ for _ in ()).throw(RuntimeError("blocked")))
    with pytest.raises(RuntimeError, match="blocked"):
        publisher_module.main()
    assert json.loads(capsys.readouterr().err)["status"] == "SAFE_BLOCKED"
    assert len(recorded_failures) == 1
    assert recorded_failures[0]["state_file"] == Path(str(config.value["state_file"]))


def test_chronyc_uncertainty_uses_offset_root_delay_and_dispersion() -> None:
    uncertainty, digest = _chronyc_uncertainty(
        "5BBD5B70,192.0.2.1,3,1788393931.672996378,0.000773283,-0.001180448,"
        "0.000632233,-7.119,0.000,0.081,0.248126164,0.008886389,1044.0,Normal\n"
    )

    assert uncertainty == 134
    assert len(digest) == 64


@pytest.mark.parametrize(
    "tracking",
    [
        "too,few,fields",
        "5BBD5B70,192.0.2.1,0,1788393931,0,0,0,0,0,0,0.1,0.1,10,Normal",
        "5BBD5B70,192.0.2.1,3,1788393931,0,0,0,0,0,0,0.1,0.1,10,Not synchronised",
        "5BBD5B70,192.0.2.1,3,1788393931,nan,0,0,0,0,0,0.1,0.1,10,Normal",
    ],
)
def test_chronyc_uncertainty_rejects_malformed_or_unsynchronized_tracking(tracking: str) -> None:
    with pytest.raises(ValueError, match="CLOCK_TRACKING"):
        _chronyc_uncertainty(tracking)


def test_clock_status_fails_closed_when_measured_uncertainty_exceeds_bound(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    outputs = iter(
        [
            "yes\n",
            "5BBD5B70,192.0.2.1,3,1788393931,0.100,0,0,0,0,0,0.400,0.200,10,Normal\n",
        ]
    )

    class Result:
        def __init__(self, stdout: str) -> None:
            self.stdout = stdout

    monkeypatch.setattr(
        "cra_no_action_soak.resilient_host_status_publisher.subprocess.run",
        lambda *_args, **_kwargs: Result(next(outputs)),
    )

    state, uncertainty, component = _clock_status(current=CURRENT, uncertainty_bound_ms=100)

    assert state == "UNCERTAIN"
    assert uncertainty == 500
    assert component.state == "INVALID"
    assert component.reason_code == "CLOCK_UNCERTAINTY_EXCEEDS_BOUND"


def test_clock_status_accepts_fresh_nonprivileged_tracking_fact(tmp_path: Path) -> None:
    path = tmp_path / "clock.json"
    path.write_text(
        json.dumps(
            {
                "schema": "cra.clock_tracking_fact.v1",
                "observed_at": CURRENT.isoformat().replace("+00:00", "Z"),
                "ntp_synchronized": True,
                "tracking_csv": (
                    "5BBD5B70,192.0.2.1,3,1788393931.672996378,0.000773283,-0.001180448,"
                    "0.000632233,-7.119,0.000,0.081,0.248126164,0.008886389,1044.0,Normal"
                ),
            }
        )
        + "\n",
        encoding="utf-8",
    )
    path.chmod(0o640)

    state, uncertainty, component = _clock_status(
        current=CURRENT + timedelta(seconds=5),
        uncertainty_bound_ms=1000,
        tracking_file=path,
        maximum_tracking_age_seconds=10,
    )

    assert state == "SYNCED"
    assert uncertainty == 134
    assert component.state == "FRESH"


def test_clock_status_rejects_expired_tracking_fact(tmp_path: Path) -> None:
    path = tmp_path / "clock.json"
    path.write_text(
        json.dumps(
            {
                "schema": "cra.clock_tracking_fact.v1",
                "observed_at": CURRENT.isoformat().replace("+00:00", "Z"),
                "ntp_synchronized": True,
                "tracking_csv": (
                    "5BBD5B70,192.0.2.1,3,1788393931.672996378,0.000773283,-0.001180448,"
                    "0.000632233,-7.119,0.000,0.081,0.248126164,0.008886389,1044.0,Normal"
                ),
            }
        )
        + "\n",
        encoding="utf-8",
    )
    path.chmod(0o640)

    state, uncertainty, component = _clock_status(
        current=CURRENT + timedelta(seconds=11),
        uncertainty_bound_ms=1000,
        tracking_file=path,
        maximum_tracking_age_seconds=10,
    )

    assert state == "UNCERTAIN"
    assert uncertainty is None
    assert component.state == "ERROR"


def test_publisher_local_health_components_cover_runtime_evidence_sources(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    systemctl_values = {
        "ActiveState": "active",
        "SubState": "running",
        "InvocationID": "invocation-one",
        "NRestarts": "0",
    }
    monkeypatch.setattr(publisher_module, "_systemctl", lambda _unit, field: systemctl_values[field])
    service = publisher_module._service_component(
        "k3s_service",
        "k3s.service",
        current=CURRENT,
        validity_seconds=10,
    )

    monkeypatch.setattr(
        publisher_module,
        "_effect_counters",
        lambda _path, _current: {
            "effect_boundary_count": 18,
            "exact_transition_count": 1,
            "outcome_unknown_count": 0,
        },
    )
    effect, counters = publisher_module._effect_component(
        tmp_path / "effects.sqlite3",
        current=CURRENT,
        validity_seconds=10,
    )
    disk = publisher_module._disk_component(
        tmp_path,
        minimum_free_bytes=1,
        current=CURRENT,
        validity_seconds=10,
    )
    monkeypatch.setattr(publisher_module, "_certificate_remaining", lambda _path, _current: 7200)
    credentials = publisher_module._credential_component(
        {"observer": str(tmp_path / "observer.pem")},
        minimum_remaining_seconds=3600,
        current=CURRENT,
        validity_seconds=10,
    )
    monkeypatch.setattr(publisher_module, "read_server_resource_status", lambda *_args, **_kwargs: (4096, 7))
    resources = publisher_module._resource_component(
        tmp_path / "resources.json",
        role="dell",
        host_id="dell-stream-runtime",
        release_id="dell-observation-0123456789ab",
        maximum_age_seconds=30,
        current=CURRENT,
        validity_seconds=10,
    )

    assert service.state == "FRESH"
    assert effect.state == "FRESH"
    assert counters is not None and counters["effect_boundary_count"] == 18
    assert disk.state == "FRESH"
    assert credentials.state == "FRESH"
    assert resources.state == "FRESH"


def test_resource_component_validates_after_read_without_local_clock_skew_allowance(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class ReadCompletedAt(datetime):
        @classmethod
        def now(cls, tz: object = None) -> datetime:
            del tz
            return CURRENT + timedelta(milliseconds=2)

    observed_arguments: dict[str, object] = {}

    def read_resources(*_args: object, **kwargs: object) -> tuple[int, int]:
        observed_arguments.update(kwargs)
        return 4096, 7

    monkeypatch.setattr(publisher_module, "datetime", ReadCompletedAt)
    monkeypatch.setattr(publisher_module, "read_server_resource_status", read_resources)

    component = publisher_module._resource_component(
        tmp_path / "resources.json",
        role="dell",
        host_id="dell-stream-runtime",
        release_id="dell-observation-0123456789ab",
        maximum_age_seconds=30,
        current=None,
        validity_seconds=10,
    )

    assert observed_arguments["now"] is None
    assert observed_arguments["maximum_future_skew_seconds"] == 0.0
    assert component.observed_at == CURRENT + timedelta(milliseconds=2)
    assert component.state == "FRESH"


def test_exact_transition_binding_requires_one_reconciled_physical_attempt(tmp_path: Path) -> None:
    before = {"host_id": "dell-yuki", "ffmpeg_pid": 100}
    after = {"host_id": "dell-yuki", "ffmpeg_pid": 200}
    database = tmp_path / "effects.sqlite3"
    connection = sqlite3.connect(database)
    connection.execute(
        "CREATE TABLE effect_scope_fences ("
        "effect_scope_id TEXT, owner_request_id TEXT, state TEXT, "
        "physical_attempt_count INTEGER, identity_json TEXT, result_json TEXT, updated_at TEXT)"
    )
    connection.execute(
        "INSERT INTO effect_scope_fences VALUES (?, ?, ?, ?, ?, ?, ?)",
        (
            "scope-one",
            "request-one",
            "RECONCILED_EFFECT_OBSERVED",
            1,
            json.dumps(before),
            json.dumps({"observed_target": after, "physical_effect_count": 1}),
            "2026-09-03T00:00:00Z",
        ),
    )
    connection.commit()
    connection.close()

    binding = publisher_module._exact_transition_binding(
        database,
        before_digest=publisher_module._digest(before),
        after_target=after,
    )

    assert binding is not None
    assert binding.effect_request_id == "request-one"
    assert binding.effect_scope_id == "scope-one"
    assert binding.before_target_identity_sha256 == publisher_module._digest(before)
    assert binding.after_target_identity_sha256 == publisher_module._digest(after)
