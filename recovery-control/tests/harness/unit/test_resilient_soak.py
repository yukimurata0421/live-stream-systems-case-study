from __future__ import annotations

import copy
import errno
import json
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from cra_dell_recovery.canonical import Signer
from cra_dell_recovery.time import isoformat_utc
from cra_no_action_soak import resilient_soak as soak_module
from cra_no_action_soak.host_status import atomic_write_json
from cra_no_action_soak.resilient_host_status import (
    ComponentStatus,
    build_resilient_status,
    record_publisher_failure,
)
from cra_no_action_soak.resilient_soak import (
    SAMPLE_SCHEMA,
    EvidenceSnapshot,
    SoakConfig,
    _sample_hash,
    collect_sample,
    evaluate_samples,
    read_samples,
)

ROOT = Path(__file__).resolve().parents[3]
SCHEMA = ROOT / "contracts/cra_no_action_soak/resilient_host_status.v3.schema.json"
CURRENT = datetime(2026, 9, 3, 0, 0, tzinfo=UTC)
IDENTITY = {
    "release_manifest_sha256": "1" * 64,
    "runtime_manifest_sha256": "2" * 64,
    "configuration_set_sha256": "3" * 64,
    "maintenance_restart_policy_sha256": "4" * 64,
}


def _public(path: Path, key: Ed25519PrivateKey) -> None:
    path.write_bytes(
        key.public_key().public_bytes(
            serialization.Encoding.PEM,
            serialization.PublicFormat.SubjectPublicKeyInfo,
        )
    )
    path.chmod(0o600)


def _config(tmp_path: Path, dell_key: Ed25519PrivateKey, arena_key: Ed25519PrivateKey) -> SoakConfig:
    dell_public = tmp_path / "dell.pem"
    arena_public = tmp_path / "arena.pem"
    _public(dell_public, dell_key)
    _public(arena_public, arena_key)
    return SoakConfig(
        {
            "cra_release_id": "cra-no-action-0123456789ab",
            "resilient_host_status_schema_file": str(SCHEMA),
            "arena_key_id": "arena-key",
            "arena_public_key_file": str(arena_public),
            "arena_host_id": "arena-monitoring-facts",
            "arena_release_id": "arena-cra-projection-0123456789ab",
            "dell_key_id": "dell-key",
            "dell_public_key_file": str(dell_public),
            "dell_host_id": "dell-stream-runtime",
            "dell_release_id": "dell-observation-0123456789ab",
            "arena_status_inbox_file": str(tmp_path / "arena-status.json"),
            "cra_pull_status_file": str(tmp_path / "pull-status.json"),
            "cra_pull_recovery_state_file": str(tmp_path / "pull-recovery-state.json"),
            "evidence_file": str(tmp_path / "evidence.jsonl"),
            "state_file": str(tmp_path / "state.json"),
            "gate_file": str(tmp_path / "gate.json"),
            "minimum_duration_seconds": 60,
            "maximum_sample_gap_seconds": 35,
            "maximum_recovery_episode_seconds": 30,
            "maximum_report_age_seconds": 15,
            "maximum_clock_uncertainty_ms": 1000,
            "maximum_evidence_bytes": 1024 * 1024,
        },
        tmp_path / "config.json",
    )


def _recovery_state(
    now: datetime,
    *,
    terminal_episode_count: int = 0,
    failure_attempt_count: int | None = None,
    event_count: int | None = None,
    current_state: str = "READY",
) -> dict[str, object]:
    failure_count = terminal_episode_count if failure_attempt_count is None else failure_attempt_count
    events = terminal_episode_count * (2 if current_state == "READY" else 1) if event_count is None else event_count
    return {
        "schema": "cra.transport_recovery_state.v1",
        "component": "cra_arena_resilient_status_pull",
        "release_id": "cra-no-action-0123456789ab",
        "current_state": current_state,
        "current_episode": None,
        "last_episode": {} if terminal_episode_count else None,
        "episode_count": terminal_episode_count,
        "recovered_episode_count": 0,
        "terminal_episode_count": terminal_episode_count,
        "failure_attempt_count": failure_count,
        "retry_attempt_count": 0,
        "exhausted_invocation_count": 0,
        "maximum_recovery_duration_ms": 0,
        "event_count": events,
        "last_event_hash": ("a" * 64 if events else "0" * 64),
        "updated_at": isoformat_utc(now),
    }


def _status_pair(
    tmp_path: Path,
    dell_key: Ed25519PrivateKey,
    arena_key: Ed25519PrivateKey,
    *,
    now: datetime,
    dell_ready: bool = True,
    arena_resource_fresh: bool = True,
    dell_boot_id: str = "dell-boot",
    arena_boot_id: str = "arena-boot",
) -> tuple[dict[str, object], dict[str, object]]:
    component = (
        ComponentStatus(
            "target_snapshot",
            "FRESH",
            "TARGET_SNAPSHOT_FRESH",
            now,
            now + timedelta(seconds=120),
            "5" * 64,
        )
        if dell_ready
        else ComponentStatus("target_snapshot", "ERROR", "TARGET_SNAPSHOT_UNAVAILABLE")
    )
    dell = build_resilient_status(
        state_file=tmp_path / "dell-state.json",
        transition_journal=tmp_path / "dell-events.jsonl",
        schema_file=SCHEMA,
        private_key=dell_key,
        key_id="dell-key",
        role="dell",
        host_id="dell-stream-runtime",
        release_id="dell-observation-0123456789ab",
        host_boot_id=dell_boot_id,
        identity=IDENTITY,
        components=[component],
        source_observed_at=now,
        source_valid_until=now + timedelta(seconds=120),
        target_identity_sha256="6" * 64,
        source_payload_sha256="7" * 64,
        origin_host_id="dell-stream-runtime",
        physical_effect_count=0,
        clock_uncertainty_ms=1000,
        reporter_lease_seconds=120,
        now=now,
    )
    arena_components = [
        ComponentStatus(
            "dell_signed_status",
            "FRESH",
            "DELL_SIGNED_STATUS_FRESH",
            now,
            now + timedelta(seconds=120),
            str(dell["payload_sha256"]),
        )
    ]
    if not arena_resource_fresh:
        arena_components.append(ComponentStatus("publisher_resources", "ERROR", "SERVER_RESOURCE_STATUS_STALE"))
    arena = build_resilient_status(
        state_file=tmp_path / "arena-state.json",
        transition_journal=tmp_path / "arena-events.jsonl",
        schema_file=SCHEMA,
        private_key=arena_key,
        key_id="arena-key",
        role="arena",
        host_id="arena-monitoring-facts",
        release_id="arena-cra-projection-0123456789ab",
        host_boot_id=arena_boot_id,
        identity=IDENTITY,
        components=arena_components,
        source_observed_at=now,
        source_valid_until=now + timedelta(seconds=120),
        target_identity_sha256="6" * 64,
        source_payload_sha256="7" * 64,
        origin_host_id="dell-stream-runtime",
        physical_effect_count=0,
        upstream_status=dell,
        track_target_transitions=False,
        explicit_state="DEGRADED_SOURCE" if not dell_ready else None,
        explicit_reason_codes=("DELL_DEGRADED_SOURCE",) if not dell_ready else (),
        clock_uncertainty_ms=1000,
        reporter_lease_seconds=120,
        now=now,
    )
    return dell, arena


def _samples(
    tmp_path: Path,
    dell_key: Ed25519PrivateKey,
    arena_key: Ed25519PrivateKey,
) -> list[dict[str, object]]:
    result: list[dict[str, object]] = []
    previous_hash: str | None = None
    for index, offset in enumerate((0, 30, 60), start=1):
        now = CURRENT + timedelta(seconds=offset)
        dell, arena = _status_pair(tmp_path, dell_key, arena_key, now=now)
        sample: dict[str, object] = {
            "schema": SAMPLE_SCHEMA,
            "sample_sequence": index,
            "observed_at": now.isoformat().replace("+00:00", "Z"),
            "previous_sample_hash": previous_hash,
            "cra_release_id": "cra-no-action-0123456789ab",
            "arena_status": arena,
            "dell_status_payload_sha256": dell["payload_sha256"],
            "cra_pull_status": {
                "status": "READY",
                "source_producer_instance_id": arena["producer_instance_id"],
                "source_producer_sequence": arena["producer_sequence"],
            },
            "cra_pull_recovery_state": _recovery_state(now),
        }
        sample["sample_hash"] = _sample_hash(sample)
        previous_hash = str(sample["sample_hash"])
        result.append(sample)
    return result


def _write_collect_inputs(config: SoakConfig, arena: dict[str, object], observed: datetime) -> None:
    atomic_write_json(Path(str(config.value["arena_status_inbox_file"])), arena)
    atomic_write_json(
        Path(str(config.value["cra_pull_status_file"])),
        {
            "schema": "cra.resilient_host_status_pull_status.v2",
            "component": "cra_arena_resilient_status_pull",
            "status": "READY",
            "source_state": "READY",
            "source_release_id": config.value["arena_release_id"],
            "source_producer_instance_id": arena["producer_instance_id"],
            "source_producer_sequence": arena["producer_sequence"],
            "disposition": "UPDATED",
            "observed_at": observed.isoformat().replace("+00:00", "Z"),
            "transport_attempt_count": 1,
            "transport_retry_count": 0,
            "source_remaining_lease_seconds": 120,
            "control_capability_count": 0,
            "physical_effect_count": 0,
        },
    )
    atomic_write_json(soak_module.pull_recovery_path(config), _recovery_state(observed))


def test_resilient_soak_gate_accepts_complete_signed_two_hop_chain(tmp_path: Path) -> None:
    dell_key = Ed25519PrivateKey.from_private_bytes(bytes(range(1, 33)))
    arena_key = Ed25519PrivateKey.from_private_bytes(bytes(range(33, 65)))
    config = _config(tmp_path, dell_key, arena_key)

    result = evaluate_samples(_samples(tmp_path, dell_key, arena_key), config=config)

    assert result["eligible"] is True
    assert result["status"] == "PASS"
    assert result["duration_seconds"] == 60
    assert result["sample_gap_summary"] == {
        "average_seconds": 30.0,
        "p95_seconds": 30.0,
        "count": 2,
        "over_15_seconds": 2,
        "over_30_seconds": 0,
        "over_maximum_seconds": 0,
    }
    assert result["physical_effect_count_delta"] == 0
    assert result["blockers"] == []


def test_resilient_soak_gate_rejects_a_recovered_terminal_pull_episode(
    tmp_path: Path,
) -> None:
    dell_key = Ed25519PrivateKey.from_private_bytes(bytes(range(1, 33)))
    arena_key = Ed25519PrivateKey.from_private_bytes(bytes(range(33, 65)))
    config = _config(tmp_path, dell_key, arena_key)
    samples = _samples(tmp_path, dell_key, arena_key)
    for sample in samples:
        sample["cra_pull_recovery_state"] = _recovery_state(
            datetime.fromisoformat(str(sample["observed_at"]).replace("Z", "+00:00")),
            terminal_episode_count=1,
        )
        sample["sample_hash"] = _sample_hash(sample)
    samples[1]["previous_sample_hash"] = samples[0]["sample_hash"]
    samples[1]["sample_hash"] = _sample_hash(samples[1])
    samples[2]["previous_sample_hash"] = samples[1]["sample_hash"]
    samples[2]["sample_hash"] = _sample_hash(samples[2])

    result = evaluate_samples(samples, config=config)

    assert result["eligible"] is False
    assert result["status"] == "FAIL"
    assert "CRA_PULL_TERMINAL_EPISODE_OBSERVED" in result["blockers"]
    assert result["cra_pull_recovery_counters_delta"]["terminal_episode_count"] == 0


def test_resilient_soak_gate_rejects_a_publisher_failure_hidden_between_ready_samples(
    tmp_path: Path,
) -> None:
    dell_key = Ed25519PrivateKey.from_private_bytes(bytes(range(1, 33)))
    arena_key = Ed25519PrivateKey.from_private_bytes(bytes(range(33, 65)))
    config = _config(tmp_path, dell_key, arena_key)
    pairs = [_status_pair(tmp_path, dell_key, arena_key, now=CURRENT)]
    record_publisher_failure(
        state_file=tmp_path / "arena-state.json",
        host_id="arena-monitoring-facts",
        host_boot_id="arena-boot",
        now=CURRENT + timedelta(seconds=10),
    )
    pairs.extend(
        [
            _status_pair(tmp_path, dell_key, arena_key, now=CURRENT + timedelta(seconds=30)),
            _status_pair(tmp_path, dell_key, arena_key, now=CURRENT + timedelta(seconds=60)),
        ]
    )
    samples: list[dict[str, object]] = []
    previous_hash: str | None = None
    for index, (offset, pair) in enumerate(zip((0, 30, 60), pairs, strict=True), start=1):
        dell, arena = pair
        observed = CURRENT + timedelta(seconds=offset)
        sample: dict[str, object] = {
            "schema": SAMPLE_SCHEMA,
            "sample_sequence": index,
            "observed_at": isoformat_utc(observed),
            "previous_sample_hash": previous_hash,
            "cra_release_id": "cra-no-action-0123456789ab",
            "arena_status": arena,
            "dell_status_payload_sha256": dell["payload_sha256"],
            "cra_pull_status": {
                "status": "READY",
                "source_producer_instance_id": arena["producer_instance_id"],
                "source_producer_sequence": arena["producer_sequence"],
            },
            "cra_pull_recovery_state": _recovery_state(observed),
        }
        sample["sample_hash"] = _sample_hash(sample)
        previous_hash = str(sample["sample_hash"])
        samples.append(sample)

    result = evaluate_samples(samples, config=config)

    assert result["eligible"] is False
    assert result["status"] == "FAIL"
    assert "ARENA_PUBLISHER_INVOCATION_FAILED" in result["blockers"]
    assert result["publisher_failure_event_count"] == {"arena": 1, "dell": 0}


def test_resilient_soak_gate_rejects_publisher_failure_in_first_sample_history(
    tmp_path: Path,
) -> None:
    dell_key = Ed25519PrivateKey.from_private_bytes(bytes(range(1, 33)))
    arena_key = Ed25519PrivateKey.from_private_bytes(bytes(range(33, 65)))
    config = _config(tmp_path, dell_key, arena_key)
    record_publisher_failure(
        state_file=tmp_path / "arena-state.json",
        host_id="arena-monitoring-facts",
        host_boot_id="arena-boot",
        now=CURRENT - timedelta(seconds=1),
    )

    result = evaluate_samples(_samples(tmp_path, dell_key, arena_key), config=config)

    assert result["status"] == "FAIL"
    assert "ARENA_PUBLISHER_INVOCATION_FAILED" in result["blockers"]
    assert result["publisher_failure_event_count"] == {"arena": 1, "dell": 0}


def test_resilient_soak_gate_uses_signed_health_transitions_instead_of_sparse_sample_aliasing(
    tmp_path: Path,
) -> None:
    dell_key = Ed25519PrivateKey.from_private_bytes(bytes(range(1, 33)))
    arena_key = Ed25519PrivateKey.from_private_bytes(bytes(range(33, 65)))
    config = _config(tmp_path, dell_key, arena_key)

    first = _status_pair(
        tmp_path,
        dell_key,
        arena_key,
        now=CURRENT,
        dell_ready=False,
    )
    _status_pair(tmp_path, dell_key, arena_key, now=CURRENT + timedelta(seconds=5))
    second = _status_pair(
        tmp_path,
        dell_key,
        arena_key,
        now=CURRENT + timedelta(seconds=15),
        arena_resource_fresh=False,
    )
    third = _status_pair(tmp_path, dell_key, arena_key, now=CURRENT + timedelta(seconds=25))
    fourth = _status_pair(tmp_path, dell_key, arena_key, now=CURRENT + timedelta(seconds=55))

    samples: list[dict[str, object]] = []
    previous_hash: str | None = None
    for index, (offset, pair) in enumerate(zip((0, 20, 40, 60), (first, second, third, fourth), strict=True), start=1):
        dell, arena = pair
        sample: dict[str, object] = {
            "schema": SAMPLE_SCHEMA,
            "sample_sequence": index,
            "observed_at": isoformat_utc(CURRENT + timedelta(seconds=offset)),
            "previous_sample_hash": previous_hash,
            "cra_release_id": "cra-no-action-0123456789ab",
            "arena_status": arena,
            "dell_status_payload_sha256": dell["payload_sha256"],
            "cra_pull_status": {
                "status": "READY",
                "source_producer_instance_id": arena["producer_instance_id"],
                "source_producer_sequence": arena["producer_sequence"],
            },
            "cra_pull_recovery_state": _recovery_state(CURRENT + timedelta(seconds=offset)),
        }
        sample["sample_hash"] = _sample_hash(sample)
        previous_hash = str(sample["sample_hash"])
        samples.append(sample)

    result = evaluate_samples(samples, config=config)

    assert result["eligible"] is True
    assert result["maximum_recovery_episode_seconds"] == {"arena": 10.0, "dell": 5.0}
    assert "ARENA_RECOVERY_EPISODE_TOO_LONG" not in result["blockers"]


def test_resilient_soak_gate_allows_exact_replay_within_lease_before_later_advance(tmp_path: Path) -> None:
    dell_key = Ed25519PrivateKey.from_private_bytes(bytes(range(1, 33)))
    arena_key = Ed25519PrivateKey.from_private_bytes(bytes(range(33, 65)))
    config = _config(tmp_path, dell_key, arena_key)
    config.value["maximum_report_age_seconds"] = 35
    samples = _samples(tmp_path, dell_key, arena_key)
    repeated = dict(samples[1])
    repeated["arena_status"] = samples[0]["arena_status"]
    repeated["dell_status_payload_sha256"] = samples[0]["dell_status_payload_sha256"]
    repeated["cra_pull_status"] = {
        "status": "READY",
        "source_producer_instance_id": samples[0]["arena_status"]["producer_instance_id"],  # type: ignore[index]
        "source_producer_sequence": samples[0]["arena_status"]["producer_sequence"],  # type: ignore[index]
    }
    repeated["sample_hash"] = _sample_hash(repeated)
    samples[1] = repeated
    samples[2]["previous_sample_hash"] = repeated["sample_hash"]
    samples[2]["sample_hash"] = _sample_hash(samples[2])

    result = evaluate_samples(samples, config=config)

    assert result["eligible"] is True
    assert result["blockers"] == []


def test_resilient_soak_gate_accepts_boot_bound_producer_and_health_journal_rotation(tmp_path: Path) -> None:
    dell_key = Ed25519PrivateKey.from_private_bytes(bytes(range(1, 33)))
    arena_key = Ed25519PrivateKey.from_private_bytes(bytes(range(33, 65)))
    config = _config(tmp_path, dell_key, arena_key)
    pairs = [
        _status_pair(tmp_path, dell_key, arena_key, now=CURRENT),
        _status_pair(
            tmp_path,
            dell_key,
            arena_key,
            now=CURRENT + timedelta(seconds=30),
            dell_boot_id="dell-boot-two",
            arena_boot_id="arena-boot-two",
        ),
        _status_pair(
            tmp_path,
            dell_key,
            arena_key,
            now=CURRENT + timedelta(seconds=60),
            dell_boot_id="dell-boot-two",
            arena_boot_id="arena-boot-two",
        ),
    ]
    samples: list[dict[str, object]] = []
    previous_hash: str | None = None
    for index, (offset, pair) in enumerate(zip((0, 30, 60), pairs, strict=True), start=1):
        dell, arena = pair
        sample: dict[str, object] = {
            "schema": SAMPLE_SCHEMA,
            "sample_sequence": index,
            "observed_at": isoformat_utc(CURRENT + timedelta(seconds=offset)),
            "previous_sample_hash": previous_hash,
            "cra_release_id": "cra-no-action-0123456789ab",
            "arena_status": arena,
            "dell_status_payload_sha256": dell["payload_sha256"],
            "cra_pull_status": {
                "status": "READY",
                "source_producer_instance_id": arena["producer_instance_id"],
                "source_producer_sequence": arena["producer_sequence"],
            },
            "cra_pull_recovery_state": _recovery_state(CURRENT + timedelta(seconds=offset)),
        }
        sample["sample_hash"] = _sample_hash(sample)
        previous_hash = str(sample["sample_hash"])
        samples.append(sample)

    result = evaluate_samples(samples, config=config)

    assert result["eligible"] is True
    assert result["producer_rotation_count"] == {"arena": 1, "dell": 1}
    assert result["health_journal_rotation_count"] == {"arena": 1, "dell": 1}
    assert result["blockers"] == []


def test_resilient_soak_gate_rejects_sequence_regression_with_valid_signatures(tmp_path: Path) -> None:
    dell_key = Ed25519PrivateKey.from_private_bytes(bytes(range(1, 33)))
    arena_key = Ed25519PrivateKey.from_private_bytes(bytes(range(33, 65)))
    config = _config(tmp_path, dell_key, arena_key)
    samples = _samples(tmp_path, dell_key, arena_key)
    regressed = dict(samples[2])
    regressed["arena_status"] = samples[0]["arena_status"]
    regressed["dell_status_payload_sha256"] = samples[0]["dell_status_payload_sha256"]
    regressed["cra_pull_status"] = {
        "status": "READY",
        "source_producer_instance_id": samples[0]["arena_status"]["producer_instance_id"],  # type: ignore[index]
        "source_producer_sequence": samples[0]["arena_status"]["producer_sequence"],  # type: ignore[index]
    }
    regressed["sample_hash"] = _sample_hash(regressed)
    samples[2] = regressed

    result = evaluate_samples(samples, config=config)

    assert result["eligible"] is False
    assert any("SEQUENCE_REGRESSION" in blocker for blocker in result["blockers"])


def test_resilient_soak_gate_rejects_same_sequence_with_a_different_valid_signature(tmp_path: Path) -> None:
    dell_key = Ed25519PrivateKey.from_private_bytes(bytes(range(1, 33)))
    arena_key = Ed25519PrivateKey.from_private_bytes(bytes(range(33, 65)))
    config = _config(tmp_path, dell_key, arena_key)
    config.value["maximum_report_age_seconds"] = 35
    samples = _samples(tmp_path, dell_key, arena_key)
    conflicting_status = cast(dict[str, Any], copy.deepcopy(samples[0]["arena_status"]))
    conflicting_status["clock"]["uncertainty_ms"] = 999
    conflicting_status.pop("payload_sha256", None)
    conflicting_status.pop("signature", None)
    conflicting_status = Signer("arena-key", arena_key).sign(conflicting_status)
    conflicting = dict(samples[1])
    conflicting["arena_status"] = conflicting_status
    conflicting["dell_status_payload_sha256"] = samples[0]["dell_status_payload_sha256"]
    conflicting["cra_pull_status"] = {
        "status": "READY",
        "source_producer_instance_id": conflicting_status["producer_instance_id"],
        "source_producer_sequence": conflicting_status["producer_sequence"],
    }
    conflicting["sample_hash"] = _sample_hash(conflicting)
    samples[1] = conflicting
    samples[2]["previous_sample_hash"] = conflicting["sample_hash"]
    samples[2]["sample_hash"] = _sample_hash(samples[2])

    result = evaluate_samples(samples, config=config)

    assert result["eligible"] is False
    assert any("SEQUENCE_CONFLICT" in blocker for blocker in result["blockers"])


def test_soak_config_load_accepts_complete_runtime_contract(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dell_key = Ed25519PrivateKey.from_private_bytes(bytes(range(1, 33)))
    arena_key = Ed25519PrivateKey.from_private_bytes(bytes(range(33, 65)))
    value = copy.deepcopy(_config(tmp_path, dell_key, arena_key).value)
    value["schema"] = soak_module.CONFIG_SCHEMA
    path = tmp_path / "soak.json"
    path.write_text(json.dumps(value) + "\n", encoding="utf-8")
    path.chmod(0o600)
    monkeypatch.setattr(soak_module, "require_runtime_release", lambda *_args: None)

    loaded = SoakConfig.load(path)

    assert loaded.value == value
    assert loaded.path == path


def test_collect_sample_persists_hash_chained_evidence_and_replay_state(tmp_path: Path) -> None:
    dell_key = Ed25519PrivateKey.from_private_bytes(bytes(range(1, 33)))
    arena_key = Ed25519PrivateKey.from_private_bytes(bytes(range(33, 65)))
    config = _config(tmp_path, dell_key, arena_key)
    dell, arena = _status_pair(tmp_path / "status", dell_key, arena_key, now=CURRENT)
    _write_collect_inputs(config, arena, CURRENT)
    first = collect_sample(config, now=CURRENT)
    _write_collect_inputs(config, arena, CURRENT + timedelta(seconds=1))
    second = collect_sample(config, now=CURRENT + timedelta(seconds=1))
    samples = read_samples(
        Path(str(config.value["evidence_file"])),
        maximum_bytes=int(config.value["maximum_evidence_bytes"]),
    )

    assert first["sample_sequence"] == 1
    assert second["sample_sequence"] == 2
    assert second["previous_sample_hash"] == first["sample_hash"]
    assert samples == [first, second]
    assert json.loads(Path(str(config.value["state_file"])).read_text(encoding="utf-8"))["sample_count"] == 2
    assert first["dell_status_payload_sha256"] == dell["payload_sha256"]


def test_first_resilient_append_state_failure_recovers_exactly_once(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dell_key = Ed25519PrivateKey.from_private_bytes(bytes(range(1, 33)))
    arena_key = Ed25519PrivateKey.from_private_bytes(bytes(range(33, 65)))
    config = _config(tmp_path, dell_key, arena_key)
    _, arena = _status_pair(tmp_path / "status", dell_key, arena_key, now=CURRENT)
    _write_collect_inputs(config, arena, CURRENT)
    original = soak_module.atomic_write_json
    failed: list[int] = []

    def fail_first_state_commit(path: Path, value: Any) -> None:
        if value.get("sample_count") == 1 and not failed:
            failed.append(1)
            raise OSError(errno.ENOSPC, "injected state commit failure")
        original(path, value)

    with monkeypatch.context() as patch:
        patch.setattr(soak_module, "atomic_write_json", fail_first_state_commit)
        with pytest.raises(OSError, match="state commit failure"):
            collect_sample(config, now=CURRENT)
    state_file = Path(str(config.value["state_file"]))
    evidence = Path(str(config.value["evidence_file"]))
    assert json.loads(state_file.read_text())["sample_count"] == 0
    first_frame = evidence.read_bytes()
    assert len(first_frame.splitlines()) == 1

    _write_collect_inputs(config, arena, CURRENT + timedelta(seconds=1))
    recovered = collect_sample(config, now=CURRENT + timedelta(seconds=1))
    assert recovered["sample_sequence"] == 2
    assert evidence.read_bytes().startswith(first_frame)
    assert [row["sample_sequence"] for row in read_samples(evidence, maximum_bytes=1024 * 1024)] == [1, 2]


def test_negative_control_detects_missing_initial_resilient_head(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dell_key = Ed25519PrivateKey.from_private_bytes(bytes(range(1, 33)))
    arena_key = Ed25519PrivateKey.from_private_bytes(bytes(range(33, 65)))
    config = _config(tmp_path, dell_key, arena_key)
    _, arena = _status_pair(tmp_path / "status", dell_key, arena_key, now=CURRENT)
    _write_collect_inputs(config, arena, CURRENT)
    original = soak_module.atomic_write_json

    def omit_empty_head(path: Path, value: Any) -> None:
        if value.get("sample_count") != 0:
            original(path, value)

    monkeypatch.setattr(soak_module, "atomic_write_json", omit_empty_head)
    failed: list[int] = []

    def fail_commit(path: Path, value: Any) -> None:
        if value.get("sample_count") == 1 and not failed:
            failed.append(1)
            raise OSError(errno.ENOSPC, "injected state commit failure")
        omit_empty_head(path, value)

    with monkeypatch.context() as patch:
        patch.setattr(soak_module, "atomic_write_json", fail_commit)
        with pytest.raises(OSError):
            collect_sample(config, now=CURRENT)
    with pytest.raises(ValueError, match="RESILIENT_SOAK_STATE_MISSING"):
        collect_sample(config, now=CURRENT + timedelta(seconds=1))


def test_last_evidence_line_fails_closed_for_truncation_size_and_non_object(tmp_path: Path) -> None:
    path = tmp_path / "evidence.jsonl"
    assert soak_module._last_line(path, maximum_bytes=1024) is None

    path.write_bytes(b"")
    assert soak_module._last_line(path, maximum_bytes=1024) is None

    path.write_bytes(b"{}\n")
    with pytest.raises(ValueError, match="TOO_LARGE"):
        soak_module._last_line(path, maximum_bytes=1)

    path.write_bytes(b"{}")
    with pytest.raises(ValueError, match="PARTIAL_TAIL"):
        soak_module._last_line(path, maximum_bytes=1024)

    path.write_bytes(b"[]\n")
    with pytest.raises(ValueError, match="SAMPLE_NOT_OBJECT"):
        soak_module._last_line(path, maximum_bytes=1024)


def test_append_rejects_capacity_before_writing_partial_sample(tmp_path: Path) -> None:
    path = tmp_path / "evidence.jsonl"
    value = {"schema": SAMPLE_SCHEMA, "sample_sequence": 1}
    raw = soak_module._serialized_line(value)

    with pytest.raises(ValueError, match="CAPACITY_EXHAUSTED"):
        soak_module._append_line(path, value, maximum_bytes=len(raw) - 1)

    assert path.read_bytes() == b""
    soak_module._append_line(path, value, maximum_bytes=len(raw))
    assert path.read_bytes() == raw


def test_evidence_snapshot_is_bound_to_atomic_state_while_collector_advances(
    tmp_path: Path,
) -> None:
    dell_key = Ed25519PrivateKey.from_private_bytes(bytes(range(1, 33)))
    arena_key = Ed25519PrivateKey.from_private_bytes(bytes(range(33, 65)))
    config = _config(tmp_path, dell_key, arena_key)
    samples = _samples(tmp_path, dell_key, arena_key)
    evidence = Path(str(config.value["evidence_file"]))
    evidence.write_text(
        "".join(json.dumps(item, separators=(",", ":"), sort_keys=True) + "\n" for item in samples[:2]),
        encoding="utf-8",
    )
    atomic_write_json(
        Path(str(config.value["state_file"])),
        {
            "schema": soak_module.STATE_SCHEMA,
            "sample_count": 1,
            "last_sample_hash": samples[0]["sample_hash"],
            "last_observed_at": samples[0]["observed_at"],
        },
    )

    snapshot = soak_module.evidence_snapshot(config)

    assert list(snapshot) == samples[:1]
    assert snapshot.bytes_read < snapshot.file_size_at_open
    assert evaluate_samples(iter(samples), config=config)["sample_count"] == 3


@pytest.mark.parametrize(
    ("duration", "limit", "expected"),
    [
        (45.0, 1024 * 1024 * 1024, "INSUFFICIENT"),
        (135.0, 2 * 1024 * 1024 * 1024, "SUFFICIENT"),
    ],
)
def test_capacity_forecast_blocks_unsafe_seven_day_profile(
    tmp_path: Path,
    duration: float,
    limit: int,
    expected: str,
) -> None:
    dell_key = Ed25519PrivateKey.from_private_bytes(bytes(range(1, 33)))
    arena_key = Ed25519PrivateKey.from_private_bytes(bytes(range(33, 65)))
    config = _config(tmp_path, dell_key, arena_key)
    config.value["minimum_duration_seconds"] = 7 * 86400
    config.value["maximum_evidence_bytes"] = limit
    snapshot = EvidenceSnapshot(tmp_path / "unused.jsonl", limit, 10, "f" * 64, "2026-09-03T00:02:15Z")
    snapshot.bytes_read = 137_460
    snapshot.file_size_at_open = 137_460
    result: dict[str, Any] = {
        "sample_count": 10,
        "duration_seconds": duration,
        "blockers": ["SOAK_DURATION_INSUFFICIENT"],
        "eligible": False,
        "status": "NOT_YET_ELIGIBLE",
    }

    soak_module.add_evidence_capacity_assessment(result, snapshot=snapshot, config=config)

    assert result["evidence_capacity"]["assessment"] == expected
    assert ("EVIDENCE_CAPACITY_INSUFFICIENT" in result["blockers"]) is (expected == "INSUFFICIENT")


def test_formal_gate_rejects_a_stale_final_sample(tmp_path: Path) -> None:
    dell_key = Ed25519PrivateKey.from_private_bytes(bytes(range(1, 33)))
    arena_key = Ed25519PrivateKey.from_private_bytes(bytes(range(33, 65)))
    config = _config(tmp_path, dell_key, arena_key)

    result = evaluate_samples(
        _samples(tmp_path, dell_key, arena_key),
        config=config,
        evaluated_at=CURRENT + timedelta(seconds=96),
    )

    assert result["final_sample_age_seconds"] == 36
    assert "FINAL_SAMPLE_STALE" in result["blockers"]
    assert result["eligible"] is False


def test_watchdog_separates_formal_duration_from_live_freshness(tmp_path: Path) -> None:
    dell_key = Ed25519PrivateKey.from_private_bytes(bytes(range(1, 33)))
    arena_key = Ed25519PrivateKey.from_private_bytes(bytes(range(33, 65)))
    config = _config(tmp_path, dell_key, arena_key)
    config.value["maximum_sample_gap_seconds"] = 45
    sample = _samples(tmp_path, dell_key, arena_key)[0]
    evidence = Path(str(config.value["evidence_file"]))
    evidence.write_text(json.dumps(sample, separators=(",", ":"), sort_keys=True) + "\n", encoding="utf-8")
    atomic_write_json(
        Path(str(config.value["state_file"])),
        {
            "schema": soak_module.STATE_SCHEMA,
            "sample_count": 1,
            "last_sample_hash": sample["sample_hash"],
            "last_observed_at": sample["observed_at"],
        },
    )
    atomic_write_json(
        Path(str(config.value["gate_file"])),
        {
            "schema": soak_module.GATE_SCHEMA,
            "status": "NOT_YET_ELIGIBLE",
            "eligible": False,
            "blockers": ["SOAK_DURATION_INSUFFICIENT"],
            "evaluated_at": isoformat_utc(CURRENT),
            "evidence_capacity": {"assessment": "SUFFICIENT"},
        },
    )
    atomic_write_json(
        soak_module.pull_recovery_path(config),
        _recovery_state(CURRENT),
    )

    ready = soak_module.evaluate_watchdog(config, now=CURRENT + timedelta(seconds=10))
    stale = soak_module.evaluate_watchdog(config, now=CURRENT + timedelta(seconds=46))

    assert ready["status"] == "READY"
    assert ready["formal_gate_eligible"] is False
    assert ready["formal_gate_blockers"] == ["SOAK_DURATION_INSUFFICIENT"]
    assert ready["blockers"] == []
    assert stale["status"] == "AT_RISK"
    assert "COLLECTOR_STATE_STALE" in stale["blockers"]


def test_watchdog_rejects_a_terminal_pull_episode_even_after_ready_is_restored(
    tmp_path: Path,
) -> None:
    dell_key = Ed25519PrivateKey.from_private_bytes(bytes(range(1, 33)))
    arena_key = Ed25519PrivateKey.from_private_bytes(bytes(range(33, 65)))
    config = _config(tmp_path, dell_key, arena_key)
    sample = _samples(tmp_path, dell_key, arena_key)[0]
    Path(str(config.value["evidence_file"])).write_text(
        json.dumps(sample, separators=(",", ":"), sort_keys=True) + "\n",
        encoding="utf-8",
    )
    atomic_write_json(
        Path(str(config.value["state_file"])),
        {
            "schema": soak_module.STATE_SCHEMA,
            "sample_count": 1,
            "last_sample_hash": sample["sample_hash"],
            "last_observed_at": sample["observed_at"],
        },
    )
    atomic_write_json(
        Path(str(config.value["gate_file"])),
        {
            "schema": soak_module.GATE_SCHEMA,
            "status": "NOT_YET_ELIGIBLE",
            "eligible": False,
            "blockers": ["SOAK_DURATION_INSUFFICIENT"],
            "evaluated_at": isoformat_utc(CURRENT),
            "evidence_capacity": {"assessment": "SUFFICIENT"},
        },
    )
    atomic_write_json(
        soak_module.pull_recovery_path(config),
        _recovery_state(CURRENT, terminal_episode_count=1),
    )

    result = soak_module.evaluate_watchdog(config, now=CURRENT + timedelta(seconds=10))

    assert result["status"] == "AT_RISK"
    assert "CRA_PULL_TERMINAL_EPISODE_OBSERVED" in result["blockers"]
    assert result["cra_pull_recovery_state"] == "READY"


def test_soak_main_covers_config_check_collect_and_evaluate(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    dell_key = Ed25519PrivateKey.from_private_bytes(bytes(range(1, 33)))
    arena_key = Ed25519PrivateKey.from_private_bytes(bytes(range(33, 65)))
    config = _config(tmp_path, dell_key, arena_key)
    monkeypatch.setattr(soak_module.SoakConfig, "load", lambda _path: config)

    monkeypatch.setattr(sys, "argv", ["soak", "--config", "/config.json", "--check-config"])
    soak_module.main()
    assert json.loads(capsys.readouterr().out)["config"] == "VALID"

    _, arena = _status_pair(tmp_path / "main-status", dell_key, arena_key, now=CURRENT)
    sample = {"sample_sequence": 1, "observed_at": "2026-09-03T00:00:00Z", "arena_status": arena}
    monkeypatch.setattr(soak_module, "collect_sample", lambda _config: sample)
    monkeypatch.setattr(sys, "argv", ["soak", "--config", "/config.json"])
    soak_module.main()
    assert json.loads(capsys.readouterr().out)["arena_state"] == "READY"

    gate = {
        "schema": soak_module.GATE_SCHEMA,
        "status": "NOT_YET_ELIGIBLE",
        "eligible": False,
        "sample_count": 0,
        "duration_seconds": 0,
        "blockers": ["SAMPLE_COUNT_INSUFFICIENT"],
    }
    monkeypatch.setattr(
        soak_module,
        "evidence_snapshot",
        lambda _config: EvidenceSnapshot(tmp_path / "missing.jsonl", 1024, 0, None, None),
    )
    monkeypatch.setattr(soak_module, "evaluate_samples", lambda *_args, **_kwargs: gate)
    monkeypatch.setattr(soak_module, "add_evidence_capacity_assessment", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(sys, "argv", ["soak", "--config", "/config.json", "--evaluate"])
    soak_module.main()
    output = json.loads(capsys.readouterr().out)
    assert output == gate
    assert output["evaluation_elapsed_seconds"] >= 0
    assert json.loads(Path(str(config.value["gate_file"])).read_text(encoding="utf-8")) == output
