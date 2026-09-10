from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from cra_dell_recovery.canonical import KeyRing, Signer
from cra_no_action_soak.resilient_host_status import (
    ComponentStatus,
    ExactTransitionBinding,
    ResilientHostStatusContract,
    build_resilient_status,
    record_publisher_failure,
    served_resilient_status_bytes,
)

ROOT = Path(__file__).resolve().parents[3]
SCHEMA = ROOT / "contracts/cra_no_action_soak/resilient_host_status.v3.schema.json"
IDENTITY = {
    "release_manifest_sha256": "1" * 64,
    "runtime_manifest_sha256": "2" * 64,
    "configuration_set_sha256": "3" * 64,
    "maintenance_restart_policy_sha256": "4" * 64,
}
CURRENT = datetime(2026, 9, 3, 0, 0, tzinfo=UTC)


def _component(name: str = "target_snapshot") -> ComponentStatus:
    return ComponentStatus(
        name,
        "FRESH",
        "SOURCE_FRESH",
        observed_at=CURRENT,
        valid_until=CURRENT + timedelta(seconds=20),
        identity_sha256="5" * 64,
    )


def _build(tmp_path: Path, key: Ed25519PrivateKey, **overrides: object) -> dict[str, object]:
    values: dict[str, object] = {
        "state_file": tmp_path / "state.json",
        "transition_journal": tmp_path / "transitions.jsonl",
        "schema_file": SCHEMA,
        "private_key": key,
        "key_id": "dell-resilient-key",
        "role": "dell",
        "host_id": "dell-stream-runtime",
        "release_id": "dell-observation-0123456789ab",
        "host_boot_id": "boot-one",
        "identity": IDENTITY,
        "components": [_component()],
        "source_observed_at": CURRENT,
        "source_valid_until": CURRENT + timedelta(seconds=20),
        "target_identity_sha256": "6" * 64,
        "source_payload_sha256": "7" * 64,
        "origin_host_id": "dell-stream-runtime",
        "physical_effect_count": 0,
        "now": CURRENT,
    }
    values.update(overrides)
    return build_resilient_status(**values)  # type: ignore[arg-type]


def _contract(key: Ed25519PrivateKey) -> ResilientHostStatusContract:
    return ResilientHostStatusContract(
        SCHEMA,
        KeyRing({"dell-resilient-key": key.public_key()}),
        key_id="dell-resilient-key",
        expected_role="dell",
        expected_host_id="dell-stream-runtime",
        expected_release_id="dell-observation-0123456789ab",
    )


def test_ready_status_is_signed_and_persists_monotonic_sequence(tmp_path: Path) -> None:
    key = Ed25519PrivateKey.from_private_bytes(bytes(range(1, 33)))

    first = _build(tmp_path, key)
    second = _build(tmp_path, key, now=CURRENT + timedelta(seconds=1))

    assert first["state"] == second["state"] == "READY"
    assert first["producer_sequence"] == 1
    assert second["producer_sequence"] == 2
    assert first["producer_instance_id"] == second["producer_instance_id"]
    assert first["last_good"]["diagnostic_only"] is True  # type: ignore[index]
    assert _contract(key).decode(second, now=CURRENT + timedelta(seconds=1)) == second


def test_component_value_stays_strict_but_report_degrades_future_component_and_source(tmp_path: Path) -> None:
    key = Ed25519PrivateKey.from_private_bytes(bytes(range(1, 33)))
    with pytest.raises(ValueError, match="COMPONENT_TIME_INVALID"):
        ComponentStatus(
            "target_snapshot",
            "FRESH",
            "SOURCE_FRESH",
            CURRENT + timedelta(milliseconds=1),
            CURRENT + timedelta(seconds=20),
            "5" * 64,
        ).value(now=CURRENT)

    future_component = _build(
        tmp_path / "component",
        key,
        components=[
            ComponentStatus(
                "target_snapshot",
                "FRESH",
                "SOURCE_FRESH",
                CURRENT + timedelta(milliseconds=1),
                CURRENT + timedelta(seconds=20),
                "5" * 64,
            )
        ],
    )
    future_source = _build(
        tmp_path / "source",
        key,
        source_observed_at=CURRENT + timedelta(milliseconds=1),
    )

    assert future_component["state"] == "DEGRADED_SOURCE"
    assert future_component["reason_codes"] == ["TARGET_SNAPSHOT_OBSERVED_IN_FUTURE"]
    assert future_component["components"][0]["state"] == "INVALID"  # type: ignore[index]
    assert future_source["state"] == "DEGRADED_SOURCE"
    assert future_source["reason_codes"] == ["SOURCE_OBSERVED_IN_FUTURE"]
    assert future_source["current"] == {
        "source_observed_at": None,
        "source_valid_until": None,
        "target_identity_sha256": None,
        "source_payload_sha256": None,
        "origin_host_id": None,
    }
    assert future_source["transition"]["open_episode_count"] == 0  # type: ignore[index]


def test_nested_future_source_is_signed_as_component_specific_degradation(tmp_path: Path) -> None:
    dell_key = Ed25519PrivateKey.from_private_bytes(bytes(range(1, 33)))
    arena_key = Ed25519PrivateKey.from_private_bytes(bytes(range(33, 65)))
    future = CURRENT + timedelta(milliseconds=500)
    dell = _build(tmp_path / "dell", dell_key, now=future)

    arena = build_resilient_status(
        state_file=tmp_path / "arena-state.json",
        transition_journal=tmp_path / "arena-transitions.jsonl",
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
                future,
                future + timedelta(seconds=20),
                str(dell["payload_sha256"]),
            )
        ],
        source_observed_at=future,
        source_valid_until=future + timedelta(seconds=20),
        target_identity_sha256="6" * 64,
        source_payload_sha256="7" * 64,
        origin_host_id="dell-stream-runtime",
        physical_effect_count=0,
        upstream_status=dell,
        track_target_transitions=False,
        now=CURRENT,
    )

    assert arena["state"] == "DEGRADED_SOURCE"
    assert arena["reason_codes"] == [
        "DELL_SIGNED_STATUS_OBSERVED_IN_FUTURE",
        "SOURCE_OBSERVED_IN_FUTURE",
    ]
    assert arena["components"][0]["state"] == "INVALID"  # type: ignore[index]


def test_report_lease_is_clamped_to_earliest_current_evidence_deadline(tmp_path: Path) -> None:
    key = Ed25519PrivateKey.from_private_bytes(bytes(range(1, 33)))

    status = _build(
        tmp_path,
        key,
        reporter_lease_seconds=45,
        components=[
            ComponentStatus(
                "target_snapshot",
                "FRESH",
                "SOURCE_FRESH",
                CURRENT,
                CURRENT + timedelta(seconds=12),
                "5" * 64,
            )
        ],
        source_valid_until=CURRENT + timedelta(seconds=10),
    )

    assert status["report_lease_until"] == "2026-09-03T00:00:10.000Z"
    assert _contract(key).decode(status, now=CURRENT + timedelta(seconds=9)) == status
    with pytest.raises(ValueError, match="RESILIENT_STATUS_REPORT_EXPIRED"):
        _contract(key).decode(status, now=CURRENT + timedelta(seconds=11))


def test_contract_rejects_report_lease_that_outlives_fresh_component(tmp_path: Path) -> None:
    key = Ed25519PrivateKey.from_private_bytes(bytes(range(1, 33)))
    status = _build(tmp_path, key)
    status["report_lease_until"] = "2026-09-03T00:00:30.000Z"
    unsigned = {name: value for name, value in status.items() if name not in {"payload_sha256", "signature"}}
    tampered = Signer("dell-resilient-key", key).sign(unsigned)

    with pytest.raises(ValueError, match="REPORT_EXCEEDS_EVIDENCE_VALIDITY"):
        _contract(key).decode(tampered, now=CURRENT)


def test_failed_publisher_invocation_is_exposed_by_the_next_signed_health_history(
    tmp_path: Path,
) -> None:
    key = Ed25519PrivateKey.from_private_bytes(bytes(range(1, 33)))
    first = _build(tmp_path, key)
    record_publisher_failure(
        state_file=tmp_path / "state.json",
        host_id="dell-stream-runtime",
        host_boot_id="boot-one",
        now=CURRENT + timedelta(seconds=1),
    )
    recovered = _build(tmp_path, key, now=CURRENT + timedelta(seconds=2))

    assert first["producer_sequence"] == 1
    assert recovered["producer_sequence"] == 3
    events = recovered["health_transition"]["event_tail"]  # type: ignore[index]
    assert [event["reason_codes"] for event in events] == [
        ["ALL_REQUIRED_SOURCES_FRESH"],
        ["PUBLISHER_INVOCATION_FAILED"],
        ["ALL_REQUIRED_SOURCES_FRESH"],
    ]
    assert _contract(key).decode(recovered, now=CURRENT + timedelta(seconds=2)) == recovered


def test_degraded_status_advances_and_retains_explicitly_diagnostic_last_good(tmp_path: Path) -> None:
    key = Ed25519PrivateKey.from_private_bytes(bytes(range(1, 33)))
    ready = _build(tmp_path, key)
    degraded = _build(
        tmp_path,
        key,
        components=[ComponentStatus("target_snapshot", "ERROR", "K3S_API_UNAVAILABLE")],
        source_observed_at=None,
        source_valid_until=None,
        target_identity_sha256=None,
        source_payload_sha256=None,
        origin_host_id=None,
        physical_effect_count=None,
        now=CURRENT + timedelta(seconds=5),
    )

    assert ready["state"] == "READY"
    assert degraded["state"] == "RECOVERING"
    assert degraded["reason_codes"] == ["K3S_API_UNAVAILABLE", "TARGET_TRANSITION_UNBOUND"]
    assert degraded["producer_sequence"] == 2
    assert degraded["last_good"]["present"] is True  # type: ignore[index]
    assert degraded["last_good"]["diagnostic_only"] is True  # type: ignore[index]
    assert degraded["last_good"]["target_identity_sha256"] == "6" * 64  # type: ignore[index]
    assert degraded["current"]["target_identity_sha256"] is None  # type: ignore[index]
    assert _contract(key).decode(degraded, now=CURRENT + timedelta(seconds=5)) == degraded


def test_unbound_target_change_is_hash_chained_and_stays_open(tmp_path: Path) -> None:
    key = Ed25519PrivateKey.from_private_bytes(bytes(range(1, 33)))
    _build(tmp_path, key)

    changed = _build(
        tmp_path,
        key,
        target_identity_sha256="8" * 64,
        now=CURRENT + timedelta(seconds=2),
    )

    assert changed["state"] == "RECOVERING"
    assert changed["transition"]["high_watermark"] == 1  # type: ignore[index]
    assert changed["transition"]["causal_state"] == "UNBOUND"  # type: ignore[index]
    episode = changed["transition"]["open_episodes"][0]  # type: ignore[index]
    assert episode["before_target_identity_sha256"] == "6" * 64
    assert episode["after_target_identity_sha256"] == "8" * 64
    event = json.loads((tmp_path / "transitions.jsonl").read_text(encoding="utf-8"))
    assert event["sequence"] == 1
    assert event["previous_event_hash"] is None
    assert event["event_hash"] == changed["transition"]["last_event_hash"]  # type: ignore[index]


def test_exact_transition_binding_does_not_create_unbound_episode(tmp_path: Path) -> None:
    key = Ed25519PrivateKey.from_private_bytes(bytes(range(1, 33)))
    _build(tmp_path, key)
    changed = _build(
        tmp_path,
        key,
        target_identity_sha256="8" * 64,
        exact_transition_binding=ExactTransitionBinding("request-one", "scope-one", "6" * 64, "8" * 64),
        now=CURRENT + timedelta(seconds=2),
    )

    assert changed["state"] == "READY"
    assert changed["transition"]["causal_state"] == "EXACT"  # type: ignore[index]
    assert changed["transition"]["open_episodes"] == []  # type: ignore[index]
    event = json.loads((tmp_path / "transitions.jsonl").read_text(encoding="utf-8"))
    assert event["episode"]["effect_request_id"] == "request-one"
    assert event["episode"]["effect_scope_id"] == "scope-one"


def test_new_boot_creates_new_instance_without_sequence_regression_ambiguity(tmp_path: Path) -> None:
    key = Ed25519PrivateKey.from_private_bytes(bytes(range(1, 33)))
    first = _build(tmp_path, key)
    after_boot = _build(tmp_path, key, host_boot_id="boot-two", now=CURRENT + timedelta(seconds=2))

    assert after_boot["producer_sequence"] == 1
    assert after_boot["producer_instance_id"] != first["producer_instance_id"]
    assert after_boot["host_boot_id"] == "boot-two"
    assert after_boot["health_transition"]["journal_id"] != first["health_transition"]["journal_id"]  # type: ignore[index]
    assert after_boot["health_transition"]["high_watermark"] == 1  # type: ignore[index]
    assert [event["producer_sequence"] for event in after_boot["health_transition"]["event_tail"]] == [1]  # type: ignore[index]
    assert _contract(key).decode(after_boot, now=CURRENT + timedelta(seconds=2)) == after_boot


def test_last_good_expires_without_becoming_current(tmp_path: Path) -> None:
    key = Ed25519PrivateKey.from_private_bytes(bytes(range(1, 33)))
    _build(tmp_path, key, last_good_retention_seconds=60)
    degraded = _build(
        tmp_path,
        key,
        components=[ComponentStatus("target_snapshot", "MISSING", "TARGET_SNAPSHOT_MISSING")],
        source_observed_at=None,
        source_valid_until=None,
        target_identity_sha256=None,
        source_payload_sha256=None,
        origin_host_id=None,
        physical_effect_count=None,
        last_good_retention_seconds=60,
        now=CURRENT + timedelta(seconds=61),
    )

    assert degraded["last_good"]["present"] is False  # type: ignore[index]
    assert degraded["current"]["target_identity_sha256"] is None  # type: ignore[index]


def test_contract_rejects_expired_report_but_not_degraded_source(tmp_path: Path) -> None:
    key = Ed25519PrivateKey.from_private_bytes(bytes(range(1, 33)))
    status = _build(
        tmp_path,
        key,
        components=[ComponentStatus("effect_evidence", "ERROR", "EFFECT_EVIDENCE_UNAVAILABLE")],
        source_observed_at=None,
        source_valid_until=None,
        target_identity_sha256=None,
        source_payload_sha256=None,
        origin_host_id=None,
        physical_effect_count=None,
    )

    assert _contract(key).decode(status, now=CURRENT)["state"] == "DEGRADED_SOURCE"
    with pytest.raises(ValueError, match="RESILIENT_STATUS_REPORT_EXPIRED"):
        _contract(key).decode(status, now=CURRENT + timedelta(seconds=46))


def test_resource_only_failure_is_signed_as_degraded_observability_with_exact_health_history(
    tmp_path: Path,
) -> None:
    key = Ed25519PrivateKey.from_private_bytes(bytes(range(1, 33)))
    ready = _build(tmp_path, key)
    degraded = _build(
        tmp_path,
        key,
        components=[ComponentStatus("publisher_resources", "ERROR", "SERVER_RESOURCE_STATUS_STALE")],
        now=CURRENT + timedelta(seconds=1),
    )
    recovered = _build(tmp_path, key, now=CURRENT + timedelta(seconds=2))

    assert ready["health_transition"]["high_watermark"] == 1  # type: ignore[index]
    assert degraded["state"] == "DEGRADED_OBSERVABILITY"
    assert recovered["state"] == "READY"
    assert recovered["health_transition"]["high_watermark"] == 3  # type: ignore[index]
    events = recovered["health_transition"]["event_tail"]  # type: ignore[index]
    assert [event["state"] for event in events] == ["READY", "DEGRADED_OBSERVABILITY", "READY"]
    assert [event["producer_sequence"] for event in events] == [1, 2, 3]
    assert _contract(key).decode(recovered, now=CURRENT + timedelta(seconds=2))["state"] == "READY"


def test_component_freshness_and_duplicate_names_fail_closed(tmp_path: Path) -> None:
    key = Ed25519PrivateKey.from_private_bytes(bytes(range(1, 33)))
    with pytest.raises(ValueError, match="COMPONENT_FRESH_EXPIRED"):
        _build(
            tmp_path,
            key,
            components=[
                ComponentStatus(
                    "target_snapshot",
                    "FRESH",
                    "SOURCE_FRESH",
                    observed_at=CURRENT - timedelta(seconds=20),
                    valid_until=CURRENT,
                )
            ],
        )
    with pytest.raises(ValueError, match="COMPONENT_SET_INVALID"):
        _build(tmp_path, key, components=[_component(), _component()])


def test_late_exact_binding_closes_matching_episode_and_appends_resolution(tmp_path: Path) -> None:
    key = Ed25519PrivateKey.from_private_bytes(bytes(range(1, 33)))
    _build(tmp_path, key)
    _build(tmp_path, key, target_identity_sha256="8" * 64, now=CURRENT + timedelta(seconds=1))

    resolved = _build(
        tmp_path,
        key,
        target_identity_sha256="8" * 64,
        exact_transition_binding=ExactTransitionBinding("request-one", "scope-one", "6" * 64, "8" * 64),
        now=CURRENT + timedelta(seconds=2),
    )

    assert resolved["state"] == "READY"
    assert resolved["transition"]["causal_state"] == "EXACT"  # type: ignore[index]
    assert resolved["transition"]["open_episode_count"] == 0  # type: ignore[index]
    events = [json.loads(line) for line in (tmp_path / "transitions.jsonl").read_text(encoding="utf-8").splitlines()]
    assert [event["event_action"] for event in events] == ["OBSERVED", "CAUSAL_BOUND"]
    assert events[1]["previous_event_hash"] == events[0]["event_hash"]


def test_multiple_unbound_transitions_are_not_collapsed(tmp_path: Path) -> None:
    key = Ed25519PrivateKey.from_private_bytes(bytes(range(1, 33)))
    _build(tmp_path, key)
    _build(tmp_path, key, target_identity_sha256="8" * 64, now=CURRENT + timedelta(seconds=1))
    changed_again = _build(tmp_path, key, target_identity_sha256="9" * 64, now=CURRENT + timedelta(seconds=2))

    assert changed_again["state"] == "RECOVERING"
    assert changed_again["transition"]["high_watermark"] == 2  # type: ignore[index]
    assert changed_again["transition"]["open_episode_count"] == 2  # type: ignore[index]
    episodes = changed_again["transition"]["open_episodes"]  # type: ignore[index]
    assert [(item["before_target_identity_sha256"], item["after_target_identity_sha256"]) for item in episodes] == [
        ("6" * 64, "8" * 64),
        ("8" * 64, "9" * 64),
    ]


def test_journal_ahead_of_state_is_replayed_after_crash(tmp_path: Path) -> None:
    key = Ed25519PrivateKey.from_private_bytes(bytes(range(1, 33)))
    _build(tmp_path, key)
    state_before = (tmp_path / "state.json").read_bytes()
    changed = _build(tmp_path, key, target_identity_sha256="8" * 64, now=CURRENT + timedelta(seconds=1))
    assert changed["transition"]["high_watermark"] == 1  # type: ignore[index]
    (tmp_path / "state.json").write_bytes(state_before)

    replayed = _build(tmp_path, key, target_identity_sha256="8" * 64, now=CURRENT + timedelta(seconds=2))

    assert replayed["transition"]["high_watermark"] == 1  # type: ignore[index]
    assert replayed["transition"]["open_episode_count"] == 1  # type: ignore[index]
    assert len((tmp_path / "transitions.jsonl").read_text(encoding="utf-8").splitlines()) == 1


def test_boot_change_preserves_transition_journal_and_opens_new_producer_instance(tmp_path: Path) -> None:
    key = Ed25519PrivateKey.from_private_bytes(bytes(range(1, 33)))
    first = _build(tmp_path, key)
    _build(tmp_path, key, target_identity_sha256="8" * 64, now=CURRENT + timedelta(seconds=1))

    after_boot = _build(
        tmp_path,
        key,
        host_boot_id="boot-two",
        target_identity_sha256="8" * 64,
        now=CURRENT + timedelta(seconds=2),
    )

    assert after_boot["producer_instance_id"] != first["producer_instance_id"]
    assert after_boot["producer_sequence"] == 1
    assert after_boot["transition"]["high_watermark"] == 1  # type: ignore[index]
    assert after_boot["transition"]["open_episode_count"] == 1  # type: ignore[index]


def test_partial_or_forked_journal_is_rejected(tmp_path: Path) -> None:
    key = Ed25519PrivateKey.from_private_bytes(bytes(range(1, 33)))
    _build(tmp_path, key)
    _build(tmp_path, key, target_identity_sha256="8" * 64, now=CURRENT + timedelta(seconds=1))
    journal = tmp_path / "transitions.jsonl"
    journal.write_bytes(journal.read_bytes()[:-1])

    with pytest.raises(ValueError, match="JOURNAL_PARTIAL_TAIL"):
        _build(tmp_path, key, target_identity_sha256="8" * 64, now=CURRENT + timedelta(seconds=2))


def test_server_keeps_fresh_degraded_report_available_but_rejects_expired_reporter_lease(tmp_path: Path) -> None:
    key = Ed25519PrivateKey.from_private_bytes(bytes(range(1, 33)))
    status = _build(
        tmp_path,
        key,
        components=[ComponentStatus("target_snapshot", "ERROR", "K3S_API_UNAVAILABLE")],
        source_observed_at=None,
        source_valid_until=None,
        target_identity_sha256=None,
        source_payload_sha256=None,
        origin_host_id=None,
        physical_effect_count=None,
    )
    path = tmp_path / "served.json"
    path.write_text(json.dumps(status) + "\n", encoding="utf-8")
    path.chmod(0o600)

    assert (
        json.loads(
            served_resilient_status_bytes(
                path,
                expected_role="dell",
                expected_host_id="dell-stream-runtime",
                expected_release_id="dell-observation-0123456789ab",
                maximum_bytes=2 * 1024 * 1024,
                now=CURRENT,
            )
        )["state"]
        == "DEGRADED_SOURCE"
    )
    with pytest.raises(ValueError, match="REPORT_EXPIRED"):
        served_resilient_status_bytes(
            path,
            expected_role="dell",
            expected_host_id="dell-stream-runtime",
            expected_release_id="dell-observation-0123456789ab",
            maximum_bytes=2 * 1024 * 1024,
            now=CURRENT + timedelta(seconds=46),
        )
