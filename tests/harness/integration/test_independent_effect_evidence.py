"""Real owner snapshot -> adapter -> signed collector -> replay across child loss."""

from __future__ import annotations

import copy
import hashlib
import json
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from cra_authority.recovery_safety import query_only
from cra_dell_recovery.canonical import Signer
from cra_dell_recovery.recovery_history import empty_policy, validate_effect_history
from cra_no_action_soak import recovery_soak
from cra_no_action_soak.recovery_facts import runtime_evidence_fact, sign_facts
from cra_no_action_soak.recovery_soak import Config
from runtime_boundary import recovery_publisher as owner_module
from runtime_boundary.recovery_evidence import effect_safety
from tests.harness.integration.test_postmortem_io_faults import BAD_JSON
from tests.harness.unit.test_recovery_soak import START, packet
from tests.harness.unit.test_recovery_soak import setup as setup
from tests.runtime_boundary.test_recovery_effect_history import history as history
from tests.runtime_boundary.test_recovery_publisher import publisher as publisher


def independent(owner: Any) -> None:
    owner.config["evidence_schema"] = "runtime.recovery_evidence.v2"


@pytest.mark.parametrize("fault", BAD_JSON)
def test_owner_input_uses_bounded_parser(publisher: Any, fault: str) -> None:
    owner, _, _ = publisher
    owner.target_path.write_bytes(BAD_JSON[fault])
    with pytest.raises(ValueError):
        owner_module.owned_json(owner.target_path, owner_uid=owner.owner_uid)


def test_poison_target_does_not_suppress_independent_global_ledger(publisher: Any) -> None:
    owner, _, _ = publisher
    independent(owner)
    owner.publish()
    owner.target_path.write_bytes(BAD_JSON["recursion"])
    result = owner.publish()
    assert result["effects"]["integrity"] == "ok"
    assert result["target_sha256"] is None
    assert result["effects"]["current_target_unresolved_scope_count"] is None
    assert all(value is None for value in result["activation"].values())


@pytest.mark.parametrize("racing_read", [1, 2])
@pytest.mark.parametrize("schema", ["runtime.recovery_evidence.v1", "runtime.recovery_evidence.v2"])
def test_atomic_target_refresh_during_read_is_not_a_future_target(
    publisher: Any, monkeypatch: pytest.MonkeyPatch, racing_read: int, schema: str
) -> None:
    owner, _, _ = publisher
    owner.config["evidence_schema"] = schema
    clock = [datetime.now(UTC)]

    class Clock:
        @staticmethod
        def now(tz: Any = None) -> datetime:
            return clock[0]

    real_read = owner_module.owned_json
    reads = 0

    def refreshed(path: Path, **kwargs: Any) -> dict[str, Any]:
        nonlocal reads
        raw = real_read(path, **kwargs)
        if path == owner.target_path:
            reads += 1
            if reads == racing_read:
                # The admitted producer atomically publishes while the reader
                # is acquiring the file. Its timestamp is past at read-end.
                clock[0] += timedelta(milliseconds=74)
                raw["observed_at"] = clock[0].isoformat()
                raw["valid_until"] = (clock[0] + timedelta(seconds=20)).isoformat()
                owner.target_path.write_text(json.dumps(raw))
        return raw

    monkeypatch.setattr(owner_module, "datetime", Clock)
    monkeypatch.setattr(owner_module, "owned_json", refreshed)
    result = owner.publish()
    assert result["target_sha256"] is not None
    assert all(value is True for value in result["activation"].values())
    if racing_read == 1:
        # The public evidence schema serializes millisecond precision.
        assert datetime.fromisoformat(result["observed_at"]) == clock[0].replace(microsecond=clock[0].microsecond // 1000 * 1000)


@pytest.mark.parametrize("racing_read", [1, 2])
@pytest.mark.parametrize("fault", ["future", "expires-during-read", "clock-regression"])
@pytest.mark.parametrize("schema", ["runtime.recovery_evidence.v1", "runtime.recovery_evidence.v2"])
def test_read_completion_clock_does_not_admit_invalid_evidence(
    publisher: Any, monkeypatch: pytest.MonkeyPatch, racing_read: int, fault: str, schema: str
) -> None:
    owner, _, _ = publisher
    owner.config["evidence_schema"] = schema
    owner.publish()
    before = owner.path.read_bytes()
    clock = [datetime.now(UTC)]

    class Clock:
        @staticmethod
        def now(tz: Any = None) -> datetime:
            return clock[0]

    real_read = owner_module.owned_json
    reads = 0

    def invalidated(path: Path, **kwargs: Any) -> dict[str, Any]:
        nonlocal reads
        raw = real_read(path, **kwargs)
        reads += 1
        if reads == racing_read:
            if fault == "future":
                raw["observed_at"] = (clock[0] + timedelta(milliseconds=74)).isoformat()
            elif fault == "expires-during-read":
                raw["valid_until"] = (clock[0] + timedelta(milliseconds=50)).isoformat()
                clock[0] += timedelta(milliseconds=74)
            else:
                clock[0] -= timedelta(milliseconds=74)
        return raw

    monkeypatch.setattr(owner_module, "datetime", Clock)
    monkeypatch.setattr(owner_module, "owned_json", invalidated)
    if fault == "clock-regression" or schema.endswith("v1"):
        with pytest.raises(ValueError):
            owner.publish()
        assert owner.path.read_bytes() == before
    else:
        result = owner.publish()
        assert result["target_sha256"] is None
        assert result["effects"]["integrity"] == "ok"
        assert all(value is None for value in result["activation"].values())


@pytest.mark.parametrize("stage", ["process", "snapshot"])
def test_wall_clock_regression_invalidates_global_snapshot(publisher: Any, monkeypatch: pytest.MonkeyPatch, stage: str) -> None:
    owner, _, _ = publisher
    independent(owner)
    owner.publish()
    before = owner.path.read_bytes()
    clock = [datetime.now(UTC)]

    class Clock:
        @staticmethod
        def now(tz: Any = None) -> datetime:
            return clock[0]

    monkeypatch.setattr(owner_module, "datetime", Clock)
    if stage == "process":
        original = owner.process_supplier

        def process() -> Any:
            clock[0] -= timedelta(milliseconds=74)
            return original()

        owner.process_supplier = process
    else:
        real = owner_module.owner_evidence

        def snapshot(db: sqlite3.Connection, **kwargs: Any) -> Any:
            result = real(db, **kwargs)
            clock[0] -= timedelta(milliseconds=74)
            return result

        monkeypatch.setattr(owner_module, "owner_evidence", snapshot)
    with pytest.raises(ValueError, match="CLOCK_REGRESSION"):
        owner.publish()
    assert owner.path.read_bytes() == before


def test_negative_control_detects_read_start_clock(publisher: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(owner_module, "_read_clock", lambda not_before: not_before)
    with pytest.raises(AssertionError):
        test_atomic_target_refresh_during_read_is_not_a_future_target(publisher, monkeypatch, 1, "runtime.recovery_evidence.v2")


def test_negative_control_detects_owner_parser_regression(publisher: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(owner_module, "load_object", lambda raw, **kwargs: json.loads(raw))
    with pytest.raises(pytest.fail.Exception):
        test_owner_input_uses_bounded_parser(publisher, "overflow")


@pytest.mark.parametrize("fault", ["missing-target", "stale-target", "missing-child", "zombie", "two-children", "wrong-boot"])
def test_target_failure_preserves_fresh_global_ledger_without_claiming_activation(publisher: Any, fault: str) -> None:
    owner, process, _ = publisher
    independent(owner)
    original = owner.publish()
    raw = json.loads(owner.target_path.read_text())
    if fault == "missing-target":
        owner.target_path.unlink()
    elif fault == "stale-target":
        raw["valid_until"] = (datetime.now(UTC) - timedelta(seconds=1)).isoformat()
        owner.target_path.write_text(json.dumps(raw))
    elif fault == "missing-child":
        process["ffmpeg_running"] = False
    elif fault == "two-children":
        process["managed_child_cardinality"] = "2+"
    elif fault == "wrong-boot":
        raw["target_identity"]["host_boot_id"] = "other-boot"
        owner.target_path.write_text(json.dumps(raw))
    else:
        path = owner.proc_root / "123/stat"
        path.write_text(path.read_text().replace(") S ", ") Z "))
    changes = owner.ledger.connection.total_changes
    result = owner.publish()
    assert result["observed_at"] > original["observed_at"]
    assert result["host_boot_id"] == "boot-id"
    assert result["effects"]["physical_attempt_count"] == 0
    assert result["effects"]["integrity"] == "ok"
    assert result["target_sha256"] is None
    assert result["effects"]["target_status"] == "UNKNOWN"
    assert result["effects"]["current_target_unresolved_scope_count"] is None
    assert all(value is None for value in result["activation"].values())
    assert owner.ledger.connection.total_changes == changes
    # Reverting the target/ledger coupling must be detected by this fixture.
    owner.config.pop("evidence_schema")
    with pytest.raises((OSError, ValueError)):
        owner.publish()


def test_transition_during_snapshot_drops_only_target_claims(publisher: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    owner, process, _ = publisher
    independent(owner)
    real = owner_module.owner_evidence

    def transition(db: sqlite3.Connection, **kwargs: Any) -> Any:
        result = real(db, **kwargs)
        process["ffmpeg_running"] = False
        return result

    monkeypatch.setattr(owner_module, "owner_evidence", transition)
    result = owner.publish()
    assert result["effects"]["integrity"] == "ok"
    assert result["effects"]["target_status"] == "UNKNOWN"
    assert result["target_sha256"] is None
    assert all(value is None for value in result["activation"].values())


def test_independent_db_failure_does_not_refresh_previous_export(publisher: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    owner, process, _ = publisher
    independent(owner)
    owner.publish()
    before = owner.path.read_bytes()
    process["ffmpeg_running"] = False

    def unavailable(*args: Any, **kwargs: Any) -> Any:
        raise sqlite3.OperationalError("injected snapshot read failure")

    monkeypatch.setattr(owner_module.sqlite3, "connect", unavailable)
    owner.publish_if_due()
    assert owner.path.read_bytes() == before
    assert owner.last_error_code == "RECOVERY_OWNER_EXPORT_UNAVAILABLE"


@pytest.mark.parametrize("fault", ["boot", "source", "stale", "zero-current", "activation", "schema"])
def test_independent_adapter_rejects_false_identity_or_unknown_substitution(publisher: Any, setup: Any, fault: str) -> None:
    owner, process, _ = publisher
    config, _ = setup
    independent(owner)
    owner.config.update(host_id=config.bindings["dell"].host_id, stream_id=config.bindings["dell"].stream_id)
    process["ffmpeg_running"] = False
    raw = owner.publish()
    binding = {k: owner.config[k] for k in ("release_id", "source_commit", "target_host_id", "evidence_schema")}
    if fault == "boot":
        raw["host_boot_id"] = "other-boot"
    elif fault == "source":
        raw["source_commit"] = "b" * 40
    elif fault == "stale":
        raw["observed_at"] = (datetime.now(UTC) - timedelta(seconds=46)).isoformat()
    elif fault == "zero-current":
        raw["effects"]["current_target_unresolved_scope_count"] = 0
    elif fault == "activation":
        raw["activation"]["rw_timeout_enabled"] = True
    else:
        binding.pop("evidence_schema")
    with pytest.raises(ValueError):
        runtime_evidence_fact(
            raw,
            target={},
            binding=config.bindings["dell"],
            runtime_binding=binding,
            now=datetime.now(UTC),
            physical_boot_id="boot-id",
            history_policy=empty_policy(),
        )


@pytest.mark.parametrize("outage", ["delivery", "network"])
@pytest.mark.parametrize(
    "violation",
    [
        "none",
        "attempt",
        "missing-ledger",
        "missing-onset",
        "schema-downgrade",
        "unknown-outside-outage",
        "activation-missing",
        "retained-target-onset",
        "invalid-target-status",
    ],
)
def test_signed_owner_to_collector_child_transition(
    publisher: Any,
    setup: tuple[Config, dict[str, Signer]],
    monkeypatch: pytest.MonkeyPatch,
    outage: str,
    violation: str,
) -> None:
    owner, process, _ = publisher
    config, signers = setup
    config.value["require_independent_effect_evidence"] = True
    independent(owner)
    owner.config.update(host_id=config.bindings["dell"].host_id, stream_id=config.bindings["dell"].stream_id)
    binding = {k: owner.config[k] for k in ("release_id", "source_commit", "target_host_id", "evidence_schema")}
    clock = [START]

    class Clock:
        @staticmethod
        def now(tz: Any = None) -> datetime:
            return clock[0]

    monkeypatch.setattr(owner_module, "datetime", Clock)
    rows = []
    last_target = None
    for seconds in range(0, 181, 15):
        now = START + timedelta(seconds=seconds)
        clock[0] = now
        absent = 45 <= seconds <= 75
        process["ffmpeg_running"] = not absent
        target = json.loads(owner.target_path.read_text())
        if seconds == 90:
            child_stat = owner.proc_root / "123/stat"
            child_stat.write_text(child_stat.read_text().replace("99 0", "100 0"))
            target["target_identity"]["ffmpeg_generation"] = (
                "ffmpeg-" + hashlib.sha256(b"pod-id:containerd://test-child:7654321:100").hexdigest()[:32]
            )
            process["ffmpeg_generation"] = "replacement-native-generation"
        target.update(observed_at=now.isoformat(), valid_until=(now + timedelta(seconds=45)).isoformat())
        owner.target_path.write_text(json.dumps(target))
        raw = owner.publish()
        assert (raw["target_sha256"] is None) == absent, raw
        effects, activation = runtime_evidence_fact(
            raw,
            target=target if not absent else {},
            binding=config.bindings["dell"],
            runtime_binding=binding,
            now=now,
            physical_boot_id="boot-id",
            history_policy=empty_policy(),
        )
        assert effects["observed_at"] == raw["observed_at"]
        if absent:
            assert effects["physical_attempt_count"] == 0
            with pytest.raises(ValueError, match="CURRENT_TARGET_UNBOUND"):
                validate_effect_history(effects, policy=empty_policy(), target=None, now=now)
        original = packet(config, signers, "dell", seconds)
        facts = copy.deepcopy(original["facts"])
        facts["effects"], facts["activation"] = effects, activation
        facts["transport"]["target"] = raw["target_sha256"]
        if absent:
            facts["transport"]["bytes_acked"] = None
        if violation == "retained-target-onset" and seconds == 45:
            facts["transport"].update(target=last_target, bytes_acked=4000, observed_at=(START + timedelta(seconds=30)).isoformat())
        if raw["target_sha256"] is not None:
            last_target = raw["target_sha256"]
        if violation == "invalid-target-status" and seconds == 60:
            facts["effects"]["target_status"] = []
        if violation == "attempt" and seconds >= 60:
            facts["effects"]["physical_attempt_count"] = 1
        if (violation == "missing-ledger" and seconds == 60) or (violation == "missing-onset" and seconds == 45):
            facts["effects"] = {"observed_at": None, "integrity": "UNKNOWN"}
        if violation == "schema-downgrade" and seconds == 120:
            facts["effects"].pop("target_status")
        if violation == "activation-missing" and seconds >= 90:
            facts["activation"] = {"bounded_termination_enabled": None, "rw_timeout_enabled": None}
        if violation == "unknown-outside-outage" and seconds == 30:
            facts["effects"].update(target_status="UNKNOWN", current_target_sha256=None, current_target_unresolved_scope_count=None)
            facts["activation"] = {"bounded_termination_enabled": None, "rw_timeout_enabled": None}
        if outage == "network" and seconds == 45:
            facts["network"]["state"] = "DOWN"
        signed = sign_facts(
            binding=config.bindings["dell"],
            signer=signers["dell"],
            host_boot_id="boot-id",
            producer_id="dell-producer",
            sequence=seconds + 1,
            now=now,
            valid_until=now + timedelta(seconds=45),
            facts=facts,
            source_hashes={"runtime_evidence": recovery_soak.digest(raw)},
        )
        inputs = {r: packet(config, signers, r, seconds) for r in config.bindings}
        inputs["dell"] = signed
        if absent:
            inputs["arena"] = packet(config, signers, "arena", seconds, platform={"state": "DOWN"})
        for role, value in inputs.items():
            recovery_soak.atomic_write_json(Path(config.value["hosts"][role]["inbox_file"]), value)
        rows.append(recovery_soak.collect(config, now=now))
    result = recovery_soak.evaluate(rows, config=config, now=clock[0])
    assert not result["oracle_errors"], result
    if violation in ("none", "retained-target-onset"):
        assert result["harness_classification"] == "PASS", result
        assert result["recovery"]["recovered_episode_count"] == 1, result
        assert result["target_transitions"]["no_authority_effect"] == 1
        assert result["recovery"]["verified_network_recovered_episode_count"] == int(outage == "network")
    elif violation == "attempt":
        assert "NO_ACTION_PHYSICAL_EFFECT_DETECTED" in result["blockers"]
    elif violation == "invalid-target-status":
        assert "DELL_EFFECT_HISTORY_INVALID" in result["blockers"]
    elif violation == "activation-missing":
        assert result["recovery"]["recovered_episode_count"] == 0
        assert result["live_health"] == "RECOVERING"
        assert result["eligible"] is False
    else:
        assert result["harness_classification"] == "MISSING_EVIDENCE", result


def test_target_absence_does_not_resolve_or_hide_retired_history(history: Any) -> None:
    ledger, policy = history
    before = list(ledger.connection.iterdump())
    with query_only(ledger.path) as db:
        effects = effect_safety(
            db, now=START, allowed_producers=["owner"], target_identity=None, history_policy=policy, independent_effects=True
        )
    effects["target_status"] = "UNKNOWN"
    validate_effect_history(effects, policy=policy, target=None, now=START, allow_unknown_target=True)
    assert effects["unresolved_scope_count_total"] == effects["physical_attempt_count"] == 2
    assert effects["current_target_unresolved_scope_count"] is None
    assert effects["historical_retired_unknown"] == policy["retired_unknown"]
    assert list(ledger.connection.iterdump()) == before
    with pytest.raises(ValueError, match="CURRENT_TARGET_UNBOUND"):
        validate_effect_history(effects, policy=policy, target=None, now=START)
