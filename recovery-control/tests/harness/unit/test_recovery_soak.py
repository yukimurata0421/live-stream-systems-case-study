from __future__ import annotations

import copy
import hashlib
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from cra_dell_recovery.canonical import KeyRing, Signer
from cra_dell_recovery.errors import SignatureValidationError
from cra_dell_recovery.recovery_history import POLICY_SCHEMA, empty_policy, history_hash
from cra_dell_recovery.time import isoformat_utc
from cra_no_action_soak import recovery_soak
from cra_no_action_soak.append_only_jsonl import _serialized_line
from cra_no_action_soak.host_status import atomic_write_json
from cra_no_action_soak.recovery_facts import ACTION_TABLES, ROLES, AdmissionCache, HostBinding, admit_facts, sign_facts, source_hash
from cra_no_action_soak.recovery_live import advance
from cra_no_action_soak.recovery_soak import (
    CONFIG_SCHEMA,
    HISTORY_CONFIG_SCHEMA,
    SAMPLE_SCHEMA,
    Config,
    collect,
    digest,
    evaluate,
    gate,
    watchdog,
)
from cra_no_action_soak.recovery_window import RecoveryWindow

START = datetime(2026, 9, 4, tzinfo=UTC)
ROOT = Path(__file__).resolve().parents[3]


@pytest.fixture
def setup(tmp_path: Path) -> tuple[Config, dict[str, Signer]]:
    keys = {r: Ed25519PrivateKey.generate() for r in ROLES}
    bindings = {r: HostBinding(r, r + "-host", r + "-release", "1" * 40, "stream-1", r + "-key") for r in ROLES}
    hosts = {r: {k: v for k, v in b.__dict__.items() if k != "role"} for r, b in bindings.items()}
    for r in ROLES:
        hosts[r].update(inbox_file=str(tmp_path / (r + ".json")), public_key_file=str(tmp_path / (r + ".pem")))
        Path(hosts[r]["public_key_file"]).write_bytes(
            keys[r].public_key().public_bytes(serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo)
        )
        Path(hosts[r]["public_key_file"]).chmod(0o644)
    # Short duration is an internal unit-test fixture only. Config.load() for
    # the actual CLI accepts exactly seven days, never this test value.
    value = {
        "schema": CONFIG_SCHEMA,
        "epoch_id": "new-recovery-epoch",
        "hosts": hosts,
        "evidence_file": str(tmp_path / "samples.jsonl"),
        "state_file": str(tmp_path / "state.json"),
        "gate_file": str(tmp_path / "gate.json"),
        "maximum_evidence_bytes": 2 * 1024**3,
        "minimum_duration_seconds": 120,
        "maximum_sample_gap_seconds": 45,
        "recovery_deadline_seconds": 600,
        "effect_history_policy": empty_policy(),
    }
    return Config(value, bindings, KeyRing({r + "-key": k.public_key() for r, k in keys.items()})), {
        r: Signer(r + "-key", k) for r, k in keys.items()
    }


def packet(config: Config, signers: dict[str, Signer], role: str, seconds: int, **changes: Any) -> dict[str, Any]:
    now = START + timedelta(seconds=seconds)
    at = isoformat_utc(now)
    facts: dict[str, Any] = {
        "dell": {
            "network": {
                "state": "UP",
                "observed_at": at,
                **({"episode": None} if config.value.get("require_network_episode_evidence") is True else {}),
            },
            "transport": {
                "target": "target-1",
                "bytes_acked": 1000 + seconds * 100,
                "observed_at": at,
                **(
                    {
                        "rtmps_episode": {
                            "schema": "stream_v3.rtmps_episode_snapshot.v2",
                            "collector_id": "collector-1",
                            "release_id": "collector-release-1",
                            "epoch_id": "collector-release-1",
                            "observed_at": at,
                            "collector_sequence": seconds + 1,
                            "source_status": "VALID",
                            "source_observed_at": at,
                            "reason_code": "SOURCE_VALID",
                            "episode_sequence": 0,
                            "episode": None,
                        }
                    }
                    if config.value.get("require_rtmps_episode_evidence") is True
                    else {}
                ),
            },
            "effects": {
                "observed_at": at,
                "integrity": "ok",
                "physical_attempt_count": 0,
                "duplicate_attempt_count": 0,
                "unauthorized_effect_count": 0,
                "unresolved_scope_count": 0,
                "last_reconciliation": {},
            },
            "activation": {"bounded_termination_enabled": True, "rw_timeout_enabled": True},
        },
        "arena": {
            "network": {"state": "UP", "observed_at": at},
            "platform": {"state": "UP", "observed_at": at, "stream_id": "stream-1"},
            "control_path": {"state": "UP", "observed_at": at},
        },
        "cra": {
            "safety": {
                "observed_at": at,
                "runtime_observed_at": at,
                "integrity": "ok",
                "runtime_readiness": "NO_ACTION_READY",
                "runtime_operating_mode": "NO_ACTION",
                "command_delivery_enabled": False,
                "control_capability_count": 0,
                "action_table_counts": dict.fromkeys(ACTION_TABLES, 0),
            }
        },
        "raspi": {"publication": {"observed_at": at, "status": "READY", "reason_codes": [], "control_input_allowed": False}},
    }[role]
    for key, values in changes.items():
        facts[key].update(values)
    if role == "dell":
        policy = config.value.get("effect_history_policy", empty_policy())
        effects = facts["effects"]
        effects.update(
            historical_retired_unknown=copy.deepcopy(policy["retired_unknown"]),
            history_policy_sha256=history_hash(policy),
            unresolved_scope_count_total=effects["unresolved_scope_count"] + len(policy["retired_unknown"]),
            current_target_unresolved_scope_count=effects["unresolved_scope_count"],
            current_target_sha256=facts["transport"]["target"],
        )
        effects["physical_attempt_count"] += len(policy["retired_unknown"])
    return sign_facts(
        binding=config.bindings[role],
        signer=signers[role],
        host_boot_id=role + "-boot",
        producer_id=role + "-producer",
        sequence=seconds + 1,
        now=now,
        valid_until=now + timedelta(seconds=45),
        facts=facts,
        source_hashes={"source": "a" * 64},
    )


def test_admission_cache_rechecks_lease_and_never_hides_mutation(
    setup: tuple[Config, dict[str, Signer]], monkeypatch: pytest.MonkeyPatch,
) -> None:
    config, signers = setup
    signed = packet(config, signers, "arena", 0)
    cache = AdmissionCache()
    calls = 0
    verify = config.keys.verify

    def counted(value: dict[str, Any]) -> None:
        nonlocal calls
        calls += 1
        verify(value)

    monkeypatch.setattr(config.keys, "verify", counted)
    assert cache.admit(signed, binding=config.bindings["arena"], keys=config.keys, now=START) == signed
    assert cache.admit(
        copy.deepcopy(signed), binding=config.bindings["arena"], keys=config.keys, now=START + timedelta(seconds=44)
    ) == signed
    assert calls == 1

    with pytest.raises(ValueError, match="RECOVERY_ENVELOPE_STALE"):
        cache.admit(signed, binding=config.bindings["arena"], keys=config.keys, now=START + timedelta(seconds=45))
    assert calls == 1

    signed["facts"]["platform"]["state"] = "DOWN"
    with pytest.raises(SignatureValidationError, match="payload digest mismatch"):
        cache.admit(signed, binding=config.bindings["arena"], keys=config.keys, now=START)
    assert calls == 2


def frames(config: Config, signers: dict[str, Signer], *, end: int = 180, outage: bool = True) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for seconds in range(0, end + 1, 15):
        inputs = {r: packet(config, signers, r, seconds) for r in ROLES}
        if seconds == 45 and outage:
            inputs["dell"] = packet(config, signers, "dell", seconds, network={"state": "DOWN"})
            inputs["arena"] = packet(config, signers, "arena", seconds, platform={"state": "DOWN"})
        value = {
            "schema": SAMPLE_SCHEMA,
            "epoch_id": config.value["epoch_id"],
            "config_sha256": config.identity,
            "sample_sequence": len(result) + 1,
            "previous_sample_hash": result[-1]["sample_hash"] if result else None,
            "observed_at": isoformat_utc(START + timedelta(seconds=seconds)),
            "inputs": inputs,
            "input_errors": {},
        }
        value["live_recovery"] = advance(
            result[-1]["live_recovery"] if result else None,
            inputs,
            bindings=config.bindings,
            keys=config.keys,
            now=START + timedelta(seconds=seconds),
            history_policy=config.value.get("effect_history_policy"),
        )
        value["sample_hash"] = digest(value)
        result.append(value)
    return result


def rechain(rows: list[dict[str, Any]]) -> None:
    for i, row in enumerate(rows):
        row["sample_sequence"] = i + 1
        row["previous_sample_hash"] = rows[i - 1]["sample_hash"] if i else None
        row["sample_hash"] = digest(row)


def test_cra_safety_unavailable_is_unknown_not_positive_violation(
    setup: tuple[Config, dict[str, Signer]],
) -> None:
    config, signers = setup
    rows = frames(config, signers, outage=False)
    for row in rows:
        signed = row["inputs"]["cra"]
        unsigned = {key: copy.deepcopy(value) for key, value in signed.items() if key not in {"signature", "payload_sha256"}}
        unsigned["facts"]["safety"] = {
            "observed_at": unsigned["observed_at"],
            "evidence_status": "UNAVAILABLE",
            "reason_code": "CENTRAL_SAFETY_READ_FAILED",
            "integrity": "UNKNOWN",
        }
        row["inputs"]["cra"] = signers["cra"].sign(unsigned)
    replay_checkpoints(config, rows)
    rechain(rows)

    result = evaluate(rows, config=config, now=START + timedelta(seconds=180))

    assert "CRA_NO_ACTION_SAFETY_VIOLATED" not in result["blockers"]
    assert "CRA_SAFETY_EVIDENCE_MISSING_OR_STALE" in result["unknown_reasons"]
    assert result["harness_classification"] == "MISSING_EVIDENCE"


def test_explicit_cra_safety_violation_survives_other_missing_fields() -> None:
    assert recovery_soak._cra_safety_state({"command_delivery_enabled": True, "integrity": "UNKNOWN"}) == "VIOLATED"
    assert recovery_soak._cra_safety_state({"control_capability_count": 1}) == "VIOLATED"
    assert recovery_soak._cra_safety_state({"action_table_counts": {**dict.fromkeys(ACTION_TABLES, 0), "commands": 1}}) == "VIOLATED"
    assert recovery_soak._cra_safety_state({"action_table_counts": {"commands": 1}}) == "VIOLATED"
    assert recovery_soak._cra_safety_state({"integrity": "UNKNOWN"}) == "UNKNOWN"
    assert recovery_soak._cra_safety_state({
        "command_delivery_enabled": False,
        "runtime_operating_mode": "NO_ACTION",
        "control_capability_count": 0,
        "integrity": "ok",
        "action_table_counts": dict.fromkeys(ACTION_TABLES, 0),
        "evidence_status": "UNAVAILABLE",
    }) == "UNKNOWN"


def replay_checkpoints(config: Config, rows: list[dict[str, Any]]) -> None:
    checkpoint = None
    for row in rows:
        checkpoint = advance(
            checkpoint,
            row["inputs"],
            bindings=config.bindings,
            keys=config.keys,
            now=datetime.fromisoformat(row["observed_at"].replace("Z", "+00:00")),
            history_policy=config.value.get("effect_history_policy"),
        )
        row["live_recovery"] = checkpoint
    rechain(rows)


def owner_running_lifecycle(seconds: int, *, successor: bool) -> tuple[dict[str, Any], str]:
    pid, ticks = (124, 100) if successor else (123, 99)
    anchor = {
        "host_id": "dell-host",
        "host_boot_id": "dell-boot",
        "namespace": "streaming",
        "pod_uid": "pod-id",
        "container_name": "stream-engine",
        "container_id": "containerd://stream-engine",
        "ffmpeg_pid": pid,
        "ffmpeg_generation": "",
    }
    anchor["ffmpeg_generation"] = "ffmpeg-" + hashlib.sha256(f"pod-id:containerd://stream-engine:{pid}:{ticks}".encode()).hexdigest()[:32]
    return (
        {
            "schema": "runtime.child_lifecycle.v1",
            "state": "RUNNING",
            "reason": "EXACT_CHILD",
            "observed_at": isoformat_utc(START + timedelta(seconds=seconds)),
            "anchor_target": anchor,
            "owner_pid": 50,
            "owner_start_ticks": 10,
            "child_pid": pid,
            "child_start_ticks": ticks,
        },
        source_hash(anchor),
    )


def rtmps_episode_snapshot(
    seconds: int,
    *,
    started_seconds: int = 45,
    before: str = "a" * 64,
    after: str = "b" * 64,
    classification: str = "RTMPS_ACK_STALL_FFMPEG_REPLACED",
) -> dict[str, Any]:
    started = isoformat_utc(START + timedelta(seconds=started_seconds))
    episode = {
        "schema": "stream_v3_rtmps_episode/v2",
        "episode_id": hashlib.sha256(f"dell-boot:1:{started}:{before}".encode()).hexdigest()[:32],
        "sequence": 1,
        "producer_instance_id": "runtime-producer-1",
        "runtime_generation": "runtime-generation-1",
        "host_boot_id": "dell-boot",
        "pod_uid": "pod-1",
        "stream_engine_container_id": "containerd://one",
        "state": "RECOVERED",
        "classification": classification,
        "detection_basis": "SESSION_CONTINUITY_LOSS" if classification.startswith("FFMPEG_SESSION_LOSS") else "ACK_STALL",
        "trigger_reason_code": (
            "SOURCE_GENERATION_INVALID" if classification.startswith("FFMPEG_SESSION_LOSS") else "ZERO_ACK_WITH_SEND_PRESSURE"
        ),
        "started_at": started,
        "detected_at": isoformat_utc(START + timedelta(seconds=max(started_seconds, 45))),
        "last_observed_failure_at": isoformat_utc(START + timedelta(seconds=max(started_seconds, 45))),
        "recovered_at": isoformat_utc(START + timedelta(seconds=60)),
        "target_sha256": before,
        "replacement_target_sha256": after,
        "first_collector_sequence": 10,
        "last_collector_sequence": 13,
        "zero_ack_interval_count": 0 if classification.startswith("FFMPEG_SESSION_LOSS") else 1,
    }
    at = isoformat_utc(START + timedelta(seconds=seconds))
    return {
        "schema": "stream_v3.rtmps_episode_snapshot.v2",
        "collector_id": "collector-1",
        "release_id": "collector-release-1",
        "epoch_id": "collector-release-1",
        "observed_at": at,
        "collector_sequence": seconds + 1,
        "source_status": "VALID",
        "source_observed_at": at,
        "reason_code": "SOURCE_VALID",
        "episode_sequence": 1,
        "episode": episode,
    }


def test_signed_recovered_episode_does_not_bind_a_later_current_target(
    setup: tuple[Config, dict[str, Signer]],
) -> None:
    config, signers = setup
    signed = packet(config, signers, "dell", 90, transport={"target": "c" * 64, "rtmps_episode": rtmps_episode_snapshot(90)})
    assert admit_facts(
        signed, binding=config.bindings["dell"], keys=config.keys, now=START + timedelta(seconds=90)
    ) == signed


def test_signed_active_episode_remains_bound_to_current_target(
    setup: tuple[Config, dict[str, Signer]],
) -> None:
    config, signers = setup
    snapshot = rtmps_episode_snapshot(50)
    snapshot["episode"].update(state="ACTIVE", classification="RTMPS_ACK_STALL", recovered_at=None, replacement_target_sha256=None)
    signed = packet(config, signers, "dell", 50, transport={"target": "c" * 64, "rtmps_episode": snapshot})
    with pytest.raises(ValueError, match="RECOVERY_RTMPS_EPISODE_TARGET_INVALID"):
        admit_facts(signed, binding=config.bindings["dell"], keys=config.keys, now=START + timedelta(seconds=50))


def test_replayed_third_target_keeps_historical_recovered_episode_admissible(
    setup: tuple[Config, dict[str, Signer]],
) -> None:
    config, signers = setup
    rows = frames(config, signers, outage=False)
    for row in rows:
        seconds = int((datetime.fromisoformat(row["observed_at"].replace("Z", "+00:00")) - START).total_seconds())
        transport: dict[str, Any] = {"target": "a" * 64 if seconds < 60 else "b" * 64 if seconds < 90 else "c" * 64}
        if seconds >= 60:
            transport["rtmps_episode"] = rtmps_episode_snapshot(seconds)
        row["inputs"]["dell"] = packet(config, signers, "dell", seconds, transport=transport)
    replay_checkpoints(config, rows)
    result = evaluate(rows, config=config, now=START + timedelta(seconds=180))
    assert result["harness_classification"] == "PASS", result
    assert "DELL_SIGNED_EVIDENCE_INVALID" not in result["blockers"]
    assert "RECOVERY_RTMPS_EPISODE_TARGET_INVALID" not in result["unknown_reasons"]


def test_signed_microsecond_session_loss_matches_owner_episode_in_live_and_replay(
    setup: tuple[Config, dict[str, Signer]],
) -> None:
    config, signers = setup
    config.value.update(require_rtmps_episode_evidence=True, require_owner_lifecycle_evidence=True)
    rows = frames(config, signers, outage=False)
    old_lifecycle, old_target = owner_running_lifecycle(0, successor=False)
    _, new_target = owner_running_lifecycle(0, successor=True)
    started = START + timedelta(seconds=60, microseconds=61621)
    started_text = started.isoformat(timespec="microseconds").replace("+00:00", "Z")
    for row in rows:
        current = datetime.fromisoformat(row["observed_at"].replace("Z", "+00:00"))
        seconds = int((current - START).total_seconds())
        if seconds < 60:
            lifecycle, target = owner_running_lifecycle(seconds, successor=False)
        elif seconds == 60:
            lifecycle = dict(old_lifecycle, state="ABSENT", reason="NO_MANAGED_CHILD",
                             observed_at=isoformat_utc(current), child_pid=None, child_start_ticks=None)
            target = None
        else:
            lifecycle, target = owner_running_lifecycle(seconds, successor=True)
        transport: dict[str, Any] = {"target": target, "lifecycle": lifecycle}
        if seconds == 60:
            transport["bytes_acked"] = None
        if seconds >= 75:
            snapshot = rtmps_episode_snapshot(
                seconds, started_seconds=60, before=old_target, after=new_target,
                classification="FFMPEG_SESSION_LOSS_FFMPEG_REPLACED",
            )
            episode = snapshot["episode"]
            episode["started_at"] = started_text
            episode["episode_id"] = hashlib.sha256(
                f"dell-boot:1:{started_text}:{old_target}".encode()
            ).hexdigest()[:32]
            episode["detected_at"] = isoformat_utc(START + timedelta(seconds=65))
            episode["last_observed_failure_at"] = isoformat_utc(START + timedelta(seconds=65))
            episode["recovered_at"] = isoformat_utc(START + timedelta(seconds=70))
            transport["rtmps_episode"] = snapshot
        signed = packet(config, signers, "dell", seconds, transport=transport)
        if target is None:
            signed["facts"]["effects"].update(
                target_status="UNKNOWN", current_target_sha256=None,
                current_target_unresolved_scope_count=None,
            )
            signed = sign_facts(
                binding=config.bindings["dell"], signer=signers["dell"], host_boot_id="dell-boot",
                producer_id="dell-producer", sequence=seconds + 1, now=current,
                valid_until=current + timedelta(seconds=45), facts=signed["facts"],
                source_hashes={"source": "a" * 64},
            )
        row["inputs"]["dell"] = signed
    replay_checkpoints(config, rows)
    result = evaluate(rows, config=config, now=START + timedelta(seconds=180))
    live = rows[-1]["live_recovery"]["window"]
    assert result["harness_classification"] == "PASS", result
    assert result["recovery"]["recovered_episode_count"] == live["recovered_count"] == 1
    assert result["recovery"]["verified_network_recovered_episode_count"] == 0
    assert "RTMPS_EPISODE_OVERLAP" not in result["blockers"]
    assert "RTMPS_EPISODE_IDENTITY_CONFLICT" not in result["blockers"]
    assert "RECOVERY_NOT_EXERCISED" in result["pending_reasons"]


def test_ack_reset_visible_before_signed_session_replacement_is_reconstructed(
    setup: tuple[Config, dict[str, Signer]],
) -> None:
    config, signers = setup
    config.value.update(require_rtmps_episode_evidence=True, require_owner_lifecycle_evidence=True)
    rows = frames(config, signers, outage=False)
    _, old_target = owner_running_lifecycle(0, successor=False)
    _, new_target = owner_running_lifecycle(0, successor=True)
    for row in rows:
        current = datetime.fromisoformat(row["observed_at"].replace("Z", "+00:00"))
        seconds = int((current - START).total_seconds())
        lifecycle, target = owner_running_lifecycle(seconds, successor=seconds >= 75)
        transport: dict[str, Any] = {"target": target, "lifecycle": lifecycle}
        if seconds == 60:
            transport.update(target=old_target, bytes_acked=0)
        elif seconds >= 75:
            transport["rtmps_episode"] = rtmps_episode_snapshot(
                seconds,
                started_seconds=60,
                before=old_target,
                after=new_target,
                classification="FFMPEG_SESSION_LOSS_FFMPEG_REPLACED",
            )
        row["inputs"]["dell"] = packet(config, signers, "dell", seconds, transport=transport)
    replay_checkpoints(config, rows)

    result = evaluate(rows, config=config, now=START + timedelta(seconds=180))

    assert result["harness_classification"] == "PASS", result
    assert "ACK_COUNTER_DISCONTINUITY_UNVERIFIED" not in result["unknown_reasons"]
    assert "ACK_COUNTER_REGRESSION_WITHIN_TARGET" not in result["blockers"]
    assert result["recovery"]["recovered_episode_count"] == 1
    assert rows[-1]["live_recovery"]["window"]["ack_high_watermarks"] == {
        old_target: 5500,
        new_target: 19_000,
    }


def test_formal_window_drift_from_same_signed_inputs_is_harness_failure(
    setup: tuple[Config, dict[str, Signer]], monkeypatch: pytest.MonkeyPatch,
) -> None:
    config, signers = setup
    rows = frames(config, signers, outage=False)

    class DriftedWindow(RecoveryWindow):
        def observe(self, item: Any) -> None:
            super().observe(item)
            self.ack_point_count += 1

    monkeypatch.setattr(recovery_soak, "RecoveryWindow", DriftedWindow)
    result = evaluate(rows, config=config, now=START + timedelta(seconds=180))
    assert result["harness_classification"] == "HARNESS_FAILURE"
    assert "RECOVERY_WINDOW_PARITY_MISMATCH" in result["oracle_errors"]
    rows[4]["inputs"]["cra"] = packet(config, signers, "cra", 60, safety={"command_delivery_enabled": True})
    replay_checkpoints(config, rows)
    safety = evaluate(rows, config=config, now=START + timedelta(seconds=180))
    assert "RECOVERY_WINDOW_PARITY_MISMATCH" in safety["oracle_errors"]
    assert "CRA_NO_ACTION_SAFETY_VIOLATED" in safety["blockers"]
    for row in rows:
        for role, signed in row["inputs"].items():
            atomic_write_json(Path(config.value["hosts"][role]["inbox_file"]), signed)
        collect(config, now=datetime.fromisoformat(row["observed_at"].replace("Z", "+00:00")))
    formal = gate(config, now=START + timedelta(seconds=180))
    assert formal["soak_status"] == "FAIL"
    assert "RECOVERY_WINDOW_PARITY_MISMATCH" in formal["oracle_errors"]
    assert "CRA_NO_ACTION_SAFETY_VIOLATED" in formal["blockers"]


def test_previous_sample_schema_cannot_be_reused_for_new_checkpoint(
    setup: tuple[Config, dict[str, Signer]],
) -> None:
    config, signers = setup
    rows = frames(config, signers, outage=False)
    rows[0]["schema"] = "cra.recovery_soak_sample.v2"
    rechain(rows)
    result = evaluate(rows, config=config, now=START + timedelta(seconds=180))
    assert result["harness_classification"] != "PASS"
    assert "SAMPLE_CHAIN_OR_EPOCH_INVALID" in result["oracle_errors"]


def test_later_durable_rtmps_episode_resolves_only_covered_transport_gap(
    setup: tuple[Config, dict[str, Signer]],
) -> None:
    config, signers = setup
    config.value["require_rtmps_episode_evidence"] = True
    rows = frames(config, signers, outage=False)
    before, after = "a" * 64, "b" * 64
    for index in range(len(rows)):
        seconds = index * 15
        target = before if seconds < 60 else after
        transport: dict[str, Any] = {"target": target}
        if seconds == 45:
            transport["bytes_acked"] = 4000
        if seconds >= 60:
            transport["rtmps_episode"] = rtmps_episode_snapshot(seconds)
        rows[index]["inputs"]["dell"] = packet(config, signers, "dell", seconds, transport=transport)
    replay_checkpoints(config, rows)

    result = evaluate(rows, config=config, now=START + timedelta(seconds=180))

    assert result["harness_classification"] == "PASS", result
    assert result["soak_status"] == "NOT_YET_ELIGIBLE"
    assert "RECOVERY_SOURCE_GAP_OUTSIDE_KNOWN_OUTAGE" not in result["unknown_reasons"]
    assert result["recovery"]["recovered_episode_count"] == 1
    assert result["recovery"]["verified_network_recovered_episode_count"] == 0
    assert "RECOVERY_NOT_EXERCISED" in result["pending_reasons"]


def test_historical_20260922_platform_first_active_rtmps_is_one_episode(
    setup: tuple[Config, dict[str, Signer]],
) -> None:
    catalog = json.loads((ROOT / "harness/scenarios/rtmps_failure_sequences_v1.json").read_text(encoding="utf-8"))
    case = next(item for item in catalog["cases"] if item["id"] == "historical_20260922_platform_first_active_rtmps")
    timeline = case["timeline_seconds"]
    expected = case["expected"]
    config, signers = setup
    config.value["require_rtmps_episode_evidence"] = True
    rows = frames(config, signers, end=timeline["end"], outage=False)
    before, after = "a" * 64, "b" * 64
    for row in rows:
        current = datetime.fromisoformat(row["observed_at"].replace("Z", "+00:00"))
        seconds = int((current - START).total_seconds())
        target = before if seconds < timeline["recovered_visible"] else after
        transport: dict[str, Any] = {"target": target}
        if seconds >= timeline["active_visible"]:
            snapshot = rtmps_episode_snapshot(
                seconds,
                started_seconds=case["episode"]["started"],
                before=before,
                after=after,
            )
            episode = snapshot["episode"]
            episode["detected_at"] = isoformat_utc(START + timedelta(seconds=case["episode"]["detected"]))
            episode["last_observed_failure_at"] = episode["detected_at"]
            episode["recovered_at"] = isoformat_utc(START + timedelta(seconds=case["episode"]["recovered"]))
            if seconds < timeline["recovered_visible"]:
                episode.update(
                    state="ACTIVE",
                    classification="RTMPS_ACK_STALL",
                    recovered_at=None,
                    replacement_target_sha256=None,
                )
            transport["rtmps_episode"] = snapshot
        row["inputs"]["dell"] = packet(config, signers, "dell", seconds, transport=transport)
        if timeline["platform_down"] <= seconds < 90:
            row["inputs"]["arena"] = packet(config, signers, "arena", seconds, platform={"state": "DOWN"})
    replay_checkpoints(config, rows)

    result = evaluate(rows, config=config, now=START + timedelta(seconds=timeline["end"]))

    assert result["harness_classification"] == expected["harness_classification"], result
    assert result["live_health"] == expected["live_health"]
    assert result["recovery"]["recovered_episode_count"] == expected["recovered_episode_count"]
    assert expected["absent_blocker"] not in result["blockers"]


def test_historical_20260912_rtmps_gap_replay_is_classified(
    setup: tuple[Config, dict[str, Signer]],
) -> None:
    catalog = json.loads((ROOT / "harness/scenarios/rtmps_failure_sequences_v1.json").read_text(encoding="utf-8"))
    case = next(item for item in catalog["cases"] if item["id"] == "historical_20260912_sanitized")
    timeline = case["timeline_seconds"]
    expected = case["expected"]
    config, signers = setup
    config.value.update(require_rtmps_episode_evidence=True, require_owner_lifecycle_evidence=True)
    rows = frames(config, signers, end=timeline["end"], outage=False)
    for index in range(len(rows)):
        seconds = index * 15
        lifecycle, target = owner_running_lifecycle(seconds, successor=seconds >= timeline["episode_visible"])
        transport: dict[str, Any] = {"target": target, "lifecycle": lifecycle}
        if seconds == timeline["gap"]:
            transport.update(
                bytes_acked=None,
                target=None,
                lifecycle={
                    "schema": "runtime.child_lifecycle.v1",
                    "state": "UNKNOWN",
                    "reason": "READ_UNAVAILABLE",
                    "observed_at": isoformat_utc(START + timedelta(seconds=seconds)),
                    "anchor_target": None,
                    "owner_pid": None,
                    "owner_start_ticks": None,
                    "child_pid": None,
                    "child_start_ticks": None,
                },
            )
        if seconds >= timeline["episode_visible"]:
            transport["rtmps_episode"] = rtmps_episode_snapshot(
                seconds,
                started_seconds=case["episode"]["started"],
                before=owner_running_lifecycle(seconds, successor=False)[1],
                after=owner_running_lifecycle(seconds, successor=True)[1],
            )
        dell_packet = packet(config, signers, "dell", seconds, transport=transport)
        if seconds == timeline["gap"]:
            facts = dell_packet["facts"]
            facts["effects"].update(
                target_status="UNKNOWN",
                current_target_sha256=None,
                current_target_unresolved_scope_count=None,
            )
            dell_packet = sign_facts(
                binding=config.bindings["dell"],
                signer=signers["dell"],
                host_boot_id="dell-boot",
                producer_id="dell-producer",
                sequence=seconds + 1,
                now=START + timedelta(seconds=seconds),
                valid_until=START + timedelta(seconds=seconds + 45),
                facts=facts,
                source_hashes={"source": "a" * 64},
            )
        rows[index]["inputs"]["dell"] = dell_packet
    replay_checkpoints(config, rows)

    result = evaluate(rows, config=config, now=START + timedelta(seconds=timeline["end"]))

    assert result["harness_classification"] == expected["harness_classification"], result
    assert result["live_health"] == expected["live_health"]
    assert result["recovery"]["recovered_episode_count"] == expected["recovered_episode_count"]
    assert result["recovery"]["verified_network_recovered_episode_count"] == expected["verified_network_recovered_episode_count"]
    assert expected["pending_reason"] in result["pending_reasons"]
    assert expected["absent_unknown_reason"] not in result["unknown_reasons"]
    assert expected["absent_owner_unknown_reason"] not in result["unknown_reasons"]
    assert result["rtmps_episode_coverage"] == {
        "transport_gap_sample_count": 1,
        "explained_transport_gap_sample_count": 1,
        "owner_unknown_sample_count": 1,
        "explained_owner_unknown_sample_count": 1,
    }


def test_historical_20260912_eof_session_loss_replay_is_classified(
    setup: tuple[Config, dict[str, Signer]],
) -> None:
    catalog = json.loads((ROOT / "harness/scenarios/rtmps_failure_sequences_v1.json").read_text(encoding="utf-8"))
    case = next(item for item in catalog["cases"] if item["id"] == "historical_20260912_eof_without_flat_ack")
    timeline = case["timeline_seconds"]
    config, signers = setup
    config.value.update(require_rtmps_episode_evidence=True, require_owner_lifecycle_evidence=True)
    rows = frames(config, signers, end=timeline["end"], outage=False)
    for index in range(len(rows)):
        seconds = index * 15
        lifecycle, target = owner_running_lifecycle(seconds, successor=seconds >= timeline["episode_visible"])
        transport: dict[str, Any] = {"target": target, "lifecycle": lifecycle}
        if seconds == timeline["gap"]:
            transport.update(
                bytes_acked=None,
                target=None,
                lifecycle={
                    "schema": "runtime.child_lifecycle.v1",
                    "state": "UNKNOWN",
                    "reason": "READ_UNAVAILABLE",
                    "observed_at": isoformat_utc(START + timedelta(seconds=seconds)),
                    "anchor_target": None,
                    "owner_pid": None,
                    "owner_start_ticks": None,
                    "child_pid": None,
                    "child_start_ticks": None,
                },
            )
        if seconds >= timeline["episode_visible"]:
            transport["rtmps_episode"] = rtmps_episode_snapshot(
                seconds,
                started_seconds=case["episode"]["started"],
                before=owner_running_lifecycle(seconds, successor=False)[1],
                after=owner_running_lifecycle(seconds, successor=True)[1],
                classification=case["episode"]["classification"],
            )
        dell_packet = packet(config, signers, "dell", seconds, transport=transport)
        if seconds == timeline["gap"]:
            facts = dell_packet["facts"]
            facts["effects"].update(
                target_status="UNKNOWN",
                current_target_sha256=None,
                current_target_unresolved_scope_count=None,
            )
            dell_packet = sign_facts(
                binding=config.bindings["dell"],
                signer=signers["dell"],
                host_boot_id="dell-boot",
                producer_id="dell-producer",
                sequence=seconds + 1,
                now=START + timedelta(seconds=seconds),
                valid_until=START + timedelta(seconds=seconds + 45),
                facts=facts,
                source_hashes={"source": "a" * 64},
            )
        rows[index]["inputs"]["dell"] = dell_packet
    replay_checkpoints(config, rows)

    result = evaluate(rows, config=config, now=START + timedelta(seconds=timeline["end"]))

    assert result["harness_classification"] == "PASS", result
    assert result["live_health"] == "READY"
    assert result["recovery"]["recovered_episode_count"] == 1
    assert result["recovery"]["verified_network_recovered_episode_count"] == 0
    assert "RECOVERY_NOT_EXERCISED" in result["pending_reasons"]
    assert result["rtmps_episode_coverage"] == {
        "transport_gap_sample_count": 1,
        "explained_transport_gap_sample_count": 1,
        "owner_unknown_sample_count": 1,
        "explained_owner_unknown_sample_count": 1,
    }


def test_rtmps_episode_does_not_explain_gap_before_its_observed_start(
    setup: tuple[Config, dict[str, Signer]],
) -> None:
    config, signers = setup
    config.value["require_rtmps_episode_evidence"] = True
    rows = frames(config, signers, outage=False)
    before, after = "a" * 64, "b" * 64
    for index in range(len(rows)):
        seconds = index * 15
        target = before if seconds < 60 else after
        transport: dict[str, Any] = {"target": target}
        if seconds == 45:
            transport["bytes_acked"] = 4000
        if seconds >= 60:
            transport["rtmps_episode"] = rtmps_episode_snapshot(seconds, started_seconds=46)
        rows[index]["inputs"]["dell"] = packet(config, signers, "dell", seconds, transport=transport)
    replay_checkpoints(config, rows)

    result = evaluate(rows, config=config, now=START + timedelta(seconds=180))

    assert result["harness_classification"] == "MISSING_EVIDENCE"
    assert "RECOVERY_SOURCE_GAP_OUTSIDE_KNOWN_OUTAGE" in result["unknown_reasons"]


def test_rtmps_episode_does_not_explain_owner_unknown_before_its_observed_start(
    setup: tuple[Config, dict[str, Signer]],
) -> None:
    config, signers = setup
    config.value.update(require_rtmps_episode_evidence=True, require_owner_lifecycle_evidence=True)
    rows = frames(config, signers, outage=False)
    for index, row in enumerate(rows):
        seconds = index * 15
        lifecycle, target = owner_running_lifecycle(seconds, successor=seconds >= 60)
        row["inputs"]["dell"] = packet(
            config,
            signers,
            "dell",
            seconds,
            transport={"target": target, "lifecycle": lifecycle},
        )
    facts = copy.deepcopy(rows[3]["inputs"]["dell"]["facts"])
    facts["transport"].update(
        target=None,
        bytes_acked=None,
        lifecycle={
            "schema": "runtime.child_lifecycle.v1",
            "state": "UNKNOWN",
            "reason": "READ_UNAVAILABLE",
            "observed_at": isoformat_utc(START + timedelta(seconds=45)),
            "anchor_target": None,
            "owner_pid": None,
            "owner_start_ticks": None,
            "child_pid": None,
            "child_start_ticks": None,
        },
    )
    facts["effects"].update(
        target_status="UNKNOWN",
        current_target_sha256=None,
        current_target_unresolved_scope_count=None,
    )
    rows[3]["inputs"]["dell"] = sign_facts(
        binding=config.bindings["dell"],
        signer=signers["dell"],
        host_boot_id="dell-boot",
        producer_id="dell-producer",
        sequence=46,
        now=START + timedelta(seconds=45),
        valid_until=START + timedelta(seconds=90),
        facts=facts,
        source_hashes={"source": "a" * 64},
    )
    for index in range(4, len(rows)):
        seconds = index * 15
        facts = copy.deepcopy(rows[index]["inputs"]["dell"]["facts"])
        facts["transport"]["rtmps_episode"] = rtmps_episode_snapshot(
            seconds,
            started_seconds=46,
            before=owner_running_lifecycle(seconds, successor=False)[1],
            after=owner_running_lifecycle(seconds, successor=True)[1],
        )
        rows[index]["inputs"]["dell"] = sign_facts(
            binding=config.bindings["dell"],
            signer=signers["dell"],
            host_boot_id="dell-boot",
            producer_id="dell-producer",
            sequence=seconds + 1,
            now=START + timedelta(seconds=seconds),
            valid_until=START + timedelta(seconds=seconds + 45),
            facts=facts,
            source_hashes={"source": "a" * 64},
        )
    replay_checkpoints(config, rows)

    result = evaluate(rows, config=config, now=START + timedelta(seconds=180))

    assert result["harness_classification"] == "MISSING_EVIDENCE"
    assert "DELL_OWNER_LIFECYCLE_UNKNOWN_OUTSIDE_RECOVERY" in result["unknown_reasons"]


def test_recovered_outage_can_pass_instead_of_requiring_no_outages(setup: tuple[Config, dict[str, Signer]]) -> None:
    config, signers = setup
    result = evaluate(frames(config, signers), config=config, now=START + timedelta(seconds=180))
    assert result["status"] == "PASS", result
    assert result["soak_status"] == result["harness_classification"] == "PASS"
    assert result["formal_evaluation_performed"] is True
    assert result["recovery"]["recovered_episode_count"] == 1
    assert result["control_capability_count"] == 0


@pytest.mark.parametrize("missing_effects", [False, True])
def test_delivery_recovery_does_not_prove_network_recovery_or_erase_onset_gap(
    setup: tuple[Config, dict[str, Signer]],
    missing_effects: bool,
) -> None:
    config, signers = setup
    config.value["require_network_episode_evidence"] = True
    rows = frames(config, signers, outage=False)
    # Reproduce the reported topology, not the user's production data: arena
    # DOWN is followed by a delivery interruption while Dell network stays UP.
    rows[2]["inputs"]["arena"] = packet(config, signers, "arena", 30, network={"state": "DOWN"})
    for index in (3, 4, 5):
        rows[index]["inputs"]["arena"] = packet(config, signers, "arena", index * 15, platform={"state": "DOWN"})
    if missing_effects:
        for index in (3, 4, 5):
            value = packet(config, signers, "dell", index * 15, transport={"target": None, "bytes_acked": None})
            value["facts"]["effects"] = {"observed_at": None, "integrity": "UNKNOWN"}
            value["facts"]["activation"] = {"bounded_termination_enabled": None, "rw_timeout_enabled": None}
            rows[index]["inputs"]["dell"] = signers["dell"].sign(value)
    for index in range(6, len(rows)):
        rows[index]["inputs"]["dell"] = packet(config, signers, "dell", index * 15, transport={"target": "target-2"})
    replay_checkpoints(config, rows)
    result = evaluate(rows, config=config, now=START + timedelta(seconds=180))
    assert result["live_health"] == "READY"
    assert result["recovery"]["recovered_episode_count"] == 1
    assert result["recovery"]["verified_network_recovered_episode_count"] == 0
    assert "RECOVERY_NOT_EXERCISED" in result["pending_reasons"]
    assert result["physical_attempt_delta"] == 0
    assert not result["blockers"] and not result["oracle_errors"]
    if missing_effects:
        assert result["harness_classification"] == "MISSING_EVIDENCE"
        assert result["soak_status"] is None
        assert "DELL_EFFECT_EVIDENCE_MISSING_OR_STALE" in result["unknown_reasons"]
    else:
        assert result["harness_classification"] == "PASS"
        assert result["soak_status"] == "NOT_YET_ELIGIBLE"


def test_required_network_episode_field_cannot_be_silently_dropped(
    setup: tuple[Config, dict[str, Signer]],
) -> None:
    config, signers = setup
    config.value["require_network_episode_evidence"] = True
    rows = frames(config, signers, outage=False)
    for row in rows:
        dell = row["inputs"]["dell"]
        dell["facts"]["network"].pop("episode")
        row["inputs"]["dell"] = signers["dell"].sign(dell)
    replay_checkpoints(config, rows)

    result = evaluate(rows, config=config, now=START + timedelta(seconds=180))

    assert result["harness_classification"] == "MISSING_EVIDENCE"
    assert result["soak_status"] is None
    assert "DELL_NETWORK_EPISODE_EVIDENCE_REQUIRED" in result["unknown_reasons"]


@pytest.mark.parametrize("tampered", [False, True])
def test_signed_durable_network_episode_closes_collector_poll_gap(setup: tuple[Config, dict[str, Signer]], tampered: bool) -> None:
    config, signers = setup
    config.value["require_network_episode_evidence"] = True
    rows = frames(config, signers, outage=False)
    started = isoformat_utc(START + timedelta(seconds=34))
    episode = {
        "schema": "stream_v3_network_episode/v1",
        "episode_id": hashlib.sha256(f"dell-boot:1:{started}".encode()).hexdigest()[:32],
        "sequence": 1,
        "host_boot_id": "dell-boot",
        "state": "RECOVERED",
        "classification": "FULL_WAN",
        "started_at": started,
        "last_down_at": isoformat_utc(START + timedelta(seconds=40)),
        "recovered_at": isoformat_utc(START + timedelta(seconds=43)),
        "anchor_names": ["cloudflare_v4", "cloudflare_v6", "google_v4", "google_v6"],
        "provider_count": 2,
        "address_family_count": 2,
    }
    if tampered:
        episode["episode_id"] = "0" * 32
    for index in range(3, len(rows)):
        seconds = index * 15
        rows[index]["inputs"]["dell"] = packet(
            config,
            signers,
            "dell",
            seconds,
            network={"episode": copy.deepcopy(episode)},
            transport={"target": "target-2"},
        )
    replay_checkpoints(config, rows)
    result = evaluate(rows, config=config, now=START + timedelta(seconds=180))
    assert result["physical_attempt_delta"] == 0
    if tampered:
        assert result["harness_classification"] == "MISSING_EVIDENCE"
        assert result["recovery"]["verified_network_recovered_episode_count"] == 0
        assert "DELL_SIGNED_EVIDENCE_INVALID" in result["blockers"] or "DELL_SIGNED_EVIDENCE_INVALID" in result["unknown_reasons"]
    else:
        assert result["harness_classification"] == "PASS", result
        assert result["soak_status"] == "PASS"
        assert result["recovery"]["verified_network_recovered_episode_count"] == 1
        assert result["recovery"]["recent_episodes"][0]["trigger"] == "NETWORK_DOWN"


def test_unconfigured_pi_is_explicit_advisory_not_a_fake_key(setup: tuple[Config, dict[str, Signer]], tmp_path: Path) -> None:
    config, _ = setup
    value = copy.deepcopy(config.value)
    value["minimum_duration_seconds"] = 604800
    value["hosts"]["raspi"] = None
    path = tmp_path / "config.json"
    atomic_write_json(path, value)
    loaded = Config.load(path)
    assert "raspi" not in loaded.bindings
    sample = collect(loaded, now=START)
    assert sample["inputs"]["raspi"] is None
    assert sample["input_errors"]["raspi"] == "ADVISORY_NOT_CONFIGURED"
    result = evaluate([sample], config=loaded, now=START)
    assert result["status"] != "PASS"
    assert "PI_EVIDENCE_UNAVAILABLE" in result["pi_advisories"]


@pytest.mark.parametrize("role", ["dell", "arena", "cra"])
def test_core_host_cannot_be_unconfigured(setup: tuple[Config, dict[str, Signer]], tmp_path: Path, role: str) -> None:
    config, _ = setup
    value = copy.deepcopy(config.value)
    value["minimum_duration_seconds"] = 604800
    value["hosts"][role] = None
    path = tmp_path / "config.json"
    atomic_write_json(path, value)
    with pytest.raises(ValueError, match="HOST_CONFIG_INVALID"):
        Config.load(path)


def frozen_history() -> dict[str, Any]:
    return {
        "schema": POLICY_SCHEMA,
        "frozen_at": "2026-09-03T00:00:00.000Z",
        "retired_unknown": [
            {
                "effect_scope_id": digit * 64,
                "target_sha256": "c" * 64,
                "retirement_sha256": "d" * 64,
                "created_at": "2026-09-01T00:00:00.000Z",
                "retired_at": "2026-09-02T00:00:00.000Z",
                "physical_attempt_count": 1,
                "physical_effect_outcome": "UNKNOWN",
            }
            for digit in ("1", "2")
        ],
    }


def test_historical_unknowns_are_reported_separately_without_claiming_resolution(setup: tuple[Config, dict[str, Signer]]) -> None:
    config, signers = setup
    config.value["effect_history_policy"] = frozen_history()
    result = evaluate(frames(config, signers), config=config, now=START + timedelta(seconds=180))
    assert result["status"] == "PASS", result  # Short isolated fixture, not production seven-day eligibility.
    reported = result["effect_history"]
    assert len(reported["historical_retired_unknown"]) == 2
    assert reported["historical_outcome_resolved"] is False
    assert reported["final_observation"]["unresolved_scope_count_total"] == 2
    assert reported["final_observation"]["unresolved_scope_count"] == 0
    assert reported["final_observation"]["current_target_unresolved_scope_count"] == 0


@pytest.mark.parametrize("change", ["drop-history", "current-target", "counter-partition", "policy-hash"])
def test_formal_and_live_replay_both_reject_false_history_claims(setup: tuple[Config, dict[str, Signer]], change: str) -> None:
    config, signers = setup
    config.value["effect_history_policy"] = frozen_history()
    rows = frames(config, signers)
    value = rows[5]["inputs"]["dell"]
    effects = value["facts"]["effects"]
    if change == "drop-history":
        effects["historical_retired_unknown"].pop()
        effects["unresolved_scope_count_total"] = 1
    elif change == "current-target":
        effects["current_target_sha256"] = "foreign-target"
    elif change == "counter-partition":
        effects["current_target_unresolved_scope_count"] = 1
    else:
        effects["history_policy_sha256"] = "a" * 64
    rows[5]["inputs"]["dell"] = signers["dell"].sign(value)
    replay_checkpoints(config, rows)
    result = evaluate(rows, config=config, now=START + timedelta(seconds=180))
    assert result["status"] == "FAIL"
    assert "DELL_EFFECT_HISTORY_INVALID" in result["blockers"]
    assert "DELL_EFFECT_HISTORY_INVALID" in rows[-1]["live_recovery"]["integrity_blockers"]


def test_new_unknown_blocks_recovery_even_with_two_approved_historical_unknowns(setup: tuple[Config, dict[str, Signer]]) -> None:
    config, signers = setup
    config.value["effect_history_policy"] = frozen_history()
    rows = frames(config, signers, end=720)
    for row in rows[4:]:
        value = row["inputs"]["dell"]
        value["facts"]["effects"].update(unresolved_scope_count=1, unresolved_scope_count_total=3)
        row["inputs"]["dell"] = signers["dell"].sign(value)
    replay_checkpoints(config, rows)
    result = evaluate(rows, config=config, now=START + timedelta(seconds=720))
    assert result["status"] == "FAIL"
    assert result["recovery"]["recovered_episode_count"] == 0
    assert result["effect_history"]["final_observation"]["unresolved_scope_count"] == 1


def test_first_sample_cannot_precede_history_freeze(setup: tuple[Config, dict[str, Signer]]) -> None:
    config, signers = setup
    config.value["effect_history_policy"] = frozen_history()
    config.value["effect_history_policy"]["frozen_at"] = (START + timedelta(seconds=1)).isoformat()
    with pytest.raises(ValueError, match="FREEZE_AFTER_EPOCH"):
        collect(config, now=START)
    assert not Path(config.value["evidence_file"]).exists()
    result = evaluate(frames(config, signers), config=config, now=START + timedelta(seconds=180))
    assert "EFFECT_HISTORY_FREEZE_AFTER_EPOCH" in result["blockers"]


def test_history_policy_change_cannot_reuse_an_epoch(setup: tuple[Config, dict[str, Signer]]) -> None:
    config, _ = setup
    collect(config, now=START)
    original = Path(config.value["evidence_file"]).read_bytes()
    config.value["effect_history_policy"] = frozen_history()
    with pytest.raises(ValueError, match="EPOCH_CHANGED"):
        collect(config, now=START + timedelta(seconds=15))
    assert Path(config.value["evidence_file"]).read_bytes() == original


def test_seven_days_and_recovery_exercise_are_independent_requirements(setup: tuple[Config, dict[str, Signer]]) -> None:
    config, signers = setup
    result = evaluate(frames(config, signers, outage=False), config=config, now=START + timedelta(seconds=180))
    assert result["status"] == "NOT_YET_ELIGIBLE"
    assert result["pending_reasons"] == ["RECOVERY_NOT_EXERCISED"]
    config.value["minimum_duration_seconds"] = 604800
    result = evaluate(frames(config, signers), config=config, now=START + timedelta(seconds=180))
    assert result["status"] == "NOT_YET_ELIGIBLE"
    assert "SOAK_DURATION_INSUFFICIENT" in result["pending_reasons"]


def test_false_dashboard_health_does_not_override_platform_nodata(setup: tuple[Config, dict[str, Signer]]) -> None:
    config, signers = setup
    rows = frames(config, signers, end=720)
    for row in rows[3:]:
        sec = int((datetime.fromisoformat(row["observed_at"].replace("Z", "+00:00")) - START).total_seconds())
        row["inputs"]["arena"] = packet(config, signers, "arena", sec, platform={"state": "DOWN"})
    replay_checkpoints(config, rows)
    result = evaluate(rows, config=config, now=START + timedelta(seconds=720))
    assert result["status"] == "FAIL"
    assert "RECOVERY_DEADLINE_EXCEEDED" in result["blockers"]


def test_upstream_oauth_probe_gap_pauses_formal_soak_without_sut_failure(
    setup: tuple[Config, dict[str, Signer]],
) -> None:
    config, signers = setup
    rows = frames(config, signers, outage=False)
    rows[4]["inputs"]["arena"] = packet(config, signers, "arena", 60, platform={"state": "UNKNOWN"})
    replay_checkpoints(config, rows)
    result = evaluate(rows, config=config, now=START + timedelta(seconds=180))
    assert result["harness_classification"] == "MISSING_EVIDENCE"
    assert result["formal_evaluation_performed"] is False
    assert result["soak_status"] is None
    assert "SOURCE_EVIDENCE_UNKNOWN_OUTSIDE_KNOWN_OUTAGE" in result["unknown_reasons"]
    assert "RECOVERY_SOURCE_GAP_OUTSIDE_KNOWN_OUTAGE" in result["unknown_reasons"]


@pytest.mark.parametrize(
    "mutation,blocker",
    [
        ("signature", "DELL_SIGNED_EVIDENCE_INVALID"),
        ("source_commit", "DELL_SIGNED_EVIDENCE_INVALID"),
        ("sequence", "DELL_SEQUENCE_REGRESSION_OR_CONFLICT"),
        ("host_boot_id", "DELL_BOOT_OR_PRODUCER_ROTATED"),
    ],
)
def test_identity_signature_sequence_failures_are_not_expected_outages(
    setup: tuple[Config, dict[str, Signer]],
    mutation: str,
    blocker: str,
) -> None:
    config, signers = setup
    rows = frames(config, signers)
    value = rows[6]["inputs"]["dell"]
    value[mutation] = 1 if mutation == "sequence" else "tampered"
    if mutation != "signature":
        rows[6]["inputs"]["dell"] = signers["dell"].sign(value)
    rechain(rows)
    result = evaluate(rows, config=config, now=START + timedelta(seconds=180))
    assert blocker in result["blockers"]


@pytest.mark.parametrize("plane", ["command_delivery_enabled", "table", "duplicate_attempt_count", "activation"])
def test_safety_or_unactivated_protection_blocks_soak(setup: tuple[Config, dict[str, Signer]], plane: str) -> None:
    config, signers = setup
    rows = frames(config, signers)
    for row in rows:
        role = "cra" if plane in {"table", "command_delivery_enabled"} else "dell"
        value = row["inputs"][role]
        if plane == "table":
            value["facts"]["safety"]["action_table_counts"]["commands"] = 1
        elif plane == "command_delivery_enabled":
            value["facts"]["safety"][plane] = True
        elif plane == "activation":
            value["facts"]["activation"]["bounded_termination_enabled"] = False
        else:
            value["facts"]["effects"][plane] = 1
        row["inputs"][role] = signers[role].sign(value)
    replay_checkpoints(config, rows)
    assert evaluate(rows, config=config, now=START + timedelta(seconds=180))["status"] == "FAIL"


def test_pi_failure_is_advisory_not_a_dell_recovery_trigger(setup: tuple[Config, dict[str, Signer]]) -> None:
    config, signers = setup
    rows = frames(config, signers, outage=False)
    for row in rows:
        row["inputs"]["raspi"] = None
    rechain(rows)
    result = evaluate(rows, config=config, now=START + timedelta(seconds=180))
    assert result["recovery"]["episode_count"] == 0
    assert result["pi_advisories"] == ["PI_EVIDENCE_UNAVAILABLE"]
    assert result["pi_is_control_input"] is False
    assert result["blockers"] == []


def test_collector_gap_without_historical_replay_pauses_certification(setup: tuple[Config, dict[str, Signer]]) -> None:
    config, signers = setup
    rows = frames(config, signers)
    del rows[4:8]
    replay_checkpoints(config, rows)
    result = evaluate(rows, config=config, now=START + timedelta(seconds=180))
    assert result["maximum_sample_gap_seconds"] == 75
    assert result["soak_status"] is None
    assert result["harness_classification"] == "MISSING_EVIDENCE"
    assert "COLLECTOR_GAP_UNRECONSTRUCTED" in result["unknown_reasons"]
    assert "COLLECTOR_GAP_UNRECONSTRUCTED" not in result["blockers"]
    assert result["operator_status"]["soak_impact"] == "CERTIFICATION_PAUSED"
    assert result["operator_status"]["failure_domain"] == "HARNESS"


def test_three_collector_periods_are_the_inclusive_gap_boundary(setup: tuple[Config, dict[str, Signer]]) -> None:
    config, signers = setup
    rows = frames(config, signers, outage=False)
    del rows[3:5]
    replay_checkpoints(config, rows)
    result = evaluate(rows, config=config, now=START + timedelta(seconds=180))
    assert result["maximum_sample_gap_seconds"] == 45
    assert "COLLECTOR_GAP_UNRECONSTRUCTED" not in result["unknown_reasons"]
    assert result["harness_classification"] == "PASS"


def test_history_config_is_new_epoch_bound_and_rejects_ambiguous_archive_paths(
    setup: tuple[Config, dict[str, Signer]]
) -> None:
    config, _ = setup
    root = Path(config.value["state_file"]).parent
    old_identity = config.identity
    config.value["schema"] = HISTORY_CONFIG_SCHEMA
    config.value["minimum_duration_seconds"] = 604800
    config.value["history_files"] = {role: str(root / f"{role}-history.jsonl") for role in ("dell", "arena", "cra")}
    path = root / "history-config.json"
    atomic_write_json(path, config.value)
    loaded = Config.load(path)
    assert loaded.identity != old_identity
    assert loaded.value["history_files"] == config.value["history_files"]
    config.value["history_files"]["cra"] = config.value["history_files"]["arena"]
    atomic_write_json(path, config.value)
    with pytest.raises(ValueError, match="PATH_COLLISION"):
        Config.load(path)


@pytest.mark.parametrize("corruption", [None, "missing_packet", "wrong_signature", "cra_effect"])
def test_complete_three_host_history_reconstructs_only_proven_gap(setup: tuple[Config, dict[str, Signer]], corruption: str | None) -> None:
    config, signers = setup
    root = Path(config.value["state_file"]).parent
    config.value["schema"] = HISTORY_CONFIG_SCHEMA
    config.value["history_files"] = {role: str(root / f"{role}-history.jsonl") for role in ("dell", "arena", "cra")}
    all_rows = frames(config, signers)
    for index, row in enumerate(all_rows):
        for role in ("dell", "arena", "cra"):
            unsigned = {key: value for key, value in row["inputs"][role].items() if key not in {"signature", "payload_sha256"}}
            unsigned["sequence"] = index + 1
            row["inputs"][role] = signers[role].sign(unsigned)
    for role in ("dell", "arena", "cra"):
        packets = [row["inputs"][role] for row in all_rows]
        if corruption == "missing_packet" and role == "dell":
            del packets[5]
        elif corruption == "wrong_signature" and role == "arena":
            packets[5] = {**packets[5], "signature": "invalid"}
        elif corruption == "cra_effect" and role == "cra":
            unsigned = {key: value for key, value in packets[5].items() if key not in {"signature", "payload_sha256"}}
            unsigned["facts"]["safety"]["command_delivery_enabled"] = True
            packets[5] = signers[role].sign(unsigned)
        Path(config.value["history_files"][role]).write_text("".join(json.dumps(packet) + "\n" for packet in packets))
    del all_rows[4:8]
    for row in all_rows:
        for role, value in row["inputs"].items():
            atomic_write_json(Path(config.value["hosts"][role]["inbox_file"]), value)
        collect(config, now=datetime.fromisoformat(row["observed_at"].replace("Z", "+00:00")))
    result = gate(config, now=START + timedelta(seconds=180))
    assert result["maximum_sample_gap_seconds"] == 75
    if corruption is None:
        assert result["historical_replay"]["status"] == "RECONSTRUCTED", result
        assert result["harness_classification"] == "PASS", result
        assert result["soak_status"] == "PASS", result
        atomic_write_json(Path(config.value["gate_file"]), result)
        assert "HISTORICAL_EVIDENCE_UNAVAILABLE" not in watchdog(config, now=START + timedelta(seconds=180))["blockers"]
        Path(config.value["history_files"]["dell"]).unlink()
        assert "HISTORICAL_EVIDENCE_UNAVAILABLE" in watchdog(config, now=START + timedelta(seconds=180))["blockers"]
    elif corruption == "cra_effect":
        assert result["historical_replay"]["status"] == "RECONSTRUCTED"
        assert result["soak_status"] == "FAIL"
        assert "CRA_NO_ACTION_SAFETY_VIOLATED" in result["blockers"]
    else:
        assert result["historical_replay"]["status"] == "UNAVAILABLE_OR_INVALID"
        assert result["soak_status"] is None
        assert "COLLECTOR_GAP_UNRECONSTRUCTED" in result["unknown_reasons"]


def test_collector_clock_regression_is_harness_error(setup: tuple[Config, dict[str, Signer]]) -> None:
    config, signers = setup
    rows = frames(config, signers, outage=False)
    rows[4]["observed_at"] = rows[3]["observed_at"]
    rechain(rows)
    result = evaluate(rows, config=config, now=START + timedelta(seconds=180))
    assert result["soak_status"] is None
    assert result["harness_classification"] == "HARNESS_FAILURE"
    assert "COLLECTOR_CLOCK_REGRESSION" in result["oracle_errors"]


def test_missing_evidence_is_unknown_not_healthy(setup: tuple[Config, dict[str, Signer]]) -> None:
    config, signers = setup
    rows = frames(config, signers)
    rows[-1]["inputs"]["arena"] = None
    rows[-1]["live_recovery"] = advance(
        rows[-2]["live_recovery"], rows[-1]["inputs"], bindings=config.bindings, keys=config.keys, now=START + timedelta(seconds=180)
    )
    rechain(rows)
    result = evaluate(rows, config=config, now=START + timedelta(seconds=180))
    assert result["status"] == "UNKNOWN"
    assert result["live_health"] != "READY"


def test_old_epoch_and_truncated_history_do_not_pass(setup: tuple[Config, dict[str, Signer]]) -> None:
    config, signers = setup
    rows = frames(config, signers)
    old = copy.deepcopy(rows)
    old[0]["epoch_id"] = "old-frozen-epoch"
    rechain(old)
    assert "SAMPLE_CHAIN_OR_EPOCH_INVALID" in evaluate(old, config=config, now=START + timedelta(seconds=180))["oracle_errors"]
    result = evaluate(rows[1:], config=config, now=START + timedelta(seconds=180))
    assert result["status"] == "UNKNOWN"
    assert result["soak_status"] is None
    assert result["harness_classification"] == "HARNESS_FAILURE"


def test_actual_collector_and_gate_roundtrip(setup: tuple[Config, dict[str, Signer]]) -> None:
    config, signers = setup
    for source in frames(config, signers):
        for role, value in source["inputs"].items():
            atomic_write_json(Path(config.value["hosts"][role]["inbox_file"]), value)
        collect(config, now=datetime.fromisoformat(source["observed_at"].replace("Z", "+00:00")))
    result = gate(config, now=START + timedelta(seconds=180))
    assert result["status"] == "PASS", result
    assert result["evidence_verified_bytes"] == Path(config.value["evidence_file"]).stat().st_size


def test_collector_and_replay_share_persisted_timestamp_precision(setup: tuple[Config, dict[str, Signer]]) -> None:
    config, signers = setup
    current = START + timedelta(microseconds=987654)
    for role in ROLES:
        atomic_write_json(Path(config.value["hosts"][role]["inbox_file"]), packet(config, signers, role, 0))
    sample = collect(config, now=current)
    assert sample["observed_at"].endswith(".987Z")
    result = gate(config, now=START + timedelta(seconds=1))
    assert result["oracle_errors"] == []
    assert result["harness_classification"] != "HARNESS_FAILURE"


def test_startup_timer_burst_does_not_become_false_seven_day_capacity_failure(
    setup: tuple[Config, dict[str, Signer]],
) -> None:
    config, signers = setup
    config.value["minimum_duration_seconds"] = 604800
    for seconds in (0, 1):
        for role in ROLES:
            atomic_write_json(Path(config.value["hosts"][role]["inbox_file"]), packet(config, signers, role, seconds))
        collect(config, now=START + timedelta(seconds=seconds))
    startup = gate(config, now=START + timedelta(seconds=2))
    assert startup["capacity_projection_warmup_seconds"] == 300
    assert startup["projected_seven_day_bytes"] is None
    assert "EVIDENCE_CAPACITY_NOT_YET_MEASURABLE" in startup["pending_reasons"]
    assert "EVIDENCE_CAPACITY_NOT_PROVEN" not in startup["blockers"]
    assert startup["harness_classification"] != "ENVIRONMENT_FAILURE"

    for seconds in range(15, 301, 15):
        for role in ROLES:
            atomic_write_json(Path(config.value["hosts"][role]["inbox_file"]), packet(config, signers, role, seconds))
        collect(config, now=START + timedelta(seconds=seconds))
    measured = gate(config, now=START + timedelta(seconds=300))
    assert measured["projected_seven_day_bytes"] is not None
    assert measured["projected_safety_margin_bytes"] < measured["evidence_limit_bytes"]
    assert "EVIDENCE_CAPACITY_NOT_YET_MEASURABLE" not in measured["pending_reasons"]
    assert not measured["blockers"]


def test_collector_records_unavailable_inputs_instead_of_stopping(setup: tuple[Config, dict[str, Signer]]) -> None:
    config, _ = setup
    sample = collect(config, now=START)
    assert sample["inputs"] == dict.fromkeys(ROLES)
    assert sample["input_errors"] == dict.fromkeys(ROLES, "INPUT_UNAVAILABLE")


def test_partial_tail_and_missing_state_fail_closed(setup: tuple[Config, dict[str, Signer]]) -> None:
    config, _ = setup
    collect(config, now=START)
    path = Path(config.value["evidence_file"])
    with path.open("ab") as stream:
        stream.write(b'{"partial":')
    with pytest.raises(ValueError, match="PARTIAL"):
        collect(config, now=START + timedelta(seconds=15))


def test_reserved_sample_conflicting_partial_tail_is_preserved(
    setup: tuple[Config, dict[str, Signer]], monkeypatch: pytest.MonkeyPatch,
) -> None:
    config, _ = setup
    collect(config, now=START)
    evidence = Path(config.value["evidence_file"])
    original = recovery_soak._append_line

    def conflicting(path: Path, _sample: dict[str, Any], **_kwargs: Any) -> None:
        with path.open("ab") as stream:
            stream.write(b'{"different":')
        raise OSError("isolated conflicting short write")

    monkeypatch.setattr(recovery_soak, "_append_line", conflicting)
    with pytest.raises(OSError, match="conflicting short write"):
        collect(config, now=START + timedelta(seconds=15))
    before = evidence.read_bytes()
    monkeypatch.setattr(recovery_soak, "_append_line", original)
    with pytest.raises(ValueError, match="PENDING_PREFIX_CONFLICT"):
        collect(config, now=START + timedelta(seconds=30))
    assert evidence.read_bytes() == before
    status = recovery_soak.watchdog(config, now=START + timedelta(seconds=30))
    assert status["status"] == "AT_RISK"
    assert "EVIDENCE_APPEND_PENDING" in status["blockers"]


def _reserved_without_append(config: Config, monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    original = recovery_soak._append_line
    with monkeypatch.context() as patch:
        patch.setattr(recovery_soak, "_append_line", lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError("reserved")))
        with pytest.raises(OSError, match="reserved"):
            collect(config, now=START + timedelta(seconds=15))
    assert recovery_soak._append_line is original
    return json.loads(Path(config.value["state_file"]).read_bytes())["pending_sample"]


def test_pending_resume_rejects_two_identical_full_frames(
    setup: tuple[Config, dict[str, Signer]], monkeypatch: pytest.MonkeyPatch,
) -> None:
    config, _ = setup
    collect(config, now=START)
    pending = _reserved_without_append(config, monkeypatch)
    evidence = Path(config.value["evidence_file"])
    with evidence.open("ab") as stream:
        stream.write(_serialized_line(pending) * 2)
    before = evidence.read_bytes()
    with pytest.raises(ValueError, match="PENDING_PREFIX_CONFLICT"):
        collect(config, now=START + timedelta(seconds=30))
    assert evidence.read_bytes() == before
    assert json.loads(Path(config.value["state_file"]).read_bytes())["sample_count"] == 1


def test_pending_resume_rechecks_committed_frame_hash(
    setup: tuple[Config, dict[str, Signer]], monkeypatch: pytest.MonkeyPatch,
) -> None:
    config, _ = setup
    collect(config, now=START)
    _reserved_without_append(config, monkeypatch)
    evidence = Path(config.value["evidence_file"])
    row = json.loads(evidence.read_bytes())
    row["observed_at"] = row["observed_at"].replace("00:00:00", "00:00:01")
    evidence.write_bytes(_serialized_line(row))
    before = evidence.read_bytes()
    with pytest.raises(ValueError, match="PENDING_PREFIX_CONFLICT"):
        collect(config, now=START + timedelta(seconds=30))
    assert evidence.read_bytes() == before
    assert json.loads(Path(config.value["state_file"]).read_bytes())["sample_count"] == 1


def test_pending_snapshot_reads_only_committed_prefix_without_certifying_tail(
    setup: tuple[Config, dict[str, Signer]], monkeypatch: pytest.MonkeyPatch,
) -> None:
    config, _ = setup
    committed = collect(config, now=START)
    pending = _reserved_without_append(config, monkeypatch)
    evidence = Path(config.value["evidence_file"])
    with evidence.open("ab") as stream:
        stream.write(_serialized_line(pending)[:19])
    before = evidence.read_bytes()
    head, rows = recovery_soak.snapshot(config)
    assert head["sample_count"] == 1
    assert list(rows) == [committed]
    result = gate(config, now=START + timedelta(seconds=30))
    assert result["sample_count"] == 1
    assert result["soak_status"] is None
    assert "EVIDENCE_APPEND_PENDING" in result["unknown_reasons"]
    assert evidence.read_bytes() == before


def test_raw_signed_failure_survives_capacity_projection_failure(
    setup: tuple[Config, dict[str, Signer]],
) -> None:
    config, signers = setup
    config.value["minimum_duration_seconds"] = 604800
    config.value["maximum_evidence_bytes"] = 1024 * 1024
    for seconds in range(0, 301, 15):
        for role in ROLES:
            changes = {"safety": {"command_delivery_enabled": True}} if role == "cra" and seconds == 300 else {}
            atomic_write_json(Path(config.value["hosts"][role]["inbox_file"]), packet(config, signers, role, seconds, **changes))
        collect(config, now=START + timedelta(seconds=seconds))
    _, rows = recovery_soak.snapshot(config)
    assert evaluate(rows, config=config, now=START + timedelta(seconds=300))["soak_status"] == "FAIL"
    after = gate(config, now=START + timedelta(seconds=300))
    assert after["projected_safety_margin_bytes"] > after["evidence_limit_bytes"]
    assert after["soak_status"] == "FAIL"
    assert after["formal_evaluation_performed"] is True
    assert after["harness_classification"] == "SUT_FAILURE"
    assert "CRA_NO_ACTION_SAFETY_VIOLATED" in after["blockers"]
    assert "EVIDENCE_CAPACITY_NOT_PROVEN" in after["unknown_reasons"]


def test_config_change_requires_new_epoch_not_state_reuse(setup: tuple[Config, dict[str, Signer]]) -> None:
    config, _ = setup
    collect(config, now=START)
    config.value["epoch_id"] = "different-epoch"
    with pytest.raises(ValueError, match="EPOCH_CHANGED"):
        collect(config, now=START + timedelta(seconds=15))


def test_json_on_disk_is_hash_bound(setup: tuple[Config, dict[str, Signer]]) -> None:
    config, _ = setup
    sample = collect(config, now=START)
    saved = json.loads(Path(config.value["evidence_file"]).read_text())
    assert saved == sample
    assert saved["sample_hash"] == digest(saved)


def test_lightweight_watchdog_preserves_deadline_across_collector_restart(setup: tuple[Config, dict[str, Signer]]) -> None:
    config, signers = setup
    for seconds in (0, 15, 30, 45, 60):
        for role in ROLES:
            changes = {"network": {"state": "DOWN"}} if role == "dell" and seconds == 45 else {}
            if role == "arena" and seconds >= 45:
                changes = {"platform": {"state": "DOWN"}}
            atomic_write_json(Path(config.value["hosts"][role]["inbox_file"]), packet(config, signers, role, seconds, **changes))
        collect(config, now=START + timedelta(seconds=seconds))
    before = Path(config.value["evidence_file"]).read_bytes()
    result = watchdog(config, now=START + timedelta(seconds=661))
    assert "RECOVERY_DEADLINE_EXCEEDED" in result["blockers"]
    assert "COLLECTOR_STALE" in result["blockers"]
    assert Path(config.value["evidence_file"]).read_bytes() == before
    assert result["formal_status"] == "NOT_EVALUATED"


def test_watchdog_binds_older_formal_gate_to_its_exact_committed_prefix(
    setup: tuple[Config, dict[str, Signer]],
) -> None:
    config, signers = setup

    def collect_at(seconds: int) -> dict[str, Any]:
        for role in ROLES:
            atomic_write_json(Path(config.value["hosts"][role]["inbox_file"]), packet(config, signers, role, seconds))
        return collect(config, now=START + timedelta(seconds=seconds))

    for seconds in (0, 15, 30):
        collect_at(seconds)
    formal = gate(config, now=START + timedelta(seconds=30))
    atomic_write_json(Path(config.value["gate_file"]), formal)
    later = [collect_at(seconds) for seconds in (45, 60)]

    accepted = watchdog(config, now=START + timedelta(seconds=60))
    assert "FORMAL_GATE_STALE_OR_UNBOUND" not in accepted["blockers"]

    forged = dict(formal)
    forged["last_sample_hash"] = later[0]["sample_hash"]
    atomic_write_json(Path(config.value["gate_file"]), forged)
    rejected = watchdog(config, now=START + timedelta(seconds=60))
    assert "FORMAL_GATE_STALE_OR_UNBOUND" in rejected["blockers"]

    forged = dict(formal)
    forged["evidence_verified_bytes"] = Path(config.value["evidence_file"]).stat().st_size
    atomic_write_json(Path(config.value["gate_file"]), forged)
    rejected = watchdog(config, now=START + timedelta(seconds=60))
    assert "FORMAL_GATE_STALE_OR_UNBOUND" in rejected["blockers"]


def test_formal_history_revision_resolves_only_the_exact_live_transport_order_unknown(
    setup: tuple[Config, dict[str, Signer]],
) -> None:
    config, signers = setup
    root = Path(config.value["state_file"]).parent
    config.value["schema"] = HISTORY_CONFIG_SCHEMA
    config.value["history_files"] = {role: str(root / f"{role}-history.jsonl") for role in ("dell", "arena", "cra")}

    targets = {
        0: ("target-initial", 0),
        15: ("target-unseen-predecessor", 15),
        30: ("target-current", 30),
        45: ("target-current", 45),
        # The live sampler did not see target-unseen-predecessor before this
        # delayed fact. The complete history did, and therefore knows it is a
        # retired predecessor rather than a successor.
        60: ("target-unseen-predecessor", 20),
        75: ("target-current", 75),
        90: ("target-current", 90),
        105: ("target-current", 105),
    }
    packets: dict[str, list[dict[str, Any]]] = {role: [] for role in ROLES}
    for sequence, seconds in enumerate(targets, start=1):
        target, transport_seconds = targets[seconds]
        for role in ROLES:
            changes: dict[str, Any] = {}
            if role == "dell":
                changes["transport"] = {
                    "target": target,
                    "observed_at": isoformat_utc(START + timedelta(seconds=transport_seconds)),
                    "bytes_acked": 1000 + seconds * 100,
                }
            signed = packet(config, signers, role, seconds, **changes)
            unsigned = {key: value for key, value in signed.items() if key not in {"signature", "payload_sha256"}}
            unsigned["sequence"] = sequence
            packets[role].append(signers[role].sign(unsigned))

    for role in ("dell", "arena", "cra"):
        Path(config.value["history_files"][role]).write_text(
            "".join(json.dumps(value) + "\n" for value in packets[role]), encoding="utf-8"
        )

    raw_seconds = (0, 45, 60, 75, 90, 105)
    for seconds in raw_seconds:
        index = list(targets).index(seconds)
        for role in ROLES:
            atomic_write_json(Path(config.value["hosts"][role]["inbox_file"]), packets[role][index])
        collect(config, now=START + timedelta(seconds=seconds))

    before = watchdog(config, now=START + timedelta(seconds=105))
    assert "TRANSPORT_TARGET_ORDER_UNVERIFIED" in before["blockers"]

    formal = gate(config, now=START + timedelta(seconds=105))
    resolution = formal["live_unknown_resolutions"]["TRANSPORT_TARGET_ORDER_UNVERIFIED"]
    assert formal["historical_replay"]["status"] == "RECONSTRUCTED", formal
    assert "TRANSPORT_TARGET_ORDER_UNVERIFIED" not in formal["unknown_reasons"]
    assert resolution["status"] == "RESOLVED"
    assert resolution["observation_revision"]["revision"] == 1
    atomic_write_json(Path(config.value["gate_file"]), formal)

    accepted = watchdog(config, now=START + timedelta(seconds=105))
    assert "TRANSPORT_TARGET_ORDER_UNVERIFIED" not in accepted["blockers"]
    assert accepted["resolved_live_unknowns"] == ["TRANSPORT_TARGET_ORDER_UNVERIFIED"]

    forged = copy.deepcopy(formal)
    forged["live_unknown_resolutions"]["TRANSPORT_TARGET_ORDER_UNVERIFIED"]["observation_revision"]["revision"] = 2
    atomic_write_json(Path(config.value["gate_file"]), forged)
    rejected = watchdog(config, now=START + timedelta(seconds=105))
    assert "TRANSPORT_TARGET_ORDER_UNVERIFIED" in rejected["blockers"]

    atomic_write_json(Path(config.value["gate_file"]), formal)
    new_packets: dict[str, dict[str, Any]] = {}
    for role in ROLES:
        changes = {
            "transport": {
                "target": "target-new-ambiguous",
                "observed_at": isoformat_utc(START + timedelta(seconds=80)),
                "bytes_acked": 99_999,
            }
        } if role == "dell" else {}
        signed = packet(config, signers, role, 120, **changes)
        unsigned = {key: value for key, value in signed.items() if key not in {"signature", "payload_sha256"}}
        unsigned["sequence"] = len(targets) + 1
        new_packets[role] = signers[role].sign(unsigned)
        atomic_write_json(Path(config.value["hosts"][role]["inbox_file"]), new_packets[role])
    collect(config, now=START + timedelta(seconds=120))
    advanced = watchdog(config, now=START + timedelta(seconds=120))
    assert "TRANSPORT_TARGET_ORDER_UNVERIFIED" in advanced["blockers"]
    assert advanced["resolved_live_unknowns"] == []

def test_cra_safe_block_uses_its_own_bounded_oracle(setup: tuple[Config, dict[str, Signer]]) -> None:
    config, signers = setup
    rows = frames(config, signers, outage=False)
    rows[3]["inputs"]["cra"] = packet(config, signers, "cra", 45, safety={"runtime_readiness": "SAFE_BLOCKED"})
    replay_checkpoints(config, rows)
    result = evaluate(rows, config=config, now=START + timedelta(seconds=180))
    assert result["recovery"]["recovered_episode_count"] == 0
    assert result["recovery"]["verified_network_recovered_episode_count"] == 0
    assert result["recovery"]["cra_safe_blocked_episode_count"] == 1
    assert result["recovery"]["cra_max_safe_blocked_duration_seconds"] == 15
    assert result["failure_domain_sample_counts"]["cra_runtime_safe_blocked"] == 1
    assert result["status"] == "NOT_YET_ELIGIBLE"
    assert "RECOVERY_NOT_EXERCISED" in result["pending_reasons"]


def test_transient_transport_gap_is_not_erased_by_final_health(setup: tuple[Config, dict[str, Signer]]) -> None:
    config, signers = setup
    rows = frames(config, signers)
    rows[8]["inputs"]["dell"] = packet(config, signers, "dell", 120, transport={"bytes_acked": None})
    replay_checkpoints(config, rows)
    result = evaluate(rows, config=config, now=START + timedelta(seconds=180))
    assert result["live_health"] == "READY"
    assert result["status"] == "UNKNOWN"
    assert "RECOVERY_SOURCE_GAP_OUTSIDE_KNOWN_OUTAGE" in result["unknown_reasons"]


def test_exact_owner_successor_covers_transport_convergence_gap(setup: tuple[Config, dict[str, Signer]]) -> None:
    config, signers = setup
    rows = frames(config, signers, outage=False)

    def lifecycle(seconds: int, *, successor: bool) -> tuple[dict[str, Any], str]:
        now = START + timedelta(seconds=seconds)
        pid, ticks = (124, 100) if successor else (123, 99)
        anchor = {
            "host_id": "dell-host",
            "host_boot_id": "dell-boot",
            "namespace": "streaming",
            "pod_uid": "pod-id",
            "container_name": "stream-engine",
            "container_id": "containerd://stream-engine",
            "ffmpeg_pid": pid,
            "ffmpeg_generation": "",
        }
        anchor["ffmpeg_generation"] = (
            "ffmpeg-" + hashlib.sha256(f"pod-id:containerd://stream-engine:{pid}:{ticks}".encode()).hexdigest()[:32]
        )
        return (
            {
                "schema": "runtime.child_lifecycle.v1",
                "state": "RUNNING",
                "reason": "EXACT_CHILD",
                "observed_at": isoformat_utc(now),
                "anchor_target": anchor,
                "owner_pid": 50,
                "owner_start_ticks": 10,
                "child_pid": pid,
                "child_start_ticks": ticks,
            },
            source_hash(anchor),
        )

    for row in rows:
        current = datetime.fromisoformat(row["observed_at"].replace("Z", "+00:00"))
        seconds = int((current - START).total_seconds())
        successor = seconds >= 60
        owner, owner_target = lifecycle(seconds, successor=successor)
        facts = copy.deepcopy(row["inputs"]["dell"]["facts"])
        facts["transport"].update(target=owner_target, lifecycle=owner)
        facts["effects"].update(target_status="VALID", current_target_sha256=owner_target)
        if seconds == 60:
            # Production ordering: the owner has proved the new child while
            # the transport reader still has no target/ACK for that child.
            facts["transport"].update(
                target=None,
                bytes_acked=None,
                observed_at=isoformat_utc(START + timedelta(seconds=45)),
            )
        row["inputs"]["dell"] = sign_facts(
            binding=config.bindings["dell"],
            signer=signers["dell"],
            host_boot_id="dell-boot",
            producer_id="dell-producer",
            sequence=seconds + 1,
            now=current,
            valid_until=current + timedelta(seconds=45),
            facts=facts,
            source_hashes={"source": "a" * 64},
        )
    replay_checkpoints(config, rows)
    result = evaluate(rows, config=config, now=START + timedelta(seconds=180))
    assert result["harness_classification"] == "PASS", result
    assert result["recovery"]["recovered_episode_count"] == 1
    assert result["recovery"]["recent_episodes"][0]["trigger"] == "TARGET_TRANSITION"
    # The onset plus the first two post-transition ACK points are UNKNOWN;
    # none are outside the bounded recovery episode.
    assert result["recovery"]["recent_episodes"][0]["unknown_sample_count"] == 3
    assert result["target_transitions"] == {"total": 1, "no_authority_effect": 1, "effect_bound": 0, "unclassified": 0}
    assert "RECOVERY_SOURCE_GAP_OUTSIDE_KNOWN_OUTAGE" not in result["unknown_reasons"]


def test_runtime_evidence_gap_during_known_recovery_is_not_a_false_failure(
    setup: tuple[Config, dict[str, Signer]],
) -> None:
    config, signers = setup
    rows = frames(config, signers)
    transition = rows[4]["inputs"]["dell"]
    transition["source_failure_count"] = 1
    transition["facts"]["transport"] = {"target": None, "bytes_acked": None, "observed_at": rows[4]["observed_at"]}
    transition["facts"]["effects"] = {"observed_at": None, "integrity": "UNKNOWN"}
    transition["facts"]["activation"] = {"bounded_termination_enabled": None, "rw_timeout_enabled": None}
    rows[4]["inputs"]["dell"] = signers["dell"].sign(transition)
    for index, row in enumerate(rows[5:], start=5):
        seconds = int((datetime.fromisoformat(row["observed_at"].replace("Z", "+00:00")) - START).total_seconds())
        successor = packet(config, signers, "dell", seconds, transport={"target": "target-2"})
        successor["source_failure_count"] = 1
        rows[index]["inputs"]["dell"] = signers["dell"].sign(successor)
    replay_checkpoints(config, rows)
    result = evaluate(rows, config=config, now=START + timedelta(seconds=180))
    assert result["status"] == "PASS", result
    assert result["harness_classification"] == "PASS"
    assert result["recovery"]["maximum_recovery_seconds"] <= 600
    assert result["target_transitions"] == {"total": 1, "no_authority_effect": 1, "effect_bound": 0, "unclassified": 0}
    assert result["physical_attempt_delta"] == 0
    assert result["source_failure_events"]["during_recovery"]["dell"] == 1
    assert result["source_failure_events"]["outside_recovery"]["dell"] == 0


@pytest.mark.parametrize("regress", [False, True])
def test_source_failure_counter_preserves_unsampled_read_failures(setup: tuple[Config, dict[str, Signer]], regress: bool) -> None:
    config, signers = setup
    rows = frames(config, signers)
    for index, row in enumerate(rows):
        value = row["inputs"]["dell"]
        value["source_failure_count"] = int(index < 5 if regress else index >= 5)
        row["inputs"]["dell"] = signers["dell"].sign(value)
    replay_checkpoints(config, rows)
    result = evaluate(rows, config=config, now=START + timedelta(seconds=180))
    assert result["status"] == ("FAIL" if regress else "PASS")
    expected_blocker = "DELL_SOURCE_FAILURE_COUNTER_REGRESSION"
    assert (expected_blocker in rows[-1]["live_recovery"]["integrity_blockers"]) is regress
    if not regress:
        assert result["source_failure_events"]["during_recovery"]["dell"] == 1
        assert result["source_failure_events"]["outside_recovery"]["dell"] == 0


def test_source_failure_outside_recovery_withholds_formal_verdict(setup: tuple[Config, dict[str, Signer]]) -> None:
    config, signers = setup
    rows = frames(config, signers, outage=False)
    for index, row in enumerate(rows):
        value = row["inputs"]["dell"]
        value["source_failure_count"] = int(index >= 5)
        row["inputs"]["dell"] = signers["dell"].sign(value)
    replay_checkpoints(config, rows)
    result = evaluate(rows, config=config, now=START + timedelta(seconds=180))
    assert result["status"] == "UNKNOWN"
    assert result["soak_status"] is None
    assert result["harness_classification"] == "MISSING_EVIDENCE"
    assert result["source_failure_events"]["outside_recovery"]["dell"] == 1


def test_config_loader_pins_keys_and_requires_seven_days(setup: tuple[Config, dict[str, Signer]], tmp_path: Path) -> None:
    config, _ = setup
    path = tmp_path / "config.json"
    obsolete = copy.deepcopy(config.value)
    obsolete["schema"] = "cra.recovery_soak_config.v1"
    obsolete["minimum_duration_seconds"] = 604800
    atomic_write_json(path, obsolete)
    with pytest.raises(ValueError, match="CONFIG_INVALID"):
        Config.load(path)
    atomic_write_json(path, config.value)
    with pytest.raises(ValueError, match="SEVEN_DAY"):
        Config.load(path)
    config.value["minimum_duration_seconds"] = 604800
    atomic_write_json(path, config.value)
    loaded = Config.load(path)
    assert len(loaded.key_fingerprints) == 4
    key = Ed25519PrivateKey.generate().public_key()
    Path(config.value["hosts"]["dell"]["public_key_file"]).write_bytes(
        key.public_bytes(serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo)
    )
    assert Config.load(path).identity != loaded.identity


@pytest.mark.parametrize("suffix", [".lock", ".watchdog.json"])
def test_config_protects_derived_output_paths(setup: tuple[Config, dict[str, Signer]], tmp_path: Path, suffix: str) -> None:
    config, _ = setup
    config.value["minimum_duration_seconds"] = 604800
    config.value["hosts"]["dell"]["inbox_file"] = str(Path(config.value["state_file"]).with_suffix(suffix))
    path = tmp_path / "config.json"
    atomic_write_json(path, config.value)
    with pytest.raises(ValueError, match="COLLISION"):
        Config.load(path)


@pytest.mark.parametrize("attempts,proof", [(0, False), (1, True), (1, False), (2, True)])
def test_target_transition_is_distinct_from_an_authority_effect(
    setup: tuple[Config, dict[str, Signer]],
    attempts: int,
    proof: bool,
) -> None:
    config, signers = setup
    rows = frames(config, signers)
    for row in rows[4:]:
        sec = int((datetime.fromisoformat(row["observed_at"].replace("Z", "+00:00")) - START).total_seconds())
        reconciliation = {"resolution": "EFFECT_OBSERVED", "before_target": "target-1", "after_target": "target-2", "action_id": "a"}
        row["inputs"]["dell"] = packet(
            config,
            signers,
            "dell",
            sec,
            transport={"target": "target-2"},
            effects={
                "physical_attempt_count": attempts,
                "unresolved_scope_count": int(sec < 90),
                "last_reconciliation": reconciliation if proof and sec >= 90 else {},
            },
        )
    replay_checkpoints(config, rows)
    result = evaluate(rows, config=config, now=START + timedelta(seconds=180))
    assert result["status"] == ("PASS" if attempts == 0 else "FAIL"), result
    assert result["physical_attempt_delta"] == attempts
    assert result["target_transitions"]["no_authority_effect"] == int(attempts == 0)
    assert result["target_transitions"]["effect_bound"] == int(attempts == 1 and proof)
    assert result["target_transitions"]["unclassified"] == int(attempts != 0 and not (attempts == 1 and proof))
    assert ("NO_ACTION_PHYSICAL_EFFECT_DETECTED" in result["blockers"]) is (attempts != 0)


def test_committed_append_is_recovered_once_without_sequence_reuse(setup: tuple[Config, dict[str, Signer]]) -> None:
    config, _ = setup
    collect(config, now=START)
    head_path = Path(config.value["state_file"])
    head = json.loads(head_path.read_text())
    collect(config, now=START + timedelta(seconds=15))
    atomic_write_json(head_path, head)  # isolated append/state interruption
    third = collect(config, now=START + timedelta(seconds=30))
    assert third["sample_sequence"] == 3
    assert len(Path(config.value["evidence_file"]).read_text().splitlines()) == 3


def test_ambiguous_jsonl_cannot_be_hidden_by_canonical_hash(setup: tuple[Config, dict[str, Signer]]) -> None:
    config, _ = setup
    collect(config, now=START)
    evidence, state = Path(config.value["evidence_file"]), Path(config.value["state_file"])
    raw = evidence.read_bytes()
    evidence.write_bytes(b'{"schema":"discarded-duplicate",' + raw[1:])
    head = json.loads(state.read_text())
    head["verified_bytes"] = evidence.stat().st_size
    atomic_write_json(state, head)
    with pytest.raises(ValueError, match="DUPLICATE"):
        gate(config, now=START)


def test_state_checkpoint_tampering_is_not_a_collector_restart(setup: tuple[Config, dict[str, Signer]]) -> None:
    config, _ = setup
    collect(config, now=START)
    path = Path(config.value["state_file"])
    head = json.loads(path.read_text())
    head["live_recovery"]["window"]["exceeded_count"] = 0x1234
    atomic_write_json(path, head)
    with pytest.raises(ValueError, match="STATE_EVIDENCE_FORK"):
        collect(config, now=START + timedelta(seconds=15))


def test_live_checkpoint_replay_mismatch_is_harness_failure_not_sut_failure(
    setup: tuple[Config, dict[str, Signer]],
) -> None:
    config, signers = setup
    rows = frames(config, signers)
    rows[6]["live_recovery"]["window"]["maximum_recovery_seconds"] = 999
    rechain(rows)
    result = evaluate(rows, config=config, now=START + timedelta(seconds=180))
    assert result["status"] == "UNKNOWN"
    assert result["soak_status"] is None
    assert result["formal_evaluation_performed"] is False
    assert result["harness_classification"] == "HARNESS_FAILURE"
    assert result["oracle_errors"] == ["LIVE_CHECKPOINT_REPLAY_MISMATCH"]


def test_no_action_effect_increment_fails_without_requiring_target_transition(
    setup: tuple[Config, dict[str, Signer]],
) -> None:
    config, signers = setup
    rows = frames(config, signers)
    for row in rows[4:]:
        seconds = int((datetime.fromisoformat(row["observed_at"].replace("Z", "+00:00")) - START).total_seconds())
        row["inputs"]["dell"] = packet(config, signers, "dell", seconds, effects={"physical_attempt_count": 1})
    replay_checkpoints(config, rows)
    result = evaluate(rows, config=config, now=START + timedelta(seconds=180))
    assert result["status"] == "FAIL"
    assert result["harness_classification"] == "SUT_FAILURE"
    assert result["physical_attempt_delta"] == result["physical_attempt_increase_count"] == 1
    assert result["target_transitions"]["total"] == 0
    assert "NO_ACTION_PHYSICAL_EFFECT_DETECTED" in result["blockers"]


def test_gap_and_failure_domain_metrics_are_sample_counts(setup: tuple[Config, dict[str, Signer]]) -> None:
    config, signers = setup
    result = evaluate(frames(config, signers), config=config, now=START + timedelta(seconds=180))
    assert result["sample_gaps"]["average_seconds"] == result["sample_gaps"]["p95_upper_bound_seconds"] == 15
    assert result["sample_gaps"]["exceeded_seconds_count"] == {"15": 0, "30": 0, "45": 0}
    assert result["failure_domain_sample_counts"]["dell_only_network_down"] == 1
    assert result["failure_domain_sample_counts"]["both_network_down"] == 0
