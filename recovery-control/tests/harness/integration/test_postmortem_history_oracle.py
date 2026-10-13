"""Source-time fault cells and fixed oracle expectations for the history profile.

Only pytest-owned files and keys are used.  These cases do not simulate a real
network partition, FFmpeg codec, RTMPS service, or seven-day elapsed time.
"""

from __future__ import annotations

import ast
import copy
import inspect
import json
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest

from cra_dell_recovery.canonical import Signer
from cra_harness.observers.evidence import EvidenceBundle, EvidenceEvent
from cra_harness.oracles.contract import IndependentOracle, OracleResult
from cra_harness.scenarios.model import InvariantExpectation, InvariantOperator
from cra_no_action_soak import recovery_soak
from cra_no_action_soak.append_only_jsonl import _serialized_line
from cra_no_action_soak.host_status import atomic_write_json
from cra_no_action_soak.recovery_soak import HISTORY_CONFIG_SCHEMA, Config
from cra_no_action_soak.recovery_window import RecoveryWindow
from tests.harness.unit.test_recovery_soak import START, frames, packet
from tests.harness.unit.test_recovery_soak import setup as setup


def _fixed_oracle(result: dict[str, Any], expectations: tuple[tuple[str, InvariantOperator, Any], ...]) -> OracleResult:
    """Compare captured gate evidence with literals, never with SUT transitions."""
    projected = dict(result)
    episodes = result.get("recovery", {}).get("recent_episodes", [])
    projected["first_recovery_episode"] = episodes[0] if episodes else {}
    bundle = EvidenceBundle(
        "history-run",
        "history-cell",
        "2026-09-04T00:00:00Z",
        "2026-09-04T00:03:00Z",
        (
            EvidenceEvent(
                "gate-result",
                1,
                "history-run",
                "history-cell",
                "gate",
                "sut",
                "2026-09-04T00:03:00Z",
                projected,
            ),
        ),
    )
    fixed = tuple(
        InvariantExpectation(f"gate.{path}", operator, expected, f"HISTORY-{index}")
        for index, (path, operator, expected) in enumerate(expectations, start=1)
    )
    return IndependentOracle().evaluate_expectations(fixed, bundle)


def _run_history_cell(
    config: Config,
    signers: dict[str, Signer],
    *,
    hidden: str | None = None,
    corrupt: str | None = None,
    late_dell_packet: bool = False,
    late_cra_packet: str | None = None,
    late_arena_unknown: bool = False,
    dell_attempt_counts: dict[int, int] | None = None,
    keep_intermediate_raw: bool = False,
    stale_first_dell_packet: bool = False,
    history_signature_faults: dict[str, int] | None = None,
) -> dict[str, Any]:
    root = Path(config.value["state_file"]).parent
    config.value["schema"] = HISTORY_CONFIG_SCHEMA
    config.value["history_files"] = {role: str(root / f"{role}-history.jsonl") for role in ("dell", "arena", "cra")}
    rows = frames(config, signers, outage=False)
    archives: dict[str, list[dict[str, Any]]] = {role: [] for role in ("dell", "arena", "cra")}
    for index, row in enumerate(rows):
        for role in archives:
            source = row["inputs"][role]
            if late_dell_packet and role == "dell" and index in (1, 2):
                source = packet(config, signers, role, 10 if index == 1 else 13)
            if late_cra_packet is not None and role == "cra" and index == 1:
                source = packet(config, signers, role, 10)
            if late_cra_packet is not None and role == "cra" and index == 2:
                intermediate = packet(config, signers, role, 13)
                unsigned_intermediate = copy.deepcopy(
                    {key: value for key, value in intermediate.items() if key not in {"signature", "payload_sha256"}}
                )
                unsigned_intermediate["sequence"] = 3
                if late_cra_packet == "unsafe_effect":
                    unsigned_intermediate["facts"]["safety"]["command_delivery_enabled"] = True
                elif late_cra_packet == "short_lease":
                    unsigned_intermediate["valid_until"] = recovery_soak.isoformat_utc(START + timedelta(seconds=13, milliseconds=1))
                else:
                    raise AssertionError("unknown late CRA case")
                archives[role].append(signers[role].sign(unsigned_intermediate))
            unsigned = copy.deepcopy({key: value for key, value in source.items() if key not in {"signature", "payload_sha256"}})
            unsigned["sequence"] = index + 1 + int(late_cra_packet is not None and role == "cra" and index >= 2)
            if stale_first_dell_packet and role == "dell" and index == 0:
                source_at = recovery_soak.isoformat_utc(START - timedelta(seconds=40))
                unsigned["observed_at"] = source_at
                unsigned["valid_until"] = recovery_soak.isoformat_utc(START - timedelta(seconds=1))
                for fact in unsigned["facts"].values():
                    if isinstance(fact, dict) and "observed_at" in fact:
                        fact["observed_at"] = source_at
            if dell_attempt_counts is not None and role == "dell":
                unsigned["facts"]["effects"]["physical_attempt_count"] = dell_attempt_counts.get(index, 1)
            if index == 3:
                if hidden == "network" and role == "dell":
                    unsigned["facts"]["network"]["state"] = "DOWN"
                elif hidden == "platform" and role == "arena":
                    unsigned["facts"]["platform"]["state"] = "DOWN"
                elif hidden == "control" and role == "arena":
                    unsigned["facts"]["control_path"]["state"] = "DOWN"
                elif hidden == "cra_delivery" and role == "cra":
                    unsigned["facts"]["safety"]["command_delivery_enabled"] = True
                elif hidden == "dell_duplicate" and role == "dell":
                    unsigned["facts"]["effects"]["duplicate_attempt_count"] = 1
                elif hidden == "dell_physical" and role == "dell":
                    unsigned["facts"]["effects"]["physical_attempt_count"] = 1
                elif hidden == "dell_activation" and role == "dell":
                    unsigned["facts"]["activation"]["bounded_termination_enabled"] = False
                elif hidden == "dell_rw_timeout" and role == "dell":
                    unsigned["facts"]["activation"]["rw_timeout_enabled"] = False
                elif hidden == "dell_activation_without_target" and role == "dell":
                    unsigned["facts"]["activation"]["bounded_termination_enabled"] = False
                    unsigned["facts"]["transport"]["target"] = None
                if corrupt == "producer_rotation" and role == "dell":
                    unsigned["producer_id"] = "other-producer"
                if corrupt == "source_time_regression" and role == "dell":
                    unsigned["observed_at"] = rows[2]["observed_at"]
            if index == 5:
                if late_arena_unknown and role == "arena":
                    unsigned["facts"]["platform"]["state"] = "UNKNOWN"
                if hidden == "raw_cra_delivery" and role == "cra":
                    unsigned["facts"]["safety"]["command_delivery_enabled"] = True
                elif hidden == "raw_dell_duplicate" and role == "dell":
                    unsigned["facts"]["effects"]["duplicate_attempt_count"] = 1
                elif hidden == "raw_dell_physical" and role == "dell":
                    unsigned["facts"]["effects"]["physical_attempt_count"] = 1
                elif hidden == "raw_dell_activation" and role == "dell":
                    unsigned["facts"]["activation"]["bounded_termination_enabled"] = False
                elif hidden == "raw_dell_rw_timeout" and role == "dell":
                    unsigned["facts"]["activation"]["rw_timeout_enabled"] = False
            signed = signers[role].sign(unsigned)
            row["inputs"][role] = signed
            archives[role].append(signed)
    if corrupt == "missing_packet":
        del archives["dell"][3]
    elif corrupt == "bad_signature":
        archives["arena"][3]["signature"] = "invalid"
    elif corrupt == "bad_later_signature":
        archives["arena"][5]["signature"] = "invalid"
    elif corrupt == "bad_later_cra_signature":
        archives["cra"][5]["signature"] = "invalid"
    elif corrupt == "bad_cra_signature":
        archives["cra"][3]["signature"] = "invalid"
    elif corrupt == "bad_dell_signature":
        archives["dell"][3]["signature"] = "invalid"
    elif corrupt == "boundary_conflict":
        unsigned = copy.deepcopy({key: value for key, value in archives["dell"][5].items() if key not in {"signature", "payload_sha256"}})
        unsigned["facts"]["network"]["state"] = "DOWN"
        archives["dell"][5] = signers["dell"].sign(unsigned)
    for role, fault_index in (history_signature_faults or {}).items():
        archives[role][fault_index]["signature"] = "invalid"
    for role, packets in archives.items():
        path = Path(config.value["history_files"][role])
        path.write_text("".join(json.dumps(packet) + "\n" for packet in packets), encoding="utf-8")
        if corrupt == "partial_tail" and role == "dell":
            with path.open("ab") as stream:
                stream.write(b"{")
    # The collector sees neither source event at t=45 nor the t=60 cycle.
    # The raw inter-sample gap is exactly 45 seconds, not over the old limit.
    if not keep_intermediate_raw:
        del rows[3:5]
    for row in rows:
        for role, signed_packet in row["inputs"].items():
            recovery_soak.atomic_write_json(Path(config.value["hosts"][role]["inbox_file"]), signed_packet)
        recovery_soak.collect(config, now=recovery_soak.timestamp(row["observed_at"]))
    return recovery_soak.gate(config, now=START + timedelta(seconds=180))


def _signed_at(
    config: Config, signers: dict[str, Signer], role: str, *, sequence: int, milliseconds: int,
    unsafe_delivery: bool = False, lease_milliseconds: int = 45_000,
) -> dict[str, Any]:
    """Test-owned signed packet with independently chosen source and raw times."""
    original = packet(config, signers, role, 10)
    unsigned = copy.deepcopy({key: value for key, value in original.items() if key not in {"signature", "payload_sha256"}})
    old_at = unsigned["observed_at"]
    at = recovery_soak.isoformat_utc(START + timedelta(milliseconds=milliseconds))

    def retime(value: Any) -> Any:
        if isinstance(value, dict):
            return {key: retime(item) for key, item in value.items()}
        if isinstance(value, list):
            return [retime(item) for item in value]
        return at if value == old_at else value

    unsigned = retime(unsigned)
    unsigned["sequence"] = sequence
    unsigned["valid_until"] = recovery_soak.isoformat_utc(
        START + timedelta(milliseconds=milliseconds + lease_milliseconds)
    )
    if unsafe_delivery:
        unsigned["facts"]["safety"]["command_delivery_enabled"] = True
    return signers[role].sign(unsigned)


def _run_millisecond_collision(
    config: Config, signers: dict[str, Signer], *, unsafe_hidden_cra: bool = False,
    expired_hidden_cra: bool = False, unsafe_overwritten_same_millisecond: bool = False,
) -> dict[str, Any]:
    root = Path(config.value["state_file"]).parent
    config.value["schema"] = HISTORY_CONFIG_SCHEMA
    config.value["history_files"] = {role: str(root / f"{role}-history.jsonl") for role in ("dell", "arena", "cra")}
    first = {role: _signed_at(config, signers, role, sequence=1, milliseconds=10_000) for role in ("dell", "arena", "cra")}
    second = {
        "dell": _signed_at(config, signers, "dell", sequence=3, milliseconds=12_000),
        "arena": _signed_at(config, signers, "arena", sequence=2, milliseconds=15_001),
        "cra": _signed_at(
            config, signers, "cra", sequence=3,
            milliseconds=12_000 if unsafe_overwritten_same_millisecond else 30_000,
        ),
    }
    archives = {
        "dell": [first["dell"], _signed_at(config, signers, "dell", sequence=2, milliseconds=11_566), second["dell"]],
        "arena": [first["arena"], second["arena"]],
        "cra": [
            first["cra"],
            _signed_at(
                config, signers, "cra", sequence=2, milliseconds=11_700,
                unsafe_delivery=unsafe_hidden_cra,
                lease_milliseconds=3_301 if expired_hidden_cra else 45_000,
            ),
            second["cra"],
        ],
    }
    for role, values in archives.items():
        Path(config.value["history_files"][role]).write_text(
            "".join(json.dumps(value) + "\n" for value in values), encoding="utf-8"
        )
    for milliseconds, inputs in ((15_000, first), (30_000, second)):
        for role, value in inputs.items():
            atomic_write_json(Path(config.value["hosts"][role]["inbox_file"]), value)
        recovery_soak.collect(config, now=START + timedelta(milliseconds=milliseconds))
    return recovery_soak.gate(config, now=START + timedelta(milliseconds=31_000))


def test_late_signed_packet_is_replayed_at_bounded_visibility_without_losing_source_time(
    setup: tuple[Config, dict[str, Signer]],
) -> None:
    result = _run_history_cell(*setup, late_dell_packet=True)
    assert _fixed_oracle(
        result,
        (
            ("historical_replay.status", InvariantOperator.EQ, "RECONSTRUCTED"),
            ("historical_replay.late_packet_count", InvariantOperator.GTE, 1),
            ("harness_classification", InvariantOperator.EQ, "PASS"),
            ("oracle_errors", InvariantOperator.EQ, []),
        ),
    ).passed, result


def test_late_packets_and_on_time_packet_share_one_millisecond_without_losing_history(
    setup: tuple[Config, dict[str, Signer]],
) -> None:
    result = _run_millisecond_collision(*setup)
    assert _fixed_oracle(
        result,
        (
            ("historical_replay.status", InvariantOperator.EQ, "RECONSTRUCTED"),
            ("historical_replay.late_packet_count", InvariantOperator.EQ, 3),
            ("historical_replay.logical_tie_count", InvariantOperator.EQ, 3),
            ("harness_classification", InvariantOperator.EQ, "PASS"),
            ("oracle_errors", InvariantOperator.EQ, []),
        ),
    ).passed, result


def test_millisecond_collision_does_not_hide_signed_no_action_violation(
    setup: tuple[Config, dict[str, Signer]],
) -> None:
    result = _run_millisecond_collision(*setup, unsafe_hidden_cra=True)
    assert _fixed_oracle(
        result,
        (
            ("historical_replay.status", InvariantOperator.EQ, "RECONSTRUCTED"),
            ("soak_status", InvariantOperator.EQ, "FAIL"),
            ("blockers", InvariantOperator.CONTAINS, "CRA_NO_ACTION_SAFETY_VIOLATED"),
        ),
    ).passed, result


def test_same_role_unsafe_then_safe_late_packets_are_both_evaluated(
    setup: tuple[Config, dict[str, Signer]],
) -> None:
    result = _run_millisecond_collision(*setup, unsafe_hidden_cra=True, unsafe_overwritten_same_millisecond=True)
    assert _fixed_oracle(
        result,
        (
            ("historical_replay.status", InvariantOperator.EQ, "RECONSTRUCTED"),
            ("historical_replay.late_packet_count", InvariantOperator.EQ, 4),
            ("soak_status", InvariantOperator.EQ, "FAIL"),
            ("blockers", InvariantOperator.CONTAINS, "CRA_NO_ACTION_SAFETY_VIOLATED"),
        ),
    ).passed, result


def test_expired_hidden_packet_remains_unverified_at_collision(
    setup: tuple[Config, dict[str, Signer]],
) -> None:
    result = _run_millisecond_collision(*setup, expired_hidden_cra=True)
    assert _fixed_oracle(
        result,
        (
            ("historical_replay.status", InvariantOperator.EQ, "UNAVAILABLE_OR_INVALID"),
            ("harness_classification", InvariantOperator.EQ, "MISSING_EVIDENCE"),
            ("formal_evaluation_performed", InvariantOperator.FALSE, False),
        ),
    ).passed, result


def test_unexpected_replay_exception_is_harness_failure_not_missing_source(
    setup: tuple[Config, dict[str, Signer]], monkeypatch: pytest.MonkeyPatch,
) -> None:
    def broken_replay(*args: Any, **kwargs: Any) -> Any:
        raise ValueError("arbitrary diagnostic that must not be exposed")

    monkeypatch.setattr(recovery_soak, "_reconstructed_samples", broken_replay)
    result = _run_millisecond_collision(*setup)
    assert _fixed_oracle(
        result,
        (
            ("historical_replay.status", InvariantOperator.EQ, "MODEL_ERROR"),
            ("historical_replay.reason_code", InvariantOperator.EQ, "RECOVERY_HISTORY_EVALUATION_FAILED"),
            ("harness_classification", InvariantOperator.EQ, "HARNESS_FAILURE"),
            ("formal_evaluation_performed", InvariantOperator.FALSE, False),
            ("soak_status", InvariantOperator.EQ, None),
        ),
    ).passed, result
    assert "arbitrary diagnostic" not in json.dumps(result)


def test_negative_control_detects_hidden_packet_skipped_inside_logical_tie(
    setup: tuple[Config, dict[str, Signer]], monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = recovery_soak._HistoryCursor.iter_through

    def broken(self: Any, sequence: object, *, on_packet: Any = None) -> Any:
        for value in original(self, sequence):
            if self.role == "cra" and value["facts"]["safety"]["command_delivery_enabled"] is True:
                continue
            if on_packet is not None:
                on_packet(value)
            yield value

    monkeypatch.setattr(recovery_soak._HistoryCursor, "iter_through", broken)
    result = _run_millisecond_collision(*setup, unsafe_hidden_cra=True)
    independent = _fixed_oracle(result, (("blockers", InvariantOperator.CONTAINS, "CRA_NO_ACTION_SAFETY_VIOLATED"),))
    assert not independent.passed
    assert independent.violations[0].invariant_id == "HISTORY-1"


@pytest.mark.parametrize("late_cra_packet", ["unsafe_effect", "short_lease"])
def test_late_hidden_packet_cannot_be_skipped_or_replayed_after_lease(
    setup: tuple[Config, dict[str, Signer]], late_cra_packet: str,
) -> None:
    result = _run_history_cell(*setup, late_cra_packet=late_cra_packet)
    if late_cra_packet == "unsafe_effect":
        expectations = (
            ("historical_replay.status", InvariantOperator.EQ, "RECONSTRUCTED"),
            ("soak_status", InvariantOperator.EQ, "FAIL"),
            ("blockers", InvariantOperator.CONTAINS, "CRA_NO_ACTION_SAFETY_VIOLATED"),
        )
    else:
        expectations = (
            ("historical_replay.status", InvariantOperator.EQ, "UNAVAILABLE_OR_INVALID"),
            ("harness_classification", InvariantOperator.EQ, "MISSING_EVIDENCE"),
            ("unknown_reasons", InvariantOperator.CONTAINS, "HISTORICAL_EVIDENCE_UNAVAILABLE"),
        )
    assert _fixed_oracle(result, expectations).passed, result


@pytest.mark.parametrize(
    ("hidden", "expected_path", "trigger", "network_recovered", "deadline", "completed"),
    [
        (
            "network", "failure_domain_sample_counts.dell_network_down", "NETWORK_DOWN",
            "2026-09-04T00:01:00.000Z", "2026-09-04T00:11:00.000Z", "2026-09-04T00:01:30.000Z",
        ),
        (
            "platform", "failure_domain_sample_counts.platform_ingest_down", "PLATFORM_DOWN",
            "2026-09-04T00:00:45.000Z", "2026-09-04T00:10:45.000Z", "2026-09-04T00:01:15.000Z",
        ),
        (
            "control", "failure_domain_sample_counts.control_path_down", "CONTROL_PATH_DOWN",
            "2026-09-04T00:00:45.000Z", "2026-09-04T00:10:45.000Z", "2026-09-04T00:01:15.000Z",
        ),
    ],
)
def test_short_gap_replays_each_hidden_source_domain(
    setup: tuple[Config, dict[str, Signer]], hidden: str, expected_path: str,
    trigger: str, network_recovered: str, deadline: str, completed: str,
) -> None:
    result = _run_history_cell(*setup, hidden=hidden)
    expected = (
        ("maximum_sample_gap_seconds", InvariantOperator.EQ, 45),
        ("historical_replay.status", InvariantOperator.EQ, "RECONSTRUCTED"),
        ("historical_replay.replay_sample_count", InvariantOperator.GTE, 12),
        (expected_path, InvariantOperator.GTE, 1),
        ("recovery.episode_count", InvariantOperator.GTE, 1),
        ("first_recovery_episode.started_at", InvariantOperator.EQ, "2026-09-04T00:00:45.000Z"),
        ("first_recovery_episode.trigger", InvariantOperator.EQ, trigger),
        ("first_recovery_episode.network_recovered_at", InvariantOperator.EQ, network_recovered),
        ("first_recovery_episode.deadline_at", InvariantOperator.EQ, deadline),
        ("first_recovery_episode.completed_at", InvariantOperator.EQ, completed),
        ("first_recovery_episode.recovery_seconds", InvariantOperator.EQ, 30.0),
        ("harness_classification", InvariantOperator.EQ, "PASS"),
    )
    assert _fixed_oracle(result, expected).passed, result


def test_short_gap_replays_hidden_no_action_safety_breach(setup: tuple[Config, dict[str, Signer]]) -> None:
    result = _run_history_cell(*setup, hidden="cra_delivery")
    assert _fixed_oracle(
        result,
        (
            ("maximum_sample_gap_seconds", InvariantOperator.EQ, 45),
            ("historical_replay.status", InvariantOperator.EQ, "RECONSTRUCTED"),
            ("soak_status", InvariantOperator.EQ, "FAIL"),
            ("blockers", InvariantOperator.CONTAINS, "CRA_NO_ACTION_SAFETY_VIOLATED"),
        ),
    ).passed, result


@pytest.mark.parametrize(
    "corrupt",
    ["missing_packet", "bad_signature", "producer_rotation", "source_time_regression", "boundary_conflict", "partial_tail"],
)
def test_short_gap_incomplete_or_modified_history_never_passes(
    setup: tuple[Config, dict[str, Signer]], corrupt: str
) -> None:
    result = _run_history_cell(*setup, corrupt=corrupt)
    if corrupt == "partial_tail":
        # A later uncommitted append is outside the frozen raw high-water.
        assert result["historical_replay"]["status"] == "RECONSTRUCTED"
        assert result["harness_classification"] == "PASS"
        assert result["historical_replay"]["sources"]["dell"]["unverified_tail_bytes"] > 0
        return
    assert _fixed_oracle(
        result,
        (
            ("historical_replay.status", InvariantOperator.EQ, "UNAVAILABLE_OR_INVALID"),
            ("harness_classification", InvariantOperator.EQ, "MISSING_EVIDENCE"),
            ("formal_evaluation_performed", InvariantOperator.FALSE, False),
            ("unknown_reasons", InvariantOperator.CONTAINS, "HISTORICAL_EVIDENCE_UNAVAILABLE"),
        ),
    ).passed, result


def test_valid_hidden_safety_violation_survives_unrelated_uncommitted_tail(
    setup: tuple[Config, dict[str, Signer]],
) -> None:
    config, signers = setup
    before = _run_history_cell(config, signers, hidden="cra_delivery")
    assert before["soak_status"] == "FAIL"
    assert "CRA_NO_ACTION_SAFETY_VIOLATED" in before["blockers"]
    with Path(config.value["history_files"]["dell"]).open("ab") as stream:
        stream.write(b"{")
    after = recovery_soak.gate(config, now=START + timedelta(seconds=180))
    assert after["soak_status"] == "FAIL"
    assert after["harness_classification"] == "SUT_FAILURE"
    assert "CRA_NO_ACTION_SAFETY_VIOLATED" in after["blockers"]
    assert after["historical_replay"]["sources"]["dell"]["unverified_tail_bytes"] == 1


def test_unsigned_hidden_safety_claim_is_not_promoted_to_formal_failure(
    setup: tuple[Config, dict[str, Signer]],
) -> None:
    result = _run_history_cell(*setup, hidden="cra_delivery", corrupt="bad_cra_signature")
    assert result["harness_classification"] == "MISSING_EVIDENCE"
    assert result["soak_status"] is None
    assert "CRA_NO_ACTION_SAFETY_VIOLATED" not in result["blockers"]


@pytest.mark.parametrize("corrupt", ["bad_later_signature", "bad_later_cra_signature"])
def test_signed_safety_violation_survives_later_required_packet_failure(
    setup: tuple[Config, dict[str, Signer]], corrupt: str,
) -> None:
    result = _run_history_cell(*setup, hidden="cra_delivery", corrupt=corrupt)
    assert result["historical_replay"]["status"] == "UNAVAILABLE_OR_INVALID"
    assert result["harness_classification"] == "SUT_FAILURE"
    assert result["soak_status"] == "FAIL"
    assert "CRA_NO_ACTION_SAFETY_VIOLATED" in result["blockers"]
    assert "HISTORICAL_EVIDENCE_UNAVAILABLE" in result["unknown_reasons"]


def test_signed_dell_effect_violation_survives_later_arena_signature_failure(
    setup: tuple[Config, dict[str, Signer]],
) -> None:
    result = _run_history_cell(*setup, hidden="dell_duplicate", corrupt="bad_later_signature")
    assert result["historical_replay"]["status"] == "UNAVAILABLE_OR_INVALID"
    assert result["soak_status"] == "FAIL"
    assert result["harness_classification"] == "SUT_FAILURE"
    assert "DELL_EFFECT_SAFETY_VIOLATED" in result["blockers"]
    assert "HISTORICAL_EVIDENCE_UNAVAILABLE" in result["unknown_reasons"]


def test_raw_signed_failure_remains_formal_fail_when_history_open_fails(
    setup: tuple[Config, dict[str, Signer]], monkeypatch: pytest.MonkeyPatch,
) -> None:
    config, signers = setup
    before = _run_history_cell(config, signers, hidden="raw_cra_delivery")
    assert before["soak_status"] == "FAIL"

    def unavailable(*_args: Any, **_kwargs: Any) -> Any:
        raise OSError("isolated history open failure")

    monkeypatch.setattr(recovery_soak, "_HistoryCursor", unavailable)
    after = recovery_soak.gate(config, now=START + timedelta(seconds=180))
    assert after["historical_replay"]["status"] == "UNAVAILABLE_OR_INVALID"
    assert after["soak_status"] == "FAIL"
    assert after["formal_evaluation_performed"] is True
    assert after["harness_classification"] == "SUT_FAILURE"
    assert "CRA_NO_ACTION_SAFETY_VIOLATED" in after["blockers"]
    assert "HISTORICAL_EVIDENCE_UNAVAILABLE" in after["unknown_reasons"]


CONFIRMED_SUT_VIOLATIONS = {
    "cra_delivery": "CRA_NO_ACTION_SAFETY_VIOLATED",
    "dell_duplicate": "DELL_EFFECT_SAFETY_VIOLATED",
    "dell_physical": "NO_ACTION_PHYSICAL_EFFECT_DETECTED",
    "dell_activation": "BOUNDED_TERMINATION_NOT_ACTIVE",
    "dell_rw_timeout": "BOUNDED_TERMINATION_NOT_ACTIVE",
}


@pytest.mark.parametrize("violation", (
    "dell_duplicate", "dell_physical", "dell_activation", "dell_rw_timeout",
))
@pytest.mark.parametrize("failure", ("none", "cra_after", "dell_before"))
def test_role_prefix_safety_survives_other_role_failure(
    setup: tuple[Config, dict[str, Signer]], violation: str, failure: str,
) -> None:
    faults = {} if failure == "none" else {"cra": 4}
    if failure == "dell_before":
        faults["dell"] = 3
    config, signers = setup
    result = _run_history_cell(config, signers, hidden=violation, history_signature_faults=faults)
    blocker = CONFIRMED_SUT_VIOLATIONS[violation]
    if failure != "dell_before":
        dell_packet = json.loads(Path(config.value["history_files"]["dell"]).read_text().splitlines()[3])
        assert recovery_soak.admit_facts(
            dell_packet, binding=config.bindings["dell"], keys=config.keys, now=START + timedelta(seconds=45),
        ) == dell_packet
    proven = failure != "dell_before"
    assert _fixed_oracle(result, (
        ("soak_status", InvariantOperator.EQ, "FAIL" if proven else None),
        ("harness_classification", InvariantOperator.EQ, "SUT_FAILURE" if proven else "MISSING_EVIDENCE"),
        ("formal_evaluation_performed", InvariantOperator.EQ, proven),
    )).passed
    assert (blocker in result["blockers"]) is proven
    if failure != "none":
        assert "HISTORICAL_EVIDENCE_UNAVAILABLE" in result["unknown_reasons"]
        failures = result["historical_replay"]["role_failures"]
        assert {item["source_role"] for item in failures} == set(faults)
        assert all(item["reason_code"] == "RECOVERY_HISTORY_SIGNATURE_INVALID" for item in failures)


@pytest.mark.parametrize("model_error", (False, True))
def test_role_prefix_aggregates_failures_and_proven_violation(
    setup: tuple[Config, dict[str, Signer]], monkeypatch: pytest.MonkeyPatch, model_error: bool,
) -> None:
    config, signers = setup
    _run_history_cell(config, signers, hidden="dell_duplicate", history_signature_faults={"cra": 4, "dell": 4, "arena": 4})
    original = recovery_soak._HistoryCursor.iter_through

    def injected(cursor: Any, sequence: object, **kwargs: Any) -> Any:
        if cursor.role == "cra" and sequence == 6:
            raise ValueError("isolated model error with private detail")
        return original(cursor, sequence, **kwargs)

    if model_error:
        monkeypatch.setattr(recovery_soak._HistoryCursor, "iter_through", injected)
    result = recovery_soak.gate(config, now=START + timedelta(seconds=180))
    assert _fixed_oracle(result, (
        ("soak_status", InvariantOperator.EQ, "FAIL"),
        ("harness_classification", InvariantOperator.EQ, "SUT_FAILURE"),
        ("formal_evaluation_performed", InvariantOperator.EQ, True),
    )).passed
    assert "DELL_EFFECT_SAFETY_VIOLATED" in result["blockers"]
    assert "HISTORICAL_EVIDENCE_UNAVAILABLE" in result["unknown_reasons"]
    failures = result["historical_replay"]["role_failures"]
    assert {item["source_role"] for item in failures} == {"cra", "dell", "arena"}
    assert all(item["source_sequence"] == (4 if model_error and item["source_role"] == "cra" else 5) for item in failures)
    assert {item["harness_classification"] for item in failures} == (
        {"MISSING_EVIDENCE", "HARNESS_FAILURE"} if model_error else {"MISSING_EVIDENCE"}
    )
    assert result["oracle_errors"] == (["RECOVERY_HISTORY_EVALUATION_FAILED"] if model_error else [])
    assert "private detail" not in json.dumps(result)


def test_confirmed_sut_safety_matrix_matches_raw_retention_registry() -> None:
    assert set(CONFIRMED_SUT_VIOLATIONS.values()) == recovery_soak._POSITIVE_SAFETY_BLOCKERS


def test_negative_control_detects_skipped_dell_prefix_after_cra_failure(
    setup: tuple[Config, dict[str, Signer]], monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = recovery_soak._HistoryCursor.iter_through
    skipped: list[int] = []

    def broken(cursor: Any, sequence: object, *, on_packet: Any = None) -> Any:
        for value in original(cursor, sequence):
            if cursor.role == "dell" and value["facts"]["effects"]["duplicate_attempt_count"] == 1:
                skipped.append(value["sequence"])
                continue
            if on_packet is not None:
                on_packet(value)
            yield value

    monkeypatch.setattr(recovery_soak._HistoryCursor, "iter_through", broken)
    result = _run_history_cell(*setup, hidden="dell_duplicate", history_signature_faults={"cra": 4})
    independent = _fixed_oracle(result, (("soak_status", InvariantOperator.EQ, "FAIL"),))
    assert skipped == [4]
    assert not independent.passed and len(independent.violations) == 1 and not independent.errors


def test_positive_safety_sinks_match_retention_registry_on_both_replay_paths() -> None:
    expected = recovery_soak._POSITIVE_SAFETY_BLOCKERS
    for function in (recovery_soak.evaluate, recovery_soak._reconstructed_samples):
        tree = ast.parse(inspect.getsource(function))
        registered = {
            call.args[0].value
            for call in ast.walk(tree)
            if isinstance(call, ast.Call)
            and isinstance(call.func, ast.Attribute)
            and isinstance(call.func.value, ast.Name)
            and call.func.value.id == "positive_blockers"
            and call.func.attr == "add"
            and len(call.args) == 1
            and isinstance(call.args[0], ast.Constant)
            and isinstance(call.args[0].value, str)
        }
        assert registered == expected, function.__name__


@pytest.mark.parametrize("hidden,corrupt", [
    ("dell_activation", "bad_dell_signature"),
    ("dell_activation_without_target", None),
])
def test_unverified_or_targetless_activation_is_not_promoted_to_sut_failure(
    setup: tuple[Config, dict[str, Signer]], hidden: str, corrupt: str | None,
) -> None:
    result = _run_history_cell(*setup, hidden=hidden, corrupt=corrupt)
    assert "BOUNDED_TERMINATION_NOT_ACTIVE" not in result["blockers"]
    assert result["soak_status"] != "FAIL"


def test_raw_signed_activation_remains_fail_when_other_raw_source_and_history_are_missing(
    setup: tuple[Config, dict[str, Signer]], monkeypatch: pytest.MonkeyPatch,
) -> None:
    config, signers = setup
    _run_history_cell(config, signers, hidden="raw_dell_activation")
    Path(config.value["hosts"]["arena"]["inbox_file"]).unlink()
    recovery_soak.collect(config, now=START + timedelta(seconds=195))
    raw_head, raw_rows = recovery_soak.snapshot(config)
    assert raw_head["sample_count"] > 0
    raw = recovery_soak.evaluate(raw_rows, config=config, now=START + timedelta(seconds=195))
    assert raw["harness_classification"] == "MISSING_EVIDENCE"
    assert "BOUNDED_TERMINATION_NOT_ACTIVE" in raw["blockers"]

    def unavailable(*_args: Any, **_kwargs: Any) -> Any:
        raise OSError("isolated history open failure")

    monkeypatch.setattr(recovery_soak, "_HistoryCursor", unavailable)
    result = recovery_soak.gate(config, now=START + timedelta(seconds=195))
    assert result["soak_status"] == "FAIL"
    assert "BOUNDED_TERMINATION_NOT_ACTIVE" in result["blockers"]
    assert "HISTORICAL_EVIDENCE_UNAVAILABLE" in result["unknown_reasons"]


@pytest.mark.parametrize("violation", CONFIRMED_SUT_VIOLATIONS)
@pytest.mark.parametrize("interruption", ("later_bad_signature", "history_open", "pending_tail", "capacity"))
def test_confirmed_sut_violation_retains_formal_fail_across_unrelated_evidence_failure(
    setup: tuple[Config, dict[str, Signer]], monkeypatch: pytest.MonkeyPatch,
    violation: str, interruption: str,
) -> None:
    config, signers = setup
    if interruption == "capacity":
        config.value["maximum_evidence_bytes"] = 1024 * 1024
    if interruption == "later_bad_signature":
        after = _run_history_cell(config, signers, hidden=violation, corrupt="bad_signature")
    else:
        before = _run_history_cell(config, signers, hidden="raw_" + violation)
        assert before["soak_status"] == "FAIL"
        if interruption == "history_open":
            def unavailable(*_args: Any, **_kwargs: Any) -> Any:
                raise OSError("isolated history open failure")

            monkeypatch.setattr(recovery_soak, "_HistoryCursor", unavailable)
        elif interruption == "pending_tail":
            with monkeypatch.context() as patch:
                patch.setattr(
                    recovery_soak, "_append_line",
                    lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError("isolated pending append")),
                )
                with pytest.raises(OSError, match="isolated pending append"):
                    recovery_soak.collect(config, now=START + timedelta(seconds=195))
            pending = json.loads(Path(config.value["state_file"]).read_bytes())["pending_sample"]
            with Path(config.value["evidence_file"]).open("ab") as stream:
                stream.write(_serialized_line(pending)[:19])
        after = recovery_soak.gate(config, now=START + timedelta(seconds=180))
    blocker = CONFIRMED_SUT_VIOLATIONS[violation]
    assert blocker in after["blockers"]
    assert after["soak_status"] == "FAIL"
    assert after["formal_evaluation_performed"] is True
    assert after["harness_classification"] == "SUT_FAILURE"
    if interruption in {"later_bad_signature", "history_open"}:
        assert "HISTORICAL_EVIDENCE_UNAVAILABLE" in after["unknown_reasons"]
    elif interruption == "pending_tail":
        assert "EVIDENCE_APPEND_PENDING" in after["unknown_reasons"]
    else:
        assert "EVIDENCE_CAPACITY_NOT_PROVEN" in after["unknown_reasons"]


@pytest.mark.parametrize("violation", CONFIRMED_SUT_VIOLATIONS)
def test_confirmed_sut_violation_retains_formal_fail_after_reconstructed_missing_evidence(
    setup: tuple[Config, dict[str, Signer]], violation: str,
) -> None:
    result = _run_history_cell(*setup, hidden=violation, late_arena_unknown=True)
    assert result["historical_replay"]["status"] == "RECONSTRUCTED"
    assert result["soak_status"] == "FAIL"
    assert result["harness_classification"] == "SUT_FAILURE"
    assert CONFIRMED_SUT_VIOLATIONS[violation] in result["blockers"]
    assert result["unknown_reasons"]


@pytest.mark.parametrize("violation", CONFIRMED_SUT_VIOLATIONS)
def test_confirmed_sut_violation_retains_formal_fail_after_final_sample_stales(
    setup: tuple[Config, dict[str, Signer]], violation: str,
) -> None:
    config, signers = setup
    before = _run_history_cell(config, signers, hidden="raw_" + violation)
    assert before["soak_status"] == "FAIL"
    after = recovery_soak.gate(config, now=START + timedelta(seconds=240))
    assert after["historical_replay"]["status"] == "RECONSTRUCTED"
    assert after["soak_status"] == "FAIL"
    assert after["harness_classification"] == "SUT_FAILURE"
    assert CONFIRMED_SUT_VIOLATIONS[violation] in after["blockers"]
    assert "FINAL_SAMPLE_MISSING_OR_STALE" in after["unknown_reasons"]


@pytest.mark.parametrize("missing", ("platform_unknown", "stale_final"))
def test_missing_evidence_without_proven_safety_violation_stays_unclassified(
    setup: tuple[Config, dict[str, Signer]], missing: str,
) -> None:
    config, signers = setup
    if missing == "platform_unknown":
        result = _run_history_cell(config, signers, late_arena_unknown=True)
    else:
        _run_history_cell(config, signers)
        result = recovery_soak.gate(config, now=START + timedelta(seconds=240))
    assert result["historical_replay"]["status"] == "RECONSTRUCTED"
    assert result["soak_status"] is None
    assert result["harness_classification"] == "MISSING_EVIDENCE"
    assert not set(result["blockers"]) & recovery_soak._POSITIVE_SAFETY_BLOCKERS


@pytest.mark.parametrize("keep_intermediate_raw", (True, False))
def test_dell_counter_regression_rebound_is_not_proof_of_new_effect(
    setup: tuple[Config, dict[str, Signer]], keep_intermediate_raw: bool,
) -> None:
    result = _run_history_cell(
        *setup, dell_attempt_counts={3: 0},
        keep_intermediate_raw=keep_intermediate_raw, late_arena_unknown=True,
    )
    assert result["historical_replay"]["status"] == "RECONSTRUCTED"
    assert "DELL_EFFECT_COUNTER_REGRESSION" in result["blockers"]
    assert "NO_ACTION_PHYSICAL_EFFECT_DETECTED" not in result["blockers"]
    assert result["soak_status"] is None
    assert result["harness_classification"] == "MISSING_EVIDENCE"
    assert result["unknown_reasons"]
    if keep_intermediate_raw:
        _head, committed = recovery_soak.snapshot(setup[0])
        raw = recovery_soak.evaluate(committed, config=setup[0], now=START + timedelta(seconds=180))
        assert "DELL_EFFECT_COUNTER_REGRESSION" in raw["blockers"]
        assert "NO_ACTION_PHYSICAL_EFFECT_DETECTED" not in raw["blockers"]
        assert raw["soak_status"] is None


def test_signed_counter_exceeding_pre_regression_high_water_remains_proven_effect(
    setup: tuple[Config, dict[str, Signer]],
) -> None:
    # The raw boundaries are both 1. Only the signed hidden packet at 60s
    # proves 2; a later invalid arena packet prevents normal reconstruction.
    result = _run_history_cell(
        *setup, dell_attempt_counts={3: 0, 4: 2}, corrupt="bad_later_signature",
    )
    assert result["historical_replay"]["status"] == "UNAVAILABLE_OR_INVALID"
    assert "NO_ACTION_PHYSICAL_EFFECT_DETECTED" in result["blockers"]
    assert result["soak_status"] == "FAIL"
    assert "HISTORICAL_EVIDENCE_UNAVAILABLE" in result["unknown_reasons"]


def test_unverified_counter_regression_does_not_create_effect(
    setup: tuple[Config, dict[str, Signer]],
) -> None:
    result = _run_history_cell(
        *setup, dell_attempt_counts={3: 0}, corrupt="bad_dell_signature",
    )
    assert "NO_ACTION_PHYSICAL_EFFECT_DETECTED" not in result["blockers"]
    assert result["soak_status"] is None


def _assert_first_dell_packet_is_source_valid_but_raw_stale(config: Config) -> None:
    first = json.loads(Path(config.value["history_files"]["dell"]).read_text(encoding="utf-8").splitlines()[0])
    recovery_soak.admit_facts(
        first, binding=config.bindings["dell"], keys=config.keys,
        now=recovery_soak.timestamp(first["observed_at"]),
    )
    with pytest.raises(ValueError, match="RECOVERY_ENVELOPE_STALE"):
        recovery_soak.admit_facts(first, binding=config.bindings["dell"], keys=config.keys, now=START)


def test_expired_initial_dell_counter_cannot_create_false_effect(
    setup: tuple[Config, dict[str, Signer]],
) -> None:
    result = _run_history_cell(
        *setup, dell_attempt_counts={0: 0}, stale_first_dell_packet=True,
        late_arena_unknown=True,
    )
    _assert_first_dell_packet_is_source_valid_but_raw_stale(setup[0])
    assert result["historical_replay"]["status"] == "RECONSTRUCTED"
    assert "NO_ACTION_PHYSICAL_EFFECT_DETECTED" not in result["blockers"]
    assert result["soak_status"] is None
    assert result["harness_classification"] == "MISSING_EVIDENCE"


def test_expired_high_initial_dell_counter_cannot_hide_later_effect(
    setup: tuple[Config, dict[str, Signer]],
) -> None:
    result = _run_history_cell(
        *setup, dell_attempt_counts={0: 10, 4: 2}, stale_first_dell_packet=True,
        corrupt="bad_later_signature",
    )
    _assert_first_dell_packet_is_source_valid_but_raw_stale(setup[0])
    assert result["historical_replay"]["status"] == "UNAVAILABLE_OR_INVALID"
    assert "NO_ACTION_PHYSICAL_EFFECT_DETECTED" in result["blockers"]
    assert result["soak_status"] == "FAIL"
    assert "HISTORICAL_EVIDENCE_UNAVAILABLE" in result["unknown_reasons"]


@pytest.mark.parametrize("hidden", ["network", "platform", "control"])
def test_negative_control_detects_shared_recovery_transition_defect(
    setup: tuple[Config, dict[str, Signer]], monkeypatch: pytest.MonkeyPatch, hidden: str
) -> None:
    original = RecoveryWindow.observe

    def broken(self: RecoveryWindow, item: Any) -> None:
        # Both collector and replay use this same broken transition.  Their
        # checkpoint comparison can agree while the independent oracle fails.
        field = {"network": "network", "platform": "platform", "control": "control_path"}[hidden]
        original(self, replace(item, **{field: "UP"}) if getattr(item, field) == "DOWN" else item)

    monkeypatch.setattr(RecoveryWindow, "observe", broken)
    result = _run_history_cell(*setup, hidden=hidden)
    assert "LIVE_CHECKPOINT_REPLAY_MISMATCH" not in result["oracle_errors"]
    independent = _fixed_oracle(result, (("recovery.episode_count", InvariantOperator.GTE, 1),))
    assert not independent.passed
    assert independent.violations[0].invariant_id == "HISTORY-1"


def test_history_cursor_scales_past_old_global_selection_limit(setup: tuple[Config, dict[str, Signer]]) -> None:
    config, signers = setup
    path = Path(config.value["state_file"]).with_name("dell-history.jsonl")
    config.value["history_files"] = {"dell": str(path)}
    with path.open("w", encoding="utf-8") as stream:
        for index in range(4_101):
            observed = packet(config, signers, "dell", index * 15)
            unsigned = {key: value for key, value in observed.items() if key not in {"signature", "payload_sha256"}}
            unsigned["sequence"] = index + 1
            stream.write(json.dumps(signers["dell"].sign(unsigned)) + "\n")
    cursor = recovery_soak._HistoryCursor(config, "dell")
    try:
        for sequence in range(1, 4_102):
            assert len(cursor.through(sequence)) == 1
        assert cursor.finish()["selected_packets"] == 4_101
    finally:
        cursor.close()
    late_start = recovery_soak._HistoryCursor(config, "dell")
    try:
        assert late_start.align(4_101)["sequence"] == 4_101
        assert late_start.finish()["selected_packets"] == 0
    finally:
        late_start.close()


def test_formal_replay_streams_more_than_4096_packets_in_one_raw_interval(
    setup: tuple[Config, dict[str, Signer]],
) -> None:
    config, signers = setup
    root = Path(config.value["state_file"]).parent
    config.value["schema"] = HISTORY_CONFIG_SCHEMA
    config.value["history_files"] = {role: str(root / f"{role}-history.jsonl") for role in ("dell", "arena", "cra")}
    last_second = 4_110
    first: dict[str, dict[str, Any]] = {}
    last: dict[str, dict[str, Any]] = {}
    for role in ("dell", "arena", "cra"):
        step = 1 if role == "dell" else 15
        with Path(config.value["history_files"][role]).open("w", encoding="utf-8") as stream:
            for sequence, second in enumerate(range(0, last_second + 1, step), start=1):
                observed = packet(config, signers, role, second)
                unsigned = {key: value for key, value in observed.items() if key not in {"signature", "payload_sha256"}}
                unsigned["sequence"] = sequence
                signed = signers[role].sign(unsigned)
                stream.write(json.dumps(signed) + "\n")
                if second == 0:
                    first[role] = signed
                if second == last_second:
                    last[role] = signed
    for second, inputs in ((0, first), (last_second, last)):
        for role, value in inputs.items():
            atomic_write_json(Path(config.value["hosts"][role]["inbox_file"]), value)
        recovery_soak.collect(config, now=START + timedelta(seconds=second))
    result = recovery_soak.gate(config, now=START + timedelta(seconds=last_second + 1))
    assert result["historical_replay"]["status"] == "RECONSTRUCTED"
    assert result["historical_replay"]["sources"]["dell"]["selected_packets"] == last_second
    assert result["historical_replay"]["replay_sample_count"] > 4_096
    assert "COLLECTOR_GAP_UNRECONSTRUCTED" not in result["unknown_reasons"]


def test_negative_control_detects_skipped_history_reconstruction(
    setup: tuple[Config, dict[str, Signer]], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(recovery_soak, "_reconstructed_samples", lambda samples, *_args, **_kwargs: samples)
    result = _run_history_cell(*setup, hidden="cra_delivery")
    independent = _fixed_oracle(result, (("blockers", InvariantOperator.CONTAINS, "CRA_NO_ACTION_SAFETY_VIOLATED"),))
    assert not independent.passed
    assert independent.violations[0].invariant_id == "HISTORY-1"


def test_negative_control_detects_shared_deadline_extension(
    setup: tuple[Config, dict[str, Signer]], monkeypatch: pytest.MonkeyPatch
) -> None:
    original = RecoveryWindow.observe

    def broken(self: RecoveryWindow, item: Any) -> None:
        # Collector and gate can share this drift and agree on the checkpoint.
        if self.active is not None and self.active.deadline_at is not None:
            self.active.deadline_at += timedelta(seconds=15)
        original(self, item)

    monkeypatch.setattr(RecoveryWindow, "observe", broken)
    result = _run_history_cell(*setup, hidden="network")
    assert "LIVE_CHECKPOINT_REPLAY_MISMATCH" not in result["oracle_errors"]
    independent = _fixed_oracle(
        result, (("first_recovery_episode.deadline_at", InvariantOperator.EQ, "2026-09-04T00:11:00.000Z"),)
    )
    assert not independent.passed
    assert independent.violations[0].invariant_id == "HISTORY-1"
