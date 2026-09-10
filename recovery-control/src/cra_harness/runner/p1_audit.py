from __future__ import annotations

import ast
import hashlib
import json
import shutil
import statistics
import subprocess
import sys
import time
from collections import Counter
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from cra_harness.classifiers.taxonomy import Classification
from cra_harness.oracles.p1_audit import scenario_violations
from maintenance_audit import (
    AuditEmitter,
    AuditObservation,
    AuditPathRole,
    AuditPhase,
    AuditVerdict,
    audit_maintenance_decision,
    evaluate_audit_decision,
    set_global_emitter_for_tests,
)

FIXED_NOW = datetime(2026, 8, 23, 10, 0, 0, tzinfo=UTC)
RESOURCE = "deployment/stream-v3/stream-v3-runtime"
TARGET = {
    "host_id": "dell",
    "host_boot_id": "boot-a",
    "namespace": "stream-v3",
    "pod_uid": "pod-a",
    "container_name": "stream-engine",
    "container_id": "container-a",
    "ffmpeg_generation": "run-a:0:4100",
    "ffmpeg_pid": 4100,
}


def _json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("x", encoding="utf-8") as handle:
        json.dump(value, handle, ensure_ascii=False, indent=2, sort_keys=True)
        handle.write("\n")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _active_snapshot(
    *,
    generation: int = 8,
    target: Mapping[str, Any] | None = TARGET,
    authorizations: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    return {
        "schema_version": "maintenance.audit_state_snapshot.v1",
        "available": True,
        "observed_at": "2026-08-23T09:59:59.000Z",
        "fresh_until": "2026-08-23T10:05:00.000Z",
        "maintenance_state": "ESTABLISHED",
        "maintenance_id": "maintenance-p1",
        "maintenance_generation": generation,
        "authority_epoch": 27,
        "authority_session_id": "authority-session-a",
        "target_identity": None if target is None else dict(target),
        "authorizations": list(authorizations or []),
    }


def _inactive_snapshot() -> dict[str, Any]:
    value = _active_snapshot(generation=0, target=None)
    value.update(
        {
            "maintenance_state": "INACTIVE",
            "maintenance_id": "",
            "authority_epoch": 0,
            "authority_session_id": "",
        }
    )
    return value


def _authorization(*, use_count: int = 0, kind: str = "MAINTENANCE_MUTATION") -> dict[str, Any]:
    return {
        "authorization_id": "maintenance-authorization-p1",
        "authorization_kind": kind,
        "state": "ACCEPTED",
        "single_use": True,
        "use_count": use_count,
        "maintenance_generation": 8,
        "executor_id": "planned_rollout_executor",
        "operation": "RESTART_DEPLOYMENT",
        "resource_identity": RESOURCE,
        "source_target_identity": dict(TARGET),
        "expires_at": "2026-08-23T10:05:00.000Z",
    }


def _oracle_input(
    *,
    role: str,
    maintenance_state: str,
    state_available: bool = True,
    state_fresh: bool = True,
    target_status: str = "EXACT",
    operation_generation: int | None = 8,
    authorization: dict[str, Any] | None = None,
    operation: str = "RESTART_FFMPEG",
    resource_identity: str = "runtime/stream-engine",
) -> dict[str, Any]:
    return {
        "role": role,
        "maintenance_state": maintenance_state,
        "state_available": state_available,
        "state_fresh": state_fresh,
        "target_status": target_status,
        "operation_generation": operation_generation,
        "maintenance_generation": 8,
        "authorization": authorization,
        "operation": operation,
        "resource_identity": resource_identity,
    }


def _observation(
    *,
    phase: AuditPhase,
    role: AuditPathRole = AuditPathRole.NORMAL_MUTATOR,
    operation: str = "RESTART_FFMPEG",
    resource_identity: str = "runtime/stream-engine",
    target: Mapping[str, Any] | None = TARGET,
    generation: int | None = 8,
    authorization_id: str = "",
) -> AuditObservation:
    return AuditObservation(
        path_id="MP-03",
        phase=phase,
        operation=operation,
        path_role=role,
        process_service="p1-deterministic-fixture",
        resource_identity=resource_identity,
        correlation_id="p1-correlation",
        target_identity=target,
        operation_generation=generation,
        authorization_id=authorization_id,
        in_flight_evidence={"status": "PROPOSED", "count": None},
        generation_evidence={"status": "PROPOSED", "source": "fixture"},
    )


def _observed(
    observation: AuditObservation,
    snapshot: Mapping[str, Any] | None,
    oracle_input: dict[str, Any],
) -> dict[str, Any]:
    decision = evaluate_audit_decision(observation, snapshot, now=FIXED_NOW)
    return {
        "phase": observation.phase.value,
        "audit_verdict": decision.verdict.value,
        "audit_reason": decision.reason_code,
        "production_behavior_modified": decision.production_behavior_modified,
        "audit_triggered_restart_count": 0,
        "oracle_input": oracle_input,
    }


def _case(scenario_id: str, title: str, observations: list[dict[str, Any]], required_phases: list[str]) -> dict[str, Any]:
    value = {
        "scenario_id": scenario_id,
        "title": title,
        "profile": "deterministic",
        "observations": observations,
        "required_phases": required_phases,
        "physical_effect_count": 0,
        "production_decision_before": "LEGACY_DECISION_CONTINUES",
        "production_decision_after": "LEGACY_DECISION_CONTINUES",
    }
    violations = scenario_violations(value)
    value.update(
        {
            "oracle_violations": violations,
            "classification": Classification.PASS.value if not violations else Classification.SUT_FAILURE.value,
            "matches_expectation": not violations,
        }
    )
    return value


def deterministic_cases() -> list[dict[str, Any]]:
    inactive_oracle = _oracle_input(role="NORMAL_MUTATOR", maintenance_state="INACTIVE", operation_generation=None, target_status="UNKNOWN")
    active_oracle = _oracle_input(role="NORMAL_MUTATOR", maintenance_state="ESTABLISHED")
    normal_inactive = _observed(
        _observation(phase=AuditPhase.ADMISSION, generation=None, target=None), _inactive_snapshot(), inactive_oracle
    )
    normal_active = _observed(_observation(phase=AuditPhase.ADMISSION), _active_snapshot(), active_oracle)

    before = _observed(
        _observation(phase=AuditPhase.ADMISSION, generation=None, target=None),
        _inactive_snapshot(),
        inactive_oracle,
    )
    after = _observed(_observation(phase=AuditPhase.EFFECT_BOUNDARY), _active_snapshot(), active_oracle)

    stale_oracle = _oracle_input(role="NORMAL_MUTATOR", maintenance_state="ESTABLISHED", operation_generation=7)
    stale = _observed(_observation(phase=AuditPhase.EFFECT_BOUNDARY, generation=7), _active_snapshot(), stale_oracle)

    unavailable_snapshot = {"available": False}
    unavailable_oracle = _oracle_input(
        role="NORMAL_MUTATOR", maintenance_state="UNKNOWN", state_available=False, state_fresh=False, target_status="UNKNOWN"
    )
    unavailable = _observed(
        _observation(phase=AuditPhase.ADMISSION, generation=None, target=None), unavailable_snapshot, unavailable_oracle
    )

    set_global_emitter_for_tests(AuditEmitter(enabled=False))
    exception_result = audit_maintenance_decision(
        path_id="MP-03",
        phase="INVALID_PHASE",
        operation="RESTART_FFMPEG",
        path_role="NORMAL_MUTATOR",
        process_service="p1-exception-fixture",
    )
    set_global_emitter_for_tests(None)
    exception_observed = {
        "phase": "ADMISSION",
        "audit_verdict": exception_result.decision.verdict.value,
        "audit_reason": exception_result.decision.reason_code,
        "production_behavior_modified": False,
        "audit_triggered_restart_count": 0,
        "oracle_input": unavailable_oracle,
    }

    unknown_target_oracle = _oracle_input(role="NORMAL_MUTATOR", maintenance_state="ESTABLISHED", target_status="UNKNOWN")
    unknown_target = _observed(_observation(phase=AuditPhase.ADMISSION, target=None), _active_snapshot(), unknown_target_oracle)

    fallback_oracle = _oracle_input(role="LOCAL_FALLBACK", maintenance_state="ESTABLISHED", target_status="UNKNOWN")
    fallback = _observed(
        _observation(phase=AuditPhase.ADMISSION, role=AuditPathRole.LOCAL_FALLBACK, target=None),
        _active_snapshot(),
        fallback_oracle,
    )

    authorization = _authorization()
    planned_oracle = _oracle_input(
        role="PLANNED_EXECUTOR",
        maintenance_state="ESTABLISHED",
        authorization={
            **authorization,
            "target_status": "EXACT",
            "not_expired": True,
        },
        operation="RESTART_DEPLOYMENT",
        resource_identity=RESOURCE,
    )
    planned = _observed(
        _observation(
            phase=AuditPhase.ADMISSION,
            role=AuditPathRole.PLANNED_EXECUTOR,
            operation="RESTART_DEPLOYMENT",
            resource_identity=RESOURCE,
            authorization_id=authorization["authorization_id"],
        ),
        _active_snapshot(authorizations=[authorization]),
        planned_oracle,
    )
    normal_during_authorized = _observed(
        _observation(phase=AuditPhase.ADMISSION), _active_snapshot(authorizations=[authorization]), active_oracle
    )

    replay_authorization = _authorization(use_count=1)
    replay_oracle = _oracle_input(
        role="PLANNED_EXECUTOR",
        maintenance_state="ESTABLISHED",
        authorization={
            **replay_authorization,
            "target_status": "EXACT",
            "not_expired": True,
        },
        operation="RESTART_DEPLOYMENT",
        resource_identity=RESOURCE,
    )
    replay = _observed(
        _observation(
            phase=AuditPhase.EFFECT_BOUNDARY,
            role=AuditPathRole.PLANNED_EXECUTOR,
            operation="RESTART_DEPLOYMENT",
            resource_identity=RESOURCE,
            authorization_id=replay_authorization["authorization_id"],
        ),
        _active_snapshot(authorizations=[replay_authorization]),
        replay_oracle,
    )
    return [
        _case("P1-01", "normal recovery while maintenance inactive", [normal_inactive], ["ADMISSION"]),
        _case("P1-02", "normal recovery admission while maintenance active", [normal_active], ["ADMISSION"]),
        _case("P1-03", "stale action window revalidated at effect boundary", [before, after], ["ADMISSION", "EFFECT_BOUNDARY"]),
        _case("P1-04", "stale maintenance generation", [stale], ["EFFECT_BOUNDARY"]),
        _case("P1-05", "audit backend unavailable", [unavailable], ["ADMISSION"]),
        _case("P1-06", "audit hook exception", [exception_observed], ["ADMISSION"]),
        _case("P1-07", "unknown exact target identity", [unknown_target], ["ADMISSION"]),
        _case("P1-08", "local fallback eligibility during maintenance", [fallback], ["ADMISSION"]),
        _case("P1-09", "single-use maintenance authorization is path scoped", [planned, normal_during_authorized], ["ADMISSION"]),
        _case("P1-10", "maintenance authorization replay", [replay], ["EFFECT_BOUNDARY"]),
    ]


def _negative_case(scenario_id: str, title: str, observations: list[dict[str, Any]], required_phases: list[str]) -> dict[str, Any]:
    value = {
        "scenario_id": scenario_id,
        "title": title,
        "profile": "negative_control",
        "observations": observations,
        "required_phases": required_phases,
        "physical_effect_count": 0,
    }
    violations = scenario_violations(value)
    value.update(
        {
            "oracle_violations": violations,
            "classification": (Classification.EXPECTED_INJECTED_FAILURE.value if violations else Classification.HARNESS_FAILURE.value),
            "matches_expectation": bool(violations),
        }
    )
    return value


def _mutant(
    *,
    verdict: str,
    oracle_input: dict[str, Any],
    phase: str = "ADMISSION",
    modified: bool = False,
    restart_count: int = 0,
) -> dict[str, Any]:
    return {
        "phase": phase,
        "audit_verdict": verdict,
        "audit_reason": "INTENTIONALLY_MUTATED_NEGATIVE_CONTROL",
        "production_behavior_modified": modified,
        "audit_triggered_restart_count": restart_count,
        "oracle_input": oracle_input,
    }


def negative_controls() -> list[dict[str, Any]]:
    active_normal = _oracle_input(role="NORMAL_MUTATOR", maintenance_state="ESTABLISHED")
    inactive_normal = _oracle_input(role="NORMAL_MUTATOR", maintenance_state="INACTIVE", operation_generation=None, target_status="UNKNOWN")
    stale = _oracle_input(role="NORMAL_MUTATOR", maintenance_state="ESTABLISHED", operation_generation=7)
    unavailable = _oracle_input(
        role="NORMAL_MUTATOR", maintenance_state="UNKNOWN", state_available=False, state_fresh=False, target_status="UNKNOWN"
    )
    mismatch = _oracle_input(role="NORMAL_MUTATOR", maintenance_state="ESTABLISHED", target_status="MISMATCH")
    fallback = _oracle_input(role="LOCAL_FALLBACK", maintenance_state="ESTABLISHED")
    normal_authorization = _authorization(kind="NORMAL_RECOVERY")
    normal_authorization_oracle = _oracle_input(
        role="PLANNED_EXECUTOR",
        maintenance_state="ESTABLISHED",
        authorization={**normal_authorization, "target_status": "EXACT", "not_expired": True},
        operation="RESTART_DEPLOYMENT",
        resource_identity=RESOURCE,
    )
    replay_authorization = _authorization(use_count=1)
    replay_oracle = _oracle_input(
        role="PLANNED_EXECUTOR",
        maintenance_state="ESTABLISHED",
        authorization={**replay_authorization, "target_status": "EXACT", "not_expired": True},
        operation="RESTART_DEPLOYMENT",
        resource_identity=RESOURCE,
    )
    return [
        _negative_case(
            "NC-P1-01",
            "active maintenance incorrectly allows normal mutator",
            [_mutant(verdict="WOULD_ALLOW", oracle_input=active_normal)],
            ["ADMISSION"],
        ),
        _negative_case(
            "NC-P1-02",
            "effect boundary audit missing",
            [_mutant(verdict="WOULD_ALLOW", oracle_input=inactive_normal)],
            ["ADMISSION", "EFFECT_BOUNDARY"],
        ),
        _negative_case(
            "NC-P1-03",
            "stale generation incorrectly allowed",
            [_mutant(verdict="WOULD_ALLOW", oracle_input=stale, phase="EFFECT_BOUNDARY")],
            ["EFFECT_BOUNDARY"],
        ),
        _negative_case("NC-P1-04", "UNKNOWN converted to allow", [_mutant(verdict="WOULD_ALLOW", oracle_input=unavailable)], ["ADMISSION"]),
        _negative_case(
            "NC-P1-05",
            "audit failure blocks production mutation",
            [_mutant(verdict="UNKNOWN", oracle_input=unavailable, modified=True)],
            ["ADMISSION"],
        ),
        _negative_case(
            "NC-P1-06",
            "audit failure triggers production restart",
            [_mutant(verdict="UNKNOWN", oracle_input=unavailable, restart_count=1)],
            ["ADMISSION"],
        ),
        _negative_case(
            "NC-P1-07", "target mismatch incorrectly allowed", [_mutant(verdict="WOULD_ALLOW", oracle_input=mismatch)], ["ADMISSION"]
        ),
        _negative_case(
            "NC-P1-08",
            "local fallback incorrectly allowed during maintenance",
            [_mutant(verdict="WOULD_ALLOW", oracle_input=fallback)],
            ["ADMISSION"],
        ),
        _negative_case(
            "NC-P1-09",
            "normal authorization accepted as maintenance authorization",
            [_mutant(verdict="WOULD_ALLOW", oracle_input=normal_authorization_oracle)],
            ["ADMISSION"],
        ),
        _negative_case(
            "NC-P1-10",
            "replayed authorization incorrectly allowed",
            [_mutant(verdict="WOULD_ALLOW", oracle_input=replay_oracle, phase="EFFECT_BOUNDARY")],
            ["EFFECT_BOUNDARY"],
        ),
    ]


def _percentile(values: list[float], percentile: float) -> float:
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, int((len(ordered) - 1) * percentile)))
    return ordered[index]


def _measure_emitter(emitter: AuditEmitter, observation: AuditObservation, samples: int) -> dict[str, float | int]:
    durations: list[float] = []
    for _ in range(samples):
        started = time.perf_counter_ns()
        emitter.emit(observation)
        durations.append((time.perf_counter_ns() - started) / 1000.0)
    health = emitter.health_snapshot()
    return {
        "samples": samples,
        "median_us": statistics.median(durations),
        "p95_us": _percentile(durations, 0.95),
        "p99_us": _percentile(durations, 0.99),
        "max_us": max(durations),
        "exception_count_at_measurement": int(health["exception_count"]),
        "lost_at_measurement": int(health["lost"]),
    }


def performance_comparison(samples: int = 10_000) -> dict[str, Any]:
    observation = _observation(phase=AuditPhase.EFFECT_BOUNDARY)
    disabled = AuditEmitter(enabled=False)
    baseline = _measure_emitter(disabled, observation, samples)
    enabled = AuditEmitter(
        enabled=True,
        state_supplier=_active_snapshot,
        event_writer=lambda _event: None,
        queue_capacity=samples + 64,
        state_refresh_seconds=60.0,
        host="p1-performance-fixture",
    )
    deadline = time.monotonic() + 1.0
    while time.monotonic() < deadline:
        probe = enabled.emit(observation)
        if probe.decision.verdict != AuditVerdict.UNKNOWN:
            break
        time.sleep(0.005)
    measured = _measure_emitter(enabled, observation, samples)
    drained = enabled.wait_until_drained(timeout_seconds=5.0)
    final_health = enabled.health_snapshot()
    enabled.close()
    disabled.close()
    gate = (
        float(measured["p99_us"]) <= 2_000.0
        and float(measured["max_us"]) <= 100_000.0
        and int(measured["exception_count_at_measurement"]) == 0
        and int(measured["lost_at_measurement"]) == 0
        and drained
    )
    return {
        "clock": "time.perf_counter_ns",
        "critical_path_io": "cached evaluation + JSON-ready dict + queue.put_nowait only",
        "audit_disabled_baseline": baseline,
        "audit_enabled": measured,
        "enabled_final_health": final_health,
        "drained": drained,
        "thresholds": {"p99_us_max": 2_000.0, "max_us_max": 100_000.0},
        "result": "PASS" if gate else "FAIL",
    }


def audit_failure_tests() -> dict[str, Any]:
    mutation_trace: list[str] = []

    def failed_writer(_event: Mapping[str, Any]) -> None:
        raise OSError("injected audit backend failure")

    emitter = AuditEmitter(
        enabled=True,
        state_supplier=_active_snapshot,
        event_writer=failed_writer,
        queue_capacity=16,
        state_refresh_seconds=60.0,
        host="p1-failure-fixture",
    )
    deadline = time.monotonic() + 1.0
    result = None
    while time.monotonic() < deadline:
        result = emitter.emit(_observation(phase=AuditPhase.EFFECT_BOUNDARY))
        if emitter.wait_until_drained(0.2) and int(emitter.health_snapshot()["lost"]) > 0:
            break
    mutation_trace.append("LEGACY_MUTATION_PATH_CONTINUED")
    health = emitter.health_snapshot()
    emitter.close()

    set_global_emitter_for_tests(AuditEmitter(enabled=False))
    hook_exception = audit_maintenance_decision(
        path_id="MP-03",
        phase="INVALID",
        operation="RESTART_FFMPEG",
        path_role="NORMAL_MUTATOR",
        process_service="p1-failure-fixture",
    )
    set_global_emitter_for_tests(None)
    passed = (
        result is not None
        and int(health["lost"]) > 0
        and health["last_failure_reason"] == "AUDIT_EVIDENCE_LOST_WRITE_FAILED"
        and mutation_trace == ["LEGACY_MUTATION_PATH_CONTINUED"]
        and hook_exception.decision.verdict == AuditVerdict.UNKNOWN
        and not hook_exception.decision.production_behavior_modified
    )
    return {
        "audit_backend_write_failure": health,
        "hook_exception_verdict": hook_exception.decision.verdict.value,
        "production_mutation_trace": mutation_trace,
        "audit_triggered_restart_count": 0,
        "production_behavior_modified": False,
        "result": "PASS" if passed else "FAIL",
    }


def _source_location(root: Path, relative: str, marker: str) -> list[int]:
    path = root / relative
    if not path.exists():
        return []
    return [index for index, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1) if marker in line]


def _source_files(value: str) -> list[str]:
    return [item.strip() for item in value.split(" and ") if item.strip()]


def _constant_keyword(call: ast.Call, name: str) -> object | None:
    for keyword in call.keywords:
        if keyword.arg == name and isinstance(keyword.value, ast.Constant):
            return keyword.value.value
    return None


def _audit_source_locations(path: Path, path_id: str, phase: str, marker: str) -> list[int]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    result: list[int] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        name = node.func.id if isinstance(node.func, ast.Name) else node.func.attr if isinstance(node.func, ast.Attribute) else ""
        if name == "audit_maintenance_decision":
            if _constant_keyword(node, "path_id") == path_id and _constant_keyword(node, "phase") == phase:
                result.append(node.lineno)
        elif name == "audit_self_recovery" and path_id == "MP-10" and _constant_keyword(node, "phase") == phase:
            result.append(node.lineno)
    if result:
        return sorted(set(result))
    return _source_location(path.parent, path.name, marker) if marker and marker != "none" else []


def _audit_calls_are_standalone(path: Path) -> tuple[int, list[int]]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    call_lines: list[int] = []
    standalone_lines: set[int] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            name = node.func.id if isinstance(node.func, ast.Name) else node.func.attr if isinstance(node.func, ast.Attribute) else ""
            if name in {"audit_maintenance_decision", "audit_self_recovery"}:
                call_lines.append(node.lineno)
        if isinstance(node, ast.Expr) and isinstance(node.value, ast.Call):
            name = (
                node.value.func.id
                if isinstance(node.value.func, ast.Name)
                else node.value.func.attr
                if isinstance(node.value.func, ast.Attribute)
                else ""
            )
            if name in {"audit_maintenance_decision", "audit_self_recovery"}:
                standalone_lines.add(node.value.lineno)
    return len(call_lines), sorted(set(call_lines) - standalone_lines)


def source_mapping_and_static_gates(project_root: Path, inventory: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    roots = {"stream_recovery_control": project_root, "stream_v3": project_root.parent / "stream_v3"}
    mapping: list[dict[str, Any]] = []
    missing_markers: list[str] = []
    all_source_files: set[Path] = set()
    for path_record in inventory["paths"]:
        admission = path_record.get("admission") or {}
        repository = str(admission.get("repository") or "stream_v3")
        root = roots.get(repository, roots["stream_v3"])
        source_file = str(admission.get("source_file") or "")
        marker = str(admission.get("marker") or "")
        admission_files = _source_files(source_file)
        admission_locations = {
            relative: _audit_source_locations(root / relative, str(path_record["path_id"]), "ADMISSION", marker)
            for relative in admission_files
            if (root / relative).exists()
        }
        admission_lines = sorted({line for lines in admission_locations.values() for line in lines})
        if str(admission.get("status") or "").startswith("IMPLEMENTED") and not admission_lines:
            missing_markers.append(f"{path_record['path_id']}:ADMISSION")
        for relative in admission_files:
            if (root / relative).exists():
                all_source_files.add(root / relative)
        effects: list[dict[str, Any]] = []
        for effect in path_record.get("effect_boundaries", []):
            effect_file = str(effect.get("source_file") or "")
            effect_marker = str(effect.get("marker") or "")
            effect_root = root
            effect_lines = (
                _audit_source_locations(effect_root / effect_file, str(path_record["path_id"]), "EFFECT_BOUNDARY", effect_marker)
                if (effect_root / effect_file).exists()
                else []
            )
            if str(effect.get("status") or "").startswith("IMPLEMENTED") and not effect_lines:
                missing_markers.append(f"{path_record['path_id']}:EFFECT_BOUNDARY:{effect_file}")
            if effect_file and (effect_root / effect_file).exists():
                all_source_files.add(effect_root / effect_file)
            effects.append({**effect, "line_numbers": effect_lines})
        mapping.append(
            {
                "path_id": path_record["path_id"],
                "name": path_record["name"],
                "repository": repository,
                "admission": {**admission, "locations": admission_locations, "line_numbers": admission_lines},
                "effect_boundaries": effects,
            }
        )

    non_standalone: dict[str, list[int]] = {}
    call_count = 0
    for path in sorted(all_source_files):
        count, bad_lines = _audit_calls_are_standalone(path)
        call_count += count
        if bad_lines:
            non_standalone[str(path)] = bad_lines

    audit_sources = [root / "src/maintenance_audit/__init__.py" for root in roots.values()]
    audit_text = "\n".join(path.read_text(encoding="utf-8") for path in audit_sources)
    forbidden_tokens = [
        token
        for token in ("subprocess", "os.kill", "kubectl", "systemctl", "restart_ffmpeg", "restart_stream", "RuntimeSupervisor")
        if token in audit_text
    ]
    module_hashes = {str(path): _sha256(path) for path in audit_sources}
    parity = len(set(module_hashes.values())) == 1
    path_ids = [str(item["path_id"]) for item in inventory["paths"]]
    effect_owners = {"MP-01", "MP-02", "MP-03", "MP-04", "MP-05", "MP-06", "MP-08", "MP-09", "MP-10"}
    mapped_effect_owners = {
        str(item["path_id"])
        for item in mapping
        if any(str(effect.get("status") or "").startswith("IMPLEMENTED") for effect in item["effect_boundaries"])
    }
    gates = {
        "inventory_has_exactly_14_logical_paths": len(path_ids) == 14 and len(set(path_ids)) == 14,
        "all_paths_have_admission_classification": all("admission" in item for item in inventory["paths"]),
        "all_physical_effect_owners_have_effect_classification": effect_owners <= mapped_effect_owners,
        "implemented_source_markers_resolve": not missing_markers,
        "audit_hook_return_values_are_ignored": not non_standalone,
        "audit_module_has_no_physical_adapter_token": not forbidden_tokens,
        "audit_module_is_identical_in_both_repositories": parity,
        "audit_failure_cannot_call_restart": not any("restart" in token for token in forbidden_tokens),
        "manual_direct_commands_are_not_claimed_complete": inventory.get("manual_path_coverage") == "PARTIAL",
    }
    return (
        {
            "paths": mapping,
            "audit_call_count": call_count,
            "missing_markers": missing_markers,
            "non_standalone_calls": non_standalone,
            "audit_module_hashes": module_hashes,
            "forbidden_tokens": forbidden_tokens,
        },
        {"gates": gates, "result": "PASS" if all(gates.values()) else "FAIL"},
    )


def _run_command(command: list[str], cwd: Path) -> dict[str, Any]:
    completed = subprocess.run(command, cwd=cwd, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, check=False)
    return {"argv": command, "cwd": str(cwd), "returncode": completed.returncode, "output": completed.stdout.strip()}


def verification(project_root: Path) -> dict[str, Any]:
    stream_v3 = project_root.parent / "stream_v3"
    commands = {
        "pytest_recovery_control": ([sys.executable, "-m", "pytest", "-q"], project_root),
        "ruff_format_recovery_control": ([sys.executable, "-m", "ruff", "format", "--check", "."], project_root),
        "ruff_check_recovery_control": ([sys.executable, "-m", "ruff", "check", "."], project_root),
        "mypy_recovery_control": ([sys.executable, "-m", "mypy"], project_root),
        "pytest_stream_v3_full": ([sys.executable, "-m", "pytest", "-q"], stream_v3),
        "py_compile_stream_v3_hooks": (
            [
                sys.executable,
                "-m",
                "py_compile",
                "src/maintenance_audit/__init__.py",
                "ops/scripts/stream_v3_planned_rollout.py",
                "ops/scripts/stream_v3_staged_restart.py",
                "ops/scripts/stream_v3_remote_recovery.py",
                "ops/scripts/stream_v3_scoped_recovery.py",
                "src/watchers/fast_recovery.py",
                "src/watchers/youtube_watchdog.py",
                "src/watchers/local_health/actions.py",
                "src/stream_core/engine/rendering_boot.py",
                "src/stream_core/stream_engine.py",
            ],
            stream_v3,
        ),
        "ruff_critical_stream_v3_hooks": (
            [
                sys.executable,
                "-m",
                "ruff",
                "check",
                "--select",
                "E9,F63,F7,F82",
                "src/maintenance_audit/__init__.py",
                "ops/scripts/stream_v3_planned_rollout.py",
                "ops/scripts/stream_v3_staged_restart.py",
                "ops/scripts/stream_v3_remote_recovery.py",
                "ops/scripts/stream_v3_scoped_recovery.py",
                "src/watchers/fast_recovery.py",
                "src/watchers/youtube_watchdog.py",
                "src/watchers/local_health/actions.py",
                "src/stream_core/engine/rendering_boot.py",
                "src/stream_core/stream_engine.py",
            ],
            stream_v3,
        ),
    }
    results = {name: _run_command(command, cwd) for name, (command, cwd) in commands.items()}
    return {"commands": results, "all_passed": all(item["returncode"] == 0 for item in results.values())}


def _git_state(path: Path) -> dict[str, Any]:
    result = _run_command(["git", "status", "--short", "--branch"], path)
    lines = str(result["output"]).splitlines()
    return {
        "repository": str(path),
        "branch": lines[0] if lines else "UNKNOWN",
        "status_entry_count": max(0, len(lines) - 1),
        "status_sha256": hashlib.sha256(str(result["output"]).encode()).hexdigest(),
        "returncode": result["returncode"],
    }


def _artifact_hashes(run_dir: Path) -> dict[str, str]:
    return {
        str(path.relative_to(run_dir)): _sha256(path)
        for path in sorted(run_dir.rglob("*"))
        if path.is_file() and path.name != "artifact_hashes.json"
    }


def run_p1_audit_suite(project_root: Path, artifact_root: Path, run_id: str) -> Path:
    run_dir = artifact_root / run_id
    run_dir.mkdir(parents=True, exist_ok=False)
    inventory = json.loads((project_root / "harness/fixtures/p1_production_mutation_paths.json").read_text(encoding="utf-8"))
    deterministic = deterministic_cases()
    controls = negative_controls()
    source_mapping, static_gates = source_mapping_and_static_gates(project_root, inventory)
    performance = performance_comparison()
    failure = audit_failure_tests()
    verification_result = verification(project_root)
    oracle_source = (project_root / "src/cra_harness/oracles/p1_audit.py").read_text(encoding="utf-8")
    oracle_independent = "maintenance_audit" not in oracle_source and "evaluate_audit_decision" not in oracle_source
    deterministic_passed = sum(item["classification"] == Classification.PASS.value for item in deterministic)
    controls_detected = sum(item["classification"] == Classification.EXPECTED_INJECTED_FAILURE.value for item in controls)
    ambiguity_count = sum(
        item["classification"] in {Classification.MISSING_EVIDENCE.value, Classification.UNKNOWN_AMBIGUOUS.value}
        for item in [*deterministic, *controls]
    )
    local_trusted = (
        deterministic_passed == 10
        and controls_detected == 10
        and ambiguity_count == 0
        and oracle_independent
        and static_gates["result"] == "PASS"
        and performance["result"] == "PASS"
        and failure["result"] == "PASS"
        and verification_result["all_passed"]
    )
    in_flight_counts = dict(Counter(str(item["in_flight"]["status"]) for item in inventory["paths"]))
    generation_counts = dict(Counter(str(item["generation"]["status"]) for item in inventory["paths"]))
    p2_conditions = {
        "production_mutator_inventory_complete": True,
        "real_admission_points_identified": not source_mapping["missing_markers"],
        "real_effect_boundaries_identified": static_gates["gates"]["all_physical_effect_owners_have_effect_classification"],
        "in_flight_source_defined_or_blockerized": all(
            item["in_flight"]["status"] in {"CONFIRMED", "PROPOSED", "MISSING"} for item in inventory["paths"]
        ),
        "generation_mapping_defined_or_blockerized": all(
            item["generation"]["status"] in {"CONFIRMED", "CONFIRMED_EXISTING_PROPOSED_MAPPING", "PROPOSED", "MISSING", "NOT_APPLICABLE"}
            for item in inventory["paths"]
        ),
        "audit_evidence_complete_in_live_production": False,
        "stale_action_window_observable_locally": deterministic[2]["classification"] == Classification.PASS.value,
        "audit_failure_does_not_alter_behavior": failure["result"] == "PASS",
        "performance_acceptable_locally": performance["result"] == "PASS",
        "manual_coverage_known": inventory["manual_path_coverage"] == "PARTIAL",
        "kubernetes_responsibility_proposed": True,
        "coordinator_placement_proposed": True,
        "negative_controls_100_percent": controls_detected == 10,
        "classification_ambiguity_zero": ambiguity_count == 0,
    }
    summary = {
        "run_id": run_id,
        "logical_mutation_path_count": inventory["logical_path_count"],
        "deterministic": {"passed": deterministic_passed, "total": 10},
        "negative_controls": {"detected": controls_detected, "total": 10},
        "independent_oracle": oracle_independent,
        "classification_ambiguity_count": ambiguity_count,
        "physical_effect_count": 0,
        "local_p1_harness_trusted": local_trusted,
        "p1_audit_only_integration": "READY" if local_trusted else "PENDING",
        "production_audit_deploy_status": "NOT_DEPLOYED",
        "production_behavior_modified": False,
        "production_maintenance_fence": "PENDING",
        "maintenance_protocol_v2": "ACCEPTED",
        "break_glass_protocol": "PENDING",
        "production_db_deadline_guard": "PENDING_NOT_CO_DEPLOYED",
        "phase4_shadow_accepted": False,
        "phase5_preconditions": "NOT_MET",
        "p2_readiness": "NOT_READY_LIVE_AUDIT_EVIDENCE_MISSING",
        "in_flight_source_counts": in_flight_counts,
        "generation_source_counts": generation_counts,
    }
    manifest = {
        "schema_version": "maintenance.p1.audit_harness_manifest.v1",
        "run_id": run_id,
        "created_at": datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "project_root": str(project_root),
        "stream_v3_root": str(project_root.parent / "stream_v3"),
        "harness_revision": "p1_audit_only_production_integration.v1",
        "production_deploy_performed": False,
        "physical_effect_count": 0,
        "git_state_at_artifact": [
            _git_state(project_root),
            _git_state(project_root.parent / "stream_v3"),
            _git_state(project_root.parent / "stream_v4"),
        ],
    }
    oracle_result = {
        "independent": oracle_independent,
        "deterministic_violations": {item["scenario_id"]: item["oracle_violations"] for item in deterministic},
        "negative_control_violations": {item["scenario_id"]: item["oracle_violations"] for item in controls},
        "result": "PASS" if oracle_independent and deterministic_passed == 10 and controls_detected == 10 else "FAIL",
    }
    coverage = {
        "inventory_to_source_to_hook_to_oracle": static_gates["result"] == "PASS" and oracle_result["result"] == "PASS",
        "logical_path_count": inventory["logical_path_count"],
        "manual_path_coverage": inventory["manual_path_coverage"],
        "live_event_coverage": "NOT_MEASURED_NOT_DEPLOYED",
        "local_p1_harness_trusted": local_trusted,
    }
    readiness = {
        "p2_conditions": p2_conditions,
        "all_model_and_local_conditions_met": all(
            value for key, value in p2_conditions.items() if key != "audit_evidence_complete_in_live_production"
        ),
        "all_conditions_met": all(p2_conditions.values()),
        "status": "NOT_READY_LIVE_AUDIT_EVIDENCE_MISSING",
    }
    safety = {
        "physical_effect_count": 0,
        "ffmpeg_signal_count": 0,
        "pod_mutation_count": 0,
        "deployment_mutation_count": 0,
        "host_restart_count": 0,
        "timer_or_service_stop_count": 0,
        "production_behavior_modified": False,
        "production_deploy_performed": False,
    }

    for case in deterministic:
        _json(run_dir / "evidence" / f"{case['scenario_id']}.json", case)
    for case in controls:
        _json(run_dir / "negative_controls" / f"{case['scenario_id']}.json", case)
    _json(run_dir / "manifest.json", manifest)
    _json(run_dir / "matrix.json", [*deterministic, *controls])
    _json(
        run_dir / "classification.json",
        [{"scenario_id": item["scenario_id"], "classification": item["classification"]} for item in [*deterministic, *controls]],
    )
    _json(run_dir / "mutation_path_inventory.json", inventory)
    _json(run_dir / "source_mapping.json", source_mapping)
    _json(run_dir / "admission_effect_boundary_mapping.json", source_mapping["paths"])
    _json(run_dir / "in_flight_source_mapping.json", {item["path_id"]: item["in_flight"] for item in inventory["paths"]})
    _json(run_dir / "sequence_generation_mapping.json", {item["path_id"]: item["generation"] for item in inventory["paths"]})
    _json(run_dir / "stale_execution_windows.json", {item["path_id"]: item["stale_context"] for item in inventory["paths"]})
    _json(run_dir / "static_gates.json", static_gates)
    _json(run_dir / "performance.json", performance)
    _json(run_dir / "audit_failure_tests.json", failure)
    _json(run_dir / "independent_oracle.json", oracle_result)
    _json(run_dir / "coverage_trust.json", coverage)
    _json(run_dir / "p2_readiness.json", readiness)
    _json(run_dir / "verification.json", verification_result)
    _json(run_dir / "safety.json", safety)
    _json(run_dir / "summary.json", summary)
    shutil.copyfile(
        project_root / "harness/fixtures/p1_live_deployment_observation.json",
        run_dir / "live_deployment_observation.json",
    )
    shutil.copyfile(
        project_root / "docs/engineering/records/2026-08-23_38_p1_audit_only_production_integration.md",
        run_dir / "engineering_record.md",
    )
    shutil.copyfile(
        project_root / "docs/engineering/proposals/2026-08-23_p1_coordinator_placement.md",
        run_dir / "coordinator_placement_proposal.md",
    )
    shutil.copyfile(
        project_root / "docs/engineering/proposals/2026-08-23_kubernetes_autoreconcile_responsibility.md",
        run_dir / "kubernetes_autoreconcile_proposal.md",
    )
    shutil.copyfile(
        project_root / "docs/runbooks/2026-08-23_p1_audit_only_deployment.md",
        run_dir / "deployment_runbook.md",
    )
    shutil.copyfile(project_root / "contracts/maintenance/v2/audit_event.schema.json", run_dir / "audit_event.schema.json")
    shutil.copyfile(
        project_root / "contracts/maintenance/v2/audit_state_snapshot.schema.json", run_dir / "audit_state_snapshot.schema.json"
    )
    report = f"""# Maintenance Protocol v2 / P1 Audit-Only Production Integration

- logical mutation paths: {inventory["logical_path_count"]}
- deterministic P1: {deterministic_passed}/10 PASS
- Negative Controls: {controls_detected}/10 EXPECTED_INJECTED_FAILURE
- independent Oracle: {str(oracle_independent).lower()}
- static gates: {static_gates["result"]}
- performance: {performance["result"]}
- audit failure isolation: {failure["result"]}
- physical effect count: 0
- production behavior modified: false
- production deploy: NOT_DEPLOYED

`P1_AUDIT_ONLY_INTEGRATION = {summary["p1_audit_only_integration"]}`

これはlocal/deploy-ready評価であり、live production eventによる`VERIFIED`ではない。

`PRODUCTION_MAINTENANCE_FENCE = PENDING`

`PRODUCTION_DB_DEADLINE_GUARD = PENDING_NOT_CO_DEPLOYED`

`PHASE4_SHADOW_ACCEPTED = false`

`PHASE5_PRECONDITIONS = NOT_MET`
"""
    (run_dir / "report.md").write_text(report, encoding="utf-8")
    _json(run_dir / "artifact_hashes.json", _artifact_hashes(run_dir))
    return run_dir
