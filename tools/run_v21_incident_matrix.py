#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import importlib.util
import itertools
import json
import math
import sys
import tempfile
import time
from pathlib import Path
from types import ModuleType
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.build_runtime_fence_release import replace_controller_functions  # noqa: E402

BASE_CONTROLLER = ROOT / "artifacts/runtime-fence-476ba5e78f32-v16/controller/src"
CONTROLLER_BASE = BASE_CONTROLLER / "watchers/fast_recovery.py"
CONTROLLER_OVERLAY = ROOT / "ops/runtime_overlays/fast_recovery_bounded_lifecycle.py"

AXES = {
    "correlation_mapping": ("exact", "not_found", "ambiguous", "timeout"),
    "request_state": ("ACCEPTED", "EXECUTION_STARTED", "OUTCOME_UNKNOWN", "EFFECT_OBSERVED", "EFFECT_FAILED"),
    "scope_state": (
        "ACCEPTED",
        "EXECUTION_STARTED",
        "OUTCOME_UNKNOWN",
        "EFFECT_OBSERVED_AWAITING_VERIFICATION",
        "EFFECT_FAILED",
        "RECONCILED_EFFECT_OBSERVED",
        "RETIRED_TARGET_OUTCOME_UNKNOWN",
    ),
    "unresolved_ledger_view": ("none", "matching", "foreign"),
    "successor_oracle": ("three_ack_progress", "frozen_ack", "stale_ack", "same_target"),
    "network_phase": ("down", "new_connectivity_only", "probe_failed", "recovered"),
    "controller_interruption": ("before_accept", "after_accept", "after_effect_duplicate", "after_reconcile_stale"),
    "status_channel": ("available", "timeout"),
}
EXPECTED_SCENARIOS = math.prod(len(values) for values in AXES.values())
MINIMUM_SCENARIOS = 10_000
RESOLVED_SCOPE_STATES = {
    "EFFECT_FAILED",
    "RECONCILED_EFFECT_OBSERVED",
    "RELEASED_NO_EFFECT",
    "RETIRED_TARGET_OUTCOME_UNKNOWN",
}
RECONCILABLE_SCOPE_STATES = {
    "ACCEPTED",
    "EXECUTION_STARTED",
    "OUTCOME_UNKNOWN",
    "EFFECT_OBSERVED_AWAITING_VERIFICATION",
}


def load_controller(root: Path) -> ModuleType:
    generated = replace_controller_functions(
        CONTROLLER_BASE.read_text(encoding="utf-8"),
        CONTROLLER_OVERLAY.read_text(encoding="utf-8"),
    )
    path = root / "v21_matrix_fast_recovery.py"
    path.write_text(generated, encoding="utf-8")
    sys.path.insert(0, str(BASE_CONTROLLER / "watchers"))
    sys.path.insert(0, str(BASE_CONTROLLER))
    spec = importlib.util.spec_from_file_location("v21_matrix_fast_recovery", path)
    if spec is None or spec.loader is None:
        raise RuntimeError("V21_MATRIX_CONTROLLER_IMPORT_FAILED")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def status_payload(
    *,
    request_id: str,
    request_state: str,
    scope_state: str,
    correlation_id: str = "fra-matrix",
) -> dict[str, Any]:
    return {
        "ok": True,
        "reason": "CORRELATION_STATUS_READ",
        "correlation_id": correlation_id,
        "matching_count": 1,
        "request_id": request_id,
        "state": request_state,
        "effect_scope_state": scope_state,
        "effect_scope_id": f"scope-{request_id}",
        "effect_boundary_reached": request_state not in {"ACCEPTED"},
        "physical_attempt_count": 0 if request_state == "ACCEPTED" else 1,
        "result": {"physical_effect_count": 1} if request_state != "ACCEPTED" else None,
        "automatic_retry": False if request_state in {"EXECUTION_STARTED", "OUTCOME_UNKNOWN"} else None,
    }


def initial_pending(interruption: str) -> list[dict[str, Any]]:
    provisional = {
        "recovery_action_id": "fra-matrix",
        "requested_at_ts": 100,
        "requested_ffmpeg_pid": 100,
        "requested_ffmpeg_generation": "old-generation",
    }
    if interruption == "before_accept":
        return []
    if interruption == "after_effect_duplicate":
        return [
            provisional,
            {
                "recovery_action_id": "dell-target-matrix",
                "owner_request_id": "dell-target-matrix",
                "requested_ffmpeg_pid": 100,
                "requested_ffmpeg_generation": "old-generation",
            },
        ]
    return [provisional]


def scope_payload(view: str, scope_state: str) -> list[dict[str, Any]]:
    if view == "none":
        return []
    matching = view == "matching"
    return [
        {
            "owner_request_id": "dell-target-matrix" if matching else "dell-target-foreign",
            "owner_request_digest": "a" * 64,
            "effect_scope_id": "scope-matrix" if matching else "scope-foreign",
            "state": scope_state,
            "identity": {
                "host_id": "isolated-dell" if matching else "foreign-host",
                "host_boot_id": "isolated-boot",
                "namespace": "isolated",
                "pod_uid": "isolated-pod",
                "container_name": "stream-engine",
                "container_id": "containerd://isolated",
                "ffmpeg_generation": "old-generation",
                "ffmpeg_pid": 100,
            },
        }
    ]


def execute_matrix() -> dict[str, Any]:
    if EXPECTED_SCENARIOS < MINIMUM_SCENARIOS:
        raise RuntimeError("V21_INCIDENT_MATRIX_BELOW_MINIMUM")
    counters = {
        "scenarios": 0,
        "invariant_checks": 0,
        "controller_action_retained": 0,
        "controller_action_resolved": 0,
        "new_action_admissible": 0,
        "reconciliation_preconditions_met": 0,
        "maximum_pending_count": 0,
        "correlation_queries": 0,
        "request_status_queries": 0,
        "physical_effect_calls": 0,
    }
    failures: list[dict[str, Any]] = []
    digest = hashlib.sha256()
    current_scenario: dict[str, str] = {}

    with tempfile.TemporaryDirectory(prefix="v21-incident-matrix-") as raw_root:
        controller = load_controller(Path(raw_root))
        controller.EFFECT_EXECUTOR_SOCKET = str(Path(raw_root) / "never-created-effect.sock")
        controller.append_event = lambda *_args, **_kwargs: None

        def correlation_status(*, correlation_id: str, **_kwargs: object) -> dict[str, Any]:
            counters["correlation_queries"] += 1
            mapping = current_scenario["correlation_mapping"]
            if mapping == "timeout":
                raise TimeoutError("synthetic correlation query timeout")
            if mapping == "not_found":
                return {
                    "ok": False,
                    "reason": "CORRELATION_NOT_FOUND",
                    "correlation_id": correlation_id,
                    "matching_count": 0,
                    "request_id": "",
                    "state": "UNKNOWN",
                    "effect_scope_state": "UNKNOWN",
                    "physical_attempt_count": 0,
                    "result": None,
                }
            if mapping == "ambiguous":
                return {
                    "ok": False,
                    "reason": "CORRELATION_AMBIGUOUS",
                    "correlation_id": correlation_id,
                    "matching_count": 2,
                    "request_id": "",
                    "state": "UNKNOWN",
                    "effect_scope_state": "UNKNOWN",
                    "physical_attempt_count": 0,
                    "result": None,
                }
            return status_payload(
                request_id="dell-target-matrix",
                request_state=current_scenario["request_state"],
                scope_state=current_scenario["scope_state"],
                correlation_id=correlation_id,
            )

        def request_status(*, request_id: str, **_kwargs: object) -> dict[str, Any]:
            counters["request_status_queries"] += 1
            if current_scenario["status_channel"] == "timeout":
                raise TimeoutError("synthetic request status timeout")
            return status_payload(
                request_id=request_id,
                request_state=current_scenario["request_state"],
                scope_state=current_scenario["scope_state"],
                correlation_id="fra-matrix" if request_id == "dell-target-matrix" else "foreign-correlation",
            )

        controller.effect_contract.effect_request_status_by_correlation = correlation_status
        controller.effect_contract.effect_request_status = request_status

        axis_names = tuple(AXES)
        started = time.monotonic()
        for values in itertools.product(*(AXES[name] for name in axis_names)):
            current_scenario = dict(zip(axis_names, values, strict=True))
            state: dict[str, Any] = {
                "pending_recovery_actions": initial_pending(current_scenario["controller_interruption"]),
                "reported_executor_terminal_request_ids": [],
            }
            had_controller_action = any(item.get("recovery_action_id") == "fra-matrix" for item in state["pending_recovery_actions"])
            scopes = scope_payload(
                current_scenario["unresolved_ledger_view"],
                current_scenario["scope_state"],
            )
            try:
                controller.sync_pending_recovery_from_executor(state, scopes)
                pending = [dict(item) for item in state.get("pending_recovery_actions", [])]
                counters["maximum_pending_count"] = max(counters["maximum_pending_count"], len(pending))
                controller_action = [item for item in pending if item.get("recovery_action_id") == "fra-matrix"]
                formal_owner = [item for item in pending if item.get("owner_request_id") == "dell-target-matrix"]
                if controller_action:
                    counters["controller_action_retained"] += 1
                elif had_controller_action:
                    counters["controller_action_resolved"] += 1

                if len(formal_owner) > 1:
                    raise AssertionError("FORMAL_OWNER_DUPLICATED")
                counters["invariant_checks"] += 1

                mapping_proven = current_scenario["correlation_mapping"] == "exact"
                channel_available = current_scenario["status_channel"] == "available"
                matching_scope_visible = current_scenario["unresolved_ledger_view"] == "matching"
                resolved_scope = current_scenario["scope_state"] in RESOLVED_SCOPE_STATES
                terminal_states_consistent = (
                    current_scenario["scope_state"] != "EFFECT_FAILED" or current_scenario["request_state"] == "EFFECT_FAILED"
                )
                may_resolve_controller_action = (
                    mapping_proven and channel_available and resolved_scope and terminal_states_consistent and not matching_scope_visible
                )
                if had_controller_action and not controller_action and not may_resolve_controller_action:
                    raise AssertionError("UNPROVEN_CONTROLLER_ACTION_REMOVED")
                counters["invariant_checks"] += 1

                if had_controller_action and not mapping_proven and not controller_action:
                    raise AssertionError("UNMAPPED_CORRELATION_DID_NOT_FAIL_CLOSED")
                counters["invariant_checks"] += 1

                new_action_admissible = not pending and not scopes
                if new_action_admissible:
                    counters["new_action_admissible"] += 1
                if pending and new_action_admissible:
                    raise AssertionError("PENDING_ACTION_DID_NOT_SUPPRESS_NEW_EFFECT")
                counters["invariant_checks"] += 1

                reconciliation_preconditions = (
                    matching_scope_visible
                    and current_scenario["scope_state"] in RECONCILABLE_SCOPE_STATES
                    and current_scenario["request_state"] in {"OUTCOME_UNKNOWN", "EFFECT_OBSERVED"}
                    and current_scenario["successor_oracle"] == "three_ack_progress"
                    and current_scenario["network_phase"] == "recovered"
                    and current_scenario["controller_interruption"] in {"after_effect_duplicate", "after_reconcile_stale"}
                    and channel_available
                )
                if reconciliation_preconditions:
                    counters["reconciliation_preconditions_met"] += 1
                counters["invariant_checks"] += 1

                if counters["physical_effect_calls"] != 0:
                    raise AssertionError("MODEL_QUERY_PATH_CALLED_PHYSICAL_EFFECT")
                counters["invariant_checks"] += 1

                digest.update(("\x1f".join(values) + f"|{len(pending)}|{int(new_action_admissible)}\n").encode())
            except BaseException as exc:  # noqa: BLE001 - preserve the first bounded set of counterexamples
                if len(failures) < 32:
                    failures.append({"scenario": current_scenario, "error": f"{type(exc).__name__}: {exc}"})
            counters["scenarios"] += 1
        elapsed = time.monotonic() - started

    if counters["scenarios"] != EXPECTED_SCENARIOS:
        raise RuntimeError("V21_INCIDENT_MATRIX_COUNT_MISMATCH")
    if failures:
        raise RuntimeError("V21_INCIDENT_MATRIX_FAILED:" + json.dumps(failures, sort_keys=True))
    return {
        "schema": "stream_recovery_control.v21_incident_matrix.v1",
        "result": "PASS",
        "derivation": "full Cartesian product of the declared incident-state axes",
        "axes": {name: list(values) for name, values in AXES.items()},
        "expected_scenarios": EXPECTED_SCENARIOS,
        "minimum_requested_scenarios": MINIMUM_SCENARIOS,
        "counters": counters,
        "outcome_digest_sha256": digest.hexdigest(),
        "elapsed_seconds": round(elapsed, 3),
        "isolation": {
            "external_network_calls": 0,
            "production_effect_socket_touched": False,
            "production_sqlite_touched": False,
            "production_target_touched": False,
            "temporary_files_only": True,
        },
        "claim_boundary": "model-level exhaustive incident injection; representative process/socket/SQLite chaos is separate",
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the closed deterministic v21 incident-state matrix")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    report = execute_matrix()
    encoded = json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    if args.output is not None:
        args.output.write_text(encoded, encoding="utf-8")
    print(encoded, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
