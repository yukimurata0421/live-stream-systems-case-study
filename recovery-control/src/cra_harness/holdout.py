from __future__ import annotations

import hashlib
import json
import random
import tempfile
import time
from collections import Counter
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from runtime_boundary.ledger import EffectLedger
from runtime_boundary.model import EffectRequest


def _target(*, token: str, generation: int, pid: int) -> dict[str, object]:
    return {
        "host_id": "dell-harness",
        "host_boot_id": f"boot-{token}",
        "namespace": "stream-v3",
        "pod_uid": f"pod-{token}",
        "container_name": "stream-engine",
        "container_id": f"containerd://{token}",
        "ffmpeg_generation": f"generation-{token}-{generation}",
        "ffmpeg_pid": pid,
    }


def _request(
    *,
    token: str,
    request_id: str,
    generation: int,
    pid: int,
    sequence: int,
) -> EffectRequest:
    now = datetime.now(UTC)
    target = _target(token=token, generation=generation, pid=pid)
    return EffectRequest.from_mapping(
        {
            "schema_version": "runtime.typed_effect_request.v2",
            "request_id": request_id,
            "producer_id": "holdout-producer",
            "producer_generation": 1,
            "intent_type": "RESTART_FFMPEG",
            "reason": "holdout state-machine evidence",
            "failure_domain": "DELIVERY_PATH",
            "issued_at": now.isoformat(),
            "expires_at": (now + timedelta(seconds=3)).isoformat(),
            "ffmpeg_target_identity": target,
            "runtime_identity": None,
            "expected_ffmpeg_generation": f"native-{token}-{generation}",
            "idempotency_key": request_id,
            "correlation_id": request_id,
            "target_snapshot_id": f"target-snapshot-{token}-{sequence}",
            "runtime_snapshot_id": "",
            "runtime_observation_id": f"runtime-observation-{token}-{sequence}",
            "expected_executor_instance_id": f"executor-{token}",
            "maintenance_evidence_status": "AVAILABLE",
            "projection_id": f"projection-{token}-{sequence}",
            "projection_sequence": sequence,
        }
    )


def _attempt_total(ledger: EffectLedger) -> int:
    return int(ledger.connection.execute("SELECT coalesce(sum(physical_attempt_count),0) FROM effect_scope_fences").fetchone()[0])


def _run_case(root: Path, *, seed: int, case_index: int) -> dict[str, Any]:
    case_seed = int(hashlib.sha256(f"{seed}:{case_index}".encode()).hexdigest()[:16], 16)
    rng = random.Random(case_seed)
    token = hashlib.sha256(f"case:{case_seed}".encode()).hexdigest()[:12]
    database = root / f"case-{case_index:05d}.sqlite3"
    ledger = EffectLedger(
        database,
        initial_producer_id="holdout-producer",
        initial_producer_generation=1,
    )
    trace: list[str] = []
    errors: list[str] = []
    first = _request(token=token, request_id=f"first-{token}", generation=1, pid=4100, sequence=1)
    try:
        accepted = ledger.accept(first)
        if not accepted["accepted"]:
            errors.append("FIRST_REQUEST_REJECTED")
        trace.append("FIRST_ACCEPTED")
        if not ledger.transition(first.request_id, expected="ACCEPTED", state="EXECUTION_STARTED"):
            errors.append("EXECUTION_START_NOT_DURABLE")
        trace.append("EXECUTION_STARTED")
        boundary_reached = rng.random() < 0.78
        if boundary_reached:
            if not ledger.mark_effect_boundary(first.request_id):
                errors.append("BOUNDARY_RESERVATION_FAILED")
            trace.append("EFFECT_BOUNDARY_REACHED")
        if not ledger.transition(first.request_id, expected="EXECUTION_STARTED", state="OUTCOME_UNKNOWN"):
            errors.append("UNKNOWN_NOT_PERSISTED")
        trace.append("OUTCOME_UNKNOWN")

        if rng.random() < 0.65:
            ledger.close()
            ledger = EffectLedger(
                database,
                initial_producer_id="ignored-after-reopen",
                initial_producer_generation=99,
                allow_initialize=False,
            )
            trace.append("PROCESS_REENTRY")

        variants = [
            _request(token=token, request_id=f"same-scope-{token}", generation=1, pid=4100, sequence=2),
            _request(token=token, request_id=f"same-generation-new-pid-{token}", generation=1, pid=4101, sequence=3),
            _request(token=token, request_id=f"successor-generation-{token}", generation=2, pid=4200, sequence=4),
        ]
        rng.shuffle(variants)
        for candidate in variants:
            before = _attempt_total(ledger)
            result = ledger.accept(candidate)
            after = _attempt_total(ledger)
            trace.append(f"REENTRY_{result['reason']}")
            if result["accepted"]:
                errors.append("UNRESOLVED_SCOPE_ALLOWED_SECOND_REQUEST")
            if after != before:
                errors.append("REENTRY_CHANGED_ATTEMPT_COUNTER")
            if ledger.request(candidate.request_id) is not None:
                errors.append("REJECTED_REENTRY_WAS_PERSISTED")

        reconciliation_id = f"holdout-reconciliation-{token}"
        if boundary_reached:
            ledger.record_reconciliation(
                reconciliation_id=reconciliation_id,
                effect_scope_id=first.effect_scope_id,
                resolution="EFFECT_OBSERVED",
                evidence={
                    "physical_effect_count": 1,
                    "automatic_retry_count": 0,
                    "operation_order": trace,
                },
            )
            expected_attempts = 1
        else:
            ledger.record_reconciliation(
                reconciliation_id=reconciliation_id,
                effect_scope_id=first.effect_scope_id,
                resolution="NO_EFFECT_PROVEN",
                evidence={
                    "physical_effect_count": 0,
                    "automatic_retry_count": 0,
                    "proof": "synthetic crash point was before the fake effect boundary",
                    "operation_order": trace,
                },
            )
            expected_attempts = 0
        trace.append("APPEND_ONLY_RECONCILIATION")

        raw = ledger.request(first.request_id)
        if raw is None or str(raw["state"]) != "OUTCOME_UNKNOWN":
            errors.append("RAW_UNKNOWN_WAS_REWRITTEN")
        if ledger.unresolved_count() != 0:
            errors.append("RECONCILED_SCOPE_REMAINS_ACTIVE")
        if _attempt_total(ledger) != expected_attempts:
            errors.append("ATTEMPT_COUNTER_NOT_CONSERVED")
        reconciliation_rows = int(
            ledger.connection.execute(
                "SELECT count(*) FROM effect_reconciliations WHERE effect_scope_id=?", (first.effect_scope_id,)
            ).fetchone()[0]
        )
        if reconciliation_rows != 1:
            errors.append("RECONCILIATION_NOT_APPEND_ONLY_ONE_ROW")

        successor = _request(token=token, request_id=f"after-resolution-{token}", generation=2, pid=4200, sequence=5)
        after_resolution = ledger.accept(successor)
        if not after_resolution["accepted"]:
            errors.append("NEW_GENERATION_BLOCKED_AFTER_PROVEN_RESOLUTION")
        trace.append("NEW_GENERATION_ACCEPTED_AFTER_RESOLUTION")
        physical_attempts = _attempt_total(ledger)
        if physical_attempts != expected_attempts:
            errors.append("ACCEPTANCE_CROSSED_EFFECT_BOUNDARY")
        return {
            "case_id": f"holdout-{seed}-{case_index:05d}",
            "case_seed": case_seed,
            "classification": "PASS" if not errors else "SUT_FAILURE",
            "boundary_reached": boundary_reached,
            "process_reentry": "PROCESS_REENTRY" in trace,
            "trace": trace,
            "errors": errors,
            "physical_attempt_count": physical_attempts,
            "automatic_retry_count": 0,
            "raw_unknown_preserved": raw is not None and str(raw["state"]) == "OUTCOME_UNKNOWN",
        }
    except BaseException as error:
        return {
            "case_id": f"holdout-{seed}-{case_index:05d}",
            "case_seed": case_seed,
            "classification": "HARNESS_ERROR",
            "trace": trace,
            "errors": [f"{type(error).__name__}:{str(error)[:160]}"],
            "physical_attempt_count": _attempt_total(ledger),
            "automatic_retry_count": 0,
            "raw_unknown_preserved": False,
        }
    finally:
        ledger.close()


def run_holdout_harness(
    *,
    seeds: tuple[int, ...] = (20260911, 20260912, 20260913, 20260914, 20260915),
    cases_per_seed: int = 128,
) -> dict[str, Any]:
    if not seeds or isinstance(cases_per_seed, bool) or not 1 <= cases_per_seed <= 4096:
        raise ValueError("HOLDOUT_VOLUME_INVALID")
    started = time.perf_counter()
    results: list[dict[str, Any]] = []
    with tempfile.TemporaryDirectory(prefix="cra-holdout-") as temporary:
        root = Path(temporary)
        for seed in seeds:
            seed_root = root / str(seed)
            seed_root.mkdir()
            results.extend(_run_case(seed_root, seed=seed, case_index=index) for index in range(cases_per_seed))
    wall_time = time.perf_counter() - started
    classifications = Counter(str(item["classification"]) for item in results)
    payload = json.dumps(results, separators=(",", ":"), sort_keys=True).encode()
    return {
        "schema": "cra.independent_state_machine_holdout.v1",
        "classification": (
            "HARNESS_FAILURE" if classifications["HARNESS_ERROR"] else "SUT_FAILURE" if classifications["SUT_FAILURE"] else "PASS"
        ),
        "scenario_family": "operation_order_and_restart_reentry_holdout",
        "seed_count": len(seeds),
        "seeds": list(seeds),
        "case_count": len(results),
        "cases_per_seed": cases_per_seed,
        "sut_failure_count": classifications["SUT_FAILURE"],
        "harness_failure_count": classifications["HARNESS_ERROR"],
        "boundary_reached_case_count": sum(bool(item.get("boundary_reached")) for item in results),
        "pre_boundary_case_count": sum(item.get("boundary_reached") is False for item in results),
        "restart_reentry_case_count": sum(bool(item.get("process_reentry")) for item in results),
        "wall_duration_seconds": round(wall_time, 6),
        "simulated_operation_count": sum(len(item["trace"]) for item in results),
        "case_results_sha256": hashlib.sha256(payload).hexdigest(),
        "safety": {
            "physical_effect_count": 0,
            "ffmpeg_signal_count": 0,
            "production_database_used": False,
            "production_network_used": False,
        },
        "failure_examples": [item for item in results if item["classification"] != "PASS"][:100],
    }
