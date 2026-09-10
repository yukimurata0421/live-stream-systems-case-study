from __future__ import annotations

import copy
import random
import threading
from collections.abc import Callable
from pathlib import Path
from types import ModuleType
from typing import Any

from cra_harness.runner.no_action_randomized import RELEASES, _campaign, _digest, _increment_from, _set_from
from cra_no_action_soak.gate import evaluate_no_action_soak


def _mutant_module(path: Path, *, identity: str, old: str, new: str, package: str) -> ModuleType:
    source = path.read_text(encoding="utf-8")
    if source.count(old) != 1:
        raise RuntimeError(f"MUTATION_SITE_NOT_UNIQUE:{identity}:{source.count(old)}")
    module = ModuleType(f"{package}.{identity}")
    module.__file__ = str(path)
    module.__package__ = package
    exec(compile(source.replace(old, new, 1), f"<{identity}>", "exec"), module.__dict__)
    return module


def _samples(seed: int) -> tuple[list[dict[str, Any]], float, int]:
    rng = random.Random(seed)
    samples, maximum_gap = _campaign(rng, seed)
    return samples, maximum_gap, rng.randint(1, len(samples) - 1)


def _blockers(function: Callable[..., dict[str, Any]], samples: list[dict[str, Any]], maximum_gap: float) -> set[str]:
    return {
        str(item).split(":", 1)[0]
        for item in function(
            samples,
            expected_releases=RELEASES,
            maximum_sample_gap_seconds=maximum_gap,
        )["blockers"]
    }


def _gate_mutations(project_root: Path) -> list[dict[str, Any]]:
    gate = project_root / "src/cra_no_action_soak/gate.py"
    definitions: list[tuple[str, str, str, str, Callable[[list[dict[str, Any]], int], dict[str, Any]]]] = [
        (
            "finite_check_deleted",
            "or not math.isfinite(float(minimum_duration_seconds))",
            "or False",
            "SOAK_GATE_LIMIT_INVALID",
            lambda samples, index: {"minimum_duration_seconds": float("nan")},
        ),
        (
            "boolean_rejection_deleted",
            "or isinstance(minimum_disk_free_bytes, bool)",
            "or False",
            "SOAK_GATE_LIMIT_INVALID",
            lambda samples, index: {"minimum_disk_free_bytes": False},
        ),
        (
            "effect_counter_comparison_deleted",
            'operation_counters["effect_boundary_count"] > operation_counters["effect_request_count"]',
            "False",
            "SOAK_EFFECT_COUNTER_RELATION_INVALID",
            lambda samples, index: _mutate_all(
                samples,
                lambda sample: sample["operations"].update(effect_boundary_count=sample["operations"]["effect_request_count"] + 1),
            ),
        ),
        (
            "epoch_baseline_drift_check_deleted",
            'blockers.add(f"SOAK_EPOCH_COUNTER_BASELINE_DRIFT:{index}")',
            "pass",
            "SOAK_EPOCH_COUNTER_BASELINE_DRIFT",
            lambda samples, index: _mutate_increment(samples, index, ("operations", "effect_request_count")),
        ),
        (
            "target_transition_conservation_deleted",
            'blockers.add(f"SOAK_TARGET_TRANSITION_COUNTER_CONSERVATION_INVALID:{index}")',
            "pass",
            "SOAK_TARGET_TRANSITION_COUNTER_CONSERVATION_INVALID",
            lambda samples, index: _mutate_target_conservation(samples, index),
        ),
        (
            "unresolved_age_check_deleted",
            'blockers.add(f"SOAK_UNRESOLVED_AGE_ORACLE_MISMATCH:{index}")',
            "pass",
            "SOAK_UNRESOLVED_AGE_ORACLE_MISMATCH",
            lambda samples, index: _mutate_all(
                samples,
                lambda sample: sample["operations"].update(oldest_unresolved_age_seconds=1),
            ),
        ),
        (
            "safe_blocked_duration_check_deleted",
            'blockers.add(f"SOAK_SAFE_BLOCKED_DURATION_ORACLE_MISMATCH:{index}")',
            "pass",
            "SOAK_SAFE_BLOCKED_DURATION_ORACLE_MISMATCH",
            lambda samples, index: _mutate_all(
                samples,
                lambda sample: sample["operations"].update(cra_current_safe_blocked_duration_seconds=1),
            ),
        ),
    ]
    results: list[dict[str, Any]] = []
    for offset, (identity, old, new, expected, inject) in enumerate(definitions):
        samples, maximum_gap, index = _samples(20260920 + offset)
        limits = inject(samples, index) or {}
        mutant = _mutant_module(gate, identity=identity, old=old, new=new, package="cra_no_action_soak")
        original_detected = False
        mutant_detected = False
        original_error = ""
        mutant_error = ""
        try:
            original = evaluate_no_action_soak(
                copy.deepcopy(samples),
                expected_releases=RELEASES,
                maximum_sample_gap_seconds=maximum_gap,
                **limits,
            )
            original_detected = expected in {str(item).split(":", 1)[0] for item in original["blockers"]}
        except ValueError as error:
            original_error = str(error)
            original_detected = expected in str(error)
        try:
            changed = mutant.evaluate_no_action_soak(
                copy.deepcopy(samples),
                expected_releases=RELEASES,
                maximum_sample_gap_seconds=maximum_gap,
                **limits,
            )
            mutant_detected = expected in {str(item).split(":", 1)[0] for item in changed["blockers"]}
        except ValueError as error:
            mutant_error = str(error)
            mutant_detected = expected in str(error)
        results.append(
            {
                "mutation_identity": identity,
                "expected_detection": expected,
                "original_detected": original_detected,
                "mutant_retained_detection": mutant_detected,
                "mutant_detection": "PASS" if original_detected and not mutant_detected else "FAIL",
                "original_error": original_error,
                "mutant_error": mutant_error,
            }
        )
    return results


def _mutate_all(samples: list[dict[str, Any]], mutation: Callable[[dict[str, Any]], None]) -> dict[str, Any]:
    for sample in samples:
        mutation(sample)
    return {}


def _mutate_increment(samples: list[dict[str, Any]], index: int, path: tuple[str, ...]) -> dict[str, Any]:
    _increment_from(samples, index, path)
    return {}


def _mutate_target_conservation(samples: list[dict[str, Any]], index: int) -> dict[str, Any]:
    _set_from(samples, index, ("infrastructure", "target_identity_sha256"), _digest(f"mutant-target-{index}"))
    _set_from(samples, index, ("operations", "target_transition_count"), 1)
    return {}


def _lock_allows_overlap(lock: Callable[[Path], Any], path: Path) -> bool:
    first_entered = threading.Event()
    release_first = threading.Event()
    second_entered = threading.Event()

    def first() -> None:
        with lock(path):
            first_entered.set()
            release_first.wait(timeout=2)

    def second() -> None:
        if not first_entered.wait(timeout=2):
            return
        with lock(path):
            second_entered.set()

    first_thread = threading.Thread(target=first)
    second_thread = threading.Thread(target=second)
    first_thread.start()
    second_thread.start()
    first_entered.wait(timeout=2)
    overlap = second_entered.wait(timeout=0.05)
    release_first.set()
    first_thread.join(timeout=2)
    second_thread.join(timeout=2)
    if first_thread.is_alive() or second_thread.is_alive():
        raise RuntimeError("COLLECTOR_LOCK_SELFTEST_DEADLOCK")
    return overlap


def _collector_serialization_mutation(project_root: Path, workspace: Path) -> dict[str, Any]:
    collector = project_root / "tools/collect_cra_no_action_soak.py"
    original = _mutant_module(
        collector,
        identity="collector_original",
        old="fcntl.flock(descriptor, fcntl.LOCK_EX)",
        new="fcntl.flock(descriptor, fcntl.LOCK_EX)",
        package="cra_harness",
    )
    mutant = _mutant_module(
        collector,
        identity="collector_serialization_deleted",
        old="fcntl.flock(descriptor, fcntl.LOCK_EX)",
        new="pass  # MUTANT: collector serialization removed",
        package="cra_harness",
    )
    sample_path = workspace / "samples.jsonl"
    original_overlap = _lock_allows_overlap(original._collector_lock, sample_path)
    mutant_overlap = _lock_allows_overlap(mutant._collector_lock, sample_path)
    return {
        "mutation_identity": "collector_serialization_deleted",
        "expected_detection": "CONCURRENT_COLLECTOR_OVERLAP",
        "original_overlap": original_overlap,
        "mutant_overlap": mutant_overlap,
        "mutant_detection": "PASS" if not original_overlap and mutant_overlap else "FAIL",
    }


def run_mutation_controls(project_root: Path, workspace: Path) -> dict[str, Any]:
    workspace.mkdir(parents=True, exist_ok=False)
    results = _gate_mutations(project_root)
    results.append(_collector_serialization_mutation(project_root, workspace))
    passed = sum(item["mutant_detection"] == "PASS" for item in results)
    return {
        "schema": "cra.harness_mutation_controls.v1",
        "classification": "PASS" if passed == len(results) else "HARNESS_FAILURE",
        "mutation_count": len(results),
        "detected_mutation_count": passed,
        "detection_rate": passed / len(results),
        "sut_failure_count": 0,
        "harness_failure_count": len(results) - passed,
        "mutations": results,
    }
