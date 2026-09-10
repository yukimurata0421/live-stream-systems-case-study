"""Source-bound regression and real mutation controls in isolated copies.

No live paths, network, production credentials, host discovery or deployment.
A failed import, timeout, skipped test, empty selection or source drift is HOLD.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
import xml.etree.ElementTree as ET
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from cra_harness.task_contract import contract_gate, prepare, report, source_hashes, verify_context
from tools.run_candidate_full_validation import classify_test_result

CATALOG = "harness/scenarios/owner_observation_chaos_v1.json"
TEST = "tests/harness/integration/test_owner_observation_chaos.py"
LIFE_TEST = "tests/harness/integration/test_owner_lifecycle_evidence.py"
OWNER = "src/runtime_boundary/recovery_publisher.py"
FACTS = "src/cra_no_action_soak/recovery_facts.py"
BASELINE = "0d13385a25828c66942dbdcc7015573ad3b2d4eb"
ESRCH_BASELINE = "c5f903a8315c04b8bbdc67efcee260494be2f902"
CMDLINE_BASELINE = "5846bade15fabdcc41ed183c82b9cd60c16ed9c9"
CMDLINE_TEST = "tests/harness/integration/test_owner_cmdline_exit.py"


def hashes(root: Path) -> dict[str, str]:
    files = [*root.glob("src/**/*.py"), *root.glob("tests/**/*.py"), *root.glob("tools/*.py"), root / CATALOG, root / "pyproject.toml"]
    return {**{str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(files)}, **source_hashes(root)}


def test_run(root: Path, output: Path, tests: list[str], *, timeout: float = 120) -> dict[str, Any]:
    output.mkdir()
    env = {k: v for k, v in os.environ.items() if not k.startswith("COV_CORE_") and k != "COVERAGE_PROCESS_START"}
    env.update(PYTHONPATH=str(root / "src") + os.pathsep + str(root), OPENBLAS_NUM_THREADS="1", OMP_NUM_THREADS="1")
    args = [sys.executable, "-m", "pytest", "-q", "--tb=short", "-o", "junit_family=legacy", "-p", "no:cacheprovider"]
    try:
        collected = subprocess.run([*args, "--collect-only", *tests], cwd=root, env=env, text=True, capture_output=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        return {"classification": "TIMEOUT", "reason": "COLLECTION_TIMEOUT"}
    (output / "collection.log").write_text(collected.stdout + collected.stderr)
    nodes = [line for line in collected.stdout.splitlines() if line.startswith("tests/") and "::test_" in line]
    if collected.returncode != 0 or not nodes:
        return {"classification": "HARNESS_FAILURE", "reason": "EMPTY_OR_FAILED_COLLECTION"}
    (output / "nodeids.json").write_text(json.dumps(nodes, indent=2) + "\n")
    try:
        run = subprocess.run(
            [*args, "--junitxml=" + str(output / "result.xml"), *tests], cwd=root, env=env, text=True, capture_output=True, timeout=timeout
        )
    except subprocess.TimeoutExpired:
        return {"classification": "TIMEOUT"}
    (output / "pytest.log").write_text(run.stdout + run.stderr)
    if not (output / "result.xml").is_file():
        return {"classification": "HARNESS_FAILURE", "reason": "JUNIT_MISSING"}
    tree = ET.parse(output / "result.xml").getroot()
    suites = list(tree.iter("testsuite"))
    counts = {key: sum(int(s.get(key, "0")) for s in suites) for key in ["tests", "failures", "errors", "skipped"]}
    classification = classify_test_result(run.returncode, counts, artifacts_complete=counts["tests"] == len(nodes))
    failures = [{"message": f.get("message", ""), "text": f.text or ""} for f in tree.iter("failure")]
    return {"classification": classification, "returncode": run.returncode, "counts": counts, "collected": len(nodes), "failures": failures}


def mutation_detected(data: dict[str, Any], fragment: str) -> bool:
    """Only the intended behavioral assertion proves that a mutant was caught."""
    if (
        data.get("classification") != "TEST_FAILURE"
        or data.get("counts") != {"tests": 1, "failures": 1, "errors": 0, "skipped": 0}
        or data.get("collected") != 1
        or not isinstance(data.get("failures"), list)
        or len(data["failures"]) != 1
    ):
        return False
    failure = data["failures"][0]
    return (
        isinstance(failure, dict)
        and fragment in failure.get("message", "")
        and not any(bad in failure.get("text", "") for bad in ["ModuleNotFoundError", "ImportError", "AttributeError", "SyntaxError"])
    )


def run(root: Path, output: Path) -> dict[str, Any]:
    root, output = root.resolve(), output.resolve()
    if output.exists():
        raise FileExistsError("OWNER_CHAOS_OUTPUT_EXISTS")
    catalog = json.loads((root / CATALOG).read_bytes())
    if (
        catalog["schema"] != "cra.owner_observation_chaos_catalog.v1"
        or catalog["assurance"]["schema"] != "cra.owner_contract_bindings.v3"
        or catalog["execution_boundary"]["production_mutation"] is not False
    ):
        raise ValueError("OWNER_CHAOS_CATALOG_INVALID")
    output.mkdir(parents=True, mode=0o700)
    prepare(root, output / "context")
    before = hashes(root)
    (output / "source_hashes_before.json").write_text(json.dumps(before, indent=2) + "\n")
    controls = [
        (
            "deployed_empty_cmdline_baseline",
            OWNER,
            None,
            None,
            [CMDLINE_TEST + "::test_real_kernel_unreaped_exit_returns_empty_cmdline[auxiliary-child_command-1]"],
            "RUNNING",
        ),
        (
            "deployed_empty_final_baseline",
            OWNER,
            None,
            None,
            [CMDLINE_TEST + "::test_empty_cmdline_never_bypasses_identity_or_read_failure[live-empty-child_command_final-2]"],
            "RUNNING",
        ),
        (
            "accept_unproven_empty",
            OWNER,
            '            final_state == "Z"',
            '            final_state in {"S", "Z"}',
            [CMDLINE_TEST + "::test_empty_cmdline_never_bypasses_identity_or_read_failure[live-empty-child_command-1]"],
            "RUNNING",
        ),
        (
            "accept_same_executable_gap",
            OWNER,
            '                    and executable_relation == "DIFFERENT"',
            '                    and executable_relation in {"SAME", "DIFFERENT"}',
            [CMDLINE_TEST + "::test_live_empty_command_corroboration_never_bypasses_unproven_identity[same-executable]"],
            "RUNNING",
        ),
        (
            "trust_unregistered_child",
            OWNER,
            '            if entry is None:\n                raise _ProcReadUnproven("RECOVERY_OWNER_CHILD_UNREGISTERED", process=capsule)',
            "            if entry is None:\n                continue  # Deliberate mutant: trust an unregistered child",
            [LIFE_TEST + "::test_registry_ambiguity_remains_unknown[unregistered]"],
            "UNKNOWN",
        ),
        (
            "trust_auxiliary_exec_as_delivery",
            OWNER,
            "            if managed_executable is not None and stable_executable == managed_executable:",
            "            if False:  # Deliberate mutant: trust auxiliary exec as delivery-safe",
            [LIFE_TEST + "::test_registry_ambiguity_remains_unknown[exec-to-delivery]"],
            "UNKNOWN",
        ),
        (
            "conflate_scheduler_state_with_identity",
            OWNER,
            "            if stable_executable != executable:\n",
            (
                "            if (stable_state, stable_parent, stable_ticks, stable_executable) != "
                "(state, parent, ticks, executable):  # Deliberate mutant: scheduler state is identity\n"
            ),
            [LIFE_TEST + "::test_registered_auxiliary_scheduler_state_churn_keeps_exact_delivery_child"],
            "RUNNING",
        ),
        (
            "skip_empty_identity",
            OWNER,
            '            final_state == "Z"\n            and (final_parent, final_ticks) == (parent, ticks)',
            '            final_state == "Z"\n            and True  # Deliberate mutant: ignore auxiliary identity',
            [CMDLINE_TEST + "::test_empty_cmdline_never_bypasses_identity_or_read_failure[pid-reuse-child_command-1]"],
            "RUNNING",
        ),
        (
            "skip_empty_final_check",
            OWNER,
            '                if not final_command and final_state != "Z":',
            "                if False:  # Deliberate mutant: skip empty final read validation",
            [CMDLINE_TEST + "::test_empty_cmdline_never_bypasses_identity_or_read_failure[live-empty-child_command_final-2]"],
            "RUNNING",
        ),
        (
            "deployed_esrch_baseline",
            OWNER,
            None,
            None,
            [TEST + "::test_real_kernel_exit_after_cmdline_open[auxiliary-child_command-1]"],
            "RUNNING",
        ),
        (
            "missing_esrch_retry",
            OWNER,
            "            except (FileNotFoundError, ProcessLookupError) as error:\n                # procfs may open successfully",
            "            except FileNotFoundError as error:\n                # procfs may open successfully",
            [TEST + "::test_real_kernel_exit_after_cmdline_open[auxiliary-child_command_final-2]"],
            "RUNNING",
        ),
        (
            "ignore_scan_deadline",
            OWNER,
            (
                "                if anchor is None or self._retry_anchor() != anchor:\n"
                '                    raise ValueError("RECOVERY_OWNER_RETRY_ANCHOR_CHANGED") from error\n'
                "                if attempt + 1 == PROC_SCAN_ATTEMPTS:\n"
                '                    raise ValueError("RECOVERY_OWNER_PROC_RETRY_EXHAUSTED") from error\n'
                "                if time.monotonic() >= until:"
            ),
            (
                "                if anchor is None or self._retry_anchor() != anchor:\n"
                '                    raise ValueError("RECOVERY_OWNER_RETRY_ANCHOR_CHANGED") from error\n'
                "                if attempt + 1 == PROC_SCAN_ATTEMPTS:\n"
                '                    raise ValueError("RECOVERY_OWNER_PROC_RETRY_EXHAUSTED") from error\n'
                "                if False:  # Deliberate mutant: ignore elapsed retry budget"
            ),
            [TEST + "::test_retry_never_certifies_unknown_or_changed_identity[deadline]"],
            "<= 2",
        ),
        (
            "forget_unknown",
            "src/cra_no_action_soak/recovery_soak.py",
            "    harness_classification = (",
            "    unknowns.clear()  # Deliberate mutant: erase earlier missing evidence\n    harness_classification = (",
            [TEST + "::test_signed_cleanup_and_unknown_retained_through_collector[permission]"],
            "PASS",
        ),
        (
            "misclassify_unverified_owner_as_healthy",
            "src/cra_no_action_soak/operator_status.py",
            '    elif findings:\n        current_sut_state = "RECOVERING" if live_health == "RECOVERING" else "NOT_OBSERVABLE"',
            '    elif findings:\n        current_sut_state = "VERIFIED_HEALTHY"  # Deliberate mutant',
            [TEST + "::test_signed_cleanup_and_unknown_retained_through_collector[permission]"],
            "NOT_OBSERVABLE",
        ),
        (
            "prune_reaped_identity_before_snapshot",
            "src/runtime_boundary/child_registry.py",
            (
                "        snapshot = {pid: dict(value) for pid, value in self._entries.items()}\n"
                "        self._prune_locked()\n"
                "        return snapshot"
            ),
            "        self._prune_locked()\n        return {pid: dict(value) for pid, value in self._entries.items()}",
            ["tests/runtime_boundary/test_child_registry.py::test_registry_returns_reaped_identity_once_then_prunes_it"],
            "registered_identity_retained",
        ),
        ("deployed_baseline", OWNER, None, None, [TEST + "::test_auxiliary_exit_does_not_invalidate_running_child"], "RUNNING"),
        (
            "no_retry",
            OWNER,
            "PROC_SCAN_ATTEMPTS = 3",
            "PROC_SCAN_ATTEMPTS = 1",
            [TEST + "::test_auxiliary_exit_does_not_invalidate_running_child"],
            "RUNNING",
        ),
        (
            "swallow_permission",
            OWNER,
            "                return self._scan_children()\n",
            (
                "                return self._scan_children()\n            except OSError:\n"
                "                return [self._child_anchor[1][0]]\n"
            ),
            [TEST + "::test_retry_never_certifies_unknown_or_changed_identity[permission]"],
            "RUNNING",
        ),
        (
            "skip_final_identity",
            OWNER,
            "final_target, final_process, final_lifecycle = self._lifecycle_context(checked_at)",
            "final_target, final_process, final_lifecycle = target, process, lifecycle",
            [LIFE_TEST + "::test_owner_snapshot_transition_preserves_only_proven_claims[exit]"],
            "ABSENT",
        ),
        (
            "accept_bad_diagnostics",
            FACTS,
            '        validate_diagnostics(value.get("read_diagnostics"))',
            "        pass  # Deliberate mutant: ignore diagnostics",
            [TEST + "::test_diagnostics_are_strictly_admitted[attempts]"],
            "DID NOT RAISE",
        ),
        (
            "skip_command_recheck",
            OWNER,
            "                if final_command != command:",
            "                if False:  # Deliberate mutant: permit exec during inspection",
            [TEST + "::test_retry_never_certifies_unknown_or_changed_identity[auxiliary-exec]"],
            "RUNNING",
        ),
        (
            "cache_activation",
            OWNER,
            '        self._read_stage = "snapshot_deadline"',
            (
                '        value["activation"] = {"bounded_termination_enabled": True, "rw_timeout_enabled": True}\n'
                '        self._read_stage = "snapshot_deadline"'
            ),
            [LIFE_TEST + "::test_live_child_revalidation_does_not_cache_activation_or_depend_on_async_target"],
            "False",
        ),
    ]
    result = {
        "schema": "cra.owner_observation_chaos_result.v1",
        "started_at": datetime.now(UTC).isoformat(),
        "production_mutation": False,
        "controls": [],
    }
    result["candidate"] = test_run(root, output / "candidate", catalog["test_files"])
    for name, relative, old, new, tests, failure_fragment in controls:
        copy = output / ("copy-" + name)
        copy.mkdir()
        for part in ["src", "tests"]:
            shutil.copytree(root / part, copy / part, ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))
        shutil.copyfile(root / "pyproject.toml", copy / "pyproject.toml")
        path = copy / relative
        original = path.read_text()
        if name.startswith("deployed_"):
            baseline = (
                CMDLINE_BASELINE
                if name.startswith("deployed_empty_")
                else (ESRCH_BASELINE if name == "deployed_esrch_baseline" else BASELINE)
            )
            replacement = subprocess.check_output(["git", "show", baseline + ":" + relative], cwd=root, text=True)
        else:
            if old is None or new is None or original.count(old) != 1:
                raise ValueError("OWNER_CHAOS_MUTATION_NOT_EXACT:" + name)
            replacement = original.replace(old, new, 1)
        path.write_text(replacement)
        data = test_run(copy, output / name, tests)
        data["name"] = name
        data["original_source_sha256"] = hashlib.sha256(original.encode()).hexdigest()
        data["mutated_source_sha256"] = hashlib.sha256(replacement.encode()).hexdigest()
        if name.startswith("deployed_"):
            data["baseline_commit"] = baseline
        data["detected"] = mutation_detected(data, failure_fragment)
        result["controls"].append(data)
        # Keep the changed file and failure logs; remove only this owned copy.
        shutil.copyfile(path, output / name / "mutated-source.py")
        shutil.rmtree(copy)
    after = hashes(root)
    result["context_errors"] = verify_context(root, output / "context")
    result["contract_gate"] = contract_gate(root, output / "candidate", result["controls"])
    result["source_stable"] = before == after and not result["context_errors"]
    result["classification"] = (
        "PASS"
        if result["candidate"]["classification"] == "PASS"
        and all(x["detected"] for x in result["controls"])
        and result["source_stable"]
        and result["contract_gate"]["classification"] == "PASS"
        else "HOLD"
    )
    result["finished_at"] = datetime.now(UTC).isoformat()
    (output / "source_hashes_after.json").write_text(json.dumps(after, indent=2) + "\n")
    (output / "summary.json").write_text(json.dumps(result, indent=2) + "\n")
    (output / "report.md").write_text(report(result), encoding="utf-8")
    artifacts = {str(p.relative_to(output)): hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(output.rglob("*")) if p.is_file()}
    (output / "artifact_hashes.json").write_text(json.dumps(artifacts, indent=2) + "\n")
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project-root", type=Path, default=Path.cwd())
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = run(args.project_root, args.output)
    print(
        json.dumps(
            {
                "classification": result["classification"],
                "candidate": result["candidate"],
                "controls": [{k: v for k, v in c.items() if k != "failures"} for c in result["controls"]],
            },
            indent=2,
        )
    )
    return 0 if result["classification"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
