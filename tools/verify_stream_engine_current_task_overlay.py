#!/usr/bin/env python3
"""Independent Oracle for the current-task-only stream-engine overlay."""

from __future__ import annotations

import argparse
import ast
import hashlib
import json
from pathlib import Path
from typing import Any

EXPECTED_STATES = [
    "INITIALIZING",
    "STARTING_FFMPEG",
    "FFMPEG_RUNNING",
    "FFMPEG_EXITED",
    "RESTART_DELAY",
    "CONNECTIVITY_WAIT",
    "STOPPING",
]
FORBIDDEN_NEW_TEXT = (
    "audit_maintenance_decision",
    "systemctl",
    "kubectl",
)


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def call_name(node: ast.Call) -> str:
    function = node.func
    if isinstance(function, ast.Name):
        return function.id
    if isinstance(function, ast.Attribute):
        parts = [function.attr]
        value = function.value
        while isinstance(value, ast.Attribute):
            parts.append(value.attr)
            value = value.value
        if isinstance(value, ast.Name):
            parts.append(value.id)
        return ".".join(reversed(parts))
    return "UNKNOWN"


def facts(source: str) -> dict[str, Any]:
    tree = ast.parse(source)
    calls: dict[str, int] = {}
    lifecycle_states: list[str] = []
    methods: dict[str, ast.FunctionDef] = {}
    imports: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            name = call_name(node)
            calls[name] = calls.get(name, 0) + 1
        elif isinstance(node, (ast.Import, ast.ImportFrom)):
            imports.append(ast.dump(node, include_attributes=False))
        elif isinstance(node, ast.FunctionDef):
            methods[node.name] = node
        elif isinstance(node, (ast.Assign, ast.AnnAssign)):
            target = node.targets[0] if isinstance(node, ast.Assign) else node.target
            value = node.value
            if (
                isinstance(target, ast.Attribute)
                and target.attr == "ffmpeg_lifecycle_state"
                and isinstance(value, ast.Constant)
                and isinstance(value.value, str)
            ):
                lifecycle_states.append(value.value)
    return {
        "calls": calls,
        "imports": sorted(imports),
        "lifecycle_states": lifecycle_states,
        "methods": methods,
    }


def evaluate(base: str, candidate: str, expected_base_sha256: str) -> dict[str, bool]:
    base_facts = facts(base)
    candidate_facts = facts(candidate)
    method = candidate_facts["methods"].get("wait_for_ffmpeg_restart_delay")
    method_calls = [call_name(node) for node in ast.walk(method) if isinstance(node, ast.Call)] if method else []
    physical_calls = (
        "os.kill",
        "subprocess.Popen",
        "proc.terminate",
        "proc.kill",
        "self.browser_proc.terminate",
        "self.browser_proc.kill",
    )
    return {
        "base_identity": sha256_text(base) == expected_base_sha256,
        "candidate_changed": candidate != base,
        "imports_unchanged": base_facts["imports"] == candidate_facts["imports"],
        "physical_call_counts_unchanged": all(
            base_facts["calls"].get(name, 0) == candidate_facts["calls"].get(name, 0) for name in physical_calls
        ),
        "forbidden_new_text_absent": all(candidate.count(token) <= base.count(token) for token in FORBIDDEN_NEW_TEXT),
        "lifecycle_states_exact": sorted(candidate_facts["lifecycle_states"]) == sorted(EXPECTED_STATES)
        and len(candidate_facts["lifecycle_states"]) == len(EXPECTED_STATES)
        and [candidate.index(f'ffmpeg_lifecycle_state = "{state}"') for state in EXPECTED_STATES]
        == sorted(candidate.index(f'ffmpeg_lifecycle_state = "{state}"') for state in EXPECTED_STATES),
        "restart_delay_hook_exact": method_calls == ["time.sleep"],
        "legacy_restart_sleep_replaced_once": base.count("time.sleep(self.cfg.restart_delay_sec)") == 1
        and candidate.count("time.sleep(self.cfg.restart_delay_sec)") == 0
        and candidate.count("self.wait_for_ffmpeg_restart_delay(self.cfg.restart_delay_sec)") == 1,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", type=Path, required=True)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--expected-base-sha256", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    base = args.base.read_text(encoding="utf-8")
    candidate = args.candidate.read_text(encoding="utf-8")
    checks = evaluate(base, candidate, args.expected_base_sha256)
    controls = {
        "NC-OV-01_ADDED_TERMINATE": not all(evaluate(base, candidate + "\nproc.terminate()\n", args.expected_base_sha256).values()),
        "NC-OV-02_ADDED_AUDIT": not all(evaluate(base, candidate + "\naudit_maintenance_decision()\n", args.expected_base_sha256).values()),
        "NC-OV-03_BASE_DRIFT": not all(evaluate(base + "\n# drift\n", candidate, args.expected_base_sha256).values()),
        "NC-OV-04_LIFECYCLE_MISSING": not all(
            evaluate(base, candidate.replace('        self.ffmpeg_lifecycle_state = "STOPPING"\n', ""), args.expected_base_sha256).values()
        ),
        "NC-OV-05_DELAY_BYPASS": not all(
            evaluate(
                base,
                candidate.replace(
                    "self.wait_for_ffmpeg_restart_delay(self.cfg.restart_delay_sec)",
                    "time.sleep(self.cfg.restart_delay_sec)",
                ),
                args.expected_base_sha256,
            ).values()
        ),
        "NC-OV-06_POPEN_ADDED": not all(evaluate(base, candidate + "\nsubprocess.Popen([])\n", args.expected_base_sha256).values()),
    }
    result = {
        "schema_version": "stream_engine.current_task_overlay_oracle.v1",
        "checks": checks,
        "negative_controls": controls,
        "negative_controls_detected": sum(controls.values()),
        "negative_controls_total": len(controls),
        "independent_oracle": "PASS" if all(checks.values()) else "FAIL",
        "classification_ambiguity": 0,
        "physical_effect_count": 0,
        "complete": all(checks.values()) and all(controls.values()),
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(result, sort_keys=True))
    return 0 if result["complete"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
