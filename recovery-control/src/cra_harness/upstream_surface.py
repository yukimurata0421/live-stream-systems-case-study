"""Local, source-bound audit of the stream_v3 OAuth observation boundary.

This does not read production credentials or infer a running release. A PASS
means the named candidate source and tests match the contract in this run.
"""

from __future__ import annotations

import ast
import hashlib
import json
import os
import re
import subprocess
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any

CONTRACT_PATH = "harness/contracts/upstream_observation_surfaces.v1.json"
_REQUIRED_TEST_IDS = frozenset({
    "broadcast_transient_each_code", "stream_transient_each_code",
    "broadcast_retry_exhausted", "stream_retry_exhausted",
    "broadcast_nontransient", "stream_nontransient",
    "broadcast_transport", "stream_transport", "action_retry_isolation",
})


def _file(root: Path, relative: str) -> Path:
    path = root / relative
    if (
        Path(relative).is_absolute() or ".." in Path(relative).parts
        or not path.resolve().is_relative_to(root.resolve())
        or path.is_symlink() or not path.is_file() or path.stat().st_size > 2 * 1024 * 1024
    ):
        raise ValueError(f"UPSTREAM_FILE_INVALID:{relative}")
    return path


def _hash(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _functions(tree: ast.Module) -> dict[str, ast.FunctionDef | ast.AsyncFunctionDef]:
    return {
        node.name: node for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }


def _calls(function: ast.AST, name: str) -> list[ast.Call]:
    return [
        node for node in ast.walk(function)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == name
    ]


def _int_assignment(tree: ast.Module, name: str) -> int | None:
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(isinstance(target, ast.Name) and target.id == name for target in node.targets):
            if isinstance(node.value, ast.Constant) and type(node.value.value) is int:
                return node.value.value
    return None


def _codes_assignment(tree: ast.Module, name: str) -> set[int] | None:
    for node in tree.body:
        if not isinstance(node, ast.Assign) or not any(isinstance(target, ast.Name) and target.id == name for target in node.targets):
            continue
        value = node.value
        if isinstance(value, ast.Call) and isinstance(value.func, ast.Name) and value.func.id == "frozenset" and len(value.args) == 1:
            try:
                codes = ast.literal_eval(value.args[0])
            except (ValueError, TypeError, SyntaxError):
                return None
            if isinstance(codes, set) and all(type(item) is int for item in codes):
                return codes
    return None


def _test_exists(tree: ast.Module, nodeid: str) -> bool:
    path, separator, rest = nodeid.partition("::")
    if not separator or not path.startswith("tests/") or not rest:
        return False
    parts = rest.split("::")
    body: list[ast.stmt] = tree.body
    for part in parts:
        match = next(
            (node for node in body if isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == part),
            None,
        )
        if match is None:
            return False
        body = match.body if isinstance(match, ast.ClassDef) else []
    return True


def _junit_results(path: Path) -> dict[str, bool]:
    root = ET.parse(path).getroot()
    results: dict[str, bool] = {}
    for case in root.iter("testcase"):
        classname, name = case.get("classname"), case.get("name")
        if not classname or not name:
            continue
        parts = classname.split(".")
        if len(parts) < 3:
            continue
        nodeid = "/".join(parts[:-1]) + ".py::" + parts[-1] + "::" + name
        if nodeid in results:
            raise ValueError(f"UPSTREAM_JUNIT_DUPLICATE:{nodeid}")
        results[nodeid] = not any(case.find(label) is not None for label in ("failure", "error", "skipped"))
    return results


def audit_upstream(
    cra_root: Path, producer_root: Path, *, pytest_bin: Path | None = None, junit_path: Path | None = None,
) -> dict[str, Any]:
    """Audit one explicit local producer checkout; execute only with an explicit test binary."""
    issues: list[str] = []
    contract_path = _file(cra_root, CONTRACT_PATH)
    contract = json.loads(contract_path.read_text(encoding="utf-8"))
    if contract.get("schema") != "cra.upstream_observation_surfaces.v1" or contract.get("producer") != "stream_v3":
        raise ValueError("UPSTREAM_CONTRACT_INVALID")
    source_path = _file(producer_root, contract["source_file"])
    source = ast.parse(source_path.read_text(encoding="utf-8"))
    functions = _functions(source)
    call_symbol = contract["call_symbol"]
    discovered: list[tuple[str, str]] = []
    for function in functions.values():
        for call in _calls(function, call_symbol):
            endpoint = call.args[0].value if call.args and isinstance(call.args[0], ast.Constant) and isinstance(call.args[0].value, str) else "<dynamic>"
            discovered.append((function.name, endpoint))
    declared = [(item["function"], item["endpoint"]) for item in contract["callsites"]]
    if sorted(discovered) != sorted(declared) or len(set(declared)) != len(declared):
        issues.append("UPSTREAM_CALLSITE_SET_DRIFT")
    expected_roles = {
        "_list_bound_streams_with_transient_retry": "PROBE_READ",
        "list_owned_broadcasts": "SHARED_READ",
        "update_video_category_from_existing_snippet": "ACTION_SUPPORT_READ",
    }
    if {item["function"]: item.get("role") for item in contract["callsites"]} != expected_roles:
        issues.append("UPSTREAM_CALLSITE_ROLES_INVALID")
    for function in functions.values():
        if function.name != call_symbol and any(
            isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
            and node.func.attr == "api_get" and isinstance(node.func.value, ast.Name)
            and node.func.value.id == "live_api_runtime"
            for node in ast.walk(function)
        ):
            issues.append(f"UPSTREAM_DIRECT_API_BYPASS:{function.name}")
    entry = functions.get(contract["probe_entry"])
    stream = functions.get("_list_bound_streams_with_transient_retry")
    broadcast = functions.get("list_owned_broadcasts")
    wrapper = contract["retry_wrapper"]
    if (
        not entry or len(_calls(entry, wrapper)) != 1
        or len(_calls(entry, "_list_bound_streams_with_transient_retry")) != 1
        or len(_calls(entry, "list_owned_broadcasts")) != 1
    ):
        issues.append("UPSTREAM_PROBE_RETRY_WIRING_INVALID")
    if not stream or len(_calls(stream, wrapper)) != 1 or not broadcast or _calls(broadcast, wrapper):
        issues.append("UPSTREAM_SHARED_READ_RETRY_BOUNDARY_INVALID")
    for function in functions.values():
        if function.name not in {contract["probe_entry"], "_list_bound_streams_with_transient_retry"} and _calls(function, wrapper):
            issues.append(f"UPSTREAM_RETRY_LEAKED_OUTSIDE_PROBE:{function.name}")
    if _codes_assignment(source, "OAUTH_PROBE_TRANSIENT_HTTP_CODES") != set(contract["transient_http_codes"]):
        issues.append("UPSTREAM_TRANSIENT_CODE_SET_DRIFT")
    if _int_assignment(source, "OAUTH_PROBE_MAX_ATTEMPTS") != contract["maximum_attempts"]:
        issues.append("UPSTREAM_RETRY_BOUND_DRIFT")
    required_tests = contract["required_tests"]
    ids = [item["id"] for item in required_tests]
    nodeids = [item["nodeid"] for item in required_tests]
    if set(ids) != _REQUIRED_TEST_IDS or len(ids) != len(_REQUIRED_TEST_IDS) or len(set(nodeids)) != len(nodeids):
        issues.append("UPSTREAM_TEST_MATRIX_INVALID")
    test_paths = sorted({nodeid.split("::", 1)[0] for nodeid in nodeids})
    test_hashes = {_path: _hash(_file(producer_root, _path)) for _path in test_paths}
    test_trees = {path: ast.parse(_file(producer_root, path).read_text(encoding="utf-8")) for path in test_paths}
    issues.extend(f"UPSTREAM_TEST_NODE_MISSING:{nodeid}" for nodeid in nodeids if not _test_exists(test_trees[nodeid.split("::", 1)[0]], nodeid))
    source_hash_before = _hash(source_path)
    execution = "NOT_RUN"
    runner_hash: str | None = None
    junit_hash: str | None = None
    if pytest_bin is not None:
        if junit_path is None or not pytest_bin.is_file() or pytest_bin.is_symlink() or junit_path.exists():
            issues.append("UPSTREAM_TEST_RUNNER_INVALID")
        else:
            junit_path.parent.mkdir(parents=True, exist_ok=True)
            runner_hash = _hash(pytest_bin)
            environment = dict(os.environ)
            environment["PYTHONPATH"] = "src"
            try:
                run = subprocess.run(
                    [str(pytest_bin), "-q", *nodeids, f"--junitxml={junit_path}"],
                    cwd=producer_root, env=environment, text=True, capture_output=True, timeout=120,
                )
            except subprocess.TimeoutExpired:
                issues.append("UPSTREAM_TEST_RUN_TIMEOUT")
                run = None
            if run is not None and _hash(pytest_bin) != runner_hash:
                issues.append("UPSTREAM_TEST_RUNNER_CHANGED")
            execution = "PASS" if run is not None and run.returncode == 0 else "FAIL"
            if run is not None and run.returncode != 0:
                issues.append("UPSTREAM_TEST_RUN_FAILED")
            try:
                results = _junit_results(junit_path)
                issues.extend(f"UPSTREAM_TEST_NOT_PASS:{nodeid}" for nodeid in nodeids if results.get(nodeid) is not True)
                junit_hash = _hash(junit_path)
            except (ET.ParseError, OSError, ValueError):
                issues.append("UPSTREAM_JUNIT_INVALID")
            if _hash(source_path) != source_hash_before or any(_hash(_file(producer_root, path)) != digest for path, digest in test_hashes.items()):
                issues.append("UPSTREAM_SOURCE_CHANGED_DURING_TEST")
    else:
        issues.append("UPSTREAM_TEST_EXECUTION_REQUIRED")
    return {
        "schema": "cra.upstream_observation_audit.v1",
        "status": "PASS" if not issues else "HOLD",
        "producer": contract["producer"],
        "source_file": contract["source_file"],
        "source_sha256": source_hash_before,
        "test_sha256": test_hashes,
        "contract_sha256": _hash(contract_path),
        "discovered_callsites": [list(item) for item in sorted(discovered)],
        "required_test_count": len(nodeids),
        "test_execution": execution,
        "test_runner_path": str(pytest_bin.resolve()) if runner_hash is not None and pytest_bin is not None else None,
        "test_runner_sha256": runner_hash,
        "junit_sha256": junit_hash,
        "issues": sorted(set(issues)),
        "claim_boundary": contract["claim_boundary"],
    }


def verify_upstream_audit(cra_root: Path, producer_root: Path, artifact_root: Path) -> list[str]:
    """Rebind a saved local test run to the current candidate before pre-soak use."""
    issues: list[str] = []
    try:
        summary = json.loads(_file(artifact_root, "summary.json").read_text(encoding="utf-8"))
        junit = _file(artifact_root, "tests.xml")
        current = audit_upstream(cra_root, producer_root)
        contract = json.loads(_file(cra_root, CONTRACT_PATH).read_text(encoding="utf-8"))
        nodeids = {item["nodeid"] for item in contract["required_tests"]}
        results = _junit_results(junit)
    except (OSError, ValueError, TypeError, KeyError, SyntaxError, ET.ParseError, json.JSONDecodeError) as error:
        return [f"UPSTREAM_AUDIT_ARTIFACT_INVALID:{type(error).__name__}:{error}"]
    if current["issues"] != ["UPSTREAM_TEST_EXECUTION_REQUIRED"]:
        issues.append("UPSTREAM_CURRENT_SOURCE_INVALID")
    if summary.get("status") != "PASS" or summary.get("test_execution") != "PASS" or summary.get("issues") != []:
        issues.append("UPSTREAM_AUDIT_NOT_PASS")
    runner_path = summary.get("test_runner_path")
    if (
        not isinstance(runner_path, str) or not Path(runner_path).is_absolute()
        or not Path(runner_path).is_file() or Path(runner_path).is_symlink()
        or not isinstance(summary.get("test_runner_sha256"), str)
        or re.fullmatch(r"[0-9a-f]{64}", summary["test_runner_sha256"]) is None
        or _hash(Path(runner_path)) != summary["test_runner_sha256"]
    ):
        issues.append("UPSTREAM_TEST_RUNNER_IDENTITY_MISSING")
    for key in (
        "schema", "producer", "source_file", "source_sha256", "test_sha256", "contract_sha256",
        "discovered_callsites", "required_test_count", "claim_boundary",
    ):
        if summary.get(key) != current.get(key):
            issues.append(f"UPSTREAM_AUDIT_SOURCE_DRIFT:{key}")
    if summary.get("junit_sha256") != _hash(junit):
        issues.append("UPSTREAM_AUDIT_JUNIT_DRIFT")
    issues.extend(f"UPSTREAM_AUDIT_TEST_NOT_PASS:{nodeid}" for nodeid in sorted(nodeids) if results.get(nodeid) is not True)
    return sorted(set(issues))
