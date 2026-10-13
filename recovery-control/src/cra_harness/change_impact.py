"""Bounded, source-derived change impact gate for CRA verification assets.

The index is a projection of existing catalogs and the evidence graph, not a
second scenario registry. It never reads production state or executes tests.
"""

from __future__ import annotations

import ast
import hashlib
import json
from collections import defaultdict
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

from cra_harness.evidence_lineage import DOC_PATH, MAP_PATH, load_map, render_document
from cra_harness.task_contract import CATALOG, SPEC, load_catalog
from cra_harness.upstream_surface import CONTRACT_PATH as UPSTREAM_CONTRACT_PATH
from tools.run_postmortem_io_harness import load_catalog as load_postmortem_catalog

REVIEW_PATH = "harness/contracts/change_impact_review.v1.json"
POSTMORTEM_PATH = "harness/scenarios/postmortem_io_v1.json"
ORACLE_SPEC = "docs/oracle/02_oracle_specification.md"
TRUST_SPEC = "docs/engineering/harness_trust_model.md"
KINDS = frozenset({"BEHAVIOR", "STRUCTURE", "NON_SEMANTIC", "HARNESS", "TEST_ONLY", "DOC_ONLY"})
def scenario_catalogs(root: Path) -> list[str]:
    return sorted(path.relative_to(root).as_posix() for path in (root / "harness/scenarios").glob("*.json"))


def _semantic_path(path: str, index: Mapping[str, Mapping[str, Any]]) -> bool:
    if path.startswith("src/"):
        return not path.startswith("src/cra_harness/") or path.startswith("src/cra_harness/oracles/")
    if path.startswith(("contracts/", "policy/", "migrations/", "ops/systemd/")):
        return True
    return path.startswith("tools/") and bool(index.get(path, {}).get("graph"))


def _source_path(reference: str) -> str:
    module = reference.split("::", 1)[0]
    prefix = "" if module.startswith("tools.") else "src/"
    return prefix + module.replace(".", "/") + ".py"


def _test_path(reference: str) -> str:
    return reference.split("::", 1)[0]


def _digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def is_tracked_change(path: str) -> bool:
    if path == REVIEW_PATH:
        return False
    if path.startswith(("src/", "tools/", "tests/")) and path.endswith(".py"):
        return True
    if path.startswith("harness/") and path.endswith(".json"):
        return True
    if path.startswith(("contracts/", "policy/")) and path.endswith(".json"):
        return True
    if path.startswith("migrations/") and path.endswith(".sql"):
        return True
    if path.startswith("ops/systemd/"):
        return True
    if path.startswith(("docs/oracle/", "docs/runbooks/")) and path.endswith(".md"):
        return True
    meta_paths = {TRUST_SPEC, "harness/README.md", "AGENTS.md", ".agents/skills/cra-evidence-workflow/SKILL.md", "pyproject.toml"}
    return path in meta_paths or path.startswith((".github/workflows/", ".forgejo/workflows/"))


def suggested_kind(path: str) -> str:
    if path == MAP_PATH:
        return "STRUCTURE"
    if path == "harness/contracts/evidence_lineage.v1.schema.json":
        return "STRUCTURE"
    if path == UPSTREAM_CONTRACT_PATH:
        return "BEHAVIOR"
    if path.startswith("src/cra_harness/oracles/"):
        return "BEHAVIOR"
    if path.startswith("src/") and not path.startswith("src/cra_harness/"):
        return "BEHAVIOR"
    if path.startswith("harness/scenarios/"):
        return "BEHAVIOR"
    if path.startswith("harness/fixtures/"):
        return "TEST_ONLY"
    if path.startswith(("contracts/", "policy/", "migrations/", "ops/systemd/")):
        return "BEHAVIOR"
    if path.startswith("tests/"):
        return "TEST_ONLY"
    if path.startswith("docs/") or path in {"harness/README.md", "AGENTS.md", ".agents/skills/cra-evidence-workflow/SKILL.md"}:
        return "DOC_ONLY"
    return "HARNESS"


def build_index(root: Path, *, graph: Mapping[str, Any] | None = None) -> dict[str, dict[str, Any]]:
    """Join existing graph, owner bindings, and PF scenarios by repo path."""
    graph = load_map(root) if graph is None else graph
    owner = load_catalog(root)
    postmortem = load_postmortem_catalog(root / POSTMORTEM_PATH, root)
    rows: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "graph": set(), "owner": set(), "postmortem": set(), "catalog": set(),
            "oracle_refs": set(), "independent_oracle_refs": set(), "tests": set(), "roles": set(),
        }
    )

    def add(path: str, family: str, identifier: str, role: str) -> None:
        if path:
            rows[path][family].add(identifier)
            rows[path]["roles"].add(role)

    nodes = {node["id"]: node for node in graph["nodes"]}
    edges = {edge["id"]: edge for edge in graph["edges"]}
    restarts = {domain["id"]: domain for domain in graph["restart_domains"]}
    for mode in graph["failure_modes"]:
        identifier = mode["id"]
        paths = set(mode["source_refs"])
        for node_id in mode["affected_nodes"]:
            node = nodes[node_id]
            paths.update(node["source_refs"])
            paths.update(_source_path(ref) for ref in node["implementation_refs"])
        for edge_id in mode["evidence_edges"]:
            paths.update(edges[edge_id]["contract_refs"])
        for restart_id in mode["restart_domains"]:
            for node_id in restarts[restart_id]["scope_nodes"]:
                paths.update(nodes[node_id]["source_refs"])
        for ref in mode["detector_refs"]:
            add(_source_path(ref), "graph", identifier, "detector")
        for ref in mode["oracle_refs"]:
            path = _source_path(ref)
            add(path, "graph", identifier, "oracle")
            rows[path]["oracle_refs"].add(ref)
            for candidate in paths:
                rows[candidate]["oracle_refs"].add(ref)
                if ref == "cra_harness.oracles.contract::IndependentOracle.evaluate_expectations":
                    rows[candidate]["independent_oracle_refs"].add(ref)
        for ref in mode["sequence_test_refs"]:
            path = _test_path(ref)
            add(path, "graph", identifier, "test")
            for candidate in paths:
                rows[candidate]["tests"].add(ref)
        for path in paths:
            add(path, "graph", identifier, "source")
        add(MAP_PATH, "graph", identifier, "catalog")

    trace = owner["assurance"]["traceability"]
    oracles = {item["id"]: item for item in trace["oracles"]}
    collectors = {item["id"]: item for item in trace["collectors"]}
    for binding in owner["assurance"]["bindings"]:
        identifier = binding["verification_case_id"]
        oracle_ref = oracles[binding["oracle_ref"]]["implementation"]
        collector_ref = collectors[oracles[binding["oracle_ref"]]["collector_ref"]]["implementation"]
        tests = {probe["nodeid"] for probe in binding["probes"]}
        sources = set(binding["sut"]) | {_source_path(oracle_ref), _source_path(collector_ref)}
        for path in sources:
            add(path, "owner", identifier, "source")
            rows[path]["oracle_refs"].add(oracle_ref)
            rows[path]["independent_oracle_refs"].add(oracle_ref)
            rows[path]["tests"].update(tests)
        add(_source_path(oracle_ref), "owner", identifier, "oracle")
        add(_source_path(collector_ref), "owner", identifier, "collector")
        for ref in tests:
            add(_test_path(ref), "owner", identifier, "test")
        for path in (CATALOG, SPEC):
            add(path, "owner", identifier, "catalog")
            rows[path]["oracle_refs"].add(oracle_ref)
            rows[path]["independent_oracle_refs"].add(oracle_ref)
            rows[path]["tests"].update(tests)

    for scenario in postmortem["scenarios"]:
        identifier = scenario["scenario_id"]
        tests = set(scenario["regression_nodeids"] + scenario["negative_control_nodeids"])
        for path in scenario["target_modules"]:
            add(path, "postmortem", identifier, "source")
            rows[path]["tests"].update(tests)
        for ref in tests:
            add(_test_path(ref), "postmortem", identifier, "test")
        add(POSTMORTEM_PATH, "postmortem", identifier, "catalog")
        rows[POSTMORTEM_PATH]["tests"].update(tests)

    # Discover literal catalog consumers from the actual Python AST. This is a
    # wiring index, not a claim that every scenario executed or passed. A new
    # catalog without a direct test consumer remains unindexed and blocks the
    # impact gate; no manual NO_CONTRACT_CHANGE can convert that to PASS.
    catalogs = scenario_catalogs(root)
    names = {Path(path).name: path for path in catalogs}
    name_bytes = tuple(name.encode("utf-8") for name in names)
    consumers: dict[str, set[str]] = defaultdict(set)
    test_imports: dict[str, set[str]] = defaultdict(set)
    for directory in ("src", "tools", "tests"):
        consumer_modules = tuple(
            path.removeprefix("src/").removesuffix(".py").replace("/", ".").encode("utf-8")
            for path in consumers
        ) if directory == "tests" else ()
        for source in (root / directory).rglob("*.py"):
            if source.is_symlink() or not source.is_file():
                continue
            relative = source.relative_to(root).as_posix()
            # The indexer and its own tests describe catalog names but are not
            # scenario runners. Counting them would make a dead catalog look
            # covered by the coverage test itself.
            if relative in {"src/cra_harness/change_impact.py", "tests/harness/unit/test_change_impact.py"}:
                continue
            content = source.read_bytes()
            if not any(name in content for name in name_bytes) and not (
                directory == "tests" and any(module in content for module in consumer_modules)
            ):
                continue
            tree = ast.parse(content, filename=relative)
            references = {
                names[value]
                for node in ast.walk(tree)
                if isinstance(node, ast.Constant) and isinstance(node.value, str)
                for value in (Path(node.value).name,)
                if value in names and (node.value == value or node.value.endswith("/" + value))
            }
            for catalog in references:
                add(relative, "catalog", catalog, "test" if directory == "tests" else "consumer")
                add(catalog, "catalog", catalog, "catalog")
                if directory == "tests":
                    rows[catalog]["tests"].add(relative)
                else:
                    consumers[relative].add(catalog)
            if directory == "tests":
                for node in ast.walk(tree):
                    if isinstance(node, ast.ImportFrom) and node.module:
                        test_imports[relative].add(node.module)
                    elif isinstance(node, ast.Import):
                        test_imports[relative].update(alias.name for alias in node.names)
    for test, imports in test_imports.items():
        for source, source_catalogs in consumers.items():
            module = source.removeprefix("src/").removesuffix(".py").replace("/", ".")
            if module in imports:
                for catalog in source_catalogs:
                    rows[catalog]["tests"].add(test)
                    add(test, "catalog", catalog, "test")
    for catalog in catalogs:
        if rows[catalog]["tests"]:
            add(catalog, "catalog", catalog, "catalog")
    for path, row in rows.items():
        for catalog in row["catalog"]:
            if path != catalog:
                row["tests"].update(rows[catalog]["tests"])
    return {path: {key: sorted(value) for key, value in row.items()} for path, row in sorted(rows.items())}


def unindexed_catalogs(root: Path, index: Mapping[str, Mapping[str, Any]] | None = None) -> list[str]:
    index = build_index(root) if index is None else index
    return [
        catalog for catalog in scenario_catalogs(root)
        if not index.get(catalog, {}).get("tests")
        or not any(
            path.startswith(("src/", "tools/")) and catalog in row.get("catalog", ())
            for path, row in index.items()
        )
    ]


def changed_paths(root: Path, diff_ref: str) -> list[str]:
    """The caller must provide an explicit base; an empty CI diff is not inferred."""
    import subprocess

    if not diff_ref or diff_ref.startswith("-"):
        raise ValueError("IMPACT_BASE_REQUIRED")
    result = subprocess.run(
        ["git", "diff", "--name-only", "--no-renames", "-z", diff_ref, "--"],
        cwd=root, capture_output=True, check=True,
    )
    untracked = subprocess.run(
        ["git", "ls-files", "--others", "--exclude-standard", "-z"],
        cwd=root, capture_output=True, check=True,
    )
    names = (result.stdout + untracked.stdout).decode("utf-8", errors="surrogateescape").split("\0")
    return sorted({path for path in names if is_tracked_change(path)})


def make_review(root: Path, paths: Iterable[str], reason: str) -> dict[str, Any]:
    if len(reason.strip()) < 40:
        raise ValueError("IMPACT_REVIEW_REASON_TOO_SHORT")
    index = build_index(root)
    changed = sorted({path for path in paths if is_tracked_change(path)})
    return {
        "schema": "cra.change_impact_review.v1",
        "reason": reason.strip(),
        "decisions": {
            path: {
                "sha256": _digest(root / path),
                "kind": suggested_kind(path),
                "impacts": {family: index.get(path, {}).get(family, []) for family in ("graph", "owner", "postmortem", "catalog")},
            }
            for path in changed
            if is_tracked_change(path) and (root / path).is_file()
        },
    }


def check_impact(root: Path, paths: Iterable[str], review: Mapping[str, Any] | None) -> dict[str, Any]:
    root = root.resolve()
    changed = sorted({path for path in paths if is_tracked_change(path)})
    issues: list[str] = []
    try:
        graph = load_map(root)
        index = build_index(root, graph=graph)
        if (root / DOC_PATH).read_text(encoding="utf-8") != render_document(graph):
            issues.append("EVIDENCE_MAP_DOCUMENT_STALE")
    except (OSError, ValueError, TypeError, KeyError, json.JSONDecodeError) as error:
        index = {}
        issues.append(f"IMPACT_INDEX_INVALID:{type(error).__name__}")

    if changed:
        review_data = review if isinstance(review, Mapping) else {}
        decisions = review_data.get("decisions")
        if (
            not isinstance(decisions, dict)
            or review_data.get("schema") != "cra.change_impact_review.v1"
            or len(str(review_data.get("reason", "")).strip()) < 40
        ):
            issues.append("IMPACT_REVIEW_INVALID")
            decisions = {}
        if set(decisions) != set(changed):
            issues.append("IMPACT_REVIEW_PATH_SET_MISMATCH")
    else:
        decisions = {}

    changed_set = set(changed)
    unindexed = unindexed_catalogs(root, index)
    issues.extend(f"UNINDEXED_SCENARIO_CATALOG:{path}" for path in unindexed)
    for path in changed:
        source = path.startswith("src/") and _semantic_path(path, index)
        if source and not (index.get(path, {}).get("graph") or index.get(path, {}).get("owner") or index.get(path, {}).get("postmortem")):
            issues.append(f"UNMAPPED_SUT_SOURCE:{path}")
        if path.startswith("harness/scenarios/") and path in unindexed:
            issues.append(f"UNMAPPED_SCENARIO_CATALOG:{path}")
        if path.startswith(("contracts/", "policy/", "migrations/", "ops/systemd/")) and path not in index:
            issues.append(f"UNMAPPED_CONTRACT_SOURCE:{path}")
        if not (root / path).is_file() or (root / path).is_symlink():
            issues.append(f"IMPACT_PATH_MISSING:{path}")
            continue
        decision = decisions.get(path)
        if (
            not isinstance(decision, dict)
            or not {"sha256", "kind", "impacts"}.issubset(decision)
            or set(decision) - {"sha256", "kind", "impacts", "explanation"}
            or decision.get("kind") not in KINDS
        ):
            issues.append(f"IMPACT_DECISION_INVALID:{path}")
            continue
        expected_impacts = {family: index.get(path, {}).get(family, []) for family in ("graph", "owner", "postmortem", "catalog")}
        if decision["impacts"] != expected_impacts:
            issues.append(f"IMPACT_RELATION_STALE:{path}")
        if decision["sha256"] != _digest(root / path):
            issues.append(f"IMPACT_REVIEW_STALE:{path}")
        kind = decision["kind"]
        if kind == "NON_SEMANTIC" and len(str(decision.get("explanation", "")).strip()) < 30:
            issues.append(f"NON_SEMANTIC_REASON_MISSING:{path}")
        if source and kind not in {"BEHAVIOR", "STRUCTURE", "NON_SEMANTIC"}:
            issues.append(f"SUT_DECISION_TOO_WEAK:{path}")
        if kind == "BEHAVIOR":
            if not changed_set.intersection({ORACLE_SPEC, SPEC, TRUST_SPEC}):
                issues.append(f"BEHAVIOR_SPEC_NOT_UPDATED:{path}")
            tests = {_test_path(ref) for ref in index.get(path, {}).get("tests", [])}
            if not tests.intersection(changed_set):
                issues.append(f"BEHAVIOR_TEST_NOT_UPDATED:{path}")
        if kind == "STRUCTURE":
            if not {MAP_PATH, DOC_PATH}.issubset(changed_set):
                issues.append(f"STRUCTURE_MAP_NOT_UPDATED:{path}")
            if "tests/harness/unit/test_evidence_lineage.py" not in changed_set:
                issues.append(f"STRUCTURE_TEST_NOT_UPDATED:{path}")
        if kind == "HARNESS" and path.startswith(("src/", "tools/")):
            mapped_tests = {_test_path(ref) for ref in index.get(path, {}).get("tests", [])}
            if not (mapped_tests.intersection(changed_set) if mapped_tests else any(item.startswith("tests/") for item in changed)):
                issues.append(f"HARNESS_TEST_NOT_UPDATED:{path}")
    return {
        "schema": "cra.change_impact_gate.v1",
        "status": "PASS" if not issues else "HOLD",
        "checked_paths": len(changed),
        "issues": sorted(set(issues)),
        "paths": changed,
        "index": index,
        "scenario_catalog_index": {
            "indexed": sorted(set(scenario_catalogs(root)) - set(unindexed)),
            "unindexed": unindexed,
            "assessment_required": False,
            "assessment_completed": 0,
        },
    }


def page(
    report: Mapping[str, Any], *, offset: int = 0, limit: int = 20,
    selected: str | None = None, ref_offset: int = 0, ref_limit: int = 8,
) -> dict[str, Any]:
    if offset < 0 or not 1 <= limit <= 50:
        raise ValueError("IMPACT_PAGE_BOUNDS")
    if ref_offset < 0 or not 1 <= ref_limit <= 30:
        raise ValueError("IMPACT_REF_PAGE_BOUNDS")
    paths = [path for path in report["paths"] if selected is None or path == selected]
    items = []
    for path in paths[offset:offset + limit]:
        row = report["index"].get(path, {})
        items.append({
            "path": path,
            "relations": {
                key: {
                    "total": len(values),
                    "items": values[ref_offset:ref_offset + ref_limit]
                    if selected is not None or key in {"graph", "owner", "postmortem", "catalog"} else [],
                }
                for key, values in row.items()
            },
        })
    return {
        "schema": report["schema"], "status": report["status"], "checked_paths": report["checked_paths"],
        "issues": report["issues"][:50], "issue_count": len(report["issues"]),
        "scenario_catalog_index": report["scenario_catalog_index"],
        "total": len(paths), "offset": offset, "limit": limit,
        "ref_offset": ref_offset, "ref_limit": ref_limit, "items": items,
    }
