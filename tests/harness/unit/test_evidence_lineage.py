from __future__ import annotations

import copy
import json
import subprocess
import sys
from pathlib import Path

from cra_harness.evidence_lineage import (
    DOC_PATH,
    MAP_PATH,
    build_view,
    check_map,
    load_map,
    mapping_digest,
    render_document,
    validate_live_overlay,
    validate_map,
)
from tools.build_recovery_soak_units import render as render_recovery_units

ROOT = Path(__file__).resolve().parents[3]


def canonical() -> dict[str, object]:
    return json.loads((ROOT / MAP_PATH).read_text(encoding="utf-8"))


def test_canonical_evidence_map_is_valid_and_generated_doc_is_current() -> None:
    value = load_map(ROOT)
    result = check_map(ROOT)

    assert result["status"] == "PASS"
    assert result["errors"] == []
    assert result["counts"] == {"hosts": 3, "layers": 5, "nodes": 42, "edges": 47, "restart_domains": 16}
    assert (ROOT / DOC_PATH).read_text(encoding="utf-8") == render_document(value)


def test_owner_contract_is_referenced_instead_of_duplicated() -> None:
    value = canonical()
    owner = value["owner_verification"]
    catalog = json.loads((ROOT / owner["catalog"]).read_text(encoding="utf-8"))

    assert owner["duplication_policy"] == "REFERENCE_CANONICAL_CATALOG"
    assert owner["component_id"] in {component["id"] for component in catalog["assurance"]["traceability"]["system_ir"]["components"]}
    assert "I-OWNER-" not in json.dumps(value, ensure_ascii=False)


def test_dynamic_recovery_unit_templates_are_emitted_by_canonical_builder() -> None:
    value = canonical()
    release = "lineage-map-test-1234"
    expected = {
        unit["template"].replace("<release>", release)
        for node in value["nodes"]
        for unit in node["unit_templates"]
        if unit["definition"] == "tools/build_recovery_soak_units.py"
    }
    rendered = {name for component in ("dell", "arena", "cra") for name in render_recovery_units(component, release)}

    assert expected
    assert expected <= rendered


def test_validator_rejects_broken_paths_symbols_graph_and_live_values() -> None:
    broken_path = canonical()
    broken_path["nodes"][0]["source_refs"][0] = "missing/source.py"
    assert any(item.startswith("NODE_SOURCE_MISSING:dell.host") for item in validate_map(ROOT, broken_path))

    broken_symbol = canonical()
    broken_symbol["nodes"][4]["implementation_refs"][0] = "runtime_boundary.child_registry::Missing.install"
    assert any(item.startswith("NODE_SYMBOL_MISSING:dell.owner") for item in validate_map(ROOT, broken_symbol))

    cyclic = canonical()
    edge = copy.deepcopy(next(item for item in cyclic["edges"] if item["id"] == "control.delivery_effect"))
    edge["id"], edge["from"], edge["to"] = "test.effect_runtime_cycle", "dell.effect_fence", "cra.runtime"
    cyclic["edges"].append(edge)
    assert "EVIDENCE_GRAPH_CYCLE" in validate_map(ROOT, cyclic)

    live_commit = canonical()
    live_commit["purpose"] += " 0123456789abcdef0123456789abcdef01234567"
    assert "CURRENT_SOURCE_COMMIT_EMBEDDED" in validate_map(ROOT, live_commit)


def test_validator_rejects_missing_restart_domain_and_disconnected_node() -> None:
    missing_restart = canonical()
    missing_restart["restart_domains"] = [item for item in missing_restart["restart_domains"] if item["id"] != "restart.ffmpeg_child"]
    assert "RESTART_DOMAIN_MISSING:restart.ffmpeg_child" in validate_map(ROOT, missing_restart)

    disconnected = canonical()
    disconnected["edges"] = [item for item in disconnected["edges"] if item["from"] != "dell.aux_child" and item["to"] != "dell.aux_child"]
    assert "NODE_DISCONNECTED:dell.aux_child" in validate_map(ROOT, disconnected)


def test_validator_requires_resilient_status_route_and_restart_coverage() -> None:
    missing_route = canonical()
    missing_route["edges"] = [item for item in missing_route["edges"] if item["id"] != "host_status.arena_cra"]
    assert "REQUIRED_ROUTE_EDGE_MISSING:host_status.arena_cra" in validate_map(ROOT, missing_route)

    wrong_route = canonical()
    edge = next(item for item in wrong_route["edges"] if item["id"] == "host_status.arena_cra")
    edge["to"] = "cra.projection_pull"
    assert "REQUIRED_ROUTE_EDGE_MISMATCH:host_status.arena_cra" in validate_map(ROOT, wrong_route)

    uncovered_unit = canonical()
    for restart in uncovered_unit["restart_domains"]:
        restart["scope_nodes"] = [node_id for node_id in restart["scope_nodes"] if node_id != "cra.watchdog"]
    assert "UNIT_NODE_RESTART_DOMAIN_MISSING:cra.watchdog" in validate_map(ROOT, uncovered_unit)

    escalated_loss = canonical()
    edge = next(item for item in escalated_loss["edges"] if item["id"] == "host_status.arena_cra")
    edge["failure_policy"]["soak_impact"] = "FAIL"
    assert "EVIDENCE_LOSS_ESCALATES_TO_FAILURE:host_status.arena_cra" in validate_map(ROOT, escalated_loss)


def test_task_view_selects_incident_edges_and_restart_domains() -> None:
    view = build_view(ROOT, node="dell.ffmpeg")

    assert view["status"] == "PASS"
    assert {item["id"] for item in view["nodes"]} >= {"dell.owner", "dell.ffmpeg", "dell.monitoring_publisher"}
    assert {item["id"] for item in view["edges"]} >= {"sut.owner_ffmpeg", "monitoring.sut_publisher"}
    assert "restart.ffmpeg_child" in {item["id"] for item in view["restart_domains"]}


def test_live_overlay_requires_map_window_identity_source_and_split_conclusions() -> None:
    value = load_map(ROOT)
    overlay = {
        "schema": "cra.evidence_lineage_live_overlay.v1",
        "mapping_sha256": mapping_digest(value),
        "window": {
            "started_at": "2026-09-09T15:00:00+09:00",
            "ended_at": "2026-09-09T15:01:00+09:00",
            "timezone": "JST",
        },
        "components": {
            node["id"]: {
                "status": "OBSERVED",
                "observed_at": "2026-09-09T15:00:30+09:00",
                "fresh_until": "2026-09-09T15:01:15+09:00",
                "identity": {field: f"observed-{field}" for field in node["identity_fields"]},
                "source": f"artifact/raw/current.json#/components/{node['id']}",
                "reason_code": None,
            }
            for node in value["nodes"]
        },
        "restart_observations": {
            restart["id"]: {
                "status": "OBSERVED",
                "count": 0,
                "latest_at": None,
                "source": f"artifact/raw/current.json#/restarts/{restart['id']}",
                "reason_code": None,
            }
            for restart in value["restart_domains"]
        },
        "conclusions": {
            "current_stream_health": {
                "status": "VERIFIED_HEALTHY",
                "failure_domain": "NONE",
                "reason_codes": [],
            },
            "evidence_harness_status": {
                "status": "PASS",
                "failure_domain": "NONE",
                "reason_codes": [],
            },
            "formal_soak_status": {
                "status": "NOT_YET_ELIGIBLE",
                "failure_domain": "NONE",
                "reason_codes": ["SOAK_DURATION_INSUFFICIENT"],
            },
        },
    }

    assert validate_live_overlay(ROOT, overlay) == []

    stale = copy.deepcopy(overlay)
    stale["mapping_sha256"] = "0" * 64
    stale["conclusions"]["current_stream_health"]["status"] = "UNKNOWN"
    stale["conclusions"]["current_stream_health"]["reason_codes"] = ["UNCLASSIFIED"]
    assert validate_live_overlay(ROOT, stale) == [
        "OVERLAY_CONCLUSION_UNCLASSIFIED:current_stream_health",
        "OVERLAY_MAPPING_DRIFT",
    ]

    missing_identity = copy.deepcopy(overlay)
    missing_identity["components"]["dell.ffmpeg"]["identity"]["ffmpeg_pid"] = ""
    assert validate_live_overlay(ROOT, missing_identity) == ["OVERLAY_IDENTITY_VALUE:dell.ffmpeg"]


def test_cra_task_evidence_map_cli_validates_and_filters() -> None:
    run = subprocess.run(
        [
            sys.executable,
            "tools/cra_task.py",
            "evidence-map",
            "--format",
            "json",
            "--restart-domain",
            "restart.ffmpeg_child",
        ],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )

    assert run.returncode == 0, run.stderr
    value = json.loads(run.stdout)
    assert value["status"] == "PASS"
    assert [item["id"] for item in value["restart_domains"]] == ["restart.ffmpeg_child"]
    assert "dell.ffmpeg" in {item["id"] for item in value["nodes"]}
