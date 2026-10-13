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
    assert result["counts"] == {
        "hosts": 3,
        "layers": 5,
        "nodes": 46,
        "edges": 53,
        "restart_domains": 17,
        "failure_modes": 9,
    }
    assert (ROOT / DOC_PATH).read_text(encoding="utf-8") == render_document(value)


def test_projection_pull_lineage_registers_oneshot_and_resident_units() -> None:
    node = next(item for item in canonical()["nodes"] if item["id"] == "cra.projection_pull")
    assert {
        "ops/systemd/cra-monitoring-projection-pull@.service",
        "ops/systemd/cra-monitoring-projection-pull-daemon@.service",
    } <= set(node["source_refs"])
    assert {item["template"] for item in node["unit_templates"]} == {
        "cra-monitoring-projection-pull@<release>.service",
        "cra-monitoring-projection-pull-daemon@<release>.service",
    }
    assert {
        "execution_mode",
        "resident_started_at",
        "cycle_sequence",
        "connection_generation",
    } <= set(node["identity_fields"])


def test_owner_contract_is_referenced_instead_of_duplicated() -> None:
    value = canonical()
    owner = value["owner_verification"]
    catalog = json.loads((ROOT / owner["catalog"]).read_text(encoding="utf-8"))

    assert owner["duplication_policy"] == "REFERENCE_CANONICAL_CATALOG"
    assert owner["component_id"] in {component["id"] for component in catalog["assurance"]["traceability"]["system_ir"]["components"]}
    assert "I-OWNER-" not in json.dumps(value, ensure_ascii=False)


def test_manual_route_grant_and_wire_share_owner_fence_lineage() -> None:
    mode = next(m for m in canonical()["failure_modes"] if m["id"]=="failure.owner_child_lifecycle")
    assert {"src/runtime_boundary/model.py", "src/runtime_boundary/ledger.py",
            "src/runtime_boundary/server.py", "src/runtime_boundary/operator_route.py",
            "contracts/runtime_boundary/typed_effect_request.v3.schema.json"} <= set(mode["source_refs"])
    assert {"tests/runtime_boundary/test_stream_route_fence.py::test_concurrent_route_and_restart_have_one_winner",
            "tests/runtime_boundary/test_stream_route_fence.py::test_server_rechecks_manual_grant_before_shared_fence"} <= set(mode["sequence_test_refs"])


def test_critical_control_entrypoints_have_reverse_lineage_registration() -> None:
    mapping = canonical()
    registered = {path for node in mapping["nodes"] for path in node["source_refs"]}
    critical = {
        "src/dell_recovery_agent/execution.py",
        "src/dell_recovery_agent/authority.py",
        "src/dell_recovery_agent/storage.py",
        "src/dell_recovery_agent/reconciliation.py",
        "src/dell_recovery_agent/runtime_effect_adapter.py",
        "src/cra_authority/authorizer.py",
        "src/cra_authority/archive_integrity.py",
        "src/cra_authority/verifier.py",
        "src/fast_recovery_controller/ack_delivery.py",
    }
    assert critical <= registered, sorted(critical - registered)
    assert all((ROOT / path).is_file() for path in critical)


def test_cra_runtime_lineage_binds_external_archive_integrity_boundary() -> None:
    mapping = canonical()
    runtime = next(node for node in mapping["nodes"] if node["id"] == "cra.runtime")
    mode = next(item for item in mapping["failure_modes"] if item["id"] == "failure.cra_control_safety")

    assert {
        "src/cra_authority/archive_integrity.py",
        "ops/systemd/cra-archive-integrity.example.json",
        "ops/systemd/cra-archive-integrity@.service",
        "ops/systemd/cra-archive-integrity@.timer",
    } <= set(runtime["source_refs"])
    assert "archive_integrity_checked_at" in runtime["freshness_fields"]
    assert "src/cra_authority/archive_integrity.py" in mode["source_refs"]
    assert {
        "tests/authority/test_cra_no_action_runtime.py::test_runtime_v3_consumes_external_integrity_without_running_full_check",
        "tests/authority/test_cra_no_action_runtime.py::test_runtime_v3_fails_closed_on_stale_external_integrity",
    } <= set(mode["sequence_test_refs"])


def test_arena_oauth_network_and_control_sources_are_independent_lineage_branches() -> None:
    mapping = canonical()
    edges = {edge["id"]: edge for edge in mapping["edges"]}
    assert {
        edges["soak.arena_oauth_source"]["from"],
        edges["soak.arena_network_source"]["from"],
        edges["soak.arena_control_source"]["from"],
    } == {"arena.oauth_source", "arena.network_source", "arena.control_path_source"}
    assert all(edges[name]["to"] == "arena.recovery_observer" for name in (
        "soak.arena_oauth_source", "soak.arena_network_source", "soak.arena_control_source"
    ))


def test_upstream_observation_contract_is_bound_to_source_failure_mode() -> None:
    mode = next(item for item in canonical()["failure_modes"] if item["id"] == "failure.upstream_oauth_observation")
    assert "harness/contracts/upstream_observation_surfaces.v1.json" in mode["source_refs"]
    assert "tests/harness/unit/test_upstream_surface.py::test_new_endpoint_and_probe_retry_leak_fail_closed" in mode["sequence_test_refs"]
    assert mode["evidence_absence_domain"] == "OBSERVATION_SOURCE"


def test_rtmps_mode_binds_platform_first_active_episode_and_negative_controls() -> None:
    mode = next(item for item in canonical()["failure_modes"] if item["id"] == "failure.rtmps_session_ack_stall")
    assert "docs/engineering/records/2026-09-22_153_platform_first_rtmps_overlap_countermeasure.md" in mode["source_refs"]
    assert {
        "tests/harness/unit/test_recovery_soak.py::test_historical_20260922_platform_first_active_rtmps_is_one_episode",
        "tests/harness/unit/test_recovery_window.py::test_active_rtmps_episode_provisionally_binds_platform_first_episode_until_replacement",
        "tests/harness/unit/test_recovery_window.py::test_active_rtmps_platform_binding_rejects_mismatched_replacement",
        "tests/harness/unit/test_recovery_window.py::test_late_rtmps_episode_does_not_merge_other_failure_domains",
        "tests/harness/unit/test_recovery_window.py::test_late_rtmps_episode_does_not_merge_concurrent_platform_and_control_failure",
    } <= set(mode["sequence_test_refs"])


def test_soak_harness_mode_covers_late_signed_history_and_startup_capacity() -> None:
    mapping = canonical()
    mode = next(item for item in mapping["failure_modes"] if item["id"] == "failure.soak_harness_or_oracle")
    history_edge = next(item for item in mapping["edges"] if item["id"] == "soak.history_replay")
    assert mode["evidence_absence_domain"] == "HARNESS"
    assert mode["formal_rule"] == "CERTIFICATION_PAUSED_IF_UNOBSERVABLE"
    assert {
        "tools/run_candidate_full_validation.py",
        "tools/evaluate_failure_mode_predeploy_gate.py",
    } <= set(mode["source_refs"])
    assert "source transitions by signed time" in history_edge["relationship"]
    assert "logical replay steps" in history_edge["evidence"]
    assert "cra_no_action_soak.recovery_window::RecoveryWindow.observe" in mode["oracle_refs"]
    assert {
        "tests/harness/unit/test_recovery_soak.py::test_startup_timer_burst_does_not_become_false_seven_day_capacity_failure",
        *{
            f"tests/harness/unit/test_recovery_window.py::test_late_signed_failure_does_not_shift_episode_onset_forward[{domain}]"
            for domain in ("network", "platform", "control", "target", "owner", "effect")
        },
        "tests/harness/unit/test_recovery_window.py::test_late_signed_network_recovery_uses_source_confirmation_for_deadline",
        "tests/harness/unit/test_recovery_window.py::test_microsecond_rtmps_identity_survives_every_sample_checkpoint",
        "tests/harness/unit/test_recovery_window.py::test_microsecond_network_identity_survives_checkpoint",
        "tests/harness/unit/test_recovery_window.py::test_microsecond_deadline_does_not_expire_early_after_checkpoint",
        "tests/harness/unit/test_recovery_window.py::test_late_session_loss_matches_owner_replacement_without_extending_deadline",
        "tests/harness/unit/test_recovery_window.py::test_late_session_loss_after_owner_completion_is_not_a_second_recovery",
        "tests/harness/unit/test_recovery_soak.py::test_signed_microsecond_session_loss_matches_owner_episode_in_live_and_replay",
        "tests/harness/unit/test_recovery_soak.py::test_formal_window_drift_from_same_signed_inputs_is_harness_failure",
        "tests/harness/unit/test_recovery_soak.py::test_previous_sample_schema_cannot_be_reused_for_new_checkpoint",
        "tests/harness/integration/test_postmortem_history_oracle.py::test_late_signed_packet_is_replayed_at_bounded_visibility_without_losing_source_time",
        "tests/harness/integration/test_postmortem_history_oracle.py::test_late_hidden_packet_cannot_be_skipped_or_replayed_after_lease",
        "tests/harness/integration/test_postmortem_history_oracle.py::test_late_packets_and_on_time_packet_share_one_millisecond_without_losing_history",
        "tests/harness/integration/test_postmortem_history_oracle.py::test_millisecond_collision_does_not_hide_signed_no_action_violation",
        "tests/harness/integration/test_postmortem_history_oracle.py::test_same_role_unsafe_then_safe_late_packets_are_both_evaluated",
        "tests/harness/integration/test_postmortem_history_oracle.py::test_expired_hidden_packet_remains_unverified_at_collision",
        "tests/harness/integration/test_postmortem_history_oracle.py::test_unexpected_replay_exception_is_harness_failure_not_missing_source",
        "tests/harness/integration/test_postmortem_history_oracle.py::test_negative_control_detects_hidden_packet_skipped_inside_logical_tie",
        "tests/harness/integration/test_postmortem_history_oracle.py::test_role_prefix_safety_survives_other_role_failure",
        "tests/harness/integration/test_postmortem_history_oracle.py::test_role_prefix_aggregates_failures_and_proven_violation",
        "tests/harness/integration/test_postmortem_history_oracle.py::test_negative_control_detects_skipped_dell_prefix_after_cra_failure",
        "tests/harness/unit/test_failure_mode_predeploy_gate.py::test_predeploy_gate_rejects_full_validation_external_source_drift",
        "tests/harness/unit/test_failure_mode_predeploy_gate.py::test_predeploy_gate_rejects_full_validation_external_source_tree_drift",
    } <= set(mode["sequence_test_refs"])


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


def test_validator_requires_failure_mode_closure_and_three_way_rtmps_qualification() -> None:
    missing_mode = canonical()
    missing_mode["failure_modes"] = [item for item in missing_mode["failure_modes"] if item["id"] != "failure.rtmps_session_ack_stall"]
    errors = validate_map(ROOT, missing_mode)
    assert "FAILURE_MODE_MISSING:failure.rtmps_session_ack_stall" in errors

    missing_tcp_collector_modes = canonical()
    missing_tcp_collector_modes["failure_modes"] = [
        item
        for item in missing_tcp_collector_modes["failure_modes"]
        if item["id"] not in {
            "failure.rtmps_session_ack_stall",
            "failure.ffmpeg_session_loss",
            "failure.cutover_reboot_persistence",
        }
    ]
    assert "FAILURE_MODE_NODE_UNCOVERED:dell.tcp_stall_collector" in validate_map(ROOT, missing_tcp_collector_modes)

    uncovered = canonical()
    for failure_mode in uncovered["failure_modes"]:
        failure_mode["evidence_edges"] = [edge_id for edge_id in failure_mode["evidence_edges"] if edge_id != "soak.rtmps_episode_observer"]
    assert "FAILURE_MODE_EDGE_UNCOVERED:soak.rtmps_episode_observer" in validate_map(ROOT, uncovered)

    missing_test = canonical()
    missing_test["failure_modes"][0]["sequence_test_refs"][0] = "tests/missing.py::test_missing"
    assert any(item.startswith("FAILURE_MODE_TEST_MISSING:") for item in validate_map(ROOT, missing_test))

    malformed_parameter = canonical()
    malformed_parameter["failure_modes"][0]["sequence_test_refs"][0] = (
        "tests/harness/unit/test_recovery_window.py::test_transport_gap_without_exact_successor_evidence_stays_unknown["
    )
    assert any(item.startswith("FAILURE_MODE_TEST_MISSING:") for item in validate_map(ROOT, malformed_parameter))

    incomplete = canonical()
    rtmps = next(item for item in incomplete["failure_modes"] if item["id"] == "failure.rtmps_session_ack_stall")
    rtmps["required_qualification"].remove("HISTORICAL_REPLAY")
    assert "RTMPS_FAILURE_MODE_QUALIFICATION_INCOMPLETE" in validate_map(ROOT, incomplete)

    incomplete_session = canonical()
    session = next(item for item in incomplete_session["failure_modes"] if item["id"] == "failure.ffmpeg_session_loss")
    session["required_qualification"].remove("PRODUCTION_READ_ONLY")
    assert "FFMPEG_SESSION_LOSS_QUALIFICATION_INCOMPLETE" in validate_map(ROOT, incomplete_session)


def test_task_view_selects_incident_edges_and_restart_domains() -> None:
    view = build_view(ROOT, node="dell.ffmpeg")

    assert view["status"] == "PASS"
    assert {item["id"] for item in view["nodes"]} >= {"dell.owner", "dell.ffmpeg", "dell.monitoring_publisher"}
    assert {item["id"] for item in view["edges"]} >= {"sut.owner_ffmpeg", "monitoring.sut_publisher"}
    assert "restart.ffmpeg_child" in {item["id"] for item in view["restart_domains"]}
    assert "failure.rtmps_session_ack_stall" in {item["id"] for item in view["failure_modes"]}


def test_live_overlay_requires_map_window_identity_source_and_split_conclusions(tmp_path: Path) -> None:
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
    snapshot = tmp_path / "synthetic-overlay.json"
    snapshot.write_text(json.dumps(overlay), encoding="utf-8")
    run = subprocess.run(
        [sys.executable, "tools/cra_task.py", "evidence-map", "--snapshot", str(snapshot), "--format", "json"],
        cwd=ROOT, text=True, capture_output=True, check=False,
    )
    cli = json.loads(run.stdout)
    assert run.returncode == 1
    assert cli["status"] == "HOLD"
    assert cli["overlay"]["status"] == "STRUCTURE_VALID"
    assert cli["overlay"]["raw_evidence_status"] == "UNVERIFIED"
    assert "OVERLAY_RAW_EVIDENCE_UNVERIFIED" in cli["errors"]

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

    old_observation = copy.deepcopy(overlay)
    old_observation["components"]["dell.ffmpeg"]["observed_at"] = "2020-01-01T00:00:00+09:00"
    old_observation["components"]["dell.ffmpeg"]["fresh_until"] = "2020-01-01T00:01:00+09:00"
    assert validate_live_overlay(ROOT, old_observation) == [
        "OVERLAY_COMPONENT_OUTSIDE_WINDOW:dell.ffmpeg",
        "OVERLAY_COMPONENT_STALE_AT_WINDOW_END:dell.ffmpeg",
    ]

    expired = copy.deepcopy(overlay)
    expired["components"]["dell.ffmpeg"]["fresh_until"] = "2026-09-09T15:00:45+09:00"
    assert validate_live_overlay(ROOT, expired) == ["OVERLAY_COMPONENT_STALE_AT_WINDOW_END:dell.ffmpeg"]

    future_restart = copy.deepcopy(overlay)
    future_restart["restart_observations"]["restart.ffmpeg_child"]["latest_at"] = "2026-09-09T15:02:00+09:00"
    assert validate_live_overlay(ROOT, future_restart) == ["OVERLAY_RESTART_AFTER_WINDOW:restart.ffmpeg_child"]

    unbound_source = copy.deepcopy(overlay)
    unbound_source["components"]["dell.ffmpeg"]["source"] = "synthetic"
    unbound_source["restart_observations"]["restart.ffmpeg_child"]["source"] = "../other.json#/restarts"
    assert validate_live_overlay(ROOT, unbound_source) == [
        "OVERLAY_RESTART_SOURCE_POINTER_INVALID:restart.ffmpeg_child",
        "OVERLAY_SOURCE_POINTER_INVALID:dell.ffmpeg",
    ]


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
