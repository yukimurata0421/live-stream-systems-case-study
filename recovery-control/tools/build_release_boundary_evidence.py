#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

CURRENT = "/opt/stream-v3/releases/<current-release-id>"
HOTFIX = "/opt/stream-v3/releases/<report-cli-release-id>"
STAGE_C = "/opt/stream-v3/releases/<evidence-binding-release-id>"


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def now_text() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def contract(*, options: list[str] | None = None, accepted: list[str] | None = None) -> dict[str, Any]:
    return {
        "entrypoint": "present",
        "entrypoint_exists": True,
        "installed_cli_options": options or [],
        "accepted_cli_options": accepted if accepted is not None else options or [],
        "required_env": [],
        "provided_env": [],
        "required_paths": [],
        "available_paths": [],
        "working_directory": CURRENT,
        "working_directory_compatible": True,
        "installed_argv_checked": True,
    }


def consumers() -> list[dict[str, Any]]:
    rows = [
        (
            "report-stream1090",
            "adsb-streamnew-stream1090-report.service",
            "bin/stream-prod stream1090-report --record",
            "REPORT",
            "timer oneshot",
        ),
        (
            "report-upstream",
            "adsb-streamnew-upstream-report.service",
            "bin/stream-prod upstream-report --record",
            "REPORT",
            "timer oneshot",
        ),
        (
            "prometheus-exporter",
            "adsb-streamnew-prometheus-exporter.service",
            "ops/scripts/stream_v3_prometheus_exporter.py",
            "OBSERVATION",
            "explicit restart",
        ),
        ("notify", "adsb-streamnew-notify.service", "bin/stream-prod notify-status", "NOTIFICATION", "timer/explicit start"),
        (
            "arena-monitor",
            "stream-v3-arena-monitor.service",
            "python -m stream_v3.control_loop --mode monitor",
            "DECISION_PRODUCER",
            "explicit restart",
        ),
        (
            "monitoring-watchdog",
            "stream-v3-monitoring-watchdog.service",
            "ops/scripts/stream_v3_monitoring_watchdog.py --repair",
            "MUTATOR",
            "timer oneshot",
        ),
        (
            "external-blackbox-import",
            "stream-v3-external-blackbox-import.service",
            "ops/scripts/stream_v3_external_blackbox_import.py",
            "OBSERVATION",
            "timer oneshot",
        ),
        (
            "health-snapshot",
            "stream-v3-health-snapshot.service",
            "ops/scripts/stream_v3_health_snapshot.py",
            "OBSERVATION",
            "timer oneshot",
        ),
        (
            "reliability-burn",
            "stream-v3-reliability-burn-evaluator.service",
            "ops/scripts/stream_v3_operational_reliability_rollup.py --quick",
            "OBSERVATION",
            "timer oneshot",
        ),
        ("shadow-sli", "stream-v3-shadow-sli.service", "bin/stream-prod shadow-sli --json", "OBSERVATION", "timer oneshot"),
        (
            "operational-rollup",
            "stream-v3-operational-reliability-rollup.service",
            "ops/scripts/stream_v3_operational_reliability_rollup.py",
            "OBSERVATION",
            "timer oneshot",
        ),
    ]
    result: list[dict[str, Any]] = []
    for consumer_id, unit, entrypoint, effect_owner, reload_semantics in rows:
        result.append(
            {
                "consumer_id": consumer_id,
                "host": "arena-server",
                "service_unit": unit,
                "entrypoint": entrypoint,
                "release_path": CURRENT,
                "shared_current_dependency": True,
                "sharing_classification": "ACCIDENTALLY_SHARED",
                "cli_contract": "CONFIRMED" if consumer_id.startswith("report-") else "SOURCE_MAPPED",
                "config_contract": "PARTIAL",
                "restart_reload_semantics": reload_semantics,
                "effect_owner": effect_owner,
                "rollback_unit": CURRENT,
                "evidence": "OBSERVED",
            }
        )
    result.extend(
        [
            {
                "consumer_id": "remote-recovery",
                "host": "arena-server",
                "service_unit": "stream-v3-remote-recovery.service",
                "entrypoint": "ops/scripts/stream_v3_remote_recovery.py",
                "release_path": STAGE_C,
                "shared_current_dependency": False,
                "sharing_classification": "SAFE_SHARED",
                "cli_contract": "SOURCE_MAPPED",
                "config_contract": "PARTIAL",
                "restart_reload_semantics": "timer oneshot",
                "effect_owner": "MUTATOR",
                "rollback_unit": STAGE_C,
                "evidence": "OBSERVED",
            },
            {
                "consumer_id": "public-publisher-source",
                "host": "UNKNOWN",
                "service_unit": "UNKNOWN",
                "entrypoint": "ops/public-publisher/site/scripts/collect_reliability.py",
                "release_path": CURRENT,
                "shared_current_dependency": True,
                "sharing_classification": "UNKNOWN",
                "cli_contract": "NOT_APPLICABLE",
                "config_contract": "SOURCE_DEFAULT_ONLY",
                "restart_reload_semantics": "UNKNOWN",
                "effect_owner": "PUBLIC_PROJECTION",
                "rollback_unit": CURRENT,
                "evidence": "OBSERVED_SOURCE_RUNTIME_UNKNOWN",
            },
        ]
    )
    return result


def release_graph() -> dict[str, Any]:
    consumer_rows = consumers()
    nodes: list[dict[str, Any]] = [
        {"node_id": "repo-stream-v3", "kind": "repository", "identity": "/srv/stream-v3"},
        {"node_id": "release-current", "kind": "mutable_pointer", "identity": CURRENT},
        {"node_id": "release-hotfix", "kind": "release", "identity": HOTFIX, "immutable": True},
        {"node_id": "release-stage-c", "kind": "release", "identity": STAGE_C, "immutable": True},
        {"node_id": "deployment-runtime", "kind": "Deployment", "identity": "stream-v3/stream-v3-runtime"},
        {"node_id": "pod-runtime", "kind": "Pod", "identity": "stream-v3-runtime-7cdcb75cdc-kddvg"},
    ]
    edges: list[dict[str, str]] = [
        {"from": "release-current", "to": "release-hotfix", "type": "resolves_to"},
        {"from": "deployment-runtime", "to": "pod-runtime", "type": "starts"},
    ]
    for row in consumer_rows:
        node_id = f"consumer-{row['consumer_id']}"
        nodes.append({"node_id": node_id, "kind": "consumer", **row})
        release = "release-current" if row["shared_current_dependency"] else "release-stage-c"
        edges.append({"from": node_id, "to": release, "type": "loads_from"})
    for container in ("stream-engine", "precipitation-fetcher", "auto-dj", "fast-recovery-loop"):
        node_id = f"container-{container}"
        nodes.append({"node_id": node_id, "kind": "container", "identity": container, "pod": "pod-runtime"})
        edges.extend(
            [
                {"from": "pod-runtime", "to": node_id, "type": "starts"},
                {"from": node_id, "to": "pod-runtime", "type": "restarts_with"},
            ]
        )
    edges.extend(
        [
            {"from": "container-fast-recovery-loop", "to": "container-stream-engine", "type": "shares_pid_namespace_with"},
            {"from": "container-fast-recovery-loop", "to": "container-stream-engine", "type": "effects_ffmpeg_child_of"},
        ]
    )
    return {
        "schema_version": "recovery_control.release_dependency_graph.v1",
        "observed_at": now_text(),
        "common_root_cause": "CONFIRMED",
        "nodes": nodes,
        "edges": edges,
        "consumer_count": len(consumer_rows),
        "shared_current_consumer_count": sum(bool(row["shared_current_dependency"]) for row in consumer_rows),
        "production_behavior_modified": False,
    }


def blast_radius_graph() -> dict[str, Any]:
    current_consumers = [row["consumer_id"] for row in consumers() if row["shared_current_dependency"]]
    return {
        "schema_version": "recovery_control.deployment_blast_radius_graph.v1",
        "observed_at": now_text(),
        "actions": [
            {
                "action_id": "arena-switch-shared-current",
                "source_files_changed": "candidate-dependent",
                "executable_identities_changed": current_consumers,
                "processes_restarted_immediately": [],
                "processes_changed_on_next_activation": current_consumers,
                "containers_restarted": [],
                "pod_uid_changes": False,
                "effect_owners_affected": ["REPORT", "OBSERVATION", "NOTIFICATION", "DECISION_PRODUCER", "MUTATOR", "PUBLIC_PROJECTION"],
                "rollback_boundary": "all shared-current consumers",
            },
            {
                "action_id": "k8s-change-fast-recovery-image-current-topology",
                "source_files_changed": ["MP-03 audit/evaluator"],
                "executable_identities_changed": ["fast-recovery-loop"],
                "processes_restarted_immediately": ["stream-engine", "precipitation-fetcher", "auto-dj", "fast-recovery-loop", "FFmpeg"],
                "containers_restarted": ["stream-engine", "precipitation-fetcher", "auto-dj", "fast-recovery-loop"],
                "pod_uid_changes": True,
                "effect_owners_affected": ["Fast Recovery", "stream-engine", "FFmpeg"],
                "rollback_boundary": "whole stream-v3-runtime Pod",
                "strategy": "Recreate",
            },
            {
                "action_id": "k8s-change-stream-engine-image-current-topology",
                "source_files_changed": ["MP-10 audit/evaluator"],
                "executable_identities_changed": ["stream-engine"],
                "processes_restarted_immediately": [
                    "stream-engine",
                    "precipitation-fetcher",
                    "auto-dj",
                    "fast-recovery-loop",
                    "FFmpeg",
                ],
                "containers_restarted": ["stream-engine", "precipitation-fetcher", "auto-dj", "fast-recovery-loop"],
                "pod_uid_changes": True,
                "effect_owners_affected": ["Fast Recovery", "stream-engine", "FFmpeg"],
                "rollback_boundary": "whole stream-v3-runtime Pod",
                "strategy": "Recreate",
            },
            {
                "action_id": "install-shadow-snapshot-projector",
                "source_files_changed": ["snapshot_projection"],
                "executable_identities_changed": ["maintenance-snapshot-projector"],
                "processes_restarted_immediately": [],
                "containers_restarted": [],
                "pod_uid_changes": False,
                "effect_owners_affected": [],
                "rollback_boundary": "projector service only",
            },
        ],
        "production_behavior_modified": False,
    }


def gate_manifest(*, stage_c_bad: bool, mp03: bool = False) -> dict[str, Any]:
    source_hash = "stage-c-tree" if stage_c_bad else "candidate-tree"
    report_contract = contract(options=["--record"], accepted=["--no-record"] if stage_c_bad else ["--record", "--no-record"])
    if mp03:
        return {
            "schema_version": "recovery_control.release_deployment_manifest.v1",
            "requested_change_scope": ["MP-03"],
            "candidate": {
                "release_id": "mp03-r2",
                "release_path": "/releases/mp03-r2",
                "immutable": True,
                "source_hash": source_hash,
                "actual_source_hash": source_hash,
            },
            "expected_consumer_ids": ["MP-03"],
            "consumers": [
                {
                    "consumer_id": "MP-03",
                    "in_dependency_graph": True,
                    "in_requested_scope": True,
                    "sharing_classification": "COUPLED_BY_DESIGN",
                    "executable_identity_changes": True,
                    "hidden_shared_dependency": False,
                    "contract": contract(),
                }
            ],
            "rollback": {"contracts": {"MP-03": contract()}, "executable_identities_changed": ["MP-03"]},
            "deployment_actions": [
                {
                    "action_id": "set-fast-recovery-image-in-current-deployment",
                    "target_component": "MP-03",
                    "executable_identities_changed": ["MP-03"],
                    "containers_replaced": ["stream-engine", "precipitation-fetcher", "auto-dj", "fast-recovery-loop"],
                    "pod_uid_changes": True,
                    "pod_blast_radius_declared": True,
                    "effect_owners_affected": ["Fast Recovery", "stream-engine", "FFmpeg"],
                    "declared_effect_owners": ["Fast Recovery", "stream-engine", "FFmpeg"],
                    "independent_deploy_claim": False,
                    "projection_requires_write_credential": False,
                    "mutable_identity": False,
                }
            ],
        }
    report_changes = True
    report_scope = not stage_c_bad
    consumers_manifest = [
        {
            "consumer_id": "remote-recovery",
            "in_dependency_graph": True,
            "in_requested_scope": stage_c_bad,
            "sharing_classification": "SAFE_SHARED",
            "executable_identity_changes": stage_c_bad,
            "hidden_shared_dependency": False,
            "contract": contract(),
        },
        {
            "consumer_id": "report",
            "in_dependency_graph": True,
            "in_requested_scope": report_scope,
            "sharing_classification": "ACCIDENTALLY_SHARED" if stage_c_bad else "COUPLED_BY_DESIGN",
            "executable_identity_changes": report_changes,
            "hidden_shared_dependency": stage_c_bad,
            "release_path": CURRENT,
            "contract": report_contract,
        },
    ]
    return {
        "schema_version": "recovery_control.release_deployment_manifest.v1",
        "requested_change_scope": ["remote-recovery"] if stage_c_bad else ["report"],
        "candidate": {
            "release_id": "stage-c-v2" if stage_c_bad else "report-hotfix-v1",
            "release_path": STAGE_C if stage_c_bad else HOTFIX,
            "immutable": True,
            "source_hash": source_hash,
            "actual_source_hash": source_hash,
        },
        "expected_consumer_ids": ["remote-recovery", "report"],
        "consumers": consumers_manifest,
        "rollback": {"contracts": {"remote-recovery": contract(), "report": report_contract}, "executable_identities_changed": []},
        "deployment_actions": [
            {
                "action_id": "switch-shared-current",
                "target_component": "remote-recovery" if stage_c_bad else "report",
                "executable_identities_changed": ["remote-recovery", "report"] if stage_c_bad else ["report"],
                "containers_replaced": [],
                "pod_uid_changes": False,
                "pod_blast_radius_declared": True,
                "effect_owners_affected": ["remote-recovery", "report"],
                "declared_effect_owners": ["remote-recovery", "report"],
                "independent_deploy_claim": False,
                "projection_requires_write_credential": False,
                "mutable_identity": stage_c_bad,
            }
        ],
    }


def mp10_gate_manifest() -> dict[str, Any]:
    return {
        "schema_version": "recovery_control.release_deployment_manifest.v1",
        "requested_change_scope": ["MP-10"],
        "candidate": {
            "release_id": "mp10-projection-p2-disabled-20260824t2059jst-v1",
            "release_path": "/releases/mp10-projection-p2-disabled-20260824t2059jst-v1",
            "immutable": True,
            "source_hash": "mp10-candidate-tree",
            "actual_source_hash": "mp10-candidate-tree",
        },
        "expected_consumer_ids": ["MP-10", "MP-03", "auto-dj", "precipitation-fetcher"],
        "consumers": [
            {
                "consumer_id": consumer_id,
                "in_dependency_graph": True,
                "in_requested_scope": consumer_id == "MP-10",
                "sharing_classification": "COUPLED_BY_DESIGN",
                "executable_identity_changes": consumer_id == "MP-10",
                "hidden_shared_dependency": False,
                "contract": contract(),
            }
            for consumer_id in ("MP-10", "MP-03", "auto-dj", "precipitation-fetcher")
        ],
        "rollback": {
            "contracts": {consumer_id: contract() for consumer_id in ("MP-10", "MP-03", "auto-dj", "precipitation-fetcher")},
            "executable_identities_changed": ["MP-10"],
        },
        "deployment_actions": [
            {
                "action_id": "set-stream-engine-image-in-current-deployment",
                "target_component": "MP-10",
                "executable_identities_changed": ["MP-10"],
                "containers_replaced": ["stream-engine", "precipitation-fetcher", "auto-dj", "fast-recovery-loop"],
                "pod_uid_changes": True,
                "pod_blast_radius_declared": True,
                "effect_owners_affected": ["Fast Recovery", "stream-engine", "FFmpeg"],
                "declared_effect_owners": ["Fast Recovery", "stream-engine", "FFmpeg"],
                "independent_deploy_claim": False,
                "projection_requires_write_credential": False,
                "mutable_identity": False,
            }
        ],
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--stream-v3-root", type=Path, required=True)
    args = parser.parse_args()
    output = args.output_dir
    write(output / "release_dependency_graph.json", release_graph())
    write(output / "blast_radius_graph.json", blast_radius_graph())
    write(output / "stage_c_bad_gate_manifest.json", gate_manifest(stage_c_bad=True))
    write(output / "report_hotfix_gate_manifest.json", gate_manifest(stage_c_bad=False))
    write(output / "mp03_current_topology_gate_manifest.json", gate_manifest(stage_c_bad=False, mp03=True))
    write(output / "mp10_current_topology_gate_manifest.json", mp10_gate_manifest())
    hashes = {
        "schema_version": "recovery_control.release_source_identity.v1",
        "parser": sha256(args.stream_v3_root / "src/stream_core/cli_support/parser.py"),
        "router": sha256(args.stream_v3_root / "src/stream_core/cli_support/router.py"),
        "observed_at": now_text(),
    }
    write(output / "source_identity.json", hashes)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
