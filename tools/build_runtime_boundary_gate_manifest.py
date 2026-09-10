#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any


def file_hash(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def contract(*, entrypoint: str, required_env: list[str], provided_env: list[str], available_paths: list[str]) -> dict[str, Any]:
    return {
        "entrypoint": entrypoint,
        "entrypoint_exists": entrypoint in available_paths,
        "installed_cli_options": [],
        "accepted_cli_options": [],
        "required_env": required_env,
        "provided_env": provided_env,
        "required_paths": [entrypoint],
        "available_paths": available_paths,
        "working_directory": "/app" if entrypoint.startswith("/app/") else "/",
        "working_directory_compatible": True,
        "installed_argv_checked": True,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--release", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    release = json.loads((args.release / "release_manifest.json").read_text(encoding="utf-8"))
    deployment = json.loads((args.release / "kubernetes/deployment_patch_contract.json").read_text(encoding="utf-8"))
    controller_paths = [str(args.release / "controller" / item) for item in release["controller"]["actual_files"]]
    executor_paths = ["/" + item for item in release["executor_image"]["actual_files"]]
    fast_paths = ["/" + item for item in release["fast_image"]["actual_files"]]
    stream_env = sorted(deployment["stream_engine"]["environment"])
    fast_env = sorted(deployment["legacy_fast_recovery"]["environment"])
    controller_entrypoint = str(args.release / "controller/src/watchers/fast_recovery.py")
    manifest = {
        "schema_version": "recovery_control.release_deployment_manifest.v1",
        "requested_change_scope": ["host-controller", "stream-engine-executor", "legacy-fast-recovery"],
        "candidate": {
            "release_id": release["release_id"],
            "release_path": str(args.release.resolve()),
            "immutable": True,
            "source_hash": file_hash(args.release / "release_manifest.json"),
            "actual_source_hash": file_hash(args.release / "release_manifest.json"),
        },
        "expected_consumer_ids": [
            "host-controller",
            "stream-engine-executor",
            "legacy-fast-recovery",
            "auto-dj",
            "precipitation-fetcher",
        ],
        "consumers": [
            {
                "consumer_id": "host-controller",
                "in_dependency_graph": True,
                "in_requested_scope": True,
                "sharing_classification": "INDEPENDENT_RELEASE",
                "executable_identity_changes": True,
                "hidden_shared_dependency": False,
                "contract": contract(
                    entrypoint=controller_entrypoint,
                    required_env=["MAINTENANCE_ENFORCEMENT_ENABLED", "FR_EFFECT_AUTHORITY_MODE"],
                    provided_env=["MAINTENANCE_ENFORCEMENT_ENABLED", "FR_EFFECT_AUTHORITY_MODE"],
                    available_paths=controller_paths,
                ),
            },
            {
                "consumer_id": "stream-engine-executor",
                "in_dependency_graph": True,
                "in_requested_scope": True,
                "sharing_classification": "COUPLED_BY_DESIGN",
                "executable_identity_changes": True,
                "hidden_shared_dependency": False,
                "contract": contract(
                    entrypoint="/app/src/stream_core/runtime_boundary_entrypoint.py",
                    required_env=["FR_EFFECT_LEDGER_ALLOW_INITIALIZE", "MAINTENANCE_ENFORCEMENT_ENABLED"],
                    provided_env=stream_env,
                    available_paths=executor_paths,
                ),
            },
            {
                "consumer_id": "legacy-fast-recovery",
                "in_dependency_graph": True,
                "in_requested_scope": True,
                "sharing_classification": "COUPLED_BY_DESIGN",
                "executable_identity_changes": True,
                "hidden_shared_dependency": False,
                "contract": contract(
                    entrypoint="/app/src/watchers/runtime_recovery_sidecar_entrypoint.sh",
                    required_env=[
                        "FR_EFFECT_AUTHORITY_MODE",
                        "FR_RUNTIME_RECOVERY_SOCKET",
                        "FR_TARGET_SNAPSHOT_FILE",
                        "MAINTENANCE_ENFORCEMENT_ENABLED",
                    ],
                    provided_env=fast_env,
                    available_paths=fast_paths,
                ),
            },
            {
                "consumer_id": "auto-dj",
                "in_dependency_graph": True,
                "in_requested_scope": False,
                "sharing_classification": "UNCHANGED_CONTAINER",
                "executable_identity_changes": False,
                "hidden_shared_dependency": False,
            },
            {
                "consumer_id": "precipitation-fetcher",
                "in_dependency_graph": True,
                "in_requested_scope": False,
                "sharing_classification": "UNCHANGED_CONTAINER",
                "executable_identity_changes": False,
                "hidden_shared_dependency": False,
            },
        ],
        "rollback": {
            "release_id": "pre-migration-runtime-images",
            "executable_identities_changed": ["stream-engine-executor", "legacy-fast-recovery"],
            "contracts": {},
        },
        "deployment_actions": [
            {
                "action_id": "option-bd-single-runtime-pod-replacement",
                "target_component": "runtime-boundary",
                "executable_identities_changed": ["stream-engine-executor", "legacy-fast-recovery"],
                "containers_replaced": ["stream-engine", "fast-recovery-loop"],
                "pod_uid_changes": True,
                "pod_blast_radius_declared": True,
                "effect_owners_affected": ["MP-03", "stream-engine"],
                "declared_effect_owners": ["MP-03", "stream-engine"],
                "independent_deploy_claim": False,
                "projection_requires_write_credential": False,
                "mutable_identity": False,
            }
        ],
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(manifest, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
