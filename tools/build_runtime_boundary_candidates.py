#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import shutil
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def tree_sha256(root: Path) -> str:
    digest = hashlib.sha256()
    for path in sorted(item for item in root.rglob("*") if item.is_file()):
        digest.update(str(path.relative_to(root)).encode())
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


def utc_text() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def copy_manifest(root: Path, sources: dict[str, Path]) -> dict[str, Any]:
    for relative, source in sources.items():
        if not source.is_file():
            raise SystemExit(f"missing source: {source}")
        destination = root / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, destination)
    actual = sorted(str(path.relative_to(root)) for path in root.rglob("*") if path.is_file())
    expected = sorted(sources)
    unexpected = sorted(set(actual) - set(expected))
    missing = sorted(set(expected) - set(actual))
    return {
        "expected_files": expected,
        "actual_files": actual,
        "unexpected_files": unexpected,
        "missing_files": missing,
        "source_files": {relative: {"source": str(sources[relative]), "sha256": sha256(root / relative)} for relative in expected},
        "tree_sha256": tree_sha256(root),
    }


def safe_controller_sources(v3: Path, recovery: Path) -> dict[str, Path]:
    result = {
        "src/watchers/fast_recovery.py": v3 / "src/watchers/fast_recovery.py",
        "src/maintenance_audit/__init__.py": v3 / "src/maintenance_audit/__init__.py",
        "src/maintenance_enforcement/__init__.py": v3 / "src/maintenance_enforcement/__init__.py",
        "src/maintenance_enforcement/model.py": v3 / "src/maintenance_enforcement/model.py",
        "src/snapshot_projection/__init__.py": recovery / "src/snapshot_projection/__init__.py",
        "src/snapshot_projection/model.py": recovery / "src/snapshot_projection/model.py",
        "src/stream_core/recovery_profile.py": v3 / "src/stream_core/recovery_profile.py",
        "tools/refresh_target_observer_credential.py": recovery / "tools/refresh_target_observer_credential.py",
    }
    for name in (
        "__init__.py",
        "budget.py",
        "connectivity_policy.py",
        "decision.py",
        "effect_contract.py",
        "probes.py",
        "remote_warning.py",
        "restart_context.py",
        "state.py",
        "tcp_metrics.py",
        "policy.py",
    ):
        result[f"src/watchers/fast_recovery_core/{name}"] = v3 / f"src/watchers/fast_recovery_core/{name}"
    for name in ("__init__.py", "client.py", "ledger.py", "model.py", "observation.py", "server.py", "target.py"):
        result[f"src/runtime_boundary/{name}"] = recovery / f"src/runtime_boundary/{name}"
    return result


def common_image_sources(v3: Path, recovery: Path) -> dict[str, Path]:
    result = {
        "app/src/maintenance_audit/__init__.py": v3 / "src/maintenance_audit/__init__.py",
        "app/src/maintenance_enforcement/__init__.py": v3 / "src/maintenance_enforcement/__init__.py",
        "app/src/maintenance_enforcement/model.py": v3 / "src/maintenance_enforcement/model.py",
        "app/src/snapshot_projection/__init__.py": recovery / "src/snapshot_projection/__init__.py",
        "app/src/snapshot_projection/model.py": recovery / "src/snapshot_projection/model.py",
    }
    for name in ("__init__.py", "client.py", "ledger.py", "model.py", "observation.py", "server.py", "target.py"):
        result[f"app/src/runtime_boundary/{name}"] = recovery / f"src/runtime_boundary/{name}"
    return result


def write_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(value.rstrip() + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--release-dir", type=Path, required=True)
    parser.add_argument("--release-id", required=True)
    parser.add_argument("--v3-root", type=Path, required=True)
    parser.add_argument("--recovery-root", type=Path, required=True)
    parser.add_argument("--stream-base-image", required=True)
    parser.add_argument("--stream-base-digest", required=True)
    parser.add_argument("--fast-base-image", required=True)
    parser.add_argument("--fast-base-digest", required=True)
    parser.add_argument("--stream-candidate-image", required=True)
    parser.add_argument("--fast-candidate-image", required=True)
    parser.add_argument("--state-pv-host-path", required=True)
    parser.add_argument("--stream-engine-source", type=Path)
    parser.add_argument("--stream-engine-overlay-manifest", type=Path)
    args = parser.parse_args()
    if args.release_dir.exists():
        raise SystemExit(f"release directory already exists: {args.release_dir}")
    args.release_dir.mkdir(parents=True)

    controller_root = args.release_dir / "controller"
    controller = copy_manifest(controller_root, safe_controller_sources(args.v3_root, args.recovery_root))
    forbidden_controller = {
        "src/watchers/fast_recovery_core/executor.py",
        "src/watchers/fast_recovery_core/legacy_preflight.py",
        "src/watchers/systemctl_control.py",
        "src/stream_core/supervisor/factory.py",
        "src/stream_core/stream_engine.py",
    }
    controller_actual = set(controller["actual_files"])
    controller["forbidden_files"] = sorted(forbidden_controller)
    controller["forbidden_files_present"] = sorted(controller_actual & forbidden_controller)

    common = common_image_sources(args.v3_root, args.recovery_root)
    stream_engine_source = args.stream_engine_source or args.v3_root / "src/stream_core/stream_engine.py"
    stream_engine_overlay_manifest: dict[str, Any] | None = None
    if args.stream_engine_overlay_manifest:
        stream_engine_overlay_manifest = json.loads(args.stream_engine_overlay_manifest.read_text(encoding="utf-8"))
        if not stream_engine_overlay_manifest.get("complete"):
            raise SystemExit("stream-engine overlay manifest is not complete")
        if stream_engine_overlay_manifest.get("output_sha256") != sha256(stream_engine_source):
            raise SystemExit("stream-engine overlay source identity mismatch")
        if stream_engine_overlay_manifest.get("physical_mutation_call_added") is not False:
            raise SystemExit("stream-engine overlay adds a physical mutation call")
        if stream_engine_overlay_manifest.get("production_behavior_modified") is not False:
            raise SystemExit("stream-engine overlay changes production behavior")
    executor_sources = {
        **common,
        "app/src/stream_core/runtime_boundary_entrypoint.py": args.v3_root / "src/stream_core/runtime_boundary_entrypoint.py",
        "app/src/stream_core/stream_engine.py": stream_engine_source,
    }
    executor_root = args.release_dir / "executor-image" / "overlay"
    executor = copy_manifest(executor_root, executor_sources)
    fast_sources = {
        **common,
        "app/src/watchers/fast_recovery.py": args.v3_root / "src/watchers/fast_recovery.py",
        "app/src/watchers/fast_recovery_core/effect_contract.py": args.v3_root / "src/watchers/fast_recovery_core/effect_contract.py",
        "app/src/watchers/fast_recovery_core/policy.py": args.v3_root / "src/watchers/fast_recovery_core/policy.py",
        "app/src/watchers/runtime_recovery_escalation.py": args.v3_root / "src/watchers/runtime_recovery_escalation.py",
        "app/src/watchers/runtime_recovery_sidecar_entrypoint.sh": args.v3_root / "src/watchers/runtime_recovery_sidecar_entrypoint.sh",
    }
    fast_root = args.release_dir / "fast-image" / "overlay"
    fast = copy_manifest(fast_root, fast_sources)

    for image_dir, base_image, overlay in (
        (args.release_dir / "executor-image", args.stream_base_image, executor),
        (args.release_dir / "fast-image", args.fast_base_image, fast),
    ):
        copy_lines = [f"COPY overlay/{relative} /{relative}" for relative in overlay["expected_files"]]
        base_capability_removal = []
        if image_dir.name == "fast-image":
            base_capability_removal = [
                "RUN rm -f /app/src/watchers/fast_recovery_core/executor.py \\",
                "          /app/src/watchers/fast_recovery_core/legacy_preflight.py \\",
                "          /app/src/watchers/systemctl_control.py \\",
                "    && rm -rf /app/src/stream_core/supervisor",
            ]
        write_text(
            image_dir / "Containerfile",
            "\n".join(
                [
                    f"FROM {base_image}",
                    *copy_lines,
                    *base_capability_removal,
                    "ENV MAINTENANCE_ENFORCEMENT_ENABLED=0",
                    f'LABEL org.stream-v3.runtime-boundary-release="{args.release_id}"',
                ]
            ),
        )

    opt_path = f"/opt/stream-recovery-control-{args.release_id}"
    source_files = {
        "youtube_watchdog_stats": f"{args.state_pv_host_path}/youtube_watchdog_stats.json",
        "youtube_quota_state": f"{args.state_pv_host_path}/youtube_quota_state.json",
    }
    unit = f"""
[Unit]
Description=stream-v3 independent Fast Recovery Decision Controller
After=network-online.target maintenance-snapshot-projection-shadow.service
Wants=network-online.target
Requires=maintenance-snapshot-projection-shadow.service

[Service]
Type=oneshot
User=stream-recovery
Group=stream-recovery
EnvironmentFile=/etc/stream-recovery-control/mp03-controller.env
Environment=PYTHONPATH={opt_path}/src
ExecStart=/usr/bin/python3 -m watchers.fast_recovery
UMask=0027
NoNewPrivileges=true
PrivateTmp=true
ProtectHome=true
ProtectSystem=strict
ReadOnlyPaths={opt_path} /var/lib/stream-recovery-control/projection/mp03 /run/stream-v3-control/source
ReadWritePaths=/var/lib/stream-recovery-control/controller /run/stream-v3-control
BindReadOnlyPaths={source_files["youtube_watchdog_stats"]}:/run/stream-v3-control/source/youtube_watchdog_stats.json
BindReadOnlyPaths=-{source_files["youtube_quota_state"]}:/run/stream-v3-control/source/youtube_quota_state.json
CapabilityBoundingSet=
RestrictAddressFamilies=AF_INET AF_INET6 AF_UNIX
LockPersonality=true
MemoryDenyWriteExecute=true
"""
    timer = """
[Unit]
Description=Run independent Fast Recovery Decision Controller every 10 seconds

[Timer]
OnBootSec=30s
OnUnitActiveSec=10s
AccuracySec=1s
Persistent=true
Unit=stream-v3-fast-recovery-controller.service

[Install]
WantedBy=timers.target
"""
    common_env = """
STREAM_RUNTIME_SUPERVISOR=k8s
FR_STREAM_SERVICE=adsb-streamnew-youtube-stream.service
FR_RTMP_HOST=a.rtmps.youtube.com
FR_DNS_HOST=a.rtmps.youtube.com
FR_RTMP_PORTS=443
FR_YTW_STATS_FILE=/run/stream-v3-control/source/youtube_watchdog_stats.json
FR_QUOTA_STATE_FILE=/run/stream-v3-control/source/youtube_quota_state.json
FR_RESTART_REASON_FILE=/var/lib/stream-recovery-control/controller/mp03-active/restart_reason.json
FR_EVENT_LOG_FILE=/var/lib/stream-recovery-control/controller/mp03-active/fast_recovery_events.jsonl
FR_STATE_FILE=/var/lib/stream-recovery-control/controller/mp03-active/fast_recovery_state.json
FR_CONTROLLER_ID=dell_fast_recovery_independent
FR_GPU_PREFLIGHT_ENABLED=0
FR_FFMPEG_MISSING_REQUIRE_CURRENT_POD_ESTABLISHED=0
MAINTENANCE_AUDIT_ENABLED=1
MAINTENANCE_AUDIT_HOST_ID=dell-yuki
MAINTENANCE_AUDIT_BIND_SOURCE_TARGET=1
MAINTENANCE_AUDIT_STATE_FILE=/var/lib/stream-recovery-control/projection/mp03/maintenance-snapshot.json
MAINTENANCE_AUDIT_PROJECTION_ENABLED=1
MAINTENANCE_AUDIT_PROJECTION_PRODUCER_ID=dell-maintenance-snapshot-projector
MAINTENANCE_AUDIT_SOURCE_PRODUCER_ID=arena-maintenance-shadow
MAINTENANCE_AUDIT_PROJECTION_HIGH_WATER_FILE=/var/lib/stream-recovery-control/controller/mp03-active/projection-high-water.json
MAINTENANCE_AUDIT_EVENT_FILE=/var/lib/stream-recovery-control/controller/mp03-active/maintenance-audit.jsonl
MAINTENANCE_AUDIT_HEALTH_FILE=/var/lib/stream-recovery-control/controller/mp03-active/maintenance-audit-health.json
MAINTENANCE_ENFORCEMENT_ENABLED=0
"""
    shadow_env = (
        common_env
        + """
FR_EXECUTION_MODE=shadow
FR_CONTROLLER_RUNTIME_MODE=SHADOW_OBSERVER_ONLY
FR_EFFECT_AUTHORITY_MODE=SHADOW_ONLY
FR_EFFECT_PRODUCER_ID=independent-controller
FR_EFFECT_PRODUCER_GENERATION=2
"""
    )
    active_env = (
        common_env
        + """
FR_EXECUTION_MODE=execute
FR_CONTROLLER_RUNTIME_MODE=NARROW_EXECUTOR_ONLY
FR_EFFECT_AUTHORITY_MODE=AUTO_FENCED
FR_EFFECT_EXECUTOR_SOCKET=/run/stream-v3-control/effect.sock
FR_RUNTIME_OBSERVATION_FILE=/run/stream-v3-control/runtime-observation.json
FR_RUNTIME_RECOVERY_SOCKET=/run/stream-v3-control/runtime-recovery.sock
FR_RUNTIME_RECOVERY_OBSERVATION_FILE=/run/stream-v3-control/runtime-recovery-observation.json
FR_EFFECT_PRODUCER_ID=independent-controller
FR_EFFECT_PRODUCER_GENERATION=2
"""
    )
    write_text(args.release_dir / "systemd/stream-v3-fast-recovery-controller.service", unit)
    write_text(args.release_dir / "systemd/stream-v3-fast-recovery-controller.timer", timer)
    write_text(args.release_dir / "config/mp03-controller-shadow.env", shadow_env)
    write_text(args.release_dir / "config/mp03-controller-active.env", active_env)

    credential_service = f"""
[Unit]
Description=Refresh read-only stream-v3 target observer credential
After=k3s.service
Requires=k3s.service

[Service]
Type=oneshot
User=root
Group=root
ExecStart=/usr/bin/python3 {opt_path}/tools/refresh_target_observer_credential.py \
  --kubeconfig /etc/stream-recovery-control/dell-target-observer.kubeconfig \
  --namespace stream-v3 --service-account cra-phase4-target-observer \
  --duration 24h --min-remaining-seconds 43200
UMask=0027
NoNewPrivileges=true
PrivateTmp=true
ProtectHome=true
ProtectSystem=strict
ReadOnlyPaths={opt_path} /etc/rancher/k3s
ReadWritePaths=/etc/stream-recovery-control
RestrictAddressFamilies=AF_INET AF_INET6 AF_UNIX
"""
    credential_timer = """
[Unit]
Description=Check target observer credential freshness every 6 hours

[Timer]
OnBootSec=5m
OnUnitActiveSec=6h
RandomizedDelaySec=5m
Persistent=true
Unit=stream-v3-target-observer-credential-refresh.service

[Install]
WantedBy=timers.target
"""
    credential_target = controller_root / "tools/refresh_target_observer_credential.py"
    write_text(args.release_dir / "systemd/stream-v3-target-observer-credential-refresh.service", credential_service)
    write_text(args.release_dir / "systemd/stream-v3-target-observer-credential-refresh.timer", credential_timer)

    deployment_patch = {
        "schema_version": "runtime_boundary.deployment_patch.v1",
        "deployment": "stream-v3/stream-v3-runtime",
        "stream_engine": {
            "image": args.stream_candidate_image,
            "command": ["/app/src/stream_v3/runtime_entrypoint.sh"],
            "args": ["python3", "-m", "stream_core.runtime_boundary_entrypoint"],
            "environment": {
                "FR_EFFECT_EXECUTOR_ENABLED": "1",
                "FR_EFFECT_LEDGER_FILE": "/state/runtime/fast_recovery_effects.sqlite3",
                "FR_EFFECT_LEDGER_ALLOW_INITIALIZE": "0",
                "FR_EFFECT_EXECUTOR_SOCKET": "/run/stream-v3-control/effect.sock",
                "FR_RUNTIME_OBSERVATION_FILE": "/run/stream-v3-control/runtime-observation.json",
                "FR_TARGET_SNAPSHOT_FILE": "/target-snapshot/target_snapshot.json",
                "FR_EFFECT_INITIAL_PRODUCER_ID": "legacy-in-pod",
                "FR_EFFECT_INITIAL_PRODUCER_GENERATION": "1",
                "FR_EFFECT_CONTROLLER_UID": "992",
                "FR_EFFECT_SOCKET_GID": "983",
                "MAINTENANCE_AUDIT_STATE_FILE": "/projection/maintenance-snapshot.json",
                "MAINTENANCE_AUDIT_PROJECTION_ENABLED": "1",
                "MAINTENANCE_ENFORCEMENT_ENABLED": "0",
            },
        },
        "legacy_fast_recovery": {
            "image": args.fast_candidate_image,
            "command": ["/bin/sh"],
            "args": ["/app/src/watchers/runtime_recovery_sidecar_entrypoint.sh"],
            "environment": {
                "FR_CONTROLLER_RUNTIME_MODE": "NARROW_EXECUTOR_ONLY",
                "FR_EFFECT_AUTHORITY_MODE": "AUTO_FENCED",
                "FR_EFFECT_EXECUTOR_SOCKET": "/run/stream-v3-control/effect.sock",
                "FR_RUNTIME_OBSERVATION_FILE": "/run/stream-v3-control/runtime-observation.json",
                "FR_RUNTIME_RECOVERY_SOCKET": "/run/stream-v3-control/runtime-recovery.sock",
                "FR_RUNTIME_RECOVERY_OBSERVATION_FILE": "/run/stream-v3-control/runtime-recovery-observation.json",
                "FR_EFFECT_LEDGER_FILE": "/state/runtime/fast_recovery_effects.sqlite3",
                "FR_TARGET_SNAPSHOT_FILE": "/target-snapshot/target_snapshot.json",
                "FR_RUNTIME_RECOVERY_TARGET": "deployment/stream-v3-runtime",
                "FR_EFFECT_CONTROLLER_UID": "992",
                "FR_EFFECT_SOCKET_GID": "983",
                "FR_EFFECT_PRODUCER_ID": "legacy-in-pod",
                "FR_EFFECT_PRODUCER_GENERATION": "1",
                "MAINTENANCE_AUDIT_STATE_FILE": "/projection/maintenance-snapshot.json",
                "MAINTENANCE_AUDIT_PROJECTION_ENABLED": "1",
                "MAINTENANCE_ENFORCEMENT_ENABLED": "0",
            },
        },
        "volumes": {
            "runtime-control": {"hostPath": "/run/stream-v3-control", "type": "DirectoryOrCreate"},
            "maintenance-projection": {
                "hostPath": "/var/lib/stream-recovery-control/projection/mp03",
                "type": "Directory",
                "readOnly": True,
            },
            "target-snapshot": {
                "hostPath": "/var/lib/stream-recovery-control/dell",
                "type": "Directory",
                "readOnly": True,
            },
        },
        "planned_pod_replacements": 1,
        "enforcement_enabled": False,
    }
    write_text(args.release_dir / "kubernetes/deployment_patch_contract.json", json.dumps(deployment_patch, indent=2))

    manifest = {
        "schema_version": "runtime_boundary.release_manifest.v1",
        "release_id": args.release_id,
        "created_at": utc_text(),
        "topology": "B2_HOST_SYSTEMD_CONTROLLER_PLUS_D_TYPED_EXECUTORS",
        "controller": controller,
        "executor_image": {
            **executor,
            "base_image": args.stream_base_image,
            "base_image_digest": args.stream_base_digest,
            "candidate_image": args.stream_candidate_image,
            "base_stream_engine_replaced": True,
            "stream_engine_change_scope": "lifecycle observation and behavior-preserving restart-delay wakeup hook",
            "stream_engine_source_path": str(stream_engine_source),
            "stream_engine_source_sha256": sha256(stream_engine_source),
            "stream_engine_overlay_manifest": stream_engine_overlay_manifest,
            "current_task_overlay_verified": stream_engine_overlay_manifest is not None,
        },
        "fast_image": {
            **fast,
            "base_image": args.fast_base_image,
            "base_image_digest": args.fast_base_digest,
            "candidate_image": args.fast_candidate_image,
            "removed_base_capabilities": [
                "watchers/fast_recovery_core/executor.py",
                "watchers/fast_recovery_core/legacy_preflight.py",
                "watchers/systemctl_control.py",
                "stream_core/supervisor",
            ],
        },
        "controller_effect_capabilities": [
            "unix_socket:restart_ffmpeg",
            "unix_socket:reconcile_ffmpeg",
            "unix_socket:escalate_runtime_recovery",
        ],
        "controller_mutation_credentials": [],
        "kubernetes_mutation_credentials": ["inherited_existing_runtime_service_account:no_rbac_change"],
        "physical_adapter_files": [
            "executor-image/overlay/app/src/stream_core/runtime_boundary_entrypoint.py",
            "fast-image/overlay/app/src/watchers/runtime_recovery_escalation.py",
        ],
        "credential_rotation_tool_sha256": sha256(credential_target),
        "enforcement_enabled": False,
        "production_branch_signal": None,
        "planned_pod_replacements": 1,
        "rollback_images": {
            "stream-engine": args.stream_base_image,
            "fast-recovery-loop": args.fast_base_image,
        },
        "change_set_contamination": sum(len(value["unexpected_files"]) for value in (controller, executor, fast)),
        "complete": not controller["forbidden_files_present"]
        and all(not value["unexpected_files"] and not value["missing_files"] for value in (controller, executor, fast)),
    }
    write_text(args.release_dir / "release_manifest.json", json.dumps(manifest, indent=2, sort_keys=True))
    print(json.dumps(manifest, sort_keys=True))
    return 0 if manifest["complete"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
