#!/usr/bin/env python3
from __future__ import annotations

import argparse
import re
import sys
from collections import Counter
from pathlib import Path
from typing import Any

try:
    import yaml  # type: ignore
except ModuleNotFoundError as exc:  # pragma: no cover - environment guard
    raise SystemExit("PyYAML is required: python3 -m pip install pyyaml") from exc


ROOT = Path(__file__).resolve().parents[2]
K3S_DIR = ROOT / "deploy" / "k3s"
APP_PATH_RE = re.compile(r"(/app/[A-Za-z0-9_./-]+)")


ResourceId = tuple[str, str, str]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Validate stream_v3 k3s manifest wiring without a live cluster.")
    parser.add_argument(
        "--overlay",
        default="shadow",
        help="kustomization directory under deploy/k3s to resolve; default: shadow",
    )
    args = parser.parse_args(argv)

    errors = validate_overlay(K3S_DIR / args.overlay)
    if errors:
        for error in errors:
            print(f"[error] {error}", file=sys.stderr)
        return 1
    print(f"[ok] k3s manifest validation passed overlay={args.overlay}")
    return 0


def validate_overlay(overlay_dir: Path) -> list[str]:
    errors: list[str] = []
    if not overlay_dir.exists():
        return [f"overlay does not exist: {overlay_dir.relative_to(ROOT)}"]

    docs = resolve_kustomization(overlay_dir)
    resources = {resource_id(doc): doc for doc in docs if resource_id(doc) is not None}
    ids = [resource_id(doc) for doc in docs if resource_id(doc) is not None]

    for rid, count in Counter(ids).items():
        if count > 1 and rid is not None:
            errors.append(f"duplicate resource {format_id(rid)} count={count}")

    mode = configmap_mode(resources)
    errors.extend(validate_required_resources(resources, mode=mode))
    errors.extend(validate_configmap(resources))
    errors.extend(validate_pod_specs(resources))
    errors.extend(validate_runtime_pulse_contract(resources))
    errors.extend(validate_app_paths(docs))
    errors.extend(validate_supervisor_targets(resources, mode=mode))
    errors.extend(validate_control_loop_contract(resources, mode=mode))
    return errors


def configmap_mode(resources: dict[ResourceId, dict[str, Any]]) -> str:
    configmap = resources.get(("ConfigMap", "stream-v3", "stream-v3-shadow-env"))
    data = configmap.get("data") if isinstance(configmap, dict) else {}
    if not isinstance(data, dict):
        return "shadow"
    return str(data.get("STREAM_V3_MODE") or "shadow").strip().lower() or "shadow"


def resolve_kustomization(path: Path, seen: set[Path] | None = None) -> list[dict[str, Any]]:
    seen = set() if seen is None else seen
    kustomization_path = path / "kustomization.yaml" if path.is_dir() else path
    kustomization_path = kustomization_path.resolve()
    if kustomization_path in seen:
        return []
    seen.add(kustomization_path)

    document = load_single_yaml(kustomization_path)
    resources = document.get("resources")
    if not isinstance(resources, list):
        raise ValueError(f"{kustomization_path.relative_to(ROOT)} has no resources list")

    resolved: list[dict[str, Any]] = []
    base_dir = kustomization_path.parent
    for item in resources:
        if not isinstance(item, str):
            raise ValueError(f"{kustomization_path.relative_to(ROOT)} contains non-string resource")
        resource_path = (base_dir / item).resolve()
        if resource_path.is_dir():
            resolved.extend(resolve_kustomization(resource_path, seen))
        elif resource_path.is_file():
            resolved.extend(load_yaml_docs(resource_path))
        else:
            raise FileNotFoundError(f"missing kustomize resource: {resource_path.relative_to(ROOT)}")
    apply_file_patches(resolved, document, base_dir)
    return resolved


def apply_file_patches(resources: list[dict[str, Any]], kustomization: dict[str, Any], base_dir: Path) -> None:
    patches = kustomization.get("patches")
    if not isinstance(patches, list):
        return
    by_id = {resource_id(resource): resource for resource in resources if resource_id(resource) is not None}
    for patch in patches:
        if not isinstance(patch, dict):
            continue
        patch_path = patch.get("path")
        if isinstance(patch_path, str):
            path = (base_dir / patch_path).resolve()
            if not path.is_file():
                raise FileNotFoundError(f"missing kustomize patch: {path.relative_to(ROOT)}")
            for patch_doc in load_yaml_docs(path):
                rid = resource_id(patch_doc)
                if rid is None or rid not in by_id:
                    continue
                strategic_merge(by_id[rid], patch_doc)
            continue
        patch_text = patch.get("patch")
        target = patch.get("target")
        if isinstance(patch_text, str) and isinstance(target, dict):
            rid = (
                str(target.get("kind") or ""),
                str(target.get("namespace") or ""),
                str(target.get("name") or ""),
            )
            if rid in by_id:
                apply_json6902_patch(by_id[rid], patch_text)


def strategic_merge(target: dict[str, Any], patch: dict[str, Any]) -> None:
    for key, value in patch.items():
        if isinstance(value, dict) and isinstance(target.get(key), dict):
            strategic_merge(target[key], value)
        else:
            target[key] = value


def apply_json6902_patch(target: dict[str, Any], patch_text: str) -> None:
    operations = yaml.safe_load(patch_text)
    if not isinstance(operations, list):
        return
    for operation in operations:
        if not isinstance(operation, dict):
            continue
        op = operation.get("op")
        path = operation.get("path")
        if op not in {"add", "replace", "remove"} or not isinstance(path, str):
            continue
        if op == "remove":
            remove_json_path(target, path)
        else:
            set_json_path(target, path, operation.get("value"), append=op == "add")


def set_json_path(target: Any, path: str, value: Any, *, append: bool) -> None:
    parts = [part.replace("~1", "/").replace("~0", "~") for part in path.strip("/").split("/") if part]
    current = target
    for part in parts[:-1]:
        if isinstance(current, list):
            current = current[int(part)]
        elif isinstance(current, dict):
            current = current.setdefault(part, {})
        else:
            return
    if not parts:
        return
    final = parts[-1]
    if isinstance(current, list):
        if final == "-":
            current.append(value)
        else:
            index = int(final)
            if append:
                current.insert(index, value)
            else:
                current[index] = value
    elif isinstance(current, dict):
        current[final] = value


def remove_json_path(target: Any, path: str) -> None:
    parts = [part.replace("~1", "/").replace("~0", "~") for part in path.strip("/").split("/") if part]
    current = target
    for part in parts[:-1]:
        if isinstance(current, list):
            current = current[int(part)]
        elif isinstance(current, dict):
            current = current.get(part)
        else:
            return
    if not parts:
        return
    final = parts[-1]
    if isinstance(current, list):
        del current[int(final)]
    elif isinstance(current, dict):
        current.pop(final, None)


def load_single_yaml(path: Path) -> dict[str, Any]:
    docs = load_yaml_docs(path)
    if len(docs) != 1:
        raise ValueError(f"{path.relative_to(ROOT)} must contain exactly one YAML document")
    return docs[0]


def load_yaml_docs(path: Path) -> list[dict[str, Any]]:
    with path.open(encoding="utf-8") as fh:
        raw_docs = list(yaml.safe_load_all(fh))
    docs: list[dict[str, Any]] = []
    for doc in raw_docs:
        if doc is None:
            continue
        if not isinstance(doc, dict):
            raise ValueError(f"{path.relative_to(ROOT)} contains a non-object YAML document")
        docs.append(doc)
    return docs


def resource_id(doc: dict[str, Any]) -> ResourceId | None:
    metadata = doc.get("metadata")
    if not isinstance(metadata, dict):
        return None
    kind = doc.get("kind")
    name = metadata.get("name")
    if not isinstance(kind, str) or not isinstance(name, str):
        return None
    namespace = metadata.get("namespace")
    if not isinstance(namespace, str):
        namespace = ""
    return (kind, namespace, name)


def validate_required_resources(resources: dict[ResourceId, dict[str, Any]], *, mode: str) -> list[str]:
    if mode == "streaming":
        required = {
            ("Namespace", "", "stream-v3"),
            ("ConfigMap", "stream-v3", "stream-v3-shadow-env"),
            ("PersistentVolumeClaim", "stream-v3", "stream-v3-state"),
            ("PersistentVolumeClaim", "stream-v3", "stream-v3-music"),
            ("Deployment", "stream-v3", "stream-v3-runtime"),
            ("ServiceAccount", "stream-v3", "stream-v3-recovery"),
            ("Role", "stream-v3", "stream-v3-recovery"),
            ("RoleBinding", "stream-v3", "stream-v3-recovery"),
            ("Secret", "stream-v3", "stream-v3-recovery-token"),
        }
        return [f"missing required resource {format_id(rid)}" for rid in sorted(required - set(resources))]

    required = {
        ("Namespace", "", "stream-v3"),
        ("ConfigMap", "stream-v3", "stream-v3-shadow-env"),
        ("PersistentVolumeClaim", "stream-v3", "stream-v3-state"),
        ("PersistentVolumeClaim", "stream-v3", "stream-v3-music"),
        ("PersistentVolumeClaim", "stream-v3", "stream-v2-state-mirror"),
        ("Deployment", "stream-v3", "stream-v3-runtime"),
        ("Deployment", "stream-v3", "stream-v3-control"),
        ("Deployment", "stream-v3", "stream-v3-observer"),
        ("CronJob", "stream-v3", "stream-v2-state-mirror"),
        ("CronJob", "stream-v3", "stream-v3-youtube-api-cost-open-day"),
        ("CronJob", "stream-v3", "stream-v3-youtube-api-cost-closed-day"),
        ("CronJob", "stream-v3", "stream-v3-stream1090-report"),
        ("CronJob", "stream-v3", "stream-v3-upstream-report"),
        ("Service", "stream-v3", "stream-v3-observer"),
    }
    return [f"missing required resource {format_id(rid)}" for rid in sorted(required - set(resources))]


def validate_configmap(resources: dict[ResourceId, dict[str, Any]]) -> list[str]:
    errors: list[str] = []
    configmap = resources.get(("ConfigMap", "stream-v3", "stream-v3-shadow-env"))
    if not configmap:
        return ["missing stream-v3-shadow-env ConfigMap"]
    data = configmap.get("data")
    if not isinstance(data, dict):
        return ["stream-v3-shadow-env ConfigMap has no data map"]

    mode = str(data.get("STREAM_V3_MODE") or "shadow").strip().lower()
    expected = {
        "STREAM_RUNTIME_SUPERVISOR": "k8s",
        "STREAM_K8S_NAMESPACE": "stream-v3",
        "STREAM_RUNTIME_STATE_DIR": "/state",
        "STREAM_V2_SOURCE_STATE_ROOT": "/source-v2-readonly",
        "VIDEO_ENCODER": "h264_nvenc",
        "VIDEO_NVENC_PRESET": "p5",
        "VIDEO_NVENC_RC": "cbr",
        "VIDEO_NVENC_CQ": "",
        "VIDEO_NVENC_MULTIPASS": "fullres",
        "VIDEO_NVENC_RC_LOOKAHEAD": "20",
        "VIDEO_NVENC_SPATIAL_AQ": "1",
        "VIDEO_NVENC_TEMPORAL_AQ": "1",
        "VIDEO_NVENC_BFRAMES": "2",
        "VIDEO_NVENC_B_REF_MODE": "middle",
        "FRAME_RATE": "30",
        "VIDEO_BITRATE": "3300k",
        "VIDEO_MAXRATE": "3300k",
        "VIDEO_BUFSIZE": "6600k",
        "AUDIO_BITRATE": "192k",
        "PULSE_SINK": "stream_v3_sink",
        "PULSE_SOURCE": "stream_v3_sink.monitor",
        "PULSE_SERVER": "unix:/run/stream-pulse/native",
        "STREAM_V3_START_PULSE": "1",
        "AUTO_DJ_KEEP_PULSE_SERVER": "1",
        "MUSIC_ROOT": "/music/time_tags",
        "FR_RTMP_HOST": "a.rtmps.youtube.com",
        "FR_RTMP_PORTS": "443",
        "FR_EVENT_LOG_FILE": "/state/logs/fast_recovery_events.jsonl",
        "FR_YTW_STATS_FILE": "/state/youtube_watchdog_stats.json",
        "FR_QUOTA_STATE_FILE": "/state/youtube_quota_state.json",
        "FR_RESTART_REASON_FILE": "/state/restart_reason.json",
        "V3_FAST_RECOVERY_INTERVAL_SEC": "10",
        "V3_VIDEO_RESOLVER_INTERVAL_SEC": "5",
        "V3_YOUTUBE_MONITOR_INTERVAL_SEC": "45",
        "V3_STREAM_WATCHDOG_INTERVAL_SEC": "60",
        "V3_NOTIFY_INTERVAL_SEC": "60",
    }
    if mode == "streaming":
        expected.update(
            {
                "STREAM_V3_MODE": "streaming",
                "STREAM_V3_CUTOVER_ENABLE": "1",
                "STREAM_K8S_DRY_RUN": "0",
                "TEST_MODE": "0",
                "TAKEOVER_ENABLED": "1",
            }
        )
    elif mode == "cutover":
        expected.update(
            {
                "STREAM_V3_MODE": "cutover",
                "STREAM_V3_CUTOVER_ENABLE": "1",
                "STREAM_K8S_DRY_RUN": "0",
                "TEST_MODE": "0",
                "TAKEOVER_ENABLED": "1",
            }
        )
    else:
        expected.update(
            {
                "STREAM_V3_MODE": "shadow",
                "STREAM_V3_CUTOVER_ENABLE": "0",
                "STREAM_K8S_DRY_RUN": "1",
                "TEST_MODE": "1",
            }
        )
    for key, value in expected.items():
        if str(data.get(key)) != value:
            errors.append(f"ConfigMap {key} expected {value!r}, got {data.get(key)!r}")
    if "STREAM_V2_MIRROR_SOURCE" not in data:
        errors.append("ConfigMap missing STREAM_V2_MIRROR_SOURCE")
    return errors


def validate_pod_specs(resources: dict[ResourceId, dict[str, Any]]) -> list[str]:
    errors: list[str] = []
    configmaps = {name for kind, namespace, name in resources if kind == "ConfigMap" and namespace == "stream-v3"}
    pvcs = {name for kind, namespace, name in resources if kind == "PersistentVolumeClaim" and namespace == "stream-v3"}
    secrets = {name for kind, namespace, name in resources if kind == "Secret" and namespace == "stream-v3"}

    for rid, pod_spec in iter_pod_specs(resources):
        containers = pod_spec.get("containers")
        if not isinstance(containers, list) or not containers:
            errors.append(f"{format_id(rid)} has no containers")
            continue
        for container in containers:
            if not isinstance(container, dict):
                continue
            name = str(container.get("name") or "<unnamed>")
            for env_from in container.get("envFrom") or []:
                if not isinstance(env_from, dict):
                    continue
                configmap_ref = env_from.get("configMapRef")
                if isinstance(configmap_ref, dict):
                    ref_name = configmap_ref.get("name")
                    if ref_name not in configmaps:
                        errors.append(f"{format_id(rid)} container={name} references missing ConfigMap {ref_name!r}")

        volumes = pod_spec.get("volumes") or []
        if not isinstance(volumes, list):
            errors.append(f"{format_id(rid)} volumes must be a list")
            continue
        for volume in volumes:
            if not isinstance(volume, dict):
                continue
            pvc = volume.get("persistentVolumeClaim")
            if isinstance(pvc, dict):
                claim_name = pvc.get("claimName")
                if claim_name not in pvcs:
                    errors.append(f"{format_id(rid)} references missing PVC {claim_name!r}")
            secret = volume.get("secret")
            if isinstance(secret, dict):
                secret_name = secret.get("secretName")
                optional = bool(secret.get("optional"))
                if secret_name not in secrets and not optional:
                    errors.append(f"{format_id(rid)} references missing non-optional Secret {secret_name!r}")
    return errors


def validate_app_paths(docs: list[dict[str, Any]]) -> list[str]:
    errors: list[str] = []
    for doc in docs:
        rid = resource_id(doc)
        if rid is None:
            continue
        for candidate in find_app_paths(doc):
            local_path = ROOT / candidate.removeprefix("/app/")
            if not local_path.exists():
                errors.append(f"{format_id(rid)} references missing repo path {candidate}")
    return sorted(set(errors))


def validate_supervisor_targets(resources: dict[ResourceId, dict[str, Any]], *, mode: str) -> list[str]:
    sys.path.insert(0, str(ROOT / "src"))
    from stream_core.supervisor.factory import STREAM_V3_K8S_TARGET_MAP  # pylint: disable=import-outside-toplevel

    deployments = {
        f"deployment/{name}"
        for kind, namespace, name in resources
        if kind == "Deployment" and namespace == "stream-v3"
    }
    cronjobs = {
        f"cronjob/{name}"
        for kind, namespace, name in resources
        if kind == "CronJob" and namespace == "stream-v3"
    }
    errors: list[str] = []
    if mode == "streaming":
        runtime_target = STREAM_V3_K8S_TARGET_MAP.get("adsb-streamnew-youtube-stream.service")
        if runtime_target not in deployments:
            errors.append("streaming overlay must map adsb-streamnew-youtube-stream.service to a runtime Deployment")
        return errors

    for source, target in sorted(STREAM_V3_K8S_TARGET_MAP.items()):
        if target.startswith("deployment/") and target not in deployments:
            errors.append(f"supervisor target map {source} -> {target} has no Deployment")
        if target.startswith("cronjob/") and target not in cronjobs:
            errors.append(f"supervisor target map {source} -> {target} has no CronJob")
    for target in (
        "cronjob/stream-v3-youtube-api-cost-open-day",
        "cronjob/stream-v3-youtube-api-cost-closed-day",
    ):
        if target not in cronjobs:
            errors.append(f"missing report CronJob target {target}")
    for rid, resource in resources.items():
        if rid[0] != "CronJob" or rid[1] != "stream-v3":
            continue
        spec = resource.get("spec") if isinstance(resource.get("spec"), dict) else {}
        if spec.get("concurrencyPolicy") != "Forbid":
            errors.append(f"{format_id(rid)} must use concurrencyPolicy=Forbid")
        if spec.get("suspend") is not True:
            errors.append(f"{format_id(rid)} must start with suspend=true in shadow manifests")
    return errors


def validate_control_loop_contract(resources: dict[ResourceId, dict[str, Any]], *, mode: str) -> list[str]:
    if mode == "streaming":
        deployment = resources.get(("Deployment", "stream-v3", "stream-v3-runtime"))
        if not deployment:
            return ["missing stream-v3-runtime Deployment"]
        pod_spec = pod_spec_for(deployment, "Deployment")
        if not pod_spec:
            return ["stream-v3-runtime has no pod spec"]
        containers = pod_spec.get("containers") if isinstance(pod_spec.get("containers"), list) else []
        if any(isinstance(container, dict) and container.get("name") == "network-observer" for container in containers):
            return ["streaming overlay must not include network-observer sidecar"]
        if not any("stream_v3.control_loop --mode streaming" in stringify(container) for container in containers if isinstance(container, dict)):
            return ["stream-v3-runtime does not include fast recovery sidecar running stream_v3.control_loop --mode streaming"]
        if pod_spec.get("serviceAccountName") != "stream-v3-recovery":
            return ["stream-v3-runtime must use stream-v3-recovery serviceAccountName in streaming overlay"]
        return []

    deployment = resources.get(("Deployment", "stream-v3", "stream-v3-control"))
    if not deployment:
        return ["missing stream-v3-control Deployment"]
    pod_spec = pod_spec_for(deployment, "Deployment")
    if not pod_spec:
        return ["stream-v3-control has no pod spec"]
    containers = pod_spec.get("containers") if isinstance(pod_spec.get("containers"), list) else []
    if not any("stream_v3.control_loop" in stringify(container) for container in containers if isinstance(container, dict)):
        return ["stream-v3-control does not run stream_v3.control_loop"]

    mounted = False
    for container in containers:
        if not isinstance(container, dict):
            continue
        for mount in container.get("volumeMounts") or []:
            if isinstance(mount, dict) and mount.get("mountPath") == "/source-v2-readonly" and mount.get("readOnly") is True:
                mounted = True
    if not mounted:
        return ["stream-v3-control must mount /source-v2-readonly readOnly"]
    return []


def validate_runtime_pulse_contract(resources: dict[ResourceId, dict[str, Any]]) -> list[str]:
    deployment = resources.get(("Deployment", "stream-v3", "stream-v3-runtime"))
    if not deployment:
        return ["missing stream-v3-runtime Deployment"]
    pod_spec = pod_spec_for(deployment, "Deployment")
    if not pod_spec:
        return ["stream-v3-runtime has no pod spec"]
    containers = pod_spec.get("containers") if isinstance(pod_spec.get("containers"), list) else []
    by_name = {container.get("name"): container for container in containers if isinstance(container, dict)}
    errors: list[str] = []
    stream_engine = by_name.get("stream-engine")
    auto_dj = by_name.get("auto-dj")
    if not isinstance(stream_engine, dict):
        errors.append("stream-v3-runtime missing stream-engine container")
    else:
        if "/app/src/stream_v3/runtime_entrypoint.sh" not in stringify(stream_engine):
            errors.append("stream-engine must run runtime_entrypoint.sh so Pulse starts inside the pod")
        resources_block = stream_engine.get("resources")
        limits = resources_block.get("limits") if isinstance(resources_block, dict) else {}
        if not isinstance(limits, dict) or str(limits.get("nvidia.com/gpu")) != "1":
            errors.append("stream-engine must request one NVIDIA GPU for h264_nvenc")
        env = stream_engine.get("env") if isinstance(stream_engine.get("env"), list) else []
        capabilities = next(
            (
                item.get("value")
                for item in env
                if isinstance(item, dict) and item.get("name") == "NVIDIA_DRIVER_CAPABILITIES"
            ),
            None,
        )
        if capabilities != "video,utility":
            errors.append("stream-engine must set NVIDIA_DRIVER_CAPABILITIES=video,utility for NVENC")
    if not isinstance(auto_dj, dict):
        errors.append("stream-v3-runtime missing auto-dj container")

    for container_name, container in (("stream-engine", stream_engine), ("auto-dj", auto_dj)):
        if not isinstance(container, dict):
            continue
        mounts = container.get("volumeMounts") if isinstance(container.get("volumeMounts"), list) else []
        if not any(
            isinstance(mount, dict)
            and mount.get("name") == "pulse-run"
            and mount.get("mountPath") == "/run/stream-pulse"
            for mount in mounts
        ):
            errors.append(f"{container_name} must mount pulse-run at /run/stream-pulse")

    volumes = pod_spec.get("volumes") if isinstance(pod_spec.get("volumes"), list) else []
    pulse_volume = next((volume for volume in volumes if isinstance(volume, dict) and volume.get("name") == "pulse-run"), None)
    if not isinstance(pulse_volume, dict) or not isinstance(pulse_volume.get("emptyDir"), dict):
        errors.append("stream-v3-runtime must define pulse-run emptyDir")
    return errors


def iter_pod_specs(resources: dict[ResourceId, dict[str, Any]]):
    for rid, resource in resources.items():
        spec = pod_spec_for(resource, rid[0])
        if spec is not None:
            yield rid, spec


def pod_spec_for(resource: dict[str, Any], kind: str) -> dict[str, Any] | None:
    if kind == "Deployment":
        spec = resource.get("spec")
        template = spec.get("template") if isinstance(spec, dict) else None
        return template.get("spec") if isinstance(template, dict) and isinstance(template.get("spec"), dict) else None
    if kind == "CronJob":
        spec = resource.get("spec")
        job_template = spec.get("jobTemplate") if isinstance(spec, dict) else None
        job_spec = job_template.get("spec") if isinstance(job_template, dict) else None
        template = job_spec.get("template") if isinstance(job_spec, dict) else None
        return template.get("spec") if isinstance(template, dict) and isinstance(template.get("spec"), dict) else None
    return None


def find_app_paths(value: Any) -> set[str]:
    paths: set[str] = set()
    if isinstance(value, dict):
        for child in value.values():
            paths.update(find_app_paths(child))
    elif isinstance(value, list):
        for child in value:
            paths.update(find_app_paths(child))
    elif isinstance(value, str):
        paths.update(APP_PATH_RE.findall(value))
    return paths


def stringify(value: Any) -> str:
    if isinstance(value, dict):
        return " ".join(f"{key}={stringify(child)}" for key, child in value.items())
    if isinstance(value, list):
        return " ".join(stringify(child) for child in value)
    return str(value)


def format_id(rid: ResourceId) -> str:
    kind, namespace, name = rid
    return f"{kind}/{namespace + '/' if namespace else ''}{name}"


if __name__ == "__main__":
    raise SystemExit(main())
