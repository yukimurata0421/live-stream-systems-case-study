#!/usr/bin/env python3
"""Read-only observability-host probe for the production ADS-B map runtime."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence


def default_repo_root() -> Path:
    return Path(os.environ.get("STREAM_V3_REPO_DIR", Path(__file__).resolve().parents[2])).expanduser()


def default_state_root(repo_root: Path) -> Path:
    configured = os.environ.get("STREAM_V3_OBSERVABILITY_STATE_ROOT") or os.environ.get("STREAM_RUNTIME_STATE_DIR")
    return Path(configured).expanduser() if configured else repo_root / ".state" / "observability-monitor"


DEFAULT_REPO_ROOT = default_repo_root()
DEFAULT_STATE_ROOT = default_state_root(DEFAULT_REPO_ROOT)
DEFAULT_NAMESPACE = "stream-v3"
DEFAULT_DEPLOYMENT = "stream-v3-runtime"
DEFAULT_SELECTOR = "app.kubernetes.io/name=stream-v3,app.kubernetes.io/component=runtime"
DEFAULT_EXPECTED_CONTAINERS = (
    "stream-engine",
    "precipitation-fetcher",
    "auto-dj",
    "fast-recovery-loop",
)


PROCESS_PROBE = r'''
import json
import urllib.request
from pathlib import Path

SWIFTSHADER_FLAGS = (
    "--enable-unsafe-swiftshader",
    "--use-gl=angle",
    "--use-angle=swiftshader",
)

def read_cmdline(pid):
    try:
        return [
            part.decode("utf-8", "replace")
            for part in (Path("/proc") / str(pid) / "cmdline").read_bytes().split(b"\0")
            if part
        ]
    except OSError:
        return []

def http_json(path):
    try:
        with urllib.request.urlopen("http://127.0.0.1:18080" + path, timeout=3) as response:
            payload = json.load(response)
        return {"payload": payload if isinstance(payload, dict) else {}, "error": ""}
    except Exception as exc:
        return {"payload": {}, "error": (type(exc).__name__ + ":" + str(exc))[:240]}

browser_main_count = 0
swiftshader_main_count = 0
adsb_map_main_count = 0
for entry in Path("/proc").iterdir():
    if not entry.name.isdigit():
        continue
    args = read_cmdline(entry.name)
    if not args:
        continue
    joined = "\0".join(args).lower()
    if "chromium" not in joined:
        continue
    if not any(arg.startswith("--app=") for arg in args):
        continue
    browser_main_count += 1
    if all(flag in args for flag in SWIFTSHADER_FLAGS):
        swiftshader_main_count += 1
    if any("/adsb-map/" in arg for arg in args):
        adsb_map_main_count += 1

browser_log = {
    "present": False,
    "webgl2_blocklisted": False,
    "context_fatal_failure": False,
}
for candidate in (Path("/app/logs/browser.log"), Path("/state/logs/browser.log")):
    try:
        tail = candidate.read_bytes()[-524288:].decode("utf-8", "replace")
    except OSError:
        continue
    browser_log = {
        "present": True,
        "webgl2_blocklisted": "WebGL2 blocklisted" in tail,
        "context_fatal_failure": "ContextResult::kFatalFailure" in tail,
    }
    break

print(json.dumps({
    "browser": {
        "main_process_count": browser_main_count,
        "swiftshader_main_process_count": swiftshader_main_count,
        "adsb_map_main_process_count": adsb_map_main_count,
    },
    "browser_log": browser_log,
    "render": http_json("/render/status.json"),
    "weather_status": http_json("/weather/status.json"),
    "weather_health": http_json("/weather/health.json"),
}, separators=(",", ":")))
'''


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def parse_ts(value: Any) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        return datetime.fromisoformat(value.strip().replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


def as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def command(args: Sequence[str], *, timeout_sec: float) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        list(args),
        capture_output=True,
        text=True,
        timeout=timeout_sec,
        check=False,
    )


def json_command(args: Sequence[str], *, timeout_sec: float) -> dict[str, Any]:
    completed = command(args, timeout_sec=timeout_sec)
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout or f"exit={completed.returncode}").strip()
        raise RuntimeError(detail[-500:])
    try:
        payload = json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"invalid JSON output: {exc}") from exc
    if not isinstance(payload, dict):
        raise RuntimeError("JSON output is not an object")
    return payload


def kubectl_binary() -> str:
    configured = os.environ.get("STREAM_V3_KUBECTL_BIN", "").strip()
    return configured or shutil.which("kubectl") or "/usr/local/bin/kubectl"


def kubectl_json(kubectl: str, args: Sequence[str], *, timeout_sec: float) -> dict[str, Any]:
    return json_command([kubectl, *args], timeout_sec=timeout_sec)


def atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2, sort_keys=True)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n")
        handle.flush()
        os.fsync(handle.fileno())


def deployment_snapshot(payload: dict[str, Any]) -> dict[str, Any]:
    metadata = as_dict(payload.get("metadata"))
    spec = as_dict(payload.get("spec"))
    status = as_dict(payload.get("status"))
    template = as_dict(spec.get("template"))
    pod_spec = as_dict(template.get("spec"))
    containers = {
        str(item.get("name")): {"image": str(item.get("image") or "")}
        for item in as_list(pod_spec.get("containers"))
        if isinstance(item, dict) and item.get("name")
    }
    return {
        "generation": metadata.get("generation"),
        "observed_generation": status.get("observedGeneration"),
        "desired_replicas": int(spec.get("replicas") or 0),
        "ready_replicas": int(status.get("readyReplicas") or 0),
        "available_replicas": int(status.get("availableReplicas") or 0),
        "containers": containers,
    }


def pod_snapshots(payload: dict[str, Any]) -> list[dict[str, Any]]:
    pods: list[dict[str, Any]] = []
    for item in as_list(payload.get("items")):
        if not isinstance(item, dict):
            continue
        metadata = as_dict(item.get("metadata"))
        if metadata.get("deletionTimestamp"):
            continue
        status = as_dict(item.get("status"))
        containers: dict[str, Any] = {}
        for row in as_list(status.get("containerStatuses")):
            if not isinstance(row, dict) or not row.get("name"):
                continue
            state = as_dict(row.get("state"))
            state_name = next(iter(state), "unknown")
            containers[str(row["name"])] = {
                "ready": row.get("ready") is True,
                "restart_count": int(row.get("restartCount") or 0),
                "state": state_name,
            }
        pods.append(
            {
                "name": str(metadata.get("name") or ""),
                "uid": str(metadata.get("uid") or ""),
                "created_at_utc": str(metadata.get("creationTimestamp") or ""),
                "phase": str(status.get("phase") or ""),
                "containers": containers,
            }
        )
    return pods


def runtime_readiness(
    kubectl: str,
    namespace: str,
    pod_name: str,
    *,
    timeout_sec: float,
) -> dict[str, Any]:
    completed = command(
        [
            kubectl,
            "-n",
            namespace,
            "exec",
            pod_name,
            "-c",
            "stream-engine",
            "--",
            "python3",
            "/app/src/stream_core/runtime_readiness.py",
            "--check-only",
        ],
        timeout_sec=timeout_sec,
    )
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout or "runtime readiness failed").strip()
        return {"ready": False, "reason": detail[-500:], "probe_error": True}
    try:
        payload = json.loads(completed.stdout.splitlines()[-1])
    except (IndexError, json.JSONDecodeError) as exc:
        return {"ready": False, "reason": f"invalid runtime readiness output: {exc}", "probe_error": True}
    return payload if isinstance(payload, dict) else {"ready": False, "reason": "invalid runtime readiness payload"}


def process_snapshot(
    kubectl: str,
    namespace: str,
    pod_name: str,
    *,
    timeout_sec: float,
) -> dict[str, Any]:
    return json_command(
        [
            kubectl,
            "-n",
            namespace,
            "exec",
            pod_name,
            "-c",
            "stream-engine",
            "--",
            "python3",
            "-c",
            PROCESS_PROBE,
        ],
        timeout_sec=timeout_sec,
    )


def endpoint_payload(process: dict[str, Any], name: str) -> tuple[dict[str, Any], str]:
    wrapper = as_dict(process.get(name))
    return as_dict(wrapper.get("payload")), str(wrapper.get("error") or "")


def evaluate_sample(
    sample: dict[str, Any],
    *,
    expected_containers: Sequence[str] = DEFAULT_EXPECTED_CONTAINERS,
    render_max_age_sec: float = 30.0,
    now_epoch: float | None = None,
) -> dict[str, Any]:
    now = time.time() if now_epoch is None else now_epoch
    expected = tuple(expected_containers)
    expected_set = set(expected)
    deployment = as_dict(sample.get("deployment"))
    pods = [item for item in as_list(sample.get("pods")) if isinstance(item, dict)]
    pod = pods[0] if len(pods) == 1 else {}
    containers = as_dict(pod.get("containers"))
    readiness = as_dict(sample.get("readiness"))
    process = as_dict(sample.get("process"))
    render, render_error = endpoint_payload(process, "render")
    weather_status, weather_status_error = endpoint_payload(process, "weather_status")
    weather_health, weather_health_error = endpoint_payload(process, "weather_health")
    browser = as_dict(process.get("browser"))
    browser_log = as_dict(process.get("browser_log"))

    desired_replicas = int(deployment.get("desired_replicas") or 0)
    deployment_containers = set(as_dict(deployment.get("containers")))
    deployment_ok = bool(
        deployment
        and deployment.get("generation") == deployment.get("observed_generation")
        and desired_replicas == 1
        and int(deployment.get("ready_replicas") or 0) == desired_replicas
        and int(deployment.get("available_replicas") or 0) == desired_replicas
        and deployment_containers == expected_set
    )
    pod_ok = bool(
        len(pods) == 1
        and pod.get("phase") == "Running"
        and set(containers) == expected_set
        and all(as_dict(containers.get(name)).get("ready") is True for name in expected)
    )
    readiness_ok = bool(
        readiness.get("ready") is True
        and readiness.get("gpu_ok") is True
        and readiness.get("nvenc_active") is True
        and readiness.get("rtmp_socket_established") is True
    )
    render_age = render.get("age_sec")
    try:
        render_age_value = float(render_age)
    except (TypeError, ValueError):
        render_age_value = -1.0
    render_ok = bool(
        not render_error
        and render.get("ready") is True
        and render.get("state") == "ready"
        and render.get("map_tiles_ready") is True
        and render.get("aircraft_sample_ready") is True
        and 0 <= render_age_value <= render_max_age_sec
    )
    browser_contract_ok = bool(
        int(browser.get("main_process_count") or 0) >= 1
        and int(browser.get("swiftshader_main_process_count") or 0) >= 1
        and int(browser.get("adsb_map_main_process_count") or 0) >= 1
        and browser_log.get("present") is True
        and browser_log.get("webgl2_blocklisted") is not True
        and browser_log.get("context_fatal_failure") is not True
    )

    observed_ts = parse_ts(weather_status.get("observed_at_utc"))
    observed_age_sec = max(0.0, now - observed_ts) if observed_ts is not None else None
    try:
        stale_after_sec = max(60.0, float(weather_status.get("stale_after_sec") or 900.0))
    except (TypeError, ValueError):
        stale_after_sec = 900.0
    weather_status_ok = bool(
        not weather_status_error
        and weather_status.get("available") is True
        and weather_status.get("analysis_only") is True
        and weather_status.get("processed") is True
        and weather_status.get("forecast_minutes") == 0
        and observed_age_sec is not None
        and observed_age_sec <= stale_after_sec
    )
    weather_health_ok = bool(
        not weather_health_error
        and weather_health.get("success") is True
        and weather_health.get("state") == "current"
        and int(weather_health.get("consecutive_failures") or 0) == 0
    )
    weather_ok = weather_status_ok and weather_health_ok

    critical_checks = {
        "deployment_ready": deployment_ok,
        "pod_topology_ready": pod_ok,
        "runtime_readiness": readiness_ok,
        "render_heartbeat": render_ok,
        "browser_contract": browser_contract_ok,
    }
    critical_reasons = [name for name, ok in critical_checks.items() if not ok]
    weather_reasons: list[str] = []
    if not weather_status_ok:
        weather_reasons.append("precipitation_status")
    if not weather_health_ok:
        weather_reasons.append("precipitation_fetcher_health")
    probe_errors = [str(item)[:500] for item in as_list(sample.get("probe_errors"))]

    delivery_critical_ok = not critical_reasons
    discovery_failed = any(
        item.startswith("deployment:") or item.startswith("pods:") for item in probe_errors
    )
    if discovery_failed:
        status = "unknown"
    elif not delivery_critical_ok:
        status = "failed"
    elif not weather_ok:
        status = "degraded"
    else:
        status = "healthy"

    return {
        "schema": "stream_v3.map_runtime_monitor.v1",
        "checked_at_utc": str(sample.get("checked_at_utc") or utc_now()),
        "status": status,
        "delivery_critical_ok": delivery_critical_ok,
        "weather_ok": weather_ok,
        "expected_containers": list(expected),
        "deployment": deployment,
        "pod_count": len(pods),
        "pod": pod,
        "readiness": readiness,
        "render": {**render, "probe_error": render_error},
        "browser": {**browser, **browser_log, "contract_ok": browser_contract_ok},
        "precipitation": {
            "status": weather_status,
            "health": weather_health,
            "status_probe_error": weather_status_error,
            "health_probe_error": weather_health_error,
            "observed_age_sec": round(observed_age_sec, 3) if observed_age_sec is not None else None,
            "stale_after_sec": stale_after_sec,
        },
        "conditions": {
            **critical_checks,
            "precipitation_status": weather_status_ok,
            "precipitation_fetcher_health": weather_health_ok,
        },
        "critical_reasons": critical_reasons,
        "weather_reasons": weather_reasons,
        "probe_errors": probe_errors,
    }


def collect_sample(
    *,
    namespace: str,
    deployment_name: str,
    selector: str,
    timeout_sec: float,
) -> dict[str, Any]:
    kubectl = kubectl_binary()
    sample: dict[str, Any] = {"checked_at_utc": utc_now(), "probe_errors": []}
    try:
        raw_deployment = kubectl_json(
            kubectl,
            ["-n", namespace, "get", "deployment", deployment_name, "-o", "json"],
            timeout_sec=timeout_sec,
        )
        sample["deployment"] = deployment_snapshot(raw_deployment)
    except Exception as exc:
        sample["deployment"] = {}
        sample["probe_errors"].append(f"deployment:{type(exc).__name__}:{exc}"[:500])

    try:
        raw_pods = kubectl_json(
            kubectl,
            ["-n", namespace, "get", "pods", "-l", selector, "-o", "json"],
            timeout_sec=timeout_sec,
        )
        sample["pods"] = pod_snapshots(raw_pods)
    except Exception as exc:
        sample["pods"] = []
        sample["probe_errors"].append(f"pods:{type(exc).__name__}:{exc}"[:500])

    pods = as_list(sample.get("pods"))
    pod = pods[0] if len(pods) == 1 and isinstance(pods[0], dict) else {}
    pod_name = str(pod.get("name") or "")
    if not pod_name:
        sample["readiness"] = {"ready": False, "reason": "exactly one runtime Pod is not available"}
        sample["process"] = {}
        return sample

    sample["readiness"] = runtime_readiness(
        kubectl,
        namespace,
        pod_name,
        timeout_sec=timeout_sec,
    )
    try:
        sample["process"] = process_snapshot(
            kubectl,
            namespace,
            pod_name,
            timeout_sec=timeout_sec,
        )
    except Exception as exc:
        sample["process"] = {}
        sample["probe_errors"].append(f"process:{type(exc).__name__}:{exc}"[:500])
    return sample


def history_row(payload: dict[str, Any]) -> dict[str, Any]:
    pod = as_dict(payload.get("pod"))
    containers = as_dict(pod.get("containers"))
    readiness = as_dict(payload.get("readiness"))
    render = as_dict(payload.get("render"))
    precipitation = as_dict(payload.get("precipitation"))
    browser = as_dict(payload.get("browser"))
    return {
        "schema": "stream_v3.map_runtime_monitor_history.v1",
        "checked_at_utc": payload.get("checked_at_utc"),
        "status": payload.get("status"),
        "delivery_critical_ok": payload.get("delivery_critical_ok"),
        "weather_ok": payload.get("weather_ok"),
        "pod_name": pod.get("name", ""),
        "pod_uid": pod.get("uid", ""),
        "container_restart_counts": {
            name: int(as_dict(item).get("restart_count") or 0)
            for name, item in containers.items()
        },
        "ffmpeg_pid": readiness.get("ffmpeg_pid"),
        "nvenc_active": readiness.get("nvenc_active"),
        "rtmp_socket_established": readiness.get("rtmp_socket_established"),
        "render_age_sec": render.get("age_sec"),
        "webgl2_blocklisted": browser.get("webgl2_blocklisted"),
        "webgl_context_fatal": browser.get("context_fatal_failure"),
        "precipitation_observed_age_sec": precipitation.get("observed_age_sec"),
        "critical_reasons": payload.get("critical_reasons") or [],
        "weather_reasons": payload.get("weather_reasons") or [],
        "probe_errors": payload.get("probe_errors") or [],
    }


def parse_expected_containers(value: str) -> tuple[str, ...]:
    parsed = tuple(item.strip() for item in value.split(",") if item.strip())
    return parsed or DEFAULT_EXPECTED_CONTAINERS


def main(argv: Sequence[str] | None = None) -> int:
    state_root = Path(os.environ.get("STREAM_RUNTIME_STATE_DIR", str(DEFAULT_STATE_ROOT)))
    log_root = Path(os.environ.get("STREAM_RUNTIME_LOG_DIR", str(state_root / "logs")))
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-file", type=Path, default=state_root / "map_runtime_status.json")
    parser.add_argument("--history-file", type=Path, default=log_root / "map_runtime_status.jsonl")
    parser.add_argument("--namespace", default=os.environ.get("STREAM_K8S_NAMESPACE", DEFAULT_NAMESPACE))
    parser.add_argument("--deployment", default=os.environ.get("STREAM_V3_RUNTIME_DEPLOYMENT", DEFAULT_DEPLOYMENT))
    parser.add_argument("--selector", default=os.environ.get("STREAM_V3_RUNTIME_SELECTOR", DEFAULT_SELECTOR))
    parser.add_argument(
        "--expected-containers",
        default=os.environ.get("STREAM_V3_MAP_EXPECTED_CONTAINERS", ",".join(DEFAULT_EXPECTED_CONTAINERS)),
    )
    parser.add_argument(
        "--render-max-age-sec",
        type=float,
        default=float(os.environ.get("STREAM_V3_MAP_RENDER_MAX_AGE_SEC", "30")),
    )
    parser.add_argument(
        "--timeout-sec",
        type=float,
        default=float(os.environ.get("STREAM_V3_MAP_COMMAND_TIMEOUT_SEC", "5")),
    )
    args = parser.parse_args(argv)
    expected = parse_expected_containers(args.expected_containers)

    try:
        sample = collect_sample(
            namespace=args.namespace,
            deployment_name=args.deployment,
            selector=args.selector,
            timeout_sec=max(3.0, args.timeout_sec),
        )
        payload = evaluate_sample(
            sample,
            expected_containers=expected,
            render_max_age_sec=max(5.0, args.render_max_age_sec),
        )
        exit_code = 0
    except Exception as exc:  # pragma: no cover - defensive service boundary
        payload = {
            "schema": "stream_v3.map_runtime_monitor.v1",
            "checked_at_utc": utc_now(),
            "status": "unknown",
            "delivery_critical_ok": False,
            "weather_ok": False,
            "expected_containers": list(expected),
            "deployment": {},
            "pod_count": 0,
            "pod": {},
            "readiness": {},
            "render": {},
            "browser": {"contract_ok": False},
            "precipitation": {},
            "conditions": {},
            "critical_reasons": ["probe_exception"],
            "weather_reasons": ["probe_exception"],
            "probe_errors": [f"{type(exc).__name__}:{exc}"[:500]],
        }
        exit_code = 1

    atomic_json(args.state_file, payload)
    append_jsonl(args.history_file, history_row(payload))
    print(
        json.dumps(
            {
                "checked_at_utc": payload.get("checked_at_utc"),
                "status": payload.get("status"),
                "delivery_critical_ok": payload.get("delivery_critical_ok"),
                "weather_ok": payload.get("weather_ok"),
                "critical_reasons": payload.get("critical_reasons"),
                "weather_reasons": payload.get("weather_reasons"),
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
