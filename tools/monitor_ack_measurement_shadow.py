#!/usr/bin/env python3
"""Read-only supervised monitor for the ACK measurement-only controller overlay."""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import sqlite3
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

UTC = dt.UTC
JST = dt.timezone(dt.timedelta(hours=9))
KUBECTL = "/usr/local/bin/kubectl"
NAMESPACE = "stream-v3"
DEPLOYMENT = "stream-v3-runtime"
LABEL_SELECTOR = "app.kubernetes.io/name=stream-v3,app.kubernetes.io/component=runtime"
CONTROLLER_UNIT = "stream-v3-fast-recovery-controller.service"
TIMER_UNIT = "stream-v3-fast-recovery-controller.timer"
CONTROLLER_STATE = Path("/var/lib/stream-recovery-control/controller/mp03-active/fast_recovery_state.json")
RUNTIME_STATE = Path("/run/stream-v3-control/runtime-observation.json")
LEDGER = Path(
    "/var/lib/rancher/k3s/storage/pvc-2db931a5-ab11-4d95-96a6-d41b70798fa7_stream-v3_stream-v3-state/runtime/fast_recovery_effects.sqlite3"
)
TERMINAL_SCOPE_STATES = {"RECONCILED_EFFECT_OBSERVED", "RELEASED_NO_EFFECT", "RETIRED_TARGET_OUTCOME_UNKNOWN"}


def now_utc() -> dt.datetime:
    return dt.datetime.now(tz=UTC)


def iso(value: dt.datetime) -> str:
    return value.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def parse_time(value: Any) -> dt.datetime:
    text = str(value or "").replace("Z", "+00:00")
    parsed = dt.datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        raise ValueError("NAIVE_TIMESTAMP")
    return parsed.astimezone(UTC)


def load_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"OBJECT_REQUIRED:{path}")
    return value


def command_value(command: list[str], *, timeout: float = 15.0) -> str:
    return subprocess.run(command, check=True, capture_output=True, text=True, timeout=timeout).stdout.strip()


def kubectl_json(*args: str) -> dict[str, Any]:
    value = json.loads(command_value([KUBECTL, "-n", NAMESPACE, *args, "-o", "json"]))
    if not isinstance(value, dict):
        raise ValueError("KUBECTL_OBJECT_REQUIRED")
    return value


def systemctl_value(unit: str, prop: str) -> str:
    return command_value(["systemctl", "show", unit, f"-p{prop}", "--value"], timeout=10.0)


def ledger_state() -> dict[str, Any]:
    connection = sqlite3.connect(f"file:{LEDGER}?mode=ro", uri=True, timeout=5)
    try:
        integrity = str(connection.execute("pragma integrity_check").fetchone()[0])
        scope_states = {
            str(state): int(count)
            for state, count in connection.execute("select state,count(*) from effect_scope_fences group by state order by state")
        }
        unresolved = sum(count for state, count in scope_states.items() if state not in TERMINAL_SCOPE_STATES)
        request_count = int(connection.execute("select count(*) from effect_requests").fetchone()[0])
        scope_count = int(connection.execute("select count(*) from effect_scope_fences").fetchone()[0])
        return {
            "integrity": integrity,
            "unresolved_scope_count": unresolved,
            "request_count": request_count,
            "scope_count": scope_count,
            "scope_states": scope_states,
        }
    finally:
        connection.close()


def append_record(handle: Any, record: dict[str, Any]) -> None:
    handle.write(json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n")
    handle.flush()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--duration-sec", type=float, default=600.0)
    parser.add_argument("--interval-sec", type=float, default=10.0)
    parser.add_argument("--startup-grace-sec", type=float, default=45.0)
    parser.add_argument("--ack-stall-sec", type=float, default=90.0)
    parser.add_argument("--expected-controller-root", required=True)
    parser.add_argument("--expected-pod-uid", required=True)
    parser.add_argument("--expected-stream-container-id", required=True)
    parser.add_argument("--expected-stream-image-id", required=True)
    parser.add_argument("--expected-pending-count", type=int, required=True)
    parser.add_argument("--expected-restart-event-count", type=int, required=True)
    parser.add_argument("--expected-ledger-request-count", type=int, required=True)
    parser.add_argument("--expected-ledger-scope-count", type=int, required=True)
    parser.add_argument("--expected-pending-effect", choices=("true", "false"), default="true")
    args = parser.parse_args()
    expected_pending_effect = args.expected_pending_effect == "true"

    started_wall = now_utc()
    started_mono = time.monotonic()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    last_ack: int | None = None
    last_ack_progress_mono = started_mono
    first_ffmpeg_identity: tuple[int, str] | None = None
    minimum_ack: int | None = None
    maximum_ack: int | None = None
    valid_measurements = 0
    samples = 0
    history_counts: list[int] = []
    healthy_counts: list[int] = []

    with args.output.open("x", encoding="utf-8") as handle:
        append_record(
            handle,
            {
                "record_type": "metadata",
                "schema": "stream_v3.ack_measurement_shadow_monitor.v1",
                "started_at_utc": iso(started_wall),
                "started_at_jst": started_wall.astimezone(JST).isoformat(timespec="milliseconds"),
                "duration_sec": args.duration_sec,
                "interval_sec": args.interval_sec,
                "expected_controller_root": args.expected_controller_root,
                "expected_pod_uid": args.expected_pod_uid,
                "expected_pending_count": args.expected_pending_count,
                "expected_restart_event_count": args.expected_restart_event_count,
                "expected_ledger_request_count": args.expected_ledger_request_count,
                "expected_ledger_scope_count": args.expected_ledger_scope_count,
            },
        )
        while True:
            sample_started = time.monotonic()
            elapsed = sample_started - started_mono
            observed = now_utc()
            stop_reasons: list[str] = []
            try:
                deployment = kubectl_json("get", "deployment", DEPLOYMENT)
                pods = kubectl_json("get", "pods", "-l", LABEL_SELECTOR)
                controller = load_json(CONTROLLER_STATE)
                runtime = load_json(RUNTIME_STATE)
                ledger = ledger_state()

                deployment_status = dict(deployment.get("status") or {})
                deployment_ok = (
                    int(deployment["metadata"].get("generation") or 0) == int(deployment_status.get("observedGeneration") or 0)
                    and int(deployment_status.get("updatedReplicas") or 0) == 1
                    and int(deployment_status.get("readyReplicas") or 0) == 1
                    and int(deployment_status.get("availableReplicas") or 0) == 1
                )
                if not deployment_ok:
                    stop_reasons.append("DEPLOYMENT_NOT_CONVERGED")

                pod_items = list(pods.get("items") or [])
                pod_name = ""
                pod_uid = ""
                restart_total = -1
                all_ready = False
                stream_status: dict[str, Any] = {}
                if len(pod_items) == 1:
                    pod = pod_items[0]
                    pod_name = str(pod["metadata"].get("name") or "")
                    pod_uid = str(pod["metadata"].get("uid") or "")
                    statuses = list(pod.get("status", {}).get("containerStatuses") or [])
                    restart_total = sum(int(item.get("restartCount") or 0) for item in statuses)
                    all_ready = len(statuses) == 4 and all(bool(item.get("ready")) for item in statuses)
                    stream_status = next((dict(item) for item in statuses if item.get("name") == "stream-engine"), {})
                pod_ok = (
                    len(pod_items) == 1
                    and pod_uid == args.expected_pod_uid
                    and all_ready
                    and restart_total == 0
                    and stream_status.get("containerID") == args.expected_stream_container_id
                    and stream_status.get("imageID") == args.expected_stream_image_id
                )
                if not pod_ok:
                    stop_reasons.append("POD_IDENTITY_OR_HEALTH_MISMATCH")

                runtime_age = (observed - parse_time(runtime.get("observed_at"))).total_seconds()
                tcp = dict(runtime.get("tcp_metrics") or {})
                ack = int(tcp.get("bytes_acked") or 0)
                ffmpeg_identity = (
                    int(runtime.get("protocol_ffmpeg_pid") or runtime.get("local_ffmpeg_pid") or 0),
                    str(runtime.get("ffmpeg_generation") or ""),
                )
                runtime_ok = (
                    runtime.get("schema_version") == "runtime.ffmpeg_observation.v1"
                    and runtime.get("runtime_snapshot_status") == "VALID"
                    and runtime.get("target_snapshot_status") == "VALID"
                    and runtime.get("ffmpeg_running") is True
                    and runtime.get("stream_established") is True
                    and ffmpeg_identity[0] > 1
                    and bool(ffmpeg_identity[1])
                    and -1.0 <= runtime_age <= 30.0
                    and ack > 0
                )
                if not runtime_ok:
                    stop_reasons.append("RUNTIME_OBSERVATION_INVALID")
                if first_ffmpeg_identity is None:
                    first_ffmpeg_identity = ffmpeg_identity
                elif ffmpeg_identity != first_ffmpeg_identity:
                    stop_reasons.append("FFMPEG_IDENTITY_DRIFT")
                if last_ack is None or ack > last_ack:
                    last_ack_progress_mono = sample_started
                elif ack < last_ack:
                    stop_reasons.append("ACK_COUNTER_DECREASED")
                elif sample_started - last_ack_progress_mono >= args.ack_stall_sec:
                    stop_reasons.append("ACK_NO_PROGRESS")
                last_ack = ack
                minimum_ack = ack if minimum_ack is None else min(minimum_ack, ack)
                maximum_ack = ack if maximum_ack is None else max(maximum_ack, ack)

                pending_count = len(controller.get("pending_recovery_actions") or [])
                restart_event_count = len(controller.get("restart_events") or [])
                if pending_count != args.expected_pending_count:
                    stop_reasons.append("PENDING_RECOVERY_COUNT_CHANGED")
                if restart_event_count != args.expected_restart_event_count:
                    stop_reasons.append("RESTART_EVENT_COUNT_CHANGED")
                if int(controller.get("restart_failure_count") or 0) != 0:
                    stop_reasons.append("RESTART_FAILURE_RECORDED")

                measurement_raw = controller.get("ack_delivery_measurement_v1")
                measurement = dict(measurement_raw) if isinstance(measurement_raw, dict) else {}
                statistics = dict(measurement.get("statistics") or {})
                latest = dict(measurement.get("latest") or {})
                history_count = int(statistics.get("history_sample_count") or 0)
                healthy_count = int(statistics.get("healthy_sample_count") or 0)
                history_counts.append(history_count)
                healthy_counts.append(healthy_count)
                if measurement:
                    safe_measurement = (
                        measurement.get("schema_version") == "stream_v3.ack_delivery_measurement.v1"
                        and measurement.get("measurement_enabled") is True
                        and measurement.get("action_enabled") is False
                        and measurement.get("configured_restart_threshold_mbps") is None
                        and measurement.get("restart_candidate_confirmed") is False
                        and measurement.get("shadow_candidate_confirmed") is False
                        and measurement.get("reason_code") not in {"ACK_RATE_CONFIG_INVALID", "ACK_RATE_MEASUREMENT_DISABLED"}
                        and not str(measurement.get("reason_code") or "").startswith("ACK_RATE_MEASUREMENT_EXCEPTION")
                    )
                    if not safe_measurement:
                        stop_reasons.append("MEASUREMENT_FAIL_CLOSED_CONTRACT_VIOLATION")
                    if measurement.get("status") == "VALID":
                        valid_measurements += 1
                        if latest.get("pending_effect") is not expected_pending_effect:
                            stop_reasons.append("PENDING_EFFECT_GATE_MISMATCH")
                        if expected_pending_effect and latest.get("healthy_baseline_eligible") is not False:
                            stop_reasons.append("STALE_PENDING_TRAINING_GATE_NOT_ENFORCED")
                        if (
                            not expected_pending_effect
                            and latest.get("queue_pressure") is False
                            and latest.get("healthy_baseline_eligible") is not True
                        ):
                            stop_reasons.append("HEALTHY_BASELINE_ELIGIBILITY_NOT_RESTORED")
                elif elapsed >= args.startup_grace_sec:
                    stop_reasons.append("MEASUREMENT_STATE_MISSING")
                if elapsed >= 120.0 and valid_measurements == 0:
                    stop_reasons.append("NO_VALID_MEASUREMENT_AFTER_120S")

                controller_size = CONTROLLER_STATE.stat().st_size
                if controller_size > 2_000_000:
                    stop_reasons.append("CONTROLLER_STATE_SIZE_EXCEEDED")

                if (
                    ledger["integrity"] != "ok"
                    or ledger["unresolved_scope_count"] != 0
                    or ledger["request_count"] != args.expected_ledger_request_count
                    or ledger["scope_count"] != args.expected_ledger_scope_count
                ):
                    stop_reasons.append("LEDGER_CHANGED_OR_UNHEALTHY")

                service_result = systemctl_value(CONTROLLER_UNIT, "Result")
                service_status = int(systemctl_value(CONTROLLER_UNIT, "ExecMainStatus") or -1)
                timer_active = systemctl_value(TIMER_UNIT, "ActiveState")
                timer_sub = systemctl_value(TIMER_UNIT, "SubState")
                environment = systemctl_value(CONTROLLER_UNIT, "Environment")
                unit_ok = (
                    service_result == "success"
                    and service_status == 0
                    and timer_active == "active"
                    and f"PYTHONPATH={args.expected_controller_root}" in environment
                    and "FR_ACK_RATE_MEASUREMENT_ENABLED=true" in environment
                    and "FR_ACK_RATE_ACTION_ENABLED=false" in environment
                    and "FR_ACK_RATE_RESTART_THRESHOLD_MBPS=" in environment
                )
                if not unit_ok:
                    stop_reasons.append("CONTROLLER_UNIT_IDENTITY_OR_RESULT_INVALID")

                samples += 1
                record = {
                    "record_type": "sample",
                    "sequence": samples,
                    "observed_at_utc": iso(observed),
                    "elapsed_sec": round(elapsed, 3),
                    "status": "STOP" if stop_reasons else "OK",
                    "stop_reasons": sorted(set(stop_reasons)),
                    "deployment": {
                        "generation": deployment["metadata"].get("generation"),
                        "observed_generation": deployment_status.get("observedGeneration"),
                        "ready": deployment_status.get("readyReplicas"),
                    },
                    "pod": {
                        "name": pod_name,
                        "uid": pod_uid,
                        "restart_total": restart_total,
                        "all_ready": all_ready,
                    },
                    "runtime": {
                        "observed_at": runtime.get("observed_at"),
                        "age_sec": round(runtime_age, 3),
                        "ffmpeg_pid": ffmpeg_identity[0],
                        "bytes_acked": ack,
                        "notsent": int(tcp.get("notsent") or 0),
                        "unacked": int(tcp.get("unacked") or 0),
                        "lastsnd_ms": int(tcp.get("lastsnd_ms") or 0),
                    },
                    "controller": {
                        "pending_count": pending_count,
                        "restart_event_count": restart_event_count,
                        "state_size_bytes": controller_size,
                        "service_result": service_result,
                        "exec_status": service_status,
                        "timer": f"{timer_active}/{timer_sub}",
                    },
                    "measurement": {
                        "present": bool(measurement),
                        "status": measurement.get("status"),
                        "reason_code": measurement.get("reason_code"),
                        "action_enabled": measurement.get("action_enabled"),
                        "configured_threshold": measurement.get("configured_restart_threshold_mbps"),
                        "restart_candidate_confirmed": measurement.get("restart_candidate_confirmed"),
                        "history_sample_count": history_count,
                        "healthy_sample_count": statistics.get("healthy_sample_count"),
                        "baseline_ready": statistics.get("baseline_ready"),
                        "ack_mbps": latest.get("ack_mbps"),
                        "queue_pressure": latest.get("queue_pressure"),
                        "pending_effect": latest.get("pending_effect"),
                        "healthy_baseline_eligible": latest.get("healthy_baseline_eligible"),
                    },
                    "ledger": ledger,
                }
                append_record(handle, record)
                print(
                    json.dumps(
                        {
                            "seq": samples,
                            "elapsed": record["elapsed_sec"],
                            "ack": ack,
                            "measurement": record["measurement"],
                            "status": record["status"],
                            "stop": record["stop_reasons"],
                        },
                        separators=(",", ":"),
                    ),
                    flush=True,
                )
            except Exception as exc:
                stop_reasons = [f"MONITOR_EXCEPTION:{type(exc).__name__}:{exc}"]
                append_record(
                    handle,
                    {
                        "record_type": "monitor_exception",
                        "observed_at_utc": iso(observed),
                        "stop_reasons": stop_reasons,
                    },
                )
                print(json.dumps({"status": "STOP", "stop": stop_reasons}), flush=True)

            if stop_reasons:
                return 2
            if elapsed >= args.duration_sec:
                ended = now_utc()
                summary = {
                    "record_type": "summary",
                    "status": "PASS",
                    "ended_at_utc": iso(ended),
                    "ended_at_jst": ended.astimezone(JST).isoformat(timespec="milliseconds"),
                    "elapsed_sec": round(elapsed, 3),
                    "sample_count": samples,
                    "valid_measurement_samples": valid_measurements,
                    "minimum_bytes_acked": minimum_ack,
                    "maximum_bytes_acked": maximum_ack,
                    "bytes_acked_delta": (None if minimum_ack is None or maximum_ack is None else maximum_ack - minimum_ack),
                    "minimum_history_sample_count": min(history_counts, default=0),
                    "maximum_history_sample_count": max(history_counts, default=0),
                    "minimum_healthy_sample_count": min(healthy_counts, default=0),
                    "maximum_healthy_sample_count": max(healthy_counts, default=0),
                }
                append_record(handle, summary)
                print(json.dumps(summary, separators=(",", ":")), flush=True)
                break
            time.sleep(max(0.0, args.interval_sec - (time.monotonic() - sample_started)))

    digest = hashlib.sha256(args.output.read_bytes()).hexdigest()
    print(json.dumps({"evidence": str(args.output), "sha256": digest}, separators=(",", ":")), flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
