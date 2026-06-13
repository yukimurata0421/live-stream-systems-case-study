#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


BASE_DIR = Path(__file__).resolve().parents[2]
DEFAULT_STATE_DIR = BASE_DIR / ".state" / "rtmps-observer"


def env(name: str, default: str = "") -> str:
    return os.environ.get(name, default).strip()


def iso_utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def iso_jst(ts_utc: str) -> str:
    dt = datetime.fromisoformat(ts_utc.replace("Z", "+00:00"))
    return (dt + timedelta(hours=9)).isoformat(timespec="seconds").replace("+00:00", "+09:00")


def append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))
        f.write("\n")


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.parent / f".{path.name}.{os.getpid()}.tmp"
    tmp.write_text(json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n", encoding="utf-8")
    tmp.replace(path)


def run(command: list[str], *, timeout_sec: float) -> dict[str, Any]:
    started = time.monotonic()
    try:
        completed = subprocess.run(command, capture_output=True, text=True, timeout=timeout_sec, check=False)
    except (OSError, subprocess.TimeoutExpired) as exc:
        return {
            "ok": False,
            "returncode": None,
            "stdout": "",
            "stderr": f"{type(exc).__name__}: {exc}",
            "elapsed_ms": round((time.monotonic() - started) * 1000, 1),
        }
    return {
        "ok": completed.returncode == 0,
        "returncode": completed.returncode,
        "stdout": completed.stdout,
        "stderr": completed.stderr,
        "elapsed_ms": round((time.monotonic() - started) * 1000, 1),
    }


def select_running_pod(namespace: str, pod_name: str, pod_prefix: str, label_selector: str, timeout_sec: float) -> dict[str, Any]:
    if pod_name:
        return {"ok": True, "pod": pod_name, "selection": "explicit"}

    command = ["kubectl", "-n", namespace, "get", "pods", "-o", "json"]
    if label_selector:
        command.extend(["-l", label_selector])
    result = run(command, timeout_sec=timeout_sec)
    if not result["ok"]:
        return {"ok": False, "error": result["stderr"] or result["stdout"], "command": command}

    try:
        document = json.loads(result["stdout"])
    except json.JSONDecodeError as exc:
        return {"ok": False, "error": f"JSONDecodeError: {exc}", "command": command}

    candidates = []
    for item in document.get("items", []):
        metadata = item.get("metadata", {})
        status = item.get("status", {})
        name = str(metadata.get("name", ""))
        if pod_prefix and not name.startswith(pod_prefix):
            continue
        if status.get("phase") != "Running":
            continue
        start_time = str(status.get("startTime", ""))
        candidates.append((start_time, name))
    if not candidates:
        return {"ok": False, "error": "no_running_pod_found", "command": command, "pod_prefix": pod_prefix, "label_selector": label_selector}
    candidates.sort(reverse=True)
    return {"ok": True, "pod": candidates[0][1], "selection": "running_pod", "candidate_count": len(candidates)}


def parse_int(value: str) -> int | None:
    try:
        return int(value)
    except ValueError:
        return None


def parse_float(value: str) -> float | None:
    try:
        return float(value)
    except ValueError:
        return None


def split_metric_value(value: str) -> Any:
    cleaned = value.rstrip(",")
    if "/" in cleaned:
        parts = cleaned.split("/", 1)
        first_int = parse_int(parts[0])
        second_int = parse_int(parts[1])
        if first_int is not None and second_int is not None:
            return [first_int, second_int]
        first_float = parse_float(parts[0])
        second_float = parse_float(parts[1])
        if first_float is not None and second_float is not None:
            return [first_float, second_float]
    as_int = parse_int(cleaned)
    if as_int is not None:
        return as_int
    as_float = parse_float(cleaned)
    if as_float is not None:
        return as_float
    return cleaned


METRIC_RE = re.compile(r"(?P<key>[A-Za-z_][A-Za-z0-9_]*):(?P<value>[^\s]+)")


def parse_ss_metrics(details: list[str]) -> dict[str, Any]:
    metrics: dict[str, Any] = {}
    raw = " ".join(details)
    for match in METRIC_RE.finditer(raw):
        key = match.group("key")
        metrics[key] = split_metric_value(match.group("value"))
    rtt = metrics.get("rtt")
    if isinstance(rtt, list) and len(rtt) == 2:
        metrics["rtt_ms"] = rtt[0]
        metrics["rtt_var_ms"] = rtt[1]
    retrans = metrics.get("retrans")
    if isinstance(retrans, list) and len(retrans) == 2:
        metrics["retrans_current"] = retrans[0]
        metrics["retrans_total"] = retrans[1]
    return metrics


def parse_socket_summary(summary: str) -> dict[str, Any]:
    parts = summary.split(None, 5)
    payload: dict[str, Any] = {"summary": summary}
    if len(parts) >= 5:
        payload.update(
            {
                "state": parts[0],
                "recv_q": parse_int(parts[1]),
                "send_q": parse_int(parts[2]),
                "local": parts[3],
                "peer": parts[4],
            }
        )
    if len(parts) >= 6:
        payload["process"] = parts[5]
        pid_match = re.search(r"pid=(\d+)", parts[5])
        if pid_match:
            payload["pid"] = int(pid_match.group(1))
    return payload


def is_rtmps_ffmpeg_socket(socket_payload: dict[str, Any]) -> bool:
    combined = " ".join(
        str(socket_payload.get(key, "")) for key in ("summary", "process", "details_raw")
    ).lower()
    peer = str(socket_payload.get("peer", ""))
    return "ffmpeg" in combined and peer.endswith(":443")


def parse_ss_output(output: str) -> list[dict[str, Any]]:
    groups: list[tuple[str, list[str]]] = []
    current_summary = ""
    current_details: list[str] = []
    for line in output.splitlines():
        if not line.strip() or line.lstrip().startswith("State "):
            continue
        if not line[0].isspace():
            if current_summary:
                groups.append((current_summary, current_details))
            current_summary = line.strip()
            current_details = []
        elif current_summary:
            current_details.append(line.strip())
    if current_summary:
        groups.append((current_summary, current_details))

    sockets = []
    for summary, details in groups:
        parsed = parse_socket_summary(summary)
        parsed["details_raw"] = details
        parsed["metrics"] = parse_ss_metrics(details)
        if is_rtmps_ffmpeg_socket(parsed):
            sockets.append(parsed)
    return sockets


def build_payload(args: argparse.Namespace) -> dict[str, Any]:
    ts_utc = iso_utc_now()
    pod_selection: dict[str, Any] = {"ok": True, "pod": args.pod, "selection": "local"}
    if not args.local:
        pod_selection = select_running_pod(args.namespace, args.pod, args.pod_prefix, args.pod_label_selector, args.kubectl_timeout_sec)
        if not pod_selection.get("ok"):
            return {
                "schema": "stream_v3_rtmps_tcp_burst_observer/v1",
                "ts_utc": ts_utc,
                "ts_jst": iso_jst(ts_utc),
                "sample_reason": args.sample_reason,
                "ok": False,
                "error": pod_selection.get("error", "pod_selection_failed"),
                "pod_selection": pod_selection,
                "rtmps_socket_connected": False,
                "sockets": [],
            }

    if args.local:
        command = ["ss", "-tinp"]
    else:
        command = [
            "kubectl",
            "-n",
            args.namespace,
            "exec",
            str(pod_selection["pod"]),
            "-c",
            args.container,
            "--",
            "ss",
            "-tinp",
        ]

    result = run(command, timeout_sec=args.command_timeout_sec)
    sockets = parse_ss_output(result["stdout"]) if result["stdout"] else []
    return {
        "schema": "stream_v3_rtmps_tcp_burst_observer/v1",
        "ts_utc": ts_utc,
        "ts_jst": iso_jst(ts_utc),
        "sample_reason": args.sample_reason,
        "ok": bool(result["ok"]),
        "error": "" if result["ok"] else (result["stderr"] or result["stdout"]).strip(),
        "command": command,
        "command_elapsed_ms": result["elapsed_ms"],
        "pod_selection": pod_selection,
        "namespace": args.namespace,
        "container": args.container,
        "rtmps_socket_connected": bool(sockets),
        "socket_count": len(sockets),
        "sockets": sockets,
    }


def write_sample(args: argparse.Namespace) -> None:
    payload = build_payload(args)
    append_jsonl(args.output_jsonl, payload)
    write_json(args.latest_file, payload)
    print(json.dumps(payload, ensure_ascii=False, separators=(",", ":")), flush=True)


def run_observer(args: argparse.Namespace) -> None:
    completed = 0
    started = time.monotonic()
    while True:
        loop_started = time.monotonic()
        write_sample(args)
        completed += 1
        if args.cycles > 0 and completed >= args.cycles:
            break
        if args.duration_sec > 0 and time.monotonic() - started >= args.duration_sec:
            break
        sleep_sec = args.interval_sec - (time.monotonic() - loop_started)
        time.sleep(max(0.0, sleep_sec))


def parse_args() -> argparse.Namespace:
    state_dir = Path(env("RTMPS_OBSERVER_STATE_DIR", str(DEFAULT_STATE_DIR)))
    parser = argparse.ArgumentParser(description="Sample ffmpeg RTMPS TCP socket state at burst cadence.")
    parser.add_argument("--namespace", default=env("RTMPS_OBSERVER_NAMESPACE", "stream-v3"))
    parser.add_argument("--pod", default=env("RTMPS_OBSERVER_POD", ""))
    parser.add_argument("--pod-prefix", default=env("RTMPS_OBSERVER_POD_PREFIX", "stream-v3-runtime-"))
    parser.add_argument("--pod-label-selector", default=env("RTMPS_OBSERVER_POD_LABEL_SELECTOR", ""))
    parser.add_argument("--container", default=env("RTMPS_OBSERVER_CONTAINER", "stream-engine"))
    parser.add_argument("--local", action="store_true", default=env("RTMPS_OBSERVER_LOCAL", "0").lower() in {"1", "true", "yes"})
    parser.add_argument("--interval-sec", type=float, default=float(env("RTMPS_OBSERVER_INTERVAL_SEC", "5") or "5"))
    parser.add_argument("--duration-sec", type=float, default=float(env("RTMPS_OBSERVER_DURATION_SEC", "0") or "0"))
    parser.add_argument("--cycles", type=int, default=int(env("RTMPS_OBSERVER_CYCLES", "0") or "0"))
    parser.add_argument("--sample-reason", default=env("RTMPS_OBSERVER_SAMPLE_REASON", "manual"))
    parser.add_argument("--kubectl-timeout-sec", type=float, default=float(env("RTMPS_OBSERVER_KUBECTL_TIMEOUT_SEC", "5") or "5"))
    parser.add_argument("--command-timeout-sec", type=float, default=float(env("RTMPS_OBSERVER_COMMAND_TIMEOUT_SEC", "5") or "5"))
    parser.add_argument("--latest-file", type=Path, default=Path(env("RTMPS_OBSERVER_LATEST_FILE", str(state_dir / "rtmps_tcp_burst_observer_latest.json"))))
    parser.add_argument("--output-jsonl", type=Path, default=Path(env("RTMPS_OBSERVER_OUTPUT_JSONL", str(state_dir / "logs" / "rtmps_tcp_burst_observer.jsonl"))))
    return parser.parse_args()


def main() -> int:
    run_observer(parse_args())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
