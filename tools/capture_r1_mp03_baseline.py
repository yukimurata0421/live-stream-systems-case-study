#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo


def run(command: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, check=False, capture_output=True, text=True, timeout=30)


def json_command(command: list[str]) -> dict[str, Any]:
    completed = run(command)
    if completed.returncode != 0:
        raise RuntimeError(f"command failed rc={completed.returncode}: {command[0]}")
    value = json.loads(completed.stdout)
    return value if isinstance(value, dict) else {}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--namespace", default="stream-v3")
    parser.add_argument("--deployment", default="stream-v3-runtime")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    deployment = json_command(["kubectl", "-n", args.namespace, "get", "deployment", args.deployment, "-o", "json"])
    pods = json_command(
        [
            "kubectl",
            "-n",
            args.namespace,
            "get",
            "pods",
            "-l",
            "app.kubernetes.io/name=stream-v3,app.kubernetes.io/component=runtime",
            "-o",
            "json",
        ]
    )
    running = [item for item in pods.get("items", []) if item.get("status", {}).get("phase") == "Running"]
    if len(running) != 1:
        raise RuntimeError(f"expected one running runtime Pod, got {len(running)}")
    pod = running[0]
    pod_name = pod["metadata"]["name"]
    statuses = {
        item["name"]: {
            "container_id": item.get("containerID", ""),
            "image": item.get("image", ""),
            "image_id": item.get("imageID", ""),
            "ready": item.get("ready", False),
            "restart_count": item.get("restartCount", 0),
        }
        for item in pod.get("status", {}).get("containerStatuses", [])
    }
    process_script = """
import json,pathlib
matches=[]
for p in pathlib.Path('/proc').iterdir():
    if not p.name.isdigit(): continue
    try: cmd=(p/'cmdline').read_bytes().replace(b'\\0',b' ').decode(errors='replace')
    except OSError: continue
    if 'python3 -m stream_v3.control_loop --mode streaming' not in cmd or 'python3 -c ' in cmd: continue
    status={}
    for line in (p/'status').read_text().splitlines():
        if ':' in line and line.split(':',1)[0] in {'VmRSS','Threads'}:
            k,v=line.split(':',1); status[k]=v.strip()
    matches.append({'pid':int(p.name),'fd_count':len(list((p/'fd').iterdir())),'status':status})
print(json.dumps({'matches':matches},sort_keys=True))
"""
    process = json_command(
        ["kubectl", "-n", args.namespace, "exec", pod_name, "-c", "fast-recovery-loop", "--", "python3", "-c", process_script]
    )
    state_script = """
import json,pathlib
p=pathlib.Path('/state/fast_recovery_state.json')
x=json.loads(p.read_text())
print(json.dumps({
 'last_restart_ts':int(x.get('last_restart_ts',0) or 0),
 'last_restart_failure_ts':int(x.get('last_restart_failure_ts',0) or 0),
 'restart_failure_count':int(x.get('restart_failure_count',0) or 0),
 'restart_event_count':len(x.get('restart_events') or []),
 'pending_recovery_count':len(x.get('pending_recovery_actions') or []),
 'last_pid':int(x.get('last_pid',0) or 0),
 'last_reason':str(x.get('last_reason') or ''),
 'observed_ts':int(x.get('observed_ts',0) or 0),
},sort_keys=True))
"""
    fast_recovery_state = json_command(
        ["kubectl", "-n", args.namespace, "exec", pod_name, "-c", "fast-recovery-loop", "--", "python3", "-c", state_script]
    )
    target = json_command(
        [
            "sudo",
            "-n",
            "python3",
            "-c",
            (
                "import json; "
                "x=json.load(open('/var/lib/stream-recovery-control/dell/target_snapshot.json')); "
                "print(json.dumps(x,sort_keys=True))"
            ),
        ]
    )
    maintenance = json_command(
        [
            "sudo",
            "-n",
            "python3",
            "-c",
            (
                "import json; "
                "x=json.load(open('/var/lib/stream-recovery-control/dell/p1-live/maintenance-audit-state.json')); "
                "print(json.dumps(x,sort_keys=True))"
            ),
        ]
    )
    top = run(["kubectl", "-n", args.namespace, "top", "pod", pod_name, "--containers", "--no-headers"])
    service = run(
        [
            "systemctl",
            "show",
            "dell-recovery-agent-shadow.service",
            "-p",
            "ActiveState",
            "-p",
            "SubState",
            "-p",
            "NRestarts",
            "-p",
            "MainPID",
            "-p",
            "MemoryCurrent",
            "-p",
            "TasksCurrent",
        ]
    )
    now = datetime.now(UTC)
    payload = {
        "schema_version": "recovery_control.r1_mp03_live_baseline.v1",
        "timestamp_utc": now.isoformat(timespec="seconds").replace("+00:00", "Z"),
        "timestamp_jst": now.astimezone(ZoneInfo("Asia/Tokyo")).isoformat(timespec="seconds"),
        "deployment": {
            "name": deployment["metadata"]["name"],
            "uid": deployment["metadata"]["uid"],
            "generation": deployment["metadata"].get("generation"),
            "observed_generation": deployment.get("status", {}).get("observedGeneration"),
        },
        "pod": {
            "name": pod_name,
            "uid": pod["metadata"]["uid"],
            "node": pod.get("spec", {}).get("nodeName", ""),
            "phase": pod.get("status", {}).get("phase", ""),
            "containers": statuses,
        },
        "fast_recovery_process": process,
        "fast_recovery_state": fast_recovery_state,
        "target_snapshot": target,
        "maintenance_snapshot": maintenance,
        "resource_top": {"exit_code": top.returncode, "rows": top.stdout.splitlines()},
        "dell_agent_service": {"exit_code": service.returncode, "properties": service.stdout.splitlines()},
        "candidate_loaded": False,
        "production_behavior_modified": False,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(payload, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
