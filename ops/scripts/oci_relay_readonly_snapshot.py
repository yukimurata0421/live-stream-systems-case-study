#!/usr/bin/env python3
"""Read-only relay facts from fixed local sources and GET endpoints."""
from __future__ import annotations

import hashlib
import json
import math
import subprocess
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path


def read_json(path: Path, limit: int = 16384) -> dict:
    if path.is_symlink() or not path.is_file():
        raise ValueError("SOURCE_NOT_REGULAR")
    with path.open("rb") as stream:
        raw = stream.read(limit + 1)
    if len(raw) > limit:
        raise ValueError("SOURCE_TOO_LARGE")
    value = json.loads(raw)
    if not isinstance(value, dict):
        raise ValueError("SOURCE_NOT_OBJECT")
    return value


def count(value: object) -> int:
    if type(value) is not int or value < 0:
        raise ValueError("COUNTER_INVALID")
    return value


def api(path: str) -> dict:
    # These are the only endpoints; redirects and proxy environment are disabled.
    class NoRedirect(urllib.request.HTTPRedirectHandler):
        def redirect_request(self, *args, **kwargs):
            return None
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())
    with opener.open("http://127.0.0.1:19998" + path, timeout=0.8) as response:
        raw = response.read(65537)
    if len(raw) > 65536:
        raise ValueError("API_TOO_LARGE")
    value = json.loads(raw)
    if not isinstance(value, dict) or not isinstance(value.get("items"), list):
        raise ValueError("API_SCHEMA_INVALID")
    return value


def snapshot() -> dict:
    contract = read_json(Path("/etc/stream-host-context/host.json"))
    machine = hashlib.sha256(Path("/etc/machine-id").read_bytes().strip()).hexdigest()
    if contract.get("host_id") != "relay-host" or contract.get("machine_id_sha256") != machine:
        raise ValueError("HOST_IDENTITY_MISMATCH")
    guard_path = Path("/run/stream-v3-oci-relay/guard-status.json")
    guard = read_json(guard_path)
    age = time.clock_gettime(time.CLOCK_BOOTTIME) - guard["heartbeat_elapsed"]
    if guard.get("schema") != "stream.relay_status.v1" or not 0 <= age <= 5:
        raise ValueError("GUARD_STALE")
    inputs = [c for c in api("/v3/rtmps/conns/list")["items"]
              if c.get("path") == "relay" and c.get("state") == "publish"]
    if len(inputs) > 1:
        raise ValueError("INPUT_CARDINALITY_INVALID")
    destinations = api("/v3/paths/forward-dests/list?path=relay")["items"]
    positions = [d.get("pos") for d in destinations]
    if len(set(positions)) != len(positions) or any(type(p) is not int or p not in (1, 2) for p in positions):
        raise ValueError("PROVIDER_BINDING_INVALID")
    planned = None
    daily_path = Path("/var/lib/stream-twitch-daily/status.json")
    try:
        daily = read_json(daily_path)
        observed = datetime.fromisoformat(daily["observed_at"].replace("Z", "+00:00")).timestamp()
        if 0 <= time.time() - observed <= 60:
            planned = daily.get("state") in ("WAIT_STOP", "WAIT_OFFLINE", "WAIT_START", "VERIFYING")
    except (OSError, ValueError, KeyError, TypeError):
        pass
    after = read_json(guard_path)
    if after.get("instance") != guard.get("instance"):
        raise ValueError("RELAY_INSTANCE_CHANGED")
    if not isinstance(after.get("input_age"), (float, int)) or not math.isfinite(after["input_age"]) or after["input_age"] < 0:
        raise ValueError("INPUT_AGE_INVALID")
    pid = int(subprocess.check_output(["systemctl", "show", "stream-v3-oci-relay.service",
                                       "-p", "MainPID", "--value"], timeout=1))
    command = Path(f"/proc/{pid}/cmdline").read_bytes().split(b"\0")
    sources = [Path(arg.decode()) for arg in command
               if arg.startswith(b"/opt/stream-v3-oci-relay/") and arg.endswith(b".py")]
    if {p.name for p in sources} != {"relay_supervisor.py", "relay_guard.py"}:
        raise ValueError("RELAY_RELEASE_UNBOUND")
    release_digest = hashlib.sha256(b"".join(p.read_bytes() for p in sorted(sources))).hexdigest()
    input_fact = None if not inputs else {
        "id": str(inputs[0]["id"]), "bytes": count(inputs[0]["inboundBytes"]), "state": "publish"}
    return {
        "schema": "stream.route_oci_facts.v1", "observed_at": datetime.now(timezone.utc).isoformat(),
        "host_boot_id": Path("/proc/sys/kernel/random/boot_id").read_text().strip(),
        "relay_source_sha256": release_digest,
        "relay_instance": str(guard["instance"]), "guard_age_sec": round(age, 6),
        "enabled": guard.get("enabled") is True, "halted": guard.get("halted") is True,
        "input_age_sec": after["input_age"], "input": input_fact,
        "twitch_planned_break": planned,
        "destinations": [{"provider": "youtube" if d["pos"] == 1 else "twitch",
                          "id": str(d["id"]), "state": str(d.get("state", "unknown")),
                          "bytes": count(d["outboundBytes"]), "error_present": bool(d.get("lastError"))}
                         for d in destinations],
    }


if __name__ == "__main__":
    try:
        result = snapshot()
    except Exception as error:
        result = {"schema": "stream.route_oci_facts.v1", "status": "UNKNOWN", "reason": type(error).__name__}
    print(json.dumps(result, allow_nan=False, separators=(",", ":")))
