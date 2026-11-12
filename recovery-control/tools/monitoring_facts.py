#!/usr/bin/env python3
"""Project fixed CRA state read-only without accessing control sockets or private keys."""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import re
import sqlite3
import subprocess
import time
from datetime import datetime
from pathlib import Path

SCHEMA = "cra.monitoring_facts.v1"
SAFE = re.compile(r"[A-Z][A-Z0-9_]{0,159}\Z")
FIELDS = {
    "mode",
    "previous_version",
    "version",
    "policy_version",
    "outcome",
    "reason",
    "state",
    "action_id",
    "result",
    "samples",
    "stalled_samples",
    "confirmed_stall_samples",
    "streak",
}


def load(path):
    with Path(path).open("rb") as f:
        raw = f.read(1048577)
    if len(raw) > 1048576:
        raise ValueError("facts_source_too_large")
    value = json.loads(raw)
    if not isinstance(value, dict):
        raise ValueError("facts_source_invalid")
    return value


def stamp(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()


def sanitized(payload):
    """Exclude free-form errors, destinations and credentials from monitoring logs."""
    return {
        k: v
        for k, v in payload.items()
        if k in FIELDS
        and (type(v) in (int, bool) or isinstance(v, str) and (SAFE.fullmatch(v) or k == "mode" and v in ("off", "dry_run", "enforce")))
    }


def active(unit):
    p = subprocess.run(["/usr/bin/systemctl", "is-active", unit], capture_output=True, text=True, timeout=2)
    return int(p.stdout.strip() == "active")


def connect(path):
    if not Path(path).is_file():
        raise ValueError("facts_database_missing")
    c = sqlite3.connect(Path(path).as_uri() + "?mode=ro", uri=True, timeout=2)
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA query_only=ON")
    return c


def dell(after):
    cfg = load("/etc/cra-agent/agent.json")
    status = load(cfg["status_path"])
    obs = load(cfg["observation_path"])
    with connect(cfg["db_path"]) as c:
        high = c.execute("SELECT coalesce(max(seq),0) FROM audit").fetchone()[0]
        ack = int(c.execute("SELECT value FROM kv WHERE key='acked_seq'").fetchone()[0])
        instance = c.execute("SELECT value FROM kv WHERE key='instance_id'").fetchone()[0]
        actions = c.execute("SELECT count(*) FROM actions WHERE mode='enforce'").fetchone()[0]
        unknown = c.execute("SELECT count(*) FROM actions WHERE mode='enforce' AND state='OUTCOME_UNKNOWN'").fetchone()[0]
        audit = []
        for r in c.execute("SELECT seq,ts,kind,payload FROM audit WHERE seq>? ORDER BY seq LIMIT 200", (after,)):
            audit.append({"seq": r["seq"], "ts": r["ts"], "kind": r["kind"], "payload": sanitized(json.loads(r["payload"]))})
    observer = {"available": 0}
    try:
        p = Path(__file__).with_name("pod_api_observer.py")
        spec = importlib.util.spec_from_file_location("cra_pod_reader", p)
        m = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(m)
        raw = m.exchange(operation="status")["status"]
        observer = {"available": 1, **{k: raw.get(k, 0) for k in ("successful_reads", "failed_reads", "busy_rejections")}}
        for key in ("last_request", "last_failure"):
            v = raw.get(key) or {}
            observer[key] = {k: v[k] for k in ("reason", "stage", "duration_seconds", "observed_at_utc") if k in v}
    except (OSError, ValueError, KeyError, TypeError, ImportError):
        pass
    return {
        "agent_service_up": active("cra-agent.service"),
        "status_timestamp": stamp(status["updated_at"]),
        "mode": status["policy"]["mode"],
        "policy_version": status["policy"]["version"],
        "decision": status["decision"]["reason"],
        "local_stop": bool(status["local_stop"]),
        "observation_timestamp": stamp(obs["observed_at"]),
        "target_valid": int(obs.get("target_snapshot_status") == "VALID" and obs.get("runtime_snapshot_status") == "VALID"),
        "authority_match": int(
            obs.get("active_producer_id") == cfg["producer_id"] and obs.get("active_producer_generation") == cfg["producer_generation"]
        ),
        "in_flight": obs.get("in_flight_count", 0),
        "enforce_actions": actions,
        "unknown_actions": unknown,
        "instance_id": instance,
        "audit_high": high,
        "audit_acked": ack,
        "audit": audit,
        "observer": observer,
        "legacy_timer_active": active("stream-v3-fast-recovery-controller.timer"),
    }


def central():
    cfg = load("/etc/cra-central/central.json")
    report = load("/var/lib/cra-central/verify-latest.json")
    with connect(cfg["db_path"]) as c:
        streams = [dict(r) for r in c.execute("SELECT agent_id,instance_id,max(seq) AS high FROM audit GROUP BY agent_id,instance_id")]
        version = c.execute("SELECT coalesce(max(version),0) FROM policies").fetchone()[0]
        gaps = c.execute("SELECT count(*) FROM audit_gaps WHERE resolved_at IS NULL").fetchone()[0]
        conflicts = c.execute("SELECT count(*) FROM conflicts").fetchone()[0]
    integrity = report["audit_integrity"]
    return {
        "central_service_up": active("cra-central.service"),
        "verify_timer_up": active("cra-central-verify.timer"),
        "verification_timestamp": stamp(report["checked_at"]),
        "violations": len(report["violations"]),
        "violation_codes": sorted({v["code"] for v in report["violations"]}),
        "missing_seq": integrity["missing_seq_count"],
        "unresolved_gaps": gaps,
        "conflicts": conflicts,
        "policy_version": version,
        "streams": streams,
    }


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("role", choices=("dell", "central"))
    p.add_argument("--after", type=int, default=0)
    a = p.parse_args()
    if not 0 <= a.after <= 2**63 - 1:
        p.error("invalid sequence")
    host = load("/etc/codex-host-context/host.json")
    # host.json format is read only for identity; no authorization is inferred.
    expected = {"dell": "delivery-host", "central": "authority-host"}[a.role]
    if host.get("host_id") != expected:
        raise ValueError("facts_host_identity_mismatch")
    value = dell(a.after) if a.role == "dell" else central()
    result = {
        "schema": SCHEMA,
        "role": a.role,
        "host_id": expected,
        "boot_id": Path("/proc/sys/kernel/random/boot_id").read_text().strip(),
        "collected_at": time.time(),
        "collected_boottime": time.clock_gettime(time.CLOCK_BOOTTIME),
        "source_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "data": value,
    }
    print(json.dumps(result, separators=(",", ":"), allow_nan=False))


if __name__ == "__main__":
    main()
