#!/usr/bin/env python3
"""Observe 48 hours after an NVIDIA update without changing delivery, packages or notifications."""
from __future__ import annotations

import argparse
import csv
import hashlib
import importlib.util
import json
import math
import os
import re
import sqlite3
import sys
import time
from contextlib import closing
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "src"))
DURATION = 48 * 3600
INTERVAL = 60
MAX_GAP = 150
TRANSPORT_RECOVERY_SEC = 300
POLICY = "driver_with_runtime_transitions_v3"
RUNTIME_TRANSITION_SEC = 300
TRANSPORT_COUNTERS = ("connection_generation", "reconnections_observed_total", "fifo_recovery_reports_total",
                      "fifo_recovery_failures_total", "fifo_queue_overflows_total")
MAX_BYTES = 1024 * 1024
APP_ID = 0x4E565331
DEFAULT_STATE = Path("/var/lib/stream-v3-nvidia-smoke")
TRANSPORT = Path("/run/stream-v3-transport-export/ffmpeg-transport.json")
KUBECTL = "/usr/local/bin/kubectl"
PACKAGES = ("nvidia-driver-580", "nvidia-dkms-580", "nvidia-utils-580", "libnvidia-encode-580")
SOURCES = (
    Path(__file__).resolve(),
    ROOT / "ops/scripts/stream_v3_map_runtime_probe.py",
    ROOT / "ops/scripts/stream_v3_transport_recorder.py",
    ROOT / "src/stream_core/common/observation_time.py",
    ROOT / "src/stream_core/common/__init__.py",
)


def packed(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def jst(wall):
    return datetime.fromtimestamp(wall, ZoneInfo("Asia/Tokyo")).isoformat()


def clocks():
    def identity(path):
        try:
            return Path(path).read_text().strip()
        except OSError:
            return ""
    machine = identity("/etc/machine-id")
    return {
        "wall": time.time(), "mono": time.clock_gettime(time.CLOCK_BOOTTIME),
        "boot": identity("/proc/sys/kernel/random/boot_id"),
        "machine": hashlib.sha256(machine.encode()).hexdigest() if machine else "",
    }


def source_digest():
    return hashlib.sha256(packed({str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest()
                                 for p in SOURCES}).encode()).hexdigest()


def adapter(name, path):
    if name not in sys.modules:
        spec = importlib.util.spec_from_file_location(name, path)
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        try:
            spec.loader.exec_module(module)
        except BaseException:
            del sys.modules[name]
            raise
    return sys.modules[name]


def command(args):
    # Shared subprocess boundary; stderr/cmdline are never copied into evidence.
    probe = adapter("nvidia_smoke_map", SOURCES[1])
    cp = probe.command(args, timeout_sec=12)
    if cp.returncode or len(cp.stdout.encode()) > MAX_BYTES:
        raise ValueError("COMMAND_FAILED_OR_TOO_LARGE")
    return cp.stdout


def kernel_sample(cursor):
    args = ["/usr/bin/journalctl", "-k", "-b", "--no-pager", "-o", "json", "-n"]
    args += ["1001", "--after-cursor=" + cursor] if cursor else ["1"]
    rows = [json.loads(line) for line in command(args).splitlines() if line.strip()]
    if len(rows) >= 1001 or (not rows and not cursor):
        raise ValueError("KERNEL_JOURNAL_COVERAGE_UNKNOWN")
    # The first cursor is a baseline, not evidence that an older Xid occurred in this campaign.
    bad = sum(bool(re.search(r"NVRM:.*Xid|fallen off the bus|API mismatch|RmInitAdapter.*failed",
                             str(row.get("MESSAGE", "")), re.I)) for row in rows) if cursor else 0
    return {"cursor": rows[-1]["__CURSOR"] if rows else cursor, "gpu_errors": bad}


def pod_binding(raw):
    rows = raw["status"]["containerStatuses"]
    return {
        "pod_uid": raw["metadata"]["uid"],
        "node": raw["spec"]["nodeName"],
        "containers": {r["name"]: {k: r[k] for k in ("containerID", "imageID", "restartCount")}
                       for r in rows},
    }


def controller(raw, kind):
    refs = [r for r in raw["metadata"].get("ownerReferences", [])
            if r.get("controller") is True and r.get("kind") == kind]
    if len(refs) != 1 or not refs[0].get("uid"):
        raise ValueError("RUNTIME_OWNER_UNBOUND")
    return refs[0]["uid"]


def epoch(runtime):
    return hashlib.sha256(packed({k: runtime[k] for k in
        ("binding", "ffmpeg_pid", "asset_revision", "ffmpeg_generation", "configuration_sha256")}).encode()).hexdigest()


def process_epoch(runtime):
    return packed([runtime["binding"]["pod_uid"], runtime["binding"]["containers"]["stream-engine"]["containerID"],
                   runtime["ffmpeg_pid"], runtime["ffmpeg_generation"]])


def collect(previous):
    """Run shared map/readiness/transport parsers; tests replace external I/O only."""
    start = clocks()
    result = {"clock": start, "failures": [], "unknowns": []}
    probe = adapter("nvidia_smoke_map", SOURCES[1])
    recorder = adapter("nvidia_smoke_transport", SOURCES[2])

    def read(name, fn):
        try:
            result[name] = fn()
        except Exception as exc:
            result["unknowns"].append(name.upper() + "_" + type(exc).__name__)

    def gpu():
        rows = list(csv.reader(command(["/usr/bin/nvidia-smi", "--query-gpu=uuid,driver_version",
                                        "--format=csv,noheader"]).splitlines()))
        if not rows or any(len(r) != 2 or not all(v.strip() for v in r) for r in rows):
            raise ValueError("GPU_QUERY_INVALID")
        return sorted([v.strip() for v in row] for row in rows)

    def packages():
        raw = command(["/usr/bin/dpkg-query", "-W", "-f=${Package}\t${db:Status-Abbrev}\t${Version}\n",
                       *PACKAGES])
        rows = [line.split("\t") for line in raw.splitlines()]
        if any(len(r) != 3 or r[1] != "ii " for r in rows):
            raise ValueError("PACKAGE_STATUS_INVALID")
        values = {r[0]: r[2] for r in rows}
        if set(values) != set(PACKAGES):
            raise ValueError("PACKAGES_MISSING")
        return values

    def runtime():
        namespace = "stream-v3"
        def get(*args):
            return probe.kubectl_json(KUBECTL, ["-n", namespace, "get", *args, "-o", "json"], timeout_sec=8)
        deployment = get("deployment", "stream-v3-runtime")
        raw = get("pods", "-l", probe.DEFAULT_SELECTOR)
        replicasets = get("replicasets", "-l", probe.DEFAULT_SELECTOR)["items"]
        anchor = {"deployment_uid": deployment["metadata"]["uid"]}
        old_anchor = (previous or {}).get("runtime_observation", {}).get("anchor", {})
        if old_anchor.get("deployment_uid", anchor["deployment_uid"]) != anchor["deployment_uid"]:
            raise ValueError("DEPLOYMENT_IDENTITY_CHANGED")
        sets = {r["metadata"]["uid"]: r for r in replicasets
                if controller(r, "Deployment") == anchor["deployment_uid"]}
        items = []
        for pod in raw["items"]:
            if controller(pod, "ReplicaSet") not in sets:
                raise ValueError("POD_NOT_OWNED_BY_DEPLOYMENT")
            if not pod["metadata"].get("deletionTimestamp"):
                items.append(pod)
        node = old_anchor.get("node") or (previous or {}).get("runtime", {}).get("binding", {}).get("node")
        if node and any(p.get("spec", {}).get("nodeName") not in (None, node) for p in items):
            raise ValueError("RUNTIME_NODE_CHANGED")
        anchor["node"] = node or (items[0].get("spec", {}).get("nodeName") if items else None)

        def pending(reason):
            return {"transition": reason, "anchor": anchor}

        if deployment["spec"]["replicas"] not in (0, 1):
            raise ValueError("UNSUPPORTED_DEPLOYMENT_SCALE")
        if not items or not deployment["spec"]["replicas"]:
            return pending("PODS_ABSENT_OR_SCALED_DOWN")
        # Prefer a ready Pod matching the desired ReplicaSet template, otherwise
        # keep observing the ready old Pod while its replacement is starting.
        def template(value):
            result = json.loads(packed(value))
            result.setdefault("metadata", {}).setdefault("labels", {}).pop("pod-template-hash", None)
            result["metadata"].pop("creationTimestamp", None)
            return result
        desired = {uid for uid, rs in sets.items()
                   if template(rs["spec"]["template"]) == template(deployment["spec"]["template"])}
        ready_items = [p for p in items if p.get("status", {}).get("phase") == "Running"
                       and all(c.get("ready") is True for c in p.get("status", {}).get("containerStatuses", []))]
        candidates = [p for p in ready_items if controller(p, "ReplicaSet") in desired] or ready_items or items
        if len(candidates) != 1:
            return pending("ROLLOUT_NOT_UNIQUE")
        pod = candidates[0]
        if pod.get("status", {}).get("phase") != "Running" or not pod.get("status", {}).get("containerStatuses"):
            return pending("POD_STARTING")
        binding = pod_binding(pod)
        if not all("running" in c.get("state", {}) for c in pod["status"]["containerStatuses"]):
            return pending("CONTAINER_STARTING")
        name = pod["metadata"]["name"]
        try:
            ready = probe.runtime_readiness(KUBECTL, namespace, name, timeout_sec=8, allow_unready=True)
            process = probe.process_snapshot(KUBECTL, namespace, name, timeout_sec=12, gpu_only=True)
            after = get("pod", name)
        except (ValueError, OSError, KeyError):
            latest = get("pods", "-l", probe.DEFAULT_SELECTOR)["items"]
            if not any(p["metadata"]["uid"] == binding["pod_uid"] and not p["metadata"].get("deletionTimestamp") for p in latest):
                return pending("POD_REPLACED_DURING_SAMPLE")
            raise
        if binding != pod_binding(after) or after["metadata"].get("deletionTimestamp"):
            return pending("RUNTIME_CHANGED_DURING_SAMPLE")
        current_deployment = get("deployment", "stream-v3-runtime")
        if current_deployment["metadata"]["uid"] != anchor["deployment_uid"]:
            raise ValueError("DEPLOYMENT_IDENTITY_CHANGED")
        if current_deployment["metadata"]["generation"] != deployment["metadata"]["generation"]:
            return pending("DEPLOYMENT_CHANGED_DURING_SAMPLE")
        evaluated = probe.evaluate_sample({
            "deployment": probe.deployment_snapshot(deployment), "pods": probe.pod_snapshots({"items": [pod]}),
            "readiness": ready, "process": process,
        }, now_epoch=start["wall"])
        if ready.get("probe_error") or evaluated["probe_errors"]:
            if binding != (previous or {}).get("runtime", {}).get("binding") and previous:
                return pending("NEW_RUNTIME_PROBE_STARTING")
            raise ValueError("RUNTIME_READINESS_UNAVAILABLE")
        if ready.get("boot_id") != start["boot"] or ready.get("pod_uid") != binding["pod_uid"]:
            raise ValueError("RUNTIME_READINESS_UNBOUND")
        render = evaluated["render"]
        if render.get("probe_error"):
            if previous and binding != previous.get("runtime", {}).get("binding"):
                return pending("NEW_RENDER_STARTING")
            raise ValueError("RENDER_UNAVAILABLE")
        # Network readiness can drop while the same GPU/encoder keeps running.
        # Keep structural/container-running and all render/asset/semantic checks.
        expected = set(probe.DEFAULT_EXPECTED_CONTAINERS)
        gpu_runtime_ok = (
            ready.get("gpu_ok") is True and ready.get("nvenc_active") is True
            and type(ready.get("ffmpeg_pid")) is int and ready["ffmpeg_pid"] > 1
            and deployment["spec"]["replicas"] == 1
            and deployment["status"].get("observedGeneration", 0) >= deployment["metadata"]["generation"]
            and {c["name"] for c in deployment["spec"]["template"]["spec"]["containers"]} == expected
            and pod["status"]["phase"] == "Running" and set(binding["containers"]) == expected
            and all("running" in c.get("state", {}) for c in pod["status"]["containerStatuses"])
            and all(evaluated["conditions"].get(k) is True for k in (
                "render_heartbeat", "semantic_visual_contract", "asset_identity", "browser_contract"))
        )
        old_runtime = (previous or {}).get("runtime", {})
        if not gpu_runtime_ok and previous and (
            binding != old_runtime.get("binding") or ready.get("ffmpeg_pid") != old_runtime.get("ffmpeg_pid")
        ):
            return {**pending("NEW_RUNTIME_WARMING"), "driver": ready.get("driver_version")}
        return {
            "anchor": anchor,
            "configuration_sha256": hashlib.sha256(packed(pod["spec"].get("volumes", [])).encode()).hexdigest(),
            "binding": binding, "driver": ready.get("driver_version"), "ffmpeg_pid": ready.get("ffmpeg_pid"),
            "critical_ok": evaluated["delivery_critical_ok"], "conditions": evaluated["conditions"],
            "gpu_runtime_ok": gpu_runtime_ok, "socket_ok": ready.get("rtmp_socket_established") is True,
            "nvidia_rendering": evaluated["browser"]["rendering_backend"] == "nvidia-vulkan",
            "render_report_ms": render.get("reported_at_ms"), "render_age_sec": render.get("age_sec"),
            "asset_revision": render.get("asset_identity", {}).get("revision"),
        }

    def transport():
        raw = json.loads(recorder.bounded_read(TRANSPORT))
        value = recorder.snapshot(raw)
        # The producer can publish while earlier commands are still running.
        # Compare freshness at the read, not against the beginning of the poll.
        age = clocks()["wall"] - value["source_us"] / 1e6
        if not -1 <= age <= 30:
            raise ValueError("TRANSPORT_STALE")
        return value

    read("source", source_digest)
    read("gpu", gpu)
    read("packages", packages)
    # Read the loaded module, not modinfo's potentially newer on-disk module.
    read("module", lambda: Path("/sys/module/nvidia/version").read_text().strip())
    read("runtime", runtime)
    read("transport", transport)
    r = result.get("runtime", {})
    old_r = (previous or {}).get("runtime", {})
    if previous and r and not r.get("transition") and (
        r.get("binding") != old_r.get("binding") or r.get("ffmpeg_pid") != old_r.get("ffmpeg_pid")
    ) and ("transport" not in result or result["transport"]["ffmpeg_pid"] != r["ffmpeg_pid"]):
        result["runtime"] = {"transition": "NEW_RUNTIME_TRANSPORT_STARTING", "anchor": r["anchor"], "driver": r["driver"]}
    if "runtime" in result and "transport" in result and not result["runtime"].get("transition"):
        result["runtime"]["ffmpeg_generation"] = result["transport"]["ffmpeg_generation"]
    read("kernel", lambda: kernel_sample((previous or {}).get("kernel", {}).get("cursor", "")))
    end = clocks()
    if end["mono"] - start["mono"] > 55:
        result["unknowns"].append("COLLECTION_TOO_SLOW")
    if (end["boot"] != start["boot"] or end["machine"] != start["machine"]
            or end["mono"] < start["mono"]
            or abs((end["wall"] - start["wall"]) - (end["mono"] - start["mono"])) > 5):
        result["unknowns"].append("COLLECTION_CLOCK_OR_IDENTITY_CHANGED")
    # Start the qualification window only after its baseline has been collected.
    result["clock"] = end
    result["collection_started_mono"] = start["mono"]
    return result


def transport_assessment(observation, previous=None, *, reset=False):
    """Persist bounded network recovery separately from GPU qualification."""
    current = observation["transport"]
    prior = (previous or {}).get("transport", {})
    prior_result = (previous or {}).get("transport_observation", {})
    now = observation["clock"]["mono"]
    deltas = {k: current[k] - prior[k] for k in TRANSPORT_COUNTERS} if previous and not reset else {}
    totals = {k: prior_result.get("event_totals", {}).get(k, 0) + max(0, deltas.get(k, 0))
              for k in TRANSPORT_COUNTERS}
    result = {"state": "HEALTHY", "recovery_limit_sec": TRANSPORT_RECOVERY_SEC,
              "event_deltas": {k: v for k, v in deltas.items() if v > 0}, "event_totals": totals,
              "recovering_since_mono": None, "recovery_episodes": prior_result.get("recovery_episodes", 0),
              "recovered_episodes": prior_result.get("recovered_episodes", 0),
              "counter_resets_total": prior_result.get("counter_resets_total", 0) + int(reset)}
    failures, unknowns = [], []
    if (any(v < 0 for v in deltas.values()) or current["state"] == "COUNTER_REGRESSION"
            or previous and not reset and current["ack_bytes_observed_total"] < prior["ack_bytes_observed_total"]):
        unknowns.append("TRANSPORT_COUNTER_REGRESSION")
    advancing = current["ack_bytes_observed_total"] > (prior["ack_bytes_observed_total"] if previous and not reset else 0)
    healthy = (current["state"] in {"ACK_ADVANCING", "TCP_ESTABLISHED"}
               and not current["fifo_recovering"] and advancing
               and observation["runtime"].get("socket_ok", True))
    since = prior_result.get("recovering_since_mono")
    if not healthy:
        if since is None:
            since = previous["clock"]["mono"] if previous else now
            result["recovery_episodes"] += 1
        result.update(state="RECOVERING", recovering_since_mono=since)
        if previous is None:
            unknowns.append("TRANSPORT_BASELINE_NOT_HEALTHY")
    if since is not None and now - since >= TRANSPORT_RECOVERY_SEC:
        result.update(state="EXPIRED", recovering_since_mono=since)
        failures.append("TRANSPORT_RECOVERY_TIMEOUT")
    elif healthy and since is not None:
        result["recovered_episodes"] += 1
    return result, failures, unknowns


def runtime_assessment(observation, previous=None):
    """Follow owned runtime epochs without resetting the fixed host-driver window."""
    r = observation["runtime"]
    old = (previous or {}).get("runtime_observation", {})
    prior = (previous or {}).get("runtime", {})
    full = not r.get("transition") and "transport" in observation
    healthy = full and r.get("gpu_runtime_ok", r.get("critical_ok")) and r.get("nvidia_rendering")
    current_epoch = epoch(r) if full else None
    accepted = old.get("accepted_epoch", current_epoch if not old else None)
    result = {"state": "STABLE", "anchor": r.get("anchor", old.get("anchor")),
              "accepted_epoch": accepted, "accepted_binding": old.get("accepted_binding", r.get("binding")),
              "candidate_epoch": None, "pending_since_mono": None,
              "segments": old.get("segments", 1), "transition_episodes": old.get("transition_episodes", 0),
              "validated_runtime_sec": old.get("validated_runtime_sec", 0),
              "runtime_unverified_sec": old.get("runtime_unverified_sec", 0)}
    result["last_process_epoch"] = process_epoch(r) if full else old.get("last_process_epoch")
    failures, unknowns = [], []
    pending = old.get("pending_since_mono")
    changing = (not full or current_epoch != accepted or pending is not None)
    if not healthy and not changing:
        failures.append("GPU_RENDER_OR_NVENC_RUNTIME_UNHEALTHY")
    elif changing:
        if pending is None:
            pending = previous["clock"]["mono"] if previous else observation["clock"]["mono"]
            network_since = (previous or {}).get("transport_observation", {}).get("recovering_since_mono")
            if network_since is not None:
                pending = min(pending, network_since)
            result["transition_episodes"] += 1
        result.update(state="TRANSITION", pending_since_mono=pending)
        if healthy:
            result.update(state="VERIFYING", candidate_epoch=current_epoch)
            progressed = (old.get("candidate_epoch") == current_epoch and not prior.get("transition")
                          and r["render_report_ms"] > prior["render_report_ms"]
                          and observation.get("transport_observation", {}).get("state") == "HEALTHY"
                          and observation["transport"]["ack_bytes_observed_total"] > previous["transport"]["ack_bytes_observed_total"])
            if progressed:
                if current_epoch != accepted:
                    result["segments"] += 1
                    observation["runtime_transition_event"] = {"from_binding": result["accepted_binding"],
                        "to_binding": r["binding"], "from_epoch": accepted, "to_epoch": current_epoch}
                result.update(state="STABLE", accepted_epoch=current_epoch, accepted_binding=r["binding"],
                              pending_since_mono=None, candidate_epoch=None)
        if observation["clock"]["mono"] - pending >= RUNTIME_TRANSITION_SEC:
            result.update(state="EXPIRED", pending_since_mono=pending)
            failures.append("RUNTIME_TRANSITION_TIMEOUT")
        if previous is None:
            unknowns.append("RUNTIME_BASELINE_NOT_HEALTHY")
    if previous:
        gap = observation["clock"]["mono"] - previous["clock"]["mono"]
        key = "validated_runtime_sec" if result["state"] == "STABLE" and old.get("state", "STABLE") == "STABLE" else "runtime_unverified_sec"
        result[key] += gap
    return result, failures, unknowns


def assess(observation, expected, previous=None, *, observer_upgrade=False):
    failures = list(observation["failures"])
    unknowns = list(observation["unknowns"])
    observation["policy"] = POLICY
    if previous and previous.get("source_lineage"):
        observation["source_lineage"] = previous["source_lineage"]
    if not observation["clock"]["boot"] or not observation["clock"]["machine"]:
        unknowns.append("HOST_IDENTITY_MISSING")
    if observation.get("kernel", {}).get("gpu_errors"):
        failures.append("KERNEL_GPU_ERROR")
    try:
        versions = [row[1] for row in observation["gpu"]]
        if versions != [expected] or observation["module"] != expected:
            failures.append("HOST_DRIVER_MISMATCH")
        if any(v.split("-")[0] != expected for v in observation["packages"].values()):
            failures.append("PACKAGE_DRIVER_MISMATCH")
        if previous:
            for field in ("source", "gpu", "packages", "module"):
                if observation[field] != previous[field] and not (field == "source" and observer_upgrade):
                    failures.append(field.upper() + "_CHANGED")
        runtime = observation["runtime"]
        if runtime.get("driver") and runtime["driver"] != expected:
            failures.append("CONTAINER_DRIVER_MISMATCH")
        transitioning = bool(runtime.get("transition"))
        if transitioning:
            # Only explicitly observed rollout states allow unavailable transport;
            # all host/GPU/kernel/clock acquisition failures remain terminal.
            unknowns = [v for v in unknowns if not v.startswith("TRANSPORT_")]
            observation["transition_input_unknowns"] = [v for v in observation["unknowns"] if v.startswith("TRANSPORT_")]
            if previous:
                observation["transport_checkpoint"] = previous.get("transport_checkpoint", previous.get("transport"))
                observation["transport_observation"] = {**previous.get("transport_observation", {}), "state": "UNAVAILABLE"}
        else:
            transport = observation["transport"]
            if runtime["ffmpeg_pid"] != transport["ffmpeg_pid"]:
                unknowns.append("TRANSPORT_PROCESS_UNBOUND")
            prior_runtime = (previous or {}).get("runtime", {})
            old_process = (previous or {}).get("runtime_observation", {}).get("last_process_epoch")
            if not old_process and prior_runtime and not prior_runtime.get("transition"):
                old_process = process_epoch({**prior_runtime, "ffmpeg_generation":
                    prior_runtime.get("ffmpeg_generation", previous.get("transport", {}).get("ffmpeg_generation"))})
            reset = bool(old_process and process_epoch(runtime) != old_process)
            network_previous = previous
            if previous and prior_runtime.get("transition"):
                network_previous = {**previous, "transport": previous["transport_checkpoint"]}
            network, nf, nu = transport_assessment(observation, network_previous, reset=reset)
            observation["transport_observation"] = network
            failures.extend(nf)
            unknowns.extend(nu)
            report = runtime["render_report_ms"]
            if type(report) not in (int, float) or not math.isfinite(report) or report <= 0:
                unknowns.append("RENDER_PROGRESS_UNAVAILABLE")
            if previous and not prior_runtime.get("transition"):
                if transport["source_us"] <= previous["transport"]["source_us"]:
                    unknowns.append("TRANSPORT_SAMPLE_NOT_ADVANCING")
                old_epoch = previous.get("runtime_observation", {}).get("accepted_epoch")
                same = epoch(runtime) == old_epoch if old_epoch else runtime["binding"] == prior_runtime.get("binding")
                if same and report <= prior_runtime["render_report_ms"]:
                    failures.append("RENDER_NOT_ADVANCING")
        result, rf, ru = runtime_assessment(observation, previous)
        observation["runtime_observation"] = result
        failures.extend(rf)
        unknowns.extend(ru)
        # A new runtime must pass GPU/render checks before its first two progress
        # observations count as a verified successor; do not fail on warmup alone.
        if result["state"] in {"TRANSITION", "VERIFYING"}:
            unknowns = [v for v in unknowns if v != "TRANSPORT_BASELINE_NOT_HEALTHY"]
    except (KeyError, TypeError, ValueError, IndexError, OverflowError):
        unknowns.append("OBSERVATION_INCOMPLETE")
    return sorted(set(failures)), sorted(set(unknowns))


def database(directory, *, create=False, readonly=False):
    path = directory / "smoke.sqlite3"
    if create and not directory.exists():
        directory.mkdir(parents=True, mode=0o700)
    if directory.is_symlink() or path.is_symlink():
        raise ValueError("STATE_SYMLINK_REFUSED")
    if create and not path.exists():
        fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        os.close(fd)
    uri = path.absolute().as_uri() + ("?mode=ro" if readonly else "?mode=rw")
    db = sqlite3.connect(uri, uri=True, timeout=0, isolation_level=None)
    db.row_factory = sqlite3.Row
    try:
        app = db.execute("PRAGMA application_id").fetchone()[0]
        if not app and create and not db.execute("SELECT name FROM sqlite_master").fetchall():
            db.executescript(f"""
                PRAGMA application_id={APP_ID}; PRAGMA user_version=1;
                CREATE TABLE runs (id TEXT PRIMARY KEY, status TEXT NOT NULL, expected TEXT NOT NULL,
                    started REAL NOT NULL, updated REAL NOT NULL, mono REAL NOT NULL,
                    samples INTEGER NOT NULL, last TEXT NOT NULL, reasons TEXT NOT NULL, observed REAL NOT NULL);
                CREATE UNIQUE INDEX one_active ON runs(status) WHERE status='RUNNING';
                CREATE TABLE samples (run_id TEXT NOT NULL, seq INTEGER NOT NULL, observation TEXT NOT NULL,
                    PRIMARY KEY (run_id,seq));
            """)
        elif app != APP_ID or db.execute("PRAGMA user_version").fetchone()[0] != 1:
            raise ValueError("STATE_DATABASE_SCHEMA_INVALID")
        if not readonly:
            db.execute("PRAGMA synchronous=FULL")
            db.execute("PRAGMA max_page_count=16384")  # 64 MiB; no automatic pruning/refund.
        return db
    except BaseException:
        db.close()
        raise


def report(row, now=None, baseline=None):
    last = json.loads(row["last"])
    baseline = baseline or last
    elapsed = row["observed"]
    reasons = json.loads(row["reasons"])
    verdict = row["status"]
    if verdict == "RUNNING" and now is not None:
        clock = last["clock"]
        if (now["boot"] != clock["boot"] or now["machine"] != clock["machine"]
                or not 0 <= now["mono"] - clock["mono"] <= MAX_GAP):
            verdict = "INCONCLUSIVE"
            reasons = [*reasons, "OBSERVER_STALE_OR_HOST_CHANGED"]
    return {
        "schema": "stream_v3.nvidia_smoke.v1", "run_id": row["id"], "status": row["status"],
        "verdict": verdict, "expected_driver": row["expected"], "duration_sec": DURATION,
        "observed_elapsed_sec": elapsed, "remaining_sec": max(0, DURATION - elapsed),
        "samples": row["samples"], "started_at_jst": jst(row["started"]),
        "last_observed_at_jst": jst(row["updated"]), "reasons": reasons,
        "policy": last.get("policy", baseline.get("policy", "strict_transport_v1")),
        "transport_observation": last.get("transport_observation"),
        "runtime_observation": last.get("runtime_observation"),
        "source_lineage": last.get("source_lineage", []),
        "current_target": {"runtime": last.get("runtime_observation", {}).get("accepted_binding", last.get("runtime", {}).get("binding")),
                           "source_sha256": last.get("source")},
        "evidence_scope": "local_gpu_render_nvenc_transport",
        "provider_receiver_verified": False,
        "target": {"boot_id": baseline["clock"]["boot"], "machine": baseline["clock"]["machine"],
                   "gpu": baseline.get("gpu"), "runtime": baseline.get("runtime", {}).get("binding"),
                   "source_sha256": baseline.get("source"), "packages": baseline.get("packages")},
    }


def record(db, row, observation, status, reasons):
    sequence = row["samples"] + 1
    db.execute("INSERT INTO samples VALUES (?,?,?)", (row["id"], sequence, packed(observation)))
    observed = row["observed"]
    if status in {"RUNNING", "PASSED"}:
        observed += observation["clock"]["mono"] - json.loads(row["last"])["clock"]["mono"]
    db.execute("UPDATE runs SET status=?,updated=?,samples=?,last=?,reasons=?,observed=? WHERE id=?",
               (status, observation["clock"]["wall"], sequence, packed(observation), packed(reasons), observed, row["id"]))


def execute(args):
    if args.command == "check":
        observation = collect(None)
        failures, unknowns = assess(observation, args.expected_driver)
        verdict = "FAILED" if failures else "INCONCLUSIVE" if unknowns else "PRECHECK_PASSED"
        return {"status": verdict, "verdict": verdict, "reasons": failures + unknowns,
                "expected_driver": args.expected_driver, "checked_at_jst": jst(observation["clock"]["wall"]),
                "campaign_started": False, "provider_receiver_verified": False}
    if args.command == "tick" and not (args.state_dir / "smoke.sqlite3").exists():
        return {"status": "IDLE"}
    with closing(database(args.state_dir, create=args.command == "start", readonly=args.command == "status")) as db:
        if args.command == "status":
            row = db.execute("SELECT * FROM runs WHERE id=?", (args.run_id,)).fetchone() if args.run_id else db.execute(
                "SELECT * FROM runs ORDER BY rowid DESC LIMIT 1").fetchone()
            if row is None:
                raise ValueError("RUN_NOT_FOUND")
            baseline = json.loads(db.execute("SELECT observation FROM samples WHERE run_id=? AND seq=1",
                                             (row["id"],)).fetchone()[0])
            return report(row, clocks(), baseline)
        db.execute("BEGIN IMMEDIATE")
        try:
            active = db.execute("SELECT * FROM runs WHERE status='RUNNING'").fetchone()
            if args.command == "upgrade-observer":
                if (active is None or active["id"] != args.run_id
                        or json.loads(active["last"]).get("source") != args.from_source
                        or json.loads(active["last"]).get("policy") != "driver_with_transport_recovery_v2"):
                    raise ValueError("OBSERVER_UPGRADE_PRECONDITION_FAILED")
                previous = json.loads(active["last"])
                now = clocks()
                gap = now["mono"] - previous["clock"]["mono"]
                if (not 0 <= gap <= MAX_GAP or any(now[k] != previous["clock"][k] for k in ("boot", "machine"))
                        or abs((now["wall"] - previous["clock"]["wall"]) - gap) > 5):
                    raise ValueError("OBSERVER_UPGRADE_WINDOW_INVALID")
                observation = collect(previous)
                if (observation.get("runtime", {}).get("binding") != previous["runtime"]["binding"]
                        or observation.get("runtime", {}).get("ffmpeg_pid") != previous["runtime"]["ffmpeg_pid"]
                        or observation.get("runtime", {}).get("asset_revision") != previous["runtime"]["asset_revision"]
                        or observation.get("transport", {}).get("ffmpeg_generation") != previous["transport"]["ffmpeg_generation"]
                        or observation.get("source") != args.to_source or args.to_source != source_digest()):
                    raise ValueError("OBSERVER_UPGRADE_TARGET_OR_SOURCE_CHANGED")
                failures, unknowns = assess(observation, active["expected"], previous, observer_upgrade=True)
                if (failures or unknowns or observation["runtime_observation"]["state"] != "STABLE"
                        or observation["transport_observation"]["state"] != "HEALTHY"
                        or observation["clock"]["mono"] - previous["clock"]["mono"] > MAX_GAP
                        or observation["clock"]["mono"] - active["mono"] >= DURATION):
                    raise ValueError("OBSERVER_UPGRADE_REQUIRES_HEALTHY_UNEXPIRED_TARGET")
                observation["observer_upgrade"] = {"from_source": args.from_source, "to_source": args.to_source,
                    "from_policy": previous["policy"], "to_policy": POLICY}
                observation["source_lineage"] = [observation["observer_upgrade"]]
                observation["runtime_observation"]["validated_runtime_sec"] = active["observed"] + observation["clock"]["mono"] - previous["clock"]["mono"]
                record(db, active, observation, "RUNNING", [])
                run_id = active["id"]
            elif args.command == "start":
                if active or db.execute("SELECT 1 FROM runs WHERE id=?", (args.run_id,)).fetchone():
                    raise ValueError("RUN_ALREADY_EXISTS_OR_ACTIVE")
                observation = collect(None)
                failures, unknowns = assess(observation, args.expected_driver)
                status = "FAILED" if failures else "INCONCLUSIVE" if unknowns else "RUNNING"
                clock = observation["clock"]
                db.execute("INSERT INTO runs VALUES (?,?,?,?,?,?,?,?,?,?)", (
                    args.run_id, status, args.expected_driver, clock["wall"], clock["wall"], clock["mono"],
                    1, packed(observation), packed(failures + unknowns), 0))
                db.execute("INSERT INTO samples VALUES (?,?,?)", (args.run_id, 1, packed(observation)))
                run_id = args.run_id
            elif args.command == "stop":
                if not active or active["id"] != args.run_id:
                    raise ValueError("ACTIVE_RUN_NOT_FOUND")
                db.execute("UPDATE runs SET status='STOPPED',reasons=? WHERE id=?",
                           (packed(["OPERATOR_STOPPED"]), args.run_id))
                run_id = args.run_id
            elif active is None:
                db.execute("COMMIT")
                return {"status": "IDLE"}
            else:
                run_id = active["id"]
                previous = json.loads(active["last"])
                now = clocks()
                old_clock = previous["clock"]
                gap = now["mono"] - old_clock["mono"]
                reasons = []
                if now["boot"] != old_clock["boot"] or now["machine"] != old_clock["machine"]:
                    reasons.append("HOST_OR_BOOT_CHANGED")
                if not 0 <= gap <= MAX_GAP:
                    reasons.append("OBSERVATION_GAP")
                if abs((now["wall"] - old_clock["wall"]) - gap) > 5:
                    reasons.append("WALL_CLOCK_JUMP")
                if reasons:
                    record(db, active, {"clock": now, "unknowns": reasons}, "INCONCLUSIVE", reasons)
                elif now["mono"] - previous.get("collection_started_mono", old_clock["mono"]) >= INTERVAL:
                    observation = collect(previous)
                    failures, unknowns = assess(observation, active["expected"], previous)
                    elapsed = observation["clock"]["mono"] - active["mono"]
                    if observation["clock"]["mono"] - old_clock["mono"] > MAX_GAP:
                        unknowns.append("COLLECTION_GAP")
                    if elapsed >= DURATION and observation.get("transport_observation", {}).get("state") == "RECOVERING":
                        unknowns.append("TRANSPORT_UNRESOLVED_AT_DEADLINE")
                    if elapsed >= DURATION and observation.get("runtime_observation", {}).get("state") != "STABLE":
                        unknowns.append("RUNTIME_UNRESOLVED_AT_DEADLINE")
                    status = "FAILED" if failures else "INCONCLUSIVE" if unknowns else (
                        "PASSED" if elapsed >= DURATION else "RUNNING")
                    record(db, active, observation, status, failures + unknowns)
            row = db.execute("SELECT * FROM runs WHERE id=?", (run_id,)).fetchone()
            baseline = json.loads(db.execute("SELECT observation FROM samples WHERE run_id=? AND seq=1",
                                             (run_id,)).fetchone()[0])
            db.execute("COMMIT")
            return report(row, baseline=baseline)
        except BaseException:
            db.execute("ROLLBACK")
            raise


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-dir", type=Path, default=DEFAULT_STATE)
    sub = parser.add_subparsers(dest="command", required=True)
    start = sub.add_parser("start")
    start.add_argument("--run-id", required=True)
    start.add_argument("--expected-driver", required=True)
    sub.add_parser("check").add_argument("--expected-driver", required=True)
    sub.add_parser("tick")
    status = sub.add_parser("status")
    status.add_argument("--run-id")
    sub.add_parser("stop").add_argument("--run-id", required=True)
    upgrade = sub.add_parser("upgrade-observer")
    upgrade.add_argument("--run-id", required=True)
    upgrade.add_argument("--from-source", required=True)
    upgrade.add_argument("--to-source", required=True)
    args = parser.parse_args(argv)
    if args.command == "upgrade-observer" and any(not re.fullmatch("[a-f0-9]{64}", v) for v in (args.from_source, args.to_source)):
        parser.error("observer upgrade requires exact source digests")
    if getattr(args, "run_id", None) and not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,79}", args.run_id):
        parser.error("run-id must be 1-80 safe identifier characters")
    if args.command in {"start", "check"} and not re.fullmatch(r"580\.\d+(?:\.\d+)?", args.expected_driver):
        parser.error("expected-driver must name an exact R580 driver version")
    try:
        result = execute(args)
    except (OSError, ValueError, sqlite3.Error) as exc:
        # Never expose raw command stderr, Kubernetes objects, credentials, or addresses.
        reason = str(exc)
        reason = reason if re.fullmatch(r"[A-Z_]{1,80}", reason) else "STATE_OR_OBSERVATION_IO_ERROR"
        print(packed({"status": "ERROR", "error": type(exc).__name__, "reason": reason}))
        return 2
    print(packed(result))
    return 1 if result.get("verdict") in {"FAILED", "INCONCLUSIVE", "STOPPED"} else 0


if __name__ == "__main__":
    raise SystemExit(main())
