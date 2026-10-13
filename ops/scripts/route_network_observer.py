#!/usr/bin/env python3
"""Route-specific measurements and connectivity facts; no recovery authority."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import signal
import sqlite3
import statistics
import subprocess
import threading
import time
import tempfile
import re
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

from oci_relay_readonly_snapshot import read_json

SCHEMA = "stream.route_network_observation.v1"
RELAY_SSH_ALIAS = "relay-host"
PUBLIC_PINGS = {"cloudflare": "1.1.1.1", "google": "8.8.8.8", "quad9": "9.9.9.9"}
RUNTIME = Path("/run/stream-control/runtime-observation.json")
TRANSPORT = Path("/var/lib/stream-control/controller/runtime/ffmpeg_transport_latest.json")
ANCHORS = Path("/var/lib/stream-network-observer/persistent_tcp_anchor_observer_latest.json")
WAN = ANCHORS.with_name("wan_address_observer_latest.json")
MAINTENANCE = Path("/var/lib/stream-control/projection/maintenance-snapshot.json")


def timestamp(value: str) -> float:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("TIMESTAMP_WITHOUT_ZONE")
    return parsed.timestamp()


def fresh(value: str, now: float, age: float = 45) -> bool:
    try:
        return -2 <= now - timestamp(value) <= age
    except (ValueError, TypeError, AttributeError):
        return False


def digest(value: object) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def atomic(path: Path, value: dict) -> None:
    temporary = path.with_suffix(".tmp")
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as out:
        json.dump(value, out, ensure_ascii=False, allow_nan=False, separators=(",", ":"))
        out.write("\n")
        out.flush()
        os.fsync(out.fileno())
    os.replace(temporary, path)


def ping(target: str) -> dict:
    try:
        result = subprocess.run(["/usr/bin/ping", "-n", "-c", "1", "-W", "1", target],
                                capture_output=True, timeout=1.5, check=False)
        return {"ok": result.returncode == 0, "observed_at": datetime.now(timezone.utc).isoformat()}
    except (OSError, subprocess.TimeoutExpired):
        return {"ok": None, "observed_at": datetime.now(timezone.utc).isoformat()}


def ping_facts() -> dict:
    targets = dict(PUBLIC_PINGS)
    try:
        route = subprocess.run(["/usr/sbin/ip", "-4", "route", "show", "default"],
                               capture_output=True, text=True, timeout=1, check=False)
        words = route.stdout.split()
        if "via" in words:
            targets["gateway"] = words[words.index("via") + 1]
    except (OSError, ValueError, IndexError, subprocess.TimeoutExpired):
        pass
    with ThreadPoolExecutor(max_workers=4) as pool:
        jobs = {name: pool.submit(ping, target) for name, target in targets.items()}
        return {name: job.result() for name, job in jobs.items()}


def remote_facts(reader: Path) -> dict:
    started = time.time()
    try:
        result = subprocess.run([
            "/usr/bin/ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=3",
            "-o", "ServerAliveInterval=2", "-o", "ServerAliveCountMax=1", "-o", "LogLevel=ERROR",
            "-o", "ControlMaster=no", "-o", "ControlPath=none", "-o", "StrictHostKeyChecking=yes",
            RELAY_SSH_ALIAS, "sudo -n /usr/bin/python3 -B -"],
            input=reader.read_bytes(), capture_output=True, timeout=5, check=False)
        if result.returncode or len(result.stdout) > 16384:
            raise ValueError("SSH_READ_FAILED")
        value = json.loads(result.stdout)
        if not isinstance(value, dict) or value.get("schema") != "stream.route_oci_facts.v1":
            raise ValueError("OCI_SCHEMA_INVALID")
        if value.get("status") == "UNKNOWN" or not fresh(value.get("observed_at"), time.time(), 5):
            raise ValueError("OCI_SOURCE_UNAVAILABLE")
        # Local receipt time is kept separately from the actual remote source time.
        return {"status": "VALID", "request_started_at": started, "received_at": time.time(), **value}
    except (OSError, ValueError, KeyError, TypeError, subprocess.TimeoutExpired):
        return {"status": "UNKNOWN", "reason": "OCI_READ_UNAVAILABLE", "request_started_at": started,
                "received_at": time.time()}


def local_facts(now: float) -> dict:
    try:
        r = read_json(RUNTIME)
        t = read_json(TRANSPORT)
        if not fresh(r.get("observed_at"), now, 30) or not fresh(t.get("ts_utc"), now, 30):
            raise ValueError("LOCAL_SOURCE_STALE")
        target = r["target_identity"]
        if (r.get("target_snapshot_status") != "VALID" or r.get("runtime_snapshot_status") != "VALID"
                or target.get("ffmpeg_pid") != t.get("ffmpeg_pid")):
            raise ValueError("LOCAL_TARGET_MISMATCH")
        m = r["tcp_metrics"]
        counter = m["bytes_acked"]
        if type(counter) is not int or counter < 0:
            raise ValueError("LOCAL_COUNTER_INVALID")
        profile = r.get("encoder_pipe_identity", {}).get("encoder_command_sha256")
        if not isinstance(profile, str) or len(profile) != 64:
            raise ValueError("ENCODER_PROFILE_UNBOUND")
        network = {k: t["network"].get(k) for k in
                   ("gateway_ok", "public_ok_count", "dns_ok", "tcp_probe_ok", "network_down")}
        return {"status": "VALID", "observed_at": r["observed_at"], "probe_observed_at": t["ts_utc"],
                "route": r["delivery_route"], "route_generation": r["delivery_route_generation"],
                "encoder_profile_sha256": profile, "target_sha256": digest(target),
                "pod_uid": r["pod_uid"], "ffmpeg_running": r.get("ffmpeg_running") is True,
                "stream_established": r.get("stream_established") is True,
                "in_flight_count": r.get("in_flight_count"), "network": network,
                "point": {"id": digest(target), "bytes": counter},
                "tcp": {k: m.get(k) for k in ("send_q", "notsent", "unacked", "lastsnd_ms", "rto_ms")}}
    except (OSError, ValueError, KeyError, TypeError):
        return {"status": "UNKNOWN", "reason": "LOCAL_SOURCE_UNAVAILABLE"}


def anchor_facts(now: float) -> dict:
    try:
        raw = read_json(ANCHORS)
        if not fresh(raw.get("ts_utc"), now, 45):
            raise ValueError("ANCHORS_STALE")
        expected = {"cloudflare_v4", "google_v4", "cloudflare_v6", "google_v6"}
        probes = raw["probes"]
        if (not isinstance(probes, list) or len(probes) != 4
                or {p.get("name") for p in probes} != expected
                or any(type(p.get("ok")) is not bool for p in probes)):
            raise ValueError("ANCHOR_SCHEMA_INVALID")
        return {"status": "VALID", "observed_at": raw["ts_utc"],
                "probes": {p["name"]: p["ok"] for p in probes}}
    except (OSError, ValueError, KeyError, TypeError):
        return {"status": "UNKNOWN", "reason": "ANCHOR_SOURCE_UNAVAILABLE"}


def network_epoch(now: float) -> dict:
    try:
        raw = read_json(WAN)
        if not fresh(raw.get("ts_utc"), now, 180):
            raise ValueError("WAN_SOURCE_STALE")
        signature = raw["signature"]
        identity = {k: signature[k] for k in ("ipv4_default_dev", "ipv4_default_gateway", "public_ipv4")}
        if not all(isinstance(v, str) and v for v in identity.values()):
            raise ValueError("WAN_EPOCH_UNBOUND")
        # Neither private nor public addresses are copied into the new evidence.
        return {"status": "VALID", "observed_at": raw["ts_utc"], "identity_sha256": digest(identity)}
    except (OSError, ValueError, KeyError, TypeError):
        return {"status": "UNKNOWN", "reason": "WAN_EPOCH_UNAVAILABLE"}


def maintenance_facts(now: float, target_sha256: str | None) -> dict:
    try:
        raw = read_json(MAINTENANCE)
        payload = raw["payload"]
        if (not fresh(raw.get("source_observed_at"), now, 30) or timestamp(raw["fresh_until"]) <= now
                or payload.get("available") is not True or raw.get("target_snapshot_status") != "VALID"
                or digest(payload["source_target_identity"]) != target_sha256):
            raise ValueError("MAINTENANCE_SOURCE_UNBOUND")
        proof = payload["proof"]
        inactive = (payload.get("maintenance_state") == "INACTIVE" and proof.get("positive_inactive_proof") is True
                    and proof.get("integrity_ok") is True and proof.get("startup_reconciled") is True
                    and proof.get("transaction_uncertain") is False
                    and all(proof.get(k) == 0 for k in ("active_fence_count", "incomplete_abort_count",
                                                      "incomplete_exit_count", "unresolved_transaction_count")))
        return {"status": "VALID", "observed_at": raw["source_observed_at"], "inactive": inactive}
    except (OSError, ValueError, KeyError, TypeError):
        return {"status": "UNKNOWN", "inactive": False}


def rate(previous: dict | None, point: dict | None, source_time: str, now: float) -> dict:
    if point is None:
        return {"status": "DISCONNECTED", "continuity": "NO_CONNECTION"}
    if not fresh(source_time, now, 30):
        return {"status": "UNKNOWN", "continuity": "SOURCE_STALE"}
    if type(point.get("bytes")) is not int or point["bytes"] < 0 or not isinstance(point.get("id"), str):
        return {"status": "UNKNOWN", "continuity": "COUNTER_INVALID"}
    current = {**point, "source_time": timestamp(source_time)}
    if previous is None or previous["id"] != current["id"]:
        return {"status": "WARMING", "continuity": "IDENTITY_CHANGED", "point": current}
    interval = current["source_time"] - previous["source_time"]
    delta = current["bytes"] - previous["bytes"]
    if not 0 < interval <= 45 or delta < 0:
        return {"status": "UNKNOWN", "continuity": "TIME_OR_COUNTER_DISCONTINUITY", "point": current}
    return {"status": "FLOWING" if delta > 0 else "STALLED", "continuity": "CONTINUOUS",
            "bytes_delta": delta, "interval_sec": round(interval, 6),
            "mbps": round(delta * 8 / interval / 1_000_000, 6), "point": current}


def assessment(sample: dict, streaks: dict) -> dict:
    """Supporting local observations only; never an incident/final recovery verdict."""
    anchors = sample["anchors"].get("probes", {})
    pings = [sample["pings"].get(name, {}).get("ok") for name in PUBLIC_PINGS]
    failed_ping = all(ok is False for ok in pings)
    v4_down = all(anchors.get(name) is False for name in ("cloudflare_v4", "google_v4"))
    any_tcp = any(value is True for value in anchors.values())
    public_reachable = any(ok is True for ok in pings) or any_tcp
    planes = sample["planes"]
    oci_progress = planes["oci_input"]["status"] == "FLOWING"
    local = sample["local"]
    network = local.get("network", {})
    corroborated = network.get("network_down") is True or network.get("tcp_probe_ok") is False
    loss = failed_ping and v4_down and corroborated and not oci_progress
    streaks["wan_loss"] = streaks.get("wan_loss", 0) + 1 if loss else 0
    if loss:
        kind = "IPV4_PATH_LOSS_SUSPECTED" if any_tcp else "LOCAL_WAN_LOSS_SUSPECTED"
        return {"kind": kind if streaks["wan_loss"] >= 3 else "LOCAL_CONNECTIVITY_LOSS_CONFIRMING",
                "confirmations": streaks["wan_loss"]}
    if failed_ping and (public_reachable or oci_progress):
        return {"kind": "ICMP_UNREACHABLE_WITH_TCP_PROGRESS", "confirmations": 0}
    if network.get("dns_ok") is False and public_reachable:
        return {"kind": "DNS_FAILURE_SUSPECTED", "confirmations": 0}
    if local.get("status") != "VALID":
        for plane in ("oci_input", "youtube", "twitch"):
            streaks[plane] = 0
        return {"kind": "LOCAL_OBSERVATION_UNAVAILABLE", "confirmations": 0}
    if sample["oci"].get("status") != "VALID":
        if local.get("route") == "FALLBACK":
            return {"kind": "DIRECT_TRANSPORT_PROGRESS_OBSERVED"
                    if planes["local_ack"]["status"] == "FLOWING" else "OBSERVATION_WARMING_OR_INCOMPLETE",
                    "confirmations": 0}
        for plane in ("oci_input", "youtube", "twitch"):
            streaks[plane] = 0
        return {"kind": "OCI_OBSERVATION_UNAVAILABLE", "confirmations": 0}
    if local.get("route") == "FALLBACK":
        return {"kind": "DIRECT_TRANSPORT_PROGRESS_OBSERVED"
                if planes["local_ack"]["status"] == "FLOWING" else "OBSERVATION_WARMING_OR_INCOMPLETE",
                "confirmations": 0}
    errors = [d["provider"] for d in sample["oci"].get("destinations", [])
              if d.get("error_present") is True
              and not (d["provider"] == "twitch" and sample["oci"].get("twitch_planned_break") is True)]
    if errors:
        return {"kind": "PROVIDER_OUTPUT_ERROR_OBSERVED", "providers": errors, "confirmations": 0}
    stalled = []
    for plane in ("oci_input", "youtube", "twitch"):
        intentional = plane == "twitch" and sample["oci"].get("twitch_planned_break") is True
        down = planes[plane]["status"] in ("STALLED", "DISCONNECTED") and not intentional
        streaks[plane] = streaks.get(plane, 0) + 1 if down else 0
        if streaks[plane] >= 3:
            stalled.append(plane)
    if "oci_input" in stalled:
        return {"kind": "OCI_INPUT_NOT_PROGRESSING", "confirmations": streaks["oci_input"]}
    if stalled and oci_progress:
        return {"kind": "PROVIDER_OUTPUT_NOT_PROGRESSING", "providers": stalled, "confirmations": 3}
    if (oci_progress and planes["youtube"]["status"] == "FLOWING"
            and (planes["twitch"]["status"] == "FLOWING" or sample["oci"].get("twitch_planned_break") is True)):
        return {"kind": "TRANSPORT_PROGRESS_OBSERVED", "confirmations": 0}
    return {"kind": "OBSERVATION_WARMING_OR_INCOMPLETE", "confirmations": 0}


def percentile(values: list[float], q: float) -> float:
    ordered = sorted(values)
    position = (len(ordered) - 1) * q
    lo = int(position)
    hi = min(lo + 1, len(ordered) - 1)
    return round(ordered[lo] + (ordered[hi] - ordered[lo]) * (position - lo), 6)


class Store:
    def __init__(self, directory: Path):
        self.directory = directory
        directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.db = sqlite3.connect(directory / "observations.sqlite3")
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS samples(ts REAL PRIMARY KEY, regime TEXT NOT NULL, payload TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS rates(ts REAL, regime TEXT, plane TEXT, mbps REAL, healthy INTEGER);
            CREATE INDEX IF NOT EXISTS rates_window ON rates(regime,plane,ts);
            CREATE TABLE IF NOT EXISTS censored(ts REAL, regime TEXT, plane TEXT, reason TEXT);
            CREATE INDEX IF NOT EXISTS censored_window ON censored(regime,plane,ts);
            CREATE TABLE IF NOT EXISTS epochs(regime TEXT PRIMARY KEY, started_at REAL, identity TEXT NOT NULL);
        """)
        self.cache: dict = {}
        self.cache_at = 0.0

    def baselines(self, regime: str, now: float) -> dict:
        if self.cache.get("regime_id") == regime and 0 <= now - self.cache_at < 60:
            return self.cache
        result = {"regime_id": regime, "evaluated_at": now, "windows": {}}
        for days in (1, 7):
            window = {}
            expected = days * 86400 / 15
            for plane in ("local_ack", "oci_input", "youtube", "twitch"):
                rows = self.db.execute("SELECT ts,mbps,healthy FROM rates WHERE regime=? AND plane=? AND ts>=? AND ts<? ORDER BY ts",
                                       (regime, plane, now - days * 86400, now)).fetchall()
                censored = [r[0] for r in self.db.execute(
                    "SELECT ts FROM censored WHERE regime=? AND plane=? AND ts>=? AND ts<? ORDER BY ts",
                    (regime, plane, now - days * 86400, now)).fetchall()]
                expected_normal = max(1, expected - len(censored))
                healthy = [value for _, value, ok in rows if ok]
                span = 0 if len(rows) < 2 else rows[-1][0] - rows[0][0]
                normal_span = 0 if len(healthy) < 2 else max(t for t, _, ok in rows if ok) - min(t for t, _, ok in rows if ok)
                continuity_times = sorted({r[0] for r in rows} | set(censored))
                longest_gap = max((b - a for a, b in zip(continuity_times, continuity_times[1:])), default=0)
                distribution = {}
                if healthy:
                    median = statistics.median(healthy)
                    distribution = {"median_mbps": round(median, 6), "p001_mbps": percentile(healthy, .001),
                                    "p01_mbps": percentile(healthy, .01), "p05_mbps": percentile(healthy, .05),
                                    "mad_mbps": round(statistics.median(abs(v - median) for v in healthy), 6)}
                window[plane] = {"sample_count": len(rows), "healthy_count": len(healthy),
                                 "censored_sample_count": len(censored), "expected_normal_samples": expected_normal,
                                 "coverage_ratio": round(min(1, len(rows) / expected_normal), 6),
                                 "healthy_span_sec": round(normal_span, 3), "max_gap_sec": round(longest_gap, 3),
                                 "ready": normal_span >= days * 86400 - 45 and len(healthy) / expected_normal >= .90 and longest_gap <= 45,
                                 "span_sec": round(span, 3), **distribution}
            result["windows"][str(days) + "d"] = window
        self.cache, self.cache_at = result, now
        return result

    def append(self, sample: dict) -> None:
        now, regime = sample["collected_at"], sample["regime_id"]
        if sample.get("regime_bound") is True:
            self.db.execute("INSERT OR IGNORE INTO epochs VALUES(?,?,?)", (regime, now, json.dumps(sample["regime"])))
        self.db.execute("INSERT INTO samples VALUES(?,?,?)", (now, regime, json.dumps(sample, separators=(",", ":"))))
        for plane, value in sample["planes"].items():
            if plane in sample.get("censored_planes", []):
                self.db.execute("INSERT INTO censored VALUES(?,?,?,?)", (now, regime, plane, "TWITCH_PLANNED_BREAK"))
            elif value.get("continuity") == "CONTINUOUS":
                self.db.execute("INSERT INTO rates VALUES(?,?,?,?,?)",
                                (now, regime, plane, value["mbps"], int(sample["training_eligible"].get(plane, False))))
        # New observer evidence has explicit 30-day retention. Legacy evidence is untouched.
        if int(now) // 3600 != getattr(self, "pruned_hour", None):
            self.db.execute("DELETE FROM samples WHERE ts<?", (now - 30 * 86400,))
            self.db.execute("DELETE FROM rates WHERE ts<?", (now - 30 * 86400,))
            self.db.execute("DELETE FROM censored WHERE ts<?", (now - 30 * 86400,))
            self.pruned_hour = int(now) // 3600
        self.db.commit()


def cycle(previous: dict, streaks: dict, store: Store, reader: Path) -> dict:
    with ThreadPoolExecutor(max_workers=2) as pool:
        pings_future = pool.submit(ping_facts)
        oci_future = pool.submit(remote_facts, reader)
        pings, oci = pings_future.result(), oci_future.result()
    now = time.time()
    local, anchors = local_facts(now), anchor_facts(now)
    uplink, maintenance = network_epoch(now), maintenance_facts(now, local.get("target_sha256"))
    return evaluate(local, oci, pings, anchors, uplink, maintenance, now, previous, streaks, store)


def evaluate(local: dict, oci: dict, pings: dict, anchors: dict, uplink: dict, maintenance: dict,
             now: float, previous: dict, streaks: dict, store: Store) -> dict:
    regime = {"route": local.get("route"), "route_generation": local.get("route_generation"),
              "encoder_profile_sha256": local.get("encoder_profile_sha256"),
              "relay_source_sha256": oci.get("relay_source_sha256") if local.get("route") == "PRIMARY" else None,
              "uplink_epoch_sha256": uplink.get("identity_sha256"),
              "collector_version": "route-network-v2"}
    # Missing observations cannot create or switch a measurement epoch.
    bound = (local.get("status") == "VALID" and uplink.get("status") == "VALID"
             and (local.get("route") != "PRIMARY" or oci.get("status") == "VALID"))
    if not bound and previous.get("regime"):
        regime = previous["regime"]
    regime_id = digest(regime)
    if previous.get("regime_id") != regime_id:
        previous.clear()
        streaks.clear()
        previous["regime_id"] = regime_id
    previous["regime"] = regime
    points = {"local_ack": (local.get("point"), local.get("observed_at"))}
    remote_id = oci.get("host_boot_id", "") + ":" + oci.get("relay_instance", "")
    point = oci.get("input")
    points["oci_input"] = (None if point is None else {**point, "id": remote_id + ":" + point["id"]}, oci.get("observed_at"))
    destinations = {d["provider"]: d for d in oci.get("destinations", [])}
    for provider in ("youtube", "twitch"):
        point = destinations.get(provider)
        points[provider] = (None if point is None else {**point, "id": remote_id + ":" + point["id"]}, oci.get("observed_at"))
    planes = {}
    for plane, (point, source_time) in points.items():
        valid = local.get("status") == "VALID" if plane == "local_ack" else (
            oci.get("status") == "VALID" and local.get("route") == "PRIMARY")
        value = rate(previous.get(plane), point, source_time, now) if valid else {"status": "UNKNOWN", "continuity": "SOURCE_UNAVAILABLE"}
        previous[plane] = value.pop("point", None)
        planes[plane] = value
    sample = {"schema": SCHEMA, "collected_at": now, "observed_at": datetime.fromtimestamp(now, timezone.utc).isoformat(),
              "authority": "FACTS_ONLY_SHADOW", "action_enabled": False, "restart_threshold_mbps": None,
              "regime_id": regime_id, "regime": regime, "regime_bound": bound, "local": local, "oci": oci,
              "pings": pings, "anchors": anchors, "uplink": uplink, "maintenance": maintenance, "planes": planes}
    sample["assessment"] = assessment(sample, streaks)
    reachable = any(p.get("ok") is True for name, p in pings.items() if name in PUBLIC_PINGS) or any(anchors.get("probes", {}).values())
    stable = (bound and local.get("status") == "VALID" and local.get("ffmpeg_running") is True
              and local.get("stream_established") is True and local.get("in_flight_count") == 0
              and oci.get("status") == "VALID" and oci.get("enabled") is True and oci.get("halted") is False
              and reachable and local.get("network", {}).get("dns_ok") is True
              and all(planes[p]["status"] == "FLOWING" for p in ("local_ack", "oci_input", "youtube")))
    if local.get("route") == "FALLBACK":
        stable = (bound and local.get("ffmpeg_running") is True and local.get("stream_established") is True
                  and local.get("in_flight_count") == 0 and reachable
                  and local.get("network", {}).get("dns_ok") is True and planes["local_ack"]["status"] == "FLOWING")
    pressure = any((local.get("tcp", {}).get(k) or 0) > 0 for k in ("send_q", "notsent"))
    stable = (stable and maintenance["status"] == "VALID" and maintenance["inactive"] is True
              and not pressure and not any(d.get("error_present") is True for d in destinations.values()))
    sample["training_eligible"] = {p: stable and planes[p]["status"] == "FLOWING"
                                   and (p != "twitch" or oci.get("twitch_planned_break") is False)
                                   for p in planes}
    sample["censored_planes"] = ["twitch"] if stable and oci.get("twitch_planned_break") is True else []
    # Score against earlier data before appending this observation.
    sample["baseline"] = store.baselines(regime_id, now)
    sample["scores"] = {}
    for plane, value in planes.items():
        baseline = sample["baseline"]["windows"]["1d"][plane]
        median = baseline.get("median_mbps")
        sample["scores"][plane] = {"baseline_ready": baseline["ready"],
                                   "ratio_to_median": round(value["mbps"] / median, 6)
                                   if baseline["ready"] and median and "mbps" in value else None,
                                   "below_p001": value["mbps"] < baseline["p001_mbps"]
                                   if baseline["ready"] and "mbps" in value else None}
    store.append(sample)
    atomic(store.directory / "latest.json", sample)
    return sample


def main() -> int:
    parser = argparse.ArgumentParser(description="Offline replay by default; live collection requires explicit configuration.")
    modes = parser.add_mutually_exclusive_group(required=True)
    modes.add_argument("--replay", type=Path, help="Synthetic normalized facts; no network, subprocess, or signal effects.")
    modes.add_argument("--live", action="store_true", help="Enable configured read-only sources and bounded probes.")
    parser.add_argument("--config", type=Path)
    parser.add_argument("--state", type=Path)
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()
    os.umask(0o077)
    if args.live:
        if args.config is None or args.state is None:
            parser.error("--live requires --config and --state")
        config = read_json(args.config)
        global RUNTIME, TRANSPORT, ANCHORS, WAN, MAINTENANCE, RELAY_SSH_ALIAS
        for key in ("runtime", "transport", "anchors", "wan", "maintenance", "relay_ssh_alias"):
            if not isinstance(config.get(key), str) or not config[key]:
                parser.error("Explicit source binding is required: " + key)
        RUNTIME, TRANSPORT, ANCHORS, WAN, MAINTENANCE = (Path(config[key]) for key in
            ("runtime", "transport", "anchors", "wan", "maintenance"))
        RELAY_SSH_ALIAS = config["relay_ssh_alias"]
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}", RELAY_SSH_ALIAS):
            parser.error("A configured SSH alias is required")
    temporary = tempfile.TemporaryDirectory(prefix="route-observation-demo-") if args.state is None else None
    directory = Path(temporary.name) if temporary else args.state
    store = Store(directory)
    previous, streaks = {}, {}
    stopped = threading.Event()
    try:
        if args.replay:
            data = read_json(args.replay, 1048576)
            if data.get("schema") != "stream.route_replay.v1" or not isinstance(data.get("frames"), list):
                raise ValueError("REPLAY_SCHEMA_INVALID")
            if not 1 <= len(data["frames"]) <= 500:
                raise ValueError("BOUNDED_REPLAY_REQUIRED")
            for frame in data["frames"]:
                sample = evaluate(*(frame[key] for key in ("local", "oci", "pings", "anchors", "uplink", "maintenance")),
                                  timestamp(frame["observed_at"]), previous, streaks, store)
                print(json.dumps({"observed_at": sample["observed_at"], "assessment": sample["assessment"],
                                  "action_enabled": False, "baseline_ready": {k: v["ready"] for k, v in
                                  sample["baseline"]["windows"]["1d"].items()}}), flush=True)
        else:
            for sig in (signal.SIGTERM, signal.SIGINT):
                signal.signal(sig, lambda *_: stopped.set())
            reader = Path(__file__).with_name("oci_relay_readonly_snapshot.py")
            while not stopped.is_set():
                started = time.monotonic()
                if sum(p.stat().st_size for p in directory.glob("observations.sqlite3*")) >= 2 * 1024**3:
                    raise RuntimeError("OBSERVER_OUTPUT_LIMIT_REACHED")
                sample = cycle(previous, streaks, store, reader)
                print(json.dumps({"observed_at": sample["observed_at"], "assessment": sample["assessment"],
                                  "action_enabled": False}), flush=True)
                if args.once: break
                stopped.wait(max(0, 15 - (time.monotonic() - started)))
    finally:
        store.db.close()
        if temporary: temporary.cleanup()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
