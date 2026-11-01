"""Observation identity and notification continuity; never grants recovery authority.

UTC is for correlation. Durations use a boot-bound CLOCK_BOOTTIME. Foreign
boottimes must be qualified by the existing clock exchange before use.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from functools import lru_cache
import hashlib
import json
import math
import os
from pathlib import Path
import secrets
import time

SCHEMA = "stream.observation_time.v1"
EPSILON = .005


def finite(value):
    return type(value) in (int, float) and math.isfinite(value)


def epoch(value):
    if finite(value):
        return float(value) if value > 0 else None
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            return None
        stamp = parsed.timestamp()
        return stamp if math.isfinite(stamp) and stamp > 0 else None
    except (ValueError, OverflowError):
        return None


@lru_cache(maxsize=1)
def local_identity():
    machine = hashlib.sha256(Path("/etc/machine-id").read_bytes()).hexdigest()
    boot = Path("/proc/sys/kernel/random/boot_id").read_text().strip()
    return os.environ.get("STREAM_OBSERVATION_HOST_ID", "machine:" + machine), boot, machine


@dataclass(frozen=True)
class Reading:
    wall: float
    mono: float
    host: str
    boot: str
    machine: str
    uncertainty: float = 0.


def capture():
    before = time.clock_gettime(time.CLOCK_BOOTTIME)
    wall = time.time()
    after = time.clock_gettime(time.CLOCK_BOOTTIME)
    host, boot, machine = local_identity()
    return Reading(wall, (before + after) / 2, host, boot, machine, (after-before)/2)


def clock_diagnostic(now, path=Path("/run/stream-clock/status.json")):
    """Read the existing NTP observer; never modifies a clock or starts NTP."""
    try:
        with Path(path).open("rb") as handle:
            raw = handle.read(16385)
        if len(raw) > 16384:
            raise ValueError("oversized_clock_status")
        value = json.loads(raw)
        if (value.get("boot_id") != now.boot or value.get("machine_id_sha256") != now.machine
                or not finite(value.get("mono")) or not 0 <= now.mono - value["mono"] <= 45
                or type(value.get("synchronized")) is not bool):
            raise ValueError("unbound_clock_status")
        return {k: value.get(k) for k in ("host_id", "mono", "synchronized", "maximum_error_seconds", "ntp",
                                          "wall_steps_total", "wall_step_seconds")}
    except (OSError, ValueError, KeyError, TypeError, AttributeError):
        return {"synchronized": None, "reason": "clock_observation_unavailable"}


def stamp(source, *, now=None):
    now = capture() if now is None else now
    if not isinstance(source, str) or not source:
        raise ValueError("observation_source_invalid")
    if not finite(now.uncertainty) or not 0 <= now.uncertainty <= EPSILON:
        raise ValueError("observation_capture_delayed")
    return {"schema": SCHEMA, "source": source, "source_host": now.host,
            "machine_id_sha256": now.machine, "boot_id": now.boot,
            "sample_id": secrets.token_hex(16),
            "observed_at_utc": datetime.fromtimestamp(now.wall, timezone.utc).isoformat(),
            "observed_boottime": now.mono, "capture_uncertainty_sec": now.uncertainty,
            "clock": clock_diagnostic(now)}


def attach(payload, source, *, now=None):
    """Only producers call this, after collecting a new sample, never on reread."""
    try:
        metadata = stamp(source, now=now)
    except (OSError, ValueError) as exc:
        # Clock instrumentation must not prevent publication of the underlying fact.
        metadata = {"schema": SCHEMA, "source": source, "reason": str(exc)}
    return {**payload, "observation_time": metadata}


@dataclass(frozen=True)
class Qualified:
    status: str
    reason: str
    source: str = ""
    host: str = ""
    boot: str = ""
    sample: str = ""
    source_mono: float = 0.
    age_upper: float | None = None
    utc_status: str = "UNAVAILABLE"

    @property
    def valid(self):
        return self.status == "VALID"


def qualify(metadata, *, now=None, max_age=180., source=None, host=None,
            clock_model=None, exchange=None):
    """Use a local boot, an authenticated exchange, or a pinned UTC estimate.

    exchange is (clock_contract module, envelope, nonce, sent, received).
    The transport, not an untrusted payload, supplies the expected clock model.
    """
    now = capture() if now is None else now
    try:
        if not finite(now.wall) or not finite(now.mono) or now.mono < 0:
            raise ValueError("observation_receiver_time_invalid")
        if not finite(now.uncertainty) or not 0 <= now.uncertainty <= EPSILON:
            raise ValueError("observation_capture_delayed")
        if (not isinstance(metadata, dict) or metadata.get("schema") != SCHEMA
                or not finite(max_age) or max_age < 0):
            raise ValueError("observation_metadata_missing_or_invalid")
        names = ("source", "source_host", "machine_id_sha256", "boot_id", "sample_id")
        if any(not isinstance(metadata.get(k), str) or not metadata[k] for k in names):
            raise ValueError("observation_identity_invalid")
        if source is not None and metadata["source"] != source:
            raise ValueError("observation_source_mismatch")
        if host is not None and metadata["source_host"] != host:
            raise ValueError("observation_host_mismatch")
        mono = metadata["observed_boottime"]
        uncertainty = metadata.get("capture_uncertainty_sec", 0.)
        if not finite(uncertainty) or not 0 <= uncertainty <= EPSILON:
            raise ValueError("observation_capture_delayed")
        wall = epoch(metadata["observed_at_utc"])
        if not finite(mono) or mono < 0 or wall is None:
            raise ValueError("observation_timestamp_invalid")
        fields = dict(source=metadata["source"], host=metadata["source_host"], boot=metadata["boot_id"],
                      sample=metadata["sample_id"], source_mono=mono)
        utc_status = "UNAVAILABLE"
        if metadata["machine_id_sha256"] == now.machine:
            if metadata["boot_id"] != now.boot or metadata["source_host"] != now.host:
                raise ValueError("observation_local_identity_changed")
            age = now.mono - mono
            if age < -EPSILON:
                raise ValueError("observation_boottime_future")
            # A UTC step is diagnostic, never an override of causal freshness.
            utc_status = "VERIFIED" if abs((now.wall - wall) - age) <= .050 else "FAULT"
        elif exchange is not None:
            clock, envelope, nonce, sent, received = exchange
            age = clock.monotonic_source_age(envelope, nonce, sent, received,
                machine=metadata["machine_id_sha256"], boot_id=metadata["boot_id"],
                source_mono=mono, max_age=max_age)
            if now.boot != clock.identity()["boot_id"]:
                raise ValueError("observation_receiver_boot_changed")
            age += now.mono - received["mono"]
            if now.mono < received["mono"]:
                raise ValueError("observation_receipt_future")
        elif clock_model is not None:
            if (clock_model.machine != metadata["machine_id_sha256"] or clock_model.boot_id != metadata["boot_id"]
                    or clock_model.local_boot != now.boot):
                raise ValueError("observation_clock_identity_mismatch")
            low, age = clock_model.age_bounds(wall, now={"wall": now.wall, "mono": now.mono})
            if not finite(low) or not finite(age) or low > age:
                raise ValueError("observation_clock_interval_invalid")
            if age < -EPSILON:
                raise ValueError("observation_utc_future")
            utc_status = "VERIFIED"
        else:
            raise ValueError("observation_remote_clock_unavailable")
        age += uncertainty + now.uncertainty
        if age > max_age:
            return Qualified("STALE", "observation_expired", **fields, age_upper=age, utc_status=utc_status)
        return Qualified("VALID", "observation_fresh", **fields, age_upper=max(0., age), utc_status=utc_status)
    except (ValueError, TypeError, KeyError, AttributeError, OverflowError) as exc:
        return Qualified("UNVERIFIABLE", str(exc))


def load_clock_model(mesh, host, *, now=None):
    """Reconstruct only a locally bound estimate from the read-only clock mesh."""
    now = capture() if now is None else now
    try:
        try:
            from . import clock_contract as clock
        except ImportError:
            import clock_contract as clock
        if (mesh.get("schema") != "stream.clock_mesh.v1" or mesh.get("boot_id") != now.boot
                or mesh.get("clock_synchronized") is not True
                or not finite(mesh.get("mono")) or not 0 <= now.mono - mesh["mono"] <= 90):
            return None
        peer = mesh.get("peers", {}).get(host, {})
        value = peer["estimate"]
        if (peer.get("status") not in {"OBSERVED", "CACHED"} or peer.get("synchronized") is not True
                or value["local_boot"] != now.boot):
            return None
        model = clock.Estimate(**value)
        if (not all(finite(getattr(model, key)) for key in
                    ("low", "high", "received_wall", "received_mono", "network_rtt"))
                or model.low > model.high or model.received_mono < 0 or model.network_rtt < 0):
            return None
        if model.boot_id != peer["boot_id"]:
            return None
        model.bounds(now={"wall": now.wall, "mono": now.mono}, local_identity={"boot_id": now.boot})
        return model
    except (ValueError, KeyError, TypeError, AttributeError):
        return None


def upstream_clock_model(payload, *, now=None, path=None):
    host = payload.get("upstream_clock_host")
    if not host:
        return None
    try:
        path = Path(path or os.environ.get("STREAM_CLOCK_MESH_FILE", "/run/stream-clock-mesh/current.json"))
        with path.open("rb") as handle:
            raw = handle.read(65537)
        if len(raw) > 65536:
            return None
        return load_clock_model(json.loads(raw), host, now=now)
    except (OSError, ValueError, TypeError):
        return None


def source_lag_bounds_at_collection(payload, *, now=None, model=None):
    """Conservative source lag at collection, separate from snapshot retention."""
    now = capture() if now is None else now
    if not finite(now.uncertainty) or not 0 <= now.uncertainty <= EPSILON:
        return None
    metadata = payload.get("observation_time", {})
    if not isinstance(metadata, dict):
        return None
    collected = epoch(metadata.get("observed_at_utc"))
    source = epoch(payload.get("source_now"))
    if collected is None or source is None:
        return None
    upstream = payload.get("upstream_clock_host")
    if model is None and not payload.get("upstream_clock_local"):
        if not isinstance(upstream, str) or not upstream:
            return None
        local = clock_diagnostic(now)
        if upstream == local.get("host_id") and local.get("synchronized") is True:
            step = local.get("wall_step_seconds")
            if finite(step) and abs(step) > .050:
                return None
        else:
            model = upstream_clock_model(payload, now=now)
            if model is None:
                return None
    low = high = collected - source
    if model is not None:
        try:
            if model.local_boot != now.boot:
                return None
            offset_low, offset_high = model.bounds(now={"wall": now.wall, "mono": now.mono})
            if not finite(offset_low) or not finite(offset_high) or offset_low > offset_high:
                return None
            low += offset_low
            high += offset_high
        except (ValueError, TypeError, AttributeError):
            return None
    return (max(0., low), max(0., high)) if high >= -EPSILON else None


def source_lag_at_collection(payload, *, now=None, model=None):
    bounds = source_lag_bounds_at_collection(payload, now=now, model=model)
    return bounds[1] if bounds is not None else None


def advance(state, sample, verdict, *, now, bad_hold=0., good_hold=45., max_gap=180., lost_after=300.):
    """Persist boot-bound continuity. Unknown/duplicates cannot vote for health.

    Returns a new state plus BAD/GOOD/UNKNOWN and a separate observation-loss
    flag. Only two distinct, ordered samples may finish a recovery.
    """
    prior = dict(state) if isinstance(state, dict) else {}
    if prior.get("receiver_boot") != now.boot:
        prior = {"receiver_boot": now.boot, "last_new": now.mono}
    result = dict(prior)
    result["receiver_boot"] = now.boot
    if not finite(prior.get("last_new")) or prior["last_new"] > now.mono:
        result = {"receiver_boot": now.boot, "last_new": now.mono}
        prior = dict(result)
    if (prior.get("source_identity") is not None and not finite(prior.get("source_mono"))
            or "since" in prior and (not finite(prior["since"]) or not 0 <= prior["since"] <= now.mono)
            or "source_since" in prior and not finite(prior["source_since"])
            or "count" in prior and (type(prior["count"]) is not int or not 0 <= prior["count"] <= 1000000)):
        for key in ("source_identity", "source_mono", "since", "source_since", "count", "verdict", "last_sample"):
            result.pop(key, None)
        prior = dict(result)
    identity = [sample.host, sample.boot, sample.source]
    fresh = sample.valid and verdict in ("BAD", "GOOD")
    previous_identity = prior.get("source_identity")
    changed = previous_identity is not None and previous_identity != identity
    if changed:
        for key in ("since", "source_since", "count", "verdict", "last_sample", "source_mono"):
            result.pop(key, None)
        prior = dict(result)
    new = (fresh and (previous_identity != identity or
           sample.sample != prior.get("last_sample") and sample.source_mono > prior.get("source_mono", -1)))
    regressed = (fresh and previous_identity == identity and finite(prior.get("source_mono"))
                 and sample.source_mono < prior["source_mono"])
    if new:
        contiguous = (prior.get("verdict") == verdict and finite(prior.get("last_new"))
                      and 0 <= now.mono - prior["last_new"] <= max_gap and not changed)
        result.update(source_identity=identity, last_sample=sample.sample, source_mono=sample.source_mono,
                      last_new=now.mono, verdict=verdict,
                      since=prior.get("since", now.mono) if contiguous else now.mono,
                      source_since=prior.get("source_since", sample.source_mono) if contiguous else sample.source_mono,
                      count=int(prior.get("count", 0)) + 1 if contiguous else 1)
    elif not fresh or regressed:
        # No continuity through a missing/invalid sample, even if UTC recovers.
        result.pop("since", None)
        result["count"] = 0
        result["verdict"] = "UNKNOWN"
    elapsed = min(now.mono - result.get("since", now.mono),
                  sample.source_mono - result.get("source_since", sample.source_mono)) if fresh else 0.
    ready = "UNKNOWN"
    if fresh and new and result.get("verdict") == verdict and elapsed >= (bad_hold if verdict == "BAD" else good_hold):
        if (verdict == "BAD" and bad_hold == 0) or result.get("count", 0) >= 2:
            ready = verdict
    lost = now.mono - result["last_new"] >= lost_after
    return result, ready, lost
