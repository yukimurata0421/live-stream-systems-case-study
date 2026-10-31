#!/usr/bin/env python3
"""Check Airspy startup reception; allow one extra restart per origin startup."""
from __future__ import annotations

import argparse
from contextlib import contextmanager
from dataclasses import asdict, dataclass
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import re
import statistics
import subprocess
import tempfile
import time

UNIT = "airspy_adsb.service"
SYSTEMCTL = "/usr/bin/systemctl"
CONFIG_FILE = Path("/etc/default/airspy_adsb")
BINARY = "/usr/local/bin/airspy_adsb"
SCHEMA = 1


def number(value):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise ValueError("invalid numeric observation")
    return float(value)


def read_json(path):
    if path.stat().st_size > 4 * 1024 * 1024:
        raise ValueError("observation/state exceeds size limit")
    value = json.loads(path.read_text())
    if not isinstance(value, dict):
        raise ValueError("JSON object required")
    return value


def atomic_json(path, value, mode=0o600):
    atomic_bytes(path, (json.dumps(value, sort_keys=True, allow_nan=False) + "\n").encode(), mode)


def atomic_bytes(path, data, mode):
    fd, name = tempfile.mkstemp(prefix="." + path.name, dir=path.parent)
    try:
        os.fchmod(fd, mode)
        with os.fdopen(fd, "wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        os.replace(name, path)
        directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if os.path.exists(name):
            os.unlink(name)


@dataclass(frozen=True)
class Policy:
    rate_ratio: float = 0.10
    baseline_sec: float = 300
    grace_sec: float = 90
    sustain_sec: float = 180
    startup_window_sec: float = 900
    sample_max_age_sec: float = 90
    readsb_max_age_sec: float = 30
    min_samples: int = 3
    baseline_min_samples: int = 3
    baseline_min_positions: int = 2
    cooldown_sec: float = 300

    @classmethod
    def from_env(cls):
        values = {}
        for field in cls.__dataclass_fields__:
            raw = os.environ.get("AIRSPY_RX_" + field.upper())
            if raw is not None:
                values[field] = float(raw)
        p = cls(**values)
        for key, value in asdict(p).items():
            number(value)
            if value <= 0:
                raise ValueError("policy values must be positive: " + key)
        if not 0 < p.rate_ratio < 1:
            raise ValueError("AIRSPY_RX_RATE_RATIO must be between 0 and 1")
        for name in ("min_samples", "baseline_min_samples", "baseline_min_positions"):
            if getattr(p, name) != int(getattr(p, name)):
                raise ValueError(name + " must be an integer")
        if p.min_samples < 2 or p.baseline_min_samples < 2:
            raise ValueError("at least two distinct source snapshots required")
        if p.startup_window_sec <= p.grace_sec + p.sustain_sec:
            raise ValueError("startup window must exceed grace plus sustain")
        return p


class LiveBackend:
    def service(self):
        names = ["ActiveState", "SubState", "MainPID", "InvocationID", "ExecMainStartTimestampMonotonic"]
        r = subprocess.run([SYSTEMCTL, "show", UNIT] + ["-p" + n for n in names],
                           capture_output=True, text=True, timeout=5, check=True)
        return dict(line.split("=", 1) for line in r.stdout.splitlines() if "=" in line)

    def collect(self, policy):
        before = self.service()
        if before.get("ActiveState") != "active" or before.get("SubState") != "running":
            raise ValueError("Airspy service is not running")
        pid = int(before["MainPID"])
        proc = Path("/proc") / str(pid)
        if str((proc / "exe").resolve()) != BINARY:
            raise ValueError("unexpected Airspy executable")
        args = (proc / "cmdline").read_bytes().split(b"\0")
        cfg = CONFIG_FILE.read_bytes()
        gains = re.findall(rb'^\s*GAIN\s*=\s*"?([^"\n#]+)', cfg, re.M)
        if len(gains) != 1:
            raise ValueError("exactly one configured gain required")
        expected_gain = float(gains[0].strip())
        if not math.isfinite(expected_gain) or not 0 <= expected_gain <= 21:
            raise ValueError("configured gain out of range")
        a = read_json(Path("/run/airspy_adsb/stats.json"))
        r = read_json(Path("/run/readsb/stats.json"))
        aircraft = read_json(Path("/run/readsb/aircraft.json"))
        now, mono = time.time(), time.monotonic()
        boot = Path("/proc/sys/kernel/random/boot_id").read_text().strip()
        if self.service() != before:
            raise ValueError("Airspy identity changed while sampling")
        started_mono = int(before["ExecMainStartTimestampMonotonic"]) / 1e6
        if not before.get("InvocationID") or not 0 < started_mono <= mono:
            raise ValueError("Airspy invocation identity unavailable")
        source_now = number(a.get("now"))
        readsb_now = number(r.get("now"))
        aircraft_now = number(aircraft.get("now"))
        for ts, limit in ((source_now, policy.sample_max_age_sec),
                          (readsb_now, policy.readsb_max_age_sec),
                          (aircraft_now, policy.readsb_max_age_sec)):
            if not 0 <= now - ts <= limit:
                raise ValueError("source snapshot stale or future")
        gain = number(a.get("gain"))
        if gain != expected_gain:
            raise ValueError("reported gain does not match configuration")
        counts = a.get("df_counts")
        if not isinstance(counts, list) or len(counts) != 32:
            raise ValueError("invalid DF counters")
        counts = [number(n) for n in counts]
        if any(n < 0 or n > 1e12 for n in counts):
            raise ValueError("DF counter out of range")
        fresh = 0
        for item in aircraft.get("aircraft", []):
            if not isinstance(item, dict):
                raise ValueError("invalid aircraft entry")
            if all(isinstance(item.get(k), (float, int)) and not isinstance(item[k], bool)
                   and math.isfinite(item[k]) for k in ("lat", "lon", "seen_pos")):
                fresh += int(0 <= item["seen_pos"] <= 30 and -90 <= item["lat"] <= 90
                             and -180 <= item["lon"] <= 180)
        total = r.get("total", {})
        lost = number(a.get("lost_buffers"))
        positions = number(total.get("position_count_total"))
        if lost < 0 or positions < 0:
            raise ValueError("negative receiver counter")
        return dict(now=now, mono=mono, boot=boot, instance=before["InvocationID"], pid=pid,
                    start_mono=started_mono, start_epoch=now - (mono - started_mono),
                    source_now=source_now, readsb_now=readsb_now, rate=sum(counts) / 60,
                    df17=counts[17], gain=gain, lost_buffers=lost, positions=positions,
                    readsb_start=number(total.get("start")), fresh_positions=fresh,
                    config_sha=hashlib.sha256(cfg).hexdigest(),
                    binary_sha=hashlib.sha256((proc / "exe").read_bytes()).hexdigest(),
                    command_sha=hashlib.sha256(b"\0".join(args)).hexdigest())

    def restart(self):
        # The action target and command cannot be changed through the environment.
        r = subprocess.run([SYSTEMCTL, "restart", UNIT], capture_output=True, timeout=15)
        return r.returncode


class Store:
    def __init__(self, directory):
        self.directory = Path(directory)
        self.path = self.directory / "state.json"
        self.status_path = self.directory / "status.json"

    @contextmanager
    def locked(self):
        with (self.directory / "lock").open("a") as f:
            fcntl.flock(f, fcntl.LOCK_EX)
            yield

    def initialize(self):
        # Initialize explicitly once; observation never recreates missing state.
        if self.path.exists() or (self.directory / "initialized").exists():
            raise ValueError("state already initialized; never reset attempts")
        atomic_json(self.directory / "initialized", {"schema": SCHEMA})
        self.save(dict(schema=SCHEMA, episode=None, history=[], last=None, last_restart_mono=None))

    def load(self):
        if not (self.directory / "initialized").is_file():
            raise ValueError("state has not been initialized")
        state = read_json(self.path)
        if state.get("schema") != SCHEMA or not isinstance(state.get("history"), list):
            raise ValueError("invalid durable state")
        e = state.get("episode")
        if e is not None and (not isinstance(e, dict) or type(e.get("attempts")) is not int
                              or e.get("attempts") not in (0, 1)):
            raise ValueError("invalid restart budget")
        return state

    def save(self, state):
        atomic_json(self.path, state)

    def event(self, event, **fields):
        row = dict(ts=time.time(), event=event, **fields)
        with (self.directory / "events.jsonl").open("a") as f:
            f.write(json.dumps(row, allow_nan=False) + "\n")
            f.flush()
            os.fsync(f.fileno())

    def status(self, state, phase, reason, sample=None, alert=False, confirmed=False):
        e = state.get("episode") or {}
        row = dict(schema=SCHEMA, ts=time.time(), state=phase, reason=reason, alert=alert,
                   reception_confirmed=confirmed, episode_id=e.get("id"),
                   recovery_attempts=e.get("attempts"), max_recovery_attempts=1,
                   policy=asdict(POLICY_CONTEXT) if POLICY_CONTEXT else None)
        if sample:
            row.update(observed_ts=sample["now"], source_now=sample["source_now"],
                       pid=sample["pid"], gain=sample["gain"], rate=sample["rate"],
                       positions=sample["fresh_positions"], lost_buffers=sample["lost_buffers"])
            baseline = e.get("baseline")
            row["rate_ratio"] = sample["rate"] / baseline["rate"] if baseline else None
        atomic_json(self.status_path, row, 0o644)
        return row


POLICY_CONTEXT = None


def baseline_for(state, sample, policy):
    rows = [r for r in state["history"] if r["boot"] == sample["boot"]
            and 0 <= sample["mono"] - r["mono"] <= policy.baseline_sec]
    if len(rows) < policy.baseline_min_samples:
        return None
    if any(r["fresh_positions"] < policy.baseline_min_positions for r in rows):
        return None
    if any(rows[i + 1]["source_now"] - rows[i]["source_now"] > policy.sample_max_age_sec
           for i in range(len(rows) - 1)):
        return None
    if len({r["readsb_start"] for r in rows}) != 1 or rows[-1]["positions"] <= rows[0]["positions"]:
        return None
    rate = statistics.median(r["rate"] for r in rows)
    if rate <= 0:
        return None
    return dict(rate=rate, samples=len(rows), readsb_start=rows[-1]["readsb_start"],
                last_positions=rows[-1]["positions"], captured_mono=sample["mono"])


def new_episode(state, sample, policy, origin="external_start"):
    baseline = baseline_for(state, sample, policy)
    e = dict(id=sample["boot"] + ":" + sample["instance"], origin=origin,
             instance=sample["instance"], origin_instance=sample["instance"],
             boot=sample["boot"], start_mono=sample["start_mono"],
             baseline=baseline, attempts=0, phase="STARTING", bad=[], good=0,
             config_sha=sample["config_sha"], binary_sha=sample["binary_sha"],
             command_sha=sample["command_sha"], last_source=None, last_post=None,
             policy=asdict(policy))
    if state.get("episode"):
        state.setdefault("closed", []).append({k:state["episode"].get(k) for k in
                                                ("id", "attempts", "phase", "origin", "restart_returncode",
                                                 "restart_error", "scheduled_restart_returncode",
                                                 "scheduled_restart_error", "scheduled_outcome")})
        state["closed"] = state["closed"][-32:]
    state["episode"] = e
    return e


def remember(state, sample, policy):
    history = state["history"]
    if not history or history[-1]["source_now"] != sample["source_now"] or history[-1]["boot"] != sample["boot"]:
        history.append(sample)
    state["history"] = [r for r in history if r["boot"] == sample["boot"]
                        and 0 <= sample["mono"] - r["mono"] <= policy.baseline_sec][-32:]
    state["last"] = sample


def observe(store, state, backend, policy, allow_effect=True):
    try:
        sample = backend.collect(policy)
    except (OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError) as exc:
        e = state.get("episode") or {}
        # Missing observations count as neither health nor sustained failure.
        if e:
            e.update(bad=[], good=0, last_post=None, last_source=None,
                     status_phase="UNKNOWN", status_confirmed=False)
        store.save(state)
        try:
            service = backend.service()
            age = time.monotonic() - int(service["ExecMainStartTimestampMonotonic"]) / 1e6
            if service.get("ActiveState") == "active" and 0 <= age < policy.grace_sec:
                return store.status(state, "STARTING", "waiting for first post-start statistics")
        except (AttributeError, OSError, ValueError, KeyError, subprocess.SubprocessError):
            pass
        return store.status(state, "UNKNOWN", type(exc).__name__ + ": " + str(exc), alert=True)
    e = state.get("episode")
    if e and sample["boot"] != e["boot"]:
        e = new_episode(state, sample, policy)
    elif e and e.get("waiting_origin"):
        if sample["instance"] == e["previous_instance"]:
            return store.status(state, "UNKNOWN", "scheduled restart has not produced a new invocation", sample, True)
        e.update(instance=sample["instance"], origin_instance=sample["instance"],
                 start_mono=sample["start_mono"], waiting_origin=False,
                 command_sha=sample["command_sha"], phase="STARTING",
                 scheduled_outcome="new invocation and requested gain observed")
        store.event("scheduled_restart_observed", episode_id=e["id"], instance=sample["instance"])
    elif e and sample["instance"] != e["instance"] and e["attempts"] == 1 and e["phase"] != "HEALTHY":
        # Bind retry children and unknown crash outcomes to the origin; never rearm.
        e.update(instance=sample["instance"], last_source=None, last_post=None,
                 bad=[], good=0, retry_start_mono=sample["start_mono"], phase="VERIFYING")
    elif not e or sample["instance"] != e["instance"] or sample["boot"] != e["boot"]:
        e = new_episode(state, sample, policy)
        store.event("startup_observed", episode_id=e["id"], baseline=e["baseline"])
    for key in ("config_sha", "binary_sha", "command_sha"):
        if sample[key] != e[key]:
            e["bad"] = []
            store.save(state)
            return store.status(state, "UNKNOWN", "receiver binding drift: " + key, sample, True)
    if e["policy"] != asdict(policy):
        e.update(policy=asdict(policy), bad=[])
    elapsed = sample["mono"] - e.get("retry_start_mono", e["start_mono"])
    if sample["source_now"] < sample["start_epoch"] or sample["readsb_now"] < sample["start_epoch"]:
        store.save(state)
        return store.status(state, "STARTING", "waiting for post-start source data", sample)
    if e["last_source"] == sample["source_now"]:
        # Repeated reads of one 60-second snapshot do not advance persistence.
        store.save(state)
        return store.status(state, e.get("status_phase", "STARTING"), e.get("status_reason", "waiting for new snapshot"),
                            sample, e.get("status_alert", False),
                            e.get("status_confirmed", False) and sample["fresh_positions"] > 0)
    previous = e.get("last_post")
    if previous and (sample["source_now"] < previous["source_now"]
                     or sample["readsb_now"] < previous["readsb_now"]):
        e.update(bad=[], good=0, status_confirmed=False)
        store.save(state)
        return store.status(state, "UNKNOWN", "source clock moved backwards", sample, True)
    if previous and (sample["readsb_start"] != previous["readsb_start"] or sample["positions"] < previous["positions"]):
        e.update(bad=[], good=0, last_source=sample["source_now"], last_post=sample)
        store.save(state)
        return store.status(state, "UNKNOWN", "readsb counter identity changed", sample, True)
    gap = previous and sample["source_now"] - previous["source_now"] > policy.sample_max_age_sec
    if gap:
        e.update(bad=[], good=0)
    progressing = previous and not gap and sample["positions"] > previous["positions"]
    good = sample["fresh_positions"] > 0 and (progressing or previous is None)
    e["good"] = e["good"] + 1 if good else 0
    e.update(last_source=sample["source_now"], last_post=sample)
    phase, reason, alert, confirmed = "STARTING", "collecting startup evidence", False, False
    if e["good"] >= 2:
        e.update(phase="HEALTHY", bad=[])
        phase, reason, confirmed = "HEALTHY", "fresh positions and position counter progress confirmed", True
    elif elapsed < policy.grace_sec:
        e["bad"] = []
    elif not e["baseline"]:
        phase, reason = "QUIET_OR_UNKNOWN", "no continuous pre-start traffic baseline; automatic restart prohibited"
    elif e["phase"] == "HEALTHY" and sample["mono"] - e["start_mono"] > policy.startup_window_sec:
        # Quiet traffic after completed startup verification is not a new fault.
        e["bad"] = []
        phase, reason = "QUIET", "startup verification completed; no current position progress"
    elif sample["readsb_start"] != e["baseline"]["readsb_start"]:
        phase, reason, alert = "UNKNOWN", "baseline decoder identity changed", True
    else:
        ratio = sample["rate"] / e["baseline"]["rate"]
        low = sample["fresh_positions"] == 0 and not progressing and ratio < policy.rate_ratio
        if not low:
            e["bad"] = []
            phase, reason = "OBSERVING", "no sustained reception collapse"
        else:
            e["bad"].append(dict(source_now=sample["source_now"], mono=sample["mono"],
                                 rate=sample["rate"], ratio=ratio, positions=sample["positions"]))
            bad = e["bad"]
            sustained = (len(bad) >= policy.min_samples
                         and sample["source_now"] - bad[0]["source_now"] >= policy.sustain_sec
                         and sample["mono"] - bad[0]["mono"] >= policy.sustain_sec)
            phase, reason = "SUSPECT", "low rate with zero positions; persistence not yet met"
            if sustained:
                phase, reason, alert = "DEGRADED", "sustained post-start reception collapse", True
                last_restart = state.get("last_restart_mono")
                cooldown_ok = (last_restart is None or state.get("last_restart_boot") != sample["boot"]
                               or sample["mono"] - last_restart >= policy.cooldown_sec)
                in_window = sample["mono"] - e["start_mono"] <= policy.startup_window_sec
                if e["attempts"]:
                    phase, reason = "EXHAUSTED", "one recovery restart already consumed for this origin startup"
                elif not in_window:
                    phase, reason = "DEGRADED", "outside startup recovery window; alert only"
                elif not cooldown_ok:
                    phase, reason = "DEGRADED", "recovery cooldown active; alert only"
                elif sample["lost_buffers"] != 0:
                    phase, reason = "DEGRADED", "sample loss observed; automatic restart signature not satisfied"
                elif not allow_effect:
                    phase, reason = "WOULD_RESTART", "observation-only mode; no attempt reserved"
                else:
                    # Recheck identity, source and binding immediately before the effect.
                    check = backend.collect(policy)
                    if any(check[k] != sample[k] for k in ("boot", "instance", "config_sha", "binary_sha", "command_sha")):
                        phase, reason = "UNKNOWN", "receiver changed before restart; no effect"
                    elif (check["fresh_positions"] > 0 or check["positions"] > sample["positions"]
                          or check["rate"] / e["baseline"]["rate"] >= policy.rate_ratio):
                        phase, reason = "OBSERVING", "positions recovered before restart; no effect"
                    elif check["readsb_start"] != sample["readsb_start"] or check["lost_buffers"] != 0:
                        phase, reason = "UNKNOWN", "decoder or sample loss changed before restart; no effect"
                    else:
                        evidence = dict(baseline=e["baseline"], bad_window=list(e["bad"]),
                                        last_source=sample, policy=asdict(policy))
                        e.update(attempts=1, phase="RESTART_RESERVED", bad=[], good=0)
                        state["last_restart_mono"] = sample["mono"]
                        state["last_restart_boot"] = sample["boot"]
                        store.save(state)  # Never call systemctl before fsync completes.
                        store.event("recovery_restart_reserved", episode_id=e["id"], attempts=1,
                                    evidence=evidence)
                        try:
                            result = backend.restart()
                            e["phase"] = "VERIFYING" if result == 0 else "RESTART_FAILED"
                            e["restart_returncode"] = result
                        except (OSError, subprocess.SubprocessError) as exc:
                            e.update(phase="OUTCOME_UNKNOWN", restart_error=type(exc).__name__)
                        store.save(state)
                        store.event("recovery_restart_result", episode_id=e["id"], phase=e["phase"],
                                    returncode=e.get("restart_returncode"), error=e.get("restart_error"))
                        phase, reason = e["phase"], "recovery attempt consumed; awaiting observed reception"
    e.update(status_phase=phase, status_reason=reason, status_alert=alert, status_confirmed=confirmed)
    remember(state, sample, policy)
    store.save(state)
    return store.status(state, phase, reason, sample, alert, confirmed)




def main(argv=None, backend=None):
    global POLICY_CONTEXT
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=["init", "observe", "observe-only"])
    args = parser.parse_args(argv)
    policy = Policy.from_env()
    POLICY_CONTEXT = policy
    store = Store(os.environ.get("AIRSPY_RX_STATE_DIR", "/var/lib/airspy-reception-guard"))
    backend = backend or LiveBackend()
    with store.locked():
        if args.mode == "init":
            store.initialize()
            print(json.dumps({"initialized": True}))
            return 0
        try:
            state = store.load()
            status = observe(
                store, state, backend, policy, allow_effect=args.mode != "observe-only")
        except (OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError) as exc:
            # Fail closed on corrupt state, storage failure or bad config; never reset.
            status = store.status({}, "UNKNOWN", type(exc).__name__ + ": " + str(exc), alert=True)
        print(json.dumps(status, sort_keys=True, allow_nan=False))
        return 0 if not status["alert"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
