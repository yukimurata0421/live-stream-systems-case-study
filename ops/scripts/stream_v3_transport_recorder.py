#!/usr/bin/env python3
"""Read-only SQLite flight recorder for main FIFO transport; no delivery or recovery actions."""

from __future__ import annotations

import argparse
import errno
import fcntl
import hashlib
import json
import math
import os
import resource
import signal
import sqlite3
import stat
import threading
import time
import uuid
from collections import Counter, deque
from contextlib import closing
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path

SCHEMA = "stream_v3.transport_recorder.v1"
APP_ID = 0x53565231
MAX_INPUT = 65536
STATES = {
    "STARTING",
    "TCP_ESTABLISHED",
    "ACK_ADVANCING",
    "TCP_ACK_STALLED",
    "SOCKET_UNAVAILABLE",
    "COUNTER_REGRESSION",
}
METRICS = (
    "bytes_acked",
    "bytes_sent",
    "send_q",
    "notsent",
    "unacked",
    "lastsnd_ms",
    "rto_ms",
)
COUNTERS = (
    "connections_observed_total",
    "reconnections_observed_total",
    "fifo_recovery_reports_total",
    "fifo_recovery_failures_total",
    "fifo_queue_overflows_total",
)
FIFO_COUNTERS = COUNTERS[2:]
JOURNAL_KINDS = {
    "tcp_connection_observed",
    "transport_state_changed",
    "fifo_output_error_observed",
    "fifo_output_reopen_reported",
    "fifo_recovery_failed",
    "fifo_queue_overflow",
}


def packed(value) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def source_us(value) -> int:
    if not isinstance(value, str) or len(value) > 64:
        raise ValueError("SOURCE_TIME_INVALID")
    d = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if d.tzinfo is None:
        raise ValueError("SOURCE_TIME_NAIVE")
    return int(d.timestamp() * 1_000_000)


def integer(value) -> int:
    if type(value) is not int or not 0 <= value < 2**63:
        raise ValueError("COUNTER_INVALID")
    return value


def identifier(value, *, empty=False) -> str:
    if not isinstance(value, str) or len(value) > 160 or (not value and not empty):
        raise ValueError("IDENTITY_INVALID")
    if any(
        c not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789:._-"
        for c in value
    ):
        raise ValueError("IDENTITY_INVALID")
    return value


def bounded_read(path: Path, limit=MAX_INPUT) -> bytes:
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, "rb") as f:
        if not stat.S_ISREG(os.fstat(f.fileno()).st_mode):
            raise ValueError("INPUT_NOT_REGULAR")
        raw = f.read(limit + 1)
    if len(raw) > limit:
        raise ValueError("INPUT_TOO_LARGE")
    return raw


def read_source(path):
    try:
        return json.loads(bounded_read(path)), None
    except FileNotFoundError:
        return None, "READ_MISSING"
    except PermissionError:
        return None, "READ_PERMISSION_DENIED"
    except (json.JSONDecodeError, RecursionError):
        return None, "READ_JSON_INVALID"
    except ValueError as e:
        return None, str(e) if str(e) in {
            "INPUT_TOO_LARGE",
            "INPUT_NOT_REGULAR",
        } else "READ_INVALID"
    except OSError as e:
        return None, "READ_SYMLINK" if e.errno == errno.ELOOP else "READ_IO_ERROR"


def snapshot(value: dict) -> dict:
    if (
        not isinstance(value, dict)
        or value.get("schema") != "stream_v3.ffmpeg_transport.v1"
    ):
        raise ValueError("SOURCE_SCHEMA_INVALID")
    if (
        value.get("state") not in STATES
        or type(value.get("fifo_recovering")) is not bool
    ):
        raise ValueError("SOURCE_STATE_INVALID")
    result = {
        "source_us": source_us(value.get("observed_at")),
        "state": value["state"],
        "ffmpeg_generation": identifier(value.get("ffmpeg_generation")),
        "connection_id": identifier(value.get("connection_id"), empty=True),
        "connection_generation": integer(value.get("connection_generation")),
        "ffmpeg_pid": integer(value.get("ffmpeg_pid")),
        "fifo_recovering": value["fifo_recovering"],
        "ack_bytes_observed_total": integer(value.get("ack_bytes_observed_total")),
    }
    result.update({k: integer(value.get(k)) for k in COUNTERS})
    metrics = value.get("tcp_metrics")
    if not isinstance(metrics, dict):
        raise TypeError("TCP_METRICS_INVALID")
    result["tcp_metrics"] = {k: integer(metrics[k]) for k in METRICS} if metrics else {}
    if result["state"] in {"TCP_ESTABLISHED", "ACK_ADVANCING", "TCP_ACK_STALLED"} and (
        not metrics or not result["connection_id"] or not result["ffmpeg_pid"]
    ):
        raise ValueError("TCP_IDENTITY_UNBOUND")
    return result


def clock_facts(value, now):
    """Read optional clock observations; never start NTP probes or replace missing offsets with zero."""
    try:
        if (
            not isinstance(value, dict)
            or value.get("schema") != "stream_v3.clock_observation.v1"
            or value.get("synchronized") is not True
        ):
            raise ValueError()
        ts = source_us(value.get("observed_at"))
        offset, uncertainty = value["offset_seconds"], value["uncertainty_seconds"]
        if (
            not -1 <= now - ts / 1e6 <= 300
            or any(
                type(n) not in (int, float) or not math.isfinite(n)
                for n in (offset, uncertainty)
            )
            or uncertainty < 0
        ):
            raise ValueError()
        return {
            "status": "VALID",
            "source_us": ts,
            "offset_seconds": offset,
            "uncertainty_seconds": uncertainty,
        }
    except (ValueError, TypeError, KeyError, OverflowError):
        return {"status": "UNKNOWN"}


def lead_interval(trigger_us, reference):
    """Compare independent good-to-bad observation intervals without inferring prediction quality or cause."""
    low = source_us(reference.get("last_good_at"))
    high = source_us(reference.get("first_bad_at"))
    if high < low:
        raise ValueError("REFERENCE_INTERVAL_REVERSED")
    uncertainty = reference.get("clock_uncertainty_seconds")
    if uncertainty is None or trigger_us is None:
        return {"comparison": "UNKNOWN_CLOCK_OR_SOURCE_TIME"}
    if (
        type(uncertainty) not in (int, float)
        or not math.isfinite(uncertainty)
        or uncertainty < 0
    ):
        raise ValueError("REFERENCE_CLOCK_BOUND_INVALID")
    minimum = (low - trigger_us) / 1e6 - uncertainty
    maximum = (high - trigger_us) / 1e6 + uncertainty
    comparison = (
        "TRIGGER_PRECEDES_REFERENCE_INTERVAL"
        if minimum > 0
        else "REFERENCE_INTERVAL_PRECEDES_TRIGGER"
        if maximum < 0
        else "OVERLAPPING_INTERVALS"
    )
    return {
        "comparison": comparison,
        "lead_min_seconds": minimum,
        "lead_max_seconds": maximum,
        "clock_bound": "EXTERNALLY_PROVIDED",
        "predictive_performance": "NOT_ESTABLISHED",
    }


@dataclass(frozen=True)
class Config:
    pre_seconds: float = 120
    post_seconds: float = 180
    max_episode_seconds: float = 600
    source_max_gap: float = 3.5
    source_stale_seconds: float = 5
    queue_bytes: int = 256 * 1024
    queue_seconds: float = 3
    max_storage_bytes: int = 256 * 1024**2

    def __post_init__(self):
        for k, v in asdict(self).items():
            if (
                isinstance(v, bool)
                or not isinstance(v, (int, float))
                or not math.isfinite(v)
                or v <= 0
            ):
                raise ValueError("CONFIG_INVALID:" + k)
        if type(self.queue_bytes) is not int or type(self.max_storage_bytes) is not int:
            raise ValueError("CONFIG_INTEGER_REQUIRED")
        if (
            self.pre_seconds > 120
            or self.post_seconds > 180
            or self.max_episode_seconds > 600
            or self.max_episode_seconds < self.post_seconds
            or self.source_max_gap > 10
            or self.source_stale_seconds > 60
            or self.max_storage_bytes < 1024**2
        ):
            raise ValueError("CONFIG_OUT_OF_BOUNDS")


class StorageLimit(RuntimeError):
    pass


class Store:
    """Use one writer and short transactions; never reinitialize an unknown database or existing evidence."""

    def __init__(self, directory: Path, config: Config):
        directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        s = directory.lstat()
        if not stat.S_ISDIR(s.st_mode) or s.st_uid != os.geteuid() or s.st_mode & 0o022:
            raise ValueError("STATE_DIRECTORY_UNSAFE")
        self.directory, self.config = directory, config
        self.lock = os.open(
            directory / "writer.lock", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600
        )
        try:
            fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BaseException:
            os.close(self.lock)
            raise
        self.db = None
        try:
            path = directory / "recorder.sqlite3"
            for name in (
                "recorder.sqlite3",
                "recorder.sqlite3-wal",
                "recorder.sqlite3-shm",
                "status.json",
            ):
                p = directory / name
                if p.is_symlink() or (p.exists() and not p.is_file()):
                    raise ValueError("STATE_FILE_UNSAFE")
            if path.exists():
                with closing(
                    sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)
                ) as check:
                    if (
                        check.execute("PRAGMA application_id").fetchone()[0] != APP_ID
                        or check.execute("PRAGMA user_version").fetchone()[0] != 1
                    ):
                        raise ValueError("EXISTING_DATABASE_NOT_OWNED")
            self.check_budget()
            self.db = sqlite3.connect(path, timeout=0.25)
            os.chmod(path, 0o600)
            self.db.execute("PRAGMA journal_mode=WAL")
            self.db.execute("PRAGMA synchronous=FULL")
            self.db.execute("PRAGMA wal_autocheckpoint=64")
            # DB and WAL each get less than 1/3 of the directory budget; CLI also sets RLIMIT_FSIZE.
            page_size = self.db.execute("PRAGMA page_size").fetchone()[0]
            pages = max(16, config.max_storage_bytes // 3 // page_size)
            if self.db.execute(f"PRAGMA max_page_count={pages}").fetchone()[0] > pages:
                raise StorageLimit("EXISTING_DATABASE_EXCEEDS_LIMIT")
            self.db.executescript(f"""
                PRAGMA application_id={APP_ID}; PRAGMA user_version=1;
                CREATE TABLE IF NOT EXISTS sessions(
                    id TEXT PRIMARY KEY, started_us INTEGER, ended_us INTEGER, reason TEXT, config TEXT);
                CREATE TABLE IF NOT EXISTS episodes(
                    id INTEGER PRIMARY KEY, session TEXT, start_us INTEGER, end_us INTEGER,
                    closed_reason TEXT, prehistory_seconds REAL);
                CREATE TABLE IF NOT EXISTS samples(
                    episode INTEGER, sample_key TEXT, received_us INTEGER, source_us INTEGER,
                    monotonic_sec REAL, quality TEXT, ack_mbps REAL, send_mbps REAL, payload TEXT,
                    PRIMARY KEY(episode,sample_key));
                CREATE INDEX IF NOT EXISTS samples_time ON samples(episode,received_us);
                CREATE TABLE IF NOT EXISTS triggers(
                    id INTEGER PRIMARY KEY, episode INTEGER, received_us INTEGER,
                    source_us INTEGER, kind TEXT, details TEXT);
                CREATE TABLE IF NOT EXISTS minutes(
                    id INTEGER PRIMARY KEY, session TEXT, bucket_us INTEGER, payload TEXT);
            """)
            self.latencies = deque(maxlen=512)
        except BaseException:
            self.close()
            raise

    def bytes_used(self):
        return sum(p.stat().st_size for p in self.directory.iterdir() if p.is_file())

    def check_budget(self):
        if self.bytes_used() >= self.config.max_storage_bytes:
            raise StorageLimit("STORAGE_LIMIT_REACHED")

    def transaction(self, callback):
        self.check_budget()
        started = time.monotonic()
        try:
            with self.db:
                result = callback(self.db)
        except sqlite3.Error:
            if self.db.in_transaction:
                self.db.rollback()
            raise
        self.latencies.append(time.monotonic() - started)
        self.check_budget()
        return result

    def start_session(self, session, now_us, provenance):
        def write(db):
            db.execute(
                "UPDATE episodes SET closed_reason='RECORDER_RESTART_GAP' WHERE closed_reason IS NULL"
            )
            db.execute(
                "UPDATE sessions SET ended_us=?,reason='RECORDER_RESTART_GAP' WHERE ended_us IS NULL",
                (now_us,),
            )
            db.execute(
                "INSERT INTO sessions VALUES(?,?,NULL,NULL,?)",
                (
                    session,
                    now_us,
                    packed(
                        {"parameters": asdict(self.config), "provenance": provenance}
                    ),
                ),
            )

        self.transaction(write)

    @staticmethod
    def add_samples(db, episode, frames):
        db.executemany(
            "INSERT OR IGNORE INTO samples VALUES(?,?,?,?,?,?,?,?,?)",
            [
                (
                    episode,
                    f["sample_key"],
                    f["received_us"],
                    f.get("source_us"),
                    f["monotonic_sec"],
                    f["quality"],
                    f.get("ack_mbps"),
                    f.get("send_mbps"),
                    packed(f),
                )
                for f in frames
            ],
        )

    def status(self, value):
        # A small status file remains writable even when SQLite hits its own page/file limit.
        path = self.directory / "status.json.tmp"
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NOFOLLOW, 0o600)
        with os.fdopen(fd, "w") as f:
            f.write(packed(value) + "\n")
            f.flush()
            os.fsync(f.fileno())
        os.replace(path, self.directory / "status.json")

    def close(self):
        try:
            if self.db is not None:
                self.db.close()
                self.db = None
        finally:
            if getattr(self, "lock", None) is not None:
                os.close(self.lock)
                self.lock = None


class JournalReader:
    """Read a bounded journal tail, seed historical events without replay, and report missing input."""

    def __init__(self, path: Path | None):
        self.path, self.initialized = path, False
        self.seen = deque(maxlen=4096)
        self.status = "DISABLED" if path is None else "WARMING"

    def poll(self):
        if self.path is None:
            return []
        try:
            fd = os.open(self.path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
            with os.fdopen(fd, "rb") as f:
                if not stat.S_ISREG(os.fstat(f.fileno()).st_mode):
                    raise ValueError("JOURNAL_NOT_REGULAR")
                size = f.seek(0, os.SEEK_END)
                offset = max(0, size - 512 * 1024)
                f.seek(offset)
                raw = f.read(512 * 1024)
            if offset:
                raw = raw.partition(b"\n")[2]
            events = []
            parsed = []
            for line in raw.splitlines():
                v = json.loads(line)
                if not isinstance(v, dict) or v.get("kind") not in JOURNAL_KINDS:
                    continue
                gen = identifier(v.get("ffmpeg_generation"))
                seq = integer(v.get("sequence"))
                event_us = source_us(v.get("observed_at"))
                parsed.append((gen, seq, event_us, v["kind"]))
            # Validate the entire bounded read before advancing the cursor.
            for gen, seq, event_us, kind in parsed:
                key = (gen, seq)
                if key in self.seen:
                    continue
                self.seen.append(key)
                if self.initialized:
                    events.append(
                        {
                            "generation": gen,
                            "sequence": seq,
                            "source_us": event_us,
                            "kind": kind,
                        }
                    )
            self.initialized = True
            self.status = "BOUNDED_TAIL" if offset else "VALID"
            return events
        except (OSError, ValueError, TypeError, RecursionError):
            self.status = "UNAVAILABLE"
            return []


class Recorder:
    def __init__(self, store: Store, *, now: float, mono: float, source_path=None):
        self.store, self.config = store, store.config
        self.session = uuid.uuid4().hex
        self.store.start_session(
            self.session,
            int(now * 1e6),
            {
                "recorder_boot_id": Path("/proc/sys/kernel/random/boot_id")
                .read_text()
                .strip(),
                "source_path": str(source_path) if source_path else "UNSPECIFIED",
                "source_binding": "SCHEMA_ONLY",
                "scope": "MAIN_FIFO_ONLY",
            },
        )
        self.ring = deque(maxlen=1024)
        self.last = None
        self.last_tick = None
        self.last_source_mono = mono
        self.unavailable_since = None
        self.issue_latched = False
        self.queue_start = None
        self.queue_latched = False
        self.active = None
        self.bucket = int(now // 60) * 60
        self.counts = Counter()
        self.stats = {}
        self.covered = 0.0
        self.previous_fresh = False
        self.rate_blocked = False
        self.polls = self.unique = self.duplicates = self.gaps = 0
        self.last_status = -math.inf
        self.clock_status = "UNKNOWN"

    def _minute(self):
        return {
            "schema": SCHEMA,
            "counts": dict(self.counts),
            "fresh_observed_seconds": self.covered,
            "metrics": {
                k: {"min": v[0], "max": v[1], "mean": v[2] / v[3], "count": v[3]}
                for k, v in self.stats.items()
            },
            "delivery_verified": False,
            "fault_label": "UNASSESSED",
        }

    def _stat(self, key, value):
        if value is None:
            return
        v = self.stats.setdefault(key, [value, value, 0, 0])
        v[0], v[1], v[2], v[3] = (
            min(v[0], value),
            max(v[1], value),
            v[2] + value,
            v[3] + 1,
        )

    def step(
        self,
        value,
        *,
        now: float,
        mono: float,
        read_error=None,
        journal=(),
        journal_status="DISABLED",
        clock=None,
    ):
        now_us = int(now * 1e6)
        reasons = []
        self.polls += 1
        minute = None
        tick_gap = False
        if self.last_tick is not None:
            old_now, old_mono = self.last_tick
            elapsed = mono - old_mono
            if elapsed < 0:
                raise ValueError("COLLECTOR_MONOTONIC_REGRESSION")
            if elapsed > self.config.source_max_gap:
                reasons.append(("COLLECTOR_GAP", None, {}))
                tick_gap = True
                self.gaps += 1
            if abs((now - old_now) - elapsed) > 1:
                reasons.append(("COLLECTOR_CLOCK_JUMP", None, {}))
                tick_gap = True
            if tick_gap:
                self.rate_blocked = True
                self.queue_start, self.queue_latched = None, False
            if self.previous_fresh:
                # Count availability, not healthy delivery. Never fill an unobserved long gap.
                self.covered += min(elapsed, self.config.source_max_gap)
        self.last_tick = (now, mono)
        if int(now // 60) * 60 != self.bucket:
            minute = (self.bucket * 1_000_000, packed(self._minute()))
            self.bucket = int(now // 60) * 60
            self.counts, self.stats, self.covered = Counter(), {}, 0.0
        frame = {
            "received_us": now_us,
            "monotonic_sec": mono,
            "quality": "UNAVAILABLE",
            "sample_key": self.session + ":" + str(self.polls),
            "source_us": None,
            "ack_mbps": None,
            "send_mbps": None,
            "reason": read_error or "SOURCE_UNAVAILABLE",
        }
        frame["clock"] = clock_facts(clock, now)
        self.clock_status = frame["clock"]["status"]
        current = None
        try:
            if read_error:
                raise ValueError(read_error)
            current = snapshot(value)
            frame["source_us"] = current["source_us"]
            frame["source"] = current
            age = (now_us - current["source_us"]) / 1e6
            if age < -1 or age > self.config.source_stale_seconds:
                raise ValueError("SOURCE_TIME_STALE_OR_FUTURE")
            same = self.last is not None and (
                self.last["ffmpeg_generation"],
                self.last["connection_id"],
                self.last["connection_generation"],
            ) == (
                current["ffmpeg_generation"],
                current["connection_id"],
                current["connection_generation"],
            )
            duplicate = same and self.last["source_us"] == current["source_us"]
            if duplicate and self.last != current:
                raise ValueError("SOURCE_SAMPLE_CONFLICT")
            if (
                duplicate
                and mono - self.last_source_mono >= self.config.source_stale_seconds
            ):
                raise ValueError("SOURCE_NOT_ADVANCING")
            if duplicate:
                self.duplicates += 1
                frame["quality"], frame["reason"] = "DUPLICATE", "SOURCE_DUPLICATE"
            else:
                self.unique += 1
                frame["quality"], frame["reason"] = "FRESH", "SOURCE_VALID"
                frame["sample_key"] = hashlib.sha256(
                    packed(current).encode()
                ).hexdigest()
                interval = (
                    (current["source_us"] - self.last["source_us"]) / 1e6
                    if same
                    else None
                )
                continuous = (
                    same
                    and not self.rate_blocked
                    and self.unavailable_since is None
                    and self.last["state"] != "COUNTER_REGRESSION"
                    and current["state"] != "COUNTER_REGRESSION"
                    and interval is not None
                    and 0 < interval <= self.config.source_max_gap
                )
                if same and interval is not None and interval <= 0:
                    reasons.append(("SOURCE_TIME_REGRESSION", current["source_us"], {}))
                elif (
                    same
                    and interval is not None
                    and interval > self.config.source_max_gap
                ):
                    reasons.append(
                        (
                            "SOURCE_GAP",
                            current["source_us"],
                            {"interval_seconds": interval},
                        )
                    )
                    self.gaps += 1
                if continuous and self.last["tcp_metrics"] and current["tcp_metrics"]:
                    deltas = {
                        k: current["tcp_metrics"][k] - self.last["tcp_metrics"][k]
                        for k in ("bytes_acked", "bytes_sent")
                    }
                    if min(deltas.values()) < 0:
                        reasons.append(("COUNTER_REGRESSION", current["source_us"], {}))
                        continuous = False
                    else:
                        frame["ack_mbps"] = deltas["bytes_acked"] * 8 / interval / 1e6
                        frame["send_mbps"] = deltas["bytes_sent"] * 8 / interval / 1e6
                if self.last and not same:
                    kind = (
                        "PROCESS_GENERATION_CHANGED"
                        if self.last["ffmpeg_generation"]
                        != current["ffmpeg_generation"]
                        else "CONNECTION_CHANGED"
                    )
                    reasons.append((kind, current["source_us"], {}))
                if current["state"] not in {
                    "STARTING",
                    "ACK_ADVANCING",
                    "TCP_ESTABLISHED",
                } and (
                    self.last is None
                    or self.last["state"] != current["state"]
                    or not same
                ):
                    reasons.append(
                        ("STATE_" + current["state"], current["source_us"], {})
                    )
                if current["fifo_recovering"] and (
                    self.last is None or not self.last["fifo_recovering"]
                ):
                    reasons.append(("FIFO_RECOVERY_STARTED", current["source_us"], {}))
                if (
                    self.last
                    and self.last["ffmpeg_generation"] == current["ffmpeg_generation"]
                ):
                    for key in FIFO_COUNTERS:
                        if current[key] != self.last[key]:
                            reasons.append(
                                (
                                    "FIFO_COUNTER_CHANGED",
                                    current["source_us"],
                                    {
                                        "counter": key,
                                        "delta": current[key] - self.last[key],
                                    },
                                )
                            )
                high = (
                    current["tcp_metrics"].get("notsent", 0) >= self.config.queue_bytes
                )
                if high:
                    if not continuous or self.queue_start is None:
                        self.queue_start, self.queue_latched = (
                            current["source_us"],
                            False,
                        )
                    if (
                        current["source_us"] - self.queue_start
                    ) / 1e6 >= self.config.queue_seconds and not self.queue_latched:
                        reasons.append(
                            (
                                "QUEUE_RECORDING_CANDIDATE",
                                current["source_us"],
                                {
                                    "threshold_started_us": self.queue_start,
                                    "recording_threshold_bytes": self.config.queue_bytes,
                                    "required_source_seconds": self.config.queue_seconds,
                                },
                            )
                        )
                        self.queue_latched = True
                else:
                    self.queue_start, self.queue_latched = None, False
                self.last, self.last_source_mono = current, mono
                self.rate_blocked = False
            self.unavailable_since, self.issue_latched = None, False
        except (ValueError, KeyError, TypeError, OverflowError) as e:
            # Only fixed reason codes are persisted; raw input/error text is never retained.
            frame["quality"], frame["reason"] = "UNAVAILABLE", "SOURCE_INVALID"
            if isinstance(e, ValueError) and str(e) in {
                "SOURCE_TIME_STALE_OR_FUTURE",
                "SOURCE_NOT_ADVANCING",
                "SOURCE_SAMPLE_CONFLICT",
                "READ_UNAVAILABLE",
                "SOURCE_UNAVAILABLE",
                "INPUT_TOO_LARGE",
                "INPUT_NOT_REGULAR",
                "READ_MISSING",
                "READ_PERMISSION_DENIED",
                "READ_JSON_INVALID",
                "READ_INVALID",
                "READ_SYMLINK",
                "READ_IO_ERROR",
                "SOURCE_SCHEMA_INVALID",
                "SOURCE_STATE_INVALID",
                "TCP_METRICS_INVALID",
                "COUNTER_INVALID",
                "IDENTITY_INVALID",
                "SOURCE_TIME_INVALID",
                "SOURCE_TIME_NAIVE",
                "TCP_IDENTITY_UNBOUND",
            }:
                frame["reason"] = str(e)
            self.queue_start, self.queue_latched = None, False
            self.rate_blocked = True
            if self.unavailable_since is None:
                self.unavailable_since = mono
            if frame["reason"] == "SOURCE_NOT_ADVANCING" or (
                frame["reason"] == "SOURCE_TIME_STALE_OR_FUTURE"
                and current is not None
                and now_us - current["source_us"]
                > self.config.source_stale_seconds * 1e6
            ):
                self.unavailable_since = min(
                    self.unavailable_since, mono - self.config.source_stale_seconds
                )
            if (
                mono - self.unavailable_since >= self.config.source_stale_seconds
                and not self.issue_latched
            ):
                reasons.append(
                    (
                        "SOURCE_UNAVAILABLE",
                        frame["source_us"],
                        {"reason": frame["reason"]},
                    )
                )
                self.issue_latched = True
        self.previous_fresh = frame["quality"] in {"FRESH", "DUPLICATE"}
        self.counts[frame["quality"]] += 1
        self.counts["reason_" + frame["reason"]] += 1
        self.counts["journal_" + journal_status] += 1
        self.counts["clock_" + self.clock_status] += 1
        if self.clock_status == "VALID":
            self._stat("clock_offset_seconds", frame["clock"]["offset_seconds"])
            self._stat(
                "clock_uncertainty_seconds", frame["clock"]["uncertainty_seconds"]
            )
        if frame["quality"] == "FRESH":
            self.counts["state_" + current["state"]] += 1
            for key, number in current["tcp_metrics"].items():
                self._stat(key, number)
            self._stat("ack_mbps", frame["ack_mbps"])
            self._stat("send_mbps", frame["send_mbps"])
        # A journal event may arrive after a brief transition vanished from latest.json.
        for event in journal:
            reasons.append(("JOURNAL_EVENT", event["source_us"], event))
        while (
            self.ring and mono - self.ring[0]["monotonic_sec"] > self.config.pre_seconds
        ):
            self.ring.popleft()
        if frame["quality"] != "DUPLICATE":
            self.ring.append(frame)
        if self.active is not None and mono >= self.active["deadline"]:
            self._close_episode(
                now_us,
                "MAX_DURATION" if mono >= self.active["cap"] else "WINDOW_COMPLETE",
            )
        if reasons and self.active is None:
            history = list(self.ring)
            span = mono - history[0]["monotonic_sec"] if history else 0

            def start(db):
                cur = db.execute(
                    "INSERT INTO episodes(session,start_us,prehistory_seconds) VALUES(?,?,?)",
                    (self.session, now_us, span),
                )
                self.store.add_samples(db, cur.lastrowid, history)
                return cur.lastrowid

            episode = self.store.transaction(start)
            self.active = {
                "id": episode,
                "cap": mono + self.config.max_episode_seconds,
                "deadline": mono + self.config.post_seconds,
            }
        if self.active is not None or minute is not None:

            def persist(db):
                if self.active is not None:
                    self.store.add_samples(
                        db,
                        self.active["id"],
                        [frame] if frame["quality"] != "DUPLICATE" else [],
                    )
                    db.executemany(
                        "INSERT INTO triggers(episode,received_us,source_us,kind,details) VALUES(?,?,?,?,?)",
                        [
                            (self.active["id"], now_us, ts, kind, packed(details))
                            for kind, ts, details in reasons
                        ],
                    )
                    db.execute(
                        "UPDATE episodes SET end_us=? WHERE id=?",
                        (now_us, self.active["id"]),
                    )
                if minute:
                    db.execute(
                        "INSERT INTO minutes(session,bucket_us,payload) VALUES(?,?,?)",
                        (self.session, *minute),
                    )

            self.store.transaction(persist)
        if self.active is not None and reasons:
            self.active["deadline"] = min(
                self.active["cap"], mono + self.config.post_seconds
            )
        if mono - self.last_status >= 5:
            self.write_status(now_us, frame["quality"], journal_status)
            self.last_status = mono
        return frame, [r[0] for r in reasons]

    def _close_episode(self, now_us, reason):
        if self.active is not None:
            ident = self.active["id"]
            self.store.transaction(
                lambda db: db.execute(
                    "UPDATE episodes SET closed_reason=?,end_us=? WHERE id=?",
                    (reason, now_us, ident),
                )
            )
            self.active = None

    def write_status(self, now_us, state, journal_status="UNKNOWN"):
        times = sorted(self.store.latencies)
        self.store.status(
            {
                "schema": SCHEMA,
                "session": self.session,
                "updated_us": now_us,
                "state": state,
                "scope": "MAIN_FIFO_ONLY",
                "action_enabled": False,
                "delivery_verified": False,
                "clock_offset_status": self.clock_status,
                "journal_status": journal_status,
                "polls": self.polls,
                "unique_samples": self.unique,
                "duplicate_polls": self.duplicates,
                "gaps": self.gaps,
                "active_episode": self.active["id"] if self.active else None,
                "storage_bytes": self.store.bytes_used(),
                "commit_p95_ms": times[int((len(times) - 1) * 0.95)] * 1000
                if times
                else None,
            }
        )

    def finish(self, *, now, reason="RECORDER_STOPPED"):
        now_us = int(now * 1e6)
        self._close_episode(now_us, reason)

        def write(db):
            db.execute(
                "INSERT INTO minutes(session,bucket_us,payload) VALUES(?,?,?)",
                (self.session, self.bucket * 1000000, packed(self._minute())),
            )
            db.execute(
                "UPDATE sessions SET ended_us=?,reason=? WHERE id=?",
                (now_us, reason, self.session),
            )

        self.store.transaction(write)
        self.write_status(now_us, reason)


def readonly_report(directory: Path, episode=None, references=()):
    path = (directory / "recorder.sqlite3").resolve()
    with closing(
        sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, timeout=0.25)
    ) as db:
        db.row_factory = sqlite3.Row
        if db.execute("PRAGMA application_id").fetchone()[0] != APP_ID:
            raise ValueError("DATABASE_NOT_OWNED")
        if episode is not None:
            for row in db.execute(
                "SELECT payload FROM samples WHERE episode=? ORDER BY received_us",
                (episode,),
            ):
                yield json.loads(row[0])
            return
        events = []
        for row in db.execute("SELECT * FROM episodes ORDER BY id DESC LIMIT 100"):
            event = dict(row)
            event["triggers"] = [
                dict(r)
                for r in db.execute(
                    "SELECT received_us,source_us,kind,details FROM triggers WHERE episode=? ORDER BY id",
                    (row["id"],),
                )
            ]
            event["sample_count"] = db.execute(
                "SELECT count(*) FROM samples WHERE episode=?", (row["id"],)
            ).fetchone()[0]
            event["prehistory_complete"] = row["prehistory_seconds"] >= 119
            event["reference_comparisons"] = [
                {"trigger_id": t["kind"], **lead_interval(t["source_us"], ref)}
                for ref in references
                if ref.get("episode_id") == row["id"]
                for t in event["triggers"]
                if t["kind"] == "QUEUE_RECORDING_CANDIDATE"
            ]
            events.append(event)
        yield {
            "schema": SCHEMA,
            "scope": "MAIN_FIFO_ONLY",
            "action_enabled": False,
            "episodes": events,
            "sessions": db.execute("SELECT count(*) FROM sessions").fetchone()[0],
            "minutes": db.execute("SELECT count(*) FROM minutes").fetchone()[0],
            "episode_total": db.execute("SELECT count(*) FROM episodes").fetchone()[0],
            "episode_list_limit": 100,
            "trigger_counts": dict(
                db.execute("SELECT kind,count(*) FROM triggers GROUP BY kind")
            ),
            "fresh_observed_seconds": db.execute(
                "SELECT coalesce(sum(json_extract(payload,'$.fresh_observed_seconds')),0) FROM minutes"
            ).fetchone()[0],
            "predictive_performance": "NOT_EVALUATED_WITHOUT_INDEPENDENT_LABELS",
        }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    subs = parser.add_subparsers(dest="command", required=True)
    collect = subs.add_parser("collect")
    collect.add_argument("--source", type=Path, required=True)
    collect.add_argument("--journal", type=Path)
    collect.add_argument(
        "--clock-source",
        type=Path,
        help="Optional existing NTP observation JSON; clock uncertainty stays UNKNOWN when omitted",
    )
    collect.add_argument("--state", type=Path, required=True)
    collect.add_argument("--steps", type=int, help="Bounded poll count for development")
    report = subs.add_parser("report")
    report.add_argument("--state", type=Path, required=True)
    report.add_argument(
        "--episode", type=int, help="Write the selected episode timeline as JSONL"
    )
    report.add_argument(
        "--references",
        type=Path,
        help="JSON array of independent last-good and first-bad observation intervals",
    )
    args = parser.parse_args(argv)
    if args.command == "report":
        references = (
            json.loads(bounded_read(args.references)) if args.references else []
        )
        if not isinstance(references, list) or not all(
            isinstance(v, dict) for v in references
        ):
            parser.error("references must be an array of objects")
        for row in readonly_report(args.state, args.episode, references):
            print(packed(row))
        return 0
    if args.state.is_symlink():
        parser.error("state directory must not be a symlink")
    state = args.state.resolve()
    for p in (args.source, args.journal, args.clock_source):
        if p and (p.resolve() == state or state in p.resolve().parents):
            parser.error("source must be outside recorder state")
    if args.steps is not None and args.steps <= 0:
        parser.error("steps must be positive")
    os.umask(0o077)
    config = Config()
    # A per-file OS limit also bounds WAL growth when a long-lived reader prevents checkpointing.
    soft, hard = resource.getrlimit(resource.RLIMIT_FSIZE)
    file_cap = config.max_storage_bytes // 3
    resource.setrlimit(
        resource.RLIMIT_FSIZE,
        (min(soft, file_cap) if soft != resource.RLIM_INFINITY else file_cap, hard),
    )
    signal.signal(signal.SIGXFSZ, signal.SIG_IGN)
    stopped = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stopped.set())
    store = None
    recorder = None
    try:
        store = Store(state, config)
        recorder = Recorder(
            store,
            now=time.time(),
            mono=time.monotonic(),
            source_path=args.source.resolve(),
        )
        journal = JournalReader(args.journal)
        count = 0
        while not stopped.is_set():
            started = time.monotonic()
            value, error = read_source(args.source)
            events = journal.poll()
            clock = None
            if args.clock_source:
                try:
                    clock = json.loads(bounded_read(args.clock_source))
                except (OSError, ValueError, RecursionError):
                    pass
            recorder.step(
                value,
                now=time.time(),
                mono=time.monotonic(),
                read_error=error,
                journal=events,
                journal_status=journal.status,
                clock=clock,
            )
            count += 1
            if args.steps is not None and count >= args.steps:
                break
            stopped.wait(max(0, 1 - (time.monotonic() - started)))
        recorder.finish(now=time.time())
        return 0
    except (StorageLimit, sqlite3.Error, OSError, ValueError) as e:
        state_code = (
            "STORAGE_LIMIT"
            if isinstance(e, StorageLimit)
            or getattr(e, "sqlite_errorcode", None) == sqlite3.SQLITE_FULL
            else "STORAGE_WRITE_FAILED"
        )
        if isinstance(e, ValueError):
            state_code = "CONFIG_OR_STATE_INVALID"
        status_failed = False
        if recorder is not None:
            try:
                recorder.write_status(int(time.time() * 1e6), state_code)
            except OSError:
                status_failed = True
        print(
            packed(
                {
                    "schema": SCHEMA,
                    "state": state_code,
                    "error_type": type(e).__name__,
                    "status_write_failed": status_failed,
                    "action_enabled": False,
                }
            ),
            flush=True,
        )
        return 75
    finally:
        if store is not None:
            store.close()


if __name__ == "__main__":
    raise SystemExit(main())
