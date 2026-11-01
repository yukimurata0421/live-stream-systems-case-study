"""Independent upload measurements and receipt-bound reads; no recovery effects."""
from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import importlib.util
import json
import math
import os
from pathlib import Path
import stat
import sys

try:
    from . import clock_contract as clock
except ImportError:
    _spec = importlib.util.spec_from_file_location("upload_sample_clock", Path(__file__).with_name("clock_contract.py"))
    clock = importlib.util.module_from_spec(_spec)
    sys.modules[_spec.name] = clock
    _spec.loader.exec_module(clock)

SCHEMA = "stream.upload_measurement.v1"
MANIFEST_SCHEMA = "stream.upload_measurement_source.v1"
TAIL_BYTES = 262144
JOURNAL_BYTES = 4 * 1024 * 1024
FRESH_SECONDS = 180
RECOVERY_SECONDS = 900


def finite(value):
    return type(value) in (int, float) and math.isfinite(value)


def epoch(value):
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("timestamp_timezone_missing")
    result = parsed.timestamp()
    if not math.isfinite(result) or result <= 0:
        raise ValueError("timestamp_invalid")
    return result


def utc(value):
    return datetime.fromtimestamp(value, timezone.utc).isoformat().replace("+00:00", "Z")


def read_regular(path, limit, *, tail=False):
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(descriptor, "rb") as handle:
        info = os.fstat(handle.fileno())
        if not stat.S_ISREG(info.st_mode):
            raise ValueError("measurement_file_not_regular")
        offset = max(0, info.st_size - limit) if tail else 0
        if not tail and info.st_size > limit:
            raise ValueError("measurement_file_too_large")
        handle.seek(offset)
        raw = handle.read(limit + (0 if tail else 1))
    if len(raw) > limit:
        raise ValueError("measurement_file_too_large")
    return raw.split(b"\n", 1)[-1] if offset else raw


def read_json(path, limit=16384):
    value = json.loads(read_regular(path, limit))
    if not isinstance(value, dict):
        raise ValueError("measurement_object_required")
    return value


def write_json(path, value):
    path = Path(path)
    temporary = path.with_name(path.name + ".tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NOFOLLOW, 0o644)
    with os.fdopen(descriptor, "w") as handle:
        json.dump(value, handle, separators=(",", ":"), allow_nan=False)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def append_sample(path, row):
    path = Path(path)
    raw = (json.dumps(row, separators=(",", ":"), allow_nan=False) + "\n").encode()
    if len(raw) > 8192:
        raise ValueError("measurement_row_too_large")
    if path.exists() and path.stat().st_size + len(raw) > JOURNAL_BYTES:
        if path.is_symlink():
            raise ValueError("measurement_journal_symlink")
        previous = path.with_name(path.name + ".1")
        if previous.is_symlink():
            raise ValueError("measurement_journal_symlink")
        os.replace(path, previous)
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND | os.O_NOFOLLOW | os.O_NONBLOCK, 0o644)
    with os.fdopen(descriptor, "ab") as handle:
        if not stat.S_ISREG(os.fstat(handle.fileno()).st_mode):
            raise ValueError("measurement_file_not_regular")
        handle.write(raw)
        handle.flush()
        os.fsync(handle.fileno())


def journal_rows(directory, *, history=False):
    result = {}
    for suffix in (".1", ""):
        path = Path(directory) / ("samples.jsonl" + suffix)
        try:
            raw = read_regular(path, JOURNAL_BYTES if history else TAIL_BYTES, tail=True)
        except FileNotFoundError:
            continue
        for line in raw.splitlines():
            try:
                row = json.loads(line)
                if not isinstance(row, dict) or not isinstance(row.get("sample_id"), str):
                    continue
                result.setdefault(row["sample_id"], row)
            except (ValueError, TypeError):
                continue
    return list(result.values())


def validate_sample(row, expected_machine):
    required = ("source_boot", "source_host", "producer_instance_id", "ffmpeg_generation", "measurement_epoch", "sample_id")
    if (row.get("schema") != SCHEMA or row.get("kind") != "tcp_send_sample"
            or row.get("source_machine") != expected_machine or len(expected_machine) != 64
            or any(not isinstance(row.get(key), str) or not row[key] for key in required)):
        raise ValueError("measurement_identity_invalid")
    for key in ("ffmpeg_pid", "sequence_start", "sequence_end"):
        if type(row.get(key)) is not int or row[key] <= 0:
            raise ValueError("measurement_counter_invalid")
    if (row["ffmpeg_pid"] <= 1 or row["sequence_end"] <= row["sequence_start"]
            or not finite(row.get("mbps")) or row["mbps"] < 0
            or not finite(row.get("sample_interval_sec")) or not 0 < row["sample_interval_sec"] <= FRESH_SECONDS
            or not finite(row.get("source_mono")) or row["source_mono"] < 0
            or not finite(row.get("source_counter_age_sec")) or not 0 <= row["source_counter_age_sec"] <= 3):
        raise ValueError("measurement_value_invalid")
    for key in ("bytes_sent", "bytes_sent_delta"):
        if type(row.get(key)) is not int or row[key] < 0:
            raise ValueError("measurement_counter_invalid")
    duration = epoch(row["sample_end_observed_at"]) - epoch(row["sample_start_observed_at"])
    if abs(duration - row["sample_interval_sec"]) > .00001:
        raise ValueError("measurement_interval_invalid")
    if abs(round(row["bytes_sent_delta"] * 8 / duration / 1_000_000, 3) - row["mbps"]) > .000001:
        raise ValueError("measurement_rate_invalid")


def age_upper(row, *, now=None, local_identity=None, expected_machine=None):
    now = clock.capture() if now is None else now
    local = clock.identity() if local_identity is None else local_identity
    machine = row.get("source_machine") if expected_machine is None else expected_machine
    validate_sample(row, machine)
    if row["source_machine"] == local["machine_id_sha256"]:
        if row["source_boot"] != local["boot_id"]:
            raise ValueError("measurement_source_boot_changed")
        age = now["mono"] - row["source_mono"]
        if age < -clock.EPSILON:
            raise ValueError("measurement_source_future")
        return max(0., age) + row["source_counter_age_sec"]
    receipt = row["receipt"]
    if (receipt["receiver_boot"] != local["boot_id"]
            or receipt["receiver_machine"] != local["machine_id_sha256"]
            or now["mono"] < receipt["received"]["mono"]):
        raise ValueError("measurement_receipt_boot_changed")
    age = clock.monotonic_source_age(receipt["clock"], receipt["nonce"], receipt["sent"], receipt["received"],
        machine=machine, boot_id=row["source_boot"], source_mono=row["source_mono"], max_age=172800)
    return age + now["mono"] - receipt["received"]["mono"] + row["source_counter_age_sec"]


def manifest(state_root):
    directory = Path(state_root) / "upload_measurement"
    path = directory / "source.json"
    try:
        value = read_json(path)
    except FileNotFoundError:
        return None
    if (value.get("schema") != MANIFEST_SCHEMA or not isinstance(value.get("source_machine"), str)
            or len(value["source_machine"]) != 64 or not isinstance(value.get("source_host"), str)):
        raise ValueError("measurement_source_manifest_invalid")
    epoch(value["cutover_at_utc"])
    return value


def current_rows(state_root, *, now=None):
    selection = manifest(state_root)
    if selection is None:
        return None
    now = clock.capture() if now is None else now
    local = clock.identity()
    directory = Path(state_root) / "upload_measurement"
    try:
        status = read_json(directory / "status.json")
    except FileNotFoundError:
        return []
    if status.get("receiver_boot") != local["boot_id"]:
        return []
    result = []
    for row in journal_rows(directory):
        try:
            if row.get("source_host") != selection["source_host"]:
                continue
            age = age_upper(row, now=now, expected_machine=selection["source_machine"])
            result.append(dict(row, _source_age=age, _ts=now["wall"] - age, _mbps=row["mbps"]))
        except (ValueError, TypeError, KeyError, OverflowError):
            continue
    return result


def current_times(state_root, now_wall):
    try:
        rows = current_rows(state_root)
        if rows is None:
            return None
        return [now_wall - row["_source_age"] for row in rows
                if row["_source_age"] <= RECOVERY_SECONDS + FRESH_SECONDS]
    except (OSError, ValueError, TypeError, KeyError):
        return []


def latest(state_root, now_wall):
    try:
        rows = current_rows(state_root)
        if rows is None:
            return None
        if not rows:
            return {}
        row = min(rows, key=lambda item: item["_source_age"])
        return dict(row, _ts=now_wall - row["_source_age"])
    except (OSError, ValueError, TypeError, KeyError):
        return {}


def merge_events(state_root, legacy, *, min_timestamp=0):
    """Keep old history; after explicit cutover only the independent source votes."""
    try:
        selection = manifest(state_root)
    except (OSError, ValueError, TypeError, KeyError):
        # A broken selection cannot establish current freshness. Keep original
        # historical evidence visible rather than silently erasing old samples.
        yield from legacy
        return
    if selection is None:
        yield from legacy
        return
    cutover = epoch(selection["cutover_at_utc"])
    for row in legacy:
        try:
            if row.get("kind") != "tcp_send_sample" or epoch(row["ts_utc"]) < cutover:
                yield row
        except (ValueError, TypeError, KeyError):
            continue
    try:
        rows = journal_rows(Path(state_root) / "upload_measurement", history=True)
    except (OSError, ValueError):
        return
    for row in rows:
        try:
            validate_sample(row, selection["source_machine"])
            if row.get("source_host") != selection["source_host"]:
                continue
            if epoch(row["ts_utc"]) >= max(cutover, min_timestamp):
                yield row
        except (ValueError, TypeError, KeyError, OverflowError):
            continue


def exporter_rows(state_root, legacy_rows, now_wall):
    """Merge original history without changing the exporter's metric contract."""
    result = []
    for row in merge_events(state_root, legacy_rows):
        try:
            if row.get("kind") != "tcp_send_sample":
                continue
            timestamp = epoch(row["ts_utc"])
            mbps = row.get("mbps", row.get("send_mbps"))
            if timestamp <= now_wall and finite(mbps) and mbps >= 0:
                result.append(dict(row, _ts=timestamp, _mbps=mbps))
        except (ValueError, TypeError, KeyError, OverflowError):
            continue
    return sorted(result, key=lambda row: row["_ts"])
