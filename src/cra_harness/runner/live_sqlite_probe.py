from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import sqlite3
import statistics
import threading
import time
from pathlib import Path
from typing import Any

from cra_dell_recovery.time import isoformat_utc, utc_now


def _append(path: Path, payload: dict[str, Any]) -> None:
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, separators=(",", ":"), sort_keys=True) + "\n")
        handle.flush()
        os.fsync(handle.fileno())


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _percentiles(values: list[float]) -> dict[str, Any]:
    ordered = sorted(values)
    return {
        "sample_count": len(ordered),
        "median": None if not ordered else round(statistics.median(ordered), 6),
        "p95": None if not ordered else round(ordered[math.ceil(0.95 * len(ordered)) - 1], 6),
        "p99": None if not ordered else round(ordered[math.ceil(0.99 * len(ordered)) - 1], 6),
        "maximum": None if not ordered else round(ordered[-1], 6),
        "unit": "ms",
    }


def _reader(database: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(f"file:{database}?mode=ro", uri=True, isolation_level=None, timeout=5.0)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only=ON")
    connection.execute("PRAGMA busy_timeout=5000")
    connection.execute("PRAGMA trusted_schema=OFF")
    return connection


def _snapshot(database: Path, target_id: str) -> dict[str, Any]:
    connection = _reader(database)
    try:
        connection.execute("BEGIN")
        fence = connection.execute("SELECT * FROM authority_fences WHERE target_id=?", (target_id,)).fetchone()
        if fence is None:
            raise KeyError(target_id)
        counts = {
            table: int(connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0])
            for table in ("agent_commands", "execution_attempts", "local_actions", "reconciliation_sessions")
        }
        return {"fence": dict(fence), "counts": counts}
    finally:
        if connection.in_transaction:
            connection.execute("ROLLBACK")
        connection.close()


def _checkpoint(database: Path, mode: str) -> tuple[int, int, int]:
    connection = sqlite3.connect(database, isolation_level=None, timeout=5.0)
    try:
        connection.execute("PRAGMA busy_timeout=5000")
        row = connection.execute(f"PRAGMA wal_checkpoint({mode})").fetchone()
        if row is None:
            raise RuntimeError("checkpoint returned no row")
        return int(row[0]), int(row[1]), int(row[2])
    finally:
        connection.close()


def _backup(database: Path, destination: Path, target_id: str) -> dict[str, Any]:
    source = _reader(database)
    backup = sqlite3.connect(destination, timeout=5.0)
    try:
        source.backup(backup)
        integrity = str(backup.execute("PRAGMA integrity_check").fetchone()[0])
        fence_count = int(backup.execute("SELECT count(*) FROM authority_fences WHERE target_id=?", (target_id,)).fetchone()[0])
    finally:
        backup.close()
        source.close()
    os.chmod(destination, 0o600)
    return {
        "path": str(destination),
        "sha256": hashlib.sha256(destination.read_bytes()).hexdigest(),
        "integrity_check": integrity,
        "authority_row_count": fence_count,
    }


def run_live_probe(
    database: Path,
    output_dir: Path,
    *,
    target_id: str,
    cycles: int = 60,
    reader_hold_seconds: float = 0.15,
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=False)
    started_at = isoformat_utc(utc_now())
    checkpoint_latencies: list[float] = []
    critical_read_latencies: list[float] = []
    checkpoint_errors = 0
    critical_read_errors = 0
    inconsistent_reads = 0
    overlap_hits = 0
    heartbeat_overlap_hits = 0
    busy_count = 0
    wal_peak = 0
    baseline = _snapshot(database, target_id)
    _write_json(output_dir / "baseline.json", baseline)
    backup_result: dict[str, Any] | None = None
    for cycle in range(cycles):
        cycle_id = f"live-sqlite-{cycle + 1:04d}"
        mode = ("PASSIVE", "FULL", "TRUNCATE")[cycle % 3]
        before = _snapshot(database, target_id)
        reader_ready = threading.Event()
        reader_window: dict[str, Any] = {}
        checkpoint_window: dict[str, Any] = {}
        errors: list[tuple[str, BaseException]] = []

        def critical_read(
            reader_window_ref: dict[str, Any] = reader_window,
            reader_ready_ref: threading.Event = reader_ready,
            errors_ref: list[tuple[str, BaseException]] = errors,
        ) -> None:
            connection = _reader(database)
            started_ns = time.perf_counter_ns()
            reader_window_ref["started_at"] = isoformat_utc(utc_now())
            reader_window_ref["started_ns"] = started_ns
            try:
                connection.execute("BEGIN")
                row = connection.execute("SELECT * FROM authority_fences WHERE target_id=?", (target_id,)).fetchone()
                if row is None:
                    raise KeyError(target_id)
                reader_window_ref["authority_epoch"] = int(row["highest_authority_epoch_seen"])
                reader_window_ref["authority_session_id"] = str(row["active_authority_session_id"])
                reader_window_ref["authority_state"] = str(row["authority_state"])
                reader_window_ref["heartbeat_seq"] = int(row["heartbeat_seq"])
                reader_ready_ref.set()
                time.sleep(reader_hold_seconds)
                repeat = connection.execute("SELECT * FROM authority_fences WHERE target_id=?", (target_id,)).fetchone()
                if repeat is None or dict(repeat) != dict(row):
                    reader_window_ref["inconsistent"] = True
                connection.execute("ROLLBACK")
            except BaseException as caught:
                errors_ref.append(("critical_reader", caught))
                reader_ready_ref.set()
            finally:
                if connection.in_transaction:
                    connection.execute("ROLLBACK")
                connection.close()
                reader_window_ref["finished_ns"] = time.perf_counter_ns()
                reader_window_ref["finished_at"] = isoformat_utc(utc_now())
                reader_window_ref["latency_ms"] = (reader_window_ref["finished_ns"] - started_ns) / 1_000_000

        def checkpoint(
            reader_ready_ref: threading.Event = reader_ready,
            checkpoint_window_ref: dict[str, Any] = checkpoint_window,
            errors_ref: list[tuple[str, BaseException]] = errors,
            mode_ref: str = mode,
        ) -> None:
            if not reader_ready_ref.wait(timeout=5):
                errors_ref.append(("checkpoint_driver", RuntimeError("reader readiness timeout")))
                return
            time.sleep(0.005)
            started_ns = time.perf_counter_ns()
            checkpoint_window_ref["started_ns"] = started_ns
            checkpoint_window_ref["started_at"] = isoformat_utc(utc_now())
            try:
                checkpoint_window_ref["result"] = list(_checkpoint(database, mode_ref))
            except BaseException as caught:
                errors_ref.append(("checkpoint", caught))
            finally:
                checkpoint_window_ref["finished_ns"] = time.perf_counter_ns()
                checkpoint_window_ref["finished_at"] = isoformat_utc(utc_now())
                checkpoint_window_ref["latency_ms"] = (checkpoint_window_ref["finished_ns"] - started_ns) / 1_000_000

        reader_thread = threading.Thread(target=critical_read, name=f"{cycle_id}-critical-reader")
        checkpoint_thread = threading.Thread(target=checkpoint, name=f"{cycle_id}-checkpoint")
        reader_thread.start()
        checkpoint_thread.start()
        reader_thread.join(timeout=10)
        checkpoint_thread.join(timeout=10)
        if reader_thread.is_alive() or checkpoint_thread.is_alive():
            errors.append(("scheduler", RuntimeError("live probe thread timeout")))
        after = _snapshot(database, target_id)
        overlapped = False
        if reader_window.get("inconsistent"):
            inconsistent_reads += 1
        if "latency_ms" in reader_window:
            critical_read_latencies.append(float(reader_window["latency_ms"]))
        if "latency_ms" in checkpoint_window:
            checkpoint_latencies.append(float(checkpoint_window["latency_ms"]))
        if "started_ns" in reader_window and "started_ns" in checkpoint_window:
            overlapped = max(int(reader_window["started_ns"]), int(checkpoint_window["started_ns"])) <= min(
                int(reader_window["finished_ns"]), int(checkpoint_window["finished_ns"])
            )
            overlap_hits += int(overlapped)
        heartbeat_changed = int(after["fence"]["heartbeat_seq"]) > int(before["fence"]["heartbeat_seq"])
        heartbeat_overlap_hits += int(heartbeat_changed)
        result = checkpoint_window.get("result")
        if isinstance(result, list):
            busy_count += int(result[0] != 0)
        for actor, error in errors:
            if actor == "critical_reader":
                critical_read_errors += 1
            else:
                checkpoint_errors += 1
            _append(
                output_dir / "sqlite_errors.jsonl",
                {
                    "cycle_id": cycle_id,
                    "actor": actor,
                    "error_type": type(error).__name__,
                    "observed_at": isoformat_utc(utc_now()),
                },
            )
        event = {
            "cycle_id": cycle_id,
            "mode": mode,
            "checkpoint": checkpoint_window,
            "critical_read": reader_window,
            "overlap": overlapped,
            "heartbeat_seq_before": before["fence"]["heartbeat_seq"],
            "heartbeat_seq_after": after["fence"]["heartbeat_seq"],
            "heartbeat_write_in_cycle": heartbeat_changed,
            "physical_attempt_count": 0,
        }
        _append(output_dir / "checkpoint_events.jsonl", event)
        _append(output_dir / "critical_read_events.jsonl", {"cycle_id": cycle_id, **reader_window})
        _append(
            output_dir / "heartbeat_events.jsonl",
            {
                "cycle_id": cycle_id,
                "heartbeat_seq_before": before["fence"]["heartbeat_seq"],
                "heartbeat_seq_after": after["fence"]["heartbeat_seq"],
                "write_observed": heartbeat_changed,
            },
        )
        wal = database.with_name(f"{database.name}-wal")
        wal_peak = max(wal_peak, wal.stat().st_size if wal.exists() else 0)
        if cycle == cycles // 2:
            backup_result = _backup(database, output_dir / "live-online-backup.sqlite3", target_id)
    final = _snapshot(database, target_id)
    integrity_connection = _reader(database)
    try:
        integrity = str(integrity_connection.execute("PRAGMA integrity_check").fetchone()[0])
    finally:
        integrity_connection.close()
    safety = {
        "physical_attempt_count": 0,
        "ffmpeg_signal_count": 0,
        "pod_mutation_count": 0,
        "deployment_mutation_count": 0,
        "host_restart_count": 0,
    }
    summary = {
        "profile": "LIVE_SQLITE_CONCURRENCY_PROBE_V1",
        "started_at": started_at,
        "finished_at": isoformat_utc(utc_now()),
        "sqlite_version": sqlite3.sqlite_version,
        "cycles": cycles,
        "checkpoint_count": cycles - checkpoint_errors,
        "checkpoint_modes": {mode: cycles // 3 for mode in ("PASSIVE", "FULL", "TRUNCATE")},
        "checkpoint_read_overlap_hits": overlap_hits,
        "heartbeat_write_cycle_hits": heartbeat_overlap_hits,
        "critical_read_count": cycles - critical_read_errors,
        "critical_read_errors": critical_read_errors,
        "checkpoint_errors": checkpoint_errors,
        "inconsistent_reads": inconsistent_reads,
        "busy_count": busy_count,
        "checkpoint_latency_ms": _percentiles(checkpoint_latencies),
        "critical_read_latency_ms": _percentiles(critical_read_latencies),
        "wal_peak_bytes": wal_peak,
        "integrity_check": integrity,
        "backup": backup_result,
        "baseline": baseline,
        "final": final,
        "safety": safety,
        "result": (
            "PASS"
            if cycles > 0
            and overlap_hits == cycles
            and critical_read_errors == checkpoint_errors == inconsistent_reads == 0
            and integrity == "ok"
            and backup_result is not None
            and backup_result["integrity_check"] == "ok"
            and not any(safety.values())
            else "FAIL"
        ),
    }
    _write_json(output_dir / "summary.json", summary)
    _write_json(output_dir / "safety.json", safety)
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description="Run a no-action live SQLite checkpoint/read overlap probe")
    parser.add_argument("--database", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--target-id", required=True)
    parser.add_argument("--cycles", type=int, default=60)
    parser.add_argument("--reader-hold-seconds", type=float, default=0.15)
    args = parser.parse_args()
    summary = run_live_probe(
        args.database,
        args.output_dir,
        target_id=args.target_id,
        cycles=args.cycles,
        reader_hold_seconds=args.reader_hold_seconds,
    )
    print(json.dumps(summary, ensure_ascii=False, sort_keys=True))
    return 0 if summary["result"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
