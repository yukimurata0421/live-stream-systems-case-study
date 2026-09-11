from __future__ import annotations

import argparse
import json
import signal
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path

from stream_contracts.monitoring_v4.time import utc_text
from stream_monitoring_v4.runtime.atomic_file import atomic_write_text
from stream_monitoring_v4.runtime.health_files import validate_distinct_health_files


STOP = False


def next_run_epoch(now: float, *, second: int, minute_modulo: int) -> int:
    candidate = int(now) + 1
    while True:
        value = datetime.fromtimestamp(candidate, tz=timezone.utc)
        if value.second == second and value.minute % minute_modulo == 0:
            return candidate
        candidate += 1


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description="Run one command on a UTC wall-clock cadence")
    result.add_argument("--second", type=int, required=True)
    result.add_argument("--minute-modulo", type=int, default=1)
    result.add_argument("--timeout-sec", type=int, default=45)
    result.add_argument("--ready-file", type=Path, default=None)
    result.add_argument("--heartbeat-file", type=Path, default=None)
    result.add_argument("command", nargs=argparse.REMAINDER)
    return result


def _stop(_signum: int, _frame: object) -> None:
    global STOP
    STOP = True


def _write_ready(path: Path, completed_ts: int) -> None:
    atomic_write_text(path, f"{completed_ts}\n", encoding="ascii", mode=0o600)


def main(argv: list[str] | None = None) -> int:
    global STOP
    STOP = False
    args = parser().parse_args(argv)
    if not 0 <= args.second <= 59:
        raise ValueError("--second must be between 0 and 59")
    if args.minute_modulo <= 0 or args.minute_modulo > 60:
        raise ValueError("--minute-modulo must be between 1 and 60")
    if not args.command:
        raise ValueError("a command is required after --")
    command = args.command[1:] if args.command[0] == "--" else args.command
    if not command:
        raise ValueError("a command is required after --")
    validate_distinct_health_files(args.ready_file, args.heartbeat_file)
    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)
    last_heartbeat = 0.0

    def heartbeat(*, force: bool = False) -> None:
        nonlocal last_heartbeat
        now = time.monotonic()
        if args.heartbeat_file is not None and (force or now - last_heartbeat >= 15.0):
            _write_ready(args.heartbeat_file, int(time.time()))
            last_heartbeat = now

    heartbeat(force=True)
    while not STOP:
        scheduled = next_run_epoch(
            time.time(), second=args.second, minute_modulo=args.minute_modulo
        )
        while not STOP and time.time() < scheduled:
            heartbeat()
            time.sleep(min(0.5, max(0.01, scheduled - time.time())))
        if STOP:
            break
        started = int(time.time())
        outcome = "ok"
        returncode = 0
        try:
            completed = subprocess.run(
                command,
                check=False,
                timeout=max(1, int(args.timeout_sec)),
            )
            returncode = completed.returncode
            outcome = "ok" if returncode == 0 else "failed"
        except subprocess.TimeoutExpired:
            returncode = 124
            outcome = "timeout"
        completed_ts = int(time.time())
        if returncode == 0 and args.ready_file is not None:
            _write_ready(args.ready_file, completed_ts)
        heartbeat(force=True)
        print(
            json.dumps(
                {
                    "schema": "monitoring_v4.wall_clock_run.v1",
                    "scheduled_at": utc_text(scheduled),
                    "started_at": utc_text(started),
                    "completed_at": utc_text(completed_ts),
                    "outcome": outcome,
                    "returncode": returncode,
                },
                sort_keys=True,
                separators=(",", ":"),
            ),
            flush=True,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
