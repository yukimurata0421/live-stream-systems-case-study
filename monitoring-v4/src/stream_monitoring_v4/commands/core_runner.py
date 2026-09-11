from __future__ import annotations

import json
import signal
import time
from pathlib import Path

from stream_contracts.monitoring_v4.time import utc_text

from stream_monitoring_v4.commands import shadow_once
from stream_monitoring_v4.commands.wall_clock_loop import next_run_epoch
from stream_monitoring_v4.runtime.atomic_file import atomic_write_text
from stream_monitoring_v4.runtime.health_files import validate_distinct_health_files


def parser():
    result = shadow_once.parser()
    result.description = (
        "Run the credential-free Monitoring v4 decision core as a persistent scheduler"
    )
    result.add_argument("--runner-second", type=int, default=20)
    result.add_argument("--ready-file", type=Path, default=Path("/tmp/core-ready"))
    result.add_argument(
        "--heartbeat-file",
        type=Path,
        default=Path("/tmp/core-heartbeat"),
    )
    result.add_argument("--startup-db-timeout-sec", type=int, default=180)
    return result


def _write_ready(path: Path, completed_ts: int) -> None:
    atomic_write_text(path, f"{completed_ts}\n", encoding="ascii", mode=0o600)


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    if args.now_ts is not None:
        raise ValueError("persistent core does not accept --now-ts; use shadow_once for replay")
    if not 0 <= int(args.runner_second) <= 59:
        raise ValueError("--runner-second must be between 0 and 59")
    validate_distinct_health_files(args.ready_file, args.heartbeat_file)

    stopping = False

    def stop(_signum: int, _frame: object) -> None:
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    repository, database = shadow_once.initialize_runtime(
        args,
        postgres_startup_timeout_sec=max(0, int(args.startup_db_timeout_sec)),
    )
    try:
        while not stopping:
            scheduled = next_run_epoch(
                time.time(),
                second=int(args.runner_second),
                minute_modulo=1,
            )
            while not stopping:
                remaining = scheduled - time.time()
                if remaining <= 0:
                    break
                time.sleep(min(0.5, remaining))
            if stopping:
                break
            cycle_started = time.monotonic()
            try:
                code = shadow_once.run_parsed_once(args, repository, database)
                if code != 0:
                    raise RuntimeError(f"shadow cycle returned {code}")
                completed_ts = int(time.time())
                _write_ready(args.ready_file, completed_ts)
                print(
                    json.dumps(
                        {
                            "schema": "monitoring_v4.core_runner_cycle.v1",
                            "scheduled_at": utc_text(int(scheduled)),
                            "completed_at": utc_text(completed_ts),
                            "duration_ms": int((time.monotonic() - cycle_started) * 1000),
                            "status": "good",
                        },
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                    flush=True,
                )
            except Exception as exc:
                print(
                    json.dumps(
                        {
                            "schema": "monitoring_v4.core_runner_cycle.v1",
                            "scheduled_at": utc_text(int(scheduled)),
                            "failed_at": utc_text(int(time.time())),
                            "status": "failed",
                            "error_type": type(exc).__name__,
                        },
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                    flush=True,
                )
            finally:
                _write_ready(args.heartbeat_file, int(time.time()))
        return 0
    finally:
        close = getattr(repository, "close", None)
        if callable(close):
            close()


if __name__ == "__main__":
    raise SystemExit(main())
