from __future__ import annotations

import argparse
import json
import math
import signal
import time
from pathlib import Path

from stream_monitoring_v4.runtime.safe_input import project_safe_inputs
from stream_monitoring_v4.runtime.atomic_file import atomic_write_text
from stream_monitoring_v4.runtime.health_files import validate_distinct_health_files


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description="Project fixed sanitized v3 state into v4-owned input")
    result.add_argument("--source-root", type=Path, required=True)
    result.add_argument("--target-root", type=Path, required=True)
    result.add_argument("--interval-sec", type=float, default=2.0)
    result.add_argument("--ready-file", type=Path, default=Path("/tmp/projector-ready"))
    result.add_argument(
        "--heartbeat-file",
        type=Path,
        default=Path("/tmp/projector-heartbeat"),
    )
    result.add_argument("--once", action="store_true")
    return result


def _write_ready(path: Path, now_ts: int) -> None:
    atomic_write_text(path, f"{now_ts}\n", encoding="ascii", mode=0o600)


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    if not math.isfinite(args.interval_sec) or args.interval_sec <= 0:
        raise ValueError("--interval-sec must be a positive finite value")
    validate_distinct_health_files(args.ready_file, args.heartbeat_file)
    stopping = False

    def stop(_signum: int, _frame: object) -> None:
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    previous = ""
    while not stopping:
        result = project_safe_inputs(args.source_root, args.target_root)
        payload = result.to_dict()
        rendered = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        if rendered != previous:
            print(rendered, flush=True)
            previous = rendered
        if result.ready:
            _write_ready(args.ready_file, int(time.time()))
        else:
            try:
                args.ready_file.unlink()
            except FileNotFoundError:
                pass
        _write_ready(args.heartbeat_file, int(time.time()))
        if args.once:
            return 0 if result.ready else 1
        deadline = time.monotonic() + max(0.25, float(args.interval_sec))
        while not stopping and time.monotonic() < deadline:
            time.sleep(min(0.25, deadline - time.monotonic()))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
