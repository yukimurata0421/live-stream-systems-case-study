from __future__ import annotations

import argparse
import json
import math
import signal
import time
from pathlib import Path

from stream_monitoring_v4.collectors.youtube_api import (
    YouTubeApiCollector,
    read_credentials,
)
from stream_monitoring_v4.runtime.atomic_file import atomic_write_text
from stream_monitoring_v4.runtime.health_files import validate_distinct_health_files


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(
        description="Collect sanitized read-only YouTube API evidence"
    )
    result.add_argument("--credentials-dir", type=Path, required=True)
    result.add_argument("--output", type=Path, required=True)
    result.add_argument("--interval-sec", type=float, default=120.0)
    result.add_argument("--maximum-backoff-sec", type=float, default=900.0)
    result.add_argument("--timeout-sec", type=float, default=10.0)
    result.add_argument("--max-response-bytes", type=int, default=512 * 1024)
    result.add_argument("--ready-file", type=Path, default=Path("/tmp/youtube-api-ready"))
    result.add_argument(
        "--heartbeat-file",
        type=Path,
        default=Path("/tmp/youtube-api-heartbeat"),
    )
    result.add_argument("--once", action="store_true")
    return result


def _validate(args: argparse.Namespace) -> None:
    for name in ("interval_sec", "maximum_backoff_sec", "timeout_sec"):
        value = float(getattr(args, name))
        if not math.isfinite(value) or value <= 0:
            raise ValueError(f"--{name.replace('_', '-')} must be positive and finite")
    if args.maximum_backoff_sec < args.interval_sec:
        raise ValueError("--maximum-backoff-sec must not be below --interval-sec")
    if not 1024 <= int(args.max_response_bytes) <= 2 * 1024 * 1024:
        raise ValueError("--max-response-bytes must be between 1024 and 2097152")
    validate_distinct_health_files(args.ready_file, args.heartbeat_file)
    validate_distinct_health_files(args.output, args.ready_file)
    validate_distinct_health_files(args.output, args.heartbeat_file)
    try:
        credentials_root = args.credentials_dir.resolve(strict=False)
        targets = (
            args.output.resolve(strict=False),
            args.ready_file.resolve(strict=False),
            args.heartbeat_file.resolve(strict=False),
        )
    except (OSError, RuntimeError) as exc:
        raise ValueError("collector paths cannot be resolved safely") from exc
    if any(target == credentials_root or target.is_relative_to(credentials_root) for target in targets):
        raise ValueError("collector output and health files must be outside credentials-dir")


def _health(path: Path, now_ts: int) -> None:
    atomic_write_text(path, f"{now_ts}\n", encoding="ascii", mode=0o600)


def _remove(path: Path) -> None:
    try:
        path.unlink()
    except FileNotFoundError:
        pass


def _safe_summary(payload: dict[str, object]) -> dict[str, object]:
    return {
        "schema": "monitoring_v4.youtube_api_collector_cycle.v1",
        "collected_at": payload["collected_at"],
        "probe_status": payload["probe_status"],
        "result_kind": payload["result_kind"],
        "http_status": payload["http_status"],
        "error_reason": payload["error_reason"],
        "api_request_count": payload["api_request_count"],
        "oauth_refresh_performed": payload["oauth_refresh_performed"],
        "oauth_scope_class": payload["oauth_scope_class"],
        "oauth_scope_count": payload["oauth_scope_count"],
    }


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    _validate(args)
    credentials = read_credentials(args.credentials_dir)
    collector = YouTubeApiCollector(
        credentials,
        timeout_sec=args.timeout_sec,
        max_response_bytes=args.max_response_bytes,
    )
    stopping = False

    def stop(_signum: int, _frame: object) -> None:
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    failures = 0
    while not stopping:
        evidence = collector.collect()
        payload = evidence.to_dict()
        atomic_write_text(
            args.output,
            json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False)
            + "\n",
            # The host sentinel runs capability-free as group yuki. The
            # sanitized evidence is group-readable but never group-writable.
            mode=0o640,
        )
        completed_ts = int(time.time())
        _health(args.heartbeat_file, completed_ts)
        if evidence.probe_status == "ok":
            failures = 0
            _health(args.ready_file, completed_ts)
        else:
            failures += 1
            _remove(args.ready_file)
        print(
            json.dumps(
                _safe_summary(payload),
                sort_keys=True,
                separators=(",", ":"),
            ),
            flush=True,
        )
        if args.once:
            return 0 if evidence.probe_status == "ok" else 1
        multiplier = 2 ** min(max(0, failures - 1), 8)
        delay = min(args.maximum_backoff_sec, args.interval_sec * multiplier)
        deadline = time.monotonic() + delay
        while not stopping and time.monotonic() < deadline:
            time.sleep(min(0.5, deadline - time.monotonic()))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
