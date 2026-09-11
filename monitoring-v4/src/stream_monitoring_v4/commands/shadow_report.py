from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import Any, Mapping

from stream_monitoring_v4.reporting.builder import build_report
from stream_monitoring_v4.reporting.parity import parity_payload_valid as _parity_payload_valid
from stream_monitoring_v4.reporting.policy import SOURCE_CADENCES
from stream_monitoring_v4.runtime.atomic_file import atomic_write_text
from stream_monitoring_v4.runtime.source_revision import (
    normalize_source_revision,
    read_source_revision,
)
from stream_monitoring_v4.storage.factory import (
    add_repository_arguments,
    repository_from_args,
)


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(
        description="Report revision-pinned Monitoring v4 shadow gates"
    )
    add_repository_arguments(result)
    result.add_argument("--build-revision", required=True)
    source = result.add_mutually_exclusive_group(required=True)
    source.add_argument("--source-revision", default="")
    source.add_argument("--source-revision-file", type=Path, default=None)
    result.add_argument("--now-ts", type=int, default=None)
    result.add_argument("--output", type=Path, default=None)
    result.add_argument("--minimum-coverage-pct", type=float, default=99.0)
    return result


def _write_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    atomic_write_text(
        path,
        json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        + "\n",
        encoding="utf-8",
        # The capability-free host sentinel has read-only access through the
        # operator group.  The report contains no credential or raw payload.
        mode=0o640,
    )


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    now_ts = int(time.time() if args.now_ts is None else args.now_ts)
    source_revision = (
        normalize_source_revision(args.source_revision)
        if args.source_revision
        else read_source_revision(args.source_revision_file)
    )
    repository = repository_from_args(
        args, application_name="stream-monitoring-v4-reporter"
    )
    try:
        report = build_report(
            repository,
            build_revision=args.build_revision,
            source_revision=source_revision,
            now_ts=now_ts,
            minimum_coverage_pct=args.minimum_coverage_pct,
        )
        if args.output is not None:
            _write_atomic(args.output, report)
        print(json.dumps(report, ensure_ascii=False, sort_keys=True, separators=(",", ":")))
        return 0
    finally:
        close = getattr(repository, "close", None)
        if callable(close):
            close()


if __name__ == "__main__":
    raise SystemExit(main())
