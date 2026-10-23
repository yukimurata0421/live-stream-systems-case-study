from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

from stream_monitoring_v4.history.normal import build_normal_report
from stream_monitoring_v4.commands.shadow_report import _write_atomic
from stream_monitoring_v4.runtime.source_revision import normalize_source_revision, read_source_revision
from stream_monitoring_v4.storage.factory import add_repository_arguments, repository_from_args


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description='Report normal monitoring from small durable aggregates; no migration gate')
    add_repository_arguments(parser)
    parser.add_argument('--build-revision', required=True)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument('--source-revision', default='')
    source.add_argument('--source-revision-file', type=Path)
    parser.add_argument('--target', default='stream-v3')
    parser.add_argument('--now-ts', type=int)
    parser.add_argument('--output', type=Path)
    args = parser.parse_args(argv)
    repository = repository_from_args(args, application_name='stream-monitoring-v4-normal-reporter')
    try:
        report = build_normal_report(
            repository, now_ts=int(time.time()) if args.now_ts is None else args.now_ts,
            build_revision=args.build_revision, target=args.target,
            source_revision=normalize_source_revision(args.source_revision) if args.source_revision else read_source_revision(args.source_revision_file),
        )
        if args.output:
            _write_atomic(args.output, report)
        print(json.dumps(report, ensure_ascii=False, sort_keys=True, separators=(',', ':')))
        # The scheduler's readiness file follows this result, not merely JSON generation.
        return 0 if report['healthy'] else 1
    finally:
        close = getattr(repository, 'close', None)
        if callable(close):
            close()


if __name__ == '__main__':
    raise SystemExit(main())
