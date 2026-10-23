from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

from stream_monitoring_v4.history.archive import archive_day
from stream_monitoring_v4.storage.factory import add_repository_arguments, repository_from_args


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description='Copy one old JST day to verified immutable SQLite; never delete source')
    add_repository_arguments(parser)
    parser.add_argument('--received-day-jst', required=True)
    parser.add_argument('--output-dir', type=Path, required=True)
    parser.add_argument('--now-ts', type=int)
    parser.add_argument('--max-bytes', type=int, default=512 * 1024**2)
    args = parser.parse_args(argv)
    repository = repository_from_args(args, application_name='stream-monitoring-v4-archive-copy')
    try:
        receipt = archive_day(repository, day=args.received_day_jst, output_dir=args.output_dir,
                              now_ts=int(time.time()) if args.now_ts is None else args.now_ts,
                              max_bytes=args.max_bytes)
        print(json.dumps(receipt, sort_keys=True, separators=(',', ':')))
        return 0
    finally:
        close = getattr(repository, 'close', None)
        if callable(close):
            close()


if __name__ == '__main__':
    raise SystemExit(main())
