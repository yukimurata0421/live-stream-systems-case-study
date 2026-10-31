#!/usr/bin/env python3
"""Regenerate checked-in clock dependencies from one canonical public source."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import tempfile

ROOT = Path(__file__).resolve().parents[2]
SOURCE = 'ops/selfheal/clock_contract.py'
COPIES = (
    'monitoring-v4/src/stream_monitoring_v4/compatibility/clock_contract.py',
    'ops/public-publisher/site/scripts/clock_contract.py',
)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument('--check', action='store_true', help='Check copies without changing files (default)')
    mode.add_argument('--write', action='store_true', help='Regenerate the two known dependency files')
    args = parser.parse_args()
    source = ROOT / SOURCE
    if source.is_symlink():
        raise SystemExit('canonical_clock_symlink_refused')
    raw = source.read_bytes()
    rows = []
    for name in COPIES:
        target = ROOT / name
        if target.is_symlink() or not target.resolve().is_relative_to(ROOT.resolve()):
            raise SystemExit('clock_copy_symlink_refused')
        matches = target.is_file() and target.read_bytes() == raw
        if args.write and not matches:
            target.parent.mkdir(parents=True, exist_ok=True)
            fd, temporary = tempfile.mkstemp(prefix='.clock-contract-', dir=target.parent)
            try:
                with os.fdopen(fd, 'wb') as handle:
                    os.fchmod(handle.fileno(), 0o644)
                    handle.write(raw)
                    handle.flush()
                    os.fsync(handle.fileno())
                os.replace(temporary, target)
            finally:
                Path(temporary).unlink(missing_ok=True)
            matches = True
        rows.append({'path': name, 'matches': matches})
    passed = all(row['matches'] for row in rows)
    print(json.dumps({'status': 'PASS' if passed else 'FAIL', 'source': SOURCE,
                      'sha256': hashlib.sha256(raw).hexdigest(), 'copies': rows}))
    return 0 if passed else 1


if __name__ == '__main__':
    raise SystemExit(main())
