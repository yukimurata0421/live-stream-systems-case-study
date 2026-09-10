from __future__ import annotations

import argparse
import os
import time
import uuid
from contextlib import suppress
from pathlib import Path

from snapshot_projection.model import ProjectionProjector


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--sequence-state", type=Path, required=True)
    parser.add_argument("--producer-id", required=True)
    parser.add_argument("--producer-instance-state", type=Path, required=True)
    parser.add_argument("--expected-source-producer-id", required=True)
    parser.add_argument("--ttl-seconds", type=float, default=5.0)
    parser.add_argument("--interval-seconds", type=float, default=1.0)
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()
    args.producer_instance_state.parent.mkdir(parents=True, exist_ok=True)
    try:
        instance_id = args.producer_instance_state.read_text(encoding="utf-8").strip()
    except OSError:
        instance_id = ""
    if not instance_id:
        instance_id = f"snapshot-projector-{uuid.uuid4()}"
        temporary = args.producer_instance_state.with_name(f".{args.producer_instance_state.name}.{os.getpid()}.tmp")
        temporary.write_text(instance_id + "\n", encoding="utf-8")
        os.replace(temporary, args.producer_instance_state)
    projector = ProjectionProjector(
        source_path=args.source,
        output_path=args.output,
        sequence_path=args.sequence_state,
        producer_id=args.producer_id,
        producer_instance_id=instance_id,
        expected_source_producer_id=args.expected_source_producer_id,
        ttl_seconds=args.ttl_seconds,
    )
    while True:
        with suppress(OSError, TypeError, ValueError):
            projector.publish()
        # The previous projection naturally expires.  Never extend freshness
        # when the source is unavailable or invalid.
        if args.once:
            return 0
        time.sleep(max(0.1, args.interval_seconds))


if __name__ == "__main__":
    raise SystemExit(main())
