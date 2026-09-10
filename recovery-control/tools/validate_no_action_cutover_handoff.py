from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from cra_harness.cutover_handoff import validate_no_action_cutover_handoff
from cra_no_action_soak.time import parse_utc


def _load(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"CUTOVER_INPUT_NOT_OBJECT:{path}")
    return value


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate an exact arena-to-CRA NO_ACTION cutover handoff")
    parser.add_argument("--arena-adapter", type=Path, required=True)
    parser.add_argument("--arena-producer-status", type=Path, required=True)
    parser.add_argument("--arena-projection", type=Path, required=True)
    parser.add_argument("--cra-pull-status", type=Path, required=True)
    parser.add_argument("--cra-projection", type=Path, required=True)
    parser.add_argument("--expected-arena-release", required=True)
    parser.add_argument("--expected-cra-release", required=True)
    parser.add_argument("--minimum-remaining-seconds", type=float, default=10.0)
    parser.add_argument("--now")
    args = parser.parse_args()
    now = parse_utc(args.now) if args.now else datetime.now(UTC)
    report = validate_no_action_cutover_handoff(
        arena_adapter=_load(args.arena_adapter),
        arena_producer_status=_load(args.arena_producer_status),
        arena_projection=_load(args.arena_projection),
        cra_pull_status=_load(args.cra_pull_status),
        cra_projection=_load(args.cra_projection),
        expected_arena_release=args.expected_arena_release,
        expected_cra_release=args.expected_cra_release,
        now=now,
        minimum_remaining_seconds=args.minimum_remaining_seconds,
    )
    print(json.dumps(report, separators=(",", ":"), sort_keys=True))


if __name__ == "__main__":
    main()
