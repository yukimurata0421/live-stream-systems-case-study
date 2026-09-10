from __future__ import annotations

import argparse
import json
from pathlib import Path

from cra_harness.soak_blocker_cardinality import run_blocker_cardinality_chaos


def main() -> int:
    parser = argparse.ArgumentParser(description="Run time-compressed soak blocker cardinality chaos")
    parser.add_argument("--reason-count", type=int, default=64)
    parser.add_argument("--samples-per-reason", type=int, default=4096)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    report = run_blocker_cardinality_chaos(
        reason_count=args.reason_count,
        samples_per_reason=args.samples_per_reason,
    )
    rendered = json.dumps(report, indent=2, sort_keys=True) + "\n"
    if args.output is not None:
        args.output.write_text(rendered, encoding="utf-8")
    print(rendered, end="")
    return 0 if report["classification"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
