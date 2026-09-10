from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from cra_harness.delayed_reconciliation import run_delayed_reconciliation_campaign


def main() -> None:
    parser = argparse.ArgumentParser(description="Run isolated delayed-effect reconciliation chaos")
    parser.add_argument("--cases", type=int, default=192)
    parser.add_argument("--seed", type=int, default=20260901)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    report = run_delayed_reconciliation_campaign(case_count=args.cases, seed=args.seed)
    encoded = json.dumps(report, indent=2, sort_keys=True) + "\n"
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        temporary = args.output.with_name(f".{args.output.name}.{os.getpid()}.tmp")
        temporary.write_text(encoded, encoding="utf-8")
        os.replace(temporary, args.output)
    print(json.dumps(report, separators=(",", ":"), sort_keys=True))
    if not report["pass"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
