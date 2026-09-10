from __future__ import annotations

import argparse
import json
from pathlib import Path

from cra_dell_recovery.time import utc_now
from cra_harness.runner.suite import run_suite


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the CRA Protocol v1 trust harness")
    parser.add_argument("--project-root", type=Path, default=Path.cwd())
    parser.add_argument("--artifact-root", type=Path, default=Path("artifacts/harness"))
    parser.add_argument("--run-id", default=f"run-{utc_now().strftime('%Y%m%dT%H%M%SZ')}")
    parser.add_argument("--seed", type=int, default=20260823)
    args = parser.parse_args()
    project_root = args.project_root.resolve()
    artifact_root = args.artifact_root
    if not artifact_root.is_absolute():
        artifact_root = project_root / artifact_root
    run_dir = run_suite(project_root, artifact_root, args.run_id, seed=args.seed)
    summary = json.loads((run_dir / "summary.json").read_text(encoding="utf-8"))
    print(json.dumps({"artifact": str(run_dir), **summary}, ensure_ascii=False, sort_keys=True))
    return 0 if summary["trust_gate"] == "HARNESS_TRUSTED" else 1


if __name__ == "__main__":
    raise SystemExit(main())
