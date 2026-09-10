from __future__ import annotations

import argparse
import json
from pathlib import Path

from cra_dell_recovery.time import utc_now
from cra_harness.runner.suite_v2 import run_suite_v2


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the CRA Pre-Phase5 Harness v2 trust suite")
    parser.add_argument("--project-root", type=Path, default=Path.cwd())
    parser.add_argument("--artifact-root", type=Path, default=Path("artifacts/harness"))
    parser.add_argument("--run-id", default=f"v2-{utc_now().strftime('%Y%m%dT%H%M%SZ')}")
    args = parser.parse_args()
    project_root = args.project_root.resolve()
    artifact_root = args.artifact_root
    if not artifact_root.is_absolute():
        artifact_root = project_root / artifact_root
    run_dir = run_suite_v2(project_root, artifact_root, args.run_id)
    summary = json.loads((run_dir / "summary.json").read_text(encoding="utf-8"))
    print(json.dumps({"artifact": str(run_dir), **summary}, ensure_ascii=False, sort_keys=True))
    return 0 if summary["harness_v2_trusted"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
