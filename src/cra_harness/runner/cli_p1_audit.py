from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path

from cra_harness.runner.p1_audit import run_p1_audit_suite


def main() -> int:
    parser = argparse.ArgumentParser(description="Run P1 audit-only production integration Harness")
    parser.add_argument("--project-root", type=Path, default=Path.cwd())
    parser.add_argument("--artifact-root", type=Path, default=Path("artifacts/harness"))
    parser.add_argument("--run-id", default=f"p1-audit-{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}")
    args = parser.parse_args()
    project_root = args.project_root.resolve()
    artifact_root = args.artifact_root if args.artifact_root.is_absolute() else project_root / args.artifact_root
    run_dir = run_p1_audit_suite(project_root, artifact_root, args.run_id)
    summary = json.loads((run_dir / "summary.json").read_text(encoding="utf-8"))
    print(json.dumps({"artifact": str(run_dir), **summary}, ensure_ascii=False, sort_keys=True))
    return 0 if summary["local_p1_harness_trusted"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
