from __future__ import annotations

import argparse
import json
from pathlib import Path

from cra_harness.controls.sqlite_runtime import fixed_sqlite_failure_message, inspect_fixed_sqlite_runtime


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify the project-local SQLite identity used for CRA full regression")
    parser.add_argument("--project-root", type=Path, required=True)
    args = parser.parse_args()
    identity = inspect_fixed_sqlite_runtime(args.project_root)
    print(json.dumps(identity.to_dict(), sort_keys=True))
    if identity.passed:
        return 0
    print(fixed_sqlite_failure_message(identity))
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
