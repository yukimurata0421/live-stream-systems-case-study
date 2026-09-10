from __future__ import annotations

import argparse
import json
import tempfile
from pathlib import Path

from cra_harness.controls.sqlite_runtime import run_sqlite_probe


def main() -> int:
    parser = argparse.ArgumentParser(description="Exercise the CRA SQLite durability contract")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="cra-sqlite-probe-") as temporary:
        result = run_sqlite_probe(Path(temporary)).to_dict()
    encoded = json.dumps(result, indent=2, sort_keys=True) + "\n"
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(encoded, encoding="utf-8")
    print(encoded, end="")
    return 0 if result["functional_gate_passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
