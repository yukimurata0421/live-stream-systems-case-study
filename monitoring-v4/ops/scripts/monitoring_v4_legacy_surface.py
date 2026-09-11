#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

from stream_monitoring_v4.inventory.legacy_surface import (  # noqa: E402
    evaluate_legacy_surface,
    load_legacy_surface_contract,
    scan_incident_templates,
    scan_prometheus_alerts,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Verify complete V3 monitoring target disposition")
    parser.add_argument("--v3-root", type=Path, required=True)
    parser.add_argument(
        "--contract",
        type=Path,
        default=ROOT / "config" / "monitoring_v4_legacy_surface_contract.json",
    )
    parser.add_argument("--require-cutover-ready", action="store_true")
    args = parser.parse_args(argv)
    v3 = args.v3_root.resolve()
    report = evaluate_legacy_surface(
        load_legacy_surface_contract(args.contract),
        observed_alerts=scan_prometheus_alerts(
            (v3 / "ops" / "monitoring" / "prometheus" / "rules").glob("*.yml")
        ),
        observed_incident_templates=scan_incident_templates(
            v3 / "src" / "stream_core" / "notifications" / "incidents.py"
        ),
    )
    print(json.dumps(report, ensure_ascii=False, sort_keys=True, separators=(",", ":")))
    if not report["static_ok"]:
        return 1
    if args.require_cutover_ready and not report["cutover_ready"]:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
