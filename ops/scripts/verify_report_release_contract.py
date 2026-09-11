#!/usr/bin/env python3
"""Verify report-only systemd/CLI compatibility before a release switch."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path


SCRIPT_PATH = Path(__file__).resolve()
ROOT = next(
    (parent for parent in SCRIPT_PATH.parents if (parent / "ops" / "systemd").is_dir()),
    Path.cwd(),
)


class ContractError(RuntimeError):
    """Raised when a candidate cannot satisfy the installed report contract."""


@dataclass(frozen=True)
class ReportContract:
    command: str
    unit: str
    target_flag: str
    target_value: str


REPORT_CONTRACTS = (
    ReportContract(
        command="stream1090-report",
        unit="adsb-streamnew-stream1090-report.service",
        target_flag="--base-url",
        target_value="http://report-contract.invalid",
    ),
    ReportContract(
        command="upstream-report",
        unit="adsb-streamnew-upstream-report.service",
        target_flag="--upstream-url",
        target_value="http://report-contract.invalid/stream1090/",
    ),
)


def _run(command: list[str], *, env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        check=False,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=15,
    )


def verify_release(release: Path, unit_root: Path) -> dict[str, object]:
    release = release.resolve()
    launcher = release / "bin" / "stream-prod"
    source_root = release / "src"
    parser_source = source_root / "stream_core" / "cli_support" / "parser.py"
    router_source = source_root / "stream_core" / "cli_support" / "router.py"
    for required in (launcher, parser_source, router_source):
        if not required.is_file():
            raise ContractError(f"candidate file missing: {required}")

    env = os.environ.copy()
    env["PYTHONPATH"] = str(source_root)
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    checks: list[dict[str, object]] = []

    for contract in REPORT_CONTRACTS:
        unit_path = unit_root / contract.unit
        if not unit_path.is_file():
            raise ContractError(f"unit missing: {unit_path}")
        unit_text = unit_path.read_text(encoding="utf-8")
        expected_fragment = f"{contract.command} --record {contract.target_flag}"
        if expected_fragment not in unit_text:
            raise ContractError(f"unit does not declare explicit record target: {contract.unit}")

        help_result = _run([str(launcher), contract.command, "--help"], env=env)
        if help_result.returncode != 0:
            raise ContractError(f"candidate help failed for {contract.command}: rc={help_result.returncode}")
        help_output = help_result.stdout + help_result.stderr
        if "--record" not in help_output or "--no-record" not in help_output:
            raise ContractError(f"candidate record flags missing for {contract.command}")

        parse_code = (
            "import sys; "
            "from stream_core.cli_support.parser import build_parser; "
            "a=build_parser().parse_args(sys.argv[1:]); "
            "assert a.record is True and a.no_record is False"
        )
        parse_result = _run(
            [
                sys.executable,
                "-c",
                parse_code,
                contract.command,
                "--record",
                contract.target_flag,
                contract.target_value,
            ],
            env=env,
        )
        if parse_result.returncode != 0:
            raise ContractError(f"candidate parser rejected installed contract: {contract.command}")

        missing_target_result = _run([str(launcher), contract.command, "--record"], env=env)
        if missing_target_result.returncode != 2:
            raise ContractError(
                f"explicit record without target was not rejected: {contract.command} "
                f"rc={missing_target_result.returncode}"
            )

        checks.append(
            {
                "command": contract.command,
                "unit": contract.unit,
                "explicit_record_parse": "PASS",
                "missing_target_rejection": "PASS",
                "physical_probe_executed": False,
            }
        )

    return {
        "schema_version": "stream_v3.report_release_contract.v1",
        "release": str(release),
        "unit_root": str(unit_root.resolve()),
        "result": "PASS",
        "production_behavior_modified": False,
        "checks": checks,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--release", type=Path, required=True)
    parser.add_argument("--unit-root", type=Path, default=ROOT / "ops" / "systemd")
    args = parser.parse_args()
    try:
        result = verify_release(args.release, args.unit_root)
    except ContractError as exc:
        print(json.dumps({"result": "FAIL", "reason": str(exc)}, ensure_ascii=False, sort_keys=True))
        return 1
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
