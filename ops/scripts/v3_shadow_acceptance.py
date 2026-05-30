#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Mapping


ROOT = Path(__file__).resolve().parents[2]


@dataclass(frozen=True)
class AcceptanceCheck:
    name: str
    ok: bool
    detail: str
    command: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, object]:
        return {
            "name": self.name,
            "ok": self.ok,
            "detail": self.detail,
            "command": list(self.command),
        }


Runner = Callable[[list[str], Mapping[str, str]], subprocess.CompletedProcess[str]]


def run_command(command: list[str], env: Mapping[str, str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, cwd=ROOT, env=dict(env), text=True, capture_output=True, check=False, timeout=90)


def acceptance(
    *,
    state_root: Path,
    source_state_root: Path,
    runner: Runner = run_command,
    base_env: Mapping[str, str] | None = None,
) -> dict[str, object]:
    env = dict(os.environ if base_env is None else base_env)
    env.update(
        {
            "PYTHONPATH": str(ROOT / "src"),
            "STREAM_V3_MODE": "shadow",
            "STREAM_RUNTIME_SUPERVISOR": "k8s",
            "STREAM_K8S_DRY_RUN": "1",
            "STREAM_V3_CUTOVER_ENABLE": "0",
            "STREAM_RUNTIME_STATE_DIR": str(state_root),
            "STREAM_V2_STATE_ROOT": str(state_root),
            "STREAM_V2_SOURCE_STATE_ROOT": str(source_state_root),
        }
    )

    checks: list[AcceptanceCheck] = []
    checks.append(_run_simple_check("manifest:shadow", ["python3", "ops/scripts/validate_k3s_manifests.py"], env, runner))

    control_command = [
        sys.executable,
        "-m",
        "stream_v3.control_loop",
        "--once",
        "--only",
        "shadow_once",
        "--only",
        "subsystems_status",
        "--only",
        "recovery_orchestrator",
        "--only",
        "shadow_sli",
    ]
    control = runner(control_command, env)
    checks.append(_control_loop_check(control_command, control))
    checks.append(_action_plan_check(state_root / "recovery_action_plan.json"))

    ok = all(check.ok for check in checks)
    return {
        "repo_root": str(ROOT),
        "state_root": str(state_root),
        "source_state_root": str(source_state_root),
        "mode": "shadow",
        "ok": ok,
        "checks": [check.to_dict() for check in checks],
    }


def _run_simple_check(
    name: str,
    command: list[str],
    env: Mapping[str, str],
    runner: Runner,
) -> AcceptanceCheck:
    try:
        cp = runner(command, env)
    except (OSError, subprocess.TimeoutExpired) as exc:
        return AcceptanceCheck(name=name, ok=False, detail=str(exc), command=tuple(command))
    detail = _first_line(cp.stdout or cp.stderr or f"rc={cp.returncode}")
    return AcceptanceCheck(name=name, ok=cp.returncode == 0, detail=detail, command=tuple(command))


def _control_loop_check(command: list[str], cp: subprocess.CompletedProcess[str]) -> AcceptanceCheck:
    if cp.returncode != 0:
        return AcceptanceCheck(
            name="control-loop:shadow-once",
            ok=False,
            detail=_first_line(cp.stderr or cp.stdout or f"rc={cp.returncode}"),
            command=tuple(command),
        )
    try:
        payload = json.loads(cp.stdout)
    except json.JSONDecodeError as exc:
        return AcceptanceCheck(
            name="control-loop:shadow-once",
            ok=False,
            detail=f"invalid json: {exc}",
            command=tuple(command),
        )
    results = payload.get("results") if isinstance(payload, dict) else None
    if not isinstance(results, list) or not results:
        return AcceptanceCheck(
            name="control-loop:shadow-once",
            ok=False,
            detail="missing results",
            command=tuple(command),
        )
    failed = [str(item.get("name", "<unknown>")) for item in results if isinstance(item, dict) and not item.get("ok")]
    if failed:
        return AcceptanceCheck(
            name="control-loop:shadow-once",
            ok=False,
            detail="failed tasks: " + ",".join(failed),
            command=tuple(command),
        )
    return AcceptanceCheck(
        name="control-loop:shadow-once",
        ok=True,
        detail="tasks ok: " + ",".join(str(item.get("name", "<unknown>")) for item in results if isinstance(item, dict)),
        command=tuple(command),
    )


def _action_plan_check(path: Path) -> AcceptanceCheck:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except OSError as exc:
        return AcceptanceCheck(name="action-plan:shadow-safe", ok=False, detail=str(exc))
    except json.JSONDecodeError as exc:
        return AcceptanceCheck(name="action-plan:shadow-safe", ok=False, detail=f"invalid json: {exc}")

    execute = bool(payload.get("execute"))
    executable = bool(payload.get("executable"))
    blocked_by = payload.get("blocked_by")
    blockers = [str(item) for item in blocked_by] if isinstance(blocked_by, list) else []
    if execute:
        return AcceptanceCheck(
            name="action-plan:shadow-safe",
            ok=False,
            detail=f"shadow action unexpectedly set execute=true executable={executable}",
        )
    if "shadow_mode" not in blockers:
        return AcceptanceCheck(name="action-plan:shadow-safe", ok=False, detail="missing shadow_mode blocker")
    return AcceptanceCheck(name="action-plan:shadow-safe", ok=True, detail="execute=false with shadow_mode blocker")


def _first_line(text: str) -> str:
    for line in text.strip().splitlines():
        if line.strip():
            return line.strip()
    return ""


def render_text(report: dict[str, object]) -> str:
    lines = [f"[v3-shadow-acceptance] ok={str(report.get('ok')).lower()} state={report.get('state_root')}"]
    checks = report.get("checks") if isinstance(report.get("checks"), list) else []
    for item in checks:
        if not isinstance(item, dict):
            continue
        status = "ok" if item.get("ok") else "fail"
        lines.append(f"[{status}] {item.get('name')}: {item.get('detail')}")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run stream_v3 local shadow acceptance without production mutation.")
    parser.add_argument("--json", action="store_true", help="print machine-readable result")
    parser.add_argument(
        "--state-root",
        type=Path,
        default=ROOT / ".state" / "adsb-streamnew-v3",
        help="v3 state root written by the acceptance run",
    )
    parser.add_argument(
        "--source-state-root",
        type=Path,
        default=ROOT / ".state" / "source-v2-readonly",
        help="read-only v2 production state mirror/source",
    )
    args = parser.parse_args(argv)

    report = acceptance(state_root=args.state_root.expanduser(), source_state_root=args.source_state_root.expanduser())
    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    else:
        print(render_text(report))
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
