from __future__ import annotations

import argparse
import importlib.util
import json
import os
import subprocess
import sys
from pathlib import Path
from types import ModuleType
from unittest import mock


def _load(name: str, path: Path) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def _completed(returncode: int, stdout: str = "", stderr: str = "") -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(args=["candidate-gate"], returncode=returncode, stdout=stdout, stderr=stderr)


def _pod_payload(*, auto_dj_restart_count: int = 0) -> dict[str, object]:
    return {
        "items": [
            {
                "metadata": {"name": "stream-v3-runtime-fixture"},
                "status": {
                    "phase": "Running",
                    "containerStatuses": [
                        {
                            "name": "stream-engine",
                            "restartCount": 0,
                            "containerID": "containerd://stream-engine-fixture",
                            "ready": True,
                        },
                        {
                            "name": "auto-dj",
                            "restartCount": auto_dj_restart_count,
                            "containerID": f"containerd://auto-dj-{auto_dj_restart_count}",
                            "ready": True,
                        },
                    ],
                },
            }
        ]
    }


def _remote_trace(module: ModuleType) -> dict[str, object]:
    module.APPLY = True
    module.APPLY_ACTION_PLAN = True
    module.ACTION_PLAN_MAX_AGE_SEC = 300
    plan = {
        "ts_utc": "1970-01-01T00:16:40Z",
        "event_id": "evt-candidate-gate",
        "action": "restart_dj",
        "executable": True,
        "blocked_by": ["shadow_mode"],
        "reason": "shadow_mode_plan_only",
        "steps": [{"description": "Restart only Auto DJ"}],
    }
    state: dict[str, object] = {}
    with (
        mock.patch.object(module, "run", return_value=_completed(0, stdout="ok scoped\n")) as run,
        mock.patch.object(module, "save_state") as save_state,
    ):
        action_ok = module.execute_action_plan(plan, state, 1000)
    action_command = list(run.call_args.args[0])
    if len(action_command) > 1:
        action_command[1] = Path(action_command[1]).name
    restart_state: dict[str, object] = {}
    with (
        mock.patch.object(module, "runtime_startup_restart_blocked", return_value=(False, "")),
        mock.patch.object(module, "runtime_gpu_restart_blocked", return_value=(False, "")),
        mock.patch.object(module, "restart_workload", return_value=True) as restart,
        mock.patch.object(module, "save_state"),
    ):
        restart_ok = module.maybe_restart(
            "deployment/stream-v3-runtime",
            "workload inactive: ready=0",
            restart_state,
            1000,
        )
    return {
        "action_ok": action_ok,
        "action_state": state,
        "action_command": action_command,
        "action_save_calls": save_state.call_count,
        "restart_ok": restart_ok,
        "restart_state": restart_state,
        "restart_call_count": restart.call_count,
        "low_upload_blocker": module.recovery_policy_blocker(
            "deployment/stream-v3-runtime",
            "low_upload_pressure send_mbps=2.1",
        ),
    }


def _scoped_trace(module: ModuleType) -> dict[str, object]:
    before = _pod_payload(auto_dj_restart_count=0)
    after = _pod_payload(auto_dj_restart_count=1)
    with (
        mock.patch.object(module, "runtime_pod_json", side_effect=[before, after]),
        mock.patch.object(module, "exec_in_container", return_value=_completed(143, stderr="terminated")) as dj_exec,
    ):
        dj_rc = module.restart_dj(reason="audio_energy_low confirmed", dry_run=False, timeout_sec=5)

    with (
        mock.patch.object(module, "runtime_pod_json", return_value=before),
        mock.patch.object(
            module,
            "exec_in_container",
            side_effect=[
                _completed(0, stdout="123\n"),
                _completed(0, stdout="terminated_rtmps_ffmpeg_pid=123\n"),
            ],
        ) as ffmpeg_exec,
        mock.patch.object(module, "wait_for_rtmps_ffmpeg_restart", return_value="456"),
    ):
        ffmpeg_rc = module.restart_ffmpeg(reason="tcp_stall", dry_run=False, timeout_sec=5)
    return {
        "dj_rc": dj_rc,
        "dj_effect_command": list(dj_exec.call_args.args),
        "ffmpeg_rc": ffmpeg_rc,
        "ffmpeg_effect_command": list(ffmpeg_exec.call_args_list[1].args),
        "low_upload_blocker": module.guard_reason("low_upload_pressure send_mbps=2.1"),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Compare live-base and P1 audit-only stream_v3 candidate behavior.")
    parser.add_argument("--baseline-root", type=Path, required=True)
    parser.add_argument("--candidate-root", type=Path, required=True)
    args = parser.parse_args()
    os.environ["MAINTENANCE_AUDIT_ENABLED"] = "0"
    baseline_remote = _load(
        "p1_baseline_remote",
        args.baseline_root / "ops/scripts/stream_v3_remote_recovery.py",
    )
    candidate_remote = _load(
        "p1_candidate_remote",
        args.candidate_root / "ops/scripts/stream_v3_remote_recovery.py",
    )
    baseline_scoped = _load(
        "p1_baseline_scoped",
        args.baseline_root / "ops/scripts/stream_v3_scoped_recovery.py",
    )
    candidate_scoped = _load(
        "p1_candidate_scoped",
        args.candidate_root / "ops/scripts/stream_v3_scoped_recovery.py",
    )
    baseline = {
        "remote": _remote_trace(baseline_remote),
        "scoped": _scoped_trace(baseline_scoped),
    }
    candidate = {
        "remote": _remote_trace(candidate_remote),
        "scoped": _scoped_trace(candidate_scoped),
    }
    passed = baseline == candidate
    print(
        json.dumps(
            {
                "schema_version": "p1.live_candidate_equivalence.v1",
                "audit_enabled": False,
                "baseline": baseline,
                "candidate": candidate,
                "production_trace_equal": passed,
                "result": "PASS" if passed else "FAIL",
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
