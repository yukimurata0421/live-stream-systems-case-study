from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping, Sequence


CONTROL_STATE_SCHEMA = "stream_v3.control_loop_state.v2"


@dataclass(frozen=True)
class ControlTask:
    name: str
    interval_sec: float
    command: tuple[str, ...]
    timeout_sec: float = 45.0


@dataclass(frozen=True)
class TaskResult:
    name: str
    command: tuple[str, ...]
    returncode: int
    duration_sec: float
    stdout_tail: str
    stderr_tail: str

    @property
    def ok(self) -> bool:
        return self.returncode == 0

    def to_dict(self) -> dict[str, object]:
        return {
            "name": self.name,
            "command": list(self.command),
            "returncode": self.returncode,
            "ok": self.ok,
            "duration_sec": round(self.duration_sec, 3),
            "stdout_tail": self.stdout_tail,
            "stderr_tail": self.stderr_tail,
        }


def env_path(source: Mapping[str, str], name: str, default: str) -> Path:
    return Path(source.get(name, default)).expanduser()


def env_path_arg(source: Mapping[str, str], name: str, default: str) -> str:
    raw = source.get(name)
    if raw is not None:
        return Path(raw).expanduser().as_posix()
    return str(Path(default).expanduser())


def env_float(source: Mapping[str, str], name: str, default: float) -> float:
    try:
        return float(source.get(name, str(default)).strip())
    except (AttributeError, ValueError):
        return default


def env_bool(source: Mapping[str, str], name: str, default: bool = False) -> bool:
    value = source.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def default_tasks(env: Mapping[str, str] | None = None, *, mode: str | None = None) -> list[ControlTask]:
    source = os.environ if env is None else env
    selected_mode = (mode or source.get("STREAM_V3_MODE", "shadow")).strip().lower() or "shadow"
    if selected_mode in {"streaming", "streaming-only", "streaming_only"}:
        return streaming_tasks(source)
    if selected_mode in {"monitor", "observability-monitor", "observability_monitor"}:
        return monitor_tasks(source)
    if selected_mode in {"cutover", "production"}:
        return cutover_tasks(source)
    return shadow_tasks(source)


def shadow_tasks(source: Mapping[str, str]) -> list[ControlTask]:
    root = repo_root()
    state_root = env_path_arg(source, "STREAM_RUNTIME_STATE_DIR", str(root / ".state" / "adsb-streamnew-v3"))
    source_state_root = env_path_arg(
        source,
        "STREAM_V2_SOURCE_STATE_ROOT",
        str(root / ".state" / "source-v2-readonly"),
    )
    python_bin = source.get("PYTHON_BIN", sys.executable)
    stream_cli = source.get("STREAM_V3_STREAM_CLI_BIN", str(root / "bin" / "stream-prod"))
    supervisor_mode = source.get("STREAM_RUNTIME_SUPERVISOR", "systemd").strip().lower() or "systemd"
    supervisor_args = () if supervisor_mode == "systemd" else ("--supervisor-mode", supervisor_mode)
    timeout = env_float(source, "V3_CONTROL_TASK_TIMEOUT_SEC", 45.0)
    shadow_interval = max(5.0, env_float(source, "V3_SHADOW_INTERVAL_SEC", 60.0))
    status_interval = max(5.0, env_float(source, "V3_SUBSYSTEMS_STATUS_INTERVAL_SEC", 60.0))
    recovery_interval = max(5.0, env_float(source, "V3_RECOVERY_ORCHESTRATOR_INTERVAL_SEC", 60.0))
    shadow_sli_interval = max(60.0, env_float(source, "V3_SHADOW_SLI_INTERVAL_SEC", 300.0))
    shadow_sli_timeout = max(timeout, env_float(source, "V3_SHADOW_SLI_TIMEOUT_SEC", 120.0))
    summary_interval = max(shadow_interval, env_float(source, "V3_OPS_SUMMARY_INTERVAL_SEC", 300.0))
    notify_interval = max(60.0, env_float(source, "V3_NOTIFY_DRY_RUN_INTERVAL_SEC", 300.0))

    tasks = [
        ControlTask(
            name="shadow_once",
            interval_sec=shadow_interval,
            timeout_sec=timeout,
            command=(
                python_bin,
                "-m",
                "stream_v2",
                "shadow-once",
                "--source-state-root",
                source_state_root,
                "--state-root",
                state_root,
                "--mode",
                "shadow",
                *supervisor_args,
            ),
        ),
        ControlTask(
            name="subsystems_status",
            interval_sec=status_interval,
            timeout_sec=timeout,
            command=(stream_cli, "subsystems-status", "--json"),
        ),
        ControlTask(
            name="recovery_orchestrator",
            interval_sec=recovery_interval,
            timeout_sec=timeout,
            command=(stream_cli, "recovery-orchestrator", "--json"),
        ),
        ControlTask(
            name="ops_summary",
            interval_sec=summary_interval,
            timeout_sec=timeout,
            command=(
                python_bin,
                "-m",
                "stream_v2",
                "ops-summary",
                "--state-root",
                state_root,
                "--text",
            ),
        ),
    ]
    if env_bool(source, "V3_ENABLE_INLINE_SHADOW_SLI", default=False):
        tasks.insert(
            -1,
            ControlTask(
                name="shadow_sli",
                interval_sec=shadow_sli_interval,
                timeout_sec=shadow_sli_timeout,
                command=(stream_cli, "shadow-sli", "--json"),
            ),
        )
    if env_bool(source, "V3_ENABLE_NOTIFY_DRY_RUN", default=False):
        tasks.append(
            ControlTask(
                name="notify_dry_run",
                interval_sec=notify_interval,
                timeout_sec=timeout,
                command=(stream_cli, "notify-status", "--dry-run"),
            )
        )
    return tasks


def streaming_tasks(source: Mapping[str, str]) -> list[ControlTask]:
    root = repo_root()
    python_bin = source.get("PYTHON_BIN", sys.executable)
    timeout = env_float(source, "V3_CONTROL_TASK_TIMEOUT_SEC", 45.0)
    fast_recovery_interval = max(1.0, env_float(source, "V3_FAST_RECOVERY_INTERVAL_SEC", 10.0))
    return [
        ControlTask(
            name="fast_recovery",
            interval_sec=fast_recovery_interval,
            timeout_sec=timeout,
            command=(python_bin, str(root / "src" / "watchers" / "fast_recovery.py")),
        ),
    ]


def monitor_tasks(source: Mapping[str, str]) -> list[ControlTask]:
    root = repo_root()
    python_bin = source.get("PYTHON_BIN", sys.executable)
    stream_cli = source.get("STREAM_V3_STREAM_CLI_BIN", str(root / "bin" / "stream-prod"))
    timeout = env_float(source, "V3_CONTROL_TASK_TIMEOUT_SEC", 45.0)
    video_resolver_interval = max(1.0, env_float(source, "V3_VIDEO_RESOLVER_INTERVAL_SEC", 5.0))
    youtube_monitor_interval = max(5.0, env_float(source, "V3_YOUTUBE_MONITOR_INTERVAL_SEC", 45.0))
    map_runtime_probe_interval = max(30.0, env_float(source, "V3_MAP_RUNTIME_PROBE_INTERVAL_SEC", 60.0))
    map_runtime_probe_timeout = max(5.0, env_float(source, "V3_MAP_RUNTIME_PROBE_TIMEOUT_SEC", 25.0))
    viewer_synthetic_interval = max(60.0, env_float(source, "V3_VIEWER_SYNTHETIC_INTERVAL_SEC", 300.0))
    viewer_synthetic_timeout = max(15.0, env_float(source, "V3_VIEWER_SYNTHETIC_TIMEOUT_SEC", 75.0))
    stream_watchdog_interval = max(5.0, env_float(source, "V3_STREAM_WATCHDOG_INTERVAL_SEC", 60.0))
    notify_interval = max(30.0, env_float(source, "V3_NOTIFY_INTERVAL_SEC", 60.0))
    subsystems_interval = max(30.0, env_float(source, "V3_SUBSYSTEMS_STATUS_INTERVAL_SEC", 60.0))
    recovery_interval = max(30.0, env_float(source, "V3_RECOVERY_ORCHESTRATOR_INTERVAL_SEC", 60.0))
    shadow_sli_interval = max(60.0, env_float(source, "V3_SHADOW_SLI_INTERVAL_SEC", 300.0))
    shadow_sli_timeout = max(timeout, env_float(source, "V3_SHADOW_SLI_TIMEOUT_SEC", 120.0))

    tasks = [
        ControlTask(
            name="youtube_video_resolver",
            interval_sec=video_resolver_interval,
            timeout_sec=timeout,
            command=(python_bin, str(root / "src" / "watchers" / "youtube_video_id_resolver.py")),
        ),
        ControlTask(
            name="youtube_monitor",
            interval_sec=youtube_monitor_interval,
            timeout_sec=timeout,
            command=(python_bin, str(root / "src" / "watchers" / "youtube_watchdog.py")),
        ),
        ControlTask(
            name="map_runtime_probe",
            interval_sec=map_runtime_probe_interval,
            timeout_sec=map_runtime_probe_timeout,
            command=(python_bin, str(root / "ops" / "scripts" / "stream_v3_map_runtime_probe.py")),
        ),
        ControlTask(
            name="viewer_synthetic_probe",
            interval_sec=viewer_synthetic_interval,
            timeout_sec=viewer_synthetic_timeout,
            command=(python_bin, str(root / "ops" / "scripts" / "stream_v3_viewer_synthetic_probe.py")),
        ),
        ControlTask(
            name="stream_watchdog",
            interval_sec=stream_watchdog_interval,
            timeout_sec=timeout,
            command=(python_bin, str(root / "src" / "watchers" / "stream_watchdog.py")),
        ),
        ControlTask(
            name="notify_status",
            interval_sec=notify_interval,
            timeout_sec=timeout,
            command=(stream_cli, "notify-status"),
        ),
        ControlTask(
            name="subsystems_status",
            interval_sec=subsystems_interval,
            timeout_sec=timeout,
            command=(stream_cli, "subsystems-status", "--json"),
        ),
        ControlTask(
            name="recovery_orchestrator",
            interval_sec=recovery_interval,
            timeout_sec=timeout,
            command=(stream_cli, "recovery-orchestrator", "--json"),
        ),
    ]
    if env_bool(source, "V3_ENABLE_INLINE_SHADOW_SLI", default=False):
        tasks.append(
            ControlTask(
                name="shadow_sli",
                interval_sec=shadow_sli_interval,
                timeout_sec=shadow_sli_timeout,
                command=(stream_cli, "shadow-sli", "--json"),
            )
        )
    return tasks


def cutover_tasks(source: Mapping[str, str]) -> list[ControlTask]:
    return [*streaming_tasks(source), *monitor_tasks(source)]


def run_task(task: ControlTask, *, env: Mapping[str, str] | None = None) -> TaskResult:
    started = time.monotonic()
    try:
        completed = subprocess.run(
            list(task.command),
            text=True,
            encoding="utf-8",
            errors="replace",
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=task.timeout_sec,
            check=False,
            env=dict(os.environ if env is None else env),
        )
    except subprocess.TimeoutExpired as exc:
        duration = time.monotonic() - started
        stderr = output_text(exc.stderr)
        timeout_detail = f"timeout after {task.timeout_sec:g}s"
        stderr = f"{stderr.rstrip()}\n{timeout_detail}" if stderr.strip() else timeout_detail
        return TaskResult(
            name=task.name,
            command=task.command,
            returncode=124,
            duration_sec=duration,
            stdout_tail=tail(exc.stdout),
            stderr_tail=tail(stderr),
        )
    except Exception as exc:
        duration = time.monotonic() - started
        return TaskResult(
            name=task.name,
            command=task.command,
            returncode=125,
            duration_sec=duration,
            stdout_tail="",
            stderr_tail=tail(f"task runner {type(exc).__name__}: {exc}"),
        )
    duration = time.monotonic() - started
    return TaskResult(
        name=task.name,
        command=task.command,
        returncode=completed.returncode,
        duration_sec=duration,
        stdout_tail=tail(completed.stdout),
        stderr_tail=tail(completed.stderr),
    )


def output_text(value: str | bytes | None) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


def tail(text: str | bytes | None, *, limit: int = 1000) -> str:
    text = output_text(text).strip()
    if len(text) <= limit:
        return text
    return text[-limit:]


def append_event(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))
        fh.write("\n")


def write_state(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        with tmp.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, path)
        directory = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        tmp.unlink(missing_ok=True)


def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def parse_utc(value: object) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.astimezone(timezone.utc) if parsed.tzinfo is not None else None


def iso_after(value: str, seconds: float) -> str:
    parsed = parse_utc(value)
    if parsed is None:
        raise ValueError("invalid control-loop timestamp")
    future = datetime.fromtimestamp(
        parsed.timestamp() + max(0.0, seconds),
        tz=timezone.utc,
    )
    return future.isoformat(timespec="seconds").replace("+00:00", "Z")


def task_fingerprint(task: ControlTask) -> str:
    content = json.dumps(
        {
            "name": task.name,
            "interval_sec": task.interval_sec,
            "timeout_sec": task.timeout_sec,
            "command": list(task.command),
        },
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(content).hexdigest()


def _task_initial(task: ControlTask) -> dict[str, object]:
    return {
        "name": task.name,
        "interval_sec": task.interval_sec,
        "timeout_sec": task.timeout_sec,
        "command_sha256": task_fingerprint(task),
        "status": "never_run",
        "run_count": 0,
        "consecutive_failures": 0,
        "last_returncode": None,
        "last_duration_sec": None,
        "last_started_at_utc": None,
        "last_completed_at_utc": None,
        "last_success_at_utc": None,
        "last_failure_at_utc": None,
        "next_due_at_utc": None,
        "fresh_until_utc": None,
    }


def _validated_tasks(tasks: Sequence[ControlTask]) -> tuple[ControlTask, ...]:
    configured = tuple(tasks)
    names = [task.name for task in configured]
    if not configured:
        raise ValueError("control loop requires at least one task")
    if len(names) != len(set(names)):
        raise ValueError("control loop task names must be unique")
    if any(not task.name.strip() or task.interval_sec <= 0 or task.timeout_sec <= 0 for task in configured):
        raise ValueError("control loop task contract is invalid")
    return configured


def load_control_state(
    path: Path,
    tasks: Sequence[ControlTask],
    *,
    mode: str,
    updated_at: str,
) -> dict[str, object]:
    configured = _validated_tasks(tasks)
    previous: dict[str, object] = {}
    try:
        if path.is_symlink():
            raise OSError("control state must not be a symlink")
        raw = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(raw, dict) and raw.get("schema") == CONTROL_STATE_SCHEMA:
            previous = raw
    except (FileNotFoundError, OSError, UnicodeDecodeError, json.JSONDecodeError):
        previous = {}
    previous_tasks = previous.get("tasks") if isinstance(previous.get("tasks"), dict) else {}
    task_states: dict[str, object] = {}
    for task in configured:
        initial = _task_initial(task)
        candidate = previous_tasks.get(task.name) if isinstance(previous_tasks, dict) else None
        if (
            isinstance(candidate, dict)
            and candidate.get("command_sha256") == initial["command_sha256"]
            and candidate.get("interval_sec") == task.interval_sec
            and candidate.get("timeout_sec") == task.timeout_sec
        ):
            for key in initial:
                if key in candidate:
                    initial[key] = candidate[key]
        task_states[task.name] = initial
    state: dict[str, object] = {
        "schema": CONTROL_STATE_SCHEMA,
        "updated_at_utc": updated_at,
        "mode": mode,
        "configured_task_count": len(configured),
        "configured_tasks": [task.name for task in configured],
        "all_tasks_observed": False,
        "ok": False,
        "tasks": task_states,
        "latest_result": None,
    }
    return evaluate_control_state(state, now_at=updated_at)


def evaluate_control_state(
    state: dict[str, object],
    *,
    now_at: str,
) -> dict[str, object]:
    now = parse_utc(now_at)
    if now is None:
        raise ValueError("invalid control-loop evaluation timestamp")
    raw_tasks = state.get("tasks")
    tasks = raw_tasks if isinstance(raw_tasks, dict) else {}
    all_observed = bool(tasks) and all(
        isinstance(item, dict) and item.get("status") in {"good", "failed"}
        for item in tasks.values()
    )
    fresh = all_observed and all(
        (deadline := parse_utc(item.get("fresh_until_utc"))) is not None
        and now <= deadline
        for item in tasks.values()
        if isinstance(item, dict)
    )
    all_good = all_observed and all(
        isinstance(item, dict) and item.get("status") == "good"
        for item in tasks.values()
    )
    state["updated_at_utc"] = now_at
    state["all_tasks_observed"] = all_observed
    state["fresh"] = fresh
    state["ok"] = all_good and fresh
    state["failed_tasks"] = sorted(
        name
        for name, item in tasks.items()
        if isinstance(item, dict) and item.get("status") == "failed"
    )
    state["stale_or_unobserved_tasks"] = sorted(
        name
        for name, item in tasks.items()
        if not isinstance(item, dict)
        or item.get("status") == "never_run"
        or (parse_utc(item.get("fresh_until_utc")) or datetime.min.replace(tzinfo=timezone.utc)) < now
    )
    return state


def record_task_result(
    state: dict[str, object],
    task: ControlTask,
    result: TaskResult,
    *,
    started_at: str,
    completed_at: str,
) -> dict[str, object]:
    raw_tasks = state.get("tasks")
    if not isinstance(raw_tasks, dict) or not isinstance(raw_tasks.get(task.name), dict):
        raise ValueError(f"task is outside the configured state contract: {task.name}")
    item = dict(raw_tasks[task.name])
    previous_failures = item.get("consecutive_failures")
    consecutive_failures = int(previous_failures) if isinstance(previous_failures, int) else 0
    item.update(
        {
            "status": "good" if result.ok else "failed",
            "run_count": max(0, int(item.get("run_count") or 0)) + 1,
            "consecutive_failures": 0 if result.ok else consecutive_failures + 1,
            "last_returncode": result.returncode,
            "last_duration_sec": round(result.duration_sec, 3),
            "last_started_at_utc": started_at,
            "last_completed_at_utc": completed_at,
            "next_due_at_utc": iso_after(completed_at, task.interval_sec),
            "fresh_until_utc": iso_after(
                completed_at,
                task.interval_sec + task.timeout_sec + max(15.0, task.interval_sec * 0.25),
            ),
        }
    )
    if result.ok:
        item["last_success_at_utc"] = completed_at
    else:
        item["last_failure_at_utc"] = completed_at
    raw_tasks[task.name] = item
    state["latest_result"] = {
        "name": result.name,
        "ok": result.ok,
        "returncode": result.returncode,
        "duration_sec": round(result.duration_sec, 3),
        "completed_at_utc": completed_at,
    }
    return evaluate_control_state(state, now_at=completed_at)


def run_once(
    tasks: Sequence[ControlTask],
    *,
    state_file: Path,
    event_log: Path,
    mode: str = "shadow",
    env: Mapping[str, str] | None = None,
) -> list[TaskResult]:
    configured = _validated_tasks(tasks)
    initialized_at = iso_now()
    state = load_control_state(state_file, configured, mode=mode, updated_at=initialized_at)
    results: list[TaskResult] = []
    for task in configured:
        started_at = iso_now()
        result = run_task(task, env=env)
        completed_at = iso_now()
        results.append(result)
        record_task_result(
            state,
            task,
            result,
            started_at=started_at,
            completed_at=completed_at,
        )
    payload = {
        "schema": "stream_v3.control_loop_event.v2",
        "ts_utc": str(state["updated_at_utc"]),
        "mode": mode,
        "results": [result.to_dict() for result in results],
        "ok": all(result.ok for result in results),
    }
    append_event(event_log, payload)
    write_state(state_file, state)
    return results


def run_loop(
    tasks: Sequence[ControlTask],
    *,
    state_file: Path,
    event_log: Path,
    mode: str = "shadow",
    env: Mapping[str, str] | None = None,
    max_task_runs: int | None = None,
) -> int:
    configured = _validated_tasks(tasks)
    state = load_control_state(state_file, configured, mode=mode, updated_at=iso_now())
    next_due = {task.name: 0.0 for task in configured}
    task_runs = 0
    while True:
        now = time.monotonic()
        due = [task for task in configured if now >= next_due[task.name]]
        if due:
            for task in due:
                started_at = iso_now()
                result = run_task(task, env=env)
                completed_at = iso_now()
                payload = {
                    "schema": "stream_v3.control_loop_event.v2",
                    "ts_utc": completed_at,
                    "mode": mode,
                    "results": [result.to_dict()],
                    "ok": result.ok,
                }
                append_event(event_log, payload)
                record_task_result(
                    state,
                    task,
                    result,
                    started_at=started_at,
                    completed_at=completed_at,
                )
                write_state(state_file, state)
                next_due[task.name] = time.monotonic() + task.interval_sec
                task_runs += 1
                if max_task_runs is not None and task_runs >= max_task_runs:
                    return 0
        sleep_sec = min(max(1.0, next_due[name] - time.monotonic()) for name in next_due)
        time.sleep(min(sleep_sec, 5.0))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="stream_v3 shadow control loop")
    parser.add_argument("--once", action="store_true", help="run due shadow tasks once and exit")
    parser.add_argument("--state-file", type=Path, default=None)
    parser.add_argument("--event-log", type=Path, default=None)
    parser.add_argument("--only", action="append", default=[], help="task name to run; can be repeated")
    parser.add_argument(
        "--mode",
        choices=["shadow", "streaming", "monitor", "cutover"],
        default=None,
        help="task set to run; streaming/monitor/cutover require STREAM_V3_CUTOVER_ENABLE=1",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    env = os.environ.copy()
    mode = args.mode or env.get("STREAM_V3_MODE", "shadow").strip().lower() or "shadow"
    if mode == "production":
        mode = "cutover"
    gated_modes = {"streaming", "monitor", "cutover"}
    if mode in gated_modes and not env_bool(env, "STREAM_V3_CUTOVER_ENABLE", default=False):
        print(f"{mode} mode requires STREAM_V3_CUTOVER_ENABLE=1", file=sys.stderr)
        return 2
    root = repo_root()
    state_root = env_path(env, "STREAM_RUNTIME_STATE_DIR", str(root / ".state" / "adsb-streamnew-v3"))
    state_file = args.state_file or state_root / "v3_control_state.json"
    event_log = args.event_log or state_root / "logs" / "v3_control_loop.jsonl"
    tasks = default_tasks(env, mode=mode)
    if args.only:
        wanted = set(args.only)
        tasks = [task for task in tasks if task.name in wanted]
    if not tasks:
        print("no control tasks selected", file=sys.stderr)
        return 2
    if args.once:
        results = run_once(tasks, state_file=state_file, event_log=event_log, mode=mode, env=env)
        print(json.dumps({"results": [result.to_dict() for result in results]}, ensure_ascii=False, separators=(",", ":")))
        return 0 if all(result.ok for result in results) else 1
    return run_loop(tasks, state_file=state_file, event_log=event_log, mode=mode, env=env)


if __name__ == "__main__":
    raise SystemExit(main())
