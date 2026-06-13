from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping, Sequence


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
    source_state_root = env_path_arg(source, "STREAM_V2_SOURCE_STATE_ROOT", str(root / ".state" / "source-v2-readonly"))
    python_bin = source.get("PYTHON_BIN", sys.executable)
    stream_cli = source.get("STREAM_V3_STREAM_CLI_BIN", str(root / "bin" / "stream-prod"))
    supervisor_mode = source.get("STREAM_RUNTIME_SUPERVISOR", "systemd").strip().lower() or "systemd"
    supervisor_args = () if supervisor_mode == "systemd" else ("--supervisor-mode", supervisor_mode)
    timeout = env_float(source, "V3_CONTROL_TASK_TIMEOUT_SEC", 45.0)
    shadow_interval = max(5.0, env_float(source, "V3_SHADOW_INTERVAL_SEC", 60.0))
    status_interval = max(5.0, env_float(source, "V3_SUBSYSTEMS_STATUS_INTERVAL_SEC", 60.0))
    recovery_interval = max(5.0, env_float(source, "V3_RECOVERY_ORCHESTRATOR_INTERVAL_SEC", 60.0))
    shadow_sli_interval = max(60.0, env_float(source, "V3_SHADOW_SLI_INTERVAL_SEC", 300.0))
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
            name="shadow_sli",
            interval_sec=shadow_sli_interval,
            timeout_sec=timeout,
            command=(stream_cli, "shadow-sli", "--json"),
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
    stream_watchdog_interval = max(5.0, env_float(source, "V3_STREAM_WATCHDOG_INTERVAL_SEC", 60.0))
    notify_interval = max(30.0, env_float(source, "V3_NOTIFY_INTERVAL_SEC", 60.0))
    subsystems_interval = max(30.0, env_float(source, "V3_SUBSYSTEMS_STATUS_INTERVAL_SEC", 60.0))
    recovery_interval = max(30.0, env_float(source, "V3_RECOVERY_ORCHESTRATOR_INTERVAL_SEC", 60.0))
    shadow_sli_interval = max(60.0, env_float(source, "V3_SHADOW_SLI_INTERVAL_SEC", 300.0))

    return [
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
        ControlTask(
            name="shadow_sli",
            interval_sec=shadow_sli_interval,
            timeout_sec=timeout,
            command=(stream_cli, "shadow-sli", "--json"),
        ),
    ]


def cutover_tasks(source: Mapping[str, str]) -> list[ControlTask]:
    return [*streaming_tasks(source), *monitor_tasks(source)]


def run_task(task: ControlTask, *, env: Mapping[str, str] | None = None) -> TaskResult:
    started = time.monotonic()
    try:
        completed = subprocess.run(
            list(task.command),
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=task.timeout_sec,
            check=False,
            env=dict(os.environ if env is None else env),
        )
    except subprocess.TimeoutExpired as exc:
        duration = time.monotonic() - started
        return TaskResult(
            name=task.name,
            command=task.command,
            returncode=124,
            duration_sec=duration,
            stdout_tail=tail(exc.stdout or ""),
            stderr_tail=tail((exc.stderr or "") + "\ntimeout"),
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


def tail(text: str, *, limit: int = 1000) -> str:
    text = text.strip()
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
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)


def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def run_once(
    tasks: Sequence[ControlTask],
    *,
    state_file: Path,
    event_log: Path,
    mode: str = "shadow",
    env: Mapping[str, str] | None = None,
) -> list[TaskResult]:
    results = [run_task(task, env=env) for task in tasks]
    payload = {
        "ts_utc": iso_now(),
        "mode": mode,
        "results": [result.to_dict() for result in results],
        "ok": all(result.ok for result in results),
    }
    append_event(event_log, payload)
    write_state(state_file, payload)
    return results


def run_loop(
    tasks: Sequence[ControlTask],
    *,
    state_file: Path,
    event_log: Path,
    mode: str = "shadow",
    env: Mapping[str, str] | None = None,
) -> int:
    next_due = {task.name: 0.0 for task in tasks}
    while True:
        now = time.monotonic()
        due = [task for task in tasks if now >= next_due[task.name]]
        if due:
            for task in due:
                result = run_task(task, env=env)
                payload = {
                    "ts_utc": iso_now(),
                    "mode": mode,
                    "results": [result.to_dict()],
                    "ok": result.ok,
                }
                append_event(event_log, payload)
                write_state(state_file, payload)
                next_due[task.name] = time.monotonic() + task.interval_sec
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
