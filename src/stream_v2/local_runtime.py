from __future__ import annotations

import json
import os
import shlex
import shutil
import signal
import subprocess
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping


LOCAL_STATE_DIRNAME = "local-run"
DEFAULT_DISPLAY = ":101"
DEFAULT_OVERLAY_PORT = 18081
DEFAULT_PULSE_SINK = "stream_v2_test_sink"
DEFAULT_STREAM1090_URL = "http://stream1090.lan/stream1090/"


def default_repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


@dataclass(frozen=True)
class LocalRuntimeConfig:
    repo_root: Path = field(default_factory=default_repo_root)
    state_root: Path | None = None
    display: str = DEFAULT_DISPLAY
    overlay_port: int = DEFAULT_OVERLAY_PORT
    pulse_sink: str = DEFAULT_PULSE_SINK
    output: str = "null"
    output_file: Path | None = None
    duration_sec: float = 30.0
    keep_running: bool = False
    with_dj: bool = False
    start_browser: bool = True
    dry_run: bool = False
    write_env: bool = False
    env_file: Path | None = None
    stream1090_url: str = field(default_factory=lambda: os.environ.get("STREAM1090_URL", DEFAULT_STREAM1090_URL))
    video_size: str = "1920x1080"
    output_size: str = "1920x1080"
    frame_rate: int = 5
    python_bin: str = field(default_factory=lambda: sys.executable)
    music_root: Path | None = None
    dj_max_track_sec: int = 0
    dj_start_delay_sec: float = 2.0
    dj_restart_delay_sec: float = 3.0


@dataclass(frozen=True)
class LocalRuntimePaths:
    state_root: Path
    logs_dir: Path
    overlay_dir: Path
    now_playing_file: Path
    now_playing_snapshot_file: Path
    play_history_jsonl_file: Path
    runtime_state_file: Path
    event_log_file: Path
    restart_reason_file: Path
    xvfb_log_file: Path
    browser_log_file: Path
    overlay_server_log_file: Path
    browser_profile_dir: Path
    lock_dir: Path
    capture_dir: Path
    output_file: Path
    env_file: Path
    music_root: Path


def resolve_paths(config: LocalRuntimeConfig) -> LocalRuntimePaths:
    repo_root = config.repo_root.resolve()
    state_root = (config.state_root or (repo_root / ".state" / LOCAL_STATE_DIRNAME)).resolve()
    logs_dir = state_root / "logs"
    overlay_dir = state_root / "overlay"
    capture_dir = state_root / "capture"
    output_file = (config.output_file or (capture_dir / "stream_v2_local_test.mkv")).resolve()
    env_file = (config.env_file or (state_root / "local.env")).resolve()
    return LocalRuntimePaths(
        state_root=state_root,
        logs_dir=logs_dir,
        overlay_dir=overlay_dir,
        now_playing_file=state_root / "now_playing.txt",
        now_playing_snapshot_file=overlay_dir / "now_playing.json",
        play_history_jsonl_file=logs_dir / "play_history.jsonl",
        runtime_state_file=state_root / "stream_runtime_state.json",
        event_log_file=logs_dir / "stream_engine_events.jsonl",
        restart_reason_file=state_root / "restart_reason.json",
        xvfb_log_file=logs_dir / "xvfb.log",
        browser_log_file=logs_dir / "browser.log",
        overlay_server_log_file=logs_dir / "overlay_server.log",
        browser_profile_dir=state_root / "chromium_profile",
        lock_dir=state_root / "locks",
        capture_dir=capture_dir,
        output_file=output_file,
        env_file=env_file,
        music_root=(config.music_root or (repo_root / "ncs_music" / "time_tags")).resolve(),
    )


def _snapshot_payload(title: str) -> dict[str, object]:
    return {
        "schema": "now_playing_snapshot/v1",
        "event_id": "evt-local-runtime-seed",
        "run_id": "stream_v2-local-runtime",
        "sequence": 0,
        "updated_at_utc": utc_now(),
        "status": "local_ready",
        "note": "seeded by stream_v2 local runtime",
        "now_playing": {
            "title": title,
            "title_line": f"Now Playing: {title}",
            "bucket": "local",
            "prefix": "",
            "source_filename": "",
            "source_path": "",
        },
        "player": {
            "name": "none",
            "force_pulse_ao": False,
            "pulse_sink": "",
            "last_exit_code": None,
        },
        "retry": {
            "attempt": 0,
            "max_attempts": 0,
            "sleep_after_failure_sec": 0,
        },
    }


def prepare_local_runtime(config: LocalRuntimeConfig) -> LocalRuntimePaths:
    paths = resolve_paths(config)
    for directory in (
        paths.state_root,
        paths.logs_dir,
        paths.overlay_dir,
        paths.browser_profile_dir,
        paths.lock_dir,
        paths.capture_dir,
    ):
        directory.mkdir(parents=True, exist_ok=True)

    source_overlay = config.repo_root.resolve() / "ui" / "overlay"
    if source_overlay.exists():
        shutil.copytree(source_overlay, paths.overlay_dir, dirs_exist_ok=True)

    title = "stream_v2 local smoke test"
    paths.now_playing_file.write_text(f"Now Playing: {title}\n", encoding="utf-8")
    paths.now_playing_snapshot_file.write_text(
        json.dumps(_snapshot_payload(title), ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )
    return paths


def build_local_env(config: LocalRuntimeConfig) -> dict[str, str]:
    repo_root = config.repo_root.resolve()
    paths = resolve_paths(config)
    output = config.output.strip().lower()
    if output not in {"null", "file"}:
        raise ValueError("local runtime output must be 'null' or 'file'")
    return {
        "STREAM_BASE_DIR": str(repo_root),
        "BASE_DIR": str(repo_root),
        "PYTHONPATH": str(repo_root / "src"),
        "STREAM_RUNTIME_STATE_DIR": str(paths.state_root),
        "STREAM_RUNTIME_LOG_DIR": str(paths.logs_dir),
        "TEST_MODE": "1",
        "TEST_OUTPUT": output,
        "TEST_OUTPUT_FILE": str(paths.output_file),
        "STREAM_KEY": "LOCAL_TEST_ONLY",
        "RTMP_URL": "rtmps://a.rtmps.youtube.com:443/live2/LOCAL_TEST_ONLY",
        "DISPLAY_NAME": config.display,
        "DISPLAY": config.display,
        "VIDEO_SIZE": config.video_size,
        "OUTPUT_SIZE": config.output_size,
        "FRAME_RATE": str(config.frame_rate),
        "AUTO_START_XVFB": "1",
        "AUTO_START_BROWSER": "1" if config.start_browser else "0",
        "USE_OVERLAY_WRAPPER": "1",
        "OVERLAY_DIR": str(paths.overlay_dir),
        "OVERLAY_BIND_HOST": "127.0.0.1",
        "OVERLAY_VIEW_HOST": "127.0.0.1",
        "OVERLAY_PORT": str(config.overlay_port),
        "OVERLAY_SERVER_LOG_FILE": str(paths.overlay_server_log_file),
        "STREAM1090_URL": config.stream1090_url,
        "BROWSER_PROFILE_DIR": str(paths.browser_profile_dir),
        "BROWSER_WINDOW_SIZE": config.video_size.replace("x", ","),
        "BROWSER_WINDOW_POS": "0,0",
        "BROWSER_START_SETTLE_SEC_TEST": "0",
        "XVFB_LOG_FILE": str(paths.xvfb_log_file),
        "BROWSER_LOG_FILE": str(paths.browser_log_file),
        "PULSE_SINK": config.pulse_sink,
        "PULSE_SOURCE": f"{config.pulse_sink}.monitor" if config.pulse_sink else "",
        "PULSE_SHM": "0",
        "LOCAL_MONITOR_AUDIO": "0",
        "STREAM_LOCK_DIR": str(paths.lock_dir),
        "REQUIRE_SYSTEMD_LAUNCH": "0",
        "ALLOW_DIRECT_STREAM_SH": "1",
        "HEALTH_GATE_ABORT_ON_FOREIGN": "0",
        "TAKEOVER_ENABLED": "0",
        "RUNTIME_STATE_FILE": str(paths.runtime_state_file),
        "RUNTIME_HEARTBEAT_SEC": "5",
        "EVENT_LOG_FILE": str(paths.event_log_file),
        "RESTART_REASON_FILE": str(paths.restart_reason_file),
        "PRE_FFMPEG_MIN_WAIT_SEC_TEST": "0",
        "PRE_FFMPEG_REQUIRE_OVERLAY_READY": "0",
        "NOW_PLAYING_FILE": str(paths.now_playing_file),
        "NOW_PLAYING_SNAPSHOT_FILE": str(paths.now_playing_snapshot_file),
        "PLAY_HISTORY_JSONL_FILE": str(paths.play_history_jsonl_file),
        "MUSIC_ROOT": str(paths.music_root),
    }


def merged_env(config: LocalRuntimeConfig) -> dict[str, str]:
    env = os.environ.copy()
    local_env = build_local_env(config)
    src_path = str(config.repo_root.resolve() / "src")
    current_pythonpath = env.get("PYTHONPATH", "")
    env.update(local_env)
    env["PYTHONPATH"] = src_path if not current_pythonpath else f"{src_path}{os.pathsep}{current_pythonpath}"
    return env


def write_env_file(config: LocalRuntimeConfig, env: Mapping[str, str] | None = None) -> Path:
    paths = resolve_paths(config)
    paths.env_file.parent.mkdir(parents=True, exist_ok=True)
    values = dict(env or build_local_env(config))
    lines = [
        "# Generated by stream_v2 local runtime. Safe for local TEST_MODE use only.",
        "# It does not contain production stream keys or OAuth material.",
    ]
    for key in sorted(values):
        lines.append(f"{key}={shlex.quote(values[key])}")
    paths.env_file.write_text("\n".join(lines) + "\n", encoding="utf-8")
    paths.env_file.chmod(0o600)
    return paths.env_file


def stream_engine_command(config: LocalRuntimeConfig) -> list[str]:
    return [
        config.python_bin,
        str(config.repo_root.resolve() / "src" / "stream_core" / "stream_engine.py"),
    ]


def auto_dj_command(config: LocalRuntimeConfig) -> list[str]:
    paths = resolve_paths(config)
    return [
        config.python_bin,
        str(config.repo_root.resolve() / "src" / "dj" / "auto_dj.py"),
        "--music-root",
        str(paths.music_root),
        "--now-playing-file",
        str(paths.now_playing_file),
        "--snapshot-file",
        str(paths.now_playing_snapshot_file),
        "--history-jsonl-file",
        str(paths.play_history_jsonl_file),
        "--player",
        "ffmpeg",
        "--pulse-sink",
        config.pulse_sink,
        "--retry-sleep-sec",
        "1",
        "--player-fail-sleep-sec",
        "1",
        "--snapshot-heartbeat-sec",
        "2",
        "--max-track-sec",
        str(config.dj_max_track_sec),
    ]


def local_runtime_summary(config: LocalRuntimeConfig) -> dict[str, object]:
    paths = resolve_paths(config)
    return {
        "mode": "local_test",
        "repo_root": str(config.repo_root.resolve()),
        "state_root": str(paths.state_root),
        "env_file": str(paths.env_file),
        "output": {
            "mode": config.output,
            "file": str(paths.output_file) if config.output == "file" else "",
        },
        "rendering": {
            "display": config.display,
            "overlay_dir": str(paths.overlay_dir),
            "overlay_url": f"http://127.0.0.1:{config.overlay_port}/index.html",
            "stream1090_url": config.stream1090_url,
            "browser": "enabled" if config.start_browser else "disabled",
        },
        "audio": {
            "pulse_sink": config.pulse_sink,
            "pulse_source": f"{config.pulse_sink}.monitor" if config.pulse_sink else "",
            "auto_dj": "enabled" if config.with_dj else "disabled",
            "music_root": str(paths.music_root),
            "max_track_sec": config.dj_max_track_sec,
        },
        "safety": {
            "test_mode": True,
            "youtube_rtmp": "disabled by TEST_MODE",
            "systemd_mutation": "not used",
            "production_root": "read-only, not invoked",
        },
        "commands": {
            "stream_engine": stream_engine_command(config),
            "auto_dj": auto_dj_command(config) if config.with_dj else [],
        },
        "env": build_local_env(config),
    }


def _terminate_process(proc: subprocess.Popen[object], *, label: str) -> None:
    if proc.poll() is not None:
        return
    try:
        proc.send_signal(signal.SIGTERM)
    except ProcessLookupError:
        return
    try:
        proc.wait(timeout=5)
        return
    except subprocess.TimeoutExpired:
        print(f"local-smoke: {label} did not stop after SIGTERM; killing pid={proc.pid}", file=sys.stderr)
    try:
        proc.kill()
    except ProcessLookupError:
        return
    try:
        proc.wait(timeout=2)
    except subprocess.TimeoutExpired:
        pass


def _start_process(command: list[str], *, env: Mapping[str, str], label: str) -> subprocess.Popen[object]:
    print(f"local-smoke: starting {label}: {' '.join(shlex.quote(part) for part in command)}", flush=True)
    return subprocess.Popen(command, env=dict(env))


def run_local_smoke(config: LocalRuntimeConfig) -> int:
    paths = prepare_local_runtime(config)
    env = merged_env(config)
    if config.write_env:
        write_env_file(config, env=build_local_env(config))

    summary = local_runtime_summary(config)
    if config.dry_run:
        print(json.dumps(summary, ensure_ascii=False, indent=2, default=str))
        return 0

    print(json.dumps({k: v for k, v in summary.items() if k != "env"}, ensure_ascii=False, indent=2, default=str))
    engine_proc = _start_process(stream_engine_command(config), env=env, label="stream_engine")
    dj_proc: subprocess.Popen[object] | None = None
    dj_restart_count = 0
    try:
        deadline = None if config.keep_running else time.monotonic() + max(0.0, float(config.duration_sec))
        if config.with_dj:
            time.sleep(max(0.0, config.dj_start_delay_sec))
            dj_proc = _start_process(auto_dj_command(config), env=env, label="auto_dj")
        while True:
            engine_rc = engine_proc.poll()
            if engine_rc is not None:
                print(f"local-smoke: stream_engine exited rc={engine_rc}", file=sys.stderr)
                return int(engine_rc)
            if dj_proc is not None:
                dj_rc = dj_proc.poll()
                if dj_rc is not None:
                    dj_restart_count += 1
                    print(
                        "local-smoke: "
                        f"auto_dj exited rc={dj_rc}; restarting in {config.dj_restart_delay_sec:.1f}s "
                        f"(restart_count={dj_restart_count})",
                        file=sys.stderr,
                        flush=True,
                    )
                    time.sleep(max(0.0, config.dj_restart_delay_sec))
                    dj_proc = _start_process(auto_dj_command(config), env=env, label="auto_dj")
            if deadline is not None and time.monotonic() >= deadline:
                print(f"local-smoke: duration reached; state_root={paths.state_root}", flush=True)
                return 0
            time.sleep(0.25)
    finally:
        if dj_proc is not None:
            _terminate_process(dj_proc, label="auto_dj")
        _terminate_process(engine_proc, label="stream_engine")
