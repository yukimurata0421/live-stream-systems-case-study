from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Sequence

from .config import DEFAULT_MAX_CONSISTENCY_WINDOW_SEC, DEFAULT_SOURCE_STATE_ROOT, DEFAULT_V2_STATE_ROOT, RuntimeConfig
from .health_summary import build_health_summary, dumps_health_summary, render_text_health_summary
from .jsonio import read_json
from .local_runtime import (
    DEFAULT_DISPLAY,
    DEFAULT_OVERLAY_PORT,
    DEFAULT_PULSE_SINK,
    LocalRuntimeConfig,
    build_local_env,
    local_runtime_summary,
    prepare_local_runtime,
    run_local_smoke,
    write_env_file,
)
from .pipeline import ShadowPipeline
from .sli import ObjectiveSliCalculator
from .stream_app import run_stream_cli, stream_app_root
from .status_summary import build_status_summary, dumps_summary, render_text_summary
from .subsystems.registry import stream_components_payload
from .timeutil import now_utc


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="stream_v2 staging subsystem/orchestrator tools")
    sub = parser.add_subparsers(dest="command", required=True)

    shadow = sub.add_parser("shadow-once", help="read production runtime and write v2 shadow state once")
    _add_common(shadow)
    shadow.add_argument("--pretty", action="store_true", help="print formatted summary")

    show = sub.add_parser("show-status", help="print latest v2 subsystem status")
    show.add_argument("--state-root", type=Path, default=DEFAULT_V2_STATE_ROOT)
    show.add_argument("--pretty", action="store_true")

    stream_cli = sub.add_parser("stream-cli", help="run the stream_v2 root stream-new CLI")
    stream_cli.add_argument(
        "--allow-mutating",
        action="store_true",
        help="allow install/start/stop/restart/enable/watch; disabled by default until cutover",
    )
    stream_cli.add_argument("stream_args", nargs=argparse.REMAINDER, help="arguments passed to bin/stream-new")

    paths = sub.add_parser("stream-paths", help="print stream_v2 root app paths")
    paths.add_argument("--pretty", action="store_true")

    subsystem_paths = sub.add_parser("subsystem-paths", help="print subsystem-owned stream_v2 components")
    subsystem_paths.add_argument("--pretty", action="store_true")

    ops = sub.add_parser("ops-summary", help="print operator-oriented v2 status summary")
    ops.add_argument("--state-root", type=Path, default=DEFAULT_V2_STATE_ROOT)
    ops.add_argument("--pretty", action="store_true", help="print formatted JSON")
    ops.add_argument("--text", action="store_true", help="print compact human-readable summary")

    health = sub.add_parser("health-summary", help="print native stream_v2 health summary without invoking legacy scripts")
    health.add_argument("--source-state-root", type=Path, default=DEFAULT_SOURCE_STATE_ROOT)
    health.add_argument("--state-root", type=Path, default=DEFAULT_V2_STATE_ROOT)
    health.add_argument("--max-youtube-stats-stale-sec", type=float, default=180.0)
    health.add_argument("--max-v2-status-stale-sec", type=float, default=300.0)
    health.add_argument("--pretty", action="store_true", help="print formatted JSON")
    health.add_argument("--text", action="store_true", help="print compact human-readable summary")

    shadow_sli = sub.add_parser("shadow-sli", help="summarize v2 shadow decision SLI and production-action diff")
    _add_common(shadow_sli)
    shadow_sli.add_argument("--pretty", action="store_true", help="print formatted JSON")
    shadow_sli.add_argument("--text", action="store_true", help="print compact human-readable summary")

    local_env = sub.add_parser("local-env", help="print or write isolated local TEST_MODE runtime env")
    _add_local_runtime_args(local_env, include_duration=False)
    local_env.add_argument("--write", action="store_true", help="write .state/local-run/local.env with 0600 permissions")
    local_env.add_argument("--pretty", action="store_true", help="print formatted JSON")

    local_smoke = sub.add_parser("local-smoke", help="run stream_v2 locally in TEST_MODE without YouTube/systemd mutation")
    _add_local_runtime_args(local_smoke, include_duration=True)
    local_smoke.add_argument("--dry-run", action="store_true", help="prepare nothing beyond summary; do not start processes")
    local_smoke.add_argument("--write-env", action="store_true", help="write .state/local-run/local.env before running")
    local_smoke.add_argument("--pretty", action="store_true", help="accepted for symmetry; local-smoke prints formatted summaries")
    return parser


def _add_common(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--source-state-root", type=Path, default=DEFAULT_SOURCE_STATE_ROOT)
    parser.add_argument("--state-root", type=Path, default=DEFAULT_V2_STATE_ROOT)
    parser.add_argument("--stream-id", default="adsb-streamnew")
    parser.add_argument("--max-consistency-window-sec", type=float, default=DEFAULT_MAX_CONSISTENCY_WINDOW_SEC)
    parser.add_argument("--mode", default="shadow", choices=["shadow"])
    parser.add_argument(
        "--supervisor-mode",
        default=os.environ.get("STREAM_RUNTIME_SUPERVISOR", "systemd"),
        choices=["systemd", "k8s", "k3s", "kubernetes"],
        help="runtime supervisor vocabulary used in shadow action plans",
    )


def _add_local_runtime_args(parser: argparse.ArgumentParser, *, include_duration: bool) -> None:
    parser.add_argument("--state-root", type=Path, default=None, help="local runtime root; default .state/local-run")
    parser.add_argument("--display", default=DEFAULT_DISPLAY, help="local X display for Xvfb/browser capture")
    parser.add_argument("--overlay-port", type=int, default=DEFAULT_OVERLAY_PORT, help="local overlay HTTP port")
    parser.add_argument("--pulse-sink", default=DEFAULT_PULSE_SINK, help="local Pulse null sink name")
    parser.add_argument("--stream1090-url", default=None, help="stream1090/tar1090 URL rendered inside the local overlay")
    parser.add_argument("--output", choices=["null", "file"], default="null", help="ffmpeg TEST_MODE output target")
    parser.add_argument("--output-file", type=Path, default=None, help="capture file when --output=file")
    parser.add_argument("--env-file", type=Path, default=None, help="env file path for --write/--write-env")
    parser.add_argument("--video-size", default="1920x1080", help="local capture size")
    parser.add_argument("--output-size", default="1920x1080", help="local encoded frame size")
    parser.add_argument("--frame-rate", type=int, default=5, help="local capture frame rate")
    parser.add_argument("--no-browser", action="store_true", help="skip browser startup; useful for dependency smoke tests")
    parser.add_argument("--with-dj", action="store_true", help="also run AutoDJ into the isolated local Pulse sink")
    parser.add_argument("--music-root", type=Path, default=None, help="AutoDJ music root; default ncs_music/time_tags")
    parser.add_argument(
        "--dj-max-track-sec",
        type=int,
        default=0,
        help="AutoDJ max track length for local smoke; 0 disables duration scan for faster startup",
    )
    if include_duration:
        parser.add_argument("--duration-sec", type=float, default=30.0, help="seconds to keep the local smoke run alive")
        parser.add_argument("--keep-running", action="store_true", help="keep running until interrupted")


def _local_runtime_config(args: argparse.Namespace) -> LocalRuntimeConfig:
    stream1090_url = args.stream1090_url if args.stream1090_url is not None else None
    kwargs = {
        "state_root": args.state_root,
        "display": args.display,
        "overlay_port": args.overlay_port,
        "pulse_sink": args.pulse_sink,
        "output": args.output,
        "output_file": args.output_file,
        "start_browser": not args.no_browser,
        "env_file": args.env_file,
        "with_dj": args.with_dj,
        "video_size": args.video_size,
        "output_size": args.output_size,
        "frame_rate": args.frame_rate,
        "music_root": args.music_root,
        "dj_max_track_sec": args.dj_max_track_sec,
    }
    if stream1090_url:
        kwargs["stream1090_url"] = stream1090_url
    if hasattr(args, "duration_sec"):
        kwargs["duration_sec"] = args.duration_sec
    if hasattr(args, "keep_running"):
        kwargs["keep_running"] = args.keep_running
    if hasattr(args, "dry_run"):
        kwargs["dry_run"] = args.dry_run
    if hasattr(args, "write_env"):
        kwargs["write_env"] = args.write_env
    return LocalRuntimeConfig(**kwargs)


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command == "shadow-once":
        config = RuntimeConfig(
            source_state_root=args.source_state_root,
            state_root=args.state_root,
            stream_id=args.stream_id,
            max_consistency_window_sec=args.max_consistency_window_sec,
            mode=args.mode,
            supervisor_mode=args.supervisor_mode,
        )
        result = ShadowPipeline(config).run_once()
        if args.pretty:
            print(json.dumps({"snapshot": result.snapshot, "orchestrator": result.orchestrator_event, "objective_sli": result.objective_sli}, ensure_ascii=False, indent=2))
        else:
            summary = {
                "status_path": str(config.subsystems_status_path),
                "overall": result.snapshot.get("overall", {}),
                "selected_action": result.orchestrator_event.get("selected_action", {}),
                "recovery_action_plan": {
                    "path": str(config.recovery_action_plan_path),
                    "action": result.recovery_action_plan.get("action", "none"),
                    "executable": result.recovery_action_plan.get("executable", False),
                    "execute": result.recovery_action_plan.get("execute", False),
                    "blocked_by": result.recovery_action_plan.get("blocked_by", []),
                },
            }
            print(json.dumps(summary, ensure_ascii=False, separators=(",", ":")))
        return 0
    if args.command == "show-status":
        payload = read_json(args.state_root / "subsystems_status.json")
        if not payload:
            print(json.dumps({"error": "subsystems_status.json not found", "state_root": str(args.state_root)}, ensure_ascii=False))
            return 2
        print(json.dumps(payload, ensure_ascii=False, indent=2 if args.pretty else None))
        return 0
    if args.command == "stream-cli":
        stream_args = list(args.stream_args)
        if stream_args and stream_args[0] == "--":
            stream_args = stream_args[1:]
        return run_stream_cli(stream_args, allow_mutating=args.allow_mutating)
    if args.command == "stream-paths":
        root = stream_app_root()
        payload = {
            "stream_v2_root": str(root),
            "stream_cli": str(root / "bin" / "stream-new"),
            "ncs_music": str(root / "ncs_music"),
        }
        print(json.dumps(payload, ensure_ascii=False, indent=2 if args.pretty else None))
        return 0
    if args.command == "subsystem-paths":
        payload = stream_components_payload()
        print(json.dumps(payload, ensure_ascii=False, indent=2 if args.pretty else None))
        return 0
    if args.command == "ops-summary":
        payload = build_status_summary(args.state_root)
        if args.text:
            print(render_text_summary(payload))
        else:
            print(dumps_summary(payload, pretty=args.pretty))
        return 0
    if args.command == "health-summary":
        payload = build_health_summary(
            source_state_root=args.source_state_root,
            state_root=args.state_root,
            max_youtube_stats_stale_sec=args.max_youtube_stats_stale_sec,
            max_v2_status_stale_sec=args.max_v2_status_stale_sec,
        )
        if args.text:
            print(render_text_health_summary(payload))
        else:
            print(dumps_health_summary(payload, pretty=args.pretty))
        return 0
    if args.command == "shadow-sli":
        config = RuntimeConfig(
            source_state_root=args.source_state_root,
            state_root=args.state_root,
            stream_id=args.stream_id,
            max_consistency_window_sec=args.max_consistency_window_sec,
            mode=args.mode,
            supervisor_mode=args.supervisor_mode,
        )
        payload = ObjectiveSliCalculator(config).calculate(now=now_utc())
        if args.text:
            print(_render_shadow_sli_text(payload))
        else:
            print(json.dumps(payload, ensure_ascii=False, indent=2 if args.pretty else None))
        return 0
    if args.command == "local-env":
        config = _local_runtime_config(args)
        if args.write:
            prepare_local_runtime(config)
            env_path = write_env_file(config)
        else:
            env_path = None
        payload = local_runtime_summary(config)
        if env_path:
            payload["written_env_file"] = str(env_path)
        else:
            payload["env"] = build_local_env(config)
        print(json.dumps(payload, ensure_ascii=False, indent=2 if args.pretty else None, default=str))
        return 0
    if args.command == "local-smoke":
        config = _local_runtime_config(args)
        return run_local_smoke(config)
    parser.error("unknown command")
    return 2


def _render_shadow_sli_text(payload: dict) -> str:
    windows = payload.get("windows") if isinstance(payload.get("windows"), dict) else {}
    lines = [
        "[shadow-sli] "
        f"generated_at={payload.get('ts_utc', '')} "
        f"interval_sec={payload.get('shadow_timer_policy', {}).get('expected_interval_sec', '')} "
        f"coverage_ratio={payload.get('window_policy', {}).get('window_complete_coverage_ratio', '')}",
    ]
    for name in ("last_24h", "last_7d", "last_30d"):
        window = windows.get(name) if isinstance(windows.get(name), dict) else {}
        shadow = window.get("subsystems_shadow") if isinstance(window.get("subsystems_shadow"), dict) else {}
        lines.append(
            "[shadow-sli] "
            f"window={name} complete={window.get('window_complete', False)} "
            f"coverage_sec={window.get('data_coverage_sec', 0)} "
            f"status_samples={shadow.get('status_sample_count', 0)} "
            f"orchestrator_samples={shadow.get('orchestrator_sample_count', 0)} "
            f"selected_actions={shadow.get('selected_action_counts', {})} "
            f"production_actions={shadow.get('production_action_counts', {})} "
            f"disagreements={shadow.get('shadow_vs_production_disagreement_count', 0)} "
            f"reasons={shadow.get('shadow_vs_production_disagreement_by_reason', {})}"
        )
    return "\n".join(lines)
