from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class CliPaths:
    base_dir: Path
    systemd_src_dir: Path
    ops_log_dir: Path
    routine_check_dir: Path
    api_cost_report_script: Path
    observe_stream_health_script: Path
    state_base_dir: Path
    log_base_dir: Path
    youtube_quota_state_file: Path
    youtube_watchdog_stats_file: Path
    youtube_api_cost_report_dir: Path
    youtube_api_cost_open_day_latest_file: Path
    youtube_api_cost_latest_file: Path
    objective_sli_file: Path
    objective_sli_events_file: Path
    memory_status_file: Path
    memory_status_events_file: Path
    resource_memory_file: Path
    resource_memory_events_file: Path
    resource_memory_assessment_file: Path
    subsystems_status_file: Path
    subsystems_status_events_file: Path
    recovery_orchestrator_events_file: Path
    recovery_action_plan_file: Path
    recovery_action_lock_file: Path
    stream_components_file: Path
    fast_recovery_events_file: Path
    stream_engine_events_file: Path
    youtube_watchdog_events_file: Path
    upstream_report_events_file: Path
    stream1090_report_events_file: Path
    stream1090_visual_dir: Path
    notify_state_file: Path
    notify_events_file: Path
    notify_outbox_file: Path
    maintenance_state_file: Path


def from_environment(source_file: str) -> CliPaths:
    base_dir = Path(os.environ.get("STREAM_BASE_DIR", Path(source_file).resolve().parents[2])).resolve()
    state_base_dir = Path(
        os.environ.get("STREAM_RUNTIME_STATE_DIR", str(base_dir / ".state" / "adsb-streamnew-v2"))
    ).expanduser()
    log_base_dir = Path(os.environ.get("STREAM_RUNTIME_LOG_DIR", str(state_base_dir / "logs"))).expanduser()
    youtube_api_cost_report_dir = state_base_dir / "reports" / "youtube_api_cost"
    return CliPaths(
        base_dir=base_dir,
        systemd_src_dir=base_dir / "ops" / "systemd",
        ops_log_dir=base_dir / "docs" / "v3" / "50_ops_logs",
        routine_check_dir=base_dir / "docs" / "v2" / "45_routine_checks",
        api_cost_report_script=base_dir / "ops" / "scripts" / "report_youtube_api_cost.py",
        observe_stream_health_script=base_dir / "ops" / "scripts" / "observe_stream_health.py",
        state_base_dir=state_base_dir,
        log_base_dir=log_base_dir,
        youtube_quota_state_file=state_base_dir / "youtube_quota_state.json",
        youtube_watchdog_stats_file=state_base_dir / "youtube_watchdog_stats.json",
        youtube_api_cost_report_dir=youtube_api_cost_report_dir,
        youtube_api_cost_open_day_latest_file=youtube_api_cost_report_dir / "open_day_latest.json",
        youtube_api_cost_latest_file=youtube_api_cost_report_dir / "latest.json",
        objective_sli_file=state_base_dir / "objective_sli.json",
        objective_sli_events_file=log_base_dir / "objective_sli.jsonl",
        memory_status_file=state_base_dir / "memory_status.json",
        memory_status_events_file=log_base_dir / "memory_status.jsonl",
        resource_memory_file=state_base_dir / "resource_memory.json",
        resource_memory_events_file=log_base_dir / "resource_memory.jsonl",
        resource_memory_assessment_file=state_base_dir / "resource_memory_assessment.json",
        subsystems_status_file=state_base_dir / "subsystems_status.json",
        subsystems_status_events_file=log_base_dir / "subsystems_status.jsonl",
        recovery_orchestrator_events_file=log_base_dir / "recovery_orchestrator.jsonl",
        recovery_action_plan_file=state_base_dir / "recovery_action_plan.json",
        recovery_action_lock_file=state_base_dir / "recovery_action.lock.json",
        stream_components_file=state_base_dir / "stream_components.json",
        fast_recovery_events_file=log_base_dir / "fast_recovery_events.jsonl",
        stream_engine_events_file=log_base_dir / "stream_engine_events.jsonl",
        youtube_watchdog_events_file=log_base_dir / "youtube_watchdog.jsonl",
        upstream_report_events_file=log_base_dir / "upstream_stream1090_report.jsonl",
        stream1090_report_events_file=log_base_dir / "stream1090_report.jsonl",
        stream1090_visual_dir=state_base_dir / "reports" / "stream1090_visual",
        notify_state_file=state_base_dir / "stream_notify_state.json",
        notify_events_file=log_base_dir / "stream_notify_events.jsonl",
        notify_outbox_file=state_base_dir / "stream_notify_outbox.jsonl",
        maintenance_state_file=state_base_dir / "maintenance_mode.json",
    )
