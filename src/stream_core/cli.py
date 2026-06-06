#!/usr/bin/env python3
from __future__ import annotations

import argparse
import subprocess
import sys
import time
from pathlib import Path

SRC_DIR = Path(__file__).resolve().parents[1]
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from stream_core.cli_support import mutation_guard as cli_mutation_guard
from stream_core.cli_support import memory_status as memory_status_cli
from stream_core.cli_support import objective_sli as objective_sli_cli
from stream_core.cli_support import resource_memory as resource_memory_cli
from stream_core.cli_support import parser as cli_parser
from stream_core.cli_support import paths as cli_paths
from stream_core.cli_support import router as cli_router
from stream_core.cli_support import contexts as cli_contexts
from stream_core.cli_support import shadow as shadow_cli
from stream_core.cli_support import systemd_units as cli_systemd_units
from stream_core.cli_support.units import (
    ALL_UNITS,
    API_COST_OPEN_DAY_REPORT_SERVICE,
    API_COST_OPEN_DAY_REPORT_TIMER,
    API_COST_REPORT_SERVICE,
    API_COST_REPORT_TIMER,
    DJ_SERVICE,
    FAST_RECOVERY_SERVICE,
    FAST_RECOVERY_TIMER,
    INSTALL_TARGETS,
    LEGACY_STREAM_SERVICE,
    MAINTENANCE_SERVICES,
    MAINTENANCE_TIMERS,
    MEMORY_STATUS_SERVICE,
    MEMORY_STATUS_TIMER,
    NOTIFY_SERVICE,
    NOTIFY_TIMER,
    RECOVERY_ORCHESTRATOR_SERVICE,
    RECOVERY_ORCHESTRATOR_TIMER,
    RESOURCE_MEMORY_SERVICE,
    RESOURCE_MEMORY_TIMER,
    STREAM1090_REPORT_SERVICE,
    STREAM1090_REPORT_TIMER,
    STREAM_SERVICE,
    SUBSYSTEMS_STATUS_SERVICE,
    SUBSYSTEMS_STATUS_TIMER,
    SYSTEM_UNITS,
    UPSTREAM_REPORT_SERVICE,
    UPSTREAM_REPORT_TIMER,
    WATCHDOG_SERVICE,
    WATCHDOG_TIMER,
    YTW_MONITOR_SERVICE,
    YTW_MONITOR_TIMER,
    YTW_VIDEO_RESOLVER_SERVICE,
    YTW_VIDEO_RESOLVER_TIMER,
)
from stream_core.commands import api_usage as api_usage_command
from stream_core.commands import doctor as doctor_command
from stream_core.commands import health as health_command
from stream_core.commands import history as history_command
from stream_core.commands import maintenance as maintenance_command
from stream_core.commands import oauth as oauth_command
from stream_core.commands import runtime_safety as runtime_safety_command
from stream_core.commands import service as service_command
from stream_core.commands import stream1090_report as stream1090_report_command
from stream_core.common import systemd as systemd_common
from stream_core.common.envfile import parse_bool, read_env_file
from stream_core.common.json_io import append_jsonl, iter_jsonl, read_json_file
from stream_core.common.timeutil import (
    jst_day,
    jst_text,
    jst_text_or_unknown,
    parse_utc_ts,
    pt_day,
    utc_now_text,
    utc_text_from_ts,
)
from stream_core.notifications import cli_adapter as notify_cli_adapter
from stream_core.notifications import discord as notify_discord
from stream_core.notifications import incidents as notify_incidents
from stream_core.notifications import outbox as notify_outbox
from stream_core.notifications import renderer as notify_renderer
from stream_core.notifications import state as notify_state
from stream_core.notifications import status_loop as notify_status_loop


STREAM_BASE_DIR_ENV = "STREAM_BASE_DIR"
PATHS = cli_paths.from_environment(__file__)
BASE_DIR = PATHS.base_dir
SYSTEMD_SRC_DIR = PATHS.systemd_src_dir
OPS_LOG_DIR = PATHS.ops_log_dir
ROUTINE_CHECK_DIR = PATHS.routine_check_dir
API_COST_REPORT_SCRIPT = PATHS.api_cost_report_script
OBSERVE_STREAM_HEALTH_SCRIPT = PATHS.observe_stream_health_script
STATE_BASE_DIR = PATHS.state_base_dir
LOG_BASE_DIR = PATHS.log_base_dir
YOUTUBE_QUOTA_STATE_FILE = PATHS.youtube_quota_state_file
YOUTUBE_WATCHDOG_STATS_FILE = PATHS.youtube_watchdog_stats_file
YOUTUBE_API_COST_REPORT_DIR = PATHS.youtube_api_cost_report_dir
YOUTUBE_API_COST_OPEN_DAY_LATEST_FILE = PATHS.youtube_api_cost_open_day_latest_file
YOUTUBE_API_COST_LATEST_FILE = PATHS.youtube_api_cost_latest_file
OBJECTIVE_SLI_FILE = PATHS.objective_sli_file
OBJECTIVE_SLI_EVENTS_FILE = PATHS.objective_sli_events_file
MEMORY_STATUS_FILE = PATHS.memory_status_file
MEMORY_STATUS_EVENTS_FILE = PATHS.memory_status_events_file
RESOURCE_MEMORY_FILE = PATHS.resource_memory_file
RESOURCE_MEMORY_EVENTS_FILE = PATHS.resource_memory_events_file
RESOURCE_MEMORY_ASSESSMENT_FILE = PATHS.resource_memory_assessment_file
SUBSYSTEMS_STATUS_FILE = PATHS.subsystems_status_file
SUBSYSTEMS_STATUS_EVENTS_FILE = PATHS.subsystems_status_events_file
RECOVERY_ORCHESTRATOR_EVENTS_FILE = PATHS.recovery_orchestrator_events_file
RECOVERY_ACTION_PLAN_FILE = PATHS.recovery_action_plan_file
RECOVERY_ACTION_LOCK_FILE = PATHS.recovery_action_lock_file
STREAM_COMPONENTS_FILE = PATHS.stream_components_file
FAST_RECOVERY_EVENTS_FILE = PATHS.fast_recovery_events_file
STREAM_ENGINE_EVENTS_FILE = PATHS.stream_engine_events_file
YOUTUBE_WATCHDOG_EVENTS_FILE = PATHS.youtube_watchdog_events_file
UPSTREAM_REPORT_EVENTS_FILE = PATHS.upstream_report_events_file
STREAM1090_REPORT_EVENTS_FILE = PATHS.stream1090_report_events_file
STREAM1090_VISUAL_DIR = PATHS.stream1090_visual_dir
NOTIFY_STATE_FILE = PATHS.notify_state_file
NOTIFY_EVENTS_FILE = PATHS.notify_events_file
NOTIFY_OUTBOX_FILE = PATHS.notify_outbox_file
MAINTENANCE_STATE_FILE = PATHS.maintenance_state_file
NOTIFY_ENV_FILE = Path("/etc/default/adsb-streamnew-notify")
YOUTUBE_MONITOR_ENV_FILE = Path("/etc/default/adsb-streamnew-youtube-monitor")

STREAM_V2_MUTATING_COMMANDS = cli_mutation_guard.STREAM_V2_MUTATING_COMMANDS
MAINTENANCE_COMMAND_ALIASES = cli_mutation_guard.MAINTENANCE_COMMAND_ALIASES
MAINTENANCE_STATUS_ACTIONS = cli_mutation_guard.MAINTENANCE_STATUS_ACTIONS
MAINTENANCE_TOP_LEVEL_ACTIONS = cli_mutation_guard.MAINTENANCE_TOP_LEVEL_ACTIONS
STREAM_V2_ALLOW_MUTATING_ENV = cli_mutation_guard.STREAM_V2_ALLOW_MUTATING_ENV

YOUTUBE_RTMP_HOSTS = {"a.rtmp.youtube.com", "a.rtmps.youtube.com"}
PREFERRED_YOUTUBE_RTMPS_URL = "rtmps://a.rtmps.youtube.com:443/live2"
MEMORY_STATUS_SERVICE_UNITS = (
    STREAM_SERVICE,
    DJ_SERVICE,
    WATCHDOG_SERVICE,
    YTW_MONITOR_SERVICE,
    YTW_VIDEO_RESOLVER_SERVICE,
    FAST_RECOVERY_SERVICE,
    STREAM1090_REPORT_SERVICE,
    UPSTREAM_REPORT_SERVICE,
    SUBSYSTEMS_STATUS_SERVICE,
    RECOVERY_ORCHESTRATOR_SERVICE,
    NOTIFY_SERVICE,
    API_COST_OPEN_DAY_REPORT_SERVICE,
    API_COST_REPORT_SERVICE,
)


def _runtime_safety_context() -> runtime_safety_command.RuntimeSafetyContext:
    return cli_contexts.runtime_safety_context(sys.modules[__name__])


def run(cmd: list[str], check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, text=True, capture_output=True, check=check)


def run_systemctl(args: list[str], check: bool = True) -> subprocess.CompletedProcess[str]:
    return systemd_common.run_systemctl(args, require_privilege=True, check=check)


def in_stream_v2_tree() -> bool:
    return BASE_DIR.name in {"stream_v2", "stream_v3"}


def mutating_systemd_allowed() -> bool:
    return cli_mutation_guard.mutating_systemd_allowed()


def command_requires_mutating_systemd(command: str, maintenance_action: str = "") -> bool:
    return cli_mutation_guard.command_requires_mutating_systemd(command, maintenance_action)


def guard_stream_v2_mutating_command(command: str, maintenance_action: str = "") -> int:
    return cli_mutation_guard.guard_stream_v2_mutating_command(
        command,
        maintenance_action,
        in_stream_v2_tree=in_stream_v2_tree,
        mutating_allowed=mutating_systemd_allowed,
    )


def guard_stream_v3_mutating_command(command: str, maintenance_action: str = "") -> int:
    return guard_stream_v2_mutating_command(command, maintenance_action)


def unit_installed(unit: str) -> bool:
    return cli_systemd_units.unit_installed(unit, run_systemctl=lambda args, check: run_systemctl(args, check=check))


def is_active(unit: str) -> bool:
    return cli_systemd_units.is_active(unit, run_systemctl=lambda args, check: run_systemctl(args, check=check))


def print_systemctl_error(action: str, unit: str, cp: subprocess.CompletedProcess[str]) -> None:
    cli_systemd_units.print_systemctl_error(action, unit, cp)


def start_unit(unit: str) -> bool:
    return cli_systemd_units.start_unit(
        unit,
        run_systemctl=lambda args, check: run_systemctl(args, check=check),
        is_active=is_active,
        print_error=print_systemctl_error,
    )


def restart_unit(unit: str, reason: str = "") -> bool:
    return cli_systemd_units.restart_unit(
        unit,
        reason=reason,
        run_systemctl=lambda args, check: run_systemctl(args, check=check),
        is_active=is_active,
        print_error=print_systemctl_error,
    )


def trigger_unit(unit: str, reason: str = "") -> bool:
    return cli_systemd_units.trigger_unit(
        unit,
        reason=reason,
        run_systemctl=lambda args, check: run_systemctl(args, check=check),
        print_error=print_systemctl_error,
    )


def enable_unit(unit: str) -> bool:
    return cli_systemd_units.enable_unit(
        unit,
        run_systemctl=lambda args, check: run_systemctl(args, check=check),
        print_error=print_systemctl_error,
    )


def stop_unit(unit: str) -> bool:
    return cli_systemd_units.stop_unit(
        unit,
        run_systemctl=lambda args, check: run_systemctl(args, check=check),
        print_error=print_systemctl_error,
    )


parse_stream_key_from_rtmp_url = runtime_safety_command.parse_stream_key_from_rtmp_url


def stream_ingest_endpoint_status(env_path: Path = Path("/etc/default/adsb-streamnew")) -> dict:
    return runtime_safety_command.stream_ingest_endpoint_status(_runtime_safety_context(), env_path)


def youtube_monitor_max_fails() -> int:
    return runtime_safety_command.youtube_monitor_max_fails(_runtime_safety_context())


def youtube_watchdog_state_path() -> Path:
    return runtime_safety_command.youtube_watchdog_state_path(_runtime_safety_context())


def youtube_watchdog_unhealthy() -> bool:
    return runtime_safety_command.youtube_watchdog_unhealthy(
        state_path=youtube_watchdog_state_path(),
        max_fails=youtube_monitor_max_fails(),
    )


def expected_stream_key() -> str:
    return runtime_safety_command.expected_stream_key(_runtime_safety_context())


def default_stream1090_upstream_url() -> str:
    return runtime_safety_command.default_stream1090_upstream_url(_runtime_safety_context())


split_url_root_and_path = runtime_safety_command.split_url_root_and_path


def stream_main_pid(unit: str) -> int:
    return runtime_safety_command.stream_main_pid(_runtime_safety_context(), unit)


def stream_ffmpeg_pid(main_pid: int) -> int:
    return runtime_safety_command.stream_ffmpeg_pid(_runtime_safety_context(), main_pid)


def running_stream_key() -> str:
    return runtime_safety_command.running_stream_key(
        _runtime_safety_context(),
        stream_main_pid_func=stream_main_pid,
        stream_ffmpeg_pid_func=stream_ffmpeg_pid,
    )


def guard_start_safety() -> int:
    return runtime_safety_command.guard_start_safety(_runtime_safety_context())


def _service_context() -> service_command.ServiceContext:
    return cli_contexts.service_context(sys.modules[__name__])


def ensure_installed() -> int:
    return service_command.ensure_installed(_service_context())


def install() -> int:
    return service_command.install(_service_context())


def start() -> int:
    return service_command.start(_service_context())


def stop() -> int:
    return service_command.stop(_service_context())


def restart() -> int:
    return service_command.restart(_service_context())


def enable() -> int:
    return service_command.enable(_service_context())


def _maintenance_context() -> maintenance_command.MaintenanceContext:
    return cli_contexts.maintenance_context(sys.modules[__name__])


def _maintenance_managed_units() -> tuple[str, ...]:
    return maintenance_command.managed_units(_maintenance_context())


def _maintenance_installed_timers() -> list[str]:
    return maintenance_command.installed_timers(_maintenance_context())


def _write_maintenance_state(payload: dict, path: Path | None = None) -> None:
    maintenance_command.write_state(_maintenance_context(), payload, path)


def _read_maintenance_state(path: Path | None = None) -> dict:
    return maintenance_command.read_state(_maintenance_context(), path)


def _maintenance_status_payload() -> dict:
    return maintenance_command.status_payload(_maintenance_context())


def maintenance_on(*, json_output: bool = False) -> int:
    return maintenance_command.on(_maintenance_context(), json_output=json_output)


def maintenance_off(*, json_output: bool = False) -> int:
    return maintenance_command.off(_maintenance_context(), json_output=json_output)


def maintenance_status(*, json_output: bool = False) -> int:
    return maintenance_command.status(_maintenance_context(), json_output=json_output)


def maintenance(action: str, *, json_output: bool = False) -> int:
    normalized = (action or "status").strip().lower()
    if normalized in {"on", "start", "pause", "enter"}:
        return maintenance_on(json_output=json_output)
    if normalized in {"off", "stop", "resume", "exit"}:
        return maintenance_off(json_output=json_output)
    if normalized in MAINTENANCE_STATUS_ACTIONS:
        return maintenance_status(json_output=json_output)
    print("[error] usage: stream m on|off|status")
    return 2


def watch() -> int:
    return service_command.watch(_service_context())


def status() -> int:
    return service_command.status(_service_context())


def logs(lines: int) -> int:
    return service_command.logs(_service_context(), lines)


_first_heading = history_command.first_heading


def _ops_history_entries(history_dir: Path, *, day: str = "", grep_text: str = "") -> list[Path]:
    return history_command.ops_history_entries(history_dir, day=day, grep_text=grep_text)


def _history_extra_dirs() -> tuple[Path, ...]:
    candidates = (
        BASE_DIR / "docs" / "v2" / "50_ops_logs",
        BASE_DIR / "docs" / "50_ops_logs",
    )
    return tuple(path for path in candidates if path != OPS_LOG_DIR)


def history(limit: int, *, day: str = "", grep_text: str = "", paths_only: bool = False) -> int:
    return history_command.history(
        BASE_DIR,
        OPS_LOG_DIR,
        limit,
        day=day,
        grep_text=grep_text,
        paths_only=paths_only,
        routine_check_dir=ROUTINE_CHECK_DIR,
        extra_history_dirs=_history_extra_dirs(),
    )


def _api_usage_context() -> api_usage_command.ApiUsageContext:
    return cli_contexts.api_usage_context(sys.modules[__name__])


file_mtime_age = api_usage_command.file_mtime_age


def api_report_effective_end_ts(payload: dict) -> int:
    return api_usage_command.api_report_effective_end_ts(_api_usage_context(), payload)


def api_report_freshness(path: Path, *, max_mtime_age_sec: int, max_effective_end_age_sec: int | None = None) -> dict:
    return api_usage_command.api_report_freshness(
        _api_usage_context(),
        path,
        max_mtime_age_sec=max_mtime_age_sec,
        max_effective_end_age_sec=max_effective_end_age_sec,
    )


def systemd_timer_status(unit: str) -> dict:
    return api_usage_command.systemd_timer_status(_api_usage_context(), unit)


def api_report_observation_payload() -> dict:
    return api_usage_command.api_report_observation_payload(_api_usage_context())


seconds_to_human = notify_incidents.seconds_to_human


ratio_percent = objective_sli_cli.ratio_percent
percentile = objective_sli_cli.percentile


def _notify_cli_context() -> notify_cli_adapter.NotifyCliContext:
    return cli_contexts.notify_cli_context(sys.modules[__name__])


def load_stream_notify_config() -> dict:
    return notify_cli_adapter.load_stream_notify_config(_notify_cli_context())


latest_jsonl_item = notify_incidents.latest_jsonl_item


def report_incident_spec(ident: str) -> tuple[Path, str] | None:
    return notify_cli_adapter.report_incident_spec(_notify_cli_context(), ident)


is_report_problem = notify_incidents.is_report_problem


compact_report_evidence = notify_incidents.compact_report_evidence


runtime_start_ts_from_run_id = notify_cli_adapter.runtime_start_ts_from_run_id


def latest_runtime_start_ts(state_base_dir: Path | None = None) -> int:
    return notify_cli_adapter.latest_runtime_start_ts(_notify_cli_context(), state_base_dir)


def notify_bootstrap_grace_active(now_ts: int, startup_grace_sec: int, *, state_base_dir: Path | None = None) -> bool:
    return notify_cli_adapter.notify_bootstrap_grace_active(
        _notify_cli_context(),
        now_ts,
        startup_grace_sec,
        state_base_dir=state_base_dir,
    )


recovery_type_from_observe = notify_incidents.recovery_type_from_observe


observe_payload_has_current_stream_problem = notify_incidents.observe_payload_has_current_stream_problem


def incident(
    *,
    ident: str,
    severity: str,
    component: str,
    summary: str,
    evidence: str,
    recovery_type: str,
    follow_up: str,
    observed_ts: int | None = None,
) -> dict:
    return notify_incidents.incident(
        ident=ident,
        severity=severity,
        component=component,
        summary=summary,
        evidence=evidence,
        recovery_type=recovery_type,
        follow_up=follow_up,
        observed_ts=observed_ts,
    )


def collect_notification_incidents(
    *,
    now_ts: int | None = None,
    report_stale_sec: int = 1800,
    startup_grace_sec: int = 0,
) -> list[dict]:
    now = int(time.time() if now_ts is None else now_ts)
    return notify_incidents.collect_notification_incidents(
        observe_payload=_observe_payload,
        stream1090_report_events_file=STREAM1090_REPORT_EVENTS_FILE,
        upstream_report_events_file=UPSTREAM_REPORT_EVENTS_FILE,
        youtube_watchdog_stats_file=YOUTUBE_WATCHDOG_STATS_FILE,
        now_ts=now,
        report_stale_sec=report_stale_sec,
        bootstrap_grace_active=notify_bootstrap_grace_active(now, startup_grace_sec),
    )


def load_notify_state(path: Path | None = None) -> dict:
    if path is None:
        path = NOTIFY_STATE_FILE
    return notify_state.load_notify_state(path)


def save_notify_state(state: dict, path: Path | None = None) -> None:
    if path is None:
        path = NOTIFY_STATE_FILE
    notify_state.save_notify_state(state, path)


def recovery_observation_for_incident(ident: str, now_ts: int) -> tuple[int, str]:
    return notify_cli_adapter.recovery_observation_for_incident(_notify_cli_context(), ident, now_ts)


def format_discord_message(*, phase: str, incidents: list[dict], state: dict, now_ts: int) -> str:
    return notify_renderer.format_discord_message(phase=phase, incidents=incidents, state=state, now_ts=now_ts)


def send_discord_webhook(webhook_url: str, content: str, *, username: str = "ADS-B Stream Watchdog", timeout: float = 10.0) -> tuple[bool, str]:
    return notify_discord.send_discord_webhook(webhook_url, content, username=username, timeout=timeout)


def load_notify_outbox(path: Path | None = None, *, now_ts: int | None = None, ttl_sec: int | None = None) -> list[dict]:
    if path is None:
        path = NOTIFY_OUTBOX_FILE
    return notify_outbox.load_notify_outbox(path, now_ts=now_ts, ttl_sec=ttl_sec)


def save_notify_outbox(rows: list[dict], path: Path | None = None) -> None:
    if path is None:
        path = NOTIFY_OUTBOX_FILE
    notify_outbox.save_notify_outbox(path, rows)


def notify_message_id(*, phase: str, incidents: list[dict], now_ts: int) -> str:
    return notify_outbox.notify_message_id(phase=phase, incidents=incidents, now_ts=now_ts)


def fast_recovery_auto_recovered_events(
    *,
    state: dict,
    now_ts: int,
    recent_sec: int,
    triggers: list[str],
    events_file: Path | None = None,
    max_events: int = 8,
) -> list[dict]:
    if events_file is None:
        events_file = FAST_RECOVERY_EVENTS_FILE
    return notify_status_loop.fast_recovery_auto_recovered_events(
        state=state,
        now_ts=now_ts,
        recent_sec=recent_sec,
        triggers=triggers,
        events_file=events_file,
        max_events=max_events,
    )


mark_fast_recovery_auto_recovered_events_notified = notify_status_loop.mark_fast_recovery_auto_recovered_events_notified


def enqueue_notify_messages(
    outbox: list[dict],
    messages: list[tuple[str, list[dict], str]],
    *,
    username: str,
    now_ts: int,
    max_pending: int,
) -> list[dict]:
    return notify_outbox.enqueue_notify_messages(
        outbox,
        messages,
        username=username,
        now_ts=now_ts,
        max_pending=max_pending,
    )


def flush_notify_outbox(*, cfg: dict, now_ts: int, dry_run: bool = False) -> tuple[int, int, int]:
    return notify_outbox.flush_notify_outbox(
        outbox_path=NOTIFY_OUTBOX_FILE,
        events_path=NOTIFY_EVENTS_FILE,
        cfg=cfg,
        now_ts=now_ts,
        send_webhook=send_discord_webhook,
        dry_run=dry_run,
    )


def maintenance_notification_incident(now_ts: int, state_file: Path | None = None) -> dict | None:
    return notify_cli_adapter.maintenance_notification_incident(_notify_cli_context(), now_ts, state_file)


def notify_maintenance_message_due(state: dict, item: dict, *, now_ts: int, repeat_sec: int, dry_run: bool) -> bool:
    return notify_status_loop.notify_maintenance_message_due(
        state,
        item,
        now_ts=now_ts,
        repeat_sec=repeat_sec,
        dry_run=dry_run,
    )


def _notify_status_context() -> notify_status_loop.NotifyStatusContext:
    return cli_contexts.notify_status_context(sys.modules[__name__])


def deliver_notify_messages(
    *,
    messages: list[tuple[str, list[dict]]],
    state: dict,
    cfg: dict,
    now_ts: int,
    dry_run: bool,
) -> tuple[int, int, int]:
    return notify_status_loop.deliver_notify_messages(
        ctx=_notify_status_context(),
        messages=messages,
        state=state,
        cfg=cfg,
        now_ts=now_ts,
        dry_run=dry_run,
    )


def notify_status(*, dry_run: bool = False, force_test: bool = False, now_ts: int | None = None) -> int:
    return notify_status_loop.notify_status(
        ctx=_notify_status_context(),
        dry_run=dry_run,
        force_test=force_test,
        now_ts=now_ts,
    )


def _oauth_status_context() -> oauth_command.OAuthStatusContext:
    return cli_contexts.oauth_status_context(sys.modules[__name__])


def load_youtube_oauth_config() -> dict[str, str]:
    return oauth_command.load_youtube_oauth_config(_oauth_status_context())


def oauth_authorization_url(cfg: dict[str, str]) -> str:
    return _oauth_status_context().authorization_url(cfg)


def oauth_status_payload(*, now_ts: int | None = None, live_probe: bool = False) -> dict:
    return oauth_command.oauth_status_payload(_oauth_status_context(), now_ts=now_ts, live_probe=live_probe)


def oauth_status(*, json_output: bool = False, live_probe: bool = False) -> int:
    return oauth_command.oauth_status(_oauth_status_context(), json_output=json_output, live_probe=live_probe)


def _health_context() -> health_command.HealthContext:
    return cli_contexts.health_context(sys.modules[__name__])


remote_warning_restart_judgment = health_command.remote_warning_restart_judgment


exit_224_judgment = health_command.exit_224_judgment


_compact_watchdog_event = health_command.compact_watchdog_event


def _nearest_watchdog_context(items: list[dict], ts: int) -> tuple[dict, dict]:
    return health_command.nearest_watchdog_context(_health_context(), items, ts)


def _remote_warning_comparison_payload(
    *,
    log_dir: Path = LOG_BASE_DIR,
    hours: int = 24,
    limit: int = 5,
    now_ts: int | None = None,
) -> dict:
    return health_command.remote_warning_comparison_payload(
        _health_context(),
        log_dir=log_dir,
        hours=hours,
        limit=limit,
        now_ts=now_ts,
    )


def remote_warning_compare(*, hours: int = 24, limit: int = 5, json_output: bool = False) -> int:
    return health_command.remote_warning_compare(_health_context(), hours=hours, limit=limit, json_output=json_output)


_parse_windows = health_command.parse_windows


def _observe_payload(hours: int) -> tuple[int, dict, str]:
    return health_command.observe_payload(_health_context(), hours)


def health_summary(*, windows: str = "1,8,24", json_output: bool = False) -> int:
    return health_command.health_summary(observe=_observe_payload, windows=windows, json_output=json_output)


OBJECTIVE_SLI_REGIMES = objective_sli_cli.OBJECTIVE_SLI_REGIMES
_timestamped_jsonl_items = objective_sli_cli.timestamped_jsonl_items
_time_bounds = objective_sli_cli.time_bounds
_youtube_sli_for_items = objective_sli_cli.youtube_sli_for_items
_stream_watchdog_sli = objective_sli_cli.stream_watchdog_sli
_fast_recovery_sli = objective_sli_cli.fast_recovery_sli
_upload_budget_sli = objective_sli_cli.upload_budget_sli
_stream_engine_sli = objective_sli_cli.stream_engine_sli
_report_only_sli = objective_sli_cli.report_only_sli
_discord_notify_sli = objective_sli_cli.discord_notify_sli
_api_usage_sli = objective_sli_cli.api_usage_sli
_daily_objective_sli = objective_sli_cli.daily_objective_sli


def _objective_sli_context() -> objective_sli_cli.ObjectiveSliContext:
    return cli_contexts.objective_sli_context(sys.modules[__name__])


def objective_sli_payload(*, now_ts: int | None = None) -> dict:
    return objective_sli_cli.objective_sli_payload(_objective_sli_context(), now_ts=now_ts)


def save_objective_sli(payload: dict) -> None:
    objective_sli_cli.save_objective_sli(_objective_sli_context(), payload)


def objective_sli(*, json_output: bool = False, record: bool = True) -> int:
    return objective_sli_cli.objective_sli(_objective_sli_context(), json_output=json_output, record=record)


def _memory_status_context() -> memory_status_cli.MemoryStatusContext:
    return cli_contexts.memory_status_context(sys.modules[__name__])


def memory_status(*, json_output: bool = False, record: bool = True) -> int:
    return memory_status_cli.memory_status(_memory_status_context(), json_output=json_output, record=record)


def _resource_memory_context() -> resource_memory_cli.ResourceMemoryContext:
    return cli_contexts.resource_memory_context(sys.modules[__name__])


def resource_memory(*, json_output: bool = False, record: bool = True) -> int:
    return resource_memory_cli.resource_memory(_resource_memory_context(), json_output=json_output, record=record)


def _shadow_context() -> shadow_cli.ShadowCommandContext:
    return cli_contexts.shadow_context(sys.modules[__name__])


def _stream_v2_shadow_result(*, record: bool = True):
    return shadow_cli.run_shadow_pipeline(_shadow_context(), record=record)


def _stream_v2_subsystems_status_result(*, record: bool = True):
    return shadow_cli.run_subsystems_status_pipeline(_shadow_context(), record=record)


def _stream_v2_recovery_orchestrator_result(*, record: bool = True):
    return shadow_cli.run_recovery_orchestrator_pipeline(_shadow_context(), record=record)


def subsystems_status(*, json_output: bool = False, record: bool = True) -> int:
    return shadow_cli.subsystems_status(
        _shadow_context(),
        json_output=json_output,
        record=record,
        shadow_runner=lambda should_record: _stream_v2_subsystems_status_result(record=should_record),
    )


def recovery_orchestrator(*, json_output: bool = False, record: bool = True) -> int:
    return shadow_cli.recovery_orchestrator(
        _shadow_context(),
        json_output=json_output,
        record=record,
        shadow_runner=lambda should_record: _stream_v2_recovery_orchestrator_result(record=should_record),
    )


def shadow_once(*, json_output: bool = False, record: bool = True) -> int:
    return shadow_cli.shadow_once(
        _shadow_context(),
        json_output=json_output,
        record=record,
        shadow_runner=lambda should_record: _stream_v2_shadow_result(record=should_record),
    )


def shadow_sli(*, json_output: bool = False) -> int:
    return shadow_cli.shadow_sli(_shadow_context(), json_output=json_output)


def _stream1090_report_context() -> stream1090_report_command.Stream1090ReportContext:
    return cli_contexts.stream1090_report_context(sys.modules[__name__])


_url_at = stream1090_report_command.url_at


_stream1090_resource_url = stream1090_report_command.stream1090_resource_url


fetch_url_text = stream1090_report_command.fetch_url_text


fetch_url_json = stream1090_report_command.fetch_url_json


_aircraft_position_map = stream1090_report_command.aircraft_position_map


_outline_points_count = stream1090_report_command.outline_points_count


_chromium_binary = stream1090_report_command.chromium_binary


_screenshot_mean_luma = stream1090_report_command.screenshot_mean_luma


def _visual_probe_payload(
    *,
    page_url: str,
    target: str,
    timeout: float,
    screenshot_dir: Path = STREAM1090_VISUAL_DIR,
) -> dict:
    return stream1090_report_command.visual_probe_payload(
        _stream1090_report_context(),
        page_url=page_url,
        target=target,
        timeout=timeout,
        screenshot_dir=screenshot_dir,
        chromium_binary_func=_chromium_binary,
        screenshot_mean_luma_func=_screenshot_mean_luma,
    )


def _report_history_summary(log_file: Path, *, target: str, hours: int = 24, include_payload: dict | None = None) -> dict:
    return stream1090_report_command.report_history_summary(
        _stream1090_report_context(),
        log_file,
        target=target,
        hours=hours,
        include_payload=include_payload,
    )


def _stream1090_report_payload(
    *,
    base_url: str = "http://127.0.0.1:18080",
    map_path: str = "/stream1090/",
    target: str = "overlay_stream1090",
    sample_sec: float = 5.0,
    timeout: float = 5.0,
    sleep_func=time.sleep,
    fetch_text_func=fetch_url_text,
    fetch_json_func=fetch_url_json,
    visual: bool = False,
    visual_fetch_func=_visual_probe_payload,
) -> dict:
    return stream1090_report_command.stream1090_report_payload(
        base_url=base_url,
        map_path=map_path,
        target=target,
        sample_sec=sample_sec,
        timeout=timeout,
        sleep_func=sleep_func,
        fetch_text_func=fetch_text_func,
        fetch_json_func=fetch_json_func,
        visual=visual,
        visual_fetch_func=visual_fetch_func,
    )


def stream1090_report(
    *,
    base_url: str = "http://127.0.0.1:18080",
    sample_sec: float = 5.0,
    timeout: float = 5.0,
    visual: bool = False,
    record: bool = True,
    json_output: bool = False,
) -> int:
    return stream1090_report_command.stream1090_report(
        _stream1090_report_context(),
        payload_func=_stream1090_report_payload,
        base_url=base_url,
        sample_sec=sample_sec,
        timeout=timeout,
        visual=visual,
        record=record,
        json_output=json_output,
    )


def upstream_report(
    *,
    upstream_url: str = "",
    sample_sec: float = 5.0,
    timeout: float = 5.0,
    visual: bool = False,
    record: bool = True,
    json_output: bool = False,
) -> int:
    return stream1090_report_command.upstream_report(
        _stream1090_report_context(),
        split_url_root_and_path=split_url_root_and_path,
        payload_func=_stream1090_report_payload,
        upstream_url=upstream_url,
        sample_sec=sample_sec,
        timeout=timeout,
        visual=visual,
        record=record,
        json_output=json_output,
    )


def _api_usage_report_command(*, closed_day: bool, day: str) -> list[str]:
    return api_usage_command.report_command(_api_usage_context(), closed_day=closed_day, day=day)


def _format_api_usage_summary(payload: dict, quota_state: dict, watchdog_stats: dict, report_observation: dict) -> str:
    return api_usage_command.format_summary(payload, quota_state, watchdog_stats, report_observation)


def api_usage(*, closed_day: bool = False, day: str = "", json_output: bool = False) -> int:
    return api_usage_command.api_usage(
        _api_usage_context(),
        closed_day=closed_day,
        day=day,
        json_output=json_output,
    )


def _doctor_context() -> doctor_command.DoctorContext:
    return cli_contexts.doctor_context(sys.modules[__name__])


def needrestart_contract_status(path: Path = Path("/etc/needrestart/conf.d/stream-24x7.conf")) -> dict:
    return doctor_command.needrestart_contract_status(path)


def pipewire_canary_status() -> dict:
    return doctor_command.pipewire_canary_status(_doctor_context())


def doctor() -> int:
    return doctor_command.doctor(_doctor_context())


def contract_check(*, json_output: bool = False) -> int:
    return doctor_command.contract_check(_doctor_context(), json_output=json_output)


def parse_args() -> argparse.Namespace:
    return cli_parser.parse_args()


def _cli_router() -> cli_router.CliRouter:
    return cli_router.CliRouter(
        maintenance_top_level_actions=MAINTENANCE_TOP_LEVEL_ACTIONS,
        maintenance_command_aliases=MAINTENANCE_COMMAND_ALIASES,
        guard_mutating_command=guard_stream_v2_mutating_command,
        install=install,
        start=start,
        stop=stop,
        restart=restart,
        maintenance=maintenance,
        enable=enable,
        watch=watch,
        status=status,
        logs=logs,
        history=history,
        api_usage=api_usage,
        health_summary=health_summary,
        objective_sli=objective_sli,
        memory_status=memory_status,
        resource_memory=resource_memory,
        subsystems_status=subsystems_status,
        recovery_orchestrator=recovery_orchestrator,
        shadow_once=shadow_once,
        shadow_sli=shadow_sli,
        oauth_status=oauth_status,
        remote_warning_compare=remote_warning_compare,
        stream1090_report=stream1090_report,
        upstream_report=upstream_report,
        notify_status=notify_status,
        doctor=doctor,
        contract_check=contract_check,
    )


def main() -> int:
    args = parse_args()
    return cli_router.dispatch(args, _cli_router())


if __name__ == "__main__":
    raise SystemExit(main())
