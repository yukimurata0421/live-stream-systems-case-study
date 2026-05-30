from __future__ import annotations

import os


def runtime_safety_context(ns):
    from stream_core.commands import runtime_safety as command

    return command.RuntimeSafetyContext(
        base_dir=ns.BASE_DIR,
        stream_service=ns.STREAM_SERVICE,
        legacy_stream_service=ns.LEGACY_STREAM_SERVICE,
        read_env_file=ns.read_env_file,
        run=ns.run,
        run_systemctl=ns.run_systemctl,
        is_active=ns.is_active,
    )


def service_context(ns):
    from stream_core.commands import service as command
    from stream_core.supervisor.factory import build_runtime_supervisor

    return command.ServiceContext(
        systemd_src_dir=ns.SYSTEMD_SRC_DIR,
        install_targets=ns.INSTALL_TARGETS,
        system_units=ns.SYSTEM_UNITS,
        all_units=ns.ALL_UNITS,
        notify_env_file=ns.NOTIFY_ENV_FILE,
        dj_service=ns.DJ_SERVICE,
        stream_service=ns.STREAM_SERVICE,
        watchdog_timer=ns.WATCHDOG_TIMER,
        watchdog_service=ns.WATCHDOG_SERVICE,
        youtube_monitor_timer=ns.YTW_MONITOR_TIMER,
        youtube_monitor_service=ns.YTW_MONITOR_SERVICE,
        youtube_video_resolver_timer=ns.YTW_VIDEO_RESOLVER_TIMER,
        youtube_video_resolver_service=ns.YTW_VIDEO_RESOLVER_SERVICE,
        fast_recovery_timer=ns.FAST_RECOVERY_TIMER,
        fast_recovery_service=ns.FAST_RECOVERY_SERVICE,
        stream1090_report_timer=ns.STREAM1090_REPORT_TIMER,
        stream1090_report_service=ns.STREAM1090_REPORT_SERVICE,
        upstream_report_timer=ns.UPSTREAM_REPORT_TIMER,
        upstream_report_service=ns.UPSTREAM_REPORT_SERVICE,
        subsystems_status_timer=ns.SUBSYSTEMS_STATUS_TIMER,
        subsystems_status_service=ns.SUBSYSTEMS_STATUS_SERVICE,
        recovery_orchestrator_timer=ns.RECOVERY_ORCHESTRATOR_TIMER,
        recovery_orchestrator_service=ns.RECOVERY_ORCHESTRATOR_SERVICE,
        memory_status_timer=ns.MEMORY_STATUS_TIMER,
        memory_status_service=ns.MEMORY_STATUS_SERVICE,
        resource_memory_timer=ns.RESOURCE_MEMORY_TIMER,
        resource_memory_service=ns.RESOURCE_MEMORY_SERVICE,
        notify_timer=ns.NOTIFY_TIMER,
        notify_service=ns.NOTIFY_SERVICE,
        run=ns.run,
        run_systemctl=ns.run_systemctl,
        unit_installed=ns.unit_installed,
        is_active=ns.is_active,
        start_unit=ns.start_unit,
        restart_unit=ns.restart_unit,
        trigger_unit=ns.trigger_unit,
        enable_unit=ns.enable_unit,
        guard_start_safety=ns.guard_start_safety,
        expected_stream_key=ns.expected_stream_key,
        running_stream_key=ns.running_stream_key,
        youtube_watchdog_unhealthy=ns.youtube_watchdog_unhealthy,
        print_systemctl_error=ns.print_systemctl_error,
        supervisor_mode=os.environ.get("STREAM_RUNTIME_SUPERVISOR", "systemd"),
        runtime_supervisor=build_runtime_supervisor(
            run_systemctl=lambda args, check: ns.run_systemctl(args, check=check),
        ),
    )


def maintenance_context(ns):
    from stream_core.commands import maintenance as command

    return command.MaintenanceContext(
        state_file=ns.MAINTENANCE_STATE_FILE,
        timers=ns.MAINTENANCE_TIMERS,
        services=ns.MAINTENANCE_SERVICES,
        status_actions=ns.MAINTENANCE_STATUS_ACTIONS,
        notify_timer=ns.NOTIFY_TIMER,
        unit_installed=ns.unit_installed,
        is_active=ns.is_active,
        start_unit=ns.start_unit,
        stop_unit=ns.stop_unit,
    )


def api_usage_context(ns):
    from stream_core.commands import api_usage as command

    return command.ApiUsageContext(
        api_cost_report_script=ns.API_COST_REPORT_SCRIPT,
        youtube_quota_state_file=ns.YOUTUBE_QUOTA_STATE_FILE,
        youtube_watchdog_stats_file=ns.YOUTUBE_WATCHDOG_STATS_FILE,
        youtube_api_cost_open_day_latest_file=ns.YOUTUBE_API_COST_OPEN_DAY_LATEST_FILE,
        youtube_api_cost_latest_file=ns.YOUTUBE_API_COST_LATEST_FILE,
        api_cost_open_day_report_timer=ns.API_COST_OPEN_DAY_REPORT_TIMER,
        api_cost_report_timer=ns.API_COST_REPORT_TIMER,
        run=ns.run,
        read_json_file=ns.read_json_file,
        parse_utc_ts=ns.parse_utc_ts,
    )


def notify_cli_context(ns):
    from stream_core.notifications import cli_adapter as adapter

    return adapter.NotifyCliContext(
        base_dir=ns.BASE_DIR,
        state_base_dir=ns.STATE_BASE_DIR,
        notify_env_file=ns.NOTIFY_ENV_FILE,
        notify_timer=ns.NOTIFY_TIMER,
        maintenance_state_file=ns.MAINTENANCE_STATE_FILE,
        stream1090_report_events_file=ns.STREAM1090_REPORT_EVENTS_FILE,
        upstream_report_events_file=ns.UPSTREAM_REPORT_EVENTS_FILE,
        youtube_watchdog_stats_file=ns.YOUTUBE_WATCHDOG_STATS_FILE,
        read_env_file=ns.read_env_file,
        read_json_file=ns.read_json_file,
        parse_bool=ns.parse_bool,
        parse_utc_ts=ns.parse_utc_ts,
        read_maintenance_state=ns._read_maintenance_state,
        observe_payload=ns._observe_payload,
    )


def notify_status_context(ns):
    from stream_core.notifications import status_loop

    return status_loop.NotifyStatusContext(
        notify_events_file=ns.NOTIFY_EVENTS_FILE,
        notify_outbox_file=ns.NOTIFY_OUTBOX_FILE,
        load_config=ns.load_stream_notify_config,
        load_state=ns.load_notify_state,
        save_state=ns.save_notify_state,
        collect_incidents=ns.collect_notification_incidents,
        recovery_observation_for_incident=ns.recovery_observation_for_incident,
        format_message=ns.format_discord_message,
        send_webhook=ns.send_discord_webhook,
        maintenance_notification_incident=ns.maintenance_notification_incident,
        fast_recovery_events_file=ns.FAST_RECOVERY_EVENTS_FILE,
    )


def oauth_status_context(ns):
    from stream_core.commands import oauth as command
    from watchers.youtube_oauth import status as oauth_status_model

    return command.OAuthStatusContext(
        base_dir=ns.BASE_DIR,
        state_base_dir=ns.STATE_BASE_DIR,
        youtube_monitor_env_file=ns.YOUTUBE_MONITOR_ENV_FILE,
        youtube_watchdog_stats_file=ns.YOUTUBE_WATCHDOG_STATS_FILE,
        read_env_file=ns.read_env_file,
        read_json_file=ns.read_json_file,
        parse_bool=ns.parse_bool,
        utc_now_text=ns.utc_now_text,
        authorization_url=oauth_status_model.authorization_url,
        build_status_payload=oauth_status_model.build_status_payload,
        attach_live_probe=oauth_status_model.attach_live_probe,
    )


def health_context(ns):
    from stream_core.commands import health as command

    return command.HealthContext(
        observe_stream_health_script=ns.OBSERVE_STREAM_HEALTH_SCRIPT,
        log_base_dir=ns.LOG_BASE_DIR,
        fast_recovery_events_file=ns.FAST_RECOVERY_EVENTS_FILE,
        youtube_watchdog_events_file=ns.YOUTUBE_WATCHDOG_EVENTS_FILE,
        run=ns.run,
        iter_jsonl=ns.iter_jsonl,
        parse_utc_ts=ns.parse_utc_ts,
    )


def objective_sli_context(ns):
    from stream_core.cli_support import objective_sli

    return objective_sli.ObjectiveSliContext(
        log_base_dir=ns.LOG_BASE_DIR,
        youtube_watchdog_events_file=ns.YOUTUBE_WATCHDOG_EVENTS_FILE,
        fast_recovery_events_file=ns.FAST_RECOVERY_EVENTS_FILE,
        stream_engine_events_file=ns.STREAM_ENGINE_EVENTS_FILE,
        stream1090_report_events_file=ns.STREAM1090_REPORT_EVENTS_FILE,
        upstream_report_events_file=ns.UPSTREAM_REPORT_EVENTS_FILE,
        notify_events_file=ns.NOTIFY_EVENTS_FILE,
        memory_status_events_file=ns.MEMORY_STATUS_EVENTS_FILE,
        objective_sli_file=ns.OBJECTIVE_SLI_FILE,
        objective_sli_events_file=ns.OBJECTIVE_SLI_EVENTS_FILE,
    )


def memory_status_context(ns):
    from stream_core.cli_support import memory_status
    from stream_core.common import systemd as systemd_common

    return memory_status.MemoryStatusContext(
        memory_status_file=ns.MEMORY_STATUS_FILE,
        memory_status_events_file=ns.MEMORY_STATUS_EVENTS_FILE,
        service_units=ns.MEMORY_STATUS_SERVICE_UNITS,
        run_systemctl_readonly=lambda args, check: systemd_common.run_systemctl_readonly(args, check=check),
    )


def resource_memory_context(ns):
    from stream_core.cli_support import resource_memory
    from stream_core.common import systemd as systemd_common

    return resource_memory.ResourceMemoryContext(
        resource_memory_file=ns.RESOURCE_MEMORY_FILE,
        resource_memory_events_file=ns.RESOURCE_MEMORY_EVENTS_FILE,
        resource_memory_assessment_file=ns.RESOURCE_MEMORY_ASSESSMENT_FILE,
        memory_status_events_file=ns.MEMORY_STATUS_EVENTS_FILE,
        service_units=ns.MEMORY_STATUS_SERVICE_UNITS,
        run_systemctl_readonly=lambda args, check: systemd_common.run_systemctl_readonly(args, check=check),
        state_base_dir=ns.STATE_BASE_DIR,
        log_base_dir=ns.LOG_BASE_DIR,
    )


def shadow_context(ns):
    from stream_core.cli_support import shadow

    return shadow.ShadowCommandContext(
        state_base_dir=ns.STATE_BASE_DIR,
        subsystems_status_file=ns.SUBSYSTEMS_STATUS_FILE,
        subsystems_status_events_file=ns.SUBSYSTEMS_STATUS_EVENTS_FILE,
        recovery_orchestrator_events_file=ns.RECOVERY_ORCHESTRATOR_EVENTS_FILE,
        recovery_action_plan_file=ns.RECOVERY_ACTION_PLAN_FILE,
    )


def stream1090_report_context(ns):
    from stream_core.commands import stream1090_report as command

    return command.Stream1090ReportContext(
        stream1090_report_events_file=ns.STREAM1090_REPORT_EVENTS_FILE,
        upstream_report_events_file=ns.UPSTREAM_REPORT_EVENTS_FILE,
        stream1090_visual_dir=ns.STREAM1090_VISUAL_DIR,
        run=ns.run,
        append_jsonl=ns.append_jsonl,
        iter_jsonl=ns.iter_jsonl,
        parse_utc_ts=ns.parse_utc_ts,
        default_upstream_url=ns.default_stream1090_upstream_url,
    )


def doctor_context(ns):
    from stream_core.commands import doctor as command

    return command.DoctorContext(
        base_dir=ns.BASE_DIR,
        read_env_file=ns.read_env_file,
        parse_bool=ns.parse_bool,
        run=ns.run,
        needrestart_status=ns.needrestart_contract_status,
        ingest_status=ns.stream_ingest_endpoint_status,
        pipewire_status=ns.pipewire_canary_status,
    )
