from __future__ import annotations

import argparse
from dataclasses import dataclass
from typing import Callable


@dataclass(frozen=True)
class CliRouter:
    maintenance_top_level_actions: dict[str, str]
    maintenance_command_aliases: set[str]
    guard_mutating_command: Callable[[str, str], int]
    install: Callable[[], int]
    start: Callable[[], int]
    stop: Callable[[], int]
    restart: Callable[[], int]
    maintenance: Callable[..., int]
    enable: Callable[[], int]
    watch: Callable[[], int]
    status: Callable[[], int]
    logs: Callable[[int], int]
    history: Callable[..., int]
    api_usage: Callable[..., int]
    health_summary: Callable[..., int]
    objective_sli: Callable[..., int]
    memory_status: Callable[..., int]
    resource_memory: Callable[..., int]
    subsystems_status: Callable[..., int]
    recovery_orchestrator: Callable[..., int]
    shadow_once: Callable[..., int]
    shadow_sli: Callable[..., int]
    oauth_status: Callable[..., int]
    remote_warning_compare: Callable[..., int]
    stream1090_report: Callable[..., int]
    upstream_report: Callable[..., int]
    notify_status: Callable[..., int]
    doctor: Callable[[], int]
    contract_check: Callable[..., int]


def dispatch(args: argparse.Namespace, router: CliRouter) -> int:
    cmd = args.command
    if cmd in router.maintenance_top_level_actions and args.maintenance_action:
        print(f"[error] usage: stream {cmd}")
        return 2
    guarded = router.guard_mutating_command(cmd, getattr(args, "maintenance_action", ""))
    if guarded != 0:
        return guarded
    if cmd == "install":
        return router.install()
    if cmd == "start":
        return router.start()
    if cmd == "stop":
        return router.stop()
    if cmd == "restart":
        return router.restart()
    if cmd in router.maintenance_command_aliases:
        return router.maintenance(args.maintenance_action, json_output=args.json)
    if cmd in router.maintenance_top_level_actions:
        return router.maintenance(router.maintenance_top_level_actions[cmd], json_output=args.json)
    if cmd == "enable":
        return router.enable()
    if cmd == "watch":
        return router.watch()
    if cmd == "status":
        return router.status()
    if cmd == "logs":
        return router.logs(args.lines)
    if cmd == "history":
        return router.history(args.limit, day=args.day, grep_text=args.grep, paths_only=args.paths)
    if cmd == "api-usage":
        return router.api_usage(closed_day=args.closed_day, day=args.day, json_output=args.json)
    if cmd == "health-summary":
        return router.health_summary(windows=args.windows, json_output=args.json)
    if cmd == "objective-sli":
        return router.objective_sli(json_output=args.json, record=not args.no_record)
    if cmd == "memory-status":
        return router.memory_status(json_output=args.json, record=not args.no_record)
    if cmd == "resource-memory":
        return router.resource_memory(json_output=args.json, record=not args.no_record)
    if cmd == "subsystems-status":
        return router.subsystems_status(json_output=args.json, record=not args.no_record)
    if cmd == "recovery-orchestrator":
        return router.recovery_orchestrator(json_output=args.json, record=not args.no_record)
    if cmd == "shadow-once":
        return router.shadow_once(json_output=args.json, record=not args.no_record)
    if cmd == "shadow-sli":
        return router.shadow_sli(json_output=args.json)
    if cmd == "oauth-status":
        return router.oauth_status(json_output=args.json, live_probe=args.probe)
    if cmd == "remote-warning-compare":
        return router.remote_warning_compare(hours=args.hours, limit=args.limit, json_output=args.json)
    if cmd == "stream1090-report":
        return router.stream1090_report(
            base_url=args.base_url,
            sample_sec=args.sample_sec,
            timeout=args.timeout,
            visual=args.visual,
            record=not args.no_record,
            json_output=args.json,
        )
    if cmd == "upstream-report":
        return router.upstream_report(
            upstream_url=args.upstream_url,
            sample_sec=args.sample_sec,
            timeout=args.timeout,
            visual=args.visual,
            record=not args.no_record,
            json_output=args.json,
        )
    if cmd == "notify-status":
        return router.notify_status(dry_run=args.dry_run, force_test=args.force_test)
    if cmd == "doctor":
        return router.doctor()
    if cmd == "contract-check":
        return router.contract_check(json_output=args.json)
    return 2
