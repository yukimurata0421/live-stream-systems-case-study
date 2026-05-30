from __future__ import annotations

import argparse


COMMAND_CHOICES = (
    "install",
    "start",
    "stop",
    "restart",
    "maintenance",
    "maint",
    "m",
    "pause",
    "resume",
    "enable",
    "watch",
    "status",
    "logs",
    "history",
    "api-usage",
    "health-summary",
    "objective-sli",
    "memory-status",
    "resource-memory",
    "subsystems-status",
    "recovery-orchestrator",
    "shadow-once",
    "shadow-sli",
    "oauth-status",
    "remote-warning-compare",
    "stream1090-report",
    "upstream-report",
    "notify-status",
    "doctor",
    "contract-check",
)

MAINTENANCE_ACTION_CHOICES = ("on", "off", "status", "start", "stop", "pause", "resume", "enter", "exit", "show", "s")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="stream-new orchestrator")
    parser.add_argument("command", nargs="?", default="start", choices=COMMAND_CHOICES)
    parser.add_argument(
        "maintenance_action",
        nargs="?",
        default="status",
        choices=MAINTENANCE_ACTION_CHOICES,
        help="maintenance: on/off/status (command aliases: m/maint; action aliases: pause/resume/s)",
    )
    parser.add_argument("--lines", type=int, default=120)
    parser.add_argument("--limit", type=int, default=20, help="history: max entries to show")
    parser.add_argument("--day", default="", help="history/API usage day in YYYY-MM-DD")
    parser.add_argument("--grep", default="", help="history: filter history docs by filename or body text")
    parser.add_argument("--paths", action="store_true", help="history: print absolute file paths only")
    parser.add_argument("--closed-day", action="store_true", help="api-usage: report latest closed PT day")
    parser.add_argument("--json", action="store_true", help="print JSON payload for supported commands")
    parser.add_argument("--hours", type=int, default=24, help="remote-warning-compare: observation window")
    parser.add_argument("--windows", default="1,8,24", help="health-summary: comma-separated hour windows")
    parser.add_argument("--base-url", default="http://127.0.0.1:18080", help="stream1090-report: overlay base URL")
    parser.add_argument("--upstream-url", default="", help="upstream-report: upstream stream1090 URL")
    parser.add_argument("--sample-sec", type=float, default=5.0, help="stream1090-report: movement sample interval")
    parser.add_argument("--timeout", type=float, default=5.0, help="stream1090-report: HTTP timeout seconds")
    parser.add_argument("--visual", action="store_true", help="stream1090/upstream-report: include chromium screenshot visual probe")
    parser.add_argument(
        "--no-record",
        action="store_true",
        help="stream1090/upstream-report/objective-sli/memory-status/resource-memory/subsystems-status/recovery-orchestrator/shadow-once: do not append or write history",
    )
    parser.add_argument("--dry-run", action="store_true", help="notify-status: print notification payload without sending")
    parser.add_argument("--force-test", action="store_true", help="notify-status: send a test notification even without incidents")
    parser.add_argument("--probe", action="store_true", help="oauth-status: run a live OAuth probe")
    return parser


def parse_args() -> argparse.Namespace:
    return build_parser().parse_args()
