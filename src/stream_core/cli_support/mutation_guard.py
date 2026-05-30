from __future__ import annotations

import os
from typing import Callable


STREAM_V2_MUTATING_COMMANDS = {"install", "start", "stop", "restart", "enable", "watch"}
MAINTENANCE_COMMAND_ALIASES = {"maintenance", "maint", "m"}
MAINTENANCE_STATUS_ACTIONS = {"status", "show", "s"}
MAINTENANCE_TOP_LEVEL_ACTIONS = {
    "pause": "on",
    "resume": "off",
}
STREAM_V2_ALLOW_MUTATING_ENV = "STREAM_V2_ALLOW_MUTATING_SYSTEMD"


def mutating_systemd_allowed(*, env: dict[str, str] | None = None) -> bool:
    source = os.environ if env is None else env
    raw = source.get(STREAM_V2_ALLOW_MUTATING_ENV, "").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def command_requires_mutating_systemd(command: str, maintenance_action: str = "") -> bool:
    command = command.strip().lower()
    if command in MAINTENANCE_COMMAND_ALIASES:
        return (maintenance_action or "status").strip().lower() not in MAINTENANCE_STATUS_ACTIONS
    if command in MAINTENANCE_TOP_LEVEL_ACTIONS:
        return MAINTENANCE_TOP_LEVEL_ACTIONS[command] not in MAINTENANCE_STATUS_ACTIONS
    return command in STREAM_V2_MUTATING_COMMANDS


def guard_stream_v2_mutating_command(
    command: str,
    maintenance_action: str = "",
    *,
    in_stream_v2_tree: Callable[[], bool],
    mutating_allowed: Callable[[], bool] = mutating_systemd_allowed,
) -> int:
    if not command_requires_mutating_systemd(command, maintenance_action):
        return 0
    if not in_stream_v2_tree():
        return 0
    if mutating_allowed():
        return 0
    print(f"[error] refusing '{command}' because it can mutate production-shaped systemd units")
    print(f"[hint] run test-mode code paths directly, or set {STREAM_V2_ALLOW_MUTATING_ENV}=1 only during an explicit cutover window")
    return 1
