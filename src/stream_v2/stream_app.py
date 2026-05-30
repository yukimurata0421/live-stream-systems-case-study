from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from typing import Sequence


STREAM_V2_ROOT = Path(__file__).resolve().parents[2]
STREAM_APP_ROOT = STREAM_V2_ROOT
STREAM_APP_BIN = STREAM_APP_ROOT / "bin" / "stream-new"
ALLOW_MUTATING_ENV = "STREAM_V2_ALLOW_MUTATING_SYSTEMD"
MUTATING_APP_COMMANDS = frozenset({"install", "start", "stop", "restart", "enable", "watch"})
MAINTENANCE_APP_COMMANDS = frozenset({"maintenance", "maint", "m"})
MAINTENANCE_STATUS_ACTIONS = frozenset({"status", "show", "s"})
MAINTENANCE_TOP_LEVEL_ACTIONS = {
    "pause": "on",
    "resume": "off",
}
APP_OPTIONS_WITH_VALUE = frozenset({"--lines"})


def stream_app_root() -> Path:
    return STREAM_APP_ROOT


def _normalized_app_args(args: Sequence[str]) -> list[str]:
    normalized = list(args)
    if normalized and normalized[0] == "--":
        return normalized[1:]
    return normalized


def app_cli_command(args: Sequence[str]) -> str | None:
    """Return the stream-new subcommand without treating option values as commands."""
    normalized = _normalized_app_args(args)
    skip_next = False
    for arg in normalized:
        if skip_next:
            skip_next = False
            continue
        if arg in {"-h", "--help"}:
            return None
        if arg in APP_OPTIONS_WITH_VALUE:
            skip_next = True
            continue
        if any(arg.startswith(f"{option}=") for option in APP_OPTIONS_WITH_VALUE):
            continue
        if arg.startswith("-"):
            continue
        return arg
    return None


def app_cli_positionals(args: Sequence[str]) -> list[str]:
    normalized = _normalized_app_args(args)
    positionals: list[str] = []
    skip_next = False
    for arg in normalized:
        if skip_next:
            skip_next = False
            continue
        if arg in {"-h", "--help"}:
            return []
        if arg in APP_OPTIONS_WITH_VALUE:
            skip_next = True
            continue
        if any(arg.startswith(f"{option}=") for option in APP_OPTIONS_WITH_VALUE):
            continue
        if arg.startswith("-"):
            continue
        positionals.append(arg)
    return positionals


def is_mutating_app_command(args: Sequence[str]) -> bool:
    positionals = app_cli_positionals(args)
    command = positionals[0] if positionals else None
    if command in MAINTENANCE_APP_COMMANDS:
        action = positionals[1] if len(positionals) > 1 else "status"
        return action not in MAINTENANCE_STATUS_ACTIONS
    if command in MAINTENANCE_TOP_LEVEL_ACTIONS:
        return MAINTENANCE_TOP_LEVEL_ACTIONS[command] not in MAINTENANCE_STATUS_ACTIONS
    return command in MUTATING_APP_COMMANDS


def mutating_app_cli_allowed(*, allow_mutating: bool = False) -> bool:
    if allow_mutating:
        return True
    return os.environ.get(ALLOW_MUTATING_ENV) == "1"


def run_stream_cli(args: Sequence[str], *, allow_mutating: bool = False) -> int:
    normalized_args = _normalized_app_args(args)
    if is_mutating_app_command(normalized_args) and not mutating_app_cli_allowed(allow_mutating=allow_mutating):
        command = app_cli_command(normalized_args) or "unknown"
        print(
            (
                f"refusing stream_v2 mutating command '{command}'; "
                f"use --allow-mutating or {ALLOW_MUTATING_ENV}=1 only during an explicit cutover window"
            ),
            file=sys.stderr,
        )
        return 2
    env = os.environ.copy()
    env.setdefault("STREAM_BASE_DIR", str(STREAM_APP_ROOT))
    if allow_mutating:
        env[ALLOW_MUTATING_ENV] = "1"
    completed = subprocess.run([str(STREAM_APP_BIN), *normalized_args], env=env, check=False)
    return int(completed.returncode)
