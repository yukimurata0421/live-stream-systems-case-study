#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import stat
from collections import OrderedDict
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SOURCE_ENV_DIR = Path("/etc/default")
DEFAULT_TARGET_ENV_DIR = REPO_ROOT / ".state" / "env"
SOURCE_PROJECT_ROOT = Path("/home/yuki/projects/stream")
TARGET_PROJECT_ROOT = REPO_ROOT
SOURCE_STATE_ROOT = Path.home() / ".local" / "state" / "adsb-streamnew"
TARGET_STATE_NAME = "adsb-streamnew-v3" if REPO_ROOT.name == "stream_v3" else "adsb-streamnew-v2"
TARGET_STATE_ROOT = REPO_ROOT / ".state" / TARGET_STATE_NAME

ENV_FILES = OrderedDict(
    (
        ("adsb-streamnew", "adsb-streamnew.env"),
        ("adsb-streamnew-youtube-monitor", "adsb-streamnew-youtube-monitor.env"),
        ("adsb-streamnew-fast-recovery", "adsb-streamnew-fast-recovery.env"),
        ("adsb-streamnew-notify", "adsb-streamnew-notify.env"),
    )
)

MAIN_ENV_OVERRIDES = {
    "BASE_DIR": str(TARGET_PROJECT_ROOT),
    "STREAM_BASE_DIR": str(TARGET_PROJECT_ROOT),
    "STREAM_RUNTIME_STATE_DIR": str(TARGET_STATE_ROOT),
    "STREAM_RUNTIME_LOG_DIR": str(TARGET_STATE_ROOT / "logs"),
    "STREAM_V2_SOURCE_STATE_ROOT": str(TARGET_STATE_ROOT),
    "STREAM_V2_STATE_ROOT": str(TARGET_STATE_ROOT),
    "RUNTIME_STATE_FILE": str(TARGET_STATE_ROOT / "stream_runtime_state.json"),
    "EVENT_LOG_FILE": str(TARGET_STATE_ROOT / "logs" / "stream_engine_events.jsonl"),
    "RESTART_REASON_FILE": str(TARGET_STATE_ROOT / "restart_reason.json"),
    "WATCHDOG_EVENT_LOG_FILE": str(TARGET_STATE_ROOT / "logs" / "stream_watchdog_events.jsonl"),
    "WATCHDOG_STATS_FILE": str(TARGET_STATE_ROOT / "stream_watchdog_stats.json"),
    "PLAY_HISTORY_JSONL_FILE": str(TARGET_STATE_ROOT / "logs" / "play_history.jsonl"),
    "RUNTIME_STATE_GLOB": str(TARGET_STATE_ROOT / "stream_runtime_state_*.json"),
    "WATCHDOG_SNAPSHOT_TIMELINE_FILE": str(TARGET_STATE_ROOT / "logs" / "watchdog_state_timeline.jsonl"),
    "SLO_FILE": str(TARGET_STATE_ROOT / "slo_snapshot.json"),
}

MONITOR_ENV_OVERRIDES = {
    "STREAM_RUNTIME_STATE_DIR": str(TARGET_STATE_ROOT),
    "STREAM_RUNTIME_LOG_DIR": str(TARGET_STATE_ROOT / "logs"),
    "YTW_STATE_FILE": str(TARGET_STATE_ROOT / "youtube_watchdog_state.json"),
    "YTW_LOG_FILE": str(TARGET_STATE_ROOT / "logs" / "youtube_watchdog.jsonl"),
    "YTW_API_CALL_LOG_FILE": str(TARGET_STATE_ROOT / "logs" / "youtube_api_calls.jsonl"),
    "YTW_API_COST_REPORT_OUTPUT_DIR": str(TARGET_STATE_ROOT / "reports" / "youtube_api_cost"),
    "YTW_API_COST_REPORT_LATEST_FILE": str(TARGET_STATE_ROOT / "reports" / "youtube_api_cost" / "latest.json"),
    "YTW_API_COST_BURN_RATE_LATEST_FILE": str(TARGET_STATE_ROOT / "reports" / "youtube_api_cost" / "open_day_latest.json"),
    "YTW_STATS_FILE": str(TARGET_STATE_ROOT / "youtube_watchdog_stats.json"),
    "YTW_VIDEO_RESOLVER_STATE_FILE": str(TARGET_STATE_ROOT / "youtube_video_id_resolver_state.json"),
    "YTW_OAUTH_TOKEN_STATE_FILE": str(TARGET_STATE_ROOT / "youtube_oauth_token_state.json"),
    "YTW_FORCE_LIVE_STATE_FILE": str(TARGET_STATE_ROOT / "youtube_force_live_state.json"),
    "RESTART_REASON_FILE": str(TARGET_STATE_ROOT / "restart_reason.json"),
}

FAST_RECOVERY_ENV_OVERRIDES = {
    "FR_EVENT_LOG_FILE": str(TARGET_STATE_ROOT / "logs" / "fast_recovery_events.jsonl"),
    "FR_YTW_STATS_FILE": str(TARGET_STATE_ROOT / "youtube_watchdog_stats.json"),
    "FR_QUOTA_STATE_FILE": str(TARGET_STATE_ROOT / "youtube_quota_state.json"),
    "FR_RESTART_REASON_FILE": str(TARGET_STATE_ROOT / "restart_reason.json"),
}

OVERRIDES_BY_SOURCE = {
    "adsb-streamnew": MAIN_ENV_OVERRIDES,
    "adsb-streamnew-youtube-monitor": MONITOR_ENV_OVERRIDES,
    "adsb-streamnew-fast-recovery": FAST_RECOVERY_ENV_OVERRIDES,
    "adsb-streamnew-notify": {},
}

SECRET_MARKERS = ("TOKEN", "SECRET", "KEY", "WEBHOOK", "PASSWORD", "CLIENT_ID", "CLIENT_SECRET")


def _split_env_line(line: str) -> tuple[str, str] | None:
    text = line.strip()
    if not text or text.startswith("#") or "=" not in text:
        return None
    key, value = text.split("=", 1)
    key = key.strip()
    if not key:
        return None
    return key, value.strip().strip("\"").strip("'")


def _rewrite_value(value: str) -> str:
    out = value
    out = out.replace(str(SOURCE_PROJECT_ROOT), str(TARGET_PROJECT_ROOT))
    out = out.replace(str(SOURCE_STATE_ROOT), str(TARGET_STATE_ROOT))
    return out


def read_env(path: Path) -> OrderedDict[str, str]:
    values: OrderedDict[str, str] = OrderedDict()
    for line in path.read_text(encoding="utf-8").splitlines():
        parsed = _split_env_line(line)
        if parsed is None:
            continue
        key, value = parsed
        values[key] = _rewrite_value(value)
    return values


def _quote_value(value: str) -> str:
    if value == "":
        return ""
    if any(ch.isspace() for ch in value) or "#" in value:
        return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'
    return value


def write_env(path: Path, source_path: Path, values: OrderedDict[str, str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Generated by ops/scripts/sync_stream_env_to_v2.py.",
        f"# Source: {source_path}",
        "# This file may contain secrets and is intentionally stored under .state/env.",
        "",
    ]
    for key, value in values.items():
        lines.append(f"{key}={_quote_value(value)}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    os.chmod(path, stat.S_IRUSR | stat.S_IWUSR)


def redacted_summary(values: OrderedDict[str, str]) -> dict[str, object]:
    secret_keys = [key for key in values if any(marker in key.upper() for marker in SECRET_MARKERS)]
    path_keys = [key for key, value in values.items() if str(TARGET_PROJECT_ROOT) in value or str(TARGET_STATE_ROOT) in value]
    return {
        "keys": len(values),
        "secret_keys_redacted": secret_keys,
        "v2_path_keys": path_keys,
    }


def sync_one(source_name: str, source_env_dir: Path, target_env_dir: Path, *, dry_run: bool = False) -> dict[str, object]:
    source_path = source_env_dir / source_name
    target_name = ENV_FILES[source_name]
    target_path = target_env_dir / target_name
    if not source_path.exists():
        return {"source": str(source_path), "target": str(target_path), "status": "missing_source"}
    values = read_env(source_path)
    for key, value in OVERRIDES_BY_SOURCE[source_name].items():
        values[key] = value
    if not dry_run:
        write_env(target_path, source_path, values)
    return {
        "source": str(source_path),
        "target": str(target_path),
        "status": "would_write" if dry_run else "written",
        **redacted_summary(values),
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Sync production stream env files into stream_v2 .state/env snapshots.")
    parser.add_argument("--source-env-dir", type=Path, default=DEFAULT_SOURCE_ENV_DIR)
    parser.add_argument("--target-env-dir", type=Path, default=DEFAULT_TARGET_ENV_DIR)
    parser.add_argument("--dry-run", action="store_true")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    for source_name in ENV_FILES:
        result = sync_one(source_name, args.source_env_dir, args.target_env_dir, dry_run=args.dry_run)
        secret_count = len(result.get("secret_keys_redacted", [])) if isinstance(result.get("secret_keys_redacted"), list) else 0
        path_count = len(result.get("v2_path_keys", [])) if isinstance(result.get("v2_path_keys"), list) else 0
        print(
            f"{source_name}: status={result['status']} target={result['target']} "
            f"keys={result.get('keys', 0)} secret_keys_redacted={secret_count} v2_path_keys={path_count}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
