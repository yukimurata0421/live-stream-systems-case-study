#!/usr/bin/env python3
"""Observe NVIDIA/NVENC package state and notify only on relevant changes."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo


SCHEMA_VERSION = 1
SUPPORTED_BRANCH = "580"
TRACKED_PACKAGES = (
    "nvidia-driver-580",
    "nvidia-dkms-580",
    "nvidia-utils-580",
    "libnvidia-encode-580",
)
FUTURE_BRANCH_PACKAGES = ("nvidia-driver-590", "nvidia-driver-595")
MAINTENANCE_GUARD = Path("/etc/apt/apt.conf.d/51stream-v3-nvidia-maintenance")
DEFAULT_STATE_FILE = Path("/var/lib/stream-v3-nvidia-driver-check/state.json")
JST = ZoneInfo("Asia/Tokyo")


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def run(command: list[str], *, timeout: float = 20.0) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=timeout,
        check=False,
    )


def command_output(command: list[str], *, timeout: float = 20.0) -> tuple[str, str | None]:
    try:
        completed = run(command, timeout=timeout)
    except (OSError, subprocess.TimeoutExpired) as exc:
        return "", f"{command[0]}: {exc}"
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout or f"exit {completed.returncode}").strip()
        return "", f"{command[0]}: {detail[:240]}"
    return completed.stdout.strip(), None


def installed_version(package: str) -> str | None:
    output, error = command_output(
        ["dpkg-query", "-W", "-f=${db:Status-Abbrev}\t${Version}", package]
    )
    if error or not output:
        return None
    status, _, version = output.partition("\t")
    return version.strip() if status.startswith("ii") and version.strip() else None


def candidate_version(package: str) -> str | None:
    output, error = command_output(["apt-cache", "policy", package])
    if error:
        return None
    match = re.search(r"^\s*Candidate:\s*(\S+)", output, flags=re.MULTILINE)
    if not match or match.group(1) == "(none)":
        return None
    return match.group(1)


def version_is_newer(candidate: str | None, installed: str | None) -> bool:
    if not candidate:
        return False
    if not installed:
        return True
    try:
        return run(["dpkg", "--compare-versions", candidate, "gt", installed], timeout=5).returncode == 0
    except (OSError, subprocess.TimeoutExpired):
        return candidate != installed


def recommended_driver() -> tuple[str | None, str | None]:
    output, error = command_output(["ubuntu-drivers", "devices"], timeout=30)
    if error:
        return None, error
    for line in output.splitlines():
        if "recommended" not in line:
            continue
        match = re.search(r"\b(nvidia-driver-\d+(?:-server)?)\b", line)
        if match:
            return match.group(1), None
    return None, "ubuntu-drivers: recommended NVIDIA driver was not found"


def runtime_driver() -> tuple[dict[str, Any], list[str]]:
    issues: list[str] = []
    output, error = command_output(
        ["nvidia-smi", "--query-gpu=name,driver_version", "--format=csv,noheader"],
        timeout=15,
    )
    gpus: list[dict[str, str]] = []
    if error:
        issues.append(error)
    else:
        for line in output.splitlines():
            name, separator, version = line.rpartition(",")
            if separator:
                gpus.append({"name": name.strip(), "driver_version": version.strip()})

    module_output, module_error = command_output(["modinfo", "-F", "version", "nvidia"])
    module_version = module_output.splitlines()[0].strip() if module_output else None
    if module_error:
        issues.append(module_error)

    runtime_versions = sorted({gpu["driver_version"] for gpu in gpus if gpu["driver_version"]})
    mismatch = bool(module_version and runtime_versions and runtime_versions != [module_version])
    return {
        "gpus": gpus,
        "module_version": module_version,
        "runtime_module_mismatch": mismatch,
    }, issues


def current_kernel_dkms() -> tuple[dict[str, Any], str | None]:
    kernel_output, kernel_error = command_output(["uname", "-r"])
    kernel = kernel_output.strip()
    if kernel_error:
        return {"kernel": None, "nvidia_installed": False}, kernel_error
    output, error = command_output(["dkms", "status"])
    if error:
        return {"kernel": kernel, "nvidia_installed": False}, error
    installed = any(
        line.lower().startswith("nvidia/")
        and f", {kernel}," in line
        and line.rstrip().endswith(": installed")
        for line in output.splitlines()
    )
    return {"kernel": kernel, "nvidia_installed": installed}, None


def apt_metadata() -> dict[str, Any]:
    try:
        mtimes = [
            path.stat().st_mtime
            for path in Path("/var/lib/apt/lists").iterdir()
            if path.is_file()
        ]
    except OSError:
        mtimes = []
    if not mtimes:
        return {"newest_utc": None, "age_hours": None, "stale": True}
    newest = max(mtimes)
    age_hours = max(0.0, (time.time() - newest) / 3600)
    return {
        "newest_utc": datetime.fromtimestamp(newest, tz=timezone.utc)
        .isoformat()
        .replace("+00:00", "Z"),
        "age_hours": round(age_hours, 1),
        "stale": age_hours > 48,
    }


def maintenance_guard() -> dict[str, Any]:
    try:
        text = MAINTENANCE_GUARD.read_text(encoding="utf-8")
    except OSError:
        return {"path": str(MAINTENANCE_GUARD), "active": False}
    required = ("nvidia-", "libnvidia-", "xserver-xorg-video-nvidia-")
    return {
        "path": str(MAINTENANCE_GUARD),
        "active": all(pattern in text for pattern in required),
    }


def held_packages() -> list[str]:
    output, _ = command_output(["apt-mark", "showhold"])
    return sorted(
        line.strip()
        for line in output.splitlines()
        if "nvidia" in line.lower() and line.strip()
    )


def collect_observation() -> dict[str, Any]:
    packages: dict[str, dict[str, Any]] = {}
    for package in (*TRACKED_PACKAGES, *FUTURE_BRANCH_PACKAGES):
        installed = installed_version(package)
        candidate = candidate_version(package)
        packages[package] = {
            "installed": installed,
            "candidate": candidate,
            "update_available": version_is_newer(candidate, installed)
            if package in TRACKED_PACKAGES
            else False,
        }

    recommended, recommended_error = recommended_driver()
    runtime, runtime_issues = runtime_driver()
    dkms, dkms_error = current_kernel_dkms()
    apt = apt_metadata()
    guard = maintenance_guard()

    collection_issues = list(runtime_issues)
    if recommended_error:
        collection_issues.append(recommended_error)
    if dkms_error:
        collection_issues.append(dkms_error)

    reasons: list[str] = []
    if any(packages[package]["update_available"] for package in TRACKED_PACKAGES):
        reasons.append("r580_update_available")
    if recommended and recommended != f"nvidia-driver-{SUPPORTED_BRANCH}":
        reasons.append("recommended_branch_changed")
    if runtime["runtime_module_mismatch"]:
        reasons.append("runtime_driver_mismatch")
    if not dkms["nvidia_installed"]:
        reasons.append("current_kernel_dkms_missing")
    if apt["stale"]:
        reasons.append("apt_metadata_stale")
    if not guard["active"]:
        reasons.append("maintenance_guard_missing")
    if collection_issues:
        reasons.append("collection_error")

    return {
        "checked_at_utc": utc_now(),
        "supported_branch": SUPPORTED_BRANCH,
        "packages": packages,
        "recommended_driver": recommended,
        "runtime": runtime,
        "dkms": dkms,
        "apt_metadata": apt,
        "maintenance_guard": guard,
        "held_packages": held_packages(),
        "collection_issues": collection_issues,
        "reasons": reasons,
    }


def fingerprint(observation: dict[str, Any]) -> str:
    packages = observation.get("packages", {})
    relevant = {
        "packages": packages,
        "recommended_driver": observation.get("recommended_driver"),
        "runtime": observation.get("runtime"),
        "dkms": observation.get("dkms"),
        "apt_metadata_stale": observation.get("apt_metadata", {}).get("stale"),
        "maintenance_guard_active": observation.get("maintenance_guard", {}).get("active"),
        "collection_issues": observation.get("collection_issues", []),
        "reasons": observation.get("reasons", []),
    }
    encoded = json.dumps(relevant, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def decide_notification(
    previous: dict[str, Any] | None,
    current_fingerprint: str,
    observation: dict[str, Any],
    *,
    notify_baseline: bool = False,
) -> str | None:
    if not previous:
        return "alert" if notify_baseline and observation.get("reasons") else None

    pending = previous.get("pending_notification")
    if (
        isinstance(pending, dict)
        and pending.get("fingerprint") == current_fingerprint
        and pending.get("kind") in {"alert", "resolved"}
    ):
        return str(pending["kind"])

    if previous.get("fingerprint") == current_fingerprint:
        return None
    previous_reasons = previous.get("observation", {}).get("reasons", [])
    if observation.get("reasons"):
        return "alert"
    if previous_reasons:
        return "resolved"
    return None


def package_lines(observation: dict[str, Any]) -> list[str]:
    lines: list[str] = []
    packages = observation.get("packages", {})
    for package in TRACKED_PACKAGES:
        item = packages.get(package, {})
        installed = item.get("installed") or "未導入"
        candidate = item.get("candidate") or "候補なし"
        marker = " ← 更新候補" if item.get("update_available") else ""
        lines.append(f"- {package}: {installed} → {candidate}{marker}")
    future = [
        f"{package}={packages.get(package, {}).get('candidate')}"
        for package in FUTURE_BRANCH_PACKAGES
        if packages.get(package, {}).get("candidate")
    ]
    if future:
        lines.append("- 非対象branch候補（適用禁止）: " + ", ".join(future))
    return lines


def render_notification(kind: str, observation: dict[str, Any]) -> str:
    checked = datetime.fromisoformat(
        str(observation["checked_at_utc"]).replace("Z", "+00:00")
    ).astimezone(JST)
    checked_jst = checked.strftime("%Y-%m-%d %H:%M JST")
    runtime = observation.get("runtime", {})
    gpu_names = ", ".join(
        str(gpu.get("name") or "") for gpu in runtime.get("gpus", []) if gpu.get("name")
    ) or "取得不可"
    guard = "有効" if observation.get("maintenance_guard", {}).get("active") else "無効"
    recommended = observation.get("recommended_driver") or "取得不可"

    if kind == "resolved":
        heading = "【NVIDIA/NVENC更新監視】前回の要対応状態が解消"
    else:
        heading = "【NVIDIA/NVENC更新監視】確認が必要な変化を検知"

    lines = [
        heading,
        f"確認時刻: {checked_jst}",
        f"GPU: {gpu_names}",
        f"Ubuntu推奨: {recommended} / 運用branch: nvidia-driver-{SUPPORTED_BRANCH}",
        f"自動更新除外: {guard}（自動適用は行っていません）",
        *package_lines(observation),
    ]
    reasons = observation.get("reasons", [])
    if reasons:
        lines.append("判定: " + ", ".join(str(reason) for reason in reasons))
        lines.append("対応: 直ちに切り替えず、候補確認後に保留期間と48時間smoke testを設定してください。")
    else:
        lines.append("判定: 要対応項目なし")
    issues = observation.get("collection_issues", [])
    if issues:
        lines.append("取得エラー: " + " / ".join(str(issue) for issue in issues)[:500])
    return "\n".join(lines)


def send_discord(webhook_url: str, content: str, username: str) -> tuple[bool, str]:
    payload = json.dumps(
        {"content": content[:1900], "username": username[:80]},
        ensure_ascii=False,
    ).encode("utf-8")
    request = urllib.request.Request(
        webhook_url,
        data=payload,
        headers={
            "Content-Type": "application/json",
            "User-Agent": "stream-v3-nvidia-update-check/1.0",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            status = int(getattr(response, "status", response.getcode()))
            body = response.read(256).decode("utf-8", errors="replace")
    except (OSError, urllib.error.URLError) as exc:
        return False, str(exc)
    if 200 <= status < 300:
        return True, f"http_{status}"
    return False, f"http_{status}:{body}"


def load_state(path: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def write_state(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.chmod(temporary, 0o600)
    temporary.replace(path)


def env_truthy(name: str, fallback: str | None = None, *, default: bool = True) -> bool:
    value = os.environ.get(name)
    if value is None and fallback:
        value = os.environ.get(fallback)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def execute(args: argparse.Namespace) -> int:
    observation = collect_observation()
    current_fingerprint = fingerprint(observation)
    previous = load_state(args.state_file)
    kind = decide_notification(
        previous,
        current_fingerprint,
        observation,
        notify_baseline=args.notify_baseline,
    )
    result: dict[str, Any] = {
        "status": "ok",
        "event": "baseline_recorded" if previous is None else "unchanged",
        "fingerprint": current_fingerprint,
        "observation": observation,
        "notification": {"required": bool(kind), "kind": kind, "sent": False},
    }

    if previous and previous.get("fingerprint") != current_fingerprint:
        result["event"] = "changed"
    if kind:
        enabled = env_truthy("NVIDIA_UPDATE_NOTIFY_ENABLED", "STREAM_NOTIFY_ENABLED")
        webhook = (
            os.environ.get("NVIDIA_UPDATE_DISCORD_WEBHOOK_URL")
            or os.environ.get("STREAM_NOTIFY_DISCORD_WEBHOOK_URL")
            or ""
        ).strip()
        username = (
            os.environ.get("NVIDIA_UPDATE_NOTIFY_USERNAME")
            or os.environ.get("STREAM_NOTIFY_USERNAME")
            or "stream_v3 NVIDIA Update Watch"
        ).strip()
        if args.dry_run:
            result["notification"]["detail"] = "dry_run"
        elif not enabled:
            result["status"] = "notification_pending"
            result["notification"]["detail"] = "notification_disabled"
        elif not webhook:
            result["status"] = "notification_pending"
            result["notification"]["detail"] = "webhook_not_configured"
        else:
            sent, detail = send_discord(webhook, render_notification(kind, observation), username)
            result["notification"].update({"sent": sent, "detail": detail})
            if not sent:
                result["status"] = "notification_pending"

    state: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "updated_at_utc": utc_now(),
        "fingerprint": current_fingerprint,
        "observation": observation,
    }
    if kind and not result["notification"]["sent"]:
        state["pending_notification"] = {
            "fingerprint": current_fingerprint,
            "kind": kind,
            "since_utc": (
                previous.get("pending_notification", {}).get("since_utc")
                if previous
                and previous.get("pending_notification", {}).get("fingerprint")
                == current_fingerprint
                else utc_now()
            ),
        }
    elif kind:
        state["last_notification"] = {
            "fingerprint": current_fingerprint,
            "kind": kind,
            "sent_at_utc": utc_now(),
        }
    elif previous and isinstance(previous.get("last_notification"), dict):
        state["last_notification"] = previous["last_notification"]

    if not args.dry_run:
        write_state(args.state_file, state)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 2 if result["status"] == "notification_pending" else 0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--state-file", type=Path, default=DEFAULT_STATE_FILE)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--notify-baseline", action="store_true")
    return parser.parse_args(argv)


def main() -> int:
    return execute(parse_args())


if __name__ == "__main__":
    sys.exit(main())
