#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import time
from pathlib import Path


def log(msg: str) -> None:
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] ensure-pulse: {msg}", flush=True)


def run(
    cmd: list[str],
    check: bool = False,
    env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, text=True, capture_output=True, check=check, env=env)


def pulse_ok() -> bool:
    return run(["pactl", "info"]).returncode == 0


def sink_ok(name: str) -> bool:
    if not name:
        return True
    cp = run(["pactl", "list", "short", "sinks"])
    return any(line.split()[1] == name for line in (cp.stdout or "").splitlines() if len(line.split()) >= 2)


def ensure_sink(name: str, dry_run: bool) -> bool:
    if not name:
        return True
    if sink_ok(name):
        log(f"Pulse sink is ready ({name})")
        return True
    if dry_run:
        log(f"[dry-run] would create virtual sink: {name}")
        return True
    log(f"Creating virtual sink: {name}")
    run(
        [
            "pactl",
            "load-module",
            "module-null-sink",
            f"sink_name={name}",
            f"sink_properties=device.description={name}",
        ]
    )
    return sink_ok(name)


def remove_stale_runtime_links(dry_run: bool) -> None:
    pulse_dir = os.path.expanduser("~/.config/pulse")
    if not os.path.isdir(pulse_dir):
        return
    for entry in os.listdir(pulse_dir):
        if not entry.endswith("-runtime"):
            continue
        path = os.path.join(pulse_dir, entry)
        if not os.path.islink(path):
            continue
        target = os.readlink(path)
        if os.path.exists(os.path.join(target, "native")):
            continue
        if dry_run:
            log(f"[dry-run] would remove stale runtime link: {path} -> {target}")
            continue
        try:
            os.unlink(path)
            log(f"removed stale runtime link: {path}")
        except OSError:
            pass


def remove_stale_pulse_pid_file(dry_run: bool) -> None:
    runtime_dir = os.environ.get("XDG_RUNTIME_DIR", "/run/user/1000")
    pid_file = Path(runtime_dir) / "pulse" / "pid"
    if not pid_file.exists():
        return
    try:
        pid = int(pid_file.read_text(encoding="utf-8").strip())
    except Exception:
        if dry_run:
            log(f"[dry-run] would remove invalid pid file: {pid_file}")
            return
        try:
            pid_file.unlink()
        except OSError:
            pass
        return
    if pid <= 1:
        if dry_run:
            log(f"[dry-run] would remove stale pid file: {pid_file}")
            return
        try:
            pid_file.unlink()
        except OSError:
            pass
        return
    try:
        os.kill(pid, 0)
    except OSError:
        if dry_run:
            log(f"[dry-run] would remove dead-process pid file: {pid_file}")
            return
        try:
            pid_file.unlink()
        except OSError:
            pass


def backup_file(path: Path, dry_run: bool) -> Path | None:
    if not path.exists():
        return None
    stamp = time.strftime("%Y%m%d_%H%M%S")
    backup = path.with_name(f"{path.name}.bak.{stamp}")
    if dry_run:
        log(f"[dry-run] would backup {path} -> {backup}")
        return backup
    shutil.copy2(path, backup)
    log(f"backup created: {backup}")
    return backup


def write_with_backup(path: Path, content: str, dry_run: bool) -> None:
    if path.exists():
        existing = path.read_text(encoding="utf-8", errors="ignore")
        if existing == content:
            log(f"unchanged: {path}")
            return
        backup_file(path, dry_run=dry_run)
    if dry_run:
        log(f"[dry-run] would write {path}")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    log(f"wrote: {path}")


def latest_backup(path: Path) -> Path | None:
    def sort_key(candidate: Path) -> tuple[str, float]:
        suffix = candidate.name.rsplit(".bak.", 1)[-1]
        try:
            mtime = candidate.stat().st_mtime
        except OSError:
            mtime = 0.0
        return suffix, mtime

    candidates = sorted(path.parent.glob(f"{path.name}.bak.*"), key=sort_key, reverse=True)
    return candidates[0] if candidates else None


def restore_pulse_configs(dry_run: bool) -> int:
    conf_dir = Path.home() / ".config" / "pulse"
    targets = [conf_dir / "client.conf", conf_dir / "daemon.conf"]
    restored = 0
    for target in targets:
        bak = latest_backup(target)
        if bak is None:
            log(f"no backup found for {target}")
            continue
        if dry_run:
            log(f"[dry-run] would restore {bak} -> {target}")
            restored += 1
            continue
        shutil.copy2(bak, target)
        log(f"restored {target} from {bak}")
        restored += 1
    return 0 if restored > 0 else 1


def write_pulse_client_overrides(dry_run: bool) -> None:
    conf_dir = Path.home() / ".config" / "pulse"
    target = conf_dir / "client.conf"
    content = (
        "# Managed by stream/ops/scripts/ensure_pulse.py\n"
        "enable-shm = no\n"
        "autospawn = yes\n"
        "daemon-binary = /usr/bin/pulseaudio\n"
    )
    write_with_backup(target, content, dry_run=dry_run)


def write_pulse_daemon_overrides(dry_run: bool) -> None:
    conf_dir = Path.home() / ".config" / "pulse"
    target = conf_dir / "daemon.conf"
    content = (
        "# Managed by stream/ops/scripts/ensure_pulse.py\n"
        "enable-shm = no\n"
        "enable-memfd = no\n"
        "avoid-resampling = no\n"
    )
    write_with_backup(target, content, dry_run=dry_run)


def user_systemd_env() -> dict[str, str]:
    env = os.environ.copy()
    runtime_dir = env.get("XDG_RUNTIME_DIR", "/run/user/1000")
    env.setdefault("DBUS_SESSION_BUS_ADDRESS", f"unix:path={runtime_dir}/bus")
    return env


def restart_user_audio_stack(prefer_pipewire: bool, dry_run: bool) -> None:
    if dry_run:
        log("[dry-run] would restart user audio stack")
        return
    sd_env = user_systemd_env()
    if prefer_pipewire:
        run(["systemctl", "--user", "restart", "pipewire.service"], check=False, env=sd_env)
        run(["systemctl", "--user", "restart", "pipewire-pulse.service"], check=False, env=sd_env)
        time.sleep(0.2)
        if pulse_ok():
            return
    run(["pulseaudio", "--kill"])
    remove_stale_pulse_pid_file(dry_run=False)
    run(["systemctl", "--user", "stop", "pulseaudio.service"], check=False, env=sd_env)
    run(["systemctl", "--user", "stop", "pulseaudio.socket"], check=False, env=sd_env)
    time.sleep(0.3)
    run(["systemctl", "--user", "start", "pulseaudio.socket"], check=False, env=sd_env)
    run(["systemctl", "--user", "start", "pulseaudio.service"], check=False, env=sd_env)
    if pulse_ok():
        return
    run(["pulseaudio", "--start"])


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Ensure PulseAudio runtime/sink is healthy for stream services.")
    p.add_argument("--dry-run", action="store_true", help="Show actions without mutating files/services.")
    p.add_argument("--no-write-config", action="store_true", help="Skip writing client.conf/daemon.conf managed overrides.")
    p.add_argument("--restore", action="store_true", help="Restore latest backup for client.conf/daemon.conf and exit.")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    os.environ.setdefault("XDG_RUNTIME_DIR", "/run/user/1000")
    os.environ.pop("PULSE_SERVER", None)
    os.environ.setdefault("PULSE_SHM", "0")

    if args.restore:
        return restore_pulse_configs(dry_run=args.dry_run)

    if not args.no_write_config:
        write_pulse_client_overrides(dry_run=args.dry_run)
        write_pulse_daemon_overrides(dry_run=args.dry_run)

    prefer_pipewire = os.environ.get("PREFER_PIPEWIRE_PULSE", "0").strip() == "1"
    pulse_sink = os.environ.get("PULSE_SINK", "stream_sink").strip()

    if pulse_ok():
        log("Pulse is healthy")
        if ensure_sink(pulse_sink, dry_run=args.dry_run):
            return 0
        log(f"Pulse sink creation failed ({pulse_sink})")
        return 1

    log("Pulse is unhealthy. Starting recovery.")
    remove_stale_runtime_links(dry_run=args.dry_run)
    remove_stale_pulse_pid_file(dry_run=args.dry_run)
    restart_user_audio_stack(prefer_pipewire=prefer_pipewire, dry_run=args.dry_run)

    if args.dry_run:
        log("[dry-run] recovery simulation complete")
        return 0

    for _ in range(20):
        if pulse_ok():
            server = ""
            cp = run(["pactl", "info"])
            for line in (cp.stdout or "").splitlines():
                if line.startswith("Server String: "):
                    server = line.split(": ", 1)[1]
                    break
            log(f"Pulse recovery succeeded (server={server or 'unknown'})")
            if ensure_sink(pulse_sink, dry_run=False):
                return 0
            log(f"Pulse sink creation failed ({pulse_sink})")
            return 1
        time.sleep(0.25)

    log("Pulse recovery failed")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
