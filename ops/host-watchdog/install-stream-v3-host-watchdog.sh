#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SRC="${ROOT}/ops/host-watchdog/etc"

if [[ "$(id -u)" -ne 0 ]]; then
  exec sudo "$0" "$@"
fi

install -d -m 0755 /etc/modules-load.d
install -d -m 0755 /etc/modprobe.d
install -d -m 0755 /etc/systemd/system.conf.d
install -d -m 0755 /etc/sysctl.d

install -m 0644 "${SRC}/modules-load.d/stream-v3-watchdog.conf" \
  /etc/modules-load.d/stream-v3-watchdog.conf
install -m 0644 "${SRC}/modprobe.d/stream-v3-watchdog.conf" \
  /etc/modprobe.d/stream-v3-watchdog.conf
install -m 0644 "${SRC}/systemd/system.conf.d/stream-v3-watchdog.conf" \
  /etc/systemd/system.conf.d/stream-v3-watchdog.conf
install -m 0644 "${SRC}/sysctl.d/99-stream-v3-watchdog.conf" \
  /etc/sysctl.d/99-stream-v3-watchdog.conf

modprobe iTCO_wdt || true
modprobe softdog
sysctl -p /etc/sysctl.d/99-stream-v3-watchdog.conf
systemctl daemon-reexec

echo "stream_v3 host watchdog installed"
systemctl show -p RuntimeWatchdogUSec -p RebootWatchdogUSec -p KExecWatchdogUSec -p ServiceWatchdogs
if [[ -d /sys/class/watchdog ]]; then
  for wd in /sys/class/watchdog/watchdog*; do
    [[ -e "${wd}" ]] || continue
    name="$(basename "${wd}")"
    identity="$(cat "${wd}/identity" 2>/dev/null || true)"
    timeout="$(cat "${wd}/timeout" 2>/dev/null || true)"
    nowayout="$(cat "${wd}/nowayout" 2>/dev/null || true)"
    echo "${name}: identity=${identity:-unknown} timeout=${timeout:-unknown}s nowayout=${nowayout:-unknown}"
  done
fi
