#!/usr/bin/env bash
set -Eeuo pipefail

ENV_FILE="${ENV_FILE:-/etc/default/adsb-streamnew}"
STREAM_SERVICE="${STREAM_SERVICE:-adsb-streamnew-youtube-stream.service}"
DJ_SERVICE="${DJ_SERVICE:-adsb-streamnew-auto-dj.service}"

usage() {
  cat <<'EOF'
Usage:
  pipewire_canary.sh enable
  pipewire_canary.sh disable
  pipewire_canary.sh status

Notes:
  - enable:  set PREFER_PIPEWIRE_PULSE=1 and restart DJ/stream services
  - disable: set PREFER_PIPEWIRE_PULSE=0 and restart DJ/stream services (rollback)
EOF
}

require_root() {
  if [[ "${EUID}" -ne 0 ]]; then
    echo "[error] run as root (sudo)" >&2
    exit 1
  fi
}

set_flag() {
  local value="$1"
  cp -a "$ENV_FILE" "${ENV_FILE}.bak.$(date +%Y%m%d_%H%M%S)"
  if grep -q '^PREFER_PIPEWIRE_PULSE=' "$ENV_FILE"; then
    sed -i "s/^PREFER_PIPEWIRE_PULSE=.*/PREFER_PIPEWIRE_PULSE=${value}/" "$ENV_FILE"
  else
    printf '\nPREFER_PIPEWIRE_PULSE=%s\n' "$value" >> "$ENV_FILE"
  fi
  systemctl daemon-reload
  systemctl restart "$DJ_SERVICE"
  systemctl restart "$STREAM_SERVICE"
}

show_status() {
  echo "ENV: $(grep -E '^PREFER_PIPEWIRE_PULSE=' "$ENV_FILE" || echo 'PREFER_PIPEWIRE_PULSE=<unset>')"
  systemctl is-active "$DJ_SERVICE" "$STREAM_SERVICE"
  sudo -u yuki env XDG_RUNTIME_DIR=/run/user/1000 bash -lc '
    systemctl --user is-active pipewire.service pipewire-pulse.service || true
    if command -v rg >/dev/null 2>&1; then
      pactl info | rg "Server Name|Server String" || true
    else
      pactl info | grep -E "Server Name|Server String" || true
    fi
  '
}

cmd="${1:-}"
case "$cmd" in
  enable)
    require_root
    set_flag "1"
    show_status
    ;;
  disable)
    require_root
    set_flag "0"
    show_status
    ;;
  status)
    show_status
    ;;
  *)
    usage
    exit 1
    ;;
esac
