#!/usr/bin/env bash
set -euo pipefail

# Public-safe legacy service health check from the prodesk monitoring layer.
# Configure DISCORD_WEBHOOK_URL in the systemd unit or environment; never hard-code it.

SERVICE_NAME="${SERVICE_NAME:-env_center.service}"
SERVICE_LABEL="${SERVICE_LABEL:-ENV_CENTER}"
WEBHOOK_URL="${DISCORD_WEBHOOK_URL:-}"
RESTART_ON_FAILURE="${RESTART_ON_FAILURE:-1}"

post_discord() {
  local message="$1"

  if [ -z "$WEBHOOK_URL" ]; then
    printf '%s\n' "$message"
    return 0
  fi

  local json_payload
  json_payload=$(printf '{"content": "%s"}' "$message")

  curl -sS -X POST \
    -H "Content-Type: application/json" \
    -d "$json_payload" \
    "$WEBHOOK_URL" >/dev/null
}

status=$(systemctl is-active "$SERVICE_NAME" 2>/dev/null || echo "unknown")

case "$status" in
  active|activating)
    exit 0
    ;;
  failed|unknown)
    post_discord "[${SERVICE_LABEL}] ${SERVICE_NAME} is ${status}; attempting restart."
    if [ "$RESTART_ON_FAILURE" = "1" ]; then
      systemctl restart "$SERVICE_NAME"
      sleep 3
      new_status=$(systemctl is-active "$SERVICE_NAME" 2>/dev/null || echo "unknown")
      post_discord "[${SERVICE_LABEL}] restart completed; status=${new_status}."
    fi
    ;;
  inactive)
    post_discord "[${SERVICE_LABEL}] ${SERVICE_NAME} is inactive; assuming manual stop."
    ;;
  *)
    post_discord "[${SERVICE_LABEL}] ${SERVICE_NAME} unexpected state: ${status}."
    ;;
esac
