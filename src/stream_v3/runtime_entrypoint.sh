#!/usr/bin/env bash
set -Eeuo pipefail

STATE_DIR="${STREAM_RUNTIME_STATE_DIR:-/state}"
LOG_DIR="${STREAM_RUNTIME_LOG_DIR:-${STATE_DIR}/logs}"
PULSE_SOCKET="${STREAM_V3_PULSE_SOCKET:-/run/stream-pulse/native}"
PULSE_RUNTIME_DIR="${STREAM_V3_PULSE_RUNTIME_DIR:-/run/stream-pulse/runtime}"
PULSE_SINK_NAME="${PULSE_SINK:-stream_v3_sink}"

export PULSE_SERVER="${PULSE_SERVER:-unix:${PULSE_SOCKET}}"
export PULSE_SHM="${PULSE_SHM:-0}"
export XDG_RUNTIME_DIR="${XDG_RUNTIME_DIR:-${PULSE_RUNTIME_DIR}}"

mkdir -p "${LOG_DIR}" "$(dirname "${PULSE_SOCKET}")" "${XDG_RUNTIME_DIR}"

log() {
  printf '[stream-v3-runtime] %s\n' "$*" >&2
}

pulse_ready() {
  pactl info >/dev/null 2>&1
}

ensure_sink() {
  if [[ -z "${PULSE_SINK_NAME}" ]]; then
    return 0
  fi
  if pactl list short sinks 2>/dev/null | awk '{print $2}' | grep -Fxq "${PULSE_SINK_NAME}"; then
    return 0
  fi
  pactl load-module module-null-sink \
    "sink_name=${PULSE_SINK_NAME}" \
    "sink_properties=device.description=${PULSE_SINK_NAME}" >/dev/null
}

start_pulse() {
  if [[ "${STREAM_V3_START_PULSE:-1}" != "1" ]]; then
    return 0
  fi
  if pulse_ready; then
    ensure_sink
    return 0
  fi
  if ! command -v pulseaudio >/dev/null 2>&1; then
    log "pulseaudio binary is missing"
    return 127
  fi

  rm -f "${PULSE_SOCKET}"
  log "starting PulseAudio on ${PULSE_SERVER}"
  pulseaudio \
    --daemonize=yes \
    --disable-shm=yes \
    --enable-memfd=no \
    --exit-idle-time=-1 \
    "--log-target=file:${LOG_DIR}/pulseaudio.log" \
    "--load=module-native-protocol-unix socket=${PULSE_SOCKET} auth-anonymous=1" \
    "--load=module-null-sink sink_name=${PULSE_SINK_NAME} sink_properties=device.description=${PULSE_SINK_NAME}"

  for _ in $(seq 1 40); do
    if pulse_ready; then
      ensure_sink
      log "PulseAudio is ready"
      return 0
    fi
    sleep 0.25
  done
  log "PulseAudio did not become ready"
  return 1
}

start_pulse

if [[ "$#" -gt 0 ]]; then
  exec "$@"
fi
exec /app/src/stream_core/stream.sh
