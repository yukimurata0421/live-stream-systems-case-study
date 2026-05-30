#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
PYTHON_BIN="${BASE_DIR}/venv/bin/python3"
if [[ ! -x "$PYTHON_BIN" ]]; then
  PYTHON_BIN="$(command -v python3)"
fi

STATE_DIR="${STREAM_RUNTIME_STATE_DIR:-${HOME}/.local/state/adsb-streamnew}"
LOG_DIR="${STREAM_RUNTIME_LOG_DIR:-${STATE_DIR}/logs}"
OUT_DIR="${BASE_DIR}/runtime/review"
OPEN_DAY_LATEST="${OUT_DIR}/open_day_latest.json"
FULL_TEST_LOG="${OUT_DIR}/unittest_full.log"
TEST_STATE_DIR="${OUT_DIR}/isolated_test_state"
TEST_LOG_DIR="${TEST_STATE_DIR}/logs"
STRICT_HEALTH=0
OPEN_DAY_REPORT_GENERATED=0
if [[ "${1:-}" == "--strict-health" ]]; then
  STRICT_HEALTH=1
fi

mkdir -p "$OUT_DIR"
rm -f "$OPEN_DAY_LATEST"
rm -rf "$TEST_STATE_DIR" "$TEST_LOG_DIR"
mkdir -p "$TEST_STATE_DIR" "$TEST_LOG_DIR"

section() {
  printf '\n== %s ==\n' "$*"
}

section "review environment"
printf 'base_dir=%s\n' "$BASE_DIR"
printf 'python=%s\n' "$PYTHON_BIN"
"$PYTHON_BIN" --version
printf 'state_dir=%s\n' "$STATE_DIR"
printf 'log_dir=%s\n' "$LOG_DIR"
printf 'out_dir=%s\n' "$OUT_DIR"

section "required tools"
for tool in rg jq systemctl journalctl ss curl ffmpeg; do
  if command -v "$tool" >/dev/null 2>&1; then
    printf 'ok %s=%s\n' "$tool" "$(command -v "$tool")"
  else
    printf 'missing %s\n' "$tool"
  fi
done

section "syntax check"
"$PYTHON_BIN" -m py_compile \
  "${BASE_DIR}/src/watchers/youtube_watchdog.py" \
  "${BASE_DIR}/src/watchers/youtube_video_id_resolver.py" \
  "${BASE_DIR}/src/watchers/youtube_api_cost_guard.py" \
  "${BASE_DIR}/src/watchers/youtube_api.py" \
  "${BASE_DIR}/src/watchers/youtube_watchdog_state.py" \
  "${BASE_DIR}/ops/scripts/report_youtube_api_cost.py" \
  "${BASE_DIR}/ops/scripts/observe_stream_health.py"
printf 'ok py_compile\n'

section "unit tests"
env -u STREAM_RUNTIME_LOG_DIR STREAM_RUNTIME_STATE_DIR="$TEST_STATE_DIR" \
"$PYTHON_BIN" -m unittest discover -s "${BASE_DIR}/tests" -p 'test_*.py' -v >"$FULL_TEST_LOG" 2>&1
tail -20 "$FULL_TEST_LOG"
printf 'full_test_log=%s\n' "$FULL_TEST_LOG"
printf 'isolated_test_state_dir=%s\n' "$TEST_STATE_DIR"
printf 'isolated_test_log_dir=%s\n' "$TEST_LOG_DIR"

section "targeted monitoring regression tests"
env -u STREAM_RUNTIME_LOG_DIR STREAM_RUNTIME_STATE_DIR="$TEST_STATE_DIR" \
"$PYTHON_BIN" -m unittest -v \
  tests.test_youtube_watchdog_cache_freshness \
  tests.test_youtube_video_id_resolver_cache_freshness \
  tests.test_youtube_api_cost_guard \
  tests.test_youtube_video_id_resolver_cost_guard \
  tests.test_youtube_watchdog_restart_backoff

section "runtime health summary"
HEALTH_RC=0
STREAM_RUNTIME_STATE_DIR="$STATE_DIR" STREAM_RUNTIME_LOG_DIR="$LOG_DIR" \
  "$PYTHON_BIN" "${BASE_DIR}/ops/scripts/observe_stream_health.py" --hours 24 || HEALTH_RC=$?
printf 'runtime_health_exit=%s\n' "$HEALTH_RC"
if [[ "$STRICT_HEALTH" == "1" && "$HEALTH_RC" -ne 0 ]]; then
  printf 'strict health mode: failing because runtime health summary returned non-zero\n'
  exit "$HEALTH_RC"
fi

section "open-day quota report"
API_CALL_LOG="${YTW_API_CALL_LOG_FILE:-${LOG_DIR}/youtube_api_calls.jsonl}"
if [[ -f "$API_CALL_LOG" ]]; then
  "$PYTHON_BIN" "${BASE_DIR}/ops/scripts/report_youtube_api_cost.py" \
    --include-open-day \
    --lag-sec 0 \
    --allow-near-boundary \
    --allow-just-closed-day \
    --coverage-gap-grace-sec "${YTW_API_COST_REPORT_START_GAP_GRACE_SEC:-300}" \
    --coverage-start-gap-mode "${YTW_API_COST_REPORT_START_GAP_MODE:-strict}" \
    --coverage-end-gap-grace-sec "${YTW_API_COST_REPORT_END_GAP_GRACE_SEC:-900}" \
    --log-file "$API_CALL_LOG" \
    --tz "${YTW_API_COST_REPORT_TZ:-America/Los_Angeles}" \
    --output-latest-file "$OPEN_DAY_LATEST"
  OPEN_DAY_REPORT_GENERATED=1
else
  printf 'skip: api call log not found: %s\n' "$API_CALL_LOG"
fi

section "burn-rate guard evaluation"
if [[ "$OPEN_DAY_REPORT_GENERATED" == "1" && -f "$OPEN_DAY_LATEST" ]]; then
  STREAM_RUNTIME_STATE_DIR="$STATE_DIR" \
  YTW_API_COST_BURN_RATE_LATEST_FILE="$OPEN_DAY_LATEST" \
  "$PYTHON_BIN" - <<'PY'
import importlib
import os
import sys
import time
from pathlib import Path

base = Path("/home/yuki/projects/stream_v2")
sys.path.insert(0, str(base / "src" / "watchers"))
import youtube_watchdog_config  # type: ignore
import youtube_api_cost_guard  # type: ignore

importlib.reload(youtube_watchdog_config)
importlib.reload(youtube_api_cost_guard)
status = youtube_api_cost_guard.load_api_cost_burn_rate_status(int(time.time()))
print(status)
PY
else
  printf 'skip: open-day latest not generated in this run\n'
fi

section "done"
printf 'review artifacts are under %s\n' "$OUT_DIR"
