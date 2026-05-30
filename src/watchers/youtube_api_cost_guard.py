from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

try:
    from .youtube_watchdog_config import (
        API_COST_BURN_RATE_ENABLE,
        API_COST_BURN_RATE_FAIL_CLOSED,
        API_COST_BURN_RATE_LATEST_FILE,
        API_COST_BURN_RATE_CLOCK_SKEW_ALLOW_SEC,
        API_COST_BURN_RATE_MAX_AGE_SEC,
        API_COST_BURN_RATE_MIN_ELAPSED_SEC,
        API_COST_BURN_RATE_THRESHOLD_UNITS_PER_DAY,
    )
except ImportError:
    from youtube_watchdog_config import (
        API_COST_BURN_RATE_ENABLE,
        API_COST_BURN_RATE_FAIL_CLOSED,
        API_COST_BURN_RATE_LATEST_FILE,
        API_COST_BURN_RATE_CLOCK_SKEW_ALLOW_SEC,
        API_COST_BURN_RATE_MAX_AGE_SEC,
        API_COST_BURN_RATE_MIN_ELAPSED_SEC,
        API_COST_BURN_RATE_THRESHOLD_UNITS_PER_DAY,
    )


@dataclass
class ApiCostBurnRateStatus:
    active: bool
    reason: str
    projected_units_per_day: int = 0
    threshold_units_per_day: int = 0
    elapsed_sec: int = 0
    units_so_far: int = 0
    snapshot_age_sec: int = -1


def _parse_ts(text: str) -> int:
    raw = (text or "").strip()
    if not raw:
        return 0
    if raw.endswith("Z"):
        raw = f"{raw[:-1]}+00:00"
    try:
        return int(datetime.fromisoformat(raw).timestamp())
    except Exception:
        return 0


def load_api_cost_burn_rate_status(now_ts: int) -> ApiCostBurnRateStatus:
    if not API_COST_BURN_RATE_ENABLE:
        return ApiCostBurnRateStatus(False, "api cost burn guard disabled")

    def _telemetry_fail_closed(reason: str, threshold: int) -> ApiCostBurnRateStatus:
        if API_COST_BURN_RATE_FAIL_CLOSED:
            return ApiCostBurnRateStatus(
                True,
                f"api cost telemetry degraded; fail-closed active ({reason})",
                threshold_units_per_day=threshold,
            )
        return ApiCostBurnRateStatus(False, reason, threshold_units_per_day=threshold)

    threshold = max(0, int(API_COST_BURN_RATE_THRESHOLD_UNITS_PER_DAY))
    if threshold <= 0:
        return ApiCostBurnRateStatus(False, "api cost burn threshold disabled")

    path = Path(API_COST_BURN_RATE_LATEST_FILE).expanduser()
    if not path.exists():
        return _telemetry_fail_closed(f"api cost snapshot missing ({path})", threshold)

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        return _telemetry_fail_closed(f"api cost snapshot parse error ({e})", threshold)

    if not isinstance(payload, dict):
        return _telemetry_fail_closed("api cost snapshot invalid", threshold)
    if str(payload.get("status", "")).strip().lower() != "ok":
        return _telemetry_fail_closed(f"api cost snapshot not ok ({payload.get('status')})", threshold)

    window = payload.get("window")
    totals = payload.get("totals")
    if not isinstance(window, dict) or not isinstance(totals, dict):
        return _telemetry_fail_closed("api cost snapshot missing fields", threshold)
    ingest = payload.get("ingest")
    if isinstance(ingest, dict) and (not bool(ingest.get("coverage_ok", True))):
        reason = str(ingest.get("coverage_reason", "")).strip() or "coverage not ok"
        return _telemetry_fail_closed(f"api cost telemetry coverage degraded ({reason})", threshold)
    if not bool(window.get("open_day", False)):
        return _telemetry_fail_closed("api cost snapshot is not open-day", threshold)

    start_ts = _parse_ts(str(window.get("start_utc", "")))
    effective_end_ts = _parse_ts(str(window.get("effective_end_utc", "")))
    if start_ts <= 0 or effective_end_ts <= start_ts:
        return _telemetry_fail_closed("api cost snapshot window invalid", threshold)
    if effective_end_ts > (now_ts + max(0, int(API_COST_BURN_RATE_CLOCK_SKEW_ALLOW_SEC))):
        return _telemetry_fail_closed(
            f"api cost snapshot from future ({effective_end_ts}>{now_ts}+{API_COST_BURN_RATE_CLOCK_SKEW_ALLOW_SEC}s)",
            threshold,
        )

    elapsed_sec = max(0, effective_end_ts - start_ts)
    if elapsed_sec < max(1, int(API_COST_BURN_RATE_MIN_ELAPSED_SEC)):
        return _telemetry_fail_closed(f"api cost snapshot insufficient elapsed ({elapsed_sec}s)", threshold)

    units = int(totals.get("units", 0) or 0)
    projected = int(math.ceil((float(units) / float(elapsed_sec)) * 86400.0))
    snapshot_age_sec = max(0, now_ts - effective_end_ts)
    if snapshot_age_sec > max(0, int(API_COST_BURN_RATE_MAX_AGE_SEC)):
        return _telemetry_fail_closed(f"api cost snapshot stale ({snapshot_age_sec}s)", threshold)

    active = projected >= threshold
    reason = (
        f"projected units/day {projected}>={threshold}"
        if active
        else f"projected units/day {projected}<{threshold}"
    )
    return ApiCostBurnRateStatus(
        active=active,
        reason=reason,
        projected_units_per_day=projected,
        threshold_units_per_day=threshold,
        elapsed_sec=elapsed_sec,
        units_so_far=units,
        snapshot_age_sec=snapshot_age_sec,
    )
