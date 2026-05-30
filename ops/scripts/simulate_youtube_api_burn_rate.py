#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math


SECONDS_PER_DAY = 24 * 60 * 60


def simulate(
    *,
    interval_sec: float,
    units_per_cycle: int,
    threshold_units_per_day: int,
    base_units_per_day: int,
    episode_sec: float,
    episodes_per_day: int = 1,
) -> dict[str, object]:
    if interval_sec <= 0:
        raise ValueError("interval_sec must be > 0")
    if units_per_cycle < 0:
        raise ValueError("units_per_cycle must be >= 0")
    if threshold_units_per_day < 0:
        raise ValueError("threshold_units_per_day must be >= 0")
    if base_units_per_day < 0:
        raise ValueError("base_units_per_day must be >= 0")
    if episode_sec < 0:
        raise ValueError("episode_sec must be >= 0")
    if episodes_per_day < 0:
        raise ValueError("episodes_per_day must be >= 0")

    cycles_per_day = SECONDS_PER_DAY / interval_sec
    fast_units_per_day = int(round(cycles_per_day * units_per_cycle))
    projected_units_per_day = base_units_per_day + fast_units_per_day
    episode_cycles = int(math.ceil(episode_sec / interval_sec)) if episode_sec > 0 and units_per_cycle > 0 else 0
    episode_units = episode_cycles * units_per_cycle
    episode_daily_units = episode_units * episodes_per_day
    actual_daily_units_with_episodes = base_units_per_day + episode_daily_units

    remaining_threshold = threshold_units_per_day - base_units_per_day
    max_safe_cycles_per_day = remaining_threshold / units_per_cycle if units_per_cycle > 0 else math.inf
    min_interval_to_stay_below_threshold_sec = (
        SECONDS_PER_DAY / max_safe_cycles_per_day
        if units_per_cycle > 0 and max_safe_cycles_per_day > 0
        else None
    )

    return {
        "interval_sec": interval_sec,
        "units_per_cycle": units_per_cycle,
        "base_units_per_day": base_units_per_day,
        "fast_units_per_day": fast_units_per_day,
        "projected_units_per_day": projected_units_per_day,
        "threshold_units_per_day": threshold_units_per_day,
        "burn_guard_would_trip": projected_units_per_day >= threshold_units_per_day
        if threshold_units_per_day > 0
        else False,
        "episode_sec": episode_sec,
        "episodes_per_day": episodes_per_day,
        "episode_cycles": episode_cycles,
        "episode_units": episode_units,
        "episode_daily_units": episode_daily_units,
        "actual_daily_units_with_episodes": actual_daily_units_with_episodes,
        "actual_daily_units_would_exceed_threshold": actual_daily_units_with_episodes >= threshold_units_per_day
        if threshold_units_per_day > 0
        else False,
        "min_interval_to_stay_below_threshold_sec": round(min_interval_to_stay_below_threshold_sec, 3)
        if min_interval_to_stay_below_threshold_sec is not None
        else None,
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Simulate YouTube API burn-rate projection for fast-mode remote probes."
    )
    parser.add_argument("--interval-sec", type=float, default=5.0, help="Fast-mode probe interval in seconds.")
    parser.add_argument(
        "--units-per-cycle",
        type=int,
        default=3,
        help="Quota units consumed by one remote-probe cycle. Default is 3 list calls.",
    )
    parser.add_argument(
        "--threshold-units-per-day",
        type=int,
        default=9000,
        help="Burn-rate guard threshold in projected units/day.",
    )
    parser.add_argument(
        "--base-units-per-day",
        type=int,
        default=0,
        help="Observed non-fast-mode baseline to add to the projection.",
    )
    parser.add_argument(
        "--episode-sec",
        type=float,
        default=180.0,
        help="Expected fast-mode episode length for actual-unit estimate.",
    )
    parser.add_argument(
        "--episodes-per-day",
        type=int,
        default=1,
        help="Expected number of fast-mode episodes per PT day for actual-unit estimate.",
    )
    args = parser.parse_args()

    payload = simulate(
        interval_sec=args.interval_sec,
        units_per_cycle=args.units_per_cycle,
        threshold_units_per_day=args.threshold_units_per_day,
        base_units_per_day=args.base_units_per_day,
        episode_sec=args.episode_sec,
        episodes_per_day=args.episodes_per_day,
    )
    print(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
