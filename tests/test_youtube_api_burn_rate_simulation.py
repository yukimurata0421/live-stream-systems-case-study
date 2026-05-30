from __future__ import annotations

import sys
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "ops" / "scripts"))

import simulate_youtube_api_burn_rate as sim  # type: ignore


class YouTubeApiBurnRateSimulationTests(unittest.TestCase):
    def test_fast_mode_five_second_three_unit_cycle_trips_9000_guard(self) -> None:
        result = sim.simulate(
            interval_sec=5,
            units_per_cycle=3,
            threshold_units_per_day=9000,
            base_units_per_day=0,
            episode_sec=180,
        )
        self.assertEqual(result["fast_units_per_day"], 51840)
        self.assertTrue(result["burn_guard_would_trip"])
        self.assertEqual(result["episode_cycles"], 36)
        self.assertEqual(result["episode_units"], 108)
        self.assertEqual(result["min_interval_to_stay_below_threshold_sec"], 28.8)

    def test_baseline_is_added_to_projection(self) -> None:
        result = sim.simulate(
            interval_sec=30,
            units_per_cycle=3,
            threshold_units_per_day=9000,
            base_units_per_day=1500,
            episode_sec=60,
        )
        self.assertEqual(result["fast_units_per_day"], 8640)
        self.assertEqual(result["projected_units_per_day"], 10140)
        self.assertTrue(result["burn_guard_would_trip"])

    def test_once_daily_fast_episode_actual_units_stay_small_while_projection_trips(self) -> None:
        result = sim.simulate(
            interval_sec=5,
            units_per_cycle=3,
            threshold_units_per_day=9000,
            base_units_per_day=1500,
            episode_sec=180,
            episodes_per_day=1,
        )
        self.assertEqual(result["episode_cycles"], 36)
        self.assertEqual(result["episode_units"], 108)
        self.assertEqual(result["episode_daily_units"], 108)
        self.assertEqual(result["actual_daily_units_with_episodes"], 1608)
        self.assertFalse(result["actual_daily_units_would_exceed_threshold"])
        self.assertTrue(result["burn_guard_would_trip"])

    def test_daily_fast_episode_count_can_exhaust_real_day_budget(self) -> None:
        result = sim.simulate(
            interval_sec=5,
            units_per_cycle=3,
            threshold_units_per_day=9000,
            base_units_per_day=1500,
            episode_sec=180,
            episodes_per_day=70,
        )
        self.assertEqual(result["episode_daily_units"], 7560)
        self.assertEqual(result["actual_daily_units_with_episodes"], 9060)
        self.assertTrue(result["actual_daily_units_would_exceed_threshold"])


if __name__ == "__main__":
    unittest.main()
