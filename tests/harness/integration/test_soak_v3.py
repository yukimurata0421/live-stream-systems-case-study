from __future__ import annotations

from cra_harness.runner.soak_v3 import run_simulated_soak


def test_accelerated_soak_has_bounded_resources_and_valid_wal(tmp_path) -> None:  # type: ignore[no-untyped-def]
    result = run_simulated_soak(tmp_path / "soak", cycles=2_000, step_seconds=60)
    assert result["result"] == "PASS"
    assert result["sqlite"]["integrity_check"] == "ok"
    assert result["sqlite"]["transaction_failures"] == 0
    assert result["process"]["fd_growth"] == 0
    assert result["physical_attempt_count"] == 0
