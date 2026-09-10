from __future__ import annotations

from pathlib import Path

from cra_harness.dell_failure_domain_chaos import run_campaign

ROOT = Path(__file__).resolve().parents[3]


def test_dell_failure_domain_chaos_is_bounded_and_fail_closed(tmp_path: Path) -> None:
    summary = run_campaign(
        ROOT,
        tmp_path / "campaign",
        lifecycle_cases=256,
        loopback_cases=48,
        seed=20260901,
    )

    assert summary["classification"] == "PASS"
    assert summary["false_authorization_count"] == 0
    assert summary["deterministic_failure_count"] == 0
    assert summary["safety"] == {
        "production_network_used": False,
        "production_database_used": False,
        "production_effect_socket_used": False,
        "ffmpeg_signal_count": 0,
        "pod_mutation_count": 0,
        "deployment_mutation_count": 0,
        "host_restart_count": 0,
        "physical_effect_count": 0,
    }
