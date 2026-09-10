from cra_harness.delayed_reconciliation import MUTATIONS, run_delayed_reconciliation_campaign


def test_delayed_reconciliation_chaos_detects_every_mutation_without_physical_effect() -> None:
    report = run_delayed_reconciliation_campaign(case_count=21, seed=20260901)

    assert report["pass"] is True
    assert report["failure_count"] == 0
    assert report["physical_effect_adapter_success_count"] == 0
    assert report["production_target_touched"] is False
    assert set(report["negative_control_detected"]) == set(MUTATIONS) - {"VALID"}
