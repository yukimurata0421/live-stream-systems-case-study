from cra_harness.soak_blocker_cardinality import run_blocker_cardinality_chaos


def test_soak_blocker_cardinality_chaos_bounds_repeated_failures_without_effects() -> None:
    report = run_blocker_cardinality_chaos(reason_count=32, samples_per_reason=128)

    assert report["classification"] == "PASS"
    assert report["injected_blocker_count"] == 4096
    assert report["bounded_blocker_count"] == 32
    assert report["cardinality_reduction_ratio"] == 128
    assert report["semantic_detail_blocker_count"] == 2
    assert report["semantic_detail_identity_preserved"] is True
    assert report["failure_count"] == 0
    assert report["physical_effect_count"] == 0
    assert report["production_target_touched"] is False
