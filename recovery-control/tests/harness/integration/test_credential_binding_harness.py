from __future__ import annotations

from cra_harness.credential_binding import run_credential_binding_harness


def test_credential_rehearsal_binding_detects_every_removed_identity() -> None:
    report = run_credential_binding_harness()

    assert report["classification"] == "PASS"
    assert report["negative_control_count"] == 7
    assert report["detected_negative_control_count"] == 7
    assert report["secret_value_count"] == 0
