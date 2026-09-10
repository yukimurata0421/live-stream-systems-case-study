from __future__ import annotations

import importlib.util
from pathlib import Path

SCRIPT = Path(__file__).parents[1] / "tools" / "p1_live_audit_replay.py"
SPEC = importlib.util.spec_from_file_location("p1_live_audit_replay", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def test_independent_oracle_keeps_unavailable_snapshot_unknown() -> None:
    expected, reason = MODULE.independent_expected(
        {
            "maintenance_observed_state": "UNKNOWN",
            "maintenance_id": "UNKNOWN",
            "maintenance_snapshot_observed_at": "UNKNOWN",
            "maintenance_snapshot_fresh_until": "UNKNOWN",
        }
    )

    assert expected == "UNKNOWN"
    assert reason == "INDEPENDENT_SNAPSHOT_UNAVAILABLE"


def test_independent_oracle_does_not_guess_unobserved_fresh_state() -> None:
    expected, reason = MODULE.independent_expected(
        {
            "maintenance_observed_state": "INACTIVE",
            "maintenance_id": "maintenance-1",
            "maintenance_snapshot_observed_at": "2026-08-23T20:00:00Z",
            "maintenance_snapshot_fresh_until": "2026-08-23T20:01:00Z",
        }
    )

    assert expected is None
    assert reason == "STATE_NOT_OBSERVED_IN_THIS_LIVE_REPLAY"


def test_secret_like_key_scan_checks_nested_values() -> None:
    assert MODULE.secret_like_keys({"target": {"private_key": "redacted"}}) == ["target.private_key"]
