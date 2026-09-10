from __future__ import annotations

import json
from pathlib import Path

from cra_authority.replay import replay_2026_08_21


def load_fixture() -> dict[str, object]:
    path = Path(__file__).resolve().parents[1] / "fixtures/2026-08-21_replay.json"
    return dict(json.loads(path.read_text(encoding="utf-8")))


def test_deployment_restart_is_never_an_automatic_protocol_action() -> None:
    decisions = replay_2026_08_21(load_fixture())
    deployment = decisions[0]
    assert deployment.automatic_command_count == 0
    assert deployment.reason == "ACTION_NOT_ALLOWLISTED"


def test_old_target_identity_has_zero_action() -> None:
    decisions = replay_2026_08_21(load_fixture())
    old_target = decisions[1]
    assert old_target.automatic_command_count == 0
    assert old_target.reason == "STALE_TARGET"


def test_new_pod_event_remains_unknown_without_fresh_evidence() -> None:
    decisions = replay_2026_08_21(load_fixture())
    new_pod = decisions[2]
    assert new_pod.automatic_command_count == 0
    assert new_pod.outcome == "UNKNOWN"
    assert new_pod.reason == "MISSING_EVIDENCE_NO_AUTOMATIC_COMMAND"


def test_fixture_explicitly_classifies_missing_evidence() -> None:
    fixture = load_fixture()
    assert fixture["source_classification"] == "MISSING_EVIDENCE"
    assert "not inferred" in str(fixture["source_note"])
