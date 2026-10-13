from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from cra_authority import projection_pull


def test_resident_continues_after_bounded_cycle_failure_and_recovers(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    status_file = tmp_path / "status.json"
    config = SimpleNamespace(value={"status_file": str(status_file)})
    loads = 0
    cycles = 0

    def load(path: Path) -> Any:
        nonlocal loads
        assert path == tmp_path / "pull.json"
        loads += 1
        return config

    def run_once(current: Any, *, resident: projection_pull.ResidentState | None = None) -> dict[str, Any]:
        nonlocal cycles
        assert current is config
        assert resident is not None
        cycles += 1
        if cycles == 1:
            resident.record_failure("2026-09-28T00:00:00Z")
            status = {
                "status": "SAFE_BLOCKED",
                "error_class": "TimeoutError",
                **resident.status_fields(),
            }
            projection_pull._atomic_json(status_file, status)
            raise TimeoutError("bounded test failure")
        resident.record_success("2026-09-28T00:00:00Z")
        status = {"status": "READY", **resident.status_fields()}
        projection_pull._atomic_json(status_file, status)
        return status

    waits: list[float] = []
    monkeypatch.setattr(projection_pull.ProjectionPullConfig, "load", load)
    monkeypatch.setattr(projection_pull, "_run_once", run_once)

    result = projection_pull.run_resident(
        tmp_path / "pull.json",
        interval_seconds=1.0,
        stop_wait=lambda seconds: waits.append(seconds) or False,
        maximum_cycles=2,
    )

    assert result == 0
    assert loads == 3  # initial pin plus one exact-config check per cycle
    assert cycles == 2
    assert waits == [1.0]
    status = json.loads(status_file.read_text(encoding="utf-8"))
    assert status["status"] == "READY"
    assert status["execution_mode"] == "RESIDENT"
    assert status["cycle_sequence"] == 2
    assert status["connection_generation"] == 2
    assert status["consecutive_failure_count"] == 0


def test_resident_failure_backoff_is_bounded_and_success_resets_it() -> None:
    state = projection_pull.ResidentState(
        started_at="2026-09-28T00:00:00Z",
        base_interval_seconds=1.0,
        next_delay_seconds=1.0,
    )
    observed: list[float] = []
    for index in range(8):
        state.record_failure(f"2026-09-28T00:00:0{index}Z")
        observed.append(state.next_delay_seconds)

    assert observed == [1.0, 2.0, 4.0, 8.0, 16.0, 30.0, 30.0, 30.0]
    assert state.consecutive_failure_count == 8
    state.record_success("2026-09-28T00:00:09Z")
    assert state.consecutive_failure_count == 0
    assert state.next_delay_seconds == 1.0


def test_resident_config_revision_change_safe_blocks_without_adoption(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    status_file = tmp_path / "status.json"
    pinned = SimpleNamespace(value={"puller_release_id": "release-a", "status_file": str(status_file)})
    changed = SimpleNamespace(value={"puller_release_id": "release-a", "status_file": str(status_file), "revision": 2})
    loads = iter((pinned, changed))
    monkeypatch.setattr(projection_pull.ProjectionPullConfig, "load", lambda path: next(loads))
    monkeypatch.setattr(
        projection_pull,
        "_run_once",
        lambda current, *, resident=None: pytest.fail("changed revision must not be executed"),
    )

    result = projection_pull.run_resident(
        tmp_path / "pull.json",
        interval_seconds=1.0,
        stop_wait=lambda seconds: pytest.fail("config drift must terminate without waiting"),
    )

    assert result == 78
    status = json.loads(status_file.read_text(encoding="utf-8"))
    assert status["status"] == "SAFE_BLOCKED"
    assert status["reason_code"] == "PROJECTION_PULL_RESIDENT_CONFIG_INVALID_OR_CHANGED"
    assert status["cycle_sequence"] == 1
    assert status["connection_generation"] == 0
    assert status["control_capability_count"] == 0
    assert status["physical_effect_count"] == 0


@pytest.mark.parametrize("interval", [False, 0.0, 60.1, float("inf")])
def test_resident_interval_is_bounded(interval: Any, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        projection_pull.ProjectionPullConfig,
        "load",
        lambda path: pytest.fail("invalid interval must be rejected before config access"),
    )
    with pytest.raises(ValueError, match="RESIDENT_INTERVAL_INVALID"):
        projection_pull.run_resident(tmp_path / "pull.json", interval_seconds=interval, stop_wait=lambda _: True)
