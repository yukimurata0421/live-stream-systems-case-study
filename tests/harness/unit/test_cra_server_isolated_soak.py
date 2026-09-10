from __future__ import annotations

import json
from pathlib import Path

from cra_harness.runner.sqlite_concurrency import ACTOR_WEIGHTS, _scaled_actor_counts
from tools.run_cra_server_isolated_1h import append_jsonl, network_snapshot, summary_result


def _summary() -> dict[str, object]:
    return {
        "wall_duration_seconds": 3_601.0,
        "required_duration_seconds": 3_600.0,
        "maximum_heartbeat_gap_seconds": 30.1,
        "maximum_heartbeat_gap_allowed_seconds": 90.0,
        "unexpected_command_failure_count": 0,
        "safety_gate_failure_count": 0,
        "production_mutation_count": 0,
        "fixed_sqlite_version": "3.51.3",
        "missing_closeout_count": 0,
    }


def test_summary_requires_every_independent_gate() -> None:
    passing = _summary()
    assert summary_result(passing) == "ISOLATED_1H_ACCEPTED"
    for field, value in {
        "wall_duration_seconds": 3_599.0,
        "maximum_heartbeat_gap_seconds": 90.1,
        "unexpected_command_failure_count": 1,
        "safety_gate_failure_count": 1,
        "production_mutation_count": 1,
        "fixed_sqlite_version": "3.46.1",
        "missing_closeout_count": 1,
    }.items():
        candidate = _summary()
        candidate[field] = value
        assert summary_result(candidate) == "ISOLATED_1H_BLOCKED", field


def test_append_jsonl_is_append_only_and_compact(tmp_path: Path) -> None:
    path = tmp_path / "events.jsonl"
    append_jsonl(path, {"sequence": 1, "summary": "first"})
    append_jsonl(path, {"sequence": 2, "summary": "second"})
    values = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
    assert values == [{"sequence": 1, "summary": "first"}, {"sequence": 2, "summary": "second"}]
    assert path.stat().st_mode & 0o077 == 0


def test_network_snapshot_records_interface_probe_failure(monkeypatch) -> None:
    def unavailable() -> list[tuple[int, str]]:
        raise OSError(97, "Address family not supported")

    class Probe:
        def settimeout(self, value: float) -> None:
            assert value == 0.25

        def connect_ex(self, address: tuple[str, int]) -> int:
            assert address == ("192.0.2.1", 9)
            return 101

        def close(self) -> None:
            return None

    monkeypatch.setattr("socket.if_nameindex", unavailable)
    monkeypatch.setattr("socket.socket", lambda *args: Probe())
    snapshot = network_snapshot()
    assert snapshot == {
        "interfaces": [],
        "interface_probe_errno": 97,
        "only_loopback_visible": False,
        "external_probe_errno": 101,
        "external_route_unavailable": True,
    }


def test_sqlite_actor_scaling_is_positive_and_exact_for_small_and_normal_budgets() -> None:
    for budget in (len(ACTOR_WEIGHTS), 11, 20, 21, 1_000, 20_000):
        counts = _scaled_actor_counts(budget)
        assert set(counts) == set(ACTOR_WEIGHTS)
        assert sum(counts.values()) == budget
        assert min(counts.values()) >= 1
