"""Authority handover against the real Executor ledger, driven from a separate process."""

from __future__ import annotations

import json
import sqlite3
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path

import pytest

from cra import effect
from tests.reflex.test_reflex_executor_fence_reconciliation import LEGACY_SRC, Runtime

TOOL = Path(__file__).resolve().parents[2] / "tools/executor_authority.py"


@pytest.fixture
def runtime(tmp_path: Path):
    value = Runtime(tmp_path, initial_producer=("independent-controller", 2))
    yield value
    value.close()


def tool(runtime: Runtime, *args: str) -> tuple[int, dict]:
    result = subprocess.run(
        [sys.executable, str(TOOL), "--ledger", str(runtime.root / "ledger.sqlite3"), "--runtime-src", str(LEGACY_SRC), *args],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    return result.returncode, json.loads(result.stdout)


def snapshot(runtime: Runtime) -> dict:
    connection = sqlite3.connect(f"file:{runtime.root / 'ledger.sqlite3'}?mode=ro", uri=True)
    try:
        tables = [r[0] for r in connection.execute("SELECT name FROM sqlite_master WHERE type='table' AND name != 'runtime_authority'")]
        return {t: connection.execute(f"SELECT * FROM {t} ORDER BY 1").fetchall() for t in tables}
    finally:
        connection.close()


def request_as(runtime: Runtime, producer: tuple[str, int], sequence: int) -> str:
    obs = runtime.observation(sequence)
    request = effect.build_restart_request(obs, now=datetime.now(UTC), producer_id=producer[0], producer_generation=producer[1], reason="t")
    return effect.classify(effect.send(runtime.socket, request), expected_request_id=request["request_id"])


def close_fence(runtime: Runtime, successor: tuple[str, int, str], sequence: int) -> None:
    scope = effect.query_unresolved(runtime.socket)[0]
    runtime.set_child(*successor)
    response = effect.send(runtime.socket, effect.build_reconciliation(scope, runtime.observation(sequence), now=datetime.now(UTC)))
    assert response.get("state") == "RECONCILED_EFFECT_OBSERVED", response


def test_handover_and_rollback(runtime: Runtime) -> None:
    before = snapshot(runtime)
    code, shown = tool(runtime, "show")
    assert code == 0 and (shown["producer_id"], shown["producer_generation"], shown["unresolved_count"]) == (
        "independent-controller",
        2,
        0,
    )
    assert snapshot(runtime) == before

    assert request_as(runtime, ("cra-agent", 3), 10) == "REJECTED"  # not the authority yet
    assert request_as(runtime, ("independent-controller", 2), 11) == "EFFECT_OBSERVED"  # old controller acts

    code, refused = tool(runtime, "switch", "--expect", "independent-controller:2", "--to", "cra-agent:3")
    assert code == 1 and refused["reason"] == "UNRESOLVED_EXECUTION_EXISTS"

    close_fence(runtime, ("protocol-2", 5151, "native-2"), 12)
    code, mismatch = tool(runtime, "switch", "--expect", "independent-controller:1", "--to", "cra-agent:3")
    assert code == 1 and mismatch["reason"] == "EXPECTED_AUTHORITY_MISMATCH"

    unchanged = snapshot(runtime)
    code, switched = tool(runtime, "switch", "--expect", "independent-controller:2", "--to", "cra-agent:3")
    assert code == 0 and switched["reason"] == "AUTHORITY_SWITCHED"
    assert snapshot(runtime) == unchanged  # only the authority row changed: no backfill or migration writes

    assert request_as(runtime, ("independent-controller", 2), 13) == "REJECTED"
    assert request_as(runtime, ("cra-agent", 3), 14) == "EFFECT_OBSERVED"

    close_fence(runtime, ("protocol-3", 6161, "native-3"), 15)
    code, back = tool(runtime, "switch", "--expect", "cra-agent:3", "--to", "independent-controller:2")
    assert code == 0 and back["producer_id"] == "independent-controller"
    assert request_as(runtime, ("cra-agent", 3), 16) == "REJECTED"
    assert request_as(runtime, ("independent-controller", 2), 17) == "EFFECT_OBSERVED"
