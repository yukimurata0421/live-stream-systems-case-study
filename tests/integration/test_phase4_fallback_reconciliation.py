from __future__ import annotations

import json
from pathlib import Path

from cra_authority.phase4_shadow import AuthorityReconciliationLoop


class _FakeReconciler:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def reconcile(self, target_id: str) -> int:
        self.calls.append(target_id)
        return len(self.calls) + 6


def test_ready_heartbeat_observing_fallback_reconciles_once_with_backoff(tmp_path: Path) -> None:
    now = [100.0]
    reconciler = _FakeReconciler()
    loop = AuthorityReconciliationLoop(
        reconciler,
        "stream-v3-runtime",
        tmp_path,
        retry_seconds=15.0,
        monotonic=lambda: now[0],
    )

    assert loop.observe({"authority_state": "CENTRAL_ACTIVE"}) is None
    assert loop.observe({"authority_state": "LOCAL_AUTHORITY_ACTIVE"}) == 7
    assert loop.observe({"authority_state": "LOCAL_AUTHORITY_ACTIVE"}) is None
    now[0] += 15.0
    assert loop.observe({"authority_state": "LOCAL_FALLBACK"}) == 8
    assert reconciler.calls == ["stream-v3-runtime", "stream-v3-runtime"]

    events = [json.loads(line) for line in (tmp_path / "agent_state.jsonl").read_text().splitlines()]
    assert [event["authority_epoch"] for event in events] == [7, 8]
    assert [event["trigger"] for event in events] == [
        "READY_AUTHORITY_RECONCILIATION_REQUIRED:LOCAL_AUTHORITY_ACTIVE",
        "READY_AUTHORITY_RECONCILIATION_REQUIRED:LOCAL_FALLBACK",
    ]


def test_agent_startup_reconciling_is_reclaimed_but_safe_blocked_is_not(tmp_path: Path) -> None:
    reconciler = _FakeReconciler()
    loop = AuthorityReconciliationLoop(reconciler, "stream-v3-runtime", tmp_path)

    assert loop.observe({"authority_state": "AGENT_STARTUP_RECONCILING"}) == 7
    assert loop.observe({"authority_state": "SAFE_BLOCKED"}) is None
    assert reconciler.calls == ["stream-v3-runtime"]
