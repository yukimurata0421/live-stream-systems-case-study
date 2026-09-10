from __future__ import annotations

import json
import threading
from pathlib import Path

from cra_authority.phase4_shadow import _initial_reconcile_with_retry


class TransientReconciler:
    def __init__(self) -> None:
        self.calls = 0

    def reconcile(self, _target_id: str) -> int:
        self.calls += 1
        if self.calls == 1:
            raise ConnectionRefusedError("injected startup race")
        return 31


def test_startup_transport_race_retries_in_process_without_restart(tmp_path: Path) -> None:
    reconciler = TransientReconciler()
    result = _initial_reconcile_with_retry(
        reconciler,
        "stream-target",
        tmp_path,
        threading.Event(),
        retry_seconds=0.001,
    )
    assert result == 31
    assert reconciler.calls == 2
    events = [json.loads(line) for line in (tmp_path / "agent_state.jsonl").read_text(encoding="utf-8").splitlines()]
    assert events == [
        {
            "error": "ConnectionRefusedError",
            "event": "STARTUP_RECONCILIATION_RETRY",
            "observed_at": events[0]["observed_at"],
            "physical_effect_count": 0,
            "production_behavior_modified": False,
        }
    ]
