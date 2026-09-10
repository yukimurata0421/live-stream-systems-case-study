from __future__ import annotations

import json
from pathlib import Path

from cra_harness.runner.real_shadow import analyze_real_shadow


def _write(path: Path, values: list[dict[str, object]]) -> None:
    path.write_text("".join(json.dumps(value) + "\n" for value in values), encoding="utf-8")


def test_real_shadow_replay_accepts_complete_no_action_evidence(tmp_path: Path) -> None:
    _write(
        tmp_path / "heartbeat.jsonl",
        [
            {
                "sent": True,
                "issued_at": f"2026-08-23T00:00:0{index}Z",
                "received_authority_state": "CENTRAL_ACTIVE",
                "processing_roundtrip_ms": 2 + index,
                "cycle_duration_ms": 3 + index,
                "authority_epoch": 2,
                "authority_session_id": "session-1",
                "heartbeat_seq": index + 1,
                "physical_attempt_count": 0,
            }
            for index in range(3)
        ],
    )
    target = {
        name: "value" for name in {"host_id", "host_boot_id", "namespace", "pod_uid", "container_name", "container_id", "ffmpeg_generation"}
    }
    target["ffmpeg_pid"] = 10
    _write(tmp_path / "target_snapshots.jsonl", [{"snapshot_id": "snapshot-1", "status": "VALID", "target_identity": target}])
    _write(tmp_path / "agent_state.jsonl", [{"event": "RECONCILED", "authority_epoch": 2, "physical_attempt_count": 0}])
    _write(tmp_path / "commands.jsonl", [])
    _write(tmp_path / "receipts.jsonl", [])
    _write(tmp_path / "verification.jsonl", [])
    result = analyze_real_shadow(tmp_path)
    assert result["classification"] == "PASS"
    assert result["harness_real_replay_trusted"] is True


def test_real_shadow_replay_detects_physical_attempt(tmp_path: Path) -> None:
    test_real_shadow_replay_accepts_complete_no_action_evidence(tmp_path)
    values = [json.loads(line) for line in (tmp_path / "heartbeat.jsonl").read_text(encoding="utf-8").splitlines()]
    values[0]["physical_attempt_count"] = 1
    _write(tmp_path / "heartbeat.jsonl", values)
    result = analyze_real_shadow(tmp_path)
    assert result["classification"] == "SAFETY_GATE_FAILURE"
