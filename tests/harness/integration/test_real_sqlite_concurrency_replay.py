from __future__ import annotations

import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
RUN_DIR = ROOT / "artifacts/phase4-shadow/20260823T1604JST_sqlite_concurrency_live_v2"


def test_terminal_real_sqlite_replay_is_trusted_and_safe() -> None:
    replay = json.loads((RUN_DIR / "real_replay.json").read_text(encoding="utf-8"))
    assert replay["profile"] == "REAL_SQLITE_CONCURRENCY_REPLAY"
    assert replay["trusted"] is True
    assert replay["defect_closure"] == "FIXED"
    assert replay["root_cause_classification"] == "LIKELY"
    assert all(replay["checks"].values())
    assert replay["physical_attempt_max"] == 0


def test_terminal_real_sqlite_artifact_hashes_match() -> None:
    hashes = json.loads((RUN_DIR / "artifact_hashes.json").read_text(encoding="utf-8"))
    assert hashes
    for name, expected in hashes.items():
        assert hashlib.sha256((RUN_DIR / name).read_bytes()).hexdigest() == expected
