from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]


@pytest.mark.parametrize("_round_id", range(8))
def test_collector_cross_process_append_and_gate_stays_linearizable(tmp_path: Path, _round_id: int) -> None:
    sample_path = tmp_path / "samples.jsonl"
    gate_path = tmp_path / "gate-current.json"
    worker = """
import importlib.util
import sys
from pathlib import Path

root = Path(sys.argv[1])
spec = importlib.util.spec_from_file_location('collector_worker', root / 'tools/collect_cra_no_action_soak.py')
assert spec is not None and spec.loader is not None
collector = importlib.util.module_from_spec(spec)
spec.loader.exec_module(collector)
collector._append_sample_and_write_gate(
    Path(sys.argv[2]),
    Path(sys.argv[3]),
    {'worker': int(sys.argv[4])},
    {'arena': 'arena-a', 'cra': 'cra-a', 'dell': 'dell-a'},
)
"""
    processes = [
        subprocess.Popen(
            [sys.executable, "-c", worker, str(ROOT), str(sample_path), str(gate_path), str(index)],
            cwd=ROOT,
            env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        for index in range(24)
    ]
    failures: list[str] = []
    for process in processes:
        stdout, stderr = process.communicate(timeout=15)
        if process.returncode != 0:
            failures.append(f"returncode={process.returncode} stdout={stdout!r} stderr={stderr!r}")

    assert failures == []
    samples = [json.loads(line) for line in sample_path.read_text(encoding="utf-8").splitlines()]
    gate = json.loads(gate_path.read_text(encoding="utf-8"))
    assert len(samples) == 24
    assert {sample["worker"] for sample in samples} == set(range(24))
    assert gate["sample_count"] == 24
    assert os.stat(sample_path.with_name(f".{sample_path.name}.lock")).st_mode & 0o777 == 0o600
