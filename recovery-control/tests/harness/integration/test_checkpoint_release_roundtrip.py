"""Real signed prefix -> packaged checkpoint CLI -> deployment evidence gate.

The other gate inputs are small fixtures. This is not a clean production RC
validation or an execution of any production service.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tarfile
from datetime import timedelta
from pathlib import Path

import pytest

from cra_dell_recovery.canonical import Signer
from cra_no_action_soak import recovery_soak
from cra_no_action_soak.recovery_soak import Config
from tests.harness.unit.test_recovery_soak import START, packet
from tests.harness.unit.test_recovery_soak import setup as setup
from tests.release_boundary.test_cra_no_action_release import _actual_source_repository
from tests.release_boundary.test_postmortem_predeploy_gate import _evaluate
from tests.release_boundary.test_postmortem_predeploy_gate import evidence as evidence
from tools.build_cra_no_action_release import build_release


def test_packaged_checkpoint_streams_real_24h_prefix_and_feeds_gate(
    setup: tuple[Config, dict[str, Signer]],
    evidence: dict[str, Path],
    tmp_path: Path,
    request: pytest.FixtureRequest,
) -> None:
    config, signers = setup
    config.value["minimum_duration_seconds"] = 604800
    config_path = tmp_path / "soak-config.json"
    recovery_soak.atomic_write_json(config_path, config.value)
    config = Config.load(config_path)
    # 15-second samples give a real 86,400-second verified duration and a
    # roughly 34 MB prefix, comparable to the reported 37 MB production prefix.
    # Only the clock is simulated; signing, collection, JSONL and replay are real.
    for seconds in range(0, 86431, 15):
        for role in config.bindings:
            recovery_soak.atomic_write_json(Path(config.value["hosts"][role]["inbox_file"]), packet(config, signers, role, seconds))
        recovery_soak.collect(config, now=START + timedelta(seconds=seconds))
    source_bytes = Path(config.value["evidence_file"]).read_bytes()
    state_bytes = Path(config.value["state_file"]).read_bytes()
    source = _actual_source_repository(tmp_path)
    archive_path = tmp_path / "checkpoint-release.tar.gz"
    build_release(source, archive_path, "cra-no-action-checkpoint-roundtrip")
    extracted = tmp_path / "extracted"
    with tarfile.open(archive_path, "r:gz") as archive:
        archive.extractall(extracted, filter="data")
    release = next(extracted.iterdir())
    assert (release / "tools/freeze_recovery_soak_checkpoint.py").is_file()
    env = {k: v for k, v in os.environ.items() if not k.startswith("COV_CORE_") and k != "COVERAGE_PROCESS_START"}
    env["PYTHONPATH"] = os.pathsep.join((str(release / "src"), str(release)))
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    output = tmp_path / "capture/24h"
    measured = tmp_path / "checkpoint-resources.json"
    run = subprocess.run(
        [
            "/usr/bin/time",
            "-f",
            '{"maximum_rss_kib":%M,"elapsed_seconds":%e}',
            "-o",
            str(measured),
            sys.executable,
            "-m",
            "tools.freeze_recovery_soak_checkpoint",
            "--config",
            str(config_path),
            "--output",
            str(output),
            "--identity-file",
            f"checkpoint_tool_manifest={release / 'release_manifest.json'}",
        ],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert run.returncode == 0, run.stderr
    resources = json.loads(measured.read_bytes())
    assert resources["maximum_rss_kib"] < 256 * 1024
    assert len(source_bytes) > 30 * 1024 * 1024
    request.node.user_properties.append(("checkpoint_resources", json.dumps({**resources, "verified_bytes": len(source_bytes)})))
    result = json.loads(run.stdout)
    assert result["classification"] == "24H_OBSERVATION_FROZEN"
    assert result["evaluation"]["verified_duration_seconds"] == 86400
    assert result["source_state"]["sample_count"] == 5763
    assert (output / "samples.committed-prefix.jsonl").read_bytes() == source_bytes
    assert Path(config.value["evidence_file"]).read_bytes() == source_bytes
    assert Path(config.value["state_file"]).read_bytes() == state_bytes
    evidence["checkpoint"] = output
    assert _evaluate(evidence)["status"] == "READY_FOR_EXPLICIT_DEPLOYMENT_REVIEW"
    # The collector's state is untouched when the capture tool is restarted.
    repeated = subprocess.run(
        [
            sys.executable,
            "-m",
            "tools.freeze_recovery_soak_checkpoint",
            "--config",
            str(config_path),
            "--output",
            str(output),
            "--identity-file",
            f"checkpoint_tool_manifest={release / 'release_manifest.json'}",
        ],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert repeated.returncode != 0 and "CHECKPOINT_OUTPUT_EXISTS" in repeated.stderr
    assert Path(config.value["state_file"]).read_bytes() == state_bytes

    for role in config.bindings:
        recovery_soak.atomic_write_json(Path(config.value["hosts"][role]["inbox_file"]), packet(config, signers, role, 86445))
    crash_code = """import os, sys
from datetime import datetime
from pathlib import Path
from cra_no_action_soak import recovery_soak as r
cfg = r.Config.load(Path(sys.argv[1]))
def interrupted_commit(*args, **kwargs):
    os._exit(75)
r.atomic_write_json = interrupted_commit
r.collect(cfg, now=datetime.fromisoformat(sys.argv[2]))
"""
    crashed = subprocess.run(
        [sys.executable, "-c", crash_code, str(config_path), (START + timedelta(seconds=86445)).isoformat()],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert crashed.returncode == 75, crashed.stderr
    assert Path(config.value["state_file"]).read_bytes() == state_bytes
    interrupted_prefix = Path(config.value["evidence_file"]).read_bytes()
    assert interrupted_prefix.startswith(source_bytes)
    assert len(interrupted_prefix.splitlines()) == 5764
    resume_code = """import json, sys
from datetime import datetime
from pathlib import Path
from cra_no_action_soak import recovery_soak as r
cfg = r.Config.load(Path(sys.argv[1]))
row = r.collect(cfg, now=datetime.fromisoformat(sys.argv[2]))
print(json.dumps({"sequence": row["sample_sequence"]}))
"""
    resumed = subprocess.run(
        [sys.executable, "-c", resume_code, str(config_path), (START + timedelta(seconds=86460)).isoformat()],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert resumed.returncode == 0, resumed.stderr
    assert json.loads(resumed.stdout)["sequence"] == 5765
    assert Path(config.value["evidence_file"]).read_bytes().startswith(interrupted_prefix)
    assert json.loads(Path(config.value["state_file"]).read_bytes())["sample_count"] == 5765
    assert (output / "samples.committed-prefix.jsonl").read_bytes() == source_bytes
