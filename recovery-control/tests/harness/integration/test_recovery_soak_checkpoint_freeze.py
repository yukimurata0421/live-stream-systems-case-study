from __future__ import annotations

import io
import json
from collections import namedtuple
from datetime import timedelta
from pathlib import Path
from unittest.mock import patch

import pytest

from cra_dell_recovery.canonical import Signer
from cra_no_action_soak import recovery_soak
from cra_no_action_soak.recovery_soak import Config
from tests.harness.unit.test_recovery_soak import START, packet
from tests.harness.unit.test_recovery_soak import setup as setup
from tools import freeze_recovery_soak_checkpoint as checkpoint_freeze
from tools.freeze_recovery_soak_checkpoint import freeze_checkpoint


def _build_prefix(config: Config, signers: dict[str, Signer], *, final_seconds: int) -> tuple[Path, Config]:
    config.value["minimum_duration_seconds"] = 604800
    config_path = Path(config.value["state_file"]).parent / "config.json"
    recovery_soak.atomic_write_json(config_path, config.value)
    config = Config.load(config_path)
    for seconds in range(0, final_seconds + 1, 15):
        for role in config.bindings:
            recovery_soak.atomic_write_json(
                Path(config.value["hosts"][role]["inbox_file"]),
                packet(config, signers, role, seconds),
            )
        recovery_soak.collect(config, now=START + timedelta(seconds=seconds))
    return config_path, config


def test_freeze_copies_a_verified_prefix_without_mutating_source(
    setup: tuple[Config, dict[str, Signer]],
    tmp_path: Path,
) -> None:
    config, signers = setup
    config_path, config = _build_prefix(config, signers, final_seconds=105)
    identity = tmp_path / "identities/runtime-manifest.json"
    identity.parent.mkdir()
    identity.write_text('{"release_id":"test-only"}\n', encoding="utf-8")
    before = {name: Path(config.value[name]).read_bytes() for name in ("evidence_file", "state_file")}
    output = tmp_path / "artifacts/checkpoint"
    result = freeze_checkpoint(
        config_path,
        output,
        {"runtime_manifest": identity},
        captured_at=START + timedelta(seconds=106),
        minimum_observation_seconds=60,
    )
    assert result["classification"] == "24H_OBSERVATION_FROZEN"
    assert result["formal_soak_status"] == "NOT_YET_ELIGIBLE"
    assert result["formal_soak_eligible"] is False
    assert result["candidate_soak_reusable_seconds"] == 0
    assert result["checkpoint_boundary"]["overshoot_seconds"] == 0
    assert result["checkpoint_boundary"]["observed_at"] == (START + timedelta(seconds=90)).isoformat(timespec="milliseconds").replace(
        "+00:00", "Z"
    )
    assert Path(config.value["evidence_file"]).read_bytes() == before["evidence_file"]
    assert Path(config.value["state_file"]).read_bytes() == before["state_file"]
    assert (output / "samples.committed-prefix.jsonl").read_bytes() == before["evidence_file"]
    assert (output / "identity-runtime_manifest.bin").read_bytes() == identity.read_bytes()
    assert {"public_key_dell", "public_key_arena", "public_key_cra"} <= set(result["identity_files"])
    assert all((output / item["artifact"]).read_bytes() for item in result["identity_files"].values())
    saved = json.loads((output / "checkpoint.json").read_text(encoding="utf-8"))
    assert saved == result


def test_freeze_rejects_a_prefix_before_the_requested_duration(
    setup: tuple[Config, dict[str, Signer]],
    tmp_path: Path,
) -> None:
    config, signers = setup
    config_path, config = _build_prefix(config, signers, final_seconds=90)
    identity = tmp_path / "identities/manifest.json"
    identity.parent.mkdir()
    identity.write_text("{}\n", encoding="utf-8")
    output = tmp_path / "artifacts/too-early"
    with pytest.raises(ValueError, match="24H_NOT_YET_ELIGIBLE"):
        freeze_checkpoint(config_path, output, {"runtime": identity}, minimum_observation_seconds=60.001)
    assert not output.exists()


def test_freeze_never_overwrites_an_existing_artifact(
    setup: tuple[Config, dict[str, Signer]],
    tmp_path: Path,
) -> None:
    config, signers = setup
    config_path, _ = _build_prefix(config, signers, final_seconds=105)
    output = tmp_path / "existing"
    output.mkdir()
    with pytest.raises(FileExistsError, match="OUTPUT_EXISTS"):
        freeze_checkpoint(config_path, output, {"identity": config_path}, minimum_observation_seconds=60)


def test_freeze_rejects_source_output_overlap(setup: tuple[Config, dict[str, Signer]]) -> None:
    config, signers = setup
    config_path, config = _build_prefix(config, signers, final_seconds=105)
    output = Path(config.value["state_file"]).parent / "nested-output"
    with pytest.raises(ValueError, match="SOURCE_COLLISION"):
        freeze_checkpoint(config_path, output, {"identity": config_path}, minimum_observation_seconds=60)


def test_freeze_accepts_a_later_append_while_preserving_the_pinned_prefix(
    setup: tuple[Config, dict[str, Signer]],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config, signers = setup
    config_path, config = _build_prefix(config, signers, final_seconds=105)
    identity = tmp_path / "identities/manifest.json"
    identity.parent.mkdir()
    identity.write_text("{}\n", encoding="utf-8")
    original = checkpoint_freeze._copy_prefix
    captured_prefix = Path(config.value["evidence_file"]).read_bytes()

    def copy_then_advance(source: Path, destination: Path, length: int, maximum_bytes: int) -> str:
        result = original(source, destination, length, maximum_bytes)
        for role in config.bindings:
            recovery_soak.atomic_write_json(
                Path(config.value["hosts"][role]["inbox_file"]),
                packet(config, signers, role, 120),
            )
        recovery_soak.collect(config, now=START + timedelta(seconds=120))
        return result

    monkeypatch.setattr("tools.freeze_recovery_soak_checkpoint._copy_prefix", copy_then_advance)
    output = tmp_path / "artifacts/checkpoint"
    result = freeze_checkpoint(
        config_path,
        output,
        {"runtime": identity},
        minimum_observation_seconds=60,
    )
    assert result["source_state"]["sample_count"] == 8
    assert json.loads(Path(config.value["state_file"]).read_text())["sample_count"] == 9
    assert (output / "samples.committed-prefix.jsonl").read_bytes() == captured_prefix


def test_freeze_rejects_same_sequence_state_rewrite_during_capture(
    setup: tuple[Config, dict[str, Signer]],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config, signers = setup
    config_path, config = _build_prefix(config, signers, final_seconds=105)
    identity = tmp_path / "identities/manifest.json"
    identity.parent.mkdir()
    identity.write_text("{}\n", encoding="utf-8")
    original = checkpoint_freeze._copy_prefix

    def copy_then_rewrite(source: Path, destination: Path, length: int, maximum_bytes: int) -> str:
        result = original(source, destination, length, maximum_bytes)
        state_path = Path(config.value["state_file"])
        state = json.loads(state_path.read_text())
        state["last_sample_hash"] = "0" * 64
        recovery_soak.atomic_write_json(state_path, state)
        return result

    monkeypatch.setattr("tools.freeze_recovery_soak_checkpoint._copy_prefix", copy_then_rewrite)
    with pytest.raises(ValueError, match="STATE_REGRESSED"):
        freeze_checkpoint(
            config_path,
            tmp_path / "artifacts/checkpoint",
            {"runtime": identity},
            minimum_observation_seconds=60,
        )


def test_freeze_rejects_capture_time_before_committed_prefix(
    setup: tuple[Config, dict[str, Signer]],
    tmp_path: Path,
) -> None:
    config, signers = setup
    config_path, _ = _build_prefix(config, signers, final_seconds=105)
    identity = tmp_path / "identities/manifest.json"
    identity.parent.mkdir()
    identity.write_text("{}\n", encoding="utf-8")
    with pytest.raises(ValueError, match="CAPTURE_TIME_BEFORE_PREFIX"):
        freeze_checkpoint(
            config_path,
            tmp_path / "artifacts/checkpoint",
            {"runtime": identity},
            captured_at=START + timedelta(seconds=104),
            minimum_observation_seconds=60,
        )


def test_rows_decode_only_on_demand() -> None:
    # An unread malformed suffix must not be decoded or retained before replay
    # asks for it. This also catches a regression back to list materialization.
    with patch.object(Path, "open", return_value=io.BytesIO(b'{"ok":1}\nnot-json\n')):
        rows = checkpoint_freeze._rows(Path("unused"), 1024)
        assert next(rows) == {"ok": 1}
        with pytest.raises(ValueError, match="FRAME_INVALID"):
            next(rows)


def test_capacity_reserves_room_for_active_soak(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    source = tmp_path / "active.jsonl"
    source.write_bytes(b"a" * 10)
    usage = namedtuple("Usage", "free")
    reserve = checkpoint_freeze.MINIMUM_FREE_RESERVE_BYTES
    monkeypatch.setattr(checkpoint_freeze.shutil, "disk_usage", lambda _: usage(free=reserve + 99))
    with pytest.raises(ValueError, match="CAPACITY_INSUFFICIENT"):
        checkpoint_freeze._check_capacity(source, tmp_path, 10, 100)
    monkeypatch.setattr(checkpoint_freeze.shutil, "disk_usage", lambda _: usage(free=reserve + 100))
    checkpoint_freeze._check_capacity(source, tmp_path, 10, 100)


def test_diagnostic_freeze_preserves_missing_evidence_without_accepting_it(
    setup: tuple[Config, dict[str, Signer]],
    tmp_path: Path,
) -> None:
    config, signers = setup
    config_path, config = _build_prefix(config, signers, final_seconds=105)
    # A signed source loses its target-bound effects at episode onset.
    for role in config.bindings:
        value = packet(config, signers, role, 120)
        if role == "dell":
            value["facts"]["effects"] = {"observed_at": None, "integrity": "UNKNOWN"}
        if role == "arena":
            value["facts"]["platform"]["state"] = "DOWN"
        recovery_soak.atomic_write_json(Path(config.value["hosts"][role]["inbox_file"]), signers[role].sign(value))
    recovery_soak.collect(config, now=START + timedelta(seconds=120))
    before = Path(config.value["evidence_file"]).read_bytes()
    out = tmp_path / "diagnostics/frozen"
    result = freeze_checkpoint(config_path, out, {"config_identity": config_path}, preserve_incomplete=True)
    assert result["classification"] == "EVIDENCE_FROZEN_FOR_DIAGNOSIS"
    assert result["observation_acceptable"] is False
    assert result["checkpoint_boundary"] is None
    assert result["evaluation"]["harness_classification"] == "MISSING_EVIDENCE"
    assert "DELL_EFFECT_EVIDENCE_MISSING_OR_STALE" in result["evaluation"]["unknown_reasons"]
    assert (out / "samples.committed-prefix.jsonl").read_bytes() == before
    assert Path(config.value["evidence_file"]).read_bytes() == before
    with pytest.raises(ValueError, match="24H_NOT_YET_ELIGIBLE"):
        freeze_checkpoint(config_path, tmp_path / "diagnostics/acceptance", {"config_identity": config_path})
