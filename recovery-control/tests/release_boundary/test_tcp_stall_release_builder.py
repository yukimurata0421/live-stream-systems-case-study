from __future__ import annotations

import json
from pathlib import Path

from tcp_stall_collector.service import load_config
from tools.build_tcp_stall_collector_release import RUNTIME_FILES, UNIT_FILE, build_release, source_tree_sha256

ROOT = Path(__file__).resolve().parents[2]


def test_builder_emits_minimal_hash_bound_release(tmp_path: Path) -> None:
    release_id = "tcp-stall-test-abcdef123456"
    manifest = build_release(source_root=ROOT, staging_root=tmp_path, release_id=release_id)
    release = tmp_path / "opt/stream-recovery-control/tcp-stall/releases" / release_id
    config_path = tmp_path / "etc/stream-recovery-control/tcp-stall/releases" / release_id / "collector.json"
    unit = tmp_path / "etc/systemd/system" / UNIT_FILE.name
    assert release.is_dir() and unit.is_file()
    assert load_config(config_path).release_id == release_id
    assert manifest["source_tree_sha256"] == source_tree_sha256(ROOT)
    assert set(manifest["runtime_file_sha256"]) == {str(path) for path in RUNTIME_FILES}
    assert json.loads((release / "release_manifest.json").read_text())["release_id"] == release_id
    assert (release / "src/tcp_stall_collector/service.py").is_file()
    assert not (release / "src/fast_recovery_controller").exists()
    assert not (release / "src/cra_authority").exists()
