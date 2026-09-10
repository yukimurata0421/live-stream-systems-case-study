from __future__ import annotations

import hashlib
import os
import subprocess
import sys
from pathlib import Path

from cra_dell_recovery.canonical import canonical_json
from runtime_boundary.recovery_evidence import source_hash
from tools.build_runtime_fence_release import copy_executor_packages


def test_owner_hash_matches_signed_facts_canonicalization() -> None:
    for value in ({}, {"ffmpeg_pid": 123, "host_id": "dell-yuki"}, {"message": "日本語", "nested": {"b": 2, "a": 1}}):
        assert source_hash(value) == hashlib.sha256(canonical_json({"raw": value})).hexdigest()


def test_owner_export_import_does_not_require_signing_or_schema_packages(tmp_path: Path) -> None:
    source = """
import importlib.abc
import sys

class DenySigningPackages(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split('.')[0] in {'cryptography', 'jsonschema'}:
            raise ModuleNotFoundError('signing dependency forbidden for runtime owner')

sys.meta_path.insert(0, DenySigningPackages())
from runtime_boundary.recovery_evidence import source_hash
assert len(source_hash({'ffmpeg_pid': 123})) == 64
from runtime_boundary.recovery_publisher import RecoveryEvidencePublisher
assert 'cra_dell_recovery.canonical' not in sys.modules
"""
    # Exercise the real image assembly, not the development package's eager
    # signing initializer (which is deliberately absent from the executor).
    root = tmp_path / "overlay/app/src"
    copy_executor_packages(Path(__file__).resolve().parents[2], root)
    result = subprocess.run(
        [sys.executable, "-c", source],
        env={**os.environ, "PYTHONPATH": str(root), "PYTHONDONTWRITEBYTECODE": "1"},
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    assert result.returncode == 0, result.stderr
