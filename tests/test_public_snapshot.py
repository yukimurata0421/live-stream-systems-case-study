from __future__ import annotations

import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("validate_public_snapshot", ROOT / "tools" / "validate_public_snapshot.py")
assert SPEC is not None and SPEC.loader is not None
validator = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(validator)


def test_current_public_tree_has_no_boundary_findings() -> None:
    assert validator.validate(ROOT) == []


def test_private_workspace_and_runtime_artifacts_are_rejected(tmp_path: Path) -> None:
    for relative in ("README.md", "LICENSE", "pyproject.toml"):
        (tmp_path / relative).write_text("fixture\n", encoding="utf-8")
    for relative in ("src", "contracts", "migrations", "policy", "tests"):
        (tmp_path / relative).mkdir()
    private_path = "/home/" + "yuki/private"
    (tmp_path / "src" / "bad.py").write_text(f"path = '{private_path}'\n", encoding="utf-8")
    (tmp_path / "runtime.sqlite3").write_bytes(b"sqlite fixture")

    reasons = {item["reason"] for item in validator.validate(tmp_path)}

    assert reasons == {"private_workspace_path", "runtime_or_secret_artifact"}
