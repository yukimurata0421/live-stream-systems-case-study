from __future__ import annotations

from pathlib import Path

import pytest

import tools.build_runtime_fence_release as builder
from tools.build_runtime_fence_release import (
    TREE_DIGEST_ALGORITHM,
    apply_controller_file_overlays,
    make_owner_writable,
    replace_controller_functions,
    replace_controller_reconciler,
    validate_controller_base,
)

BASE = """\
def before():
    return 1

def maybe_reconcile_delayed_executor_effects(old):
    return old

def trim_samples(samples):
    return samples[-1:]
"""

OVERLAY = """\
# ruff: noqa: F821
def maybe_reconcile_delayed_executor_effects(new):
    return new, "TARGET_RETIRED"
"""


def test_controller_reconciler_overlay_replaces_exactly_one_function() -> None:
    generated = replace_controller_reconciler(BASE, OVERLAY)

    assert "return old" not in generated
    assert 'return new, "TARGET_RETIRED"' in generated
    assert generated.count("def maybe_reconcile_delayed_executor_effects(") == 1
    assert "def before():" in generated
    assert "def trim_samples(samples):" in generated
    compile(generated, "generated-fast-recovery.py", "exec")
    assert TREE_DIGEST_ALGORITHM == "sha256-concat-relative-name-nul-file-sha256-nul-v1"


@pytest.mark.parametrize(
    "base",
    [
        "def trim_samples(samples):\n    return samples\n",
        BASE.replace("def maybe_reconcile_delayed_executor_effects(old):", "def unrelated(old):"),
        BASE + "\ndef maybe_reconcile_delayed_executor_effects(duplicate):\n    return duplicate\n",
    ],
)
def test_controller_reconciler_overlay_fails_closed_on_base_boundary_drift(base: str) -> None:
    with pytest.raises(ValueError, match="RUNTIME_FENCE_CONTROLLER_RECONCILER_BOUNDARY_INVALID"):
        replace_controller_reconciler(base, OVERLAY)


def test_controller_reconciler_overlay_rejects_more_than_one_top_level_function() -> None:
    with pytest.raises(ValueError, match="RUNTIME_FENCE_CONTROLLER_RECONCILER_OVERLAY_INVALID"):
        replace_controller_reconciler(
            BASE,
            OVERLAY + "\ndef unexpected():\n    return None\n",
        )


def test_controller_function_overlay_replaces_multiple_named_functions_only() -> None:
    base = """\
def first(value):
    return "old-first", value

def untouched():
    return "same"

def second(value):
    return "old-second", value
"""
    overlay = """\
# ruff: noqa: F821
def first(value):
    return "new-first", value

def second(value):
    return "new-second", value
"""

    generated = replace_controller_functions(base, overlay)

    assert "old-first" not in generated
    assert "old-second" not in generated
    assert "new-first" in generated
    assert "new-second" in generated
    assert 'return "same"' in generated
    compile(generated, "generated-controller.py", "exec")


def test_controller_function_overlay_fails_closed_on_missing_or_non_function_nodes() -> None:
    with pytest.raises(ValueError, match="RUNTIME_FENCE_CONTROLLER_FUNCTION_MISSING"):
        replace_controller_functions("def present():\n    pass\n", "def absent():\n    pass\n")
    with pytest.raises(ValueError, match="RUNTIME_FENCE_CONTROLLER_FUNCTION_OVERLAY_INVALID"):
        replace_controller_functions("def present():\n    pass\n", "import os\ndef present():\n    pass\n")


@pytest.mark.parametrize("cache_name", ["module.pyc", "module.pyo", "__pycache__/module.cpython-314.pyc"])
def test_controller_base_rejects_generated_bytecode(tmp_path: Path, cache_name: str) -> None:
    cache = tmp_path / cache_name
    cache.parent.mkdir(parents=True, exist_ok=True)
    cache.write_bytes(b"generated")

    with pytest.raises(ValueError, match="RUNTIME_FENCE_BASE_CONTROLLER_CACHE_PRESENT"):
        validate_controller_base(tmp_path)


def test_controller_base_rejects_symlink(tmp_path: Path) -> None:
    source = tmp_path / "source.py"
    source.write_text("pass\n", encoding="utf-8")
    (tmp_path / "linked.py").symlink_to(source)

    with pytest.raises(ValueError, match="RUNTIME_FENCE_BASE_CONTROLLER_SYMLINK"):
        validate_controller_base(tmp_path)


def test_builder_can_make_copied_read_only_controller_file_writable(tmp_path: Path) -> None:
    target = tmp_path / "fast_recovery.py"
    target.write_text("old\n", encoding="utf-8")
    target.chmod(0o444)

    make_owner_writable(target)
    target.write_text("new\n", encoding="utf-8")

    assert target.read_text(encoding="utf-8") == "new\n"


def test_controller_file_overlay_adds_hashed_module_under_src(tmp_path: Path) -> None:
    controller = tmp_path / "controller"
    controller.mkdir()
    controller.chmod(0o555)
    source = tmp_path / "ack_delivery.py"
    source.write_text("VALUE = 1\n", encoding="utf-8")

    applied = apply_controller_file_overlays(
        controller,
        {Path("src/watchers/fast_recovery_core/ack_delivery.py"): source},
    )

    target = controller / "src/watchers/fast_recovery_core/ack_delivery.py"
    assert target.read_text(encoding="utf-8") == "VALUE = 1\n"
    assert applied == {
        "src/watchers/fast_recovery_core/ack_delivery.py": builder._sha256(target),
    }


@pytest.mark.parametrize(
    "relative",
    [Path("/tmp/ack_delivery.py"), Path("../ack_delivery.py"), Path("ack_delivery.py")],
)
def test_controller_file_overlay_rejects_path_escape_or_non_src_target(tmp_path: Path, relative: Path) -> None:
    controller = tmp_path / "controller"
    controller.mkdir()
    source = tmp_path / "ack_delivery.py"
    source.write_text("VALUE = 1\n", encoding="utf-8")

    with pytest.raises(ValueError, match="RUNTIME_FENCE_CONTROLLER_FILE_TARGET_INVALID"):
        apply_controller_file_overlays(controller, {relative: source})


def test_builder_removes_partial_output_after_failure(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    output = tmp_path / "candidate"

    def fail(**_kwargs: object) -> dict[str, object]:
        partial = output / "partial"
        partial.write_text("incomplete\n", encoding="utf-8")
        partial.chmod(0o444)
        output.chmod(0o555)
        raise ValueError("synthetic build failure")

    monkeypatch.setattr(builder, "_build_into", fail)
    with pytest.raises(ValueError, match="synthetic build failure"):
        builder.build(
            source=tmp_path,
            base_controller=tmp_path,
            output=output,
            overlay=tmp_path / "overlay.py",
            release_id="candidate-release",
            base_image="base",
            candidate_image="candidate",
            expected_base_tree_sha256="0" * 64,
            dependency_wheels=[],
            controller_install_root=Path("/opt/candidate"),
        )

    assert not output.exists()
