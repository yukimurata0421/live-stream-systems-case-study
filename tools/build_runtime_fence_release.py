from __future__ import annotations

import argparse
import ast
import hashlib
import json
import os
import shutil
import stat
import subprocess
import zipfile
from pathlib import Path
from typing import Any

TREE_DIGEST_ALGORITHM = "sha256-concat-relative-name-nul-file-sha256-nul-v1"


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _tree(root: Path) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for path in sorted(item for item in root.rglob("*") if item.is_file()):
        relative = str(path.relative_to(root))
        result[relative] = {"sha256": _sha256(path), "size": path.stat().st_size}
    return result


def _tree_digest(files: dict[str, dict[str, Any]]) -> str:
    value = b"".join(f"{name}\0{files[name]['sha256']}\0".encode() for name in sorted(files))
    return hashlib.sha256(value).hexdigest()


def validate_controller_base(root: Path) -> None:
    if root.is_symlink() or not root.is_dir():
        raise ValueError("RUNTIME_FENCE_BASE_CONTROLLER_NOT_DIRECTORY")
    for path in root.rglob("*"):
        if path.is_symlink():
            raise ValueError("RUNTIME_FENCE_BASE_CONTROLLER_SYMLINK")
        if "__pycache__" in path.parts or path.suffix in {".pyc", ".pyo"}:
            raise ValueError("RUNTIME_FENCE_BASE_CONTROLLER_CACHE_PRESENT")


def make_owner_writable(path: Path) -> None:
    os.chmod(path, stat.S_IMODE(path.stat().st_mode) | stat.S_IWUSR)


def apply_controller_file_overlays(
    controller: Path,
    overlays: dict[Path, Path],
) -> dict[str, str]:
    """Copy explicitly named controller files without accepting path escape."""

    applied: dict[str, str] = {}
    for relative, source_file in sorted(overlays.items(), key=lambda item: str(item[0])):
        if relative.is_absolute() or ".." in relative.parts or not relative.parts or relative.parts[0] != "src":
            raise ValueError("RUNTIME_FENCE_CONTROLLER_FILE_TARGET_INVALID")
        if not source_file.is_file() or source_file.is_symlink():
            raise ValueError("RUNTIME_FENCE_CONTROLLER_FILE_OVERLAY_INVALID")
        destination = controller / relative
        if destination.is_symlink():
            raise ValueError("RUNTIME_FENCE_CONTROLLER_FILE_TARGET_INVALID")
        parent = controller
        make_owner_writable(parent)
        for part in relative.parent.parts:
            parent = parent / part
            if parent.is_symlink():
                raise ValueError("RUNTIME_FENCE_CONTROLLER_FILE_TARGET_INVALID")
            if parent.exists():
                if not parent.is_dir():
                    raise ValueError("RUNTIME_FENCE_CONTROLLER_FILE_TARGET_INVALID")
                make_owner_writable(parent)
            else:
                parent.mkdir()
        if destination.exists():
            make_owner_writable(destination)
        shutil.copy2(source_file, destination)
        applied[str(relative)] = _sha256(destination)
    return applied


def remove_partial_build_output(output: Path) -> None:
    """Remove only the exact output tree created by this builder."""

    if not output.exists():
        return
    paths = sorted(output.rglob("*"), key=lambda path: len(path.parts), reverse=True)
    for path in paths:
        if path.is_symlink():
            continue
        try:
            os.chmod(path, stat.S_IMODE(path.stat().st_mode) | stat.S_IWUSR)
        except FileNotFoundError:
            continue
    make_owner_writable(output)
    shutil.rmtree(output)


def _commit(source: Path) -> str:
    return subprocess.run(["git", "rev-parse", "HEAD"], cwd=source, check=True, capture_output=True, text=True).stdout.strip()


def copy_executor_packages(source: Path, image_overlay: Path) -> None:
    """Build the runtime-only import boundary used by the executor image."""
    shutil.copytree(source / "src/runtime_boundary", image_overlay / "runtime_boundary")
    shutil.copytree(source / "src/cra_dell_recovery", image_overlay / "cra_dell_recovery")
    (image_overlay / "cra_dell_recovery/__init__.py").write_text(
        '"""Effect-fence-only protocol package boundary."""\n',
        encoding="utf-8",
    )
    for cache in image_overlay.rglob("__pycache__"):
        shutil.rmtree(cache)


def replace_controller_reconciler(current_source: str, replacement_source: str) -> str:
    start_marker = "def maybe_reconcile_delayed_executor_effects("
    end_marker = "\ndef trim_samples("
    if current_source.count(start_marker) != 1 or current_source.count(end_marker) != 1:
        raise ValueError("RUNTIME_FENCE_CONTROLLER_RECONCILER_BOUNDARY_INVALID")
    start = current_source.index(start_marker)
    end = current_source.index(end_marker, start)
    replacement = replacement_source.strip()
    replacement = replacement[replacement.index(start_marker) :] if start_marker in replacement else replacement
    if not replacement.startswith(start_marker) or "\ndef " in replacement:
        raise ValueError("RUNTIME_FENCE_CONTROLLER_RECONCILER_OVERLAY_INVALID")
    return current_source[:start] + replacement + "\n\n" + current_source[end + 1 :]


def replace_controller_functions(current_source: str, overlay_source: str) -> str:
    """Replace named top-level functions while preserving the exact deployed base."""

    try:
        current_tree = ast.parse(current_source)
        overlay_tree = ast.parse(overlay_source)
    except SyntaxError as exc:
        raise ValueError("RUNTIME_FENCE_CONTROLLER_FUNCTION_OVERLAY_INVALID") from exc
    overlay_functions = [node for node in overlay_tree.body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))]
    if not overlay_functions or len(overlay_functions) != len(overlay_tree.body):
        raise ValueError("RUNTIME_FENCE_CONTROLLER_FUNCTION_OVERLAY_INVALID")
    current_functions: dict[str, ast.FunctionDef | ast.AsyncFunctionDef] = {}
    duplicates: set[str] = set()
    for node in current_tree.body:
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        if node.name in current_functions:
            duplicates.add(node.name)
        current_functions[node.name] = node
    lines = current_source.splitlines(keepends=True)
    overlay_lines = overlay_source.splitlines(keepends=True)
    replacements: list[tuple[int, int, str]] = []
    for replacement in overlay_functions:
        current = current_functions.get(replacement.name)
        if current is None or replacement.name in duplicates:
            raise ValueError(f"RUNTIME_FENCE_CONTROLLER_FUNCTION_MISSING:{replacement.name}")
        if current.end_lineno is None or replacement.end_lineno is None:
            raise ValueError("RUNTIME_FENCE_CONTROLLER_FUNCTION_OVERLAY_INVALID")
        current_start_line = min([current.lineno, *(item.lineno for item in current.decorator_list)])
        replacement_start_line = min([replacement.lineno, *(item.lineno for item in replacement.decorator_list)])
        start = sum(len(line) for line in lines[: current_start_line - 1])
        end = sum(len(line) for line in lines[: current.end_lineno])
        replacement_text = "".join(overlay_lines[replacement_start_line - 1 : replacement.end_lineno]).rstrip() + "\n"
        replacements.append((start, end, replacement_text))
    generated = current_source
    for start, end, replacement_text in sorted(replacements, reverse=True):
        generated = generated[:start] + replacement_text + generated[end:]
    try:
        compile(generated, "generated-controller-function-overlay.py", "exec")
    except SyntaxError as exc:
        raise ValueError("RUNTIME_FENCE_CONTROLLER_FUNCTION_OVERLAY_INVALID") from exc
    return generated


def _build_into(
    *,
    source: Path,
    base_controller: Path,
    output: Path,
    overlay: Path,
    release_id: str,
    base_image: str,
    candidate_image: str,
    expected_base_tree_sha256: str,
    dependency_wheels: list[Path],
    controller_install_root: Path,
    controller_reconciler_overlay: Path | None = None,
    controller_function_overlays: dict[Path, Path] | None = None,
    controller_file_overlays: dict[Path, Path] | None = None,
    executor_file_overlays: dict[Path, Path] | None = None,
) -> dict[str, Any]:
    if not controller_install_root.is_absolute():
        raise ValueError("RUNTIME_FENCE_INSTALL_ROOT_NOT_ABSOLUTE")
    validate_controller_base(base_controller)
    base_files = _tree(base_controller)
    actual_base_digest = _tree_digest(base_files)
    if actual_base_digest != expected_base_tree_sha256:
        raise ValueError("RUNTIME_FENCE_BASE_TREE_MISMATCH")
    controller = output / "controller"
    shutil.copytree(base_controller, controller, symlinks=True)
    for cache in controller.rglob("__pycache__"):
        shutil.rmtree(cache)
    applied_controller_file_overlays = apply_controller_file_overlays(
        controller,
        controller_file_overlays or {},
    )
    if controller_reconciler_overlay is not None:
        if not controller_reconciler_overlay.is_file():
            raise ValueError("RUNTIME_FENCE_CONTROLLER_RECONCILER_OVERLAY_MISSING")
        controller_entrypoint = controller / "src/watchers/fast_recovery.py"
        current_source = controller_entrypoint.read_text(encoding="utf-8")
        make_owner_writable(controller_entrypoint)
        controller_entrypoint.write_text(
            replace_controller_reconciler(
                current_source,
                controller_reconciler_overlay.read_text(encoding="utf-8"),
            ),
            encoding="utf-8",
        )
    applied_function_overlays: dict[str, str] = {}
    for relative, function_overlay in sorted((controller_function_overlays or {}).items(), key=lambda item: str(item[0])):
        if relative.is_absolute() or ".." in relative.parts or not relative.parts:
            raise ValueError("RUNTIME_FENCE_CONTROLLER_FUNCTION_TARGET_INVALID")
        if not function_overlay.is_file():
            raise ValueError("RUNTIME_FENCE_CONTROLLER_FUNCTION_OVERLAY_MISSING")
        target = controller / relative
        if not target.is_file() or target.is_symlink():
            raise ValueError("RUNTIME_FENCE_CONTROLLER_FUNCTION_TARGET_INVALID")
        make_owner_writable(target)
        target.write_text(
            replace_controller_functions(
                target.read_text(encoding="utf-8"),
                function_overlay.read_text(encoding="utf-8"),
            ),
            encoding="utf-8",
        )
        applied_function_overlays[str(relative)] = _sha256(function_overlay)
    destination = controller / "src/watchers/fast_recovery_core/effect_contract.py"
    make_owner_writable(destination)
    shutil.copy2(overlay, destination)

    image_overlay = output / "executor-image/overlay/app/src"
    copy_executor_packages(source, image_overlay)
    applied_executor_file_overlays: dict[str, str] = {}
    for relative, file_overlay in sorted((executor_file_overlays or {}).items(), key=lambda item: str(item[0])):
        if relative.is_absolute() or ".." in relative.parts or not relative.parts:
            raise ValueError("RUNTIME_FENCE_EXECUTOR_FILE_TARGET_INVALID")
        if not file_overlay.is_file() or file_overlay.is_symlink():
            raise ValueError("RUNTIME_FENCE_EXECUTOR_FILE_OVERLAY_INVALID")
        destination_file = image_overlay / relative
        if destination_file.is_symlink():
            raise ValueError("RUNTIME_FENCE_EXECUTOR_FILE_TARGET_INVALID")
        destination_file.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(file_overlay, destination_file)
        applied_executor_file_overlays[str(relative)] = _sha256(destination_file)
    containerfile = output / "executor-image/Containerfile"
    wheel_directory = output / "executor-image/wheels"
    wheel_directory.mkdir()
    wheels: dict[str, dict[str, Any]] = {}
    for wheel in dependency_wheels:
        if not wheel.is_file() or wheel.suffix != ".whl":
            raise ValueError("RUNTIME_FENCE_DEPENDENCY_WHEEL_INVALID")
        destination_wheel = wheel_directory / wheel.name
        shutil.copy2(wheel, destination_wheel)
        wheels[wheel.name] = {"sha256": _sha256(destination_wheel), "size": destination_wheel.stat().st_size}
    if not any(name.startswith("rfc8785-") for name in wheels):
        raise ValueError("RUNTIME_FENCE_REQUIRED_DEPENDENCY_MISSING")
    vendor_directory = output / "executor-image/vendor"
    vendor_directory.mkdir()
    allowed_packages = {"rfc8785"}
    extracted_packages: set[str] = set()
    for wheel in dependency_wheels:
        with zipfile.ZipFile(wheel) as archive:
            for member in archive.infolist():
                parts = Path(member.filename).parts
                if not parts or parts[0] not in allowed_packages:
                    continue
                if member.is_dir():
                    continue
                if any(part in {"", ".", ".."} for part in parts):
                    raise ValueError("RUNTIME_FENCE_WHEEL_MEMBER_UNSAFE")
                destination_member = vendor_directory.joinpath(*parts)
                destination_member.parent.mkdir(parents=True, exist_ok=True)
                destination_member.write_bytes(archive.read(member))
                extracted_packages.add(parts[0])
    if extracted_packages != allowed_packages:
        raise ValueError("RUNTIME_FENCE_DEPENDENCY_EXTRACTION_INCOMPLETE")
    copy_lines = [
        "COPY vendor /usr/local/lib/python3.11/dist-packages",
        "COPY overlay/app/src/runtime_boundary /app/src/runtime_boundary",
        "COPY overlay/app/src/cra_dell_recovery /app/src/cra_dell_recovery",
    ]
    if (image_overlay / "stream_core").is_dir():
        copy_lines.append("COPY overlay/app/src/stream_core /app/src/stream_core")
    containerfile.write_text(
        "\n".join(
            [
                f"FROM {base_image}",
                *copy_lines,
                f'LABEL org.stream-v3.effect-fence-release="{release_id}"',
                "",
            ]
        ),
        encoding="utf-8",
    )
    unit = output / "stream-v3-fast-recovery-controller.service"
    old_root = str(base_controller)
    current_unit = Path("/etc/systemd/system/stream-v3-fast-recovery-controller.service").read_text(encoding="utf-8")
    if old_root not in current_unit:
        raise ValueError("RUNTIME_FENCE_CONTROLLER_ROOT_NOT_IN_UNIT")
    generated_unit = current_unit.replace(old_root, str(controller_install_root))
    if old_root in generated_unit or str(output) in generated_unit:
        raise ValueError("RUNTIME_FENCE_GENERATED_UNIT_NOT_RELOCATABLE")
    unit.write_text(generated_unit, encoding="utf-8")
    manifest = {
        "schema": "runtime.effect_fence_release.v1",
        "release_id": release_id,
        "source_commit": _commit(source),
        "base_controller_root": str(base_controller),
        "base_controller_tree_sha256": actual_base_digest,
        "controller_tree_sha256": _tree_digest(_tree(controller)),
        "tree_digest_algorithm": TREE_DIGEST_ALGORITHM,
        "controller_install_root": str(controller_install_root),
        "effect_contract_sha256": _sha256(destination),
        "controller_reconciler_overlay_sha256": (None if controller_reconciler_overlay is None else _sha256(controller_reconciler_overlay)),
        "controller_function_overlay_sha256": applied_function_overlays,
        "controller_file_overlay_sha256": applied_controller_file_overlays,
        "base_image": base_image,
        "candidate_image": candidate_image,
        "executor_overlay_tree_sha256": _tree_digest(_tree(image_overlay)),
        "executor_file_overlay_sha256": applied_executor_file_overlays,
        "dependency_wheels": wheels,
        "dependency_vendor_tree_sha256": _tree_digest(_tree(vendor_directory)),
        "rollback_controller_root": str(base_controller),
        "rollback_image": base_image,
        "planned_pod_replacements": 1,
        "raw_ledger_rows_mutated": False,
    }
    manifest_path = output / "release_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    for path in output.rglob("*"):
        if path.is_file():
            os.chmod(path, 0o644 if not os.access(path, os.X_OK) else 0o755)
    return manifest


def build(
    *,
    source: Path,
    base_controller: Path,
    output: Path,
    overlay: Path,
    release_id: str,
    base_image: str,
    candidate_image: str,
    expected_base_tree_sha256: str,
    dependency_wheels: list[Path],
    controller_install_root: Path,
    controller_reconciler_overlay: Path | None = None,
    controller_function_overlays: dict[Path, Path] | None = None,
    controller_file_overlays: dict[Path, Path] | None = None,
    executor_file_overlays: dict[Path, Path] | None = None,
) -> dict[str, Any]:
    try:
        output.mkdir(parents=True, mode=0o700)
    except FileExistsError as error:
        raise FileExistsError("RUNTIME_FENCE_OUTPUT_EXISTS") from error
    try:
        return _build_into(
            source=source,
            base_controller=base_controller,
            output=output,
            overlay=overlay,
            release_id=release_id,
            base_image=base_image,
            candidate_image=candidate_image,
            expected_base_tree_sha256=expected_base_tree_sha256,
            dependency_wheels=dependency_wheels,
            controller_install_root=controller_install_root,
            controller_reconciler_overlay=controller_reconciler_overlay,
            controller_function_overlays=controller_function_overlays,
            controller_file_overlays=controller_file_overlays,
            executor_file_overlays=executor_file_overlays,
        )
    except BaseException:
        remove_partial_build_output(output)
        raise


def main() -> None:
    parser = argparse.ArgumentParser(description="Build a target-wide effect fence overlay release")
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--base-controller", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--overlay", type=Path, required=True)
    parser.add_argument("--release-id", required=True)
    parser.add_argument("--base-image", required=True)
    parser.add_argument("--candidate-image", required=True)
    parser.add_argument("--expected-base-tree-sha256", required=True)
    parser.add_argument("--dependency-wheel", type=Path, action="append", required=True)
    parser.add_argument("--controller-install-root", type=Path, required=True)
    parser.add_argument("--controller-reconciler-overlay", type=Path)
    parser.add_argument(
        "--controller-function-overlay",
        action="append",
        default=[],
        metavar="RELATIVE_PATH=OVERLAY_PATH",
    )
    parser.add_argument(
        "--controller-file-overlay",
        action="append",
        default=[],
        metavar="RELATIVE_PATH=OVERLAY_PATH",
    )
    parser.add_argument(
        "--executor-file-overlay",
        action="append",
        default=[],
        metavar="RELATIVE_PATH=OVERLAY_PATH",
    )
    args = parser.parse_args()
    function_overlays: dict[Path, Path] = {}
    for raw in args.controller_function_overlay:
        relative_text, separator, overlay_text = raw.partition("=")
        if not separator or not relative_text or not overlay_text:
            raise ValueError("RUNTIME_FENCE_CONTROLLER_FUNCTION_OVERLAY_ARGUMENT_INVALID")
        relative = Path(relative_text)
        if relative in function_overlays:
            raise ValueError("RUNTIME_FENCE_CONTROLLER_FUNCTION_OVERLAY_DUPLICATE")
        function_overlays[relative] = Path(overlay_text).resolve()
    controller_file_overlays: dict[Path, Path] = {}
    for raw in args.controller_file_overlay:
        relative_text, separator, overlay_text = raw.partition("=")
        if not separator or not relative_text or not overlay_text:
            raise ValueError("RUNTIME_FENCE_CONTROLLER_FILE_OVERLAY_ARGUMENT_INVALID")
        relative = Path(relative_text)
        if relative in controller_file_overlays:
            raise ValueError("RUNTIME_FENCE_CONTROLLER_FILE_OVERLAY_DUPLICATE")
        controller_file_overlays[relative] = Path(overlay_text).resolve()
    executor_file_overlays: dict[Path, Path] = {}
    for raw in args.executor_file_overlay:
        relative_text, separator, overlay_text = raw.partition("=")
        if not separator or not relative_text or not overlay_text:
            raise ValueError("RUNTIME_FENCE_EXECUTOR_FILE_OVERLAY_ARGUMENT_INVALID")
        relative = Path(relative_text)
        if relative in executor_file_overlays:
            raise ValueError("RUNTIME_FENCE_EXECUTOR_FILE_OVERLAY_DUPLICATE")
        executor_file_overlays[relative] = Path(overlay_text).resolve()
    report = build(
        source=args.source.resolve(),
        base_controller=args.base_controller.resolve(),
        output=args.output.resolve(),
        overlay=args.overlay.resolve(),
        release_id=args.release_id,
        base_image=args.base_image,
        candidate_image=args.candidate_image,
        expected_base_tree_sha256=args.expected_base_tree_sha256,
        dependency_wheels=[path.resolve() for path in args.dependency_wheel],
        controller_install_root=args.controller_install_root,
        controller_reconciler_overlay=(
            None if args.controller_reconciler_overlay is None else args.controller_reconciler_overlay.resolve()
        ),
        controller_function_overlays=function_overlays,
        controller_file_overlays=controller_file_overlays,
        executor_file_overlays=executor_file_overlays,
    )
    print(json.dumps(report, separators=(",", ":"), sort_keys=True))


if __name__ == "__main__":
    main()
