from __future__ import annotations

import json
import os
import shutil
import stat
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from ops.release import cluster
from ops.release import service as release
from ops.release import tree
from ops.release.tree import records_digest, release_lock, tree_records
from ops.scripts.monitoring_v4_source_identity import content_identity


ROOT = Path(__file__).resolve().parents[1]
APP_IMAGE_ID = "sha256:" + "5" * 64


def _copy_snapshot(target: Path) -> Path:
    ignored = shutil.ignore_patterns(
        ".git",
        ".state",
        ".pytest_cache",
        "__pycache__",
        "*.pyc",
        "build",
    )
    shutil.copytree(ROOT, target, ignore=ignored)
    return target


def _prepare(source: Path, base: Path, revision: int) -> tuple[str, Path]:
    identity = content_identity(source)
    snapshot = _snapshot(source)
    name = f"k3s-postgres-{identity}-r{revision}"
    result = release.prepare_release(
        source_root=source,
        release_base=base,
        release_name=name,
        expected_identity=identity,
        expected_snapshot_sha256=snapshot,
        app_image=f"stream-monitoring-v4:{identity}",
        app_image_id=APP_IMAGE_ID,
    )
    return identity, result.release


def _snapshot(source: Path) -> str:
    return records_digest(tree_records(source, require_source_layout=True))


class ImmutableReleasePreparationTests(unittest.TestCase):
    def test_source_tree_work_bounds_fail_before_copy(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            source = _copy_snapshot(Path(td) / "source")
            with patch.object(tree, "MAX_FILES", 1):
                with self.assertRaisesRegex(RuntimeError, "file-count"):
                    tree.tree_records(source, require_source_layout=True)
            with patch.object(tree, "MAX_DIRECTORIES", 1):
                with self.assertRaisesRegex(RuntimeError, "directory-count"):
                    tree.tree_records(source, require_source_layout=True)

    def test_prepare_keeps_templates_and_creates_exact_readonly_rendered_tree(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            source = _copy_snapshot(root / "source")
            base = root / "releases"
            base.mkdir()
            identity, prepared = _prepare(source, base, 101)

            self.assertEqual(content_identity(prepared), identity)
            self.assertIn(
                "__BUILD_REVISION__",
                (prepared / "deploy/k3s/config.yaml").read_text(encoding="utf-8"),
            )
            rendered = (prepared / "deploy/k3s-rendered/config.yaml").read_text(
                encoding="utf-8"
            )
            self.assertIn(identity, rendered)
            self.assertIn(APP_IMAGE_ID, rendered)
            self.assertNotIn("__BUILD_REVISION__", rendered)
            self.assertFalse((base / "current").exists())

            files, total_bytes = release.validate_release(
                release=prepared,
                expected_identity=identity,
                expected_snapshot_sha256=_snapshot(source),
                app_image=f"stream-monitoring-v4:{identity}",
                app_image_id=APP_IMAGE_ID,
            )
            self.assertGreater(files, 200)
            self.assertGreater(total_bytes, 0)
            for directory, _, file_names in os.walk(prepared):
                self.assertEqual(stat.S_IMODE(os.lstat(directory).st_mode), 0o555)
                for name in file_names:
                    self.assertEqual(
                        stat.S_IMODE(os.lstat(Path(directory) / name).st_mode),
                        0o444,
                    )

    def test_prepare_rejects_changed_source_symlink_unknown_placeholder_and_existing_target(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            base = root / "releases"
            base.mkdir()

            changed = _copy_snapshot(root / "changed")
            identity = content_identity(changed)
            with (changed / "deploy/k3s/core.yaml").open("a", encoding="utf-8") as stream:
                stream.write("\n# changed after identity\n")
            with self.assertRaisesRegex(RuntimeError, "identity does not match"):
                release.prepare_release(
                    source_root=changed,
                    release_base=base,
                    release_name=f"k3s-postgres-{identity}-r102",
                    expected_identity=identity,
                    expected_snapshot_sha256=_snapshot(changed),
                    app_image=f"stream-monitoring-v4:{identity}",
                    app_image_id=APP_IMAGE_ID,
                )

            linked = _copy_snapshot(root / "linked")
            (linked / "docs/link.md").symlink_to(linked / "README.md")
            linked_identity = content_identity(linked)
            with self.assertRaisesRegex(RuntimeError, "safely|symlink|single-link"):
                release.prepare_release(
                    source_root=linked,
                    release_base=base,
                    release_name=f"k3s-postgres-{linked_identity}-r103",
                    expected_identity=linked_identity,
                    expected_snapshot_sha256="0" * 64,
                    app_image=f"stream-monitoring-v4:{linked_identity}",
                    app_image_id=APP_IMAGE_ID,
                )

            placeholder = _copy_snapshot(root / "placeholder")
            with (placeholder / "deploy/k3s/core.yaml").open("a", encoding="utf-8") as stream:
                stream.write("\n# __UNRECOGNIZED_RELEASE_VALUE__\n")
            placeholder_identity = content_identity(placeholder)
            with self.assertRaisesRegex(RuntimeError, "unrecognized placeholder"):
                release.prepare_release(
                    source_root=placeholder,
                    release_base=base,
                    release_name=f"k3s-postgres-{placeholder_identity}-r104",
                    expected_identity=placeholder_identity,
                    expected_snapshot_sha256=_snapshot(placeholder),
                    app_image=f"stream-monitoring-v4:{placeholder_identity}",
                    app_image_id=APP_IMAGE_ID,
                )

            clean = _copy_snapshot(root / "clean")
            clean_identity = content_identity(clean)
            existing = base / f"k3s-postgres-{clean_identity}-r105"
            existing.mkdir()
            marker = existing / "preserved"
            marker.write_text("do not overwrite", encoding="utf-8")
            with self.assertRaisesRegex(RuntimeError, "already exists"):
                release.prepare_release(
                    source_root=clean,
                    release_base=base,
                    release_name=existing.name,
                    expected_identity=clean_identity,
                    expected_snapshot_sha256=_snapshot(clean),
                    app_image=f"stream-monitoring-v4:{clean_identity}",
                    app_image_id=APP_IMAGE_ID,
                )
            self.assertEqual(marker.read_text(encoding="utf-8"), "do not overwrite")
            self.assertEqual(list(base.glob(".*.staging-*")), [])

    def test_release_lock_rejects_a_concurrent_prepare(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            source = _copy_snapshot(root / "source")
            base = root / "releases"
            base.mkdir()
            identity = content_identity(source)
            with release_lock(base):
                with self.assertRaisesRegex(RuntimeError, "holds the release lock"):
                    release.prepare_release(
                        source_root=source,
                        release_base=base,
                        release_name=f"k3s-postgres-{identity}-r106",
                        expected_identity=identity,
                        expected_snapshot_sha256=_snapshot(source),
                        app_image=f"stream-monitoring-v4:{identity}",
                        app_image_id=APP_IMAGE_ID,
                    )

    def test_promotion_requires_exact_current_valid_release_and_zero_diff_validator(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            source = _copy_snapshot(root / "source")
            base = root / "releases"
            base.mkdir()
            old = base / f"k3s-postgres-{'0' * 40}-r100"
            old.mkdir()
            old.chmod(0o555)
            current = base / "current"
            current.symlink_to(old)
            identity, prepared = _prepare(source, base, 107)
            calls: list[Path] = []

            def validate_while_locked(rendered: Path) -> None:
                calls.append(rendered)
                with self.assertRaisesRegex(RuntimeError, "holds the release lock"):
                    with release_lock(base):
                        pass

            promoted = release.promote_release(
                release_base=base,
                release_name=prepared.name,
                expected_current_release=old.name,
                expected_identity=identity,
                expected_snapshot_sha256=_snapshot(source),
                app_image=f"stream-monitoring-v4:{identity}",
                app_image_id=APP_IMAGE_ID,
                cluster_validator=validate_while_locked,
            )
            self.assertEqual(promoted, prepared)
            self.assertEqual(current.resolve(), prepared)
            self.assertEqual(calls, [prepared / "deploy/k3s-rendered"])

            _, second = _prepare(source, base, 108)

            def reject(_: Path) -> None:
                raise RuntimeError("cluster differs")

            with self.assertRaisesRegex(RuntimeError, "cluster differs"):
                release.promote_release(
                    release_base=base,
                    release_name=second.name,
                    expected_current_release=prepared.name,
                    expected_identity=identity,
                    expected_snapshot_sha256=_snapshot(source),
                    app_image=f"stream-monitoring-v4:{identity}",
                    app_image_id=APP_IMAGE_ID,
                    cluster_validator=reject,
                )
            self.assertEqual(current.resolve(), prepared)

            with self.assertRaisesRegex(RuntimeError, "changed before promotion"):
                release.promote_release(
                    release_base=base,
                    release_name=second.name,
                    expected_current_release=old.name,
                    expected_identity=identity,
                    expected_snapshot_sha256=_snapshot(source),
                    app_image=f"stream-monitoring-v4:{identity}",
                    app_image_id=APP_IMAGE_ID,
                    cluster_validator=lambda _: None,
                )
            self.assertEqual(current.resolve(), prepared)

            core = prepared / "deploy/k3s/core.yaml"
            core.chmod(0o644)
            with core.open("a", encoding="utf-8") as stream:
                stream.write("\n# tampered\n")
            core.chmod(0o444)
            with self.assertRaisesRegex(RuntimeError, "identity does not match"):
                release.validate_release(
                    release=prepared,
                    expected_identity=identity,
                    expected_snapshot_sha256=_snapshot(source),
                    app_image=f"stream-monitoring-v4:{identity}",
                    app_image_id=APP_IMAGE_ID,
                )

    def test_snapshot_cli_does_not_create_bytecode_and_reports_exact_tree(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            source = _copy_snapshot(Path(td) / "source")
            expected_snapshot = _snapshot(source)
            environment = os.environ.copy()
            environment.pop("PYTHONDONTWRITEBYTECODE", None)
            completed = subprocess.run(
                [
                    sys.executable,
                    os.fspath(source / "ops/scripts/monitoring_v4_release.py"),
                    "snapshot",
                    "--source-root",
                    os.fspath(source),
                ],
                check=True,
                cwd=td,
                env=environment,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            payload = json.loads(completed.stdout)
            self.assertEqual(payload["source_identity"], content_identity(source))
            self.assertEqual(payload["snapshot_sha256"], expected_snapshot)
            self.assertFalse(list(source.rglob("__pycache__")))
            self.assertFalse(list(source.rglob("*.pyc")))

    def test_prepare_rejects_generated_artifact_and_snapshot_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            base = root / "releases"
            base.mkdir()
            contaminated = _copy_snapshot(root / "contaminated")
            identity = content_identity(contaminated)
            expected_snapshot = _snapshot(contaminated)
            cache = contaminated / "ops/release/__pycache__"
            cache.mkdir()
            (cache / "service.cpython-314.pyc").write_bytes(b"generated")
            with self.assertRaisesRegex(RuntimeError, "generated artifact"):
                release.prepare_release(
                    source_root=contaminated,
                    release_base=base,
                    release_name=f"k3s-postgres-{identity}-r109",
                    expected_identity=identity,
                    expected_snapshot_sha256=expected_snapshot,
                    app_image=f"stream-monitoring-v4:{identity}",
                    app_image_id=APP_IMAGE_ID,
                )

            clean = _copy_snapshot(root / "clean-snapshot")
            clean_identity = content_identity(clean)
            with self.assertRaisesRegex(RuntimeError, "snapshot does not match"):
                release.prepare_release(
                    source_root=clean,
                    release_base=base,
                    release_name=f"k3s-postgres-{clean_identity}-r110",
                    expected_identity=clean_identity,
                    expected_snapshot_sha256="0" * 64,
                    app_image=f"stream-monitoring-v4:{clean_identity}",
                    app_image_id=APP_IMAGE_ID,
                )

    def test_cluster_validation_requires_clean_dry_run_and_exact_zero_diff(self) -> None:
        clean = SimpleNamespace(returncode=0, stdout=b"", stderr=b"")
        with patch.object(cluster.subprocess, "run", side_effect=[clean, clean]) as run:
            cluster.validate_cluster_unchanged(
                Path("/release/deploy/k3s-rendered"),
                kubectl="/usr/local/bin/k3s",
                sudo="/usr/bin/sudo",
            )
        self.assertEqual(run.call_count, 2)
        self.assertIn("--dry-run=server", run.call_args_list[0].args[0])
        self.assertIn("diff", run.call_args_list[1].args[0])

        diagnostic = SimpleNamespace(returncode=0, stdout=b"", stderr=b"warning")
        with patch.object(cluster.subprocess, "run", return_value=diagnostic):
            with self.assertRaisesRegex(RuntimeError, "dry-run"):
                cluster.validate_cluster_unchanged(
                    Path("/release/deploy/k3s-rendered"),
                    kubectl="/usr/local/bin/k3s",
                    sudo="/usr/bin/sudo",
                )

        changed = SimpleNamespace(returncode=1, stdout=b"manifest diff", stderr=b"")
        with patch.object(cluster.subprocess, "run", side_effect=[clean, changed]):
            with self.assertRaisesRegex(RuntimeError, "differ"):
                cluster.validate_cluster_unchanged(
                    Path("/release/deploy/k3s-rendered"),
                    kubectl="/usr/local/bin/k3s",
                    sudo="/usr/bin/sudo",
                )


if __name__ == "__main__":
    unittest.main()
