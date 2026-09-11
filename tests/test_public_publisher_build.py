from __future__ import annotations

import importlib.util
import tempfile
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
PUBLISHER = ROOT / "ops" / "public-publisher"


def load_script(name: str):
    path = PUBLISHER / "site" / "scripts" / name
    spec = importlib.util.spec_from_file_location(f"public_publisher_{path.stem}", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class PublicPublisherBuildTests(unittest.TestCase):
    def test_active_source_tree_is_ported_without_generated_snapshots(self) -> None:
        expected = (
            PUBLISHER / "status" / "collect_stream_v3_public.py",
            PUBLISHER / "status" / "stream-v3-prometheus.html",
            PUBLISHER / "status" / "stream-v3-loki.html",
            PUBLISHER / "site" / "scripts" / "build_public.py",
            PUBLISHER / "site" / "scripts" / "collect_reliability.py",
            PUBLISHER / "site" / "scripts" / "push_to_gcs.py",
            PUBLISHER / "site" / "public" / "index.html",
            PUBLISHER / "site" / "public" / "assets" / "site.js",
            PUBLISHER / "site" / "public" / "assets" / "site.css",
            PUBLISHER / "site" / "cloudflare-worker" / "src" / "index.js",
            PUBLISHER / "site" / "systemd" / "yukimurata-public-push.service",
            PUBLISHER / "site" / "systemd" / "yukimurata-public-push.timer",
        )
        self.assertTrue(all(path.is_file() for path in expected))
        self.assertFalse(any((PUBLISHER / "status").glob("*.json")))
        self.assertFalse(any((PUBLISHER / "site" / "public").glob("*.json")))

        executable = (
            PUBLISHER / "status" / "collect_stream_v3_public.py",
            PUBLISHER / "site" / "scripts" / "build_public.sh",
            PUBLISHER / "site" / "scripts" / "collect_reliability.py",
            PUBLISHER / "site" / "scripts" / "push_to_gcs.py",
            PUBLISHER / "site" / "scripts" / "push_to_gcs.sh",
        )
        self.assertTrue(all(path.stat().st_mode & 0o111 for path in executable))

    def test_push_requires_explicit_gcs_destination(self) -> None:
        push = load_script("push_to_gcs.py")
        push.DEST = ""
        with self.assertRaisesRegex(RuntimeError, "explicit bucket and prefix"):
            push.ensure_destination()

        push.DEST = "gs://public-status-test"
        with self.assertRaisesRegex(RuntimeError, "explicit bucket and prefix"):
            push.ensure_destination()

        push.DEST = "gs://public-status-test/site"
        push.ensure_destination()

    def test_build_runs_both_collectors_and_copies_required_snapshots(self) -> None:
        build = load_script("build_public.py")
        with tempfile.TemporaryDirectory() as temporary_dir:
            temporary = Path(temporary_dir)
            status = temporary / "status"
            public = temporary / "public"
            status.mkdir()
            (status / "stream-v3-prometheus.json").write_text("{}\n", encoding="utf-8")
            (status / "stream-v3-loki.json").write_text("{}\n", encoding="utf-8")
            (status / "stream-v3-prometheus.html").write_text("prom\n", encoding="utf-8")
            (status / "stream-v3-loki.html").write_text("loki\n", encoding="utf-8")
            build.STATUS_DIR = status
            build.PUBLIC_DIR = public

            with (
                mock.patch.object(build, "run_collect") as collect,
                mock.patch.object(build, "run_reliability_collect") as reliability,
            ):
                self.assertEqual(build.main(), 0)

            collect.assert_called_once_with()
            reliability.assert_called_once_with()
            self.assertEqual((public / "stream-v3-prometheus.json").read_text(), "{}\n")
            self.assertEqual((public / "stream-v3-loki.json").read_text(), "{}\n")
            self.assertEqual((public / "stream-v3-prometheus.html").read_text(), "prom\n")
            self.assertEqual((public / "stream-v3-loki.html").read_text(), "loki\n")

    def test_build_forces_one_reliability_evaluation_per_run(self) -> None:
        build = load_script("build_public.py")
        collector = build.SITE_DIR / "scripts" / "collect_reliability.py"
        with mock.patch.object(build.subprocess, "run") as run:
            build.run_reliability_collect()

        run.assert_called_once_with(
            [build.sys.executable, str(collector), "--force"],
            check=True,
        )

    def test_push_builds_then_syncs_and_sets_cache_control(self) -> None:
        push = load_script("push_to_gcs.py")
        push.DEST = "gs://public-status-test/site"
        with tempfile.TemporaryDirectory() as temporary_dir:
            status_file = Path(temporary_dir) / "publication-status.json"
            with (
                mock.patch.dict(
                    push.os.environ,
                    {"YUKIMURATA_PUBLISH_STATUS_FILE": str(status_file)},
                ),
                mock.patch.object(push, "ensure_gcloud") as ensure_gcloud,
                mock.patch.object(push, "run") as run,
                mock.patch.object(
                    push,
                    "stage_public_tree",
                    return_value={"index.html": "0" * 64},
                ) as stage_public_tree,
            ):
                self.assertEqual(push.main(), 0)

            status = push.read_status(status_file)

        ensure_gcloud.assert_called_once_with()
        stage_public_tree.assert_called_once()
        commands = [call.args[0] for call in run.call_args_list]
        self.assertEqual(commands[0][0], push.sys.executable)
        self.assertEqual(commands[1][:3], ["gcloud", "storage", "rsync"])
        self.assertEqual(commands[2][:3], ["gcloud", "storage", "rsync"])
        self.assertTrue(any("json" in argument for argument in commands[1]))
        self.assertTrue(any("json" in argument for argument in commands[2]))
        self.assertEqual(status["status"], "SUCCEEDED")
        self.assertFalse(status["public_mirror_verified"])

    def test_systemd_source_keeps_host_values_in_external_environment_file(self) -> None:
        unit = (
            PUBLISHER / "site" / "systemd" / "yukimurata-public-push.service"
        ).read_text(encoding="utf-8")
        example = (
            PUBLISHER / "site" / "systemd" / "stream-v3-public-publisher.env.example"
        ).read_text(encoding="utf-8")

        self.assertIn("EnvironmentFile=-/etc/default/stream-v3-public-publisher", unit)
        self.assertNotIn("192.168.", unit)
        self.assertNotIn("gs://addevlab", unit)
        self.assertIn("STREAM_V3_RELIABILITY_SSH_TARGET=stream-monitor", example)
        self.assertIn("STREAM_V3_RELIABILITY_SOURCE_DIR=/opt/stream_v3", example)
        self.assertNotIn("/home/", unit + example)
        self.assertNotIn("192.168.", example)

    def test_public_source_tree_contains_no_private_address_or_generated_data(self) -> None:
        checked = (
            PUBLISHER / "site" / "public" / "index.html",
            PUBLISHER / "site" / "public" / "assets" / "site.js",
            PUBLISHER / "site" / "public" / "assets" / "site.css",
            PUBLISHER / "site" / "systemd" / "yukimurata-public-push.service",
            PUBLISHER / "site" / "systemd" / "stream-v3-public-publisher.env.example",
        )
        combined = "\n".join(path.read_text(encoding="utf-8") for path in checked)
        self.assertNotIn("192.168.", combined)
        self.assertNotIn("PRIVATE KEY", combined)
        self.assertNotIn("refresh_token", combined.lower())


if __name__ == "__main__":
    unittest.main()
