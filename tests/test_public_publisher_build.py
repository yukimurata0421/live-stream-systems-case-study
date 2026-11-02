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
        for destination in ("", "gs://public-status-test", "gs://public-status-test/site/*"):
            push.DEST = destination
            with self.assertRaisesRegex(RuntimeError, "explicit bucket and prefix"):
                push.ensure_destination()

        push.DEST = "gs://public-status-test/site"
        push.ensure_destination()

    def test_build_fetches_owner_bundle_and_does_not_copy_legacy_json(self) -> None:
        build = load_script("build_public.py")
        with tempfile.TemporaryDirectory() as temporary_dir:
            status = Path(temporary_dir) / "status"
            public = Path(temporary_dir) / "public"
            status.mkdir()
            (status / "stream-v3-prometheus.json").write_text("legacy")
            (status / "stream-v3-prometheus.html").write_text("prom")
            build.STATUS_DIR, build.PUBLIC_DIR = status, public
            with mock.patch.object(build.subprocess, "run") as run:
                self.assertEqual(build.main(), 0)
            run.assert_called_once_with([build.sys.executable, str(build.SITE_DIR/'scripts/collect_observation_bundle.py')], check=True)
            self.assertFalse((public/'stream-v3-prometheus.json').exists())
            self.assertEqual((public/'stream-v3-prometheus.html').read_text(), 'prom')

    def test_push_builds_then_syncs_and_sets_cache_control(self) -> None:
        push = load_script("push_to_gcs.py")
        push.DEST = "gs://public-status-test/site"
        with (
            tempfile.TemporaryDirectory() as directory,
            mock.patch.dict(push.os.environ, {"YUKIMURATA_PUBLISH_STATUS_FILE": str(Path(directory) / 'status.json')}),
            mock.patch.object(push, "ensure_gcloud") as ensure_gcloud,
            mock.patch.object(push, "stage_public_tree", return_value={"index.html": "verified-digest"}),
            mock.patch.object(push, "run") as run,
        ):
            self.assertEqual(push.main(), 0)

        ensure_gcloud.assert_called_once_with()
        commands = [call.args[0] for call in run.call_args_list]
        self.assertEqual(commands[0][0], push.sys.executable)
        self.assertEqual(commands[1][:3], ["gcloud", "storage", "rsync"])
        self.assertEqual(len(commands), 3)
        self.assertIn('--cache-control=public, max-age=300', commands[1])
        self.assertIn('--cache-control=public, max-age=60', commands[2])
        self.assertEqual(commands[1][4], push.DEST)
        self.assertEqual(commands[2][4], push.DEST)

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

    def test_active_incidents_are_rendered_outside_the_recovery_loop(self) -> None:
        site_js = (
            PUBLISHER / "site" / "public" / "assets" / "site.js"
        ).read_text(encoding="utf-8")
        collector = (
            PUBLISHER / "status" / "collect_stream_v3_public.py"
        ).read_text(encoding="utf-8")

        self.assertIn('title.textContent = "Current active incidents"', site_js)
        self.assertIn("makeActiveIncidentsCard", site_js)
        self.assertIn('label": "Notification backlog"', collector)
        notify_query = collector.split('"id": "notify_issues"', 1)[1].split("},", 1)[0]
        self.assertNotIn("notify_active_incidents", notify_query)


if __name__ == "__main__":
    unittest.main()
