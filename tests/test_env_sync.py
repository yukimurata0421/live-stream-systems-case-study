from __future__ import annotations

import importlib.util
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SYNC_SCRIPT = ROOT / "ops" / "scripts" / "sync_stream_env_to_v2.py"


def load_sync_module():
    spec = importlib.util.spec_from_file_location("sync_stream_env_to_v2", SYNC_SCRIPT)
    if spec is None or spec.loader is None:
        raise RuntimeError("failed to load sync_stream_env_to_v2.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class EnvSyncTests(unittest.TestCase):
    def test_sync_rewrites_project_and_state_paths_without_printing_secrets(self) -> None:
        sync = load_sync_module()
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            source_dir = root / "source"
            target_dir = root / "target"
            source_dir.mkdir()
            (source_dir / "adsb-streamnew").write_text(
                "\n".join(
                    [
                        "BASE_DIR=/home/yuki/projects/stream",
                        "STREAM_KEY=REDACTED_TEST_STREAM_KEY",
                        "RUNTIME_STATE_FILE=/home/yuki/.local/state/adsb-streamnew/stream_runtime_state.json",
                        "AUDIO_BITRATE=192k",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            result = sync.sync_one("adsb-streamnew", source_dir, target_dir)
            text = (target_dir / "adsb-streamnew.env").read_text(encoding="utf-8")

        self.assertEqual(result["status"], "written")
        state_name = "adsb-streamnew-v3" if ROOT.name == "stream_v3" else "adsb-streamnew-v2"
        self.assertIn(f"BASE_DIR={ROOT}", text)
        self.assertIn(f"STREAM_BASE_DIR={ROOT}", text)
        self.assertIn(f"RUNTIME_STATE_FILE={ROOT}/.state/{state_name}/stream_runtime_state.json", text)
        self.assertIn("STREAM_KEY=REDACTED_TEST_STREAM_KEY", text)
        self.assertIn("STREAM_KEY", result["secret_keys_redacted"])
        self.assertNotIn("REDACTED_TEST_STREAM_KEY", str(result))

    def test_stream_v3_systemd_templates_use_synced_env_snapshots(self) -> None:
        services = sorted((ROOT / "ops" / "systemd").glob("adsb-streamnew*.service"))
        self.assertGreater(len(services), 0)
        for path in services:
            text = path.read_text(encoding="utf-8")
            with self.subTest(path=path.name):
                self.assertNotIn("EnvironmentFile=/etc/default/adsb-streamnew", text)
                self.assertNotIn("EnvironmentFile=-/etc/default/adsb-streamnew", text)
                if "EnvironmentFile=" in text:
                    self.assertIn("/home/yuki/projects/stream_v2/.state/env/", text)

    def test_program_map_shadow_systemd_entrypoints_exist(self) -> None:
        expected = (
            "adsb-streamnew-subsystems-status.service",
            "adsb-streamnew-subsystems-status.timer",
            "adsb-streamnew-recovery-orchestrator.service",
            "adsb-streamnew-recovery-orchestrator.timer",
            "adsb-streamnew-memory-status.service",
            "adsb-streamnew-memory-status.timer",
            "adsb-streamnew-resource-memory.service",
            "adsb-streamnew-resource-memory.timer",
        )
        for name in expected:
            with self.subTest(name=name):
                self.assertTrue((ROOT / "ops" / "systemd" / name).exists())


if __name__ == "__main__":
    unittest.main()
