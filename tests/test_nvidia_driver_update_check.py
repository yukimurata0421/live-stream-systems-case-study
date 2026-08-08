from __future__ import annotations

import importlib.util
import io
import json
import os
import tempfile
import unittest
from argparse import Namespace
from contextlib import redirect_stdout
from pathlib import Path
from unittest import mock


MODULE_PATH = (
    Path(__file__).resolve().parents[1]
    / "ops"
    / "host-maintenance"
    / "bin"
    / "nvidia_driver_update_check.py"
)
SPEC = importlib.util.spec_from_file_location("nvidia_driver_update_check", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
checker = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(checker)


def observation(reasons: list[str]) -> dict:
    return {
        "checked_at_utc": "2026-07-31T00:00:00Z",
        "supported_branch": "580",
        "packages": {
            package: {
                "installed": "580.1",
                "candidate": "580.1",
                "update_available": False,
            }
            for package in (*checker.TRACKED_PACKAGES, *checker.FUTURE_BRANCH_PACKAGES)
        },
        "recommended_driver": "nvidia-driver-580",
        "runtime": {
            "gpus": [{"name": "GTX 1070", "driver_version": "580.1"}],
            "module_version": "580.1",
            "runtime_module_mismatch": False,
        },
        "dkms": {"kernel": "test", "nvidia_installed": True},
        "apt_metadata": {
            "newest_utc": "2026-07-31T00:00:00Z",
            "age_hours": 1.0,
            "stale": False,
        },
        "maintenance_guard": {"path": "/test", "active": True},
        "held_packages": [],
        "collection_issues": [],
        "reasons": reasons,
    }


class NvidiaDriverUpdateCheckTest(unittest.TestCase):
    def test_systemd_contract_is_read_only_and_uses_portable_identity(self) -> None:
        service = (
            Path(__file__).resolve().parents[1]
            / "ops"
            / "host-maintenance"
            / "systemd"
            / "stream-v3-nvidia-driver-check.service"
        ).read_text(encoding="utf-8")

        self.assertIn("User=stream-v3", service)
        self.assertIn("ExecStart=/usr/local/libexec/stream-v3-nvidia-driver-check", service)
        self.assertNotIn("apt upgrade", service)
        self.assertNotIn("/home/yuki/", service)

    def test_first_run_records_baseline_without_notification(self) -> None:
        current = observation(["r580_update_available"])
        self.assertIsNone(
            checker.decide_notification(None, checker.fingerprint(current), current)
        )

    def test_change_to_update_available_alerts(self) -> None:
        old = observation([])
        current = observation(["r580_update_available"])
        current["packages"]["nvidia-driver-580"]["candidate"] = "580.2"
        current["packages"]["nvidia-driver-580"]["update_available"] = True
        previous = {
            "fingerprint": checker.fingerprint(old),
            "observation": old,
        }
        self.assertEqual(
            checker.decide_notification(
                previous, checker.fingerprint(current), current
            ),
            "alert",
        )

    def test_resolution_notifies(self) -> None:
        old = observation(["apt_metadata_stale"])
        current = observation([])
        previous = {
            "fingerprint": checker.fingerprint(old),
            "observation": old,
        }
        self.assertEqual(
            checker.decide_notification(
                previous, checker.fingerprint(current), current
            ),
            "resolved",
        )

    def test_failed_notification_is_retried_while_state_is_unchanged(self) -> None:
        current = observation(["r580_update_available"])
        current_fingerprint = checker.fingerprint(current)
        previous = {
            "fingerprint": current_fingerprint,
            "observation": current,
            "pending_notification": {
                "fingerprint": current_fingerprint,
                "kind": "alert",
            },
        }
        self.assertEqual(
            checker.decide_notification(
                previous, current_fingerprint, current
            ),
            "alert",
        )

    def test_future_branch_candidate_alone_is_not_an_alert(self) -> None:
        current = observation([])
        current["packages"]["nvidia-driver-590"]["candidate"] = "595.84"
        self.assertEqual(current["reasons"], [])
        self.assertIn("595.84", checker.render_notification("resolved", current))

    def test_notification_delivery_is_persisted_without_exposing_webhook(self) -> None:
        current = observation(["r580_update_available"])
        with tempfile.TemporaryDirectory() as temporary:
            state_file = Path(temporary) / "state.json"
            args = Namespace(
                state_file=state_file,
                dry_run=False,
                notify_baseline=True,
            )
            with (
                mock.patch.object(checker, "collect_observation", return_value=current),
                mock.patch.object(
                    checker, "send_discord", return_value=(True, "http_204")
                ) as sender,
                mock.patch.dict(
                    os.environ,
                    {
                        "NVIDIA_UPDATE_NOTIFY_ENABLED": "1",
                        "NVIDIA_UPDATE_DISCORD_WEBHOOK_URL": "https://example.invalid/secret",
                    },
                    clear=True,
                ),
                redirect_stdout(io.StringIO()),
            ):
                self.assertEqual(checker.execute(args), 0)
            sender.assert_called_once()
            payload = json.loads(state_file.read_text(encoding="utf-8"))
            self.assertNotIn("pending_notification", payload)
            self.assertEqual(payload["last_notification"]["kind"], "alert")
            self.assertNotIn("example.invalid", state_file.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
