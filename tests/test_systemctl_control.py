from __future__ import annotations

import os
import sys
from pathlib import Path
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src" / "watchers"))

import systemctl_control  # type: ignore


class SystemctlPrefixTests(unittest.TestCase):
    def test_non_privileged_never_uses_sudo(self) -> None:
        with mock.patch.dict(os.environ, {"WATCHDOG_SYSTEMCTL_USE_SUDO": "1"}, clear=False):
            with mock.patch("os.geteuid", return_value=1000):
                self.assertEqual(systemctl_control.systemctl_prefix(require_privilege=False), ["systemctl"])

    def test_privileged_auto_non_root_uses_sudo(self) -> None:
        with mock.patch.dict(os.environ, {"WATCHDOG_SYSTEMCTL_USE_SUDO": "auto"}, clear=False):
            with mock.patch("os.geteuid", return_value=1000):
                self.assertEqual(
                    systemctl_control.systemctl_prefix(require_privilege=True),
                    ["sudo", "-n", "systemctl"],
                )

    def test_privileged_auto_root_no_sudo(self) -> None:
        with mock.patch.dict(os.environ, {"WATCHDOG_SYSTEMCTL_USE_SUDO": "auto"}, clear=False):
            with mock.patch("os.geteuid", return_value=0):
                self.assertEqual(systemctl_control.systemctl_prefix(require_privilege=True), ["systemctl"])

    def test_privileged_explicit_never_no_sudo(self) -> None:
        with mock.patch.dict(os.environ, {"WATCHDOG_SYSTEMCTL_USE_SUDO": "0"}, clear=False):
            with mock.patch("os.geteuid", return_value=1000):
                self.assertEqual(systemctl_control.systemctl_prefix(require_privilege=True), ["systemctl"])


if __name__ == "__main__":
    unittest.main()

