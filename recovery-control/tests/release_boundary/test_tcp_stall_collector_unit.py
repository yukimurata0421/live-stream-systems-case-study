from __future__ import annotations

import configparser
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_tcp_stall_collector_has_no_control_or_network_authority() -> None:
    text = (ROOT / "ops/systemd/stream-v3-tcp-stall-collector@.service").read_text(encoding="utf-8")
    unit = configparser.ConfigParser(interpolation=None, strict=False)
    unit.read_string(text)
    service = unit["Service"]
    assert service["User"] == service["Group"] == "stream-recovery"
    assert service["PrivateNetwork"] == "true"
    assert service["ProtectSystem"] == "strict"
    assert service["CapabilityBoundingSet"] == ""
    assert service["RestrictAddressFamilies"] == "AF_UNIX"
    assert service["ReadWritePaths"] == "/var/lib/stream-recovery-control/tcp-stall/epochs/%i"
    assert service["ReadOnlyPaths"] == "-/run/stream-v3-control/runtime-observation.json"
    assert service["SystemCallFilter"] == "~@network-io @mount @reboot @swap"
    assert "BindReadOnlyPaths=/etc/stream-recovery-control/tcp-stall/releases/%i/collector.json:" in text
    assert "InaccessiblePaths=-/etc/stream-recovery-control" in text
    assert "effect.sock" in text and "runtime-recovery.sock" in text
    assert "/var/lib/stream-recovery-control/controller" in text
    assert "/var/lib/stream-recovery-control/cra" in text
    assert "/var/lib/rancher/k3s" in text
    assert "Wants=" not in text and "Requires=" not in text
    assert "ExecStartPre=" not in text and "ExecStartPost=" not in text
    assert "restart_threshold" not in text
