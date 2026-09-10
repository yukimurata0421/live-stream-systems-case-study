from __future__ import annotations

import configparser
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.parametrize(
    ("name", "flag", "interval", "memory", "timeout"),
    [
        ("cra-recovery-soak", "", "15s", "128M", "10s"),
        ("cra-recovery-soak-gate", " --gate --operator-report", "1h", "256M", "1800s"),
        ("cra-recovery-soak-watchdog", " --watchdog --operator-report", "5min", "128M", "15s"),
    ],
)
def test_recovery_units_are_independent_local_only_candidates(name: str, flag: str, interval: str, memory: str, timeout: str) -> None:
    service_text = (ROOT / "ops/systemd" / f"{name}@.service").read_text()
    timer_text = (ROOT / "ops/systemd" / f"{name}@.timer").read_text()
    service = configparser.ConfigParser(interpolation=None, strict=False)
    service.read_string(service_text)
    values = service["Service"]
    assert values["ExecStart"] == (
        "/opt/stream-recovery-control/runtimes/%i/.venv/bin/python -m cra_no_action_soak.recovery_soak "
        "--config /etc/stream-recovery-control/releases/%i/recovery-soak.json" + flag
    )
    assert values["User"] == values["Group"] == "stream-recovery"
    assert values["ReadWritePaths"] == "/var/lib/stream-recovery-control/recovery-soak/%i"
    assert values["MemoryMax"] == memory
    assert values["TimeoutStartSec"] == timeout
    assert values["PrivateNetwork"] == "true"
    assert values["ProtectSystem"] == "strict"
    assert values["RestrictAddressFamilies"] == "AF_UNIX"
    assert values["CapabilityBoundingSet"] == ""
    assert "InaccessiblePaths=-/var/lib/stream-recovery-control/cra\n" in service_text
    assert "-/run/stream-v3-control" in service_text
    assert "-/run/credentials" in service_text
    assert "resilient_soak" not in service_text
    assert not any(line.startswith(("Wants=", "Requires=", "ExecStartPre=", "ExecStartPost=")) for line in service_text.splitlines())
    timer = configparser.ConfigParser(interpolation=None)
    timer.read_string(timer_text)
    assert timer["Timer"]["Unit"] == name + "@%i.service"
    assert timer["Timer"]["OnUnitInactiveSec"] == interval
    assert timer["Timer"]["Persistent"] == "false"
    assert timer["Timer"]["RandomizedDelaySec"] == "0"


def test_release_builder_requires_all_new_units_and_example() -> None:
    from tools.build_cra_no_action_release import ALLOWED_FILES, RECOVERY_SOAK_FILES, REQUIRED_FILES

    assert len(RECOVERY_SOAK_FILES) == 7
    assert RECOVERY_SOAK_FILES <= ALLOWED_FILES
    assert RECOVERY_SOAK_FILES <= REQUIRED_FILES
    assert all((ROOT / path).is_file() for path in RECOVERY_SOAK_FILES)
