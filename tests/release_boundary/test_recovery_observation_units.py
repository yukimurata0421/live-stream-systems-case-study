from __future__ import annotations

import pytest

from tools.build_recovery_soak_units import render


@pytest.mark.parametrize("component", ["dell", "arena", "cra"])
def test_observation_units_separate_io_and_do_not_touch_old_soak(component: str) -> None:
    units = render(component, component + "-recovery-test-1234")
    for name, text in units.items():
        assert "resilient" not in text
        assert "Wants=" not in text and "Requires=" not in text
        assert "ExecStartPre=" not in text and "ExecStartPost=" not in text
        assert "sudo" not in text and "User=root" not in text
        if name.endswith(".service"):
            assert "ProtectSystem=strict" in text and "CapabilityBoundingSet=\n" in text
            assert "MemoryMax=128M" in text and "TimeoutStartSec=12s" in text
            assert "-/var/lib/rancher" in text and "-/run/stream-v3-control" in text
        if "-observer-" in name and name.endswith(".service"):
            assert "PrivateNetwork=true" in text
            assert "RestrictAddressFamilies=AF_UNIX\n" in text
            assert "ProcSubset=pid" not in text  # physical boot identity remains readable
        if name.endswith(".timer"):
            assert "OnUnitActiveSec=10s" in text and "Persistent=false" in text


def test_dell_source_bind_keeps_atomic_replacement_visible() -> None:
    text = next(v for k, v in render("dell", "dell-recovery-test-1234").items() if "-observer-" in k and k.endswith(".service"))
    assert "wan-observer:/run/recovery-inputs/network" in text
    assert "latest.json:" not in text
    assert "ProtectHome=true" in text


@pytest.mark.parametrize("release", ["../../escape", "x\nUser=root", "bad space", "/absolute"])
def test_release_cannot_inject_unit_directives(release: str) -> None:
    with pytest.raises(ValueError):
        render("dell", release)
