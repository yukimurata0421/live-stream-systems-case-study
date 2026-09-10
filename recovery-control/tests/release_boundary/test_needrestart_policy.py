from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
POLICIES = ROOT / "ops/needrestart"


def test_needrestart_policies_defer_only_scoped_long_lived_units() -> None:
    cra = (POLICIES / "cra-01-stream-recovery-control.conf").read_text(encoding="utf-8")
    arena = (POLICIES / "arena-server-stream-recovery-control.conf").read_text(encoding="utf-8")
    dell = (POLICIES / "dell-stream-recovery-control.conf").read_text(encoding="utf-8")

    assert "blacklist_rc" not in cra + arena + dell
    assert "restart} = 'l'" not in cra + arena + dell
    assert "cra-runtime-no-action@.+\\.service" in cra
    assert "monitoring-v4-cra-projection-server@.+\\.service" in arena
    assert "stream-v3-arena-monitor\\.service" in arena
    assert "stream-v3-persistent-anchor-observer\\.service" in arena
    assert "dell-observation-server@.+\\.service" in dell
    assert "dell-recovery-agent-shadow\\.service" in dell
    assert all(" = 0;" in policy for policy in (cra, arena, dell))
