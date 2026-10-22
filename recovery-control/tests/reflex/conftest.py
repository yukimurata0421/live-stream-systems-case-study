from __future__ import annotations

from pathlib import Path

import pytest

from tests.reflex.support import Harness


@pytest.fixture
def harness(tmp_path: Path) -> Harness:
    return Harness(tmp_path)


@pytest.fixture(autouse=True)
def immediate_builtin_policy(monkeypatch: pytest.MonkeyPatch) -> None:
    """Most tests exercise other invariants right after confirmation; grace has its own tests."""
    import dataclasses

    import cra.agent
    from cra.policy import BUILTIN

    monkeypatch.setattr(cra.agent, "BUILTIN", dataclasses.replace(BUILTIN, stall_grace_sec=0))
