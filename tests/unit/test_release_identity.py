from __future__ import annotations

import pytest

from cra_dell_recovery.release_identity import RELEASE_ID_ENVIRONMENT, require_runtime_release


def test_release_identity_is_optional_for_local_tools_but_exact_when_systemd_sets_it(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(RELEASE_ID_ENVIRONMENT, raising=False)
    require_runtime_release("local-release", "RELEASE_MISMATCH")

    monkeypatch.setenv(RELEASE_ID_ENVIRONMENT, "immutable-release-a")
    require_runtime_release("immutable-release-a", "RELEASE_MISMATCH")
    with pytest.raises(ValueError, match="RELEASE_MISMATCH"):
        require_runtime_release("immutable-release-b", "RELEASE_MISMATCH")

    monkeypatch.setenv(RELEASE_ID_ENVIRONMENT, "")
    with pytest.raises(ValueError, match="RELEASE_MISMATCH"):
        require_runtime_release("immutable-release-a", "RELEASE_MISMATCH")
