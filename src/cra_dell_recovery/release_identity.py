from __future__ import annotations

import os

RELEASE_ID_ENVIRONMENT = "CRA_IMMUTABLE_RELEASE_ID"


def require_runtime_release(configured_release_id: object, error_code: str) -> None:
    """Bind a release-bearing config to the systemd instance when the unit sets it."""

    expected = os.environ.get(RELEASE_ID_ENVIRONMENT)
    if expected is not None and (not expected or str(configured_release_id) != expected):
        raise ValueError(error_code)
