from __future__ import annotations

from pathlib import Path

import pytest

from tests.reflex.support import Harness


@pytest.fixture
def harness(tmp_path: Path) -> Harness:
    return Harness(tmp_path)
