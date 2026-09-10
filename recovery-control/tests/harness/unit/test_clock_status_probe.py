from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from cra_no_action_soak.clock_status_probe import collect

CURRENT = datetime(2026, 9, 3, 0, 0, tzinfo=UTC)


class Result:
    def __init__(self, stdout: str) -> None:
        self.stdout = stdout


def test_clock_probe_persists_local_tracking_fact(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    outputs = iter(
        [
            "yes\n",
            "5BBD5B70,192.0.2.1,3,1788393931.672996378,0.000773283,-0.001180448,"
            "0.000632233,-7.119,0.000,0.081,0.248126164,0.008886389,1044.0,Normal\n",
        ]
    )
    monkeypatch.setattr(
        "cra_no_action_soak.clock_status_probe.subprocess.run",
        lambda *_args, **_kwargs: Result(next(outputs)),
    )
    output = tmp_path / "tracking.json"

    value = collect(output, now=CURRENT)

    assert value["schema"] == "cra.clock_tracking_fact.v1"
    assert value["ntp_synchronized"] is True
    assert json.loads(output.read_text(encoding="utf-8")) == value
    assert output.stat().st_mode & 0o777 == 0o640
