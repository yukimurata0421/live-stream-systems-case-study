"""The public mesh entrypoint accepts its example and rejects peer drift."""
import json
from pathlib import Path
from unittest.mock import Mock

import pytest

from stream_monitoring_v4.compatibility import clock_mesh as mesh


@pytest.mark.parametrize("drifted", [False, True])
def test_entrypoint_validates_the_public_peer_configuration(monkeypatch, tmp_path, drifted):
    example = Path(__file__).resolve().parents[1] / "config/clock-mesh.example.json"
    config = json.loads(example.read_text())
    if drifted:
        config["peers"][0]["ssh_alias"] = "unexpected-peer"
    candidate = tmp_path / "mesh.json"
    candidate.write_text(json.dumps(config))
    monkeypatch.setattr(mesh.sys, "argv", ["clock-mesh", "--config", str(candidate),
                                         "--output", str(tmp_path / "output.json")])
    # Stop before collection; registration calls the stop handler immediately.
    monkeypatch.setattr(mesh.signal, "signal", lambda _signum, handler: handler())
    probe = Mock(side_effect=AssertionError("network must not be contacted"))
    monkeypatch.setattr(mesh, "probe", probe)
    if drifted:
        with pytest.raises(SystemExit, match="fixed_clock_peers_required"):
            mesh.main()
    else:
        mesh.main()
    probe.assert_not_called()
