from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import timedelta
from email.message import Message
from pathlib import Path
from typing import Any

import pytest

import cra_authority.monitoring_evidence as evidence_module
from cra_authority.monitoring_evidence import MonitoringEvidenceContract
from cra_authority.projection_pull import ProjectionPuller, _read_json
from cra_authority.retention import load_archive_private_key
from cra_authority.runtime import CraNoActionRuntime, RuntimeConfig
from cra_dell_recovery.canonical import KeyRing
from cra_dell_recovery.time import parse_utc
from tests.authority.test_cra_no_action_runtime import _write_runtime_fixture

ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.parametrize(
    "raw", [b'{"ready":false,"ready":true}', b'{"x":NaN}', b'{"x":1e999}', b'{"x":' + b"[" * 40 + b"0" + b"]" * 40 + b"}"]
)
def test_projection_json_rejects_ambiguous_or_unbounded_input(tmp_path: Path, raw: bytes) -> None:
    path = tmp_path / "input.json"
    path.write_bytes(raw)
    path.chmod(0o600)
    with pytest.raises(ValueError, match="JSON_INPUT_"):
        _read_json(path, maximum_bytes=4096)


@pytest.mark.parametrize("bound", [True, float("nan"), float("inf")])
@pytest.mark.parametrize(
    "field", ["maximum_ttl_seconds", "maximum_observation_age_seconds", "maximum_check_age_seconds", "maximum_future_skew_seconds"]
)
def test_all_contract_time_bounds_are_finite_numbers(field: str, bound: float) -> None:
    with pytest.raises(ValueError, match="MONITORING_EVIDENCE_TIME_BOUND_INVALID"):
        MonitoringEvidenceContract(
            ROOT / "contracts/monitoring_v4/evidence_projection.v1.schema.json",
            KeyRing({}),
            allowed_sources={},
            **{field: bound},
        )


@pytest.mark.parametrize("entrypoint", ["pull", "runtime", "inbox"])
def test_fifo_input_is_rejected_without_waiting_for_writer(tmp_path: Path, entrypoint: str) -> None:
    fifo = tmp_path / "input.fifo"
    os.mkfifo(fifo, 0o600)
    code = {
        "pull": "from cra_authority.projection_pull import _read_json; _read_json(p, maximum_bytes=4096)",
        "runtime": "from cra_authority.runtime import _secure_read; _secure_read(p, maximum_bytes=4096)",
        "inbox": "from cra_authority.monitoring_evidence import SignedProjectionFileSource; SignedProjectionFileSource(p, None).latest()",
    }[entrypoint]
    try:
        result = subprocess.run(
            [sys.executable, "-c", "from pathlib import Path; import sys; p=Path(sys.argv[1]); " + code, str(fifo)],
            env={**os.environ, "PYTHONPATH": str(Path(evidence_module.__file__).resolve().parents[1])},
            capture_output=True,
            text=True,
            timeout=2,
        )
    except subprocess.TimeoutExpired:
        pytest.fail("regular-file validation blocked while opening FIFO")
    assert result.returncode != 0
    assert "NOT_REGULAR_FILE" in result.stderr


def test_archive_key_fifo_is_rejected_without_waiting_for_writer(tmp_path: Path) -> None:
    fifo = tmp_path / "key.fifo"
    os.mkfifo(fifo, 0o600)
    try:
        load_archive_private_key(fifo)
    except ValueError as error:
        assert str(error) == "CRA_ARCHIVE_KEY_NOT_REGULAR_FILE"
    else:
        pytest.fail("archive key FIFO was accepted")


class Response:
    def __init__(self, body: bytes = b"{}", **headers: str) -> None:
        self.body = body
        self.headers = Message()
        self.headers["Content-Type"] = "application/json"
        for name, value in headers.items():
            self.headers[name] = value
        self.closed = False

    def __enter__(self) -> Response:
        return self

    def __exit__(self, *args: object) -> None:
        self.closed = True

    def read(self, size: int) -> bytes:
        body, self.body = self.body[:size], self.body[size:]
        return body

    def read1(self, size: int) -> bytes:
        return self.read(size)


@pytest.mark.parametrize("length", ["-1", "+2", "3", "1", "2, 2"])
def test_invalid_http_length_is_not_accepted_or_retried(tmp_path: Path, length: str) -> None:
    import ssl
    import urllib.request

    response = Response(**{"Content-Length": length})
    attempts = []

    def open_url(*args: Any, **kwargs: Any) -> Response:
        attempts.append(1)
        return response

    puller = ProjectionPuller(
        endpoint_url="https://test.invalid",
        ssl_context=ssl.create_default_context(),
        contract=object(),
        projection_file=tmp_path / "inbox",
        open_url=open_url,
    )  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="MONITORING_PROJECTION_CONTENT_LENGTH_"):
        puller._fetch_body(urllib.request.Request(puller.endpoint_url))
    assert len(attempts) == 1
    assert response.closed


@pytest.mark.parametrize("replay", [False, True])
@pytest.mark.parametrize("offset", [-10, 46])
def test_runtime_rechecks_time_after_database_work(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    replay: bool,
    offset: int,
) -> None:
    config = RuntimeConfig.load(_write_runtime_fixture(tmp_path))
    runtime = CraNoActionRuntime(config)
    original = runtime.authorizer.evaluate
    value = json.loads(Path(config.value["monitoring"]["projection_file"]).read_text())
    try:
        if replay:
            assert runtime.run_once()["policy_decision"] == "WOULD_AUTHORIZE"

        def delayed(*args: Any, **kwargs: Any) -> Any:
            result = original(*args, **kwargs)
            monkeypatch.setattr("cra_authority.runtime.utc_now", lambda: parse_utc(value["issued_at"]) + timedelta(seconds=offset))
            return result

        monkeypatch.setattr(runtime.authorizer, "evaluate", delayed)
        status = runtime.run_once()
        assert status["readiness"] == "SAFE_BLOCKED"
        assert status["policy_decision"] == "NO_ACTION"
        assert status["physical_effect_count"] == 0
        assert runtime.store.read_one("SELECT count(*) FROM recovery_authorizations")[0] == 0
    finally:
        runtime.close()
