from __future__ import annotations

import json
from pathlib import Path

from cra_harness.upstream_surface import CONTRACT_PATH, audit_upstream

ROOT = Path(__file__).resolve().parents[3]


def _fixture(tmp_path: Path) -> tuple[Path, Path]:
    cra = tmp_path / "cra"
    producer = tmp_path / "stream_v3"
    contract = json.loads((ROOT / CONTRACT_PATH).read_text(encoding="utf-8"))
    contract_path = cra / CONTRACT_PATH
    contract_path.parent.mkdir(parents=True)
    contract_path.write_text(json.dumps(contract), encoding="utf-8")
    source = producer / contract["source_file"]
    source.parent.mkdir(parents=True)
    source.write_text(
        "OAUTH_PROBE_TRANSIENT_HTTP_CODES = frozenset({500, 502, 503, 504})\n"
        "OAUTH_PROBE_MAX_ATTEMPTS = 2\n"
        "def youtube_live_api_get(endpoint, token, params):\n"
        "    return live_api_runtime.api_get(endpoint, token, params)\n"
        "def _probe_oauth_read_with_transient_retry(operation):\n"
        "    return operation()\n"
        "def _list_bound_streams_with_transient_retry(token, bound_stream_id):\n"
        "    return _probe_oauth_read_with_transient_retry(lambda: youtube_live_api_get('liveStreams', token, {}))\n"
        "def list_owned_broadcasts(token):\n"
        "    return youtube_live_api_get('liveBroadcasts', token, {})\n"
        "def update_video_category_from_existing_snippet(token):\n"
        "    return youtube_live_api_get('videos', token, {})\n"
        "def probe_with_oauth():\n"
        "    broadcasts = _probe_oauth_read_with_transient_retry(lambda: list_owned_broadcasts('token'))\n"
        "    return _list_bound_streams_with_transient_retry('token', 'id'), broadcasts\n",
        encoding="utf-8",
    )
    test_path = producer / "tests/test_youtube_api_quota.py"
    test_path.parent.mkdir(parents=True)
    methods = "\n".join(f"    def {row['nodeid'].split('::')[-1]}(self): pass" for row in contract["required_tests"])
    test_path.write_text("class YouTubeApiQuotaResetTests:\n" + methods + "\n", encoding="utf-8")
    return cra, producer


def test_local_static_audit_never_claims_test_execution(tmp_path: Path) -> None:
    cra, producer = _fixture(tmp_path)
    result = audit_upstream(cra, producer)
    assert result["status"] == "HOLD"
    assert result["issues"] == ["UPSTREAM_TEST_EXECUTION_REQUIRED"]
    assert result["required_test_count"] == 9
    assert len(result["discovered_callsites"]) == 3


def test_new_endpoint_and_probe_retry_leak_fail_closed(tmp_path: Path) -> None:
    cra, producer = _fixture(tmp_path)
    path = producer / "src/watchers/youtube_api.py"
    path.write_text(path.read_text(encoding="utf-8") + "\ndef new_probe():\n    return youtube_live_api_get('newEndpoint', 'token', {})\n", encoding="utf-8")
    assert "UPSTREAM_CALLSITE_SET_DRIFT" in audit_upstream(cra, producer)["issues"]
    path.write_text(path.read_text(encoding="utf-8") + "\ndef action():\n    return _probe_oauth_read_with_transient_retry(lambda: 1)\n", encoding="utf-8")
    assert "UPSTREAM_RETRY_LEAKED_OUTSIDE_PROBE:action" in audit_upstream(cra, producer)["issues"]


def test_removed_test_or_changed_retry_contract_cannot_pass(tmp_path: Path) -> None:
    cra, producer = _fixture(tmp_path)
    source = producer / "src/watchers/youtube_api.py"
    source.write_text(source.read_text(encoding="utf-8").replace("OAUTH_PROBE_MAX_ATTEMPTS = 2", "OAUTH_PROBE_MAX_ATTEMPTS = 3"), encoding="utf-8")
    assert "UPSTREAM_RETRY_BOUND_DRIFT" in audit_upstream(cra, producer)["issues"]
    test = producer / "tests/test_youtube_api_quota.py"
    test.write_text(test.read_text(encoding="utf-8").replace("def test_probe_with_oauth_retries_transient_live_streams_errors", "def removed_stream_test"), encoding="utf-8")
    assert any(issue.startswith("UPSTREAM_TEST_NODE_MISSING:") for issue in audit_upstream(cra, producer)["issues"])
