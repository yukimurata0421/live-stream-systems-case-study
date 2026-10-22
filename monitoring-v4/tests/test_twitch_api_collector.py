from pathlib import Path

import pytest

from stream_monitoring_v4.collectors.twitch_api import (
    NoRedirect, ProbeError, STREAMS_URL, TOKEN_URL, USERS_URL, VALIDATE_URL,
    TwitchCollector, retry_delay, stream_facts, metrics, next_poll_deadline, request_json,
)

TARGET = "100000001"
LOGIN = "fixturechannel"
NOW = 1791845400.0


def row(**changes):
    return dict(id="900000001", user_id=TARGET, user_login=LOGIN, type="live",
                started_at="2026-10-12T22:00:00Z", viewer_count=2, **changes)


class Transport:
    def __init__(self):
        self.calls = []
        self.error = None
        self.streams = {"data": [row()], "pagination": {}}
        self.user = {"id": TARGET, "login": LOGIN}
        self.scopes = []

    def __call__(self, url, headers, body, timeout):
        self.calls.append((url, headers, body))
        if url == TOKEN_URL:
            return {"access_token": "dummyToken123", "expires_in": 7200, "token_type": "bearer"}, {}
        if url == VALIDATE_URL:
            return {"client_id": "dummyClient123", "expires_in": 7200, "scopes": self.scopes}, {}
        if url.startswith(USERS_URL):
            return {"data": [self.user]}, {}
        assert url.startswith(STREAMS_URL)
        if self.error:
            raise self.error
        return self.streams, {"Ratelimit-Remaining": "799"}


@pytest.fixture
def setup(tmp_path: Path):
    (tmp_path / "client-id").write_text("dummyClient123")
    (tmp_path / "client-secret").write_text("dummySecret123")
    clock = [NOW, 1000.0]
    transport = Transport()
    collector = TwitchCollector(tmp_path, TARGET, LOGIN, transport=transport,
                                wall=lambda: clock[0], monotonic=lambda: clock[1])
    return collector, transport, clock


def test_live_then_offline_then_api_error_never_reuses_live(setup):
    collector, transport, _ = setup
    first = collector.collect()
    assert first["provider_state"] == "LIVE" and first["api_request_count"] == 4
    assert first["stream_id"] == "900000001"
    assert "dummy" not in str(first)
    transport.streams = {"data": []}
    assert collector.collect()["provider_state"] == "OFFLINE"
    transport.error = ProbeError("network")
    failure = collector.collect()
    assert failure["provider_state"] == "UNKNOWN" and failure["stream_id"] is None
    assert failure["last_success_at"] and failure["probe_status"] == "unknown"


@pytest.mark.parametrize("status,reason", [(401, "auth"), (429, "rate_limited"), (503, "upstream")])
def test_failed_response_is_unknown_and_no_inline_retry(setup, status, reason):
    collector, transport, _ = setup
    collector.collect()
    transport.error = ProbeError(reason, status, 300)
    transport.calls.clear()
    result = collector.collect()
    assert result["provider_state"] == "UNKNOWN" and result["http_status"] == status
    assert result["retry_after_sec"] == 300 and len(transport.calls) == 1
    if status == 401:
        assert collector.token == ""


def test_identity_mismatch_cannot_claim_offline(setup):
    collector, transport, _ = setup
    transport.user["id"] = "999"
    transport.streams = {"data": []}
    result = collector.collect()
    assert result["provider_state"] == "UNKNOWN"
    assert result["error_reason"] == "users_target_mismatch"
    assert not any(c[0].startswith(STREAMS_URL) for c in transport.calls)


def test_hourly_validate_and_renew_after_expiry(setup):
    collector, transport, clock = setup
    collector.collect()
    transport.calls.clear()
    collector.collect()
    assert len(transport.calls) == 1
    clock[1] += 3600
    collector.collect()
    assert sum(c[0] == VALIDATE_URL for c in transport.calls) == 1
    clock[1] += 7200
    collector.collect()
    assert sum(c[0] == TOKEN_URL for c in transport.calls) == 1


def test_scoped_token_rejected(setup):
    collector, transport, _ = setup
    transport.scopes = ["channel:read:stream_key"]
    result = collector.collect()
    assert result["error_reason"] == "validate_app_token_required"
    assert result["provider_state"] == "UNKNOWN" and collector.token == ""


def test_scope_free_app_token_with_null_scopes_is_accepted(setup):
    collector, transport, _ = setup
    transport.scopes = None
    result = collector.collect()
    assert result["probe_status"] == "ok" and result["provider_state"] == "LIVE"
    assert result["api_request_count"] == 4


@pytest.mark.parametrize("scopes", ["", {}, False, 0])
def test_malformed_app_token_scopes_are_rejected(setup, scopes):
    collector, transport, _ = setup
    transport.scopes = scopes
    result = collector.collect()
    assert result["provider_state"] == "UNKNOWN" and collector.token == ""
    assert result["error_reason"] == "validate_app_token_required"


def test_missing_credentials_performs_no_network(tmp_path):
    def forbidden(*args):
        raise AssertionError("network must not be called")
    result = TwitchCollector(tmp_path, TARGET, LOGIN, transport=forbidden).collect()
    assert result["error_reason"] == "credentials_missing"
    assert result["api_request_count"] == 0 and result["provider_state"] == "UNKNOWN"


@pytest.mark.parametrize("change", [{"user_id": "999"}, {"type": ""}, {"id": "secret-url"},
                                    {"started_at": "2026-10-12T22:00:00"}, {"viewer_count": True}])
def test_invalid_or_mismatched_live_response(change):
    value = row()
    value.update(change)
    with pytest.raises(ProbeError):
        stream_facts({"data": [value]}, TARGET, LOGIN, NOW)


def test_exact_user_live_response_with_pagination_cursor_is_valid(setup):
    collector, transport, _ = setup
    transport.streams["pagination"] = {"cursor": "opaque-next-page"}
    result = collector.collect()
    assert result["probe_status"] == "ok" and result["provider_state"] == "LIVE"
    assert sum(c[0].startswith(STREAMS_URL) for c in transport.calls) == 1


@pytest.mark.parametrize("pagination", [None, {"cursor": None}, {"cursor": 1}, {"cursor": "x" * 4097}])
def test_malformed_pagination_is_rejected(pagination):
    with pytest.raises(ProbeError):
        stream_facts({"data": [row()], "pagination": pagination}, TARGET, LOGIN, NOW)


def test_multiple_rows_with_cursor_cannot_claim_unique_target():
    with pytest.raises(ProbeError):
        stream_facts({"data": [row(), row()], "pagination": {"cursor": "next"}}, TARGET, LOGIN, NOW)


def test_rate_limit_wait_is_preserved():
    assert retry_delay({"Retry-After": "120", "Ratelimit-Reset": str(NOW + 1800)}, NOW) == 1800
    assert retry_delay({"Retry-After": "NaN"}, NOW) == 0


def test_client_secret_only_in_post_body_and_never_evidence(setup):
    collector, transport, _ = setup
    result = collector.collect()
    request = transport.calls[0]
    assert request[0] == TOKEN_URL and b"client_secret=dummySecret123" in request[2]  # Fixture only. gitleaks:allow
    assert "dummySecret123" not in str(result)
    assert "dummyToken123" not in str(result)


def test_redirect_closes_response_without_forwarding_credentials():
    from io import BytesIO
    response = BytesIO(b"unused")
    with pytest.raises(ProbeError, match="redirect_rejected"):
        NoRedirect().redirect_request(None, response, 302, "", {}, "https://untrusted.invalid")
    assert response.closed


def test_clock_jump_cannot_claim_live(setup):
    collector, transport, clock = setup
    original = transport.__call__
    def moved_clock(*args):
        result = original(*args)
        if args[0].startswith(STREAMS_URL):
            clock[0] += 30
        return result
    collector.transport = moved_clock
    result = collector.collect()
    assert result["provider_state"] == "UNKNOWN" and result["error_reason"] == "streams_clock_jump"


def test_normal_request_latency_does_not_create_missing_calendar_minutes():
    # A wait-after-completion loop loses minutes with the same 0.4-second latency.
    due = 30.0
    completed = []
    for _ in range(1440):
        finished = due + 0.4
        completed.append(int(finished // 60))
        due = next_poll_deadline(30.0, finished, 60)
    assert completed == list(range(1440))
    assert due == 1440 * 60 + 30


def test_overrun_skips_slots_and_backoff_respects_provider_delay():
    assert next_poll_deadline(30, 155, 60) == 210
    assert next_poll_deadline(30, 35, 60, failures=1) == 90
    assert next_poll_deadline(30, 35, 60, failures=2) == 150
    assert next_poll_deadline(30, 35, 60, failures=3) == 210
    assert next_poll_deadline(30, 35, 60, failures=8) >= 935
    assert next_poll_deadline(30, 35, 60, failures=1, retry_after=1800) >= 1835


def test_isolated_timeouts_do_not_skip_the_following_minute():
    due = 30.0
    minutes = []
    failures = 0
    for minute in range(1440):
        failed = minute in (14, 180, 360, 720, 1080)
        finished = due + (13.7 if failed else .4)
        minutes.append(int(finished // 60))
        failures = failures + 1 if failed else 0
        due = next_poll_deadline(30, finished, 60, failures=failures)
    assert minutes == list(range(1440))
    # Rate limiting still overrides the otherwise immediate next scheduled slot.
    assert next_poll_deadline(30, 43.7, 60, failures=1, retry_after=120) == 210


def test_diagnostics_keep_failures_after_recovery_and_separate_latency_by_stage(setup):
    collector, transport, clock = setup
    original = transport.__call__
    def timed(*args):
        clock[0] += .2
        clock[1] += .2
        return original(*args)
    collector.transport = timed
    first = collector.collect(scheduled_at=clock[1] - .5)
    assert first['schedule_lag_seconds'] == pytest.approx(.5)
    assert first['probe_duration_seconds'] == pytest.approx(.8)
    assert [s['stage'] for s in first['request_stats']] == ['oauth','validate','users','streams']
    transport.error = ProbeError('timeout')
    failed = collector.collect()
    assert failed['error_category'] == 'timeout' and failed['http_status'] == 0
    assert failed['rate_limit_remaining'] is None
    transport.error = None
    recovered = collector.collect()
    assert recovered['probe_counts']['ok'] == 2 and recovered['probe_counts']['timeout'] == 1
    assert first['probe_counts']['timeout'] == 0  # snapshots must not mutate
    text = metrics(recovered)
    assert 'stream_v4_twitch_api_probes_total{result="timeout"} 1' in text
    assert 'stream_v4_twitch_api_rate_limit_remaining 799' in text
    assert 'dummy' not in text and 'dummy' not in str(recovered)
    assert 'stream_v4_twitch_api_requests_total{stage="streams"} 3' in text


def test_http_429_preserves_quota_headers_without_provider_body(setup, monkeypatch):
    from urllib.error import HTTPError
    from io import BytesIO
    import stream_monitoring_v4.collectors.twitch_api as module
    body = BytesIO(b'dummySecret123')
    class Opener:
        def open(self, *args, **kwargs):
            raise HTTPError(STREAMS_URL, 429, 'unused',
                            {'Ratelimit-Remaining':'0','Ratelimit-Limit':'800','Retry-After':'120'}, body)
    monkeypatch.setattr(module, 'build_opener', lambda *args: Opener())
    collector, transport, _ = setup
    collector.collect()
    collector.transport = request_json
    result = collector.collect()
    assert result['http_status'] == 429 and result['error_category'] == 'rate_limited'
    assert result['rate_limit_remaining'] == 0 and result['rate_limit_limit'] == 800
    assert result['retry_after_sec'] == 120 and body.closed
    assert result['probe_counts']['rate_limited'] == 1
    assert 'dummy' not in str(result)


def test_urlerror_wrapped_timeout_is_not_mislabeled_as_network(monkeypatch):
    from urllib.error import URLError
    import stream_monitoring_v4.collectors.twitch_api as module
    class Opener:
        def open(self, *args, **kwargs): raise URLError(TimeoutError())
    monkeypatch.setattr(module, 'build_opener', lambda *args: Opener())
    with pytest.raises(ProbeError) as caught:
        request_json(STREAMS_URL, {}, None, 5)
    assert caught.value.reason == 'timeout'
