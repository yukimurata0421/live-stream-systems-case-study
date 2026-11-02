"""Exercise selected-source recovery through the existing incident consumer."""
import copy
import json
from unittest.mock import patch

import pytest

from stream_core.common import upload_prometheus_source as direct
from stream_core.notifications import upload_source, incidents
import test_upload_source_recovery as legacy_tests


def source():
    return {"schema": direct.SCHEMA, **direct.LABELS, "host": "encoder-host",
            "instance": "encoder.internal:9120", "source_hash": "a" * 64, "config_hash": "b" * 64}


def select(root, value):
    directory = root / "upload_measurement"
    directory.mkdir(exist_ok=True)
    path = directory / "prometheus-source.json"
    path.write_text(json.dumps(value))
    return path


@pytest.fixture
def setup():
    case = legacy_tests.UploadSourceTests()
    case.setUp()
    yield case
    case.doCleanups()


def payload(case):
    value = case.prom()
    value["data"]["result"][0]["metric"] = direct.expected_labels(source())
    return value


def test_current_recovery_and_incident_use_selected_source(setup):
    setup.stable(); select(setup.root, source())
    status = setup.status_file(); before = status.read_bytes()
    with patch.object(upload_source, "fetch_prometheus", return_value=payload(setup)) as fetch:
        assert upload_source.current_upload_source(setup.root, setup.now)["status"] == "RECOVERED"
        assert incidents.operational_reliability_incidents(status_file=status, now_ts=setup.now) == []
        assert fetch.call_args.args == (setup.now, source())
    assert status.read_bytes() == before


@pytest.mark.parametrize("field,value", [("schema", "bad"), ("job", "legacy"), ("host", None),
    ("source", "other"), ("scope", "other"), ("instance", 'bad\"} or up'),
    ("source_hash", ""), ("config_hash", "A" * 64)])
def test_invalid_selection_cannot_fall_back_to_legacy(setup, field, value):
    setup.stable(); selected = source(); selected[field] = value; select(setup.root, selected)
    with patch.object(upload_source, "fetch_prometheus", return_value=setup.prom()) as fetch:
        assert upload_source.current_upload_source(setup.root, setup.now)["status"] == "UNKNOWN"
        fetch.assert_not_called()


def test_symlink_and_oversized_selection_are_not_followed(setup):
    setup.stable(); path = select(setup.root, source()); actual = path.with_suffix(".actual")
    path.rename(actual); path.symlink_to(actual)
    for _ in range(2):
        with patch.object(upload_source, "fetch_prometheus") as fetch:
            assert upload_source.current_upload_source(setup.root, setup.now)["status"] == "UNKNOWN"
            fetch.assert_not_called()
        path.unlink(); path.write_text("x" * 2049)


def test_direct_cold_start_missing_stale_wrong_identity_do_not_recover(setup):
    setup.stable(); select(setup.root, source())
    full = payload(setup)
    cases = []
    missing = copy.deepcopy(full); missing["data"]["result"][0]["values"].pop(1); cases.append((missing, "RECOVERING"))
    stale = copy.deepcopy(full); stale["data"]["result"][0]["values"][-1][1] = "181"; cases.append((stale, "FAILED"))
    cases.append((setup.prom(), "UNKNOWN"))
    wrong = copy.deepcopy(full); wrong["data"]["result"][0]["metric"]["scope"] = "player"; cases.append((wrong, "UNKNOWN"))
    for evidence, expected in cases:
        with patch.object(upload_source, "fetch_prometheus", return_value=evidence):
            assert upload_source.current_upload_source(setup.root, setup.now)["status"] == expected


def test_only_explicit_selection_preserves_legacy_without_mixing_series(setup):
    assert direct.selected(setup.root) is None
    (setup.root/'upload_measurement/prometheus-source.json').unlink()
    with pytest.raises(FileNotFoundError):direct.selected(setup.root)
    assert not upload_source.prom_health(payload(setup), setup.now)["available"]
    assert not upload_source.prom_health(setup.prom(), setup.now, source())["available"]


def test_deleted_direct_selection_cannot_be_rescued_by_healthy_legacy_series(setup):
    setup.stable();path=select(setup.root,source());path.unlink()
    with patch.object(upload_source,'fetch_prometheus',return_value=setup.prom()) as fetch:
        result=upload_source.current_upload_source(setup.root,setup.now)
    assert result['status']=='UNKNOWN' and not result['measurement']['available']
    fetch.assert_not_called()


def test_raw_replication_failure_is_separate_from_current_measurement(setup):
    select(setup.root,source());setup.rows([]);report=setup.status_file();before=report.read_bytes()
    with patch.object(upload_source,'fetch_prometheus',return_value=payload(setup)):
        value=upload_source.current_upload_source(setup.root,setup.now)
        found=incidents.operational_reliability_incidents(status_file=report,now_ts=setup.now)
    assert value['measurement']['sustained'] and not value['history']['available']
    assert value['status']=='UNKNOWN'  # Combined raw/Prometheus qualification is unchanged.
    assert [i['id'] for i in found]==['reliability:upload_history_replication']
    assert report.read_bytes()==before


def test_formal_coverage_loss_keeps_its_own_incident(setup):
    setup.stable();select(setup.root,source());report=setup.status_file(['minimum_coverage_not_met'])
    with patch.object(upload_source,'fetch_prometheus',return_value=payload(setup)):
        found=incidents.operational_reliability_incidents(status_file=report,now_ts=setup.now)
    assert [i['id']for i in found]==['reliability:upload_ceiling_historical_coverage']


def test_actual_request_uses_selected_query_and_original_recovery_window(setup):
    from urllib.parse import parse_qs, urlparse
    from io import BytesIO
    with patch.object(upload_source, "build_opener") as opener:
        opener.return_value.open.return_value = BytesIO(json.dumps(payload(setup)).encode())
        upload_source.fetch_prometheus(setup.now, source())
    url = opener.return_value.open.call_args.args[0]
    query = parse_qs(urlparse(url).query)
    assert query == {"query": [direct.query(source())], "start": [str(setup.now - 900)], "end": [str(setup.now)], "step": ["60"]}


@pytest.mark.parametrize("case", ["fresh", "rule_frozen", "scrape_frozen", "down", "wrong_build", "mixed_rule_times"])
def test_real_promql_rejects_stale_and_unbound_evidence(tmp_path, case):
    import os
    import subprocess
    binary = os.environ.get("PROMTOOL")
    if not binary:
        pytest.skip("PROMTOOL is required for native query evaluation")
    labels = direct.expected_labels(source())
    selector = lambda values: "{" + ",".join(k + "=" + json.dumps(v) for k, v in sorted(values.items())) + "}"
    age_values = "10" if case == "rule_frozen" else "10+0x4"
    fresh_values = "1+0x3" if case == "mixed_rule_times" else "1+0x4"
    build_labels = {**labels, "source_hash": source()["source_hash"], "config_hash": source()["config_hash"]}
    if case == "wrong_build": build_labels["source_hash"] = "c" * 64
    fixture = {"evaluation_interval": "15s", "tests": [{"interval": "15s", "input_series": [
        {"series": "stream_v3:observation_age_upper_seconds" + selector(labels), "values": age_values},
        {"series": "stream_v3:observation_fresh" + selector(labels), "values": fresh_values},
        {"series": "stream_v3_observer_build_info" + selector(build_labels), "values": "1+0x4"},
        {"series": 'up{job="stream_v3_observation",instance="encoder.internal:9120"}',
         "values": "1" if case == "scrape_frozen" else "0+0x4" if case == "down" else "1+0x4"}],
        "promql_expr_test": [{"expr": direct.query(source()), "eval_time": "60s",
            "exp_samples": [{"labels": selector(labels), "value": 10}] if case == "fresh" else []}]}]}
    test = tmp_path / "query-tests.json"
    test.write_text(json.dumps(fixture))
    result = subprocess.run([binary, "test", "rules", str(test)], capture_output=True, text=True, timeout=10)
    assert result.returncode == 0, result.stdout + result.stderr
