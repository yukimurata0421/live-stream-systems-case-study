import json
from pathlib import Path

from stream_monitoring_v4.collectors.twitch_api import TwitchCollector, metrics
from stream_monitoring_v4.compatibility import twitch_sli as sli

NOW = 1791900000.0


def diagnostic_files(tmp_path):
    # Missing credentials exercises a real failed probe without any network call.
    collector = TwitchCollector(tmp_path / 'credentials', '100000001', 'fixturechannel',
                                wall=lambda: NOW, monotonic=lambda: 1000.0)
    raw = collector.collect()
    source = tmp_path / 'twitch_api_evidence.json'
    source.write_text(json.dumps(raw))
    text = metrics(raw)
    source.with_name('twitch_api.prom').write_text(text)
    return source, raw, text


def test_failed_probe_diagnostics_are_scrapable_but_stale_or_wrong_source_is_not(tmp_path):
    source, raw, text = diagnostic_files(tmp_path)
    collector_revision = raw['collector_revision']
    result = sli.api_diagnostic_metrics(NOW, source, binding={'user_id':'100000001','collector_revision':collector_revision})
    assert 'stream_v4_twitch_api_diagnostics_available 1' in result
    assert 'stream_v4_twitch_api_probes_total{result="credentials"} 1' in result
    assert 'rate_limit_remaining' not in text  # unknown quota is not zero quota
    assert sli.api_diagnostic_metrics(NOW+181, source, binding={'user_id':'100000001','collector_revision':collector_revision}).strip().endswith('0')
    raw['collector_revision'] = 'wrong'
    source.write_text(json.dumps(raw))
    assert sli.api_diagnostic_metrics(NOW, source, binding={'user_id':'100000001','collector_revision':collector_revision}).strip().endswith('0')


def test_separate_atomic_files_cannot_export_mismatched_or_unbounded_text(tmp_path):
    source, raw, text = diagnostic_files(tmp_path)
    collector_revision = raw['collector_revision']
    prom = source.with_name('twitch_api.prom')
    prom.write_text(text.replace(str(NOW), str(NOW-60)))
    assert sli.api_diagnostic_metrics(NOW, source, binding={'user_id':'100000001','collector_revision':collector_revision}).strip().endswith('0')
    prom.write_text(text+'unrelated_metric{token="secret"} 1\n')
    assert sli.api_diagnostic_metrics(NOW, source, binding={'user_id':'100000001','collector_revision':collector_revision}).strip().endswith('0')
    prom.write_text(text+'x'*65537)
    assert sli.api_diagnostic_metrics(NOW, source, binding={'user_id':'100000001','collector_revision':collector_revision}).strip().endswith('0')


def test_new_diagnostic_evidence_matches_the_backward_compatible_schema(tmp_path):
    from jsonschema import Draft202012Validator
    _, raw, _ = diagnostic_files(tmp_path)
    root = Path(__file__).resolve().parents[1]
    schema = json.loads((root/'src/stream_contracts/monitoring_v4/schema/twitch_api_evidence.v1.json').read_text())
    Draft202012Validator(schema).validate(raw)


def test_observed_success_and_collection_gaps_keep_distinct_denominators(tmp_path):
    history = sli.History(tmp_path/'history.sqlite3', NOW)
    try:
        for offset, state in ((30, 'GOOD'), (90, 'BAD'), (210, 'GOOD')):
            history.tick(NOW+offset, {'observed':NOW+offset, 'api':state, 'live':'UNKNOWN',
                                     'stream':'', 'started':None})
        value = history.public(NOW+240)
        text = sli.metrics(value)
        assert 'stream_v4_twitch_api_observed_success_percent{window="24h"} 66.66666666666667' in text
        assert 'stream_v4_twitch_sli_good_minutes{sli="api_measurement",window="24h"} 2' in text
        assert 'stream_v4_twitch_sli_bad_minutes{sli="api_measurement",window="24h"} 1' in text
        assert 'stream_v4_twitch_sli_missing_minutes{sli="api_measurement",window="24h"} 1437' in text
    finally:
        history.db.close()
