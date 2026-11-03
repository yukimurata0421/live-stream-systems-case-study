import importlib.util
import json
import os
from pathlib import Path
import subprocess

import pytest

ROOT = Path(__file__).parents[1]
spec = importlib.util.spec_from_file_location('provenance_rule', ROOT / 'ops/scripts/render_provenance_alert.py')
renderer = importlib.util.module_from_spec(spec)
spec.loader.exec_module(renderer)
REGISTRY = {'components': [{'name': 'observer', 'unit': 'observer.service', 'entrypoint': '/bin/observer'}]}


def rendered():
    return renderer.render(REGISTRY, 'a'*64, 'b'*64, 'monitor:9126')


def fixture(case, rulefile):
    labels = 'job="stream_v3_monitor_observation",instance="monitor:9126"'
    component = labels + ',component="observer"'
    rows = [{"series": "up{" + labels + "}", "values": "1+0x75"}]
    if case != "missing":
        rows += [
            {"series": "stream_v3_release_audit_available{" + labels + "}", "values": ("0" if case == "stale" else "1") + "+0x75"},
            {"series": "stream_v3_release_audit_age_seconds{" + labels + "}", "values": "0+0x75"},
            {"series": "stream_v3_observed_release_source_match{" + component + "}", "values": ("0" if case == "drift" else "1") + "+0x75"},
            {"series": "stream_v3_observer_build_info{" + labels + ',source_hash="' + 'a'*64 + '",config_hash="' + 'b'*64 + '"}', "values": "1+0x75"}]
        if case != "entry_missing":
            rows.append({"series": "stream_v3_observed_release_configured_entrypoint_match{" + component + "}", "values": "1+0x75"})
    if case == 'frozen':
        for row in rows:
            row['values'] = row['values'].split('+')[0]
    if case == 'wrong_build':
        for row in rows:
            if row['series'].startswith('stream_v3_observer_build_info{'):
                row['series'] = row['series'].replace('a'*64, 'c'*64)
        # A matching label in a different family cannot confirm startup identity.
        rows.append({'series': 'stream_v3_observed_release_info{' + labels + ',source_hash="' + 'a'*64 + '",config_hash="' + 'b'*64 + '"}', 'values': '1+0x75'})
    if case == 'transition':
        for row in rows:
            if row['series'].startswith('stream_v3_release_audit_available{'):
                row['values'] = '1+0x25 0+0x25 1+0x25'
            if row['series'].startswith('stream_v3_observed_release_source_match{'):
                row['values'] = '0+0x25 1+0x51'
    if case == 'alternating':
        for row in rows:
            if row['series'].startswith('stream_v3_release_audit_available{'):
                row['values'] = ' '.join('1' if i % 2 == 0 else '0' for i in range(76))
            if row['series'].startswith('stream_v3_observed_release_source_match{'):
                row['values'] = ' '.join('0' if i % 2 == 0 else '1' for i in range(76))
    expected_labels = {'job': 'stream_v3_monitor_observation', 'instance': 'monitor:9126', 'severity': 'warning', 'scope': 'observation'}
    alert = {'exp_labels': expected_labels, 'exp_annotations': {'summary': 'The observation source inventory is not fully confirmed'}}
    tests = [{'eval_time': '6m', 'alertname': 'ObservationSourceProvenanceUnconfirmed',
              'exp_alerts': [] if case == 'healthy' else [alert]}]
    if case == 'transition':
        tests += [{'eval_time': '12m', 'alertname': 'ObservationSourceProvenanceUnconfirmed', 'exp_alerts': [alert]},
                  {'eval_time': '15m', 'alertname': 'ObservationSourceProvenanceUnconfirmed', 'exp_alerts': []}]
    return {'rule_files': [str(rulefile)], 'evaluation_interval': '15s', 'tests': [
        {'interval': '15s', 'input_series': rows, 'alert_rule_test': tests}]}


def test_rule_counts_are_derived_from_the_pinned_inventory():
    value = rendered()
    assert '@' not in value and '== 1)' in value
    with pytest.raises(ValueError):
        renderer.render({'components': []}, 'a'*64, 'b'*64, 'monitor:9126')


@pytest.mark.parametrize('case', ['healthy', 'drift', 'stale', 'missing', 'entry_missing', 'frozen', 'transition', 'alternating', 'wrong_build'])
def test_provenance_recovers_only_after_positive_confirmation(tmp_path, case):
    tool = os.environ.get('PROMTOOL')
    if not tool:
        pytest.skip('native promtool required')
    rulefile = tmp_path / 'rules.yml'
    rulefile.write_text(rendered())
    p = tmp_path / 'test.json'
    p.write_text(json.dumps(fixture(case, rulefile)))
    result = subprocess.run([tool, 'test', 'rules', str(p)], capture_output=True, text=True, timeout=15)
    assert result.returncode == 0, result.stdout + result.stderr
