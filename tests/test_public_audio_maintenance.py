"""Verify the public audio denominator from trusted facts through projection."""

from dataclasses import replace
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import time

from stream_core.cli_support import sli_report
from stream_core.common import manual_maintenance_facts as source, observation_time as timing
from stream_core.notifications import manual_maintenance
from test_manual_maintenance_notifications import WALL, bundle, reading, save, session


def test_weekly_facts_require_confirmed_history_and_keep_notification_window(tmp_path):
    now = time.time()
    machine = hashlib.sha256(Path('/etc/machine-id').read_text().strip().encode()).hexdigest()
    current = session()
    current['phase'] = 'NORMAL'
    current_path = tmp_path / 'session.json'
    current_path.write_text(json.dumps(current))
    current_path.chmod(0o644)
    history = tmp_path / 'history'
    history.mkdir(mode=0o755)

    def completed(index, days_ago, *, confirmed, overdue=False):
        start = now - days_ago * 86400
        end = start + 120
        value = session()
        value.update(phase='NORMAL', generation=index, session_id=f'session-{index}',
                     deadline=start + (90 if overdue else 600))
        value['before']['dell']['identity']['machine'] = machine
        value['effects'][0]['at'] = start
        value['effects'].append({'name':'source_resume', 'role':'dell', 'action':'scale_main',
                                 'result':'APPLIED', 'at':end, 'request':{'uid':'main','after':1}})
        if confirmed:
            value['slate_player_confirmation'] = {'kind':'operator_attestation', 'youtube':True,
                                                  'twitch':True, 'at':start + 30}
        path = history / f'{index}.json'
        path.write_text(json.dumps(value))
        path.chmod(0o644)
        return {'start':start, 'end':end}

    expected = completed(2, 2, confirmed=True)
    completed(3, 1, confirmed=False)
    completed(4, 1, confirmed=True, overdue=True)
    completed(5, 8, confirmed=True)
    facts = source.read_manual_maintenance(tmp_path, uid=os.getuid())
    assert facts['audio_maintenance_stops'] == [expected]
    assert facts['notification_stops'] == []


def test_open_exclusion_requires_player_confirmation_and_fresh_bound_output(tmp_path, monkeypatch):
    value = bundle()
    facts = value['manual_maintenance']
    facts['audio_maintenance_stops'] = []
    save(tmp_path, value)
    monkeypatch.setattr(timing, 'capture', lambda: reading())
    assert manual_maintenance.public_audio_stops(tmp_path, end=WALL) == ()

    facts['audio_slate_confirmed'] = True
    save(tmp_path, value)
    assert manual_maintenance.public_audio_stops(tmp_path, end=WALL) == ((WALL-10, WALL+1),)

    value['native_delivery']['destinations']['youtube']['flowing'] = False
    save(tmp_path, value)
    assert manual_maintenance.public_audio_stops(tmp_path, end=WALL) is None
    value['native_delivery']['destinations']['youtube']['flowing'] = True
    save(tmp_path, value)
    monkeypatch.setattr(timing, 'capture', lambda: reading(36))
    assert manual_maintenance.public_audio_stops(tmp_path, end=WALL) is None


def test_public_report_keeps_pre_stop_and_new_post_resume_failures(tmp_path, monkeypatch):
    end = int(WALL)
    grid = range(end - 7 * 86400, end + 1, 60)
    audio = dict.fromkeys(grid, 1)
    scrapes = {stamp: stamp - 10 for stamp in grid}
    for delta in (360, 300, 180, 120, 60):
        audio[end - delta] = 0
    del audio[end - 240]
    scrapes[end - 360] = end - 375  # A failed scrape from before the stop.
    scrapes[end - 120] = end - 140  # A held scrape from the stop.
    series = {
        'stream_v3_audio_ok': audio,
        'stream_v3_audio_evidence_available': dict.fromkeys(grid, 1),
        'stream_v3_audio_evidence_age_seconds': dict.fromkeys(grid, 10),
    }

    def query(_base, metric, start, stop, _step, _timeout):
        values = scrapes if metric.startswith('timestamp(stream_v3_audio_ok)') else series.get(metric, {})
        return [{'values': [[stamp, str(value)] for stamp, value in values.items()
                            if start <= stamp <= stop]}]

    value = bundle(phase='NORMAL')
    value['manual_maintenance']['audio_maintenance_stops'] = [{'start':end-370, 'end':end-130}]
    save(tmp_path, value)
    monkeypatch.setattr(timing, 'capture', lambda: reading())
    ctx = sli_report.SliReportContext(tmp_path / 'watchdog.jsonl')
    args = dict(windows='24h,7d', end_time=str(end), query_range=query)
    raw = sli_report.build_sli_report(ctx, **args)
    normal = sli_report.build_sli_report(replace(ctx, public_audio_maintenance_root=tmp_path), **args)
    week = normal['windows']['7d']
    view = week['audio_normal_operation']
    assert view['planned_maintenance_excluded_minutes'] == 4
    assert view['coverage_pct'] == 100 and view['bad_points'] == 2
    assert view['expected_points'] == 10077 and view['nominal_budget_minutes'] == 50.38
    assert week['audio_correctness']['bad_points'] == 5

    script = Path(__file__).resolve().parents[1] / 'ops/public-publisher/site/scripts/reliability_projection.py'
    spec = importlib.util.spec_from_file_location('audio_public_projection_test', script)
    projection = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(projection)
    item = projection.audio_item(week, normal['windows']['24h'])
    assert item['label'] == 'Audio checks (normal operation)'
    facts = {fact['label']: fact['value'] for fact in item['facts']}
    assert facts['All periods bad minutes'] == 5
    assert facts['Verified planned maintenance excluded'] == 4

    for window in normal['windows'].values():
        window.pop('audio_normal_operation')
    assert normal == raw

    del value['manual_maintenance']['audio_maintenance_stops']  # An older producer.
    save(tmp_path, value)
    fallback = sli_report.build_sli_report(replace(ctx, public_audio_maintenance_root=tmp_path), **args)
    assert fallback == raw
