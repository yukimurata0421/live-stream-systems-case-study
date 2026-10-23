from __future__ import annotations

import contextlib
import io
import json
import os
import random
import sqlite3
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from dataclasses import replace
from unittest.mock import patch

from stream_contracts.monitoring_v4.time import utc_text
from stream_monitoring_v4.adapters.factory import SHADOW_DOMAINS
from stream_monitoring_v4.commands.normal_report import main as normal_main
from stream_monitoring_v4.history.archive import archive_day, day_bounds, verify_archive, JST
from stream_monitoring_v4.history.normal import build_normal_report, stage_normal_cycle
from stream_monitoring_v4.history.rolling import STATUSES, WINDOWS, advance, read_windows, record_slot
from stream_monitoring_v4.reporting.policy import COLLECTOR_ADAPTERS, SOURCE_CADENCES
from stream_monitoring_v4.runtime.observer import ObserverRun
from stream_monitoring_v4.runtime.shadow_cycle import ShadowCycleRequest, execute_shadow_cycle
from stream_monitoring_v4.sentinel.evidence import report_status
from stream_monitoring_v4.storage.repository import MonitoringRepository
from tests.helpers import BASE_TS, current, observation
import datetime as dt

BUILD = 'a' * 40
SOURCE = 'fixture-source-r1'
START = BASE_TS // 60 * 60


def cycle_result(ts, *, unknown=False):
    facts = [{'source': source, 'observed_at': utc_text(ts), 'received_at': utc_text(ts),
              'freshness_limit_sec': 180, 'observation_id': 'event',
              'evidence_role': 'current_authoritative', 'status': 'good'} for source in SOURCE_CADENCES]
    sources = tuple({'source': name, 'outcome': 'observed', 'observation_events': facts if i == 0 else []}
                    for i, name in enumerate(COLLECTOR_ADAPTERS))
    return SimpleNamespace(
        observer=ObserverRun(utc_text(ts), utc_text(ts), 0, len(sources), 0, 0, 0, (), sources),
        currents=[replace(current(domain=name, state='unknown' if unknown else 'good', reduced_ts=ts,
                          observed_ts=ts), valid_until=utc_text(ts + 180)) for name in SHADOW_DOMAINS],
    )


class HistoryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.repo = MonitoringRepository(self.root / 'source' / 'monitor.sqlite3')
        self.repo.initialize(applied_at=utc_text(START))

    def tearDown(self):
        self.temp.cleanup()

    def slot(self, c, ts, *, event=None, status='good', now=None, definition='test-v1'):
        return record_slot(c, metric='fixture', target='stable-stream', sample_ts=ts,
                           event_id=event or str(ts), status=status,
                           now_ts=ts if now is None else now, definition_revision=definition)

    def stage(self, ts=START, *, build=BUILD, unknown=False):
        with self.repo.transaction() as c:
            stage_normal_cycle(c, result=cycle_result(ts, unknown=unknown), cycle_id=str(ts),
                               completed_ts=ts, build_revision=build, source_revision=SOURCE,
                               projection_status={'complete': True})

    def test_current_minute_excluded_missing_and_deduplicated(self):
        with self.repo.transaction() as c:
            self.slot(c, START)
            self.slot(c, START)
            self.assertEqual(read_windows(c, target='stable-stream')[0]['observed'], 0)
            self.slot(c, START + 180)
        # The two missed completed minutes are never filled in as GOOD.
        with self.repo.connection(read_only=True) as c:
            rows = read_windows(c, target='stable-stream')
        self.assertTrue(all((r['observed'], r['missing'], r['expected']) == (1, 2, 3) for r in rows))
        self.assertEqual(rows[0]['good_pct_known'], 100)
        self.assertAlmostEqual(rows[0]['good_pct_conservative'], 100 / 3)

    def test_late_replacement_conflicts_rollback_and_definition_changes(self):
        with self.repo.transaction() as c:
            self.slot(c, START)
            self.slot(c, START + 120)
            self.slot(c, START + 1, event='correction', status='bad', now=START + 120)
            self.slot(c, START + 1, event='correction', status='bad', now=START + 120)
            row = read_windows(c, target='stable-stream')[0]
            self.assertEqual((row['good'], row['bad'], row['missing']), (0, 1, 1))
        with self.assertRaises(ValueError), self.repo.transaction() as c:
            self.slot(c, START + 1, event='correction', status='good', now=START + 120)
        with self.assertRaises(ValueError), self.repo.transaction() as c:
            self.slot(c, START - 60, now=START + 120)
        with self.assertRaises(ValueError), self.repo.transaction() as c:
            self.slot(c, START + 2, now=START + 8 * 86400)
        with self.repo.transaction() as c:
            self.slot(c, START + 180, definition='test-v2')
        with self.repo.connection(read_only=True) as c:
            self.assertEqual(c.execute('SELECT COUNT(*) FROM metric_series').fetchone()[0], 2)

    def test_incremental_totals_match_sql_rebuild_after_expiry_restart_and_late_corrections(self):
        rng = random.Random(11)
        with self.repo.transaction() as c:
            self.slot(c, START)
        for i in range(1, 100):
            ts = START + i * 600
            with self.repo.transaction() as c:
                self.slot(c, ts, status=rng.choice(STATUSES))
                late = max(START, ts - rng.randrange(0, 40) * 60)
                self.slot(c, late + 1, event=f'correction-{i}', status=rng.choice(STATUSES), now=ts + 1)
            # Simulate a new process/repository after each commit.
            self.repo = MonitoringRepository(self.repo.path)
            with self.repo.connection(read_only=True) as c:
                for r in read_windows(c, target='stable-stream'):
                    direct = c.execute('SELECT status, COUNT(*) n FROM metric_minutes WHERE series_id=? AND minute_ts>=? AND minute_ts<? GROUP BY status',
                                       (r['series_id'], r['end_ts'] - r['window_sec'], r['end_ts'])).fetchall()
                    counts = {row['status']: row['n'] for row in direct}
                    self.assertEqual([r[s] for s in STATUSES], [counts.get(s, 0) for s in STATUSES])
        with self.repo.transaction() as c:
            series = c.execute('SELECT series_id FROM metric_series').fetchone()[0]
            advance(c, series, end_ts=START + 40 * 86400)
            self.assertTrue(all(r['observed'] == 0 for r in read_windows(c, target='stable-stream')))

    def test_report_is_small_and_independent_of_qualification_and_build_history(self):
        self.stage()
        self.stage(START + 60, build='b' * 40)
        # It must work even if every access to the raw/report stream raises.
        with patch.object(self.repo, 'iter_shadow_cycle_report_rows', side_effect=AssertionError('raw read')):
            report = build_normal_report(self.repo, now_ts=START + 61, build_revision='b' * 40, source_revision=SOURCE)
        self.assertTrue(report['healthy'])
        self.assertFalse(report['qualification']['required_for_normal_health'])
        self.assertTrue(all(r['started_ts'] == START for r in report['rolling']))
        self.assertTrue(all(r['observed'] == 1 for r in report['rolling']))
        path = self.root / 'normal.json'; path.write_text(json.dumps(report))
        status = report_status(path, now_ts=START + 62, max_age_sec=420)
        self.assertTrue(status['normal_health_clean'])
        self.assertNotIn('parity_clean', status)
        report['healthy'] = True
        report['health_checks'].pop('aggregates_fresh')
        path.write_text(json.dumps(report))
        self.assertFalse(report_status(path, now_ts=START + 62, max_age_sec=420)['normal_health_clean'])
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            code = normal_main(['--db', str(self.repo.path), '--build-revision', 'b' * 40,
                                '--source-revision', SOURCE, '--now-ts', str(START + 1000)])
        self.assertEqual(code, 1)
        self.assertFalse(json.loads(out.getvalue())['healthy'])

    def test_unknown_and_stale_facts_remain_unhealthy(self):
        self.stage(unknown=True)
        report = build_normal_report(self.repo, now_ts=START + 1, build_revision=BUILD, source_revision=SOURCE)
        self.assertFalse(report['healthy'])
        self.assertFalse(report['latest_cycle']['health_checks']['currents_known'])

    def test_default_cycle_skips_comparison_and_failed_optional_campaign_does_not_stop_normal(self):
        state = self.root / 'facts'; state.mkdir()
        request = ShadowCycleRequest(state, self.root / 'unrelated-source', self.repo.path, None,
                                     BUILD, SOURCE, START, True)
        with patch('stream_monitoring_v4.runtime.shadow_cycle.normalized_v3_current', side_effect=RuntimeError('comparison failed')) as legacy:
            first = execute_shadow_cycle(self.repo, request)
            legacy.assert_not_called()
            self.assertEqual(first['parity']['qualification_status'], 'disabled')
            from dataclasses import replace
            second = execute_shadow_cycle(self.repo, replace(request, fixed_now_ts=START + 60, migration_campaign='explicit-fixture'))
            self.assertEqual(second['parity']['qualification_status'], 'failed')
        with self.repo.connection(read_only=True) as c:
            self.assertEqual(c.execute('SELECT COUNT(*) FROM normal_cycle_current').fetchone()[0], 1)
        rows = list(self.repo.iter_shadow_cycle_report_rows(build_revision=BUILD, source_revision=SOURCE, start_ts=START, end_ts=START + 120, campaign_id='explicit-fixture'))
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]['parity']['qualification_status'], 'failed')
        self.assertEqual(len(list(self.repo.iter_shadow_cycle_report_rows(build_revision=BUILD, source_revision=SOURCE, start_ts=START, end_ts=START + 120, campaign_id='other'))), 0)

    def archive_fixture(self):
        day = dt.datetime.fromtimestamp(START, JST).date().isoformat()
        begin, end = day_bounds(day)
        item = observation(observed_ts=begin - 86400, received_ts=begin + 10,
                           payload={'late': True, 'text': 'archive fixture'})
        self.repo.append_observation(item)
        return day, begin, end, item

    def test_archive_copy_idempotent_preserves_observed_and_received_clocks(self):
        day, begin, end, item = self.archive_fixture()
        root = self.root / 'archive'
        receipt = archive_day(self.repo, day=day, output_dir=root, now_ts=end + 8 * 86400)
        again = archive_day(self.repo, day=day, output_dir=root, now_ts=end + 9 * 86400)
        self.assertEqual(receipt, again)
        path = root / receipt['archive']
        self.assertEqual(path.stat().st_mode & 0o777, 0o400)
        self.assertEqual(verify_archive(path)['tables']['observations']['count'], 1)
        with sqlite3.connect(f'{path.as_uri()}?mode=ro', uri=True) as c:
            archived = json.loads(c.execute("SELECT row_json FROM records WHERE table_name='observations'").fetchone()[0])
        self.assertEqual(archived['observation_id'], item.observation_id)
        self.assertEqual(archived['observed_ts'], begin - 86400)
        self.assertEqual(archived['received_ts'], begin + 10)
        with self.repo.connection(read_only=True) as c:
            self.assertEqual(c.execute('SELECT COUNT(*) FROM observations').fetchone()[0], 1)
        # A late insertion creates a new immutable snapshot, preserving the old one.
        self.repo.append_observation(observation(observed_ts=begin + 20, event='late-import'))
        later = archive_day(self.repo, day=day, output_dir=root, now_ts=end + 9 * 86400)
        self.assertNotEqual(later['archive'], receipt['archive'])
        self.assertEqual(len(list(root.glob('*.sqlite3'))), 2)

    def test_archive_publish_before_receipt_failure_recovers_and_corruption_stops(self):
        day, begin, end, _ = self.archive_fixture()
        root = self.root / 'archive'
        with patch('stream_monitoring_v4.history.archive.atomic_write_text', side_effect=OSError('disk unavailable')):
            with self.assertRaises(OSError):
                archive_day(self.repo, day=day, output_dir=root, now_ts=end + 8 * 86400)
        self.assertEqual(len(list(root.glob('*.sqlite3'))), 1)
        self.assertEqual(list(root.glob('*.staging')), [])
        receipt = archive_day(self.repo, day=day, output_dir=root, now_ts=end + 8 * 86400)
        path = root / receipt['archive']; path.chmod(0o600)
        with sqlite3.connect(path) as c:
            c.execute("UPDATE records SET row_json='{}'")
        with self.assertRaises((KeyError, RuntimeError)):
            archive_day(self.repo, day=day, output_dir=root, now_ts=end + 8 * 86400)
        with self.repo.connection(read_only=True) as c:
            self.assertEqual(c.execute('SELECT COUNT(*) FROM observations').fetchone()[0], 1)

    def test_archive_budget_and_recent_day_refusal_do_not_mutate_source(self):
        day, begin, end, _ = self.archive_fixture()
        root = self.root / 'archive'
        with self.assertRaises(ValueError):
            archive_day(self.repo, day=day, output_dir=root, now_ts=end + 6 * 86400)
        self.repo.append_observation(observation(observed_ts=begin + 1, payload={'large': 'x' * 10000}))
        with self.assertRaises(RuntimeError):
            archive_day(self.repo, day=day, output_dir=root, now_ts=end + 8 * 86400, max_bytes=4096)
        self.assertEqual(list(root.glob('*.sqlite3')), [])
        self.assertEqual(list(root.glob('*.staging')), [])
        with self.repo.connection(read_only=True) as c:
            self.assertEqual(c.execute('SELECT COUNT(*) FROM observations').fetchone()[0], 2)

    def test_unknown_na_and_missing_have_separate_denominators_and_cold_rebuild(self):
        with self.repo.transaction() as c:
            for i, status in enumerate(STATUSES):
                self.slot(c, START + i * 60, status=status)
            series = c.execute('SELECT series_id FROM metric_series').fetchone()[0]
            advance(c, series, end_ts=START + 5 * 60)
            before = read_windows(c, target='stable-stream')
            self.assertEqual((before[0]['good'], before[0]['bad'], before[0]['unknown'], before[0]['not_applicable'], before[0]['missing']), (1, 1, 1, 1, 1))
            self.assertEqual(before[0]['good_pct_known'], 50)
            self.assertEqual(before[0]['good_pct_conservative'], 25)
            c.execute('DELETE FROM metric_windows')  # Owned synthetic fixture only.
            advance(c, series, end_ts=START + 5 * 60)
            self.assertEqual(before, read_windows(c, target='stable-stream'))
        with self.assertRaises(ValueError), self.repo.transaction() as c:
            self.slot(c, START + 4 * 60, now=START + 4 * 60)
        with self.repo.connection(read_only=True) as c:
            self.assertEqual(before, read_windows(c, target='stable-stream'))

    def test_seven_day_window_sliding_matches_bucket_sum(self):
        with self.repo.transaction() as c:
            self.slot(c, START)
        for i in range(1, 40):
            ts = START + i * 90000
            with self.repo.transaction() as c:
                self.slot(c, ts, status=STATUSES[i % 4])
                for row in read_windows(c, target='stable-stream'):
                    direct = c.execute('SELECT COUNT(*) FROM metric_minutes WHERE series_id=? AND minute_ts>=? AND minute_ts<?',
                                       (row['series_id'], row['end_ts'] - row['window_sec'], row['end_ts'])).fetchone()[0]
                    self.assertEqual(row['observed'], direct)

    def test_retired_series_do_not_make_current_aggregate_health_stale(self):
        from dataclasses import replace
        self.stage()
        result = cycle_result(START + 86400)
        result.observer = replace(result.observer,
                                  source_results=tuple(r for r in result.observer.source_results if r['source'] != 'youtube_api_direct_state'),
                                  adapters=result.observer.adapters - 1)
        with self.repo.transaction() as c:
            stage_normal_cycle(c, result=result, cycle_id='configuration-updated', completed_ts=START + 86400,
                               build_revision=BUILD, source_revision=SOURCE, projection_status={'complete': True})
        report = build_normal_report(self.repo, now_ts=START + 86400 + 1, build_revision=BUILD, source_revision=SOURCE)
        self.assertTrue(report['healthy'])
        self.assertTrue(all(w['end_ts'] == START + 86400 for w in report['rolling']))

    def test_report_does_not_extend_source_ttl_until_cycle_age_limit(self):
        result = cycle_result(START)
        result.currents[0] = replace(result.currents[0], valid_until=utc_text(START + 30))
        with self.repo.transaction() as c:
            stage_normal_cycle(c, result=result, cycle_id='short-ttl', completed_ts=START,
                               build_revision=BUILD, source_revision=SOURCE, projection_status={'complete': True})
        report = build_normal_report(self.repo, now_ts=START + 31, build_revision=BUILD, source_revision=SOURCE)
        self.assertTrue(report['health_checks']['cycle_fresh'])
        self.assertFalse(report['health_checks']['facts_currently_valid'])
        self.assertFalse(report['healthy'])

    def test_sentinel_rechecks_original_deadline_between_normal_report_runs(self):
        result = cycle_result(START)
        result.currents[0] = replace(result.currents[0], valid_until=utc_text(START + 30))
        with self.repo.transaction() as c:
            stage_normal_cycle(c, result=result, cycle_id='short-ttl', completed_ts=START,
                               build_revision=BUILD, source_revision=SOURCE, projection_status={'complete': True})
        report = build_normal_report(self.repo, now_ts=START + 1, build_revision=BUILD, source_revision=SOURCE)
        self.assertTrue(report['healthy'])
        path = self.root / 'normal.json'; path.write_text(json.dumps(report))
        status = report_status(path, now_ts=START + 31, max_age_sec=420)
        self.assertTrue(status['fresh'])
        self.assertFalse(status['normal_health_clean'])
