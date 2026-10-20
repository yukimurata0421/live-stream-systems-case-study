import importlib.util
import io
import json
import pathlib
import tempfile
import unittest
from unittest import mock

PATH=pathlib.Path(__file__).resolve().parents[1]/'src/stream_monitoring_v4/compatibility/twitch_sli.py'
spec=importlib.util.spec_from_file_location('twitch_sli',PATH);sli=importlib.util.module_from_spec(spec);spec.loader.exec_module(sli)


class TwitchSliTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.path=pathlib.Path(self.tmp.name)/'history.sqlite3'
        self.start=1791900000.
        self.h=sli.History(self.path,self.start);self.addCleanup(lambda:self.h.db.close())

    def api(self,at,state='GOOD',stream='fixture-before'):
        return {'observed':at,'api':'GOOD','live':state,'stream':stream,'started':sli.iso(at-3600) if state=='GOOD' else None}

    def metric(self,now,kind='provider_live',window=0):
        return next(m for m in self.h.public(now)['windows'][window]['metrics'] if m['id']==kind)

    def test_cold_start_unknown_time_not_counted_as_success(self):
        self.h.tick(self.start+2,self.api(self.start+2))
        m=self.metric(self.start+60)
        self.assertEqual(m['value_percent'],100)
        self.assertEqual(m['assessment'],'COLLECTING')
        self.assertEqual(m['missing_minutes'],1439)
        self.assertLess(m['conservative_percent'],.1)

    def test_duplicate_cached_samples_and_prestart_backfill_are_ignored(self):
        self.h.tick(self.start+2,self.api(self.start-60))
        self.h.tick(self.start+3,self.api(self.start+2))
        self.h.tick(self.start+62,self.api(self.start+2))
        self.assertEqual(self.metric(self.start+120)['good_minutes'],1)

    def test_same_minute_failure_survives_later_success(self):
        self.h.tick(self.start+2,self.api(self.start+2,'BAD'))
        self.h.tick(self.start+30,self.api(self.start+30))
        self.assertEqual(self.metric(self.start+60)['bad_minutes'],1)

    def test_persistence_keeps_start_and_history_across_reopen(self):
        self.h.tick(self.start+2,self.api(self.start+2))
        other=sli.History(self.path,self.start+1000)
        self.addCleanup(other.db.close)
        self.assertEqual(other.started,self.start)
        self.assertEqual(other.public(self.start+120)['windows'][0]['metrics'][0]['good_minutes'],1)

    def fill(self,bad=0,missing=0):
        rows=[(int(self.start//60)+i,'provider_live','BAD' if i<bad else 'GOOD',self.start+i*60+2) for i in range(1440-missing)]
        self.h.db.executemany('INSERT INTO minutes VALUES (?,?,?,?)',rows);self.h.db.commit()

    def test_full_window_met_and_confirmed_budget_breach(self):
        self.fill(bad=15);m=self.metric(self.start+86400)
        self.assertEqual(m['assessment'],'BREACHED');self.assertGreater(m['confirmed_budget_used_percent'],100)
        self.h.db.execute("UPDATE minutes SET state='GOOD'");self.h.db.commit()
        self.assertEqual(self.metric(self.start+86400)['assessment'],'MET')

    def test_insufficient_coverage_cannot_pass_even_with_observed_100(self):
        self.fill(missing=30);m=self.metric(self.start+86400)
        self.assertEqual(m['value_percent'],100);self.assertEqual(m['assessment'],'UNKNOWN')

    def test_api_failure_counts_bad_measurement_and_unknown_live(self):
        api=self.api(self.start+2,'UNKNOWN');api['api']='BAD';self.h.tick(self.start+2,api)
        self.assertEqual(self.metric(self.start+60)['unknown_minutes'],1)
        self.assertEqual(self.metric(self.start+60,'api_measurement')['bad_minutes'],1)

    def test_planned_breaks_are_included_and_other_provider_is_not_changed(self):
        self.h.tick(self.start+2,self.api(self.start+2,'BAD'),{'observed':self.start+2,'flow':'BAD'})
        p=self.h.public(self.start+60);self.assertTrue(p['planned_downtime_included'])
        self.assertEqual(self.metric(self.start+60,'relay_flow')['bad_minutes'],1)
        self.assertNotIn('youtube',str(p))

    def test_no_renewal_is_marked_success_before_offline_and_new_id(self):
        due=int((self.start-54000)//86400)*86400+54000
        if due<=self.start:due+=86400
        self.h.tick(due-30,self.api(due-30))
        self.h.tick(due+30,self.api(due+30))
        self.assertEqual(self.metric(due+60,'daily_renewal')['good_events'],0)
        self.h.tick(due+90,self.api(due+90,'BAD',''))
        fresh=self.api(due+180,stream='fixture-new');fresh['started']=sli.iso(due+150)
        self.h.tick(due+180,fresh)
        self.assertEqual(self.metric(due+240,'daily_renewal')['good_events'],1)
        self.assertNotIn('fixture-before',str(self.h.public(due+240)))

    def test_renewal_missing_evidence_remains_unknown(self):
        due=int((self.start-54000)//86400)*86400+54000
        if due<=self.start:due+=86400
        self.h.tick(due+800)
        self.assertEqual(self.metric(due+840,'daily_renewal')['unknown_events'],1)
        self.assertEqual(self.metric(due+840,'daily_renewal')['good_events'],0)

    def test_same_id_and_late_new_id_do_not_verify_renewal(self):
        due=int((self.start-54000)//86400)*86400+54000
        if due<=self.start:due+=86400
        self.h.tick(due-30,self.api(due-30))
        for i in range(10):
            sample=self.api(due+i*60+2,'BAD' if i<2 else 'GOOD')
            sample['started']=sli.iso(due+121) if i>=2 else None
            self.h.tick(sample['observed'],sample)
        late=self.api(due+650,stream='fixture-late-new');late['started']=sli.iso(due+640)
        self.h.tick(due+650,late)
        self.h.tick(due+800)
        m=self.metric(due+840,'daily_renewal')
        self.assertEqual(m['good_events'],0);self.assertEqual(m['bad_events'],1)

    def test_stale_wrong_revision_and_wrong_boot_are_not_observations(self):
        r={'schema':'monitoring_v4.twitch_api_evidence.v1','source_host_id':'arena-monitoring-facts',
           'source_boot_id':pathlib.Path('/proc/sys/kernel/random/boot_id').read_text().strip(),
           'collector_revision':sli.REVISION,'user_id':'100000001','collected_at':sli.iso(self.start),
           'valid_until':sli.iso(self.start+180),'probe_status':'ok','http_status':200,'provider_state':'LIVE'}
        self.assertIsNotNone(self.api_sample(r,self.start))
        self.assertIsNone(self.api_sample(r,self.start+181))
        r['collector_revision']='wrong';self.assertIsNone(self.api_sample(r,self.start))
        r['collector_revision']=sli.REVISION;r['source_boot_id']='wrong';self.assertIsNone(self.api_sample(r,self.start))

    def test_relay_counter_unknown_is_not_success(self):
        r={'schema':'stream.relay_public.v1','observed_at':sli.iso(self.start),'source_fresh':True,'destinations':{'twitch':{'status':'CONNECTED'}}}
        self.assertEqual(sli.relay_sample(r,self.start)['flow'],'UNKNOWN')
        r['observed_at']=sli.iso(self.start-181);self.assertIsNone(sli.relay_sample(r,self.start))

    def api_sample(self, raw, now):
        return sli.api_sample(raw, now, {'user_id':'100000001','collector_revision':sli.REVISION})

    def raw_api(self, at):
        return {'schema':'monitoring_v4.twitch_api_evidence.v1','source_host_id':'arena-monitoring-facts',
                'source_boot_id':pathlib.Path('/proc/sys/kernel/random/boot_id').read_text().strip(),
                'collector_revision':sli.REVISION,'user_id':'100000001','collected_at':sli.iso(at),
                'valid_until':sli.iso(at+180),'probe_status':'ok','http_status':200,'provider_state':'LIVE'}

    def test_invalid_timestamp_types_are_rejected_without_attribute_error(self):
        for value in (None, 123, True, {}, [], '', '2026-10-14T06:00:00', 'not-a-time'):
            with self.subTest(value=value), self.assertRaises(ValueError): sli.stamp(value)

    def test_invalid_api_timestamps_do_not_escape_the_sample_boundary(self):
        for field in ('collected_at','valid_until'):
            for value in (None, 123, {}, [], 'not-a-time'):
                raw=self.raw_api(self.start);raw[field]=value
                with self.subTest(field=field,value=value): self.assertIsNone(self.api_sample(raw,self.start))

    def test_missing_and_malformed_relay_observations_are_not_success(self):
        base={'schema':'stream.relay_public.v1','source_fresh':True,'observed_at':sli.iso(self.start),
              'destinations':{'twitch':{'status':'FLOWING'}}}
        for value in (None, 123, {}, [], 'not-a-time'):
            raw=dict(base,observed_at=value)
            with self.subTest(value=value): self.assertIsNone(sli.relay_sample(raw,self.start))
        self.assertIsNone(sli.relay_sample({'schema':'stream.relay_public.v1','source_fresh':False,'observed_at':None},self.start))
        self.assertIsNone(sli.relay_sample(dict(base,destinations={'twitch':{'status':None}}),self.start))

    def test_non_object_sources_and_invalid_clocks_do_not_crash_or_count(self):
        for value in (None, [], 123, True, 'invalid'):
            self.assertIsNone(self.api_sample(value,self.start));self.assertIsNone(sli.relay_sample(value,self.start))
        for now in (None, 'invalid', float('nan'), float('inf'), True):
            self.assertIsNone(self.api_sample(self.raw_api(self.start),now))
            self.assertIsNone(sli.relay_sample({},now))

    def test_relay_loss_and_recovery_leave_a_gap_while_api_continues(self):
        source=pathlib.Path(self.tmp.name)/'api.json';opener=mock.Mock()
        at=self.start+2
        for offset,relay in ((0,None),(60,'FLOWING')):
            now=at+offset;source.write_text(json.dumps(self.raw_api(now)))
            raw={'schema':'stream.relay_public.v1','source_fresh':relay is not None,
                 'observed_at':sli.iso(now) if relay else None,'destinations':{'twitch':{'status':relay}}}
            opener.open.return_value=io.BytesIO(json.dumps(raw).encode())
            with mock.patch.object(sli.time,'time',return_value=now),mock.patch.object(sli,'log_event'):
                sli.collect_once(self.h,opener,now,source,{'user_id':'100000001','collector_revision':sli.REVISION})
        self.assertEqual(self.metric(self.start+120)['good_minutes'],2)
        self.assertEqual(self.metric(self.start+120,'relay_flow')['good_minutes'],1)
        self.assertEqual(self.metric(self.start+120,'relay_flow')['missing_minutes'],1439)

    def test_bad_json_and_unreachable_relay_keep_original_history(self):
        self.h.tick(self.start+2,self.api(self.start+2))
        source=pathlib.Path(self.tmp.name)/'api.json';source.write_text('{broken')
        opener=mock.Mock();opener.open.side_effect=OSError('fixture unavailable')
        with mock.patch.object(sli.time,'time',return_value=self.start+120),mock.patch.object(sli,'log_event'):
            value=sli.collect_once(self.h,opener,self.start+120,source)
        self.assertEqual(value['windows'][0]['metrics'][0]['good_minutes'],1)
        self.assertEqual(value['history_started_at'],sli.iso(self.start))
        self.assertEqual(self.metric(self.start+120)['missing_minutes'],1439)

    def test_database_cycle_is_rolled_back_on_unexpected_failure(self):
        original=self.h._tick
        def fail_after_write(*args):
            original(*args)
            raise RuntimeError('fixture transaction failure')
        with mock.patch.object(self.h,'_tick',side_effect=fail_after_write),self.assertRaises(RuntimeError):
            self.h.tick(self.start+2,self.api(self.start+2))
        self.assertEqual(self.h.db.execute('SELECT COUNT(*) FROM minutes').fetchone()[0],0)
        self.assertIsNone(self.h.meta('last_api'))

    def test_source_status_logs_transitions_without_repeating_payloads(self):
        with mock.patch.dict(sli.SOURCE_STATUS,{},clear=True),mock.patch.object(sli,'log_event') as log:
            sli.source_status('relay','unavailable');sli.source_status('relay','unavailable')
            sli.source_status('relay','observed')
        self.assertEqual(log.call_count,2)
        self.assertEqual(log.call_args.kwargs,{'source':'relay','status':'observed'})

    def test_watchdog_supports_abstract_socket_without_external_network(self):
        with mock.patch.dict(sli.os.environ,{'NOTIFY_SOCKET':'@fixture-notify'}),mock.patch.object(sli.socket,'socket') as factory:
            self.assertTrue(sli.notify('READY=1\nWATCHDOG=1'))
        channel=factory.return_value.__enter__.return_value
        channel.connect.assert_called_once_with('\0fixture-notify')
        channel.sendall.assert_called_once_with(b'READY=1\nWATCHDOG=1')

    def test_fatal_cycle_reports_safe_fields_and_clears_projection(self):
        binding=pathlib.Path(self.tmp.name)/'binding.json';binding.write_text(json.dumps({'user_id':'100000001','collector_revision':sli.REVISION}))
        fake_history=mock.Mock();fake_worker=mock.Mock();fake_worker.is_alive.return_value=True
        with mock.patch('sys.argv',['twitch_sli.py','--state',self.tmp.name,'--binding',str(binding)]),\
             mock.patch.object(sli.signal,'signal'),mock.patch.object(sli,'History',return_value=fake_history),\
             mock.patch.object(sli,'ThreadingHTTPServer') as server,\
             mock.patch.object(sli.threading,'Thread',return_value=fake_worker),\
             mock.patch.object(sli,'collect_once',side_effect=RuntimeError('PRIVATE_FIXTURE_SECRET')),\
             mock.patch.object(sli,'log_event') as log, mock.patch.dict(sli.LATEST,{'old':'fixture'},clear=True):
            self.assertEqual(sli.main(),1);self.assertEqual(sli.LATEST,{})
        failure=next(call for call in log.call_args_list if call.args==('collector_failed',))
        self.assertEqual(failure.kwargs['phase'],'collection')
        self.assertEqual(failure.kwargs['exception_class'],'RuntimeError')
        self.assertNotIn('PRIVATE_FIXTURE_SECRET',str(log.call_args_list))
        server.return_value.shutdown.assert_called_once();fake_history.db.close.assert_called_once()

    def test_service_restart_is_bounded_and_logs_and_watchdog_are_enabled(self):
        unit=(PATH.parents[3]/'deploy/systemd/stream-v4-twitch-sli.service').read_text()
        for setting in ('Type=notify','StartLimitIntervalSec=900','StartLimitBurst=3',
                        'Restart=on-failure','RestartSec=30','WatchdogSec=90',
                        'WatchdogSignal=SIGTERM','StandardError=journal'):
            self.assertIn(setting,unit)


    def test_readiness_and_watchdog_follow_completed_cycles(self):
        binding=pathlib.Path(self.tmp.name)/'binding.json';binding.write_text(json.dumps({'user_id':'100000001','collector_revision':sli.REVISION}))
        stopped=mock.Mock();stopped.is_set.side_effect=[False,False,True]
        worker=mock.Mock();worker.is_alive.return_value=True
        value={'schema':sli.SCHEMA,'generated_at':sli.iso(self.start)}
        notifications=[]
        def notified(message):
            self.assertEqual(sli.LATEST,value)
            notifications.append(message)
            return True
        with mock.patch('sys.argv',['twitch_sli.py','--state',self.tmp.name,'--binding',str(binding)]),\
             mock.patch.object(sli.signal,'signal'),mock.patch.object(sli,'History') as history,\
             mock.patch.object(sli,'ThreadingHTTPServer'),\
             mock.patch.object(sli.threading,'Thread',return_value=worker),\
             mock.patch.object(sli.threading,'Event',return_value=stopped),\
             mock.patch.object(sli,'collect_once',return_value=value) as collect,\
             mock.patch.object(sli,'notify',side_effect=notified),\
             mock.patch.object(sli,'log_event'),mock.patch.dict(sli.LATEST,{},clear=True):
            self.assertEqual(sli.main(),0)
        self.assertEqual(collect.call_count,2)
        self.assertEqual(notifications,['READY=1\nWATCHDOG=1','WATCHDOG=1'])
        history.return_value.db.close.assert_called_once()


if __name__=='__main__':unittest.main()
