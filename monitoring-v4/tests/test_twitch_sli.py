import importlib.util
import pathlib
import tempfile
import unittest

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
        self.assertIsNotNone(sli.api_sample(r,self.start,{'user_id':'100000001','collector_revision':sli.REVISION}))
        self.assertIsNone(sli.api_sample(r,self.start+181,{'user_id':'100000001','collector_revision':sli.REVISION}))
        r['collector_revision']='wrong';self.assertIsNone(sli.api_sample(r,self.start,{'user_id':'100000001','collector_revision':sli.REVISION}))
        r['collector_revision']=sli.REVISION;r['source_boot_id']='wrong';self.assertIsNone(sli.api_sample(r,self.start,{'user_id':'100000001','collector_revision':sli.REVISION}))

    def test_relay_counter_unknown_is_not_success(self):
        r={'schema':'stream.relay_public.v1','observed_at':sli.iso(self.start),'source_fresh':True,'destinations':{'twitch':{'status':'CONNECTED'}}}
        self.assertEqual(sli.relay_sample(r,self.start)['flow'],'UNKNOWN')
        r['observed_at']=sli.iso(self.start-181);self.assertIsNone(sli.relay_sample(r,self.start))


if __name__=='__main__':unittest.main()
