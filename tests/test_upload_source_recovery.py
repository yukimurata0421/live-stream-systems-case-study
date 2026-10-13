import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from stream_core.notifications import incidents, upload_source

class UploadSourceTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name);(self.root/'logs').mkdir()
        self.path=self.root/'logs/fast_recovery_events.jsonl';self.now=1791889800

    def rows(self,times):
        self.path.write_text(''.join(json.dumps({'kind':'tcp_send_sample','ts_utc':datetime.fromtimestamp(t,timezone.utc).isoformat(),'mbps':3.6})+'\n' for t in times))

    def prom(self,ages=None):
        return {'status':'success','data':{'resultType':'matrix','result':[{'metric':{'job':'stream_v3_arena_monitor','instance':'127.0.0.1:9108'},'values':[[self.now-900+i*60,str(a)] for i,a in enumerate(ages if ages is not None else [30]*16)]}]}}

    def stable(self):self.rows(range(self.now-960,self.now+1,60))

    def test_current_source_recovery_requires_raw_and_prometheus(self):
        self.stable()
        with patch.object(upload_source,'fetch_prometheus',return_value=self.prom()):
            self.assertEqual(upload_source.current_upload_source(self.root,self.now)['status'],'RECOVERED')
        with patch.object(upload_source,'fetch_prometheus',side_effect=OSError('fixture')):
            self.assertEqual(upload_source.current_upload_source(self.root,self.now)['status'],'UNKNOWN')

    def test_stale_or_future_raw_cannot_be_recovered(self):
        self.rows([self.now-181]);self.assertEqual(upload_source.current_upload_source(self.root,self.now)['status'],'FAILED')
        self.rows([self.now+1]);self.assertEqual(upload_source.current_upload_source(self.root,self.now)['status'],'UNKNOWN')

    def test_raw_gap_and_cold_start_require_recovery_window(self):
        for times in [[self.now-960,self.now-900,self.now-30],range(self.now-300,self.now+1,60)]:
            self.rows(times)
            with patch.object(upload_source,'fetch_prometheus',return_value=self.prom()):
                self.assertEqual(upload_source.current_upload_source(self.root,self.now)['status'],'RECOVERING')

    def test_raw_tail_does_not_follow_symlink_or_rejuvenate_old_samples(self):
        target=self.root/'actual';target.write_text('{}\n');self.path.symlink_to(target)
        self.assertFalse(upload_source.raw_health(self.path,self.now)['available'])

    def test_prometheus_gap_stale_nonfinite_and_duplicate_series_are_not_recovered(self):
        cases=[]
        missing=self.prom();missing['data']['result'][0]['values'].pop(2);cases.append(missing)
        cases.append(self.prom([30]*15+[181]));cases.append(self.prom([30]*15+[float('nan')]))
        multi=self.prom();multi['data']['result']*=2;cases.append(multi)
        for case in cases:self.assertFalse(upload_source.prom_health(case,self.now)['sustained'])

    def status_file(self,reasons=None,family='upload_ceiling',status='unknown'):
        p=self.root/'operational_reliability_status.json'
        p.write_text(json.dumps({'checked_at_utc':datetime.fromtimestamp(self.now,timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
            'formal_gates':{family:{'compliance_status':status,'measurement_status':'unknown','measurement_unknown_reasons':reasons if reasons is not None else ['source_freshness_not_met'],'coverage_pct':100.0,'source_freshness_pct':96.53}}}))
        return p

    def test_recovery_closes_source_incident_and_keeps_historical_gate_bytes(self):
        self.stable();p=self.status_file();before=p.read_bytes()
        with patch.object(upload_source,'fetch_prometheus',return_value=self.prom()):found=incidents.operational_reliability_incidents(status_file=p,now_ts=self.now)
        self.assertEqual(found,[]);self.assertEqual(p.read_bytes(),before)
        self.assertEqual(json.loads(p.read_text())['formal_gates']['upload_ceiling']['compliance_status'],'unknown')

    def test_source_failure_is_not_hidden_by_a_previously_met_window(self):
        p=self.status_file(reasons=[],status='met');self.rows([self.now-181])
        found=incidents.operational_reliability_incidents(status_file=p,now_ts=self.now)
        self.assertEqual(found[0]['id'],'reliability:upload_ceiling_coverage_or_freshness')
        self.assertIn('current_source=FAILED',found[0]['evidence'])
        self.assertEqual(found[0]['summary'],'upload current measurement source requires attention')

    def test_previously_met_window_does_not_close_during_recovery_warmup(self):
        p=self.status_file(reasons=[],status='met');self.rows(range(self.now-300,self.now+1,60))
        with patch.object(upload_source,'fetch_prometheus',return_value=self.prom()):
            found=incidents.operational_reliability_incidents(status_file=p,now_ts=self.now)
        self.assertEqual(found[0]['id'],'reliability:upload_ceiling_coverage_or_freshness')
        self.assertIn('current_source=RECOVERING',found[0]['evidence'])

    def test_other_coverage_reasons_and_other_families_keep_incident(self):
        self.stable()
        for reasons,family in [(['minimum_coverage_not_met'],'upload_ceiling'),(['source_freshness_not_met'],'audio_correctness')]:
            p=self.status_file(reasons,family)
            with patch.object(upload_source,'fetch_prometheus',return_value=self.prom()):found=incidents.operational_reliability_incidents(status_file=p,now_ts=self.now)
            self.assertEqual(len(found),1)
            self.assertEqual(found[0]['id'],'reliability:'+family+'_coverage_or_freshness')

if __name__=='__main__':unittest.main()
