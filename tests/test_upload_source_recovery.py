import json
import os
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from stream_core.notifications import incidents, renderer, state, status_loop, upload_source
from stream_core.common.timeutil import jst_text

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

    def rotate(self):
        previous=self.path.with_name(self.path.name+'.1')
        self.path.replace(previous)
        self.path.write_text('')
        return previous

    def test_copytruncate_keeps_current_source_recovered(self):
        self.stable();self.rotate()
        with patch.object(upload_source,'fetch_prometheus',return_value=self.prom()):
            self.assertEqual(upload_source.current_upload_source(self.root,self.now)['status'],'RECOVERED')

    def test_rotation_overlap_preserves_continuity_and_original_ages(self):
        self.rows(range(self.now-1020,self.now-119,60));self.rotate()
        self.rows(range(self.now-180,self.now+1,60))
        health=upload_source.raw_health(self.path,self.now)
        self.assertTrue(health['sustained']);self.assertEqual(health['latest_age_seconds'],0)

    def test_previous_file_cannot_hide_a_real_gap_or_stale_source(self):
        self.rows([self.now-1020,self.now-960]);self.rotate();self.rows([self.now-30])
        self.assertFalse(upload_source.raw_health(self.path,self.now)['sustained'])
        self.rows([self.now-181]);self.rotate()
        health=upload_source.raw_health(self.path,self.now)
        self.assertTrue(health['available']);self.assertFalse(health['fresh'])
        self.assertEqual(health['latest_age_seconds'],181)

    def test_rotation_does_not_bypass_prometheus_recovery_conditions(self):
        self.stable();self.rotate()
        with patch.object(upload_source,'fetch_prometheus',return_value=self.prom([30]*15+[181])):
            self.assertEqual(upload_source.current_upload_source(self.root,self.now)['status'],'FAILED')
        missing=self.prom();missing['data']['result'][0]['values'].pop(2)
        with patch.object(upload_source,'fetch_prometheus',return_value=missing):
            self.assertEqual(upload_source.current_upload_source(self.root,self.now)['status'],'RECOVERING')

    def test_symlink_current_cannot_fall_back_to_valid_previous_file(self):
        self.stable();previous=self.rotate();self.path.unlink();self.path.symlink_to(previous)
        self.assertFalse(upload_source.raw_health(self.path,self.now)['available'])

    def test_previous_symlink_future_sample_and_fifo_are_rejected(self):
        self.stable();previous=self.rotate();previous.unlink();previous.symlink_to(self.path)
        self.assertFalse(upload_source.raw_health(self.path,self.now)['available'])
        previous.unlink();self.rows([self.now+1]);self.rotate()
        self.assertFalse(upload_source.raw_health(self.path,self.now)['available'])
        self.path.unlink();os.mkfifo(self.path)
        self.assertFalse(upload_source.raw_health(self.path,self.now)['available'])

    def test_large_rotated_file_uses_bounded_tail(self):
        self.stable();previous=self.rotate()
        previous.write_bytes(b'x'*(upload_source.TAIL_BYTES+30)+b'\n'+previous.read_bytes())
        self.assertTrue(upload_source.raw_health(self.path,self.now)['sustained'])

    def test_missing_current_during_rename_uses_previous_original_timestamps(self):
        self.stable();self.rotate();self.path.unlink()
        self.assertTrue(upload_source.raw_health(self.path,self.now)['sustained'])

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

    def test_current_incident_notification_window_uses_first_detection_not_hourly_report(self):
        p=self.status_file(reasons=[],status='met');payload=json.loads(p.read_text())
        payload['checked_at_utc']=datetime.fromtimestamp(self.now-3600,timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
        p.write_text(json.dumps(payload));self.rows([self.now-181])
        state_file=self.root/'notify.json';sent=[]
        ctx=status_loop.NotifyStatusContext(
            notify_events_file=self.root/'events.jsonl',notify_outbox_file=self.root/'outbox.jsonl',
            load_config=lambda:{'enabled':True,'webhook_url':'https://discord.example/test',
                'username':'test','slack_enabled':False,'repeat_sec':3600,'report_stale_sec':1800,
                'outbox_ttl_sec':86400,'outbox_max_pending':50,'outbox_flush_limit':10},
            load_state=lambda:state.load_notify_state(state_file),
            save_state=lambda value:state.save_notify_state(value,state_file),
            collect_incidents=lambda **kw:incidents.operational_reliability_incidents(status_file=p,now_ts=kw['now_ts']),
            recovery_observation_for_incident=lambda _ident,now:(now,'current evidence recovered'),
            format_message=renderer.format_discord_message,
            send_webhook=lambda _url,content,**kw:(sent.append(content) or True,'http_204'),
            send_slack_webhook=lambda *a,**kw:self.fail('unexpected Slack route'),
            maintenance_notification_incident=lambda _:None,
            fast_recovery_events_file=self.root/'fast.jsonl',stream_engine_events_file=self.root/'engine.jsonl',
            stream_watchdog_events_file=self.root/'watchdog.jsonl',runtime_state_base_dir=self.root)
        ident='reliability:upload_ceiling_coverage_or_freshness'
        self.assertEqual(status_loop.notify_status(ctx=ctx,now_ts=self.now),0)
        active=state.load_notify_state(state_file)['active'][ident]
        self.assertEqual(active['first_seen_ts'],self.now)
        self.assertIn(f'window={jst_text(self.now)} -> ongoing duration=0s',sent[0])
        payload['checked_at_utc']=datetime.fromtimestamp(self.now+30,timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
        p.write_text(json.dumps(payload))
        self.assertEqual(status_loop.notify_status(ctx=ctx,now_ts=self.now+60),0)
        active=state.load_notify_state(state_file)['active'][ident]
        self.assertEqual(active['first_seen_ts'],self.now)
        self.assertEqual(active['last_bad_ts'],self.now+60)
        with patch.object(incidents,'current_upload_source',return_value={'status':'RECOVERED','reason':'fixture'}):
            self.assertEqual(status_loop.notify_status(ctx=ctx,now_ts=self.now+120),0)
        self.assertEqual(state.load_notify_state(state_file)['active'],{})
        self.assertIn(f'window={jst_text(self.now)} -> {jst_text(self.now+120)} duration=2m00s',sent[-1])
        self.assertIn(f'last_bad_sample_at={jst_text(self.now+60)}',sent[-1])

    def test_historical_measurement_incident_keeps_report_timestamp(self):
        p=self.status_file(['minimum_coverage_not_met']);self.stable()
        with patch.object(incidents,'current_upload_source',return_value={'status':'RECOVERED','reason':'fixture'}):
            found=incidents.operational_reliability_incidents(status_file=p,now_ts=self.now+60)
        self.assertEqual(found[0]['observed_ts'],self.now)

if __name__=='__main__':unittest.main()
