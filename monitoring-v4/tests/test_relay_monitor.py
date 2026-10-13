import copy
import importlib.util
import pathlib
import unittest
from datetime import datetime,timezone

SOURCE=pathlib.Path(__file__).resolve().parents[1]/'src/stream_monitoring_v4/compatibility/relay_monitor.py'
spec=importlib.util.spec_from_file_location('relay_monitor',SOURCE);monitor=importlib.util.module_from_spec(spec);spec.loader.exec_module(monitor)


class RelayMonitorTests(unittest.TestCase):
    def setUp(self):
        self.now=1791884000.;self.date=lambda t:datetime.fromtimestamp(t,timezone.utc).isoformat()
        self.raw={'schema':'stream.relay_raw.v1','host_id':'relay-host','machine_id_sha256':'fixture-machine','boot_id':'boot',
                  'observed_at':self.date(self.now),'monotonic_seconds':200.,'vm':{'cpu_percent':2.,'memory_total_bytes':1000,'memory_used_bytes':200,'disk_total_bytes':10000,'disk_available_bytes':8000,'uptime_seconds':200},
                  'relay':{'active':True,'pid':123,'restart_count':0},'input':{'available':True,'ready':True,'bytes':38000000,'connection_id':'input'},
                  'destinations':{n:{'id':n,'state':'forwarding','bytes':38000000} for n in ['youtube','twitch']},
                  'daily':{'state':'TRACKING','observed_at':self.date(self.now),'next_renewal_at':self.date(self.now+10000),'offline_wait_seconds':120}}
        self.before=copy.deepcopy(self.raw);self.before['monotonic_seconds']=140
        self.before['input']['bytes']=500000
        for row in self.before['destinations'].values():row['bytes']=500000
        self.api={'schema':'monitoring_v4.twitch_api_evidence.v1','source_host_id':'arena-monitoring-facts','collector_revision':'af3e63b2a45810ef81bd6567c5ca41ee9e54d3e437cae4b997900e044fa9a578',
                  'user_id':'100000001','probe_status':'ok','http_status':200,'collected_at':self.date(self.now),'valid_until':self.date(self.now+180),'provider_state':'LIVE'}

    def project(self):return monitor.project(self.raw,self.before,self.api,self.now,'fixture-machine',{'user_id':'100000001','collector_revision':'af3e63b2a45810ef81bd6567c5ca41ee9e54d3e437cae4b997900e044fa9a578'})

    def test_missing_or_drifted_api_binding_is_unknown(self):
        unbound=monitor.project(self.raw,self.before,self.api,self.now,'fixture-machine')
        self.assertEqual(unbound['twitch_api']['state'],'UNKNOWN')
        self.api['collector_revision']='0'*64
        self.assertEqual(self.project()['twitch_api']['state'],'UNKNOWN')
        self.api['collector_revision']='af3e63b2a45810ef81bd6567c5ca41ee9e54d3e437cae4b997900e044fa9a578'
        self.api['user_id']='100000002'
        self.assertEqual(self.project()['twitch_api']['state'],'UNKNOWN')

    def test_rates_and_public_allowlist_do_not_include_private_fields(self):
        self.raw['secret']='fixture-private-value';self.raw['destinations']['twitch']['conf']={'url':'fixture-private-value'}
        v=self.project();self.assertEqual(v['destinations']['twitch']['mbps'],5)
        self.assertEqual(v['input']['status'],'FLOWING');self.assertEqual(v['issues'],[])
        self.assertNotIn('fixture-private-value',str(v));self.assertNotIn('boot_id',str(v))

    def test_stale_and_wrong_machine_observations_are_unknown(self):
        self.raw['observed_at']=self.date(self.now-181);self.assertFalse(self.project()['source_fresh'])
        self.raw['observed_at']=self.date(self.now);self.raw['machine_id_sha256']='wrong'
        self.assertEqual(self.project()['vm']['status'],'UNKNOWN')

    def test_counter_reset_boot_change_and_changed_session_do_not_invent_rates(self):
        self.raw['destinations']['twitch']['bytes']=1;self.assertIsNone(self.project()['destinations']['twitch']['mbps'])
        self.raw['boot_id']='new-boot';self.assertIsNone(self.project()['input']['mbps'])
        self.raw['boot_id']='boot';self.raw['destinations']['twitch']['id']='new-id'
        self.assertIsNone(self.project()['destinations']['twitch']['mbps'])

    def test_zero_counter_progress_is_a_stall(self):
        self.raw['destinations']['twitch']['bytes']=500000
        self.assertEqual(self.project()['destinations']['twitch']['status'],'STALLED')
        self.assertIn('TWITCH_NOT_PROGRESSING',self.project()['issues'])

    def test_only_actual_bounded_daily_phase_suppresses_twitch_issues(self):
        self.api['provider_state']='OFFLINE';self.raw['destinations']['twitch']['state']='idle'
        self.raw['daily']['next_renewal_at']=self.date(self.now-30)
        self.assertIn('TWITCH_OFFLINE',self.project()['issues'])
        self.raw['daily']['state']='WAIT_OFFLINE';v=self.project()
        self.assertTrue(v['daily']['planned_break']);self.assertNotIn('TWITCH_OFFLINE',v['issues'])
        self.raw['destinations']['youtube']['state']='idle'
        self.assertIn('YOUTUBE_NOT_PROGRESSING',self.project()['issues'])
        self.raw['daily']['next_renewal_at']=self.date(self.now-361)
        self.assertIn('TWITCH_OFFLINE',self.project()['issues'])

    def test_stale_api_and_unknown_api_are_not_offline(self):
        self.api['collected_at']=self.date(self.now-181);v=self.project()
        self.assertEqual(v['twitch_api']['state'],'UNKNOWN');self.assertNotIn('TWITCH_OFFLINE',v['issues'])
        self.api['collected_at']=self.date(self.now);self.api['probe_status']='unknown';self.api['provider_state']='UNKNOWN'
        self.assertIn('TWITCH_API_UNAVAILABLE',self.project()['issues'])

    def test_resource_limits_and_metrics_do_not_hide_unknown(self):
        self.raw['vm']['memory_used_bytes']=950;self.raw['vm']['disk_available_bytes']=500
        v=self.project();self.assertIn('VM_MEMORY_HIGH',v['issues']);self.assertIn('VM_DISK_LOW',v['issues'])
        text=monitor.metrics(v);self.assertIn('provider="youtube"',text)
        self.assertNotIn('fixture-machine',text)


if __name__=='__main__':unittest.main()
