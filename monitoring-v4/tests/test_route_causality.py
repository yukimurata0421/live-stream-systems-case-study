import dataclasses
import json
from pathlib import Path
import tempfile
import types
import unittest
from unittest.mock import Mock,patch
from stream_monitoring_v4.compatibility import route_causality as cause


class CausalGuardTests(unittest.TestCase):
    def test_notification_context_requires_fresh_projection_and_verified_ack_hold(self):
        value={'schema':'stream_v3.fallback_controller.v1','assessment':'CLOCK_FAULT','reason':'clock_unavailable',
               'fallback_replicas':0,'causality':{'cause_id':'a'*32,'cause_active':True,
               'assessment':'CLOCK_FAULT','reason':'clock_unavailable','fallback_active':False,
               'transport_witness':{'status':'FLOWING'},'progress_deadline':{
               'clock_hold_for_sec':300,'last_ack_progress_age_sec':0}}}
        projected=cause.validate_source(value)
        self.assertEqual(projected['clock_observation_hold_sec'],300)
        with tempfile.TemporaryDirectory() as d:
            p=Path(d)/'projection.json'
            p.write_text(json.dumps({**projected,'schema':'stream.route_causality.v1',
                'boot_id':'b','status':'VERIFIED','received_mono':100,'source_age_upper_seconds':1}))
            self.assertEqual(cause.notification_context(p,now=101,boot='b')['clock_observation_hold_sec'],300)
            self.assertEqual(cause.notification_context(p,now=150,boot='b')['status'],'UNKNOWN')
            self.assertEqual(cause.notification_context(p,now=101,boot='new')['status'],'UNKNOWN')
        for bad in (None,{'clock_hold_for_sec':float('nan')},
                    {'clock_hold_for_sec':300,'last_ack_progress_age_sec':46}):
            value['causality']['progress_deadline']=bad
            self.assertEqual(cause.validate_source(value)['clock_observation_hold_sec'],0)

    def test_clock_safe_route_progress_can_support_hold_without_local_ack_export(self):
        value={'schema':'stream_v3.fallback_controller.v1','assessment':'CLOCK_FAULT','reason':'clock_unavailable',
            'fallback_replicas':0,'causality':{'cause_id':'a'*32,'cause_active':True,'assessment':'CLOCK_FAULT',
            'reason':'clock_unavailable','fallback_active':False,'transport_witness':{'status':'UNKNOWN'},
            'progress_deadline':{'clock_hold_for_sec':300,'route_progress_confirmed':True,
                'last_route_progress_age_sec':0,'last_ack_progress_age_sec':None}}}
        self.assertEqual(cause.validate_source(value)['clock_observation_hold_sec'],300)
        for changed in ({'route_progress_confirmed':False},{'route_progress_confirmed':1},
                        {'last_route_progress_age_sec':46},{'last_route_progress_age_sec':float('nan')}):
            saved=value['causality']['progress_deadline'].copy()
            value['causality']['progress_deadline'].update(changed)
            self.assertEqual(cause.validate_source(value)['clock_observation_hold_sec'],0)
            value['causality']['progress_deadline']=saved

    def test_missing_stale_reboot_context_is_a_veto(self):
        with tempfile.TemporaryDirectory() as d:
            p=Path(d)/'c.json';self.assertTrue(cause.guard(p,now=10,boot='b')['secondary_recovery_blocked'])
            value={'schema':'stream.route_causality.v1','boot_id':'b','received_mono':10,'status':'VERIFIED',
                'cause_id':None,'cause_active':False,'fallback_active':False,'assessment':'HEALTHY','reason':'progress_confirmed',
                'secondary_recovery_blocked':False,'source_age_upper_seconds':1}
            p.write_text(json.dumps(value));self.assertFalse(cause.guard(p,now=11,boot='b')['secondary_recovery_blocked'])
            self.assertTrue(cause.guard(p,now=60,boot='b')['secondary_recovery_blocked'])
            self.assertTrue(cause.guard(p,now=11,boot='new')['secondary_recovery_blocked'])
            value['assessment']='CLOCK_FAULT';p.write_text(json.dumps(value))
            self.assertTrue(cause.guard(p,now=11,boot='b')['secondary_recovery_blocked'])

    def test_clock_free_source_age_rejects_replay_and_boot_change(self):
        c=cause.clock;nonce='a'*32;machine='b'*64
        packet={'schema':c.SCHEMA,'nonce':nonce,'machine_id_sha256':machine,'boot_id':'source',
            'received':{'mono':100,'wall':600},'sent':{'mono':101,'wall':0}}
        args=dict(machine=machine,boot_id='source',source_mono=99)
        self.assertEqual(c.monotonic_source_age(packet,nonce,{'mono':10,'wall':100}, {'mono':12,'wall':-500},**args),4)
        for bad in ({'source_mono':0},{'boot_id':'new'},{'source_mono':float('nan')}):
            with self.assertRaises(c.ClockRejected):c.monotonic_source_age(packet,nonce,{'mono':10},{'mono':12},**{**args,**bad})
        with self.assertRaises(c.ClockRejected):c.monotonic_source_age(packet,'c'*32,{'mono':10},{'mono':12},**args)

    def test_enabled_legacy_effects_cannot_reserve_budget_or_mutate(self):
        @dataclasses.dataclass(frozen=True)
        class Decision:
            action:str='restart_stream'
            reason:str='warning'
            blocked_by:tuple=()
        yt=types.SimpleNamespace(decide_action=Mock(return_value=Decision()),force_transition_live_once=Mock(),
                                 restart_stream=Mock(),record_status=Mock())
        wd=types.SimpleNamespace(allow_restart=Mock(),log=Mock(),record_watchdog_stats=Mock())
        block={'secondary_recovery_blocked':True,'cause_id':'a'*32,'reason':'clock_unavailable'}
        with patch.object(cause,'guard',return_value=block):
            cause.install_hooks(yt,'youtube_monitor');cause.install_hooks(wd,'stream_watchdog')
            self.assertEqual(yt.decide_action().action,'none')
            self.assertFalse(yt.force_transition_live_once()[0]);self.assertFalse(yt.restart_stream('bad')[0])
            self.assertFalse(wd.allow_restart('stream','bad'))
            yt.record_status({'raw_warning':'bitrateLow'})
        self.assertEqual(yt.record_status.__closure__[0].cell_contents.call_args[0][0]['raw_warning'],'bitrateLow')
        self.assertEqual(cause.gate_action(Decision(action='alert')).action,'alert')


if __name__=='__main__':unittest.main()


class ClockDiagnosticSeparationTests(unittest.TestCase):
    def source(self,status='FAULT',age=300):
        return {'schema':'stream_v3.fallback_controller.v1','assessment':'HEALTHY','reason':'progress_confirmed',
            'fallback_replicas':0,'clock_diagnostic':{'status':status,'fault_for_sec':age},
            'causality':{'cause_id':None,'cause_active':False,'assessment':'HEALTHY',
                         'reason':'progress_confirmed','fallback_active':False}}

    def test_media_health_cannot_remove_arena_veto_while_utc_diagnostics_fail(self):
        for state in ('FAULT','UNAVAILABLE'):
            out=cause.validate_source(self.source(state))
            self.assertTrue(out['secondary_recovery_blocked'])
            self.assertEqual(out['assessment'],'HEALTHY')
        self.assertFalse(cause.validate_source(self.source('VERIFIED',0))['secondary_recovery_blocked'])

    def test_bad_duration_or_state_is_rejected(self):
        for state,age in [('FAULT',True),('FAULT',float('nan')),('FAULT',-1),('invalid',300),('VERIFIED',300)]:
            with self.assertRaises(ValueError):cause.validate_source(self.source(state,age))

    def test_notification_reads_new_diagnostics_and_guard_cannot_be_unblocked_by_media_health(self):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'projection.json'
            value={**cause.validate_source(self.source()),'schema':'stream.route_causality.v1',
                'boot_id':'b','status':'VERIFIED','received_mono':100,'source_age_upper_seconds':1}
            path.write_text(json.dumps(value))
            self.assertEqual(cause.notification_context(path,now=101,boot='b')['clock_diagnostic']['fault_for_sec'],300)
            value['secondary_recovery_blocked']=False;path.write_text(json.dumps(value))
            self.assertEqual(cause.guard(path,now=101,boot='b')['status'],'UNKNOWN')
            value['clock_diagnostic']={'status':'VERIFIED','fault_for_sec':True}
            path.write_text(json.dumps(value))
            self.assertEqual(cause.guard(path,now=101,boot='b')['status'],'UNKNOWN')
