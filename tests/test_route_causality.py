"""Failure classification, independent transport, persistent causal identity."""
import copy
import json
from pathlib import Path
import tempfile
import types
import unittest
from unittest.mock import patch
from test_fallback_copy_route import control, facts, clock_model, MACHINE, DESTINATION, NOW
from route_assessment import CauseBook, TransportWitness, ProgressDeadline


class PolicyTests(unittest.TestCase):
    def test_real_watcher_entrypoints_veto_before_budget_and_effect(self):
        from watchers import route_guard
        from watchers.decision.action_gate import decide_action
        from watchers.local_health.actions import restart_service,RestartActionContext
        from watchers.youtube_lifecycle.actions import restart_stream,force_transition_live_once
        from unittest.mock import Mock
        # This is the adapter's real missing-context path, rather than a fake gate.
        with patch.dict('os.environ',{'STREAM_ROUTE_CAUSALITY_MODULE':'/missing/guard.py'}),patch.object(route_guard,'_module',None):
            result=decide_action(types.SimpleNamespace(state='local_unhealthy'),None,None,None)
            self.assertEqual(result.blocked_by,('route_causal_guard',))
            budget,effect,reason=Mock(),Mock(),Mock()
            self.assertFalse(restart_service(RestartActionContext('stream.service','stream','warning'),
                allow_restart=budget,append_event=Mock(),write_restart_reason=reason,run_systemctl=effect,log=Mock()))
            self.assertFalse(restart_stream(reason='warning',stream_service='stream.service',
                write_restart_reason=reason,run_systemctl=effect,log=Mock())[0])
            self.assertFalse(force_transition_live_once(True)[0])
            budget.assert_not_called();effect.assert_not_called();reason.assert_not_called()

    def test_clock_and_observation_faults_without_expired_deadline_never_change_replicas(self):
        for reason in ('clock_unavailable','source_future','facts_timeout','copy_stale','facts_invalid'):
            for replicas in (0,1):
                c=control.Controller()
                for now in range(0,1200,15):
                    self.assertIsNone(c.decide(now,c.healthy(None,reason),replicas))
                self.assertIsNone(c.unhealthy_since)

    def test_progress_deadline_survives_generations_and_never_compares_their_counters(self):
        d=ProgressDeadline();w=TransportWitness()
        row={'schema':'stream_v3.ffmpeg_transport.v1','ffmpeg_generation':'a',
             'observed_at':'0','ack_bytes_observed_total':0,'state':'SOCKET_UNAVAILABLE','tcp_metrics':{}}
        for n in range(8):
            row.update(ffmpeg_generation=str(n),observed_at=str(n),ack_bytes_observed_total=n*100)
            value=d.observe(n * 15)
            self.assertEqual(value['expired'],n>=6)
            self.assertNotIn('last_ack_progress_age_sec',value)

    def test_verified_relay_health_and_planned_maintenance_reset_uncertainty(self):
        d=ProgressDeadline();unknown={'status':'UNKNOWN'}
        d.observe(0)
        self.assertFalse(d.observe(90, route_progress=True)['expired'])
        self.assertFalse(d.observe(165)['expired'])
        self.assertFalse(d.observe(180, inhibited=True)['expired'])
        self.assertFalse(d.observe(255)['expired'])
        self.assertTrue(d.observe(270)['expired'])

    def test_verified_media_failure_still_starts_and_clock_fault_breaks_window(self):
        c=control.Controller()
        for now in (0,15,30):
            bad={'ready':False};self.assertEqual(c.decide(now,c.healthy(bad),0),'START' if now==30 else None)
        c.healthy(None,'clock_unavailable');c.decide(45,False,0)
        self.assertIsNone(c.decide(60,c.healthy({'ready':False}),0))

    def test_confirmed_failure_after_clock_hold_keeps_thirty_second_window(self):
        d=ProgressDeadline();c=control.Controller();flow={'status':'FLOWING'}
        for now in range(0,315,15):
            ok=c.healthy(None,'clock_unavailable')
            self.assertIsNone(c.decide(now,ok,0,uncertainty=d.observe(now, route_progress=True)))
        for now in (315,330,345):
            ok=c.healthy({'ready':False})
            action=c.decide(now,ok,0,uncertainty=d.observe(now))
            self.assertEqual(action,'START' if now==345 else None)
        value=d.observe(360, route_progress=True)
        self.assertEqual(value['unconfirmed_for_sec'],0)
        self.assertNotIn('youtube_grace_active',value)

    def test_frozen_copy_is_unknown_but_fresh_frozen_counters_are_failure_after_progress_stales(self):
        for frozen in (True,False):
            c=control.Controller()
            for n in range(5):
                now=NOW+n*15;row=facts(n)
                row['relay']['observed_at']=control.datetime.fromtimestamp(now-1,control.timezone.utc).isoformat()
                row['youtube_copy']['updated_at']=control.datetime.fromtimestamp(NOW-1 if frozen else now-1,control.timezone.utc).isoformat()
                row['youtube_copy']['output_bytes']=1000
                if frozen:
                    row['copy_observation'].update(age_seconds=n*15, new_observation=False)
                    row['youtube_copy'].update(video_packets=1,audio_packets=1)
                diagnostic={}
                value=control.copy_route_sample(row, machine=MACHINE, destination=DESTINATION, received=n * 15, diagnostic=diagnostic, source_age=0.0)
                action=c.decide(n*15,c.healthy(value,diagnostic['reason']),0)
                if frozen:self.assertIsNone(action)
                elif n>=3:self.assertEqual(action,'START')

    def test_counter_regression_is_observation_failure(self):
        from test_fallback_copy_route import sample
        c=control.Controller();c.healthy(sample(1));c.healthy(sample(0))
        self.assertEqual(c.domain,'OBSERVATION_LOST')

    def test_independent_transport_can_confirm_failure_during_clock_loss(self):
        c=control.Controller();w=TransportWitness()
        for n in range(5):
            row={'schema':'stream_v3.ffmpeg_transport.v1','ffmpeg_generation':'opaque-container-pid',
                 'observed_at':str(n),'ack_bytes_observed_total':500,'state':'TCP_ACK_STALLED',
                 'tcp_metrics':{'notsent':12}}
            c.healthy(None,'clock_unavailable');ok=c.combine_transport(w.sample(row,n*15))
            self.assertEqual(c.decide(n*15,ok,0),'START' if n>=3 else None)
        # Recopying old bytes cannot extend a failure window.
        c.healthy(None,'clock_unavailable');ok=c.combine_transport(w.sample(row,75))
        self.assertIsNone(c.decide(75,ok,0))

    def test_witness_missing_socket_reboot_and_same_timestamp_never_prove_failure(self):
        w=TransportWitness();row={'schema':'stream_v3.ffmpeg_transport.v1','ffmpeg_generation':'g1',
            'observed_at':'old','ack_bytes_observed_total':10,'state':'SOCKET_UNAVAILABLE','tcp_metrics':{}}
        self.assertEqual(w.sample(row,0)['status'],'UNKNOWN')
        row['observed_at']='new';self.assertEqual(w.sample(row,15)['status'],'UNKNOWN')
        row.update(fifo_recovering=True);self.assertEqual(w.sample(row,30)['status'],'UNKNOWN')
        row.update(observed_at='third',ffmpeg_generation='g2');self.assertEqual(w.sample(row,45)['status'],'UNKNOWN')

    def test_cause_identity_survives_restart_and_settles_only_healthy_with_backup_off(self):
        with tempfile.TemporaryDirectory() as d:
            p=Path(d)/'current.json';book=CauseBook(p,'boot1')
            first=book.update('CLOCK_FAULT','clock_unavailable',0,0)['cause_id']
            book=CauseBook(p,'boot2');self.assertEqual(book.update('MEDIA_FAILURE','output_not_advancing',1,30)['cause_id'],first)
            self.assertTrue(book.update('HEALTHY','progress_confirmed',1,300)['secondary_recovery_blocked'])
            self.assertTrue(book.update('HEALTHY','progress_confirmed',0,600)['secondary_recovery_blocked'])
            self.assertIsNone(book.update('HEALTHY','progress_confirmed',0,780)['cause_id'])
            p.write_text('{bad');self.assertRaises(ValueError,CauseBook,p,'boot2')

    def test_durable_write_failure_precedes_action(self):
        with tempfile.TemporaryDirectory() as d:
            book=CauseBook(Path(d)/'missing/current.json','boot')
            self.assertRaises(OSError,book.update,'MEDIA_FAILURE','output_not_advancing',0,0)


if __name__=='__main__':unittest.main()
