"""Adversarial transition traces; real HTTP/main-loop cases live alongside these.

The exhaustive model checks deadline/decision composition, not classification.
Bound clock-fault packet classification is exercised separately via real HTTP.
"""
import itertools
import unittest
from test_fallback_copy_route import control,sample
from route_assessment import ProgressDeadline


class TransitionAuditTests(unittest.TestCase):
    def prefix(self):
        d=ProgressDeadline();c=control.Controller()
        for now in range(0,315,15):
            c.domain='CLOCK_FAULT'
            u=d.observe(now, route_progress=True)
            self.assertIsNone(c.decide(now,False,0,uncertainty=u))
        return c,d


    def test_media_failures_and_unknown_labels_cannot_starve_start(self):
        labels=('MEDIA_FAILURE','CLOCK_FAULT','OBSERVATION_LOST','INITIALIZING')
        for pattern in itertools.product(labels,repeat=5):
            c,d=self.prefix();first=None
            for n,label in enumerate(pattern*3):
                now=315+n*15;c.domain=label
                u=d.observe(now)
                if c.decide(now,False,0,uncertainty=u)=='START':first=now;break
            self.assertIsNotNone(first,pattern)
            self.assertLessEqual(first,420,pattern)

    def test_legacy_unknown_labels_with_progress_neither_expire_nor_invent_healthy_credit(self):
        for pattern in itertools.product((False,True),repeat=6):
            flow=(True,*pattern,True)
            # Keep proof opportunities within the real 45-second sample bound.
            if any(not any(flow[n:n+3]) for n in range(len(flow)-2)):continue
            d=ProgressDeadline();c=control.Controller()
            for n in range(45):
                progressed=flow[n%len(flow)];c.domain='CLOCK_FAULT' if progressed else 'OBSERVATION_LOST'
                u=d.observe(n * 15, route_progress=progressed)
                self.assertFalse(u['expired'],pattern)
                self.assertIsNone(c.decide(n*15,False,0,uncertainty=u),pattern)
                self.assertIsNone(c.decide(n*15,False,1,uncertainty=u),pattern)

    def test_local_boottime_gap_blocks_continuity_even_if_remote_monotonic_is_short(self):
        c=control.Controller();before=sample(0);after=sample(1)
        before['received_boottime']=0;after['received_boottime']=100
        c.healthy(before);self.assertFalse(c.healthy(after))
        self.assertEqual(c.reason,'sample_gap');self.assertFalse(c.route_progress)

    def test_media_error_is_not_hidden_behind_a_bad_clock_label(self):
        c=control.Controller();c.healthy(sample(0));value=sample(1)
        value['copy_clock_issue']='copy_future';value['youtube'].update(state='unknown',error=True)
        self.assertFalse(c.healthy(value));self.assertEqual(c.domain,'MEDIA_FAILURE')
        self.assertEqual(c.reason,'output_not_forwarding');self.assertFalse(c.route_progress)

    def test_missing_samples_never_add_healthy_time_while_baseline_is_retained(self):
        c=control.Controller()
        for n in range(30):
            now=n*15
            if n%2:
                ok=c.healthy(None,'facts_timeout')
            else:
                value=sample(n);value['received_boottime']=now;ok=c.healthy(value)
            self.assertIsNone(c.decide(now,ok,1))
        self.assertIsNone(c.healthy_since)

    def test_all_6561_unknown_label_suffixes_share_one_progress_deadline(self):
        for labels in itertools.product(('CLOCK_FAULT','OBSERVATION_LOST','INITIALIZING'),repeat=8):
            c,d=self.prefix();first=None
            for n,label in enumerate(labels):
                now=315+n*15;c.domain=label
                u=d.observe(now)
                if c.decide(now,False,0,uncertainty=u)=='START' and first is None:first=now
            self.assertEqual(first,390,labels)

class ClockSafeHTTPTests(unittest.TestCase):
    def test_bound_media_progress_survives_wall_step_but_replay_and_identity_errors_do_not(self):
        import copy,json,tempfile,threading,time
        from unittest.mock import patch
        from test_fallback_copy_route import CollectorBindingTests,DESTINATION
        import route_facts_http as producer
        with tempfile.TemporaryDirectory() as directory:
            module,replies=CollectorBindingTests().fixture(directory);watch=producer.CopyHeartbeat()
            row=json.loads(module.YOUTUBE_STATUS.read_text());mutation=[None]
            def collect():
                for key in ('output_bytes','video_packets','audio_packets'):row[key]+=100
                replies[0]['bytesReceived']+=1000;module.YOUTUBE_STATUS.write_text(json.dumps(row))
                with patch.object(producer,'unit_pid',side_effect=[855,123,855,123]),patch.object(producer,'api',side_effect=replies):
                    value=producer.collect(module,heartbeat=watch)
                if mutation[0]:mutation[0](value)
                return value
            server=producer.FactsServer(('127.0.0.1',0),collect,'127.0.0.1');thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
            original=producer.clock.envelope;corrupt=[None]
            def stepped(*args,**kwargs):
                result=original(*args,**kwargs);result['sent']['wall']+=10
                if corrupt[0]:corrupt[0](result)
                return result
            try:
                with patch.object(producer.clock,'envelope',side_effect=stepped),patch.multiple(control,
                    FACTS_URL=f'http://127.0.0.1:{server.server_port}/facts',EXPECTED_MACHINE=producer.clock.identity()['machine_id_sha256'],EXPECTED_DESTINATION=DESTINATION):
                    c=control.Controller();d=ProgressDeadline()
                    for n in range(25):
                        diagnostic={};value=control.observe(diagnostic);ok=c.healthy(value,diagnostic['reason'])
                        u=d.observe(n * 15, route_progress=c.route_progress)
                        action=c.decide(n*15,ok,1,uncertainty=u)
                        if n>=22:self.assertEqual(action,'STOP')
                        else:self.assertIsNone(action)
                        if n>=2:
                            self.assertEqual(c.domain,'HEALTHY');self.assertTrue(c.route_progress)
                            self.assertEqual(diagnostic['media_evidence_timing'],'bound_boottime')
                        if n>=2:self.assertIsNotNone(c.healthy_since)
                    self.assertEqual(u['unconfirmed_for_sec'],0)
                    changes=[lambda v:v['relay'].__setitem__('observed_boottime_seconds',producer.clock.mono()-21),
                             lambda v:v['youtube_copy'].__setitem__('boot_id','wrong'),
                             lambda v:v['youtube_copy'].__setitem__('destination_fingerprint','f'*64),
                             lambda v:v['copy_observation'].__setitem__('confirmed',False),
                             lambda v:v['copy_observation'].__setitem__('age_seconds',21)]
                    for change in changes:
                        mutation[0]=change;self.assertIsNone(control.observe({}))
                    mutation[0]=None
                    for change in (lambda p:p.__setitem__('nonce','0'*32),lambda p:p.__setitem__('boot_id','wrong'),lambda p:p.__setitem__('machine_id_sha256','f'*64)):
                        corrupt[0]=change;self.assertIsNone(control.observe({}))
            finally:server.shutdown();server.server_close();thread.join(timeout=2)


class IndependentClockAndMediaTests(unittest.TestCase):
    def test_verified_downstream_progress_outweighs_local_stop_but_missing_proof_does_not(self):
        c=control.Controller();c.healthy(sample(0))
        for n in range(1,25):
            self.assertTrue(c.healthy(sample(n)))
            self.assertTrue(c.combine_transport({'status':'STOPPED','reason':'local_fifo_recovering'}))
            self.assertTrue(c.transport_conflict)
            self.assertIsNone(c.decide(n*15,True,0))
        self.assertFalse(c.healthy(None,'facts_timeout'))
        self.assertFalse(c.combine_transport({'status':'STOPPED','reason':'local_fifo_recovering'}))
        self.assertFalse(c.transport_conflict);self.assertEqual(c.domain,'MEDIA_FAILURE')

    def test_clock_diagnostics_survive_gaps_but_do_not_change_media_stop_window(self):
        from route_assessment import ClockDiagnostics
        d=ClockDiagnostics();c=control.Controller();stops=[]
        for n in range(25):
            now=n*15
            row=d.observe({'clock':{'status':'UNKNOWN'}},now)
            ok=c.healthy(sample(n));action=c.decide(now,ok,1)
            if action:stops.append(now)
        self.assertEqual(stops[0],315)
        self.assertEqual(row,{'status':'FAULT','fault_for_sec':360})
        self.assertEqual(d.observe({},375),{'status':'UNAVAILABLE','fault_for_sec':375})
        self.assertEqual(d.observe({'clock':{'status':'UNKNOWN'}},390)['fault_for_sec'],390)
        self.assertEqual(d.observe({'clock':{'status':'VERIFIED'}},405)['fault_for_sec'],0)

    def test_observation_gaps_break_stop_continuity_even_with_verified_boottime_progress(self):
        c=control.Controller();d=ProgressDeadline()
        for n in range(50):
            value=None if n%3==2 else sample(n)
            ok=c.healthy(value,'facts_timeout')
            u=d.observe(n*15,route_progress=c.route_progress)
            self.assertIsNone(c.decide(n*15,ok,1,uncertainty=u))
            self.assertFalse(u['expired'])
