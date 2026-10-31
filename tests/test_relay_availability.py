"""Stopped unit evidence, real clock-bound HTTP and rejection boundaries."""
import copy
import json
from pathlib import Path
import threading
import time
import tempfile
import unittest
from unittest.mock import patch

from test_fallback_copy_route import control,clock_model,DESTINATION
import route_facts_http as producer


class RelayAvailabilityTests(unittest.TestCase):
    def fixture(self,directory):
        from test_fallback_copy_route import CollectorBindingTests
        return CollectorBindingTests().fixture(directory)

    def parse(self,value):
        now=time.time();diagnostic={}
        result=control.copy_route_sample(value, machine=value['relay']['machine_id_sha256'], destination=DESTINATION, received=now, diagnostic=diagnostic, source_age=0.0)
        return result,diagnostic

    def test_copy_unit_stop_is_explicit_and_ack_cannot_hide_it(self):
        with tempfile.TemporaryDirectory() as directory:
            module,_=self.fixture(directory)
            with patch.object(producer,'unit_pid',side_effect=[855,producer.UnitNotRunning(),producer.UnitNotRunning(),855]):
                value=producer.collect(module)
            self.assertEqual(value['copy_availability']['state'],'STOPPED')
            sample,diagnostic=self.parse(value);self.assertIsNotNone(sample)
            c=control.Controller()
            for now in (0,15,30):
                c.healthy(sample,diagnostic['reason']);ok=c.combine_transport({'status':'FLOWING'})
                self.assertEqual(c.decide(now,ok,0),'START' if now==30 else None)
                self.assertEqual(c.reason,'copy_unit_not_running')

    def test_wall_step_cannot_hide_a_fresh_nonce_bound_negative_copy_fact(self):
        with tempfile.TemporaryDirectory() as directory:
            module,_=self.fixture(directory);value=producer.unavailable_copy_facts(855,DESTINATION)
            server=producer.FactsServer(('127.0.0.1',0),lambda:copy.deepcopy(value),'127.0.0.1')
            worker=threading.Thread(target=server.serve_forever,daemon=True);worker.start()
            original=producer.clock.envelope
            def stepped(*args,**kwargs):
                result=original(*args,**kwargs);result['sent']['wall']+=10;return result
            try:
                with patch.object(producer.clock,'envelope',side_effect=stepped),patch.multiple(control,
                    FACTS_URL=f'http://127.0.0.1:{server.server_port}/facts',EXPECTED_MACHINE=value['relay']['machine_id_sha256'],
                    EXPECTED_DESTINATION=DESTINATION):
                    diagnostic={};sample=control.observe(diagnostic)
                    self.assertIsNotNone(sample);self.assertEqual(diagnostic['negative_evidence_timing'],'bound_boottime')
                    self.assertEqual(diagnostic['clock']['status'],'UNKNOWN')
                    # A stale source boottime is never rejuvenated by a new nonce.
                    value['relay']['observed_boottime_seconds']-=30
                    self.assertIsNone(control.observe({}))
            finally:server.shutdown();server.server_close();worker.join(timeout=2)

    def test_live_copy_with_unchanged_heartbeat_is_stalled_by_boottime(self):
        with tempfile.TemporaryDirectory() as directory:
            module,replies=self.fixture(directory);watch=producer.CopyHeartbeat()
            for now in (0,20,21,45):
                with patch.object(producer,'unit_pid',side_effect=[855,123,855,123]),patch.object(producer,'api',side_effect=replies),patch.object(producer.clock,'mono',return_value=now):
                    value=producer.collect(module,heartbeat=watch)
                self.assertEqual('copy_availability' in value,now>20)
            self.assertEqual(value['copy_availability']['state'],'STALLED')
            sample,diagnostic=self.parse(value);self.assertIsNotNone(sample)
            c=control.Controller();c.healthy(sample,diagnostic['reason'])
            self.assertEqual(c.reason,'copy_heartbeat_stalled')

    def test_wall_step_does_not_hide_a_fresh_bound_stalled_copy(self):
        with tempfile.TemporaryDirectory() as directory:
            module,replies=self.fixture(directory);watch=producer.CopyHeartbeat()
            for now in (0,45):
                with patch.object(producer,'unit_pid',side_effect=[855,123,855,123]),patch.object(producer,'api',side_effect=replies),patch.object(producer.clock,'mono',return_value=now):
                    value=producer.collect(module,heartbeat=watch)
            self.assertEqual(value['copy_availability']['state'],'STALLED')
            value['relay']['observed_boottime_seconds']=producer.clock.mono()
            server=producer.FactsServer(('127.0.0.1',0),lambda:copy.deepcopy(value),'127.0.0.1')
            worker=threading.Thread(target=server.serve_forever,daemon=True);worker.start()
            original=producer.clock.envelope
            def stepped(*args,**kwargs):
                result=original(*args,**kwargs);result['sent']['wall']+=10;return result
            try:
                with patch.object(producer.clock,'envelope',side_effect=stepped),patch.multiple(control,
                    FACTS_URL=f'http://127.0.0.1:{server.server_port}/facts',EXPECTED_MACHINE=value['relay']['machine_id_sha256'],EXPECTED_DESTINATION=DESTINATION):
                    diagnostic={};sample=control.observe(diagnostic);self.assertIsNotNone(sample)
                    self.assertEqual(diagnostic['negative_evidence_timing'],'bound_boottime')
                    c=control.Controller();c.healthy(sample,diagnostic['reason'])
                    self.assertEqual((c.domain,c.reason),('MEDIA_FAILURE','copy_heartbeat_stalled'))
            finally:server.shutdown();server.server_close();worker.join(timeout=2)


    def test_copy_restart_and_bad_binding_cannot_reuse_heartbeat_evidence(self):
        watch=producer.CopyHeartbeat();row={'schema':'stream_v3.youtube_forward.v1','pid':1,'boot_id':'boot',
            'session':1,'destination_fingerprint':DESTINATION,'updated_at':'fixed','state':'FLOWING',
            'output_bytes':10,'video_packets':1,'audio_packets':1,'last_progress_at':'fixed'}
        watch.observe(row,'owned',0);row['output_bytes']+=1;self.assertTrue(watch.observe(row,'owned',15)['confirmed'])
        row['pid']=2;self.assertFalse(watch.observe(row,'owned',90)['confirmed'])
        self.assertEqual(watch.observe(row,'owned',90)['age_seconds'],0)
        self.assertFalse(producer.bound_copy(row,1,'boot',DESTINATION))

    def test_another_reader_cannot_consume_clock_fault_progress_evidence(self):
        from route_assessment import ProgressDeadline
        with tempfile.TemporaryDirectory() as directory:
            module,replies=self.fixture(directory);watch=producer.CopyHeartbeat();c=control.Controller();deadline=ProgressDeadline()
            row=json.loads(module.YOUTUBE_STATUS.read_text());fixed=control.datetime.fromtimestamp(time.time()+3600,control.timezone.utc).isoformat()
            for n in range(14):
                if n:row['updated_at']=fixed
                for key in ('output_bytes','video_packets','audio_packets'):row[key]+=100
                replies[0]['bytesReceived']+=1000;module.YOUTUBE_STATUS.write_text(json.dumps(row))
                # A recorder fetches first. The controller still sees progress
                # relative to its own previous bound counters.
                for _ in range(2):
                    with patch.object(producer,'unit_pid',side_effect=[855,123,855,123]),patch.object(producer,'api',side_effect=replies),patch.object(producer.clock,'mono',return_value=n*15):
                        value=producer.collect(module,heartbeat=watch)
                self.assertFalse(value['copy_observation']['new_observation'])
                sample,diagnostic=self.parse(value)
                if n==0:self.assertIsNone(sample);continue
                self.assertIsNotNone(sample)
                healthy=c.healthy(sample,diagnostic['reason'])
                if n>1:self.assertEqual(c.domain,'HEALTHY')
                uncertainty=deadline.observe(n * 15, route_progress=healthy or c.route_progress)
                self.assertIsNone(c.decide(n*15,healthy,0,uncertainty=uncertainty))

    def test_copy_stop_start_race_and_permission_errors_are_not_explicit_failures(self):
        with tempfile.TemporaryDirectory() as directory:
            module,_=self.fixture(directory)
            for values,exception in (([855,producer.UnitNotRunning(),123],ValueError),([855,PermissionError()],PermissionError)):
                with patch.object(producer,'unit_pid',side_effect=values):self.assertRaises(exception,producer.collect,module)

    def test_legacy_observer_input_mapping_preserves_the_operated_contract(self):
        self.assertEqual(producer.legacy_input_facts({'ready':True,'source':{'id':'connection'},'inboundBytes':10}),
                         {'available':True,'ready':True,'connection_id':'connection','bytes':10})

    def test_only_absent_or_empty_unit_is_explicitly_stopped(self):
        for response in (FileNotFoundError(), ''):
            with patch.object(Path,'read_text',side_effect=response if isinstance(response,Exception) else None,
                              return_value=response if isinstance(response,str) else ''):
                self.assertRaises(producer.UnitNotRunning,producer.unit_pid,'owned-relay','mediamtx')
        for response,expected in ((PermissionError(),PermissionError),('1 2',ValueError)):
            with patch.object(Path,'read_text',side_effect=response if isinstance(response,Exception) else None,
                              return_value=response if isinstance(response,str) else ''):
                self.assertRaises(expected,producer.unit_pid,'owned-relay','mediamtx')

    def test_real_http_stopped_evidence_uses_nonce_clock_and_starts_after_thirty_seconds(self):
        with patch.object(producer,'unit_pid',side_effect=producer.UnitNotRunning('unit_not_running')):
            server=producer.FactsServer(('127.0.0.1',0),lambda:producer.collect(None),'127.0.0.1')
            worker=threading.Thread(target=server.serve_forever,daemon=True);worker.start()
            try:
                with patch.multiple(control,FACTS_URL=f'http://127.0.0.1:{server.server_port}/facts',
                                    EXPECTED_MACHINE=producer.clock.identity()['machine_id_sha256'],
                                    EXPECTED_DESTINATION=DESTINATION):
                    c=control.Controller()
                    for n in range(3):
                        diagnostic={};sample=control.observe(diagnostic)
                        self.assertEqual(diagnostic['clock']['status'],'VERIFIED')
                        self.assertEqual(c.decide(n*15,c.healthy(sample,diagnostic['reason']),0),'START' if n==2 else None)
                        self.assertEqual(c.reason,'relay_unit_not_running')
            finally:server.shutdown();server.server_close();worker.join(timeout=2)

    def test_source_errors_and_start_race_do_not_fabricate_stopped_evidence(self):
        for values,error in (([PermissionError()],PermissionError),
                             ([producer.UnitNotRunning(),42],ValueError),
                             ([ValueError('unit_process_not_unique')],ValueError)):
            with patch.object(producer,'unit_pid',side_effect=values):
                self.assertRaises(error,producer.collect,None)

    def test_invalid_stopped_facts_still_reject_binding_freshness_and_typed_fields(self):
        original=producer.stopped_relay_facts();machine=original['relay']['machine_id_sha256']
        for change in (lambda v:v['relay'].__setitem__('machine_id_sha256','0'*64),
                       lambda v:v['relay'].__setitem__('monotonic_seconds',float('nan')),
                       lambda v:v['relay']['relay'].__setitem__('active',0),
                       lambda v:v['relay']['input'].__setitem__('bytes',False),
                       lambda v:v['relay_availability'].__setitem__('reason','read_failed')):
            value=copy.deepcopy(original);change(value);now=time.time();diagnostic={}
            self.assertIsNone(control.copy_route_sample(value, machine=machine, destination=DESTINATION, received=0, diagnostic=diagnostic, source_age=0.0))
            self.assertEqual(diagnostic['health'],'UNKNOWN')

    def test_utc_stamp_errors_with_real_record_progress_do_not_block_verified_media_health(self):
        for shift in (-3600,3600):
            with self.subTest(shift=shift),tempfile.TemporaryDirectory() as directory:
                module,replies=self.fixture(directory);watch=producer.CopyHeartbeat();c=control.Controller()
                row=json.loads(module.YOUTUBE_STATUS.read_text())
                stamp=control.datetime.fromtimestamp(time.time()+shift,control.timezone.utc).isoformat()
                for n in range(11):
                    if n:row['updated_at']=stamp
                    for key in ('output_bytes','video_packets','audio_packets'):row[key]+=100
                    replies[0]['bytesReceived']+=1000;module.YOUTUBE_STATUS.write_text(json.dumps(row))
                    with patch.object(producer,'unit_pid',side_effect=[855,123,855,123]),patch.object(producer,'api',side_effect=replies),patch.object(producer.clock,'mono',return_value=n*15):
                        value=producer.collect(module,heartbeat=watch)
                    self.assertNotIn('copy_availability',value)
                    sample,diagnostic=self.parse(value)
                    if n==0:self.assertIsNone(sample);continue
                    self.assertIsNotNone(sample);c.healthy(sample,diagnostic['reason'])
                    if n>=2:self.assertEqual(c.domain,'HEALTHY')


if __name__=='__main__':unittest.main()
