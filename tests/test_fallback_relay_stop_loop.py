"""Actual main loop, HTTP 503, atomic transport files and an owned CLI effect.

The boottime/sleep dependency is controlled; these are deterministic integration
regressions, not real-time relay-stop or provider/player measurements.
"""
import importlib.util
import json
import os
from pathlib import Path
import sys
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
from types import SimpleNamespace
from urllib.parse import parse_qs,urlsplit
import unittest
from unittest.mock import patch
from test_fallback_copy_route import facts as route_facts

SOURCE=Path(__file__).resolve().parents[1]/'ops/selfheal'
sys.path.insert(0,str(SOURCE))
spec=importlib.util.spec_from_file_location('relay_stop_controller_main',SOURCE/'fallback_controller.py')
control=importlib.util.module_from_spec(spec);sys.modules[spec.name]=control;spec.loader.exec_module(control)


class EndOfTrace(Exception):pass


class MainLoopRelayStopTests(unittest.TestCase):
    def exercise(self,rows,*,clock_fault=False,copy_missing=False,facts_states=None):
        class FactsUnavailable(BaseHTTPRequestHandler):
            def do_GET(self):
                mode=(facts_states[index[0]] if facts_states is not None else
                      'CLOCK_FAULT' if clock_fault else 'COPY_MISSING' if copy_missing else 'UNAVAILABLE')
                if mode in ('CLOCK_FAULT','CLOCK_COPY_MISSING','CLOCK_EMPTY','COPY_MISSING','HEALTHY_FLOW'):
                    nonce=parse_qs(urlsplit(self.path).query)['nonce'][0]
                    envelope=control.clock.envelope(nonce,control.clock.capture())
                    if mode.startswith('CLOCK_'):envelope['sent']['wall']+=10
                    if mode!='CLOCK_EMPTY':
                        body=route_facts(index[0],stamp=control.datetime.now(control.timezone.utc).isoformat())
                        body['relay']['machine_id_sha256']=control.clock.identity()['machine_id_sha256']
                        body['relay']['boot_id']=control.clock.identity()['boot_id']
                        body['relay']['observed_boottime_seconds']=control.clock.mono()
                        body['youtube_copy']['boot_id']=body['relay']['boot_id']
                        body['copy_observation']={'schema':'stream_v3.copy_observation.v1','age_seconds':0.,'confirmed':True,'new_observation':True}
                        if mode in ('COPY_MISSING','CLOCK_COPY_MISSING'):body['youtube_copy']=None
                        body['clock']=envelope
                    else:body={'clock':envelope,'relay':{'boot_id':control.clock.identity()['boot_id']}}
                    self.send_response(200);self.end_headers();self.wfile.write(json.dumps(body).encode())
                else:
                    self.send_response(503);self.end_headers();self.wfile.write(b'{"error":"facts_unavailable"}')
            def log_message(self,*_):pass
        server=ThreadingHTTPServer(('127.0.0.1',0),FactsUnavailable)
        worker=threading.Thread(target=server.serve_forever,daemon=True);worker.start()
        try:
            with tempfile.TemporaryDirectory() as d:
                root=Path(d);binary=root/'kubectl';state=root/'replicas';state.write_text('0')
                actions=root/'actions.jsonl';now_file=root/'now';transport=root/'transport.json';statuses=[];now=[0.];index=[0]
                binary.write_text('''#!'''+sys.executable+'''
import json,os,pathlib,sys
args=sys.argv[1:];assert args[:2]==['-n','stream-v3'] and args[3:5]==['deploy','isolated-backup']
state=pathlib.Path(os.environ['LAB_REPLICAS'])
if args[2]=='get':print(state.read_text())
elif args[2]=='scale':
 assert args[5:]==['--replicas=1']
 with pathlib.Path(os.environ['LAB_ACTIONS']).open('a') as f:f.write(json.dumps({'at':float(pathlib.Path(os.environ['LAB_NOW']).read_text()),'replicas':1})+'\\n')
 state.write_text('1')
else:raise SystemExit('unexpected effect')
''');binary.chmod(0o755)
                def advance():
                    value=rows[index[0]]
                    if value is None:transport.unlink(missing_ok=True)
                    else:
                        tmp=transport.with_suffix('.tmp');tmp.write_text(json.dumps(value));os.replace(tmp,transport)
                    now_file.write_text(str(now[0]))
                advance()
                def sleep(seconds):
                    self.assertEqual(seconds,15)
                    statuses.append(json.loads((root/'status.json').read_text()))
                    index[0]+=1;now[0]+=seconds
                    if index[0]>=len(rows):raise EndOfTrace()
                    advance()
                env={'PATH':str(root)+os.pathsep+os.environ.get('PATH',''),
                     'LAB_REPLICAS':str(state),'LAB_ACTIONS':str(actions),'LAB_NOW':str(now_file)}
                with patch.dict(os.environ,env),patch.object(sys,'argv',['controller']), \
                     patch.object(control,'FACTS_URL',f'http://127.0.0.1:{server.server_port}/facts'), \
                     patch.object(control,'EXPECTED_MACHINE',control.clock.identity()['machine_id_sha256']), \
                     patch.object(control,'EXPECTED_DESTINATION','b'*64), \
                     patch.object(control,'FALLBACK','isolated-backup'), \
                     patch.object(control,'TRANSPORT_STATE',transport), \
                     patch.object(control,'STATUS_PATH',root/'status.json'), \
                     patch.object(control,'CAUSE_STATE',root/'cause.json'), \
                     patch.object(control,'MAINTENANCE_ROOT',root/'maintenance'), \
                     patch.object(control.clock,'mono',side_effect=lambda:now[0]), \
                     patch.object(control,'time',SimpleNamespace(time=time.time,monotonic=lambda:now[0],sleep=sleep)):
                    with self.assertRaises(EndOfTrace):control.main()
                events=[] if not actions.exists() else [json.loads(line) for line in actions.read_text().splitlines()]
                if facts_states is None:
                    if clock_fault:self.assertTrue(all(s['clock']['status']=='UNKNOWN' for s in statuses))
                    elif not copy_missing:self.assertTrue(all(s['http_status']==503 for s in statuses))
                self.assertTrue(all(s['start_after_sec']==30 for s in statuses))
                return events,statuses
        finally:server.shutdown();server.server_close();worker.join(timeout=2)

    def rows(self,kind,count=9):
        out=[]
        for n in range(count):
            generation='stable'
            if kind=='churn':generation=str(n)
            if kind=='one-restart':generation='old' if n==0 else 'new'
            if kind=='missing':out.append(None);continue
            ack=100
            if kind in ('flow','flow-frozen-clock'):ack+=n*100
            if kind=='flow-then-stall':ack+=min(n,2)*100
            out.append({'schema':'stream_v3.ffmpeg_transport.v1','ffmpeg_generation':generation,
                        'observed_at':'fixed' if kind in ('frozen','flow-frozen-clock') else f'observation-{n}',
                        'ack_bytes_observed_total':ack,'state':'SOCKET_UNAVAILABLE',
                        'fifo_recovering':kind not in ('socket-only','flow','flow-frozen-clock','flow-then-stall'),'tcp_metrics':{}})
        return out

    def test_503_with_same_generation_fifo_failure_dispatches_once_after_full_window(self):
        events,status=self.exercise(self.rows('fifo'))
        self.assertEqual(events,[{'at':45.,'replicas':1}])
        self.assertEqual(status[3]['reason'],'local_fifo_recovering')
        self.assertEqual(status[3]['unhealthy_for_sec'],30)
        self.assertEqual(status[-1]['fallback_replicas'],1)

    def test_503_with_frozen_missing_socket_or_churning_generation_dispatches_at_uncertainty_deadline(self):
        for kind in ('frozen','missing','socket-only','churn'):
            with self.subTest(kind=kind):
                events,status=self.exercise(self.rows(kind))
                self.assertEqual(events,[{'at':90.,'replicas':1}])
                self.assertTrue(all(s['assessment']=='OBSERVATION_LOST' for s in status))
                self.assertTrue(all(s['unhealthy_for_sec']==0 for s in status))
                self.assertFalse(status[5]['automatic_switch_eligible'])
                self.assertTrue(status[6]['automatic_switch_eligible'])
                self.assertEqual(status[6]['reason'],'youtube_observation_timeout')

    def test_only_clock_fault_with_ack_holds_generic_facts_loss_starts(self):
        for clock_fault in (False,True):
            with self.subTest(clock_fault=clock_fault):
                events,status=self.exercise(self.rows('flow',count=11),clock_fault=clock_fault)
                self.assertEqual(events,[] if clock_fault else [{'at':90.,'replicas':1}])
                self.assertTrue(all(s['assessment']==('HEALTHY' if clock_fault else 'OBSERVATION_LOST') for s in status[1:]))
                if clock_fault:self.assertTrue(all(not s['automatic_switch_eligible'] for s in status))

    def test_ack_progress_does_not_renew_youtube_uncertainty(self):
        events,status=self.exercise(self.rows('flow-then-stall',count=11))
        self.assertEqual(events,[{'at':90.,'replicas':1}])
        self.assertEqual(status[7]['causality']['progress_deadline']['unconfirmed_for_sec'],105)

    def test_copy_missing_with_ack_flowing_cannot_hold_the_route_for_600_seconds(self):
        events,status=self.exercise(self.rows('flow',count=41),copy_missing=True)
        self.assertEqual(events,[{'at':90.,'replicas':1}])
        self.assertEqual(status[5]['reason'],'copy_facts_missing')
        self.assertEqual(status[6]['reason'],'youtube_observation_timeout')
        self.assertEqual(status[-1]['causality']['progress_deadline']['unconfirmed_for_sec'],600)

    def test_one_503_after_long_clock_hold_does_not_start_or_reset_hold_duration(self):
        modes=['CLOCK_FAULT']*21+['UNAVAILABLE']+['CLOCK_FAULT']*20
        events,status=self.exercise(self.rows('flow',count=len(modes)),facts_states=modes)
        self.assertEqual(events,[])
        self.assertEqual(status[21]['assessment'],'OBSERVATION_LOST')
        before=status[20]['clock_diagnostic']['fault_for_sec']
        during=status[21]['clock_diagnostic']['fault_for_sec']
        after=status[22]['clock_diagnostic']['fault_for_sec']
        self.assertEqual((during-before,after-during),(15,15))
        self.assertEqual(status[21]['causality']['progress_deadline']['unconfirmed_for_sec'],15)


    def test_brief_gaps_separated_by_clock_responses_do_not_reuse_a_previous_expired_grace(self):
        modes=['CLOCK_FAULT']*21+['UNAVAILABLE','CLOCK_FAULT']*15
        events,status=self.exercise(self.rows('flow',count=len(modes)),facts_states=modes)
        self.assertEqual(events,[])
        self.assertGreater(status[-1]['clock_diagnostic']['fault_for_sec'],600)

    def test_clock_labels_and_missing_copy_facts_cannot_renew_the_deadline(self):
        modes=['CLOCK_COPY_MISSING','UNAVAILABLE']*21
        events,status=self.exercise(self.rows('flow',count=len(modes)),facts_states=modes)
        self.assertEqual(events,[{'at':90.,'replicas':1}])

    def test_real_downstream_progress_during_clock_fault_does_not_require_local_ack_export(self):
        events,status=self.exercise(self.rows('missing',count=41),clock_fault=True)
        self.assertEqual(events,[])

    def test_alternating_clock_safe_progress_and_missing_samples_can_share_a_bounded_baseline(self):
        modes=['CLOCK_FAULT','UNAVAILABLE']*21
        events,status=self.exercise(self.rows('flow',count=len(modes)),facts_states=modes)
        self.assertEqual(events,[])

    def test_verified_flow_and_503_alternation_never_invent_an_outage_or_healthy_time(self):
        modes=['HEALTHY_FLOW','UNAVAILABLE']*21
        events,status=self.exercise(self.rows('flow',count=len(modes)),facts_states=modes)
        self.assertEqual(events,[])
        self.assertTrue(all(s['healthy_for_sec']==0 for s in status))

    def test_frozen_wall_stamp_with_advancing_ack_is_still_progress_during_clock_fault(self):
        events,status=self.exercise(self.rows('flow-frozen-clock',count=11),clock_fault=True)
        self.assertEqual(events,[])
        self.assertTrue(all(s['assessment']=='HEALTHY' for s in status[1:]))

    def test_clock_fault_plus_no_progress_still_starts_under_uncertainty(self):
        for kind in ('churn','frozen'):
            with self.subTest(kind=kind):
                rows=self.rows(kind)
                events,status=self.exercise(rows,facts_states=['CLOCK_EMPTY']*len(rows))
                self.assertEqual(events,[{'at':90.,'replicas':1}])
                self.assertTrue(all(s['assessment']=='OBSERVATION_LOST' for s in status))
                self.assertEqual(status[6]['reason'],'youtube_observation_timeout')
                self.assertEqual(len({s['causality']['cause_id'] for s in status}),1)

    def test_one_generation_change_can_reestablish_failure_from_fresh_stable_evidence(self):
        events,status=self.exercise(self.rows('one-restart'))
        self.assertEqual(events,[{'at':60.,'replicas':1}])
        self.assertEqual(status[1]['assessment'],'OBSERVATION_LOST')
        self.assertEqual(status[2]['assessment'],'MEDIA_FAILURE')

    def test_sustained_nonclock_gap_starts_ninety_seconds_after_last_verified_progress(self):
        modes=['CLOCK_FAULT']*21+['UNAVAILABLE']*9
        events,status=self.exercise(self.rows('flow',count=len(modes)),facts_states=modes)
        self.assertEqual(events,[{'at':390.,'replicas':1}])
        self.assertEqual(status[27]['reason'],'youtube_observation_timeout')


if __name__=='__main__':unittest.main()
