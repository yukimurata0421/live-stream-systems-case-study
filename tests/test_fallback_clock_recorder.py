"""Exercise fixed deadlines, disk guards, redaction and clock summaries without delivery effects."""
import importlib.util
import contextlib
import io
import json
from pathlib import Path
import subprocess
import tempfile
import time
import unittest
from unittest import mock
from datetime import datetime, timezone

ROOT=Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location('clock_recorder',ROOT/'ops/scripts/stream_v3_fallback_clock_recorder.py')
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)


class ClockRecorderTests(unittest.TestCase):
    def test_corrected_clock_fields_are_bounded_and_private_envelope_is_omitted(self):
        value=self.status(clock={'status':'VERIFIED','offset_seconds':-.01,'uncertainty_seconds':.02,
                                 'network_rtt_seconds':.03,'private':'private-envelope'},
                          source_age_bounds_seconds=[-.001,.04],copy_age_bounds_seconds=[.1,.2],
                          progress_age_bounds_seconds=[.2,.3])
        row=m.controller_record(value)
        self.assertEqual(row['clock_offset_seconds'],-.01)
        self.assertEqual(row['clock_uncertainty_seconds'],.02)
        self.assertEqual(row['source_age_upper_seconds'],.04)
        self.assertNotIn('private-envelope',json.dumps(row))
        value['clock']['uncertainty_seconds']=float('nan')
        with self.assertRaises(ValueError):m.controller_record(value)
    def job(self, output, *, start=None, deadline=None):
        now=time.time()
        return {'started_at':m.utc(now-1 if start is None else start),
                'deadline':m.utc(now+1 if deadline is None else deadline),
                'output':str(output),'mount':'/srv/observations','filesystem_uuid':'fixed',
                'machines':{'sender':'a'*64,'relay':'b'*64},'ssh':{'config':'/etc/stream-clock/ssh_config','host':'relay-observer'},'max_bytes':1024*1024,'min_free_bytes':1024}

    def status(self, now=None, **changes):
        now=time.time() if now is None else now
        v={'schema':'stream_v3.fallback_controller.v1','observed_at':m.utc(now),
           'source_observed_at':m.utc(now+.065),'http_started_at':m.utc(now-.01),
           'http_completed_at':m.utc(now),'http_elapsed_seconds':.01,
           'source_age_seconds':-.065,'copy_age_seconds':.1,'progress_age_seconds':.2,
           'controller_pid':123,'healthy_resets_total':0,'health':'HEALTHY','reason':'progress_confirmed'}
        v.update(changes);return v

    def test_projection_bounds_and_unsafe_fields(self):
        row=m.controller_record(self.status(private_url='rtmps://private.invalid/secret'))
        self.assertNotIn('private',json.dumps(row))
        self.assertAlmostEqual(row['remote_minus_local_lower_seconds'],.065,places=5)
        self.assertAlmostEqual(row['remote_minus_local_upper_seconds'],.075,places=5)
        self.assertAlmostEqual(row['http_wall_minus_monotonic_seconds'],0,places=5)
        for bad in [[],self.status(source_age_seconds=float('nan')),self.status(reason='private: text'),self.status(observed_at='2026-01-01')]:
            with self.subTest(bad=bad),self.assertRaises((ValueError,TypeError)):
                m.controller_record(bad)

    def test_mount_missing_rejects_before_writing(self):
        with tempfile.TemporaryDirectory() as d:
            job=self.job(Path(d));job['mount']=d
            with self.assertRaises(ValueError):m.storage_guard(job)
            self.assertEqual(list(Path(d).iterdir()),[])

    def test_wrong_filesystem_uuid_is_rejected(self):
        with mock.patch.object(m.Path,'is_mount',return_value=True), \
             mock.patch.object(m.subprocess,'run',return_value=subprocess.CompletedProcess([],0,'different\n','')):
            with self.assertRaisesRegex(ValueError,'hdd_device_mismatch'):
                m.storage_guard(self.job(Path('/srv/observations')))

    def test_unbounded_duration_and_capacity_are_rejected(self):
        for change in [{'deadline':m.utc(time.time()+8*86400)},{'max_bytes':129*1024*1024}]:
            job=self.job(Path('/tmp'));job.update(change)
            with self.assertRaises(ValueError):m.validate_job(job)

    def test_real_file_loop_deduplicates_status_and_stops_at_fixed_deadline(self):
        with tempfile.TemporaryDirectory() as d:
            output=Path(d);source=output/'status.json';source.write_text(json.dumps(self.status()))
            job=self.job(output,deadline=time.time()+.15)
            with mock.patch.object(m,'storage_guard'):
                reason=m.collect(job,status=source,clock_probe=lambda j:[])
                # Reopening the same expired job must not extend it or create new observations.
                self.assertEqual(m.collect(job,status=source,clock_probe=lambda j:[]),'DEADLINE_REACHED')
            self.assertEqual(reason,'DEADLINE_REACHED')
            rows=list(m.records(output));self.assertEqual(sum(x['kind']=='controller' for x in rows),1)
            self.assertEqual(json.loads((output/'summary.json').read_text())['controller_samples'],1)

    def test_capacity_stop_preserves_existing_segments(self):
        with tempfile.TemporaryDirectory() as d:
            output=Path(d);old=output/'records-old.jsonl';original=m.encode({'kind':'preserved_failure'});old.write_bytes(original)
            job=self.job(output);job['max_bytes']=len(original)+1
            with mock.patch.object(m,'storage_guard'):
                self.assertTrue(m.collect(job,clock_probe=lambda j:[]).startswith('STORAGE_STOP'))
            self.assertEqual(old.read_bytes(),original)

    def test_summary_counts_gaps_resets_future_spans_and_ntp_events(self):
        with tempfile.TemporaryDirectory() as d:
            output=Path(d);a=m.controller_record(self.status(100,healthy_resets_total=2))
            b=m.controller_record(self.status(115,healthy_resets_total=3))
            c=m.controller_record(self.status(180,controller_pid=456,healthy_resets_total=0,source_age_seconds=.01))
            clock={'kind':'clock','host':'relay','status':'OBSERVED','tracking':{'offset_seconds':-.003},
                   'events':[{'id':'event','kind':'initial_sync','at':100}]}
            rows=[a,b,b,c,clock,clock]
            (output/'records-a.jsonl').write_bytes(b''.join(m.encode(x) for x in rows)+b'{incomplete')
            summary=m.summarize(output,self.job(output),'DEADLINE_REACHED')
            self.assertEqual(summary['controller_samples'],3)
            self.assertEqual(summary['healthy_resets_observed'],1)
            self.assertEqual(summary['future_samples'],2)
            self.assertEqual(summary['max_consecutive_future_observation_span_seconds'],15)
            self.assertEqual(summary['gaps_over_45_seconds'][0]['seconds'],65)
            self.assertEqual(len(summary['clock_events']),1)
            self.assertEqual(summary['record_counts']['incomplete_record'],1)
            self.assertEqual(summary['clock_distributions']['relay.offset_seconds']['min'],-.003)

    def test_remote_host_binding_failure_is_unknown_without_raw_output(self):
        reply={'machine_id_sha256':'other','private_url':'rtmps://secret'}
        with mock.patch.object(m.subprocess,'run',return_value=subprocess.CompletedProcess([],0,json.dumps(reply),'')):
            row=m.probe('relay','b'*64,{'config':'/etc/stream-clock/ssh_config','host':'relay-observer'})
        self.assertEqual(row['status'],'UNKNOWN');self.assertNotIn('secret',json.dumps(row))

    def test_configured_ssh_target_and_matching_machine_are_used(self):
        ssh={'config':'/etc/stream-clock/ssh_config','host':'relay-observer'}
        reply={'machine_id_sha256':'b'*64,'observed_at':m.utc(),'tracking':{'offset_seconds':.001}}
        with mock.patch.object(m.subprocess,'run',return_value=subprocess.CompletedProcess([],0,json.dumps(reply),'')) as run:
            row=m.probe('relay','b'*64,ssh)
        self.assertEqual(row['status'],'OBSERVED')
        args=run.call_args.args[0]
        self.assertEqual(args[args.index('-F')+1],ssh['config'])
        self.assertIn(ssh['host'],args)
        self.assertNotIn('/home/',str(args))

    def test_missing_or_unsafe_ssh_configuration_fails_before_probing(self):
        for ssh in [{},{'config':'relative','host':'relay-observer'},
                    {'config':'/etc/stream-clock/ssh_config','host':'-unsafe'},
                    {'config':'/etc/stream-clock/ssh_config','host':'relay; command'}]:
            job=self.job(Path('/tmp'));job['ssh']=ssh
            with self.subTest(ssh=ssh),self.assertRaises(ValueError):m.validate_job(job)

    def test_probe_parser_uses_same_code_for_both_clock_daemons(self):
        def reply(args,**kwargs):
            text=''
            if args[0]=='journalctl' and daemon=='journal_timeout':
                raise subprocess.TimeoutExpired(args,3)
            if args[0]=='chronyc':
                text='5BBD5B70,192.0.2.1,3,1000,0.0017,-0.001,0.0008,-6.4,-0.01,0.17,0.23,0.004,1032,Normal\n'
            elif args[:2]==['timedatectl','timesync-status']:
                text=' Server: private-address\n Offset: -159us\n Delay: 631us\n Jitter: 40.045ms\n Root distance: 213us (max: 5s)\n'
            elif args[0]=='journalctl':
                text=json.dumps({'__REALTIME_TIMESTAMP':'1000000000','MESSAGE':'Initial clock synchronization to private text'})
            elif args[0]=='systemctl':text='ActiveState=active\nMainPID=123\nNRestarts=0\n'
            else:text='NTP=yes\nNTPSynchronized=yes\n'
            return subprocess.CompletedProcess(args,0,text,'')
        for daemon in ['chrony','systemd','journal_timeout']:
            with self.subTest(daemon=daemon),mock.patch.object(m.subprocess,'run',side_effect=reply), \
                 mock.patch.object(m.shutil,'which',return_value='/usr/bin/chronyc' if daemon=='chrony' else None), \
                 mock.patch.object(m.Path,'exists',return_value=True),mock.patch.object(m.Path,'glob',return_value=[]), \
                 contextlib.redirect_stdout(io.StringIO()) as output:
                exec(m.CLOCK_PROBE,{})
                value=json.loads(output.getvalue())
                if daemon=='chrony':self.assertEqual(value['tracking']['system_time_seconds'],.0017)
                else:self.assertAlmostEqual(value['tracking']['offset_seconds'],-.000159)
                if daemon=='journal_timeout':
                    self.assertEqual(value['errors'],['journalctl_timeout']);self.assertEqual(value['events'],[])
                else:self.assertEqual(value['events'][0]['kind'],'initial_sync')
                self.assertNotIn('private',output.getvalue());self.assertNotIn('192.0.2.1',output.getvalue())


if __name__=='__main__':unittest.main()
