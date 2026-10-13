import dataclasses
import json
import pathlib
import sys
import tempfile
import unittest
from datetime import UTC, datetime
from unittest.mock import patch

ROOT=pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'ops/relay'))
from twitch_daily import Journal,Helix,Relay,Clock,Command,Action,Binding,Outcome,RenewalState,Phase,pack,state_from,valid_validation,digest
from twitch_session import BroadcastStatus


class DailyBoundaryTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.root=pathlib.Path(self.temp.name)
        self.approval={'twitch_handler_id':'old-twitch'}
        self.binding=Binding('123',1,2,'relay','boot',1)
        self.command=Command(Action.STOP_TWITCH,'operation:stop',self.binding)

    def tearDown(self):self.temp.cleanup()

    def test_journal_cas_atomic_outbox_and_no_effect_replay(self):
        j=Journal(self.root/'daily.sqlite3',self.approval)
        old=j.load();new=dataclasses.replace(old,revision=1,phase=Phase.WAIT_STOP)
        j.commit(old,new,self.command)
        with self.assertRaisesRegex(ValueError,'CAS_FAILED'):j.commit(old,new)
        self.assertEqual(j.pending(),1)
        j.dispatch_once(self.command)
        with self.assertRaisesRegex(ValueError,'REPLAY_BLOCKED'):j.dispatch_once(self.command)
        outcome=Outcome(Action.STOP_TWITCH,self.command.request_id,self.binding,'STOPPED',datetime.now(UTC))
        j.finish(self.command,outcome)
        self.assertEqual(j.pending(),0);self.assertEqual(j.writer(),(1,'old-twitch',1))
        j.close()
        restarted=Journal(self.root/'daily.sqlite3',self.approval)
        self.assertEqual(restarted.load().phase,Phase.WAIT_STOP)
        with self.assertRaisesRegex(ValueError,'REPLAY_BLOCKED'):restarted.dispatch_once(self.command)
        restarted.close()

    def test_unobserved_dispatch_survives_restart(self):
        j=Journal(self.root/'daily.sqlite3',self.approval)
        state=j.load();j.commit(state,dataclasses.replace(state,revision=1),self.command)
        j.dispatch_once(self.command);j.close()
        restarted=Journal(self.root/'daily.sqlite3',self.approval)
        self.assertEqual(restarted.pending(),1);restarted.close()

    def test_new_writer_generation_is_durable_with_outcome(self):
        j=Journal(self.root/'daily.sqlite3',self.approval)
        command=Command(Action.START_TWITCH,'operation:start',self.binding)
        state=j.load();j.commit(state,dataclasses.replace(state,revision=1),command);j.dispatch_once(command)
        outcome=Outcome(Action.START_TWITCH,command.request_id,self.binding,'STARTED',datetime.now(UTC))
        with self.assertRaisesRegex(ValueError,'WRITER_ID_REQUIRED'):j.finish(command,outcome)
        self.assertEqual(j.pending(),1)
        j.finish(command,outcome,'new-twitch');j.close()
        restarted=Journal(self.root/'daily.sqlite3',self.approval)
        self.assertEqual(restarted.writer(),(2,'new-twitch',0));restarted.close()

    def test_second_owner_and_approval_drift_rejected(self):
        j=Journal(self.root/'daily.sqlite3',self.approval)
        with self.assertRaises(BlockingIOError):Journal(self.root/'daily.sqlite3',self.approval)
        j.close()
        with self.assertRaisesRegex(ValueError,'APPROVAL_CHANGED'):Journal(self.root/'daily.sqlite3',{'twitch_handler_id':'changed'})

    def test_state_codec_preserves_clock_and_bindings(self):
        state=RenewalState(revision=3,phase=Phase.WAIT_STOP,binding=self.binding,
            began_at=datetime(2026,10,13,15,tzinfo=UTC),last_clock=Clock(datetime(2026,10,13,15,tzinfo=UTC),100,'boot'))
        self.assertEqual(state_from(json.loads(json.dumps(pack(state)))),state)

    def test_missing_api_credentials_never_become_offline_or_call_network(self):
        p=self.root/'credentials.json';p.write_text(json.dumps({'client_id':'','client_secret':''}))
        with patch.object(Helix,'read') as network:
            facts,reason=Helix(p,'123').fetch(datetime.now(UTC))
        network.assert_not_called();self.assertEqual(facts.status,BroadcastStatus.UNKNOWN);self.assertEqual(reason,'CREDENTIALS_MISSING')

    def test_401_is_unknown_and_does_not_cache_token(self):
        import urllib.error
        p=self.root/'credentials.json';p.write_text(json.dumps({'client_id':'test-app','client_secret':'test-only'}))
        h=Helix(p,'123');h.token='old';h.expires=1e99;h.validated=1e99
        error=urllib.error.HTTPError('redacted',401,'',{},None)
        with patch.object(h,'read',side_effect=error):
            facts,reason=h.fetch(datetime.now(UTC))
        error.close()
        self.assertEqual(facts.status,BroadcastStatus.UNKNOWN);self.assertEqual(reason,'HELIX_HTTP_401');self.assertEqual(h.token,'')

    def test_validation_requires_exact_approval_boot_instance_and_finite_time(self):
        a={'relay_instance':'relay'};now=Clock(datetime.fromtimestamp(1000,UTC),50,'boot')
        g={'schema':'stream.twitch_daily_validation.v1','approval_sha256':digest(a),'boot_id':'boot','relay_instance':'relay',
           'issued_at':999,'expires_at':1500,'issued_elapsed':49}
        self.assertTrue(valid_validation(g,a,now))
        for change in [{'boot_id':'other'},{'relay_instance':'other'},{'expires_at':2000},{'issued_elapsed':51}]:
            self.assertFalse(valid_validation({**g,**change},a,now))

    def test_forward_write_keeps_youtube_entry_and_index(self):
        yt={'dest':'rtmps://a.rtmps.youtube.com/live2#test-youtube','destFingerprint':''}
        tw={'dest':'rtmps://test.live-video.net/app#test-twitch','destFingerprint':''}
        r=Relay({'forward':[yt,tw]},self.root/'unused')
        with patch.object(r,'observe'),patch.object(r,'bound'),patch.object(r,'api') as api:
            r.change(self.command,None)
            self.assertEqual(api.call_args.args,('/v3/config/paths/patch/relay',{'forward':[yt]}))
            r.change(dataclasses.replace(self.command,action=Action.START_TWITCH),None)
            self.assertEqual(api.call_args.args,('/v3/config/paths/patch/relay',{'forward':[yt,tw]}))

    def test_youtube_native_handler_change_blocks_operation(self):
        config={'name':'relay','forward':[{},{}]}
        a={'forward':[{},{}],'path_configuration_sha256':digest({'name':'relay'}),'youtube_handler_id':'original','youtube_handler_created':'before'}
        r=Relay(a,self.root/'unused')
        rows={'items':[{'pos':1,'id':'replacement','created':'after','state':'forwarding','outboundBytes':100,'lastError':None}]}
        with patch.object(r,'bound',return_value={}),patch.object(r,'api',side_effect=[config,rows]):
            with self.assertRaisesRegex(ValueError,'YOUTUBE_HANDLER_CHANGED'):
                r.observe(Clock(datetime.now(UTC),100,'boot'))

    def test_empty_moq_transport_must_be_omitted_to_keep_native_conf(self):
        yt={'dest':'rtmps://a.rtmps.youtube.com/live2#test-youtube','moqTransport':'','destFingerprint':'','whipBearerToken':''}
        tw={'dest':'rtmps://test.live-video.net/app#test-twitch','moqTransport':''}
        r=Relay({'forward':[yt,tw]},self.root/'unused')
        with patch.object(r,'observe'),patch.object(r,'bound'),patch.object(r,'api') as api:
            r.change(self.command,None)
            payload=api.call_args.args[1]['forward'][0]
        self.assertNotIn('moqTransport',payload)
        # Go decoderはkey省略ならzero-value、明示された空文字ならquic。
        decoded={**payload,'moqTransport':payload.get('moqTransport','')}
        self.assertEqual(decoded,yt)

    def test_heartbeat_written_during_read_uses_fresh_clock(self):
        approval={'boot_id':'boot','relay_instance':'relay','mediamtx_pid':777,'mediamtx_start_ticks':123}
        guard={'instance':'relay','enabled':True,'halted':False,'heartbeat_elapsed':50.2,
               'lease_elapsed_deadline':60,'lease_expires_at':1010,'input_age':.1}
        r=Relay(approval,self.root/'guard.json')
        stat='777 (mediamtx) '+' '.join(['S']+['0']*18+['123'])
        def read(p):return stat if str(p).endswith('/stat') else json.dumps(guard)
        with patch.object(pathlib.Path,'read_text',read),patch('twitch_daily.os.readlink',return_value='/opt/relay/mediamtx'), \
             patch('twitch_daily.time.clock_gettime',return_value=50.3),patch('twitch_daily.time.time',return_value=1000.5):
            self.assertEqual(r.bound(Clock(datetime.fromtimestamp(1000,UTC),50,'boot')),guard)
            guard['heartbeat_elapsed']=47
            with self.assertRaisesRegex(ValueError,'HEARTBEAT_INVALID'):
                r.bound(Clock(datetime.fromtimestamp(1000,UTC),50,'boot'))


if __name__=='__main__':unittest.main()
