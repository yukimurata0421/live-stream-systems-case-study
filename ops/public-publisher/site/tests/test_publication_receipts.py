"""Crash, partial publication and private diagnostic boundaries."""
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import sys
import socket
import tempfile
import time
import unittest
import urllib.error
from datetime import datetime, timezone
from unittest.mock import patch

SCRIPTS = Path(__file__).resolve().parents[1] / 'scripts'
spec = importlib.util.spec_from_file_location('receipted_publisher', SCRIPTS/'push_to_r2.py')
publisher = importlib.util.module_from_spec(spec);sys.modules[spec.name]=publisher
with patch.object(sys,'path',[str(SCRIPTS),*sys.path]):spec.loader.exec_module(publisher)
import publication_receipts as storage


class ReceiptTests(unittest.TestCase):
    def setUp(self):
        health = patch.dict(storage.os.environ, {'YUKIMURATA_PUBLICATION_HEALTH_URL':'https://status.example.invalid/health/publication'})
        health.start(); self.addCleanup(health.stop)
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup);self.root=Path(self.tmp.name)
        self.allowed=['good.json','bad.json','asset.js','index.html','site-release.json']

    def receipts(self, seed=None):
        return storage.Receipts(self.root/'objects.json', self.root/'events.jsonl', binding='test',
            allowed=self.allowed, write=publisher.write_status, seed=seed)

    def test_crash_after_put_is_resolved_from_disk_without_another_put(self):
        objects={};writes=[]
        class Client:
            def get(self,n):return objects.get(n)
            def put(self,n,raw):objects[n]=raw;writes.append(n);raise SystemExit('owned child crash')
        with self.assertRaises(SystemExit):publisher.publish(Client(),{'good.json':b'one'},{},receipts=self.receipts())
        self.assertEqual(self.receipts().get('good.json')['state'],'OUTCOME_UNKNOWN')
        result=publisher.publish(Client(),{'good.json':b'one'},{},receipts=self.receipts())
        self.assertEqual(result['unchanged'],1);self.assertEqual(writes,['good.json'])

    def test_unknown_nonmatching_readback_is_held_not_retransmitted(self):
        writes=[]
        class Client:
            def get(self,n):return None
            def put(self,n,raw):writes.append(n);raise OSError('secret transport text')
        for _ in range(2):
            result=publisher.publish(Client(),{'bad.json':b'one'},{},receipts=self.receipts(),continue_on_error=True)
            self.assertEqual(result['errors'][0]['code'],'R2_UPLOAD_OUTCOME_UNCONFIRMED')
        self.assertEqual(writes,['bad.json'])
        self.assertNotIn('secret transport text',(self.root/'events.jsonl').read_text())

    def test_dns_failure_before_http_send_is_not_an_uncertain_provider_effect(self):
        def dns(*args,**kwargs):
            raise urllib.error.URLError(socket.gaierror(socket.EAI_AGAIN,'private DNS detail'))
        client=publisher.R2Client(publisher.Credentials('a'*32,'private-token'),'public-site',deadline=time.monotonic()+10,opener=dns)
        receipts=self.receipts({'good.json':hashlib.sha256(b'old').hexdigest()})
        first=publisher.publish(client,{'good.json':b'new'},{},receipts=receipts,continue_on_error=True)
        self.assertEqual(first['errors'][0]['code'],'R2_REQUEST_NOT_SENT')
        self.assertEqual(self.receipts().get('good.json')['state'],'NOT_SENT')
        events=(self.root/'events.jsonl').read_text();self.assertNotIn('private',events)
        self.assertIn(hashlib.sha256(b'new').hexdigest(),events)
        objects={};writes=[]
        class Recovered:
            def get(self,n):return objects.get(n)
            def put(self,n,raw):writes.append(n);objects[n]=raw
        result=publisher.publish(Recovered(),{'good.json':b'newer'},{},receipts=self.receipts())
        self.assertEqual(result['uploaded'],1);self.assertEqual(writes,['good.json'])

    def test_one_failure_does_not_cancel_other_objects_or_forget_verified_assets(self):
        objects={};calls=[]
        class Client:
            def get(self,n):calls.append(('GET',n));return objects.get(n)
            def put(self,n,raw):
                calls.append(('PUT',n))
                if n=='bad.json':raise OSError('lost')
                objects[n]=raw
        seed={'asset.js':hashlib.sha256(b'asset').hexdigest()}
        result=publisher.publish(Client(),{'bad.json':b'bad','good.json':b'good','asset.js':b'asset'},
            {},receipts=self.receipts(seed),continue_on_error=True)
        self.assertEqual(objects['good.json'],b'good');self.assertEqual(len(result['errors']),1)
        calls.clear();publisher.publish(Client(),{'asset.js':b'asset','good.json':b'good'},{},receipts=self.receipts())
        self.assertEqual(calls,[])

    def test_get_retry_is_bounded_and_does_not_repeat_put(self):
        calls=[];objects={}
        class Client:
            deadline=time.monotonic()+10
            def get(self,n):
                calls.append('GET')
                if len(calls)==1:raise publisher.ProviderError(503)
                return objects.get(n)
            def put(self,n,raw):calls.append('PUT');objects[n]=raw
        publisher.publish(Client(),{'good.json':b'good'},{},receipts=self.receipts())
        self.assertEqual(calls,['GET','GET','PUT','GET'])
        event=json.loads((self.root/'events.jsonl').read_text().splitlines()[0])
        self.assertEqual((event['stage'],event['http_status']),('initial_reconcile',503))

    def test_receipt_binding_corruption_and_unknown_file_are_rejected(self):
        self.receipts()
        for change in ({'destination':'other'}, {'objects':{'private':{'state':'VERIFIED','sha256':'a'*64}}}):
            p=self.root/'objects.json';value=json.loads(p.read_text());before=p.read_bytes();value.update(change);p.write_text(json.dumps(value))
            with self.assertRaises(ValueError):self.receipts()
            p.write_bytes(before)

    def test_stale_nonlive_report_does_not_block_fresh_live_files(self):
        now=time.time();names=['index.html',*storage.LIVE_FILES,'reliability-indicators.json']
        for name in names:
            (self.root/name).write_text('<head></head>' if name.endswith('.html') else json.dumps({'generated_at':now-(181 if name.startswith('reliability') else 10)}))
        frozen,errors=publisher.freeze_independently(self.root,{'files':names,'required':names,'live':list(storage.LIVE_FILES)},now=now)
        self.assertTrue(all(n in frozen for n in storage.LIVE_FILES))
        self.assertEqual(errors[0]['object'],'reliability-indicators.json')
        self.assertAlmostEqual(errors[0]['source_age_seconds'],181)
        self.assertNotIn('reliability-indicators.json',frozen)

    def test_health_503_diagnostics_include_age_and_exclude_arbitrary_fields(self):
        payload={'schema':'public.publication_health.v1','status':'unhealthy','page':'available','issues':['prom','secret'],
                 'private':'credential','sources':[{'source':'prom','state':'stale','ageSeconds':181,'private':'credential'}]}
        class Response(io.BytesIO):status=503
        out=storage.health_observation(deadline=time.monotonic()+10,
            opener=lambda *a,**kw:Response(json.dumps(payload).encode()))
        self.assertEqual(out['http_status'],503);self.assertEqual(out['issues'],['prom'])
        self.assertEqual(out['sources'][0]['ageSeconds'],181);self.assertNotIn('credential',json.dumps(out))

    def test_iso_source_timestamp_is_validated_without_rewriting_public_bytes(self):
        now=time.time();name='twitch-sli.json';p=self.root/name
        manifest={'files':[name],'required':[]}
        for age in (30,181,-10):
            raw=json.dumps({'schema':'stream.twitch_sli_public.v1','generated_at':datetime.fromtimestamp(now-age,timezone.utc).isoformat()}).encode();p.write_bytes(raw)
            frozen,errors=publisher.freeze_independently(self.root,manifest,now=now)
            self.assertEqual(p.read_bytes(),raw)
            if age==30:self.assertEqual(frozen[name],raw);self.assertEqual(errors,[])
            else:self.assertEqual(frozen,{});self.assertAlmostEqual(errors[0]['source_age_seconds'],age,places=3)
        for stamp in ('2026-11-03T02:00:00','invalid',True,None):
            p.write_text(json.dumps({'generated_at':stamp}))
            _,errors=publisher.freeze_independently(self.root,manifest,now=now)
            self.assertEqual(errors[0]['code'],'PUBLIC_SOURCE_TIMESTAMP_INVALID')

    def test_health_non_json_failure_retains_http_status(self):
        class Response(io.BytesIO):status=503
        out=storage.health_observation(deadline=time.monotonic()+10,opener=lambda *a,**kw:Response(b'private provider error'))
        self.assertEqual(out['http_status'],503);self.assertNotIn('private',json.dumps(out))

    def test_event_rotation_is_bounded_and_keeps_current_uncertainty(self):
        receipt=self.receipts();receipt.set('bad.json','OUTCOME_UNKNOWN','a'*64)
        with patch.object(storage,'MAX_EVENTS_BYTES',256):
            for n in range(15):receipt.event({'stage':'readback','code':'TIMEOUT','attempt':n})
        self.assertLess((self.root/'events.jsonl').stat().st_size,256)
        self.assertLess((self.root/'events.jsonl.1').stat().st_size,256)
        self.assertEqual(self.receipts().get('bad.json')['state'],'OUTCOME_UNKNOWN')


if __name__=='__main__':unittest.main()
