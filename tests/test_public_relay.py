import importlib.util
import json
import pathlib
import tempfile
import unittest
import sys
from datetime import datetime,timezone
from unittest.mock import patch

SOURCE=pathlib.Path(__file__).resolve().parents[1]/'ops/public-publisher/site/scripts/collect_relay_public.py'
spec=importlib.util.spec_from_file_location('relay_public_collector',SOURCE);collector=importlib.util.module_from_spec(spec)
with patch.object(sys,'path',[str(SOURCE.parent),*sys.path]):spec.loader.exec_module(collector)


class PublicRelayTests(unittest.TestCase):
    def test_transport_offset_is_removed_without_rejuvenating_old_public_data(self):
        root=pathlib.Path(__file__).resolve().parents[1]/'ops/selfheal/clock_contract.py'
        spec=importlib.util.spec_from_file_location('clock_public_test',root)
        clock=importlib.util.module_from_spec(spec);sys.modules[spec.name]=clock;spec.loader.exec_module(clock)
        for offset in (-600,600):
            # Model is bound to current local clocks. Source was generated 30s ago.
            now=clock.capture();model=clock.Estimate(offset-.01,offset+.01,now['wall'],now['mono'],'remote','a'*64,'local',.02)
            source=self.value();source['generated_at']=now['wall']+offset-30
            source['observed_at']=datetime.fromtimestamp(source['generated_at']-5,timezone.utc).isoformat()
            source['private']='must-not-publish'
            with self.assertRaises(ValueError):collector.clock_payload({'value':source},model,now['wall'])
            source.pop('private');out=collector.clock_payload({'value':self.value()|{'generated_at':now['wall']+offset-30}},model,now['wall'])
            self.assertAlmostEqual(now['wall']-out['generated_at'],30.01,delta=.02)
            with self.assertRaises(ValueError):collector.clock_payload({'value':self.value()|{'generated_at':now['wall']+offset-181}},model,now['wall'])

    def test_controller_unknown_is_preserved_and_private_fields_are_rejected(self):
        row={'schema':'stream_v3.fallback_controller_public.v1','observed_at':'2026-10-29T10:00:00Z',
             'health':'UNKNOWN','reason':'facts_timeout','healthy_for_sec':0,'stop_after_sec':300,
             'healthy_resets_total':1,'last_reset_at':'2026-10-29T09:59:00Z','last_reset_reason':'facts_timeout'}
        v=self.value();v['fallback']={'active':True,'ready':True,'observed_at':'2026-10-29T10:00:00Z','controller':row}
        self.assertIs(collector.validate(v,1001),v)
        for key,value in (('reason','private-error'),('healthy_for_sec',True),('healthy_for_sec',float('nan')),('stop_after_sec',True),('private_url','secret')):
            with self.subTest(key=key,value=value):
                before=dict(row);row[key]=value
                with self.assertRaises(ValueError):collector.validate(v,1001)
                row.clear();row.update(before)

    def test_safe_causal_fields_survive_without_private_identity(self):
        v=self.value();v['fallback']={'active':False,'ready':False,'observed_at':None,
            'causality':{'status':'VERIFIED','assessment':'CLOCK_FAULT','cause_id':'a'*32,'secondary_recovery_blocked':True}}
        self.assertIs(collector.validate(v,1001),v)
        for key,item in (('cause_id','private-id'),('machine_id_sha256','private-hash'),('secondary_recovery_blocked',1)):
            before=dict(v['fallback']['causality']);v['fallback']['causality'][key]=item
            with self.assertRaises(ValueError):collector.validate(v,1001)
            v['fallback']['causality']=before

    def value(self):
        return {'schema':'stream.relay_public.v1','generated_at':1000.,'observed_at':None,'status':'UNKNOWN','source_fresh':False,
                'vm':{'status':'UNKNOWN','cpu_percent':None,'memory_percent':None,'disk_free_percent':None,'uptime_seconds':None},
                'relay':{'status':'UNKNOWN','restart_count':None},'input':{'status':'UNKNOWN','mbps':None},
                'destinations':{n:{'status':'UNKNOWN','mbps':None} for n in ['youtube','twitch']},
                'daily':{'state':'UNKNOWN','next_renewal_at':None,'planned_break':False},
                'twitch_api':{'state':'UNKNOWN','observed_at':None,'source_fresh':False},'issues':['RELAY_OBSERVATION_UNAVAILABLE']}

    def test_unknown_observation_remains_unknown(self):
        v=collector.validate(self.value(),1001)
        self.assertEqual(v['relay']['status'],'UNKNOWN');self.assertIsNone(v['input']['mbps'])

    def test_private_fields_and_unapproved_issue_text_are_rejected(self):
        v=self.value();v['private_url']='fixture-only'
        with self.assertRaises(ValueError):collector.validate(v,1001)
        v=self.value();v['destinations']['twitch']['stream_key']='fixture-only'
        with self.assertRaises(ValueError):collector.validate(v,1001)
        v=self.value();v['issues']=['fixture-only']
        with self.assertRaises(ValueError):collector.validate(v,1001)

    def test_stale_future_nonfinite_and_bool_measurements_are_rejected(self):
        for change in [{'generated_at':800},{'generated_at':1002.001},{'generated_at':float('nan')}]:
            v=self.value();v.update(change)
            with self.assertRaises(ValueError):collector.validate(v,1001)
        v=self.value();v['vm']['cpu_percent']=True
        with self.assertRaises(ValueError):collector.validate(v,1001)

    def test_small_future_offset_is_accepted_without_rewriting_time(self):
        for offset in (0.05,1):
            v=self.value();v['generated_at']=1001+offset
            self.assertIs(collector.validate(v,1001),v)
            self.assertEqual(v['generated_at'],1001+offset)

    def test_failed_fetch_preserves_artifact_bytes_and_timestamp(self):
        with tempfile.TemporaryDirectory() as d:
            root=pathlib.Path(d);p=root/'relay-observation.json';p.write_text(json.dumps(self.value()));before=p.read_bytes();stamp=p.stat().st_mtime_ns
            with patch.object(collector,'PUBLIC',root),patch.object(collector.subprocess,'run',side_effect=OSError('fixture')):
                self.assertEqual(collector.main(),0)
            self.assertEqual(p.read_bytes(),before);self.assertEqual(p.stat().st_mtime_ns,stamp)


if __name__=='__main__':unittest.main()
