import importlib.util
import json
import pathlib
import tempfile
import unittest
from unittest.mock import patch

SOURCE=pathlib.Path(__file__).resolve().parents[1]/'ops/public-publisher/site/scripts/collect_relay_public.py'
spec=importlib.util.spec_from_file_location('relay_public_collector',SOURCE);collector=importlib.util.module_from_spec(spec);spec.loader.exec_module(collector)


class PublicRelayTests(unittest.TestCase):
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
        for change in [{'generated_at':800},{'generated_at':1002},{'generated_at':float('nan')}]:
            v=self.value();v.update(change)
            with self.assertRaises(ValueError):collector.validate(v,1001)
        v=self.value();v['vm']['cpu_percent']=True
        with self.assertRaises(ValueError):collector.validate(v,1001)

    def test_failed_fetch_preserves_artifact_bytes_and_timestamp(self):
        with tempfile.TemporaryDirectory() as d:
            root=pathlib.Path(d);p=root/'relay-observation.json';p.write_text(json.dumps(self.value()));before=p.read_bytes();stamp=p.stat().st_mtime_ns
            with patch.object(collector,'PUBLIC',root),patch.object(collector.subprocess,'run',side_effect=OSError('fixture')):
                self.assertEqual(collector.main(),0)
            self.assertEqual(p.read_bytes(),before);self.assertEqual(p.stat().st_mtime_ns,stamp)


if __name__=='__main__':unittest.main()
