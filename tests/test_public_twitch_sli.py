import importlib.util
import json
import pathlib
import tempfile
import unittest
from unittest.mock import patch

PATH=pathlib.Path(__file__).resolve().parents[1]/'ops/public-publisher/site/scripts/collect_twitch_sli.py'
spec=importlib.util.spec_from_file_location('sli_publish',PATH);pub=importlib.util.module_from_spec(spec);spec.loader.exec_module(pub)

def fixture():
    v={'schema':'stream.twitch_sli_public.v1','generated_at':'2026-10-13T10:00:00+00:00','history_started_at':'2026-10-13T09:00:00+00:00','cadence_sec':60,'freshness_limit_sec':180,'primary_window':'7d','coverage_gate_percent':99.,'planned_downtime_included':True,'windows':[],'timeline':[]}
    for name,length,start in [('24h',1440,'2026-10-12T10:00:00+00:00'),('7d',10080,'2026-10-06T10:00:00+00:00')]:
        metrics=[]
        for kind,(label,definition,target) in pub.DEFINITIONS.items():
            m={'id':kind,'label':label,'definition':definition,'target_percent':target,'value_percent':None,'assessment':'COLLECTING'}
            if kind=='daily_renewal':m.update(good_events=0,bad_events=0,unknown_events=1 if name=='24h' else 7,pending_events=0,expected_events=1 if name=='24h' else 7)
            else:m.update(conservative_percent=0.,coverage_percent=0.,good_minutes=0,bad_minutes=0,unknown_minutes=0,missing_minutes=length,expected_minutes=length,error_budget_minutes=length*.01,confirmed_budget_used_percent=0.)
            metrics.append(m)
        v['windows'].append({'id':name,'start_at':start,'end_at':v['generated_at'],'full_history':False,'metrics':metrics})
    v['timeline']=[{'start_at':v['generated_at'],'good':0,'bad':0,'unknown':0,'missing':60} for _ in range(24)]
    return v

class PublicSliTests(unittest.TestCase):
    def test_current_and_previous_api_wording_are_accepted_without_changing_counts(self):
        now=pub.timestamp(fixture()['generated_at'])+1
        for text in [
            ('API observation availability','Successful fresh Helix observation minutes / all expected minutes; includes collection gaps.',99.0),
            ('API measurement coverage','Successful fresh Helix observations / all expected minutes.',99.0),
        ]:
            with self.subTest(label=text[0]):
                v=fixture()
                for w in v['windows']:
                    m=w['metrics'][1];m['label'],m['definition'],m['target_percent']=text
                self.assertIs(pub.validate(v,now),v)

    def test_api_wording_compatibility_does_not_allow_unknown_or_mixed_definitions(self):
        now=pub.timestamp(fixture()['generated_at'])+1
        for field,value in [('label','unexpected'),('definition','unexpected'),('target_percent',98.0),
                            ('label','API measurement coverage')]:
            with self.subTest(field=field,value=value):
                v=fixture();v['windows'][0]['metrics'][1][field]=value
                with self.assertRaises(ValueError):pub.validate(v,now)

    def test_projection_accepts_unknown_and_rejects_private_nested_fields(self):
        now=pub.timestamp(fixture()['generated_at'])+1
        self.assertIsNone(pub.validate(fixture(),now)['windows'][0]['metrics'][0]['value_percent'])
        for level in ['top','metric','timeline']:
            v=fixture();target=v if level=='top' else v['windows'][0]['metrics'][0] if level=='metric' else v['timeline'][0]
            target['credential']='fixture-private'
            with self.assertRaises(ValueError):pub.validate(v,now)

    def test_arbitrary_strings_and_inconsistent_counts_are_rejected(self):
        now=pub.timestamp(fixture()['generated_at'])+1
        v=fixture();v['windows'][0]['metrics'][0]['definition']='fixture-private'
        with self.assertRaises(ValueError):pub.validate(v,now)
        v=fixture();v['windows'][0]['metrics'][0]['good_minutes']=1
        with self.assertRaises(ValueError):pub.validate(v,now)
        v=fixture();v['windows'][0]['metrics'][0]['coverage_percent']=float('nan')
        with self.assertRaises(ValueError):pub.validate(v,now)

    def test_stale_or_future_artifact_is_rejected(self):
        now=pub.timestamp(fixture()['generated_at'])
        for at in [now+181,now-1]:
            with self.assertRaises(ValueError):pub.validate(fixture(),at)

    def test_fetch_failure_keeps_original_bytes_and_mtime(self):
        with tempfile.TemporaryDirectory() as d:
            root=pathlib.Path(d);p=root/'twitch-sli.json';p.write_text(json.dumps(fixture()));before=p.read_bytes();mtime=p.stat().st_mtime_ns
            with patch.object(pub,'PUBLIC',root),patch.object(pub.subprocess,'run',side_effect=OSError('fixture')):
                self.assertEqual(pub.main(),0)
            self.assertEqual(p.read_bytes(),before);self.assertEqual(p.stat().st_mtime_ns,mtime)

if __name__=='__main__':unittest.main()
