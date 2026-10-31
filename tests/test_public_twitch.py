import sys
import importlib.util,json,pathlib,time,unittest,tempfile
from unittest import mock
ROOT=pathlib.Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location('public_twitch',ROOT/'ops/public-publisher/site/scripts/collect_twitch_public.py')
sys.path.insert(0,str(ROOT/'ops/public-publisher/site/scripts'))
module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)

class PublicTwitchTests(unittest.TestCase):
    def test_small_future_and_exact_freshness_boundaries_preserve_source_time(self):
        now=1800000000.
        for offset in (.003,.05,1.,-180.):
            with self.subTest(offset=offset):
                value=self.value();value['generated_at']=now+offset
                self.assertIs(module.validate(value,now),value)
                self.assertEqual(value['generated_at'],now+offset)

    def test_nonfinite_bool_excess_future_and_stale_generation_fail_closed(self):
        now=1800000000.
        for generated in (now+1.001,now-180.001,True,False,float('nan'),float('inf'),-float('inf'),None,'1800000000'):
            with self.subTest(generated=generated):
                value=self.value();value['generated_at']=generated
                with self.assertRaises(ValueError):module.validate(value,now)

    def test_rejected_source_keeps_prior_artifact_bytes_and_mtime(self):
        now=1800000000.
        with tempfile.TemporaryDirectory() as directory:
            root=pathlib.Path(directory);path=root/'twitch-observation.json'
            path.write_text(json.dumps(self.value()));before=path.read_bytes();mtime=path.stat().st_mtime_ns
            for generated in (now+1.001,now-180.001,float('nan')):
                value=self.value();value['generated_at']=generated
                with mock.patch.object(module,'PUBLIC',root),mock.patch.object(module.time,'time',return_value=now),\
                     mock.patch.object(module.subprocess,'run',return_value=mock.Mock(returncode=0,stdout=json.dumps(value).encode())):
                    self.assertEqual(module.main(),0)
                self.assertEqual(path.read_bytes(),before);self.assertEqual(path.stat().st_mtime_ns,mtime)

    def value(self):
        return {'schema':module.SCHEMA,'generated_at':time.time(),'channel':{'name':'TokyoFlightTracker','url':'https://www.twitch.tv/tokyoflighttracker'},'observation':{'provider_state':'UNKNOWN','status':'NOT_CONFIGURED','observed_at':None,'source_fresh':False},'session':{'id':None,'started_at':None,'viewer_count':None},'reception':{'verified_at':None,'video':None,'audio':None,'non_silent':False}}
    def test_public_artifact_is_accepted(self):
        self.assertEqual(module.validate(self.value())['observation']['provider_state'],'UNKNOWN')
    def test_extra_private_data_is_rejected(self):
        for section in ['observation','session','reception']:
            value=self.value();value[section]['private_token']='private-secret'
            with self.assertRaises(ValueError):module.validate(value)
    def test_arbitrary_link_and_stale_payload_rejected(self):
        value=self.value();value['channel']['url']='https://untrusted.invalid'
        with self.assertRaises(ValueError):module.validate(value)
    def test_source_failure_keeps_old_timestamp_and_bytes(self):
        with tempfile.TemporaryDirectory() as directory:
            root=pathlib.Path(directory);value=self.value();value['generated_at']-=400
            path=root/'twitch-observation.json';path.write_text(json.dumps(value));before=path.read_bytes()
            with mock.patch.object(module,'PUBLIC',root),mock.patch.object(module.subprocess,'run',return_value=mock.Mock(returncode=1,stdout=b'',stderr=b'private error')):
                self.assertEqual(module.main(),0)
            self.assertEqual(path.read_bytes(),before)
        value=self.value();value['generated_at']-=181
        with self.assertRaises(ValueError):module.validate(value)
