import importlib.util,json,pathlib,time,unittest,tempfile
from unittest import mock
ROOT=pathlib.Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location('public_twitch',ROOT/'ops/public-publisher/site/scripts/collect_twitch_public.py')
module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)

class PublicTwitchTests(unittest.TestCase):
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
