import importlib.util
import json
import pathlib
import tempfile
import unittest
import sys
from unittest.mock import patch

SOURCE=pathlib.Path(__file__).resolve().parents[1]/'ops/public-publisher/site/scripts/collect_clock_public.py'
spec=importlib.util.spec_from_file_location('clock_public_collector',SOURCE);collector=importlib.util.module_from_spec(spec)
with patch.object(sys,'path',[str(SOURCE.parent),*sys.path]):spec.loader.exec_module(collector)
NOW=1792640000.
START=NOW-NOW%600-600*143


def state_rows(active,allowed,**labels):
    return [[{**labels,'state':s},[NOW,'1' if s==active else '0']] for s in allowed]


def raw(**changes):
    """Prometheus results shaped as the remote script returns them."""
    value={'now':NOW-5,'start':START,'end':START+600*143,'instant':{
        'oci_state':state_rows('ADVANCING_CONFIRMED',collector.CLOCK_STATES),
        'fb_state':state_rows('UNKNOWN',collector.CLOCK_STATES),
        'thumb_state':state_rows('AGING',collector.THUMB_STATES),
        'class':state_rows('target_good',collector.CLASSES,target='oci_relay')+state_rows('standby',collector.CLASSES,target='youtube_fallback'),
        'reason':[[{'target':'oci_relay','reason':'ocr_time_advanced'},[NOW,'1']],[{'target':'youtube_fallback','reason':'fixture free text'},[NOW,'1']]],
        'scalar':[[{'__name__':'stream_v3_oci_monitor_observer_heartbeat_ok'},[NOW,'1']],[{'__name__':'stream_v3_oci_monitor_sample_age_seconds'},[NOW,'3.4']],
                  [{'__name__':'stream_v3_youtube_fallback_monitor_expected_active'},[NOW,'0']],[{'__name__':'stream_v3_twitch_thumbnail_api_live'},[NOW,'1']],
                  [{'__name__':'stream_v3_twitch_thumbnail_clock_verified'},[NOW,'1']],[{'__name__':'stream_v3_twitch_thumbnail_frame_age_seconds'},[NOW,'420']]],
        'probe':[[{'__name__':'stream_v3_monitor_probe_stream_delay_seconds','target':'oci_relay'},[NOW,'4.05']]],
        'reads_24h':[[{'target':'oci_relay','kind':k},[NOW,v]] for k,v in (('observations','3852.9'),('valid_clock','2588.7'),('ocr_uncertain','263.6'),('cadence_drift','2.0'))],
        'thumb_samples_24h':[[{},[NOW,'15.04']]],'thumb_reads_24h':[[{},[NOW,'6.0']]],'thumb_retries_24h':[],
        'delay_p50_24h':[[{},[NOW,'3.5']]],'delay_p95_24h':[[{},[NOW,'NaN']]]},
     'range':{
        'class':[[{'target':'oci_relay','state':'target_good'},[[START+600*i,'1'] for i in range(130,144)]],
                 [{'target':'oci_relay','state':'target_bad'},[[START+600*130,'0.5']]],
                 [{'target':'youtube_fallback','state':'standby'},[[START+600*i,'1'] for i in range(144)]]],
        'thumb_state':[[{'state':'FRESH'},[[START+600*143,'0.6']]],[{'state':'AGING'},[[START+600*143,'0.4']]]],
        'oci_delay':[[{},[[START+600*143,'4.06']]]],'thumb_age':[[{},[[START+600*143,'433.8']]]]}}
    value.update(changes)
    return value


class PublicClockTests(unittest.TestCase):
    def test_projection_keeps_only_fixed_states_and_numbers(self):
        v=collector.build(raw(),NOW)
        relay,fb,thumb=v['sources']
        self.assertEqual((relay['state'],relay['classification'],relay['reason']),('ADVANCING_CONFIRMED','target_good','ocr_time_advanced'))
        self.assertEqual(relay['reads_24h'],{'observations':3853,'valid_clock':2589,'ocr_uncertain':264,'cadence_drift':2})
        self.assertIsNone(relay['delay_p95_24h'])  # NaN quantile is not published as a number
        self.assertEqual(fb['reason'],'other')    # unexpected label text never reaches the page
        self.assertEqual((fb['classification'],fb['expected_active']),('standby',False))
        self.assertEqual((thumb['state'],thumb['frame_age_seconds'],thumb['retries_24h']),('AGING',420.,None))
        self.assertNotIn('fixture free text',json.dumps(v))

    def test_buckets_without_data_stay_null_and_failures_are_visible(self):
        relay,fb,thumb=collector.build(raw(),NOW)['sources']
        self.assertIsNone(relay['timeline'][0])
        self.assertEqual(relay['timeline'][130],{'good':0.667,'bad':0.333,'unknown':0.,'standby':0.})
        self.assertEqual(fb['timeline'][0]['standby'],1.)
        self.assertEqual(thumb['timeline'][143]['good'],1.)
        self.assertEqual((relay['value_series'][143],relay['value_series'][0]),(4.06,None))

    def test_ambiguous_or_missing_state_is_unknown(self):
        r=raw();r['instant']['oci_state']=state_rows('WARMUP',collector.CLOCK_STATES)+[[{'state':'UNKNOWN'},[NOW,'1']]]
        r['instant']['thumb_state']=[]
        relay,_,thumb=collector.build(r,NOW)['sources']
        self.assertEqual((relay['state'],thumb['state']),('UNKNOWN','UNKNOWN'))

    def test_stale_source_and_unexpected_fields_are_rejected(self):
        with self.assertRaises(ValueError):collector.build(raw(now=NOW-200),NOW)
        v=collector.build(raw(),NOW);v['sources'][0]['url']='fixture-only'
        with self.assertRaises(ValueError):collector.validate(v,NOW)

    def test_clock_skew_is_bounded_and_original_age_limit_is_preserved(self):
        for offset in (0.028,0.05,1,-120):
            v=collector.build(raw(now=NOW+offset),NOW)
            self.assertEqual(v['generated_at'],round(NOW+offset,3))
            self.assertIs(collector.validate(v,NOW),v)
        for offset in (1.002,-120.002):
            with self.assertRaises(ValueError):collector.build(raw(now=NOW+offset),NOW)
            v=collector.build(raw(),NOW);v['generated_at']=NOW+offset
            with self.assertRaises(ValueError):collector.validate(v,NOW)

    def test_clock_future_rejection_retains_last_good_file_and_reason(self):
        with tempfile.TemporaryDirectory() as directory:
            root=pathlib.Path(directory);p=root/'clock-observation.json';p.write_text(json.dumps(collector.build(raw(),NOW)))
            before=p.read_bytes();stamp=p.stat().st_mtime_ns
            result=type('R',(),{'returncode':0,'stdout':json.dumps(raw(now=NOW+2)).encode()})()
            with patch.object(collector,'PUBLIC',root),patch.object(collector.subprocess,'run',return_value=result),patch.object(collector.time,'time',return_value=NOW),patch('builtins.print') as output:
                self.assertEqual(collector.main(),0)
            self.assertEqual((p.read_bytes(),p.stat().st_mtime_ns),(before,stamp))
            self.assertEqual(json.loads(output.call_args.args[0])['reason'],'PUBLIC_TIMESTAMP_FUTURE')
        v=collector.build(raw(),NOW);v['sources'][2]['state']='fixture-only'
        with self.assertRaises(ValueError):collector.validate(v,NOW)
        v=collector.build(raw(),NOW);v['sources'][0]['sample_age_seconds']=True
        with self.assertRaises(ValueError):collector.validate(v,NOW)

    def test_failed_fetch_preserves_artifact_bytes_and_timestamp(self):
        with tempfile.TemporaryDirectory() as d:
            root=pathlib.Path(d);p=root/'clock-observation.json';p.write_text('{"fixture":1}\n');before=p.read_bytes();stamp=p.stat().st_mtime_ns
            with patch.object(collector,'PUBLIC',root),patch.object(collector.subprocess,'run',side_effect=OSError('fixture')):
                self.assertEqual(collector.main(),0)
            self.assertEqual(p.read_bytes(),before);self.assertEqual(p.stat().st_mtime_ns,stamp)

    def test_successful_fetch_writes_validated_artifact(self):
        result=type('R',(),{'returncode':0,'stdout':json.dumps(raw()).encode()})()
        with tempfile.TemporaryDirectory() as d,patch.object(collector,'PUBLIC',pathlib.Path(d)),\
             patch.object(collector.subprocess,'run',return_value=result),patch.object(collector.time,'time',return_value=NOW):
            self.assertEqual(collector.main(),0)
            v=json.loads((pathlib.Path(d)/'clock-observation.json').read_text())
        self.assertEqual(v['schema'],'stream.clock_observation_public.v1')


if __name__=='__main__':unittest.main()
