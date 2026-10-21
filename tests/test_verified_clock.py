import time
from stream_v3.verified_clock import VerifiedClockAnalyzer,VerifiedClockConfig
from stream_v3.viewer_clock import Capture,GrayImage,ClockReading


def test_pair_every_sample_caches_prior_and_rejects_confident_misread():
    analyzer=VerifiedClockAnalyzer('fixture',VerifiedClockConfig((0,0,.5,1),(.5,0,.5,1)))
    calls=[]
    def sample(second,mono,*,bad=False):
        values=iter([ClockReading('09:00:'+second,97),ClockReading('00:00:'+('54' if bad else second),97)]*2)
        def ocr(_):calls.append(1);return next(values)
        return analyzer.analyze(Capture(GrayImage(40,20,b'\x80'*800),'source',86400+int(second)+12,mono,'h264'),ocr,now=86400+int(second)+12)[0]
    assert sample('00',1)['clock_state']=='WARMUP'
    good=sample('10',11);assert good['clock_state']=='ADVANCING_CONFIRMED' and len(calls)==4
    assert good['stream_delay_seconds']==12 and good['delay_histogram']['count']==1
    bad=sample('49',21,bad=True)
    assert bad['clock_state']=='OCR_UNCERTAIN' and not bad['ocr_verified']
    assert bad['delay_histogram']['count']==1
    assert sample('20',21)['clock_state']=='ADVANCING_CONFIRMED'


def test_missing_intervals_preserve_clock_without_confirming_unobserved_freeze():
    analyzer=VerifiedClockAnalyzer('fixture',VerifiedClockConfig((0,0,.5,1),(.5,0,.5,1)))
    def sample(mono):
        pair=iter([ClockReading('09:00:00',99),ClockReading('00:00:00',99)])
        return analyzer.analyze(Capture(GrayImage(40,20,b'\x80'*800),'source',86400+mono,mono,'h264'),lambda _:next(pair),now=86400+mono)[0]
    sample(1);assert sample(11)['clock_state']=='STALLED_SUSPECT'
    analyzer.unknown('capture_timeout',now=86430)
    row=sample(81)
    assert row['clock_state']=='STALLED_SUSPECT' and row['clock_frozen_seconds']==80
    assert row['clock_frozen_observed_seconds']==10 and row['freeze_has_observation_gap']
    row=sample(131)
    assert row['clock_state']=='STALLED_CONFIRMED' and row['clock_frozen_observed_seconds']==60


def test_midnight_cadence_drift_and_timing_invalid():
    analyzer=VerifiedClockAnalyzer('fixture',VerifiedClockConfig((0,0,.5,1),(.5,0,.5,1)))
    def sample(jst,utc,epoch,mono,timing=True):
        pair=iter([ClockReading(jst,99),ClockReading(utc,99)])
        return analyzer.analyze(Capture(GrayImage(40,20,b'\x80'*800),'source',epoch,mono,'h264'),lambda _:next(pair),now=epoch,timing_valid=timing)[0]
    sample('08:59:54','23:59:54',86400+6,1)
    row=sample('09:00:04','00:00:04',86400+16,11,False)
    assert row['clock_state']=='ADVANCING_CONFIRMED' and not row['delay_valid']
    row=sample('09:00:06','00:00:06',86400+26,21)
    assert row['clock_state']=='OCR_UNCERTAIN' and row['reason']=='clock_cadence_drift'
