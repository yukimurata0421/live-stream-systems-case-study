import time
import pytest
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


def test_pair_diagnostics_keep_failed_retry_without_changing_original_readings():
    analyzer=VerifiedClockAnalyzer('fixture',VerifiedClockConfig((0,0,.5,1),(.5,0,.5,1)))
    pair=iter([ClockReading('09:00:00',96),ClockReading('00:00:00',45),
               ClockReading('09:00:00',97),ClockReading(error='deadline_exceeded')])
    frame=GrayImage(40,20,bytes((v%240)+13 for v in range(800)))
    row,_=analyzer.analyze(Capture(frame,'source',86400,1,'h264'),lambda _:next(pair),now=86400)
    assert row['reason']=='clock_pair_unreliable' and row['ocr_utc_current']=='00:00:00'
    assert row['ocr_utc_confidence_current']==45
    assert row['ocr_pair_stages']['original']['utc']['confidence']==45
    assert row['ocr_pair_stages']['contrast']['utc']['error']=='deadline_exceeded'
    assert not row['ocr_pair_stages']['contrast']['accepted']
    bounds=row['clock_evidence_bounds'];x,y,_,_=bounds['utc_context'];left,top,right,bottom=bounds['utc']
    context=analyzer.evidence_images['utc_context']
    recovered=context.crop(((left-x)/context.width,(top-y)/context.height,(right-left)/context.width,(bottom-top)/context.height))
    assert recovered==analyzer.evidence_images['utc']


def test_successful_retry_records_both_stages_and_normal_frame_keeps_no_evidence():
    analyzer=VerifiedClockAnalyzer('fixture',VerifiedClockConfig((0,0,.5,1),(.5,0,.5,1)))
    values=iter([ClockReading('09:00:00',96),ClockReading('00:00:00',45),
                 ClockReading('09:00:00',97),ClockReading('00:00:00',92)])
    row,images=analyzer.analyze(Capture(GrayImage(40,20,b'\x80'*800),'source',86400,1,'h264'),lambda _:next(values),now=86400)
    assert row['ocr_verified'] and row['ocr_utc_confidence_current']==92
    assert not row['ocr_pair_stages']['original']['accepted'] and row['ocr_pair_stages']['contrast']['accepted']
    assert images is None and analyzer.evidence_images=={}


def test_periodic_sample_is_explicit_and_does_not_change_clock_verdict():
    analyzer=VerifiedClockAnalyzer('fixture',VerifiedClockConfig((0,0,.5,1),(.5,0,.5,1)))
    for second in ('00','10'):
        values=iter([ClockReading('09:00:'+second,97),ClockReading('00:00:'+second,96)])
        row,images=analyzer.analyze(Capture(GrayImage(40,20,b'\x80'*800),'source',86400+int(second),int(second),'h264'),lambda _:next(values),now=86400+int(second),retain_evidence=True)
    assert row['clock_state']=='ADVANCING_CONFIRMED' and row['clock_evidence_kind']=='periodic_roi'
    assert images is None and set(analyzer.evidence_images)=={'jst','utc','utc_context'}


@pytest.mark.parametrize('contrast',[False,True])
def test_accepted_current_roi_never_reads_expanded_roi(contrast):
    analyzer=VerifiedClockAnalyzer('fixture',VerifiedClockConfig())
    first=ClockReading('09:00:00',97);second=ClockReading('00:00:00',96)
    values=iter(([first,ClockReading('00:00:00',45)] if contrast else [])+[first,second])
    def no_expansion():raise AssertionError('accepted current ROI must not expand')
    frame=GrayImage(40,20,b'\x80'*800)
    result=analyzer._read_pair(frame,frame,lambda _:next(values),expanded_second=no_expansion)
    assert result==(first,second,contrast,True)
    assert analyzer.selected_roi_method=='current' and not analyzer.expanded_utc_attempted


def test_expanded_utc_uses_original_jst_and_only_one_extra_read():
    analyzer=VerifiedClockAnalyzer('fixture',VerifiedClockConfig())
    anchor=ClockReading('09:00:00',97);values=iter([anchor,ClockReading('00:00:00',45),
        ClockReading('09:00:00',96),ClockReading('00:00:00',55),ClockReading('00:00:00',96)])
    calls=[];frame=GrayImage(40,20,b'\x80'*800);wider=GrayImage(43,20,b'\x80'*860)
    def read(image):calls.append(image);return next(values)
    result=analyzer._read_pair(frame,frame,read,expanded_second=lambda:wider)
    assert result[0] is anchor and result[1].text=='00:00:00' and result[3]
    assert len(calls)==5 and calls[-1] is wider
    assert analyzer.ocr_pair_stages['expanded_utc']['accepted'] and analyzer.selected_roi_method=='expanded_utc'


@pytest.mark.parametrize('case',['weak_jst','conflicting_utc','conflicting_contrast','process_failure'])
def test_expansion_does_not_resolve_unreliable_anchors_or_conflicts(case):
    analyzer=VerifiedClockAnalyzer('fixture',VerifiedClockConfig())
    pair=[ClockReading('09:00:00',97),ClockReading('00:00:00',45)]
    retry=list(pair)
    if case=='weak_jst':pair[0]=retry[0]=ClockReading('09:00:00',69)
    if case=='conflicting_utc':pair[1]=retry[1]=ClockReading('00:00:01',45)
    if case=='conflicting_contrast':retry[1]=ClockReading('00:00:01',45)
    if case=='process_failure':retry[1]=ClockReading(error='deadline_exceeded')
    values=iter(pair+retry);frame=GrayImage(40,20,b'\x80'*800)
    def no_expansion():raise AssertionError('conflict must not request new ROI')
    result=analyzer._read_pair(frame,frame,lambda _:next(values),expanded_second=no_expansion)
    assert not result[3] and not analyzer.expanded_utc_attempted


@pytest.mark.parametrize('alternative',[ClockReading('00:00:01',98),ClockReading('00:00:00',69),ClockReading(error='deadline_exceeded')])
def test_failed_expansion_keeps_original_reading_and_uncertainty(alternative):
    analyzer=VerifiedClockAnalyzer('fixture',VerifiedClockConfig());anchor=ClockReading('09:00:00',97);original=ClockReading('00:00:00',45)
    values=iter([anchor,original,anchor,original,alternative]);frame=GrayImage(40,20,b'\x80'*800)
    result=analyzer._read_pair(frame,frame,lambda _:next(values),expanded_second=lambda:frame)
    assert result==(anchor,original,True,False) and not analyzer.ocr_pair_stages['expanded_utc']['accepted']


def test_expanded_reading_preserves_freeze_and_time_jump_detection():
    analyzer=VerifiedClockAnalyzer('fixture',VerifiedClockConfig((0,0,.5,1),(.5,0,.5,1)))
    def sample(mono,second):
        jst=ClockReading('09:00:'+second,97);utc=ClockReading('00:00:'+second,45)
        values=iter([jst,utc,jst,utc,ClockReading(utc.text,96)])
        return analyzer.analyze(Capture(GrayImage(40,20,b'\x80'*800),'source',86400+mono,mono,'h264'),lambda _:next(values),now=86400+mono,utc_retry_left_pixels=3)[0]
    assert sample(1,'00')['clock_state']=='WARMUP'
    assert sample(11,'00')['clock_state']=='STALLED_SUSPECT'
    assert sample(71,'00')['clock_state']=='STALLED_CONFIRMED'
    row=sample(81,'59');assert row['clock_state']=='OCR_UNCERTAIN' and row['reason']=='clock_time_jump'
