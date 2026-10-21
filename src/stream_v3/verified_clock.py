"""Verify JST/UTC on every sample; separate missing data, freeze evidence, reading quality, and display age."""
from dataclasses import dataclass
import math

from .viewer_clock import ClockReading, image_difference, utc_timestamp

BUCKETS=(.25,.5,1.,2.,5.,10.,20.,40.,80.,160.,300.)


@dataclass(frozen=True)
class VerifiedClockConfig:
    jst_roi: tuple = (.019,.968,.04,.022)
    utc_roi: tuple = (.081,.968,.04,.022)
    minimum_confidence: float = 70.
    progression_tolerance_seconds: float = 5.
    drift_limit_seconds: float = 30.
    stall_seconds: float = 60.
    maximum_gap_seconds: float = 900.


class VerifiedClockAnalyzer:
    def __init__(self,platform,config):
        self.platform,self.config=platform,config
        self.previous=None;self.identity=None;self.first_value_at=None
        self.last_valid_epoch=None;self.last_capture_epoch=None
        self.stalls=0;self.failures=0;self.gap_since_valid=False;self.freeze_has_gap=False
        self.frozen_span=0.;self.frozen_observed=0.
        self.stats={'observations':0,'ocr_uncertain':0,'cadence_drift':0,'valid_clock':0}
        self.histogram={'bounds':list(BUCKETS),'buckets':[0]*len(BUCKETS),'count':0,'sum':0.}

    def _metadata(self,row,now):
        row.update(clock_frozen_seconds=round(self.frozen_span,3),clock_frozen_observed_seconds=round(self.frozen_observed,3),
                   freeze_has_observation_gap=self.freeze_has_gap,
                   last_valid_observation_at_utc=utc_timestamp(self.last_valid_epoch) if self.last_valid_epoch is not None else None,
                   observation_counters=dict(self.stats),delay_histogram={k:list(v) if isinstance(v,list) else v for k,v in self.histogram.items()})
        return row

    def unknown(self,reason,*,now):
        self.failures+=1;self.gap_since_valid=True;self.stats['observations']+=1
        return self._metadata({'platform':self.platform,'checked_at_utc':utc_timestamp(now),
                              'captured_at_utc':utc_timestamp(self.last_capture_epoch) if self.last_capture_epoch is not None else None,
                              'capture_ok':False,'clock_state':'UNKNOWN','ocr_verified':False,'reason':reason,
                              'consecutive_failures':self.failures,'consecutive_stalls':self.stalls,
                              'frame_state':'UNKNOWN','delay_valid':False},now)

    def _read_pair(self,first,second,ocr):
        def read(image):
            try:return ocr(image)
            except Exception:return ClockReading(error='ocr_failed')
        jst,utc=read(first),read(second)
        def valid(a,b):
            left,right=a.seconds(self.config.minimum_confidence),b.seconds(self.config.minimum_confidence)
            return left is not None and right is not None and (left-right)%86400==9*3600
        if valid(jst,utc):return jst,utc,False,True
        # One alternate contrast pass; repeated transforms are not independent votes.
        def contrasted(image):
            from .viewer_clock import GrayImage
            low,high=min(image.pixels),max(image.pixels)
            if high-low<16:return image
            return GrayImage(image.width,image.height,bytes(round((v-low)*255/(high-low)) for v in image.pixels))
        other_jst,other_utc=read(contrasted(first)),read(contrasted(second))
        if valid(other_jst,other_utc):return other_jst,other_utc,True,True
        return jst,utc,True,False

    def analyze(self,capture,ocr,*,now,timing_valid=True):
        cfg=self.config
        if capture.identity!=self.identity:
            self.previous=None;self.first_value_at=None;self.stalls=0
            self.frozen_span=self.frozen_observed=0.;self.freeze_has_gap=False;self.gap_since_valid=False
            self.identity=capture.identity
        try:jst_image,utc_image=capture.frame.crop(cfg.jst_roi),capture.frame.crop(cfg.utc_roi)
        except ValueError:return self.unknown('roi_invalid_or_too_small',now=now),None
        self.stats['observations']+=1;self.last_capture_epoch=capture.captured_epoch;self.failures=0
        row={'platform':self.platform,'checked_at_utc':utc_timestamp(now),'captured_at_utc':utc_timestamp(capture.captured_epoch),
             'capture_ok':True,'clock_state':'WARMUP','ocr_verified':False,'reason':'comparison_unavailable',
             'capture_width':capture.frame.width,'capture_height':capture.frame.height,'codec':capture.codec,
             'capture_duration_seconds':round(capture.duration_seconds,3),'source_width':capture.source_width,'source_height':capture.source_height,
             'sample_age_seconds':max(0.,now-capture.captured_epoch),'consecutive_failures':0,'consecutive_stalls':self.stalls,
             'frame_state':'WARMUP','clock_anomaly':None,'delay_valid':False,'ocr_trigger':'every_sample'}
        if sum(x<=12 for x in capture.frame.pixels)/len(capture.frame.pixels)>=.995:
            self.gap_since_valid=True;row.update(clock_state='BLACK_FRAME',frame_state='BLACK_FRAME',reason='black_frame')
            return self._metadata(row,now),(jst_image,jst_image)
        jst,utc,retried,consistent=self._read_pair(jst_image,utc_image,ocr)
        row.update(ocr_current=jst.text,ocr_utc_current=utc.text,ocr_confidence_current=jst.confidence,
                   ocr_utc_confidence_current=utc.confidence,clock_pair_consistent=consistent,ocr_retried=retried)
        previous=self.previous
        if not consistent:
            self.stats['ocr_uncertain']+=1;self.gap_since_valid=True
            row.update(clock_state='OCR_UNCERTAIN',reason='clock_pair_unreliable',ocr_error=jst.error or utc.error or 'low_confidence_or_pair_mismatch')
            return self._metadata(row,now),(previous['roi'] if previous else jst_image,jst_image)
        value=utc.seconds(cfg.minimum_confidence)
        assert value is not None
        self.stats['valid_clock']+=1;self.last_valid_epoch=capture.captured_epoch
        row.update(ocr_verified=True,clock_reading_valid=True)
        # Seconds-only display: choose the nearest UTC day; precision remains at least one second.
        midnight=math.floor(capture.captured_epoch/86400)*86400
        stamp=min((midnight+value+offset for offset in (-86400,0,86400)),key=lambda x:abs(capture.captured_epoch-x))
        display_age=capture.captured_epoch-stamp
        row.update(display_clock_at_utc=utc_timestamp(stamp),display_age_seconds=round(display_age,3),display_time_quantization_seconds=1.)
        comparable=previous and 0<capture.captured_monotonic-previous['mono']<=cfg.maximum_gap_seconds
        if comparable:
            gap=capture.captured_monotonic-previous['mono'];forward=(value-previous['value'])%86400
            if forward>43200:forward-=86400
            row.update(ocr_previous=previous['jst'].text,ocr_utc_previous=previous['utc'].text,
                       ocr_confidence_previous=previous['jst'].confidence,ocr_utc_confidence_previous=previous['utc'].confidence,
                       clock_delta_seconds=forward,observation_elapsed_seconds=round(gap,3),cadence_error_seconds=round(forward-gap,3),
                       image_difference=image_difference(previous['roi'],jst_image),
                       frame_state='CHANGING' if image_difference(previous['summary'],capture.frame.reduced())>=.015 else 'STATIC_SUSPECT')
            if forward==0:
                self.stalls+=1;self.frozen_span=capture.captured_monotonic-self.first_value_at
                if not self.gap_since_valid:self.frozen_observed+=gap
                self.freeze_has_gap=self.freeze_has_gap or self.gap_since_valid
                row.update(clock_state='STALLED_CONFIRMED' if self.stalls>=2 and self.frozen_observed>=cfg.stall_seconds else 'STALLED_SUSPECT',reason='ocr_time_unchanged')
            else:
                self.stalls=0;self.first_value_at=capture.captured_monotonic
                self.frozen_span=self.frozen_observed=0.;self.freeze_has_gap=False
                error=abs(forward-gap)
                if forward>0 and error<=cfg.progression_tolerance_seconds:
                    row.update(clock_state='ADVANCING_CONFIRMED',reason='ocr_time_advanced',cadence_status='consistent')
                else:
                    drift=forward>0 and error<=cfg.drift_limit_seconds
                    self.stats['cadence_drift' if drift else 'ocr_uncertain']+=1
                    row.update(clock_state='OCR_UNCERTAIN',reason='clock_cadence_drift' if drift else 'clock_time_regressed' if forward<0 else 'clock_time_jump',
                               cadence_status='drift' if drift else 'uncertain',clock_anomaly='cadence_drift' if drift else 'time_regression' if forward<0 else 'time_jump')
        else:
            self.first_value_at=capture.captured_monotonic;self.stalls=0;self.frozen_span=self.frozen_observed=0.;self.freeze_has_gap=False
        self.previous={'value':value,'jst':jst,'utc':utc,'mono':capture.captured_monotonic,'roi':jst_image,'summary':capture.frame.reduced()}
        self.gap_since_valid=False
        row.update(consecutive_stalls=self.stalls,delay_valid=bool(timing_valid and row['clock_state']=='ADVANCING_CONFIRMED' and 0<=display_age<=43200))
        if row['delay_valid']:
            row['stream_delay_seconds']=round(display_age,3)
            self.histogram['count']+=1;self.histogram['sum']+=display_age
            for i,bound in enumerate(BUCKETS):
                if display_age<=bound:self.histogram['buckets'][i]+=1
        abnormal=row['clock_state'] in ('STALLED_SUSPECT','STALLED_CONFIRMED','OCR_UNCERTAIN')
        evidence=(previous['roi'] if previous else jst_image,jst_image) if abnormal else None
        return self._metadata(row,now),evidence
