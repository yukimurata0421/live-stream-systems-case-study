"""Observe clocks in official Twitch thumbnails without claiming public-player E2E health."""
from datetime import datetime
from email.utils import parsedate_to_datetime
import hashlib
import io
import json
import math
from pathlib import Path
import time
from urllib.error import HTTPError,URLError
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler,ProxyHandler,Request,build_opener

from .viewer_clock import GrayImage,utc_timestamp
from .viewer_clock_capture import ProbeError
from .verified_clock import VerifiedClockAnalyzer,VerifiedClockConfig

SCHEMA='stream_v3.twitch_thumbnail_observation.v1'
SCOPE='twitch_provider_thumbnail'


def epoch(value):
    try:
        stamp=datetime.fromisoformat(value.replace('Z','+00:00'))
        return stamp.timestamp() if stamp.tzinfo else None
    except (ValueError,AttributeError,TypeError,OverflowError):return None


def load_json(path):
    raw=Path(path).read_bytes()
    if len(raw)>65536:raise ProbeError('source_too_large')
    value=json.loads(raw)
    if not isinstance(value,dict):raise ProbeError('source_invalid')
    return value


def api_status(api,config,*,now):
    stamp,until=epoch(api.get('collected_at')),epoch(api.get('valid_until'))
    if (api.get('schema')!='monitoring_v4.twitch_api_evidence.v1' or api.get('source_host_id')!='arena-monitoring-facts'
            or api.get('source_boot_id')!=config['boot_id'] or api.get('user_id')!=config['user_id']
            or api.get('user_login')!=config['user_login'] or api.get('probe_status')!='ok'
            or stamp is None or until is None or not -2<=now-stamp<=180 or now>until):return None
    return True if api.get('provider_state')=='LIVE' else False if api.get('provider_state')=='OFFLINE' else None


def validate_source(source,api,config,*,now):
    if source.get('schema')!='stream_v3.twitch_thumbnail_source.v1' or api.get('schema')!='monitoring_v4.twitch_api_evidence.v1':raise ProbeError('source_schema_invalid')
    fields=('source_host_id','source_boot_id','collector_instance','collector_revision','sequence','user_id','user_login',
            'stream_id','started_at','provider_state','probe_status','collected_at','valid_until')
    if any(source.get(k)!=api.get(k) for k in fields):raise ProbeError('api_source_not_yet_published')
    if (source.get('source_host_id')!='arena-monitoring-facts' or source.get('source_boot_id')!=config['boot_id']
            or source.get('user_id')!=config['user_id'] or source.get('user_login')!=config['user_login']):raise ProbeError('source_identity_invalid')
    stamp,until=epoch(source.get('collected_at')),epoch(source.get('valid_until'))
    if stamp is None or until is None or not -2<=now-stamp<=180 or not now<=until<=stamp+300:raise ProbeError('api_observation_stale')
    if source.get('probe_status')!='ok':raise ProbeError('api_observation_unavailable')
    if source.get('provider_state')=='OFFLINE':return None
    if source.get('provider_state')!='LIVE':raise ProbeError('api_observation_unavailable')
    if source.get('thumbnail_status')!='available':raise ProbeError('thumbnail_url_unavailable')
    return source


def image_url(template,login,width,height):
    try:
        p=urlsplit(template)
        if (p.scheme!='https' or p.hostname!='static-cdn.jtvnw.net' or p.port not in (None,443)
                or p.username is not None or p.password is not None or p.query or p.fragment
                or p.path!='/previews-ttv/live_user_'+login+'-{width}x{height}.jpg'):raise ValueError()
        if (width,height) not in ((1280,720),(1920,1080)):raise ValueError()
        return template.replace('{width}',str(width)).replace('{height}',str(height))
    except (TypeError,ValueError,AttributeError):raise ProbeError('thumbnail_url_invalid') from None


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self,req,fp,code,msg,headers,newurl):
        fp.close();raise ProbeError('thumbnail_redirect_rejected')


class ThumbnailHTTP:
    def __init__(self):
        self.opener=build_opener(ProxyHandler({}),NoRedirect());self.etag=None;self.cache_url=None
        self.retry_at=0.

    def get(self,url,*,deadline):
        if time.monotonic()<self.retry_at:raise ProbeError('thumbnail_retry_after')
        headers={'User-Agent':'stream-v3-thumbnail-observer/1.0','Accept':'image/jpeg'}
        if url==self.cache_url and self.etag:headers['If-None-Match']=self.etag
        try:
            with self.opener.open(Request(url,headers=headers),timeout=max(.1,min(6,deadline-time.monotonic()))) as response:
                if response.headers.get_content_type()!='image/jpeg':raise ProbeError('thumbnail_content_type_invalid')
                chunks=[];size=0
                while True:
                    if time.monotonic()>=deadline:raise ProbeError('thumbnail_timeout')
                    data=response.read1(min(65536,2*1024*1024+1-size))
                    if not data:break
                    chunks.append(data);size+=len(data)
                    if size>2*1024*1024:raise ProbeError('thumbnail_too_large')
                body=b''.join(chunks)
                tag=response.headers.get('ETag','')
                self.etag=tag if len(tag)<=256 and all(32<=ord(c)<127 for c in tag) else None
                self.cache_url=url
                return body,{'http_status':200,'download_bytes':size}
        except HTTPError as error:
            status=error.code
            if status==304 and self.cache_url==url:
                error.close();return None,{'http_status':304,'download_bytes':0}
            if status in (429,503):
                retry=error.headers.get('Retry-After','60')
                try:wait=float(retry)
                except (ValueError,TypeError):
                    try:wait=parsedate_to_datetime(retry).timestamp()-time.time()
                    except (ValueError,TypeError,AttributeError,OverflowError):wait=60.
                self.retry_at=time.monotonic()+min(86400,max(60,wait if math.isfinite(wait) else 60))
            error.close();raise ProbeError('thumbnail_rate_limited' if status==429 else 'thumbnail_http_failed') from None
        except (TimeoutError,URLError,OSError):raise ProbeError('thumbnail_transport_failed') from None


def decode_rois(body,config):
    from PIL import Image
    try:
        with Image.open(io.BytesIO(body)) as image:
            if image.format!='JPEG' or not (320<=image.width<=1920 and 180<=image.height<=1080):raise ValueError()
            image.load();size=image.size
            # Keep only the two small regions after this call.
            gray=GrayImage(image.width,image.height,image.convert('L').tobytes())
            return (gray.crop(tuple(config.get('jst_roi',(.019,.968,.04,.022)))),
                    gray.crop(tuple(config.get('utc_roi',(.081,.968,.04,.022))))),size
    except (OSError,ValueError):raise ProbeError('thumbnail_image_invalid') from None


class ThumbnailAnalyzer:
    def __init__(self,config,previous=None):
        self.config=config;self.pair=VerifiedClockAnalyzer('twitch_thumbnail',VerifiedClockConfig())
        self.previous=previous if isinstance(previous,dict) else {};self.rois=None
        self.verified=self.previous.get('last_verified',{})
        if self.previous.get('ocr_verified') is True:self.verified=self.previous

    def remember(self,row):
        if row.get('ocr_verified') is True:
            self.verified={k:v for k,v in row.items() if k!='last_verified'}
        row['last_verified']={k:v for k,v in self.verified.items() if k!='last_verified'}
        self.previous=row
        return row

    def observe(self,body,source,ocr,*,now,forward_flowing=None):
        previous=self.previous;identity=source['stream_id'];same=previous.get('stream_id')==identity
        row={'schema':SCHEMA,'target':'twitch_thumbnail','scope':SCOPE,'checked_at_utc':utc_timestamp(now),
             'api_collected_at_utc':source['collected_at'],'api_live':True,'stream_id':identity,
             'capture_ok':True,'ocr_verified':False,'clock_advancing':False,'clock_state':'OCR_UNCERTAIN',
             'reason':'clock_pair_unreliable','unique_sample':False,'image_duplicate':False,
             'unique_samples_total':previous.get('unique_samples_total',0),'forward_flowing':forward_flowing,
             'last_progress_at_utc':previous.get('last_progress_at_utc') if same else None,
             'last_valid_observation_at_utc':previous.get('last_valid_observation_at_utc') or self.verified.get('last_valid_observation_at_utc')}
        digest=hashlib.sha256(body).hexdigest() if body is not None else previous.get('image_sha256')
        duplicate=same and digest is not None and digest==previous.get('image_sha256')
        if body is None and not duplicate:raise ProbeError('thumbnail_cache_unavailable')
        row.update(image_sha256=digest,image_duplicate=duplicate)
        if duplicate and previous.get('ocr_verified') is True:
            if self.rois is None and body is not None:self.rois,_=decode_rois(body,self.config)
            for key in ('ocr_jst','ocr_utc','ocr_confidence_jst','ocr_confidence_utc','display_clock_at_utc','width','height','clock_pair_consistent'):
                row[key]=previous.get(key)
            stamp=epoch(row.get('display_clock_at_utc'))
        else:
            if body is None:raise ProbeError('thumbnail_cache_unverified')
            self.rois,(row['width'],row['height'])=decode_rois(body,self.config)
            jst,utc,retried,consistent=self.pair._read_pair(*self.rois,ocr)
            row.update(ocr_jst=jst.text,ocr_utc=utc.text,ocr_confidence_jst=jst.confidence,ocr_confidence_utc=utc.confidence,
                       clock_pair_consistent=consistent,ocr_retried=retried)
            if not consistent:return self.remember(row)
            seconds=utc.seconds(70);midnight=math.floor(now/86400)*86400
            stamp=min((midnight+seconds+x for x in (-86400,0,86400)),key=lambda x:abs(now-x))
            # A changed JPEG can still represent the same provider sample.
            if self.verified.get('stream_id')==identity and utc.text==self.verified.get('ocr_utc'):
                stamp=epoch(self.verified.get('display_clock_at_utc'))
            row['display_clock_at_utc']=utc_timestamp(stamp)
        started=epoch(source.get('started_at'))
        if stamp is None or not -2<=now-stamp or started is None or stamp<started-30:
            row.update(reason='thumbnail_clock_or_stream_binding_uncertain');return self.remember(row)
        before=epoch(self.verified.get('display_clock_at_utc')) if self.verified.get('stream_id')==identity else None
        if before is not None and stamp<before:
            row.update(reason='thumbnail_clock_regressed');return self.remember(row)
        progressed=before is not None and stamp>before
        unique=before is None or stamp>before
        row.update(ocr_verified=True,clock_state='CLOCK_OBSERVED' if before is None else 'ADVANCING_CONFIRMED' if progressed else 'UNCHANGED',
                   reason='thumbnail_clock_observed' if before is None else 'thumbnail_clock_advanced' if progressed else 'thumbnail_cached_frame',
                   clock_advancing=progressed,unique_sample=unique,frame_age_seconds=max(0.,now-stamp),
                   last_valid_observation_at_utc=utc_timestamp(now))
        if unique:row['unique_samples_total']+=1
        if progressed:row['last_progress_at_utc']=utc_timestamp(now)
        age=max(0.,now-stamp)
        row['freshness_state']='FRESH' if age<=360 else 'AGING' if age<=600 else 'STALE_CORRELATED' if age>900 and forward_flowing is True else 'STALE_SUSPECT'
        row['recovery_unixtime']=now if age<=360 else 0.
        return self.remember(row)
