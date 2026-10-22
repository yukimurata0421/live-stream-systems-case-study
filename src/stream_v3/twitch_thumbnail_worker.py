"""Poll and OCR thumbnails without repeating Helix calls or storing ordinary images."""
import argparse
import fcntl
import json
import os
from pathlib import Path
import signal
import threading
import time
from urllib.request import ProxyHandler,build_opener

from .twitch_thumbnail import (SCHEMA,SCOPE,ThumbnailAnalyzer,ThumbnailHTTP,api_status,epoch,image_url,load_json,validate_source)
from .viewer_clock import utc_timestamp
from .viewer_clock_capture import ProcessRunner,ProbeError,TesseractOCR
from .viewer_clock_worker import EvidenceStore,append_history


def publish(path,value):
    temp=path.with_name('.'+path.name+'.tmp')
    try:
        with temp.open('w') as file:
            os.chmod(temp,0o640);json.dump(value,file,sort_keys=True,allow_nan=False);file.write('\n');file.flush();os.fsync(file.fileno())
        os.replace(temp,path)
    finally:temp.unlink(missing_ok=True)


def native_forward(now):
    try:
        opener=build_opener(ProxyHandler({}))
        with opener.open('http://127.0.0.1:9116/twitch-delivery',timeout=2) as response:raw=response.read(32769)
        if len(raw)>32768:return None
        value=json.loads(raw);stamp=epoch(value.get('observed_at'))
        if value.get('schema')!='stream.relay_public.v1' or value.get('source_fresh') is not True or stamp is None or not -2<=now-stamp<=90:return None
        states=(value['relay']['status'],value['input']['status'],value['destinations']['twitch']['status'])
        flowing=True if states==('RUNNING','FLOWING','FLOWING') else False if any(s in ('STOPPED','STALLED','DISCONNECTED') for s in states) else None
        return {'flowing':flowing,'observed_at_utc':value['observed_at']}
    except (OSError,ValueError,KeyError,TypeError):return None


class Worker:
    def __init__(self,config,root,*,http=None,ocr=None):
        self.config=config;self.root=root;root.mkdir(parents=True,exist_ok=True)
        self.state_file=root/'observation.json'
        try:self.previous=load_json(self.state_file)
        except (OSError,ValueError,ProbeError):self.previous={}
        self.runner=ProcessRunner();self.http=http or ThumbnailHTTP()
        self.ocr=ocr or TesseractOCR(self.runner,binary=config['tesseract'],tessdata_dir=config['tessdata_dir'])
        self.analyzer=ThumbnailAnalyzer(config,self.previous)
        self.store=EvidenceStore(root/'evidence',max_bytes=8*1024*1024,retention_seconds=7*86400,platforms=('twitch_thumbnail',))
        self.cycle=self.previous.get('cycle',0)

    def once(self):
        started=time.monotonic();now=time.time();self.cycle+=1
        row={'schema':SCHEMA,'target':'twitch_thumbnail','platform':'twitch_thumbnail','scope':SCOPE,
             'checked_at_utc':utc_timestamp(now),'capture_ok':False,'ocr_verified':False,'clock_state':'UNKNOWN',
             'api_live':None,'reason':'source_unavailable','cycle':self.cycle,
             'unique_samples_total':self.previous.get('unique_samples_total',0),
             'last_valid_observation_at_utc':self.previous.get('last_valid_observation_at_utc'),
             'monitoring_started_at_utc':self.previous.get('monitoring_started_at_utc') or utc_timestamp(now),
             'observation_required_since_utc':self.previous.get('observation_required_since_utc'),
             'active_stream_id':self.previous.get('active_stream_id') or self.previous.get('stream_id') or self.analyzer.verified.get('stream_id'),
             'startup_grace_started_at_utc':self.previous.get('startup_grace_started_at_utc'),
             'capture_attempted':False,
             'ocr_attempted':False,'ocr_binary_retried':False,
             'last_request_at_utc':self.previous.get('last_request_at_utc'),
             'next_fetch_not_before_utc':self.previous.get('next_fetch_not_before_utc')}
        analysis_started=False
        try:
            api=load_json(self.config['api_file'])
            row['api_live']=api_status(api,self.config,now=now)
            if row['api_live'] is not None:row['api_collected_at_utc']=api['collected_at']
            if row['api_live'] is False:row['observation_required_since_utc']=None
            elif row['api_live'] is True and not row['observation_required_since_utc']:row['observation_required_since_utc']=utc_timestamp(now)
            identity=api.get('stream_id')
            if row['api_live'] is True and isinstance(identity,str) and identity:
                if identity!=row['active_stream_id']:
                    # Channel thumbnail URLs survive stream changes. Never send
                    # the old stream's ETag before obtaining this stream's body.
                    self.http.etag=None;self.http.cache_url=None
                    row.update(active_stream_id=identity,observation_required_since_utc=utc_timestamp(now),
                               startup_grace_started_at_utc=None)
                if row['startup_grace_started_at_utc'] is None:
                    started_at=epoch(api.get('started_at'))
                    # Restarting a worker on an old stream grants no new grace.
                    row['startup_grace_started_at_utc']=utc_timestamp(min(now,started_at) if started_at is not None else now)
            source=load_json(self.config['source_file'])
            source=validate_source(source,api,self.config,now=now)
            if source is None:
                row.update(clock_state='SOURCE_OFFLINE',reason='api_offline',api_live=False)
            else:
                row.update(api_live=True,api_collected_at_utc=source['collected_at'])
                cooldown=epoch(row.get('next_fetch_not_before_utc')) or 0
                last=epoch(row.get('last_request_at_utc')) or 0
                if now<max(cooldown,last+60):
                    waiting='WAITING_RETRY' if now<cooldown else 'WAITING_POLL'
                    row.update(clock_state=waiting,freshness_state=waiting,
                               reason='thumbnail_provider_backoff' if now<cooldown else 'thumbnail_poll_interval')
                else:
                    row.update(last_request_at_utc=utc_timestamp(now),capture_attempted=True)
                    url=image_url(source['thumbnail_url'],source['user_login'],1920,1080)
                    body,metadata=self.http.get(url,deadline=started+8)
                    forward=native_forward(time.time()) or {}
                    analysis_started=True
                    result=self.analyzer.observe(body,source,lambda image:self.ocr.read(image,deadline=min(started+15,time.monotonic()+2)),
                                                 now=time.time(),forward_flowing=forward.get('flowing'))
                    row.update(result,**metadata,forward_observed_at_utc=forward.get('observed_at_utc'))
                    del body
        except ProbeError as error:
            row['reason']=str(error)
            retry=getattr(self.http,'retry_at',0)-time.monotonic()
            if retry>0:row['next_fetch_not_before_utc']=utc_timestamp(time.time()+retry)
        except (OSError,ValueError,TypeError,KeyError):row['reason']='thumbnail_source_or_storage_unavailable'
        except Exception:row['reason']='thumbnail_analysis_failed'
        row.update(self.analyzer.ocr_counters)
        if analysis_started:row.update(self.analyzer.ocr_activity)
        if not row.get('ocr_verified') and row['clock_state'] not in ('WAITING_NEW_STREAM_THUMBNAIL','WAITING_POLL','WAITING_RETRY'):
            # Include exceptions from observe/decode: a 304 cannot repair a
            # missing or unverified cached frame.
            self.http.etag=None
        row['stream_clock_observed']=bool(row.get('active_stream_id') and self.analyzer.verified.get('stream_id')==row['active_stream_id'])
        row.update(checked_at_utc=utc_timestamp(time.time()),cycle=self.cycle,platform='twitch_thumbnail',
                   duration_seconds=round(time.monotonic()-started,3),normal_image_storage='memory')
        # Keep a numeric anchor across transient source/capture/OCR failures.
        row['last_verified']={k:v for k,v in self.analyzer.verified.items() if k!='last_verified'}
        try:
            abnormal=row.get('clock_state')=='OCR_UNCERTAIN' or row.get('freshness_state','').startswith('STALE')
            was_abnormal=self.previous.get('clock_state')=='OCR_UNCERTAIN' or self.previous.get('freshness_state','').startswith('STALE')
            if abnormal and not was_abnormal and self.analyzer.rois:row['evidence_id']=self.store.save(row,self.analyzer.rois,now=time.time())
            elif abnormal:row['evidence_id']=self.previous.get('evidence_id')
            else:self.store.recovered('twitch_thumbnail')
            self.store.prune(now=time.time())
        except (OSError,ValueError):row['evidence_status']='storage_unavailable'
        publish(self.state_file,row);append_history(self.root,row,max_bytes=1024*1024)
        self.previous=row
        return row


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config',type=Path,required=True);parser.add_argument('--state-root',type=Path,required=True);parser.add_argument('--once',action='store_true')
    args=parser.parse_args();config=load_json(args.config)
    config['boot_id']=Path('/proc/sys/kernel/random/boot_id').read_text().strip()
    args.state_root.mkdir(parents=True,exist_ok=True);lock=(args.state_root/'.worker.lock').open('a')
    fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    worker=Worker(config,args.state_root);stop=threading.Event()
    def stopping(*_):stop.set();worker.runner.stopping=True
    signal.signal(signal.SIGTERM,stopping);signal.signal(signal.SIGINT,stopping)
    while not stop.is_set():
        before=time.monotonic();last=epoch(worker.previous.get('last_request_at_utc'))
        if last is not None and time.time()-last<60 and not args.once:stop.wait(min(60,max(0,last+60-time.time())))
        if stop.is_set():break
        try:
            row=worker.once()
            print(json.dumps({k:row.get(k) for k in ('checked_at_utc','cycle','capture_ok','clock_state','reason','freshness_state','frame_age_seconds','image_duplicate','unique_sample','duration_seconds')}),flush=True)
        except OSError:print(json.dumps({'event':'thumbnail_publication_failed'}),flush=True)
        if args.once:break
        stop.wait(max(1,60-(time.monotonic()-before)))


if __name__=='__main__':main()
