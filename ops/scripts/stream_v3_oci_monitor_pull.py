#!/usr/bin/env python3
"""Import delivery-host RAM snapshots and heartbeat through fixed read-only GET requests."""
import argparse
import base64
import hashlib
import hmac
import json
import os
from pathlib import Path
import shlex
import time

from stream_v3.oci_monitor_probe import assert_tmpfs
from stream_v3.oci_monitor_metrics import age
from stream_v3.viewer_clock import utc_timestamp
from stream_v3.viewer_clock_capture import ProcessRunner
from stream_v3.viewer_clock_worker import EvidenceStore,append_history,atomic_bytes,json_bytes
from stream_v3.monitor_facts import native_facts,classify
from stream_v3.verified_clock_evidence import ClockEvidenceStore,MAX_BUNDLE_BYTES

REMOTE_READ="""import base64,json,urllib.request,urllib.parse,subprocess,hashlib,re,copy,time
from pathlib import Path
opener=urllib.request.build_opener(urllib.request.ProxyHandler({}))
query=urllib.parse.urlencode({'after_cycle':__AFTER_CYCLE__,'pod_uid':__POD_UID__,'wait':10})
with opener.open('http://127.0.0.1:8766/bundle.json?'+query,timeout=12) as response:
 payload=response.read(4*1024*1024+1);signature=response.headers.get('X-Stream-Observation-Signature','')
 if len(payload)>4*1024*1024:raise ValueError('bundle_too_large')
def read(*args):
 r=subprocess.run(['sudo','-n','k3s','kubectl','-n','stream-v3',*args,'-o','json'],capture_output=True,timeout=2,check=True)
 if len(r.stdout)>512*1024:raise ValueError('status_too_large')
 return json.loads(r.stdout)
boot=Path('/proc/sys/kernel/random/boot_id').read_text().strip()
source=json.loads(payload)['state'];peer=source.get('targets',{}).get('youtube_fallback',{}).get('transport_peer_ip')
cache=__RUNTIME_CACHE__
cached_runtime=cache.get('observer_runtime',{})
ready=(cached_runtime.get('available') is True and any(p.get('uid')==source.get('pod_uid') for p in cached_runtime.get('pods',[])))
reuse=(0<=time.time()-cache.get('checked_epoch',0)<(60 if ready else 10) and cache.get('ssh_boot_id')==boot
       and cache.get('pod_uid')==source.get('pod_uid') and cache.get('fallback_peer_ip')==peer)
runtime=cache.get('observer_runtime',{}) if reuse else {'available':False}
fallback=cache.get('fallback_status',{}) if reuse else {'available':False}
checked=cache.get('checked_epoch') if reuse else time.time()
try:
 if reuse:raise StopIteration
 deployments=read('get','deploy','stream-v3-youtube-backup','stream-v3-oci-monitor-probe')['items']
 d=next(x for x in deployments if x['metadata']['name']=='stream-v3-youtube-backup')
 observer=next(x for x in deployments if x['metadata']['name']=='stream-v3-oci-monitor-probe')
 pods=read('get','pods')['items']
 desired=observer['spec']['template']['spec']['containers'][0]['image']
 if not re.fullmatch(r'.+@sha256:[a-f0-9]{64}',desired):raise ValueError('observer_image_not_pinned')
 observed=[]
 for p in pods:
  if p['metadata'].get('deletionTimestamp') or p['status'].get('phase')!='Running':continue
  if p['metadata'].get('labels',{}).get('app')!='stream-v3-oci-monitor-probe':continue
  for c in p['status'].get('containerStatuses',[]):
   if c['name']=='observer' and c.get('ready') and c.get('imageID','').split('@')[-1]==desired.split('@')[-1]:
    observed.append({'uid':p['metadata']['uid'],'image_id':c['imageID']})
 runtime={'available':True,'deployment_uid':observer['metadata']['uid'],'desired_image':desired,'pods':observed}
 template=d['spec']['template']
 volume=next(v for v in template['spec']['volumes'] if v['name']=='fallback-monitor-tap')
 name=volume['configMap']['name']
 if not re.fullmatch('stream-v3-fallback-monitor-tap-[a-f0-9]{12}',name):raise ValueError('unexpected_tap_config')
 cm=read('get','configmap',name)
 if not cm.get('immutable'):raise ValueError('mutable_tap_config')
 replicasets=read('get','replicasets','-l','app.kubernetes.io/name=stream-v3-youtube-backup')['items']
 owners=set()
 for rs in replicasets:
  t=copy.deepcopy(rs['spec']['template']);t['metadata'].get('labels',{}).pop('pod-template-hash',None)
  if t==template and any(o.get('kind')=='Deployment' and o.get('uid')==d['metadata']['uid'] for o in rs['metadata'].get('ownerReferences',[])):
   owners.add(rs['metadata']['uid'])
 producer=[]
 for p in pods:
  if p['metadata'].get('labels',{}).get('app.kubernetes.io/name')!='stream-v3-youtube-backup':continue
  if p['metadata'].get('deletionTimestamp') or p['status'].get('phase')!='Running':continue
  if not any(o.get('kind')=='ReplicaSet' and o.get('uid') in owners for o in p['metadata'].get('ownerReferences',[])):continue
  container=next(c for c in p['spec']['containers'] if c['name']=='stream-engine')
  status=next((c for c in p['status'].get('containerStatuses',[]) if c['name']=='stream-engine'),{})
  producer.append({'uid':p['metadata']['uid'],'ip':p['status'].get('podIP'),'ready':bool(status.get('ready')),
                   'tap_revision':next((v.get('value') for v in container['env'] if v['name']=='FALLBACK_MONITOR_TAP_REVISION'),None)})
 fallback={'available':True,'replicas':d['spec']['replicas'],'deployment_uid':d['metadata']['uid'],
           'template_revision':hashlib.sha256(json.dumps(template,sort_keys=True,separators=(',',':')).encode()).hexdigest(),
           'tap_revision':hashlib.sha256(cm['data']['fallback_monitor.py'].encode()).hexdigest(),'pods':producer}
except Exception:pass
cache={'checked_epoch':checked,'ssh_boot_id':boot,'pod_uid':source.get('pod_uid'),'fallback_peer_ip':peer,
       'observer_runtime':runtime,'fallback_status':fallback}
print(json.dumps({'signature':signature,'payload':base64.b64encode(payload).decode(),'ssh_boot_id':boot,
                  'fallback_status':fallback,'observer_runtime':runtime,'runtime_cache':cache,'runtime_cache_reused':reuse}))
"""


def bind_fallback(row,status,config):
    def unknown(reason,**fields):
        return {'platform':'youtube_fallback','checked_at_utc':row.get('checked_at_utc',utc_timestamp(time.time())),
                'clock_state':'UNKNOWN','capture_ok':False,'ocr_verified':False,'reason':reason,
                'observation_scope':'dell_youtube_fallback_encoded_output',**fields}
    if not status.get('available') or not isinstance(status.get('replicas'),int):
        return unknown('fallback_status_unavailable',expected_active=None)
    active=status['replicas']>0
    changed=status.get('template_revision')!=config.get('fallback_template_revision')
    if (status.get('deployment_uid')!=config['fallback_deployment_uid']
            or status.get('tap_revision')!=config['fallback_tap_revision']):
        return unknown('fallback_producer_identity_changed',expected_active=active,configuration_changed=True,source_binding_ok=False)
    if not active:return unknown('fallback_inactive',expected_active=False,producer_ready=False,source_binding_ok=True,configuration_changed=changed)
    pods=[p for p in status.get('pods',[]) if p.get('ready') and p.get('tap_revision')==config['fallback_tap_revision']]
    if len(pods)!=1:return unknown('fallback_producer_not_ready_or_ambiguous',expected_active=True,producer_ready=False)
    if row.get('capture_ok') and row.get('transport_peer_ip')!=pods[0].get('ip'):
        return unknown('fallback_transport_producer_mismatch',expected_active=True,producer_ready=True)
    return {**row,'expected_active':True,'producer_ready':True,'source_binding_ok':True,'producer_pod_uid':pods[0]['uid'],'configuration_changed':changed}


def collect(config,ram_root,state_file):
    stage='ssh_transport'
    expected={'oci_relay','youtube_fallback'} if config.get('fallback_template_revision') else {'oci_relay'}
    directories={'oci_relay':'oci_monitor','youtube_fallback':'youtube_fallback'}
    try:old=json.loads(state_file.read_text()) if state_file.exists() else {}
    except (OSError,ValueError):old={}
    try:
        previous_state=old.get('state',{})
        if not previous_state.get('pod_uid'):
            last=old.get('collector',{}).get('last_cycle_identity',[])
            if len(last)==3:previous_state={'source_boot_id':last[0],'pod_uid':last[1],'cycle':last[2]}
        remote=REMOTE_READ.replace('__AFTER_CYCLE__',repr(int(previous_state.get('cycle',-1)))).replace('__POD_UID__',repr(str(previous_state.get('pod_uid','')))).replace('__RUNTIME_CACHE__',repr(old.get('runtime_cache',{})))
        result=ProcessRunner().run(['ssh','-o','BatchMode=yes','-o','StrictHostKeyChecking=yes','-o','ConnectTimeout=5','-o','ControlMaster=no','-o','ControlPath=none','-o','ControlPersist=no',config['ssh_host'],shlex.join(['python3','-c',remote])],deadline=time.monotonic()+22,max_stdout=6*1024*1024)
        if result.returncode:raise ValueError('dell_observer_unavailable')
        stage='bundle_signature'
        envelope=json.loads(result.stdout);raw=base64.b64decode(envelope['payload'],validate=True)
        key=Path(config['key_file']).read_bytes()
        if len(key)!=32 or not hmac.compare_digest(envelope['signature'],hmac.new(key,b'oci-monitor-observation-v1\0'+raw,hashlib.sha256).hexdigest()):raise ValueError('observation_signature_invalid')
        bundle=json.loads(raw);state=bundle['state'];heartbeat=bundle['heartbeat']
        bundle['runtime_cache']=envelope.get('runtime_cache',{})
        bundle['runtime_cache_reused']=envelope.get('runtime_cache_reused') is True
        stage='observation_binding'
        if bundle['schema']!='stream_v3.oci_monitor_bundle.v1' or state['schema'] not in ('stream_v3.oci_monitor_probe.v1','stream_v3.oci_monitor_probe.v2'):raise ValueError('observation_schema_invalid')
        if state.get('source_identity')!=config['source_identity']:raise ValueError('observation_source_invalid')
        for row in (state,heartbeat):
            if row['source_host_id']!='delivery-host' or row['source_boot_id']!=envelope.get('ssh_boot_id'):raise ValueError('observation_identity_invalid')
        if age(heartbeat['checked_at_utc'],time.time()) is None or age(heartbeat['checked_at_utc'],time.time())>30:raise ValueError('observation_stale')
        runtime=envelope.get('observer_runtime',{})
        attested=(runtime.get('available') is True and runtime.get('deployment_uid')==config.get('observer_deployment_uid')
                  and any(p.get('uid')==state.get('pod_uid') for p in runtime.get('pods',[])))
        # A known v1 source remains readable during migration, without v2 health semantics.
        legacy=(state['schema']=='stream_v3.oci_monitor_probe.v1' and state.get('worker_revision')==config.get('worker_revision'))
        compatible=state.get('contract_version')==2 and all(state.get('capabilities',{}).get(x) is True for x in ('continuous_capture','ocr_every_sample','clock_pair_verification'))
        bundle['observer_contract']={'runtime_attested':attested or legacy,'compatible':compatible,
                                     'configuration_changed':(state.get('worker_revision')!=config.get('worker_revision')
                                                              or bool(config.get('observer_image') and runtime.get('desired_image')!=config['observer_image'])),
                                     'observed_worker_revision':state.get('worker_revision'),'observed_image':runtime.get('desired_image')}
        assert_tmpfs(ram_root)
        stage='ram_publication'
        images=bundle.pop('images')
        native_evidence=bundle.pop('clock_evidence',{})
        if not isinstance(native_evidence,dict):native_evidence={}
        if set(state['targets'])!=expected:raise ValueError('unexpected_monitor_targets')
        if 'youtube_fallback' in expected:
            state['targets']['youtube_fallback']=bind_fallback(state['targets']['youtube_fallback'],envelope.get('fallback_status',{}),config)
        identity=[state.get('source_boot_id'),state.get('pod_uid'),state.get('cycle')]
        previous_identity=[previous_state.get('source_boot_id'),previous_state.get('pod_uid'),previous_state.get('cycle')]
        if identity[:2]==previous_identity[:2] and isinstance(identity[2],int) and isinstance(previous_identity[2],int) and identity[2]<previous_identity[2]:
            raise ValueError('observation_cycle_regressed')
        duplicate=identity==previous_identity
        prior_collector=old.get('collector',{})
        bundle['collector']={'ingested_at_utc':utc_timestamp(time.time()),'duplicate':duplicate,
                             'last_cycle_identity':identity,
                             'unique_observations_total':prior_collector.get('unique_observations_total',0)+int(not duplicate),
                             'duplicates_total':prior_collector.get('duplicates_total',0)+int(duplicate)}
        quality=prior_collector.get('observation_quality',{})
        bundle['collector']['observation_quality']={}
        try:
            native_path=ram_root/'relay-facts.json'
            if native_path.stat().st_size>65536:raise ValueError('native_facts_too_large')
            native=json.loads(native_path.read_text())
        except (OSError,ValueError):native={}
        bundle['native_delivery']=native_facts(native,old.get('native_delivery',{}),now=time.time())
        for target,row in state['targets'].items():
            row['source_binding_ok']=bool(row.get('source_binding_ok',True) and (attested or legacy))
            if not row['source_binding_ok'] and row.get('expected_active') is not False:
                row['source_binding_reason']='runtime_attestation_unavailable'
            state['targets'][target]=classify(row,bundle['native_delivery'],now=time.time(),target=target)
            if not row['source_binding_ok'] and row.get('expected_active') is not False:state['targets'][target]['classification']='observation_unknown'
            now=time.time();prior=quality.get(target,{})
            required=not (row.get('expected_active') is False and attested and compatible and row['source_binding_ok'])
            since=prior.get('required_since_utc') if prior.get('required') is True else None
            since=(since or utc_timestamp(now)) if required else None
            last=prior.get('last_valid_at_utc')
            if not required:last=None
            sample_age=age(row.get('captured_at_utc'),now)
            if (required and attested and compatible and row['source_binding_ok'] and row.get('capture_ok') is True
                    and sample_age is not None and sample_age<=30 and row.get('clock_reading_valid') is True):
                last=row['captured_at_utc']
            bundle['collector']['observation_quality'][target]={'required':required,'required_since_utc':since,'last_valid_at_utc':last}
        store=EvidenceStore(state_file.parent/'evidence',platforms=tuple(sorted(expected)))
        try:
            native_store=ClockEvidenceStore(state_file.parent/'native_clock_evidence',max_bytes=8*1024*1024,
                                            retention_seconds=86400,platforms=tuple(sorted(expected)))
        except OSError:native_store=None
        for target,row in state['targets'].items():
            if not row.get('capture_ok'):continue
            image=base64.b64decode(images[target],validate=True)
            if len(image)>1024*1024 or hashlib.sha256(image).hexdigest()!=row['image_sha256']:raise ValueError('snapshot_hash_or_size_invalid')
            sample_id=row['sample_id']
            if len(sample_id)!=24 or any(c not in '0123456789abcdef' for c in sample_id):raise ValueError('snapshot_id_invalid')
            path=ram_root/'capture'/directories[target]/(sample_id+'.jpg');path.parent.mkdir(parents=True,exist_ok=True)
            if not duplicate or not path.exists():atomic_bytes(path,image)
            row['local_image_path']=str(path)
            # Only abnormal snapshots leave tmpfs. The previous manifest points
            # to an immutable RAM image; no extra normal-frame acquisition.
            try:
                previous_row=old.get('state',{}).get('targets',{}).get(target,{})
                if row['clock_state'] in ('STALLED_SUSPECT','STALLED_CONFIRMED','BLACK_FRAME','OCR_UNCERTAIN'):
                    if previous_row.get('sample_id')==sample_id:
                        row['persistent_evidence_id']=previous_row.get('persistent_evidence_id')
                    else:
                        previous_path=previous_row.get('local_image_path')
                        previous=Path(previous_path).read_bytes() if previous_path and Path(previous_path).is_relative_to(ram_root) and Path(previous_path).exists() else None
                        row['persistent_evidence_id']=store.save_jpeg_pair(row,previous,image,now=time.time())
                    row['evidence_status']='saved' if row.get('persistent_evidence_id') else 'capacity_exhausted'
                else:store.recovered(target)
                store.prune(now=time.time())
            except (ValueError,OSError):row['evidence_status']='storage_or_evidence_error'
            encoded=native_evidence.get(target)
            if encoded is not None:
                try:
                    if native_store is None:raise OSError('native_clock_store_unavailable')
                    if not isinstance(encoded,str) or len(encoded)>4*((MAX_BUNDLE_BYTES+2)//3):raise ValueError('native_clock_bundle_too_large')
                    if duplicate and previous_row.get('native_clock_evidence_id') and (native_store.root/previous_row['native_clock_evidence_id']).is_file():
                        row['native_clock_evidence_id']=previous_row['native_clock_evidence_id']
                    else:
                        row['native_clock_evidence_id']=native_store.import_bundle(base64.b64decode(encoded,validate=True),row,now=time.time())
                    row['native_evidence_status']='saved' if row['native_clock_evidence_id'] else 'capacity_exhausted'
                except (ValueError,OSError):row['native_evidence_status']='storage_or_evidence_error'
            elif native_store is not None and row['clock_state'].startswith('ADVANCING'):native_store.recovered(target)
        if native_store is not None:
            try:native_store.prune(now=time.time())
            except OSError:bundle['collector']['native_evidence_retention_error']=True
        atomic_bytes(state_file,json_bytes(bundle))
        if not duplicate:append_history(state_file.parent,bundle,max_bytes=2*1024*1024)
        for target,row in state['targets'].items():
            latest=ram_root/'capture'/directories[target]/'latest.jpg'
            if row.get('capture_ok'):
                temporary=latest.with_name('.latest.tmp');temporary.unlink(missing_ok=True)
                temporary.symlink_to(row['sample_id']+'.jpg');os.replace(temporary,latest)
            else:latest.unlink(missing_ok=True)
            stored=sorted((p for p in latest.parent.glob('*.jpg') if not p.is_symlink()),key=lambda p:p.stat().st_mtime,reverse=True)
            for path in stored[2:]:path.unlink(missing_ok=True)
        return True
    except Exception:
        for directory in directories.values():
            try:(ram_root/'capture'/directory/'latest.jpg').unlink(missing_ok=True)
            except OSError:pass
        collector=dict(old.get('collector',{}));quality=collector.setdefault('observation_quality',{})
        for target in expected:
            prior=quality.get(target,{})
            # An unavailable status is not proof that the fallback is inactive.
            quality[target]={**prior,'required':True,'required_since_utc':prior.get('required_since_utc') or utc_timestamp(time.time())}
        failure={'schema':'stream_v3.oci_monitor_bundle.v1','state':{'schema':'stream_v3.oci_monitor_probe.v2','checked_at_utc':utc_timestamp(time.time()),'targets':{target:{'platform':target,'clock_state':'UNKNOWN','capture_ok':False,'reason':'dell_observation_unavailable_or_invalid','error_stage':stage,'classification':'observation_unknown'} for target in expected}},'heartbeat':{},'collector':collector,'runtime_cache':old.get('runtime_cache',{})}
        try:
            atomic_bytes(state_file,json_bytes(failure));errors=state_file.parent/'collector_failures';errors.mkdir(mode=0o700,exist_ok=True)
            append_history(errors,failure,max_bytes=1024*1024)
        except OSError:pass
        return False


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config',type=Path,required=True);parser.add_argument('--ram-root',type=Path,required=True);parser.add_argument('--state-file',type=Path,required=True)
    args=parser.parse_args()
    return 0 if collect(json.loads(args.config.read_text()),args.ram_root,args.state_file) else 1


if __name__=='__main__':raise SystemExit(main())
