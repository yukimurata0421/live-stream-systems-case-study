import base64
import hashlib
import hmac
import importlib.util
import json
from pathlib import Path
import tempfile
import time
from urllib.request import urlopen

import pytest

from stream_v3.oci_monitor_probe import MonitorWorker, WORKER_REVISION, assert_tmpfs
from stream_v3.oci_monitor_metrics import write_metrics
from stream_v3.viewer_clock import Capture, GrayImage, utc_timestamp
from stream_v3.viewer_clock_capture import CommandResult, ProbeError
from stream_v3.viewer_clock_worker import EvidenceStore


def load_pull():
    spec=importlib.util.spec_from_file_location('oci_monitor_pull',Path(__file__).parents[1]/'ops/scripts/stream_v3_oci_monitor_pull.py')
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize('standalone', [False, True])
def test_exporter_overlay_preserves_baseline_in_checkout_and_standalone_layout(tmp_path, monkeypatch, standalone):
    root = Path(__file__).parents[1]
    script = root / 'ops/scripts/stream_v3_oci_monitor_exporter_overlay.py'
    if standalone:
        script = tmp_path / script.name
        script.write_bytes((root / 'ops/scripts' / script.name).read_bytes())
        (tmp_path / 'oci_monitor_metrics.py').write_bytes((root / 'src/stream_v3/oci_monitor_metrics.py').read_bytes())
    baseline = tmp_path / 'baseline.py'
    baseline.write_text(r"""class MetricWriter:
    def __init__(self): self.values = {}; self.lines=[];self.seen=set()
    def metric(self, name, value, **kwargs): self.values[name] = value; self.lines.append(f'{name} {value}')
    def render(self): return '\n'.join(self.lines)+'\n'
class MetricsCache:
    def get(self): return 'legacy_metric 7\n',''
def main(): return MetricsCache().get()[0]
""")
    state = tmp_path / 'observation.json'
    state.write_text('{}')
    monkeypatch.setenv('OCI_MONITOR_EXPORTER_BASELINE', str(baseline))
    monkeypatch.setenv('OCI_MONITOR_EXPORTER_BASELINE_SHA256', hashlib.sha256(baseline.read_bytes()).hexdigest())
    monkeypatch.setenv('OCI_MONITOR_OBSERVATION_FILE', str(state))
    spec = importlib.util.spec_from_file_location('oci_monitor_overlay', script)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    metrics = module.main()
    assert 'legacy_metric 7' in metrics
    assert 'stream_v3_oci_monitor_capture_ok 0' in metrics
    assert 'stream_v3_youtube_fallback_monitor_expected_active -1' in metrics


def test_only_ram_accepts_normal_snapshots():
    with pytest.raises(ValueError,match='require_tmpfs'):assert_tmpfs(Path('/'))


def test_periodic_native_clock_evidence_is_rate_limited_without_extra_ocr(monkeypatch):
    from datetime import datetime,timezone
    from stream_v3 import oci_monitor_probe as module
    from stream_v3.viewer_clock import ClockReading
    clock=[1000.];reads=[]
    monkeypatch.setattr(module.time,'monotonic',lambda:clock[0])
    config={'source_identity':'fixture','roi':[0,0,.5,1],'utc_roi':[.5,0,.5,1]}
    def capture(self,deadline):
        return Capture(GrayImage(40,20,b'\x80'*800),'fixture',86400+clock[0]-1000,clock[0],'h264'),b'jpeg',{}
    monkeypatch.setattr(MonitorWorker,'capture_relay',capture)
    def read(*args,**kwargs):
        second=int(clock[0]-1000)+(32400 if len(reads)%2==0 else 0)
        reads.append(second)
        return ClockReading(datetime.fromtimestamp(second,timezone.utc).strftime('%H:%M:%S'),97)
    monkeypatch.setattr(module.TesseractOCR,'read',read)
    with tempfile.TemporaryDirectory(dir='/dev/shm') as directory:
        worker=MonitorWorker(config,Path(directory))
        for stamp,expected in ((1000,0),(1010,1),(1020,1),(1070,2)):
            clock[0]=stamp;worker.run_cycle()
            assert len(list((Path(directory)/'evidence').glob('*.zip')))==expected
        assert len(reads)==8 and worker.current['targets']['oci_relay']['clock_state']=='ADVANCING_CONFIRMED'


def test_anomaly_evidence_keeps_only_bounded_jpegs(tmp_path):
    import zipfile
    store=EvidenceStore(tmp_path/'evidence',max_bytes=16384,platforms=('oci_relay',))
    row={'platform':'oci_relay','clock_state':'STALLED_CONFIRMED'}
    name=store.save_jpeg_pair(row,b'\xff\xd8previous',b'\xff\xd8current',now=time.time())
    with zipfile.ZipFile(store.root/name) as archive:
        assert set(archive.namelist())=={'observation.json','current.jpg','previous.jpg'}
        assert json.loads(archive.read('observation.json'))['ocr_uses_native_resolution'] is True
    with pytest.raises(ValueError):store.save_jpeg_pair(row,None,b'invalid',now=time.time())


def test_ram_snapshot_heartbeat_signed_pull_and_failure(monkeypatch,tmp_path):
    from stream_v3 import oci_monitor_probe as module
    if not Path('/dev/shm').is_dir():pytest.skip('tmpfs unavailable')
    key=tmp_path/'observation.key';key.write_bytes(b'k'*32)
    config={'dell_boot_id':'fixture-boot','key_file':str(key),'source_identity':'fixture-source','roi':[0,0,.5,1],'utc_roi':[.5,0,.5,1]}
    def capture(self,deadline):
        return Capture(GrayImage(200,80,bytes([128])*16000),'fixture-source',time.time(),time.monotonic(),'h264'),b'jpeg',{}
    monkeypatch.setattr(MonitorWorker,'capture_relay',capture)
    from stream_v3.viewer_clock import ClockReading
    monkeypatch.setattr(module.TesseractOCR,'read',lambda *_a,**_k:ClockReading('09:00:00',99))
    with tempfile.TemporaryDirectory(dir='/dev/shm') as producer, tempfile.TemporaryDirectory(dir='/dev/shm') as consumer:
        worker=MonitorWorker(config,Path(producer))
        assert worker.run_cycle()['targets']['oci_relay']['clock_state']=='OCR_UNCERTAIN'
        server=worker.serve('127.0.0.1',0)
        try:
            pull=load_pull()
            corrupt_native=[]
            def transport(*_args,**_kwargs):
                with urlopen(f'http://127.0.0.1:{server.server_port}/bundle.json') as response:
                    content=response.read();signature=response.headers['X-Stream-Observation-Signature']
                    if corrupt_native:
                        value=json.loads(content);value['clock_evidence']['oci_relay']=base64.b64encode(b'bad zip').decode()
                        content=json.dumps(value).encode();signature=hmac.new(key.read_bytes(),b'oci-monitor-observation-v1\0'+content,hashlib.sha256).hexdigest()
                    envelope={'signature':signature,'payload':base64.b64encode(content).decode(),'ssh_boot_id':Path('/proc/sys/kernel/random/boot_id').read_text().strip(),'observer_runtime':{'available':True,'deployment_uid':'observer','pods':[{'uid':'local'}]}}
                return CommandResult(0,json.dumps(envelope).encode(),b'')
            monkeypatch.setattr(pull.ProcessRunner,'run',transport)
            cfg={**config,'ssh_host':'fixture','worker_revision':WORKER_REVISION,'observer_deployment_uid':'observer'}
            state=tmp_path/'oci_monitor_observation.json'
            assert pull.collect(cfg,Path(consumer),state)
            assert json.loads(state.read_text())['state']['targets']['oci_relay']['capture_ok']
            import zipfile
            native=next((state.parent/'native_clock_evidence').glob('*.zip'))
            with zipfile.ZipFile(native) as archive:
                assert 'utc.pgm' in archive.namelist() and 'utc_context.pgm' in archive.namelist()
                metadata=json.loads(archive.read('observation.json'))
                assert set(metadata['ocr_pair_stages'])=={'original','contrast'}
                assert metadata['sample_id']==worker.current['targets']['oci_relay']['sample_id']
            history=state.parent/'viewer_clock_history.jsonl'
            first_history=history.read_bytes()
            assert 'clock_evidence' not in json.loads(state.read_text())
            assert pull.collect(cfg,Path(consumer),state)
            assert history.read_bytes()==first_history
            assert json.loads(state.read_text())['collector']['duplicates_total']==1
            assert len(list((state.parent/'native_clock_evidence').glob('*.zip')))==1
            cfg['observer_image']='different-baseline-image'
            assert pull.collect(cfg,Path(consumer),state)
            contract=json.loads(state.read_text())['observer_contract']
            assert contract['configuration_changed'] and contract['runtime_attested'] and contract['compatible']
            cfg.pop('observer_image')
            corrupt_native.append(True);worker.run_cycle()
            assert pull.collect(cfg,Path(consumer),state)
            observed=json.loads(state.read_text())['state']['targets']['oci_relay']
            assert observed['capture_ok'] and observed['native_evidence_status']=='storage_or_evidence_error'
            corrupt_native.clear()
            for _ in range(4):
                worker.run_cycle();assert pull.collect(cfg,Path(consumer),state)
            assert len([p for p in Path(producer).glob('capture/oci_monitor/*.jpg') if not p.is_symlink()])==2
            assert len([p for p in Path(consumer).glob('capture/oci_monitor/*.jpg') if not p.is_symlink()])==2
            cfg['source_identity']='different-source'
            assert not pull.collect(cfg,Path(consumer),state)
            row=json.loads(state.read_text())['state']['targets']['oci_relay']
            assert row['clock_state']=='UNKNOWN' and not row['capture_ok']
            assert 'local_image_path' not in row
            monkeypatch.setattr(MonitorWorker,'capture_relay',lambda *_args,**_kwargs:(_ for _ in ()).throw(ProbeError('capture_failed')))
            assert worker.run_cycle()['targets']['oci_relay']['clock_state']=='UNKNOWN'
            with urlopen(f'http://127.0.0.1:{server.server_port}/heartbeat.json') as response:
                assert json.load(response)['worker_ok']
        finally:server.shutdown();server.server_close()


@pytest.mark.parametrize('age_seconds,capture_expected',[(0,1),(29,1),(31,0),(-10,0)])
def test_metrics_freshness_independent_from_heartbeat(age_seconds,capture_expected):
    now=time.time();values={}
    class Writer:
        def metric(self,name,value,**kwargs):values[name]=value
    observation={'schema':'stream_v3.oci_monitor_bundle.v1','state':{'targets':{'oci_relay':{
        'checked_at_utc':utc_timestamp(now-age_seconds),'captured_at_utc':utc_timestamp(now-age_seconds),
        'capture_ok':True,'clock_state':'ADVANCING_CONFIRMED','ocr_verified':True,'clock_pair_consistent':True,'source_binding_ok':True}}},'observer_contract':{'compatible':True,'runtime_attested':True},
        'heartbeat':{'checked_at_utc':utc_timestamp(now),'worker_ok':True}}
    write_metrics(Writer(),observation,now=now)
    assert values['stream_v3_oci_monitor_capture_ok']==capture_expected
    assert values['stream_v3_oci_monitor_clock_advancing']==capture_expected
    assert values['stream_v3_oci_monitor_observer_heartbeat_ok']==1


def test_fallback_transport_bound_freshness_and_producer_change():
    from stream_v3.fallback_monitor import TransportBuffer
    ring=TransportBuffer(max_bytes=188*2,seconds=6)
    packet=b'\x47'+b'x'*187
    for i in range(10):ring.feed(packet,'producer-one',now=float(i))
    data,metadata=ring.snapshot(now=9)
    assert len(data)==188*2 and metadata['buffer_bytes']==188*2
    assert ring.snapshot(now=13) is None
    ring.feed(packet,'producer-two',now=14)
    data,metadata=ring.snapshot(now=14)
    assert data==packet and metadata['transport_peer_ip']=='producer-two'
    ring.feed(b'bad packet','producer-three',now=15)
    assert ring.snapshot(now=15)[1]['transport_peer_ip']=='producer-two'


def test_fallback_tap_preserves_primary_options_and_fails_open():
    from stream_v3.fallback_monitor import add_monitor_output, install, render_launcher
    args=['ffmpeg','-i',':display','-i','pulse','-filter_complex','scale=1920:1080,setsar=1',
          '-c:v','libx264','-c:a','aac','-rw_timeout','15000000','-threads:v','1',
          '-flvflags','aac_seq_header_detect','-f','flv','rtmps://b.rtmps.youtube.com/live2/fixture']
    destination='udp://127.0.0.1:8767?pkt_size=1316&connect=0&buffer_size=65536'
    out=add_monitor_output(args,destination)
    assert out[-3:-1]==['-f','tee']
    assert 'use_fifo=0:onfail=abort:rw_timeout=15000000:flvflags=aac_seq_header_detect' in out[-1]
    assert '[f=mpegts:use_fifo=1:onfail=ignore]' in out[-1]
    assert out[out.index('-filter_complex')+1]=='[0:v:0]scale=1920:1080,setsar=1[monitor_video]'
    assert out[out.index('-map')+1]=='[monitor_video]'
    class Engine:
        def ffmpeg_args(self):return ['ffmpeg','-f','null','-']
    install(Engine,destination)
    assert Engine().ffmpeg_args()==['ffmpeg','-f','null','-']
    original="se.StreamEngine.ffmpeg_args=args\nsys.argv=['stream_engine']\nraise SystemExit(se.main())\n"
    rendered=render_launcher(original,destination)
    assert rendered.endswith("sys.argv=['stream_engine']\nraise SystemExit(se.main())\n")
    with pytest.raises(ValueError):render_launcher(rendered,destination)


def test_fallback_standby_binding_and_unknown_are_independent_from_oci():
    pull=load_pull()
    config={'fallback_template_revision':'template','fallback_deployment_uid':'deployment','fallback_tap_revision':'tap'}
    status={'available':True,'replicas':0,'template_revision':'template','deployment_uid':'deployment','tap_revision':'tap','pods':[]}
    row={'platform':'youtube_fallback','checked_at_utc':utc_timestamp(time.time()),'capture_ok':True,
         'clock_state':'ADVANCING_CONFIRMED','transport_peer_ip':'producer'}
    standby=pull.bind_fallback(row,status,config)
    assert standby['expected_active'] is False and not standby['capture_ok']
    assert standby['reason']=='fallback_inactive'
    status.update(replicas=1,pods=[{'uid':'pod','ip':'producer','ready':True,'tap_revision':'tap'}])
    active=pull.bind_fallback(row,status,config)
    assert active['capture_ok'] and active['producer_pod_uid']=='pod'
    assert active['expected_active'] is True
    mismatch=pull.bind_fallback({**row,'transport_peer_ip':'other'},status,config)
    assert not mismatch['capture_ok'] and mismatch['clock_state']=='UNKNOWN'
    changed=pull.bind_fallback(row,{**status,'template_revision':'changed'},config)
    assert changed['capture_ok'] and changed['configuration_changed']
    status['pods'].append({'uid':'duplicate','ip':'other','ready':True,'tap_revision':'tap'})
    assert not pull.bind_fallback(row,status,config)['capture_ok']
    assert pull.bind_fallback(row,{},config)['expected_active'] is None


def test_fallback_metrics_show_expected_sender_even_when_capture_fails():
    now=time.time();values={}
    class Writer:
        def metric(self,name,value,**kwargs):values[name]=value
    observation={'schema':'stream_v3.oci_monitor_bundle.v1','state':{'targets':{
        'oci_relay':{'checked_at_utc':utc_timestamp(now),'capture_ok':False,'clock_state':'UNKNOWN'},
        'youtube_fallback':{'checked_at_utc':utc_timestamp(now),'expected_active':True,'capture_ok':False,'clock_state':'UNKNOWN'}}}}
    write_metrics(Writer(),observation,now=now)
    assert values['stream_v3_oci_monitor_capture_ok']==0
    assert values['stream_v3_youtube_fallback_monitor_expected_active']==1
    assert values['stream_v3_youtube_fallback_monitor_capture_ok']==0
    observation['state']['targets']['youtube_fallback']['expected_active']=False
    write_metrics(Writer(),observation,now=now)
    assert values['stream_v3_youtube_fallback_monitor_expected_active']==0


def test_fallback_ram_publication_when_oci_fails_and_standby_removes_alias(monkeypatch,tmp_path):
    from stream_v3 import oci_monitor_probe as module
    from stream_v3.fallback_monitor import TransportBuffer
    monkeypatch.setattr(TransportBuffer,'start',lambda *_a,**_k:None)
    monkeypatch.setattr(MonitorWorker,'capture_relay',lambda *_a,**_k:(_ for _ in ()).throw(ProbeError('oci_unavailable')))
    def fallback(self,deadline):
        return Capture(GrayImage(200,100,bytes([128])*20000),'fallback',time.time(),time.monotonic(),'h264'),b'jpeg',{'transport_peer_ip':'producer'}
    monkeypatch.setattr(MonitorWorker,'capture_fallback',fallback)
    key=tmp_path/'key';key.write_bytes(b'k'*32)
    config={'key_file':str(key),'source_identity':'source','fallback_udp_port':8767,'roi':[0,0,.5,1],'utc_roi':[.5,0,.5,1],
            'fallback_template_revision':'template','fallback_deployment_uid':'deployment','fallback_tap_revision':'tap'}
    status={'available':True,'replicas':1,'template_revision':'template','deployment_uid':'deployment','tap_revision':'tap',
            'pods':[{'uid':'pod','ip':'producer','ready':True,'tap_revision':'tap'}]}
    with tempfile.TemporaryDirectory(dir='/dev/shm') as producer,tempfile.TemporaryDirectory(dir='/dev/shm') as consumer:
        worker=MonitorWorker(config,Path(producer));worker.run_cycle();server=worker.serve('127.0.0.1',0)
        try:
            pull=load_pull()
            def transport(*_a,**_k):
                with urlopen(f'http://127.0.0.1:{server.server_port}/bundle.json') as response:
                    envelope={'signature':response.headers['X-Stream-Observation-Signature'],'payload':base64.b64encode(response.read()).decode(),
                              'ssh_boot_id':Path('/proc/sys/kernel/random/boot_id').read_text().strip(),'fallback_status':status,'observer_runtime':{'available':True,'deployment_uid':'observer','pods':[{'uid':'local'}]}}
                return CommandResult(0,json.dumps(envelope).encode(),b'')
            monkeypatch.setattr(pull.ProcessRunner,'run',transport)
            cfg={**config,'ssh_host':'fixture','worker_revision':WORKER_REVISION,'observer_deployment_uid':'observer'};state=tmp_path/'observation.json'
            assert pull.collect(cfg,Path(consumer),state)
            targets=json.loads(state.read_text())['state']['targets']
            assert targets['oci_relay']['clock_state']=='UNKNOWN' and targets['youtube_fallback']['capture_ok']
            latest=Path(consumer)/'capture/youtube_fallback/latest.jpg'
            assert latest.read_bytes()==b'jpeg'
            status['replicas']=0
            assert pull.collect(cfg,Path(consumer),state)
            assert not latest.exists()
            assert json.loads(state.read_text())['state']['targets']['youtube_fallback']['expected_active'] is False
        finally:server.shutdown();server.server_close();worker.fallback.close()


def test_probable_is_diagnostic_not_clock_progression():
    now=time.time();values={}
    class Writer:
        def metric(self,name,value,**kwargs):values[name]=value
    row={'checked_at_utc':utc_timestamp(now),'captured_at_utc':utc_timestamp(now),'capture_ok':True,
         'source_binding_ok':True,'clock_state':'ADVANCING_PROBABLE','ocr_verified':False}
    observation={'schema':'stream_v3.oci_monitor_bundle.v1','observer_contract':{'compatible':True,'runtime_attested':True},'state':{'targets':{'oci_relay':row}}}
    write_metrics(Writer(),observation,now=now)
    assert values['stream_v3_oci_monitor_capture_ok']==1
    assert values['stream_v3_oci_monitor_clock_advancing']==0


def test_storage_failure_does_not_reset_worker_or_clock(monkeypatch,tmp_path):
    import stream_v3.oci_monitor_probe as module
    from stream_v3.viewer_clock import ClockReading
    key=tmp_path/'key';key.write_bytes(b'k'*32)
    with tempfile.TemporaryDirectory(dir='/dev/shm') as root:
        worker=MonitorWorker({'key_file':str(key),'source_identity':'s','roi':[0,0,.5,1],'utc_roi':[.5,0,.5,1]},Path(root))
        def capture(self,deadline):return Capture(GrayImage(40,20,b'\x80'*800),'s',time.time(),time.monotonic(),'h264'),b'jpeg',{}
        monkeypatch.setattr(MonitorWorker,'capture_relay',capture)
        monkeypatch.setattr(module.TesseractOCR,'read',lambda *_a,**_k:ClockReading('00:00:00',99))
        monkeypatch.setattr(module,'atomic_bytes',lambda *_a,**_k:(_ for _ in ()).throw(OSError('fixture-full')))
        first=worker.run_cycle();second=worker.run_cycle()
        assert first['cycle']==1 and second['cycle']==2 and worker.heartbeat()['completed_cycle']==2
        assert second['targets']['oci_relay']['reason']=='snapshot_io_error'
        assert second['worker']['state_write_error']


def test_long_poll_does_not_block_heartbeat(tmp_path):
    import threading
    key=tmp_path/'key';key.write_bytes(b'k'*32)
    with tempfile.TemporaryDirectory(dir='/dev/shm') as root:
        worker=MonitorWorker({'source_identity':'fixture','key_file':str(key)},Path(root))
        worker.current={'schema':'stream_v3.oci_monitor_probe.v2','cycle':1,'targets':{}}
        server=worker.serve('127.0.0.1',0);results=[]
        def poll():
            with urlopen(f'http://127.0.0.1:{server.server_port}/bundle.json?after_cycle=1&pod_uid=local&wait=2',timeout=3) as response:results.append(json.load(response))
        thread=threading.Thread(target=poll);thread.start();time.sleep(.1)
        try:
            before=time.monotonic()
            with urlopen(f'http://127.0.0.1:{server.server_port}/heartbeat.json',timeout=1) as response:assert json.load(response)['worker_ok']
            assert time.monotonic()-before<1
            with worker.changed:worker.current={**worker.current,'cycle':2};worker.changed.notify_all()
            thread.join(timeout=2);assert results[0]['state']['cycle']==2
        finally:server.shutdown();server.server_close()
