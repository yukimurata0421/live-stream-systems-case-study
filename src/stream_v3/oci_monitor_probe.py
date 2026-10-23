"""Read the common OCI relay through an independent branch; public-player reception is separate."""
from __future__ import annotations

import argparse
import base64
from datetime import datetime
import fcntl
import hashlib
import hmac
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import io
import json
import os
from pathlib import Path
import re
import signal
import threading
import time
from urllib.parse import urlsplit,parse_qs

from .viewer_clock import AnalysisConfig, Capture, ClockAnalyzer, GrayImage, utc_timestamp
from .viewer_clock_capture import ProbeError, ProcessRunner, TesseractOCR
from .viewer_clock_worker import EvidenceStore, append_history, atomic_bytes, json_bytes
from .frame_receiver import FrameReceiver,ObservedTransportBuffer,relay_args,fallback_args
from .verified_clock import VerifiedClockAnalyzer,VerifiedClockConfig
from .verified_clock_evidence import ClockEvidenceStore,MAX_BUNDLE_BYTES

SCHEMA = "stream_v3.oci_monitor_probe.v2"
TARGET = "oci_relay"
FALLBACK = "youtube_fallback"
WORKER_REVISION = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()


def epoch(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()


def assert_tmpfs(path: Path):
    resolved = str(path.resolve())
    matches = []
    for line in Path("/proc/self/mountinfo").read_text().splitlines():
        left, right = line.split(" - ", 1)
        mount = left.split()[4].replace("\\040", " ")
        if resolved == mount or resolved.startswith(mount.rstrip("/") + "/"):
            matches.append((len(mount), right.split()[0]))
    if not matches or max(matches)[1] != "tmpfs":
        raise ValueError("normal_snapshots_require_tmpfs")


def capture_frame(config, runner, *, deadline):
    from PIL import Image
    before = time.monotonic()
    url = config["source_url"]
    tls = ["-tls_verify", "1", "-ca_file", config["ca_file"]] if url.startswith("rtmps://") else []
    result = runner.run(["ffmpeg", "-hide_banner", "-nostdin", "-loglevel", "info", "-threads", "1", "-filter_threads", "1",
                         "-protocol_whitelist", "rtmp,rtmps,tls,tcp,crypto", "-rw_timeout", "5000000", *tls,
                         "-i", url, "-an", "-sn", "-dn",
                         "-frames:v", "1", "-threads", "1", "-c:v", "ppm", "-f", "image2pipe", "pipe:1"],
                        deadline=deadline, max_stdout=8 * 1024 * 1024)
    if result.returncode:
        raise ProbeError("oci_frame_capture_failed")
    return decoded_capture(result,config["source_identity"],before)


def decoded_capture(result,identity,before,*,received_epoch=None,received_monotonic=None):
    from PIL import Image
    try:
        with Image.open(io.BytesIO(result.stdout)) as image:
            image.load()
            if image.width > 1920 or image.height > 1080:
                raise ProbeError("oci_frame_dimensions_invalid")
            gray = GrayImage(image.width, image.height, image.convert("L").tobytes())
            image.thumbnail((1280,720))
            jpeg = io.BytesIO()
            image.save(jpeg, format="JPEG", quality=80)
    except (ValueError, OSError):
        raise ProbeError("oci_frame_invalid") from None
    codec=re.search(rb"Video:\s*([a-zA-Z0-9_.-]+)",result.stderr)
    return Capture(gray, identity, received_epoch if received_epoch is not None else time.time(),
                   received_monotonic if received_monotonic is not None else time.monotonic(),
                   codec[1].decode() if codec else "unknown", time.monotonic() - before, gray.width, gray.height), jpeg.getvalue()


class MonitorWorker:
    def __init__(self, config: dict, root: Path):
        self.config, self.root = config, root
        self.boot_id = Path('/proc/sys/kernel/random/boot_id').read_text().strip()
        assert_tmpfs(root)
        root.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.runner = ProcessRunner()
        self.ocr = TesseractOCR(self.runner)
        self.analyzer = VerifiedClockAnalyzer(TARGET,VerifiedClockConfig(tuple(config.get('roi',[.019,.968,.04,.022])),tuple(config.get('utc_roi',[.081,.968,.04,.022]))))
        self.analyzers={TARGET:self.analyzer}
        self.readers={TARGET:FrameReceiver()}
        self.fallback=None
        if config.get('fallback_udp_port'):
            self.fallback=ObservedTransportBuffer()
            self.fallback.start(port=config['fallback_udp_port'])
            self.analyzers[FALLBACK]=VerifiedClockAnalyzer(FALLBACK,VerifiedClockConfig(tuple(config.get('fallback_roi',config.get('roi',[.019,.968,.04,.022]))),tuple(config.get('fallback_utc_roi',config.get('utc_roi',[.081,.968,.04,.022])))))
            self.readers[FALLBACK]=FrameReceiver()
        self.evidence = ClockEvidenceStore(root / "evidence", max_bytes=8 * 1024 * 1024, retention_seconds=86400, platforms=tuple(self.analyzers))
        self.current = {"schema": SCHEMA, "targets": {}}
        self.stop = threading.Event()
        self.changed=threading.Condition()
        self.completed_monotonic = time.monotonic()
        self.cycle = 0
        self.completed_cycle=0
        self.started_epoch=time.time()
        self.last_valid_publications={}
        self.last_clock_sample_monotonic=0.

    def _decoded(self,frame,identity,before):
        from .viewer_clock_capture import CommandResult
        capture,jpeg=decoded_capture(CommandResult(0,frame.content,('Video: '+frame.codec).encode()),identity,before,
                                     received_epoch=frame.received_epoch,received_monotonic=frame.received_monotonic)
        return capture,jpeg,{'frame_received_at_utc':utc_timestamp(frame.received_epoch),'frame_pts_seconds':frame.pts,
                             'reader_session':frame.session,'reader_frame_sequence':frame.sequence,
                             'receiver_frame_age_seconds':round(time.monotonic()-frame.received_monotonic,3),
                             'measurement_timing_valid':frame.timing_valid,'measurement_timing_reason':frame.timing_reason}

    def capture_relay(self,deadline):
        before=time.monotonic();reader=self.readers[TARGET]
        if not reader.running:reader.start(relay_args(self.config))
        frame=reader.take(deadline=deadline)
        return self._decoded(frame,self.config['source_identity'],before)

    def capture_fallback(self,deadline):
        packet=self.fallback.snapshot()
        if packet is None:raise ProbeError('fallback_stream_not_observed')
        data,metadata=packet;before=time.monotonic();reader=self.readers[FALLBACK]
        if not reader.running or reader.input_peer!=metadata['transport_peer_ip']:
            reader.start(fallback_args(),piped=True,peer=metadata['transport_peer_ip'],bootstrap=data)
            self.fallback.on_packet=reader.offer
        frame=reader.take(deadline=deadline)
        capture,jpeg,timing=self._decoded(frame,'dell_youtube_fallback:'+metadata['transport_peer_ip'],before)
        return capture,jpeg,{**metadata,**timing,'receiver_dropped_packets':reader.dropped}

    def evaluate(self,name,started):
        analyzer=self.analyzers[name];metadata={}
        try:
            if name==FALLBACK:
                capture,jpeg,metadata=self.capture_fallback(min(started+5,time.monotonic()+3))
            else:capture,jpeg,metadata=self.capture_relay(min(started+5,time.monotonic()+3))
            sample_due=name==TARGET and time.monotonic()-self.last_clock_sample_monotonic>=60
            row, images = analyzer.analyze(capture, lambda roi: self.ocr.read(roi, deadline=min(started+8,time.monotonic()+1)), now=time.time(),timing_valid=metadata.get('measurement_timing_valid',False),retain_evidence=sample_due,
                                           utc_retry_left_pixels=3 if name==TARGET else 0)
            sample_id = hashlib.sha256(f'{os.environ.get("POD_UID","local")}:{time.time_ns()}:{self.cycle}'.encode()).hexdigest()[:24]
            path = self.root / "capture" / ("oci_monitor" if name==TARGET else FALLBACK) / (sample_id + ".jpg")
            path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            row.update(sample_id=sample_id,image_path=str(path.relative_to(self.root)),
                       image_sha256=hashlib.sha256(jpeg).hexdigest(),observer_pod_uid=os.environ.get('POD_UID','local'))
            try:
                atomic_bytes(path, jpeg)
                if row.get('clock_reading_valid'):self.last_valid_publications[name]=capture.captured_epoch
                if images or row.get('clock_evidence_kind') in ('periodic_roi','roi_retry'):
                    if row.get('clock_evidence_kind')=='periodic_roi':self.last_clock_sample_monotonic=time.monotonic()
                    try:
                        row['evidence_id']=self.evidence.save(row,images,now=time.time(),clock_images=analyzer.evidence_images)
                        row['native_evidence_status']='saved' if row['evidence_id'] else 'capacity_exhausted'
                    except (OSError,ValueError):row['native_evidence_status']='storage_or_evidence_error'
                elif row['clock_state'].startswith('ADVANCING'):self.evidence.recovered(name)
            except OSError:
                row.update(analyzed_clock_state=row['clock_state'],clock_state='UNKNOWN',capture_ok=False,
                           reason='snapshot_io_error',error_stage='publication',delay_valid=False)
            row.update(sample_id=sample_id, image_path=str(path.relative_to(self.root)), image_sha256=hashlib.sha256(jpeg).hexdigest(), checked_at_utc=utc_timestamp(time.time()))
            del capture, jpeg
        except ProbeError as exc:
            row = analyzer.unknown(str(exc), now=time.time())
        except Exception:
            row = analyzer.unknown("observer_analysis_or_resource_error", now=time.time())
        row.update(metadata)
        row["observation_scope"] = "oci_common_relay_output" if name==TARGET else 'dell_youtube_fallback_encoded_output'
        row['last_verified_clock_at_utc']=row.get('last_valid_observation_at_utc')
        row['last_valid_observation_at_utc']=utc_timestamp(self.last_valid_publications[name]) if name in self.last_valid_publications else None
        try:
            latest=self.root/'capture'/('oci_monitor' if name==TARGET else FALLBACK)/'latest.jpg'
            if row.get('capture_ok'):
                temporary=latest.with_name('.latest.tmp');temporary.unlink(missing_ok=True)
                temporary.symlink_to(Path(row['image_path']).name);os.replace(temporary,latest)
            else:latest.unlink(missing_ok=True)
            files=sorted((p for p in latest.parent.glob('*.jpg') if not p.is_symlink()),key=lambda p:p.stat().st_mtime,reverse=True)
            for path in files[2:]:path.unlink(missing_ok=True)
        except OSError:
            row.update(clock_state='UNKNOWN',capture_ok=False,reason='snapshot_io_error',error_stage='publication',delay_valid=False)
        return row

    def run_cycle(self):
        started=time.monotonic();self.cycle+=1
        # Fallback is checked first, including when the OCI connection stalls.
        targets={}
        for name in ([FALLBACK,TARGET] if self.fallback else [TARGET]):targets[name]=self.evaluate(name,started)
        payload = {"schema": SCHEMA, "checked_at_utc": utc_timestamp(time.time()), "targets": targets,
                   "source_host_id":"delivery-host", "source_boot_id":self.boot_id,
                   "worker_revision":WORKER_REVISION, "source_identity":self.config["source_identity"],
                   "pod_uid":os.environ.get("POD_UID","local"), "cycle":self.cycle,
                   'contract_version':2,'started_at_utc':utc_timestamp(self.started_epoch),
                   'capabilities':{'continuous_capture':True,'ocr_every_sample':True,'clock_pair_verification':True},
                   "worker":{"duration_seconds":round(time.monotonic()-started,3),"normal_image_storage":"tmpfs",'sample_interval_seconds':10}}
        try:atomic_bytes(self.root / "oci_monitor_status.json", json_bytes(payload))
        except OSError:payload['worker']['state_write_error']=True
        self.completed_monotonic = time.monotonic()
        self.completed_cycle=self.cycle
        try:
            history=self.root/'history';history.mkdir(mode=0o700,exist_ok=True)
            append_history(history,payload,max_bytes=1024*1024);self.evidence.prune(now=time.time())
        except OSError:payload['worker']['history_or_retention_error']=True
        with self.changed:self.current=payload;self.changed.notify_all()
        return payload

    def heartbeat(self):
        return {"schema":"stream_v3.observer_heartbeat.v1","component":"oci_monitor_probe","checked_at_utc":utc_timestamp(time.time()),
                "completed_cycle":self.completed_cycle,"cycle_age_seconds":round(time.monotonic()-self.completed_monotonic,3),
                "worker_ok":time.monotonic()-self.completed_monotonic<90,"source_host_id":"delivery-host",
                "source_boot_id":self.boot_id,"pod_uid":os.environ.get("POD_UID","local")}

    def serve(self, host, port):
        worker=self
        class Handler(BaseHTTPRequestHandler):
            def setup(self):
                super().setup(); self.connection.settimeout(2)
            def log_message(self,*_args):pass
            def do_GET(self):
                parsed=urlsplit(self.path)
                if parsed.path=='/bundle.json':
                    query=parse_qs(parsed.query)
                    try:after=int(query.get('after_cycle',[-1])[0]);wait=min(10.,max(0.,float(query.get('wait',[0])[0])))
                    except (ValueError,TypeError):self.send_error(400);return
                    limit=time.monotonic()+wait
                    with worker.changed:
                        while (worker.current.get('cycle',0)==after and query.get('pod_uid',[os.environ.get('POD_UID','local')])[0]==os.environ.get('POD_UID','local')
                               and not worker.stop.is_set() and time.monotonic()<limit):worker.changed.wait(min(.5,limit-time.monotonic()))
                code,mime=200,"application/json"
                if self.path in ("/healthz","/heartbeat.json"):
                    content=json_bytes(worker.heartbeat());code=200 if worker.heartbeat()["worker_ok"] else 503
                elif self.path=="/oci_monitor_status.json":content=json_bytes(worker.current)
                elif parsed.path=="/bundle.json":
                    current=worker.current;images={};clock_evidence={}
                    for name,row in current.get("targets",{}).items():
                        if row.get("capture_ok") and time.time()-epoch(row["checked_at_utc"])<=180:
                            images[name]=base64.b64encode((worker.root/row["image_path"]).read_bytes()).decode()
                            evidence_id=row.get('evidence_id')
                            if evidence_id:
                                try:
                                    with (worker.evidence.root/evidence_id).open('rb') as file:native=file.read(MAX_BUNDLE_BYTES+1)
                                    if len(native)<=MAX_BUNDLE_BYTES:clock_evidence[name]=base64.b64encode(native).decode()
                                except OSError:pass
                    content=json_bytes({"schema":"stream_v3.oci_monitor_bundle.v1","state":current,"heartbeat":worker.heartbeat(),"images":images,
                                        "clock_evidence":clock_evidence})
                elif self.path in ("/capture/oci_monitor/latest.jpg","/capture/youtube_fallback/latest.jpg"):
                    name=TARGET if '/oci_monitor/' in self.path else FALLBACK
                    row=worker.current.get("targets",{}).get(name,{})
                    if row.get("capture_ok") and time.time()-epoch(row["checked_at_utc"])<=180:
                        content=(worker.root/row["image_path"]).read_bytes();mime="image/jpeg"
                    else:code,content=503,b""
                else:code,content=404,b""
                self.send_response(code);self.send_header("Content-Type",mime);self.send_header("Cache-Control","no-store")
                self.send_header("Content-Length",str(len(content)))
                key=Path(worker.config["key_file"]).read_bytes()
                self.send_header("X-Stream-Observation-Signature",hmac.new(key,b"oci-monitor-observation-v1\0"+content,hashlib.sha256).hexdigest())
                self.end_headers();self.wfile.write(content)
        class Server(ThreadingHTTPServer):
            daemon_threads=True
            def __init__(self,*args):self.slots=threading.BoundedSemaphore(4);super().__init__(*args)
            def process_request(self,request,address):
                if not self.slots.acquire(blocking=False):self.shutdown_request(request);return
                try:super().process_request(request,address)
                except Exception:self.slots.release();raise
            def process_request_thread(self,request,address):
                try:super().process_request_thread(request,address)
                finally:self.slots.release()
        server=Server((host,port),Handler)
        threading.Thread(target=server.serve_forever,daemon=True).start()
        return server


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config",type=Path,required=True);parser.add_argument("--ram-root",type=Path,default=Path("/ram"))
    parser.add_argument("--host",default="127.0.0.1");parser.add_argument("--port",type=int,default=8766);parser.add_argument("--once",action="store_true")
    args=parser.parse_args();config=json.loads(args.config.read_text())
    source=urlsplit(config["source_url"])
    if source.scheme!="rtmps" or source.username or source.password or source.path!="/relay" or source.query or source.fragment:
        raise ValueError("source_must_be_pinned_oci_rtmps_relay")
    if hashlib.sha256(Path(config["ca_file"]).read_bytes()).hexdigest()!=config["source_identity"]:
        raise ValueError("source_ca_identity_mismatch")
    worker=MonitorWorker(config,args.ram_root)
    lock=(args.ram_root/"worker.lock").open("w");fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    for sig in (signal.SIGINT,signal.SIGTERM):signal.signal(sig,lambda *_:(worker.stop.set(),setattr(worker.runner,"stopping",True)))
    server=worker.serve(args.host,args.port)
    try:
        while not worker.stop.is_set():
            started=time.monotonic();worker.run_cycle()
            if args.once:break
            worker.stop.wait(max(0,10-(time.monotonic()-started)))
    finally:
        worker.runner.stopping=True
        for reader in worker.readers.values():reader.close()
        if worker.fallback:worker.fallback.close()
        server.shutdown();server.server_close();lock.close()
    return 0


if __name__=="__main__":raise SystemExit(main())
