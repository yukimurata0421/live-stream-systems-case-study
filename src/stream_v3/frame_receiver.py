"""Bounded live-frame slot; continuously drain FFmpeg without saving normal images."""
from collections import deque
from dataclasses import dataclass,field
import os
import math
import queue
import random
import re
import selectors
import signal
import subprocess
import threading
import time

from .viewer_clock_capture import ProbeError
from .fallback_monitor import TransportBuffer
from urllib.parse import urlsplit

FRAME_LIMIT=1920*1080*3+128
HEADER=re.compile(rb'P6\s+(\d+)\s+(\d+)\s+255\n')
PTS=re.compile(rb'\bn:\s*(\d+).*?pts_time:\s*([-+0-9.eE]+)')


def error_code(line):
    text=line.lower()
    if any(x in text for x in (b'could not find codec parameters',b'does not contain any stream')):return 'capture_probe_failed'
    if any(x in text for x in (b'certificate',b'tls handshake',b'ssl error')):return 'capture_tls_failed'
    if any(x in text for x in (b'connection refused',b'no route to host',b'network is unreachable',b'failed to resolve')):return 'capture_connect_failed'
    if any(x in text for x in (b'timed out',b'connection timeout')):return 'capture_timeout'
    if any(x in text for x in (b'no one is publishing',b'stream not found')):return 'capture_source_unavailable'
    if any(x in text for x in (b'error while decoding',b'corrupt',b'invalid data',b'non-existing pps',b'decode_slice_header error')):return 'capture_decode_failed'
    return None


@dataclass(frozen=True)
class ReceivedFrame:
    content: bytes=field(repr=False)
    received_epoch: float=0.
    received_monotonic: float=0.
    pts: float=0.
    sequence: int=0
    session: int=0
    timing_valid: bool=False
    timing_reason: str='starting'
    codec: str='unknown'


class FrameTiming:
    """Session-local timing check; cumulative drift never reanchors itself."""
    def __init__(self):self.origin=None;self.previous=None;self.steady=0

    def observe(self,pts,mono,wall):
        valid=False;reason='timing_warmup'
        if self.previous:
            p,m,w=self.previous;pg=pts-p;mg=mono-m;wg=wall-w
            if pg<=0 or mg<.25 or abs(pg-mg)>2:
                self.steady=0;reason='frame_timing_discontinuity'
            elif abs(wg-mg)>1:
                self.steady=0;reason='observer_wall_clock_step'
            else:
                self.steady+=1;valid=self.steady>=2;reason='consistent' if valid else 'timing_warmup'
        # Input probing can release several buffered frames in a burst. Anchor
        # once after warmup, then keep that origin for the whole session.
        if self.origin is None and valid:self.origin=(pts,mono,wall)
        if self.origin is not None and abs((pts-self.origin[0])-(mono-self.origin[1]))>1:
            valid=False;reason='receiver_cumulative_timing_drift'
        if self.origin is not None and abs((wall-self.origin[2])-(mono-self.origin[1]))>1:
            valid=False;reason='observer_wall_clock_step'
        self.previous=(pts,mono,wall)
        return valid,reason


class FrameReceiver:
    def __init__(self):
        self.lock=threading.Lock();self.changed=threading.Condition(self.lock)
        self.process=None;self.thread=None;self.stop_requested=threading.Event()
        self.latest=None;self.session=0;self.error='capture_starting';self.last_taken=None
        self.input=queue.Queue(maxsize=256);self.input_peer=None;self.dropped=0;self.last_drop=-1e30
        self.codec='unknown'
        self.starts=0

    def start(self,args,*,piped=False,peer=None,bootstrap=b''):
        self.close();self.stop_requested.clear();self.session+=1;self.latest=None;self.error='capture_starting'
        self.input_peer=peer;self.last_taken=None;self.input=queue.Queue(maxsize=256);self.codec='unknown'
        # Wait in the reader thread so the scheduler's fixed tick cannot lock
        # reconnects to the source GOP. First connections start immediately.
        delay=random.uniform(.1,2.) if self.starts else 0.
        self.starts+=1
        if bootstrap:self.input.put_nowait(bootstrap[-32768:])
        self.process=None
        self.thread=threading.Thread(target=self._launch,args=(args,piped,delay),daemon=True);self.thread.start()

    def _launch(self,args,piped,delay):
        if self.stop_requested.wait(delay):return
        try:
            self.process=subprocess.Popen(args,stdin=subprocess.PIPE if piped else subprocess.DEVNULL,
                                          stdout=subprocess.PIPE,stderr=subprocess.PIPE,start_new_session=True,
                                          env={**os.environ,'OMP_THREAD_LIMIT':'1','OMP_NUM_THREADS':'1','OPENBLAS_NUM_THREADS':'1'})
        except OSError:
            self.error='capture_executable_unavailable'
            with self.changed:self.changed.notify_all()
            return
        self._read()

    def offer(self,data,peer):
        if peer!=self.input_peer or not self.running:return
        try:self.input.put_nowait(data)
        except queue.Full:self.dropped+=1;self.last_drop=time.monotonic()

    @property
    def running(self):return self.thread is not None and self.thread.is_alive()

    def _read(self):
        process=self.process;selector=selectors.DefaultSelector();pixels=bytearray();errors=bytearray()
        frames=deque();timestamps={};index=0;pending=b'';last_io=time.monotonic();timing=FrameTiming()
        warning=-1e30;fixed_error=None;session=self.session
        try:
            for stream,kind in ((process.stdout,'out'),(process.stderr,'err')):
                os.set_blocking(stream.fileno(),False);selector.register(stream,selectors.EVENT_READ,kind)
            if process.stdin:os.set_blocking(process.stdin.fileno(),False)
            while not self.stop_requested.is_set():
                if process.stdin and not process.stdin.closed and not pending:
                    chunks=[]
                    for _ in range(128):
                        try:chunks.append(self.input.get_nowait())
                        except queue.Empty:break
                    pending=b''.join(chunks)
                    if pending and process.stdin not in [k.fileobj for k in selector.get_map().values()]:
                        selector.register(process.stdin,selectors.EVENT_WRITE,'in')
                for key,_ in selector.select(.1):
                    if key.data=='in':
                        try:n=os.write(key.fd,pending);pending=pending[n:]
                        except BrokenPipeError:pending=b''
                        if not pending:selector.unregister(key.fileobj)
                        continue
                    try:data=os.read(key.fd,65536)
                    except BlockingIOError:continue
                    if not data:
                        selector.unregister(key.fileobj);continue
                    last_io=time.monotonic()
                    if key.data=='err':
                        errors.extend(data)
                        if len(errors)>65536:del errors[:-8192]
                        while b'\n' in errors:
                            line,_,rest=errors.partition(b'\n');errors=bytearray(rest)
                            match=PTS.search(line)
                            if match:
                                value=float(match[2])
                                if not math.isfinite(value):raise ProbeError('capture_frame_timestamp_invalid')
                                timestamps[int(match[1])]=value
                                if len(timestamps)>32:raise ProbeError('capture_frame_metadata_overflow')
                            codec=re.search(rb'Video:\s*([a-zA-Z0-9_.-]+)',line)
                            if codec and self.codec=='unknown':self.codec=codec[1].decode()
                            code=error_code(line)
                            if code:
                                fixed_error=code
                                if code=='capture_decode_failed':warning=time.monotonic()
                    else:
                        pixels.extend(data)
                        while pixels:
                            match=HEADER.match(pixels[:128])
                            if not match:
                                if len(pixels)>128:raise ProbeError('capture_frame_header_invalid')
                                break
                            width,height=map(int,match.groups())
                            if not (8<=width<=1920 and 4<=height<=1080):raise ProbeError('capture_frame_dimensions_invalid')
                            end=match.end()+width*height*3
                            if len(pixels)<end:break
                            frames.append((index,bytes(pixels[:end]),time.time(),time.monotonic()));del pixels[:end];index+=1
                            if len(frames)>3:raise ProbeError('capture_frame_metadata_unavailable')
                        if len(pixels)>FRAME_LIMIT:raise ProbeError('capture_frame_buffer_limit')
                while frames and frames[0][0] in timestamps:
                    n,raw,wall,mono=frames.popleft();pts=timestamps.pop(n)
                    valid,reason=timing.observe(pts,mono,wall)
                    if mono-warning<2:valid=False;reason='decoder_warning'
                    if mono-self.last_drop<10:valid=False;reason='input_packet_drop'
                    frame=ReceivedFrame(raw,wall,mono,pts,n,session,valid,reason,self.codec)
                    # A recovered decode warning must not mask a later EOF.
                    fixed_error=None
                    with self.changed:self.latest=frame;self.error='';self.changed.notify_all()
                if process.poll() is not None and not selector.get_map():break
                if time.monotonic()-last_io>12:raise ProbeError('capture_timeout')
            if not self.stop_requested.is_set():self.error=fixed_error or 'capture_stream_ended'
        except ProbeError as error:self.error=str(error)
        except Exception:self.error='capture_reader_io_error'
        finally:
            try:os.killpg(process.pid,signal.SIGKILL)
            except ProcessLookupError:pass
            process.wait();selector.close()
            for stream in (process.stdin,process.stdout,process.stderr):
                if stream is not None:stream.close()
            with self.changed:self.changed.notify_all()

    def take(self,*,deadline,max_age=3.):
        with self.changed:
            while True:
                frame=self.latest
                key=(frame.session,frame.sequence) if frame else None
                if frame and key!=self.last_taken and time.monotonic()-frame.received_monotonic<=max_age:
                    self.last_taken=key;return frame
                remaining=deadline-time.monotonic()
                if remaining<=0 or not self.running:raise ProbeError(self.error or 'capture_frame_stale')
                self.changed.wait(min(.2,remaining))

    def close(self):
        self.stop_requested.set()
        process=self.process
        if process and process.poll() is None:
            try:os.killpg(process.pid,signal.SIGKILL)
            except ProcessLookupError:pass
        if self.thread:self.thread.join(timeout=3)
        # Launch may have crossed the stop request just before Popen.
        process=self.process
        if self.thread and self.thread.is_alive() and process and process.poll() is None:
            try:os.killpg(process.pid,signal.SIGKILL)
            except ProcessLookupError:pass
            self.thread.join(timeout=2)
        if self.thread and self.thread.is_alive():raise ProbeError('capture_child_cleanup_timeout')
        if process:
            try:process.wait(timeout=2)
            except subprocess.TimeoutExpired:raise ProbeError('capture_child_cleanup_timeout')


class ObservedTransportBuffer(TransportBuffer):
    def __init__(self):super().__init__();self.on_packet=None
    def feed(self,data,peer,*,now=None):
        valid=bool(data) and len(data)<=1316 and len(data)%188==0 and all(data[i]==0x47 for i in range(0,len(data),188))
        if not valid:return
        super().feed(data,peer,now=now)
        if self.on_packet:self.on_packet(data,peer)


def output_args():
    return ['-an','-sn','-dn','-vf',"select='isnan(prev_selected_t)+gte(t-prev_selected_t,1)',showinfo=checksum=0",
            '-vsync','0','-threads','1','-c:v','ppm','-f','image2pipe','pipe:1']


def relay_args(config):
    tls=['-tls_verify','1','-ca_file',config['ca_file']] if urlsplit(config['source_url']).scheme=='rtmps' else []
    return ['ffmpeg','-hide_banner','-nostdin','-loglevel','info','-threads','1','-filter_threads','1',
            '-protocol_whitelist','rtmp,rtmps,tls,tcp,crypto','-rw_timeout','5000000',
            *tls,'-analyzeduration','5000000','-probesize','5000000',
            '-i',config['source_url'],*output_args()]


def fallback_args():
    return ['ffmpeg','-hide_banner','-nostdin','-loglevel','info','-threads','1','-filter_threads','1',
            '-skip_frame','nokey','-protocol_whitelist','pipe,crypto','-analyzeduration','500000','-probesize','262144',
            '-f','mpegts','-i','pipe:0',*output_args()]
