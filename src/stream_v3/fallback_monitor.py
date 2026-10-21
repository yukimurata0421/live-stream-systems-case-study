"""Direct-YouTube secondary output and bounded RAM reception of droppable MPEG-TS packets.

Use only the standard library so the sender can mount this module independently.
"""
from collections import deque
import ipaddress
import json
import socket
import sys
import threading
import time
from urllib.parse import parse_qs, urlsplit


FIFO_OPTIONS = ('queue_size=32:drop_pkts_on_overflow=1:attempt_recovery=1:'
                'max_recovery_attempts=3:recovery_wait_time=1:restart_with_keyframe=1')


def render_launcher(original, destination):
    marker="sys.argv=['stream_engine']"
    if original.count(marker)!=1 or 'install_monitor_tap' in original:
        raise ValueError('backup_launcher_preimage_unexpected')
    addition=("sys.path.insert(0, '/monitor-tap')\n"
              "from fallback_monitor import install as install_monitor_tap\n"
              f"install_monitor_tap(se.StreamEngine, {destination!r})\n")
    return original.replace(marker,addition+marker,1)


def add_monitor_output(args, destination):
    """Add tee output only to the known backup encoder; the caller retains original arguments for unsupported shapes."""
    source = urlsplit(args[-1])
    target = urlsplit(destination)
    address = ipaddress.ip_address(target.hostname or '')
    if (target.scheme != 'udp' or not address.is_private or address.is_unspecified
            or address.is_multicast or target.port != 8767 or target.username or target.password
            or target.path or target.fragment
            or parse_qs(target.query) != {'pkt_size':['1316'], 'connect':['0'], 'buffer_size':['65536']}):
        raise ValueError('invalid_local_monitor_destination')
    if (args[-3:-1] != ['-f', 'flv'] or source.scheme not in ('rtmp','rtmps')
            or not source.hostname or any(x in args[-1] for x in "|[]'\\\n\r")
            or args.count('-i') != 2 or '-map' in args
            or args[args.index('-c:v')+1] != 'libx264'
            or args[args.index('-c:a')+1] != 'aac'):
        raise ValueError('unsupported_backup_output_shape')
    prefix = list(args[:-3])
    index = prefix.index('-filter_complex')+1
    if any(x in prefix[index] for x in '[];'):
        raise ValueError('unsupported_backup_video_filter')
    prefix[index] = '[0:v:0]'+prefix[index]+'[monitor_video]'
    options = ['f=flv','use_fifo=0','onfail=abort']
    for option in ('-rw_timeout','-flvflags'):
        if option in prefix:
            index=prefix.index(option);value=prefix[index+1]
            if any(x in value for x in ":|[]'\\"):
                raise ValueError('unsupported_backup_muxer_option')
            options.append(option[1:]+'='+value);del prefix[index:index+2]
    primary='['+':'.join(options)+']'+args[-1]
    monitor='[f=mpegts:use_fifo=1:onfail=ignore]'+destination
    return prefix+['-map','[monitor_video]','-map','1:a:0','-flags:v','+global_header',
                   '-use_fifo','1','-fifo_options',FIFO_OPTIONS,'-f','tee',primary+'|'+monitor]


def install(engine_class, destination):
    original=engine_class.ffmpeg_args
    reported=False
    def monitored(self,*args,**kwargs):
        nonlocal reported
        values=original(self,*args,**kwargs)
        try:return add_monitor_output(values,destination)
        except (ValueError,IndexError,TypeError):
            if not reported:
                print(json.dumps({'event':'fallback_monitor_tap_unavailable',
                                  'reason':'unsupported_output_or_destination'}),file=sys.stderr,flush=True)
                reported=True
            return values
    engine_class.ffmpeg_args=monitored


class TransportBuffer:
    """Retain at most 4 MiB / 6 seconds; a stopped UDP receiver never waits on the sender."""
    def __init__(self, *, max_bytes=4*1024*1024, seconds=6., freshness=3.):
        self.max_bytes,self.seconds,self.freshness=max_bytes,seconds,freshness
        self.packets=deque();self.size=0;self.peer=None;self.last_received=None
        self.lock=threading.Lock();self.stop=threading.Event();self.socket=None;self.thread=None

    def feed(self,data,peer,*,now=None):
        now=time.monotonic() if now is None else now
        if not data or len(data)>1316 or len(data)%188 or any(data[i]!=0x47 for i in range(0,len(data),188)):
            return
        with self.lock:
            if peer!=self.peer:
                self.packets.clear();self.size=0;self.peer=peer
            self.last_received=now;self.packets.append((now,data));self.size+=len(data)
            while self.packets and (self.size>self.max_bytes or self.packets[0][0]<now-self.seconds):
                self.size-=len(self.packets.popleft()[1])

    def snapshot(self,*,now=None):
        now=time.monotonic() if now is None else now
        with self.lock:
            if not self.packets or self.last_received is None or not 0<=now-self.last_received<=self.freshness:
                return None
            return b''.join(x[1] for x in self.packets),{
                'transport_peer_ip':self.peer,'transport_age_seconds':round(now-self.last_received,3),
                'buffer_bytes':self.size,'buffer_span_seconds':round(self.packets[-1][0]-self.packets[0][0],3)}

    def start(self,host='0.0.0.0',port=8767):
        sock=socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET,socket.SO_RCVBUF,262144);sock.bind((host,port));sock.settimeout(.25)
        self.socket=sock
        def receive():
            while not self.stop.is_set():
                try:
                    data,peer=sock.recvfrom(65536);self.feed(data,peer[0])
                except socket.timeout:continue
                except OSError:break
        self.thread=threading.Thread(target=receive,daemon=True);self.thread.start()

    def close(self):
        self.stop.set()
        if self.socket:self.socket.close()
        if self.thread:self.thread.join(timeout=1)
