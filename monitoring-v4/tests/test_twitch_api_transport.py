"""Real TLS/HTTP and process-deadline tests use only owned local endpoints."""
import os
import shutil
import socket
import ssl
import subprocess
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from stream_monitoring_v4.collectors import twitch_api as api


@pytest.fixture(scope='module')
def tls_server(tmp_path_factory):
    root=tmp_path_factory.mktemp('twitch-tls');cert=root/'cert.pem';key=root/'key.pem'
    subprocess.run(['openssl','req','-x509','-newkey','rsa:2048','-nodes','-days','1',
                    '-subj','/CN=api.twitch.tv','-addext','subjectAltName=DNS:api.twitch.tv',
                    '-keyout',str(key),'-out',str(cert)],check=True,capture_output=True)
    calls=[]
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            calls.append((self.path,self.headers.get('Authorization')))
            if self.path=='/slow_headers':time.sleep(1)
            status=429 if self.path=='/rate' else 302 if self.path=='/redirect' else 200
            self.send_response(status)
            self.send_header('Ratelimit-Remaining','0' if status==429 else '799')
            self.send_header('Ratelimit-Limit','800');self.send_header('Retry-After','120')
            if status==302:self.send_header('Location','https://api.twitch.tv/never-follow')
            if self.path!='/flood':self.send_header('Content-Length','2' if self.path=='/invalid' else '11')
            self.end_headers()
            try:
                if self.path=='/slow_body':
                    self.wfile.write(b'{"data":');self.wfile.flush();time.sleep(1);self.wfile.write(b'[]}')
                elif self.path=='/flood':self.wfile.write(b'x'*100000)
                elif self.path=='/invalid':self.wfile.write(b'[]')
                else:self.wfile.write(b'{"data":[]}')
            except OSError:pass
        def do_POST(self):
            body=self.rfile.read(int(self.headers.get('Content-Length','0')))
            calls.append(('@body',body.decode('ascii')))
            self.do_GET()
        def log_message(self,*args):pass
    server=ThreadingHTTPServer(('127.0.0.1',0),Handler);server.daemon_threads=True
    context=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER);context.load_cert_chain(cert,key)
    server.socket=context.wrap_socket(server.socket,server_side=True)
    server.handle_error=lambda *args:None
    thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
    yield cert,server.server_address[1],calls
    server.shutdown();server.server_close();thread.join(timeout=2)


@pytest.fixture
def local_transport(monkeypatch,tls_server):
    cert,port,calls=tls_server;original=api.subprocess.Popen;commands=[]
    def process(command,**kwargs):
        commands.append(list(command))
        return original(command+['--cacert',str(cert),'--connect-to',f'api.twitch.tv:443:127.0.0.1:{port}'],**kwargs)
    monkeypatch.setattr(api.subprocess,'Popen',process)
    return commands,calls


def test_real_tls_json_quota_and_secret_stdin(local_transport):
    commands,calls=local_transport
    value,headers=api.request_json('https://api.twitch.tv/ok',{'Authorization':'Bearer dummyToken123'},b'dummySecret123',2)
    assert value=={'data':[]} and headers['Ratelimit-Remaining']=='799'
    assert set(headers.transport_seconds)==set(api.TIMED_PHASES)
    assert calls[-1]==('/ok','Bearer dummyToken123')
    assert all('dummy' not in str(command) for command in commands)
    assert '--insecure' not in commands[-1] and '--retry' in commands[-1]


def test_request_body_is_literal_and_cannot_read_a_file(local_transport):
    _,calls=local_transport
    api.request_json('https://api.twitch.tv/ok',{},b'@/etc/hostname',2)
    assert calls[-2]==('@body','@/etc/hostname')


def test_untrusted_tls_certificate_is_rejected(monkeypatch,tls_server):
    _,port,_=tls_server;original=api.subprocess.Popen
    def process(command,**kwargs):
        return original(command+['--connect-to',f'api.twitch.tv:443:127.0.0.1:{port}'],**kwargs)
    monkeypatch.setattr(api.subprocess,'Popen',process)
    with pytest.raises(api.ProbeError) as caught:api.request_json('https://api.twitch.tv/test',{},None,2)
    assert caught.value.reason=='network' and caught.value.transport_phase=='tls'


def test_deadline_without_footer_keeps_observed_phase(monkeypatch):
    monkeypatch.setattr(api,'_curl_transfer',lambda *_:(b'',-9,'timeout','tls'))
    with pytest.raises(api.ProbeError) as caught:api.request_json('https://timeout.invalid/test',{},None,.1)
    assert caught.value.reason=='timeout' and caught.value.transport_phase=='tls'
    assert caught.value.transport_seconds=={}


@pytest.mark.parametrize('path,phase',[('/slow_headers','response_headers'),('/slow_body','response_body')])
def test_stalled_http_is_bounded_and_attributed(local_transport,path,phase):
    started=time.monotonic()
    with pytest.raises(api.ProbeError) as caught:api.request_json('https://api.twitch.tv'+path,{},None,.25)
    assert caught.value.reason=='timeout' and caught.value.transport_phase==phase
    assert time.monotonic()-started<1.0


def test_redirect_never_forwards_authorization(local_transport):
    _,calls=local_transport;count=len(calls)
    with pytest.raises(api.ProbeError) as caught:api.request_json('https://api.twitch.tv/redirect',{'Authorization':'Bearer dummyToken123'},None,2)
    assert caught.value.reason=='redirect_rejected'
    assert calls[count:]==[('/redirect','Bearer dummyToken123')]


def test_429_keeps_retry_after_and_quota(local_transport):
    with pytest.raises(api.ProbeError) as caught:api.request_json('https://api.twitch.tv/rate',{},None,2)
    assert caught.value.status==429 and caught.value.retry_after==120
    assert caught.value.rate_limit['remaining']==0
    assert caught.value.transport_phase=='http'


@pytest.mark.parametrize('path,reason',[('/flood','response_too_large'),('/invalid','response_invalid')])
def test_untrusted_response_is_not_an_observation(local_transport,path,reason):
    with pytest.raises(api.ProbeError) as caught:api.request_json('https://api.twitch.tv'+path,{},None,2)
    assert caught.value.reason==reason


def test_tls_handshake_stall_is_bounded(monkeypatch):
    server=socket.socket();server.bind(('127.0.0.1',0));server.listen(1);port=server.getsockname()[1]
    stop=threading.Event()
    def accept():
        connection,_=server.accept()
        with connection:stop.wait(2)
    thread=threading.Thread(target=accept,daemon=True);thread.start();original=api.subprocess.Popen
    def process(command,**kwargs):return original(command+['--connect-to',f'api.twitch.tv:443:127.0.0.1:{port}'],**kwargs)
    monkeypatch.setattr(api.subprocess,'Popen',process)
    try:
        with pytest.raises(api.ProbeError) as caught:api.request_json('https://api.twitch.tv/test',{},None,.25)
        assert caught.value.reason=='timeout' and caught.value.transport_phase=='tls'
    finally:stop.set();server.close();thread.join(timeout=2)


def test_tcp_connection_stall_is_bounded(monkeypatch):
    server=socket.socket();server.bind(('127.0.0.1',0));server.listen(1);port=server.getsockname()[1]
    clients=[]
    for _ in range(8):
        client=socket.socket();client.setblocking(False);client.connect_ex(('127.0.0.1',port));clients.append(client)
    original=api.subprocess.Popen
    def process(command,**kwargs):return original(command+['--connect-to',f'api.twitch.tv:443:127.0.0.1:{port}'],**kwargs)
    monkeypatch.setattr(api.subprocess,'Popen',process)
    try:
        with pytest.raises(api.ProbeError) as caught:api.request_json('https://api.twitch.tv/test',{},None,.25)
        assert caught.value.reason=='timeout' and caught.value.transport_phase=='connect'
    finally:
        for client in clients:client.close()
        server.close()


def test_dns_stall_is_bounded_without_a_live_dns_query(monkeypatch,tmp_path):
    if not shutil.which('cc'):pytest.skip('C compiler unavailable')
    source=tmp_path/'dns.c';library=tmp_path/'dns.so'
    source.write_text('#include <netdb.h>\n#include <unistd.h>\nint getaddrinfo(const char*a,const char*b,const struct addrinfo*c,struct addrinfo**d){usleep(2000000);return EAI_AGAIN;}\n')
    subprocess.run(['cc','-shared','-fPIC','-o',str(library),str(source)],check=True,capture_output=True)
    original=api.subprocess.Popen
    def process(command,**kwargs):
        kwargs['env']=dict(kwargs['env'],LD_PRELOAD=str(library));return original(command,**kwargs)
    monkeypatch.setattr(api.subprocess,'Popen',process)
    started=time.monotonic()
    with pytest.raises(api.ProbeError) as caught:api.request_json('https://timeout.invalid/test',{},None,.25)
    assert caught.value.reason=='timeout' and caught.value.transport_phase=='dns'
    assert time.monotonic()-started<1.0


def test_hung_transport_process_is_killed_and_reaped(monkeypatch,tmp_path):
    pidfile=tmp_path/'pid';worker=tmp_path/'worker'
    worker.write_text('#!/usr/bin/python3\nimport os,time\nopen('+repr(str(pidfile))+',"w").write(str(os.getpid()))\ntime.sleep(60)\n');worker.chmod(0o700)
    monkeypatch.setattr(api,'CURL_PATH',str(worker));started=time.monotonic()
    with pytest.raises(api.ProbeError) as caught:api.request_json('https://timeout.invalid/test',{},None,.1)
    assert caught.value.reason=='timeout' and caught.value.transport_phase=='deadline'
    assert time.monotonic()-started<1.5
    with pytest.raises(ProcessLookupError):os.kill(int(pidfile.read_text()),0)


def test_config_rejects_plain_http_and_header_injection():
    for url,headers in [('http://api.twitch.tv/test',{}),('https://api.twitch.tv/test',{'Authorization':'bad\r\nInjected: value'})]:
        with pytest.raises(api.ProbeError):api.request_json(url,headers,None,1)
