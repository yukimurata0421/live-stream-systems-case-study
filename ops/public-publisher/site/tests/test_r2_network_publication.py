"""Real TLS/HTTP publication: pooling, receipt safety, DNS and certificate errors."""
import hashlib
import importlib.util
import json
from pathlib import Path
import socket
import ssl
import struct
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest.mock import patch

SCRIPTS = Path(__file__).resolve().parents[1] / 'scripts'
with patch.object(sys, 'path', [str(SCRIPTS), *sys.path]):
    import r2_network as network
    import publication_receipts as storage
    import render_r2_resolver as resolver_renderer
    spec = importlib.util.spec_from_file_location('pooled_publication', SCRIPTS/'push_to_r2.py')
    publisher = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = publisher
    spec.loader.exec_module(publisher)


class NetworkPublicationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        cls.root = Path(cls.tmp.name)
        cls.cert, cls.key = cls.root/'cert.pem', cls.root/'key.pem'
        subprocess.run(['openssl','req','-x509','-newkey','ec','-pkeyopt','ec_paramgen_curve:P-256',
            '-nodes','-days','1','-subj','/CN=api.cloudflare.com','-addext','subjectAltName=DNS:api.cloudflare.com',
            '-keyout',str(cls.key),'-out',str(cls.cert)],capture_output=True,check=True,timeout=10)

    @classmethod
    def tearDownClass(cls):
        cls.tmp.cleanup()

    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(); self.addCleanup(self.directory.cleanup)
        self.objects, self.calls, self.sni = {}, [], []
        self.drop_put = self.discard_put = self.redirect = self.close_after_get = False
        self.barrier = None
        self.put_count = 0
        outer = self

        class Server(ThreadingHTTPServer):
            daemon_threads = True
            def handle_error(self,*args): pass

        class Server6(Server):
            address_family=socket.AF_INET6

        class Handler(BaseHTTPRequestHandler):
            protocol_version = 'HTTP/1.1'
            def log_message(self,*args): pass
            def reply(self,status,raw):
                self.send_response(status);self.send_header('Content-Length',str(len(raw)))
                if outer.redirect:self.send_header('Location','https://unowned.invalid/credential')
                self.end_headers();self.wfile.write(raw);self.wfile.flush()
            def do_GET(self):
                outer.calls.append(('GET',self.path,id(self.connection),self.connection.family))
                if outer.redirect:self.reply(302,b'');return
                if self.path not in outer.objects:self.reply(404,b'not found');return
                self.reply(200,outer.objects[self.path])
                if outer.close_after_get:
                    self.close_connection=True;self.connection.shutdown(socket.SHUT_RDWR)
            def do_PUT(self):
                raw=self.rfile.read(int(self.headers['Content-Length']))
                outer.calls.append(('PUT',self.path,id(self.connection),self.connection.family));outer.put_count+=1
                count=outer.put_count
                if not outer.discard_put:outer.objects[self.path]=raw
                if outer.barrier is not None and count<=2:outer.barrier.wait(timeout=3)
                if outer.drop_put:
                    self.close_connection=True;self.connection.shutdown(socket.SHUT_RDWR);return
                self.reply(200,json.dumps({'success':True,'result':{'key':self.path.split('/objects/',1)[1]}}).encode())

        server_context=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        server_context.load_cert_chain(self.cert,self.key)
        server_context.set_servername_callback(lambda sock,name,ctx:self.sni.append(name))
        self.server=Server(('127.0.0.1',0),Handler)
        self.server.socket=server_context.wrap_socket(self.server.socket,server_side=True)
        self.thread=threading.Thread(target=self.server.serve_forever,daemon=True);self.thread.start()
        self.addCleanup(self.server.server_close);self.addCleanup(self.server.shutdown)
        self.server6=Server6(('::1',0),Handler)
        self.server6.socket=server_context.wrap_socket(self.server6.socket,server_side=True)
        self.thread6=threading.Thread(target=self.server6.serve_forever,daemon=True);self.thread6.start()
        self.addCleanup(self.server6.server_close);self.addCleanup(self.server6.shutdown)
        original=socket.getaddrinfo
        self.original_resolve=original
        def resolve(host,port,family=0,*args,**kwargs):
            if host==network.API_HOST:
                self.assertEqual(family,socket.AF_UNSPEC)
                # The resolver returns IPv4 first; the transport must reorder it.
                return (original('127.0.0.1',self.server.server_port,socket.AF_INET,*args,**kwargs)
                    + original('::1',self.server6.server_port,socket.AF_INET6,*args,**kwargs))
            return original(host,port,family,*args,**kwargs)
        self.resolver=patch.object(network.socket,'getaddrinfo',side_effect=resolve)
        self.resolver.start();self.addCleanup(self.resolver.stop)
        context=ssl.create_default_context(cafile=str(self.cert))
        self.pool=network.KeepAliveHTTPS(context=context)
        self.client=publisher.R2Client(publisher.Credentials('a'*32,'private-test-token'),
            'public-site',deadline=time.monotonic()+20,opener=self.pool.open)
        self.addCleanup(self.pool.close)

    def path(self,name):
        return '/client/v4/accounts/'+'a'*32+'/r2/buckets/public-site/objects/public/'+name

    def receipts(self,names,seed=None):
        root=Path(self.directory.name)
        return storage.Receipts(root/'objects.json',root/'events.jsonl',binding='test',allowed=names,
            write=publisher.write_status,seed=seed)

    def test_eight_updates_reuse_two_tls_connections_and_keep_receipts(self):
        names=[f'file{i}.json' for i in range(8)]
        for name in names:self.objects[self.path(name)]=b'old'
        seed={n:hashlib.sha256(b'old').hexdigest() for n in names}
        self.barrier=threading.Barrier(2)
        receipt=self.receipts(names,seed)
        result=publisher.publish(self.client,{n:n.encode() for n in names},{},receipts=receipt)
        self.assertEqual(result['uploaded'],8)
        self.assertEqual(len(self.calls),16);self.assertEqual(len({x[2] for x in self.calls}),2)
        stats=self.pool.snapshot()
        self.assertEqual((stats['connection_attempts'],stats['dns_lookups'],stats['reused_requests']),(2,2,14))
        self.assertEqual(stats['max_in_flight'],2);self.assertEqual(stats['in_flight'],0)
        self.assertEqual((stats['ipv6_connections'],stats['ipv4_connections'],stats['ipv4_fallbacks']),(2,0,0))
        self.assertTrue(all(x[3]==socket.AF_INET6 for x in self.calls))
        self.assertTrue(all(x['state']=='VERIFIED' for x in self.receipts(names).objects.values()))
        self.assertEqual(self.sni,['api.cloudflare.com']*2)

    def test_response_lost_after_put_reconciles_without_a_second_put(self):
        self.drop_put=True
        result=publisher.publish(self.client,{'file.json':b'updated'},{},receipts=self.receipts(['file.json']))
        self.assertEqual(result['reconciled'],1)
        self.assertEqual(sum(x[0]=='PUT' for x in self.calls),1)
        self.assertEqual(self.pool.snapshot()['ipv4_fallbacks'],0)

    def test_lost_uncommitted_put_remains_unknown_across_cycles(self):
        self.drop_put=True;self.discard_put=True
        for _ in range(2):
            result=publisher.publish(self.client,{'file.json':b'updated'},{},receipts=self.receipts(['file.json']),continue_on_error=True)
            self.assertEqual(result['errors'][0]['code'],'R2_UPLOAD_OUTCOME_UNCONFIRMED')
        self.assertEqual(sum(x[0]=='PUT' for x in self.calls),1)
        self.assertEqual(self.receipts(['file.json']).get('file.json')['state'],'OUTCOME_UNKNOWN')

    def test_peer_closed_idle_socket_does_not_replay_a_put(self):
        self.objects[self.path('file.json')]=b'old';self.close_after_get=True
        self.assertEqual(self.client.get('file.json'),b'old')
        with self.assertRaises(urllib.error.URLError):self.client.put('file.json',b'updated')
        self.assertLessEqual(sum(x[0]=='PUT' for x in self.calls),1)
        self.assertEqual(self.pool.snapshot()['connection_attempts'],1)

    def test_redirect_is_rejected_without_following_or_leaking_credentials(self):
        self.redirect=True
        with self.assertRaises(publisher.ProviderError) as result:self.client.get('file.json')
        self.assertEqual(result.exception.status,302);self.assertEqual(len(self.calls),1)
        self.assertNotIn('private-test-token',str(result.exception))

    def test_untrusted_certificate_is_rejected_before_any_http_request(self):
        pool=network.KeepAliveHTTPS();self.addCleanup(pool.close)
        request=urllib.request.Request('https://api.cloudflare.com/client/v4/test')
        with self.assertRaises(urllib.error.URLError) as result:pool.open(request,timeout=3)
        self.assertIsInstance(result.exception.reason,ssl.SSLCertVerificationError)
        self.assertEqual(self.calls,[]);self.assertEqual(pool.snapshot()['in_flight'],0)

    def test_wrong_hostname_is_rejected_even_with_a_trusted_certificate(self):
        request=urllib.request.Request('https://wrong.invalid/client/v4/test')
        with patch.object(network,'API_HOST','wrong.invalid'):
            with self.assertRaises(urllib.error.URLError) as result:self.pool.open(request,timeout=3)
        self.assertIsInstance(result.exception.reason,ssl.SSLCertVerificationError)
        self.assertEqual(self.calls,[])

    def test_foreign_origins_and_nonverified_contexts_are_rejected(self):
        for url in ['http://api.cloudflare.com/test','https://unowned.invalid/test',
                    'https://user@api.cloudflare.com/test','https://api.cloudflare.com:444/test']:
            with self.assertRaises(ValueError):self.pool.open(urllib.request.Request(url),timeout=3)
        with self.assertRaises(ValueError):network.KeepAliveHTTPS(context=ssl._create_unverified_context())
        self.assertEqual(self.calls,[])

    def test_unread_responses_hold_two_leases_and_a_third_request_times_out(self):
        self.objects[self.path('file.json')]=b'public'
        request=urllib.request.Request('https://api.cloudflare.com'+self.path('file.json'))
        first=self.pool.open(request,timeout=2);second=self.pool.open(request,timeout=2)
        try:
            with self.assertRaises(TimeoutError):self.pool.open(request,timeout=.02)
            self.assertEqual(len(self.calls),2)
        finally:
            first.close();second.close()
        self.assertEqual(self.pool.snapshot()['in_flight'],0)

    def test_dns_errno_diagnostic_budget_and_not_sent_state_are_preserved(self):
        self.pool.servers=['127.0.0.1','127.0.0.2','127.0.0.3']
        self.pool.resolver_fingerprint='b'*64
        def unavailable(*args,**kwargs):raise socket.gaierror(socket.EAI_AGAIN,'private DNS error')
        names=['file.json'];seed={'file.json':hashlib.sha256(b'old').hexdigest()}
        with patch.object(network.socket,'getaddrinfo',side_effect=unavailable),patch.object(network,'dns_probe',return_value={
                'code':'DNS_REPLY','rcode':2,'answers':0,'elapsed_seconds':.001,'private':'private value'}) as probes:
            result=publisher.publish(self.client,{'file.json':b'new'},{},receipts=self.receipts(names,seed),continue_on_error=True)
            self.assertEqual(probes.call_count,3)
            self.pool.diagnose_dns(time.monotonic()+2);self.assertEqual(probes.call_count,3)
        self.assertEqual(result['errors'][0]['code'],'R2_REQUEST_NOT_SENT')
        self.assertEqual(self.receipts(names).get('file.json')['state'],'NOT_SENT')
        events=(Path(self.directory.name)/'events.jsonl').read_text()
        self.assertNotIn('private',events)
        failed=json.loads(events.splitlines()[0])
        self.assertEqual((failed['gai_errno'],failed['gai_name']),(socket.EAI_AGAIN,'EAI_AGAIN'))
        self.assertEqual(failed['resolver_config_sha256'],'b'*64)
        self.assertEqual([x['rcode'] for x in failed['dns_probes']],[2,2,2])
        self.assertEqual(self.pool.snapshot()['in_flight'],0)

    def rows4(self):
        return self.original_resolve('127.0.0.1',self.server.server_port,socket.AF_INET,socket.SOCK_STREAM)

    def rows6(self,port):
        return self.original_resolve('::1',port,socket.AF_INET6,socket.SOCK_STREAM)

    def test_no_ipv6_record_falls_back_to_ipv4(self):
        self.objects[self.path('file.json')]=b'public'
        with patch.object(network.socket,'getaddrinfo',return_value=self.rows4()):
            self.assertEqual(self.client.get('file.json'),b'public')
        stats=self.pool.snapshot()
        self.assertEqual((stats['ipv6_connect_attempts'],stats['ipv4_connections'],stats['ipv4_fallbacks']),(0,1,1))

    def test_ipv6_connect_refused_falls_back_before_the_single_put(self):
        with socket.socket(socket.AF_INET6,socket.SOCK_STREAM) as unused:
            unused.bind(('::1',0));closed_port=unused.getsockname()[1]
        rows=self.rows6(closed_port)+self.rows4()
        with patch.object(network.socket,'getaddrinfo',return_value=rows):
            result=publisher.publish(self.client,{'file.json':b'new'},{},receipts=self.receipts(['file.json']))
        self.assertEqual(result['uploaded'],1)
        self.assertEqual(sum(x[0]=='PUT' for x in self.calls),1)
        self.assertTrue(all(x[3]==socket.AF_INET for x in self.calls))
        # Cold GET returns 404 and closes its connection; the PUT needs a second one.
        self.assertEqual(self.pool.snapshot()['ipv4_fallbacks'],2)

    def test_ipv6_tls_stall_uses_one_shared_short_budget_then_ipv4(self):
        stop=threading.Event();accepted=[]
        with socket.socket(socket.AF_INET6,socket.SOCK_STREAM) as listener:
            listener.bind(('::1',0));listener.listen(1);listener.settimeout(2)
            def stall():
                try:
                    peer,_=listener.accept();accepted.append(peer)
                    stop.wait(2)
                except OSError:pass
            thread=threading.Thread(target=stall,daemon=True);thread.start()
            rows=self.rows6(listener.getsockname()[1])*4+self.rows4()
            self.objects[self.path('file.json')]=b'public';started=time.monotonic()
            try:
                with patch.object(network,'IPV6_CONNECT_BUDGET',.15),patch.object(network.socket,'getaddrinfo',return_value=rows):
                    self.assertEqual(self.client.get('file.json'),b'public')
                elapsed=time.monotonic()-started
            finally:
                stop.set();thread.join(timeout=2)
                for peer in accepted:peer.close()
        self.assertLess(elapsed,.8)
        self.assertEqual(self.pool.snapshot()['ipv6_connect_attempts'],1)
        self.assertEqual(self.pool.snapshot()['ipv4_connections'],1)

    def test_both_families_fail_before_http_is_not_sent_and_later_cycle_recovers(self):
        with socket.socket(socket.AF_INET6,socket.SOCK_STREAM) as a,socket.socket(socket.AF_INET,socket.SOCK_STREAM) as b:
            a.bind(('::1',0));b.bind(('127.0.0.1',0))
            rows=self.rows6(a.getsockname()[1])+self.original_resolve('127.0.0.1',b.getsockname()[1],socket.AF_INET,socket.SOCK_STREAM)
        names=['file.json'];seed={'file.json':hashlib.sha256(b'old').hexdigest()}
        with patch.object(network.socket,'getaddrinfo',return_value=rows):
            result=publisher.publish(self.client,{'file.json':b'new'},{},receipts=self.receipts(names,seed),continue_on_error=True)
        self.assertEqual(result['errors'][0]['code'],'R2_REQUEST_NOT_SENT')
        self.assertEqual(self.receipts(names).get('file.json')['state'],'NOT_SENT')
        self.assertEqual(self.calls,[])
        result=publisher.publish(self.client,{'file.json':b'new'},{},receipts=self.receipts(names))
        self.assertEqual(result['uploaded'],1)
        self.assertEqual(sum(x[0]=='PUT' for x in self.calls),1)


class ResolverConfigurationTests(unittest.TestCase):
    def test_dns_probe_reads_a_real_udp_reply_without_exposing_the_server(self):
        with socket.socket(socket.AF_INET,socket.SOCK_DGRAM) as server:
            server.bind(('127.0.0.1',0));server.settimeout(2)
            def reply():
                raw,peer=server.recvfrom(512)
                server.sendto(raw[:2]+struct.pack('!5H',0x8182,1,0,0,0)+raw[12:],peer)
            thread=threading.Thread(target=reply,daemon=True);thread.start()
            result=network.dns_probe('127.0.0.1',timeout=1,port=server.getsockname()[1])
            thread.join(timeout=2)
        self.assertEqual((result['code'],result['rcode'],result['answers']),('DNS_REPLY',2,0))
        self.assertNotIn('127.0.0.1',json.dumps(result))

    def test_renderer_keeps_primary_and_adds_ipv4_backups_without_private_literals(self):
        raw=resolver_renderer.render(b'# managed\nnameserver 192.0.2.53\nnameserver 2001:db8::53\n')
        self.assertIn(b'nameserver 192.0.2.53\nnameserver 1.1.1.1\nnameserver 8.8.8.8\n',raw)
        self.assertIn(b'options timeout:1 attempts:2\n',raw)
        self.assertNotIn(b'2001:db8',raw)

    def test_renderer_deduplicates_and_does_not_silently_drop_search_domains(self):
        raw=resolver_renderer.render(b'nameserver 1.1.1.1\n')
        self.assertEqual(raw.count(b'nameserver 1.1.1.1'),1)
        with self.assertRaises(ValueError):resolver_renderer.render(b'search internal.example\n')
        with self.assertRaises(ValueError):resolver_renderer.render(b'nameserver arbitrary.invalid\n')


if __name__=='__main__':unittest.main()
