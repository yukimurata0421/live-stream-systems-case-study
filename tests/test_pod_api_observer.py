"""Exercise TLS/API reads, UNIX sockets and token replacement with isolated fixtures."""
import base64
from contextlib import contextmanager
import copy
from functools import partial
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import io
import os
from pathlib import Path
import socket
import socketserver
import ssl
import subprocess
import sys
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "ops/selfheal"))
import pod_api_observer as observer
import pod_api_health as health


def pod_list():
    return {"kind": "PodList", "items": [{"metadata": {"namespace": "stream-v3", "name": "runtime",
        "uid": "pod-1", "resourceVersion": "10", "labels": {
            "app.kubernetes.io/name": "stream-v3", "app.kubernetes.io/component": "runtime"}},
        "spec": {"nodeName": "test-node", "containers": [{"env": [{"value": "DO_NOT_EXPORT"}]}]},
        "status": {"containerStatuses": [{"name": "stream-engine", "containerID": "containerd://container-1",
            "ready": True, "state": {"running": {"startedAt": "2026-11-02T00:00:00Z"}}}]}}]}


def token(iat):
    payload = base64.urlsafe_b64encode(json.dumps({"iat": iat, "exp": iat + 600}).encode()).decode().rstrip("=")
    return "fixture." + payload + ".not-a-real-signature"


@contextmanager
def broker(config, fetcher=observer.bounded_fetch):
    with observer.Server(config, fetcher) as server:
        thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.02}, daemon=True)
        thread.start()
        try:
            yield server
        finally:
            server.shutdown()
            thread.join(timeout=5)


class ObserverTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory(prefix="cra-pod-api-test-")
        cls.root = Path(cls.temp.name)
        cls.cert = cls.root / "cert.pem"
        cls.key = cls.root / "key.pem"
        subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
            "-keyout", str(cls.key), "-out", str(cls.cert), "-subj", "/CN=127.0.0.1",
            "-addext", "subjectAltName=IP:127.0.0.1"], check=True, capture_output=True)

    @classmethod
    def tearDownClass(cls):
        cls.temp.cleanup()

    def setUp(self):
        self.local = tempfile.TemporaryDirectory(dir=self.root)
        self.path = Path(self.local.name)
        self.token_path = self.path / "token"
        self.token_path.write_text(token(100))
        self.state = {"token": token(100), "response": pod_list(), "code": 200, "requests": 0, "delay": 0}
        state = self.state

        class API(BaseHTTPRequestHandler):
            def do_GET(self):
                state["requests"] += 1
                code = state["code"]
                if self.headers.get("Authorization") != "Bearer " + state["token"]:
                    code = 401
                if not self.path.startswith("/api/v1/namespaces/stream-v3/pods?"):
                    code = 404
                time.sleep(state["delay"])
                self.send_response(code)
                if code == 302:
                    self.send_header("Location", "/should-not-follow")
                self.end_headers()
                try:
                    self.wfile.write(json.dumps(state["response"]).encode())
                except (BrokenPipeError, ssl.SSLError):
                    pass

            def log_message(self, *args):
                pass

        self.api = ThreadingHTTPServer(("127.0.0.1", 0), API)
        tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        tls.load_cert_chain(self.cert, self.key)
        self.api.socket = tls.wrap_socket(self.api.socket, server_side=True)
        self.thread = threading.Thread(target=self.api.serve_forever, daemon=True)
        self.thread.start()
        self.config = SimpleNamespace(socket=str(self.path / "observer.sock"), client_uid=os.getuid(),
            api_url="https://127.0.0.1:" + str(self.api.server_port), token=str(self.token_path),
            ca=str(self.cert), node_name="test-node")
        self.read = partial(observer.exchange, self.config.socket, expected_uid=os.getuid())

    def tearDown(self):
        self.api.shutdown()
        self.api.server_close()
        self.thread.join(timeout=5)
        self.local.cleanup()

    def test_rotated_token_is_reread_and_pod_secrets_are_not_exported(self):
        with broker(self.config):
            first = self.read()
            self.assertEqual(first["pods"]["items"][0]["metadata"]["uid"], "pod-1")
            self.assertNotIn("DO_NOT_EXPORT", json.dumps(first))
            newer = self.path / "new-token"
            newer.write_text(token(700))
            newer.replace(self.token_path)
            self.state["token"] = token(700)
            self.read()
            status = self.read(operation="status")["status"]
            self.assertEqual(status["successful_reads"], 2)
            self.assertEqual(status["token"], {"issued_at": 700, "expires_at": 1300})

    def test_unauthorized_api_has_no_cached_success_and_recovers(self):
        with broker(self.config):
            self.read()
            self.state["code"] = 401
            with self.assertRaisesRegex(ValueError, "UNAVAILABLE"):
                self.read()
            self.assertEqual(self.read(operation="status")["status"]["failed_reads"], 1)
            failure=self.read(operation='status')['status']['last_failure']
            self.assertEqual(failure['reason'],'POD_API_HTTP_401')
            self.assertEqual(failure['stage'],'http_request')
            self.assertGreater(failure['duration_seconds'],0)
            self.assertNotIn(token(100),json.dumps(failure))
            self.state["code"] = 200
            self.read()
            self.assertEqual(self.read(operation='status')['status']['last_failure'],failure)

    def test_redirect_is_not_followed(self):
        self.state["code"] = 302
        with broker(self.config):
            with self.assertRaisesRegex(ValueError, "UNAVAILABLE"):
                self.read()
        self.assertEqual(self.state["requests"], 1)

    def test_untrusted_tls_certificate_fails(self):
        self.config.api_url = self.config.api_url.replace("127.0.0.1", "localhost")
        with broker(self.config):
            with self.assertRaisesRegex(ValueError, "UNAVAILABLE"):
                self.read()
            self.assertEqual(self.read(operation='status')['status']['last_failure']['reason'],'POD_API_TLS_VERIFY_FAILED')

    def test_missing_token_fails_without_leaking_exception(self):
        self.token_path.unlink()
        with broker(self.config):
            with self.assertRaisesRegex(ValueError, "^POD_API_OBSERVATION_UNAVAILABLE$"):
                self.read()

    def test_wrong_target_is_rejected(self):
        self.state["response"]["items"][0]["spec"]["nodeName"] = "other-node"
        with broker(self.config):
            with self.assertRaisesRegex(ValueError, "UNAVAILABLE"):
                self.read()

    def test_no_arbitrary_query_or_effect_operation(self):
        with broker(self.config):
            for operation in ("delete", "exec", "/api/v1/secrets"):
                with self.assertRaisesRegex(ValueError, "UNAVAILABLE"):
                    self.read(operation=operation)
        self.assertEqual(self.state["requests"], 0)

    def test_peer_uid_is_checked(self):
        with broker(self.config):
            with self.assertRaisesRegex(ValueError, "PEER_MISMATCH"):
                observer.exchange(self.config.socket, expected_uid=os.getuid() + 1)

    def test_stopped_broker_is_unknown_then_restart_recovers(self):
        with broker(self.config):
            self.read()
        with self.assertRaises(OSError):
            self.read()
        with broker(self.config):
            self.read()

    def test_exclusive_socket_writer(self):
        with broker(self.config):
            with self.assertRaises(BlockingIOError):
                observer.Server(self.config)
            self.read()

    def test_timeout_is_bounded(self):
        self.state["delay"] = 7
        with broker(self.config):
            start = time.monotonic()
            with self.assertRaisesRegex(ValueError, "UNAVAILABLE"):
                self.read()
            self.assertLess(time.monotonic() - start, 5)
            self.assertEqual(self.read(operation='status')['status']['last_failure']['reason'],'POD_API_TIMEOUT')

    def test_light_health_checks_actual_socket_and_does_not_fetch_api(self):
        with broker(self.config):
            health.check(self.config.socket,os.getuid())
            command=[sys.executable,str(Path(health.__file__)),self.config.socket,str(os.getuid())]
            result=subprocess.run(command,capture_output=True,timeout=6)
            self.assertEqual(result.returncode,0,result.stderr)
            self.assertEqual(self.state['requests'],0)
            self.state['code']=503
            with self.assertRaises(ValueError):self.read()
            health.check(self.config.socket,os.getuid())
            self.assertEqual(self.read(operation='status')['status']['last_failure']['reason'],'POD_API_HTTP_503')

    def test_light_health_rejects_wrong_peer_and_stopped_broker(self):
        with broker(self.config):
            with self.assertRaisesRegex(ValueError,'PEER_MISMATCH'):
                health.check(self.config.socket,os.getuid()+1)
        with self.assertRaises(OSError):health.check(self.config.socket,os.getuid())

    def test_light_health_imports_no_api_or_tls_modules(self):
        code='import sys; sys.path.insert(0,'+repr(str(Path(health.__file__).parent))+'); import pod_api_health; assert "ssl" not in sys.modules and "urllib.request" not in sys.modules'
        subprocess.run([sys.executable,'-B','-c',code],check=True,capture_output=True,timeout=5)

    def test_light_health_rejects_malformed_oversized_and_unbound_responses(self):
        for case in ['wrong_nonce','wrong_schema','missing_status','oversize','no_reply']:
            with self.subTest(case=case):
                path=str(self.path/('bad-'+case+'.sock'))
                class BadReply(socketserver.BaseRequestHandler):
                    def handle(inner):
                        request=observer.read_line(inner.request,512,1)
                        row={'schema':observer.SCHEMA,'request_id':request['request_id'],'status':{}}
                        if case=='wrong_nonce':row['request_id']='0'*32
                        if case=='wrong_schema':row['schema']='other'
                        if case=='missing_status':row.pop('status')
                        if case=='no_reply':time.sleep(.15);return
                        try:inner.request.sendall(b'x'*65537 if case=='oversize' else observer.encode(row))
                        except OSError:pass
                with socketserver.UnixStreamServer(path,BadReply) as server:
                    thread=threading.Thread(target=server.handle_request,daemon=True);thread.start()
                    with self.assertRaises((OSError,ValueError)):
                        health.check(path,os.getuid(),timeout=.1 if case=='no_reply' else 1)
                    thread.join(timeout=2)

    def test_diagnostic_parser_rejects_free_text_and_invalid_values(self):
        source=b'private exception text\n'+observer.encode({'stage':'secret-like','elapsed_seconds':1,'cpu_seconds':1})
        source+=observer.encode({'stage':'http_request','elapsed_seconds':-1,'cpu_seconds':1})
        source+=observer.encode({'stage':'token_read','elapsed_seconds':0.25,'cpu_seconds':0.1,'token':'DO_NOT_EXPORT'})
        value=observer.diagnostics(source,time.monotonic()-0.5)
        self.assertEqual(value['stage'],'token_read')
        self.assertNotIn('DO_NOT_EXPORT',json.dumps(value))
        self.assertEqual(observer.safe_reason('arbitrary private exception'),'POD_API_FETCH_FAILED')
        self.assertEqual(observer.safe_reason({'secret':'value'}),'POD_API_FETCH_FAILED')

    def test_child_deadline_keeps_last_stage_without_exception_text(self):
        error=subprocess.TimeoutExpired(['not-executed'],4,stderr=observer.encode({'stage':'startup','elapsed_seconds':0,'cpu_seconds':0}))
        with patch.object(observer.subprocess,'run',side_effect=error):
            with self.assertRaises(observer.ObservationError) as caught:observer.bounded_fetch(self.config)
        self.assertEqual(str(caught.exception),'POD_API_TIMEOUT')
        self.assertEqual(caught.exception.details['stage'],'startup')

    def test_fetch_cli_distinguishes_missing_files_from_connection_reset(self):
        for error,expected in [(FileNotFoundError('PRIVATE'),'POD_API_FILE_UNAVAILABLE'),
                               (PermissionError('PRIVATE'),'POD_API_FILE_UNAVAILABLE'),
                               (ConnectionResetError('PRIVATE'),'POD_API_TRANSPORT_ERROR'),
                               (TimeoutError('PRIVATE'),'POD_API_TIMEOUT')]:
            with self.subTest(expected=expected),patch.object(sys,'argv',['observer','fetch']),\
                 patch.object(observer,'fetch',side_effect=error),\
                 patch.object(sys,'stdout',SimpleNamespace(buffer=io.BytesIO())) as output:
                observer.main()
                value=json.loads(output.buffer.getvalue())
                self.assertEqual(value['error'],expected)
                self.assertNotIn('PRIVATE',json.dumps(value))

    def test_socket_capacity_rejection_is_recorded_and_bounded(self):
        with broker(self.config) as server:
            for _ in range(observer.MAX_CONNECTIONS):server.capacity.acquire()
            try:
                with self.assertRaises((OSError,ValueError)):self.read()
            finally:
                for _ in range(observer.MAX_CONNECTIONS):server.capacity.release()
            status=self.read(operation='status')['status']
            self.assertEqual(status['busy_rejections'],1)
            self.assertEqual(status['last_failure']['reason'],'POD_API_BUSY')

    def test_probes_remain_available_during_fetch_and_second_fetch_is_rejected(self):
        entered=threading.Event();release=threading.Event();results=[]
        def fetcher(config):
            entered.set();release.wait(3)
            return {'pods':observer.project_pods(pod_list(),'test-node'),'token':{'issued_at':100,'expires_at':700}}
        with broker(self.config,fetcher):
            thread=threading.Thread(target=lambda:results.append(self.read()),daemon=True);thread.start()
            self.assertTrue(entered.wait(1))
            try:
                # A second API request cannot consume another child process.
                with self.assertRaisesRegex(ValueError,'UNAVAILABLE'):self.read()
                checks=[]
                def check():health.check(self.config.socket,os.getuid());checks.append(True)
                probes=[threading.Thread(target=check) for _ in range(2)]
                for p in probes:p.start()
                for p in probes:p.join(timeout=2)
                self.assertEqual(checks,[True,True])
                status=self.read(operation='status')['status']
                self.assertEqual(status['busy_rejections'],1)
                self.assertEqual(status['last_failure']['stage'],'api_admission')
            finally:release.set();thread.join(timeout=2)
            self.assertEqual(len(results),1)

    def test_projector_preserves_cardinality_for_consumer(self):
        value = pod_list()
        value["items"].append(copy.deepcopy(value["items"][0]))
        self.assertEqual(len(observer.project_pods(value, "test-node")["items"]), 2)
        self.assertEqual(observer.project_pods({"kind": "PodList", "items": []}, "test-node")["items"], [])

    def test_deployed_collector_preserves_two_reads_and_rejects_drift_and_missing_api(self):
        try:
            from dell_recovery_agent import target_snapshot
        except ImportError:
            self.skipTest("set PYTHONPATH to the pinned deployed collector for integration")
        import pod_api_target_snapshot as adapter
        original = target_snapshot.AtomicKubernetesTargetSnapshotProducer
        module = adapter.install()
        self.addCleanup(setattr, target_snapshot, "AtomicKubernetesTargetSnapshotProducer", original)
        proc = self.path / "proc"
        process = proc / "42"
        process.mkdir(parents=True)
        (process / "cgroup").write_text("0::/kubepods/container-1")
        (process / "cmdline").write_bytes(b"ffmpeg\0-f\0fifo\0-fifo_format\0flv\0rtmps://example.invalid/test\0")
        (process / "stat").write_text("42 (ffmpeg) " + " ".join(["S"] + ["0"] * 18 + ["12345"]))
        producer = module.AtomicKubernetesTargetSnapshotProducer(host_id="fixture", namespace=observer.NAMESPACE,
            label_selector=observer.SELECTOR, proc_root=proc)
        changes = []

        def fetcher(config):
            result = observer.bounded_fetch(config)
            if changes:
                result["pods"]["items"][0]["metadata"]["resourceVersion"] = str(changes.pop(0))
            return result

        with patch.dict(os.environ, {"CRA_POD_API_SOCKET": self.config.socket}), patch.object(
                adapter, "exchange", partial(observer.exchange, expected_uid=os.getuid())):
            with broker(self.config, fetcher):
                good = producer.collect()
                self.assertEqual(good["status"], "VALID", good)
                self.assertEqual(good["runtime_status"], "VALID")
                self.assertEqual(good["target_identity"]["ffmpeg_pid"], 42)
                self.assertEqual(self.state["requests"], 2)
                changes[:] = ["11", "12"]
                drift = producer.collect()
                self.assertEqual(drift["status"], "INVALID")
                self.assertEqual(drift["reason_code"], "SNAPSHOT_UNSTABLE")
                self.state["code"] = 401
                self.assertEqual(producer.collect()["status"], "INVALID")
            absent = producer.collect()
            self.assertEqual(absent["status"], "INVALID")
            self.assertIsNone(absent["target_identity"])


if __name__ == "__main__":
    unittest.main()
