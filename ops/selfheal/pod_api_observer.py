"""Expose fixed Pod API observations through a restricted UNIX socket."""
from __future__ import annotations

import sys
import time
PROCESS_STARTED = time.monotonic()
PROCESS_CPU_STARTED = time.process_time()
if __name__ == '__main__' and sys.argv[1:2] == ['fetch']:
    # A timeout during imports must remain distinguishable from an API timeout.
    sys.stderr.write('{"stage":"startup","elapsed_seconds":0,"cpu_seconds":0}\n')
    sys.stderr.flush()

import argparse
import base64
import fcntl
import hashlib
import json
import os
from pathlib import Path
import socket
import socketserver
import ssl
import stat
import struct
import subprocess
import threading
import urllib.error
import urllib.parse
import urllib.request
import uuid

NAMESPACE = "stream-v3"
SELECTOR = "app.kubernetes.io/name=stream-v3,app.kubernetes.io/component=runtime"
SCHEMA = "stream_v3.pod_api_observation.v1"
MAX_API_BYTES = 1024 * 1024
MAX_REPLY_BYTES = 65536
SOCKET = "/run/stream-v3-pod-api-observer/observer.sock"
TOKEN = "/var/run/cra-observer/token"
CA = "/var/run/cra-observer/ca.crt"
FETCH_TIMEOUT = 4.0
MAX_CONNECTIONS = 4  # one fetch, two probes and one read-only diagnostic
STAGES = frozenset(('startup','imports_complete','token_read','tls_setup','http_request',
                    'http_body','projection','complete','child_spawn','child_output',
                    'socket_request','socket_admission','api_admission'))
ERRORS = frozenset(('POD_API_TIMEOUT','POD_API_CHILD_START_FAILED','POD_API_FETCH_FAILED',
    'POD_API_INVALID_RESPONSE','POD_API_RESPONSE_TOO_LARGE','POD_API_TARGET_MISMATCH',
    'TARGET_POD_CARDINALITY','POD_API_CONFIG_INVALID','POD_API_FILE_UNAVAILABLE',
    'POD_API_TLS_ERROR','POD_API_TLS_VERIFY_FAILED','POD_API_TRANSPORT_ERROR',
    'POD_API_INVALID_TOKEN','POD_API_REQUEST_REJECTED','POD_API_INCOMPLETE_RESPONSE','POD_API_BUSY'))


def safe_reason(value):
    if not isinstance(value,str):return 'POD_API_FETCH_FAILED'
    if value in ERRORS:return value
    if isinstance(value,str) and value.startswith('POD_API_HTTP_'):
        number=value.removeprefix('POD_API_HTTP_')
        if len(number)==3 and number.isascii() and number.isdigit() and 100<=int(number)<=599:return value
    return 'POD_API_FETCH_FAILED'


def progress(stage):
    if __name__=='__main__' and sys.argv[1:2]==['fetch']:
        sys.stderr.buffer.write(encode({'stage':stage,'elapsed_seconds':round(time.monotonic()-PROCESS_STARTED,6),
            'cpu_seconds':round(time.process_time()-PROCESS_CPU_STARTED,6)}))
        sys.stderr.buffer.flush()


def diagnostics(stderr, started):
    stages={};last='child_spawn'
    for line in (stderr or b'')[:8192].splitlines():
        try:
            row=json.loads(line)
            if not isinstance(row,dict) or row.get('stage') not in STAGES:continue
            elapsed=float(row['elapsed_seconds']);cpu=float(row['cpu_seconds'])
            if not 0<=elapsed<=60 or not 0<=cpu<=60:continue
            last=row['stage'];stages[last]={'elapsed_seconds':elapsed,'cpu_seconds':cpu}
        except (ValueError,KeyError,TypeError):continue
    return {'stage':last,'duration_seconds':round(time.monotonic()-started,6),'stages':stages}


class ObservationError(ValueError):
    def __init__(self, reason, details):
        super().__init__(safe_reason(reason))
        self.details=details


def encode(value):
    return json.dumps(value, separators=(",", ":"), sort_keys=True).encode() + b"\n"


def token_metadata(token):
    # Only diagnostics, never authorization or JWT verification.
    payload = token.split(".")[1]
    value = json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))
    return {"issued_at": int(value["iat"]), "expires_at": int(value["exp"])}


def project_pods(value, node_name):
    if not isinstance(value, dict) or value.get("kind") != "PodList" or not isinstance(value.get("items"), list):
        raise ValueError("POD_API_INVALID_RESPONSE")
    if len(value["items"]) > 16:
        raise ValueError("TARGET_POD_CARDINALITY")
    items = []
    for pod in value["items"]:
        meta = pod["metadata"]
        labels = meta.get("labels", {})
        if (meta["namespace"] != NAMESPACE or pod["spec"]["nodeName"] != node_name
                or labels.get("app.kubernetes.io/name") != "stream-v3"
                or labels.get("app.kubernetes.io/component") != "runtime"):
            raise ValueError("POD_API_TARGET_MISMATCH")
        statuses = []
        for row in pod.get("status", {}).get("containerStatuses", []):
            if row.get("name") == "stream-engine":
                running = row.get("state", {}).get("running", {})
                statuses.append({"name": "stream-engine", "containerID": row.get("containerID", ""),
                                 "ready": row.get("ready") is True,
                                 "state": {"running": {"startedAt": running.get("startedAt", "")}}})
        items.append({"metadata": {k: meta[k] for k in ("namespace", "name", "uid", "resourceVersion")},
                      "status": {"containerStatuses": statuses}})
    return {"kind": "PodList", "items": items}


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


def fetch(api_url, token_path, ca_path, node_name):
    if urllib.parse.urlsplit(api_url).scheme != "https" or not node_name:
        raise ValueError("POD_API_CONFIG_INVALID")
    # Read the projected file anew for every API request, including after rotation.
    progress('token_read')
    token = Path(token_path).read_text().strip()
    try:metadata = token_metadata(token)
    except (ValueError,KeyError,IndexError,TypeError):raise ValueError('POD_API_INVALID_TOKEN') from None
    query = urllib.parse.urlencode({"labelSelector": SELECTOR, "fieldSelector": "spec.nodeName=" + node_name})
    url = api_url.rstrip("/") + "/api/v1/namespaces/" + NAMESPACE + "/pods?" + query
    progress('tls_setup')
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect(),
        urllib.request.HTTPSHandler(context=ssl.create_default_context(cafile=ca_path)))
    request = urllib.request.Request(url, headers={"Authorization": "Bearer " + token, "Accept": "application/json"})
    progress('http_request')
    with opener.open(request, timeout=3) as response:
        progress('http_body')
        data = response.read(MAX_API_BYTES + 1)
        if len(data) > MAX_API_BYTES:
            raise ValueError("POD_API_RESPONSE_TOO_LARGE")
    progress('projection')
    result={"pods": project_pods(json.loads(data), node_name), "token": metadata}
    progress('complete')
    return result


def bounded_fetch(config):
    # A child deadline also bounds slow headers/body and TLS stalls. It has no effect API.
    command = [sys.executable, str(Path(__file__).resolve()), "fetch", "--api-url", config.api_url,
               "--token", config.token, "--ca", config.ca, "--node-name", config.node_name]
    started=time.monotonic()
    try:
        result = subprocess.run(command, capture_output=True, timeout=FETCH_TIMEOUT, check=False)
    except subprocess.TimeoutExpired as exc:
        raise ObservationError('POD_API_TIMEOUT',diagnostics(exc.stderr,started)) from None
    except OSError:
        raise ObservationError('POD_API_CHILD_START_FAILED',diagnostics(None,started)) from None
    details=diagnostics(result.stderr,started)
    if result.returncode or len(result.stdout) > MAX_REPLY_BYTES:
        raise ObservationError('POD_API_FETCH_FAILED',details)
    try:value = json.loads(result.stdout)
    except (ValueError,TypeError):raise ObservationError('POD_API_INVALID_RESPONSE',details) from None
    if not isinstance(value,dict):raise ObservationError('POD_API_INVALID_RESPONSE',details)
    if value.get("error"):
        raise ObservationError(safe_reason(value['error']),details)
    value['diagnostics']=details
    return value


def read_line(connection, limit, timeout):
    deadline = time.monotonic() + timeout
    data = b""
    while not data.endswith(b"\n"):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("POD_API_TIMEOUT")
        connection.settimeout(remaining)
        chunk = connection.recv(min(4096, limit + 1 - len(data)))
        if not chunk:
            raise ValueError("POD_API_INCOMPLETE_RESPONSE")
        data += chunk
        if len(data) > limit:
            raise ValueError("POD_API_RESPONSE_TOO_LARGE")
    return json.loads(data)


def peer_uid(connection):
    return struct.unpack("3i", connection.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, 12))[1]


def exchange(path=SOCKET, *, operation="pods", expected_uid=992, timeout=5.0):
    nonce = uuid.uuid4().hex
    with socket.socket(socket.AF_UNIX) as connection:
        connection.settimeout(timeout)
        connection.connect(str(path))
        if peer_uid(connection) != expected_uid:
            raise ValueError("POD_API_PEER_MISMATCH")
        connection.sendall(encode({"operation": operation, "request_id": nonce}))
        value = read_line(connection, MAX_REPLY_BYTES, timeout)
    if not isinstance(value, dict) or value.get("schema") != SCHEMA or value.get("request_id") != nonce:
        raise ValueError("POD_API_RESPONSE_BINDING_MISMATCH")
    if value.get("error"):
        raise ValueError(value["error"])
    return value


class Handler(socketserver.BaseRequestHandler):
    def handle(self):
        nonce = ""
        started=time.monotonic()
        try:
            if peer_uid(self.request) not in (0, self.server.config.client_uid):
                return
            request = read_line(self.request, 512, 1)
            if not isinstance(request, dict):
                raise ValueError("POD_API_REQUEST_REJECTED")
            nonce = request.get("request_id", "")
            if (set(request) != {"request_id", "operation"} or not isinstance(nonce, str)
                    or len(nonce) != 32 or any(c not in "0123456789abcdef" for c in nonce)):
                raise ValueError("POD_API_REQUEST_REJECTED")
            operation = request["operation"]
            if operation == "status":
                with self.server.state_lock:
                    value = {"status": {**self.server.state,'failure_counts':dict(self.server.state['failure_counts'])}}
            elif operation == "pods":
                if not self.server.fetch_capacity.acquire(blocking=False):
                    with self.server.state_lock:self.server.state['busy_rejections']+=1
                    raise ObservationError('POD_API_BUSY',{'stage':'api_admission','duration_seconds':0})
                try:result = self.server.fetcher(self.server.config)
                finally:self.server.fetch_capacity.release()
                with self.server.state_lock:
                    self.server.state["successful_reads"] += 1
                    self.server.state["token"] = result["token"]
                    self.server.state['last_request']=result.get('diagnostics')
                value = {"pods": result["pods"]}
            else:
                raise ValueError("POD_API_REQUEST_REJECTED")
        except (OSError, ValueError, KeyError, TypeError) as exc:
            with self.server.state_lock:
                self.server.state["failed_reads"] += 1
            details=exc.details if isinstance(exc,ObservationError) else {
                'stage':'socket_request','duration_seconds':round(time.monotonic()-started,6)}
            self.server.failure(safe_reason(str(exc)) if isinstance(exc,ValueError) else 'POD_API_TRANSPORT_ERROR',details)
            # Do not serialize exception text, API response bodies, or credentials.
            value = {"error": "POD_API_OBSERVATION_UNAVAILABLE"}
        value.update(schema=SCHEMA, request_id=nonce)
        try:
            self.request.settimeout(1)
            self.request.sendall(encode(value))
        except OSError:
            pass


class Server(socketserver.ThreadingMixIn, socketserver.UnixStreamServer):
    daemon_threads = True
    request_queue_size = 4

    def __init__(self, config, fetcher=bounded_fetch):
        self.config = config
        self.fetcher = fetcher
        self.state_lock = threading.Lock()
        self.capacity = threading.BoundedSemaphore(MAX_CONNECTIONS)
        self.fetch_capacity = threading.BoundedSemaphore(1)
        self.state = {"successful_reads": 0, "failed_reads": 0, "token": None,
                      'last_request':None,'last_failure':None,'failure_counts':{},'busy_rejections':0,
                      "release_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest()}
        path = Path(config.socket)
        self.lock = path.with_suffix(".lock").open("a")
        try:
            fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            if path.exists():
                if not stat.S_ISSOCK(path.lstat().st_mode):
                    raise ValueError("POD_API_SOCKET_PATH_UNSAFE")
                path.unlink()
            super().__init__(str(path), Handler)
            os.chmod(path, 0o660)
        except BaseException:
            self.lock.close()
            raise

    def failure(self, reason, details):
        row={'event':'pod_api_observation_failed','reason':safe_reason(reason),
             'observed_at_utc':time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime()),**details}
        with self.state_lock:
            self.state['last_failure']=row
            counts=self.state['failure_counts'];counts[row['reason']]=counts.get(row['reason'],0)+1
        # Only fixed codes and numeric timings; no exception text or HTTP/token data.
        sys.stdout.buffer.write(encode(row));sys.stdout.buffer.flush()

    def process_request(self, request, client_address):
        if not self.capacity.acquire(blocking=False):
            with self.state_lock:self.state['busy_rejections']+=1
            self.failure('POD_API_BUSY',{'stage':'socket_admission','duration_seconds':0})
            self.shutdown_request(request)
            return
        try:
            super().process_request(request, client_address)
        except BaseException:
            self.capacity.release()
            raise

    def process_request_thread(self, request, client_address):
        try:
            super().process_request_thread(request, client_address)
        finally:
            self.capacity.release()

    def server_close(self):
        super().server_close()
        self.lock.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=("serve", "fetch", "status", "pods"))
    parser.add_argument("--socket", default=SOCKET)
    parser.add_argument("--client-uid", type=int, default=992)
    parser.add_argument("--server-uid", type=int, default=992)
    host = os.environ.get("KUBERNETES_SERVICE_HOST", "kubernetes.default.svc")
    port = os.environ.get("KUBERNETES_SERVICE_PORT_HTTPS", "443")
    parser.add_argument("--api-url", default=f"https://{host}:{port}")
    parser.add_argument("--token", default=TOKEN)
    parser.add_argument("--ca", default=CA)
    parser.add_argument("--node-name", default=os.environ.get("NODE_NAME", ""))
    args = parser.parse_args()
    if args.mode == "serve":
        os.umask(0o007)
        with Server(args) as server:
            server.serve_forever(poll_interval=0.2)
    elif args.mode == "fetch":
        progress('imports_complete')
        try:
            value = fetch(args.api_url, args.token, args.ca, args.node_name)
        except urllib.error.HTTPError as exc:
            value = {"error": "POD_API_HTTP_" + str(exc.code)}
        except urllib.error.URLError as exc:
            code=('POD_API_TLS_VERIFY_FAILED' if isinstance(exc.reason,ssl.SSLCertVerificationError)
                  else 'POD_API_TLS_ERROR' if isinstance(exc.reason,ssl.SSLError)
                  else 'POD_API_TIMEOUT' if isinstance(exc.reason,TimeoutError) else 'POD_API_TRANSPORT_ERROR')
            value={'error':code}
        except ssl.SSLCertVerificationError:value={'error':'POD_API_TLS_VERIFY_FAILED'}
        except ssl.SSLError:value={'error':'POD_API_TLS_ERROR'}
        except TimeoutError:value={'error':'POD_API_TIMEOUT'}
        except OSError as exc:
            value={'error':'POD_API_FILE_UNAVAILABLE' if isinstance(exc,
                (FileNotFoundError,PermissionError,IsADirectoryError,NotADirectoryError)) else 'POD_API_TRANSPORT_ERROR'}
        except (ValueError,KeyError,TypeError) as exc:value={'error':safe_reason(str(exc))}
        except Exception:
            value = {"error": "POD_API_FETCH_FAILED"}
        sys.stdout.buffer.write(encode(value))
    else:
        sys.stdout.buffer.write(encode(exchange(args.socket, operation=args.mode, expected_uid=args.server_uid)))


if __name__ == "__main__":
    main()
