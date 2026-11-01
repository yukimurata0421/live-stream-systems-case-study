"""Bounded GET-only transport for private thumbnail facts and publications."""
import json
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlsplit
from urllib.error import HTTPError
from urllib.request import HTTPHandler, HTTPDefaultErrorHandler, HTTPErrorProcessor, OpenerDirector


def http_opener():
    opener = OpenerDirector()
    for handler in (HTTPHandler(), HTTPDefaultErrorHandler(), HTTPErrorProcessor()):
        opener.add_handler(handler)
    return opener


def read_json(url, *, opener, limit=65536, timeout=2):
    parsed = urlsplit(url)
    if (parsed.scheme != 'http' or not parsed.hostname or parsed.username or parsed.password
            or parsed.query or parsed.fragment):
        raise ValueError('invalid_private_facts_endpoint')
    deadline = time.monotonic() + timeout
    try:
        with opener.open(url, timeout=timeout) as response:
            raw = bytearray()
            while len(raw) <= limit:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError('facts_deadline')
                transport = getattr(getattr(getattr(response, 'fp', None), 'raw', None), '_sock', None)
                if transport is not None:
                    transport.settimeout(remaining)
                chunk = getattr(response, 'read1', response.read)(min(65536, limit + 1 - len(raw)))
                if not chunk:
                    break
                raw.extend(chunk)
                if getattr(response, 'isclosed', lambda: False)():
                    break
    except HTTPError as error:
        error.close()
        raise
    if len(raw) > limit:
        raise ValueError('facts_response_too_large')
    value = json.loads(raw)
    if not isinstance(value, dict):
        raise ValueError('facts_response_invalid')
    return value


def publication_server(address, worker):
    """Health measures loop progress, including UNKNOWN results, independently of UTC."""
    class Handler(BaseHTTPRequestHandler):
        def setup(self):
            super().setup()
            self.connection.settimeout(2)

        def do_GET(self):
            status = 200
            if self.path == '/healthz':
                status = 200 if time.monotonic() - worker.last_cycle_monotonic <= 180 else 503
                value = {'loop_responsive': status == 200}
            elif self.path == '/observation':
                value = worker.previous
            else:
                status, value = 404, {'error': 'not_found'}
            raw = json.dumps(value, allow_nan=False).encode()
            self.send_response(status)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Cache-Control', 'no-store')
            self.send_header('Content-Length', str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def log_message(self, *_):
            pass

    server = HTTPServer(address, Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server
