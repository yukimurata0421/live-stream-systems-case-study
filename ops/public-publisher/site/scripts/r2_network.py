"""Two bounded HTTPS connections to the fixed R2 API, without request retries."""
from __future__ import annotations

import hashlib
import http.client
import ipaddress
import queue
import secrets
import socket
import ssl
import struct
import threading
import time
import urllib.error
import urllib.parse
from pathlib import Path

API_HOST = "api.cloudflare.com"
IPV6_CONNECT_BUDGET = 1.0


def resolver_config(path=Path('/etc/resolv.conf')):
    """Keep addresses private; expose only an identity fingerprint and labels."""
    try:
        with path.open('rb') as handle:
            raw = handle.read(8193)
        if len(raw) > 8192:
            raise ValueError('resolver_config_too_large')
        servers = []
        for line in raw.decode().splitlines():
            parts = line.split('#', 1)[0].split()
            if len(parts) == 2 and parts[0] == 'nameserver' and len(servers) < 3:
                # Diagnostics use reachable IPv4 servers, matching the service.
                address = ipaddress.ip_address(parts[1])
                if address.version == 4:
                    servers.append(str(address))
        return hashlib.sha256(raw).hexdigest(), servers
    except (OSError, ValueError, UnicodeError):
        return 'unavailable', []


def dns_probe(server, *, timeout, port=53):
    """One fixed-name A query. No recursive lookups or arbitrary response text."""
    ident = secrets.randbits(16)
    question = b''.join(bytes([len(x)]) + x.encode() for x in API_HOST.split('.')) + b'\0'
    packet = struct.pack('!6H', ident, 0x100, 1, 0, 0, 0) + question + struct.pack('!HH', 1, 1)
    started = time.monotonic()
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.settimeout(timeout)
            sock.connect((server, port))
            sock.send(packet)
            raw = sock.recv(4096)
        actual, flags, questions, answers, _, _ = struct.unpack('!6H', raw[:12])
        if actual != ident or not flags & 0x8000 or questions != 1:
            raise ValueError('invalid_dns_response')
        value = {'code': 'DNS_REPLY', 'rcode': flags & 15, 'answers': answers,
                 'truncated': bool(flags & 0x200)}
    except (OSError, ValueError, struct.error) as exc:
        value = {'code': 'DNS_TIMEOUT' if isinstance(exc, TimeoutError) else 'DNS_PROBE_FAILED'}
        if type(getattr(exc, 'errno', None)) is int:
            value['errno'] = exc.errno
    return {**value, 'elapsed_seconds': round(time.monotonic() - started, 6)}


class PreferredHTTPSConnection(http.client.HTTPSConnection):
    def __init__(self, owner, *, context, timeout):
        super().__init__(API_HOST, timeout=timeout, context=context)
        self.owner = owner

    def connect(self):
        # HTTPConnection invokes connect before sending any HTTP header/body.
        try:
            self.connect_preferred()
        except OSError as exc:
            exc.http_request_not_sent = True
            raise

    def connect_preferred(self):
        end = time.monotonic() + self.timeout
        started = time.monotonic()
        self.owner.count('dns_lookups')
        try:
            rows = socket.getaddrinfo(self.host, self.port, socket.AF_UNSPEC, socket.SOCK_STREAM)
        except socket.gaierror as exc:
            exc.dns_elapsed_seconds = round(time.monotonic() - started, 6)
            self.owner.count('dns_seconds', time.monotonic() - started)
            exc.resolver_config_sha256 = self.owner.resolver_fingerprint
            self.owner.count('dns_failures')
            exc.dns_probes = self.owner.diagnose_dns(end)
            raise
        else:
            self.owner.count('dns_seconds', time.monotonic() - started)
        last = None
        # One shared IPv6 TCP+TLS budget, independent of the number of AAAAs.
        for family, label in ((socket.AF_INET6, 'ipv6'), (socket.AF_INET, 'ipv4')):
            candidates = [row for row in rows if row[0] == family]
            if label == 'ipv4' and candidates:
                self.owner.count('ipv4_fallbacks')
            family_end = min(end, time.monotonic() + IPV6_CONNECT_BUDGET) if label == 'ipv6' else end
            for _, kind, protocol, _, peer in candidates:
                remaining = family_end - time.monotonic()
                if remaining <= 0:
                    last = TimeoutError('connection_budget_exhausted')
                    break
                sock = secured = None
                self.owner.count(label + '_connect_attempts')
                try:
                    sock = socket.socket(family, kind, protocol)
                    sock.settimeout(remaining)
                    if self.source_address:
                        sock.bind(self.source_address)
                    sock.connect(peer)
                    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                    remaining = family_end - time.monotonic()
                    if remaining <= 0:
                        raise TimeoutError('connection_budget_exhausted')
                    sock.settimeout(remaining)
                    secured = self._context.wrap_socket(sock, server_hostname=self.host)
                    sock = None  # SSL socket now owns the connected descriptor.
                    remaining = family_end - time.monotonic()
                    if remaining <= 0:
                        raise TimeoutError('connection_budget_exhausted')
                    remaining = end - time.monotonic()
                    if remaining <= 0:
                        raise TimeoutError('connection_budget_exhausted')
                    secured.settimeout(remaining)
                    self.sock = secured
                    self.owner.count(label + '_connections')
                    return
                except OSError as exc:
                    last = exc
                    self.owner.count(label + '_connect_failures')
                    if secured is not None:
                        secured.close()
                    if sock is not None:
                        sock.close()
        if last is not None:
            raise last
        raise OSError('no_ip_address')


class ResponseLease:
    def __init__(self, owner, connection, response):
        self.owner, self.connection, self.response = owner, connection, response
        self.status = response.status
        self.released = False
        self.failed = False

    def __enter__(self):
        return self

    def read(self, size):
        try:
            return self.response.read(size)
        except (OSError, http.client.HTTPException) as exc:
            self.failed = True
            raise urllib.error.URLError(exc) from None

    def close(self):
        if not self.released:
            self.released = True
            reusable = not self.failed and self.response.isclosed() and not self.response.will_close
            self.response.close()
            self.owner.release(self.connection if reusable else None, discard=None if reusable else self.connection)

    def __exit__(self, kind, value, traceback):
        self.failed = self.failed or kind is not None
        self.close()


class KeepAliveHTTPS:
    """The lease stays checked out until the response has been fully consumed."""
    def __init__(self, *, context=None, connection_factory=None, resolver_path=Path('/etc/resolv.conf')):
        self.context = context or ssl.create_default_context()
        if not self.context.check_hostname or self.context.verify_mode != ssl.CERT_REQUIRED:
            raise ValueError('verified_tls_required')
        self.context.set_alpn_protocols(['http/1.1'])
        self.connection_factory = connection_factory
        self.resolver_fingerprint, self.servers = resolver_config(resolver_path)
        self.free = queue.LifoQueue(maxsize=2)
        for _ in range(2):
            self.free.put(None)
        self.lock = threading.Lock()
        self.closed = False
        self.dns_diagnosed = False
        self.stats = {'http_requests': 0, 'connection_attempts': 0, 'reused_requests': 0,
                      'dns_lookups': 0, 'dns_failures': 0, 'dns_seconds': 0.,
                      'in_flight': 0, 'max_in_flight': 0,
                      'ipv6_connect_attempts': 0, 'ipv4_connect_attempts': 0,
                      'ipv6_connect_failures': 0, 'ipv4_connect_failures': 0,
                      'ipv6_connections': 0, 'ipv4_connections': 0, 'ipv4_fallbacks': 0}

    def count(self, name, amount=1):
        with self.lock:
            self.stats[name] += amount

    def diagnose_dns(self, end):
        with self.lock:
            if self.dns_diagnosed:
                return []
            self.dns_diagnosed = True
        # At most one diagnostic set per cycle, <=3 queries and <=1.05 seconds.
        end = min(end, time.monotonic() + 1.05)
        results = []
        for i, server in enumerate(self.servers):
            remaining = end - time.monotonic()
            if remaining <= 0:
                break
            results.append({'resolver': i + 1, **dns_probe(server, timeout=min(.35, remaining))})
        return results

    def release(self, connection, *, discard=None):
        if discard is not None:
            discard.close()
        with self.lock:
            self.stats['in_flight'] -= 1
            closed = self.closed
        if closed:
            if connection is not None:
                connection.close()
        else:
            self.free.put(connection)

    def open(self, request, *, timeout):
        parsed = urllib.parse.urlsplit(request.full_url)
        if (parsed.scheme != 'https' or parsed.hostname != API_HOST or parsed.port not in (None, 443)
                or parsed.username is not None or parsed.password is not None or parsed.fragment
                or request.get_method() not in {'GET', 'PUT'}):
            raise ValueError('fixed_r2_origin_required')
        if self.closed or timeout <= 0:
            raise ValueError('transport_unavailable')
        end = time.monotonic() + timeout
        try:
            connection = self.free.get(timeout=timeout)
        except queue.Empty:
            raise TimeoutError('connection_pool_budget_exhausted') from None
        with self.lock:
            self.stats['in_flight'] += 1
            self.stats['max_in_flight'] = max(self.stats['max_in_flight'], self.stats['in_flight'])
        try:
            timeout = end - time.monotonic()
            if timeout <= 0:
                raise TimeoutError('connection_pool_budget_exhausted')
            if connection is None:
                connection = (self.connection_factory(timeout) if self.connection_factory else
                              PreferredHTTPSConnection(self, context=self.context, timeout=timeout))
            connection.timeout = timeout
            if connection.sock is not None:
                connection.sock.settimeout(timeout)
                self.count('reused_requests')
            else:
                self.count('connection_attempts')
            self.count('http_requests')
            target = urllib.parse.urlunsplit(('', '', parsed.path or '/', parsed.query, ''))
            # Exactly one request invocation. No implicit retry, including stale sockets.
            connection.request(request.get_method(), target, body=request.data, headers=dict(request.header_items()))
            if connection.sock is not None:
                remaining = end - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError('request_budget_exhausted')
                connection.sock.settimeout(remaining)
            response = connection.getresponse()
            if not 200 <= response.status < 300:
                status, headers = response.status, response.headers
                response.close()
                raise urllib.error.HTTPError(request.full_url, status, 'R2_HTTP_ERROR', headers, None)
            return ResponseLease(self, connection, response)
        except BaseException as exc:
            self.release(None, discard=connection)
            if isinstance(exc, (OSError, http.client.HTTPException)) and not isinstance(exc, urllib.error.URLError):
                raise urllib.error.URLError(exc) from None
            raise

    def snapshot(self):
        with self.lock:
            return {**self.stats, 'dns_seconds': round(self.stats['dns_seconds'], 6),
                    'address_policy': 'ipv6_preferred_ipv4_fallback',
                    'ipv6_connect_budget_seconds': IPV6_CONNECT_BUDGET,
                    'resolver_config_sha256': self.resolver_fingerprint}

    def close(self):
        with self.lock:
            self.closed = True
        while True:
            try:
                connection = self.free.get_nowait()
            except queue.Empty:
                break
            if connection is not None:
                connection.close()
