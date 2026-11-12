"""UNIX socket health client that does not import API or TLS modules."""
import json
import socket
import struct
import sys
import time
import uuid


def check(path='/run/stream-v3-pod-api-observer/observer.sock', expected_uid=992, timeout=5.0):
    nonce=uuid.uuid4().hex
    deadline=time.monotonic()+timeout
    with socket.socket(socket.AF_UNIX) as connection:
        connection.settimeout(timeout)
        connection.connect(path)
        uid=struct.unpack('3i',connection.getsockopt(socket.SOL_SOCKET,socket.SO_PEERCRED,12))[1]
        if uid!=expected_uid:raise ValueError('POD_API_PEER_MISMATCH')
        connection.sendall(json.dumps({'operation':'status','request_id':nonce}).encode()+b'\n')
        data=b''
        while not data.endswith(b'\n'):
            remaining=deadline-time.monotonic()
            if remaining<=0:raise TimeoutError('POD_API_TIMEOUT')
            connection.settimeout(remaining)
            part=connection.recv(min(4096,65537-len(data)))
            if not part:raise ValueError('POD_API_INCOMPLETE_RESPONSE')
            data+=part
            if len(data)>65536:raise ValueError('POD_API_RESPONSE_TOO_LARGE')
    value=json.loads(data)
    if (not isinstance(value,dict) or value.get('schema')!='stream_v3.pod_api_observation.v1'
            or value.get('request_id')!=nonce or value.get('error') or not isinstance(value.get('status'),dict)):
        raise ValueError('POD_API_RESPONSE_BINDING_MISMATCH')


if __name__=='__main__':
    # Positional overrides are for isolated fixtures; probes use the fixed defaults.
    try:check(*(sys.argv[1:2]),expected_uid=int(sys.argv[2]) if len(sys.argv)>2 else 992)
    except (OSError,ValueError,TypeError):raise SystemExit(1)
