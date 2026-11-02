"""Shared delivery client. Success means provider acceptance, never queue admission."""
from __future__ import annotations
import http.client
import json
import socket

SOCKET='/run/stream-notification-gateway/gateway.sock'


class Connection(http.client.HTTPConnection):
    def __init__(self,path,timeout):super().__init__('localhost',timeout=timeout);self.path=path
    def connect(self):
        self.sock=socket.socket(socket.AF_UNIX,socket.SOCK_STREAM);self.sock.settimeout(self.timeout);self.sock.connect(self.path)


def send(route,content,*,username='ADS-B Stream Watchdog',timeout=10.,path=SOCKET):
    if route not in ('slack','discord'):return False,'gateway_route_invalid'
    connection=Connection(path,timeout)
    try:
        body=json.dumps({'route':route,'content':content,'username':username},ensure_ascii=False).encode()
        if len(body)>32768:return False,'gateway_message_too_large'
        connection.request('POST','/deliver',body,{'Content-Type':'application/json'})
        response=connection.getresponse();raw=response.read(4097)
        if len(raw)>4096:return False,'gateway_response_invalid'
        value=json.loads(raw)
        if response.status==200 and value.get('delivered')is True and value.get('route')==route:
            return True,'gateway_provider_accepted'
        return False,'gateway_'+str(value.get('status','unavailable'))[:64]
    except (OSError,ValueError,http.client.HTTPException):return False,'gateway_unavailable'
    finally:connection.close()
