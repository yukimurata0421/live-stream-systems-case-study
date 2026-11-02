#!/usr/bin/env python3
"""One bounded provider-delivery boundary for existing incident owners.

A receipt is durable before replying. Unknown HTTP outcomes are held, not
silently retried. Existing owners retain incident timing and Slack failover.
"""
from __future__ import annotations
import argparse
import hashlib
from http.server import BaseHTTPRequestHandler
import json
import os
import pwd
from pathlib import Path
import socket
import socketserver
import sqlite3
import struct
import time
from urllib.error import HTTPError,URLError
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler,ProxyHandler,Request,build_opener


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self,*_,**__):return None


def provider_send(route,content,username,webhooks):
    url=webhooks.get(route,'');parsed=urlsplit(url)
    host=parsed.hostname or ''
    allowed=(route=='discord' and host in ('discord.com','discordapp.com'))or(route=='slack' and host=='hooks.slack.com')
    if parsed.scheme!='https' or not allowed:return 'REJECTED',0
    body=({'content':content,'username':username} if route=='discord' else {'text':content,'username':username})
    request=Request(url,data=json.dumps(body,ensure_ascii=False).encode(),headers={'Content-Type':'application/json','User-Agent':'stream-notification-gateway/1.0'},method='POST')
    try:
        with build_opener(ProxyHandler({}),NoRedirect()).open(request,timeout=6)as response:
            code=response.status;response.read(4096)
        return ('DELIVERED' if 200<=code<300 else 'RETRY'),code
    except HTTPError as e:
        code=e.code;e.close();return 'RETRY',code
    except (OSError,URLError):return 'UNKNOWN',0


class Gateway:
    def __init__(self,path,send,*,clock=None):
        self.send=send;self.clock=clock or (lambda:(Path('/proc/sys/kernel/random/boot_id').read_text().strip(),time.clock_gettime(time.CLOCK_BOOTTIME)))
        self.db=sqlite3.connect(path,timeout=2)
        self.db.execute('PRAGMA journal_mode=WAL');self.db.execute('PRAGMA synchronous=FULL')
        self.db.execute('CREATE TABLE IF NOT EXISTS receipts (key TEXT PRIMARY KEY,route TEXT NOT NULL,status TEXT NOT NULL,code INTEGER NOT NULL,boot TEXT NOT NULL,mono REAL NOT NULL,created REAL NOT NULL)')
        # No payload or webhook is persisted in the receipt database.
    def deliver(self,value):
        if type(value)is not dict or set(value)!={'route','content','username'}:raise ValueError('shape_invalid')
        route,content,username=(value[k]for k in ('route','content','username'))
        if route not in ('slack','discord') or not isinstance(content,str) or not 0<len(content)<=12000 or not isinstance(username,str) or len(username)>128:raise ValueError('message_invalid')
        key=hashlib.sha256(json.dumps(value,sort_keys=True,ensure_ascii=False).encode()).hexdigest()
        boot,mono=self.clock();row=self.db.execute('SELECT status,code,boot,mono FROM receipts WHERE key=?',(key,)).fetchone()
        if row:
            status,code,oldboot,oldmono=row
            if status in ('SENDING','UNKNOWN'):return self.result(route,'UNKNOWN',code)
            if status=='DELIVERED':
                if boot!=oldboot:
                    with self.db:self.db.execute('UPDATE receipts SET boot=?,mono=? WHERE key=?',(boot,mono,key))
                    return self.result(route,'DELIVERED',code)
                if mono-oldmono<180:return self.result(route,'DELIVERED',code)
        # Keep unknowns for operator reconciliation. Only known completed rows
        # expire, and capacity exhaustion is visible rather than destructive.
        with self.db:
            self.db.execute("DELETE FROM receipts WHERE status IN ('DELIVERED','RETRY','REJECTED') AND created<?",(time.time()-7*86400,))
            if self.db.execute('SELECT count(*) FROM receipts').fetchone()[0]>=10000 and not row:return self.result(route,'CAPACITY',0)
            self.db.execute('INSERT OR REPLACE INTO receipts VALUES(?,?,?,?,?,?,?)',(key,route,'SENDING',0,boot,mono,time.time()))
        status,code=self.send(route,content,username)
        if status not in ('DELIVERED','RETRY','REJECTED','UNKNOWN'):status='UNKNOWN'
        with self.db:self.db.execute('UPDATE receipts SET status=?,code=? WHERE key=?',(status,code,key))
        print(json.dumps({'event':'delivery','key':key,'route':route,'status':status,'provider_status':code}),flush=True)
        return self.result(route,status,code)
    @staticmethod
    def result(route,status,code):return {'delivered':status=='DELIVERED','route':route,'status':status.lower(),'provider_status':code}
    def health(self):
        return {status:count for status,count in self.db.execute('SELECT status,count(*) FROM receipts GROUP BY status')}


class Server(socketserver.UnixStreamServer):
    request_queue_size=8
    def get_request(self):
        conn,address=super().get_request();conn.settimeout(10);return conn,address


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--socket',type=Path,required=True);p.add_argument('--database',type=Path,required=True);p.add_argument('--allow-uid',type=int,action='append',default=[]);p.add_argument('--allow-user',action='append',default=[])
    a=p.parse_args();webhooks={k:os.environ.get('STREAM_NOTIFY_'+k.upper()+'_WEBHOOK_URL','')for k in ('discord','slack')}
    g=Gateway(a.database,lambda *args:provider_send(*args,webhooks));allowed=set(a.allow_uid)
    class Handler(BaseHTTPRequestHandler):
        def authorized(self):
            _,uid,_=struct.unpack('3i',self.connection.getsockopt(socket.SOL_SOCKET,socket.SO_PEERCRED,12))
            current=set(allowed)
            for user in a.allow_user:
                try:current.add(pwd.getpwnam(user).pw_uid)
                except KeyError:pass
            return uid in current
        def reply(self,code,value):
            raw=json.dumps(value).encode();self.send_response(code);self.send_header('Content-Type','application/json');self.send_header('Content-Length',str(len(raw)));self.end_headers();self.wfile.write(raw)
        def do_GET(self):
            if not self.authorized():self.reply(403,{'status':'forbidden'});return
            if self.path!='/health':self.reply(404,{'status':'not_found'});return
            self.reply(200,{'receipts':g.health()})
        def do_POST(self):
            if not self.authorized():self.reply(403,{'status':'forbidden'});return
            try:
                n=int(self.headers.get('Content-Length','0'))
                if self.path!='/deliver' or not 0<n<=32768 or self.headers.get('Transfer-Encoding'):raise ValueError()
                result=g.deliver(json.loads(self.rfile.read(n)))
                self.reply(200 if result['delivered']else 503,result)
            except (ValueError,TypeError,sqlite3.Error):self.reply(400,{'status':'invalid'})
        def log_message(self,*_):pass
    # RuntimeDirectory is owned by this one service. Reclaim only its socket.
    if a.socket.exists():
        import stat
        if not stat.S_ISSOCK(a.socket.lstat().st_mode)or a.socket.lstat().st_uid!=os.getuid():raise ValueError('socket_owner_changed')
        a.socket.unlink()
    class HealthServer(Server):
        last=0
        def service_actions(self):
            mono=time.clock_gettime(time.CLOCK_BOOTTIME)
            if mono-self.last<5:return
            self.last=mono
            value={'schema':'stream.notification_gateway.v1','boot_id':Path('/proc/sys/kernel/random/boot_id').read_text().strip(),
                   'mono':mono,'receipts':g.health()}
            temporary=a.socket.with_name('status.pending');temporary.write_text(json.dumps(value));temporary.chmod(0o644);os.replace(temporary,a.socket.with_name('status.json'))
    with HealthServer(str(a.socket),Handler)as server:
        a.socket.chmod(0o666);server.serve_forever()

if __name__=='__main__':main()
