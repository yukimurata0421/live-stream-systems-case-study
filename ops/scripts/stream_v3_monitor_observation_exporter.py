#!/usr/bin/env python3
"""Read-only common-schema projection of arena's existing observation sources."""
from __future__ import annotations
import argparse,hashlib,json
from pathlib import Path
import sys
from types import SimpleNamespace
ROOT=Path(__file__).resolve().parents[2];sys.path.insert(0,str(ROOT/'src'))
sys.path.insert(0,str(ROOT/'ops/scripts'))
from stream_core.common import monitor_observation, observation_time, observation_contract
import stream_v3_observation_exporter as exporter
from stream_v3_release_audit import CachedAudit
import stream_v3_release


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--state-root',type=Path,required=True);p.add_argument('--clock-mesh',type=Path,required=True)
    p.add_argument('--source-host',required=True);p.add_argument('--port',type=int,default=9126)
    p.add_argument('--manifest',type=Path)
    p.add_argument('--release-registry',type=Path)
    a=p.parse_args();observation_contract.identifier(a.source_host)
    files=[Path(__file__),Path(exporter.__file__),Path(monitor_observation.__file__),Path(observation_time.__file__),Path(observation_contract.__file__),Path(stream_v3_release.__file__),ROOT/'ops/scripts/stream_v3_release_audit.py']
    digest=hashlib.sha256(b''.join(x.read_bytes() for x in files)).hexdigest()
    if a.manifest:
        manifest=json.loads(a.manifest.read_text())
        if not isinstance(manifest,dict) or len(manifest)>4096:raise ValueError('manifest_invalid')
        if manifest.get('schema')=='stream.release.v1':
            stream_v3_release.verify(ROOT,manifest['artifact_sha256'],require_bindings=True)
            digest=manifest['artifact_sha256']
        else:
            for name,expected in manifest.items():
                candidate=ROOT/name
                if candidate.is_symlink() or not candidate.resolve().is_relative_to(ROOT) or hashlib.sha256(candidate.read_bytes()).hexdigest()!=expected:
                    raise ValueError('release_drift')
            digest=hashlib.sha256(a.manifest.read_bytes()).hexdigest()
    effective=dict(vars(a))
    if a.release_registry:effective['release_registry_sha256']=hashlib.sha256(a.release_registry.read_bytes()).hexdigest()
    config=hashlib.sha256(json.dumps(effective,default=str,sort_keys=True).encode()).hexdigest()
    release_audit=CachedAudit(a.release_registry) if a.release_registry else None
    class Handler(exporter.BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path!='/metrics':self.send_error(404);return
            blocks=[]
            for fact in monitor_observation.read(a.state_root,a.clock_mesh):
                row=SimpleNamespace(observation=fact,collector_available=fact.validity==observation_contract.Validity.VALID,mbps=None)
                blocks.append(exporter.render(row,host=a.source_host,source_hash=digest,config_hash=config))
            # HELP/TYPE may appear only once for each metric family.
            seen=set();lines=[]
            for line in ''.join(blocks).splitlines():
                if line.startswith('#'):
                    if line in seen:continue
                    seen.add(line)
                lines.append(line)
            raw=('\n'.join(lines)+'\n').encode()
            if release_audit:
                raw += release_audit.read().encode()
            self.send_response(200);self.send_header('Content-Type','text/plain; version=0.0.4');self.send_header('Content-Length',str(len(raw)));self.end_headers();self.wfile.write(raw)
        def log_message(self,*_):pass
    server=exporter.Server(('127.0.0.1',a.port),Handler)
    print(json.dumps({'source_hash':digest,'config_hash':config,'port':server.server_address[1]}),flush=True)
    server.serve_forever()

if __name__=='__main__':main()
