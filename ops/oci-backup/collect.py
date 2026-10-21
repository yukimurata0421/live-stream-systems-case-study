#!/usr/bin/env python3
"""Capture the active OCI relay read-only; stdout is a tar stream containing secrets."""
import hashlib
import io
import json
import os
from pathlib import Path
import re
import resource
import socket
import stat
import struct
import subprocess
import sys
import tarfile
from datetime import datetime, timezone
from urllib.request import build_opener, ProxyHandler

SCHEMA='stream_v3.oci_backup.v1'
UNITS=('stream-v3-oci-mediamtx.service','stream-v3-oci-keeper.service','wg-quick@wg-stream.service')


def digest(path):
    h=hashlib.sha256()
    with path.open('rb') as f:
        for chunk in iter(lambda:f.read(65536),b''):h.update(chunk)
    return h.hexdigest()


def properties(unit):
    keys=('Id','ActiveState','SubState','MainPID','FragmentPath','DropInPaths','ExecStart','UnitFileState')
    args=['systemctl','show',unit]
    for key in keys:args+=['-p',key]
    return dict(l.split('=',1) for l in subprocess.check_output(args,text=True,timeout=5).splitlines())


def api(path):
    with build_opener(ProxyHandler({})).open('http://127.0.0.1:9997'+path,timeout=3) as r:
        data=r.read(262145)
    if len(data)>262144:raise ValueError('native_api_too_large')
    return json.loads(data)


def normalized(users):
    return [{'user':u.get('user',''),'pass':u.get('pass',''),'ips':u.get('ips',[]),
             'permissions':[{'action':p['action'],'path':p.get('path','')} for p in u['permissions']]} for u in users]


def source_identity(host):
    identity_stat=host.lstat()
    if not stat.S_ISREG(identity_stat.st_mode) or identity_stat.st_uid!=0 or identity_stat.st_mode&0o022:
        raise ValueError('source_identity_configuration_not_root_owned')
    identity=json.loads(host.read_text())
    if (identity.get('host_id')!='relay-host'
            or identity.get('machine_id_sha256')!=digest(Path('/etc/machine-id'))):
        raise ValueError('unexpected_source_host')
    return identity


def collect(stream):
    if os.geteuid()!=0:raise ValueError('root_read_access_required')
    os.nice(19)
    resource.setrlimit(resource.RLIMIT_CPU,(30,30))
    source_identity(Path('/etc/stream-v3-oci-backup/source.json'))
    boot=Path('/proc/sys/kernel/random/boot_id').read_text().strip()
    units={name:properties(name) for name in UNITS}
    if any(u['ActiveState']!='active' for u in units.values()):raise ValueError('source_not_active')
    relay_pid=int(units[UNITS[0]]['MainPID']);keeper_pid=int(units[UNITS[1]]['MainPID'])
    executable=Path(f'/proc/{relay_pid}/exe').resolve(strict=True)
    if not str(executable).startswith('/opt/stream-v3-oci-relay/') or str(executable) not in units[UNITS[0]]['ExecStart']:
        raise ValueError('relay_executable_binding_invalid')
    # Go reserves a large virtual address range even for --version. Bound that
    # short read-only invocation by time, before limiting the Python collector.
    version=subprocess.check_output([str(executable),'--version'],text=True,timeout=5).strip()
    resource.setrlimit(resource.RLIMIT_AS,(256*1024*1024,256*1024*1024))
    keeper=Path('/opt/stream-v3-oci-keeper/forward_keeper.py')
    if str(keeper) not in units[UNITS[1]]['ExecStart']:raise ValueError('keeper_executable_binding_invalid')
    # The release binary must be self contained for redeployment without a compiler.
    with executable.open('rb') as f:
        header=f.read(64)
        if header[:6]!=b'\x7fELF\x02\x01' or struct.unpack_from('<H',header,18)[0]!=62:
            raise ValueError('expected_linux_amd64_binary')
        offset=struct.unpack_from('<Q',header,32)[0];size,count=struct.unpack_from('<HH',header,54)
        f.seek(offset)
        if any(struct.unpack_from('<I',f.read(size))[0]==3 for _ in range(count)):
            raise ValueError('dynamic_binary_dependencies_not_bundled')
    entries={};sources={};content={}
    def add(path,role='runtime',secret=False):
        path=Path(path)
        if not path.is_file() or path.is_symlink():raise ValueError('required_regular_file_missing')
        size=path.stat().st_size
        if size>128*1024*1024:raise ValueError('source_file_too_large')
        name=('rootfs' if role in ('runtime','network') else 'reference/rootfs')+str(path)
        entries[name]={'type':'file','target':str(path),'role':role,'secret':secret,
                       'size':size,'mode':stat.S_IMODE(path.stat().st_mode),'sha256':digest(path)}
        sources[name]=path
    def memory(name,data):
        entries[name]={'type':'file','role':'reference','secret':True,'size':len(data),'mode':0o600,'sha256':hashlib.sha256(data).hexdigest()}
        content[name]=data
    for unit,props in units.items():
        role='network' if unit.startswith('wg-') else 'runtime'
        for filename in [props['FragmentPath'],*props['DropInPaths'].split()]:
            if not filename:continue
            # Distribution template comes from the package manager, not an overwrite.
            add(filename,'reference' if filename.startswith('/usr/lib/') else role)
    add(executable);add(keeper)
    credential_sources=[]
    for unit in UNITS[:2]:
        text='\n'.join(Path(p).read_text() for p in [units[unit]['FragmentPath'],*units[unit]['DropInPaths'].split()])
        for value in re.findall(r'^LoadCredential=([^\n]+)$',text,re.M):
            path=value.split(':',1)[1]
            if not path.startswith(('/etc/stream-v3-oci-mediamtx/','/etc/stream-v3-twitch-daily/')):
                raise ValueError('unexpected_credential_source')
            credential_sources.append(path);add(path,secret=True)
    add('/etc/wireguard/wg-stream.conf','network',True)
    observer=Path('/opt/stream-v3-relay-observer/current')
    release=observer.resolve(strict=True)
    if not observer.is_symlink() or not release.is_relative_to('/opt/stream-v3-relay-observer/releases'):
        raise ValueError('observer_release_binding_invalid')
    for path in sorted(release.iterdir()):
        if path.is_file() and path.suffix in ('.py','.json'):add(path)
    name='rootfs'+str(observer)
    entries[name]={'type':'symlink','target':str(observer),'role':'runtime','linkname':os.path.relpath(release,observer.parent),'mode':0o777}
    for filename in ('/etc/sudoers.d/stream-v3-relay-observe','/etc/iptables/rules.v4','/etc/iptables/rules.v6',
                     '/var/lib/stream-v3-oci-keeper/keeper.json','/etc/stream-v3-oci-backup/source.json',
                     ):
        add(filename,'reference',True)
    # Do not back up administrator credentials or SSH host private keys.
    lines=Path('/home/ubuntu/.ssh/authorized_keys').read_text().splitlines()
    scoped=[x for x in lines if 'command="sudo -n /usr/bin/python3 -I -B /opt/stream-v3-relay-observer/current/relay_observe.py"' in x]
    if len(scoped)!=1:raise ValueError('scoped_observer_key_not_unique')
    memory('reference/observer-authorized-key.lines',('\n'.join(scoped)+'\n').encode())
    config=json.loads(Path('/etc/stream-v3-oci-mediamtx/mediamtx.yml').read_text())
    live=api('/v3/config/global/get')
    if normalized(config['authInternalUsers'])!=normalized(live['authInternalUsers']):
        raise ValueError('persistent_and_live_auth_differ')
    expected=sorted(x['dest'] for x in config['paths']['relay']['forward'])
    forwards=api('/v3/paths/forward-dests/list?path=relay')['items']
    if len(expected)!=2 or sorted(x['conf']['dest'] for x in forwards)!=expected:
        raise ValueError('persistent_and_live_forward_destinations_differ')
    metadata={'schema':SCHEMA,'host_id':'relay-host','boot_id':boot,'hostname':socket.gethostname(),
              'machine_id_sha256':digest(Path('/etc/machine-id')),'architecture':'x86_64',
              'captured_at_utc':datetime.now(timezone.utc).isoformat(),'os_release':Path('/etc/os-release').read_text(),
              'components':units,'credential_sources':sorted(set(credential_sources)),
              'binary_version':version,
              'packages':subprocess.check_output(['dpkg-query','-W','-f','${Package}=${Version}\n',
                            'python3','wireguard-tools','iptables','iptables-persistent','ca-certificates'],text=True,timeout=5).splitlines(),
              'forward_observation':[{'id':x['id'],'state':x['state'],'outbound_bytes':x['outboundBytes']} for x in forwards],
              'runtime_config_matches_persistent':True,'files':entries,
              'excluded':['SSH private keys','administrator authorized_keys','cloud credentials','CRA/control databases','inactive legacy services'],
              'services_started':False}
    with tarfile.open(fileobj=stream,mode='w|gz',compresslevel=1) as archive:
        for name,row in sorted(entries.items()):
            info=tarfile.TarInfo(name);info.mode=row['mode'];info.uid=info.gid=0
            if row['type']=='symlink':info.type=tarfile.SYMTYPE;info.linkname=row['linkname'];archive.addfile(info)
            else:
                info.size=row['size']
                if name in content:archive.addfile(info,io.BytesIO(content[name]))
                else:
                    with sources[name].open('rb') as f:archive.addfile(info,f)
        # Keeper state can change while read-only collection runs: omit a racy snapshot.
        if any(digest(path)!=entries[name]['sha256'] for name,path in sources.items()):
            raise ValueError('source_changed_during_capture')
        if Path('/proc/sys/kernel/random/boot_id').read_text().strip()!=boot:
            raise ValueError('source_boot_changed')
        if observer.resolve()!=release:raise ValueError('observer_release_changed')
        for name,old in units.items():
            if properties(name)!=old:raise ValueError('source_service_identity_changed')
        if Path(f'/proc/{relay_pid}/exe').resolve()!=executable or not Path(f'/proc/{keeper_pid}').exists():
            raise ValueError('source_process_changed')
        metadata['complete']=True
        data=(json.dumps(metadata,sort_keys=True,indent=2)+'\n').encode()
        info=tarfile.TarInfo('manifest.private.json');info.mode=0o600;info.size=len(data)
        archive.addfile(info,io.BytesIO(data))


if __name__=='__main__':
    try:collect(sys.stdout.buffer)
    except Exception as error:
        print(json.dumps({'result':'CAPTURE_FAILED','error_type':type(error).__name__,'reason':str(error) if isinstance(error,ValueError) else 'read_or_dependency_failure'}),file=sys.stderr)
        raise SystemExit(1)
