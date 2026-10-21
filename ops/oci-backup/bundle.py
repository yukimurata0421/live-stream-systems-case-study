#!/usr/bin/env python3
"""Create, verify offline, and stage an OCI restore bundle without starting services."""
import argparse
import ast
from datetime import datetime, timezone
import hashlib
import io
import json
import os
from pathlib import Path, PurePosixPath
import platform
import posixpath
import re
import secrets
import shlex
import shutil
import subprocess
import tarfile
import tempfile

SCHEMA='stream_v3.oci_backup.v1'
MAX_BYTES=256*1024*1024
MAX_FILES=160
UNITS=('stream-v3-oci-mediamtx.service','stream-v3-oci-keeper.service','wg-quick@wg-stream.service')


def digest(path):
    h=hashlib.sha256()
    with path.open('rb') as f:
        for chunk in iter(lambda:f.read(65536),b''):h.update(chunk)
    return h.hexdigest()


def private_dir(path):
    path.mkdir(mode=0o700,parents=True,exist_ok=True)
    if path.stat().st_uid!=os.geteuid() or path.stat().st_mode&0o077:
        raise ValueError('backup_directory_must_be_private')


def safe_member(name):
    p=PurePosixPath(name)
    if not name or p.is_absolute() or '..' in p.parts or '\\' in name or str(p)!=name:
        raise ValueError('unsafe_archive_path')
    return p


def runtime_path(name,role):
    prefix='rootfs' if role in ('runtime','network') else 'reference/rootfs'
    if not name.startswith(prefix+'/'):raise ValueError('invalid_runtime_member')
    value='/'+name[len(prefix)+1:]
    allowed=('/opt/stream-v3-oci-relay/','/opt/stream-v3-oci-keeper/',
             '/opt/stream-v3-relay-observer/','/etc/stream-v3-oci-mediamtx/',
             '/etc/stream-v3-twitch-daily/','/etc/systemd/system/stream-v3-oci-mediamtx.service',
             '/etc/systemd/system/stream-v3-oci-keeper.service',
             '/etc/systemd/system/wg-quick@wg-stream.service.d/')
    if role in ('runtime','network') and value!='/etc/wireguard/wg-stream.conf' and not any(value.startswith(x) for x in allowed):
        raise ValueError('target_outside_relay_scope')
    return value


def release_link(name,link,entries):
    resolved=posixpath.normpath(posixpath.join(posixpath.dirname(name),link))
    if (name!='rootfs/opt/stream-v3-relay-observer/current' or link.startswith('/')
            or not resolved.startswith('rootfs/opt/stream-v3-relay-observer/releases/')
            or not any(x.startswith(resolved+'/') for x in entries)):
        raise ValueError('unresolved_release_symlink')


def read_archive(stream,output=None):
    """Verify manifest hashes against content; extract only into a new dedicated directory."""
    if output is not None:
        output.mkdir(mode=0o700,parents=False,exist_ok=False)
    seen={};links=[];manifest=None;total=0
    with tarfile.open(fileobj=stream,mode='r|gz') as archive:
        for member in archive:
            safe_member(member.name)
            if manifest is not None:raise ValueError('data_after_final_manifest')
            if member.name in seen:raise ValueError('duplicate_archive_member')
            if len(seen)>=MAX_FILES or member.size<0 or member.size>MAX_BYTES:
                raise ValueError('archive_limit_exceeded')
            total+=member.size
            if total>MAX_BYTES:raise ValueError('archive_limit_exceeded')
            if member.name=='manifest.private.json':
                if not member.isfile() or member.size>1024*1024:raise ValueError('invalid_manifest')
                manifest=json.load(archive.extractfile(member));continue
            if member.issym():
                safe_member(posixpath.normpath(posixpath.join(posixpath.dirname(member.name),member.linkname)))
                resolved=posixpath.normpath(posixpath.join(posixpath.dirname(member.name),member.linkname))
                if member.linkname.startswith('/') or not resolved.startswith('rootfs/'):
                    raise ValueError('unsafe_symlink')
                seen[member.name]={'type':'symlink','mode':member.mode,'linkname':member.linkname}
                links.append((member.name,member.linkname));continue
            if not member.isfile():raise ValueError('unsupported_archive_member')
            h=hashlib.sha256();remaining=member.size;source=archive.extractfile(member)
            handle=None
            if output is not None:
                target=output/member.name;target.parent.mkdir(mode=0o700,parents=True,exist_ok=True)
                handle=target.open('xb');os.chmod(target,0o600)
            data=bytearray() if member.name.endswith(('.py','.json','.yml')) and member.size<=1024*1024 else None
            try:
                while remaining:
                    chunk=source.read(min(65536,remaining))
                    if not chunk:raise ValueError('truncated_archive_file')
                    h.update(chunk);remaining-=len(chunk)
                    if handle:handle.write(chunk)
                    if data is not None:data.extend(chunk)
            finally:
                if handle:handle.close()
            if data is not None:
                if member.name.endswith('.py'):ast.parse(data.decode(),filename=member.name)
                elif member.name.endswith('.json') or member.name.endswith('mediamtx.yml'):json.loads(data)
            seen[member.name]={'type':'file','mode':member.mode,'size':member.size,'sha256':h.hexdigest()}
    if not manifest or manifest.get('schema')!=SCHEMA or manifest.get('complete') is not True:
        raise ValueError('capture_not_complete')
    if set(manifest['files'])!=set(seen):raise ValueError('manifest_inventory_mismatch')
    for name,actual in seen.items():
        row=manifest['files'][name]
        if any(row.get(k)!=v for k,v in actual.items()):raise ValueError('manifest_content_mismatch')
        if name.startswith('rootfs/'):
            if row.get('role') not in ('runtime','network') or row.get('target')!=runtime_path(name,row['role']):
                raise ValueError('invalid_runtime_binding')
        elif not name.startswith('reference/'):raise ValueError('unexpected_archive_member')
    for name,link in links:
        release_link(name,link,seen)
        if output is not None:
            target=output/name;target.parent.mkdir(mode=0o700,parents=True,exist_ok=True);target.symlink_to(link)
    for props in manifest['components'].values():
        for p in [props['FragmentPath'],*props.get('DropInPaths','').split()]:
            if not any(name in seen for name in ('rootfs'+p,'reference/rootfs'+p)):
                raise ValueError('unit_file_missing')
    for p in manifest['credential_sources']:
        if 'rootfs'+p not in seen:raise ValueError('credential_missing')
    return manifest


def gpg_args(key):
    if key.stat().st_mode&0o077 or key.stat().st_uid!=os.geteuid():raise ValueError('key_file_must_be_private')
    home=key.parent/'gnupg';private_dir(home)
    return ['gpg','--no-options','--no-autostart','--homedir',str(home),'--batch','--no-tty',
            '--pinentry-mode','loopback','--no-symkey-cache','--s2k-count','65011712',
            '--s2k-digest-algo','SHA256','--passphrase-file',str(key)]


def decrypted(snapshot,key,output=None,expected_version=None):
    process=subprocess.Popen(gpg_args(key)+['--decrypt',str(snapshot/'runtime.tar.gz.gpg')],stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    try:
        manifest=read_archive(process.stdout,output)
        # Read through the authenticated trailer before trusting the archive.
        trailing=process.stdout.read(65537)
        if len(trailing)>65536:raise ValueError('unexpected_decrypted_trailer')
        process.stdout.close();errors=process.stderr.read(4096)
        if process.wait(timeout=15):raise ValueError('decryption_or_authentication_failed')
        if expected_version is not None and manifest['binary_version']!=expected_version:
            raise ValueError('upstream_source_version_mismatch')
        if output is not None:
            p=output/'manifest.private.json';p.write_text(json.dumps(manifest,indent=2)+'\n');p.chmod(0o600)
        return manifest
    finally:
        if process.poll() is None:process.kill()
        process.wait();process.stdout.close();process.stderr.close()


def verify(snapshot,key,output=None):
    index=json.loads((snapshot/'index.json').read_text())
    if index.get('schema')!='stream_v3.oci_backup_index.v1':raise ValueError('invalid_bundle_index')
    if index.get('key_id')!=hashlib.sha256(key.read_bytes()).hexdigest()[:16]:raise ValueError('wrong_backup_key')
    for name,row in index['parts'].items():
        safe_member(name)
        if '/' in name or (snapshot/name).is_symlink():raise ValueError('invalid_bundle_part')
        if (snapshot/name).stat().st_size!=row['size'] or digest(snapshot/name)!=row['sha256']:
            raise ValueError('bundle_part_corrupt')
    required={'runtime.tar.gz.gpg','bundle.py','collect.py','workspace-source.tar.gz','mediamtx-source.tar.gz','README.md'}
    if set(index['parts'])!=required:raise ValueError('incomplete_bundle_parts')
    manifest=decrypted(snapshot,key,output,index['upstream_tag'])
    return {'result':'VERIFIED','snapshot':snapshot.name,'file_count':len(manifest['files']),
            'binary_version':manifest['binary_version'],'services_started':False}


def install_files(stage,root):
    """Install verified files only; never invoke systemctl, change networking, or grant authority."""
    manifest=json.loads((stage/'manifest.private.json').read_text());plan=[]
    if root.resolve()==Path('/') and (os.geteuid()!=0 or platform.machine()!='x86_64'):
        raise ValueError('root_linux_amd64_required')
    for name,row in manifest['files'].items():
        if row['role'] not in ('runtime','network'):continue
        if row['target']!=runtime_path(name,row['role']):raise ValueError('invalid_runtime_binding')
        source=stage/name;target=root/row['target'].lstrip('/')
        # No symlink parent traversal and no replacement of different existing files.
        for parent in target.parents:
            if parent==root.parent:break
            if parent.is_symlink():raise ValueError('target_parent_is_symlink')
        if row['type']=='file':
            if source.is_symlink() or digest(source)!=row['sha256']:raise ValueError('staging_content_changed')
            mode=0o600 if row.get('secret') else row['mode']&0o777
            same=target.is_file() and not target.is_symlink() and digest(target)==row['sha256'] and (target.stat().st_mode&0o777)==mode
        else:
            release_link(name,row['linkname'],manifest['files'])
            if not source.is_symlink() or os.readlink(source)!=row['linkname']:raise ValueError('staging_link_changed')
            mode=None;same=target.is_symlink() and os.readlink(target)==row['linkname']
        if (target.exists() or target.is_symlink()) and not same:raise ValueError('existing_target_differs')
        plan.append((source,target,row,mode,same))
    for source,target,row,mode,same in sorted(plan,key=lambda x:x[2]['type']=='symlink'):
        if same:continue
        if not root.exists():root.mkdir(mode=0o700)
        missing=[];directory=target.parent
        while directory!=root and not directory.exists():missing.append(directory);directory=directory.parent
        for directory in reversed(missing):directory.mkdir(mode=0o755);directory.chmod(0o755)
        if row['type']=='symlink':target.symlink_to(row['linkname'])
        else:
            with source.open('rb') as src,target.open('xb') as dst:shutil.copyfileobj(src,dst,65536)
            target.chmod(mode)
    return {'result':'FILES_INSTALLED','files':len(plan),'services_started':False,'reference_files_installed':False}


def create(args):
    root=args.backup_root;private_dir(root);private_dir(root/'keys');private_dir(root/'snapshots')
    key=root/'keys/archive.passphrase'
    if not key.exists():
        fd=os.open(key,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
        with os.fdopen(fd,'w') as f:f.write(secrets.token_hex(32)+'\n')
    snapshot_id=datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')+'-'+secrets.token_hex(4)
    pending=root/'snapshots'/('.partial-'+snapshot_id);pending.mkdir(mode=0o700)
    source=Path(__file__).resolve().parent
    for name in ('bundle.py','collect.py','README.md'):shutil.copyfile(source/name,pending/name);(pending/name).chmod(0o600)
    if not re.fullmatch(r'v\d+\.\d+\.\d+',args.upstream_tag):raise ValueError('invalid_upstream_version_tag')
    commit=subprocess.check_output(['git','-C',str(args.upstream_repository),'rev-parse',args.upstream_tag+'^{commit}'],text=True).strip()
    subprocess.run(['git','-C',str(args.upstream_repository),'archive','--format=tar.gz','--prefix=mediamtx-'+args.upstream_tag+'/',
                    '-o',str(pending/'mediamtx-source.tar.gz'),args.upstream_tag],check=True,capture_output=True)
    repo=args.repository
    selected=['src/stream_v3/__init__.py','src/stream_v3/viewer_clock.py','src/stream_v3/viewer_clock_capture.py',
              'src/stream_v3/viewer_clock_worker.py','src/stream_v3/oci_monitor_probe.py','src/stream_v3/oci_monitor_metrics.py',
              'src/stream_v3/fallback_monitor.py','src/stream_v3/verified_clock.py','src/stream_v3/frame_receiver.py',
              'src/stream_v3/monitor_facts.py','ops/scripts/stream_v3_oci_monitor_native_facts.py',
              'tests/test_verified_clock.py','tests/test_frame_receiver.py','tests/test_monitor_facts.py','ops/scripts/stream_v3_oci_monitor_pull.py',
              'ops/scripts/stream_v3_oci_monitor_exporter_overlay.py','ops/config/oci_monitor_probe.example.json',
              'ops/config/oci_monitor_observer.example.json','ops/config/oci_backup_source.example.json',
              'ops/systemd/stream-v3-oci-monitor-pull.service','ops/systemd/stream-v3-oci-monitor-pull.timer',
              'tests/test_oci_monitor_probe.py','tests/test_oci_backup.py']
    selected += [str(p.relative_to(repo)) for directory in ('deploy/k3s/oci-monitor-probe','ops/oci-monitor','ops/oci-backup')
                 for p in sorted((repo/directory).iterdir()) if p.is_file() and (p.suffix in ('.py','.yaml','.md') or p.name=='Containerfile')]
    with tarfile.open(pending/'workspace-source.tar.gz','w:gz') as archive:
        for name in sorted(set(selected)):archive.add(repo/name,arcname=name,recursive=False)
    # Data is piped over two existing SSH connections straight into GPG.
    payload=(source/'collect.py').read_text()
    remote="import subprocess,sys\nr=subprocess.run(['ssh','-o','BatchMode=yes','-o','StrictHostKeyChecking=yes','-o','ConnectTimeout=5',"+repr(args.source)+",'sudo -n python3 -'],input="+repr(payload.encode())+")\nsys.exit(r.returncode)\n"
    transport=subprocess.Popen(['ssh','-o','BatchMode=yes','-o','StrictHostKeyChecking=yes','-o','ConnectTimeout=5',args.via,
                                shlex.join(['python3','-c',remote])],stdout=subprocess.PIPE,stderr=subprocess.PIPE,start_new_session=True)
    encrypt=subprocess.Popen(gpg_args(key)+['--cipher-algo','AES256','--compress-algo','none','--output',str(pending/'runtime.tar.gz.gpg'),'--symmetric'],stdin=transport.stdout,stdout=subprocess.DEVNULL,stderr=subprocess.PIPE)
    transport.stdout.close()
    try:
        _,encrypt_error=encrypt.communicate(timeout=90);transport.wait(timeout=10);remote_error=transport.stderr.read(65536)
        if encrypt.returncode or transport.returncode:
            (pending/'capture-failure.private.log').write_bytes(remote_error+encrypt_error)
            (pending/'capture-failure.private.log').chmod(0o600)
            raise ValueError('source_capture_or_encryption_failed')
    finally:
        if encrypt.poll() is None:encrypt.kill()
        if transport.poll() is None:
            import signal
            os.killpg(transport.pid,signal.SIGKILL)
        encrypt.wait();transport.wait()
    parts={p.name:{'sha256':digest(p),'size':p.stat().st_size} for p in pending.iterdir() if p.is_file()}
    index={'schema':'stream_v3.oci_backup_index.v1','snapshot_id':snapshot_id,'created_at_utc':datetime.now(timezone.utc).isoformat(),
           'key_id':hashlib.sha256(key.read_bytes()).hexdigest()[:16],'parts':parts,
           'upstream_tag':args.upstream_tag,'upstream_commit':commit,'upstream_worktree_changes_included':False,
           'media_server_build_reproduced':False,'services_started':False}
    (pending/'index.json').write_text(json.dumps(index,indent=2)+'\n')
    for p in pending.iterdir():p.chmod(0o600)
    result=verify(pending,key);final=pending.with_name(snapshot_id);pending.rename(final)
    latest=root/'.latest.tmp';latest.unlink(missing_ok=True);latest.symlink_to('snapshots/'+snapshot_id);os.replace(latest,root/'latest')
    result.update(snapshot=str(final),encrypted_bytes=(final/'runtime.tar.gz.gpg').stat().st_size)
    return result


def main():
    parser=argparse.ArgumentParser(description=__doc__);commands=parser.add_subparsers(dest='command',required=True)
    p=commands.add_parser('create');p.add_argument('--backup-root',type=Path,required=True);p.add_argument('--repository',type=Path,required=True)
    p.add_argument('--upstream-repository',type=Path,required=True);p.add_argument('--upstream-tag',default='v1.21.2')
    p.add_argument('--via',default='dell');p.add_argument('--source',default='relay-host')
    for name in ('verify','stage'):
        p=commands.add_parser(name);p.add_argument('--snapshot',type=Path,required=True);p.add_argument('--key-file',type=Path,required=True)
        if name=='stage':p.add_argument('--directory',type=Path,required=True)
    p=commands.add_parser('install-files');p.add_argument('--directory',type=Path,required=True);p.add_argument('--root',type=Path,required=True)
    args=parser.parse_args()
    if args.command=='create':result=create(args)
    elif args.command=='verify':result=verify(args.snapshot,args.key_file)
    elif args.command=='stage':
        result=verify(args.snapshot,args.key_file,args.directory);result['result']='STAGED'
    else:result=install_files(args.directory,args.root)
    print(json.dumps(result))


if __name__=='__main__':
    try:main()
    except Exception as error:
        print(json.dumps({'result':'FAILED','error_type':type(error).__name__,
                          'reason':str(error) if isinstance(error,ValueError) else 'operation_or_dependency_failed'}))
        raise SystemExit(1)
