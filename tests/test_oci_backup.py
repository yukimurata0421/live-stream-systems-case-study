import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import tarfile

import pytest

ROOT=Path(__file__).parents[1]
spec=importlib.util.spec_from_file_location('oci_backup',ROOT/'ops/oci-backup/bundle.py')
backup=importlib.util.module_from_spec(spec);spec.loader.exec_module(backup)


def archive_bytes(*,corrupt=False,missing_manifest=False,extra=None,link='releases/current',duplicate=False):
    unit='/etc/systemd/system/stream-v3-oci-mediamtx.service'
    cfg='/etc/stream-v3-oci-mediamtx/mediamtx.yml'
    observer='/opt/stream-v3-relay-observer/releases/current/relay_observe.py'
    data={'rootfs'+unit:b'[Service]\nType=simple\n',
          'rootfs'+cfg:b'{"paths":{"relay":{}}}\n',
          'rootfs'+observer:b'print("fixture")\n',
          'rootfs/etc/wireguard/wg-stream.conf':b'[Interface]\nPrivateKey=fixture-only\n',
          'reference/state.json':b'{"phase":"IDLE"}\n'}
    if extra:data[extra]=b'extra'
    rows={name:{'type':'file','size':len(value),'mode':0o600,'sha256':hashlib.sha256(value).hexdigest(),
                'role':'reference' if name.startswith('reference/') else ('network' if 'wireguard' in name else 'runtime'),
                'target':'/'+name.removeprefix('rootfs/'),'secret':'mediamtx.yml' in name}
          for name,value in data.items()}
    alias='rootfs/opt/stream-v3-relay-observer/current'
    rows[alias]={'type':'symlink','linkname':link,'mode':0o777,'role':'runtime','target':'/opt/stream-v3-relay-observer/current'}
    manifest={'schema':backup.SCHEMA,'complete':True,'files':rows,'credential_sources':[cfg],
              'components':{'relay':{'FragmentPath':unit,'DropInPaths':''}},'binary_version':'v1.21.2'}
    if corrupt:rows['rootfs'+cfg]['sha256']='0'*64
    output=io.BytesIO()
    with tarfile.open(fileobj=output,mode='w:gz') as archive:
        for name,value in data.items():
            info=tarfile.TarInfo(name);info.size=len(value);info.mode=0o600;archive.addfile(info,io.BytesIO(value))
        if duplicate:
            info=tarfile.TarInfo('rootfs'+unit);info.size=0;info.mode=0o600;archive.addfile(info,io.BytesIO())
        info=tarfile.TarInfo(alias);info.type=tarfile.SYMTYPE;info.mode=0o777;info.linkname=link;archive.addfile(info)
        if not missing_manifest:
            value=json.dumps(manifest).encode();info=tarfile.TarInfo('manifest.private.json');info.size=len(value);info.mode=0o600
            archive.addfile(info,io.BytesIO(value))
    return output.getvalue()


def test_verified_archive_and_files_only_install_do_not_restore_authority(tmp_path,monkeypatch):
    stage=tmp_path/'stage';manifest=backup.read_archive(io.BytesIO(archive_bytes()),stage)
    (stage/'manifest.private.json').write_text(json.dumps(manifest))
    monkeypatch.setattr(backup.subprocess,'run',lambda *_a,**_k:pytest.fail('installer must not run external commands'))
    root=tmp_path/'installed';result=backup.install_files(stage,root)
    assert result['services_started'] is False and result['reference_files_installed'] is False
    assert (root/'etc/stream-v3-oci-mediamtx/mediamtx.yml').stat().st_mode&0o777==0o600
    assert (root/'opt/stream-v3-relay-observer/current/relay_observe.py').read_text()=='print("fixture")\n'
    assert not (root/'reference').exists()
    assert backup.install_files(stage,root)==result


@pytest.mark.parametrize('kwargs',[
    {'corrupt':True},{'missing_manifest':True},{'extra':'../escape'},
    {'extra':'rootfs/etc/shadow'},{'duplicate':True},{'link':'/etc'},
    {'link':'../../../../../../etc'},{'link':'releases/missing'},
])
def test_rejects_incomplete_corrupt_and_unsafe_archives(kwargs):
    with pytest.raises(ValueError):backup.read_archive(io.BytesIO(archive_bytes(**kwargs)))


def test_staging_mutation_and_existing_target_refuse_before_any_write(tmp_path):
    stage=tmp_path/'stage';manifest=backup.read_archive(io.BytesIO(archive_bytes()),stage)
    (stage/'manifest.private.json').write_text(json.dumps(manifest))
    root=tmp_path/'installed';conf=root/'etc/stream-v3-oci-mediamtx/mediamtx.yml';conf.parent.mkdir(parents=True);conf.write_text('existing')
    with pytest.raises(ValueError,match='existing_target'):backup.install_files(stage,root)
    assert not (root/'etc/systemd').exists() and conf.read_text()=='existing'
    (stage/'rootfs/etc/stream-v3-oci-mediamtx/mediamtx.yml').write_text('modified')
    with pytest.raises(ValueError,match='staging_content'):backup.install_files(stage,tmp_path/'other')
    assert not (tmp_path/'other').exists()


def test_does_not_follow_existing_target_symlink(tmp_path):
    stage=tmp_path/'stage';manifest=backup.read_archive(io.BytesIO(archive_bytes()),stage)
    (stage/'manifest.private.json').write_text(json.dumps(manifest))
    root=tmp_path/'installed';root.mkdir();outside=tmp_path/'outside';outside.mkdir();(root/'etc').symlink_to(outside)
    with pytest.raises(ValueError,match='symlink'):backup.install_files(stage,root)
    assert not list(outside.iterdir())


def test_altered_staging_manifest_cannot_redirect_install(tmp_path):
    stage=tmp_path/'stage';manifest=backup.read_archive(io.BytesIO(archive_bytes()),stage)
    row=manifest['files']['rootfs/etc/stream-v3-oci-mediamtx/mediamtx.yml'];row['target']='/etc/shadow'
    (stage/'manifest.private.json').write_text(json.dumps(manifest))
    with pytest.raises(ValueError,match='binding'):backup.install_files(stage,tmp_path/'root')
    assert not (tmp_path/'root').exists()


def test_new_runtime_directories_are_traversable_under_private_umask(tmp_path):
    stage=tmp_path/'stage';manifest=backup.read_archive(io.BytesIO(archive_bytes()),stage)
    (stage/'manifest.private.json').write_text(json.dumps(manifest));root=tmp_path/'root'
    previous=os.umask(0o077)
    try:backup.install_files(stage,root)
    finally:os.umask(previous)
    assert (root/'opt/stream-v3-relay-observer/releases').stat().st_mode&0o777==0o755


@pytest.fixture
def encrypted_snapshot(tmp_path):
    if not shutil.which('gpg'):pytest.skip('GnuPG unavailable')
    key=tmp_path/'keys/archive.passphrase';key.parent.mkdir(mode=0o700);key.write_text('fixture-only-passphrase\n');key.chmod(0o600)
    snapshot=tmp_path/'snapshot';snapshot.mkdir(mode=0o700)
    subprocess.run(backup.gpg_args(key)+['--cipher-algo','AES256','--compress-algo','none',
                    '--output',str(snapshot/'runtime.tar.gz.gpg'),'--symmetric'],input=archive_bytes(),check=True,capture_output=True)
    for name in ('bundle.py','collect.py','README.md'):shutil.copyfile(ROOT/'ops/oci-backup'/name,snapshot/name)
    for name in ('workspace-source.tar.gz','mediamtx-source.tar.gz'):
        with tarfile.open(snapshot/name,'w:gz'):pass
    parts={p.name:{'sha256':backup.digest(p),'size':p.stat().st_size} for p in snapshot.iterdir()}
    index={'schema':'stream_v3.oci_backup_index.v1','key_id':hashlib.sha256(key.read_bytes()).hexdigest()[:16],'parts':parts,'upstream_tag':'v1.21.2'}
    (snapshot/'index.json').write_text(json.dumps(index))
    return snapshot,key,index


def test_gpg_offline_verify_and_authenticated_stage(encrypted_snapshot,tmp_path):
    snapshot,key,_=encrypted_snapshot
    assert backup.verify(snapshot,key)['result']=='VERIFIED'
    stage=tmp_path/'stage';backup.verify(snapshot,key,stage)
    assert (stage/'manifest.private.json').exists()
    assert backup.install_files(stage,tmp_path/'root')['services_started'] is False


def test_gpg_authentication_failure_never_completes_staging(encrypted_snapshot,tmp_path):
    snapshot,key,index=encrypted_snapshot
    file=snapshot/'runtime.tar.gz.gpg';data=bytearray(file.read_bytes());data[-6]^=1;file.write_bytes(data)
    index['parts'][file.name]['sha256']=backup.digest(file);(snapshot/'index.json').write_text(json.dumps(index))
    stage=tmp_path/'stage'
    with pytest.raises(ValueError):backup.verify(snapshot,key,stage)
    assert not (stage/'manifest.private.json').exists()


def test_wrong_key_and_changed_part_are_rejected(encrypted_snapshot,tmp_path):
    snapshot,key,_=encrypted_snapshot
    other=tmp_path/'other-key';other.write_text('different');other.chmod(0o600)
    with pytest.raises(ValueError,match='wrong_backup_key'):backup.verify(snapshot,other)
    (snapshot/'README.md').write_text('changed')
    with pytest.raises(ValueError,match='bundle_part_corrupt'):backup.verify(snapshot,key)


def test_wrong_source_version_never_completes_stage(encrypted_snapshot,tmp_path):
    snapshot,key,index=encrypted_snapshot;index['upstream_tag']='v0.0.0';(snapshot/'index.json').write_text(json.dumps(index))
    with pytest.raises(ValueError,match='version_mismatch'):backup.verify(snapshot,key,tmp_path/'stage')
    assert not (tmp_path/'stage/manifest.private.json').exists()
